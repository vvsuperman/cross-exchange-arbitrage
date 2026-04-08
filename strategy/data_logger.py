"""Data logging module for trade and BBO data."""
import csv
import json
import os
import logging
import time
import threading
from decimal import Decimal
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from .db_logger import ArbInfoDB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class DataLogger:
    """Handles CSV and JSON logging for trades and BBO data."""

    @staticmethod
    def _calculate_time_gap(start_time_str: str, end_time_str: str) -> float:
        """Calculate time gap in seconds between two time strings.
        
        Args:
            start_time_str: Start time string in format '%Y-%m-%dT%H:%M:%S.%f'[:-3]
            end_time_str: End time string in format '%Y-%m-%dT%H:%M:%S.%f'[:-3]
            
        Returns:
            Time gap in seconds, or empty string if either time is empty
        """
        if not start_time_str or not end_time_str:
            return ''
        try:
            # Parse time strings: format is 'YYYY-MM-DDTHH:MM:SS.mmm'
            start_dt = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%f')
            end_dt = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S.%f')
            gap_seconds = (end_dt - start_dt).total_seconds()
            return gap_seconds
        except (ValueError, AttributeError) as e:
            return ''


    def __init__(self, exchange: str, ticker: str, logger: logging.Logger):
        """Initialize data logger with file paths."""
        self.exchange = exchange
        self.ticker = ticker
        self.logger = logger
        os.makedirs("logs", exist_ok=True)

        self.csv_filename = f"logs/{exchange}_{ticker}_trades.csv"
        self.bbo_csv_filename = f"logs/{exchange}_{ticker}_bbo_data.csv"
        self.thresholds_json_filename = f"logs/{exchange}_{ticker}_thresholds.json"
        # 新增套利记录CSV文件
        self.arbitrage_csv_filename = f"logs/{exchange}_{ticker}_arbitrage.csv"

        # CSV file handles for efficient writing (kept open)
        self.bbo_csv_file = None
        self.bbo_csv_writer = None
        self.bbo_write_counter = 0
        self.bbo_flush_interval = 10  # Flush every N writes

        # 套利记录相关
        self.arbitrage_csv_file = None
        self.arbitrage_csv_writer = None
        self.pending_arbitrage_records = {}  # 存储 discovery {client_order_id: record_dict}
        self.arbitrage_lock = Lock()  # 线程锁，保护 pending_arbitrage_records 和 CSV 写入
        self.cleanup_interval = 3600  # 清理间隔（秒），每小时清理一次
        self.record_timeout = 3600  # 记录超时时间（秒），1小时之前的记录将被清理
        self.calculation_interval = 30  # 计算间隔（秒），每30秒计算一次

        # 线程池用于异步处理 log_fill，不阻塞主监听
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="log_fill")
        # 启动清理线程
        self._start_cleanup_thread()
        # 启动计算线程
        self._start_calculation_thread()

        self._initialize_csv_file()
        self._initialize_bbo_csv_file()
        self._initialize_arbitrage_csv_file()
        
        # 初始化数据库记录器
        self.arb_info_db = ArbInfoDB()

    def _initialize_csv_file(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity'])

    def _initialize_bbo_csv_file(self):
        """Initialize BBO CSV file with headers if it doesn't exist."""
        file_exists = os.path.exists(self.bbo_csv_filename)

        # Open file in append mode (will create if doesn't exist)
        self.bbo_csv_file = open(self.bbo_csv_filename, 'a', newline='', buffering=8192)  # 8KB buffer
        self.bbo_csv_writer = csv.writer(self.bbo_csv_file)

        # Write header only if file is new
        if not file_exists:
            self.bbo_csv_writer.writerow([
                'timestamp',
                'maker_bid',
                'maker_ask',
                'lighter_bid',
                'lighter_ask',
                'long_maker_spread',
                'short_maker_spread',
                'long_maker',
                'short_maker',
                'long_maker_threshold',
                'short_maker_threshold'
            ])
            self.bbo_csv_file.flush()  # Ensure header is written immediately

    def _initialize_arbitrage_csv_file(self):
        """Initialize arbitrage CSV file with headers if it doesn't exist."""
        file_exists = os.path.exists(self.arbitrage_csv_filename)

        # Open file in append mode (will create if doesn't exist)
        self.arbitrage_csv_file = open(self.arbitrage_csv_filename, 'a', newline='', buffering=8192)
        self.arbitrage_csv_writer = csv.writer(self.arbitrage_csv_file)

        # Write header only if file is new
        if not file_exists:
            self.arbitrage_csv_writer.writerow([
                'client_order_id',
                'discovery_time',
                'edgex_discovery_price',
                'lighter_discovery_price',
                'discovery_spread',
                'edgex_fill_time',
                'edgex_fill_price',
                'edgex_fill_amount',
                'edgex_fill_quantity',
                'lighter_fill_time',
                'lighter_fill_price',
                'lighter_fill_amount',
                'lighter_fill_quantity',
                'actual_spread',
                'actual_profit',
                'profit_rate',
                'edgex_fill_gap',
                'lighter_fill_gap',
                'edgex_direction',
                'lighter_direction',
                'calculated'
            ])
            self.arbitrage_csv_file.flush()

    def log_fill(self, client_order_id: str, exchange: str, fill_amount: Decimal = None,
                 fill_quantity: Decimal = None, fill_price: Decimal = None,
                 edgex_discovery_price: Decimal = None, lighter_discovery_price: Decimal = None,
                 discovery_spread: Decimal = None, edgex_direction: str = None, lighter_direction: str = None):
        """统一记录成交：edgex 只写 edgex 列、lighter 留空；lighter 只写 lighter 列、edgex 留空；discovery 只写 discovery 列、fill 留空；exchange 含「风控拦截」时写审计行，calculated 存完整原因串。异步提交到线程池处理，不阻塞主监听。"""
        # 提交到线程池异步处理
        self.executor.submit(
            self._log_fill_sync,
            client_order_id, exchange, fill_amount, fill_quantity, fill_price,
            edgex_discovery_price, lighter_discovery_price, discovery_spread,
            edgex_direction, lighter_direction
        )

    def _log_fill_sync(self, client_order_id: str, exchange: str, fill_amount: Decimal = None,
                       fill_quantity: Decimal = None, fill_price: Decimal = None,
                       edgex_discovery_price: Decimal = None, lighter_discovery_price: Decimal = None,
                       discovery_spread: Decimal = None, edgex_direction: str = None, lighter_direction: str = None):
        """同步执行日志写入（在线程中执行）。"""
        with self.arbitrage_lock:
            record = None
            # discovery 模式：创建 pending record 并写入 CSV
            if exchange == 'discovery':
                discovery_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                record = {
                    'client_order_id': client_order_id,
                    'discovery_time': discovery_time,
                    'edgex_discovery_price': float(edgex_discovery_price) if edgex_discovery_price is not None else '',
                    'lighter_discovery_price': float(lighter_discovery_price) if lighter_discovery_price is not None else '',
                    'discovery_spread': float(discovery_spread) if discovery_spread is not None else '',
                    'edgex_fill_time': '',
                    'edgex_fill_price': '',
                    'edgex_fill_amount': '',
                    'edgex_fill_quantity': '',
                    'lighter_fill_time': '',
                    'lighter_fill_price': '',
                    'lighter_fill_amount': '',
                    'lighter_fill_quantity': '',
                    'actual_spread': '',
                    'actual_profit': '',
                    'profit_rate': '',
                    'edgex_direction': edgex_direction or '',
                    'lighter_direction': lighter_direction or '',
                    'edgex_fill_gap': '',
                    'lighter_fill_gap': ''
                }
                self.logger.info(
                    f"📊 Arbitrage discovery logged [client_order_id: {client_order_id}]: "
                    f"EdgeX={edgex_discovery_price}, Lighter={lighter_discovery_price}, DiscoverySpread={discovery_spread}"
                )
                
                # 记录到数据库 (received类型 - edgex_arb中redis接受到价差信号)
                try:
                    self.arb_info_db.log_arb_info(
                        symbol=self.ticker,
                        info_type=ArbInfoDB.INFO_TYPE_RECEIVED,
                        client_order_id=client_order_id,
                        edgex_price=edgex_discovery_price,
                        lighter_price=lighter_discovery_price,
                        spread_ratio=discovery_spread,
                        edgex_direction=edgex_direction,
                        lighter_direction=lighter_direction,
                        timestamp=discovery_time,
                    )
                except Exception as e:
                    self.logger.warning(f"写入数据库失败 (received): {e}")
            elif exchange == 'edgex':
                # 创建 edgex record        
                record = {
                    'client_order_id': client_order_id,
                    'discovery_time': '',
                    'edgex_discovery_price': '',
                    'lighter_discovery_price': '',
                    'discovery_spread': '',
                    'edgex_fill_time': '',
                    'edgex_fill_price': '',
                    'edgex_fill_amount': '',
                    'edgex_fill_quantity': '',
                    'lighter_fill_time': '',
                    'lighter_fill_price': '',
                    'lighter_fill_amount': '',
                    'lighter_fill_quantity': '',
                    'actual_spread': '',
                    'actual_profit': '',
                    'profit_rate': '',
                    'edgex_fill_gap': '',
                    'lighter_fill_gap': '',
                    'edgex_direction': '',
                    'lighter_direction': ''
                }
                
                edgex_fill_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                record['edgex_fill_time'] = edgex_fill_time
                record['edgex_fill_price'] = float(fill_price)
                record['edgex_fill_amount'] = float(fill_amount)
                record['edgex_fill_quantity'] = float(fill_quantity)
                # 保存 edgex direction
                if edgex_direction:
                    record['edgex_direction'] = edgex_direction
                # 计算 edgex_fill_gap
               
             
                self.logger.info(
                    f"📊 EdgeX fill logged [client_order_id: {client_order_id}]: "
                    f"Price={fill_price}, Amount={fill_amount}, Quantity={fill_quantity}"
                )
                
                # 记录到数据库 (edgex_filled类型)
                try:
                    self.arb_info_db.log_arb_info(
                        symbol=self.ticker,
                        info_type=ArbInfoDB.INFO_TYPE_EDGEX_FILLED,
                        client_order_id=client_order_id,
                        edgex_price=fill_price,
                        edgex_quantity=fill_quantity,
                        edgex_direction=edgex_direction,
                        timestamp=edgex_fill_time,
                    )
                except Exception as e:
                    self.logger.warning(f"写入数据库失败 (edgex_filled): {e}")
            elif exchange == 'lighter':  # lighter
                # 创建 lighter record       
                record = {
                    'client_order_id': client_order_id,
                    'discovery_time': '',
                    'edgex_discovery_price': '',
                    'lighter_discovery_price': '',
                    'discovery_spread': '',
                    'edgex_fill_time': '',
                    'edgex_fill_price': '',
                    'edgex_fill_amount': '',
                    'edgex_fill_quantity': '',
                    'lighter_fill_time': '',
                    'lighter_fill_price': '',
                    'lighter_fill_amount': '',
                    'lighter_fill_quantity': '',
                    'actual_spread': '',
                    'actual_profit': '',
                    'profit_rate': '',
                    'edgex_fill_gap': '',
                    'lighter_fill_gap': '',
                    'edgex_direction': '',
                    'lighter_direction': ''
                }
                
                # 保留 pending 中的 edgex 字段（如果有）
               
                lighter_fill_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                record['lighter_fill_time'] = lighter_fill_time
                record['lighter_fill_price'] = float(fill_price)
                record['lighter_fill_amount'] = float(fill_amount)
                record['lighter_fill_quantity'] = float(fill_quantity)
                # 保存 lighter direction
                if lighter_direction:
                    record['lighter_direction'] = lighter_direction
                # 计算 lighter_fill_gap
               
                self.logger.info(
                    f"📊 Lighter fill logged [client_order_id: {client_order_id}]: "
                    f"Price={fill_price}, Amount={fill_amount}, Quantity={fill_quantity}"
                )
                # 确保记录被保存到 pending_arbitrage_records（如果上面没有保存，即当有 edgex 记录时）
                
                # 记录到数据库 (lighter_filled类型)
                try:
                    self.arb_info_db.log_arb_info(
                        symbol=self.ticker,
                        info_type=ArbInfoDB.INFO_TYPE_LIGHTER_FILLED,
                        client_order_id=client_order_id,
                        lighter_price=fill_price,
                        lighter_quantity=fill_quantity,
                        lighter_direction=lighter_direction,
                        timestamp=lighter_fill_time,
                    )
                except Exception as e:
                    self.logger.warning(f"写入数据库失败 (lighter_filled): {e}")
            elif '风控拦截' in exchange:
                risk_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                record = {
                    'client_order_id': client_order_id,
                    'discovery_time': risk_time,
                    'edgex_discovery_price': '',
                    'lighter_discovery_price': '',
                    'discovery_spread': '',
                    'edgex_fill_time': '',
                    'edgex_fill_price': '',
                    'edgex_fill_amount': '',
                    'edgex_fill_quantity': '',
                    'lighter_fill_time': '',
                    'lighter_fill_price': '',
                    'lighter_fill_amount': '',
                    'lighter_fill_quantity': '',
                    'actual_spread': '',
                    'actual_profit': '',
                    'profit_rate': '',
                    'edgex_fill_gap': '',
                    'lighter_fill_gap': '',
                    'edgex_direction': edgex_direction or '',
                    'lighter_direction': lighter_direction or '',
                    'calculated': exchange,
                }
                self.logger.info(
                    f"📊 风控拦截记录 CSV [client_order_id: {client_order_id}]: {exchange}"
                )
            else:
                self.logger.warning(
                    f"Skipping arbitrage CSV: unknown exchange={exchange!r} (expected discovery|edgex|lighter|含「风控拦截」)"
                )

            if record is None:
                return

            try:
                if not self.arbitrage_csv_file or not self.arbitrage_csv_writer:
                    self._initialize_arbitrage_csv_file()
                self.arbitrage_csv_writer.writerow([
                    record['client_order_id'],
                    record['discovery_time'],
                    record['edgex_discovery_price'],
                    record['lighter_discovery_price'],
                    record['discovery_spread'],
                    record['edgex_fill_time'],
                    record['edgex_fill_price'],
                    record['edgex_fill_amount'],
                    record['edgex_fill_quantity'],
                    record['lighter_fill_time'],
                    record['lighter_fill_price'],
                    record['lighter_fill_amount'],
                    record['lighter_fill_quantity'],
                    record['actual_spread'],
                    record.get('actual_profit', ''),
                    record.get('profit_rate', ''),
                    record.get('edgex_fill_gap', ''),
                    record.get('lighter_fill_gap', ''),
                    record.get('edgex_direction', ''),
                    record.get('lighter_direction', ''),
                    record.get('calculated', '')
                ])
                self.arbitrage_csv_file.flush()
            except Exception as e:
                self.logger.error(f"Error writing arbitrage record to CSV: {e}")


    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
        """Log trade details to CSV file."""
        timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

        with open(self.csv_filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                exchange,
                timestamp,
                side,
                price,
                quantity
            ])

        self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price}")

    def log_bbo_to_csv(self, maker_bid: Decimal, maker_ask: Decimal, lighter_bid: Decimal,
                       lighter_ask: Decimal, long_maker: bool, short_maker: bool,
                       long_maker_threshold: Decimal, short_maker_threshold: Decimal):
        """Log BBO data to CSV file using buffered writes."""
        if not self.bbo_csv_file or not self.bbo_csv_writer:
            # Fallback: reinitialize if file handle is lost
            self._initialize_bbo_csv_file()

        timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

        # Calculate spreads
        long_maker_spread = (lighter_bid - maker_bid
                             if lighter_bid and lighter_bid > 0 and maker_bid > 0
                             else Decimal('0'))
        short_maker_spread = (maker_ask - lighter_ask
                              if maker_ask > 0 and lighter_ask and lighter_ask > 0
                              else Decimal('0'))

        try:
            self.bbo_csv_writer.writerow([
                timestamp,
                float(maker_bid),
                float(maker_ask),
                float(lighter_bid) if lighter_bid and lighter_bid > 0 else 0.0,
                float(lighter_ask) if lighter_ask and lighter_ask > 0 else 0.0,
                float(long_maker_spread),
                float(short_maker_spread),
                long_maker,
                short_maker,
                float(long_maker_threshold),
                float(short_maker_threshold)
            ])

            # Increment counter and flush periodically
            self.bbo_write_counter += 1
            if self.bbo_write_counter >= self.bbo_flush_interval:
                self.bbo_csv_file.flush()
                self.bbo_write_counter = 0
        except Exception as e:
            self.logger.error(f"Error writing to BBO CSV: {e}")
            # Try to reinitialize on error
            try:
                if self.bbo_csv_file:
                    self.bbo_csv_file.close()
            except Exception:
                pass
            self._initialize_bbo_csv_file()

    def _cleanup_expired_records(self):
        """清理1小时之前的记录。"""
        with self.arbitrage_lock:
            current_time = datetime.now()
            expired_ids = []
            
            for client_order_id, record in self.pending_arbitrage_records.items():
                # 获取记录的最早时间（discovery_time 或最早的 fill_time）
                discovery_time_str = record.get('discovery_time', '')
                edgex_fill_time_str = record.get('edgex_fill_time', '')
                lighter_fill_time_str = record.get('lighter_fill_time', '')
                
                # 找到最早的时间作为记录时间
                time_str = discovery_time_str
                if not time_str or (edgex_fill_time_str and (not time_str or edgex_fill_time_str < time_str)):
                    time_str = edgex_fill_time_str
                if not time_str or (lighter_fill_time_str and (not time_str or lighter_fill_time_str < time_str)):
                    time_str = lighter_fill_time_str
                
                if time_str:
                    try:
                        record_dt = datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S.%f')
                        elapsed_seconds = (current_time - record_dt).total_seconds()
                        
                        # 如果超过1小时，标记为过期
                        if elapsed_seconds > self.record_timeout:
                            expired_ids.append(client_order_id)
                    except (ValueError, AttributeError):
                        # 时间解析失败，跳过
                        continue
            
            # 删除过期记录
            for client_order_id in expired_ids:
                del self.pending_arbitrage_records[client_order_id]
            
            if expired_ids:
                self.logger.info(f"🧹 Cleaned up {len(expired_ids)} records older than 1 hour, remaining: {len(self.pending_arbitrage_records)}")

    def _start_cleanup_thread(self):
        """启动定期清理线程。"""
        def cleanup_worker():
            while True:
                try:
                    time.sleep(self.cleanup_interval)
                    self._cleanup_expired_records()
                except Exception as e:
                    self.logger.error(f"Error in cleanup thread: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True, name="arbitrage_cleanup")
        cleanup_thread.start()
        self.logger.info(f"🧹 Started cleanup thread: clears records older than 1 hour every {self.cleanup_interval // 60} minutes")

    def _start_calculation_thread(self):
        """启动后台计算线程，计算actual_spread, actual_profit, profit_rate, fill_gap等字段。"""
        def calculation_worker():
            # 等待初始数据写入
            time.sleep(10)
            while True:
                try:
                    self._calculate_arbitrage_metrics()
                except Exception as e:
                    self.logger.error(f"Error in calculation thread: {e}")
                time.sleep(self.calculation_interval)
        
        calc_thread = threading.Thread(target=calculation_worker, daemon=True, name="arbitrage_calculation")
        calc_thread.start()
        self.logger.info(f"🧮 Started calculation thread: calculates metrics every {self.calculation_interval} seconds")

    def _calculate_arbitrage_metrics(self):
        """从CSV读取数据，从后往前匹配client_order_id，计算actual_spread, actual_profit, profit_rate, fill_gap。"""
        with self.arbitrage_lock:
            if not os.path.exists(self.arbitrage_csv_filename):
                return
            
            # 先关闭当前的文件句柄，以便安全读取
            if self.arbitrage_csv_file:
                try:
                    self.arbitrage_csv_file.flush()
                except Exception:
                    pass
            
            # 读取CSV到内存
            rows = []
            try:
                with open(self.arbitrage_csv_filename, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
            except Exception as e:
                self.logger.error(f"Error reading arbitrage CSV: {e}")
                return
            
            if not rows:
                return
            
            # 从后往前扫描，按client_order_id分组
            # 只处理最近的记录（最多1000条）
            max_scan = min(len(rows), 1000)
            groups = {}
            
            for i in range(len(rows) - 1, len(rows) - max_scan - 1, -1):
                if i < 0:
                    break
                row = rows[i]
                client_order_id = row.get('client_order_id', '')
                if not client_order_id:
                    continue
                
                if client_order_id not in groups:
                    groups[client_order_id] = {
                        'discovery': None,
                        'discovery_idx': None,
                        'edgex': None,
                        'lighter': None
                    }
                
                # 判断行类型
                if row.get('discovery_time') and row.get('edgex_discovery_price'):
                    groups[client_order_id]['discovery'] = row
                    groups[client_order_id]['discovery_idx'] = i
                elif row.get('edgex_fill_time') and row.get('edgex_fill_price'):
                    groups[client_order_id]['edgex'] = row
                elif row.get('lighter_fill_time') and row.get('lighter_fill_price'):
                    groups[client_order_id]['lighter'] = row
            
            # 对完整的组进行计算，只更新discovery行
            updated = False
            for client_order_id, group in groups.items():
                discovery = group['discovery']
                discovery_idx = group['discovery_idx']
                edgex = group['edgex']
                lighter = group['lighter']
                
                # 需要同时有discovery、edgex、lighter才能计算
                if not (discovery and edgex and lighter and discovery_idx is not None):
                    continue
                
                # 跳过已计算的记录（使用calculated标识）
                if discovery.get('calculated') == '1':
                    continue
                
                try:
                    # 获取价格和数量
                    edgex_price = float(edgex['edgex_fill_price'])
                    lighter_price = float(lighter['lighter_fill_price'])
                    edgex_quantity = float(edgex['edgex_fill_quantity'])
                    lighter_quantity = float(lighter['lighter_fill_quantity'])
                    
                    # 获取方向
                    edgex_direction = discovery.get('edgex_direction', '') or edgex.get('edgex_direction', '')
                    
                    # 计算actual_spread
                    if edgex_direction == 'sell':
                        # edgex卖出(获得edgex_price), lighter买入(支付lighter_price)
                        actual_spread = (edgex_price - lighter_price) / lighter_price
                    else:
                        # edgex买入(支付edgex_price), lighter卖出(获得lighter_price)
                        actual_spread = (lighter_price - edgex_price) / edgex_price
                    
                    # 计算actual_profit (spread * quantity)
                    quantity = min(edgex_quantity, lighter_quantity)
                    actual_profit = actual_spread * quantity
                    
                    # 计算profit_rate (profit / cost * 100)
                    cost = lighter_price * quantity if edgex_direction == 'sell' else edgex_price * quantity
                    profit_rate = (actual_profit / cost * 100) if cost > 0 else 0
                    
                    # 计算fill_gap (fill_time - discovery_time)
                    discovery_time = discovery.get('discovery_time', '')
                    edgex_fill_gap = self._calculate_time_gap(discovery_time, edgex.get('edgex_fill_time', ''))
                    lighter_fill_gap = self._calculate_time_gap(discovery_time, lighter.get('lighter_fill_time', ''))
                    
                    # 只更新discovery行，合并edgex和lighter的fill数据
                    rows[discovery_idx]['edgex_fill_time'] = edgex.get('edgex_fill_time', '')
                    rows[discovery_idx]['edgex_fill_price'] = edgex.get('edgex_fill_price', '')
                    rows[discovery_idx]['edgex_fill_amount'] = edgex.get('edgex_fill_amount', '')
                    rows[discovery_idx]['edgex_fill_quantity'] = edgex.get('edgex_fill_quantity', '')
                    rows[discovery_idx]['lighter_fill_time'] = lighter.get('lighter_fill_time', '')
                    rows[discovery_idx]['lighter_fill_price'] = lighter.get('lighter_fill_price', '')
                    rows[discovery_idx]['lighter_fill_amount'] = lighter.get('lighter_fill_amount', '')
                    rows[discovery_idx]['lighter_fill_quantity'] = lighter.get('lighter_fill_quantity', '')
                    rows[discovery_idx]['actual_spread'] = f"{actual_spread:.6f}"
                    rows[discovery_idx]['actual_profit'] = f"{actual_profit:.6f}"
                    rows[discovery_idx]['profit_rate'] = f"{profit_rate:.4f}%"
                    rows[discovery_idx]['edgex_fill_gap'] = f"{edgex_fill_gap:.3f}" if isinstance(edgex_fill_gap, float) else ''
                    rows[discovery_idx]['lighter_fill_gap'] = f"{lighter_fill_gap:.3f}" if isinstance(lighter_fill_gap, float) else ''
                    rows[discovery_idx]['calculated'] = '1'
                    
                    updated = True
                    self.logger.info(
                        f"🧮 Calculated metrics for {client_order_id}: "
                        f"spread={actual_spread:.6f}, profit={actual_profit:.6f}, rate={profit_rate:.4f}%, "
                        f"edgex_gap={edgex_fill_gap}, lighter_gap={lighter_fill_gap}"
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to calculate metrics for {client_order_id}: {e}")
            
            # 如果有更新，写回CSV
            if updated:
                try:
                    # 先写入临时文件，再重命名
                    temp_filename = self.arbitrage_csv_filename + '.tmp'
                    fieldnames = [
                        'client_order_id', 'discovery_time', 'edgex_discovery_price', 'lighter_discovery_price',
                        'discovery_spread', 'edgex_fill_time', 'edgex_fill_price', 'edgex_fill_amount',
                        'edgex_fill_quantity', 'lighter_fill_time', 'lighter_fill_price', 'lighter_fill_amount',
                        'lighter_fill_quantity', 'actual_spread', 'actual_profit', 'profit_rate',
                        'edgex_fill_gap', 'lighter_fill_gap', 'edgex_direction', 'lighter_direction', 'calculated'
                    ]
                    with open(temp_filename, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                        writer.writeheader()
                        writer.writerows(rows)
                    os.replace(temp_filename, self.arbitrage_csv_filename)
                    
                    # 重新打开文件句柄
                    if self.arbitrage_csv_file:
                        try:
                            self.arbitrage_csv_file.close()
                        except Exception:
                            pass
                    self.arbitrage_csv_file = open(self.arbitrage_csv_filename, 'a', newline='', buffering=8192)
                    self.arbitrage_csv_writer = csv.writer(self.arbitrage_csv_file)
                    
                    self.logger.info(f"🧮 Updated arbitrage CSV with calculated metrics")
                except Exception as e:
                    self.logger.error(f"Error writing updated arbitrage CSV: {e}")

    def close(self):
        """Close file handles and thread pool."""
        # 关闭线程池，等待所有任务完成
        if self.executor:
            try:
                # shutdown(wait=True) 会等待所有任务完成，timeout 参数在 Python 3.9+ 才支持
                # 为了兼容性，使用 wait=True 不指定 timeout
                self.executor.shutdown(wait=True)
                self.logger.info("📊 Log fill thread pool closed")
            except Exception as e:
                self.logger.error(f"Error closing thread pool: {e}")
        
        # 关闭数据库连接
        if hasattr(self, 'arb_info_db') and self.arb_info_db:
            try:
                self.arb_info_db.close()
                self.logger.info("📊 ArbInfoDB closed")
            except Exception as e:
                self.logger.error(f"Error closing ArbInfoDB: {e}")

        if self.bbo_csv_file:
            try:
                self.bbo_csv_file.flush()
                self.bbo_csv_file.close()
                self.bbo_csv_file = None
                self.bbo_csv_writer = None
                self.logger.info("📊 BBO CSV file closed")
            except (ValueError, OSError) as e:
                # File already closed or I/O error - ignore silently
                self.bbo_csv_file = None
                self.bbo_csv_writer = None
            except Exception as e:
                self.logger.error(f"Error closing BBO CSV file: {e}")
                self.bbo_csv_file = None
                self.bbo_csv_writer = None

        if self.arbitrage_csv_file:
            try:
                self.arbitrage_csv_file.flush()
                self.arbitrage_csv_file.close()
                self.arbitrage_csv_file = None
                self.arbitrage_csv_writer = None
                self.logger.info("📊 Arbitrage CSV file closed")
            except (ValueError, OSError) as e:
                self.arbitrage_csv_file = None
                self.arbitrage_csv_writer = None
            except Exception as e:
                self.logger.error(f"Error closing Arbitrage CSV file: {e}")
                self.arbitrage_csv_file = None
                self.arbitrage_csv_writer = None
