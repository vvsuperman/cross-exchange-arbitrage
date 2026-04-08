"""
CSV交易日志记录器模块

提供异步CSV交易日志记录功能，用于记录开仓和平仓操作。
"""

import os
import csv
import threading
import queue
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CSVTradeLogger:
    """异步CSV交易日志记录器"""
    
    def __init__(self, csv_file: str = 'logs/stat_log.csv'):
        """
        初始化CSV日志记录器
        
        参数:
        csv_file: CSV文件路径
        """
        self.csv_file = csv_file
        self.queue = queue.Queue()
        self.running = True
        self.lock = threading.Lock()
        self.file_initialized = False
        
        # 启动后台写入线程
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()
        
        logger.info(f"CSV交易日志记录器已启动，文件: {csv_file}")
    
    def _ensure_directory_exists(self):
        """确保CSV文件所在的目录存在"""
        directory = os.path.dirname(self.csv_file)
        if directory and not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
                logger.info(f"创建目录: {directory}")
            except Exception as e:
                logger.error(f"创建目录失败: {e}")
                raise
    
    def _initialize_csv_file(self):
        """初始化CSV文件，写入表头"""
        if self.file_initialized:
            return
        
        try:
            # 确保目录存在
            self._ensure_directory_exists()
            
            # 检查文件是否存在
            file_exists = os.path.exists(self.csv_file)
            
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    # 写入表头
                    headers = [
                        'timestamp', 'action', 'symbol', 'direction',
                        'entry_spread_pct', 'close_spread_pct',
                        'entry_zscore', 'expected_profit_pct',
                        'entry_edgex_price', 'entry_lighter_price',
                        'close_edgex_price', 'close_lighter_price',
                        'position_size', 'hold_time_seconds',
                        'gross_pnl', 'total_fee', 'net_pnl',
                        'baseline_mean1', 'baseline_std1',
                        'baseline_mean2', 'baseline_std2',
                        'reason', 'client_order_id'
                    ]
                    writer.writerow(headers)
                    logger.info(f"CSV文件已创建，表头已写入: {self.csv_file}")
            
            self.file_initialized = True
        except Exception as e:
            logger.error(f"初始化CSV文件失败: {e}", exc_info=True)
    
    def _worker(self):
        """后台工作线程，负责写入CSV"""
        while self.running:
            try:
                # 从队列获取数据，超时1秒
                record = self.queue.get(timeout=1)
                
                if record is None:  # 停止信号
                    break
                
                # 初始化文件（如果还未初始化）
                self._initialize_csv_file()
                
                # 确保目录存在（防止目录被删除的情况）
                self._ensure_directory_exists()
                
                # 写入CSV
                with self.lock:
                    with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(record)
                
                self.queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"写入CSV失败: {e}", exc_info=True)
                self.queue.task_done()
    
    def log_open(self, symbol: str, direction: str, spread_pct: float, z_score: float,
                 expected_profit_pct: float, edgex_price: float, lighter_price: float,
                 position_size: float, baseline_mean1: float, baseline_std1: float,
                 baseline_mean2: float, baseline_std2: float, reason: str = '', client_order_id: str = ''):
        """
        记录开仓日志
        
        参数:
        symbol: 交易对符号
        direction: 方向（long_spread1/long_spread2）
        spread_pct: 价差百分比
        z_score: Z-score
        expected_profit_pct: 预期利润百分比
        edgex_price: edgex价格
        lighter_price: lighter价格
        position_size: 仓位大小
        baseline_mean1: 基准均值1
        baseline_std1: 基准标准差1
        baseline_mean2: 基准均值2
        baseline_std2: 基准标准差2
        reason: 交易原因
        client_order_id: 客户端订单ID
        """
        timestamp = datetime.now().isoformat()
        record = [
            timestamp, 'open', symbol, direction,
            spread_pct, None,  # close_spread_pct
            z_score, expected_profit_pct,
            edgex_price, lighter_price,
            None, None,  # close_edgex_price, close_lighter_price
            position_size, None,  # hold_time_seconds
            None, None, None,  # gross_pnl, total_fee, net_pnl
            baseline_mean1, baseline_std1,
            baseline_mean2, baseline_std2,
            reason, client_order_id
        ]
        
        try:
            self.queue.put(record, block=False)
        except queue.Full:
            logger.warning("CSV日志队列已满，跳过本次记录")
    
    def log_close(self, symbol: str, direction: str, entry_spread_pct: float,
                  close_spread_pct: float, entry_zscore: float,
                  entry_edgex_price: float, entry_lighter_price: float,
                  close_edgex_price: float, close_lighter_price: float,
                  position_size: float, hold_time_seconds: float,
                  gross_pnl: float, total_fee: float, net_pnl: float,
                  reason: str = ''):
        """
        记录平仓日志
        
        参数:
        symbol: 交易对符号
        direction: 方向（long_spread1/long_spread2）
        entry_spread_pct: 开仓价差百分比
        close_spread_pct: 平仓价差百分比
        entry_zscore: 开仓Z-score
        entry_edgex_price: 开仓edgex价格
        entry_lighter_price: 开仓lighter价格
        close_edgex_price: 平仓edgex价格
        close_lighter_price: 平仓lighter价格
        position_size: 仓位大小
        hold_time_seconds: 持仓时间（秒）
        gross_pnl: 毛利润
        total_fee: 总手续费
        net_pnl: 净利润
        reason: 交易原因
        """
        timestamp = datetime.now().isoformat()
        record = [
            timestamp, 'close', symbol, direction,
            entry_spread_pct, close_spread_pct,
            entry_zscore, None,  # expected_profit_pct
            entry_edgex_price, entry_lighter_price,
            close_edgex_price, close_lighter_price,
            position_size, hold_time_seconds,
            gross_pnl, total_fee, net_pnl,
            None, None,  # baseline_mean1, baseline_std1
            None, None,  # baseline_mean2, baseline_std2
            reason
        ]
        
        try:
            self.queue.put(record, block=False)
        except queue.Full:
            logger.warning("CSV日志队列已满，跳过本次记录")
    
    def close(self):
        """关闭日志记录器"""
        self.running = False
        self.queue.put(None)  # 发送停止信号
        self.worker_thread.join(timeout=5)
        logger.info("CSV交易日志记录器已关闭")
