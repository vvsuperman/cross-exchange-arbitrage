#!/usr/bin/env python3
"""
SQLite3数据库类，用于记录套利信息
"""
import sqlite3
import os
import logging
import shutil
import json
from decimal import Decimal
from typing import Optional
from datetime import datetime
from contextlib import contextmanager
import pytz

# 定义 UTC+8 时区
TZ_UTC8 = pytz.timezone('Asia/Shanghai')

logger = logging.getLogger('db_logger')
logger.setLevel(logging.INFO)


class ArbInfoDB:
    """记录套利信息到SQLite3数据库"""
    
    # info_type 常量
    INFO_TYPE_DISCOVERY = 'discovery'      # simple_stat中刚发现价差
    INFO_TYPE_RECEIVED = 'received'        # edgex_arb中redis接受到价差信号
    INFO_TYPE_EDGEX_FILLED = 'edgex_filled'    # edgex ws里收到订单确认
    INFO_TYPE_LIGHTER_FILLED = 'lighter_filled'  # lighter中收到订单确认
    
    def __init__(self, db_dir: Optional[str] = None):
        """
        初始化数据库记录器
        
        Args:
            db_dir: 数据库文件目录，如果为None则使用项目根目录下的logs目录
        """
        # 确定数据库文件路径
        if db_dir is None:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            db_dir = os.path.join(project_root, "logs")
        
        os.makedirs(db_dir, exist_ok=True)
        self.db_path = os.path.join(db_dir, "arb_info.db")
        self.db_dir = db_dir
        
        # 初始化数据库
        self._initialize_database()
        # 计数器：每500次写入执行一次WAL检查点
        self._write_count = 0
        self._checkpoint_interval = 500
    
    @contextmanager
    def _get_connection(self):
        """上下文管理器，确保数据库连接正确关闭"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            # 启用WAL模式
            conn.execute('PRAGMA journal_mode=WAL')
            # 设置同步模式为NORMAL
            conn.execute('PRAGMA synchronous=NORMAL')
            try:
                conn.execute('PRAGMA page_size=4096')
            except:
                pass
            yield conn
            conn.commit()
        except sqlite3.DatabaseError as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            if "malformed" in str(e).lower():
                logger.warning(f"检测到数据库损坏: {self.db_path}")
                self._recover_database()
                conn = sqlite3.connect(self.db_path, timeout=10.0)
                conn.execute('PRAGMA journal_mode=WAL')
                yield conn
                conn.commit()
            else:
                raise
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
    
    def _check_integrity(self) -> bool:
        """检查数据库完整性"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            cursor = conn.cursor()
            cursor.execute('PRAGMA integrity_check')
            result = cursor.fetchone()
            conn.close()
            return result and result[0] == 'ok'
        except Exception as e:
            logger.error(f"数据库完整性检查失败: {e}")
            return False
    
    def _recover_database(self):
        """恢复损坏的数据库"""
        logger.warning(f"开始恢复数据库: {self.db_path}")
        
        backup_path = f"{self.db_path}.corrupted.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            if os.path.exists(self.db_path):
                shutil.copy2(self.db_path, backup_path)
                logger.info(f"已备份损坏的数据库到: {backup_path}")
            
            wal_path = f"{self.db_path}-wal"
            shm_path = f"{self.db_path}-shm"
            
            if os.path.exists(wal_path):
                try:
                    shutil.copy2(wal_path, f"{backup_path}-wal")
                except:
                    pass
            if os.path.exists(shm_path):
                try:
                    shutil.copy2(shm_path, f"{backup_path}-shm")
                except:
                    pass
            
            if os.path.exists(wal_path):
                try:
                    os.remove(wal_path)
                    logger.info("已删除WAL文件")
                except Exception as e:
                    logger.warning(f"删除WAL文件失败: {e}")
                    
            if os.path.exists(shm_path):
                try:
                    os.remove(shm_path)
                    logger.info("已删除SHM文件")
                except Exception as e:
                    logger.warning(f"删除SHM文件失败: {e}")
            
            if not self._check_integrity():
                logger.warning("数据库无法修复，创建新的空数据库")
                if os.path.exists(self.db_path):
                    try:
                        os.remove(self.db_path)
                    except Exception as e:
                        logger.error(f"删除损坏的数据库文件失败: {e}")
                
        except Exception as e:
            logger.error(f"数据库恢复失败: {e}", exc_info=True)
            try:
                if os.path.exists(self.db_path):
                    os.remove(self.db_path)
                wal_path = f"{self.db_path}-wal"
                shm_path = f"{self.db_path}-shm"
                if os.path.exists(wal_path):
                    try:
                        os.remove(wal_path)
                    except:
                        pass
                if os.path.exists(shm_path):
                    try:
                        os.remove(shm_path)
                    except:
                        pass
            except Exception as e2:
                logger.error(f"清理损坏文件失败: {e2}")
    
    def _checkpoint_wal(self):
        """定期检查点，将WAL文件合并到主数据库"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
            conn.close()
            logger.debug(f"WAL检查点完成: {self.db_path}")
        except Exception as e:
            logger.warning(f"WAL检查点失败: {e}")
    
    def _initialize_database(self):
        """初始化数据库，创建表结构"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # 创建arb_info表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS arb_info (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        info_type TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        client_order_id TEXT,
                        edgex_price REAL,
                        lighter_price REAL,
                        edgex_quantity REAL,
                        lighter_quantity REAL,
                        edgex_direction TEXT,
                        lighter_direction TEXT,
                        time_gap REAL,
                        spread_ratio REAL,
                        actual_spread_ratio REAL,
                        calculated INTEGER DEFAULT 0,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # 创建索引以提高查询性能
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_arb_info_symbol ON arb_info(symbol)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_arb_info_timestamp ON arb_info(timestamp)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_arb_info_info_type ON arb_info(info_type)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_arb_info_client_order_id ON arb_info(client_order_id)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_arb_info_symbol_timestamp ON arb_info(symbol, timestamp)
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS minute_prices (
                        id                INTEGER PRIMARY KEY AUTOINCREMENT,
                        exchange          TEXT    NOT NULL,      -- 交易所，如 "omni", "binance", "hyperliquid"
                        symbol            TEXT    NOT NULL,      -- 交易对，如 "SOL", "ETH", "BTC"
                        ask               REAL    NOT NULL,      -- 卖一价
                        ask_quantity      REAL    NOT NULL,      -- 卖一量（如果有）
                        bid               REAL    NOT NULL,      -- 买一价
                        bid_quantity      REAL    NOT NULL,      -- 买一量（如果有）
                        spread            REAL    NOT NULL,      -- 价差 = ask - bid
                        minute            TEXT    NOT NULL,      -- 分钟时间，如 "2025-12-14 10:23"
                        full_timestamp    TEXT    NOT NULL,      -- 完整采集时间，如 "2025-12-14 10:23:07"
                        
                        -- 复合唯一索引：同一交易所+交易对+分钟 只保留一条
                        UNIQUE(exchange, symbol, minute)
                    )
                ''')
                
                # 为常用查询字段添加索引，提升查询速度
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_exchange_symbol_minute ON minute_prices(exchange, symbol, minute)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_minute ON minute_prices(minute)')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS frontend_price_ticks (
                        id                INTEGER PRIMARY KEY AUTOINCREMENT,
                        exchange          TEXT    NOT NULL,
                        symbol            TEXT    NOT NULL,
                        market            TEXT    NOT NULL,
                        ask               REAL    NOT NULL,
                        ask_quantity      REAL,
                        bid               REAL    NOT NULL,
                        bid_quantity      REAL,
                        spread            REAL    NOT NULL,
                        mid_price         REAL,
                        input_quantity    REAL,
                        page_url          TEXT,
                        captured_at       TEXT    NOT NULL,
                        metadata_json     TEXT,
                        created_at        TEXT    DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_frontend_price_ticks_lookup
                    ON frontend_price_ticks(exchange, symbol, captured_at)
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS frontend_spread_signals (
                        id                INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol            TEXT    NOT NULL,
                        market            TEXT    NOT NULL,
                        buy_exchange      TEXT    NOT NULL,
                        sell_exchange     TEXT    NOT NULL,
                        buy_price         REAL    NOT NULL,
                        sell_price        REAL    NOT NULL,
                        spread_abs        REAL    NOT NULL,
                        spread_bps        REAL,
                        target_quantity   REAL,
                        threshold_abs     REAL,
                        threshold_bps     REAL,
                        triggered         INTEGER NOT NULL DEFAULT 0,
                        observed_at       TEXT    NOT NULL,
                        metadata_json     TEXT,
                        created_at        TEXT    DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_frontend_spread_signals_lookup
                    ON frontend_spread_signals(symbol, observed_at)
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS frontend_order_events (
                        id                INTEGER PRIMARY KEY AUTOINCREMENT,
                        exchange          TEXT    NOT NULL,
                        symbol            TEXT    NOT NULL,
                        market            TEXT    NOT NULL,
                        side              TEXT    NOT NULL,
                        quantity          REAL    NOT NULL,
                        requested_price   REAL,
                        clicked_price     REAL,
                        order_kind        TEXT,
                        status            TEXT    NOT NULL,
                        client_order_id   TEXT,
                        external_order_id TEXT,
                        page_url          TEXT,
                        notes             TEXT,
                        metadata_json     TEXT,
                        created_at        TEXT    NOT NULL,
                        updated_at        TEXT    NOT NULL
                    )
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_frontend_order_events_lookup
                    ON frontend_order_events(exchange, symbol, created_at)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_frontend_order_events_client_order_id
                    ON frontend_order_events(client_order_id)
                ''')
                
                logger.debug(f"数据库初始化成功: {self.db_path}")
        except sqlite3.DatabaseError as e:
            if "malformed" in str(e).lower():
                logger.warning(f"检测到数据库损坏，尝试恢复: {self.db_path}")
                self._recover_database()
                self._initialize_database()
            else:
                logger.error(f"数据库初始化失败: {e}", exc_info=True)
                raise
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}", exc_info=True)
            raise
    
    def log_arb_info(
        self,
        symbol: str,
        info_type: str,
        client_order_id: Optional[str] = None,
        edgex_price: Optional[Decimal] = None,
        lighter_price: Optional[Decimal] = None,
        edgex_quantity: Optional[Decimal] = None,
        lighter_quantity: Optional[Decimal] = None,
        spread_ratio: Optional[Decimal] = None,
        edgex_direction: Optional[str] = None,
        lighter_direction: Optional[str] = None,
        time_gap: Optional[float] = None,
        actual_spread_ratio: Optional[Decimal] = None,
        calculated: int = 0,
        timestamp: Optional[str] = None
    ):
        """
        记录套利信息到数据库
        
        Args:
            symbol: 交易对符号
            info_type: 信息类型 ('discovery', 'received', 'edgex_filled', 'lighter_filled')
            client_order_id: 客户订单ID
            edgex_price: EdgeX价格
            lighter_price: Lighter价格
            edgex_quantity: EdgeX数量
            lighter_quantity: Lighter数量
            spread_ratio: 价差比率
            edgex_direction: EdgeX方向 ('buy' 或 'sell')
            lighter_direction: Lighter方向 ('buy' 或 'sell')
            time_gap: 成交时间差
            actual_spread_ratio: 实际价差比率
            calculated: 是否已计算 (0=未计算, 1=已计算)
            timestamp: 时间戳，如果为None则使用当前时间
        """
        if timestamp is None:
            timestamp = datetime.now(TZ_UTC8).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO arb_info (
                        symbol,
                        info_type,
                        timestamp,
                        client_order_id,
                        edgex_price,
                        lighter_price,
                        edgex_quantity,
                        lighter_quantity,
                        spread_ratio,
                        edgex_direction,
                        lighter_direction,
                        time_gap,
                        actual_spread_ratio,
                        calculated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol,
                    info_type,
                    timestamp,
                    client_order_id,
                    float(edgex_price) if edgex_price is not None else None,
                    float(lighter_price) if lighter_price is not None else None,
                    float(edgex_quantity) if edgex_quantity is not None else None,
                    float(lighter_quantity) if lighter_quantity is not None else None,
                    float(spread_ratio) if spread_ratio is not None else None,
                    edgex_direction,
                    lighter_direction,
                    time_gap,
                    float(actual_spread_ratio) if actual_spread_ratio is not None else None,
                    calculated
                ))
            
            # 定期执行WAL检查点
            self._write_count += 1
            if self._write_count >= self._checkpoint_interval:
                self._checkpoint_wal()
                self._write_count = 0
                
        except sqlite3.DatabaseError as e:
            if "malformed" in str(e).lower():
                logger.warning(f"写入时检测到数据库损坏，尝试恢复: {self.db_path}")
                self._recover_database()
                # 恢复后重试一次
                try:
                    with self._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO arb_info (
                                symbol,
                                info_type,
                                timestamp,
                                client_order_id,
                                edgex_price,
                                lighter_price,
                                edgex_quantity,
                                lighter_quantity,
                                spread_ratio,
                                edgex_direction,
                                lighter_direction,
                                time_gap,
                                actual_spread_ratio,
                                calculated
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            symbol,
                            info_type,
                            timestamp,
                            client_order_id,
                            float(edgex_price) if edgex_price is not None else None,
                            float(lighter_price) if lighter_price is not None else None,
                            float(edgex_quantity) if edgex_quantity is not None else None,
                            float(lighter_quantity) if lighter_quantity is not None else None,
                            float(spread_ratio) if spread_ratio is not None else None,
                            edgex_direction,
                            lighter_direction,
                            time_gap,
                            float(actual_spread_ratio) if actual_spread_ratio is not None else None,
                            calculated
                        ))
                except Exception as e2:
                    logger.error(f"恢复后重试写入失败: {e2}", exc_info=True)
                    raise
            else:
                logger.error(f"写入数据库失败: {e}", exc_info=True)
                raise
        except Exception as e:
            logger.error(f"写入数据库失败: {e}", exc_info=True)
            raise
    
    def query_data(
        self,
        symbol: Optional[str] = None,
        info_type: Optional[str] = None,
        client_order_id: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None
    ) -> list:
        """
        查询套利信息
        
        Args:
            symbol: 交易对符号
            info_type: 信息类型
            client_order_id: 客户订单ID
            start_time: 开始时间（ISO格式字符串）
            end_time: 结束时间（ISO格式字符串）
            limit: 返回记录数限制
        
        Returns:
            查询结果列表
        """
        try:
            with self._get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                query = "SELECT * FROM arb_info WHERE 1=1"
                params = []
                
                if symbol:
                    query += " AND symbol = ?"
                    params.append(symbol)
                
                if info_type:
                    query += " AND info_type = ?"
                    params.append(info_type)
                
                if client_order_id:
                    query += " AND client_order_id = ?"
                    params.append(client_order_id)
                
                if start_time:
                    query += " AND timestamp >= ?"
                    params.append(start_time)
                
                if end_time:
                    query += " AND timestamp <= ?"
                    params.append(end_time)
                
                query += " ORDER BY timestamp DESC"
                
                if limit:
                    query += " LIMIT ?"
                    params.append(limit)
                
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"查询数据库失败: {e}", exc_info=True)
            return []
    
    def get_count(self, symbol: Optional[str] = None, info_type: Optional[str] = None) -> int:
        """获取记录数"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                query = "SELECT COUNT(*) FROM arb_info WHERE 1=1"
                params = []
                
                if symbol:
                    query += " AND symbol = ?"
                    params.append(symbol)
                
                if info_type:
                    query += " AND info_type = ?"
                    params.append(info_type)
                
                cursor.execute(query, params)
                count = cursor.fetchone()[0]
                return count
        except Exception as e:
            logger.error(f"获取记录数失败: {e}", exc_info=True)
            return 0
    
    def update_record(self, record_id: int, time_gap: Optional[float] = None, 
                       actual_spread_ratio: Optional[float] = None, calculated: int = 1):
        """
        更新单条记录
        
        Args:
            record_id: 记录ID
            time_gap: 时间差
            actual_spread_ratio: 实际价差比率
            calculated: 是否已计算
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                updates = []
                params = []
                
                if time_gap is not None:
                    updates.append("time_gap = ?")
                    params.append(time_gap)
                
                if actual_spread_ratio is not None:
                    updates.append("actual_spread_ratio = ?")
                    params.append(actual_spread_ratio)
                
                updates.append("calculated = ?")
                params.append(calculated)
                
                params.append(record_id)
                
                query = f"UPDATE arb_info SET {', '.join(updates)} WHERE id = ?"
                cursor.execute(query, params)
                
        except Exception as e:
            logger.error(f"更新记录失败: {e}", exc_info=True)
            raise
    
    def get_uncalculated_client_order_ids(self, limit: int = 100) -> list:
        """
        获取未计算的client_order_id列表（按时间倒序）
        
        Args:
            limit: 最大返回数量
        
        Returns:
            未计算的client_order_id列表
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # 查找有未计算记录的client_order_id
                cursor.execute('''
                    SELECT DISTINCT client_order_id 
                    FROM arb_info 
                    WHERE calculated = 0 AND client_order_id IS NOT NULL
                    ORDER BY timestamp DESC
                    LIMIT ?
                ''', (limit,))
                
                results = cursor.fetchall()
                return [row[0] for row in results]
        except Exception as e:
            logger.error(f"获取未计算client_order_id失败: {e}", exc_info=True)
            return []
    
    def get_records_by_client_order_id(self, client_order_id: str) -> list:
        """
        根据client_order_id获取所有相关记录
        
        Args:
            client_order_id: 客户订单ID
        
        Returns:
            记录列表
        """
        try:
            with self._get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM arb_info 
                    WHERE client_order_id = ?
                    ORDER BY timestamp ASC
                ''', (client_order_id,))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"获取记录失败: {e}", exc_info=True)
            return []

    def save_minute_price(self,
        exchange: str,
        symbol: str,
        ask: float,
        ask_quantity: float,
        bid: float,
        bid_quantity: float,
        spread: float,
        minute_str: str,
        full_timestamp_str: str
    ):
        """
        保存或更新一条分钟级价格数据
        使用 INSERT OR REPLACE 确保同一分钟只保留最新一条
        """
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                INSERT OR REPLACE INTO minute_prices 
                (exchange, symbol, ask, ask_quantity, bid, bid_quantity, spread, minute, full_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (exchange, symbol, ask, ask_quantity, bid, bid_quantity, spread, minute_str, full_timestamp_str))

    def save_frontend_price_tick(
        self,
        exchange: str,
        symbol: str,
        market: str,
        ask: float,
        bid: float,
        spread: float,
        captured_at: str,
        ask_quantity: Optional[float] = None,
        bid_quantity: Optional[float] = None,
        mid_price: Optional[float] = None,
        input_quantity: Optional[float] = None,
        page_url: Optional[str] = None,
        metadata: Optional[dict] = None,
    ):
        """保存前端轮询得到的价格快照。"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO frontend_price_ticks
                (exchange, symbol, market, ask, ask_quantity, bid, bid_quantity, spread,
                 mid_price, input_quantity, page_url, captured_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                exchange,
                symbol,
                market,
                ask,
                ask_quantity,
                bid,
                bid_quantity,
                spread,
                mid_price,
                input_quantity,
                page_url,
                captured_at,
                json.dumps(metadata, ensure_ascii=True) if metadata else None,
            ))

    def save_frontend_spread_signal(
        self,
        symbol: str,
        market: str,
        buy_exchange: str,
        sell_exchange: str,
        buy_price: float,
        sell_price: float,
        spread_abs: float,
        observed_at: str,
        spread_bps: Optional[float] = None,
        target_quantity: Optional[float] = None,
        threshold_abs: Optional[float] = None,
        threshold_bps: Optional[float] = None,
        triggered: bool = False,
        metadata: Optional[dict] = None,
    ):
        """记录跨交易所价差信号。"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO frontend_spread_signals
                (symbol, market, buy_exchange, sell_exchange, buy_price, sell_price,
                 spread_abs, spread_bps, target_quantity, threshold_abs, threshold_bps,
                 triggered, observed_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                symbol,
                market,
                buy_exchange,
                sell_exchange,
                buy_price,
                sell_price,
                spread_abs,
                spread_bps,
                target_quantity,
                threshold_abs,
                threshold_bps,
                1 if triggered else 0,
                observed_at,
                json.dumps(metadata, ensure_ascii=True) if metadata else None,
            ))

    def save_frontend_order_event(
        self,
        exchange: str,
        symbol: str,
        market: str,
        side: str,
        quantity: float,
        status: str,
        created_at: str,
        requested_price: Optional[float] = None,
        clicked_price: Optional[float] = None,
        order_kind: Optional[str] = None,
        client_order_id: Optional[str] = None,
        external_order_id: Optional[str] = None,
        page_url: Optional[str] = None,
        notes: Optional[str] = None,
        metadata: Optional[dict] = None,
        updated_at: Optional[str] = None,
    ):
        """记录前端下单动作和结果。"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO frontend_order_events
                (exchange, symbol, market, side, quantity, requested_price, clicked_price,
                 order_kind, status, client_order_id, external_order_id, page_url, notes,
                 metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                exchange,
                symbol,
                market,
                side,
                quantity,
                requested_price,
                clicked_price,
                order_kind,
                status,
                client_order_id,
                external_order_id,
                page_url,
                notes,
                json.dumps(metadata, ensure_ascii=True) if metadata else None,
                created_at,
                updated_at or created_at,
            ))
    
    def close(self):
        """关闭数据库连接并执行最后的WAL检查点"""
        try:
            self._checkpoint_wal()
            logger.debug(f"数据库连接已关闭: {self.db_path}")
        except Exception as e:
            logger.warning(f"关闭数据库连接时出错: {e}")


# 全局单例实例（可选，用于跨模块共享）
_arb_info_db_instance: Optional[ArbInfoDB] = None


def get_arb_info_db() -> ArbInfoDB:
    """获取全局ArbInfoDB单例实例"""
    global _arb_info_db_instance
    if _arb_info_db_instance is None:
        _arb_info_db_instance = ArbInfoDB()
    return _arb_info_db_instance
