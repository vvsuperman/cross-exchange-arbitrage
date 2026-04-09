#!/usr/bin/env python3
"""
SQLite3数据库类，用于记录BBO数据
每个交易所使用独立的数据库
"""
import logging
import os
import shutil
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Optional

import pytz

# 定义 UTC+8 时区
TZ_UTC8 = pytz.timezone("Asia/Shanghai")

logger = logging.getLogger("record_db")
logger.setLevel(logging.INFO)

DEFAULT_BBO_DB_NAME = "exchange_bbo.db"


class BBODataDB:
    """记录单个交易所的BBO数据到SQLite3数据库"""

    def __init__(self, exchange: str, ticker: str, db_dir: Optional[str] = None):
        self.exchange = exchange.lower()
        self.ticker = ticker

        if db_dir is None:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            db_dir = os.path.join(project_root, "logs")

        os.makedirs(db_dir, exist_ok=True)
        self.db_path = os.path.join(db_dir, DEFAULT_BBO_DB_NAME)
        self.db_dir = db_dir

        self._initialize_database()
        self._write_count = 0
        self._checkpoint_interval = 500

    @contextmanager
    def _get_connection(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            try:
                conn.execute("PRAGMA page_size=4096")
            except Exception:
                pass
            yield conn
            conn.commit()
        except sqlite3.DatabaseError as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if "malformed" in str(e).lower():
                logger.warning(f"检测到数据库损坏: {self.db_path}")
                self._recover_database()
                conn = sqlite3.connect(self.db_path, timeout=10.0)
                conn.execute("PRAGMA journal_mode=WAL")
                yield conn
                conn.commit()
            else:
                raise
        except Exception:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _check_integrity(self) -> bool:
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            conn.close()
            return result and result[0] == "ok"
        except Exception as e:
            logger.error(f"数据库完整性检查失败: {e}")
            return False

    def _recover_database(self):
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
                except Exception:
                    pass
            if os.path.exists(shm_path):
                try:
                    shutil.copy2(shm_path, f"{backup_path}-shm")
                except Exception:
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
                    except Exception:
                        pass
                if os.path.exists(shm_path):
                    try:
                        os.remove(shm_path)
                    except Exception:
                        pass
            except Exception as e2:
                logger.error(f"清理损坏文件失败: {e2}")

    def _checkpoint_wal(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            conn.close()
            logger.debug(f"WAL检查点完成: {self.db_path}")
        except Exception as e:
            logger.warning(f"WAL检查点失败: {e}")

    def _ensure_bbo_exchange_column(self, cursor: sqlite3.Cursor) -> None:
        cursor.execute("PRAGMA table_info(bbo_data)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if "exchange" not in existing_columns:
            cursor.execute("ALTER TABLE bbo_data ADD COLUMN exchange TEXT")
        cursor.execute(
            "UPDATE bbo_data SET exchange = ? WHERE exchange IS NULL OR exchange = ''",
            (self.exchange,),
        )

    def _initialize_database(self):
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS bbo_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        exchange TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        best_bid REAL,
                        best_bid_size REAL,
                        best_ask REAL,
                        best_ask_size REAL,
                        price_timestamp TEXT,
                        timestamp TEXT NOT NULL,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                self._ensure_bbo_exchange_column(cursor)

                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_symbol ON bbo_data(symbol)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_exchange ON bbo_data(exchange)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_timestamp ON bbo_data(timestamp)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_symbol_timestamp ON bbo_data(symbol, timestamp)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_exchange_symbol_timestamp
                    ON bbo_data(exchange, symbol, timestamp)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_price_timestamp ON bbo_data(price_timestamp)
                    """
                )

                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS spread_signal (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        exchange_pair TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        expected_profit_pct REAL,
                        current_z REAL,
                        log_spread REAL,
                        spread_pct REAL,
                        direction TEXT,
                        edgex_price REAL,
                        lighter_price REAL,
                        client_order_id TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )

                try:
                    cursor.execute("ALTER TABLE spread_signal ADD COLUMN client_order_id TEXT")
                except sqlite3.OperationalError:
                    pass

                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_signal_symbol ON spread_signal(symbol)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_signal_timestamp ON spread_signal(timestamp)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_signal_symbol_timestamp
                    ON spread_signal(symbol, timestamp)
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_signal_exchange_pair
                    ON spread_signal(exchange_pair)
                    """
                )

                logger.debug(f"数据库初始化成功: {self.db_path} (交易所: {self.exchange})")
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

    def log_bbo_data(
        self,
        best_bid: Optional[Decimal],
        best_bid_size: Optional[Decimal],
        best_ask: Optional[Decimal],
        best_ask_size: Optional[Decimal],
        price_timestamp: Optional[str],
        timestamp: Optional[str] = None,
        symbol: Optional[str] = None,
    ):
        if timestamp is None:
            timestamp = datetime.now(TZ_UTC8).replace(microsecond=0, tzinfo=None).isoformat()

        if symbol is None:
            symbol = self.ticker

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO bbo_data (
                        exchange,
                        symbol,
                        best_bid,
                        best_bid_size,
                        best_ask,
                        best_ask_size,
                        price_timestamp,
                        timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.exchange,
                        symbol,
                        float(best_bid) if best_bid is not None else None,
                        float(best_bid_size) if best_bid_size is not None else None,
                        float(best_ask) if best_ask is not None else None,
                        float(best_ask_size) if best_ask_size is not None else None,
                        price_timestamp if price_timestamp else None,
                        timestamp,
                    ),
                )

            self._write_count += 1
            if self._write_count >= self._checkpoint_interval:
                self._checkpoint_wal()
                self._write_count = 0

        except sqlite3.DatabaseError as e:
            if "malformed" in str(e).lower():
                logger.warning(f"写入时检测到数据库损坏，尝试恢复: {self.db_path}")
                self._recover_database()
                try:
                    with self._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            """
                            INSERT INTO bbo_data (
                                exchange,
                                symbol,
                                best_bid,
                                best_bid_size,
                                best_ask,
                                best_ask_size,
                                price_timestamp,
                                timestamp
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                self.exchange,
                                symbol,
                                float(best_bid) if best_bid is not None else None,
                                float(best_bid_size) if best_bid_size is not None else None,
                                float(best_ask) if best_ask is not None else None,
                                float(best_ask_size) if best_ask_size is not None else None,
                                price_timestamp if price_timestamp else None,
                                timestamp,
                            ),
                        )
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
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
        symbol: Optional[str] = None,
    ) -> list:
        try:
            with self._get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                query = "SELECT * FROM bbo_data WHERE 1=1"
                params = []

                if symbol:
                    query += " AND symbol = ?"
                    params.append(symbol)

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
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"查询数据库失败: {e}", exc_info=True)
            return []

    def get_count(self) -> int:
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM bbo_data")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"获取记录数失败: {e}", exc_info=True)
            return 0

    def log_spread_signal(
        self,
        exchange_pair: str,
        symbol: str,
        timestamp: str,
        expected_profit_pct: float,
        current_z: float,
        log_spread: Optional[float] = None,
        spread_pct: Optional[float] = None,
        direction: Optional[str] = None,
        edgex_price: Optional[float] = None,
        lighter_price: Optional[float] = None,
        client_order_id: Optional[str] = None,
    ):
        if timestamp is None:
            timestamp = datetime.now(TZ_UTC8).replace(microsecond=0, tzinfo=None).isoformat()

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO spread_signal (
                        exchange_pair,
                        symbol,
                        timestamp,
                        expected_profit_pct,
                        current_z,
                        log_spread,
                        spread_pct,
                        direction,
                        edgex_price,
                        lighter_price,
                        client_order_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        exchange_pair,
                        symbol,
                        timestamp,
                        expected_profit_pct,
                        current_z,
                        log_spread,
                        spread_pct,
                        direction,
                        edgex_price,
                        lighter_price,
                        client_order_id,
                    ),
                )

            self._write_count += 1
            if self._write_count >= self._checkpoint_interval:
                self._checkpoint_wal()
                self._write_count = 0

        except sqlite3.DatabaseError as e:
            if "malformed" in str(e).lower():
                logger.warning(f"写入信号时检测到数据库损坏，尝试恢复: {self.db_path}")
                self._recover_database()
                try:
                    with self._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            """
                            INSERT INTO spread_signal (
                                exchange_pair,
                                symbol,
                                timestamp,
                                expected_profit_pct,
                                current_z,
                                log_spread,
                                spread_pct,
                                direction,
                                edgex_price,
                                lighter_price,
                                client_order_id
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                exchange_pair,
                                symbol,
                                timestamp,
                                expected_profit_pct,
                                current_z,
                                log_spread,
                                spread_pct,
                                direction,
                                edgex_price,
                                lighter_price,
                                client_order_id,
                            ),
                        )
                except Exception as e2:
                    logger.error(f"恢复后重试写入信号失败: {e2}", exc_info=True)
                    raise
            else:
                logger.error(f"写入信号数据库失败: {e}", exc_info=True)
                raise
        except Exception as e:
            logger.error(f"写入信号数据库失败: {e}", exc_info=True)
            raise
