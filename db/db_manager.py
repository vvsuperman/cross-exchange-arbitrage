"""SQLite storage for Omni frontend order and position captures."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Optional


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT.parent / "data"
DEFAULT_DB_PATH = DATA_DIR / "omni_listener.db"


class OmniListenerDB:
    """Persist key Omni order and position fields into a dedicated SQLite DB."""

    def __init__(self, db_path: Optional[str] = None):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.db_path = str(Path(db_path).expanduser() if db_path else DEFAULT_DB_PATH)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize(self) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS omni_order_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_order_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    order_id TEXT,
                    order_type TEXT,
                    status TEXT,
                    created_at TEXT,
                    instrument_underlying TEXT,
                    is_reduce_only INTEGER,
                    raw_url TEXT,
                    raw_payload_json TEXT,
                    logged_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_omni_order_logs_client_order_id
                ON omni_order_logs(client_order_id)
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS omni_position_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_order_id TEXT,
                    symbol TEXT NOT NULL,
                    qty TEXT,
                    avg_entry_price TEXT,
                    updated_at TEXT,
                    opened_at TEXT,
                    instrument_underlying TEXT,
                    value TEXT,
                    upnl TEXT,
                    rpnl TEXT,
                    raw_url TEXT,
                    raw_payload_json TEXT,
                    logged_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_omni_position_logs_client_order_id
                ON omni_position_logs(client_order_id)
                """
            )

    def save_order_log(
        self,
        *,
        client_order_id: str,
        symbol: str,
        side: str,
        quantity: float,
        order: dict[str, Any] | None,
        raw_url: str | None,
        raw_payload: Any,
        logged_at: str,
    ) -> None:
        order = order or {}
        instrument = order.get("instrument") or {}
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO omni_order_logs (
                    client_order_id, symbol, side, quantity, order_id, order_type, status,
                    created_at, instrument_underlying, is_reduce_only, raw_url,
                    raw_payload_json, logged_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_order_id,
                    symbol,
                    side,
                    quantity,
                    order.get("order_id"),
                    order.get("order_type"),
                    order.get("status"),
                    order.get("created_at"),
                    instrument.get("underlying"),
                    1 if order.get("is_reduce_only") else 0 if "is_reduce_only" in order else None,
                    raw_url,
                    json.dumps(raw_payload, ensure_ascii=True) if raw_payload is not None else None,
                    logged_at,
                ),
            )

    def save_position_snapshot(
        self,
        *,
        client_order_id: str,
        raw_url: str | None,
        raw_payload: Any,
        logged_at: str,
        fallback_symbol: str | None = None,
    ) -> None:
        positions = raw_payload if isinstance(raw_payload, list) else []
        payload_json = json.dumps(raw_payload, ensure_ascii=True) if raw_payload is not None else None
        with self._connect() as conn:
            for position in positions:
                if not isinstance(position, dict):
                    continue
                info = position.get("position_info") or {}
                instrument = info.get("instrument") or {}
                symbol = instrument.get("underlying") or fallback_symbol or "UNKNOWN"
                conn.execute(
                    """
                    INSERT INTO omni_position_logs (
                        client_order_id, symbol, qty, avg_entry_price, updated_at, opened_at,
                        instrument_underlying, value, upnl, rpnl, raw_url, raw_payload_json, logged_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        client_order_id,
                        symbol,
                        info.get("qty"),
                        info.get("avg_entry_price"),
                        info.get("updated_at"),
                        info.get("opened_at"),
                        instrument.get("underlying"),
                        position.get("value"),
                        position.get("upnl"),
                        position.get("rpnl"),
                        raw_url,
                        payload_json,
                        logged_at,
                    ),
                )
