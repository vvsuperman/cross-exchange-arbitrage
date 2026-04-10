#!/usr/bin/env python3
"""Record Lighter BBO data into the shared record_db schema."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

import pytz
from dotenv import load_dotenv


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db.record_db import BBODataDB
from exchanges.lighter import LighterClient
from monitor.websocket_managers import LighterMultiTickerWebSocketManager


TZ_UTC8 = pytz.timezone("Asia/Shanghai")
_DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, "strategy", "exchange_record_config.json")

logger = logging.getLogger("lighter_record")


def _configure_logging() -> None:
    if logger.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False


def format_timestamp_to_seconds(dt: datetime) -> str:
    return dt.replace(microsecond=0, tzinfo=None).isoformat()


@dataclass
class Config:
    ticker: str
    contract_id: Optional[int] = None
    tick_size: Decimal = Decimal("0.01")
    quantity: Decimal = Decimal("0.001")
    close_order_side: str = "buy"


@dataclass
class LighterTargetConfig:
    exchange: str
    logical_symbol: str
    symbol: str


@dataclass
class RecorderConfig:
    interval: float
    targets: list[LighterTargetConfig]


@dataclass
class LighterTargetRuntime:
    config: LighterTargetConfig
    db: BBODataDB
    client: LighterClient


class LighterRecorderApp:
    def __init__(self, config: RecorderConfig):
        self.config = config
        self.stop_requested = False
        self.ws_manager: Optional[LighterMultiTickerWebSocketManager] = None
        self.runtimes: list[LighterTargetRuntime] = []

    def _symbol_bbo_ready(self, symbol: str) -> bool:
        if self.ws_manager is None:
            return False
        row = self.ws_manager.ticker_data.get(symbol)
        if not row:
            return False
        return bool(
            row.get("ready")
            and row.get("best_bid") is not None
            and row.get("best_ask") is not None
        )

    async def _await_bbo_ready(self, timeout_s: float = 30.0) -> None:
        """Wait until each target has snapshot + best bid/ask (same readiness as get_ticker_data)."""
        if self.ws_manager is None or not self.runtimes:
            return
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if all(self._symbol_bbo_ready(rt.config.symbol) for rt in self.runtimes):
                logger.info("lighter websocket BBO ready for all %s targets", len(self.runtimes))
                return
            await asyncio.sleep(0.25)
        pending = [rt.config.symbol for rt in self.runtimes if not self._symbol_bbo_ready(rt.config.symbol)]
        logger.warning(
            "lighter BBO not ready after %.0fs for: %s; continuing (check WS / auth / market)",
            timeout_s,
            ", ".join(pending) if pending else "(unknown)",
        )

    async def initialize(self) -> None:
        lighter_client_instance = None
        lighter_account_index = None
        market_indexes: list[int] = []
        ticker_names: list[str] = []

        for target in self.config.targets:
            client = LighterClient(Config(ticker=target.symbol))
            try:
                await client._initialize_lighter_client()
                market_index, _base_multiplier, _price_multiplier = await client._get_market_config(target.symbol)
            except Exception as exc:
                logger.warning(
                    "lighter target skipped logical=%s symbol=%s error=%s",
                    target.logical_symbol,
                    target.symbol,
                    exc,
                )
                try:
                    await client.disconnect()
                except Exception:
                    pass
                continue

            if lighter_client_instance is None:
                lighter_client_instance = client.lighter_client
                lighter_account_index = client.account_index

            self.runtimes.append(
                LighterTargetRuntime(
                    config=target,
                    db=BBODataDB(exchange="lighter", ticker=target.logical_symbol),
                    client=client,
                )
            )
            ticker_names.append(target.symbol)
            market_indexes.append(int(market_index))
            logger.info(
                "lighter target ready logical=%s symbol=%s market_index=%s",
                target.logical_symbol,
                target.symbol,
                market_index,
            )

        if lighter_client_instance is None or lighter_account_index is None:
            raise RuntimeError("no valid lighter targets configured")

        self.ws_manager = LighterMultiTickerWebSocketManager(
            lighter_client_instance,
            lighter_account_index,
            logger,
        )
        await self.ws_manager.subscribe_all(ticker_names, market_indexes)
        await self._await_bbo_ready()

    async def record_once(self) -> None:
        if self.ws_manager is None:
            raise RuntimeError("lighter websocket manager not initialized")

        for runtime in self.runtimes:
            local_dt = datetime.now(TZ_UTC8)
            timestamp = format_timestamp_to_seconds(local_dt)
            local_ms = int(local_dt.timestamp() * 1000)

            bid, bid_size, ask, ask_size, price_timestamp, exchange_ts_ms = self.ws_manager.get_ticker_data(
                runtime.config.symbol
            )
            if bid is None or ask is None:
                logger.warning("lighter data not ready symbol=%s", runtime.config.symbol)
                continue

            gap_ms: Optional[int] = None
            if exchange_ts_ms is not None:
                gap_ms = int(local_ms - float(exchange_ts_ms))

            runtime.db.log_bbo_data(
                best_bid=bid,
                best_bid_size=bid_size,
                best_ask=ask,
                best_ask_size=ask_size,
                price_timestamp=price_timestamp or timestamp,
                timestamp=timestamp,
                symbol=runtime.config.logical_symbol,
            )
            logger.info(
                "[lighter:%s] symbol=%s ask=%s bid=%s ask_qty=%s bid_qty=%s gap_ms=%s",
                runtime.config.logical_symbol,
                runtime.config.symbol,
                ask,
                bid,
                ask_size,
                bid_size,
                gap_ms if gap_ms is not None else "n/a",
            )

    async def run(self) -> int:
        await self.initialize()
        while not self.stop_requested:
            await self.record_once()
            await asyncio.sleep(self.config.interval)
        return 0

    async def cleanup(self) -> None:
        if self.ws_manager is not None:
            try:
                await self.ws_manager.disconnect()
            except Exception as exc:
                logger.warning("lighter websocket disconnect failed: %s", exc)

        for runtime in self.runtimes:
            try:
                await runtime.client.disconnect()
            except Exception:
                pass


def _load_json_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _normalize_target_config(data: dict) -> LighterTargetConfig:
    logical_symbol = str(data.get("logical_symbol") or data.get("symbol") or "").strip().upper()
    symbol = str(data.get("symbol") or "").strip().upper()
    if not logical_symbol or not symbol:
        raise ValueError("lighter target requires logical_symbol and symbol")

    return LighterTargetConfig(
        exchange="lighter",
        logical_symbol=logical_symbol,
        symbol=symbol,
    )


def parse_args() -> RecorderConfig:
    parser = argparse.ArgumentParser(description="Record Lighter BBO data into record_db")
    parser.add_argument("--config", default=_DEFAULT_CONFIG_PATH, help="JSON config file")
    parser.add_argument("--interval", type=float, default=5.0, help="Polling interval in seconds")
    args = parser.parse_args()

    config_path = os.path.expanduser(args.config)
    payload = _load_json_file(config_path)
    target_payloads = payload.get("lighter")
    if not isinstance(target_payloads, list) or not target_payloads:
        raise ValueError("config must contain a non-empty lighter list")

    targets = [_normalize_target_config(item) for item in target_payloads]
    if not targets:
        raise ValueError("config must contain at least one lighter target")
    return RecorderConfig(interval=args.interval, targets=targets)


async def async_main() -> int:
    _configure_logging()
    load_dotenv()
    config = parse_args()
    app = LighterRecorderApp(config)

    def _handle_shutdown(_signum, _frame) -> None:
        app.stop_requested = True
        logger.info("shutdown requested")

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    try:
        return await app.run()
    finally:
        await app.cleanup()


if __name__ == "__main__":
    sys.exit(asyncio.run(async_main()))
