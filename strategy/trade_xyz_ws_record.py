#!/usr/bin/env python3
"""Record trade.xyz BBO data via Hyperliquid WebSocket."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

import pytz
import websockets


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db.record_db import BBODataDB


TZ_UTC8 = pytz.timezone("Asia/Shanghai")
WS_URL = "wss://api.hyperliquid.xyz/ws"
DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, "strategy", "exchange_record_config.json")

logger = logging.getLogger("trade_xyz_ws_record")


def configure_logging() -> None:
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
class TradeXyzTargetConfig:
    exchange: str
    logical_symbol: str
    symbol: str
    ws_coin: str


@dataclass
class RecorderConfig:
    interval: float
    duration: float
    targets: list[TradeXyzTargetConfig]


@dataclass
class TradeXyzTargetRuntime:
    config: TradeXyzTargetConfig
    db: BBODataDB


@dataclass
class LatestBBO:
    bid: Decimal
    bid_size: Decimal
    ask: Decimal
    ask_size: Decimal
    price_timestamp: str


class TradeXyzBboWebSocketManager:
    def __init__(self, targets: list[TradeXyzTargetConfig]):
        self.targets = targets
        self.latest_by_coin: dict[str, LatestBBO] = {}
        self.ws: Optional[websockets.ClientConnection] = None
        self.stop_requested = False
        self.ready_event = asyncio.Event()

    async def connect_and_run(self) -> None:
        subscribed = False
        backoff_seconds = 1.0
        while not self.stop_requested:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=None,
                ) as ws:
                    self.ws = ws
                    if not subscribed:
                        logger.info("trade.xyz ws connected")
                    for target in self.targets:
                        await ws.send(
                            json.dumps(
                                {
                                    "method": "subscribe",
                                    "subscription": {"type": "bbo", "coin": target.ws_coin},
                                }
                            )
                        )
                    subscribed = True
                    backoff_seconds = 1.0
                    async for raw_message in ws:
                        if self.stop_requested:
                            break
                        self._handle_message(raw_message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.stop_requested:
                    break
                logger.warning("trade.xyz ws error: %s", exc)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2.0, 30.0)
            finally:
                self.ws = None

    def _handle_message(self, raw_message: str) -> None:
        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError:
            return

        channel = message.get("channel")
        if channel == "subscriptionResponse":
            return
        if channel != "bbo":
            return

        payload = message.get("data") or {}
        coin = str(payload.get("coin") or "").strip()
        bbo = payload.get("bbo") or []
        if not coin or len(bbo) != 2:
            return

        bid_level, ask_level = bbo
        if not bid_level or not ask_level:
            return

        try:
            bid = Decimal(str(bid_level["px"]))
            bid_size = Decimal(str(bid_level["sz"]))
            ask = Decimal(str(ask_level["px"]))
            ask_size = Decimal(str(ask_level["sz"]))
        except Exception:
            return

        price_timestamp = format_timestamp_to_seconds(
            datetime.fromtimestamp(float(payload.get("time", 0)) / 1000.0, TZ_UTC8)
        )
        self.latest_by_coin[coin] = LatestBBO(
            bid=bid,
            bid_size=bid_size,
            ask=ask,
            ask_size=ask_size,
            price_timestamp=price_timestamp,
        )
        self.ready_event.set()

    def get_bbo(self, coin: str) -> Optional[LatestBBO]:
        return self.latest_by_coin.get(coin)

    async def disconnect(self) -> None:
        self.stop_requested = True
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass


class TradeXyzWsRecorderApp:
    def __init__(self, config: RecorderConfig):
        self.config = config
        self.stop_requested = False
        self.started_at: Optional[float] = None
        self.ws_manager = TradeXyzBboWebSocketManager(config.targets)
        self.ws_task: Optional[asyncio.Task] = None
        self.runtimes = [
            TradeXyzTargetRuntime(
                config=target,
                db=BBODataDB(exchange="trade_xyz", ticker=target.logical_symbol),
            )
            for target in config.targets
        ]

    async def initialize(self) -> None:
        self.ws_task = asyncio.create_task(self.ws_manager.connect_and_run())
        await asyncio.wait_for(self.ws_manager.ready_event.wait(), timeout=15.0)
        for runtime in self.runtimes:
            logger.info(
                "trade.xyz ws target ready logical=%s symbol=%s ws_coin=%s",
                runtime.config.logical_symbol,
                runtime.config.symbol,
                runtime.config.ws_coin,
            )

    async def record_once(self) -> None:
        timestamp = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
        for runtime in self.runtimes:
            latest = self.ws_manager.get_bbo(runtime.config.ws_coin)
            if latest is None:
                logger.warning("trade.xyz ws data not ready symbol=%s", runtime.config.symbol)
                continue

            runtime.db.log_bbo_data(
                best_bid=latest.bid,
                best_bid_size=latest.bid_size,
                best_ask=latest.ask,
                best_ask_size=latest.ask_size,
                price_timestamp=latest.price_timestamp,
                timestamp=timestamp,
                symbol=runtime.config.logical_symbol,
            )
            logger.info(
                "[trade_xyz_ws:%s] symbol=%s coin=%s ask=%s bid=%s ask_qty=%s bid_qty=%s",
                runtime.config.logical_symbol,
                runtime.config.symbol,
                runtime.config.ws_coin,
                latest.ask,
                latest.bid,
                latest.ask_size,
                latest.bid_size,
            )

    async def run(self) -> int:
        self.started_at = asyncio.get_running_loop().time()
        await self.initialize()
        while not self.stop_requested:
            await self.record_once()
            if self.config.duration > 0:
                elapsed = asyncio.get_running_loop().time() - self.started_at
                if elapsed >= self.config.duration:
                    break
            await asyncio.sleep(self.config.interval)
        return 0

    async def cleanup(self) -> None:
        await self.ws_manager.disconnect()
        if self.ws_task is not None:
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.warning("trade.xyz ws cleanup failed: %s", exc)


def load_json_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_target_config(data: dict) -> TradeXyzTargetConfig:
    logical_symbol = str(data.get("logical_symbol") or data.get("symbol") or "").strip().upper()
    symbol = str(data.get("symbol") or "").strip().upper()
    if not logical_symbol or not symbol:
        raise ValueError("trade_xyz target requires logical_symbol and symbol")

    ws_coin = str(data.get("ws_coin") or f"xyz:{symbol}").strip()
    if not ws_coin:
        raise ValueError("trade_xyz target requires ws_coin")

    return TradeXyzTargetConfig(
        exchange="trade_xyz",
        logical_symbol=logical_symbol,
        symbol=symbol,
        ws_coin=ws_coin,
    )


def parse_args() -> RecorderConfig:
    parser = argparse.ArgumentParser(description="Record trade.xyz BBO data via WebSocket")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="JSON config file")
    parser.add_argument("--interval", type=float, default=2.0, help="Record interval in seconds")
    parser.add_argument("--duration", type=float, default=0.0, help="Total runtime in seconds; 0 means forever")
    args = parser.parse_args()

    payload = load_json_file(os.path.expanduser(args.config))
    target_payloads = payload.get("tradexyz")
    if not isinstance(target_payloads, list) or not target_payloads:
        raise ValueError("config must contain a non-empty tradexyz list")

    targets = [normalize_target_config(item) for item in target_payloads]
    if not targets:
        raise ValueError("config must contain at least one trade_xyz target")

    return RecorderConfig(interval=args.interval, duration=args.duration, targets=targets)


async def async_main() -> int:
    configure_logging()
    config = parse_args()
    app = TradeXyzWsRecorderApp(config)

    def handle_shutdown(_signum, _frame) -> None:
        app.stop_requested = True
        logger.info("shutdown requested")

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        return await app.run()
    finally:
        await app.cleanup()


if __name__ == "__main__":
    sys.exit(asyncio.run(async_main()))
