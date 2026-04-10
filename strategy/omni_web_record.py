#!/usr/bin/env python3
"""Record Omni bid/ask by calling /api/quotes/simple from a browser page context."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from selenium.common.exceptions import InvalidSessionIdException, WebDriverException


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db.record_db import BBODataDB
from exchanges.omni_var import (
    _cleanup_browser_processes_for_profile as cleanup_omni_profile,
    _safe_quit_client as safe_quit_omni_client,
    build_client as build_omni_client,
)


_SHUTDOWN_REQUESTED = False
_MAX_SESSION_RECOVERY_ATTEMPTS = 3
_DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, "strategy", "exchange_record_config.json")


def _handle_shutdown(signum, _frame) -> None:
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = True
    print(f"[omni_web_record] received signal {signum}, shutting down...")


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)


@dataclass
class OmniRecordTarget:
    logical_symbol: str
    symbol: str
    quantity: Decimal
    db: BBODataDB


@dataclass
class RecorderConfig:
    interval: float
    duration: float
    order_user_data_dir: str
    record_user_data_dir: str
    symbols: list[dict[str, Any]]


def _load_json_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_args() -> RecorderConfig:
    parser = argparse.ArgumentParser(description="Record Omni bid/ask through in-page fetch calls")
    parser.add_argument("--config", default=_DEFAULT_CONFIG_PATH, help="JSON config file")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds")
    parser.add_argument("--duration", type=float, default=0.0, help="Total runtime in seconds; 0 means forever")
    args = parser.parse_args()

    config_path = os.path.expanduser(args.config) if args.config else ""
    if not config_path or not os.path.exists(config_path):
        raise ValueError(f"config file not found: {config_path}")

    payload = _load_json_file(config_path)
    omni_payload = payload.get("omni")
    if not isinstance(omni_payload, dict):
        raise ValueError("config must contain omni object")
    symbols = omni_payload.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("config must contain a non-empty omni.symbols list")

    order_user_data_dir = os.path.expanduser(str(omni_payload.get("order_user_data_dir") or "").strip())
    record_user_data_dir = os.path.expanduser(str(omni_payload.get("record_user_data_dir") or "").strip())
    if not record_user_data_dir:
        raise ValueError("config must contain omni.record_user_data_dir")

    return RecorderConfig(
        interval=args.interval,
        duration=args.duration,
        order_user_data_dir=order_user_data_dir,
        record_user_data_dir=record_user_data_dir,
        symbols=symbols,
    )


def _build_targets(config: RecorderConfig) -> list[OmniRecordTarget]:
    targets: list[OmniRecordTarget] = []
    for item in config.symbols:
        symbol = str(item.get("symbol") or "").strip().upper()
        logical_symbol = str(item.get("logical_symbol") or symbol).strip().upper()
        quantity_raw = item.get("quantity", 1.0)
        if not symbol or not logical_symbol:
            raise ValueError("omni.symbols items require logical_symbol and symbol")
        quantity = Decimal(str(quantity_raw))
        targets.append(
            OmniRecordTarget(
                logical_symbol=logical_symbol,
                symbol=symbol,
                quantity=quantity,
                db=BBODataDB(exchange="omni", ticker=logical_symbol),
            )
        )
    return targets


def _start_client(config: RecorderConfig, bootstrap_symbol: str):
    client = build_omni_client(
        [bootstrap_symbol],
        user_data_dir=config.record_user_data_dir,
        dry_run=True,
        market_overrides={bootstrap_symbol: {"path": f"perpetual/{bootstrap_symbol}", "quantity": 1.0}},
    )
    print(
        f"[omni_web_record] opening omni market bootstrap={bootstrap_symbol} "
        f"profile={config.record_user_data_dir}"
    )
    client.open_market_tabs()
    return client


def _fetch_quotes(client, targets: list[OmniRecordTarget]) -> list[dict[str, Any]]:
    request_payloads = [
        {
            "logical_symbol": target.logical_symbol,
            "symbol": target.symbol,
            "qty": str(target.quantity),
            "instrument": {
                "underlying": target.symbol,
                "funding_interval_s": 3600,
                "settlement_asset": "USDC",
                "instrument_type": "perpetual_future",
            },
        }
        for target in targets
    ]
    return client.driver.execute_async_script(
        """
        const payloads = arguments[0];
        const done = arguments[arguments.length - 1];
        Promise.all(
          payloads.map(async (item) => {
            const response = await fetch('/api/quotes/simple', {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({
                instrument: item.instrument,
                qty: item.qty,
              }),
            });
            let data = null;
            let text = null;
            try {
              data = await response.json();
            } catch (err) {
              text = await response.text();
            }
            return {
              logical_symbol: item.logical_symbol,
              symbol: item.symbol,
              status: response.status,
              ok: response.ok,
              data,
              text,
              captured_at: new Date().toISOString(),
              page_url: window.location.href,
            };
          })
        ).then(done).catch((err) => done([{ ok: false, error: String(err) }]));
        """,
        request_payloads,
    )


def _persist_result(target: OmniRecordTarget, result: dict[str, Any]) -> None:
    data = result.get("data") or {}
    bid = data.get("bid")
    ask = data.get("ask")
    if bid is None or ask is None:
        raise RuntimeError(
            f"[omni_web_record] quote response missing bid/ask for {target.symbol}: "
            f"status={result.get('status')} body={data or result.get('text')}"
        )
    timestamp = str(result.get("captured_at") or "")
    target.db.log_bbo_data(
        best_bid=Decimal(str(bid)),
        best_bid_size=target.quantity,
        best_ask=Decimal(str(ask)),
        best_ask_size=target.quantity,
        price_timestamp=timestamp,
        timestamp=timestamp,
        symbol=target.logical_symbol,
    )
    print(
        f"[omni:{target.logical_symbol}] {timestamp} "
        f"symbol={target.symbol} ask={ask} bid={bid} ask_qty={target.quantity} bid_qty={target.quantity}"
    )


def _is_driver_session_error(exc: Exception) -> bool:
    if isinstance(exc, InvalidSessionIdException):
        return True
    if isinstance(exc, WebDriverException):
        message = str(exc).lower()
        return (
            "invalid session id" in message
            or "disconnected" in message
            or "not connected to devtools" in message
            or "chrome not reachable" in message
            or "target window already closed" in message
        )
    return False


def main() -> int:
    global _SHUTDOWN_REQUESTED
    config = parse_args()
    install_signal_handlers()

    targets = _build_targets(config)
    cleanup_omni_profile(config.record_user_data_dir, reason="omni_web_record_start")
    client = _start_client(config, targets[0].symbol)
    recovery_attempts = 0

    try:
        started = time.time()
        while not _SHUTDOWN_REQUESTED:
            cycle_started = time.time()
            try:
                results = _fetch_quotes(client, targets)
            except Exception as exc:
                if not _is_driver_session_error(exc):
                    raise
                recovery_attempts += 1
                print(
                    f"[omni_web_record] webdriver session dropped "
                    f"(attempt {recovery_attempts}/{_MAX_SESSION_RECOVERY_ATTEMPTS}): {exc}"
                )
                if recovery_attempts > _MAX_SESSION_RECOVERY_ATTEMPTS:
                    return 1
                safe_quit_omni_client(client)
                cleanup_omni_profile(config.record_user_data_dir, reason="omni_web_record_recover")
                time.sleep(2.0)
                client = _start_client(config, targets[0].symbol)
                continue

            recovery_attempts = 0
            result_map = {str(item.get("logical_symbol") or "").upper(): item for item in results if isinstance(item, dict)}
            for target in targets:
                result = result_map.get(target.logical_symbol)
                if not result:
                    raise RuntimeError(f"[omni_web_record] missing quote result for {target.logical_symbol}")
                _persist_result(target, result)

            if config.duration > 0 and (time.time() - started) >= config.duration:
                break
            elapsed = time.time() - cycle_started
            sleep_seconds = max(0.0, config.interval - elapsed)
            time.sleep(sleep_seconds)
        return 0
    finally:
        safe_quit_omni_client(client)


if __name__ == "__main__":
    sys.exit(main())
