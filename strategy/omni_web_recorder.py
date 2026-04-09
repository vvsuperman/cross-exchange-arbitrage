#!/usr/bin/env python3
"""Record Omni frontend quotes into bbo_data."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Optional

from selenium.common.exceptions import InvalidSessionIdException, WebDriverException


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db.record_db import BBODataDB
from exchanges.omni_var import (
    OMNI_TRADER_USER_DATA_DIR,
    _cleanup_browser_processes_for_profile as cleanup_omni_profile,
    _safe_quit_client as safe_quit_omni_client,
    build_client as build_omni_client,
    run_auto_connect_flow as run_omni_auto_connect_flow,
)


_SHUTDOWN_REQUESTED = False
_MAX_SESSION_RECOVERY_ATTEMPTS = 3
_DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, "strategy", "exchange_record_config.json")


def _handle_shutdown(signum, _frame) -> None:
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = True
    print(f"[omni_web_recorder] received signal {signum}, shutting down...")


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)


@dataclass
class ExchangeTargetConfig:
    exchange: str
    logical_symbol: str
    symbol: str
    user_data_dir: str
    path: Optional[str] = None
    quantity: float = 1.0


@dataclass
class RecorderConfig:
    interval: float
    duration: float
    auto_connect: bool
    no_manual_wallet_confirm: bool
    targets: list[ExchangeTargetConfig]


@dataclass
class TargetRuntime:
    config: ExchangeTargetConfig
    client: Any
    db: BBODataDB
    recovery_attempts: int = 0


def _load_json_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _normalize_target_config(data: dict) -> ExchangeTargetConfig:
    payload = data or {}
    symbol = str(payload.get("symbol") or "").strip().upper()
    logical_symbol = str(payload.get("logical_symbol") or symbol).strip().upper()
    if not symbol:
        raise ValueError("omni symbol is required")
    if not logical_symbol:
        raise ValueError("omni logical_symbol is required")
    user_data_dir = os.path.expanduser(
        str(payload.get("user_data_dir") or OMNI_TRADER_USER_DATA_DIR).strip()
    )
    if not user_data_dir:
        raise ValueError("omni user_data_dir is required")
    raw_path = payload.get("path")
    path = str(raw_path).strip() if raw_path is not None else None
    quantity = float(payload.get("quantity", 1.0))
    return ExchangeTargetConfig(
        exchange="omni",
        logical_symbol=logical_symbol,
        symbol=symbol,
        user_data_dir=user_data_dir,
        path=path or None,
        quantity=quantity,
    )


def _validate_unique_profiles(targets: list[ExchangeTargetConfig]) -> None:
    seen: dict[str, str] = {}
    for target in targets:
        label = f"{target.logical_symbol}:{target.symbol}"
        existing = seen.get(target.user_data_dir)
        if existing:
            raise ValueError(
                f"duplicate user_data_dir detected: {target.user_data_dir} used by {existing} and {label}"
            )
        seen[target.user_data_dir] = label


def parse_args() -> RecorderConfig:
    parser = argparse.ArgumentParser(description="Start Omni browsers, then persist quotes into bbo_data")
    parser.add_argument("--config", default=_DEFAULT_CONFIG_PATH, help="JSON config file")
    parser.add_argument("--interval", type=float, default=10.0, help="Polling interval in seconds")
    parser.add_argument("--duration", type=float, default=0.0, help="Total runtime in seconds; 0 means forever")
    parser.add_argument("--auto-connect", action="store_true", help="Run Omni wallet auto-connect if needed")
    parser.add_argument(
        "--no-manual-wallet-confirm",
        action="store_true",
        help="Do not wait for Enter after OKX confirmation",
    )
    args = parser.parse_args()

    config_path = os.path.expanduser(args.config) if args.config else ""
    if not config_path or not os.path.exists(config_path):
        raise ValueError(f"config file not found: {config_path}")

    payload = _load_json_file(config_path)
    target_payloads = payload.get("omni")
    if not isinstance(target_payloads, list) or not target_payloads:
        raise ValueError("config must contain a non-empty omni list")

    targets = [_normalize_target_config(item) for item in target_payloads]
    if not targets:
        raise ValueError("config must contain at least one omni target")

    _validate_unique_profiles(targets)
    return RecorderConfig(
        interval=args.interval,
        duration=args.duration,
        auto_connect=args.auto_connect,
        no_manual_wallet_confirm=args.no_manual_wallet_confirm,
        targets=targets,
    )


def _to_decimal(value: Optional[float]) -> Optional[Decimal]:
    if value is None:
        return None
    return Decimal(str(value))


def _persist_snapshots(db: BBODataDB, symbol: str, snapshots: Iterable) -> None:
    for snapshot in snapshots:
        db.log_bbo_data(
            best_bid=_to_decimal(snapshot.bid),
            best_bid_size=_to_decimal(snapshot.bid_quantity),
            best_ask=_to_decimal(snapshot.ask),
            best_ask_size=_to_decimal(snapshot.ask_quantity),
            price_timestamp=snapshot.captured_at,
            timestamp=snapshot.captured_at,
            symbol=symbol,
        )


def _print_snapshots(label: str, snapshots: Iterable) -> None:
    for snapshot in snapshots:
        print(
            f"[{label}] {snapshot.captured_at} "
            f"symbol={snapshot.symbol} ask={snapshot.ask} bid={snapshot.bid} "
            f"ask_qty={snapshot.ask_quantity} bid_qty={snapshot.bid_quantity}"
        )


def _market_override_payload(target: ExchangeTargetConfig) -> dict[str, dict[str, Any]]:
    payload: dict[str, Any] = {"quantity": target.quantity}
    if target.path:
        payload["path"] = target.path
    return {target.symbol: payload}


def _start_runtime(target: ExchangeTargetConfig) -> TargetRuntime:
    client = build_omni_client(
        [target.symbol],
        user_data_dir=target.user_data_dir,
        dry_run=True,
        market_overrides=_market_override_payload(target),
    )
    print(
        f"[omni_web_recorder] opening omni market: "
        f"logical={target.logical_symbol} symbol={target.symbol} profile={target.user_data_dir}"
    )
    client.open_market_tabs()
    client.install_quote_cache()
    return TargetRuntime(
        config=target,
        client=client,
        db=BBODataDB(exchange=target.exchange, ticker=target.logical_symbol),
    )


def _safe_quit_runtime(runtime: TargetRuntime) -> None:
    safe_quit_omni_client(runtime.client)


def _maybe_auto_connect(runtime: TargetRuntime, config: RecorderConfig) -> None:
    if config.auto_connect:
        run_omni_auto_connect_flow(
            runtime.client,
            manual_confirm=not config.no_manual_wallet_confirm,
        )


def _capture_runtime(runtime: TargetRuntime) -> list:
    return [runtime.client.capture_cached_quote(persist=False)]


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

    runtimes: list[TargetRuntime] = []
    for target in config.targets:
        cleanup_omni_profile(target.user_data_dir, reason="omni_web_recorder_start")
        runtimes.append(_start_runtime(target))

    try:
        for runtime in runtimes:
            _maybe_auto_connect(runtime, config)

        started = time.time()
        while not _SHUTDOWN_REQUESTED:
            for index, runtime in enumerate(list(runtimes)):
                try:
                    snapshots = _capture_runtime(runtime)
                except Exception as exc:
                    if not _is_driver_session_error(exc):
                        raise
                    runtime.recovery_attempts += 1
                    print(
                        "[omni_web_recorder] webdriver session dropped "
                        f"logical={runtime.config.logical_symbol} "
                        f"(attempt {runtime.recovery_attempts}/{_MAX_SESSION_RECOVERY_ATTEMPTS}): {exc}"
                    )
                    if runtime.recovery_attempts > _MAX_SESSION_RECOVERY_ATTEMPTS:
                        print(
                            "[omni_web_recorder] exceeded webdriver recovery attempts "
                            f"for logical={runtime.config.logical_symbol}; exiting"
                        )
                        return 1

                    _safe_quit_runtime(runtime)
                    cleanup_omni_profile(runtime.config.user_data_dir, reason="omni_web_recorder_recover")
                    time.sleep(2.0)
                    recovered = _start_runtime(runtime.config)
                    recovered.recovery_attempts = runtime.recovery_attempts
                    runtimes[index] = recovered
                    _maybe_auto_connect(recovered, config)
                    continue

                runtime.recovery_attempts = 0
                _persist_snapshots(runtime.db, runtime.config.logical_symbol, snapshots)
                _print_snapshots(f"omni:{runtime.config.logical_symbol}", snapshots)

            if config.duration > 0 and (time.time() - started) >= config.duration:
                break
            time.sleep(config.interval)
        return 0
    finally:
        for runtime in runtimes:
            _safe_quit_runtime(runtime)


if __name__ == "__main__":
    sys.exit(main())
