#!/usr/bin/env python3
"""Record frontend quotes from Omni and trade.xyz into bbo_data."""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Optional

from selenium.common.exceptions import InvalidSessionIdException, WebDriverException


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db.record_db import BBODataDB
from exchanges.omni_var import (
    OMNI_TRADER_USER_DATA_DIR,
    build_client as build_omni_client,
    run_auto_connect_flow as run_omni_auto_connect_flow,
    _cleanup_browser_processes_for_profile as cleanup_omni_profile,
    _safe_quit_client as safe_quit_omni_client,
)
from exchanges.trade_xyz import (
    XYZ_TRADER_USER_DATA_DIR,
    build_client as build_trade_xyz_client,
    run_auto_connect_flow as run_trade_xyz_auto_connect_flow,
    _cleanup_browser_processes_for_profile as cleanup_trade_xyz_profile,
)


_SHUTDOWN_REQUESTED = False
_MAX_SESSION_RECOVERY_ATTEMPTS = 3


def _handle_shutdown(signum, _frame) -> None:
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = True
    print(f"[frontend_spread_recorder] received signal {signum}, shutting down...")


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)


@dataclass
class RecorderConfig:
    logical_symbol: str
    omni_symbol: str
    trade_xyz_symbol: str
    interval: float
    duration: float
    auto_connect_omni: bool
    auto_connect_trade_xyz: bool
    no_manual_wallet_confirm: bool
    omni_user_data_dir: str
    trade_xyz_user_data_dir: str


def parse_args() -> RecorderConfig:
    parser = argparse.ArgumentParser(
        description="Start Omni and trade.xyz browsers, then persist quotes into bbo_data",
    )
    parser.add_argument("--symbol", default="XAU", help="Logical symbol stored in bbo_data")
    parser.add_argument("--omni-symbol", default="XAUT", help="Omni market symbol to open")
    parser.add_argument("--trade-symbol", default="GOLD", help="trade.xyz market symbol to open")
    parser.add_argument("--interval", type=float, default=10.0, help="Polling interval in seconds")
    parser.add_argument("--duration", type=float, default=0.0, help="Total runtime in seconds; 0 means forever")
    parser.add_argument("--auto-connect-omni", action="store_true", help="Run Omni wallet auto-connect if needed")
    parser.add_argument(
        "--auto-connect-trade-xyz",
        action="store_true",
        help="Run trade.xyz wallet auto-connect if needed",
    )
    parser.add_argument(
        "--no-manual-wallet-confirm",
        action="store_true",
        help="Do not wait for Enter after OKX confirmation",
    )
    parser.add_argument("--omni-user-data-dir", default=OMNI_TRADER_USER_DATA_DIR)
    parser.add_argument("--trade-user-data-dir", default=XYZ_TRADER_USER_DATA_DIR)
    args = parser.parse_args()
    return RecorderConfig(
        logical_symbol=args.symbol.strip().upper(),
        omni_symbol=args.omni_symbol.strip().upper(),
        trade_xyz_symbol=args.trade_symbol.strip().upper(),
        interval=args.interval,
        duration=args.duration,
        auto_connect_omni=args.auto_connect_omni,
        auto_connect_trade_xyz=args.auto_connect_trade_xyz,
        no_manual_wallet_confirm=args.no_manual_wallet_confirm,
        omni_user_data_dir=os.path.expanduser(args.omni_user_data_dir),
        trade_xyz_user_data_dir=os.path.expanduser(args.trade_user_data_dir),
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


def _safe_quit_trade_xyz_client(client, profile: str) -> None:
    try:
        client.driver.quit()
    except Exception:
        pass
    cleanup_trade_xyz_profile(profile, reason="strategy_recorder_quit")


def _start_clients(config: RecorderConfig) -> tuple:
    omni_client = build_omni_client(
        [config.omni_symbol],
        user_data_dir=config.omni_user_data_dir,
        dry_run=True,
    )
    trade_xyz_client = build_trade_xyz_client(
        [config.trade_xyz_symbol],
        user_data_dir=config.trade_xyz_user_data_dir,
        dry_run=True,
    )

    print(f"[frontend_spread_recorder] opening Omni market: {config.omni_symbol}")
    omni_client.open_market_tabs()
    omni_client.install_quote_cache()
    print(f"[frontend_spread_recorder] opening trade.xyz market: {config.trade_xyz_symbol}")
    trade_xyz_client.open_market_tabs()
    trade_xyz_client.install_quote_cache()
    return omni_client, trade_xyz_client


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

    cleanup_omni_profile(config.omni_user_data_dir, reason="strategy_recorder_start")
    cleanup_trade_xyz_profile(config.trade_xyz_user_data_dir, reason="strategy_recorder_start")

    omni_client, trade_xyz_client = _start_clients(config)

    omni_db = BBODataDB(exchange="omni", ticker=config.logical_symbol)
    trade_xyz_db = BBODataDB(exchange="trade_xyz", ticker=config.logical_symbol)

    try:
        if config.auto_connect_omni:
            run_omni_auto_connect_flow(
                omni_client,
                manual_confirm=not config.no_manual_wallet_confirm,
            )
        if config.auto_connect_trade_xyz:
            run_trade_xyz_auto_connect_flow(
                trade_xyz_client,
                manual_confirm=not config.no_manual_wallet_confirm,
            )

        started = time.time()
        recovery_attempts = 0
        while not _SHUTDOWN_REQUESTED:
            try:
                omni_snapshots = [omni_client.capture_cached_quote(persist=False)]
                trade_xyz_snapshots = [trade_xyz_client.capture_cached_quote(persist=False)]
            except Exception as exc:
                if not _is_driver_session_error(exc):
                    raise
                recovery_attempts += 1
                print(
                    "[frontend_spread_recorder] webdriver session dropped "
                    f"(attempt {recovery_attempts}/{_MAX_SESSION_RECOVERY_ATTEMPTS}): {exc}"
                )
                if recovery_attempts > _MAX_SESSION_RECOVERY_ATTEMPTS:
                    print("[frontend_spread_recorder] exceeded webdriver recovery attempts; exiting")
                    return 1

                safe_quit_omni_client(omni_client)
                _safe_quit_trade_xyz_client(trade_xyz_client, config.trade_xyz_user_data_dir)
                cleanup_omni_profile(config.omni_user_data_dir, reason="strategy_recorder_recover")
                cleanup_trade_xyz_profile(config.trade_xyz_user_data_dir, reason="strategy_recorder_recover")

                time.sleep(2.0)
                omni_client, trade_xyz_client = _start_clients(config)
                continue

            recovery_attempts = 0

            _persist_snapshots(omni_db, config.logical_symbol, omni_snapshots)
            _persist_snapshots(trade_xyz_db, config.logical_symbol, trade_xyz_snapshots)

            _print_snapshots("omni", omni_snapshots)
            _print_snapshots("trade_xyz", trade_xyz_snapshots)

            if config.duration > 0 and (time.time() - started) >= config.duration:
                break
            time.sleep(config.interval)

        return 0
    finally:
        safe_quit_omni_client(omni_client)
        _safe_quit_trade_xyz_client(trade_xyz_client, config.trade_xyz_user_data_dir)


if __name__ == "__main__":
    sys.exit(main())
