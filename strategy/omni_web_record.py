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
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from selenium.common.exceptions import InvalidSessionIdException, TimeoutException, WebDriverException


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db.record_db import BBODataDB, TZ_UTC8
from exchanges.omni_var import (
    _cleanup_browser_processes_for_profile as cleanup_omni_profile,
    _safe_quit_client as safe_quit_omni_client,
    build_client as build_omni_client,
)


_SHUTDOWN_REQUESTED = False
_MAX_SESSION_RECOVERY_ATTEMPTS = 3
_DEFAULT_CONFIG_PATH = os.path.join(PROJECT_ROOT, "strategy", "exchange_record_config.json")
# Selenium async script must call back within this many seconds.
_ASYNC_SCRIPT_TIMEOUT_S = 3000
# Abort single /api/quotes/simple if it hangs (ms). Slightly below script timeout.
_QUOTE_FETCH_DEADLINE_MS = 2_900_000
# Delay between serial quote requests on the Python side (seconds between scripts).
_SERIAL_GAP_MS = 200


# One /api/quotes/simple per async script; serial rhythm enforced in Python (sleep between calls).
_OMNI_SINGLE_QUOTE_SCRIPT = """
const item = arguments[0];
const fetchDeadlineMs = arguments[1];
const done = arguments[arguments.length - 1];
(async () => {
  const captured_at = new Date().toISOString();
  const page_url = window.location.href;
  const controller = new AbortController();
  const kill = setTimeout(() => controller.abort(), fetchDeadlineMs);
  try {
    const response = await fetch('/api/quotes/simple', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        instrument: item.instrument,
        qty: item.qty,
      }),
      signal: controller.signal,
    });
    let data = null;
    let text = null;
    const raw = await response.text();
    if (raw) {
      try {
        data = JSON.parse(raw);
      } catch (parseErr) {
        text = raw;
      }
    }
    return {
      logical_symbol: item.logical_symbol,
      symbol: item.symbol,
      status: response.status,
      ok: response.ok,
      data,
      text,
      captured_at,
      page_url,
    };
  } catch (err) {
    return {
      logical_symbol: item.logical_symbol,
      symbol: item.symbol,
      status: null,
      ok: false,
      data: null,
      text: null,
      error: String(err),
      captured_at,
      page_url,
    };
  } finally {
    clearTimeout(kill);
  }
})()
  .then(done)
  .catch((err) =>
    done({
      logical_symbol: item.logical_symbol,
      symbol: item.symbol,
      status: null,
      ok: false,
      data: null,
      text: null,
      error: String(err),
      captured_at: new Date().toISOString(),
      page_url: window.location.href,
    })
  );
"""


def _format_ts_db(dt: datetime) -> str:
    """Match lighter_record / trade_xyz_ws_record: naive ISO, Asia/Shanghai wall time, second precision."""
    return dt.replace(microsecond=0, tzinfo=None).isoformat()


def _captured_at_to_db_timestamp(captured_at: str) -> str:
    """Browser uses Date.toISOString() (UTC, …Z). DB uses same convention as other recorders (UTC+8, no tz suffix)."""
    s = (captured_at or "").strip()
    if not s:
        return _format_ts_db(datetime.now(TZ_UTC8))
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = TZ_UTC8.localize(dt)
        else:
            dt = dt.astimezone(TZ_UTC8)
        return _format_ts_db(dt)
    except ValueError:
        return s


def _quote_body_snippet(result: dict[str, Any], max_len: int = 240) -> str:
    raw = result.get("text")
    if raw is None and result.get("data") is not None:
        raw = json.dumps(result.get("data"), default=str)
    if raw is None:
        return ""
    text = raw if isinstance(raw, str) else str(raw)
    one_line = " ".join(text.split())
    if len(one_line) > max_len:
        return one_line[:max_len] + "..."
    return one_line


def _cloudflare_challenge_hint(result: dict[str, Any]) -> str:
    raw = result.get("text")
    if not isinstance(raw, str) or not raw:
        return ""
    lower = raw.lower()
    if (
        "cdn-cgi/challenge" in lower
        or "challenges.cloudflare.com" in lower
        or "_cf_chl_opt" in raw
    ):
        return (
            " Cloudflare returned a browser challenge page instead of JSON — "
            "open Omni in this Chrome profile, pass the check once, then retry."
        )
    return ""


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
    parser.add_argument("--interval", type=float, default=2.0, help="Polling interval in seconds")
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
    client.driver.set_script_timeout(_ASYNC_SCRIPT_TIMEOUT_S)
    return client


def _fetch_quotes(client, targets: list[OmniRecordTarget]) -> list[dict[str, Any]]:
    """串行拉报价：Python 在两次 execute_async_script 之间 sleep，避免单脚本里间隔不生效或难以观察。"""
    fetch_ms = int(_QUOTE_FETCH_DEADLINE_MS)
    gap_s = _SERIAL_GAP_MS / 1000.0
    results: list[dict[str, Any]] = []
    for i, target in enumerate(targets):
        if i > 0:
            time.sleep(gap_s)
        payload = {
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
        row = client.driver.execute_async_script(_OMNI_SINGLE_QUOTE_SCRIPT, payload, fetch_ms)
        if not isinstance(row, dict):
            raise RuntimeError(
                f"[omni_web_record] unexpected quote script return for {target.logical_symbol}: {type(row).__name__!r}"
            )
        results.append(row)
    return results


def _persist_result(target: OmniRecordTarget, result: dict[str, Any]) -> None:
    data = result.get("data") or {}
    bid = data.get("bid")
    ask = data.get("ask")
    if bid is None or ask is None:
        err = result.get("error")
        snippet = _quote_body_snippet(result)
        cf = _cloudflare_challenge_hint(result)
        raise RuntimeError(
            f"[omni_web_record] quote response missing bid/ask for {target.logical_symbol} "
            f"(symbol={target.symbol}): status={result.get('status')} error={err!r}"
            f"{cf}"
            + (f" snippet={snippet!r}" if snippet else "")
        )
    timestamp = _captured_at_to_db_timestamp(str(result.get("captured_at") or ""))
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
            results: Optional[list] = None
            try:
                results = _fetch_quotes(client, targets)
            except TimeoutException as exc:
                print(f"[omni_web_record] async script timeout (batch quotes): {exc}")
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
            if results is None:
                pass
            elif not isinstance(results, list):
                print(f"[omni_web_record] quotes script returned non-list: {type(results).__name__!r}")
            elif len(results) != len(targets):
                print(
                    f"[omni_web_record] quotes length mismatch: len={len(results)} expected={len(targets)}"
                )
            else:
                result_map = {
                    str(item.get("logical_symbol") or "").upper(): item
                    for item in results
                    if isinstance(item, dict) and str(item.get("logical_symbol") or "").strip()
                }
                for target in targets:
                    result = result_map.get(target.logical_symbol)
                    if not result:
                        keys = sorted(result_map.keys())
                        raise RuntimeError(
                            f"[omni_web_record] missing quote result for {target.logical_symbol} "
                            f"(result_map keys={keys})"
                        )
                    try:
                        _persist_result(target, result)
                    except RuntimeError as exc:
                        print(str(exc))

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
