"""SeleniumBase driver for trade.xyz price observation only."""



# 钱包连接查考omni_var.py
# 市价下单功能
# 1. 找到并点击 "Long" / "Short" tab
# 2. 切换到 "Market" tab
# 3. 确保是标的数量而非usd：确保不是<p class="text-brand-slate-400 flex items-center text-sm select-none">USDC</p>，而是<p class="text-brand-slate-400 flex items-center text-sm select-none">GOLD</p>这样的名称，否则点击下面的标签
#<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-arrow-right-left text-brand-slate-400 hover:text-brand-white-100 size-4 cursor-pointer" aria-hidden="true"><path d="m16 3 4 4-4 4"></path><path d="M20 7H4"></path><path d="m8 21-4-4 4-4"></path><path d="M4 17h16"></path></svg>
# 4. 输入数量
# 5. 随机暂停，点击 "Long"/ "Short" 按钮
# 6. 监控是否post请求 https://api-ui.hyperliquid.xyz/exchange，判断totalSz和status是否符合预期
#    且报文如下
# {
#     "status": "ok",
#     "response": {
#         "type": "order",
#         "data": {
#             "statuses": [
#                 {
#                     "filled": {
#                         "totalSz": "0.0028",
#                         "avgPx": "4808.5",
#                         "oid": 374078790396
#                     }
#                 }
#             ]
#         }
#     }
# }

from __future__ import annotations

import argparse
import os
import subprocess
import signal
import sys
import time
from typing import Any, Dict, List

try:
    from .frontend_arbitrage import (
        MarketConfig,
        XYZ_TRADER_USER_DATA_DIR,
        XYZ_VIEWER_USER_DATA_DIR,
        TradeXyzSeleniumClient,
        build_default_site_configs,
        build_driver,
    )
except ImportError:
    from frontend_arbitrage import (
        MarketConfig,
        XYZ_TRADER_USER_DATA_DIR,
        XYZ_VIEWER_USER_DATA_DIR,
        TradeXyzSeleniumClient,
        build_default_site_configs,
        build_driver,
    )


_SHUTDOWN_REQUESTED = False


def _handle_shutdown(signum, _frame) -> None:
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = True
    print(f"[trade_xyz] received signal {signum}, shutting down...")


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)


def _list_process_table() -> list[tuple[int, int, str]]:
    """Read current process table as pid, ppid, command rows."""
    try:
        result = subprocess.run(
            ["ps", "-ax", "-o", "pid=,ppid=,command="],
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return []

    rows: list[tuple[int, int, str]] = []
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) != 3:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        rows.append((pid, ppid, parts[2]))
    return rows


def _collect_descendant_pids(root_pids: set[int], process_rows: list[tuple[int, int, str]]) -> set[int]:
    """Expand a PID set to include all descendants."""
    descendants = set(root_pids)
    changed = True
    while changed:
        changed = False
        for pid, ppid, _command in process_rows:
            if pid in descendants:
                continue
            if ppid in descendants:
                descendants.add(pid)
                changed = True
    return descendants


def _terminate_pids(pids: set[int], sig: int) -> None:
    for pid in sorted(pids):
        if pid in {os.getpid(), os.getppid()}:
            continue
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            continue
        except Exception:
            continue


def _cleanup_browser_processes_for_profile(profile_path: str, reason: str) -> None:
    """Kill Chrome/WebDriver processes tied to a specific Chrome user data dir."""
    normalized_profile = os.path.expanduser(profile_path).strip()
    if not normalized_profile:
        return

    process_rows = _list_process_table()
    if not process_rows:
        return

    matching_roots: set[int] = set()
    for pid, _ppid, command in process_rows:
        if normalized_profile in command:
            matching_roots.add(pid)

    if not matching_roots:
        return

    matching_pids = _collect_descendant_pids(matching_roots, process_rows)
    print(
        f"[trade_xyz] cleaning browser processes for profile={normalized_profile} "
        f"reason={reason} pids={sorted(matching_pids)}"
    )
    _terminate_pids(matching_pids, signal.SIGTERM)
    time.sleep(1.0)

    remaining_rows = _list_process_table()
    remaining_pids = {
        pid
        for pid, _ppid, command in remaining_rows
        if pid in matching_pids or normalized_profile in command
    }
    if remaining_pids:
        print(
            f"[trade_xyz] force killing remaining browser processes "
            f"for profile={normalized_profile} pids={sorted(remaining_pids)}"
        )
        _terminate_pids(remaining_pids, signal.SIGKILL)


def parse_args() -> argparse.Namespace:
    """解析 trade.xyz 脚本命令行参数。

    默认 user_data_dir 指向 xyz-trader。
    如果只是看价格，也可以手动传入 XYZ_VIEWER_USER_DATA_DIR。
    """
    parser = argparse.ArgumentParser(description="Operate trade.xyz browser tabs and fetch live prices")
    parser.add_argument(
        "--mode",
        choices=["open", "once", "watch", "debug", "trade"],
        default="watch",
        help="open: only open browser tabs, once: fetch one round of prices, watch: keep polling, debug: dump DOM inventory, trade: submit one UI order",
    )
    parser.add_argument(
        "--symbols",
        default="GOLD",
        help="Comma separated trade.xyz symbols to enable",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Polling interval in seconds for watch mode",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Total runtime in seconds for watch mode; 0 means run forever",
    )
    parser.add_argument(
        "--auto-connect",
        action="store_true",
        help="Try wallet connect flow if needed",
    )
    parser.add_argument(
        "--no-manual-wallet-confirm",
        action="store_true",
        help="After OKX flow, do not wait for Enter (for automated / CI runs)",
    )
    parser.add_argument(
        "--prepare-inputs",
        action="store_true",
        help="Fill quantity input on each market tab",
    )
    parser.add_argument(
        "--action",
        choices=["buy", "sell", "close-long", "close-short"],
        help="For trade mode: submit a market long/short or a reduce-only market close",
    )
    parser.add_argument(
        "--quantity",
        type=float,
        default=None,
        help="Order quantity to use for trade mode; defaults to the market's configured quantity",
    )
    parser.add_argument(
        "--execute-order",
        action="store_true",
        help="Actually click the trade.xyz order submit button in trade mode; default is dry-run only",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Also persist fetched prices into the database",
    )
    parser.add_argument(
        "--user-data-dir",
        default=XYZ_TRADER_USER_DATA_DIR,
        help=(
            "Chrome user data dir for this trade.xyz browser instance. "
            f"viewer={XYZ_VIEWER_USER_DATA_DIR}, trader={XYZ_TRADER_USER_DATA_DIR}"
        ),
    )
    return parser.parse_args()


def build_client(
    symbols: List[str],
    user_data_dir: str | None = None,
    dry_run: bool = True,
    market_overrides: Dict[str, Dict[str, Any]] | None = None,
) -> TradeXyzSeleniumClient:
    """创建 trade.xyz 浏览器 client。"""
    site_configs = build_default_site_configs()
    driver = build_driver(user_data_dir=user_data_dir)
    config = site_configs["trade_xyz"]
    normalized_overrides = {
        symbol.upper(): data
        for symbol, data in (market_overrides or {}).items()
    }
    for symbol, data in normalized_overrides.items():
        existing = config.markets.get(symbol)
        config.markets[symbol] = MarketConfig(
            symbol=symbol,
            path=str(data.get("path") or (existing.path if existing else f"?market={symbol}")),
            quantity=float(data.get("quantity", existing.quantity if existing else 1.0)),
        )
    config.markets = {
        symbol: market
        for symbol, market in config.markets.items()
        if symbol.upper() in symbols
    }
    return TradeXyzSeleniumClient(
        config=config,
        driver=driver,
        dry_run=dry_run,
        user_data_dir=user_data_dir,
    )


def print_snapshots(snapshots) -> None:
    """把抓到的价格快照打印到终端。"""
    for item in snapshots:
        print(
            f"{item.captured_at} {item.symbol:<6} "
            f"ask={item.ask:<12.3f} bid={item.bid:<12.3f} spread={item.spread:<12.3f} "
            f"url={item.page_url}"
        )


def run_auto_connect_flow(
    client: TradeXyzSeleniumClient,
    manual_confirm: bool = True,
) -> bool:
    """Orchestrate trade.xyz wallet connect using atomic page actions."""

    def _get_primary_connect_state() -> dict:
        if not client.switch_to_primary_tab():
            return {"url": "", "title": "", "matches": [], "switch_failed": True}
        try:
            return client.driver.execute_script(
                """
                const textOf = (el) => ((el.innerText || el.textContent || '') + '').trim().replace(/\\s+/g, ' ');
                const nodes = Array.from(document.querySelectorAll('button, [role="button"], a[role="button"], div[tabindex], span[tabindex]'));
                const matches = nodes
                  .map((el) => textOf(el))
                  .filter(Boolean)
                  .filter((text) => /connect wallet|connect|connected|disconnect/i.test(text))
                  .slice(0, 20);
                return { url: window.location.href, title: document.title, matches };
                """
            ) or {}
        except Exception as exc:
            return {"url": "", "title": "", "matches": [], "error": repr(exc)}

    def _dump_primary_connect_state(label: str) -> None:
        state = _get_primary_connect_state()
        if state.get("switch_failed"):
            print(f"[trade_xyz] {label}: unable to switch to primary tab")
            return
        if state.get("error"):
            print(f"[trade_xyz] {label}: state dump failed {state.get('error')}")
            return
        print(f"[trade_xyz] {label}: url={state.get('url')!r} title={state.get('title')!r} matches={state.get('matches')}")

    def _primary_still_requires_connect() -> bool:
        state = _get_primary_connect_state()
        matches = [str(item).lower() for item in (state.get("matches") or [])]
        return any("connect wallet" in item or item == "connect" for item in matches)

    def _dump_window_urls(label: str) -> None:
        urls: list[str] = []
        titles: list[str] = []
        current_handle = None
        try:
            current_handle = client.driver.current_window_handle
        except Exception:
            current_handle = None
        for handle in list(getattr(client.driver, "window_handles", []) or []):
            try:
                client.driver.switch_to.window(handle)
                urls.append((client.driver.get_current_url() or "").strip())
                titles.append((client.driver.get_title() or "").strip())
            except Exception:
                urls.append("<switch_failed>")
                titles.append("<switch_failed>")
        if current_handle:
            try:
                client.driver.switch_to.window(current_handle)
            except Exception:
                pass
        print(f"[trade_xyz] {label}: window_urls={urls} window_titles={titles}")

    def _check_okx_provider(label: str) -> bool:
        """Return True if window.okxwallet is injected into the primary tab."""
        if not client.switch_to_primary_tab():
            return False
        try:
            injected = client.driver.execute_script(
                "return typeof window.okxwallet !== 'undefined' && window.okxwallet !== null"
            )
            print(f"[trade_xyz] {label}: window.okxwallet injected={injected}")
            return bool(injected)
        except Exception as exc:
            print(f"[trade_xyz] {label}: okxwallet check failed {exc!r}")
            return False

    def _modal_state_after_okx(label: str) -> str:
        """Snapshot the page state right after clicking OKX.

        Returns 'closed' | 'qr_code' | 'okx_still_visible' | 'unknown'.
        """
        if not client.switch_to_primary_tab():
            return "unknown"
        try:
            result = client.driver.execute_script(
                """
                const seen = new Set();
                const queue = [document];
                let hasModal = false, hasOkx = false, hasQr = false;
                while (queue.length) {
                  const root = queue.shift();
                  if (!root || seen.has(root)) continue;
                  seen.add(root);
                  if (!root.querySelectorAll) continue;
                  for (const el of Array.from(root.querySelectorAll('*'))) {
                    if (el.shadowRoot) queue.push(el.shadowRoot);
                    const style = window.getComputedStyle(el);
                    if (style.display === 'none' || style.visibility === 'hidden') continue;
                    const rect = el.getBoundingClientRect();
                    if (rect.width === 0 && rect.height === 0) continue;
                    const t = ((el.innerText || el.textContent || '') + '').trim().toLowerCase();
                    if (t.includes('okx')) hasOkx = true;
                    if (t.includes('qr') || el.tagName.toLowerCase() === 'canvas'
                        || el.tagName.toLowerCase() === 'img' && t === '') hasQr = true;
                    if (t.includes('connect') && t.includes('wallet')) hasModal = true;
                  }
                }
                return { hasModal, hasOkx, hasQr };
                """
            ) or {}
            has_okx = result.get("hasOkx", False)
            has_qr = result.get("hasQr", False)
            if has_okx:
                state = "okx_still_visible"
            elif has_qr:
                state = "qr_code"
            else:
                state = "closed"
            print(f"[trade_xyz] {label}: modal_state={state!r} detail={result}")
            return state
        except Exception as exc:
            print(f"[trade_xyz] {label}: modal state check failed {exc!r}")
            return "unknown"

    def _attempt_connect_once(label: str) -> bool:
        """Pre-unlock OKX extension → refresh page → Connect Wallet → OKX → confirm."""
        if not client.switch_to_primary_tab():
            print(f"[trade_xyz] {label}: unable to switch to primary tab")
            return False

        # Step 1: Reload the page so the OKX extension content script re-runs and
        # re-injects window.okxwallet. If the extension was locked at page-load time,
        # the provider object may be absent and AppKit falls back to WalletConnect QR.
        if not _check_okx_provider(label):
            print(f"[trade_xyz] {label}: window.okxwallet missing — reloading page to reinject ...")
            try:
                client.driver.refresh()
                time.sleep(client.config.page_load_seconds)
            except Exception as exc:
                print(f"[trade_xyz] {label}: page reload failed {exc!r}")
            _check_okx_provider(f"{label}: after_reload")

        # Step 2: Open the wallet selector modal.
        print(f"[trade_xyz] {label}: clicking Connect Wallet ...")
        connect_clicked = client.click_connect_wallet_button(timeout_seconds=20.0)
        print(f"[trade_xyz] {label}: Connect Wallet clicked: {connect_clicked}")
        if not connect_clicked:
            return False

        # Step 3: Wait for modal to render, then click OKX.
        time.sleep(client.config.connect_wait_seconds)
        handles_before_okx = set(client.driver.window_handles)
        wallet_clicked = client.click_okx_wallet_option(timeout=15.0)
        print(f"[trade_xyz] {label}: OKX option clicked: {wallet_clicked}")
        if not wallet_clicked:
            _dump_primary_connect_state(f"{label}: after_wallet_click_failed")
            return False

        # Diagnostic: check modal state after selecting OKX.
        modal_state = _modal_state_after_okx(f"{label}: post_okx_click")
        _dump_window_urls(f"{label}: after_okx_option")

        # trade.xyz uses Privy which shows "Sign to verify" + WalletConnect QR after
        # selecting OKX. This is the expected state — OKX extension will receive the
        # pairing request. Do NOT abort here; give WalletConnect time to pair.
        if modal_state == "closed":
            print(f"[trade_xyz] {label}: modal closed unexpectedly after OKX click, aborting")
            return False

        # Step 4: Wait for WalletConnect pairing then open extension to confirm.
        time.sleep(1.0)
        print(f"[trade_xyz] {label}: modal_state={modal_state!r}, opening OKX extension and waiting for confirm (timeout=60s)")
        confirmed = client.confirm_okx_extension_window(
            handles_before_okx,
            timeout=60.0,
            open_extension_first=True,
            wc_pairing_wait_seconds=6.0,
        )
        print(f"[trade_xyz] {label}: OKX extension auto-confirm: {confirmed}")
        _dump_window_urls(f"{label}: after_okx_confirm")
        _dump_primary_connect_state(f"{label}: after_okx_confirm")
        return confirmed

    if not client.switch_to_primary_tab():
        print("[trade_xyz] unable to switch to primary tab before wallet flow")
        return False

    print(f"[trade_xyz] current url before connect: {client.driver.get_current_url()}")
    _dump_primary_connect_state("before_connect")

    confirmed = _attempt_connect_once("attempt1")

    # If still showing Connect Wallet, try once more (e.g. modal was dismissed without connecting)
    if not confirmed and _primary_still_requires_connect():
        print("[trade_xyz] primary page still shows Connect after attempt1; retrying ...")
        confirmed = _attempt_connect_once("attempt2")

    if manual_confirm and not confirmed:
        input("[trade_xyz] OKX Wallet 连接并签名完成后，按回车继续...")
    return confirmed and not _primary_still_requires_connect()


def main() -> int:
    """trade.xyz 脚本入口。"""
    global _SHUTDOWN_REQUESTED
    args = parse_args()
    install_signal_handlers()
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    profile = os.path.expanduser(args.user_data_dir)
    print(f"[trade_xyz] chrome user_data_dir={profile}")
    _cleanup_browser_processes_for_profile(profile, reason="main_start")

    client = build_client(symbols, profile, dry_run=not args.execute_order)
    client.open_market_tabs()

    if args.auto_connect:
        run_auto_connect_flow(client, manual_confirm=not args.no_manual_wallet_confirm)
    if args.prepare_inputs:
        client.prepare_market_inputs()

    try:
        if args.mode == "open":
            while not _SHUTDOWN_REQUESTED:
                time.sleep(60)
            return 0

        if args.mode == "debug":
            for path in client.debug_dump():
                print(path)
            return 0

        if args.mode == "once":
            print_snapshots(client.capture_all_quotes(persist=args.persist))
            return 0

        if args.mode == "trade":
            if not args.action:
                raise ValueError("[trade_xyz] trade mode requires --action")
            symbol = symbols[0]
            if args.action == "buy":
                result = client.place_order(
                    symbol=symbol,
                    side="buy",
                    quantity=args.quantity,
                    order_kind="market-ui",
                    notes="trade_xyz_trade_mode",
                )
            elif args.action == "sell":
                result = client.place_order(
                    symbol=symbol,
                    side="sell",
                    quantity=args.quantity,
                    order_kind="market-ui",
                    notes="trade_xyz_trade_mode",
                )
            elif args.action == "close-long":
                result = client.close_position(
                    symbol=symbol,
                    position_side="long",
                    quantity=args.quantity,
                    notes="trade_xyz_trade_mode",
                )
            else:
                result = client.close_position(
                    symbol=symbol,
                    position_side="short",
                    quantity=args.quantity,
                    notes="trade_xyz_trade_mode",
                )
            print(f"[trade_xyz] trade result: {result}")
            return 0

        started = time.time()
        while not _SHUTDOWN_REQUESTED:
            print_snapshots(client.capture_all_quotes(persist=args.persist))
            if args.duration > 0 and (time.time() - started) >= args.duration:
                break
            time.sleep(args.interval)
        return 0
    finally:
        try:
            client.driver.quit()
        except Exception:
            pass
        _cleanup_browser_processes_for_profile(profile, reason="client_quit")


if __name__ == "__main__":
    sys.exit(main())
