"""SeleniumBase driver for Omni price observation only."""

from __future__ import annotations

import argparse
import os
import select
import signal
import subprocess
import sys
import time
from typing import List


### 第一阶段：Connect Wallet
# 1. 页面上找 "Connect Wallet" 按钮（如果有的话）
# 2. 点击 "Connect Wallet"
# 3. 等待钱包选择器出现
# 4. 如果对话框里没有 "OKX Wallet" 选项，说明钱包列表未正常加载：
#    - 刷新 Omni 页面
#    - 回到主页面重新点击 "Connect Wallet"
#    - 再次等待钱包选择器出现
# 5. 选择 "OKX" 选项
# 6. **自动打开新 tab** → OKX 扩展窗口打开

# ### 第二阶段：OKX 扩展确认（第一次）
# 1. 在 OKX 扩展中输入密码（如果需要）
# 2. 点击确认/解锁按钮
# 3. 完成签名确认

# ### 第三阶段：回到主页面 + Authenticate
# 1. 切回主标签页
# 2. 找到并点击 "Authenticate" 按钮
# 3. **自动打开新 tab** → OKX 扩展窗口再次打开

# ### 第四阶段：OKX 扩展确认（第二次）
# 1. 在 OKX 扩展中输入密码（如果需要）
# 2. 点击确认/签名按钮
# 3. 完成最终签名确认

# ### 市价下单功能
# 1. 切换到 "Market" tab
# 2. 找到并点击 "Buy" / "Sell" tab
# 3. 确保是标的数量而非usd：确保不是<span class="max-w-12 text-ellipsis text-nowrap overflow-hidden">$</span>，而是<span class="max-w-12 text-ellipsis text-nowrap overflow-hidden">XAUT</span>这样的名称，否则点击该标签确保是数量而非usd
# 4. 输入数量
# 5. 点击 "Buy"/ "Sell" 按钮
# 6. 监控get请求 https://omni.variational.io/api/orders/v2?status=pending&order_by=created_at&order=desc  resulst列表最新一条元素 created_at在下单时间后10s之类且qty=下单金额，元素instrument.underlying=标的名称
#    记录order信息：报文如下
#  {
#     "pagination": {
#         "last_page": {
#             "limit": "100",
#             "offset": "0"
#         },
#         "next_page": null,
#         "object_count": 3
#     },
#     "result": [
#         {
#             "cancel_reason": null,
#             "clearing_status": null,
#             "company": "e661b02d-7bfc-43ea-b858-248e9c38a04c",
#             "created_at": "2026-04-08T06:16:16.274566Z",
#             "execution_timestamp": null,
#             "failed_risk_checks": [],
#             "instrument": {
#                 "funding_interval_s": 3600,
#                 "instrument_type": "perpetual_future",
#                 "settlement_asset": "USDC",
#                 "underlying": "ENA"
#             },
#             "is_auto_resize": false,
#             "is_reduce_only": false,
#             "limit_price": null,
#             "mark_price": null,
#             "order_id": "2f85d317-3ade-4612-8fdb-6bb933f164da",
#             "order_type": "market",
#             "pool_location": "be9ae810-9968-46e5-b34e-fabcb17eadfa",
#             "price": null,
#             "qty": "20",
#             "rfq_id": "03827c99-ca4b-472c-9bf3-0a738070a22b",
#             "side": "sell",
#             "slippage_limit": "0.003000000000",
#             "status": "pending",
#             "tif": "fill_or_kill",
#             "trigger_price": null,
#             "use_mark_price": false
#         },
#         {
#             "cancel_reason": null,
#             "clearing_status": null,
#             "company": "e661b02d-7bfc-43ea-b858-248e9c38a04c",
#             "created_at": "2026-04-08T05:31:40.030171Z",
#             "execution_timestamp": null,
#             "failed_risk_checks": [],
#             "instrument": {
#                 "funding_interval_s": 3600,
#                 "instrument_type": "perpetual_future",
#                 "settlement_asset": "USDC",
#                 "underlying": "BASED"
#             },
#             "is_auto_resize": false,
#             "is_reduce_only": false,
#             "limit_price": "0.0646",
#             "mark_price": null,
#             "order_id": "fd2e3746-d836-456b-a8d1-c818982c90ec",
#             "order_type": "limit",
#             "pool_location": "be9ae810-9968-46e5-b34e-fabcb17eadfa",
#             "price": null,
#             "qty": "4867",
#             "rfq_id": "eff8c82a-795d-4a22-b8a9-96fe80d4ffb8",
#             "side": "sell",
#             "slippage_limit": "0.003000000000",
#             "status": "pending",
#             "tif": "good_til_canceled",
#             "trigger_price": null,
#             "use_mark_price": false
#         },
#         {
#             "cancel_reason": null,
#             "clearing_status": null,
#             "company": "e661b02d-7bfc-43ea-b858-248e9c38a04c",
#             "created_at": "2026-04-08T05:31:40.030171Z",
#             "execution_timestamp": null,
#             "failed_risk_checks": [],
#             "instrument": {
#                 "funding_interval_s": 3600,
#                 "instrument_type": "perpetual_future",
#                 "settlement_asset": "USDC",
#                 "underlying": "BASED"
#             },
#             "is_auto_resize": false,
#             "is_reduce_only": true,
#             "limit_price": null,
#             "mark_price": null,
#             "order_id": "c4cdbfea-7cad-46d9-bce9-3fc2eb3a5524",
#             "order_type": "stop_loss",
#             "pool_location": "be9ae810-9968-46e5-b34e-fabcb17eadfa",
#             "price": null,
#             "qty": "4867",
#             "rfq_id": "7bf6b4e3-f98b-4b52-b09f-79f811cc2638",
#             "side": "buy",
#             "slippage_limit": null,
#             "status": "pending",
#             "tif": "good_til_canceled",
#             "trigger_price": "0.072",
#             "use_mark_price": true
#         }
#     ]
# }
# 7. 记录position信息，报文如下
# [
#     {
#         "position_info": {
#             "company": "e661b02d-7bfc-43ea-b858-248e9c38a04c",
#             "counterparty": "3065dd5e-49eb-41b6-be2c-283d7cf19726",
#             "instrument": {
#                 "instrument_type": "perpetual_future",
#                 "underlying": "ENA",
#                 "funding_interval_s": 3600,
#                 "settlement_asset": "USDC"
#             },
#             "pool_location": "be9ae810-9968-46e5-b34e-fabcb17eadfa",
#             "updated_at": "2026-04-08T06:16:14.789Z",
#             "opened_at": "2026-04-08T06:16:14.789Z",
#             "qty": "-20",
#             "avg_entry_price": "0.09154",
#             "prev_avg_entry_price": null,
#             "prev_qty": null,
#             "taker_qty": "-20",
#             "last_local_sequence": 4
#         },
#         "pending_order_counts": {},
#         "price_info": {
#             "price": "0.09158",
#             "native_price": "0.9993",
#             "delta": "1",
#             "gamma": "0",
#             "theta": "0",
#             "vega": "0",
#             "rho": "0",
#             "iv": "0",
#             "underlying_price": "0.09165",
#             "interest_rate": "0.0000500000000000000023960868",
#             "timestamp": "2026-04-08T06:16:17.140389Z"
#         },
#         "value": "-1.8316",
#         "upnl": "-0.0008",
#         "rpnl": "0",
#         "cum_funding": "0",
#         "estimated_liquidation_price": "33.2964200547699"
#     },
#     {
#         "position_info": {
#             "company": "e661b02d-7bfc-43ea-b858-248e9c38a04c",
#             "counterparty": "3065dd5e-49eb-41b6-be2c-283d7cf19726",
#             "instrument": {
#                 "instrument_type": "perpetual_future",
#                 "underlying": "BASED",
#                 "funding_interval_s": 3600,
#                 "settlement_asset": "USDC"
#             },
#             "pool_location": "be9ae810-9968-46e5-b34e-fabcb17eadfa",
#             "updated_at": "2026-04-08T01:23:37.967Z",
#             "opened_at": "2026-04-03T14:58:20.141Z",
#             "qty": "-1709",
#             "avg_entry_price": "0.073759698891",
#             "prev_avg_entry_price": "0.073759698891",
#             "prev_qty": "-2163.000000000000",
#             "taker_qty": "-1709",
#             "last_local_sequence": 14
#         },
#         "pending_order_counts": {},
#         "price_info": {
#             "price": "0.06123",
#             "native_price": "0.9992",
#             "delta": "1",
#             "gamma": "0",
#             "theta": "0",
#             "vega": "0",
#             "rho": "0",
#             "iv": "0",
#             "underlying_price": "0.06128",
#             "interest_rate": "0.0000500000000000000023960868",
#             "timestamp": "2026-04-08T06:16:14.246593Z"
#         },
#         "value": "-104.64207",
#         "upnl": "21.413255404719",
#         "rpnl": "224.944142",
#         "cum_funding": "-1.860861",
#         "estimated_liquidation_price": "0.4498179479115185"
#     }
# ]



try:
    from .frontend_arbitrage import (
        OMNI_TRADER_USER_DATA_DIR,
        OMNI_VIEWER_USER_DATA_DIR,
        OmniSeleniumClient,
        build_default_site_configs,
        build_driver,
    )
except ImportError:
    from frontend_arbitrage import (
        OMNI_TRADER_USER_DATA_DIR,
        OMNI_VIEWER_USER_DATA_DIR,
        OmniSeleniumClient,
        build_default_site_configs,
        build_driver,
    )


_SHUTDOWN_REQUESTED = False
_MAX_PROCESS_RESTARTS = 2
_CHILD_ENV_FLAG = "OMNI_CHILD_PROCESS"


def _handle_shutdown(signum, _frame) -> None:
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = True
    print(f"[omni] received signal {signum}, shutting down...")


def install_signal_handlers() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)


def parse_args() -> argparse.Namespace:
    """解析 Omni 脚本命令行参数。

    默认 user_data_dir 指向 omni-trader。
    如果只是看价格，也可以手动传入 OMNI_VIEWER_USER_DATA_DIR。
    """
    parser = argparse.ArgumentParser(description="Operate Omni browser tabs and fetch live prices")
    parser.add_argument(
        "--mode",
        choices=["open", "once", "watch", "debug", "trade"],
        default="watch",
        help="open: only open browser tabs, once: fetch one round of prices, watch: keep polling, debug: dump DOM inventory, trade: submit one UI order",
    )
    parser.add_argument(
        "--symbols",
        default="XAUT",
        help="Comma separated Omni symbols to enable",
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
        help=(
            "Try the Omni wallet connection flow (Connect → OKX → extension). "
            "Optional env: OKX_WALLET_UNLOCK_PASSWORD when the extension asks for password."
        ),
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
        help="For trade mode: submit a market buy/sell or a reduce-only market close",
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
        help="Actually click the Omni order submit button in trade mode; default is dry-run only",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Also persist fetched prices into the database",
    )
    parser.add_argument(
        "--user-data-dir",
        default=OMNI_TRADER_USER_DATA_DIR,
        help=(
            "Chrome user data dir for this Omni browser instance. "
            f"viewer={OMNI_VIEWER_USER_DATA_DIR}, trader={OMNI_TRADER_USER_DATA_DIR}"
        ),
    )
    return parser.parse_args()


def build_client(
    symbols: List[str],
    user_data_dir: str | None = None,
    dry_run: bool = True,
) -> OmniSeleniumClient:
    """创建 Omni 浏览器 client。

    symbols 决定要打开哪些市场标签页；
    user_data_dir 决定使用看盘空间还是下单空间。
    """
    site_configs = build_default_site_configs()
    driver = build_driver(user_data_dir=user_data_dir)
    config = site_configs["omni"]
    config.markets = {
        symbol: market
        for symbol, market in config.markets.items()
        if symbol.upper() in symbols
    }
    return OmniSeleniumClient(
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


def _iter_window_urls(client: OmniSeleniumClient) -> list[str]:
    """Collect current browser window URLs safely."""
    urls: list[str] = []
    current_handle = None
    try:
        current_handle = client.driver.current_window_handle
    except Exception:
        current_handle = None

    for handle in list(getattr(client.driver, "window_handles", []) or []):
        try:
            client.driver.switch_to.window(handle)
            url = (client.driver.get_current_url() or "").strip()
            if url:
                urls.append(url)
        except Exception:
            continue

    if current_handle:
        try:
            client.driver.switch_to.window(current_handle)
        except Exception:
            pass
    return urls


def _has_okx_popup_init_crash(client: OmniSeleniumClient) -> bool:
    """Treat popup-init pages as a crash case that requires a full browser restart."""
    urls = _iter_window_urls(client)
    crashed = any("/popup-init.html" in url for url in urls)
    if crashed:
        print(f"[omni] detected OKX popup-init crash urls={urls}")
    return crashed


def _browser_is_healthy(client: OmniSeleniumClient) -> bool:
    """Check whether the webdriver session is still responsive."""
    try:
        handles = list(client.driver.window_handles or [])
        if not handles:
            print("[omni] browser unhealthy: no window handles")
            return False
        current_handle = client.driver.current_window_handle
        if current_handle not in handles:
            print(f"[omni] browser unhealthy: current handle missing {current_handle!r}")
            return False
        current_url = (client.driver.get_current_url() or "").strip()
        if not current_url:
            print("[omni] browser unhealthy: empty current url")
            return False
        return True
    except Exception as exc:
        print(f"[omni] browser unhealthy: webdriver exception {exc!r}")
        return False


def _should_restart_browser(client: OmniSeleniumClient) -> bool:
    """Decide whether the whole browser should be restarted."""
    return (not _browser_is_healthy(client)) or _has_okx_popup_init_crash(client)


def _safe_quit_client(client: OmniSeleniumClient) -> None:
    """Best-effort webdriver shutdown."""
    profile_path = os.path.expanduser(getattr(client, "user_data_dir", "") or "")
    try:
        client.driver.quit()
    except Exception:
        pass
    if profile_path:
        _cleanup_browser_processes_for_profile(profile_path, reason="client_quit")


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
        f"[omni] cleaning browser processes for profile={normalized_profile} "
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
            f"[omni] force killing remaining browser processes "
            f"for profile={normalized_profile} pids={sorted(remaining_pids)}"
        )
        _terminate_pids(remaining_pids, signal.SIGKILL)


def _kill_process_force(pid: int, reason: str) -> None:
    """Force-kill a stuck child process with SIGKILL."""
    try:
        os.kill(pid, signal.SIGKILL)
        print(f"[omni] kill -9 child pid={pid} reason={reason}")
    except ProcessLookupError:
        pass


def run_auto_connect_flow(
    client: OmniSeleniumClient,
    manual_confirm: bool = True,
) -> bool:
    """Orchestrate the Omni wallet/auth flow using atomic page actions."""
    if not client.switch_to_primary_tab():
        print("[omni] unable to switch to primary tab before wallet flow")
        return False

    print(f"[omni] current url before connect: {client.driver.get_current_url()}")
    print("[omni] trying to click top-right Connect Wallet ...")
    connect_clicked = client.click_connect_wallet_button(timeout_seconds=30.0)
    if not connect_clicked:
        print("[omni] Connect Wallet not found, trying Authenticate directly ...")
        handles_before_auth = set(client.driver.window_handles)
        authenticated = client.click_authenticate_button(timeout_seconds=12.0)
        print(f"[omni] Authenticate clicked without reconnect: {authenticated}")
        if authenticated:
            second_confirmed = client.confirm_okx_extension_window(
                handles_before_auth,
                timeout=45.0,
            )
            print(f"[omni] OKX extension second confirm: {second_confirmed}")
        return authenticated

    wallet_clicked = False
    handles_before_okx: set[str] = set(client.driver.window_handles)
    for attempt in range(2):
        if attempt == 0:
            print("[omni] Connect Wallet clicked, waiting for wallet options ...")
        else:
            print("[omni] OKX option missing in wallet dialog, refreshing page and retrying Connect Wallet ...")
            refreshed = client.refresh_primary_page()
            print(f"[omni] primary page refreshed: {refreshed}")
            if not refreshed:
                continue
            reconnect_clicked = client.click_connect_wallet_button(timeout_seconds=20.0)
            print(f"[omni] Connect Wallet clicked after refresh: {reconnect_clicked}")
            if not reconnect_clicked:
                continue
        time.sleep(client.config.connect_wait_seconds)
        handles_before_okx = set(client.driver.window_handles)
        wallet_clicked = client.click_okx_wallet_option(timeout=12.0)
        print(f"[omni] OKX option clicked (attempt {attempt + 1}): {wallet_clicked}")
        time.sleep(1.0)
        if wallet_clicked:
            break

    first_confirmed = False
    if wallet_clicked:
        first_confirmed = client.confirm_okx_extension_window(
            handles_before_okx,
            timeout=45.0,
            open_extension_first=True,
        )
        print(f"[omni] OKX extension auto-confirm: {first_confirmed}")

    if first_confirmed:
        time.sleep(1.0)
        client.switch_to_primary_tab()
        handles_before_auth = set(client.driver.window_handles)
        authenticated = client.click_authenticate_button(timeout_seconds=20.0)
        print(f"[omni] Authenticate clicked: {authenticated}")
        if authenticated:
            second_confirmed = client.confirm_okx_extension_window(
                handles_before_auth,
                timeout=45.0,
            )
            print(f"[omni] OKX extension second confirm: {second_confirmed}")

    if manual_confirm and not first_confirmed:
        input("[omni] OKX Wallet 连接并签名完成后，按回车继续...")
    return True


def worker_main() -> int:
    """Omni 子进程入口。

    open: 只打开浏览器和页面
    once/watch: 抓价格
    debug: 导出 DOM 信息，便于排查 selector
    """
    global _SHUTDOWN_REQUESTED
    args = parse_args()
    install_signal_handlers()
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]

    profile = os.path.expanduser(args.user_data_dir)
    print(f"[omni] chrome user_data_dir={profile}")
    _cleanup_browser_processes_for_profile(profile, reason="worker_start")
    client = build_client(
        symbols,
        profile,
        dry_run=not args.execute_order,
    )
    print(f"[omni] opening markets: {','.join(symbols)}")
    client.open_market_tabs()
    print("[omni] browser tabs opened")

    if args.auto_connect:
        connected = run_auto_connect_flow(
            client,
            manual_confirm=not args.no_manual_wallet_confirm,
        )
        print(f"[omni] auto_connect result: {connected}")
    if args.prepare_inputs:
        client.prepare_market_inputs()
        print("[omni] quantity inputs prepared")

    try:
        if args.mode == "open":
            while not _SHUTDOWN_REQUESTED:
                time.sleep(60)
            return 0

        if args.mode == "debug":
            for path in client.debug_dump():
                print(path)
            return 0

        if args.mode == "trade":
            if not args.action:
                raise ValueError("[omni] trade mode requires --action")
            symbol = symbols[0]
            if args.action == "buy":
                result = client.place_order(
                    symbol=symbol,
                    side="buy",
                    quantity=args.quantity,
                    order_kind="market-ui",
                    notes="omni_var_trade_mode",
                )
            elif args.action == "sell":
                result = client.place_order(
                    symbol=symbol,
                    side="sell",
                    quantity=args.quantity,
                    order_kind="market-ui",
                    notes="omni_var_trade_mode",
                )
            elif args.action == "close-long":
                result = client.close_position(
                    symbol=symbol,
                    position_side="long",
                    quantity=args.quantity,
                    notes="omni_var_trade_mode",
                )
            else:
                result = client.close_position(
                    symbol=symbol,
                    position_side="short",
                    quantity=args.quantity,
                    notes="omni_var_trade_mode",
                )
            print(f"[omni] trade result: {result}")
            return 0

        if args.mode == "once":
            print_snapshots(client.capture_all_quotes(persist=args.persist))
            return 0

        started = time.time()
        while not _SHUTDOWN_REQUESTED:
            print_snapshots(client.capture_all_quotes(persist=args.persist))
            if args.duration > 0 and (time.time() - started) >= args.duration:
                break
            time.sleep(args.interval)
        return 0
    finally:
        _safe_quit_client(client)


def supervisor_main() -> int:
    """Parent process: run worker as child, kill -9 on hang, then restart."""
    restart_count = 0
    inactivity_timeout = 45.0

    while restart_count <= _MAX_PROCESS_RESTARTS:
        env = os.environ.copy()
        env[_CHILD_ENV_FLAG] = "1"
        env["PYTHONUNBUFFERED"] = "1"
        child = subprocess.Popen(
            [sys.executable, *sys.argv],
            cwd=os.getcwd(),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        last_output_at = time.time()
        restart_reason = ""

        try:
            while True:
                if child.stdout is None:
                    break
                ready, _, _ = select.select([child.stdout], [], [], 1.0)
                if ready:
                    line = child.stdout.readline()
                    if line == "":
                        break
                    print(line, end="")
                    last_output_at = time.time()
                    if "detected OKX popup-init crash" in line:
                        restart_reason = "popup_init_crash"
                        _kill_process_force(child.pid, restart_reason)
                        break
                    if "browser unhealthy:" in line:
                        restart_reason = "browser_unhealthy"
                        _kill_process_force(child.pid, restart_reason)
                        break
                elif child.poll() is not None:
                    break
                elif (time.time() - last_output_at) >= inactivity_timeout:
                    restart_reason = f"no_output_for_{int(inactivity_timeout)}s"
                    _kill_process_force(child.pid, restart_reason)
                    break

            exit_code = child.wait()
        finally:
            if child.poll() is None:
                _kill_process_force(child.pid, "supervisor_cleanup")
                child.wait()

        if exit_code == 0 and not restart_reason:
            return 0

        restart_count += 1
        if restart_count > _MAX_PROCESS_RESTARTS:
            print(
                f"[omni] exceeded supervisor restart limit; "
                f"last_exit={exit_code} reason={restart_reason or 'child_exit'}"
            )
            return exit_code or 1

        print(
            f"[omni] restarting child after kill -9/exit "
            f"attempt={restart_count}/{_MAX_PROCESS_RESTARTS} "
            f"reason={restart_reason or f'exit_{exit_code}'}"
        )
        time.sleep(2)

    return 1


if __name__ == "__main__":
    if os.getenv(_CHILD_ENV_FLAG) == "1":
        sys.exit(worker_main())
    sys.exit(supervisor_main())
