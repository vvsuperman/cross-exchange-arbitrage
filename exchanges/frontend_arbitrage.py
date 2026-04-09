"""Frontend browser automation for quote collection and pair trading."""

from __future__ import annotations

import json
import os
import random
import re
import sys
import time
import uuid
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from seleniumbase import Driver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys

def _load_okx_wallet_env() -> None:
    """Load okx_wallet.env from the project root if env vars are not already set."""
    env_path = Path(__file__).parent.parent / "okx_wallet.env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value

_load_okx_wallet_env()

try:
    from .db_manager import ArbInfoDB, get_arb_info_db
except ImportError:
    from db_manager import ArbInfoDB, get_arb_info_db

try:
    from ..db.db_manager import OmniListenerDB
except ImportError:
    import sys

    _REPO_ROOT = Path(__file__).resolve().parent.parent
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))
    from db.db_manager import OmniListenerDB


# 浏览器 profile 常量：
# viewer_* 用于价格查看/抓价；
# *_trader 用于安装钱包、连接钱包、后续下单。
OMNI_VIEWER_USER_DATA_DIR = os.path.expanduser(
    "~/Library/Application Support/Google/Chrome/auto-user2"
)
XYZ_VIEWER_USER_DATA_DIR = os.path.expanduser(
    "~/Library/Application Support/Google/Chrome/auto-user-tradexyz-test"
)
OMNI_TRADER_USER_DATA_DIR = os.path.expanduser(
    "~/Library/Application Support/Google/Chrome/omni-trader"
)
XYZ_TRADER_USER_DATA_DIR = os.path.expanduser(
    "~/Library/Application Support/Google/Chrome/xyz-trader"
)

# 通用默认 profile：
# 如果上层脚本没有显式指定 user_data_dir，这里优先取环境变量，
# 否则回落到 Omni 的价格查看 profile。
DEFAULT_USER_DATA_DIR = os.path.expanduser(
    os.getenv("FRONTEND_ARB_USER_DATA_DIR", OMNI_VIEWER_USER_DATA_DIR)
)


def utc8_now_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Return timestamp string in local timezone used by the scripts."""
    return datetime.now().strftime(fmt)


def parse_number(text: str) -> float:
    """Extract numeric value from UI text such as '$3,311.23'."""
    cleaned = re.sub(r"[^0-9.\-]", "", text or "")
    if cleaned.count(".") > 1:
        first, *rest = cleaned.split(".")
        cleaned = first + "." + "".join(rest)
    if not cleaned or cleaned in {"-", ".", "-."}:
        raise ValueError(f"Unable to parse number from text: {text!r}")
    return float(cleaned)


def normalize_market_path(path: str) -> str:
    """Build absolute URL path fragments consistently."""
    return path.lstrip("/")


def load_json_env(env_name: str) -> Optional[dict]:
    """Load JSON object from an environment variable if present."""
    raw = os.getenv(env_name)
    if not raw:
        return None
    return json.loads(raw)


@dataclass
class SelectorSet:
    """页面元素选择器集合。

    同一字段可以放多个候选 selector，脚本会按顺序尝试，
    用于兼容 DOM 结构变化。
    """
    ask: List[str]
    bid: List[str]
    quantity_input: List[str]
    buy_button: List[str]
    sell_button: List[str]
    submit_button_buy: List[str] = field(default_factory=list)
    submit_button_sell: List[str] = field(default_factory=list)
    connect_wallet: List[str] = field(default_factory=list)
    wallet_option: List[str] = field(default_factory=list)


@dataclass
class MarketConfig:
    """单个交易市场配置。"""
    symbol: str
    path: str
    quantity: float


@dataclass
class ExchangeSiteConfig:
    """交易所页面配置。

    包含站点地址、交易市场列表，以及该交易所专属的 DOM selector。
    """
    name: str
    base_url: str
    selectors: SelectorSet
    markets: Dict[str, MarketConfig]
    connect_wait_seconds: float = 6.0
    page_load_seconds: float = 6.0
    tab_switch_delay_range: tuple[float, float] = (0.4, 1.2)


@dataclass
class QuoteSnapshot:
    """一次页面抓价得到的标准化报价快照。"""
    exchange: str
    symbol: str
    market: str
    bid: float
    ask: float
    bid_quantity: Optional[float]
    ask_quantity: Optional[float]
    input_quantity: Optional[float]
    captured_at: str
    minute: str
    page_url: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def spread(self) -> float:
        return self.bid - self.ask

    @property
    def mid_price(self) -> float:
        return (self.bid + self.ask) / 2


class FrontendExchangeClient:
    """基于 SeleniumBase 的前端交易所会话。

    一个 client 对应一个浏览器实例，可为多个 market 打开多个 tab。
    """

    def __init__(
        self,
        config: ExchangeSiteConfig,
        driver: Driver,
        db: Optional[ArbInfoDB] = None,
        dry_run: bool = False,
        user_data_dir: Optional[str] = None,
    ):
        self.config = config
        self.driver = driver
        self.db = db or get_arb_info_db()
        self.dry_run = dry_run
        self.tabs: Dict[str, Dict[str, Any]] = {}
        self.user_data_dir = os.path.expanduser(user_data_dir or "")

    def open_market_tabs(self) -> None:
        """Open one browser tab per configured market."""
        for index, market in enumerate(self.config.markets.values()):
            url = self._build_market_url(market.path)
            if index == 0 and not self.driver.window_handles:
                self.driver.get(url)
            elif index == 0:
                self.driver.switch_to.window(self.driver.window_handles[0])
                self.driver.get(url)
            else:
                self.driver.execute_script("window.open('');")
                self.driver.switch_to.window(self.driver.window_handles[-1])
                self.driver.get(url)

            self.tabs[self.driver.current_window_handle] = {
                "symbol": market.symbol,
                "market": market.path,
                "quantity": market.quantity,
                "url": url,
            }
            time.sleep(self.config.page_load_seconds)

    def install_quote_cache(self) -> None:
        """Install in-page quote cache hooks. Subclasses can override."""
        return None

    def capture_cached_quote(self, persist: bool = True) -> QuoteSnapshot:
        """Read the latest in-page cached quote from the current tab."""
        if not self.tabs:
            raise RuntimeError(f"[{self.config.name}] no market tabs opened")
        handle = list(self.tabs.keys())[0]
        return self.capture_quote(handle, persist=persist)

    def connect_wallet(self, manual_confirm: bool = True) -> bool:
        """Attempt connect-wallet flow with optional manual fallback."""
        if not self.tabs or not self.config.selectors.connect_wallet:
            return False

        first_handle = list(self.tabs.keys())[0]
        self.driver.switch_to.window(first_handle)
        clicked = self._click_first_visible(self.config.selectors.connect_wallet)
        if not clicked:
            return False

        time.sleep(self.config.connect_wait_seconds)
        handles_before = set(self.driver.window_handles)
        self._click_first_visible(self.config.selectors.wallet_option)
        confirmed = self._try_confirm_okx_extension_window(handles_before)
        if confirmed:
            print(f"[{self.config.name}] OKX extension window handled (auto confirm)")

        if manual_confirm and not confirmed:
            input(
                f"[{self.config.name}] 完成钱包连接/签名后按回车继续..."
            )
        return True

    def prepare_market_inputs(self) -> None:
        """Populate quantity input for all tabs."""
        for handle, info in self.tabs.items():
            self.driver.switch_to.window(handle)
            self._human_pause()
            self.set_quantity(info["quantity"])

    def set_quantity(self, quantity: float) -> None:
        """Set order quantity using the first matching quantity input selector."""
        selector = self._find_first_visible(self.config.selectors.quantity_input, timeout=25)
        if not selector:
            raise RuntimeError(f"[{self.config.name}] quantity input not found")

        field = self._find_element(selector)
        try:
            field.clear()
        except Exception:
            self._click_selector(selector)
            field = self._find_element(selector)
            field.send_keys("\ue009a")  # Ctrl/Cmd + A
            field.send_keys("\ue003")   # Delete
        field.send_keys(str(quantity))
        time.sleep(0.6)

    def capture_all_quotes(self, persist: bool = True) -> List[QuoteSnapshot]:
        """Capture quotes across all market tabs."""
        snapshots: List[QuoteSnapshot] = []
        for handle in list(self.tabs.keys()):
            snapshots.append(self.capture_quote(handle, persist=persist))
        return snapshots

    def capture_quote(self, handle: str, persist: bool = True) -> QuoteSnapshot:
        """Read ask/bid from a specific tab and persist both tick and minute rollup."""
        info = self.tabs[handle]
        self.driver.switch_to.window(handle)
        self._human_pause()

        ask_text = self._get_text(self.config.selectors.ask)
        bid_text = self._get_text(self.config.selectors.bid)
        ask = parse_number(ask_text)
        bid = parse_number(bid_text)

        snapshot = QuoteSnapshot(
            exchange=self.config.name,
            symbol=info["symbol"],
            market=info["market"],
            ask=ask,
            bid=bid,
            ask_quantity=info["quantity"],
            bid_quantity=info["quantity"],
            input_quantity=info["quantity"],
            captured_at=utc8_now_str("%Y-%m-%d %H:%M:%S"),
            minute=utc8_now_str("%Y-%m-%d %H:%M"),
            page_url=self.driver.get_current_url(),
            metadata={
                "ask_text": ask_text,
                "bid_text": bid_text,
                "window_handle": handle,
            },
        )
        if persist:
            self.persist_quote(snapshot)
        return snapshot

    def persist_quote(self, snapshot: QuoteSnapshot) -> None:
        """Store granular tick data and minute aggregation."""
        self.db.save_frontend_price_tick(
            exchange=snapshot.exchange,
            symbol=snapshot.symbol,
            market=snapshot.market,
            ask=snapshot.ask,
            bid=snapshot.bid,
            spread=snapshot.spread,
            captured_at=snapshot.captured_at,
            ask_quantity=snapshot.ask_quantity,
            bid_quantity=snapshot.bid_quantity,
            mid_price=snapshot.mid_price,
            input_quantity=snapshot.input_quantity,
            page_url=snapshot.page_url,
            metadata=snapshot.metadata,
        )
        self.db.save_minute_price(
            exchange=snapshot.exchange,
            symbol=snapshot.symbol,
            ask=snapshot.ask,
            ask_quantity=snapshot.ask_quantity or 0.0,
            bid=snapshot.bid,
            bid_quantity=snapshot.bid_quantity or 0.0,
            spread=snapshot.spread,
            minute_str=snapshot.minute,
            full_timestamp_str=snapshot.captured_at,
        )

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        confirm: bool = False,
        order_kind: str = "market-ui",
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place an order through the UI and write an execution log."""
        handle, info = self._get_handle_for_symbol(symbol)
        self.driver.switch_to.window(handle)
        self._human_pause()

        order_quantity = quantity if quantity is not None else info["quantity"]
        self.set_quantity(order_quantity)
        quote = self.capture_quote(handle)
        requested_price = quote.ask if side.lower() == "buy" else quote.bid
        client_order_id = str(uuid.uuid4())
        status = "simulated" if self.dry_run else "clicked"
        side_button_selectors = (
            self.config.selectors.buy_button
            if side.lower() == "buy"
            else self.config.selectors.sell_button
        )
        submit_selectors = (
            self.config.selectors.submit_button_buy
            if side.lower() == "buy"
            else self.config.selectors.submit_button_sell
        )

        action_metadata = {
            "confirm": confirm,
            "dry_run": self.dry_run,
            "selectors": {
                "side": side_button_selectors,
                "submit": submit_selectors,
            },
        }

        if not self.dry_run:
            clicked = self._click_first_visible(side_button_selectors)
            if not clicked:
                raise RuntimeError(
                    f"[{self.config.name}] unable to find {side} button for {symbol}"
                )
            time.sleep(0.5)
            if confirm and submit_selectors:
                submitted = self._click_first_visible(submit_selectors)
                if not submitted:
                    status = "awaiting_manual_confirm"
            elif confirm and not submit_selectors:
                status = "awaiting_manual_confirm"

        created_at = utc8_now_str("%Y-%m-%d %H:%M:%S")
        self.db.save_frontend_order_event(
            exchange=self.config.name,
            symbol=symbol,
            market=info["market"],
            side=side.lower(),
            quantity=order_quantity,
            status=status,
            order_kind=order_kind,
            requested_price=requested_price,
            clicked_price=requested_price,
            client_order_id=client_order_id,
            page_url=self.driver.get_current_url(),
            notes=notes,
            metadata=action_metadata,
            created_at=created_at,
        )

        return {
            "exchange": self.config.name,
            "symbol": symbol,
            "side": side.lower(),
            "quantity": order_quantity,
            "status": status,
            "requested_price": requested_price,
            "client_order_id": client_order_id,
        }

    def debug_dump(self, output_dir: str = "logs/frontend_debug") -> List[str]:
        """Dump visible inputs/buttons/test IDs for each tab to help tune selectors."""
        os.makedirs(output_dir, exist_ok=True)
        written_files: List[str] = []
        for handle, info in self.tabs.items():
            self.driver.switch_to.window(handle)
            self._human_pause()
            dom_snapshot = self.driver.execute_script(
                """
                const textOf = (el) => (el.innerText || el.textContent || '').trim().replace(/\\s+/g, ' ');
                return {
                  url: window.location.href,
                  title: document.title,
                  buttons: Array.from(document.querySelectorAll('button')).slice(0, 80).map((el) => ({
                    text: textOf(el),
                    testid: el.getAttribute('data-testid'),
                    classes: el.className,
                    disabled: !!el.disabled
                  })),
                  inputs: Array.from(document.querySelectorAll('input')).slice(0, 80).map((el) => ({
                    type: el.type,
                    placeholder: el.placeholder,
                    testid: el.getAttribute('data-testid'),
                    classes: el.className,
                    value: el.value
                  })),
                  testids: Array.from(document.querySelectorAll('[data-testid]')).slice(0, 120).map((el) => ({
                    tag: el.tagName,
                    testid: el.getAttribute('data-testid'),
                    text: textOf(el).slice(0, 120)
                  }))
                };
                """
            )
            file_name = (
                f"{self.config.name}_{info['symbol'].lower()}_{int(time.time())}.json"
            )
            file_path = os.path.join(output_dir, file_name)
            with open(file_path, "w", encoding="utf-8") as handle_fp:
                json.dump(dom_snapshot, handle_fp, indent=2, ensure_ascii=False)
            written_files.append(file_path)
        return written_files

    def _human_pause(self) -> None:
        low, high = self.config.tab_switch_delay_range
        time.sleep(random.uniform(low, high))

    def _build_market_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if path.startswith("?"):
            return f"{self.config.base_url.rstrip('/')}/{path}".replace("/?", "?")
        return f"{self.config.base_url.rstrip('/')}/{normalize_market_path(path)}"

    def _get_handle_for_symbol(self, symbol: str) -> tuple[str, Dict[str, Any]]:
        for handle, info in self.tabs.items():
            if info["symbol"].upper() == symbol.upper():
                return handle, info
        raise KeyError(f"{self.config.name} symbol not configured: {symbol}")

    def _find_first_visible(
        self, selectors: Iterable[str], timeout: float = 10
    ) -> Optional[str]:
        for selector in selectors:
            try:
                if self._is_element_visible(selector, timeout=timeout):
                    return selector
            except Exception:
                continue
        return None

    def _click_first_visible(self, selectors: Iterable[str], timeout: float = 8) -> bool:
        selector = self._find_first_visible(selectors, timeout=timeout)
        if not selector:
            return False
        self._click_selector(selector)
        return True

    def _get_text(self, selectors: Iterable[str]) -> str:
        selector = self._find_first_visible(selectors, timeout=20)
        if not selector:
            raise RuntimeError(
                f"[{self.config.name}] element not found; selectors={list(selectors)}"
            )
        if selector.startswith("xpath=") or selector.startswith("//"):
            return self._find_element(selector).text.strip()
        return self.driver.get_text(selector).strip()

    def _find_element(self, selector: str):
        if selector.startswith("xpath="):
            return self.driver.find_element("xpath", selector[len("xpath="):])
        if selector.startswith("//"):
            return self.driver.find_element("xpath", selector)
        return self.driver.find_element("css selector", selector)

    def _is_element_visible(self, selector: str, timeout: float = 10) -> bool:
        if selector.startswith("xpath=") or selector.startswith("//"):
            xpath = selector[len("xpath="):] if selector.startswith("xpath=") else selector
            elements = self.driver.find_elements("xpath", xpath)
            return any(element.is_displayed() for element in elements)
        return self.driver.is_element_visible(selector, timeout=timeout)

    def _click_selector(self, selector: str) -> None:
        element = self._find_element(selector)
        try:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
                element,
            )
        except Exception:
            pass
        time.sleep(random.uniform(0.08, 0.2))
        try:
            ActionChains(self.driver).move_to_element(element).pause(random.uniform(0.08, 0.18)).click().perform()
            return
        except Exception:
            pass
        try:
            element.click()
            return
        except Exception:
            pass
        try:
            if selector.startswith("xpath="):
                self.driver.click(f"xpath={selector[len('xpath='):]}")
            elif selector.startswith("//"):
                self.driver.click(f"xpath={selector}")
            else:
                self.driver.click(selector)
            return
        except Exception:
            pass
        self.driver.execute_script("arguments[0].click();", element)

    def _human_click_element(self, element) -> None:
        try:
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
                element,
            )
        except Exception:
            pass
        time.sleep(random.uniform(0.08, 0.2))
        try:
            ActionChains(self.driver).move_to_element(element).pause(random.uniform(0.08, 0.18)).click().perform()
            return
        except Exception:
            pass
        try:
            element.click()
            return
        except Exception:
            pass
        self.driver.execute_script("arguments[0].click();", element)

    def _human_type_into_field(self, element, text: str, per_char_delay: tuple[float, float] = (0.03, 0.09)) -> None:
        select_all_key = Keys.COMMAND if sys.platform == "darwin" else Keys.CONTROL
        self._human_click_element(element)
        time.sleep(random.uniform(0.08, 0.18))
        element.send_keys(select_all_key, "a")
        time.sleep(random.uniform(0.05, 0.12))
        element.send_keys(Keys.DELETE)
        time.sleep(random.uniform(0.08, 0.16))
        for char in str(text):
            element.send_keys(char)
            time.sleep(random.uniform(*per_char_delay))
        time.sleep(random.uniform(0.08, 0.18))
        element.send_keys(Keys.TAB)
        time.sleep(random.uniform(0.12, 0.25))

    def _get_current_url_safe(self) -> str:
        try:
            return (self.driver.get_current_url() or "").strip()
        except Exception:
            return ""

    def _sort_handles_extension_first(self, handles: List[str]) -> List[str]:
        """Prefer chrome-extension:// tabs when several new windows appear."""
        ext: List[str] = []
        other: List[str] = []
        for h in handles:
            try:
                self.driver.switch_to.window(h)
                url = self._get_current_url_safe()
                if url.startswith("chrome-extension://"):
                    ext.append(h)
                else:
                    other.append(h)
            except Exception:
                other.append(h)
        return ext + other

    def _window_looks_like_okx(self) -> bool:
        """Heuristic check for OKX wallet pages/popups."""
        try:
            url = self._get_current_url_safe().lower()
            title = (self.driver.get_title() or "").strip().lower()
        except Exception:
            url = ""
            title = ""
        if "okx" in url or "okx" in title:
            return True
        try:
            body_text = self.driver.execute_script(
                """
                return ((document.body && (document.body.innerText || document.body.textContent)) || '')
                  .trim()
                  .slice(0, 4000)
                  .toLowerCase();
                """
            ) or ""
        except Exception:
            body_text = ""
        signals = ["okx wallet", "okx", "connect wallet", "connect", "confirm", "sign"]
        return any(signal in body_text for signal in signals)

    def _detect_okx_extension_info(self) -> tuple[str, List[str]]:
        """Detect OKX extension id and entry paths from the current Chrome profile."""
        if not self.user_data_dir:
            return "", []

        extensions_root = Path(self.user_data_dir).expanduser() / "Default" / "Extensions"
        if not extensions_root.exists():
            return "", []

        for manifest_path in sorted(extensions_root.glob("*/*/manifest.json")):
            try:
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            author = str(data.get("author", "")).lower()
            name = str(data.get("name", "")).lower()
            short_name = str(data.get("short_name", "")).lower()
            description = str(data.get("description", "")).lower()
            text_blob = " ".join([author, name, short_name, description])
            if "okx" not in text_blob and "web3.okx.com" not in text_blob:
                continue

            extension_id = manifest_path.parents[1].name
            action = data.get("action") or {}
            side_panel = data.get("side_panel") or {}
            candidate_paths: List[str] = []
            for raw_path in [
                action.get("default_popup"),
                side_panel.get("default_path"),
                "popup-init.html",
                "popup.html",
                "home.html",
                "index.html",
                "notification.html",
            ]:
                normalized = str(raw_path or "").strip().lstrip("/")
                if normalized and normalized not in candidate_paths:
                    candidate_paths.append(normalized)
            return extension_id, candidate_paths

        return "", []

    def _build_okx_extension_urls(self) -> List[str]:
        """Build candidate OKX extension entry URLs from environment variables."""
        extension_id = (os.getenv("OKX_EXTENSION_ID") or "").strip()
        detected_paths: List[str] = []
        if not extension_id:
            extension_id, detected_paths = self._detect_okx_extension_info()
        if not extension_id:
            return []

        raw_paths = (os.getenv("OKX_EXTENSION_PATHS") or "").strip()
        candidate_paths = [
            "popup-init.html",
            "popup.html",
            "home.html",
            "index.html",
            "notification.html",
        ]
        if detected_paths:
            candidate_paths = detected_paths + [
                path for path in candidate_paths if path not in detected_paths
            ]
        if raw_paths:
            candidate_paths = [part.strip().lstrip("/") for part in raw_paths.split(",") if part.strip()]

        urls: List[str] = []
        preferred_unlock_paths = []
        for path in candidate_paths:
            preferred_unlock_paths.append(f"{path}#/unlock")
        preferred_unlock_paths.extend(
            [
                "home.html#/unlock",
                "notification.html#/unlock",
                "popup.html#/unlock",
                "#/unlock",
            ]
        )

        for path in preferred_unlock_paths + [""] + candidate_paths:
            normalized = path.strip()
            if normalized:
                urls.append(f"chrome-extension://{extension_id}/{normalized}")
            else:
                urls.append(f"chrome-extension://{extension_id}/")
        # Keep order stable while removing duplicates.
        urls = list(dict.fromkeys(urls))
        return urls

    def _open_okx_extension_tab(self) -> Optional[str]:
        """
        Open a dedicated tab for the OKX extension.

        Returns the new handle if a likely OKX tab was opened successfully.
        """
        candidate_urls = self._build_okx_extension_urls()
        if not candidate_urls:
            return None

        handles_before = set(self.driver.window_handles)
        try:
            self.driver.execute_script("window.open('about:blank', '_blank');")
            time.sleep(0.25)
            current_handles = list(self.driver.window_handles)
            new_handles = [h for h in current_handles if h not in handles_before]
            if not new_handles:
                return None
            new_handle = new_handles[-1]
            self.driver.switch_to.window(new_handle)
        except Exception:
            return None

        for url in candidate_urls:
            try:
                self.driver.get(url)
                time.sleep(0.8)
                current_url = self._get_current_url_safe()
                if current_url.startswith("chrome-extension://") and self._window_looks_like_okx():
                    print(f"[{self.config.name}] opened OKX extension tab: {current_url}")
                    return new_handle
            except Exception:
                continue
        return new_handle

    # ------------------------------------------------------------------
    # Password unlock helpers
    # ------------------------------------------------------------------

    _SUBMIT_LABELS = [
        "unlock", "confirm", "next", "continue", "log in", "login",
        "ok", "done", "submit", "解锁", "确认", "下一步", "继续", "完成", "提交",
    ]

    def _fill_password_js(self, secret: str) -> bool:
        """Fill the password input using JS nativeValue trick (works in current frame)."""
        try:
            result = self.driver.execute_script(
                """
                const pwd = arguments[0];
                const setNativeValue = (el, value) => {
                  const proto = el && Object.getPrototypeOf(el);
                  const descriptor = proto ? Object.getOwnPropertyDescriptor(proto, 'value') : null;
                  if (descriptor && typeof descriptor.set === 'function') {
                    descriptor.set.call(el, value);
                  } else {
                    el.value = value;
                  }
                };
                const input =
                    document.querySelector('input[type="password"]')
                    || document.querySelector('input[data-testid="okd-input"]')
                    || document.querySelector('input[placeholder*="密码"]')
                    || document.querySelector('input[placeholder*="password" i]')
                    || document.querySelector('input');
                if (!input) return { filled: false, reason: 'no_password_input' };
                try {
                  input.focus();
                  setNativeValue(input, '');
                  input.dispatchEvent(new Event('input', { bubbles: true }));
                  setNativeValue(input, pwd);
                  input.dispatchEvent(new Event('input', { bubbles: true }));
                  input.dispatchEvent(new Event('change', { bubbles: true }));
                  input.dispatchEvent(new Event('blur', { bubbles: true }));
                } catch (e) {
                  return { filled: false, reason: 'fill_failed', error: String(e) };
                }
                return { filled: true };
                """,
                secret,
            )
            return bool(result and result.get("filled"))
        except Exception:
            return False

    def _fill_password_webdriver(self, secret: str) -> bool:
        """Fill the password input using Selenium send_keys (works in current frame)."""
        for selector in [
            "input[type='password']",
            "input[data-testid='okd-input']",
            "input[placeholder*='密码']",
            "input[placeholder*='Password']",
            "input[placeholder*='password']",
            "input",
        ]:
            try:
                elements = self.driver.find_elements("css selector", selector)
            except Exception:
                continue
            for el in elements:
                try:
                    if not el.is_displayed():
                        continue
                    el.click()
                    try:
                        el.clear()
                    except Exception:
                        pass
                    el.send_keys(secret)
                    print(
                        f"[{self.config.name}] OKX extension: filled password via webdriver selector={selector}"
                    )
                    time.sleep(0.3)
                    return True
                except Exception:
                    continue
        return False

    def _click_submit_in_current_frame(self) -> bool:
        """Click the unlock/confirm button in the current frame."""
        try:
            clicked = self.driver.execute_script(
                """
                const labels = arguments[0].map((s) => s.toLowerCase());
                const seen = new Set();
                const queue = [document];
                const nodes = [];
                while (queue.length) {
                  const root = queue.shift();
                  if (!root || seen.has(root)) continue;
                  seen.add(root);
                  if (!root.querySelectorAll) continue;
                  nodes.push(...root.querySelectorAll(
                    'button, [type="submit"], [role="button"], a[role="button"]'
                  ));
                  for (const el of root.querySelectorAll('*')) {
                    if (el.shadowRoot) queue.push(el.shadowRoot);
                  }
                }
                for (const el of nodes) {
                  if (el.disabled || el.getAttribute('aria-disabled') === 'true') continue;
                  const t = ((el.innerText || el.textContent || '') + '').trim().toLowerCase();
                  if (!t) continue;
                  if (labels.some((l) => t === l || t.includes(l))) {
                    el.click();
                    return true;
                  }
                }
                return false;
                """,
                self._SUBMIT_LABELS,
            )
            if clicked:
                return True
        except Exception:
            pass
        # Form submit / Enter fallback
        try:
            self.driver.execute_script(
                """
                const input =
                  document.querySelector('input[type="password"]')
                  || document.querySelector('input[data-testid="okd-input"]')
                  || document.querySelector('input');
                const form = (input && input.closest('form')) || document.querySelector('form');
                if (form && typeof form.requestSubmit === 'function') {
                  form.requestSubmit(); return;
                }
                if (input) {
                  ['keydown','keypress','keyup'].forEach(t =>
                    input.dispatchEvent(new KeyboardEvent(t, { key: 'Enter', code: 'Enter', bubbles: true }))
                  );
                }
                """
            )
            return True
        except Exception:
            return False

    def _try_fill_and_submit_in_current_frame(self, secret: str, label: str) -> bool:
        """Try to fill the password and submit in whichever frame is currently active."""
        filled = self._fill_password_js(secret)
        if not filled:
            filled = self._fill_password_webdriver(secret)
        if not filled:
            return False
        print(f"[{self.config.name}] OKX extension: filled password in {label}")
        time.sleep(0.4)
        submitted = self._click_submit_in_current_frame()
        if submitted:
            print(f"[{self.config.name}] OKX extension: submitted unlock form in {label}")
            time.sleep(0.5)
        return True

    def _try_okx_extension_password_unlock(self) -> bool:
        """
        Fill the OKX extension password input and submit.

        The OKX wallet extension renders its unlock UI inside a sandboxed iframe
        (``ses.html``).  Plain ``document.querySelector`` on the popup page misses
        it, so we enumerate iframes and switch into each one until we find an input.

        Set ``OKX_WALLET_UNLOCK_PASSWORD`` in the environment.
        """
        current_url = self._get_current_url_safe()
        if not current_url.startswith("chrome-extension://"):
            print(
                f"[{self.config.name}] OKX extension: skip password helper on non-extension page "
                f"url={current_url!r}"
            )
            return False
        print(f"[{self.config.name}] OKX extension: entering password unlock helper url={current_url!r}")
        secret = (os.getenv("OKX_WALLET_UNLOCK_PASSWORD") or "").strip()
        if not secret:
            print(f"[{self.config.name}] OKX extension: no OKX_WALLET_UNLOCK_PASSWORD configured")
            return False

        # --- Strategy 1: top-level document ---
        if self._try_fill_and_submit_in_current_frame(secret, "top-level frame"):
            return True

        # --- Strategy 2: enumerate iframes and try each ---
        try:
            iframes = self.driver.find_elements("tag name", "iframe")
        except Exception:
            iframes = []

        print(f"[{self.config.name}] OKX extension: no input in top frame, trying {len(iframes)} iframe(s)")
        for idx, iframe in enumerate(iframes):
            try:
                self.driver.switch_to.default_content()
                iframe_src = ""
                try:
                    iframe_src = iframe.get_attribute("src") or ""
                except Exception:
                    pass
                self.driver.switch_to.frame(iframe)
                time.sleep(0.3)
                frame_url = self._get_current_url_safe()
                print(
                    f"[{self.config.name}] OKX extension: iframe[{idx}] src={iframe_src!r} url={frame_url!r}"
                )
                if self._try_fill_and_submit_in_current_frame(secret, f"iframe[{idx}] {frame_url!r}"):
                    self.driver.switch_to.default_content()
                    return True
            except Exception as exc:
                print(f"[{self.config.name}] OKX extension: iframe[{idx}] error {exc!r}")
            finally:
                try:
                    self.driver.switch_to.default_content()
                except Exception:
                    pass

        print(f"[{self.config.name}] OKX extension: password input not found in any frame")
        return False

    def _click_okx_wallet_option(self, timeout: float = 12.0) -> bool:
        """Pick OKX in the wallet chooser, including web component / shadow DOM cases."""
        # OKX may appear under several display names depending on the dApp's wallet modal version.
        _OKX_LABELS = ["okx wallet", "okx web3", "okx"]

        def _click_privy_okx_button() -> bool:
            """Handle the Privy wallet chooser used by trade.xyz."""
            try:
                return bool(
                    self.driver.execute_script(
                        """
                        const labels = arguments[0];
                        const dialog = document.querySelector('#privy-dialog');
                        if (!dialog) return false;
                        const buttons = Array.from(dialog.querySelectorAll('button'));
                        for (const button of buttons) {
                          const text = ((button.innerText || button.textContent || '') + '')
                            .trim()
                            .replace(/\\s+/g, ' ')
                            .toLowerCase();
                          if (!text) continue;
                          if (!labels.some((label) => text.includes(label))) continue;
                          try { button.scrollIntoView({ block: 'center', inline: 'center' }); } catch (e) {}
                          try { button.click(); } catch (e) { return false; }
                          return true;
                        }
                        return false;
                        """,
                        _OKX_LABELS,
                    )
                )
            except Exception:
                return False

        def _dialog_shows_sign_to_verify() -> bool:
            try:
                return bool(
                    self.driver.execute_script(
                        """
                        return document.querySelector('#privy-dialog h3')?.textContent?.trim() === 'Sign to verify';
                        """,
                    )
                )
            except Exception:
                return False

        def _click_okx_js() -> bool:
            """Click the OKX option. Returns True if the element was found and click dispatched."""
            try:
                return bool(self.driver.execute_script(
                    """
                    const labels = arguments[0];
                    const isVisible = (el) => {
                      if (!el) return false;
                      const style = window.getComputedStyle(el);
                      if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                        return false;
                      }
                      const rect = el.getBoundingClientRect();
                      return rect.width > 0 && rect.height > 0;
                    };
                    const clickElement = (el) => {
                      if (!el) return false;
                      try { el.scrollIntoView({ block: 'center', inline: 'center' }); } catch (e) {}
                      ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click'].forEach((type) => {
                        try {
                          el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window }));
                        } catch (e) {}
                      });
                      try { el.click(); } catch (e) {}
                      return true;
                    };
                    const seen = new Set();
                    const queue = [document];
                    const nodes = [];
                    while (queue.length) {
                      const root = queue.shift();
                      if (!root || seen.has(root)) continue;
                      seen.add(root);
                      if (!root.querySelectorAll) continue;
                      nodes.push(...Array.from(root.querySelectorAll('*')));
                      for (const el of Array.from(root.querySelectorAll('*'))) {
                        if (el.shadowRoot) queue.push(el.shadowRoot);
                      }
                    }
                    for (const el of nodes) {
                      if (!isVisible(el)) continue;
                      const text = ((el.innerText || el.textContent || '') + '').trim().toLowerCase();
                      if (!text) continue;
                      if (!labels.some((label) => text === label || text.includes(label))) continue;
                      const target =
                        el.closest('button,[role="button"],wui-list-item,wui-card,li,[tabindex],div')
                        || el;
                      if (clickElement(target)) return true;
                    }
                    return false;
                    """,
                    _OKX_LABELS,
                ))
            except Exception:
                return False

        def _click_and_verify() -> bool:
            """Click OKX option and verify success via 'Sign to verify'."""
            if not _click_okx_js():
                return False
            deadline = time.time() + 5.0
            while time.time() < deadline:
                if _dialog_shows_sign_to_verify():
                    return True
                time.sleep(0.25)
            return False

        if _click_privy_okx_button():
            deadline = time.time() + 5.0
            while time.time() < deadline:
                if _dialog_shows_sign_to_verify():
                    return True
                time.sleep(0.25)

        okx_text_selectors = [
            "text='OKX Wallet'",
            "text=\"OKX Wallet\"",
            "text='OKX Web3'",
            "text='OKX'",
            "wui-text:has-text('OKX Wallet')",
            "wui-list-item:has-text('OKX Wallet')",
            "wui-card:has-text('OKX Wallet')",
            "xpath=//*[contains(normalize-space(), 'OKX Wallet')]",
            "xpath=//*[contains(normalize-space(), 'OKX Web3')]",
            "xpath=//*[normalize-space()='OKX']",
        ]
        selector = self._find_first_visible(okx_text_selectors, timeout=min(timeout, 8.0))
        if selector:
            try:
                self._click_selector(selector)
                deadline = time.time() + 5.0
                while time.time() < deadline:
                    if _dialog_shows_sign_to_verify():
                        return True
                    time.sleep(0.25)
            except Exception:
                pass

        deadline = time.time() + timeout
        while time.time() < deadline:
            if _click_and_verify():
                return True
            time.sleep(0.35)
        return False

    def _click_okx_confirm_in_current_window(self, max_attempts: int = 60) -> bool:
        """Click primary action in OKX Wallet popup (Connect / Confirm / Approve / Sign)."""
        confirm_selectors = [
            "button:has-text('Confirm')",
            "button:has-text('Connect')",
            "button:has-text('Approve')",
            "button:has-text('Sign')",
            "button:has-text('Next')",
            "button:has-text('Continue')",
            "text='Confirm'",
            "text='Connect'",
            "text='Approve'",
            "text='Sign'",
            "text='Next'",
            "text='Continue'",
            "xpath=//button[normalize-space()='Confirm']",
            "xpath=//button[normalize-space()='Connect']",
            "xpath=//button[normalize-space()='Approve']",
            "xpath=//button[normalize-space()='Sign']",
            "xpath=//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'confirm')]",
            "xpath=//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'connect')]",
            "xpath=//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'approve')]",
            "xpath=//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sign')]",
        ]
        for attempt in range(max_attempts):
            if attempt in (0, 6, 12, 20):
                self._try_okx_extension_password_unlock()
            if self._click_first_visible(confirm_selectors, timeout=1.5):
                return True
            try:
                clicked = self.driver.execute_script(
                    """
                    const labels = ['confirm', 'connect', 'approve', 'sign', 'next', 'continue', '授权', '确认', '连接', '签名', '下一步', '继续'];
                    const seen = new Set();
                    const queue = [document];
                    const nodes = [];
                    while (queue.length) {
                      const root = queue.shift();
                      if (!root || seen.has(root)) continue;
                      seen.add(root);
                      if (root.querySelectorAll) {
                        nodes.push(...Array.from(root.querySelectorAll('button, [role="button"], a[role="button"], div[tabindex], span[tabindex]')));
                        for (const el of Array.from(root.querySelectorAll('*'))) {
                          if (el.shadowRoot) queue.push(el.shadowRoot);
                        }
                      }
                    }
                    for (const el of nodes) {
                      if (el.disabled || el.getAttribute('aria-disabled') === 'true') continue;
                      const t = ((el.innerText || el.textContent || '') + '').trim().toLowerCase();
                      if (!t) continue;
                      if (labels.some((l) => t === l || t.includes(l))) {
                        el.click();
                        return true;
                      }
                    }
                    return false;
                    """
                )
                if clicked:
                    return True
            except Exception:
                pass
            time.sleep(0.35)
        return False

    @staticmethod
    def _okx_url_is_request_page(url: str) -> bool:
        """Return True when the OKX extension URL represents a pending dApp request.

        Home page (``#/`` or ``#/home``) and unlock page are NOT request pages.
        """
        if not url.startswith("chrome-extension://"):
            return False
        fragment = url.split("#", 1)[1].rstrip("/") if "#" in url else ""
        # Explicit non-request pages
        if fragment in ("", "/", "/home", "/assets", "/unlock"):
            return False
        # Recognisable request paths
        request_fragments = (
            "/connect",
            "/dapp-entry",
            "/sign",
            "/transaction",
            "/approval",
            "/eth-sign",
            "/personal-sign",
            "/typed-data",
        )
        return any(fragment.startswith(f) for f in request_fragments)

    def _wait_for_okx_request_page(self, handle: str, wait_seconds: float = 8.0) -> bool:
        """After unlock the extension may take a few seconds to navigate to the request page.

        Polls the current URL until it looks like a request page or the wait expires.
        Returns True if a request page was detected.
        """
        deadline = time.time() + wait_seconds
        while time.time() < deadline:
            try:
                self.driver.switch_to.window(handle)
                url = self._get_current_url_safe()
                if self._okx_url_is_request_page(url):
                    print(f"[{self.config.name}] OKX confirm: request page detected url={url!r}")
                    return True
            except Exception:
                pass
            time.sleep(0.3)
        url = self._get_current_url_safe()
        print(f"[{self.config.name}] OKX confirm: timed out waiting for request page, current={url!r}")
        return False

    def _find_okx_request_handle(self) -> Optional[str]:
        """Scan all open windows and return the handle of any OKX request page, or None."""
        current_handle = None
        try:
            current_handle = self.driver.current_window_handle
        except Exception:
            pass
        for handle in list(getattr(self.driver, "window_handles", []) or []):
            try:
                self.driver.switch_to.window(handle)
                url = self._get_current_url_safe()
                if self._okx_url_is_request_page(url):
                    print(f"[{self.config.name}] OKX confirm: found request page handle={handle[:12]} url={url!r}")
                    return handle
            except Exception:
                continue
        if current_handle:
            try:
                self.driver.switch_to.window(current_handle)
            except Exception:
                pass
        return None

    def _ensure_okx_extension_unlocked(self) -> bool:
        """Open the OKX extension tab, unlock it if needed, then return to the primary tab.

        Call this BEFORE opening the dApp connect-wallet modal so the extension is ready
        to receive and display the connection request immediately when OKX is selected.
        Returns True if the extension was successfully opened (whether already unlocked or
        just unlocked), False if the extension tab could not be opened at all.
        """
        current_handle: Optional[str] = None
        try:
            current_handle = self.driver.current_window_handle
        except Exception:
            pass

        ext_handle = self._open_okx_extension_tab()
        if not ext_handle:
            print(f"[{self.config.name}] pre-unlock: could not open OKX extension tab")
            return False

        try:
            self.driver.switch_to.window(ext_handle)
            time.sleep(0.6)
            url = self._get_current_url_safe()
            if "#/unlock" in url:
                print(f"[{self.config.name}] pre-unlock: extension locked, unlocking now ...")
                self._try_okx_extension_password_unlock()
                # Wait for the extension to navigate away from the unlock page
                for _ in range(12):
                    time.sleep(0.5)
                    url = self._get_current_url_safe()
                    if "#/unlock" not in url:
                        break
                print(f"[{self.config.name}] pre-unlock: after unlock url={url!r}")
            else:
                print(f"[{self.config.name}] pre-unlock: extension already unlocked url={url!r}")

            # If the extension landed on a stale dApp request page from a previous session,
            # navigate to the home page to clear it. Otherwise the code would later open the
            # extension, find this stale request, confirm it (false success), and the dApp
            # would never see a valid connection response.
            url = self._get_current_url_safe()
            if self._okx_url_is_request_page(url):
                print(f"[{self.config.name}] pre-unlock: clearing stale request page {url!r} → navigating to home")
                try:
                    base = url.split("#")[0]
                    self.driver.get(base + "#/")
                    time.sleep(0.5)
                except Exception:
                    pass

            # Close the extension tab so it does not interfere with the dApp modal.
            # When the dApp sends the connect request, the extension will create its own popup.
            try:
                self.driver.close()
            except Exception:
                pass
        except Exception as exc:
            print(f"[{self.config.name}] pre-unlock: error {exc!r}")
        finally:
            if current_handle:
                try:
                    self.driver.switch_to.window(current_handle)
                except Exception:
                    pass
        return True

    def _try_confirm_okx_extension_window(
        self,
        handles_before: set[str],
        timeout: float = 45.0,
        open_extension_first: bool = False,
        wc_pairing_wait_seconds: float = 12.0,
    ) -> bool:
        """
        After choosing OKX in the dApp modal, switch to the new extension/wallet window and confirm.

        Strategy:
        1. Every tick, scan ALL open windows for an OKX request page (connect/sign/dapp-entry).
           WalletConnect dApps (e.g. trade.xyz) may open the request in notification.html or a
           chrome.windows.create popup — a different window than the popup we manually opened.
        2. If an unlock page is found, unlock it (password fill + submit) then keep scanning.
        3. Only open the extension manually (open_extension_first) after a short WalletConnect
           pairing delay, so the pairing has time to complete before we navigate.
        4. Never click Confirm/Connect on the extension home page to avoid false positives.
        wc_pairing_wait_seconds controls how long to wait for a natural extension popup before
        opening the extension manually. Use a shorter value (~4s) when the extension is
        pre-unlocked (browser extension flow is much faster than QR-code WalletConnect).
        """
        if not self.tabs:
            return False

        preferred_return = list(self.tabs.keys())[0]
        deadline = time.time() + timeout
        last_error: Optional[Exception] = None

        # Delay manual extension open to allow WalletConnect pairing to complete first.
        # After this time, if no request window appeared naturally, open the extension manually.
        wc_wait_until = time.time() + wc_pairing_wait_seconds
        manual_opened = False

        while time.time() < deadline:
            try:
                # --- Priority 1: scan ALL windows for a ready request page ---
                request_handle = self._find_okx_request_handle()
                if request_handle:
                    self.driver.switch_to.window(request_handle)
                    if self._click_okx_confirm_in_current_window():
                        time.sleep(0.6)
                        try:
                            self.driver.switch_to.window(preferred_return)
                        except Exception:
                            pass
                        return True
                    # Click didn't land yet — keep trying
                    time.sleep(0.25)
                    continue

                # --- Priority 2: handle unlock page if present ---
                current_handles = list(self.driver.window_handles or [])
                for handle in current_handles:
                    if handle in self.tabs:
                        continue
                    try:
                        self.driver.switch_to.window(handle)
                        url = self._get_current_url_safe()
                        if url.startswith("chrome-extension://") and "#/unlock" in url:
                            self._try_okx_extension_password_unlock()
                            break
                    except Exception:
                        continue

                # --- Priority 3: open extension manually after WalletConnect pairing delay ---
                if open_extension_first and not manual_opened and time.time() >= wc_wait_until:
                    ext_handle = self._open_okx_extension_tab()
                    if ext_handle:
                        manual_opened = True
                        self.driver.switch_to.window(ext_handle)
                        time.sleep(0.5)
                        self._try_okx_extension_password_unlock()

                # --- Also check for any existing non-tab extension windows ---
                else:
                    for handle in current_handles:
                        if handle in self.tabs:
                            continue
                        try:
                            self.driver.switch_to.window(handle)
                            url = self._get_current_url_safe()
                            if url.startswith("chrome-extension://") or self._window_looks_like_okx():
                                self._try_okx_extension_password_unlock()
                                break
                        except Exception:
                            continue

            except Exception as exc:
                last_error = exc
            time.sleep(0.4)

        try:
            self.driver.switch_to.window(preferred_return)
        except Exception:
            pass
        if last_error:
            print(f"[{self.config.name}] OKX auto-confirm: gave up ({last_error!r})")
        return False


class OmniSeleniumClient(FrontendExchangeClient):
    """Preserve the original Omni SeleniumBase interaction style."""

    _MARKET_TAB_SELECTORS = [
        "xpath=(//button[normalize-space()='Market'])[last()]",
        "xpath=(//*[@data-testid='toggle-select']//button[normalize-space()='Market'])[last()]",
        "xpath=(//*[@data-testid='toggle-select']//*[normalize-space()='Market'])[last()]",
    ]
    _LIMIT_TAB_SELECTORS = [
        "xpath=(//button[normalize-space()='Limit'])[last()]",
        "xpath=(//*[@data-testid='toggle-select']//button[normalize-space()='Limit'])[last()]",
        "xpath=(//*[@data-testid='toggle-select']//*[normalize-space()='Limit'])[last()]",
    ]
    _SUBMIT_BUTTON_SELECTORS = [
        "button[data-testid='submit-button']",
        "xpath=(//button[@data-testid='submit-button'])[last()]",
    ]
    _REDUCE_ONLY_SELECTORS = [
        "button[data-testid='reduce-only-checkbox']",
        "xpath=(//button[@data-testid='reduce-only-checkbox'])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Reduce Only')])[last()]",
    ]
    _BUY_SIDE_SELECTORS = [
        "xpath=(//*[@data-testid='toggle-select']/following::button[contains(normalize-space(), 'Buy $')])[1]",
        "xpath=(//button[contains(normalize-space(), 'Buy $')])[last()]",
    ]
    _SELL_SIDE_SELECTORS = [
        "xpath=(//*[@data-testid='toggle-select']/following::button[contains(normalize-space(), 'Sell $')])[1]",
        "xpath=(//button[contains(normalize-space(), 'Sell $')])[last()]",
    ]
    _QUANTITY_UNIT_TOGGLE_SELECTORS = [
        "button[data-testid='input-mode-toggle']",
        "xpath=(//button[@data-testid='input-mode-toggle'])[last()]",
    ]
    _ORDER_CAPTURE_PATTERNS = ["/api/orders/v2"]
    _POSITION_CAPTURE_PATTERNS = ["/api/positions", "/api/portfolio/positions", "/api/position"]
    _CAPTURE_TIMEOUT_SECONDS = 12.0

    def install_quote_cache(self) -> None:
        """Continuously cache bid/ask from the current Omni tab without later tab switching."""
        self.driver.execute_script(
            """
            if (window.__quoteCacheInstalled) return true;
            window.__quoteCacheInstalled = true;
            window.__latestQuoteCache = null;

            const readText = (selector) => {
              const el = document.querySelector(selector);
              return el ? ((el.innerText || el.textContent || '') + '').trim() : '';
            };

            const updateQuoteCache = () => {
              const askText = readText("[data-testid='ask-price-display']");
              const bidText = readText("[data-testid='bid-price-display']");
              if (!askText || !bidText) return;
              window.__latestQuoteCache = {
                askText,
                bidText,
                pageUrl: window.location.href,
                cachedAt: new Date().toISOString(),
              };
            };

            updateQuoteCache();
            window.__quoteCacheTimer = window.__quoteCacheTimer || window.setInterval(updateQuoteCache, 500);
            return true;
            """
        )

    def capture_cached_quote(self, persist: bool = True) -> QuoteSnapshot:
        if not self.tabs:
            raise RuntimeError("[omni] no market tabs opened")
        info = self.tabs[list(self.tabs.keys())[0]]
        data = self.driver.execute_script("return window.__latestQuoteCache || null;")
        if not data:
            raise RuntimeError("[omni] quote cache is empty")

        ask_text = str(data.get("askText") or "").strip()
        bid_text = str(data.get("bidText") or "").strip()
        ask = parse_number(ask_text)
        bid = parse_number(bid_text)
        snapshot = QuoteSnapshot(
            exchange=self.config.name,
            symbol=info["symbol"],
            market=info["market"],
            ask=ask,
            bid=bid,
            ask_quantity=info["quantity"],
            bid_quantity=info["quantity"],
            input_quantity=info["quantity"],
            captured_at=utc8_now_str("%Y-%m-%d %H:%M:%S"),
            minute=utc8_now_str("%Y-%m-%d %H:%M"),
            page_url=str(data.get("pageUrl") or self.driver.get_current_url()),
            metadata={
                "ask_text": ask_text,
                "bid_text": bid_text,
                "source": "omni_quote_cache",
                "cached_at": data.get("cachedAt"),
            },
        )
        if persist:
            self.persist_quote(snapshot)
        return snapshot

    def get_primary_handle(self) -> Optional[str]:
        """Return the main Omni market tab handle."""
        if not self.tabs:
            return None
        return list(self.tabs.keys())[0]

    def switch_to_primary_tab(self) -> bool:
        """Switch webdriver focus back to the main Omni market tab."""
        handle = self.get_primary_handle()
        if not handle:
            return False
        try:
            self.driver.switch_to.window(handle)
            return True
        except Exception:
            return False

    def click_connect_wallet_button(self, timeout_seconds: float = 30.0) -> bool:
        """Click the top-right Connect Wallet button on Omni."""
        connect_selectors = [
            "button[data-testid='connect-button']",
            "xpath=(//button[@data-testid='connect-button'])[1]",
            "xpath=(//button[normalize-space()='Connect Wallet'])[1]",
        ]
        button_found = self._click_first_visible(connect_selectors, timeout=timeout_seconds)
        if button_found:
            return True
        try:
            clicked = self.driver.execute_script(
                """
                const btn = Array.from(document.querySelectorAll('button'))
                  .find((el) => (el.innerText || '').trim() === 'Connect Wallet');
                if (btn) {
                  btn.click();
                  return true;
                }
                return false;
                """
            )
            return bool(clicked)
        except Exception:
            return False

    def refresh_primary_page(self, wait_seconds: Optional[float] = None) -> bool:
        """Refresh the main Omni tab and wait for the page to settle."""
        if not self.switch_to_primary_tab():
            return False
        try:
            self.driver.refresh()
        except Exception:
            try:
                self.driver.get(self.driver.get_current_url())
            except Exception:
                return False
        time.sleep(wait_seconds if wait_seconds is not None else max(self.config.page_load_seconds, 3.0))
        return self.switch_to_primary_tab()

    def _get_required_text(self, selector: str, timeout_seconds: float = 20.0) -> str:
        deadline = time.time() + timeout_seconds
        last_text = ""
        while time.time() < deadline:
            try:
                text = self.driver.get_text(selector).strip()
                if text:
                    return text
                last_text = text
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError(f"[omni] unable to read non-empty text from {selector!r}, last={last_text!r}")

    def click_authenticate_button(self, timeout_seconds: float = 20.0) -> bool:
        """After wallet connection, Omni may require clicking Authenticate in the top-right."""
        selectors = [
            "button:has-text('Authenticate')",
            "nav button:has-text('Authenticate')",
            "header button:has-text('Authenticate')",
            "text='Authenticate'",
            "xpath=(//button[normalize-space()='Authenticate'])[1]",
            "xpath=//*[self::button or @role='button'][contains(normalize-space(), 'Authenticate')]",
        ]
        if self._click_first_visible(selectors, timeout=timeout_seconds):
            return True

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                clicked = self.driver.execute_script(
                    """
                    const labels = ['authenticate'];
                    const seen = new Set();
                    const queue = [document];
                    const nodes = [];
                    while (queue.length) {
                      const root = queue.shift();
                      if (!root || seen.has(root)) continue;
                      seen.add(root);
                      if (root.querySelectorAll) {
                        nodes.push(...Array.from(root.querySelectorAll('button, [role="button"], a[role="button"], div[tabindex], span[tabindex]')));
                        for (const el of Array.from(root.querySelectorAll('*'))) {
                          if (el.shadowRoot) queue.push(el.shadowRoot);
                        }
                      }
                    }
                    for (const el of nodes) {
                      if (el.disabled || el.getAttribute('aria-disabled') === 'true') continue;
                      const t = ((el.innerText || el.textContent || '') + '').trim().toLowerCase();
                      if (!t) continue;
                      if (labels.some((l) => t === l || t.includes(l))) {
                        el.click();
                        return true;
                      }
                    }
                    return false;
                    """
                )
                if clicked:
                    return True
            except Exception:
                pass
            time.sleep(0.35)
        return False

    def click_okx_wallet_option(self, timeout: float = 12.0) -> bool:
        """Select OKX Wallet from the wallet chooser."""
        return self._click_okx_wallet_option(timeout=timeout)

    def confirm_okx_extension_window(
        self,
        handles_before: set[str],
        timeout: float = 45.0,
        open_extension_first: bool = False,
        wc_pairing_wait_seconds: float = 12.0,
    ) -> bool:
        """Handle the OKX extension window for connect/confirm/sign."""
        return self._try_confirm_okx_extension_window(
            handles_before,
            timeout=timeout,
            open_extension_first=open_extension_first,
            wc_pairing_wait_seconds=wc_pairing_wait_seconds,
        )

    def capture_quote(self, handle: str, persist: bool = True) -> QuoteSnapshot:
        info = self.tabs[handle]
        self.driver.switch_to.window(handle)
        self._human_pause()

        ask_text = self._get_required_text("[data-testid='ask-price-display']")
        bid_text = self._get_required_text("[data-testid='bid-price-display']")
        ask = parse_number(ask_text)
        bid = parse_number(bid_text)

        snapshot = QuoteSnapshot(
            exchange=self.config.name,
            symbol=info["symbol"],
            market=info["market"],
            ask=ask,
            bid=bid,
            ask_quantity=info["quantity"],
            bid_quantity=info["quantity"],
            input_quantity=info["quantity"],
            captured_at=utc8_now_str("%Y-%m-%d %H:%M:%S"),
            minute=utc8_now_str("%Y-%m-%d %H:%M"),
            page_url=self.driver.get_current_url(),
            metadata={
                "ask_text": ask_text,
                "bid_text": bid_text,
                "window_handle": handle,
                "source": "omni_seleniumbase",
            },
        )
        if persist:
            self.persist_quote(snapshot)
        return snapshot

    @staticmethod
    def _parse_utc_timestamp(value: str) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _quantities_match(left: Any, right: Any, tolerance: float = 1e-9) -> bool:
        try:
            return abs(float(left) - float(right)) <= tolerance
        except (TypeError, ValueError):
            return False

    def _ensure_network_capture_hooks(self) -> None:
        self.driver.execute_script(
            """
            if (window.__omniNetworkCaptureInstalled) {
              return;
            }
            window.__omniNetworkCaptureInstalled = true;
            window.__omniCapturedResponses = [];

            const pushRecord = (record) => {
              window.__omniCapturedResponses.push(record);
              if (window.__omniCapturedResponses.length > 100) {
                window.__omniCapturedResponses = window.__omniCapturedResponses.slice(-100);
              }
            };

            const shouldTrack = (url, method) => {
              const normalizedUrl = String(url || '');
              const normalizedMethod = String(method || 'GET').toUpperCase();
              if (normalizedMethod !== 'GET') return false;
              return normalizedUrl.includes('/api/orders') || normalizedUrl.includes('/api/positions') || normalizedUrl.includes('/api/portfolio/positions');
            };

            const originalFetch = window.fetch;
            window.fetch = async function(...args) {
              const resource = args[0];
              const init = args[1] || {};
              const url = typeof resource === 'string' ? resource : (resource && resource.url) || '';
              const method = init.method || (resource && resource.method) || 'GET';
              const response = await originalFetch.apply(this, args);
              if (shouldTrack(url, method)) {
                try {
                  const clone = response.clone();
                  const body = await clone.text();
                  pushRecord({
                    transport: 'fetch',
                    method: String(method).toUpperCase(),
                    url: clone.url || url,
                    status: response.status,
                    captured_at: new Date().toISOString(),
                    body,
                  });
                } catch (error) {
                }
              }
              return response;
            };

            const originalOpen = XMLHttpRequest.prototype.open;
            const originalSend = XMLHttpRequest.prototype.send;
            XMLHttpRequest.prototype.open = function(method, url, ...rest) {
              this.__omniCaptureMeta = { method, url };
              return originalOpen.call(this, method, url, ...rest);
            };
            XMLHttpRequest.prototype.send = function(...args) {
              this.addEventListener('load', function() {
                const meta = this.__omniCaptureMeta || {};
                if (!shouldTrack(meta.url, meta.method)) {
                  return;
                }
                try {
                  pushRecord({
                    transport: 'xhr',
                    method: String(meta.method || 'GET').toUpperCase(),
                    url: this.responseURL || meta.url || '',
                    status: this.status,
                    captured_at: new Date().toISOString(),
                    body: typeof this.responseText === 'string' ? this.responseText : '',
                  });
                } catch (error) {
                }
              });
              return originalSend.apply(this, args);
            };
            """
        )

    def _reset_network_capture(self) -> None:
        self._ensure_network_capture_hooks()
        self.driver.execute_script("window.__omniCapturedResponses = [];")

    def _get_captured_network_records(self) -> List[Dict[str, Any]]:
        self._ensure_network_capture_hooks()
        result = self.driver.execute_script(
            "return Array.isArray(window.__omniCapturedResponses) ? window.__omniCapturedResponses : [];"
        )
        return result if isinstance(result, list) else []

    def _parse_capture_body(self, body: Any) -> Any:
        if isinstance(body, (dict, list)):
            return body
        if not isinstance(body, str) or not body.strip():
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return None

    def _match_order_from_payload(
        self,
        payload: Any,
        *,
        symbol: str,
        side: str,
        quantity: float,
        submitted_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        result = payload.get("result")
        if not isinstance(result, list):
            return None
        symbol_upper = symbol.upper()
        side_lower = side.lower()
        submitted_ts = submitted_at.timestamp()
        for item in result:
            if not isinstance(item, dict):
                continue
            instrument = item.get("instrument") or {}
            created_at = self._parse_utc_timestamp(str(item.get("created_at") or ""))
            if str(instrument.get("underlying") or "").upper() != symbol_upper:
                continue
            if str(item.get("side") or "").lower() != side_lower:
                continue
            if not self._quantities_match(item.get("qty"), quantity):
                continue
            if created_at and created_at.timestamp() + 1.0 < submitted_ts:
                continue
            return item
        return None

    def _wait_for_order_capture(
        self,
        *,
        symbol: str,
        side: str,
        quantity: float,
        submitted_at: datetime,
        timeout_seconds: float,
    ) -> tuple[Optional[Dict[str, Any]], Optional[str], Any]:
        deadline = time.time() + timeout_seconds
        seen_keys: set[tuple[str, str]] = set()
        last_payload = None
        last_url = None
        while time.time() < deadline:
            for record in self._get_captured_network_records():
                url = str(record.get("url") or "")
                captured_at = str(record.get("captured_at") or "")
                key = (url, captured_at)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                if not any(pattern in url for pattern in self._ORDER_CAPTURE_PATTERNS):
                    continue
                payload = self._parse_capture_body(record.get("body"))
                last_payload = payload
                last_url = url
                matched = self._match_order_from_payload(
                    payload,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    submitted_at=submitted_at,
                )
                if matched:
                    return matched, url, payload
            time.sleep(0.5)
        return None, last_url, last_payload

    def _wait_for_position_capture(
        self,
        *,
        timeout_seconds: float,
    ) -> tuple[Any, Optional[str]]:
        deadline = time.time() + timeout_seconds
        seen_keys: set[tuple[str, str]] = set()
        last_url = None
        while time.time() < deadline:
            for record in self._get_captured_network_records():
                url = str(record.get("url") or "")
                captured_at = str(record.get("captured_at") or "")
                key = (url, captured_at)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                if not any(pattern in url for pattern in self._POSITION_CAPTURE_PATTERNS):
                    continue
                payload = self._parse_capture_body(record.get("body"))
                last_url = url
                if isinstance(payload, list):
                    return payload, url
            time.sleep(0.5)
        return None, last_url

    def set_quantity(self, quantity: float) -> None:
        """Set Omni order quantity using the live quantity input test id."""
        selectors = [
            "xpath=(//*[@data-testid='quantity-input'])[last()]",
            "[data-testid='quantity-input']",
        ]
        selector = self._find_first_visible(selectors, timeout=25)
        if not selector:
            raise RuntimeError("[omni] quantity input not found")

        field = self._find_element(selector)
        self._human_type_into_field(field, str(quantity))

    def _is_market_mode_active(self) -> bool:
        try:
            return bool(
                self.driver.execute_script(
                    """
                    const root = document.querySelector('[data-testid="toggle-select"]') || document.body;
                    const buttons = Array.from(root.querySelectorAll('button'));
                    const market = buttons.find((el) => ((el.innerText || el.textContent || '') + '').trim() === 'Market');
                    if (!market) return false;
                    return market.classList.contains('text-blackwhite')
                      || market.classList.contains('text-azure')
                      || market.classList.contains('pointer-events-none')
                      || /border-azure/.test(market.className || '');
                    """
                )
            )
        except Exception:
            return False

    def ensure_market_mode(self, timeout_seconds: float = 8.0) -> bool:
        """Switch the Omni order ticket to Market mode."""
        if self._is_market_mode_active():
            return True
        clicked = self._click_first_visible(self._MARKET_TAB_SELECTORS, timeout=timeout_seconds)
        if not clicked:
            return False
        time.sleep(0.5)
        return self._is_market_mode_active()

    def _reduce_only_state(self) -> Optional[bool]:
        try:
            state = self.driver.execute_script(
                """
                const btn =
                  document.querySelector('button[data-testid="reduce-only-checkbox"]')
                  || Array.from(document.querySelectorAll('button')).find(
                    (el) => ((el.innerText || el.textContent || '') + '').trim().includes('Reduce Only')
                  );
                if (!btn) return null;
                const text = ((btn.innerText || btn.textContent || '') + '').trim().toLowerCase();
                const ariaPressed = btn.getAttribute('aria-pressed');
                if (ariaPressed === 'true') return true;
                if (ariaPressed === 'false') return false;
                return /bg-azure|text-azure|border-azure|checked|selected|active/.test(btn.className || '');
                """
            )
            return state if state is None else bool(state)
        except Exception:
            return None

    def set_reduce_only(self, enabled: bool, timeout_seconds: float = 8.0) -> bool:
        """Enable or disable the Reduce Only toggle."""
        current = self._reduce_only_state()
        if current is not None and current == enabled:
            return True

        clicked = self._click_first_visible(self._REDUCE_ONLY_SELECTORS, timeout=timeout_seconds)
        if not clicked:
            return False
        time.sleep(0.4)

        current = self._reduce_only_state()
        if current is None:
            return True
        return current == enabled

    def _select_omni_side(self, side: str, timeout_seconds: float = 8.0) -> bool:
        selectors = self._BUY_SIDE_SELECTORS if side.lower() == "buy" else self._SELL_SIDE_SELECTORS
        return self._click_first_visible(selectors, timeout=timeout_seconds)

    def _submit_omni_order(self, timeout_seconds: float = 8.0) -> bool:
        return self._click_first_visible(self._SUBMIT_BUTTON_SELECTORS, timeout=timeout_seconds)

    def _quantity_unit_label(self) -> str:
        try:
            label = self.driver.execute_script(
                """
                const button = document.querySelector('button[data-testid="input-mode-toggle"]');
                if (!button) return '';
                return ((button.innerText || button.textContent || '') + '').trim();
                """
            )
            return str(label or "").strip()
        except Exception:
            return ""

    def ensure_quantity_unit_symbol(self, symbol: str, timeout_seconds: float = 8.0) -> bool:
        """Ensure the ticket quantity unit is the instrument symbol, not USD."""
        target_symbol = symbol.strip().upper()
        current = self._quantity_unit_label().upper()
        if current and current != "$":
            return current == target_symbol

        clicked = self._click_first_visible(self._QUANTITY_UNIT_TOGGLE_SELECTORS, timeout=timeout_seconds)
        if not clicked:
            return False

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            current = self._quantity_unit_label().upper()
            if current and current != "$":
                return current == target_symbol
            time.sleep(0.25)
        return False

    def _pre_submit_delay(self) -> None:
        """Pause briefly before the final submit click to reduce UI risk triggers."""
        time.sleep(random.uniform(1.0, 2.0))

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        confirm: bool = False,
        order_kind: str = "market-ui",
        notes: Optional[str] = None,
        reduce_only: bool = False,
    ) -> Dict[str, Any]:
        """Place an Omni market order through the UI."""
        handle, info = self._get_handle_for_symbol(symbol)
        self.driver.switch_to.window(handle)
        self._human_pause()

        if not self.ensure_market_mode():
            raise RuntimeError("[omni] unable to switch ticket to Market mode")

        order_quantity = quantity if quantity is not None else info["quantity"]
        if not self.ensure_quantity_unit_symbol(symbol):
            raise RuntimeError(f"[omni] unable to switch quantity unit to {symbol}")
        self.set_quantity(order_quantity)

        if not self.set_reduce_only(reduce_only):
            raise RuntimeError(f"[omni] unable to set Reduce Only={reduce_only}")

        if not self._select_omni_side(side):
            raise RuntimeError(f"[omni] unable to select {side} side for {symbol}")

        quote = self.capture_quote(handle)
        requested_price = quote.ask if side.lower() == "buy" else quote.bid
        client_order_id = str(uuid.uuid4())
        status = "simulated" if self.dry_run else "clicked"
        omni_listener_db = OmniListenerDB()
        action_metadata = {
            "confirm": confirm,
            "dry_run": self.dry_run,
            "reduce_only": reduce_only,
            "selectors": {
                "market_tab": self._MARKET_TAB_SELECTORS,
                "side": self._BUY_SIDE_SELECTORS if side.lower() == "buy" else self._SELL_SIDE_SELECTORS,
                "submit": self._SUBMIT_BUTTON_SELECTORS,
                "reduce_only": self._REDUCE_ONLY_SELECTORS,
                "quantity_unit_toggle": self._QUANTITY_UNIT_TOGGLE_SELECTORS,
            },
            "quantity_unit": self._quantity_unit_label(),
        }

        if not self.dry_run:
            self._pre_submit_delay()
            self._reset_network_capture()
            submitted_at = datetime.now(timezone.utc)
            submitted = self._submit_omni_order()
            if not submitted:
                raise RuntimeError(f"[omni] unable to submit {side} order for {symbol}")
            if confirm:
                time.sleep(0.5)
            order_record, order_url, order_payload = self._wait_for_order_capture(
                symbol=symbol,
                side=side,
                quantity=order_quantity,
                submitted_at=submitted_at,
                timeout_seconds=self._CAPTURE_TIMEOUT_SECONDS,
            )
            position_payload, position_url = self._wait_for_position_capture(
                timeout_seconds=self._CAPTURE_TIMEOUT_SECONDS,
            )
            omni_listener_db.save_order_log(
                client_order_id=client_order_id,
                symbol=symbol,
                side=side.lower(),
                quantity=order_quantity,
                order=order_record,
                raw_url=order_url,
                raw_payload=order_payload,
                logged_at=utc8_now_str("%Y-%m-%d %H:%M:%S"),
            )
            omni_listener_db.save_position_snapshot(
                client_order_id=client_order_id,
                raw_url=position_url,
                raw_payload=position_payload,
                logged_at=utc8_now_str("%Y-%m-%d %H:%M:%S"),
                fallback_symbol=symbol,
            )
            action_metadata["omni_order_captured"] = bool(order_record)
            action_metadata["omni_position_captured"] = isinstance(position_payload, list) and bool(position_payload)
            action_metadata["omni_order_id"] = (order_record or {}).get("order_id")

        created_at = utc8_now_str("%Y-%m-%d %H:%M:%S")
        self.db.save_frontend_order_event(
            exchange=self.config.name,
            symbol=symbol,
            market=info["market"],
            side=side.lower(),
            quantity=order_quantity,
            status=status,
            order_kind=order_kind,
            requested_price=requested_price,
            clicked_price=requested_price,
            client_order_id=client_order_id,
            page_url=self.driver.get_current_url(),
            notes=notes,
            metadata=action_metadata,
            created_at=created_at,
        )

        return {
            "exchange": self.config.name,
            "symbol": symbol,
            "side": side.lower(),
            "quantity": order_quantity,
            "status": status,
            "requested_price": requested_price,
            "client_order_id": client_order_id,
            "reduce_only": reduce_only,
        }

    def close_position(
        self,
        symbol: str,
        position_side: str,
        quantity: Optional[float] = None,
        confirm: bool = False,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Close an existing Omni position via Reduce Only market order."""
        normalized = position_side.strip().lower()
        if normalized not in {"long", "short"}:
            raise ValueError(f"[omni] unsupported position_side={position_side!r}")
        closing_side = "sell" if normalized == "long" else "buy"
        return self.place_order(
            symbol=symbol,
            side=closing_side,
            quantity=quantity,
            confirm=confirm,
            order_kind="market-close-ui",
            notes=notes or f"close_{normalized}",
            reduce_only=True,
        )


class TradeXyzSeleniumClient(FrontendExchangeClient):
    """SeleniumBase flow for trade.xyz with DOM heuristics for order-book prices."""

    _MARKET_TAB_SELECTORS = [
        "xpath=(//button[normalize-space()='Market'])[last()]",
        "xpath=(//*[@role='tablist']/following::button[normalize-space()='Market'])[1]",
    ]
    _LONG_SIDE_SELECTORS = [
        "xpath=(//*[@role='tab' and normalize-space()='Long'])[last()]",
        "xpath=(//button[normalize-space()='Long'])[last()]",
    ]
    _SHORT_SIDE_SELECTORS = [
        "xpath=(//*[@role='tab' and normalize-space()='Short'])[last()]",
        "xpath=(//button[normalize-space()='Short'])[last()]",
    ]
    _REDUCE_ONLY_SELECTORS = [
        "xpath=(//input[@type='checkbox' and @aria-label='Reduce Only']/following::div[contains(normalize-space(), 'Reduce Only')])[1]",
        "xpath=(//*[contains(normalize-space(), 'Reduce Only')])[last()]",
    ]
    _SUBMIT_LONG_SELECTORS = [
        "xpath=(//button[contains(normalize-space(), 'Long GOLD')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Long XAU')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Long ')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Long')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Buy')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Place Order')])[last()]",
    ]
    _SUBMIT_SHORT_SELECTORS = [
        "xpath=(//button[contains(normalize-space(), 'Short GOLD')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Short XAU')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Short ')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Short')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Sell')])[last()]",
        "xpath=(//button[contains(normalize-space(), 'Place Order')])[last()]",
    ]
    _CAPTURE_TIMEOUT_SECONDS = 12.0

    def install_quote_cache(self) -> None:
        """Continuously cache parsed order-book top levels in the current trade.xyz tab."""
        self.driver.execute_script(
            """
            if (window.__quoteCacheInstalled) return true;
            window.__quoteCacheInstalled = true;
            window.__latestQuoteCache = null;

            const parsePriceRow = (button, index) => {
              const text = (button.innerText || '').trim().replace(/\\s+/g, ' ');
              const match = text.match(/^\\$?([\\d,]+(?:\\.\\d+)?)\\s+([\\d,]+(?:\\.\\d+)?)\\s+([\\d,]+(?:\\.\\d+)?)/);
              if (!match) return null;
              return {
                index,
                text,
                price: Number(match[1].replace(/,/g, '')),
                size: Number(match[2].replace(/,/g, '')),
                total: Number(match[3].replace(/,/g, ''))
              };
            };

            const updateQuoteCache = () => {
              const buttons = Array.from(document.querySelectorAll('button'));
              const orderBookIndex = buttons.findIndex((button) => /Order Book/i.test(button.innerText || ''));
              const rows = buttons
                .map((button, index) => parsePriceRow(button, index))
                .filter(Boolean)
                .filter((row) => orderBookIndex === -1 || row.index > orderBookIndex);

              let splitIndex = -1;
              for (let i = 0; i < rows.length - 1; i += 1) {
                if (rows[i + 1].total > rows[i].total) {
                  splitIndex = i;
                  break;
                }
              }

              const askRow = splitIndex >= 0 ? rows[splitIndex] : rows[0];
              const bidRow = splitIndex >= 0 ? rows[splitIndex + 1] : rows[1];
              if (!askRow || !bidRow) return;
              window.__latestQuoteCache = {
                ask: askRow.price,
                bid: bidRow.price,
                askQuantity: askRow.size,
                bidQuantity: bidRow.size,
                askText: askRow.text,
                bidText: bidRow.text,
                pageUrl: window.location.href,
                cachedAt: new Date().toISOString(),
              };
            };

            updateQuoteCache();
            window.__quoteCacheTimer = window.__quoteCacheTimer || window.setInterval(updateQuoteCache, 500);
            return true;
            """
        )

    def capture_cached_quote(self, persist: bool = True) -> QuoteSnapshot:
        if not self.tabs:
            raise RuntimeError("[trade_xyz] no market tabs opened")
        info = self.tabs[list(self.tabs.keys())[0]]
        data = self.driver.execute_script("return window.__latestQuoteCache || null;")
        if not data or data.get("ask") is None or data.get("bid") is None:
            raise RuntimeError("[trade_xyz] quote cache is empty")

        snapshot = QuoteSnapshot(
            exchange=self.config.name,
            symbol=info["symbol"],
            market=info["market"],
            ask=float(data["ask"]),
            bid=float(data["bid"]),
            ask_quantity=data.get("askQuantity"),
            bid_quantity=data.get("bidQuantity"),
            input_quantity=info["quantity"],
            captured_at=utc8_now_str("%Y-%m-%d %H:%M:%S"),
            minute=utc8_now_str("%Y-%m-%d %H:%M"),
            page_url=str(data.get("pageUrl") or self.driver.get_current_url()),
            metadata={
                "ask_text": data.get("askText"),
                "bid_text": data.get("bidText"),
                "source": "trade_xyz_quote_cache",
                "cached_at": data.get("cachedAt"),
            },
        )
        if persist:
            self.persist_quote(snapshot)
        return snapshot

    def get_primary_handle(self) -> Optional[str]:
        """Return the main trade.xyz market tab handle."""
        if not self.tabs:
            return None
        return list(self.tabs.keys())[0]

    def switch_to_primary_tab(self) -> bool:
        """Switch webdriver focus back to the main trade.xyz market tab."""
        handle = self.get_primary_handle()
        if not handle:
            return False
        try:
            self.driver.switch_to.window(handle)
            return True
        except Exception:
            return False

    def click_connect_wallet_button(self, timeout_seconds: float = 20.0) -> bool:
        """Click the trade.xyz Connect Wallet button."""
        if not self.config.selectors.connect_wallet:
            return False
        clicked = self._click_first_visible(
            self.config.selectors.connect_wallet,
            timeout=timeout_seconds,
        )
        if clicked:
            return True
        try:
            clicked = self.driver.execute_script(
                """
                const labels = ['connect wallet', 'connect'];
                const seen = new Set();
                const queue = [document];
                const nodes = [];
                while (queue.length) {
                  const root = queue.shift();
                  if (!root || seen.has(root)) continue;
                  seen.add(root);
                  if (!root.querySelectorAll) continue;
                  nodes.push(...Array.from(root.querySelectorAll('button, [role="button"], a[role="button"], div[tabindex], span[tabindex]')));
                  for (const el of Array.from(root.querySelectorAll('*'))) {
                    if (el.shadowRoot) queue.push(el.shadowRoot);
                  }
                }
                for (const el of nodes) {
                  if (el.disabled || el.getAttribute('aria-disabled') === 'true') continue;
                  const text = ((el.innerText || el.textContent || '') + '').trim().toLowerCase();
                  if (!text) continue;
                  if (labels.some((label) => text === label || text.includes(label))) {
                    el.click();
                    return true;
                  }
                }
                return false;
                """
            )
            return bool(clicked)
        except Exception:
            return False

    def click_okx_wallet_option(self, timeout: float = 12.0) -> bool:
        """Select OKX in the trade.xyz wallet chooser."""
        return self._click_okx_wallet_option(timeout=timeout)

    def confirm_okx_extension_window(
        self,
        handles_before: set[str],
        timeout: float = 45.0,
        open_extension_first: bool = True,
        wc_pairing_wait_seconds: float = 12.0,
    ) -> bool:
        """Handle the OKX extension window for trade.xyz connect/sign."""
        return self._try_confirm_okx_extension_window(
            handles_before,
            timeout=timeout,
            open_extension_first=open_extension_first,
            wc_pairing_wait_seconds=wc_pairing_wait_seconds,
        )

    def capture_quote(self, handle: str, persist: bool = True) -> QuoteSnapshot:
        info = self.tabs[handle]
        self.driver.switch_to.window(handle)
        self._human_pause()

        quote_data = self.driver.execute_script(
            """
            const parsePriceRow = (button, index) => {
              const text = (button.innerText || '').trim().replace(/\\s+/g, ' ');
              const match = text.match(/^\\$?([\\d,]+(?:\\.\\d+)?)\\s+([\\d,]+(?:\\.\\d+)?)\\s+([\\d,]+(?:\\.\\d+)?)/);
              if (!match) return null;
              return {
                index,
                text,
                price: Number(match[1].replace(/,/g, '')),
                size: Number(match[2].replace(/,/g, '')),
                total: Number(match[3].replace(/,/g, ''))
              };
            };

            const buttons = Array.from(document.querySelectorAll('button'));
            const orderBookIndex = buttons.findIndex((button) => /Order Book/i.test(button.innerText || ''));
            const rows = buttons
              .map((button, index) => parsePriceRow(button, index))
              .filter(Boolean)
              .filter((row) => orderBookIndex === -1 || row.index > orderBookIndex);

            let splitIndex = -1;
            for (let i = 0; i < rows.length - 1; i += 1) {
              if (rows[i + 1].total > rows[i].total) {
                splitIndex = i;
                break;
              }
            }

            const askRow = splitIndex >= 0 ? rows[splitIndex] : rows[0];
            const bidRow = splitIndex >= 0 ? rows[splitIndex + 1] : rows[1];
            return {
              ask: askRow ? askRow.price : null,
              bid: bidRow ? bidRow.price : null,
              askQuantity: askRow ? askRow.size : null,
              bidQuantity: bidRow ? bidRow.size : null,
              askText: askRow ? askRow.text : null,
              bidText: bidRow ? bidRow.text : null,
              title: document.title,
              rowCount: rows.length,
            };
            """
        )
        if not quote_data or quote_data.get("ask") is None or quote_data.get("bid") is None:
            raise RuntimeError("[trade_xyz] unable to parse order book from live DOM")

        snapshot = QuoteSnapshot(
            exchange=self.config.name,
            symbol=info["symbol"],
            market=info["market"],
            ask=float(quote_data["ask"]),
            bid=float(quote_data["bid"]),
            ask_quantity=quote_data.get("askQuantity"),
            bid_quantity=quote_data.get("bidQuantity"),
            input_quantity=info["quantity"],
            captured_at=utc8_now_str("%Y-%m-%d %H:%M:%S"),
            minute=utc8_now_str("%Y-%m-%d %H:%M"),
            page_url=self.driver.get_current_url(),
            metadata={
                "ask_text": quote_data.get("askText"),
                "bid_text": quote_data.get("bidText"),
                "page_title": quote_data.get("title"),
                "row_count": quote_data.get("rowCount"),
                "window_handle": handle,
                "source": "trade_xyz_seleniumbase",
            },
        )
        if persist:
            self.persist_quote(snapshot)
        return snapshot

    def set_quantity(self, quantity: float) -> None:
        """Set trade.xyz market quantity using the Amount numeric input."""
        selector = "xpath=(//p[normalize-space()='Amount']/following::input[@type='text' and @inputmode='numeric'])[1]"
        if not self._is_element_visible(selector, timeout=25):
            raise RuntimeError("[trade_xyz] amount input not found")
        field = self._find_element(selector)
        self._human_type_into_field(field, str(quantity))

    def _is_trade_xyz_market_mode_active(self) -> bool:
        try:
            return bool(
                self.driver.execute_script(
                    """
                    const button = Array.from(document.querySelectorAll('button')).find(
                      (el) => ((el.innerText || el.textContent || '') + '').trim() === 'Market'
                    );
                    if (!button) return false;
                    const className = button.className || '';
                    return /bg-brand|text-brand-white|border-brand/.test(className) || button.getAttribute('aria-selected') === 'true';
                    """
                )
            )
        except Exception:
            return False

    def ensure_market_mode(self, timeout_seconds: float = 8.0) -> bool:
        if self._is_trade_xyz_market_mode_active():
            return True
        if not self._click_first_visible(self._MARKET_TAB_SELECTORS, timeout=timeout_seconds):
            return False
        time.sleep(0.5)
        return self._is_trade_xyz_market_mode_active()

    def _is_trade_xyz_side_active(self, side: str) -> bool:
        target = "Long" if side.lower() == "buy" else "Short"
        try:
            return bool(
                self.driver.execute_script(
                    """
                    const target = arguments[0];
                    const tab = Array.from(document.querySelectorAll('[role="tab"], button')).find(
                      (el) => ((el.innerText || el.textContent || '') + '').trim() === target
                    );
                    if (!tab) return false;
                    const className = tab.className || '';
                    return tab.getAttribute('aria-selected') === 'true' || /bg-brand|text-brand-white|border-brand/.test(className);
                    """,
                    target,
                )
            )
        except Exception:
            return False

    def _select_trade_xyz_side(self, side: str, timeout_seconds: float = 8.0) -> bool:
        if self._is_trade_xyz_side_active(side):
            return True
        selectors = self._LONG_SIDE_SELECTORS if side.lower() == "buy" else self._SHORT_SIDE_SELECTORS
        if not self._click_first_visible(selectors, timeout=timeout_seconds):
            return False
        time.sleep(0.4)
        return self._is_trade_xyz_side_active(side)

    def _trade_xyz_quantity_unit_label(self) -> str:
        try:
            label = self.driver.execute_script(
                """
                const amountLabel = Array.from(document.querySelectorAll('p')).find(
                  (el) => ((el.innerText || el.textContent || '') + '').trim() === 'Amount'
                );
                if (!amountLabel) return '';
                const container = amountLabel.parentElement;
                if (!container) return '';
                const unit = Array.from(container.querySelectorAll('p')).map(
                  (el) => ((el.innerText || el.textContent || '') + '').trim()
                ).find((text) => text && text !== 'Amount');
                return unit || '';
                """
            )
            return str(label or "").strip()
        except Exception:
            return ""

    def ensure_quantity_unit_symbol(self, symbol: str, timeout_seconds: float = 8.0) -> bool:
        target_symbol = symbol.strip().upper()
        current = self._trade_xyz_quantity_unit_label().upper()
        if current and current != "USDC":
            return current == target_symbol
        try:
            clickable = self.driver.execute_script(
                """
                const amountLabel = Array.from(document.querySelectorAll('p')).find(
                  (el) => ((el.innerText || el.textContent || '') + '').trim() === 'Amount'
                );
                if (!amountLabel || !amountLabel.parentElement) return null;
                const container = amountLabel.parentElement;
                const toggle = container.querySelector('img[cursor], img') || container.querySelector('svg');
                if (!toggle) return null;
                return toggle.closest('button, div, span') || toggle;
                """
            )
            if clickable:
                self._human_click_element(clickable)
                clicked = True
            else:
                clicked = False
        except Exception:
            clicked = False
        if not clicked:
            return False
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            current = self._trade_xyz_quantity_unit_label().upper()
            if current and current != "USDC":
                return current == target_symbol
            time.sleep(0.25)
        return False

    def _trade_xyz_reduce_only_state(self) -> Optional[bool]:
        try:
            return self.driver.execute_script(
                """
                const checkbox = document.querySelector('input[type="checkbox"][aria-label="Reduce Only"]');
                if (!checkbox) return null;
                return !!checkbox.checked;
                """
            )
        except Exception:
            return None

    def set_reduce_only(self, enabled: bool, timeout_seconds: float = 8.0) -> bool:
        current = self._trade_xyz_reduce_only_state()
        if current is not None and current == enabled:
            return True
        if not enabled and current is None:
            return True
        if not self._click_first_visible(self._REDUCE_ONLY_SELECTORS, timeout=timeout_seconds):
            return not enabled
        time.sleep(0.4)
        current = self._trade_xyz_reduce_only_state()
        if current is None:
            return not enabled
        return current == enabled

    def _pre_submit_delay(self) -> None:
        time.sleep(random.uniform(1.0, 2.0))

    def _submit_trade_xyz_order(self, side: str, timeout_seconds: float = 8.0) -> bool:
        selectors = self._SUBMIT_LONG_SELECTORS if side.lower() == "buy" else self._SUBMIT_SHORT_SELECTORS
        return self._click_first_visible(selectors, timeout=timeout_seconds)

    def _ensure_network_capture_hooks(self) -> None:
        self.driver.execute_script(
            """
            if (window.__tradeXyzCaptureInstalled) {
              return;
            }
            window.__tradeXyzCaptureInstalled = true;
            window.__tradeXyzCapturedResponses = [];

            const pushRecord = (record) => {
              window.__tradeXyzCapturedResponses.push(record);
              if (window.__tradeXyzCapturedResponses.length > 50) {
                window.__tradeXyzCapturedResponses = window.__tradeXyzCapturedResponses.slice(-50);
              }
            };

            const shouldTrack = (url, method) => {
              const normalizedUrl = String(url || '');
              const normalizedMethod = String(method || '').toUpperCase();
              return normalizedMethod === 'POST' && normalizedUrl.includes('api-ui.hyperliquid.xyz/exchange');
            };

            const originalFetch = window.fetch;
            window.fetch = async function(...args) {
              const resource = args[0];
              const init = args[1] || {};
              const url = typeof resource === 'string' ? resource : (resource && resource.url) || '';
              const method = init.method || (resource && resource.method) || 'GET';
              const requestBody = init.body || null;
              const response = await originalFetch.apply(this, args);
              if (shouldTrack(url, method)) {
                try {
                  const clone = response.clone();
                  const responseBody = await clone.text();
                  pushRecord({
                    transport: 'fetch',
                    method: String(method).toUpperCase(),
                    url: clone.url || url,
                    status: response.status,
                    captured_at: new Date().toISOString(),
                    request_body: requestBody,
                    body: responseBody,
                  });
                } catch (error) {
                }
              }
              return response;
            };

            const originalOpen = XMLHttpRequest.prototype.open;
            const originalSend = XMLHttpRequest.prototype.send;
            XMLHttpRequest.prototype.open = function(method, url, ...rest) {
              this.__tradeXyzCaptureMeta = { method, url };
              return originalOpen.call(this, method, url, ...rest);
            };
            XMLHttpRequest.prototype.send = function(body) {
              this.__tradeXyzRequestBody = body || null;
              this.addEventListener('load', function() {
                const meta = this.__tradeXyzCaptureMeta || {};
                if (!shouldTrack(meta.url, meta.method)) {
                  return;
                }
                try {
                  pushRecord({
                    transport: 'xhr',
                    method: String(meta.method || '').toUpperCase(),
                    url: this.responseURL || meta.url || '',
                    status: this.status,
                    captured_at: new Date().toISOString(),
                    request_body: this.__tradeXyzRequestBody || null,
                    body: typeof this.responseText === 'string' ? this.responseText : '',
                  });
                } catch (error) {
                }
              });
              return originalSend.call(this, body);
            };
            """
        )

    def _reset_network_capture(self) -> None:
        self._ensure_network_capture_hooks()
        self.driver.execute_script("window.__tradeXyzCapturedResponses = [];")

    def _get_captured_network_records(self) -> List[Dict[str, Any]]:
        self._ensure_network_capture_hooks()
        result = self.driver.execute_script(
            "return Array.isArray(window.__tradeXyzCapturedResponses) ? window.__tradeXyzCapturedResponses : [];"
        )
        return result if isinstance(result, list) else []

    def _parse_capture_body(self, body: Any) -> Any:
        if isinstance(body, (dict, list)):
            return body
        if not isinstance(body, str) or not body.strip():
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return None

    def _match_trade_xyz_order_response(self, payload: Any, quantity: float) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        if payload.get("status") != "ok":
            return None
        response = payload.get("response") or {}
        if response.get("type") != "order":
            return None
        data = response.get("data") or {}
        statuses = data.get("statuses")
        if not isinstance(statuses, list):
            return None
        for item in statuses:
            if not isinstance(item, dict):
                continue
            filled = item.get("filled")
            resting = item.get("resting")
            candidate = filled or resting or item
            if not isinstance(candidate, dict):
                continue
            total_size = candidate.get("totalSz") or candidate.get("sz")
            if total_size is None:
                continue
            try:
                if abs(float(total_size) - float(quantity)) <= 1e-9:
                    return candidate
            except (TypeError, ValueError):
                continue
        return None

    def _wait_for_order_capture(self, quantity: float, timeout_seconds: float) -> tuple[Optional[Dict[str, Any]], Optional[str], Any]:
        deadline = time.time() + timeout_seconds
        seen_keys: set[tuple[str, str]] = set()
        last_url = None
        last_payload = None
        while time.time() < deadline:
            for record in self._get_captured_network_records():
                url = str(record.get("url") or "")
                captured_at = str(record.get("captured_at") or "")
                key = (url, captured_at)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                payload = self._parse_capture_body(record.get("body"))
                last_url = url
                last_payload = payload
                matched = self._match_trade_xyz_order_response(payload, quantity)
                if matched:
                    return matched, url, payload
            time.sleep(0.5)
        return None, last_url, last_payload

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        confirm: bool = False,
        order_kind: str = "market-ui",
        notes: Optional[str] = None,
        reduce_only: bool = False,
    ) -> Dict[str, Any]:
        """Place a trade.xyz market order through the live UI."""
        handle, info = self._get_handle_for_symbol(symbol)
        self.driver.switch_to.window(handle)
        self._human_pause()

        if not self._select_trade_xyz_side(side):
            raise RuntimeError(f"[trade_xyz] unable to select {side} tab for {symbol}")
        if not self.ensure_market_mode():
            raise RuntimeError("[trade_xyz] unable to switch ticket to Market mode")

        order_quantity = quantity if quantity is not None else info["quantity"]
        if not self.ensure_quantity_unit_symbol(symbol):
            raise RuntimeError(f"[trade_xyz] unable to switch quantity unit to {symbol}")
        self.set_quantity(order_quantity)

        if not self.set_reduce_only(reduce_only):
            raise RuntimeError(f"[trade_xyz] unable to set Reduce Only={reduce_only}")

        quote = self.capture_quote(handle)
        requested_price = quote.ask if side.lower() == "buy" else quote.bid
        client_order_id = str(uuid.uuid4())
        status = "simulated" if self.dry_run else "clicked"
        order_record = None
        order_url = None
        order_payload = None
        action_metadata = {
            "confirm": confirm,
            "dry_run": self.dry_run,
            "reduce_only": reduce_only,
            "selectors": {
                "market_tab": self._MARKET_TAB_SELECTORS,
                "side": self._LONG_SIDE_SELECTORS if side.lower() == "buy" else self._SHORT_SIDE_SELECTORS,
                "submit": self._SUBMIT_LONG_SELECTORS if side.lower() == "buy" else self._SUBMIT_SHORT_SELECTORS,
                "reduce_only": self._REDUCE_ONLY_SELECTORS,
            },
            "quantity_unit": self._trade_xyz_quantity_unit_label(),
        }

        if not self.dry_run:
            self._pre_submit_delay()
            self._reset_network_capture()
            submitted = self._submit_trade_xyz_order(side)
            if not submitted:
                raise RuntimeError(f"[trade_xyz] unable to submit {side} order for {symbol}")
            if confirm:
                time.sleep(0.5)
            order_record, order_url, order_payload = self._wait_for_order_capture(
                quantity=order_quantity,
                timeout_seconds=self._CAPTURE_TIMEOUT_SECONDS,
            )
            action_metadata["trade_xyz_order_captured"] = bool(order_record)
            action_metadata["trade_xyz_order_response"] = order_payload
            action_metadata["trade_xyz_order_url"] = order_url
            action_metadata["trade_xyz_oid"] = (order_record or {}).get("oid")

        created_at = utc8_now_str("%Y-%m-%d %H:%M:%S")
        self.db.save_frontend_order_event(
            exchange=self.config.name,
            symbol=symbol,
            market=info["market"],
            side=side.lower(),
            quantity=order_quantity,
            status=status,
            order_kind=order_kind,
            requested_price=requested_price,
            clicked_price=requested_price,
            client_order_id=client_order_id,
            external_order_id=str((order_record or {}).get("oid")) if not self.dry_run and order_record else None,
            page_url=self.driver.get_current_url(),
            notes=notes,
            metadata=action_metadata,
            created_at=created_at,
        )

        return {
            "exchange": self.config.name,
            "symbol": symbol,
            "side": side.lower(),
            "quantity": order_quantity,
            "status": status,
            "requested_price": requested_price,
            "client_order_id": client_order_id,
            "reduce_only": reduce_only,
            "external_order_id": str((order_record or {}).get("oid")) if not self.dry_run and order_record else None,
        }

    def close_position(
        self,
        symbol: str,
        position_side: str,
        quantity: Optional[float] = None,
        confirm: bool = False,
        notes: Optional[str] = None,
    ) -> Dict[str, Any]:
        normalized = position_side.strip().lower()
        if normalized not in {"long", "short"}:
            raise ValueError(f"[trade_xyz] unsupported position_side={position_side!r}")
        closing_side = "sell" if normalized == "long" else "buy"
        return self.place_order(
            symbol=symbol,
            side=closing_side,
            quantity=quantity,
            confirm=confirm,
            order_kind="market-close-ui",
            notes=notes or f"close_{normalized}",
            reduce_only=True,
        )


class CrossExchangeArbitrageRunner:
    """Monitor price differences and optionally submit paired UI orders."""

    def __init__(
        self,
        clients: List[FrontendExchangeClient],
        db: Optional[ArbInfoDB] = None,
        threshold_abs: float = 0.0,
        threshold_bps: float = 0.0,
        dry_run: bool = True,
        confirm_orders: bool = False,
    ):
        self.clients = clients
        self.db = db or get_arb_info_db()
        self.threshold_abs = threshold_abs
        self.threshold_bps = threshold_bps
        self.dry_run = dry_run
        self.confirm_orders = confirm_orders

    def capture_once(self) -> List[QuoteSnapshot]:
        """Read quotes from all clients once."""
        snapshots: List[QuoteSnapshot] = []
        for client in self.clients:
            snapshots.extend(client.capture_all_quotes())
        return snapshots

    def evaluate_and_maybe_trade(self, snapshots: List[QuoteSnapshot]) -> List[dict]:
        """Evaluate spread across exchanges for symbols present in all sessions."""
        grouped: Dict[str, List[QuoteSnapshot]] = {}
        for snapshot in snapshots:
            grouped.setdefault(snapshot.symbol.upper(), []).append(snapshot)

        actions: List[dict] = []
        for symbol, quotes in grouped.items():
            if len(quotes) < 2:
                continue

            buy_leg = min(quotes, key=lambda item: item.ask)
            sell_leg = max(quotes, key=lambda item: item.bid)
            spread_abs = sell_leg.bid - buy_leg.ask
            spread_bps = 0.0
            if buy_leg.ask:
                spread_bps = (spread_abs / buy_leg.ask) * 10000

            target_quantity = min(
                quote.input_quantity or 0.0 for quote in (buy_leg, sell_leg)
            )
            triggered = (
                spread_abs >= self.threshold_abs and spread_bps >= self.threshold_bps
            )
            observed_at = utc8_now_str("%Y-%m-%d %H:%M:%S")
            self.db.save_frontend_spread_signal(
                symbol=symbol,
                market=buy_leg.market,
                buy_exchange=buy_leg.exchange,
                sell_exchange=sell_leg.exchange,
                buy_price=buy_leg.ask,
                sell_price=sell_leg.bid,
                spread_abs=spread_abs,
                spread_bps=spread_bps,
                target_quantity=target_quantity,
                threshold_abs=self.threshold_abs,
                threshold_bps=self.threshold_bps,
                triggered=triggered,
                observed_at=observed_at,
                metadata={
                    "buy_market": buy_leg.market,
                    "sell_market": sell_leg.market,
                },
            )

            action = {
                "symbol": symbol,
                "buy_exchange": buy_leg.exchange,
                "sell_exchange": sell_leg.exchange,
                "buy_price": buy_leg.ask,
                "sell_price": sell_leg.bid,
                "spread_abs": spread_abs,
                "spread_bps": spread_bps,
                "triggered": triggered,
            }
            actions.append(action)

            if triggered:
                self.execute_pair_trade(
                    symbol=symbol,
                    buy_exchange=buy_leg.exchange,
                    sell_exchange=sell_leg.exchange,
                    quantity=target_quantity,
                )

        return actions

    def execute_pair_trade(
        self,
        symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        quantity: float,
    ) -> List[dict]:
        """Execute the paired UI click flow."""
        if quantity <= 0:
            raise ValueError(f"Invalid target quantity for {symbol}: {quantity}")

        results: List[dict] = []
        for client in self.clients:
            if client.config.name == buy_exchange:
                results.append(
                    client.place_order(
                        symbol=symbol,
                        side="buy",
                        quantity=quantity,
                        confirm=self.confirm_orders,
                        notes=f"paired_with={sell_exchange}",
                    )
                )
            elif client.config.name == sell_exchange:
                results.append(
                    client.place_order(
                        symbol=symbol,
                        side="sell",
                        quantity=quantity,
                        confirm=self.confirm_orders,
                        notes=f"paired_with={buy_exchange}",
                    )
                )
        return results

    def monitor_loop(self, interval_seconds: float) -> None:
        """Continuous monitor and optional execution loop."""
        while True:
            started = time.time()
            snapshots = self.capture_once()
            self.evaluate_and_maybe_trade(snapshots)
            elapsed = time.time() - started
            time.sleep(max(0, interval_seconds - elapsed))


def build_default_site_configs() -> Dict[str, ExchangeSiteConfig]:
    """构建默认交易所配置。

    这里主要定义：
    1. 每个交易所支持的市场路径
    2. 默认下单数量
    3. 抓价/连钱包/下单相关 selector
    环境变量可覆盖默认市场路径与数量。
    """
    os.makedirs(DEFAULT_USER_DATA_DIR, exist_ok=True)

    omni_market_override = load_json_env("OMNI_MARKETS_JSON") or {}
    trade_market_override = load_json_env("TRADE_XYZ_MARKETS_JSON") or {}

    omni_markets = {
        "XAUT": MarketConfig(
            symbol="XAUT",
            path=omni_market_override.get("XAUT", {}).get("path", "perpetual/XAUT"),
            quantity=float(omni_market_override.get("XAUT", {}).get("quantity", 1.0)),
        ),
        "GOLD": MarketConfig(
            symbol="GOLD",
            path=omni_market_override.get("GOLD", {}).get("path", "perpetual/GOLD"),
            quantity=float(omni_market_override.get("GOLD", {}).get("quantity", 1.0)),
        ),
    }
    trade_markets = {
        "XAU": MarketConfig(
            symbol="XAU",
            path=trade_market_override.get("XAU", {}).get("path", "?market=XAU"),
            quantity=float(trade_market_override.get("XAU", {}).get("quantity", 1.0)),
        ),
        "GOLD": MarketConfig(
            symbol="GOLD",
            path=trade_market_override.get("GOLD", {}).get("path", "?market=GOLD"),
            quantity=float(trade_market_override.get("GOLD", {}).get("quantity", 1.0)),
        ),
    }

    return {
        "omni": ExchangeSiteConfig(
            name="omni",
            base_url="https://omni.variational.io/",
            markets=omni_markets,
            selectors=SelectorSet(
                ask=["[data-testid='ask-price-display']"],
                bid=["[data-testid='bid-price-display']"],
                quantity_input=[
                    "xpath=(//*[@data-testid='quantity-input'])[last()]",
                    "[data-testid='quantity-input']",
                ],
                buy_button=[
                    "xpath=(//*[@data-testid='toggle-select']/following::button[contains(normalize-space(), 'Buy $')])[1]",
                    "xpath=(//button[contains(normalize-space(), 'Buy $')])[last()]",
                ],
                sell_button=[
                    "xpath=(//*[@data-testid='toggle-select']/following::button[contains(normalize-space(), 'Sell $')])[1]",
                    "xpath=(//button[contains(normalize-space(), 'Sell $')])[last()]",
                ],
                submit_button_buy=[
                    "button:has-text('Confirm Buy')",
                    "button:has-text('Place Buy Order')",
                ],
                submit_button_sell=[
                    "button:has-text('Confirm Sell')",
                    "button:has-text('Place Sell Order')",
                ],
                connect_wallet=[
                    "button[data-testid='connect-button']",
                    "xpath=(//button[@data-testid='connect-button'])[1]",
                    "xpath=(//button[normalize-space()='Connect Wallet'])[1]",
                ],
                wallet_option=[
                    "text='OKX Wallet'",
                    "wui-list-item:has-text('OKX Wallet')",
                ],
            ),
        ),
        "trade_xyz": ExchangeSiteConfig(
            name="trade_xyz",
            base_url="https://app.trade.xyz/",
            markets=trade_markets,
            selectors=SelectorSet(
                ask=[
                    "[data-testid='ask-price-display']",
                    "[data-testid='best-ask']",
                    "[data-testid='orderbook-best-ask']",
                ],
                bid=[
                    "[data-testid='bid-price-display']",
                    "[data-testid='best-bid']",
                    "[data-testid='orderbook-best-bid']",
                ],
                quantity_input=[
                    "[data-testid='quantity-input']",
                    "input[placeholder*='Qty']",
                    "input[placeholder*='Size']",
                    "xpath=(//p[normalize-space()='Amount']/following::input[@type='text'])[1]",
                    "xpath=(//*[@role='tablist']/following::input[@type='text'])[1]",
                ],
                buy_button=[
                    "button[data-testid='buy-button']",
                    "button[data-side='buy']",
                    "xpath=(//*[@role='tab' and normalize-space()='Long'])[last()]",
                    "text='Long'",
                ],
                sell_button=[
                    "button[data-testid='sell-button']",
                    "button[data-side='sell']",
                    "xpath=(//*[@role='tab' and normalize-space()='Short'])[last()]",
                    "text='Short'",
                ],
                submit_button_buy=[
                    "xpath=(//button[normalize-space()='Connect Wallet'])[last()]",
                    "xpath=(//button[contains(normalize-space(), 'Long')])[last()]",
                    "xpath=(//button[contains(normalize-space(), 'Buy')])[last()]",
                    "button:has-text('Confirm Buy')",
                    "button:has-text('Place Order')",
                ],
                submit_button_sell=[
                    "xpath=(//button[normalize-space()='Connect Wallet'])[last()]",
                    "xpath=(//button[contains(normalize-space(), 'Short')])[last()]",
                    "xpath=(//button[contains(normalize-space(), 'Sell')])[last()]",
                    "button:has-text('Confirm Sell')",
                    "button:has-text('Place Order')",
                ],
                connect_wallet=[
                    "button:has-text('Connect Wallet')",
                    "button:has-text('Connect')",
                ],
                wallet_option=[
                    "text='OKX Wallet'",
                    "text='OKX'",
                ],
            ),
            page_load_seconds=8.0,
        ),
    }


def build_driver(
    driver_kwargs: Optional[dict] = None,
    user_data_dir: Optional[str] = None,
) -> Driver:
    """构建 SeleniumBase Driver。

    user_data_dir 决定使用哪个 Chrome 用户空间：
    - 看盘/抓价：通常用 viewer profile
    - 连钱包/下单：通常用 trader profile
    """
    resolved_dir = os.path.expanduser(user_data_dir or DEFAULT_USER_DATA_DIR)
    os.makedirs(resolved_dir, exist_ok=True)
    kwargs = {
        "uc": True,
        "headless": False,
        "user_data_dir": resolved_dir,
    }
    if driver_kwargs:
        kwargs.update(driver_kwargs)
    return Driver(**kwargs)
