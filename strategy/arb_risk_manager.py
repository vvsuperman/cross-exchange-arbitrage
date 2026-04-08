"""Risk management daemon for arbitrage trading."""
import os
import sys
import json
import time
import csv
import logging
import asyncio
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from edgex_sdk import (
    Client,
    CreateOrderParams,
    GetOrderBookDepthParams,
    OrderSide,
    OrderType,
    TimeInForce,
    CancelOrderParams,
    WebSocketManager,
)
from websocket_manager import WebSocketManagerWrapper

# We must adjust sys.path to ensure we can import monitor.redis_client safely
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from monitor.redis_client import RedisPriceClient

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('arb_risk')

# 探针私有 ORDER_UPDATE：FOK 未成交通常为 CANCELED；亦可能短暂 OPEN/PENDING
_PROBE_ORDER_DONE_STATUSES = frozenset({
    'OPEN', 'PENDING', 'NEW', 'CANCELED', 'FILLED', 'REJECTED', 'PARTIALLY_FILLED',
})


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _arb_env_path() -> str:
    return os.path.join(_project_root(), "arb.env")


def _arb_risk_env_path() -> str:
    return os.path.join(_project_root(), "arb_risk.env")


def _load_arb_env() -> None:
    load_dotenv(_arb_env_path())
    # 风控专用参数（覆盖 arb.env 中同名键）
    load_dotenv(_arb_risk_env_path(), override=True)


def _parse_arb_tickers() -> List[str]:
    """
    ARB_TICKERS=ASTER,LIT or 'ASTER LIT' — comma/space separated, uppercased for Redis keys.
    If ARB_TICKERS is empty, falls back to ARB_TICKER (default ETH).
    """
    raw = os.getenv("ARB_TICKERS", "").strip()
    if raw:
        parts = [p.strip().upper() for p in raw.replace(",", " ").split() if p.strip()]
    else:
        single = os.getenv("ARB_TICKER", "ETH").strip()
        parts = [single.upper()] if single else []
    seen = set()
    out: List[str] = []
    for t in parts:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _parse_edge_version_int(raw) -> int:
    """
    EdgeX REST/WS 有时把 endVersion 打成字符串；int('123.0') 会 ValueError，需先 float 再 int。
    """
    if raw is None or raw == "":
        return 0
    try:
        return int(raw)
    except (ValueError, TypeError):
        try:
            return int(float(raw))
        except (ValueError, TypeError):
            return 0


def _edgex_contract_name_for_ticker(ticker: str) -> str:
    """
    Map config ticker to EdgeX metadata contractName (e.g. ETH / ETH-USDT -> ETHUSD).
    Public metadata uses contractName, not symbol; symbol is often absent on each contract.
    """
    t = (ticker or "").strip().upper()
    if not t:
        return ""
    if "-" in t:
        return f"{t.split('-')[0]}USD"
    return f"{t}USD"


class ArbRiskControlDaemon:
    """Standalone risk daemon tracking EdgeX data freshness."""
    
    def __init__(self, ticker: str, contract_id: str, tick_size: Decimal, min_order_size: Decimal):
        self.ticker = ticker
        self.contract_id = contract_id
        self.tick_size = tick_size
        self.min_order_size = min_order_size
        
        _load_arb_env()
        
        # Threshold configs (see arb_risk.env)
        self.stale_threshold_ms = int(os.getenv('STALE_THRESHOLD_MS', '50'))
        self.version_lag_threshold = int(os.getenv('VERSION_LAG_THRESHOLD', '15'))
        self.rest_poll_interval_sec = int(os.getenv('REST_POLL_INTERVAL_SEC', '2'))
        # get_order_book_depth 的 limit；EdgeX 不接受 1，会 INVALID_DEPTH_LEVEL（与全站 15 档一致）
        self.rest_depth_limit = int(os.getenv('REST_DEPTH_LIMIT', '15'))
        self.probe_interval_sec = int(os.getenv('PROBE_INTERVAL_SEC', '10'))
        self.risk_heartbeat_interval_sec = float(os.getenv('RISK_HEARTBEAT_INTERVAL_SEC', '1'))
        self.arrival_latency_threshold_ms = float(os.getenv('ARRIVAL_LATENCY_THRESHOLD_MS', '50'))
        self.ping_latency_threshold_ms = float(os.getenv('PING_LATENCY_THRESHOLD_MS', '50'))
        self.anomaly_log_debounce_sec = float(os.getenv('ANOMALY_LOG_DEBOUNCE_SEC', '1.0'))
        self.risk_redis_ttl_sec = int(os.getenv('RISK_REDIS_TTL_SEC', '5'))
        self.metrics_log_interval_sec = float(os.getenv('RISK_METRICS_LOG_INTERVAL_SEC', '60'))
        # 主动探测：买限价 = 当前买一价 * PROBE_PRICE_RATIO（默认 0.3，远离盘口减少误成交）
        self.probe_price_ratio = Decimal(os.getenv("PROBE_PRICE_RATIO", "0.3"))
        
        self.edgex_base_url = os.getenv('EDGEX_BASE_URL', 'https://pro.edgex.exchange')
        self.edgex_ws_url = os.getenv('EDGEX_WS_URL', 'wss://quote.edgex.exchange')
        self.edgex_account_id = os.getenv('EDGEX_ACCOUNT_ID')
        self.edgex_stark_private_key = os.getenv('EDGEX_STARK_PRIVATE_KEY')
        
        self.redis_client = RedisPriceClient()
        self.edgex_client = Client(
            base_url=self.edgex_base_url,
            account_id=int(self.edgex_account_id),
            stark_private_key=self.edgex_stark_private_key
        )
        
        self.edgex_ws_sdk = WebSocketManager(
            base_url=self.edgex_ws_url,
            account_id=int(self.edgex_account_id),
            stark_pri_key=self.edgex_stark_private_key
        )
        
        self.ws_wrapper = WebSocketManagerWrapper(None, logger)
        self.ws_wrapper.set_edgex_ws_manager(
            self.edgex_ws_sdk,
            self.contract_id,
            public_ws_url=self.edgex_ws_url,
        )
        self.ws_wrapper.set_callbacks(
            on_edgex_trade=self._handle_trade_message_json,
            on_edgex_order_update=self._handle_probe_order_update,
        )
        
        self.last_trade = None
        self.last_trade_time = 0.0
        self.rest_stale_flag = False
        self._last_rest_end_version: Optional[int] = None
        self._last_ws_end_version: Optional[int] = None
        self._last_version_lag: Optional[int] = None
        # 最近一次 REST 轮询为何未写入 rest_ver（便于对照 metrics 里全 None）
        self._rest_poll_status: str = "init"
        self._last_published_risk_code = "00000"
        
        # Active probing states
        self.pending_probe_order_id = None
        self.probe_expected_price = None
        self.probe_expected_size = None
        self.probe_start_time = 0.0
        self.probe_latency_ms = 0.0
        self._probe_latency_measured: bool = False  # 至少收到过一次私有单回报（ORDER_UPDATE）
        self._probe_ack_handled: bool = False  # 本轮探测是否已计过时（防重复推送）
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self.active_probe_trigger = False

        self.log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
        os.makedirs(self.log_dir, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        self.csv_filepath = os.path.join(self.log_dir, f"risk_anomalies_{self.ticker}_{date_str}.csv")
        self._init_csv()
        self._last_anomaly_logs = {}

    def _init_csv(self):
        file_exists = os.path.isfile(self.csv_filepath)
        try:
            with open(self.csv_filepath, 'a', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["Timestamp", "Datetime", "Ticker", "AnomalyName", "Details", "BestBid", "BestAsk"])
        except Exception as e:
            logger.error(f"Failed to initialize CSV: {e}")

    def _log_anomaly_to_csv(self, anomaly_name: str, details: str, best_bid, best_ask):
        date_str = datetime.now().strftime("%Y%m%d")
        filepath = os.path.join(self.log_dir, f"risk_anomalies_{self.ticker}_{date_str}.csv")
        if filepath != self.csv_filepath:
            self.csv_filepath = filepath
            self._init_csv()
            
        try:
            with open(self.csv_filepath, 'a', newline='') as f:
                writer = csv.writer(f)
                now = time.time()
                dt_str = datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                writer.writerow([
                    f"{now:.3f}",
                    dt_str,
                    self.ticker,
                    anomaly_name,
                    details,
                    str(best_bid) if best_bid is not None else "",
                    str(best_ask) if best_ask is not None else ""
                ])
        except Exception as e:
            logger.error(f"Failed to write anomaly to CSV: {e}")

    async def _handle_trade_message_json(self, msg: dict):
        """Parse raw websocket trade messages."""
        try:
            content = msg.get("content", {})
            data = content.get("data", [])
            if data and isinstance(data, list):
                latest_trade = data[-1]
                price_str = latest_trade.get("price")
                size_str = latest_trade.get("size")
                side = latest_trade.get("side", "").lower()
                
                if price_str and size_str and side:
                    self.last_trade = {
                        'price': Decimal(str(price_str)),
                        'side': side
                    }
                    self.last_trade_time = time.time()
                    
                    # Whenever a trade comes in, re-evaluate global risk
                    self.evaluate_and_publish_risk()
        except Exception as e:
            logger.debug(f"Error handling trade message: {e}")

    async def _check_rest_price(self):
        """Periodically poll REST API to detect massive WS lags."""
        while True:
            try:
                await asyncio.sleep(self.rest_poll_interval_sec)
                
                depth_params = GetOrderBookDepthParams(
                    contract_id=self.contract_id, limit=self.rest_depth_limit
                )
                ob = await self.edgex_client.quote.get_order_book_depth(depth_params)
                
                ob_data = ob.get('data', [])
                if not ob_data:
                    self._rest_poll_status = "no_rest_ob"
                    continue
                    
                rest_end_version = _parse_edge_version_int(ob_data[0].get("endVersion", 0))
                
                bbo_data = self.redis_client.get_latest_bbo('edgex', self.ticker)
                if not bbo_data:
                    self._rest_poll_status = "no_redis_bbo"
                    continue
                    
                # Redis Hash 字段名为 end_version（见 monitor/redis_client.store_latest_bbo）
                ws_end_version = bbo_data.get('end_version')
                if ws_end_version is None:
                    self._rest_poll_status = "redis_no_end_version"
                    continue
                
                lag = rest_end_version - ws_end_version
                self._last_rest_end_version = rest_end_version
                self._last_ws_end_version = ws_end_version
                self._last_version_lag = lag
                if lag > self.version_lag_threshold:
                    logger.warning(f"⚠️ REST Lag Detected! (REST={rest_end_version}, WS={ws_end_version})")
                    self.rest_stale_flag = True
                else:
                    self.rest_stale_flag = False

                self._rest_poll_status = "ok"
                # Re-evaluate global risk periodically
                self.evaluate_and_publish_risk()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._rest_poll_status = f"err:{type(e).__name__}"
                logger.warning(f"REST polling error [{self.ticker}]: {e}", exc_info=True)

    def _handle_probe_order_update(self, order: dict):
        """
        私有 ORDER_UPDATE：用 order_id 唯一匹配探测单。
        公共 depth 只有价位聚合量，无法区分「我下的 20」与「同档总量 +100」。
        """
        if not self.pending_probe_order_id or self._probe_ack_handled:
            return
        oid = order.get('id')
        if oid is None:
            return
        if str(oid) != str(self.pending_probe_order_id):
            return
        if str(order.get('contractId')) != str(self.contract_id):
            return
        status = (order.get('status') or '').upper()
        if status not in _PROBE_ORDER_DONE_STATUSES:
            return
        self._probe_ack_handled = True
        rt_latency_ms = (time.time() - self.probe_start_time) * 1000
        logger.debug(
            f"🌊 Probe ACK (private ORDER_UPDATE): RT Latency = {rt_latency_ms:.2f} ms (status={status})"
        )
        if self._main_loop is None:
            self._probe_ack_handled = False
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self._finalize_probe_measurement(rt_latency_ms), self._main_loop
            )
        except Exception as e:
            self._probe_ack_handled = False
            logger.warning(f"probe finalize schedule failed: {e}")

    async def _finalize_probe_measurement(self, latency_ms: float):
        """更新探测延迟；FOK 单多已终结，撤单尽力而为（失败可忽略）。"""
        self.probe_latency_ms = latency_ms
        self._probe_latency_measured = True
        if latency_ms > self.stale_threshold_ms:
            self.active_probe_trigger = True
        else:
            self.active_probe_trigger = False
        self.evaluate_and_publish_risk()
        cancel_id = self.pending_probe_order_id
        self.pending_probe_order_id = None
        self.probe_expected_price = None
        if cancel_id:
            try:
                await self.edgex_client.cancel_order(CancelOrderParams(order_id=cancel_id))
                logger.debug(f"Canceled probe order {cancel_id}")
            except Exception as e:
                logger.debug(f"Failed to cancel probe order: {e}")

    async def _place_order_check(self):
        """Active probing: Place a deep out-of-the-money order to measure round-trip delay."""
        while True:
            try:
                await asyncio.sleep(self.probe_interval_sec)
                
                if self.pending_probe_order_id:
                    try:
                        await self.edgex_client.cancel_order(CancelOrderParams(order_id=self.pending_probe_order_id))
                    except Exception:
                        pass
                    self.pending_probe_order_id = None
                    self._probe_ack_handled = False

                bbo_data = self.redis_client.get_latest_bbo('edgex', self.ticker)
                
                best_bid = bbo_data['best_bid'] if bbo_data and bbo_data.get('best_bid') else None
                best_ask = bbo_data.get('best_ask') if bbo_data else None
                if not best_bid:
                    continue

                probe_price = (
                    Decimal(str(best_bid)) * self.probe_price_ratio / self.tick_size
                ).quantize(Decimal("1")) * self.tick_size
                if probe_price <= 0:
                    continue
                if best_ask is not None and probe_price >= best_ask:
                    logger.debug("Active Probing: skip, probe would cross spread")
                    continue
                probe_size = self.min_order_size
                
                t1 = time.time()
                
                self._probe_ack_handled = False
                self.probe_expected_price = str(probe_price)
                self.probe_expected_size = str(probe_size)
                self.probe_start_time = t1
                
                # 探针限价 + FOK：全成或全撤，不在簿上残留；深价通常无法成交→立即撤销
                probe_params = CreateOrderParams(
                    contract_id=self.contract_id,
                    price=str(probe_price),
                    size=str(probe_size),
                    type=OrderType.LIMIT,
                    side=OrderSide.BUY,
                    client_order_id=f"probe_{int(t1*1000)}",
                    time_in_force=TimeInForce.FILL_OR_KILL,
                )
                res = await self.edgex_client.create_order(probe_params)
                
                if res and 'data' in res:
                    order_id = res['data'].get('orderId')
                    self.pending_probe_order_id = order_id
                else:
                    self.pending_probe_order_id = None
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Active Probing Error: {e}")

    async def _check_trades_vs_orderbook(self):
        """Ensure safe risk is continuously published even absent events."""
        while True:
            try:
                self.evaluate_and_publish_risk()
                await asyncio.sleep(self.risk_heartbeat_interval_sec)
            except asyncio.CancelledError:
                break

    def evaluate_and_publish_risk(self):
        """Evaluates freshness rules and publishes result to Redis."""
        bbo_data = self.redis_client.get_latest_bbo('edgex', self.ticker)
        
        best_bid = bbo_data['best_bid'] if bbo_data and bbo_data.get('best_bid') else None
        best_ask = bbo_data['best_ask'] if bbo_data and bbo_data.get('best_ask') else None
        
        is_stale = False
        anomalies_found = []
        risk_code_list = ['0'] * 5
        
        # 1. Check REST Lag
        if self.rest_stale_flag:
            logger.debug("Risk Evaluated: STALE (REST polling flag)")
            is_stale = True
            risk_code_list[0] = '1'
            anomalies_found.append(("REST轮询延迟", "检测到REST接口数据落后"))
            
        # 1.5 Check Active Probe Latency
        if self.active_probe_trigger:
            logger.debug(f"Risk Evaluated: STALE (Active Probe Latency {self.probe_latency_ms:.2f}ms > threshold)")
            is_stale = True
            risk_code_list[1] = '1'
            anomalies_found.append(("主动下单探测延迟", f"探测延迟: {self.probe_latency_ms:.2f}ms"))
            
        # 1.6 Check Arrival Timestamp (Local Heuristics)
        # if self.ws_wrapper.edgex_public_last_message_time > 0:
        #     time_since_msg = (time.time() - self.ws_wrapper.edgex_public_last_message_time) * 1000
        #     if time_since_msg > self.arrival_latency_threshold_ms:
        #         logger.debug(
        #             f"Risk Evaluated: STALE (Arrival Latency {time_since_msg:.2f}ms > "
        #             f"{self.arrival_latency_threshold_ms}ms)"
        #         )
        #         is_stale = True
        #         risk_code_list[2] = '1'
        #         anomalies_found.append(("消息到达延迟", f"到达延迟: {time_since_msg:.2f}ms"))

        # 1.7 Check Ping/Pong RTT (Transport Level)
        if self.ws_wrapper.edgex_public_latency > self.ping_latency_threshold_ms:
            logger.debug(
                f"Risk Evaluated: STALE (Ping/Pong RTT {self.ws_wrapper.edgex_public_latency:.2f}ms > "
                f"{self.ping_latency_threshold_ms}ms)"
            )
            is_stale = True
            risk_code_list[3] = '1'
            anomalies_found.append(("网络pingpong延迟", f"传输层RTT: {self.ws_wrapper.edgex_public_latency:.2f}ms"))
            
        # 2. Check Trade Freshness
        if self.last_trade: # modified to evaluate regardless of other stales, so risk_code is populated
            time_since_trade = (time.time() - self.last_trade_time) * 1000
            if time_since_trade <= self.stale_threshold_ms:
                trade_price = self.last_trade['price']
                trade_side = self.last_trade['side']
                
                if trade_side == 'sell' and best_bid is not None and trade_price < best_bid:
                    logger.debug(f"Risk Evaluated: STALE (Trade sell price {trade_price} < best_bid {best_bid})")
                    is_stale = True
                    risk_code_list[4] = '1'
                    anomalies_found.append(("trades ws vs order ws 异常_卖出", f"卖出价 {trade_price} 小于 买一价 {best_bid}"))
                elif trade_side == 'buy' and best_ask is not None and trade_price > best_ask:
                    logger.debug(f"Risk Evaluated: STALE (Trade buy price {trade_price} > best_ask {best_ask})")
                    is_stale = True
                    risk_code_list[4] = '1'
                    anomalies_found.append(("trades ws vs order ws异常_买入", f"买入价 {trade_price} 大于 卖一价 {best_ask}"))

        # Log anomalies to CSV with debouncing (max 1 log per type per second)
        now = time.time()
        for anomaly_name, details in anomalies_found:
            last_time = self._last_anomaly_logs.get(anomaly_name, 0)
            if now - last_time > self.anomaly_log_debounce_sec:
                self._log_anomaly_to_csv(anomaly_name, details, best_bid, best_ask)
                self._last_anomaly_logs[anomaly_name] = now

        risk_code = "".join(risk_code_list)
        self._last_published_risk_code = risk_code
        self.redis_client.set_risk_status('edgex', self.ticker, risk_code, ttl=self.risk_redis_ttl_sec)

    def _log_metrics_snapshot(self) -> None:
        """每分钟（可配置）打印当前各风控指标快照，便于对照 Redis 风险码与阈值。"""
        now = time.time()
        bbo_data = self.redis_client.get_latest_bbo('edgex', self.ticker)
        best_bid = bbo_data.get('best_bid') if bbo_data else None
        best_ask = bbo_data.get('best_ask') if bbo_data else None
        redis_end_ver = bbo_data.get('end_version') if bbo_data else None

        public_idle_ms = None
        if self.ws_wrapper.edgex_public_last_message_time > 0:
            public_idle_ms = (now - self.ws_wrapper.edgex_public_last_message_time) * 1000

        trade_age_ms = None
        trade_hint = ""
        if self.last_trade:
            trade_age_ms = (now - self.last_trade_time) * 1000
            trade_hint = f"{self.last_trade.get('side', '')}@{self.last_trade.get('price', '')}"

        rc = self._last_published_risk_code
        probe_ms_s = f"{self.probe_latency_ms:.2f}" if self._probe_latency_measured else "na"
        # probe_last_ms=数字：最近一次私有 ORDER_UPDATE(OPEN/PENDING/NEW) 相对下单时刻的耗时；na=本轮还未成功记过一次
        probe_ms_note = (
            "na=本轮尚未收到私有单ACK"
            if probe_ms_s == "na"
            else "ms=下单→私有ORDER_UPDATE首条可交易状态"
        )
        logger.info(
            "[risk_metrics] ticker=%s risk_code=%s [0=REST 1=probe 2=rsv 3=ping 4=trade_ob]\n"
            "| rest_poll=%s REST_stale=%s rest_ver=%s ws_bbo_ver=%s ver_lag=%s ver_lag_thr=%s\n"
            "| probe_alert=%s probe_last_ms=%s (%s) stale_thr_ms=%s\n"
            "| public_ws_idle_ms=%s arrival_thr_ms=%s (idle=距上一帧公共WS)\n"
            "| ping_ms=%s ping_thr_ms=%s\n"
            "| trade_age_ms=%s trade=%s trade_check_win_ms=%s\n"
            "| bbo=%s/%s redis_end_ver=%s",
            self.ticker,
            rc,
            self._rest_poll_status,
            self.rest_stale_flag,
            self._last_rest_end_version,
            self._last_ws_end_version,
            self._last_version_lag,
            self.version_lag_threshold,
            self.active_probe_trigger,
            probe_ms_s,
            probe_ms_note,
            self.stale_threshold_ms,
            f"{public_idle_ms:.2f}" if public_idle_ms is not None else "na",
            self.arrival_latency_threshold_ms,
            f"{self.ws_wrapper.edgex_public_latency:.2f}",
            self.ping_latency_threshold_ms,
            f"{trade_age_ms:.2f}" if trade_age_ms is not None else "na",
            trade_hint or "na",
            self.stale_threshold_ms,
            best_bid,
            best_ask,
            redis_end_ver,
        )

    async def _periodic_metrics_log(self) -> None:
        if self.metrics_log_interval_sec <= 0:
            return
        while True:
            try:
                await asyncio.sleep(self.metrics_log_interval_sec)
                self._log_metrics_snapshot()
            except asyncio.CancelledError:
                break

    async def run(self):
        """Starts all monitoring and heartbeating tasks in parallel."""
        logger.info(f"Starting Arb Risk Daemon for {self.ticker}")
        
        self._main_loop = asyncio.get_running_loop()
        self.ws_wrapper.start_edgex_websocket()
        
        tasks = [
            self._check_rest_price(),
            self._check_trades_vs_orderbook(),
            self._place_order_check(),
        ]
        if self.metrics_log_interval_sec > 0:
            tasks.append(self._periodic_metrics_log())
        await asyncio.gather(*tasks)


def _resolve_contracts_from_metadata(
    contract_list: list, tickers: List[str]
) -> Tuple[List[Tuple[str, str, Decimal, Decimal]], List[Tuple[str, str]]]:
    """Returns (resolved configs, missing as (ticker, expected_contract_name))."""
    by_name = {c.get("contractName"): c for c in contract_list if c.get("contractName")}
    resolved: List[Tuple[str, str, Decimal, Decimal]] = []
    missing: List[Tuple[str, str]] = []
    for ticker in tickers:
        expected = _edgex_contract_name_for_ticker(ticker)
        c = by_name.get(expected)
        ts = c.get("tickSize") if c else None
        mos = c.get("minOrderSize") if c else None
        cid = c.get("contractId") if c else None
        if cid and ts is not None and mos is not None:
            resolved.append((ticker, str(cid), Decimal(str(ts)), Decimal(str(mos))))
        else:
            missing.append((ticker, expected))
    return resolved, missing


async def main():
    _load_arb_env()
    tickers = _parse_arb_tickers()
    if not tickers:
        logger.error("No tickers: set ARB_TICKERS (list) or ARB_TICKER in arb.env")
        return

    base_url = os.getenv('EDGEX_BASE_URL', 'https://pro.edgex.exchange')
    account_id = os.getenv('EDGEX_ACCOUNT_ID')
    stark_key = os.getenv('EDGEX_STARK_PRIVATE_KEY')
    
    if not all([account_id, stark_key]):
        logger.error("Missing EdgeX credentials in arb.env")
        return

    logger.info(f"Arb risk tickers from env: {tickers}")
    client = Client(base_url=base_url, account_id=int(account_id), stark_private_key=stark_key)
    try:
        metadata = await client.get_metadata()
    finally:
        await client.close()

    data = metadata.get('data', {})
    contract_list = data.get('contractList', [])
    resolved, missing = _resolve_contracts_from_metadata(contract_list, tickers)
    if missing:
        for t, exp in missing:
            logger.error(f"No EdgeX contract for ticker={t} (contractName={exp})")
        return
    if not resolved:
        return

    daemons = [
        ArbRiskControlDaemon(ticker, cid, ts, mos)
        for ticker, cid, ts, mos in resolved
    ]
    await asyncio.gather(*(d.run() for d in daemons))

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Daemon gracefully shutdown")
