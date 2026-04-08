import argparse
import csv
import logging
import os
import sys
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
import lighter

from redis_client import RedisPriceClient

# 添加项目根目录到sys.path以导入strategy模块
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from strategy.db_logger import ArbInfoDB


def load_simple_tickers(tickers_file: str = "simple_tickers.txt") -> List[dict]:
    """
    读取简单套利配置文件。

    格式:
    SYMBOL 开仓数量 最大开仓数量 开仓价差 平仓价差
    例如:
    BTC 0.001 0.01 0.003 0.002
    """
    # 如果不是绝对路径，尝试从多个位置查找
    if not os.path.isabs(tickers_file):
        candidate_paths = [
            tickers_file,
            os.path.join(PROJECT_ROOT, tickers_file),
            os.path.join(os.path.dirname(__file__), tickers_file),
        ]
        for path in candidate_paths:
            if os.path.exists(path):
                tickers_file = path
                break

    if not os.path.exists(tickers_file):
        raise FileNotFoundError(f"未找到simple_tickers文件: {tickers_file}")

    configs = []
    with open(tickers_file, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 5:
                raise ValueError(
                    f"simple_tickers格式错误（第{line_num}行）: "
                    f"需要5列(SYMBOL size max_position open_spread close_spread)"
                )

            symbol = parts[0].upper()
            open_size = float(parts[1])
            max_position = float(parts[2])
            open_spread = float(parts[3])
            close_spread = float(parts[4])

            configs.append(
                {
                    "symbol": symbol,
                    "open_size": open_size,
                    "max_position": max_position,
                    "open_spread": open_spread,
                    "close_spread": close_spread,
                }
            )

    if not configs:
        raise ValueError(f"simple_tickers为空或没有有效配置: {tickers_file}")

    return configs


def build_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger("simple_arb")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


class SimpleArb:
    # 全局共享的开仓时间和锁，用于所有线程之间的5秒冷却
    _global_last_open_time: float = 0
    _global_lock = threading.Lock()

    def __init__(
        self,
        symbol_config: dict,
        update_interval: float = 1.0,
        cooldown_seconds: float = 5.0,
        csv_file: str = "logs/simple_arb_log.csv",
        logger: Optional[logging.Logger] = None,
    ):
        self.symbol = symbol_config["symbol"]
        self.symbol_config = symbol_config
        self.update_interval = update_interval
        self.cooldown_seconds = cooldown_seconds
        self.logger = logger or logging.getLogger(f"simple_arb_{self.symbol}")
        self.csv_file = csv_file

        self.redis_client = RedisPriceClient(host='172.18.58.232')

        self.position = {
            "size": 0.0,
            "entry_time": None,
        }
        self.cooldown_until: Optional[datetime] = None

        self._ensure_csv_header()
        
        # 初始化数据库记录器
        self.arb_info_db = ArbInfoDB()

        self.use_quantile = os.getenv("SIMPLE_USE_QUANTILE", "0") == "1"
        self.quantile = float(os.getenv("SIMPLE_QUANTILE", "0.95"))
        self.history_limit = int(os.getenv("SIMPLE_HISTORY_LIMIT", "1000"))
        self.quantile_refresh_seconds = int(
            os.getenv("SIMPLE_QUANTILE_REFRESH_SECONDS", "300")
        )
        self.next_quantile_update = None

        if self.use_quantile:
            self._apply_quantile_thresholds(
                quantile=self.quantile, history_limit=self.history_limit
            )
            self.next_quantile_update = datetime.now() + timedelta(
                seconds=self.quantile_refresh_seconds
            )

    def _ensure_csv_header(self):
        directory = os.path.dirname(self.csv_file)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        if os.path.exists(self.csv_file):
            return
        with open(self.csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "timestamp",
                    "action",
                    "symbol",
                    "direction",
                    "spread",
                    "entry_spread",
                    "entry_gap",
                    "entry_latency",
                    "edgex_price",
                    "lighter_price",
                    "entry_edgex_price",
                    "entry_lighter_price",
                    "client_order_id",
                ]
            )

    def fetch_latest_data(self, symbol: str) -> Tuple[Optional[dict], Optional[dict]]:
        edgex_redis_data = self.redis_client.get_latest_bbo("edgex", symbol)
        lighter_redis_data = self.redis_client.get_latest_bbo("lighter", symbol)

        edgex_data = None
        lighter_data = None

        if edgex_redis_data:
            edgex_data = {
                "symbol": symbol,
                "best_bid": float(edgex_redis_data["best_bid"])
                if edgex_redis_data.get("best_bid")
                else 0.0,
                "best_ask": float(edgex_redis_data["best_ask"])
                if edgex_redis_data.get("best_ask")
                else 0.0,
                "timestamp": edgex_redis_data.get("timestamp"),
                "gap": edgex_redis_data.get("gap"),
            }

        if lighter_redis_data:
            lighter_data = {
                "symbol": symbol,
                "best_bid": float(lighter_redis_data["best_bid"])
                if lighter_redis_data.get("best_bid")
                else 0.0,
                "best_ask": float(lighter_redis_data["best_ask"])
                if lighter_redis_data.get("best_ask")
                else 0.0,
                "timestamp": lighter_redis_data.get("timestamp"),
                "exchange_ts_ms": lighter_redis_data.get("exchange_ts_ms"),
            }

        return edgex_data, lighter_data

    @staticmethod
    def calculate_spreads(edgex_data: dict, lighter_data: dict) -> Tuple[float, float]:
        edgex_ask = float(edgex_data["best_ask"])
        edgex_bid = float(edgex_data["best_bid"])
        lighter_ask = float(lighter_data["best_ask"])
        lighter_bid = float(lighter_data["best_bid"])

        if edgex_ask <= 0 or edgex_bid <= 0 or lighter_ask <= 0 or lighter_bid <= 0:
            return 0.0, 0.0

        spread1 = (lighter_bid - edgex_ask) / edgex_ask
        spread2 = (edgex_bid - lighter_ask) / lighter_ask
        return spread1, spread2

    def _generate_unique_order_id(self) -> str:
        """生成唯一的client_order_id，存入Redis Set防止重复"""
        base_id = int(time.time() * 1000)
        order_id_key = "simple_arb:client_order_ids"
        
        while True:
            order_id = str(base_id)
            # SADD返回1表示新增成功（不重复），0表示已存在
            added = self.redis_client.client.sadd(order_id_key, order_id)
            if added:
                # 设置整个set的过期时间为60秒
                self.redis_client.client.expire(order_id_key, 60)
                return order_id
            # 如果重复，+1后重试
            base_id += 1

    def _is_data_quality_ok(
        self, edgex_data: dict, lighter_data: dict
    ) -> Tuple[bool, str]:
        """
        数据质量检查：EdgeX gap、Lighter 延迟。
        返回 (是否通过, 原因描述)
        """
        max_edgex_gap = int(os.getenv("EDGEX_MAX_GAP", "2"))
        edgex_gap = edgex_data.get("gap")
        if edgex_gap is not None:
            if edgex_gap < 0:
                return False, f"EdgeX gap异常(负值): {edgex_gap}"
            if edgex_gap > max_edgex_gap:
                return False, f"EdgeX gap={edgex_gap} > {max_edgex_gap}"
        max_lighter_latency_ms = int(os.getenv("LIGHTER_MAX_LATENCY_MS", "50"))
        lighter_ts = lighter_data.get("exchange_ts_ms")
        if lighter_ts is not None:
            now_ms = int(time.time() * 1000)
            latency_ms = now_ms - int(lighter_ts)
            if latency_ms > max_lighter_latency_ms:
                return False, f"Lighter延迟={latency_ms}ms > {max_lighter_latency_ms}ms"
        return True, ""

    def publish_signal(self, symbol: str, edgex_direction: str, lighter_direction: str) -> str:
        client_order_id = self._generate_unique_order_id()
        
        success = self.redis_client.publish_trading_signal(
            symbol, edgex_direction, lighter_direction, client_order_id
        )
        if success:
            self.logger.info(
                "📤 推送交易信号: %s+%s+edgex+%s+lighter+%s",
                symbol,
                edgex_direction,
                lighter_direction,
                client_order_id,
            )
        else:
            self.logger.warning(
                "推送交易信号失败: %s %s/%s",
                symbol,
                edgex_direction,
                lighter_direction,
            )
        return client_order_id

    def check_open_signal(
        self, spread1: float, spread2: float
    ) -> Optional[str]:
        config = self.symbol_config
        open_spread1 = config.get("open_spread1", config["open_spread"])
        open_spread2 = config.get("open_spread2", config["open_spread"])

        trigger1 = spread1 >= open_spread1
        trigger2 = spread2 >= open_spread2

        if not trigger1 and not trigger2:
            return None

        if trigger1 and trigger2:
            return "long_spread1" if spread1 >= spread2 else "long_spread2"

        return "long_spread1" if trigger1 else "long_spread2"

    def open_position(
        self,
        symbol: str,
        direction: str,
        spread: float,
        edgex_data: dict,
        lighter_data: dict,
        size: float,
    ):
        if size <= 0:
            return
        if direction == "long_spread1":
            client_order_id = self.publish_signal(symbol, "buy", "sell")
        else:
            client_order_id = self.publish_signal(symbol, "sell", "buy")

        if direction == "long_spread1":
            edgex_price = float(edgex_data["best_ask"])
            lighter_price = float(lighter_data["best_bid"])
        else:
            edgex_price = float(edgex_data["best_bid"])
            lighter_price = float(lighter_data["best_ask"])

        current_size = self.position.get("size", 0.0) or 0.0
        delta = size if direction == "long_spread1" else -size
        new_size = current_size + delta
        self.position["size"] = new_size
        self.position["entry_time"] = datetime.now().timestamp() if new_size != 0 else None

        self.logger.info(
            "%s 开仓: %s | edgex_price=%.6f|lighter_price=%.6f | spread=%.6f",
            symbol,
            direction,
            edgex_price,
            lighter_price,
            spread,
        )
        entry_gap = edgex_data.get("gap")
        lighter_ts = lighter_data.get("exchange_ts_ms")
        entry_latency = (int(time.time() * 1000) - int(lighter_ts)) if lighter_ts is not None else None
        self._write_csv(
            action="open",
            symbol=symbol,
            direction=direction,
            spread=spread,
            entry_spread=spread,
            entry_gap=entry_gap,
            entry_latency=entry_latency,
            edgex_price=edgex_price,
            lighter_price=lighter_price,
            entry_edgex_price=edgex_price,
            entry_lighter_price=lighter_price,
            client_order_id=client_order_id,
        )
        
        # 记录到数据库 (discovery类型)
        try:
            # 根据direction确定edgex和lighter的方向
            if direction == "long_spread1":
                edgex_direction = "buy"
                lighter_direction = "sell"
            else:
                edgex_direction = "sell"
                lighter_direction = "buy"
            
            self.arb_info_db.log_arb_info(
                symbol=symbol,
                info_type=ArbInfoDB.INFO_TYPE_DISCOVERY,
                client_order_id=client_order_id,
                edgex_price=edgex_price,
                lighter_price=lighter_price,
                spread_ratio=spread,
                edgex_direction=edgex_direction,
                lighter_direction=lighter_direction,
            )
        except Exception as e:
            self.logger.warning(f"写入数据库失败: {e}")

   

    def update_symbol(self):
        symbol = self.symbol
        edgex_data, lighter_data = self.fetch_latest_data(symbol)
        if not edgex_data or not lighter_data:
            return

        spread1, spread2 = self.calculate_spreads(edgex_data, lighter_data)
        config = self.symbol_config
        open_size = float(config["open_size"])
        max_position = float(config["max_position"])
        current_size = self.position.get("size", 0.0) or 0.0
        min_spread_sum = float(os.getenv("SIMPLE_MIN_SPREAD_SUM", "0.00052"))
 
        # spread 确认是否开仓
        spread_type = self.check_open_signal(spread1, spread2)
        if not spread_type:
            return

        # min_spread_sum 确认是否开仓
        if min_spread_sum > 0:          
            open_threshold =  config.get("open_spread1", config["open_spread"])
            close_threshold = config.get("close_spread1", config["close_spread"])
            if (open_threshold + close_threshold) < min_spread_sum:
                return
        
        # 100ms后二次确认
        # time.sleep(0.1)
        edgex_data_2, lighter_data_2 = self.fetch_latest_data(symbol)
        if not edgex_data_2 or not lighter_data_2:
            return
        spread1_2, spread2_2 = self.calculate_spreads(edgex_data_2, lighter_data_2)
        spread_type_2 = self.check_open_signal(spread1_2, spread2_2)
        if spread_type_2 != spread_type:
            return
        if min_spread_sum > 0:
            if (open_threshold + close_threshold) < min_spread_sum:
                return
        
        if spread1_2 < spread1 and spread2_2 < spread2:
            self.logger.debug(f'{symbol}二次确认差价在下降, 不进行开仓')
            return
        # 二次确认通过，使用最新数据开仓
        edgex_data, lighter_data = edgex_data_2, lighter_data_2
        spread1, spread2 = spread1_2, spread2_2

        delta = open_size if spread_type == "long_spread1" else -open_size
        new_size = current_size + delta
        if new_size > max_position or new_size < -max_position:
            self.logger.debug(f'{symbol}超出最大仓位{current_size} {delta } {max_position}')
            return
        current_timestamp = datetime.now().timestamp()
        # 使用全局锁检查和更新开仓时间，确保所有线程共享5秒冷却
        with SimpleArb._global_lock:
            if SimpleArb._global_last_open_time != 0:
                if current_timestamp - SimpleArb._global_last_open_time < 5:
                    self.logger.debug(f'{symbol}全局5秒内不能重复开仓')
                    return
            SimpleArb._global_last_open_time = current_timestamp
        ok, reason = self._is_data_quality_ok(edgex_data, lighter_data)
        if not ok:
            self.logger.warning(f"{symbol} 数据质量不达标，跳过开仓: {reason}")
            spread = spread1 if spread_type == "long_spread1" else spread2
            edgex_price = float(edgex_data["best_ask"]) if spread_type == "long_spread1" else float(edgex_data["best_bid"])
            lighter_price = float(lighter_data["best_bid"]) if spread_type == "long_spread1" else float(lighter_data["best_ask"])
            entry_gap = edgex_data.get("gap")
            lighter_ts = lighter_data.get("exchange_ts_ms")
            entry_latency = (int(time.time() * 1000) - int(lighter_ts)) if lighter_ts is not None else None
            self._write_csv(
                action=f"discover_low_quantity:{reason}",
                symbol=symbol,
                direction=spread_type,
                spread=spread,
                entry_spread=spread,
                entry_gap=entry_gap,
                entry_latency=entry_latency,
                edgex_price=edgex_price,
                lighter_price=lighter_price,
                entry_edgex_price=edgex_price,
                entry_lighter_price=lighter_price,
                client_order_id="-",
            )
            return
        spread = spread1 if spread_type == "long_spread1" else spread2
        self.open_position(symbol, spread_type, spread, edgex_data, lighter_data, open_size)

    def run_once(self):
        self.update_symbol()

    def run_continuous(self, duration_hours: Optional[float] = None):
        self.logger.info("开始运行简单价差套利程序")
        self.logger.info("监控标的: %s", self.symbol)

        start_time = time.time()
        try:
            while True:
                if self.use_quantile and self.next_quantile_update:
                    if datetime.now() >= self.next_quantile_update:
                        self._apply_quantile_thresholds(
                            quantile=self.quantile, history_limit=self.history_limit
                        )
                        self.next_quantile_update = datetime.now() + timedelta(
                            seconds=self.quantile_refresh_seconds
                        )

                loop_start = time.time()
                self.run_once()

               
                elapsed = time.time() - loop_start
                time.sleep(0.01)
        except KeyboardInterrupt:
            self.logger.info("接收到中断信号，停止运行")
        finally:
            self.redis_client.close()
            if hasattr(self, 'arb_info_db') and self.arb_info_db:
                try:
                    self.arb_info_db.close()
                except Exception as e:
                    self.logger.warning(f"关闭数据库连接失败: {e}")

    def _apply_quantile_thresholds(self, quantile: float, history_limit: int):
        symbol = self.symbol
        config = self.symbol_config
        self.logger.info(
            "%s 使用分位数法计算阈值: quantile=%.3f, history_limit=%d",
            symbol,
            quantile,
            history_limit,
        )
        spread1_list = []
        spread2_list = []
        total_pairs = 0
        zero_or_invalid_spread = 0
        non_positive_spread1 = 0
        non_positive_spread2 = 0
        edgex_history = self.redis_client.get_history_bbo(
            "edgex", symbol, history_limit
        )
        lighter_history = self.redis_client.get_history_bbo(
            "lighter", symbol, history_limit
        )

        if not edgex_history or not lighter_history:
            self.logger.info("历史数据不足，跳过分位数阈值: %s", symbol)
            return

        min_len = min(len(edgex_history), len(lighter_history))
        for i in range(min_len):
            edgex_item = edgex_history[i]
            lighter_item = lighter_history[i]
            edgex_data = {
                "best_bid": float(edgex_item.get("best_bid") or 0.0),
                "best_ask": float(edgex_item.get("best_ask") or 0.0),
            }
            lighter_data = {
                "best_bid": float(lighter_item.get("best_bid") or 0.0),
                "best_ask": float(lighter_item.get("best_ask") or 0.0),
            }
            spread1, spread2 = self.calculate_spreads(edgex_data, lighter_data)
            total_pairs += 1
            if spread1 == 0.0 and spread2 == 0.0:
                zero_or_invalid_spread += 1
                continue
            if spread1 <= 0:
                non_positive_spread1 += 1
            else:
                spread1_list.append(spread1)
            if spread2 <= 0:
                non_positive_spread2 += 1
            else:
                spread2_list.append(spread2)

        if not spread1_list or not spread2_list:
            self.logger.info(
                "有效价差不足，跳过分位数阈值: %s | total=%d invalid/zero=%d "
                "non_positive_spread1=%d non_positive_spread2=%d",
                symbol,
                total_pairs,
                zero_or_invalid_spread,
                non_positive_spread1,
                non_positive_spread2,
            )
            return

        p95_spread1 = self._quantile(spread1_list, quantile)
        p95_spread2 = self._quantile(spread2_list, quantile)

        config["open_spread1"] = p95_spread1
        config["open_spread2"] = p95_spread2
        config["close_spread1"] = p95_spread2
        config["close_spread2"] = p95_spread1

        self.logger.info(
            "%s 分位数阈值: open1=%.6f open2=%.6f ",
            symbol,
            p95_spread1,
            p95_spread2
        )

    @staticmethod
    def _quantile(values: List[float], q: float) -> float:
        if not values:
            return 0.0
        if q <= 0:
            return min(values)
        if q >= 1:
            return max(values)
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        idx = (n - 1) * q
        lower = int(idx)
        upper = min(lower + 1, n - 1)
        if lower == upper:
            return sorted_vals[lower]
        weight = idx - lower
        return sorted_vals[lower] * (1 - weight) + sorted_vals[upper] * weight

    def _write_csv(
        self,
        action: str,
        symbol: str,
        direction: str,
        spread: float,
        entry_spread: Optional[float],
        entry_gap: Optional[int],
        entry_latency: Optional[int],
        edgex_price: float,
        lighter_price: float,
        entry_edgex_price: Optional[float],
        entry_lighter_price: Optional[float],
        client_order_id: str,
    ):
        record = [
            datetime.now().isoformat(),
            action,
            symbol,
            direction,
            spread,
            entry_spread,
            entry_gap,
            entry_latency,
            edgex_price,
            lighter_price,
            entry_edgex_price,
            entry_lighter_price,
            client_order_id,
        ]
        try:
            with open(self.csv_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(record)
        except Exception as exc:
            self.logger.warning("写入CSV失败: %s", exc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple spread arbitrage")
    parser.add_argument(
        "--tickers-file",
        type=str,
        default=os.getenv("SIMPLE_TICKERS_FILE", "simple_tickers.txt"),
        help="simple_tickers文件路径 (默认: simple_tickers.txt)",
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=None,
        help="运行时长（小时），默认持续运行",
    )
    return parser.parse_args()


def main():
    simple_env_path = os.path.join(PROJECT_ROOT, "simple.env")
    if os.path.exists(simple_env_path):
        load_dotenv(simple_env_path, override=False)
    else:
        load_dotenv(override=False)

    args = parse_args()
    log_file = os.getenv("SIMPLE_LOG_FILE", "simple_arb.log")
    logger = build_logger(log_file)

    update_interval = float(os.getenv("SIMPLE_UPDATE_INTERVAL", "1.0"))
    cooldown_seconds = float(os.getenv("SIMPLE_COOLDOWN_SECONDS", "5.0"))
    csv_file = os.getenv("SIMPLE_CSV_FILE", "logs/simple_arb_log.csv")

    configs = load_simple_tickers(args.tickers_file)
    logger.info("加载simple_tickers: %s", args.tickers_file)
    logger.info("启动 %d 个标的的并行线程", len(configs))

    threads = []
    for config in configs:
        arb = SimpleArb(
            symbol_config=config,
            update_interval=update_interval,
            cooldown_seconds=cooldown_seconds,
            csv_file=csv_file,
            logger=logger,
        )
        thread = threading.Thread(
            target=arb.run_continuous,
            kwargs={"duration_hours": args.duration_hours},
            name=f"SimpleArb-{config['symbol']}",
            daemon=True,
        )
        threads.append(thread)
        thread.start()
        logger.info("已启动线程: %s", config['symbol'])

    try:
        # 主线程等待所有子线程
        while True:
            alive_threads = [t for t in threads if t.is_alive()]
            if not alive_threads:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("接收到中断信号，停止运行")


if __name__ == "__main__":
    main()
