#!/usr/bin/env python3
"""
独立的 WebSocket managers 用于记录多个标的的数据
每个交易所使用一个 WebSocket 连接订阅所有标的
"""

import asyncio
import json
import logging
import time
import websockets
from decimal import Decimal
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Callable
import pytz

from edgex_sdk import WebSocketManager

# 定义 UTC+8 时区
TZ_UTC8 = pytz.timezone('Asia/Shanghai')

# 导入Redis客户端（可选）
try:
    from redis_client import RedisPriceClient
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    RedisPriceClient = None


def format_timestamp_to_seconds(dt: datetime) -> str:
    """格式化时间戳到秒级别（去掉毫秒和时区）"""
    return dt.replace(microsecond=0, tzinfo=None).isoformat()


class EdgeXMultiTickerWebSocketManager:
    """EdgeX WebSocket manager 支持多标的订阅"""
    
    def __init__(self, ws_manager: WebSocketManager, logger: logging.Logger = None, redis_client: Optional[Any] = None):
        """
        初始化 EdgeX 多标的 WebSocket manager
        
        Args:
            ws_manager: EdgeX SDK 的 WebSocketManager 实例
            logger: 日志记录器
            redis_client: Redis客户端实例（可选），用于实时存储价格数据
        """
        self.ws_manager = ws_manager
        self.logger = logger or logging.getLogger(__name__)
        
        # Redis客户端（可选）
        self.redis_client = redis_client
        if self.redis_client is None and REDIS_AVAILABLE:
            try:
                self.redis_client = RedisPriceClient()
                self.logger.info("EdgeX WebSocket Manager: Redis客户端初始化成功")
            except Exception as e:
                self.logger.warning(f"EdgeX WebSocket Manager: Redis客户端初始化失败: {e}")
                self.redis_client = None
        
        # 存储每个 ticker 的订单簿数据
        # {ticker: {order_book, best_bid, best_bid_size, best_ask, best_ask_size, price_timestamp, ready, last_message_time, last_end_version}}
        self.ticker_data: Dict[str, Dict[str, Any]] = {}
        
        # 合约ID到ticker的映射 {contract_id: ticker}
        self.contract_to_ticker: Dict[str, str] = {}
        
        # 配置
        self.websocket_stale_threshold = 10  # WebSocket数据超过30秒未更新视为过期（秒）
        
        # 重连相关状态
        self.running = False
        self._reconnect_task = None
        self._tickers_list = []  # 保存订阅的tickers列表，用于重连时重新订阅
        self._contract_ids_list = []  # 保存订阅的contract_ids列表
        self._manual_reconnect_task = None
        self._last_manual_reconnect_ts = 0.0
        self._manual_reconnect_min_interval = 10
        
    def register_ticker(self, ticker: str, contract_id: str):
        """注册一个标的"""
        if ticker not in self.ticker_data:
            self.ticker_data[ticker] = {
                'order_book': {"bids": [], "asks": []},
                'best_bid': None,
                'best_bid_size': None,
                'best_ask': None,
                'best_ask_size': None,
                'price_timestamp': None,
                'ready': False,
                'last_message_time': None,
                'last_end_version': None
            }
        self.contract_to_ticker[contract_id] = ticker
        
    def get_ticker_data(
        self, ticker: str
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[str], Optional[float]]:
        """获取指定ticker的BBO数据。最后一项为交易所消息时间戳毫秒数（仅部分源可用，否则为 None）。"""
        if ticker not in self.ticker_data:
            return None, None, None, None, None, None

        data = self.ticker_data[ticker]

        # 检查是否就绪
        if not data["ready"]:
            return None, None, None, None, None, None

        # 检查数据是否过期
        if data["last_message_time"]:
            time_since_last_msg = (datetime.now(TZ_UTC8) - data["last_message_time"]).total_seconds()
            if time_since_last_msg > self.websocket_stale_threshold:
                self.logger.warning(
                    f"{ticker} EdgeX WebSocket数据已过期: 距离上次消息 {time_since_last_msg:.1f} 秒"
                )
                self.request_reconnect(
                    reason=f"{ticker} EdgeX WebSocket数据已过期",
                    force=False,
                )
                return None, None, None, None, None, None

        return (
            data["best_bid"],
            data["best_bid_size"],
            data["best_ask"],
            data["best_ask_size"],
            data["price_timestamp"],
            None,
        )

    def request_reconnect(self, reason: str = "", force: bool = False) -> bool:
        """请求一次重连（节流，避免频繁重连）"""
        if not self.running:
            return False
        
        now = time.time()
        if not force and (now - self._last_manual_reconnect_ts) < self._manual_reconnect_min_interval:
            return False
        
        if self._manual_reconnect_task and not self._manual_reconnect_task.done():
            return False
        
        self._last_manual_reconnect_ts = now
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        
        async def _run():
            if reason:
                self.logger.warning(f"触发EdgeX WebSocket重连: {reason}")
            await self._reconnect()
        
        self._manual_reconnect_task = loop.create_task(_run())
        return True
    
    def handle_order_book_update(self, message):
        """处理EdgeX订单簿更新消息"""
        try:
            if isinstance(message, str):
                message = json.loads(message)
            
            # 处理 ping/pong 消息以保持连接
            msg_type = message.get("type", "")
            if msg_type == "ping" or msg_type == "ping-message":
                # 所有ticker的时间戳都更新（ping表示连接活跃）
                for ticker in self.ticker_data.keys():
                    self.ticker_data[ticker]['last_message_time'] = datetime.now(TZ_UTC8)
                return
            
            # 检查是否是quote-event消息，包含depth数据
            if msg_type == "quote-event":
                content = message.get("content", {})
                channel = message.get("channel", "")
                
                if channel.startswith("depth."):
                    # 从channel提取contract_id: "depth.{contract_id}.15"
                    parts = channel.split(".")
                    if len(parts) >= 2:
                        contract_id = parts[1]
                        ticker = self.contract_to_ticker.get(contract_id)
                        
                        if not ticker or ticker not in self.ticker_data:
                            return
                        
                        data = content.get('data', [])
                        if data and len(data) > 0:
                            order_book_data = data[0]
                            depth_type = order_book_data.get('depthType', '')
                            start_version = order_book_data.get('startVersion', 0)
                            end_version = order_book_data.get('endVersion', 0)
                            
                            ticker_info = self.ticker_data[ticker]
                            
                            # 更新最后接收消息的时间
                            ticker_info['last_message_time'] = datetime.now(TZ_UTC8)
                            
                            # 验证版本号gap，并记录供Redis写入
                            version_gap = 0
                            try:
                                sv = int(start_version)
                                ev = int(end_version)
                            except (ValueError, TypeError):
                                sv, ev = 0, 0
                            if depth_type == 'SNAPSHOT':
                                ticker_info['last_end_version'] = end_version
                                ticker_info['version_gap'] = 0
                                ticker_info['start_version'] = sv
                                ticker_info['end_version'] = ev
                            elif depth_type == 'CHANGED':
                                if ticker_info['last_end_version'] is not None:
                                    version_gap = sv - int(ticker_info['last_end_version'])
                                    if version_gap > 5:
                                        self.logger.warning(
                                            f"{ticker} EdgeX WebSocket version gap过大: gap={version_gap}"
                                        )
                                    if version_gap < 0:
                                        version_gap = 0  # 乱序时视为0，不执行
                                ticker_info['last_end_version'] = end_version
                                ticker_info['version_gap'] = version_gap
                                ticker_info['start_version'] = sv
                                ticker_info['end_version'] = ev
                            
                            # 处理订单簿更新
                            if depth_type in ['SNAPSHOT', 'CHANGED']:
                                bids = order_book_data.get('bids', [])
                                asks = order_book_data.get('asks', [])
                                
                                if depth_type == 'SNAPSHOT':
                                    ticker_info['order_book']["bids"] = bids
                                    ticker_info['order_book']["asks"] = asks
                                else:
                                    # 增量更新
                                    self._update_order_book(ticker_info['order_book'], bids, asks)
                                
                                # 提取时间戳
                                timestamp = order_book_data.get('timestamp') or order_book_data.get('ts')
                                if timestamp:
                                    if isinstance(timestamp, (int, float)):
                                        dt = datetime.fromtimestamp(timestamp / 1000.0, tz=pytz.UTC)
                                        dt_utc8 = dt.astimezone(TZ_UTC8)
                                        ticker_info['price_timestamp'] = format_timestamp_to_seconds(dt_utc8)
                                    else:
                                        try:
                                            dt = datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                                            if dt.tzinfo is None:
                                                dt = pytz.UTC.localize(dt)
                                            dt_utc8 = dt.astimezone(TZ_UTC8)
                                            ticker_info['price_timestamp'] = format_timestamp_to_seconds(dt_utc8)
                                        except:
                                            ticker_info['price_timestamp'] = str(timestamp)
                                else:
                                    ticker_info['price_timestamp'] = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
                                
                                # 更新best bid/ask
                                self._update_bbo(ticker_info, ticker)
                                ticker_info['ready'] = True
                                
        except Exception as e:
            self.logger.error(f"处理EdgeX订单簿更新时出错: {e}", exc_info=True)
    
    def _update_order_book(self, order_book: dict, bids: list, asks: list):
        """更新订单簿（增量更新）"""
        bid_dict = {Decimal(bid['price']): Decimal(bid['size']) for bid in bids}
        ask_dict = {Decimal(ask['price']): Decimal(ask['size']) for ask in asks}
        
        # 更新bids
        for price, size in bid_dict.items():
            if size == 0:
                order_book["bids"] = [b for b in order_book["bids"] 
                                     if Decimal(b.get('price', 0)) != price]
            else:
                found = False
                for bid in order_book["bids"]:
                    if Decimal(bid.get('price', 0)) == price:
                        bid['size'] = str(size)
                        found = True
                        break
                if not found:
                    order_book["bids"].append({'price': str(price), 'size': str(size)})
        
        # 更新asks
        for price, size in ask_dict.items():
            if size == 0:
                order_book["asks"] = [a for a in order_book["asks"] 
                                     if Decimal(a.get('price', 0)) != price]
            else:
                found = False
                for ask in order_book["asks"]:
                    if Decimal(ask.get('price', 0)) == price:
                        ask['size'] = str(size)
                        found = True
                        break
                if not found:
                    order_book["asks"].append({'price': str(price), 'size': str(size)})
    
    def _update_bbo(self, ticker_info: dict, ticker: str):
        """更新best bid/ask并存储到Redis"""
        bids = ticker_info['order_book'].get("bids", [])
        asks = ticker_info['order_book'].get("asks", [])
        
        if bids:
            best_bid_entry = max(bids, key=lambda x: Decimal(x.get('price', 0)))
            ticker_info['best_bid'] = Decimal(best_bid_entry.get('price', 0))
            ticker_info['best_bid_size'] = Decimal(best_bid_entry.get('size', 0))
        else:
            ticker_info['best_bid'] = None
            ticker_info['best_bid_size'] = None
        
        if asks:
            best_ask_entry = min(asks, key=lambda x: Decimal(x.get('price', float('inf'))))
            ticker_info['best_ask'] = Decimal(best_ask_entry.get('price', 0))
            ticker_info['best_ask_size'] = Decimal(best_ask_entry.get('size', 0))
        else:
            ticker_info['best_ask'] = None
            ticker_info['best_ask_size'] = None
        
        # 如果价格数据完整，立即存储到Redis（含gap、start_version、end_version，条件写）
        if self.redis_client and ticker_info.get('best_bid') and ticker_info.get('best_ask'):
            try:
                timestamp = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
                self.redis_client.store_latest_bbo(
                    'edgex', ticker,
                    ticker_info['best_bid'],
                    ticker_info['best_bid_size'],
                    ticker_info['best_ask'],
                    ticker_info['best_ask_size'],
                    ticker_info.get('price_timestamp'),
                    timestamp=timestamp,
                    edgex_gap=ticker_info.get('version_gap'),
                    edgex_start_version=ticker_info.get('start_version'),
                    edgex_end_version=ticker_info.get('end_version'),
                )
            except Exception as e:
                self.logger.debug(f"EdgeX Redis存储失败 [{ticker}]: {e}")
    
    async def _reconnect(self):
        """重新连接并重新订阅所有标的"""
        try:
            # 断开现有连接
            try:
                self.ws_manager.disconnect_public()
            except Exception as e:
                self.logger.debug(f"断开EdgeX连接时出错（可忽略）: {e}")
            
            await asyncio.sleep(1)
            
            # 重新连接
            try:
                self.ws_manager.connect_public()
                await asyncio.sleep(1)  # 等待连接建立
            except Exception as e:
                self.logger.error(f"EdgeX重新连接失败: {e}", exc_info=True)
                raise
            
            public_client = self.ws_manager.get_public_client()
            if not public_client:
                raise Exception("无法获取EdgeX public client")
            
            # 重新注册消息处理器
            public_client.on_message("depth", self.handle_order_book_update)
            
            # 重新订阅所有标的
            for ticker, contract_id in zip(self._tickers_list, self._contract_ids_list):
                public_client.subscribe(f"depth.{contract_id}.15")
                # 重置订单簿状态，等待新的SNAPSHOT
                if ticker in self.ticker_data:
                    self.ticker_data[ticker]['ready'] = False
                    self.ticker_data[ticker]['last_message_time'] = None
                    self.ticker_data[ticker]['last_end_version'] = None
            
            self.logger.info("EdgeX WebSocket重连并重新订阅成功")
            return True
            
        except Exception as e:
            self.logger.error(f"EdgeX重连失败: {e}", exc_info=True)
            return False
    
    async def _monitor_and_reconnect(self):
        """监控连接状态并在断开时重连"""
        reconnect_delay = 1
        max_reconnect_delay = 30
        check_interval = 5  # 每5秒检查一次连接状态
        
        while self.running:
            try:
                # 检查连接状态
                public_client = self.ws_manager.get_public_client()
                if not public_client:
                    self.logger.error("⚠️⚠️⚠️ EdgeX WebSocket连接中断！⚠️⚠️⚠️")
                    self.logger.error("EdgeX public client不可用，尝试重连...")
                    reconnect_success = await self._reconnect()
                    if reconnect_success:
                        reconnect_delay = 1  # 重置延迟
                        self.logger.info("✅ EdgeX WebSocket重连成功")
                    else:
                        await asyncio.sleep(reconnect_delay)
                        reconnect_delay = min(max_reconnect_delay, reconnect_delay * 2)
                    continue
                
                # 检查数据是否过期（作为连接健康指标）
                all_stale = True
                has_any_data = False
                for ticker, data in self.ticker_data.items():
                    if data['last_message_time']:
                        has_any_data = True
                        time_since = (datetime.now(TZ_UTC8) - data['last_message_time']).total_seconds()
                        if time_since < self.websocket_stale_threshold:
                            all_stale = False
                            break
                
                # 如果有数据但全部过期，说明连接可能断开
                if has_any_data and all_stale:
                    self.logger.error("⚠️⚠️⚠️ EdgeX WebSocket连接中断！⚠️⚠️⚠️")
                    self.logger.error(f"EdgeX WebSocket数据全部过期（超过{self.websocket_stale_threshold}秒未更新），连接可能已断开，尝试重连...")
                    reconnect_success = await self._reconnect()
                    if reconnect_success:
                        reconnect_delay = 1  # 重置延迟
                        self.logger.info("✅ EdgeX WebSocket重连成功")
                    else:
                        await asyncio.sleep(reconnect_delay)
                        reconnect_delay = min(max_reconnect_delay, reconnect_delay * 2)
                else:
                    reconnect_delay = 1  # 连接正常，重置延迟
                
                await asyncio.sleep(check_interval)
                
            except asyncio.CancelledError:
                self.logger.info("EdgeX连接监控任务被取消")
                break
            except Exception as e:
                self.logger.error(f"EdgeX连接监控出错: {e}", exc_info=True)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(max_reconnect_delay, reconnect_delay * 2)
    
    async def subscribe_all(self, tickers: List[str], contract_ids: List[str]):
        """订阅所有标的并启动监控任务"""
        try:
            # 保存订阅信息，用于重连时重新订阅
            self._tickers_list = list(tickers)
            self._contract_ids_list = list(contract_ids)
            
            # 连接public websocket
            self.ws_manager.connect_public()
            await asyncio.sleep(1)  # 等待连接建立
            
            public_client = self.ws_manager.get_public_client()
            if not public_client:
                self.logger.error("无法获取EdgeX public client")
                return False
            
            # 注册消息处理器
            public_client.on_message("depth", self.handle_order_book_update)
            
            # 订阅所有标的
            for ticker, contract_id in zip(tickers, contract_ids):
                self.register_ticker(ticker, contract_id)
                public_client.subscribe(f"depth.{contract_id}.15")
                self.logger.info(f"EdgeX 订阅 {ticker} (contract_id: {contract_id})")
            
            # 启动监控和重连任务
            self.running = True
            self._reconnect_task = asyncio.create_task(self._monitor_and_reconnect())
            
            return True
        except Exception as e:
            self.logger.error(f"EdgeX订阅失败: {e}", exc_info=True)
            return False
    
    async def disconnect(self):
        """断开连接"""
        self.running = False
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        try:
            self.ws_manager.disconnect_public()
        except Exception as e:
            self.logger.debug(f"断开EdgeX连接时出错（可忽略）: {e}")


class LighterMultiTickerWebSocketManager:
    """Lighter WebSocket manager 支持多标的订阅"""
    
    def __init__(self, lighter_client, account_index: int, logger: logging.Logger = None, redis_client: Optional[Any] = None):
        """
        初始化 Lighter 多标的 WebSocket manager
        
        Args:
            lighter_client: Lighter客户端
            account_index: 账户索引
            logger: 日志记录器
            redis_client: Redis客户端实例（可选），用于实时存储价格数据
        """
        self.lighter_client = lighter_client
        self.account_index = account_index
        self.logger = logger or logging.getLogger(__name__)
        
        # Redis客户端（可选）
        self.redis_client = redis_client
        if self.redis_client is None and REDIS_AVAILABLE:
            try:
                self.redis_client = RedisPriceClient()
                self.logger.info("Lighter WebSocket Manager: Redis客户端初始化成功")
            except Exception as e:
                self.logger.warning(f"Lighter WebSocket Manager: Redis客户端初始化失败: {e}")
                self.redis_client = None
        
        # 存储每个 ticker 的订单簿数据
        # {ticker: {order_book, best_bid, best_ask, order_book_offset, price_timestamp, ready}}
        self.ticker_data: Dict[str, Dict[str, Any]] = {}
        
        # market_index到ticker的映射 {market_index: ticker}
        self.market_to_ticker: Dict[int, str] = {}
        
        # WebSocket连接
        self.ws = None
        self.running = False
        self.ws_url = "wss://mainnet.zklighter.elliot.ai/stream"
        self._connect_task = None  # 保存连接任务引用，用于追踪和重启
        self._manual_reconnect_task = None
        self._last_manual_reconnect_ts = 0.0
        self._manual_reconnect_min_interval = 10
        
    def register_ticker(self, ticker: str, market_index: int):
        """注册一个标的"""
        if ticker not in self.ticker_data:
            self.ticker_data[ticker] = {
                'order_book': {"bids": [], "asks": []},
                'best_bid': None,
                'best_ask': None,
                'best_bid_size': None,
                'best_ask_size': None,
                'order_book_offset': None,
                'price_timestamp': None,
                'exchange_ts_ms': None,
                'ready': False,
                'snapshot_loaded': False
            }
        self.market_to_ticker[market_index] = ticker
    
    def _handle_order_book_cutoff(self, data: dict) -> bool:
        """检查订单簿更新是否完整"""
        order_book = data.get("order_book", {})
        
        # 验证必需字段
        if not order_book or "code" not in order_book or "offset" not in order_book:
            self.logger.warning("Incomplete order book update - missing required fields")
            return False
        
        # 检查订单簿是否有预期的结构
        if "asks" not in order_book or "bids" not in order_book:
            self.logger.warning("Incomplete order book update - missing bids/asks")
            return False
        
        # 验证asks和bids是列表
        if not isinstance(order_book["asks"], list) or not isinstance(order_book["bids"], list):
            self.logger.warning("Invalid order book structure - asks/bids should be lists")
            return False
        
        return True
    
    def _validate_order_book_offset(self, ticker_info: dict, new_offset: int) -> bool:
        """验证offset的连续性（允许gaps，但拒绝out-of-order或duplicate）"""
        current_offset = ticker_info.get('order_book_offset')
        
        if current_offset is None:
            # 第一个offset，总是有效
            ticker_info['order_book_offset'] = new_offset
            return True
        
        # 只拒绝 new_offset <= current_offset（out of order或duplicate）
        # 允许gaps（new_offset > current_offset + 1）
        if new_offset <= current_offset:
            self.logger.warning(
                f"Out of order update received! Expected offset > {current_offset}, got {new_offset}"
            )
            return False
        
        # 更新offset并继续处理
        ticker_info['order_book_offset'] = new_offset
        return True
    
    def _update_order_book(self, order_book: dict, bids: list, asks: list):
        """更新订单簿（增量更新），保持bids降序排列，asks升序排列"""
        bid_dict = {Decimal(bid['price']): Decimal(bid['size']) for bid in bids}
        ask_dict = {Decimal(ask['price']): Decimal(ask['size']) for ask in asks}
        
        # 更新bids
        for price, size in bid_dict.items():
            if size == 0:
                order_book["bids"] = [b for b in order_book["bids"] 
                                     if Decimal(b.get('price', 0)) != price]
            else:
                found = False
                for bid in order_book["bids"]:
                    if Decimal(bid.get('price', 0)) == price:
                        bid['size'] = str(size)
                        found = True
                        break
                if not found:
                    order_book["bids"].append({'price': str(price), 'size': str(size)})
        
        # 更新asks
        for price, size in ask_dict.items():
            if size == 0:
                order_book["asks"] = [a for a in order_book["asks"] 
                                     if Decimal(a.get('price', 0)) != price]
            else:
                found = False
                for ask in order_book["asks"]:
                    if Decimal(ask.get('price', 0)) == price:
                        ask['size'] = str(size)
                        found = True
                        break
                if not found:
                    order_book["asks"].append({'price': str(price), 'size': str(size)})
        
        # 保持排序：bids降序，asks升序
        order_book["bids"].sort(key=lambda x: Decimal(x.get('price', 0)), reverse=True)
        order_book["asks"].sort(key=lambda x: Decimal(x.get('price', float('inf'))))
    
    def get_ticker_data(
        self, ticker: str
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[str], Optional[float]]:
        """获取指定ticker的BBO数据。exchange_ts_ms 为 Lighter 消息里的 timestamp 字段（毫秒 UTC）。"""
        if ticker not in self.ticker_data:
            return None, None, None, None, None, None

        data = self.ticker_data[ticker]

        # 检查是否就绪
        if not data['ready'] or not data['best_bid'] or not data['best_ask']:
            # 添加调试日志
            if not data['ready']:
                self.logger.debug(f"{ticker} Lighter WebSocket数据未就绪: ready={data['ready']}, snapshot_loaded={data.get('snapshot_loaded', False)}")
            elif not data['best_bid'] or not data['best_ask']:
                bids_list = data['order_book'].get('bids', [])
                asks_list = data['order_book'].get('asks', [])
                self.logger.debug(f"{ticker} Lighter WebSocket数据不完整: best_bid={data['best_bid']}, best_ask={data['best_ask']}, bids_count={len(bids_list)}, asks_count={len(asks_list)}")
            return None, None, None, None, None, None

        best_bid = Decimal(str(data['best_bid']))
        best_ask = Decimal(str(data['best_ask']))
        
        # 从orderbook中获取对应的size（bids降序，asks升序，直接取第一个元素）
        bids_list = data['order_book'].get('bids', [])
        asks_list = data['order_book'].get('asks', [])

        best_bid_size = data.get('best_bid_size')
        if best_bid_size is not None:
            best_bid_size = Decimal(str(best_bid_size))
        elif bids_list:
            best_bid_size = Decimal(str(bids_list[0]['size']))

        best_ask_size = data.get('best_ask_size')
        if best_ask_size is not None:
            best_ask_size = Decimal(str(best_ask_size))
        elif asks_list:
            best_ask_size = Decimal(str(asks_list[0]['size']))

        ex_ms = data.get("exchange_ts_ms")
        if ex_ms is not None:
            try:
                ex_ms = float(ex_ms)
            except (TypeError, ValueError):
                ex_ms = None

        return best_bid, best_bid_size, best_ask, best_ask_size, data['price_timestamp'], ex_ms

    def request_reconnect(self, reason: str = "", force: bool = False) -> bool:
        """请求一次重连（节流，避免频繁重连）"""
        now = time.time()
        if not force and (now - self._last_manual_reconnect_ts) < self._manual_reconnect_min_interval:
            return False
        
        if self._manual_reconnect_task and not self._manual_reconnect_task.done():
            return False
        
        self._last_manual_reconnect_ts = now
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        
        async def _run():
            if reason:
                self.logger.warning(f"触发Lighter WebSocket重连: {reason}")
            await self._restart_connection()
        
        self._manual_reconnect_task = loop.create_task(_run())
        return True

    async def _restart_connection(self):
        """强制重启连接任务"""
        self.running = False
        
        # 取消连接任务
        if self._connect_task and not self._connect_task.done():
            self._connect_task.cancel()
            try:
                await self._connect_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.warning(f"取消连接任务时出错: {e}")
        
        # 关闭 WebSocket 连接
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.logger.debug(f"关闭WebSocket连接时出错（可忽略）: {e}")
        
        self.ws = None
        self._connect_task = asyncio.create_task(self.connect())
    
    async def connect(self):
        """连接到Lighter WebSocket并订阅所有标的"""
        reconnect_delay = 1
        max_reconnect_delay = 30
        
        while True:
            try:
                # 确保 self.ws 在连接前为 None
                self.ws = None
                
                # 配置 ping_interval 和 ping_timeout 以保持连接活跃
                # ping_interval: 每20秒主动发送一次ping，确保连接保持活跃
                # ping_timeout: 等待pong响应的超时时间为10秒
                # close_timeout: 关闭连接的超时时间
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,    # 每20秒主动发送ping
                    ping_timeout=10,     # 等待pong响应的超时时间
                    close_timeout=10
                ) as self.ws:
                    # 订阅所有标的的order_book
                    for market_index, ticker in self.market_to_ticker.items():
                        await self.ws.send(json.dumps({
                            "type": "subscribe",
                            "channel": f"order_book/{market_index}"
                        }))
                        self.logger.info(f"Lighter 订阅 order_book {ticker} (market_index: {market_index})")
                    for market_index, ticker in self.market_to_ticker.items():
                        await self.ws.send(json.dumps({
                            "type": "subscribe",
                            "channel": f"ticker/{market_index}"
                        }))
                        self.logger.info(f"Lighter 订阅 ticker(BBO) {ticker} (market_index: {market_index})")

                    self.running = True
                    reconnect_delay = 1
                    self.logger.info("✅ Lighter WebSocket连接成功")
                    
                    # 消息处理循环
                    async for message in self.ws:
                        if not self.running:
                            break
                        
                        try:
                            data = json.loads(message)
                            await self._handle_message(data)
                        except json.JSONDecodeError as e:
                            self.logger.error(f"JSON解析错误: {e}")
                        except Exception as e:
                            self.logger.error(f"处理消息时出错: {e}", exc_info=True)
            
            except asyncio.CancelledError:
                self.logger.info("Lighter WebSocket连接任务被取消")
                break
            except Exception as e:
                self.logger.error(f"⚠️⚠️⚠️ Lighter WebSocket连接失败！⚠️⚠️⚠️")
                self.logger.error(f"Lighter WebSocket错误: {e}", exc_info=True)

            
            if not self.running:
                break
            
            self.running = False
            self.ws = None  # 确保清理连接状态
            
            try:
                self.logger.warning(f"Lighter WebSocket将在 {reconnect_delay} 秒后重连...")
                await asyncio.sleep(reconnect_delay)
            except asyncio.CancelledError:
                self.logger.info("重连等待被取消")
                break
            
            reconnect_delay = min(max_reconnect_delay, reconnect_delay * 2)

    def _apply_ticker_channel_bbo(self, ticker: str, data: dict) -> None:
        """Lighter 官方 BBO：`ticker/{id}` 的 update/ticker（文档：随 nonce 推送最优买卖）。 """
        ticker_info = self.ticker_data[ticker]
        tk = data.get("ticker") or {}
        bid = tk.get("b") or {}
        ask = tk.get("a") or {}
        bid_px = bid.get("price")
        ask_px = ask.get("price")
        if bid_px is None or ask_px is None:
            return

        bid_sz = bid.get("size")
        ask_sz = ask.get("size")
        ticker_info["best_bid"] = Decimal(str(bid_px))
        ticker_info["best_ask"] = Decimal(str(ask_px))
        ticker_info["best_bid_size"] = Decimal(str(bid_sz)) if bid_sz is not None else None
        ticker_info["best_ask_size"] = Decimal(str(ask_sz)) if ask_sz is not None else None

        timestamp = data.get("timestamp")
        if timestamp:
            if isinstance(timestamp, (int, float)):
                dt = datetime.fromtimestamp(timestamp / 1000.0, tz=pytz.UTC)
                ticker_info["price_timestamp"] = format_timestamp_to_seconds(dt.astimezone(TZ_UTC8))
            else:
                try:
                    dt = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = pytz.UTC.localize(dt)
                    ticker_info["price_timestamp"] = format_timestamp_to_seconds(dt.astimezone(TZ_UTC8))
                except Exception:
                    ticker_info["price_timestamp"] = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
        else:
            ticker_info["price_timestamp"] = format_timestamp_to_seconds(datetime.now(TZ_UTC8))

        raw_ts = data.get("timestamp")
        try:
            ticker_info["exchange_ts_ms"] = float(raw_ts) if raw_ts is not None else None
        except (TypeError, ValueError):
            ticker_info["exchange_ts_ms"] = None

        if ticker_info["best_bid"] and ticker_info["best_ask"]:
            if not ticker_info["ready"]:
                self.logger.info(
                    "%s Lighter ticker 通道 BBO 就绪 bid=%s ask=%s",
                    ticker,
                    ticker_info["best_bid"],
                    ticker_info["best_ask"],
                )
            ticker_info["ready"] = True
            if self.redis_client:
                try:
                    receive_ts = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
                    exchange_ts_ms = None
                    if data.get("timestamp") is not None:
                        try:
                            exchange_ts_ms = float(data["timestamp"])
                        except (ValueError, TypeError):
                            pass
                    ob_offset = ticker_info.get("order_book_offset")
                    self.redis_client.store_latest_bbo(
                        "lighter",
                        ticker,
                        ticker_info["best_bid"],
                        ticker_info.get("best_bid_size"),
                        ticker_info["best_ask"],
                        ticker_info.get("best_ask_size"),
                        ticker_info.get("price_timestamp"),
                        timestamp=receive_ts,
                        lighter_exchange_ts_ms=exchange_ts_ms,
                        lighter_offset=int(ob_offset) if ob_offset is not None else None,
                    )
                except Exception as exc:
                    self.logger.debug("Lighter Redis存储失败 [%s]: %s", ticker, exc)
        else:
            ticker_info["ready"] = False
    
    async def _handle_message(self, data: dict):
        """处理WebSocket消息"""
        if data.get("type") == "ping":
            if self.ws:
                try:
                    await self.ws.send(json.dumps({"type": "pong"}))
                except Exception as exc:
                    self.logger.debug("Lighter pong send failed: %s", exc)
            return

        channel = data.get("channel", "")

        if channel.startswith("ticker:"):
            try:
                market_index = int(channel.split(":")[1])
            except (ValueError, IndexError):
                return
            ticker = self.market_to_ticker.get(market_index)
            if not ticker or ticker not in self.ticker_data:
                return
            if data.get("type") in ("update/ticker", "subscribed/ticker"):
                self._apply_ticker_channel_bbo(ticker, data)
            return
        
        # 处理order_book更新
        if channel.startswith("order_book:"):
            market_index = int(channel.split(":")[1])
            ticker = self.market_to_ticker.get(market_index)
            
            if not ticker or ticker not in self.ticker_data:
                return
            
            ticker_info = self.ticker_data[ticker]
            
            # 获取消息类型
            msg_type = data.get("type", "")
            
            # 处理订阅确认消息或快照数据
            if msg_type == "subscribed/order_book":
                # 初始快照 - 清空并填充订单簿
                ticker_info['order_book']['bids'].clear()
                ticker_info['order_book']['asks'].clear()
                
                # 处理初始快照
                order_book_data = data.get("order_book", {})
                if order_book_data and "offset" in order_book_data:
                    # 设置初始offset
                    initial_offset = order_book_data["offset"]
                    ticker_info['order_book_offset'] = initial_offset
                    self.logger.debug(f"{ticker} Lighter初始订单簿offset设置为: {initial_offset}")
                
                bids = order_book_data.get("bids", [])
                asks = order_book_data.get("asks", [])
                
                # 更新订单簿
                self._update_order_book(ticker_info['order_book'], bids, asks)
                ticker_info['snapshot_loaded'] = True
                
                self.logger.debug(f"{ticker} Lighter收到快照数据: bids={len(bids)}, asks={len(asks)}")
            
            elif msg_type == "update/order_book" and ticker_info['snapshot_loaded']:
                # 增量更新 - 首先检查是否完整
                if not self._handle_order_book_cutoff(data):
                    self.logger.warning(f"{ticker} Lighter跳过不完整的订单簿更新")
                    return
                
                # 提取offset
                order_book_data = data.get("order_book", {})
                if not order_book_data or "offset" not in order_book_data:
                    self.logger.warning(f"{ticker} Lighter订单簿更新缺少offset，跳过")
                    return
                
                new_offset = order_book_data["offset"]
                
                # 验证offset序列
                if not self._validate_order_book_offset(ticker_info, new_offset):
                    # Out-of-order或duplicate更新，跳过
                    return
                
                bids = order_book_data.get("bids", [])
                asks = order_book_data.get("asks", [])
                
                # 更新订单簿
                self._update_order_book(ticker_info['order_book'], bids, asks)
            
            elif msg_type == "update/order_book" and not ticker_info['snapshot_loaded']:
                # 在收到快照之前忽略更新
                return
            
            # elif msg_type == "ping":
            #     # 响应ping
            #     if self.ws:
            #         await self.ws.send(json.dumps({"type": "pong"}))
            #     return
            
            else:
                # 未知消息类型
                self.logger.debug(f"{ticker} Lighter未知消息类型: {msg_type}")
                return
            
            # 更新best bid/ask（bids降序排列，asks升序排列，直接取第一个元素）
            bids_list = ticker_info['order_book']['bids']
            asks_list = ticker_info['order_book']['asks']
            
            if bids_list:
                ticker_info['best_bid'] = Decimal(str(bids_list[0]['price']))
            else:
                ticker_info['best_bid'] = None
            
            if asks_list:
                ticker_info['best_ask'] = Decimal(str(asks_list[0]['price']))
            else:
                ticker_info['best_ask'] = None

            if bids_list:
                ticker_info["best_bid_size"] = Decimal(str(bids_list[0]["size"]))
            else:
                ticker_info["best_bid_size"] = None
            if asks_list:
                ticker_info["best_ask_size"] = Decimal(str(asks_list[0]["size"]))
            else:
                ticker_info["best_ask_size"] = None
            
            # 更新时间戳
            timestamp = data.get("timestamp", None)
            if timestamp:
                if isinstance(timestamp, (int, float)):
                    dt = datetime.fromtimestamp(timestamp / 1000.0, tz=pytz.UTC)
                    dt_utc8 = dt.astimezone(TZ_UTC8)
                    ticker_info['price_timestamp'] = format_timestamp_to_seconds(dt_utc8)
                else:
                    try:
                        dt = datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                        if dt.tzinfo is None:
                            dt = pytz.UTC.localize(dt)
                        dt_utc8 = dt.astimezone(TZ_UTC8)
                        ticker_info['price_timestamp'] = format_timestamp_to_seconds(dt_utc8)
                    except:
                        ticker_info['price_timestamp'] = str(timestamp)
            else:
                ticker_info['price_timestamp'] = format_timestamp_to_seconds(datetime.now(TZ_UTC8))

            raw_ts = data.get("timestamp")
            try:
                ticker_info["exchange_ts_ms"] = float(raw_ts) if raw_ts is not None else None
            except (TypeError, ValueError):
                ticker_info["exchange_ts_ms"] = None
            
            # 更新ready状态：只有当best_bid和best_ask都存在时才设置为True
            # 如果任何一个为None，则设置为False
            if ticker_info['best_bid'] and ticker_info['best_ask']:
                if not ticker_info['ready']:
                    self.logger.info(f"{ticker} Lighter WebSocket数据已就绪: best_bid={ticker_info['best_bid']}, best_ask={ticker_info['best_ask']}")
                ticker_info['ready'] = True
                
                # 如果价格数据完整，立即存储到Redis（含exchange_ts_ms、offset，条件写）
                if self.redis_client:
                    try:
                        best_bid_size = ticker_info.get("best_bid_size")
                        best_ask_size = ticker_info.get("best_ask_size")

                        receive_ts = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
                        exchange_ts_ms = None
                        if data.get("timestamp") is not None:
                            try:
                                exchange_ts_ms = float(data["timestamp"])
                            except (ValueError, TypeError):
                                pass
                        ob_offset = ticker_info.get('order_book_offset')
                        self.redis_client.store_latest_bbo(
                            'lighter', ticker,
                            ticker_info['best_bid'],
                            best_bid_size,
                            ticker_info['best_ask'],
                            best_ask_size,
                            ticker_info.get('price_timestamp'),
                            timestamp=receive_ts,
                            lighter_exchange_ts_ms=exchange_ts_ms,
                            lighter_offset=int(ob_offset) if ob_offset is not None else None,
                        )
                    except Exception as e:
                        self.logger.debug(f"Lighter Redis存储失败 [{ticker}]: {e}")
            else:
                if ticker_info['ready']:
                    self.logger.warning(f"{ticker} Lighter WebSocket数据变为不可用: best_bid={ticker_info['best_bid']}, best_ask={ticker_info['best_ask']}")
                ticker_info['ready'] = False
    
    async def subscribe_all(self, tickers: List[str], market_indexes: List[int]):
        """订阅所有标的并启动WebSocket连接"""
        # 注册所有标的
        for ticker, market_index in zip(tickers, market_indexes):
            self.register_ticker(ticker, market_index)
        
        # 检查任务状态，如果已结束则重新创建
        if self._connect_task is None or self._connect_task.done():
            # 如果任务已结束，检查是否有异常
            if self._connect_task and self._connect_task.done():
                try:
                    await self._connect_task  # 获取异常信息
                except asyncio.CancelledError:
                    self.logger.debug("之前的连接任务被取消")
                except Exception as e:
                    self.logger.warning(f"之前的连接任务异常: {e}")
            
            # 创建新任务
            self._connect_task = asyncio.create_task(self.connect())
            self.logger.info("Lighter WebSocket连接任务已启动/重启")
        else:
            self.logger.info("Lighter WebSocket连接任务已在运行")
        
        await asyncio.sleep(2)  # 等待连接建立
    
    async def disconnect(self):
        """断开连接"""
        self.running = False
        
        # 取消连接任务
        if self._connect_task and not self._connect_task.done():
            self._connect_task.cancel()
            try:
                await self._connect_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.warning(f"取消连接任务时出错: {e}")
        
        # 关闭 WebSocket 连接
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.logger.debug(f"关闭WebSocket连接时出错（可忽略）: {e}")
        
        self.ws = None


class BackpackMultiTickerWebSocketManager:
    """Backpack WebSocket manager 支持多标的订阅"""
    
    def __init__(self, logger: logging.Logger = None, redis_client: Optional[Any] = None):
        """
        初始化 Backpack 多标的 WebSocket manager
        
        Args:
            logger: 日志记录器
            redis_client: Redis客户端实例（可选），用于实时存储价格数据
        """
        self.logger = logger or logging.getLogger(__name__)
        
        # Redis客户端（可选）
        self.redis_client = redis_client
        if self.redis_client is None and REDIS_AVAILABLE:
            try:
                self.redis_client = RedisPriceClient()
                self.logger.info("Backpack WebSocket Manager: Redis客户端初始化成功")
            except Exception as e:
                self.logger.warning(f"Backpack WebSocket Manager: Redis客户端初始化失败: {e}")
                self.redis_client = None
        
        # 存储每个 ticker 的订单簿数据
        # {ticker: {order_book, best_bid, best_bid_size, best_ask, best_ask_size, price_timestamp, ready}}
        self.ticker_data: Dict[str, Dict[str, Any]] = {}
        
        # contract_id到ticker的映射 {contract_id: ticker}
        self.contract_to_ticker: Dict[str, str] = {}
        
        # WebSocket连接
        self.ws = None
        self.running = False
        self.ws_url = "wss://ws.backpack.exchange"
        
    def register_ticker(self, ticker: str, contract_id: str):
        """注册一个标的"""
        if ticker not in self.ticker_data:
            self.ticker_data[ticker] = {
                'order_book': {"bids": {}, "asks": {}},
                'best_bid': None,
                'best_bid_size': None,
                'best_ask': None,
                'best_ask_size': None,
                'price_timestamp': None,
                'ready': False
            }
        self.contract_to_ticker[contract_id] = ticker
    
    def get_ticker_data(
        self, ticker: str
    ) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[str], Optional[float]]:
        """获取指定ticker的BBO数据。"""
        if ticker not in self.ticker_data:
            return None, None, None, None, None, None

        data = self.ticker_data[ticker]

        # 检查是否就绪
        if not data['ready'] or not data['best_bid'] or not data['best_ask']:
            return None, None, None, None, None, None

        return (
            data['best_bid'],
            data['best_bid_size'],
            data['best_ask'],
            data['best_ask_size'],
            data['price_timestamp'],
            None,
        )
    
    async def connect(self):
        """连接到Backpack WebSocket并订阅所有标的"""
        reconnect_delay = 1
        max_reconnect_delay = 30
        
        while True:
            try:
                async with websockets.connect(self.ws_url) as self.ws:
                    # 订阅所有标的的depth
                    for contract_id, ticker in self.contract_to_ticker.items():
                        subscribe_message = {
                            "method": "SUBSCRIBE",
                            "params": [f"depth.{contract_id}"]
                        }
                        await self.ws.send(json.dumps(subscribe_message))
                        self.logger.info(f"Backpack 订阅 {ticker} (contract_id: {contract_id})")
                    
                    self.running = True
                    reconnect_delay = 1
                    self.logger.info("Backpack WebSocket连接成功")
                    
                    # 消息处理循环
                    async for message in self.ws:
                        if not self.running:
                            break
                        
                        try:
                            # 处理ping帧
                            if isinstance(message, bytes) and message == b'\x09':
                                await self.ws.pong()
                                continue
                            
                            data = json.loads(message)
                            self._handle_message(data)
                        except json.JSONDecodeError as e:
                            self.logger.error(f"JSON解析错误: {e}")
                        except Exception as e:
                            self.logger.error(f"处理消息时出错: {e}", exc_info=True)
            
            except websockets.exceptions.ConnectionClosed:
                self.logger.warning("Backpack WebSocket连接关闭，重连中...")
            except Exception as e:
                self.logger.error(f"Backpack WebSocket错误: {e}", exc_info=True)
            
            if not self.running:
                break
            
            self.running = False
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(max_reconnect_delay, reconnect_delay * 2)
    
    def _handle_message(self, data: dict):
        """处理WebSocket消息"""
        stream = data.get("stream", "")
        
        # 处理depth更新
        if stream.startswith("depth."):
            contract_id = stream.split(".")[1] if len(stream.split(".")) > 1 else None
            ticker = self.contract_to_ticker.get(contract_id)
            
            if not ticker or ticker not in self.ticker_data:
                return
            
            ticker_info = self.ticker_data[ticker]
            depth_data = data.get("data", {})
            
            if not depth_data:
                return
            
            # 更新bids
            bids = depth_data.get('b', [])
            for bid in bids:
                price = Decimal(bid[0])
                size = Decimal(bid[1])
                if size > 0:
                    ticker_info['order_book']['bids'][price] = size
                else:
                    ticker_info['order_book']['bids'].pop(price, None)
            
            # 更新asks
            asks = depth_data.get('a', [])
            for ask in asks:
                price = Decimal(ask[0])
                size = Decimal(ask[1])
                if size > 0:
                    ticker_info['order_book']['asks'][price] = size
                else:
                    ticker_info['order_book']['asks'].pop(price, None)
            
            # 更新best bid和ask
            if ticker_info['order_book']['bids']:
                ticker_info['best_bid'] = max(ticker_info['order_book']['bids'].keys())
                ticker_info['best_bid_size'] = ticker_info['order_book']['bids'][ticker_info['best_bid']]
            else:
                ticker_info['best_bid'] = None
                ticker_info['best_bid_size'] = None
            
            if ticker_info['order_book']['asks']:
                ticker_info['best_ask'] = min(ticker_info['order_book']['asks'].keys())
                ticker_info['best_ask_size'] = ticker_info['order_book']['asks'][ticker_info['best_ask']]
            else:
                ticker_info['best_ask'] = None
                ticker_info['best_ask_size'] = None
            
            # 更新时间戳
            ticker_info['price_timestamp'] = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
            
            if ticker_info['best_bid'] and ticker_info['best_ask']:
                ticker_info['ready'] = True
                if not ticker_info.get('ready_logged', False):
                    self.logger.info(f"Backpack {ticker} order book ready")
                    ticker_info['ready_logged'] = True
                
                # 如果价格数据完整，立即存储到Redis
                if self.redis_client:
                    try:
                        timestamp = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
                        self.redis_client.store_latest_bbo(
                            'backpack', ticker,
                            ticker_info['best_bid'],
                            ticker_info['best_bid_size'],
                            ticker_info['best_ask'],
                            ticker_info['best_ask_size'],
                            ticker_info.get('price_timestamp'),
                            timestamp=timestamp
                        )
                    except Exception as e:
                        self.logger.debug(f"Backpack Redis存储失败 [{ticker}]: {e}")
    
    async def subscribe_all(self, tickers: List[str], contract_ids: List[str]):
        """订阅所有标的并启动WebSocket连接"""
        # 注册所有标的
        for ticker, contract_id in zip(tickers, contract_ids):
            self.register_ticker(ticker, contract_id)
        
        # 启动WebSocket连接任务
        asyncio.create_task(self.connect())
        await asyncio.sleep(2)  # 等待连接建立
    
    async def disconnect(self):
        """断开连接"""
        self.running = False
        if self.ws:
            await self.ws.close()

