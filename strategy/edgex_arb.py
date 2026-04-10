"""Main arbitrage trading bot for edgeX and Lighter exchanges."""
import asyncio
import signal
import logging
import os
import sys
import time
import requests
import traceback
import ssl
import socket
from decimal import Decimal
from typing import Tuple, Optional
from datetime import datetime


from lighter.signer_client import SignerClient
import aiohttp
import aiohttp_retry
from edgex_sdk import Client, WebSocketManager

from .data_logger import DataLogger
from .db_logger import ArbInfoDB
from .order_book_manager import OrderBookManager
from .websocket_manager import WebSocketManagerWrapper
from .order_manager import OrderManager
from .position_tracker import PositionTracker
from monitor.redis_client import RedisPriceClient


class Config:
    """Simple config class to wrap dictionary for edgeX client."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class EdgexArb:
    """Arbitrage trading bot: makes post-only orders on edgeX, and market orders on Lighter."""
    
    # 全局信号执行时间限制（跨所有实例/线程共享）
    _global_last_signal_time = 0
    _global_signal_cooldown = 5  # 秒
    _global_signal_lock = asyncio.Lock()

    def __init__(self, ticker: str, order_quantity: Decimal,
                 fill_timeout: int = 5, max_position: Decimal = Decimal('0'),
                 long_ex_threshold: Decimal = Decimal('-0.0002'),
                 short_ex_threshold: Decimal = Decimal('0.0002')):
        """Initialize the arbitrage trading bot."""
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.max_position = max_position
        self.stop_flag = False
        self._cleanup_done = False
        self.redis_listener_task = None  # 跟踪 Redis 监听任务
        self.background_tasks = set()  # 保存后台任务的强引用，防止被垃圾回收

        self.long_ex_threshold = long_ex_threshold
        self.short_ex_threshold = short_ex_threshold

        # Setup logger
        self._setup_logger()

        # Initialize modules
        self.data_logger = DataLogger(exchange="edgex", ticker=ticker, logger=self.logger)
        self.arb_info_db = ArbInfoDB()  # 初始化套利信息数据库
        self.order_book_manager = OrderBookManager(self.logger)
        
        self.ws_manager = WebSocketManagerWrapper(self.order_book_manager, self.logger)
        self.order_manager = OrderManager(self.order_book_manager, self.logger, ticker=ticker)

        # Initialize clients (will be set later)
        self.edgex_client = None
        self.edgex_ws_manager = None
        self.lighter_client = None

        # Configuration
        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX'))
        self.edgex_account_id = os.getenv('EDGEX_ACCOUNT_ID')
        self.edgex_stark_private_key = os.getenv('EDGEX_STARK_PRIVATE_KEY')
        self.edgex_base_url = os.getenv('EDGEX_BASE_URL', 'https://pro.edgex.exchange')
        self.edgex_ws_url = os.getenv('EDGEX_WS_URL', 'wss://quote.edgex.exchange')

        # Contract/market info (will be set during initialization)
        self.edgex_contract_id = None
        self.edgex_tick_size = None
        self.lighter_market_index = None
        self.base_amount_multiplier = None
        self.price_multiplier = None
        self.tick_size = None

        # Position tracker (will be initialized after clients)
        self.position_tracker = None

        # Redis client for trading signals
        try:
            self.redis_client = RedisPriceClient()
            # self.redis_client = RedisPriceClient()
            self.logger.info("✅ Redis client initialized for trading signals")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to initialize Redis client: {e}. Trading signals will not be published.")
            self.redis_client = None

        # 当前套利机会的 client_order_id（发现时生成，下单时传入）
        
        # Debug日志时间控制（每秒输出一次）
        self.last_debug_log_time = 0

        # Setup callbacks
        self._setup_callbacks()

    def _setup_logger(self):
        """Setup logging configuration."""
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/edgex_{self.ticker}_log.txt"

        self.logger = logging.getLogger(f"arbitrage_bot_{self.ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        # Disable verbose logging from external libraries
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)
        root_logger = logging.getLogger()
        if root_logger.level == logging.DEBUG:
            root_logger.setLevel(logging.INFO)

       

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)

        # Create formatters
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(asctime)s -%(levelname)s:%(name)s:%(message)s')

        console_handler.setFormatter(console_formatter)

        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

    def _setup_callbacks(self):
        """Setup callback functions for order updates."""
        self.ws_manager.set_callbacks(
            on_lighter_order_filled=self._handle_lighter_order_filled,
            on_edgex_order_update=self._handle_edgex_order_update
        )
        self.order_manager.set_callbacks(
            on_order_filled=self._handle_lighter_order_filled
        )

    def _handle_lighter_order_filled(self, order_data: dict):
        """Handle Lighter order fill."""
        try:
            order_data["avg_filled_price"] = (
                Decimal(order_data["filled_quote_amount"]) /
                Decimal(order_data["filled_base_amount"])
            )
            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                order_type = "OPEN"
                if self.position_tracker:
                    self.position_tracker.update_lighter_position(
                        -Decimal(order_data["filled_base_amount"]))
            else:
                order_data["side"] = "LONG"
                order_type = "CLOSE"
                if self.position_tracker:
                    self.position_tracker.update_lighter_position(
                        Decimal(order_data["filled_base_amount"]))

            client_order_id = str(order_data.get("client_order_id") or order_data.get("client_order_index", ""))
            order_type_label = "平仓" if order_type == "CLOSE" else "开仓"
            self.logger.info(
                f"[{client_order_id}] [{order_type}] [{order_type_label}] [Lighter] [FILLED]: "
                f"{order_data['filled_base_amount']} @ {order_data['avg_filled_price']}")
            # 获取 lighter 订单方向
            lighter_direction = 'sell' if order_data["is_ask"] else 'buy'
            # 记录到数据库 (lighter_filled类型)
            try:
                self.arb_info_db.log_arb_info(
                    symbol=self.ticker,
                    info_type=ArbInfoDB.INFO_TYPE_LIGHTER_FILLED,
                    client_order_id=client_order_id,
                    lighter_price=order_data["avg_filled_price"],
                    lighter_quantity=Decimal(order_data["filled_base_amount"])
                )
            except Exception as e:
                self.logger.warning(f"写入数据库失败 (lighter_filled): {e}")
            # 记录 Lighter 成交到套利 CSV（只写 lighter 列，edgex 留空）
            self.data_logger.log_fill(
                client_order_id=client_order_id,
                exchange='lighter',
                fill_amount=Decimal(order_data["filled_quote_amount"]),
                fill_quantity=Decimal(order_data["filled_base_amount"]),
                fill_price=order_data["avg_filled_price"],
                lighter_direction=lighter_direction
            )

            # Mark execution as complete
            self.order_manager.lighter_order_filled = True
            self.order_manager.order_execution_complete = True

        except Exception as e:
            self.logger.error(f"Error handling Lighter order result: {e}")

    def _handle_edgex_order_update(self, order: dict):
        """Handle EdgeX order update from WebSocket."""
        try:
            if order.get('contractId') != self.edgex_contract_id:
                return

            order_id = order.get('id')
            client_order_id = order.get('clientOrderId') or ''
            status = order.get('status')
            side = order.get('side', '').lower()
            # filled_size = Decimal(order.get('cumMatchSize', '0'))
            size = Decimal(order.get('cumMatchSize', '0'))
            filled_size = size

            if self.position_tracker:
                current_position = self.position_tracker.get_current_edgex_position()
                if side == 'buy':
                    is_close_order = current_position < 0
                else:
                    is_close_order = current_position > 0
                order_type = "CLOSE" if is_close_order else "OPEN"
                order_type_label = "平仓" if is_close_order else "开仓"
            else:
                if side == 'buy':
                    order_type = "OPEN"
                    order_type_label = "开仓"
                else:
                    order_type = "CLOSE"
                    order_type_label = "平仓"

            if status == 'CANCELED' and filled_size > 0:
                status = 'FILLED'

            # Update order status
            self.order_manager.update_edgex_order_status(status)

            # Handle filled orders
            if status == 'FILLED' and filled_size > 0:
                
                cumMatchValue = order.get('cumMatchValue', '0')
                price = Decimal(cumMatchValue) / filled_size
                if side == 'buy':
                    if self.position_tracker:
                        self.position_tracker.update_edgex_position(filled_size)
                else:
                    if self.position_tracker:
                        self.position_tracker.update_edgex_position(-filled_size)

                self.logger.info(
                    f"[{order_id}] [{order_type}] [{order_type_label}] [EdgeX] [{status}]: {filled_size} @ {price}, client_order_id: {client_order_id}")

                if filled_size > 0.0001:
                    # Log EdgeX trade to CSV (保留原有功能)
                   
                    # 记录 EdgeX 成交到套利 CSV（只写 edgex 列，lighter 留空）
                    fill_amount = Decimal(price) * filled_size
                    # side 已经是 'buy' 或 'sell'
                    # 记录到数据库 (edgex_filled类型)
                    try:
                        self.arb_info_db.log_arb_info(
                            symbol=self.ticker,
                            info_type=ArbInfoDB.INFO_TYPE_EDGEX_FILLED,
                            client_order_id=client_order_id,
                            edgex_price=Decimal(price),
                            edgex_quantity=filled_size
                        )
                    except Exception as e:
                        self.logger.warning(f"写入数据库失败 (edgex_filled): {e}")
                    self.data_logger.log_fill(
                        client_order_id=client_order_id,
                        exchange='edgex',
                        fill_amount=fill_amount,
                        fill_quantity=filled_size,
                        fill_price=Decimal(price),
                        edgex_direction=side
                    )

                # Update order status (no longer triggering Lighter order as we use concurrent market orders)
                self.order_manager.update_edgex_order_status(status)
            # elif status != 'FILLED':
            #     if status == 'OPEN':
            #         self.logger.info(f"[{order_id}] [{order_type}] [EdgeX] [{status}]: {size} @ ")
            #     else:
            #         self.logger.info(
            #             f"[{order_id}] [{order_type}] [EdgeX] [{status}]: {filled_size} ")

        except Exception as e:
            self.logger.error(f"Error handling EdgeX order update: {e}")

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        # Prevent multiple shutdown calls
        if self.stop_flag:
            return

        self.stop_flag = True

        if signum is not None:
            self.logger.info("\n🛑 Stopping...")
        else:
            self.logger.info("🛑 Stopping...")

        # Shutdown WebSocket connections
        try:
            if self.ws_manager:
                self.ws_manager.shutdown()
        except Exception as e:
            self.logger.error(f"Error shutting down WebSocket manager: {e}")

        # Close data logger
        try:
            if self.data_logger:
                self.data_logger.close()
        except Exception as e:
            self.logger.error(f"Error closing data logger: {e}")

        # Close arb info database
        try:
            if hasattr(self, 'arb_info_db') and self.arb_info_db:
                self.arb_info_db.close()
        except Exception as e:
            self.logger.error(f"Error closing arb info database: {e}")

        # Close logging handlers
        for handler in self.logger.handlers[:]:
            try:
                handler.close()
                self.logger.removeHandler(handler)
            except Exception:
                pass

        # Note: Async cleanup will be handled in run() finally block

    async def _async_cleanup(self):
        """Async cleanup for aiohttp sessions and other async resources."""
        if self._cleanup_done:
            return

        self._cleanup_done = True

        # 取消 Redis 监听任务
        if self.redis_listener_task and not self.redis_listener_task.done():
            self.redis_listener_task.cancel()
            try:
                await asyncio.wait_for(self.redis_listener_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception as e:
                self.logger.warning(f"⚠️ Error cancelling Redis listener task: {e}")
            self.logger.info("🔌 Redis listener task cancelled")

        # 取消所有后台任务
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
            self.logger.info(f"🔌 Cancelled {len(self.background_tasks)} background tasks")
        self.background_tasks.clear()

        # Close EdgeX client (closes aiohttp sessions) with timeout
        try:
            if self.edgex_client:
                await asyncio.wait_for(
                    self.edgex_client.close(),
                    timeout=2.0
                )
                self.logger.info("🔌 EdgeX client closed")
        except asyncio.TimeoutError:
            self.logger.warning("⚠️ Timeout closing EdgeX client, forcing shutdown")
        except Exception as e:
            self.logger.error(f"Error closing EdgeX client: {e}")

        # Close EdgeX WebSocket manager connections
        try:
            if self.edgex_ws_manager:
                self.edgex_ws_manager.disconnect_all()
        except Exception as e:
            self.logger.error(f"Error disconnecting EdgeX WebSocket manager: {e}")

        # Close async Redis client
        try:
            if self.redis_client:
                await self.redis_client.close_async()
                self.logger.info("🔌 Async Redis client closed")
        except Exception as e:
            self.logger.error(f"Error closing async Redis client: {e}")

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def initialize_lighter_client(self):
        """Initialize the Lighter client."""
        if self.lighter_client is None:
            api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
            if not api_key_private_key:
                raise Exception("API_KEY_PRIVATE_KEY environment variable not set")

            self.lighter_client = SignerClient(
                self.lighter_base_url,
                self.account_index,
                {self.api_key_index: api_key_private_key},
            )
            self._configure_lighter_http_client(self.lighter_client)

            err = self.lighter_client.check_client()
            if err is not None:
                raise Exception(f"CheckClient error: {err}")

            self.logger.info("✅ Lighter client initialized successfully")
        return self.lighter_client

    def _configure_lighter_http_client(self, lighter_client: SignerClient):
        """Force IPv4 and disable env proxy lookup for Lighter SDK."""
        try:
            api_client = lighter_client.api_client
            config = api_client.configuration
            rest_client = api_client.rest_client

            ssl_context = ssl.create_default_context(cafile=config.ssl_ca_cert)
            if config.cert_file:
                ssl_context.load_cert_chain(config.cert_file, keyfile=config.key_file)
            if not config.verify_ssl:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

            trace_config = aiohttp.TraceConfig()

         

            connector = aiohttp.TCPConnector(
                limit=config.connection_pool_maxsize,
                ssl=ssl_context,
                family=socket.AF_INET,
            )
            timeout = aiohttp.ClientTimeout(total=20, connect=5, sock_connect=5, sock_read=10)

            rest_client.pool_manager = aiohttp.ClientSession(
                connector=connector,
                trust_env=False,
                timeout=timeout,
                trace_configs=[trace_config],
            )

            if config.retries is not None:
                rest_client.retry_client = aiohttp_retry.RetryClient(
                    client_session=rest_client.pool_manager,
                    retry_options=aiohttp_retry.ExponentialRetry(
                        attempts=config.retries,
                        factor=0.0,
                        start_timeout=0.0,
                        max_timeout=120.0,
                    ),
                )
            else:
                rest_client.retry_client = None
        except Exception as e:
            self.logger.warning(f"Failed to configure Lighter HTTP client: {e}")

    def initialize_edgex_client(self):
        """Initialize the EdgeX client."""
        if not self.edgex_account_id or not self.edgex_stark_private_key:
            raise ValueError(
                "EDGEX_ACCOUNT_ID and EDGEX_STARK_PRIVATE_KEY must be set in environment variables")

        self.edgex_client = Client(
            base_url=self.edgex_base_url,
            account_id=int(self.edgex_account_id),
            stark_private_key=self.edgex_stark_private_key
        )

        self.edgex_ws_manager = WebSocketManager(
            base_url=self.edgex_ws_url,
            account_id=int(self.edgex_account_id),
            stark_pri_key=self.edgex_stark_private_key
        )

        self.logger.info("✅ EdgeX client initialized successfully")
        return self.edgex_client

    def get_lighter_market_config(self) -> Tuple[int, int, int, Decimal]:
        """Get Lighter market configuration."""
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        headers = {"accept": "application/json"}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            if not response.text.strip():
                raise Exception("Empty response from Lighter API")

            data = response.json()

            if "order_books" not in data:
                raise Exception("Unexpected response format")

            for market in data["order_books"]:
                if market["symbol"] == self.ticker:
                    price_multiplier = pow(10, market["supported_price_decimals"])
                    return (market["market_id"],
                            pow(10, market["supported_size_decimals"]),
                            price_multiplier,
                            Decimal("1") / (Decimal("10") ** market["supported_price_decimals"]))
            raise Exception(f"Ticker {self.ticker} not found")

        except Exception as e:
            self.logger.error(f"⚠️ Error getting market config: {e}")
            raise

    async def get_edgex_contract_info(self) -> Tuple[str, Decimal]:
        """Get EdgeX contract ID and tick size."""
        if not self.edgex_client:
            raise Exception("EdgeX client not initialized")

        response = await self.edgex_client.get_metadata()
        data = response.get('data', {})
        if not data:
            raise ValueError("Failed to get EdgeX metadata")

        contract_list = data.get('contractList', [])
        if not contract_list:
            raise ValueError("Failed to get EdgeX contract list")

        current_contract = None
        for c in contract_list:
            if c.get('contractName') == self.ticker + 'USD':
                current_contract = c
                break

        if not current_contract:
            raise ValueError(f"Failed to get contract ID for ticker {self.ticker}")

        contract_id = current_contract.get('contractId')
        min_quantity = Decimal(current_contract.get('minOrderSize'))
        tick_size = Decimal(current_contract.get('tickSize'))

        if self.order_quantity < min_quantity:
            raise ValueError(
                f"Order quantity is less than min quantity: {self.order_quantity} < {min_quantity}")

        return contract_id, tick_size

    

    async def run(self):
        """Run the arbitrage bot."""
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        except asyncio.CancelledError:
            self.logger.info("\n🛑 Task cancelled...")
        finally:
            self.logger.info("🔄 Cleaning up...")
            self.shutdown()
            try:
                await asyncio.wait_for(self._async_cleanup(), timeout=5.0)
            except asyncio.TimeoutError:
                self.logger.warning("⚠️ Cleanup timeout, forcing exit")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")
    
    async def trading_loop(self):
        """Main trading loop (Direct Market Order Hedging)."""
        self.logger.info("🚀 Starting arbitrage bot for " + self.ticker)

        # Initialize clients
        try:
            self.initialize_lighter_client()
            self.initialize_edgex_client()
            self.edgex_contract_id, self.edgex_tick_size = await self.get_edgex_contract_info()
            (self.lighter_market_index, self.base_amount_multiplier,
             self.price_multiplier, self.tick_size) = self.get_lighter_market_config()
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            return

        # Initialize position tracker
        self.position_tracker = PositionTracker(
            self.ticker, self.edgex_client, self.edgex_contract_id,
            self.lighter_base_url, self.account_index, self.logger
        )

        # Configure modules
        self.order_manager.set_edgex_config(self.edgex_client, self.edgex_contract_id, self.edgex_tick_size)
        self.order_manager.set_lighter_config(
            self.lighter_client, self.lighter_market_index,
            self.base_amount_multiplier, self.price_multiplier, self.tick_size
        )

        self.ws_manager.set_edgex_ws_manager(self.edgex_ws_manager, self.edgex_contract_id)
        self.ws_manager.set_lighter_config(self.lighter_client, self.lighter_market_index, self.account_index)

        # Setup WebSockets
        try:
            self.ws_manager.start_edgex_websocket()
            
            # Wait for EdgeX Orderbook
            # start_time = time.time()
            # while not self.order_book_manager.edgex_order_book_ready and not self.stop_flag:
            #     if time.time() - start_time > 10: break
            #     await asyncio.sleep(0.5)
            
            self.ws_manager.start_lighter_websocket()
            
            # Wait for Lighter Orderbook
            start_time = time.time()
            while not self.order_book_manager.lighter_order_book_ready and not self.stop_flag:
                if time.time() - start_time > 10: break
                await asyncio.sleep(0.5)

        except Exception as e:
            self.logger.error(f"❌ Failed to setup websockets: {e}")
            return

        await asyncio.sleep(5)
        self.position_tracker.edgex_position = await self.position_tracker.get_edgex_position()
        self.position_tracker.lighter_position = await self.position_tracker.get_lighter_position()

        # 启动Redis信号监听任务 (推送模式)
        if self.redis_client:
            # asyncio.create_task(self.listen_for_signals())
            self.redis_listener_task = asyncio.create_task(self.listen_for_signals())
            
        # 启动套利指标计算任务
        self.calculation_task = asyncio.create_task(self._calculate_arb_metrics_task())
        self.background_tasks.add(self.calculation_task)
        self.calculation_task.add_done_callback(self.background_tasks.discard)
        self.logger.info("🧮 启动套利指标计算任务")

        # --- MAIN LOOP ---
        self.logger.info("🟢 Entering main trading loop (Direct Market Order Hedging)")
        
        while not self.stop_flag:
            try:
                # 主循环现在只负责保持运行，信号由 listen_for_signals 处理
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"❌ Error in trading loop: {e}")
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(1)

    async def listen_for_signals(self):
        """监听Redis推送信号并处理（异步处理，不阻塞信号接收，带错误处理和重试）"""
        retry_count = 0
        base_delay = 1  # 初始重试延迟（秒）
        max_delay = 30  # 最大重试延迟（秒）
        
        while not self.stop_flag :
            try:
                self.logger.info(f"🔌 开启Redis信号推送监听: {self.ticker} (channel: trading_channel:{self.ticker.upper()})")
                
                async for signal_message in self.redis_client.listen_trading_signals(self.ticker):
                    if self.stop_flag:
                        self.logger.info(f"🛑 停止Redis信号监听: {self.ticker}")
                        return
                                 
                    # 异步处理信号，不阻塞信号接收
                    # 使用 background_tasks 保存强引用，防止任务被垃圾回收
                    task = asyncio.create_task(self._process_signal_async(signal_message))
                    self.background_tasks.add(task)
                    task.add_done_callback(self.background_tasks.discard)
                
                # 如果正常退出循环（生成器结束），记录日志
                self.logger.warning(f"⚠️ Redis信号监听循环正常结束: {self.ticker}")
                retry_count = 0
                
            except asyncio.CancelledError:
                self.logger.info(f"🛑 Redis信号监听任务被取消: {self.ticker}")
                return
            except Exception as e:
                retry_count += 1
                delay = min(base_delay * (2 ** (retry_count - 1)), max_delay)
                self.logger.error(
                    f"❌ Redis信号监听出错 [{self.ticker}]: {e}, "
                    f"{delay}秒后重连 (第{retry_count}次)"
                )
                self.logger.error(traceback.format_exc())
                         
                await asyncio.sleep(delay)
               
        
        self.logger.warning(f"⚠️ Redis信号监听任务退出: {self.ticker}")
    
    async def _process_signal_async(self, signal_message: str):
        """异步处理信号（不阻塞信号接收循环）"""
        try:
           
            # 首先记录所有接收到的原始信号（用于调试）
            self.logger.info(f" -------> 收到原始推送信号: {signal_message}")
            
            # 全局5秒冷却限制（跨所有实例/线程）
            async with EdgexArb._global_signal_lock:
                current_time = time.time()
                time_since_last = current_time - EdgexArb._global_last_signal_time
                if time_since_last < EdgexArb._global_signal_cooldown:
                    self.logger.info(f"⏳ 全局信号冷却中，跳过执行: 距离上次执行 {time_since_last:.2f}s < {EdgexArb._global_signal_cooldown}s")
                    return
                # 立即更新时间戳，防止其他线程在同一时间进入
                EdgexArb._global_last_signal_time = current_time
            
            # 解析信号格式: symbol+{edgex_direction}+edgex+{lighter_direction}+lighter+{client_order_id}
            parts = signal_message.split('+')
            if len(parts) == 6:
                signal_symbol = parts[0]
                edgex_direction = parts[1].lower()  # 'buy' 或 'sell'
                exchange1 = parts[2]  # 应该是 'edgex'
                lighter_direction = parts[3].lower()  # 'buy' 或 'sell'
                exchange2 = parts[4]  # 应该是 'lighter'
                client_order_id = parts[5]  # 从信号中提取client_order_id
                
                # 根据当前持仓与方向判断是开仓还是平仓
                current_position = self.position_tracker.get_current_edgex_position() if self.position_tracker else 0
                if edgex_direction == 'sell':
                    is_close_order = current_position > 0
                else:  # edgex_direction == 'buy'
                    is_close_order = current_position < 0
                order_type_label = "平仓" if is_close_order else "开仓"
                
                # 记录解析后的信号信息
                
                # 检查信号是否匹配当前ticker和交易所
                if signal_symbol.upper() == self.ticker.upper() and exchange1.lower() == 'edgex' and exchange2.lower() == 'lighter':
                    self.logger.info(f" 🧧 收到推送信号（{order_type_label}）: {signal_message}, client_order_id: {client_order_id}")
                    
                    # 检查持仓限制
                    # current_position = self.position_tracker.get_current_edgex_position() if self.position_tracker else 0
                    # can_execute = False
                    
                    # if is_close_order:
                    #     # 平仓信号：检查是否有对应持仓需要平仓
                    #     if edgex_direction == 'sell':  # 平多仓（edgex卖出）
                    #         if current_position > 0:
                    #             can_execute = True
                    #             self.logger.info(f"✅ 平仓信号验证通过（平多仓）: 当前持仓={current_position}, edgex方向={edgex_direction}")
                    #         else:
                    #             self.logger.warning(f"⚠️ 平仓信号但无多仓可平: 当前持仓={current_position}, 信号={signal_message}")
                    #     else:  # edgex_direction == 'buy'，平空仓（edgex买入）
                    #         if current_position < 0:
                    #             can_execute = True
                    #             self.logger.info(f"✅ 平仓信号验证通过（平空仓）: 当前持仓={current_position}, edgex方向={edgex_direction}")
                    #         else:
                    #             self.logger.warning(f"⚠️ 平仓信号但无空仓可平: 当前持仓={current_position}, 信号={signal_message}")
                    # else:
                        # 开仓信号：检查持仓限制
                        # if edgex_direction == 'buy':
                        #     if current_position < self.max_position:
                        #         can_execute = True
                        # else:  # edgex_direction == 'sell'
                        #     if current_position > -1 * self.max_position:
                        #         can_execute = True
                    can_execute = True
                    
                    if can_execute:
                        self.logger.info(f"⚡ 执行套利（{order_type_label}，来自推送）: EdgeX {edgex_direction.upper()}, Lighter {lighter_direction.upper()}, client_order_id: {client_order_id}")
                        

                        # 记录discovery价格
                        try:
                            current_ex_bid, current_ex_ask = await self.order_manager.fetch_edgex_bbo_prices()
                            
                            # 执行风控检查: 从Redis读取风控标志位
                            risk_code = self.redis_client.get_risk_status('edgex', self.ticker)
                            if '1' in risk_code:
                                error_types = []
                                if len(risk_code) >= 5:
                                    if risk_code[0] == '1': error_types.append("REST延时")
                                    if risk_code[1] == '1': error_types.append("Probe延时")
                                    if risk_code[2] == '1': error_types.append("到达延时")
                                    if risk_code[3] == '1': error_types.append("Ping延时")
                                    if risk_code[4] == '1': error_types.append("Trade盘口倒挂")
                                error_detail = ",".join(error_types) if error_types else "网络断开或未知异常"
                                
                                self.logger.warning(f"❌ 风控拦截: Redis风控标志位 {risk_code} ({error_detail})，取消本次套利。client_order_id: {client_order_id}")
                                self.data_logger.log_fill(
                                    client_order_id=client_order_id,
                                    exchange=f'风控拦截({risk_code}:{error_detail})',
                                    edgex_direction=edgex_direction,
                                    lighter_direction=lighter_direction,
                                )
                                return

                            current_lighter_bid, current_lighter_ask = await self.order_manager.fetch_lighter_bbo_prices()
                            
                            edgex_price = current_ex_ask if edgex_direction == 'buy' else current_ex_bid
                            lighter_price = current_lighter_ask if lighter_direction == 'buy' else current_lighter_bid
                            
                            # 计算discovery_spread: 根据信号方向计算价差
                            # edgex sell + lighter buy: spread = edgex_bid - lighter_ask
                            # edgex buy + lighter sell: spread = lighter_bid - edgex_ask
                            received_spread = None
                            if edgex_price and lighter_price:
                                if edgex_direction == 'sell':
                                    # 我们在edgex卖出(获得edgex_bid), 在lighter买入(支付lighter_ask)
                                    received_spread = (edgex_price - lighter_price)/lighter_price
                                else:
                                    # 我们在edgex买入(支付edgex_ask), 在lighter卖出(获得lighter_bid)
                                    received_spread = (lighter_price - edgex_price)/edgex_price
                            
                              # 检查spread是否满足>3bps的条件，未满足则不下单
                            min_spread_bps = 5
                            min_spread = min_spread_bps / 10000.0  # 3bps = 0.0003
                            if received_spread is None:
                                self.logger.info("❌ 收到信号但无法计算有效spread，跳过本次下单。")
                                return
                            if received_spread <= min_spread:
                                self.logger.info(
                                    f"❌ 收到信号但spread不足 {min_spread_bps}bps，"
                                    f"实际spread={received_spread:.6f}，跳过本次下单。"
                                )
                                return
                            asyncio.create_task(self._execute_arbitrage(edgex_direction, lighter_direction, client_order_id, is_close_order))

                            # 记录到数据库 (received类型 - 带价格信息)
                            try:
                                self.arb_info_db.log_arb_info(
                                    symbol=self.ticker,
                                    info_type=ArbInfoDB.INFO_TYPE_RECEIVED,
                                    client_order_id=client_order_id,
                                    edgex_price=edgex_price if current_ex_bid and current_ex_ask else None,
                                    lighter_price=lighter_price if current_lighter_bid and current_lighter_ask else None,
                                    spread_ratio=received_spread,
                                    edgex_direction=edgex_direction,
                                    lighter_direction=lighter_direction
                                )
                            except Exception as e:
                                self.logger.warning(f"写入数据库失败 (received with price): {e}")
                            
                            self.data_logger.log_fill(
                                client_order_id=client_order_id,
                                exchange='discovery',
                                edgex_discovery_price=edgex_price if current_ex_bid and current_ex_ask else None,
                                lighter_discovery_price=lighter_price if current_lighter_bid and current_lighter_ask else None,
                                discovery_spread=received_spread,
                                edgex_direction=edgex_direction,
                                lighter_direction=lighter_direction
                            )
                        except Exception as e:
                            self.logger.warning(f"⚠️ 记录discovery价格失败: {e}")

                      

                        #  tmd 正着不行我反着来
                        # if edgex_direction == 'buy':
                        #     edgex_direction = 'sell'
                        # else:
                        #     edgex_direction = 'buy'

                        # if lighter_direction == 'buy':
                        #     lighter_direction = 'sell'
                        # else:
                        #     lighter_direction = 'buy'

                        # 异步执行套利（不阻塞信号接收）
                        
                       
                    else:
                        self.logger.warning(f"⚠️ 持仓限制，无法执行{order_type_label}信号: 当前持仓={current_position}, 最大持仓={self.max_position}, 信号={signal_message}")
                else:
                    # 信号不匹配当前ticker或交易所，记录日志以便调试
                    if signal_symbol.upper() != self.ticker.upper():
                        self.logger.debug(f"🔍 忽略信号（ticker不匹配）: 信号ticker={signal_symbol.upper()}, 当前ticker={self.ticker.upper()}, 信号={signal_message}")
                    elif exchange1.lower() != 'edgex' or exchange2.lower() != 'lighter':
                        self.logger.debug(f"🔍 忽略信号（交易所不匹配）: exchange1={exchange1}, exchange2={exchange2}, 信号={signal_message}")
                    else:
                        # 理论上不应该到这里，但添加日志以防万一
                        self.logger.warning(f"⚠️ 信号格式正确但未匹配: ticker={signal_symbol.upper()}, exchange1={exchange1}, exchange2={exchange2}, 信号={signal_message}")
            else:
                self.logger.warning(f"⚠️ 信号格式错误（部分数量不正确）: 期望6部分，实际{len(parts)}部分, 信号={signal_message}")
        except Exception as e:
            self.logger.error(f"❌ 处理信号失败: {e}, 信号内容: {signal_message}")
            self.logger.error(traceback.format_exc())
    
    async def _execute_arbitrage(self, edgex_side: str, lighter_side: str, client_order_id: str = None, is_close_order: bool = None):
        """Execute arbitrage by placing market orders on both exchanges simultaneously."""
        if is_close_order is None:
            order_type_label = "交易"
        else:
            order_type_label = "平仓" if is_close_order else "开仓"
        self.logger.info(f"⚡ 执行套利（{order_type_label}）: EdgeX {edgex_side.upper()} / Lighter {lighter_side.upper()}, client_order_id: {client_order_id}")

            
        try:
            # Get current prices for logging
            ex_bid, ex_ask = await self.order_manager.fetch_edgex_bbo_prices()
            lighter_bid, lighter_ask = self.order_book_manager.get_lighter_bbo()
            
            # Place market orders on both exchanges concurrently，传入 client_order_id
            edgex_task = asyncio.create_task(
                self.order_manager.place_edgex_market_order(
                    edgex_side, self.order_quantity, client_order_id=client_order_id
                )
            )
            lighter_task = asyncio.create_task(
                self.order_manager.place_lighter_market_order(
                    lighter_side, 
                    self.order_quantity, 
                    client_order_id=client_order_id
                )
            )
            
            # Wait for both orders to complete
            edgex_order_id, lighter_tx_hash = await asyncio.gather(
                edgex_task, 
                lighter_task,
                return_exceptions=True
            )
            
            # Check results
            if isinstance(edgex_order_id, Exception):
                self.logger.error(f"❌ {order_type_label}失败 - EdgeX订单失败: {edgex_order_id}")
                if not isinstance(lighter_tx_hash, Exception) and lighter_tx_hash:
                    self.logger.warning(f"⚠️ {order_type_label} - EdgeX失败但Lighter订单成功，持仓不匹配！")
            elif isinstance(lighter_tx_hash, Exception):
                self.logger.error(f"❌ {order_type_label}失败 - Lighter订单失败: {lighter_tx_hash}")
                if edgex_order_id:
                    self.logger.warning(f"⚠️ {order_type_label} - Lighter失败但EdgeX订单成功，持仓不匹配！")
            else:
                self.logger.info(f"✅ {order_type_label}完成: EdgeX订单 {edgex_order_id}, Lighter交易 {lighter_tx_hash}, client_order_id: {client_order_id}")
                
        except Exception as e:
            self.logger.error(f"❌ 执行{order_type_label}时出错: {e}, client_order_id: {client_order_id}")
            self.logger.error(traceback.format_exc())
    
    async def _calculate_arb_metrics_task(self):
        """定时计算套利指标的后台任务"""
        calculation_interval = 30  # 每30秒计算一次
        
        # 等待初始化完成
        await asyncio.sleep(10)
        
        while not self.stop_flag:
            try:
                await self._calculate_arb_metrics()
            except Exception as e:
                self.logger.error(f"计算套利指标出错: {e}")
                self.logger.error(traceback.format_exc())
            
            await asyncio.sleep(calculation_interval)
    
    async def _calculate_arb_metrics(self):
        """
        计算套利指标：
        - lighter_filled.time_gap = lighter_filled.timestamp - received.timestamp
        - edgex_filled.time_gap = edgex_filled.timestamp - received.timestamp
        - received.time_gap = received.timestamp - discovery.timestamp
        - actual_spread_ratio 根据方向计算
        """
        try:
            # 获取未计算的client_order_id列表
            uncalculated_ids = self.arb_info_db.get_uncalculated_client_order_ids(limit=50)
            
            if not uncalculated_ids:
                return
            
            updated_count = 0
            
            for client_order_id in uncalculated_ids:
                records = self.arb_info_db.get_records_by_client_order_id(client_order_id)
                
                if not records:
                    continue
                
                # 按info_type分组
                records_by_type = {}
                for record in records:
                    info_type = record.get('info_type')
                    if info_type not in records_by_type:
                        records_by_type[info_type] = []
                    records_by_type[info_type].append(record)
                
                # 获取各类型的第一条记录
                discovery_record = records_by_type.get(ArbInfoDB.INFO_TYPE_DISCOVERY, [None])[0]
                received_record = records_by_type.get(ArbInfoDB.INFO_TYPE_RECEIVED, [None])[0]
                edgex_filled_record = records_by_type.get(ArbInfoDB.INFO_TYPE_EDGEX_FILLED, [None])[0]
                lighter_filled_record = records_by_type.get(ArbInfoDB.INFO_TYPE_LIGHTER_FILLED, [None])[0]
                
                # 计算time_gap并更新
                
                # 1. received.time_gap = received.timestamp - discovery.timestamp
                if received_record and discovery_record:
                    time_gap = self._calculate_time_gap(
                        discovery_record.get('timestamp'),
                        received_record.get('timestamp')
                    )
                    if time_gap is not None:
                        self.arb_info_db.update_record(
                            record_id=received_record['id'],
                            time_gap=time_gap,
                            calculated=1
                        )
                        updated_count += 1
                elif received_record:
                    # 没有discovery记录，只标记为已计算
                    self.arb_info_db.update_record(
                        record_id=received_record['id'],
                        calculated=1
                    )
                
                # 2. edgex_filled.time_gap = edgex_filled.timestamp - received.timestamp
                if edgex_filled_record and received_record:
                    time_gap = self._calculate_time_gap(
                        received_record.get('timestamp'),
                        edgex_filled_record.get('timestamp')
                    )
                    if time_gap is not None:
                        self.arb_info_db.update_record(
                            record_id=edgex_filled_record['id'],
                            time_gap=time_gap,
                            calculated=1
                        )
                        updated_count += 1
                elif edgex_filled_record:
                    self.arb_info_db.update_record(
                        record_id=edgex_filled_record['id'],
                        calculated=1
                    )
                
                # 3. lighter_filled.time_gap = lighter_filled.timestamp - received.timestamp
                if lighter_filled_record and received_record:
                    time_gap = self._calculate_time_gap(
                        received_record.get('timestamp'),
                        lighter_filled_record.get('timestamp')
                    )
                    if time_gap is not None:
                        self.arb_info_db.update_record(
                            record_id=lighter_filled_record['id'],
                            time_gap=time_gap,
                            calculated=1
                        )
                        updated_count += 1
                elif lighter_filled_record:
                    self.arb_info_db.update_record(
                        record_id=lighter_filled_record['id'],
                        calculated=1
                    )
                
                # 4. 计算actual_spread_ratio并更新discovery记录
                if discovery_record and edgex_filled_record and lighter_filled_record:
                    edgex_price = edgex_filled_record.get('edgex_price')
                    lighter_price = lighter_filled_record.get('lighter_price')
                    edgex_direction = discovery_record.get('edgex_direction')
                    
                    if edgex_price and lighter_price and edgex_direction:
                        # 根据方向计算actual_spread_ratio
                        if edgex_direction == 'sell':
                            # edgex卖出(获得edgex_price), lighter买入(支付lighter_price)
                            actual_spread_ratio = (edgex_price - lighter_price) / lighter_price
                        else:
                            # edgex买入(支付edgex_price), lighter卖出(获得lighter_price)
                            actual_spread_ratio = (lighter_price - edgex_price) / edgex_price
                        
                        self.arb_info_db.update_record(
                            record_id=discovery_record['id'],
                            actual_spread_ratio=actual_spread_ratio,
                            calculated=1
                        )
                        updated_count += 1
                        
                        self.logger.info(
                            f"🧮 计算套利指标 [{client_order_id}]: "
                            f"actual_spread={actual_spread_ratio:.6f}, "
                            f"edgex_price={edgex_price}, lighter_price={lighter_price}"
                        )
                elif discovery_record:
                    # 没有完整的filled记录，只标记为已计算
                    self.arb_info_db.update_record(
                        record_id=discovery_record['id'],
                        calculated=1
                    )
            
            if updated_count > 0:
                self.logger.info(f"🧮 更新了 {updated_count} 条套利指标记录")
                
        except Exception as e:
            self.logger.error(f"计算套利指标失败: {e}")
            self.logger.error(traceback.format_exc())
    
    def _calculate_time_gap(self, start_time_str: str, end_time_str: str) -> Optional[float]:
        """
        计算两个时间字符串之间的时间差（秒）
        
        Args:
            start_time_str: 开始时间字符串
            end_time_str: 结束时间字符串
            
        Returns:
            时间差（秒），如果无法计算则返回None
        """
        if not start_time_str or not end_time_str:
            return None
        try:
            # 解析时间字符串: 格式是 'YYYY-MM-DDTHH:MM:SS.mmm'
            start_dt = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%f')
            end_dt = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S.%f')
            gap_seconds = (end_dt - start_dt).total_seconds()
            return gap_seconds
        except (ValueError, AttributeError):
            return None
