import asyncio
import json
import signal
import logging
import os
import sys
import time
import requests
import argparse
import traceback
import csv
from decimal import Decimal
from typing import Tuple

from lighter.signer_client import SignerClient
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from exchanges.backpack import BackpackClient
import websockets
import datetime
import pytz

from typing import Dict, Any, List, Optional, Tuple, Type, Union
from dataclasses import dataclass
from bpx.constants.enums import OrderTypeEnum, TimeInForceEnum


@dataclass
class OrderResult:
    """Standardized order result structure."""
    success: bool
    order_id: Optional[str] = None
    side: Optional[str] = None
    size: Optional[Decimal] = None
    price: Optional[Decimal] = None
    status: Optional[str] = None
    error_message: Optional[str] = None
    filled_size: Optional[Decimal] = None


class Config:
    """Simple config class to wrap dictionary for Backpack client."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class BackPack_Maker:
    """Trading bot that places post-only orders on Backpack and hedges with market orders on Lighter."""

    def __init__(self, ticker: str, order_quantity: Decimal, fill_timeout: int = 5, iterations: int = 20):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.max_exposure = Decimal(3) * order_quantity
        self.fill_timeout = fill_timeout
        self.iterations = iterations
        self.backpack_position = Decimal('0')
        self.current_order = {}

        # Initialize logging to file
        os.makedirs("logs", exist_ok=True)
        # self.log_filename = f"logs/backpack_{ticker}_hedge_mode_log.txt"
        # self.csv_filename = f"logs/backpack_{ticker}_hedge_mode_trades.csv"
        self.original_stdout = sys.stdout

        # # Initialize CSV file with headers if it doesn't exist
        # self._initialize_csv_file()

        # Setup logger
        self.logger = logging.getLogger(f"backpack_maker_{ticker}")
        self.logger.setLevel(logging.INFO)

        # Clear any existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Disable verbose logging from external libraries
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        # Create file handler
        # file_handler = logging.FileHandler(self.log_filename)
        # file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Create different formatters for file and console
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

        # file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)

        # Add handlers to logger
        # self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # Prevent propagation to root logger to avoid duplicate messages
        self.logger.propagate = False

        # State management
        self.stop_flag = False
        self.order_counter = 0

        # Backpack state
        self.backpack_client = None
        self.backpack_contract_id = None
        self.backpack_tick_size = None
        self.backpack_order_status = None

        # Backpack order book state for websocket-based BBO
        self.backpack_order_book = {'bids': {}, 'asks': {}}
        self.backpack_best_bid = None
        self.backpack_best_ask = None
        self.backpack_order_book_ready = False

        

        # Strategy state
        self.waiting_for_lighter_fill = False
        self.wait_start_time = None

        # Order execution tracking
        self.order_execution_complete = False

       
        # Backpack configuration
        self.backpack_public_key = os.getenv('BACKPACK_PUBLIC_KEY')
        self.backpack_secret_key = os.getenv('BACKPACK_SECRET_KEY')

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")

        # Close WebSocket connections
        if self.backpack_client:
            try:
                # Note: disconnect() is async, but shutdown() is sync
                # We'll let the cleanup happen naturally
                self.logger.info("🔌 Backpack WebSocket will be disconnected")
            except Exception as e:
                self.logger.error(f"Error disconnecting Backpack WebSocket: {e}")


        # Close logging handlers properly
        for handler in self.logger.handlers[:]:
            try:
                handler.close()
                self.logger.removeHandler(handler)
            except Exception:
                pass

    # def _initialize_csv_file(self):
    #     """Initialize CSV file with headers if it doesn't exist."""
    #     if not os.path.exists(self.csv_filename):
    #         with open(self.csv_filename, 'w', newline='') as csvfile:
    #             writer = csv.writer(csvfile)
    #             writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity'])

    # def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
    #     """Log trade details to CSV file."""
    #     timestamp = datetime.now(pytz.UTC).isoformat()

    #     with open(self.csv_filename, 'a', newline='') as csvfile:
    #         writer = csv.writer(csvfile)
    #         writer.writerow([
    #             exchange,
    #             timestamp,
    #             side,
    #             price,
    #             quantity
    #         ])

    #     self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price}")

  
    
   
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

   
    def initialize_backpack_client(self):
        """Initialize the Backpack client."""
        if not self.backpack_public_key or not self.backpack_secret_key:
            raise ValueError("BACKPACK_PUBLIC_KEY and BACKPACK_SECRET_KEY must be set in environment variables")

        # Create config for Backpack client
        config_dict = {
            'ticker': self.ticker,
            'contract_id': '',  # Will be set when we get contract info
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),  # Will be updated when we get contract info
            'close_order_side': 'sell'  # Default, will be updated based on strategy
        }

        # Wrap in Config class for Backpack client
        config = Config(config_dict)

        # Initialize Backpack client
        self.backpack_client = BackpackClient(config)
        self.logger.info("✅ Backpack client initialized successfully")
        return self.backpack_client

    
    async def get_backpack_contract_info(self) -> Tuple[str, Decimal]:
        """Get Backpack contract ID and tick size."""
        if not self.backpack_client:
            raise Exception("Backpack client not initialized")

        contract_id, tick_size = await self.backpack_client.get_contract_attributes()

        # if self.order_quantity < self.backpack_client.config.quantity:
        #     raise ValueError(
        #         f"Order quantity is less than min quantity: {self.order_quantity} < {self.backpack_client.config.quantity}")

        return contract_id, tick_size

    async def fetch_backpack_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        """Fetch best bid/ask prices from Backpack using websocket data."""
        time_gap = time.time() - self.best_order_time
        # Use WebSocket data if available
        if self.backpack_order_book_ready and self.backpack_best_bid and self.backpack_best_ask  and time_gap < 5:
            if self.backpack_best_bid > 0 and self.backpack_best_ask > 0 and self.backpack_best_bid < self.backpack_best_ask:
                return self.backpack_best_bid, self.backpack_best_ask

        if time_gap >=5:
            self.logger.warning("WebSocket price too late, falling back to REST API")
        # Fallback to REST API if websocket data is not available
        else:
            self.logger.warning("WebSocket BBO data not available, falling back to REST API")
        if not self.backpack_client:
            raise Exception("Backpack client not initialized")

        best_bid, best_ask = await self.backpack_client.fetch_bbo_prices(self.backpack_contract_id)

        return best_bid, best_ask

    def round_to_tick(self, price: Decimal) -> Decimal:
        """Round price to tick size."""
        if self.backpack_tick_size is None:
            return price
        return (price / self.backpack_tick_size).quantize(Decimal('1')) * self.backpack_tick_size
    
    async def get_account_positions(self) -> Decimal:
        """Get account positions using official SDK."""      
        return  await self.backpack_client.get_account_positions()
    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel order."""
        return await self.backpack_client.cancel_order(order_id)    

    async def place_bbo_order(self, side: str, quantity: Decimal):
        # Get best bid/ask prices
        max_retries = 15
        retry_count = 0
 
        while retry_count < max_retries:
        # Place the order using Backpack client
            retry_count += 1
            best_bid, best_ask = await self.fetch_backpack_bbo_prices()

            order_result = await self.backpack_client.place_post_only_order(
                contract_id=self.backpack_contract_id,
                quantity=quantity,
                direction=side.lower(),
                best_bid = best_bid,
                best_ask = best_ask
            )

            if order_result.success:
                return order_result.order_id
            else:
                self.logger.error(f"⚠️ place_bbo_order failed: {order_result.error_message}")
                await asyncio.sleep(3)
        return None

    async def place_backpack_post_only_order(self, side: str, quantity: Decimal):
        """Place a post-only order on Backpack."""
        if not self.backpack_client:
            raise Exception("Backpack client not initialized")

        self.backpack_order_status = None
        self.logger.info(f"[OPEN] [Backpack] [{side}] Placing Backpack POST-ONLY order")
        order_id = await self.place_bbo_order(side, quantity)
        if order_id:
            self.backpack_order_status = 'NEW'

        start_time = time.time()
        while not self.stop_flag:
            if self.backpack_order_status == 'CANCELED':
                self.backpack_order_status = 'NEW'
                order_id = await self.place_bbo_order(side, quantity)
                start_time = time.time()
                await asyncio.sleep(0.5)
            elif self.backpack_order_status in ['NEW', 'OPEN', 'PENDING', 'CANCELING', 'PARTIALLY_FILLED']:
                await asyncio.sleep(0.5)
                if time.time() - start_time > 10:
                    try:
                        # Cancel the order using Backpack client
                        cancel_result = await self.backpack_client.cancel_order(order_id)
                        if not cancel_result.success:
                            self.logger.error(f"❌ Error canceling Backpack order: {cancel_result.error_message}")
                    except Exception as e:
                        self.logger.error(f"❌ Error canceling Backpack order: {e}")
            elif self.backpack_order_status == 'FILLED':
                break
            else:
                if self.backpack_order_status is not None:
                    self.logger.error(f"❌ Unknown Backpack order status: {self.backpack_order_status}")
                    break
                else:
                    self.logger.error("❌ max tried reached Backpack order, backpack order status is none")
                    break

    def handle_backpack_order_book_update(self, message):
        """Handle Backpack order book updates from WebSocket."""
        try:
            if isinstance(message, str):
                message = json.loads(message)

            self.logger.debug(f"Received Backpack depth message: {message}")

            # Check if this is a depth update message
            if message.get("stream") and "depth" in message.get("stream", ""):
                data = message.get("data", {})

                if data:
                    # Update bids - format is [["price", "size"], ...]
                    # Backpack API uses 'b' for bids
                    bids = data.get('b', [])
                    for bid in bids:
                        price = Decimal(bid[0])
                        size = Decimal(bid[1])
                        if size > 0:
                            self.backpack_order_book['bids'][price] = size
                        else:
                            # Remove zero size orders
                            self.backpack_order_book['bids'].pop(price, None)

                    # Update asks - format is [["price", "size"], ...]
                    # Backpack API uses 'a' for asks
                    asks = data.get('a', [])
                    for ask in asks:
                        price = Decimal(ask[0])
                        size = Decimal(ask[1])
                        if size > 0:
                            self.backpack_order_book['asks'][price] = size
                        else:
                            # Remove zero size orders
                            self.backpack_order_book['asks'].pop(price, None)

                    # Update best bid and ask
                    if self.backpack_order_book['bids']:
                        self.backpack_best_bid = max(self.backpack_order_book['bids'].keys())
                        self.best_order_time = time.time()
                    if self.backpack_order_book['asks']:
                        self.backpack_best_ask = min(self.backpack_order_book['asks'].keys())
                        self.best_order_time = time.time()

                    if not self.backpack_order_book_ready:
                        self.backpack_order_book_ready = True
                        self.logger.info(f"📊 Backpack order book ready - Best bid: {self.backpack_best_bid}, "
                                         f"Best ask: {self.backpack_best_ask}")
                    else:
                        self.logger.debug(f"📊 Order book updated - Best bid: {self.backpack_best_bid}, "
                                          f"Best ask: {self.backpack_best_ask}")

        except Exception as e:
            self.logger.error(f"Error handling Backpack order book update: {e}")
            self.logger.error(f"Message content: {message}")

    def handle_backpack_order_update(self, order_data):
        """Handle Backpack order updates from WebSocket."""
        side = order_data.get('side', '').lower()
        filled_size = Decimal(order_data.get('filled_size', '0'))
        price = Decimal(order_data.get('price', '0'))

        if side == 'buy':
            lighter_side = 'sell'
        else:
            lighter_side = 'buy'

        # Store order details for immediate execution
        self.current_lighter_side = lighter_side
        self.current_lighter_quantity = filled_size
        self.current_lighter_price = price

        self.lighter_order_info = {
            'lighter_side': lighter_side,
            'quantity': filled_size,
            'price': price
        }

        self.waiting_for_lighter_fill = True


    

    async def setup_backpack_websocket(self):
        """Setup Backpack websocket for order updates and order book data."""
        if not self.backpack_client:
            raise Exception("Backpack client not initialized")

        def order_update_handler(order_data):
            """Handle order updates from Backpack WebSocket."""
            if order_data.get('contract_id') != self.backpack_contract_id:
                return
            try:
                order_id = order_data.get('order_id')
                status = order_data.get('status')
                side = order_data.get('side', '').lower()
                filled_size = Decimal(order_data.get('filled_size', '0'))
                size = Decimal(order_data.get('size', '0'))
                price = order_data.get('price', '0')

                if side == 'buy':
                    order_type = "OPEN"
                else:
                    order_type = "CLOSE"
                
                if status == 'CANCELED' and filled_size > 0:
                    status = 'FILLED'

                # Handle the order update
                if status == 'FILLED' and self.backpack_order_status != 'FILLED':
                    if side == 'buy':
                        self.backpack_position += filled_size
                    else:
                        self.backpack_position -= filled_size
                    self.logger.info(f"[{order_id}] [{order_type}] [Backpack] [{status}]: {filled_size} @ {price}")
                    self.backpack_order_status = status

                    # Log Backpack trade to CSV
                    # self.log_trade_to_csv(
                    #     exchange='Backpack',
                    #     side=side,
                    #     price=str(price),
                    #     quantity=str(filled_size)
                    # )

                    self.handle_backpack_order_update({
                        'order_id': order_id,
                        'side': side,
                        'status': status,
                        'size': size,
                        'price': price,
                        'contract_id': self.backpack_contract_id,
                        'filled_size': filled_size
                    })
                elif self.backpack_order_status != 'FILLED':
                    if status == 'OPEN':
                        self.logger.info(f"[{order_id}] [{order_type}] [Backpack] [{status}]: {size} @ {price}")
                    else:
                        self.logger.info(f"[{order_id}] [{order_type}] [Backpack] [{status}]: {filled_size} @ {price}")
                    self.backpack_order_status = status

            except Exception as e:
                self.logger.error(f"Error handling Backpack order update: {e}")

        try:
            # Setup order update handler
            self.backpack_client.setup_order_update_handler(order_update_handler)
            self.logger.info("✅ Backpack WebSocket order update handler set up")

            # Connect to Backpack WebSocket
            await self.backpack_client.connect()
            self.logger.info("✅ Backpack WebSocket connection established")

            # Setup separate WebSocket connection for depth updates
            await self.setup_backpack_depth_websocket()

        except Exception as e:
            self.logger.error(f"Could not setup Backpack WebSocket handlers: {e}")

    async def setup_backpack_depth_websocket(self):
        """Setup separate WebSocket connection for Backpack depth updates."""
        try:
            import websockets

            async def handle_depth_websocket():
                """Handle depth WebSocket connection."""
                url = "wss://ws.backpack.exchange"

                while not self.stop_flag:
                    try:
                        async with websockets.connect(url) as ws:
                            # Subscribe to depth updates
                            subscribe_message = {
                                "method": "SUBSCRIBE",
                                "params": [f"depth.{self.backpack_contract_id}"]
                            }
                            await ws.send(json.dumps(subscribe_message))
                            self.logger.info(f"✅ Subscribed to depth updates for {self.backpack_contract_id}")

                            # Listen for messages
                            async for message in ws:
                                if self.stop_flag:
                                    break

                                try:
                                    # Handle ping frames
                                    if isinstance(message, bytes) and message == b'\x09':
                                        await ws.pong()
                                        continue

                                    data = json.loads(message)

                                    # Handle depth updates
                                    if data.get('stream') and 'depth' in data.get('stream', ''):
                                        self.handle_backpack_order_book_update(data)

                                except json.JSONDecodeError as e:
                                    self.logger.warning(f"Failed to parse depth WebSocket message: {e}")
                                except Exception as e:
                                    self.logger.error(f"Error handling depth WebSocket message: {e}")

                    except websockets.exceptions.ConnectionClosed:
                        self.logger.warning("Depth WebSocket connection closed, reconnecting...")
                    except Exception as e:
                        self.logger.error(f"Depth WebSocket error: {e}")

                    # Wait before reconnecting
                    if not self.stop_flag:
                        await asyncio.sleep(2)

            # Start depth WebSocket in background
            asyncio.create_task(handle_depth_websocket())
            self.logger.info("✅ Backpack depth WebSocket task started")

        except Exception as e:
            self.logger.error(f"Could not setup Backpack depth WebSocket: {e}")

    # def is_beijing_midnight_to_8am(self):
    #     """
    #     判断当前北京时间是否在凌晨 0:00 到 8:00（不包含 8:00）
    #     返回 True 表示在该时间段内，否则返回 False
    #     """
    #     # 获取北京时区
    #     beijing_tz = pytz.timezone('Asia/Shanghai')
    #     # 当前北京时间
    #     now_beijing = datetime.now(beijing_tz)
    #     # 当前小时（0-23）
    #     hour = now_beijing.hour
        
    #     # 判断是否在 0 点到 8 点之间（不包含 8 点）
    #     return 0 <= hour < 8
    
    def get_gold_activity_level(self) -> int:
        """
        返回当前黄金市场的活跃程度级别（北京时间）
        
        返回值（数值越小越活跃）:
            1 - 最活跃（欧美重叠）
            2 - 中等活跃（欧洲或美国时段）
            3 - 不活跃（亚洲时段）
            4 - 休市（周末，最不活跃）
        """
        # 获取北京时间
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now = datetime.datetime.now(beijing_tz)  # 使用 datetime.datetime
        
        # 周六、周日返回最不活跃
        if now.weekday() >= 5:  # 5=周六, 6=周日
            return 4
        
        # 当前时间（使用 datetime.time() 获取当前时间对象）
        current_time = now.time()
        
        # 定义各时段（北京时间）
        if datetime.time(20, 0) <= current_time < datetime.time(23, 59):  # 19:00 - 24:00
            return 1  # 最活跃（欧美重叠）
        
        elif datetime.time(17, 0) <= current_time < datetime.time(20, 0):  # 15:00 - 19:00
            return 2  # 中等活跃（欧洲时段）
        
        elif datetime.time(0, 0) <= current_time < datetime.time(7, 0):  # 00:00 - 07:00
            return 3  # 不活跃（美国时段延续）
        
        elif datetime.time(7, 0) <= current_time < datetime.time(12, 0):
            return 1
        
        else:  # 12:00 - 17:00（亚洲时段）
            return 1  # 不活跃

    async def trading_loop(self):
        """Main trading loop implementing the new strategy."""
        self.logger.info(f"🚀 Starting hedge bot for {self.ticker}")
        # if  self.is_beijing_midnight_to_8am():
        #     self.order_quantity = Decimal('0.3')
        # else:
        #     self.order_quantity = Decimal('0.3') 

        # Initialize clients
        try:
            
            self.initialize_backpack_client()

            # Get contract info
            self.backpack_contract_id, self.backpack_tick_size = await self.get_backpack_contract_info()
           

            self.logger.info(f"Contract info loaded - Backpack: {self.backpack_contract_id}")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            return

        # Setup Backpack websocket
        try:
            await self.setup_backpack_websocket()
            self.logger.info("✅ Backpack WebSocket connection established")

            # Wait for initial order book data with timeout
            self.logger.info("⏳ Waiting for initial order book data...")
            timeout = 10  # seconds
            start_time = time.time()
            while not self.backpack_order_book_ready and not self.stop_flag:
                if time.time() - start_time > timeout:
                    self.logger.warning(f"⚠️ Timeout waiting for WebSocket order book data after {timeout}s")
                    break
                await asyncio.sleep(0.5)

            if self.backpack_order_book_ready:
                self.logger.info("✅ WebSocket order book data received")
            else:
                self.logger.warning("⚠️ WebSocket order book not ready, will use REST API fallback")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Backpack websocket: {e}")
            return

       

        await asyncio.sleep(5)

        iterations = 0
        while iterations < self.iterations and not self.stop_flag:
            try:
                iterations += 1
                self.logger.info("-----------------------------------------------")
                self.logger.info(f"🔄 Trading loop iteration {iterations}")
                self.logger.info("-----------------------------------------------")

                bp_position = await self.get_account_positions()
                self.logger.info(f"[STEP 1] Backpack position:  {bp_position}")

                
                if bp_position > 0:
                    side = 'sell'
                else:
                    side = 'buy'

                order_quantity = self.get_gold_activity_level() * self.order_quantity 

                
            
                try:
                    # Determine side based on some logic (for now, alternate)          
                    await self.place_backpack_post_only_order(side, order_quantity)
                except Exception as e:
                    self.logger.error(f"⚠️ Error in trading loop: {e}")
                    self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")

                # 进行订单重置
                if iterations % 5 == 0:
                    await self.backpack_client.cancel_all_active_orders()      
      
                await self.position_safety_guard()        

            except Exception as e:
                self.logger.error(f"⚠️ Error in trading loop: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                await self.position_safety_guard()
                

    async def position_safety_guard(self):
        # self.logger.info("----------守护任务 -- query account and cancel all active orders")        
        position = await self.backpack_client.get_account_positions()
        if abs(position) > self.max_exposure:
            await self.backpack_client.cancel_all_active_orders()  
            side = 'sell' if position > 0 else 'buy'
            await self.backpack_client.place_market_order(self.backpack_contract_id, self.order_quantity,side)
           
           

    async def run(self):
        """Run the hedge bot."""
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        finally:
            self.logger.info("🔄 Cleaning up...")
            self.shutdown()

def main():
    backpack_maker = BackPack_Maker(
        ticker='PAXG',
        order_quantity=Decimal('0.1'),
        fill_timeout=5,
        iterations=200000
    )

    asyncio.run(backpack_maker.run())

main()



def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Trading bot for Backpack and Lighter')
    parser.add_argument('--exchange', type=str,
                        help='Exchange')
    parser.add_argument('--ticker', type=str, default='BTC',
                        help='Ticker symbol (default: BTC)')
    parser.add_argument('--size', type=str,
                        help='Number of tokens to buy/sell per order')
    parser.add_argument('--iter', type=int,
                        help='Number of iterations to run')
    parser.add_argument('--fill-timeout', type=int, default=5,
                        help='Timeout in seconds for maker order fills (default: 5)')

    return parser.parse_args()
