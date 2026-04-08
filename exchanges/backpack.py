"""
Backpack exchange client implementation.
"""

import os
import asyncio
import json
import time
import base64
import sys
import logging
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import pytz
from cryptography.hazmat.primitives.asymmetric import ed25519
import websockets
from bpx.public import Public
from .bp_client import Account
from bpx.constants.enums import OrderTypeEnum, TimeInForceEnum

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry

# 定义 UTC+8 时区
TZ_UTC8 = pytz.timezone('Asia/Shanghai')

logger = logging.getLogger(__name__)


def format_timestamp_to_seconds(dt: datetime) -> str:
    """格式化时间戳到秒级别（去掉毫秒和时区）"""
    return dt.replace(microsecond=0, tzinfo=None).isoformat()


class BackpackWebSocketManager:
    """WebSocket manager for Backpack order updates."""

    def __init__(self, public_key: str, secret_key: str, symbol: str, order_update_callback):
        self.public_key = public_key
        self.secret_key = secret_key
        self.symbol = symbol
        self.order_update_callback = order_update_callback
        self.websocket = None
        self.running = False
        self.ws_url = "wss://ws.backpack.exchange"
        self.connected = False
        self.running = False
        self.stop_flag = False

        # Backpack订单簿数据
        self.backpack_order_book = {"bids": {}, "asks": {}}
        self.backpack_best_bid = None
        self.backpack_best_bid_size = None
        self.backpack_best_ask = None
        self.backpack_best_ask_size = None
        self.backpack_order_book_ready = False
        self.backpack_contract_id = None  # 将在setup时设置
        self.best_order_time = 0  # Initialize to 0 instead of None
        self.price_timestamp = None  # 价格时间戳

        # Initialize ED25519 private key from base64 decoded secret
        self.private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            base64.b64decode(secret_key)
        )

    def _generate_signature(self, instruction: str, timestamp: int, window: int = 5000) -> str:
        """Generate ED25519 signature for WebSocket authentication."""
        # Create the message string in the same format as BPX package
        message = f"instruction={instruction}&timestamp={timestamp}&window={window}"

        # Sign the message using ED25519 private key
        signature_bytes = self.private_key.sign(message.encode())

        # Return base64 encoded signature
        return base64.b64encode(signature_bytes).decode()

    async def connect(self):
        """Connect to Backpack WebSocket."""
        # while True:
        #     try:
        #         self.logger.log("Connecting to Backpack WebSocket", "INFO")
        #         self.websocket = await websockets.connect(self.ws_url)
        #         self.running = True

        #         # Subscribe to order updates for the specific symbol
        #         timestamp = int(time.time() * 1000)
        #         signature = self._generate_signature("subscribe", timestamp)

        #         subscribe_message = {
        #             "method": "SUBSCRIBE",
        #             "params": [f"account.orderUpdate.{self.symbol}"],
        #             "signature": [
        #                 self.public_key,
        #                 signature,
        #                 str(timestamp),
        #                 "5000"
        #             ]
        #         }

        #         await self.websocket.send(json.dumps(subscribe_message))
        #         if self.logger:
        #             self.logger.log(f"Subscribed to order updates for {self.symbol}", "INFO")

        #         # Start listening for messages
        #         await self._listen()

        #     except Exception as e:
        #         if self.logger:
        #             self.logger.log(f"WebSocket connection error: {e}", "ERROR")

        
        while True:
            # 如果已经连接成功且正在运行，就什么都不做，等待自然断开
            if self.connected and self.running:
                await asyncio.sleep(1)  # 轻轻睡一下，降低 CPU 占用
                continue

            # 如果未连接或连接已断，才尝试（重）连接
            try:
                logger.info("Attempting to (re)connect to Backpack order update WebSocket...")

                # 建立连接
                self.websocket = await asyncio.wait_for(
                    websockets.connect(self.ws_url), timeout=10
                )

                # 认证并订阅
                timestamp = int(time.time() * 1000)
                signature = self._generate_signature("subscribe", timestamp)
                subscribe_message = {
                    "method": "SUBSCRIBE",
                    "params": [f"account.orderUpdate.{self.symbol}"],
                    "signature": [
                        self.public_key,
                        signature,
                        str(timestamp),
                        "5000"
                    ]
                }

                await self.websocket.send(json.dumps(subscribe_message))
                logger.info("Subscribed to order updates successfully")

                # 标记为已连接
                self.connected = True
                self.running = True

                # 开始监听（异常向上抛）
                await self._listen()

                # 如果这里执行到，说明 _listen 退出了（连接断了或异常）
                logger.warning("Listener exited, will reconnect soon...")

            except (websockets.exceptions.ConnectionClosed,
                    asyncio.TimeoutError,
                    OSError) as e:
                logger.warning(f"Connection lost or failed: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during connect/listen: {e}")
            finally:
                # 无论如何，断开后都要清理状态
                self.connected = False
                self.running = False
                if self.websocket:
                    try:
                        await self.websocket.close()
                    except:
                        pass
                    self.websocket = None

                # 可选：加上指数退避
                await asyncio.sleep(2)  # 或使用前面说的 exponential backoff

        logger.info("WebSocket manager stopped gracefully")

    async def _listen(self):
        """Listen for WebSocket messages."""
        try:
            async for message in self.websocket:
                if not self.running:
                    break

                try:
                    data = json.loads(message)
                    await self._handle_message(data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse WebSocket message: {e}")
                    raise e
                except Exception as e:
                    logger.error(f"Error handling WebSocket message: {e}")
                    raise e

        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
            raise
        except Exception as e:
            logger.error(f"WebSocket listen error: {e}")
            raise

    async def setup_backpack_depth_websocket(self):
        """Setup separate WebSocket connection for Backpack depth updates."""
        if not self.backpack_contract_id:
            logger.warning("Cannot setup depth WebSocket: contract_id not set")
            return
            
        try:
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
                            logger.info(f"✅ Subscribed to depth updates for {self.backpack_contract_id}")

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
                                    logger.warning(f"Failed to parse depth WebSocket message: {e}")
                                except Exception as e:
                                    logger.error(f"Error handling depth WebSocket message: {e}")

                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("Depth WebSocket connection closed, reconnecting...")
                    except Exception as e:
                        logger.error(f"Depth WebSocket error: {e}")

                    # Wait before reconnecting
                    if not self.stop_flag:
                        await asyncio.sleep(2)

            # Start depth WebSocket in background
            asyncio.create_task(handle_depth_websocket())
            logger.info("✅ Backpack depth WebSocket task started")

        except Exception as e:
            logger.error(f"Could not setup Backpack depth WebSocket: {e}")

    def handle_backpack_order_book_update(self, message):
        """Handle Backpack order book updates from WebSocket."""
        try:
            if isinstance(message, str):
                message = json.loads(message)

            # Check if this is a depth update message
            if not (message.get("stream") and "depth" in message.get("stream", "")):
                return
                
            data = message.get("data", {})
            if not data:
                return

            # Update bids - format is [["price", "size"], ...]
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
            updated = False
            if self.backpack_order_book['bids']:
                self.backpack_best_bid = max(self.backpack_order_book['bids'].keys())
                self.backpack_best_bid_size = self.backpack_order_book['bids'][self.backpack_best_bid]
                updated = True
            if self.backpack_order_book['asks']:
                self.backpack_best_ask = min(self.backpack_order_book['asks'].keys())
                self.backpack_best_ask_size = self.backpack_order_book['asks'][self.backpack_best_ask]
                updated = True
            
            # Update timestamp only once when order book is updated
            if updated:
                self.best_order_time = time.time()
                self.price_timestamp = format_timestamp_to_seconds(datetime.now(TZ_UTC8))

                if not self.backpack_order_book_ready:
                    self.backpack_order_book_ready = True
                    logger.info(f"📊 Backpack order book ready - Best bid: {self.backpack_best_bid}, "
                                     f"Best ask: {self.backpack_best_ask}")

        except Exception as e:
            logger.error(f"Error handling Backpack order book update: {e}", exc_info=True)

    async def _handle_message(self, data: Dict[str, Any]):
        """Handle incoming WebSocket messages."""
        try:
            stream = data.get('stream', '')
            payload = data.get('data', {})

            if 'orderUpdate' in stream:
                await self._handle_order_update(payload)
            else:
                logger.error(f"Unknown WebSocket message: {data}")

        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")

    async def _handle_order_update(self, order_data: Dict[str, Any]):
        """Handle order update messages."""
        try:
            # Call the order update callback if it exists
            if hasattr(self, 'order_update_callback') and self.order_update_callback:
                await self.order_update_callback(order_data)
        except Exception as e:
            logger.error(f"Error handling order update: {e}")

    async def disconnect(self):
        """Disconnect from WebSocket."""
        self.running = False
        self.stop_flag = True
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket disconnected")

    def set_order_filled_event(self, event):
        """Set the order filled event for synchronization."""
        self.order_filled_event = event


class BackpackClient(BaseExchangeClient):
    """Backpack exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Backpack client."""
        super().__init__(config)

        # Backpack credentials from environment
        self.public_key = os.getenv('BACKPACK_PUBLIC_KEY')
        self.secret_key = os.getenv('BACKPACK_SECRET_KEY')

        if not self.public_key or not self.secret_key:
            raise ValueError("BACKPACK_PUBLIC_KEY and BACKPACK_SECRET_KEY must be set in environment variables")

        # Initialize Backpack clients using official SDK
        self.public_client = Public()
        self.account_client = Account(
            public_key=self.public_key,
            secret_key=self.secret_key
        )

        self._order_update_handler = None


    def _validate_config(self) -> None:
        """Validate Backpack configuration."""
        required_env_vars = ['BACKPACK_PUBLIC_KEY', 'BACKPACK_SECRET_KEY']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def connect(self) -> None:
        """Connect to Backpack WebSocket."""
        # Get contract_id first if not set
        if not self.config.contract_id:
            self.config.contract_id, self.config.tick_size = await self.get_contract_attributes()
        
        # Initialize WebSocket manager
        self.ws_manager = BackpackWebSocketManager(
            public_key=self.public_key,
            secret_key=self.secret_key,
            symbol=self.config.contract_id,  # Use contract_id as symbol for Backpack
            order_update_callback=self._handle_websocket_order_update
        )
        # Pass config to WebSocket manager for order type determination
        self.ws_manager.config = self.config
        # Set contract_id for depth WebSocket
        self.ws_manager.backpack_contract_id = self.config.contract_id

        try:
            # Start WebSocket connection in background task
            asyncio.create_task(self.ws_manager.connect())
            # Setup depth WebSocket for order book updates
            await self.ws_manager.setup_backpack_depth_websocket()
            # Wait a moment for connection to establish
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Error connecting to Backpack WebSocket: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Backpack."""
        try:
            if hasattr(self, 'ws_manager') and self.ws_manager:
                self.ws_manager.stop_flag = True
                await self.ws_manager.disconnect()
        except Exception as e:
            logger.error(f"Error during Backpack disconnect: {e}")

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "backpack"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler

    async def _handle_websocket_order_update(self, order_data: Dict[str, Any]):
        """Handle order updates from WebSocket."""
        try:
            event_type = order_data.get('e', '')
            order_id = order_data.get('i', '')
            symbol = order_data.get('s', '')
            side = order_data.get('S', '')
            quantity = order_data.get('q', '0')
            price = order_data.get('p', '0')
            fill_quantity = order_data.get('z', '0')

            # Only process orders for our symbol
            if symbol != self.config.contract_id:
                return

            # Determine order side
            if side.upper() == 'BID':
                order_side = 'buy'
            elif side.upper() == 'ASK':
                order_side = 'sell'
            else:
                logger.error(f"Unexpected order side: {side}")
                sys.exit(1)

            # Check if this is a close order (opposite side from bot direction)
            is_close_order = (order_side == self.config.close_order_side)
            order_type = "CLOSE" if is_close_order else "OPEN"

            if event_type == 'orderFill' and quantity == fill_quantity:
                if self._order_update_handler:
                    self._order_update_handler({
                        'order_id': order_id,
                        'side': order_side,
                        'order_type': order_type,
                        'status': 'FILLED',
                        'size': quantity,
                        'price': price,
                        'contract_id': symbol,
                        'filled_size': fill_quantity
                    })

            elif event_type in ['orderFill', 'orderAccepted', 'orderCancelled', 'orderExpired']:
                if event_type == 'orderFill':
                    status = 'PARTIALLY_FILLED'
                elif event_type == 'orderAccepted':
                    status = 'OPEN'
                elif event_type in ['orderCancelled', 'orderExpired']:
                    status = 'CANCELED'

                if self._order_update_handler:
                    self._order_update_handler({
                        'order_id': order_id,
                        'side': order_side,
                        'order_type': order_type,
                        'status': status,
                        'size': quantity,
                        'price': price,
                        'contract_id': symbol,
                        'filled_size': fill_quantity
                    })

        except Exception as e:
            logger.error(f"Error handling WebSocket order update: {e}")

    async def get_order_price(self, direction: str) -> Decimal:
        """Get the price of an order with Backpack using official SDK."""
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0:
            logger.error("Invalid bid/ask prices")
            raise ValueError("Invalid bid/ask prices")

        if direction == 'buy':
            # For buy orders, place slightly below best ask to ensure execution
            order_price = best_ask - self.config.tick_size
        else:
            # For sell orders, place slightly above best bid to ensure execution
            order_price = best_bid + self.config.tick_size
        return self.round_to_tick(order_price)

    @query_retry(default_return=(0, 0))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Fetch best bid/ask prices, preferring WebSocket data if available and recent."""
        # Try to use WebSocket data if available and recent (within 5 seconds)
        if (hasattr(self, 'ws_manager') and self.ws_manager and 
            self.ws_manager.backpack_order_book_ready and
            self.ws_manager.backpack_best_bid and self.ws_manager.backpack_best_ask):
            
            # Check if WebSocket data is recent
            if self.ws_manager.best_order_time:
                time_gap = time.time() - self.ws_manager.best_order_time
                if time_gap < 5:  # Use WebSocket data if less than 5 seconds old
                    best_bid = self.ws_manager.backpack_best_bid
                    best_ask = self.ws_manager.backpack_best_ask
                    if best_bid > 0 and best_ask > 0 and best_bid < best_ask:
                        return best_bid, best_ask
        
        # Fallback to REST API
        order_book = self.public_client.get_depth(contract_id)

        # Extract bids and asks directly from Backpack response
        bids = order_book.get('bids', [])
        asks = order_book.get('asks', [])

        # Sort bids and asks
        bids = sorted(bids, key=lambda x: Decimal(x[0]), reverse=True)  # (highest price first)
        asks = sorted(asks, key=lambda x: Decimal(x[0]))                # (lowest price first)

        # Best bid is the highest price someone is willing to buy at
        best_bid = Decimal(bids[0][0]) if bids and len(bids) > 0 else 0
        # Best ask is the lowest price someone is willing to sell at
        best_ask = Decimal(asks[0][0]) if asks and len(asks) > 0 else 0

        return best_bid, best_ask
    
    async def place_post_only_order(self, contract_id: str, quantity: Decimal, direction: str, best_bid, best_ask) -> OrderResult:
        """Place an open order with Backpack using official SDK with retry logic for POST_ONLY rejections."""
       

        

        if direction == 'buy':
            # For buy orders, place slightly below best ask to ensure execution            
            side = 'Bid'
            order_price = best_bid
        else:
            # For sell orders, place slightly above best bid to ensure execution          
            side = 'Ask'
            order_price = best_ask

        # Place the order using Backpack SDK (post-only to ensure maker order)
        order_result = self.account_client.execute_order(
            symbol=contract_id,
            side=side,
            order_type=OrderTypeEnum.LIMIT,
            quantity=str(quantity),
            price=str(self.round_to_tick(order_price)),
            post_only=True,
            time_in_force=TimeInForceEnum.GTC
        )

        if not order_result:
            return OrderResult(success=False, error_message='Failed to place order')

        if 'code' in order_result:
            message = order_result.get('message', 'Unknown error')
            logger.warning(f"[OPEN] Order rejected: {message}")
            return OrderResult(success=False, error_message=message)

        # Extract order ID from response
        order_id = order_result.get('id')
        if not order_id:
            logger.error(f"[OPEN] No order ID in response: {order_result}")
            return OrderResult(success=False, error_message='No order ID in response')

        # Order successfully placed
        return OrderResult(
            success=True,
            order_id=order_id,
            side=side.lower(),
            size=quantity,
            price=order_price,
            status='New'
        )


    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Backpack using official SDK with retry logic for POST_ONLY rejections."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            retry_count += 1

            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

            if best_bid <= 0 or best_ask <= 0:
                return OrderResult(success=False, error_message='Invalid bid/ask prices')

            if direction == 'buy':
                # For buy orders, place slightly below best ask to ensure execution
                order_price = best_ask - self.config.tick_size
                side = 'Bid'
            else:
                # For sell orders, place slightly above best bid to ensure execution
                order_price = best_bid + self.config.tick_size
                side = 'Ask'

            # Place the order using Backpack SDK (post-only to ensure maker order)
            order_result = self.account_client.execute_order(
                symbol=contract_id,
                side=side,
                order_type=OrderTypeEnum.LIMIT,
                quantity=str(quantity),
                price=str(self.round_to_tick(order_price)),
                post_only=True,
                time_in_force=TimeInForceEnum.GTC
            )

            if not order_result:
                return OrderResult(success=False, error_message='Failed to place order')

            if 'code' in order_result:
                message = order_result.get('message', 'Unknown error')
                logger.warning(f"[OPEN] Order rejected: {message}")
                continue

            # Extract order ID from response
            order_id = order_result.get('id')
            if not order_id:
                logger.error(f"[OPEN] No order ID in response: {order_result}")
                return OrderResult(success=False, error_message='No order ID in response')

            # Order successfully placed
            return OrderResult(
                success=True,
                order_id=order_id,
                side=side.lower(),
                size=quantity,
                price=order_price,
                status='New'
            )

        return OrderResult(success=False, error_message='Max retries exceeded')

    async def place_market_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place a market order with Backpack."""
        # Validate direction
        if direction == 'buy':
            side = 'Bid'
        elif direction == 'sell':
            side = 'Ask'
        else:
            raise Exception(f"[OPEN] Invalid direction: {direction}")

        result = self.account_client.execute_order(
            symbol=contract_id,
            side=side,
            order_type=OrderTypeEnum.MARKET,
            quantity=str(quantity)
        )

        order_id = result.get('id')
        order_status = result.get('status')

      

        if order_status is None or order_status != 'FILLED':
            logger.error(f"Market order failed with status: {result}")
            
        # For market orders, we expect them to be filled immediately
        else:
            price = Decimal(result.get('executedQuoteQuantity', '0'))/Decimal(result.get('executedQuantity'))
            return OrderResult(
                success=True,
                order_id=order_id,
                side=direction.lower(),
                size=quantity,
                price=price,
                status='FILLED'
            )

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Backpack using official SDK with retry logic for POST_ONLY rejections."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            retry_count += 1
            # Get current market prices to adjust order price if needed
            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

            if best_bid <= 0 or best_ask <= 0:
                return OrderResult(success=False, error_message='No bid/ask data available')

            # Adjust order price based on market conditions and side
            adjusted_price = price
            if side.lower() == 'sell':
                order_side = 'Ask'
                # For sell orders, ensure price is above best bid to be a maker order
                if price <= best_bid:
                    adjusted_price = best_bid + self.config.tick_size
            elif side.lower() == 'buy':
                order_side = 'Bid'
                # For buy orders, ensure price is below best ask to be a maker order
                if price >= best_ask:
                    adjusted_price = best_ask - self.config.tick_size

            adjusted_price = self.round_to_tick(adjusted_price)
            # Place the order using Backpack SDK (post-only to avoid taker fees)
            order_result = self.account_client.execute_order(
                symbol=contract_id,
                side=order_side,
                order_type=OrderTypeEnum.LIMIT,
                quantity=str(quantity),
                price=str(adjusted_price),
                post_only=True,
                time_in_force=TimeInForceEnum.GTC
            )

            if not order_result:
                return OrderResult(success=False, error_message='Failed to place order')

            if 'code' in order_result:
                message = order_result.get('message', 'Unknown error')
                logger.error(f"[CLOSE] Error placing order: {message}")
                await asyncio.sleep(1)
                continue

            # Extract order ID from response
            order_id = order_result.get('id')
            if not order_id:
                logger.error(f"[CLOSE] No order ID in response: {order_result}")
                return OrderResult(success=False, error_message='No order ID in response')

            # Order successfully placed
            return OrderResult(
                success=True,
                order_id=order_id,
                side=side.lower(),
                size=quantity,
                price=adjusted_price,
                status='New'
            )

        return OrderResult(success=False, error_message='Max retries exceeded for close order')

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Backpack using official SDK."""
        try:
            # Cancel the order using Backpack SDK
            cancel_result = self.account_client.cancel_order(
                symbol=self.config.contract_id,
                order_id=order_id
            )

            if not cancel_result:
                return OrderResult(success=False, error_message='Failed to cancel order')
            if 'code' in cancel_result:
                logger.error(
                    f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.get('message', 'Unknown error')}")
                filled_size = self.config.quantity
            else:
                filled_size = Decimal(cancel_result.get('executedQuantity', 0))
            return OrderResult(success=True, filled_size=filled_size)

        except Exception as e:
            return OrderResult(success=False, error_message=str(e))

    @query_retry()
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information from Backpack using official SDK."""
        # Get order information using Backpack SDK
        order_result = self.account_client.get_open_order(
            symbol=self.config.contract_id,
            order_id=order_id
        )

        if not order_result:
            return None

        # Return the order data as OrderInfo
        return OrderInfo(
            order_id=order_result.get('id', ''),
            side=order_result.get('side', '').lower(),
            size=Decimal(order_result.get('quantity', 0)),
            price=Decimal(order_result.get('price', 0)),
            status=order_result.get('status', ''),
            filled_size=Decimal(order_result.get('executedQuantity', 0)),
            remaining_size=Decimal(order_result.get('quantity', 0)) - Decimal(order_result.get('executedQuantity', 0))
        )
    
    async def cancel_all_active_orders(self) -> None:
       order_list: List[OrderInfo]  = await self.get_active_orders(self.config.contract_id)
       for order in order_list:
           await self.cancel_order(order.order_id)  
    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract using official SDK."""
        # Get active orders using Backpack SDK
        active_orders = self.account_client.get_open_orders(symbol=contract_id)

        if not active_orders:
            return []

        # Return the orders list as OrderInfo objects
        order_list = active_orders if isinstance(active_orders, list) else active_orders.get('orders', [])
        orders = []

        for order in order_list:
            if isinstance(order, dict):
                if order.get('side', '') == 'Bid':
                    side = 'buy'
                elif order.get('side', '') == 'Ask':
                    side = 'sell'
                orders.append(OrderInfo(
                    order_id=order.get('id', ''),
                    side=side,
                    size=Decimal(order.get('quantity', 0)),
                    price=Decimal(order.get('price', 0)),
                    status=order.get('status', ''),
                    filled_size=Decimal(order.get('executedQuantity', 0)),
                    remaining_size=Decimal(order.get('quantity', 0)) - Decimal(order.get('executedQuantity', 0))
                ))

        return orders

    @query_retry(default_return=0)
    async def get_account_positions(self) -> Decimal:
        """Get account positions using official SDK."""
        positions_data = self.account_client.get_open_positions()
        position_amt = 0
        for position in positions_data:
            if position.get('symbol', '') == self.config.contract_id:
                position_amt = Decimal(position.get('netQuantity', 0))
                break
        return position_amt

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Get contract ID for a ticker."""
        ticker = self.config.ticker
        if len(ticker) == 0:
            logger.error("Ticker is empty")
            raise ValueError("Ticker is empty")

        markets = self.public_client.get_markets()
        for market in markets:
            if (market.get('marketType', '') == 'PERP' and market.get('baseSymbol', '') == ticker and
                    market.get('quoteSymbol', '') == 'USDC'):
                self.config.contract_id = market.get('symbol', '')
                min_quantity = Decimal(market.get('filters', {}).get('quantity', {}).get('minQuantity', 0))
                self.config.tick_size = Decimal(market.get('filters', {}).get('price', {}).get('tickSize', 0))
                break

        if self.config.contract_id == '':
            logger.error("Failed to get contract ID for ticker")
            raise ValueError("Failed to get contract ID for ticker")

        # if self.config.quantity < min_quantity:
        #     logger.error(f"Order quantity is less than min quantity: {self.config.quantity} < {min_quantity}")
        #     raise ValueError(f"Order quantity is less than min quantity: {self.config.quantity} < {min_quantity}")

        if self.config.tick_size == 0:
            logger.error("Failed to get tick size for ticker")
            raise ValueError("Failed to get tick size for ticker")

        return self.config.contract_id, self.config.tick_size


def get_backpack_bbo_from_websocket(backpack_client: BackpackClient) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[str]]:
    """从websocket数据获取Backpack的best bid/ask和对应的size"""
    try:
        # Backpack通过WebSocket获取orderbook数据
        if (hasattr(backpack_client, 'ws_manager') and 
            backpack_client.ws_manager and
            backpack_client.ws_manager.backpack_best_bid and 
            backpack_client.ws_manager.backpack_best_ask):
            
            best_bid = backpack_client.ws_manager.backpack_best_bid
            best_ask = backpack_client.ws_manager.backpack_best_ask
            best_bid_size = backpack_client.ws_manager.backpack_best_bid_size
            best_ask_size = backpack_client.ws_manager.backpack_best_ask_size
            price_timestamp = backpack_client.ws_manager.price_timestamp
            
            return best_bid, best_bid_size, best_ask, best_ask_size, price_timestamp
        else:
            return None, None, None, None, None
    except Exception as e:
        logger.error(f"Error getting Backpack BBO from websocket: {e}", exc_info=True)
        return None, None, None, None, None


async def get_backpack_bbo_from_rest(backpack_client: BackpackClient, contract_id: str) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[str]]:
    """从REST API获取Backpack的best bid/ask和对应的size"""
    try:
        # Backpack通过REST API获取订单簿数据（直接使用get_depth避免重复调用）
        order_book = backpack_client.public_client.get_depth(contract_id)
        bids = order_book.get('bids', [])
        asks = order_book.get('asks', [])
        
        if not bids or not asks:
            return None, None, None, None, None
        
        # 排序bids和asks（bids按价格降序，asks按价格升序）
        bids = sorted(bids, key=lambda x: Decimal(x[0]), reverse=True)  # 最高价在前
        asks = sorted(asks, key=lambda x: Decimal(x[0]))  # 最低价在前
        
        # Best bid是最高价格（第一个）
        best_bid = Decimal(bids[0][0])
        best_bid_size = Decimal(bids[0][1])
        
        # Best ask是最低价格（第一个）
        best_ask = Decimal(asks[0][0])
        best_ask_size = Decimal(asks[0][1])
        
        if best_bid <= 0 or best_ask <= 0:
            return None, None, None, None, None
        
        # 使用当前时间作为时间戳（REST API通常不返回时间戳）
        price_timestamp = format_timestamp_to_seconds(datetime.now(TZ_UTC8))
        
        return best_bid, best_bid_size, best_ask, best_ask_size, price_timestamp
    except Exception as e:
        logger.error(f"Error getting Backpack BBO from REST API: {e}", exc_info=True)
        return None, None, None, None, None
