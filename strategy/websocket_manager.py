"""WebSocket management for EdgeX and Lighter exchanges."""
import asyncio
import json
import logging
import time
import traceback
import websockets
from typing import Callable, Optional

from edgex_sdk import WebSocketManager


def edgex_public_quote_ws_url(ws_base: str) -> str:
    """Match edgex_sdk WebSocketManager: {base}/api/v1/public/ws (root host alone returns HTTP 404)."""
    b = (ws_base or "").strip().rstrip("/")
    if not b:
        b = "wss://quote.edgex.exchange"
    if b.endswith("/api/v1/public/ws"):
        return b
    return f"{b}/api/v1/public/ws"


class WebSocketManagerWrapper:
    """Manages WebSocket connections for both exchanges."""

    def __init__(self, order_book_manager, logger: logging.Logger):
        """Initialize WebSocket manager."""
        self.order_book_manager = order_book_manager
        self.logger = logger
        self.stop_flag = False

        # EdgeX WebSocket
        self.edgex_ws_manager: Optional[WebSocketManager] = None
        self.edgex_contract_id: Optional[str] = None
        self.edgex_ws_task: Optional[asyncio.Task] = None
        self.edgex_private_connected = False
        self.edgex_last_message_time = None
        self.edgex_heartbeat_timeout = 30  # 30 seconds without message triggers reconnect

        # Lighter WebSocket
        self.lighter_ws_task: Optional[asyncio.Task] = None
        self.lighter_client = None
        self.lighter_market_index: Optional[int] = None
        self.account_index: Optional[int] = None

        # Callbacks
        self.on_lighter_order_filled: Optional[Callable] = None
        self.on_edgex_order_update: Optional[Callable] = None
        self.on_edgex_trade: Optional[Callable] = None
        self.on_edgex_depth: Optional[Callable] = None

    def set_edgex_ws_manager(
        self,
        ws_manager: WebSocketManager,
        contract_id: str,
        public_ws_url: str = "wss://quote.edgex.exchange",
        public_depth_levels: int = 15,
    ):
        """Set EdgeX WebSocket manager and contract ID."""
        self.edgex_ws_manager = ws_manager
        self.edgex_contract_id = contract_id
        self.edgex_public_ws_url = edgex_public_quote_ws_url(public_ws_url)
        self.edgex_public_depth_levels = max(1, min(int(public_depth_levels), 500))
        self.edgex_public_latency = 0.0
        self.edgex_public_last_message_time = 0.0
        self.edgex_public_ws_task = None

    def set_lighter_config(self, client, market_index: int, account_index: int):
        """Set Lighter client and configuration."""
        self.lighter_client = client
        self.lighter_market_index = market_index
        self.account_index = account_index

    def set_callbacks(self, on_lighter_order_filled: Callable = None,
                      on_edgex_order_update: Callable = None,
                      on_edgex_trade: Callable = None,
                      on_edgex_depth: Callable = None):
        """Set callback functions for order updates and public messages."""
        if on_lighter_order_filled: self.on_lighter_order_filled = on_lighter_order_filled
        if on_edgex_order_update: self.on_edgex_order_update = on_edgex_order_update
        if on_edgex_trade: self.on_edgex_trade = on_edgex_trade
        if on_edgex_depth: self.on_edgex_depth = on_edgex_depth

    # EdgeX WebSocket methods
    def _update_edgex_heartbeat(self):
        """Update last message time for heartbeat detection."""
        self.edgex_last_message_time = time.time()

    async def handle_edgex_websocket(self):
        """Handle EdgeX WebSocket connection with auto-reconnect and heartbeat monitoring."""
        if not self.edgex_ws_manager:
            raise Exception("EdgeX WebSocket manager not initialized")

        reconnect_delay = 1.0
        max_reconnect_delay = 60.0
        heartbeat_check_interval = 5  # Check heartbeat every 5 seconds

        def order_update_handler(message):
            """Handle order updates from EdgeX WebSocket."""
            # Update heartbeat on any message
            self._update_edgex_heartbeat()
            
            if isinstance(message, str):
                message = json.loads(message)

            content = message.get("content", {})
            event = content.get("event", "")
            try:
                if event == "ORDER_UPDATE":
                    data = content.get('data', {})
                    orders = data.get('order', [])

                    if orders and len(orders) > 0:
                        # EdgeX returns TWO filled events for the same order; skip the second one
                        # The second one has collateral data, so we filter it out
                        order_status = orders[0].get('status', '')
                        if order_status == "FILLED" and len(data.get('collateral', [])):
                            return  # Skip duplicate FILLED event

                        for order in orders:
                            if order.get('contractId') != self.edgex_contract_id:
                                continue

                            if self.on_edgex_order_update:
                                self.on_edgex_order_update(order)

            except Exception as e:
                self.logger.error(f"Error handling EdgeX order update: {e}")

        while not self.stop_flag:
            try:
                # Setup disconnect/connect callbacks
                def on_private_disconnect(exc):
                    """Handle private WebSocket disconnect."""
                    self.edgex_private_connected = False

                def on_private_connect():
                    """Handle private WebSocket connect."""
                    self.edgex_private_connected = True
                    self._update_edgex_heartbeat()

                # Setup clients and handlers
                try:
                    private_client = self.edgex_ws_manager.get_private_client()
                    
                    if private_client:
                        private_client.on_disconnect(on_private_disconnect)
                        private_client.on_connect(on_private_connect)
                        private_client.on_message("trade-event", order_update_handler)

                except Exception as e:
                    self.logger.error(f"⚠️ Failed to setup EdgeX WebSocket handlers: {e}")
                    raise

                # Connect to EdgeX WebSocket
                try:
                    self.edgex_ws_manager.connect_private()
                    self._update_edgex_heartbeat()
                    reconnect_delay = 1.0  # Reset delay on successful connection
                    
                except Exception as e:
                    self.logger.error(f"⚠️ Failed to connect EdgeX WebSocket: {e}")
                    raise

                # Monitor connection health
                while not self.stop_flag:
                    await asyncio.sleep(heartbeat_check_interval)
                    
                    # Check if connections are still alive
                    current_time = time.time()
                    time_since_last_message = current_time - self.edgex_last_message_time if self.edgex_last_message_time else float('inf')
                    
                    # Check private client status
                    try:
                        private_client = self.edgex_ws_manager.get_private_client()
                        if not private_client or not self.edgex_private_connected:
                            break
                    except Exception as e:
                        break
                    
                    # Check heartbeat (no messages for too long)
                    if time_since_last_message > self.edgex_heartbeat_timeout:
                        break
                    
                    # Connection is healthy, reset reconnect delay
                    reconnect_delay = 1.0

            except Exception as e:
                self.logger.error(f"⚠️ EdgeX WebSocket error: {e}")
                self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
            finally:
                # Cleanup: disconnect before reconnecting
                try:
                    self.edgex_ws_manager.disconnect_all()
                    self.edgex_private_connected = False
                except Exception as e:
                    self.logger.debug(f"Error during EdgeX disconnect (can be ignored): {e}")

            # Wait before reconnecting with exponential backoff
            if not self.stop_flag:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(max_reconnect_delay, reconnect_delay * 2)

    async def setup_edgex_websocket(self):
        """Setup EdgeX websocket (deprecated, use start_edgex_websocket instead)."""
        self.logger.warning("⚠️ setup_edgex_websocket is deprecated, use start_edgex_websocket instead")
        await self.handle_edgex_websocket()

    async def handle_edgex_public_websocket(self):
        """Handle EdgeX public WebSocket (trades/depth) with manual websockets for RTT/timestamping."""
        reconnect_delay = 1.0
        max_reconnect_delay = 60.0
        
        while not self.stop_flag:
            try:
                # SDK public client appends ?timestamp=ms (see edgex_sdk/ws/client.py)
                ts = int(time.time() * 1000)
                sep = "&" if "?" in self.edgex_public_ws_url else "?"
                ws_connect_url = f"{self.edgex_public_ws_url}{sep}timestamp={ts}"
                async with websockets.connect(ws_connect_url, ping_interval=1.0, ping_timeout=2.0) as ws:
                    self.logger.debug(f"✅ Connected to EdgeX Public WS: {self.edgex_public_ws_url}")
                    reconnect_delay = 1.0
                    
                    subscribe_args = []
                    if self.on_edgex_trade:
                        subscribe_args.append(f"trades.{self.edgex_contract_id}")
                    if self.on_edgex_depth:
                        lv = getattr(self, "edgex_public_depth_levels", 15)
                        subscribe_args.append(f"depth.{self.edgex_contract_id}.{lv}")
                        
                    if subscribe_args:
                        await ws.send(json.dumps({"op": "subscribe", "args": subscribe_args}))
                        
                    while not self.stop_flag:
                        msg_str = await ws.recv()
                        t_arrival = time.time()
                        self.edgex_public_last_message_time = t_arrival
                        self.edgex_public_latency = ws.latency * 1000 if ws.latency is not None else 0.0
                        
                        try:
                            msg = json.loads(msg_str)
                            channel = msg.get("channel", "")
                            if channel.startswith("trades.") and self.on_edgex_trade:
                                ret = self.on_edgex_trade(msg)
                                if asyncio.iscoroutine(ret):
                                    await ret
                            elif channel.startswith("depth.") and self.on_edgex_depth:
                                ret = self.on_edgex_depth(msg)
                                if asyncio.iscoroutine(ret):
                                    await ret
                        except Exception as e:
                            self.logger.debug(f"Public message parse error: {e}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.warning(f"⚠️ EdgeX Public WS error: {e}")
                
            if not self.stop_flag:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(max_reconnect_delay, reconnect_delay * 2)

    def start_edgex_websocket(self):
        """Start EdgeX WebSocket tasks with auto-reconnect."""
        if self.edgex_ws_task is None or self.edgex_ws_task.done():
            self.edgex_ws_task = asyncio.create_task(self.handle_edgex_websocket())
            
        if self.on_edgex_trade or self.on_edgex_depth:
            if self.edgex_public_ws_task is None or self.edgex_public_ws_task.done():
                self.edgex_public_ws_task = asyncio.create_task(self.handle_edgex_public_websocket())

    # Lighter WebSocket methods
    async def handle_lighter_ws(self):
        """Handle Lighter WebSocket connection and messages."""
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        cleanup_counter = 0

        while not self.stop_flag:
            timeout_count = 0
            try:
                async with websockets.connect(url) as ws:
                    # Subscribe to account orders updates (no price subscription)
                    account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"

                    # Get auth token for the subscription
                    try:
                        ten_minutes_deadline = int(time.time() + 10 * 60)
                        auth_token, err = self.lighter_client.create_auth_token_with_expiry(ten_minutes_deadline)
                        if err is not None:
                            self.logger.warning(f"⚠️ Failed to create auth token: {err}")
                        else:
                            auth_message = {
                                "type": "subscribe",
                                "channel": account_orders_channel,
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(auth_message))
                            self.logger.info("✅ Subscribed to account orders with auth token (expires in 10 minutes)")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error creating auth token: {e}")

                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError as e:
                                self.logger.warning(f"⚠️ JSON parsing error: {e}")
                                continue

                            timeout_count = 0

                            if data.get("type") == "ping":
                                await ws.send(json.dumps({"type": "pong"}))

                            elif data.get("type") == "update/account_orders":
                                orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                                for order in orders:
                                    if order.get("status") == "filled" and self.on_lighter_order_filled:
                                        self.on_lighter_order_filled(order)

                            cleanup_counter += 1
                            if cleanup_counter >= 1000:
                                cleanup_counter = 0

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            continue
                        except websockets.exceptions.ConnectionClosed as e:
                            break
                        except websockets.exceptions.WebSocketException as e:
                            self.logger.warning(f"⚠️ Lighter websocket error: {e}")
                            break
                        except Exception as e:
                            self.logger.error(f"⚠️ Error in Lighter websocket: {e}")
                            self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                            break
            except Exception as e:
                pass

            await asyncio.sleep(2)

    def start_lighter_websocket(self):
        """Start Lighter WebSocket task."""
        if self.lighter_ws_task is None or self.lighter_ws_task.done():
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())

    def shutdown(self):
        """Shutdown WebSocket connections."""
        self.stop_flag = True
        
        # Cancel EdgeX WebSocket tasks
        if self.edgex_ws_task and not self.edgex_ws_task.done():
            try:
                self.edgex_ws_task.cancel()
                self.logger.info("🔌 EdgeX WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling EdgeX WebSocket task: {e}")
                
        if self.edgex_public_ws_task and not self.edgex_public_ws_task.done():
            try:
                self.edgex_public_ws_task.cancel()
                self.logger.info("🔌 EdgeX Public WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling EdgeX Public WebSocket task: {e}")

        # Close EdgeX WebSocket connections
        if self.edgex_ws_manager:
            try:
                self.edgex_ws_manager.disconnect_all()
                self.logger.info("🔌 EdgeX WebSocket connections disconnected")
            except Exception as e:
                self.logger.error(f"Error disconnecting EdgeX WebSocket: {e}")

        # Cancel Lighter WebSocket task
        if self.lighter_ws_task and not self.lighter_ws_task.done():
            try:
                self.lighter_ws_task.cancel()
                self.logger.info("🔌 Lighter WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling Lighter WebSocket task: {e}")
