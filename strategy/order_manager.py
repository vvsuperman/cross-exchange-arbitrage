"""Order placement and monitoring for EdgeX and Lighter exchanges."""
import asyncio
import logging
import time
from decimal import Decimal
from typing import Optional

from edgex_sdk import Client, OrderSide, CancelOrderParams, GetOrderBookDepthParams
from lighter.signer_client import SignerClient
from monitor.redis_client import RedisPriceClient


class OrderManager:
    """Manages order placement and monitoring for both exchanges."""

    def __init__(self, order_book_manager, logger: logging.Logger, ticker: Optional[str] = None):
        """Initialize order manager."""
        self.order_book_manager = order_book_manager
        self.logger = logger
        self.ticker = ticker

        # Redis client for price data
        try:
            # self.redis_client = RedisPriceClient(host='172.18.58.232')
            self.redis_client = RedisPriceClient()
        except Exception as e:
            self.logger.warning(f"Failed to initialize Redis client: {e}. Will fallback to order_book_manager.")
            self.redis_client = None

        # EdgeX client and config
        self.edgex_client: Optional[Client] = None
        self.edgex_contract_id: Optional[str] = None
        self.edgex_tick_size: Optional[Decimal] = None
        self.edgex_order_status: Optional[str] = None
        self.edgex_client_order_id: str = ''

        # Lighter client and config
        self.lighter_client: Optional[SignerClient] = None
        self.lighter_market_index: Optional[int] = None
        self.base_amount_multiplier: Optional[int] = None
        self.price_multiplier: Optional[int] = None
        self.tick_size: Optional[Decimal] = None

        # Lighter order state
        self.lighter_order_filled = False
        self.lighter_order_price: Optional[Decimal] = None
        self.lighter_order_side: Optional[str] = None
        self.lighter_order_size: Optional[Decimal] = None

        # Order execution tracking
        self.order_execution_complete = False
        self.waiting_for_lighter_fill = False
        self.current_lighter_side: Optional[str] = None
        self.current_lighter_quantity: Optional[Decimal] = None
        self.current_lighter_price: Optional[Decimal] = None

        # Callbacks
        self.on_order_filled: Optional[callable] = None

    def set_edgex_config(self, client: Client, contract_id: str, tick_size: Decimal):
        """Set EdgeX client and configuration."""
        self.edgex_client = client
        self.edgex_contract_id = contract_id
        self.edgex_tick_size = tick_size

    def set_lighter_config(self, client: SignerClient, market_index: int,
                           base_amount_multiplier: int, price_multiplier: int, tick_size: Decimal):
        """Set Lighter client and configuration."""
        self.lighter_client = client
        self.lighter_market_index = market_index
        self.base_amount_multiplier = base_amount_multiplier
        self.price_multiplier = price_multiplier
        self.tick_size = tick_size

    def set_callbacks(self, on_order_filled: callable = None):
        """Set callback functions."""
        self.on_order_filled = on_order_filled

    def reset_edgex_order_state(self):
        """Reset EdgeX order state after failure or cancellation."""
        self.edgex_order_status = None
        self.edgex_client_order_id = ''

    def reset_lighter_order_state(self):
        """Reset Lighter order state after failure or completion."""
        self.lighter_order_filled = False
        self.lighter_order_price = None
        self.lighter_order_side = None
        self.lighter_order_size = None
        self.waiting_for_lighter_fill = False
        self.current_lighter_side = None
        self.current_lighter_quantity = None
        self.current_lighter_price = None
        self.order_execution_complete = False

    def _refresh_lighter_nonce(self) -> None:
        """Force refresh Lighter nonce from API."""
        try:
            if not self.lighter_client:
                return
            api_key_index = getattr(self.lighter_client, "api_key_index", None)
            if api_key_index is None:
                return
            self.lighter_client.nonce_manager.hard_refresh_nonce(api_key_index)
            self.logger.warning("Refreshed Lighter nonce from API")
        except Exception as e:
            self.logger.warning(f"Failed to refresh Lighter nonce: {e}")

    def round_to_tick(self, price: Decimal) -> Decimal:
        """Round price to tick size."""
        if self.edgex_tick_size is None:
            return price
        return (price / self.edgex_tick_size).quantize(Decimal('1')) * self.edgex_tick_size

    async def fetch_edgex_bbo_prices(self) -> tuple[Decimal, Decimal]:
        """Fetch best bid/ask prices from EdgeX using Redis data."""
        # Try to get from Redis first
        if self.redis_client and self.ticker:
            try:
                bbo_data = self.redis_client.get_latest_bbo('edgex', self.ticker)
                if bbo_data and bbo_data.get('best_bid') and bbo_data.get('best_ask'):
                    best_bid = bbo_data['best_bid']
                    best_ask = bbo_data['best_ask']
                    if best_bid > 0 and best_ask > 0 and best_bid < best_ask:
                        return best_bid, best_ask
            except Exception as e:
                self.logger.exception(f"Failed to get EdgeX BBO from Redis: {e}")

    async def fetch_lighter_bbo_prices(self) -> tuple[Decimal, Decimal]:
        """Fetch best bid/ask prices from Lighter using Redis data."""
         
        if self.redis_client and self.ticker:
            try:
                bbo_data = self.redis_client.get_latest_bbo('lighter', self.ticker)
                if bbo_data and bbo_data.get('best_bid') and bbo_data.get('best_ask'):
                    best_bid = bbo_data['best_bid']
                    best_ask = bbo_data['best_ask']
                    if best_bid > 0 and best_ask > 0 and best_bid < best_ask:
                        return best_bid, best_ask
            except Exception as e:
                self.logger.exception(f"Failed to get Lighter BBO from Redis: {e}")
        return None, None

    async def place_edgex_market_order(self, side: str, quantity: Decimal,
                                       client_order_id: Optional[str] = None) -> Optional[str]:
        """Place a market order on EdgeX. Uses client_order_id if provided."""
        if not self.edgex_client:
            raise Exception("EdgeX client not initialized")

        coid = client_order_id if client_order_id else str(int(time.time() * 1000))
        self.edgex_client_order_id = coid

        # Convert side string to OrderSide enum
        if side.lower() == 'buy':
            order_side = OrderSide.BUY
        else:
            order_side = OrderSide.SELL
        try:
            order_result = await self.edgex_client.create_market_order(
                contract_id=self.edgex_contract_id,
                size=str(quantity),
                side=order_side,
                client_order_id=str(coid)
            )

            if not order_result or 'data' not in order_result:
                raise Exception("Failed to place order")

            order_id = order_result['data'].get('orderId')
            if not order_id:
                raise Exception("No order ID in response")

            return order_id

        except Exception as e:
            self.logger.exception(f"❌ Error placing EdgeX market order: {e}")
            raise


    async def place_bbo_order(self, side: str, quantity: Decimal) -> str:
        """Place a BBO order on EdgeX."""
        best_bid, best_ask = await self.fetch_edgex_bbo_prices()

        if side.lower() == 'buy':
            order_price = best_ask - self.edgex_tick_size
            order_side = OrderSide.BUY
        else:
            order_price = best_bid + self.edgex_tick_size
            order_side = OrderSide.SELL
        

        self.edgex_client_order_id = str(int(time.time() * 1000))
        order_result = await self.edgex_client.create_limit_order(
            contract_id=self.edgex_contract_id,
            size=str(quantity),
            price=str(self.round_to_tick(order_price)),
            side=order_side,
            post_only=True,
            client_order_id=self.edgex_client_order_id
        )

        if not order_result or 'data' not in order_result:
            raise Exception("Failed to place order")

        order_id = order_result['data'].get('orderId')
        if not order_id:
            raise Exception("No order ID in response")

        return order_id

    async def place_edgex_order_async(self, side: str, quantity: Decimal) -> Optional[str]:
        """Place a post-only order on EdgeX asynchronously."""
        if not self.edgex_client:
            raise Exception("EdgeX client not initialized")

        self.edgex_order_status = 'PENDING' # Initialize status
        self.logger.info(f"[OPEN] [EdgeX] [{side}] Placing EdgeX POST-ONLY order")
        
        try:
            order_id = await self.place_bbo_order(side, quantity)
            return order_id
        except Exception as e:
            self.logger.error(f"❌ Error placing EdgeX order: {e}")
            # Reset all EdgeX order state on failure
            self.reset_edgex_order_state()
            return None

    async def cancel_edgex_order(self, order_id: str):
        """Cancel an EdgeX order."""
        if not self.edgex_client:
            return

        try:
            cancel_params = CancelOrderParams(order_id=order_id)
            cancel_result = await self.edgex_client.cancel_order(cancel_params)
            if not cancel_result or 'data' not in cancel_result:
                self.logger.error("❌ Error canceling EdgeX order")
        except Exception as e:
            self.logger.error(f"❌ Error canceling EdgeX order: {e}")

    def handle_edgex_order_update(self, order_data: dict):
        """Handle EdgeX order update."""
        side = order_data.get('side', '').lower()
        filled_size = order_data.get('filled_size')
        price = order_data.get('price', '0')

        if side == 'buy':
            lighter_side = 'sell'
        else:
            lighter_side = 'buy'

        self.current_lighter_side = lighter_side
        self.current_lighter_quantity = filled_size
        self.current_lighter_price = Decimal(price)
        self.waiting_for_lighter_fill = True

    def update_edgex_order_status(self, status: str):
        """Update EdgeX order status."""
        self.edgex_order_status = status

    async def place_lighter_market_order(self, lighter_side: str, quantity: Decimal, client_order_id: Optional[str] = None) -> Optional[str]:
        """Place a market order on Lighter.
        
        Returns:
            Transaction hash if successful, None if failed.
            
        Raises:
            Exception: If order placement fails or times out.
        """
        if not self.lighter_client:
            raise Exception("Lighter client not initialized")
        
        start_total = time.perf_counter()

        # Try to get from Redis first
        best_bid_price = None
        best_ask_price = None
        
        if self.redis_client and self.ticker:
            try:
                bbo_data = self.redis_client.get_latest_bbo('lighter', self.ticker)
                if bbo_data and bbo_data.get('best_bid') and bbo_data.get('best_ask'):
                    best_bid_price = bbo_data['best_bid']
                    best_ask_price = bbo_data['best_ask']
            except Exception as e:
                self.logger.exception(f"Failed to get Lighter BBO from Redis: {e}")

        # Fallback to order_book_manager if Redis data not available
        if best_bid_price is None or best_ask_price is None:
            best_levels = self.order_book_manager.get_lighter_best_levels()
            if not best_levels or not best_levels[0] or not best_levels[1]:
                raise Exception("Lighter order book not ready")
            best_bid_price = best_levels[0][0]
            best_ask_price = best_levels[1][0]

        if lighter_side.lower() == 'buy':
            order_type = "CLOSE"
            is_ask = False
            price = best_ask_price * Decimal('1.05')
        else:
            order_type = "OPEN"
            is_ask = True
            price = best_bid_price * Decimal('0.95')

        # Initialize order state
        self.lighter_order_filled = False
        self.lighter_order_price = price
        self.lighter_order_side = lighter_side
        self.lighter_order_size = quantity


        try:
            client_order_index = client_order_id if client_order_id else str(int(time.time() * 1000))
            # tx_info, error = self.lighter_client.sign_create_order(

            tx, tx_hash, error = await self.lighter_client.create_market_order(
                market_index=self.lighter_market_index,
                client_order_index=int(client_order_index),
                base_amount=int(quantity * self.base_amount_multiplier),
                avg_execution_price=int(price * self.price_multiplier),
                is_ask=is_ask   
            )

            if error is not None and "invalid nonce" in str(error).lower():
                self.logger.warning("Lighter invalid nonce detected, retrying once")
                self._refresh_lighter_nonce()
                tx, tx_hash, error = await self.lighter_client.create_market_order(
                    market_index=self.lighter_market_index,
                    client_order_index=int(client_order_index),
                    base_amount=int(quantity * self.base_amount_multiplier),
                    avg_execution_price=int(price * self.price_multiplier),
                    is_ask=is_ask   
                )
            if error is not None:
                raise Exception(f"Sign error: {error}")

       
            return client_order_index
      
        except Exception as e:
            self.logger.exception(f"❌ Error placing Lighter order: {e}")
            # Reset state on error
            self.reset_lighter_order_state()
            raise

    async def monitor_lighter_order(self, client_order_index: int, stop_flag):
        """Monitor Lighter order and wait for fill.
        
        Raises:
            TimeoutError: If order does not fill within 30 seconds.
        """
        start_time = time.time()
        while not self.lighter_order_filled and not stop_flag:
            if time.time() - start_time > 30:
                elapsed_time = time.time() - start_time
                self.logger.error(
                    f"❌ Timeout waiting for Lighter order fill after {elapsed_time:.1f}s")
                # Reset state before raising exception
                self.reset_lighter_order_state()
                raise TimeoutError(
                    f"Lighter order {client_order_index} did not fill within 30 seconds. "
                    "This may indicate a position mismatch issue."
                )

            await asyncio.sleep(0.1)

    def handle_lighter_order_filled(self, order_data: dict):
        """Handle Lighter order fill notification."""
        try:
            order_data["avg_filled_price"] = (
                Decimal(order_data["filled_quote_amount"]) /
                Decimal(order_data["filled_base_amount"])
            )
            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                order_type = "OPEN"
            else:
                order_data["side"] = "LONG"
                order_type = "CLOSE"

            client_order_index = order_data["client_order_id"]

            self.logger.info(
                f"[{client_order_index}] [{order_type}] [Lighter] [FILLED]: "
                f"{order_data['filled_base_amount']} @ {order_data['avg_filled_price']}")

            if self.on_order_filled:
                self.on_order_filled(order_data)

            self.lighter_order_filled = True
            self.order_execution_complete = True

        except Exception as e:
            self.logger.error(f"Error handling Lighter order result: {e}")

    def get_edgex_client_order_id(self) -> str:
        """Get current EdgeX client order ID."""
        return self.edgex_client_order_id
