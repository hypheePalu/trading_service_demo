import math
from decimal import Decimal
from msg_queue import AsyncMessageQueue
from src.data_format import MaxOrderUpdate, Signal, OrderStatus, Order, MarketInfo
from datetime import datetime
from system_config import NATSConfig
from typing import Dict, Any, Optional, List, Tuple, Union, Literal
import logging
from api_client import MaxAPIClient
import asyncio
from src.config_enum import MaxOrderState, StrategyState, WalletType, RunningStrategy, OrderType, OrderSide, Exchange
from position_manager import MaxPositionManager
import time
from market_info import ExchangeMarketConfig
from settings import STRATEGY_CONFIG, ORDER_CONFIG
from market_data import MarketData
from max_api_service import MaxAPIService
from order_reconciler import OrderReconciler


#TODO: periodic cancel expired orders / place an order and lock the state
class MaxOrderManager:
    _logger = logging.getLogger("order_manager.MaxOrderManager")

    def __init__(self, client:MaxAPIClient, interval: int = ORDER_CONFIG.CLEANUP_INTERVAL_SECONDS, 
                 position_manager:MaxPositionManager = None,
                 market_data:MarketData = None,
                 maker_spread: float = ORDER_CONFIG.MAKER_SPREAD, 
                 taker_spread: float = ORDER_CONFIG.TAKER_SPREAD, 
                 order_expire_seconds: int = ORDER_CONFIG.ORDER_EXPIRE_SECONDS):
        self.client = client
        self.unfilled_orders: Dict[str, Order]= {} # client_order_id : Order
        self.unfilled_orders_lock = asyncio.Lock()
        self.strategy_state = None # symbol : {state: buy, sell}
        self.mq: Optional[AsyncMessageQueue] = None
        self._listener_tasks: List[asyncio.Task] = []
        self._periodic_task: Optional[asyncio.Task] = None
        self.logger = MaxOrderManager._logger
        self.interval = interval
        self.id_running_strategy = {k.value: k for k in RunningStrategy}
        self.max_market_data_feed =  market_data
        # Configurable parameters from settings.py
        self.maker_spread = maker_spread
        self.taker_spread = taker_spread
        self.order_expire_seconds = order_expire_seconds

        # for syncing
        self._periodic_sync_task: Optional[asyncio.Task] = None
        self.sync_interval = 60  # Sync every 60 seconds, can be adjusted as needed
        self._is_syncing = False
        self._pending_updates_queue = asyncio.Queue()

        if not position_manager:
            self.logger.error("No position manager provided")
            raise
        self.position_manager = position_manager
        self.done_orders: Dict = {}
        self.order_has_snapshot = False

        # Initialize service classes
        self.api_service = MaxAPIService(client)
        self.reconciler = OrderReconciler(position_manager)


    async def initialize(self):
        self.strategy_state = {s.value: {symbol: {OrderSide.BUY.value: StrategyState.IDLE, OrderSide.SELL.value:StrategyState.IDLE}
                                         for symbol in ExchangeMarketConfig.exchange_info.MAX.keys()}
                               for s in RunningStrategy}

    @staticmethod
    def get_market_info(symbol) -> MarketInfo:
        return ExchangeMarketConfig.exchange_info.MAX[symbol]

    async def _initialize_message_queue(self):
        if self.mq is None:
            try:
                self.mq = await AsyncMessageQueue.get_instance()
                self.logger.info("AsyncMessageQueue instance obtained.")
            except Exception as e:
                self.logger.error("Exception while initializing message queue: " + str(e))
                raise


    async def _message_handler(self, subject: str, msg: Dict[str, Any]):
        if self._is_syncing:
            await self._pending_updates_queue.put((subject, msg))
            return
        self.logger.debug(f"Message handler started. msg: {msg}")

        parts = subject.split('.')
        # New format: {domain}.{market_type}.{sub_type}.{event_type}.{symbol}.{exchange}[.{strategy}]
        
        if len(parts) >= 6:
            domain = parts[0]
            market_type = parts[1] 
            sub_type = parts[2]
            event_type = parts[3]
            
            # Handle user order updates: user.spot.order.update.{symbol}.{exchange}
            if (domain == "user" and market_type == "spot" and 
                sub_type == "order" and event_type == "update"):
                order_update = MaxOrderUpdate(**msg)
                await self.on_order_update(order_update)
                return
            
            # Handle strategy signals: strategy.spot.signal.detect.{symbol}.{exchange}.{strategy}
            if (domain == "strategy" and market_type == "spot" and 
                sub_type == "signal" and event_type == "detect"):
                signal = Signal(**msg)
                self.on_signal_detect(signal)
                return



    async def cal_new_executed_amount(self, order_update: MaxOrderUpdate) -> Optional[float]:
        new_executed = order_update.executed_volume

        old_order_detail = self.unfilled_orders.get(order_update.client_order_id, None)
        if not old_order_detail:
            self.logger.error(f'not able to find {order_update.client_order_id} in {self.unfilled_orders}')
            return None
        old_executed = old_order_detail.filled_sz

        executed_increment = new_executed - old_executed
        if executed_increment < 0:
            self.logger.error(f'executed increment {executed_increment} is negative')
            return None
        return executed_increment


    async def on_order_update(self, order_update: MaxOrderUpdate):
        order_state: MaxOrderState = order_update.state
        symbol = order_update.market
        client_order_id = order_update.client_order_id
        side = order_update.side

        # 步驟一：處理不存在於系統中的訂單
        try:
            parts = client_order_id.split('_')
            strategy_id = parts[0]
            wallet_type = parts[1]
            # Check if this order comes from a known strategy in this system
            if strategy_id not in self.strategy_state:
                self.logger.warning(
                    f"Received update for unknown strategy ID: {strategy_id}. Order ID: {client_order_id}. Ignoring.")
                return  # Exit directly, don't process this order

            if wallet_type not in [WalletType.SPOT, WalletType.M]:
                self.logger.warning(f'Received update for unknown wallet type in order {client_order_id}: {wallet_type}')
                return

        except IndexError:
            # Handle case where client_order_id doesn't contain '_'
            self.logger.warning(f"Received update for an unknown client order ID format: {client_order_id}. Ignoring.")
            return

        # Step 2: Update order data
        # This is the general update logic, execute once at the beginning
        if not client_order_id or client_order_id not in self.unfilled_orders:
            self.logger.warning(f'An order cannot be found in self.unfilled_orders. client_order_id: {client_order_id} order_id: {order_update.idx}. skip')
            return

        executed_amount = await self.cal_new_executed_amount(order_update)
        if executed_amount is None:
            self.logger.error(f'executed amount {executed_amount} is negative')
            return
        self._update_an_order(order_update)

        # update to position manager

        self.position_manager.on_order_increment(strategy_id_str=strategy_id,
                                                 side=side,
                                                 symbol=symbol,
                                                 wallet_type_str=wallet_type,
                                                 executed_amount=executed_amount
                                                 )

        # Step 3: Update strategy state and transfer orders based on terminal status
        if order_state in [MaxOrderState.CANCEL, MaxOrderState.DONE, MaxOrderState.CONVERT]:
            if order_state == MaxOrderState.DONE:
                self.logger.info(f'Order Filled, move to done orders: {order_update}')
            elif order_state == MaxOrderState.CANCEL:
                self.logger.info(f'Order Cancelled, move to done orders: {order_update}')
            else:
                self.logger.info(f'Order Converted, move to done orders: {order_update}')

            # Remove order from unfilled dictionary and move to done dictionary
            async with self.unfilled_orders_lock:
                done_order_data = self.unfilled_orders.pop(client_order_id, None)
            if done_order_data:
                self.done_orders[client_order_id] = done_order_data

            # Release lock state for specific trading pair, allow new trading signals
            self.change_strategy_symbol_state(strategy_id=strategy_id, symbol=symbol, action=StrategyState.IDLE, side=done_order_data.side)
            
            # If order is filled, log successful execution
            if order_state == MaxOrderState.DONE:
                self.logger.info(f'Strategy {strategy_id} for {symbol} successfully filled and state reset to IDLE')

        elif order_state == MaxOrderState.WAIT:
            self.logger.debug(f'client_order_id={client_order_id} waiting to be filled')

    def on_signal_detect(self, signal: Signal):
        self.logger.debug(f'Process received signal: {signal}')
        strategy_id = signal.strategy_id
        symbol = signal.symbol
        side = signal.side
        strategy_state = self.strategy_state[strategy_id][symbol][side]

        if not strategy_state:
            self.logger.warning(f'Not able to find strategy state of strategy id: {strategy_id}')
            return
        # self.logger.info(f'unfilled_orders={self.unfilled_orders.keys()}')

        if strategy_state == StrategyState.IDLE:

            target_mid_px = signal.attempted_mid
            is_maker: bool = signal.maker
            ticket_sz = STRATEGY_CONFIG.TICKET_SIZE[strategy_id]
            mid_px = self.max_market_data_feed.get_mid_price(symbol=symbol)

            # Use target_mid_px for order size calculation since best_px might be None
            base_order_sz = self.get_minimum_base_order_amount(symbol, ticket_sz, mid_px)
            # Place order directly - allocation checks handled by position manager
            self.logger.info(f'Placing order for signal: {signal}')

            asyncio.create_task(self._execute_signal_order(signal=signal,
                                                           base_order_sz= base_order_sz,
                                                           target_px=target_mid_px,
                                                           spread=signal.spread_mode,
                                                           is_maker=is_maker))
        else:
            self.logger.debug(f'SIGNAL SKIPPED: Strategy {strategy_id} for {symbol} is not IDLE (current state: {strategy_state}). Signal: side={signal.side}, price={signal.attempted_mid}, timestamp={signal.signal_generated_ts}')
            return


    @staticmethod
    def generate_client_order_id(strategy_id:RunningStrategy, wallet_type:WalletType) -> str:
        return f'{strategy_id}_{wallet_type}_{time.perf_counter_ns()}'

    # decode_client_order_id method moved to OrderReconciler service

    @staticmethod
    def get_minimum_base_order_amount(symbol: str, ticket_sz: float, price: float) -> float:
        """
        Calculate the minimum base order amount that satisfies exchange requirements.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT', 'USDTTWD')
            ticket_sz: Target value in USDT (e.g., 10.0 means $10 worth)
            price: Order price
            
        Returns:
            float: Minimum base order amount that meets exchange requirements
        """
        try:
            # Get market info for the symbol
            market_info = ExchangeMarketConfig.exchange_info.MAX.get(symbol)
            if not market_info:
                raise ValueError(f"Market info not found for symbol: {symbol}")
            
            minimum_base_amount = market_info.min_base_amount
            base_unit_precision = market_info.base_unit_precision
            base_currency = market_info.base
            quote_currency = market_info.quote
            
            if price <= 0 or ticket_sz <= 0 or minimum_base_amount <= 0:
                return 0.0

            # 1. Convert ticket_sz (USDT value) to base currency amount
            if base_currency.upper() == 'USDT':
                # For pairs like USDTTWD: base=USDT, quote=TWD
                # ticket_sz is already in USDT, so this is our target base amount
                ticket_base_amount = ticket_sz
            elif quote_currency.upper() == 'USDT':
                # For pairs like BTCUSDT: base=BTC, quote=USDT  
                # ticket_sz is in USDT value, so divide by price to get base currency amount
                ticket_base_amount = ticket_sz / price
            else:
                # For other pairs, assume ticket_sz represents quote currency value
                # and convert to base currency amount
                ticket_base_amount = ticket_sz / price

            # 2. Calculate the minimum order quantity to meet minimum base amount requirement
            required_sz_for_min_amount = minimum_base_amount

            # 3. Take the maximum of ticket size and minimum base amount requirements
            required_sz = max(ticket_base_amount, required_sz_for_min_amount)

            # 4. Handle base unit precision
            # Round to the required precision for the base currency
            precision_factor = 10 ** base_unit_precision
            final_sz = math.ceil(required_sz * precision_factor) / precision_factor
            
            return final_sz
            
        except Exception as e:
            # Log error and return 0 as fallback
            import logging
            logger = logging.getLogger("order_manager.MaxOrderManager")
            logger.error(f"Error calculating minimum base order amount for {symbol}: {e}")
            return 0.0


    @staticmethod
    def _init_an_order(client_order_id:str,
                       data:Signal) -> Order:
        created_time = time.time_ns()

        return Order(exchange=Exchange.MAX,
                     client_order_id=client_order_id,
                     side = data.side,
                     order_type=None,
                     price=None,
                     average_price=None,
                     sz=None,
                     unfilled_sz=None,
                     filled_sz=None,
                     order_status=OrderStatus.PENDING,
                     symbol=data.symbol,
                     order_created_ts=created_time,
                     order_updated_ts=created_time,
                     price_received_ts=data.price_received_ts,
                     signal_generated_ts=data.signal_generated_ts,
                     attempted_mid=data.attempted_mid,
                     exchange_created_ts=0,
                     exchange_updated_ts=0,
                     )

    def _update_an_order(self, data: MaxOrderUpdate):

        updated_time = int(datetime.now().timestamp() * 1000)
        if data.client_order_id not in self.unfilled_orders:
            self.logger.error(f'Client order ID: {data.client_order_id} not found in unfilled orders')
            return

        unfilled_order = self.unfilled_orders[data.client_order_id]

        if data.state == MaxOrderState.DONE:
            status = OrderStatus.FILLED
        elif data.state == MaxOrderState.WAIT:
            if float(data.executed_volume) == 0:
                status = OrderStatus.NEW
            else:
                status = OrderStatus.PARTIALLY_FILLED
        elif data.state == MaxOrderState.CANCEL:
            status = OrderStatus.CANCELED
        else:
            self.logger.error(f'Unknown state {data.state} for client order ID: {data.client_order_id}. data: {data}')
            return

        exchanged_updated_ts = data.order_updated_ts

        if not unfilled_order.exchange_updated_ts:
            self.logger.debug(f'Client order ID: {data.client_order_id} first update')

        if exchanged_updated_ts < unfilled_order.exchange_updated_ts:
            self.logger.info(f'order: {data.client_order_id} has been updated to the latest since update ts({exchanged_updated_ts})'
                             f'is smaller than the latest order ts({unfilled_order.exchange_updated_ts})')
            return
        self.logger.debug(f'Updating client order id: {data.client_order_id}')

        unfilled_order.update(price=data.price,
                              average_price=data.average_price,
                              sz=data.volume,
                              unfilled_sz=data.remaining_volume,
                              filled_sz=data.executed_volume,
                              order_status=status,
                              exchange_created_ts=data.order_created_ts,
                              exchange_updated_ts=data.order_updated_ts,
                              order_updated_ts=updated_time)


    async def cancel_an_order(self, client_order_id: str):
        try:
            if client_order_id not in self.unfilled_orders:
                self.logger.warning(f'Cannot cancel order {client_order_id}: not found in unfilled orders')
                return False
            response = await self.client.cancel_an_order(
                order_id=None,  # Using client_order_id instead
                client_oid=client_order_id
            )
            
            self.logger.info(f'Cancel order request sent for {client_order_id}: {response}')
            return True
            
        except Exception as e:
            self.logger.error(f'Failed to cancel order {client_order_id}: {e}')
            return False

    def change_strategy_symbol_state(self, strategy_id:str, symbol:str, side:str, action:StrategyState):
        if strategy_id not in self.strategy_state:
            self.logger.error(f'Strategy ID: {strategy_id} not found in strategy state dict: {self.strategy_state}')
            return

        if symbol not in self.strategy_state[strategy_id]:
            self.logger.warning(f'Symbol {symbol} not found in strategy state {strategy_id}')
            return
        self.strategy_state[strategy_id][symbol][side] = action

    async def place_an_order(self,
                             wallet_type: WalletType,
                             client_order_id: str,
                             symbol: str,
                             order_type: OrderType,
                             side: OrderSide,
                             price: str,
                             amount: str,
                             group_id: Optional[int] = None
                            ):
        try:
            # self.logger.info(f'Client order ID: {client_order_id}')
            order_response = await self.client.place_an_order(
                path_wallet_type=wallet_type,
                market=symbol,
                order_type=order_type,
                side=side,
                price=price,
                amount=amount,
                group_id=group_id,
                client_oid=client_order_id
            )
            return order_response
        except Exception as e:
            # self.logger.error(f'Failed to place an order: {e}')
            raise e

    def get_orders(self, status_filter_list: Optional[List[OrderStatus]] = None) -> Dict[str, Order]:
        if status_filter_list:
            return {k: v for k, v in self.unfilled_orders.items()
                   if v.order_status in status_filter_list}
        return self.unfilled_orders.copy()
    
    def get_done_orders(self) -> Dict[str, Order]:
        return self.done_orders.copy()
    
    def get_order_count(self) -> Dict[str, int]:
        return {
            'unfilled': len(self.unfilled_orders),
            'done': len(self.done_orders)
        }

    def get_orders_by_account_type(self, account_type: WalletType) -> Dict[str, Order]:
        """
        Get all orders filtered by account type (wallet type).

        Args:
            account_type: WalletType enum to filter by

        Returns:
            Dict[str, Order]: Dictionary of client_order_id -> Order for the specified account type
        """

        try:
            filtered_orders = {}
            for client_order_id, order in self.unfilled_orders.items():
                try:
                    _, wallet_type = self.reconciler.decode_client_order_id(client_order_id)
                    if wallet_type == account_type:
                        filtered_orders[client_order_id] = order
                except ValueError as e:
                    self.logger.warning(f"Failed to decode client_order_id '{client_order_id}': {e}")
                    continue
            return filtered_orders
        except Exception as e:
            self.logger.error(f"Error filtering orders by account type {account_type}: {e}")
            return {}



    async def start(self):
        self.logger.info("Starting MaxOrderManager.")

        await self._initialize_message_queue()

        if self._periodic_task is None or self._periodic_task.done():
            self._periodic_task = asyncio.create_task(self._periodic_cleanup_task())
            self.logger.info('Started periodic cleanup task')

        # Start periodic sync task
        if self._periodic_sync_task is None or self._periodic_sync_task.done():
            self._periodic_sync_task = asyncio.create_task(self._run_periodic_sync())
            self.logger.info('Started periodic sync task')

        self._listener_tasks.append(asyncio.create_task(
            self.mq.subscribe(NATSConfig.PATTERN_ALL_USER_ORDER_UPDATES, handler=self._message_handler),
            name=f"MaxSpotOrder_Listener"
        ))
        self.logger.info(f'Subscribed to {NATSConfig.PATTERN_ALL_USER_ORDER_UPDATES}')

        self._listener_tasks.append(asyncio.create_task(
            self.mq.subscribe(NATSConfig.PATTERN_ALL_STRATEGY_SIGNALS, handler=self._message_handler),
            name=f"StrategyMaxArbSpotSignal_Listener"
        ))
        self.logger.info(f'Subscribed to {NATSConfig.PATTERN_ALL_STRATEGY_SIGNALS}')
        
        self.logger.info("MaxOrderManager started successfully")

    def round_tick(self, symbol: str, price: Union[float, Decimal], mode: Literal['up', 'down']) -> float:
        """
        Round the input price by one tick for the given symbol.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            price: Input price to round by one tick
            mode: 'up' to round up one tick, 'down' to round down one tick
            
        Returns:
            float: Price adjusted by one tick (minimum price increment)
        """
        try:
            price = float(price)
            # Get market info for the symbol
            market_info = ExchangeMarketConfig.exchange_info.MAX.get(symbol)
            if not market_info:
                self.logger.warning(f"Market info not found for symbol: {symbol}, returning original price")
                return price
            
            # Get quote unit precision (number of decimal places for price)
            precision = market_info.quote_unit_precision
            
            # Calculate the precision factor and tick size
            precision_factor = 10 ** precision
            tick_size = 1.0 / precision_factor
            
            # Adjust price by one tick based on mode
            if mode == 'up':
                new_price = price + tick_size
            elif mode == 'down':
                new_price = price - tick_size
            else:
                raise ValueError(f"Invalid mode: {mode}. Must be 'up' or 'down'")
            
            # Round to proper precision to avoid floating point errors
            rounded_price = round(new_price, precision)
            
            self.logger.debug(f"Rounded {mode} one tick for {symbol}: {price} -> {rounded_price} (tick size: {tick_size})")
            
            return rounded_price
            
        except Exception as e:
            self.logger.error(f"Error rounding {mode} one tick for {symbol}: {e}")
            return price

    async def _execute_signal_order(self,
                                    signal: Signal,
                                    base_order_sz: float,
                                    target_px: float,
                                    is_maker: bool,
                                    spread:float):
        """Execute an order based on arbitrage signal.

        Args:



            """

        strategy_id = signal.strategy_id
        symbol = signal.symbol
        
        # Calculate and log signal-to-order latency
        current_time = int(time.time() * 1000)
        signal_to_order_latency = current_time - signal.signal_generated_ts
        self.logger.debug(f"SIGNAL LATENCY: {signal_to_order_latency}ms from signal generation to order execution start. Signal: side={signal.side}, symbol={symbol}, signal_ts={signal.signal_generated_ts}, order_start_ts={current_time}")
        
        # Lock the strategy state to prevent concurrent orders
        self.change_strategy_symbol_state(strategy_id=strategy_id, symbol=symbol, action=StrategyState.LOCKED, side=signal.side)

        client_order_id = self.generate_client_order_id(strategy_id, signal.wallet_type)
        self.logger.info(f"Executed signal order for {symbol}: {client_order_id}")
        try:
            # Create initial order record

            order = self._init_an_order(client_order_id, signal)
            order.sz = base_order_sz
            order.unfilled_sz = base_order_sz
            order.filled_sz = 0.0
            async with self.unfilled_orders_lock:
                self.unfilled_orders[client_order_id] = order
            best_bid, best_ask = self.max_market_data_feed.get_best_bid_ask(symbol=symbol)

            if not best_bid or not best_ask:
                self.logger.warning(f"Bid or ask not found for symbol: {symbol}. Skipping signal")
                return
            if best_bid > best_ask:
                self.logger.warning(f'Best bid: {best_bid} > Best ask: {best_ask}. Skipping signal')
                return

            if is_maker:
                # Place maker order with tight spread
                if signal.side == OrderSide.BUY:
                    px = self.round_tick(price= target_px * (1 - spread),
                                         symbol=symbol,
                                         mode='down'
                                         )
                    if px > best_bid:
                        px = best_bid
                elif signal.side == OrderSide.SELL:
                    px = self.round_tick(price=target_px * (1 + spread),
                                         symbol=symbol,
                                         mode='up'
                                         )
                    if px < best_ask:
                        px = best_ask
                else:
                    self.logger.warning('Unknown Order Side. Skipping signal')
                    return

                await self.place_an_order(
                    wallet_type=signal.wallet_type,
                    client_order_id=client_order_id,
                    symbol=symbol,
                    order_type=OrderType.POST_ONLY,
                    side=signal.side,
                    price=str(px),
                    amount=str(base_order_sz)
                )
            else:
                if signal.side == OrderSide.BUY:
                    px = self.round_tick(price= target_px * (1 + spread),
                                         symbol=symbol,
                                         mode='up'
                                         )

                elif signal.side == OrderSide.SELL:
                    px = self.round_tick(price=target_px * (1 + spread),
                                         symbol=symbol,
                                         mode='up'
                                         )
                else:
                    self.logger.warning('Unknown Order Side. Skipping signal')
                    return
                # it is a taker order with target spread
                await self.place_an_order(
                    wallet_type=signal.wallet_type,
                    client_order_id=client_order_id,
                    symbol=symbol,
                    order_type=OrderType.IOC_LIMIT,
                    side=signal.side,
                    price=str(px),
                    amount=str(base_order_sz)
                )

            self.logger.info(f'Signal order executed successfully: {client_order_id}')
            
        except Exception as e:
            self.logger.warning(f'Failed to execute signal order: {e}')
            # Unlock strategy state on error
            self.change_strategy_symbol_state(strategy_id= strategy_id, symbol= symbol, action= StrategyState.IDLE,
                                              side=signal.side)
            # Remove failed order from tracking

            if client_order_id in self.unfilled_orders:
                async with self.unfilled_orders_lock:
                    del self.unfilled_orders[client_order_id]

    async def _periodic_cleanup_task(self):
        """Periodic task to clean up expired orders and log statistics."""
        while True:
            try:
                self.logger.info(f"Clean up expired orders: {len(self.unfilled_orders)}")
                await asyncio.sleep(self.interval)
                await self._cleanup_expired_orders()
                await self._log_order_statistics()
                
            except asyncio.CancelledError:
                self.logger.info("Periodic cleanup task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in periodic cleanup: {e}")

    async def _cleanup_expired_orders(self):
        """Cancel orders that have been unfilled for too long."""
        current_time = time.time_ns()
        expired_orders = []
        for client_order_id, order in self.unfilled_orders.items():
            # Cancel orders older than configured expiration time
            order_age = (current_time - order.order_created_ts) / 1_000_000_000  # Convert to seconds
            if order_age > self.order_expire_seconds:
                expired_orders.append((client_order_id, order_age))
        
        if expired_orders:
            await self.api_service.cancel_orders_parallel(expired_orders)

    async def _log_order_statistics(self):
        """Log current order statistics."""
        stats = self.get_order_count()
        if stats['unfilled'] > 0 or stats['done'] > 0:
            self.logger.info(f'Order statistics: {stats["unfilled"]} unfilled, {stats["done"]} completed')
    
    async def stop(self):
        """Stop the order manager and clean up resources."""
        self.logger.info("Stopping MaxOrderManager...")
        
        # Cancel periodic tasks
        if self._periodic_task and not self._periodic_task.done():
            self._periodic_task.cancel()
            try:
                await self._periodic_task
            except asyncio.CancelledError:
                pass

        if self._periodic_sync_task and not self._periodic_sync_task.done():
            self._periodic_sync_task.cancel()
            try:
                await self._periodic_sync_task
            except asyncio.CancelledError:
                pass
        
        # Cancel all listener tasks
        for task in self._listener_tasks:
            task.cancel()
        
        if self._listener_tasks:
            await asyncio.gather(*self._listener_tasks, return_exceptions=True)

        # Cancel all remaining unfilled orders using API service
        if self.unfilled_orders:
            order_ids = list(self.unfilled_orders.keys())
            cancellation_results = await self.api_service.cancel_orders_by_ids_parallel(order_ids)

            cancelled_orders = sum(1 for success in cancellation_results.values() if success)
            if cancelled_orders > 0:
                self.logger.info(f"Successfully cancelled {cancelled_orders} orders during shutdown.")
        
        self.logger.info("MaxOrderManager stopped successfully")

    async def _run_periodic_sync(self):
        """Periodically reconcile orders and feed differences to PositionManager."""
        while True:
            try:
                self.logger.info('Starting periodic order sync with exchange...')

                # Step 1: Enable lock
                self._is_syncing = True

                # Step 2: Get all local active orders, group by wallet_type and symbol
                wallet_symbol_combinations = self._get_active_wallet_symbol_combinations()
                
                if not wallet_symbol_combinations:
                    self.logger.debug('No active orders to sync')
                else:
                    # Step 3: Query orders for all wallet_type + symbol combinations in parallel
                    all_exchange_orders = await self.api_service.fetch_orders_parallel(wallet_symbol_combinations)

                    # Step 4: Find mismatched orders and get detailed information

                    mismatched_order_ids = [oid for oid in self.unfilled_orders.keys() if oid not in all_exchange_orders]
                    
                    if mismatched_order_ids:
                        detailed_orders = await self.api_service.fetch_order_details_parallel(mismatched_order_ids)


                        # Step 5: Use reconciler for reconciliation
                        synced_count = await self.reconciler.reconcile_orders(
                            self.unfilled_orders, all_exchange_orders, detailed_orders
                        )

                        # Step 6: Remove completed orders
                        completed_orders = self.reconciler.update_local_orders_from_detailed_info(
                            self.unfilled_orders, detailed_orders
                        )

                        # Move completed orders to done_orders
                        for client_order_id, completed_order in completed_orders.items():
                            if client_order_id in self.unfilled_orders:
                                self.done_orders[client_order_id] = self.unfilled_orders.pop(client_order_id)
                                parts = client_order_id.split('_')
                                strategy_id = parts[0]
                                self.change_strategy_symbol_state(strategy_id=strategy_id,
                                                                  symbol=completed_order.symbol,
                                                                  side=completed_order.side,
                                                                  action=StrategyState.IDLE)
                        if synced_count > 0:
                            self.logger.info(f'Synchronized {synced_count} orders with position manager')
                    else:
                        self.logger.debug('All orders in sync with exchange')

                self.logger.info('Order sync completed.')

            except Exception as e:
                self.logger.error(f'Error during periodic sync: {e}')

            finally:
                # Step 7: Release lock and process buffered messages
                self._is_syncing = False
                while not self._pending_updates_queue.empty():
                    subject, msg = await self._pending_updates_queue.get()
                    self.logger.warning(f'Processing delayed message from queue: {subject}')
                    await self._message_handler(subject, msg)

            await asyncio.sleep(self.sync_interval)

    def _get_active_wallet_symbol_combinations(self) -> Dict[Tuple[WalletType, str], List[str]]:
        """
        Get wallet_type + symbol combinations for all active orders.

        Returns:
            Dict[Tuple[WalletType, str], List[str]]: {(wallet_type, symbol): [client_order_ids]}
        """
        combinations = {}
        
        for client_order_id, order in self.unfilled_orders.items():
            try:
                strategy_id, wallet_type = self.reconciler.decode_client_order_id(client_order_id)
                symbol = order.symbol
                key = (wallet_type, symbol)
                
                if key not in combinations:
                    combinations[key] = []
                combinations[key].append(client_order_id)
                
            except ValueError as e:
                self.logger.warning(f'Failed to decode client_order_id for sync: {client_order_id}: {e}')
                continue
        
        return combinations

