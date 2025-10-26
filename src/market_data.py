import asyncio
import logging
import datetime
from typing import Dict, Any, Optional, List, Tuple
from decimal import Decimal
from msg_queue import AsyncMessageQueue
from src.data_format import L1BookUpdate, L2BookUpdate, PriceUpdate
from dataclasses import asdict
from system_config import NATSConfig


class MarketData:
    _logger = logging.getLogger("market_data.MarketData")
    _instance: Optional['MarketData'] = None
    _is_initialized: bool = False

    def __init__(self):
        if MarketData._instance is not None:
            raise RuntimeError("MarketData is a singleton. Use create_instance() instead.")

        self.logger = MarketData._logger
        self.mq: Optional[AsyncMessageQueue] = None

        # Multi-exchange market data storage
        # Structure: {symbol: {exchanges: {exchange_name: {l1_data, l2_data, best_bid, best_ask}}, aggregated: {...}}}
        self.market_data: Dict[str, Dict[str, Any]] = {}

        # Backward compatibility - deprecated, use aggregated data instead
        self.best_bids: Dict[str, Decimal] = {}
        self.best_asks: Dict[str, Decimal] = {}

        self._listener_tasks: List[asyncio.Task] = []
        self._closing: bool = False

    @classmethod
    async def create_instance(cls):
        """
        異步地建立 MarketData 的單例實例，並啟動訂閱。
        """
        if cls._instance is None:
            instance = cls()
            instance.mq = await AsyncMessageQueue.get_instance()
            cls._instance = instance
            cls._is_initialized = True

            await instance._start_listening()

            cls._logger.info("MarketData singleton instance created and listening started.")
        return cls._instance

    async def _message_handler(self, subject: str, data: Dict[str, Any]):
        """
        Handle L1 and L2 book updates from all exchanges.
        Subject format: market.spot.{l1Book|l2Book}.update.{symbol}.{exchange}
        """
        if self._closing:
            return

        try:
            # Parse subject to extract exchange and data type
            parts = subject.split('.')
            if len(parts) < 6:
                self.logger.warning(f"Invalid subject format: {subject}")
                return
                
            data_type = parts[2]  # l1Book or l2Book
            symbol = parts[4].lower()
            exchange = parts[5].lower()  # Store exchange keys in lowercase
            
            message_processed_at_local_ts = datetime.datetime.now().timestamp()
            
            # Initialize symbol data structure if not exists
            if symbol not in self.market_data:
                self.market_data[symbol] = {
                    "exchanges": {},
                    "aggregated": {}
                }
            
            if exchange not in self.market_data[symbol]["exchanges"]:
                self.market_data[symbol]["exchanges"][exchange] = {
                    "l1_data": None,
                    "l2_data": None,
                    "best_bid": Decimal('0'),
                    "best_ask": Decimal('inf'),
                    "last_updated": 0
                }
            
            exchange_data = self.market_data[symbol]["exchanges"][exchange]
            
            # Process L1 or L2 data
            if data_type == "l1Book":
                l1_data = L1BookUpdate(**data)
                exchange_data["l1_data"] = l1_data
                exchange_data["best_bid"] = Decimal(str(l1_data.bid_px))
                exchange_data["best_ask"] = Decimal(str(l1_data.ask_px))
                exchange_data["last_updated"] = message_processed_at_local_ts
                
            elif data_type == "l2Book":  
                l2_data = L2BookUpdate(**data)
                exchange_data["l2_data"] = l2_data
                
                # Extract best prices from L2 data
                if l2_data.bids:
                    exchange_data["best_bid"] = Decimal(l2_data.bids[0][0])
                else:
                    exchange_data["best_bid"] = Decimal('0')
                    
                if l2_data.asks:
                    exchange_data["best_ask"] = Decimal(l2_data.asks[0][0])
                else:
                    exchange_data["best_ask"] = Decimal('inf')
                    
                exchange_data["last_updated"] = message_processed_at_local_ts
            
            # Update aggregated data (handles n=1 case automatically)
            self._update_aggregated_data(symbol)
            
            # Publish price update
            await self._publish_price_update(symbol, exchange, data_type)
            
            # Backward compatibility
            aggregated = self.market_data[symbol]["aggregated"]
            if aggregated:
                self.best_bids[symbol] = aggregated["best_bid"]
                self.best_asks[symbol] = aggregated["best_ask"]
            
            self.logger.debug(
                f"[{symbol}] {data_type} update from {exchange}. "
                f"Best bid/ask: {exchange_data['best_bid']}/{exchange_data['best_ask']}. "
                f"Aggregated: {aggregated.get('best_bid', 'N/A')}/{aggregated.get('best_ask', 'N/A')}"
            )
            
        except Exception as e:
            self.logger.error(f"Error processing market data message: {e}", exc_info=True)

    def _update_aggregated_data(self, symbol: str):
        """Update aggregated best prices across all exchanges (handles n=1 case)"""
        exchanges_data = self.market_data[symbol]["exchanges"]
        
        if not exchanges_data:
            # n=0 case: No data available
            self.market_data[symbol]["aggregated"] = {}
            return
            
        valid_exchanges = {
            name: data for name, data in exchanges_data.items()
            if data["best_bid"] > 0 and data["best_ask"] < Decimal('inf')
        }
        
        if not valid_exchanges:
            self.market_data[symbol]["aggregated"] = {}
            return
        
        if len(valid_exchanges) == 1:
            # n=1 case: Single exchange, use its data directly
            exchange_name = list(valid_exchanges.keys())[0]
            exchange_data = valid_exchanges[exchange_name]
            
            self.market_data[symbol]["aggregated"] = {
                "best_bid": exchange_data["best_bid"],
                "best_ask": exchange_data["best_ask"],
                "best_bid_exchange": exchange_name,
                "best_ask_exchange": exchange_name,
                "active_exchanges": [exchange_name],
                "exchange_count": 1
            }
        else:
            # n>1 case: Multiple exchanges, find best across all
            best_bid = max(ex["best_bid"] for ex in valid_exchanges.values())
            best_ask = min(ex["best_ask"] for ex in valid_exchanges.values())
            
            # Find which exchanges have the best prices
            best_bid_exchange = next(name for name, data in valid_exchanges.items() 
                                   if data["best_bid"] == best_bid)
            best_ask_exchange = next(name for name, data in valid_exchanges.items()
                                   if data["best_ask"] == best_ask)
            
            self.market_data[symbol]["aggregated"] = {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "best_bid_exchange": best_bid_exchange,
                "best_ask_exchange": best_ask_exchange,
                "active_exchanges": list(valid_exchanges.keys()),
                "exchange_count": len(valid_exchanges)
            }

    def get_exchange_last_update(self, symbol:str, exchange:str):
        if not self.market_data[symbol]:
            self.logger.debug(f"Market data for {symbol} is empty")
            return None
        return self.market_data[symbol]["exchanges"][exchange]['last_updated']


    async def _publish_price_update(self, symbol: str, exchange: str, data_source: str):
        """Publish clean price update for scanners and other consumers"""
        try:
            exchange_data = self.market_data[symbol]["exchanges"][exchange]
            aggregated = self.market_data[symbol]["aggregated"]
            
            # Skip if no valid exchange data
            if not exchange_data or exchange_data["best_bid"] <= 0 or exchange_data["best_ask"] >= Decimal('inf'):
                return
            
            # Skip if no aggregated data
            if not aggregated:
                return
            
            # Calculate spreads
            exchange_spread = exchange_data["best_ask"] - exchange_data["best_bid"]
            exchange_mid = (exchange_data["best_bid"] + exchange_data["best_ask"]) / 2
            exchange_spread_bps = int((exchange_spread / exchange_mid) * 10000)
            
            aggregated_mid = (aggregated["best_bid"] + aggregated["best_ask"]) / 2
            
            # Create price update message
            price_update = PriceUpdate(
                symbol=symbol,
                exchange=exchange,
                best_bid=float(exchange_data["best_bid"]),
                best_ask=float(exchange_data["best_ask"]),
                mid_price=float(exchange_mid),
                spread=float(exchange_spread),
                spread_bps=exchange_spread_bps,
                timestamp=int(exchange_data["last_updated"] * 1000),
                aggregated_bid=float(aggregated["best_bid"]),
                aggregated_ask=float(aggregated["best_ask"]),
                aggregated_mid=float(aggregated_mid),
                is_best_bid=(exchange_data["best_bid"] == aggregated["best_bid"]),
                is_best_ask=(exchange_data["best_ask"] == aggregated["best_ask"]),
                exchange_count=aggregated["exchange_count"],
                data_source=data_source
            )
            
            # Publish to multiple topics for flexibility using new format
            topics = [
                NATSConfig.build_price_update_topic(symbol, exchange),  # Exchange-specific
                NATSConfig.pattern_symbol_price_updates(symbol),        # All exchanges for symbol
            ]
            
            for topic in topics:
                await self.mq.publish(topic, asdict(price_update))
            
            self.logger.debug(f"Published price update: {exchange}.{symbol} bid/ask={price_update.best_bid}/{price_update.best_ask}")
            
        except Exception as e:
            self.logger.error(f"Error publishing price update for {symbol}.{exchange}: {e}")

    async def _start_listening(self):
        """Start NATS subscriptions for all exchange L1 and L2 data"""
        if self._listener_tasks:
            self.logger.info("MarketData is already listening.")
            return

        # Subscribe to all exchanges L1 and L2 book updates using new wildcard patterns
        topics = [
            # NATSConfig.PATTERN_ALL_MARKET_L1_UPDATES,
            # NATSConfig.PATTERN_ALL_MARKET_L2_UPDATES,
            NATSConfig.PATTERN_ALL_LOCAL_L2_UPDATES,
            NATSConfig.PATTERN_ALL_LOCAL_L1_UPDATES,
        ]
        
        for i, topic in enumerate(topics):
            task = asyncio.create_task(
                self.mq.subscribe(topic, handler=self._message_handler),
                name=f"MarketData_Sub_{i}"
            )
            self._listener_tasks.append(task)
            self.logger.info(f"Subscribed to topic '{topic}' for multi-exchange market data.")

    async def stop(self):
        """Cancel all subscription tasks and stop data processing"""
        self.logger.info("Stopping MarketData subscription tasks.")
        self._closing = True

        for task in self._listener_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self._listener_tasks.clear()
        self.logger.info("MarketData stopped.")

    def get_mid_price(self, symbol: str) -> Optional[float]:
        """Get aggregated mid price across all exchanges (works for n=1 or n>1 exchanges)"""
        symbol = symbol.lower()
        aggregated = self.market_data.get(symbol, {}).get("aggregated")
        
        if not aggregated or not aggregated.get("best_bid") or not aggregated.get("best_ask"):
            self.logger.debug(f'No aggregated data for symbol {symbol}')
            return None
        
        best_bid = aggregated["best_bid"]
        best_ask = aggregated["best_ask"]
        
        if best_bid <= 0 or best_ask >= Decimal('inf'):
            return None
            
        return float((best_bid + best_ask) / 2)

    def get_best_bid_ask(self, symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get aggregated best bid/ask across all exchanges"""
        symbol = symbol.lower()
        aggregated = self.market_data.get(symbol, {}).get("aggregated")
        
        if not aggregated:
            return None, None
            
        return aggregated.get("best_bid"), aggregated.get("best_ask")
    
    # New multi-exchange API methods
    def get_exchange_mid_price(self, symbol: str, exchange: str) -> Optional[float]:
        """Get mid price from specific exchange"""
        symbol = symbol.lower()
        exchange = exchange.lower()
        exchange_data = self.market_data.get(symbol, {}).get("exchanges", {}).get(exchange)
        # self.logger.warning(f"Exchange {self.market_data}")
        if not exchange_data:
            return None
            
        best_bid = exchange_data["best_bid"]
        best_ask = exchange_data["best_ask"]
        
        if best_bid <= 0 or best_ask >= Decimal('inf'):
            return None
            
        return float((best_bid + best_ask) / 2)
    
    def get_exchange_best_bid_ask(self, exchange: str, symbol: str)  -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get best bid/ask from specific exchange"""
        symbol = symbol.lower()
        exchange = exchange.lower()
        
        exchange_data = self.market_data.get(symbol, {}).get("exchanges", {}).get(exchange)
        
        if not exchange_data:
            return None, None
        
        bid = exchange_data["best_bid"]
        ask = exchange_data["best_ask"]
        
        # Return None for invalid prices
        if bid <= 0 or ask <= 0 or ask >= Decimal('inf') or bid >= ask:
            return None, None
            
        return bid, ask
    
    def get_best_exchange_for_side(self, symbol: str, side: str) -> Optional[str]:
        """Get which exchange has the best price for buy/sell side"""
        symbol = symbol.lower()
        aggregated = self.market_data.get(symbol, {}).get("aggregated")
        
        if not aggregated:
            return None
            
        if side.upper() == "BUY":
            return aggregated.get("best_ask_exchange")  # Best ask for buying
        elif side.upper() == "SELL":
            return aggregated.get("best_bid_exchange")  # Best bid for selling
        else:
            return None
    
    def get_all_exchange_prices(self, symbol: str) -> Dict[str, Dict]:
        """Get prices from all exchanges for a symbol"""
        symbol = symbol.lower()
        exchanges_data = self.market_data.get(symbol, {}).get("exchanges", {})
        
        result = {}
        for exchange, data in exchanges_data.items():
            if data["best_bid"] > 0 and data["best_ask"] < Decimal('inf'):
                result[exchange] = {
                    "best_bid": float(data["best_bid"]),
                    "best_ask": float(data["best_ask"]),
                    "mid_price": float((data["best_bid"] + data["best_ask"]) / 2),
                    "last_updated": data["last_updated"]
                }
        
        return result
    
    def is_single_exchange_symbol(self, symbol: str) -> bool:
        """Check if symbol only has data from one exchange (n=1 case)"""
        symbol = symbol.lower()
        aggregated = self.market_data.get(symbol, {}).get("aggregated")
        return aggregated and aggregated.get("exchange_count", 0) == 1
    
    def get_primary_exchange(self, symbol: str) -> Optional[str]:
        """Get the primary/only exchange for a symbol (useful for n=1 case)"""
        symbol = symbol.lower()
        aggregated = self.market_data.get(symbol, {}).get("aggregated")
        
        if aggregated and aggregated.get("exchange_count", 0) == 1:
            return aggregated.get("active_exchanges", [None])[0]
        
        return None  # Multiple exchanges, no single primary
    
    def get_active_exchanges(self, symbol: str) -> List[str]:
        """Get list of exchanges that have data for this symbol"""
        symbol = symbol.lower()
        aggregated = self.market_data.get(symbol, {}).get("aggregated")
        return aggregated.get("active_exchanges", []) if aggregated else []

    # Backward compatibility methods (deprecated)
    def get_order_book(self, symbol: str) -> Optional[L2BookUpdate]:
        """Get L2 order book from primary exchange (backward compatibility)"""
        symbol = symbol.lower()
        
        # Try to get from primary exchange first
        primary_exchange = self.get_primary_exchange(symbol)
        if primary_exchange:
            exchange_data = self.market_data.get(symbol, {}).get("exchanges", {}).get(primary_exchange)
            if exchange_data and exchange_data["l2_data"]:
                return exchange_data["l2_data"]
        
        # Fallback: get from any available exchange
        exchanges_data = self.market_data.get(symbol, {}).get("exchanges", {})
        for exchange_data in exchanges_data.values():
            if exchange_data["l2_data"]:
                return exchange_data["l2_data"]
                
        return None

    def get_last_updated_at(self, symbol: str) -> Optional[float]:
        """Get last updated timestamp (from most recent exchange update)"""
        symbol = symbol.lower()
        exchanges_data = self.market_data.get(symbol, {}).get("exchanges", {})
        
        if not exchanges_data:
            return None
            
        # Return the most recent update time across all exchanges
        return max(data["last_updated"] for data in exchanges_data.values())