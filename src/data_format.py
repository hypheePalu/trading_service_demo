# dataclasses.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict
from config_enum import OrderSide, OrderType, OrderStatus, MaxOrderState, MaxOrderType, MaxOrderSide, WalletType, RunningStrategy, Exchange
from typing import Literal, List, Union
from settings import SpreadMode




@dataclass
class MarketInfo:
    is_active: bool
    base: str
    quote: str
    base_unit_precision: int
    quote_unit_precision: int
    min_base_amount: float
    min_quote_amount: float
    is_margin_supported: bool

@dataclass
class ExchangeInfo:
    MAX: Dict[str, MarketInfo]



@dataclass
class MaxOrderUpdate:
    idx: int
    side: OrderSide
    order_type: MaxOrderType
    price: Union[str, float]
    stop_price: Optional[Union[str, float]]
    average_price: Union[str, float]
    volume: Union[str, float]
    remaining_volume: Union[str, float]
    executed_volume: Union[str, float]
    state: MaxOrderState
    market: str
    trade_count: int
    order_created_ts: int
    order_updated_ts: int
    group_id: Optional[int]
    client_order_id: Optional[str]

    @staticmethod
    def from_dict(data: dict) -> "MaxOrderUpdate":
        order_status = data['S']
        if order_status == 'wait':
            state = MaxOrderState.WAIT
        elif order_status == 'done':
            state = MaxOrderState.DONE
        elif order_status == 'cancel':
            state = MaxOrderState.CANCEL
        else:
            raise ValueError(f'Unknown order status: {order_status}')

        return MaxOrderUpdate(
            idx=int(data['i']),
            side=OrderSide.BUY if data['sd']=='bid' else OrderSide.SELL,
            order_type=MaxOrderType(str(data['ot']).lower()),
            price=float(data["p"]),
            stop_price=float(data["sp"]) if data.get("sp") not in [None, ""] else None,
            average_price=float(data["ap"]),
            volume=float(data["v"]),
            remaining_volume=float(data["rv"]),
            executed_volume=float(data["ev"]),
            state=state,
            market=data["M"],
            trade_count=int(data["tc"]),
            order_created_ts=int(data["T"]),
            order_updated_ts=int(data["TU"]),
            group_id=int(data["gi"]) if data.get('gi') is not None else None,
            client_order_id=data.get("ci"),
        )



@dataclass
class MaxAccountUpdate:
    currency: str
    available: Union[str, float]
    locked: Union[str, float]
    staked: Union[str, float]
    updated_at: int

    @staticmethod
    def from_dict(data: dict) -> "MaxAccountUpdate":
        return MaxAccountUpdate(
            currency=data['cu'],
            available=float(data['av']),
            locked=float(data['l']),
            staked=float(data['stk']),
            updated_at=int(data['TU']),
        )



@dataclass
class L1BookUpdate:
    symbol: str
    exchange: str
    bid_px: float
    bid_sz: float
    ask_px: float
    ask_sz: float
    tx_timestamp: int
    event_timestamp: int # Unix timestamp
    price_received_ts: int

@dataclass
class L2BookUpdate:
    symbol: str
    exchange: str
    bids: list[list[str]]
    asks: list[list[str]]
    tx_timestamp: int
    event_timestamp: int # Unix timestamp
    price_received_ts: int
    depth: int


@dataclass
class MaxOrderBookUpdate:
    symbol: str  # market
    asks: list[[list[str]]]  # asks
    bids: list[[list[str]]]  # bids
    T: int  # created_at
    fi: int  # First update ID
    li: int  # Last update ID
    v: int  # Event version
    price_received_ts: int

@dataclass
class MaxOrderBookSnapShot:
    symbol: str  # market
    asks: list[[list[str]]]  # asks
    bids: list[[list[str]]]  # bids
    T: int  # created_at
    fi: int  # First update ID
    li: int  # Last update ID
    v: int  # Event version
    price_received_ts: int

@dataclass
class PriceUpdate:
    symbol: str
    exchange: str
    best_bid: float
    best_ask: float
    mid_price: float
    spread: float
    spread_bps: int
    timestamp: int
    aggregated_bid: float
    aggregated_ask: float
    aggregated_mid: float
    is_best_bid: bool        # Is this exchange's bid the best across all?
    is_best_ask: bool        # Is this exchange's ask the best across all?
    exchange_count: int      # How many exchanges have data for this symbol
    data_source: str         # 'l1' or 'l2'

@dataclass
class Signal:
    symbol: str
    expected_spread: float
    attempted_mid: float
    local_timestamp: int
    time_elapsed: int
    side: OrderSide
    strategy_id: RunningStrategy
    wallet_type: WalletType
    maker: bool
    price_received_ts: int
    signal_generated_ts: int
    spread_mode: float

@dataclass
class OrderRequest:
    exchange: str # "Binance" or "Backpack"
    symbol: str
    side: str     # "BUY" or "SELL"
    order_type: str # "LIMIT" or "MARKET"
    quantity: float
    client_order_id: str
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp())
    price: Optional[float] = None

@dataclass
class OrderUpdate:
    exchange: str
    symbol: str
    order_id: str
    client_order_id: str
    status: str # "NEW", "FILLED", "PARTIALLY_FILLED", "CANCELED", "REJECTED"
    filled_quantity: float
    average_price: float
    latency_ms: Optional[float] = None # For tracking latency
    update_time: float = field(default_factory=lambda: datetime.now().timestamp())


@dataclass
class Order:
    exchange: Exchange
    client_order_id: str
    side: OrderSide
    order_type: Optional[OrderType]
    price: Optional[float]
    average_price: Optional[float]
    sz: Optional[float]
    unfilled_sz: Optional[float]
    filled_sz: Optional[float]
    order_status: OrderStatus
    symbol: str
    exchange_created_ts:Optional[int]
    exchange_updated_ts:Optional[int]
    order_created_ts: int
    order_updated_ts: int
    price_received_ts: int
    signal_generated_ts: int
    attempted_mid: float

    def update(self,**kwargs):
        for k,v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                raise AttributeError(f"Attribute '{k}' not found.")

@dataclass
class PositionUpdate:
    exchange: str
    symbol: str
    current_position: float
    available_balance: float
    maintenance_margin_rate: Optional[float] = None # Maintenance margin rate if applicable
    update_time: float = field(default_factory=lambda: datetime.now().timestamp())

@dataclass
class DBRecord:
    collection: str
    data: Dict
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp())


##### BinanceBookTicker