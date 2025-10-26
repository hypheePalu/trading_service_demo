from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum


class RunningStrategy(str, Enum):
    ARBITRAGE = 'arb'
    TEST = 'test'


class StrategyState(str, Enum):
    IDLE = 'IDLE'
    LOCKED = 'LOCKED'

class Exchange(str, Enum):
    BINANCE = 'BINANCE'
    MAX = 'MAX'
    BACKPACK = 'BACKPACK'
    REFERENCE_SOURCE = 'REFERENCE_SOURCE'


class OrderSide(str, Enum):
    BUY = 'BUY'
    SELL = 'SELL'


class OrderStatus(str, Enum):
    PENDING = 'PENDING'
    NEW = 'NEW'
    PARTIALLY_FILLED = 'PARTIALLY_FILLED'
    FILLED = 'FILLED'
    CANCELED = 'CANCELED'
    REJECTED = 'REJECTED'
    EXPIRED = 'EXPIRED'

class OrderType(str, Enum):
    LIMIT = 'LIMIT'
    MARKET = 'MARKET'
    POST_ONLY = 'POST_ONLY'
    STOP_LIMIT = 'STOP_LIMIT'
    STOP_MARKET = 'STOP_MARKET'
    IOC_LIMIT = 'IOC_LIMIT'
    NEW = 'NEW'
    PENDING = 'PENDING'

class WalletType(str, Enum):
    SPOT = 'SPOT'
    M = 'M'


@dataclass
class Order:
    # --- 訂單基本信息 (通常在下單時確定) ---
    order_id: str # 交易所返回的訂單 ID
    client_order_id: str # 我們自己生成的訂單 ID，必須提供
    symbol: str
    side: OrderSide
    order_type: OrderType
    initial_price: Decimal # 下單時的價格
    initial_amount: Decimal # 下單時的數量

    # --- 訂單狀態和成交信息 (會動態更新) ---
    # --- 時間相關信息 ---
    placed_time: int # 下單時間（本地時間，使用 UTC）
    last_update_time: int # 最後一次狀態更新時間
    latency_on_place: int # 下單時的延遲

    # --- 策略相關信息 ---
    reference_price: Decimal # 下單時的市場參考價 (e.g., mid-price)
    expiration_time: int = field(init=False) # 訂單的最長存續期間
    filled_amount: Decimal = Decimal(0) # 已成交數量
    filled_price: Decimal= Decimal(0)# 加權平均成交價格 (WAP)
    status: OrderStatus = OrderStatus.PENDING # 當前訂單狀態
    # --- 延遲紀錄 (可選，根據交易所提供信息完善) ---
    exchange_placed_time: datetime = field(default=None) # 交易所確認訂單的時間

    remaining_amount: Decimal = field(init=False) # 剩餘未成交數量，由 initial_amount - filled_amount 計算

    def __post_init__(self):
        """
        dataclass 特有的方法，在 __init__ 之後調用。
        用於初始化那些依賴其他字段的字段，例如 remaining_amount 和 expiration_time。
        """
        self.remaining_amount = self.initial_amount - self.filled_amount
        # 假設 MaxOrderManager 會設定這個 TTL，這裡先用一個默認值或在 Manager 中傳入
        # 這裡為了簡化，先假設一個固定的 TTL，實際應由 Manager 控制
        self.expiration_time = self.placed_time + 3000 # 默認 300 秒 TTL

    def update_expiration_time(self, expiration_ts: int):
        self.expiration_time = self.placed_time + expiration_ts

    def update_status(self, new_status: OrderStatus,
                      new_filled_amount: Decimal= None, # 這次更新的累計成交量
                      new_filled_price: Decimal = None,  # 這次更新的累計加權平均成交價
                      update_ts: int= None) -> bool:
        """
        根據交易所推送更新訂單狀態。
        返回 True 表示訂單已終結，False 表示仍在活動中。
        """
        old_filled_amount = self.filled_amount

        # 僅在有新的成交量數據時更新 filled_amount 和 filled_price
        if new_filled_amount is not None and new_filled_amount > self.filled_amount:
            # 計算新的加權平均成交價 (處理增量成交)
            if self.filled_amount > 0:
                # 累計新的成交量和價格來計算新的加權平均
                total_value_old = self.filled_amount * self.filled_price
                total_value_new_fill = (new_filled_amount - self.filled_amount) * (new_filled_price if new_filled_price is not None else 0.0)
                self.filled_price = (total_value_old + total_value_new_fill) / new_filled_amount
            else:
                # 第一次成交或從未成交到成交
                self.filled_price = new_filled_price if new_filled_price is not None else 0.0

            self.filled_amount = new_filled_amount
            self.remaining_amount = self.initial_amount - self.filled_amount

        self.status = new_status
        self.last_update_time = update_ts if update_ts else datetime.now().timestamp() * 1000

        # 判斷訂單是否終結
        if self.status in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]:
            return True
        return False

    def is_active(self) -> bool:
        """判斷訂單是否仍在活動中 (未終結)。"""
        return self.status not in [OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED]

    def time_since_placed(self) -> float:
        """計算下單至今的時間。"""
        return datetime.now().timestamp()*1000 - self.placed_time

    def get_fill_slippage(self) -> Decimal:
        """
        計算成交滑點。
        這裡只是簡化示範，實際滑點計算可能更複雜，例如考慮掛單價、市場中點價等。
        """
        if (self.status == OrderStatus.FILLED and self.initial_price != 0) or \
            (self.status == OrderStatus.CANCELED and self.initial_price == 0 and self.filled_amount != Decimal(0))  :
            # 例如：對於買單，滑點 = (成交價 - 掛單價) / 掛單價
            # 對於賣單，滑點 = (掛單價 - 成交價) / 掛單價
            if self.side == OrderSide.BUY:
                return (self.filled_price - self.initial_price) / self.initial_price
            elif self.side == OrderSide.SELL:
                return (self.initial_price - self.filled_price) / self.initial_price
        return Decimal(0)


# MAX

import json # 僅用於示範 json 轉換

# --- 定義必要的 Enum 類別 ---
# walletType 可以直接用字符串，但如果未來選擇數量多，也可以轉成 Enum
class MaxWalletType(str, Enum):
    SPOT = 'spot'
    M = 'm' # Max 的合約帳戶可能是 'm'

class MaxOrderSide(str, Enum):
    BUY = 'buy'
    SELL = 'sell'

class MaxOrderState(str, Enum):
    WAIT = 'wait'
    DONE = 'done'
    CANCEL = 'cancel'
    CONVERT = 'convert' # 轉換中，通常是內部訂單類型

# Max API 可能返回的訂單類型字符串，根據實際情況擴展
class MaxOrderType(str, Enum):
    LIMIT = 'limit'
    MARKET = 'market'
    STOP_LIMIT = 'stop_limit'
    STOP_MARKET = 'stop_market'
    POST_ONLY = 'post_only'
    # ... 其他可能的類型

# --- Order 資料類別 ---
@dataclass
class MaxOrder:
    id: int
    walletType: MaxWalletType
    market: str
    clientOid: str | None # Max API 文檔顯示可以是 string 或 null
    groupId: int | None
    side: OrderSide
    state: MaxOrderState
    ordType: MaxOrderType # 這裡我將其改為 OrderType Enum
    price: Decimal | None
    stopPrice: Decimal | None
    avgPrice: Decimal
    volume: Decimal
    remainingVolume: Decimal
    executedVolume: Decimal
    tradesCount: int
    createdAt: datetime
    updatedAt: datetime

    @classmethod
    def from_json(cls, data: dict):
        """
        從 JSON 字典自動拆解並填充 MaxOrder 實例。
        處理 Decimal 和 datetime 字符串轉換。
        """
        # 創建一個可變的字典副本，避免修改原始輸入
        parsed_data = data.copy()

        # 處理 Enum 類型
        parsed_data['walletType'] = MaxWalletType(parsed_data['walletType'])
        parsed_data['side'] = MaxOrderSide(parsed_data['side'])
        parsed_data['state'] = MaxOrderState(parsed_data['state'])
        parsed_data['ordType'] = MaxOrderType(parsed_data['ordType']) # 假設 Max 的 ordType 字符串與你的 Enum 值一致

        # 處理 Decimal 類型：將字符串轉換為 Decimal 對象
        # 檢查是否存在且不是 None，因為 Decimal('null') 會報錯
        for key in ['price', 'stopPrice', 'avgPrice', 'volume', 'remainingVolume', 'executedVolume']:
            if key in parsed_data and parsed_data[key] is not None:
                parsed_data[key] = Decimal(str(parsed_data[key])) # 確保是字符串再轉換

        if 'createdAt' in parsed_data and parsed_data['createdAt'] is not None:
            parsed_data['createdAt'] = float(parsed_data['createdAt'])
        if 'updatedAt' in parsed_data and parsed_data['updatedAt'] is not None:
            parsed_data['updatedAt'] =  float(parsed_data['updatedAt'])

        # 根據處理後的字典創建 dataclass 實例
        return cls(**parsed_data)