from src.config_enum import RunningStrategy
from dataclasses import field, dataclass
from typing import Dict
from enum import Enum


class SpreadMode(Enum):
    PAR: float = 0.00002
    TIGHT: float = 0.0001
    NORMAL: float = 0.0007
    WIDE: float = 0.001


@dataclass
class StrategyConfig:
    ALLOCATION: Dict[RunningStrategy, float] = field(default_factory=dict)
    TICKET_SIZE: Dict[RunningStrategy, float] = field(default_factory=dict)

@dataclass 
class OrderConfig:
    MAKER_SPREAD: float = 0.001  # 0.1% spread for maker orders
    TAKER_SPREAD: float = 0.001  # 0.1% maximum spread for taker orders
    ORDER_EXPIRE_SECONDS: int = 10  # 5 minutes order expiration
    CLEANUP_INTERVAL_SECONDS: int = 30  # Cleanup task interval

@dataclass
class TestScannerConfig:
    """Configuration for TestScanner - simple alternating bid/ask strategy"""
    TARGET_SYMBOL: str = "usdttwd"  # Target trading pair
    SPREAD_MODE: SpreadMode = SpreadMode.WIDE  # Always use WIDE spread
    SIGNAL_INTERVAL_SECONDS: int = 30  # Generate signal every 30 seconds
    WALLET_TYPE: str = "SPOT"  # Use SPOT wallet for testing
    STRATEGY_ID: str = "TEST"  # Strategy identifier
    MAKER_ORDERS: bool = True  # Use maker orders for better execution
    ENABLE_ALTERNATING: bool = True  # Alternate between BUY and SELL
    MAX_DAILY_SIGNALS: int = 100  # Limit signals per day for safety

@dataclass
class TxFeeConfig:
    MAKER_FEE: float = 0.0003
    TAKER_FEE: float = 0.0015


STRATEGY_CONFIG = StrategyConfig(
    ALLOCATION={
        RunningStrategy.ARBITRAGE.value: 100000,
        RunningStrategy.TEST.value: 1000  # Small allocation for testing
    },
    TICKET_SIZE={
        RunningStrategy.ARBITRAGE.value: 12,
        RunningStrategy.TEST.value: 10  # Small ticket size for testing
    },
)

ORDER_CONFIG = OrderConfig()
TEST_SCANNER_CONFIG = TestScannerConfig()

# if __name__ == "__main__":
    # print(SpreadMode.NORMAL.value)