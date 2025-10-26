from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from src.config_enum import Exchange

# NATS server configuration
NATS_SERVERS = ["nats://localhost:4222"] # Your NATS server address


class NATSConfig:

    # domain
    DOMAIN_EXCHANGE = "exchange"
    DOMAIN_STRATEGY = "strategy"


    # exchange
    EXCHANGE_MAX = Exchange.MAX.value
    EXCHANGE_BINANCE= Exchange.BINANCE.value
    EXCHANGE_BACKPACK = Exchange.BACKPACK.value
    EXCHANGE_TAIWAN_BANK = Exchange.REFERENCE_SOURCE.value

    # data type
    DATA_TYPE_MARKET= 'market'
    DATA_TYPE_USER = 'user'
    DATA_TYPE_LOCAL = 'local'
    DATA_TYPE_ARB= 'arb'
    DATA_TYPE_STRATEGY = 'strategy'


    # market type
    MARKET_TYPE_SPOT = 'spot'
    MARKET_TYPE_FUTURE = 'future'

    # subtype
    SUB_TYPE_ORDER= 'order'
    SUB_TYPE_L1_BOOK = 'l1Book'
    SUB_TYPE_L2_BOOK = 'l2Book'
    SUB_TYPE_ACCOUNT = 'account'
    SUB_TYPE_RECONNECT = 'reconnect'
    SUB_TYPE_SIGNAL = 'signal'
    SUB_TYPE_PRICE = 'price'

    # EVENT_TYPE
    EVENT_TYPE_UPDATE = 'update'
    EVENT_TYPE_SNAPSHOT = 'snapshot'
    EVENT_TYPE_REQUEST = 'request'
    EVENT_TYPE_DETECT = 'detect'

    # Topic builders for new {symbol}.{exchange} format
    @staticmethod
    def build_market_l1_topic(symbol: str, exchange: str):
        """Build L1 book update topic: market.spot.l1Book.update.{symbol}.{exchange}"""
        return f'{NATSConfig.DATA_TYPE_MARKET}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_L1_BOOK}.{NATSConfig.EVENT_TYPE_UPDATE}.{symbol.lower()}.{exchange.lower()}'
    
    @staticmethod 
    def build_market_l2_update_topic(symbol: str, exchange: str):
        """Build L2 book update topic: market.spot.l2Book.update.{symbol}.{exchange}"""
        return f'{NATSConfig.DATA_TYPE_MARKET}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_L2_BOOK}.{NATSConfig.EVENT_TYPE_UPDATE}.{symbol.lower()}.{exchange.lower()}'

    @staticmethod
    def build_market_l2_snapshot_topic(symbol: str, exchange: str):
        """Build L2 book update topic: market.spot.l2Book.update.{symbol}.{exchange}"""
        return f'{NATSConfig.DATA_TYPE_MARKET}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_L2_BOOK}.{NATSConfig.EVENT_TYPE_SNAPSHOT}.{symbol.lower()}.{exchange.lower()}'
    
    @staticmethod
    def build_local_l1_topic(symbol: str, exchange: str):
        """Build local book update topic: local.spot.l1Book.update.{symbol}.{exchange}"""
        return f'{NATSConfig.DATA_TYPE_LOCAL}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_L1_BOOK}.{NATSConfig.EVENT_TYPE_UPDATE}.{symbol.lower()}.{exchange.lower()}'
    @staticmethod
    def build_local_l2_topic(symbol: str, exchange: str):
        """Build local book update topic: local.spot.l2Book.update.{symbol}.{exchange}"""
        return f'{NATSConfig.DATA_TYPE_LOCAL}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_L2_BOOK}.{NATSConfig.EVENT_TYPE_UPDATE}.{symbol.lower()}.{exchange.lower()}'


    @staticmethod
    def build_price_update_topic(symbol: str, exchange: str):
        """Build price update topic: market.spot.price.update.{symbol}.{exchange}"""
        return f'{NATSConfig.DATA_TYPE_MARKET}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_PRICE}.{NATSConfig.EVENT_TYPE_UPDATE}.{symbol.lower()}.{exchange.lower()}'
    
    @staticmethod
    def build_strategy_signal_topic(symbol: str, exchange: str, strategy: str = 'test'):
        """Build strategy signal topic: strategy.spot.signal.detect.{symbol}.{exchange}"""
        return f'{NATSConfig.DATA_TYPE_STRATEGY}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_SIGNAL}.{NATSConfig.EVENT_TYPE_DETECT}.{symbol.lower()}.{exchange.lower()}.{strategy}'
    
    @staticmethod
    def build_user_order_topic(symbol: str, exchange: str):
        """Build user order update topic: user.spot.order.update.{symbol}.{exchange}"""
        return f'{NATSConfig.DATA_TYPE_USER}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_ORDER}.{NATSConfig.EVENT_TYPE_UPDATE}.{symbol.lower()}.{exchange.lower()}'
    
    @staticmethod
    def build_user_order_snapshot_topic(symbol: str, exchange: str):
        """Build user order snapshot topic: user.spot.order.snapshot.{symbol}.{exchange}"""
        return f'{NATSConfig.DATA_TYPE_USER}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_ORDER}.{NATSConfig.EVENT_TYPE_SNAPSHOT}.{symbol.lower()}.{exchange.lower()}'

    # Wildcard patterns for subscriptions
    PATTERN_ALL_MARKET_L1_UPDATES = f'{DATA_TYPE_MARKET}.{MARKET_TYPE_SPOT}.{SUB_TYPE_L1_BOOK}.{EVENT_TYPE_UPDATE}.*.*'
    PATTERN_ALL_MARKET_L2_UPDATES = f'{DATA_TYPE_MARKET}.{MARKET_TYPE_SPOT}.{SUB_TYPE_L2_BOOK}.{EVENT_TYPE_UPDATE}.*.*'
    PATTERN_ALL_MARKET_L2_SNAPSHOTS = f'{DATA_TYPE_MARKET}.{MARKET_TYPE_SPOT}.{SUB_TYPE_L2_BOOK}.{EVENT_TYPE_SNAPSHOT}.*.*'
    PATTERN_ALL_LOCAL_L2_UPDATES = f'{DATA_TYPE_LOCAL }.{MARKET_TYPE_SPOT}.{SUB_TYPE_L2_BOOK}.{EVENT_TYPE_UPDATE}.*.*'
    PATTERN_ALL_LOCAL_L1_UPDATES = f'{DATA_TYPE_LOCAL}.{MARKET_TYPE_SPOT}.{SUB_TYPE_L1_BOOK}.{EVENT_TYPE_UPDATE}.*.*'
    PATTERN_ALL_PRICE_UPDATES = f'{DATA_TYPE_MARKET}.{MARKET_TYPE_SPOT}.{SUB_TYPE_PRICE}.{EVENT_TYPE_UPDATE}.*.*'
    PATTERN_ALL_USER_ORDER_UPDATES = f'{DATA_TYPE_USER}.{MARKET_TYPE_SPOT}.{SUB_TYPE_ORDER}.{EVENT_TYPE_UPDATE}.*.*'
    PATTERN_ALL_STRATEGY_SIGNALS = f'{DATA_TYPE_STRATEGY}.{MARKET_TYPE_SPOT}.{SUB_TYPE_SIGNAL}.{EVENT_TYPE_DETECT}.*.*.*'
    PATTERN_ALL_FUTURE_L1_UPDATES = f'{DATA_TYPE_MARKET}.{MARKET_TYPE_FUTURE}.{SUB_TYPE_L1_BOOK}.{EVENT_TYPE_UPDATE}.*.*'
    
    # Symbol-specific patterns
    @staticmethod
    def pattern_symbol_price_updates(symbol: str):
        """Pattern for all exchanges for a symbol: market.spot.price.update.{symbol}.*"""
        return f'{NATSConfig.DATA_TYPE_MARKET}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_PRICE}.{NATSConfig.EVENT_TYPE_UPDATE}.{symbol.lower()}.*'
    
    @staticmethod
    def pattern_exchange_market_l2_updates(exchange: str):
        return f'{NATSConfig.DATA_TYPE_MARKET}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_L2_BOOK}.{NATSConfig.EVENT_TYPE_UPDATE}.*.{exchange.lower()}'

    @staticmethod
    def pattern_exchange_market_l2_snapshot(exchange: str):
        return f'{NATSConfig.DATA_TYPE_MARKET}.{NATSConfig.MARKET_TYPE_SPOT}.{NATSConfig.SUB_TYPE_L2_BOOK}.{NATSConfig.EVENT_TYPE_SNAPSHOT}.*.{exchange.lower()}'

    # Legacy topics (deprecated - for backward compatibility)
    TOPIC_MAX_SPOT_L2_BOOK_UPDATE = f'{DOMAIN_EXCHANGE}.{EXCHANGE_MAX}.{DATA_TYPE_MARKET}.{MARKET_TYPE_SPOT}.{SUB_TYPE_L2_BOOK}.{EVENT_TYPE_UPDATE}'
    TOPIC_MAX_SPOT_LOCAL_BOOK_UPDATE = f'{DOMAIN_EXCHANGE}.{EXCHANGE_MAX}.{DATA_TYPE_LOCAL}.{MARKET_TYPE_SPOT}.{SUB_TYPE_L2_BOOK}.{EVENT_TYPE_UPDATE}'
    TOPIC_MAX_USER_SPOT_ORDER_UPDATE = f'{DOMAIN_EXCHANGE}.{EXCHANGE_MAX}.{DATA_TYPE_USER}.{MARKET_TYPE_SPOT}.{SUB_TYPE_ORDER}.{EVENT_TYPE_UPDATE}'
    
    # Legacy strategy topics
    TOPIC_STRATEGY_MAX_ARB_SIGNAL_DETECT = f'{DOMAIN_STRATEGY}.{EXCHANGE_MAX}.{DATA_TYPE_ARB}.{MARKET_TYPE_SPOT}.{SUB_TYPE_SIGNAL}.{EVENT_TYPE_DETECT}'
    TOPIC_STRATEGY_MAX_TEST_SIGNAL_DETECT = f'{DOMAIN_STRATEGY}.{EXCHANGE_MAX}.test.{MARKET_TYPE_SPOT}.{SUB_TYPE_SIGNAL}.{EVENT_TYPE_DETECT}'
    
    # Reconnect topics
    TOPIC_MAX_MARKET_SPOT_RECONNECT = f'{DOMAIN_EXCHANGE}.{EXCHANGE_MAX}.{DATA_TYPE_MARKET}.{MARKET_TYPE_SPOT}.{SUB_TYPE_RECONNECT}'
    TOPIC_MAX_USER_SPOT_RECONNECT = f'{DOMAIN_EXCHANGE}.{EXCHANGE_MAX}.{DATA_TYPE_USER}.{MARKET_TYPE_SPOT}.{SUB_TYPE_RECONNECT}'
    
    # User data topics
    TOPIC_MAX_USER_SPOT_ORDER_SNAPSHOT = f'{DOMAIN_EXCHANGE}.{EXCHANGE_MAX}.{DATA_TYPE_USER}.{MARKET_TYPE_SPOT}.{SUB_TYPE_ORDER}.{EVENT_TYPE_SNAPSHOT}'
    TOPIC_MAX_USER_SPOT_ACCOUNT = f'{DOMAIN_EXCHANGE}.{EXCHANGE_MAX}.{DATA_TYPE_USER}.{MARKET_TYPE_SPOT}.{SUB_TYPE_ACCOUNT}'

    # user data reconnect


    # LOCAL BOOK UPDATE



# NATS JetStream Streams (persistence) and related topic definitions
# Note: One Stream can contain multiple related topics
# Stream names are usually uppercase, representing a group of data flows
# Stream subjects configuration usually uses wildcards, e.g. "HFT_ORDERS.*"
JETSTREAM_STREAM_ORDERS = "HFT_ORDERS"
JETSTREAM_SUBJECT_ORDER_REQUESTS = "order.requests" # Belongs to HFT_ORDERS Stream
JETSTREAM_SUBJECT_ORDER_UPDATES = "order.updates"   # Belongs to HFT_ORDERS Stream

JETSTREAM_STREAM_POSITIONS = "HFT_POSITIONS"
JETSTREAM_SUBJECT_POSITION_UPDATES = "position.updates" # Belongs to HFT_POSITIONS Stream

# *_USDC_PERP
BACK_PACK_SYMBOLS = ['']
BINANCE_SYMBOLS = ['']







class DatabaseSettings(BaseSettings):
    # pydantic_settings configuration
    # This tells BaseSettings to load variables from .env file
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore' # Ignore variables in .env not defined in class, avoid warnings
    )

    # Define your database connection parameters
    DO_USER: str
    DO_PASSWORD: str
    DO_HOST: str
    DO_DB: str

    # Optional: you can define a property to generate complete MongoDB connection URL
    @property
    def mongo_url(self) -> str:
        # Ensure special characters in password (like @ : /) are properly encoded
        # But for common passwords, usually no extra encoding needed
        return f"mongodb+srv://{self.DO_USER}:{self.DO_PASSWORD}@{self.DO_HOST}/{self.DO_DB}?tls=true&authSource=admin&replicaSet=db-mongodb-sgp1-43703"


class PostgresSetting(BaseSettings):
    # pydantic_settings configuration
    # This tells BaseSettings to load variables from .env file
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore' # Ignore variables in .env not defined in class, avoid warnings
    )

    # Define your database connection parameters
    PG_HOST: str
    PG_PORT: int
    PG_USER: str
    PG_PASSWORD: Optional[str]
    PG_NAME: str
    PG_MIN_CONN: int
    PG_MAX_CONN: int

    # Optional: you can define a property to generate complete MongoDB connection URL

class MaxSetting(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

    MAX_API_KEY: str
    MAX_SECRET_KEY: str

# Instantiate this settings class, it will automatically load from .env
# Can directly import this instance from here in other modules
# settings = DatabaseSettings()
pg_settings = PostgresSetting()
max_setting = MaxSetting()


if __name__ == '__main__':
    print(settings.mongo_url)
    print(settings.DO_USER)
    print(settings.DO_PASSWORD)