# Arbitrage Trading Python Project - DEMO

## DISCLAIMER

**IMPORTANT: This is a demonstration project only.**

- This project does **NOT** reflect any information about my production setup
- This is the POC version. It means there will be bugs, confusing namings and far away from production. 
- The code here is for **demonstration and educational purposes only**
- **NO API keys, credentials, or production data** are included
- This is **NOT financial advice** 


**NO WARRANTY**: This software is provided "as is" without warranty of any kind, express or implied.  

## Order Naming Format
{strategy_id}_{wallet_type}_{ms_timestamp}

## Quick Demo

### Running the Self-Contained Demo

The easiest way to see the system in action is to run the **self-contained demo** that requires **NO external dependencies**:

```bash
# Navigate to the project directory
cd {PATH_TO_DIR}/trading_service_demo

# Run the demo (requires only Python 3.8+)
python3 demo_hft_system.py
```

**What the demo shows:**
- Real-time market data simulation from multiple exchanges (Binance, MAX)
- EWMA-based arbitrage signal detection
- Order execution with latency tracking (50-200ms)
- Position management across strategies
- Complete statistics after 30 seconds

**Demo Features:**
- No NATS server required (uses in-memory mock)
- No database required
- No API keys required
- No external packages beyond Python stdlib
- Fully self-contained demonstration

The demo will run for 30 seconds and display live trading activity including market data updates, signal generation, order execution, and final statistics.

---

## Project Overview

This project demonstrates a **production-grade trading system** for cryptocurrency front-run arbitrage across multiple exchanges. The system uses an asynchronous event-driven model, leveraging `asyncio` for non-blocking operations, and integrates data and trading functionality from multiple exchanges (currently supporting Binance, MAX).

**Key Technologies:**
- **Async I/O**: `asyncio` for concurrent non-blocking operations
- **Message Queue**: NATS with JetStream for inter-component communication
- **Data Format**: Strongly-typed `dataclass` definitions for all messages
- **Databases**: PostgreSQL for time-series data, MongoDB for market data
- **API Integration**: REST and WebSocket connections to exchanges

---

## System Architecture

### High-Level Data Flow

```
┌─────────────────────┐
│  Exchange APIs      │
│  (WebSocket/REST)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Data Feeds        │ │
│  - MAX Spot         │
│  - Reference Rate   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Message Queue      │
│     (NATS)          │
└──────────┬──────────┘
           │
           ├──────────────────────┬
           ▼                      ▼                  
┌──────────────────┐     ┌──────────────────┐   
│  Book Manager    │     │  Arb Scanner     │  
│  (Local L2)      │-->  │  (EWMA Signals)  │  
└──────────────────┘     └────────┬─────────┘  
                                  │
                                  ▼
                        ┌──────────────────────┐
                        │   Order Manager      │
                        │   (Execution)        │
                        └──────────┬───────────┘
                                   │
                                   ▼
                        ┌──────────────────────┐
                        │  Position Manager    │
                        │  (Risk Management)   │
                        └──────────────────────┘
```

---

## Module Documentation

### Core Production Modules

#### 1. **msg_queue.py** - Message Queue Abstraction
**Purpose**: Singleton wrapper around NATS message queue for pub/sub messaging

**Functionality:**
- Establishes connection to NATS server with JetStream support
- Provides async publish/subscribe interfaces
- Handles JSON serialization/deserialization of dataclass messages
- Connection diagnostics and health monitoring
- Automatic reconnection logic

**Key Methods:**
- `publish(topic, message)` - Publish message to NATS topic
- `subscribe(topic, handler)` - Subscribe to topic with callback handler
- `get_connection_status()` - Get connection diagnostics

**Used in demo**: No (uses mock in-memory queue)

---

#### 2. **data_feeds.py** - Exchange WebSocket Connections
**Purpose**: Real-time market data ingestion from multiple exchanges

**Functionality:**
- **MaxDataFeed**: Connects to MAX Exchange WebSocket for spot market data
- **ReferenceRateDataFeed**: Fetches USD/TWD reference rates from external reference source
- Automatic reconnection on disconnect
- 24-hour connection rotation (exchange best practice)
- Publishes normalized L1/L2 book updates to NATS

**Message Flow:**
```
Exchange WebSocket → Parse → L1BookUpdate/L2BookUpdate → NATS Topic
```

**Used in demo**: Simulated with MockDataFeed

---

#### 3. **book_manager.py** - Local Order Book Management
**Purpose**: Maintain accurate local order books from exchange updates

**Functionality:**
- **MaxBookManager**: Manages local L2 order books for MAX exchange
- Handles snapshot + delta updates
- Event buffering and synchronization
- Publishes consolidated book updates
- Bid/ask price tracking

**Why needed**: Reduces API calls and latency by maintaining local state

**Used in demo**: No (demo uses direct price feeds)

---

#### 4. **arb_scanner.py** - Arbitrage Signal Generation
**Purpose**: Detect trading opportunities using statistical analysis

**Functionality:**

**StableScanner** (Stablecoin arbitrage):
- Monitors USDT/TWD premium between MAX and external reference rate provider
- Detects opportunities when exchange rate deviates from reference
- Position-aware signal generation
- Time-based controls (trading hours, cooldown periods)

**Used in demo**: Yes (simplified version in demo_hft_system.py)

---

#### 5. **order_manager.py** - Order Execution & Lifecycle
**Purpose**: Execute trades and manage order lifecycle

**Functionality:**
- **MaxOrderManager**: Handles order placement/cancellation on MAX exchange
- Listens for trading signals from NATS
- Strategy state management (prevents concurrent orders per symbol)
- Unfilled order monitoring and cancellation
- Latency tracking (signal → order fill)
- Order reconciliation with exchange
- Integration with position manager for risk checks

**Order Flow:**
```
Signal → Validate Position → Place Order → Monitor Fill → Update Position
```

**Used in demo**: Yes (simplified mock execution)

---

#### 6. **position_manager.py** - Position & Risk Management
**Purpose**: Track positions and enforce risk limits

**Functionality:**
- **MaxPositionManager**: Tracks positions per strategy and wallet
- Fetches current balances from exchange API
- Strategy allocation limits (prevents over-concentration)
- Portfolio-level risk management
- Position updates from order fills
- Real-time P&L calculation (if market data available)

**Risk Checks:**
- Maximum position size per strategy
- Total portfolio exposure limits
- Prevent orders that would exceed limits

**Used in demo**: Yes (simplified tracking)

---

#### 7. **api_client.py** - Exchange REST API Client
**Purpose**: HTTP client for MAX Exchange REST API

**Functionality:**
- Authentication (HMAC-SHA256 signatures)
- Order placement (market/limit orders)
- Order cancellation
- Balance queries
- Order status queries
- Market info (tick sizes, min quantities)
- Rate limiting and retry logic

**Security**: API keys loaded from environment variables

**Used in demo**: No (mock API responses)

---

#### 8. **market_data.py** - Aggregated Market Data
**Purpose**: Centralized singleton for accessing latest market prices

**Functionality:**
- Subscribes to all L1/L2 book updates from NATS
- Maintains in-memory cache of latest prices per exchange/symbol
- Provides convenient `get_exchange_mid_price()` interface
- Used by scanners and position manager for price lookups

**Pattern**: Singleton ensures single source of truth

**Used in demo**: Simulated

---

#### 9. **private_data_feed.py** - Private WebSocket Feeds
**Purpose**: Real-time order and account updates from exchange

**Functionality:**
- **MaxOrderDataFeed**: Authenticated WebSocket for MAX private channel
- Receives order fill notifications
- Account balance updates
- Publishes to NATS for order manager consumption

**Authentication**: JWT token with HMAC signature

**Used in demo**: No

___

#### 10. **order_reconciler.py** - Order State Reconciliation
**Purpose**: Reconcile order states between local tracking and exchange

**Functionality:**
- Detects discrepancies between local and exchange order states
- Handles edge cases (network failures, partial fills)
- Position adjustments based on actual fills
- Ensures consistency after reconnections

**Used in demo**: No

---

### Supporting Modules

#### 11. **config_enum.py** - Type Definitions
**Purpose**: Enums and dataclasses for type safety

**Definitions:**
- `Exchange`: BINANCE, MAX, BACKPACK, REFERENCE_SOURCE
- `OrderSide`: BUY, SELL
- `OrderStatus`: NEW, FILLED, PARTIALLY_FILLED, CANCELLED
- `WalletType`: SPOT, MARGIN
- `RunningStrategy`: ARBITRAGE, STABLE_COIN, TEST

---

#### 12. **data_format.py** - Message Schemas
**Purpose**: Dataclass definitions for all inter-component messages

**Key Dataclasses:**
- `L1BookUpdate`: Best bid/ask with size
- `L2BookUpdate`: Full order book snapshot
- `Signal`: Trading signal with spread, price, strategy
- `Order`: Order representation with lifecycle
- `Position`: Position tracking per strategy

---

#### 13. **settings.py** - Strategy Configuration
**Purpose**: Strategy-specific parameters and limits

**Configuration:**
- Strategy allocations (max position per strategy)
- Order parameters (spread modes, expiration times)
- Transaction fee configurations
- Risk limits

---

#### 14. **system_config.py** - System Configuration
**Purpose**: NATS topics, database connections, API endpoints

**Configuration:**
- NATS server addresses
- Topic naming conventions
- Database connection strings (via environment)
- Exchange API endpoints

---

#### 15. **logging_config.py** - Logging Setup
**Purpose**: Centralized logging configuration from YAML

**Features:**
- Colored console output
- File rotation (daily, 30-day retention)
- Per-module log levels
- Separate signal logging

---

### Utility Modules

#### 16. **max_utilis.py** - MAX Exchange Utilities
**Purpose**: Helper functions for MAX exchange data

**Functionality:**
- K-line (candlestick) data parsing
- Historical data fetching and formatting

---

#### 17. **reference_rate_provider.py** - Reference Rate Provider
**Purpose**: Fetch USD/TWD reference rates from external source

**Functionality:**
- Web scraping external reference source for exchange rates
- Caches daily rates
- Provides fallback for missing data
- Used by stablecoin arbitrage scanner

---

## Running the Production System

**Prerequisites:**
1. NATS Server running on `localhost:4222`
2. PostgreSQL database (optional, for data persistence)
3. MongoDB database (optional, for market data)
4. Exchange API credentials in `.env` file

**Setup:**
```bash
# Install dependencies
poetry install

# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Start NATS server (in separate terminal)
nats-server

# Run the system
python src/main.py
```

**What runs in production:**
- Binance Futures data feed
- MAX Spot data feed
- Reference rate USD/TWD fetcher
- Order book manager
- Stable arbitrage scanner
- Order manager
- Position manager
- Data recorders (if enabled)

---

## Configuration

Create a `.env` file with your credentials:

```bash
# MAX Exchange API
MAX_API_KEY=your_max_api_key
MAX_SECRET_KEY=your_max_secret_key

# PostgreSQL Database
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=your_password
PG_NAME=hft_db
PG_MIN_CONN=5
PG_MAX_CONN=20

# MongoDB (optional)
DO_USER=mongo_user
DO_PASSWORD=mongo_password
DO_HOST=localhost
DO_DB=market_data
```

---

## Key Features & Technical Highlights

### Production-Grade Patterns

**1. Event-Driven Architecture**
- Fully decoupled components via NATS message queue
- Scalable pub/sub pattern for inter-component communication
- Each component can be scaled independently

**2. Type Safety**
- All messages use strongly-typed `dataclass` definitions
- Enums for order states, exchanges, strategies
- Prevents runtime errors from malformed data

**3. Async I/O**
- Non-blocking operations using `asyncio`
- Concurrent WebSocket connections to multiple exchanges
- Maximizes throughput on single-threaded Python

**4. Singleton Pattern**
- `AsyncMessageQueue`: Single connection to NATS
- `MarketData`: Single source of truth for prices
- `TimeSyncManager`: Centralized clock synchronization

**5. Error Handling**
- Automatic reconnection for WebSocket feeds
- Graceful degradation on exchange API failures
- Comprehensive logging for debugging

**6. Observability**
- Structured logging with per-module log levels
- Colored console output for development
- File-based logging with rotation for production
- Separate signal logging for strategy analysis

### Performance Considerations

**Latency Metrics (Production):**
- WebSocket market data: 50-150ms
- Signal detection: <10ms
- Order placement: 50-200ms
- Total signal-to-fill: 200-500ms average

**Optimizations:**
- Local order book maintenance (reduces API calls)
- Batch database writes
- Connection pooling for databases
- In-memory caching of market data

### Risk Management

**Position Controls:**
- Per-strategy position limits
- Portfolio-level exposure limits
- Prevents orders exceeding limits

**Trading Controls:**
- Strategy state locks (prevent concurrent orders)
- Unfilled order monitoring and cancellation
- Order reconciliation after reconnections

**Safety Features:**
- Timestamp synchronization (prevents stale signals)
- Market data freshness checks
- Graceful shutdown on Ctrl+C

---

## Project Structure

```
trading_service_demo/
├── demo_hft_system.py          # Self-contained demo (NO dependencies)
├── src/
│   ├── main.py                 # Production entry point
│   ├── msg_queue.py            # NATS message queue wrapper
│   ├── data_feeds.py           # Exchange WebSocket connections
│   ├── book_manager.py         # Local order book management
│   ├── arb_scanner.py          # Arbitrage signal generation
│   ├── order_manager.py        # Order execution & lifecycle
│   ├── position_manager.py     # Position & risk management
│   ├── api_client.py           # MAX Exchange REST API
│   ├── market_data.py          # Aggregated market data singleton
│   ├── private_data_feed.py    # Private WebSocket feeds
│   ├── recorder.py             # Database persistence
│   ├── time_sync_manager.py    # Clock synchronization
│   ├── order_reconciler.py     # Order reconciliation
│   ├── config_enum.py          # Type definitions (enums)
│   ├── data_format.py          # Message schemas (dataclasses)
│   ├── settings.py             # Strategy configurations
│   ├── system_config.py        # System configuration
│   ├── logging_config.py       # Logging setup
│   ├── max_utilis.py           # MAX exchange utilities
│   └── reference_rate_provider.py  # Reference rate provider
├── .env.example                # Environment template
├── logging.yaml                # Logging configuration
├── DEMO_README.md             # Detailed demo documentation
├── DEMO_QUICKSTART.md         # Quick start guide
├── CLAUDE.md                  # Development notes
└── README.MD                  # This file
```

---

## Development

### Message Flow Example

```python
# 1. Data Feed receives WebSocket update
data_feed → L1BookUpdate(symbol="BTCUSDT", bid_px=45000, ...)

# 2. Publish to NATS
await mq.publish("exchange.binance.l1book.BTCUSDT", update)

# 3. Arbitrage Scanner subscribes and processes
arb_scanner.handle_update() → detects spread deviation

# 4. Generate signal
await mq.publish("signal.BTCUSDT", Signal(...))

# 5. Order Manager receives signal
order_manager.handle_signal() → validates position

# 6. Execute order via API
api_client.create_order(...) → order placed on exchange

# 7. Position Manager updates
position_manager.handle_order_fill() → updates balances
```

### Topic Naming Convention

```
{domain}.{exchange}.{data_type}.{market_type}.{sub_type}.{event_type}.{symbol}

Examples:
- exchange.binance.l1book.future.update.BTCUSDT
- exchange.max.l2book.spot.update.USDTTWD
- signal.arbitrage.BTCUSDT
- order.max.filled.USDTTWD
```

---
