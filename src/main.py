# main.py
import asyncio
import logging
from logging_config import setup_logging
from src.msg_queue import AsyncMessageQueue
from src.data_feeds import BinanceDataFeed, MaxDataFeed, ReferenceRateDataFeed
from src.private_data_feed import MaxOrderDataFeed
from src.book_manager import MaxBookManager
from config_enum import Exchange
from src.order_manager import MaxOrderManager
from src.position_manager import MaxPositionManager
from src.arb_scanner import StableScanner
from src.api_client import MaxAPIClient
import system_config
from src.time_sync_manager import TimeSyncManager
from src.market_data import MarketData


# Get NATS_SERVERS from config
NATS_SERVERS = system_config.NATS_SERVERS

setup_logging()
logger = logging.getLogger(__name__)

async def diagnose_message_queue(mq: AsyncMessageQueue):
    """Diagnostic function to check message queue status and help with debugging."""
    status = mq.get_connection_status()
    logger.info("=== Message Queue Diagnostics ===")
    logger.info(f"Connection Status: {status}")
    
    if not status['is_connected']:
        logger.error("NATS is not connected!")
        logger.error("   Check if NATS server is running on localhost:4222")
        logger.error("   Check network connectivity")
        logger.error("   Check firewall settings")
    else:
        logger.info("NATS connection is healthy")

    if not status['jetstream_available']:
        logger.warning("JetStream not available")
    else:
        logger.info("JetStream is available")
    
    logger.info("=================================")


async def main():
    logger.info("Starting HFT project...")

    # Create an Event to coordinate shutdown process
    stop_event = asyncio.Event()

    # Get AsyncMessageQueue singleton instance and connect
    mq = await AsyncMessageQueue.get_instance(servers=NATS_SERVERS)
    
    # Enable debug mode for better error diagnostics during development
    mq.enable_debug_mode(True)
    
    try:
        await mq.connect()
        logger.info("Message queue connected.")
        
        # Run diagnostics after connection
        await diagnose_message_queue(mq)
        
    except Exception as e:
        logger.critical(f"Failed to connect to message queue: {e}")
        await diagnose_message_queue(mq)
        raise
    target_symbols = ['usdttwd']
    # Initialize API clients and core components
    max_api_client = MaxAPIClient()
    market_data = await  MarketData.create_instance()
    tsm =  await TimeSyncManager().create_instance()
    
    # Data feeds
    max_order_feed = MaxOrderDataFeed()
    binance_future_df = BinanceDataFeed(symbols=target_symbols , market_type='future')
    max_df = MaxDataFeed(symbols=target_symbols )
    ref_rate_df = ReferenceRateDataFeed(fetch_interval_seconds=60)  # Fetch reference USD/TWD rates every 60 seconds
    
    # Local order book manager
    max_book_manager = MaxBookManager()
    
    # Trading infrastructure
    position_manager = MaxPositionManager(client=max_api_client, market_data=market_data, symbols=target_symbols)
    order_manager = MaxOrderManager(client=max_api_client, position_manager=position_manager,market_data=market_data,
                                    interval=60)



    # Stable scanner for USDTTWD arbitrage between MAX and reference rate source
    stable_scanner = StableScanner(
        position_manager=position_manager,
        market_data=market_data# 0.1% premium threshold for signals
    )
    



    # Initialize all components
    logger.info("Initializing system components...")
    await order_manager.initialize()

    # Create and start data feed tasks first
    logger.info("Starting market data sources...")
    logger.info("Starting trading system components...")
    order_manager_task = asyncio.create_task(order_manager.start())

    # Data feed tasks - start these first
    max_order_feed_task = asyncio.create_task(max_order_feed.start())
    binance_future_task = asyncio.create_task(binance_future_df.connect_and_listen())
    max_df_task = asyncio.create_task(max_df.start())
    ref_rate_df_task = asyncio.create_task(ref_rate_df.start())
    
    # Book management task
    max_book_manager_task = asyncio.create_task(max_book_manager.start_listening())
    
    # Wait for data feeds to initialize and start receiving data
    logger.info("Waiting for data sources to establish connections...")
    await asyncio.sleep(5)  # Give data feeds time to connect

    # Check if we have initial market data
    logger.info("Checking market data availability...")
    data_check_attempts = 0
    max_data_check_attempts = 12  # 60 seconds total wait time
    
    while data_check_attempts < max_data_check_attempts:
        ref_rate_price = market_data.get_exchange_mid_price(exchange=Exchange.REFERENCE_SOURCE.lower(), symbol='usdtwd')
        max_price = market_data.get_exchange_mid_price(exchange=Exchange.MAX.lower(), symbol='usdttwd')

        if ref_rate_price and max_price:
            logger.info(f"Market data available - Reference USD/TWD: {ref_rate_price:.4f}, MAX USDTTWD: {max_price:.4f}")
            break
        else:
            logger.info(f"Waiting for market data... (attempt {data_check_attempts + 1}/{max_data_check_attempts})")
            await asyncio.sleep(5)
            data_check_attempts += 1

    if data_check_attempts >= max_data_check_attempts:
        logger.warning("Market data still not fully available, but continuing to start trading strategy...")
    
    # Now start trading system components

    # Start signal generators after data feeds are ready
    logger.info("Starting signal generators...")
    stable_scanner_task = asyncio.create_task(stable_scanner.start_listening())
    # arb_scanner_task = asyncio.create_task(arb_scanner.start_listening())
    
    # Collect all running tasks
    running_tasks = [
        max_order_feed_task,
        binance_future_task,
        max_df_task,
        ref_rate_df_task,
        max_book_manager_task,
        order_manager_task,
        stable_scanner_task,
        # arb_scanner_task,
    ]
    
    logger.info("All system components started, HFT system running...")
    logger.info(f"Monitoring trading pairs: {['BTCUSDT', 'ETHUSDT', 'USDTTWD']}")
    # logger.info(f"Arbitrage scanning: Binance Futures (lead) -> MAX Spot (lag), threshold: 0.1%")
    logger.info(f"Stable arbitrage strategy: USDTTWD (MAX) vs USD/TWD (Reference Source), threshold: 0.1%")
    logger.info(f"Reference rate data update interval: 60 seconds")

    try:
        # Main loop waits for stop_event to be set (e.g., via Ctrl+C)
        await stop_event.wait()
        logger.info("Received stop signal, preparing to shutdown application...")

    except asyncio.CancelledError:
        logger.info("Main task was cancelled (e.g., via Ctrl+C). Initiating graceful shutdown.")
    except Exception as e:
        logger.critical(f"Main loop encountered an unexpected error: {e}", exc_info=True)
    finally:
        logger.info("Starting graceful shutdown of all running tasks and connections...")

        # Stop all components gracefully
        try:
            # Stop trading components first
            await order_manager.stop()
            # Stop stable scanner gracefully
            if hasattr(stable_scanner, 'stop'):
                await stable_scanner.stop()
            else:
                stable_scanner.is_running = False  # Set running flag to False
            # await arb_scanner.stop() if hasattr(arb_scanner, 'stop') else None

            
            # Stop data feeds
            binance_future_df.stop()
            await max_df.stop()
            await ref_rate_df.stop() if hasattr(ref_rate_df, 'stop') else None
            await max_book_manager.stop()
            await max_order_feed.stop() if hasattr(max_order_feed, 'stop') else None

            logger.info("Core components stopped")

        except Exception as e:
            logger.error(f"Error occurred while stopping core components: {e}", exc_info=True)

        # Cancel all running tasks
        for task in running_tasks:
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete or timeout after 10 seconds
        try:
            await asyncio.wait_for(
                asyncio.gather(*running_tasks, return_exceptions=True),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            logger.warning("Some tasks failed to complete shutdown within 10 seconds")
        except Exception as e:
            logger.error(f"Error occurred while waiting for task shutdown: {e}", exc_info=True)

        logger.info("All background tasks stopped")

        # Close message queue connection
        try:
            if mq.is_connected:
                await mq.close()
                logger.info("Message queue closed")
        except Exception as e:
            logger.error(f"Error occurred while closing message queue: {e}")

        logger.info("HFT system fully shutdown")


if __name__ == "__main__":
    # Set up Ctrl+C handling to allow asyncio.run to capture and trigger CancelledError
    # This is the standard approach for Python 3.8+
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("HFT system interrupted by user (Ctrl+C).")
    except Exception as e:
        logger.critical(f"HFT system terminated with unexpected error: {e}", exc_info=True)
