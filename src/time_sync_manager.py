import asyncio
from typing import Dict, Any, Optional

from api_client import MaxAPIClient, BinanceAPIClient
from src.config_enum import Exchange
import time
import logging


class TimeSyncManager:
    _instance: Optional['TimeSyncManager'] = None
    _logger = logging.getLogger('time_sync_manager.TimeSyncManager')
    ewma_alpha = 0.3

    def __init__(self):
        self.logger = TimeSyncManager._logger
        # Store EWMA corrected offset for each exchange, unit is milliseconds
        self.offsets: Dict[str, float] = {}
        self.latencies: Dict[str, float] = {}
        # Store EWMA smoothing parameter alpha
        self.clients: Dict[str, Any] = {
            Exchange.BINANCE: BinanceAPIClient(),
            Exchange.MAX: MaxAPIClient()
        }

    @classmethod
    async def create_instance(cls):
        if cls._instance is None:
            instance = cls()
            # Start independent sync task for each exchange
            for exchange in Exchange:
                asyncio.create_task(instance._sync_task(exchange))
            cls._instance = instance
        return cls._instance

    async def _sync_task(self, exchange: Exchange):
        if exchange not in self.clients:
            self.logger.warning(f"No client for exchange {exchange}")
            return
        client = self.clients[exchange]

        while True:
            try:
                # Use millisecond precision
                local_t1 = time.time() * 1000
                server_time = await client.get_server_time()
                local_t2 = time.time() * 1000

                # Calculate raw offset for single request
                raw_offset = self.calculate_offset(local_t1, local_t2, server_time)

                # Perform EWMA correction
                current_ewma_offset = self.offsets.get(exchange.value)
                if current_ewma_offset is None:
                    # First run, use raw offset directly as initial value
                    ewma_offset = raw_offset
                else:
                    # Use EWMA formula for smoothing
                    ewma_offset = self.ewma_alpha * raw_offset + (1 - self.ewma_alpha) * current_ewma_offset

                # Store EWMA corrected offset
                self.offsets[exchange.value] = ewma_offset
                self.latencies[exchange.value] = (local_t2 - local_t1)/2

                self.logger.debug(f"[{exchange.value.upper()}] Raw Offset: {raw_offset:.3f} ms")
                self.logger.debug(f"[{exchange.value.upper()}] EWMA Offset: {ewma_offset:.3f} ms")
                self.logger.debug(f"[{exchange.value.upper()}] latency: {(local_t2 - local_t1)/2:.3f} ms\n")

            except Exception as e:
                self.logger.error(f"Error syncing time with {exchange.value}: {e}")

            # Sleep for 5 seconds
            await asyncio.sleep(5)

    @staticmethod
    def calculate_offset(local_sent_ts: float, local_received_ts: float, server_ts: float) -> float:
        """
        Calculate clock offset, all timestamps use milliseconds.
        Returned offset unit is milliseconds.
        """
        return server_ts - (local_sent_ts + local_received_ts) / 2

    def get_offset(self, exchange: Exchange) -> float:
        return self.offsets.get(exchange.value)

    def get_internal_offset(self, lead_exchange: Exchange, lag_exchange: Exchange) -> float:
        return self.offsets.get(lead_exchange.value) - self.offsets.get(lag_exchange.value)





if __name__ == '__main__':
    async def main():
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        main_logger = logging.getLogger("main")

        # Custom EWMA parameters: adjust alpha value here
        ewma_alpha = 0.3
        main_logger.info(f"Using EWMA alpha: {ewma_alpha}")

        # Asynchronously create TimeSyncManager singleton
        main_logger.info("Starting TimeSyncManager...")
        tsm = await TimeSyncManager.create_instance()

        # Program will run in background and continuously print corrected offsets
        main_logger.info("Running in background. Press Ctrl+C to stop.")

        # Keep event loop running so background tasks continue executing
        while True:
            await asyncio.sleep(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram stopped by user.")


