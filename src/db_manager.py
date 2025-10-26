# db_manager.py
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from data_format import L1BookUpdate
from msg_queue import AsyncMessageQueue
from dataclasses import asdict
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime
from system_config import settings


class AsyncBookTickerRecorder:
    _instance: Optional["AsyncBookTickerRecorder"] = None
    _lock: asyncio.Lock = asyncio.Lock()
    _logger = logging.getLogger("db_manager.AsyncBookTickerRecorder")

    BATCH_SIZE = 2000
    BATCH_INTERVAL_SECONDS = 2

    def __init__(self, db_name:str = "hft", collection_name: str = "book_tickers"):
        self.logger = AsyncBookTickerRecorder._logger
        if AsyncBookTickerRecorder._instance is not None:
            self.logger.error(
                "AsyncBookTickerRecorder is a singleton, please use AsyncBookTickerRecorder.get_instance() to get instance.")
            raise RuntimeError(
                "AsyncBookTickerRecorder is a singleton, please use AsyncBookTickerRecorder.get_instance() to get instance.")


        self.logger.debug("Initializing AsyncBookTickerRecorder instance.")

        self.db_name = db_name
        self.collection_name = collection_name

        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.collection: Optional[AsyncIOMotorClient] = None

        self.mq: Optional[AsyncMessageQueue] = None

        self.is_connected = False
        self._closing = False
        self._listener_task: Optional[asyncio.Task] = None
        self._batch_writer_task: Optional[asyncio.Task] = None

        self._buffer: List[Dict[str, Any]] = []
        self._buffer_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()

    @classmethod
    async def get_instance(cls, db_name: str = "hft",
                           collection_name: str = "book_tickers") -> "AsyncBookTickerRecorder":
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(db_name, collection_name)
                    await cls._instance._connect_to_mongo()
        return cls._instance

    async def _connect_to_mq(self):
        if self.mq is None:
            self.mq = await AsyncMessageQueue.get_instance()
            self.logger.info("AsyncMessageQueue instance obtained.")

    async def _message_handler(self, subject:str, raw_book_ticker_data: Dict[str, Any]):
        """
        處理從消息隊列接收到的 BookTicker 數據。
        預期 raw_book_ticker_data 已經是從 JSON 解析後的字典。
        """

        if self._closing:
            self.logger.debug(f"Recorder is closing, dropping message for {subject}.")
            return

        try:
            # 嘗試將收到的字典數據轉換為 BookTicker dataclass 實例。
            # 這能提供類型檢查和數據結構驗證。
            # 如果數據不符合 BookTicker 的結構，這裡會拋出 TypeError 或相關錯誤。
            book_ticker = L1BookUpdate(**raw_book_ticker_data)
        except TypeError as e:
            self.logger.warning(
                f"Received malformed BookTicker data on topic {subject}: {raw_book_ticker_data}. Error: {e}"
            )
            return
        except Exception as e:  # 捕獲其他可能發生的異常
            self.logger.error(
                f"Unexpected error converting raw data to BookTicker on topic {subject}: {raw_book_ticker_data}. Error: {e}",
                exc_info=True
            )
            return

        # 將 BookTicker 對象轉換為 MongoDB 友好的字典格式。
        # 使用 asdict() 確保 dataclass 實例能被正確序列化。
        data_to_store = asdict(book_ticker)
        # 添加本地接收時間，毫秒級時間戳。
        data_to_store["received_at"] = datetime.now()
        trigger_write = False
        async with self._buffer_lock:
            self._buffer.append(data_to_store)
            # self.logger.debug(f"Buffer size: {len(self._buffer)}")

            if len(self._buffer) >= self.BATCH_SIZE:
                trigger_write = True
        if trigger_write:
            self.logger.info(f"Buffer reached {self.BATCH_SIZE} items, triggering batch write.")
                    # 移到 lock 外 async 寫入
            await self._write_batch_to_db()

    async def _write_batch_to_db(self):
        """將緩衝區中的數據批量寫入 MongoDB"""
        async with self._write_lock:
            if not self.is_connected or self.collection is None:
                self.logger.warning("MongoDB not connected, cannot write batch.")
                return

            async with self._buffer_lock:
                if not self._buffer:
                    self.logger.debug("Buffer is empty, no data to write.")
                    return

                documents_to_insert = self._buffer[:]  # 複製一份，防止在寫入時 buffer 被修改
                self._buffer.clear()  # 清空緩衝區

            try:
                result = await self.collection.insert_many(documents_to_insert)
                self.logger.info(
                    f"Successfully inserted {len(result.inserted_ids)} documents into {self.db_name}.{self.collection_name}.")
            except Exception as e:
                self.logger.error(f"Failed to insert batch into MongoDB: {e}", exc_info=True)

    async def _periodic_batch_writer(self):
        """定期將緩衝區中的數據寫入 MongoDB"""
        while not self._closing:
            try:
                await asyncio.sleep(self.BATCH_INTERVAL_SECONDS)
                async with self._buffer_lock:
                    should_write = bool(self._buffer)
                if should_write:
                    self.logger.info(f"Periodic write triggered. Buffer has {len(self._buffer)} items.")
                    await self._write_batch_to_db()
            except asyncio.CancelledError:
                self.logger.info("Periodic batch writer task cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Error in periodic batch writer: {e}", exc_info=True)

    async def start_listening(self):
        """啟動監聽消息隊列和定期寫入任務"""
        await self._connect_to_mq()
        if not self.mq:
            self.logger.error("Message queue not available, cannot start listening.")
            return

        # 訂閱 book_ticker.* topic
        self._listener_task = asyncio.create_task(
            self.mq.subscribe("book_ticker.*.*", handler=self._message_handler)
        )

        # 啟動定期批量寫入任務
        self._batch_writer_task = asyncio.create_task(self._periodic_batch_writer())
        self.logger.info("Periodic batch writer task started.")

        try:
            await asyncio.gather(self._listener_task, self._batch_writer_task)
        except asyncio.CancelledError:
            self.logger.info("AsyncBookTickerRecorder listening tasks cancelled.")
        except Exception as e:
            self.logger.error(f"Error in AsyncBookTickerRecorder main listening loop: {e}", exc_info=True)

    async def stop(self):
        """停止記錄器並執行清理工作"""
        self.logger.info("Stopping AsyncBookTickerRecorder...")
        self._closing = True

        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self.logger.info("Message listener task stopped.")

        if self._batch_writer_task:
            self._batch_writer_task.cancel()
            try:
                await self._batch_writer_task
            except asyncio.CancelledError:
                pass
            self.logger.info("Periodic batch writer task stopped.")

        if self._buffer:
            self.logger.info(f"Writing remaining {len(self._buffer)} items to DB before closing.")
            await self._write_batch_to_db()

        if self.mongo_client:
            self.mongo_client.close()
            self.logger.info("MongoDB connection closed.")

        self.is_connected = False
        AsyncBookTickerRecorder._instance = None