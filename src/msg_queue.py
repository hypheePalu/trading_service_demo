# message_queue.py
import asyncio
import json
from dataclasses import asdict
from typing import Callable, Optional, Dict, Any, List, Awaitable
import logging

import nats
from nats.js import JetStreamContext
from nats.errors import TimeoutError, NoRespondersError
import nats.js.errors # 引入特定的 JetStream 錯誤類型

class AsyncMessageQueue:
    # 類級別變數，用於儲存單例實例
    _instance: Optional["AsyncMessageQueue"] = None
    # 類級別的異步鎖，用於在多個協程嘗試獲取實例時確保線程安全
    _lock: asyncio.Lock = asyncio.Lock()

    # 將 logger 定義為類級別變數，這樣在類方法和實例方法中都可以使用
    _logger = logging.getLogger("msg_queue.AsyncMessageQueue") # 給它一個特定的名稱

    def __init__(self, servers: List[str]):
        # 防止直接通過 AsyncMessageQueue() 創建新實例
        if AsyncMessageQueue._instance is not None:
            # 使用類級別的 logger 來記錄這個錯誤
            AsyncMessageQueue._logger.error("AsyncMessageQueue is a singleton, please use AsyncMessageQueue.get_instance() to get instance.")
            raise RuntimeError("AsyncMessageQueue is a singleton, please use AsyncMessageQueue.get_instance() to get instance.")

        # 實例級別的 logger，與 _logger 指向同一個日誌器對象
        # 這樣在實例方法中仍可使用 self.logger
        self.logger = AsyncMessageQueue._logger
        self.logger.debug("Initializing AsyncMessageQueue instance.")

        self.servers = servers
        self.nc: Optional[nats.NATS] = None
        self.js: Optional[JetStreamContext] = None
        self.is_connected = False
        self._closing = False # 新增標誌，用於控制關閉邏輯
        self._debug_mode = False # Debug mode for enhanced error diagnostics

    @classmethod
    async def get_instance(cls, servers: Optional[List[str]] = None) -> "AsyncMessageQueue":
        """
        獲取 AsyncMessageQueue 的單例實例。
        如果實例不存在，則創建它；如果已存在，則返回現有實例。
        Args:
            servers (Optional[List[str]]): NATS 伺服器地址列表。
                                          只有在第一次創建實例時才需要提供。
        Returns:
            AsyncMessageQueue: 單例實例。
        """
        async with cls._lock: # 使用異步鎖確保線程安全
            if cls._instance is None:
                if servers is None:
                    cls._logger.error("Attempted to get AsyncMessageQueue instance for the first time without providing 'servers' parameter.")
                    raise ValueError("第一次獲取 AsyncMessageQueue 實例時，必須提供 'servers' 參數。")
                cls._instance = cls(servers)
                cls._logger.info("AsyncMessageQueue 單例實例已創建。")
            elif servers is not None and cls._instance.servers != servers:
                # 警告：如果實例已存在但嘗試用不同的伺服器列表獲取，可能存在配置問題
                cls._logger.warning(f"AsyncMessageQueue 實例已存在，但嘗試用不同的伺服器列表初始化。使用現有實例的配置：{cls._instance.servers}。")
            else:
                cls._logger.debug("Returning existing AsyncMessageQueue instance.")
        return cls._instance

    async def connect(self):
        """
        連接到 NATS 伺服器並獲取 JetStream 上下文。
        """
        if self.is_connected:
            self.logger.debug("Already connected to NATS.")
            return
        if self._closing:
            self.logger.warning("NATS connection is in closing state. Cannot connect.")
            return

        try:
            async def disconnected_cb():
                self.is_connected = False
                if not self._closing:
                    self.logger.warning("NATS disconnected. Attempting to reconnect...")

            async def reconnected_cb():
                self.is_connected = True
                self.logger.info("NATS reconnected!")

            async def error_cb(e):
                self.logger.error(f"NATS client error: {e}")

            self.nc = await nats.connect(self.servers,
                                         disconnected_cb=disconnected_cb,
                                         reconnected_cb=reconnected_cb,
                                         error_cb=error_cb,
                                         # 可以添加其他選項，例如連接超時時間
                                         # timeout=10,
                                         # reconnect_time_wait=0.5 # 重連等待時間
                                         )

            self.is_connected = True
            self.logger.info("Connected to NATS.")

            self.js = self.nc.jetstream()
            self.logger.info("JetStream context acquired.")
        except Exception as e:
            self.logger.critical(f"Failed to connect to NATS server. Client error: {e}", exc_info=True)
            self.is_connected = False
            raise

    async def close(self):
        """
        關閉 NATS 連接。
        """
        if self.nc and self.is_connected:
            self._closing = True # 設置關閉標誌
            try:
                await self.nc.close()
                self.logger.info("NATS connection closed.")
            except Exception as e:
                self.logger.error(f"Error closing NATS connection: {e}")
            finally:
                self.is_connected = False
                self.nc = None
                self.js = None
                self._closing = False # 重置標誌
        elif self.nc is None:
            self.logger.debug("NATS connection is already None, nothing to close.")
        else:
            self.logger.info("NATS connection is not active or already closed.")

    def enable_debug_mode(self, enabled: bool = True):
        """
        Enable or disable debug mode for enhanced error diagnostics.
        In debug mode, exceptions in message handlers will be re-raised.
        """
        self._debug_mode = enabled
        self.logger.info(f"Debug mode {'enabled' if enabled else 'disabled'}")

    def get_connection_status(self) -> Dict[str, Any]:
        """Get detailed connection status for debugging."""
        return {
            'is_connected': self.is_connected,
            'servers': self.servers,
            'closing': self._closing,
            'debug_mode': self._debug_mode,
            'nc_status': 'connected' if self.nc and self.nc.is_connected else 'disconnected',
            'jetstream_available': self.js is not None
        }

    # --- 標準 NATS Pub/Sub (非持久化, 適合行情數據) ---

    async def publish(self, subject: str, message: Any):
        """
        發布消息到 NATS 主題 (標準 Pub/Sub)。
        此模式下消息不保證持久化，消費者離線時將錯過消息。
        Args:
            subject: NATS 主題名稱 (e.g., "market.binance.btcusdt")
            message: 要發布的數據，建議是 dataclass 實例
        """
        if not self.is_connected or not self.nc:
            self.logger.warning(f"NATS is not connected. Cannot publish to {subject}.")
            return

        try:
            # 將 dataclass 轉換為字典並序列化為 JSON
            if hasattr(message, '__dataclass_fields__'):
                message_dict = asdict(message)
                self.logger.debug(f"Publishing dataclass to '{subject}': {type(message).__name__}")
            else:
                message_dict = message
                self.logger.debug(f"Publishing dict to '{subject}': {type(message).__name__}")
            
            # Enhanced JSON serialization with error details
            try:
                payload = json.dumps(message_dict).encode('utf-8')
            except (TypeError, ValueError) as e:
                self.logger.error(f"JSON serialization error for {subject}:")
                self.logger.error(f"  Message type: {type(message)}")
                self.logger.error(f"  Message content: {message}")
                self.logger.error(f"  Serialization error: {e}")
                if self._debug_mode:
                    raise
                return

            await self.nc.publish(subject, payload)
            self.logger.debug(f"Published to '{subject}' (Pub/Sub), payload size: {len(payload)} bytes")
            
        except Exception as e:
            self.logger.error(f"Error publishing to {subject} (Pub/Sub):")
            self.logger.error(f"  Exception type: {type(e).__name__}")
            self.logger.error(f"  Exception message: {str(e)}")
            self.logger.error(f"  Connection status: {self.get_connection_status()}")
            self.logger.exception("Full publish error traceback:")
            if self._debug_mode:
                raise

    async def subscribe(self, subject: str, handler: Callable[[str, Dict[str, Any]], Awaitable[Any]]):
        """
        訂閱 NATS 主題 (標準 Pub/Sub)。
        Args:
            subject: NATS 主題名稱
            handler: 處理接收到消息的異步函數 (將接收解碼後的 dict)
        """
        if not self.is_connected or not self.nc:
            self.logger.error(f"NATS is not connected. Cannot subscribe to {subject}.")
            return

        async def _msg_handler(msg):
            try:
                # Enhanced JSON decoding with detailed error info
                raw_data = msg.data.decode('utf-8')
                self.logger.debug(f"Received message on {msg.subject}: {raw_data[:200]}...")
                
                data = json.loads(raw_data)
                await handler(msg.subject, data)
                
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error on {msg.subject}:")
                self.logger.error(f"  Error: {e}")
                self.logger.error(f"  Raw data: {raw_data}")
                self.logger.error(f"  Data length: {len(raw_data)}")
                # Re-raise to help with debugging
                raise
                
            except Exception as e:
                self.logger.error(f"Handler error on {msg.subject}:")
                self.logger.error(f"  Exception type: {type(e).__name__}")
                self.logger.error(f"  Exception message: {str(e)}")
                self.logger.error(f"  Message data: {data if 'data' in locals() else 'N/A'}")
                self.logger.error(f"  Handler function: {handler.__name__ if hasattr(handler, '__name__') else handler}")
                self.logger.exception("Full traceback:")  # This includes the full stack trace
                # Optionally re-raise for debugging (can be configured)
                if self._debug_mode:
                    raise

        # 使用 asyncio.create_task 確保訂閱非阻塞
        asyncio.create_task(self.nc.subscribe(subject, cb=_msg_handler))
        self.logger.info(f"Subscribed to '{subject}' (Pub/Sub)")


    # --- NATS JetStream (持久化, 適合訂單/部位數據) ---

    async def publish_jetstream(self, stream_name: str, subject: str, message: Any):
        """
        發布消息到 JetStream Stream。
        此模式保證消息持久化，消費者可重放或獲取離線期間的消息。
        Args:
            stream_name: Stream 的名稱 (e.g., "ORDERS", "POSITIONS")
            subject: JetStream 主題名稱 (必須是 Stream 的一部分，e.g., "order.updates")
            message: 要發布的數據，建議是 dataclass 實例
        """
        if not self.is_connected or not self.js:
            self.logger.warning(f"JetStream is not connected. Cannot publish to {subject}.")
            return

        try:
            # 確保 Stream 存在 (可以在啟動時一次性創建)
            await self._ensure_stream_exists(stream_name, subjects=[f"{stream_name}.*"])

            if hasattr(message, '__dataclass_fields__'):
                payload = json.dumps(asdict(message)).encode('utf-8')
            else:
                payload = json.dumps(message).encode('utf-8')

            # publish_msg 將確保消息被寫入 Stream
            ack = await self.js.publish(subject, payload)
            self.logger.debug(f"Published to '{subject}' (JetStream). Stream sequence: {ack.seq}")
        except NoRespondersError:
            self.logger.error(f"No responders for JetStream subject '{subject}'. Check Stream configuration.")
        except TimeoutError:
            self.logger.error(f"Timeout publishing to JetStream subject '{subject}'.")
        except Exception as e:
            self.logger.error(f"Error publishing to {subject} (JetStream): {e}")

    async def subscribe_jetstream(self, stream_name: str, subject: str, handler: Callable[[Dict[str, Any]], Any], durable_name: str, deliver_new: bool = True):
        """
        訂閱 JetStream Stream。
        此模式使用 durable consumer，保證消息交付和重放。
        Args:
            stream_name: Stream 的名稱
            subject: JetStream 主題名稱 (e.g., "order.updates")
            handler: 處理接收到消息的異步函數 (將接收解碼後的 dict)
            durable_name: 持久化消費者的名稱，用於標識和恢復狀態 (e.g., "order_manager_consumer")
            deliver_new: 是否從 Stream 的最新消息開始消費 (True) 或從頭開始 (False)
        """
        if not self.is_connected or not self.js:
            self.logger.error(f"JetStream is not connected. Cannot subscribe to {subject}.")
            return

        async def _js_msg_handler(msg):
            try:
                data = json.loads(msg.data.decode('utf-8'))
                await handler(data)
                await msg.ack() # 消息處理完畢後，務必進行確認 (Ack)
            except json.JSONDecodeError as e:
                self.logger.error(f"Error decoding JSON from JetStream {msg.subject}: {e} - Data: {msg.data.decode('utf-8')}")
                await msg.nak() # 解析失敗，發送 NAK，讓消息重新進入隊列
            except Exception as e:
                self.logger.error(f"Error in JetStream handler for {msg.subject}: {e}")
                await msg.nak() # 處理失敗，發送 NAK

        try:
            # 確保 Stream 存在
            await self._ensure_stream_exists(stream_name, subjects=[f"{stream_name}.*"])

            # 從 Stream 獲取消息
            consumer_options = {}
            if deliver_new:
                consumer_options['deliver_policy'] = nats.js.api.DeliverPolicy.NEW
            else:
                consumer_options['deliver_policy'] = nats.js.api.DeliverPolicy.ALL # 從 Stream 的所有消息開始

            # 異步訂閱，使用 durable consumer
            sub = await self.js.pull_subscribe(
                subject=subject,
                durable=durable_name,
                stream=stream_name,
                **consumer_options
            )
            self.logger.info(f"Subscribed to '{subject}' (JetStream, durable: {durable_name})")

            # 在一個單獨的任務中拉取消息
            asyncio.create_task(self._pull_messages_loop(sub, _js_msg_handler))

        except Exception as e:
            self.logger.error(f"Error subscribing to JetStream {subject}: {e}")


    async def _pull_messages_loop(self, sub, handler):
        """
        持續從 JetStream Pull Consumer 拉取消息的循環。
        """
        while True:
            try:
                # 批量拉取消息，並設置超時時間
                # 這裡的 batch size 和 timeout 可以根據吞吐量調整
                messages = await sub.fetch(100, timeout=1.0)
                for msg in messages:
                    await handler(msg)
            except TimeoutError:
                # print("No new messages from JetStream (pull_subscribe timeout).")
                pass # 正常情況，沒有新消息時會超時
            except Exception as e:
                self.logger.error(f"Error fetching messages from JetStream pull consumer: {e}")
                await asyncio.sleep(1) # 發生錯誤時稍作等待

    async def _ensure_stream_exists(self, stream_name: str, subjects: List[str]):
        """
        確保 JetStream Stream 存在。如果不存在，則創建它。
        Args:
            stream_name: Stream 的名稱
            subjects: Stream 應該包含的主題列表 (e.g., ["ORDERS.*"])
        """
        if not self.is_connected or not self.js:
            return

        try:
            await self.js.add_stream(name=stream_name, subjects=subjects)
            self.logger.debug(f"Ensured JetStream Stream '{stream_name}' exists.")
        except nats.js.errors.APIError: # Use specific error type for clarity
            self.logger.debug(f"JetStream Stream '{stream_name}' already exists.")
            pass
        except Exception as e:
            # Stream already exists is a common "error" here, can be ignored if it's the specific error
            # Or handle it based on your exact nats-py version and error types
            if "stream already exists" in str(e): # Check for specific error message
                 pass
            else:
                 self.logger.error(f"Error ensuring JetStream Stream '{stream_name}' exists: {e}")