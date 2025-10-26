import asyncio
import logging
import json
import websockets
from websockets.legacy.client import WebSocketClientProtocol

import time
from typing import Optional, Dict, Any, List
from datetime import datetime
from system_config import max_setting
import hmac
from src.data_format import MaxOrderUpdate, MaxAccountUpdate
import hashlib
from msg_queue import AsyncMessageQueue

# Import from your BookTicker and NATS topic prefix
# from data_format import UserDataEvent # If you have defined user data dataclass
from system_config import  NATSConfig
logger = logging.getLogger("data_feeds.BinancePrivateDataFeed")


# class BinancePrivateDataFeed:
#     WS_BASE_URL = "wss://fstream.binance.com/ws/"  # 期貨用戶數據流基礎 URL
#     LISTEN_KEY_FETCH_INTERVAL_SECONDS = 23 * 60 * 60  # 23 小時獲取新 key
#     LISTEN_KEY_KEEP_ALIVE_INTERVAL_SECONDS = 30 * 60  # 每 30 分鐘發送一次 keep-alive 請求
#     LISTEN_KEY_EXPIRY_SECONDS = 60 * 60  # listenKey 60 分鐘失效
#
#     RECONNECT_INITIAL_DELAY_SECONDS = 1
#     RECONNECT_MAX_DELAY_SECONDS = 60
#
#     def __init__(self):
#         self.api_client = BinanceAPIClient(
#             api_key=binance_api_settings.BINANCE_API_KEY,
#             secret_key=binance_api_settings.BINANCE_SECRET_KEY
#         )
#         self.message_queue: Optional[AsyncMessageQueue] = None
#         self.keep_running = True
#
#         self._active_websocket: Optional[websockets] = None
#         self._active_listen_key: Optional[str] = None
#         self._active_key_start_time: float = 0  # 記錄當前 listenKey 的獲取時間
#
#         self._main_connection_task: Optional[asyncio.Task] = None
#         self._keep_alive_task: Optional[asyncio.Task] = None
#         self._key_rotation_task: Optional[asyncio.Task] = None  # 新增：負責 listenKey 輪換
#
#         self._reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS
#
#         logger.info("BinancePrivateDataFeed initialized.")
#
#     async def _initialize_message_queue(self):
#         if self.message_queue is None:
#             self.message_queue = await AsyncMessageQueue.get_instance()
#             logger.info("AsyncMessageQueue instance obtained for private feed.")
#
#     async def _connect_websocket(self, listen_key: str) -> Optional[websockets]:
#         """嘗試連接到指定 listenKey 的 WebSocket。"""
#         ws_url = f"{self.WS_BASE_URL}{listen_key}"
#         try:
#             ws = await websockets.connect(
#                 ws_url,
#                 ping_interval=180,  # 配合 Binance 的 Ping
#                 ping_timeout=600  # 配合 Binance 的 Timeout
#             )
#             logger.info(f"Successfully connected to Binance Private WebSocket with listenKey: {listen_key}")
#             return ws
#         except Exception as e:
#             logger.error(f"Failed to connect to private WebSocket with listenKey {listen_key}: {e}", exc_info=True)
#             return None
#
#     async def _handle_private_message(self, data: Dict[str, Any]):
#         """
#         處理從 Binance 私有 WebSocket 接收到的不同類型的消息。
#         這裡可以根據實際消息類型進行解析和發布到 NATS。
#         """
#         event_type = data.get('e')
#
#         # 為了簡潔，這裡只處理部分類型並直接發布原始數據。
#         # 實際應用中，您會為每種事件定義 dataclass 並進行數據轉換。
#         if event_type == 'ACCOUNT_UPDATE':
#             logger.debug(f"Received Account Update: {data.get('a')}")
#             await self.message_queue.publish(f"{NATS_SUBJECT_USER_DATA_PREFIX}.account_update", data)
#         elif event_type == 'ORDER_TRADE_UPDATE':
#             logger.debug(f"Received Order/Trade Update for {data.get('o', {}).get('s')}: {data.get('o', {}).get('X')}")
#             await self.message_queue.publish(f"{NATS_SUBJECT_USER_DATA_PREFIX}.order_trade_update", data)
#         elif event_type == 'MARGIN_CALL':
#             logger.warning(f"Received Margin Call: {data}")
#             await self.message_queue.publish(f"{NATS_SUBJECT_USER_DATA_PREFIX}.margin_call", data)
#         elif event_type == 'ACCOUNT_CONFIG_UPDATE':
#             logger.debug(f"Received Account Config Update: {data}")
#             await self.message_queue.publish(f"{NATS_SUBJECT_USER_DATA_PREFIX}.account_config_update", data)
#         else:
#             logger.debug(f"Received unhandled private WebSocket message: {data}")
#
#     async def _listen_loop(self, ws: websockets, listen_key: str,
#                            connected_event: Optional[asyncio.Event] = None):
#         """單個 WebSocket 連線的監聽循環。"""
#         logger.info(f"Starting listen loop for listenKey: {listen_key}")
#         try:
#             while self.keep_running and ws.open:
#                 try:
#                     message = await asyncio.wait_for(ws.recv(), timeout=30)
#                     data = json.loads(message)
#
#                     # 收到第一條消息時設置事件
#                     if connected_event and not connected_event.is_set():
#                         connected_event.set()  # <-- 標記為已連接且收到數據
#                         logger.info(f"Listener for {listen_key} received first message, marking as ready.")
#
#                     await self._handle_private_message(data)
#                 except asyncio.TimeoutError:
#                     pass  # 正常情況，沒有新消息
#                 except websockets.exceptions.ConnectionClosed as e:
#                     logger.warning(f"WebSocket {listen_key} closed: Code={e.rcvd.code}, Reason={e.rcvd.reason}.")
#                     raise  # 向上拋出，觸發外層重連/輪換邏輯
#                 except json.JSONDecodeError as e:
#                     logger.error(f"Error decoding JSON from {listen_key} WS: {e}. Message: {message}", exc_info=True)
#                 except Exception as e:
#                     logger.error(f"Error processing message from {listen_key} WS: {e}", exc_info=True)
#             # 如果循環因 self.keep_running = False 或 ws.open = False 而退出
#             if connected_event and not connected_event.is_set():  # 如果在退出前都沒收到消息
#                 logger.warning(f"Listener for {listen_key} stopped without receiving any message.")
#         finally:
#             logger.info(f"Listen loop for listenKey: {listen_key} stopped.")
#
#     async def _keep_alive_loop(self):
#         """定期發送 keep-alive 請求以延長 listenKey 有效期。"""
#         while self.keep_running:
#             if self._active_listen_key:
#                 try:
#                     await self.api_client.keep_alive_listen_key(self._active_listen_key)
#                     logger.debug(f"ListenKey {self._active_listen_key} kept alive.")
#                 except Exception as e:
#                     logger.error(f"Error keeping active listenKey {self._active_listen_key} alive: {e}", exc_info=True)
#                     # 如果 keep-alive 失敗，強制觸發 listenKey 輪換
#                     logger.warning("Active listenKey keep-alive failed, forcing key rotation.")
#                     if self._key_rotation_task and not self._key_rotation_task.done():
#                         self._key_rotation_task.cancel()  # 取消當前輪換任務
#                     self._key_rotation_task = asyncio.create_task(self._rotate_listen_key())  # 立即啟動輪換
#             await asyncio.sleep(self.LISTEN_KEY_KEEP_ALIVE_INTERVAL_SECONDS)
#
#     async def _rotate_listen_key(self):
#         """負責主動輪換 listenKey 和 WebSocket 連線。"""
#         logger.info("Initiating listenKey rotation process...")
#
#         new_listen_key = None
#         new_ws = None
#         new_key_start_time = 0.0
#         new_ws_ready_event = asyncio.Event()  # <-- 新增：用於判斷新連線是否就緒
#
#         try:
#             # 1. 獲取新的 listenKey
#             new_listen_key = await self.api_client.get_listen_key()
#             new_key_start_time = time.monotonic()
#             logger.info(f"Obtained new listenKey for rotation: {new_listen_key}")
#
#             # 2. 建立新的 WebSocket 連線
#             new_ws = await self._connect_websocket(new_listen_key)
#             if not new_ws:
#                 logger.error(
#                     f"Failed to establish new WebSocket connection with key {new_listen_key}. Aborting rotation.")
#                 return  # 輪換失敗，等待下次觸發
#
#             # 3. 啟動新連線的監聽循環，並傳遞 ready_event
#             new_listener_task = asyncio.create_task(
#                 self._listen_loop(new_ws, new_listen_key, new_ws_ready_event),  # <-- 傳遞 event
#                 name=f"PrivateWS_Listener_New_{new_listen_key[:8]}"
#             )
#
#             # 4. 等待新連線就緒或超時
#             try:
#                 # 設定一個合理的超時時間，例如 10 秒
#                 await asyncio.wait_for(new_ws_ready_event.wait(), timeout=10)  # <-- 等待事件被設置
#                 logger.info(f"New WebSocket connection for key {new_listen_key} is ready and receiving data.")
#             except asyncio.TimeoutError:
#                 logger.error(
#                     f"New WebSocket connection for key {new_listen_key} timed out during warm-up. Aborting rotation.")
#                 new_listener_task.cancel()  # 取消新任務
#                 if new_ws and new_ws.open:
#                     await new_ws.close()
#                 return
#             except asyncio.CancelledError:
#                 logger.warning(f"Rotation process for key {new_listen_key} was cancelled during warm-up.")
#                 new_listener_task.cancel()
#                 if new_ws and new_ws.open:
#                     await new_ws.close()
#                 return
#
#             # 如果任務已經結束或被取消（儘管有 await wait_for，以防萬一）
#             if new_listener_task.done() or new_listener_task.cancelled():
#                 logger.error(
#                     f"New listener task for key {new_listen_key} failed or cancelled after warm-up. Aborting rotation.")
#                 if new_ws and new_ws.open:
#                     await new_ws.close()
#                 return
#
#             # 5. 切換活躍連線
#             old_listen_key = self._active_listen_key
#             old_websocket = self._active_websocket
#
#             self._active_listen_key = new_listen_key
#             self._active_websocket = new_ws
#             self._active_key_start_time = new_key_start_time
#
#             # 取消並等待舊的監聽任務結束
#             if self._main_connection_task and not self._main_connection_task.done():
#                 self._main_connection_task.cancel()
#                 try:
#                     await self._main_connection_task
#                 except asyncio.CancelledError:
#                     pass
#                 except Exception as e:
#                     logger.error(f"Error awaiting old main connection task: {e}", exc_info=True)
#
#             # 將新的監聽任務設為主要的連線任務
#             self._main_connection_task = new_listener_task
#
#             logger.info(
#                 f"Successfully rotated listenKey. Old key {old_listen_key} replaced by new key {new_listen_key}.")
#
#             # 6. 關閉舊的 WebSocket 連線 (確保在所有處理都切換到新連線後再關閉)
#             if old_websocket and old_websocket.open:
#                 logger.info(f"Closing old WebSocket connection for listenKey: {old_listen_key}")
#                 await old_websocket.close()
#
#         except Exception as e:
#             logger.error(f"Error during listenKey rotation: {e}", exc_info=True)
#             # 如果輪換失敗，確保任何新建立的連線都被關閉
#             if new_ws and new_ws.open:
#                 await new_ws.close()
#
#         self._reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS  # 重置延遲
#
#     async def connect_and_listen(self):
#         """主入口點：管理 WebSocket 連線的生命週期和輪換。"""
#         await self._initialize_message_queue()
#
#         while self.keep_running:
#             try:
#                 # 如果沒有活躍連線，則首次建立或重連
#                 if not self._active_websocket or not self._active_websocket.open:
#                     logger.info("No active WebSocket connection found. Establishing initial/reconnecting...")
#                     # 嘗試獲取 listenKey 並建立連線
#                     temp_listen_key = await self.api_client.get_listen_key()
#                     temp_ws = await self._connect_websocket(temp_listen_key)
#
#                     if temp_ws:
#                         self._active_listen_key = temp_listen_key
#                         self._active_websocket = temp_ws
#                         self._active_key_start_time = time.monotonic()
#                         logger.info(f"Initial connection established with key: {self._active_listen_key}")
#                         self._reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS  # 重置延遲
#                     else:
#                         logger.error("Failed to establish initial/reconnect WebSocket connection. Retrying...")
#                         await asyncio.sleep(self._reconnect_delay)
#                         self._reconnect_delay = min(self._reconnect_delay * 2, self.RECONNECT_MAX_DELAY_SECONDS)
#                         continue  # 繼續外層循環，重試
#
#                 # 確保 keep-alive 和 key rotation 任務在運行
#                 if self._keep_alive_task is None or self._keep_alive_task.done():
#                     self._keep_alive_task = asyncio.create_task(self._keep_alive_loop(),
#                                                                 name="PrivateWS_KeepAlive_Task")
#                     logger.info("ListenKey keep-alive task started.")
#
#                 # 只有當沒有 rotation 任務在運行時才創建，並在下次檢查時安排輪換
#                 if self._key_rotation_task is None or self._key_rotation_task.done():
#                     # 計算下一次輪換的時間
#                     time_to_next_rotation = (self._active_key_start_time + self.LISTEN_KEY_FETCH_INTERVAL_SECONDS) - time.monotonic()
#                     if time_to_next_rotation <= 0:  # 如果已經過期或接近過期
#                         time_to_next_rotation = 1  # 立即觸發
#
#                     self._key_rotation_task = asyncio.create_task(
#                         self._schedule_rotation(time_to_next_rotation), name="PrivateWS_KeyRotation_Scheduler"
#                     )
#                     logger.info(f"ListenKey rotation scheduled in {time_to_next_rotation:.1f} seconds.")
#
#                 # 運行主要的 WebSocket 監聽循環
#                 # 如果這個任務由於斷開而結束，外層循環會捕捉並觸發重連/輪換
#                 self._main_connection_task = asyncio.create_task(
#                     self._listen_loop(self._active_websocket, self._active_listen_key),
#                     name=f"PrivateWS_Listener_Active_{self._active_listen_key[:8]}"
#                 )
#                 await self._main_connection_task
#
#                 logger.info("Active WebSocket listener task ended. Initiating reconnection/rotation.")
#                 self._active_websocket = None  # 任務結束，將活躍連線標記為 None
#                 self._active_listen_key = None  # 強制重新獲取 key
#
#             except asyncio.CancelledError:
#                 logger.info("BinancePrivateDataFeed main task cancelled.")
#                 self.keep_running = False
#             except Exception as e:
#                 logger.error(f"Unexpected error in BinancePrivateDataFeed main loop: {e}", exc_info=True)
#                 # 錯誤發生後，強制重新獲取 listenKey 並重連
#                 self._active_websocket = None
#                 self._active_listen_key = None
#                 self._reconnect_delay = min(self._reconnect_delay * 2, self.RECONNECT_MAX_DELAY_SECONDS)
#                 logger.info(f"Retrying connection in {self._reconnect_delay:.2f} seconds...")
#                 await asyncio.sleep(self._reconnect_delay)
#
#         logger.info("BinancePrivateDataFeed main loop finished.")
#
#     async def _schedule_rotation(self, delay: float):
#         """延遲一段時間後觸發 listenKey 輪換。"""
#         try:
#             if delay > 0:
#                 logger.info(f"Scheduling next key rotation in {delay:.1f} seconds.")
#                 await asyncio.sleep(delay)
#
#             # 如果主控仍在運行，則執行輪換
#             if self.keep_running:
#                 logger.info("Time to rotate listenKey!")
#                 await self._rotate_listen_key()
#         except asyncio.CancelledError:
#             logger.info("ListenKey rotation schedule cancelled.")
#         except Exception as e:
#             logger.error(f"Error in listenKey rotation scheduler: {e}", exc_info=True)
#
#     async def stop(self):
#         """停止數據訂閱器的所有運行任務和連接。"""
#         logger.info("Stopping BinancePrivateDataFeed...")
#         self.keep_running = False
#
#         tasks_to_cancel = [
#             self._main_connection_task,
#             self._keep_alive_task,
#             self._key_rotation_task
#         ]
#
#         # 取消所有相關的內部任務
#         for task in tasks_to_cancel:
#             if task and not task.done():
#                 task.cancel()
#
#         # 等待這些任務完成清理
#         try:
#             await asyncio.gather(*[t for t in tasks_to_cancel if t and not t.done()], return_exceptions=True)
#         except Exception as e:
#             logger.error(f"Error during graceful shutdown of private feed tasks: {e}", exc_info=True)
#
#         # 關閉活躍的 WebSocket 連接
#         if self._active_websocket and self._active_websocket.open:
#             try:
#                 await self._active_websocket.close()
#                 logger.info("Active private WebSocket connection closed.")
#             except Exception as e:
#                 logger.error(f"Error closing active private WebSocket: {e}", exc_info=True)
#
#         # 關閉 API 客戶端
#         try:
#             await self.api_client.close()
#             logger.info("BinancePrivateAPIClient closed.")
#         except Exception as e:
#             logger.error(f"Error closing BinanceAPIClient: {e}", exc_info=True)
#
#         logger.info("BinancePrivateDataFeed stopped.")


class MaxOrderDataFeed:
    _logger = logging.getLogger("private_data_feed.MaxOrderDataFeed")
    WS_URL: str = 'wss://max-stream.maicoin.com/ws'
    MAX_CONNECTION_LIFETIME_SECONDS = 24 * 60 * 60 - 60
    RECONNECT_INITIAL_DELAY_SECONDS = 1
    RECONNECT_MAX_DELAY_SECONDS = 60
    WS_AUTH_TIMEOUT_SECONDS: int = 5

    def __init__(self, api_key: str = max_setting.MAX_API_KEY, secret_key: str = max_setting.MAX_SECRET_KEY,
                 auth_id: str = "MaxOrderFeed", filters: Optional[List[str]] = None):
        self.ws_url = MaxOrderDataFeed.WS_URL
        self.logger = MaxOrderDataFeed._logger
        self.api_key = api_key
        # Secret key must be byte string, used for signing
        self.secret_key_bytes = secret_key.encode('utf-8')
        self.auth_id = auth_id # ID for authentication request
        # If no filter specified, use document default "order", "trade", "account"
        self.filters = filters if filters is not None else ["order"]

        self.keep_running = True
        self.message_queue: Optional[AsyncMessageQueue] = None
        self._connection_start_time = 0
        self._main_loop_task: Optional[asyncio.Task] = None
        self._reconnect_listener_task: Optional[asyncio.Task] = None
        self._current_websocket: Optional[WebSocketClientProtocol] = None
        self._internal_stop_event: asyncio.Event = asyncio.Event()

        self.logger.info(f"MaxOrderDataFeed initialized with filters: {self.filters}")

    async def _initialize_message_queue(self):
        if self.message_queue is None:
            self.message_queue = await AsyncMessageQueue.get_instance()
            self.logger.info("AsyncMessageQueue instance obtained.")

    def _generate_signature(self, data_to_sign: str) -> str:
        """
        Generate HMAC SHA256 signature.
        According to documentation, signature is performed on the string form of nonce.
        """
        m = hmac.new(self.secret_key_bytes, data_to_sign.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    async def _generate_auth_message(self) -> Dict[str, Any]:
        """
        Generate Max private WebSocket authentication message, including `nonce` and `filters`.
        """
        nonce = int(time.time() * 1000)
        signature = self._generate_signature(str(nonce)) # Signature is for the nonce string

        auth_message = {
            "action": "auth",
            "apiKey": self.api_key,
            "nonce": nonce,
            "signature": signature,
            "id": self.auth_id,
            "filters": self.filters # Add filters to authentication message
        }
        self.logger.debug(f"Generated authentication message")
        return auth_message

    async def _reconnect_request_handler(self, subject: str, payload: Dict[str, Any]):
        if payload.get("exchange") == "max" and payload.get("stream_type") == "order_feed":
            reason = payload.get('reason', 'unknown')
            self.logger.warning(
                f"Received external reconnection request for Max order feed. Reason: {reason}. "
                f"Triggering a full DataFeed restart."
            )
            self._internal_stop_event.set()
        else:
            self.logger.debug(f"Ignored reconnect request for other stream type: {payload}")

    async def _run_websocket_loop(self):
        reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS

        while self.keep_running and not self._internal_stop_event.is_set():
            self.logger.info(f"Attempting to connect to Max Private WebSocket: {self.ws_url}")
            try:
                self._internal_stop_event.clear()

                async with websockets.connect(self.ws_url) as ws:
                    self._current_websocket = ws
                    self.logger.info(f"Successfully connected to Max Private WebSocket.")
                    self._connection_start_time = time.monotonic()
                    reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS

                    # --- Authentication phase ---
                    auth_msg = await self._generate_auth_message()
                    await self._current_websocket.send(json.dumps(auth_msg))
                    self.logger.info(f"Sent authentication and subscription message for private stream. ID: {self.auth_id}")

                    # Wait for authentication response
                    try:
                        auth_response_raw = await asyncio.wait_for(self._current_websocket.recv(), timeout=self.WS_AUTH_TIMEOUT_SECONDS)
                        auth_response = json.loads(auth_response_raw)
                        if auth_response.get('e') == 'authenticated' and auth_response.get('i') == self.auth_id:
                            self.logger.info(f"Max Private WebSocket authenticated successfully. Server time: {datetime.fromtimestamp(auth_response.get('T', 0) / 1000)}")
                        elif auth_response.get('e') == 'error': # Error response
                            error_msg = auth_response.get('m', 'Unknown error') # Error message usually in 'm' field
                            self.logger.error(f"Max Private WebSocket authentication failed: {error_msg}. Response: {auth_response_raw}")
                            raise websockets.exceptions.ConnectionClosed(None, None)
                        else:
                            self.logger.warning(f"Received unexpected response during auth phase: {auth_response_raw}")
                            # Unexpected response, may also need to reconnect
                            raise websockets.exceptions.ConnectionClosed(None, None)
                    except asyncio.TimeoutError:
                        self.logger.error("Max Private WebSocket authentication response timed out.")
                        raise websockets.exceptions.ConnectionClosedOK
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Error decoding auth JSON from WebSocket: {e}. Message: {auth_response_raw}", exc_info=True)
                        raise websockets.exceptions.ConnectionClosedOK
                    except websockets.exceptions.ConnectionClosed as e:
                        self.logger.error(f"WebSocket closed during auth handshake: Code={e.rcvd.code}, Reason={e.rcvd.reason}")
                        raise # Let outer layer catch and reconnect

                    # --- Main data receiving loop ---
                    while self.keep_running and not self._internal_stop_event.is_set():
                        if time.monotonic() - self._connection_start_time >= self.MAX_CONNECTION_LIFETIME_SECONDS:
                            self.logger.info(f"Max Private WebSocket approaching {self.MAX_CONNECTION_LIFETIME_SECONDS}s limit. Actively closing connection for graceful reconnect.")
                            await self._current_websocket.close(code=1000, reason="Lifetime limit reached")
                            break

                        try:
                            message = await asyncio.wait_for(self._current_websocket.recv(), timeout=30)
                            data = json.loads(message)

                            # --- Handle received events ---
                            # Documentation doesn't provide specific JSON structure for order, trade, account updates
                            # Here we make assumptions based on common patterns and Max REST API naming conventions
                            if data.get('e') == 'order_update': # Assume order update event 'e' is 'order'
                                orders_data = data.get('o', []) # Assume order data is in 'o' field
                                if orders_data:
                                    while orders_data:
                                        order_data = orders_data.pop(0)

                                        try:
                                            parsed_order = MaxOrderUpdate.from_dict(order_data)

                                            await self.message_queue.publish(
                                                NATSConfig.build_user_order_topic(parsed_order.market.lower(), "max"),
                                                parsed_order
                                            )
                                            self.logger.debug(f"Spot {parsed_order.market}.{data.get('e')}.max sent: {parsed_order}")

                                        except (KeyError, ValueError) as parse_error:
                                            self.logger.error(f"Error parsing order update message: {parse_error}. Data: {data}", exc_info=True)

                                else:
                                    self.logger.warning(f"Received 'order' event with no 'o' data: {data}")

                            elif data.get('e') == 'order_snapshot':
                                orders_data = data.get('o', [])  # Assume order data is in 'o' field
                                if orders_data:
                                    while orders_data:
                                        order_data = orders_data.pop(0)

                                        try:
                                            parsed_order = MaxOrderUpdate.from_dict(order_data)

                                            await self.message_queue.publish(
                                                NATSConfig.build_user_order_snapshot_topic(parsed_order.market.lower(), "max"),
                                                parsed_order
                                            )
                                            self.logger.debug(
                                                f"Spot {parsed_order.market}.{data.get('e')}.max sent: {parsed_order}")

                                        except (KeyError, ValueError) as parse_error:
                                            self.logger.error(
                                                f"Error parsing order snapshot message: {parse_error}. Data: {data}",
                                                exc_info=True)
                                elif not orders_data and data.get('e') == 'order_snapshot':
                                    self.logger.info(f"No open orders, no message broadcast")
                                else:
                                    self.logger.warning(f"Received 'order' event with no 'o' data: {data}")




                            elif data.get('e') == 'account_update' or data.get('e') == 'account_snapshot':# Assume account update event 'e' is 'account'
                                self.logger.debug(f"Received account update: {account_data}")
                                account_data = data.get('B', {}) # Assume account data is in 'a' field
                                published_balance = {'balances': []}
                                if account_data:
                                    while account_data:
                                        balance_data = account_data.pop(0)
                                        MaxAccountUpdate.from_dict(balance_data)
                                        published_balance['balances'].append(balance_data)
                                        if data.get('e') == 'account_update':
                                            event_type = NATSConfig.EVENT_TYPE_UPDATE
                                        else:
                                            event_type = NATSConfig.EVENT_TYPE_SNAPSHOT
                                        await self.message_queue.publish(
                                            f"{NATSConfig.TOPIC_MAX_USER_SPOT_ACCOUNT}.{event_type}.{MaxAccountUpdate.currency.lower()}",
                                            published_balance
                                        )

                                    # await self.message_queue.publish(...)
                                else:
                                    self.logger.warning(f"Received 'account' event with no 'B' data: {data}")

                            else:
                                self.logger.debug(f"Received unknown WebSocket message: {data}")

                        except asyncio.TimeoutError:
                            self.logger.debug("No message received for 30 seconds, maintaining connection...")
                            # websockets library usually handles ping/pong automatically, no need to send here
                            pass
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(f"Max Private WebSocket connection closed: Code={e.rcvd.code}, Reason={e.rcvd.reason}. Triggering reconnect...")
                            break
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Error decoding JSON from WebSocket: {e}. Message: {message}", exc_info=True)
                            break
                        except Exception as e:
                            self.logger.error(f"Error processing Max Private WebSocket message: {e}", exc_info=True)
                            break

            except websockets.exceptions.ConnectionClosedOK:
                self.logger.info("Max Private WebSocket connection closed gracefully. Attempting re-initialization or stopping.")
            except websockets.exceptions.ConnectionClosedError as e:
                self.logger.error(f"Max Private WebSocket connection closed with error: Code={e.rcvd.code}, Reason={e.rcvd.reason}. Retrying connection...", exc_info=True)
            except asyncio.CancelledError:
                self.logger.info("MaxOrderDataFeed _run_websocket_loop task cancelled. Stopping connection loop.")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error during Max Private WebSocket connection: {e}. Retrying in {reconnect_delay} seconds...", exc_info=True)

            if self.keep_running and not self._internal_stop_event.is_set():
                self.logger.info(f"Waiting {reconnect_delay:.2f} seconds before reconnecting...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.RECONNECT_MAX_DELAY_SECONDS)
            else:
                self.logger.info("MaxOrderDataFeed _run_websocket_loop terminating due to stop request or internal stop trigger.")
                break

    async def start(self):
        await self._initialize_message_queue()

        if self._reconnect_listener_task is None or self._reconnect_listener_task.done():
            self._reconnect_listener_task = asyncio.create_task(
                self.message_queue.subscribe(NATSConfig.TOPIC_MAX_USER_SPOT_RECONNECT, handler=self._reconnect_request_handler),
                name="MaxOrderDataFeed_Reconnect_Listener"
            )
            self.logger.info(f"Subscribed to reconnection requests on '{NATSConfig.TOPIC_MAX_USER_SPOT_RECONNECT}'")

        if self._main_loop_task and not self._main_loop_task.done():
            self.logger.info("MaxOrderDataFeed is already running.")
            return

        self.logger.info("Starting MaxOrderDataFeed...")
        self.keep_running = True
        self._internal_stop_event.clear()

        self._main_loop_task = asyncio.create_task(
            self._run_websocket_loop(),
            name="MaxOrderDataFeed_MainLoop"
        )
        self.logger.info("MaxOrderDataFeed main loop task started.")

    async def stop(self):
        self.logger.info("Stopping MaxOrderDataFeed...")
        self.keep_running = False
        self._internal_stop_event.set()

        if self._main_loop_task and not self._main_loop_task.done():
            self._main_loop_task.cancel()
            try:
                await self._main_loop_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.error(f"Error waiting for main loop task to cancel during stop: {e}", exc_info=True)
            finally:
                self._main_loop_task = None

        if self._reconnect_listener_task and not self._reconnect_listener_task.done():
            self._reconnect_listener_task.cancel()
            try:
                await self._reconnect_listener_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.error(f"Error waiting for reconnect listener task to cancel during stop: {e}", exc_info=True)
            finally:
                self._reconnect_listener_task = None

        if self._current_websocket:
            try:
                await self._current_websocket.close()
                self.logger.info("WebSocket connection explicitly closed by stop().")
            except Exception as e:
                self.logger.error(f"Error closing WebSocket during stop: {e}", exc_info=True)
            finally:
                self._current_websocket = None

        self.logger.info("MaxOrderDataFeed stopped.")

    async def restart(self, reason: str = "internal_request"):
        self.logger.info(f"Initiating full restart of MaxOrderDataFeed due to: {reason}")

        self._internal_stop_event.set()

        if self._main_loop_task and not self._main_loop_task.done():
            self._main_loop_task.cancel()
            try:
                await self._main_loop_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.error(f"Error waiting for main loop task to cancel during restart: {e}", exc_info=True)
            finally:
                self._main_loop_task = None

        if self._current_websocket and not self._current_websocket.closed:
            try:
                await self._current_websocket.close()
                self.logger.info("WebSocket connection explicitly closed for restart.")
            except Exception as e:
                self.logger.error(f"Error closing WebSocket during restart: {e}", exc_info=True)
            finally:
                self._current_websocket = None

        await asyncio.sleep(0.5)

        self.keep_running = True
        self._internal_stop_event.clear()
        self._connection_start_time = 0

        self.logger.info("MaxOrderDataFeed state reset. Attempting to re-establish connection.")
        await self.start()
        self.logger.info(f"MaxOrderDataFeed restart completed. Reason: {reason}")

