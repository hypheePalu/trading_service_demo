# binance_data_feed.py
import asyncio
import websockets
from websockets.legacy.client import WebSocketClientProtocol
import json
import logging
import time  # 用於記錄連接開始時間
from data_format import L1BookUpdate, MaxOrderBookUpdate, MaxOrderBookSnapShot # 假設 data_format.py 在相同或可導航的路徑
from msg_queue import AsyncMessageQueue  # 確保路徑正確，假設是 msg_queue.py
from typing import Optional, Dict, Any, List
from system_config import  NATSConfig
from datetime import datetime
import httpx
from bs4 import BeautifulSoup




class BinanceDataFeed:
    # 類級別的日誌器
    _logger = logging.getLogger("data_feeds.BinanceDataFeed")

    # Binance WebSocket 連接限制
    MAX_CONNECTION_LIFETIME_SECONDS = 24 * 60 * 60 - 60  # 24小時 - 1分鐘的緩衝
    RECONNECT_INITIAL_DELAY_SECONDS = 1
    RECONNECT_MAX_DELAY_SECONDS = 60

    def __init__(self, symbols: list, market_type: str = "future"):
        """
        初始化 Binance 數據訂閱器。
        Args:
            symbols: 要訂閱的交易對列表，例如 ["BTCUSDT", "ETHUSDT"]
            market_type: "futures" (U本位合約) 或 "spot" (現貨)
        """
        # 實例級別的日誌器，指向類級別的日誌器
        self.logger = BinanceDataFeed._logger
        self.symbols = [s.upper() for s in symbols]  # 確保符號是大寫
        self.market_type = market_type.lower()

        if self.market_type == "future":
            self.ws_url = "wss://fstream.binance.com/ws"  # U本位合約
        elif self.market_type == "spot":
            self.ws_url = "wss://stream.binance.com:9443/ws"  # 現貨
        else:
            raise ValueError("Invalid market_type. Must be 'future' or 'spot'.")

        self.keep_running = True
        self.message_queue: Optional[AsyncMessageQueue] = None  # 在 connect_and_listen 中異步獲取
        self._connection_start_time = 0  # 記錄當前連接的開始時間

    async def _initialize_message_queue(self):
        """異步獲取 AsyncMessageQueue 的單例實例。"""
        if self.message_queue is None:
            # 假設 AsyncMessageQueue 已經被 main.py 初始化並連接
            self.message_queue = await AsyncMessageQueue.get_instance()
            self.logger.info("AsyncMessageQueue instance obtained.")

    async def _subscribe_message(self):
        """生成訂閱消息 for bookTicker"""
        # 注意：Binance 的 stream 名稱是小寫
        streams = [f"{s.lower()}@bookTicker" for s in self.symbols]
        return {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }

    async def connect_and_listen(self):
        """
        連接到 Binance WebSocket 並開始監聽數據。
        包含自動重連和 24 小時強制重連邏輯。
        """
        await self._initialize_message_queue()  # 確保消息隊列已初始化

        reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS

        while self.keep_running:
            self.logger.info(f"Attempting to connect to Binance {self.market_type} WebSocket: {self.ws_url}")
            try:
                # 設置 ping_interval 和 ping_timeout
                # Binance WebSocket server will send a ping frame every 3 minutes (180 seconds).
                # If the server does not receive a pong frame back within a 10 minute (600 seconds) period, the connection will be disconnected.
                # websockets 庫會自動處理 pong 回覆。
                async with websockets.connect(
                        self.ws_url,
                        ping_interval=self.MAX_CONNECTION_LIFETIME_SECONDS,  # 設置一個較大的值，讓它不幹預 Binance 的 ping/pong
                        ping_timeout=self.RECONNECT_MAX_DELAY_SECONDS + 5  # 比重連最大延遲稍長，避免因為ping/pong timeout導致過早斷開
                ) as ws:
                    self.logger.info(
                        f"Successfully connected to Binance {self.market_type} WebSocket for symbols: {self.symbols}")
                    self._connection_start_time = time.monotonic()  # 記錄連接成功時間
                    reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS  # 重置重連延遲

                    # 發送訂閱消息
                    subscribe_msg = await self._subscribe_message()
                    await ws.send(json.dumps(subscribe_msg))
                    self.logger.info(f"Sent subscription message: {subscribe_msg}")

                    while self.keep_running:
                        try:
                            # 檢查連接壽命，主動斷開並重連以避免 24 小時限制
                            if time.monotonic() - self._connection_start_time >= self.MAX_CONNECTION_LIFETIME_SECONDS:
                                self.logger.info(
                                    f"Binance WebSocket approaching 24-hour limit. Actively closing connection for graceful reconnect.")
                                # 通過拋出異常來觸發外層的重連邏輯
                                await ws.close(code=1000, reason="24-hour limit reached")
                                break  # 主動退出內層 while，讓外層重新 connect

                            message = await asyncio.wait_for(ws.recv(), timeout=30)  # 設置接收超時，以便檢查 keep_running 狀態和連接壽命
                            data = json.loads(message)

                            # Binance 可能會發送其他的消息，例如訂閱確認、錯誤等
                            if 'result' in data and data.get('id') == 1:
                                self.logger.info(f"Subscription confirmation received: {data}")
                            elif 'error' in data:
                                self.logger.error(f"Binance WebSocket received error: {data}")
                            elif data.get('e', None) == "bookTicker":  # 檢查是否是 bookTicker 更新
                                book_ticker = L1BookUpdate(
                                    symbol=data['s'],
                                    exchange='binance',
                                    bid_px=float(data['b']),
                                    bid_sz=float(data['B']),
                                    ask_px=float(data['a']),
                                    ask_sz=float(data['A']),
                                    event_timestamp=data['E'],
                                    tx_timestamp=data['T'], # Event time, Binance docs state it's in milliseconds
                                    price_received_ts=int(datetime.now().timestamp() * 1000)
                                )
                                # 使用消息隊列發布數據
                                await self.message_queue.publish(
                                    NATSConfig.build_market_l1_topic(book_ticker.symbol.lower(), "binance"),
                                    book_ticker
                                )
                                self.logger.debug(
                                    f"Published BookTicker for {book_ticker.symbol} at timestamp: {book_ticker.event_timestamp}")

                            else:
                                self.logger.debug(f"Received unknown WebSocket message: {data}") # 調試時可開啟

                        except asyncio.TimeoutError:
                            # print("No message received for 30 seconds, maintaining connection...")
                            # 這是正常情況，WebSocket 連接仍然活躍，只是暫時沒有數據
                            pass
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(
                                f"Binance WebSocket connection closed: Code={e.rcvd.code}, Reason={e.rcvd.reason}. Reconnecting...")
                            break  # 跳出內層循環，觸發外層的重連邏輯
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Error decoding JSON from WebSocket: {e}. Message: {message}",
                                              exc_info=True)
                        except Exception as e:
                            self.logger.error(f"Error processing Binance WebSocket message: {e}", exc_info=True)
                            # 在處理消息時發生未知錯誤，可以選擇在此處斷開以重連，或者繼續嘗試接收下一條消息
                            # 為了穩定性，這裡選擇斷開重連
                            break

            except websockets.exceptions.ConnectionClosedOK:
                # 正常關閉（例如達到24小時限制或 stop() 被調用）
                self.logger.info(
                    "Binance WebSocket connection closed gracefully. Attempting re-initialization or stopping.")
            except websockets.exceptions.ConnectionClosedError as e:
                self.logger.error(
                    f"Binance WebSocket connection closed with error: Code={e.rcvd.code}, Reason={e.rcvd.reason}. Retrying connection...",
                    exc_info=True)
            except asyncio.CancelledError:
                self.logger.info("BinanceDataFeed task cancelled. Stopping connection loop.")
                self.keep_running = False  # 確保停止循環
            except Exception as e:
                self.logger.error(
                    f"Unexpected error during Binance WebSocket connection: {e}. Retrying in {reconnect_delay} seconds...",
                    exc_info=True)

            if self.keep_running:
                self.logger.info(f"Waiting {reconnect_delay:.2f} seconds before reconnecting...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.RECONNECT_MAX_DELAY_SECONDS)  # 指數退避

    def stop(self):
        """
        停止數據訂閱器的運行。
        """
        self.logger.info("Stopping BinanceDataFeed...")
        self.keep_running = False




class BackPackDataFeed:
    _logger = logging.getLogger("data_feeds.BackPackDataFeed")
    WS_URL = "wss://ws.backpack.exchange"
    # BackPack WebSocket 連接限制
    MAX_CONNECTION_LIFETIME_SECONDS = 24 * 60 * 60 - 60  # 24小時 - 1分鐘的緩衝
    RECONNECT_INITIAL_DELAY_SECONDS = 1
    RECONNECT_MAX_DELAY_SECONDS = 60

    def __init__(self, symbols: list, market_type: str = "future"):
        """
        初始化 Binance 數據訂閱器。
        Args:
            symbols: 要訂閱的交易對列表，例如 ["BTCUSDT", "ETHUSDT"]
            market_type: "futures" (U本位合約) 或 "spot" (現貨)
        """
        # 實例級別的日誌器，指向類級別的日誌器
        self.ws_url = BackPackDataFeed.WS_URL
        self.logger = BackPackDataFeed._logger
        self.symbols = [s.upper() for s in symbols]  # 確保符號是大寫
        self.market_type = market_type.lower()
        self.keep_running = True
        self.message_queue: Optional[AsyncMessageQueue] = None  # 在 connect_and_listen 中異步獲取
        self._connection_start_time = 0  # 記錄當前連接的開始時間
    async def _initialize_message_queue(self):
        """異步獲取 AsyncMessageQueue 的單例實例。"""
        if self.message_queue is None:
            # 假設 AsyncMessageQueue 已經被 main.py 初始化並連接
            self.message_queue = await AsyncMessageQueue.get_instance()
            self.logger.info("AsyncMessageQueue instance obtained.")

    async def _subscribe_message(self):
        """生成訂閱消息 for bookTicker"""
        # 注意：Binance 的 stream 名稱是小寫
        streams = [f"bookTicker.{s}" for s in self.symbols]
        return {
            "method": "SUBSCRIBE",
            "params": streams,
        }

    async def connect_and_listen(self):
        """
        連接到 Binance WebSocket 並開始監聽數據。
        包含自動重連和 24 小時強制重連邏輯。
        """
        await self._initialize_message_queue()  # 確保消息隊列已初始化

        reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS

        while self.keep_running:
            self.logger.info(f"Attempting to connect to BackPack {self.market_type} WebSocket: {self.ws_url}")
            try:
                async with websockets.connect(
                        self.ws_url,
                        ping_interval=self.MAX_CONNECTION_LIFETIME_SECONDS,  # 設置一個較大的值，讓它不幹預 Binance 的 ping/pong
                        ping_timeout=self.RECONNECT_MAX_DELAY_SECONDS + 5  # 比重連最大延遲稍長，避免因為ping/pong timeout導致過早斷開
                ) as ws:
                    self.logger.info(
                        f"Successfully connected to BackPack {self.market_type} WebSocket for symbols: {self.symbols}")
                    self._connection_start_time = time.monotonic()  # 記錄連接成功時間
                    reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS  # 重置重連延遲

                    # 發送訂閱消息
                    subscribe_msg = await self._subscribe_message()
                    await ws.send(json.dumps(subscribe_msg))
                    self.logger.info(f"Sent subscription message: {subscribe_msg}")

                    while self.keep_running:
                        try:
                            # 檢查連接壽命，主動斷開並重連以避免 24 小時限制
                            if time.monotonic() - self._connection_start_time >= self.MAX_CONNECTION_LIFETIME_SECONDS:
                                self.logger.info(
                                    f"BackPack WebSocket approaching 24-hour limit. Actively closing connection for graceful reconnect.")
                                # 通過拋出異常來觸發外層的重連邏輯
                                await ws.close(code=1000, reason="24-hour limit reached")
                                break  # 主動退出內層 while，讓外層重新 connect

                            message = await asyncio.wait_for(ws.recv(), timeout=30)  # 設置接收超時，以便檢查 keep_running 狀態和連接壽命
                            data = json.loads(message)

                            if data.get('data') and data['data'].get('e')=='bookTicker':  # 檢查是否是 bookTicker 更新
                                data = data['data']
                                book_ticker = L1BookUpdate(
                                    symbol=data['s'],
                                    exchange='backpack',
                                    bid_px=float(data['b']),
                                    bid_sz=float(data['B']),
                                    ask_px=float(data['a']),
                                    ask_sz=float(data['A']),
                                    event_timestamp=int(float(data['E'])/1000),
                                    tx_timestamp=int(float(data['T'])/1000),
                                    price_received_ts= int(datetime.now().timestamp() * 1000)
                                    # Event time, Backpack docs state it's in nanoseconds
                                )


                                # 使用消息隊列發布數據
                                await self.message_queue.publish(
                                    NATSConfig.build_market_l1_topic(book_ticker.symbol.lower(), "backpack"),
                                    book_ticker
                                )
                                self.logger.debug(
                                    f"Published BookTicker for {book_ticker.symbol} at datetime: {datetime.fromtimestamp(book_ticker.event_timestamp / 10**6)}")
                            # else:
                            #     self.logger.debug(f"Received unknown WebSocket message: {data}") # 調試時可開啟

                        except asyncio.TimeoutError:
                            # print("No message received for 30 seconds, maintaining connection...")
                            # 這是正常情況，WebSocket 連接仍然活躍，只是暫時沒有數據
                            pass
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(
                                f"BackPack WebSocket connection closed: Code={e.rcvd.code}, Reason={e.rcvd.reason}. Reconnecting...")
                            break  # 跳出內層循環，觸發外層的重連邏輯
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Error decoding JSON from WebSocket: {e}. Message: {message}",
                                              exc_info=True)
                        except Exception as e:
                            self.logger.error(f"Error processing BackPack WebSocket message: {e}", exc_info=True)
                            # 在處理消息時發生未知錯誤，可以選擇在此處斷開以重連，或者繼續嘗試接收下一條消息
                            # 為了穩定性，這裡選擇斷開重連
                            break

            except websockets.exceptions.ConnectionClosedOK:
                # 正常關閉（例如達到24小時限制或 stop() 被調用）
                self.logger.info(
                    "BackPack WebSocket connection closed gracefully. Attempting re-initialization or stopping.")
            except websockets.exceptions.ConnectionClosedError as e:
                self.logger.error(
                    f"BBackPack WebSocket connection closed with error: Code={e.rcvd.code}, Reason={e.rcvd.reason}. Retrying connection...",
                    exc_info=True)
            except asyncio.CancelledError:
                self.logger.info("BackPackDataFeed task cancelled. Stopping connection loop.")
                self.keep_running = False  # 確保停止循環
            except Exception as e:
                self.logger.error(
                    f"Unexpected error during BackPack WebSocket connection: {e}. Retrying in {reconnect_delay} seconds...",
                    exc_info=True)

            if self.keep_running:
                self.logger.info(f"Waiting {reconnect_delay:.2f} seconds before reconnecting...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.RECONNECT_MAX_DELAY_SECONDS)  # 指數退避

    def stop(self):
        """
        停止數據訂閱器的運行。
        """
        self.logger.info("Stopping BinanceDataFeed...")
        self.keep_running = False


class MaxDataFeed:
    _logger = logging.getLogger("data_feeds.MaxDataFeed")
    WS_URL: str = 'wss://max-stream.maicoin.com/ws'
    # WebSocket 連接限制
    MAX_CONNECTION_LIFETIME_SECONDS = 24 * 60 * 60 - 60  # 24小時 - 1分鐘的緩衝
    RECONNECT_INITIAL_DELAY_SECONDS = 1
    RECONNECT_MAX_DELAY_SECONDS = 60

    def __init__(self, symbols: List[str], market_type: str = 'spot'):
        self.ws_url = MaxDataFeed.WS_URL
        self.logger = MaxDataFeed._logger
        self.symbols = [s.lower() for s in symbols]  # 確保符號是小寫，以便匹配 Max 的市場名稱
        self.keep_running = True  # 控制主循環 (由 start/stop/restart 管理)
        self.message_queue: Optional[AsyncMessageQueue] = None
        self._connection_start_time = 0  # 記錄當前連接的開始時間
        self.market_type = market_type

        # 用於管理內部任務和 WebSocket 連接
        self._main_loop_task: Optional[asyncio.Task] = None  # 主 WebSocket 連接和監聽任務
        self._reconnect_listener_task: Optional[asyncio.Task] = None  # NATS 重連請求監聽任務
        self._current_websocket: Optional[WebSocketClientProtocol] = None  # 當前激活的 WebSocket 連接

        # 新增一個事件，用於在外部請求重啟時，向當前 connect_and_listen 循環發出中斷信號
        self._internal_stop_event: asyncio.Event = asyncio.Event()

        self.logger.info(f"MaxDataFeed initialized for symbols: {self.symbols}, market_type: {self.market_type}")

    async def _initialize_message_queue(self):
        """異步獲取 AsyncMessageQueue 的單例實例。"""
        if self.message_queue is None:
            self.message_queue = await AsyncMessageQueue.get_instance()
            self.logger.info("AsyncMessageQueue instance obtained.")

    async def _subscribe_message(self):
        """生成訂閱消息 for book"""
        streams = [{'channel': 'book', 'market': s, 'depth': 10} for s in self.symbols]
        return {
            "action": "sub",
            "subscriptions": streams,
            'id': 'MaxDataFeed'
        }

    async def _reconnect_request_handler(self, subject: str, payload: Dict[str, Any]):
        """
        處理來自訂單簿管理器的重連請求事件。
        現在這個處理器將觸發 DataFeed 的完整重啟。
        """
        if payload.get("exchange") == "max" and payload.get("market_type") == self.market_type:
            reason = payload.get('reason', 'unknown')
            self.logger.warning(
                f"Received external reconnection request from MaxBookManager for {payload.get('exchange')}/{payload.get('market_type')}. Reason: {reason}. "
                f"Triggering a full DataFeed restart."
            )
            # 設置內部停止事件，這會導致當前的 _run_websocket_loop 退出
            self._internal_stop_event.set()
        else:
            self.logger.debug(f"Ignored reconnect request for other exchange/market_type: {payload}")

    async def _run_websocket_loop(self):
        """
        獨立的 WebSocket 連接和數據監聽循環。
        這個循環會在連接中斷、達到壽命限制或被外部請求停止時退出。
        """
        reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS

        while self.keep_running and not self._internal_stop_event.is_set():
            self.logger.info(f"Attempting to connect to Max WebSocket: {self.ws_url}")
            try:
                # 重置內部停止事件，為新的連接周期做準備
                self._internal_stop_event.clear()

                async with websockets.connect(
                        self.ws_url,
                        # 設置 ping_interval 和 ping_timeout 可能干擾交易所的行為
                        # 建議不設置或根據交易所建議設置
                        ping_interval=None,
                        ping_timeout=None
                ) as ws:
                    self._current_websocket = ws  # 儲存當前 WebSocket 實例
                    self.logger.info(
                        f"Successfully connected to Max WebSocket for symbols: {self.symbols}")
                    self._connection_start_time = time.monotonic()  # 記錄連接成功時間
                    reconnect_delay = self.RECONNECT_INITIAL_DELAY_SECONDS  # 重置重連延遲

                    # 發送訂閱消息
                    subscribe_msg = await self._subscribe_message()
                    await self._current_websocket.send(json.dumps(subscribe_msg))
                    self.logger.info(f"Sent subscription message: {subscribe_msg}")

                    # --- 主數據接收循環 ---
                    while self.keep_running and not self._internal_stop_event.is_set():
                        # 檢查連接壽命，主動斷開並重連以避免 24 小時限制
                        if time.monotonic() - self._connection_start_time >= self.MAX_CONNECTION_LIFETIME_SECONDS:
                            self.logger.info(
                                f"Max WebSocket approaching {self.MAX_CONNECTION_LIFETIME_SECONDS}s limit. Actively closing connection for graceful reconnect.")
                            await self._current_websocket.close(code=1000, reason="Lifetime limit reached")
                            break  # 退出內層循環，讓外層循環處理重連

                        try:
                            # 設置接收超時，以便定期檢查 keep_running 狀態和連接壽命
                            message = await asyncio.wait_for(self._current_websocket.recv(), timeout=30)
                            data = json.loads(message)

                            # --- 保持原始的數據鍵訪問方式 ---
                            if data.get('e') and data.get('e') == 'snapshot' and (data.get('a') and data.get('b')):

                                book_snapshot = MaxOrderBookSnapShot(
                                    symbol=data['M'],  # market,
                                    asks=data['a'],  # asks
                                    bids=data['b'],  # bids
                                    T=data['T'],  # created_at
                                    fi=data['fi'],  # First update ID
                                    li=data['li'],  # Last update ID
                                    v=data['v'],  # Event version
                                    price_received_ts=int(datetime.now().timestamp() * 1000)
                                )
                                # 使用消息隊列發布數據
                                await self.message_queue.publish(
                                    NATSConfig.build_market_l2_snapshot_topic(book_snapshot.symbol.lower(), "max"),
                                    # 確保符號小寫
                                    book_snapshot # 將 dataclass 實例轉換為字典以便發布
                                )
                                self.logger.debug(
                                    f"Published Book Snapshot for {book_snapshot.symbol} at datetime: {datetime.fromtimestamp(book_snapshot.T / 1000)} "
                                    f"BookTicker is {book_snapshot}")
                                # self.logger.error(f"{NATSConfig.TOPIC_MAX_SPOT_L2_BOOK_SNAPSHOT}.{book_snapshot.symbol.lower()}")

                            elif data.get('e') and data.get('e') == 'update':

                                book_update = MaxOrderBookUpdate(
                                    symbol=data['M'],  # market,
                                    asks=data['a'],  # asks
                                    bids=data['b'],  # bids
                                    T=data['T'],  # created_at
                                    fi=data['fi'],  # First update ID
                                    li=data['li'],  # Last update ID
                                    v=data['v'],  # Event version
                                    price_received_ts=int(datetime.now().timestamp() * 1000)
                                )
                                await self.message_queue.publish(
                                    NATSConfig.build_market_l2_update_topic(book_update.symbol.lower(), "max"),
                                    # 確保符號小寫
                                    book_update # 將 dataclass 實例轉換為字典以便發布
                                )
                                self.logger.debug(
                                    f"Published Book Update for {book_update.symbol} at datetime: {datetime.fromtimestamp(book_update.T / 1000)} "
                                    f"BookTicker is {book_update}")

                            elif data.get('e') == 'subscribed' and data.get('i') == 'MaxDataFeed':
                                self.logger.info(f"Subscription confirmation received: {data}")
                            else:
                                self.logger.debug(f"Received unknown WebSocket message: {data}")  # 調試時可開啟

                        except asyncio.TimeoutError:
                            self.logger.debug("No message received for 30 seconds, maintaining connection...")
                            pass  # 正常情況，沒有數據傳輸，但連接活躍
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(
                                f"Max WebSocket connection closed: Code={e.rcvd.code}, Reason={e.rcvd.reason}. Triggering reconnect...")
                            break  # 跳出內層循環，觸發外層的重連邏輯
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Error decoding JSON from WebSocket: {e}. Message: {message}",
                                              exc_info=True)
                            break  # JSON 解析錯誤，斷開重連
                        except Exception as e:
                            self.logger.error(f"Error processing Max WebSocket message: {e}", exc_info=True)
                            break  # 其他未知錯誤，斷開重連

            except websockets.exceptions.ConnectionClosedOK:
                self.logger.info(
                    "Max WebSocket connection closed gracefully. Attempting re-initialization or stopping.")
            except websockets.exceptions.ConnectionClosedError as e:
                self.logger.error(
                    f"Max WebSocket connection closed with error: Code={e.rcvd.code}, Reason={e.rcvd.reason}. Retrying connection...",
                    exc_info=True)
            except asyncio.CancelledError:
                self.logger.info("MaxDataFeed _run_websocket_loop task cancelled. Stopping connection loop.")
                # 此時 keep_running 可能已被 stop 或 restart 設置為 False
                break  # 任務被取消，退出主循環
            except Exception as e:
                self.logger.error(
                    f"Unexpected error during Max WebSocket connection: {e}. Retrying in {reconnect_delay} seconds...",
                    exc_info=True)

            # 如果 keep_running 仍然為 True 且沒有被 _internal_stop_event 觸發停止，則等待並重連
            if self.keep_running and not self._internal_stop_event.is_set():
                self.logger.info(f"Waiting {reconnect_delay:.2f} seconds before reconnecting...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, self.RECONNECT_MAX_DELAY_SECONDS)  # 指數退避
            else:
                self.logger.info(
                    "MaxDataFeed _run_websocket_loop terminating due to stop request or internal stop trigger.")
                break  # 退出外層重連循環

    async def start(self):
        """
        啟動 DataFeed 的連接和監聽循環。
        """
        await self._initialize_message_queue()  # 確保 NATS 消息隊列已初始化

        # 確保重連監聽任務只啟動一次
        if self._reconnect_listener_task is None or self._reconnect_listener_task.done():
            self._reconnect_listener_task = asyncio.create_task(
                self.message_queue.subscribe(NATSConfig.TOPIC_MAX_MARKET_SPOT_RECONNECT, handler=self._reconnect_request_handler),
                name="MaxDataFeed_Reconnect_Listener"
            )
            self.logger.info(f"Subscribed to reconnection requests on '{NATSConfig.TOPIC_MAX_MARKET_SPOT_RECONNECT}'")

        if self._main_loop_task and not self._main_loop_task.done():
            self.logger.info("MaxDataFeed is already running.")
            return

        self.logger.info("Starting MaxDataFeed...")
        self.keep_running = True  # 確保主循環可以運行
        self._internal_stop_event.clear()  # 清除任何歷史重啟觸發，準備新的運行

        self._main_loop_task = asyncio.create_task(
            self._run_websocket_loop(),  # 調用新的私有方法
            name="MaxDataFeed_MainLoop"
        )
        self.logger.info("MaxDataFeed main loop task started.")

    async def stop(self):
        """
        停止數據訂閱器的運行。
        """
        self.logger.info("Stopping MaxDataFeed...")
        self.keep_running = False  # 設置標誌，讓主循環停止
        self._internal_stop_event.set()  # 觸發內部停止事件，強制退出當前連接循環

        # 等待主循環任務結束
        if self._main_loop_task and not self._main_loop_task.done():
            self._main_loop_task.cancel()
            try:
                await self._main_loop_task
            except asyncio.CancelledError:
                pass  # 預期中的取消錯誤
            except Exception as e:
                self.logger.error(f"Error waiting for main loop task to cancel during stop: {e}", exc_info=True)
            finally:
                self._main_loop_task = None

        # 停止重連請求監聽任務
        if self._reconnect_listener_task and not self._reconnect_listener_task.done():
            self._reconnect_listener_task.cancel()
            try:
                await self._reconnect_listener_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.error(f"Error waiting for reconnect listener task to cancel during stop: {e}",
                                  exc_info=True)
            finally:
                self._reconnect_listener_task = None

        # 確保 WebSocket 連接被關閉
        if self._current_websocket:
            try:
                # 這裡不檢查 .closed 或 .open，直接嘗試關閉
                # 這是最可靠的方式，因為它能處理任何狀態
                await self._current_websocket.close()
                self.logger.info("WebSocket connection explicitly closed by stop().")
            except websockets.exceptions.ConnectionClosed:
                self.logger.info("WebSocket connection was already closed. No action needed.")
            except Exception as e:
                self.logger.error(f"Error closing WebSocket during stop: {e}", exc_info=True)
            finally:
                self._current_websocket = None

        self.logger.info("MaxDataFeed stopped.")

    async def restart(self, reason: str = "internal_request"):
        """
        徹底重啟 DataFeed，關閉所有連接並重新建立。
        這將模擬一個全新的連接，以期解決某些數據流問題。
        """
        self.logger.info(f"Initiating full restart of MaxDataFeed due to: {reason}")

        # 1. 設置內部停止事件，並取消當前主循環任務
        # 這會導致當前的 _run_websocket_loop 循環退出
        self._internal_stop_event.set()

        if self._main_loop_task and not self._main_loop_task.done():
            self._main_loop_task.cancel()
            try:
                # 等待任務真正結束，包括底層 WebSocket 的關閉
                await self._main_loop_task
            except asyncio.CancelledError:
                pass  # 預期中的取消錯誤
            except Exception as e:
                self.logger.error(f"Error waiting for main loop task to cancel during restart: {e}", exc_info=True)
            finally:
                self._main_loop_task = None  # 清除任務引用

        # 確保 WebSocket 連接被關閉
        if self._current_websocket and not self._current_websocket.closed:
            try:
                await self._current_websocket.close()
                self.logger.info("WebSocket connection explicitly closed for restart.")
            except Exception as e:
                self.logger.error(f"Error closing WebSocket during restart: {e}", exc_info=True)
            finally:
                self._current_websocket = None  # 清除引用

        # 短暫延遲，確保所有資源有時間釋放，尤其是在網絡層面
        await asyncio.sleep(0.5)

        # 2. 重置內部狀態並重新啟動
        self.keep_running = True  # 允許主循環重新啟動
        self._internal_stop_event.clear()  # 清除觸發器，準備新的生命週期
        self._connection_start_time = 0  # 重置連接開始時間

        self.logger.info("MaxDataFeed state reset. Attempting to re-establish connection.")
        await self.start()  # 呼叫 start 方法重新啟動主循環
        self.logger.info(f"MaxDataFeed restart completed. Reason: {reason}")


class ReferenceRateDataFeed:
    """
    Reference exchange rate data feed that fetches USD/TWD spot rates
    from external source and publishes them as USD/TWD in L1BookUpdate format.
    """
    _logger = logging.getLogger("data_feeds.ReferenceRateDataFeed")

    def __init__(self, fetch_interval_seconds: int = 60):
        """
        Initialize reference rate data feed.

        Args:
            fetch_interval_seconds: How often to fetch exchange rates (default: 60 seconds)
        """
        self.logger = ReferenceRateDataFeed._logger
        self.fetch_interval = fetch_interval_seconds
        self.keep_running = False
        self.message_queue: Optional[AsyncMessageQueue] = None
        self._fetch_task: Optional[asyncio.Task] = None
        
        # HTTP client settings to avoid being banned
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        self.url = "https://rate.bot.com.tw/xrt?Lang=zh-TW"
    
    async def _initialize_message_queue(self):
        """Initialize AsyncMessageQueue instance."""
        if self.message_queue is None:
            try:
                self.message_queue = await AsyncMessageQueue.get_instance()
                self.logger.info("AsyncMessageQueue instance obtained for ReferenceRateDataFeed.")
            except Exception as e:
                self.logger.error(f"Failed to initialize message queue: {e}")
                raise
    
    async def _fetch_usd_rates(self) -> Optional[Dict[str, float]]:
        """
        Fetch USD exchange rates from reference source.

        Returns:
            Dict with 'buy' and 'sell' rates, or None if failed
        """
        try:

            async with httpx.AsyncClient(
                headers=self.headers,
                timeout=30.0,
                follow_redirects=True
            ) as client:

                self.logger.debug("Fetching USD exchange rates from reference source...")
                response = await client.get(self.url)
                response.raise_for_status()
                
                # Parse HTML content
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find the exchange rate table
                rate_table = soup.find('table', {'title': '匯率'}) or soup.find('table', class_='table')
                
                if not rate_table:
                    self.logger.error("Could not find exchange rate table on page")
                    return None
                
                # Look for USD row in the table
                rows = rate_table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) < 5:  # Need at least currency, cash buy, cash sell, spot buy, spot sell
                        continue
                    
                    # Check if this row contains USD data
                    currency_cell = cells[0].get_text(strip=True)
                    if 'USD' in currency_cell or '美金' in currency_cell:
                        try:
                            # Extract spot rates (即期匯率) - typically columns 3 and 4
                            # Format: [Currency][Cash Buy][Cash Sell][Spot Buy][Spot Sell]
                            spot_buy_text = cells[3].get_text(strip=True)  # Bank buys USD (we sell TWD)
                            spot_sell_text = cells[4].get_text(strip=True)  # Bank sells USD (we buy TWD)
                            
                            # Remove any non-numeric characters except decimal point
                            import re
                            spot_buy_clean = re.sub(r'[^\d.]', '', spot_buy_text)
                            spot_sell_clean = re.sub(r'[^\d.]', '', spot_sell_text)
                            
                            if spot_buy_clean and spot_sell_clean:
                                usd_twd_buy = float(spot_buy_clean)  # Bank buys USD at this TWD rate
                                usd_twd_sell = float(spot_sell_clean)  # Bank sells USD at this TWD rate
                                
                                self.logger.debug(f"Parsed USD rates - Buy: {usd_twd_buy}, Sell: {usd_twd_sell}")
                                
                                return {
                                    'usd_twd_buy': usd_twd_buy,
                                    'usd_twd_sell': usd_twd_sell
                                }
                            
                        except (ValueError, IndexError) as e:
                            self.logger.error(f"Error parsing USD rates from row: {e}")
                            continue
                
                self.logger.warning("USD rates not found in exchange rate table")
                return None
                
        except Exception as e:
            self.logger.error(f"Error fetching USD exchange rates: {e}")
            return None
    
    def _format_usd_twd_rates(self, usd_twd_rates: Dict[str, float]) -> Dict[str, float]:
        """
        Format USD/TWD rates for L1BookUpdate.
        
        Args:
            usd_twd_rates: Dict with 'usd_twd_buy' and 'usd_twd_sell' rates
            
        Returns:
            Dict with USD/TWD bid and ask rates
        """
        try:
            usd_twd_buy = usd_twd_rates['usd_twd_buy']   # Bank buys USD at this rate
            usd_twd_sell = usd_twd_rates['usd_twd_sell'] # Bank sells USD at this rate
            
            # For USD/TWD from trader perspective:
            # Bank buys USD = trader sells USD = bid rate
            # Bank sells USD = trader buys USD = ask rate
            
            self.logger.debug(f"USD/TWD rates - Bid: {usd_twd_buy:.6f}, Ask: {usd_twd_sell:.6f}")
            
            return {
                'bid': usd_twd_buy,   # Trader can sell USD at this rate
                'ask': usd_twd_sell   # Trader can buy USD at this rate
            }
            
        except Exception as e:
            self.logger.error(f"Error formatting USD/TWD rates: {e}")
            return {'bid': 0.0, 'ask': 0.0}
    
    async def _create_l1_book_update(self, usd_twd_rates: Dict[str, float]) -> L1BookUpdate:
        """
        Create L1BookUpdate from USD/TWD rates.
        
        Args:
            usd_twd_rates: Dict with 'bid' and 'ask' rates
            
        Returns:
            L1BookUpdate object
        """
        current_time = int(time.time() * 1000)
        
        return L1BookUpdate(
            symbol='usdtwd',
            exchange='taiwan_bank',
            bid_px=usd_twd_rates['bid'],
            bid_sz=1000000.0,  # Assume large size available
            ask_px=usd_twd_rates['ask'],
            ask_sz=1000000.0,  # Assume large size available
            tx_timestamp=current_time,
            event_timestamp=current_time,
            price_received_ts=current_time
        )
    
    async def _fetch_and_publish_loop(self):
        """Main loop that fetches rates and publishes updates."""
        self.logger.info(f"Starting Taiwan Bank data feed with {self.fetch_interval}s interval")
        
        while self.keep_running:
            try:
                # Fetch USD rates
                usd_rates = await self._fetch_usd_rates()
                
                if usd_rates:
                    # Format USD/TWD rates
                    usd_twd_rates = self._format_usd_twd_rates(usd_rates)
                    
                    if usd_twd_rates['bid'] > 0 and usd_twd_rates['ask'] > 0:
                        # Create L1BookUpdate
                        book_update = await self._create_l1_book_update(usd_twd_rates)
                        
                        # Publish to message queue
                        topic = NATSConfig.build_local_l1_topic("usdtwd", "taiwan_bank")
                        await self.message_queue.publish(topic, book_update.__dict__)
                        
                        self.logger.info(f"Published USD/TWD rates - Bid: {usd_twd_rates['bid']:.4f}, Ask: {usd_twd_rates['ask']:.4f}")
                    else:
                        self.logger.warning("Invalid USD/TWD rates calculated, skipping publish")
                else:
                    self.logger.warning("Failed to fetch USD rates from Taiwan Bank")
                
                # Wait for next fetch interval
                await asyncio.sleep(self.fetch_interval)
                
            except asyncio.CancelledError:
                self.logger.info("Taiwan Bank data feed was cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in Taiwan Bank data feed loop: {e}")
                # Wait a bit before retrying to avoid hammering the server
                await asyncio.sleep(min(self.fetch_interval, 30))
    
    async def start(self):
        """Start the Taiwan Bank data feed."""
        if self.keep_running:
            self.logger.warning("Taiwan Bank data feed is already running")
            return
        
        try:
            # Initialize message queue
            await self._initialize_message_queue()
            
            # Start the fetch loop
            self.keep_running = True
            self._fetch_task = asyncio.create_task(self._fetch_and_publish_loop())
            
            self.logger.info("Taiwan Bank data feed started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start Taiwan Bank data feed: {e}")
            self.keep_running = False
            raise
    
    async def stop(self):
        """Stop the Taiwan Bank data feed."""
        self.logger.info("Stopping Taiwan Bank data feed...")
        
        self.keep_running = False
        
        if self._fetch_task and not self._fetch_task.done():
            self._fetch_task.cancel()
            try:
                await self._fetch_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Taiwan Bank data feed stopped")
    
    def is_running(self) -> bool:
        """Check if the data feed is currently running."""
        return self.keep_running and self._fetch_task and not self._fetch_task.done()