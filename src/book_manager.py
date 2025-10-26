import asyncio
import logging
import dataclasses
import time
from typing import Optional, Dict, Any, List, Tuple, Callable, Awaitable, Literal
from datetime import datetime
from decimal import Decimal

# 假設這些是你的數據格式 dataclasses
from src.data_format import (
    MaxOrderBookSnapShot,  # 快照事件數據格式
    MaxOrderBookUpdate,  # 更新事件數據格式
    L2BookUpdate,  # 本地組建後的訂單簿數據格式
)
from msg_queue import AsyncMessageQueue
from system_config import NATSConfig
from src.config_enum import Exchange


class MaxBookManager:
    _logger = logging.getLogger("book_manager.MaxBookManager")

    def __init__(self, market_type='spot', on_resync_needed: Optional[Callable[[str, str], Awaitable[None]]] = None):
        """
        初始化訂單簿管理器。
        :param on_resync_needed: 當需要重新同步（獲取快照）時的回調函數。
        """
        self.logger = MaxBookManager._logger
        self.exchange = 'max'
        self.market_type = market_type
        self.mq: Optional[AsyncMessageQueue] = None
        self._listener_tasks: List[asyncio.Task] = []
        self._closing: bool = False
        self.on_resync_needed = on_resync_needed  # 新增：重新同步的回調函數

        self.order_books_data: Dict[str, Dict[str, Any]] = {}

        self.logger.info("MaxOrderBookManager initialized.")

    async def _initialize_message_queue(self):
        if self.mq is None:
            self.mq = await AsyncMessageQueue.get_instance()
            self.logger.info("AsyncMessageQueue instance obtained.")

    def _get_or_create_pair_data(self, pair: str) -> Dict[str, Any]:
        if pair not in self.order_books_data:
            self.order_books_data[pair] = {
                "order_book": {"asks": [], "bids": []},  # asks, bids
                "last_id": None,
                "version": None,
                "event_buffer": [],
                "has_snapshot": False,
                "first_update_processed": False,  # 追蹤是否處理過第一個更新
                "counter_snapshot": 0,
                "counter_update": 0,
                "event_timestamp": None,
                "last_local_timestamp": None,
            }
            self.logger.info(f"Initialized order book data structure for pair: {pair}")
        return self.order_books_data[pair]

    def _reset_pair_data(self, pair: str, reason: str = "unknown"):
        """重置指定幣對的訂單簿狀態，並可選地觸發重新同步。"""
        self.logger.info(f"Resetting order book for pair: {pair} due to: {reason}")
        pair_data = self._get_or_create_pair_data(pair)
        pair_data["order_book"] = {"asks": [], "bids": []}
        pair_data["last_id"] = None
        pair_data["version"] = None
        pair_data["event_buffer"].clear()
        pair_data["has_snapshot"] = False
        pair_data["first_update_processed"] = False  # 重置時也要重置這個標誌
        pair_data["event_timestamp"] = None
        pair_data["last_local_timestamp"] = None
        pair_data["counter_snapshot"] = 0
        pair_data["counter_update"] = 0

        # 觸發重新同步的回調
        if self.mq:  # 確保消息隊列已初始化
            reconnect_payload = {
                "exchange": self.exchange,
                "market_type": self.market_type,
                "reason": reason,
                "timestamp": int(datetime.now().timestamp() * 1000)
            }
            # 使用 create_task 以避免阻塞 _reset_pair_data
            asyncio.create_task(self.mq.publish(NATSConfig.TOPIC_MAX_MARKET_SPOT_RECONNECT, reconnect_payload))
            self.logger.warning(
                f"[{pair}] Published reconnection request for {self.exchange}/{self.market_type} due to '{reason}'.")
        else:
            self.logger.error(
                f"[{pair}] Cannot publish reconnection request, AsyncMessageQueue not initialized.")

    def process_snapshot(self, pair: str, event: MaxOrderBookSnapShot):
        pair_data = self._get_or_create_pair_data(pair)

        # 規範: If you receive a snapshot event during the subscription process, please go back to step 3 to rebuild your local order book.
        # 這裡會完全替換，所以直接執行下面的邏輯即可
        pair_data["event_buffer"].clear()  # 清空事件緩衝區
        pair_data["first_update_processed"] = False  # 快照後，準備處理第一個更新
        pair_data["has_snapshot"] = True  # 標記已接收到 snapshot

        # 清空現有訂單簿並用 snapshot 填充
        pair_data["order_book"] = {"asks": [], "bids": []}

        if not event.asks and not event.bids:
            self.logger.info(f'[{pair}] Get empty snapshot. empty order book')
        else:
            # 核心修正：將所有數據存儲為 Decimal
            for quote in event.asks:
                pair_data["order_book"]["asks"].append([Decimal(str(quote[0])), Decimal(str(quote[1]))])
            for quote in event.bids:
                pair_data["order_book"]["bids"].append([Decimal(str(quote[0])), Decimal(str(quote[1]))])
            self.logger.info(
                f"[{pair}] Created order book snapshot. Asks: {len(pair_data['order_book']["asks"])}, Bids: {len(pair_data['order_book']["bids"])}")

        pair_data["last_id"] = event.li
        pair_data["version"] = event.v
        pair_data["event_timestamp"] = event.T
        pair_data["last_local_timestamp"] = datetime.now().timestamp() * 1000
        pair_data["counter_snapshot"] += 1
        self.logger.debug(
            f"[{pair}] Snapshot processed. version: {pair_data['version']}, last_id: {pair_data['last_id']}")

    def process_update(self, pair: str, event: MaxOrderBookUpdate):
        pair_data = self._get_or_create_pair_data(pair)

        if not pair_data["has_snapshot"]:
            self.logger.debug(f'[{pair}] Snapshot not yet arrived, saved to the update buffer.')
            pair_data["event_buffer"].append(event)
            return

        event_version = event.v
        # 規範 6: First verify if its v is the same. If it's different, go back to step 1 to resubscribe.
        if event_version != pair_data["version"]:
            self.logger.info(
                f"[{pair}] Misaligned version: {event_version} vs {pair_data['version']}. Triggering resync.")
            self._reset_pair_data(pair, reason="version_mismatch")  # 觸發重置並請求重同步
            return

        event_fi = event.fi
        event_li = event.li

        # 規範 7: Drop any event where fi <= local li.
        if pair_data["last_id"] is not None and event_fi <= pair_data["last_id"]:
            self.logger.debug(f"[{pair}] Dropping event (fi={event_fi} <= last_id={pair_data['last_id']}).")
            return

        # 規範 8: The first update event to be processed must satisfy fi <= (local li + 1) <= li; otherwise, go back to step 1 to resubscribe.
        if not pair_data["first_update_processed"]:
            # 必須有快照且 local_id 已經設定（即快照已處理）
            if pair_data["last_id"] is not None and \
                    not (event_fi <= (pair_data["last_id"] + 1) <= event_li):
                self.logger.error(
                    f"[{pair}] First update event discontinuity after snapshot. Expected fi <= (local li + 1) <= li, got fi={event_fi}, local li={pair_data['last_id']}, li={event_li}. Triggering resync.")
                self._reset_pair_data(pair, reason="first_update_discontinuity")  # 觸發重置並請求重同步
                return
            pair_data["first_update_processed"] = True  # 標記為已處理第一個更新

        self.logger.debug(f"[{pair}] Max update event: asks={event.asks}, bids={event.bids}")
        event_dict = dataclasses.asdict(event)
        self.logger.debug(
            f'[{pair}] Event dict: {event_dict}')  # Changed to DEBUG, CRITICAL might be too much for every update

        for side in ["asks", "bids"]:
            updates_for_side = event_dict.get(side, [])

            # 將當前訂單簿的價格檔位轉換為字典，以便 O(1) 查找和更新
            # 鍵為價格 (Decimal)，值為數量 (Decimal)
            temp_book_dict = {item[0]: item[1] for item in pair_data["order_book"].get(side, [])}

            for update in updates_for_side:
                price = Decimal(str(update[0]))
                amount = Decimal(str(update[1]))

                self.logger.debug(
                    f"[{pair}] Processing update: side={side}, price={price!r}, amount={amount!r}")

                if amount == Decimal('0'):
                    # 如果數量為0，表示刪除此價格檔位
                    if price in temp_book_dict:
                        del temp_book_dict[price]
                        self.logger.debug(f"[{pair}] Removed {side}: {price!r}")
                    else:
                        self.logger.debug(
                            f"[{pair}] Attempted to remove non-existent {side} level with zero amount @ {price!r}. (Normal per spec)")
                else:
                    # 如果數量不為0，表示更新或新增此價格檔位
                    if price in temp_book_dict:
                        # 更新現有檔位的數量
                        temp_book_dict[price] = amount
                        self.logger.debug(f"[{pair}] Updated {side}: {price!r}, {amount!r}")
                    else:
                        # 新增價格檔位
                        temp_book_dict[price] = amount
                        self.logger.debug(f"[{pair}] Added {side}: {price!r}, {amount!r}")

            # 將更新後的字典轉換回列表，並重新賦值給 order_book
            pair_data["order_book"][side] = [[price, amount] for price, amount in temp_book_dict.items()]

            # 每次更新後都重新排序
            if side == "asks":  # 賣方價格從小到大
                pair_data["order_book"]["asks"].sort(key=lambda x: x[0])
            else:  # 買方價格從大到小
                pair_data["order_book"]["bids"].sort(key=lambda x: x[0], reverse=True)

        pair_data["last_id"] = event.li  # 更新最後處理的 ID
        pair_data["version"] = event_version  # 保持版本一致
        pair_data["event_timestamp"] = event.T
        pair_data["last_local_timestamp"] = datetime.now().timestamp() * 1000
        pair_data["counter_update"] += 1
        self.logger.debug(f"[{pair}] Update processed. last_id updated to: {pair_data['last_id']}")

    def flush_event_buffer(self, pair: str):
        pair_data = self._get_or_create_pair_data(pair)

        if not pair_data["has_snapshot"]:
            # self.logger.debug(f'[{pair}] Snapshot has not arrived, buffer not flushed.')
            return

        if not pair_data["event_buffer"]:
            self.logger.debug(f'[{pair}] Event buffer is empty, no flush needed.')
            return

        self.logger.info(f'[{pair}] Flushing event buffer ({len(pair_data["event_buffer"])} events)...')

        pair_data["event_buffer"].sort(key=lambda x: x.fi)

        processed_count = 0
        while pair_data["event_buffer"]:
            next_event = pair_data["event_buffer"][0]

            # 規範：緩衝區事件也需要滿足連續性
            # 快照之後的第一個緩衝區事件必須滿足規範8的條件
            if not pair_data["first_update_processed"]:
                if pair_data["last_id"] is not None and \
                        not (next_event.fi <= (pair_data["last_id"] + 1) <= next_event.li):
                    self.logger.error(
                        f"[{pair}] First buffered event discontinuity after snapshot. Expected fi <= (local li + 1) <= li, got fi={next_event.fi}, local li={pair_data['last_id']}, li={next_event.li}. Resetting.")
                    self._reset_pair_data(pair, reason="buffered_first_update_discontinuity")
                    break  # 重置後退出循環
                pair_data["first_update_processed"] = True  # 標記為已處理第一個更新

            # 檢查是否是預期的下一個事件 (必須是 last_id + 1)
            # 或者如果 last_id 為 None (剛重置後) 則處理第一個事件
            if pair_data["last_id"] is None or next_event.fi == (pair_data["last_id"] + 1):
                self.process_update(pair, next_event)
                pair_data["event_buffer"].pop(0)  # 處理後移除
                processed_count += 1
            elif next_event.fi <= pair_data["last_id"]:
                self.logger.warning(
                    f"[{pair}] Dropping stale event from buffer (fi={next_event.fi} <= last_id={pair_data['last_id']}).")
                pair_data["event_buffer"].pop(0)
            else:
                # 發現不連續的事件，無法按序處理
                self.logger.error(
                    f"[{pair}] Event buffer discontinuity detected (expected fi={pair_data['last_id'] + 1}, got {next_event.fi}). Resetting.")
                self._reset_pair_data(pair, reason="buffer_discontinuity")
                break  # 重置後退出循環

        self.logger.info(f'[{pair}] Flushed {processed_count} events from buffer.')

    def get_order_book_depth(self, pair: str) -> Tuple[int, int]:
        pair_data = self.order_books_data.get(pair)
        if not pair_data:
            return 0, 0
        return len(pair_data["order_book"]["asks"]), len(pair_data["order_book"]["bids"])

    def get_best_price(self, pair: str, side: Literal['asks', 'bids']) -> Optional[float]:
        """
        Get the best price for a given trading pair and side.
        
        Args:
            pair: Trading pair symbol (e.g., 'BTCUSDT')
            side: Order book side - 'asks' for best offer, 'bids' for best bid
            
        Returns:
            Optional[float]: Best price for the specified side, or None if not available
        """
        pair_data = self.order_books_data.get(pair)
        
        if not pair_data:
            self.logger.warning(f"No order book data found for pair: {pair}")
            return None
            
        order_book = pair_data.get("order_book", {})
        side_data = order_book.get(side, [])
        
        if not side_data:
            self.logger.debug(f"No {side} data available for pair: {pair}")
            return None
        
        # For asks: best price is the lowest (first in sorted list)
        # For bids: best price is the highest (first in reverse-sorted list)
        best_price_decimal = side_data[0][0]  # [price, amount] - get price
        
        try:
            return float(best_price_decimal)
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error converting best price to float for {pair} {side}: {e}")
            return None

    def get_mid_price(self, pair: str) -> Optional[float]:
        """
        Get the mid price for a given trading pair.
        Mid price = (best_ask + best_bid) / 2
        
        Args:
            pair: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            Optional[float]: Mid price, or None if ask/bid data not available
        """
        best_ask = self.get_best_price(pair, 'asks')
        best_bid = self.get_best_price(pair, 'bids')
        
        if best_ask is not None and best_bid is not None:
            return (best_ask + best_bid) / 2.0
        else:
            self.logger.debug(f"Cannot calculate mid price for {pair} - ask: {best_ask}, bid: {best_bid}")
            return None

    def parse_local_order_book(self, pair: str, event_timestamp_from_feed: int) -> Optional[L2BookUpdate]:
        pair_data = self.order_books_data.get(pair)
        if not pair_data or (not pair_data["order_book"]["asks"] and not pair_data["order_book"]["bids"]):
            self.logger.debug(f"[{pair}] Empty order book, ignoring parse request.")
            return None

        asks_depth, bids_depth = self.get_order_book_depth(pair)

        # 核心修正：將 Decimal 轉換為 str 在這裡進行，用於發布
        asks_str = [[str(price), str(amount)] for price, amount in pair_data["order_book"]["asks"]]
        bids_str = [[str(price), str(amount)] for price, amount in pair_data["order_book"]["bids"]]

        event_created_at = event_timestamp_from_feed

        return L2BookUpdate(
            exchange=self.exchange,
            symbol=pair,
            asks=asks_str,  # 使用轉換後的字串列表
            bids=bids_str,  # 使用轉換後的字串列表
            tx_timestamp=event_created_at,
            event_timestamp=event_created_at,
            price_received_ts=int(datetime.now().timestamp() * 1000),
            depth=min(asks_depth, bids_depth)
        )

    async def _message_handler(self, subject: str, raw_event_data: Dict[str, Any]):
        if self._closing:
            self.logger.debug(f"Manager is closing, dropping message for {subject}.")
            return

        parts = subject.split('.')
        if len(parts) < 6:
            self.logger.warning(f"Invalid NATS subject format: {subject}. Skipping.")
            return
        data_type, market_type,sub_type,event_type, pair, exchange = parts
        start_time = time.monotonic()

        pair_data = self._get_or_create_pair_data(pair)

        try:
            if event_type == "snapshot":
                event = MaxOrderBookSnapShot(**raw_event_data)
                self.logger.info(f"[{pair}] Snapshot event received (li={event.li}).")
                self.process_snapshot(pair, event)
                self.flush_event_buffer(pair)

            elif event_type == "update":
                event = MaxOrderBookUpdate(**raw_event_data)
                self.logger.debug(f"[{pair}] Update event received (fi={event.fi}, li={event.li}).")
                self.process_update(pair, event)
            else:
                self.logger.warning(f"[{pair}] Unknown order book event type: {event_type} on subject {subject}.")
                return

            local_order_book = self.parse_local_order_book(pair, raw_event_data.get('T'))

            if local_order_book:
                # 核心修正：打印更深層的 asks/bids 以便觀察
                await self.mq.publish(NATSConfig.build_local_l2_topic(pair, "max"),
                                      local_order_book)


        except TypeError as e:
            # self.logger.warning(
            #     f"[{pair}] Received malformed Max Order Book data on topic {subject}: {raw_event_data}. Error: {e}")
            self._reset_pair_data(pair, reason=f"malformed_data:{e}")
        except Exception as e:
            # self.logger.error(
            #     f"[{pair}] Unexpected error processing event on topic {subject}: {raw_event_data}. Error: {e}",
            #     exc_info=True)
            self._reset_pair_data(pair, reason=f"unexpected_error:{e}")

        self.logger.debug(f"[{pair}] Event handler took {time.monotonic() - start_time:.4f} seconds.")




    async def start_listening(self):
        await self._initialize_message_queue()

        snapshot_subject = NATSConfig.pattern_exchange_market_l2_snapshot(exchange=Exchange.MAX.value)
        update_subject = NATSConfig.pattern_exchange_market_l2_updates(exchange=Exchange.MAX.value)

        self._listener_tasks.append(asyncio.create_task(
            self.mq.subscribe(snapshot_subject, handler=self._message_handler),
            name=f"MaxOrderBook_Snapshot_Listener"
        ))
        self._listener_tasks.append(asyncio.create_task(
            self.mq.subscribe(update_subject, handler=self._message_handler),
            name=f"MaxOrderBook_Update_Listener"
        ))

        self.logger.info(f"Subscribed to Max Order Book {self.market_type} snapshots on '{snapshot_subject}'")
        self.logger.info(f"Subscribed to Max Order Book {self.market_type} updates on '{update_subject}'")

        try:
            await asyncio.gather(*self._listener_tasks)
        except asyncio.CancelledError:
            self.logger.info("MaxOrderBookManager listening tasks cancelled.")
        except Exception as e:
            self.logger.error(f"Error in MaxOrderBookManager main listening loop: {e}", exc_info=True)

    async def stop(self):
        self.logger.info("Stopping MaxOrderBookManager...")
        self._closing = True

        for task in self._listener_tasks:
            if task and not task.done():
                task.cancel()

        try:
            await asyncio.gather(*[t for t in self._listener_tasks if t and not t.done()], return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error during graceful shutdown of MaxOrderBookManager tasks: {e}", exc_info=True)

        self.logger.info("MaxOrderBookManager stopped.")