import time

from max_utilis import get_parsed_max_k_line_df
from reference_rate_provider import fetch_quote_closest_to_15_00_async
from msg_queue import AsyncMessageQueue
from system_config import  NATSConfig
import logging
import asyncio
from typing import Optional, Any, Dict, Tuple, Union
from datetime import datetime, timezone, timedelta, time
from src.config_enum import OrderSide
from dataclasses import asdict
from src.config_enum import Exchange, WalletType, RunningStrategy
from position_manager import MaxPositionManager
import holidays
from market_data import MarketData
from src.data_format import Signal
from settings import SpreadMode, TxFeeConfig
from api_client import MaxAPIClient
from settings import STRATEGY_CONFIG


class StableScanner:
    _logger = logging.getLogger('arb_scanner.StableScanner')
    _signal_logger = logging.getLogger('arb_scanner.StableScanner.signals')

    def __init__(self,
                 position_manager: MaxPositionManager,
                 market_data: MarketData):
        self.logger = StableScanner._logger
        self.signal_logger = StableScanner._signal_logger
        self.mq: Optional[AsyncMessageQueue] = None
        self.position_manager: MaxPositionManager = position_manager
        self.tw_holidays = holidays.TW(years = datetime.today().year)
        self.market_data = market_data
        self.symbol_info = {}
        self.is_running = True
        # self.threshold_setting = {'open': , 'close'}
        self.premium_info: Dict[str, Optional[Union[float, int]]] = {'long_ewma_premium': None,
                                                                     'short_ewma_premium': None,
                                                                     'ewma_mad': None,
                                                                     'long_alpha': 0.9958,
                                                                     'short_alpha':0.935,
                                                                     'last_updated': None
                                                                     }
        self.premium_info_in_close_time: Dict[str, Optional[Union[float, int, Dict[str,Optional[float, int]]]]] = {
            'benchmark': {'ma_price': None, 'last_updated': None, 'alpha': 0.98, 'benchmark_premium': None,'quote_timestamp': None} ,
            'long_ewma_premium': None,
            'short_ewma_premium': None,
            'shrink_premium': None,
            'shrink_rho':0.7,
            'ewma_mad': None,
            'long_alpha': 0.9958,
            'short_alpha':0.935,
            'last_updated': None
        }
        self._close_time_data_lock = asyncio.Lock()
        self._signal_task: Optional[asyncio.Task] = None
        self._reset_benchmark_price_task: Optional[asyncio.Task] = None
        self._update_benchmark_price_task: Optional[asyncio.Task] = None

    async def _initialize_message_queue(self):
        if self.mq is None:
            self.mq = await AsyncMessageQueue.get_instance()
            self.logger.info("AsyncMessageQueue instance obtained.")


    def is_in_open_market(self, utc_timestamp:int)-> Tuple[bool, bool]:
        """
        Check if the given timestamp is during market hours and on a business day.

        :param utc_timestamp: UTC timestamp in milliseconds
        :return: is_working_day: bool, is_open: bool
        """
        utc_time = datetime.fromtimestamp(int(utc_timestamp/1000),tz=timezone.utc)
        tz_tw = timezone(timedelta(hours=8))
        local_time = utc_time.astimezone(tz_tw)

        # Check if it's a business day (not weekend and not holiday)
        is_weekend = local_time.weekday() >= 5  # Saturday=5, Sunday=6
        is_holiday = local_time.date() in self.tw_holidays

        if not is_weekend and not is_holiday:
            # It's a business day, now check if market is open (9:00-15:00)
            if 9 <= local_time.hour <= 15:
                return True, True
            else:
                return True, False
        else:
            # Weekend or holiday
            return False, False

    def _get_symbol_price_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self.symbol_info[symbol]

    async def reset_benchmark_price_periodically(self):
        self.logger.info("Start resetting benchmark price periodically.")
        while self.is_running:
            ts_now = int(datetime.now().timestamp()*1000)
            is_business, is_open = self.is_in_open_market(ts_now)
            local_date = self.convert_to_local_datetime(ts_now)

            if is_business:
                if 9 <= local_date.hour <= 14:
                    async with self._close_time_data_lock:
                        self.logger.info("Market opened. Resetting close-time benchmark data.")
                        self.premium_info_in_close_time['benchmark']['quote_timestamp'] = None
                        self.premium_info_in_close_time['benchmark']['last_updated'] = None
                        self.premium_info_in_close_time['benchmark']['ma_price'] = None
                        self.premium_info_in_close_time['benchmark']['benchmark_premium'] = None


            await asyncio.sleep(60*60)


    async def init_benchmark_price_periodically(self):
        self.logger.info("Start init benchmark price periodically.")
        while self.is_running:
            ts_now = int(datetime.now().timestamp()*1000)
            is_business, is_open = self.is_in_open_market(ts_now)
            premium_info_in_close_time = self.premium_info_in_close_time
            if not is_business or not is_open:
                if self.premium_info_in_close_time['benchmark']['last_updated']:
                    self.logger.info(f"benchmark moving average price exists: {self.premium_info_in_close_time['benchmark']['ma_price']} "
                                     f"updated at {self.premium_info_in_close_time['benchmark']['last_updated']}"
                                     f"skipping update")
                    return

                close_open_day = self.find_closest_business_day(ts_now) # this local date

                benchmark_dict = await fetch_quote_closest_to_15_00_async(close_open_day, logger=self.logger)

                client = MaxAPIClient()
                # sample_start_ts = int((datetime.strptime(close_open_day, '%Y-%m-%d') + timedelta(hours=15)).timestamp() * 1000)
                close_ts = int((datetime.strptime(close_open_day, '%Y-%m-%d') + timedelta(hours=6, minutes=50) ).timestamp() * 1000)
                k_lines_raw = await client.get_api_v3_k(period=1, limit=10000, symbol='usdttwd', timestamp=close_ts)
                if not k_lines_raw:
                    self.logger.info(f'Failed to get klines from {close_open_day}.')
                    return
                kline_df = get_parsed_max_k_line_df(k_lines_raw)
                self.logger.info(kline_df.head())
                kline_df['premium'] = kline_df['close'] / benchmark_dict['mid_price'] - 1
                target_hour_date_start = (datetime.strptime(close_open_day, '%Y-%m-%d') + timedelta(hours=6,minutes=50)).strftime(
                    '%Y-%m-%d %H:%M:%S'
                )
                target_hour_date_end = (
                            datetime.strptime(close_open_day, '%Y-%m-%d') + timedelta(hours=7)).strftime(
                    '%Y-%m-%d %H:%M:%S')

                benchmark_premium = kline_df.loc[target_hour_date_start: target_hour_date_end]['premium'].mean()
                self.logger.info(f'From {target_hour_date_start} to {target_hour_date_end}: {benchmark_premium:.4f}')
                # self.logger.warning(f'|DEBUG|benchmark_premium: {benchmark_premium}')
                # self.logger.info(f'|DEBUG|target_hour_date_start: {target_hour_date_start}')
                # self.logger.info(f'|DEBUG|target_hour_date_end: {target_hour_date_end}')
                # self.logger.info(f'|DEBUG|selected df: {kline_df.loc[target_hour_date_start: target_hour_date_end]}')
                # self.logger.info(f'|DEBUG|benchmark_premium: {benchmark_premium}')

                benchmark_price = benchmark_dict['mid_price']


                # initial data
                premium_info_in_close_time['benchmark']['benchmark_premium'] = benchmark_premium

                initial_mad = kline_df['premium'].iloc[-60:].abs().mean()
                initial_lma = kline_df['premium'].iloc[-60:].mean()
                initial_sma = kline_df['premium'].iloc[-1]

                shrink_rho = premium_info_in_close_time['shrink_rho']
                async with self._close_time_data_lock:
                    premium_info_in_close_time['benchmark']['ma_price'] = benchmark_price
                    premium_info_in_close_time['long_ewma_premium'] = initial_lma
                    premium_info_in_close_time['short_ewma_premium'] = initial_sma
                    premium_info_in_close_time['shrink_premium'] = initial_lma * (1-shrink_rho ) + \
                                                                   shrink_rho  * premium_info_in_close_time['benchmark']['benchmark_premium']
                    premium_info_in_close_time['ewma_mad'] = initial_mad
                    premium_info_in_close_time['last_updated'] =  ts_now

                self.logger.info(f'Initial benchmark info: benchmark_price={benchmark_price}, long_ewma={initial_lma}, short_ewma={initial_sma}, shrink_premium={premium_info_in_close_time['shrink_premium'] }')
            else:
                ts = int(datetime.now().timestamp() * 1000)
                local_time = self.convert_to_local_datetime(ts)
                self.logger.warning(f'skipping the init_benchmark_price_periodically, '
                                    f'local time: {local_time}, is_business: {is_business}, is_open: {is_open}')

            await asyncio.sleep(60)

    def _update_ewma_premium(self, new_usdt_px:float, new_usd_px: float, new_ts: int):
        """開市時間spread 的計算應不中斷，而收市時間的 spread 則以 benchmark price 為基準計算"""
        new_premium = new_usdt_px / new_usd_px - 1
        last_update = self.premium_info['last_updated']
        if not last_update:
            self.premium_info['long_ewma_premium'] = new_premium
            self.premium_info['short_ewma_premium'] = new_premium
            self.premium_info['ewma_mad'] = abs(new_premium)
            self.premium_info['last_updated'] = new_ts
            return

        current_long_premium = self.premium_info['long_ewma_premium']
        current_short_premium = self.premium_info['short_ewma_premium']
        current_mad = self.premium_info['ewma_mad']
        long_alpha = self.premium_info['long_alpha']
        short_alpha = self.premium_info['short_alpha']

        if new_ts == last_update:
            self.logger.debug(f'Skipping premium update since last update ts = new_ts = {new_ts:.3f}')
            return

        self.premium_info['long_ewma_premium'] = (1 - long_alpha) * new_premium + long_alpha * current_long_premium
        self.premium_info['short_ewma_premium'] = (1 - short_alpha) * new_premium + short_alpha * current_short_premium
        self.premium_info['ewma_mad'] = (1 - long_alpha) * abs(new_premium) + long_alpha * current_mad
        self.premium_info['last_updated'] = new_ts
        self.logger.info(f'Premium in open: lewma={self.premium_info['long_ewma_premium']:.4f}, '
                         f'sewma={self.premium_info['short_ewma_premium']:.4f}')

        if not  self.premium_info_in_close_time['benchmark']['ma_price']:
            self.logger.error(f'Can not find benchmark price in close time, skipping premium update')
            return

        premium_on_benchmark = new_usdt_px / self.premium_info_in_close_time['benchmark']['ma_price'] - 1

        if not self.premium_info_in_close_time['last_updated']:
            self.premium_info_in_close_time['long_ewma_premium'] = premium_on_benchmark
            self.premium_info_in_close_time['short_ewma_premium'] = premium_on_benchmark
            self.premium_info_in_close_time['ewma_mad'] = abs(premium_on_benchmark)
            self.premium_info_in_close_time['last_updated'] = new_ts
            return

        short_alpha = self.premium_info_in_close_time['short_alpha']
        long_alpha = self.premium_info_in_close_time['long_alpha']
        shrink_rho = self.premium_info_in_close_time['shrink_rho']
        benchmark_premium = self.premium_info_in_close_time['benchmark']['benchmark_premium']
        current_short_premium = self.premium_info_in_close_time['short_ewma_premium']
        current_long_premium = self.premium_info_in_close_time['long_ewma_premium']
        current_mad =  self.premium_info_in_close_time['ewma_mad']


        self.premium_info_in_close_time['long_ewma_premium'] = (1 - long_alpha) *   premium_on_benchmark + long_alpha * current_long_premium
        self.premium_info_in_close_time['short_ewma_premium'] = ( 1 - short_alpha) * premium_on_benchmark + short_alpha * current_short_premium
        self.premium_info_in_close_time['shrink_premium'] = (1-shrink_rho) * self.premium_info_in_close_time['long_ewma_premium']  + shrink_rho * benchmark_premium
        self.premium_info_in_close_time['ewma_mad'] = (1 - long_alpha) * abs( premium_on_benchmark) + long_alpha * current_mad
        self.premium_info_in_close_time['last_updated'] = new_ts
        self.logger.info(f'Premium in close: lewma={self.premium_info_in_close_time['long_ewma_premium']:.4f}, '
                         f'sewma={self.premium_info_in_close_time['short_ewma_premium']:.4f}, '
                         f'shrink_premium= {self.premium_info_in_close_time['shrink_premium']:.4f}'
                         )

        self.logger.debug(f"Done update at {new_ts}")

    @staticmethod
    def is_in_close_range(utc_timestamp:int, buffer_seconds: int = 300) -> bool:
        """
        檢查 dt 是否落在 15:00 ± buffer_seconds 範圍內
        dt 必須是 tz-aware datetime，建議轉成台灣時間
        """
        # 取當天的 15:00
        utc_time = datetime.fromtimestamp(int(utc_timestamp/1000), tz=timezone.utc)
        tz_tw = timezone(timedelta(hours=8))
        local_time = utc_time.astimezone(tz_tw)
        target = datetime.combine(local_time.date(), time(15, 0), tzinfo=local_time.tzinfo)

        lower_bound = target - timedelta(seconds=buffer_seconds)

        return lower_bound <= local_time <= target

    @staticmethod
    def convert_to_local_datetime(utc_timestamp:int) -> datetime:
        utc_time = datetime.fromtimestamp(int(utc_timestamp/1000), tz=timezone.utc)
        tz_tw = timezone(timedelta(hours=8))
        local_time = utc_time.astimezone(tz_tw)
        return local_time



    def _check_position_limits(self) -> Tuple[bool, bool]:
        """
        Check if position limits allow for new signals.

        Returns:
            bool: True if position limits are okay
        """
        try:
            wallet_type = WalletType.SPOT
            strategy = RunningStrategy.ARBITRAGE
            # self.logger.info(f"Checking position limits for {self.target_symbol}")
            # Check if strategy allowance is available
            buy_allowed, sell_allowed, = self.position_manager.is_strategy_allowance_available(
                strategy=strategy,
                wallet_type=wallet_type,
                symbol='usdttwd',
                order_size=STRATEGY_CONFIG.TICKET_SIZE[strategy.value]  # TEST_SCANNER_CONFIG ticket size
            )
            self.logger.warning(f'Position limits allow for new signals: buy:{buy_allowed}, sell:{sell_allowed}')

            return buy_allowed, sell_allowed
        except Exception as e:
            self.logger.warning(f"Error checking position limits: {e}")
            return False , False # Be conservative on errors

    def _create_signal(self,symbol:str, target_px:float, side: OrderSide, spread_mode: SpreadMode, utc_ts:int) -> Signal:
        """
        Create a Signal object with current market data.

        Args:
            side: Order side (BUY or SELL)

        Returns:
            Signal: Generated signal
        """
        current_time = int(datetime.now().timestamp() * 1000)

        signal = Signal(
            symbol=symbol,
            expected_spread=spread_mode.value,  # Always WIDE spread
            attempted_mid=target_px,
            local_timestamp=current_time,
            time_elapsed=current_time - utc_ts,
            side=side,
            strategy_id=RunningStrategy.ARBITRAGE.value,
            wallet_type=WalletType.SPOT.value,
            maker=True,
            price_received_ts=  utc_ts,
            signal_generated_ts=current_time,
            spread_mode=spread_mode.value
        )

        return signal

    # TODO: replace harcoded tx fee with configuration
    def is_short_crossing_long(self, up_down: str, new_ts: int):

        is_business, is_open = self.is_in_open_market(new_ts)
        fee = 2 * TxFeeConfig.MAKER_FEE
        if is_business and is_open:
            long_ewma_premium = self.premium_info['long_ewma_premium']
            short_ewma_premium = self.premium_info['short_ewma_premium']

            if (not long_ewma_premium) or (not short_ewma_premium) :
                self.logger.info(
                    f"short_ewma_premium: {self.premium_info['short_ewma_premium']}, long_ewma_premium: {self.premium_info['long_ewma_premium']}, "
                    f" either of them is not set ")
                return False


            if up_down =='up':
                if self.premium_info['short_ewma_premium'] > self.premium_info['long_ewma_premium'] + fee:
                    return True
            elif up_down == 'down':
                if self.premium_info['short_ewma_premium'] < self.premium_info['long_ewma_premium'] - fee:
                    return True
        else:
            long_ewma_premium = self.premium_info_in_close_time['long_ewma_premium']
            short_ewma_premium = self.premium_info_in_close_time['short_ewma_premium']
            shrink_premium =   self.premium_info_in_close_time['shrink_premium']

            if (not long_ewma_premium) or (not short_ewma_premium) or not shrink_premium:
                self.logger.info(
                    f"short_ewma_premium: {short_ewma_premium}, long_ewma_premium: { long_ewma_premium}, "
                    f"shrink_mean: {shrink_premium}. either of them is not set ")
                return False

            if up_down == 'up':
                if self.premium_info_in_close_time['short_ewma_premium'] > shrink_premium + fee:
                    return True
            elif up_down == 'down':
                if self.premium_info_in_close_time['short_ewma_premium'] <shrink_premium - fee:
                    return True
        return False


    def get_ewma_mad_by_ts(self, utc_timestamp:int) -> float:
        is_business, is_open = self.is_in_open_market(utc_timestamp)
        if is_business and is_open:
            return self.premium_info['ewma_mad']
        else:
            return self.premium_info_in_close_time['ewma_mad']

    async def _generate_signal_loop(self):
        self.logger.debug("Starting test signal generator loop")
        while self.is_running:
            try:
                buy_allowed, sell_allowed = self._check_position_limits()


                mid_px_usd = self.market_data.get_exchange_mid_price(exchange=Exchange.REFERENCE_SOURCE.value, symbol='usdtwd')
                mid_px_usdt = self.market_data.get_exchange_mid_price(exchange=Exchange.MAX.value, symbol='usdttwd')
                if not mid_px_usdt or not mid_px_usd:
                    self.logger.warning(f'either of usd or usdt is none. mid_px_usd: {mid_px_usd}, mid_px_usdt: {mid_px_usdt}, sleep 3 seconds')
                    await asyncio.sleep(3)
                    continue
                new_ts = int(datetime.now().timestamp() * 1000)

                self._update_ewma_premium(new_ts=new_ts, new_usd_px=mid_px_usd ,
                                                new_usdt_px=mid_px_usdt)

                if self.is_short_crossing_long('up', new_ts=new_ts):
                    if sell_allowed:
                        sell_signal = self._create_signal(symbol='usdttwd',
                                                     target_px=mid_px_usdt,
                                                     side=OrderSide.SELL,
                                                     spread_mode=SpreadMode.PAR,
                                                          utc_ts=new_ts)
                        topic = NATSConfig.build_strategy_signal_topic('usdttwd', exchange=Exchange.MAX.value, strategy=RunningStrategy.ARBITRAGE.value)
                        await self.mq.publish(topic ,asdict(sell_signal))
                        self.signal_logger.info(f"🔴 SELL SIGNAL [TIGHT] - Price: {mid_px_usdt:.4f}, Premium: {(mid_px_usdt/mid_px_usd-1)*100:.3f}%")

                    if buy_allowed:
                        buy_signal = self._create_signal(symbol='usdttwd',
                                                        target_px=mid_px_usdt,
                                                        side=OrderSide.BUY,
                                                        spread_mode=SpreadMode.WIDE,
                                                         utc_ts=new_ts)
                        topic = NATSConfig.build_strategy_signal_topic('usdttwd', exchange=Exchange.MAX.value,
                                                                       strategy=RunningStrategy.ARBITRAGE.value)
                        await self.mq.publish(topic, asdict(buy_signal))
                        self.signal_logger.info(f"🟢 BUY SIGNAL [WIDE] - Price: {mid_px_usdt:.4f}, Premium: {(mid_px_usdt/mid_px_usd-1)*100:.3f}%")



                elif self.is_short_crossing_long('down',new_ts=new_ts):
                    if buy_allowed:
                        buy_signal = self._create_signal(symbol='usdttwd',
                                                         target_px=mid_px_usdt,
                                                         side=OrderSide.BUY,
                                                         spread_mode=SpreadMode.PAR,
                                                         utc_ts=new_ts)
                        topic = NATSConfig.build_strategy_signal_topic('usdttwd', exchange=Exchange.MAX.value,
                                                                       strategy=RunningStrategy.ARBITRAGE.value)
                        await self.mq.publish(topic, asdict(buy_signal))
                        self.signal_logger.info(f"🟢 BUY SIGNAL [TIGHT] - Price: {mid_px_usdt:.4f}, Premium: {(mid_px_usdt/mid_px_usd-1)*100:.3f}%")

                    if sell_allowed:
                        sell_signal = self._create_signal(symbol='usdttwd',
                                                          target_px=mid_px_usdt,
                                                          side=OrderSide.SELL,
                                                          spread_mode=SpreadMode.WIDE,
                                                          utc_ts=new_ts)
                        topic = NATSConfig.build_strategy_signal_topic('usdttwd', exchange=Exchange.MAX.value,
                                                                       strategy=RunningStrategy.ARBITRAGE.value)
                        await self.mq.publish(topic, asdict(sell_signal))
                        self.signal_logger.info(f"SELL SIGNAL [WIDE] - Price: {mid_px_usdt:.4f}, Premium: {(mid_px_usdt/mid_px_usd-1)*100:.3f}%")
                else:
                    if buy_allowed:
                        buy_signal = self._create_signal(symbol='usdttwd',
                                                         target_px=mid_px_usdt,
                                                         side=OrderSide.BUY,
                                                         spread_mode=SpreadMode.NORMAL,
                                                         utc_ts=new_ts)
                        topic = NATSConfig.build_strategy_signal_topic('usdttwd', exchange=Exchange.MAX.value,
                                                                       strategy=RunningStrategy.ARBITRAGE.value)
                        await self.mq.publish(topic, asdict(buy_signal))
                        self.signal_logger.info(f"BUY SIGNAL [NORMAL] - Price: {mid_px_usdt:.4f}, Premium: {(mid_px_usdt/mid_px_usd-1)*100:.3f}%")

                    if sell_allowed:
                        sell_signal = self._create_signal(symbol='usdttwd',
                                                          target_px=mid_px_usdt,
                                                          side=OrderSide.SELL,
                                                          spread_mode=SpreadMode.NORMAL,
                                                          utc_ts=new_ts)
                        topic = NATSConfig.build_strategy_signal_topic('usdttwd', exchange=Exchange.MAX.value,
                                                                       strategy=RunningStrategy.ARBITRAGE.value)
                        await self.mq.publish(topic, asdict(sell_signal))
                        self.signal_logger.info(f"🔴 SELL SIGNAL [NORMAL] - Price: {mid_px_usdt:.4f}, Premium: {(mid_px_usdt/mid_px_usd-1)*100:.3f}%")
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                self.logger.info("Signal generator loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in signal generator loop: {e}", exc_info=True)
                await asyncio.sleep(30)  # Wait longer on errors


    def find_closest_business_day(self, timestamp: int) -> str:
        """
        Find the closest business day to the given timestamp.

        Args:
            timestamp: Unix timestamp in milliseconds

        Returns:
            str: Date in YYYY-MM-DD format representing the closest business day
        """
        # Convert timestamp to datetime
        if timestamp > 1e12:  # If timestamp is in milliseconds
            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        else:  # If timestamp is in seconds
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        # Convert to Taiwan timezone
        tz_tw = timezone(timedelta(hours=8))
        local_dt = dt.astimezone(tz_tw)

        # Start with the current date
        current_date = local_dt.date()

        # Check if current date is a business day
        if current_date not in self.tw_holidays and current_date.weekday() < 5 and local_dt.hour >14 :  # Monday=0, Sunday=6
            return current_date.strftime('%Y-%m-%d')

        # Search for the closest business day (check both directions)
        for days_offset in range(1, 15):  # Check up to 2 weeks
            # Check previous day
            prev_date = current_date - timedelta(days=days_offset)
            if prev_date not in self.tw_holidays and prev_date.weekday() < 5:
                return prev_date.strftime('%Y-%m-%d')

        # Fallback: return current date if no business day found within 2 weeks
        self.logger.warning(f"No business day found within 2 weeks of {current_date}")
        return current_date.strftime('%Y-%m-%d')

    async def start_listening(self):
        await self._initialize_message_queue()
        self.is_running = True

        self._signal_task = asyncio.create_task(self._generate_signal_loop(),name="ArbScanner_SignalGenerator")
        self._reset_benchmark_price_task = asyncio.create_task(self.reset_benchmark_price_periodically())
        self._update_benchmark_price_task = asyncio.create_task(self.init_benchmark_price_periodically())

        self.logger.info('start signal generator')
        try:
            await asyncio.gather(self._signal_task,self._reset_benchmark_price_task,self._update_benchmark_price_task)
        except asyncio.CancelledError:
            self.logger.info("Arb scanner listening tasks cancelled.")
        except Exception as e:
            self.logger.error(f"Error in ArbScanner  main listening loop: {e}", exc_info=True)
