import asyncio
from msg_queue import AsyncMessageQueue
import logging
from typing import List, Literal, Dict, Any, Tuple, Union, Optional, Set
from api_client import MaxAPIClient
from src.config_enum import WalletType, RunningStrategy, OrderSide
from settings import STRATEGY_CONFIG
from market_data import MarketData


class MaxPositionManager:
    _logger = logging.getLogger('position_manager.PositionManager')

    def __init__(self, client: MaxAPIClient, symbols: List[str], interval:int = 30, market_data:MarketData = None):
        self.logger = MaxPositionManager._logger
        if not client:
            self.logger.error('No client provided')
            return
        if not market_data:
            self.logger.error('No market_data provided')
            return
        self.mq = None
        self.client = client
        self.interval = interval
        self.market_data = market_data
        self.symbols = symbols
        self._initialize()

    def _initialize(self) :
        total_balance = {WalletType.SPOT.value: {}, WalletType.M.value: {}}
        for strategy in RunningStrategy:
            for wallet_type in total_balance.keys():
                total_balance[wallet_type][strategy.value] = {s.lower(): 0 for s in self.symbols}

        self.total_balance : Dict[str, Dict[str,any]] = total_balance

    def get_symbol_balance_in_strategy(self, wallet_type: WalletType, strategy:RunningStrategy, symbol:str) -> Optional[float]:
        wallet_balance = self.total_balance.get(wallet_type, None)
        if not wallet_balance:
            self.logger.warning(f'No strategy balance for {wallet_type} wallet for strategy {strategy}')
            return None
        strategy_balance = wallet_balance[strategy.value]

        if symbol in strategy_balance:
            return strategy_balance[symbol]
        else:
            self.logger.warning(f'No strategy balance for {symbol} in {wallet_type.value} wallet for strategy {strategy}')
            return None

    def get_all_running_symbols(self, wallet_type: WalletType) -> Set[str]:
        target_wallet = self.total_balance[wallet_type.value]
        symbols = []
        for strategy_id in target_wallet.keys():
            unique_symbols = target_wallet[strategy_id].keys()
            symbols.extend(unique_symbols)

        return set(symbols)

    def is_strategy_allowance_available(self, strategy:RunningStrategy, wallet_type: WalletType, symbol:str, order_size: Union[str, float]) -> Tuple[bool, bool]:

        current_trial_allowance = self.get_trial_strategy_allowance(strategy, wallet_type, symbol, order_size)
        self.logger.error(f'allowance: {current_trial_allowance}')
        self.logger.warning(f'Strategy {strategy.value} has current allowance: {current_trial_allowance:.2f}')
        abs_value = abs(current_trial_allowance)
        if current_trial_allowance is None:
            return False, False

        if  abs_value < STRATEGY_CONFIG.ALLOCATION[strategy.value]:
            return True, True

        if abs_value >= STRATEGY_CONFIG.ALLOCATION[strategy.value]:
            if current_trial_allowance > 0:
                return False, True
            if current_trial_allowance < 0:
                return True, False

        self.logger.warning(f'Exception case. Please check current_trial_allowance: {current_trial_allowance}, abs_value: {abs_value}')
        return False, False



    def get_trial_strategy_allowance(self,  strategy:RunningStrategy, wallet_type: WalletType, symbol:str, order_size: Union[str, float])-> Optional[float]:
        # self.logger.info(f'get_trial_strategy_allowance: {strategy.value}, {wallet_type.value}, {symbol}, {order_size}')
        strategy_balance = self.total_balance[wallet_type.value][strategy.value]
        total_value = 0
        if symbol not in strategy_balance:
            self.logger.error(f'symbol {symbol} has not been initialized in strategy {strategy.value}')
            return None

        for existing_symbol in strategy_balance.keys():
            mid_px = self.market_data.get_mid_price(symbol)
            # self.logger.warning(f'mid px: {mid_px}')
            if not mid_px:
                self.logger.error(f'symbol {symbol} not found in MarketData')
                return None
            if existing_symbol == symbol:
                amount = strategy_balance[existing_symbol] + float(order_size)
            else:
                amount = strategy_balance[existing_symbol]
            market_value = float(mid_px) * abs(float(amount))
            total_value += market_value


        return float(total_value)


    async def _initialize_message_queue(self):
        if self.mq is None:
            try:
                self.mq = await AsyncMessageQueue.get_instance()
                self.logger.info("AsyncMessageQueue instance obtained.")
            except Exception as e:
                self.logger.error("Exception while initializing message queue: " + str(e))
                return


    def on_get_account_balance(self,wallet_type:WalletType, parsed_data: Dict[str, Any]):
        if not wallet_type:
            self.logger.error('No wallet_type provided')


    def on_order_increment(self,
                           side:OrderSide,
                           strategy_id_str: str,
                           symbol:str,
                           wallet_type_str:str,
                           executed_amount: float):
        if wallet_type_str not in WalletType.__members__:
            self.logger.error(f'Wallet type {wallet_type_str} is not supported, please check the wallet type')
            return

        wallet_balance_dict = self.total_balance[wallet_type_str]

        if strategy_id_str not in  wallet_balance_dict:
            self.logger.error(f'Strategy {strategy_id_str} not found in Running Strategy, please check the strategy string.')
            return

        balance_dict = wallet_balance_dict[strategy_id_str]
        if symbol not in balance_dict:
            balance_dict[symbol] = 0
        if side == OrderSide.BUY:
            balance_dict[symbol] += executed_amount
        elif side == OrderSide.SELL:
            balance_dict[symbol] -= executed_amount
        else:
            self.logger.error(f'Unknown side: {side}')
            return
        self.logger.info(f'Send {symbol} to {strategy_id_str}: {executed_amount}')

    async def _fetch_account_balance_periodically(self):
        while True:
            await asyncio.sleep(self.interval)

            try:
                task_position_s = asyncio.create_task(self.client.get_all_account_balances(wallet_type='spot'))
                task_position_m = asyncio.create_task(self.client.get_all_account_balances(wallet_type='m'))

                positions_s, positions_m = await asyncio.gather(task_position_s, task_position_m)
                if positions_s:
                    self.on_get_account_balance(*self._parse_positions(positions_s, wallet_type='spot'))
                if positions_m:
                    self.on_get_account_balance(*self._parse_positions(positions_m, wallet_type='m'))
            except Exception as e:
                self.logger.error(f"An unexpected error occurred during API request: {e}", exc_info=True)


    @staticmethod
    def _parse_positions(resp: List[Dict[str, Any]], wallet_type: Literal['spot','m']) -> Tuple[str, Dict[str, Any]]:
        res = {}
        for position in resp:
            key = position.get('currency', None)
            if key:
                idx = position.pop('currency')
                res[idx] =  position

        return wallet_type, res
