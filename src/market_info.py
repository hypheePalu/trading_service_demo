from typing import List, Dict, Any
from api_client import SyncMaxAPIClient
from src.data_format import MarketInfo, ExchangeInfo


class ExchangeMarketConfig:
    def __init__(self):
        self.client = SyncMaxAPIClient()
        self._initialize()

    def _initialize(self):
        resp = self.client.get_all_available_markets()
        self.exchange_info = ExchangeInfo(MAX=self._parse_available_markets(resp))


    @staticmethod
    def _parse_available_markets(resp: List[Dict[str, Any]]) -> Dict[str, Any]:
        res = {}
        for market in resp:
            key = market.get('id', None)
            if key:
                idx = market.pop('id')
                res[idx] = MarketInfo(is_active= True if market['status']=='active' else False,
                                      base=market['base_unit'], base_unit_precision=market['base_unit_precision'],
                                      min_base_amount=market['min_base_amount'], quote=market['quote_unit'], quote_unit_precision=market['quote_unit_precision'],
                                      min_quote_amount=market['min_quote_amount'], is_margin_supported=market['m_wallet_supported']
                                      )
        return res


ExchangeMarketConfig = ExchangeMarketConfig()

if __name__ == '__main__':
    strategy_config = ExchangeMarketConfig()
    print(strategy_config.exchange_info.MAX)