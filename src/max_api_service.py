import asyncio
import logging
from typing import Dict, List, Tuple, Optional
from api_client import MaxAPIClient
from src.config_enum import WalletType


class MaxAPIService:
    """Service class for handling parallel MAX API operations."""
    
    _logger = logging.getLogger("max_api_service.MaxAPIService")
    
    def __init__(self, client: MaxAPIClient):
        self.client = client
        self.logger = MaxAPIService._logger
    
    async def _fetch_orders_for_single_combination(self, wallet_type: WalletType, symbol: str) -> List[Dict]:
        """單個組合的訂單獲取函數"""
        try:
            self.logger.debug(f'Fetching exchange orders for {wallet_type.value} {symbol}')
            return await self.client.get_open_orders(
                path_wallet_type=wallet_type,
                market=symbol.lower(),
                limit=100  # Increase limit to catch more orders
            )
        except Exception as e:
            self.logger.error(f'Failed to fetch orders for {wallet_type.value} {symbol}: {e}')
            return []

    async def fetch_orders_parallel(self, wallet_symbol_combinations: Dict[Tuple[WalletType, str], List[str]]) -> Dict[str, Dict]:
        """
        並行獲取所有 wallet_type + symbol 組合的交易所訂單。
        
        Args:
            wallet_symbol_combinations: {(wallet_type, symbol): [client_order_ids]}
            
        Returns:
            Dict[str, Dict]: {client_oid: order_data}
        """
        # 創建並行任務
        tasks = []
        task_keys = []
        
        for (wallet_type, symbol), local_order_ids in wallet_symbol_combinations.items():
            task = self._fetch_orders_for_single_combination(wallet_type, symbol)
            tasks.append(task)
            task_keys.append((wallet_type, symbol))

        # 並行執行所有 API 調用
        self.logger.info(f'Starting parallel fetch for {len(tasks)} wallet/symbol combinations')
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 合並結果
        all_exchange_orders = {}
        successful_fetches = 0
        
        for i, result in enumerate(results):
            wallet_type, symbol = task_keys[i]
            
            if isinstance(result, Exception):
                self.logger.error(f'Exception fetching orders for {wallet_type.value} {symbol}: {result}')
                continue
                
            if not isinstance(result, list):
                self.logger.warning(f'Unexpected result type for {wallet_type.value} {symbol}: {type(result)}')
                continue
                
            # 處理成功的結果
            for order in result:
                if 'client_oid' in order and order['client_oid']:
                    all_exchange_orders[order['client_oid']] = order
            
            successful_fetches += 1
            self.logger.debug(f'Successfully fetched {len(result)} orders for {wallet_type.value} {symbol}')

        self.logger.info(f'Parallel fetch completed: {successful_fetches}/{len(tasks)} successful, total orders: {len(all_exchange_orders)}')
        return all_exchange_orders

    async def _fetch_single_order_detail(self, client_order_id: str) -> Tuple[str, Optional[Dict]]:
        """獲取單個訂單詳細信息"""
        try:
            self.logger.debug(f'Fetching order detail for {client_order_id}')
            detail = await self.client.get_an_order_detail(order_id=None, client_oid=client_order_id)
            return client_order_id, detail
        except Exception as e:
            self.logger.warning(f'Failed to fetch order detail for {client_order_id}: {e}')
            return client_order_id, None

    async def fetch_order_details_parallel(self, client_order_ids: List[str]) -> Dict[str, Optional[Dict]]:
        """
        並行獲取訂單詳細信息。
        
        Args:
            client_order_ids: 需要獲取詳細信息的訂單ID列表
            
        Returns:
            Dict[str, Optional[Dict]]: {client_order_id: order_detail or None}
        """
        # 創建並行任務
        tasks = [self._fetch_single_order_detail(client_order_id) for client_order_id in client_order_ids]
        
        # 並行執行
        self.logger.info(f'Starting parallel fetch for {len(tasks)} order details')
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 處理結果
        detailed_orders = {}
        successful_fetches = 0
        
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f'Exception fetching order detail: {result}')
                continue
                
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.warning(f'Unexpected result format: {result}')
                continue
                
            client_order_id, detail = result
            detailed_orders[client_order_id] = detail
            
            if detail is not None:
                successful_fetches += 1

        self.logger.info(f'Order detail fetch completed: {successful_fetches}/{len(tasks)} successful')
        return detailed_orders

    async def _cancel_single_expired_order(self, client_order_id: str, order_age: float) -> Tuple[str, bool]:
        """取消單個過期訂單"""
        try:
            self.logger.warning(f'Cancelling expired order (age: {order_age:.1f}s): {client_order_id}')
            response = await self.client.cancel_an_order(order_id=None, client_oid=client_order_id)
            # Consider successful if no exception is raised
            return client_order_id, True
        except Exception as e:
            self.logger.error(f'Failed to cancel expired order {client_order_id}: {e}')
            return client_order_id, False

    async def cancel_orders_parallel(self, expired_orders: List[Tuple[str, float]]) -> Dict[str, bool]:
        """
        並行取消過期訂單。
        
        Args:
            expired_orders: List of (client_order_id, order_age) tuples
            
        Returns:
            Dict[str, bool]: {client_order_id: success_status}
        """
        # 創建並行任務
        tasks = [self._cancel_single_expired_order(client_order_id, order_age) 
                for client_order_id, order_age in expired_orders]
        
        # 並行執行所有取消操作
        self.logger.info(f'Starting parallel cancellation of {len(tasks)} expired orders')
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 處理結果
        cancellation_results = {}
        successful_cancellations = 0
        failed_cancellations = 0
        
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f'Exception during order cancellation: {result}')
                failed_cancellations += 1
                continue
                
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.warning(f'Unexpected cancellation result format: {result}')
                failed_cancellations += 1
                continue
                
            client_order_id, success = result
            cancellation_results[client_order_id] = success
            
            if success:
                successful_cancellations += 1
            else:
                failed_cancellations += 1

        if successful_cancellations > 0:
            self.logger.info(f'Successfully cancelled {successful_cancellations} expired orders')
        if failed_cancellations > 0:
            self.logger.warning(f'Failed to cancel {failed_cancellations} expired orders')
            
        return cancellation_results

    async def _cancel_single_order_by_id(self, client_order_id: str) -> Tuple[str, bool]:
        """取消單個訂單"""
        try:
            response = await self.client.cancel_an_order(order_id=None, client_oid=client_order_id)
            return client_order_id, True
        except Exception as e:
            self.logger.error(f'Failed to cancel order {client_order_id}: {e}')
            return client_order_id, False

    async def cancel_orders_by_ids_parallel(self, client_order_ids: List[str]) -> Dict[str, bool]:
        """
        並行取消指定的訂單列表。
        
        Args:
            client_order_ids: 要取消的訂單ID列表
            
        Returns:
            Dict[str, bool]: {client_order_id: success_status}
        """
        # 創建並行任務
        tasks = [self._cancel_single_order_by_id(client_order_id) for client_order_id in client_order_ids]
        
        # 並行執行所有取消操作
        self.logger.info(f'Starting parallel cancellation of {len(tasks)} orders')
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 處理結果
        cancellation_results = {}
        successful_cancellations = 0
        
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f'Exception during order cancellation: {result}')
                continue
                
            if not isinstance(result, tuple) or len(result) != 2:
                self.logger.warning(f'Unexpected cancellation result format: {result}')
                continue
                
            client_order_id, success = result
            cancellation_results[client_order_id] = success
            
            if success:
                successful_cancellations += 1

        self.logger.info(f'Successfully cancelled {successful_cancellations}/{len(tasks)} orders')
        return cancellation_results