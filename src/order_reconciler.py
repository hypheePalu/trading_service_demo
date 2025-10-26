import logging
from typing import Dict, List, Optional
from src.data_format import Order, OrderStatus
from src.config_enum import RunningStrategy, WalletType
from position_manager import MaxPositionManager


class OrderReconciler:
    """Service class for handling order reconciliation logic."""
    
    _logger = logging.getLogger("order_reconciler.OrderReconciler")
    
    def __init__(self, position_manager: MaxPositionManager):
        self.position_manager = position_manager
        self.logger = OrderReconciler._logger
    
    @staticmethod
    def decode_client_order_id(client_order_id: str) -> tuple[RunningStrategy, WalletType]:
        """
        Decode client order ID to extract strategy and wallet type.
        
        Args:
            client_order_id: Client order ID in format 'STRATEGY_WALLETTYPE_TIMESTAMP'
            
        Returns:
            Tuple[RunningStrategy, WalletType]: Strategy ID and wallet type enums
            
        Raises:
            ValueError: If client_order_id format is invalid or values not found in enums
        """
        try:
            parts = client_order_id.split('_')
            if len(parts) < 2:
                raise ValueError(f"Invalid client_order_id format: {client_order_id}")
            
            strategy_id_str = parts[0]
            wallet_type_str = parts[1]
            
            # Convert to enums
            strategy_id = RunningStrategy(strategy_id_str)
            wallet_type = WalletType(wallet_type_str)
            
            return strategy_id, wallet_type
            
        except ValueError as e:
            raise ValueError(f"Failed to decode client_order_id '{client_order_id}': {e}")
    
    async def reconcile_orders(self, 
                             local_orders: Dict[str, Order], 
                             exchange_orders: Dict[str, Dict],
                             detailed_orders: Optional[Dict[str, Optional[Dict]]] = None) -> int:
        """
        比較本地訂單與交易所訂單，處理差異。
        
        Args:
            local_orders: 本地未完成訂單 {client_order_id: Order}
            exchange_orders: 交易所返回的訂單 {client_oid: order_data}
            detailed_orders: 可選的詳細訂單信息 {client_order_id: order_detail or None}
            
        Returns:
            int: 同步的訂單數量
        """
        # 找出需要詳細檢查的訂單
        mismatched_order_ids = []
        
        for client_order_id in local_orders.keys():
            if client_order_id not in exchange_orders:
                mismatched_order_ids.append(client_order_id)
        
        synced_count = 0
        
        if mismatched_order_ids:
            self.logger.info(f'Found {len(mismatched_order_ids)} potentially mismatched orders')
            
            if detailed_orders:
                # Use provided detailed information for reconciliation
                synced_count = await self._reconcile_with_details(
                    local_orders, detailed_orders, mismatched_order_ids
                )
            else:
                # If no detailed information, log but don't sync
                self.logger.warning(f'No detailed order information provided for {len(mismatched_order_ids)} mismatched orders')
        else:
            self.logger.debug('No mismatched orders found, all orders in sync')
            
        return synced_count
    
    async def _reconcile_with_details(self,
                                    local_orders: Dict[str, Order],
                                    detailed_orders: Dict[str, Optional[Dict]],
                                    mismatched_order_ids: List[str]) -> int:
        """
        Perform reconciliation using detailed order information.

        Args:
            local_orders: Local orders dictionary
            detailed_orders: {client_order_id: order_detail or None}
            mismatched_order_ids: List of order IDs that need reconciliation

        Returns:
            int: Number of orders synchronized
        """
        synced_count = 0
        orders_to_remove = []
        
        for client_order_id in mismatched_order_ids:
            local_order = local_orders.get(client_order_id)
            if not local_order:
                continue
                
            try:
                strategy_id, wallet_type = self.decode_client_order_id(client_order_id)
            except ValueError as e:
                self.logger.warning(f'Skipping sync for invalid client_order_id: {client_order_id}: {e}')
                continue
            
            order_detail = detailed_orders.get(client_order_id)
            
            if order_detail is None:
                # API call failed, cannot get detailed information - handle conservatively
                self.logger.warning(f'Could not fetch details for {client_order_id}, assuming still active')
                continue

            # Analyze order status
            order_state = order_detail.get('state', '').lower()
            executed_volume_str = order_detail.get('executed_volume', '0')
            
            try:
                exchange_executed_volume = float(executed_volume_str)
            except (ValueError, TypeError):
                self.logger.error(f'Invalid executed_volume for {client_order_id}: {executed_volume_str}')
                continue
            
            local_executed_volume = (local_order.sz or 0) - (local_order.unfilled_sz or 0)
            volume_difference = exchange_executed_volume - local_executed_volume
            
            if order_state in ['done', 'cancel'] or volume_difference > 0:
                # Order completed or has new execution volume
                if volume_difference > 0:
                    self.logger.info(f'Order {client_order_id} has additional execution: {volume_difference}')

                    # Adjust position
                    self.position_manager.on_order_increment(
                        strategy_id_str=strategy_id.value,
                        side=local_order.side,
                        symbol=local_order.symbol,
                        wallet_type_str=wallet_type.value,
                        executed_amount=volume_difference
                    )
                    synced_count += 1

                if order_state in ['done', 'cancel']:
                    # Order completed, mark for removal
                    self.logger.info(f'Order {client_order_id} is {order_state}, should be moved to done orders')

                    # Update local order status
                    local_order.order_status = OrderStatus.FILLED if order_state == 'done' else OrderStatus.CANCELED
                    local_order.filled_sz = exchange_executed_volume
                    local_order.unfilled_sz = 0 if order_state == 'done' else (local_order.sz or 0) - exchange_executed_volume
                    
                    orders_to_remove.append(client_order_id)
            else:
                self.logger.debug(f'Order {client_order_id} state: {order_state}, no sync needed')

        # Return list of orders to remove and sync count
        return synced_count
    
    @staticmethod
    def get_orders_to_remove_after_reconciliation(detailed_orders: Dict[str, Optional[Dict]]) -> List[str]:
        """
        Get list of orders that need to be removed from unfilled orders after reconciliation.

        Args:
            local_orders: Local orders dictionary
            detailed_orders: Detailed order information

        Returns:
            List[str]: List of order IDs to remove
        """
        orders_to_remove = []
        
        for client_order_id, order_detail in detailed_orders.items():
            if order_detail is None:
                continue
                
            order_state = order_detail.get('state', '').lower()
            if order_state in ['done', 'cancel']:
                orders_to_remove.append(client_order_id)
        
        return orders_to_remove
    
    def update_local_orders_from_detailed_info(self,
                                             local_orders: Dict[str, Order],
                                             detailed_orders: Dict[str, Optional[Dict]]) -> Dict[str, Order]:
        """
        Update local order status based on detailed order information.

        Args:
            local_orders: Local orders dictionary
            detailed_orders: Detailed order information

        Returns:
            Dict[str, Order]: Dictionary of orders that need to be moved to completed orders
        """
        completed_orders = {}
        
        for client_order_id, order_detail in detailed_orders.items():
            if client_order_id not in local_orders or order_detail is None:
                continue
                
            local_order = local_orders[client_order_id]
            order_state = order_detail.get('state', '').lower()
            executed_volume_str = order_detail.get('executed_volume', '0')
            
            try:
                exchange_executed_volume = float(executed_volume_str)
            except (ValueError, TypeError):
                self.logger.error(f'Invalid executed_volume for {client_order_id}: {executed_volume_str}')
                continue
            
            if order_state in ['done', 'cancel']:
                # Update order status
                local_order.order_status = OrderStatus.FILLED if order_state == 'done' else OrderStatus.CANCELED
                local_order.filled_sz = exchange_executed_volume
                local_order.unfilled_sz = 0 if order_state == 'done' else (local_order.sz or 0) - exchange_executed_volume
                
                completed_orders[client_order_id] = local_order
        
        return completed_orders