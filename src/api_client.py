# api_client.py
import httpx
import hashlib
import hmac
import time
import logging
from typing import Dict, Any, Optional, Literal
import json
import base64
from urllib.parse import urlencode
from system_config import max_setting
import asyncio
from src.config_enum import WalletType, OrderType, OrderSide

class MaxAPIClient:
    _logger = logging.getLogger("api_client.MaxAPIClient")

    def __init__(self, api_key: str = max_setting.MAX_API_KEY, secret_key: str = max_setting.MAX_SECRET_KEY):
        self.base_url = "https://max-api.maicoin.com"
        self.api_key = api_key
        # Secret key must be bytes, encode once during initialization to avoid repeated encoding for each signature
        self.secret_key_bytes = secret_key.encode('utf-8')
        self.logger = MaxAPIClient._logger
        # Set up httpx client, can configure timeout etc
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)
        # Nonce counter for parallel requests
        self._last_nonce = 0

    async def _send_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Send HTTP request to Max API.
        method: HTTP method (GET, POST, PUT, DELETE)
        path: API path (e.g. /api/v3/info)
        params: Actual request parameters (Dict[str, Any]), these parameters will be used for URL query string (GET) or
                as request body (POST/PUT/DELETE).
                Also, these parameters will be included in the signature payload.
        """
        # 1. Prepare actual request parameters (including nonce)
        # These parameters will be used for URL query string or request body
        actual_request_params = params.copy() if params else {}
        # Generate unique nonce for parallel requests
        current_time_ms = int(time.time() * 1000)
        if current_time_ms <= self._last_nonce:
            self._last_nonce += 1
        else:
            self._last_nonce = current_time_ms
        actual_request_params['nonce'] = self._last_nonce

        # 2. Build payload content for signature (original JSON for X-MAX-PAYLOAD)
        # This dictionary must include 'path', 'nonce', and all actual request parameters
        payload_for_signing_content = {
            'path': path,
        }
        payload_for_signing_content.update(actual_request_params) # Add all actual request parameters to signature payload

        # 3. Convert signature payload content to JSON string and Base64 encode
        # Max usually requires compact JSON format (no spaces or newlines), using separators=(',', ':')
        json_payload_str = json.dumps(payload_for_signing_content, separators=(',', ':'))
        encoded_payload = base64.b64encode(json_payload_str.encode('utf-8')).decode('utf-8')

        # 4. Generate signature
        signature = self._create_signature(encoded_payload)

        # 5. Prepare HTTP headers
        headers = {
            'X-MAX-ACCESSKEY': self.api_key,
            'X-MAX-PAYLOAD': encoded_payload,
            'X-MAX-SIGNATURE': signature,
            'Content-Type': 'application/json' # Tell server request body is JSON
        }

        url = self.base_url + path
        response = None
        try:
            if method == "GET":
                # GET request: parameters appended to URL (like your requests example)
                # The params here is actual_request_params
                url_with_params = f"{url}?{urlencode(actual_request_params)}"
                response = await self.client.get(url_with_params, headers=headers)
            else:
                # DELETE/POST/PUT request: parameters in request body (like your requests example)
                # The params here is actual_request_params
                response = await self.client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=actual_request_params # httpx uses json= instead of data=json.dumps()
                )

            response.raise_for_status() # Raises HTTPStatusError for 4xx/5xx responses
            return response.json()
        except httpx.HTTPStatusError as e:
            error_response = e.response
            try:
                # Use error_response.json() instead of response.json()
                error_details = error_response.json()
            except:
                # If not JSON format, extract raw text
                error_details = error_response.text
            # self.logger.error(f"HTTP error for {method} {path}: {e.response.status_code} - {e.response.text}", exc_info=True)
            exception_message = (
                f"API Error ({error_response.status_code}) for {method} {error_response.url}. "
                f"Server Message: {error_details}. "
                f"Request Payload: {actual_request_params}"  # Use actual_request_params here
            )
            # self.logger.error(exception_message, exc_info=True)

            # Re-raise a clearer exception
            raise Exception(exception_message) from e
        except Exception as general_e:
            # Handle other connection or non-HTTP status code errors
            self.logger.error(f"General error in {method} {path}: {general_e}", exc_info=True)
            raise general_e

    def _create_signature(self, payload: str) -> str:
        """Generate HMAC SHA256 signature."""
        # self.secret_key_bytes already encoded in __init__
        m = hmac.new(self.secret_key_bytes, payload.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    async def get_all_available_markets(self):
        path ='/api/v3/markets'
        self.logger.debug(f"Getting all available markets: {path}")
        try:
            response = await self._send_request("GET", path)
        except Exception:
            raise

        return response

    async def get_all_account_balances(self, wallet_type: Literal['m', 'spot'] = 'spot'):
        path = f'/api/v3/wallet/{wallet_type}/accounts'
        self.logger.debug(f"Getting all account balances: {path}")
        try:
            response = await self._send_request("GET", path)
            return response
        except Exception:
            raise


    async def get_user_info(self) -> Dict[str, Any]:
        path = '/api/v3/info'
        self.logger.debug(f"Requesting user info for {path}")
        # /api/v3/info 沒有額外參數，所以 params=None
        try:
            response = await self._send_request("GET", path)
            return response
        except Exception:
            raise
        # response = await self._send_request("GET", path)
        # self.logger.debug(f"User info for {path}: {response}")
        # return response

    async def get_server_time(self) -> int:
        path = '/api/v3/timestamp'
        self.logger.debug(f"Requesting server time for {path}")
        try:
            response = await self._send_request("GET", path)
            return int(response['timestamp']) * 1000
        except httpx.ReadTimeout as e:
            raise e
        except Exception:
            raise


    async def get_open_orders(self,
                              path_wallet_type: WalletType,
                              market: str,
                              timestamp: Optional[int] = None, # This is timestamp for filtering, not nonce
                              order_by: Literal['desc', 'asc'] = 'desc',
                              limit: int = 10):
        path = f'/api/v3/wallet/{path_wallet_type.value.lower()}/orders/open'

        # These are actual request parameters, they will be included in signature payload and sent as URL query parameters.
        request_params = {
            "market": market,
            "orderBy": order_by, # Max API usually uses 'orderBy' (camelCase)
            "limit": limit
        }
        if timestamp is not None:
            request_params['timestamp'] = timestamp # timestamp for filtering

        self.logger.debug(f"Requesting open orders for {path} with params: {request_params}")
        try:
            response = await self._send_request("GET", path, params=request_params)
            return response
        except Exception:
            raise



    async def get_closed_orders(self,
                              path_wallet_type: Literal['spot', 'm'],
                              market: str,
                              timestamp: Optional[int] = None, # This is timestamp for filtering, not nonce
                              order_by: Literal['desc', 'asc'] = 'desc',
                              limit: int = 10):
        path = f'/api/v3/wallet/{path_wallet_type}/orders/closed'
        request_params = {
            "market": market,
            "orderBy": order_by,  # Max API usually uses 'orderBy' (camelCase)
            "limit": limit
        }
        if timestamp is not None:
            request_params['timestamp'] = timestamp  # timestamp for filtering

        self.logger.debug(f"Requesting open orders for {path} with params: {request_params}")
        try:
            response = await self._send_request("GET", path)
            return response
        except Exception as e:
            raise e

    async def get_order_history_by_order_id(self,
                                            path_wallet_type: Literal['spot', 'm'],
                                            market: str,
                                            from_id: int,
                                            limit: int = 10):
        path = f'/api/v3/wallet/{path_wallet_type}/orders/history'

        request_params = {
            "market": market,
            "limit": limit,
            'from_id': from_id
        }
        self.logger.debug(f"Requesting open orders for {path} with params: {request_params}")
        try:
            response = await self._send_request("GET", path)
            return response
        except Exception as e:
            raise e

    async def place_an_order(self,
                             path_wallet_type: WalletType,
                             market: str,
                             order_type: OrderType,
                             side: OrderSide,
                             amount: str,
                             price: str,
                             client_oid: str,
                             group_id:Optional[int]=None):
        self.logger.debug(f"Placing an order for {market} on {client_oid}")
        path = f'/api/v3/wallet/{path_wallet_type.lower()}/order'
        request_params = {'market': market,
                          'ord_type': order_type.lower(),
                          'side': side.lower(),
                          'volume': amount,
                          'price': price,
                          'client_oid': client_oid
                          }
        if group_id:
            request_params['group_id'] = group_id
        self.logger.debug(f"Requesting order for {path} with params: {request_params}")
        try:
            response = await self._send_request("POST", path, params=request_params)
            return response
        except Exception as e:
            raise e

    async def cancel_an_order(self, client_oid:Optional[str], order_id: Optional[str] = None):
        path = '/api/v3/order'
        request_params = {}
        if client_oid:
            request_params['client_oid'] = client_oid
        else:
            request_params['order_id'] = order_id

        self.logger.debug(f"Cancelling order for {path} with params: {request_params}")
        try:
            resp = await self._send_request("DELETE", path, params=request_params)
            return resp
        except Exception as e:
            raise e


    async def cancel_all_orders(self,
                                path_wallet_type: Literal['spot', 'm'],
                                market: str
                                ):
        path =  f'/api/v3/wallet/{path_wallet_type.lower()}/orders'
        request_params = {'market': market}

        self.logger.debug(f"Cancelling orders for {path} with params: {request_params}")
        try:
            resp = await self._send_request("DELETE", path, params=request_params)
            return resp
        except Exception as e:
            raise e


    async def get_an_order_detail(self, order_id:Optional[str], client_oid:str):
        path = '/api/v3/order'
        request_params = {}
        if client_oid:
            request_params['client_oid'] = client_oid
        else:
            request_params['order_id'] = order_id

        self.logger.debug(f"Requesting order detail for {path} with params: {request_params}")
        try:
            resp = await self._send_request("GET", path, params=request_params)
            return resp
        except Exception as e:
            raise e

    async def get_api_v3_k(self, symbol:str,period:int, timestamp:int= None, limit:int=10000):
        path = '/api/v3/k'
        request_params = {
            "market": symbol,
            "limit": limit,
            'period': period
        }
        if timestamp:
            request_params['timestamp']= int(timestamp / 1000)
        self.logger.debug(f"Requesting k bar for {path} with params: {request_params}")
        try:
            response = await self._send_request("GET", path,  params=request_params)
            return response
        except Exception as e:
            raise e


class SyncMaxAPIClient:
    _logger = logging.getLogger("api_client.SyncMaxAPIClient")

    def __init__(self, api_key: str = max_setting.MAX_API_KEY, secret_key: str = max_setting.MAX_SECRET_KEY):
        self.base_url = "https://max-api.maicoin.com"
        self.api_key = api_key
        # Secret key must be bytes, encode once during initialization
        self.secret_key_bytes = secret_key.encode('utf-8')
        self.logger = SyncMaxAPIClient._logger
        # Use a synchronous httpx.Client to maintain connection
        self.client = httpx.Client(base_url=self.base_url)
        # Nonce counter for parallel requests
        self._last_nonce = 0

    def _send_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Send synchronous HTTP request to Max API.
        """
        actual_request_params = params.copy() if params else {}
        # Generate unique nonce for parallel requests
        current_time_ms = int(time.time() * 1000)
        if current_time_ms <= self._last_nonce:
            self._last_nonce += 1
        else:
            self._last_nonce = current_time_ms
        actual_request_params['nonce'] = self._last_nonce

        payload_for_signing_content = {'path': path}
        payload_for_signing_content.update(actual_request_params)

        json_payload_str = json.dumps(payload_for_signing_content, separators=(',', ':'))
        encoded_payload = base64.b64encode(json_payload_str.encode('utf-8')).decode('utf-8')
        signature = self._create_signature(encoded_payload)

        headers = {
            'X-MAX-ACCESSKEY': self.api_key,
            'X-MAX-PAYLOAD': encoded_payload,
            'X-MAX-SIGNATURE': signature,
            'Content-Type': 'application/json'
        }

        try:
            if method == "GET":
                # Use synchronous method of httpx.Client, put parameters in params
                response = self.client.get(url=path, headers=headers, params=actual_request_params)
            else:
                # Use synchronous request method of httpx.Client
                response = self.client.request(
                    method=method,
                    url=path,
                    headers=headers,
                    json=actual_request_params
                )

            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as e:
            self.logger.error(f"HTTP error for {method} {path}: {e.response.status_code} - {e.response.text}")
            self.logger.error(f"Request URL: {e.request.url}")
            self.logger.error(f"Request Headers: {e.request.headers}")
            self.logger.error(f"Response Body: {e.response.text}")
            raise
        except httpx.RequestError as e:
            self.logger.error(f"Request error for {method} {path}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during API request: {e}")
            raise

    def _create_signature(self, payload: str) -> str:
        """Generate HMAC SHA256 signature."""
        m = hmac.new(self.secret_key_bytes, payload.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    def get_all_available_markets(self):
        path ='/api/v3/markets'
        self.logger.debug(f"Getting all available markets: {path}")
        try:
            response = self._send_request("GET", path)
        except Exception:
            raise

        return response



class BinanceAPIClient:
    _logger = logging.getLogger("api_client.BinanceAPIClient")
    def __init__(self):
        self.logger = BinanceAPIClient._logger
        self.url = 'https://fapi.binance.com'
        self.client = httpx.AsyncClient(base_url=self.url)


    async def get_server_time(self):
        path = '/fapi/v1/time'
        self.logger.debug(f"Getting server time: {self.url+path}")

        try:
            response = await self.client.get(url=path)
        except Exception:
            raise

        return response.json()['serverTime']







if __name__ == '__main__':
    from max_utilis import get_parsed_max_k_line_df
    async def main():
        # client = MaxAPIClient()
        # resp = await client.get_all_available_markets()
        # open_orders  = await client.get_open_orders(path_wallet_type='spot',market='btcusdt')
        # closed_orders = await client.get_closed_orders(path_wallet_type='spot',market='usdttwd')
        # order_history = await client.get_order_history_by_order_id(path_wallet_type='spot', market='usdttwd', from_id=81000000000, limit=10)
        # binance_api = BinanceAPIClient()
        # print(await binance_api.get_server_time())
        res = await MaxAPIClient().get_api_v3_k('usdttwd', period=1)
        print(get_parsed_max_k_line_df(res))
        # print(closed_orders )
        # print(open_orders)
    asyncio.run(main())