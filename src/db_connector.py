import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple
import asyncpg
import os
from system_config import pg_settings



class DBConnector:
    _instance: Optional["DBConnector"] = None
    _lock: asyncio.Lock = asyncio.Lock()  # 用於控制單例實例的創建
    _pool: Optional[asyncpg.pool.Pool] = None  # asyncpg 連接池

    def __init__(self):
        # 初始化時不直接連線，而是等待 connect() 被呼叫
        self.logger = logging.getLogger(self.__class__.__name__)
        self._is_connected = False
        self.db_host = pg_settings.PG_HOST
        self.db_port = pg_settings.PG_PORT
        self.db_user = pg_settings.PG_USER
        self.db_password = pg_settings.PG_PASSWORD
        self.db_name = pg_settings.PG_NAME
        self.db_min_conns = pg_settings.PG_MIN_CONN
        self.db_max_conns = pg_settings.PG_MAX_CONN

    @classmethod
    async def get_instance(cls) -> "DBConnector":
        """
        獲取 DBConnector 的單例實例。如果實例不存在，則會創建並初始化連接池。
        """
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
                    await cls._instance.connect()  # 在第一次獲取實例時建立連接池
        return cls._instance

    async def connect(self):
        """
        建立到 PostgreSQL 資料庫的連接池。
        """
        if self._is_connected and self._pool is not None:
            self.logger.info("Database connection pool already exists.")
            return

        try:
            self.logger.info(f"Connecting to PostgreSQL at {self.db_host}:{self.db_port}/{self.db_name}...")
            DBConnector._pool = await asyncpg.create_pool(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                min_size=self.db_min_conns,
                max_size=self.db_max_conns,
                timeout=5  # 連接嘗試超時時間 (秒)
            )
            self._is_connected = True
            self.logger.info("Successfully established PostgreSQL connection pool.")
        except Exception as e:
            self.logger.critical(f"Failed to connect to PostgreSQL: {e}", exc_info=True)
            # 確保在連接失敗時，實例和池被重置，以便下次嘗試
            DBConnector._instance = None
            DBConnector._pool = None
            self._is_connected = False
            raise  # 重新拋出異常，讓調用者知道連接失敗

    async def disconnect(self):
        """
        關閉資料庫連接池。
        """
        if self._pool is not None and not self._pool.is_closing():
            self.logger.info("Closing PostgreSQL connection pool...")
            await self._pool.close()
            self._is_connected = False
            DBConnector._pool = None
            DBConnector._instance = None  # 重置實例，以便下次可以重新建立
            self.logger.info("PostgreSQL connection pool closed.")
        else:
            self.logger.info("No active PostgreSQL connection pool to close.")

    async def execute_query(self, query: str, *args: Any) -> List[asyncpg.Record]:
        """
        執行一個 SQL 查詢並返回結果。
        """
        if not self._is_connected or self._pool is None:
            self.logger.error("Database is not connected. Cannot execute query.")
            raise ConnectionError("Database connection is not active.")

        async with self._pool.acquire() as conn:
            # begin/end 語句是可選的，asyncpg 默認會自動處理事務
            # 這裡簡單起見直接執行
            self.logger.debug(f"Executing query: {query} with args: {args}")
            try:
                result = await conn.fetch(query, *args)
                return result
            except Exception as e:
                self.logger.error(f"Error executing query '{query[:100]}...': {e}", exc_info=True)
                raise  # 重新拋出異常

    async def batch_insert(self, full_table_path: str, data: List[Dict[str, Any]]):
        """
        批量插入多條記錄到指定的資料庫表。
        Args:
            full_table_path: 完整的表路徑，例如 "SpotL2BookUpdate_5.max"
                             我們會將其解析為 schema 和 table_name。
            data: 要插入的資料列表，每個字典代表一行，鍵為列名。
        """
        if not self._is_connected or self._pool is None:
            self.logger.error("Database is not connected. Cannot perform batch insert.")
            raise ConnectionError("Database connection is not active.")

        if not data:
            self.logger.info(f"No data provided for batch insert into {full_table_path}.")
            return

        # 解析 full_table_path 為 schema 和 table_name
        parts = full_table_path.split('.')
        if len(parts) == 2:
            schema_name, table_name = parts[0], parts[1]
        elif len(parts) == 1:
            schema_name = "public"  # 默認 schema
            table_name = parts[0]
        else:
            raise ValueError(f"Invalid table path format: {full_table_path}. Expected 'schema.table' or 'table'.")

        # 確保 schema 和 table_name 是合法的識別符，避免 SQL 注入
        # 注意：這裡只是簡單檢查，生產環境應使用更健壯的驗證
        if not (schema_name.replace('_', '').isalnum() and table_name.replace('_', '').isalnum()):
            raise ValueError(f"Invalid characters in schema or table name: {full_table_path}")

        # 動態構建列名和值
        # 假設所有數據字典都有相同的鍵集，取第一個字典的鍵作為列
        columns = list(data[0].keys())

        # 移除 bidsX_px/sz 和 asksX_px/sz 中的下標，以符合實際的 column name
        # 並且要將其替換為 "bids_px_0", "bids_sz_0" 等形式，以匹配實際的列名
        # 或者說，你的column name應該就是 "bids0_px", "bids0_sz" 這種格式
        # 這裡根據你提供的列名格式，假設是 `bids0_px`, `asks1_sz` 這種直接的格式

        # 轉換為asyncpg可以處理的元組列表
        values_list: List[Tuple[Any, ...]] = []
        for item in data:
            row_values = tuple(item[col] for col in columns)
            values_list.append(row_values)
        columns_str = ", ".join(f'"{col}"' for col in columns)  # 用雙引號包住列名，處理大小寫或特殊字元
        # 使用 asyncpg 的 copy_records_to_table 進行高效批量插入
        try:
            self.logger.debug(f"Attempting batch insert into {schema_name}.{table_name} with {len(data)} records.")
            async with self._pool.acquire() as conn:
                # 這裡需要指定完整的表名，包括 schema
                await conn.copy_records_to_table(
                    table_name,
                    records=values_list,
                    columns=columns,
                    schema_name=schema_name
                )
            self.logger.info(f"Successfully inserted {len(data)} records into {schema_name}.{table_name}.")
        except Exception as e:
            self.logger.error(f"Failed to batch insert into {schema_name}.{table_name}: {e}", exc_info=True)
            raise  # 重新拋出異常

