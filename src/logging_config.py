# logging_config.py
import logging
import logging.config # 導入 logging.config 模組
import yaml
import os
# 我們仍然需要 colorlog，因為它的 ColoredFormatter 是在 YAML 中引用的
# 儘管不在代碼中直接使用，但它是作為一個類名被引用
import colorlog # 必須導入，否則 yaml.load 找不到 ColoredFormatter

def setup_logging(config_path: str = "logging.yaml", default_level: str = "INFO"):
    """
    從 YAML 檔案配置專案的日誌系統。

    Args:
        config_path (str): 日誌配置 YAML 檔案的路徑。預設為 'logging.yaml'。
        default_level (str): 如果 YAML 檔案未找到或解析失敗，使用的預設日誌等級。
    """
    if not os.path.exists(config_path):
        print(f"錯誤：日誌配置文件 '{config_path}' 不存在。使用預設配置。")
        # 如果配置文件不存在， fallback 到一個基本的彩色配置
        _setup_basic_colored_logging(default_level)
        return

    try:
        with open(config_path, 'rt', encoding='utf-8') as f:
            config = yaml.safe_load(f.read())

        # 應用 YAML 配置
        logging.config.dictConfig(config)
        print(f"成功從 '{config_path}' 加載日誌配置。")

    except Exception as e:
        print(f"錯誤：加載或解析日誌配置文件 '{config_path}' 失敗: {e}。使用預設配置。")
        # 加載失敗時，fallback 到一個基本的彩色配置
        _setup_basic_colored_logging(default_level)

def _setup_basic_colored_logging(log_level: str = "INFO"):
    """
    當 YAML 配置失敗或不存在時，提供一個基本的彩色日誌 fallback。
    """
    if logging.getLogger().handlers: # 避免重複添加 handler
        return

    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        print(f"警告：無效的預設日誌等級 '{log_level}'。將使用 INFO 級別。")
        numeric_level = logging.INFO

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s | %(asctime)s | %(blue)s%(filename)s:%(lineno)d - %(funcName)s()%(reset)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red,bg_white',
        }
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(numeric_level)
    root_logger.addHandler(console_handler)
    print("已啟用基本的彩色日誌配置。")


# 如果在測試或獨立運行時需要，可以這樣調用
if __name__ == "__main__":
    setup_logging(config_path="../logging.yaml") # 測試時使用當前目錄下的 logging.yaml

    logger = logging.getLogger(__name__)
    logger.debug("這是一條來自 __main__ 的 DEBUG 訊息。")
    logger.info("這是一條來自 __main__ 的 INFO 訊息。")

    # 模擬其他模組的日誌輸出
    binance_logger = logging.getLogger("binance_data_feed")
    binance_logger.debug("Binance: 收到原始數據包。") # 如果 logging.yaml 中 binance_data_feed 等級是 INFO，這條不會顯示
    binance_logger.info("Binance: 成功解析 Book Ticker。")
    binance_logger.warning("Binance: 連接斷開，正在重連。")

    order_logger = logging.getLogger("order_manager")
    order_logger.debug("OrderManager: 準備發送訂單 ID: XYZ123。") # 如果 logging.yaml 中 order_manager 等級是 DEBUG，這條會顯示
    order_logger.info("OrderManager: 訂單 ID: XYZ123 已發送。")
    order_logger.error("OrderManager: 訂單 ID: ABC456 提交失敗！")

    db_logger = logging.getLogger("db_manager")
    db_logger.info("DBManager: 正在寫入數據。") # 如果 logging.yaml 中 db_manager 等級是 WARNING，這條不會顯示
    db_logger.warning("DBManager: 數據庫寫入操作超時。")