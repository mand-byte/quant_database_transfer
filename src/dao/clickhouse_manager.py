import os
import clickhouse_connect
from src.utils.logger import app_logger

class ClickHouseManager:
    """
    管理原端（FROM）和目标端（TO）ClickHouse的连接
    """
    _source_client = None
    _target_client = None

    @classmethod
    def get_source_client(cls):
        if cls._source_client is None:
            host = os.getenv("FROM_CLICKHOUSE_HOST")
            port = int(os.getenv("FROM_DATABASE_PORT", "8123"))
            user = os.getenv("FROM_CLICKHOUSE_USER", "default")
            password = os.getenv("FROM_CLICKHOUSE_PASSWORD", "")
            database = os.getenv("FROM_CLICKHOST_DATABASE", "default")
            
            app_logger.info(f"Connecting to SOURCE ClickHouse at {host}:{port} db={database}")
            cls._source_client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                connect_timeout=30,
                send_receive_timeout=300
            )
        return cls._source_client

    @classmethod
    def get_target_client(cls):
        if cls._target_client is None:
            host = os.getenv("TO_CLICKHOUSE_HOST")
            port = int(os.getenv("TO_DATABASE_PORT", "8123"))
            user = os.getenv("TO_CLICKHOUSE_USER", "default")
            password = os.getenv("TO_CLICKHOUSE_PASSWORD", "")
            database = os.getenv("TO_CLICKHOST_DATABASE", "default")
            
            app_logger.info(f"Connecting to TARGET ClickHouse at {host}:{port} db={database}")
            cls._target_client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                connect_timeout=30,
                send_receive_timeout=300
            )
        return cls._target_client
