import os
import time
from datetime import date, datetime
from typing import Any

import pandas as pd

from src.dao.clickhouse_manager import ClickHouseManager
from src.utils.logger import app_logger


class DatabaseSyncer:
    INCREMENTAL_CANDIDATES = (
        "update_time",
        "filing_date",
        "period_end",
        "timestamp",
        "date",
        "ex_dividend_date",
    )

    def __init__(self) -> None:
        self.src = ClickHouseManager.get_source_client()
        self.dest = ClickHouseManager.get_target_client()
        self.batch_size = int(os.getenv("SYNC_BATCH_SIZE", "50000"))
        self.max_retries = int(os.getenv("SYNC_RETRY_TIMES", "3"))
        self.retry_sleep_seconds = float(os.getenv("SYNC_RETRY_SLEEP_SECONDS", "1.0"))

    def sync_all(self) -> None:
        res = self.src.query("SHOW TABLES")
        tables = [row[0] for row in res.result_rows]

        failed_tables: list[str] = []
        for table in tables:
            try:
                self.sync_table(table)
            except Exception as e:
                failed_tables.append(table)
                app_logger.error(f"❌ Error syncing table {table}: {e}")

        if failed_tables:
            app_logger.error(f"❌ Sync finished with failures: {failed_tables}")
        else:
            app_logger.info(f"✅ Sync finished. {len(tables)} tables processed successfully.")

    def sync_table(self, table: str) -> None:
        table_ident = self._quote_ident(table)

        self._ensure_target_table_exists(table, table_ident)

        cols = self.src.query(f"DESCRIBE TABLE {table_ident}").result_rows
        col_names = [col[0] for col in cols]
        col_type_map = {col[0]: col[1] for col in cols}

        inc_col = next((c for c in self.INCREMENTAL_CANDIDATES if c in col_names), None)
        if not inc_col:
            app_logger.warning(
                f"⚠️ Table {table} has no recognized incremental column. Skipping to avoid extreme full copy."
            )
            return

        inc_col_ident = self._quote_ident(inc_col)
        max_val_res = self.dest.query(f"SELECT MAX({inc_col_ident}) FROM {table_ident}").result_rows[0][0]
        where_clause = self._build_where_clause_for_source(max_val_res, inc_col_ident)

        # 边界修复：以 >= 边界拉取时，先清掉目标端边界点，避免重复并防止同时间戳漏数
        if max_val_res is not None and not str(max_val_res).startswith("1970"):
            boundary_literal = self._to_sql_literal(max_val_res)
            delete_sql = (
                f"ALTER TABLE {table_ident} DELETE WHERE {inc_col_ident} = {boundary_literal} "
                "SETTINGS mutations_sync=1"
            )
            self._with_retry(lambda: self.dest.command(delete_sql), f"delete boundary rows for {table}")

        app_logger.info(
            f"Syncing [{table}] via incremental column [{inc_col}] with batch_size={self.batch_size}, where={where_clause}"
        )

        offset = 0
        total_synced = 0
        while True:
            query = (
                f"SELECT * FROM {table_ident} "
                f"WHERE {where_clause} "
                f"ORDER BY {inc_col_ident} ASC "
                f"LIMIT {self.batch_size} OFFSET {offset}"
            )
            df = self._with_retry(lambda: self.src.query_df(query), f"query source batch for {table}")

            if df.empty:
                break

            self._normalize_datetime_columns(df, col_type_map)
            self._with_retry(lambda: self.dest.insert_df(table, df), f"insert batch for {table}")

            batch_rows = len(df)
            total_synced += batch_rows
            offset += batch_rows
            app_logger.info(
                f"✅ Synced batch for [{table}] rows={batch_rows}, total={total_synced}, next_offset={offset}"
            )

            if batch_rows < self.batch_size:
                break

        if total_synced == 0:
            app_logger.info(f"⚡ Table {table} is already up-to-date.")
        else:
            app_logger.info(f"✅ Synced {total_synced} rows for table: {table}")

    def _ensure_target_table_exists(self, table: str, table_ident: str) -> None:
        create_stmt = self.src.query(f"SHOW CREATE TABLE {table_ident}").result_rows[0][0]
        if "CREATE TABLE" in create_stmt and "IF NOT EXISTS" not in create_stmt:
            create_stmt = create_stmt.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)

        # 跨环境时 ON CLUSTER 经常不兼容，目标库单机同步场景直接去掉
        create_stmt = create_stmt.replace(" ON CLUSTER default", "")
        create_stmt = create_stmt.replace(" ON CLUSTER 'default'", "")

        try:
            self.dest.command(create_stmt)
        except Exception as e:
            # 如果建表失败且目标表已存在，则继续；否则抛错
            exists = self.dest.query(f"EXISTS TABLE {table_ident}").result_rows[0][0]
            if exists == 1:
                app_logger.warning(
                    f"⚠️ CREATE TABLE for [{table}] failed but target table already exists, continue. err={e}"
                )
                return
            raise

    def _build_where_clause_for_source(self, max_val_res: Any, inc_col_ident: str) -> str:
        if max_val_res is None or str(max_val_res).startswith("1970"):
            return "1=1"
        return f"{inc_col_ident} >= {self._to_sql_literal(max_val_res)}"

    def _normalize_datetime_columns(self, df: pd.DataFrame, col_type_map: dict[str, str]) -> None:
        # 统一把 tz-aware 时间转成 UTC 后再去掉 tz，避免本地时区误差
        for col in df.columns:
            if "DateTime" not in col_type_map.get(col, ""):
                continue
            if not pd.api.types.is_datetime64tz_dtype(df[col]):
                continue
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)

    def _with_retry(self, func, action: str):
        for attempt in range(1, self.max_retries + 1):
            try:
                return func()
            except Exception as e:
                if attempt == self.max_retries:
                    raise
                app_logger.warning(
                    f"⚠️ {action} failed (attempt {attempt}/{self.max_retries}): {e}; retrying..."
                )
                time.sleep(self.retry_sleep_seconds * attempt)
        raise RuntimeError(f"{action} failed unexpectedly after retries")

    @staticmethod
    def _quote_ident(name: str) -> str:
        return f"`{name.replace('`', '``')}`"

    @staticmethod
    def _to_sql_literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, datetime):
            return f"toDateTime('{value.strftime('%Y-%m-%d %H:%M:%S')}')"
        if isinstance(value, date):
            return f"toDate('{value.strftime('%Y-%m-%d')}')"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        return str(value)
