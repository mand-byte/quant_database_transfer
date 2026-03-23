import os
import time
from datetime import date, datetime
from typing import Any

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
        # Use a conservative default to keep container memory stable on NAS.
        self.batch_size = int(os.getenv("SYNC_BATCH_SIZE", "5000"))
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
        self._ensure_target_columns_compatible(table, table_ident)

        cols = self.src.query(f"DESCRIBE TABLE {table_ident}").result_rows
        col_names = [col[0] for col in cols]
        col_names_ident = ", ".join(self._quote_ident(col) for col in col_names)

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

        # Cursor window pagination without OFFSET:
        # 1) probe a small batch to get current upper bound
        # 2) pull full window (lower_bound, upper_bound] (or [lower_bound, upper_bound] for first window)
        # This avoids deep OFFSET scans and supports safe resume after interruption.
        lower_bound = max_val_res
        include_lower_bound = max_val_res is not None and not str(max_val_res).startswith("1970")
        total_synced = 0
        while True:
            probe_cursor_clause = self._build_cursor_clause(
                inc_col_ident,
                lower_bound,
                include_lower_bound,
            )
            probe_where = f"({where_clause}) AND ({probe_cursor_clause})"
            probe_query = (
                f"SELECT {inc_col_ident} FROM {table_ident} "
                f"WHERE {probe_where} "
                f"ORDER BY {inc_col_ident} ASC "
                f"LIMIT {self.batch_size}"
            )
            app_logger.info(
                f"🔎 Probing [{table}] cursor window with lower="
                f"{self._to_sql_literal(lower_bound) if lower_bound is not None else 'NULL'}"
            )
            probe_result = self._with_retry(lambda: self.src.query(probe_query), f"probe source batch for {table}")
            probe_rows = probe_result.result_rows

            if not probe_rows:
                break

            upper_bound = probe_rows[-1][0]
            window_cursor_clause = self._build_window_clause(
                inc_col_ident,
                lower_bound,
                include_lower_bound,
                upper_bound,
            )
            window_where = f"({where_clause}) AND ({window_cursor_clause})"
            query = (
                f"SELECT {col_names_ident} FROM {table_ident} "
                f"WHERE {window_where} "
                f"ORDER BY {inc_col_ident} ASC"
            )
            result = self._with_retry(lambda: self.src.query(query), f"query source window for {table}")
            rows = result.result_rows

            if not rows:
                lower_bound = upper_bound
                include_lower_bound = False
                continue

            self._with_retry(
                lambda: self.dest.insert(table=table, data=rows, column_names=col_names),
                f"insert batch for {table}",
            )

            batch_rows = len(rows)
            total_synced += batch_rows
            app_logger.info(
                f"✅ Synced batch for [{table}] rows={batch_rows}, total={total_synced}, "
                f"next_cursor>{self._to_sql_literal(upper_bound)}"
            )

            lower_bound = upper_bound
            include_lower_bound = False

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

    def _ensure_target_columns_compatible(self, table: str, table_ident: str) -> None:
        src_cols = self.src.query(f"DESCRIBE TABLE {table_ident}").result_rows
        dst_cols = self.dest.query(f"DESCRIBE TABLE {table_ident}").result_rows
        dst_col_names = {row[0] for row in dst_cols}

        for col_name, col_type, *_ in src_cols:
            if col_name in dst_col_names:
                continue
            add_col_sql = (
                f"ALTER TABLE {table_ident} "
                f"ADD COLUMN IF NOT EXISTS {self._quote_ident(col_name)} {col_type}"
            )
            self._with_retry(
                lambda sql=add_col_sql: self.dest.command(sql),
                f"add missing column {col_name} for {table}",
            )
            app_logger.warning(
                f"⚠️ Added missing target column for [{table}]: {col_name} {col_type}"
            )

    def _build_where_clause_for_source(self, max_val_res: Any, inc_col_ident: str) -> str:
        if max_val_res is None or str(max_val_res).startswith("1970"):
            return "1=1"
        return f"{inc_col_ident} >= {self._to_sql_literal(max_val_res)}"

    def _build_cursor_clause(self, inc_col_ident: str, lower_bound: Any, include_lower: bool) -> str:
        if lower_bound is None or str(lower_bound).startswith("1970"):
            return "1=1"
        op = ">=" if include_lower else ">"
        return f"{inc_col_ident} {op} {self._to_sql_literal(lower_bound)}"

    def _build_window_clause(
        self,
        inc_col_ident: str,
        lower_bound: Any,
        include_lower: bool,
        upper_bound: Any,
    ) -> str:
        if lower_bound is None or str(lower_bound).startswith("1970"):
            return f"{inc_col_ident} <= {self._to_sql_literal(upper_bound)}"
        lower_op = ">=" if include_lower else ">"
        return (
            f"{inc_col_ident} {lower_op} {self._to_sql_literal(lower_bound)} "
            f"AND {inc_col_ident} <= {self._to_sql_literal(upper_bound)}"
        )

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
            if value.microsecond:
                # Preserve sub-second precision to avoid widening incremental windows.
                return f"toDateTime64('{value.strftime('%Y-%m-%d %H:%M:%S.%f')}', 6)"
            return f"toDateTime('{value.strftime('%Y-%m-%d %H:%M:%S')}')"
        if isinstance(value, date):
            return f"toDate('{value.strftime('%Y-%m-%d')}')"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        return str(value)
