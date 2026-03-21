import pandas as pd
from src.dao.clickhouse_manager import ClickHouseManager
from src.utils.logger import app_logger

class DatabaseSyncer:
    def __init__(self):
        self.src = ClickHouseManager.get_source_client()
        self.dest = ClickHouseManager.get_target_client()

    def sync_all(self):
        # 1. 获取源端所有表名
        res = self.src.query("SHOW TABLES")
        tables = [row[0] for row in res.result_rows]
        
        for table in tables:
            self.sync_table(table)
            
    def sync_table(self, table: str):
        try:
            # 2. 确保目标端表存在
            create_stmt = self.src.query(f"SHOW CREATE TABLE {table}").result_rows[0][0]
            
            # 若包含 ENGINE 定义，ClickHouse 跨库可能带有不同的配置，一般原样执行 CREATE TABLE IF NOT EXISTS
            # 替换 CREATE TABLE 为 CREATE TABLE IF NOT EXISTS
            if "CREATE TABLE" in create_stmt and "IF NOT EXISTS" not in create_stmt:
                create_stmt = create_stmt.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
                
            self.dest.command(create_stmt)
            
            # 3. 寻找增量字段优先级 (优先 update_time)
            cols = self.src.query(f"DESCRIBE TABLE {table}").result_rows
            col_names = [col[0] for col in cols]
            
            inc_col = None
            for candidate in ['update_time', 'filing_date', 'period_end', 'timestamp', 'date', 'ex_dividend_date']:
                if candidate in col_names:
                    inc_col = candidate
                    break
                    
            if not inc_col:
                app_logger.warning(f"⚠️ Table {table} has no recognized incremental column. Skipping to avoid extreme full copy.")
                return
                
            # 4. 获取目标端最大时间点
            max_val_res = self.dest.query(f"SELECT MAX({inc_col}) FROM {table}").result_rows[0][0]
            
            if max_val_res is None or str(max_val_res).startswith('1970'):
                where_clause = "1=1"
            else:
                where_clause = f"{inc_col} > '{max_val_res}'"
                    
            # 5. 读取与插入
            query = f"SELECT * FROM {table} WHERE {where_clause} ORDER BY {inc_col} ASC"
            app_logger.info(f"Syncing [{table}] via: {where_clause}")
            
            df = self.src.query_df(query)
            if not df.empty:
                # 去除源库独有而目标库因未知原因没有的 timezone 引发的异常
                for col in df.select_dtypes(include=['datetimetz']).columns:
                    df[col] = df[col].dt.tz_localize(None)
                    
                self.dest.insert_df(table, df)
                app_logger.info(f"✅ Synced {len(df)} rows for table: {table}")
            else:
                app_logger.info(f"⚡ Table {table} is already up-to-date.")
                
        except Exception as e:
            app_logger.error(f"❌ Error syncing table {table}: {e}")
