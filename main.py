import os
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

from src.core.syncer import DatabaseSyncer
from src.utils.logger import app_logger

def job():
    app_logger.info("🎬 Starting scheduled database transfer...")
    syncer = DatabaseSyncer()
    syncer.sync_all()
    app_logger.info("🏁 Finished database transfer.")

def main():
    # Load .env (如果在 Docker 内部可以依赖实际的环境变量注入，但 dotenv 提供兜底)
    load_dotenv()
    
    hour = os.getenv("SCHEDULER_HOUR", "0")
    minute = os.getenv("SCHEDULER_MINUTE", "0")
    
    scheduler = BlockingScheduler()
    app_logger.info(f"🕰️ Scheduled sync daily at {hour}:{minute} (Cron mode).")
    
    scheduler.add_job(
        job,
        'cron',
        hour=hour,
        minute=minute,
        id="daily_clickhouse_sync"
    )
    
    # 启动时先立即跑一次
    app_logger.info("🚀 Running initial sync on startup...")
    job()
    
    try:
        app_logger.info("⏳ Scheduler sleeping, waiting for cron triggers...")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        app_logger.info("🛑 Scheduler stopped.")

if __name__ == "__main__":
    main()
