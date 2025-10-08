import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import date
from reports import generate_and_send_report

async def daily_report():
    """Функция-обертка для передачи правильной даты"""
    await generate_and_send_report(report_date=None, debug=False)

async def main():
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        daily_report,
        "cron",
        hour=23,
        minute=10,
        misfire_grace_time=3600
    )
    scheduler.start()
    print("Worker started. Next report at 23:10 UTC")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())