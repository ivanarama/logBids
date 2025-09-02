import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import date
from reports import generate_and_send_report

async def main():
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        generate_and_send_report,
        "cron",
        hour=23,
        minute=10,
        args=[date.today()],
        misfire_grace_time=3600
    )
    scheduler.start()
    print("Worker started")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())