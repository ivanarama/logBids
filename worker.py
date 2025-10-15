import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import date, datetime, timedelta
import pytz
from reports import generate_and_send_report

async def daily_report():
    """Функция-обертка для передачи правильной даты"""
    # Получаем текущее время в московском часовом поясе
    moscow_tz = pytz.timezone('Europe/Moscow')
    moscow_now = datetime.now(moscow_tz)
    
    # Генерируем отчет за предыдущий день по московскому времени
    report_date = moscow_now.date() - timedelta(days=1)
    print(f"[WORKER] Запуск отчета за {report_date} (вчера по московскому времени, сейчас в Москве: {moscow_now.strftime('%Y-%m-%d %H:%M:%S %Z')})")
    try:
        result = await generate_and_send_report(report_date=report_date, debug=False)
        if result is None:
            print(f"[WORKER] ВНИМАНИЕ: Отчет вернул None - возможно нет данных или ошибка отправки")
        else:
            print(f"[WORKER] Отчет успешно отправлен: {result}")
    except Exception as e:
        print(f"[WORKER] Ошибка при отправке отчета: {e}")
        import traceback
        traceback.print_exc()

async def main():
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        daily_report,
        "cron",
        hour=23,
        minute=10,
        misfire_grace_time=86400,  # 24 часа - достаточно для обработки пропущенных задач
        coalesce=True,  # объединяет пропущенные задачи в одну
        max_instances=1  # только один экземпляр задачи может выполняться одновременно
    )
    scheduler.start()
    print("Worker started. Next report at 23:10 UTC (02:10 Moscow time)")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())