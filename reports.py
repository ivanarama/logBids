from datetime import date
import asyncio, ssl, smtplib
from email.message import EmailMessage
from email.utils import formataddr
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter
from sqlalchemy import cast, Date
from models import SessionLocal, Bid
from config import settings
from collections import defaultdict

from collections import defaultdict

async def generate_and_send_report(report_date: date):
    db = SessionLocal()
    try:
        rows = db.query(
            Bid.branch, Bid.direction, Bid.bidid, Bid.biddate, Bid.created_at, Bid.isrepeat
        ).filter(cast(Bid.biddate, Date) == report_date).order_by(
            Bid.branch, Bid.direction, Bid.biddate
        ).all()
    finally:
        db.close()

    if not rows:
        print(f"Нет заявок за {report_date}")
        return

    wb = Workbook()
    ws = wb.active
    ws.title = "Отчёт"

    # Заголовки
    ws.append(["Филиал", "Направление", "Заявка", "Дата", "Создано", "Всего", "Повторные"])
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")

    row_idx = 2
    current_branch = None
    current_direction = None
    branch_start_row = None
    direction_start_row = None

    # считаем заявки
    branch_stats = defaultdict(lambda: {"total": 0, "repeat": 0})
    direction_stats = defaultdict(lambda: {"total": 0, "repeat": 0})

    for branch, direction, bidid, biddate, created_at, isRepeat in rows:
        branch_stats[branch]["total"] += 1
        branch_stats[branch]["repeat"] += 1 if isRepeat else 0
        direction_stats[(branch, direction)]["total"] += 1
        direction_stats[(branch, direction)]["repeat"] += 1 if isRepeat else 0

    for branch, direction, bidid, biddate, created_at, isRepeat in rows:
        # новый филиал
        if branch != current_branch:
            # закрываем предыдущий филиал
            if branch_start_row and row_idx > branch_start_row + 1:
                ws.row_dimensions.group(branch_start_row + 1, row_idx - 1, outline_level=1)
            current_branch = branch
            branch_start_row = row_idx
            ws.append([
                branch, "", "", "", "",
                branch_stats[branch]["total"],
                branch_stats[branch]["repeat"]
            ])
            row_idx += 1
            current_direction = None

        # новое направление
        if direction != current_direction:
            # закрываем предыдущее направление
            if direction_start_row and row_idx > direction_start_row + 1:
                ws.row_dimensions.group(direction_start_row + 1, row_idx - 1, outline_level=2, hidden=True)
            current_direction = direction
            direction_start_row = row_idx
            ws.append([
                "", direction, "", "", "",
                direction_stats[(branch, direction)]["total"],
                direction_stats[(branch, direction)]["repeat"]
            ])
            row_idx += 1

        # заявка
        ws.append([
            "", "", bidid,
            biddate.strftime("%d.%m.%Y %H:%M:%S"),
            created_at.strftime("%d.%m.%Y %H:%M:%S"),
            1,
            1 if isRepeat else 0
        ])
        row_idx += 1

    # закрываем последний direction
    if direction_start_row and row_idx > direction_start_row + 1:
        ws.row_dimensions.group(direction_start_row + 1, row_idx - 1, outline_level=2, hidden=True)

    # закрываем последний branch
    if branch_start_row and row_idx > branch_start_row + 1:
        ws.row_dimensions.group(branch_start_row + 1, row_idx - 1, outline_level=1)

    # включаем группировку
    ws.sheet_properties.outlinePr.summaryBelow = True
    ws.sheet_properties.outlinePr.applyStyles = True

    # ширина колонок
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = max_len + 2

    # сохраняем
    filename = f"/tmp/report_{report_date}.xlsx"
    wb.save(filename)
    print(f"Отчёт сохранён: {filename}")

    email_addresses = [e.strip() for e in settings.REPORT_EMAIL_TO.split(",") if e.strip()]
    if not email_addresses:
        print("Нет email-адресов")
        return

    def _send():
        msg = EmailMessage()
        msg["Subject"] = f"Ежедневный отчёт {report_date}"
        msg["From"] = formataddr(("А-Айсберг", settings.SMTP_USER))
        msg["To"] = ", ".join(email_addresses)
        msg.set_content("В приложении отчёт", subtype="plain", charset="utf-8")

        with open(filename, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=f"report_{report_date}.xlsx"
            )

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(settings.SMTP_SERVER, int(settings.SMTP_PORT), context=ctx) as server:
            server.login(settings.SMTP_USER, settings.SMTP_PASS)
            server.send_message(msg)
            print(f"Письмо отправлено на {', '.join(email_addresses)}")

    await asyncio.to_thread(_send)