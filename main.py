from fastapi import FastAPI
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from sqlalchemy import create_engine, Column, String, DateTime, Integer
from sqlalchemy.orm import sessionmaker, declarative_base

# ---------- Настройки ----------
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@db:5432/mydb"

EMAIL_FROM = "your_email@gmail.com"
EMAIL_TO = "receiver@example.com"
EMAIL_PASSWORD = "your_app_password"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# ---------- База данных ----------
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Bid(Base):
    __tablename__ = "bids"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    bidid = Column(String, nullable=False)
    biddate = Column(DateTime, nullable=False)
    direction = Column(String, nullable=False)
    branch = Column(String, nullable=False)

Base.metadata.create_all(bind=engine)

# ---------- FastAPI ----------
app = FastAPI()

@app.post("/add_bid/")
def add_bid(bidid: str, biddate: datetime, direction: str, branch: str):
    db = SessionLocal()
    bid = Bid(bidid=bidid, biddate=biddate, direction=direction, branch=branch)
    db.add(bid)
    db.commit()
    db.refresh(bid)
    db.close()
    return {"status": "ok", "id": bid.id}

@app.get("/send_report_now/")
def send_report_now():
    generate_and_send_report()
    return {"status": "report sent"}

# ---------- Отчёт + почта ----------
def generate_and_send_report():
    db = SessionLocal()
    bids = db.query(Bid).all()
    db.close()

    if not bids:
        return

    data = [{"bidid": b.bidid, "biddate": b.biddate, "direction": b.direction, "branch": b.branch} for b in bids]
    df = pd.DataFrame(data)

    grouped = df.groupby("branch").size().reset_index(name="count")

    file_path = "report.xlsx"
    grouped.to_excel(file_path, index=False)

    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    msg['Subject'] = "Daily Report"

    part = MIMEBase('application', 'octet-stream')
    with open(file_path, "rb") as f:
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename=report.xlsx')
    msg.attach(part)

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(EMAIL_FROM, EMAIL_PASSWORD)
    server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
    server.quit()

# ---------- Планировщик ----------
scheduler = BackgroundScheduler()
scheduler.add_job(generate_and_send_report, 'cron', hour=23, minute=59)
scheduler.start()