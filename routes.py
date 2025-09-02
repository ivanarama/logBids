from fastapi import Header, HTTPException, Query
from datetime import datetime, date
from models import SessionLocal, Bid
from reports import generate_and_send_report
from config import settings
from pydantic import BaseModel, field_validator

class BidRequest(BaseModel):
    bidid: str
    biddate: datetime
    direction: str
    branch: str
    source_id: str
    isrepeat: bool

    @field_validator("biddate", mode="before")
    def parse_biddate(cls, v):
        if isinstance(v, str):
            try:
                return datetime.strptime(v, "%d.%m.%Y %H:%M:%S")
            except ValueError:
                raise ValueError("biddate must be в формате DD.MM.YYYY HH:MM:SS")
        return v

async def add_bid(bid: BidRequest, authorization: str = Header(...)):
    if authorization != settings.SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    db = SessionLocal()
    new_bid = Bid(
        bidid=bid.bidid,
        biddate=bid.biddate,
        direction=bid.direction,
        branch=bid.branch,
        isrepeat=bid.isrepeat,
        source_id=bid.source_id
    )
    db.add(new_bid)
    db.commit()
    db.refresh(new_bid)
    db.close()
    return {"status": "ok", "id": new_bid.id}

async def send_report_now(
    report_date: str | None = Query(None),
    authorization: str = Header(...),
):
    if authorization != settings.SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if report_date:
        try:
            report_date_obj = datetime.strptime(report_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты, нужен YYYY-MM-DD")
    else:
        report_date_obj = date.today()

    await generate_and_send_report(report_date_obj)
    return {"status": "ok", "message": "report sent"}