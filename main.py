from fastapi import FastAPI
from routes import add_bid, send_report_now

app = FastAPI()
app.post("/add_bid/")(add_bid)
app.post("/send_report_now/")(send_report_now)