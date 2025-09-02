from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, func
from sqlalchemy.orm import declarative_base, sessionmaker
from config import settings

engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Bid(Base):
    __tablename__ = "bids"

    id = Column(Integer, primary_key=True, index=True)
    bidid = Column(String, index=True)
    biddate = Column(DateTime)
    direction = Column(String)
    branch = Column(String)
    isrepeat = Column(Boolean, nullable=False, server_default="false")
    source_id = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

# создаём таблицы, если их ещё нет
Base.metadata.create_all(bind=engine)