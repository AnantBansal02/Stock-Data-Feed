# app/models.py - Database models

from sqlalchemy import (
    Column, Integer, String, Numeric, BigInteger, DateTime, ForeignKey, UniqueConstraint, Index, desc
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from app.db import Base

class Instrument(Base):
    __tablename__ = "instruments"
    instrument_key = Column(String, primary_key=True)  # e.g. "NSE_EQ|INE839G01010"
    trading_symbol = Column(String, nullable=True)
    company_name = Column(String, nullable=False)
    industry = Column(String, nullable=False)

    # Relationship to candles
    candles = relationship("Candle", back_populates="instrument")


class Candle(Base):
    __tablename__ = "candles"
    id = Column(Integer, primary_key=True, index=True)
    instrument_key = Column(String, ForeignKey("instruments.instrument_key"), nullable=False, index=True)
    timeframe = Column(String, nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    open = Column(Numeric(12, 4), nullable=False)
    high = Column(Numeric(12, 4), nullable=False)
    low = Column(Numeric(12, 4), nullable=False)
    close = Column(Numeric(12, 4), nullable=False)
    volume = Column(BigInteger, nullable=False)
    oi = Column(BigInteger, default=0)

    # Backref to instruments
    instrument = relationship("Instrument", back_populates="candles")

    __table_args__ = (
        UniqueConstraint("instrument_key", "timeframe", "timestamp", name="uix_instrument_timeframe_timestamp"),
        Index(
            "idx_instrument_timeframe_timestamp_desc",
            "instrument_key", "timeframe", desc("timestamp")
        ),
    )
