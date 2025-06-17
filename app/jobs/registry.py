# app/jobs/registry.py
from app.jobs.load_instruments_table import loadInstrumentsTable
from app.jobs.load_historical_15m_candles import loadHistoricalFifteenMinutesCandles
from app.jobs.load_intraday_15m_candles import loadIntradayFifteenMinutesCandles

def register_all_jobs():
    jobs = [
        loadInstrumentsTable(), 
        loadHistoricalFifteenMinutesCandles(), 
        loadIntradayFifteenMinutesCandles()
    ]
    for job in jobs:
        job.schedule()
