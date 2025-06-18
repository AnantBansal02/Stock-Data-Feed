from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from app import models
from app.models import Instrument, Candle
from sqlalchemy.future import select
from app.utils.ingest_instruments import load_instruments
from app.jobs.scheduler import start_scheduler, shutdown_scheduler
from app.jobs.registry import register_all_jobs
from app.db import Base, sync_engine, async_engine, get_sync_session, get_async_session
from app.logging_config import setup_logging
import os, logging

setup_logging()

app = FastAPI(title="Stock Feed Service")

@app.on_event("startup")
async def startup_event():
    logging.info(f"Connecting to database at {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
    try:
        # Create tables
        Base.metadata.create_all(bind=sync_engine)
        logging.info("Database tables created successfully")
        # Start scheduler
        start_scheduler()
        register_all_jobs()
    except Exception as e:
        logging.error(f"Error during startup: {e}")
     
# --- Example sync route ---
@app.get("/instruments", summary="Get all instruments (sync)")
def get_instruments(db: Session = Depends(get_sync_session)):
    res = db.query(Instrument).all()
    db.close()
    return res

# --- Example async route ---
@app.get("/instruments-async", summary="Get all instruments (async)")
async def get_instruments_async(db: AsyncSession = Depends(get_async_session)):
    stmt = select(Instrument)
    result = await db.execute(stmt)
    instruments = result.scalars().all()
    return instruments


@app.on_event("shutdown")
def on_shutdown():
    shutdown_scheduler()

@app.get("/")
def health_check():
    return {"status": "running"}