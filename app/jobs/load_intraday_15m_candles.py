from app.jobs.base import BaseJob
from app.jobs.scheduler import scheduler
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.db_crud import sync_intraday_candles_with_db, fetch_all_instruments
from app.upstox_api import get_intraday_candle_data
from datetime import datetime, timedelta
import pandas as pd
import pytz
import logging

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

def fetch_candles_from_upstox_api_and_sync_with_db():
    try:
        instruments = fetch_all_instruments()
    
        def process_instrument(instrument):
            symbol = instrument.instrument_key
            try:
                raw = get_intraday_candle_data(
                    symbol=symbol,
                    timePeriod='minutes',
                    multiplier='15'
                )
                if(raw.status != 'success'):
                    logger.error(f"Error fetching intraday candles for {symbol} 15m: {raw}")
                    return
                
                candles_list = getattr(raw.data, "candles", None)

                candles_df = pd.DataFrame(
                    candles_list,
                    columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
                )
                candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"])
                sync_intraday_candles_with_db(candles_df, instrument.instrument_key, "15m")
            except Exception as e:
                logger.error(f"Error fetching candles for {symbol}: {e}")

        # Parallelize this task as each instrument is independent
        max_workers = min(20, len(instruments))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_instrument, instrument) for instrument in instruments]
            for f in as_completed(futures):
                f.result()

    except Exception as e:
        logger.error(f"Error in fetch_candles_from_upstox_api_and_sync_with_db: {e}")
    
class loadIntradayFifteenMinutesCandles(BaseJob):
    def run(self):
        logger.info("Running loadIntradayFifteenMinutesCandles Job...")
        try:
            fetch_candles_from_upstox_api_and_sync_with_db()
            logger.info("loadIntradayFifteenMinutesCandles job finished.")
        except Exception as e:
            logger.error(f"loadIntradayFifteenMinutesCandles job failed: {e}")

    def schedule(self):
        scheduler.add_job(
            self.run, 
            'cron', 
            hour=3, 
            minute=47, 
            day_of_week='mon-fri', 
            id='load_15m_candles_1',
            misfire_grace_time=120,
        )
        scheduler.add_job(
            self.run, 
            'cron', 
            hour='4-9', 
            minute='2,17,32,47', 
            day_of_week='mon-fri', 
            id='load_15m_candles_2',
            misfire_grace_time=120,
        )
        scheduler.add_job(self.run, 
            'cron', 
            hour=10, 
            minute=2, 
            day_of_week='mon-fri', 
            id='load_15m_candles_3',
            misfire_grace_time=120,
        )
        # run it once on startup
        self.run()