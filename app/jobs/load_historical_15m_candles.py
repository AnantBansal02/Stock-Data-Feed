from app.jobs.base import BaseJob
from app.jobs.scheduler import scheduler
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.db_crud import sync_historical_candles_with_db, fetch_all_instruments
from app.upstox_api import get_historical_candle_data
from datetime import datetime, timedelta
import pandas as pd
import pytz
import logging

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

def fetch_candles_from_upstox_api_and_sync_with_db():
    try:
        instruments = fetch_all_instruments()
        to_datetime = datetime.now(IST).replace(second=0, microsecond=0)
        to_datetime = to_datetime - timedelta(days=1)
        from_datetime = to_datetime - timedelta(weeks=2)

        to_date = to_datetime.strftime('%Y-%m-%d')
        from_date = from_datetime.strftime('%Y-%m-%d')
        
        def process_instrument(instrument):
            symbol = instrument.instrument_key
            try:
                raw = get_historical_candle_data(
                    symbol=symbol,
                    toDate = to_date,
                    fromDate = from_date,
                    timePeriod='minutes',
                    multiplier='15'
                )
                if(raw.status != 'success'):
                    logger.error(f"Error fetching historical candles for {symbol} 15m: {raw}")
                    return
                
                candles_list = getattr(raw.data, "candles", None)

                if not candles_list or len(candles_list) < 100:
                    logger.warning(f"{symbol}: Expected at least 100 candles, got {len(candles_list) if candles_list else 0}")
                    return

                candles_df = pd.DataFrame(
                    candles_list,
                    columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
                )
                candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"])
                sync_historical_candles_with_db(candles_df, instrument.instrument_key, "15m")
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
    
class loadHistoricalFifteenMinutesCandles(BaseJob):
    def run(self):
        logger.info("Running loadHistoricalFifteenMinutesCandles Job...")
        try:
            fetch_candles_from_upstox_api_and_sync_with_db()
            logger.info("loadHistoricalFifteenMinutesCandles job finished.")
        except Exception as e:
            logger.error(f"loadHistoricalFifteenMinutesCandles job failed: {e}")

    def schedule(self):
        scheduler.add_job(self.run, 'cron', hour=12, minute=30, day_of_week='tue-sat', id='load_historical_15m_candles')

        # run it once on startup
        self.run()