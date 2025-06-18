from app.jobs.base import BaseJob
from app.jobs.scheduler import scheduler
from app.db_crud import sync_instruments_with_db
import pandas as pd
import logging

logger = logging.getLogger(__name__)
CSV_PATH = "data/NSE_500.csv"

def load_instruments_from_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    required_cols = {
        'instrument_key', 'trading_symbol', 'company_name','industry'
    }
    if not required_cols.issubset(df.columns):
        raise ValueError(f"CSV is missing required columns: {required_cols - set(df.columns)}")

    df['instrument_key'] = "NSE_EQ|" + df["instrument_key"].astype(str)
    logger.info(f"Loaded {len(df)} total instruments from csv.")
    return df

class loadInstrumentsTable(BaseJob):
    def run(self):
        logger.info("Running loadInstrumentsTable Job...")
        try:
            equity_df = load_instruments_from_csv(CSV_PATH)
            sync_instruments_with_db(equity_df)
            logger.info("loadInstrumentsTable job finished.")
        except Exception as e:
            logger.error(f"loadInstrumentsTable job failed: {e}")

    def schedule(self):
        # Weekly job
        scheduler.add_job(
            self.run,
            trigger='interval',
            weeks=26,
            id='loadInstrumentsTable',
            replace_existing=True,
            misfire_grace_time=120,
        )
        # run it once on startup
        self.run()
