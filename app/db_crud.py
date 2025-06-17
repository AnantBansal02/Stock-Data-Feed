from sqlalchemy.orm import Session
from sqlalchemy import select, desc, text, update, delete
from sqlalchemy.dialects.postgresql import insert
from app.db import get_sync_session
from app.models import Instrument, Candle
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def fetch_all_instruments():
    try:
        session_gen = get_sync_session()
        db: Session = next(session_gen)
        return db.execute(select(Instrument)).scalars().all()
    finally:
        db.close()

def sync_instruments_with_db(equity_df: pd.DataFrame):
    session_gen = get_sync_session()
    db: Session = next(session_gen)

    try:
        csv_keys = set(equity_df["instrument_key"].tolist())

        # Get all existing instrument keys from DB
        existing_rows = db.execute(select(Instrument.instrument_key)).scalars().all()
        existing_keys = set(existing_rows)

        # Determine new and stale keys
        new_keys = csv_keys - existing_keys
        stale_keys = existing_keys - csv_keys

        
        # Insert new instruments
        new_instruments = [
            Instrument(
                instrument_key=row['instrument_key'],
                trading_symbol=row['trading_symbol'],
                company_name=row['company_name'],
                industry=row['industry'],
            )
            for _, row in equity_df.iterrows()
            if row['instrument_key'] in new_keys
        ]

        if new_instruments:
            db.add_all(new_instruments)
            logger.info(f"Inserted {len(new_instruments)} new instruments.")

        # Delete stale instruments
        if stale_keys:
            deleted = db.query(Instrument).filter(Instrument.instrument_key.in_(stale_keys)).delete(synchronize_session=False)
            logger.info(f"Deleted {deleted} stale instruments.")

        db.commit()

    except Exception as e:
        db.rollback()
        logger.error(f"Error syncing instruments: {e}")
    finally:
        db.close()

# No upsert logic!
def sync_historical_candles_with_db(candles_df: pd.DataFrame, instrument_key: str, timeframe: str):
    if candles_df.empty:
        logger.info(f"No valid historical candles for {instrument_key}, skipping DB sync.")
        return
    try:
        session_gen = get_sync_session()
        db: Session = next(session_gen)

        existing_timestamps = db.execute(
            select(Candle.timestamp)
            .where(
                (Candle.instrument_key == instrument_key) &
                (Candle.timeframe == timeframe)
            )
        ).scalars().all()

        existing_set = set(existing_timestamps)  # already datetime w/ timezone

        # Ensure same timezone (very important!)
        # converting to GMT as our timestamps in db are in GMT
        candles_df["timestamp"] = candles_df["timestamp"].dt.tz_convert("GMT")
        
        # Ensure to keep only the latest 100 candles
        candles_df = candles_df.sort_values(by="timestamp", ascending=False).head(100)

        # Compare datetime to datetime
        new_candles_df = candles_df[~candles_df["timestamp"].isin(existing_set)]


        if new_candles_df.empty:
            logger.info(f"No new candles to insert for {instrument_key} {timeframe}.")
        else:
            # Create new Candle model instances
            new_candles = [
                Candle(
                    instrument_key=instrument_key,
                    timeframe=timeframe,
                    timestamp=row["timestamp"],
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                    oi=row.get("oi", 0),
                )
                for _, row in new_candles_df.iterrows()
            ]

            db.add_all(new_candles)
            db.commit()

        # Keep only latest 100 candles for instrument + timeframe
        result = db.execute(text(f"""
            DELETE FROM candles
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                        ROW_NUMBER() OVER (
                            PARTITION BY instrument_key, timeframe
                            ORDER BY timestamp DESC
                        ) as rn
                    FROM candles
                    WHERE instrument_key = :instrument_key AND timeframe = :timeframe
                ) AS ranked
                WHERE rn > 100
            )
        """), {"instrument_key": instrument_key, "timeframe": timeframe})

        deleted = result.rowcount

        db.commit()
        logger.info(f"{instrument_key} {timeframe}: Inserted {len(new_candles_df)} new candles. Deleted {deleted} old candles.")

    except Exception as e:
        db.rollback()
        logger.error(f"Error syncing candles for {instrument_key} {timeframe}: {e}")
    finally:
        db.close()

# with upsert logic!
def sync_intraday_candles_with_db(candles_df: pd.DataFrame, instrument_key: str, timeframe: str):
    if candles_df.empty:
        logger.info(f"No valid intraday candles for {instrument_key}, skipping DB sync.")
        return
    try:
        session_gen = get_sync_session()
        db: Session = next(session_gen)

        candles_df["instrument_key"] = instrument_key
        candles_df["timeframe"] = timeframe
        candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"], utc=True)
        
        candle_dicts = candles_df.to_dict(orient="records")

        stmt = insert(Candle).values(candle_dicts)

        update_cols = ["open", "high", "low", "close", "volume", "oi"]

        stmt = stmt.on_conflict_do_update(
            index_elements=["instrument_key", "timeframe", "timestamp"],
            set_={col: getattr(stmt.excluded, col) for col in update_cols}
        )

        db.execute(stmt)
        db.commit()

        logger.info(f"{instrument_key} {timeframe}: Bulk upserted {len(candles_df)} candles.")
    except Exception as e:
        db.rollback()
        logger.error(f"{instrument_key} {timeframe}: Failed to bulk upsert candles: {e}")
    finally:
        db.close()

