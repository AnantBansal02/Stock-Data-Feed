from app.db import get_sync_session
from app.models import Instrument, Candle
from sqlalchemy import func
from sqlalchemy.orm import Session
import pytz

def query_instruments():
    session_gen = get_sync_session()
    db: Session = next(session_gen)
    limit: int = 1000
    try:
        count = db.query(Instrument).count()
        print(f"Total instruments: {count}")

        instruments = db.query(Instrument).limit(limit).all()
        for instrument in instruments:
            print(f"{instrument.instrument_key} - {instrument.trading_symbol} - {instrument.company_name} - {instrument.industry}")
    
    except Exception as e:
        print(f"Error querying instruments: {e}")
    finally:
        db.close()


def query_candles():
    session_gen = get_sync_session()
    db: Session = next(session_gen)
    limit: int = 1000
    try:
        count = db.query(Candle).count()
        print(f"Total candles: {count}")

        candles = db.query(Candle).limit(limit).all()
        for candle in candles:
            print(candle.timestamp.astimezone(pytz.timezone("Asia/Kolkata")))
            print(f"{candle.instrument_key} - {candle.timestamp} - {candle.close}")
    
    except Exception as e:
        print(f"Error querying candles: {e}")
    finally:
        db.close()
    
def analyze_candle_counts_per_instrument():
    session_gen = get_sync_session()
    db: Session = next(session_gen)
    try:
        instrument_keys = [i.instrument_key for i in db.query(Instrument.instrument_key).all()]

        for key in instrument_keys[:1]:
            candles = db.query(Candle).filter(
                Candle.instrument_key == key,
                Candle.timeframe == "15m"
            ).all()
            for candle in candles:
                print(candle.timestamp.astimezone(pytz.timezone("Asia/Kolkata")))
                print(f"{candle.instrument_key} - {candle.timestamp} - {candle.close}")

        unique_count = set()
        for key in instrument_keys:
            count = db.query(func.count(Candle.id)).filter(
                Candle.instrument_key == key,
                Candle.timeframe == "15m"
            ).scalar()
            unique_count.add(count)

        count = db.query(Candle).count()
        print(f"Total candles: {count}")
        print(unique_count)
    except Exception as e:
        print(f"Error analyzing candle counts: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    query_instruments()
    # query_candles()
    analyze_candle_counts_per_instrument()
