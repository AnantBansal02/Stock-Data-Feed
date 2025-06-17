# app/utils/ingest_instruments.py

import csv
from sqlalchemy.orm import Session
from app.models import Instrument

def load_instruments(csv_file: str, db: Session):
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            if row.get("instrument_type") != "EQUITY":
                continue

            instrument = Instrument(
                instrument_key=row["instrument_key"],
                exchange_token=row.get("exchange_token"),
                trading_symbol=row.get("trading_symbol"),
                name=row["name"],
                tick_size=row.get("tick_size"),
            )
            db.merge(instrument)
            count += 1

        db.commit()
        print(f"Ingested {count} EQUITY instruments.")