import pandas as pd
from sqlalchemy import create_engine
import hashlib
import logging
from datetime import datetime
import os

DB_URI = "postgresql+psycopg2://postgres:root@localhost:5432/bronze"
CSV_FOLDER = "bronze_inputs/"
LOG_FILE = "logs/bronze_load.log"

tables = {
    "users": "users_raw.csv",
    "content": "content_raw.csv",
    "subscriptions": "subscriptions_raw.csv",
    "payments": "payments_raw.csv",
    "viewing_logs": "viewing_logs_raw.csv"
}

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(message)s"
)

engine = create_engine(DB_URI)

for table, csv_file in tables.items():
    start_time = datetime.now()
    df = pd.read_csv(CSV_FOLDER + csv_file)
    rows = len(df)
    checksum = hashlib.md5(
        pd.util.hash_pandas_object(df, index=True).values
    ).hexdigest()

    df.to_sql(table, engine, if_exists="replace", index=False)

    logging.info(
        f"TABLE={table} | ROWS={rows} | CHECKSUM={checksum} | STATUS=SUCCESS | TIME={datetime.now() - start_time}"
    )

    print(f"Loaded {table}: {rows} rows")
