import pandas as pd
import hashlib
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import re

# ------------------------
# CONFIG
# ------------------------
DB_URL = "postgresql+psycopg2://postgres:root@localhost:5432/postgres"
engine = create_engine(DB_URL)

BRONZE_DIR = "bronze_inputs"
BRONZE_SCHEMA = "bronze"
AUDIT_SCHEMA = "audit"
AUDIT_TABLE = "bronze_load_log"

TABLE_MAP = {
    "users_raw.csv": "users",
    "content_raw.csv": "content",
    "subscriptions_raw.csv": "subscriptions",
    "payments_raw.csv": "payments",
    "viewing_logs_raw.csv": "viewing_logs"
}

LOAD_ORDER = [
    "users_raw.csv",
    "content_raw.csv",
    "subscriptions_raw.csv",
    "payments_raw.csv",
    "viewing_logs_raw.csv"
]

# ------------------------
# HELPERS
# ------------------------
def sanitize_columns(df):
    df = df.copy()

    # Normalize names
    df.columns = [re.sub(r'\s+', '_', col.strip()) for col in df.columns]

    # Rename index columns to IDs (CRITICAL)
    rename_map = {
        "Unnamed:_0": "subscription_id",
        "Unnamed:_0.1": "payment_id",
        "Unnamed:_0.2": "log_id"
    }
    df.rename(columns=rename_map, inplace=True)

    # Drop any remaining unnamed junk
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    return df


def calculate_checksum(file_path):
    try:
        with open(file_path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception:
        return None

def parse_dates(df, table_name):
    """Normalize dates for Postgres"""
    if table_name == "users":
        df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce").dt.date

    elif table_name == "subscriptions":
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date

    elif table_name == "payments":
        df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce").dt.date

    elif table_name == "viewing_logs":
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    elif table_name == "content":
        df["release_date"] = pd.to_numeric(
            df["release_date"], errors="coerce"
        ).astype("Int64")

    return df

# ------------------------
# ENSURE SCHEMAS + AUDIT
# ------------------------
with engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}"))
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA}"))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_SCHEMA}.{AUDIT_TABLE} (
            table_name TEXT,
            file_name TEXT,
            row_count BIGINT,
            checksum TEXT,
            status TEXT,
            load_timestamp TIMESTAMP
        )
    """))

    # Ensure payments.user_id exists
    conn.execute(text("""
        ALTER TABLE bronze.payments
        ADD COLUMN IF NOT EXISTS user_id TEXT
    """))

# ------------------------
# LOAD BRONZE TABLES
# ------------------------
for file_name in LOAD_ORDER:
    table_name = TABLE_MAP[file_name]
    file_path = os.path.join(BRONZE_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        continue

    try:
        df = pd.read_csv(file_path)
        df = sanitize_columns(df)
        df = parse_dates(df, table_name)

        row_count = len(df)
        checksum = calculate_checksum(file_path)

        with engine.begin() as conn:
            conn.execute(
                text(f"TRUNCATE TABLE {BRONZE_SCHEMA}.{table_name} CASCADE")
            )
            df.to_sql(
                table_name,
                conn,
                schema=BRONZE_SCHEMA,
                if_exists="append",
                index=False
            )

        audit_df = pd.DataFrame([{
            "table_name": f"{BRONZE_SCHEMA}.{table_name}",
            "file_name": file_name,
            "row_count": row_count,
            "checksum": checksum,
            "status": "success",
            "load_timestamp": datetime.now()
        }])

        with engine.begin() as conn:
            audit_df.to_sql(
                AUDIT_TABLE,
                conn,
                schema=AUDIT_SCHEMA,
                if_exists="append",
                index=False
            )

        print(f"✅ Loaded {file_name} → {BRONZE_SCHEMA}.{table_name} ({row_count} rows)")

    except Exception as e:
        print(f"❌ Failed {file_name} → {table_name}")
        print(e)
