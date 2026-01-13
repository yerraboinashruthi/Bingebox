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

# Mapping CSV files → Bronze table names
TABLE_MAP = {
    "users_raw.csv": "users",
    "content_raw.csv": "content",
    "subscriptions_raw.csv": "subscriptions",
    "payments_raw.csv": "payments",
    "viewing_logs_raw.csv": "viewing_logs"
}

# Dependency-safe load order
LOAD_ORDER = [
    "users_raw.csv",
    "content_raw.csv",
    "subscriptions_raw.csv",
    "payments_raw.csv",
    "viewing_logs_raw.csv"
]

# ------------------------
# HELPER: sanitize column names
# ------------------------
def sanitize_columns(df):
    df = df.copy()
    df.columns = [re.sub(r'\s+', '_', col.strip()) for col in df.columns]
    return df

# ------------------------
# HELPER: calculate checksum
# ------------------------
def calculate_checksum(file_path):
    try:
        with open(file_path, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception:
        return None

# ------------------------
# ENSURE SCHEMAS EXIST
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

# ------------------------
# LOAD BRONZE TABLES
# ------------------------
for file_name in LOAD_ORDER:
    table_name = TABLE_MAP[file_name]
    file_path = os.path.join(BRONZE_DIR, file_name)

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        continue

    # Read CSV and sanitize
    df = sanitize_columns(pd.read_csv(file_path))
    row_count = len(df)
    checksum = calculate_checksum(file_path)

    try:
        with engine.begin() as conn:
            # Truncate table instead of dropping
            conn.execute(text(f"TRUNCATE TABLE {BRONZE_SCHEMA}.{table_name} CASCADE"))
            df.to_sql(
                table_name,
                conn,
                schema=BRONZE_SCHEMA,
                if_exists="append",
                index=False
            )

        # Insert audit log
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
        print(f"❌ Failed to load {file_name} → {BRONZE_SCHEMA}.{table_name} | Error: {e}")
