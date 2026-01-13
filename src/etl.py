import argparse
from sqlalchemy import create_engine, text

# ------------------------
# CONFIG
# ------------------------
DB_URL = "postgresql+psycopg2://postgres:root@localhost:5432/postgres"
engine = create_engine(DB_URL)

# ------------------------
# SILVER TABLES & DQ
# ------------------------
SILVER_QUERIES = [
    # ------------------- users -------------------
    """
    TRUNCATE TABLE silver.users;
    INSERT INTO silver.users (user_id, signup_date, country, age, device_type)
    SELECT DISTINCT
        user_id,
        signup_date::date,
        country,
        age::int,
        device_type
    FROM bronze.users
    WHERE user_id IS NOT NULL
      AND age IS NOT NULL
      AND age::int BETWEEN 0 AND 150;
    """,
    """
    INSERT INTO audit.rejected_rows (table_name, rule_name, rejection_reason, rejected_at, row_data)
    SELECT
        'silver.users',
        'age_range_check',
        'Age must be between 5 and 120',
        CURRENT_TIMESTAMP,
        to_jsonb(u)
    FROM bronze.users u
    WHERE age::int < 5 OR age::int > 120;
    """,

    # ------------------- content -------------------
    """
    TRUNCATE TABLE silver.content;
    INSERT INTO silver.content (content_id, content_type, genre, release_year, duration_min)
    SELECT
        content_id,
        content_type,
        genre,
        release_date::date AS release_year,
        duration_min::int
    FROM bronze.content
    WHERE content_id IS NOT NULL;
    """,
    """
    INSERT INTO audit.rejected_rows (table_name, rule_name, rejection_reason, rejected_at, row_data)
    SELECT
        'silver.content',
        'duration_check',
        'Duration must be > 0',
        CURRENT_TIMESTAMP,
        to_jsonb(c)
    FROM bronze.content c
    WHERE duration_min::int <= 0;
    """,

    # ------------------- subscriptions -------------------
    """
    TRUNCATE TABLE silver.subscriptions;
    INSERT INTO silver.subscriptions (subscription_id, user_id, plan, start_date, end_date, status)
    SELECT
        subscription_id,
        user_id,
        plan,
        start_date::date,
        end_date::date,
        status
    FROM bronze.subscriptions
    WHERE subscription_id IS NOT NULL;
    """,
    """
    INSERT INTO audit.rejected_rows (table_name, rule_name, rejection_reason, rejected_at, row_data)
    SELECT
        'silver.subscriptions',
        'fk_user_check',
        'User does not exist',
        CURRENT_TIMESTAMP,
        to_jsonb(s)
    FROM bronze.subscriptions s
    LEFT JOIN silver.users u
        ON s.user_id = u.user_id
    WHERE u.user_id IS NULL;
    """,

    # ------------------- payments -------------------
    """
    TRUNCATE TABLE silver.payments;
    INSERT INTO silver.payments (payment_id, subscription_id, amount, payment_date, payment_method)
    SELECT
        payment_id,
        subscription_id,
        amount::numeric,
        payment_date::date,
        payment_method
    FROM bronze.payments
    WHERE payment_id IS NOT NULL;
    """,
    """
    INSERT INTO audit.rejected_rows (table_name, rule_name, rejection_reason, rejected_at, row_data)
    SELECT
        'silver.payments',
        'amount_check',
        'Payment amount cannot be negative',
        CURRENT_TIMESTAMP,
        to_jsonb(p)
    FROM bronze.payments p
    WHERE amount::numeric < 0;
    """,

    # ------------------- viewing_logs -------------------
    """
    TRUNCATE TABLE silver.viewing_logs;
    INSERT INTO silver.viewing_logs (log_id, user_id, content_id, genre, watch_time_m, date, completed)
    SELECT
        log_id,
        user_id,
        content_id,
        genre,
        watch_time_m::int,
        date::date,
        CASE
            WHEN completion_flag ILIKE 'y%' THEN TRUE
            WHEN completion_flag ILIKE 'n%' THEN FALSE
            ELSE NULL
        END AS completed
    FROM bronze.viewing_logs
    WHERE log_id IS NOT NULL;
    """,
    """
    INSERT INTO audit.rejected_rows (table_name, rule_name, rejection_reason, rejected_at, row_data)
    SELECT
        'silver.viewing_logs',
        'watch_time_check',
        'Watch time cannot be negative',
        CURRENT_TIMESTAMP,
        to_jsonb(v)
    FROM bronze.viewing_logs v
    WHERE watch_time_m::int < 0;
    """
]

# ------------------------
# BUILD SILVER FUNCTION
# ------------------------
def build_silver():
    print("Starting Silver load...")
    with engine.begin() as conn:
        for query in SILVER_QUERIES:
            conn.execute(text(query))
    print("✅ Silver load completed successfully.")


# ------------------------
# CLI SUPPORT
# ------------------------
if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="ETL pipeline - Silver load")
    parser.add_argument("--build_silver", action="store_true", help="Load Silver tables from Bronze")
    args = parser.parse_args()

    if args.build_silver:
        build_silver()
