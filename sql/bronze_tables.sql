CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.rejected_rows (
    table_name TEXT,
    rule_name TEXT,
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_data JSONB
);
CREATE TABLE silver.users AS
SELECT DISTINCT
    user_id,
    signup_date,
    country,
    age,
    device_type
FROM bronze.users
WHERE user_id IS NOT NULL
  AND age IS NOT NULL
  AND age BETWEEN 0 AND 150;


INSERT INTO audit.rejected_rows
SELECT
    'silver.users',
    'age_range_check',
    'Age must be between 5 and 120',
    CURRENT_TIMESTAMP,
    to_jsonb(u)
FROM silver.users u
WHERE age < 5 OR age > 120;
CREATE TABLE silver.content AS
SELECT
    content_id,
    content_type,
    genre,
    release_date AS release_year,
    duration_min
FROM bronze.content
WHERE content_id IS NOT NULL;
INSERT INTO audit.rejected_rows
SELECT
    'silver.content',
    'duration_check',
    'Duration must be > 0',
    CURRENT_TIMESTAMP,
    to_jsonb(c)
FROM silver.content c
WHERE duration_min <= 0;
CREATE TABLE silver.subscriptions AS
SELECT
    subscription_id,
    user_id,
    plan,
    start_date,
    end_date,
    status
FROM bronze.subscriptions
WHERE subscription_id IS NOT NULL;
INSERT INTO audit.rejected_rows
SELECT
    'silver.subscriptions',
    'fk_user_check',
    'User does not exist',
    CURRENT_TIMESTAMP,
    to_jsonb(s)
FROM silver.subscriptions s
LEFT JOIN silver.users u
    ON s.user_id = u.user_id
WHERE u.user_id IS NULL;
CREATE TABLE silver.payments AS
SELECT
    payment_id,
    subscription_id,
    amount,
    payment_date,
    payment_status
FROM bronze.payments
WHERE payment_id IS NOT NULL;
INSERT INTO audit.rejected_rows
SELECT
    'silver.payments',
    'amount_check',
    'Payment amount cannot be negative',
    CURRENT_TIMESTAMP,
    to_jsonb(p)
FROM silver.payments p
WHERE amount < 0;
CREATE TABLE silver.viewing_logs AS
SELECT
    log_id,
    user_id,
    content_id,
    genre,
    watch_time_m,
    date,
    CASE
        WHEN completion_flag ILIKE 'y%' THEN TRUE
        WHEN completion_flag ILIKE 'n%' THEN FALSE
        ELSE NULL
    END AS completed
FROM bronze.viewing_logs
WHERE log_id IS NOT NULL;
INSERT INTO audit.rejected_rows
SELECT
    'silver.viewing_logs',
    'watch_time_check',
    'Watch time cannot be negative',
    CURRENT_TIMESTAMP,
    to_jsonb(v)
FROM silver.viewing_logs v
WHERE watch_time_m < 0;
