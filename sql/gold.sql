-- ===============================
-- GOLD SCHEMA
-- ===============================
CREATE SCHEMA gold;

-- ===============================
-- USER METRICS (Country + Age Group)
-- ===============================
CREATE TABLE gold.user_metrics AS
SELECT
    country,
    CASE
        WHEN age < 18 THEN 'under_18'
        WHEN age BETWEEN 18 AND 25 THEN '18_25'
        WHEN age BETWEEN 26 AND 35 THEN '26_35'
        WHEN age BETWEEN 36 AND 50 THEN '36_50'
        ELSE '50_plus'
    END AS age_group,
    COUNT(DISTINCT user_id) AS total_users
FROM silver.users
GROUP BY country, age_group;

-- ===============================
-- CONTENT METRICS
-- ===============================
CREATE TABLE gold.content_metrics AS
SELECT
    c.genre,
    COUNT(DISTINCT v.user_id) AS unique_viewers,
    SUM(v.watch_time_min) AS total_watch_time
FROM silver.viewing_logs v
JOIN silver.content c
  ON v.content_id = c.content_id
GROUP BY c.genre;

-- ===============================
-- REVENUE METRICS (MONTHLY)
-- ===============================
CREATE TABLE gold.revenue_metrics AS
SELECT
    DATE_TRUNC('month', payment_date) AS month,
    payment_method,
    SUM(amount) AS total_revenue,
    COUNT(DISTINCT s.user_id) AS paying_users
FROM silver.payments p
JOIN silver.subscriptions s
  ON p.subscription_id = s.subscription_id
GROUP BY month, payment_method;

-- ===============================
-- DASHBOARD METRICS (BI READY)
-- ===============================
CREATE TABLE gold.dashboard_metrics AS
SELECT
    DATE_TRUNC('month', p.payment_date) AS month,
    u.country,
    CASE
        WHEN u.age < 18 THEN 'under_18'
        WHEN u.age BETWEEN 18 AND 25 THEN '18_25'
        WHEN u.age BETWEEN 26 AND 35 THEN '26_35'
        WHEN u.age BETWEEN 36 AND 50 THEN '36_50'
        ELSE '50_plus'
    END AS age_group,
    p.payment_method,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT s.user_id) AS paying_users,
    COALESCE(SUM(p.amount), 0) AS total_revenue,
    COALESCE(SUM(v.watch_time_min), 0) AS total_watch_time
FROM silver.users u
LEFT JOIN silver.subscriptions s
  ON u.user_id = s.user_id
LEFT JOIN silver.payments p
  ON s.subscription_id = p.subscription_id
LEFT JOIN silver.viewing_logs v
  ON u.user_id = v.user_id
GROUP BY month, u.country, age_group, p.payment_method;


-- Silver total
SELECT SUM(amount) FROM silver.payments;

-- Gold total
SELECT SUM(total_revenue) FROM gold.revenue_metrics;

-- Silver users
SELECT COUNT(DISTINCT user_id) FROM silver.users;

-- Gold users
SELECT SUM(total_users) FROM gold.user_metrics;
