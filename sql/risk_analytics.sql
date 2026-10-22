-- =============================================================
-- FraudPulse Risk Analytics — Databricks SQL Queries
-- =============================================================
-- These queries run against the FraudPulse Gold and Anomaly
-- tables to provide actionable fraud-detection insights.
-- =============================================================

USE CATALOG workspace;
USE SCHEMA fraudpulse;

-- ---------------------------------------------------------
-- 1. Daily Fraud Dashboard Summary
-- ---------------------------------------------------------
SELECT
    event_date,
    total_transactions,
    total_amount,
    avg_amount,
    success_count,
    failed_count,
    pending_count,
    success_rate,
    cross_border_count,
    ROUND(cross_border_count * 100.0 / total_transactions, 2) AS cross_border_pct
FROM gold_daily_summary
ORDER BY event_date DESC
LIMIT 30;

-- ---------------------------------------------------------
-- 2. Top 10 Highest-Risk Merchants
-- ---------------------------------------------------------
SELECT
    merchant_id,
    total_transactions,
    total_amount,
    avg_risk_score,
    max_risk_score,
    critical_count,
    high_risk_count,
    failure_rate_pct,
    merchant_risk_tier
FROM gold_merchant_risk_profile
WHERE merchant_risk_tier IN ('CRITICAL', 'HIGH')
ORDER BY avg_risk_score DESC
LIMIT 10;

-- ---------------------------------------------------------
-- 3. Top 10 Highest-Risk Customers
-- ---------------------------------------------------------
SELECT
    customer_id,
    total_transactions,
    total_spent,
    avg_risk_score,
    max_risk_score,
    high_risk_txn_count,
    high_amount_flags,
    velocity_flags,
    unique_merchants,
    unique_countries,
    customer_risk_tier
FROM gold_customer_risk_profile
WHERE customer_risk_tier IN ('CRITICAL', 'HIGH')
ORDER BY avg_risk_score DESC
LIMIT 10;

-- ---------------------------------------------------------
-- 4. Risk Level Distribution
-- ---------------------------------------------------------
SELECT
    risk_level,
    COUNT(*) AS transaction_count,
    ROUND(AVG(risk_score), 2) AS avg_score,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(SUM(amount), 2) AS total_amount
FROM anomaly_scores
GROUP BY risk_level
ORDER BY avg_score DESC;

-- ---------------------------------------------------------
-- 5. Hourly Transaction Heatmap Data
-- ---------------------------------------------------------
SELECT
    event_hour,
    total_transactions,
    total_amount,
    avg_amount,
    failed_count,
    failure_rate_pct,
    unique_customers
FROM gold_hourly_pattern
ORDER BY event_hour;

-- ---------------------------------------------------------
-- 6. Payment Method Risk Analysis
-- ---------------------------------------------------------
SELECT
    pm.payment_method,
    pm.total_transactions,
    pm.total_amount,
    pm.avg_amount,
    pm.success_rate_pct,
    ROUND(AVG(a.risk_score), 2) AS avg_risk_score,
    SUM(CASE WHEN a.risk_level IN ('CRITICAL', 'HIGH') THEN 1 ELSE 0 END) AS high_risk_count
FROM gold_payment_method_summary pm
LEFT JOIN anomaly_scores a ON pm.payment_method = a.payment_method
GROUP BY
    pm.payment_method,
    pm.total_transactions,
    pm.total_amount,
    pm.avg_amount,
    pm.success_rate_pct
ORDER BY avg_risk_score DESC;

-- ---------------------------------------------------------
-- 7. Cross-Border Transaction Risk
-- ---------------------------------------------------------
SELECT
    country,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(risk_score), 2) AS avg_risk_score,
    SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_count
FROM anomaly_scores
GROUP BY country
ORDER BY avg_risk_score DESC;

-- ---------------------------------------------------------
-- 8. Recent Critical Alerts (Last 24 Hours)
-- ---------------------------------------------------------
SELECT
    event_id,
    transaction_id,
    customer_id,
    merchant_id,
    event_timestamp,
    amount,
    payment_method,
    country,
    risk_score,
    risk_level,
    flag_high_amount,
    flag_velocity,
    flag_cross_border,
    flag_off_hours,
    flag_merchant_risk
FROM anomaly_scores
WHERE risk_level = 'CRITICAL'
  AND event_timestamp >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
ORDER BY risk_score DESC, amount DESC
LIMIT 50;

-- ---------------------------------------------------------
-- 9. Customer Velocity Anomalies
-- ---------------------------------------------------------
SELECT
    customer_id,
    COUNT(*) AS flagged_transactions,
    MAX(txn_count_1h) AS max_velocity,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(SUM(amount), 2) AS total_amount,
    COLLECT_SET(merchant_id) AS merchants_involved
FROM anomaly_scores
WHERE flag_velocity = TRUE
GROUP BY customer_id
HAVING COUNT(*) >= 3
ORDER BY flagged_transactions DESC
LIMIT 20;

-- ---------------------------------------------------------
-- 10. Merchant Failure Rate Monitoring
-- ---------------------------------------------------------
SELECT
    merchant_id,
    total_transactions,
    failure_rate_pct,
    total_amount,
    avg_risk_score,
    critical_count,
    merchant_risk_tier
FROM gold_merchant_risk_profile
WHERE failure_rate_pct > 20
ORDER BY failure_rate_pct DESC;
