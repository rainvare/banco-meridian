-- =============================================================================
-- queries.sql  —  Banco Meridian · KPI Queries
-- All monetary values in USD for cross-country comparability
-- =============================================================================


-- ── 1. Executive Overview ─────────────────────────────────────────────────────
-- Portfolio snapshot: customers, accounts, loans, fraud exposure
SELECT
    (SELECT COUNT(*) FROM dim_customer WHERE status = 'A')          AS active_customers,
    (SELECT COUNT(*) FROM dim_account  WHERE status = 'ACTIVA')     AS active_accounts,
    (SELECT ROUND(SUM(balance_usd),0) FROM dim_account
     WHERE status='ACTIVA' AND balance_usd > 0)                     AS total_deposits_usd,
    (SELECT COUNT(*) FROM fact_loans WHERE status='active')         AS active_loans,
    (SELECT ROUND(SUM(principal_usd),0) FROM fact_loans
     WHERE status='active')                                         AS loan_portfolio_usd,
    (SELECT COUNT(*) FROM fact_transactions WHERE fraud_flag=1)     AS flagged_transactions,
    (SELECT ROUND(SUM(amount_usd),0) FROM fact_transactions
     WHERE fraud_flag=1)                                            AS fraud_exposure_usd;


-- ── 2. Customer Portfolio by Country ─────────────────────────────────────────
SELECT
    c.country,
    COUNT(DISTINCT c.customer_id)                           AS customers,
    COUNT(DISTINCT a.account_id)                            AS accounts,
    ROUND(SUM(a.balance_usd) FILTER (WHERE a.balance_usd > 0), 0) AS deposits_usd,
    COUNT(DISTINCT l.loan_id)                               AS loans,
    ROUND(SUM(l.principal_usd), 0)                          AS loan_portfolio_usd
FROM dim_customer c
LEFT JOIN dim_account a      ON c.customer_id = a.customer_id
LEFT JOIN fact_loans  l      ON c.customer_id = l.customer_id AND l.status = 'active'
GROUP BY c.country
ORDER BY deposits_usd DESC;


-- ── 3. Churn Risk Distribution ────────────────────────────────────────────────
SELECT
    c.churn_risk,
    c.country,
    COUNT(*)                                                AS customers,
    ROUND(AVG(c.nps_score), 1)                             AS avg_nps,
    COUNT(DISTINCT l.loan_id)                              AS active_loans,
    ROUND(SUM(a.balance_usd) FILTER (WHERE a.balance_usd > 0), 0) AS deposits_usd
FROM dim_customer c
LEFT JOIN dim_account a ON c.customer_id = a.customer_id AND a.status = 'ACTIVA'
LEFT JOIN fact_loans  l ON c.customer_id = l.customer_id AND l.status = 'active'
WHERE c.churn_risk IS NOT NULL
GROUP BY c.churn_risk, c.country
ORDER BY c.churn_risk, deposits_usd DESC;


-- ── 4. Transaction Volume by Channel and Country ──────────────────────────────
SELECT
    c.country,
    t.channel,
    COUNT(*)                                AS total_transactions,
    ROUND(SUM(t.amount_usd), 0)            AS volume_usd,
    ROUND(AVG(t.amount_usd), 2)            AS avg_amount_usd,
    SUM(t.fraud_flag)                       AS fraud_count,
    ROUND(SUM(t.fraud_flag) * 100.0 /
          COUNT(*), 2)                      AS fraud_rate_pct
FROM fact_transactions t
JOIN dim_account  a ON t.account_id  = a.account_id
JOIN dim_customer c ON a.customer_id = c.customer_id
WHERE t.amount_usd IS NOT NULL
GROUP BY c.country, t.channel
ORDER BY c.country, volume_usd DESC;


-- ── 5. Fraud Analysis ─────────────────────────────────────────────────────────
SELECT
    fraud_reason,
    COUNT(*)                                AS flagged_txns,
    ROUND(SUM(amount_usd), 0)              AS total_exposure_usd,
    ROUND(AVG(amount_usd), 2)              AS avg_amount_usd,
    MIN(date)                               AS first_seen,
    MAX(date)                               AS last_seen
FROM fact_transactions
WHERE fraud_flag = 1 AND amount_usd IS NOT NULL
GROUP BY fraud_reason
ORDER BY total_exposure_usd DESC;


-- ── 6. Loan Portfolio Quality by Country ─────────────────────────────────────
SELECT
    country,
    status,
    COUNT(*)                                AS loans,
    ROUND(SUM(principal_usd), 0)           AS principal_usd,
    ROUND(AVG(interest_rate) * 100, 2)     AS avg_rate_pct,
    ROUND(AVG(days_overdue), 1)            AS avg_days_overdue
FROM fact_loans
GROUP BY country, status
ORDER BY country, loans DESC;


-- ── 7. Non-Performing Loan (NPL) Rate by Country ──────────────────────────────
WITH portfolio AS (
    SELECT country,
           SUM(principal_usd)                                       AS total_portfolio,
           SUM(principal_usd) FILTER (WHERE status IN
               ('defaulted','written_off','restructured'))          AS npl_portfolio
    FROM fact_loans
    GROUP BY country
)
SELECT
    country,
    ROUND(total_portfolio, 0)              AS total_portfolio_usd,
    ROUND(npl_portfolio, 0)               AS npl_usd,
    ROUND(npl_portfolio * 100.0 /
          total_portfolio, 2)             AS npl_rate_pct
FROM portfolio
ORDER BY npl_rate_pct DESC;


-- ── 8. Customer Support: Resolution Time and Satisfaction ────────────────────
SELECT
    country,
    category,
    COUNT(*)                                                AS tickets,
    ROUND(AVG(
        CASE WHEN resolved_at IS NOT NULL
        THEN (julianday(resolved_at) - julianday(created_at)) * 1440
        END
    ), 1)                                                   AS avg_resolution_min,
    ROUND(AVG(satisfaction), 2)                            AS avg_satisfaction,
    SUM(CASE WHEN resolution = 'escalated' THEN 1 ELSE 0 END) AS escalated_count
FROM fact_support_tickets
GROUP BY country, category
ORDER BY country, tickets DESC;


-- ── 9. Digital Adoption by Country ───────────────────────────────────────────
SELECT
    c.country,
    COUNT(DISTINCT e.customer_id)                           AS digital_customers,
    COUNT(*)                                                AS total_events,
    ROUND(AVG(e.duration_sec), 1)                          AS avg_session_sec,
    SUM(CASE WHEN e.device = 'ios' THEN 1 ELSE 0 END)     AS ios_events,
    SUM(CASE WHEN e.device = 'android' THEN 1 ELSE 0 END) AS android_events,
    ROUND(SUM(CASE WHEN e.success = 0 THEN 1 ELSE 0 END) * 100.0 /
          COUNT(*), 2)                                      AS failure_rate_pct
FROM fact_app_events e
JOIN dim_customer c ON e.customer_id = c.customer_id
GROUP BY c.country
ORDER BY digital_customers DESC;


-- ── 10. High-Value Customers at Risk (Churn + High Balance) ──────────────────
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name     AS customer_name,
    c.country,
    c.segment,
    c.churn_risk,
    c.nps_score,
    ROUND(SUM(a.balance_usd), 0)           AS total_deposits_usd,
    COUNT(DISTINCT l.loan_id)              AS active_loans,
    COUNT(DISTINCT t.ticket_id)            AS support_tickets_ytd
FROM dim_customer c
JOIN dim_account a         ON c.customer_id = a.customer_id AND a.status = 'ACTIVA' AND a.balance_usd > 0
LEFT JOIN fact_loans l     ON c.customer_id = l.customer_id AND l.status = 'active'
LEFT JOIN fact_support_tickets t ON c.customer_id = t.customer_id
WHERE c.churn_risk = 'high'
GROUP BY c.customer_id
HAVING total_deposits_usd > 10000
ORDER BY total_deposits_usd DESC
LIMIT 20;
