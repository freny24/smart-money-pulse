-- ============================================================
-- Smart Money Pulse — Behavioral Finance SQL Analytics Pipeline
-- Advanced: STDDEV() OVER · LAG · PARTITION BY · rolling windows
-- ============================================================

CREATE OR REPLACE VIEW stg_transactions AS
SELECT
    t.transaction_id, t.user_id, t.transaction_date,
    DATE_TRUNC('month', t.transaction_date)::DATE AS txn_month,
    DATE_TRUNC('week',  t.transaction_date)::DATE AS txn_week,
    DAYOFWEEK(t.transaction_date)                 AS day_of_week,
    t.merchant_name, t.category, t.amount, t.transaction_type, t.channel, t.is_stress_period,
    CASE WHEN t.transaction_type = 'credit' THEN 1 ELSE 0 END AS is_income,
    CASE WHEN t.category IN ('payday_loan','atm_cash','gambling','alcohol_tobacco') THEN 1 ELSE 0 END AS is_high_risk,
    u.cohort, u.state, u.age, u.avg_monthly_income AS declared_income
FROM raw_transactions t
JOIN raw_users u ON t.user_id = u.user_id;

CREATE OR REPLACE VIEW int_spend_velocity AS
WITH base AS (
    SELECT user_id, transaction_date, txn_month, category, amount, transaction_type, is_high_risk,
        SUM(CASE WHEN transaction_type='debit' THEN amount ELSE 0 END)
            OVER (PARTITION BY user_id ORDER BY transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_spend,
        SUM(CASE WHEN transaction_type='debit' THEN amount ELSE 0 END)
            OVER (PARTITION BY user_id ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_spend,
        SUM(CASE WHEN transaction_type='credit' THEN amount ELSE 0 END)
            OVER (PARTITION BY user_id ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_income,
        COUNT(*) OVER (PARTITION BY user_id ORDER BY transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_txn_count,
        SUM(is_high_risk * amount)
            OVER (PARTITION BY user_id ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30d_high_risk_spend
    FROM stg_transactions
)
SELECT *,
    rolling_7d_spend - LAG(rolling_7d_spend, 7) OVER (PARTITION BY user_id ORDER BY transaction_date) AS spend_acceleration_7d
FROM base;

CREATE OR REPLACE VIEW int_income_signals AS
WITH monthly_agg AS (
    SELECT user_id, txn_month, cohort, state,
        SUM(amount)  FILTER (WHERE transaction_type='credit') AS monthly_income,
        COUNT(*)     FILTER (WHERE transaction_type='credit') AS income_events,
        SUM(amount)  FILTER (WHERE transaction_type='debit')  AS monthly_spend,
        SUM(amount)  FILTER (WHERE is_high_risk=1)            AS high_risk_spend,
        COUNT(*)     FILTER (WHERE category='payday_loan')    AS payday_loan_count,
        SUM(amount)  FILTER (WHERE transaction_type='debit' AND day_of_week IN (0,6)) AS weekend_spend,
        SUM(amount)  FILTER (WHERE category='atm_cash')       AS atm_spend
    FROM stg_transactions GROUP BY user_id, txn_month, cohort, state
),
with_windows AS (
    SELECT *,
        STDDEV(monthly_income) OVER (PARTITION BY user_id ORDER BY txn_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS income_stddev_3m,
        AVG(monthly_income)    OVER (PARTITION BY user_id ORDER BY txn_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS income_avg_3m,
        STDDEV(monthly_spend)  OVER (PARTITION BY user_id ORDER BY txn_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS spend_stddev_3m,
        monthly_income - LAG(monthly_income,1) OVER (PARTITION BY user_id ORDER BY txn_month) AS income_mom_delta,
        monthly_spend  - LAG(monthly_spend ,1) OVER (PARTITION BY user_id ORDER BY txn_month) AS spend_mom_delta,
        LAG(monthly_income,2)  OVER (PARTITION BY user_id ORDER BY txn_month) AS income_2m_ago
    FROM monthly_agg
)
SELECT *,
    ROUND(monthly_spend / NULLIF(monthly_income,0), 4) AS spend_to_income_ratio,
    ROUND(income_stddev_3m / NULLIF(income_avg_3m,0), 4) AS income_cv,
    CASE WHEN monthly_income < income_avg_3m * 0.70 THEN 1 ELSE 0 END AS income_drop_flag,
    CASE WHEN monthly_income < income_avg_3m * 0.70
          AND LAG(monthly_income,1) OVER (PARTITION BY user_id ORDER BY txn_month)
              < LAG(income_avg_3m,1)  OVER (PARTITION BY user_id ORDER BY txn_month) * 0.70
         THEN 1 ELSE 0 END AS consecutive_low_income,
    ROUND(weekend_spend / NULLIF(monthly_spend,0), 4) AS weekend_spend_share,
    ROUND(atm_spend     / NULLIF(monthly_spend,0), 4) AS atm_cash_ratio
FROM with_windows;

CREATE OR REPLACE VIEW int_stress_scores AS
WITH scored AS (
    SELECT *,
        LEAST(1.0, COALESCE(spend_to_income_ratio,0) / 1.5)                              AS score_spend_pressure,
        LEAST(1.0, COALESCE(income_cv,0) * 2.0)                                           AS score_income_irregular,
        LEAST(1.0, COALESCE(high_risk_spend/NULLIF(monthly_spend,0),0) * 5.0)             AS score_high_risk_exposure,
        LEAST(1.0, CAST(income_drop_flag AS DOUBLE)*0.8 + CAST(consecutive_low_income AS DOUBLE)*0.2) AS score_income_shock,
        LEAST(1.0, CAST(payday_loan_count AS DOUBLE)*0.5)                                  AS score_payday_dependency,
        LEAST(1.0, COALESCE(atm_cash_ratio,0)*4.0)                                        AS score_atm_dependency,
        LEAST(1.0, COALESCE(weekend_spend_share,0)*2.5)                                   AS score_impulse_spend
    FROM int_income_signals
),
with_composite AS (
    SELECT *,
        ROUND(score_spend_pressure*0.25 + score_income_irregular*0.20 + score_high_risk_exposure*0.15
            + score_income_shock*0.20 + score_payday_dependency*0.10
            + score_atm_dependency*0.05 + score_impulse_spend*0.05, 4) AS behavioral_stress_score
    FROM scored
)
SELECT *,
    CASE WHEN behavioral_stress_score >= 0.70 THEN 'CRITICAL'
         WHEN behavioral_stress_score >= 0.50 THEN 'HIGH'
         WHEN behavioral_stress_score >= 0.30 THEN 'MODERATE'
         ELSE 'LOW' END AS stress_band,
    ROUND(behavioral_stress_score - LAG(behavioral_stress_score,1)
          OVER (PARTITION BY user_id ORDER BY txn_month), 4) AS stress_score_delta,
    ROUND(AVG(behavioral_stress_score) OVER (
          PARTITION BY user_id ORDER BY txn_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 4) AS rolling_3m_avg_stress
FROM with_composite;

CREATE OR REPLACE VIEW mart_cohort_health AS
SELECT cohort, txn_month,
    COUNT(DISTINCT user_id) AS active_users,
    ROUND(AVG(behavioral_stress_score),4)  AS avg_stress_score,
    ROUND(STDDEV(behavioral_stress_score),4) AS stress_score_stddev,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY behavioral_stress_score),4) AS stress_p50,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY behavioral_stress_score),4) AS stress_p90,
    COUNT(*) FILTER (WHERE stress_band='CRITICAL')  AS critical_count,
    COUNT(*) FILTER (WHERE stress_band='HIGH')      AS high_count,
    COUNT(*) FILTER (WHERE stress_band='MODERATE')  AS moderate_count,
    COUNT(*) FILTER (WHERE stress_band='LOW')       AS low_count,
    ROUND(100.0*COUNT(*) FILTER (WHERE stress_band IN ('CRITICAL','HIGH'))/COUNT(*),2) AS pct_at_risk,
    ROUND(AVG(spend_to_income_ratio),4) AS avg_spend_to_income,
    ROUND(AVG(income_cv),4)             AS avg_income_cv,
    ROUND(AVG(CASE WHEN payday_loan_count>0 THEN 1.0 ELSE 0.0 END)*100,2) AS pct_using_payday_loans,
    ROUND(AVG(AVG(behavioral_stress_score)) OVER (
          PARTITION BY cohort ORDER BY txn_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),4) AS rolling_3m_avg_stress
FROM int_stress_scores
GROUP BY cohort, txn_month;

CREATE OR REPLACE VIEW mart_user_risk_profile AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY txn_month DESC) AS rn FROM int_stress_scores
),
history AS (
    SELECT user_id,
        COUNT(DISTINCT txn_month) AS active_months,
        ROUND(AVG(behavioral_stress_score),4) AS lifetime_avg_stress,
        ROUND(MAX(behavioral_stress_score),4) AS peak_stress_score,
        SUM(CASE WHEN stress_band IN ('CRITICAL','HIGH') THEN 1 ELSE 0 END) AS high_stress_months,
        SUM(payday_loan_count) AS total_payday_loans,
        ROUND(AVG(spend_to_income_ratio),4) AS avg_spend_to_income
    FROM int_stress_scores GROUP BY user_id
)
SELECT l.user_id, l.cohort, l.state, l.txn_month AS latest_month,
    l.behavioral_stress_score AS current_stress_score,
    l.stress_band AS current_stress_band, l.stress_score_delta, l.rolling_3m_avg_stress,
    l.monthly_income, l.monthly_spend, l.spend_to_income_ratio, l.income_cv,
    l.payday_loan_count, l.score_spend_pressure, l.score_income_irregular,
    l.score_high_risk_exposure, l.score_income_shock, l.score_payday_dependency,
    h.active_months, h.lifetime_avg_stress, h.peak_stress_score,
    h.high_stress_months, h.total_payday_loans, h.avg_spend_to_income,
    ROUND(CAST(h.high_stress_months AS DOUBLE)/NULLIF(h.active_months,0),4) AS stress_persistence_rate,
    ROUND(l.behavioral_stress_score*0.50
        + CAST(h.high_stress_months AS DOUBLE)/NULLIF(h.active_months,0)*0.30
        + LEAST(1.0,h.total_payday_loans/5.0)*0.20, 4) AS intervention_priority
FROM latest l JOIN history h ON l.user_id=h.user_id WHERE l.rn=1;

CREATE OR REPLACE VIEW mart_kpi_summary AS
SELECT
    COUNT(DISTINCT user_id) AS total_users, COUNT(*) AS total_user_months,
    ROUND(AVG(behavioral_stress_score),4)  AS platform_avg_stress,
    COUNT(*) FILTER (WHERE stress_band='CRITICAL') AS critical_alerts,
    COUNT(*) FILTER (WHERE stress_band='HIGH')     AS high_alerts,
    ROUND(100.0*COUNT(*) FILTER (WHERE stress_band IN ('CRITICAL','HIGH'))/COUNT(*),2) AS platform_pct_at_risk,
    ROUND(AVG(spend_to_income_ratio),4) AS avg_spend_to_income,
    ROUND(AVG(income_cv),4)             AS avg_income_instability,
    SUM(payday_loan_count)              AS total_payday_loan_events
FROM int_stress_scores;
