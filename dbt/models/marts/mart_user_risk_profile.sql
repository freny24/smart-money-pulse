{{ config(materialized='table', description='User-level risk profiles with lifetime stress history and intervention priority') }}
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY txn_month DESC) AS rn
    FROM {{ ref('int_stress_scores') }}
),
history AS (
    SELECT user_id,
        COUNT(DISTINCT txn_month) AS active_months,
        ROUND(AVG(behavioral_stress_score),4) AS lifetime_avg_stress,
        ROUND(MAX(behavioral_stress_score),4) AS peak_stress_score,
        SUM(CASE WHEN stress_band IN ('CRITICAL','HIGH') THEN 1 ELSE 0 END) AS high_stress_months,
        SUM(payday_loan_count) AS total_payday_loans,
        ROUND(AVG(spend_to_income_ratio),4) AS avg_spend_to_income
    FROM {{ ref('int_stress_scores') }} GROUP BY user_id
)
SELECT l.user_id, l.cohort, l.state, l.txn_month AS latest_month,
    l.behavioral_stress_score AS current_stress_score,
    l.stress_band AS current_stress_band, l.stress_score_delta,
    l.monthly_income, l.monthly_spend, l.spend_to_income_ratio, l.income_cv, l.payday_loan_count,
    h.active_months, h.lifetime_avg_stress, h.peak_stress_score,
    h.high_stress_months, h.total_payday_loans, h.avg_spend_to_income,
    ROUND(CAST(h.high_stress_months AS DOUBLE)/NULLIF(h.active_months,0),4) AS stress_persistence_rate,
    ROUND(l.behavioral_stress_score*0.50
        + CAST(h.high_stress_months AS DOUBLE)/NULLIF(h.active_months,0)*0.30
        + LEAST(1.0,h.total_payday_loans/5.0)*0.20,4) AS intervention_priority
FROM latest l JOIN history h ON l.user_id=h.user_id WHERE l.rn=1
