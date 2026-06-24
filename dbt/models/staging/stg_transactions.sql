{{ config(materialized='view', description='Cleaned transactions with user cohort context and behavioral flags') }}
SELECT
    t.transaction_id, t.user_id, t.transaction_date,
    DATE_TRUNC('month', t.transaction_date)::DATE AS txn_month,
    DATE_TRUNC('week',  t.transaction_date)::DATE AS txn_week,
    DAYOFWEEK(t.transaction_date) AS day_of_week,
    t.merchant_name, t.category, t.amount, t.transaction_type, t.channel, t.is_stress_period,
    CASE WHEN t.transaction_type = 'credit' THEN 1 ELSE 0 END AS is_income,
    CASE WHEN t.category IN ('payday_loan','atm_cash','gambling','alcohol_tobacco') THEN 1 ELSE 0 END AS is_high_risk,
    u.cohort, u.state, u.age, u.avg_monthly_income AS declared_income
FROM {{ source('raw', 'raw_transactions') }} t
JOIN {{ source('raw', 'raw_users') }} u ON t.user_id = u.user_id
