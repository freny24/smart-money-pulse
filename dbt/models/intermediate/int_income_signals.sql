{{ config(materialized='view', description='Monthly income/spend aggregates with rolling STDDEV, LAG, and CV irregularity signals') }}
WITH monthly_agg AS (
    SELECT user_id, txn_month, cohort, state,
        SUM(amount) FILTER (WHERE transaction_type='credit') AS monthly_income,
        SUM(amount) FILTER (WHERE transaction_type='debit')  AS monthly_spend,
        SUM(amount) FILTER (WHERE is_high_risk=1)            AS high_risk_spend,
        COUNT(*)    FILTER (WHERE category='payday_loan')    AS payday_loan_count,
        SUM(amount) FILTER (WHERE transaction_type='debit' AND day_of_week IN (0,6)) AS weekend_spend,
        SUM(amount) FILTER (WHERE category='atm_cash')       AS atm_spend
    FROM {{ ref('stg_transactions') }}
    GROUP BY user_id, txn_month, cohort, state
),
with_windows AS (
    SELECT *,
        STDDEV(monthly_income) OVER (PARTITION BY user_id ORDER BY txn_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS income_stddev_3m,
        AVG(monthly_income)    OVER (PARTITION BY user_id ORDER BY txn_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS income_avg_3m,
        monthly_income - LAG(monthly_income,1) OVER (PARTITION BY user_id ORDER BY txn_month) AS income_mom_delta,
        monthly_spend  - LAG(monthly_spend ,1) OVER (PARTITION BY user_id ORDER BY txn_month) AS spend_mom_delta
    FROM monthly_agg
)
SELECT *,
    ROUND(monthly_spend/NULLIF(monthly_income,0),4)          AS spend_to_income_ratio,
    ROUND(income_stddev_3m/NULLIF(income_avg_3m,0),4)        AS income_cv,
    CASE WHEN monthly_income < income_avg_3m*0.70 THEN 1 ELSE 0 END AS income_drop_flag,
    CASE WHEN monthly_income < income_avg_3m*0.70
          AND LAG(monthly_income,1) OVER (PARTITION BY user_id ORDER BY txn_month)
              < LAG(income_avg_3m,1) OVER (PARTITION BY user_id ORDER BY txn_month)*0.70
         THEN 1 ELSE 0 END AS consecutive_low_income,
    ROUND(weekend_spend/NULLIF(monthly_spend,0),4) AS weekend_spend_share,
    ROUND(atm_spend/NULLIF(monthly_spend,0),4)     AS atm_cash_ratio
FROM with_windows
