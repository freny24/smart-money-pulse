{{ config(materialized='view', description='7-component weighted behavioral stress score per user per month') }}
WITH scored AS (
    SELECT *,
        LEAST(1.0, COALESCE(spend_to_income_ratio,0)/1.5)                                AS score_spend_pressure,
        LEAST(1.0, COALESCE(income_cv,0)*2.0)                                             AS score_income_irregular,
        LEAST(1.0, COALESCE(high_risk_spend/NULLIF(monthly_spend,0),0)*5.0)               AS score_high_risk_exposure,
        LEAST(1.0, CAST(income_drop_flag AS DOUBLE)*0.8+CAST(consecutive_low_income AS DOUBLE)*0.2) AS score_income_shock,
        LEAST(1.0, CAST(payday_loan_count AS DOUBLE)*0.5)                                  AS score_payday_dependency,
        LEAST(1.0, COALESCE(atm_cash_ratio,0)*4.0)                                        AS score_atm_dependency,
        LEAST(1.0, COALESCE(weekend_spend_share,0)*2.5)                                   AS score_impulse_spend
    FROM {{ ref('int_income_signals') }}
),
with_composite AS (
    SELECT *,
        ROUND(score_spend_pressure*0.25+score_income_irregular*0.20+score_high_risk_exposure*0.15
            +score_income_shock*0.20+score_payday_dependency*0.10
            +score_atm_dependency*0.05+score_impulse_spend*0.05,4) AS behavioral_stress_score
    FROM scored
)
SELECT *,
    CASE WHEN behavioral_stress_score>=0.70 THEN 'CRITICAL'
         WHEN behavioral_stress_score>=0.50 THEN 'HIGH'
         WHEN behavioral_stress_score>=0.30 THEN 'MODERATE'
         ELSE 'LOW' END AS stress_band,
    ROUND(behavioral_stress_score-LAG(behavioral_stress_score,1)
          OVER (PARTITION BY user_id ORDER BY txn_month),4) AS stress_score_delta,
    ROUND(AVG(behavioral_stress_score) OVER (
          PARTITION BY user_id ORDER BY txn_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),4) AS rolling_3m_avg_stress
FROM with_composite
