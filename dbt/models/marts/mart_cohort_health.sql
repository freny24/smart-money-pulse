{{ config(materialized='table', description='Cohort-month health KPIs for segment-level dashboard delivery') }}
SELECT cohort, txn_month,
    COUNT(DISTINCT user_id) AS active_users,
    ROUND(AVG(behavioral_stress_score),4)  AS avg_stress_score,
    ROUND(STDDEV(behavioral_stress_score),4) AS stress_score_stddev,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY behavioral_stress_score),4) AS stress_p50,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY behavioral_stress_score),4) AS stress_p90,
    COUNT(*) FILTER (WHERE stress_band='CRITICAL') AS critical_count,
    COUNT(*) FILTER (WHERE stress_band='HIGH')     AS high_count,
    COUNT(*) FILTER (WHERE stress_band='MODERATE') AS moderate_count,
    COUNT(*) FILTER (WHERE stress_band='LOW')      AS low_count,
    ROUND(100.0*COUNT(*) FILTER (WHERE stress_band IN ('CRITICAL','HIGH'))/NULLIF(COUNT(*),0),2) AS pct_at_risk,
    ROUND(AVG(spend_to_income_ratio),4) AS avg_spend_to_income,
    ROUND(AVG(income_cv),4)             AS avg_income_cv,
    ROUND(AVG(CASE WHEN payday_loan_count>0 THEN 1.0 ELSE 0.0 END)*100,2) AS pct_using_payday_loans,
    ROUND(AVG(AVG(behavioral_stress_score)) OVER (
          PARTITION BY cohort ORDER BY txn_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),4) AS rolling_3m_avg_stress
FROM {{ ref('int_stress_scores') }}
GROUP BY cohort, txn_month
