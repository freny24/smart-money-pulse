# 📊 Smart Money Pulse — Behavioral Finance Stress Detection Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10-yellow)](https://duckdb.org)
[![dbt](https://img.shields.io/badge/dbt-1.7-orange)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-red)](https://airflow.apache.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32-ff4b4b)](https://streamlit.io)

> Detects financial stress from behavioral transaction patterns — before defaults occur.
> Uses advanced SQL window functions to compute rolling signals, income irregularity, and cohort health scores across 47K+ synthetic transactions.

## 🚀 Quick Start

```bash
git clone https://github.com/frenyreji/smart-money-pulse
cd smart-money-pulse
pip install -r requirements.txt
python scripts/generate_data.py      # Generate 47K+ transactions in DuckDB
python scripts/run_pipeline.py       # Run SQL analytics pipeline
streamlit run dashboard/app.py       # Launch dashboard
```

## 🏗 Architecture

```
generate_data.py          → 47K synthetic transactions · DuckDB
        ↓
analytics_pipeline.sql    → STDDEV() OVER · LAG · PARTITION BY · rolling windows
        ↓
dbt/models/               → staging → intermediate → marts
        ↓
Airflow DAG               → daily 06:00 UTC · DQ checks · KPI export
        ↓
Streamlit Dashboard       → 4 tabs: Overview · Cohorts · Risk Signals · Intervention
```

## 🔬 Key SQL Techniques

```sql
-- Rolling income volatility (irregularity signal)
STDDEV(monthly_income) OVER (
    PARTITION BY user_id ORDER BY txn_month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
) AS income_stddev_3m,

-- Month-over-month income shock detection
monthly_income - LAG(monthly_income, 1) OVER (
    PARTITION BY user_id ORDER BY txn_month
) AS income_mom_delta,

-- 7-day spend velocity
SUM(amount) OVER (
    PARTITION BY user_id ORDER BY transaction_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
) AS rolling_7d_spend
```

## 📊 Stress Score — 7 Components

| Component | Signal | Weight |
|---|---|---|
| Spend Pressure | Spend-to-income ratio | 25% |
| Income Irregularity | Coefficient of variation (STDDEV/AVG) | 20% |
| Income Shock | >30% drop vs 3m rolling average | 20% |
| High-Risk Exposure | Payday loans, ATM, gambling spend % | 15% |
| Payday Dependency | Payday loan transaction count | 10% |
| ATM Cash-Out | Cash withdrawal ratio | 5% |
| Impulse Spend | Weekend spend share | 5% |

**Bands:** LOW (<0.30) · MODERATE (0.30–0.50) · HIGH (0.50–0.70) · CRITICAL (≥0.70)

## 📁 Structure

```
smart_money_pulse/
├── scripts/
│   ├── generate_data.py      # Synthetic data generator
│   └── run_pipeline.py       # SQL pipeline runner
├── sql/
│   └── analytics_pipeline.sql
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/stress_band.sql
│   └── models/
│       ├── staging/          # stg_transactions · stg_users
│       ├── intermediate/     # int_income_signals · int_stress_scores
│       └── marts/            # mart_cohort_health · mart_user_risk_profile
├── airflow/dags/
│   └── smart_money_pulse_dag.py
├── dashboard/
│   └── app.py                # Streamlit dashboard
├── data/
│   └── smart_money.duckdb    # Generated database
├── .streamlit/config.toml
└── requirements.txt
```

## 🛠 Tech Stack

| Layer | Tech |
|---|---|
| Database | DuckDB (embedded OLAP) |
| Transformations | dbt (3-layer model DAG) |
| Orchestration | Apache Airflow |
| Analytics | Python · Pandas · Advanced SQL |
| Dashboard | Streamlit · Plotly |

## 👩‍💻 Author

**Freny Reji** · M.S. Data Science, Indiana University  
freny.reji.ds@gmail.com · [LinkedIn](https://linkedin.com/in/frenyreji) · [GitHub](https://github.com/frenyreji)
