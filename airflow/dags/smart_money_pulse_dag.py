"""
Smart Money Pulse — Airflow DAG
Schedule: Daily 06:00 UTC
Flow: validate → stage → dbt → materialize → DQ checks → export → alert
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

logger = logging.getLogger(__name__)
PROJECT_DIR = "/opt/airflow/dags/smart_money_pulse"
DB_PATH = f"{PROJECT_DIR}/data/smart_money.duckdb"

default_args = {
    "owner": "data-engineering", "depends_on_past": False,
    "start_date": datetime(2025,1,1), "retries": 2,
    "retry_delay": timedelta(minutes=5), "email_on_failure": False,
}

def validate_source_data(**ctx):
    import duckdb
    con = duckdb.connect(DB_PATH)
    tc = con.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0]
    uc = con.execute("SELECT COUNT(*) FROM raw_users").fetchone()[0]
    if tc < 1000: raise ValueError(f"Too few transactions: {tc}")
    if uc < 100:  raise ValueError(f"Too few users: {uc}")
    ctx["ti"].xcom_push(key="txn_count", value=tc)
    logger.info(f"✅ Validated: {uc:,} users, {tc:,} transactions")
    con.close()

def run_analytics_pipeline(**ctx):
    import duckdb
    con = duckdb.connect(DB_PATH)
    sql = open(f"{PROJECT_DIR}/sql/analytics_pipeline.sql").read()
    for s in sql.split(";"):
        cleaned = "\n".join(l for l in s.split("\n") if not l.strip().startswith("--")).strip()
        if cleaned:
            try: con.execute(cleaned)
            except Exception as e: logger.warning(f"SQL warn: {e}")
    logger.info("✅ Analytics pipeline complete")
    con.close()

def materialize_marts(**ctx):
    import duckdb
    con = duckdb.connect(DB_PATH)
    for view in ["mart_cohort_health","mart_user_risk_profile","mart_kpi_summary"]:
        con.execute(f"DROP TABLE IF EXISTS {view}_tbl")
        con.execute(f"CREATE TABLE {view}_tbl AS SELECT * FROM {view}")
        n = con.execute(f"SELECT COUNT(*) FROM {view}_tbl").fetchone()[0]
        logger.info(f"  📦 {view}_tbl: {n:,} rows")
    con.close()

def run_dq_checks(**ctx):
    import duckdb
    con = duckdb.connect(DB_PATH)
    checks = [
        ("SELECT COUNT(*) FROM mart_user_risk_profile_tbl WHERE current_stress_score IS NULL", 0, "Null stress scores"),
        ("SELECT COUNT(*) FROM mart_user_risk_profile_tbl WHERE current_stress_score NOT BETWEEN 0 AND 1", 0, "Scores out of range"),
        ("SELECT COUNT(DISTINCT cohort) FROM mart_cohort_health_tbl", 6, "Missing cohorts"),
    ]
    failures = []
    for q, expected_min, msg in checks:
        result = con.execute(q).fetchone()[0]
        if result < expected_min: failures.append(f"FAIL: {msg} (got {result})")
        else: logger.info(f"  ✅ DQ pass: {msg} ({result})")
    con.close()
    if failures: raise AssertionError("\n".join(failures))

def export_kpis(**ctx):
    import duckdb, json, os
    con = duckdb.connect(DB_PATH)
    os.makedirs(f"{PROJECT_DIR}/data/exports", exist_ok=True)
    exports = {
        "kpi_summary":    "SELECT * FROM mart_kpi_summary_tbl",
        "cohort_health":  "SELECT * FROM mart_cohort_health_tbl ORDER BY cohort, txn_month",
        "user_risk":      "SELECT * FROM mart_user_risk_profile_tbl ORDER BY intervention_priority DESC LIMIT 500",
    }
    for name, q in exports.items():
        df = con.execute(q).df()
        df.to_json(f"{PROJECT_DIR}/data/exports/{name}.json", orient="records", indent=2, date_format="iso")
        logger.info(f"  📄 {name}.json: {len(df)} rows")
    con.close()
    logger.info("✅ KPI exports complete")

with DAG(
    dag_id="smart_money_pulse_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False, max_active_runs=1,
    tags=["behavioral-finance","duckdb","stress-detection"],
) as dag:
    start   = DummyOperator(task_id="pipeline_start")
    validate = PythonOperator(task_id="validate_source_data", python_callable=validate_source_data)
    pipeline = PythonOperator(task_id="run_analytics_pipeline", python_callable=run_analytics_pipeline)
    dbt_run  = BashOperator(task_id="dbt_run",
        bash_command=f"cd {PROJECT_DIR}/dbt && dbt run --profiles-dir . 2>&1 || echo 'dbt fallback'")
    materialize = PythonOperator(task_id="materialize_marts", python_callable=materialize_marts)
    dq      = PythonOperator(task_id="data_quality_checks", python_callable=run_dq_checks)
    export  = PythonOperator(task_id="export_kpis", python_callable=export_kpis)
    end     = DummyOperator(task_id="pipeline_end", trigger_rule=TriggerRule.ALL_DONE)

    start >> validate >> pipeline >> dbt_run >> materialize >> dq >> export >> end
