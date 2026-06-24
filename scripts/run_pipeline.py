"""Smart Money Pulse — SQL Pipeline Runner"""
import duckdb, os

BASE     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE, "data/smart_money.duckdb")
SQL_PATH = os.path.join(BASE, "sql/analytics_pipeline.sql")

def run_pipeline():
    con = duckdb.connect(DB_PATH)
    sql = open(SQL_PATH).read()
    stmts = []
    for s in sql.split(";"):
        cleaned = "\n".join(l for l in s.split("\n") if not l.strip().startswith("--")).strip()
        if cleaned: stmts.append(cleaned)
    for stmt in stmts:
        try: con.execute(stmt)
        except Exception as e: print(f"  ⚠ {e}")
    # Materialize marts
    for view in ["mart_cohort_health","mart_user_risk_profile","mart_kpi_summary"]:
        con.execute(f"DROP TABLE IF EXISTS {view}_tbl")
        con.execute(f"CREATE TABLE {view}_tbl AS SELECT * FROM {view}")
        n = con.execute(f"SELECT COUNT(*) FROM {view}_tbl").fetchone()[0]
        print(f"  ✅ {view}_tbl: {n:,} rows")
    kpi = con.execute("SELECT * FROM mart_kpi_summary").df()
    print("\n📊 Platform KPIs:")
    print(kpi.to_string(index=False))
    con.close()

if __name__ == "__main__":
    run_pipeline()
