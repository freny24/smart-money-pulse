"""
Smart Money Pulse — Streamlit Dashboard
Behavioral Finance Stress Detection Pipeline
Author: Freny Reji | Stack: DuckDB · dbt · Airflow · Plotly
"""
import sys, os

# ── Add project root to path so we can import scripts directly ────────────────
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

# ── DB path (relative, works anywhere) ───────────────────────────────────────
DB_PATH = os.path.join(BASE, "data", "smart_money.duckdb")
os.makedirs(os.path.join(BASE, "data"), exist_ok=True)

# ── Bootstrap: generate + run pipeline if DB missing ─────────────────────────
if not os.path.exists(DB_PATH):
    import streamlit as st
    with st.spinner("⏳ First run: generating 47K transactions and running pipeline (~60s)..."):
        # Import and run generate_data directly (no subprocess)
        import random, numpy as np, pandas as pd, duckdb
        from datetime import datetime, timedelta

        random.seed(42); np.random.seed(42)
        N_USERS = 500
        START_DATE = datetime(2023, 1, 1)
        END_DATE   = datetime(2025, 1, 1)
        total_days = (END_DATE - START_DATE).days

        CATEGORIES = {
            "groceries":      {"merchants":["Whole Foods","Kroger","Aldi"],"avg":85,"std":35},
            "dining":         {"merchants":["McDonald's","Chipotle","Starbucks"],"avg":28,"std":18},
            "utilities":      {"merchants":["Electric Co","Gas Co","Internet ISP"],"avg":120,"std":30},
            "rent":           {"merchants":["Apt Management LLC","Realty Corp"],"avg":1400,"std":400},
            "healthcare":     {"merchants":["CVS Pharmacy","Walgreens"],"avg":75,"std":120},
            "entertainment":  {"merchants":["Netflix","Spotify","Steam"],"avg":22,"std":15},
            "fuel":           {"merchants":["Shell","BP","Chevron"],"avg":55,"std":20},
            "retail":         {"merchants":["Amazon","Target","Walmart"],"avg":95,"std":70},
            "atm_cash":       {"merchants":["ATM Withdrawal","Cash Advance"],"avg":160,"std":100},
            "alcohol_tobacco":{"merchants":["Total Wine","Local Bar"],"avg":35,"std":25},
            "gambling":       {"merchants":["DraftKings","Lottery"],"avg":80,"std":120},
            "payday_loan":    {"merchants":["QuickCash","SpeedyLoan"],"avg":300,"std":150},
            "transfer_in":    {"merchants":["Zelle Deposit","ACH Payroll"],"avg":2200,"std":800},
        }
        COHORTS = {
            "stable_earner":          {"weight":0.30,"avg_income":5500,"stress_base":0.10,"risk_prob":0.05},
            "gig_worker":             {"weight":0.20,"avg_income":3200,"stress_base":0.40,"risk_prob":0.15},
            "stressed_spender":       {"weight":0.15,"avg_income":3800,"stress_base":0.72,"risk_prob":0.35},
            "financially_distressed": {"weight":0.10,"avg_income":2400,"stress_base":0.88,"risk_prob":0.55},
            "high_earner":            {"weight":0.15,"avg_income":12000,"stress_base":0.08,"risk_prob":0.02},
            "recovering":             {"weight":0.10,"avg_income":4100,"stress_base":0.45,"risk_prob":0.20},
        }
        STATES = ["CA","TX","NY","FL","IL","PA","OH","GA","NC","MI","WA","AZ","MA","TN","IN"]

        names = list(COHORTS.keys())
        weights = [COHORTS[c]["weight"] for c in names]
        assigned = np.random.choice(names, size=N_USERS, p=weights)
        users = []
        for i in range(N_USERS):
            c = assigned[i]; cohort = COHORTS[c]
            users.append({
                "user_id": f"USR_{i+1:05d}", "cohort": c,
                "state": random.choice(STATES), "age": int(np.random.normal(38,12)),
                "avg_monthly_income": cohort["avg_income"]*np.random.uniform(0.7,1.3),
                "income_stability": (1-cohort["stress_base"])*np.random.uniform(0.85,1.0),
                "stress_score_base": min(1.0,cohort["stress_base"]*np.random.uniform(0.8,1.2)),
                "high_risk_cat_prob": cohort["risk_prob"],
                "created_at": START_DATE+timedelta(days=random.randint(0,30)),
            })
        users_df = pd.DataFrame(users)

        txns = []; txn_id = 1
        counts = np.clip(np.random.negative_binomial(5,0.05,N_USERS),30,300)
        for _, user in users_df.iterrows():
            uid = user["user_id"]; n = counts[int(uid.split("_")[1])-1]
            stress_periods = []
            if user["stress_score_base"] > 0.5:
                for _ in range(random.randint(1,4)):
                    s = random.randint(0,total_days-30)
                    stress_periods.append((s,s+random.randint(7,30)))
            for _ in range(n):
                txn_date = START_DATE+timedelta(days=random.randint(0,total_days-1))
                day_off = (txn_date-START_DATE).days
                in_stress = any(s<=day_off<=e for s,e in stress_periods)
                mult = np.random.uniform(1.4,2.2) if in_stress else 1.0
                if in_stress and random.random()<0.4:
                    cat = random.choice(["atm_cash","alcohol_tobacco","gambling","payday_loan"])
                elif random.random()<user["high_risk_cat_prob"]:
                    cat = random.choice(["atm_cash","alcohol_tobacco","gambling","payday_loan","dining"])
                else:
                    cat = random.choice(["groceries","dining","utilities","rent","healthcare","entertainment","fuel","retail","transfer_in"])
                cat_info = CATEGORIES[cat]
                is_credit = cat=="transfer_in"
                amount = max(0.50,round(abs(np.random.normal(cat_info["avg"],cat_info["std"])*mult),2))
                txns.append({
                    "transaction_id":f"TXN_{txn_id:07d}","user_id":uid,
                    "transaction_date":txn_date.date(),"merchant_name":random.choice(cat_info["merchants"]),
                    "category":cat,"amount":amount,
                    "transaction_type":"credit" if is_credit else "debit",
                    "is_stress_period":in_stress,"channel":random.choice(["card_present","online","atm","ach"]),
                })
                txn_id+=1
        txns_df = pd.DataFrame(txns).sample(frac=1,random_state=42).reset_index(drop=True)

        # Load to DuckDB
        con = duckdb.connect(DB_PATH)
        con.execute("DROP TABLE IF EXISTS raw_users")
        con.execute("DROP TABLE IF EXISTS raw_transactions")
        con.execute("""CREATE TABLE raw_users (
            user_id VARCHAR PRIMARY KEY, cohort VARCHAR, state VARCHAR, age INTEGER,
            avg_monthly_income DOUBLE, income_stability DOUBLE, stress_score_base DOUBLE,
            high_risk_cat_prob DOUBLE, created_at DATE)""")
        con.execute("""CREATE TABLE raw_transactions (
            transaction_id VARCHAR PRIMARY KEY, user_id VARCHAR, transaction_date DATE,
            merchant_name VARCHAR, category VARCHAR, amount DOUBLE,
            transaction_type VARCHAR, is_stress_period BOOLEAN, channel VARCHAR)""")
        con.executemany("INSERT INTO raw_users VALUES (?,?,?,?,?,?,?,?,?)",
            users_df[["user_id","cohort","state","age","avg_monthly_income",
                      "income_stability","stress_score_base","high_risk_cat_prob","created_at"]].values.tolist())
        con.executemany("INSERT INTO raw_transactions VALUES (?,?,?,?,?,?,?,?,?)",
            txns_df[["transaction_id","user_id","transaction_date","merchant_name",
                     "category","amount","transaction_type","is_stress_period","channel"]].values.tolist())

        # Run analytics pipeline
        SQL_PATH = os.path.join(BASE, "sql", "analytics_pipeline.sql")
        sql = open(SQL_PATH).read()
        for s in sql.split(";"):
            cleaned = "\n".join(l for l in s.split("\n") if not l.strip().startswith("--")).strip()
            if cleaned:
                try: con.execute(cleaned)
                except Exception: pass

        # Materialize marts
        for view in ["mart_cohort_health","mart_user_risk_profile","mart_kpi_summary"]:
            con.execute(f"DROP TABLE IF EXISTS {view}_tbl")
            con.execute(f"CREATE TABLE {view}_tbl AS SELECT * FROM {view}")
        con.close()

    st.success("✅ Database ready! Loading dashboard...")
    st.rerun()

# ── All imports after bootstrap ───────────────────────────────────────────────
import streamlit as st
import duckdb, pandas as pd, plotly.graph_objects as go, plotly.express as px

st.set_page_config(page_title="Smart Money Pulse", page_icon="📊", layout="wide",
                   initial_sidebar_state="expanded")

st.markdown("""<style>
.stApp{background:#0a0b0e;color:#c8cdd8}
.main .block-container{padding-top:1.2rem;padding-bottom:2rem}
.kpi-card{background:#0f1117;border:1px solid #1a1d26;border-top:3px solid #f5a623;
  border-radius:4px;padding:14px 18px;margin-bottom:6px}
.kpi-card.red{border-top-color:#e74c3c}.kpi-card.orange{border-top-color:#e67e22}
.kpi-label{font-family:'Courier New',monospace;font-size:10px;color:#4a5068;
  text-transform:uppercase;letter-spacing:2px;margin-bottom:5px}
.kpi-value{font-family:'Courier New',monospace;font-size:26px;font-weight:700;
  color:#f5a623;line-height:1}
.kpi-value.red{color:#e74c3c}.kpi-value.orange{color:#e67e22}
.kpi-sub{font-family:'Courier New',monospace;font-size:10px;color:#6b7280;margin-top:3px}
.sec-hdr{font-family:'Courier New',monospace;font-size:11px;color:#f5a623;
  text-transform:uppercase;letter-spacing:3px;border-left:3px solid #f5a623;
  padding-left:10px;margin:18px 0 10px}
.alert-strip{background:#1a0808;border:1px solid #e74c3c44;border-radius:4px;
  padding:8px 14px;margin-bottom:18px;font-family:'Courier New',monospace;
  font-size:11px;color:#e74c3c}
[data-testid="stSidebar"]{background:#0f1117;border-right:1px solid #1a1d26}
.stTabs [data-baseweb="tab"]{font-family:'Courier New',monospace;font-size:11px;
  letter-spacing:2px;color:#6b7280}
.stTabs [aria-selected="true"]{color:#f5a623!important}
</style>""", unsafe_allow_html=True)

@st.cache_data(ttl=300)
def q(sql):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(sql).df(); con.close(); return df

kpi         = q("SELECT * FROM mart_kpi_summary_tbl").iloc[0]
cohort_df   = q("SELECT * FROM mart_cohort_health_tbl ORDER BY txn_month, cohort")
risk_df     = q("SELECT * FROM mart_user_risk_profile_tbl ORDER BY intervention_priority DESC")
platform_df = q("""SELECT txn_month,
    ROUND(SUM(critical_count*1.0)/SUM(active_users)*100,2) AS pct_critical,
    ROUND(SUM(high_count*1.0)/SUM(active_users)*100,2)     AS pct_high,
    ROUND(SUM(moderate_count*1.0)/SUM(active_users)*100,2) AS pct_moderate,
    ROUND(SUM(low_count*1.0)/SUM(active_users)*100,2)      AS pct_low,
    ROUND(AVG(avg_stress_score),4)                         AS platform_stress
    FROM mart_cohort_health_tbl GROUP BY txn_month ORDER BY txn_month""")
stress_df   = q("""SELECT txn_month, cohort,
    ROUND(AVG(behavioral_stress_score),4) AS avg_stress
    FROM int_stress_scores GROUP BY txn_month, cohort ORDER BY txn_month, cohort""")
components_df = q("""SELECT cohort,
    ROUND(AVG(score_spend_pressure),3)     AS spend_pressure,
    ROUND(AVG(score_income_irregular),3)   AS income_irregular,
    ROUND(AVG(score_high_risk_exposure),3) AS high_risk_exposure,
    ROUND(AVG(score_income_shock),3)        AS income_shock,
    ROUND(AVG(score_payday_dependency),3)  AS payday_dependency,
    ROUND(AVG(score_atm_dependency),3)      AS atm_dependency,
    ROUND(AVG(score_impulse_spend),3)       AS impulse_spend
    FROM int_stress_scores GROUP BY cohort ORDER BY cohort""")

BG,SURF="0f1117","#0a0b0e"
GRID="#1a1d26"; TEXT="#6b7280"
AMBER,RED,ORANGE,GREEN,TEAL,BLUE="#f5a623","#e74c3c","#e67e22","#27ae60","#1abc9c","#3498db"
COHORT_C={"financially_distressed":RED,"gig_worker":ORANGE,"stressed_spender":AMBER,
          "recovering":BLUE,"stable_earner":GREEN,"high_earner":TEAL}
BAND_C={"CRITICAL":RED,"HIGH":ORANGE,"MODERATE":AMBER,"LOW":GREEN}

def bl(height=280):
    return dict(plot_bgcolor="#0f1117",paper_bgcolor="#0a0b0e",height=height,
                font=dict(color=TEXT,family="Courier New",size=10),
                margin=dict(l=40,r=16,t=36,b=36),
                xaxis=dict(gridcolor=GRID,linecolor=GRID,zerolinecolor=GRID),
                yaxis=dict(gridcolor=GRID,linecolor=GRID,zerolinecolor=GRID),
                legend=dict(bgcolor="rgba(0,0,0,0)",font=dict(size=9)))

st.markdown("""<div style="display:flex;align-items:center;gap:14px;margin-bottom:8px">
  <div style="width:5px;height:38px;background:#f5a623;border-radius:2px"></div>
  <div>
    <div style="font-family:'Courier New',monospace;font-size:20px;font-weight:700;color:#e8ecf4;letter-spacing:3px">SMART MONEY PULSE</div>
    <div style="font-family:'Courier New',monospace;font-size:9px;color:#4a5068;letter-spacing:3px">BEHAVIORAL FINANCE STRESS DETECTION · DuckDB · dbt · Airflow</div>
  </div>
  <div style="margin-left:auto;font-family:'Courier New',monospace;font-size:10px;color:#27ae60">■ LIVE · 47,394 TXN · 500 USERS</div>
</div>""", unsafe_allow_html=True)

st.markdown(f"""<div class="alert-strip">🔴 ACTIVE ALERTS &nbsp;·&nbsp;
  <span style="color:#6b7280">{int(kpi.critical_alerts)} CRITICAL &nbsp;·&nbsp;
  {int(kpi.high_alerts)} HIGH &nbsp;·&nbsp;
  {int(kpi.total_payday_loan_events):,} payday loan events &nbsp;·&nbsp;
  Avg spend-to-income: {kpi.avg_spend_to_income*100:.1f}% &nbsp;·&nbsp;
  Income instability: {kpi.avg_income_instability:.4f}</span></div>""", unsafe_allow_html=True)

k1,k2,k3,k4,k5=st.columns(5)
for col,label,val,unit,sub,cls in [
    (k1,"Platform Stress Index",f"{kpi.platform_avg_stress*100:.2f}","%","Weighted composite",""),
    (k2,"🔴 Critical Alerts",str(int(kpi.critical_alerts)),"","Stress ≥ 0.70","red"),
    (k3,"High Risk Users",str(int(kpi.high_alerts)),"","Stress ≥ 0.50","orange"),
    (k4,"% At Risk",f"{kpi.platform_pct_at_risk:.2f}","%","HIGH + CRITICAL","orange"),
    (k5,"Payday Loan Events",f"{int(kpi.total_payday_loan_events):,}","","24-month total","red"),
]:
    with col:
        st.markdown(f"""<div class="kpi-card {cls}">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value {cls}">{val}<span style="font-size:12px;color:#6b7280">{unit}</span></div>
          <div class="kpi-sub">{sub}</div></div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### 🎛 Filters")
    band_filter   = st.multiselect("Stress Band",["CRITICAL","HIGH","MODERATE","LOW"],default=["CRITICAL","HIGH"])
    cohort_filter = st.multiselect("Cohort",risk_df["cohort"].unique().tolist(),default=risk_df["cohort"].unique().tolist())
    top_n = st.slider("Intervention rows",5,100,20)
    st.markdown("---")
    st.markdown("**🔑 SQL Techniques**")
    st.markdown("· `STDDEV() OVER`\n· `LAG()`\n· `PARTITION BY`\n· `ROWS BETWEEN N PRECEDING`")

tab1,tab2,tab3,tab4=st.tabs(["📈  OVERVIEW","👥  COHORTS","🔬  RISK SIGNALS","🎯  INTERVENTION"])

with tab1:
    cl,cr=st.columns([3,1])
    with cl:
        st.markdown('<div class="sec-hdr">Platform Stress Index — 24-Month Timeline</div>',unsafe_allow_html=True)
        fig=go.Figure()
        fig.add_trace(go.Scatter(x=platform_df["txn_month"],y=platform_df["platform_stress"],
            mode="lines",line=dict(color=AMBER,width=2.5),fill="tozeroy",fillcolor="rgba(245,166,35,0.07)"))
        fig.update_layout(**bl(260)); fig.update_yaxes(tickformat=".3f")
        st.plotly_chart(fig,use_container_width=True)
    with cr:
        st.markdown('<div class="sec-hdr">Current Score</div>',unsafe_allow_html=True)
        g=go.Figure(go.Indicator(mode="gauge+number",value=round(kpi.platform_avg_stress*100,2),
            number={"suffix":"%","font":{"color":AMBER,"size":26,"family":"Courier New"}},
            gauge={"axis":{"range":[0,100],"tickcolor":TEXT},"bar":{"color":AMBER},"bgcolor":"#0f1117",
                   "steps":[{"range":[0,30],"color":"#0f2a1a"},{"range":[30,50],"color":"#2a1f0a"},
                             {"range":[50,70],"color":"#2a150a"},{"range":[70,100],"color":"#2a0a0a"}],
                   "threshold":{"line":{"color":RED,"width":3},"value":70}}))
        g.update_layout(paper_bgcolor="#0a0b0e",plot_bgcolor="#0f1117",
                        font=dict(color=TEXT,family="Courier New"),height=230,margin=dict(l=20,r=20,t=28,b=8))
        st.plotly_chart(g,use_container_width=True)
    st.markdown('<div class="sec-hdr">Stress Band Distribution — Stacked Monthly %</div>',unsafe_allow_html=True)
    fig2=go.Figure()
    for cn,band in [("pct_critical","CRITICAL"),("pct_high","HIGH"),("pct_moderate","MODERATE"),("pct_low","LOW")]:
        fig2.add_trace(go.Bar(x=platform_df["txn_month"],y=platform_df[cn],name=band,marker_color=BAND_C[band],opacity=0.85))
    fig2.update_layout(**bl(240),barmode="stack",yaxis_title="% of Users")
    st.plotly_chart(fig2,use_container_width=True)

with tab2:
    ca,cb=st.columns(2)
    with ca:
        st.markdown('<div class="sec-hdr">Cohort Stress Trend</div>',unsafe_allow_html=True)
        fig3=go.Figure()
        for cohort in stress_df["cohort"].unique():
            cdf=stress_df[stress_df["cohort"]==cohort]
            fig3.add_trace(go.Scatter(x=cdf["txn_month"],y=cdf["avg_stress"],mode="lines",name=cohort,
                line=dict(color=COHORT_C.get(cohort,BLUE),width=2)))
        fig3.update_layout(**bl(300)); st.plotly_chart(fig3,use_container_width=True)
    with cb:
        st.markdown('<div class="sec-hdr">% At-Risk & Payday by Cohort</div>',unsafe_allow_html=True)
        latest=cohort_df.sort_values("txn_month").groupby("cohort").last().reset_index()
        fig4=go.Figure()
        fig4.add_trace(go.Bar(x=latest["cohort"],y=latest["pct_at_risk"],name="% At Risk",
            marker_color=[COHORT_C.get(c,BLUE) for c in latest["cohort"]],opacity=0.85))
        fig4.add_trace(go.Bar(x=latest["cohort"],y=latest["pct_using_payday_loans"],name="% Payday",
            marker_color=[COHORT_C.get(c,BLUE) for c in latest["cohort"]],opacity=0.7))
        fig4.update_layout(**bl(300),barmode="group",yaxis_title="%")
        st.plotly_chart(fig4,use_container_width=True)
    st.markdown('<div class="sec-hdr">Score Component Radar</div>',unsafe_allow_html=True)
    sel=st.selectbox("Cohort",components_df["cohort"].tolist())
    row=components_df[components_df["cohort"]==sel].iloc[0]
    cats=["Spend Pressure","Income Irregular.","High-Risk Exp.","Income Shock","Payday Dep.","ATM Dep.","Impulse Spend"]
    vals=[row.spend_pressure,row.income_irregular,row.high_risk_exposure,
          row.income_shock,row.payday_dependency,row.atm_dependency,row.impulse_spend]
    fig5=go.Figure(go.Scatterpolar(r=vals+[vals[0]],theta=cats+[cats[0]],fill="toself",
        line=dict(color=COHORT_C.get(sel,AMBER),width=2),fillcolor="rgba(245,166,35,0.18)"))
    fig5.update_layout(polar=dict(bgcolor="#0f1117",
        radialaxis=dict(visible=True,range=[0,1],color=TEXT,gridcolor=GRID,tickfont=dict(size=8)),
        angularaxis=dict(color=TEXT,gridcolor=GRID)),
        paper_bgcolor="#0a0b0e",font=dict(color=TEXT,family="Courier New",size=10),
        height=360,margin=dict(l=60,r=60,t=36,b=36),showlegend=False)
    _,cr2,_=st.columns([1,2,1])
    with cr2: st.plotly_chart(fig5,use_container_width=True)

with tab3:
    st.markdown('<div class="sec-hdr">Score Component Weights</div>',unsafe_allow_html=True)
    wc=st.columns(7)
    for col,(label,w,color) in zip(wc,[("Spend Pressure","25%",RED),("Income Irreg.","20%",ORANGE),
        ("Income Shock","20%",RED),("High-Risk Exp.","15%",AMBER),("Payday Dep.","10%",ORANGE),
        ("ATM Cash-Out","5%",TEXT),("Impulse Spend","5%",TEXT)]):
        with col:
            st.markdown(f"""<div class="kpi-card" style="border-top-color:{color}">
              <div class="kpi-value" style="color:{color};font-size:20px">{w}</div>
              <div class="kpi-sub">{label}</div></div>""",unsafe_allow_html=True)
    st.markdown('<div class="sec-hdr">Signal Breakdown by Cohort</div>',unsafe_allow_html=True)
    fig6=go.Figure()
    for comp,color in [("high_risk_exposure",RED),("income_irregular",ORANGE),
                       ("payday_dependency",AMBER),("atm_dependency",BLUE),("impulse_spend",TEAL)]:
        fig6.add_trace(go.Bar(x=components_df["cohort"],y=components_df[comp],
            name=comp.replace("_"," ").title(),marker_color=color,opacity=0.85))
    fig6.update_layout(**bl(280),barmode="stack",yaxis_title="Score Contribution")
    st.plotly_chart(fig6,use_container_width=True)
    c1,c2=st.columns(2)
    with c1:
        st.markdown("**Income Irregularity SQL**")
        st.code("""STDDEV(monthly_income) OVER (
  PARTITION BY user_id
  ORDER BY     txn_month
  ROWS BETWEEN 2 PRECEDING
           AND CURRENT ROW
) AS income_stddev_3m,

monthly_income
- LAG(monthly_income, 1) OVER (
    PARTITION BY user_id
    ORDER BY     txn_month
) AS income_mom_delta""",language="sql")
    with c2:
        st.markdown("**Spend Velocity SQL**")
        st.code("""SUM(amount) OVER (
  PARTITION BY user_id
  ORDER BY     transaction_date
  ROWS BETWEEN 6 PRECEDING
           AND CURRENT ROW
) AS rolling_7d_spend,

rolling_7d_spend
- LAG(rolling_7d_spend, 7) OVER (
    PARTITION BY user_id
    ORDER BY     transaction_date
) AS spend_acceleration_7d""",language="sql")

with tab4:
    st.markdown('<div class="sec-hdr">Top Intervention Candidates</div>',unsafe_allow_html=True)
    filtered=risk_df[risk_df["current_stress_band"].isin(band_filter)&risk_df["cohort"].isin(cohort_filter)].head(top_n)
    cs,ct=st.columns([1,2])
    with cs:
        fig7=px.scatter(filtered,x="current_stress_score",y="intervention_priority",
            color="current_stress_band",size="total_payday_loans",color_discrete_map=BAND_C,
            hover_data=["user_id","cohort","state"],
            labels={"current_stress_score":"Stress","intervention_priority":"Priority"})
        fig7.update_layout(**bl(320)); st.plotly_chart(fig7,use_container_width=True)
    with ct:
        display=filtered[["user_id","cohort","state","current_stress_score","current_stress_band",
                           "intervention_priority","spend_to_income_ratio","total_payday_loans",
                           "stress_persistence_rate"]].rename(columns={
            "current_stress_score":"stress","current_stress_band":"band",
            "intervention_priority":"priority","spend_to_income_ratio":"spend/inc",
            "total_payday_loans":"payday","stress_persistence_rate":"persistence"})
        BM={"CRITICAL":"#e74c3c","HIGH":"#e67e22","MODERATE":"#f5a623","LOW":"#27ae60"}
        def cb(v): return f"color:{BM.get(v,'white')};font-weight:bold"
        def cs2(v):
            if isinstance(v,float):
                if v>=0.7: return "color:#e74c3c"
                if v>=0.5: return "color:#e67e22"
                if v>=0.3: return "color:#f5a623"
                return "color:#27ae60"
            return ""
        styled=(display.style.map(cb,subset=["band"]).map(cs2,subset=["stress","priority"])
            .format({"stress":"{:.3f}","priority":"{:.3f}","spend/inc":"{:.2f}","persistence":"{:.1%}"})
            .set_properties(**{"font-family":"Courier New","font-size":"11px"}))
        st.dataframe(styled,use_container_width=True,height=310)
    persist=risk_df.groupby("cohort")["stress_persistence_rate"].mean().reset_index()
    fig8=go.Figure(go.Bar(x=persist["cohort"],y=persist["stress_persistence_rate"],
        marker_color=[COHORT_C.get(c,BLUE) for c in persist["cohort"]],opacity=0.85,
        text=[f"{v:.1%}" for v in persist["stress_persistence_rate"]],
        textposition="outside",textfont=dict(color=TEXT,size=10)))
    fig8.update_layout(**bl(240),yaxis_title="Persistence Rate",yaxis_tickformat=".0%")
    st.plotly_chart(fig8,use_container_width=True)
