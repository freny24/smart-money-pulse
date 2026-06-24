"""
Smart Money Pulse — Streamlit Dashboard
Behavioral Finance Stress Detection Pipeline
Author: Freny Reji | Stack: DuckDB · dbt · Airflow · Plotly
"""
import streamlit as st
import duckdb, pandas as pd, plotly.graph_objects as go
import plotly.express as px, os, sys

st.set_page_config(page_title="Smart Money Pulse", page_icon="📊", layout="wide",
                   initial_sidebar_state="expanded")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""<style>
.stApp{background:#0a0b0e;color:#c8cdd8}
.main .block-container{padding-top:1.2rem;padding-bottom:2rem}
.kpi-card{background:#0f1117;border:1px solid #1a1d26;border-top:3px solid #f5a623;
  border-radius:4px;padding:14px 18px;margin-bottom:6px}
.kpi-card.red{border-top-color:#e74c3c}.kpi-card.orange{border-top-color:#e67e22}
.kpi-card.green{border-top-color:#27ae60}.kpi-card.teal{border-top-color:#1abc9c}
.kpi-label{font-family:'Courier New',monospace;font-size:10px;color:#4a5068;
  text-transform:uppercase;letter-spacing:2px;margin-bottom:5px}
.kpi-value{font-family:'Courier New',monospace;font-size:26px;font-weight:700;
  color:#f5a623;line-height:1}
.kpi-value.red{color:#e74c3c}.kpi-value.orange{color:#e67e22}.kpi-value.green{color:#27ae60}
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

# ── DB connection ─────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "data/smart_money.duckdb")

@st.cache_data(ttl=300)
def q(sql):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(sql).df(); con.close(); return df

# ── Load data ─────────────────────────────────────────────────────────────────
kpi       = q("SELECT * FROM mart_kpi_summary_tbl").iloc[0]
cohort_df = q("SELECT * FROM mart_cohort_health_tbl ORDER BY txn_month, cohort")
risk_df   = q("SELECT * FROM mart_user_risk_profile_tbl ORDER BY intervention_priority DESC")
platform_df = q("""
    SELECT txn_month,
        ROUND(SUM(critical_count*1.0)/SUM(active_users)*100,2) AS pct_critical,
        ROUND(SUM(high_count*1.0)/SUM(active_users)*100,2)     AS pct_high,
        ROUND(SUM(moderate_count*1.0)/SUM(active_users)*100,2) AS pct_moderate,
        ROUND(SUM(low_count*1.0)/SUM(active_users)*100,2)      AS pct_low,
        ROUND(AVG(avg_stress_score),4)                         AS platform_stress
    FROM mart_cohort_health_tbl GROUP BY txn_month ORDER BY txn_month""")
stress_df = q("""
    SELECT txn_month, cohort, ROUND(AVG(behavioral_stress_score),4) AS avg_stress
    FROM int_stress_scores GROUP BY txn_month, cohort ORDER BY txn_month, cohort""")
components_df = q("""
    SELECT cohort,
        ROUND(AVG(score_spend_pressure),3)     AS spend_pressure,
        ROUND(AVG(score_income_irregular),3)   AS income_irregular,
        ROUND(AVG(score_high_risk_exposure),3) AS high_risk_exposure,
        ROUND(AVG(score_income_shock),3)        AS income_shock,
        ROUND(AVG(score_payday_dependency),3)  AS payday_dependency,
        ROUND(AVG(score_atm_dependency),3)      AS atm_dependency,
        ROUND(AVG(score_impulse_spend),3)       AS impulse_spend
    FROM int_stress_scores GROUP BY cohort ORDER BY cohort""")

# ── Theme ─────────────────────────────────────────────────────────────────────
BG, SURF = "#0f1117", "#0a0b0e"
GRID     = "#1a1d26"
TEXT     = "#6b7280"
AMBER, RED, ORANGE, GREEN, TEAL, BLUE = "#f5a623","#e74c3c","#e67e22","#27ae60","#1abc9c","#3498db"
COHORT_C = {"financially_distressed":RED,"gig_worker":ORANGE,"stressed_spender":AMBER,
            "recovering":BLUE,"stable_earner":GREEN,"high_earner":TEAL}
BAND_C   = {"CRITICAL":RED,"HIGH":ORANGE,"MODERATE":AMBER,"LOW":GREEN}

def blayout(height=280):
    return dict(plot_bgcolor=BG, paper_bgcolor=SURF, height=height,
                font=dict(color=TEXT,family="Courier New",size=10),
                margin=dict(l=40,r=16,t=36,b=36),
                xaxis=dict(gridcolor=GRID,linecolor=GRID,zerolinecolor=GRID),
                yaxis=dict(gridcolor=GRID,linecolor=GRID,zerolinecolor=GRID),
                legend=dict(bgcolor="rgba(0,0,0,0)",font=dict(size=9)))

def tip(active, payload, label):
    return None  # Plotly handles its own tooltips

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="display:flex;align-items:center;gap:14px;margin-bottom:8px">
  <div style="width:5px;height:38px;background:#f5a623;border-radius:2px"></div>
  <div>
    <div style="font-family:'Courier New',monospace;font-size:20px;font-weight:700;
      color:#e8ecf4;letter-spacing:3px">SMART MONEY PULSE</div>
    <div style="font-family:'Courier New',monospace;font-size:9px;color:#4a5068;
      letter-spacing:3px">BEHAVIORAL FINANCE STRESS DETECTION · DuckDB · dbt · Airflow</div>
  </div>
  <div style="margin-left:auto;font-family:'Courier New',monospace;font-size:10px;color:#27ae60">
    ■ LIVE · 47,394 TXN · 500 USERS
  </div>
</div>""", unsafe_allow_html=True)

st.markdown(f"""<div class="alert-strip">
  🔴 ACTIVE ALERTS &nbsp;·&nbsp;
  <span style="color:#6b7280">
  {int(kpi.critical_alerts)} CRITICAL &nbsp;·&nbsp; {int(kpi.high_alerts)} HIGH &nbsp;·&nbsp;
  {int(kpi.total_payday_loan_events):,} payday loan events &nbsp;·&nbsp;
  Avg spend-to-income: {kpi.avg_spend_to_income*100:.1f}% &nbsp;·&nbsp;
  Income instability: {kpi.avg_income_instability:.4f}
  </span></div>""", unsafe_allow_html=True)

# ── KPI Row ───────────────────────────────────────────────────────────────────
k1,k2,k3,k4,k5 = st.columns(5)
for col, label, val, unit, sub, cls in [
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

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🎛 Filters")
    band_filter = st.multiselect("Stress Band",["CRITICAL","HIGH","MODERATE","LOW"],
                                 default=["CRITICAL","HIGH"])
    cohort_filter = st.multiselect("Cohort", risk_df["cohort"].unique().tolist(),
                                   default=risk_df["cohort"].unique().tolist())
    top_n = st.slider("Intervention table rows",5,100,20)
    st.markdown("---")
    st.markdown("**📐 Architecture**")
    for layer,items in [
        ("Staging",["stg_transactions","stg_users"]),
        ("Intermediate",["int_income_signals","int_stress_scores","int_spend_velocity"]),
        ("Marts",["mart_cohort_health","mart_user_risk_profile","mart_kpi_summary"]),
    ]:
        st.markdown(f"**{layer}**")
        for item in items: st.markdown(f"  · `{item}`")
    st.markdown("---")
    st.markdown("**🔑 SQL Techniques**")
    st.markdown("· `STDDEV() OVER`\n· `LAG()` \n· `PARTITION BY`\n· Rolling windows\n· `ROWS BETWEEN N PRECEDING`")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1,tab2,tab3,tab4 = st.tabs(["📈  OVERVIEW","👥  COHORTS","🔬  RISK SIGNALS","🎯  INTERVENTION"])

# ══ TAB 1: OVERVIEW ══════════════════════════════════════════════════════════
with tab1:
    col_l, col_r = st.columns([3,1])
    with col_l:
        st.markdown('<div class="sec-hdr">Platform Stress Index — 24-Month Timeline</div>',
                    unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=platform_df["txn_month"], y=platform_df["platform_stress"],
            mode="lines", name="Stress Index",
            line=dict(color=AMBER,width=2.5),
            fill="tozeroy", fillcolor="rgba(245,166,35,0.07)"))
        fig.update_layout(**blayout(260))
        fig.update_yaxes(title_text="Stress Score", tickformat=".3f")
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown('<div class="sec-hdr">Current Score</div>', unsafe_allow_html=True)
        gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=round(kpi.platform_avg_stress*100,2),
            number={"suffix":"%","font":{"color":AMBER,"size":26,"family":"Courier New"}},
            gauge={
                "axis":{"range":[0,100],"tickcolor":TEXT,"tickfont":{"color":TEXT,"size":9}},
                "bar":{"color":AMBER},
                "bgcolor":BG,
                "steps":[{"range":[0,30],"color":"#0f2a1a"},{"range":[30,50],"color":"#2a1f0a"},
                         {"range":[50,70],"color":"#2a150a"},{"range":[70,100],"color":"#2a0a0a"}],
                "threshold":{"line":{"color":RED,"width":3},"value":70}
            }))
        gauge.update_layout(paper_bgcolor=SURF,plot_bgcolor=BG,
                            font=dict(color=TEXT,family="Courier New"),
                            height=230,margin=dict(l=20,r=20,t=28,b=8))
        st.plotly_chart(gauge, use_container_width=True)

    st.markdown('<div class="sec-hdr">Stress Band Distribution — Stacked Monthly %</div>',
                unsafe_allow_html=True)
    fig2 = go.Figure()
    for col_name,band in [("pct_critical","CRITICAL"),("pct_high","HIGH"),
                          ("pct_moderate","MODERATE"),("pct_low","LOW")]:
        fig2.add_trace(go.Bar(x=platform_df["txn_month"],y=platform_df[col_name],
                              name=band,marker_color=BAND_C[band],opacity=0.85))
    fig2.update_layout(**blayout(240),barmode="stack",yaxis_title="% of Users")
    st.plotly_chart(fig2, use_container_width=True)

# ══ TAB 2: COHORTS ═══════════════════════════════════════════════════════════
with tab2:
    c_a, c_b = st.columns(2)
    with c_a:
        st.markdown('<div class="sec-hdr">Cohort Stress Trend Over Time</div>',
                    unsafe_allow_html=True)
        fig3 = go.Figure()
        for cohort in stress_df["cohort"].unique():
            cdf = stress_df[stress_df["cohort"]==cohort]
            fig3.add_trace(go.Scatter(x=cdf["txn_month"],y=cdf["avg_stress"],
                mode="lines",name=cohort,
                line=dict(color=COHORT_C.get(cohort,BLUE),width=2)))
        fig3.update_layout(**blayout(300)); fig3.update_yaxes(title_text="Avg Stress Score")
        st.plotly_chart(fig3, use_container_width=True)

    with c_b:
        st.markdown('<div class="sec-hdr">% At-Risk & Payday Loans by Cohort</div>',
                    unsafe_allow_html=True)
        latest = cohort_df.sort_values("txn_month").groupby("cohort").last().reset_index()
        fig4 = go.Figure()
        fig4.add_trace(go.Bar(x=latest["cohort"],y=latest["pct_at_risk"],name="% At Risk",
            marker_color=[COHORT_C.get(c,BLUE) for c in latest["cohort"]],opacity=0.85))
        fig4.add_trace(go.Bar(x=latest["cohort"],y=latest["pct_using_payday_loans"],
            name="% Payday Loans",marker_color=[COHORT_C.get(c,BLUE)+"88" for c in latest["cohort"]],opacity=0.7))
        fig4.update_layout(**blayout(300),barmode="group",yaxis_title="%")
        st.plotly_chart(fig4, use_container_width=True)

    st.markdown('<div class="sec-hdr">Score Component Radar — Select Cohort</div>',
                unsafe_allow_html=True)
    sel = st.selectbox("Cohort", components_df["cohort"].tolist())
    row = components_df[components_df["cohort"]==sel].iloc[0]
    cats = ["Spend Pressure","Income Irregular.","High-Risk Exp.",
            "Income Shock","Payday Dep.","ATM Dep.","Impulse Spend"]
    vals = [row.spend_pressure,row.income_irregular,row.high_risk_exposure,
            row.income_shock,row.payday_dependency,row.atm_dependency,row.impulse_spend]
    fig5 = go.Figure(go.Scatterpolar(
        r=vals+[vals[0]], theta=cats+[cats[0]],
        fill="toself", line=dict(color=COHORT_C.get(sel,AMBER),width=2),
        fillcolor=COHORT_C.get(sel,AMBER)+"30"))
    fig5.update_layout(
        polar=dict(bgcolor=BG,
                   radialaxis=dict(visible=True,range=[0,1],color=TEXT,gridcolor=GRID,tickfont=dict(size=8)),
                   angularaxis=dict(color=TEXT,gridcolor=GRID)),
        paper_bgcolor=SURF,font=dict(color=TEXT,family="Courier New",size=10),
        height=360,margin=dict(l=60,r=60,t=36,b=36),showlegend=False)
    c_r1,c_r2,c_r3 = st.columns([1,2,1])
    with c_r2: st.plotly_chart(fig5, use_container_width=True)

# ══ TAB 3: RISK SIGNALS ══════════════════════════════════════════════════════
with tab3:
    st.markdown('<div class="sec-hdr">Score Component Weights</div>', unsafe_allow_html=True)
    w_cols = st.columns(7)
    for col,(label,w,color) in zip(w_cols,[
        ("Spend Pressure","25%",RED),("Income Irregularity","20%",ORANGE),
        ("Income Shock","20%",RED),("High-Risk Exposure","15%",AMBER),
        ("Payday Dep.","10%",ORANGE),("ATM Cash-Out","5%",TEXT),("Impulse Spend","5%",TEXT)]):
        with col:
            st.markdown(f"""<div class="kpi-card" style="border-top-color:{color}">
              <div class="kpi-value" style="color:{color};font-size:20px">{w}</div>
              <div class="kpi-sub">{label}</div></div>""", unsafe_allow_html=True)

    st.markdown('<div class="sec-hdr">Signal Breakdown by Cohort — Stacked Components</div>',
                unsafe_allow_html=True)
    comp_colors = {"high_risk_exposure":RED,"income_irregular":ORANGE,
                   "payday_dependency":AMBER,"atm_dependency":BLUE,"impulse_spend":TEAL}
    fig6 = go.Figure()
    for comp,color in comp_colors.items():
        fig6.add_trace(go.Bar(x=components_df["cohort"],y=components_df[comp],
            name=comp.replace("_"," ").title(),marker_color=color,opacity=0.85))
    fig6.update_layout(**blayout(280),barmode="stack",yaxis_title="Score Contribution")
    st.plotly_chart(fig6, use_container_width=True)

    st.markdown('<div class="sec-hdr">Core SQL — Window Functions</div>', unsafe_allow_html=True)
    c1,c2 = st.columns(2)
    with c1:
        st.markdown("**Income Irregularity — `STDDEV() OVER` + `LAG()`**")
        st.code("""-- 3-month rolling income volatility
STDDEV(monthly_income) OVER (
  PARTITION BY user_id
  ORDER BY     txn_month
  ROWS BETWEEN 2 PRECEDING
           AND CURRENT ROW
) AS income_stddev_3m,

-- Month-over-month income change
monthly_income
- LAG(monthly_income, 1) OVER (
    PARTITION BY user_id
    ORDER BY     txn_month
) AS income_mom_delta,

-- Income Coefficient of Variation
ROUND(income_stddev_3m
  / NULLIF(income_avg_3m, 0)
, 4) AS income_cv""", language="sql")

    with c2:
        st.markdown("**Spend Velocity — Rolling Windows + Acceleration**")
        st.code("""-- 7-day rolling spend per user
SUM(amount) OVER (
  PARTITION BY user_id
  ORDER BY     transaction_date
  ROWS BETWEEN 6 PRECEDING
           AND CURRENT ROW
) AS rolling_7d_spend,

-- 30-day rolling spend
SUM(amount) OVER (
  PARTITION BY user_id
  ORDER BY     transaction_date
  ROWS BETWEEN 29 PRECEDING
           AND CURRENT ROW
) AS rolling_30d_spend,

-- Spend acceleration (LAG on rolling sum)
rolling_7d_spend
- LAG(rolling_7d_spend, 7) OVER (
    PARTITION BY user_id
    ORDER BY     transaction_date
) AS spend_acceleration_7d""", language="sql")

# ══ TAB 4: INTERVENTION ══════════════════════════════════════════════════════
with tab4:
    st.markdown('<div class="sec-hdr">Top Intervention Candidates — mart_user_risk_profile</div>',
                unsafe_allow_html=True)
    st.caption("Ranked by: current stress (50%) + stress persistence (30%) + payday dependency (20%)")

    filtered = risk_df[
        risk_df["current_stress_band"].isin(band_filter) &
        risk_df["cohort"].isin(cohort_filter)
    ].head(top_n)

    # Scatter
    c_scatter, c_table = st.columns([1,2])
    with c_scatter:
        st.markdown('<div class="sec-hdr">Stress vs Priority</div>', unsafe_allow_html=True)
        fig7 = px.scatter(filtered, x="current_stress_score", y="intervention_priority",
            color="current_stress_band", size="total_payday_loans",
            color_discrete_map=BAND_C,
            hover_data=["user_id","cohort","state"],
            labels={"current_stress_score":"Current Stress","intervention_priority":"Priority"})
        fig7.update_layout(**blayout(320))
        st.plotly_chart(fig7, use_container_width=True)

    with c_table:
        st.markdown('<div class="sec-hdr">Priority Table</div>', unsafe_allow_html=True)
        display = filtered[["user_id","cohort","state","current_stress_score",
                             "current_stress_band","intervention_priority",
                             "spend_to_income_ratio","total_payday_loans",
                             "stress_persistence_rate"]].rename(columns={
            "current_stress_score":"stress","current_stress_band":"band",
            "intervention_priority":"priority","spend_to_income_ratio":"spend/inc",
            "total_payday_loans":"payday","stress_persistence_rate":"persistence"})
        BAND_COLOR_MAP = {"CRITICAL":"#e74c3c","HIGH":"#e67e22","MODERATE":"#f5a623","LOW":"#27ae60"}

        def color_band(val):
            c = BAND_COLOR_MAP.get(val,"white")
            return f"color:{c};font-weight:bold"
        def color_score(val):
            if isinstance(val,float):
                if val>=0.7: return "color:#e74c3c"
                if val>=0.5: return "color:#e67e22"
                if val>=0.3: return "color:#f5a623"
                return "color:#27ae60"
            return ""

        styled = (display.style
            .applymap(color_band, subset=["band"])
            .applymap(color_score, subset=["stress","priority"])
            .format({"stress":"{:.3f}","priority":"{:.3f}",
                     "spend/inc":"{:.2f}","persistence":"{:.1%}"})
            .set_properties(**{"font-family":"Courier New","font-size":"11px"}))
        st.dataframe(styled, use_container_width=True, height=310)

    # Persistence bar
    st.markdown('<div class="sec-hdr">Stress Persistence Rate by Cohort</div>',
                unsafe_allow_html=True)
    persist = risk_df.groupby("cohort")["stress_persistence_rate"].mean().reset_index()
    fig8 = go.Figure(go.Bar(
        x=persist["cohort"], y=persist["stress_persistence_rate"],
        marker_color=[COHORT_C.get(c,BLUE) for c in persist["cohort"]], opacity=0.85,
        text=[f"{v:.1%}" for v in persist["stress_persistence_rate"]],
        textposition="outside", textfont=dict(color=TEXT,size=10)))
    fig8.update_layout(**blayout(240),yaxis_title="Avg Persistence Rate",yaxis_tickformat=".0%")
    st.plotly_chart(fig8, use_container_width=True)

    # Pipeline Architecture
    st.markdown('<div class="sec-hdr">Pipeline Architecture</div>', unsafe_allow_html=True)
    arch_cols = st.columns(5)
    arch = [
        ("RAW SOURCE","raw_transactions\nraw_users","#3498db"),
        ("STAGING","stg_transactions\nstg_users","#1abc9c"),
        ("INTERMEDIATE","int_income_signals\nint_stress_scores\nint_spend_velocity","#f5a623"),
        ("MARTS","mart_cohort_health\nmart_user_risk_profile\nmart_kpi_summary","#e67e22"),
        ("DASHBOARD","Streamlit\nPlotly\nMetabase-ready","#e74c3c"),
    ]
    for col,(label,items,color) in zip(arch_cols, arch):
        with col:
            lines = "".join(f"<div style='font-family:Courier New;font-size:9px;color:#4a5068;margin-top:3px'>· {l}</div>"
                            for l in items.split("\n"))
            st.markdown(f"""<div style="background:#13151f;border:1px solid {color}44;
              border-top:2px solid {color};border-radius:3px;padding:10px 12px">
              <div style="font-family:Courier New;font-size:10px;color:{color};font-weight:700">{label}</div>
              {lines}</div>""", unsafe_allow_html=True)
