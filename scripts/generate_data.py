"""
Smart Money Pulse — Synthetic Transaction Data Generator
Generates 47K+ realistic behavioral finance transactions with embedded stress signals
"""
import duckdb, pandas as pd, numpy as np
from datetime import datetime, timedelta
import random, os

random.seed(42); np.random.seed(42)

N_USERS = 500; N_TRANSACTIONS = 50000
START_DATE = datetime(2023, 1, 1); END_DATE = datetime(2025, 1, 1)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/smart_money.duckdb")

CATEGORIES = {
    "groceries":      {"merchants":["Whole Foods","Kroger","Aldi","Trader Joe's"],"avg":85,"std":35},
    "dining":         {"merchants":["McDonald's","Chipotle","Starbucks","Local Bistro"],"avg":28,"std":18},
    "utilities":      {"merchants":["Electric Co","Gas Co","Water Dept","Internet ISP"],"avg":120,"std":30},
    "rent":           {"merchants":["Apt Management LLC","Realty Corp"],"avg":1400,"std":400},
    "healthcare":     {"merchants":["CVS Pharmacy","Walgreens","City Hospital"],"avg":75,"std":120},
    "entertainment":  {"merchants":["Netflix","Spotify","AMC Theaters","Steam"],"avg":22,"std":15},
    "fuel":           {"merchants":["Shell","BP","Chevron","ExxonMobil"],"avg":55,"std":20},
    "retail":         {"merchants":["Amazon","Target","Walmart","Best Buy"],"avg":95,"std":70},
    "atm_cash":       {"merchants":["ATM Withdrawal","Cash Advance"],"avg":160,"std":100},
    "alcohol_tobacco":{"merchants":["Total Wine","BevMo","Local Bar"],"avg":35,"std":25},
    "gambling":       {"merchants":["DraftKings","Casino Online","Lottery"],"avg":80,"std":120},
    "payday_loan":    {"merchants":["QuickCash","SpeedyLoan","CashNow"],"avg":300,"std":150},
    "transfer_in":    {"merchants":["Zelle Deposit","Venmo","ACH Payroll"],"avg":2200,"std":800},
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

def generate_users():
    names = list(COHORTS.keys())
    weights = [COHORTS[c]["weight"] for c in names]
    assigned = np.random.choice(names, size=N_USERS, p=weights)
    users = []
    for i in range(N_USERS):
        c = assigned[i]; cohort = COHORTS[c]
        users.append({
            "user_id": f"USR_{i+1:05d}", "cohort": c,
            "state": random.choice(STATES), "age": int(np.random.normal(38,12)),
            "avg_monthly_income": cohort["avg_income"] * np.random.uniform(0.7,1.3),
            "income_stability": (1 - cohort["stress_base"]) * np.random.uniform(0.85,1.0),
            "stress_score_base": min(1.0, cohort["stress_base"] * np.random.uniform(0.8,1.2)),
            "high_risk_cat_prob": cohort["risk_prob"],
            "created_at": START_DATE + timedelta(days=random.randint(0,30)),
        })
    return pd.DataFrame(users)

def generate_transactions(users_df):
    txns, txn_id = [], 1
    total_days = (END_DATE - START_DATE).days
    counts = np.clip(np.random.negative_binomial(5, 0.05, N_USERS), 30, 300)
    all_cats = list(CATEGORIES.keys())
    for _, user in users_df.iterrows():
        uid = user["user_id"]; n = counts[int(uid.split("_")[1])-1]
        stress_periods = []
        if user["stress_score_base"] > 0.5:
            for _ in range(random.randint(1,4)):
                s = random.randint(0, total_days-30)
                stress_periods.append((s, s+random.randint(7,30)))
        for _ in range(n):
            txn_date = START_DATE + timedelta(days=random.randint(0,total_days-1))
            day_off = (txn_date - START_DATE).days
            in_stress = any(s<=day_off<=e for s,e in stress_periods)
            mult = np.random.uniform(1.4,2.2) if in_stress else 1.0
            if in_stress and random.random()<0.4:
                cat = random.choice(["atm_cash","alcohol_tobacco","gambling","payday_loan"])
            elif random.random() < user["high_risk_cat_prob"]:
                cat = random.choice(["atm_cash","alcohol_tobacco","gambling","payday_loan","dining"])
            else:
                cat = random.choice(["groceries","dining","utilities","rent","healthcare","entertainment","fuel","retail","transfer_in"])
            cat_info = CATEGORIES[cat]
            is_credit = cat == "transfer_in"
            amount = max(0.50, round(abs(np.random.normal(cat_info["avg"], cat_info["std"]) * mult), 2))
            txns.append({
                "transaction_id": f"TXN_{txn_id:07d}", "user_id": uid,
                "transaction_date": txn_date.date(), "merchant_name": random.choice(cat_info["merchants"]),
                "category": cat, "amount": amount,
                "transaction_type": "credit" if is_credit else "debit",
                "is_stress_period": in_stress,
                "channel": random.choice(["card_present","online","atm","ach"]),
            })
            txn_id += 1
    return pd.DataFrame(txns).sample(frac=1, random_state=42).reset_index(drop=True)

def load_to_duckdb(users_df, txns_df):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
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
    tc = con.execute("SELECT COUNT(*) FROM raw_transactions").fetchone()[0]
    uc = con.execute("SELECT COUNT(*) FROM raw_users").fetchone()[0]
    print(f"✅ {uc:,} users · {tc:,} transactions loaded into DuckDB")
    con.close()

if __name__ == "__main__":
    print("🔄 Generating users..."); users_df = generate_users()
    print("🔄 Generating transactions..."); txns_df = generate_transactions(users_df)
    print("🔄 Loading to DuckDB..."); load_to_duckdb(users_df, txns_df)
    users_df.to_csv("/home/claude/smart_money_pulse/data/users_seed.csv", index=False)
    print("✅ Done")
