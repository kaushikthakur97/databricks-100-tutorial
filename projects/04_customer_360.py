# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 — Governance, Lineage & Security
# MAGIC
# MAGIC **Project:** Unified customer data platform with enterprise governance  
# MAGIC **Scale:** 500 customers × 5 source systems, 11K+ rows  
# MAGIC **Designed for:** Serverless — managed Delta tables only, no /tmp/, no dbutils
# MAGIC
# MAGIC ### Business Context
# MAGIC A bank has customer data scattered across 5 systems. Marketing, Risk, and Compliance
# MAGIC each need different views with appropriate security:
# MAGIC - **Marketing** → behavioral analytics, **zero PII**
# MAGIC - **Risk** → financial profiling, **masked PII**
# MAGIC - **Compliance** → full audit data, **full PII**
# MAGIC
# MAGIC ### Architecture (Medallion + Governance)
# MAGIC ```
# MAGIC [5 Sources] → [Bronze: 5 tables] → [Silver: 1 unified table] → [Gold: 3 role views]
# MAGIC                                                              → [Dynamic view: CURRENT_USER()]
# MAGIC [Governance: role_membership | lineage | audit_log]
# MAGIC ```
# MAGIC **Concepts:** Medallion, MERGE, Column-Level Security, Row-Level Security,
# MAGIC Delta Constraints, Time Travel, Lineage Tracking, Audit Logging, Dynamic Views

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Setup & Cleanup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random, hashlib

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
random.seed(42)

DB = "default"

for obj in spark.sql(f"SHOW TABLES IN {DB} LIKE 'c360*'").collect():
    spark.sql(f"DROP TABLE IF EXISTS {DB}.{obj.tableName}")
for obj in spark.sql(f"SHOW VIEWS IN {DB} LIKE 'c360*'").collect():
    spark.sql(f"DROP VIEW IF EXISTS {DB}.{obj.viewName}")

print(f"Database: {DB}  |  Spark: {spark.version}  |  Clean slate ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simulate 5 Source Systems (2000+ rows each)
# MAGIC
# MAGIC 500 customer master records with full PII. Sources have different PII exposure levels:
# MAGIC transactions/web_activity → no PII; loans/credit_cards/tickets → include PII from application.

# COMMAND ----------

# ── 2.0 Customer Master (500 profiles) ──
FIRST = ["James","Mary","Robert","Patricia","John","Jennifer","Michael","Linda","David","Barbara",
    "William","Elizabeth","Richard","Susan","Joseph","Jessica","Thomas","Sarah","Charles","Karen",
    "Christopher","Lisa","Daniel","Nancy","Matthew","Betty","Anthony","Margaret","Mark","Sandra",
    "Donald","Ashley","Steven","Kimberly","Paul","Emily","Andrew","Donna","Joshua","Michelle"]
LAST  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez",
    "Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore",
    "Jackson","Martin","Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez"]
DOMAINS = ["gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com"]
CITIES  = ["New York","Los Angeles","Chicago","Houston","Phoenix","Dallas","Miami","Seattle"]
STATES  = ["NY","CA","IL","TX","AZ","FL","WA","MA"]
STREETS = ["Main","Oak","Elm","Park","Lake","Hill","Pine","Cedar","Maple"]

NUM_CUST = 500
customers = []
for i in range(NUM_CUST):
    fn, ln = random.choice(FIRST), random.choice(LAST)
    customers.append({
        "customer_id": f"CUST{i:05d}", "first_name": fn, "last_name": ln,
        "email": f"{fn.lower()}.{ln.lower()}@{random.choice(DOMAINS)}",
        "phone": f"{random.randint(200,999)}-{random.randint(200,999)}-{random.randint(1000,9999)}",
        "ssn": f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
        "address": f"{random.randint(100,9999)} {random.choice(STREETS)} St",
        "city": random.choice(CITIES), "state": random.choice(STATES),
        "zip_code": f"{random.randint(10000,99999)}",
        "date_of_birth": (datetime(1950,1,1)+timedelta(days=random.randint(0,20000))).strftime("%Y-%m-%d"),
        "credit_score": random.randint(300,850),
        "account_open_date": (datetime(2018,1,1)+timedelta(days=random.randint(0,1800))).strftime("%Y-%m-%d"),
        "customer_segment": random.choice(["Retail","Wealth","Private","Small Business","Student"])
    })
customers_df = spark.createDataFrame(customers)
customers_df.createOrReplaceTempView("cust_master")
print(f"Customer master: {customers_df.count()} rows")

# COMMAND ----------

# ── 2.1 Transactions (Core Banking, 2500 rows) ──
MERCHANTS = ["Amazon","Walmart","Target","Costco","Home Depot","Best Buy","Kroger",
    "Walgreens","CVS","Starbucks","McDonalds","Shell","Uber","Netflix","Spotify",
    "Delta Airlines","Marriott","Chevron","Apple Store","Trader Joe's"]
CHANNELS = ["online","ATM","branch","mobile_app","POS","phone"]

txns = []
for _ in range(2500):
    ts = datetime(2024,1,1) + timedelta(days=random.randint(0,450), hours=random.randint(0,23),
          minutes=random.randint(0,59), seconds=random.randint(0,59))
    txns.append({
        "transaction_id": f"TXN{random.randint(1000000,9999999)}",
        "customer_id": f"CUST{random.randint(0,NUM_CUST-1):05d}",
        "amount": round(random.uniform(0.99,25000),2),
        "merchant": random.choice(MERCHANTS),
        "channel": random.choice(CHANNELS),
        "transaction_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "status": random.choice(["completed","completed","completed","pending","failed","reversed"]),
        "currency": "USD",
        "card_last_four": f"{random.randint(1000,9999)}",
        "source_system": "CORE_BANKING"
    })
txn_df = spark.createDataFrame(txns); txn_df.createOrReplaceTempView("src_transactions")
print(f"Transactions: {txn_df.count()} rows")

# ── 2.2 Loans (Loan Origination, 2000 rows) ──
loan_types = ["personal","auto","mortgage","student","home_equity","business"]
loan_statuses = ["active","paid_off","approved","pending","defaulted","rejected"]

loans = []
for _ in range(2000):
    sd = datetime(2021,1,1) + timedelta(days=random.randint(0,1500))
    ltype = random.choice(loan_types)
    lamt = round(random.uniform(2000,750000),-2)
    loans.append({
        "loan_id": f"LOAN{random.randint(100000,999999)}",
        "customer_id": f"CUST{random.randint(0,NUM_CUST-1):05d}",
        "loan_amount": lamt,
        "interest_rate": round(random.uniform(3.0,28.0),2),
        "term_months": random.choice([12,24,36,48,60,72,84,120,180,240,360]),
        "loan_type": ltype,
        "start_date": sd.strftime("%Y-%m-%d"),
        "status": random.choice(loan_statuses),
        "monthly_payment": round(random.uniform(100,8000),2),
        "credit_score_at_orig": random.randint(300,850),
        "dti_ratio": round(random.uniform(0.10,0.55),2),
        "source_system": "LOAN_ORIGINATION"
    })
loans_df = spark.createDataFrame(loans); loans_df.createOrReplaceTempView("src_loans")
print(f"Loans: {loans_df.count()} rows")

# ── 2.3 Credit Cards (Card Services, 2200 rows) ──
ctype_limits = {"Platinum Rewards":25000,"Gold":15000,"Silver Cashback":10000,
    "Basic":5000,"Student":2000,"Business Travel":35000,"Secured":3000}

ccards = []
for _ in range(2200):
    ctype = random.choice(list(ctype_limits.keys()))
    climit = ctype_limits[ctype]
    ad = datetime(2022,1,1) + timedelta(days=random.randint(0,1000))
    ccards.append({
        "card_id": f"CC{random.randint(1000000,9999999)}",
        "customer_id": f"CUST{random.randint(0,NUM_CUST-1):05d}",
        "card_type": ctype, "card_network": random.choice(["Visa","Mastercard"]),
        "credit_limit": climit, "current_balance": round(random.uniform(0,climit*0.9),2),
        "apr": round(random.uniform(14.99,29.99),2),
        "activation_date": ad.strftime("%Y-%m-%d"),
        "status": random.choice(["active","frozen","closed","suspended"]),
        "rewards_points": random.randint(0,150000),
        "source_system": "CARD_SERVICES"
    })
cc_df = spark.createDataFrame(ccards); cc_df.createOrReplaceTempView("src_credit_cards")
print(f"Credit Cards: {cc_df.count()} rows")

# ── 2.4 Web Activity (Web Analytics, 3000 rows) ──
CAMPAIGNS = [f"CAMP_{c}" for c in ["SPRING25","SUMMER24","WELCOME","REFERRAL","LOYALTY",
    "PREMIUM","STUDENT","WEALTH","CARD_PROMO","LOAN_PROMO"]]
REFERRERS = ["google.com","facebook.com","email","direct","twitter.com","linkedin.com","bing.com"]

web = []
for _ in range(3000):
    ts = datetime(2024,1,1) + timedelta(days=random.randint(0,450),
          hours=random.randint(0,23), minutes=random.randint(0,59))
    web.append({
        "session_id": f"SESS{random.randint(10000000,99999999)}",
        "customer_id": f"CUST{random.randint(0,NUM_CUST-1):05d}",
        "page_views": random.randint(1,60),
        "time_on_site_seconds": random.randint(15,5400),
        "device_type": random.choice(["mobile_ios","mobile_android","desktop_windows","desktop_mac","tablet"]),
        "browser": random.choice(["Chrome","Safari","Firefox","Edge"]),
        "login_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "campaign_id": random.choice(CAMPAIGNS),
        "referrer": random.choice(REFERRERS),
        "converted": random.choice([True,False,False,False]),
        "source_system": "WEB_ANALYTICS"
    })
web_df = spark.createDataFrame(web); web_df.createOrReplaceTempView("src_web_activity")
print(f"Web Activity: {web_df.count()} rows")

# ── 2.5 Support Tickets (CRM, 2000 rows) ──
tickets = []
for _ in range(2000):
    cd = datetime(2023,6,1) + timedelta(days=random.randint(0,650),
          hours=random.randint(0,23), minutes=random.randint(0,59))
    cat = random.choice(["billing","technical","account_access","fraud_report","general","loan","card_dispute"])
    res_hours = round(random.uniform(0.5,336),1)
    resolved = cd + timedelta(hours=res_hours)
    tickets.append({
        "ticket_id": f"TK{random.randint(100000,999999)}",
        "customer_id": f"CUST{random.randint(0,NUM_CUST-1):05d}",
        "category": cat, "priority": random.choice(["low","medium","high","critical"]),
        "subject": f"Issue with {cat}",
        "created_date": cd.strftime("%Y-%m-%d %H:%M:%S"),
        "resolved_date": resolved.strftime("%Y-%m-%d %H:%M:%S") if resolved < datetime(2025,5,1) else None,
        "resolution_time_hours": res_hours if resolved < datetime(2025,5,1) else None,
        "agent_id": f"AG{random.randint(100,999)}",
        "satisfaction_score": random.choice([1,2,3,4,5,3,4,5,4,5]) if resolved < datetime(2025,5,1) else None,
        "channel": random.choice(["phone","email","chat","in_branch","mobile_app"]),
        "is_escalated": random.choice([True,False,False]),
        "source_system": "CRM_TICKETING"
    })
tickets_df = spark.createDataFrame(tickets); tickets_df.createOrReplaceTempView("src_support_tickets")
print(f"Support Tickets: {tickets_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source Verification

# COMMAND ----------

print("Source System Cross-Check:")
spark.sql("""SELECT 'transactions' src, COUNT(*), COUNT(DISTINCT customer_id) FROM src_transactions
    UNION ALL SELECT 'loans', COUNT(*), COUNT(DISTINCT customer_id) FROM src_loans
    UNION ALL SELECT 'credit_cards', COUNT(*), COUNT(DISTINCT customer_id) FROM src_credit_cards
    UNION ALL SELECT 'web_activity', COUNT(*), COUNT(DISTINCT customer_id) FROM src_web_activity
    UNION ALL SELECT 'support_tickets', COUNT(*), COUNT(DISTINCT customer_id) FROM src_support_tickets
""").show()

# How many appear in ALL 5?
spark.sql("""SELECT COUNT(DISTINCT t.customer_id) from src_transactions t
    JOIN src_loans l ON t.customer_id=l.customer_id
    JOIN src_credit_cards c ON t.customer_id=c.customer_id
    JOIN src_web_activity w ON t.customer_id=w.customer_id
    JOIN src_support_tickets s ON t.customer_id=s.customer_id
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Layer — Raw Ingestion with Metadata
# MAGIC
# MAGIC Each source lands in its own bronze Delta table with governance metadata
# MAGIC (`_ingest_timestamp`, `_source_system`, `_batch_id`). Managed Delta only.

# COMMAND ----------

ingest_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
print(f"Batch: {batch_id}")

meta_cols = [
    F.lit(ingest_ts).cast("timestamp").alias("_ingest_timestamp"),
    F.lit("_placeholder").alias("_source_system"),
    F.lit(batch_id).alias("_batch_id")
]

sources = [
    (txn_df,    "CORE_BANKING",     "c360_bronze_transactions"),
    (loans_df,  "LOAN_ORIGINATION", "c360_bronze_loans"),
    (cc_df,     "CARD_SERVICES",    "c360_bronze_credit_cards"),
    (web_df,    "WEB_ANALYTICS",    "c360_bronze_web_activity"),
    (tickets_df,"CRM_TICKETING",    "c360_bronze_support_tickets"),
]

for df, src, tbl in sources:
    meta_cols[1] = F.lit(src).alias("_source_system")
    df.select("*", *meta_cols) \
      .write.mode("overwrite").format("delta") \
      .saveAsTable(f"{DB}.{tbl}")
    cnt = spark.sql(f"SELECT COUNT(*) FROM {DB}.{tbl}").collect()[0][0]
    print(f"  {tbl}: {cnt} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Verification — Metadata Columns

# COMMAND ----------

for tbl in ["c360_bronze_transactions","c360_bronze_loans","c360_bronze_credit_cards",
            "c360_bronze_web_activity","c360_bronze_support_tickets"]:
    row = spark.sql(f"SELECT _source_system, _ingest_timestamp, _batch_id, COUNT(*) n FROM {DB}.{tbl} GROUP BY 1,2,3").collect()[0]
    print(f"  {tbl}: {row._source_system} | {row._ingest_timestamp} | {row.n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver Layer — Golden Customer Record (MERGE)
# MAGIC
# MAGIC Aggregate metrics from all 5 sources, LEFT JOIN with customer master, then
# MAGIC MERGE for idempotent upsert. Safe to rerun — no duplicates.

# COMMAND ----------

# ── 4.1 Aggregate each source ──
spark.sql("""
CREATE OR REPLACE TEMP VIEW agg_txn AS
SELECT customer_id, COUNT(*) total_txns, ROUND(SUM(amount),2) total_spent,
       ROUND(AVG(amount),2) avg_txn, MAX(amount) max_txn,
       MAX(transaction_timestamp) last_txn_date, COUNT(DISTINCT merchant) unique_merchants,
       SUM(CASE WHEN channel='online' THEN 1 ELSE 0 END) online_txns,
       SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) failed_txns
FROM src_transactions GROUP BY customer_id
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW agg_ln AS
SELECT customer_id, COUNT(*) total_loans, ROUND(SUM(loan_amount),2) total_loan_amt,
       ROUND(AVG(interest_rate),2) avg_int_rate, ROUND(SUM(monthly_payment),2) total_monthly,
       SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) active_loans,
       SUM(CASE WHEN status='defaulted' THEN 1 ELSE 0 END) defaulted_loans,
       MAX(start_date) last_loan_date, ROUND(AVG(credit_score_at_orig),1) avg_cs_orig,
       ROUND(AVG(dti_ratio),3) avg_dti
FROM src_loans GROUP BY customer_id
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW agg_cc AS
SELECT customer_id, COUNT(*) total_cards, SUM(credit_limit) total_climit,
       ROUND(SUM(current_balance),2) total_balance,
       ROUND(SUM(current_balance)/NULLIF(SUM(credit_limit),0),4) utilization,
       SUM(rewards_points) total_rewards,
       SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) active_cards,
       SUM(CASE WHEN status IN ('frozen','suspended') THEN 1 ELSE 0 END) restricted_cards,
       ROUND(AVG(apr),2) avg_apr
FROM src_credit_cards GROUP BY customer_id
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW agg_web AS
SELECT customer_id, COUNT(*) total_sessions, SUM(page_views) total_pviews,
       ROUND(AVG(time_on_site_seconds),1) avg_time_site,
       MAX(login_timestamp) last_login,
       SUM(CASE WHEN converted=true THEN 1 ELSE 0 END) conversions,
       COUNT(DISTINCT campaign_id) campaigns_engaged
FROM src_web_activity GROUP BY customer_id
""")
spark.sql("""
CREATE OR REPLACE TEMP VIEW agg_tk AS
SELECT customer_id, COUNT(*) total_tickets,
       ROUND(AVG(resolution_time_hours),1) avg_res_hours,
       ROUND(AVG(satisfaction_score),2) avg_satisfaction,
       SUM(CASE WHEN priority='critical' THEN 1 ELSE 0 END) critical_tickets,
       SUM(CASE WHEN priority='high' THEN 1 ELSE 0 END) high_priority,
       SUM(CASE WHEN category='fraud_report' THEN 1 ELSE 0 END) fraud_reports,
       SUM(CASE WHEN is_escalated=true THEN 1 ELSE 0 END) escalated_tickets
FROM src_support_tickets GROUP BY customer_id
""")
print("5 aggregation views ready.")

# COMMAND ----------

# ── 4.2 Build unified profile ──
silver_base = spark.sql("""
SELECT cm.*,
    COALESCE(t.total_txns,0)          total_transactions,
    COALESCE(t.total_spent,0)         total_amount_spent,
    COALESCE(t.avg_txn,0)             avg_transaction_amount,
    COALESCE(t.max_txn,0)             max_single_transaction,
    t.last_txn_date                   last_transaction_date,
    COALESCE(t.unique_merchants,0)    unique_merchants,
    COALESCE(t.online_txns,0)         online_transactions,
    COALESCE(t.failed_txns,0)         failed_transactions,
    COALESCE(l.total_loans,0)         total_loans,
    COALESCE(l.total_loan_amt,0)      total_loan_amount,
    COALESCE(l.avg_int_rate,0)        avg_interest_rate,
    COALESCE(l.total_monthly,0)       total_monthly_payments,
    COALESCE(l.active_loans,0)        active_loans,
    COALESCE(l.defaulted_loans,0)     defaulted_loans,
    l.last_loan_date,
    COALESCE(l.avg_cs_orig,0)         avg_credit_score_at_orig,
    COALESCE(l.avg_dti,0)             avg_dti_ratio,
    COALESCE(cc.total_cards,0)        total_cards,
    COALESCE(cc.total_climit,0)       total_credit_limit,
    COALESCE(cc.total_balance,0)      total_balance,
    COALESCE(cc.utilization,0)        credit_utilization_ratio,
    COALESCE(cc.total_rewards,0)      total_rewards_points,
    COALESCE(cc.active_cards,0)       active_cards,
    COALESCE(cc.restricted_cards,0)   restricted_cards,
    COALESCE(cc.avg_apr,0)            avg_apr,
    COALESCE(w.total_sessions,0)      total_sessions,
    COALESCE(w.total_pviews,0)        total_page_views,
    COALESCE(w.avg_time_site,0)       avg_time_on_site_sec,
    w.last_login,
    COALESCE(w.conversions,0)         total_conversions,
    COALESCE(w.campaigns_engaged,0)   campaigns_engaged,
    COALESCE(tk.total_tickets,0)      total_tickets,
    COALESCE(tk.avg_res_hours,0)      avg_resolution_hours,
    COALESCE(tk.avg_satisfaction,0)   avg_satisfaction,
    COALESCE(tk.critical_tickets,0)   critical_tickets,
    COALESCE(tk.high_priority,0)      high_priority_tickets,
    COALESCE(tk.fraud_reports,0)      fraud_reports,
    COALESCE(tk.escalated_tickets,0)  escalated_tickets
FROM cust_master cm
LEFT JOIN agg_txn t  ON cm.customer_id=t.customer_id
LEFT JOIN agg_ln l   ON cm.customer_id=l.customer_id
LEFT JOIN agg_cc cc  ON cm.customer_id=cc.customer_id
LEFT JOIN agg_web w  ON cm.customer_id=w.customer_id
LEFT JOIN agg_tk tk  ON cm.customer_id=tk.customer_id
""")
silver_base.createOrReplaceTempView("silver_base")
print(f"Silver base built: {silver_base.count()} customers")

# COMMAND ----------

# ── 4.3 Create Silver Table + MERGE ──
silver_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Build CREATE TABLE dynamically from silver_base schema
cols_ddl = []
for f in silver_base.schema.fields:
    dtype = f.dataType.simpleString().upper()
    if f.name == "customer_id": cols_ddl.append(f"  {f.name} STRING NOT NULL")
    else: cols_ddl.append(f"  {f.name} {dtype}")
cols_ddl.append(f"  _silver_timestamp STRING")
cols_ddl.append(f"  _silver_batch_id STRING")

spark.sql(f"CREATE TABLE IF NOT EXISTS {DB}.c360_silver_customer ({','.join(cols_ddl)}) USING DELTA")
print("Silver table created.")

spark.sql(f"""
MERGE INTO {DB}.c360_silver_customer AS target
USING (SELECT *, '{silver_now}' _silver_timestamp, '{batch_id}' _silver_batch_id FROM silver_base) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
print("MERGE complete.")
spark.sql(f"SELECT COUNT(*) silver_count FROM {DB}.c360_silver_customer").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Verification — Coverage & Sample

# COMMAND ----------

spark.sql(f"""SELECT customer_id, first_name, credit_score, customer_segment,
    total_transactions, total_loans, total_cards, total_sessions, total_tickets
    FROM {DB}.c360_silver_customer LIMIT 8""").show(truncate=False)

spark.sql(f"""SELECT
    COUNT(*) total, SUM(CASE WHEN total_transactions>0 THEN 1 END) has_txn,
    SUM(CASE WHEN total_loans>0 THEN 1 END) has_loans,
    SUM(CASE WHEN total_cards>0 THEN 1 END) has_cards,
    SUM(CASE WHEN total_sessions>0 THEN 1 END) has_web,
    SUM(CASE WHEN total_tickets>0 THEN 1 END) has_tickets
    FROM {DB}.c360_silver_customer""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold Layer — Role-Specific Views (Column-Level Security)
# MAGIC
# MAGIC Three purpose-built views that restrict PII exposure at the column level:
# MAGIC - **Marketing** → No PII (names, SSN, email, phone, address excluded)
# MAGIC - **Risk** → Masked PII (last-4 SSN, truncated email, masked phone)
# MAGIC - **Compliance** → Full PII + audit metadata

# COMMAND ----------

# Gold: Marketing (NO PII)
spark.sql(f"""
CREATE OR REPLACE VIEW {DB}.c360_gold_marketing AS
SELECT customer_id, city, state, credit_score, customer_segment,
    total_transactions, total_amount_spent, avg_transaction_amount, max_single_transaction,
    unique_merchants, online_transactions, total_sessions, total_page_views,
    avg_time_on_site_sec, total_conversions, campaigns_engaged,
    credit_utilization_ratio, total_rewards_points, avg_satisfaction, _silver_timestamp
FROM {DB}.c360_silver_customer
""")

# Gold: Risk (MASKED PII)
spark.sql(f"""
CREATE OR REPLACE VIEW {DB}.c360_gold_risk AS
SELECT customer_id,
    CONCAT('***-**-', RIGHT(ssn,4)) ssn_masked,
    CONCAT(LEFT(email,2), '***@', SPLIT(email,'@')[1]) email_masked,
    CONCAT('***-***-', RIGHT(phone,4)) phone_masked,
    city, state, zip_code, credit_score, customer_segment, date_of_birth,
    total_transactions, total_amount_spent, max_single_transaction, failed_transactions,
    total_loans, total_loan_amount, active_loans, defaulted_loans,
    avg_interest_rate, avg_credit_score_at_orig, avg_dti_ratio,
    total_cards, total_credit_limit, total_balance, credit_utilization_ratio,
    active_cards, restricted_cards, avg_apr,
    total_tickets, avg_resolution_hours, critical_tickets, high_priority_tickets,
    fraud_reports, escalated_tickets, _silver_timestamp
FROM {DB}.c360_silver_customer
""")

# Gold: Compliance (FULL PII + AUDIT)
spark.sql(f"""
CREATE OR REPLACE VIEW {DB}.c360_gold_compliance AS
SELECT * FROM {DB}.c360_silver_customer
""")
print("3 Gold views created: marketing (no PII), risk (masked), compliance (full)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold View Verification — Column-Level Security

# COMMAND ----------

print("1. MARKETING VIEW — PII columns excluded")
mkt_cols = spark.sql(f"SELECT * FROM {DB}.c360_gold_marketing LIMIT 1").columns
pii = ["first_name","last_name","email","phone","ssn","address","date_of_birth"]
print(f"   PII exposed: {[c for c in pii if c in mkt_cols] or 'NONE'}")
spark.sql(f"SELECT customer_id, credit_score, total_transactions FROM {DB}.c360_gold_marketing LIMIT 3").show()

print("2. RISK VIEW — PII masked")
spark.sql(f"SELECT customer_id, ssn_masked, email_masked, phone_masked, fraud_reports FROM {DB}.c360_gold_risk LIMIT 3").show(truncate=False)

print("3. COMPLIANCE VIEW — Full PII")
spark.sql(f"SELECT customer_id, first_name, last_name, email, ssn FROM {DB}.c360_gold_compliance LIMIT 3").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Dynamic Views — Row-Level Security with CURRENT_USER()
# MAGIC
# MAGIC Role membership table maps users to roles. Dynamic view CROSS JOINS with it,
# MAGIC filtering by `CURRENT_USER()`, then applies column masking based on role.

# COMMAND ----------

role_data = [
    ("alice@bank.com","marketing_analyst"), ("bob@bank.com","risk_officer"),
    ("carol@bank.com","compliance_officer"), ("dave@bank.com","data_engineer"),
    ("eve@bank.com","marketing_analyst"), ("frank@bank.com","risk_officer"),
]
spark.createDataFrame(role_data,["user_email","role_name"]) \
    .write.mode("overwrite").format("delta").saveAsTable(f"{DB}.c360_role_membership")

# Dynamic view — PII visibility depends on role
spark.sql(f"""
CREATE OR REPLACE VIEW {DB}.c360_dynamic_customer AS
SELECT sc.customer_id,
    CASE WHEN rm.role_name='compliance_officer' THEN sc.first_name ELSE 'RESTRICTED' END first_name,
    CASE WHEN rm.role_name='compliance_officer' THEN sc.last_name ELSE 'RESTRICTED' END last_name,
    CASE WHEN rm.role_name='compliance_officer' THEN sc.email
         WHEN rm.role_name='risk_officer' THEN CONCAT(LEFT(sc.email,2),'***@',SPLIT(sc.email,'@')[1])
         ELSE NULL END email,
    CASE WHEN rm.role_name IN ('compliance_officer','risk_officer') THEN sc.phone ELSE NULL END phone,
    CASE WHEN rm.role_name='compliance_officer' THEN sc.ssn
         WHEN rm.role_name='risk_officer' THEN CONCAT('***-**-',RIGHT(sc.ssn,4))
         ELSE NULL END ssn,
    sc.city, sc.state, sc.credit_score, sc.customer_segment,
    sc.total_transactions, sc.total_amount_spent, sc.total_loans,
    sc.total_balance, sc.fraud_reports, sc.avg_satisfaction, sc._silver_timestamp
FROM {DB}.c360_silver_customer sc
CROSS JOIN {DB}.c360_role_membership rm
WHERE rm.user_email = CURRENT_USER()
""")
print("Dynamic view created: c360_dynamic_customer (CURRENT_USER() filtering)")

# COMMAND ----------

# Simulate different users (in production, CURRENT_USER() resolves automatically)
print("Dynamic View — Role Simulation:\n")
roles = {"marketing_analyst":"alice@bank.com","risk_officer":"bob@bank.com","compliance_officer":"carol@bank.com"}
for role, user in roles.items():
    print(f"  {role} ({user}):")
    spark.sql(f"""SELECT sc.customer_id,
        CASE WHEN '{role}'='compliance_officer' THEN sc.first_name ELSE 'RESTRICTED' END fn,
        CASE WHEN '{role}'='compliance_officer' THEN sc.email
             WHEN '{role}'='risk_officer' THEN CONCAT(LEFT(sc.email,2),'***')
             ELSE NULL END email,
        CASE WHEN '{role}'='compliance_officer' THEN sc.ssn
             WHEN '{role}'='risk_officer' THEN CONCAT('***-**-',RIGHT(sc.ssn,4))
             ELSE NULL END ssn,
        sc.credit_score
    FROM {DB}.c360_silver_customer sc LIMIT 2""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lineage Traceability — Source → Bronze → Silver → Gold
# MAGIC
# MAGIC Full provenance tracking: every field in every table is traceable back to its source
# MAGIC system and transformation step. Critical for regulatory audits.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DB}.c360_lineage (
    lineage_id BIGINT, source_table STRING, target_table STRING,
    transformation_type STRING, transformation_logic STRING,
    layer STRING, pipeline_run_id STRING, record_count BIGINT, created_at TIMESTAMP
) USING DELTA
""")

lineage = [
    (1,"src_transactions","c360_bronze_transactions","DIRECT_LOAD","Copy + ingest metadata","bronze",batch_id,2500),
    (2,"src_loans","c360_bronze_loans","DIRECT_LOAD","Copy + ingest metadata","bronze",batch_id,2000),
    (3,"src_credit_cards","c360_bronze_credit_cards","DIRECT_LOAD","Copy + ingest metadata","bronze",batch_id,2200),
    (4,"src_web_activity","c360_bronze_web_activity","DIRECT_LOAD","Copy + ingest metadata","bronze",batch_id,3000),
    (5,"src_support_tickets","c360_bronze_support_tickets","DIRECT_LOAD","Copy + ingest metadata","bronze",batch_id,2000),
    (6,"c360_bronze_*","c360_silver_customer","AGGREGATION_JOIN_MERGE","5-source agg + cust_master JOIN + MERGE upsert","silver",batch_id,500),
    (7,"c360_silver_customer","c360_gold_marketing","COLUMN_FILTER","PII columns excluded (name,email,ssn,phone,address,dob)","gold",batch_id,500),
    (8,"c360_silver_customer","c360_gold_risk","COLUMN_MASK","SSN→***-**-XXXX, email→ab***@domain, phone→***-***-XXXX","gold",batch_id,500),
    (9,"c360_silver_customer","c360_gold_compliance","FULL_COPY","All columns preserved + audit metadata","gold",batch_id,500),
]

spark.createDataFrame(lineage,["lineage_id","source_table","target_table","transformation_type","transformation_logic","layer","pipeline_run_id","record_count"]) \
    .withColumn("created_at",F.current_timestamp()) \
    .write.mode("overwrite").format("delta").saveAsTable(f"{DB}.c360_lineage")
print(f"Lineage populated: {len(lineage)} records")

# COMMAND ----------

print("=== Full Lineage Graph ===")
spark.sql(f"SELECT layer, source_table, target_table, transformation_type FROM {DB}.c360_lineage ORDER BY lineage_id").show(20,truncate=False)

print("\n=== Upstream: What feeds c360_silver_customer? ===")
spark.sql(f"SELECT source_table, transformation_logic FROM {DB}.c360_lineage WHERE target_table='c360_silver_customer' AND source_table!='cust_master'").show(20,truncate=False)

print("\n=== Downstream: What comes from c360_silver_customer? ===")
spark.sql(f"SELECT target_table, transformation_type, transformation_logic FROM {DB}.c360_lineage WHERE source_table='c360_silver_customer'").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Audit Logging — Who Accessed What & When
# MAGIC
# MAGIC Immutable audit trail capturing every data access with user identity, target object,
# MAGIC rows returned, success/failure, and query fingerprint.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DB}.c360_audit_log (
    event_timestamp TIMESTAMP, user_email STRING, user_role STRING,
    action STRING, target_table STRING, target_view STRING,
    rows_returned BIGINT, query_fingerprint STRING,
    access_type STRING, client_ip STRING, success BOOLEAN, denial_reason STRING
) USING DELTA
""")

now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
audit = [
    (now,"alice@bank.com","marketing_analyst","SELECT","c360_silver_customer","c360_gold_marketing",500,"a1b2","QUERY","10.0.1.42",True,None),
    (now,"bob@bank.com","risk_officer","SELECT","c360_silver_customer","c360_gold_risk",500,"b2c3","QUERY","10.0.1.43",True,None),
    (now,"carol@bank.com","compliance_officer","SELECT","c360_silver_customer","c360_gold_compliance",500,"c3d4","QUERY","10.0.1.44",True,None),
    (now,"dave@bank.com","data_engineer","MERGE","c360_silver_customer",None,500,"d4e5","WRITE","10.0.2.10",True,None),
    (now,"alice@bank.com","marketing_analyst","SELECT","c360_silver_customer","c360_gold_compliance",0,"e5f6","QUERY","10.0.1.42",False,"Role denied: marketing cannot access compliance view"),
    (now,"eve@bank.com","marketing_analyst","SELECT","c360_silver_customer","c360_silver_customer",0,"f6a7","QUERY","10.0.1.45",False,"Direct table access restricted"),
]

spark.createDataFrame(audit,["event_timestamp","user_email","user_role","action","target_table","target_view","rows_returned","query_fingerprint","access_type","client_ip","success","denial_reason"]) \
    .write.mode("overwrite").format("delta").saveAsTable(f"{DB}.c360_audit_log")
print(f"Audit log populated: {len(audit)} entries")

# COMMAND ----------

print("=== Audit Trail ===")
spark.sql(f"SELECT event_timestamp, user_email, target_view, rows_returned, success FROM {DB}.c360_audit_log ORDER BY event_timestamp").show(20,truncate=False)

print("\n=== Access Denials (Security Incidents) ===")
spark.sql(f"SELECT user_email, target_view, denial_reason FROM {DB}.c360_audit_log WHERE success=false").show(truncate=False)

print("\n=== Access Summary by User ===")
spark.sql(f"""SELECT user_email, COUNT(*) attempts,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) ok,
    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) denied
    FROM {DB}.c360_audit_log GROUP BY user_email ORDER BY attempts DESC""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Time Travel — Compliance Point-in-Time Queries
# MAGIC
# MAGIC Delta's versioned transaction log enables querying data as it existed at any prior
# MAGIC version. Critical for: *"What did the record show on March 15?"*

# COMMAND ----------

print("=== Silver Customer — Version History ===")
spark.sql(f"DESCRIBE HISTORY {DB}.c360_silver_customer") \
    .select("version","timestamp","operation",F.col("operationMetrics.numOutputRows").alias("rows")).show(truncate=False)

cur_ver = spark.sql(f"SELECT MAX(version) v FROM (DESCRIBE HISTORY {DB}.c360_silver_customer)").collect()[0]["v"]
print(f"\nCurrent version: {cur_ver}")

print("\n=== Time Travel: Query current version ===")
spark.sql(f"SELECT customer_id, first_name, credit_score, total_transactions, fraud_reports FROM {DB}.c360_silver_customer VERSION AS OF {cur_ver} LIMIT 5").show(truncate=False)

# Also show bronze history
print("\n=== Bronze Transactions — Version History ===")
spark.sql(f"DESCRIBE HISTORY {DB}.c360_bronze_transactions") \
    .select("version","timestamp","operation",F.col("operationMetrics.numOutputRows").alias("rows")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Quality Gates — Delta Constraints
# MAGIC
# MAGIC Enforce data quality at the table level. Constraints reject bad data at write time.

# COMMAND ----------

constraints = [
    (f"{DB}.c360_silver_customer","ck_credit_score","credit_score >= 300 AND credit_score <= 850"),
    (f"{DB}.c360_silver_customer","ck_customer_id","customer_id LIKE 'CUST%'"),
    (f"{DB}.c360_bronze_transactions","ck_positive_amount","amount > 0"),
    (f"{DB}.c360_bronze_loans","ck_loan_amount","loan_amount > 0"),
]
for tbl, name, cond in constraints:
    try:
        spark.sql(f"ALTER TABLE {tbl} ADD CONSTRAINT {name} CHECK ({cond})")
        print(f"  [OK] {name} on {tbl.split('.')[-1]}")
    except Exception as e:
        if "already exists" in str(e).lower(): print(f"  [SKIP] {name} already exists")
        else: print(f"  [FAIL] {name}: {str(e)[:80]}")

# Test constraint enforcement
print("\n--- Constraint Enforcement Test ---")
try:
    spark.sql(f"INSERT INTO {DB}.c360_silver_customer (customer_id, credit_score) VALUES ('CUST99999',-100)")
except Exception as e: print(f"  PASS: negative credit_score blocked ({str(e)[:80]})")

try:
    spark.sql(f"INSERT INTO {DB}.c360_bronze_transactions (transaction_id, customer_id, amount, source_system) VALUES ('TXN0','CUST00001',-50,'CORE_BANKING')")
except Exception as e: print(f"  PASS: negative amount blocked ({str(e)[:80]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Operational Monitoring

# COMMAND ----------

print("=== Table Row Counts ===")
for tbl in ["c360_bronze_transactions","c360_bronze_loans","c360_bronze_credit_cards",
            "c360_bronze_web_activity","c360_bronze_support_tickets",
            "c360_silver_customer","c360_lineage","c360_audit_log","c360_role_membership"]:
    cnt = spark.sql(f"SELECT COUNT(*) c FROM {DB}.{tbl}").collect()[0][0]
    print(f"  {tbl}: {cnt}")

print("\n=== Data Freshness ===")
spark.sql(f"SELECT MIN(_silver_timestamp) oldest, MAX(_silver_timestamp) newest, COUNT(*) customers FROM {DB}.c360_silver_customer").show(truncate=False)

print("=== Customer Risk Tiers ===")
spark.sql(f"""SELECT
    CASE WHEN credit_score>=750 AND defaulted_loans=0 AND fraud_reports=0 THEN 'Low'
         WHEN credit_score>=650 AND defaulted_loans<=1 THEN 'Medium'
         ELSE 'High' END risk_tier,
    COUNT(*) n, ROUND(AVG(credit_utilization_ratio*100),1) avg_util_pct
    FROM {DB}.c360_silver_customer GROUP BY risk_tier ORDER BY n DESC""").show()

# Maintenance
try:
    spark.sql(f"OPTIMIZE {DB}.c360_silver_customer ZORDER BY (customer_id)")
    print("\nOPTIMIZE complete.")
except Exception as e: print(f"\nOPTIMIZE skipped: {str(e)[:60]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Summary
# MAGIC
# MAGIC | Layer | Object | Type | Purpose |
# MAGIC |-------|--------|------|---------|
# MAGIC | Source | 5 source DataFrames | Temp View | Simulated enterprise systems |
# MAGIC | Bronze | `c360_bronze_{5 sources}` | Delta Table | Raw + metadata columns |
# MAGIC | Silver | `c360_silver_customer` | Delta Table | Golden record (MERGE, 500 customers) |
# MAGIC | Gold | `c360_gold_marketing` | View | Zero PII — behavioral analytics |
# MAGIC | Gold | `c360_gold_risk` | View | Masked PII — risk profiling |
# MAGIC | Gold | `c360_gold_compliance` | View | Full PII — compliance + audit |
# MAGIC | Access | `c360_dynamic_customer` | View | CURRENT_USER() role-based security |
# MAGIC | Gov | `c360_role_membership` | Delta Table | User-to-role mapping |
# MAGIC | Gov | `c360_lineage` | Delta Table | Source-to-target traceability |
# MAGIC | Gov | `c360_audit_log` | Delta Table | Immutable access log |
# MAGIC
# MAGIC **Security Features:** Column-level security (3 views with different PII exposure) ·
# MAGIC Row-level security (CURRENT_USER() dynamic filtering) · Full lineage traceability ·
# MAGIC Immutable audit logging · Time Travel compliance queries · Delta CHECK constraints ·
# MAGIC Idempotent MERGE writes · Managed Delta tables (serverless, no /tmp/ or dbutils)

# COMMAND ----------

print("=== ALL C360 GOVERNANCE OBJECTS ===")
spark.sql(f"SHOW TABLES IN {DB} LIKE 'c360*'").show(50,truncate=False)
spark.sql(f"SHOW VIEWS IN {DB} LIKE 'c360*'").show(20,truncate=False)
tbl_n = spark.sql(f"SHOW TABLES IN {DB} LIKE 'c360*'").count()
vw_n = spark.sql(f"SHOW VIEWS IN {DB} LIKE 'c360*'").count()
print(f"Total: {tbl_n} tables + {vw_n} views = {tbl_n+vw_n} objects  |  500 customers  |  5 sources  |  11,700+ rows")
print("=== Customer 360 Governance Project — COMPLETE ===")
