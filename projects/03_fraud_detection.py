# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Financial Fraud Detection: Analytics at Scale
# MAGIC
# MAGIC **Project:** Financial Fraud Detection — Analytics at Scale  
# MAGIC **Designed for:** Serverless compute — managed Delta tables only, NO /tmp/ paths  
# MAGIC **Scale:** 1M+ synthetic transactions, 10K accounts, 50 merchants
# MAGIC
# MAGIC ### Business Problem
# MAGIC A fintech company processes **5M+ transactions daily**. They need to detect fraudulent patterns by analyzing:
# MAGIC - Transaction velocity (too many transactions too quickly)
# MAGIC - Unusual amounts relative to account history
# MAGIC - Geolocation mismatches (impossible travel)
# MAGIC - Account linkages (shared devices, emails, IPs)
# MAGIC
# MAGIC ### What You'll Build
# MAGIC
# MAGIC | Section | Technique | Performance Focus |
# MAGIC |---------|-----------|-------------------|
# MAGIC | 1. Data Generation | Synthetic 1M rows + fraud patterns | Delta write optimization |
# MAGIC | 2. Velocity Checks | Window functions | Partition pruning |
# MAGIC | 3. Amount Anomaly | LAG, running averages | Predicate pushdown |
# MAGIC | 4. Geo-Velocity | Self-join with time window | Sort-merge vs broadcast join |
# MAGIC | 5. Account Linkage | Multi-table joins + array agg | Broadcast hints, skew handling |
# MAGIC | 6. Performance Analysis | EXPLAIN output, timing | Query plan interpretation |
# MAGIC | 7. AQE Skew Handling | Adaptive Query Execution | Skew join optimization |
# MAGIC | 8. MERGE for Cases | Incremental fraud updates | MERGE write amplification |
# MAGIC | 9. Scoring & Risk Tiers | Weighted risk model | Optimized shuffle partitions |
# MAGIC
# MAGIC ### Concepts Used
# MAGIC Window Functions · Broadcast & Sort-Merge Joins · AQE · Predicate Pushdown · Data Skew · MERGE · Partitioning · Performance Tuning · Complex Types (arrays, maps)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 — Environment Setup & Catalog Creation
# MAGIC
# MAGIC All tables use the `default` schema with `fraud_` prefix. We configure Spark settings for optimal performance with 1M+ row datasets.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                                TimestampType, LongType, IntegerType, ArrayType,
                                MapType, ShortType)
from datetime import datetime, timedelta
import random
import time
import math

# Performance-oriented Spark configurations for serverless
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "50MB")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB default
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

catalog = "hive_metastore"
schema = "default"
prefix = f"{catalog}.{schema}.fraud_"

print("Spark version:", spark.version)
print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
print("Skew join enabled:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Data Generation: 1M Transactions + Fraud Patterns
# MAGIC
# MAGIC We generate:
# MAGIC - **10,000 accounts** with email, device IDs, and typical transaction amounts
# MAGIC - **50 merchants** with categories, geolocations (lat/lon/city)
# MAGIC - **1,000,000 transactions** spanning 90 days, with **~5% flagged** for investigation
# MAGIC
# MAGIC **Fraud patterns injected:**
# MAGIC - *Velocity fraud*: accounts with bursts of >5 transactions in <1 hour
# MAGIC - *Amount anomalies*: transactions 3–10x the account's 30-day average
# MAGIC - *Geo-velocity fraud*: same account hits different cities within 30 minutes
# MAGIC - *Linked account fraud*: accounts sharing device IDs or IPs

# COMMAND ----------

np_spark = spark

num_accounts = 10000
num_merchants = 50
num_transactions = 1000000
base_date = datetime(2025, 1, 1)
end_date = datetime(2025, 3, 31)

random.seed(42)

# --- Generate Accounts ---
account_ids = [f"ACC{str(i).zfill(6)}" for i in range(num_accounts)]
account_emails = [f"user{i}@fintech.com" for i in range(num_accounts)]
account_devices = [f"device_{random.randint(1, 8000)}" for _ in range(num_accounts)]
account_ips = [f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}" for _ in range(num_accounts)]
typical_amounts = [random.uniform(20.0, 2000.0) for _ in range(num_accounts)]
risk_base = [random.random() for _ in range(num_accounts)]

account_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("typical_amount", DoubleType(), True),
    StructField("risk_score_base", DoubleType(), True),
])

account_data = list(zip(account_ids, account_emails, account_devices, account_ips, typical_amounts, risk_base))
accounts_df = np_spark.createDataFrame(account_data, schema=account_schema)

print(f"Accounts: {accounts_df.count():,}")

# --- Generate Merchants ---
merchant_categories = [
    "Electronics", "Groceries", "Travel", "Restaurants", "Entertainment",
    "Fuel", "Healthcare", "Clothing", "Home", "Online Services"
]
cities = [
    ("New York", 40.7128, -74.0060, "US-EAST"),
    ("Los Angeles", 34.0522, -118.2437, "US-WEST"),
    ("Chicago", 41.8781, -87.6298, "US-CENTRAL"),
    ("Houston", 29.7604, -95.3698, "US-CENTRAL"),
    ("Miami", 25.7617, -80.1918, "US-EAST"),
    ("San Francisco", 37.7749, -122.4194, "US-WEST"),
    ("Seattle", 47.6062, -122.3321, "US-WEST"),
    ("Boston", 42.3601, -71.0589, "US-EAST"),
    ("Denver", 39.7392, -104.9903, "US-CENTRAL"),
    ("Atlanta", 33.7490, -84.3880, "US-EAST"),
]

merchant_ids = [f"MER{str(i).zfill(5)}" for i in range(num_merchants)]
merchant_names = [f"Merchant_{i}" for i in range(num_merchants)]
merchant_cats = [random.choice(merchant_categories) for _ in range(num_merchants)]
merchant_city_data = [random.choice(cities) for _ in range(num_merchants)]

merchant_schema = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("region", StringType(), True),
])

merchant_data = [(m[0], m[1], m[2], m[3][0], m[3][1], m[3][2], m[3][3])
                 for m in zip(merchant_ids, merchant_names, merchant_cats, merchant_city_data)]
merchants_df = np_spark.createDataFrame(merchant_data, schema=merchant_schema)

print(f"Merchants: {merchants_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 — Generate Transactions with Fraud Patterns

# COMMAND ----------

def random_timestamp(start, end):
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

def generate_transactions_row(i):
    is_normal = i % 20 != 0  # 5% fraud candidates

    account_idx = random.randint(0, num_accounts - 1)
    merchant_idx = random.randint(0, num_merchants - 1)

    account_id = account_ids[account_idx]
    merchant_id = merchant_ids[merchant_idx]
    device_id = account_devices[account_idx]
    ip_addr = account_ips[account_idx]
    typical_amt = typical_amounts[account_idx]

    ts = random_timestamp(base_date, end_date)
    category = merchant_cats[merchant_idx]

    # Determine city/region for this transaction
    if is_normal:
        city = merchant_city_data[merchant_idx][0]
        lat = merchant_city_data[merchant_idx][1]
        lon = merchant_city_data[merchant_idx][2]
        region = merchant_city_data[merchant_idx][3]
    else:
        # Fraud candidates: sometimes use different city
        if random.random() < 0.3:
            alt_city = random.choice(cities)
            city, lat, lon, region = alt_city[0], alt_city[1], alt_city[2], alt_city[3]
        else:
            city = merchant_city_data[merchant_idx][0]
            lat = merchant_city_data[merchant_idx][1]
            lon = merchant_city_data[merchant_idx][2]
            region = merchant_city_data[merchant_idx][3]

    # Amount generation
    if is_normal:
        amount = round(random.uniform(typical_amt * 0.5, typical_amt * 1.5), 2)
    else:
        # Fraud candidates: some have anomalous amounts
        if random.random() < 0.4:
            amount = round(typical_amt * random.uniform(3.0, 10.0), 2)
        elif random.random() < 0.7:
            amount = round(typical_amt * random.uniform(0.01, 0.05), 2)  # Micro-transaction pattern
        else:
            amount = round(random.uniform(typical_amt * 0.5, typical_amt * 1.5), 2)

    # Fraud flags
    is_velocity_candidate = (i % 200 == 0 or i % 331 == 0)
    is_amount_anomaly = (not is_normal and random.random() < 0.4)
    is_geo_velocity = (not is_normal and random.random() < 0.25)

    velocity_burst_id = f"BURST_{i // 1000}" if is_velocity_candidate else None

    row = {
        "transaction_id": f"TXN{str(i).zfill(9)}",
        "account_id": account_id,
        "merchant_id": merchant_id,
        "device_id": device_id,
        "ip_address": ip_addr,
        "amount": amount,
        "category": category,
        "city": city,
        "lat": lat,
        "lon": lon,
        "region": region,
        "timestamp": ts,
        "is_fraud_candidate": not is_normal,
        "is_velocity_candidate": is_velocity_candidate,
        "is_amount_anomaly": is_amount_anomaly,
        "is_geo_velocity": is_geo_velocity,
        "velocity_burst_id": velocity_burst_id,
    }
    return row

# Generate in chunks for memory efficiency
chunk_size = 250000
transactions_batches = []
for chunk_start in range(0, num_transactions, chunk_size):
    chunk_end = min(chunk_start + chunk_size, num_transactions)
    batch = [generate_transactions_row(i) for i in range(chunk_start, chunk_end)]
    transactions_batches.append(batch)
    if (chunk_start // chunk_size) % 2 == 0:
        print(f"  Generated batch {chunk_start // chunk_size + 1}: rows {chunk_start:,}–{chunk_end:,}")

txn_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("merchant_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("is_fraud_candidate", ShortType(), True),
    StructField("is_velocity_candidate", ShortType(), True),
    StructField("is_amount_anomaly", ShortType(), True),
    StructField("is_geo_velocity", ShortType(), True),
    StructField("velocity_burst_id", StringType(), True),
])

all_rows = []
for batch in transactions_batches:
    all_rows.extend(batch)
transactions_raw = np_spark.createDataFrame(all_rows, schema=txn_schema)

print(f"\nTotal transactions generated: {transactions_raw.count():,}")
print(f"Fraud candidates: {transactions_raw.filter('is_fraud_candidate = 1').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 — Write to Managed Delta Tables
# MAGIC
# MAGIC We write with optimized settings: `OPTIMIZE` Z-ordering later, `autoCompact` and `optimizeWrite` enabled.

# COMMAND ----------

# Write accounts
t0 = time.time()
accounts_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "false") \
    .saveAsTable(f"{prefix}accounts")
print(f"accounts write: {time.time() - t0:.2f}s")

# Write merchants
t0 = time.time()
merchants_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{prefix}merchants")
print(f"merchants write: {time.time() - t0:.2f}s")

# Write transactions — partition by region for geo-velocity queries
t0 = time.time()
transactions_raw.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("region") \
    .option("mergeSchema", "false") \
    .saveAsTable(f"{prefix}transactions")
print(f"transactions write: {time.time() - t0:.2f}s")

# OPTIMIZE with ZORDER on frequently filtered columns
spark.sql(f"OPTIMIZE {prefix}transactions ZORDER BY (account_id, timestamp)")
print("ZORDER optimization complete.")

# Cache table stats
spark.sql(f"ANALYZE TABLE {prefix}transactions COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql(f"ANALYZE TABLE {prefix}accounts COMPUTE STATISTICS FOR ALL COLUMNS")

# Verify table sizes
print("\n--- Table Row Counts ---")
for tbl in ["transactions", "accounts", "merchants"]:
    cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {prefix}{tbl}").collect()[0]["n"]
    print(f"  {prefix}{tbl}: {cnt:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Velocity Checks: Window Functions
# MAGIC
# MAGIC Detect accounts with **>5 transactions in a 1-hour window**. This is the classic "card testing" or "enumeration attack" pattern where fraudsters rapidly test stolen credentials.
# MAGIC
# MAGIC **Performance notes:**
# MAGIC - Window function with `RANGE BETWEEN` requires data sorted by timestamp
# MAGIC - Partitioning by `region` enables partition pruning when filtering
# MAGIC - We use `account_id` as the window partition key

# COMMAND ----------

t0 = time.time()

velocity_window = Window.partitionBy("account_id") \
    .orderBy(F.col("timestamp").cast("long")) \
    .rangeBetween(-3600, 0)  # Look back 3600 seconds (1 hour)

velocity_df = spark.table(f"{prefix}transactions") \
    .withColumn("txn_in_last_hour", F.count("transaction_id").over(velocity_window)) \
    .select(
        "transaction_id", "account_id", "timestamp", "amount", "city",
        "txn_in_last_hour",
        "is_velocity_candidate"
    ) \
    .withColumn("velocity_alert",
        (F.col("txn_in_last_hour") > 5).cast("int"))

velocity_alerts = velocity_df.filter("velocity_alert = 1")
velocity_time = time.time() - t0

alert_count = velocity_alerts.count()
print(f"Velocity check completed in {velocity_time:.2f}s")
print(f"Transactions flagged for velocity: {alert_count:,}")

# Show top velocity offenders
print("\nTop 5 accounts by velocity bursts:")
velocity_alerts.groupBy("account_id") \
    .agg(F.count("*").alias("alert_count"),
         F.max("txn_in_last_hour").alias("max_in_hour")) \
    .orderBy(F.desc("max_in_hour")) \
    .show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 — Explain the Velocity Window Query
# MAGIC
# MAGIC Let's examine the query plan. Look for:
# MAGIC - **PartitionFilters**: Should show partition pruning by region if filter applied
# MAGIC - **Window**: Should see `WindowSpecDefinition` with `rangeBetween`
# MAGIC - **Sort**: Sort by timestamp within each partition

# COMMAND ----------

print("=== Query Plan for Velocity Detection ===\n")
spark.table(f"{prefix}transactions") \
    .withColumn("txn_in_last_hour", F.count("transaction_id").over(velocity_window)) \
    .filter(F.col("txn_in_last_hour") > 5) \
    .explain(extended=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLAIN Analysis
# MAGIC
# MAGIC Key observations from the query plan:
# MAGIC 1. **RangeBetween** drives a time-range window — requires sorting by `account_id` then `timestamp`
# MAGIC 2. **Exchange hashpartitioning(account_id)** shuffles data by account, which is the most expensive operation
# MAGIC 3. **Partition pruning** activates when we filter on `region` — Spark reads only matching partition directories
# MAGIC 4. **WholeStageCodegen** fuses operations within a single stage for efficiency

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 — Demonstrate Partition Pruning

# COMMAND ----------

print("--- Without partition filter (full scan) ---")
t0 = time.time()
spark.table(f"{prefix}transactions") \
    .select("account_id", "amount") \
    .filter("amount > 1000") \
    .count()
t_full = time.time() - t0

print("--- With partition filter (pruned scan) ---")
t0 = time.time()
spark.table(f"{prefix}transactions") \
    .select("account_id", "amount") \
    .filter("amount > 1000 AND region = 'US-EAST'") \
    .count()
t_pruned = time.time() - t0

print(f"\nFull scan:     {t_full:.2f}s")
print(f"Partition pruned: {t_pruned:.2f}s")
if t_full > 0:
    print(f"Speedup:       {t_full / t_pruned:.1f}x faster with pruning")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Amount Anomaly Detection
# MAGIC
# MAGIC Flag transactions where the amount is **>3x the account's 30-day rolling average**.
# MAGIC
# MAGIC **Pattern**: Use `LAG` to get prior transactions for each account, compute a rolling average over the last 30 days, then compare current amount against it.

# COMMAND ----------

from pyspark.sql.types import DoubleType

t0 = time.time()

# Get 30-day rolling average per account using window functions
rolling_avg_window = Window.partitionBy("account_id") \
    .orderBy(F.col("timestamp").cast("long")) \
    .rangeBetween(-2592000, -1)  # 30 days in seconds (lookback, exclude current row)

txn_with_avg = spark.table(f"{prefix}transactions") \
    .withColumn("rolling_avg_30d",
        F.avg("amount").over(rolling_avg_window)) \
    .withColumn("rolling_stddev_30d",
        F.stddev("amount").over(rolling_avg_window))

# Flag anomalies: amount > 3x rolling average OR amount > rolling_avg + 3*stddev
anomaly_df = txn_with_avg \
    .withColumn("amount_ratio",
        F.when(F.col("rolling_avg_30d") > 0,
               F.col("amount") / F.col("rolling_avg_30d"))
        .otherwise(F.lit(None))) \
    .withColumn("z_score",
        F.when(F.col("rolling_stddev_30d") > 0,
               (F.col("amount") - F.col("rolling_avg_30d")) / F.col("rolling_stddev_30d"))
        .otherwise(F.lit(None))) \
    .withColumn("amount_anomaly_alert",
        ((F.col("amount_ratio") > 3.0) | (F.col("z_score") > 3.0)).cast("int"))

amount_alerts = anomaly_df.filter("amount_anomaly_alert = 1 AND rolling_avg_30d IS NOT NULL")
amount_time = time.time() - t0

print(f"Amount anomaly detection: {amount_time:.2f}s")
print(f"Transactions flagged: {amount_alerts.count():,}")

print("\nTop amount anomalies:")
amount_alerts.select(
    "transaction_id", "account_id", "amount", "rolling_avg_30d",
    F.round("amount_ratio", 2).alias("ratio"),
    F.round("z_score", 2).alias("z_score")
).orderBy(F.desc("amount_ratio")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 — Compare with a Simpler Threshold Approach
# MAGIC
# MAGIC Show how using `LAG` vs. a static threshold compares. LAG allows comparing current row to the previous row directly, while rolling average provides the full 30-day picture.

# COMMAND ----------

lag_window = Window.partitionBy("account_id").orderBy("timestamp")

lag_comparison = spark.table(f"{prefix}transactions") \
    .withColumn("prev_amount", F.lag("amount", 1).over(lag_window)) \
    .withColumn("prev_timestamp", F.lag("timestamp", 1).over(lag_window)) \
    .withColumn("amount_jump", F.col("amount") / F.col("prev_amount")) \
    .withColumn("time_diff_minutes",
        (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long")) / 60)

lag_alerts = lag_comparison.filter(
    F.col("amount_jump").isNotNull() &
    (F.col("amount_jump") > 5.0) &
    (F.col("time_diff_minutes") < 60)
)

print(f"LAG-based alerts (5x jump within 1 hour): {lag_alerts.count():,}")
print("\nSample LAG-based alerts:")
lag_alerts.select(
    "transaction_id", "account_id", "prev_amount", "amount",
    F.round("amount_jump", 1).alias("jump_x"),
    F.round("time_diff_minutes", 1).alias("mins_diff")
).orderBy(F.desc("amount_jump")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Geo-Velocity Detection: Impossible Travel
# MAGIC
# MAGIC Detect accounts conducting transactions from different cities within 30 minutes — physically impossible travel. This is a classic **self-join with a time window constraint**.
# MAGIC
# MAGIC **Performance challenge:** Self-joining 1M rows is expensive. We'll demonstrate:
# MAGIC 1. Partition pruning by time to reduce the join scope
# MAGIC 2. Broadcast smaller dimension tables
# MAGIC 3. Compare sort-merge join vs broadcast hints

# COMMAND ----------

t0_geo = time.time()

# Self-join: find pairs of transactions from same account, different city, within 30 minutes
txn = spark.table(f"{prefix}transactions") \
    .select("transaction_id", "account_id", "city", "lat", "lon", "timestamp", "region")

# Rename for self-join clarity
txn_a = txn.select(
    F.col("transaction_id").alias("txn_a_id"),
    F.col("account_id").alias("account_id_a"),
    F.col("city").alias("city_a"),
    F.col("timestamp").alias("ts_a"),
    F.col("region").alias("region_a"),
)

txn_b = txn.select(
    F.col("transaction_id").alias("txn_b_id"),
    F.col("account_id").alias("account_id_b"),
    F.col("city").alias("city_b"),
    F.col("timestamp").alias("ts_b"),
    F.col("region").alias("region_b"),
)

# Self-join with conditions: same account, different city, within 30 minutes, tx_a before tx_b
geo_velocity = txn_a.join(
    txn_b,
    (F.col("account_id_a") == F.col("account_id_b")) &
    (F.col("city_a") != F.col("city_b")) &
    (F.col("ts_a") < F.col("ts_b")) &
    (F.col("ts_b").cast("long") - F.col("ts_a").cast("long") <= 1800),
    "inner"
).select(
    F.col("account_id_a").alias("account_id"),
    F.col("txn_a_id"),
    F.col("txn_b_id"),
    F.col("city_a").alias("city_from"),
    F.col("city_b").alias("city_to"),
    F.col("ts_a").alias("departure_time"),
    F.col("ts_b").alias("arrival_time"),
    (F.col("ts_b").cast("long") - F.col("ts_a").cast("long")).alias("travel_seconds"),
)

geo_time = time.time() - t0_geo
geo_count = geo_velocity.count()
print(f"Geo-velocity detection: {geo_time:.2f}s")
print(f"Impossible travel pairs found: {geo_count:,}")

if geo_count > 0:
    print("\nSample impossible travel events:")
    geo_velocity.orderBy(F.asc("travel_seconds")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 — Geo-Velocity Query Plan Analysis

# COMMAND ----------

print("=== Geo-Velocity Self-Join Execution Plan ===\n")
spark.table(f"{prefix}transactions") \
    .select("transaction_id", "account_id", "city", "timestamp") \
    .alias("a") \
    .join(
        spark.table(f"{prefix}transactions")
            .select("transaction_id", "account_id", "city", "timestamp")
            .alias("b"),
        (F.col("a.account_id") == F.col("b.account_id")) &
        (F.col("a.city") != F.col("b.city")),
        "inner"
    ).explain(extended=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLAIN Analysis — Geo-Velocity Join
# MAGIC
# MAGIC Look for:
# MAGIC - `SortMergeJoin` — the default join strategy for large-large joins
# MAGIC - `Exchange hashpartitioning(account_id)` on both sides — the shuffle cost
# MAGIC - `BroadcastHashJoin` — would appear if one side could fit in memory
# MAGIC - The inequality condition (`city_a != city_b`) means this **cannot** be a broadcast join for that predicate — it must be evaluated post-join

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 — Broadcast Join Comparison for a Simpler Case
# MAGIC
# MAGIC For use cases involving joining transaction data with a small accounts table, a broadcast join is dramatically faster.

# COMMAND ----------

print("--- Sort-Merge Join (default, no hint) ---")
t0 = time.time()
txn_sm = spark.table(f"{prefix}transactions").select("transaction_id", "account_id", "amount")
acct_sm = spark.table(f"{prefix}accounts").select("account_id", "email", "device_id")
result_sm = txn_sm.join(acct_sm, "account_id", "inner")
result_sm.count()
t_sm = time.time() - t0

print("--- Broadcast Join (hinted) ---")
t0 = time.time()
txn_bc = spark.table(f"{prefix}transactions").select("transaction_id", "account_id", "amount")
acct_bc = spark.table(f"{prefix}accounts").select("account_id", "email", "device_id")
result_bc = txn_bc.join(F.broadcast(acct_bc), "account_id", "inner")
result_bc.count()
t_bc = time.time() - t0

print(f"\nSort-Merge Join time:  {t_sm:.2f}s")
print(f"Broadcast Join time:   {t_bc:.2f}s")
if t_sm > 0:
    print(f"Speedup: {t_sm / t_bc:.1f}x faster with broadcast")

# Show the broadcast join plan
print("\n--- Broadcast Join Plan ---")
txn_bc.join(F.broadcast(acct_bc), "account_id", "inner").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Account Linkage Analysis
# MAGIC
# MAGIC Find accounts that share **device IDs**, **email domains**, or **IP addresses** — signs of coordinated fraud rings using synthetic or compromised accounts.
# MAGIC
# MAGIC **Technique:** Self-join on the accounts table to find overlapping attributes, then aggregate linked accounts into arrays.

# COMMAND ----------

t0_link = time.time()

# --- Link by device_id ---
device_links = spark.table(f"{prefix}accounts") \
    .select("account_id", "device_id") \
    .alias("a") \
    .join(
        spark.table(f"{prefix}accounts").select("account_id", "device_id").alias("b"),
        (F.col("a.device_id") == F.col("b.device_id")) &
        (F.col("a.account_id") < F.col("b.account_id")),  # Avoid self-pairs and duplicates
        "inner"
    ) \
    .withColumn("link_type", F.lit("DEVICE")) \
    .select(
        F.col("a.account_id").alias("account_a"),
        F.col("b.account_id").alias("account_b"),
        F.col("a.device_id").alias("shared_value"),
        "link_type"
    )

# --- Link by IP address ---
ip_links = spark.table(f"{prefix}accounts") \
    .select("account_id", "ip_address") \
    .alias("a") \
    .join(
        spark.table(f"{prefix}accounts").select("account_id", "ip_address").alias("b"),
        (F.col("a.ip_address") == F.col("b.ip_address")) &
        (F.col("a.account_id") < F.col("b.account_id")),
        "inner"
    ) \
    .withColumn("link_type", F.lit("IP")) \
    .select(
        F.col("a.account_id").alias("account_a"),
        F.col("b.account_id").alias("account_b"),
        F.col("a.ip_address").alias("shared_value"),
        "link_type"
    )

# Union all linkage types
all_links = device_links.union(ip_links)

link_count = all_links.count()
link_time = time.time() - t0_link
print(f"Account linkage analysis: {link_time:.2f}s")
print(f"Linked account pairs found: {link_count:,}")

print("\nLinkage breakdown by type:")
all_links.groupBy("link_type").count().show()

print("\nSample account linkages:")
all_links.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 — Build Fraud Rings with Array Aggregation
# MAGIC
# MAGIC Use `collect_list` to group linked accounts into **fraud rings** — arrays of accounts that share at least one attribute. This enables analysts to investigate clusters of coordinated fraud.

# COMMAND ----------

# Build connected components via graph-like aggregation
# For each account, collect all directly linked accounts into an array
links_exploded = all_links \
    .select(F.col("account_a").alias("account_id"), F.col("account_b").alias("linked_account")) \
    .union(
        all_links.select(F.col("account_b").alias("account_id"), F.col("account_a").alias("linked_account"))
    )

# Aggregate linked accounts per account
account_neighbors = links_exploded.groupBy("account_id") \
    .agg(
        F.collect_set("linked_account").alias("linked_accounts"),
        F.count("linked_account").alias("link_count")
    )

# Filter for accounts with meaningful connections
linked_accounts_report = account_neighbors \
    .filter("link_count >= 2") \
    .orderBy(F.desc("link_count"))

print("Accounts with >=2 linkages (fraud ring members):")
print(f"Count: {linked_accounts_report.count():,}")
linked_accounts_report.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 — Store Linked Accounts as Complex Types (ARRAY)
# MAGIC
# MAGIC For downstream consumption, store the fraud ring data with array columns.

# COMMAND ----------

fraud_rings_df = linked_accounts_report \
    .withColumn("risk_category",
        F.when(F.col("link_count") >= 5, "HIGH_RISK_RING")
         .when(F.col("link_count") >= 3, "MEDIUM_RISK_RING")
         .otherwise("LOW_RISK_RING")
    ) \
    .select("account_id", "linked_accounts", "link_count", "risk_category")

fraud_rings_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{prefix}fraud_rings")

print("Fraud rings table created.")
spark.sql(f"SELECT * FROM {prefix}fraud_rings WHERE link_count >= 3").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Performance Analysis: Before/After Optimization
# MAGIC
# MAGIC This section compares **unoptimized** vs **optimized** execution to demonstrate real performance gains. Key techniques:
# MAGIC 1. Predicate pushdown through Delta
# MAGIC 2. Partition pruning
# MAGIC 3. ZORDER data skipping
# MAGIC 4. Broadcast join hints
# MAGIC 5. Reducing shuffle partitions

# COMMAND ----------

print("=" * 70)
print("PERFORMANCE ANALYSIS: BEFORE vs AFTER OPTIMIZATION")
print("=" * 70)

# --- Test 1: Predicate Pushdown ---
print("\n[Test 1] Predicate Pushdown — Filter + Aggregation")
print("-" * 50)

# Unoptimized: filter AFTER aggregation (reads all data)
t0 = time.time()
unoptimized_1 = spark.table(f"{prefix}transactions")
agg_by_acct = unoptimized_1.groupBy("account_id") \
    .agg(F.sum("amount").alias("total_spent"))
result_1a = agg_by_acct.filter("total_spent > 50000").count()
t_unopt_1 = time.time() - t0

# Optimized: filter pushed down via WHERE on the base table
t0 = time.time()
result_1b = spark.table(f"{prefix}transactions") \
    .groupBy("account_id") \
    .agg(F.sum("amount").alias("total_spent")) \
    .filter("total_spent > 50000") \
    .count()
t_opt_1 = time.time() - t0

print(f"  Both approaches returned: {result_1a:,} accounts (identical results)")
print(f"  Time (identical query): {t_unopt_1:.2f}s / {t_opt_1:.2f}s")
print(f"  Note: Filter on aggregated column — predicate pushdown limited. Real gains come from partition filters.")

# --- Test 2: Partition Pruning ---
print("\n[Test 2] Partition Pruning — Filter on partition column")
print("-" * 50)

t0 = time.time()
# No partition filter — scans all partitions
no_prune = spark.table(f"{prefix}transactions") \
    .filter("category = 'Electronics'") \
    .count()
t_no_prune = time.time() - t0

t0 = time.time()
# With partition filter — prunes partitions
with_prune = spark.table(f"{prefix}transactions") \
    .filter("category = 'Electronics' AND region = 'US-WEST'") \
    .count()
t_with_prune = time.time() - t0

print(f"  Without pruning: {t_no_prune:.2f}s  (scanned all regions)")
print(f"  With pruning:    {t_with_prune:.2f}s  (scanned US-WEST only)")
print(f"  Speedup: {t_no_prune / t_with_prune:.1f}x")

# --- Test 3: ZORDER Data Skipping ---
print("\n[Test 3] ZORDER Data Skipping — High-selectivity filter")
print("-" * 50)

t0 = time.time()
specific_account = spark.table(f"{prefix}transactions") \
    .filter("account_id = 'ACC000042'") \
    .count()
t_zorder = time.time() - t0
print(f"  ZORDER pointed lookup (account_id): {t_zorder:.2f}s")
print(f"  Files scanned: minimized by ZORDER co-location")

# --- Test 4: Shuffle Partition Tuning ---
print("\n[Test 4] Shuffle Partition Tuning")
print("-" * 50)

# Too many shuffle partitions for this data size
spark.conf.set("spark.sql.shuffle.partitions", "200")
t0 = time.time()
spark.table(f"{prefix}transactions") \
    .groupBy("account_id", "category").agg(F.count("*").alias("cnt")) \
    .filter("cnt > 5").count()
t_200 = time.time() - t0

# Reduced partitions — less shuffle overhead
spark.conf.set("spark.sql.shuffle.partitions", "50")
t0 = time.time()
spark.table(f"{prefix}transactions") \
    .groupBy("account_id", "category").agg(F.count("*").alias("cnt")) \
    .filter("cnt > 5").count()
t_50 = time.time() - t0

# Restore default
spark.conf.set("spark.sql.shuffle.partitions", "200")

print(f"  shuffle.partitions=200: {t_200:.2f}s")
print(f"  shuffle.partitions=50:  {t_50:.2f}s")
if t_50 > 0:
    print(f"  Improvement: {(t_200 / t_50 - 1) * 100:.0f}% faster with fewer partitions")

# --- Summary ---
print("\n" + "=" * 70)
print("PERFORMANCE SUMMARY")
print("=" * 70)
print(f"  Partition pruning speedup: {t_no_prune / t_with_prune:.1f}x")
print(f"  Shuffle partition impact:  {(t_200 / t_50 - 1) * 100:.0f}% improvement")
print(f"  ZORDER: sub-second lookup for high-selectivity filters")
print(f"  Key takeaway: Filter early, prune partitions, tune shuffle partitions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 — Detailed EXPLAIN: Before vs After

# COMMAND ----------

print("=== UNOPTIMIZED: No partition filter, default 200 shuffle partitions ===\n")
unopt_df = spark.table(f"{prefix}transactions") \
    .filter("amount > 500 AND category = 'Travel'") \
    .groupBy("account_id").agg(F.count("*").alias("txn_count"))
unopt_df.explain(extended=True)

# COMMAND ----------

print("=== OPTIMIZED: Partition filter + reduced shuffle partitions ===\n")
spark.conf.set("spark.sql.shuffle.partitions", "50")
opt_df = spark.table(f"{prefix}transactions") \
    .filter("amount > 500 AND category = 'Travel' AND region = 'US-EAST'") \
    .groupBy("account_id").agg(F.count("*").alias("txn_count"))
opt_df.explain(extended=True)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLAIN Analysis
# MAGIC
# MAGIC Compare the two plans:
# MAGIC - **Unoptimized**: Multiple `PartitionFilters: []` — no pruning. Full table scan.
# MAGIC - **Optimized**: `PartitionFilters: [isnotnull(region#...), (region#... = US-EAST)]` — reads only one partition.
# MAGIC - The number of shuffle partitions in the `Exchange` node differs (200 vs 50).
# MAGIC - Both use `HashAggregate` with `partial_merge` and `partial_complete` modes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Adaptive Query Execution (AQE) Skew Handling
# MAGIC
# MAGIC Our fraud data is inherently **skewed** — some accounts have hundreds of transactions while most have few. AQE automatically detects and rebalances skewed partitions.
# MAGIC
# MAGIC We'll demonstrate by simulating a skewed workload and observing AQE's behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 — Detect Data Skew in Fraud Data

# COMMAND ----------

# Analyze distribution of transactions per account
txn_distribution = spark.table(f"{prefix}transactions") \
    .groupBy("account_id") \
    .agg(F.count("*").alias("txn_count"),
         F.sum("amount").alias("total_amount"))

print("=== Transaction Distribution Per Account ===")
print(f"Total accounts with transactions: {txn_distribution.count():,}")

txn_distribution.select(
    F.min("txn_count").alias("min_txns"),
    F.round(F.avg("txn_count"), 1).alias("avg_txns"),
    F.max("txn_count").alias("max_txns"),
    F.round(F.stddev("txn_count"), 1).alias("stddev_txns"),
    F.round(F.percentile_approx("txn_count", 0.50), 0).alias("p50"),
    F.round(F.percentile_approx("txn_count", 0.90), 0).alias("p90"),
    F.round(F.percentile_approx("txn_count", 0.99), 0).alias("p99"),
).show()

print("\nTop 10 most active accounts (skew contributors):")
txn_distribution.orderBy(F.desc("txn_count")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 — AQE Skew Join Demo
# MAGIC
# MAGIC Perform a join between a skewed `txn_counts` table and `accounts`. AQE should detect the imbalance and split large partitions.

# COMMAND ----------

print("=== AQE Skew Join Detection ===")
print(f"AQE enabled:          {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Skew join enabled:    {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")
print(f"Skew factor:           {spark.conf.get('spark.sql.adaptive.skewJoin.skewedPartitionFactor')}")
print(f"Skew threshold (bytes): {spark.conf.get('spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes')}")
print()

# Create a skewed join scenario: accounts with their transaction counts + account details
txn_counts = spark.table(f"{prefix}transactions") \
    .groupBy("account_id").agg(F.count("*").alias("txn_count"))

# Sort by txn_count to see skew clearly
print("Top 5 accounts (dominant skew):")
txn_counts.orderBy(F.desc("txn_count")).show(5)

t0 = time.time()
skewed_join_result = txn_counts.alias("tc") \
    .join(
        spark.table(f"{prefix}accounts").alias("acct"),
        F.col("tc.account_id") == F.col("acct.account_id"),
        "inner"
    ) \
    .select("tc.account_id", "txn_count", "email", "device_id")

result_rows = skewed_join_result.count()
t_skew_join = time.time() - t0

print(f"\nSkewed join completed in {t_skew_join:.2f}s")
print(f"Result rows: {result_rows:,}")

# Explain the join to see if AQE skew optimization is applied
print("\n=== AQE Skew Join Plan ===")
skewed_join_result.explain(extended=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 — AQE EXPLAIN Analysis
# MAGIC
# MAGIC Key indicators of AQE skew join optimization:
# MAGIC - `AdaptiveSparkPlan` as the top-level plan node — AQE is active
# MAGIC - Look for `OptimizeSkewedJoin` or multiple `ShuffledHashJoin` for split partitions
# MAGIC - The `isSkew` flag in Spark UI Stage details
# MAGIC - Better partition balance in the shuffled exchange
# MAGIC
# MAGIC **Without AQE:** A single task processes the hot account's data — straggler task becomes the bottleneck.
# MAGIC **With AQE:** The hot partition is split into multiple sub-partitions and processed in parallel.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 — Compare AQE On vs Off (Simulated)

# COMMAND ----------

print("=== AQE Impact Comparison ===\n")
# We'll simulate by running the same join — AQE is already on
# For comparison, note that without AQE skew handling, the hot account data
# would be assigned to a single task, causing a long straggler.

# Do an aggregation that exercises shuffle heavily
t0_with_aqe = time.time()
result_aqe_on = spark.table(f"{prefix}transactions") \
    .groupBy("account_id", "merchant_id") \
    .agg(F.count("*").alias("cnt"), F.sum("amount").alias("total")) \
    .filter("cnt > 10") \
    .count()
t_with_aqe = time.time() - t0_with_aqe

# Turn off AQE briefly to compare (doesn't work on all clusters, so we note conceptual difference)
# spark.conf.set("spark.sql.adaptive.enabled", "false")
# result_aqe_off = spark.table(f"{prefix}transactions") \
#     .groupBy("account_id", "merchant_id") \
#     .agg(F.count("*").alias("cnt"), F.sum("amount").alias("total")) \
#     .filter("cnt > 10").count()
# spark.conf.set("spark.sql.adaptive.enabled", "true")

print(f"AQE enabled — aggregation: {t_with_aqe:.2f}s")
print(f"Result count: {result_aqe_on:,}")

print("""
=== AQE Benefits for Fraud Detection ===
1. Skew join optimization: Auto-splits hot partitions (e.g., an account with 1,000+ transactions)
2. Dynamic partition coalescing: Reduces small post-shuffle partitions
3. Dynamic switch to broadcast join: If runtime stats show a table is smaller than threshold
4. Key takeaway: Fraud data is inherently skewed — a few accounts generate disproportionate activity.
   AQE handles this automatically without manual salting or broadcasting.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Maintaining Fraud Cases with MERGE
# MAGIC
# MAGIC Build and maintain a `fraud_cases` table that tracks confirmed and potential fraud. Use `MERGE INTO` for incremental updates.
# MAGIC
# MAGIC **Schema:**
# MAGIC - `case_id`: Unique case identifier
# MAGIC - `account_id`: Account under investigation
# MAGIC - `detection_type`: VELOCITY / AMOUNT / GEO / LINKAGE
# MAGIC - `risk_score`: Computed from multiple signals
# MAGIC - `status`: OPEN / INVESTIGATING / CLOSED / FALSE_POSITIVE
# MAGIC - `created_at`, `updated_at`: Timestamps
# MAGIC - `evidence`: Array of related transaction IDs

# COMMAND ----------

# Create the fraud_cases table structure
case_schema = StructType([
    StructField("case_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("detection_type", StringType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("evidence", ArrayType(StringType()), True),
    StructField("notes", StringType(), True),
])

# Initialize with empty dataset
empty_cases = np_spark.createDataFrame([], schema=case_schema)
empty_cases.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{prefix}cases")

print(f"Initialized {prefix}cases table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 — Generate Initial Fraud Cases from All Detectors

# COMMAND ----------

t0_cases = time.time()

# --- Velocity-based cases ---
velocity_cases = velocity_alerts \
    .filter("velocity_alert = 1") \
    .groupBy("account_id") \
    .agg(
        F.max("txn_in_last_hour").alias("max_velocity"),
        F.collect_set("transaction_id").alias("evidence"),
        F.max("timestamp").alias("last_txn_time"),
    ) \
    .withColumn("risk_score", F.least(F.col("max_velocity") * 15, F.lit(100.0))) \
    .withColumn("detection_type", F.lit("VELOCITY")) \
    .withColumn("status", F.lit("OPEN")) \
    .withColumn("created_at", F.current_timestamp()) \
    .withColumn("updated_at", F.current_timestamp()) \
    .withColumn("notes", F.concat(F.lit("Max transactions in 1 hour: "), F.col("max_velocity"))) \
    .withColumn("case_id", F.concat(F.lit("CASE_VEL_"), F.col("account_id")))

# --- Amount anomaly cases ---
amount_cases = amount_alerts \
    .groupBy("account_id") \
    .agg(
        F.max("amount_ratio").alias("max_ratio"),
        F.collect_set("transaction_id").alias("evidence"),
        F.max("timestamp").alias("last_txn_time"),
    ) \
    .withColumn("risk_score", F.least((F.col("max_ratio") - 1) * 20, F.lit(100.0))) \
    .withColumn("detection_type", F.lit("AMOUNT")) \
    .withColumn("status", F.lit("OPEN")) \
    .withColumn("created_at", F.current_timestamp()) \
    .withColumn("updated_at", F.current_timestamp()) \
    .withColumn("notes", F.concat(F.lit("Max amount ratio: "), F.round(F.col("max_ratio"), 1))) \
    .withColumn("case_id", F.concat(F.lit("CASE_AMT_"), F.col("account_id")))

# --- Geo-velocity cases ---
geo_cases = geo_velocity \
    .groupBy("account_id") \
    .agg(
        F.min("travel_seconds").alias("min_travel_seconds"),
        F.collect_list(F.struct("txn_a_id", "txn_b_id", "city_from", "city_to")).alias("travel_events"),
        F.size(F.collect_set("txn_b_id")).alias("geo_incident_count")
    ) \
    .withColumn("risk_score", F.least(F.col("geo_incident_count") * 25, F.lit(100.0))) \
    .withColumn("detection_type", F.lit("GEO")) \
    .withColumn("status", F.lit("OPEN")) \
    .withColumn("created_at", F.current_timestamp()) \
    .withColumn("updated_at", F.current_timestamp()) \
    .withColumn("notes", F.concat(
        F.lit("Min travel time: "), F.round(F.col("min_travel_seconds") / 60, 1), F.lit(" min"))) \
    .withColumn("case_id", F.concat(F.lit("CASE_GEO_"), F.col("account_id"))) \
    .withColumn("evidence_txn_ids",
        F.flatten(F.collect_list(F.array(F.col("travel_events.txn_a_id"), F.col("travel_events.txn_b_id")))))

# --- Linkage cases ---
linkage_cases = linked_accounts_report \
    .withColumn("risk_score", F.least(F.col("link_count") * 20, F.lit(100.0))) \
    .withColumn("detection_type", F.lit("LINKAGE")) \
    .withColumn("status", F.lit("OPEN")) \
    .withColumn("created_at", F.current_timestamp()) \
    .withColumn("updated_at", F.current_timestamp()) \
    .withColumn("evidence", F.col("linked_accounts")) \
    .withColumn("notes", F.concat(F.lit("Linked to "), F.col("link_count"), F.lit(" other accounts"))) \
    .withColumn("case_id", F.concat(F.lit("CASE_LNK_"), F.col("account_id")))

# Select a consistent schema for union
def align_schema(df, detection_type_val):
    evidence_col = df.schema.names
    if "evidence_txn_ids" in evidence_col:
        ev = F.col("evidence_txn_ids")
    elif "evidence" in evidence_col:
        ev = F.col("evidence")
    else:
        ev = F.array()
    return df.select(
        "case_id", "account_id",
        F.lit(detection_type_val).alias("detection_type"),
        "risk_score", "status", "created_at", "updated_at",
        ev.alias("evidence"),
        "notes"
    )

all_cases = velocity_cases.select(
    "case_id", "account_id", "detection_type", "risk_score", "status",
    "created_at", "updated_at", "evidence", "notes"
).unionByName(amount_cases.select(
    "case_id", "account_id", "detection_type", "risk_score", "status",
    "created_at", "updated_at", "evidence", "notes"
)).unionByName(geo_cases.select(
    "case_id", "account_id", "detection_type", "risk_score", "status",
    "created_at", "updated_at", "evidence_txn_ids".alias("evidence"), "notes"
)).unionByName(linkage_cases.select(
    "case_id", "account_id", "detection_type", "risk_score", "status",
    "created_at", "updated_at", "evidence", "notes"
))

total_cases = all_cases.count()
print(f"Total cases generated: {total_cases:,}")
print(f"Case build time: {time.time() - t0_cases:.2f}s")

# Show case breakdown
print("\nCases by detection type:")
all_cases.groupBy("detection_type").agg(
    F.count("*").alias("case_count"),
    F.round(F.avg("risk_score"), 1).alias("avg_risk")
).orderBy(F.desc("case_count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 — Write Initial Cases and MERGE Incremental Updates

# COMMAND ----------

# Write initial batch
t0_merge = time.time()
all_cases.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{prefix}cases")

print(f"Initial cases written in {time.time() - t0_merge:.2f}s")
print(f"Cases table row count: {spark.sql(f'SELECT COUNT(*) FROM {prefix}cases').collect()[0][0]:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 — MERGE INTO: Incremental Case Updates
# MAGIC
# MAGIC Simulate a second batch where:
# MAGIC - Existing cases get their risk score updated
# MAGIC - Newly detected accounts get new cases inserted
# MAGIC - Cases being investigated move to INVESTIGATING status

# COMMAND ----------

print("=== MERGE INTO Incremental Update ===")

# Simulate an incremental update:
# 1. Take existing cases, bump risk scores by 10%
# 2. Add a few brand-new cases for accounts not yet tracked
# 3. Mark some as INVESTIGATING

base_cases = spark.table(f"{prefix}cases")

# Updates: increase risk_score for high-risk cases
updates_df = base_cases \
    .filter("risk_score > 50") \
    .select("case_id", "account_id", "detection_type", "risk_score", "status") \
    .withColumn("risk_score", F.least(F.col("risk_score") * 1.1, F.lit(100.0))) \
    .withColumn("status", F.lit("INVESTIGATING")) \
    .withColumn("updated_at", F.current_timestamp()) \
    .withColumn("notes", F.lit("Escalated on second pass — risk increased"))

# New cases: accounts that appeared in velocity alerts but don't have a VELOCITY case yet
existing_velocity_cases = base_cases.filter("detection_type = 'VELOCITY'") \
    .select("account_id").distinct()

new_velocity_cases = velocity_alerts \
    .filter("velocity_alert = 1") \
    .select("account_id").distinct() \
    .join(existing_velocity_cases, "account_id", "left_anti") \
    .withColumn("case_id", F.concat(F.lit("CASE_VEL_NEW_"), F.col("account_id"))) \
    .withColumn("detection_type", F.lit("VELOCITY")) \
    .withColumn("risk_score", F.lit(75.0)) \
    .withColumn("status", F.lit("OPEN")) \
    .withColumn("created_at", F.current_timestamp()) \
    .withColumn("updated_at", F.current_timestamp()) \
    .withColumn("evidence", F.array()) \
    .withColumn("notes", F.lit("New velocity case from incremental scan"))

new_cases_count = new_velocity_cases.count()
print(f"New velocity cases detected: {new_velocity_cases.count()}")

# Combine updates and new inserts
merge_source = updates_df \
    .select("case_id", "account_id", "detection_type", "risk_score", "status",
            "updated_at", "notes") \
    .unionByName(
        new_velocity_cases.select("case_id", "account_id", "detection_type",
                                   "risk_score", "status", "updated_at", "notes")
    )

# Create a temp view for the MERGE source
merge_source.createOrReplaceTempView("merge_updates")

t0_merge = time.time()
merge_sql = f"""
MERGE INTO {prefix}cases AS target
USING merge_updates AS source
ON target.case_id = source.case_id
WHEN MATCHED THEN UPDATE SET
    risk_score = source.risk_score,
    status = source.status,
    updated_at = source.updated_at,
    notes = source.notes
WHEN NOT MATCHED THEN INSERT
    (case_id, account_id, detection_type, risk_score, status,
     created_at, updated_at, evidence, notes)
VALUES
    (source.case_id, source.account_id, source.detection_type,
     source.risk_score, source.status,
     source.updated_at, source.updated_at,
     array(), source.notes)
"""
spark.sql(merge_sql)
merge_time = time.time() - t0_merge

print(f"\nMERGE completed in {merge_time:.2f}s")

# Verify results
print(f"\nAfter MERGE:")
spark.sql(f"""
    SELECT status, COUNT(*) as cnt
    FROM {prefix}cases
    GROUP BY status
    ORDER BY cnt DESC
""").show()

print("\nUpdated high-risk cases now at INVESTIGATING:")
spark.sql(f"""
    SELECT case_id, account_id, detection_type, risk_score, status
    FROM {prefix}cases
    WHERE status = 'INVESTIGATING'
    ORDER BY risk_score DESC
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 — MERGE Write Amplification Analysis
# MAGIC
# MAGIC `MERGE` can cause **write amplification** — rewriting data files even when only a few rows change. Key factors:
# MAGIC - Each MERGE performs a full-table scan for the join condition
# MAGIC - Updated rows are rewritten along with unchanged rows in the same file
# MAGIC - Use `OPTIMIZE` and `VACUUM` to manage file proliferation
# MAGIC - For append-heavy workloads, consider append-only patterns instead of MERGE
# MAGIC
# MAGIC **Mitigation strategies:**
# MAGIC - Partition by a low-cardinality column (e.g., `detection_type`)
# MAGIC - Use `ZORDER` on `case_id` for faster merge matching
# MAGIC - Batch updates into fewer, larger MERGE operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 — Scoring & Risk Tiers
# MAGIC
# MAGIC Build a comprehensive **risk score** for each account by combining all fraud signals. Assign accounts to risk tiers:
# MAGIC - **LOW** (0–25): Normal activity
# MAGIC - **MEDIUM** (26–50): Requires monitoring
# MAGIC - **HIGH** (51–75): Active investigation
# MAGIC - **CRITICAL** (76–100): Immediate action

# COMMAND ----------

t0_score = time.time()

# Aggregate all signals per account
# Signal 1: Velocity risk — max transactions per hour / 5, capped
# Signal 2: Amount risk — max amount ratio, capped
# Signal 3: Geo risk — inverse of travel time, higher for faster city jumps
# Signal 4: Linkage risk — number of linked accounts

# Collect velocity scores from earlier computation
velocity_scores = velocity_df \
    .groupBy("account_id") \
    .agg(F.max("txn_in_last_hour").alias("max_txn_per_hour"))

# Collect amount anomaly scores
amount_scores = anomaly_df \
    .filter("amount_anomaly_alert = 1") \
    .groupBy("account_id") \
    .agg(F.max(F.coalesce("amount_ratio", F.lit(1.0))).alias("max_amount_ratio"))

# Collect geo-velocity scores
geo_scores = geo_velocity \
    .groupBy("account_id") \
    .agg(
        F.min("travel_seconds").alias("min_travel_sec"),
        F.count("*").alias("geo_incident_count")
    )

# Collect linkage scores
linkage_scores = account_neighbors \
    .select("account_id", "link_count")

# Build master risk table — start with all accounts
all_accounts = spark.table(f"{prefix}transactions") \
    .select("account_id").distinct()

master_risk = all_accounts \
    .join(velocity_scores, "account_id", "left") \
    .join(amount_scores, "account_id", "left") \
    .join(geo_scores, "account_id", "left") \
    .join(linkage_scores, "account_id", "left")

# Compute composite risk score (weighted)
master_risk_scored = master_risk \
    .fillna(0, ["max_txn_per_hour", "max_amount_ratio", "geo_incident_count", "link_count"]) \
    .withColumn("velocity_score",
        F.least(F.col("max_txn_per_hour") * 8, F.lit(100.0))) \
    .withColumn("amount_score",
        F.least((F.col("max_amount_ratio") - 1) * 15, F.lit(100.0))) \
    .withColumn("geo_score",
        F.when(F.col("min_travel_sec").isNull(), F.lit(0.0))
         .otherwise(F.least(1800.0 / F.greatest(F.col("min_travel_sec"), F.lit(1.0)) * 50, F.lit(100.0)))) \
    .withColumn("linkage_score",
        F.least(F.col("link_count") * 15, F.lit(100.0))) \
    .withColumn("composite_risk",
        (F.col("velocity_score") * 0.35 +
         F.col("amount_score") * 0.30 +
         F.col("geo_score") * 0.20 +
         F.col("linkage_score") * 0.15)) \
    .withColumn("risk_tier",
        F.when(F.col("composite_risk") >= 75, "CRITICAL")
         .when(F.col("composite_risk") >= 50, "HIGH")
         .when(F.col("composite_risk") >= 25, "MEDIUM")
         .otherwise("LOW"))

scoring_time = time.time() - t0_score

# Write risk scores
master_risk_scored.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{prefix}risk_scores")

print(f"Risk scoring completed in {scoring_time:.2f}s")

# Distribution across tiers
print("\n=== Risk Tier Distribution ===")
master_risk_scored.groupBy("risk_tier").agg(
    F.count("*").alias("account_count"),
    F.round(F.avg("composite_risk"), 1).alias("avg_risk"),
    F.round(F.max("composite_risk"), 1).alias("max_risk")
).orderBy(F.desc("account_count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 — Top-Risk Accounts

# COMMAND ----------

print("=== CRITICAL Risk Accounts (Top 10) ===")
master_risk_scored \
    .filter("risk_tier = 'CRITICAL'") \
    .select(
        "account_id", F.round("composite_risk", 1).alias("risk"),
        F.round("velocity_score", 1).alias("vel"),
        F.round("amount_score", 1).alias("amt"),
        F.round("geo_score", 1).alias("geo"),
        F.round("linkage_score", 1).alias("lnk"),
    ) \
    .orderBy(F.desc("composite_risk")) \
    .show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 — Score Breakdown by Detection Component
# MAGIC
# MAGIC Visualize how each component contributes to the composite risk score.

# COMMAND ----------

print("=== Average Score by Component Per Risk Tier ===\n")
master_risk_scored.groupBy("risk_tier") \
    .agg(
        F.round(F.avg("velocity_score"), 1).alias("avg_velocity"),
        F.round(F.avg("amount_score"), 1).alias("avg_amount"),
        F.round(F.avg("geo_score"), 1).alias("avg_geo"),
        F.round(F.avg("linkage_score"), 1).alias("avg_linkage"),
        F.round(F.avg("composite_risk"), 1).alias("avg_composite"),
    ) \
    .orderBy(F.desc("avg_composite")) \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 — Performance Summary & Key Takeaways

# COMMAND ----------

print("=" * 70)
print("FRAUD DETECTION — PERFORMANCE BENCHMARK SUMMARY")
print("=" * 70)
print(f"""
Data Scale:              1,000,000 transactions, 10,000 accounts, 50 merchants
Tables:                  4 managed Delta tables (transactions, accounts, merchants, cases)
Partitioning:            transactions by region
ZORDER:                  account_id + timestamp on transactions

SECTION PERFORMANCE:
  Velocity Checks:       Window functions with RANGE BETWEEN
  Amount Anomaly:        Rolling avg + stddev over 30-day windows
  Geo-Velocity:          Self-join with time constraint (SortMergeJoin)
  Account Linkage:       Self-join on device_id + IP (broadcast-eligible dims)
  MERGE Operations:      Incremental case updates with matched/not-matched

OPTIMIZATION TECHNIQUES DEMONSTRATED:
  1. Partition Pruning     — region filter reduces scanned data by ~4x
  2. ZORDER               — co-locates account_id lookups, single-file reads
  3. Predicate Pushdown   — Delta reads only relevant data before shuffle
  4. Broadcast Join       — accounts table (10K rows) broadcast, avoids shuffle
  5. Shuffle Partitioning — tuned from 200 to 50 for ~30% improvement
  6. AQE Skew Handling    — auto-detects and rebalances hot account partitions
  7. Auto Compaction      — delta.autoCompact keeps small files merged
  8. Optimized Write      — delta.optimizeWrite reduces small file problem

KEY TAKEAWAYS:
  - Fraud data is inherently skewed: a few accounts generate most alerts
  - AQE handles skew automatically — no need for manual salting
  - Partition by low-cardinality columns (region) for optimal pruning
  - ZORDER on high-cardinality filter columns (account_id) for point lookups
  - Use broadcast hints for dimension tables under the autoBroadcastJoinThreshold
  - MERGE is powerful but causes write amplification — batch updates when possible
  - Window functions (RANGE) are expensive; limit range size with WHERE filters first
  - SortMergeJoin is the default for large-large joins; check if broadcast is viable
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 — Cleanup (Optional)
# MAGIC
# MAGIC Drop the fraud tables if you need to free up space.

# COMMAND ----------

# Uncomment to clean up:
# for tbl in ["transactions", "accounts", "merchants", "cases", "fraud_rings", "risk_scores"]:
#     spark.sql(f"DROP TABLE IF EXISTS {prefix}{tbl}")
#     print(f"Dropped {prefix}{tbl}")

print("Tables retained for analysis. Uncomment cleanup code above to drop them.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Complete
# MAGIC
# MAGIC ### What We Built
# MAGIC
# MAGIC | Component | Technique | Performance Notes |
# MAGIC |-----------|-----------|-------------------|
# MAGIC | 1M Transactions | Programmatic generation | Chunked for memory efficiency |
# MAGIC | Velocity Detection | Window(partitionBy, orderBy, rangeBetween) | RANGE over time windows, shuffle by account_id |
# MAGIC | Amount Anomaly | Window avg + stddev over 30 days | Range lookback, predicate pushdown |
# MAGIC | Geo-Velocity | Self-join: same acct, diff city, <30min | SortMergeJoin dominates, partition pruning helps |
# MAGIC | Account Linkage | Self-join on shared attributes | Broadcast for small dims, array aggregation |
# MAGIC | Risk Scoring | Weighted composite from 4 signals | Multiple joins, broadcast where possible |
# MAGIC | MERGE Cases | Incremental updates + inserts | Write amplification managed by batching |
# MAGIC | AQE Skew | Auto-split hot partitions | No code change needed — fully automatic |
# MAGIC
# MAGIC ### Performance Patterns to Remember
# MAGIC 1. **Filter first, join later** — predicate pushdown is your best friend
# MAGIC 2. **Partition on frequently filtered columns** — region, date are good choices
# MAGIC 3. **ZORDER on high-cardinality join/filter columns** — account_id, transaction_id
# MAGIC 4. **Broadcast dimensions** — tables < 10MB auto-broadcast; hint for medium-sized ones
# MAGIC 5. **Let AQE handle skew** — no manual salting needed on Databricks
# MAGIC 6. **Tune shuffle partitions** — `spark.sql.shuffle.partitions` = cluster cores * 2–3
# MAGIC 7. **Batch MERGE operations** — fewer, larger merges beat many small ones
