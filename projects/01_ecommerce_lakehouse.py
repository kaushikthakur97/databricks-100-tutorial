# Databricks notebook source

# MAGIC %md
# MAGIC # E-Commerce Lakehouse — Building a Production-Grade Data Platform
# MAGIC
# MAGIC ## End-to-End Project: Bronze → Silver → Gold → Serving
# MAGIC
# MAGIC **Business Problem:** An e-commerce company needs a unified data platform to track orders, customers, products, and inventory across their entire business. They need clean, queryable data for analytics, reporting, and ML.
# MAGIC
# MAGIC **What You'll Build:**
# MAGIC 1. **Bronze Layer** — Raw order ingestion with schema evolution and data validation
# MAGIC 2. **Silver Layer** — Cleaned, deduplicated data with type casting and quality checks
# MAGIC 3. **Gold Layer** — Business aggregates: daily sales, top products, customer lifetime value, inventory alerts
# MAGIC 4. **SCD Type 2** — Slowly changing dimension for customer addresses
# MAGIC 5. **Incremental Processing** — CDF-based incremental pipeline that only processes new/changed data
# MAGIC 6. **Data Quality** — Quality monitoring with pass/fail tracking
# MAGIC
# MAGIC **Concepts Used:** ACID Transactions, Schema Enforcement, Time Travel, OPTIMIZE, CDF, Medallion Architecture, SCD Type 2, Incremental Processing, MERGE INTO, Window Functions, Aggregations
# MAGIC
# MAGIC **Designed For:** Serverless compute — uses managed Delta tables (`saveAsTable`), no `/tmp/` paths, no `dbutils.fs` calls.
# MAGIC
# MAGIC **Table Prefix:** `ecom_`
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Setup & Cleanup
# MAGIC
# MAGIC Configure Spark for Delta Lake operations and define the working database. Drop any pre-existing tables from previous runs to ensure a clean state.

# COMMAND ----------

import time
import uuid
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("EcomLakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
    .getOrCreate()

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

DB = "default"

ALL_TABLES = [
    "ecom_bronze_orders", "ecom_silver_orders", "ecom_silver_customers",
    "ecom_silver_products", "ecom_silver_inventory",
    "ecom_gold_daily_sales", "ecom_gold_top_products", "ecom_gold_customer_ltv",
    "ecom_gold_inventory_alerts", "ecom_gold_category_performance",
    "ecom_dim_customer_scd2", "ecom_bronze_orders_incremental", "ecom_dq_results",
    "ecom_silver_orders_cdf", "ecom_bronze_orders_batch2"
]

for tbl in ALL_TABLES:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {DB}.{tbl}")
    except:
        pass

print(f"Working database: {DB}")
print(f"Cleaned up {len(ALL_TABLES)} tables from previous runs.")
print("Ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Synthetic Data Generation
# MAGIC
# MAGIC **Why this matters:** In a real e-commerce system, data arrives from multiple sources — order systems, CRM, inventory management, and product catalogs. We generate realistic synthetic data spanning 90 days of operations to simulate this diversity.
# MAGIC
# MAGIC **Tables generated:**
# MAGIC - `orders` — 4,000+ rows with timestamps, statuses, and region data
# MAGIC - `customers` — 500 rows with full demographic profiles
# MAGIC - `products` — 100 rows with categories, prices, and cost data
# MAGIC - `inventory` — product stock levels with reorder thresholds

# COMMAND ----------

print("=" * 60)
print("GENERATING SYNTHETIC E-COMMERCE DATA")
print("=" * 60)

# ── Seed for reproducibility ──
import random
random.seed(42)

# ── Shared reference data ──
CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Books", "Sports & Outdoors", "Health & Beauty"]
STATUSES = ["completed", "pending", "cancelled", "refunded", "shipped"]
REGIONS = ["North", "South", "East", "West", "Central"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
          "San Antonio", "San Diego", "Dallas", "Austin", "Seattle", "Miami"]
STATES = ["NY", "CA", "IL", "TX", "AZ", "PA", "FL", "WA", "GA", "NC"]
STREETS = ["Main St", "Oak Ave", "Elm Rd", "Pine Blvd", "Maple Dr", "Cedar Ln",
           "Birch Ct", "Walnut Way", "Lake Dr", "Hill Rd"]

BASE_DATE = date.today() - timedelta(days=90)

# ── Products (dimension table) ──
product_data = []
for i in range(1, 101):
    cat = random.choice(CATEGORIES)
    cost = round(random.uniform(5.0, 800.0), 2)
    margin = random.uniform(0.15, 0.65)
    price = round(cost * (1 + margin), 2)
    product_data.append((
        f"PROD-{i:04d}", f"Product_{i}",
        cat, cost, price, random.choice([True, False]),
        datetime.now().isoformat()
    ))

products_df = spark.createDataFrame(product_data, [
    "product_id", "product_name", "category", "unit_cost", "unit_price",
    "is_active", "created_at"
])
products_df.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_silver_products")
print(f"  Products: {products_df.count()} rows")

# ── Customers (dimension table) ──
customer_data = []
for i in range(1, 501):
    city = random.choice(CITIES)
    state_map = {"New York": "NY", "Los Angeles": "CA", "Chicago": "IL", "Houston": "TX",
                 "Phoenix": "AZ", "Philadelphia": "PA", "San Antonio": "TX", "San Diego": "CA",
                 "Dallas": "TX", "Austin": "TX", "Seattle": "WA", "Miami": "FL"}
    customer_data.append((
        f"CUST-{i:05d}",
        f"first_{random.randint(1,200)}",
        f"last_{random.randint(1,200)}",
        f"user{i}@example.com",
        f"555-{random.randint(1000,9999)}",
        city,
        state_map.get(city, "XX"),
        f"{random.randint(1,9999)} {random.choice(STREETS)}",
        f"{random.randint(10000,99999)}",
        random.choice(["Gold", "Silver", "Bronze"]),
        datetime.now().isoformat()
    ))

customers_df = spark.createDataFrame(customer_data, [
    "customer_id", "first_name", "last_name", "email", "phone",
    "city", "state", "street_address", "zip_code", "loyalty_tier",
    "created_at"
])
customers_df.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_silver_customers")
print(f"  Customers: {customers_df.count()} rows")

# ── Inventory (fact table) ──
inv_data = []
for i in range(1, 101):
    inv_data.append((
        f"PROD-{i:04d}", random.randint(0, 500), random.randint(20, 100),
        random.randint(5, 50), datetime.now().isoformat()
    ))

inventory_df = spark.createDataFrame(inv_data, [
    "product_id", "quantity_on_hand", "reorder_threshold", "reorder_quantity",
    "last_updated"
])
inventory_df.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_silver_inventory")
print(f"  Inventory: {inventory_df.count()} rows")

# ── Orders (bronze fact table — the heart of the pipeline) ──
order_data = []
for i in range(1, 5001):
    days_ago = random.randint(0, 90)
    order_date = BASE_DATE + timedelta(days=days_ago)
    prod_id = f"PROD-{random.randint(1,100):04d}"
    cust_id = f"CUST-{random.randint(1,500):05d}"
    qty = random.randint(1, 10)
    status = random.choice(STATUSES)
    region = random.choice(REGIONS)

    order_data.append((
        f"ORD-{i:07d}", cust_id, prod_id, qty, order_date.isoformat(),
        status, random.choice(["web", "mobile", "in-store"]),
        random.choice(["credit_card", "paypal", "gift_card", "bank_transfer"]),
        region, datetime.now().isoformat()
    ))

# Introduce some data quality issues intentionally (for silver layer to handle)
# Duplicate — same order_id
order_data.append(("ORD-0000001", "CUST-00050", "PROD-0042", 1, order_data[0][4],
                    "completed", "web", "credit_card", "North", datetime.now().isoformat()))
# Null status
order_data.append(("ORD-0050001", "CUST-00100", "PROD-0050", 3,
                    (BASE_DATE + timedelta(days=5)).isoformat(),
                    None, "mobile", "paypal", "South", datetime.now().isoformat()))
# Future date
order_data.append(("ORD-0050002", "CUST-00200", "PROD-0075", 2,
                    (date.today() + timedelta(days=30)).isoformat(),
                    "pending", "web", "credit_card", "East", datetime.now().isoformat()))
# Negative quantity (edge case)
order_data.append(("ORD-0050003", "CUST-00300", "PROD-0025", -1,
                    BASE_DATE.isoformat(), "cancelled", "in-store", "gift_card",
                    "West", datetime.now().isoformat()))

orders_schema = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("order_date", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("_ingested_at", T.StringType()),
])

orders_df = spark.createDataFrame(order_data, schema=orders_schema)
orders_df.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_bronze_orders")

total_rows = spark.table(f"{DB}.ecom_bronze_orders").count()
print(f"  Orders (Bronze): {total_rows} rows (4 intentional data quality issues included)")
print(f"\nAll 4 source tables created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Generation — Verification
# MAGIC
# MAGIC Let's inspect each source table to confirm the data looks realistic before we begin the pipeline.

# COMMAND ----------

print("=== Products (sample) ===")
spark.table(f"{DB}.ecom_silver_products").select(
    "product_id", "product_name", "category", "unit_cost", "unit_price"
).show(5, truncate=False)

print("=== Customers (sample) ===")
spark.table(f"{DB}.ecom_silver_customers").select(
    "customer_id", "first_name", "last_name", "city", "state", "loyalty_tier"
).show(5, truncate=False)

print("=== Inventory (sample) ===")
spark.table(f"{DB}.ecom_silver_inventory").select(
    "product_id", "quantity_on_hand", "reorder_threshold"
).show(5, truncate=False)

print("=== Orders — Bronze (sample) ===")
spark.table(f"{DB}.ecom_bronze_orders").select(
    "order_id", "customer_id", "product_id", "quantity", "order_date", "status"
).orderBy("order_id").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC ### What You're Building
# MAGIC
# MAGIC The Bronze layer is the **raw landing zone** for all incoming data. It preserves the original format with minimal transformation — effectively an append-only log of everything that arrived.
# MAGIC
# MAGIC ### Why It Matters in Production
# MAGIC
# MAGIC - **Auditability** — You can always replay from raw data if downstream logic changes
# MAGIC - **Schema Evolution** — Source systems change their schemas; Bronze handles this gracefully
# MAGIC - **Reprocessing** — Debug data issues by going back to the original source
# MAGIC
# MAGIC ### Bronze Layer Design
# MAGIC
# MAGIC | Principle | Implementation |
# MAGIC |-----------|---------------|
# MAGIC | Append-only | New data is appended, never updated in place |
# MAGIC | Schema on write | Validate structure at ingestion time |
# MAGIC | Metadata enrichment | Add `_ingested_at`, `_source_file` timestamps |
# MAGIC | No business logic | Keep raw — transformations happen in Silver |

# COMMAND ----------

print("=" * 60)
print("BRONZE LAYER IMPLEMENTATION")
print("=" * 60)

print(f"\nBronze orders table: {DB}.ecom_bronze_orders")
print(f"Row count: {spark.table(f'{DB}.ecom_bronze_orders').count()}")

print("\nSchema:")
spark.table(f"{DB}.ecom_bronze_orders").printSchema()

print("\nData quality snapshot — value distribution:")
spark.table(f"{DB}.ecom_bronze_orders").groupBy("status").count() \
    .orderBy("count", ascending=False).show()

spark.table(f"{DB}.ecom_bronze_orders").groupBy("channel").count() \
    .orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer — Schema Evolution Demo
# MAGIC
# MAGIC What happens when the source system adds a new field? We demonstrate schema evolution by appending a batch of orders that includes a new `discount_code` column.

# COMMAND ----------

new_orders_data = []
for i in range(1, 201):
    order_date = date.today() - timedelta(days=random.randint(0, 3))
    new_orders_data.append((
        f"ORD-NEW-{i:05d}",
        f"CUST-{random.randint(1,500):05d}",
        f"PROD-{random.randint(1,100):04d}",
        random.randint(1, 5),
        order_date.isoformat(),
        random.choice(["completed", "pending", "shipped"]),
        random.choice(["web", "mobile"]),
        random.choice(["credit_card", "paypal"]),
        random.choice(REGIONS),
        datetime.now().isoformat(),
        random.choice(["SAVE10", "FLASH20", None, "VIP30"])  # NEW COLUMN
    ))

new_orders_schema = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("order_date", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("channel", T.StringType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("_ingested_at", T.StringType()),
    T.StructField("discount_code", T.StringType()),  # NEW COLUMN
])

new_orders_df = spark.createDataFrame(new_orders_data, schema=new_orders_schema)

new_orders_df.write.mode("append").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_bronze_orders")

print("After schema evolution (mergeSchema=true):")
spark.table(f"{DB}.ecom_bronze_orders").printSchema()
print(f"\nNew total rows: {spark.table(f'{DB}.ecom_bronze_orders').count()}")
print(f"Rows with discount_code: {spark.table(f'{DB}.ecom_bronze_orders').filter('discount_code IS NOT NULL').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Takeaway — Bronze Layer:** Raw data is ingested as-is with `mergeSchema=true`. New columns are automatically added without breaking existing queries. Old rows get `NULL` for the new column — which is the correct and expected behaviour.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Silver Layer — Cleaned & Validated Data
# MAGIC
# MAGIC ### What You're Building
# MAGIC
# MAGIC The Silver layer takes Bronze data and applies:
# MAGIC - **Deduplication** — Remove duplicate order IDs
# MAGIC - **Type casting** — String dates → proper date types
# MAGIC - **Null handling** — Fill or flag missing values
# MAGIC - **Data quality checks** — Validate business rules
# MAGIC - **Enrichment** — Join with dimension tables for product/customer attributes
# MAGIC
# MAGIC ### Why It Matters in Production
# MAGIC
# MAGIC Analysts and data scientists should never have to clean raw data. Silver provides a trusted, query-ready view that handles all common data quality issues.

# COMMAND ----------

print("=" * 60)
print("SILVER LAYER IMPLEMENTATION")
print("=" * 60)

bronze = spark.table(f"{DB}.ecom_bronze_orders")

# Step 1 — Deduplicate: keep the latest _ingested_at per order_id
window_dedup = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())

silver = bronze \
    .withColumn("_row_num", F.row_number().over(window_dedup)) \
    .filter(F.col("_row_num") == 1) \
    .drop("_row_num")

dedup_count = silver.count()
dup_count = bronze.count() - dedup_count
print(f"Duplicates removed: {dup_count}")
print(f"Unique orders: {dedup_count}")

# Step 2 — Type casting: string dates → proper date types
silver = silver \
    .withColumn("order_date", F.to_date("order_date")) \
    .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))

# Step 3 — Null handling: fill missing status with 'unknown'
silver = silver.fillna({"status": "unknown", "discount_code": "NONE"})

# Step 4 — Flag data quality issues without dropping rows
silver = silver \
    .withColumn("dq_negative_qty", F.when(F.col("quantity") < 0, True).otherwise(False)) \
    .withColumn("dq_future_date",
                F.when(F.col("order_date") > F.current_date(), True).otherwise(False)) \
    .withColumn("dq_null_status",
                F.when(F.col("status") == "unknown", True).otherwise(False))

# Step 5 — Enrich: join with products and customers
products_ref = spark.table(f"{DB}.ecom_silver_products") \
    .select("product_id", "product_name", "category", "unit_cost", "unit_price")

customers_ref = spark.table(f"{DB}.ecom_silver_customers") \
    .select("customer_id", "city", "state", "loyalty_tier")

silver = silver \
    .join(products_ref, "product_id", "left") \
    .join(customers_ref, "customer_id", "left")

# Step 6 — Calculate derived fields
silver = silver \
    .withColumn("line_total", F.col("quantity") * F.col("unit_price")) \
    .withColumn("line_profit", F.col("quantity") * (F.col("unit_price") - F.col("unit_cost")))

# Write Silver layer
silver.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_silver_orders")

print("\nSilver orders schema:")
spark.table(f"{DB}.ecom_silver_orders").printSchema()
print(f"\nSilver row count: {spark.table(f'{DB}.ecom_silver_orders').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer — Data Quality Inspection
# MAGIC
# MAGIC We flagged quality issues instead of dropping rows — this preserves the data for investigation while making problematic rows easy to filter.

# COMMAND ----------

print("=== Data Quality Flags in Silver Layer ===")
silver_tbl = spark.table(f"{DB}.ecom_silver_orders")

print("\nRows with negative quantity:")
silver_tbl.filter("dq_negative_qty = true").select(
    "order_id", "product_id", "quantity", "status"
).show()

print("Rows with future order dates:")
silver_tbl.filter("dq_future_date = true").select(
    "order_id", "order_date"
).show()

print("Rows missing status:")
silver_tbl.filter("dq_null_status = true").select(
    "order_id", "status"
).show()

print("\nDeduplication — checking for remaining duplicates:")
remaining_dupes = silver_tbl.groupBy("order_id").count().filter("count > 1").count()
print(f"  Remaining duplicate order_ids: {remaining_dupes} (expect 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer — Time Travel
# MAGIC
# MAGIC Delta Lake retains history. We can query earlier versions of the Silver table to see how data evolved — critical for auditing and debugging.

# COMMAND ----------

print("=== Time Travel — Querying Historical Versions ===")

# Show version history
versions = spark.sql(f"DESCRIBE HISTORY {DB}.ecom_silver_orders").select("version", "timestamp", "operation").orderBy("version")
versions.show(truncate=False)

# Get version counts for each
for ver_row in versions.collect():
    ver = ver_row["version"]
    try:
        cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DB}.ecom_silver_orders VERSION AS OF {ver}").collect()[0]["cnt"]
        op = ver_row["operation"]
        print(f"  Version {ver} ({op}): {cnt} rows")
    except Exception as e:
        print(f"  Version {ver}: error — {str(e)[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Gold Layer — Business Aggregates
# MAGIC
# MAGIC ### What You're Building
# MAGIC
# MAGIC The Gold layer transforms Silver data into **business-ready aggregates** optimised for dashboards, reports, and ML models:
# MAGIC
# MAGIC 1. **Daily Sales Summary** — Revenue, orders, and profit by day
# MAGIC 2. **Top Products** — Best-selling products by revenue and quantity
# MAGIC 3. **Customer Lifetime Value (CLV)** — Total spend per customer
# MAGIC 4. **Inventory Alerts** — Products below reorder thresholds
# MAGIC 5. **Category Performance** — Revenue and margin by product category and region
# MAGIC
# MAGIC ### Why It Matters in Production
# MAGIC
# MAGIC Gold aggregates are **pre-computed** so dashboards load instantly. Instead of scanning millions of rows every time a VP opens a report, they query a small, optimised aggregate table.

# COMMAND ----------

print("=" * 60)
print("GOLD LAYER — BUSINESS AGGREGATES")
print("=" * 60)

silver_orders = spark.table(f"{DB}.ecom_silver_orders")

# ── Gold #1: Daily Sales Summary ──
print("\n[1/5] Building Gold — Daily Sales Summary...")

daily_sales = silver_orders \
    .groupBy("order_date") \
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.sum("line_total"), 2).alias("total_revenue"),
        F.round(F.sum("line_profit"), 2).alias("total_profit"),
        F.round(F.avg("line_total"), 2).alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers")
    ) \
    .orderBy("order_date")

daily_sales.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_gold_daily_sales")

print("  Daily Sales Summary:")
spark.table(f"{DB}.ecom_gold_daily_sales").orderBy("order_date").show(10)

# ── Gold #2: Top Products ──
print("\n[2/5] Building Gold — Top Products...")

top_products = silver_orders \
    .groupBy("product_id", "product_name", "category") \
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.sum("line_total"), 2).alias("total_revenue"),
        F.round(F.sum("line_profit"), 2).alias("total_profit"),
        F.round(F.avg("unit_price"), 2).alias("avg_selling_price")
    ) \
    .orderBy(F.col("total_revenue").desc())

top_products.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_gold_top_products")

print("  Top 10 Products by Revenue:")
spark.table(f"{DB}.ecom_gold_top_products").show(10, truncate=False)

# ── Gold #3: Customer Lifetime Value ──
print("\n[3/5] Building Gold — Customer Lifetime Value...")

customer_ltv = silver_orders \
    .groupBy("customer_id", "city", "state", "loyalty_tier") \
    .agg(
        F.countDistinct("order_id").alias("lifetime_orders"),
        F.round(F.sum("line_total"), 2).alias("lifetime_revenue"),
        F.round(F.sum("line_profit"), 2).alias("lifetime_profit"),
        F.round(F.avg("line_total"), 2).alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("product_id").alias("unique_products_purchased")
    ) \
    .orderBy(F.col("lifetime_revenue").desc())

customer_ltv.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_gold_customer_ltv")

print("  Top 10 Customers by Lifetime Value:")
spark.table(f"{DB}.ecom_gold_customer_ltv").show(10, truncate=False)

# ── Gold #4: Inventory Alerts ──
print("\n[4/5] Building Gold — Inventory Alerts...")

inventory = spark.table(f"{DB}.ecom_silver_inventory")
products = spark.table(f"{DB}.ecom_silver_products")

# Calculate sales velocity (units sold in last 30 days per product)
thirty_days_ago = date.today() - timedelta(days=30)
sales_velocity = silver_orders \
    .filter(F.col("order_date") >= thirty_days_ago) \
    .filter(F.col("dq_negative_qty") == False) \
    .groupBy("product_id") \
    .agg(
        F.sum("quantity").alias("units_sold_30d"),
        F.round(F.sum("quantity") / 30.0, 1).alias("daily_sell_rate")
    )

inventory_alerts = inventory \
    .join(products.select("product_id", "product_name", "category"), "product_id") \
    .join(sales_velocity, "product_id", "left") \
    .fillna(0, ["units_sold_30d", "daily_sell_rate"]) \
    .withColumn("stock_status",
        F.when(F.col("quantity_on_hand") == 0, "OUT_OF_STOCK")
         .when(F.col("quantity_on_hand") <= F.col("reorder_threshold"), "LOW_STOCK")
         .otherwise("IN_STOCK")
    ) \
    .withColumn("days_until_stockout",
        F.when(F.col("daily_sell_rate") > 0,
               F.round(F.col("quantity_on_hand") / F.col("daily_sell_rate"), 1))
         .otherwise(F.lit(None))
    ) \
    .withColumn("suggested_reorder",
        F.when(F.col("stock_status") != "IN_STOCK", F.col("reorder_quantity"))
         .otherwise(0)
    )

inventory_alerts.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_gold_inventory_alerts")

print("  Inventory Alerts (low/out of stock):")
spark.table(f"{DB}.ecom_gold_inventory_alerts") \
    .filter("stock_status != 'IN_STOCK'") \
    .select("product_id", "product_name", "quantity_on_hand", "reorder_threshold",
            "stock_status", "days_until_stockout", "suggested_reorder") \
    .orderBy("quantity_on_hand").show(10, truncate=False)

# ── Gold #5: Category Performance ──
print("\n[5/5] Building Gold — Category Performance by Region...")

category_perf = silver_orders \
    .groupBy("category", "region") \
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.sum("line_total"), 2).alias("total_revenue"),
        F.round(F.sum("line_profit"), 2).alias("total_profit"),
        F.round(F.sum("line_profit") / F.sum("line_total") * 100, 1).alias("margin_pct")
    ) \
    .orderBy(F.col("total_revenue").desc())

category_perf.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_gold_category_performance")

print("  Category Performance:")
spark.table(f"{DB}.ecom_gold_category_performance").show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer — Summary
# MAGIC
# MAGIC | Gold Table | Purpose | Refresh Cadence |
# MAGIC |------------|---------|----------------|
# MAGIC | `ecom_gold_daily_sales` | Executive dashboard KPIs | Daily |
# MAGIC | `ecom_gold_top_products` | Merchandising decisions | Daily |
# MAGIC | `ecom_gold_customer_ltv` | Marketing segmentation | Weekly |
# MAGIC | `ecom_gold_inventory_alerts` | Supply chain alerts | Hourly |
# MAGIC | `ecom_gold_category_performance` | Category manager reports | Daily |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. SCD Type 2 — Customer Dimension with History
# MAGIC
# MAGIC ### What You're Building
# MAGIC
# MAGIC A **Slowly Changing Dimension Type 2** table for customers. When a customer changes their address, we don't overwrite the old record — we insert a new row with effective dates, preserving the full history. This is critical for accurate historical reporting.
# MAGIC
# MAGIC ### Why It Matters in Production
# MAGIC
# MAGIC Without SCD Type 2, if a customer moves from NY to CA and you join their orders to their *current* address, all historical orders appear to come from CA — distorting regional analysis.

# COMMAND ----------

print("=" * 60)
print("SCD TYPE 2 — CUSTOMER DIMENSION")
print("=" * 60)

# ── Create the SCD2 customer dimension ──
spark.sql(f"DROP TABLE IF EXISTS {DB}.ecom_dim_customer_scd2")

# Initial load from source customers
scd2_init = spark.table(f"{DB}.ecom_silver_customers") \
    .select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email"),
        F.col("phone"),
        F.col("city"),
        F.col("state"),
        F.col("street_address"),
        F.col("zip_code"),
        F.col("loyalty_tier")
    ) \
    .withColumn("eff_start_date", F.current_date()) \
    .withColumn("eff_end_date", F.lit(None).cast("date")) \
    .withColumn("is_current", F.lit(True))

scd2_init.write.mode("overwrite").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_dim_customer_scd2")

print(f"Initial SCD2 dimension: {spark.table(f'{DB}.ecom_dim_customer_scd2').count()} rows")
print("\nSample — all current records:")
spark.table(f"{DB}.ecom_dim_customer_scd2") \
    .filter("is_current = true") \
    .select("customer_id", "first_name", "city", "state", "eff_start_date", "is_current") \
    .show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 2 — Simulating Customer Changes
# MAGIC
# MAGIC We simulate a batch of changes: some customers move to new addresses, some change loyalty tier, and one new customer is added. The SCD2 MERGE will close old records and insert new ones.

# COMMAND ----------

# ── Generate incoming changes ──
changes_data = [
    # Existing customers — address changes
    ("CUST-00001", "first_1", "last_1", "user1@example.com", "555-1111",
     "Boston", "MA", "100 New St", "02101", "Gold"),
    ("CUST-00002", "first_2", "last_2", "user2@example.com", "555-2222",
     "Denver", "CO", "200 New Ave", "80201", "Gold"),
    ("CUST-00050", "first_50", "last_50", "user50@example.com", "555-5050",
     "Portland", "OR", "300 New Rd", "97201", "Platinum"),  # tier upgrade
    # New customer
    ("CUST-01001", "Alice", "NewCustomer", "alice@example.com", "555-9999",
     "Nashville", "TN", "400 Music Row", "37201", "Silver"),
]

changes_schema = T.StructType([
    T.StructField("customer_id", T.StringType()),
    T.StructField("first_name", T.StringType()),
    T.StructField("last_name", T.StringType()),
    T.StructField("email", T.StringType()),
    T.StructField("phone", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("street_address", T.StringType()),
    T.StructField("zip_code", T.StringType()),
    T.StructField("loyalty_tier", T.StringType()),
])

changes_df = spark.createDataFrame(changes_data, schema=changes_schema)
changes_df.createOrReplaceTempView("_scd2_changes")

print("Incoming changes:")
changes_df.select("customer_id", "first_name", "city", "state", "loyalty_tier").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 2 — Production-Grade MERGE Statement
# MAGIC
# MAGIC A single MERGE handles: (1) closing old records for changed customers, (2) inserting new current records, and (3) inserting brand-new customers.

# COMMAND ----------

spark.sql(f"""
    MERGE INTO {DB}.ecom_dim_customer_scd2 AS target
    USING _scd2_changes AS source
    ON target.customer_id = source.customer_id AND target.is_current = true

    WHEN MATCHED AND (
        target.city <> source.city OR
        target.state <> source.state OR
        target.street_address <> source.street_address OR
        target.zip_code <> source.zip_code OR
        target.loyalty_tier <> source.loyalty_tier OR
        target.email <> source.email OR
        target.phone <> source.phone
    ) THEN
        UPDATE SET
            eff_end_date = current_date(),
            is_current = false

    WHEN NOT MATCHED THEN
        INSERT (customer_id, first_name, last_name, email, phone,
                city, state, street_address, zip_code, loyalty_tier,
                eff_start_date, eff_end_date, is_current)
        VALUES (source.customer_id, source.first_name, source.last_name,
                source.email, source.phone,
                source.city, source.state, source.street_address,
                source.zip_code, source.loyalty_tier,
                current_date(), NULL, true)
""")

# Step 2 — Insert new current records for the updated customers
spark.sql(f"""
    INSERT INTO {DB}.ecom_dim_customer_scd2
    SELECT
        s.customer_id, s.first_name, s.last_name, s.email, s.phone,
        s.city, s.state, s.street_address, s.zip_code, s.loyalty_tier,
        current_date() AS eff_start_date,
        NULL AS eff_end_date,
        true AS is_current
    FROM _scd2_changes s
    WHERE EXISTS (
        SELECT 1 FROM {DB}.ecom_dim_customer_scd2 t
        WHERE t.customer_id = s.customer_id
          AND t.eff_end_date = current_date()
          AND t.is_current = false
    )
""")

scd2_total = spark.table(f"{DB}.ecom_dim_customer_scd2").count()
scd2_current = spark.table(f"{DB}.ecom_dim_customer_scd2").filter("is_current = true").count()
scd2_historical = scd2_total - scd2_current

print(f"SCD2 Total rows: {scd2_total}")
print(f"  Current: {scd2_current}")
print(f"  Historical: {scd2_historical}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 2 — Querying Current vs. Historical Views

# COMMAND ----------

print("=== Current Customer Records (is_current = true) ===")
spark.table(f"{DB}.ecom_dim_customer_scd2") \
    .filter("is_current = true") \
    .select("customer_id", "city", "state", "loyalty_tier", "eff_start_date", "eff_end_date") \
    .show(10, truncate=False)

print("\n=== Customers with History (more than 1 row) ===")
spark.table(f"{DB}.ecom_dim_customer_scd2") \
    .groupBy("customer_id").count() \
    .filter("count > 1") \
    .join(spark.table(f"{DB}.ecom_dim_customer_scd2"), "customer_id") \
    .select("customer_id", "city", "state", "loyalty_tier",
            "eff_start_date", "eff_end_date", "is_current") \
    .orderBy("customer_id", "eff_start_date") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 2 — Point-in-Time Query
# MAGIC
# MAGIC "What was customer CUST-00001's address 60 days ago?" — This query answers that by filtering on effective date ranges.

# COMMAND ----------

point_in_time = date.today() - timedelta(days=60)
point_str = point_in_time.isoformat()

result = spark.sql(f"""
    SELECT customer_id, first_name, last_name, city, state,
           eff_start_date, eff_end_date, is_current
    FROM {DB}.ecom_dim_customer_scd2
    WHERE customer_id = 'CUST-00001'
      AND eff_start_date <= '{point_str}'
      AND (eff_end_date IS NULL OR eff_end_date > '{point_str}')
""")

print(f"Customer CUST-00001 as of {point_str}:")
result.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Takeaway — SCD Type 2:** A single MERGE + INSERT pattern tracks full history. Point-in-time queries use `eff_start_date` and `eff_end_date` to reconstruct the customer's state at any historical moment. This is essential for accurate cohort analysis and regional reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Incremental Processing with CDF
# MAGIC
# MAGIC ### What You're Building
# MAGIC
# MAGIC Change Data Feed (CDF) captures row-level changes (inserts, updates, deletes) from a Delta table. Instead of reprocessing the entire Bronze layer every time, we only process **what changed** since the last pipeline run — dramatically reducing compute cost and latency.
# MAGIC
# MAGIC ### Why It Matters in Production
# MAGIC
# MAGIC Processing 100M rows daily because 10K changed is wasteful and slow. CDF enables true incremental pipelines: read only the delta, apply transformations, and merge into downstream tables.

# COMMAND ----------

print("=" * 60)
print("INCREMENTAL PROCESSING WITH CDF")
print("=" * 60)

# ── Create a fresh Bronze table with CDF enabled ──
spark.sql(f"DROP TABLE IF EXISTS {DB}.ecom_bronze_orders_cdf")

# Simulate "Day 1" data — first batch of orders
day1 = spark.table(f"{DB}.ecom_bronze_orders") \
    .filter("order_id < 'ORD-0001001'") \
    .drop("discount_code")

day1.write.mode("overwrite").format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable(f"{DB}.ecom_bronze_orders_cdf")

print(f"Day 1 orders loaded: {spark.table(f'{DB}.ecom_bronze_orders_cdf').count()}")

# ── Create Silver CDF target ──
spark.sql(f"DROP TABLE IF EXISTS {DB}.ecom_silver_orders_cdf")

day1_silver = day1 \
    .withColumn("order_date", F.to_date("order_date")) \
    .withColumn("_ingested_at", F.to_timestamp("_ingested_at")) \
    .fillna({"status": "unknown"})

day1_silver.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{DB}.ecom_silver_orders_cdf")

print(f"Silver CDF target initialised: {spark.table(f'{DB}.ecom_silver_orders_cdf').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDF — Read Change Data Feed
# MAGIC
# MAGIC We can read CDF from any starting version to get only the rows that changed. This is how incremental pipelines work: the pipeline records its last processed version, then reads CDF from that version onward.

# COMMAND ----------

# Step 1 — Record the current version (this is what a pipeline would checkpoint)
current_version = spark.sql(f"DESCRIBE HISTORY {DB}.ecom_bronze_orders_cdf") \
    .select(F.max("version").alias("max_ver")).collect()[0]["max_ver"]
print(f"Last processed version: {current_version}")

# Step 2 — Simulate "Day 2" data arrival
day2 = spark.table(f"{DB}.ecom_bronze_orders") \
    .filter("order_id >= 'ORD-0001001' AND order_id < 'ORD-0002001'") \
    .drop("discount_code")

day2.write.mode("append").format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{DB}.ecom_bronze_orders_cdf")

print(f"Day 2 data appended. New total: {spark.table(f'{DB}.ecom_bronze_orders_cdf').count()}")

# Step 3 — Read CDF: only the changes since last processed version
changes_since = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", current_version)
    .table(f"{DB}.ecom_bronze_orders_cdf")
    .filter("_change_type != 'update_preimage'")
)

change_count = changes_since.count()
print(f"\nCDF changes detected: {change_count} rows")

print("\nChange types distribution:")
changes_since.groupBy("_change_type").count().show()

print("\nSample CDF rows (with change metadata):")
changes_since.select(
    "_change_type", "_commit_version", "_commit_timestamp",
    "order_id", "customer_id", "product_id", "quantity"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDF — Merge Changes into Silver (Incremental)
# MAGIC
# MAGIC Apply Silver transformations to **only the changed rows**, then MERGE into the Silver CDF table. This is a true incremental pipeline — we process 1,000 rows instead of 5,000.

# COMMAND ----------

# Transform only changed rows (same Silver logic)
silver_changes = changes_since \
    .withColumn("order_date", F.to_date("order_date")) \
    .withColumn("_ingested_at", F.to_timestamp("_ingested_at")) \
    .fillna({"status": "unknown"}) \
    .select(
        "order_id", "customer_id", "product_id", "quantity",
        "order_date", "status", "channel", "payment_method", "region",
        "_ingested_at", "_change_type"
    )

silver_changes.createOrReplaceTempView("_silver_changes")

# Upsert into Silver target
spark.sql(f"""
    MERGE INTO {DB}.ecom_silver_orders_cdf AS target
    USING _silver_changes AS source
    ON target.order_id = source.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Silver CDF table after incremental merge: {spark.table(f'{DB}.ecom_silver_orders_cdf').count()} rows")

# Verify: Day 2 orders are now in Silver
day2_in_silver = spark.table(f"{DB}.ecom_silver_orders_cdf") \
    .filter("order_id >= 'ORD-0001001' AND order_id < 'ORD-0002001'").count()
print(f"Day 2 orders in Silver: {day2_in_silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDF — Ongoing Incremental Pipeline Pattern
# MAGIC
# MAGIC Here's the production pattern in pseudo-code:
# MAGIC
# MAGIC ```python
# MAGIC def incremental_pipeline():
# MAGIC     last_version = get_checkpoint("bronze_version")
# MAGIC     changes = spark.read.format("delta")
# MAGIC         .option("readChangeFeed", "true")
# MAGIC         .option("startingVersion", last_version)
# MAGIC         .table("bronze_table")
# MAGIC         .filter("_change_type != 'update_preimage'")
# MAGIC
# MAGIC     silver_updates = transform(changes)   # clean, enrich, validate
# MAGIC     silver_updates.merge_into("silver_table", key="order_id")
# MAGIC     save_checkpoint("bronze_version", get_latest_version())
# MAGIC ```
# MAGIC
# MAGIC **Key Takeaway — Incremental Processing:** CDF decouples the pipeline from full table scans. You process only what changed, when it changed. This is the foundation of cost-efficient, low-latency data pipelines in production.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Data Quality Monitoring
# MAGIC
# MAGIC ### What You're Building
# MAGIC
# MAGIC A data quality monitoring framework that runs a suite of validation checks against the Silver layer and records pass/fail results with timestamps. This creates an audit trail of data quality over time.
# MAGIC
# MAGIC ### Why It Matters in Production
# MAGIC
# MAGIC Bad data in, bad decisions out. Automated quality checks catch issues before they reach dashboards and ML models. The results table enables trending analysis — "are our quality scores improving or degrading over time?"

# COMMAND ----------

print("=" * 60)
print("DATA QUALITY MONITORING")
print("=" * 60)

spark.sql(f"DROP TABLE IF EXISTS {DB}.ecom_dq_results")
silver = spark.table(f"{DB}.ecom_silver_orders")
total_rows = silver.count()
run_ts = datetime.now().isoformat()

def run_dq_check(check_name, condition, severity="ERROR"):
    """Execute a single DQ check and return (check_name, passed, failing_rows, severity)."""
    failing = silver.filter(~condition).count()
    return (check_name, failing == 0, failing, severity)

dq_checks = [
    # Completeness checks
    ("order_id_not_null",         F.col("order_id").isNotNull(), "ERROR"),
    ("customer_id_not_null",      F.col("customer_id").isNotNull(), "ERROR"),
    ("product_id_not_null",       F.col("product_id").isNotNull(), "ERROR"),
    ("status_not_null",           F.col("status").isNotNull(), "ERROR"),
    ("order_date_not_null",       F.col("order_date").isNotNull(), "ERROR"),
    # Validity checks
    ("quantity_positive",         F.col("quantity") > 0, "ERROR"),
    ("order_date_not_future",     F.col("order_date") <= F.current_date(), "WARN"),
    ("status_valid",              F.col("status").isin("completed", "pending", "cancelled",
                                                       "refunded", "shipped", "unknown"), "WARN"),
    # Referential integrity
    ("product_exists",            F.col("product_name").isNotNull(), "ERROR"),
    ("customer_exists",           F.col("city").isNotNull(), "WARN"),
    # Business rule checks
    ("line_total_positive",       F.col("line_total") > 0, "WARN"),
    ("line_profit_reasonable",    (F.col("line_profit") >= -1000) & (F.col("line_profit") <= 100000), "WARN"),
]

results = []
for name, condition, severity in dq_checks:
    check_name, passed, failing_rows, sev = run_dq_check(name, condition, severity)
    pct = round((total_rows - failing_rows) / total_rows * 100, 2) if total_rows > 0 else 100.0
    results.append((run_ts, check_name, passed, failing_rows, total_rows, pct, sev))

dq_results_df = spark.createDataFrame(results, [
    "check_timestamp", "check_name", "passed", "failing_rows",
    "total_rows", "pass_pct", "severity"
])

dq_results_df.write.mode("overwrite").format("delta") \
    .saveAsTable(f"{DB}.ecom_dq_results")

print(f"Data Quality Results ({len(results)} checks executed):")
dq_results_df.orderBy(F.col("severity").asc(), F.col("pass_pct").asc()).show(20, truncate=False)

# Summary
passed = dq_results_df.filter("passed = true").count()
failed = dq_results_df.filter("passed = false").count()
errors = dq_results_df.filter("passed = false AND severity = 'ERROR'").count()
warns = dq_results_df.filter("passed = false AND severity = 'WARN'").count()

print(f"\nSummary: {passed}/{len(results)} checks PASSED | "
      f"{failed} FAILED ({errors} errors, {warns} warnings)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ Monitoring — Detailed Failure Drill-Down
# MAGIC
# MAGIC For any failing check, we can drill into the specific rows that caused the failure. This is how data engineers triage quality issues in production.

# COMMAND ----------

print("=== Rows Failing Key Quality Checks ===")

print("\n[ERROR] Negative quantities:")
silver.filter("quantity <= 0") \
    .select("order_id", "product_id", "quantity", "status").show(truncate=False)

print("[WARN] Future order dates:")
silver.filter("order_date > current_date()") \
    .select("order_id", "order_date").show(truncate=False)

print("[WARN] Missing product names (referential integrity):")
silver.filter("product_name IS NULL") \
    .select("order_id", "product_id", "product_name").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Takeaway — DQ Monitoring:** Production pipelines should run quality checks after every batch. Store results in a time-series table to track quality trends. Set up alerts when ERROR-level checks fail. The pattern scales: add new checks as business rules evolve without changing the pipeline core.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Performance Optimization — OPTIMIZE & ZORDER
# MAGIC
# MAGIC ### What You're Building
# MAGIC
# MAGIC Apply Delta Lake's built-in optimization commands to compact small files and co-locate related data for faster queries. We'll measure the impact with before/after comparisons.
# MAGIC
# MAGIC ### Why It Matters in Production
# MAGIC
# MAGIC As data accumulates, Delta tables accumulate many small files from frequent appends. This slows down reads because Spark must open and scan many files. OPTIMIZE compacts them into larger files; ZORDER co-locates data that is queried together.

# COMMAND ----------

print("=" * 60)
print("PERFORMANCE OPTIMIZATION")
print("=" * 60)

target_table = f"{DB}.ecom_silver_orders"

# ── Before: collect table statistics ──
before_detail = spark.sql(f"DESCRIBE DETAIL {target_table}").select(
    "numFiles", "sizeInBytes"
).collect()[0]

before_files = before_detail["numFiles"]
before_size_mb = round(before_detail["sizeInBytes"] / (1024 * 1024), 2)

print(f"Before OPTIMIZE:")
print(f"  Files: {before_files}")
print(f"  Size:  {before_size_mb} MB")

# ── Run OPTIMIZE with ZORDER ──
print("\nRunning OPTIMIZE with ZORDER on (order_date, customer_id)...")
optimize_result = spark.sql(f"""
    OPTIMIZE {target_table}
    ZORDER BY (order_date, customer_id)
""")

opt_files_added = optimize_result.select("metrics").collect()[0][0]
print(f"OPTIMIZE result: {opt_files_added}")

# ── After: collect updated statistics ──
after_detail = spark.sql(f"DESCRIBE DETAIL {target_table}").select(
    "numFiles", "sizeInBytes"
).collect()[0]

after_files = after_detail["numFiles"]
after_size_mb = round(after_detail["sizeInBytes"] / (1024 * 1024), 2)

print(f"\nAfter OPTIMIZE:")
print(f"  Files: {after_files}")
print(f"  Size:  {after_size_mb} MB")
print(f"\nFile reduction: {before_files} → {after_files} "
      f"({round((1 - after_files/before_files) * 100, 1)}% reduction)")

# ── Optimize Gold tables too ──
print("\nOptimizing Gold tables...")
for gold_tbl in [f"{DB}.ecom_gold_daily_sales", f"{DB}.ecom_gold_customer_ltv",
                 f"{DB}.ecom_gold_category_performance"]:
    try:
        spark.sql(f"OPTIMIZE {gold_tbl}")
        print(f"  ✓ {gold_tbl}")
    except Exception as e:
        print(f"  ⚠ {gold_tbl}: {str(e)[:60]}")

# ── Also ZORDER the top products table ──
spark.sql(f"OPTIMIZE {DB}.ecom_gold_top_products ZORDER BY (category)")
print(f"  ✓ {DB}.ecom_gold_top_products (ZORDER by category)")

print("\nAll optimizations complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE — When to Run in Production
# MAGIC
# MAGIC | Pattern | Recommendation |
# MAGIC |---------|---------------|
# MAGIC | Bronze (append-heavy) | Run daily during low-traffic windows |
# MAGIC | Silver (MERGE-heavy) | Run after each batch if >100 small files |
# MAGIC | Gold (overwrite) | Not needed — full overwrite produces ideal files |
# MAGIC | ZORDER columns | Pick 1-2 columns you frequently filter on (dates, IDs) |
# MAGIC
# MAGIC **Key Takeaway:** OPTIMIZE is not free — it rewrites data. Schedule it during maintenance windows. ZORDER on `order_date` makes date-range queries dramatically faster because related data is physically co-located.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Summary & Production Considerations
# MAGIC
# MAGIC ### What We Built
# MAGIC
# MAGIC | Layer | Table | Purpose | Key Techniques |
# MAGIC |-------|-------|---------|---------------|
# MAGIC | **Bronze** | `ecom_bronze_orders` | Raw ingestion | Append-only, schema evolution, `mergeSchema` |
# MAGIC | **Silver** | `ecom_silver_orders` | Cleaned & enriched | Dedup, type casting, null handling, DQ flags, joins |
# MAGIC | **Silver** | `ecom_dim_customer_scd2` | Customer dimension | SCD Type 2, effective dates, point-in-time queries |
# MAGIC | **Gold** | `ecom_gold_daily_sales` | Daily KPIs | Aggregation, window functions |
# MAGIC | **Gold** | `ecom_gold_top_products` | Product ranking | Ranked aggregation, revenue/profit analysis |
# MAGIC | **Gold** | `ecom_gold_customer_ltv` | Customer LTV | Lifetime value, segmentation attributes |
# MAGIC | **Gold** | `ecom_gold_inventory_alerts` | Stock alerts | Sales velocity, days-until-stockout, reorder logic |
# MAGIC | **Gold** | `ecom_gold_category_performance` | Category analytics | Multi-dim aggregation, margin analysis |
# MAGIC | **Ops** | `ecom_dq_results` | Quality monitoring | Pass/fail framework, trendable results |
# MAGIC
# MAGIC ### Concepts Demonstrated
# MAGIC
# MAGIC | Concept | Where Used |
# MAGIC |---------|-----------|
# MAGIC | ACID Transactions | All table writes use atomic Delta commits |
# MAGIC | Schema Enforcement & Evolution | Bronze layer `mergeSchema`, Silver validates types |
# MAGIC | Time Travel | Silver layer version history queries |
# MAGIC | OPTIMIZE & ZORDER | Section 9 — file compaction and data co-location |
# MAGIC | CDF (Change Data Feed) | Section 7 — incremental pipeline |
# MAGIC | Medallion Architecture | Bronze → Silver → Gold three-layer design |
# MAGIC | SCD Type 2 | Section 6 — customer address history |
# MAGIC | Incremental Processing | CDF-based MERGE, only processing changed rows |
# MAGIC | MERGE INTO | Silver dedup, SCD2 updates, CDF upserts |
# MAGIC | Window Functions | Deduplication, ranking in Gold aggregates |
# MAGIC | Data Quality Framework | Section 8 — automated pass/fail checks |
# MAGIC
# MAGIC ### Production Readiness Checklist
# MAGIC
# MAGIC - [ ] **Scheduling** — Wrap each layer in a Databricks Workflow with dependencies (Bronze → Silver → Gold)
# MAGIC - [ ] **Alerting** — Set up alerts on `ecom_dq_results` for ERROR-level failures
# MAGIC - [ ] **Retention** — Configure VACUUM policies on Bronze/Silver (retain 7 days for time travel, 30 days for audit)
# MAGIC - [ ] **Partitioning** — For tables >1TB, consider partitioning `ecom_bronze_orders` by `order_date`
# MAGIC - [ ] **Unity Catalog** — Move tables to a catalog (e.g. `ecom_lakehouse`) with proper grants
# MAGIC - [ ] **Testing** — Add unit tests for transformation logic using `chispa` or Databricks' built-in test framework
# MAGIC - [ ] **Monitoring** — Track pipeline latency, row counts, and DQ scores in a dashboard
# MAGIC - [ ] **Disaster Recovery** — Use Deep Clone to create independent copies for DR scenarios
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Add Streaming** — Replace batch ingestion with Structured Streaming + Auto Loader for real-time orders
# MAGIC 2. **ML Integration** — Build a customer churn model on top of `ecom_gold_customer_ltv`
# MAGIC 3. **Serving Layer** — Publish Gold aggregates to a SQL Warehouse for BI tools (Tableau, Power BI)
# MAGIC 4. **Data Contracts** — Define schemas with Delta constraints (NOT NULL, CHECK) to enforce quality at write time
# MAGIC 5. **Cross-Domain** — Extend with marketing campaign data, shipping logistics, and returns processing
# MAGIC
# MAGIC ---
# MAGIC *End of Project — E-Commerce Lakehouse*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Verification — All Tables Summary

# COMMAND ----------

print("=" * 60)
print("FINAL VERIFICATION — ALL TABLES")
print("=" * 60)

all_final = [
    ("BRONZE", f"{DB}.ecom_bronze_orders"),
    ("SILVER", f"{DB}.ecom_silver_orders"),
    ("SILVER", f"{DB}.ecom_silver_customers"),
    ("SILVER", f"{DB}.ecom_silver_products"),
    ("SILVER", f"{DB}.ecom_silver_inventory"),
    ("SILVER (SCD2)", f"{DB}.ecom_dim_customer_scd2"),
    ("GOLD", f"{DB}.ecom_gold_daily_sales"),
    ("GOLD", f"{DB}.ecom_gold_top_products"),
    ("GOLD", f"{DB}.ecom_gold_customer_ltv"),
    ("GOLD", f"{DB}.ecom_gold_inventory_alerts"),
    ("GOLD", f"{DB}.ecom_gold_category_performance"),
    ("OPS (CDF)", f"{DB}.ecom_bronze_orders_cdf"),
    ("OPS (CDF)", f"{DB}.ecom_silver_orders_cdf"),
    ("OPS (DQ)", f"{DB}.ecom_dq_results"),
]

print(f"{'Layer':<20} {'Table':<45} {'Rows':<10} {'Format'}")
print("-" * 90)
for layer, tbl in all_final:
    try:
        cnt = spark.table(tbl).count()
        detail = spark.sql(f"DESCRIBE DETAIL {tbl}").select("format").collect()[0]["format"]
        print(f"{layer:<20} {tbl:<45} {cnt:<10} {detail}")
    except Exception as e:
        print(f"{layer:<20} {tbl:<45} {'ERROR':<10} {str(e)[:40]}")

print("-" * 90)
print("\nAll tables validated. Pipeline complete.")
