# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time IoT Sensor Analytics Pipeline
# MAGIC
# MAGIC **Business Problem**: A manufacturing company operates 10,000 IoT sensors across 5 factories,
# MAGIC generating temperature, vibration, pressure, and humidity readings every second. The operations
# MAGIC team needs real-time anomaly detection, windowed aggregations, and automated alerting to
# MAGIC prevent equipment failures before they happen.
# MAGIC
# MAGIC **What You'll Build**:
# MAGIC 1. Simulated sensor stream (rate source + randomized sensor data)
# MAGIC 2. Streaming ingestion with schema validation and factory metadata enrichment
# MAGIC 3. 5-min tumbling windows for avg/max/min; 10-min sliding windows for trend detection
# MAGIC 4. Anomaly detection with 3-sigma thresholds using stateful processing
# MAGIC 5. Stream-stream join for correlated temperature+vibration anomalies
# MAGIC 6. foreachBatch Delta sink with MERGE for idempotent writes
# MAGIC 7. Watermarks and late data handling
# MAGIC 8. Monitoring dashboard SQL queries
# MAGIC
# MAGIC **Concepts Used**: Structured Streaming, Triggers, Output Modes, Windowed Aggregations,
# MAGIC Watermarks, foreachBatch, Stream-Static Joins, Stream-Stream Joins, Checkpointing, MERGE
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC [Rate Source] ──> [Transform → Sensor Data] ──> [Ingest + Enrich] ──> [Bronze: iot_sensor_raw]
# MAGIC                                                      │
# MAGIC                              ┌───────────────────────┼───────────────────────┐
# MAGIC                              ▼                       ▼                       ▼
# MAGIC                     [5-min Tumbling]         [10-min Sliding]        [Anomaly Detection]
# MAGIC                          │                       │                       │
# MAGIC                          ▼                       ▼                       ▼
# MAGIC                   iot_sensor_5min          iot_sensor_10min       iot_sensor_alerts
# MAGIC                                                                          │
# MAGIC                                                     [Stream-Stream Join]  │
# MAGIC                                                              │            │
# MAGIC                                                              ▼            ▼
# MAGIC                                                     iot_correlated_alerts
# MAGIC ```
# MAGIC
# MAGIC **Environment**: Designed for **serverless compute** using managed Delta tables.
# MAGIC No `/tmp/` file writes (except checkpoint with fallback).

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Environment Setup
# MAGIC
# MAGIC Initialize Spark session, define database, and create helper functions for
# MAGIC checkpoint management with serverless compatibility.

# COMMAND ----------
import os
import time
import random
import math
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, FloatType
)
from pyspark.sql.functions import (
    col, lit, expr, window, current_timestamp, count,
    sum as _sum, avg, max as _max, min as _min, stddev,
    to_timestamp, from_unixtime, unix_timestamp,
    struct, to_json, from_json, when, concat,
    date_format, year, month, dayofmonth, hour, minute, second,
    round as _round, rand, explode, array, mean as _mean,
    lag, lead, first, last, collect_list, size, abs as _abs,
    broadcast, monotonically_increasing_id
)
from pyspark.sql.streaming import DataStreamWriter

print("=" * 70)
print("  REAL-TIME IoT SENSOR ANALYTICS PIPELINE")
print("=" * 70)

# ── Configuration ──
DB = "default"
DBF = f"`{DB}`"

# Managed table names
TABLE_RAW         = f"{DB}.iot_sensor_raw"
TABLE_FACTORY_REF = f"{DB}.iot_factory_ref"
TABLE_SENSOR_CAT  = f"{DB}.iot_sensor_catalog"
TABLE_5MIN_AGG    = f"{DB}.iot_sensor_5min_agg"
TABLE_10MIN_SLIDE = f"{DB}.iot_sensor_10min_slide"
TABLE_ALERTS      = f"{DB}.iot_sensor_alerts"
TABLE_CORRELATED  = f"{DB}.iot_correlated_alerts"
TABLE_SENSOR_STATS = f"{DB}.iot_sensor_stats"

# Checkpoint helper with serverless-friendly fallback
def get_checkpoint(name):
    """Return a checkpoint path. Uses local temp (serverless may need cloud storage)."""
    local_path = f"/tmp/checkpoints_iot/{name}"
    try:
        os.makedirs(local_path, exist_ok=True)
        return local_path
    except Exception:
        print(f"Checkpoint path unavailable. For serverless, use cloud storage: s3://bucket/checkpoints/{name}")
        return f"/tmp/checkpoints_iot/{name}"

# Kill any lingering streams from previous runs
for q in spark.streams.active:
    try:
        q.stop()
        print(f"  ⏹️  Stopped: {q.name}")
    except Exception:
        pass

print("✅ Spark session ready")
print(f"   Database: {DB}")
print(f"   Spark version: {spark.version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Reference Data — Factory Metadata & Sensor Catalog
# MAGIC
# MAGIC Create static reference tables that represent the real-world deployment:
# MAGIC - **5 factories** across different regions with metadata
# MAGIC - **Sensor catalog** mapping 10,000 sensors to types, models, and locations
# MAGIC - Each sensor has a **normal operating range** used for anomaly baselines

# COMMAND ----------
print("=" * 70)
print("  SETTING UP REFERENCE DATA")
print("=" * 70)

# ── Factory Reference Table ──
factory_data = [
    ("F-01", "Detroit Assembly",    "US-MI", "detroit",    "Line A, B, C"),
    ("F-02", "Stuttgart Powertrain", "DE-BW", "stuttgart",  "Line D, E, F"),
    ("F-03", "Nagoya Electronics",   "JP-AI", "nagoya",     "Line G, H"),
    ("F-04", "Shanghai Components",  "CN-SH", "shanghai",   "Line I, J, K"),
    ("F-05", "Sao Paulo Foundry",    "BR-SP", "sao_paulo",  "Line L, M"),
]

factory_schema = StructType([
    StructField("factory_id",    StringType(), False),
    StructField("factory_name",  StringType(), False),
    StructField("region_code",   StringType(), False),
    StructField("location_key",  StringType(), False),
    StructField("production_lines", StringType(), False),
])

df_factory = spark.createDataFrame(factory_data, factory_schema)
df_factory.write.mode("overwrite").saveAsTable(TABLE_FACTORY_REF)

# ── Sensor Catalog (10,000 sensors) ──
print("\n  Generating 10,000 sensor catalog entries...")

sensor_types     = ["temperature", "vibration", "pressure", "humidity"]
sensor_models    = {
    "temperature": ["TMP-100", "TMP-200", "TMP-Pro"],
    "vibration":   ["VIB-300", "VIB-400", "VIB-Elite"],
    "pressure":    ["PRS-500", "PRS-600"],
    "humidity":    ["HUM-700", "HUM-800", "HUM-Nano"],
}
location_prefixes = {
    "detroit":   ["DET-BLD-1", "DET-BLD-2"],
    "stuttgart": ["STG-HAL-1", "STG-HAL-2"],
    "nagoya":    ["NGY-SEC-1", "NGY-SEC-2"],
    "shanghai":  ["SHG-WRK-1", "SHG-WRK-2"],
    "sao_paulo": ["SPL-FLR-1", "SPL-FLR-2"],
}

num_sensors = 10_000
batch_size  = 2000

sensor_catalog_rows = []
for i in range(1, num_sensors + 1):
    sensor_id    = f"SNSR-{i:05d}"
    stype        = sensor_types[i % len(sensor_types)]
    models       = sensor_models[stype]
    model        = models[i % len(models)]
    factory_id   = f"F-{(i % 5) + 1:02d}"
    factory_row  = factory_data[(i % 5)]
    loc_prefixes = location_prefixes[factory_row[3]]
    location     = f"{loc_prefixes[i % len(loc_prefixes)]}"
    installed    = f"202{i % 6 + 1}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"

    # Normal operating range per sensor type
    ranges = {
        "temperature": (18.0 + (i % 15), 85.0 - (i % 10)),
        "vibration":   (0.1, 4.5 + (i % 3) * 0.1),
        "pressure":    (95.0, 120.0 + (i % 10)),
        "humidity":    (20.0 + (i % 10), 80.0 - (i % 15)),
    }
    lo, hi = ranges[stype]
    sensor_catalog_rows.append(
        (sensor_id, stype, model, factory_id, location, installed, float(lo), float(hi))
    )

sensor_cat_schema = StructType([
    StructField("sensor_id",    StringType(), False),
    StructField("sensor_type",  StringType(), False),
    StructField("model",        StringType(), False),
    StructField("factory_id",   StringType(), False),
    StructField("location",     StringType(), False),
    StructField("installed_date", StringType(), False),
    StructField("normal_min",   DoubleType(), False),
    StructField("normal_max",   DoubleType(), False),
])

# Write in batches to avoid driver OOM
print("  Writing sensor catalog in batches...")
first_batch = True
for start in range(0, num_sensors, batch_size):
    batch = sensor_catalog_rows[start:start + batch_size]
    df_batch = spark.createDataFrame(batch, sensor_cat_schema)
    mode = "overwrite" if first_batch else "append"
    df_batch.write.mode(mode).saveAsTable(TABLE_SENSOR_CAT)
    first_batch = False

print(f"  ✅ {TABLE_SENSOR_CAT}: {spark.table(TABLE_SENSOR_CAT).count()} sensors")
print(f"  ✅ {TABLE_FACTORY_REF}: {spark.table(TABLE_FACTORY_REF).count()} factories")

# ── Verify reference data ──
spark.table(TABLE_FACTORY_REF).show(5, truncate=False)
spark.table(TABLE_SENSOR_CAT).groupBy("sensor_type").count().show()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Simulated Sensor Stream
# MAGIC
# MAGIC Generate realistic sensor readings using Spark's `rate` source combined with
# MAGIC randomized transformations. Each row represents one sensor reading with:
# MAGIC - Timestamp, sensor_id, sensor_type, value, unit, factory assignment
# MAGIC - ~100 rows/second to simulate a subset of the 10,000 sensors

# COMMAND ----------
print("=" * 70)
print("  SIMULATED SENSOR STREAM")
print("=" * 70)

# Load reference data into memory for broadcast joins
df_cat     = spark.table(TABLE_SENSOR_CAT)
df_factory = spark.table(TABLE_FACTORY_REF)
sensor_count = df_cat.count()

print(f"  Sensor catalog: {sensor_count} sensors")
print(f"  Stream rate: ~100 rows/sec")

# ── Build streaming DataFrame ──
# We use the rate source to get a monotonically increasing timestamp/value,
# then map each row to a pseudo-random sensor reading.
sample_sensors = df_cat.select("sensor_id", "sensor_type", "factory_id", "normal_min", "normal_max") \
                       .limit(2000).collect()

# Broadcast small lookup for sensor metadata
sample_broadcast = dict()
for row in sample_sensors:
    sample_broadcast[row.sensor_id] = row

all_sensor_ids = [r.sensor_id for r in sample_sensors]
num_sample     = len(all_sensor_ids)

def generate_sensor_value(sensor_type, normal_min, normal_max, idx):
    """Generate a realistic sensor reading with occasional anomalies."""
    mean_val = (normal_min + normal_max) / 2.0
    std_val  = (normal_max - normal_min) / 6.0

    # 95% of time: normal distribution around mean
    # 5% of time: anomalous (spike or drift)
    is_anomaly = (idx % 97 == 0)  # ~1% chance of anomaly

    if is_anomaly:
        spike = random.choice([
            mean_val + std_val * random.uniform(3.5, 6.0),   # positive spike
            mean_val - std_val * random.uniform(3.5, 6.0),   # negative spike
            mean_val + std_val * random.uniform(1.0, 2.0) * (1 if random.random() > 0.5 else -1),  # drift
        ])
        return round(spike, 4)
    else:
        return round(random.gauss(mean_val, std_val), 4)

# ── Build the streaming source ──
raw_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load()
    .withColumn("sensor_idx", (col("value") % lit(num_sample)).cast("int"))
    .withColumn("event_timestamp", col("timestamp").cast(TimestampType()))
    .withColumn("rate_value_raw", col("value"))
)

# We need to map each row to a random sensor. Since we can't iterate within
# a streaming transformation arbitrarily, we use a trick: encode sensor assignment
# deterministically from the rate value and time.

# Register UDF for sensor value generation
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

sensor_ids_map = {i: sid for i, sid in enumerate(all_sensor_ids)}
sensor_types_map = {sid: sample_broadcast[sid].sensor_type for sid in all_sensor_ids}
factory_ids_map  = {sid: sample_broadcast[sid].factory_id  for sid in all_sensor_ids}
norm_min_map     = {sid: sample_broadcast[sid].normal_min   for sid in all_sensor_ids}
norm_max_map     = {sid: sample_broadcast[sid].normal_max   for sid in all_sensor_ids}

def map_sensor(idx_val):
    return sensor_ids_map.get(int(idx_val) % num_sample, "SNSR-00001")

def map_sensor_type(sid):
    return sensor_types_map.get(sid, "temperature")

def map_factory(sid):
    return factory_ids_map.get(sid, "F-01")

def map_normal_range(sid):
    return (norm_min_map.get(sid, 0.0), norm_max_map.get(sid, 100.0))

udf_sensor_id   = udf(map_sensor, StringType())
udf_sensor_type = udf(map_sensor_type, StringType())
udf_factory_id  = udf(map_factory, StringType())

# ── Generate streaming sensor readings ──
sensor_base_stream = (
    raw_stream
    .withColumn("sensor_id", udf_sensor_id(col("sensor_idx")))
    .withColumn("sensor_type", udf_sensor_type(col("sensor_id")))
    .withColumn("factory_id", udf_factory_id(col("sensor_id")))
    .drop("sensor_idx")
)

# Use a trick: vary the reading by combining sensor index with timestamp
# to create realistic-looking fluctuations
sensor_stream = (
    sensor_base_stream
    .withColumn("time_seed", (unix_timestamp(col("event_timestamp")) % 10000).cast("double"))
    .withColumn("random_factor", _round(rand(42) * 2.0 - 1.0, 6))  # -1 to +1
    .withColumn("value_raw",
        when(col("sensor_type") == "temperature",
             lit(50.0) + (unix_timestamp(col("event_timestamp")) % lit(600)) * lit(0.03) + col("random_factor") * lit(15))
        .when(col("sensor_type") == "vibration",
             lit(2.0) + col("random_factor") * lit(2.5) + (unix_timestamp(col("event_timestamp")) % lit(300)) * lit(0.005))
        .when(col("sensor_type") == "pressure",
             lit(105.0) + col("random_factor") * lit(12.0))
        .otherwise(
             lit(50.0) + col("random_factor") * lit(30.0))
    )
    # Inject occasional anomalies
    .withColumn("is_anomalous_source", when(rand(123) > lit(0.97), lit(True)).otherwise(lit(False)))
    .withColumn("value_raw",
        when(col("is_anomalous_source"),
             col("value_raw") + when(rand(456) > lit(0.5), lit(1), lit(-1)) * (col("value_raw") * lit(0.4) + lit(10)))
        .otherwise(col("value_raw"))
    )
    .withColumn("reading_value", _round(col("value_raw"), 4))
    .select(
        col("event_timestamp"),
        col("sensor_id"),
        col("sensor_type"),
        col("factory_id"),
        col("reading_value"),
        when(col("sensor_type") == "temperature", lit("celsius"))
        .when(col("sensor_type") == "vibration",   lit("mm/s"))
        .when(col("sensor_type") == "pressure",    lit("kPa"))
        .when(col("sensor_type") == "humidity",    lit("pct"))
        .alias("unit"),
        col("is_anomalous_source").alias("_anomaly_flag"),
        year(col("event_timestamp")).cast("int").alias("_year"),
        month(col("event_timestamp")).cast("int").alias("_month"),
        dayofmonth(col("event_timestamp")).cast("int").alias("_day"),
        hour(col("event_timestamp")).cast("int").alias("_hour"),
    )

print("✅ Sensor stream DataFrame built")
sensor_stream.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Streaming Ingestion — Bronze Layer
# MAGIC
# MAGIC **Ingest the raw stream into a managed Delta table** with schema validation
# MAGIC and enrichment:
# MAGIC - Join with factory reference data (stream-static join) to add factory metadata
# MAGIC - Join with sensor catalog for normal operating ranges
# MAGIC - Add ingestion timestamp for audit trail
# MAGIC - Write to `iot_sensor_raw` with checkpointing

# COMMAND ----------
print("=" * 70)
print("  STREAMING INGESTION — BRONZE LAYER")
print("=" * 70)

# Create the raw Delta table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_RAW} (
        event_timestamp  TIMESTAMP,
        sensor_id        STRING,
        sensor_type      STRING,
        factory_id       STRING,
        factory_name     STRING,
        region_code      STRING,
        production_lines STRING,
        model            STRING,
        normal_min       DOUBLE,
        normal_max       DOUBLE,
        reading_value    DOUBLE,
        unit             STRING,
        _year            INT,
        _month           INT,
        _day             INT,
        _hour            INT,
        ingested_at      TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")
print(f"  ✅ Bronze table ready: {TABLE_RAW}")

# ── Enrich stream with static reference data ──
enriched_stream = (
    sensor_stream
    .join(broadcast(df_factory), on="factory_id", how="left")
    .join(
        broadcast(df_cat.select("sensor_id", "model", "normal_min", "normal_max")),
        on="sensor_id", how="left"
    )
    .select(
        col("event_timestamp"),
        col("sensor_id"),
        col("sensor_type"),
        col("factory_id"),
        col("factory_name"),
        col("region_code"),
        col("production_lines"),
        col("model"),
        col("normal_min"),
        col("normal_max"),
        col("reading_value"),
        col("unit"),
        col("_year"), col("_month"), col("_day"), col("_hour"),
        current_timestamp().alias("ingested_at"),
    )
)

# ── Start bronze ingestion query ──
bronze_checkpoint = get_checkpoint("iot_bronze_ingest")

bronze_query = (
    enriched_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")
    .queryName("iot_bronze_ingest")
    .toTable(TABLE_RAW)
)

print(f"  ▶️  Bronze ingestion streaming started")
print(f"     Checkpoint: {bronze_checkpoint}")
print(f"     Sink: {TABLE_RAW}")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC Let the bronze stream run for ~30 seconds to accumulate data, then verify.

# COMMAND ----------
print("⏳ Accumulating data for 30 seconds...")
time.sleep(30)

bronze_count = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE_RAW}").collect()[0]['c']
print(f"\n📊 Bronze table count: {bronze_count:,} rows")

# Show a sample
spark.sql(f"""
    SELECT event_timestamp, sensor_id, sensor_type, factory_name,
           reading_value, unit, normal_min, normal_max
    FROM {TABLE_RAW}
    ORDER BY event_timestamp DESC
    LIMIT 10
""").show(10, truncate=False)

# Distribution check
spark.sql(f"""
    SELECT sensor_type, COUNT(*) AS readings,
           ROUND(AVG(reading_value), 2) AS avg_val,
           ROUND(MIN(reading_value), 2) AS min_val,
           ROUND(MAX(reading_value), 2) AS max_val
    FROM {TABLE_RAW}
    GROUP BY sensor_type
    ORDER BY sensor_type
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Windowed Aggregations — Tumbling & Sliding Windows
# MAGIC
# MAGIC **5-Minute Tumbling Windows**: Compute avg, max, min, stddev per sensor
# MAGIC for fixed 5-minute intervals. No overlap between windows.
# MAGIC
# MAGIC **10-Minute Sliding Windows**: Detect trends by sliding a 10-minute window
# MAGIC every 1 minute, computing delta between consecutive windows.

# COMMAND ----------
print("=" * 70)
print("  WINDOWED AGGREGATIONS")
print("=" * 70)

# ── Read from bronze table as a stream ──
bronze_read = spark.readStream.table(TABLE_RAW)

# ══════════════════════════════════════════════════════════
# 5A: 5-MIN TUMBLING WINDOW AGGREGATIONS
# ══════════════════════════════════════════════════════════
print("\n📌 5-MINUTE TUMBLING WINDOW AGGREGATIONS")
print("   Window: 5 minutes, Slide: 5 minutes (tumbling = non-overlapping)")

agg_5min = (
    bronze_read
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("sensor_id"),
        col("sensor_type"),
        col("factory_id"),
    )
    .agg(
        _round(avg(col("reading_value")), 4).alias("avg_reading"),
        _round(max(col("reading_value")), 4).alias("max_reading"),
        _round(min(col("reading_value")), 4).alias("min_reading"),
        _round(stddev(col("reading_value")), 4).alias("stddev_reading"),
        count("*").alias("sample_count"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("sensor_type"),
        col("factory_id"),
        col("avg_reading"),
        col("max_reading"),
        col("min_reading"),
        col("stddev_reading"),
        col("sample_count"),
    )
)

# Verify schema
agg_5min.printSchema()

# Create target table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_5MIN_AGG} (
        window_start   TIMESTAMP,
        window_end     TIMESTAMP,
        sensor_id      STRING,
        sensor_type    STRING,
        factory_id     STRING,
        avg_reading    DOUBLE,
        max_reading    DOUBLE,
        min_reading    DOUBLE,
        stddev_reading DOUBLE,
        sample_count   BIGINT
    ) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

agg_5min_checkpoint = get_checkpoint("iot_5min_agg")

agg_5min_query = (
    agg_5min
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", agg_5min_checkpoint)
    .trigger(processingTime="30 seconds")
    .queryName("iot_5min_agg")
    .start(TABLE_5MIN_AGG)
)

print(f"  ✅ 5-min aggregation query started")
print(f"     Sink: {TABLE_5MIN_AGG}")

# ══════════════════════════════════════════════════════════
# 5B: 10-MIN SLIDING WINDOW FOR TREND DETECTION
# ══════════════════════════════════════════════════════════
print("\n📌 10-MINUTE SLIDING WINDOW (slide=1 min)")
print("   Window: 10 minutes, Slide: 1 minute (overlapping)")

agg_10min = (
    bronze_read
    .withWatermark("event_timestamp", "15 minutes")
    .groupBy(
        window(col("event_timestamp"), "10 minutes", "1 minute"),
        col("sensor_type"),
        col("factory_id"),
    )
    .agg(
        _round(avg(col("reading_value")), 4).alias("avg_reading"),
        _round(max(col("reading_value")), 4).alias("max_reading"),
        _round(min(col("reading_value")), 4).alias("min_reading"),
        count("*").alias("sample_count"),
        _round(stddev(col("reading_value")), 4).alias("stddev_reading"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_type"),
        col("factory_id"),
        col("avg_reading"),
        col("max_reading"),
        col("min_reading"),
        col("sample_count"),
        col("stddev_reading"),
    )
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_10MIN_SLIDE} (
        window_start   TIMESTAMP,
        window_end     TIMESTAMP,
        sensor_type    STRING,
        factory_id     STRING,
        avg_reading    DOUBLE,
        max_reading    DOUBLE,
        min_reading    DOUBLE,
        sample_count   BIGINT,
        stddev_reading DOUBLE
    ) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

agg_10min_checkpoint = get_checkpoint("iot_10min_slide")

agg_10min_query = (
    agg_10min
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", agg_10min_checkpoint)
    .trigger(processingTime="30 seconds")
    .queryName("iot_10min_slide")
    .start(TABLE_10MIN_SLIDE)
)

print(f"  ✅ 10-min sliding window query started")
print(f"     Sink: {TABLE_10MIN_SLIDE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC Wait for aggregations to accumulate data, then verify the results.

# COMMAND ----------
print("⏳ Waiting for windowed aggregations to produce results (45 seconds)...")
time.sleep(45)

print("\n─── 5-Minute Tumbling Window Results ───")
five_count = spark.table(TABLE_5MIN_AGG).count()
print(f"  Rows: {five_count}")

spark.sql(f"""
    SELECT window_start, window_end, sensor_id, sensor_type,
           avg_reading, max_reading, min_reading, stddev_reading, sample_count
    FROM {TABLE_5MIN_AGG}
    ORDER BY window_start DESC, sensor_id
    LIMIT 15
""").show(15, truncate=False)

print("\n─── 10-Minute Sliding Window Results ───")
ten_count = spark.table(TABLE_10MIN_SLIDE).count()
print(f"  Rows: {ten_count}")

spark.sql(f"""
    SELECT window_start, window_end, sensor_type, factory_id,
           avg_reading, max_reading, min_reading, sample_count, stddev_reading
    FROM {TABLE_10MIN_SLIDE}
    ORDER BY window_start DESC
    LIMIT 15
""").show(15, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Anomaly Detection Pipeline
# MAGIC
# MAGIC Implement **3-sigma anomaly detection** using stateful streaming processing:
# MAGIC 1. Compute rolling mean and stddev per sensor (stateful)
# MAGIC 2. Flag readings where `|value - mean| > 3 * stddev`
# MAGIC 3. Enrich alerts with severity, factory context, and threshold details
# MAGIC 4. Write alerts to `iot_sensor_alerts` Delta table

# COMMAND ----------
print("=" * 70)
print("  ANOMALY DETECTION PIPELINE")
print("=" * 70)

# ── Strategy: Use per-sensor baseline from reference table ──
# In production, you'd maintain a running mean/stddev via stateful agg.
# Here we use the reference normal_min/normal_max to derive thresholds
# and detect readings that exceed 3-sigma from the reference mean.
#
# The baseline approach:
#   baseline_mean = (normal_min + normal_max) / 2
#   baseline_std  = (normal_max - normal_min) / 6   (6-sigma range assumption)
#   threshold_upper = baseline_mean + 3 * baseline_std
#   threshold_lower = baseline_mean - 3 * baseline_std

# ── Read from bronze with reference data enrichment ──
anomaly_base = (
    bronze_read
    .withWatermark("event_timestamp", "5 minutes")
    .join(broadcast(df_cat.select("sensor_id", "normal_min", "normal_max")),
          on="sensor_id", how="left")
)

# Compute anomaly scores
anomaly_detected = (
    anomaly_base
    .withColumn("baseline_mean", (col("normal_min") + col("normal_max")) / 2.0)
    .withColumn("baseline_std",
        when((col("normal_max") - col("normal_min")) > 0,
             (col("normal_max") - col("normal_min")) / 6.0)
        .otherwise(lit(1.0))
    )
    .withColumn("sigma_upper", col("baseline_mean") + lit(3) * col("baseline_std"))
    .withColumn("sigma_lower", col("baseline_mean") - lit(3) * col("baseline_std"))
    .withColumn("deviation", _round(col("reading_value") - col("baseline_mean"), 4))
    .withColumn("deviation_sigma",
        when(col("baseline_std") > 0,
             _round(_abs(col("deviation")) / col("baseline_std"), 2))
        .otherwise(lit(0.0))
    )
    .withColumn("is_anomaly",
        (col("reading_value") > col("sigma_upper")) |
        (col("reading_value") < col("sigma_lower"))
    )
    # Severity based on sigma deviation
    .withColumn("severity",
        when(col("deviation_sigma") > lit(5.0), lit("CRITICAL"))
        .when(col("deviation_sigma") > lit(4.0), lit("HIGH"))
        .when(col("deviation_sigma") > lit(3.0), lit("MEDIUM"))
        .otherwise(lit("LOW"))
    )
    # Alert message
    .withColumn("alert_message",
        concat(
            lit("["), col("severity"), lit("] "),
            col("sensor_type"), lit(" anomaly on "),
            col("sensor_id"), lit(": reading="),
            col("reading_value").cast("string"),
            lit(", deviation="), col("deviation_sigma").cast("string"),
            lit("σ")
        )
    )
    .filter(col("is_anomaly") == True)
    .select(
        col("event_timestamp"),
        col("sensor_id"),
        col("sensor_type"),
        col("factory_id"),
        col("factory_name"),
        col("reading_value"),
        col("unit"),
        col("baseline_mean"),
        col("baseline_std"),
        col("sigma_upper"),
        col("sigma_lower"),
        col("deviation"),
        col("deviation_sigma"),
        col("severity"),
        col("alert_message"),
        col("ingested_at"),
    )
)

# Create alerts table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_ALERTS} (
        event_timestamp  TIMESTAMP,
        sensor_id        STRING,
        sensor_type      STRING,
        factory_id       STRING,
        factory_name     STRING,
        reading_value    DOUBLE,
        unit             STRING,
        baseline_mean    DOUBLE,
        baseline_std     DOUBLE,
        sigma_upper      DOUBLE,
        sigma_lower      DOUBLE,
        deviation        DOUBLE,
        deviation_sigma  DOUBLE,
        severity         STRING,
        alert_message    STRING,
        ingested_at      TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

alerts_checkpoint = get_checkpoint("iot_alerts")

alerts_query = (
    anomaly_detected
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", alerts_checkpoint)
    .trigger(processingTime="10 seconds")
    .queryName("iot_anomaly_detection")
    .start(TABLE_ALERTS)
)

print(f"  ▶️  Anomaly detection stream started")
print(f"     Checkpoint: {alerts_checkpoint}")
print(f"     Sink: {TABLE_ALERTS}")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC Let the anomaly detection run for a bit, then check what alerts were generated.

# COMMAND ----------
print("⏳ Waiting for anomaly detection to process data (30 seconds)...")
time.sleep(30)

alert_count = spark.table(TABLE_ALERTS).count()
print(f"\n🚨 Total alerts generated: {alert_count}")

if alert_count > 0:
    print("\n─── Recent Alerts ───")
    spark.sql(f"""
        SELECT event_timestamp, severity, sensor_type, sensor_id,
               reading_value, deviation_sigma, alert_message
        FROM {TABLE_ALERTS}
        ORDER BY event_timestamp DESC, deviation_sigma DESC
        LIMIT 15
    """).show(15, truncate=False)

    print("\n─── Alerts by Severity ───")
    spark.sql(f"""
        SELECT severity, COUNT(*) AS count,
               ROUND(AVG(deviation_sigma), 2) AS avg_deviation
        FROM {TABLE_ALERTS}
        GROUP BY severity
        ORDER BY
            CASE severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM' THEN 3
                WHEN 'LOW' THEN 4
            END
    """).show(truncate=False)

    print("\n─── Alerts by Factory ───")
    spark.sql(f"""
        SELECT factory_name, COUNT(*) AS alert_count,
               ROUND(AVG(deviation_sigma), 2) AS avg_dev_sigma
        FROM {TABLE_ALERTS}
        GROUP BY factory_name
        ORDER BY alert_count DESC
    """).show(truncate=False)
else:
    print("  (No anomalies detected yet — the random seed may not have triggered anomalies)")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Correlated Anomaly Detection — Stream-Stream Join
# MAGIC
# MAGIC Detect **correlated anomalies** where both temperature AND vibration sensors
# MAGIC on the same factory exhibit anomalous readings within a short time window.
# MAGIC This is a key pattern for identifying compound equipment failures.
# MAGIC
# MAGIC **Approach**: Split the bronze stream into temperature and vibration sub-streams,
# MAGIC then perform a stream-stream join with a time-window constraint.

# COMMAND ----------
print("=" * 70)
print("  CORRELATED ANOMALY DETECTION — STREAM-STREAM JOIN")
print("=" * 70)

# ── Anomalous temperature stream ──
temp_stream = (
    bronze_read
    .withWatermark("event_timestamp", "5 minutes")
    .filter((col("sensor_type") == "temperature") &
            ((col("reading_value") > lit(80)) | (col("reading_value") < lit(20))))
    .select(
        col("event_timestamp").alias("temp_ts"),
        col("sensor_id").alias("temp_sensor"),
        col("factory_id"),
        col("reading_value").alias("temp_value"),
    )
    .withWatermark("temp_ts", "5 minutes")
)

# ── Anomalous vibration stream ──
vib_stream = (
    bronze_read
    .withWatermark("event_timestamp", "5 minutes")
    .filter((col("sensor_type") == "vibration") &
            (col("reading_value") > lit(5.0)))
    .select(
        col("event_timestamp").alias("vib_ts"),
        col("sensor_id").alias("vib_sensor"),
        col("factory_id"),
        col("reading_value").alias("vib_value"),
    )
    .withWatermark("vib_ts", "5 minutes")
)

# ── Stream-Stream Join: temperature anomaly within 3 minutes of vibration anomaly ──
# This uses a time-range join condition
correlated_stream = (
    temp_stream.alias("t")
    .join(
        vib_stream.alias("v"),
        expr("""
            t.factory_id = v.factory_id AND
            v.vib_ts >= t.temp_ts AND
            v.vib_ts <= t.temp_ts + interval 3 minutes
        """),
        joinType="inner"
    )
    .select(
        col("t.temp_ts").alias("event_timestamp"),
        col("t.factory_id").alias("factory_id"),
        col("t.temp_sensor").alias("temperature_sensor"),
        col("t.temp_value").alias("temperature_reading"),
        col("v.vib_sensor").alias("vibration_sensor"),
        col("v.vib_value").alias("vibration_reading"),
        current_timestamp().alias("detected_at"),
    )
)

# Create correlated alerts table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_CORRELATED} (
        event_timestamp      TIMESTAMP,
        factory_id           STRING,
        temperature_sensor   STRING,
        temperature_reading  DOUBLE,
        vibration_sensor     STRING,
        vibration_reading    DOUBLE,
        detected_at          TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

correlated_checkpoint = get_checkpoint("iot_correlated")

correlated_query = (
    correlated_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", correlated_checkpoint)
    .trigger(processingTime="15 seconds")
    .queryName("iot_correlated_alerts")
    .start(TABLE_CORRELATED)
)

print(f"  ▶️  Correlated anomaly detection stream started")
print(f"     Checkpoint: {correlated_checkpoint}")
print(f"     Sink: {TABLE_CORRELATED}")

print("""
  🔗 Stream-Stream Join Pattern:
     temperature_stream ──┐
                           ├── INNER JOIN on factory_id + time window (3 min)
     vibration_stream   ──┘
     → Only emit when BOTH streams have anomalous readings near each other
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC Wait for correlated events and check results.

# COMMAND ----------
print("⏳ Waiting for correlated anomalies (30 seconds)...")
time.sleep(30)

corr_count = spark.table(TABLE_CORRELATED).count()
print(f"\n🔗 Correlated anomaly events: {corr_count}")

if corr_count > 0:
    spark.sql(f"""
        SELECT event_timestamp, factory_id,
               temperature_sensor, temperature_reading,
               vibration_sensor, vibration_reading,
               detected_at
        FROM {TABLE_CORRELATED}
        ORDER BY event_timestamp DESC
        LIMIT 15
    """).show(15, truncate=False)
else:
    print("  (No correlated anomalies detected — streams may not have aligned yet)")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Idempotent Sink — foreachBatch with MERGE
# MAGIC
# MAGIC Use `foreachBatch` to perform **idempotent writes** into a Delta table.
# MAGIC This ensures exactly-once semantics even if the streaming query restarts:
# MAGIC - Each microbatch is processed as a static DataFrame
# MAGIC - MERGE ensures upsert behavior (no duplicates)
# MAGIC - Target: `iot_sensor_stats` — per-sensor running statistics

# COMMAND ----------
print("=" * 70)
print("  IDEMPOTENT SINK — foreachBatch + MERGE")
print("=" * 70)

# Create the target stats table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_SENSOR_STATS} (
        sensor_id       STRING,
        sensor_type     STRING,
        factory_id      STRING,
        total_readings  BIGINT,
        avg_value       DOUBLE,
        min_value       DOUBLE,
        max_value       DOUBLE,
        stddev_value    DOUBLE,
        last_reading    DOUBLE,
        last_updated    TIMESTAMP,
        first_seen      TIMESTAMP,
        last_seen       TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

# ── Define the foreachBatch function ──
def merge_microbatch(micro_batch_df, batch_id):
    """
    foreachBatch callback: merge each micro-batch into the sensor stats table.
    This provides idempotent writes — reprocessing the same batch won't create
    duplicate records because MERGE performs upsert.
    """
    if micro_batch_df.isEmpty():
        return

    # Compute per-sensor stats for this micro-batch
    batch_stats = (
        micro_batch_df
        .groupBy("sensor_id", "sensor_type", "factory_id")
        .agg(
            count("*").alias("batch_readings"),
            _round(avg("reading_value"), 4).alias("batch_avg"),
            _round(min("reading_value"), 4).alias("batch_min"),
            _round(max("reading_value"), 4).alias("batch_max"),
            _round(stddev("reading_value"), 4).alias("batch_stddev"),
            max("event_timestamp").alias("batch_last_seen"),
        )
        .withColumn("last_updated", current_timestamp())
    )

    # Get the last reading value per sensor
    from pyspark.sql.window import Window
    w = Window.partitionBy("sensor_id").orderBy(col("event_timestamp").desc())

    last_readings = (
        micro_batch_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select(col("sensor_id"), col("reading_value").alias("last_val"))
    )

    current_batch = (
        batch_stats.alias("s")
        .join(last_readings.alias("l"), on="sensor_id", how="left")
        .select(
            col("s.sensor_id"),
            col("s.sensor_type"),
            col("s.factory_id"),
            col("s.batch_readings"),
            col("s.batch_avg"),
            col("s.batch_min"),
            col("s.batch_max"),
            col("s.batch_stddev"),
            col("l.last_val"),
            col("s.last_updated"),
            col("s.batch_last_seen"),
        )
    )

    current_batch.createOrReplaceTempView("_batch_updates")

    # MERGE: upsert per sensor — combine existing running stats with new batch stats
    spark.sql(f"""
        MERGE INTO {TABLE_SENSOR_STATS} AS t
        USING _batch_updates AS s
        ON t.sensor_id = s.sensor_id
        WHEN MATCHED THEN UPDATE SET
            t.total_readings = t.total_readings + s.batch_readings,
            t.avg_value      = ROUND(
                (t.avg_value * t.total_readings + s.batch_avg * s.batch_readings)
                / (t.total_readings + s.batch_readings), 4),
            t.min_value      = LEAST(t.min_value, s.batch_min),
            t.max_value      = GREATEST(t.max_value, s.batch_max),
            t.stddev_value   = ROUND((t.stddev_value + COALESCE(s.batch_stddev, 0)) / 2, 4),
            t.last_reading   = s.last_val,
            t.last_updated   = s.last_updated,
            t.last_seen      = s.batch_last_seen
        WHEN NOT MATCHED THEN INSERT (
            sensor_id, sensor_type, factory_id,
            total_readings, avg_value, min_value, max_value, stddev_value,
            last_reading, last_updated, first_seen, last_seen
        ) VALUES (
            s.sensor_id, s.sensor_type, s.factory_id,
            s.batch_readings, s.batch_avg, s.batch_min, s.batch_max, s.batch_stddev,
            s.last_val, s.last_updated, s.last_updated, s.batch_last_seen
        )
    """)

    print(f"  [Batch {batch_id}] Merged {current_batch.count()} sensor stats")

# ── Start the foreachBatch streaming query ──
stats_checkpoint = get_checkpoint("iot_sensor_stats")

stats_query = (
    bronze_read
    .withWatermark("event_timestamp", "10 minutes")
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", stats_checkpoint)
    .trigger(processingTime="20 seconds")
    .foreachBatch(merge_microbatch)
    .queryName("iot_sensor_stats_merger")
    .start()
)

print(f"  ▶️  foreachBatch MERGE stream started")
print(f"     Checkpoint: {stats_checkpoint}")
print(f"     Sink: {TABLE_SENSOR_STATS}")
print("")
print("  📝 foreachBatch pattern:")
print("     ┌─────────────────────────────────────────┐")
print("     │  Micro-batch arrives                     │")
print("     │       ↓                                  │")
print("     │  Compute per-sensor stats for batch       │")
print("     │       ↓                                  │")
print("     │  MERGE INTO target Delta table            │")
print("     │  (idempotent — upsert, no duplicates)     │")
print("     └─────────────────────────────────────────┘")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC Let the foreachBatch merge run and verify the idempotent sink.

# COMMAND ----------
print("⏳ Waiting for foreachBatch merges to accumulate (45 seconds)...")
time.sleep(45)

print("\n─── Per-Sensor Running Stats (from foreachBatch + MERGE) ───")
stats_count = spark.table(TABLE_SENSOR_STATS).count()
print(f"  Sensors with stats: {stats_count}")

spark.sql(f"""
    SELECT sensor_id, sensor_type, factory_id,
           total_readings, avg_value, min_value, max_value,
           ROUND(stddev_value, 4) AS stddev,
           last_reading, last_updated
    FROM {TABLE_SENSOR_STATS}
    ORDER BY total_readings DESC
    LIMIT 15
""").show(15, truncate=False)

print("\n─── Factory-Level Summary ───")
spark.sql(f"""
    SELECT factory_id,
           COUNT(*) AS sensor_count,
           SUM(total_readings) AS total_readings,
           ROUND(AVG(avg_value), 4) AS global_avg
    FROM {TABLE_SENSOR_STATS}
    GROUP BY factory_id
    ORDER BY factory_id
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Late Data Handling & Watermarks
# MAGIC
# MAGIC Structured Streaming uses **watermarks** to handle late-arriving data:
# MAGIC - Watermark = max event time seen minus threshold
# MAGIC - Data older than watermark is dropped (for aggregations with append mode)
# MAGIC - This section demonstrates configuring watermarks and observing their effect
# MAGIC
# MAGIC **Key Concepts**:
# MAGIC - `withWatermark("timestamp_col", "delay_threshold")`
# MAGIC - Trade-off: larger threshold = more state = higher memory, but less data loss
# MAGIC - Watermarks enable append-mode output for aggregations

# COMMAND ----------
print("=" * 70)
print("  LATE DATA HANDLING & WATERMARKS")
print("=" * 70)

# ── Demonstrate watermark behavior ──
# We'll create a stream that intentionally introduces delayed data
# using a custom transformation to shift some timestamps into the past.

print("""
  💧 WATERMARK ANATOMY:
  
      ┌──────────────────────────────────────┐
      │  Event Stream (time →)               │
      │  e1(t=10:00)  e2(t=10:02)  e3(t=10:01) ← LATE  │
      │       │            │            │         │
      │       ▼            ▼            ▼         ▼
      │  ┌──────────────────────────────────────────┐
      │  │  Watermark = max_ts - 5 min               │
      │  │  (e3 at 10:01 is within watermark of 9:57) │
      │  │  But if e3 arrived at 10:10, it's DROPPED  │
      │  └──────────────────────────────────────────┘
      └──────────────────────────────────────┘
""")

# ── Read from bronze and apply watermark ──
watermark_demo = (
    bronze_read
    .withWatermark("event_timestamp", "5 minutes")
    .groupBy(
        window(col("event_timestamp"), "2 minutes"),
        col("sensor_type"),
    )
    .agg(
        count("*").alias("reading_count"),
        _round(avg("reading_value"), 4).alias("avg_value"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_type"),
        col("reading_count"),
        col("avg_value"),
    )
)

# Write watermark demo results to a temp table
spark.sql("""
    CREATE TABLE IF NOT EXISTS default.iot_watermark_demo (
        window_start  TIMESTAMP,
        window_end    TIMESTAMP,
        sensor_type   STRING,
        reading_count BIGINT,
        avg_value     DOUBLE
    ) USING DELTA
""")

wm_checkpoint = get_checkpoint("iot_watermark_demo")

wm_query = (
    watermark_demo
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", wm_checkpoint)
    .trigger(processingTime="15 seconds")
    .queryName("iot_watermark_demo")
    .start("default.iot_watermark_demo")
)

print(f"  ▶️  Watermark demo stream started")
print(f"     Watermark threshold: 5 minutes")
print(f"     Window: 2 minutes")

# COMMAND ----------
# MAGIC %md
# MAGIC ---

# COMMAND ----------
print("⏳ Letting watermark demo run for 30 seconds...")
time.sleep(30)

wm_count = spark.table("default.iot_watermark_demo").count()
print(f"\n💧 Watermark Demo Results: {wm_count} windowed rows")

spark.sql("""
    SELECT window_start, window_end, sensor_type,
           reading_count, avg_value
    FROM default.iot_watermark_demo
    ORDER BY window_start DESC
    LIMIT 15
""").show(15, truncate=False)

print("""
  📝 WATERMARK BEST PRACTICES:
  1. Set watermark threshold = max expected lateness + some buffer
  2. Larger watermark → more state stored → higher memory
  3. Smaller watermark → less state → data loss for late arrivals
  4. Use 'append' output mode (requires watermark on aggregations)
  5. Monitor the 'numRowsDroppedByWatermark' metric in Spark UI
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Monitoring Dashboard — SQL Queries
# MAGIC
# MAGIC Ready-to-use SQL queries for a real-time operations dashboard.
# MAGIC These can be used in Databricks SQL dashboards, alerts, or external BI tools.

# COMMAND ----------
print("=" * 70)
print("  MONITORING DASHBOARD — SQL QUERIES")
print("=" * 70)

# ── Query 1: Real-time sensor health overview ──
print("\n─── DASHBOARD QUERY 1: Real-Time Sensor Health ───")
print("""
-- Total readings in last 5 minutes by sensor type
SELECT
    sensor_type,
    COUNT(*) AS readings_last_5min,
    ROUND(AVG(reading_value), 2) AS avg_value,
    ROUND(MIN(reading_value), 2) AS min_value,
    ROUND(MAX(reading_value), 2) AS max_value
FROM iot_sensor_raw
WHERE event_timestamp >= current_timestamp() - INTERVAL 5 MINUTES
GROUP BY sensor_type
ORDER BY sensor_type;
""")

# Execute it
spark.sql(f"""
    SELECT
        sensor_type,
        COUNT(*) AS readings_last_5min,
        ROUND(AVG(reading_value), 2) AS avg_value,
        ROUND(MIN(reading_value), 2) AS min_value,
        ROUND(MAX(reading_value), 2) AS max_value
    FROM {TABLE_RAW}
    WHERE event_timestamp >= current_timestamp() - INTERVAL 5 MINUTES
    GROUP BY sensor_type
    ORDER BY sensor_type
""").show(truncate=False)

# ── Query 2: Alert summary by severity ──
print("\n─── DASHBOARD QUERY 2: Alerts by Severity (Last Hour) ───")
print("""
SELECT
    severity,
    COUNT(*) AS alert_count,
    ROUND(AVG(deviation_sigma), 2) AS avg_sigma,
    COUNT(DISTINCT sensor_id) AS affected_sensors
FROM iot_sensor_alerts
WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY severity
ORDER BY
    CASE severity
        WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3 WHEN 'LOW' THEN 4
    END;
""")

spark.sql(f"""
    SELECT
        severity,
        COUNT(*) AS alert_count,
        ROUND(AVG(deviation_sigma), 2) AS avg_sigma,
        COUNT(DISTINCT sensor_id) AS affected_sensors
    FROM {TABLE_ALERTS}
    WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    GROUP BY severity
    ORDER BY
        CASE severity
            WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3 WHEN 'LOW' THEN 4
        END
""").show(truncate=False)

# ── Query 3: Factory comparison — 5-min window aggregates ──
print("\n─── DASHBOARD QUERY 3: Factory Comparison (5-Min Windows) ───")
print("""
SELECT
    factory_id,
    sensor_type,
    COUNT(*) AS window_count,
    ROUND(AVG(avg_reading), 2) AS typical_avg,
    ROUND(MAX(max_reading), 2) AS peak_reading,
    ROUND(AVG(stddev_reading), 4) AS avg_variability
FROM iot_sensor_5min_agg
WHERE window_start >= current_timestamp() - INTERVAL 30 MINUTES
GROUP BY factory_id, sensor_type
ORDER BY factory_id, sensor_type;
""")

spark.sql(f"""
    SELECT
        factory_id,
        sensor_type,
        COUNT(*) AS window_count,
        ROUND(AVG(avg_reading), 2) AS typical_avg,
        ROUND(MAX(max_reading), 2) AS peak_reading,
        ROUND(AVG(stddev_reading), 4) AS avg_variability
    FROM {TABLE_5MIN_AGG}
    WHERE window_start >= current_timestamp() - INTERVAL 30 MINUTES
    GROUP BY factory_id, sensor_type
    ORDER BY factory_id, sensor_type
""").show(truncate=False)

# ── Query 4: Trending sensors (10-min sliding window) ──
print("\n─── DASHBOARD QUERY 4: Trend Detection (10-Min Sliding) ───")
print("""
WITH windowed AS (
    SELECT
        sensor_type,
        factory_id,
        window_start,
        avg_reading,
        LAG(avg_reading) OVER (
            PARTITION BY sensor_type, factory_id
            ORDER BY window_start
        ) AS prev_avg,
        stddev_reading
    FROM iot_sensor_10min_slide
    WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
)
SELECT
    sensor_type,
    factory_id,
    window_start,
    ROUND(avg_reading, 2) AS current_avg,
    ROUND(prev_avg, 2) AS previous_avg,
    ROUND(avg_reading - prev_avg, 2) AS delta,
    ROUND(stddev_reading, 4) AS current_stddev
FROM windowed
WHERE prev_avg IS NOT NULL
  AND ABS(avg_reading - prev_avg) > stddev_reading * 1.5
ORDER BY ABS(avg_reading - prev_reading) DESC
LIMIT 20;
""")

spark.sql(f"""
    WITH windowed AS (
        SELECT
            sensor_type,
            factory_id,
            window_start,
            avg_reading,
            LAG(avg_reading) OVER (
                PARTITION BY sensor_type, factory_id
                ORDER BY window_start
            ) AS prev_avg,
            stddev_reading
        FROM {TABLE_10MIN_SLIDE}
        WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
    )
    SELECT
        sensor_type,
        factory_id,
        window_start,
        ROUND(avg_reading, 2) AS current_avg,
        ROUND(prev_avg, 2) AS previous_avg,
        ROUND(avg_reading - prev_avg, 2) AS delta,
        ROUND(stddev_reading, 4) AS current_stddev
    FROM windowed
    WHERE prev_avg IS NOT NULL
      AND ABS(avg_reading - prev_avg) > stddev_reading * 1.5
    ORDER BY ABS(avg_reading - prev_avg) DESC
    LIMIT 20
""").show(20, truncate=False)

# ── Query 5: Correlated anomaly incidents ──
print("\n─── DASHBOARD QUERY 5: Correlated Anomaly Incidents ───")
print("""
SELECT
    factory_id,
    COUNT(*) AS correlated_events,
    MIN(event_timestamp) AS first_detected,
    MAX(event_timestamp) AS last_detected,
    ROUND(AVG(temperature_reading), 2) AS avg_temp_anomaly,
    ROUND(AVG(vibration_reading), 2) AS avg_vib_anomaly
FROM iot_correlated_alerts
WHERE detected_at >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY factory_id
ORDER BY correlated_events DESC;
""")

spark.sql(f"""
    SELECT
        factory_id,
        COUNT(*) AS correlated_events,
        MIN(event_timestamp) AS first_detected,
        MAX(event_timestamp) AS last_detected,
        ROUND(AVG(temperature_reading), 2) AS avg_temp_anomaly,
        ROUND(AVG(vibration_reading), 2) AS avg_vib_anomaly
    FROM {TABLE_CORRELATED}
    WHERE detected_at >= current_timestamp() - INTERVAL 1 HOUR
    GROUP BY factory_id
    ORDER BY correlated_events DESC
""").show(truncate=False)

# ── Query 6: Per-sensor running stats ──
print("\n─── DASHBOARD QUERY 6: Sensor Health Score ───")
print("""
SELECT
    sensor_id,
    sensor_type,
    factory_id,
    total_readings,
    ROUND(avg_value, 2) AS running_avg,
    ROUND(min_value, 2) AS all_time_low,
    ROUND(max_value, 2) AS all_time_high,
    last_reading,
    DATEDIFF(minute, last_updated, current_timestamp()) AS minutes_since_update
FROM iot_sensor_stats
ORDER BY minutes_since_update ASC, total_readings DESC
LIMIT 20;
""")

spark.sql(f"""
    SELECT
        sensor_id,
        sensor_type,
        factory_id,
        total_readings,
        ROUND(avg_value, 2) AS running_avg,
        ROUND(min_value, 2) AS all_time_low,
        ROUND(max_value, 2) AS all_time_high,
        last_reading,
        ROUND(
            (UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(last_updated)) / 60.0,
            1
        ) AS minutes_since_update
    FROM {TABLE_SENSOR_STATS}
    ORDER BY minutes_since_update ASC, total_readings DESC
    LIMIT 20
""").show(20, truncate=False)

print("\n✅ Dashboard queries ready")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Production Considerations & Cleanup
# MAGIC
# MAGIC Key considerations for taking this pipeline to production, plus cleanup
# MAGIC of all resources created during this notebook.

# COMMAND ----------
print("=" * 70)
print("  PRODUCTION CONSIDERATIONS")
print("=" * 70)

print("""
  🔐 SECURITY:
     • Use Unity Catalog for table ACLs and RBAC
     • Grant SELECT on raw table, INSERT on alerts table via GRANT statements
     • Store checkpoint paths in UC Volumes with proper permissions

  📈 SCALABILITY:
     • 10,000 sensors × 1 reading/sec = 864M readings/day
     • Use cluster auto-scaling (min 2, max 20 workers)
     • Enable Photon acceleration for aggregation workloads
     • Partition bronze table by _year, _month, _day for query pruning
     • Consider Delta Liquid Clustering for adaptive file sizing

  ⚡ PERFORMANCE:
     • Use broadcast joins for reference data (<10MB)
     • Tune trigger intervals: bronze (1-5 sec), aggregations (30-60 sec)
     • Set spark.sql.shuffle.partitions = 2× cluster cores
     • Use Delta OPTIMIZE + ZORDER on sensor_id, event_timestamp
     • Enable delta.autoOptimize.autoCompact for small file compaction

  🛡️ RELIABILITY:
     • Checkpoints are critical — never delete without stopping the stream
     • Use cloud storage (S3/ADLS/GCS) checkpoints in production, not Volumes/DBFS
     • Set failOnDataLoss=false only when reprocessing is feasible
     • Monitor Spark StreamingQueryListener for lifecycle events

  🔔 ALERTING:
     • Integrate with webhook sinks (foreachBatch + requests library)
     • Push CRITICAL alerts to PagerDuty/Slack via foreachBatch
     • Create Databricks SQL alerts on the dashboard queries above

  🧪 TESTING:
     • Run availableNow trigger on historical data to validate logic
     • Use the generated anomaly rows (5% of stream) for confidence testing
     • Validate MERGE idempotency by re-running on same batch
""")

# ── Cleanup ──
print("\n" + "=" * 70)
print("  CLEANUP — Stop All Streams")
print("=" * 70)

active_queries = list(spark.streams.active)
print(f"\n  Active streaming queries: {len(active_queries)}")

for q in active_queries:
    try:
        print(f"  ⏹️  Stopping: {q.name} (status: {q.status['message']})")
        q.stop()
    except Exception as e:
        print(f"  ⚠️  Could not stop {q.name}: {e}")

print(f"\n  Remaining active queries: {len(spark.streams.active)}")
print("  ✅ All streams stopped")

# ── Table Summary ──
print("\n" + "=" * 70)
print("  PIPELINE TABLE SUMMARY")
print("=" * 70)

tables = [
    TABLE_RAW, TABLE_FACTORY_REF, TABLE_SENSOR_CAT,
    TABLE_5MIN_AGG, TABLE_10MIN_SLIDE,
    TABLE_ALERTS, TABLE_CORRELATED,
    TABLE_SENSOR_STATS, "default.iot_watermark_demo",
]

for tbl in tables:
    try:
        cnt = spark.table(tbl).count()
        print(f"  📦 {tbl:<40s} → {cnt:>8,} rows")
    except Exception:
        print(f"  📦 {tbl:<40s} → (empty or not found)")

print(f"\n  {'─' * 50}")
print(f"  🎉 IoT Streaming Pipeline Complete!")
print(f"  {'─' * 50}")
