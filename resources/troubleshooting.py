# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Troubleshooting Guide
# MAGIC
# MAGIC Common errors, diagnostic patterns, and solutions for Databricks workloads.
# MAGIC All examples are **serverless compatible** — no DBFS paths, no `/dbfs/` mounts.
# MAGIC
# MAGIC ## Table of Contents
# MAGIC 1. DBFS / Path Errors
# MAGIC 2. Delta Lake Errors
# MAGIC 3. Performance Issues
# MAGIC 4. Streaming Issues
# MAGIC 5. Unity Catalog Issues
# MAGIC 6. Diagnostic Tools
# MAGIC 7. Quick Diagnostic Function

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — DBFS / Path Errors
# MAGIC
# MAGIC The most common category of errors, especially on serverless where DBFS is disabled entirely.

# COMMAND ----------

# MAGIC %md
# MAGIC ### `[DBFS_DISABLED]` — DBFS is not available on serverless
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# MAGIC AnalysisException: [DBFS_DISABLED] DBFS is not available on serverless.
# MAGIC To read and write files, use Unity Catalog volumes or cloud storage URIs.
# MAGIC ```
# MAGIC
# MAGIC **Root cause:** Serverless compute does not expose the legacy DBFS layer.
# MAGIC References to `dbfs:/` or `/dbfs/` will always fail.
# MAGIC
# MAGIC **Fix — use Volumes instead of DBFS:**

# COMMAND ----------

# Before (fails on serverless)
# df = spark.read.csv("dbfs:/mnt/data/file.csv")
# df.write.parquet("/dbfs/tmp/output")

# After: use Unity Catalog Volumes
df = spark.read.csv("/Volumes/my_catalog/my_schema/my_volume/file.csv")
df.write.format("parquet").save("/Volumes/my_catalog/my_schema/my_volume/output/")

# Or use cloud storage URIs directly
# df.write.parquet("abfss://container@storage.dfs.core.windows.net/path/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `Path does not exist` — missing mount or volume
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# AnalysisException: Path does not exist: dbfs:/mnt/some_mount/...
# ```
# MAGIC
# MAGIC **Root cause:** The mount point was never created, its storage credential
# MAGIC expired, or the external location was removed.
# MAGIC
# MAGIC **Fixes:**

# COMMAND ----------

# Step 1 — list existing mounts (deprecated; prefer external locations)
display(dbutils.fs.mounts())

# Step 2 — list external locations in Unity Catalog
display(spark.sql("SHOW EXTERNAL LOCATIONS"))

# Step 3 — describe a specific external location
display(spark.sql("DESCRIBE EXTERNAL LOCATION my_location"))

# Step 4 — list volumes in a schema
display(spark.sql("SHOW VOLUMES IN my_catalog.my_schema"))

# Step 5 — describe a volume to verify its storage path
display(spark.sql("DESCRIBE VOLUME my_catalog.my_schema.my_volume"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### `Access denied` — storage credential or permission missing
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# Magic Comm Enable Request Failed. ErrorCode: Forbidden
# or
# org.apache.spark.sql.AnalysisException: Access denied ...
# ```
# MAGIC
# MAGIC **Root cause:** The service principal or user lacks read/write permissions on the
# MAGIC underlying cloud storage, or the storage credential is broken.
# MAGIC
# MAGIC **Fixes:**

# COMMAND ----------

# Verify the storage credential exists and is healthy
display(spark.sql("SHOW STORAGE CREDENTIALS"))

# Describe a specific credential
display(spark.sql("DESCRIBE STORAGE CREDENTIAL my_credential"))

# Verify external location maps to the expected cloud path
display(spark.sql("DESCRIBE EXTERNAL LOCATION my_location"))

# List grants on an external location to confirm your principal is authorized
display(spark.sql("SHOW GRANTS ON EXTERNAL LOCATION my_location"))

# Test connectivity with a minimal read
try:
    spark.read.format("delta").load("/Volumes/my_catalog/my_schema/my_volume/").limit(1).show()
    print("Access OK")
except Exception as e:
    print(f"Access failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Delta Lake Errors

# COMMAND ----------

# MAGIC %md
# MAGIC ### `ConcurrentAppendException` — conflicting writes
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# ConcurrentAppendException: Files were added to the root of the table
# by a concurrent update. Please try the operation again.
# ```
# MAGIC
# MAGIC **Root cause:** Two writers attempted to add files to the same table partition
# MAGIC simultaneously. Delta's optimistic concurrency detected the conflict.
# MAGIC
# MAGIC **Fixes:**

# COMMAND ----------

# Fix 1 — retry the operation (transient conflict for independent writes)
import time

max_retries = 3
for attempt in range(max_retries):
    try:
        spark.sql("INSERT INTO my_catalog.my_schema.my_table SELECT * FROM source")
        break
    except Exception as e:
        if "ConcurrentAppendException" in str(e) and attempt < max_retries - 1:
            wait = 2 ** attempt
            print(f"Retrying in {wait}s (attempt {attempt + 1}/{max_retries})...")
            time.sleep(wait)
        else:
            raise

# Fix 2 — use MERGE instead of INSERT for idempotent upserts
spark.sql("""
    MERGE INTO target USING source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Fix 3 — reduce partition granularity or use liquid clustering
spark.sql("ALTER TABLE my_catalog.my_schema.my_table CLUSTER BY (id)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `Schema mismatch` — column name or type does not match
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# AnalysisException: Failed to merge fields 'col_a' and 'col_a'.
# ```
# MAGIC
# MAGIC **Root cause:** The incoming DataFrame or source table has a different schema
# MAGIC than the target Delta table (case-sensitive name differences, type changes,
# MAGIC or missing/extra columns).
# MAGIC
# MAGIC **Fixes:**

# COMMAND ----------

# Fix 1 — auto-merge schema during writes (allows schema evolution)
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("my_catalog.my_schema.my_table")

# Fix 2 — set table property for auto-evolution on all writes
spark.sql("ALTER TABLE my_catalog.my_schema.my_table SET TBLPROPERTIES ('delta.autoOptimize.autoMergeSchema' = 'true')")

# Fix 3 — inspect both schemas before writing
source_df = spark.read.table("my_catalog.my_schema.source_table")
target_df = spark.read.table("my_catalog.my_schema.my_table")

print("Source schema:", source_df.schema)
print("Target schema:", target_df.schema)

# Find schema differences
source_cols = set(source_df.columns)
target_cols = set(target_df.columns)
print("In source but not target:", source_cols - target_cols)
print("In target but not source:", target_cols - source_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `Cannot time travel after VACUUM` — data files deleted
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# AnalysisException: Cannot time travel Delta table to version X.
# Available versions: [Y, Z].
# ```
# MAGIC
# MAGIC **Root cause:** VACUUM deleted the Parquet files required for that older version.
# MAGIC By default, Delta retains 7 days of history; VACUUM with 0 hours removes all
# MAGIC data files not referenced by the current version.
# MAGIC
# MAGIC **Fixes:**

# COMMAND ----------

# Check current retention settings
display(spark.sql("DESCRIBE DETAIL my_catalog.my_schema.my_table"))
# Look for: minReaderVersion, minWriterVersion, tableFeatures; check properties

# Show table properties (retention is in the properties)
props = spark.sql("SHOW TBLPROPERTIES my_catalog.my_schema.my_table").collect()
for row in props:
    key = row["key"]
    val = row["value"]
    if "retention" in key.lower():
        print(f"{key} = {val}")

# Extend retention period to 30 days
spark.sql("""
    ALTER TABLE my_catalog.my_schema.my_table
    SET TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 30 days'
    )
""")

# Check the latest available history window
display(spark.sql("DESCRIBE HISTORY my_catalog.my_schema.my_table"))

# If you need to time travel to a specific version or timestamp, do it before VACUUM:
# spark.read.option("versionAsOf", 5).table("my_catalog.my_schema.my_table")
# spark.read.option("timestampAsOf", "2025-01-01").table("my_catalog.my_schema.my_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `Table is not a Delta table` — wrong or missing format
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# AnalysisException: `my_catalog`.`my_schema`.`my_table` is not a Delta table.
# ```
# MAGIC
# MAGIC **Root cause:** The table was created in Parquet/CSV/JSON format, or the
# MAGIC Delta transaction log (`_delta_log/`) is corrupt or missing.
# MAGIC
# MAGIC **Fixes:**

# COMMAND ----------

# Check the table format
display(spark.sql("DESCRIBE EXTENDED my_catalog.my_schema.my_table"))
# Look for the Provider field — must be "delta"

# Convert an existing Parquet table to Delta
spark.sql("CONVERT TO DELTA my_catalog.my_schema.my_table")

# Convert with partitioning preserved
spark.sql("CONVERT TO DELTA my_catalog.my_schema.my_table PARTITIONED BY (date_col INT)")

# If the table does not exist at all, create it as Delta:
spark.sql("""
    CREATE TABLE my_catalog.my_schema.my_table (
        id BIGINT,
        name STRING,
        created_at TIMESTAMP
    )
    USING DELTA
    LOCATION '/Volumes/my_catalog/my_schema/my_volume/tables/my_table'
""")

# If _delta_log is corrupt — restore from the last good checkpoint
# (delete corrupt JSON files after the last known-good checkpoint,
#  then rebuild metadata with FSCK)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Performance Issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Slow queries — general diagnostic framework
# MAGIC
# MAGIC **Symptoms:** Queries take much longer than expected, job stages show high
# MAGIC skew or excessive shuffle, or tasks spill to disk frequently.

# COMMAND ----------

# Step 1 — check table-level statistics
display(spark.sql("DESCRIBE DETAIL my_catalog.my_schema.my_table"))
# Key metrics:
#   numFiles — many small files = slow listing + scheduling overhead
#   sizeInBytes / numFiles — average file size (aim for 128–256 MB)
#   partitionColumns — too many partitions = metadata bloat

# Step 2 — check file distribution (small files problem)
spark.sql("""
    SELECT
        (sizeInBytes / (1024 * 1024)) AS file_size_mb,
        COUNT(*) AS num_files
    FROM (DESCRIBE DETAIL my_catalog.my_schema.my_table)
    GROUP BY 1
    ORDER BY file_size_mb
""")

# Step 3 — check clustering / data skipping stats
display(spark.sql("ANALYZE TABLE my_catalog.my_schema.my_table COMPUTE STATISTICS"))
display(spark.sql("DESCRIBE EXTENDED my_catalog.my_schema.my_table"))

# Step 4 — analyze a specific query
spark.sql("EXPLAIN EXTENDED SELECT * FROM my_catalog.my_schema.my_table WHERE col = 'value'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Small files problem — excessive file count
# MAGIC
# MAGIC **Symptom:** Hundreds or thousands of tiny files (< 10 MB each) causing slow
# MAGIC listing and per-file overhead.
# MAGIC
# MAGIC **Root cause:** Frequent small appends, no compaction strategy.
# MAGIC
# MAGIC **Fixes:**

# COMMAND ----------

# Fix 1 — manual OPTIMIZE to compact files (default target: 1 GB)
spark.sql("OPTIMIZE my_catalog.my_schema.my_table")

# Fix 2 — OPTIMIZE with Z-ordering on high-cardinality filter columns
spark.sql("OPTIMIZE my_catalog.my_schema.my_table ZORDER BY (customer_id, event_date)")

# Fix 3 — enable auto-compaction on the table
spark.sql("""
    ALTER TABLE my_catalog.my_schema.my_table
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# Fix 4 — increase target file size if the default is too small
spark.sql("""
    ALTER TABLE my_catalog.my_schema.my_table
    SET TBLPROPERTIES ('delta.targetFileSize' = '256mb')
""")

# Periodically run OPTIMIZE (e.g., in a scheduled job)
spark.sql("OPTIMIZE my_catalog.my_schema.my_table ZORDER BY (event_date)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data skew — uneven partition distribution
# MAGIC
# MAGIC **Symptom:** A few tasks take much longer than others; Spark UI shows skewed
# MAGIC partition sizes (one task processes GB while others process KB).
# MAGIC
# MAGIC **Root cause:** A join or group-by key has a "hot" value that concentrates
# MAGIC most rows in a single partition.
# MAGIC
# MAGIC **Fixes:**

# COMMAND ----------

# Fix 1 — enable Adaptive Query Execution (AQE) skew join optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Fix 2 — salting: add a random salt to spread hot keys
from pyspark.sql import functions as F

salt_buckets = 10

df_salted = (
    spark.table("my_catalog.my_schema.fact_table")
    .withColumn("salt", (F.rand() * salt_buckets).cast("int"))
    .withColumn("join_key_salted", F.concat(F.col("join_key"), F.lit("_"), F.col("salt")))
)

dim_salted = (
    spark.table("my_catalog.my_schema.dim_table")
    .withColumn("salt", F.explode(F.split(F.lit("_").join([str(i) for i in range(salt_buckets)]), "_")))
    .withColumn("join_key_salted", F.concat(F.col("join_key"), F.lit("_"), F.col("salt")))
)

df_joined = df_salted.join(dim_salted, "join_key_salted")

# Fix 3 — pre-aggregate the hot side before a join
# If dim_table has many duplicate rows, deduplicate it first
dim_deduped = spark.table("my_catalog.my_schema.dim_table").dropDuplicates(["join_key"])
df_joined = spark.table("my_catalog.my_schema.fact_table").join(dim_deduped, "join_key")

# Fix 4 — broadcast the smaller side (works for dims < 8 GB driver memory)
from pyspark.sql import functions as F

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable auto
dim = spark.table("my_catalog.my_schema.dim_table")
fact = spark.table("my_catalog.my_schema.fact_table")
result = fact.join(F.broadcast(dim), "join_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Out of memory — executor OOM or disk spill
# MAGIC
# MAGIC **Symptom:** Tasks fail with `OutOfMemoryError` or Spark UI shows heavy
# MAGIC spill (shuffle spill memory / disk).
# MAGIC
# MAGIC **Root cause:** Shuffle partitions too few (each task holds too much data),
# MAGIC or executor memory too small for the workload.
# MAGIC
# MAGIC **Fixes:**

# COMMAND ----------

# Check current shuffle partition count
print(f"Current shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Fix 1 — increase shuffle partitions (default 200 is often too low for large data)
# Rule of thumb: target 128–256 MB per partition after shuffle
spark.conf.set("spark.sql.shuffle.partitions", "800")

# Or let AQE dynamically adjust
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")

# Fix 2 — increase executor memory (set at cluster creation in environment or SQL config)
# spark.conf.set("spark.executor.memory", "16g")
# spark.conf.set("spark.executor.memoryOverhead", "4g")
# spark.conf.set("spark.executor.cores", "4")

# Fix 3 — repartition large DataFrames before wide transformations
df = (
    spark.table("my_catalog.my_schema.large_table")
    .repartition(400)
    .groupBy("key")
    .agg(F.sum("value"))
)

# Fix 4 — use Dataset checkpoint or break the DAG
spark.sparkContext.setCheckpointDir("/Volumes/my_catalog/my_schema/my_volume/checkpoints/")
df = df.checkpoint()

# Fix 5 — cache wisely (use disk where needed)
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Streaming Issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream stuck or not progressing
# MAGIC
# MAGIC **Symptom:** Structured Streaming query runs but produces no new rows, or
# MAGIC lags behind without progress.
# MAGIC
# MAGIC **Root cause:** Checkpoint state blocks replay, source data has stopped
# MAGIC arriving, or watermark is too large (waiting for late data).

# COMMAND ----------

# Fix 1 — inspect the streaming query and its metrics
streaming_queries = spark.streams.active
for sq in streaming_queries:
    print(f"Query: {sq.name}, Status: {sq.status}")
    print(f"Last progress: {sq.lastProgress}")
    recent = sq.recentProgress
    if recent:
        for p in recent[-3:]:
            print(f"  Batch {p['batchId']}: {p['numInputRows']} rows, "
                  f"duration={p.get('durationMs', {}).get('triggerExecution', 'N/A')} ms")

# Fix 2 — verify source data is still arriving
source_df = spark.read.format("delta").load("/Volumes/my_catalog/my_schema/source_landing/")
print(f"Latest records: {source_df.count()}")
display(source_df.orderBy(F.col("timestamp").desc()).limit(10))

# Fix 3 — check the watermark window (a large watermark may block output)
# Reduce watermark if late data is not critical
(
    spark.readStream
    .format("delta")
    .load("/Volumes/my_catalog/my_schema/my_volume/raw/")
    .withWatermark("event_time", "10 minutes")  # tune this duration
    .groupBy(F.window("event_time", "5 minutes"), "key")
    .agg(F.count("*").alias("cnt"))
    .writeStream
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/Volumes/my_catalog/my_schema/my_volume/checkpoints/query1/")
    .toTable("my_catalog.my_schema.aggregated_table")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### State store too large — stream OOM or timeout
# MAGIC
# MAGIC **Symptom:** State store grows unbounded; query slows, OOMs, or checkpoint
# MAGIC directory balloons.
# MAGIC
# MAGIC **Root cause:** Stateful aggregation (groupBy, window, mapGroupsWithState)
# MAGIC without a watermark, or with very long windows.

# COMMAND ----------

# Fix 1 — add a watermark (MANDATORY for stateful aggregations)
(
    spark.readStream
    .format("delta")
    .load("/Volumes/my_catalog/my_schema/my_volume/raw/")
    .withWatermark("event_time", "1 hour")        # keep state for 1 hour
    .groupBy(F.window("event_time", "10 minutes"))  # window duration
    .agg(F.count("*"))
)

# Fix 2 — reduce window duration or increase watermark frequency
# Shorter windows = less state retained = lower memory pressure

# Fix 3 — use append mode instead of update/complete (reduces retained state)
(
    spark.readStream
    .format("delta")
    .load("/Volumes/my_catalog/my_schema/my_volume/raw/")
    .withWatermark("timestamp", "5 minutes")
    .groupBy(F.window("timestamp", "5 minutes"))
    .count()
    .writeStream
    .outputMode("append")  # only finalized windows are emitted
    .option("checkpointLocation", "/Volumes/my_catalog/my_schema/my_volume/checkpoints/agg/")
    .toTable("my_catalog.my_schema.windowed_counts")
)

# Fix 4 — for flatMapGroupsWithState, set timeouts and clean expired state
# state.setTimeoutDuration("1 hour")      # Spark 3.x
# state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + (1 * 60 * 60 * 1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint corruption or recovery
# MAGIC
# MAGIC **Symptom:** Stream fails to start with checkpoint-related errors, or loses
# MAGIC progress and reprocesses all data from the beginning.
# MAGIC
# MAGIC **Root cause:** Checkpoint files corrupted by inconsistent writes, manual
# MAGIC deletion, or multiple queries sharing the same checkpoint location.

# COMMAND ----------

# Fix 1 — always use a UNIQUE checkpoint per streaming query
# NEVER reuse a checkpoint path across two different queries
query1_checkpoint = "/Volumes/my_catalog/my_schema/my_volume/checkpoints/query1/"
query2_checkpoint = "/Volumes/my_catalog/my_schema/my_volume/checkpoints/query2/"

# Fix 2 — if checkpoint is corrupt, start fresh with a new location
# Add a timestamp suffix to auto-create a fresh checkpoint
from datetime import datetime

fresh_checkpoint = (
    f"/Volumes/my_catalog/my_schema/my_volume/checkpoints/query1_"
    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}/"
)

# Fix 3 — recover from checkpoint by modifying the offsets file
# (advanced: locate the offsets file in _spark_metadata, and revert to the
#  last known-good batch offset)

# Show checkpoint directory contents
display(dbutils.fs.ls("/Volumes/my_catalog/my_schema/my_volume/checkpoints/"))

# Fix 4 — for Delta source, set maxFilesPerTrigger to throttle reads
(
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", "50")
    .load("/Volumes/my_catalog/my_schema/my_volume/raw/")
    .writeStream
    .option("checkpointLocation", fresh_checkpoint)
    .trigger(availableNow=True)
    .toTable("my_catalog.my_schema.stream_sink")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Unity Catalog Issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table not found — namespace or default catalog
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# TABLE_OR_VIEW_NOT_FOUND — The table or view `my_schema`.`my_table` cannot be found.
# ```
# MAGIC
# MAGIC **Root cause:** Either the table doesn't exist, the default catalog isn't set,
# MAGIC or you're using a two-part name when Unity Catalog requires three-level namespaces.

# COMMAND ----------

# Fix 1 — always use the full three-level namespace
# catalog.schema.table  (NOT schema.table)
display(spark.sql("SELECT * FROM my_catalog.my_schema.my_table"))

# Fix 2 — set the default catalog (can be done at the SQL warehouse / cluster level)
spark.sql("USE CATALOG my_catalog")
spark.sql("USE SCHEMA my_schema")
display(spark.sql("SELECT * FROM my_table"))

# Check current catalog and schema
print(f"Catalog: {spark.catalog.currentCatalog()}")
print(f"Schema:  {spark.catalog.currentDatabase()}")

# Fix 3 — list all catalogs, schemas, and tables to find your resource
display(spark.sql("SHOW CATALOGS"))
display(spark.sql("SHOW SCHEMAS IN my_catalog"))
display(spark.sql("SHOW TABLES IN my_catalog.my_schema"))
display(spark.sql("SHOW VIEWS IN my_catalog.my_schema"))

# Fix 4 — check if the table was created in the legacy Hive metastore
display(spark.sql("SHOW TABLES IN hive_metastore.my_schema"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permission denied — GRANTs, ownership, inheritance
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# org.apache.spark.sql.AnalysisException: [PERMISSION_DENIED] User does
# not have SELECT on table `my_catalog`.`my_schema`.`my_table`.
# ```
# MAGIC
# MAGIC **Root cause:** The principal (user, group, or service principal) lacks the
# MAGIC required Unity Catalog privilege on the securable object.

# COMMAND ----------

# Fix 1 — check current grants on the object
display(spark.sql("SHOW GRANTS ON TABLE my_catalog.my_schema.my_table"))
display(spark.sql("SHOW GRANTS ON SCHEMA my_catalog.my_schema"))
display(spark.sql("SHOW GRANTS ON CATALOG my_catalog"))

# Fix 2 — check who owns the object
display(spark.sql("DESCRIBE EXTENDED my_catalog.my_schema.my_table"))
# Look for the Owner field in the output

# Fix 3 — check effective permissions (inherited from parent securables)
# Permissions inherit:  CATALOG -> SCHEMA -> TABLE
# If a principal has USAGE on catalog AND USAGE on schema, they can list tables
# If a principal has SELECT on schema, ALL tables in the schema inherit it

# Fix 4 — grant the required privilege (requires owner / admin)
# GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `user@domain.com`
# GRANT USAGE ON CATALOG my_catalog TO `group_name`
# GRANT USAGE ON SCHEMA my_catalog.my_schema TO `group_name`

# Fix 5 — verify your own identity token and group memberships
display(spark.sql("SELECT current_user(), current_groups()"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### External location issues — credential or path mismatch
# MAGIC
# MAGIC **Error message:**
# MAGIC ```
# FAILED_EXECUTE_UDF: Failed to create external location ... check storage credential
# ```
# MAGIC
# MAGIC **Root cause:** The storage credential used by the external location is invalid,
# MAGIC expired, or does not cover the target cloud storage path.

# COMMAND ----------

# Fix 1 — list and verify external locations
display(spark.sql("SHOW EXTERNAL LOCATIONS"))

# Fix 2 — describe the problematic location
display(spark.sql("DESCRIBE EXTERNAL LOCATION my_location"))
# Check: url (cloud path), credential_name, read_only property

# Fix 3 — verify the storage credential's health
display(spark.sql("DESCRIBE STORAGE CREDENTIAL my_credential"))

# Fix 4 — check the cloud path aligns with the credential's scope
# For Azure: the credential's scope must include the storage account and container
# For AWS: the IAM role must have s3:ListBucket and s3:GetObject on the bucket/prefix

# Fix 5 — test with a direct cloud URI (if you have direct permissions)
# Azure: abfss://container@account.dfs.core.windows.net/path/
# AWS:   s3://bucket/prefix/
# GCP:   gs://bucket/path/

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Diagnostic Tools

# COMMAND ----------

# MAGIC %md
# MAGIC ### DESCRIBE DETAIL — table-level metadata and file counts
# MAGIC
# MAGIC Key metrics: number of files, total size, partitioning, clustering columns,
# MAGIC table properties, reader/writer protocol versions.

# COMMAND ----------

from pyspark.sql import functions as F

detail = spark.sql("DESCRIBE DETAIL my_catalog.my_schema.my_table").collect()[0]

print(f"Location:          {detail.location}")
print(f"Table type:        {detail.format}")
print(f"Number of files:   {detail.numFiles}")
print(f"Total size (MB):   {round(detail.sizeInBytes / (1024 * 1024), 2)}")
print(f"Avg file size(MB): {round(detail.sizeInBytes / (1024 * 1024) / max(detail.numFiles, 1), 2)}")
print(f"Partition columns: {detail.partitionColumns}")
print(f"Clustering cols:   {detail.clusteringColumns}")
print(f"Properties:        {detail.properties}")

# Check for small-files warning
if detail.numFiles > 0 and (detail.sizeInBytes / detail.numFiles) < (16 * 1024 * 1024):
    print("WARNING: Average file size < 16 MB — run OPTIMIZE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DESCRIBE HISTORY — audit trail and operation metrics
# MAGIC
# MAGIC Shows every write/update/delete/merge operation with version, timestamp,
# MAGIC operation metrics (rows affected, files added/removed), and user identity.

# COMMAND ----------

# Full history (can be large for active tables; limit if needed)
display(spark.sql("""
    SELECT version, timestamp, operation, operationMetrics, userName
    FROM (DESCRIBE HISTORY my_catalog.my_schema.my_table)
    ORDER BY version DESC
    LIMIT 50
"""))

# Check for VACUUM operations — these are destructive
display(spark.sql("""
    SELECT version, timestamp, operation
    FROM (DESCRIBE HISTORY my_catalog.my_schema.my_table)
    WHERE operation = 'VACUUM END'
    ORDER BY version DESC
"""))

# Check write throughput over time
display(spark.sql("""
    SELECT
        DATE(timestamp) AS write_date,
        operation,
        COUNT(*) AS num_operations
    FROM (DESCRIBE HISTORY my_catalog.my_schema.my_table)
    GROUP BY 1, 2
    ORDER BY write_date DESC
    LIMIT 30
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLAIN EXTENDED — query plan analysis
# MAGIC
# MAGIC Shows the physical plan including partitioning, sorting, pruning statistics,
# MAGIC and join strategies. Use it to verify filters are pushed down.

# COMMAND ----------

# Basic EXPLAIN
spark.sql("""
    EXPLAIN EXTENDED
    SELECT a.id, b.name
    FROM my_catalog.my_schema.fact_table a
    JOIN my_catalog.my_schema.dim_table b ON a.dim_id = b.id
    WHERE a.event_date >= '2025-01-01'
""")

# Check if partition pruning is happening
# Look for "PartitionFilters" and "PushedFilters" in the plan output
# If PushedFilters is empty, statistics may be missing — run ANALYZE

# Verify statistics are populated
spark.sql("ANALYZE TABLE my_catalog.my_schema.fact_table COMPUTE STATISTICS FOR ALL COLUMNS")

# Check column-level stats
spark.sql("DESCRIBE EXTENDED my_catalog.my_schema.fact_table event_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark UI — stage, task, and shuffle diagnostics
# MAGIC
# MAGIC The Spark UI (port 4040 on the driver) is the most powerful diagnostic tool.
# MAGIC Key tabs: **Stages**, **Storage**, **Executors**, **SQL/DataFrame**.

# COMMAND ----------

# Programmatically access key metrics from the Spark UI
sc = spark.sparkContext

# Executor metrics
print(f"Total executors: {sc.getConf().get('spark.executor.instances', 'dynamic')}")
print(f"Executor memory: {sc.getConf().get('spark.executor.memory', 'default')}")
print(f"Executor cores:  {sc.getConf().get('spark.executor.cores', 'default')}")

# Adaptive Query Execution status
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled', 'false')}")
print(f"AQE coalesce: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled', 'false')}")

# Shuffle config
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions', 'not set')}")

# Key things to check in the Spark UI Stages tab:
#   - Input Size / Records per task — look for skew (one task >> others)
#   - Shuffle Read Size / Records — high shuffle = expensive joins/aggregations
#   - Spill (Memory) / Spill (Disk) — > 0 means executor OOM and disk writes
#   - GC Time — > 10% of task time = heap pressure
#   - Scheduler Delay — high values = too many tasks or cluster contention

# COMMAND ----------

# MAGIC %md
# MAGIC ### System tables — billing, audit, and compute history
# MAGIC
# MAGIC Unity Catalog system tables provide historical data for cost analysis,
# MAGIC audit trails, and usage patterns. Requires system table access.

# COMMAND ----------

# Check if system tables are available
display(spark.sql("SHOW CATALOGS"))
display(spark.sql("SHOW SCHEMAS IN system"))

# Billing / usage — query history and consumed DBUs
display(spark.sql("""
    SELECT
        account_id,
        workspace_id,
        usage_date,
        sku_name,
        usage_quantity
    FROM system.billing.usage
    WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
    ORDER BY usage_date DESC
    LIMIT 100
"""))

# Audit logs — who did what and when
display(spark.sql("""
    SELECT
        event_time,
        user_identity.email,
        action_name,
        request_params.object_type,
        request_params.object_name
    FROM system.access.audit
    WHERE event_date >= CURRENT_DATE - INTERVAL 7 DAYS
    ORDER BY event_time DESC
    LIMIT 100
"""))

# Compute history — cluster and warehouse usage
display(spark.sql("""
    SELECT
        usage_date,
        cluster_id,
        cluster_name,
        usage_quantity
    FROM system.compute.hourly_usage
    WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
    ORDER BY usage_date DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### FSCK REPAIR TABLE — fix metadata inconsistencies
# MAGIC
# MAGIC Used when the transaction log has entries for files that no longer exist on
# MAGIC storage, or when data files exist without corresponding log entries.

# COMMAND ----------

# Identify metadata issues (dry-run first)
spark.sql("FSCK REPAIR TABLE my_catalog.my_schema.my_table DRY RUN")

# Apply the repair (removes references to missing files,
# adds missing files to the log)
spark.sql("FSCK REPAIR TABLE my_catalog.my_schema.my_table")

# For severe cases, restore to a known-good version and re-append missing data
# spark.sql("RESTORE TABLE my_catalog.my_schema.my_table TO VERSION AS OF 42")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Quick Diagnostic Function
# MAGIC
# MAGIC Run this function against any table to get a consolidated diagnostic report.

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import functions as F


def diagnose_table(catalog, schema, table):
    """
    Run a comprehensive diagnostic report on a Delta table.

    Parameters
    ----------
    catalog : str   — Unity Catalog catalog name
    schema  : str   — Unity Catalog schema name
    table   : str   — table name

    Returns a dict with keys:
        - table_exists, format, storage_location
        - num_files, total_size_mb, avg_file_size_mb
        - num_partitions, partition_columns, clustering_columns
        - last_operation, last_operation_time, total_versions
        - recent_operations (last 10)
        - vacuum_history
        - tbl_properties retention settings
        - warnings (list of actionable issues)
    """

    fqn = f"{catalog}.{schema}.{table}"
    report = {
        "table": fqn,
        "diagnostic_time": datetime.now().isoformat(),
        "warnings": []
    }

    # -------------------------- 1. Table existence & format -------------------------
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0]
        report["table_exists"] = True
        report["format"] = detail.format
        report["storage_location"] = detail.location
    except Exception as e:
        report["table_exists"] = False
        report["error"] = str(e)
        report["warnings"].append(f"Table not found or inaccessible: {e}")
        return report

    # -------------------------- 2. File metrics -------------------------
    num_files = detail.numFiles or 0
    total_size = detail.sizeInBytes or 0
    report["num_files"] = num_files
    report["total_size_mb"] = round(total_size / (1024 * 1024), 2)

    if num_files > 0:
        avg_file_mb = round(total_size / (1024 * 1024) / num_files, 2)
        report["avg_file_size_mb"] = avg_file_mb
        if avg_file_mb < 16:
            report["warnings"].append(
                f"Small files detected: avg {avg_file_mb} MB/file. "
                f"Run OPTIMIZE {fqn} ZORDER BY (column)."
            )
        if num_files > 10000:
            report["warnings"].append(
                f"Very high file count ({num_files} files). "
                "Consider running OPTIMIZE and/or enabling auto-compaction."
            )
    else:
        report["avg_file_size_mb"] = 0

    # -------------------------- 3. Partitioning & clustering -------------------------
    report["partition_columns"] = detail.partitionColumns or []
    report["clustering_columns"] = detail.clusteringColumns or []
    report["num_partitions"] = detail.numPartitions or 0

    if detail.partitionColumns:
        partition_count_sql = (
            f"SELECT COUNT(DISTINCT {', '.join(detail.partitionColumns)}) "
            f"FROM {fqn}"
        )
        try:
            part_cnt = spark.sql(partition_count_sql).collect()[0][0]
            report["num_partitions_actual"] = part_cnt
            if part_cnt > 5000:
                report["warnings"].append(
                    f"High partition count ({part_cnt}). "
                    "Consider reducing granularity or using liquid clustering."
                )
        except Exception:
            pass

    # -------------------------- 4. History & operations -------------------------
    try:
        history = spark.sql(f"DESCRIBE HISTORY {fqn}")
        report["total_versions"] = history.count()

        latest = history.orderBy(F.col("version").desc()).limit(1).collect()[0]
        report["last_operation"] = latest.operation
        report["last_operation_time"] = str(latest.timestamp)

        recent_ops = history.orderBy(F.col("version").desc()).limit(10).collect()
        report["recent_operations"] = [
            {
                "version": r["version"],
                "operation": r["operation"],
                "timestamp": str(r["timestamp"]),
                "user": r.get("userName", "unknown")
            }
            for r in recent_ops
        ]

        vacuums = history.filter(F.col("operation").contains("VACUUM")).collect()
        report["vacuum_history"] = [
            {"version": r["version"], "timestamp": str(r["timestamp"])}
            for r in vacuums
        ]

        last_vacuum = vacuums[-1].timestamp if vacuums else None
        if last_vacuum:
            days_since = (datetime.now() - last_vacuum).days if hasattr(last_vacuum, "days") else "unknown"
            report["days_since_last_vacuum"] = days_since

    except Exception as e:
        report["warnings"].append(f"Could not read history: {e}")

    # -------------------------- 5. Retention properties -------------------------
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()
        for row in props:
            key = row["key"]
            val = row["value"]
            if "logRetentionDuration" in key:
                report["log_retention"] = val
            if "deletedFileRetentionDuration" in key:
                report["deleted_file_retention"] = val
            if "delta.autoOptimize" in key:
                report["auto_optimize_settings"] = report.get("auto_optimize_settings", {})
                report["auto_optimize_settings"][key] = val
    except Exception:
        pass

    # -------------------------- 6. Check for recent OPTIMIZE -------------------------
    try:
        opt_count = history.filter(F.col("operation").contains("OPTIMIZE")).count()
        if opt_count == 0 and num_files > 100:
            report["warnings"].append(
                "Table has never been optimized. Consider running OPTIMIZE periodically."
            )
    except Exception:
        pass

    # -------------------------- Summary output -------------------------
    print("=" * 70)
    print(f"  DIAGNOSTIC REPORT: {fqn}")
    print(f"  Generated at: {report['diagnostic_time']}")
    print("=" * 70)
    print(f"  Format:              {report.get('format', 'N/A')}")
    print(f"  Storage:             {report.get('storage_location', 'N/A')}")
    print(f"  Files:               {report.get('num_files', 'N/A')}")
    print(f"  Total size:          {report.get('total_size_mb', 'N/A')} MB")
    print(f"  Avg file size:       {report.get('avg_file_size_mb', 'N/A')} MB")
    print(f"  Partition cols:      {report.get('partition_columns', [])}")
    print(f"  Clustering cols:     {report.get('clustering_columns', [])}")
    print(f"  Total versions:      {report.get('total_versions', 'N/A')}")
    print(f"  Last operation:      {report.get('last_operation', 'N/A')} "
          f"at {report.get('last_operation_time', 'N/A')}")
    print(f"  Log retention:       {report.get('log_retention', 'not set')}")
    print(f"  File retention:      {report.get('deleted_file_retention', 'not set')}")
    print("-" * 70)
    if report["warnings"]:
        print(f"  WARNINGS ({len(report['warnings'])}):")
        for i, w in enumerate(report["warnings"], 1):
            print(f"    {i}. {w}")
    else:
        print("  No warnings detected.")
    print("=" * 70)

    return report


# ---------------------------------------------------------------
# Usage — replace with your own catalog.schema.table
# ---------------------------------------------------------------
# result = diagnose_table("my_catalog", "my_schema", "my_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix — Quick Reference Commands

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table inspection (no code — copy/paste into a SQL cell)
# MAGIC
# MAGIC | Command | Purpose |
# MAGIC |---------|---------|
# MAGIC | `DESCRIBE DETAIL my_table` | File count, size, partitions, clustering |
# MAGIC | `DESCRIBE HISTORY my_table` | All operations, versions, metrics |
# MAGIC | `DESCRIBE EXTENDED my_table` | Full metadata dump |
# MAGIC | `SHOW TBLPROPERTIES my_table` | Delta config, retention settings |
# MAGIC | `EXPLAIN EXTENDED SELECT ...` | Physical query plan |
# MAGIC | `OPTIMIZE my_table ZORDER BY (col)` | Compact files + data skipping |
# MAGIC | `VACUUM my_table` | Remove old files (retention-gated) |
# MAGIC | `RESTORE TABLE my_table TO VERSION AS OF N` | Time-travel restore |
# MAGIC | `FSCK REPAIR TABLE my_table` | Fix metadata inconsistencies |
# MAGIC | `ANALYZE TABLE my_table COMPUTE STATISTICS` | Update query-plan statistics |
# MAGIC | `ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS` | Column-level stats |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Useful Spark configs (set at session or cluster level)
# MAGIC
# MAGIC ```python
# MAGIC # AQE — Adaptive Query Execution
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# MAGIC spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# MAGIC spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")
# MAGIC
# MAGIC # Shuffle
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "400")
# MAGIC
# MAGIC # Broadcast hint threshold
# MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
# MAGIC
# MAGIC # Delta
# MAGIC spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
# MAGIC spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
# MAGIC spark.conf.set("spark.databricks.delta.merge.enableLowShuffle", "true")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Useful system table queries (SQL)
# MAGIC
# MAGIC ```sql
# MAGIC -- Recent failed jobs
# MAGIC SELECT *
# MAGIC FROM system.query.history
# MAGIC WHERE status = 'FAILED'
# MAGIC   AND query_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC ORDER BY query_start_time DESC;
# MAGIC
# MAGIC -- Top users by DBU consumption
# MAGIC SELECT
# MAGIC   account_id,
# MAGIC   usage_date,
# MAGIC   sku_name,
# MAGIC   SUM(usage_quantity) AS total_dbu
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY total_dbu DESC;
# MAGIC
# MAGIC -- Recent table drops or creates
# MAGIC SELECT
# MAGIC   event_time,
# MAGIC   user_identity.email,
# MAGIC   action_name,
# MAGIC   request_params.object_name
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name IN ('createTable', 'deleteTable', 'dropTable')
# MAGIC   AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC ORDER BY event_time DESC;
# MAGIC ```

# COMMAND ----------
