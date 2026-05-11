# Databricks notebook source
# MAGIC %md
# MAGIC # ULTIMATE DATABRICKS CHEAT SHEET
# MAGIC
# MAGIC **Reference notebook covering daily-use commands, syntax, and configuration across the Databricks ecosystem.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Table of Contents
# MAGIC
# MAGIC | # | Section | Description |
# MAGIC |---|---------|-------------|
# MAGIC | 1 | Delta Lake | CRUD, Merge, Time Travel, Clone, CDF, Optimize |
# MAGIC | 2 | Spark Config | shuffle, broadcast, memory, AQE settings |
# MAGIC | 3 | DataFrame API | read/write, joins, aggregations, windows, complex types |
# MAGIC | 4 | Spark SQL | CTEs, PIVOT, MERGE, generated columns, constraints |
# MAGIC | 5 | Streaming | readStream, writeStream, triggers, watermarks, foreachBatch |
# MAGIC | 6 | Performance | pushdown, hints, caching, EXPLAIN, ANALYZE |
# MAGIC | 7 | Unity Catalog | GRANT/REVOKE, row filters, column masks, info_schema |
# MAGIC | 8 | Workflows | widgets, %run, dbutils.notebook, secrets, task values |
# MAGIC | 9 | File I/O | CSV, JSON, Parquet, Delta, COPY INTO, schema inference |
# MAGIC |10 | Troubleshooting | DESCRIBE DETAIL/HISTORY, EXPLAIN, FSCK REPAIR, logs |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Delta Lake Commands
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **CREATE TABLE** | `CREATE TABLE my_db.my_table (id BIGINT, name STRING, ts TIMESTAMP) USING DELTA LOCATION '/path/to/data'` | Add `TBLPROPERTIES` for advanced config |
# MAGIC | **CREATE TABLE AS SELECT** | `CREATE OR REPLACE TABLE t USING DELTA AS SELECT ...` | CTAS pattern, auto-infers schema |
# MAGIC | **INSERT** | `INSERT INTO target [(col1, col2)] VALUES (...), (...)` or `INSERT INTO target SELECT ...` | Use `INSERT OVERWRITE` to replace data |
# MAGIC | **MERGE (Upsert)** | `MERGE INTO target t USING source s ON t.key = s.key WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...` | Supports `UPDATE SET *`, `INSERT *`; can DELETE matched rows; multiple `WHEN` clauses allowed |
# MAGIC | **DELETE** | `DELETE FROM table WHERE condition` | Can target partitions or use subqueries |
# MAGIC | **UPDATE** | `UPDATE table SET col = val WHERE condition` | Available since DBR 7.3+ |
# MAGIC | **OPTIMIZE** | `OPTIMIZE table [WHERE date = '2025-01-01'] [ZORDER BY (col1, col2)]` | Compacts small files; ZORDER for multi-dimensional clustering |
# MAGIC | **VACUUM** | `VACUUM table [RETAIN n HOURS]` | Default retain = 7 days; run after OPTIMIZE; does NOT delete live data |
# MAGIC | **RESTORE** | `RESTORE TABLE table TO VERSION AS OF <versionOrTimestamp>` | Time-travel rollback; restores metadata AND data |
# MAGIC | **Time Travel (Query)** | `SELECT * FROM table VERSION AS OF 5` or `...TIMESTAMP AS OF '2025-06-01'` or `...@v5` | Read-only; use RESTORE to rollback permanently |
# MAGIC | **Change Data Feed** | `ALTER TABLE table SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` | Then: `SELECT * FROM table_changes('table', start, end)` |
# MAGIC | **CLONE (Shallow)** | `CREATE OR REPLACE TABLE target SHALLOW CLONE source` | Copies metadata only; cheap, fast; `DEEP CLONE` copies data too |
# MAGIC | **DESCRIBE DETAIL** | `DESCRIBE DETAIL table` | Shows format, location, partition columns, size, numFiles |
# MAGIC | **DESCRIBE HISTORY** | `DESCRIBE HISTORY table` | Shows version, timestamp, operation, operationMetrics |
# MAGIC | **SHOW COLUMNS** | `SHOW COLUMNS IN table` | Column list with data types |
# MAGIC | **ALTER TABLE (properties)** | `ALTER TABLE table SET TBLPROPERTIES ('delta.logRetentionDuration'='30 days')` | Also: `delta.deletedFileRetentionDuration`; affects VACUUM behavior |
# MAGIC | **GENERATED COLUMNS** | `CREATE TABLE t (id BIGINT, date DATE GENERATED ALWAYS AS (CAST(ts AS DATE)))` | Useful for partition columns derived from timestamp |
# MAGIC | **CONVERT TO DELTA** | `CONVERT TO DELTA parquet_table [PARTITIONED BY (col)]` | Converts Parquet/Iceberg tables in-place |
# MAGIC | **REORG TABLE** | `REORG TABLE table APPLY (PURGE)` | De-dups rows with Liquid clustering; 15.4+ |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake — Executable Examples

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a Delta table and load sample data

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime

df = spark.range(0, 10000).select(
    F.col("id").cast("bigint"),
    (F.col("id") % 100).alias("category_id"),
    F.round(F.rand() * 500, 2).alias("amount"),
    F.date_add(F.lit("2025-01-01"), (F.col("id") % 365).cast("int")).alias("event_date")
)

df.write.mode("overwrite").saveAsTable("main.default.cheatsheet_demo")
display(spark.table("main.default.cheatsheet_demo").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Time Travel: read a specific version

# COMMAND ----------

spark.sql("DESCRIBE HISTORY main.default.cheatsheet_demo").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### MERGE pattern (upsert from source table to target)

# COMMAND ----------

df_updates = spark.range(0, 5).select(
    F.col("id").cast("bigint"),
    F.lit(999).alias("category_id"),
    F.lit(99.99).alias("amount"),
    F.lit("2025-06-01").cast("date").alias("event_date")
)
df_updates.createOrReplaceTempView("updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO main.default.cheatsheet_demo t
# MAGIC USING updates s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.category_id = s.category_id,
# MAGIC   t.amount = s.amount,
# MAGIC   t.event_date = s.event_date
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC #### OPTIMIZE with ZORDER

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE main.default.cheatsheet_demo ZORDER BY (category_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Spark Configuration
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **Shuffle Partitions** | `spark.conf.set("spark.sql.shuffle.partitions", 200)` | Default 200; tune to 2–4× number of cores |
# MAGIC | **Auto Broadcast Join** | `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)` | Default 10 MB (-1 disables); affects shuffle-vs-broadcast decisions |
# MAGIC | **Adaptive Query Execution** | `spark.conf.set("spark.sql.adaptive.enabled", True)` `spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)` | DBR 7.3+; auto-optimizes shuffle partitions |
# MAGIC | **AQE Skew Join** | `spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)` | Handles skewed data in joins; enabled by default 10.4+ |
# MAGIC | **Partition Overwrite Mode** | `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")` | `dynamic` only overwrites partitions present in new data |
# MAGIC | **Driver Memory** | Set at cluster creation: `spark.driver.memory 4g` | Also `spark.driver.maxResultSize` for large collect() |
# MAGIC | **Executor Memory** | Set at cluster creation: `spark.executor.memory 8g` | Keep `spark.executor.memoryOverhead` in mind (~10%+ of executor) |
# MAGIC | **Photon** | Set at cluster creation: enable Photon Acceleration | Vectorized engine; 2–8× faster for SQL/DataFrame ops |
# MAGIC | **Delta Auto Compaction** | `spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)` | Compacts small files during writes (OPTIMIZE-like) |
# MAGIC | **Optimized Writes** | `spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)` | Automatic `df.repartition(partitionCols)` before write |
# MAGIC | **Arrow/Columnar batch size** | `spark.conf.set("spark.sql.parquet.enableVectorizedReader", True)` `spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 20000)` | Vectorized Parquet reader; columnar batch size for caching |
# MAGIC | **Read Delta as of version** | `spark.read.option("versionAsOf", 5).table("tablename")` | Time travel via DataFrame reader |
# MAGIC | **ANSII SQL Mode** | `spark.conf.set("spark.sql.ansi.enabled", True)` | Throws errors on invalid operations instead of NULL |
# MAGIC | **DEFAULT Catalog** | `spark.conf.set("spark.databricks.sql.initial.catalog.name", "hive_metastore")` | Set at session or cluster level |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark Config — Executable Examples

# COMMAND ----------

# Print current spark config values
conf_keys = [
    "spark.sql.shuffle.partitions",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.databricks.delta.optimizeWrite.enabled",
    "spark.databricks.delta.autoCompact.enabled",
    "spark.sql.ansi.enabled",
]
for key in conf_keys:
    print(f"{key:>55s} = {spark.conf.get(key, 'NOT SET')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. DataFrame API Quick Reference
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **Read Table** | `df = spark.table("cat.schema.table")` or `spark.read.table(...)` | Preferred over `spark.sql("SELECT *...")` |
# MAGIC | **Read Files** | `spark.read.format("delta").load("/path")` | Also `.csv`, `.json`, `.parquet`, `.orc` |
# MAGIC | **Write Table** | `df.write.mode("overwrite").saveAsTable("name")` | Modes: append, overwrite, ignore, error |
# MAGIC | **Write Files** | `df.write.format("delta").save("/path")` | Supports partitionedBy, bucketBy |
# MAGIC | **Select Columns** | `df.select("col1", "col2")` or `df.select(F.col("c1"), F.expr("c2+1"))` | Use `F.col()` for transformations |
# MAGIC | **Select with expr** | `df.selectExpr("col1", "col1 + col2 as total")` | SQL-like expressions as strings |
# MAGIC | **Filter / Where** | `df.filter(F.col("amount") > 100)` or `df.where("amount > 100")` | Chain conditions: `df.filter((F.col("a")>10) & (F.col("b")<5))` |
# MAGIC | **Filter NULLs** | `df.filter(F.col("col").isNull())` or `df.na.drop(subset=["col1","col2"])` | `df.na.fill({"col": 0})` to replace NULLs |
# MAGIC | **Add / Rename Columns** | `df.withColumn("new", F.col("old") * 2)` or `df.withColumnRenamed("old","new")` | Use `drop("col")` to remove |
# MAGIC | **CASE WHEN** | `F.when(F.col("x") > 0, "positive").otherwise("non-positive")` | Chain `.when()` for multiple conditions |
# MAGIC | **Cast Types** | `F.col("amount").cast("double")` or `F.col("date_str").cast("date")` | Use `DataTypes` class for complex types |
# MAGIC | **String Functions** | `F.concat`, `F.split`, `F.regexp_replace`, `F.substring`, `F.lower`, `F.upper`, `F.trim`, `F.length` | Pattern: `F.regexp_replace("col", "pattern", "replacement")` |
# MAGIC | **Date / Timestamp** | `F.current_date()`, `F.current_timestamp()`, `F.date_add`, `F.date_sub`, `F.datediff`, `F.to_date`, `F.to_timestamp`, `F.date_format` | Cast string to date: `F.to_date("col", "yyyy-MM-dd")` |
# MAGIC | **Aggregation** | `df.groupBy("cat").agg(F.sum("amt").alias("total"), F.count("*"))` | Also: `avg`, `min`, `max`, `stddev`, `approx_count_distinct` |
# MAGIC | **Pivot** | `df.groupBy("cat").pivot("year", [2024,2025]).sum("amt")` | Provide explicit pivot values for performance |
# MAGIC | **Joins** | `df1.join(df2, on="key", how="inner")` | how = inner, left, right, full, left_semi, left_anti, cross |
# MAGIC | **Join on expressions** | `df1.join(df2, df1.key == df2.key, "left")` | Use `broadcast(df2)` hint for small tables |
# MAGIC | **Sort / OrderBy** | `df.orderBy(F.col("date").desc(), F.col("id").asc())` | `F.col("x").asc_nulls_last()` controls null ordering |
# MAGIC | **Limit / Sample** | `df.limit(n)`, `df.sample(fraction=0.1, seed=42)` | limit() does NOT sample; sample uses Bernoulli by default |
# MAGIC | **Distinct / Drop Duplicates** | `df.distinct()` or `df.dropDuplicates(["col1","col2"])` | dropDuplicates allows column subset |
# MAGIC | **Union** | `df1.union(df2)` (by position) or `df1.unionByName(df2, allowMissingColumns=True)` | `unionByName` matches by column name |
# MAGIC | **Explode (arrays)** | `df.select(F.explode("array_col").alias("item"))` | Explodes each element to new row |
# MAGIC | **Collect List/Set** | `df.groupBy("cat").agg(F.collect_list("val"), F.collect_set("val"))` | collect_set deduplicates |
# MAGIC | **Structs** | `F.struct("col1","col2").alias("nested")` or `F.col("s").getField("f1")` | Access: `F.col("struct_col.field")` |
# MAGIC | **Map / Array functions** | `F.map_from_arrays`, `F.array_contains`, `F.size`, `F.element_at`, `F.transform` | Higher-order functions introduced in Spark 2.4+ |
# MAGIC | **JSON Parsing** | `F.from_json("str_col", schema)` | Requires explicit schema or `schema_of_json` |
# MAGIC | **User-Defined Function** | `my_udf = F.udf(lambda x: ..., returnType=StringType())` | Prefer built-in functions over UDFs for performance |
# MAGIC | **Pandas UDF (Vectorized)** | `@F.pandas_udf(returnType=DoubleType())` with `F.PandasUDFType.GROUPED_MAP` or `SCALAR` | Much faster than row-based UDFs |

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame API — Executable Examples

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.range(1, 1000)

# Demonstrate chaining common operations
result = (
    df
    .withColumn("random_val", F.rand())
    .withColumn("bucket", (F.col("id") % 10).cast("int"))
    .filter(F.col("random_val") > 0.8)
    .groupBy("bucket")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("random_val"), 4).alias("avg_val"),
    )
    .orderBy("bucket")
)
result.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

from pyspark.sql.window import Window

df = spark.table("main.default.cheatsheet_demo")
window_spec = Window.partitionBy("category_id").orderBy(F.col("amount").desc())

df.withColumn("rank", F.rank().over(window_spec)) \
  .withColumn("row_num", F.row_number().over(window_spec)) \
  .withColumn("pct_rank", F.percent_rank().over(window_spec)) \
  .filter("rank <= 3") \
  .orderBy("category_id", "rank") \
  .show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Spark SQL Quick Reference
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **CREATE TABLE** | `CREATE TABLE t (id INT, name STRING, dob DATE) USING DELTA` | Use `USING` clause; Delta is default in DBR |
# MAGIC | **CTE (WITH clause)** | `WITH cte1 AS (SELECT ...), cte2 AS (SELECT ...) SELECT ... FROM cte1 JOIN cte2 ...` | Supports multiple CTEs; recursive CTEs not supported |
# MAGIC | **SELECT basic** | `SELECT col1, col2 + 1 AS incremented FROM table WHERE col3 > 100 LIMIT 10` | All standard SQL |
# MAGIC | **CASE statement** | `SELECT CASE WHEN amount > 100 THEN 'HIGH' WHEN amount > 50 THEN 'MED' ELSE 'LOW' END AS tier FROM t` | Same as DataFrame `F.when()` |
# MAGIC | **GROUP BY + HAVING** | `SELECT cat, SUM(amt) FROM t GROUP BY cat HAVING SUM(amt) > 1000` | HAVING filters after aggregation |
# MAGIC | **ORDER BY / SORT BY** | `SELECT ... ORDER BY col1 DESC, col2 ASC NULLS LAST` | `DISTRIBUTE BY col SORT BY col` for final write layout |
# MAGIC | **JOIN** | `SELECT ... FROM t1 [INNER|LEFT|RIGHT|FULL|SEMI|ANTI] JOIN t2 ON ...` | Supports CROSS JOIN, NATURAL JOIN |
# MAGIC | **Subquery in WHERE** | `SELECT * FROM t WHERE col IN (SELECT col FROM t2)` | Scalar subqueries: `WHERE col = (SELECT MAX(c) FROM t2)` |
# MAGIC | **PIVOT** | `SELECT * FROM (SELECT cat, yr, amt FROM t) PIVOT (SUM(amt) FOR yr IN (2024, 2025))` | Explicit pivot values improve plan quality |
# MAGIC | **UNPIVOT / STACK** | `SELECT id, stack(2, 'col1', col1, 'col2', col2) AS (metric_name, metric_value) FROM t` | Melts wide columns to long format |
# MAGIC | **Window Functions** | `SELECT col, RANK() OVER (PARTITION BY cat ORDER BY amt DESC) FROM t` | Also: ROW_NUMBER, DENSE_RANK, LAG, LEAD, SUM, AVG, FIRST_VALUE, LAST_VALUE |
# MAGIC | **MERGE INTO** | `MERGE INTO target AS t USING source AS s ON t.key = s.key WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT (...) VALUES (...)` | See Delta Lake section for full syntax |
# MAGIC | **INSERT OVERWRITE** | `INSERT OVERWRITE target SELECT ...` | Replaces entire table or specified partitions |
# MAGIC | **CREATE TABLE with GENERATED COLUMNS** | `CREATE TABLE t (ts TIMESTAMP, event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))) PARTITIONED BY (event_date)` | Generated column MUST match partition column |
# MAGIC | **Constraints** | `CREATE TABLE t (id INT NOT NULL, name STRING, CONSTRAINT pk PRIMARY KEY(id), CONSTRAINT ck CHECK (amount > 0))` | NOT NULL enforced on writes; PK/CHECK not enforced on writes (metadata only) |
# MAGIC | **IDENTITY Columns** | `CREATE TABLE t (id BIGINT GENERATED ALWAYS AS IDENTITY, data STRING)` | Auto-increment; DBR 10.4+ |
# MAGIC | **LIQUID CLUSTERING** | `CREATE TABLE t (...) CLUSTER BY (col1, col2)` | Alternative to partitioning + ZORDER; self-tuning |
# MAGIC | **ANALYZE TABLE** | `ANALYZE TABLE t COMPUTE STATISTICS [FOR COLUMNS col1, col2]` | Helps cost-based optimizer choose better plans |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL — Executable Examples

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CTE + Window function example
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     category_id,
# MAGIC     amount,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY amount DESC) AS rn
# MAGIC   FROM main.default.cheatsheet_demo
# MAGIC )
# MAGIC SELECT category_id, AVG(amount) AS avg_amount, COUNT(*) AS row_count
# MAGIC FROM ranked
# MAGIC WHERE rn <= 10
# MAGIC GROUP BY category_id
# MAGIC ORDER BY category_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### PIVOT example

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT category_id, event_date, amount
# MAGIC   FROM main.default.cheatsheet_demo
# MAGIC   WHERE event_date BETWEEN '2025-01-01' AND '2025-06-30'
# MAGIC )
# MAGIC PIVOT (ROUND(AVG(amount), 2) FOR category_id IN (1, 2, 3, 4, 5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Streaming Quick Reference
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **readStream — Delta** | `spark.readStream.table("source_table")` or `spark.readStream.format("delta").load("/path")` | Best practice: read from Delta tables |
# MAGIC | **readStream — Kafka** | `spark.readStream.format("kafka").option("kafka.bootstrap.servers", "...").option("subscribe", "topic").load()` | Key Auto Loader alternative for Kafka |
# MAGIC | **readStream — Auto Loader** | `spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", ...).load("/input")` | Recommended for cloud file ingestion |
# MAGIC | **writeStream — Delta** | `stream.writeStream.format("delta").outputMode("append").option("checkpointLocation", ...).table("target")` | Checkpoint required for all streams |
# MAGIC | **Output Modes** | `.outputMode("append")` `.outputMode("complete")` `.outputMode("update")` | Append: new rows only; Complete: full result each trigger; Update: changed rows |
# MAGIC | **Trigger Types** | `.trigger(processingTime="10 seconds")` `.trigger(availableNow=True)` `.trigger(once=True)` `.trigger(continuous="1 second")` | `availableNow` processes all pending then stops; best for scheduled jobs |
# MAGIC | **foreachBatch** | `stream.writeStream.foreachBatch(lambda df, epoch_id: transform_and_write(df)).start()` | Arbitrary DataFrame logic per micro-batch |
# MAGIC | **Watermarks** | `df.withWatermark("event_time", "10 minutes")` | Enables late-data handling + state cleanup; required for aggregations |
# MAGIC | **Stream-Stream Join** | `s1.join(s2, expr="id", "inner").withWatermark("ts", "30 minutes")` | Requires watermarks on both sides |
# MAGIC | **Rate Source (Testing)** | `spark.readStream.format("rate").option("rowsPerSecond", 100).load()` | Generates test data; use with `trigger(once=True)` for demos |
# MAGIC | **queryName** | `.queryName("my_stream")` | Names the streaming query (visible in UI; accessible via `spark.streams.get("my_stream")`) |
# MAGIC | **Stop Stream** | `spark.streams.active` or `q = stream.start(); q.stop()` or `spark.streams.get("name").stop()` | Manage all active streams via `spark.streams` |
# MAGIC | **Last Progress / Status** | `q.lastProgress`, `q.status`, `q.recentProgress` | Metrics: inputRowsPerSecond, processedRowsPerSecond, state metrics |
# MAGIC | **Auto Loader Schema Evolution** | `.option("cloudFiles.schemaEvolutionMode", "addNewColumns")` | Other modes: `rescue`, `failOnNewColumns`, `none` |
# MAGIC | **Auto Loader Schema Hints** | `.option("cloudFiles.schemaHints", "col1 int, col2 string")` | Inject missing schema info for semi-structured files |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming — Executable Examples

# COMMAND ----------

# Read a Delta table as a stream (append only)
# This is a reference pattern — uncomment to run live streaming

# stream_df = spark.readStream.table("main.default.cheatsheet_demo")
#
# query = (
#     stream_df
#     .writeStream
#     .outputMode("append")
#     .format("delta")
#     .option("checkpointLocation", "/tmp/cheatsheet_stream_cp")
#     .trigger(availableNow=True)
#     .toTable("main.default.cheatsheet_stream_output")
# )
# query.awaitTermination()

print("Streaming reference: use 'availableNow' trigger for scheduled batch-stream workflows.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### foreachBatch Pattern

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC def process_batch(df, epoch_id):
# MAGIC     # Apply arbitrary transformations per micro-batch
# MAGIC     result = df.groupBy("category_id").agg(F.sum("amount"))
# MAGIC     result.write.mode("append").saveAsTable("aggregated_table")
# MAGIC
# MAGIC stream.writeStream \
# MAGIC     .foreachBatch(process_batch) \
# MAGIC     .option("checkpointLocation", "/checkpoint/path") \
# MAGIC     .trigger(processingTime="1 minute") \
# MAGIC     .start()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Performance Tuning
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **Predicate Pushdown** | Happens automatically. Verify: `df.filter("col = 1").explain()` shows `PartitionFilters:` | Works with partitioned tables; use `format("parquet")` with `pushDownFilters` |
# MAGIC | **Partition Pruning** | `df.filter(F.col("event_date") == "2025-06-01")` | Partition filter in WHERE clause avoids scanning all partitions |
# MAGIC | **Broadcast Hint** | `from pyspark.sql.functions import broadcast; large_df.join(broadcast(small_df), "key")` | Small table <10 MB (or `autoBroadcastJoinThreshold`) sent to all executors; avoids shuffle |
# MAGIC | **Repartition** | `df.repartition(200, "country")` | Causes full shuffle; use before write; partitions on column |
# MAGIC | **Coalesce** | `df.coalesce(8)` | Reduces partitions WITHOUT shuffle (narrow dependency); use after filter |
# MAGIC | **Cache / Persist** | `df.cache()` or `df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)` | Call `.count()` or write to trigger; `unpersist()` to free memory |
# MAGIC | **Local Checkpoint** | `df.localCheckpoint(eager=True)` | Truncates lineage on executor local storage; good for iterative algorithms |
# MAGIC | **EXPLAIN** | `df.explain("extended")` or `spark.sql("EXPLAIN EXTENDED ...")` | Levels: simple, extended, codegen, cost, formatted |
# MAGIC | **EXPLAIN PLAN** | `EXPLAIN EXTENDED SELECT ...` | Shows parsed, analyzed, optimized, physical plan |
# MAGIC | **ANALYZE TABLE** | `ANALYZE TABLE t COMPUTE STATISTICS` or `ANALYZE TABLE t COMPUTE STATISTICS FOR COLUMNS c1, c2` | Column-level stats help cost-based optimizer |
# MAGIC | **OPTIMIZE delta table** | `OPTIMIZE table [WHERE ...] [ZORDER BY (c1, c2)]` | Bin-packing to reduce small files; ZORDER for multi-dim clustering |
# MAGIC | **REORG TABLE** | `REORG TABLE t APPLY (PURGE)` | 15.4+; de-duplicates rows when using liquid clustering |
# MAGIC | **Photon** | Built-in — enable at cluster creation | 2–8× faster for standard SQL/DataFrame ops |
# MAGIC | **Adaptive Query Execution** | `spark.conf.set("spark.sql.adaptive.enabled", True)` | Dynamically coalesces post-shuffle partitions at runtime |
# MAGIC | **Dynamic Partition Pruning** | `spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", True)` | Enabled by default; prunes partitions at scan time based on broadcast side |
# MAGIC | **Join Reorder** | `spark.conf.set("spark.sql.cbo.joinReorder.enabled", True)` | Cost-based optimizer reorders joins; requires stats |
# MAGIC | **Skew Hint** | `df.hint("skew", "join_key")` | Tells optimizer key is skewed; enables split join strategy |
# MAGIC | **File Size Tuning** | `spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)` | Default 128 MB; smaller values = more partitions |
# MAGIC | **File Open Cost** | `spark.conf.set("spark.sql.files.openCostInBytes", 4194304)` | Default 4 MB; smaller = more parallelism for small files |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance — Executable Examples

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import broadcast

df = spark.table("main.default.cheatsheet_demo")

# EXPLAIN — shows Spark's physical plan
print("=== EXPLAIN (simple) ===")
df.filter("category_id = 5").explain()
print("\n=== EXPLAIN (extended) ===")
df.filter("category_id = 5").explain("extended")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Caching and broadcast demonstration

# COMMAND ----------

small_lookup = spark.range(1, 101).select(F.col("id").alias("lookup_id"), F.concat(F.lit("cat_"), F.col("id").cast("string")).alias("name"))
small_lookup.cache()
small_lookup.count()

joined = df.join(broadcast(small_lookup), df.category_id == small_lookup.lookup_id, "left")
joined.explain()
joined.select("id", "name", "amount").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Unity Catalog Commands
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **CREATE CATALOG** | `CREATE CATALOG [IF NOT EXISTS] my_catalog` | Catalogs are top-level containers |
# MAGIC | **CREATE SCHEMA/DATABASE** | `CREATE SCHEMA [IF NOT EXISTS] my_catalog.my_schema [LOCATION '...']` | Schema = Database; must be inside a catalog |
# MAGIC | **USE CATALOG / SCHEMA** | `USE CATALOG my_catalog; USE SCHEMA my_schema` | Sets default context for session |
# MAGIC | **GRANT (privileges)** | `GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `user@domain.com`` | Also `GRANT USAGE ON CATALOG ...`, `GRANT MODIFY`, `GRANT CREATE TABLE`, `GRANT EXECUTE` |
# MAGIC | **REVOKE** | `REVOKE SELECT ON TABLE my_catalog.my_schema.my_table FROM `user@domain.com`` | Revokes previously granted permissions |
# MAGIC | **SHOW GRANTS** | `SHOW GRANTS ON TABLE my_catalog.my_schema.my_table` or `SHOW GRANTS `user@domain.com`` | Shows effective permissions |
# MAGIC | **CREATE VOLUME** | `CREATE VOLUME [IF NOT EXISTS] my_volume` | Object store path for non-tabular data |
# MAGIC | **Row Filters** | `CREATE FUNCTION my_filter(col1 INT) RETURN col1 > 100;` then `ALTER TABLE t SET ROW FILTER my_filter ON (col1)` | Row-level security; DBR 10.4+ |
# MAGIC | **Column Masks** | `CREATE FUNCTION my_mask(col STRING) RETURN CASE WHEN current_user() = 'admin' THEN col ELSE '***' END;` then `ALTER TABLE t ALTER COLUMN col SET MASK my_mask` | Column-level masking; DBR 10.4+ |
# MAGIC | **FOREIGN CATALOGS** | `CREATE FOREIGN CATALOG my_foreign USING CONNECTION conn_name OPTIONS (database 'my_db');` | Lakehouse Federation for external databases (e.g., Postgres, Snowflake) |
# MAGIC | **SHARE (Delta Sharing)** | `CREATE SHARE my_share; ALTER SHARE my_share ADD TABLE my_table; GRANT SELECT ON SHARE my_share TO recipient` | Cross-org data sharing; recipient can read as if it were native |
# MAGIC | **information_schema.tables** | `SELECT * FROM my_catalog.information_schema.tables WHERE table_schema = 'my_schema'` | Metadata discovery across all managed objects |
# MAGIC | **information_schema.columns** | `SELECT * FROM my_catalog.information_schema.columns WHERE table_name = 'my_table' ORDER BY ordinal_position` | Column-level metadata |
# MAGIC | **information_schema.table_constraints** | `SELECT * FROM my_catalog.information_schema.table_constraints` | PK/FK constraint metadata |
# MAGIC | **SHOW TABLE EXTENDED** | `SHOW TABLE EXTENDED IN my_schema LIKE 'my_table*'` | Shows table type, location, provider, partition info |
# MAGIC | **SET TAGS** | `ALTER TABLE t SET TAGS ('cost_center' = 'analytics', 'data_classification' = 'pii')` | Metadata tagging; DBR 13.2+ |
# MAGIC | **CHECK ACCESS** | `SHOW GRANTS ON CATALOG main` or query `system.information_schema.object_privileges` | Audit permissions |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog — Executable Examples

# COMMAND ----------

# Inspect UC metadata via information_schema
spark.sql("""
    SELECT table_catalog, table_schema, table_name, table_type, data_source_format
    FROM system.information_schema.tables
    WHERE table_schema = 'default'
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Show current catalog / schema

# COMMAND ----------

print("Current Catalog:", spark.catalog.currentCatalog())
print("Current Database:", spark.catalog.currentDatabase())
print("\nAll Catalogs:")
spark.sql("SHOW CATALOGS").show(truncate=False)
print("\nAll Databases:")
spark.sql("SHOW DATABASES").show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Workflows & Jobs
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **IPyWidgets (Text)** | `dbutils.widgets.text("param_name", "default_value", "label")` | Get value: `dbutils.widgets.get("param_name")` |
# MAGIC | **IPyWidgets (Dropdown)** | `dbutils.widgets.dropdown("param", "default", ["opt1","opt2"])` | List-based selection |
# MAGIC | **IPyWidgets (Multiselect)** | `dbutils.widgets.multiselect("param", "default", ["opt1","opt2"])` | Multi-select; returns comma-separated |
# MAGIC | **Remove Widget** | `dbutils.widgets.remove("param_name")` | Cleanup at notebook end |
# MAGIC | **Remove All Widgets** | `dbutils.widgets.removeAll()` | Starts fresh |
# MAGIC | **dbutils.notebook.run** | `dbutils.notebook.run("path/to/notebook", timeout_seconds=300, arguments={"key":"val"})` | Runs as a sub-job; returns any exit value |
# MAGIC | **dbutils.notebook.exit** | `dbutils.notebook.exit("my_result")` | Exits child notebook, returning value to caller |
# MAGIC | **%run** | `%run /path/to/notebook` | Inline execution; shares variable scope (no isolation); use sparingly |
# MAGIC | **Secrets — get scope** | `dbutils.secrets.get(scope="my_scope", key="my_key")` or `dbutils.secrets.getBytes(...)` | Scopes created via Databricks CLI or UI; API keys, passwords |
# MAGIC | **Secrets — list scopes** | `dbutils.secrets.listScopes()` | Lists all available secret scopes |
# MAGIC | **Secrets — list secrets** | `dbutils.secrets.list("my_scope")` | Lists secret keys (not values) in a scope |
# MAGIC | **File System (DBFS)** | `dbutils.fs.ls("dbfs:/path/")` `dbutils.fs.rm("path", recurse=True)` `dbutils.fs.cp`, `dbutils.fs.mv`, `dbutils.fs.mkdirs`, `dbutils.fs.head` | `dbutils.fs.help()` for command list |
# MAGIC | **dbutils.library** | `dbutils.library.installPyPI("pandas", version="2.0")` or `dbutils.library.restartPython()` | Install Python packages at notebook scope |
# MAGIC | **Task Values** | `dbutils.jobs.taskValues.set(key="my_key", value=my_df)` (setter) / `dbutils.jobs.taskValues.get(taskKey="prev_task", key="my_key")` (getter) | Pass data between tasks in a workflow; 48 MB limit |
# MAGIC | **System Information** | `spark.conf.get("spark.databricks.clusterUsageTags.clusterName")` or `dbutils.notebook.getContext()` | Identify cluster, notebook path, user |
# MAGIC | **Utility JSON** | `dbutils.notebook.getContext().toJson()` | Dump all execution context as JSON |
# MAGIC | **Workflow Parameters** | Define in Job UI: `{"my_param": "value"}` → retrieve via `dbutils.widgets.get("my_param")` | Params auto-mapped to widgets |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Workflows — Executable Examples

# COMMAND ----------

# Create and read a widget
dbutils.widgets.text("run_date", "2025-01-01", "Run Date")
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"])
run_date = dbutils.widgets.get("run_date")
env = dbutils.widgets.get("env")
print(f"Running for date: {run_date} | Environment: {env}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### dbutils.fs examples + secret retrieval

# COMMAND ----------

# List workspace or DBFS files
display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Secret retrieval (scope must pre-exist)
# MAGIC # password = dbutils.secrets.get(scope="my_scope", key="db_password")
# MAGIC # spark.read.jdbc(url=url, table="table", properties={"password": password})
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. File I/O
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **Read CSV** | `spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/path")` | Default delimiter=`,`; set `sep` for tab, pipe |
# MAGIC | **Write CSV** | `df.write.format("csv").option("header", "true").mode("overwrite").save("/path")` | Use `compression`: gzip, bzip2, snappy, lz4 |
# MAGIC | **Read JSON** | `spark.read.format("json").load("/path")` or `spark.read.json("/path")` | Also reads newline-delimited JSON (NDJSON) |
# MAGIC | **Write JSON** | `df.write.format("json").mode("overwrite").save("/path")` | Writes NDJSON format |
# MAGIC | **Read Parquet** | `spark.read.format("parquet").load("/path")` or `spark.read.parquet("/path")` | Columnar, compressed; best for large structured data |
# MAGIC | **Write Parquet** | `df.write.format("parquet").mode("overwrite").save("/path")` | Use `.partitionBy("date_col")` for layout |
# MAGIC | **Read Delta** | `spark.read.format("delta").load("/path")` or `spark.read.table("name")` or `spark.table("name")` | Preferred for all tabular data; supports time travel |
# MAGIC | **Write Delta** | `df.write.format("delta").mode("overwrite").saveAsTable("name")` | Supports `partitionedBy`, `replaceWhere`, `overwriteSchema` |
# MAGIC | **Read JDBC** | `spark.read.format("jdbc").option("url", "jdbc:...").option("dbtable", "table").option("user","u").option("password","p").load()` | Use `numPartitions` + `partitionColumn` + `lowerBound`/`upperBound` for parallelism |
# MAGIC | **Schema Inference Options** | `spark.read.option("samplingRatio", 0.1).option("inferSchema", "true").csv(...)` | `samplingRatio` controls how much data is scanned for type inference |
# MAGIC | **Schema Definition** | `schema = "id LONG, name STRING, amount DOUBLE"; spark.read.schema(schema).csv("/path")` | Always prefer explicit schema over inference for production |
# MAGIC | **String to Date (CSV)** | `.option("dateFormat", "yyyy-MM-dd").option("timestampFormat", "yyyy-MM-dd HH:mm:ss")` | Controls date/timestamp parsing during read |
# MAGIC | **Copy INTO (Delta)** | `COPY INTO my_table FROM '/source/' FILEFORMAT = PARQUET [PATTERN = '*.parquet'] [FORMAT_OPTIONS ('mergeSchema' = 'true')] [COPY_OPTIONS ('force' = 'true')]` | Incremental file ingestion; safer than Spark write for cloud storage |
# MAGIC | **COPY INTO (Validation)** | `COPY INTO my_table FROM '/source/' FILEFORMAT = PARQUET VALIDATE ALL` | Test run — validates schema match without loading data |
# MAGIC | **Overwrite Schema** | `df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("t")` | Replaces Delta table schema completely |
# MAGIC | **Replace Where** | `df.write.option("replaceWhere", "event_date = '2025-06-01'").mode("overwrite").saveAsTable("t")` | Overwrites only rows matching predicate |
# MAGIC | **Multi-line JSON / CSV** | `.option("multiline", "true")` | For JSON arrays spanning multiple lines |
# MAGIC | **Binary file** | `spark.read.format("binaryFile").load("/path")` | Reads files as raw bytes; for images, documents |
# MAGIC | **UTF-8** | `.option("encoding", "UTF-8").option("charset", "UTF-8")` | Encoding options for text file reads |

# COMMAND ----------

# MAGIC %md
# MAGIC ### File I/O — Executable Examples

# COMMAND ----------

import pyspark.sql.functions as F

# Write Parquet to DBFS, then read it back
output_path = "dbfs:/tmp/cheatsheet_demo_parquet"
df = spark.table("main.default.cheatsheet_demo")

df.write.format("parquet").mode("overwrite").option("compression", "snappy").save(output_path)

read_back = spark.read.format("parquet").load(output_path)
print(f"Parquet files count: {read_back.count()} rows")
print(f"Schema:")
read_back.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read CSV with explicit schema (production pattern)

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("id", LongType(), True),
# MAGIC     StructField("name", StringType(), True),
# MAGIC     StructField("amount", DoubleType(), True),
# MAGIC ])
# MAGIC
# MAGIC df = (spark.read
# MAGIC     .format("csv")
# MAGIC     .option("header", "true")
# MAGIC     .option("dateFormat", "yyyy-MM-dd")
# MAGIC     .schema(schema)
# MAGIC     .load("/path/to/data.csv"))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Troubleshooting Commands
# MAGIC
# MAGIC | Command / Task | Syntax | Notes |
# MAGIC |---|---|------|
# MAGIC | **DESCRIBE DETAIL** | `DESCRIBE DETAIL my_table` | One-stop for table metadata: format, location, numFiles, sizeInBytes, partitionColumns, clusteringColumns, tableFeatures |
# MAGIC | **DESCRIBE HISTORY** | `DESCRIBE HISTORY my_table` | Audit trail: version, timestamp, operation, userName, operationMetrics (numOutputRows, numOutputBytes, etc.) |
# MAGIC | **SHOW TABLE PROPERTIES** | `SHOW TBLPROPERTIES my_table` | Shows delta config: retention duration, CDF status, column mapping mode |
# MAGIC | **SHOW CREATE TABLE** | `SHOW CREATE TABLE my_table` | Displays the CREATE TABLE DDL used (helpful for recreating tables) |
# MAGIC | **SHOW PARTITIONS** | `SHOW PARTITIONS my_table` | Lists all partition values present in the table |
# MAGIC | **EXPLAIN** | `EXPLAIN EXTENDED SELECT ...` | See logical/physical plan; check if filters push down, broadcast used, partitioning applied |
# MAGIC | **FSCK REPAIR TABLE** | `FSCK REPAIR TABLE my_table [DRY RUN]` | Recovers partitions that exist on storage but not in the Hive metastore (legacy; use Delta where possible) |
# MAGIC | **MSCK REPAIR TABLE** | `MSCK REPAIR TABLE my_table` | Alternative to FSCK REPAIR for Hive-style partition recovery |
# MAGIC | **DESCRIBE EXTENDED** | `DESCRIBE EXTENDED my_table` | Table-level metadata (provider, location, type, owner, properties) |
# MAGIC | **DESCRIBE TABLE EXTENDED** | `DESCRIBE TABLE EXTENDED my_table` | Includes partition metadata summary |
# MAGIC | **DESCRIBE DATABASE** | `DESCRIBE DATABASE EXTENDED my_schema` | Shows schema location, properties, owner |
# MAGIC | **ANALYZE TABLE (stats)** | `ANALYZE TABLE t COMPUTE STATISTICS [NOSCAN] [FOR COLUMNS c1, c2]` | `NOSCAN` only computes file-level stats; `FOR COLUMNS` adds column stats |
# MAGIC | **ANALYZE TABLE (partitions)** | `ANALYZE TABLE t PARTITION (dt='2025-01-01') COMPUTE STATISTICS` | Scoped to one partition |
# MAGIC | **SPARK UI** | Go to `https://<workspace-url>/#setting/clusters/<cluster_id>/sparkUi` | Check stages, tasks, shuffle read/write size, skew |
# MAGIC | **Ganglia UI** | Go to `https://<workspace-url>/#setting/clusters/<cluster_id>/ganglia` | Cluster-level CPU, memory, disk metrics |
# MAGIC | **Cluster Logs (Driver)** | `/databricks/driver/logs/` on the cluster driver node | stdout, stderr, log4j files; `dbutils.fs.ls("file:///databricks/driver/logs/")` |
# MAGIC | **Cluster Logs (Spark)** | `dbutils.fs.ls("file:///databricks/driver/spark/logs/")` | Spark executor and driver logs |
# MAGIC | **Delta Log** | Look in table `_delta_log/` directory (e.g., `dbutils.fs.ls("dbfs:/user/hive/warehouse/t/_delta_log/")`) | JSON transaction logs; 0000.json, 0001.json, ..., 0000.crc |
# MAGIC | **Garbage Collection** | `%sql SET spark.databricks.delta.retentionDurationCheck.enabled = false; VACUUM my_table RETAIN 0 HOURS` | Force immediate vacuum (DANGEROUS — time travel lost) |
# MAGIC | **Streaming Debug** | `SELECT * FROM your_checkpoint_location` or query `spark.streams.active` | Check `/state`, `/commits`, `/sources` in checkpoint dir |
# MAGIC | **PySpark Profiling** | Prefix cell with `%%python` → `import cProfile; cProfile.run('...')` or use SparkListeners | Debug Python execution time |
# MAGIC | **SQL Query History** | Query `system.information_schema.query_history` in DBSQL or use the SQL Query History UI | See past queries, durations, errors |
# MAGIC | **Table ACLs / UC Permissions** | `SHOW GRANTS ON TABLE my_table` or query `system.information_schema.table_privileges` | Debug access-denied errors |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Troubleshooting — Executable Examples

# COMMAND ----------

# DESCRIBE DETAIL — essential diagnostics for any Delta table
print("=" * 80)
print("TABLE DETAIL")
print("=" * 80)
spark.sql("DESCRIBE DETAIL main.default.cheatsheet_demo").show(truncate=False)

# COMMAND ----------

# DESCRIBE HISTORY — all operations on the table
print("=" * 80)
print("TABLE HISTORY")
print("=" * 80)
spark.sql("DESCRIBE HISTORY main.default.cheatsheet_demo").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(10, truncate=100)

# COMMAND ----------

# SHOW TBLPROPERTIES — check Delta configuration
print("=" * 80)
print("TABLE PROPERTIES")
print("=" * 80)
spark.sql("SHOW TBLPROPERTIES main.default.cheatsheet_demo").show(truncate=80)

# COMMAND ----------

# EXPLAIN — check query plan for a complex operation
print("=" * 80)
print("EXPLAIN EXTENDED")
print("=" * 80)
spark.sql("""
    EXPLAIN EXTENDED
    SELECT category_id, SUM(amount) as total
    FROM main.default.cheatsheet_demo
    WHERE event_date > '2025-06-01'
    GROUP BY category_id
""").show(truncate=120)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check Delta _delta_log directory

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Inspect Delta transaction log (JSON files)
# MAGIC # table_path = spark.sql("DESCRIBE DETAIL main.default.cheatsheet_demo").collect()[0]["location"]
# MAGIC # dbutils.fs.ls(table_path + "/_delta_log/")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### View driver logs

# COMMAND ----------

# View driver log files available on the cluster
display(dbutils.fs.ls("file:///databricks/driver/logs/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Quick Reference Cards
# MAGIC
# MAGIC ### Common `dbutils` Commands
# MAGIC
# MAGIC | Command | Description |
# MAGIC |----------|-------------|
# MAGIC | `dbutils.fs.help()` | List all file-system commands |
# MAGIC | `dbutils.fs.ls("dbfs:/")` | List DBFS root |
# MAGIC | `dbutils.fs.head("path", maxBytes=65536)` | Read first bytes of a file |
# MAGIC | `dbutils.secrets.listScopes()` | List secret scopes |
# MAGIC | `dbutils.notebook.getContext()` | Get notebook/user/cluster context |
# MAGIC | `dbutils.jobs.taskValues.set(key, value)` | Set task value for downstream tasks |
# MAGIC | `dbutils.jobs.taskValues.get(taskKey, key)` | Get task value from upstream task |
# MAGIC | `dbutils.widgets.removeAll()` | Clear all notebook widgets |
# MAGIC | `dbutils.library.restartPython()` | Restart Python interpreter after library install |
# MAGIC
# MAGIC ### Common `spark` Methods
# MAGIC
# MAGIC | Method | Description |
# MAGIC |--------|-------------|
# MAGIC | `spark.sql("query")` | Execute SQL and return DataFrame |
# MAGIC | `spark.table("name")` | Read table as DataFrame |
# MAGIC | `spark.read.format("x").load("path")` | Read files |
# MAGIC | `spark.range(n)` | Generate sequence DataFrame |
# MAGIC | `spark.conf.get("key")` / `.set("key",v)` | Get/set Spark config |
# MAGIC | `spark.catalog.currentCatalog()` | Active Unity Catalog catalog |
# MAGIC | `spark.catalog.currentDatabase()` | Active schema/database |
# MAGIC | `spark.catalog.listTables()` | List all tables in current schema |
# MAGIC | `spark.createDataFrame(data, schema)` | Create DataFrame from Python collection |
# MAGIC | `spark.streams.active` | List active streaming queries |
# MAGIC
# MAGIC ### Delta Table Property Reference
# MAGIC
# MAGIC | Property | Default | Description |
# MAGIC |----------|---------|-------------|
# MAGIC | `delta.logRetentionDuration` | 30 days | How long history is kept for time travel |
# MAGIC | `delta.deletedFileRetentionDuration` | 7 days | How long deleted files remain (VACUUM threshold) |
# MAGIC | `delta.enableChangeDataFeed` | false | Enable CDF for incremental processing |
# MAGIC | `delta.autoOptimize.optimizeWrite` | false | Auto-compact during writes |
# MAGIC | `delta.autoOptimize.autoCompact` | false | Auto-compact after writes |
# MAGIC | `delta.columnMapping.mode` | none | `name` or `id` for column mapping (renaming support) |
# MAGIC | `delta.minReaderVersion` | 1 | Minimum reader protocol version |
# MAGIC | `delta.minWriterVersion` | 2 | Minimum writer protocol version |
# MAGIC | `delta.appendOnly` | false | Prevent UPDATE/DELETE operations |
# MAGIC | `delta.isolationLevel` | WriteSerializable | `Serializable` for strict serializability |
# MAGIC
# MAGIC ### Write Mode Reference
# MAGIC
# MAGIC | Mode | Behavior |
# MAGIC |------|----------|
# MAGIC | `error` / `errorifexists` (default) | Throw exception if data already exists |
# MAGIC | `append` | Add data without removing existing data |
# MAGIC | `overwrite` | Fully replace existing data |
# MAGIC | `ignore` | Silently skip if data already exists |
# MAGIC
# MAGIC ### Join Type Reference
# MAGIC
# MAGIC | Type | Description |
# MAGIC |------|-------------|
# MAGIC | `inner` | Rows matching in both DataFrames |
# MAGIC | `left` / `leftouter` | All rows from left, matching from right |
# MAGIC | `right` / `rightouter` | All rows from right, matching from left |
# MAGIC | `full` / `outer` / `fullouter` | All rows from both sides |
# MAGIC | `left_semi` | Left rows where key exists in right (filter, no right cols) |
# MAGIC | `left_anti` | Left rows where key does NOT exist in right |
# MAGIC | `cross` | Cartesian product (use cautiously) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *Last updated: 2025-06. For the latest always check [docs.databricks.com](https://docs.databricks.com).*

# COMMAND ----------

# Cleanup: drop the demo table
spark.sql("DROP TABLE IF EXISTS main.default.cheatsheet_demo")
spark.sql("DROP TABLE IF EXISTS main.default.cheatsheet_stream_output")
print("Cleanup complete.")
