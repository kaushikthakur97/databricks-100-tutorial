# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Interview Question Bank
# MAGIC
# MAGIC **50 curated Q&A** organized by category for Data Engineer roles.  
# MAGIC Covers both **Associate-level** (A) and **Professional-level** (P) questions.  
# MAGIC All code examples are serverless compatible.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Table of Contents
# MAGIC 1. Delta Lake (10 Q&A)
# MAGIC 2. Spark Execution (8 Q&A)
# MAGIC 3. SQL & DataFrames (7 Q&A)
# MAGIC 4. Data Ingestion (6 Q&A)
# MAGIC 5. Streaming (6 Q&A)
# MAGIC 6. Performance (6 Q&A)
# MAGIC 7. Governance (5 Q&A)
# MAGIC 8. Production (2 Q&A)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Delta Lake &mdash; 10 Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1: What are ACID transactions in Delta Lake and why are they important?
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You are migrating from Parquet to Delta and your team is worried about concurrent writes corrupting data during nightly ETL.  
# MAGIC **Answer:** ACID (Atomicity, Consistency, Isolation, Durability) transactions in Delta Lake guarantee that every write operation is either fully committed or fully rolled back, preventing partial writes. Delta achieves this through a transaction log (`_delta_log/`) implemented as an ordered sequence of JSON commit files, with optimistic concurrency control to handle conflicts between concurrent writers. This enables safe multi-user workloads on data lakes, with serializable isolation when writing to the same table and snapshot isolation for concurrent reads.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Concurrent writes to the same table are safe
# MAGIC -- Delta handles conflict resolution automatically
# MAGIC INSERT INTO sales_delta
# MAGIC SELECT * FROM staging_sales
# MAGIC WHERE sale_date = current_date();
# MAGIC
# MAGIC -- Check transaction history
# MAGIC DESCRIBE HISTORY sales_delta;
# MAGIC ```
# MAGIC **Key Point:** Delta Lake brings database-level ACID guarantees to data lakes by using a transaction log with optimistic concurrency control, enabling safe concurrent reads and writes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2: How does Delta Lake implement Time Travel and what are its practical use cases?
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** A downstream report shows wrong numbers. You need to query what the table looked like yesterday before a buggy pipeline ran.  
# MAGIC **Answer:** Delta Lake natively supports Time Travel by retaining old versions of data files (until they are VACUUMed) and storing the transaction log history. You can query a table as of a specific timestamp or version number using `TIMESTAMP AS OF` or `VERSION AS OF` syntax. Common use cases include auditing historical data states, rolling back bad writes with `RESTORE`, reproducing ML experiments with exact training data snapshots, and debugging data quality issues by comparing across versions.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Query table as of 24 hours ago
# MAGIC SELECT * FROM sales_delta
# MAGIC TIMESTAMP AS OF current_timestamp() - INTERVAL '1' DAY;
# MAGIC
# MAGIC -- Query specific version
# MAGIC SELECT * FROM sales_delta VERSION AS OF 42;
# MAGIC
# MAGIC -- Restore table to a previous version
# MAGIC RESTORE TABLE sales_delta TO VERSION AS OF 42;
# MAGIC ```
# MAGIC **Key Point:** Time Travel lets you query and restore historical table states using version numbers or timestamps, with retention governed by the vacuum retention period (default 7 days).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3: Explain Schema Evolution in Delta Lake. How does it handle schema mismatches?
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** The upstream source adds a new column `customer_segment` to the JSON feed. Your pipeline must not break and should automatically include it.  
# MAGIC **Answer:** Schema evolution allows Delta Lake to adapt to changing schemas at write time without manual intervention. When `mergeSchema` is enabled (or `autoMerge` for streaming), new columns in the incoming DataFrame are automatically appended to the target table schema, while missing columns resolve to `null`. Schema overwriting (explicit schema change) is controlled by `overwriteSchema`. Delta enforces an intersection of schemas by default — if a column type changes to an incompatible type, the write fails to prevent data corruption.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Enable schema evolution during append
# MAGIC df.write \
# MAGIC     .mode("append") \
# MAGIC     .option("mergeSchema", "true") \
# MAGIC     .table("sales_delta")
# MAGIC
# MAGIC # Overwrite with a completely new schema
# MAGIC df.write \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .table("sales_delta")
# MAGIC ```
# MAGIC **Key Point:** `mergeSchema` appends new columns; `overwriteSchema` replaces the entire schema. Incompatible type changes are rejected to protect data integrity.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4: Compare Liquid Clustering vs Z-ORDER partitioning. When would you choose one over the other?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** Your dimension table is growing to billions of rows and query performance is degrading. You need to pick a clustering strategy.  
# MAGIC **Answer:** Liquid Clustering is an incremental, self-tuning clustering method that replaces both traditional Hive-style partitioning and Z-ORDER. Unlike Z-ORDER which optimizes for a fixed set of columns sorted linearly, Liquid Clustering uses a Hilbert curve to locate data in multi-dimensional space, adapts automatically as data grows, and allows concurrent writes without reclustering interference. Choose Liquid Clustering as the modern default (especially for tables with frequent writes and high cardinality columns). Z-ORDER is still useful for legacy compatibility or when you need manual control over the exact sort order.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Liquid Clustering (recommended for new tables)
# MAGIC CREATE TABLE sales_delta
# MAGIC CLUSTER BY (customer_id, sale_date);
# MAGIC
# MAGIC -- Convert existing table from Z-ORDER to Liquid Clustering
# MAGIC ALTER TABLE sales_delta
# MAGIC CLUSTER BY (customer_id, sale_date);
# MAGIC
# MAGIC -- Legacy: Z-ORDER optimization
# MAGIC OPTIMIZE sales_delta
# MAGIC ZORDER BY (customer_id);
# MAGIC ```
# MAGIC **Key Point:** Liquid Clustering is the modern successor to Z-ORDER — it uses Hilbert curves for multi-dimensional clustering, adapts automatically, and handles concurrent writes natively without blocking OPTIMIZE.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5: What is VACUUM in Delta Lake? What are the retention period implications?
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** Your storage costs are rising. You notice old Parquet files from deleted records are still sitting in cloud storage.  
# MAGIC **Answer:** VACUUM physically deletes data files that are no longer referenced by the Delta transaction log (typically files older than the retention period, default 7 days). This reclaims storage but also removes the ability to Time Travel beyond the retention window. The default retention of 168 hours exists to ensure there is always at least 7 days of rollback capability and that no active reader relies on stale files. You can lower the retention to as little as 0 hours (zero retention) in test environments, but in production you must balance storage costs against recovery needs.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Dry run to see what files would be deleted
# MAGIC VACUUM sales_delta RETAIN 168 HOURS DRY RUN;
# MAGIC
# MAGIC -- Purge files older than 7 days
# MAGIC VACUUM sales_delta RETAIN 168 HOURS;
# MAGIC
# MAGIC -- Check current retention config
# MAGIC DESCRIBE DETAIL sales_delta;
# MAGIC ```
# MAGIC **Key Point:** VACUUM permanently removes old data files. Default 7-day retention ensures rollback and concurrent read safety — never VACUUM with zero retention in production without understanding the consequences.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6: What is Change Data Feed (CDF) and how does it enable CDC patterns?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You need to propagate row-level changes from your Delta Silver table to downstream systems (Redis cache, search index) without full table reloads.  
# MAGIC **Answer:** Change Data Feed (CDF) records every row-level change (insert, update, delete) to a Delta table into a separate change log, accessible via `table_changes()` or the `readChangeData` option. Each change entry includes the operation type (`insert`, `update_preimage`, `update_postimage`, `delete`) and version/timestamp metadata. This enables efficient Change Data Capture patterns: downstream consumers only process net-new changes since their last checkpoint instead of full table scans. CDF is opt-in — set `delta.enableChangeDataFeed = true` at the table or cluster level.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Enable CDF on a table
# MAGIC ALTER TABLE sales_delta
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC -- Query all changes since version 10
# MAGIC SELECT * FROM table_changes('sales_delta', 10);
# MAGIC
# MAGIC -- Query changes within a time range
# MAGIC SELECT * FROM table_changes(
# MAGIC     'sales_delta',
# MAGIC     '2026-05-10 00:00:00',
# MAGIC     '2026-05-11 00:00:00'
# MAGIC );
# MAGIC ```
# MAGIC **Key Point:** CDF provides a row-level change log (`insert`/`update_preimage`/`update_postimage`/`delete`) for Delta tables, enabling efficient CDC to downstream systems without full table refreshes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7: What are Deletion Vectors and how do they improve delete/update performance?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You need to delete 10 million rows from a 1-billion-row table for GDPR compliance. The operation is taking hours.  
# MAGIC **Answer:** Deletion Vectors are a performance optimization that marks rows as "soft-deleted" in the Delta log without physically rewriting the underlying Parquet files. Instead of a full file rewrite for every `DELETE` or `UPDATE` (copy-on-write), Delta stores a compact bitmap mapping file paths to row indices that should be excluded during reads. This dramatically reduces write amplification for targeted deletes and updates while ensuring committed readers see a consistent view. The cost is a slight read overhead to filter out deleted rows, which is offset by periodic OPTIMIZE operations.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Enable Deletion Vectors (enabled by default in newer runtimes)
# MAGIC ALTER TABLE sales_delta
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.enableDeletionVectors' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Efficient GDPR delete — targets 10M rows, no full rewrite
# MAGIC DELETE FROM sales_delta
# MAGIC WHERE customer_id = 'GDPR-subject-12345';
# MAGIC ```
# MAGIC **Key Point:** Deletion Vectors avoid full file rewrites during DELETEs/UPDATES by marking rows as soft-deleted via compact bitmaps, reducing write amplification dramatically — at the cost of minor read-time filtering.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8: Explain the OPTIMIZE command. When should you run it?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** After months of small incremental writes, your Delta table has 50,000 tiny files and queries are slow due to excessive file listing.  
# MAGIC **Answer:** OPTIMIZE (formerly called `BINPACKING`) compacts many small Parquet files into fewer, larger files (default target ~1 GB each) to reduce file listing overhead and improve read parallelism. It also optionally applies Z-ORDER clustering to colocate related data. Run OPTIMIZE after heavy small-batch writes (streaming micro-batches, many concurrent MERGEs), or when you observe high "number of files read" in query plans. Schedule it during off-peak hours since it rewrites data. Liquid Clustering uses a different approach — `OPTIMIZE FULL` with clustering columns instead of Z-ORDER.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Basic compaction — merge small files into ~1 GB files
# MAGIC OPTIMIZE sales_delta;
# MAGIC
# MAGIC -- Compaction with Z-ORDER on high-cardinality columns (legacy)
# MAGIC OPTIMIZE sales_delta
# MAGIC ZORDER BY (customer_id, sale_date);
# MAGIC
# MAGIC -- For Liquid Clustering tables
# MAGIC OPTIMIZE sales_delta FULL;
# MAGIC ```
# MAGIC **Key Point:** OPTIMIZE reduces the "small files problem" by compacting into larger files (~1 GB target) and optionally co-locating data via Z-ORDER or Liquid Clustering — run it periodically after heavy write workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q9: Compare Shallow Clone vs Deep Clone in Delta Lake.
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You need to create a development copy of a production 10 TB table for testing. A full copy takes too long and costs too much.  
# MAGIC **Answer:** A Shallow Clone (also called zero-copy clone) creates a new Delta table that references the same underlying data files as the source — it only copies the metadata/transaction log, creating a separate table with its own version history starting from the clone point. A Deep Clone physically copies all data files to a new location, making it fully independent. Shallow Clones are near-instantaneous regardless of table size but share storage with the source (VACUUM on source after clone can orphan files). Deep Clones are expensive but fully isolated — ideal for environment separation or cross-region copies.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Shallow Clone: instant, shares data files, own version history
# MAGIC CREATE OR REPLACE TABLE sales_dev
# MAGIC SHALLOW CLONE sales_prod;
# MAGIC
# MAGIC -- Deep Clone: full physical copy, independent
# MAGIC CREATE OR REPLACE TABLE sales_dev
# MAGIC DEEP CLONE sales_prod
# MAGIC LOCATION 'abfss://dev@storage.dfs.core.windows.net/sales_dev';
# MAGIC ```
# MAGIC **Key Point:** Shallow Clone = metadata-only copy (instant, shares files, cost-efficient); Deep Clone = full physical copy (expensive, fully independent). Use shallow for dev/test, deep for true data isolation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q10: Compare managed tables vs external tables in Databricks.
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You're designing a data architecture and need to decide whether to use managed or external tables for your Bronze layer.  
# MAGIC **Answer:** Managed tables (default in Unity Catalog) have their lifecycle and data files fully managed by Databricks — dropping the table deletes both metadata AND underlying data files. External tables reference data at a user-specified `LOCATION` — dropping the table only removes the metadata, leaving files intact. Managed tables benefit from full Unity Catalog governance (automatic lineage, discovery, data isolation per catalog/schema), while external tables are useful when you need to read/write data that exists outside Databricks' managed storage (e.g., existing cloud storage, cross-system sharing). For new workloads, managed tables are the recommended default.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Managed table (default) — data lifecycle managed by Databricks
# MAGIC CREATE TABLE sales_managed (
# MAGIC     id BIGINT,
# MAGIC     amount DECIMAL(10,2)
# MAGIC );
# MAGIC
# MAGIC -- External table — data at custom location, not deleted on DROP
# MAGIC CREATE TABLE sales_external (
# MAGIC     id BIGINT,
# MAGIC     amount DECIMAL(10,2)
# MAGIC )
# MAGIC LOCATION 'abfss://external@storage.dfs.core.windows.net/sales/';
# MAGIC
# MAGIC -- DROP on managed: metadata + data deleted
# MAGIC -- DROP on external: only metadata deleted, files remain
# MAGIC DROP TABLE sales_managed;
# MAGIC DROP TABLE sales_external;
# MAGIC ```
# MAGIC **Key Point:** Managed tables = Databricks controls the data lifecycle (DROP deletes everything); External tables = you control the location (DROP leaves files intact). Managed is the Unity Catalog default.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Spark Execution &mdash; 8 Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q11: What is lazy evaluation in Spark? How does it benefit query execution?
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You write 10 transformations chained together but notice no jobs appear in the Spark UI until you call `.show()`.  
# MAGIC **Answer:** Lazy evaluation means Spark does not execute transformations (map, filter, join, etc.) immediately — instead it builds a DAG (Directed Acyclic Graph) of operations and only triggers computation when an action (collect, show, write, count) is called. This allows the Catalyst optimizer to analyze the entire query plan holistically, combining and reordering operations for maximum efficiency (predicate pushdown, projection pruning, join reordering). Without lazy evaluation, each transformation would trigger a separate MapReduce-style pass, dramatically increasing I/O and shuffle overhead.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # None of these execute yet — just building the DAG
# MAGIC df = spark.read.table("sales_delta")
# MAGIC filtered = df.filter("amount > 100")
# MAGIC aggregated = filtered.groupBy("region").sum("amount")
# MAGIC
# MAGIC # This action triggers execution of the entire DAG
# MAGIC aggregated.show()
# MAGIC ```
# MAGIC **Key Point:** Transformations build a DAG lazily; actions trigger execution. This lets the Catalyst optimizer globally optimize the entire query plan before any data is processed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q12: Walk through the Catalyst optimizer stages from parsed query to executed RDD.
# MAGIC **Difficulty:** Hard &bull; **Level:** Professional  
# MAGIC **Real Scenario:** A complex SQL query with 12 joins runs in 45 minutes. You need to understand what the optimizer does to identify why it's slow.  
# MAGIC **Answer:** Catalyst processes queries through four stages: (1) **Analysis** — parses SQL/DataFrame into an unresolved logical plan, then uses the catalog to resolve table names, column names, and types into a resolved logical plan. (2) **Logical Optimization** — applies standard rule-based optimizations (predicate pushdown, constant folding, projection pruning, boolean simplification). (3) **Physical Planning** — converts the optimized logical plan into one or more physical plans, selecting the cheapest based on cost model (e.g., BroadcastHashJoin vs SortMergeJoin). (4) **Code Generation** — generates JVM bytecode via whole-stage code generation (Tungsten) to produce the final RDD execution plan. If AQE is enabled, Catalyst can re-optimize between stages at runtime.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Inspect the plan at each stage
# MAGIC df = spark.sql("SELECT region, SUM(amount) FROM sales WHERE year = 2026 GROUP BY region")
# MAGIC
# MAGIC # Resolved logical plan with table/column resolution
# MAGIC df._jdf.queryExecution().analyzed().toString()
# MAGIC
# MAGIC # Optimized logical plan (after rule-based optimizations)
# MAGIC df._jdf.queryExecution().optimizedPlan().toString()
# MAGIC
# MAGIC # Chosen physical plan
# MAGIC df._jdf.queryExecution().executedPlan().toString()
# MAGIC
# MAGIC # Or simply: df.explain(True)
# MAGIC ```
# MAGIC **Key Point:** Catalyst stages: Analysis (resolve) → Logical Optimization (rule-based) → Physical Planning (cost-based) → Code Generation (Tungsten). AQE adds runtime re-optimization.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q13: What optimizations does Adaptive Query Execution (AQE) bring?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** Your join query allocates 200 partitions for a shuffle but after filtering, only 3 partitions have data — the rest are empty tasks wasting scheduling overhead.  
# MAGIC **Answer:** AQE (enabled by default in Databricks) re-optimizes the query plan at runtime based on actual statistics gathered during execution. Three key optimizations: (1) **Dynamically coalesce shuffle partitions** — reduces the number of output partitions post-shuffle when the data is smaller than expected, eliminating empty/sparse partitions. (2) **Dynamically switch join strategies** — promotes a SortMergeJoin to BroadcastHashJoin when runtime statistics show the build side is small enough to broadcast. (3) **Dynamically optimize skew joins** — splits skewed shuffle partitions into smaller sub-partitions to balance load across tasks. (4) **Dynamically create partitions** — for queries with unknown partitioning on the first iteration.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- AQE configurations (enabled by default in Databricks 9.1+)
# MAGIC SET spark.sql.adaptive.enabled = true;
# MAGIC SET spark.sql.adaptive.coalescePartitions.enabled = true;
# MAGIC SET spark.sql.adaptive.skewJoin.enabled = true;
# MAGIC
# MAGIC -- Check if AQE was used in a query
# MAGIC -- Look for "AdaptiveSparkPlan" in the query plan
# MAGIC ```
# MAGIC ```python
# MAGIC df = spark.sql("SELECT /*+ COALESCE(4) */ * FROM large_table")
# MAGIC df.explain("formatted")
# MAGIC # Look for AdaptiveSparkPlan and CustomShuffleReader nodes
# MAGIC ```
# MAGIC **Key Point:** AQE re-optimizes at runtime: dynamically coalesces partitions, switches to broadcast joins when possible, and splits skewed partitions — all based on actual intermediate data statistics.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q14: Explain shuffle operations in Spark — what triggers them and how to minimize their cost?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** Your Spark job spends 80% of its time in shuffle operations according to the Spark UI, and you're over budget on compute costs.  
# MAGIC **Answer:** A shuffle is triggered when data must be redistributed across partitions, typically by operations that require data from different partitions to be co-located: `JOIN` (except broadcast), `GROUP BY`, `distinct()`, `repartition()`, and window functions without a matching `PARTITION BY`. The cost comes from disk I/O (shuffle data is written to local disk and read by downstream stages) and network transfer. Minimize shuffles by: using broadcast joins for small tables, filtering data as early as possible before joins/aggregations, bucketing tables on common join keys, using appropriate partition keys that match query patterns, and avoiding `distinct()`/`countDistinct()` in favor of `approx_count_distinct()` when exactness isn't required.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Expensive — shuffle due to GROUP BY across all data
# MAGIC df.groupBy("customer_id", "region").sum("amount")
# MAGIC
# MAGIC # Better — filter first to reduce shuffle volume
# MAGIC df.filter("year = 2026").groupBy("customer_id").sum("amount")
# MAGIC
# MAGIC # Best for small dim table — broadcast join avoids shuffle entirely
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC large_fact.join(broadcast(small_dim), "customer_id")
# MAGIC ```
# MAGIC **Key Point:** Shuffles are triggered by joins (except broadcast), groupBy, repartition, distinct, and window functions. Filter early, broadcast small tables, and align partition keys with query patterns to minimize cost.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q15: Compare different join strategies in Spark — when does the optimizer choose each?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You have a 100 GB fact table joining with a 50 MB dimension table. The join is taking 20 minutes and you suspect it's using the wrong join strategy.  
# MAGIC **Answer:** (1) **Broadcast Hash Join (BHJ)** — broadcasts the smaller table to all executors for a local hash join; chosen when one side is < `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). Extremely fast, no shuffle. (2) **Sort-Merge Join (SMJ)** — both tables are shuffled by join key, sorted, and merged; default for large-large joins. Requires a shuffle but works for any size. (3) **Shuffled Hash Join (SHJ)** — one table is shuffled and built into a hash map on each executor; rarely chosen but works when SMJ would overflow memory. (4) **Broadcast Nested Loop Join (BNLJ)** — used for non-equi joins (cartesian product fallback); very expensive. With AQE enabled, Spark can switch from SMJ to BHJ at runtime if statistics show a table is small enough.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Force broadcast join (override auto-detection)
# MAGIC from pyspark.sql.functions import broadcast
# MAGIC fact_df.join(broadcast(dim_df), "customer_id", "inner")
# MAGIC
# MAGIC # SQL hint syntax
# MAGIC spark.sql("""
# MAGIC     SELECT /*+ BROADCAST(dim) */ fact.*, dim.region
# MAGIC     FROM fact JOIN dim ON fact.customer_id = dim.customer_id
# MAGIC """)
# MAGIC
# MAGIC # Increase broadcast threshold (default 10 MB is conservative)
# MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")  # 50 MB
# MAGIC ```
# MAGIC **Key Point:** Spark picks Broadcast Hash Join (< 10 MB), Sort-Merge Join (large-large), or Shuffled Hash Join. AQE can switch strategies at runtime. Use `broadcast()` hints for tables just over the threshold.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q16: How do you determine the right number of partitions for a Spark job?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** Your Spark job has 200 partitions of 128 MB each on a cluster with 8 worker cores. Tasks are queued and taking forever.  
# MAGIC **Answer:** The ideal partition count balances parallelism with overhead. Aim for each partition to be 100–200 MB (compressed) after filtering. Too few partitions underutilize cores and risk OOM; too many partitions cause excessive task scheduling overhead and small-file reads. A good starting heuristic: target 2–4× the total number of executor cores, but verify partition sizes in the Spark UI. For writes, Delta recommends file sizes of ~1 GB, so you may want `coalesce()` before writing. Specific formulas: `spark.sql.files.maxPartitionBytes` (default 128 MB) controls how Parquet/Delta files are split into partitions during reads.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Check current partition count
# MAGIC print(f"Partitions: {df.rdd.getNumPartitions()}")
# MAGIC
# MAGIC # Adjust for narrow transformations (no shuffle)
# MAGIC df = df.coalesce(8)   # Reduce without shuffle
# MAGIC df = df.repartition(16)  # Full shuffle to increase evenly
# MAGIC
# MAGIC # Adjust partition size at read time
# MAGIC spark.conf.set("spark.sql.files.maxPartitionBytes", "268435456")  # 256 MB
# MAGIC
# MAGIC # Write with target file size using Delta
# MAGIC df.write.option("maxRecordsPerFile", "1000000").save()
# MAGIC
# MAGIC # For AQE, set initial partitions high; AQE will coalesce
# MAGIC spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1048576")
# MAGIC ```
# MAGIC **Key Point:** Target 100–200 MB per partition and 2–4× total cores. Use `coalesce()` for reduction without shuffle, `repartition()` for redistribution. With AQE, set a high initial count and let AQE coalesce down.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q17: What strategies exist to handle data skew in Spark?
# MAGIC **Difficulty:** Hard &bull; **Level:** Professional  
# MAGIC **Real Scenario:** A `GROUP BY customer_id` join takes hours because one customer (a mega-enterprise) has 100× more rows than average — one task processes 50 GB while others handle 50 MB.  
# MAGIC **Answer:** (1) **AQE Skew Join Optimization** — automatically detects skewed partitions during shuffle and splits them into smaller sub-partitions (enabled by default). (2) **Salting** — add a random suffix to the join key to spread the skewed key across multiple partitions, then aggregate results afterwards. (3) **Broadcast Join** — eliminates shuffle by broadcasting the smaller table (works only if one table fits in memory). (4) **Separate Skewed Data** — isolate the skewed key(s) in a separate Dataframe, join it using a strategy that handles it efficiently (broadcast or salt), and union with the non-skewed result. (5) **Increase Partition Count** — more partitions means each partition is smaller, reducing the impact (but doesn't fully solve extreme skew).  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col, concat, lit, rand, floor
# MAGIC
# MAGIC # Strategy: Salt the skewed join key
# MAGIC salt_count = 10
# MAGIC
# MAGIC # Salt the fact table — spread the skewed key across 10 partitions
# MAGIC fact_salted = fact_df.withColumn(
# MAGIC     "salted_key",
# MAGIC     concat(col("customer_id"), lit("_"), floor(rand() * salt_count))
# MAGIC )
# MAGIC
# MAGIC # Expand the dimension table — replicate each row 10 times
# MAGIC dim_salted = dim_df.crossJoin(
# MAGIC     spark.range(salt_count).withColumnRenamed("id", "salt")
# MAGIC ).withColumn("salted_key", concat(col("customer_id"), lit("_"), col("salt")))
# MAGIC
# MAGIC # Join on salted key, aggregate back to original key
# MAGIC result = fact_salted.join(dim_salted, "salted_key") \
# MAGIC     .groupBy("customer_id").sum("amount")
# MAGIC ```
# MAGIC **Key Point:** Handle skew with AQE auto-splitting (first line of defense), salting for extreme skew, broadcast joins for small tables, or isolating skewed keys for special handling.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q18: What causes memory spill in Spark and how do you address it?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** Your Spark UI shows tasks spilling to disk — "Shuffle spill (memory)" and "Shuffle spill (disk)" with hundreds of GB spilled, tanking performance.  
# MAGIC **Answer:** Memory spill occurs when executor memory allocation is insufficient to hold intermediate data during operations like shuffles, sorts, and aggregations. Spark must write data to local disk and re-read it, which is orders of magnitude slower. Root causes: (1) partition sizes too large for executor memory, (2) insufficient `spark.memory.fraction` (default 0.6 of heap) or off-heap memory, (3) complex aggregations with many distinct keys, (4) large broadcast tables exceeding driver/executor memory. Solutions: increase partition count to reduce per-partition size, increase executor memory (`spark.executor.memory`), tune the memory fraction, enable off-heap memory for shuffle, and consider Photon (Databricks' native engine) which has superior memory management and reduces spill dramatically.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Increase partition count to reduce per-partition size
# MAGIC df = df.repartition(200)
# MAGIC
# MAGIC # Increase executor memory and off-heap memory
# MAGIC spark.conf.set("spark.executor.memory", "16g")
# MAGIC spark.conf.set("spark.memory.offHeap.enabled", "true")
# MAGIC spark.conf.set("spark.memory.offHeap.size", "4g")
# MAGIC
# MAGIC # Tune memory fraction (fraction of heap for execution + storage)
# MAGIC spark.conf.set("spark.memory.fraction", "0.8")
# MAGIC
# MAGIC # Monitor spill in Spark UI Storage tab per executor
# MAGIC # Low spill = healthy. High spill = tune partitions or memory
# MAGIC ```
# MAGIC **Key Point:** Memory spill = data forced to disk during shuffle/sort/aggregation. Fix by increasing partition count, boosting executor memory, enabling off-heap, using Photon, or reducing per-partition data volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SQL &amp; DataFrames &mdash; 7 Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q19: Explain Spark Window functions with a practical example using ROW_NUMBER, RANK, and DENSE_RANK.
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You need to rank sales representatives by quarterly revenue within each region, handling ties correctly for bonus calculations.  
# MAGIC **Answer:** Window functions perform calculations across a set of rows that are related to the current row, defined by `PARTITION BY` (reset the window) and `ORDER BY` (determine row ordering). `ROW_NUMBER` assigns a unique sequential number to each row (no ties); `RANK` assigns the same rank to ties but leaves gaps in numbering (1, 2, 2, 4); `DENSE_RANK` also assigns same rank to ties but without gaps (1, 2, 2, 3). Other key window functions include `LAG`/`LEAD` (access previous/next row), `SUM`/`AVG` with frame specification, and `FIRST_VALUE`/`LAST_VALUE`. Window functions trigger a shuffle when `PARTITION BY` doesn't match the existing partitioning.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC WITH ranked AS (
# MAGIC     SELECT
# MAGIC         region,
# MAGIC         rep_name,
# MAGIC         SUM(revenue) AS total_revenue,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(revenue) DESC) AS rn,
# MAGIC         RANK() OVER (PARTITION BY region ORDER BY SUM(revenue) DESC) AS rnk,
# MAGIC         DENSE_RANK() OVER (PARTITION BY region ORDER BY SUM(revenue) DESC) AS dense_rnk,
# MAGIC         LAG(SUM(revenue)) OVER (PARTITION BY region ORDER BY SUM(revenue) DESC) AS prev_revenue,
# MAGIC         SUM(SUM(revenue)) OVER (
# MAGIC             PARTITION BY region
# MAGIC             ORDER BY SUM(revenue) DESC
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         ) AS running_total
# MAGIC     FROM quarterly_sales
# MAGIC     GROUP BY region, rep_name
# MAGIC )
# MAGIC SELECT * FROM ranked WHERE dense_rnk <= 3;
# MAGIC ```
# MAGIC **Key Point:** `ROW_NUMBER` = no ties; `RANK` = ties with gaps; `DENSE_RANK` = ties without gaps. Combine with `LAG`/`LEAD` and frame specifications for running totals, moving averages, and comparisons.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q20: How do you work with complex types (arrays, structs, maps) in Spark SQL?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** Your source data has a nested JSON column with arrays of order line items. You need to flatten it for analytics without using Python UDFs.  
# MAGIC **Answer:** Spark SQL provides rich native functions for complex types. Arrays: `explode()` to flatten, `array_contains()`, `array_distinct()`, `size()`, `slice()`, `sort_array()`, and higher-order `transform()`/`filter()`/`aggregate()`. Structs: access fields with dot notation (`struct_col.field`), create with `struct()`, and add fields with `named_struct()`. Maps: `map_keys()`, `map_values()`, `map_from_entries()`, and element access with bracket notation (`map_col['key']`). Use `explode_outer()` instead of `explode()` to preserve rows with null/empty arrays. When flattening nested JSON, you can often replace complex parsing UDFs with `from_json()` + schema definition + `explode()`.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- JSON string with nested arrays
# MAGIC WITH orders AS (
# MAGIC     SELECT from_json(order_json, 'order_id LONG,
# MAGIC         line_items ARRAY<STRUCT<sku STRING, qty INT, price DOUBLE>>,
# MAGIC         tags ARRAY<STRING>,
# MAGIC         metadata MAP<STRING,STRING>') AS data
# MAGIC     FROM raw_orders
# MAGIC )
# MAGIC SELECT
# MAGIC     data.order_id,
# MAGIC     -- Explode array of structs into rows
# MAGIC     explode(data.line_items) AS item,
# MAGIC     -- Filter array using higher-order function
# MAGIC     filter(data.tags, t -> t LIKE 'VIP%') AS vip_tags,
# MAGIC     -- Access map by key
# MAGIC     data.metadata['source_system'] AS source,
# MAGIC     -- Transform array elements
# MAGIC     transform(data.line_items, i -> i.price * i.qty) AS line_totals
# MAGIC FROM orders;
# MAGIC ```
# MAGIC **Key Point:** Use `explode()` for arrays, `filter()`/`transform()`/`aggregate()` as higher-order functions, struct field access with dot notation, and map key access with bracket notation — all without UDFs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q21: What are higher-order functions in Spark SQL and when would you use them?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You have an array column `scores` and need to compute `(score - mean) / stddev` for each element without exploding — the array has 1000 elements and exploding would explode row count.  
# MAGIC **Answer:** Higher-order functions accept a lambda expression and apply it to each element of an array without flattening. Key functions: `transform(array, elem -> expression)` — applies an expression to each element; `filter(array, elem -> condition)` — keeps only elements matching a condition; `exists(array, elem -> condition)` — returns true if any element matches; `aggregate(array, initial_value, (acc, elem) -> expr, finish -> expr)` — reduces an array to a single value using an accumulator. Use them when you need element-wise array operations that shouldn't explode row count or when you need to reduce arrays inline within a SELECT without UDFs.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC WITH data AS (
# MAGIC     SELECT array(82, 91, 76, 85, 93) AS scores
# MAGIC )
# MAGIC SELECT
# MAGIC     scores,
# MAGIC     -- Transform: add 5 bonus points to each score
# MAGIC     transform(scores, s -> s + 5) AS adjusted,
# MAGIC     -- Filter: only keep scores above 80
# MAGIC     filter(scores, s -> s > 80) AS passing,
# MAGIC     -- Exists: check if anyone scored above 90
# MAGIC     exists(scores, s -> s > 90) AS has_high_scorer,
# MAGIC     -- Aggregate: compute mean score
# MAGIC     aggregate(scores, 0D, (acc, s) -> acc + s, acc -> acc / size(scores)) AS mean_score,
# MAGIC     -- Z-score each element (transform with computed stats)
# MAGIC     transform(
# MAGIC         scores,
# MAGIC         s -> (s - aggregate(scores, 0D, (acc, x) -> acc + x, acc -> acc / size(scores)))
# MAGIC              / stddev_pop(scores)
# MAGIC     ) AS z_scores
# MAGIC FROM data;
# MAGIC ```
# MAGIC **Key Point:** Higher-order functions (`transform`, `filter`, `exists`, `aggregate`) operate element-wise on arrays without exploding rows — essential for inline array math, filtering, and reduction within a single row.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q22: Compare UDFs with built-in Spark functions in terms of performance.
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** A Python UDF that validates email formats processes 100 million rows in 2 hours. Your team suggests replacing it with Spark SQL.  
# MAGIC **Answer:** Built-in Spark functions are orders of magnitude faster than UDFs because they compile down to Catalyst expressions and benefit from whole-stage code generation (Tungsten) — Spark can process them without serializing data to Python/JVM boundary. Standard Python UDFs are the slowest: data is serialized per-row from JVM to Python, processed in the Python interpreter, and serialized back. Pandas UDFs (Series UDFs) mitigate this by using Apache Arrow for columnar batch transfer, achieving near-native speed for operations that can be vectorized. Always exhaust built-in functions and Spark SQL expressions before resorting to UDFs.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # SLOW — per-row serialization JVM ↔ Python
# MAGIC from pyspark.sql.functions import udf
# MAGIC @udf("string")
# MAGIC def clean_email(email):
# MAGIC     return email.strip().lower() if email else None
# MAGIC
# MAGIC # FAST — Pandas UDF with Arrow batch transfer
# MAGIC from pyspark.sql.functions import pandas_udf
# MAGIC import pandas as pd
# MAGIC @pandas_udf("string")
# MAGIC def clean_email_pandas(email: pd.Series) -> pd.Series:
# MAGIC     return email.str.strip().str.lower()
# MAGIC
# MAGIC # FASTEST — pure Spark SQL, no UDF overhead
# MAGIC from pyspark.sql.functions import lower, trim
# MAGIC df.withColumn("email_clean", lower(trim(col("email"))))
# MAGIC ```
# MAGIC **Key Point:** Built-in Spark functions > Pandas UDFs (Arrow) >> standard Python UDFs. Built-ins compile natively via Catalyst/Tungsten with zero serialization overhead. Always prefer built-ins first.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q23: How does MERGE INTO work in Delta Lake and what optimizations are available?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You have a 5 TB fact table and a staging table with 50,000 incremental updates (inserts, updates, deletes). The MERGE takes 45 minutes.  
# MAGIC **Answer:** `MERGE INTO` (UPSERT) applies insert, update, and delete operations from a source table/view into a target Delta table based on a matching condition. Under the hood it performs a left-anti join (for inserts) and an inner join (for updates/deletes), then rewrites matched Parquet files (or uses Deletion Vectors to avoid rewrites). Key optimizations: (1) Use `WHEN MATCHED AND <condition>` to prune unnecessary updates. (2) Add a `partitionFilter` hint on the source to restrict which partitions are scanned. (3) Use Deletion Vectors (`delta.enableDeletionVectors = true`) to avoid full file rewrites. (4) Ensure the target table uses Liquid Clustering on the merge key. (5) Consider low-shuffle merge when source and target share the same partitioning. (6) For massive tables, consider using CDF + incremental merge batches.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Optimized MERGE with partition filter
# MAGIC MERGE INTO sales_target t
# MAGIC USING (
# MAGIC     SELECT * FROM sales_staging
# MAGIC     WHERE processing_date = '2026-05-11'
# MAGIC ) s
# MAGIC ON t.order_id = s.order_id
# MAGIC WHEN MATCHED AND t.amount != s.amount THEN
# MAGIC     UPDATE SET
# MAGIC         t.amount = s.amount,
# MAGIC         t.updated_at = current_timestamp()
# MAGIC WHEN MATCHED AND s.deleted_flag = true THEN
# MAGIC     DELETE
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (order_id, customer_id, amount, created_at)
# MAGIC     VALUES (s.order_id, s.customer_id, s.amount, current_timestamp());
# MAGIC
# MAGIC -- Check merge metrics
# MAGIC -- num_inserted_rows, num_updated_rows, num_deleted_rows in output
# MAGIC ```
# MAGIC **Key Point:** MERGE INTO performs insert/update/delete in one operation. Optimize with partition filters, Deletion Vectors, Liquid Clustering, and low-shuffle merge to minimize full file rewrites.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q24: Explain PIVOT operations in Spark SQL. How do you unpivot?
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You receive sales data in row format (one row per region per month) and the BI team needs it in a cross-tab format with months as columns.  
# MAGIC **Answer:** `PIVOT` transforms unique values from a column into multiple columns, aggregating a specified metric. It's essentially `GROUP BY` followed by spread. Syntax: `PIVOT (agg_function(value_column) FOR pivot_column IN (value1, value2, ...))`. The inverse operation — converting wide columns back to rows — uses the `STACK` function (or `UNPIVOT` in some SQL dialects which Spark doesn't natively support outside of `stack()`). PIVOT works well when you know the pivot values ahead of time; for dynamic pivots in PySpark you'd group, pivot, and aggregate programmatically.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- PIVOT: rows → columns (months become column headers)
# MAGIC SELECT * FROM (
# MAGIC     SELECT region, month, revenue
# MAGIC     FROM monthly_sales
# MAGIC )
# MAGIC PIVOT (
# MAGIC     SUM(revenue)
# MAGIC     FOR month IN ('Jan' AS Jan, 'Feb' AS Feb, 'Mar' AS Mar)
# MAGIC )
# MAGIC ORDER BY region;
# MAGIC
# MAGIC -- UNPIVOT (stack): columns → rows
# MAGIC SELECT region, month, revenue FROM pivoted_sales
# MAGIC LATERAL VIEW STACK(3,
# MAGIC     'Jan', Jan, 'Feb', Feb, 'Mar', Mar
# MAGIC ) AS month, revenue
# MAGIC WHERE revenue IS NOT NULL;
# MAGIC ```
# MAGIC **Key Point:** `PIVOT` converts rows to columns by aggregating a metric over distinct pivot values. Unpivot with `STACK()` to reverse the transformation. Know pivot values in advance or build dynamically in PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q25: What is the VARIANT data type and how does it differ from using JSON strings?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** Your ingestion pipeline receives semi-structured events with varying schemas (different event types have different fields). You're currently storing them as raw JSON strings.  
# MAGIC **Answer:** The VARIANT type (GA in Databricks Runtime 15.4+) is a native semi-structured data type that stores JSON-like data in a binary-encoded, strongly-typed format. Unlike raw JSON strings, VARIANT preserves type information (distinguishing `123` as integer vs `"123"` as string) and enables efficient partial extraction of paths without parsing the entire JSON blob. Queries over VARIANT columns are significantly faster than `get_json_object()` or `from_json()` on raw strings because VARIANT uses a columnar binary encoding with built-in path indexing. Use VARIANT when you have polymorphic schemas, deeply nested optional fields, or when ingestion speed matters more than upfront schema enforcement.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Create table with VARIANT column
# MAGIC CREATE TABLE events (
# MAGIC     event_id BIGINT,
# MAGIC     event_time TIMESTAMP,
# MAGIC     payload VARIANT
# MAGIC );
# MAGIC
# MAGIC -- Insert semi-structured data — no upfront schema required
# MAGIC INSERT INTO events
# MAGIC SELECT
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     parse_json('{"user": "alice", "action": "click", "details": {"page": "/home", "duration_ms": 350}}');
# MAGIC
# MAGIC -- Query with path extraction — faster than raw JSON
# MAGIC SELECT
# MAGIC     payload:user::STRING AS user_name,
# MAGIC     payload:action::STRING AS action,
# MAGIC     payload:details.page::STRING AS page,
# MAGIC     payload:details.duration_ms::INT AS duration
# MAGIC FROM events;
# MAGIC
# MAGIC -- Check schema of variant data
# MAGIC SELECT schema_of_variant_agg(payload) FROM events;
# MAGIC ```
# MAGIC **Key Point:** VARIANT is a binary-encoded semi-structured type that's faster to query and more storage-efficient than raw JSON strings while preserving full type fidelity — ideal for polymorphic or evolving schemas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Ingestion &mdash; 6 Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q26: Compare COPY INTO vs Auto Loader for data ingestion. When should you use each?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You need to ingest 10,000 new CSV files per hour from cloud storage into Delta Bronze. Both COPY INTO and Auto Loader are options.  
# MAGIC **Answer:** `COPY INTO` is a SQL command that performs a one-shot or scheduled batch load of files from cloud storage into a Delta table, supporting idempotent loads via file tracking. Auto Loader is the streaming ingestion framework that continuously discovers and loads new files as they arrive, with built-in schema inference/evolution and cloud notification support (e.g., SQS, Event Grid). Use `COPY INTO` for periodic/batch loads with known file sets or when you want SQL-based control. Use Auto Loader for continuous streaming ingestion, when you need automatic schema evolution, or when event-driven ingestion (vs polling) is required. Auto Loader uses `fileNotificationMode` for event-driven vs `directoryListing` for polling-based file discovery.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- COPY INTO: batch-style incremental load, respects already-loaded files
# MAGIC COPY INTO bronze_sales
# MAGIC FROM 'abfss://landing@storage.dfs.core.windows.net/sales/'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true');
# MAGIC ```
# MAGIC ```python
# MAGIC # Auto Loader: streaming, continuous, with schema evolution
# MAGIC spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "parquet") \
# MAGIC     .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
# MAGIC     .option("cloudFiles.schemaLocation", "/checkpoints/sales_schema/") \
# MAGIC     .load("abfss://landing@storage.dfs.core.windows.net/sales/") \
# MAGIC     .writeStream \
# MAGIC     .option("checkpointLocation", "/checkpoints/sales_cp/") \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .table("bronze_sales")
# MAGIC ```
# MAGIC **Key Point:** `COPY INTO` = batch/idempotent SQL load; Auto Loader = continuous streaming ingestion with schema evolution. Use Auto Loader for streaming pipelines and event-driven patterns; use COPY INTO for periodic batch loads.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q27: Explain the Medallion architecture (Bronze, Silver, Gold) and its rationale.
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You're designing a greenfield data lakehouse. Your architect diagrams three layers but stakeholders ask "why not just one table?"  
# MAGIC **Answer:** The Medallion architecture organizes data into three progressive quality tiers. **Bronze** preserves raw ingested data as-is (immutable, append-only), retaining the original fidelity for reprocessing and auditing. **Silver** deduplicates, cleanses, standardizes, and joins data — it's the "single source of truth" where data quality issues are resolved and business keys are enforced. **Gold** contains business-level aggregates, features, and denormalized views optimized for consumption by BI tools, ML models, and applications. This layered approach provides data lineage traceability, enables reprocessing from raw data when logic changes, isolates quality transformations, and allows different refresh cadences and access patterns per layer.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Bronze: raw ingestion, append-only, preserve original fidelity
# MAGIC CREATE TABLE IF NOT EXISTS bronze.sales_raw (
# MAGIC     ingestion_time TIMESTAMP,
# MAGIC     source_file STRING,
# MAGIC     raw_data STRING
# MAGIC );
# MAGIC
# MAGIC -- Silver: cleansed, deduplicated, enriched with business keys
# MAGIC CREATE TABLE IF NOT EXISTS silver.sales_cleaned
# MAGIC CLUSTER BY (sale_date)
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC     order_id,
# MAGIC     CAST(sale_date AS DATE) AS sale_date,
# MAGIC     customer_id,
# MAGIC     CAST(regexp_replace(amount, '[^0-9.]', '') AS DECIMAL(10,2)) AS amount,
# MAGIC     CASE WHEN amount > 0 THEN 'valid' ELSE 'investigate' END AS quality_flag
# MAGIC FROM bronze.sales_raw;
# MAGIC
# MAGIC -- Gold: aggregated business metrics
# MAGIC CREATE TABLE IF NOT EXISTS gold.daily_revenue_by_region
# MAGIC AS
# MAGIC SELECT
# MAGIC     sale_date,
# MAGIC     region,
# MAGIC     SUM(amount) AS total_revenue,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM silver.sales_cleaned
# MAGIC WHERE quality_flag = 'valid'
# MAGIC GROUP BY sale_date, region;
# MAGIC ```
# MAGIC **Key Point:** Bronze = raw/immutable, Silver = cleaned/deduplicated, Gold = business aggregates. Enables lineage, reprocessing, isolated quality logic, and tiered refresh cadences.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q28: Compare SCD Type 1 vs SCD Type 2 implementations using Delta Lake.
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You're building a customer dimension table. Marketing needs current addresses for mailings (no history needed), but Finance needs to track address changes over time for compliance.  
# MAGIC **Answer:** **SCD Type 1** simply overwrites the existing row with new values — no history retained. Implement with a straightforward `MERGE INTO ... WHEN MATCHED THEN UPDATE SET`. **SCD Type 2** tracks full history by inserting a new row for each change and marking the previous row as inactive, using `effective_date`, `end_date`, and `is_current` columns. Implement with `MERGE INTO`: (1) `WHEN MATCHED AND source.hash != target.hash AND target.is_current = true THEN UPDATE SET end_date = current_date(), is_current = false` to close the old record, then (2) `WHEN NOT MATCHED BY SOURCE ...` logic is not needed — instead, a second `INSERT` in the same MERGE adds the new version with `is_current = true` and `effective_date = current_date()`. Delta Lake's ACID guarantees make SCD operations reliable.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- SCD Type 1: simple overwrite, no history
# MAGIC MERGE INTO dim_customer_type1 t
# MAGIC USING staging_customer s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     t.address = s.address,
# MAGIC     t.phone = s.phone,
# MAGIC     t.updated_at = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```
# MAGIC ```sql
# MAGIC -- SCD Type 2: full history with effective/end dates
# MAGIC MERGE INTO dim_customer_type2 t
# MAGIC USING staging_customer s
# MAGIC ON t.customer_id = s.customer_id AND t.is_current = true
# MAGIC WHEN MATCHED AND t.row_hash != s.row_hash THEN
# MAGIC     UPDATE SET
# MAGIC         t.end_date = current_date(),
# MAGIC         t.is_current = false
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         customer_id, name, address, email,
# MAGIC         effective_date, end_date, is_current
# MAGIC     ) VALUES (
# MAGIC         s.customer_id, s.name, s.address, s.email,
# MAGIC         current_date(), NULL, true
# MAGIC     );
# MAGIC
# MAGIC -- Query current view only
# MAGIC SELECT * FROM dim_customer_type2 WHERE is_current = true;
# MAGIC
# MAGIC -- Query as of a point in time
# MAGIC SELECT * FROM dim_customer_type2
# MAGIC WHERE effective_date <= '2026-01-01'
# MAGIC   AND (end_date IS NULL OR end_date > '2026-01-01');
# MAGIC ```
# MAGIC **Key Point:** SCD Type 1 = overwrite (no history); SCD Type 2 = insert new row + expire old row with `effective_date`/`end_date`/`is_current`. Delta Lake ACID ensures reliable SCD operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q29: How do you design an idempotent data pipeline?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** Your pipeline failed mid-way due to a cluster crash. On retry, it re-processed the same batch, creating duplicate rows. Stakeholders want guarantees this won't happen.  
# MAGIC **Answer:** An idempotent pipeline produces the same result regardless of how many times it runs with the same input. Design strategies: (1) Use Delta Lake's `MERGE INTO` with a unique business key instead of simple `INSERT` — if the key already exists, update or skip; (2) Store a watermark/bookmark of the last successfully processed offset in a metadata table (or use Structured Streaming checkpointing); (3) Use `COPY INTO` with its built-in file tracking (it skips already-loaded files); (4) Write with `txnVersion` or `appId` columns and filter out duplicate runs; (5) Use `OVERWRITE` with a predicate `WHERE date = process_date` for partition-level idempotency in batch pipelines; (6) Design upstream sources to support re-reading the same slice (e.g., by timestamp range rather than absolute offsets).  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Strategy 1: MERGE on business key ensures idempotency
# MAGIC process_date = "2026-05-11"
# MAGIC staging_df = spark.read.parquet(f"staging/sales/{process_date}")
# MAGIC
# MAGIC staging_df.createOrReplaceTempView("staging")
# MAGIC
# MAGIC spark.sql("""
# MAGIC     MERGE INTO silver.sales t
# MAGIC     USING staging s
# MAGIC     ON t.order_id = s.order_id  -- business key
# MAGIC     WHEN MATCHED THEN UPDATE SET *
# MAGIC     WHEN NOT MATCHED THEN INSERT *
# MAGIC """)
# MAGIC
# MAGIC # Strategy 2: Overwrite partition — safe to re-run
# MAGIC staging_df.write \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("replaceWhere", f"sale_date = '{process_date}'") \
# MAGIC     .table("silver.sales_partitioned")
# MAGIC ```
# MAGIC **Key Point:** Idempotent pipelines use MERGE with business keys, bookmark-based checkpoints, `replaceWhere` for partition overwrites, or `COPY INTO` file tracking — ensuring re-runs don't duplicate data.

# COMMAND ----------

# MAGIC ### Q30: What patterns exist for incremental data processing in Databricks?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You process 500 GB of raw data daily but only 5 GB is new. Full reloads take 4 hours and cost $200/day. You need an incremental approach.  
# MAGIC **Answer:** (1) **Streaming with Auto Loader + `trigger(availableNow=True)`** — auto-discovers new files and processes them incrementally with exactly-once guarantees (deprecated: single invocation). (2) **Delta Change Data Feed (CDF)** — read only changed rows since a version/timestamp: `table_changes('table', start_version)`. (3) **File metadata tracking** — store a `last_processed_file` watermark in a Delta table, then filter source files by creation time or filename pattern. (4) **Change tracking columns** — if the source has reliable `updated_at`/`modified_date` columns, filter by `modified_date > max_processed_date`. (5) **`COPY INTO`** — natively tracks loaded files in the Delta transaction log. (6) **Structured Streaming** — long-running stream with checkpoint; reprocess only offsets since last commit. Choose streaming patterns for continuous/low-latency use cases; batch patterns for scheduled jobs.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Pattern 1: Auto Loader — incremental + idempotent
# MAGIC spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "parquet") \
# MAGIC     .option("cloudFiles.schemaLocation", "/check/schema/") \
# MAGIC     .load("/landing/sales/") \
# MAGIC     .writeStream \
# MAGIC     .option("checkpointLocation", "/check/data/") \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .table("bronze.sales")
# MAGIC
# MAGIC # Pattern 2: Read only since last version via CDF (Silver → Gold)
# MAGIC last_version = spark.sql("SELECT MAX(version) FROM gold.daily_kpi").collect()[0][0]
# MAGIC changes = spark.sql(f"SELECT * FROM table_changes('silver.sales', {last_version})")
# MAGIC # Process changes incrementally...
# MAGIC
# MAGIC # Pattern 3: Filter by modified column timestamp
# MAGIC max_date = spark.sql("SELECT MAX(modified_date) FROM target").collect()[0][0]
# MAGIC incremental_df = spark.read.parquet("/source/") \
# MAGIC     .filter(f"modified_date > '{max_date}'")
# MAGIC ```
# MAGIC **Key Point:** Incremental patterns: `trigger(availableNow=True)` streaming, CDF `table_changes()`, file metadata tracking, `modified_date` watermarks, and `COPY INTO` file tracking — choose based on source capabilities and latency requirements.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q31: How does schema evolution work during data ingestion and what are the risks?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** The upstream team renamed a column from `customer_id` to `cust_id` in their Kafka topic. Your Auto Loader pipeline broke on the next run.  
# MAGIC **Answer:** Schema evolution during ingestion allows the target table schema to adapt to changes in incoming data. Auto Loader provides three modes: `addNewColumns` (default — appends new columns, fills missing with null), `rescue` (captures all data including mismatched columns into a `_rescued_data` column), `failOnNewColumns` (throws an error on schema mismatch), and `none` (schema validation without evolution). Risks: (1) Renamed columns are NOT handled by schema evolution — the old column gets nulls and the new column gets added as a separate field, silently fragmenting data. (2) Incompatible type changes (string → int) may fail or produce corrupted data. (3) Backfill implications — historical partitions won't have the new columns, requiring `REORG TABLE` or manual backfill. Mitigation: use schema contracts, `_rescued_data` for debugging, schema enforcement in CI/CD, and `schema_of_json()` to validate.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Auto Loader with rescued data column for debugging schema issues
# MAGIC spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .option("cloudFiles.schemaEvolutionMode", "rescue") \
# MAGIC     .option("rescuedDataColumn", "_rescued_data") \
# MAGIC     .option("cloudFiles.schemaLocation", "/check/schema/") \
# MAGIC     .load("/landing/events/") \
# MAGIC     .writeStream \
# MAGIC     .option("checkpointLocation", "/check/data/") \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .table("bronze.events")
# MAGIC
# MAGIC -- Inspect rescued data to find schema mismatches
# MAGIC SELECT _rescued_data FROM bronze.events WHERE _rescued_data IS NOT NULL;
# MAGIC
# MAGIC -- Discover the evolved schema after ingestion
# MAGIC SELECT * FROM bronze.events LIMIT 1;
# MAGIC ```
# MAGIC **Key Point:** Schema evolution adds new columns but cannot handle column renames. Use `_rescued_data` to catch mismatches, set clear schema contracts, and validate in CI/CD to prevent silent data corruption.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Streaming &mdash; 6 Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q32: Compare Structured Streaming vs DLT/Lakeflow streaming tables.
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You're choosing between writing raw Structured Streaming code with `foreachBatch` or using Delta Live Tables for a complex medallion pipeline with 15 downstream tables.  
# MAGIC **Answer:** **Structured Streaming** (`spark.readStream` + `writeStream`) gives you full programmatic control over sources, sinks, triggers, and output modes — ideal for custom logic, complex `foreachBatch` patterns, or integration with non-Delta sinks. **DLT (Delta Live Tables)** is a declarative framework where you define tables using `@dlt.table` decorators, and Databricks handles orchestration, error handling, state recovery, and visualization. DLT streaming tables (recently renamed Lakeflow Streaming) provide automatic incremental processing with `read_stream()` producing streaming tables, or `read_statestream()` for non-streaming incremental. Choose DLT when you want managed infrastructure, automatic retries, data quality expectations, and pipeline lineage. Choose raw Structured Streaming when you need fine-grained control, custom sinks, or you're not on DLT compute.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Raw Structured Streaming — full control
# MAGIC df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .load("/landing/events/")
# MAGIC
# MAGIC df.writeStream \
# MAGIC     .option("checkpointLocation", "/check/") \
# MAGIC     .trigger(processingTime="1 minute") \
# MAGIC     .foreachBatch(lambda batch_df, batch_id: process(batch_df)) \
# MAGIC     .start()
# MAGIC ```
# MAGIC ```python
# MAGIC # DLT declarative — managed orchestration
# MAGIC @dlt.table
# MAGIC def bronze_events():
# MAGIC     return spark.readStream \
# MAGIC         .format("cloudFiles") \
# MAGIC         .option("cloudFiles.format", "json") \
# MAGIC         .load("/landing/events/")
# MAGIC
# MAGIC @dlt.table
# MAGIC @dlt.expect("valid_order_id", "order_id IS NOT NULL")
# MAGIC def silver_orders():
# MAGIC     return dlt.read_stream("events") \
# MAGIC         .filter("event_type = 'order'")
# MAGIC ```
# MAGIC **Key Point:** Structured Streaming = full control, flexible sinks, manual orchestration. DLT/Lakeflow = declarative, managed compute, built-in quality checks, auto-orchestration. Choose based on control vs. managed needs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q33: What trigger types are available in Structured Streaming and what are their trade-offs?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You have a dashboard that needs fresh data within 10 seconds, but your nightly aggregations should run once and terminate.  
# MAGIC **Answer:** (1) **Unspecified / Default** — runs the next micro-batch as soon as the previous one finishes; lowest latency but may consume resources continuously. (2) **`processingTime`** — runs at fixed intervals (e.g., every 2 minutes); predictable resource usage, latency bounded by interval; useful for steady-state workloads. (3) **`availableNow`** — processes all available data and then stops; ideal for scheduled batch jobs that still benefit from streaming semantics (incremental, exactly-once). (4) **`once`** — processes a single micro-batch and stops; rarely used, mostly replaced by `availableNow`. (5) **`continuous`** — sub-millisecond latency using a long-running daemon mode; more expensive, less mature, fewer guarantees.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # availableNow — incremental batch (most common pattern)
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/check/") \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .table("bronze.events")
# MAGIC
# MAGIC # processingTime — steady interval streaming
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/check/") \
# MAGIC     .trigger(processingTime="30 seconds") \
# MAGIC     .table("bronze.events")
# MAGIC
# MAGIC # Low-latency (default, runs as fast as possible)
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/check/") \
# MAGIC     .start()  # no explicit trigger = continuous micro-batch
# MAGIC ```
# MAGIC **Key Point:** `availableNow` = incremental batch with termination; `processingTime` = fixed-interval streaming; default (unspecified) = lowest latency; `continuous` = experimental sub-ms. Use `availableNow` for scheduled jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q34: Explain watermarking in Structured Streaming and its role in state management.
# MAGIC **Difficulty:** Hard &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You have a streaming aggregation with a 1-hour window. Over weeks, the state store grows to 100 GB because late-arriving data prevents state cleanup.  
# MAGIC **Answer:** Watermarking defines a threshold for how late data can arrive relative to the event time. It allows the streaming engine to determine when a window/aggregation is "complete" and its state can be safely discarded. Without watermarking, state grows unboundedly because the engine must keep all windows open indefinitely in case late data arrives. Watermark = `max_observed_event_time - watermark_delay`. When the watermark passes the end of a window, that window's state is dropped. Key considerations: (1) Set the delay to the maximum expected lateness, (2) The watermark moves only when new data arrives (stagnant streams won't trigger cleanup), (3) Output modes: `append` mode + watermark enables window results to be emitted only when the watermark passes the window end — never updated later.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC from pyspark.sql.functions import window, col
# MAGIC
# MAGIC # Streaming aggregation with watermark
# MAGIC events_df = spark.readStream \
# MAGIC     .format("delta") \
# MAGIC     .table("bronze.events")
# MAGIC
# MAGIC aggregated = events_df \
# MAGIC     .withWatermark("event_timestamp", "10 minutes") \
# MAGIC     .groupBy(
# MAGIC         window("event_timestamp", "5 minutes"),
# MAGIC         col("event_type")
# MAGIC     ) \
# MAGIC     .count()
# MAGIC
# MAGIC # append mode: emits once, after watermark passes window end
# MAGIC aggregated.writeStream \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/check/") \
# MAGIC     .trigger(processingTime="1 minute") \
# MAGIC     .table("gold.event_counts_5min")
# MAGIC
# MAGIC -- Late data beyond watermark: dropped or written to side output
# MAGIC ```
# MAGIC **Key Point:** Watermarking bounds state growth by defining a lateness threshold. Without it, state grows unboundedly. With watermark + append mode, window results are final once emitted. Set delay to your maximum expected lateness.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q35: What output modes are available in Structured Streaming and when to use each?
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You built a streaming aggregation that appends each new count to a Delta table. On the 100th run, a product manager asks why the totals don't add up — each window appears multiple times with different counts.  
# MAGIC **Answer:** (1) **`append`** — only new rows are added to the output sink; existing results are never updated. Requires watermarking for aggregations, otherwise not allowed. Best for immutable event sinks or append-only tables where you want exactly one row per window. (2) **`update`** — rows that have changed since the last trigger are written; existing rows are updated in-place. Useful for continuously updating dashboards or mutable state tables. (3) **`complete`** — the entire result table is rewritten on every trigger; used only for unbounded aggregations (no watermark possible). Memory-intensive for large state; avoid for high-cardinality results. Choose `append` with watermarking for final-once results; `update` for changing results; `complete` only for small aggregations without cleanup.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # append mode: emit each window result exactly once (final)
# MAGIC df.withWatermark("ts", "10 min") \
# MAGIC     .groupBy(window("ts", "5 min"), "event") \
# MAGIC     .count() \
# MAGIC     .writeStream.outputMode("append").table("gold.counts")
# MAGIC
# MAGIC # update mode: emit whenever row changes (dashboard)
# MAGIC df.groupBy("event").count() \
# MAGIC     .writeStream.outputMode("update").table("silver.live_counts")
# MAGIC
# MAGIC # complete mode: rewrite entire result each trigger (small aggregates)
# MAGIC df.groupBy("event").count() \
# MAGIC     .writeStream.outputMode("complete") \
# MAGIC     .foreachBatch(lambda df, _: df.write.mode("overwrite").saveAsTable("gold.complete_counts"))
# MAGIC
# MAGIC -- Reference comparison
# MAGIC -- append: row appears once, watermark required for aggs
# MAGIC -- update: rows mutate with each trigger
# MAGIC -- complete: full table rewrite, no watermark
# MAGIC ```
# MAGIC **Key Point:** `append` = emit once (final after watermark), `update` = emit on change, `complete` = full rewrite. Choose append for final results, update for live dashboards, complete for small windowless aggregates.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q36: How does Structured Streaming achieve exactly-once semantics?
# MAGIC **Difficulty:** Hard &bull; **Level:** Professional  
# MAGIC **Real Scenario:** A production pipeline processes financial transactions. You must guarantee that every transaction is processed exactly once — no duplicates, no data loss.  
# MAGIC **Answer:** Exactly-once in Structured Streaming is achieved through a combination of (1) **Replayable sources** (Kafka offsets, Delta version, cloud files metadata) that allow re-reading the same data, (2) **Checkpointing** — the engine writes the current processing offset and state store snapshot to a fault-tolerant checkpoint location before each micro-batch commit, (3) **Idempotent sinks** — the sink must support transactional writes (Delta Lake `transactionalWrite`, `foreachBatch` with idempotent logic). For each micro-batch, the engine: writes the micro-batch output and checkpoint offset atomically. On failure/restart, it reads the last committed offset from the checkpoint and resumes processing from there. Delta Lake as a sink provides ACID transactional commits, so the write and offset commit are atomic via Delta's transaction log.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Exactly-once from Kafka to Delta
# MAGIC df = spark.readStream \
# MAGIC     .format("kafka") \
# MAGIC     .option("kafka.bootstrap.servers", "broker:9092") \
# MAGIC     .option("subscribe", "transactions") \
# MAGIC     .option("startingOffsets", "earliest") \  # replay if needed
# MAGIC     .load()
# MAGIC
# MAGIC parsed = df.selectExpr(
# MAGIC     "CAST(value AS STRING)",
# MAGIC     "offset"
# MAGIC )
# MAGIC
# MAGIC query = parsed.writeStream \
# MAGIC     .format("delta") \  # Delta = idempotent + transactional sink
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/check/transactions/") \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .table("bronze.transactions")
# MAGIC
# MAGIC # On failure, restart with same checkpoint → resumes from last committed offset
# MAGIC # No duplicate processing because offset commit is atomic with data write
# MAGIC ```
# MAGIC **Key Point:** Exactly-once = replayable source + checkpointing (offset + state) + idempotent transactional sink (Delta ACID). Offset commit and data write happen atomically in the same transaction.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q37: What are practical use cases for foreachBatch in Structured Streaming?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You need to write streaming aggregates to both a Delta table AND a Redis cache in the same micro-batch, or apply a complex MERGE that streaming sinks don't natively support.  
# MAGIC **Answer:** `foreachBatch` provides a callback on each micro-batch's output DataFrame, giving full programmatic access to the batch DataFrame for arbitrary writes. Use cases: (1) **Multi-sink writes** — write the same micro-batch to Delta, Kafka, and a cache in one atomic-ish operation. (2) **Complex MERGE/UPSERT** — streaming sinks only support append/complete output modes natively; `foreachBatch` allows you to execute `MERGE INTO` for upsert semantics. (3) **Applying non-Spark transformations** — send data to REST APIs, update vector databases, or trigger external workflows after each micro-batch. (4) **Dynamic partitioning** — write each batch to a different table/path based on content (e.g., tenant-based routing). (5) **Applying watermark-based deduplication** that isn't supported natively in all cases. Important: exactly-once guarantees rely on idempotent logic within the callback.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC def write_microbatch(df, batch_id):
# MAGIC     # Sink 1: Append raw events
# MAGIC     df.write.mode("append").saveAsTable("bronze.events")
# MAGIC
# MAGIC     # Sink 2: MERGE aggregations (upsert pattern)
# MAGIC     df.createOrReplaceTempView("batch")
# MAGIC     spark.sql("""
# MAGIC         MERGE INTO silver.agg_events t
# MAGIC         USING batch s
# MAGIC         ON t.event_type = s.event_type
# MAGIC         WHEN MATCHED THEN UPDATE SET t.count = t.count + s.count
# MAGIC         WHEN NOT MATCHED THEN INSERT (event_type, count) VALUES (s.event_type, s.count)
# MAGIC     """)
# MAGIC
# MAGIC     # Sink 3: Write to Kafka topic for downstream consumers
# MAGIC     df.selectExpr("to_json(struct(*)) AS value") \
# MAGIC         .write \
# MAGIC         .format("kafka") \
# MAGIC         .option("kafka.bootstrap.servers", "broker:9092") \
# MAGIC         .option("topic", "processed.events") \
# MAGIC         .save()
# MAGIC
# MAGIC df.writeStream \
# MAGIC     .foreachBatch(write_microbatch) \
# MAGIC     .option("checkpointLocation", "/check/multi_sink/") \
# MAGIC     .trigger(processingTime="1 minute") \
# MAGIC     .start()
# MAGIC ```
# MAGIC **Key Point:** `foreachBatch` gives full programmatic access to each micro-batch for multi-sink writes, complex MERGEs, external API calls, and dynamic routing — but idempotency must be handled within the callback.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Performance &amp; Optimization &mdash; 6 Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q38: What is predicate pushdown and how does it improve query performance?
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** A query against a 100 TB Delta table with `WHERE sale_date = '2026-05-01'` still reads 100 GB of data. The Spark UI shows 10,000 files read.  
# MAGIC **Answer:** Predicate pushdown is a query optimization where filter conditions are applied at the storage layer (Parquet/Delta file reader) before data is passed to Spark for processing, using file-level and row-group-level statistics (min/max, bloom filters). For partitioned tables, partition pruning eliminates entire directories using the partition metadata stored in `_delta_log/`. This means Spark can skip entire files or row groups without physically opening them, dramatically reducing I/O. Without it, Spark would read every file and filter in-memory. Predicate pushdown works best when: (1) the table is partitioned on commonly filtered columns, (2) Delta statistics (min/max) are up to date (run OPTIMIZE with Z-ORDER or Liquid Clustering), and (3) queries use conjunctive WHERE clauses (AND) rather than OR/negation which defeat pruning.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Partition elimination: skips all directories except 2026-05-01
# MAGIC SELECT * FROM sales_partitioned
# MAGIC WHERE sale_date = '2026-05-01';
# MAGIC
# MAGIC -- File-level pruning via min/max statistics (requires OPTIMIZE)
# MAGIC SELECT * FROM sales
# MAGIC WHERE amount > 10000;  -- skips files where max(amount) <= 10000
# MAGIC
# MAGIC -- Check files read vs. files pruned in query plan
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT * FROM sales WHERE sale_date = '2026-05-01';
# MAGIC -- Look for: PartitionFilters, PushedFilters, numFilesRead
# MAGIC
# MAGIC -- Verify column statistics are available
# MAGIC DESCRIBE DETAIL sales;
# MAGIC ```
# MAGIC **Key Point:** Predicate pushdown filters at the storage layer using partition metadata and file-level min/max statistics, skipping entire files without reading them — the single most impactful query optimization.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q39: How do Delta Lake column statistics (min/max, null count) accelerate queries?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** A `WHERE customer_id BETWEEN 1000 AND 2000` query processes 10,000 files but returns only 50 rows. You need to understand why so many files were opened.  
# MAGIC **Answer:** Delta Lake automatically collects min/max, null count, and total record count for the first 32 columns of each data file during writes and stores them in the Delta log as part of each commit's metadata. During query planning, Spark reads these statistics and can skip files whose min/max range doesn't overlap with query predicates — a technique called data skipping or file pruning. For example, if file A has `min(customer_id)=5000, max(customer_id)=10000` and your query is `WHERE customer_id BETWEEN 1000 AND 2000`, file A is entirely skipped. To maximize pruning: run `OPTIMIZE` regularly (or use Liquid Clustering) to co-locate similar values, keep the column in the first 32 columns, and avoid casting/functions on the filtered column which defeat pruning. Use `DESCRIBE DETAIL` and `DESCRIBE HISTORY` to inspect statistics.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Check which columns have statistics collected
# MAGIC DESCRIBE DETAIL sales;
# MAGIC -- Look for "numFiles", "partitionColumns", "clusteringColumns"
# MAGIC
# MAGIC -- Force data layout that maximizes pruning
# MAGIC OPTIMIZE sales
# MAGIC ZORDER BY (customer_id);  -- co-locates similar values
# MAGIC
# MAGIC -- Query that benefits from ZORDER + statistics pruning
# MAGIC SELECT * FROM sales
# MAGIC WHERE customer_id = 123456;  -- may read only 1-2 files
# MAGIC
# MAGIC -- Avoid defeating pruning
# MAGIC -- BAD:  WHERE CAST(customer_id AS STRING) = '123456'  -- no pruning
# MAGIC -- GOOD: WHERE customer_id = 123456
# MAGIC
# MAGIC -- Check partition pruning with OPTIMIZE FULL (Liquid Clustering)
# MAGIC OPTIMIZE liquidity_transactions FULL;
# MAGIC ```
# MAGIC **Key Point:** Delta collects min/max/null statistics for the first 32 columns per file, enabling file-level data skipping. Maximize pruning with OPTIMIZE/Z-ORDER/Liquid Clustering and avoid wrapping filter columns in functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q40: How do you determine the right cluster size for a workload?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** Your team's ETL jobs take 2 hours on a small cluster. You double the cluster size and the job now takes... 1 hour 50 minutes.  
# MAGIC **Answer:** Cluster sizing follows a "sweet spot" principle — past a point, adding more cores doesn't reduce wall-clock time due to Amdahl's Law (shuffle/coordination bottleneck). Process: (1) Profile the job via Spark UI — check if tasks are skewed, if shuffle read/write dominates, or if there's excessive GC. (2) Look at executor utilization: if all cores are busy, increase executors; if many cores are idle, you've hit I/O or shuffle limits. (3) For I/O bound jobs, increase instances (horizontal scaling) to increase aggregate throughput. (4) For memory-bound jobs, increase executor memory rather than core count. (5) Use Photon (Databricks native engine) which generally runs 2–4× faster on the same cluster size for SQL/DataFrame workloads. Rule of thumb: start with a modest cluster, profile, then scale horizontally first (more workers, not bigger workers). Use serverless for unpredictable workloads.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Profile cluster utilization in Spark UI
# MAGIC # Navigate to Spark UI → Executors tab → check:
# MAGIC #   - Task Time (GC Time > 10% of Task Time = memory pressure)
# MAGIC #   - Shuffle Read/Write (if > 50% of task time = shuffle-bound)
# MAGIC #   - Input / Output records (CPU vs I/O bound)
# MAGIC
# MAGIC # Tuning executor configs via cluster settings
# MAGIC spark.conf.set("spark.executor.memory", "16g")
# MAGIC spark.conf.set("spark.executor.cores", "4")
# MAGIC spark.conf.set("spark.executor.instances", "8")
# MAGIC
# MAGIC # Quick benchmark: time with different instance counts
# MAGIC import time
# MAGIC start = time.time()
# MAGIC df = spark.sql("SELECT region, SUM(amount) FROM sales GROUP BY region")
# MAGIC df.collect()  # force compute
# MAGIC print(f"Wall clock: {time.time() - start:.2f}s")
# MAGIC ```
# MAGIC **Key Point:** Profile first (Spark UI → Executors tab); scale horizontally for I/O-bound, vertically for memory-bound. Past the sweet spot, diminishing returns set in. Start small, profile, scale. Use Photon for 2–4× boost.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q41: Explain the Databricks DBU cost model and optimization strategies.
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** Your monthly Databricks bill is $15,000. The CFO asks you to cut 30% without sacrificing pipeline reliability.  
# MAGIC **Answer:** Databricks costs are determined by DBUs (Databricks Units) — a normalized unit of compute per hour, multiplied by the SKU tier (Standard, Premium, Enterprise) and workload type (Jobs Compute, All-Purpose Compute, SQL Warehouses, Serverless). Job Compute (formerly Jobs Light) costs significantly less than All-Purpose Compute. Optimization strategies: (1) Use Job Compute clusters for all production workloads (not All-Purpose compute). (2) Auto-termination + auto-scaling to avoid idle time. (3) Serverless compute for bursty/unpredictable workloads (pay only for active processing). (4) Photon acceleration reduces total DBU consumption by finishing jobs faster (2–4× speedup = fewer DBU-hours). (5) SQL Warehouses over All-Purpose for BI queries, with auto-suspend. (6) Consolidate small jobs into fewer/larger pipelines with shared clusters. (7) Use Delta Live Tables with enhanced autoscaling for production ETL.  
# MAGIC **Code Example:**
# MAGIC ```python
# MAGIC # Use Job Compute (lower DBU rate) for scheduled jobs — configure in UI
# MAGIC # These settings go in the cluster configuration, not code
# MAGIC # Cluster type: "Jobs"  (not "All-Purpose Compute")
# MAGIC # Auto termination: 15 minutes (avoid idle DBU burn)
# MAGIC # Enable autoscaling: 2–8 workers (scale with workload)
# MAGIC
# MAGIC # Enable Photon in cluster config for 2-4x speedup (less DBU time)
# MAGIC # Photon: ON (Databricks Runtime option)
# MAGIC
# MAGIC # For notebooks used interactively → switch to Jobs cluster for prod
# MAGIC # Quick DBU cost estimator:
# MAGIC # DBUs/hour = (driver + N_workers) * DBU_rate * hours
# MAGIC # Example: 1 + 8 workers * 0.55 DBUs/node/hr * 24h = ~118 DBU/day on Jobs
# MAGIC
# MAGIC # Track DBU consumption programmatically
# MAGIC # Via System Tables: system.billing.usage
# MAGIC spark.sql("""
# MAGIC     SELECT
# MAGIC         usage_date,
# MAGIC         workspace_id,
# MAGIC         SUM(usage_quantity) AS dbus_consumed,
# MAGIC         sku_name
# MAGIC     FROM system.billing.usage
# MAGIC     WHERE usage_date > current_date() - INTERVAL 30 DAYS
# MAGIC     GROUP BY usage_date, workspace_id, sku_name
# MAGIC     ORDER BY usage_date DESC
# MAGIC """)
# MAGIC ```
# MAGIC **Key Point:** DBU costs = SKU tier × workload type × hours. Optimize with Job Compute (not All-Purpose), auto-termination, Photon (2-4× fewer hours), SQL Warehouses for BI, and serverless for bursty workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q42: How do you analyze and debug a slow query plan in Spark?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** A dashboard query that used to take 5 seconds now takes 45 seconds. The SQL looks fine — you need to dig into the plan to find the regression.  
# MAGIC **Answer:** Start with `EXPLAIN FORMATTED` or `df.explain("formatted")` to see the complete physical plan, including cost estimates, join strategy, and filter locations. Key red flags: (1) `SortMergeJoin` where one side is small (should be BroadcastHashJoin) — check `autoBroadcastJoinThreshold`, (2) `TableScan` reading many more files than expected — check partition pruning and file statistics, (3) `Exchange hashpartitioning` (shuffle) appearing in the plan — can it be avoided? (4) `CartesianProduct` — nearly always a bug (missing join condition). Use the Spark UI SQL tab to see per-stage metrics: duration, records read/shuffled, and spilled memory. For Delta tables, `DESCRIBE HISTORY` and `DESCRIBE DETAIL` reveal version size, file count, and statistics. Compare plans across versions to identify when degradation started.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Formatted explain: operator tree + cost estimates
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT
# MAGIC     c.region,
# MAGIC     SUM(s.amount) AS total
# MAGIC FROM silver.sales s
# MAGIC JOIN dim.customer c ON s.customer_id = c.customer_id
# MAGIC WHERE s.sale_date >= '2026-01-01'
# MAGIC GROUP BY c.region;
# MAGIC
# MAGIC -- Key things to scan in the output:
# MAGIC -- 1. Is it a BroadcastHashJoin or SortMergeJoin?
# MAGIC -- 2. What's the estimated vs. actual row count per operator?
# MAGIC -- 3. Are there PushedFilters or PartitionFilters listed?
# MAGIC -- 4. Is there an Exchange (shuffle) stage?
# MAGIC ```
# MAGIC ```python
# MAGIC # Programmatic plan analysis
# MAGIC df = spark.sql("SELECT region, SUM(amount) FROM sales GROUP BY region")
# MAGIC df.explain("formatted")
# MAGIC
# MAGIC # Check table health
# MAGIC spark.sql("DESCRIBE DETAIL silver.sales").show(truncate=False)
# MAGIC spark.sql("DESCRIBE HISTORY silver.sales").select("version", "operationMetrics").show()
# MAGIC
# MAGIC # Use Spark UI SQL tab (localhost:4040/SQL/) — look at:
# MAGIC #   - Duration per stage
# MAGIC #   - Records read vs. output (large gap = filter late)
# MAGIC #   - Shuffle read/write (spill = memory pressure)
# MAGIC #   - Task time distribution (skew = some tasks much slower)
# MAGIC ```
# MAGIC **Key Point:** Use `EXPLAIN FORMATTED` to inspect join strategies, shuffle location, and filter pushdown. Cross-reference with Spark UI SQL tab metrics to find the bottleneck operator.

# COMMAND ----------

# MAGIC ### Q43: What is MERGE write amplification and how can you minimize it?
# MAGIC **Difficulty:** Hard &bull; **Level:** Professional  
# MAGIC **Real Scenario:** A daily MERGE of 5,000 updates into a 5 TB table rewrites 500 GB of Parquet files. You need to understand why and fix it.  
# MAGIC **Answer:** Write amplification in MERGE occurs when a small number of row changes requires rewriting entire Parquet files (potentially 100s of MB each) because Parquet is immutable. A single updated row forces the entire file to be rewritten with a new copy. Mitigation strategies: (1) **Deletion Vectors** — instead of rewriting files, mark rows as deleted/updated in the Delta log; reduces write amplification to near-zero for the actual write but adds read overhead. (2) **Low-Shuffle Merge** — when source and target share the same partition scheme, Spark avoids re-shuffling and rewrites only affected partitions. (3) **Partition Pruning in Merge** — use `partitionFilter` or `WHERE` clause on the source side to restrict which partitions are scanned and rewritten. (4) **Liquid Clustering** — colocate data so updates cluster in fewer files. (5) **Z-ORDER on merge key** — colocate similar keys, reducing the number of files touched per update batch. (6) **Batch size tuning** — very small merge batches touch disproportionately many files; batch updates together for efficiency.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Enable Deletion Vectors: minimal file rewrites
# MAGIC ALTER TABLE orders
# MAGIC SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
# MAGIC
# MAGIC -- MERGE with source partition filter to limit scope
# MAGIC MERGE INTO orders t
# MAGIC USING (
# MAGIC     SELECT * FROM staging_orders
# MAGIC     WHERE order_date = '2026-05-11'  -- filter source
# MAGIC ) s
# MAGIC ON t.order_id = s.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC -- Low-Shuffle Merge: partition on merge key
# MAGIC CREATE TABLE orders
# MAGIC PARTITIONED BY (order_date)
# MAGIC AS SELECT * FROM parquet.`/raw/orders/`;
# MAGIC
# MAGIC -- Liquid Clustering: co-locates updates in fewer files
# MAGIC ALTER TABLE orders CLUSTER BY (order_id);
# MAGIC
# MAGIC -- Check write amplification after MERGE
# MAGIC DESCRIBE HISTORY orders;
# MAGIC -- Look for: numOutputBytes vs numTargetRowsUpdated
# MAGIC -- High ratio = write amplification
# MAGIC ```
# MAGIC **Key Point:** MERGE rewrites entire files per changed row. Minimize with Deletion Vectors (soft-delete), partition filtering, Liquid Clustering/Z-ORDER to colocate updates, and low-shuffle merge to limit rewrite scope.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Governance — 5 Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q44: Explain the Unity Catalog three-level namespace (catalog.schema.table) and its role in data governance.
# MAGIC **Difficulty:** Easy &bull; **Level:** Associate  
# MAGIC **Real Scenario:** Your organization has 3 business units (Sales, Finance, Marketing), each with their own data teams. You need to isolate their data while enabling cross-team sharing.  
# MAGIC **Answer:** Unity Catalog organizes all securable objects into a three-level hierarchy: **catalog** → **schema** → **table/view/volume/function**. A catalog is the top-level container (typically one per data domain or environment, e.g., `sales_prod`, `dev`). Schemas group related tables within a catalog (e.g., `sales_prod.bronze`, `sales_prod.silver`). Tables/views store the actual data. Permissions are applied at any level using SQL `GRANT`/`REVOKE` and cascade from parent to child objects. This hierarchy enables: (1) Environment isolation (dev <-> prod), (2) Team-level access boundaries (Sales catalog vs Finance catalog), (3) Fine-grained access control (table or column-level), and (4) Centralized metadata discovery across the entire metastore.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Create the namespace hierarchy
# MAGIC CREATE CATALOG IF NOT EXISTS sales_prod;
# MAGIC CREATE SCHEMA IF NOT EXISTS sales_prod.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS sales_prod.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS sales_prod.gold;
# MAGIC
# MAGIC -- Create a table within the namespace
# MAGIC CREATE TABLE sales_prod.bronze.raw_orders (
# MAGIC     order_id BIGINT,
# MAGIC     payload STRING
# MAGIC );
# MAGIC
# MAGIC -- Grant catalog-level access to the Sales team
# MAGIC GRANT USAGE ON CATALOG sales_prod TO `sales_team`;
# MAGIC GRANT SELECT ON TABLE sales_prod.gold.daily_kpi TO `analyst_group`;
# MAGIC
# MAGIC -- Discover all tables
# MAGIC SHOW TABLES IN sales_prod.silver;
# MAGIC
# MAGIC -- Check current grants
# MAGIC SHOW GRANTS ON TABLE sales_prod.gold.daily_kpi;
# MAGIC ```
# MAGIC **Key Point:** Unity Catalog namespace = `catalog.schema.table`. Catalogs isolate environments/domains, schemas organize tables logically, and permissions cascade from parent to child for centralized governance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q45: How does permission inheritance work in Unity Catalog?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** You grant `SELECT` on `sales_prod` catalog to a group of 50 analysts. Two months later, a new table is created — do the analysts automatically have access?  
# MAGIC **Answer:** Yes — Unity Catalog uses hierarchical permission inheritance. Permissions granted on a catalog automatically propagate to all schemas, tables, views, and volumes within it (existing AND future objects). Similarly, granting on a schema propagates to all objects within that schema. Permissions set on a child object override or supplement the parent's permissions (more restrictive permissions take precedence). Key permissions: `USAGE` (required to see/use the catalog or schema), `SELECT` (read data), `MODIFY` (insert/update/delete), `CREATE` (create child objects), `MANAGE` (grant/revoke on the object). You can audit effective permissions per principal with `SHOW GRANTS`.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Grant top-level — analysts get SELECT on ALL tables, present and future
# MAGIC GRANT USAGE ON CATALOG sales_prod TO `analyst_group`;
# MAGIC GRANT USAGE ON SCHEMA sales_prod.gold TO `analyst_group`;
# MAGIC GRANT SELECT ON SCHEMA sales_prod.gold TO `analyst_group`;
# MAGIC
# MAGIC -- New table: analysts automatically have SELECT (inherited from schema)
# MAGIC CREATE TABLE sales_prod.gold.new_metrics AS SELECT 1 AS id;
# MAGIC -- analysts CAN query sales_prod.gold.new_metrics without additional grants
# MAGIC
# MAGIC -- Override: deny access to a specific sensitive table
# MAGIC REVOKE SELECT ON TABLE sales_prod.gold.executive_kpi FROM `analyst_group`;
# MAGIC -- Now analysts have SELECT on all gold tables EXCEPT executive_kpi
# MAGIC
# MAGIC -- Audit: what can analysts actually access?
# MAGIC SHOW GRANTS ON CATALOG sales_prod;
# MAGIC SHOW GRANTS ON SCHEMA sales_prod.gold;
# MAGIC SHOW GRANTS ON TABLE sales_prod.gold.executive_kpi;
# MAGIC ```
# MAGIC **Key Point:** Unity Catalog permissions cascade: catalog → schema → table. Grant on a parent object auto-applies to all current AND future children. Revoke on child to selectively restrict.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q46: Compare dynamic views vs row filters for fine-grained access control.
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** Sales managers should see all rows for their region only. Finance should see rows with `amount > $10,000` only. A single table serves both consumers.  
# MAGIC **Answer:** **Row Filters** are declarative SQL predicates applied directly on the table that filter rows based on a user-defined function — e.g., `CREATE ROW FILTER region_filter ON sales AS region = current_user_region()`. They automatically propagate to all queries against the table regardless of the client tool (notebook, Power BI, SQL query). **Dynamic Views** are parameterized queries where the filter logic is embedded in the view definition using `CASE WHEN ... IS_ACCOUNT_GROUP_MEMBER(...)` to return different results per user. Row filters are simpler, centrally enforced, and harder to bypass. Dynamic views offer more flexibility (can return completely different columns or aggregates per group) but require users to query the view, not the table. Use row filters for simple row-level security; use dynamic views for complex multi-tenant logic where different groups see different column subsets or aggregations.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Row Filter: declarative, central, applies to ALL direct table queries
# MAGIC CREATE FUNCTION sales_prod.regional_access() RETURN
# MAGIC     CASE
# MAGIC         WHEN IS_ACCOUNT_GROUP_MEMBER('sales_managers') THEN
# MAGIC             (SELECT region_id FROM employee_mapping WHERE user_email = current_user() LIMIT 1)
# MAGIC         ELSE NULL
# MAGIC     END;
# MAGIC
# MAGIC ALTER TABLE sales_prod.gold.sales
# MAGIC SET ROW FILTER regional_access ON (region_id = regional_access());
# MAGIC
# MAGIC -- Dynamic View: flexible, per-group logic, must query the view
# MAGIC CREATE VIEW sales_prod.gold.sales_secure AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     CASE
# MAGIC         WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN amount
# MAGIC         WHEN IS_ACCOUNT_GROUP_MEMBER('analyst') THEN amount
# MAGIC         ELSE NULL  -- mask sensitive column for other groups
# MAGIC     END AS amount,
# MAGIC     region_id
# MAGIC FROM sales_prod.silver.sales
# MAGIC WHERE CASE
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('sales_managers') THEN region_id = regional_access()
# MAGIC     WHEN IS_ACCOUNT_GROUP_MEMBER('finance') THEN amount > 10000
# MAGIC     ELSE false
# MAGIC END;
# MAGIC ```
# MAGIC **Key Point:** Row filters = declarative, enforced at table level, automatic; Dynamic views = flexible (column masking, aggregates), must query view. Use row filters for simplicity; dynamic views for complex multi-tenant logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q47: How does Delta Sharing enable cross-organization data sharing?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** Your company is partnering with a marketing agency that needs read-only access to a subset of your Gold tables. They use Snowflake, not Databricks.  
# MAGIC **Answer:** Delta Sharing is an open protocol that enables sharing Delta tables across organizations without data replication. The provider (your org) creates a Share, adds tables to it, and generates recipient activation links. The recipient receives a credential file that they configure in their Databricks workspace (or any Delta Sharing-compatible client — Snowflake, Power BI, pandas). Shared data is read-only, preserves partitioning/statistics, and can be queried via SQL, DataFrames, or BI tools. No ETL needed on either side — the recipient sees a live, read-only view of the shared tables. Uses Unity Catalog for access control: who can create shares, who can add tables, who can be recipients. Supports table, notebook, and volume sharing.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Provider side: create and populate a share
# MAGIC CREATE SHARE IF NOT EXISTS partner_access;
# MAGIC
# MAGIC ALTER SHARE partner_access ADD TABLE sales_prod.gold.daily_kpi;
# MAGIC ALTER SHARE partner_access ADD TABLE sales_prod.gold.customer_360
# MAGIC     AS VERSION 42;  -- share specific snapshot
# MAGIC
# MAGIC -- Create a recipient and generate activation link
# MAGIC CREATE RECIPIENT marketing_agency;
# MAGIC -- Databricks generates a .share credential file for the recipient
# MAGIC
# MAGIC -- Grant recipient access to the share
# MAGIC GRANT SELECT ON SHARE partner_access TO RECIPIENT marketing_agency;
# MAGIC ```
# MAGIC ```python
# MAGIC # Recipient side (Databricks, any compute): consume shared data
# MAGIC shared_df = spark.read \
# MAGIC     .format("deltaSharing") \
# MAGIC     .option("responseFormat", "delta") \
# MAGIC     .load("marketing_agency.sales_prod.gold.daily_kpi")
# MAGIC
# MAGIC shared_df.select("sale_date", "revenue").show()
# MAGIC ```
# MAGIC **Key Point:** Delta Sharing = open protocol for sharing live Delta tables across orgs/providers without data replication. Recipients read via Databricks, Snowflake, pandas, or any compatible client using a credential file.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q48: What audit logging capabilities does Unity Catalog provide?
# MAGIC **Difficulty:** Medium &bull; **Level:** Associate  
# MAGIC **Real Scenario:** A security audit requires you to produce: all users who queried `sales_prod.gold.financials` in the last 90 days, all SELECT statements against PII columns, and all GRANT/REVOKE operations.  
# MAGIC **Answer:** Unity Catalog provides comprehensive audit logging through System Tables (`system.access.audit`) and Cloud audit logs (Azure Monitor, AWS CloudTrail). Audit logs capture: (1) **Data access events** — who queried what table/view, timestamp, source IP, client type (notebook, SQL warehouse, BI tool). (2) **Administrative events** — GRANT, REVOKE, CREATE/DROP catalog/schema/table, schema changes, and ownership transfers. (3) **Authentication events** — login attempts, token creations. System tables provide SQL-based access to recent audit data (typically 30-90 day retention), while cloud-native auditing services store indefinitely per cloud provider configurations. Audit logs can be streamed to SIEM systems (Splunk, Sentinel) for real-time alerting on unauthorized access patterns.  
# MAGIC **Code Example:**
# MAGIC ```sql
# MAGIC -- Query audit logs via system tables (last 30 days)
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     user_identity.email,
# MAGIC     action_name,  -- 'SELECT', 'INSERT', 'CREATE_TABLE', 'GRANT'
# MAGIC     request_params.commandText,
# MAGIC     source_ip_address,
# MAGIC     service_name  -- 'unityCatalog', 'sqlAnalytics', 'notebook'
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date > current_date() - INTERVAL 30 DAYS
# MAGIC   AND request_params.table_name = 'financials'
# MAGIC   AND action_name = 'SELECT'
# MAGIC ORDER BY event_time DESC;
# MAGIC
# MAGIC -- Track permission changes
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     user_identity.email,
# MAGIC     action_name,  -- 'GRANT', 'REVOKE'
# MAGIC     request_params.full_name_arg,  -- object path
# MAGIC     request_params.principal,       -- who was granted/revoked
# MAGIC     request_params.privileges       -- what permissions
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name IN ('GRANT', 'REVOKE')
# MAGIC ORDER BY event_time DESC;
# MAGIC
# MAGIC -- Find PII column access
# MAGIC SELECT DISTINCT
# MAGIC     user_identity.email,
# MAGIC     request_params.table_name
# MAGIC FROM system.access.audit
# MAGIC WHERE request_params.commandText LIKE '%ssn%'
# MAGIC    OR request_params.commandText LIKE '%credit_card%';
# MAGIC ```
# MAGIC **Key Point:** UC audit logs capture who, what, when, and from where for all data access and admin operations. Query via `system.access.audit` (30-90 day retention) or stream to SIEM for long-term storage and real-time alerting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Production &amp; Operations &mdash; 2 Q&A

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q49: What are Databricks Workflow design patterns for production pipelines?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** You need to orchestrate 15 interdependent tasks: raw ingestion → Bronze validation → Silver transformation → Gold aggregation → BI refresh → Slack notification. Failures in any stage should trigger retries and alerts.  
# MAGIC **Answer:** Databricks Workflows provide native orchestration for multi-task pipelines. Key patterns: (1) **Linear chain** — sequential dependent tasks; simplest pattern, use `depends_on: [previous_task]`. (2) **Fan-out / Fan-in** — a task spawns multiple parallel downstream tasks that converge into a single task after all complete. (3) **Conditional branching** — use `if/else` conditions and `run_job` tasks to dynamically choose paths based on data quality checks or row counts. (4) **Repair & rerun** — selectively re-run a failed task and its downstream dependencies without re-running the entire pipeline. (5) **Parameterized jobs** — pass runtime parameters (dates, table names) via job parameters for reusable, idempotent pipelines. (6) **For each** iterate a task over a list returned by a previous task. Use Job Clusters (not All-Purpose) for all tasks. Integrate with DBSQL dashboards for visual monitoring.  
# MAGIC **Code Example:**
# MAGIC ```yaml
# MAGIC # Databricks Asset Bundle: workflow definition
# MAGIC # resources/pipeline_job.yml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     medallion_pipeline:
# MAGIC       name: "Medallion Pipeline"
# MAGIC       parameters:
# MAGIC         - name: process_date
# MAGIC           default: "2026-05-11"
# MAGIC       tasks:
# MAGIC         - task_key: bronze_ingest
# MAGIC           job_cluster_key: etl_cluster
# MAGIC           notebook_task:
# MAGIC             notebook_path: /pipelines/01_bronze_ingest
# MAGIC             base_parameters:
# MAGIC               process_date: "{{job.parameters.process_date}}"
# MAGIC
# MAGIC         - task_key: silver_transform
# MAGIC           depends_on:
# MAGIC             - task_key: bronze_ingest
# MAGIC           job_cluster_key: etl_cluster
# MAGIC           notebook_task:
# MAGIC             notebook_path: /pipelines/02_silver_transform
# MAGIC             base_parameters:
# MAGIC               process_date: "{{job.parameters.process_date}}"
# MAGIC
# MAGIC         - task_key: validate_silver
# MAGIC           depends_on:
# MAGIC             - task_key: silver_transform
# MAGIC           condition_task:
# MAGIC             op: GREATER_THAN
# MAGIC             left: "{{tasks.silver_transform.values.row_count}}"
# MAGIC             right: "0"
# MAGIC
# MAGIC         - task_key: gold_aggregate
# MAGIC           depends_on:
# MAGIC             - task_key: validate_silver
# MAGIC           job_cluster_key: etl_cluster
# MAGIC           notebook_task:
# MAGIC             notebook_path: /pipelines/03_gold_aggregate
# MAGIC
# MAGIC         - task_key: notify_slack
# MAGIC           depends_on:
# MAGIC             - task_key: gold_aggregate
# MAGIC           webhook_notifications:
# MAGIC             on_success:
# MAGIC               id: slack_alert_id
# MAGIC ```
# MAGIC **Key Point:** Workflows support linear chains, fan-out/fan-in, conditional branching, repair-and-rerun, parameterization, and for-each loops. Use Job Clusters, built-in retries, and webhook notifications for production resilience.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q50: What are Databricks Asset Bundles and how do they support CI/CD?
# MAGIC **Difficulty:** Medium &bull; **Level:** Professional  
# MAGIC **Real Scenario:** Your team manages 30 notebooks, 5 workflows, and 2 DLT pipelines across dev/staging/prod. Deploying changes manually is error-prone and you need version-controlled, automated deployments.  
# MAGIC **Answer:** Databricks Asset Bundles (DABs) are a declarative infrastructure-as-code (IaC) framework that packages notebooks, workflows, DLT pipelines, clusters, and other workspace assets into version-controlled YAML definitions. A bundle consists of `databricks.yml` (the bundle manifest), resource definitions (separate YAML files for jobs/pipelines/clusters), and source files (notebooks, Python scripts). DABs integrate with CI/CD: (1) Commit to git triggers validation via `databricks bundle validate`. (2) `databricks bundle deploy` creates/updates all defined resources in the target workspace. (3) `databricks bundle run` executes a workflow. (4) Bundle targets (dev/staging/prod) allow different configurations per environment (different cluster sizes, schedules, parameters). DABs replace manual UI configuration and ad-hoc CLI commands, enabling GitOps-style deployment with pull request reviews and automated testing.  
# MAGIC **Code Example:**
# MAGIC ```yaml
# MAGIC # databricks.yml — bundle manifest
# MAGIC bundle:
# MAGIC   name: sales_data_pipeline
# MAGIC
# MAGIC include:
# MAGIC   - resources/*.yml  # job definitions
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     workspace:
# MAGIC       host: https://my-dev-workspace.cloud.databricks.com
# MAGIC     default: true
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         medallion_pipeline:
# MAGIC           schedule:
# MAGIC             quartz_cron_expression: "0 0 6 * * ?"
# MAGIC             timezone_id: "America/New_York"
# MAGIC   prod:
# MAGIC     workspace:
# MAGIC       host: https://my-prod-workspace.cloud.databricks.com
# MAGIC     run_as:
# MAGIC       service_principal_name: "prod-sp-12345"
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         medallion_pipeline:
# MAGIC           schedule:
# MAGIC             quartz_cron_expression: "0 30 2 * * ?"  # different schedule
# MAGIC             timezone_id: "America/New_York"
# MAGIC ```
# MAGIC ```bash
# MAGIC # CI/CD commands
# MAGIC # Validate bundle configuration
# MAGIC databricks bundle validate
# MAGIC
# MAGIC # Deploy to dev target
# MAGIC databricks bundle deploy --target dev
# MAGIC
# MAGIC # Run a specific job from the bundle
# MAGIC databricks bundle run medallion_pipeline --target dev
# MAGIC
# MAGIC # Deploy to production (after PR approval)
# MAGIC databricks bundle deploy --target prod
# MAGIC ```
# MAGIC **Key Point:** Asset Bundles package all Databricks assets into version-controlled YAML with environment-specific targets. Enable GitOps CI/CD via `validate → deploy → run` in automated pipelines, replacing manual UI deployment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Study Tips
# MAGIC
# MAGIC - **Associate-level focus:** Delta ACID, Time Travel, schema evolution, medallion architecture, COPY INTO, lazy evaluation, window functions, predicate pushdown.
# MAGIC - **Professional-level focus:** Liquid Clustering vs Z-ORDER, CDF, Deletion Vectors, AQE, Catalyst stages, salting for skew, watermarking, exactly-once semantics, MERGE write amplification, Asset Bundles.
# MAGIC - For each question, practice writing the SQL or PySpark code from memory.
# MAGIC - Use Databricks' free Community Edition or a trial workspace to run these examples.
# MAGIC - Review [Databricks Documentation](https://docs.databricks.com/) and [Delta Lake Documentation](https://docs.delta.io/) for deeper dives on each topic.
