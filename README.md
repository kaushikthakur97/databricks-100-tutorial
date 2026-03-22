<p align="center">
  <img src="social-preview.png" alt="The Databricks 100" width="800">
</p>

<sub>Independent educational resource. Not affiliated with or endorsed by Databricks, Inc.</sub>

**Databricks has hundreds of features, configs, and services. Here are the 100 that actually matter.**

Go through each category. Be honest: can you explain each concept to a colleague without looking anything up? If not, that's your gap.

Easy = what you should know after month one. Medium = solid mid-level knowledge. Hard = senior-level depth. All concepts reflect 2026 Databricks best practices.

> Built by [dataengineer.wiki](https://dataengineer.wiki) - cheat sheets, exercises, labs, and career tools for Databricks data engineers.

---

## Score Yourself

Go through every concept. Can you explain it without looking anything up? Count your score.

| Your Score | What It Means |
|-----------|---------------|
| **80-100** | Senior-level. You can architect and debug production systems. Focus on the Hard concepts you missed. |
| **50-79** | Solid mid-level. You know the daily work but have gaps in internals and advanced patterns. |
| **30-49** | Building foundations. Focus on Easy and Medium concepts in Categories 1, 3, and 4 first. |
| **Under 30** | Early stage. Start with all Easy concepts. Come back in 3 months and re-score. |

Share your score. Send this to a colleague who's prepping for Databricks interviews or certification.

### The 100-Day Challenge

One concept per day. 100 days. Post what you learned on LinkedIn with **#100DaysOfDatabricks**.

1. Fork this repo
2. One concept per day, starting at #1
3. Post your takeaway on LinkedIn
4. Check it off and move on

Day 1: ACID Transactions. Day 100: Testing Patterns. By the end, you can explain every concept a senior Databricks engineer needs to know.

---

## Progress Tracker

| # | Category | Concepts | Focus |
|---|----------|----------|-------|
| 1 | [Delta Lake](#1-delta-lake) | #1 - #10 | Storage layer fundamentals |
| 2 | [Spark Execution](#2-spark-execution) | #11 - #20 | How the engine works |
| 3 | [SQL & DataFrames](#3-sql--dataframes) | #21 - #30 | Daily transformation work |
| 4 | [Data Ingestion](#4-data-ingestion) | #31 - #40 | Getting data in |
| 5 | [Streaming](#5-streaming) | #41 - #50 | Real-time processing |
| 6 | [Performance & Cost](#6-performance--cost) | #51 - #60 | Making it fast and cheap |
| 7 | [Unity Catalog & Governance](#7-unity-catalog--governance) | #61 - #70 | Security and access control |
| 8 | [Lakeflow Declarative Pipelines](#8-lakeflow-declarative-pipelines) | #71 - #80 | Managed pipeline engine (formerly DLT) |
| 9 | [Workflows, CI/CD & Operations](#9-workflows-cicd--operations) | #81 - #90 | Running it in production |
| 10 | [Architecture & Advanced Patterns](#10-architecture--advanced-patterns) | #91 - #100 | Senior-level decisions |

---

## 1. Delta Lake

The storage layer that makes Databricks a lakehouse. Every table you create, query, or maintain runs on Delta.

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 1 | **ACID Transactions** | Easy | How Delta Lake guarantees atomicity, consistency, isolation, and durability on top of cloud object storage. Optimistic concurrency control. Why this matters vs. raw Parquet. |
| 2 | **Transaction Log (`_delta_log`)** | Easy | How the JSON-based transaction log tracks every change. What happens during reads (snapshot isolation). Log checkpoints every 100 commits. How the log enables time travel. |
| 3 | **Time Travel & RESTORE** | Easy | Querying previous versions with `VERSION AS OF` and `TIMESTAMP AS OF`. Restoring tables with `RESTORE`. When time travel stops working (after VACUUM). Retention configuration. |
| 4 | **Schema Enforcement vs. Evolution** | Medium | How `mergeSchema` and `overwriteSchema` work. What happens when a write doesn't match the schema. When to use enforcement vs. evolution in production. |
| 5 | **Liquid Clustering** | Medium | The recommended layout strategy for all new tables. How `CLUSTER BY` works. Replaces legacy partitioning and Z-ORDER. Clustering triggered on writes that exceed size thresholds. Why OPTIMIZE is still recommended for comprehensive clustering. |
| 6 | **OPTIMIZE & File Compaction** | Medium | Why small files kill read performance (causes: frequent appends, streaming micro-batches). How OPTIMIZE rewrites files and triggers Liquid Clustering. Auto Compaction as a lighter-weight alternative. When to rely on Predictive Optimization (#53) vs. running manually. |
| 7 | **VACUUM & Storage Lifecycle** | Medium | How VACUUM removes unreferenced files. The 7-day default retention and why lowering it breaks time travel. Interaction with concurrent readers. How Predictive Optimization automates VACUUM on managed tables. |
| 8 | **Change Data Feed (CDF)** | Medium | How to enable and query `table_changes()`. The `_change_type` column (insert, update_preimage, update_postimage, delete). Use cases: incremental downstream processing, audit trails, replication. |
| 9 | **Deletion Vectors** | Medium | Soft-delete markers that avoid rewriting files on DELETE, UPDATE, and MERGE. Enabled by default on new tables (rolling out). Physical deletion happens at OPTIMIZE/compaction. Compatibility considerations for external readers. |
| 10 | **Delta Table Properties & Configuration** | Hard | Key table properties every DE manages: `delta.enableChangeDataFeed`, `delta.enableDeletionVectors`, `delta.columnMapping.mode`, `delta.minReaderVersion`/`minWriterVersion`, `delta.checkpointInterval`. How these affect compatibility, performance, and feature availability. Checking and setting with `ALTER TABLE SET TBLPROPERTIES`. |

---

## 2. Spark Execution

Spark is the compute engine. Senior engineers don't just write transformations; they understand how Spark executes them and why it matters.

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 11 | **Lazy Evaluation & Actions** | Easy | Why transformations build a plan but don't execute. Which operations are actions (collect, count, write, show) vs. transformations (filter, select, join). Why this enables optimization. |
| 12 | **Photon Engine** | Easy | Native vectorized C++ query engine for SQL and DataFrame operations. Which operations benefit most (scans, filters, aggregations, joins). How to verify Photon is active in query plans. Automatically enabled on serverless compute. |
| 13 | **Catalyst Optimizer & Query Plans** | Medium | How Catalyst transforms logical plans into physical plans. Reading `EXPLAIN` output. Recognizing when the optimizer makes good vs. poor decisions. |
| 14 | **Adaptive Query Execution (AQE)** | Medium | How AQE adjusts plans at runtime based on actual data statistics. Three key optimizations: coalescing shuffle partitions, converting sort-merge joins to broadcast, optimizing skew joins. Enabled by default. |
| 15 | **Shuffle Operations** | Medium | What triggers a shuffle (groupBy, join, repartition, distinct). Why shuffles are expensive (serialization + network + disk I/O). Reading shuffle metrics in Spark UI. |
| 16 | **Join Strategies** | Medium | The three join types: broadcast hash join (small table fits in memory), sort-merge join (default for large-large), shuffle hash join (rare). The 10MB broadcast threshold (higher with Photon). Using `broadcast()` hints. How AQE dynamically converts at runtime. |
| 17 | **Reading the Spark UI** | Medium | The tabs that matter: Jobs, Stages, Tasks, SQL. What to look for: task duration variance, spill metrics, shuffle read/write sizes, scan statistics. Diagnosing bottlenecks from the UI. |
| 18 | **Partitioning & Parallelism** | Medium | How `spark.sql.shuffle.partitions` controls parallelism after shuffles. When to repartition vs. coalesce. The relationship between partition count, core count, and task parallelism. How to diagnose over/under-partitioning from Spark UI task metrics. |
| 19 | **Data Skew: Detection & Mitigation** | Hard | Recognizing skew in Spark UI (one task takes 10x longer). Why skew kills parallelism. Salting technique for skewed keys. AQE's automatic skew join optimization. When manual salting is still needed. |
| 20 | **Memory Management & Spill** | Hard | The unified memory model (execution + storage pool). What causes spill to disk. How to diagnose spill from Spark UI. Tuning `spark.sql.shuffle.partitions` and executor memory. |

---

## 3. SQL & DataFrames

The daily work. Transforming data, writing queries, building ELT logic.

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 21 | **DataFrame API: Core Operations** | Easy | select, filter, withColumn, groupBy, agg, join. Method chaining patterns. When to use DataFrame API vs. Spark SQL (same execution plan either way). |
| 22 | **Temp Views & Global Temp Views** | Easy | Scope: temp views live in the SparkSession, global temp views in `global_temp` and survive across notebooks on the same cluster. Neither persists across restarts. |
| 23 | **Window Functions** | Medium | ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, running totals with SUM OVER. Partition and ordering clauses. How these translate to physical execution (shuffles on partition keys). |
| 24 | **Complex Types: ARRAY, STRUCT, MAP** | Medium | Creating and querying nested structures. Exploding arrays with `explode()` and `posexplode()`. Struct field access with dot notation. Flattening deeply nested JSON. |
| 25 | **Higher-Order Functions** | Medium | `transform()`, `filter()`, `exists()`, `aggregate()` for array manipulation without exploding. When these outperform explode + groupBy. Real patterns: cleaning arrays, conditional filtering. |
| 26 | **CTEs, PIVOT & Subqueries** | Medium | Writing CTEs for readable multi-step queries. PIVOT for wide-format reshaping. Correlated vs. uncorrelated subqueries. How these map to physical plans. |
| 27 | **UDFs vs. Native Functions** | Medium | Why Python UDFs break Catalyst and force row-by-row execution. The 10-100x performance gap. Pandas UDFs (vectorized) as a middle ground. When a UDF is genuinely the right choice. |
| 28 | **VARIANT Type** | Medium | Storing semi-structured data (JSON) in a native binary format. Schema-on-read without parsing overhead. Querying with `:` path syntax. When VARIANT outperforms STRING or STRUCT columns for JSON data. |
| 29 | **MERGE INTO & Upsert Patterns** | Hard | The MERGE syntax for insert/update/delete in one statement. Clause ordering (MATCHED before NOT MATCHED). Write amplification and why MERGE rewrites entire files. Optimization: match on Liquid Clustering keys. How deletion vectors reduce cost. |
| 30 | **Table Constraints & Generated Columns** | Medium | CHECK constraints, NOT NULL, PRIMARY KEY (informational), FOREIGN KEY (informational). Generated columns with expressions (e.g., `date GENERATED ALWAYS AS CAST(timestamp AS DATE)`). How constraints enforce data quality at the write layer. |

---

## 4. Data Ingestion

Getting data into the lakehouse reliably, incrementally, and at scale.

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 31 | **COPY INTO vs. Auto Loader** | Easy | COPY INTO for ad-hoc/batch loads (idempotent, simpler). Auto Loader for continuous/streaming ingestion (exactly-once, scalable, schema evolution). Why Auto Loader is preferred for production. |
| 32 | **File Formats in the Lakehouse** | Easy | Why Parquet is the default (columnar, compressed, schema-embedded). Delta stores Parquet under the hood. When to use JSON, CSV, or Avro for ingestion sources. Schema-on-read vs. schema-on-write. |
| 33 | **Volumes** | Easy | Governed access to non-tabular files (CSVs, images, ML models) within Unity Catalog. Managed vs. external volumes. Using Volumes as landing zones and staging areas. |
| 34 | **Auto Loader: File Discovery** | Medium | File notification mode (cloud events, efficient at scale, requires setup) vs. directory listing mode (simpler, slower with millions of files). When to use each. Why Auto Loader is the default for production ingestion. |
| 35 | **Auto Loader: Schema Evolution & Rescued Data** | Medium | How schema inference works from the first batch. `cloudFiles.schemaEvolutionMode` options (addNewColumns, rescue, failOnNewColumns, none). The `_rescued_data` column for capturing mismatches instead of failing. |
| 36 | **Multi-Hop Medallion Architecture** | Medium | Bronze (raw, append-only), Silver (cleaned, deduplicated, typed), Gold (business aggregates). Why this pattern exists. When it's overkill. How each layer serves different consumers with different freshness requirements. |
| 37 | **Incremental Processing Patterns** | Medium | Processing only new/changed data. Techniques: Auto Loader (file-level), Change Data Feed (row-level), watermark-based (time-based), merge keys (hash-based). Cost vs. latency tradeoffs for each. |
| 38 | **Lakehouse Federation** | Medium | Querying external databases (PostgreSQL, MySQL, SQL Server, Snowflake) without copying data. Query pushdown optimization. Connection objects and security. When federation beats ETL and when it doesn't. |
| 39 | **Idempotent Pipeline Design** | Hard | Why every production pipeline must produce the same result when re-run. Databricks-specific techniques: MERGE with natural keys, `INSERT OVERWRITE` with `replaceWhere`, Auto Loader checkpoint recovery, Workflows retry policies. How streaming checkpoints provide built-in idempotency. |
| 40 | **SCD Type 1 & Type 2 Patterns** | Hard | SCD1 (overwrite current values) vs. SCD2 (maintain full history with effective dates). Implementing SCD2 with MERGE. Why Lakeflow Declarative Pipelines' `AUTO CDC INTO` (#75) is simpler for SCD2 at scale. |

---

## 5. Streaming

Processing data as it arrives. Databricks Structured Streaming and its integration with the lakehouse.

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 41 | **Structured Streaming Fundamentals** | Medium | How Structured Streaming on Databricks uses Delta Lake as both source and sink. The `readStream`/`writeStream` API. Why Delta-to-Delta streaming is the most common production pattern. Integration with Auto Loader as a file source. |
| 42 | **Triggers** | Medium | `processingTime` (recurring micro-batches at fixed intervals). `availableNow` (process all available data then stop; replaces deprecated `once`). Choosing the right trigger for latency vs. cost. |
| 43 | **Output Modes** | Medium | Append (new rows only, default). Complete (full result table, aggregations only). Update (changed rows only). Which modes work with which operations and sinks. |
| 44 | **Stream-Static Joins** | Medium | Joining streaming DataFrames with batch DataFrames. Static side re-read each micro-batch. Use cases: enriching events with dimension data. Outer join limitations on the streaming side. |
| 45 | **Windowed Aggregations** | Medium | Tumbling windows (fixed, non-overlapping), sliding windows (fixed, overlapping), session windows (gap-based). Combining windows with watermarks for bounded state. |
| 46 | **foreachBatch Pattern** | Medium | Using `foreachBatch()` to apply arbitrary batch logic to each micro-batch. Use cases: writing to multiple sinks, MERGE operations in streaming, calling external APIs. Exactly-once considerations. |
| 47 | **Streaming Tables in Lakeflow Pipelines** | Medium | How streaming tables in Lakeflow Declarative Pipelines abstract away checkpoint and state management. The difference between streaming tables and raw Structured Streaming. When to use each approach. |
| 48 | **Checkpointing & Exactly-Once** | Hard | How checkpoint directories track processing state. Stream restart behavior (replay from checkpoint). Why checkpoint location must be stable and unique per query. Limitations of exactly-once guarantees. |
| 49 | **Watermarks & Late Data** | Hard | How `withWatermark()` bounds how late data can arrive. The tradeoff: longer watermark = more complete results but higher memory/state. How Spark drops state for expired windows. |
| 50 | **Stream-Stream Joins** | Hard | Joining two streaming DataFrames. Watermark requirements for state cleanup. Inner, left outer, and right outer join semantics. State management implications. |

---

## 6. Performance & Cost

The difference between a pipeline that costs $50/month and one that costs $5,000/month.

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 51 | **Serverless Compute Economics** | Easy | How serverless pricing works (DBU-based, pay per query/task vs. per cluster-hour). When serverless is cheaper vs. classic (bursty workloads vs. sustained compute). Why serverless eliminates idle cluster waste. |
| 52 | **Predicate Pushdown & Data Skipping** | Medium | How Spark skips reading files/partitions that can't match filter predicates. File-level min/max column statistics. What breaks pushdown (casting, functions on filter columns). How Liquid Clustering keys maximize skipping. |
| 53 | **Predictive Optimization** | Medium | Databricks automatically runs OPTIMIZE, VACUUM, and ANALYZE on managed tables based on query patterns. Enabled by default on new accounts. Why you rarely need manual maintenance jobs on managed tables. Monitoring via system tables. |
| 54 | **Column Statistics & File Pruning** | Medium | Per-file min/max statistics collected for the first 32 columns by default (managed tables with Predictive Optimization choose columns intelligently). Why column order matters. How `ANALYZE TABLE` refreshes stats for the cost-based optimizer. |
| 55 | **Write Optimization** | Medium | `optimizeWrite` for automatic file sizing on write. `maxRecordsPerFile` for controlling output file sizes. When to coalesce before writing. Target file sizes for different workloads. How write optimization prevents the small file problem before it starts. |
| 56 | **Cluster Sizing & Autoscaling** | Medium | Right-sizing for ETL vs. ML vs. SQL analytics workloads. Autoscaling min/max workers. Spot instances for cost savings. The relationship between partition count, core count, and parallelism. |
| 57 | **DBU Cost Model & Billing** | Medium | How Databricks bills (DBUs by SKU: Jobs, SQL, All-Purpose, Serverless). Understanding the billing system tables (`system.billing.usage`). Cluster policies and budgets to prevent runaway costs. Tagging jobs for cost allocation and chargeback. |
| 58 | **Query Profile & Execution Plans** | Medium | Reading the DBSQL Query Profile (visual plan, statistics, bottleneck identification). The difference between logical and physical plans. Identifying slow operators: scans, joins, aggregations, sorts. |
| 59 | **Join Performance Diagnosis** | Hard | When a join is the bottleneck: reading shuffle sizes, spill metrics, and task skew in Spark UI for join stages. Tuning `spark.sql.autoBroadcastJoinThreshold`. Choosing between forcing broadcast hints vs. redesigning the query. When to pre-aggregate or filter before joining to reduce shuffle volume. |
| 60 | **MERGE Write Amplification** | Hard | Why MERGE rewrites entire files containing matched rows. Optimization: match on Liquid Clustering keys, leverage deletion vectors, right-size target files. Measuring amplification with `DESCRIBE HISTORY`. |

---

## 7. Unity Catalog & Governance

Security, access control, and data governance. Required for every production Databricks environment.

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 61 | **Three-Level Namespace** | Easy | `catalog.schema.table` hierarchy. How it maps to organizational structure. Default catalog and schema settings. Migrating from the legacy Hive Metastore. |
| 62 | **Permission Model & Inheritance** | Medium | How privileges cascade down (catalog -> schema -> table). Key privileges: USE CATALOG, USE SCHEMA, SELECT, MODIFY, CREATE TABLE. Owner vs. granted access. GRANT and REVOKE syntax. |
| 63 | **Dynamic Views for Security** | Medium | Using `CURRENT_USER()` and `IS_MEMBER()` in view definitions. Column masking patterns (full SSN for HR, masked for everyone else). When to use dynamic views vs. row filters/column masks. |
| 64 | **Data Lineage** | Medium | How Unity Catalog automatically tracks column-level lineage across notebooks, jobs, and SQL. Viewing lineage in Catalog Explorer. Using lineage for impact analysis before schema changes. |
| 65 | **Information Schema & Metadata Queries** | Medium | Querying `system.information_schema.tables`, `.columns`, `.table_privileges` to discover and audit catalog contents. Building compliance reports from metadata. Automating governance checks. |
| 66 | **External Locations & Storage Credentials** | Medium | Mapping cloud storage paths to Unity Catalog governance. Storage credentials (IAM roles, service principals). When to use managed vs. external tables based on data ownership requirements. |
| 67 | **Service Principals & Managed Identity** | Medium | Using service principals for automated jobs instead of personal credentials. Managed identity on Azure, instance profiles on AWS. Why production workloads must never run under personal accounts. |
| 68 | **Audit Logging** | Medium | `system.access.audit` for tracking who did what and when. Querying audit logs for compliance, access reviews, and security investigations. Building audit dashboards. |
| 69 | **Row Filters & Column Masks** | Hard | Applying row-level and column-level security directly to tables via SQL UDFs. `ALTER TABLE SET ROW FILTER`. `ALTER TABLE ALTER COLUMN SET MASK`. When to use these vs. dynamic views. |
| 70 | **Delta Sharing** | Hard | Secure, cross-organization data sharing without copying data. Open protocol (works with non-Databricks recipients). Share objects, recipients, activation links. Sharing live tables vs. snapshots. |

---

## 8. Lakeflow Declarative Pipelines

The managed pipeline engine for reliable, maintainable data pipelines. Formerly known as Delta Live Tables (DLT).

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 71 | **Python vs. SQL Pipeline Syntax** | Easy | SQL: `CREATE STREAMING TABLE`, `CREATE MATERIALIZED VIEW`. Python: `@table`, `@materialized_view`, `@temporary_view` decorators (from `pyspark.pipelines`). Old `import dlt` syntax still works. Choosing based on team skills. |
| 72 | **Streaming Tables vs. Materialized Views** | Medium | Streaming tables for append-only ingestion with exactly-once guarantees. Materialized views for aggregations that need full recomputation. How the engine manages refresh, schema, and state automatically. |
| 73 | **Expectations (Quality Rules)** | Medium | Defining data quality constraints that warn, drop, or fail on violation. Monitoring expectation pass/fail rates in the pipeline UI. Using expectations as production data quality gates. |
| 74 | **Pipeline Development & Testing** | Medium | Development vs. production modes. Incremental vs. full refresh. Testing pipeline logic locally before deployment. The pipeline settings and configuration options. |
| 75 | **Pipeline Monitoring & Event Log** | Medium | Reading the event log for pipeline run history, data quality metrics, and lineage. Setting up alerts on pipeline failures or expectation violations. Integration with Databricks SQL dashboards. |
| 76 | **Pipeline Modes: Triggered vs. Continuous** | Medium | Triggered pipelines (batch-like, process available data then stop) vs. continuous pipelines (always-on, sub-second latency). Cost and latency tradeoffs. When each mode is appropriate. |
| 77 | **CDC Processing: AUTO CDC INTO** | Hard | Processing Change Data Capture feeds with `AUTO CDC INTO` (SQL) or `create_auto_cdc_flow()` (Python). Formerly `APPLY CHANGES INTO`. Automatic SCD Type 1 and Type 2 handling. Sequencing columns and deduplication. |
| 78 | **Error Handling & Dead Letter Patterns** | Hard | Routing bad records to quarantine tables instead of failing the pipeline. Using expectations with `ON VIOLATION DROP ROW` plus a separate quality monitoring table. Building observability into pipeline design. |
| 79 | **Pipeline Parameters & Configuration** | Hard | Parameterizing pipelines for dev/staging/prod environments. Configuration management patterns. Passing runtime settings. Managing pipeline dependencies across environments. |
| 80 | **Multi-Pipeline Architecture** | Hard | Splitting large pipelines into smaller, focused pipelines with clear boundaries. Sharing tables between pipelines. Managing dependencies and execution ordering. When a single pipeline becomes too complex to maintain. |

---

## 9. Workflows, CI/CD & Operations

Running Databricks in production. The difference between a notebook that works and a system that's reliable.

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 81 | **Choosing Compute for Jobs** | Easy | Serverless compute as the default for new jobs (no cluster management, Photon auto-enabled). When to use job clusters instead (custom init scripts, GPU workloads, unsupported task types). Why all-purpose clusters are for development, not production. Instance pools for fast classic cluster startup. |
| 82 | **Secrets Management** | Easy | Databricks Secret Scopes for credentials (API keys, database passwords). `dbutils.secrets.get()` in notebooks. Key-vault backed scopes (Azure Key Vault, AWS Secrets Manager). Why secrets must never be hardcoded. |
| 83 | **Parameterization: Widgets, Job Parameters, Task Values** | Easy | `dbutils.widgets` for notebook parameters. Job parameters passed from Workflows. Task values for inter-task communication. Databricks SQL parameters for parameterized queries. |
| 84 | **Notebook Development Patterns** | Easy | `%run` for shared utilities. `dbutils.notebook.run()` for orchestration. Magic commands (`%sql`, `%python`, `%md`). Notebook-scoped libraries. When to use notebooks vs. Python files in production. |
| 85 | **Databricks Workflows: Multi-Task Jobs** | Medium | Creating jobs with multiple tasks (notebooks, Python scripts, SQL, dbt, JARs). Task dependencies (linear, fan-out, fan-in). Run conditions and error handling. |
| 86 | **Task Dependencies, Retries & Conditional Logic** | Medium | Per-task retry policies and timeout limits. Conditional execution based on upstream task outcomes. Task values for passing data between tasks. Email and webhook alerting on failure. |
| 87 | **Git Integration & Repos** | Medium | Connecting Databricks to GitHub/GitLab/Azure DevOps. Databricks Repos for version-controlled notebooks. Branch-based development. Pull request workflows for notebook code. |
| 88 | **Monitoring, Alerting & SQL Alerts** | Medium | Setting up SQL alerts in DBSQL (threshold-based notifications). System tables for operational monitoring. Cluster event logs. Integrating with PagerDuty, Slack, or email for pipeline failures. |
| 89 | **System Tables for Operations** | Medium | `system.billing.usage` for cost tracking and DBU consumption by SKU. `system.compute.clusters` for cluster lifecycle and utilization. Building operational dashboards for cost monitoring and capacity planning. |
| 90 | **Databricks Asset Bundles (DABs)** | Hard | The `databricks.yml` configuration file. Defining jobs, pipelines, and models as code. Environment targets (dev/staging/prod). `databricks bundle validate/deploy/destroy`. GitHub Actions integration. |

---

## 10. Architecture & Advanced Patterns

Senior-level decisions. Understanding tradeoffs, designing systems, managing complexity.

| # | Concept | Difficulty | What You Should Know |
|---|---------|------------|---------------------|
| 91 | **Shallow vs. Deep Clones** | Easy | SHALLOW CLONE (zero-copy, references source files) vs. DEEP CLONE (full copy, independent). Use cases: dev/test environments, table migration, disaster recovery snapshots, schema evolution testing. |
| 92 | **Managed vs. External Tables: Architecture Tradeoffs** | Medium | When managed tables are the right default (governance, Predictive Optimization, automatic lifecycle). When external tables are necessary (multi-engine access, data ownership boundaries, regulatory requirements). |
| 93 | **Legacy: Partitioning & Z-ORDER** | Medium | Why these are legacy approaches superseded by Liquid Clustering. When partitioning still makes sense (very large tables, low-cardinality columns, non-Databricks reader compatibility). Z-ORDER mechanics for maintaining legacy tables. Migration planning. |
| 94 | **Lakehouse Table Design Patterns** | Medium | When to denormalize vs. normalize in a lakehouse (wide tables for analytics, normalized for transactional). Star schema in Gold layer. Choosing between many small tables vs. fewer wide tables. Clustering key selection and column ordering for statistics. |
| 95 | **Delta UniForm & Multi-Engine Interoperability** | Medium | How UniForm enables a single Delta table to be read as Apache Iceberg or Apache Hudi. When to use UniForm vs. Delta Sharing vs. Lakehouse Federation for cross-platform access. Enabling and configuring. |
| 96 | **Compute Policies & Cluster Governance** | Medium | Using cluster policies to control what users can provision (instance types, max workers, spot ratios, auto-termination). Preventing cost overruns. Balancing developer freedom with budget discipline. |
| 97 | **AI Functions for Data Engineers** | Medium | Using `ai_query()` and `ai_generate()` SQL functions for data enrichment, classification, and extraction within pipelines. When AI functions replace complex regex/UDF logic. Cost and latency considerations. |
| 98 | **Multi-Workspace Architecture** | Hard | When to use one workspace vs. many (team isolation, compliance boundaries, regional deployment). Sharing Unity Catalog metastore across workspaces. Cross-workspace job orchestration. Account-level vs. workspace-level governance. |
| 99 | **Performance Troubleshooting Methodology** | Hard | A structured approach to diagnosing slow pipelines: check Spark UI stage breakdown, identify shuffle/spill bottlenecks, examine query plan for full scans or bad joins, verify clustering/statistics, check cluster utilization. Building runbooks for common performance patterns. |
| 100 | **Testing Patterns for Data Pipelines** | Hard | Unit testing transformations with sample DataFrames. Integration testing with representative data subsets. Data quality assertions beyond Lakeflow expectations. Testing schema evolution scenarios. CI/CD test pipelines with DABs. |

---

## Practice & Resources

| Resource | What It Covers | Link |
|----------|---------------|------|
| **DataDojo** | 633+ practice exercises across all categories | [dojo.dataengineer.wiki](https://dojo.dataengineer.wiki) |
| **Data Engineer Wiki** | Cheat sheets, learning paths, cert prep guides | [dataengineer.wiki](https://dataengineer.wiki) |
| **Hands-On Labs** | Production-grade Databricks labs on GitHub | [GitHub Labs](https://github.com/topics/databricks-lab) |
| **Interview Cheat Sheet** | 86 senior-level Databricks interview Q&A | [dataengineer.wiki/cheat-sheet-senior](https://dataengineer.wiki/cheat-sheet-senior) |
| **Databricks Academy** | Official free courses | [academy.databricks.com](https://academy.databricks.com) |
| **Databricks Docs** | Official reference | [docs.databricks.com](https://docs.databricks.com) |

---

## Contributing

Found an error? Think a concept should be added or replaced? Open an issue or PR.

**Inclusion criteria**: A concept must be something 80%+ of Databricks data engineers encounter at least monthly in production. Niche edge cases don't make the list.

---

## FAQ

**Why 100?**
Databricks has hundreds of features. These are the 100 concepts Databricks data engineers encounter most often in production. Every concept earns its spot through a universality test: if most engineers rarely encounter it, it's not on the list.

**Is this for the Associate or Professional cert?**
Both. The Associate covers roughly 60 of the 100. The Professional covers all 100 at production depth.

**I scored under 50. Should I panic?**
No. Most engineers have gaps they don't know about. That's the point of this list. Focus on the Easy concepts first, re-score in 3 months.

**Can I use this for interview prep?**
Yes. These concepts appear in real Databricks data engineering interviews. The [Interview Cheat Sheet](https://dataengineer.wiki/cheat-sheet-senior) has detailed answers for the most common questions.

**Something is outdated or wrong.**
Open an issue. Databricks evolves fast. This list reflects 2026 best practices (Liquid Clustering over Z-ORDER, Lakeflow Declarative Pipelines over DLT, serverless as default, Predictive Optimization, etc.). We update when the platform changes.

---

## License

MIT License. Use it, share it, fork it.

---

**Ready to start? Fork the repo, pick concept #1, and post your first #100DaysOfDatabricks on LinkedIn.**

Built and maintained by [dataengineer.wiki](https://dataengineer.wiki).
