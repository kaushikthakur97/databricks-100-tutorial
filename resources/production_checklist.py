# Databricks notebook source
# MAGIC %md
# MAGIC # Production Best Practices Checklist
# MAGIC
# MAGIC A comprehensive verification guide for data engineers preparing workloads for
# MAGIC production deployment on Databricks. Run each verification cell against your
# MAGIC target tables and pipelines to catch issues before they reach production.
# MAGIC
# MAGIC **Prerequisites:** Unity Catalog enabled, `SELECT` on target tables, access
# MAGIC to `information_schema` and `system.billing` (for cost checks).
# MAGIC
# MAGIC This notebook is serverless-compatible — all commands use pure Spark SQL or
# MAGIC standard Python APIs.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Table Design Checklist
# MAGIC
# MAGIC Good table design is the foundation of reliable, performant, and governable
# MAGIC data. Each item below targets a specific design decision.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Format & Storage
# MAGIC
# MAGIC | # | Check | Guidance |
# MAGIC |---|-------|----------|
# MAGIC | [ ] | **Use Delta Lake format** | Never deploy raw Parquet, ORC, JSON,
# MAGIC or CSV tables to production. Delta provides ACID transactions, time travel,
# MAGIC schema enforcement, and performance optimisations. |
# MAGIC | [ ] | **Managed vs External tables chosen deliberately** | Use managed
# MAGIC tables when Databricks should own the lifecycle. Use external tables when
# MAGIC data must survive catalog drops or be shared across workspaces. |
# MAGIC | [ ] | **Liquid Clustering keys defined** | Choose 2-4 columns from
# MAGIC high-cardinality filter/join keys (not low-cardinality like `status`). |
# MAGIC | [ ] | **Z-ordering removed** | If liquid clustering is enabled, remove
# MAGIC legacy `ZORDER BY` from OPTIMIZE commands — they conflict. |
# MAGIC | [ ] | **Change Data Feed (CDF) enabled if needed** | Enable only if
# MAGIC downstream consumers read incremental changes. CDF adds write overhead. |
# MAGIC | [ ] | **Table properties set** | At minimum: `delta.logRetentionDuration`,
# MAGIC `delta.deletedFileRetentionDuration`, `delta.checkpointInterval`. |
# MAGIC | [ ] | **CHECK constraints for data quality** | Add `CHECK` constraints for
# MAGIC business rules (e.g. `CHECK (amount > 0)`, `CHECK (email LIKE '%@%')`). |
# MAGIC | [ ] | **Column comments present** | Every column should have `COMMENT`
# MAGIC for discoverability in Unity Catalog. |
# MAGIC | [ ] | **Generated columns for enrichment** | Use generated columns
# MAGIC (e.g. partition derivations, hash keys) to avoid repeated computation. |
# MAGIC | [ ] | **Partition evolution used if partitions change** | Delta supports
# MAGIC partition evolution — no need to rewrite the entire table. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Table Design — Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replace <catalog>.<schema>.<table> with your table name
# MAGIC -- Verify table format, properties and constraints
# MAGIC
# MAGIC DESCRIBE DETAIL <catalog>.<schema>.<table>;
# MAGIC
# MAGIC SHOW TBLPROPERTIES <catalog>.<schema>.<table>;
# MAGIC
# MAGIC SHOW CREATE TABLE <catalog>.<schema>.<table>;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Check CDF & Clustering Status (run after replacing table name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify CDF status
# MAGIC DESCRIBE HISTORY <catalog>.<schema>.<table> LIMIT 1;
# MAGIC
# MAGIC -- Check clustering columns
# MAGIC DESCRIBE DETAIL <catalog>.<schema>.<table>;
# MAGIC -- Look at 'clusteringColumns' field in the output

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Performance Checklist
# MAGIC
# MAGIC Performance tuning prevents runaway costs and keeps SLAs green. Every item
# MAGIC here targets a common bottleneck seen in production workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Configuration & Tuning
# MAGIC
# MAGIC | # | Check | Guidance |
# MAGIC |---|-------|----------|
# MAGIC | [ ] | **Target file size 128 MB – 1 GB** | Run `OPTIMIZE` to compact small
# MAGIC files. Files smaller than 64 MB cause excessive listing overhead on cloud
# MAGIC object stores. |
# MAGIC | [ ] | **`spark.sql.shuffle.partitions` = 2x–4x core count** | Default of
# MAGIC 200 is often wrong. For a cluster with 32 cores, set 64–128. Too many
# MAGIC partitions creates scheduling overhead; too few causes spills. |
# MAGIC | [ ] | **`spark.sql.autoBroadcastJoinThreshold` configured** | Default
# MAGIC 10 MB. Increase to 50–100 MB for clusters with ample memory. |
# MAGIC | [ ] | **Adaptive Query Execution (AQE) enabled** | Enabled by default in
# MAGIC DBR 10.4+. Verify `spark.sql.adaptive.enabled = true`. |
# MAGIC | [ ] | **`spark.sql.adaptive.coalescePartitions.enabled` true** | AQE
# MAGIC sub-setting; merges small post-shuffle partitions automatically. |
# MAGIC | [ ] | **`spark.sql.adaptive.skewJoin.enabled` true** | Handles data skew
# MAGIC by splitting oversized partitions at join time. |
# MAGIC | [ ] | **Predicate pushdown verified** | Use `EXPLAIN` on your top-5
# MAGIC queries and look for `PushedFilters` and partition pruning in the plan. |
# MAGIC | [ ] | **Column statistics up to date** | Run `ANALYZE TABLE ... COMPUTE
# MAGIC STATISTICS FOR ALL COLUMNS` weekly or after large data changes. |
# MAGIC | [ ] | **Photon enabled** | Photon accelerates most Spark SQL workloads
# MAGIC 2x–5x. Enable on all production warehouses and job clusters. |
# MAGIC | [ ] | **Caching used sparingly** | `CACHE TABLE` consumes executor storage.
# MAGIC Use only for repeated interactive queries, not for ETL jobs. |
# MAGIC | [ ] | **Bloom filters for high-cardinality join keys** | Add bloom filter
# MAGIC indices on columns that are frequently joined and have >10M distinct values. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Performance — Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Replace with your most critical query
# MAGIC -- Check the physical plan for PushedFilters, partition pruning, and
# MAGIC -- broadcast joins vs shuffle joins
# MAGIC
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT   *
# MAGIC FROM     <catalog>.<schema>.<table>
# MAGIC WHERE    event_date >= current_date() - INTERVAL 7 DAYS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Freshen column stats
# MAGIC ANALYZE TABLE <catalog>.<schema>.<table> COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file sizes and count
# MAGIC DESCRIBE DETAIL <catalog>.<schema>.<table>;
# MAGIC -- Focus on: numFiles, sizeInBytes. Aim for sizeInBytes/numFiles between
# MAGIC -- 128 MB and 1 GB per file.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm clustering is active
# MAGIC DESCRIBE DETAIL <catalog>.<schema>.<table>;
# MAGIC -- Look for clusteringColumns and clusteringProvider in the output
# MAGIC -- Empty list or absent means no clustering is enabled

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 OPTIMIZE with Liquid Clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC -- For liquid-clustered tables, run OPTIMIZE without ZORDER BY
# MAGIC OPTIMIZE <catalog>.<schema>.<table>;
# MAGIC
# MAGIC -- For legacy tables still using Z-ordering (should migrate to liquid)
# MAGIC -- OPTIMIZE <catalog>.<schema>.<table> ZORDER BY (col1, col2);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Security Checklist
# MAGIC
# MAGIC Security is not optional. A single leaked secret or misconfigured access
# MAGIC control can be a compliance violation. Every production pipeline must pass
# MAGIC these checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Access Control & Secrets
# MAGIC
# MAGIC | # | Check | Guidance |
# MAGIC |---|-------|----------|
# MAGIC | [ ] | **No hardcoded secrets** | All credentials, tokens, and connection
# MAGIC strings must reference Databricks secret scopes (`secrets('scope','key')`).
# MAGIC Search your codebase for `password=`, `api_key=`, `token=`. |
# MAGIC | [ ] | **Service principals for automated jobs** | Never use personal
# MAGIC user accounts for scheduled jobs or CI/CD pipelines. Service principals
# MAGIC survive personnel changes and have auditable, limited permissions. |
# MAGIC | [ ] | **Row filters for PII and sensitive data** | Apply row-level
# MAGIC security with `CREATE ROW FILTER` on tables containing PII, PHI, or
# MAGIC commercially sensitive records. |
# MAGIC | [ ] | **Column masks for fine-grained redaction** | Use `CREATE COLUMN
# MAGIC MASK` to mask PII columns (email, SSN, phone) based on group membership. |
# MAGIC | [ ] | **Dynamic views for role-based access** | Prefer dynamic views
# MAGIC over static copies. Use `CURRENT_USER()`, `IS_ACCOUNT_GROUP_MEMBER()` in
# MAGIC view definitions. |
# MAGIC | [ ] | **Audit logging enabled** | Unity Catalog captures all read/write
# MAGIC operations. Verify `information_schema.table_privileges` and
# MAGIC `system.access.audit` are queryable. |
# MAGIC | [ ] | **External location credentials scoped** | Storage credentials
# MAGIC should grant least-privilege access to specific buckets/paths only. |
# MAGIC | [ ] | **Catalog-level grants, not table-level** | Grant `USE CATALOG`
# MAGIC and `USE SCHEMA` plus specific table/view permissions rather than
# MAGIC managing per-table grants across hundreds of tables. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Security — Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check which principals have access to your table
# MAGIC SELECT   grantee, privilege_type
# MAGIC FROM     information_schema.table_privileges
# MAGIC WHERE    table_catalog  = '<catalog>'
# MAGIC   AND    table_schema   = '<schema>'
# MAGIC   AND    table_name     = '<table>'
# MAGIC ORDER BY grantee, privilege_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for column masks and row filters on your table
# MAGIC SELECT   catalog_name, schema_name, table_name, function_name, function_type
# MAGIC FROM     information_schema.table_functions
# MAGIC WHERE    function_type IN ('ROW FILTER', 'COLUMN MASK')
# MAGIC   AND    table_catalog = '<catalog>'
# MAGIC   AND    table_schema  = '<schema>'
# MAGIC   AND    table_name    = '<table>';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Secret Scope Check (Python)

# COMMAND ----------

# DBTITLE 1,Check Available Secret Scopes
scopes = spark.sql("SHOW SECRETS").collect()
if scopes:
    print(f"Found {len(scopes)} secret scope(s). Using scopes is required for production.")
    for s in scopes:
        print(f"  - {s.scopeName}")
else:
    print("WARNING: No secret scopes found. Secrets must not be hardcoded!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pipeline Checklist
# MAGIC
# MAGIC Production pipelines must be robust, idempotent, and observable. A pipeline
# MAGIC that silently drops bad records or produces different results on re-run will
# MAGIC erode trust in the data platform.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Reliability & Correctness
# MAGIC
# MAGIC | # | Check | Guidance |
# MAGIC |---|-------|----------|
# MAGIC | [ ] | **Idempotent — re-run produces identical results** | Use
# MAGIC `MERGE` with deterministic conditions, `INSERT OVERWRITE` with stable
# MAGIC partitions, or complete refresh patterns. Never use `INSERT INTO` without
# MAGIC deduplication logic. |
# MAGIC | [ ] | **Retry policies configured** | Set `maxRetries` >= 3 on all
# MAGIC job and task definitions. Use exponential backoff where supported. |
# MAGIC | [ ] | **Dead letter queue for malformed records** | Streaming pipelines
# MAGIC should route unparseable records to a DLQ table or cloud storage path for
# MAGIC later inspection. |
# MAGIC | [ ] | **Checkpoint directories stable and unique** | Each streaming
# MAGIC query must have its own checkpoint location. Use persistent cloud storage
# MAGIC (not `/tmp/` or ephemeral DBFS mounts). |
# MAGIC | [ ] | **Schema evolution strategy defined** | Auto-merge on write
# MAGIC (`mergeSchema = true`), or explicit `ALTER TABLE ADD COLUMN` before writes.
# MAGIC Document which approach your pipeline uses. |
# MAGIC | [ ] | **Monitoring alerts on failures** | Configure webhook notifications,
# MAGIC email alerts, or Databricks alerts for job failures, task failures, and
# MAGIC duration spikes. |
# MAGIC | [ ] | **Data freshness SLA monitored** | Add a `EXPECT` check or
# MAGIC dashboard query that alerts when source data is stale (e.g. no new records
# MAGIC for >2 hours). |
# MAGIC | [ ] | **Backfill procedure documented** | Document how to re-process a
# MAGIC date range. Include the exact commands so any engineer can backfill at
# MAGIC 3 AM without guesswork. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Idempotency Check Example

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Before running your pipeline, capture a checksum or row count
# MAGIC SELECT   COUNT(*) AS row_count_before,
# MAGIC          SUM(HASH(*)) AS hash_before
# MAGIC FROM     <catalog>.<schema>.<table>
# MAGIC WHERE    event_date = '2025-01-01';
# MAGIC
# MAGIC -- Run pipeline...
# MAGIC
# MAGIC -- After pipeline, verify results are identical
# MAGIC SELECT   COUNT(*) AS row_count_after,
# MAGIC          SUM(HASH(*)) AS hash_after
# MAGIC FROM     <catalog>.<schema>.<table>
# MAGIC WHERE    event_date = '2025-01-01';
# MAGIC -- hash_before must equal hash_after for idempotency

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Checkpoint Verification (Python)

# COMMAND ----------

# Verify checkpoint location is not ephemeral
# Serverless-compatible: checks the table metadata rather than inspecting DBFS

checkpoint_query = """
    SELECT   option_value
    FROM     information_schema.table_options
    WHERE    option_name = 'checkpointLocation'
      AND    table_catalog = '<catalog>'
      AND    table_schema  = '<schema>'
"""
# Note: streaming checkpoints are set per-query, not stored in table metadata.
# For streaming pipelines, manually verify:
#   - Checkpoint path is on cloud storage (s3://..., abfss://..., gs://...)
#   - NOT on /tmp/, /dbfs/tmp/, or any ephemeral location
#   - Each streaming query has a unique path

# For Delta tables, verify table history is maintained
history = spark.sql("DESCRIBE HISTORY <catalog>.<schema>.<table>")
print(f"Table has {history.count()} historical versions available for time travel.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cost Optimization Checklist
# MAGIC
# MAGIC Databricks costs are driven by three levers: compute type, uptime, and
# instance selection. Every item here yields measurable savings when applied
# consistently.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Compute & Resource Management
# MAGIC
# MAGIC | # | Check | Guidance |
# MAGIC |---|-------|----------|
# MAGIC | [ ] | **Serverless for bursty, job clusters for sustained** | Serverless
# MAGIC SQL warehouses for ad-hoc and BI. Job clusters for predictable ETL.
# MAGIC All-purpose clusters only for interactive development. |
# MAGIC | [ ] | **Auto-termination configured** | Dev/shared clusters: 15-30 min.
# MAGIC Job clusters: 5-10 min after job completes (or rely on job cluster
# MAGIC auto-termination). Never leave clusters running overnight. |
# MAGIC | [ ] | **Spot instances for non-critical workloads** | Spot/fallback
# MAGIC instances can reduce costs 50-70%. Use on-demand for driver node and
# MAGIC critical production jobs. |
# MAGIC | [ ] | **Cluster policies enforce limits** | Create cluster policies that
# MAGIC cap instance types, enforce tags, and disable expensive features for users
# MAGIC who don't need them. |
# MAGIC | [ ] | **Tags on all resources** | Tag clusters, jobs, SQL warehouses,
# MAGIC and pipelines with `team`, `project`, `cost_center`, and `environment`.
# MAGIC Tags flow through to cloud billing for chargeback. |
# MAGIC | [ ] | **Photon right-sized** | Photon doubles per-DBU cost. Verify your
# MAGIC workload benefits (vectorised I/O, aggregation-heavy). Disable it for
# MAGIC simple single-table scans. |
# MAGIC | [ ] | **Scaling limits set** | Set `max_workers` on clusters to prevent
# MAGIC autoscaling runaways. Review historical peak worker count and cap at 1.5x. |
# MAGIC | [ ] | **Idle cluster detection** | Set up alerts for clusters running
# MAGIC with <10% CPU utilisation for >1 hour. These are cost leaks. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Cost — Billing Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DBU consumption by job/cluster (past 7 days)
# MAGIC -- Requires access to system.billing schema
# MAGIC SELECT   usage_date,
# MAGIC          workspace_id,
# MAGIC          sku_name,
# MAGIC          usage_quantity,
# MAGIC          list_price
# MAGIC FROM     system.billing.usage
# MAGIC WHERE    usage_date >= current_date() - INTERVAL 7 DAYS
# MAGIC ORDER BY usage_date DESC, list_price DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cost by custom tag (requires tags to be in billing export)
# MAGIC SELECT   usage_metadata:tags:project    AS project,
# MAGIC          usage_metadata:tags:team       AS team,
# MAGIC          SUM(usage_quantity)            AS total_dbus,
# MAGIC          SUM(list_price)                AS total_cost
# MAGIC FROM     system.billing.usage
# MAGIC WHERE    usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY project, team
# MAGIC ORDER BY total_cost DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Cost-Saving Actions (Python)

# COMMAND ----------

# List clusters that are running and could be terminated (serverless-compatible)
# Note: In serverless, you may not have compute management APIs.
# Use this for interactive/workspace-level checks.

running_clusters = spark.sql("SHOW CLUSTERS").collect()
print(f"Total clusters visible: {len(running_clusters)}")
# Manually review any running all-purpose clusters without active notebooks
# and terminate idle ones to reduce cost

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Operational Checklist
# MAGIC
# MAGIC Operational maturity separates production systems from prototypes. A pipeline
# MAGIC without version control, CI/CD, and runbooks is a production incident waiting
# MAGIC to happen.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 DevOps & Documentation
# MAGIC
# MAGIC | # | Check | Guidance |
# MAGIC |---|-------|----------|
# MAGIC | [ ] | **Git integration / Databricks Repos enabled** | All notebooks,
# MAGIC Python modules, and SQL files must live in a Git repo, not in the
# MAGIC Workspace file browser. Repos provide version history, code review, and
# MAGIC rollback. |
# MAGIC | [ ] | **Branch-based development workflow** | Feature branches for
# MAGIC development, `staging`/`main` for promotion. Protect `main` with branch
# MAGIC policies requiring PR approval. |
# MAGIC | [ ] | **CI/CD with Asset Bundles** | Databricks Asset Bundles (DABs)
# MAGIC deploy jobs, pipelines, and schemas together. Every merge to `main`
# MAGIC triggers a deployment to production. |
# MAGIC | [ ] | **Documentation for each pipeline** | Every pipeline should have a
# MAGIC README or top-of-notebook docstring covering: purpose, input tables, output
# MAGIC tables, SLAs, failure behaviour, and on-call contact. |
# MAGIC | [ ] | **Runbook for common failures** | Document 3-5 common failure
# MAGIC scenarios with step-by-step recovery instructions. Store in the same repo
# MAGIC as the pipeline code. |
# MAGIC | [ ] | **Schema contracts documented** | Downstream consumers need to
# MAGIC know which columns are stable, which may change, and the semantics of
# MAGIC nullable vs non-nullable fields. |
# MAGIC | [ ] | **Environment parity** | Dev, staging, and production
# MAGIC environments should differ only in catalog/schema names and data volume,
# MAGIC not in configuration, libraries, or Spark versions. |
# MAGIC | [ ] | **Dependency graph understood** | Know the upstream and downstream
# MAGIC lineage of every pipeline. Unity Catalog lineage helps, but document the
# MAGIC SLOs for each dependency explicitly. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Git Integration Status (Python)

# COMMAND ----------

import subprocess
import sys

def check_git_in_repo():
    """Check if current notebook directory is in a Git repository."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--git-dir"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            branch = subprocess.run(
                ["git", "branch", "--show-current"],
                capture_output=True, text=True, timeout=10
            )
            remote = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                capture_output=True, text=True, timeout=10
            )
            print(f"Git repository: {remote.stdout.strip()}")
            print(f"Current branch: {branch.stdout.strip()}")
            return True
        else:
            print("WARNING: Not in a Git repository.")
            print("All production code must be version-controlled via Databricks Repos.")
            return False
    except Exception as e:
        print(f"Git check failed: {e}")
        return False

check_git_in_repo()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Unity Catalog Lineage Check

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table lineage relationships
# MAGIC SELECT   source_table_catalog,
# MAGIC          source_table_schema,
# MAGIC          source_table_name,
# MAGIC          target_table_catalog,
# MAGIC          target_table_schema,
# MAGIC          target_table_name
# MAGIC FROM     system.access.table_lineage
# MAGIC WHERE    target_table_catalog = '<catalog>'
# MAGIC   AND    target_table_schema  = '<schema>'
# MAGIC   AND    target_table_name    = '<table>';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pre-Production Verification Script
# MAGIC
# MAGIC Run this cell against any table before promoting to production. It performs
# MAGIC a comprehensive set of checks and prints a pass/fail report.
# MAGIC
# MAGIC **Usage:** Set `CATALOG`, `SCHEMA`, and `TABLE` in the widget or replace
# MAGIC the defaults below.

# COMMAND ----------

import time
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# =============================================================================
# CONFIGURATION — Replace with your target table
# =============================================================================
CATALOG = "main"
SCHEMA  = "default"
TABLE   = "your_table_name"

FQN = f"{CATALOG}.{SCHEMA}.{TABLE}"
print(f"Running production readiness checks on: {FQN}")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def run_sql(sql: str):
    """Execute SQL and return results as list of Row objects. Serverless-safe."""
    return spark.sql(sql).collect()

def try_sql(sql: str):
    """Execute SQL; return (results, None) on success or (None, error_msg) on failure."""
    try:
        return spark.sql(sql).collect(), None
    except Exception as e:
        return None, str(e)

def format_bytes(n_bytes):
    """Convert bytes to human-readable string."""
    if n_bytes is None:
        return "N/A"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n_bytes) < 1024.0:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024.0
    return f"{n_bytes:.1f} PB"

# =============================================================================
# REPORT COLLECTOR
# =============================================================================

report = []
PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
SKIP = "SKIP"

def check(category: str, name: str, passed: bool, detail: str = ""):
    status = PASS if passed else FAIL
    report.append((category, name, status, detail))
    icon = "[PASS]" if passed else "[FAIL]"
    print(f"  {icon} {name} — {detail}")

def check_warn(category: str, name: str, detail: str = ""):
    report.append((category, name, WARN, detail))
    print(f"  [WARN] {name} — {detail}")

# =============================================================================
# 1. TABLE EXISTENCE
# =============================================================================
print("\n" + "=" * 70)
print("1. TABLE EXISTENCE & ACCESS")
print("=" * 70)

result, err = try_sql(f"SELECT 1 FROM {FQN} LIMIT 1")
if err:
    check("Existence", "Table accessible", False, f"Table not found or no access: {err}")
    # Early exit — no point in continuing
    raise SystemExit(f"Cannot access {FQN}. Verify the table exists and you have SELECT permission.")
else:
    check("Existence", "Table accessible", True, f"Successfully queried {FQN}")

# =============================================================================
# 2. TABLE PROPERTIES
# =============================================================================
print("\n" + "=" * 70)
print("2. TABLE PROPERTIES")
print("=" * 70)

detail_rows = run_sql(f"DESCRIBE DETAIL {FQN}")
detail = detail_rows[0] if detail_rows else None

if detail:
    fmt = detail.format.lower()
    if fmt == "delta":
        check("Properties", "Table format", True, f"Delta Lake ({fmt})")
    else:
        check("Properties", "Table format", False, f"Expected 'delta', got '{fmt}'")

    table_type = detail.tableType.lower() if hasattr(detail, 'tableType') else "managed"
    check("Properties", "Table type", True, f"{table_type}")

    num_files = getattr(detail, 'numFiles', 0)
    size_bytes = getattr(detail, 'sizeInBytes', 0)
    avg_file_size = size_bytes / num_files if num_files > 0 else 0
    print(f"  [INFO] Total size: {format_bytes(size_bytes)} across {num_files} files")
    print(f"  [INFO] Average file size: {format_bytes(avg_file_size)}")

    if avg_file_size < 64 * 1024 * 1024 and num_files > 1:
        check_warn("Properties", "File size",
                   f"Avg file {format_bytes(avg_file_size)} < 64 MB. Run OPTIMIZE before production.")
    elif avg_file_size > 1024 * 1024 * 1024:
        check_warn("Properties", "File size",
                   f"Avg file {format_bytes(avg_file_size)} > 1 GB. Files may be too large for efficient scanning.")
    else:
        check("Properties", "File size", True, f"Avg {format_bytes(avg_file_size)} per file")

    # Clustering check
    clustering_cols = getattr(detail, 'clusteringColumns', None)
    if clustering_cols and len(clustering_cols) > 0:
        check("Properties", "Clustering", True, f"Liquid clustering on: {clustering_cols}")
    else:
        check_warn("Properties", "Clustering", "No liquid clustering keys defined. Add for large tables.")

    # Partitioning check
    partition_cols = getattr(detail, 'partitionColumns', None)
    if partition_cols and len(partition_cols) > 0:
        check_warn("Properties", "Partitioning",
                   f"Table uses legacy partitions: {partition_cols}. Consider migrating to liquid clustering.")

    # Location check
    location = getattr(detail, 'location', '')
    if location and ('/tmp/' in location or '/user/' in location):
        check_warn("Properties", "Storage location",
                   f"Location looks ephemeral: {location}. Use a dedicated cloud storage path.")
    else:
        check("Properties", "Storage location", True, f"{location}")

    # Table owner
    owner = getattr(detail, 'owner', None)
    if owner:
        check("Properties", "Table owner", True, f"Owned by {owner}")

# =============================================================================
# 3. TABLE PROPERTIES (retention, checkpoint)
# =============================================================================
print("\n" + "=" * 70)
print("3. RETENTION & MAINTENANCE PROPERTIES")
print("=" * 70)

props_result, props_err = try_sql(f"SHOW TBLPROPERTIES {FQN}")
if props_result:
    props_dict = {}
    for row in props_result:
        props_dict[row.key] = row.value

    log_retention = props_dict.get("delta.logRetentionDuration", "not set")
    deleted_retention = props_dict.get("delta.deletedFileRetentionDuration", "not set")
    checkpoint_interval = props_dict.get("delta.checkpointInterval", "not set")
    cdf_enabled = props_dict.get("delta.enableChangeDataFeed", "false")

    if log_retention != "not set":
        check("Retention", "Log retention", True, f"{log_retention}")
    else:
        check_warn("Retention", "Log retention", "delta.logRetentionDuration not explicitly set. Default: 30 days.")

    if deleted_retention != "not set":
        check("Retention", "Deleted file retention", True, f"{deleted_retention}")
    else:
        check_warn("Retention", "Deleted file retention",
                   "delta.deletedFileRetentionDuration not set. Default: 7 days.")

    if checkpoint_interval != "not set":
        check("Maintenance", "Checkpoint interval", True, f"Every {checkpoint_interval} commits")
    else:
        check_warn("Maintenance", "Checkpoint interval",
                   "delta.checkpointInterval not set. Default: 10. Consider reducing for large tables.")

    if cdf_enabled.lower() == "true":
        check("CDF", "Change Data Feed", True, "CDF enabled — downstream consumers can read incremental changes")
    else:
        check_warn("CDF", "Change Data Feed", "CDF disabled. Enable if downstream consumers need incremental reads.")
else:
    check("Retention", "Properties query", False, f"Cannot read table properties: {props_err}")

# =============================================================================
# 4. COLUMN STATISTICS
# =============================================================================
print("\n" + "=" * 70)
print("4. COLUMN STATISTICS")
print("=" * 70)

stats_result, stats_err = try_sql(
    f"DESCRIBE EXTENDED {FQN}"
)
if stats_result:
    # Try a test ANALYZE to see if we have permissions and it's fresh
    check("Statistics", "Statistics queryable", True, "DESCRIBE EXTENDED succeeded")
    check_warn("Statistics", "Freshness",
               "Manually verify: ANALYZE TABLE COMPUTE STATISTICS should be run weekly.")
else:
    check("Statistics", "Statistics queryable", False, str(stats_err))

# =============================================================================
# 5. CONSTRAINTS
# =============================================================================
print("\n" + "=" * 70)
print("5. CONSTRAINTS & DATA QUALITY")
print("=" * 70)

constraints_result, constraints_err = try_sql(f"SHOW CONSTRAINTS ON {FQN}")
if constraints_result and len(constraints_result) > 0:
    for c in constraints_result:
        check("Constraints", c.constraint_name, True, f"Type: {c.constraint_type}")
    check("Constraints", "Constraint count", True, f"Found {len(constraints_result)} constraint(s)")
elif constraints_result and len(constraints_result) == 0:
    check_warn("Constraints", "No constraints defined",
               "Add CHECK constraints and NOT NULL constraints for data quality enforcement.")
else:
    check_warn("Constraints", "Constraint check failed", str(constraints_err))

# =============================================================================
# 6. COLUMN METADATA
# =============================================================================
print("\n" + "=" * 70)
print("6. COLUMN METADATA")
print("=" * 70)

cols_result, cols_err = try_sql(f"DESCRIBE {FQN}")
if cols_result:
    total_cols = len(cols_result)
    commented_cols = 0
    for col in cols_result:
        # col_name, data_type, comment (optional)
        col_comment = getattr(col, 'comment', None)
        if col_comment and col_comment.strip():
            commented_cols += 1

    comment_pct = (commented_cols / total_cols * 100) if total_cols > 0 else 0
    if comment_pct >= 80:
        check("Columns", "Column comments", True, f"{commented_cols}/{total_cols} columns documented ({comment_pct:.0f}%)")
    else:
        check_warn("Columns", "Column comments",
                   f"Only {commented_cols}/{total_cols} columns have comments ({comment_pct:.0f}%). Add documentation.")
else:
    check("Columns", "Column describe", False, str(cols_err))

# =============================================================================
# 7. HISTORY & TIME TRAVEL
# =============================================================================
print("\n" + "=" * 70)
print("7. HISTORY & TIME TRAVEL")
print("=" * 70)

hist_result, hist_err = try_sql(f"DESCRIBE HISTORY {FQN} LIMIT 10")
if hist_result:
    version_count = len(hist_result)
    check("History", "Table history accessible", True, f"Last {version_count} versions available")

    # Check for VACUUM / OPTIMIZE operations
    recent_ops = []
    for row in hist_result[:5]:
        op = getattr(row, 'operation', 'UNKNOWN')
        recent_ops.append(op)

    if 'OPTIMIZE' in recent_ops:
        check("History", "Recent OPTIMIZE", True, "Table has been optimized recently")
    else:
        check_warn("History", "Recent OPTIMIZE", "No recent OPTIMIZE detected in last 5 versions. Schedule regular maintenance.")

    if 'VACUUM' in recent_ops:
        check_warn("History", "Recent VACUUM", "VACUUM detected. Ensure retention settings are adequate before vacuuming.")
else:
    check("History", "History accessible", False, str(hist_err))

# =============================================================================
# 8. ACCESS & PRIVILEGES
# =============================================================================
print("\n" + "=" * 70)
print("8. ACCESS & PRIVILEGES")
print("=" * 70)

priv_result, priv_err = try_sql(f"""
    SELECT grantee, privilege_type
    FROM information_schema.table_privileges
    WHERE table_catalog = '{CATALOG}'
      AND table_schema  = '{SCHEMA}'
      AND table_name    = '{TABLE}'
""")
if priv_result:
    grants = {}
    for row in priv_result:
        g = row.grantee
        p = row.privilege_type
        if g not in grants:
            grants[g] = []
        grants[g].append(p)
    check("Access", "Privileges verified", True, f"{len(grants)} principal(s) have explicit grants")
    for principal, privs in grants.items():
        print(f"  [INFO] {principal}: {', '.join(privs)}")
elif priv_err and "TABLE_OR_VIEW_NOT_FOUND" in priv_err.upper():
    check_warn("Access", "Privileges", "information_schema not available. Ensure Unity Catalog is enabled.")
else:
    check_warn("Access", "Privileges", "No explicit grants found or access denied. Verify permissions.")

# =============================================================================
# 9. ROW COUNT QUICK CHECK
# =============================================================================
print("\n" + "=" * 70)
print("9. DATA VOLUME")
print("=" * 70)

count_result, count_err = try_sql(f"SELECT COUNT(*) AS cnt FROM {FQN}")
if count_result:
    row_count = count_result[0].cnt
    if row_count == 0:
        check_warn("Volume", "Row count", "Table is empty! Is data ingestion working?")
    elif row_count < 1000:
        check_warn("Volume", "Row count", f"Only {row_count} rows. Ensure this is sufficient for production.")
    else:
        check("Volume", "Row count", True, f"{row_count:,} rows")
else:
    check("Volume", "Row count", False, str(count_err))

# =============================================================================
# SUMMARY REPORT
# =============================================================================
print("\n" + "=" * 70)
print("SUMMARY REPORT")
print("=" * 70)

total   = len(report)
passed  = sum(1 for r in report if r[2] == PASS)
failed  = sum(1 for r in report if r[2] == FAIL)
warned  = sum(1 for r in report if r[2] == WARN)

print(f"\n  Total checks: {total}")
print(f"  Passed:       {passed}")
print(f"  Failed:       {failed}")
print(f"  Warnings:     {warned}")
print(f"\n  Score:        {passed}/{total} ({passed*100//total}%)")

if failed > 0:
    print(f"\n  ACTION REQUIRED: Fix {failed} failing check(s) before production deployment.")
elif warned > 0:
    print(f"\n  RECOMMENDATION: Address {warned} warning(s) for optimal production readiness.")
else:
    print(f"\n  Table {FQN} is production-ready!")

print("\n" + "=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC | Status | Action |
# MAGIC |--------|--------|
# MAGIC | [ ] | Schedule this notebook to run weekly against all production tables |
# MAGIC | [ ] | Set up alerts for any check that falls below threshold |
# MAGIC | [ ] | Create a production-readiness dashboard using the reported metrics |
# MAGIC | [ ] | Integrate the verification script into your CI/CD pipeline |
# MAGIC | [ ] | Review the [Databricks Production Readiness Guide](https://docs.databricks.com/en/getting-started/production.html) |
# MAGIC
# MAGIC For issues or improvements, contact the data platform team.
