<p align="center">
  <img src="social-preview.png" alt="Databricks 100" width="700">
</p>

<p align="center">
  <a href="#quick-start"><img src="https://img.shields.io/badge/Start_Here-5_Minutes-00C853?style=for-the-badge"></a>
  <a href="#learning-roadmap"><img src="https://img.shields.io/badge/Roadmap-Beginner_%E2%86%92_Pro-FF6D00?style=for-the-badge"></a>
  <a href="#whats-inside"><img src="https://img.shields.io/badge/Content-100_Concepts-2962FF?style=for-the-badge"></a>
  <a href="https://github.com/kaushikthakur97/databricks-100-tutorial/blob/main/Databricks_100_Complete.ipynb"><img src="https://img.shields.io/badge/Notebook-1_Click_Run-FF5722?style=for-the-badge"></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Level-Beginner_to_Advanced-blue?style=flat-square">
  <img src="https://img.shields.io/badge/Notebooks-22_Total-green?style=flat-square">
  <img src="https://img.shields.io/badge/Concepts-100-orange?style=flat-square">
  <img src="https://img.shields.io/badge/Serverless-Ready-success?style=flat-square">
  <img src="https://img.shields.io/badge/Databricks-Free_Edition_+_Serverless-red?style=flat-square">
  <img src="https://img.shields.io/badge/License-MIT-yellow?style=flat-square">
</p>

---

#  Databricks 100: Zero to Pro

> The most comprehensive, hands-on Databricks tutorial on GitHub. **100 concepts. 10 tutorials. 4 projects. 4 bonus resources.** From `SELECT *` to production-grade lakehouse architecture — fully serverless-compatible.

```
   Beginner                   Intermediate                    Advanced
  (Month 1)                  (Month 2-3)                    (Month 4+)
      |                           |                              |
 [1-30] Easy  ------------> [31-70] Medium  ------------> [71-100] Hard
      |                           |                              |
  Delta Lake               Ingestion / Streaming        Architecture / CICD
  Spark SQL                Performance / Unity          Advanced Patterns
```

---

<a id="whats-inside"></a>
##  What's Inside

### 10 Notebooks | 100 Concepts | 900+ Interactive Cells

<table>
<tr>
  <td align="center" width="40"><b>1</b></td>
  <td width="200"><b> Delta Lake Fundamentals</b></td>
  <td width="80">#1-#10</td>
  <td width="60"><code>Easy</code></td>
  <td>ACID, Time Travel, Schema Evolution, Clustering, OPTIMIZE, VACUUM, CDF, Deletion Vectors</td>
</tr>
<tr>
  <td align="center"><b>2</b></td>
  <td><b> Spark Execution</b></td>
  <td>#11-#20</td>
  <td><code>Medium</code></td>
  <td>Lazy Eval, Catalyst, AQE, Shuffle, Joins, SparkUI, Data Skew, Memory</td>
</tr>
<tr>
  <td align="center"><b>3</b></td>
  <td><b> SQL & DataFrames</b></td>
  <td>#21-#30</td>
  <td><code>Medium</code></td>
  <td>Windows, Complex Types, Higher-Order Functions, UDFs, MERGE, CTEs, PIVOT, VARIANT</td>
</tr>
<tr>
  <td align="center"><b>4</b></td>
  <td><b> Data Ingestion</b></td>
  <td>#31-#40</td>
  <td><code>Medium</code></td>
  <td>COPY INTO, Auto Loader, Medallion Architecture, Incremental, SCD Type 1 & 2</td>
</tr>
<tr>
  <td align="center"><b>5</b></td>
  <td><b> Streaming</b></td>
  <td>#41-#50</td>
  <td><code>Medium</code></td>
  <td>Structured Streaming, Triggers, Windowed Aggregations, foreachBatch, Watermarks</td>
</tr>
<tr>
  <td align="center"><b>6</b></td>
  <td><b> Performance & Cost</b></td>
  <td>#51-#60</td>
  <td><code>Medium</code></td>
  <td>Predicate Pushdown, Column Stats, Write Optimization, Cluster Sizing, DBU Model</td>
</tr>
<tr>
  <td align="center"><b>7</b></td>
  <td><b> Unity Catalog & Governance</b></td>
  <td>#61-#70</td>
  <td><code>Medium</code></td>
  <td>Namespace, Permissions, Dynamic Views, Lineage, Row Filters, Delta Sharing</td>
</tr>
<tr>
  <td align="center"><b>8</b></td>
  <td><b> Lakeflow Pipelines</b></td>
  <td>#71-#80</td>
  <td><code>Hard</code></td>
  <td>Streaming Tables, Materialized Views, Expectations, CDC, Dead Letter, Multi-Pipeline</td>
</tr>
<tr>
  <td align="center"><b>9</b></td>
  <td><b> Workflows & CI/CD</b></td>
  <td>#81-#90</td>
  <td><code>Medium</code></td>
  <td>Jobs, Secrets, Widgets, Git Integration, Monitoring, System Tables, Asset Bundles</td>
</tr>
<tr>
  <td align="center"><b>10</b></td>
  <td><b> Architecture & Advanced</b></td>
  <td>#91-#100</td>
  <td><code>Hard</code></td>
  <td>Clones, Table Design, Delta UniForm, Multi-Workspace, Troubleshooting, Testing</td>
</tr>
</table>

---

##  What Problem Does This Solve?

> *"Databricks has 500+ features. Tutorials are either hello-world or assume you're a senior. Certification prep is scattered. I want ONE resource that takes me from zero to job-ready."*

| Pain Point | How This Tutorial Fixes It |
|------------|---------------------------|
|  "Where do I even start?" | **Progressive difficulty**: Easy (Month 1) -> Medium -> Hard -> Production |
|  "I learn by doing, not reading" | **Every concept** has copy-paste-ready executable code |
|  "I don't have a paid account" | Works on **free Databricks tier** (no credit card) + serverless compute |
|  "I need to pass the cert" | Covers **Associate** (~60 concepts) AND **Professional** (all 100) |
|  "I'm stuck on a concept" | **Self-assessment questions** after each concept — find your gaps |
|  "Tutorials don't explain WHY" | Each concept includes **Real-World Use Case** and **What Problem It Solves** |

---

<a id="quick-start"></a>
##  Quick Start (5 Minutes)

### Step 1: Free Databricks Account
Sign up at [databricks.com/try](https://databricks.com/try) for the **free tier** (no credit card). Also works on serverless compute.

### Step 2: Import the Complete Notebook

```
Databricks Workspace  >  Import
Paste this URL:
https://raw.githubusercontent.com/kaushikthakur97/databricks-100-tutorial/main/Databricks_100_Complete.ipynb
```

**Or** clone the entire repo with Databricks Repos:

```
Workspace  >  Add  >  Repo
URL: https://github.com/kaushikthakur97/databricks-100-tutorial
Branch: main (or master)
```

### Step 3: Run
```python
print("Welcome to Databricks 100! Let's begin.")
```

> **Databricks Repos auto-syncs with GitHub.** Every push you make updates in Databricks automatically.

---

<a id="learning-roadmap"></a>
##  Learning Roadmap

```
WEEK 1-2                WEEK 3-4                WEEK 5-6                WEEK 7-8
+-------------+        +-------------+        +-------------+        +-------------+
| 1 | 2 |     |        | 3 | 4 |     |        | 5 | 6 |     |        | 7 | 8 |     |
|Delta|Spark |        | SQL| Data |        |Stream| Perf|        |Unity|Lake-|
|Lake |Exec  |        | DF |Ingest|        |     |Cost |        |Cat. |flow |
+------+------+        +------+------+        +------+------+        +------+------+
   FOUNDATIONS           DAILY WORK            REAL-TIME             GOVERNANCE

WEEK 9-10               WEEK 11-12+
+-------------+        +-------------+
| 9 | 10|     |        | YOU'RE A    |
|CICD|Arch|    |        | PRO!  |  |  |
|Ops |Patt|    |        |       |  |  |
+------+------+        +-------------+
  PRODUCTION              SHIP IT!
```

### Difficulty Breakdown

```
  Easy (30)          Medium (40)          Hard (30)
  Month 1            Month 2-3            Month 4+
|||||||||||       ||||||||||||||       |||||||||||
concepts 1-30     concepts 31-70       concepts 71-100
```

| Level | What It Means | Your Goal |
|-------|--------------|-----------|
| **Easy** ` ` | Know after Month 1 | Master all 30 first |
| **Medium** ` ` | Solid mid-level engineer | Understand daily production work |
| **Hard** `  ` | Senior-level depth | Architect and lead projects |

---

##  How Each Concept Is Taught

Every single concept follows this proven 5-step structure:

```
+-------------------------------------------------------+
|  1. WHAT PROBLEM IT SOLVES                             |
|  "Before Delta Lake, data corruption was common..."    |
+-------------------------------------------------------+
|  2. REAL-WORLD USE CASE                                |
|  "An e-commerce company needs inventory updates..."    |
+-------------------------------------------------------+
|  3. HANDS-ON CODE (copy-paste ready)                   |
|  df.write.format("delta").save("/path")               |
+-------------------------------------------------------+
|  4. KEY TAKEAWAYS (2-3 bullets)                        |
|  * Delta provides ACID guarantees on cloud storage     |
|  * Time travel enables audit trails and rollbacks      |
+-------------------------------------------------------+
|  5. SELF-ASSESSMENT QUESTION                           |
|  "Can you explain optimistic concurrency control?"     |
+-------------------------------------------------------+
```

---

##  Real-World Datasets You'll Work With

| Dataset | What You'll Learn | Industry Parallel |
|---------|-------------------|-------------------|
|   **NYC Taxi Trips** | Time-series, window functions, aggregations | Ride-sharing (Uber, Lyft) |
|   **Retail Sales** | MERGE, SCD Type 2, medallion architecture | E-commerce (Amazon, Shopify) |
|   **Customer Profiles** | Joins, enrichment, PII masking | CRM/CDP platforms |
|   **IoT Sensor Data** | Streaming, watermarks, windowed aggregation | Manufacturing / Smart Devices |
|   **Bank Transactions** | ACID, idempotency, exactly-once | Financial Services / Fraud Detection |

> **Zero setup.** All data is generated inside the notebooks or uses built-in Databricks samples.

---

##  Self-Assessment: Find Your Level

```
   0-30                30-49               50-79               80-100
+---------+         +---------+         +---------+         +---------+
|         |         |         |         |         |         |         |
|  EARLY  |  ---->  |FOUNDATION| ---->  |MID-LEVEL| ---->  | SENIOR  |
|  STAGE  |         |          |         |         |         |         |
+---------+         +---------+         +---------+         +---------+
 Start with         Focus on          Fill Medium        Polish Hard
 Easy concepts      cats 1, 3, 4      gaps               concepts
```

| Score | Level | What To Do |
|-------|-------|------------|
| **80-100** | Senior | You're ready for architecture roles. Polish Hard concepts. |
| **50-79** | Mid-Level | You handle daily work. Fill Medium/Hard gaps. |
| **30-49** | Foundation | Focus on Easy concepts in categories 1, 3, and 4. |
| **0-30** | Early Stage | Start with all Easy concepts. Re-score in 3 months. |

---

##  100-Day Challenge

```
Day 1  : ACID Transactions           Day 51 : Serverless Economics
Day 2  : Transaction Log             ...
...                                   Day 97 : AI Functions
Day 30 : Table Constraints           Day 98 : Multi-Workspace Architecture
...                                   Day 99 : Performance Troubleshooting
Day 50 : Stream-Stream Joins         Day 100: Testing Patterns 
```

**One concept per day. Post on LinkedIn with `#100DaysOfDatabricks`.**

```
1. Star & Fork this repo
2. Pick today's concept number
3. Open the corresponding notebook
4. Read, run, understand
5. Post your takeaway on LinkedIn
6. Check it off 
7. Repeat. 100 days. You'll be a Databricks pro.
```

---

##  File Formats Available

| Format | Location | Best For |
|--------|----------|----------|
|   Combined IPYNB | `Databricks_100_Complete.ipynb` | **One-click import of all 100 concepts** |
|   Concept Notebooks | `notebooks/01-10_*.py` | Learn one topic at a time |
|   End-to-End Projects | `projects/01-04_*.py` | Practice with real-world scenarios |
|   Bonus Resources | `resources/*.py` | Cheat sheet, interview Q&A, troubleshooting |
|   Markdown Docs | `docs/01-10_*.md` | Read offline, phone, tablet |
|   Features Guide | `docs/FEATURES_AND_PROBLEMS.md` | Every feature mapped to business problems |
|   Clone Entire Repo | This repository | Contribute, customize, full access |

---

##  Certification Roadmap

This repo is designed to take you from **zero to certified**. Every concept maps to exam objectives.

| Certification | Concepts Covered | Difficulty | Study Path |
|--------------|:----------------:|-----------|-------------|
| **Databricks Associate DE** | ~60 / 100 | Easy + Medium | Notebooks 1-4, 7, 9 + Cheat Sheet + Interview Q&A |
| **Databricks Professional DE** | 100 / 100 | Production depth | All 10 notebooks + All 4 projects + Production Checklist |
| **Apache Spark Developer** | #11 - #30 | Core internals | Notebooks 2-3 + Interview Q&A |

### Study Plan (12 Weeks)

```
Week 1-2:  Notebooks 1-2 (Delta Lake + Spark Execution) — FOUNDATIONS
Week 3-4:  Notebooks 3-4 (SQL/DFs + Ingestion) — DAILY WORK
Week 5:    Notebook 5 (Streaming) — REAL-TIME
Week 6:    Notebook 6 (Performance) — OPTIMIZATION
Week 7-8:  Notebooks 7-8 (Unity Catalog + Lakeflow) — GOVERNANCE
Week 9:    Notebook 9 (Workflows/CI/CD) — PRODUCTION
Week 10:   Notebook 10 (Architecture) — ADVANCED
Week 11:   Projects 1-4 (End-to-End) — HANDS-ON
Week 12:   Interview Q&A + Cheat Sheet + Mock Exams — REVIEW
```

> See [docs/FEATURES_AND_PROBLEMS.md](docs/FEATURES_AND_PROBLEMS.md) for a complete mapping of every Databricks feature to the business problems it solves.

---

##  Prerequisites

```
 Essential:                    Nice to Have:
   Python basics                Prior Spark exposure
   SQL fundamentals              Cloud concepts (AWS/Azure/GCP)
   A web browser                 Data engineering experience
   Curiosity and patience        A free Databricks account
```

---

##  Free Tier & Serverless Compatibility

All 22 notebooks are **serverless-compatible** using managed Delta tables. No DBFS paths, no `/tmp/` dependencies.

| Feature | Availability | How The Tutorial Handles It |
|---------|:-----------:|----------------------------|
| Delta Lake |  Free + Serverless | Fully executable with `saveAsTable` |
| Spark SQL & DataFrames |  Free + Serverless | Fully executable |
| Structured Streaming |  Free + Serverless | Executable (checkpoints need cloud storage on serverless) |
| Unity Catalog |  Full Platform | Concepts explained; managed tables in `default` |
| Photon Engine |  Serverless | Explained + performance comparisons |
| Serverless Compute |  Full Platform | Architecture & economics explained |
| Lakeflow Pipelines (DLT) |  Full Platform | Manual equivalents + syntax reference |
| Databricks Workflows |  Full Platform | Notebook-based alternatives + patterns |
| Predictive Optimization |  Full Platform | Manual OPTIMIZE/VACUUM patterns shown |

---

##  End-to-End Projects

Put concepts into practice with 4 real-world projects:

| # | Project | Concepts Applied | What You'll Build |
|---|---------|-----------------|-------------------|
|   | **E-Commerce Lakehouse** | Medallion, SCD2, CDF, MERGE, Window Functions | End-to-end Bronze→Silver→Gold pipeline |
|   | **IoT Streaming Pipeline** | Structured Streaming, Watermarks, Windowed Agg | Real-time anomaly detection from 10K sensors |
|   | **Fraud Detection** | Window Functions, Joins, AQE, Performance Tuning | 1M+ transaction fraud detection at scale |
|   | **Customer 360** | Unity Catalog, Dynamic Views, Lineage, Audit | Governed multi-source customer data platform |

---

##  Bonus Resources

| Resource | What It Is | Perfect For |
|----------|-----------|-------------|
|   **Cheat Sheet** | All commands, configs, syntax in one place | Quick reference, interviews |
|   **Interview Q&A** | 50 curated questions with detailed answers | Certification & interview prep |
|   **Troubleshooting Guide** | Common errors + diagnostic patterns | Debugging production issues |
|   **Production Checklist** | 7 checklists with verification SQL | Go-live readiness |

---

##  Repo Structure

```
databricks-100-tutorial/
|
|-- README.md                           <-- You are here
|-- LICENSE                             <-- MIT License
|-- .gitignore
|-- social-preview.png                  <-- Social share image
|-- Databricks_100_Complete.ipynb       <-- ONE-CLICK: All 100 concepts
|
|-- notebooks/                          <-- 10 individual Databricks notebooks (.py)
|   |-- 01_Delta_Lake_Fundamentals.py
|   |-- ...                             (02-09)
|   `-- 10_Architecture_Advanced_Patterns.py
|
|-- projects/                           <-- 4 end-to-end project notebooks
|   |-- 01_ecommerce_lakehouse.py
|   |-- 02_iot_streaming.py
|   |-- 03_fraud_detection.py
|   `-- 04_customer_360.py
|
|-- resources/                          <-- Cheat sheets, Q&A, guides
|   |-- cheat_sheet.py
|   |-- interview_questions.py
|   |-- troubleshooting.py
|   `-- production_checklist.py
|
|-- docs/                               <-- Reference documentation
|   |-- 01-10_*.md                     <-- Offline-readable markdown files
|   `-- FEATURES_AND_PROBLEMS.md       <-- 187 features → business problems guide
|
`-- scripts/                            <-- Build utilities (gitignored)
    |-- build_ipynb.py
    `-- build_md.py
```

---

##  Who Is This For?

```
   Aspiring Data Engineers          Data Analysts
   "I want to break into            "I know SQL, ready for
    data engineering"                big data engineering"

   Software Engineers               Students
   "Adding Databricks to            "Need a capstone project
    my backend toolkit"              for my degree"

   Interview Preppers               Engineering Managers
   "Databricks interview            "Onboard junior engineers
    in 2 weeks"                      in days, not months"
```

---

##  Databricks Repos: Auto-Sync

**Yes, Databricks Repos automatically syncs with GitHub.**

1. In your Databricks workspace: **Workspace > Add > Repo**
2. Enter: `https://github.com/kaushikthakur97/databricks-100-tutorial`
3. Every `git push` to this repo will appear in your workspace
4. Pull the latest with one click in the Databricks UI
5. Work on branches, create PRs — full Git workflow inside Databricks

> **You don't need to re-import notebooks.** The repo stays synced. Edit in Databricks, commit from Databricks, or push from your local machine — it all stays in sync.

---

##  FAQ

<details>
<summary><b>Q: Can I really learn Databricks 100% free?</b></summary>
Yes. The Databricks free tier requires no credit card. All 100 concepts + 4 projects run on it. Full-platform features are explained with managed table alternatives.
</details>

<details>
<summary><b>Q: I'm a total beginner. Where do I start?</b></summary>
Notebook 1 (Delta Lake Fundamentals). It assumes zero Databricks knowledge. Start with Easy concepts (marked with `Easy`). Skip Hard on first pass — come back later.
</details>

<details>
<summary><b>Q: How long does it take?</b></summary>
<ul>
<li><b>Intensive:</b> 2 weeks full-time</li>
<li><b>Part-time:</b> 2-3 months (1 notebook/week)</li>
<li><b>100-day challenge:</b> 1 concept/day = ~3 months</li>
</ul>
</details>

<details>
<summary><b>Q: Will this help me pass the certification?</b></summary>
Yes. The Associate cert covers ~60 of these 100 concepts. The Professional cert covers all 100 at production depth. Most concepts include self-assessment questions similar to interview/cert scenarios.
</details>

<details>
<summary><b>Q: How do I contribute or report an issue?</b></summary>
Open an issue or PR. Found a bug? Better example? Outdated info? We welcome contributions. See <a href="#contributing">contributing guidelines</a>.
</details>

---

##  Resources

| Resource | Description | Link |
|----------|------------|------|
|   Features & Problems Guide | Every Databricks feature mapped to the problem it solves | [docs/FEATURES_AND_PROBLEMS.md](docs/FEATURES_AND_PROBLEMS.md) |
|   Databricks Academy | Official free courses | [academy.databricks.com](https://academy.databricks.com) |
|   Databricks Docs | Official reference documentation | [docs.databricks.com](https://docs.databricks.com) |
|   Data Engineer Wiki | Cheat sheets, learning paths | [dataengineer.wiki](https://dataengineer.wiki) |
|   DataDojo | 633+ practice exercises | [dojo.dataengineer.wiki](https://dojo.dataengineer.wiki) |
|   Interview Cheat Sheet | 86 senior-level Q&A | [dataengineer.wiki/cheat-sheet-senior](https://dataengineer.wiki/cheat-sheet-senior) |

---

##  Contributing

```
1. Fork   2. Fix   3. PR
```

**Inclusion criteria:** Concepts that 80%+ of Databricks engineers encounter monthly in production. Niche edge cases don't make the list.

---

##  License

MIT License — Use freely. Learn. Teach. Share.

---

<p align="center">
  <b> Start Your Journey Now</b>
</p>

<p align="center">
  <a href="https://github.com/kaushikthakur97/databricks-100-tutorial/blob/main/Databricks_100_Complete.ipynb">
    <img src="https://img.shields.io/badge/Open_the_Complete_Notebook-FF5722?style=for-the-badge&logo=jupyter&logoColor=white">
  </a>
</p>

<p align="center">
  <sub>Independent educational resource. Not affiliated with Databricks, Inc.</sub><br>
  <sub>Built with  by the data engineering community &bull; Inspired by <a href="https://github.com/kaushikthakur97/databricks-100">databricks-100</a></sub>
</p>
