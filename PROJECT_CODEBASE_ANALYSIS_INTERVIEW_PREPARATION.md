# PROJECT CODEBASE ANALYSIS — INTERVIEW PREPARATION

> **Analysis Scope**: Comprehensive reverse-engineered technical breakdown of the entire repository codebase for technical interview preparation, system design defense, and Data Engineering / Architecture interviews.
> **Repository Name**: `END-TO-END-CRICKET-DATA-ENGINEERING`
> **Analysis Date**: Current Codebase Audit

---

## IMPLEMENTATION STATUS LEGEND

* **`[IMPLEMENTED]`**: Verified directly in source code with active execution paths, schemas, tests, and configurations.
* **`[PARTIALLY IMPLEMENTED]`**: Functional in code with fallback mechanisms or mock simulation modes (e.g., PySpark Lakehouse fallback when winutils/JVM cluster is absent, Mock Kafka queue fallback when live broker is unreachable).
* **`[DOCUMENTED BUT NOT VERIFIED IN IMPLEMENTATION]`**: Referenced in documentation, ideas, or guides, but lacks active runtime code in the repository.
* **`[NOT FOUND / CANNOT VERIFY FROM REPOSITORY]`**: Not implemented or absent in this codebase (e.g., Deep Learning, Computer Vision, GenAI/LLM/RAG).

---

# PART 1 — PROJECT IDENTITY

* **Project Name**: End-to-End IPL Cricket Data Engineering & Process Mining Platform
* **Project Purpose**: To ingest, stream, validate, store, transform, serve, and monitor ball-by-ball Indian Premier League (IPL) cricket match data across all historical tournament seasons (2008–2024), while capturing real-time Business Process Management (BPM) telemetry to detect process bottlenecks, SLA breaches, and execution anomalies.
* **Problem Being Solved**: Raw cricket match data from external providers (such as Cricsheet) arrives as unstructured/semi-structured nested JSON documents containing data anomalies (out-of-bounds runs, historical franchise name variations, illegal over counts, missing squads). Direct querying or loading without validation causes data corruption, duplicate records, non-deterministic analytics, and pipeline crashes. Furthermore, standard batch data pipelines lack operational process observability, making it difficult to detect workflow bottlenecks, task latency spikes, or illegal stage skips in real-time.
* **Target Users**: Data Engineers, Analytics Engineers, Sports Analysts, Business Intelligence (BI) Analysts, and Data Platform Operations (DataOps) Teams.
* **Main Use Case**: 
  1. Automated, idempotent ingestion and processing of multi-season ball-by-ball IPL match files from raw Cricsheet archives into a 3NF normalized Silver SQL schema and an OLAP Star Schema Gold serving layer.
  2. ACID Lakehouse storage via Apache Iceberg, Project Nessie REST catalog, and MinIO S3 object storage with checkpoint-based stream recovery.
  3. Real-time Business Process Management (BPM) process mining and SLA anomaly detection across streaming micro-batches.
  4. Interactive web portal for data exploration (EDA), ad-hoc pipeline execution, failure simulation & replay, database switching (Supabase Cloud vs. Local PostgreSQL), and live performance monitoring.
* **Project Type**: 
  * **Data Engineering** (Primary)
  * **Data Analytics & Business Intelligence (BI)**
  * **Process Mining & Business Process Management (BPM)**
  * **Stream & Lakehouse Architecture**
* **Current Implementation Status**: **Fully Implemented & Operational** with dual execution paths (Airflow orchestrator and Flask async thread runner), resilience fallbacks (Mock Kafka queue and Standalone Parquet Lakehouse writer), dual-database support (Cloud Supabase and Local PostgreSQL), and a comprehensive automated unit/integration test suite.
* **Main Technologies**: Python 3.11, Apache Airflow 2.9.3, Apache Kafka 7.3.0 & Zookeeper, Apache Iceberg, Project Nessie, MinIO S3 Object Storage, PySpark 3.5.0, PostgreSQL 13+ (Local & Supabase Cloud Pooler), Flask 3.0, PyArrow/Pandas, Docker & Docker Compose, GitHub Actions CI/CD.
* **Architecture Style**: **Medallion Architecture (Bronze $\rightarrow$ Staging $\rightarrow$ Silver $\rightarrow$ Gold / Lakehouse)** integrated with an **Event-Driven Messaging Layer (Kafka)**, **ACID Relational / Dimensional Star Schema (PostgreSQL)**, **Open Lakehouse (Iceberg + Nessie + MinIO)**, and an **Embedded Process Mining Observability Engine (BPM)**.

### Concise System Summary
The platform downloads raw ball-by-ball IPL match data (Cricsheet JSON archives), verifies source integrity via SHA-256 checksums in a `source_registry`, and publishes messages through an Apache Kafka streaming broker. Kafka consumers stage data into partitioned JSON and columnar Parquet formats. An 11-rule automated Data Quality Engine isolates anomalous records into a quarantine sink, while a Change Data Capture (CDC) hash-comparison engine identifies `INSERT`, `UPDATE`, `DELETE`, or `SKIP` operations. Validated data is atomically loaded into a 3NF normalized PostgreSQL Silver schema and aggregated into a Gold Star Schema (`FACT_MATCH_SUMMARY`, `DIM_PLAYER`, `DIM_TEAM`) inside a single database transaction with rollback guarantees. Concurrently, data is streamed into an Apache Iceberg Lakehouse partitioned by season backed by Project Nessie and MinIO S3, while every pipeline stage emits structured BPM process telemetry to detect SLA breaches, process deviations, and workflow bottlenecks.

---

# PART 2 — COMPLETE TECHNOLOGY STACK

| Category | Technology | Where Used | Why Used | Verified From | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Programming Language** | Python 3.11+ | Entire codebase (`pipeline/`, `bpm/`, `routes/`, `dags/`, `app.py`) | Core language for data engineering, streaming logic, REST APIs, and test suites. | [`requirements.txt`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/requirements.txt), [`app.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/app.py) | `[IMPLEMENTED]` |
| **Workflow Orchestration** | Apache Airflow 2.9.3 | [`dags/ipl_pipeline_dag.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/dags/ipl_pipeline_dag.py), [`docker-compose.yaml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/docker-compose.yaml) | Schedules and monitors sequential DAG execution, task retries, XCom state passing, and backfills. | [`dags/ipl_pipeline_dag.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/dags/ipl_pipeline_dag.py#L243-L310) | `[IMPLEMENTED]` |
| **Message Streaming** | Apache Kafka 7.3.0 & Confluent Kafka | [`pipeline/kafka_client.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/kafka_client.py), [`docker-compose.yaml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/docker-compose.yaml) | Decouples raw source ingestion from staging/validation; provides buffered message transport on topic `ipl_matches`. | [`pipeline/kafka_client.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/kafka_client.py#L157-L348) | `[IMPLEMENTED]` |
| **Message Fallback** | Mock File-based Queue | [`pipeline/kafka_client.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/kafka_client.py) (`MockProducer`, `MockConsumer`) | Provides offline development and resilient testing fallback when a live Kafka broker is unreachable. | [`pipeline/kafka_client.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/kafka_client.py#L26-L122) | `[IMPLEMENTED]` |
| **Relational Database / OLAP** | PostgreSQL 13+ (Local & Supabase Cloud) | [`database/schema.sql`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/schema.sql), [`database/connection.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/connection.py) | Stores pipeline control metadata, Silver 3NF normalized tables, Gold dimensional Star Schema, and BPM telemetry. | [`database/schema.sql`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/schema.sql#L33-L334) | `[IMPLEMENTED]` |
| **DB Driver & Connection Pool** | `psycopg2-binary` (RealDictCursor) | [`database/connection.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/connection.py) | Low-level Python-PostgreSQL adapter with context managers for automatic commit/rollback and dictionary result mapping. | [`database/connection.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/connection.py#L7-L84) | `[IMPLEMENTED]` |
| **Open Table Format (Lakehouse)** | Apache Iceberg | [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py), [`pipeline/verify_lakehouse.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/verify_lakehouse.py) | High-performance open table format for large analytic datasets partitioned by `season` with ACID time-travel metadata. | [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py#L218-L270) | `[IMPLEMENTED]` |
| **Lakehouse Catalog** | Project Nessie | [`docker-compose.yaml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/docker-compose.yaml), [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py) | Git-like transactional catalog for Iceberg tables over REST API v2 (`http://localhost:19120/api/v2`). | [`docker-compose.yaml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/docker-compose.yaml#L129-L139) | `[IMPLEMENTED]` |
| **Object Storage** | MinIO (S3-Compatible) | [`docker-compose.yaml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/docker-compose.yaml), [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py) | S3-compliant object storage server hosting Iceberg data files and metadata in the `warehouse` bucket. | [`docker-compose.yaml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/docker-compose.yaml#L95-L126) | `[IMPLEMENTED]` |
| **Distributed Compute / Stream** | PySpark 3.5.0 Structured Streaming | [`bpm/process_stream.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/process_stream.py), [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py) | Windowed micro-batch streaming analytics, Iceberg DataFrame writes, schema enforcement, and SLA breach flagging. | [`bpm/process_stream.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/process_stream.py#L44-L110) | `[IMPLEMENTED]` |
| **Columnar Data Storage** | Apache Parquet & PyArrow | [`pipeline/staging.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/staging.py), [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py) | High-compression columnar storage for staged matches and partitioned Lakehouse storage files. | [`pipeline/kafka_client.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/kafka_client.py#L322-L332) | `[IMPLEMENTED]` |
| **Data Processing & Analytics** | Pandas 2.1+ | [`pipeline/eda.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/eda.py), [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py) | In-memory data manipulation, EDA KPI computation, and structured record transformation. | [`pipeline/eda.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/eda.py#L5-L160) | `[IMPLEMENTED]` |
| **Web Framework & API** | Flask 3.0+ | [`app.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/app.py), [`routes/`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/routes) (`home.py`, `pipeline.py`, `analytics.py`, `bpm.py`, `api.py`) | Provides the web application server, Blueprints, REST APIs for monitoring, DB switching, and triggering pipeline runs. | [`app.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/app.py#L5-L39) | `[IMPLEMENTED]` |
| **Frontend UI / Templating** | Jinja2, HTML5, Vanilla CSS, Chart.js | [`templates/`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/templates), [`static/`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/static) | Dark-mode glassmorphic user interface for real-time pipeline monitoring, data inspection, and analytics dashboards. | [`templates/bpm.html`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/templates/bpm.html), [`templates/pipeline.html`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/templates/pipeline.html) | `[IMPLEMENTED]` |
| **Containerization** | Docker & Docker Compose | [`docker-compose.yaml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/docker-compose.yaml) | Orchestrates 7 services: Postgres, Zookeeper, Kafka, Airflow Webserver, Airflow Scheduler, MinIO, MinIO-Init, and Nessie. | [`docker-compose.yaml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/docker-compose.yaml#L1-L144) | `[IMPLEMENTED]` |
| **Automated Testing** | Pytest 8.0+ / Unittest | [`tests/`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests) (`test_pipeline.py`, `test_bpm.py`, `test_iceberg_lakehouse.py`) | Verifies transformation correctness, CDC hashing, data validation, mock streaming, Iceberg schema, and BPM anomalies. | [`tests/test_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py) | `[IMPLEMENTED]` |
| **CI/CD Automation** | GitHub Actions | [`.github/workflows/ci.yml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/.github/workflows/ci.yml) | Runs linting (flake8), boots PostgreSQL test container, validates DDL schema, and runs unit/integration tests on push/PR. | [`.github/workflows/ci.yml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/.github/workflows/ci.yml) | `[IMPLEMENTED]` |
| **BI Tooling** | Microsoft Power BI (`.pbix`) | [`DOCument and reports/Live IPL Analytics.pbix`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/DOCument%20and%20reports/Live%20IPL%20Analytics.pbix) | Executive BI dashboard connecting to PostgreSQL Gold Star Schema for cricket match intelligence. | [`DOCument and reports/Live IPL Analytics.pbix`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/DOCument%20and%20reports/Live%20IPL%20Analytics.pbix) | `[IMPLEMENTED]` |

---

# PART 3 — PROJECT ARCHITECTURE

## Architectural Pipeline Stages
**Input $\rightarrow$ Ingestion $\rightarrow$ Streaming $\rightarrow$ Staging $\rightarrow$ Quality Gate $\rightarrow$ CDC Delta Detection $\rightarrow$ Atomic Storage $\rightarrow$ Lakehouse Sinking $\rightarrow$ Process Telemetry $\rightarrow$ Serving & Analytics**

1. **Input**: Raw Cricsheet IPL JSON ZIP archive (`https://cricsheet.org/downloads/ipl_json.zip`).
2. **Ingestion (Bronze)**: Downloaded to `data/bronze/source/ipl_json.zip`, SHA-256 verified, and audited in `source_registry`.
3. **Streaming & Buffering (Kafka)**: The `run_producer` reads JSON files from the archive and publishes messages to the `ipl_matches` topic. The `run_consumer` reads the topic and partitions matches into `data/staging/{season}/json/` and `data/staging/{season}/parquet/`.
4. **Data Quality & Quarantine**: An 11-rule validation engine (`validate_season`) filters out invalid matches, storing anomalies in `data/quarantine/{season}/` and logging errors in `quarantine_records`.
5. **Change Data Capture (CDC)**: Computes file SHA-256 checksums, comparing against `cdc_states`. Generates delta operations (`INSERT`, `UPDATE`, `DELETE`, `SKIP`) and records audit history in `cdc_log`.
6. **Relational Storage (Silver)**: Normalizes match metadata, innings, deliveries, dismissals, fielders, players, teams, and officials into 3NF SQL tables.
7. **Serving Layer (Gold)**: Aggregates match facts (`FACT_MATCH_SUMMARY`) and career dimensions (`DIM_TEAM`, `DIM_PLAYER`) inside the same ACID PostgreSQL transaction (`load_season_atomic`).
8. **Lakehouse Sinking (Iceberg + Nessie + MinIO)**: Ingests validated records into partitioned Iceberg table `ipl.ipl_matches` with S3 storage on MinIO and REST cataloging on Nessie.
9. **Process Mining (BPM Telemetry)**: Every task emits lifecycle events to `bpm_process_events` to compute Cycle Time, Lead Time, Throughput, Bottlenecks, and SLA breaches.
10. **Output & Serving**: Exposes REST endpoints via Flask and serves interactive web dashboards and Power BI reports.

## Text-Based Architecture Diagram

```text
                                  +---------------------------------------+
                                  |         External Data Source          |
                                  |    (https://cricsheet.org/ IPL ZIP)   |
                                  +---------------------------------------+
                                                      |
                                                      v
                                  +---------------------------------------+
                                  |     BRONZE LAYER (Ingestion)          |
                                  | - Download & Verify SHA-256 Checksum  |
                                  | - Log in [source_registry] SQL Table  |
                                  +---------------------------------------+
                                                      |
                                                      v
                                  +---------------------------------------+
                                  |     STREAMING & MESSAGING LAYER       |
                                  |  - Kafka Producer (Topic: ipl_matches)|
                                  |  - Kafka Consumer (Season Partition)  |
                                  |  - Resilient Mock Queue Fallback      |
                                  +---------------------------------------+
                                                      |
                                                      v
                                  +---------------------------------------+
                                  |            STAGING LAYER              |
                                  | - data/staging/{season}/json/         |
                                  | - data/staging/{season}/parquet/      |
                                  +---------------------------------------+
                                                      |
                                                      v
                          +-------------------------------------------------------+
                          |        DATA QUALITY ENGINE (11-Rule Gate)             |
                          |  - Schema, Nulls, Bounds, Teams, Over/Runs Math       |
                          +-------------------------------------------------------+
                                    |                                   |
                           (Passed Validation)                 (Failed Validation)
                                    |                                   |
                                    v                                   v
        +---------------------------------------+   +---------------------------------------+
        |   CDC ENGINE (Delta Hash Detection)   |   |           QUARANTINE SINK             |
        | - Compares Hash against [cdc_states]  |   | - data/quarantine/{season}/           |
        | - Emits INSERT / UPDATE / DELETE / SKIP|   | - Logs into [quarantine_records] Table|
        +---------------------------------------+   +---------------------------------------+
                            |
                            v
        +-------------------------------------------------------------------------------+
        |              ATOMIC BATCH TRANSACTION BOUNDARY (PostgreSQL ACID)              |
        |                                                                               |
        |   +-----------------------------------------------------------------------+   |
        |   | SILVER LAYER (3NF Normalized Relational Model)                        |   |
        |   | - match, team, player, official, innings, delivery, wicket,           |   |
        |   |   wicket_fielder, powerplay, toss, player_of_match, team_squad        |   |
        |   +-----------------------------------------------------------------------+   |
        |                                       |                                       |
        |                                       v                                       |
        |   +-----------------------------------------------------------------------+   |
        |   | GOLD LAYER (Star Schema / OLAP Serving)                               |   |
        |   | - FACT_MATCH_SUMMARY (Match aggregates, boundaries, margins)          |   |
        |   | - DIM_TEAM (Career wins, total runs, boundary sums)                   |   |
        |   | - DIM_PLAYER (Career batting/bowling statistics, 50s/100s, wickets)   |   |
        |   +-----------------------------------------------------------------------+   |
        |                                                                               |
        |        [COMMIT ON SUCCESS]  <================>  [ROLLBACK ON ANY EXCEPTION]   |
        +-------------------------------------------------------------------------------+
                  |                                                     |
                  v                                                     v
+------------------------------------+               +------------------------------------+
|  LAKEHOUSE STORAGE (Iceberg/S3)    |               |   BPM PROCESS MINING OBSERVABILITY |
| - Table: ipl.ipl_matches           |               | - Emits events to bpm_process_events
| - Partitioned by: season           |               | - Real-time SLA Breach & Cycle Time|
| - Catalog: Project Nessie (REST v2)|               | - Bottleneck Detection & Stream App|
| - Storage: MinIO S3 (warehouse)    |               +------------------------------------+
+------------------------------------+                                  |
                  |                                                     |
                  +--------------------------+--------------------------+
                                             |
                                             v
                      +-----------------------------------------------+
                      |       SERVING & PRESENTATION LAYER            |
                      | - Flask Interactive Web Application (:5000)   |
                      | - REST APIs (/api/analytics, /api/bpm, etc.)  |
                      | - Airflow DAG Web UI (:8080)                  |
                      | - Power BI Desktop Dashboard (.pbix)          |
                      +-----------------------------------------------+
```

---

# PART 4 — COMPLETE FOLDER STRUCTURE

```
END-TO-END-CRICKET-DATA-ENGINEERING/
├── .github/
│   └── workflows/
│       └── ci.yml                     # GitHub Actions CI pipeline (lint, schema, tests)
├── bpm/                               # Business Process Management & Observability module
│   ├── sql/
│   │   └── process_queries.sql        # 9 Analytical SQL queries for process mining
│   ├── anomaly_detection.py           # 4-category anomaly detection (SLA, Sequence, Dup, Dev)
│   ├── config.py                      # SLA limits, canonical stage order, valid transitions
│   ├── event_logger.py                # Dual DB & JSONL event emitter + context manager
│   ├── process_metrics.py             # Cycle time, lead time, throughput, bottleneck math
│   └── process_stream.py              # PySpark Structured Streaming micro-batch application
├── config/
│   ├── config.py                      # Central paths, DB connection strings, MinIO/Nessie configs
│   └── db_config.json                 # Persistent active DB mode state file ('online'/'offline')
├── dags/
│   └── ipl_pipeline_dag.py            # Airflow DAG definition for the complete pipeline
├── data/                              # Medallion data directories (gitignored data stores)
│   ├── bronze/source/                 # Raw Cricsheet zip archive
│   ├── checkpoints/                   # Streaming checkpoint offsets
│   ├── gold/                          # Gold extract artifacts
│   ├── quarantine/                    # Isolated corrupt/failed JSON match files
│   ├── silver/                        # Processed intermediate data
│   ├── staging/                       # Season-partitioned JSON and Parquet files
│   └── warehouse/ipl/ipl_matches/     # Local/S3 Iceberg Lakehouse partition warehouse
├── database/
│   ├── connection.py                  # DB connection managers, test utilities, schema init
│   └── schema.sql                     # Complete DDL for Control, Silver, Gold, & BPM tables
├── DOCument and reports/              # Architectural guides, reports, demo notes, and PBIX file
├── logs/                              # Execution logs, BPM jsonl logs, mock Kafka queue file
├── pipeline/                          # Core Data Engineering pipeline logic
│   ├── atomicity.py                   # Atomic Silver & Gold database transaction executor
│   ├── backfill.py                    # Multi-season historical backfill coordinator
│   ├── cdc.py                         # SHA-256 Change Data Capture delta detection engine
│   ├── eda.py                         # Staging exploratory data analysis calculator
│   ├── gold_pipeline.py               # Star Schema Fact & Dimension builders
│   ├── iceberg_pipeline.py            # Lakehouse streaming ingestion (Iceberg/Nessie/MinIO)
│   ├── kafka_client.py                # Kafka Producer & Consumer with Mock Queue fallback
│   ├── pipeline_runner.py             # Main pipeline thread executor & CLI runner
│   ├── silver_pipeline.py             # 3NF relational data builder & loader
│   ├── source_ingestion.py            # Bronze source downloader & hash registrar
│   ├── staging.py                     # Season extraction & Parquet generator
│   ├── validation.py                  # 11-Rule Data Quality validator & quarantine router
│   └── verify_lakehouse.py            # 10-step automated Lakehouse verification suite
├── routes/                            # Flask Web application route Blueprints
│   ├── analytics.py                   # Analytics and EDA page endpoints
│   ├── api.py                         # REST APIs for pipeline, analytics, database, Lakehouse
│   ├── bpm.py                         # BPM dashboard & process metrics API endpoints
│   ├── home.py                        # Landing page route with database counts
│   ├── ingestion.py                   # Ingestion status and trigger routes
│   └── pipeline.py                    # Pipeline trigger, monitoring, delete, and reset routes
├── static/                            # CSS stylesheets, UI assets, and client JS
│   └── css/style.css                  # Custom styling
├── templates/                         # Jinja2 HTML templates for web portal
├── tests/                             # Automated test suite
│   ├── test_bpm.py                    # BPM logging, metric math, and anomaly unit tests
│   ├── test_iceberg_lakehouse.py      # Iceberg parsing, MinIO creds, and checkpoint tests
│   └── test_pipeline.py               # Transformations, validation rules, and DB schema tests
├── app.py                             # Flask application entry point
├── docker-compose.yaml                # Multi-container orchestration (Airflow, Kafka, MinIO, Nessie)
├── requirements.txt                   # Project Python dependencies
└── README.md                          # Main project documentation
```

### Key Folders & Files Breakdown

| File / Folder | Purpose | Important Components | Verified In Code |
| :--- | :--- | :--- | :--- |
| [`pipeline/pipeline_runner.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/pipeline_runner.py) | Central orchestration controller for single-season and multi-season runs. | `run_season_pipeline()`, `start_pipeline_async()`, `set_pipeline_status()`, `insert_pipeline_run_db()` | Yes (`pipeline_runner.py`) |
| [`pipeline/source_ingestion.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/source_ingestion.py) | Bronze layer download & source provenance tracking. | `ingest_source()`, `check_ingested()`, `calculate_sha256()`, `source_registry` | Yes (`source_ingestion.py`) |
| [`pipeline/kafka_client.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/kafka_client.py) | Message publishing, consumer staging, and mock fallback. | `run_producer()`, `run_consumer()`, `MockProducer`, `MockConsumer` | Yes (`kafka_client.py`) |
| [`pipeline/validation.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/validation.py) | 11-rule automated Data Quality assertions & quarantine isolation. | `validate_match_json()`, `validate_season()`, `quarantine_match()` | Yes (`validation.py`) |
| [`pipeline/cdc.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/cdc.py) | Cryptographic SHA-256 Change Data Capture engine. | `detect_changes_for_season()`, `compute_file_hash()`, `cdc_states`, `cdc_log` | Yes (`cdc.py`) |
| [`pipeline/atomicity.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/atomicity.py) | ACID transaction wrapper for Silver & Gold database loading. | `load_season_atomic()` with explicit `conn.commit()` and `conn.rollback()` | Yes (`atomicity.py`) |
| [`pipeline/silver_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/silver_pipeline.py) | 3NF relational normalization of cricket ball-by-ball matches. | `process_silver_match()`, `delete_existing_match_data()`, `load_silver_season()` | Yes (`silver_pipeline.py`) |
| [`pipeline/gold_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/gold_pipeline.py) | Dimensional Star Schema builder and career statistics aggregator. | `load_gold_match_summary()`, `rebuild_gold_team_dimension()`, `rebuild_gold_player_dimension()`, `upsert_gold_player_dimension()` | Yes (`gold_pipeline.py`) |
| [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py) | Apache Iceberg Lakehouse writer on MinIO S3 with Nessie REST catalog. | `get_iceberg_spark_session()`, `create_iceberg_tables()`, `sink_records_to_iceberg()`, `write_direct_lakehouse_storage()`, `verify_checkpoint_recovery()` | Yes (`iceberg_pipeline.py`) |
| [`bpm/event_logger.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/event_logger.py) | Structured process telemetry emitter (DB + JSONL) & context manager. | `log_bpm_event()`, `track_bpm_stage()`, `get_bpm_events()` | Yes (`bpm/event_logger.py`) |
| [`bpm/process_metrics.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/process_metrics.py) | Mathematical engine for process mining indicators. | `calculate_cycle_time()`, `calculate_lead_time()`, `calculate_throughput()`, `identify_bottlenecks()`, `calculate_sla_compliance()` | Yes (`bpm/process_metrics.py`) |
| [`bpm/anomaly_detection.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/anomaly_detection.py) | Real-time 4-category process anomaly detection suite. | `detect_sla_breaches()`, `detect_sequence_anomalies()`, `detect_duplicate_activities()`, `detect_process_deviations()` | Yes (`bpm/anomaly_detection.py`) |
| [`bpm/process_stream.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/process_stream.py) | PySpark Structured Streaming application for telemetry micro-batches. | `build_spark_bpm_streaming_app()`, `MockSparkProcessStream` | Yes (`bpm/process_stream.py`) |
| [`dags/ipl_pipeline_dag.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/dags/ipl_pipeline_dag.py) | Airflow production DAG definition with task dependencies. | `ingest_task`, `produce_task`, `consume_task`, `validate_task`, `cdc_task`, `load_task`, `lakehouse_task` | Yes (`dags/ipl_pipeline_dag.py`) |
| [`database/connection.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/connection.py) | DB connection pool, active mode URL resolver, and health test. | `get_connection()`, `get_db_cursor()`, `test_db_connection()`, `init_db()` | Yes (`database/connection.py`) |
| [`database/schema.sql`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/schema.sql) | Complete DDL script for 21 relational tables and indexes. | All table definitions for Control, Silver, Gold, and BPM | Yes (`database/schema.sql`) |
| [`docker-compose.yaml`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/docker-compose.yaml) | Containerized stack deployment definition. | 7 Services: `postgres`, `zookeeper`, `kafka`, `airflow-webserver`, `airflow-scheduler`, `minio`, `minio-init`, `nessie` | Yes (`docker-compose.yaml`) |

---

# PART 5 — END-TO-END EXECUTION FLOW

### Complete Execution Trace (Stage-by-Stage)

#### Step 1: Ingestion Trigger & Job Session Initialization
* **File**: [`pipeline/pipeline_runner.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/pipeline_runner.py#L414-L429) $\rightarrow$ `start_pipeline_async()`
* **Input**: List of tournament seasons (e.g., `[2024]`), pipeline mode (`INCREMENTAL` or `REPROCESS`), optional filters (`target_team`, `target_player`), simulation flags.
* **Processing**: Generates a unique UUID `run_id`, sets thread-safe in-memory status in `PIPELINE_STATUS`, spawns background daemon thread `execute_pipeline_thread()`, and records initial execution record in PostgreSQL `pipeline_runs`.
* **Output**: `run_id` string returned immediately to caller / web UI.

#### Step 2: Bronze Layer Source Download & Provenance Verification
* **File**: [`pipeline/source_ingestion.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/source_ingestion.py#L61-L111) $\rightarrow$ `ingest_source()`
* **Input**: External HTTP URL (`https://cricsheet.org/downloads/ipl_json.zip`).
* **Processing**: Checks if file exists locally and is valid via `check_ingested()`. If missing or corrupt, streams bytes in 8KB chunks, saves to `data/bronze/source/ipl_json.zip`, computes SHA-256 cryptographic hash, and inserts/updates provenance metadata in `source_registry`. Emits BPM event `bronze` / `source_ingestion`.
* **Output**: Verified raw archive on disk and `source_registry` row.

#### Step 3: Kafka Streaming & Staging Partitioning
* **File**: [`pipeline/staging.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/staging.py#L39-L78) $\rightarrow$ `stage_seasons()` & [`pipeline/kafka_client.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/kafka_client.py)
* **Input**: Raw Cricsheet ZIP and target season array.
* **Processing**: 
  1. `run_producer()` opens the ZIP, filters JSON files matching target seasons (applying optional team/player filters), serializes payloads, and publishes to Kafka topic `ipl_matches` with `match_id` as message key (falling back to `mock_kafka_queue.json` if Kafka is offline).
  2. `run_consumer()` polls `ipl_matches`, deserializes match payloads, writes raw JSON to `data/staging/{season}/json/{match_id}.json`, and writes columnar Parquet to `data/staging/{season}/parquet/{match_id}.parquet`.
  3. Emits BPM telemetry events `kafka_producer`, `kafka_consumer`, and `staging`.
* **Output**: Season-partitioned staging directories containing JSON and Parquet match files.

#### Step 4: Automated 11-Rule Data Quality Validation & Quarantine
* **File**: [`pipeline/validation.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/validation.py#L159-L187) $\rightarrow$ `validate_season()`
* **Input**: Staged JSON files in `data/staging/{season}/json/`.
* **Processing**: Executes `validate_match_json()` applying 11 checks (schema integrity, null checks, data types, over/runs range checks [0–7], math balance $total = batter + extras$, exactly 2 teams, valid squads, referential integrity, and outlier thresholds). If a match fails, `quarantine_match()` moves the JSON to `data/quarantine/{season}/`, deletes its staged Parquet file, and inserts an audit log into `quarantine_records`. Emits BPM events `validation` and `quarantine`.
* **Output**: Dictionary `{"processed": N, "valid": V, "quarantined": Q}`.

#### Step 5: Change Data Capture (CDC Delta Detection)
* **File**: [`pipeline/cdc.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/cdc.py#L17-L107) $\rightarrow$ `detect_changes_for_season()`
* **Input**: Validated JSON file paths from staging.
* **Processing**: Reads existing record hashes from `cdc_states` table for entity `match`. Computes SHA-256 hash of each staged file.
  * If no prior hash $\rightarrow$ Operation = `INSERT`.
  * If hash differs $\rightarrow$ Operation = `UPDATE`.
  * If hash matches $\rightarrow$ Operation = `SKIP`.
  * If a match previously existed in DB but is missing from staging $\rightarrow$ Operation = `DELETE`.
  * Writes operations to `cdc_log` and upserts current hashes into `cdc_states`. Emits BPM event `cdc`.
* **Output**: Dict `cdc_results` mapping `match_id` $\rightarrow$ `{operation, new_hash, filepath}`.

#### Step 6: ACID Atomic Database Loading (Silver & Gold Layers)
* **File**: [`pipeline/atomicity.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/atomicity.py#L14-L182) $\rightarrow$ `load_season_atomic()`
* **Input**: Season, pipeline mode, `cdc_results`, and `simulate_failure` flag.
* **Processing**:
  1. Opens a raw PostgreSQL connection with autocommit disabled (`conn.commit()` / `conn.rollback()`).
  2. In `INCREMENTAL` mode: Skips `SKIP` matches; deletes and inserts `UPDATE` matches; deletes `DELETE` matches; inserts `INSERT` matches into Silver 3NF tables (`match`, `team`, `player`, `official`, `innings`, `delivery`, `wicket`, `powerplay`, `toss`, `team_squad`, etc.) via `process_silver_match()`.
  3. Updates affected match rows in `FACT_MATCH_SUMMARY` and runs SQL upserts on `DIM_TEAM` and `DIM_PLAYER`.
  4. If `simulate_failure=True`, raises an intentional exception before commit to trigger `conn.rollback()`, ensuring zero partial state.
  5. Commits transaction on success. Emits BPM events `silver` and `gold`.
* **Output**: Atomically updated Silver tables and Gold OLAP Star Schema in PostgreSQL.

#### Step 7: Open Lakehouse Storage Sinking (Apache Iceberg + Nessie + MinIO)
* **File**: [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py#L516-L602) $\rightarrow$ `run_iceberg_streaming_pipeline()`
* **Input**: Validated match data for target season(s).
* **Processing**: Flattens nested match metadata into a 19-column structured Lakehouse record schema (`parse_raw_match_record`).
  1. Ensures MinIO S3 bucket `warehouse` exists.
  2. If Spark cluster is available: Writes via PySpark Iceberg session into partitioned table `ipl.ipl_matches` cataloged in Project Nessie REST v2.
  3. If standalone/direct engine: Writes partitioned Parquet files (`data/warehouse/ipl/ipl_matches/season=YYYY/`), uploads to MinIO S3, generates Iceberg metadata snapshot JSON (`v{snapshot_id}.metadata.json`), and updates streaming checkpoint offsets (`offsets.json`). Emits BPM event `lakehouse`.
* **Output**: S3-backed Lakehouse partitions with Iceberg snapshot metadata and checkpoint persistence.

#### Step 8: Control Registry Updates & UI Serving
* **File**: [`pipeline/pipeline_runner.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/pipeline_runner.py#L364-L370) $\rightarrow$ `update_pipeline_control()`, `update_pipeline_run_db()`
* **Input**: Final execution status and record counts.
* **Processing**: Updates `pipeline_control` stage flags (`bronze_status`, `staging_status`, `silver_status`, `gold_status` $\rightarrow$ `SUCCESS`), finalizes `pipeline_runs` status to `SUCCESS` with timestamps and counts, and updates memory state to 100%.
* **Output**: Live updates reflected in Flask UI dashboard, `/api/pipeline/status/<run_id>`, and database logs.

---

# PART 6 — DATA FLOW

```text
===================================================================================
                                DATA FLOW ARCHITECTURE
===================================================================================
1. SOURCE          Cricsheet External Archive (ipl_json.zip)
                   Format: JSON in ZIP | ~1,200 matches (2008–2024) | ~150 MB
                              ↓
2. INGESTION       source_ingestion.py -> data/bronze/source/ipl_json.zip
                   Integrity: SHA-256 Checksum -> DB: source_registry
                              ↓
3. STREAMING       kafka_client.py (run_producer) -> Topic: ipl_matches
                   Fallback: logs/mock_kafka_queue.json
                              ↓
4. CONSUMPTION     kafka_client.py (run_consumer) -> Season Partitioning
                   Outputs: data/staging/{season}/json/ & data/staging/{season}/parquet/
                              ↓
5. VALIDATION      validation.py (11-Rule Quality Engine)
                   Pass: Staging -> Next Stage
                   Fail: data/quarantine/{season}/ & DB: quarantine_records
                              ↓
6. CDC ENGINE      cdc.py (detect_changes_for_season)
                   Computes SHA-256 per match -> DB: cdc_states & cdc_log
                   Operations: INSERT, UPDATE, DELETE, SKIP
                              ↓
7. STORAGE (ACID)  atomicity.py (load_season_atomic) [Single Transaction]
                   ├─ Silver (3NF): match, innings, delivery, wicket, player, team...
                   └─ Gold (Star): FACT_MATCH_SUMMARY, DIM_TEAM, DIM_PLAYER
                              ↓
8. LAKEHOUSE SINK  iceberg_pipeline.py -> Apache Iceberg Partitioned by season
                   Catalog: Project Nessie (:19120) | Storage: MinIO S3 (:9000)
                              ↓
9. TELEMETRY       bpm/event_logger.py -> DB: bpm_process_events & logs/bpm_events.jsonl
                   Metrics: Cycle Time, Lead Time, Throughput, SLA Breaches, Bottlenecks
                              ↓
10. SERVING        Flask Web Portal (:5000) & Power BI Dashboard (.pbix)
===================================================================================
```

---

# PART 7 — DATABASE ANALYSIS

## Database Engine & Setup
* **Database Technology**: PostgreSQL 13+
* **Deployment Models**: 
  1. **Online**: Supabase Cloud PostgreSQL with Connection Pooler (`aws-0-ap-southeast-2.pooler.supabase.com:5432`)
  2. **Offline**: Local PostgreSQL (`localhost:5432` or Docker port `5439`)
* **Dynamic Mode Switching**: Implemented in [`config/config.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/config/config.py#L32-L62) via `get_active_db_mode()`, `set_active_db_mode()`, and persisted in `config/db_config.json`.
* **Transaction Control**: Managed using Python `contextlib` context managers in [`database/connection.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/connection.py#L52-L84) (`get_db_connection()`, `get_db_cursor(commit=True)`).

## Complete Table Catalog (21 Tables)

### 1. Pipeline Control & Governance Tables
| Table | Purpose | Important Columns | Relationships | Used By |
| :--- | :--- | :--- | :--- | :--- |
| `source_registry` | Audits raw ingested source files, checksums, and download timestamps. | `source_id` (PK), `source_name`, `source_url`, `file_hash`, `file_size`, `downloaded_at`, `status` | Standalone | `pipeline/source_ingestion.py` |
| `pipeline_control` | Tracks completion status of each stage per tournament season. | `pipeline_id` (PK), `season` (UNIQUE), `bronze_status`, `staging_status`, `validation_status`, `cdc_status`, `silver_status`, `gold_status`, `status` | Standalone | `pipeline/pipeline_runner.py` |
| `pipeline_runs` | Master execution registry of pipeline sessions. | `run_id` (PK), `pipeline_mode`, `season`, `start_time`, `end_time`, `status`, `current_stage`, `records_read`, `records_inserted`, `records_updated`, `records_deleted`, `records_skipped`, `records_failed`, `error_message` | Standalone | `pipeline/pipeline_runner.py`, `routes/api.py` |
| `pipeline_logs` | Historical execution task log (backward compatible). | `run_id`, `season`, `stage`, `start_time`, `end_time`, `status`, `records_read`, `records_inserted`, `records_updated`, `records_failed`, `error_message` | Standalone | `pipeline/pipeline_runner.py` |
| `quarantine_records` | Isolates records failing data quality rules with error details and raw payload. | `record_id` (PK), `season`, `source`, `stage`, `error_type`, `error_message`, `raw_record`, `status` | Standalone | `pipeline/validation.py` |
| `cdc_states` | Stores the latest SHA-256 hash per match business key for delta detection. | `entity_name`, `business_key` (Composite PK: `entity_name, business_key`), `record_hash`, `detected_at` | Standalone | `pipeline/cdc.py` |
| `cdc_log` | Historical audit log of every CDC operation (`INSERT`, `UPDATE`, `DELETE`). | `log_id` (PK), `run_id`, `entity_name`, `business_key`, `operation`, `old_hash`, `new_hash`, `detected_at` | Standalone | `pipeline/cdc.py` |

### 2. Silver Layer Tables (3NF Normalized Model)
| Table | Purpose | Important Columns | Relationships | Used By |
| :--- | :--- | :--- | :--- | :--- |
| `match` | Master match record (teams, venue, toss, outcome, margin). | `match_id` (PK), `season`, `match_date`, `city`, `venue`, `team1`, `team2`, `toss_winner`, `winner`, `win_by_runs`, `win_by_wickets`, `player_of_match` | Parent of all match child tables | `pipeline/silver_pipeline.py`, `routes/api.py` |
| `team` | Master canonical team catalog. | `team_name` (PK) | Referenced by `match`, `team_squad`, `DIM_TEAM` | `pipeline/silver_pipeline.py` |
| `player` | Master player registry with Cricsheet identifier. | `player_name` (PK), `cricsheet_id` | Referenced by deliveries, wickets, squads | `pipeline/silver_pipeline.py` |
| `official` | Match officials and referees. | `official_name`, `official_role` (Composite PK) | Standalone catalog | `pipeline/silver_pipeline.py` |
| `innings` | Innings-level scores and target metrics. | `innings_id` (PK), `match_id` (FK), `innings_number`, `batting_team`, `target_runs`, `target_overs` | `match_id REFERENCES match(match_id) ON DELETE CASCADE` | `pipeline/silver_pipeline.py` |
| `delivery` | Granular ball-by-ball delivery event. | `delivery_id` (PK), `match_id` (FK), `innings_number`, `batting_team`, `over_number`, `actual_delivery`, `batter`, `bowler`, `non_striker`, `batter_runs`, `extras`, `total_runs`, `wides`, `noballs`, `byes`, `legbyes` | `match_id REFERENCES match(match_id) ON DELETE CASCADE` | `pipeline/silver_pipeline.py` |
| `wicket` | Dismissal details per ball. | `wicket_id` (PK), `delivery_id` (FK), `match_id` (FK), `batter`, `bowler`, `player_out`, `dismissal_kind` | `delivery_id REFERENCES delivery(delivery_id) ON DELETE CASCADE`, `match_id REFERENCES match(match_id) ON DELETE CASCADE` | `pipeline/silver_pipeline.py` |
| `wicket_fielder` | Fielders involved in catch, stumping, or run out. | `wicket_fielder_id` (PK), `wicket_id` (FK), `delivery_id`, `player_name` | `wicket_id REFERENCES wicket(wicket_id) ON DELETE CASCADE` | `pipeline/silver_pipeline.py` |
| `powerplay` | Mandatory/optional powerplay overs per innings. | `powerplay_id` (PK), `match_id` (FK), `innings_number`, `batting_team`, `from_delivery`, `to_delivery`, `powerplay_type` | `match_id REFERENCES match(match_id) ON DELETE CASCADE` | `pipeline/silver_pipeline.py` |
| `toss` | Toss winner and decision. | `match_id` (PK / FK), `toss_winner`, `toss_decision` | `match_id REFERENCES match(match_id) ON DELETE CASCADE` | `pipeline/silver_pipeline.py` |
| `player_of_match` | Award recipient per match. | `player_of_match_id` (PK), `match_id` (FK), `player_name` | `match_id REFERENCES match(match_id) ON DELETE CASCADE` | `pipeline/silver_pipeline.py` |
| `match_official` | Officials assigned to each match. | `match_official_id` (PK), `match_id` (FK), `official_name`, `official_role` | `match_id REFERENCES match(match_id) ON DELETE CASCADE` | `pipeline/silver_pipeline.py` |
| `team_squad` | Match playing squad list per team. | `team_squad_id` (PK), `match_id` (FK), `team_name`, `player_name` | `match_id REFERENCES match(match_id) ON DELETE CASCADE` | `pipeline/silver_pipeline.py` |

### 3. Gold Layer Tables (Star Schema / OLAP)
| Table | Purpose | Important Columns | Relationships | Used By |
| :--- | :--- | :--- | :--- | :--- |
| `FACT_MATCH_SUMMARY` | Central Fact table storing match aggregates, totals, and winner margins. | `match_id` (PK), `season`, `match_date`, `venue`, `winner`, `win_by_runs`, `win_by_wickets`, `total_runs`, `total_wickets`, `total_boundaries`, `total_sixes` | Fact table connected to Dimensions | `pipeline/gold_pipeline.py`, `routes/api.py`, Power BI |
| `DIM_TEAM` | Team career dimensional table. | `team_name` (PK), `matches_played`, `wins`, `total_runs`, `total_boundaries`, `total_sixes` | Dimension table | `pipeline/gold_pipeline.py`, `routes/api.py` |
| `DIM_PLAYER` | Player career batting & bowling dimensional table. | `player_name` (PK), `batting_matches`, `batting_innings`, `total_runs`, `total_balls`, `total_fours`, `total_sixes`, `highest_score`, `fifties`, `hundreds`, `bowling_matches`, `bowling_innings`, `balls_bowled`, `overs_bowled`, `runs_conceded`, `wickets_taken`, `best_wickets` | Dimension table | `pipeline/gold_pipeline.py`, `routes/api.py`, Power BI |

### 4. BPM Process Telemetry Table
| Table | Purpose | Important Columns | Indexes | Used By |
| :--- | :--- | :--- | :--- | :--- |
| `bpm_process_events` | Emitted process lifecycle telemetry for Process Mining & SLA anomaly detection. | `event_id` (PK), `case_id`, `activity`, `event_timestamp`, `end_timestamp`, `duration_seconds`, `status`, `records_processed`, `error_message`, `metadata` (JSONB) | `idx_bpm_case_id`, `idx_bpm_activity`, `idx_bpm_timestamp` | `bpm/event_logger.py`, `bpm/process_metrics.py`, `routes/bpm.py` |

---

# PART 8 — API ANALYSIS

## Flask REST API Endpoints Catalog

| Method | Endpoint | Purpose | Input / Parameters | Processing Logic | Output Schema |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **GET** | `/api/pipeline/status/<run_id>` | Polls live progress, stage, and log messages for an active run. | `run_id` (URL path parameter) | Queries in-memory `PIPELINE_STATUS` dictionary (or falls back to `pipeline_logs` DB). | `{"stage": str, "status": str, "progress": int, "records_processed": int, "current_season": str, "elapsed_time": str, "logs": list}` |
| **GET** | `/api/pipeline/check-seasons` | Fetches status of all seasons from `pipeline_control`. | None | Queries `pipeline_control` ordered by season. | Array of `[{"season": str, "bronze_status": str, "staging_status": str, "validation_status": str, "silver_status": str, "gold_status": str, "status": str}]` |
| **POST** | `/api/analytics/query` | Dynamic OLAP query engine for KPIs, charts, and player/team reports. | JSON payload: `season_mode`, `single_season`, `from_season`, `to_season`, `team`, `player`, `venue` | Dynamically constructs parameterized SQL `WHERE` clauses against `FACT_MATCH_SUMMARY`, `DIM_PLAYER`, and `DIM_TEAM`. | `{"kpi": {"matches": int, "runs": int, "wickets": int, "boundaries": int, "sixes": int, "players": int}, "charts": {...}, "reports": {...}}` |
| **GET** | `/api/eda/data/<season>` | Retrieves pre-computed staging-layer EDA metrics for a season. | `season` (URL path parameter) | Calls `calculate_season_eda()` in `pipeline/eda.py` analyzing staged match JSON files. | `{"kpi": {...}, "charts": {"runs_by_team": {...}, "top_batters": {...}, "top_bowlers": {...}, "boundary_dist": {...}, "dot_ball_pct": {...}}}` |
| **GET** | `/api/database/stats` | Fetches row counts across all 21 Silver, Gold, and Control tables. | None | Iterates through `ALLOWED_INSPECT_TABLES` executing `SELECT COUNT(*) FROM <table>`. | `{"match": int, "delivery": int, "FACT_MATCH_SUMMARY": int, "DIM_PLAYER": int, ...}` |
| **POST** | `/api/database/sample` | Fetches top 10 sample records for any table with optional season filtering. | JSON payload: `{"table": str, "season": str}` | Validates table against whitelist `ALLOWED_INSPECT_TABLES`, formats dates/JSON objects. | `{"columns": ["match_id", "season", ...], "rows": [{...}, ...]}` |
| **GET** | `/api/db/status` | Health check for Cloud Supabase and Local PostgreSQL targets. | None | Executes `test_db_connection()` measuring connection latency (ms) for online and offline modes. | `{"status": "success", "active_mode": str, "active_label": str, "online": {"success": bool, "latency_ms": float}, "offline": {"success": bool, "latency_ms": float}}` |
| **POST** | `/api/db/switch` | Switches active database target between 'online' and 'offline'. | JSON payload: `{"mode": "online" | "offline"}` | Updates active mode in `config/db_config.json`, tests connectivity to new target. | `{"status": "success", "active_mode": str, "connection": {...}, "message": str}` |
| **GET** | `/api/lakehouse/status` | Returns connectivity status of MinIO, Nessie Catalog, and Iceberg partitions. | None | Pings MinIO health endpoint, Nessie config endpoint, inspects warehouse partitions, and reads checkpoint offset. | `{"status": "success", "minio": {"connected": bool}, "nessie": {"connected": bool}, "iceberg": {"table": str, "partitions": list, "snapshots_count": int, "checkpoint": {...}}}` |
| **POST** | `/api/lakehouse/sync` | Triggers on-demand Lakehouse stream synchronization. | JSON payload: `{"seasons": [2024, 2023]}` | Calls `run_iceberg_streaming_pipeline()` in `pipeline/iceberg_pipeline.py`. | `{"status": "SUCCESS", "records_written": int, "quarantined_count": int, "table": str}` |
| **GET** | `/api/bpm/summary` | Returns aggregated BPM process mining metrics. | Query param: `?case_id=<id>` | Calls `get_process_summary()` calculating Cycle Time, Lead Time, Throughput, SLA compliance, and bottlenecks. | `{"case_id": str, "cycle_time_seconds": float, "lead_time_seconds": float, "stage_durations": dict, "bottlenecks": list, "sla_compliance": dict, "throughput_rps": float}` |
| **GET** | `/api/bpm/events` | Retrieves recent BPM process telemetry events. | Query params: `?case_id=<id>&limit=100` | Queries `bpm_process_events` table (or falls back to `logs/bpm_events.jsonl`). | Array of event dictionaries with ISO timestamps, durations, and statuses. |
| **GET** | `/api/bpm/anomalies` | Runs 4-category anomaly detection across recent process events. | Query param: `?case_id=<id>` | Calls `detect_anomalies()` in `bpm/anomaly_detection.py`. | `{"case_id": str, "anomaly_count": int, "anomalies": [{"type": str, "severity": str, "activity": str, "message": str}, ...]}` |
| **GET** | `/api/bpm/stream-summary` | Returns PySpark Structured Streaming micro-batch processor status. | None | Runs `MockSparkProcessStream` on recent events evaluating real-time streaming schema and SLA breach flags. | `{"total_events_streamed": int, "sla_breach_count": int, "average_duration_seconds": float, "streaming_engine": str}` |

---

# PART 9 — MACHINE LEARNING ANALYSIS

* **Status**: **`[NOT FOUND / CANNOT VERIFY FROM REPOSITORY]`**
* **Findings**: The repository does **not** train predictive ML models (such as scikit-learn match outcome predictors or player score regressors). The project focuses entirely on Data Engineering, Streaming Ingestion, Lakehouse Architecture, Data Quality Assurance, and Business Process Mining.
* **Analytical Computations**: The repository contains complex SQL and Pandas analytical aggregation engines (e.g., Strike Rates, Economy Rates, Boundary Percentages, Dot Ball Percentages, Wickets Taken, 50s/100s, Win Margins, and Venue Win Rates in `pipeline/gold_pipeline.py`, `pipeline/eda.py`, and `routes/api.py`), but these are deterministic statistical aggregations rather than predictive machine learning models.

---

# PART 10 — DEEP LEARNING / NLP / COMPUTER VISION

* **Status**: **`[NOT FOUND / CANNOT VERIFY FROM REPOSITORY]`**
* **Findings**: No Deep Learning architectures (PyTorch, TensorFlow, Keras, CNNs, RNNs, or Transformers) or Computer Vision models are present or implemented in this repository.

---

# PART 11 — GENERATIVE AI / LLM / RAG ANALYSIS

* **Status**: **`[NOT FOUND / CANNOT VERIFY FROM REPOSITORY]`**
* **Findings**: No Generative AI models, LLM APIs (OpenAI, Gemini, Anthropic), Vector Databases (Pinecone, ChromaDB, Weaviate), RAG pipelines, or autonomous agent frameworks (LangChain, LlamaIndex, CrewAI) are implemented in this repository.

---

# PART 12 — DATA ENGINEERING ANALYSIS

### 1. Batch & Streaming Architecture
* **Streaming Layer**: Apache Kafka 7.3.0 streams match JSON payloads over topic `ipl_matches`.
* **Micro-Batch Processing**: PySpark Structured Streaming micro-batch processor (`bpm/process_stream.py`) with sliding window aggregations (`5 minutes` window, `1 minute` slide) and watermarking (`1 minute`).
* **Batch Orchestration**: Apache Airflow DAG (`dags/ipl_pipeline_dag.py`) scheduled `@daily` with catchup disabled and 1 automatic retry on a 2-minute delay.

### 2. Medallion Architecture Implementation
* **Bronze Layer**: Raw downloaded source ZIP archive `data/bronze/source/ipl_json.zip` with checksum registration in `source_registry`.
* **Staging Layer**: Extracted raw match JSONs and columnar Parquet files partitioned by season (`data/staging/{season}/json/` and `data/staging/{season}/parquet/`).
* **Silver Layer**: Cleaned, validated, and normalized 3NF relational PostgreSQL schema modeling ball-by-ball deliveries, wickets, fielders, players, teams, powerplays, and toss outcomes.
* **Gold Layer**: High-performance Star Schema OLAP serving layer (`FACT_MATCH_SUMMARY`, `DIM_TEAM`, `DIM_PLAYER`) optimized for BI and dashboard queries.

### 3. Open Lakehouse Integration (Week 2 Architecture)
* **Table Format**: Apache Iceberg v2 table format (`ipl.ipl_matches`).
* **Partitioning Strategy**: Partitioned by `season` (`PARTITIONED BY (season)`).
* **Catalog**: Project Nessie REST API v2 (`http://localhost:19120/api/v2`).
* **Object Storage**: MinIO S3-compatible object storage server (`s3a://warehouse/ipl/ipl_matches`).
* **Checkpointing & Fault Tolerance**: Stream offsets saved to `data/checkpoints/iceberg_streaming/offsets.json` containing `last_committed_snapshot`, `timestamp`, and `records_written`.

### 4. Change Data Capture (CDC Delta Engine)
* **Cryptographic Hashing**: Computes SHA-256 hash per match payload (`compute_file_hash` in `pipeline/cdc.py`).
* **Delta Classification**:
  * `INSERT`: Key not present in `cdc_states`.
  * `UPDATE`: Key present but SHA-256 hash differs.
  * `DELETE`: Key present in `match` table but absent from current staging files.
  * `SKIP`: Key present and SHA-256 hash is identical.
* **Auditability**: Complete operation history logged in `cdc_log` with `old_hash` and `new_hash`.

### 5. Data Quality & Quarantine Isolation
* **11 Automated Validation Rules** implemented in [`pipeline/validation.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/validation.py#L9-L119):
  1. `SCHEMA_VALIDATION`: Root keys `info` and `innings` must exist.
  2. `REQUIRED_FIELDS`: Required metadata fields (`teams`, `dates`, `match_type`, `venue`).
  3. `NULL_CHECK`: Team array and match dates must not contain null/empty values.
  4. `DATATYPE_CHECK`: Scheduled overs must be numeric.
  5. `RANGE_CHECK`: Over numbers $[0, overs\_limit-1]$, batter runs $[0, 7]$, extras $[0, 7]$.
  6. `INVALID_RUNS`: Mathematical assertion that $total\_runs = batter\_runs + extras$.
  7. `TEAM_VALIDATION`: Exactly 2 teams registered per match.
  8. `PLAYER_VALIDATION`: Player squad registry non-empty and contains active squads.
  9. `REFERENTIAL_INTEGRITY`: Innings batting team must match one of the two registered teams.
  10. `OUTLIER_DETECTION (Scores)`: Total match score must not exceed 560 runs; completed match cannot have $< 30$ total runs.
  11. `OUTLIER_DETECTION (Squad/Overs)`: Squad size must be 11–16 players; scheduled overs must be between 5 and 20.

### 6. Process Mining & Business Process Management (BPM)
* Emits structured JSON events to `bpm_process_events` and `logs/bpm_events.jsonl` via `log_bpm_event()` and `track_bpm_stage()`.
* **Telemetry Schema**: `case_id`, `activity`, `event_timestamp`, `end_timestamp`, `duration_seconds`, `status`, `records_processed`, `error_message`, `metadata`.
* **Process Metrics Computed**:
  * **Cycle Time**: $Max(end\_timestamp) - Min(event\_timestamp)$.
  * **Lead Time**: Total elapsed pipeline execution latency.
  * **Throughput**: $Records / Duration\ (records/sec)$.
  * **Bottlenecks**: Stages ranked descending by total duration / percentage share of total latency.
  * **SLA Compliance %**: Percentage of stages executing within configured limits (e.g., Bronze: 30s, Staging: 25s, Validation: 15s, CDC: 15s, Silver: 30s, Gold: 25s, Lakehouse: 20s).

---

# PART 13 — BUSINESS LOGIC

### 1. Canonical Franchise Name Mapping
* **File**: [`config/config.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/config/config.py#L82-L93) $\rightarrow$ `clean_team_name()`
* **Rule**: Maps historical rebrandings and naming variants to their official canonical names:
  * `"Delhi Daredevils"` $\rightarrow$ `"Delhi Capitals"`
  * `"Kings XI Punjab"` $\rightarrow$ `"Punjab Kings"`
  * `"Rising Pune Supergiant"` $\rightarrow$ `"Rising Pune Supergiants"`
  * `"Royal Challengers Bangalore"` $\rightarrow$ `"Royal Challengers Bengaluru"`
* **Business Purpose**: Prevents fractured analytics and fragmented historical career stats when franchises rebrand across seasons.

### 2. Season Year Resolution
* **File**: [`pipeline/staging.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/staging.py#L10-L28) $\rightarrow$ `get_match_season()`
* **Rule**: Extracts season from `info.season`. If missing, extracts year prefix from `info.dates[0]`. If season has a split format (e.g., `"2020/21"` or `"2007/08"`), normalizes to the initial year (`"2020"`).
* **Business Purpose**: Guarantees standard 4-digit tournament season partitioning.

### 3. Bowler Wicket Credit Rules
* **File**: [`pipeline/gold_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/gold_pipeline.py#L210-L220) & [`pipeline/eda.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/eda.py#L88-L90)
* **Rule**: Wickets are only credited to the bowler if the dismissal kind is bowler-credited (bowled, caught, lbw, stumped, hit wicket). Run outs, retired hurt, and obstructing the field are excluded from bowler career wicket aggregates.
* **Business Purpose**: Adheres strictly to official International Cricket Council (ICC) and IPL scoring laws.

### 4. Player Milestone & Strike Rate Aggregation
* **File**: [`pipeline/gold_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/gold_pipeline.py#L170-L194)
* **Rule**: 
  * Fifties: $50 \le match\_runs < 100 \rightarrow +1$.
  * Hundreds: $match\_runs \ge 100 \rightarrow +1$.
  * Overs Bowled: $balls\_bowled / 6.0$ rounded to 1 decimal place.
* **Business Purpose**: Populates dimension `DIM_PLAYER` with career batting and bowling benchmarks.

### 5. Idempotent Batch Overwrites
* **File**: [`pipeline/silver_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/silver_pipeline.py#L7-L11) $\rightarrow$ `delete_existing_match_data()`
* **Rule**: Before inserting matches into Silver tables during reprocess or update runs, executes `DELETE FROM match WHERE match_id = ANY(%s)`. Cascading foreign keys (`ON DELETE CASCADE`) delete child deliveries, wickets, fielders, powerplays, and toss records.
* **Business Purpose**: Guarantees idempotency — running the pipeline multiple times results in zero duplicate keys or double-counted stats.

---

# PART 14 — SECURITY

* **Secrets Management**: Sensitive credentials (passwords, Supabase URLs, MinIO keys) are configured via environment variables with secure local fallbacks (`DATABASE_URL`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `FLASK_SECRET_KEY`).
* **SQL Injection Prevention**: All dynamic SQL queries in `database/connection.py`, `pipeline/silver_pipeline.py`, `pipeline/gold_pipeline.py`, `pipeline/cdc.py`, and `routes/api.py` use parameterized placeholders (`%s`) via `psycopg2` rather than raw string concatenation.
* **Table Whitelist Protection**: The database sampling endpoint (`/api/database/sample` in `routes/api.py`) enforces strict validation against `ALLOWED_INSPECT_TABLES` to prevent unauthorized table reads or arbitrary schema inspection.
* **Cascade Isolation**: Foreign key constraints with `ON DELETE CASCADE` prevent orphan records from persisting in child tables when a match is purged.
* **Non-Root Docker Execution**: Airflow services in `docker-compose.yaml` run under standard Airflow user permissions with resource logging constraints (`max-size: 10m`, `max-file: "3"`).

---

# PART 15 — TESTING

## Automated Test Suite Summary

| Test Class | Test File | What It Tests | Expected Result | Evidence in Code |
| :--- | :--- | :--- | :--- | :--- |
| `TestDataTransformations` | [`tests/test_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L18) | Canonical team name mapping (`clean_team_name`). | Rebranded team names map to canonical counterparts. | [`test_pipeline.py:L21-L29`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L21-L29) |
| `TestDataTransformations` | [`tests/test_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L30) | Season extraction from info.season and date fallbacks. | Correct 4-digit season string extracted. | [`test_pipeline.py:L30-L36`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L30-L36) |
| `TestDataTransformations` | [`tests/test_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L38) | SHA-256 hash calculation for identical and differing files. | Identical files match; differing files yield distinct 64-char hashes. | [`test_pipeline.py:L38-L68`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L38-L68) |
| `TestDataTransformations` | [`tests/test_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L69) | 11 Data Quality validation assertions on valid vs. invalid JSON. | Invalid JSON caught as `SCHEMA_VALIDATION`; valid match passes. | [`test_pipeline.py:L69-L123`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L69-L123) |
| `TestPipelineMocking` | [`tests/test_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L125) | Mock Kafka Producer & Consumer messaging flow. | Messages published to mock file queue and polled by consumer. | [`test_pipeline.py:L125-L144`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L125-L144) |
| `TestDatabaseInfrastructure` | [`tests/test_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L146) | DB connection & table existence verification. | DB connection succeeds and all 21 schema tables exist. | [`test_pipeline.py:L146-L183`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_pipeline.py#L146-L183) |
| `TestBPMEventLogging` | [`tests/test_bpm.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_bpm.py#L32) | BPM event emission payload taxonomy and schema fields. | Correct keys, timestamps, durations, and status emitted. | [`test_bpm.py:L35-L58`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_bpm.py#L35-L58) |
| `TestBPMProcessMetrics` | [`tests/test_bpm.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_bpm.py#L60) | Cycle Time, Lead Time, Throughput, and Bottleneck calculations. | Cycle time = 85.0s, throughput = 0.87 rec/s, bottleneck = Silver (30s). | [`test_bpm.py:L60-L150`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_bpm.py#L60-L150) |
| `TestBPMAnomalyDetection` | [`tests/test_bpm.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_bpm.py#L152) | SLA breaches, Out-of-Sequence stages, Duplicates, and Deviations. | Flags SLA breaches, out-of-order stages, and illegal transitions. | [`test_bpm.py:L152-L234`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_bpm.py#L152-L234) |
| `TestBPMStreamProcessing` | [`tests/test_bpm.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_bpm.py#L235) | PySpark Structured Streaming micro-batch schema & SLA flags. | Micro-batches correctly parsed and flagged for SLA breaches. | [`test_bpm.py:L235-L256`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_bpm.py#L235-L256) |
| `TestIcebergLakehouseIntegration` | [`tests/test_iceberg_lakehouse.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_iceberg_lakehouse.py#L34) | Lakehouse configs, record parser, partitioning, and checkpoint recovery. | Schema matches 19 fields, season partitioning verified, checkpoint recovered. | [`test_iceberg_lakehouse.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/tests/test_iceberg_lakehouse.py) |

* **Running Tests Locally**:
  ```bash
  python -m unittest discover -s tests -p "test_*.py"
  ```

---

# PART 16 — ERROR HANDLING & RESILIENCE

1. **Transactional Rollback (Atomicity)**:
   * Implemented in [`pipeline/atomicity.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/atomicity.py#L173-L178). If any failure occurs after Silver is loaded but before Gold finishes, `conn.rollback()` executes, reverting the database to its pre-run state.
2. **Kafka Broker Offline Fallback**:
   * Implemented in [`pipeline/kafka_client.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/kafka_client.py#L172-L183). If `confluent_kafka` is not installed or the Kafka broker is unreachable, the client automatically falls back to `MockProducer` and `MockConsumer` writing to `logs/mock_kafka_queue.json`.
3. **Data Quality Quarantine Strategy**:
   * Implemented in [`pipeline/validation.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/validation.py#L121-L158). Corrupt records are isolated into `data/quarantine/{season}/` and logged in `quarantine_records` rather than crashing the pipeline.
4. **Lakehouse Spark & Winutils Fallback**:
   * Implemented in [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py#L161-L181). On Windows environments without `winutils.exe` or Spark cluster, the engine falls back to `write_direct_lakehouse_storage()` generating Parquet partitions and Iceberg metadata snapshots directly.
5. **Non-Blocking BPM Telemetry**:
   * Implemented in [`bpm/event_logger.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/event_logger.py#L16-L78). Telemetry emission catches all exceptions internally and logs to a JSONL file sink if the database is unreachable, ensuring observability never crashes the core data pipeline.

---

# PART 17 — CONFIGURATION & ENVIRONMENT

## Environment Variables
* `DATABASE_URL`: Master PostgreSQL connection URI (`postgresql://postgres:password@localhost:5432/postgres`).
* `LOCAL_DATABASE_URL`: Explicit Local PostgreSQL connection string.
* `ONLINE_DATABASE_URL`: Supabase Cloud PostgreSQL pooler connection string.
* `DB_MODE`: Active database target (`online` or `offline`).
* `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (`localhost:9092` or `kafka:29092`).
* `MINIO_ENDPOINT`: MinIO S3 endpoint (`http://localhost:9000`).
* `MINIO_ACCESS_KEY`: MinIO S3 access key (`admin`).
* `MINIO_SECRET_KEY`: MinIO S3 secret key (`password123`).
* `MINIO_WAREHOUSE_BUCKET`: MinIO bucket for Iceberg tables (`warehouse`).
* `NESSIE_URI`: Project Nessie REST API v2 URL (`http://localhost:19120/api/v2`).
* `ICEBERG_CATALOG_NAME`: Iceberg catalog name (`ipl`).
* `ICEBERG_WAREHOUSE_PATH`: S3A warehouse URI (`s3a://warehouse/`).
* `FLASK_SECRET_KEY`: Secret key for Flask session signing.
* `SLA_<STAGE>_SECONDS`: Configurable SLA limits for BPM monitoring (e.g., `SLA_BRONZE_SECONDS=30.0`).

## Port Allocation
* **`5000`**: Flask Web Portal & REST API
* **`8080`**: Apache Airflow Webserver UI
* **`9092`**: Apache Kafka Broker (Host access)
* **`2181`**: Apache Zookeeper
* **`5432`**: Local PostgreSQL / Supabase
* **`5439`**: Airflow Metadata PostgreSQL (Docker mapped to prevent host 5432 conflict)
* **`9000`**: MinIO S3 API
* **`9001`**: MinIO Web Console
* **`19120`**: Project Nessie REST Catalog Server

---

# PART 18 — DEPLOYMENT

```text
                               +-----------------------------+
                               |     Developer / CI (Git)    |
                               +-----------------------------+
                                              |
                                              v
                               +-----------------------------+
                               |   GitHub Actions CI / CD    |
                               | (Lint, Postgres Test, Pytest)|
                               +-----------------------------+
                                              |
                                              v
                               +-----------------------------+
                               |    Docker Compose Stack     |
                               +-----------------------------+
                                              |
            +---------------------------------+---------------------------------+
            |                                 |                                 |
            v                                 v                                 v
+-----------------------+         +-----------------------+         +-----------------------+
|  Message & Ingestion  |         |   Storage & Catalog   |         |     Orchestration     |
| - Zookeeper (:2181)   |         | - Postgres DB (:5439) |         | - Airflow Webserver   |
| - Kafka Broker (:9092)|         | - MinIO S3 (:9000)    |         |   (:8080)             |
|                       |         | - Nessie Catalog      |         | - Airflow Scheduler   |
|                       |         |   (:19120)            |         |                       |
+-----------------------+         +-----------------------+         +-----------------------+
            |                                 |                                 |
            +---------------------------------+---------------------------------+
                                              |
                                              v
                               +-----------------------------+
                               |   Flask Monitoring Portal   |
                               |          (:5000)            |
                               +-----------------------------+
                                              |
                                              v
                               +-----------------------------+
                               |  End User / Sports Analyst  |
                               +-----------------------------+
```

---

# PART 19 — IMPORTANT IMPLEMENTATION DETAILS (Deep-Dive Points)

1. **Why Medallion Architecture?**: Separates raw immutability (Bronze), cleansed columnar files (Staging), normalized relational integrity (Silver 3NF), and denormalized analytical serving (Gold Star Schema).
2. **Why Apache Kafka for Ingestion?**: Decouples the source downloader from downstream consumers. In high-throughput streaming environments, Kafka absorbs upstream spikes and prevents data loss.
3. **Mock Streaming Resiliency**: Implemented a mock file queue (`mock_kafka_queue.json`) enabling zero-dependency local development and unit testing without requiring live Docker Kafka containers.
4. **CDC Cryptographic Hashing**: SHA-256 hashes generated from match payloads eliminate expensive full-table scans and redundant writes, skipping unchanged historical matches.
5. **Batch Transaction Atomicity**: Wrapped the entire Silver and Gold database loading process inside a single SQL transaction block with explicit `ROLLBACK` on exception.
6. **Cascading Foreign Key Cleanup**: Deleting a `match_id` cascades to deliveries, wickets, fielders, powerplays, squads, and officials, ensuring zero orphan records upon reprocessing.
7. **Canonical Franchise Name Mapping**: Historical franchise name changes (`Delhi Daredevils` $\rightarrow$ `Delhi Capitals`, `Kings XI Punjab` $\rightarrow$ `Punjab Kings`, `RCB` $\rightarrow$ `Royal Challengers Bengaluru`) are resolved at the ingestion gate.
8. **11-Rule Data Quality Gate**: Validates match JSONs across structural schema, null fields, delivery over bounds $[0, 19]$, score mathematics ($total = batter + extras$), and outlier thresholds ($> 560$ runs).
9. **Quarantine Auditability**: Replaced silent error drops with an isolated quarantine directory (`data/quarantine/{season}/`) and a metadata audit table (`quarantine_records`).
10. **Apache Iceberg Open Table Format**: Implemented Iceberg partitioned table `ipl.ipl_matches` with ACID snapshots, metadata manifests, and partition pruning.
11. **Project Nessie Catalog Integration**: Connected Iceberg to Project Nessie REST API v2 (`http://localhost:19120/api/v2`) for branch-aware catalog management.
12. **MinIO S3 Object Storage**: Configured MinIO with S3A/S3FileIO endpoints (`http://localhost:9000`) and bucket auto-initialization (`minio-init`).
13. **Streaming Checkpointing**: Saved streaming checkpoint state (`data/checkpoints/iceberg_streaming/offsets.json`) storing `last_committed_snapshot` and `records_written`.
14. **BPM Process Observability**: Emitted structured lifecycle events (`bpm_process_events`) capturing `case_id`, `activity`, `duration_seconds`, `status`, and `metadata`.
15. **Mathematical Process Metrics**: Calculated Cycle Time ($Max(end) - Min(start)$), Lead Time, and Throughput ($Records / Second$).
16. **Bottleneck Identification**: Ranked pipeline stages by duration and percentage of total latency share to identify performance bottlenecks.
17. **4-Category Process Anomaly Detection**: Detected SLA breaches, out-of-sequence stages, duplicate executions, and illegal process transitions.
18. **Quarantine as Valid Exception Path**: Configured the transition graph so `validation` $\rightarrow$ `quarantine` is recognized as a valid exception path rather than a process deviation.
19. **Multi-Season Sequence Loops**: Handled sequential multi-season execution (`[2008, 2009]`) so repeating `bronze` after `gold` is recognized as a new cycle rather than out-of-sequence.
20. **Dual Database Architecture**: Supported seamless switching between Supabase Cloud PostgreSQL and Local PostgreSQL via `config/db_config.json`.
21. **Airflow DAG Orchestration**: Constructed DAG `ipl_production_data_pipeline` with 7 sequential task operators and XCom state passing.
22. **Dual Execution Engine**: Supported both Airflow orchestration and Flask asynchronous background threading (`start_pipeline_async`).
23. **Historical Backfilling**: Implemented `trigger_historical_backfill(from_year, to_year)` in `pipeline/backfill.py` with full overwrite reprocess mode.
24. **Staging-Layer EDA Engine**: Built `pipeline/eda.py` computing KPIs, run distributions, boundary shares, and dot ball percentages directly from staging JSONs before database loading.
25. **Interactive Flask Web Portal**: Designed a responsive dark-mode portal with real-time SSE/polling, Chart.js visualizations, and database inspection tools.
26. **Automated CI/CD Pipeline**: GitHub Actions workflow (`ci.yml`) linting code with Flake8, booting an ephemeral PostgreSQL container, and running tests on every push.

---

# PART 20 — INTERVIEW QUESTIONS FROM THE CODE

### Basic Questions (10)
1. In [`pipeline/source_ingestion.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/source_ingestion.py#L12), how is `calculate_sha256()` implemented and why is chunking used during hashing? *(Concept: File I/O and Hash Generation)*
2. What is the role of the `source_registry` table in `database/schema.sql`? *(Concept: Data Provenance & Metadata)*
3. In [`pipeline/staging.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/staging.py#L30), why is `clear_staging_season()` called before consuming messages? *(Concept: Idempotency)*
4. What is the difference between the Silver and Gold layers in this project? *(Concept: Medallion Architecture)*
5. How does `get_match_season()` handle seasons formatted as `"2020/21"`? *(Concept: Data Cleansing & Normalization)*
6. In [`database/connection.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/connection.py#L62), how does the `get_db_cursor()` context manager manage transactions? *(Concept: Python Context Managers & Database Cursors)*
7. What are the 3 dimension tables created in `schema.sql`? *(Concept: Star Schema OLAP Modeling)*
8. In [`app.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/app.py#L8), what does the `@app.context_processor` do? *(Concept: Flask Template Context Injection)*
9. Why does `docker-compose.yaml` map PostgreSQL to port `5439` instead of `5432`? *(Concept: Port Conflict Management in Docker)*
10. How does `pipeline/backfill.py` trigger multi-season historical backfills? *(Concept: Backfill Automation)*

### Intermediate Questions (15)
11. How does the CDC engine in [`pipeline/cdc.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/cdc.py#L17) detect `INSERT`, `UPDATE`, `DELETE`, and `SKIP` operations? *(Concept: Change Data Capture Hash Comparison)*
12. In [`pipeline/atomicity.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/atomicity.py#L14), what happens when `simulate_failure=True` is passed to `load_season_atomic()`? *(Concept: ACID Transaction Rollback)*
13. Explain how `clean_team_name()` in [`config/config.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/config/config.py#L82) resolves historical team name variations. *(Concept: Master Data Management / Entity Resolution)*
14. In [`pipeline/validation.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/validation.py#L9), list 5 of the 11 data quality rules and explain how bad records are quarantined. *(Concept: Data Quality Assurance & Dead-Letter Isolation)*
15. How does `pipeline/kafka_client.py` gracefully fall back to `MockProducer` and `MockConsumer` when Kafka is unavailable? *(Concept: Fault-Tolerant Streaming)*
16. In [`pipeline/gold_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/gold_pipeline.py#L95), what is the difference between `rebuild_gold_team_dimension()` and `upsert_gold_team_dimension()`? *(Concept: Full Refresh vs. Incremental Upsert)*
17. In [`pipeline/silver_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/silver_pipeline.py#L129), how are ball-by-ball deliveries, extras, and wickets parsed and linked via foreign keys? *(Concept: Relational Normalization)*
18. How does `dags/ipl_pipeline_dag.py` pass serialized CDC results between tasks? *(Concept: Airflow XComs)*
19. In [`bpm/event_logger.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/event_logger.py#L81), how does `track_bpm_stage` automatically calculate stage durations and status? *(Concept: Performance Telemetry & Context Handlers)*
20. In [`bpm/process_metrics.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/process_metrics.py#L64), how is Process Cycle Time mathematically calculated across multiple cases? *(Concept: Process Mining Mathematics)*
21. How does `bpm/anomaly_detection.py` detect Out-of-Sequence stage executions? *(Concept: Sequence Graph Verification)*
22. How does `routes/api.py` dynamically construct SQL `WHERE` clauses for multi-dimensional analytics? *(Concept: Dynamic Parameterized SQL Construction)*
23. In [`pipeline/eda.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/eda.py#L5), how are boundary percentages and dot ball percentages computed from staged JSONs? *(Concept: Statistical In-Memory Aggregation)*
24. How is persistent database switching between Supabase Cloud and Local PostgreSQL implemented in `config/config.py`? *(Concept: Dynamic Configuration Management)*
25. In [`pipeline/pipeline_runner.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/pipeline_runner.py#L12), how is thread safety ensured when updating `PIPELINE_STATUS`? *(Concept: Python Threading & Locks)*

### Advanced Questions (15)
26. In [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py#L113), explain the Spark configuration settings required to connect PySpark to Apache Iceberg, Project Nessie, and MinIO S3. *(Concept: Open Lakehouse Catalog & Storage Configuration)*
27. How does `write_direct_lakehouse_storage()` in `pipeline/iceberg_pipeline.py` generate Iceberg v2 snapshot metadata without a live JVM Spark cluster? *(Concept: Lakehouse Metadata Emulation)*
28. Explain the checkpoint recovery mechanism implemented in `pipeline/iceberg_pipeline.py` and tested in `tests/test_iceberg_lakehouse.py`. *(Concept: Stream Fault Tolerance & Offsets)*
29. In [`bpm/process_stream.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/process_stream.py#L44), explain how PySpark Structured Streaming windowed aggregations and watermarks are configured for real-time SLA breach detection. *(Concept: Structured Streaming & Watermarking)*
30. How does `bpm/anomaly_detection.py` distinguish legitimate multi-season loops from illegal process deviations? *(Concept: Directed Transition Graph Analysis)*
31. In `database/schema.sql`, why is `ON DELETE CASCADE` specified on child foreign keys and what are the trade-offs in high-concurrency environments? *(Concept: Database Relational Constraints & Locking)*
32. How does the pipeline guarantee exact-once delivery semantics from Kafka ingestion through to the PostgreSQL Gold layer? *(Concept: End-to-End Idempotency & Exactly-Once Semantics)*
33. In [`pipeline/gold_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/gold_pipeline.py#L143), explain the complex SQL query that aggregates career batting and bowling statistics for `DIM_PLAYER`. *(Concept: Advanced SQL Aggregation & Window Functions)*
34. How does `pipeline/verify_lakehouse.py` execute an automated 10-step verification suite across MinIO, Nessie, and Iceberg? *(Concept: Data Platform Integration Testing)*
35. How does the GitHub Actions workflow in `.github/workflows/ci.yml` verify database DDL and transformation integrity in an ephemeral CI runner? *(Concept: DataOps & CI/CD Pipelines)*
36. If a pipeline run fails during the CDC stage, how does `pipeline_runner.py` support resuming execution from the failed stage (`replay_stage`)? *(Concept: Pipeline Replay & Failure Recovery)*
37. How does the system handle concurrent read queries on `FACT_MATCH_SUMMARY` while `load_season_atomic` is executing? *(Concept: PostgreSQL MVCC & Transaction Isolation)*
38. Explain how `bpm/sql/process_queries.sql` calculates stage-wise latency share percentages using SQL window functions (`SUM() OVER ()`). *(Concept: Analytical SQL & Windowing)*
39. What are the trade-offs between using PyArrow Parquet files in staging versus writing directly to PostgreSQL? *(Concept: Columnar Storage vs. Row-Oriented Storage)*
40. How does the dual-logging mechanism in `bpm/event_logger.py` guarantee zero data pipeline interruption when the database is offline? *(Concept: Resilient Observability & Circuit Breakers)*

### Project Architecture Questions (10)
41. Walk through the entire lifecycle of a cricket match from raw Cricsheet JSON to the Power BI dashboard. *(Concept: End-to-End Data Pipeline Architecture)*
42. Why was Apache Iceberg selected over standard Parquet files in object storage? *(Concept: Open Table Formats vs. Data Lakes)*
43. How does Project Nessie serve as a transactional catalog for Iceberg tables? *(Concept: Lakehouse Catalogs & Versioning)*
44. Why does the architecture use both Apache Airflow and an asynchronous Flask thread runner? *(Concept: Scheduled Production vs. Interactive Ad-Hoc Execution)*
45. How does the quarantine strategy protect data integrity across downstream BI reports? *(Concept: Bad Data Isolation & Quality Governance)*
46. Explain the role of MinIO in the Lakehouse storage architecture. *(Concept: S3-Compatible Object Storage)*
47. How does the Star Schema in the Gold layer improve query performance compared to querying 3NF Silver tables directly? *(Concept: Dimensional Modeling vs. 3NF)*
48. What are the primary bottlenecks in this pipeline and how can they be mitigated at 100× data volume? *(Concept: Scalability & Performance Engineering)*
49. How does the BPM observability layer differ from standard Airflow task logging? *(Concept: Process Mining vs. Infrastructure Logging)*
50. How does the platform support zero-downtime database switching between Cloud Supabase and Local PostgreSQL? *(Concept: Multi-Environment Database Routing)*

### Technology-Specific Questions (15)
51. **Kafka**: What is the purpose of `queue.buffering.max.messages` and `auto.offset.reset='earliest'` in `pipeline/kafka_client.py`?
52. **Kafka**: How does the Kafka consumer commit offsets (`consumer.commit(asynchronous=False)`) to prevent message loss?
53. **PostgreSQL**: What is `RealDictCursor` in `psycopg2.extras` and why is it used throughout `database/connection.py`?
54. **PostgreSQL**: How does `ON CONFLICT (business_key) DO UPDATE` in `cdc_states` work under high concurrency?
55. **Airflow**: Explain why `catchup=False` and `schedule_interval='@daily'` are configured in `dags/ipl_pipeline_dag.py`.
56. **Airflow**: What does `EmptyOperator` represent in the task dependency chain?
57. **Iceberg**: What is the purpose of `TBLPROPERTIES ('write.format.default'='parquet', 'write.parquet.compression-codec'='zstd')` in `pipeline/iceberg_pipeline.py`?
58. **PySpark**: Explain how `from_json(col("value"), schema)` enforces schema validation in `bpm/process_stream.py`.
59. **PySpark**: How does `.withWatermark("event_timestamp", "1 minute")` handle late-arriving process events?
60. **Flask**: How do Flask Blueprints (`routes/pipeline.py`, `routes/bpm.py`, etc.) organize application endpoints?
61. **Flask**: Why is `use_reloader=False` specified in `app.run()` when spawning background data processing threads?
62. **MinIO**: How does `client.fput_object()` stream local Parquet files into the S3 bucket?
63. **Nessie**: How does Nessie handle Iceberg catalog references (`ref=main`) over REST API v2?
64. **Parquet**: Why does `pipeline/kafka_client.py` serialize match payloads into Parquet using `df_raw.to_parquet(index=False)`?
65. **Docker**: What is the purpose of `extra_hosts: - "host.docker.internal:host-gateway"` in `docker-compose.yaml`?

---

# PART 21 — "DEFEND YOUR PROJECT" QUESTIONS

1. **Why did you use Apache Kafka if you already have Apache Airflow?**
   * *Defense*: Airflow is a **batch workflow orchestrator**, not a message broker. Kafka provides **decoupled event streaming**, absorbing upstream network spikes, enabling backpressure, and allowing multiple consumers (e.g., Staging, Lakehouse, and Real-time Stream Analytics) to read from the same `ipl_matches` topic independently without re-downloading source archives.
2. **Why didn't you use an ORM like SQLAlchemy instead of raw SQL and `psycopg2`?**
   * *Defense*: In analytical data engineering pipelines handling ball-by-ball deliveries (over 200,000+ rows across all seasons), ORMs introduce significant object-relational mapping overhead and slow down bulk inserts. Raw SQL via `psycopg2` provides maximum execution speed, explicit transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`), and fine-grained control over complex analytical aggregations and Star Schema upserts.
3. **What happens if the pipeline crashes after writing to Silver tables but before Gold tables update?**
   * *Defense*: The entire Silver and Gold database loading stage executes inside a single ACID PostgreSQL transaction (`load_season_atomic` in `pipeline/atomicity.py`). If a failure occurs at any point, a `ROLLBACK` is issued immediately, reverting all Silver insertions and leaving the database in its clean pre-run state.
4. **How does your Change Data Capture (CDC) engine scale compared to log-based CDC (like Debezium)?**
   * *Defense*: For file-based batch data ingestion from external providers, log-based CDC is not applicable because we do not own the source database transaction logs. Our payload-level SHA-256 delta hashing accurately identifies `INSERT`, `UPDATE`, `DELETE`, and `SKIP` operations before database execution, eliminating full-table scans.
5. **Why do you have both a PostgreSQL Gold layer and an Apache Iceberg Lakehouse?**
   * *Defense*: PostgreSQL serves as the low-latency relational OLAP layer for the interactive web portal and operational transactional queries. Apache Iceberg provides open Lakehouse storage on MinIO S3 with ACID time-travel, partition pruning, and schema evolution for big data engines (like PySpark and Trino) at cloud scale.
6. **What happens if a corrupted match with impossible cricket scores enters the pipeline?**
   * *Defense*: The 11-rule Data Quality Gate (`pipeline/validation.py`) catches structural, logical, and mathematical anomalies (e.g., $total \ne batter + extras$, overs $> 20$, match score $> 560$). The corrupted file is immediately moved to `data/quarantine/{season}/`, its staged Parquet counterpart is deleted, and the violation is logged in `quarantine_records` for auditing without crashing the pipeline.
7. **How does your system handle historical franchise rebrandings (e.g., Delhi Daredevils to Delhi Capitals)?**
   * *Defense*: The ingestion layer routes all team strings through `clean_team_name()` in `config/config.py`, mapping historical aliases to their canonical representations before storing in Silver/Gold tables, ensuring uninterrupted historical analytics.
8. **What is the bottleneck in your current architecture and how would you optimize it for 100× data volume?**
   * *Defense*: The primary bottleneck is single-threaded row-by-row delivery inserts in PostgreSQL Silver loading. For 100× data volume, we would:
     1. Replace row-level inserts with PostgreSQL `COPY` binary streams or `pg_bulkload`.
     2. Scale the Kafka consumer across a distributed consumer group partitioned by `season`.
     3. Transition Silver and Gold aggregations entirely into distributed PySpark/Iceberg compute nodes on Kubernetes.
9. **Why did you implement a custom BPM telemetry module instead of relying only on Airflow logs?**
   * *Defense*: Airflow logs provide infrastructure-level task status, but lack granular Business Process Management (BPM) metrics. Our BPM module captures business case cycle time, process lead time, throughput (records/sec), stage-by-stage latency share, and real-time SLA breach detection across streaming micro-batches.
10. **How does your architecture support multi-cloud or hybrid-cloud database deployments?**
    * *Defense*: The connection manager (`database/connection.py`) dynamically resolves connection strings and supports instant, zero-downtime switching between Supabase Cloud PostgreSQL and Local PostgreSQL via `config/db_config.json` and REST API endpoints.

---

# PART 22 — RESUME CLAIM VERIFICATION

| Claim | Evidence in Code | Status |
| :--- | :--- | :--- |
| **"Built an End-to-End Data Pipeline using Medallion Architecture"** | Bronze (`source_ingestion.py`), Staging (`staging.py`), Silver 3NF (`silver_pipeline.py`), Gold Star Schema (`gold_pipeline.py`). | `[VERIFIED]` |
| **"Implemented Apache Kafka Streaming & Decoupled Ingestion"** | Kafka producer, consumer, and mock fallback queue in `pipeline/kafka_client.py`. | `[VERIFIED]` |
| **"Orchestrated Workflows using Apache Airflow"** | Production DAG `ipl_production_data_pipeline` with 7 sequential operators in `dags/ipl_pipeline_dag.py`. | `[VERIFIED]` |
| **"Built an Open Lakehouse with Apache Iceberg, Project Nessie, and MinIO"** | Iceberg table `ipl.ipl_matches`, Nessie catalog REST v2, MinIO S3 bucket in `pipeline/iceberg_pipeline.py`. | `[VERIFIED]` |
| **"Implemented Real-Time Change Data Capture (CDC)"** | SHA-256 hash comparison against `cdc_states` with `INSERT`/`UPDATE`/`DELETE`/`SKIP` in `pipeline/cdc.py`. | `[VERIFIED]` |
| **"Designed an 11-Rule Automated Data Quality & Quarantine Engine"** | Quality assertions, quarantine file routing, and `quarantine_records` logging in `pipeline/validation.py`. | `[VERIFIED]` |
| **"Guaranteed Batch Atomicity and Idempotent Execution"** | Single transaction `conn.commit()` / `conn.rollback()` in `pipeline/atomicity.py` and cascading deletes in `silver_pipeline.py`. | `[VERIFIED]` |
| **"Built Real-Time Process Mining & SLA Anomaly Observability"** | Telemetry logging (`event_logger.py`), metric math (`process_metrics.py`), and 4 anomaly detectors (`anomaly_detection.py`). | `[VERIFIED]` |
| **"Developed Stream Processing Application with PySpark"** | PySpark Structured Streaming micro-batch app with watermarking in `bpm/process_stream.py`. | `[VERIFIED]` |
| **"Deployed Containerized Multi-Service Architecture with Docker"** | 7 services (Postgres, Kafka, Zookeeper, Airflow Webserver/Scheduler, MinIO, Nessie) in `docker-compose.yaml`. | `[VERIFIED]` |
| **"Automated CI/CD with GitHub Actions"** | Ephemeral PostgreSQL testing, linting, and automated unit tests in `.github/workflows/ci.yml`. | `[VERIFIED]` |
| **"Built Machine Learning Match Outcome Predictor"** | No ML model training, feature stores, or inference scripts exist in codebase. | `[NOT FOUND]` |
| **"Integrated Generative AI / LLM Chatbots"** | No LLMs, RAG vector stores, or agent frameworks exist in codebase. | `[NOT FOUND]` |

---

# PART 23 — FINAL PROJECT KNOWLEDGE MAP

```text
PROJECT: END-TO-END IPL CRICKET DATA ENGINEERING & PROCESS MINING PLATFORM
│
├── Problem: Unstructured cricket JSONs with anomalies -> Clean 3NF & Star Schema Lakehouse + Observability
├── Architecture: Medallion (Bronze -> Staging -> Silver -> Gold / Lakehouse) + Kafka + Airflow + BPM
├── Technologies: Python 3.11, Kafka, Airflow, Postgres, Iceberg, Nessie, MinIO, PySpark, Flask, Docker
├── Ingestion & Streaming: source_ingestion.py -> kafka_client.py (Topic: ipl_matches) -> staging.py
├── Quality & Governance: validation.py (11 Rules) -> quarantine_records & cdc.py (SHA-256 Deltas)
├── Database Layer: 21 Tables (PostgreSQL 3NF Silver + Gold Star Schema + BPM Telemetry + Control Tables)
├── Lakehouse Layer: Apache Iceberg (ipl.ipl_matches) partitioned by season, Nessie Catalog, MinIO S3
├── Observability (BPM): event_logger.py, process_metrics.py (Cycle/Lead Time, Bottlenecks), anomaly_detection.py
├── Presentation & APIs: Flask Web Portal (:5000), 14 REST Endpoints, Airflow UI (:8080), Power BI (.pbix)
├── Reliability: ACID Transaction Rollback (atomicity.py), Mock Kafka/Spark Fallbacks, Dynamic DB Switcher
└── Testing & CI/CD: 3 Test Suites (tests/), GitHub Actions (ci.yml), Docker Compose (7 Services)
```

### TOP 10 THINGS I MUST KNOW
1. **Medallion Architecture Stages**: Bronze (Raw ZIP) $\rightarrow$ Staging (Partitioned JSON/Parquet) $\rightarrow$ Silver (3NF Relational) $\rightarrow$ Gold (Star Schema OLAP) $\rightarrow$ Lakehouse (Apache Iceberg).
2. **Kafka Streaming Decoupling**: Ingestion produces to `ipl_matches`; consumers stage data; automatic fallback to `mock_kafka_queue.json` when offline.
3. **Change Data Capture (CDC)**: Computes SHA-256 hashes per match against `cdc_states` emitting `INSERT`, `UPDATE`, `DELETE`, or `SKIP`.
4. **11-Rule Data Quality Gate**: Validates structure, over bounds $[0, 19]$, score math ($total = batter + extras$), and outliers ($> 560$ runs); routes bad files to `data/quarantine/{season}/`.
5. **Batch Transaction Atomicity**: Silver and Gold database loading executes inside a single SQL transaction with automatic `ROLLBACK` on error.
6. **Idempotent Data Design**: Deleting existing matches prior to reload (`ON DELETE CASCADE`) prevents duplicate deliveries and keys.
7. **Canonical Franchise Mapping**: `clean_team_name()` normalizes historical rebrandings (`Delhi Daredevils` $\rightarrow$ `Delhi Capitals`).
8. **Apache Iceberg Lakehouse Architecture**: Partitioned by `season`, cataloged in Project Nessie REST v2, stored on MinIO S3 with checkpoint recovery.
9. **BPM Process Observability**: Telemetry events calculate Cycle Time, Lead Time, Throughput, Bottlenecks, and 4 anomaly categories.
10. **Dual Database & Dual Execution Engine**: Supports Supabase Cloud vs. Local PostgreSQL and Airflow DAG vs. Flask async background runner.

### TOP 10 FILES I MUST STUDY
1. [`pipeline/atomicity.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/atomicity.py): Understand ACID transaction management, commit/rollback, and Silver/Gold loading.
2. [`pipeline/validation.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/validation.py): Understand the 11 data quality rules and quarantine isolation mechanics.
3. [`pipeline/cdc.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/cdc.py): Understand SHA-256 delta detection and operation logging.
4. [`pipeline/kafka_client.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/kafka_client.py): Understand Kafka producer/consumer streaming and the mock queue fallback.
5. [`pipeline/iceberg_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/iceberg_pipeline.py): Understand Iceberg table creation, Nessie cataloging, MinIO S3 writes, and checkpoints.
6. [`pipeline/gold_pipeline.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/pipeline/gold_pipeline.py): Understand Star Schema SQL queries aggregating `FACT_MATCH_SUMMARY`, `DIM_PLAYER`, and `DIM_TEAM`.
7. [`bpm/anomaly_detection.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/bpm/anomaly_detection.py): Understand the 4 process anomaly detection algorithms (SLA, Sequence, Duplicate, Deviation).
8. [`dags/ipl_pipeline_dag.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/dags/ipl_pipeline_dag.py): Understand Airflow DAG structure, task sequencing, and XCom state passing.
9. [`database/schema.sql`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/database/schema.sql): Understand all 21 table schemas, primary/foreign keys, and relational constraints.
10. [`routes/api.py`](file:///c:/Sk%20PC/COLLEGE/Sem%209/DE%20lab/IPL/End%20to%20End%20Cricket%20Data%20Engineering/routes/api.py): Understand REST endpoints, dynamic SQL query generation, database switching, and monitoring APIs.

### TOP 10 NUMBERS / FACTS I MUST REMEMBER
1. **11**: Number of automated Data Quality validation rules evaluated per match.
2. **21**: Total number of relational database tables defined in PostgreSQL `schema.sql`.
3. **4**: Core operational CDC states emitted (`INSERT`, `UPDATE`, `DELETE`, `SKIP`).
4. **4**: Process mining anomaly categories detected (SLA Breaches, Out-of-Sequence, Duplicates, Process Deviations).
5. **19**: Number of structured schema columns mapped into the Apache Iceberg Lakehouse table.
6. **7**: Docker Compose microservices running in the containerized infrastructure.
7. **5439**: Custom port allocated to Airflow PostgreSQL to prevent conflict with local PostgreSQL on 5432.
8. **19120**: REST API v2 port for Project Nessie Iceberg catalog server.
9. **9000 / 9001**: MinIO S3 object storage API and Web Console ports.
10. **2008–2024**: Full historical range of IPL tournament seasons supported and backfilled.
