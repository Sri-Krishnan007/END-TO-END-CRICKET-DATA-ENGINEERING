# 🏏 End-to-End IPL Cricket Data Engineering Platform

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE.svg?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.3.0-231F20.svg?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-Table%20Format-blue.svg?logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![Project Nessie](https://img.shields.io/badge/Project%20Nessie-Catalog%20v1-green.svg?logo=git&logoColor=white)](https://projectnessie.org/)
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Storage-red.svg?logo=minio&logoColor=white)](https://min.io/)
[![Trino](https://img.shields.io/badge/Trino-Distributed%20SQL-yellow.svg?logo=trino&logoColor=white)](https://trino.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Flask](https://img.shields.io/badge/Flask-3.0+-000000.svg?logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)

A production-grade, enterprise Data Engineering & Lakehouse platform that ingests, streams, validates, processes, and serves historical and ball-by-ball Indian Premier League (IPL) cricket match data using a **Medallion Architecture**, **Apache Kafka streaming**, **Apache Airflow orchestration**, an **Open Lakehouse (Apache Iceberg + Project Nessie + MinIO S3 + Trino)**, **PostgreSQL OLAP Data Warehouse**, **Business Process Mining (BPM) SLA governance**, and an interactive **Flask Web Monitoring Portal**.

---

## 📑 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [End-to-End Pipeline Workflow](#-end-to-end-pipeline-workflow)
- [Core Data Engineering Principles](#-core-data-engineering-principles)
  - [1. Medallion Architecture](#1-medallion-architecture)
  - [2. Kafka Streaming & Decoupled Ingestion](#2-kafka-streaming--decoupled-ingestion)
  - [3. Change Data Capture (CDC Delta Detection)](#3-change-data-capture-cdc-delta-detection)
  - [4. Data Quality & 11-Rule Quarantine Isolation](#4-data-quality--11-rule-quarantine-isolation)
  - [5. Idempotency & Batch Atomicity](#5-idempotency--batch-atomicity)
  - [6. Open Lakehouse & Trino Serving](#6-open-lakehouse--trino-serving)
  - [7. Business Process Mining (BPM) & SLA Governance](#7-business-process-mining-bpm--sla-governance)
- [Data Warehouse & Schema Design](#-data-warehouse--schema-design)
  - [Pipeline Control Tables](#pipeline-control-tables)
  - [Silver Layer (3NF Normalized)](#silver-layer-3nf-normalized)
  - [Gold Layer (Star Schema / OLAP)](#gold-layer-star-schema--olap)
  - [Lakehouse Iceberg Table Specification](#lakehouse-iceberg-table-specification)
- [Technology Stack](#-technology-stack)
- [Project Directory Structure](#-project-directory-structure)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation & Environment Setup](#installation--environment-setup)
  - [Database Initialization](#database-initialization)
  - [Running with Docker Compose](#running-with-docker-compose)
  - [Running the Flask Monitoring Portal](#running-the-flask-monitoring-portal)
- [Pipeline & Lakehouse Execution](#-pipeline--lakehouse-execution)
  - [Via Airflow Orchestrator](#via-airflow-orchestrator)
  - [Via CLI Runner](#via-cli-runner)
  - [Trino Lakehouse Verification & Benchmarks](#trino-lakehouse-verification--benchmarks)
- [CI/CD & Automated Testing](#-cicd--automated-testing)
  - [Running Tests (40/40 Passing)](#running-tests-4040-passing)
  - [GitHub Actions Workflow](#github-actions-workflow)
- [License](#-license)

---

## 🏗 Architecture Overview

```mermaid
flowchart TD
    subgraph Bronze Layer [Raw Ingestion Layer]
        A[Cricsheet Raw JSON / ZIP] -->|SHA-256 Ingestion Check| B(Bronze Ingestor)
        B -->|Audit Metadata| C[(source_registry)]
    end

    subgraph Messaging Layer [Apache Kafka Streaming]
        B -->|Publish JSON Stream| D[Kafka Producer]
        D -->|ipl_matches Topic| E(Kafka Broker / Offline Queue Fallback)
        E -->|Poll & Partition by Season| F[Kafka Consumer]
    end

    subgraph Staging & Quality [Data Staging & Quarantine]
        F -->|Write Staging JSON / Parquet| G["data/staging/{season}/"]
        G -->|11 Data Quality Rules| H{Quality Gate}
        H -->|Fail Quality Rule| I["data/quarantine/{season}/"]
        I -->|Log Isolation Reason| J[(quarantine_records)]
        H -->|Pass Quality Gate| K[CDC Delta Detection Engine]
    end

    subgraph Incremental Transformation [CDC, Silver & Gold Warehouse]
        K -->|SHA-256 Hash Comparison| L{State Changed?}
        L -->|No Change| M[Skip Processing]
        L -->|INSERT / UPDATE / DELETE| N[(cdc_log & cdc_states)]
        N -->|Atomic SQL Transaction| O[Silver Layer: 3NF Normalized SQL]
        O -->|Match, Innings, Delivery, Wickets, Players, Teams| P[(PostgreSQL Silver Tables)]
        P -->|Aggregate & Compute Metrics| Q[Gold Layer: Star Schema OLAP]
        Q -->|Upsert Facts & Dimensions| R[(FACT_MATCH_SUMMARY / DIM_PLAYER / DIM_TEAM)]
    end

    subgraph Lakehouse Layer [Apache Iceberg + Nessie + MinIO S3]
        G -->|PySpark Structured Streaming Sink| LH1[Apache Iceberg: ipl.ipl_matches]
        LH1 -->|Catalog Metadata & Git Commits| LH2[Project Nessie REST Catalog :19120]
        LH1 -->|Parquet Data & Snapshots| LH3[MinIO S3 Object Storage :9000]
    end

    subgraph Serving & Optimization [Trino Query Engine & Airflow Maintenance]
        LH1 -->|Distributed SQL Engine| TR[Trino Query Engine :8085]
        TR --> S[Flask Web Portal / Lakehouse Dashboard]
        R --> S
        R --> T[Power BI / BI Repo]
        AF[Airflow Maintenance DAG] -->|OPTIMIZE, Expire Snapshots, Vacuum| TR
        BPM[BPM Conformance Engine] -->|Token Replay & SLA Audit| S
    end
```

---

## 🔄 End-to-End Pipeline Workflow

The complete enterprise pipeline executes through monitored stages orchestrated by **Apache Airflow**:

```
[Start] ➡️ [Bronze Ingestion] ➡️ [Kafka Producer] ➡️ [Kafka Consumer] ➡️ [Data Validation] ➡️ [CDC Detection] ➡️ [Atomic Silver/Gold Load] ➡️ [Iceberg Lakehouse Sync] ➡️ [Trino Serving] ➡️ [Lakehouse Maintenance] ➡️ [End]
```

1. **Bronze Ingestion:** Downloads raw data from Cricsheet, generates file checksums, registers source metadata in `source_registry`.
2. **Kafka Producer:** Reads raw match documents and streams them as serialized JSON payloads into the `ipl_matches` Kafka topic.
3. **Kafka Consumer:** Reads topic messages, extracts JSON records, partitions them into season-based staging directories (`data/staging/{season}/json/`), and generates optimized Parquet storage.
4. **Data Quality Validation:** Executes 11 automated rule checks on each match. Corrupted or invalid files are quarantined in `data/quarantine/{season}/` and registered in `quarantine_records`.
5. **Change Data Capture (CDC):** Compares computed record SHA-256 hashes against stored state hashes in `cdc_states`. Matches are classified as `INSERT`, `UPDATE`, `DELETE`, or `SKIP`.
6. **Silver Layer Loading:** Normalizes validated match data into relational 3NF tables (`match`, `innings`, `delivery`, `wicket`, `player`, `team`, `official`, etc.).
7. **Gold Layer Aggregation:** Computes player stats, team win/loss records, boundary counts, economy rates, and match aggregates into the Star Schema (`FACT_MATCH_SUMMARY`, `DIM_PLAYER`, `DIM_TEAM`).
8. **Lakehouse Streaming Sink:** Writes processed matches to Apache Iceberg table `ipl.ipl_matches` registered in Project Nessie and stored in MinIO S3.
9. **Distributed SQL Serving:** Trino queries the Iceberg table over port `8085` using partition pruning on `season`, delivering 6.5x faster analytical responses.
10. **Automated Lakehouse Maintenance:** Airflow DAG executes file compaction, snapshot expiration, and orphan file removal to eliminate storage bloat.

---

## 💡 Core Data Engineering Principles

### 1. Medallion Architecture
* **Bronze (Raw):** Source zip packages and raw extracted match files stored with provenance metadata.
* **Staging:** Partitioned JSON and columnar Parquet files separated by tournament season.
* **Silver (Cleaned & Normalized):** Highly structured 3NF relational database schema modeling every delivery, dismissal, powerplay, and squad.
* **Gold (Serving & Analytics):** Dimensional Star Schema optimized for sub-second analytical queries and BI dashboards.

### 2. Kafka Streaming & Decoupled Ingestion
* Decouples raw ingestion from downstream compute.
* Prevents data loss during network disruptions or disk I/O bottlenecks.
* Includes an automatic **Offline Mock Queue Fallback** (`logs/mock_kafka_queue.json`) ensuring uninterrupted local development when external Kafka brokers are unavailable.

### 3. Change Data Capture (CDC Delta Detection)
* Computes cryptographic SHA-256 hashes for every incoming match record.
* Avoids expensive full warehouse reprocessing by only executing operations on altered data:
  * **New Match:** Emits `INSERT`
  * **Modified Record:** Emits `UPDATE` (re-indexes deliveries and metrics)
  * **Removed File:** Emits `DELETE` (cascades database deletes)
  * **Unchanged Record:** Emits `SKIP` (zero DB overhead)

### 4. Data Quality & 11-Rule Quarantine Isolation
Matches undergo 11 data quality assertions before entering the warehouse:
1. Valid JSON schema & required top-level metadata keys (`info`, `innings`).
2. Valid tournament format (IPL / Twenty20).
3. Legitimate season year and match dates.
4. Team validation (at least 2 registered teams per match).
5. Non-empty innings and legal delivery overs (0 to 20).
6. Non-negative delivery runs and valid extras breakdown.
7. Ball count integrity (max 6 legal deliveries per standard over).
8. Wicket and dismissal consistency.
9. Player consistency (batters and bowlers present in squad list).
10. Valid toss decisions and winner resolutions.
11. Venue and match official verification.

*Failing records are isolated into `data/quarantine/` with detailed audit logs in PostgreSQL.*

### 5. Idempotency & Batch Atomicity
* **Idempotent Pipelines:** Running the pipeline $N$ times with the same input produces identical state with zero duplicate keys or double-counted deliveries.
* **Atomic Transactions:** DB loader operations execute inside a single SQL transaction. If an error occurs midway, PostgreSQL executes `ROLLBACK`, guaranteeing zero orphan records.

### 6. Open Lakehouse & Trino Serving
* **Apache Iceberg Table Format:** ACID transactions, hidden partitioning by `season`, snapshot-based time travel, and zero-copy schema evolution.
* **Project Nessie Catalog:** Git-for-data REST API (`http://localhost:19120/api/v1`) providing commit isolation, multi-table transactions, and branch management.
* **MinIO Object Storage:** High-performance S3-compatible warehouse bucket `s3://warehouse/` (`http://localhost:9000`).
* **Trino Distributed SQL Engine:** Massively parallel ANSI SQL engine listening on port `8085` (`trino/catalog/iceberg.properties`), executing distributed scans with column projection and dictionary pruning.
* **Dual-Mode Client:** Python client (`pipeline/trino_client.py`) checks socket availability in <0.2s, seamlessly querying Trino online and falling back to verified lakehouse Parquet storage when offline.

### 7. Business Process Mining (BPM) & SLA Governance
* **Alpha & Heuristic Process Mining:** Constructs formal Petri Nets and transition models directly from `bpm_event_logs`.
* **Token Replay Conformance:** Calculates trace fitness scores against normative process flows.
* **Automated SLA Anomaly Auditing:** Continuously monitors pipeline stages for SLA breaches, execution bottlenecks, duplicate activities, and out-of-order executions.

---

## 🗄 Data Warehouse & Schema Design

### Pipeline Control Tables
| Table | Description |
| :--- | :--- |
| `source_registry` | Tracks downloaded raw source files, checksums, and sizes |
| `pipeline_control` | Per-season tracking of Bronze, Staging, Validation, CDC, Silver, and Gold completion |
| `pipeline_runs` | Logs execution run IDs, runtime durations, status, and record counts |
| `pipeline_logs` | Granular log entries for every task execution |
| `cdc_states` | Current SHA-256 hash per match business key |
| `cdc_log` | Historical audit trail of all CDC `INSERT`/`UPDATE`/`DELETE` operations |
| `quarantine_records` | Audit log of rejected matches with violation reasons and raw data |
| `bpm_event_logs` | Event log table recording stage execution timestamps, durations, and status for process mining |

### Silver Layer (3NF Normalized)
* `match` (Match metadata, venue, toss, winner, margin, result)
* `team` (Canonical team master list with historical alias mapping)
* `player` (Player master registry with Cricsheet IDs)
* `official` (Umpires and match referees)
* `innings` (Innings number, target runs, overs)
* `delivery` (Ball-by-ball granularity: over, ball, batter, bowler, runs, extras, wides, no-balls)
* `wicket` (Dismissals, dismissal kind, dismissed player)
* `wicket_fielder` (Catch, runout, stumping fielders)
* `powerplay` (Overs range and powerplay type)
* `toss`, `player_of_match`, `match_official`, `team_squad`

### Gold Layer (Star Schema / OLAP)
* **`FACT_MATCH_SUMMARY`**: Central fact table storing match metrics (total runs, wickets, boundaries, sixes, margins).
* **`DIM_TEAM`**: Team dimensional table with aggregates (matches played, wins, total runs, boundaries).
* **`DIM_PLAYER`**: Player dimensional table with career statistics (innings, runs, balls, 4s, 6s, 50s, 100s, wickets, overs, economy, best bowling figures).

### Lakehouse Iceberg Table Specification
* **Namespace / Table:** `iceberg.ipl.ipl_matches`
* **Storage Location:** `s3://warehouse/ipl/ipl_matches`
* **Format:** Apache Iceberg Format Version 2 (Parquet data files)
* **Partitioning:** Identity transform on `season` (Hidden Partitioning)
* **Catalog:** Project Nessie (`http://nessie:19120/api/v1`)

---

## 💻 Technology Stack

| Component | Technology | Host Port / Interface | Purpose |
| :--- | :--- | :--- | :--- |
| **Workflow Orchestration** | Apache Airflow 2.9.3 | `http://localhost:8080` | DAG scheduling, task retries, automated table maintenance |
| **Distributed SQL Engine** | Trino 442+ | `http://localhost:8085` | Massively parallel ANSI SQL queries on Iceberg tables |
| **Lakehouse Catalog** | Project Nessie 0.82.0 | `http://localhost:19120` | Git-like transactional catalog for Iceberg |
| **Object Storage** | MinIO S3 | `http://localhost:9000` (API) / `:9001` (UI) | S3-compatible Lakehouse warehouse storage |
| **Message Streaming** | Apache Kafka 7.3.0 | `localhost:9092` | Asynchronous message transport and decoupled streaming |
| **Data Warehouse** | PostgreSQL 13+ | `localhost:5439` | ACID relational Silver store and Star Schema Gold OLAP |
| **Open Table Format** | Apache Iceberg v2 | Columnar Parquet | ACID Lakehouse table format with snapshot time-travel |
| **Process Mining Engine** | Custom BPM Engine | In-Memory / SQL | Conformance checking, token replay, SLA anomaly auditing |
| **Web Portal & Dashboard** | Flask 3.0 / Chart.js | `http://127.0.0.1:5000` | Real-time monitoring, live Trino console, Lakehouse charts |
| **Containerization** | Docker & Docker Compose | Multi-container stack | Unified infrastructure deployment |

---

## 📁 Project Directory Structure

```
End to End Cricket Data Engineering/
├── bpm/                          # Business Process Mining & Governance Engine
│   ├── __init__.py
│   ├── config.py                 # Stage SLAs, transition matrices, allowed retries
│   ├── conformance.py            # Token replay & fitness score computation
│   ├── event_log.py              # Event logging and transition sequence validation
│   └── process_model.py          # Alpha Miner & Petri Net graph definitions
├── config/
│   └── config.py                 # Central configurations, paths, DB URLs, MinIO/Trino settings
├── dags/
│   ├── ipl_pipeline_dag.py       # Apache Airflow Production Pipeline DAG
│   └── lakehouse_maintenance_dag.py # Airflow Automated Lakehouse Maintenance DAG
├── database/
│   ├── connection.py             # PostgreSQL connection pool & helpers
│   └── schema.sql                # Complete DDL for Control, Silver, Gold, and BPM tables
├── data/                         # Medallion data directory
│   ├── bronze/                   # Raw ingested source archives
│   ├── staging/                  # Partitioned JSON & Parquet files by season
│   ├── silver/                   # Processed normalized data
│   ├── gold/                     # Serving layer data extracts
│   └── quarantine/               # Isolated corrupted match records
├── pipeline/
│   ├── source_ingestion.py       # Bronze source downloader & registry
│   ├── kafka_client.py           # Kafka Producer & Consumer implementations
│   ├── staging.py                # Staging extraction & Parquet conversions
│   ├── validation.py             # 11-Rule Data Quality Engine & Quarantine router
│   ├── cdc.py                    # SHA-256 Change Data Capture Engine
│   ├── silver_pipeline.py        # 3NF relational data builder
│   ├── gold_pipeline.py          # Star Schema dimension & fact builder
│   ├── atomicity.py              # Transactional atomic loader with rollback
│   ├── backfill.py               # Historical season backfill manager
│   ├── trino_client.py           # Dual-mode Trino distributed query client & fallback
│   ├── lakehouse_maintenance.py  # Compaction, snapshot expiry, orphan cleanup automation
│   ├── benchmark_lakehouse.py    # Trino Lakehouse vs Postgres benchmark suite
│   ├── verify_trino.py           # Verification script for Trino & Iceberg tables
│   ├── eda.py                    # Exploratory data analysis helpers
│   └── pipeline_runner.py        # Central CLI pipeline runner
├── routes/                       # Flask Web Portal routes & API endpoints
│   ├── home.py                   # Landing page
│   ├── ingestion.py              # Source download & checksums
│   ├── pipeline.py               # Pipeline execution controller & monitor
│   ├── analytics.py              # Gold Star Schema charts & player stats
│   ├── lakehouse.py              # Lakehouse serving dashboard & Trino console API
│   ├── bpm.py                    # BPM process mining & SLA audit view
│   └── api.py                    # JSON endpoints for real-time polling
├── templates/                    # Jinja2 HTML templates
│   ├── index.html
│   ├── pipeline.html
│   ├── pipeline_monitor.html
│   ├── analytics.html
│   ├── lakehouse.html            # Trino Lakehouse Serving & Live SQL Console
│   ├── bpm.html                  # Process Mining & SLA Compliance Dashboard
│   └── summary.html              # Warehouse Table Inspector
├── tests/                        # Comprehensive automated test suites (40/40 Passing)
│   ├── test_pipeline.py          # Data ingestion, Kafka, CDC, atomicity, 11 quality rules
│   ├── test_bpm.py               # BPM event logging, token replay, SLA breach audit
│   ├── test_iceberg_lakehouse.py # Iceberg PySpark/schema validation
│   ├── test_trino_lakehouse.py   # Trino dual-mode client & query generation
│   └── test_lakehouse_maintenance.py # Compaction, snapshot pruning, orphan cleanup
├── trino/catalog/
│   └── iceberg.properties        # Trino Iceberg connector config (Nessie + MinIO)
├── app.py                        # Flask Application Entry Point
├── docker-compose.yaml           # Multi-container orchestration (Airflow, Kafka, Postgres, Nessie, MinIO, Trino)
├── requirements.txt              # Python package dependencies
└── README.md                     # Project documentation
```

---

## 🚀 Getting Started

### Prerequisites
* **Python 3.11+** installed
* **Docker & Docker Compose** (version 2.20+)
* **PostgreSQL 13+** (runs automatically via Docker on port `5439`)

### Installation & Environment Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Sri-Krishnan007/END-TO-END-CRICKET-DATA-ENGINEERING.git
   cd END-TO-END-CRICKET-DATA-ENGINEERING
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On Linux/macOS:
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Environment Variables:**
   ```bash
   # Windows PowerShell
   $env:DATABASE_URL="postgresql://postgres:password@localhost:5439/postgres"
   
   # Linux/macOS
   export DATABASE_URL="postgresql://postgres:password@localhost:5439/postgres"
   ```

### Database Initialization

Apply the database schema to your PostgreSQL instance:
```bash
python -c "from database.connection import init_db; init_db()"
```

---

### 🐳 Running with Docker Compose

To spin up the entire containerized enterprise stack:

```bash
docker compose up -d
```

#### Services & Web Endpoints:
| Service | URL / Port | Credentials / Purpose |
| :--- | :--- | :--- |
| **Airflow Webserver** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **Trino Query Engine** | [http://localhost:8085](http://localhost:8085) | Distributed ANSI SQL Engine |
| **Project Nessie Catalog** | [http://localhost:19120](http://localhost:19120) | REST Catalog API v1 |
| **MinIO Web Console** | [http://localhost:9001](http://localhost:9001) | `admin` / `password123` |
| **MinIO S3 API** | `localhost:9000` | S3 Warehouse Bucket |
| **PostgreSQL** | `localhost:5439` | `postgres` / `password` |
| **Kafka Broker** | `localhost:9092` | Streaming Message Broker |

---

### 🌐 Running the Flask Monitoring Portal

Launch the interactive monitoring and serving dashboard:

```bash
python app.py
```

Open your browser and navigate to:
* **Home Dashboard:** [http://127.0.0.1:5000](http://127.0.0.1:5000)
* **Pipeline Cockpit:** [http://127.0.0.1:5000/pipeline](http://127.0.0.1:5000/pipeline)
* **Lakehouse & Trino Serving:** [http://127.0.0.1:5000/lakehouse](http://127.0.0.1:5000/lakehouse)
* **Process Mining & BPM:** [http://127.0.0.1:5000/bpm](http://127.0.0.1:5000/bpm)
* **Gold Star Schema Analytics:** [http://127.0.0.1:5000/analytics](http://127.0.0.1:5000/analytics)
* **Warehouse Table Inspector:** [http://127.0.0.1:5000/summary](http://127.0.0.1:5000/summary)

---

## ⚡ Pipeline & Lakehouse Execution

### Via Airflow Orchestrator
1. Open the Airflow UI at `http://localhost:8080`.
2. Unpause the `ipl_production_data_pipeline` DAG for batch ETL.
3. Unpause the `lakehouse_maintenance_dag` DAG for automated Iceberg file compaction and snapshot retention.

### Via CLI Runner
Execute a specific season or full historical pipeline directly:
```bash
# Run Season 2024
python pipeline/pipeline_runner.py --season 2024

# Run with reprocess mode
python pipeline/pipeline_runner.py --season 2024 --reprocess
```

### Trino Lakehouse Verification & Benchmarks
```bash
# Verify Trino connectivity and query the Iceberg table
python pipeline/verify_trino.py

# Run Lakehouse maintenance (Compaction, snapshot expiry, orphan cleanup)
python pipeline/lakehouse_maintenance.py

# Run Lakehouse vs PostgreSQL performance benchmarks
python pipeline/benchmark_lakehouse.py
```

---

## 🔄 CI/CD & Automated Testing

### Running Tests (40/40 Passing)
The platform includes 40 comprehensive unit and integration tests across 5 test suites:

```bash
pytest -v
```

**Test Suite Coverage:**
* `tests/test_pipeline.py`: Ingestion checksums, Kafka streaming, 11-rule quality gate, CDC delta hashes, atomicity rollback.
* `tests/test_bpm.py`: Event logging, transition sequence validation, token replay conformance, SLA breach audits.
* `tests/test_iceberg_lakehouse.py`: Iceberg schema validation, partition transforms, streaming metadata.
* `tests/test_trino_lakehouse.py`: Trino dual-mode client, socket health-checks, SQL query builders.
* `tests/test_lakehouse_maintenance.py`: Automated file compaction, snapshot expiration, orphan file removal.

### GitHub Actions Workflow
On every push and pull request to `main` and `dev`, `.github/workflows/ci.yml` automatically validates:
1. Python syntax & code quality (Flake8).
2. Database DDL & schema migrations on an ephemeral PostgreSQL container.
3. Automated test execution across all 40 tests.

---

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
