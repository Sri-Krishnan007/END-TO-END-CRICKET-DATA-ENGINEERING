# 🏏 End-to-End IPL Cricket Data Engineering Platform

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE.svg?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.3.0-231F20.svg?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Flask](https://img.shields.io/badge/Flask-3.0+-000000.svg?logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker&logoColor=white)](https://www.docker.com/)

A production-grade, enterprise Data Engineering platform that ingests, streams, validates, processes, and serves historical and ball-by-ball Indian Premier League (IPL) cricket match data using a **Medallion Architecture**, **Apache Kafka messaging**, **Apache Airflow orchestration**, **PostgreSQL OLAP Data Warehouse**, and an interactive **Flask Web Monitoring Portal**.

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
- [Data Warehouse & Schema Design](#-data-warehouse--schema-design)
  - [Pipeline Control Tables](#pipeline-control-tables)
  - [Silver Layer (3NF Normalized)](#silver-layer-3nf-normalized)
  - [Gold Layer (Star Schema / OLAP)](#gold-layer-star-schema--olap)
- [Technology Stack](#-technology-stack)
- [Project Directory Structure](#-project-directory-structure)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation & Environment Setup](#installation--environment-setup)
  - [Database Initialization](#database-initialization)
  - [Running with Docker Compose](#running-with-docker-compose)
  - [Running the Flask Monitoring Portal](#running-the-flask-monitoring-portal)
- [Pipeline Execution](#-pipeline-execution)
  - [Via Airflow Orchestrator](#via-airflow-orchestrator)
  - [Via CLI / Python Runner](#via-cli--python-runner)
  - [Via Web Dashboard](#via-web-dashboard)
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
        F -->|Write Staging JSON / Parquet| G[data/staging/{season}/]
        G -->|11 Data Quality Rules| H{Quality Gate}
        H -->|Fail Quality Rule| I[data/quarantine/{season}/]
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

    subgraph Serving & UI [Monitoring & Analytics]
        R --> S[Flask Web Portal / Analytics Dashboard]
        R --> T[Power BI / BI Reports]
        N --> U[Airflow DAG Monitoring UI]
    end

    style C fill:#f9d5e5,stroke:#333
    style J fill:#fcc,stroke:#333
    style N fill:#f9d5e5,stroke:#333
    style P fill:#d4edda,stroke:#333
    style R fill:#c3e6cb,stroke:#28a745,stroke-width:2px
```

---

## 🔄 End-to-End Pipeline Workflow

The pipeline executes through sequential, monitored stages orchestrated by **Apache Airflow**:

```
[Start] ➡️ [Bronze Ingestion] ➡️ [Kafka Producer] ➡️ [Kafka Consumer] ➡️ [Data Validation] ➡️ [CDC Detection] ➡️ [Atomic Silver/Gold Load] ➡️ [End]
```

1. **Bronze Ingestion:** Downloads raw data from Cricsheet, generates file checksums, registers source metadata in `source_registry`.
2. **Kafka Producer:** Reads raw match documents and streams them as serialized JSON payloads into the `ipl_matches` Kafka topic.
3. **Kafka Consumer:** Reads topic messages, extracts JSON records, partitions them into season-based staging directories (`data/staging/{season}/json/`), and generates optimized Parquet storage.
4. **Data Quality Validation:** Executes 11 automated rule checks on each match. Corrupted or invalid files are quarantined in `data/quarantine/{season}/` and registered in `quarantine_records`.
5. **Change Data Capture (CDC):** Compares computed record SHA-256 hashes against stored state hashes in `cdc_states`. Matches are classified as `INSERT`, `UPDATE`, `DELETE`, or `SKIP`.
6. **Silver Layer Loading:** Normalizes validated match data into relational 3NF tables (`match`, `innings`, `delivery`, `wicket`, `player`, `team`, `official`, etc.).
7. **Gold Layer Aggregation:** Computes player stats, team win/loss records, boundary counts, economy rates, and match aggregates into the Star Schema (`FACT_MATCH_SUMMARY`, `DIM_PLAYER`, `DIM_TEAM`).
8. **Batch Atomicity:** All SQL executions for a batch run within an ACID-compliant PostgreSQL transaction block with complete automatic rollback on any failure.

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

---

## 💻 Technology Stack

| Component | Technology | Purpose |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 2.9.3 | Workflow scheduling, DAG execution, retry management |
| **Message Broker** | Apache Kafka 7.3.0 & Zookeeper | Asynchronous message transport and decoupled streaming |
| **Storage & Warehouse** | PostgreSQL 13+ | ACID relational Silver store and Star Schema Gold OLAP |
| **File Formats** | JSON, Apache Parquet (PyArrow) | Bronze raw and Staging columnar storage |
| **Transformation Engine** | Python 3.11 / Pandas | Data cleansing, aggregation, and quality validation |
| **Web Dashboard** | Flask 3.0, HTML5, CSS3, Chart.js | Interactive control portal and live pipeline monitor |
| **Containerization** | Docker & Docker Compose | Containerized multi-service deployment |

---

## 📁 Project Directory Structure

```
End to End Cricket Data Engineering/
├── config/
│   └── config.py                 # Central configurations, paths, and DB URLs
├── dags/
│   └── ipl_pipeline_dag.py       # Apache Airflow Production DAG definition
├── database/
│   ├── connection.py             # PostgreSQL connection pool & helpers
│   └── schema.sql                # Complete DDL for Control, Silver, and Gold tables
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
│   ├── eda.py                    # Exploratory data analysis helpers
│   └── pipeline_runner.py        # Central CLI pipeline runner
├── routes/                       # Flask Web Portal routes & API endpoints
│   ├── home.py
│   ├── ingestion.py
│   ├── pipeline.py
│   ├── analytics.py
│   └── api.py
├── static/                       # CSS stylesheets, UI assets, and client JS
│   └── css/style.css
├── templates/                    # Jinja2 HTML templates for monitoring portal
├── tests/
│   └── test_pipeline.py          # Unit and integration test suites
├── app.py                        # Flask Application Entry Point
├── docker-compose.yaml           # Multi-container orchestration (Airflow, Kafka, Postgres)
├── requirements.txt              # Python package dependencies
├── .gitignore                    # Git ignore specifications
└── README.md                     # Project documentation
```

---

## 🚀 Getting Started

### Prerequisites
* **Python 3.11+** installed
* **PostgreSQL 13+** running locally or via Docker
* **Docker & Docker Compose** (optional for containerized setup)

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
   Set your PostgreSQL connection string:
   ```bash
   # Windows PowerShell
   $env:DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres"
   
   # Linux/macOS
   export DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres"
   ```

### Database Initialization

Apply the database schema to your PostgreSQL instance:
```bash
python -c "from database.connection import init_db; init_db()"
```
*Or execute `database/schema.sql` directly in your PostgreSQL client (pgAdmin, DBeaver, psql).*

---

### 🐳 Running with Docker Compose

To spin up the entire containerized stack (**Airflow Webserver, Scheduler, PostgreSQL, Kafka, Zookeeper**):

```bash
docker-compose up -d
```

Access the web interfaces:
* **Airflow Web UI:** [http://localhost:8080](http://localhost:8080) (Credentials: `admin` / `admin`)
* **Kafka Broker:** `localhost:9092`
* **PostgreSQL (Airflow Metadata):** `localhost:5439`

---

### 🌐 Running the Flask Monitoring Portal

Launch the interactive monitoring dashboard:

```bash
python app.py
```

Open your browser and navigate to:
* **Dashboard:** [http://127.0.0.1:5000](http://127.0.0.1:5000)
* **Pipeline Monitor:** [http://127.0.0.1:5000/pipeline](http://127.0.0.1:5000/pipeline)
* **Data Quality & Quarantine:** [http://127.0.0.1:5000/errors](http://127.0.0.1:5000/errors)
* **Gold Analytics:** [http://127.0.0.1:5000/analytics](http://127.0.0.1:5000/analytics)

---

## ⚡ Pipeline Execution

### Via Airflow Orchestrator
1. Open the Airflow UI at `http://localhost:8080`.
2. Unpause the `ipl_production_data_pipeline` DAG.
3. Trigger the DAG with custom configuration parameters:
   ```json
   {
     "seasons": [2024, 2023],
     "reprocess": false
   }
   ```

### Via CLI / Python Runner
Execute a specific season or full historical pipeline directly:
```bash
# Run Season 2024
python pipeline/pipeline_runner.py --season 2024

# Run with full reprocess mode
python pipeline/pipeline_runner.py --season 2024 --reprocess
```

---

## 🧪 Testing

Run the test suite to verify pipeline integrity:
```bash
pytest tests/
```

---

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
