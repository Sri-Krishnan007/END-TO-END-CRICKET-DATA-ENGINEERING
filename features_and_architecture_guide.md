# IPL Data Engineering Platform - Architecture & Features Guide

This guide details the complete architecture of the **End-to-End IPL Cricket Data Engineering Platform**, explaining **why** each technology and design pattern was chosen, and how data flows through the pipeline.

---

## 1. The Technology Stack & "Why We Used It"

### 🛡️ Apache Kafka (Message Queue & Transport Layer)
*   **Why we used it:** In production data pipelines, writing raw files directly from the ingestion zip to the staging folder is a bad practice. If the network drops or the disk fills up, the entire run fails. Kafka decouples **Ingestion (Producer)** from **Staging (Consumer)**.
*   **How it works:** 
    *   The **Producer** reads match files from the source ZIP and streams them as serialized JSON payloads to the `ipl_matches` topic.
    *   The **Consumer** listens to the topic, deserializes the payload, and saves them to the season directories.
    *   **Resiliency Fallback:** If the Kafka broker is offline, the client automatically falls back to an offline simulated file queue (`mock_kafka_queue.json`) to keep the system running.

### ⚙️ Apache Airflow (Workflow Orchestration)
*   **Why we used it:** Running data pipelines using basic Python cron jobs or custom background threads lacks tracking, monitoring, and error handling. Airflow serves as the enterprise orchestration engine.
*   **How it works:**
    *   It structures the stages (Ingest $\rightarrow$ Kafka Produce $\rightarrow$ Kafka Consume $\rightarrow$ Validate $\rightarrow$ CDC $\rightarrow$ DB Atomic Load) into a **Directed Acyclic Graph (DAG)**.
    *   If a task fails (e.g., the DB goes offline), Airflow automatically retries the task after a delay instead of crashing.
    *   It provides a web UI to view task logs, manage schedules, and run backfills.

### 🐘 PostgreSQL (Relational Data Warehouse & OLAP Serving Layer)
*   **Why we used it:** We need a structured relational database that supports transactions (ACID properties) for Silver relational tables and Star Schema (Fact/Dimension) Gold OLAP tables.
*   **How it works:** 
    *   Stores normalized matches, deliveries, wickets, players, and teams in the Silver layer.
    *   Aggregates analytical summaries (e.g. batting/bowling statistics) into the serving Gold layer (`FACT_MATCH_SUMMARY`, `DIM_PLAYER`, `DIM_TEAM`).

### 🌐 Python + Flask (Interactive Application & Analytics)
*   **Why we used it:** Engineers and business users need an interactive portal. Flask provides the web interface to trigger runs, monitor tasks, view database states, and display live charts.
*   **How it works:** Spawns asynchronous background threads to run tasks locally, and exposes JSON APIs so the browser can poll the live status.

---

## 2. End-to-End Pipeline Data Flow

The following diagram illustrates how raw IPL data from Cricsheet is transformed into OLAP analytics:

```mermaid
flowchart TD
    subgraph Ingestion Layer (Bronze)
        A[Cricsheet ZIP] -->|Calculates SHA-256| B(Ingest Source)
        B -->|Register Metadata| C[(source_registry Table)]
    end

    subgraph Messaging Layer (Kafka)
        B -->|Publish to ipl_matches topic| D[Kafka Producer]
        D -->|Topic Queue| E(Kafka Topic)
        E -->|Poll and Write files| F[Kafka Consumer]
    end

    subgraph Isolation & Quality (Staging & Validation)
        F -->|Extract JSON/Parquet| G[data/staging/season/]
        G -->|Apply 11 Quality Rules| H{Data Validation}
        H -->|Fail| I[data/quarantine/]
        H -->|Pass| J[cdc_states check]
    end

    subgraph Incremental Transformation (CDC, Silver, & Gold)
        J -->|Hash Changed?| K[CDC Stage]
        K -->|Log Operation| L[(cdc_log Table)]
        K -->|Process INSERT/UPDATE/DELETE| M[Silver Pipeline]
        M -->|Load Relational Tables| N[(Silver Tables)]
        N -->|Atomic SQL Transaction Commit| O[Gold Serving Layer]
        O -->|Upsert Dimensions & Facts| P[(Gold Star Schema)]
    end

    style C fill:#fdf,stroke:#333
    style I fill:#fcc,stroke:#333
    style L fill:#fdf,stroke:#333
    style N fill:#cfc,stroke:#333
    style P fill:#bfb,stroke:#333,stroke-width:2px
```

---

## 3. Core Data Engineering Design Patterns

### 🔄 1. Idempotency (Repeatable Runs)
*   **What it is:** Running the pipeline multiple times with the same input data must result in the same database state (no duplicate records).
*   **How we solved it:** Before writing data to SQL, the pipeline deletes existing entries for that specific `match_id` (cascading deletes) and then inserts the new ones, acting as a clean overwrite.

### ⚡ 2. Change Data Capture (CDC Delta Detection)
*   **What it is:** Instead of re-reading and re-inserting all 1,200 matches on every run (which is very slow), the pipeline only processes new or updated matches.
*   **How we solved it:** The CDC stage computes a SHA-256 hash of each validated match JSON. It compares it with the previous hash stored in the database (`cdc_states`):
    *   **Identical Hash:** Stage is marked `SKIP` (no database write).
    *   **New Match ID:** Stage is marked `INSERT`.
    *   **Different Hash:** Stage is marked `UPDATE` (overwrites the changed match).
    *   **Missing file on disk:** Stage is marked `DELETE` (deletes the match from DB).

### 🔒 3. Batch Atomicity (All-or-Nothing Transactions)
*   **What it is:** If a run fails halfway through (e.g. after loading Silver but before Gold updates), the database shouldn't contain half-loaded data.
*   **How we solved it:** The database loader runs inside a single SQL transaction block. If an error is caught at any stage, a `ROLLBACK` is issued, leaving the database completely clean, restoring it to its pre-run state.

### 📁 4. Quarantine Strategy
*   **What it is:** Instead of letting corrupted data (like negative runs or future match dates) crash the database or slip through undetected, we must isolate it.
*   **How we solved it:** Matches that fail any of the 11 validation rules are written to `data/quarantine/{season}/` and logged in the `quarantine_records` metadata table with the exact reason, allowing engineers to audit it.

---

## 4. How to Explain This in Your Presentation

1.  **Introduce the Problem:** *"Raw data comes in raw JSON documents from Cricsheet. We needed to build a pipeline that cleans, validates, and loads this data into a structured relational schema for business analytics without losing data integrity."*
2.  **Highlight Kafka & Airflow:** *"We containerized the architecture in Docker. We used Apache Kafka to stream incoming files reliably and decouple ingestion from staging. We used Apache Airflow to orchestrate the validation and load tasks sequentially with built-in retries."*
3.  **Explain CDC & Performance:** *"To keep execution fast, we built a Change Data Capture layer. It uses SHA-256 delta hashes so that we only process inserts, updates, and deletes, skipping unchanged files."*
4.  **Show the Resiliency:** *"Our pipeline is completely idempotent (repeated runs do not duplicate data) and atomic (database commits only happen if every single stage finishes successfully, otherwise it rolls back)."*
