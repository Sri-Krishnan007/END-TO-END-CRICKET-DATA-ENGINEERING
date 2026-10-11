# Flask Web UI - Components & Usages Guide

This document describes all the web interface views, layout components, and operations available in the **IPL Data Engineering Platform** UI, including how to showcase them during your evaluation or presentation.

---

## 1. Navigating the UI (The Base Layout)

The web application features a responsive sidebar and top navigation bar providing direct access to all layers of the Medallion Data Warehouse, the Open Lakehouse, and the Governance engines:

*   🏠 **Home Dashboard (`/`)**: Project landing page, architecture overview, and quick metrics.
*   📥 **Source Ingestion (`/ingestion`)**: Bronze layer raw download controller and SHA-256 integrity verifier.
*   ⚙️ **Data Pipeline (`/pipeline`)**: Main execution controller, configuration forms, and season reset bench.
*   🖥️ **Pipeline Monitor (`/pipeline/monitor/<run_id>`)**: Real-time SSE execution logs, live progress bar, and stage indicators.
*   📊 **Staging EDA (`/eda`)**: Staging data quality statistics, anomalies alerts, and JSON schema inspection.
*   💿 **Silver Catalog (`/silver`)**: Normalized 3NF relational database schema viewer.
*   🏆 **Gold Star Schema (`/gold`)**: OLAP dimensional schema specification and Star Schema entity maps.
*   🔍 **Data Warehouse Inspector (`/summary`)**: Real-time PostgreSQL table row inspector and live query sampler.
*   📈 **IPL Analytics (`/analytics`)**: Interactive analytics dashboard with KPIs, player filters, and leaderboards.
*   🧊 **Lakehouse Serving (`/lakehouse`)**: Distributed Trino SQL query engine, live SQL console, and Iceberg analytics.
*   🔄 **Process Mining & BPM (`/bpm`)**: Petri net process mining graph, token replay conformance, and SLA breach audits.

---

## 2. Page-by-Page UI Components & Usages

### 🏠 Page 1: Home Dashboard (`index.html` - `/`)
*   **Components:**
    *   **Project Overview Hero:** Introduction to the dual Medallion + Lakehouse platform architecture.
    *   **Interactive Architecture Card:** Visual diagrams detailing Kafka streaming, 11-rule validation, CDC, PostgreSQL 3NF/Star Schema, and Apache Iceberg + Trino serving.
    *   **Quick Metrics Banner:** Total processed seasons, warehouse records count, and system status indicators.
*   **How to use:** Start your presentation here to set the technical context and outline the technology stack.

---

### 📥 Page 2: Source Ingestion / Ingest Registry (`ingestion.html` - `/ingestion`)
*   **Components:**
    *   **Source Registry Card:** Displays download status, local file path, file size, and the calculated **SHA-256 hash** of the raw Cricsheet ZIP.
    *   **Download Button:** Triggers a background thread to fetch `ipl_json.zip` if missing or refresh checksums.
*   **How to use:** Demonstrate **Source Integrity & Provenance**. Show that downloading verifies cryptographic file hashes on the fly and logs them into `source_registry`.

---

### ⚙️ Page 3: Data Pipeline Control Panel (`pipeline.html` - `/pipeline`)
This is the main operational cockpit of the platform.

*   **Components:**
    *   **Pipeline Config Form:**
        *   *Execution Modes:* Single Year dropdown, Year-Range selectors (From/To), or Multiple checkboxes.
        *   *Team/Player Filters:* Dropdown filters to run the pipeline exclusively for a specific team or player.
        *   *Reprocess Checkbox:* Forces deletion of existing database rows and rebuilds from staging.
        *   *Rollback Simulation Checkbox:* Intentionally fails the transaction halfway to demonstrate SQL atomicity and transaction rollback.
    *   **Historical Execution Table:** Chronological log of previous runs showing run ID, status (`SUCCESS`/`FAILED`), records loaded, duration, and error alerts.
    *   **Season Registry Matrix:** Grid showing completion status of each season at every pipeline step (Bronze, Staging, Validation, CDC, Silver, Gold).
    *   **Platform Reset Bench:** Red alert buttons to cascade-delete individual seasons or completely wipe the database and recreate tables using `schema.sql`.
*   **How to use:** Trigger your live pipeline demonstrations from here. Show how selecting a season creates a new run ID and redirects to the live monitor.

---

### 🖥️ Page 4: Real-Time Pipeline Monitor (`pipeline_monitor.html` - `/pipeline/monitor/<run_id>`)
*   **Components:**
    *   **Live Progress Bar:** Displays dynamic completion percentage (0% to 100%) as stages execute.
    *   **Stage Indicators:** Graphical checkmarks that light up green as the pipeline completes each task (Staging, Validation, CDC, Silver, Gold, Lakehouse Sync).
    *   **Live Log Console:** A high-contrast dark terminal window streaming log outputs in real-time from the backend thread via `/api/pipeline/status/<run_id>`.
*   **How to use:** Keep this page open during a run to show **Asynchronous Execution & Streaming Logs** without page refreshes.

---

### 📊 Page 5: Staging EDA / Exploratory Data Analysis (`eda.html` - `/eda`)
*   **Components:**
    *   **Season Selector:** Dropdown to load staging statistics for any specific tournament year.
    *   **Summary Stats Grid:** Total matches, run aggregates, extras breakdown, venue counts, and unique squads.
    *   **Nulls & Missing Value Matrix:** Visual chart showing key frequencies and missing optional fields in raw JSON files.
    *   **Anomalies Alerts:** Highlights files containing outliers (e.g. match runs > 450) flagged before database insertion.
*   **How to use:** Demonstrate thorough data profiling and exploration before applying ETL silver transformations.

---

### 💿 Page 6 & 🏆 Page 7: Silver and Gold Catalogs (`silver.html` & `gold.html`)
*   **Components:**
    *   **Database Schema Trees:** Expandable cards displaying tables, column names, data types, primary keys, and foreign keys.
    *   **Entity-Relationship Maps:** Mermaid flowcharts displaying database schemas (13 normalized 3NF tables for Silver; Fact and Dimension tables for Gold).
*   **How to use:** Explain your **Data Warehouse Schema Design**. Emphasize how Silver prevents redundancy through normalization, while Gold aggregates facts for sub-second analytical reporting.

---

### 🔍 Page 8: Live Data Warehouse Inspector (`summary.html` - `/summary`)
*   **Components:**
    *   **Table Selector:** Dropdown containing all 23 database tables (e.g. `match`, `delivery`, `cdc_states`, `quarantine_records`, `bpm_event_logs`).
    *   **Season Filter:** Optional filter to narrow down rows by season.
    *   **Interactive SQL Sample Grid:** A table showing the top 10 rows retrieved directly from PostgreSQL.
    *   **Active Row Counter:** Live stats displaying total rows loaded in the selected table.
*   **How to use:** Verify pipeline outputs directly in the browser. After running a season, load `match` or `cdc_log` here to show the newly inserted rows to the evaluator without needing a separate database client.

---

### 📈 Page 9: Interactive Analytics Dashboard (`analytics.html` - `/analytics`)
*   **Components:**
    *   **Dynamic Selection Dashboard:** Filter KPIs by season, team, batting/bowling team, player name, and venue.
    *   **KPI Cards:** Statistics showing Matches, Total Runs, Wickets, Boundaries (4s), Sixes (6s), and Total Players.
    *   **Runs by Season (Line Chart):** Year-on-year score distributions.
    *   **Top 10 Batting / Bowling (Bar Charts):** Leaderboards filtered dynamically.
    *   **Venue Distribution (Doughnut Chart):** Distribution of matches across stadiums.
*   **How to use:** Filter by a player (e.g., "V Kohli" or "MS Dhoni") and show how the charts immediately rebuild from the Gold Star Schema to display career figures.

---

### 🧊 Page 10: Lakehouse Serving & Trino Dashboard (`lakehouse.html` - `/lakehouse`)
This view showcases the **Modern Lakehouse Architecture (Apache Iceberg + Nessie + MinIO + Trino)**:

*   **Components:**
    *   **Trino Status Pill:** Live indicator showing `TRINO DISTRIBUTED SQL ONLINE` (green) or `DIRECT LAKEHOUSE STORAGE FALLBACK` (blue).
    *   **Latency Badge:** Live query latency in milliseconds (e.g., `18 ms`).
    *   **Engine Description Callout:** Explains whether queries are running via Trino distributed SQL over port `8085` or via local columnar lakehouse storage.
    *   **Season Dropdown Filter:** Instant filtering for all seasons (2008–2024) with partition pruning.
    *   **4 Lakehouse KPI Cards:** Total Matches, Total Runs, Total Wickets, Total Overs.
    *   **3 Chart.js Lakehouse Visualizations:**
        1. *Season Performance Trends (Bar Chart):* Total runs scored across seasons.
        2. *Team Victories & Win Distribution (Pie Chart):* Match wins grouped by franchise.
        3. *Stadium Utilization (Bar Chart):* Top venues hosted by match frequency.
    *   **Interactive Trino SQL Query Console:**
        *   Live SQL text area allowing users to enter custom ANSI SQL queries (e.g. `SELECT season, COUNT(*) FROM iceberg.ipl.ipl_matches GROUP BY season`).
        *   *Quick Template Buttons:* One-click buttons to populate common analytical queries (`SELECT * LIMIT 10`, `Season Aggregates`, `Top Batters`, `Team Wins`).
        *   *Execute Query Button:* Dispatches queries to `/api/trino/query` and renders tabular results with column headers, row counts, and execution duration.
*   **How to use:** Show the evaluator how Trino queries the Iceberg table with zero-copy speed. Click the "Season Aggregates" template and hit "Execute Query" to see live results returned in ~18 ms!

---

### 🔄 Page 11: Business Process Mining (BPM) & SLA Compliance (`bpm.html` - `/bpm`)
This view demonstrates enterprise **Process Governance, Conformance Checking, and SLA Auditing**:

*   **Components:**
    *   **Petri Net / Process Flow Graph:** Visual representation of pipeline transitions from `bronze_ingest` $\rightarrow$ `kafka_streaming` $\rightarrow$ `validation` $\rightarrow$ `cdc` $\rightarrow$ `silver_load` $\rightarrow$ `gold_load` $\rightarrow$ `lakehouse_sync` $\rightarrow$ `lakehouse_maintenance`.
    *   **Transition Matrix & SLA Thresholds:** Table displaying normative transitions and allowed SLAs per activity (e.g., Bronze Ingest $\le 30$s, Silver Load $\le 60$s, Lakehouse Sync $\le 30$s).
    *   **Token Replay Conformance KPI:** Overall process model fitness score (e.g., `0.98` fitness).
    *   **Process Anomaly & SLA Breach Audit Table:**
        *   *Anomaly Types:* Flags `SLA_BREACH`, `BOTTLENECK_STAGE`, `DUPLICATE_ACTIVITY`, and `OUT_OF_ORDER_EXECUTION`.
        *   *Severity Levels:* `HIGH`, `MEDIUM`, `LOW`.
        *   *Audit Detail:* Exact reason, case ID, stage, and duration recorded in `bpm_event_logs`.
*   **How to use:** Open this page to show that you don't just run data pipelines—you mathematically govern their execution, detect anomalies, and enforce SLAs using process mining algorithms.

---

## 3. Recommended Presentation Flow

When demonstrating the platform to professors or evaluators, follow this 5-step sequence:

1.  **Start at Home Dashboard (`/`)**: Give an architectural overview, pointing out the dual Medallion + Lakehouse stack.
2.  **Trigger a Live Run in Data Pipeline (`/pipeline`)**: Select Season 2024, click "Run Pipeline", and observe the real-time progress bar and log stream on the **Monitor (`/pipeline/monitor/<run_id>`)**.
3.  **Inspect Quality & Schema in Warehouse Inspector (`/summary`)**: Show `match` table rows and `quarantine_records` to demonstrate the 11-rule quality gate.
4.  **Showcase Modern Lakehouse Serving (`/lakehouse`)**: Display Trino status, demonstrate the 3 Lakehouse charts, and run an interactive SQL query via the **Live Trino Console**.
5.  **Conclude with Governance & BPM (`/bpm`)**: Show the Petri net process model, the conformance fitness score, and the automated SLA anomaly audit log.
