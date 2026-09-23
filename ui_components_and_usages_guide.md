# Flask Web UI - Components & Usages Guide

This document describes all the web interface views, layout components, and operations available in the **IPL Data Engineering Platform** UI, including how to showcase them during your presentation.

---

## 1. Navigating the UI (The Base Layout)
The web application features a responsive sidebar/top navigation bar containing direct access to all layers of the data warehouse:
*   🏠 **Home Dashboard**: Project landing page and architectural layout.
*   📥 **Source Ingestion**: Bronze layer download controller.
*   ⚙️ **Data Pipeline**: Main execution controller, logs history, and reset bench.
*   📊 **Staging EDA**: Staging data quality statistics and analysis.
*   💿 **Silver Catalog**: Normalized relational database schema.
*   🏆 **Gold Star Schema**: OLAP analytics schema dimension specifications.
*   🔍 **Data Warehouse Inspector**: Real-time SQL database row inspector.
*   📈 **IPL Analytics**: Interactive analytics dashboard with KPIs and charts.

---

## 2. Page-by-Page UI Components & Usages

### 🏠 Page 1: Home Dashboard (`index.html`)
*   **Components:**
    *   **Project Overview Card:** General introduction to the end-to-end platform.
    *   **Interactive Architecture Card:** Step-by-step description of the Bronze-Silver-Gold ingestion workflow.
    *   **Quick Metrics Banner:** Total records and processed seasons count.
*   **How to use:** Start your presentation here to set the context of what you have built and outline the tech stack.

---

### 📥 Page 2: Source Ingestion / Ingest Registry (`ingestion.html`)
*   **Components:**
    *   **Source Registry Card:** Displays download status, local file path, file size, and the calculated **SHA-256 hash** of the raw Cricsheet ZIP.
    *   **Download Button:** Triggers a background thread to fetch `ipl_json.zip` if missing.
*   **How to use:** Use this to demonstrate **Source Integrity Checks**. Show that clicking download verifies the file hash on the fly and logs it in the database.

---

### ⚙️ Page 3: Data Pipeline Control Panel (`pipeline.html`)
This is the main interaction console of the application.

*   **Components:**
    *   **Pipeline Config Form:**
        *   *Execution Modes:* Single Year dropdown, Year-Range selectors (From/To), or Multiple checkboxes.
        *   *Team/Player Filters:* Dropdown filters to run the pipeline exclusively for a specific team or player.
        *   *Reprocess Checkbox:* Forces deletion of existing database rows and rebuilds from staging.
        *   *Rollback Simulation Checkbox:* Intentionally fails the transaction halfway to test SQL atomicity.
    *   **Historical Execution Table:** Chronological log of previous runs showing run ID, status (SUCCESS/FAILED), records loaded, and error alerts.
    *   **Season Registry Matrix:** Grid showing the status of each season at every step (Bronze, Staging, Validation, CDC, Silver, Gold).
    *   **Platform Reset Bench:** Red alert buttons to cascade-delete individual seasons or completely wipe the database and recreate tables using `schema.sql`.
*   **How to use:** This is where you trigger all your pipeline demos (Test Cases 1–9). Show how triggering a season creates a new run entry.

---

### 🖥️ Page 4: Real-Time Pipeline Monitor (`pipeline_monitor.html`)
*   **Components:**
    *   **Live Progress Bar:** Displays dynamic percentages (0% to 100%) as the pipeline moves.
    *   **Stage Indicators:** Graphical checkmarks that light up green as the pipeline completes each task (Staging, Validation, CDC, Silver, Gold).
    *   **Live Log Console:** A simulated black terminal window that streams log outputs in real-time from the backend thread.
*   **How to use:** Keep this page open during a run to show the **Asynchronous Processing**. The browser polls `/api/pipeline/status/<run_id>` to fetch the logs dynamically.

---

### 📊 Page 5: Staging EDA / Exploratory Data Analysis (`eda.html`)
*   **Components:**
    *   **Season Selector:** Dropdown to load staging statistics for a specific season.
    *   **Summary Stats Grid:** Displays total matches, run totals, extras, match venues, and unique teams.
    *   **Nulls & Missing Value Matrix:** Visual chart showing missing keys in raw files.
    *   **Anomalies Alerts:** Highlights files containing outliers (e.g. match runs > 560).
*   **How to use:** Show this to demonstrate how you understand the staged raw JSON data structure before performing ETL silver transformations.

---

### 💿 Page 6 & 🏆 Page 7: Silver and Gold Catalogs (`silver.html` & `gold.html`)
*   **Components:**
    *   **Database Schema Trees:** Expandable cards displaying tables, column names, data types, primary keys, and foreign keys.
    *   **Relationship Maps:** Mermaid flowcharts displaying database schemas (Normalized schema for Silver; Star Schema for Gold).
*   **How to use:** Open these pages to explain the **Data Warehouse Schema Design**. Explain how Silver is heavily normalized to prevent redundancy, while Gold is structured as Fact and Dimension tables for fast OLAP queries.

---

### 🔍 Page 8: Live Data Warehouse Inspector (`summary.html`)
*   **Components:**
    *   **Table Selector:** Dropdown containing all 23 database tables (e.g. `match`, `delivery`, `cdc_states`, `quarantine_records`).
    *   **Season Filter:** Optional filter to narrow down rows.
    *   **Interactive SQL Sample Grid:** A table showing the top 10 rows retrieved directly from PostgreSQL.
    *   **Active Row Counter:** Summary stats of total rows loaded in the selected table.
*   **How to use:** Excellent for verification. After running a pipeline, load `match` or `cdc_log` here to show the newly inserted rows to the faculty without leaving the web browser.

---

### 📈 Page 9: Interactive Analytics Dashboard (`analytics.html`)
*   **Components:**
    *   **Dynamic Selection Dashboard:** Filter KPIs by season, team, batting/bowling team, player name, and venue.
    *   **KPI Cards:** Interactive statistics showing Matches, Total Runs, Wickets, Boundaries (4s), Sixes (6s), and Total Players.
    *   **Runs by Season (Line Chart):** Year-on-year score distributions.
    *   **Top 10 Batting / Bowling (Bar Charts):** Leaderboards filtered dynamically.
    *   **Venue Distribution (Doughnut Chart):** Distribution of matches across stadiums.
*   **How to use:** Filter by a player (e.g., "V Kohli") and show how the charts immediately rebuild to show only his career stats. This demonstrates the performance of your Gold serving layer.
