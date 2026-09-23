-- DROP TABLES IF EXIST TO ENSURE FRESH INITIALIZATION
DROP TABLE IF EXISTS FACT_MATCH_SUMMARY CASCADE;
DROP TABLE IF EXISTS DIM_TEAM CASCADE;
DROP TABLE IF EXISTS DIM_PLAYER CASCADE;

DROP TABLE IF EXISTS team_squad CASCADE;
DROP TABLE IF EXISTS match_official CASCADE;
DROP TABLE IF EXISTS player_of_match CASCADE;
DROP TABLE IF EXISTS toss CASCADE;
DROP TABLE IF EXISTS powerplay CASCADE;
DROP TABLE IF EXISTS wicket_fielder CASCADE;
DROP TABLE IF EXISTS wicket CASCADE;
DROP TABLE IF EXISTS delivery CASCADE;
DROP TABLE IF EXISTS innings CASCADE;
DROP TABLE IF EXISTS official CASCADE;
DROP TABLE IF EXISTS player CASCADE;
DROP TABLE IF EXISTS team CASCADE;
DROP TABLE IF EXISTS match CASCADE;

DROP TABLE IF EXISTS source_registry CASCADE;
DROP TABLE IF EXISTS pipeline_control CASCADE;
DROP TABLE IF EXISTS pipeline_logs CASCADE;
DROP TABLE IF EXISTS quarantine_records CASCADE;
DROP TABLE IF EXISTS cdc_states CASCADE;
DROP TABLE IF EXISTS cdc_log CASCADE;
DROP TABLE IF EXISTS pipeline_runs CASCADE;
DROP TABLE IF EXISTS bpm_process_events CASCADE;

-- ============================================================================
-- 1. PIPELINE CONTROL TABLES
-- ============================================================================

CREATE TABLE source_registry (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100),
    source_url TEXT,
    local_path TEXT,
    file_hash VARCHAR(64),
    file_size BIGINT,
    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20)
);

CREATE TABLE pipeline_control (
    pipeline_id SERIAL PRIMARY KEY,
    season VARCHAR(10) UNIQUE,
    bronze_status VARCHAR(20),
    staging_status VARCHAR(20),
    validation_status VARCHAR(20),
    cdc_status VARCHAR(20),
    silver_status VARCHAR(20),
    gold_status VARCHAR(20),
    last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20)
);

CREATE TABLE pipeline_logs (
    run_id VARCHAR(50),
    season VARCHAR(10),
    stage VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    records_read INT,
    records_inserted INT,
    records_updated INT,
    records_deleted INT,
    records_failed INT,
    error_message TEXT
);

CREATE TABLE quarantine_records (
    record_id SERIAL PRIMARY KEY,
    season VARCHAR(10),
    source VARCHAR(100),
    stage VARCHAR(50),
    error_type VARCHAR(100),
    error_message TEXT,
    raw_record TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20)
);

CREATE TABLE cdc_states (
    entity_name VARCHAR(100),
    business_key VARCHAR(100),
    record_hash VARCHAR(64),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_name, business_key)
);

CREATE TABLE cdc_log (
    log_id SERIAL PRIMARY KEY,
    run_id VARCHAR(50),
    entity_name VARCHAR(100),
    business_key VARCHAR(100),
    operation VARCHAR(20),
    old_hash VARCHAR(64),
    new_hash VARCHAR(64),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pipeline_runs (
    run_id VARCHAR(50) PRIMARY KEY,
    pipeline_mode VARCHAR(20),
    season VARCHAR(10),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    current_stage VARCHAR(50),
    last_successful_stage VARCHAR(50),
    records_read INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    records_deleted INT DEFAULT 0,
    records_skipped INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    error_message TEXT
);

-- ============================================================================
-- 2. SILVER LAYER (NORMALIZED RELATIONAL MODEL)
-- ============================================================================

CREATE TABLE match (
    match_id VARCHAR(20) PRIMARY KEY,
    tournament VARCHAR(100),
    match_number VARCHAR(50),
    season VARCHAR(10),
    match_date DATE,
    city VARCHAR(100),
    venue VARCHAR(150),
    match_type VARCHAR(20),
    gender VARCHAR(20),
    scheduled_overs INT,
    balls_per_over INT,
    team_type VARCHAR(20),
    team1 VARCHAR(100),
    team2 VARCHAR(100),
    toss_winner VARCHAR(100),
    toss_decision VARCHAR(20),
    winner VARCHAR(100),
    win_by_runs INT,
    win_by_wickets INT,
    result VARCHAR(50),
    result_method VARCHAR(50),
    eliminator VARCHAR(100),
    player_of_match VARCHAR(100)
);

CREATE TABLE team (
    team_name VARCHAR(100) PRIMARY KEY
);

CREATE TABLE player (
    player_name VARCHAR(100) PRIMARY KEY,
    cricsheet_id VARCHAR(50)
);

CREATE TABLE official (
    official_name VARCHAR(100),
    official_role VARCHAR(50),
    PRIMARY KEY (official_name, official_role)
);

CREATE TABLE innings (
    innings_id SERIAL PRIMARY KEY,
    match_id VARCHAR(20) REFERENCES match(match_id) ON DELETE CASCADE,
    innings_number INT,
    batting_team VARCHAR(100),
    target_runs INT,
    target_overs NUMERIC
);

CREATE TABLE delivery (
    delivery_id SERIAL PRIMARY KEY,
    match_id VARCHAR(20) REFERENCES match(match_id) ON DELETE CASCADE,
    innings_number INT,
    batting_team VARCHAR(100),
    over_number INT,
    actual_delivery INT,
    batter VARCHAR(100),
    bowler VARCHAR(100),
    non_striker VARCHAR(100),
    batter_runs INT,
    extras INT,
    total_runs INT,
    wides INT,
    noballs INT,
    byes INT,
    legbyes INT
);

CREATE TABLE wicket (
    wicket_id SERIAL PRIMARY KEY,
    delivery_id INT REFERENCES delivery(delivery_id) ON DELETE CASCADE,
    match_id VARCHAR(20) REFERENCES match(match_id) ON DELETE CASCADE,
    innings_number INT,
    over_number INT,
    actual_delivery INT,
    batter VARCHAR(100),
    bowler VARCHAR(100),
    player_out VARCHAR(100),
    dismissal_kind VARCHAR(50)
);

CREATE TABLE wicket_fielder (
    wicket_fielder_id SERIAL PRIMARY KEY,
    wicket_id INT REFERENCES wicket(wicket_id) ON DELETE CASCADE,
    delivery_id INT,
    player_name VARCHAR(100)
);

CREATE TABLE powerplay (
    powerplay_id SERIAL PRIMARY KEY,
    match_id VARCHAR(20) REFERENCES match(match_id) ON DELETE CASCADE,
    innings_number INT,
    batting_team VARCHAR(100),
    from_delivery NUMERIC,
    to_delivery NUMERIC,
    powerplay_type VARCHAR(50)
);

CREATE TABLE toss (
    match_id VARCHAR(20) PRIMARY KEY REFERENCES match(match_id) ON DELETE CASCADE,
    toss_winner VARCHAR(100),
    toss_decision VARCHAR(20)
);

CREATE TABLE player_of_match (
    player_of_match_id SERIAL PRIMARY KEY,
    match_id VARCHAR(20) REFERENCES match(match_id) ON DELETE CASCADE,
    player_name VARCHAR(100)
);

CREATE TABLE match_official (
    match_official_id SERIAL PRIMARY KEY,
    match_id VARCHAR(20) REFERENCES match(match_id) ON DELETE CASCADE,
    official_name VARCHAR(100),
    official_role VARCHAR(50)
);

CREATE TABLE team_squad (
    team_squad_id SERIAL PRIMARY KEY,
    match_id VARCHAR(20) REFERENCES match(match_id) ON DELETE CASCADE,
    team_name VARCHAR(100),
    player_name VARCHAR(100)
);

-- ============================================================================
-- 3. GOLD LAYER (STAR SCHEMA / OLAP SERVING TABLES)
-- ============================================================================

CREATE TABLE FACT_MATCH_SUMMARY (
    match_id VARCHAR(20) PRIMARY KEY,
    tournament VARCHAR(100),
    match_number VARCHAR(50),
    season VARCHAR(10),
    match_date DATE,
    city VARCHAR(100),
    venue VARCHAR(150),
    match_type VARCHAR(20),
    gender VARCHAR(20),
    scheduled_overs INT,
    balls_per_over INT,
    team_type VARCHAR(20),
    team1 VARCHAR(100),
    team2 VARCHAR(100),
    toss_winner VARCHAR(100),
    toss_decision VARCHAR(20),
    winner VARCHAR(100),
    win_by_runs INT,
    win_by_wickets INT,
    result VARCHAR(50),
    result_method VARCHAR(50),
    eliminator VARCHAR(100),
    player_of_match VARCHAR(100),
    total_runs INT,
    total_wickets INT,
    total_boundaries INT,
    total_sixes INT
);

CREATE TABLE DIM_TEAM (
    team_name VARCHAR(100) PRIMARY KEY,
    matches_played INT,
    wins INT,
    total_runs INT,
    total_boundaries INT,
    total_sixes INT
);

CREATE TABLE DIM_PLAYER (
    player_name VARCHAR(100) PRIMARY KEY,
    batting_matches INT,
    batting_innings INT,
    total_runs INT,
    total_balls INT,
    total_fours INT,
    total_sixes INT,
    highest_score INT,
    fifties INT,
    hundreds INT,
    bowling_matches INT,
    bowling_innings INT,
    balls_bowled INT,
    overs_bowled NUMERIC,
    runs_conceded INT,
    wickets_taken INT,
    best_wickets INT
);

-- ============================================================================
-- 4. BPM PROCESS EVENT LOG TABLE
-- ============================================================================

CREATE TABLE bpm_process_events (
    event_id SERIAL PRIMARY KEY,
    case_id VARCHAR(50) NOT NULL,
    activity VARCHAR(100) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_timestamp TIMESTAMP,
    duration_seconds NUMERIC,
    status VARCHAR(30) NOT NULL,
    records_processed INT DEFAULT 0,
    error_message TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX idx_bpm_case_id ON bpm_process_events(case_id);
CREATE INDEX idx_bpm_activity ON bpm_process_events(activity);
CREATE INDEX idx_bpm_timestamp ON bpm_process_events(event_timestamp);

