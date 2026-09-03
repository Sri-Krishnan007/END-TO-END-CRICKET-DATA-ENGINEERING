import time
import uuid
import threading
from datetime import datetime
from database.connection import get_db_cursor
from pipeline.source_ingestion import ingest_source, check_ingested
from pipeline.staging import stage_seasons
from pipeline.validation import validate_season
from pipeline.cdc import detect_changes_for_season

# In-memory thread-safe global tracking dictionary
PIPELINE_STATUS = {}
status_lock = threading.Lock()

def get_pipeline_status(run_id):
    """Safe getter for pipeline progress."""
    with status_lock:
        return PIPELINE_STATUS.get(run_id)

def set_pipeline_status(run_id, stage, status, progress, records_processed, current_season, logs_append=None):
    """Safe setter for pipeline progress."""
    with status_lock:
        if run_id not in PIPELINE_STATUS:
            PIPELINE_STATUS[run_id] = {
                "stage": "init",
                "status": "starting",
                "progress": 0,
                "records_processed": 0,
                "current_season": "",
                "elapsed_time": "00:00",
                "logs": [],
                "start_time": time.time()
            }
        
        state = PIPELINE_STATUS[run_id]
        state["stage"] = stage
        state["status"] = status
        state["progress"] = progress
        state["records_processed"] = records_processed
        state["current_season"] = str(current_season)
        
        # Calculate elapsed time
        elapsed = int(time.time() - state["start_time"])
        mins = elapsed // 60
        secs = elapsed % 60
        state["elapsed_time"] = f"{mins:02d}:{secs:02d}"
        
        if logs_append:
            state["logs"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {logs_append}")

def insert_pipeline_run_db(run_id, mode, season):
    """Creates a new entry in the pipeline_runs table."""
    try:
        with get_db_cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (run_id, pipeline_mode, season, start_time, status, current_stage)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'RUNNING', 'bronze')
                """,
                (run_id, mode, str(season))
            )
    except Exception as e:
        print(f"Failed to insert pipeline_run to DB: {e}")

def update_pipeline_run_db(run_id, status, current_stage, last_successful, counts=None, error_msg=None):
    """Updates progress and record counts in the pipeline_runs table."""
    try:
        with get_db_cursor(commit=True) as cur:
            if counts:
                cur.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = %s,
                        current_stage = %s,
                        last_successful_stage = %s,
                        records_read = %s,
                        records_inserted = %s,
                        records_updated = %s,
                        records_deleted = %s,
                        records_skipped = %s,
                        records_failed = %s,
                        error_message = %s,
                        end_time = CASE WHEN %s IN ('SUCCESS', 'FAILED') THEN CURRENT_TIMESTAMP ELSE end_time END
                    WHERE run_id = %s
                    """,
                    (status, current_stage, last_successful,
                     counts.get('read', 0), counts.get('inserted', 0), counts.get('updated', 0),
                     counts.get('deleted', 0), counts.get('skipped', 0), counts.get('failed', 0),
                     error_msg, status, run_id)
                )
            else:
                cur.execute(
                    """
                    UPDATE pipeline_runs
                    SET status = %s,
                        current_stage = %s,
                        last_successful_stage = %s,
                        error_message = %s,
                        end_time = CASE WHEN %s IN ('SUCCESS', 'FAILED') THEN CURRENT_TIMESTAMP ELSE end_time END
                    WHERE run_id = %s
                    """,
                    (status, current_stage, last_successful, error_msg, status, run_id)
                )
    except Exception as e:
        print(f"Failed to update pipeline_run in DB: {e}")

def log_stage_to_db(run_id, season, stage, start_time, status, records_read=0, records_inserted=0, records_updated=0, records_failed=0, error_message=None):
    """Inserts a run log record in pipeline_logs table (retained for backward compatibility)."""
    end_time = datetime.now()
    try:
        with get_db_cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO pipeline_logs (run_id, season, stage, start_time, end_time, status, 
                                           records_read, records_inserted, records_updated, records_deleted, 
                                           records_failed, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s)
                """,
                (run_id, str(season), stage, start_time, end_time, status, 
                 records_read, records_inserted, records_updated, records_failed, error_message)
            )
    except Exception as e:
        print(f"Failed to write log to DB: {e}")

def update_pipeline_control(season, stage_name, status):
    """Updates status flags inside pipeline_control table."""
    try:
        with get_db_cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO pipeline_control (season, status, last_processed_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (season) DO UPDATE SET 
                    status = EXCLUDED.status,
                    last_processed_at = CURRENT_TIMESTAMP
                """,
                (str(season), status)
            )
            # Update specific stage status column
            if stage_name in ['bronze', 'staging', 'validation', 'cdc', 'silver', 'gold']:
                col = f"{stage_name}_status"
                cur.execute(
                    f"UPDATE pipeline_control SET {col} = %s WHERE season = %s",
                    (status, str(season))
                )
    except Exception as e:
        print(f"Failed to update pipeline_control for season {season}: {e}")

def run_season_pipeline(season, run_id, reprocess=False, target_team=None, target_player=None, simulate_failure=False, replay_stage=None):
    """Executes the data pipeline stages for a single season, supporting failure simulation and replaying from a failed stage."""
    pipeline_mode = 'REPROCESS' if reprocess else 'INCREMENTAL'
    print(f"Starting execution loop for Season: {season} in Mode: {pipeline_mode}")
    
    # Initialize run log registry
    insert_pipeline_run_db(run_id, pipeline_mode, season)
    

            
    # Determine starting stage (Replay support)
    start_at_stage = "bronze"
    if replay_stage:
        start_at_stage = replay_stage.lower()
        print(f"[REPLAY] Resuming pipeline for Season {season} from failed stage: {start_at_stage}")

    # Track overall record counts
    counts = {'read': 0, 'inserted': 0, 'updated': 0, 'deleted': 0, 'skipped': 0, 'failed': 0}
    valid_count = 0
    staged_json_files = []

    # 1. Stage: Bronze source verification
    if start_at_stage == "bronze":
        start_time = datetime.now()
        set_pipeline_status(run_id, "bronze", "running", 10, 0, season, f"Verifying Bronze source for season {season}")
        update_pipeline_control(season, "bronze", "RUNNING")
        update_pipeline_run_db(run_id, 'RUNNING', 'bronze', 'init')
        
        if not check_ingested():
            set_pipeline_status(run_id, "bronze", "running", 15, 0, season, "Downloading Cricsheet dataset...")
            ingest_res = ingest_source()
            if ingest_res["status"] == "FAILED":
                set_pipeline_status(run_id, "bronze", "failed", 15, 0, season, f"Bronze Ingestion Failed: {ingest_res['message']}")
                log_stage_to_db(run_id, season, "bronze", start_time, "FAILED", error_message=ingest_res["message"])
                update_pipeline_control(season, "bronze", "FAILED")
                update_pipeline_run_db(run_id, 'FAILED', 'bronze', 'init', error_msg=ingest_res["message"])
                return False
                
        log_stage_to_db(run_id, season, "bronze", start_time, "SUCCESS")
        update_pipeline_control(season, "bronze", "SUCCESS")
    else:
        print(f"[REPLAY] Skipping Bronze validation (stage: {start_at_stage})")

    # 2. Stage: Staging (JSON and Parquet multi-format output)
    if start_at_stage in ["bronze", "staging"]:
        start_time = datetime.now()
        set_pipeline_status(run_id, "staging", "running", 25, 0, season, "Extracting matches to JSON and Parquet staging directories...")
        update_pipeline_control(season, "staging", "RUNNING")
        update_pipeline_run_db(run_id, 'RUNNING', 'staging', 'bronze')
        try:
            staged_counts = stage_seasons([season], target_team=target_team, target_player=target_player)
            records_read = staged_counts.get(str(season), 0)
            counts['read'] = records_read
            
            log_stage_to_db(run_id, season, "staging", start_time, "SUCCESS", records_read=records_read, records_inserted=records_read)
            update_pipeline_control(season, "staging", "SUCCESS")
        except Exception as e:
            err_msg = str(e)
            set_pipeline_status(run_id, "staging", "failed", 25, 0, season, f"Staging Failed: {err_msg}")
            log_stage_to_db(run_id, season, "staging", start_time, "FAILED", error_message=err_msg)
            update_pipeline_control(season, "staging", "FAILED")
            update_pipeline_run_db(run_id, 'FAILED', 'staging', 'bronze', error_msg=err_msg)
            return False
    else:
        print(f"[REPLAY] Skipping Staging extract (stage: {start_at_stage})")

    # Resolve staged JSON paths
    try:
        from pathlib import Path
        from config.config import STAGING_PATH
        staged_json_files = list((STAGING_PATH / str(season) / "json").glob("*.json"))
        counts['read'] = len(staged_json_files)
    except:
        pass

    # 3. Stage: Validation (schema, referential integrity and outlier checks)
    if start_at_stage in ["bronze", "staging", "validation"]:
        start_time = datetime.now()
        set_pipeline_status(run_id, "validation", "running", 45, counts['read'], season, "Validating staging records with outlier detection checks...")
        update_pipeline_control(season, "validation", "RUNNING")
        update_pipeline_run_db(run_id, 'RUNNING', 'validation', 'staging')
        try:
            val_stats = validate_season(season)
            log_stage_to_db(run_id, season, "validation", start_time, "SUCCESS", 
                            records_read=val_stats["processed"], 
                            records_inserted=val_stats["valid"], 
                            records_failed=val_stats["quarantined"])
            update_pipeline_control(season, "validation", "SUCCESS")
            valid_count = val_stats["valid"]
            counts['failed'] = val_stats["quarantined"]
        except Exception as e:
            err_msg = str(e)
            set_pipeline_status(run_id, "validation", "failed", 45, 0, season, f"Validation Failed: {err_msg}")
            log_stage_to_db(run_id, season, "validation", start_time, "FAILED", error_message=err_msg)
            update_pipeline_control(season, "validation", "FAILED")
            update_pipeline_run_db(run_id, 'FAILED', 'validation', 'staging', error_msg=err_msg)
            return False
    else:
        print(f"[REPLAY] Skipping Validation suite (stage: {start_at_stage})")
        valid_count = len(staged_json_files)

    # 4. Stage: Change Data Capture (CDC) detection
    cdc_results = None
    if start_at_stage in ["bronze", "staging", "validation", "cdc"]:
        start_time = datetime.now()
        set_pipeline_status(run_id, "cdc", "running", 60, valid_count, season, "Running CDC change detection checks...")
        update_pipeline_control(season, "cdc", "RUNNING")
        update_pipeline_run_db(run_id, 'RUNNING', 'cdc', 'validation')
        try:
            from pathlib import Path
            from config.config import STAGING_PATH
            actual_staged_json = list((STAGING_PATH / str(season) / "json").glob("*.json"))
            json_filepaths = [str(p) for p in actual_staged_json]
            cdc_results = detect_changes_for_season(season, json_filepaths, run_id)
            
            # Log CDC run
            log_stage_to_db(run_id, season, "cdc", start_time, "SUCCESS")
            update_pipeline_control(season, "cdc", "SUCCESS")
        except Exception as e:
            err_msg = str(e)
            set_pipeline_status(run_id, "cdc", "failed", 60, 0, season, f"CDC Stage Failed: {err_msg}")
            log_stage_to_db(run_id, season, "cdc", start_time, "FAILED", error_message=err_msg)
            update_pipeline_control(season, "cdc", "FAILED")
            update_pipeline_run_db(run_id, 'FAILED', 'cdc', 'validation', error_msg=err_msg)
            return False
    else:
        print(f"[REPLAY] Skipping CDC stage (stage: {start_at_stage})")

    # 5. Stage 5 & 6: Silver & Gold Atomic loads (All-or-Nothing database transaction)
    start_time = datetime.now()
    set_pipeline_status(run_id, "silver", "running", 75, valid_count, season, "Executing atomic database loads...")
    update_pipeline_control(season, "silver", "RUNNING")
    update_pipeline_control(season, "gold", "RUNNING")
    update_pipeline_run_db(run_id, 'RUNNING', 'silver', 'cdc')
    
    try:
        from pipeline.atomicity import load_season_atomic
        atomic_res = load_season_atomic(
            season, 
            pipeline_mode=pipeline_mode, 
            cdc_results=cdc_results, 
            simulate_failure=simulate_failure
        )
        
        # Merge counts
        loaded_counts = atomic_res["counts"]
        counts.update({
            'inserted': loaded_counts.get('inserted', 0),
            'updated': loaded_counts.get('updated', 0),
            'skipped': loaded_counts.get('skipped', 0),
            'deleted': loaded_counts.get('deleted', 0)
        })
        
        # Log successful loads
        log_stage_to_db(run_id, season, "silver", start_time, "SUCCESS", records_read=valid_count, records_inserted=counts['inserted'], records_updated=counts['updated'])
        log_stage_to_db(run_id, season, "gold", start_time, "SUCCESS")
        
        update_pipeline_control(season, "silver", "SUCCESS")
        update_pipeline_control(season, "gold", "SUCCESS")
        update_pipeline_control(season, "pipeline", "COMPLETED")
        
        set_pipeline_status(run_id, "gold", "success", 100, counts['inserted'] + counts['updated'], season, f"Season {season} fully loaded successfully in a single transaction.")
        update_pipeline_run_db(run_id, 'SUCCESS', 'done', 'gold', counts=counts)
        return True
        
    except Exception as e:
        err_msg = str(e)
        failed_stage = "gold" if "SIMULATED_FAILURE" in err_msg or "gold" in err_msg.lower() else "silver"
        
        set_pipeline_status(run_id, failed_stage, "failed", 75, 0, season, f"Pipeline Failed: {err_msg}")
        log_stage_to_db(run_id, season, failed_stage, start_time, "FAILED", error_message=err_msg)
        
        update_pipeline_control(season, failed_stage, "FAILED")
        update_pipeline_control(season, "pipeline", "FAILED")
        update_pipeline_run_db(run_id, 'FAILED', failed_stage, 'cdc', counts=counts, error_msg=err_msg)
        return False

def execute_pipeline_thread(seasons, run_id, reprocess=False, target_team=None, target_player=None, simulate_failure=False, replay_stage=None):
    """Executes the pipeline loop over multiple seasons sequentially in a background thread."""
    set_pipeline_status(run_id, "init", "running", 0, 0, "", "Initializing pipeline execution thread...")
    
    total_seasons = len(seasons)
    success_count = 0
    
    for idx, season in enumerate(seasons):
        set_pipeline_status(run_id, "init", "running", int((idx / total_seasons) * 100), 0, season, f"Processing Season {season} ({idx+1}/{total_seasons})")
        
        ok = run_season_pipeline(
            season, run_id, 
            reprocess=reprocess, 
            target_team=target_team, 
            target_player=target_player,
            simulate_failure=simulate_failure,
            replay_stage=replay_stage
        )
        if ok:
            success_count += 1
        else:
            set_pipeline_status(run_id, "init", "failed", int(((idx + 1) / total_seasons) * 100), 0, season, f"Pipeline execution failed at Season {season}. Transaction rolled back.")
            return
            
    set_pipeline_status(run_id, "done", "success", 100, 0, "", f"Pipeline completed successfully. Processed {success_count} seasons.")

def start_pipeline_async(seasons, reprocess=False, target_team=None, target_player=None, simulate_failure=False, replay_stage=None):
    """Starts the pipeline execution loop asynchronously in the background.
    Returns the unique run_id string.
    """
    run_id = str(uuid.uuid4())
    # Create initial status
    set_pipeline_status(run_id, "init", "running", 0, 0, "", "Starting pipeline session...")
    
    thread = threading.Thread(
        target=execute_pipeline_thread,
        args=(seasons, run_id, reprocess, target_team, target_player, simulate_failure, replay_stage),
        daemon=True
    )
    thread.start()
    return run_id
