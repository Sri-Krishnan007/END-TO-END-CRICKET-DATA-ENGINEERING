import re
from flask import Blueprint, render_template, request, redirect, url_for, flash
from pipeline.pipeline_runner import start_pipeline_async, get_pipeline_status
from database.connection import get_db_cursor

pipeline_bp = Blueprint('pipeline', __name__)

@pipeline_bp.route('/pipeline')
def index():
    # Load available history logs from the database
    logs = []
    teams = []
    players = []
    try:
        with get_db_cursor(commit=False, cursor_factory='dict') as cur:
            cur.execute(
                """
                SELECT run_id, season, stage, start_time, end_time, status, records_inserted, error_message
                FROM pipeline_logs
                ORDER BY start_time DESC
                LIMIT 50
                """
            )
            logs = cur.fetchall()
            
            # Fetch teams
            cur.execute("SELECT team_name FROM team ORDER BY team_name ASC")
            teams = [r['team_name'] for r in cur.fetchall()]
            
            # Fetch players
            cur.execute("SELECT player_name FROM player ORDER BY player_name ASC")
            players = [r['player_name'] for r in cur.fetchall()]
    except Exception as e:
        print(f"Failed to query database catalog for pipeline index: {e}")
        
    # Fallback lists if DB is empty
    if not teams:
        teams = ["Chennai Super Kings", "Delhi Capitals", "Punjab Kings", "Kolkata Knight Riders", "Mumbai Indians", "Rajasthan Royals", "Royal Challengers Bengaluru", "Sunrisers Hyderabad", "Gujarat Titans", "Lucknow Super Giants", "Rising Pune Supergiants"]
    if not players:
        players = ["V Kohli", "MS Dhoni", "RG Sharma", "DA Warner", "S Dhawan", "AB de Villiers", "CH Gayle", "SK Raina", "Yuvraj Singh", "G Gambhir"]
        
    return render_template('pipeline.html', history_logs=logs, teams=teams, players=players)

@pipeline_bp.route('/pipeline/trigger', methods=['POST'])
def trigger():
    mode = request.form.get('mode')
    reprocess = request.form.get('reprocess') == 'true'
    simulate_failure = request.form.get('simulate_failure') == 'true'
    replay_stage = request.form.get('replay_stage')
    
    # 3 types of trigger filters
    filter_type = request.form.get('filter_type', 'season_only')
    target_team = None
    target_player = None
    
    if filter_type == 'team':
        target_team = request.form.get('filter_team')
    elif filter_type == 'player':
        target_player = request.form.get('filter_player')
        
    seasons = []
    if mode == 'single':
        year = request.form.get('single_year')
        if year:
            seasons.append(int(year))
    elif mode == 'range':
        from_year = request.form.get('from_year')
        to_year = request.form.get('to_year')
        if from_year and to_year:
            seasons = list(range(int(from_year), int(to_year) + 1))
    elif mode == 'multiple':
        selected = request.form.getlist('multi_years')
        seasons = [int(y) for y in selected if y]
        
    if not seasons:
        flash("No valid seasons selected for processing.", "warning")
        return redirect(url_for('pipeline.index'))
        
    # Start pipeline asynchronously passing failure simulation & replay arguments
    run_id = start_pipeline_async(
        seasons, 
        reprocess=reprocess, 
        target_team=target_team, 
        target_player=target_player,
        simulate_failure=simulate_failure,
        replay_stage=replay_stage
    )
    return redirect(url_for('pipeline.monitor', run_id=run_id))

@pipeline_bp.route('/pipeline/monitor/<run_id>')
def monitor(run_id):
    status = get_pipeline_status(run_id)
    if not status:
        try:
            with get_db_cursor(commit=False, cursor_factory='dict') as cur:
                cur.execute(
                    "SELECT status, error_message FROM pipeline_logs WHERE run_id = %s ORDER BY end_time DESC LIMIT 1",
                    (run_id,)
                )
                db_status = cur.fetchone()
                if db_status:
                    status = {
                        "stage": "done",
                        "status": db_status['status'].lower(),
                        "progress": 100,
                        "records_processed": 0,
                        "current_season": "",
                        "elapsed_time": "Finished",
                        "logs": [f"Pipeline run registered in DB history with status: {db_status['status']}"],
                    }
                    if db_status['error_message']:
                        status["logs"].append(f"Error: {db_status['error_message']}")
        except Exception as e:
            print(f"Failed to query database logs for monitoring page: {e}")
            
    return render_template('pipeline_monitor.html', run_id=run_id, initial_status=status)

@pipeline_bp.route('/silver')
def silver_catalog():
    return render_template('silver.html')

@pipeline_bp.route('/gold')
def gold_schema():
    return render_template('gold.html')

@pipeline_bp.route('/summary')
def summary_page():
    return render_template('summary.html')

@pipeline_bp.route('/pipeline/delete/<season>', methods=['POST'])
def delete_season(season):
    """Cascades SQL deletes of a season, purges its staging files, and resets catalog status."""
    try:
        # 1. Database cleanup
        with get_db_cursor(commit=True) as cur:
            # Query match_ids to delete first (cascading deletes to deliver, wicket etc)
            cur.execute("SELECT match_id FROM match WHERE season = %s", (str(season),))
            match_ids = [r[0] for r in cur.fetchall()]
            
            if match_ids:
                cur.execute("DELETE FROM match WHERE match_id = ANY(%s)", (match_ids,))
            
            # Delete from FACT_MATCH_SUMMARY
            cur.execute("DELETE FROM FACT_MATCH_SUMMARY WHERE season = %s", (str(season),))
            
            # Reset pipeline control catalog status
            cur.execute(
                """
                UPDATE pipeline_control 
                SET bronze_status = NULL, staging_status = NULL, validation_status = NULL, 
                    silver_status = NULL, gold_status = NULL, status = 'NOT PROCESSED', 
                    last_processed_at = CURRENT_TIMESTAMP
                WHERE season = %s
                """,
                (str(season),)
            )
            
            # Log deletion action
            cur.execute(
                """
                INSERT INTO pipeline_logs (run_id, season, stage, start_time, end_time, status, 
                                           records_read, records_inserted, records_updated, records_deleted, 
                                           records_failed, error_message)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s)
                """,
                (f"DEL_{season}", str(season), "delete", "SUCCESS", 0, 0, 0, len(match_ids), 0, f"Deleted season data and reset pipeline control status.")
            )
            
        # 2. File staging cleanup
        from config.config import STAGING_PATH
        import shutil
        season_staging_dir = STAGING_PATH / str(season)
        if season_staging_dir.exists():
            shutil.rmtree(season_staging_dir)
            
        flash(f"Successfully deleted all data for Season {season} and reset registry status.", "success")
    except Exception as e:
        flash(f"Failed to delete data for Season {season}: {str(e)}", "danger")
        
    return redirect(url_for('pipeline.index'))

@pipeline_bp.route('/pipeline/reset', methods=['POST'])
def reset_platform():
    """Wipes the database, rebuilds schemas, and deletes all staging/quarantine files to reset the playground."""
    try:
        # 1. Database Wiping
        tables = [
            'FACT_MATCH_SUMMARY', 'DIM_TEAM', 'DIM_PLAYER',
            'team_squad', 'match_official', 'player_of_match', 'toss',
            'powerplay', 'wicket_fielder', 'wicket', 'delivery', 'innings',
            'official', 'player', 'team', 'match', 'source_registry',
            'pipeline_control', 'pipeline_logs', 'quarantine_records',
            'cdc_states', 'cdc_log', 'pipeline_runs'
        ]
        
        with get_db_cursor(commit=True) as cur:
            for t in tables:
                cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE;")
                
            # Recreate tables from database/schema.sql
            from config.config import PROJECT_ROOT
            from pathlib import Path
            schema_path = Path(PROJECT_ROOT) / "database" / "schema.sql"
            if schema_path.exists():
                with open(schema_path, "r", encoding="utf-8") as f:
                    cur.execute(f.read())
            else:
                raise FileNotFoundError(f"schema.sql not found at {schema_path}")
                
        # 2. Filesystem staging & quarantine purge
        from config.config import STAGING_PATH
        import shutil
        if STAGING_PATH.exists():
            shutil.rmtree(STAGING_PATH)
            STAGING_PATH.mkdir(parents=True, exist_ok=True)
            
        quarantine_path = Path(PROJECT_ROOT) / "data" / "quarantine"
        if quarantine_path.exists():
            shutil.rmtree(quarantine_path)
            quarantine_path.mkdir(parents=True, exist_ok=True)
            
        flash("Platform reset successfully! All database tables and staging folders have been cleared to start fresh.", "success")
    except Exception as e:
        flash(f"Failed to reset platform: {str(e)}", "danger")
        
    return redirect(url_for('pipeline.index'))
