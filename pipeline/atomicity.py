import json
from pathlib import Path
from config.config import STAGING_PATH
from database.connection import get_connection
from pipeline.silver_pipeline import delete_existing_match_data, process_silver_match
from pipeline.gold_pipeline import (
    load_gold_match_summary, 
    rebuild_gold_team_dimension, 
    rebuild_gold_player_dimension,
    upsert_gold_team_dimension,
    upsert_gold_player_dimension
)

def load_season_atomic(season, pipeline_mode='INCREMENTAL', cdc_results=None, simulate_failure=False):
    """Loads Silver and Gold layers for a season within a single database transaction.
    Supports INCREMENTAL and REPROCESS modes using CDC operations (INSERT, UPDATE, DELETE, SKIP).
    If simulate_failure=True, intentionally raises an exception after Silver is loaded 
    but before Gold is committed to show transactional rollback.
    """
    season_json_dir = STAGING_PATH / str(season) / "json"
    if not season_json_dir.exists():
        raise FileNotFoundError(f"Staging JSON directory not found for season {season}")
        
    match_files = list(season_json_dir.glob("*.json"))
    if not match_files and pipeline_mode != 'INCREMENTAL':
        raise ValueError(f"No match files found in staging for season {season}")
        
    conn = get_connection()
    cur = conn.cursor()
    
    # Initialize run counts
    counts = {
        'read': len(match_files),
        'inserted': 0,
        'updated': 0,
        'deleted': 0,
        'skipped': 0,
        'failed': 0
    }
    
    try:
        print(f"[ATOMIC] Starting {pipeline_mode} transaction for season {season}...")
        
        if pipeline_mode == 'INCREMENTAL' and cdc_results:
            # 1. Handle Incremental loading
            matches_to_process = []
            
            for match_id, info in cdc_results.items():
                op = info['operation']
                filepath = info['filepath']
                
                if op == 'SKIP':
                    counts['skipped'] += 1
                    print(f"[ATOMIC-CDC] Skipping unchanged match {match_id}")
                    continue
                    
                if op == 'DELETE':
                    counts['deleted'] += 1
                    print(f"[ATOMIC-CDC] Deleting match {match_id} (missing from source)")
                    delete_existing_match_data(cur, [match_id])
                    cur.execute("DELETE FROM FACT_MATCH_SUMMARY WHERE match_id = %s", (match_id,))
                    continue
                    
                if op == 'UPDATE':
                    counts['updated'] += 1
                    print(f"[ATOMIC-CDC] Updating match {match_id}")
                    delete_existing_match_data(cur, [match_id])
                    cur.execute("DELETE FROM FACT_MATCH_SUMMARY WHERE match_id = %s", (match_id,))
                    matches_to_process.append((match_id, filepath))
                    
                if op == 'INSERT':
                    counts['inserted'] += 1
                    print(f"[ATOMIC-CDC] Inserting match {match_id}")
                    matches_to_process.append((match_id, filepath))
            
            # Load the new or updated matches into Silver Relational Layer
            for match_id, filepath in matches_to_process:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                process_silver_match(cur, match_id, data)
                
            print(f"[ATOMIC] Silver incremental load completed. Proceeding to Gold serving...")
            
            # Simulate failure check
            if simulate_failure:
                raise RuntimeError("SIMULATED_FAILURE: Intentional crash at Gold boundary to test transaction rollback.")
                
            # Gold Fact update: Load only affected match summaries
            for match_id, _ in matches_to_process:
                # Load gold match summary for this specific match ID
                cur.execute("DELETE FROM FACT_MATCH_SUMMARY WHERE match_id = %s", (match_id,))
                cur.execute(
                    """
                    INSERT INTO FACT_MATCH_SUMMARY (
                        match_id, tournament, match_number, season, match_date, city, venue, 
                        match_type, gender, scheduled_overs, balls_per_over, team_type, 
                        team1, team2, toss_winner, toss_decision, winner, win_by_runs, 
                        win_by_wickets, result, result_method, eliminator, player_of_match,
                        total_runs, total_wickets, total_boundaries, total_sixes
                    )
                    SELECT 
                        m.match_id, m.tournament, m.match_number, m.season, m.match_date, m.city, m.venue, 
                        m.match_type, m.gender, m.scheduled_overs, m.balls_per_over, m.team_type, 
                        m.team1, m.team2, m.toss_winner, m.toss_decision, m.winner, m.win_by_runs, 
                        m.win_by_wickets, m.result, m.result_method, m.eliminator, m.player_of_match,
                        COALESCE(r.total_runs, 0)::int,
                        COALESCE(w.total_wickets, 0)::int,
                        COALESCE(b.total_boundaries, 0)::int,
                        COALESCE(s.total_sixes, 0)::int
                    FROM match m
                    LEFT JOIN (
                        SELECT match_id, SUM(total_runs) as total_runs 
                        FROM delivery 
                        GROUP BY match_id
                    ) r ON m.match_id = r.match_id
                    LEFT JOIN (
                        SELECT match_id, COUNT(wicket_id) as total_wickets 
                        FROM wicket 
                        GROUP BY match_id
                    ) w ON m.match_id = w.match_id
                    LEFT JOIN (
                        SELECT match_id, COUNT(delivery_id) as total_boundaries 
                        FROM delivery 
                        WHERE batter_runs = 4 
                        GROUP BY match_id
                    ) b ON m.match_id = b.match_id
                    LEFT JOIN (
                        SELECT match_id, COUNT(delivery_id) as total_sixes 
                        FROM delivery 
                        WHERE batter_runs = 6 
                        GROUP BY match_id
                    ) s ON m.match_id = s.match_id
                    WHERE m.match_id = %s
                    """,
                    (match_id,)
                )
            
            # Upsert dimensions (ON CONFLICT DO UPDATE)
            upsert_gold_team_dimension(cur)
            upsert_gold_player_dimension(cur)
            
        else:
            # 2. Handle Reprocess / Full load mode
            match_ids = [mf.stem for mf in match_files]
            
            # Silver Layer: Delete all matching season entries
            delete_existing_match_data(cur, match_ids)
            
            # Silver Layer: Insert fresh
            for mf in match_files:
                with open(mf, "r", encoding="utf-8") as f:
                    data = json.load(f)
                process_silver_match(cur, mf.stem, data)
                counts['inserted'] += 1
                
            print(f"[ATOMIC] Silver reprocess load completed ({len(match_files)} matches). Proceeding to Gold...")
            
            if simulate_failure:
                raise RuntimeError("SIMULATED_FAILURE: Intentional crash at Gold boundary to test transaction rollback.")
                
            # Gold Layer: Load FACT_MATCH_SUMMARY
            load_gold_match_summary(cur, season)
            
            # Gold Layer: Full Rebuild of dimensions
            rebuild_gold_team_dimension(cur)
            rebuild_gold_player_dimension(cur)
            
        # Commit transaction
        conn.commit()
        print(f"[ATOMIC] {pipeline_mode} transaction COMMITTED successfully for season {season}!")
        return {"status": "SUCCESS", "counts": counts}
        
    except Exception as e:
        conn.rollback()
        print(f"[ATOMIC] Transaction ROLLED BACK due to error: {e}")
        counts['failed'] = counts['read']
        raise e
        
    finally:
        cur.close()
        conn.close()
