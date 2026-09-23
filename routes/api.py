from flask import Blueprint, jsonify, request
from pipeline.pipeline_runner import get_pipeline_status
from database.connection import get_db_cursor

api_bp = Blueprint('api', __name__)

@api_bp.route('/api/pipeline/status/<run_id>')
def status(run_id):
    """Returns the live progress of the pipeline run."""
    st = get_pipeline_status(run_id)
    if not st:
        return jsonify({"status": "unknown", "progress": 0, "logs": []})
    return jsonify(st)

@api_bp.route('/api/pipeline/check-seasons', methods=['GET'])
def check_seasons():
    """Checks the pipeline_control database table for status of all seasons."""
    seasons_status = []
    try:
        with get_db_cursor(commit=False, cursor_factory='dict') as cur:
            cur.execute(
                """
                SELECT season, bronze_status, staging_status, validation_status, silver_status, gold_status, status
                FROM pipeline_control
                ORDER BY season ASC
                """
            )
            seasons_status = cur.fetchall()
    except Exception as e:
        print(f"Failed to query seasons status: {e}")
        
    return jsonify([dict(row) for row in seasons_status])

@api_bp.route('/api/analytics/query', methods=['POST'])
def query_analytics():
    """Executes dynamic filters against the Star Schema/Gold layer and returns KPI cards and charts data."""
    data = request.get_json() or {}
    
    season_mode = data.get('season_mode', 'all')
    single_season = data.get('single_season')
    from_season = data.get('from_season')
    to_season = data.get('to_season')
    team = data.get('team')
    batting_team = data.get('batting_team')
    bowling_team = data.get('bowling_team')
    player = data.get('player')
    venue = data.get('venue')
    
    # 1. Build dynamic match-level WHERE clauses
    where_clauses = []
    params = []
    
    if season_mode == 'single' and single_season:
        where_clauses.append("season = %s")
        params.append(str(single_season))
    elif season_mode == 'range' and from_season and to_season:
        where_clauses.append("season >= %s AND season <= %s")
        params.extend([str(from_season), str(to_season)])
        
    if team:
        where_clauses.append("(team1 = %s OR team2 = %s)")
        params.extend([team, team])
        
    if venue:
        where_clauses.append("venue = %s")
        params.append(venue)
        
    where_str = " AND ".join(where_clauses)
    where_sql = f"WHERE {where_str}" if where_clauses else ""
    
    # Initialise default KPI metrics
    kpi = {
        "matches": 0,
        "runs": 0,
        "wickets": 0,
        "boundaries": 0,
        "sixes": 0,
        "players": 0
    }
    
    # Chart data placeholders
    runs_by_season = {"labels": [], "data": []}
    top_batters = {"labels": [], "data": []}
    top_bowlers = {"labels": [], "data": []}
    venue_matches = {"labels": [], "data": []}
    
    try:
        with get_db_cursor(commit=False, cursor_factory='dict') as cur:
            # Query match-level aggregates if no specific player is filtered
            if not player:
                # KPI Card data
                cur.execute(
                    f"""
                    SELECT 
                        COUNT(match_id) as matches,
                        SUM(total_runs) as runs,
                        SUM(total_wickets) as wickets,
                        SUM(total_boundaries) as boundaries,
                        SUM(total_sixes) as sixes
                    FROM FACT_MATCH_SUMMARY
                    {where_sql}
                    """,
                    params
                )
                res = cur.fetchone()
                if res and res['matches'] > 0:
                    kpi["matches"] = res["matches"]
                    kpi["runs"] = int(res["runs"]) if res["runs"] else 0
                    kpi["wickets"] = int(res["wickets"]) if res["wickets"] else 0
                    kpi["boundaries"] = int(res["boundaries"]) if res["boundaries"] else 0
                    kpi["sixes"] = int(res["sixes"]) if res["sixes"] else 0
                    
                # Active player count in selected matches
                if where_clauses:
                    cur.execute(
                        f"""
                        SELECT COUNT(DISTINCT player_name) as count
                        FROM team_squad 
                        WHERE match_id IN (SELECT match_id FROM FACT_MATCH_SUMMARY {where_sql})
                        """,
                        params
                    )
                else:
                    cur.execute("SELECT COUNT(*) as count FROM player")
                kpi["players"] = cur.fetchone()["count"]
                
            else:
                # Query player-specific statistics
                # Batting KPIs
                cur.execute(
                    """
                    SELECT batting_matches, batting_innings, total_runs, total_balls, total_fours, total_sixes,
                           bowling_matches, bowling_innings, balls_bowled, runs_conceded, wickets_taken
                    FROM DIM_PLAYER
                    WHERE player_name = %s
                    """,
                    (player,)
                )
                p_res = cur.fetchone()
                if p_res:
                    kpi["matches"] = p_res["batting_matches"] or p_res["bowling_matches"] or 0
                    kpi["runs"] = p_res["total_runs"] or 0
                    kpi["wickets"] = p_res["wickets_taken"] or 0
                    kpi["boundaries"] = p_res["total_fours"] or 0
                    kpi["sixes"] = p_res["total_sixes"] or 0
                    kpi["players"] = 1
                    
            # 2. Runs by Season chart query
            cur.execute(
                f"""
                SELECT season, SUM(total_runs) as runs
                FROM FACT_MATCH_SUMMARY
                {where_sql}
                GROUP BY season
                ORDER BY season ASC
                """,
                params
            )
            for row in cur.fetchall():
                runs_by_season["labels"].append(row["season"])
                runs_by_season["data"].append(int(row["runs"]) if row["runs"] else 0)
                
            # 3. Top Batters chart query
            if team:
                # Filter players by team
                cur.execute(
                    """
                    SELECT p.player_name, p.total_runs 
                    FROM DIM_PLAYER p
                    JOIN team_squad s ON p.player_name = s.player_name
                    WHERE s.team_name = %s AND p.total_runs > 0
                    GROUP BY p.player_name, p.total_runs
                    ORDER BY p.total_runs DESC
                    LIMIT 10
                    """,
                    (team,)
                )
            else:
                cur.execute(
                    "SELECT player_name, total_runs FROM DIM_PLAYER WHERE total_runs > 0 ORDER BY total_runs DESC LIMIT 10"
                )
            for row in cur.fetchall():
                top_batters["labels"].append(row["player_name"])
                top_batters["data"].append(row["total_runs"])
                
            # 4. Top Bowlers chart query
            if team:
                cur.execute(
                    """
                    SELECT p.player_name, p.wickets_taken 
                    FROM DIM_PLAYER p
                    JOIN team_squad s ON p.player_name = s.player_name
                    WHERE s.team_name = %s AND p.wickets_taken > 0
                    GROUP BY p.player_name, p.wickets_taken
                    ORDER BY p.wickets_taken DESC
                    LIMIT 10
                    """,
                    (team,)
                )
            else:
                cur.execute(
                    "SELECT player_name, wickets_taken FROM DIM_PLAYER WHERE wickets_taken > 0 ORDER BY wickets_taken DESC LIMIT 10"
                )
            for row in cur.fetchall():
                top_bowlers["labels"].append(row["player_name"])
                top_bowlers["data"].append(row["wickets_taken"])
                
            # 5. Venue distribution query
            cur.execute(
                f"""
                SELECT venue, COUNT(match_id) as matches
                FROM FACT_MATCH_SUMMARY
                {where_sql}
                GROUP BY venue
                ORDER BY matches DESC
                LIMIT 10
                """,
                params
            )
            for row in cur.fetchall():
                venue_matches["labels"].append(row["venue"])
                venue_matches["data"].append(row["matches"])
                
            # 6. Generate Grouped Season-wise Reports (Team/Player/Overall Year-wise)
            reports = {"type": "overall", "rows": []}
            if player:
                cur.execute(
                    """
                    SELECT 
                        COALESCE(bat.season, bowl.season) as season,
                        COALESCE(bat.matches, 0)::int as batting_matches,
                        COALESCE(bat.runs, 0)::int as batting_runs,
                        COALESCE(bat.balls, 0)::int as batting_balls,
                        COALESCE(bowl.matches, 0)::int as bowling_matches,
                        COALESCE(bowl.wickets, 0)::int as bowling_wickets,
                        COALESCE(bowl.runs_conceded, 0)::int as runs_conceded
                    FROM (
                        SELECT 
                            m.season,
                            COUNT(DISTINCT d.match_id) as matches,
                            SUM(d.batter_runs) as runs,
                            COUNT(d.delivery_id) as balls
                        FROM match m
                        JOIN delivery d ON m.match_id = d.match_id
                        WHERE d.batter = %s
                        GROUP BY m.season
                    ) bat
                    FULL OUTER JOIN (
                        SELECT 
                            m.season,
                            COUNT(DISTINCT d.match_id) as matches,
                            SUM(d.total_runs) as runs_conceded,
                            COUNT(w.wicket_id) as wickets
                        FROM match m
                        JOIN delivery d ON m.match_id = d.match_id
                        LEFT JOIN wicket w ON d.delivery_id = w.delivery_id
                        WHERE d.bowler = %s
                        GROUP BY m.season
                    ) bowl ON bat.season = bowl.season
                    ORDER BY season DESC
                    """,
                    (player, player)
                )
                reports["type"] = "player"
                for r in cur.fetchall():
                    reports["rows"].append({
                        "season": r["season"],
                        "bat_matches": r["batting_matches"],
                        "runs": r["batting_runs"],
                        "balls": r["batting_balls"],
                        "bow_matches": r["bowling_matches"],
                        "wickets": r["bowling_wickets"],
                        "conceded": r["runs_conceded"]
                    })
            elif team:
                cur.execute(
                    """
                    SELECT 
                        m.season,
                        COUNT(DISTINCT m.match_id) as matches_played,
                        COUNT(DISTINCT CASE WHEN m.winner = %s THEN m.match_id END) as wins,
                        COALESCE(SUM(d.total_runs), 0)::int as runs,
                        COALESCE(SUM(CASE WHEN d.batter_runs = 4 THEN 1 ELSE 0 END), 0)::int as fours,
                        COALESCE(SUM(CASE WHEN d.batter_runs = 6 THEN 1 ELSE 0 END), 0)::int as sixes
                    FROM match m
                    LEFT JOIN delivery d ON m.match_id = d.match_id AND d.batting_team = %s
                    WHERE m.team1 = %s OR m.team2 = %s
                    GROUP BY m.season
                    ORDER BY m.season DESC
                    """,
                    (team, team, team, team)
                )
                reports["type"] = "team"
                for r in cur.fetchall():
                    reports["rows"].append({
                        "season": r["season"],
                        "matches": r["matches_played"],
                        "wins": r["wins"],
                        "runs": r["runs"],
                        "fours": r["fours"],
                        "sixes": r["sixes"]
                    })
            else:
                cur.execute(
                    """
                    SELECT 
                        season, 
                        COUNT(match_id) as matches, 
                        COALESCE(SUM(total_runs), 0)::int as runs, 
                        COALESCE(SUM(total_wickets), 0)::int as wickets 
                    FROM FACT_MATCH_SUMMARY 
                    GROUP BY season 
                    ORDER BY season DESC
                    """
                )
                reports["type"] = "overall"
                for r in cur.fetchall():
                    reports["rows"].append({
                        "season": r["season"],
                        "matches": r["matches"],
                        "runs": r["runs"],
                        "wickets": r["wickets"]
                    })
                
    except Exception as e:
        print(f"Failed to query analytics statistics: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
        
    return jsonify({
        "kpi": kpi,
        "charts": {
            "runs_by_season": runs_by_season,
            "top_batters": top_batters,
            "top_bowlers": top_bowlers,
            "venue_matches": venue_matches
        },
        "reports": reports
    })

@api_bp.route('/api/eda/data/<season>')
def eda_data(season):
    """Retrieves computed staging-layer EDA metrics for the specified season."""
    from pipeline.eda import calculate_season_eda
    data = calculate_season_eda(season)
    if not data:
        return jsonify({
            "status": "error", 
            "message": f"EDA data not available for Season {season}. Please ensure it is staged and validated first."
        })
    return jsonify(data)

ALLOWED_INSPECT_TABLES = [
    'match', 'team', 'player', 'official', 'innings', 'delivery', 'wicket',
    'team_squad', 'powerplay', 'toss', 'player_of_match', 'match_official',
    'FACT_MATCH_SUMMARY', 'DIM_TEAM', 'DIM_PLAYER',
    'source_registry', 'pipeline_control', 'quarantine_records',
    'cdc_states', 'cdc_log', 'pipeline_runs'
]

@api_bp.route('/api/database/stats')
def database_stats():
    """Queries row counts from all Silver and Gold database tables."""
    stats = {}
    try:
        with get_db_cursor(commit=False) as cur:
            for tbl in ALLOWED_INSPECT_TABLES:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                    stats[tbl] = cur.fetchone()[0]
                except Exception:
                    stats[tbl] = 0
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify(stats)

@api_bp.route('/api/database/sample', methods=['POST'])
def database_sample():
    """Fetches top 10 sample rows of any allowed database table for inspection."""
    req_data = request.get_json() or {}
    table = req_data.get("table")
    season = req_data.get("season")
    
    if table not in ALLOWED_INSPECT_TABLES:
        return jsonify({"status": "error", "message": "Unauthorized table access."}), 400
        
    try:
        from datetime import date, datetime
        with get_db_cursor(commit=False, cursor_factory='dict') as cur:
            # Query table with optional season filter
            if season and table in ['match', 'FACT_MATCH_SUMMARY']:
                cur.execute(f"SELECT * FROM {table} WHERE season = %s LIMIT 10", (str(season),))
            elif season and table in ['innings', 'delivery', 'wicket', 'team_squad', 'powerplay', 'toss', 'player_of_match', 'match_official']:
                cur.execute(f"SELECT * FROM {table} WHERE match_id IN (SELECT match_id FROM match WHERE season = %s) LIMIT 10", (str(season),))
            else:
                cur.execute(f"SELECT * FROM {table} LIMIT 10")
                
            rows = cur.fetchall()
            
            # Format row records for JSON serialization (dates/objects need to be strings)
            formatted_rows = []
            for r in rows:
                row_dict = dict(r)
                for k, v in row_dict.items():
                    if isinstance(v, (date, datetime)) or hasattr(v, 'isoformat'):
                        row_dict[k] = v.isoformat()
                    elif isinstance(v, (int, float, str, bool)) or v is None:
                        pass
                    else:
                        row_dict[k] = str(v)
                formatted_rows.append(row_dict)
                
            return jsonify({
                "columns": list(rows[0].keys()) if rows else [],
                "rows": formatted_rows
            })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@api_bp.route('/api/db/status', methods=['GET'])
def get_database_status():
    """Returns the current active database target and connection health for Online & Offline."""
    from config.config import get_active_db_mode
    from database.connection import test_db_connection
    
    active_mode = get_active_db_mode()
    online_stat = test_db_connection("online")
    offline_stat = test_db_connection("offline")
    
    return jsonify({
        "status": "success",
        "active_mode": active_mode,
        "active_label": "Online (Supabase Cloud)" if active_mode == "online" else "Offline (Local PostgreSQL)",
        "online": online_stat,
        "offline": offline_stat
    })


@api_bp.route('/api/db/switch', methods=['POST'])
def switch_database():
    """Switches the active database target between 'online' (Supabase) and 'offline' (Local)."""
    from config.config import set_active_db_mode, get_active_db_mode
    from database.connection import test_db_connection
    
    data = request.get_json() or {}
    target_mode = data.get("mode", "").strip().lower()
    
    if target_mode not in ["online", "offline", "local"]:
        return jsonify({"status": "error", "message": "Invalid mode. Choose 'online' or 'offline'."}), 400
        
    normalized_mode = "offline" if target_mode in ["offline", "local"] else "online"
    new_mode = set_active_db_mode(normalized_mode)
    conn_result = test_db_connection(new_mode)
    
    return jsonify({
        "status": "success",
        "active_mode": new_mode,
        "active_label": "Online (Supabase Cloud)" if new_mode == "online" else "Offline (Local PostgreSQL)",
        "connection": conn_result,
        "message": f"Successfully switched to {conn_result['name']}."
    })


@api_bp.route('/api/lakehouse/status', methods=['GET'])

def get_lakehouse_status():
    """Returns status of MinIO, Nessie Catalog, and Iceberg table metadata."""
    import requests
    from config.config import (
        MINIO_ENDPOINT, NESSIE_URI, ICEBERG_CATALOG_NAME, 
        ICEBERG_TABLE_NAME, ICEBERG_CHECKPOINT_PATH, PROJECT_ROOT
    )
    from pipeline.iceberg_pipeline import is_minio_reachable, verify_checkpoint_recovery
    
    minio_alive = is_minio_reachable()
    nessie_alive = False
    try:
        r = requests.get(f"{NESSIE_URI}/config", timeout=1)
        nessie_alive = (r.status_code == 200)
    except Exception:
        pass
        
    # Check local warehouse partitions
    warehouse_local = PROJECT_ROOT / "data" / "warehouse" / "ipl" / "ipl_matches"
    partitions = [p.name for p in warehouse_local.glob("season=*")] if warehouse_local.exists() else []
    metadata_count = len(list((warehouse_local / "metadata").glob("*.json"))) if warehouse_local.exists() else 0
    
    chk_ok, chk_state = verify_checkpoint_recovery()

    return jsonify({
        "status": "success",
        "minio": {
            "endpoint": MINIO_ENDPOINT,
            "connected": minio_alive
        },
        "nessie": {
            "uri": NESSIE_URI,
            "connected": nessie_alive
        },
        "iceberg": {
            "catalog": ICEBERG_CATALOG_NAME,
            "table": ICEBERG_TABLE_NAME,
            "partition_key": "season",
            "partitions": partitions,
            "snapshots_count": metadata_count,
            "checkpoint": chk_state
        }
    })


@api_bp.route('/api/lakehouse/sync', methods=['POST'])
def trigger_lakehouse_sync():
    """Triggers on-demand Lakehouse Iceberg stream sync for requested seasons."""
    from pipeline.iceberg_pipeline import run_iceberg_streaming_pipeline
    data = request.get_json() or {}
    seasons = data.get("seasons", [2024])
    try:
        res = run_iceberg_streaming_pipeline(seasons)
        return jsonify(res)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

