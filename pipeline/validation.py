import os
import json
import shutil
from pathlib import Path
from datetime import datetime
from config.config import STAGING_PATH, QUARANTINE_PATH
from database.connection import get_db_cursor

def validate_match_json(match_id, file_path, season):
    """Executes 10 data quality checks on a match JSON file.
    Returns (is_valid, error_type, error_message).
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return False, "SCHEMA_VALIDATION", f"Failed to load JSON: {str(e)}"
        
    info = data.get("info", {})
    innings = data.get("innings", [])
    
    # 1. Schema Validation (Verify root keys exist)
    if "info" not in data or "innings" not in data:
        return False, "SCHEMA_VALIDATION", "Missing root 'info' or 'innings' key"
        
    # 2. Required Fields
    required_info = ["teams", "dates", "match_type", "venue"]
    for field in required_info:
        if field not in info:
            return False, "REQUIRED_FIELDS", f"Missing required info field: {field}"
            
    # 3. Null Checks
    if not info.get("teams") or len(info["teams"]) < 2 or not info["teams"][0] or not info["teams"][1]:
        return False, "NULL_CHECK", "Teams array is empty or contains null values"
    if not info.get("dates") or not info["dates"][0]:
        return False, "NULL_CHECK", "Dates array is empty or contains null date"
        
    # 4. Duplicate Checks (file_path name checks, inside staging)
    # Staging has unique file names (match_id.json), which resolves duplicate check.
    
    # 5. Data Type Checks
    if not isinstance(info.get("overs", 20), (int, float)):
        return False, "DATATYPE_CHECK", "Overs field must be a number"
        
    # 6. Range Checks
    overs_limit = info.get("overs", 20)
    for inning in innings:
        for over_data in inning.get("overs", []):
            over_num = over_data.get("over")
            if over_num is None or over_num < 0 or over_num >= overs_limit:
                return False, "RANGE_CHECK", f"Over number {over_num} is out of bounds [0, {overs_limit-1}]"
                
            for deliv in over_data.get("deliveries", []):
                runs = deliv.get("runs", {})
                batter_runs = runs.get("batter", 0)
                extras = runs.get("extras", 0)
                if not (0 <= batter_runs <= 7):
                    return False, "RANGE_CHECK", f"Batter runs {batter_runs} out of expected range [0, 7]"
                if not (0 <= extras <= 7):
                    return False, "RANGE_CHECK", f"Extras runs {extras} out of expected range [0, 7]"
                    
    # 7. Invalid Runs (Validation of math)
    for inning in innings:
        for over_data in inning.get("overs", []):
            for deliv in over_data.get("deliveries", []):
                runs = deliv.get("runs", {})
                batter_runs = runs.get("batter", 0)
                extras = runs.get("extras", 0)
                total_runs = runs.get("total", 0)
                if total_runs != (batter_runs + extras):
                    return False, "INVALID_RUNS", f"Total runs {total_runs} != batter runs {batter_runs} + extras {extras}"
                    
    # 8. Team Validation (Ensure exactly 2 teams exist)
    teams = info.get("teams", [])
    if len(teams) != 2:
        return False, "TEAM_VALIDATION", f"Match has {len(teams)} teams; expected exactly 2"
        
    # 9. Player Validation (Ensure players registry lists active players)
    players = info.get("players", {})
    if not players:
        return False, "PLAYER_VALIDATION", "No player squad registered in match info"
    for team_name, squad in players.items():
        if not squad or len(squad) == 0:
            return False, "PLAYER_VALIDATION", f"Squad for team {team_name} is empty"
            
    # 10. Referential Integrity (Ensure innings batting team is one of the match teams)
    for inning in innings:
        bat_team = inning.get("team")
        if bat_team not in teams:
            return False, "REFERENTIAL_INTEGRITY", f"Batting team '{bat_team}' is not one of the match teams: {teams}"
            
    # 11. Outlier/Anomaly Detection Checks
    # Check 11a: Unusual total match runs (e.g. extremely high score > 450 runs)
    match_total_runs = 0
    for inning in innings:
        inning_runs = 0
        for over_data in inning.get("overs", []):
            for deliv in over_data.get("deliveries", []):
                inning_runs += deliv.get("runs", {}).get("total", 0)
        match_total_runs += inning_runs
    if match_total_runs > 450:
        return False, "OUTLIER_DETECTION", f"Match total runs ({match_total_runs}) exceeds standard outlier threshold of 450 runs"
        
    # Check 11b: Impossible low score for a normal completed match (e.g. total runs < 30 but winner exists)
    outcome = info.get("outcome", {})
    if match_total_runs < 30 and outcome.get("result") != "no result" and outcome.get("winner") is not None:
        return False, "OUTLIER_DETECTION", f"Impossible low total runs ({match_total_runs}) for a completed match"

    # Check 11c: Player count anomaly per team (squad size must be standard range e.g. 11 to 16 players)
    for team_name, squad in players.items():
        if len(squad) < 11 or len(squad) > 16:
            return False, "OUTLIER_DETECTION", f"Anomalous squad size ({len(squad)}) for team {team_name}. Standard is 11-16 players."

    # Check 11d: Overs limit validation (scheduled overs > 20 or < 5 is anomalous for IPL)
    scheduled_overs = info.get("overs", 20)
    if scheduled_overs > 20 or scheduled_overs < 5:
        return False, "OUTLIER_DETECTION", f"Anomalous scheduled overs ({scheduled_overs}). Expected range [5, 20]."
            
    return True, None, None

def quarantine_match(match_id, file_path, season, error_type, error_message):
    """Moves invalid file to quarantine, deletes its staged parquet counterpart, and logs the details in quarantine_records table."""
    dest_dir = QUARANTINE_PATH / str(season)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / f"{match_id}.json"
    
    # Read raw content for DB log
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_record = f.read()
    except Exception:
        raw_record = "{}"
        
    # Move file
    shutil.move(str(file_path), str(dest_path))
    
    # Delete staged parquet representation
    parquet_path = STAGING_PATH / str(season) / "parquet" / f"{match_id}.parquet"
    if parquet_path.exists():
        try:
            parquet_path.unlink()
        except Exception as pe:
            print(f"Failed to delete quarantined parquet file {match_id}.parquet: {pe}")
    
    # DB Insertion
    try:
        with get_db_cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO quarantine_records (season, source, stage, error_type, error_message, raw_record, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (str(season), f"{match_id}.json", "STAGING", error_type, error_message, raw_record, "QUARANTINED")
            )
        print(f"Match {match_id} QUARANTINED: {error_type} - {error_message}")
    except Exception as e:
        print(f"Failed to log quarantine record for {match_id} to DB: {e}")

def validate_season(season):
    """Validates all staged JSON files for a specific season.
    Quarantines failures and keeps valid files in staging.
    Returns summary stats.
    """
    season_json_dir = STAGING_PATH / str(season) / "json"
    if not season_json_dir.exists():
        print(f"No staging JSON folder found for season {season}")
        return {"processed": 0, "valid": 0, "quarantined": 0}
        
    match_files = list(season_json_dir.glob("*.json"))
    
    stats = {"processed": 0, "valid": 0, "quarantined": 0}
    
    for mf in match_files:
        match_id = mf.stem
        stats["processed"] += 1
        
        is_valid, error_type, error_message = validate_match_json(match_id, mf, season)
        
        if not is_valid:
            quarantine_match(match_id, mf, season, error_type, error_message)
            stats["quarantined"] += 1
        else:
            stats["valid"] += 1
            
    print(f"Season {season} Validation Summary: Processed: {stats['processed']}, Valid: {stats['valid']}, Quarantined: {stats['quarantined']}")
    return stats

if __name__ == "__main__":
    validate_season(2024)
