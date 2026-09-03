import os
import json
from pathlib import Path
from config.config import STAGING_PATH, clean_team_name
from database.connection import get_db_cursor

def delete_existing_match_data(cur, match_ids):
    """Deletes existing matches and cascaded rows to guarantee idempotency on reprocessing."""
    if not match_ids:
        return
    cur.execute("DELETE FROM match WHERE match_id = ANY(%s)", (list(match_ids),))

def process_silver_match(cur, match_id, data):
    """Processes and inserts a single match JSON data into Silver tables."""
    info = data.get("info", {})
    innings = data.get("innings", [])
    
    # 1. Insert Teams (team table) - applying clean_team_name
    teams = [clean_team_name(t) for t in info.get("teams", [])]
    for t in teams:
        cur.execute(
            "INSERT INTO team (team_name) VALUES (%s) ON CONFLICT (team_name) DO NOTHING",
            (t,)
        )
        
    # 2. Insert Players (player table)
    registry = info.get("registry", {}).get("people", {})
    # Standardize player squad team names
    players_squad = {clean_team_name(team): squad for team, squad in info.get("players", {}).items()}
    
    # Collect all players mentioned in registry or squads
    all_players = set(registry.keys())
    for team_name, squad in players_squad.items():
        all_players.update(squad)
        
    for p in all_players:
        cricsheet_id = registry.get(p)
        cur.execute(
            """
            INSERT INTO player (player_name, cricsheet_id) 
            VALUES (%s, %s) 
            ON CONFLICT (player_name) DO UPDATE SET cricsheet_id = EXCLUDED.cricsheet_id
            """,
            (p, cricsheet_id)
        )
        
    # 3. Insert Officials (official table)
    officials = info.get("officials", {})
    for role, names in officials.items():
        for name in names:
            cur.execute(
                "INSERT INTO official (official_name, official_role) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (name, role)
            )
            
    # 4. Insert Match (match table) - applying clean_team_name to team1, team2, toss_winner, winner
    match_date = info.get("dates", [None])[0]
    scheduled_overs = info.get("overs", 20)
    balls_per_over = info.get("balls_per_over", 6)
    
    cur.execute(
        """
        INSERT INTO match (
            match_id, tournament, match_number, season, match_date, city, venue, 
            match_type, gender, scheduled_overs, balls_per_over, team_type, 
            team1, team2, toss_winner, toss_decision, winner, win_by_runs, 
            win_by_wickets, result, result_method, eliminator, player_of_match
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            match_id,
            info.get("event", {}).get("name"),
            str(info.get("event", {}).get("match_number")),
            str(info.get("season")),
            match_date,
            info.get("city"),
            info.get("venue"),
            info.get("match_type"),
            info.get("gender"),
            scheduled_overs,
            balls_per_over,
            info.get("team_type"),
            teams[0] if len(teams) > 0 else None,
            teams[1] if len(teams) > 1 else None,
            clean_team_name(info.get("toss", {}).get("winner")),
            info.get("toss", {}).get("decision"),
            clean_team_name(info.get("outcome", {}).get("winner")),
            info.get("outcome", {}).get("by", {}).get("runs"),
            info.get("outcome", {}).get("by", {}).get("wickets"),
            info.get("outcome", {}).get("result"),
            info.get("outcome", {}).get("method"),
            info.get("outcome", {}).get("eliminator"),
            info.get("player_of_match", [None])[0] if info.get("player_of_match") else None
        )
    )
    
    # 5. Insert Toss Details (toss table)
    toss = info.get("toss", {})
    if toss:
        cur.execute(
            "INSERT INTO toss (match_id, toss_winner, toss_decision) VALUES (%s, %s, %s)",
            (match_id, clean_team_name(toss.get("winner")), toss.get("decision"))
        )
        
    # 6. Insert Player of Match (player_of_match table)
    pom_list = info.get("player_of_match", [])
    for pom in pom_list:
        cur.execute(
            "INSERT INTO player_of_match (match_id, player_name) VALUES (%s, %s)",
            (match_id, pom)
        )
        
    # 7. Insert Match Officials (match_official table)
    for role, names in officials.items():
        for name in names:
            cur.execute(
                "INSERT INTO match_official (match_id, official_name, official_role) VALUES (%s, %s, %s)",
                (match_id, name, role)
            )
            
    # 8. Insert Team Squads (team_squad table)
    for team_name, squad in players_squad.items():
        for player_name in squad:
            cur.execute(
                "INSERT INTO team_squad (match_id, team_name, player_name) VALUES (%s, %s, %s)",
                (match_id, team_name, player_name)
            )
            
    # 9. Insert Innings and Deliveries
    for innings_number, inning in enumerate(innings, start=1):
        batting_team = clean_team_name(inning.get("team"))
        target = inning.get("target", {})
        
        cur.execute(
            """
            INSERT INTO innings (match_id, innings_number, batting_team, target_runs, target_overs)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                match_id,
                innings_number,
                batting_team,
                target.get("runs"),
                target.get("overs")
            )
        )
        
        # Powerplays
        for pplay in inning.get("powerplays", []):
            cur.execute(
                """
                INSERT INTO powerplay (match_id, innings_number, batting_team, from_delivery, to_delivery, powerplay_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    match_id,
                    innings_number,
                    batting_team,
                    pplay.get("from"),
                    pplay.get("to"),
                    pplay.get("type")
                )
            )
            
            
        # Deliveries & Wickets
        for over_data in inning.get("overs", []):
            over_number = over_data.get("over")
            
            for delivery_index, delivery_data in enumerate(over_data.get("deliveries", []), start=1):
                runs = delivery_data.get("runs", {})
                extras = delivery_data.get("extras", {})
                
                cur.execute(
                    """
                    INSERT INTO delivery (
                        match_id, innings_number, batting_team, over_number, actual_delivery, 
                        batter, bowler, non_striker, batter_runs, extras, total_runs, 
                        wides, noballs, byes, legbyes
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING delivery_id
                    """,
                    (
                        match_id,
                        innings_number,
                        batting_team,
                        over_number,
                        delivery_index,
                        delivery_data.get("batter"),
                        delivery_data.get("bowler"),
                        delivery_data.get("non_striker"),
                        runs.get("batter", 0),
                        runs.get("extras", 0),
                        runs.get("total", 0),
                        extras.get("wides", 0),
                        extras.get("noballs", 0),
                        extras.get("byes", 0),
                        extras.get("legbyes", 0)
                    )
                )
                delivery_id = cur.fetchone()[0]
                
                # Wickets
                if "wickets" in delivery_data:
                    for wicket in delivery_data["wickets"]:
                        cur.execute(
                            """
                            INSERT INTO wicket (
                                delivery_id, match_id, innings_number, over_number, actual_delivery,
                                batter, bowler, player_out, dismissal_kind
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING wicket_id
                            """,
                            (
                                delivery_id,
                                match_id,
                                innings_number,
                                over_number,
                                delivery_index,
                                delivery_data.get("batter"),
                                delivery_data.get("bowler"),
                                wicket.get("player_out"),
                                wicket.get("kind")
                            )
                        )
                        wicket_id = cur.fetchone()[0]
                        
                        # Fielder
                        for fielder in wicket.get("fielders", []):
                            cur.execute(
                                """
                                INSERT INTO wicket_fielder (wicket_id, delivery_id, player_name)
                                VALUES (%s, %s, %s)
                                """,
                                (wicket_id, delivery_id, fielder.get("name"))
                            )

def load_silver_season(season):
    """Loads all valid staged JSON matches for the season into Silver relational tables."""
    season_json_dir = STAGING_PATH / str(season) / "json"
    if not season_json_dir.exists():
        print(f"Staging JSON folder not found for season {season}")
        return {"loaded": 0, "failed": 0}
        
    match_files = list(season_json_dir.glob("*.json"))
    if not match_files:
        print(f"No match files found in staging for season {season}")
        return {"loaded": 0, "failed": 0}
        
    match_ids = [mf.stem for mf in match_files]
    
    # Process within a single transaction
    loaded_count = 0
    failed_count = 0
    
    print(f"Loading {len(match_files)} matches to Silver database for season {season}...")
    
    try:
        with get_db_cursor(commit=True) as cur:
            # Delete first to guarantee idempotency (Cascades delete to all child tables)
            delete_existing_match_data(cur, match_ids)
            
            for mf in match_files:
                match_id = mf.stem
                try:
                    with open(mf, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    process_silver_match(cur, match_id, data)
                    loaded_count += 1
                except Exception as e:
                    print(f"Error loading match {match_id} to database: {e}")
                    failed_count += 1
                    raise e # Trigger transaction rollback
                    
        print(f"Season {season} Silver Load complete: Loaded: {loaded_count}, Failed: {failed_count}")
        return {"loaded": loaded_count, "failed": failed_count}
        
    except Exception as e:
        print(f"Transaction rolled back for season {season} due to errors: {e}")
        return {"loaded": 0, "failed": len(match_files)}

if __name__ == "__main__":
    load_silver_season(2024)
