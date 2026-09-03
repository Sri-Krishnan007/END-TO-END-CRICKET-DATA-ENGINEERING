import json
from pathlib import Path
from config.config import STAGING_PATH, clean_team_name

def calculate_season_eda(season):
    """Calculates staging-layer EDA metrics on raw JSON files before importing to Database."""
    season_json_dir = STAGING_PATH / str(season) / "json"
    if not season_json_dir.exists():
        return None
        
    match_files = list(season_json_dir.glob("*.json"))
    if not match_files:
        return None
        
    # Metrics aggregators
    total_matches = len(match_files)
    total_deliveries = 0
    total_runs = 0
    total_wickets = 0
    total_fours = 0
    total_sixes = 0
    total_dots = 0
    
    team_runs = {}
    team_wins = {}
    team_matches = {}
    player_runs = {}
    player_wickets = {}
    venue_matches = {}
    
    all_players = set()
    all_teams = set()
    
    for mf in match_files:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
            
        info = data.get("info", {})
        innings = data.get("innings", [])
        
        # Venue
        venue = info.get("venue", "Unknown Venue")
        venue_matches[venue] = venue_matches.get(venue, 0) + 1
        
        # Teams & Wins
        teams = [clean_team_name(t) for t in info.get("teams", [])]
        for t in teams:
            all_teams.add(t)
            team_matches[t] = team_matches.get(t, 0) + 1
            
        winner = clean_team_name(info.get("outcome", {}).get("winner"))
        if winner:
            team_wins[winner] = team_wins.get(winner, 0) + 1
            
        # Players squad
        players_dict = info.get("players", {})
        for t, squad in players_dict.items():
            for p in squad:
                all_players.add(p)
                
        # Innings deliveries
        for inning in innings:
            batting_team = clean_team_name(inning.get("team"))
            for over_data in inning.get("overs", []):
                for deliv in over_data.get("deliveries", []):
                    total_deliveries += 1
                    runs = deliv.get("runs", {})
                    bat_runs = runs.get("batter", 0)
                    tot_runs = runs.get("total", 0)
                    
                    total_runs += tot_runs
                    team_runs[batting_team] = team_runs.get(batting_team, 0) + tot_runs
                    
                    # Batter runs
                    batter = deliv.get("batter")
                    player_runs[batter] = player_runs.get(batter, 0) + bat_runs
                    
                    # Bowler wickets and extras
                    bowler = deliv.get("bowler")
                    
                    # Wickets
                    if "wickets" in deliv:
                        for w in deliv["wickets"]:
                            total_wickets += 1
                            # Exclude run outs for bowler wickets
                            if w.get("kind") not in ["run out", "retired hurt"]:
                                player_wickets[bowler] = player_wickets.get(bowler, 0) + 1
                                
                    # Fours and Sixes
                    if bat_runs == 4:
                        total_fours += 1
                    elif bat_runs == 6:
                        total_sixes += 1
                        
                    # Dot balls
                    extras_dict = deliv.get("extras", {})
                    is_extra = any(k in extras_dict for k in ["wides", "noballs"])
                    if tot_runs == 0 and not is_extra:
                        total_dots += 1
                        
    # Sort top lists
    sorted_batters = sorted(player_runs.items(), key=lambda x: x[1], reverse=True)[:10]
    sorted_bowlers = sorted(player_wickets.items(), key=lambda x: x[1], reverse=True)[:10]
    sorted_venues = sorted(venue_matches.items(), key=lambda x: x[1], reverse=True)[:10]
    sorted_team_runs = sorted(team_runs.items(), key=lambda x: x[1], reverse=True)
    
    # Format for JSON response
    eda_data = {
        "kpi": {
            "matches": total_matches,
            "deliveries": total_deliveries,
            "runs": total_runs,
            "wickets": total_wickets,
            "boundaries": total_fours,
            "sixes": total_sixes,
            "players": len(all_players),
            "teams": len(all_teams)
        },
        "charts": {
            "runs_by_team": {
                "labels": [x[0] for x in sorted_team_runs],
                "data": [x[1] for x in sorted_team_runs]
            },
            "top_batters": {
                "labels": [x[0] for x in sorted_batters],
                "data": [x[1] for x in sorted_batters]
            },
            "top_bowlers": {
                "labels": [x[0] for x in sorted_bowlers],
                "data": [x[1] for x in sorted_bowlers]
            },
            "boundary_dist": {
                "labels": ["Fours", "Sixes", "Other Runs"],
                "data": [total_fours * 4, total_sixes * 6, max(0, total_runs - (total_fours * 4 + total_sixes * 6))]
            },
            "six_dist": {
                "labels": ["Sixes", "Other runs"],
                "data": [total_sixes * 6, max(0, total_runs - total_sixes * 6)]
            },
            "dot_ball_pct": {
                "labels": ["Dot Balls", "Scoring Balls"],
                "data": [total_dots, max(0, total_deliveries - total_dots)]
            },
            "venue_matches": {
                "labels": [x[0] for x in sorted_venues],
                "data": [x[1] for x in sorted_venues]
            },
            "team_performance": {
                "labels": list(all_teams),
                "wins": [team_wins.get(t, 0) for t in all_teams],
                "matches": [team_matches.get(t, 0) for t in all_teams]
            }
        }
    }
    
    return eda_data
