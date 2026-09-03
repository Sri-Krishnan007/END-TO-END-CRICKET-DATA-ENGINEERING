import json
import zipfile
import shutil
import pandas as pd
from pathlib import Path
from config.config import BRONZE_PATH, STAGING_PATH, clean_team_name

ZIP_PATH = BRONZE_PATH / "source" / "ipl_json.zip"

def get_match_season(match_data):
    """Extracts the season year from the match JSON.
    Uses info.season if available; otherwise falls back to info.dates year.
    Cleans up any multi-year formats (e.g. '2020/21' -> '2020').
    """
    info = match_data.get("info", {})
    season = info.get("season")
    if not season:
        dates = info.get("dates", [])
        if dates:
            season = dates[0].split("-")[0]
            
    if season:
        season_str = str(season).strip()
        # Handle '2020/21' or '2007/08'
        if "/" in season_str:
            season_str = season_str.split("/")[0]
        return season_str
    return None

def clear_staging_season(season):
    """Clears any existing staging data for a specific season to ensure idempotency."""
    season_dir = STAGING_PATH / str(season)
    if season_dir.exists():
        shutil.rmtree(season_dir)
    # Create subdirectories for JSON and Parquet staging layers
    (season_dir / "json").mkdir(parents=True, exist_ok=True)
    (season_dir / "parquet").mkdir(parents=True, exist_ok=True)

def stage_seasons(target_seasons, target_team=None, target_player=None):
    """Extracts match JSONs from the Cricsheet ZIP and copies them to season-specific staging folders
    by routing them through Kafka. Applies optional team-wise and player-wise filtering.
    """
    target_seasons_str = [str(s) for s in target_seasons]
    staged_counts = {s: 0 for s in target_seasons_str}
    
    if not ZIP_PATH.exists():
        raise FileNotFoundError(f"Source zip file not found at {ZIP_PATH}. Run ingestion first.")
        
    # Clear staging dirs for the target seasons before copying
    for season in target_seasons_str:
        clear_staging_season(season)
        
    print(f"Staging matches for seasons: {target_seasons_str} via Kafka...")
    if target_team:
        print(f"Filter Team: {clean_team_name(target_team)}")
    if target_player:
        print(f"Filter Player: {target_player}")
    
    # 1. Run Kafka Producer to push matches to topic
    from pipeline.kafka_client import run_producer, run_consumer
    published = run_producer(target_seasons, target_team=target_team, target_player=target_player)
    print(f"Staging Producer: Published {published} matching files to Kafka topic.")
    
    # 2. Run Kafka Consumer to fetch matches and write to staging directories
    consumed = run_consumer(target_seasons)
    print(f"Staging Consumer: Staged {consumed} files from Kafka topic.")
    
    # 3. Populate return counts based on files physically written in staging folders
    for season in target_seasons_str:
        json_dir = STAGING_PATH / season / "json"
        if json_dir.exists():
            staged_counts[season] = len(list(json_dir.glob("*.json")))
            
    for season, count in staged_counts.items():
        print(f"Season {season}: Staged {count} matches via Kafka consumer.")
        
    return staged_counts

if __name__ == "__main__":
    # Test staging for 2024
    stage_seasons([2024])
