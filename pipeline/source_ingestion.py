import os
import hashlib
import requests
from pathlib import Path
from datetime import datetime
from config.config import BRONZE_PATH, DATABASE_URL
from database.connection import get_db_cursor

SOURCE_URL = "https://cricsheet.org/downloads/ipl_json.zip"
ZIP_PATH = BRONZE_PATH / "source" / "ipl_json.zip"

def calculate_sha256(file_path):
    """Calculates SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256.update(byte_block)
    return sha256.hexdigest()

def check_ingested():
    """Checks database source_registry and local filesystem to verify if ZIP is available and valid.
    If a valid ZIP exists locally but is missing or mismatched in the DB, registers it automatically.
    """
    import zipfile
    if not ZIP_PATH.exists() or not zipfile.is_zipfile(ZIP_PATH):
        # Delete corrupted/partial file if it exists
        if ZIP_PATH.exists():
            try:
                ZIP_PATH.unlink()
            except:
                pass
        return False
        
    try:
        # Calculate stats for the valid local file
        local_hash = calculate_sha256(ZIP_PATH)
        local_size = ZIP_PATH.stat().st_size
        
        with get_db_cursor(commit=True) as cur:
            cur.execute(
                "SELECT file_hash FROM source_registry WHERE source_name = %s LIMIT 1",
                ('cricsheet_ipl_json',)
            )
            res = cur.fetchone()
            if not res or res[0] != local_hash:
                # Update DB registry to match the valid local file
                cur.execute("DELETE FROM source_registry WHERE source_name = %s", ('cricsheet_ipl_json',))
                cur.execute(
                    """
                    INSERT INTO source_registry (source_name, source_url, local_path, file_hash, file_size, downloaded_at, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    ('cricsheet_ipl_json', SOURCE_URL, str(ZIP_PATH), local_hash, local_size, datetime.now(), 'AVAILABLE')
                )
        return True
    except Exception as e:
        print(f"Database sync check failed: {e}")
        # Fallback to True since file exists locally and is a valid zip
        return True

def ingest_source():
    """Downloads the Cricsheet ZIP if it does not already exist, computes hash and registers metadata."""
    if check_ingested():
        msg = "IPL source already exists. Download skipped."
        print(msg)
        return {"status": "SUCCESS", "message": msg, "path": str(ZIP_PATH)}

    print(f"Downloading IPL source dataset from {SOURCE_URL}...")
    ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Download file
        response = requests.get(SOURCE_URL, stream=True)
        response.raise_for_status()
        
        with open(ZIP_PATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        # Calculate stats
        file_size = ZIP_PATH.stat().st_size
        file_hash = calculate_sha256(ZIP_PATH)
        downloaded_at = datetime.now()
        
        # Register in DB
        with get_db_cursor(commit=True) as cur:
            # First delete existing record to prevent duplicates
            cur.execute("DELETE FROM source_registry WHERE source_name = %s", ('cricsheet_ipl_json',))
            cur.execute(
                """
                INSERT INTO source_registry (source_name, source_url, local_path, file_hash, file_size, downloaded_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                ('cricsheet_ipl_json', SOURCE_URL, str(ZIP_PATH), file_hash, file_size, downloaded_at, 'AVAILABLE')
            )
            
        msg = "IPL source downloaded and registered successfully."
        print(msg)
        return {"status": "SUCCESS", "message": msg, "path": str(ZIP_PATH)}
        
    except Exception as e:
        # Clean up partial download
        if ZIP_PATH.exists():
            try:
                ZIP_PATH.unlink()
            except:
                pass
        err_msg = f"Failed to ingest source: {str(e)}"
        print(err_msg)
        return {"status": "FAILED", "message": err_msg}

if __name__ == "__main__":
    ingest_source()
