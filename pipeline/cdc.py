import os
import hashlib
import json
from database.connection import get_db_cursor

def compute_file_hash(filepath):
    """Computes SHA-256 hash of a file's content to detect changes."""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()

def detect_changes_for_season(season, staged_files, run_id):
    """
    Scans staged JSON files for a season, compares them against cdc_states,
    records INSERT, UPDATE, SKIP, and DELETE operations, and logs them in cdc_log.
    
    staged_files: List of file paths to stage-validated JSON matches.
    Returns: A dict mapping match_id (str) -> { 'operation': str, 'new_hash': str, 'filepath': str }
    """
    cdc_results = {}
    staged_match_ids = set()
    
    # 1. Read existing hashes from cdc_states
    existing_states = {}
    with get_db_cursor() as cur:
        cur.execute("SELECT business_key, record_hash FROM cdc_states WHERE entity_name = 'match'")
        for row in cur.fetchall():
            existing_states[row[0]] = row[1]
            
    # 2. Process staged files (INSERT / UPDATE / SKIP)
    with get_db_cursor(commit=True) as cur:
        for filepath in staged_files:
            filename = os.path.basename(filepath)
            match_id = os.path.splitext(filename)[0]
            staged_match_ids.add(match_id)
            
            new_hash = compute_file_hash(filepath)
            old_hash = existing_states.get(match_id)
            
            if old_hash is None:
                operation = 'INSERT'
            elif old_hash != new_hash:
                operation = 'UPDATE'
            else:
                operation = 'SKIP'
                
            cdc_results[match_id] = {
                'operation': operation,
                'new_hash': new_hash,
                'filepath': filepath
            }
            
            # Log to cdc_log
            cur.execute(
                """
                INSERT INTO cdc_log (run_id, entity_name, business_key, operation, old_hash, new_hash)
                VALUES (%s, 'match', %s, %s, %s, %s)
                """,
                (run_id, match_id, operation, old_hash, new_hash)
            )
            
            # Upsert state in cdc_states for INSERT or UPDATE
            if operation in ('INSERT', 'UPDATE'):
                cur.execute(
                    """
                    INSERT INTO cdc_states (entity_name, business_key, record_hash, detected_at)
                    VALUES ('match', %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (entity_name, business_key)
                    DO UPDATE SET record_hash = EXCLUDED.record_hash, detected_at = CURRENT_TIMESTAMP
                    """,
                    (match_id, new_hash)
                )

        # 3. Detect DELETES
        # Find matches of this season currently registered in the database match table
        cur.execute("SELECT match_id FROM match WHERE season = %s", (str(season),))
        db_match_ids = [str(row[0]) for row in cur.fetchall()]
        
        for db_match_id in db_match_ids:
            if db_match_id not in staged_match_ids:
                # This match was previously loaded but is now missing from staging files (DELETE case)
                old_hash = existing_states.get(db_match_id)
                cur.execute(
                    """
                    INSERT INTO cdc_log (run_id, entity_name, business_key, operation, old_hash, new_hash)
                    VALUES (%s, 'match', %s, 'DELETE', %s, NULL)
                    """,
                    (run_id, db_match_id, old_hash)
                )
                # Remove from cdc_states
                cur.execute(
                    "DELETE FROM cdc_states WHERE entity_name = 'match' AND business_key = %s",
                    (db_match_id,)
                )
                cdc_results[db_match_id] = {
                    'operation': 'DELETE',
                    'new_hash': None,
                    'filepath': None
                }

    return cdc_results
