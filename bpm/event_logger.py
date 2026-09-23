import os
import json
import time
from datetime import datetime
from contextlib import contextmanager
from database.connection import get_db_cursor
from bpm.config import BPM_CONFIG

def log_bpm_event(case_id, activity, status, start_time=None, end_time=None, 
                  duration_seconds=None, records_processed=0, error_message=None, metadata=None):
    """
    Logs a discrete BPM lifecycle event into the PostgreSQL bpm_process_events table
    and appends to a local jsonl audit sink.
    Fails safely without raising exceptions to protect the core data pipeline.
    """
    try:
        now = datetime.now()
        start_ts = start_time if isinstance(start_time, datetime) else (now if start_time is None else datetime.fromtimestamp(start_time))
        end_ts = end_time if isinstance(end_time, datetime) else (now if status in ['SUCCESS', 'FAILED', 'COMPLETED', 'QUARANTINED'] and end_time is None else None)
        
        if duration_seconds is None and start_ts and end_ts:
            duration_seconds = round((end_ts - start_ts).total_seconds(), 3)
            
        event_payload = {
            "case_id": str(case_id),
            "activity": str(activity).lower(),
            "event_timestamp": start_ts.isoformat(),
            "end_timestamp": end_ts.isoformat() if end_ts else None,
            "duration_seconds": duration_seconds,
            "status": str(status).upper(),
            "records_processed": int(records_processed or 0),
            "error_message": str(error_message) if error_message else None,
            "metadata": metadata or {}
        }
        
        # 1. Database Logging
        if BPM_CONFIG.get("enable_db_logging", True):
            try:
                with get_db_cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO bpm_process_events (
                            case_id, activity, event_timestamp, end_timestamp, 
                            duration_seconds, status, records_processed, error_message, metadata
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            event_payload["case_id"],
                            event_payload["activity"],
                            start_ts,
                            end_ts,
                            duration_seconds,
                            event_payload["status"],
                            event_payload["records_processed"],
                            event_payload["error_message"],
                            json.dumps(event_payload["metadata"])
                        )
                    )
            except Exception as db_err:
                print(f"[BPM WARNING] Database logging failed: {db_err}")
                
        # 2. Filesystem JSONL Fallback Logging
        if BPM_CONFIG.get("enable_file_logging", True):
            try:
                log_file = BPM_CONFIG.get("log_file")
                if log_file:
                    os.makedirs(os.path.dirname(log_file), exist_ok=True)
                    with open(log_file, "a", encoding="utf-8") as f:
                        f.write(json.dumps(event_payload) + "\n")
            except Exception as file_err:
                print(f"[BPM WARNING] File sink logging failed: {file_err}")
                
        return event_payload

    except Exception as e:
        print(f"[BPM ERROR] Failed to log event: {e}")
        return None


@contextmanager
def track_bpm_stage(case_id, activity, records_processed=0, metadata=None):
    """
    Context manager to automatically record BPM event start, execution, duration,
    and terminal status (SUCCESS / FAILED).
    """
    start_time = datetime.now()
    log_bpm_event(case_id, activity, status="RUNNING", start_time=start_time, metadata=metadata)
    
    stage_state = {
        "records_processed": records_processed,
        "metadata": metadata or {}
    }
    
    try:
        yield stage_state
        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(), 3)
        log_bpm_event(
            case_id=case_id,
            activity=activity,
            status="SUCCESS",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            records_processed=stage_state.get("records_processed", records_processed),
            metadata=stage_state.get("metadata", metadata)
        )
    except Exception as exc:
        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(), 3)
        log_bpm_event(
            case_id=case_id,
            activity=activity,
            status="FAILED",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            records_processed=stage_state.get("records_processed", 0),
            error_message=str(exc),
            metadata=stage_state.get("metadata", metadata)
        )
        raise exc


def get_bpm_events(case_id=None, limit=200):
    """
    Retrieves process events from PostgreSQL (or falls back to JSONL file).
    """
    events = []
    try:
        with get_db_cursor(commit=False, cursor_factory='dict') as cur:
            if case_id:
                cur.execute(
                    """
                    SELECT event_id, case_id, activity, event_timestamp, end_timestamp, 
                           duration_seconds, status, records_processed, error_message, metadata
                    FROM bpm_process_events
                    WHERE case_id = %s
                    ORDER BY event_timestamp ASC
                    LIMIT %s
                    """,
                    (str(case_id), limit)
                )
            else:
                cur.execute(
                    """
                    SELECT event_id, case_id, activity, event_timestamp, end_timestamp, 
                           duration_seconds, status, records_processed, error_message, metadata
                    FROM bpm_process_events
                    ORDER BY event_timestamp DESC
                    LIMIT %s
                    """,
                    (limit,)
                )
            rows = cur.fetchall()
            for r in rows:
                row_dict = dict(r)
                if row_dict.get("event_timestamp"):
                    row_dict["event_timestamp"] = str(row_dict["event_timestamp"])
                if row_dict.get("end_timestamp"):
                    row_dict["end_timestamp"] = str(row_dict["end_timestamp"])
                if row_dict.get("duration_seconds") is not None:
                    row_dict["duration_seconds"] = float(row_dict["duration_seconds"])
                events.append(row_dict)
        return events
    except Exception as e:
        print(f"[BPM WARNING] Failed to fetch events from DB: {e}. Checking JSONL fallback.")
        
    # JSONL Fallback
    log_file = BPM_CONFIG.get("log_file")
    if log_file and os.path.exists(log_file):
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            for line in lines[-limit:]:
                data = json.loads(line.strip())
                if case_id is None or data.get("case_id") == str(case_id):
                    events.append(data)
        except Exception:
            pass
    return events
