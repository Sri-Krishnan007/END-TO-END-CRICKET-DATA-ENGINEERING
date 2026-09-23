import time
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from config.config import get_active_db_url, get_active_db_mode, LOCAL_DB_URL, ONLINE_DB_URL

def get_connection():
    """Establishes a raw connection to the currently active PostgreSQL database (Online/Supabase or Offline/Local)."""
    db_url = get_active_db_url()
    conn = psycopg2.connect(db_url)
    return conn

def test_db_connection(mode=None):
    """Tests connectivity to the target database and returns status dictionary."""
    if mode in ["offline", "local"]:
        url = LOCAL_DB_URL
        name = "Local PostgreSQL"
        target_mode = "offline"
    elif mode == "online":
        url = ONLINE_DB_URL
        name = "Supabase Cloud"
        target_mode = "online"
    else:
        target_mode = get_active_db_mode()
        url = get_active_db_url()
        name = "Supabase Cloud" if target_mode == "online" else "Local PostgreSQL"
        
    start_time = time.time()
    try:
        conn = psycopg2.connect(url, connect_timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        latency = round((time.time() - start_time) * 1000, 1)
        return {
            "success": True,
            "name": name,
            "mode": target_mode,
            "latency_ms": latency,
            "error": None
        }
    except Exception as e:
        return {
            "success": False,
            "name": name,
            "mode": target_mode,
            "latency_ms": None,
            "error": str(e)
        }

@contextmanager
def get_db_connection():
    """Context manager for managing PostgreSQL connection lifecycle."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_db_cursor(commit=True, cursor_factory=None):
    """Context manager for managing PostgreSQL cursors with optional transaction handling.
    If commit=True, it will commit the transaction on success or rollback on exception.
    """
    conn = get_connection()
    # If dict cursor is requested, use psycopg2's RealDictCursor
    if cursor_factory == 'dict':
        cur = conn.cursor(cursor_factory=RealDictCursor)
    else:
        cur = conn.cursor()
        
    try:
        yield cur
        if commit:
            conn.commit()
    except Exception as e:
        if commit:
            conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def init_db():
    """Initializes the database schema by executing database/schema.sql."""
    from pathlib import Path
    from config.config import PROJECT_ROOT
    
    schema_path = Path(PROJECT_ROOT) / "database" / "schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found at {schema_path}")
        
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
        
    with get_db_cursor(commit=True) as cur:
        cur.execute(schema_sql)
    print("Database schema successfully initialized in PostgreSQL/Supabase.")

