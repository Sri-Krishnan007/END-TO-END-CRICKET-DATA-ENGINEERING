import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from config.config import DATABASE_URL

def get_connection():
    """Establishes a raw connection to the PostgreSQL database."""
    conn = psycopg2.connect(DATABASE_URL)
    return conn

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
