"""
===============================================================================
TRINO LAKEHOUSE QUERY CLIENT & SERVING ENGINE (WEEK 3)
Connects to Trino Distributed SQL Query Engine to query Apache Iceberg
tables stored in MinIO S3 and cataloged in Project Nessie.
===============================================================================
"""

import time
import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.config import (
    TRINO_HOST,
    TRINO_PORT,
    TRINO_USER,
    TRINO_CATALOG,
    TRINO_SCHEMA,
    TRINO_HTTP_SCHEME,
    MINIO_WAREHOUSE_BUCKET
)

try:
    import trino
    from trino.dbapi import connect as trino_connect
    TRINO_LIB_AVAILABLE = True
except ImportError:
    TRINO_LIB_AVAILABLE = False


def is_trino_reachable(timeout: float = 0.2) -> bool:
    """Checks if Trino host port is listening with minimal latency."""
    import socket
    try:
        s = socket.create_connection((TRINO_HOST, TRINO_PORT), timeout=timeout)
        s.close()
        return True
    except Exception:
        return False


def get_trino_connection(timeout_seconds: int = 5):
    """
    Establishes and returns an active Trino DBAPI connection.
    """
    if not TRINO_LIB_AVAILABLE:
        raise ImportError("Python 'trino' library is not installed.")
        
    return trino_connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme=TRINO_HTTP_SCHEME,
        request_timeout=timeout_seconds
    )


def check_trino_connection() -> Dict[str, Any]:
    """
    Performs a live connectivity test against Trino and verifies
    the Iceberg catalog presence.
    """
    if not TRINO_LIB_AVAILABLE:
        return {
            "status": "UNAVAILABLE",
            "connected": False,
            "engine": "trino_distributed_sql",
            "host": f"{TRINO_HOST}:{TRINO_PORT}",
            "catalog": TRINO_CATALOG,
            "latency_ms": None,
            "error": "Python 'trino' package is not installed."
        }

    if not is_trino_reachable(timeout=0.2):
        return {
            "status": "OFFLINE",
            "connected": False,
            "engine": "trino_distributed_sql",
            "host": f"{TRINO_HOST}:{TRINO_PORT}",
            "catalog": TRINO_CATALOG,
            "schema": TRINO_SCHEMA,
            "latency_ms": 0.0,
            "error": f"Trino daemon is not reachable at {TRINO_HOST}:{TRINO_PORT}"
        }

    start_time = time.time()
    try:
        conn = get_trino_connection(timeout_seconds=3)
        cur = conn.cursor()
        cur.execute("SELECT 1 AS live, version() AS version")
        row = cur.fetchone()
        cur.close()
        conn.close()
        latency = round((time.time() - start_time) * 1000, 2)
        version = row[1] if row and len(row) > 1 else "Unknown"

        return {
            "status": "ONLINE",
            "connected": True,
            "engine": "trino_distributed_sql",
            "version": version,
            "host": f"{TRINO_HOST}:{TRINO_PORT}",
            "catalog": TRINO_CATALOG,
            "schema": TRINO_SCHEMA,
            "latency_ms": latency,
            "error": None
        }
    except Exception as e:
        latency = round((time.time() - start_time) * 1000, 2)
        return {
            "status": "OFFLINE",
            "connected": False,
            "engine": "trino_distributed_sql",
            "host": f"{TRINO_HOST}:{TRINO_PORT}",
            "catalog": TRINO_CATALOG,
            "schema": TRINO_SCHEMA,
            "latency_ms": latency,
            "error": str(e)
        }

# Alias for compatibility
test_trino_connection = check_trino_connection


def execute_trino_query(sql: str, params: Optional[Tuple] = None, timeout: int = 10) -> Dict[str, Any]:
    """
    Executes an analytical SQL statement against the Trino Iceberg engine.
    Returns column names, result rows as dicts, row count, and actual latency.
    """
    if not is_trino_reachable(timeout=0.2):
        return {
            "success": False,
            "source": "trino_iceberg",
            "columns": [],
            "records": [],
            "row_count": 0,
            "latency_ms": 0.0,
            "error": f"Trino service is not reachable at {TRINO_HOST}:{TRINO_PORT}."
        }

    start_time = time.time()
    try:
        conn = get_trino_connection(timeout_seconds=timeout)
        cur = conn.cursor()
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        description = cur.description or []
        columns = [col[0] for col in description]
        raw_rows = cur.fetchall()
        cur.close()
        conn.close()

        latency_ms = round((time.time() - start_time) * 1000, 2)
        records = [dict(zip(columns, row)) for row in raw_rows]

        return {
            "success": True,
            "source": "trino_iceberg",
            "columns": columns,
            "records": records,
            "row_count": len(records),
            "latency_ms": latency_ms,
            "error": None
        }
    except Exception as e:
        latency_ms = round((time.time() - start_time) * 1000, 2)
        return {
            "success": False,
            "source": "trino_iceberg",
            "columns": [],
            "records": [],
            "row_count": 0,
            "latency_ms": latency_ms,
            "error": str(e)
        }



# =============================================================================
# LAKEHOUSE DIRECT PARQUET LOCAL FALLBACK (When Trino container is offline)
# Transparently reads verified Iceberg warehouse parquet files directly
# =============================================================================

def read_lakehouse_storage_fallback(season: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fallback reader that queries local Lakehouse warehouse Parquet files
    when Trino distributed cluster is not running.
    Transparently labels source as 'direct_lakehouse_storage_fallback'.
    """
    import pandas as pd
    warehouse_dir = PROJECT_ROOT / "data" / "warehouse" / "ipl" / "ipl_matches"
    
    if not warehouse_dir.exists():
        return []

    parquet_files = []
    if season and str(season).lower() != "all":
        season_dir = warehouse_dir / f"season={season}"
        if season_dir.exists():
            parquet_files = list(season_dir.glob("*.parquet"))
    else:
        parquet_files = list(warehouse_dir.glob("season=*/*.parquet"))

    if not parquet_files:
        return []

    dfs = []
    for pf in parquet_files:
        try:
            df = pd.read_parquet(pf)
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return []

    merged = pd.concat(dfs, ignore_index=True)
    return merged.to_dict(orient="records")


# =============================================================================
# HIGH-LEVEL ANALYTICAL DASHBOARD QUERIES
# =============================================================================

def get_lakehouse_overview(season: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns high-level KPI card metrics:
    total_matches, total_runs, total_wickets, total_overs, distinct seasons.
    """
    season_filter = ""
    params = None
    if season and str(season).lower() != "all":
        season_filter = "WHERE season = ?"
        params = (str(season),)

    sql = f"""
        SELECT 
            COUNT(*) AS total_matches,
            COALESCE(SUM(total_runs), 0) AS total_runs,
            COALESCE(SUM(total_wickets), 0) AS total_wickets,
            COALESCE(SUM(total_overs), 0) AS total_overs,
            COUNT(DISTINCT season) AS season_count
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.ipl_matches
        {season_filter}
    """

    res = execute_trino_query(sql, params)
    
    if res["success"] and res["records"]:
        card = res["records"][0]
        # Query distinct seasons for dropdown filter
        seasons_res = execute_trino_query(
            f"SELECT DISTINCT season FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.ipl_matches ORDER BY season DESC"
        )
        available_seasons = [r["season"] for r in seasons_res.get("records", [])]
        
        return {
            "success": True,
            "engine": "trino_distributed_sql",
            "latency_ms": res["latency_ms"],
            "data": {
                "total_matches": int(card.get("total_matches", 0)),
                "total_runs": int(card.get("total_runs", 0)),
                "total_wickets": int(card.get("total_wickets", 0)),
                "total_overs": int(card.get("total_overs", 0)),
                "season_count": int(card.get("season_count", 0)),
                "seasons": available_seasons,
                "selected_season": season or "all"
            },
            "error": None
        }

    # Transparent local Lakehouse storage fallback
    fallback_records = read_lakehouse_storage_fallback(season)
    if fallback_records:
        import pandas as pd
        df = pd.DataFrame(fallback_records)
        all_seasons = sorted(df["season"].dropna().unique().tolist(), reverse=True) if "season" in df else []
        return {
            "success": True,
            "engine": "direct_lakehouse_storage_fallback",
            "latency_ms": res.get("latency_ms", 0),
            "data": {
                "total_matches": len(df),
                "total_runs": int(df["total_runs"].sum()) if "total_runs" in df else 0,
                "total_wickets": int(df["total_wickets"].sum()) if "total_wickets" in df else 0,
                "total_overs": int(df["total_overs"].sum()) if "total_overs" in df else 0,
                "season_count": len(all_seasons),
                "seasons": all_seasons,
                "selected_season": season or "all"
            },
            "error": f"Trino offline ({res.get('error')}). Served from Lakehouse S3 Parquet files directly."
        }

    return {
        "success": False,
        "engine": "none",
        "data": {},
        "error": res.get("error", "No Lakehouse data available.")
    }


def get_lakehouse_season_trends() -> Dict[str, Any]:
    """
    Returns season-by-season match and scoring metrics across the Lakehouse.
    """
    sql = f"""
        SELECT 
            season,
            COUNT(*) AS match_count,
            COALESCE(SUM(total_runs), 0) AS total_runs,
            COALESCE(SUM(total_wickets), 0) AS total_wickets
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.ipl_matches
        GROUP BY season
        ORDER BY season ASC
    """
    res = execute_trino_query(sql)
    if res["success"]:
        return {
            "success": True,
            "engine": "trino_distributed_sql",
            "latency_ms": res["latency_ms"],
            "records": res["records"]
        }

    # Fallback
    fallback_records = read_lakehouse_storage_fallback()
    if fallback_records:
        import pandas as pd
        df = pd.DataFrame(fallback_records)
        if "season" in df:
            agg = df.groupby("season").agg(
                match_count=("match_id", "count"),
                total_runs=("total_runs", "sum"),
                total_wickets=("total_wickets", "sum")
            ).reset_index().sort_values("season")
            return {
                "success": True,
                "engine": "direct_lakehouse_storage_fallback",
                "latency_ms": res.get("latency_ms", 0),
                "records": agg.to_dict(orient="records"),
                "error": "Served from Lakehouse Parquet fallback."
            }

    return {"success": False, "records": [], "error": res.get("error")}


def get_lakehouse_wins_by_team(season: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns team victory distribution from the Lakehouse.
    """
    season_filter = "WHERE winner IS NOT NULL AND winner != ''"
    params = None
    if season and str(season).lower() != "all":
        season_filter += " AND season = ?"
        params = (str(season),)

    sql = f"""
        SELECT 
            winner AS team,
            COUNT(*) AS wins
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.ipl_matches
        {season_filter}
        GROUP BY winner
        ORDER BY wins DESC
        LIMIT 10
    """
    res = execute_trino_query(sql, params)
    if res["success"]:
        return {
            "success": True,
            "engine": "trino_distributed_sql",
            "latency_ms": res["latency_ms"],
            "records": res["records"]
        }

    # Fallback
    fallback_records = read_lakehouse_storage_fallback(season)
    if fallback_records:
        import pandas as pd
        df = pd.DataFrame(fallback_records)
        if "winner" in df:
            wins = df[df["winner"].notna() & (df["winner"] != "")]["winner"].value_counts().head(10).reset_index()
            wins.columns = ["team", "wins"]
            return {
                "success": True,
                "engine": "direct_lakehouse_storage_fallback",
                "latency_ms": res.get("latency_ms", 0),
                "records": wins.to_dict(orient="records"),
                "error": "Served from Lakehouse Parquet fallback."
            }

    return {"success": False, "records": [], "error": res.get("error")}


def get_lakehouse_venue_stats(season: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns top venues by matches hosted from the Lakehouse.
    """
    season_filter = "WHERE venue IS NOT NULL AND venue != ''"
    params = None
    if season and str(season).lower() != "all":
        season_filter += " AND season = ?"
        params = (str(season),)

    sql = f"""
        SELECT 
            venue,
            COUNT(*) AS matches_hosted,
            COALESCE(AVG(total_runs), 0) AS avg_match_runs
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.ipl_matches
        {season_filter}
        GROUP BY venue
        ORDER BY matches_hosted DESC
        LIMIT 10
    """
    res = execute_trino_query(sql, params)
    if res["success"]:
        return {
            "success": True,
            "engine": "trino_distributed_sql",
            "latency_ms": res["latency_ms"],
            "records": res["records"]
        }

    # Fallback
    fallback_records = read_lakehouse_storage_fallback(season)
    if fallback_records:
        import pandas as pd
        df = pd.DataFrame(fallback_records)
        if "venue" in df:
            venues = df.groupby("venue").agg(
                matches_hosted=("match_id", "count"),
                avg_match_runs=("total_runs", "mean")
            ).reset_index().sort_values("matches_hosted", ascending=False).head(10)
            venues["avg_match_runs"] = venues["avg_match_runs"].round(1)
            return {
                "success": True,
                "engine": "direct_lakehouse_storage_fallback",
                "latency_ms": res.get("latency_ms", 0),
                "records": venues.to_dict(orient="records"),
                "error": "Served from Lakehouse Parquet fallback."
            }

    return {"success": False, "records": [], "error": res.get("error")}
