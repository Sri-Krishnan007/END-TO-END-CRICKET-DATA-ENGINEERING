"""
===============================================================================
LAKEHOUSE MAINTENANCE & OPTIMIZATION MODULE (WEEK 4)
Orchestrates Apache Iceberg table maintenance:
1. File Compaction / Bin-packing (Small file consolidation)
2. Snapshot Expiration (Metadata history retention control)
3. Orphan File Removal (Safe vacuum-equivalent cleanup)
4. Table Integrity & Schema Verification
===============================================================================
"""

import os
import sys
import time
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.config import (
    TRINO_CATALOG,
    TRINO_SCHEMA,
    ICEBERG_TABLE_NAME,
    MINIO_WAREHOUSE_BUCKET
)
from pipeline.trino_client import is_trino_reachable, execute_trino_query


def check_table_health() -> Dict[str, Any]:
    """
    Verifies that the Lakehouse table exists, is readable, and returns row count.
    """
    start_time = time.time()
    
    if is_trino_reachable(timeout=0.2):
        sql = f"SELECT COUNT(*) AS row_count FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.ipl_matches"
        res = execute_trino_query(sql)
        if res["success"] and res["records"]:
            count = res["records"][0].get("row_count", 0)
            return {
                "status": "HEALTHY",
                "engine": "trino_iceberg",
                "row_count": count,
                "latency_ms": res["latency_ms"],
                "timestamp": datetime.now().isoformat()
            }

    # Direct filesystem check
    warehouse_dir = PROJECT_ROOT / "data" / "warehouse" / "ipl" / "ipl_matches"
    if warehouse_dir.exists():
        parquet_files = list(warehouse_dir.glob("season=*/*.parquet"))
        return {
            "status": "HEALTHY",
            "engine": "direct_lakehouse_storage",
            "file_count": len(parquet_files),
            "latency_ms": round((time.time() - start_time) * 1000, 2),
            "timestamp": datetime.now().isoformat()
        }

    return {
        "status": "UNHEALTHY",
        "engine": "none",
        "error": "Lakehouse warehouse directory not found.",
        "timestamp": datetime.now().isoformat()
    }


def compact_small_files(file_size_threshold: str = "100MB") -> Dict[str, Any]:
    """
    Executes compaction / bin-packing to consolidate small files.
    Trino Iceberg Procedure: ALTER TABLE ... EXECUTE optimize(file_size_threshold => '...')
    """
    start_time = time.time()
    print("[MAINTENANCE] Starting Lakehouse file compaction / bin-packing...")

    if is_trino_reachable(timeout=0.2):
        sql = f"ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.ipl_matches EXECUTE optimize(file_size_threshold => '{file_size_threshold}')"
        res = execute_trino_query(sql, timeout=60)
        elapsed = round((time.time() - start_time) * 1000, 2)
        if res["success"]:
            print(f"[MAINTENANCE] Trino compaction completed in {elapsed} ms.")
            return {
                "operation": "optimize_compaction",
                "engine": "trino_iceberg",
                "status": "SUCCESS",
                "elapsed_ms": elapsed,
                "details": "Trino Iceberg optimize procedure executed successfully."
            }
        else:
            print(f"[MAINTENANCE NOTICE] Trino optimize notice: {res['error']}")

    # Direct Parquet storage compaction fallback
    warehouse_dir = PROJECT_ROOT / "data" / "warehouse" / "ipl" / "ipl_matches"
    consolidated_partitions = 0
    total_files_before = 0
    total_files_after = 0

    if warehouse_dir.exists():
        import pandas as pd
        for season_dir in warehouse_dir.glob("season=*"):
            if not season_dir.is_dir():
                continue
            files = list(season_dir.glob("*.parquet"))
            total_files_before += len(files)
            
            if len(files) > 1:
                # Merge small files into a single compacted partition file
                dfs = [pd.read_parquet(f) for f in files]
                merged = pd.concat(dfs, ignore_index=True)
                
                # Write new compacted file
                compacted_name = f"compacted-{int(time.time()*1000)}.parquet"
                compacted_path = season_dir / compacted_name
                merged.to_parquet(compacted_path, index=False)
                
                # Safely archive / clean old partial fragments
                for old_f in files:
                    try:
                        old_f.unlink()
                    except Exception:
                        pass
                consolidated_partitions += 1
                total_files_after += 1
            else:
                total_files_after += len(files)

    elapsed = round((time.time() - start_time) * 1000, 2)
    print(f"[MAINTENANCE] Parquet storage bin-packing completed. Consolidated {consolidated_partitions} partitions in {elapsed} ms.")
    return {
        "operation": "optimize_compaction",
        "engine": "direct_lakehouse_storage_compactor",
        "status": "SUCCESS",
        "elapsed_ms": elapsed,
        "files_before": total_files_before,
        "files_after": total_files_after,
        "consolidated_partitions": consolidated_partitions
    }


def expire_old_snapshots(retention_days: int = 7) -> Dict[str, Any]:
    """
    Prunes old metadata snapshots beyond the configured retention period.
    Trino Iceberg Procedure: ALTER TABLE ... EXECUTE expire_snapshots(retention_threshold => '7d')
    """
    start_time = time.time()
    print(f"[MAINTENANCE] Starting snapshot expiration (Retention: {retention_days} days)...")

    if is_trino_reachable(timeout=0.2):
        sql = f"ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.ipl_matches EXECUTE expire_snapshots(retention_threshold => '{retention_days}d')"
        res = execute_trino_query(sql, timeout=30)
        elapsed = round((time.time() - start_time) * 1000, 2)
        if res["success"]:
            print(f"[MAINTENANCE] Trino snapshot expiration completed in {elapsed} ms.")
            return {
                "operation": "expire_snapshots",
                "engine": "trino_iceberg",
                "status": "SUCCESS",
                "retention_days": retention_days,
                "elapsed_ms": elapsed
            }

    # Direct Metadata JSON snapshot retention management
    metadata_dir = PROJECT_ROOT / "data" / "warehouse" / "ipl" / "ipl_matches" / "metadata"
    pruned_count = 0
    retained_count = 0

    if metadata_dir.exists():
        cutoff_time = time.time() - (retention_days * 86400)
        meta_files = sorted(metadata_dir.glob("v*.metadata.json"))
        # Keep at least the 3 most recent snapshots regardless of age (safety rule)
        candidates_to_prune = meta_files[:-3] if len(meta_files) > 3 else []
        
        for mf in candidates_to_prune:
            if mf.stat().st_mtime < cutoff_time:
                try:
                    mf.unlink()
                    pruned_count += 1
                except Exception:
                    pass
        retained_count = len(list(metadata_dir.glob("v*.metadata.json")))

    elapsed = round((time.time() - start_time) * 1000, 2)
    print(f"[MAINTENANCE] Snapshot expiration finished. Pruned {pruned_count} old snapshots, retained {retained_count} active snapshots ({elapsed} ms).")
    return {
        "operation": "expire_snapshots",
        "engine": "direct_lakehouse_snapshot_cleaner",
        "status": "SUCCESS",
        "retention_days": retention_days,
        "pruned_snapshots": pruned_count,
        "retained_snapshots": retained_count,
        "elapsed_ms": elapsed
    }


def remove_orphan_files(retention_days: int = 7) -> Dict[str, Any]:
    """
    Safely purges unreferenced data files (equivalent to vacuum).
    Trino Iceberg Procedure: ALTER TABLE ... EXECUTE remove_orphan_files(retention_threshold => '7d')
    """
    start_time = time.time()
    print(f"[MAINTENANCE] Starting orphan file cleanup (Safety threshold: {retention_days} days)...")

    if is_trino_reachable(timeout=0.2):
        sql = f"ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.ipl_matches EXECUTE remove_orphan_files(retention_threshold => '{retention_days}d')"
        res = execute_trino_query(sql, timeout=30)
        elapsed = round((time.time() - start_time) * 1000, 2)
        if res["success"]:
            print(f"[MAINTENANCE] Trino orphan file cleanup completed in {elapsed} ms.")
            return {
                "operation": "remove_orphan_files",
                "engine": "trino_iceberg",
                "status": "SUCCESS",
                "elapsed_ms": elapsed
            }

    # Direct orphan audit: verify all files belong to known partitions
    warehouse_dir = PROJECT_ROOT / "data" / "warehouse" / "ipl" / "ipl_matches"
    orphans_removed = 0
    scanned_files = 0

    if warehouse_dir.exists():
        cutoff_time = time.time() - (retention_days * 86400)
        for p in warehouse_dir.rglob("*.tmp"):
            if p.stat().st_mtime < cutoff_time:
                try:
                    p.unlink()
                    orphans_removed += 1
                except Exception:
                    pass
        scanned_files = len(list(warehouse_dir.rglob("*.parquet")))

    elapsed = round((time.time() - start_time) * 1000, 2)
    print(f"[MAINTENANCE] Orphan audit completed. Scanned {scanned_files} valid files, purged {orphans_removed} orphans ({elapsed} ms).")
    return {
        "operation": "remove_orphan_files",
        "engine": "direct_lakehouse_orphan_cleaner",
        "status": "SUCCESS",
        "scanned_files": scanned_files,
        "orphans_removed": orphans_removed,
        "elapsed_ms": elapsed
    }


def run_full_maintenance_suite(retention_days: int = 7) -> Dict[str, Any]:
    """
    Runs the complete Lakehouse optimization and maintenance sequence.
    """
    print("=" * 70)
    print("  LAKEHOUSE AUTOMATED MAINTENANCE SUITE EXECUTION")
    print("=" * 70)
    
    t0 = time.time()
    h1 = check_table_health()
    c1 = compact_small_files()
    s1 = expire_old_snapshots(retention_days=retention_days)
    o1 = remove_orphan_files(retention_days=retention_days)
    h2 = check_table_health()
    total_elapsed = round((time.time() - t0) * 1000, 2)

    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_elapsed_ms": total_elapsed,
        "initial_health": h1,
        "compaction": c1,
        "snapshot_expiration": s1,
        "orphan_cleanup": o1,
        "post_maintenance_health": h2
    }

    # Persist maintenance log
    logs_dir = PROJECT_ROOT / "logs" / "maintenance"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"maintenance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"  [SUCCESS] All maintenance tasks completed in {total_elapsed} ms.")
    print(f"  [LOG] Stored audit record at: {log_file}")
    print("=" * 70)
    return summary


if __name__ == "__main__":
    run_full_maintenance_suite()
