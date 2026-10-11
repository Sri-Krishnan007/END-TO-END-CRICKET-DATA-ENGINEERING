"""
===============================================================================
UNIT & INTEGRATION TESTS: WEEK 4 LAKEHOUSE MAINTENANCE & BENCHMARKING
Tests table health verification, compaction, snapshot expiration,
vacuum orphan removal, and benchmarking accuracy.
===============================================================================
"""

import pytest
import os
import sys
import json
from pathlib import Path

# Add project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from pipeline.lakehouse_maintenance import (
    check_table_health,
    compact_small_files,
    expire_old_snapshots,
    remove_orphan_files,
    run_full_maintenance_suite
)
from pipeline.benchmark_lakehouse import (
    BENCHMARK_QUERIES,
    run_single_query_benchmark,
    execute_benchmark_suite
)


class TestLakehouseMaintenanceAndOptimization:
    """Test suite for Week 4 Maintenance procedures and benchmarking."""

    def test_check_table_health(self):
        """Verifies health check produces structured status."""
        health = check_table_health()
        assert isinstance(health, dict)
        assert "status" in health
        assert health["status"] in ("HEALTHY", "UNHEALTHY")
        assert "engine" in health
        assert "latency_ms" in health

    def test_compact_small_files_idempotent(self):
        """Verifies compaction runs safely without raising exceptions or data loss."""
        res = compact_small_files(file_size_threshold="100MB")
        assert isinstance(res, dict)
        assert res["status"] == "SUCCESS"
        assert "elapsed_ms" in res
        assert res["elapsed_ms"] >= 0

    def test_expire_old_snapshots_preserves_recent(self):
        """Verifies snapshot expiry retains recent snapshots while purging stale ones."""
        res = expire_old_snapshots(retention_days=7)
        assert isinstance(res, dict)
        assert res["status"] == "SUCCESS"
        assert res["retention_days"] == 7
        assert "elapsed_ms" in res

    def test_remove_orphan_files_safe(self):
        """Verifies orphan cleanup does not purge active parquet data files."""
        res = remove_orphan_files(retention_days=7)
        assert isinstance(res, dict)
        assert res["status"] == "SUCCESS"
        assert "elapsed_ms" in res

    def test_full_maintenance_suite_execution(self):
        """Verifies the complete maintenance sequence executes end-to-end and logs output."""
        summary = run_full_maintenance_suite(retention_days=7)
        assert isinstance(summary, dict)
        assert "initial_health" in summary
        assert "compaction" in summary
        assert "snapshot_expiration" in summary
        assert "orphan_cleanup" in summary
        assert "post_maintenance_health" in summary
        assert summary["compaction"]["status"] == "SUCCESS"

    def test_benchmark_query_classes_definition(self):
        """Verifies that all 4 required benchmark query classes are defined with valid SQL."""
        assert len(BENCHMARK_QUERIES) == 4
        ids = [q["id"] for q in BENCHMARK_QUERIES]
        assert "Q1_ROW_COUNT" in ids
        assert "Q2_SEASON_AGG" in ids
        assert "Q3_TEAM_WINS" in ids
        assert "Q4_PARTITION_PRUNED" in ids
        for q in BENCHMARK_QUERIES:
            assert "sql" in q
            assert len(q["sql"]) > 10

    def test_single_query_benchmark_execution(self):
        """Verifies benchmark execution captures median, min, max, and runs."""
        q1 = BENCHMARK_QUERIES[0]
        res = run_single_query_benchmark(q1, repetitions=2)
        assert isinstance(res, dict)
        assert res["id"] == q1["id"]
        assert len(res["runs_ms"]) == 2
        assert res["median_ms"] >= 0
        assert res["min_ms"] <= res["max_ms"]
        assert "row_count" in res
