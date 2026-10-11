"""
===============================================================================
UNIT & INTEGRATION TESTS: WEEK 3 TRINO LAKEHOUSE SERVING
Tests Trino configuration, query execution structures, aggregation queries,
and transparent fallback behavior.
===============================================================================
"""

import pytest
import os
import sys
from pathlib import Path

# Add project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.config import (
    TRINO_HOST,
    TRINO_PORT,
    TRINO_CATALOG,
    TRINO_SCHEMA,
    ICEBERG_TABLE_NAME
)
from pipeline.trino_client import (
    check_trino_connection,
    execute_trino_query,
    get_lakehouse_overview,
    get_lakehouse_season_trends,
    get_lakehouse_wins_by_team,
    get_lakehouse_venue_stats,
    read_lakehouse_storage_fallback
)


class TestTrinoLakehouseIntegration:
    """Verification suite for Trino Iceberg Serving Layer."""

    def test_trino_configuration_keys(self):
        """Verifies that all required Trino configuration settings are populated."""
        assert TRINO_HOST is not None
        assert isinstance(TRINO_PORT, int)
        assert TRINO_PORT == 8085 or TRINO_PORT == 8080
        assert TRINO_CATALOG == "iceberg" or TRINO_CATALOG == "ipl"
        assert TRINO_SCHEMA == "ipl"
        assert ICEBERG_TABLE_NAME == "ipl.ipl_matches"

    def test_trino_connection_tester_structure(self):
        """Verifies check_trino_connection() returns a well-formed status contract."""
        res = check_trino_connection()
        assert isinstance(res, dict)
        assert "status" in res
        assert "connected" in res
        assert "engine" in res
        assert "host" in res
        assert "catalog" in res
        assert res["engine"] == "trino_distributed_sql"
        assert res["status"] in ("ONLINE", "OFFLINE", "UNAVAILABLE")

    def test_lakehouse_overview_structure(self):
        """Tests that get_lakehouse_overview returns structured metrics."""
        res = get_lakehouse_overview(season="all")
        assert isinstance(res, dict)
        assert "success" in res
        assert "engine" in res
        if res["success"]:
            d = res["data"]
            assert "total_matches" in d
            assert "total_runs" in d
            assert "total_wickets" in d
            assert "total_overs" in d
            assert isinstance(d["total_matches"], int)
            assert d["total_matches"] >= 0

    def test_lakehouse_season_trends_aggregation(self):
        """Verifies season progression aggregation query format."""
        res = get_lakehouse_season_trends()
        assert isinstance(res, dict)
        assert "success" in res
        assert "records" in res
        if res["success"] and res["records"]:
            first = res["records"][0]
            assert "season" in first
            assert "match_count" in first
            assert "total_runs" in first

    def test_lakehouse_team_standings_aggregation(self):
        """Verifies team victory standings format."""
        res = get_lakehouse_wins_by_team(season="all")
        assert isinstance(res, dict)
        assert "success" in res
        assert "records" in res
        if res["success"] and res["records"]:
            first = res["records"][0]
            assert "team" in first
            assert "wins" in first
            assert isinstance(first["wins"], (int, float))

    def test_lakehouse_venue_stats_aggregation(self):
        """Verifies top venue metrics structure."""
        res = get_lakehouse_venue_stats(season="all")
        assert isinstance(res, dict)
        assert "success" in res
        assert "records" in res
        if res["success"] and res["records"]:
            first = res["records"][0]
            assert "venue" in first
            assert "matches_hosted" in first

    def test_direct_lakehouse_storage_fallback_reader(self):
        """Verifies that read_lakehouse_storage_fallback reads verified Parquet data."""
        records = read_lakehouse_storage_fallback()
        assert isinstance(records, list)
        if records:
            first = records[0]
            assert "match_id" in first
            assert "season" in first
            assert "total_runs" in first
            assert "total_wickets" in first

    def test_trino_query_safety_guard(self):
        """Tests custom query executor handles errors safely without unhandled crashes."""
        # Non-existent table query should fail gracefully with error string
        res = execute_trino_query("SELECT * FROM non_existent_catalog.dummy.table_123")
        assert isinstance(res, dict)
        assert res["success"] is False
        assert res["records"] == []
        assert res["error"] is not None
