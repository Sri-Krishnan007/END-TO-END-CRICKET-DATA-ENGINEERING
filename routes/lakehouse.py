"""
===============================================================================
LAKEHOUSE SERVING & TRINO DASHBOARD ROUTES (WEEK 3)
Serves analytical endpoints querying Apache Iceberg via Trino SQL.
===============================================================================
"""

from flask import Blueprint, render_template, jsonify, request
from pipeline.trino_client import (
    test_trino_connection,
    execute_trino_query,
    get_lakehouse_overview,
    get_lakehouse_season_trends,
    get_lakehouse_wins_by_team,
    get_lakehouse_venue_stats
)
from config.config import TRINO_HOST, TRINO_PORT, TRINO_CATALOG, TRINO_SCHEMA, ICEBERG_TABLE_NAME

lakehouse_bp = Blueprint('lakehouse', __name__)

@lakehouse_bp.route('/lakehouse')
def lakehouse_dashboard():
    """Renders the executive Lakehouse Trino Serving Dashboard."""
    conn_info = test_trino_connection()
    overview_res = get_lakehouse_overview(season="all")
    overview_data = overview_res.get("data", {})
    seasons = overview_data.get("seasons", [])
    
    return render_template(
        'lakehouse.html',
        trino_status=conn_info,
        overview=overview_data,
        seasons=seasons,
        trino_host=f"{TRINO_HOST}:{TRINO_PORT}",
        trino_catalog=TRINO_CATALOG,
        trino_schema=TRINO_SCHEMA,
        table_name=ICEBERG_TABLE_NAME
    )


@lakehouse_bp.route('/api/lakehouse/status')
def api_lakehouse_status():
    """Returns real-time Trino health, latency, and catalog connectivity."""
    status = test_trino_connection()
    return jsonify(status)


@lakehouse_bp.route('/api/lakehouse/overview')
def api_lakehouse_overview():
    """Returns KPI summary metrics from Trino for a given season."""
    season = request.args.get('season', 'all')
    res = get_lakehouse_overview(season=season)
    return jsonify(res)


@lakehouse_bp.route('/api/lakehouse/season-trends')
def api_lakehouse_season_trends():
    """Returns season-wise match counts and run aggregates from Trino."""
    res = get_lakehouse_season_trends()
    return jsonify(res)


@lakehouse_bp.route('/api/lakehouse/team-wins')
def api_lakehouse_team_wins():
    """Returns team victory distributions from Trino."""
    season = request.args.get('season', 'all')
    res = get_lakehouse_wins_by_team(season=season)
    return jsonify(res)


@lakehouse_bp.route('/api/lakehouse/venues')
def api_lakehouse_venues():
    """Returns venue utilization metrics from Trino."""
    season = request.args.get('season', 'all')
    res = get_lakehouse_venue_stats(season=season)
    return jsonify(res)


@lakehouse_bp.route('/api/lakehouse/query', methods=['POST'])
def api_lakehouse_custom_query():
    """Executes a custom SELECT or SHOW query against Trino with safety constraints."""
    data = request.get_json() or {}
    raw_query = data.get('query', '').strip()

    if not raw_query:
        return jsonify({"success": False, "error": "Query cannot be empty."}), 400

    # Safety guard: only allow read-only analytical queries in dashboard console
    upper_query = raw_query.upper()
    allowed_prefixes = ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN")
    if not any(upper_query.startswith(p) for p in allowed_prefixes):
        return jsonify({
            "success": False, 
            "error": "Safety violation: Only SELECT, SHOW, DESCRIBE, and EXPLAIN queries are permitted in the analytics console."
        }), 403

    res = execute_trino_query(raw_query, timeout=15)
    return jsonify(res)
