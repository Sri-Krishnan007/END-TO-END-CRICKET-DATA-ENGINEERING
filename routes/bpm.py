from flask import Blueprint, render_template, jsonify, request
from bpm.event_logger import get_bpm_events
from bpm.process_metrics import get_process_summary
from bpm.anomaly_detection import detect_anomalies
from bpm.process_stream import MockSparkProcessStream, PYSPARK_AVAILABLE
from database.connection import get_db_cursor

bpm_bp = Blueprint('bpm', __name__)

@bpm_bp.route('/bpm')
def bpm_dashboard():
    """Renders the executive Business Process Management (BPM) dashboard."""
    return render_template('bpm.html', pyspark_available=PYSPARK_AVAILABLE)


@bpm_bp.route('/api/bpm/summary')
def api_bpm_summary():
    """Returns aggregated BPM metrics (Cycle Time, Lead Time, Throughput, SLA Compliance %, Bottlenecks)."""
    case_id = request.args.get('case_id')
    summary = get_process_summary(case_id=case_id)
    return jsonify(summary)


@bpm_bp.route('/api/bpm/events')
def api_bpm_events():
    """Returns the list of recent BPM process events."""
    case_id = request.args.get('case_id')
    limit = int(request.args.get('limit', 100))
    events = get_bpm_events(case_id=case_id, limit=limit)
    return jsonify(events)


@bpm_bp.route('/api/bpm/events/<case_id>')
def api_bpm_case_events(case_id):
    """Returns all events for a specific pipeline execution case ID."""
    events = get_bpm_events(case_id=case_id, limit=100)
    return jsonify(events)


@bpm_bp.route('/api/bpm/anomalies')
def api_bpm_anomalies():
    """Runs anomaly detection (SLA breaches, out-of-sequence, duplicates, process deviations)."""
    case_id = request.args.get('case_id')
    events = get_bpm_events(case_id=case_id, limit=100)
    anomalies = detect_anomalies(events)
    return jsonify({
        "case_id": case_id or "all",
        "anomaly_count": len(anomalies),
        "anomalies": anomalies
    })


@bpm_bp.route('/api/bpm/stream-summary')
def api_bpm_stream():
    """Returns PySpark Structured Streaming processor status and micro-batch metrics."""
    events = get_bpm_events(limit=50)
    mock_stream = MockSparkProcessStream(events)
    summary = mock_stream.get_summary()
    return jsonify(summary)
