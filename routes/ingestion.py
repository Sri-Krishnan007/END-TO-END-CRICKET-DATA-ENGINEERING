from flask import Blueprint, render_template, jsonify, redirect, url_for, flash
from pipeline.source_ingestion import ingest_source, check_ingested
from database.connection import get_db_cursor

ingestion_bp = Blueprint('ingestion', __name__)

@ingestion_bp.route('/ingestion')
def index():
    is_available = check_ingested()
    metadata = None
    
    if is_available:
        try:
            with get_db_cursor(commit=False, cursor_factory='dict') as cur:
                cur.execute(
                    "SELECT local_path, file_hash, file_size, downloaded_at, status FROM source_registry WHERE source_name = %s LIMIT 1",
                    ('cricsheet_ipl_json',)
                )
                metadata = cur.fetchone()
        except Exception as e:
            print(f"Failed to fetch source registry details: {e}")
            
    return render_template('ingestion.html', is_available=is_available, metadata=metadata)

@ingestion_bp.route('/ingestion/trigger', methods=['POST'])
def trigger():
    res = ingest_source()
    if res["status"] == "SUCCESS":
        flash(res["message"], "success")
    else:
        flash(res["message"], "danger")
    return redirect(url_for('ingestion.index'))
