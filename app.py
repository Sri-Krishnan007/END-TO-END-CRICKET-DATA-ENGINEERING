import os
from flask import Flask, redirect, url_for
from config.config import PROJECT_ROOT, get_active_db_mode

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "ipl_data_engineering_secret_key_12345")

@app.context_processor
def inject_db_context():
    mode = get_active_db_mode()
    return {
        "active_db_mode": mode,
        "is_online_db": (mode == "online"),
        "active_db_label": "Online (Supabase Cloud)" if mode == "online" else "Offline (Local PostgreSQL)"
    }

# Register Blueprints
from routes.home import home_bp
from routes.ingestion import ingestion_bp
from routes.pipeline import pipeline_bp
from routes.analytics import analytics_bp
from routes.api import api_bp
from routes.bpm import bpm_bp

app.register_blueprint(home_bp)
app.register_blueprint(ingestion_bp, url_for_security=True)
app.register_blueprint(pipeline_bp)
app.register_blueprint(analytics_bp)
app.register_blueprint(api_bp)
app.register_blueprint(bpm_bp)

@app.route('/')
def index_redirect():
    return redirect(url_for('home.index'))

if __name__ == '__main__':
    # Running locally on port 5000 (disabling reloader to prevent restarts when writing data files)
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)
