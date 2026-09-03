import os
from flask import Flask, redirect, url_for
from config.config import PROJECT_ROOT

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "ipl_data_engineering_secret_key_12345")

# Register Blueprints
from routes.home import home_bp
from routes.ingestion import ingestion_bp
from routes.pipeline import pipeline_bp
from routes.analytics import analytics_bp
from routes.api import api_bp

app.register_blueprint(home_bp)
app.register_blueprint(ingestion_bp, url_for_security=True)
app.register_blueprint(pipeline_bp)
app.register_blueprint(analytics_bp)
app.register_blueprint(api_bp)

@app.route('/')
def index_redirect():
    return redirect(url_for('home.index'))

if __name__ == '__main__':
    # Running locally on port 5000 (disabling reloader to prevent restarts when writing data files)
    app.run(debug=True, host='127.0.0.1', port=5000, use_reloader=False)
