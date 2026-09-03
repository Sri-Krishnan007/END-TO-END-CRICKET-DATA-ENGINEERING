from flask import Blueprint, render_template
from database.connection import get_db_cursor

analytics_bp = Blueprint('analytics', __name__)

@analytics_bp.route('/analytics')
def index():
    seasons = []
    teams = []
    players = []
    venues = []
    
    try:
        with get_db_cursor(commit=False) as cur:
            # Seasons
            cur.execute("SELECT DISTINCT season FROM FACT_MATCH_SUMMARY ORDER BY season DESC")
            seasons = [r[0] for r in cur.fetchall()]
            
            # Teams
            cur.execute("SELECT team_name FROM team ORDER BY team_name ASC")
            teams = [r[0] for r in cur.fetchall()]
            
            # Players
            cur.execute("SELECT player_name FROM player ORDER BY player_name ASC")
            players = [r[0] for r in cur.fetchall()]
            
            # Venues
            cur.execute("SELECT DISTINCT venue FROM FACT_MATCH_SUMMARY WHERE venue IS NOT NULL ORDER BY venue ASC")
            venues = [r[0] for r in cur.fetchall()]
    except Exception as e:
        print(f"Failed to fetch dropdown filters for analytics: {e}")
        
    return render_template(
        'analytics.html',
        seasons=seasons,
        teams=teams,
        players=players,
        venues=venues
    )

@analytics_bp.route('/eda')
def eda_page():
    seasons = []
    try:
        with get_db_cursor(commit=False) as cur:
            cur.execute("SELECT DISTINCT season FROM FACT_MATCH_SUMMARY ORDER BY season DESC")
            seasons = [r[0] for r in cur.fetchall()]
    except Exception as e:
        print(f"Failed to fetch seasons for EDA page: {e}")
        
    if not seasons:
        seasons = list(range(2024, 2007, -1))
        
    return render_template('eda.html', seasons=seasons)
