from flask import Blueprint, render_template
from database.connection import get_db_cursor

home_bp = Blueprint('home', __name__)

@home_bp.route('/home')
def index():
    # Fetch general statistics from Database for the landing page
    stats = {
        "matches": 0,
        "seasons": 0,
        "teams": 0,
        "players": 0
    }
    
    try:
        with get_db_cursor(commit=False) as cur:
            # Matches
            cur.execute("SELECT COUNT(*) FROM match")
            stats["matches"] = cur.fetchone()[0]
            
            # Seasons
            cur.execute("SELECT COUNT(DISTINCT season) FROM match")
            stats["seasons"] = cur.fetchone()[0]
            
            # Teams
            cur.execute("SELECT COUNT(*) FROM team")
            stats["teams"] = cur.fetchone()[0]
            
            # Players
            cur.execute("SELECT COUNT(*) FROM player")
            stats["players"] = cur.fetchone()[0]
    except Exception as e:
        print(f"Failed to fetch home page statistics: {e}")
        
    return render_template('index.html', stats=stats)
