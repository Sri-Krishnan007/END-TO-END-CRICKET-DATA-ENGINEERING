from database.connection import get_db_cursor

def load_gold_match_summary(cur, season):
    """Populates FACT_MATCH_SUMMARY for matches of the specified season."""
    print(f"Loading Gold match summary facts for season {season}...")
    
    # Clean up existing match summaries for this season first
    cur.execute("DELETE FROM FACT_MATCH_SUMMARY WHERE season = %s", (str(season),))
    
    # Calculate aggregates from Silver and insert into FACT_MATCH_SUMMARY
    cur.execute(
        """
        INSERT INTO FACT_MATCH_SUMMARY (
            match_id, tournament, match_number, season, match_date, city, venue, 
            match_type, gender, scheduled_overs, balls_per_over, team_type, 
            team1, team2, toss_winner, toss_decision, winner, win_by_runs, 
            win_by_wickets, result, result_method, eliminator, player_of_match,
            total_runs, total_wickets, total_boundaries, total_sixes
        )
        SELECT 
            m.match_id, m.tournament, m.match_number, m.season, m.match_date, m.city, m.venue, 
            m.match_type, m.gender, m.scheduled_overs, m.balls_per_over, m.team_type, 
            m.team1, m.team2, m.toss_winner, m.toss_decision, m.winner, m.win_by_runs, 
            m.win_by_wickets, m.result, m.result_method, m.eliminator, m.player_of_match,
            COALESCE(r.total_runs, 0)::int,
            COALESCE(w.total_wickets, 0)::int,
            COALESCE(b.total_boundaries, 0)::int,
            COALESCE(s.total_sixes, 0)::int
        FROM match m
        LEFT JOIN (
            SELECT match_id, SUM(total_runs) as total_runs 
            FROM delivery 
            GROUP BY match_id
        ) r ON m.match_id = r.match_id
        LEFT JOIN (
            SELECT match_id, COUNT(wicket_id) as total_wickets 
            FROM wicket 
            GROUP BY match_id
        ) w ON m.match_id = w.match_id
        LEFT JOIN (
            SELECT match_id, COUNT(delivery_id) as total_boundaries 
            FROM delivery 
            WHERE batter_runs = 4 
            GROUP BY match_id
        ) b ON m.match_id = b.match_id
        LEFT JOIN (
            SELECT match_id, COUNT(delivery_id) as total_sixes 
            FROM delivery 
            WHERE batter_runs = 6 
            GROUP BY match_id
        ) s ON m.match_id = s.match_id
        WHERE m.season = %s
        """,
        (str(season),)
    )

def rebuild_gold_team_dimension(cur):
    """Regenerates the DIM_TEAM dimension table based on all matches in the system."""
    print("Rebuilding Gold team dimension table (DIM_TEAM)...")
    cur.execute("TRUNCATE TABLE DIM_TEAM")
    
    cur.execute(
        """
        INSERT INTO DIM_TEAM (team_name, matches_played, wins, total_runs, total_boundaries, total_sixes)
        SELECT 
            t.team_name,
            COUNT(DISTINCT m.match_id) as matches_played,
            COUNT(DISTINCT CASE WHEN m.winner = t.team_name THEN m.match_id END) as wins,
            COALESCE(r.total_runs, 0)::int,
            COALESCE(b.total_boundaries, 0)::int,
            COALESCE(s.total_sixes, 0)::int
        FROM team t
        LEFT JOIN match m ON m.team1 = t.team_name OR m.team2 = t.team_name
        LEFT JOIN (
            SELECT batting_team, SUM(total_runs) as total_runs
            FROM delivery
            GROUP BY batting_team
        ) r ON t.team_name = r.batting_team
        LEFT JOIN (
            SELECT batting_team, COUNT(delivery_id) as total_boundaries
            FROM delivery
            WHERE batter_runs = 4
            GROUP BY batting_team
        ) b ON t.team_name = b.batting_team
        LEFT JOIN (
            SELECT batting_team, COUNT(delivery_id) as total_sixes
            FROM delivery
            WHERE batter_runs = 6
            GROUP BY batting_team
        ) s ON t.team_name = s.batting_team
        GROUP BY t.team_name, r.total_runs, b.total_boundaries, s.total_sixes
        """
    )

def upsert_gold_team_dimension(cur):
    """Upserts team career summaries into DIM_TEAM dynamically without full truncation."""
    print("Performing incremental upsert on DIM_TEAM...")
    cur.execute(
        """
        INSERT INTO DIM_TEAM (team_name, matches_played, wins, total_runs, total_boundaries, total_sixes)
        SELECT 
            t.team_name,
            COUNT(DISTINCT m.match_id) as matches_played,
            COUNT(DISTINCT CASE WHEN m.winner = t.team_name THEN m.match_id END) as wins,
            COALESCE(r.total_runs, 0)::int,
            COALESCE(b.total_boundaries, 0)::int,
            COALESCE(s.total_sixes, 0)::int
        FROM team t
        LEFT JOIN match m ON m.team1 = t.team_name OR m.team2 = t.team_name
        LEFT JOIN (
            SELECT batting_team, SUM(total_runs) as total_runs
            FROM delivery
            GROUP BY batting_team
        ) r ON t.team_name = r.batting_team
        LEFT JOIN (
            SELECT batting_team, COUNT(delivery_id) as total_boundaries
            FROM delivery
            WHERE batter_runs = 4
            GROUP BY batting_team
        ) b ON t.team_name = b.batting_team
        LEFT JOIN (
            SELECT batting_team, COUNT(delivery_id) as total_sixes
            FROM delivery
            WHERE batter_runs = 6
            GROUP BY batting_team
        ) s ON t.team_name = s.batting_team
        GROUP BY t.team_name, r.total_runs, b.total_boundaries, s.total_sixes
        ON CONFLICT (team_name) DO UPDATE SET
            matches_played = EXCLUDED.matches_played,
            wins = EXCLUDED.wins,
            total_runs = EXCLUDED.total_runs,
            total_boundaries = EXCLUDED.total_boundaries,
            total_sixes = EXCLUDED.total_sixes
        """
    )

def rebuild_gold_player_dimension(cur):
    """Regenerates the DIM_PLAYER dimension table based on all matches in the system."""
    print("Rebuilding Gold player dimension table (DIM_PLAYER)...")
    cur.execute("TRUNCATE TABLE DIM_PLAYER")
    
    cur.execute(
        """
        INSERT INTO DIM_PLAYER (
            player_name, batting_matches, batting_innings, total_runs, total_balls, 
            total_fours, total_sixes, highest_score, fifties, hundreds,
            bowling_matches, bowling_innings, balls_bowled, overs_bowled, runs_conceded, 
            wickets_taken, best_wickets
        )
        SELECT 
            p.player_name,
            COALESCE(bat.matches_played, 0)::int,
            COALESCE(bat.innings_played, 0)::int,
            COALESCE(bat.total_runs, 0)::int,
            COALESCE(bat.total_balls, 0)::int,
            COALESCE(bat.total_fours, 0)::int,
            COALESCE(bat.total_sixes, 0)::int,
            COALESCE(bat.highest_score, 0)::int,
            COALESCE(bat.fifties, 0)::int,
            COALESCE(bat.hundreds, 0)::int,
            COALESCE(bowl.matches_bowled, 0)::int,
            COALESCE(bowl.innings_bowled, 0)::int,
            COALESCE(bowl.balls_bowled, 0)::int,
            COALESCE(bowl.overs_bowled, 0)::numeric,
            COALESCE(bowl.runs_conceded, 0)::int,
            COALESCE(bowl.wickets_taken, 0)::int,
            COALESCE(bowl.best_wickets, 0)::int
        FROM player p
        LEFT JOIN (
            SELECT 
                player_name,
                COUNT(DISTINCT match_id) as matches_played,
                COUNT(DISTINCT (match_id, innings_number)) as innings_played,
                SUM(match_runs) as total_runs,
                SUM(match_balls) as total_balls,
                SUM(match_fours) as total_fours,
                SUM(match_sixes) as total_sixes,
                MAX(match_runs) as highest_score,
                SUM(CASE WHEN match_runs >= 50 AND match_runs < 100 THEN 1 ELSE 0 END) as fifties,
                SUM(CASE WHEN match_runs >= 100 THEN 1 ELSE 0 END) as hundreds
            FROM (
                SELECT 
                    match_id, 
                    innings_number,
                    batter as player_name, 
                    SUM(batter_runs) as match_runs,
                    COUNT(delivery_id) as match_balls,
                    SUM(CASE WHEN batter_runs = 4 THEN 1 ELSE 0 END) as match_fours,
                    SUM(CASE WHEN batter_runs = 6 THEN 1 ELSE 0 END) as match_sixes
                FROM delivery
                GROUP BY match_id, innings_number, batter
            ) mr
            GROUP BY player_name
        ) bat ON p.player_name = bat.player_name
        LEFT JOIN (
            SELECT 
                player_name,
                COUNT(DISTINCT match_id) as matches_bowled,
                COUNT(DISTINCT (match_id, innings_number)) as innings_bowled,
                SUM(match_balls) as balls_bowled,
                ROUND((SUM(match_balls) / 6.0), 1) as overs_bowled,
                SUM(match_runs_conceded) as runs_conceded,
                SUM(match_wickets) as wickets_taken,
                MAX(match_wickets) as best_wickets
            FROM (
                SELECT 
                    d.match_id,
                    d.innings_number,
                    d.bowler as player_name,
                    COUNT(d.delivery_id) as match_balls,
                    SUM(d.total_runs) as match_runs_conceded,
                    COALESCE(w.wickets, 0) as match_wickets
                FROM delivery d
                LEFT JOIN (
                    SELECT match_id, innings_number, bowler, COUNT(wicket_id) as wickets
                    FROM wicket
                    GROUP BY match_id, innings_number, bowler
                ) w ON d.match_id = w.match_id AND d.innings_number = w.innings_number AND d.bowler = w.bowler
                GROUP BY d.match_id, d.innings_number, d.bowler, w.wickets
            ) mb
            GROUP BY player_name
        ) bowl ON p.player_name = bowl.player_name
        WHERE bat.player_name IS NOT NULL OR bowl.player_name IS NOT NULL
        """
    )

def upsert_gold_player_dimension(cur):
    """Upserts player career statistics into DIM_PLAYER dynamically without full truncation."""
    print("Performing incremental upsert on DIM_PLAYER...")
    cur.execute(
        """
        INSERT INTO DIM_PLAYER (
            player_name, batting_matches, batting_innings, total_runs, total_balls, 
            total_fours, total_sixes, highest_score, fifties, hundreds,
            bowling_matches, bowling_innings, balls_bowled, overs_bowled, runs_conceded, 
            wickets_taken, best_wickets
        )
        SELECT 
            p.player_name,
            COALESCE(bat.matches_played, 0)::int,
            COALESCE(bat.innings_played, 0)::int,
            COALESCE(bat.total_runs, 0)::int,
            COALESCE(bat.total_balls, 0)::int,
            COALESCE(bat.total_fours, 0)::int,
            COALESCE(bat.total_sixes, 0)::int,
            COALESCE(bat.highest_score, 0)::int,
            COALESCE(bat.fifties, 0)::int,
            COALESCE(bat.hundreds, 0)::int,
            COALESCE(bowl.matches_bowled, 0)::int,
            COALESCE(bowl.innings_bowled, 0)::int,
            COALESCE(bowl.balls_bowled, 0)::int,
            COALESCE(bowl.overs_bowled, 0)::numeric,
            COALESCE(bowl.runs_conceded, 0)::int,
            COALESCE(bowl.wickets_taken, 0)::int,
            COALESCE(bowl.best_wickets, 0)::int
        FROM player p
        LEFT JOIN (
            SELECT 
                player_name,
                COUNT(DISTINCT match_id) as matches_played,
                COUNT(DISTINCT (match_id, innings_number)) as innings_played,
                SUM(match_runs) as total_runs,
                SUM(match_balls) as total_balls,
                SUM(match_fours) as total_fours,
                SUM(match_sixes) as total_sixes,
                MAX(match_runs) as highest_score,
                SUM(CASE WHEN match_runs >= 50 AND match_runs < 100 THEN 1 ELSE 0 END) as fifties,
                SUM(CASE WHEN match_runs >= 100 THEN 1 ELSE 0 END) as hundreds
            FROM (
                SELECT 
                    match_id, 
                    innings_number,
                    batter as player_name, 
                    SUM(batter_runs) as match_runs,
                    COUNT(delivery_id) as match_balls,
                    SUM(CASE WHEN batter_runs = 4 THEN 1 ELSE 0 END) as match_fours,
                    SUM(CASE WHEN batter_runs = 6 THEN 1 ELSE 0 END) as match_sixes
                FROM delivery
                GROUP BY match_id, innings_number, batter
            ) mr
            GROUP BY player_name
        ) bat ON p.player_name = bat.player_name
        LEFT JOIN (
            SELECT 
                player_name,
                COUNT(DISTINCT match_id) as matches_bowled,
                COUNT(DISTINCT (match_id, innings_number)) as innings_bowled,
                SUM(match_balls) as balls_bowled,
                ROUND((SUM(match_balls) / 6.0), 1) as overs_bowled,
                SUM(match_runs_conceded) as runs_conceded,
                SUM(match_wickets) as wickets_taken,
                MAX(match_wickets) as best_wickets
            FROM (
                SELECT 
                    d.match_id,
                    d.innings_number,
                    d.bowler as player_name,
                    COUNT(d.delivery_id) as match_balls,
                    SUM(d.total_runs) as match_runs_conceded,
                    COALESCE(w.wickets, 0) as match_wickets
                FROM delivery d
                LEFT JOIN (
                    SELECT match_id, innings_number, bowler, COUNT(wicket_id) as wickets
                    FROM wicket
                    GROUP BY match_id, innings_number, bowler
                ) w ON d.match_id = w.match_id AND d.innings_number = w.innings_number AND d.bowler = w.bowler
                GROUP BY d.match_id, d.innings_number, d.bowler, w.wickets
            ) mb
            GROUP BY player_name
        ) bowl ON p.player_name = bowl.player_name
        WHERE bat.player_name IS NOT NULL OR bowl.player_name IS NOT NULL
        ON CONFLICT (player_name) DO UPDATE SET
            batting_matches = EXCLUDED.batting_matches,
            batting_innings = EXCLUDED.batting_innings,
            total_runs = EXCLUDED.total_runs,
            total_balls = EXCLUDED.total_balls,
            total_fours = EXCLUDED.total_fours,
            total_sixes = EXCLUDED.total_sixes,
            highest_score = EXCLUDED.highest_score,
            fifties = EXCLUDED.fifties,
            hundreds = EXCLUDED.hundreds,
            bowling_matches = EXCLUDED.bowling_matches,
            bowling_innings = EXCLUDED.bowling_innings,
            balls_bowled = EXCLUDED.balls_bowled,
            overs_bowled = EXCLUDED.overs_bowled,
            runs_conceded = EXCLUDED.runs_conceded,
            wickets_taken = EXCLUDED.wickets_taken,
            best_wickets = EXCLUDED.best_wickets
        """
    )

def load_gold_season(season):
    """Processes Gold layer updates for the target season within a single transaction."""
    try:
        with get_db_cursor(commit=True) as cur:
            load_gold_match_summary(cur, season)
            rebuild_gold_team_dimension(cur)
            rebuild_gold_player_dimension(cur)
        print(f"Season {season} Gold layer processing completed successfully!")
        return True
    except Exception as e:
        print(f"Failed to process Gold layer for season {season}: {e}")
        return False

if __name__ == "__main__":
    load_gold_season(2024)
