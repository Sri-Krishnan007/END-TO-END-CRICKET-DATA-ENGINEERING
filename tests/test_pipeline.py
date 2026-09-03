import unittest
import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.config import BRONZE_PATH, DATABASE_URL
from database.connection import get_connection, get_db_cursor
from pipeline.source_ingestion import check_ingested
from pipeline.staging import get_match_season

class TestPipelineInfrastructure(unittest.TestCase):
    
    def test_database_connection(self):
        """Verifies database connectivity and credential matching."""
        try:
            conn = get_connection()
            self.assertIsNotNone(conn)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            res = cur.fetchone()
            self.assertEqual(res[0], 1)
            cur.close()
            conn.close()
        except Exception as e:
            self.fail(f"PostgreSQL connection test failed: {e}")
            
    def test_schema_tables_exist(self):
        """Checks if critical control and serving tables exist in the schema."""
        tables_to_check = [
            'source_registry', 'pipeline_control', 'pipeline_logs', 'quarantine_records',
            'match', 'team', 'player', 'innings', 'delivery', 'wicket',
            'fact_match_summary', 'dim_team', 'dim_player'
        ]
        
        try:
            with get_db_cursor(commit=False) as cur:
                for table in tables_to_check:
                    cur.execute(
                        "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = %s)",
                        (table,)
                    )
                    exists = cur.fetchone()[0]
                    self.assertTrue(exists, f"Table '{table}' does not exist in PostgreSQL schema")
        except Exception as e:
            self.fail(f"Failed to query database catalog schema: {e}")

    def test_get_match_season_fallback(self):
        """Checks season extraction helper logic."""
        sample_match = {
            "info": {
                "season": 2024
            }
        }
        self.assertEqual(get_match_season(sample_match), "2024")
        
        sample_fallback = {
            "info": {
                "dates": ["2018-04-07"]
            }
        }
        self.assertEqual(get_match_season(sample_fallback), "2018")

if __name__ == '__main__':
    unittest.main()
