import unittest
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.config import clean_team_name, DATABASE_URL
from database.connection import get_connection, get_db_cursor
from pipeline.staging import get_match_season
from pipeline.cdc import compute_file_hash, detect_changes_for_season
from pipeline.validation import validate_match_json

class TestDataTransformations(unittest.TestCase):
    """Unit tests for core data engineering transformation and quality logic."""

    def test_canonical_team_name_mapping(self):
        """Verify historical team names are standardized to canonical names."""
        self.assertEqual(clean_team_name("Delhi Daredevils"), "Delhi Capitals")
        self.assertEqual(clean_team_name("Kings XI Punjab"), "Punjab Kings")
        self.assertEqual(clean_team_name("Royal Challengers Bangalore"), "Royal Challengers Bengaluru")
        self.assertEqual(clean_team_name("Rising Pune Supergiant"), "Rising Pune Supergiants")
        self.assertEqual(clean_team_name("Chennai Super Kings"), "Chennai Super Kings")
        self.assertIsNone(clean_team_name(None))

    def test_get_match_season_extraction(self):
        """Verify extraction of tournament season from diverse Cricsheet JSON formats."""
        sample_match = {"info": {"season": 2024}}
        self.assertEqual(get_match_season(sample_match), "2024")
        
        sample_fallback = {"info": {"dates": ["2018-04-07"]}}
        self.assertEqual(get_match_season(sample_fallback), "2018")

    def test_cdc_sha256_hash_calculation(self):
        """Verify that identical match file payloads produce identical SHA-256 hashes."""
        import tempfile
        import json
        
        payload_1 = {"info": {"match_id": 1001, "teams": ["CSK", "MI"]}}
        payload_2 = {"info": {"match_id": 1001, "teams": ["CSK", "MI"]}}
        payload_3 = {"info": {"match_id": 1001, "teams": ["CSK", "RCB"]}}
        
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as f1, \
             tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as f2, \
             tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as f3:
            
            json.dump(payload_1, f1)
            json.dump(payload_2, f2)
            json.dump(payload_3, f3)
            f1_path, f2_path, f3_path = f1.name, f2.name, f3.name

        try:
            hash_1 = compute_file_hash(f1_path)
            hash_2 = compute_file_hash(f2_path)
            hash_3 = compute_file_hash(f3_path)
            
            self.assertEqual(hash_1, hash_2, "Identical files must yield the same SHA-256 hash")
            self.assertNotEqual(hash_1, hash_3, "Different files must yield different hashes")
            self.assertEqual(len(hash_1), 64, "SHA-256 hash length must be 64 characters")
        finally:
            for p in [f1_path, f2_path, f3_path]:
                if os.path.exists(p):
                    os.remove(p)

    def test_data_quality_validation_rules(self):
        """Verify that malformed match documents are caught and isolated."""
        import tempfile
        import json

        # Missing required keys
        invalid_match = {"some_other_key": 123}
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as f_inv:
            json.dump(invalid_match, f_inv)
            inv_path = f_inv.name

        # Valid mock match
        valid_match = {
            "info": {
                "match_type": "T20",
                "teams": ["Chennai Super Kings", "Mumbai Indians"],
                "dates": ["2024-04-14"],
                "venue": "Wankhede Stadium",
                "overs": 20,
                "players": {
                    "Chennai Super Kings": ["MS Dhoni", "RD Gaikwad", "RA Jadeja", "S Dube", "MM Ali", "DL Chahar", "TU Deshpande", "Mustafizur Rahman", "SN Thakur", "AM Rahane", "R Ravindra"],
                    "Mumbai Indians": ["RG Sharma", "Ishan Kishan", "SA Yadav", "HH Pandya", "TH David", "Romario Shepherd", "Mohammad Nabi", "SL Kamboj", "JJ Bumrah", "G Coetzee", "Akash Madhwal"]
                }
            },
            "innings": [
                {
                    "team": "Chennai Super Kings",
                    "overs": [
                        {
                            "over": 0,
                            "deliveries": [
                                {"batter": "MS Dhoni", "bowler": "JJ Bumrah", "runs": {"batter": 4, "extras": 0, "total": 4}}
                            ]
                        }
                    ]
                }
            ]
        }
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as f_val:
            json.dump(valid_match, f_val)
            val_path = f_val.name

        try:
            is_valid_bad, err_type, err_msg = validate_match_json("9999", inv_path, 2024)
            self.assertFalse(is_valid_bad)
            self.assertEqual(err_type, "SCHEMA_VALIDATION")

            is_valid_good, err_type_good, err_msg_good = validate_match_json("1001", val_path, 2024)
            self.assertTrue(is_valid_good, f"Valid match failed: {err_msg_good}")
        finally:
            if os.path.exists(inv_path):
                os.remove(inv_path)
            if os.path.exists(val_path):
                os.remove(val_path)


class TestPipelineMocking(unittest.TestCase):
    """Mock testing for message streaming and external service isolation."""

    def test_mock_kafka_producer_and_consumer(self):
        """Verify Kafka producer & consumer message flow via mock queue."""
        from pipeline.kafka_client import MockProducer, MockConsumer
        
        producer = MockProducer()
        test_payload = {"match_id": "test_001", "season": "2024", "raw_data": {"test": True}}
        producer.produce(topic="ipl_matches", key="test_001", value=test_payload)
        producer.flush()
        
        consumer = MockConsumer()
        consumer.subscribe(["ipl_matches"])
        msg = consumer.poll(timeout=0.5)
        self.assertIsNotNone(msg)
        self.assertEqual(msg.topic(), "ipl_matches")
        self.assertEqual(msg.key(), "test_001")
        consumer.close()


class TestDatabaseInfrastructure(unittest.TestCase):
    """Tests database connectivity and schema integrity on PostgreSQL/Supabase."""

    def test_database_connection(self):
        """Verifies database connectivity."""
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
            self.fail(f"Database connection test failed: {e}")

    def test_schema_tables_exist(self):
        """Checks if all required Silver, Gold, and Pipeline Control tables exist."""
        tables_to_check = [
            'source_registry', 'pipeline_control', 'pipeline_logs', 'quarantine_records',
            'cdc_states', 'cdc_log', 'pipeline_runs',
            'match', 'team', 'player', 'innings', 'delivery', 'wicket',
            'fact_match_summary', 'dim_team', 'dim_player'
        ]
        
        try:
            with get_db_cursor(commit=False) as cur:
                for table in tables_to_check:
                    cur.execute(
                        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE lower(table_name) = lower(%s))",
                        (table,)
                    )
                    exists = cur.fetchone()[0]
                    self.assertTrue(exists, f"Table '{table}' does not exist in schema")
        except Exception as e:
            self.fail(f"Failed to query database schema: {e}")

if __name__ == '__main__':
    unittest.main()
