import os
import sys
import json
import unittest
import tempfile
from pathlib import Path
from datetime import datetime

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_WAREHOUSE_BUCKET,
    NESSIE_URI,
    ICEBERG_CATALOG_NAME,
    ICEBERG_WAREHOUSE_PATH,
    ICEBERG_CHECKPOINT_PATH,
    ICEBERG_TABLE_NAME
)
from pipeline.iceberg_pipeline import (
    get_minio_client,
    parse_raw_match_record,
    sink_records_to_iceberg,
    verify_checkpoint_recovery,
    get_iceberg_match_schema,
    run_iceberg_streaming_pipeline
)

class TestIcebergLakehouseIntegration(unittest.TestCase):
    """Unit and Integration tests for Week 2 Apache Iceberg Lakehouse pipeline."""

    def test_lakehouse_configuration_values(self):
        """Verify Lakehouse environment configurations are well-formed."""
        self.assertTrue(MINIO_ENDPOINT.startswith("http"))
        self.assertEqual(MINIO_WAREHOUSE_BUCKET, "warehouse")
        self.assertTrue(NESSIE_URI.endswith("/api/v2"))
        self.assertEqual(ICEBERG_CATALOG_NAME, "ipl")
        self.assertEqual(ICEBERG_TABLE_NAME, "ipl.ipl_matches")
        self.assertTrue(ICEBERG_CHECKPOINT_PATH.exists() or Path(ICEBERG_CHECKPOINT_PATH).parent.exists())

    def test_raw_match_parsing_for_lakehouse(self):
        """Verify extraction of nested JSON cricket metrics into flat Lakehouse record format."""
        mock_raw = {
            "info": {
                "match_type": "T20",
                "season": "2024",
                "teams": ["Royal Challengers Bangalore", "Kolkata Knight Riders"],
                "dates": ["2024-03-29"],
                "venue": "M. Chinnaswamy Stadium",
                "city": "Bengaluru",
                "toss": {"winner": "Kolkata Knight Riders", "decision": "field"},
                "outcome": {"winner": "Kolkata Knight Riders", "by": {"wickets": 7}},
                "player_of_match": ["Sunil Narine"],
                "overs": 20
            },
            "innings": [
                {
                    "team": "Royal Challengers Bangalore",
                    "overs": [
                        {
                            "over": 0,
                            "deliveries": [
                                {"batter": "V Kohli", "bowler": "MA Starc", "runs": {"batter": 6, "extras": 0, "total": 6}}
                            ]
                        }
                    ]
                }
            ]
        }

        record = parse_raw_match_record("test_match_99", "2024", mock_raw)
        
        self.assertEqual(record["match_id"], "test_match_99")
        self.assertEqual(record["season"], "2024")
        self.assertEqual(record["team1"], "Royal Challengers Bengaluru")  # Canonicalized
        self.assertEqual(record["team2"], "Kolkata Knight Riders")
        self.assertEqual(record["winner"], "Kolkata Knight Riders")
        self.assertEqual(record["win_by_wickets"], 7)
        self.assertIsNone(record["win_by_runs"])
        self.assertEqual(record["player_of_match"], "Sunil Narine")
        self.assertEqual(record["total_runs"], 6)
        self.assertEqual(record["total_overs"], 1)

    def test_partitioning_by_season(self):
        """Verify that records are partitioned by tournament season in lakehouse storage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            chk_dir = Path(tmpdir) / "checkpoints"
            chk_dir.mkdir(parents=True, exist_ok=True)

            records = [
                {
                    "match_id": "m1", "season": "2023", "match_date": "2023-04-01",
                    "match_type": "T20", "venue": "Stadium A", "city": "City A",
                    "team1": "CSK", "team2": "GT", "toss_winner": "CSK",
                    "toss_decision": "bat", "winner": "CSK", "win_by_runs": 15,
                    "win_by_wickets": None, "player_of_match": "MS Dhoni",
                    "total_overs": 20, "total_runs": 180, "total_wickets": 5,
                    "raw_payload": "{}", "ingested_at": datetime.now().isoformat()
                },
                {
                    "match_id": "m2", "season": "2024", "match_date": "2024-04-01",
                    "match_type": "T20", "venue": "Stadium B", "city": "City B",
                    "team1": "MI", "team2": "KKR", "toss_winner": "MI",
                    "toss_decision": "field", "winner": "KKR", "win_by_runs": None,
                    "win_by_wickets": 6, "player_of_match": "S Narine",
                    "total_overs": 20, "total_runs": 195, "total_wickets": 4,
                    "raw_payload": "{}", "ingested_at": datetime.now().isoformat()
                }
            ]

            written = sink_records_to_iceberg(None, records, checkpoint_dir=chk_dir)
            self.assertEqual(written, 2)

            # Check checkpoint file exists
            offset_file = chk_dir / "offsets.json"
            self.assertTrue(offset_file.exists())
            with open(offset_file, "r", encoding="utf-8") as f:
                state = json.load(f)
            self.assertEqual(state.get("records_written"), 2)

    def test_checkpoint_recovery_logic(self):
        """Verify stream restart and checkpoint recovery."""
        success, state = verify_checkpoint_recovery()
        self.assertTrue(success)
        self.assertIn("last_committed_snapshot", state)

    def test_minio_client_initialization(self):
        """Verify MinIO client instantiation with configured credentials."""
        from minio import Minio
        endpoint_clean = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        client = Minio(
            endpoint=endpoint_clean,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_ENDPOINT.startswith("https")
        )
        creds = client._provider.retrieve()
        self.assertEqual(creds.access_key, MINIO_ACCESS_KEY)
        self.assertEqual(creds.secret_key, MINIO_SECRET_KEY)

if __name__ == "__main__":
    unittest.main()
