import unittest
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from bpm.config import STAGE_SLAS, PROCESS_SEQUENCE, VALID_TRANSITIONS
from bpm.event_logger import log_bpm_event, track_bpm_stage, get_bpm_events
from bpm.process_metrics import (
    calculate_cycle_time,
    calculate_stage_durations,
    calculate_throughput,
    identify_bottlenecks,
    calculate_sla_compliance,
    calculate_lead_time
)
from bpm.anomaly_detection import (
    detect_sla_breaches,
    detect_sequence_anomalies,
    detect_duplicate_activities,
    detect_process_deviations,
    detect_anomalies
)
from bpm.process_stream import MockSparkProcessStream


class TestBPMEventLogging(unittest.TestCase):
    """Tests event taxonomy, logging mechanics, and schema consistency."""

    def test_bpm_event_payload_structure(self):
        """Verifies that emitted BPM events strictly adhere to required event taxonomy."""
        start = datetime(2026, 1, 1, 10, 0, 0)
        end = datetime(2026, 1, 1, 10, 0, 15)
        
        event = log_bpm_event(
            case_id="test_case_001",
            activity="validation",
            status="SUCCESS",
            start_time=start,
            end_time=end,
            records_processed=74,
            metadata={"season": "2024"}
        )
        
        self.assertIsNotNone(event)
        self.assertEqual(event["case_id"], "test_case_001")
        self.assertEqual(event["activity"], "validation")
        self.assertEqual(event["status"], "SUCCESS")
        self.assertEqual(event["records_processed"], 74)
        self.assertEqual(event["duration_seconds"], 15.0)
        self.assertIn("event_timestamp", event)
        self.assertIn("end_timestamp", event)


class TestBPMProcessMetrics(unittest.TestCase):
    """Tests calculation of Cycle Time, Lead Time, Throughput, and Bottlenecks."""

    def setUp(self):
        t0 = datetime(2026, 1, 1, 12, 0, 0)
        self.sample_events = [
            {
                "case_id": "run_100",
                "activity": "bronze",
                "event_timestamp": t0.isoformat(),
                "end_timestamp": (t0 + timedelta(seconds=10)).isoformat(),
                "duration_seconds": 10.0,
                "status": "SUCCESS",
                "records_processed": 1
            },
            {
                "case_id": "run_100",
                "activity": "staging",
                "event_timestamp": (t0 + timedelta(seconds=10)).isoformat(),
                "end_timestamp": (t0 + timedelta(seconds=25)).isoformat(),
                "duration_seconds": 15.0,
                "status": "SUCCESS",
                "records_processed": 74
            },
            {
                "case_id": "run_100",
                "activity": "validation",
                "event_timestamp": (t0 + timedelta(seconds=25)).isoformat(),
                "end_timestamp": (t0 + timedelta(seconds=33)).isoformat(),
                "duration_seconds": 8.0,
                "status": "SUCCESS",
                "records_processed": 74
            },
            {
                "case_id": "run_100",
                "activity": "cdc",
                "event_timestamp": (t0 + timedelta(seconds=33)).isoformat(),
                "end_timestamp": (t0 + timedelta(seconds=40)).isoformat(),
                "duration_seconds": 7.0,
                "status": "SUCCESS",
                "records_processed": 74
            },
            {
                "case_id": "run_100",
                "activity": "silver",
                "event_timestamp": (t0 + timedelta(seconds=40)).isoformat(),
                "end_timestamp": (t0 + timedelta(seconds=70)).isoformat(),
                "duration_seconds": 30.0,
                "status": "SUCCESS",
                "records_processed": 74
            },
            {
                "case_id": "run_100",
                "activity": "gold",
                "event_timestamp": (t0 + timedelta(seconds=70)).isoformat(),
                "end_timestamp": (t0 + timedelta(seconds=85)).isoformat(),
                "duration_seconds": 15.0,
                "status": "SUCCESS",
                "records_processed": 74
            }
        ]

    def test_cycle_time_calculation(self):
        """Cycle time must equal Max(end_timestamp) - Min(event_timestamp) = 85.0s."""
        cycle_time = calculate_cycle_time(self.sample_events)
        self.assertEqual(cycle_time, 85.0)

    def test_lead_time_calculation(self):
        """Lead time must match overall process elapsed time."""
        lead_time = calculate_lead_time(self.sample_events)
        self.assertEqual(lead_time, 85.0)

    def test_stage_durations(self):
        """Verify individual stage duration mapping."""
        durations = calculate_stage_durations(self.sample_events)
        self.assertEqual(durations["bronze"], 10.0)
        self.assertEqual(durations["staging"], 15.0)
        self.assertEqual(durations["silver"], 30.0)

    def test_throughput_calculation(self):
        """Throughput = 74 records / 85 seconds = 0.87 rec/sec."""
        tp = calculate_throughput(74, 85.0)
        self.assertEqual(tp, 0.87)

    def test_bottleneck_detection(self):
        """Silver (30s) must be ranked as the #1 primary bottleneck."""
        bottlenecks = identify_bottlenecks(self.sample_events)
        self.assertTrue(len(bottlenecks) > 0)
        self.assertEqual(bottlenecks[0]["activity"], "silver")
        self.assertEqual(bottlenecks[0]["duration_seconds"], 30.0)


class TestBPMAnomalyDetection(unittest.TestCase):
    """Tests detection of SLA breaches, out-of-sequence, duplicates, and process deviations."""

    def test_sla_breach_detection(self):
        """Detect stage whose duration exceeds SLA limit (e.g. staging takes 40s when limit is 25s)."""
        events = [{
            "case_id": "case_sla",
            "activity": "staging",
            "duration_seconds": 40.0,
            "status": "SUCCESS"
        }]
        breaches = detect_sla_breaches(events, sla_thresholds={"staging": 25.0})
        self.assertEqual(len(breaches), 1)
        self.assertEqual(breaches[0]["type"], "SLA_BREACH")
        self.assertEqual(breaches[0]["activity"], "staging")
        self.assertEqual(breaches[0]["actual_seconds"], 40.0)

    def test_out_of_sequence_detection(self):
        """Detect when Silver is executed before Validation."""
        t0 = datetime(2026, 1, 1, 10, 0, 0)
        out_of_order_events = [
            {
                "case_id": "case_seq",
                "activity": "silver",
                "event_timestamp": t0.isoformat()
            },
            {
                "case_id": "case_seq",
                "activity": "validation",
                "event_timestamp": (t0 + timedelta(seconds=10)).isoformat()
            }
        ]
        anomalies = detect_sequence_anomalies(out_of_order_events)
        self.assertTrue(len(anomalies) > 0)
        self.assertEqual(anomalies[0]["type"], "OUT_OF_SEQUENCE")

    def test_duplicate_activity_detection(self):
        """Detect duplicate activity executions."""
        duplicate_events = [
            {"case_id": "case_dup", "activity": "ingest_source_zip"},
            {"case_id": "case_dup", "activity": "ingest_source_zip"}
        ]
        anomalies = detect_duplicate_activities(duplicate_events, allow_retries=False)
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]["type"], "DUPLICATE_ACTIVITY")

    def test_quarantine_is_valid_exception_path(self):
        """Validation -> Quarantine must NOT be flagged as an illegal process deviation."""
        t0 = datetime(2026, 1, 1, 10, 0, 0)
        quarantine_events = [
            {"case_id": "case_q", "activity": "validation", "event_timestamp": t0.isoformat()},
            {"case_id": "case_q", "activity": "quarantine", "event_timestamp": (t0 + timedelta(seconds=5)).isoformat()}
        ]
        deviations = detect_process_deviations(quarantine_events)
        self.assertEqual(len(deviations), 0, "Quarantine following validation is a valid exception path")

    def test_illegal_process_deviation_detection(self):
        """Bronze transitioning directly to Gold must be flagged as an invalid process deviation."""
        t0 = datetime(2026, 1, 1, 10, 0, 0)
        illegal_events = [
            {"case_id": "case_ill", "activity": "bronze", "event_timestamp": t0.isoformat()},
            {"case_id": "case_ill", "activity": "gold", "event_timestamp": (t0 + timedelta(seconds=5)).isoformat()}
        ]
        deviations = detect_process_deviations(illegal_events)
        self.assertEqual(len(deviations), 1)
        self.assertEqual(deviations[0]["type"], "PROCESS_DEVIATION")


    def test_multi_season_sequence_loop(self):
        """Verifies that running multiple seasons sequentially does NOT falsely flag bronze after gold as out of sequence."""
        t0 = datetime(2026, 1, 1, 10, 0, 0)
        multi_season_events = [
            # Season 2008 cycle
            {"case_id": "run_batch", "activity": "bronze", "metadata": {"season": "2008"}, "event_timestamp": t0.isoformat()},
            {"case_id": "run_batch", "activity": "gold", "metadata": {"season": "2008"}, "event_timestamp": (t0 + timedelta(seconds=10)).isoformat()},
            # Season 2009 cycle
            {"case_id": "run_batch", "activity": "bronze", "metadata": {"season": "2009"}, "event_timestamp": (t0 + timedelta(seconds=20)).isoformat()},
            {"case_id": "run_batch", "activity": "gold", "metadata": {"season": "2009"}, "event_timestamp": (t0 + timedelta(seconds=30)).isoformat()}
        ]
        anomalies = detect_sequence_anomalies(multi_season_events)
        self.assertEqual(len(anomalies), 0, "Multi-season batch looping is a valid sequential lifecycle")


class TestBPMStreamProcessing(unittest.TestCase):
    """Tests PySpark Structured Streaming micro-batch event processor."""

    def test_mock_spark_stream_micro_batch(self):
        """Verifies schema parsing and real-time SLA breach flagging in streaming micro-batches."""
        stream_events = [
            {"case_id": "stream_1", "activity": "validation", "duration_seconds": 12.0, "status": "SUCCESS", "records_processed": 50},
            {"case_id": "stream_1", "activity": "silver", "duration_seconds": 45.0, "status": "SUCCESS", "records_processed": 50}  # Breaches 30s limit
        ]
        
        processor = MockSparkProcessStream(stream_events)
        batch_results = processor.process_micro_batch()
        
        self.assertEqual(len(batch_results), 2)
        self.assertFalse(batch_results[0]["is_sla_breach"])
        self.assertTrue(batch_results[1]["is_sla_breach"])
        
        summary = processor.get_summary()
        self.assertEqual(summary["total_events_streamed"], 2)
        self.assertEqual(summary["sla_breach_count"], 1)


if __name__ == '__main__':
    unittest.main()
