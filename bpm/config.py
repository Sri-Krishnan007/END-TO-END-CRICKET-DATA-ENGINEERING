import os
from pathlib import Path
from config.config import PROJECT_ROOT, LOGS_PATH

# ============================================================================
# 1. PROCESS STAGE SLA THRESHOLDS (IN SECONDS)
# Note: These are project-defined process benchmark thresholds for monitoring.
# ============================================================================

STAGE_SLAS = {
    # Core Pipeline Stage SLAs
    "bronze": float(os.environ.get("SLA_BRONZE_SECONDS", 30.0)),
    "source_ingestion": float(os.environ.get("SLA_SOURCE_INGESTION_SECONDS", 30.0)),
    "produce": float(os.environ.get("SLA_KAFKA_PRODUCER_SECONDS", 20.0)),
    "kafka_producer": float(os.environ.get("SLA_KAFKA_PRODUCER_SECONDS", 20.0)),
    "consume": float(os.environ.get("SLA_KAFKA_CONSUMER_SECONDS", 20.0)),
    "kafka_consumer": float(os.environ.get("SLA_KAFKA_CONSUMER_SECONDS", 20.0)),
    "staging": float(os.environ.get("SLA_STAGING_SECONDS", 25.0)),
    "validation": float(os.environ.get("SLA_VALIDATION_SECONDS", 15.0)),
    "cdc": float(os.environ.get("SLA_CDC_SECONDS", 15.0)),
    "silver": float(os.environ.get("SLA_SILVER_SECONDS", 30.0)),
    "silver_load": float(os.environ.get("SLA_SILVER_SECONDS", 30.0)),
    "gold": float(os.environ.get("SLA_GOLD_SECONDS", 25.0)),
    "gold_load": float(os.environ.get("SLA_GOLD_SECONDS", 25.0)),
    "lakehouse": float(os.environ.get("SLA_LAKEHOUSE_SECONDS", 20.0)),
    "lakehouse_sync": float(os.environ.get("SLA_LAKEHOUSE_SECONDS", 20.0)),
    "lakehouse_streaming_ingest": float(os.environ.get("SLA_LAKEHOUSE_SECONDS", 20.0)),
    "quarantine": float(os.environ.get("SLA_QUARANTINE_SECONDS", 10.0)),
    "total_pipeline": float(os.environ.get("SLA_TOTAL_PIPELINE_SECONDS", 180.0))
}

# ============================================================================
# 2. STANDARD PROCESS SEQUENCES & TOPOLOGY
# ============================================================================

# Primary canonical sequential pipeline stages
PROCESS_SEQUENCE = [
    "bronze",
    "staging",
    "validation",
    "cdc",
    "silver",
    "gold",
    "lakehouse"
]

# Fine-grained streaming process sequence (including Kafka messaging)
FINE_GRAINED_SEQUENCE = [
    "source_ingestion",
    "kafka_producer",
    "kafka_consumer",
    "staging",
    "validation",
    "cdc",
    "silver_load",
    "gold_load",
    "lakehouse_sync"
]

# Valid directed transitions graph (Source -> Target set)
# Includes happy paths and legitimate exception paths (e.g. validation -> quarantine)
VALID_TRANSITIONS = {
    "init": {"bronze", "source_ingestion", "staging"},
    "bronze": {"staging", "kafka_producer", "validation"},
    "source_ingestion": {"kafka_producer", "staging"},
    "kafka_producer": {"kafka_consumer", "staging"},
    "kafka_consumer": {"staging", "validation"},
    "staging": {"validation", "cdc"},
    "validation": {"cdc", "quarantine", "silver", "silver_load"},
    "quarantine": {"done", "end", "cdc", "silver", "silver_load"},  # Valid exception path (partial or total)
    "cdc": {"silver", "silver_load"},
    "silver": {"gold", "gold_load", "done"},
    "silver_load": {"gold", "gold_load", "done"},
    "gold": {"done", "end", "serving", "bronze", "staging", "source_ingestion", "lakehouse", "lakehouse_sync"},
    "gold_load": {"done", "end", "serving", "bronze", "staging", "source_ingestion", "lakehouse", "lakehouse_sync"},
    "lakehouse": {"done", "end", "serving", "bronze", "staging", "source_ingestion"},
    "lakehouse_sync": {"done", "end", "serving", "bronze", "staging", "source_ingestion"}
}

# Stages where activity repetition constitutes legitimate replay/retry
ALLOWED_RETRY_STAGES = {
    "bronze", "source_ingestion", "produce", "consume",
    "staging", "validation", "cdc", "silver", "silver_load", 
    "gold", "gold_load", "lakehouse", "lakehouse_sync", "lakehouse_streaming_ingest"
}


# ============================================================================
# 3. BPM EVENT LOGGING CONFIGURATION
# ============================================================================

BPM_EVENT_TOPIC = os.environ.get("BPM_KAFKA_TOPIC", "ipl_bpm_events")
BPM_LOG_FILE = LOGS_PATH / "bpm_events.jsonl"

BPM_CONFIG = {
    "stage_slas": STAGE_SLAS,
    "process_sequence": PROCESS_SEQUENCE,
    "fine_grained_sequence": FINE_GRAINED_SEQUENCE,
    "valid_transitions": VALID_TRANSITIONS,
    "allowed_retry_stages": ALLOWED_RETRY_STAGES,
    "event_topic": BPM_EVENT_TOPIC,
    "log_file": str(BPM_LOG_FILE),
    "enable_db_logging": True,
    "enable_file_logging": True
}
