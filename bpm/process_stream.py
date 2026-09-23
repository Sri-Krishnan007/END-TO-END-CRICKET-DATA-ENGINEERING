import os
import json
import time
from datetime import datetime
from bpm.config import BPM_CONFIG, STAGE_SLAS

# Check if PySpark is available
try:
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json, col, when, window, current_timestamp, round as spark_round
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


# ============================================================================
# 1. EXPLICIT SPARK SCHEMA DEFINITION
# ============================================================================

def get_bpm_spark_schema():
    """Returns the explicit StructType schema for BPM event stream parsing."""
    if not PYSPARK_AVAILABLE:
        return None
        
    return StructType([
        StructField("case_id", StringType(), False),
        StructField("activity", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("end_timestamp", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("status", StringType(), False),
        StructField("records_processed", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("metadata", StringType(), True)
    ])


# ============================================================================
# 2. PYSPARK STRUCTURED STREAMING APPLICATION
# ============================================================================

def build_spark_bpm_streaming_app(kafka_bootstrap="localhost:9092", topic="ipl_bpm_events"):
    """
    Constructs an end-to-end PySpark Structured Streaming pipeline:
    Kafka / Event Stream -> from_json -> Schema Validation -> SLA Anomaly Detection -> Console/Memory Sink
    """
    if not PYSPARK_AVAILABLE:
        print("[PYSPARK STREAMING] PySpark is not installed in this environment. Use simulated streaming.")
        return None

    spark = SparkSession.builder \
        .appName("IPLPipelineBPMStreamAnalytics") \
        .master("local[*]") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = get_bpm_spark_schema()

    # 1. Read Stream from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON Value with Schema Enforcement
    parsed_stream = raw_stream \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. Add SLA Breach Detection Column
    # (Checking against 30s general stage SLA threshold)
    enriched_stream = parsed_stream \
        .withColumn(
            "is_sla_breach",
            when(col("duration_seconds") > 30.0, True).otherwise(False)
        ) \
        .withColumn(
            "throughput_rps",
            when(
                (col("records_processed") > 0) & (col("duration_seconds") > 0),
                spark_round(col("records_processed") / col("duration_seconds"), 2)
            ).otherwise(0.0)
        )

    # 4. Windowed Aggregations (5-minute sliding window with 1-minute watermark)
    windowed_aggregates = enriched_stream \
        .withWatermark("event_timestamp", "1 minute") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("activity")
        ) \
        .agg({
            "duration_seconds": "avg",
            "is_sla_breach": "sum",
            "records_processed": "sum"
        })

    return {
        "spark": spark,
        "parsed_stream": parsed_stream,
        "enriched_stream": enriched_stream,
        "windowed_aggregates": windowed_aggregates
    }


# ============================================================================
# 3. SIMULATED STREAM RUNNER (FALLBACK FOR LOCAL & UNIT TESTING)
# ============================================================================

class MockSparkProcessStream:
    """
    Simulates the PySpark Structured Streaming micro-batch event processor
    for lightweight local execution and unit test verification.
    """
    def __init__(self, events=None):
        self.events = events or []
        self.processed_micro_batches = []
        
    def add_event(self, event_dict):
        self.events.append(event_dict)
        
    def process_micro_batch(self):
        """Processes events in micro-batches, enforcing schema and calculating SLA flags."""
        results = []
        for ev in self.events:
            dur = float(ev.get("duration_seconds") or 0.0)
            rec = int(ev.get("records_processed") or 0)
            act = str(ev.get("activity") or "unknown")
            sla_limit = STAGE_SLAS.get(act, 30.0)
            
            row = {
                "case_id": str(ev.get("case_id")),
                "activity": act,
                "event_timestamp": str(ev.get("event_timestamp")),
                "duration_seconds": dur,
                "status": str(ev.get("status")),
                "records_processed": rec,
                "is_sla_breach": dur > sla_limit,
                "throughput_rps": round(rec / dur, 2) if dur > 0 else 0.0,
                "sla_limit": sla_limit
            }
            results.append(row)
            
        self.processed_micro_batches.append(results)
        return results

    def get_summary(self):
        latest = self.process_micro_batch()
        total_events = len(latest)
        breaches = sum(1 for r in latest if r["is_sla_breach"])
        avg_dur = round(sum(r["duration_seconds"] for r in latest) / total_events, 3) if total_events > 0 else 0.0
        
        return {
            "total_events_streamed": total_events,
            "sla_breach_count": breaches,
            "average_duration_seconds": avg_dur,
            "streaming_engine": "PySpark Structured Streaming" if PYSPARK_AVAILABLE else "Simulated PySpark Engine"
        }
