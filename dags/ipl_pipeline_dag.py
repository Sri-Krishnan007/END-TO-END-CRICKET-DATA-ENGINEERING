import datetime
import sys
from pathlib import Path

# Add project root to sys.path to resolve workspace packages (pipeline, config, database)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Fallback classes if executing outside of Airflow environment (validation/compilation)
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    class DummyOperator:
        def __init__(self, *args, **kwargs): pass
        def __rshift__(self, other): return other
        def __lshift__(self, other): return other
    class DAG:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
    class PythonOperator(DummyOperator): pass
    class EmptyOperator(DummyOperator): pass
    AIRFLOW_AVAILABLE = False


# ==========================================
# AIRFLOW CALLABLE TASKS
# ==========================================

def ingest_task(**context):
    print("[AIRFLOW DAG] Starting Bronze Source Ingestion...")
    from pipeline.source_ingestion import ingest_source
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'airflow_manual_run'
    start_time = datetime.datetime.now()
    log_bpm_event(case_id, "source_ingestion", "RUNNING", start_time=start_time)
    
    res = ingest_source()
    end_time = datetime.datetime.now()
    
    if res["status"] == "FAILED":
        log_bpm_event(case_id, "source_ingestion", "FAILED", start_time=start_time, end_time=end_time, error_message=res['message'])
        raise RuntimeError(f"Bronze ingestion failed: {res['message']}")
        
    log_bpm_event(case_id, "source_ingestion", "SUCCESS", start_time=start_time, end_time=end_time)
    print(f"[AIRFLOW DAG] Ingestion completed. Source path: {res.get('path')}")


def produce_task(**context):
    print("[AIRFLOW DAG] Starting Kafka Producer...")
    from pipeline.kafka_client import run_producer
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'airflow_manual_run'
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    target_team = conf.get('target_team')
    target_player = conf.get('target_player')
    
    start_time = datetime.datetime.now()
    log_bpm_event(case_id, "kafka_producer", "RUNNING", start_time=start_time)
    
    published = run_producer(seasons, target_team=target_team, target_player=target_player)
    end_time = datetime.datetime.now()
    log_bpm_event(case_id, "kafka_producer", "SUCCESS", start_time=start_time, end_time=end_time, records_processed=published)
    print(f"[AIRFLOW DAG] Kafka Producer complete. Published {published} matches.")


def consume_task(**context):
    print("[AIRFLOW DAG] Starting Kafka Consumer...")
    from pipeline.kafka_client import run_consumer
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'airflow_manual_run'
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    
    start_time = datetime.datetime.now()
    log_bpm_event(case_id, "kafka_consumer", "RUNNING", start_time=start_time)
    
    staged = run_consumer(seasons)
    end_time = datetime.datetime.now()
    log_bpm_event(case_id, "kafka_consumer", "SUCCESS", start_time=start_time, end_time=end_time, records_processed=staged)
    print(f"[AIRFLOW DAG] Kafka Consumer complete. Staged {staged} matches in staging directory.")


def validate_task(**context):
    print("[AIRFLOW DAG] Starting Data Quality validation...")
    from pipeline.validation import validate_season
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'airflow_manual_run'
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    
    start_time = datetime.datetime.now()
    log_bpm_event(case_id, "validation", "RUNNING", start_time=start_time)
    
    total_valid = 0
    total_quarantined = 0
    for season in seasons:
        print(f"[AIRFLOW DAG] Running validations for Season {season}...")
        stats = validate_season(season)
        total_valid += stats.get("valid", 0)
        total_quarantined += stats.get("quarantined", 0)
        print(f"[AIRFLOW DAG] Season {season} validation completed: {stats}")
        
    end_time = datetime.datetime.now()
    log_bpm_event(case_id, "validation", "SUCCESS", start_time=start_time, end_time=end_time, records_processed=total_valid, metadata={"quarantined": total_quarantined})
    if total_quarantined > 0:
        log_bpm_event(case_id, "quarantine", "QUARANTINED", start_time=start_time, end_time=end_time, records_processed=total_quarantined)


def cdc_task(**context):
    print("[AIRFLOW DAG] Starting CDC detection check...")
    from pipeline.cdc import detect_changes_for_season
    from config.config import STAGING_PATH
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'airflow_manual_run'
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    run_id = context.get('run_id', case_id)
    
    start_time = datetime.datetime.now()
    log_bpm_event(case_id, "cdc", "RUNNING", start_time=start_time)
    
    cdc_results_by_season = {}
    total_cdc_records = 0
    for season in seasons:
        print(f"[AIRFLOW DAG] Running CDC check for Season {season}...")
        staged_json_files = list((STAGING_PATH / str(season) / "json").glob("*.json"))
        json_filepaths = [str(p) for p in staged_json_files]
        cdc_results = detect_changes_for_season(season, json_filepaths, run_id)
        cdc_results_serialized = {}
        for m_id, val in cdc_results.items():
            cdc_results_serialized[str(m_id)] = {
                "operation": val["operation"],
                "filepath": str(val["filepath"])
            }
        cdc_results_by_season[str(season)] = cdc_results_serialized
        total_cdc_records += len(cdc_results_serialized)
        print(f"[AIRFLOW DAG] Season {season} CDC check complete. Results count: {len(cdc_results_serialized)}")

    end_time = datetime.datetime.now()
    log_bpm_event(case_id, "cdc", "SUCCESS", start_time=start_time, end_time=end_time, records_processed=total_cdc_records)
    context['ti'].xcom_push(key='cdc_results_by_season', value=cdc_results_by_season)


def load_task(**context):
    print("[AIRFLOW DAG] Starting Atomic Silver/Gold DB Load...")
    from pipeline.atomicity import load_season_atomic
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'airflow_manual_run'
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    reprocess = conf.get('reprocess', False)
    simulate_failure = conf.get('simulate_failure', False)
    pipeline_mode = 'REPROCESS' if reprocess else 'INCREMENTAL'
    
    start_time = datetime.datetime.now()
    log_bpm_event(case_id, "silver_load", "RUNNING", start_time=start_time)
    log_bpm_event(case_id, "gold_load", "RUNNING", start_time=start_time)
    
    ti = context.get('ti')
    cdc_results_by_season = ti.xcom_pull(key='cdc_results_by_season', task_ids='detect_cdc_changes') or {}
    
    total_loaded = 0
    try:
        for season in seasons:
            print(f"[AIRFLOW DAG] Loading data for Season {season} in Mode: {pipeline_mode}...")
            cdc_results = cdc_results_by_season.get(str(season))
            
            cdc_results_deserialized = {}
            if cdc_results:
                for m_id, val in cdc_results.items():
                    cdc_results_deserialized[int(m_id)] = {
                        "operation": val["operation"],
                        "filepath": Path(val["filepath"])
                    }
            
            load_res = load_season_atomic(
                season,
                pipeline_mode=pipeline_mode,
                cdc_results=cdc_results_deserialized if cdc_results_deserialized else None,
                simulate_failure=simulate_failure
            )
            cnts = load_res.get('counts', {})
            total_loaded += cnts.get('inserted', 0) + cnts.get('updated', 0)
            print(f"[AIRFLOW DAG] Season {season} Atomic DB Load complete. Counts: {cnts}")
            
        end_time = datetime.datetime.now()
        log_bpm_event(case_id, "silver_load", "SUCCESS", start_time=start_time, end_time=end_time, records_processed=total_loaded)
        log_bpm_event(case_id, "gold_load", "SUCCESS", start_time=start_time, end_time=end_time, records_processed=total_loaded)
    except Exception as e:
        end_time = datetime.datetime.now()
        log_bpm_event(case_id, "silver_load", "FAILED", start_time=start_time, end_time=end_time, error_message=str(e))
        log_bpm_event(case_id, "gold_load", "FAILED", start_time=start_time, end_time=end_time, error_message=str(e))
        raise e


def lakehouse_task(**context):
    print("[AIRFLOW DAG] Starting Lakehouse Iceberg/Nessie/MinIO Sync...")
    from pipeline.iceberg_pipeline import run_iceberg_streaming_pipeline
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'airflow_manual_run'
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    
    start_time = datetime.datetime.now()
    log_bpm_event(case_id, "lakehouse_sync", "RUNNING", start_time=start_time)
    
    try:
        res = run_iceberg_streaming_pipeline(seasons)
        written = res.get('records_written', 0)
        end_time = datetime.datetime.now()
        log_bpm_event(case_id, "lakehouse_sync", "SUCCESS", start_time=start_time, end_time=end_time, records_processed=written)
        print(f"[AIRFLOW DAG] Lakehouse Sync complete. Ingested {written} records into Iceberg.")
    except Exception as e:
        end_time = datetime.datetime.now()
        log_bpm_event(case_id, "lakehouse_sync", "FAILED", start_time=start_time, end_time=end_time, error_message=str(e))
        raise e


# ==========================================
# DAG CONFIGURATION
# ==========================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
}

with DAG(
    'ipl_production_data_pipeline',
    default_args=default_args,
    description='An Airflow-orchestrated end-to-end IPL pipeline using Kafka messaging & Iceberg Lakehouse.',
    schedule_interval='@daily',
    catchup=False
) as dag:

    start = EmptyOperator(task_id='start')

    ingest_source_zip = PythonOperator(
        task_id='ingest_source_zip',
        python_callable=ingest_task,
        provide_context=True
    )

    produce_match_data = PythonOperator(
        task_id='produce_match_data',
        python_callable=produce_task,
        provide_context=True
    )

    consume_match_data = PythonOperator(
        task_id='consume_match_data',
        python_callable=consume_task,
        provide_context=True
    )

    validate_data_quality = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_task,
        provide_context=True
    )

    detect_cdc_changes = PythonOperator(
        task_id='detect_cdc_changes',
        python_callable=cdc_task,
        provide_context=True
    )

    load_warehouse_tables = PythonOperator(
        task_id='load_warehouse_tables',
        python_callable=load_task,
        provide_context=True
    )

    lakehouse_sync = PythonOperator(
        task_id='lakehouse_sync',
        python_callable=lakehouse_task,
        provide_context=True
    )

    end = EmptyOperator(task_id='end')

    # Task dependency graph
    start >> ingest_source_zip >> produce_match_data >> consume_match_data >> validate_data_quality >> detect_cdc_changes >> load_warehouse_tables >> lakehouse_sync >> end

