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
    res = ingest_source()
    if res["status"] == "FAILED":
        raise RuntimeError(f"Bronze ingestion failed: {res['message']}")
    print(f"[AIRFLOW DAG] Ingestion completed. Source path: {res.get('path')}")


def produce_task(**context):
    print("[AIRFLOW DAG] Starting Kafka Producer...")
    from pipeline.kafka_client import run_producer
    
    # Read runtime configurations passed to DAG Run
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    target_team = conf.get('target_team')
    target_player = conf.get('target_player')
    
    published = run_producer(seasons, target_team=target_team, target_player=target_player)
    print(f"[AIRFLOW DAG] Kafka Producer complete. Published {published} matches.")


def consume_task(**context):
    print("[AIRFLOW DAG] Starting Kafka Consumer...")
    from pipeline.kafka_client import run_consumer
    
    # Read runtime configurations
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    
    staged = run_consumer(seasons)
    print(f"[AIRFLOW DAG] Kafka Consumer complete. Staged {staged} matches in staging directory.")


def validate_task(**context):
    print("[AIRFLOW DAG] Starting Data Quality validation...")
    from pipeline.validation import validate_season
    
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    
    for season in seasons:
        print(f"[AIRFLOW DAG] Running validations for Season {season}...")
        stats = validate_season(season)
        print(f"[AIRFLOW DAG] Season {season} validation completed: {stats}")


def cdc_task(**context):
    print("[AIRFLOW DAG] Starting CDC detection check...")
    from pipeline.cdc import detect_changes_for_season
    from config.config import STAGING_PATH
    
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    run_id = context.get('run_id', 'airflow_run')
    
    cdc_results_by_season = {}
    for season in seasons:
        print(f"[AIRFLOW DAG] Running CDC check for Season {season}...")
        staged_json_files = list((STAGING_PATH / str(season) / "json").glob("*.json"))
        json_filepaths = [str(p) for p in staged_json_files]
        cdc_results = detect_changes_for_season(season, json_filepaths, run_id)
        # Convert Path keys to strings for clean XCom serialization
        cdc_results_serialized = {}
        for m_id, val in cdc_results.items():
            cdc_results_serialized[str(m_id)] = {
                "operation": val["operation"],
                "filepath": str(val["filepath"])
            }
        cdc_results_by_season[str(season)] = cdc_results_serialized
        print(f"[AIRFLOW DAG] Season {season} CDC check complete. Results count: {len(cdc_results_serialized)}")

    # Push to XCom to pass changes to database loader
    context['ti'].xcom_push(key='cdc_results_by_season', value=cdc_results_by_season)


def load_task(**context):
    print("[AIRFLOW DAG] Starting Atomic Silver/Gold DB Load...")
    from pipeline.atomicity import load_season_atomic
    
    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    seasons = conf.get('seasons', [2024])
    reprocess = conf.get('reprocess', False)
    simulate_failure = conf.get('simulate_failure', False)
    pipeline_mode = 'REPROCESS' if reprocess else 'INCREMENTAL'
    
    # Pull CDC results from XCom
    ti = context.get('ti')
    cdc_results_by_season = ti.xcom_pull(key='cdc_results_by_season', task_ids='detect_cdc_changes') or {}
    
    for season in seasons:
        print(f"[AIRFLOW DAG] Loading data for Season {season} in Mode: {pipeline_mode}...")
        cdc_results = cdc_results_by_season.get(str(season))
        
        # Deserialise string paths back to Path objects for database loader
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
        print(f"[AIRFLOW DAG] Season {season} Atomic DB Load complete. Counts: {load_res.get('counts')}")


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
    description='An Airflow-orchestrated end-to-end IPL pipeline using Kafka messaging.',
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

    end = EmptyOperator(task_id='end')

    # Task dependency graph
    start >> ingest_source_zip >> produce_match_data >> consume_match_data >> validate_data_quality >> detect_cdc_changes >> load_warehouse_tables >> end
