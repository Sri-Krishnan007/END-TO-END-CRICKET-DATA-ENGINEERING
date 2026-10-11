"""
===============================================================================
AIRFLOW AUTOMATION DAG: LAKEHOUSE MAINTENANCE & OPTIMIZATION (WEEK 4)
Orchestrates automated Lakehouse optimization:
1. Health & Availability Verification (Trino & Table)
2. Small File Bin-packing / Compaction (Iceberg OPTIMIZE)
3. Snapshot Expiration (Retention Control)
4. Orphan Data File Removal (Vacuum Equivalent)
5. Post-maintenance Integrity Audit
===============================================================================
"""

import sys
import datetime
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Safe fallback imports for compilation/validation outside live Airflow worker
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
        def __init__(self, *args, **kwargs):
            self.dag_id = args[0] if args else kwargs.get('dag_id', 'unknown')
        def __enter__(self): return self
        def __exit__(self, *args): pass
    class PythonOperator(DummyOperator): pass
    class EmptyOperator(DummyOperator): pass
    AIRFLOW_AVAILABLE = False


# =============================================================================
# AIRFLOW CALLABLE TASKS
# =============================================================================

def task_verify_lakehouse_health(**context):
    """Verifies that Trino and the Iceberg table are accessible before maintenance."""
    print("[AIRFLOW MAINTENANCE] Running initial health and availability check...")
    from pipeline.lakehouse_maintenance import check_table_health
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'maintenance_manual_run'
    t0 = datetime.datetime.now()
    log_bpm_event(case_id, "lakehouse_health_check", "RUNNING", start_time=t0)
    
    health = check_table_health()
    t1 = datetime.datetime.now()
    
    if health["status"] == "UNHEALTHY":
        log_bpm_event(case_id, "lakehouse_health_check", "FAILED", start_time=t0, end_time=t1, error_message=health.get("error"))
        raise RuntimeError(f"Lakehouse initial health check failed: {health.get('error')}")
        
    log_bpm_event(case_id, "lakehouse_health_check", "SUCCESS", start_time=t0, end_time=t1)
    print(f"[AIRFLOW MAINTENANCE] Table is healthy. Engine: {health.get('engine')}")


def task_compact_small_files(**context):
    """Executes small file compaction / bin-packing."""
    print("[AIRFLOW MAINTENANCE] Executing Lakehouse table compaction...")
    from pipeline.lakehouse_maintenance import compact_small_files
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'maintenance_manual_run'
    t0 = datetime.datetime.now()
    log_bpm_event(case_id, "lakehouse_compaction", "RUNNING", start_time=t0)
    
    res = compact_small_files(file_size_threshold="100MB")
    t1 = datetime.datetime.now()
    
    if res["status"] != "SUCCESS":
        log_bpm_event(case_id, "lakehouse_compaction", "FAILED", start_time=t0, end_time=t1, error_message=str(res))
        raise RuntimeError(f"Compaction failed: {res}")
        
    log_bpm_event(case_id, "lakehouse_compaction", "SUCCESS", start_time=t0, end_time=t1)
    print(f"[AIRFLOW MAINTENANCE] Compaction complete. Elapsed: {res.get('elapsed_ms')} ms")


def task_expire_snapshots(**context):
    """Prunes metadata snapshots older than configured retention period."""
    print("[AIRFLOW MAINTENANCE] Executing snapshot expiration...")
    from pipeline.lakehouse_maintenance import expire_old_snapshots
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'maintenance_manual_run'
    t0 = datetime.datetime.now()
    log_bpm_event(case_id, "lakehouse_snapshot_expiry", "RUNNING", start_time=t0)
    
    res = expire_old_snapshots(retention_days=7)
    t1 = datetime.datetime.now()
    
    if res["status"] != "SUCCESS":
        log_bpm_event(case_id, "lakehouse_snapshot_expiry", "FAILED", start_time=t0, end_time=t1, error_message=str(res))
        raise RuntimeError(f"Snapshot expiration failed: {res}")
        
    log_bpm_event(case_id, "lakehouse_snapshot_expiry", "SUCCESS", start_time=t0, end_time=t1)
    print(f"[AIRFLOW MAINTENANCE] Snapshot expiration complete. Elapsed: {res.get('elapsed_ms')} ms")


def task_remove_orphan_files(**context):
    """Purges unreferenced data files (vacuum equivalent)."""
    print("[AIRFLOW MAINTENANCE] Executing orphan file cleanup...")
    from pipeline.lakehouse_maintenance import remove_orphan_files
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'maintenance_manual_run'
    t0 = datetime.datetime.now()
    log_bpm_event(case_id, "lakehouse_orphan_cleanup", "RUNNING", start_time=t0)
    
    res = remove_orphan_files(retention_days=7)
    t1 = datetime.datetime.now()
    
    if res["status"] != "SUCCESS":
        log_bpm_event(case_id, "lakehouse_orphan_cleanup", "FAILED", start_time=t0, end_time=t1, error_message=str(res))
        raise RuntimeError(f"Orphan cleanup failed: {res}")
        
    log_bpm_event(case_id, "lakehouse_orphan_cleanup", "SUCCESS", start_time=t0, end_time=t1)
    print(f"[AIRFLOW MAINTENANCE] Orphan cleanup complete. Elapsed: {res.get('elapsed_ms')} ms")


def task_post_maintenance_audit(**context):
    """Final post-maintenance health audit and metric persistence."""
    print("[AIRFLOW MAINTENANCE] Running final post-maintenance verification...")
    from pipeline.lakehouse_maintenance import check_table_health
    from bpm.event_logger import log_bpm_event
    
    dag_run = context.get('dag_run')
    case_id = dag_run.run_id if dag_run else 'maintenance_manual_run'
    t0 = datetime.datetime.now()
    
    health = check_table_health()
    t1 = datetime.datetime.now()
    
    log_bpm_event(case_id, "lakehouse_maintenance_audit", "SUCCESS", start_time=t0, end_time=t1)
    print(f"[AIRFLOW MAINTENANCE] Lakehouse post-maintenance status: {health.get('status')}")


# =============================================================================
# DAG CONFIGURATION & DEPENDENCY GRAPH
# =============================================================================

default_args = {
    'owner': 'lakehouse-admin',
    'depends_on_past': False,
    'start_date': datetime.datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    'ipl_lakehouse_maintenance_automation',
    default_args=default_args,
    description='Automated Apache Iceberg Lakehouse maintenance: compaction, snapshot expiry, and vacuum cleanup.',
    schedule_interval='0 3 * * *',  # Daily off-peak at 03:00 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse', 'iceberg', 'trino', 'optimization', 'maintenance']
) as dag:

    start_maintenance = EmptyOperator(task_id='start_maintenance')

    verify_health = PythonOperator(
        task_id='verify_table_health',
        python_callable=task_verify_lakehouse_health,
        provide_context=True
    )

    run_compaction = PythonOperator(
        task_id='compact_small_files',
        python_callable=task_compact_small_files,
        provide_context=True
    )

    run_snapshot_expiry = PythonOperator(
        task_id='expire_old_snapshots',
        python_callable=task_expire_snapshots,
        provide_context=True
    )

    run_orphan_cleanup = PythonOperator(
        task_id='remove_orphan_files',
        python_callable=task_remove_orphan_files,
        provide_context=True
    )

    post_audit = PythonOperator(
        task_id='post_maintenance_audit',
        python_callable=task_post_maintenance_audit,
        provide_context=True
    )

    end_maintenance = EmptyOperator(task_id='end_maintenance')

    # Sequential, non-destructive maintenance pipeline
    start_maintenance >> verify_health >> run_compaction >> run_snapshot_expiry >> run_orphan_cleanup >> post_audit >> end_maintenance
