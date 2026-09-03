from pipeline.pipeline_runner import start_pipeline_async

def trigger_historical_backfill(from_year, to_year, reason=None):
    """Launches a background pipeline execution to backfill a range of seasons."""
    seasons = list(range(from_year, to_year + 1))
    print(f"[BACKFILL] Initiating historical backfill for seasons {from_year} to {to_year}")
    print(f"[BACKFILL] Audited Reason: {reason or 'No reason provided'}")
    
    # Reprocess=True forces overwrite/merge to ensure idempotency
    run_id = start_pipeline_async(seasons, reprocess=True)
    return run_id
