from datetime import datetime
from bpm.config import STAGE_SLAS, PROCESS_SEQUENCE
from database.connection import get_db_cursor

def parse_iso(ts):
    """Safely converts string or datetime object to Python datetime."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    try:
        return datetime.fromisoformat(str(ts).replace('Z', ''))
    except Exception:
        return None

def calculate_stage_durations(events):
    """
    Computes average duration in seconds for each activity in the provided events list.
    Returns: dict mapping activity -> float duration_seconds.
    """
    stage_totals = {}
    stage_counts = {}
    for ev in events:
        activity = ev.get("activity")
        dur = ev.get("duration_seconds")
        if dur is None:
            start = parse_iso(ev.get("event_timestamp"))
            end = parse_iso(ev.get("end_timestamp"))
            if start and end:
                dur = max(0.0, round((end - start).total_seconds(), 3))
        if dur is not None and float(dur) >= 0:
            stage_totals[activity] = stage_totals.get(activity, 0.0) + float(dur)
            stage_counts[activity] = stage_counts.get(activity, 0) + 1
            
    durations = {}
    for act, total in stage_totals.items():
        count = stage_counts.get(act, 1)
        durations[act] = round(total / count, 3)
    return durations


def _calculate_single_case_cycle_time(case_events):
    """Calculates cycle time for a single case's events."""
    if not case_events:
        return 0.0
    start_times = []
    end_times = []
    for ev in case_events:
        st = parse_iso(ev.get("event_timestamp"))
        et = parse_iso(ev.get("end_timestamp"))
        if st:
            start_times.append(st)
        if et:
            end_times.append(et)
        elif st and ev.get("duration_seconds"):
            end_times.append(st)
    if not start_times:
        return 0.0
    min_start = min(start_times)
    max_end = max(end_times) if end_times else max(start_times)
    return max(0.0, round((max_end - min_start).total_seconds(), 3))


def calculate_cycle_time(events):
    """
    Calculates Process Cycle Time.
    If events span multiple case IDs, calculates the average cycle time per case.
    For a single case, returns that case's cycle time.
    """
    if not events:
        return 0.0
        
    cases = {}
    for ev in events:
        cid = ev.get("case_id") or "default"
        cases.setdefault(cid, []).append(ev)
        
    cycle_times = [_calculate_single_case_cycle_time(case_evs) for case_evs in cases.values()]
    cycle_times = [ct for ct in cycle_times if ct > 0]
    
    if not cycle_times:
        return 0.0
        
    return round(sum(cycle_times) / len(cycle_times), 3)


def calculate_lead_time(events):
    """
    Calculates total Lead Time for a case (total elapsed execution latency).
    For a single pipeline run, Lead Time equals the end-to-end Cycle Time.
    """
    return calculate_cycle_time(events)


def calculate_throughput(records_processed, duration_seconds):
    """
    Calculates operational Throughput:
    Throughput = Records Processed / Duration in seconds (records/sec).
    """
    if not duration_seconds or duration_seconds <= 0:
        return 0.0
    return round(float(records_processed) / float(duration_seconds), 2)


def identify_bottlenecks(events):
    """
    Identifies the stage(s) with the highest processing duration / latency.
    Returns: list of dicts [{'activity': str, 'duration_seconds': float, 'pct_of_total': float}]
    sorted by duration descending.
    """
    durations = calculate_stage_durations(events)
    if not durations:
        return []
        
    total_time = sum(durations.values())
    ranked = []
    for act, dur in durations.items():
        pct = round((dur / total_time * 100), 1) if total_time > 0 else 0.0
        ranked.append({
            "activity": act,
            "duration_seconds": dur,
            "pct_of_total": pct
        })
        
    ranked.sort(key=lambda x: x["duration_seconds"], reverse=True)
    return ranked


def calculate_sla_compliance(events, sla_thresholds=None):
    """
    Compares stage durations against configured SLA thresholds.
    Returns:
      - compliance_pct: float (0.0 to 100.0)
      - total_stages: int
      - breaches: list of {'activity': str, 'actual_seconds': float, 'sla_limit': float, 'breach_seconds': float}
    """
    slas = sla_thresholds or STAGE_SLAS
    durations = calculate_stage_durations(events)
    
    if not durations:
        return {"compliance_pct": 100.0, "total_stages": 0, "breaches": []}
        
    breaches = []
    compliant_count = 0
    
    for act, actual_dur in durations.items():
        limit = slas.get(act, slas.get("silver", 30.0))
        if actual_dur > limit:
            breaches.append({
                "activity": act,
                "actual_seconds": actual_dur,
                "sla_limit": limit,
                "breach_seconds": round(actual_dur - limit, 3)
            })
        else:
            compliant_count += 1
            
    total = len(durations)
    pct = round((compliant_count / total) * 100, 1) if total > 0 else 100.0
    
    return {
        "compliance_pct": pct,
        "total_stages": total,
        "breaches": breaches
    }


def get_process_summary(case_id=None):
    """
    Aggregates full BPM metrics summary for a specific case or across all recent executions.
    """
    from bpm.event_logger import get_bpm_events
    events = get_bpm_events(case_id=case_id, limit=200)
    
    if not events:
        return {
            "case_id": case_id,
            "cycle_time_seconds": 0.0,
            "lead_time_seconds": 0.0,
            "stage_durations": {},
            "bottlenecks": [],
            "sla_compliance": {"compliance_pct": 100.0, "breaches": []},
            "throughput_rps": 0.0,
            "total_records": 0,
            "event_count": 0
        }
        
    cycle_time = calculate_cycle_time(events)
    lead_time = calculate_lead_time(events)
    durations = calculate_stage_durations(events)
    bottlenecks = identify_bottlenecks(events)
    sla_comp = calculate_sla_compliance(events)
    
    total_records = sum(int(e.get("records_processed") or 0) for e in events if e.get("status") == "SUCCESS")
    throughput = calculate_throughput(total_records, cycle_time)
    
    primary_bottleneck = bottlenecks[0]["activity"] if bottlenecks else "none"
    
    return {
        "case_id": case_id or (events[0].get("case_id") if events else "all"),
        "cycle_time_seconds": cycle_time,
        "lead_time_seconds": lead_time,
        "stage_durations": durations,
        "bottlenecks": bottlenecks,
        "primary_bottleneck": primary_bottleneck,
        "sla_compliance": sla_comp,
        "throughput_rps": throughput,
        "total_records": total_records,
        "event_count": len(events)
    }
