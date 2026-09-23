import json
from datetime import datetime
from bpm.config import STAGE_SLAS, PROCESS_SEQUENCE, VALID_TRANSITIONS, ALLOWED_RETRY_STAGES
from bpm.process_metrics import parse_iso


def _extract_season(ev):
    """Safely extracts season from event metadata if available."""
    meta = ev.get("metadata")
    if not meta:
        return None
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            return None
    if isinstance(meta, dict):
        return meta.get("season")
    return None


def detect_sla_breaches(events, sla_thresholds=None):
    """
    Detects any stage whose execution duration exceeded its configured SLA limit.
    """
    slas = sla_thresholds or STAGE_SLAS
    anomalies = []
    
    for ev in events:
        activity = ev.get("activity")
        dur = ev.get("duration_seconds")
        
        # Calculate duration if not populated
        if dur is None:
            start = parse_iso(ev.get("event_timestamp"))
            end = parse_iso(ev.get("end_timestamp"))
            if start and end:
                dur = round((end - start).total_seconds(), 3)
                
        if dur is not None:
            limit = slas.get(activity, slas.get("silver", 30.0))
            if float(dur) > limit:
                anomalies.append({
                    "type": "SLA_BREACH",
                    "severity": "HIGH",
                    "activity": activity,
                    "case_id": ev.get("case_id"),
                    "actual_seconds": float(dur),
                    "sla_limit": limit,
                    "message": f"Stage '{activity}' took {dur}s, breaching configured SLA limit of {limit}s."
                })
                
    return anomalies


def _detect_sequence_anomalies_for_case(case_events, seq_index_map, seq):
    """Detects sequence anomalies within a single case ID, respecting multi-season loops."""
    anomalies = []
    # Sort primarily by timestamp, and secondary by canonical stage index for simultaneous events
    sorted_events = sorted(
        case_events,
        key=lambda x: (
            str(x.get("event_timestamp") or ""),
            seq_index_map.get(x.get("activity"), 999),
            x.get("event_id") or 0
        )
    )
    
    highest_seen_idx = -1
    seen_activities = []
    current_season = None
    
    for ev in sorted_events:
        activity = ev.get("activity")
        if activity not in seq_index_map:
            continue
            
        season = _extract_season(ev)
        
        # Reset cycle if season boundary changed or if restarting with bronze after gold/end
        if season is not None and current_season is not None and season != current_season:
            highest_seen_idx = -1
            seen_activities = []
            current_season = season
        elif highest_seen_idx >= (len(seq) - 2) and activity in {"bronze", "staging"}:
            # Reached late stage (silver/gold) and looping back to bronze/staging for next cycle
            highest_seen_idx = -1
            seen_activities = []
            
        if current_season is None and season is not None:
            current_season = season
            
        current_idx = seq_index_map[activity]
        
        # If current activity rank is strictly lower than highest seen stage within this cycle
        if current_idx < highest_seen_idx:
            # Allow consecutive repeat of the exact same activity as a retry
            if seen_activities and activity == seen_activities[-1]:
                seen_activities.append(activity)
            else:
                expected_stage = seq[highest_seen_idx]
                anomalies.append({
                    "type": "OUT_OF_SEQUENCE",
                    "severity": "CRITICAL",
                    "activity": activity,
                    "case_id": ev.get("case_id"),
                    "observed_order": seen_activities + [activity],
                    "message": f"Activity '{activity}' executed out-of-sequence after '{expected_stage}'."
                })
        else:
            highest_seen_idx = max(highest_seen_idx, current_idx)
            seen_activities.append(activity)
            
    return anomalies


def detect_sequence_anomalies(events, canonical_sequence=None):
    """
    Detects Out-of-Sequence activities where a stage occurs prior to its required predecessor
    in the defined pipeline execution sequence. Groups by case_id.
    """
    seq = canonical_sequence or PROCESS_SEQUENCE
    seq_index_map = {stage: idx for idx, stage in enumerate(seq)}
    
    # Group events by case_id
    cases = {}
    for ev in events:
        cid = ev.get("case_id") or "default"
        cases.setdefault(cid, []).append(ev)
        
    all_anomalies = []
    for case_id, case_evs in cases.items():
        all_anomalies.extend(_detect_sequence_anomalies_for_case(case_evs, seq_index_map, seq))
        
    return all_anomalies


def detect_duplicate_activities(events, allow_retries=True):
    """
    Detects unexpected duplicate activity executions within the same case ID and season cycle.
    Distinguishes legitimate retries/reprocessing and multi-season iterations from accidental duplicates.
    Ignores intermediate 'RUNNING' start events to prevent false positives between start and terminal events.
    """
    anomalies = []
    cases = {}
    for ev in events:
        cid = ev.get("case_id") or "default"
        cases.setdefault(cid, []).append(ev)
        
    for cid, case_evs in cases.items():
        # Count terminal or unique activity instances (skip RUNNING to avoid double counting start and end)
        cycle_counts = {}
        for ev in case_evs:
            st = str(ev.get("status", "")).upper()
            if st == "RUNNING" and len(case_evs) > 1:
                # Check if there is a corresponding terminal event for this activity
                act = ev.get("activity")
                has_terminal = any(
                    e.get("activity") == act and str(e.get("status", "")).upper() in {"SUCCESS", "FAILED", "COMPLETED", "QUARANTINED"}
                    for e in case_evs
                )
                if has_terminal:
                    continue  # Skip start marker since terminal event is tracked

            act = ev.get("activity")
            season = _extract_season(ev) or "global"
            key = (season, act)
            cycle_counts[key] = cycle_counts.get(key, 0) + 1
            
        for (season, act), count in cycle_counts.items():
            if count > 1:
                is_valid_retry = allow_retries and (act in ALLOWED_RETRY_STAGES)
                if not is_valid_retry:
                    anomalies.append({
                        "type": "DUPLICATE_ACTIVITY",
                        "severity": "MEDIUM",
                        "activity": act,
                        "case_id": cid,
                        "execution_count": count,
                        "message": f"Activity '{act}' executed {count} times within the same process case cycle ({season})."
                    })
                    
    return anomalies


def detect_process_deviations(events, valid_transitions=None):
    """
    Detects illegal process transitions between consecutive stages for each case ID.
    Explicitly recognizes 'validation' -> 'quarantine' as a legitimate valid exception path
    and 'gold' -> 'bronze/staging' as valid multi-season iteration.
    """
    transitions = valid_transitions or VALID_TRANSITIONS
    anomalies = []
    
    cases = {}
    for ev in events:
        cid = ev.get("case_id") or "default"
        cases.setdefault(cid, []).append(ev)
        
    for cid, case_evs in cases.items():
        seq_map = {st: idx for idx, st in enumerate(PROCESS_SEQUENCE)}
        sorted_events = sorted(
            case_evs,
            key=lambda x: (
                str(x.get("event_timestamp") or ""),
                seq_map.get(x.get("activity"), 999),
                x.get("event_id") or 0
            )
        )
        if len(sorted_events) < 2:
            continue
            
        for i in range(len(sorted_events) - 1):
            curr_act = sorted_events[i].get("activity")
            next_act = sorted_events[i + 1].get("activity")
            
            # Same activity transition (e.g. RUNNING -> SUCCESS or retry) is valid
            if curr_act == next_act:
                continue
                
            allowed_targets = transitions.get(curr_act)
            
            if allowed_targets is not None and next_act not in allowed_targets:
                # Special check: quarantine is ALWAYS valid after validation failure
                if curr_act == "validation" and next_act == "quarantine":
                    continue
                # Multi-season loop check: gold can transition to bronze/staging
                if curr_act in {"gold", "gold_load"} and next_act in {"bronze", "staging", "source_ingestion"}:
                    continue
                    
                anomalies.append({
                    "type": "PROCESS_DEVIATION",
                    "severity": "HIGH",
                    "from_activity": curr_act,
                    "to_activity": next_act,
                    "case_id": cid,
                    "message": f"Invalid transition from '{curr_act}' to '{next_act}'. Expected one of {list(allowed_targets)}."
                })
                
    return anomalies


def detect_anomalies(events, sla_thresholds=None):
    """
    Runs the comprehensive 4-category BPM anomaly detection suite:
    1. SLA Breaches
    2. Out-of-Sequence Activities
    3. Duplicate Activities
    4. Process Deviations (with valid quarantine exception path)
    """
    if not events:
        return []
        
    all_anomalies = []
    all_anomalies.extend(detect_sla_breaches(events, sla_thresholds))
    all_anomalies.extend(detect_sequence_anomalies(events))
    all_anomalies.extend(detect_duplicate_activities(events))
    all_anomalies.extend(detect_process_deviations(events))
    
    return all_anomalies
