-- ============================================================================
-- BPM ANALYTICAL SQL QUERIES FOR IPL CRICKET DATA ENGINEERING PLATFORM
-- Database Target: PostgreSQL / Supabase
-- Target Table: bpm_process_events
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. AVERAGE PROCESS LEAD TIME (Across All Completed Cases)
-- ----------------------------------------------------------------------------
SELECT 
    COUNT(DISTINCT case_id) AS total_cases,
    ROUND(AVG(case_lead_time), 2) AS avg_lead_time_seconds,
    ROUND(MIN(case_lead_time), 2) AS min_lead_time_seconds,
    ROUND(MAX(case_lead_time), 2) AS max_lead_time_seconds
FROM (
    SELECT 
        case_id,
        EXTRACT(EPOCH FROM (MAX(COALESCE(end_timestamp, event_timestamp)) - MIN(event_timestamp))) AS case_lead_time
    FROM bpm_process_events
    GROUP BY case_id
    HAVING COUNT(event_id) >= 2
) case_durations;


-- ----------------------------------------------------------------------------
-- 2. CASE-LEVEL LEAD TIME & THROUGHPUT BREAKDOWN
-- ----------------------------------------------------------------------------
SELECT 
    case_id,
    MIN(event_timestamp) AS process_start,
    MAX(COALESCE(end_timestamp, event_timestamp)) AS process_end,
    ROUND(EXTRACT(EPOCH FROM (MAX(COALESCE(end_timestamp, event_timestamp)) - MIN(event_timestamp)))::numeric, 2) AS lead_time_seconds,
    SUM(records_processed) AS total_records_processed,
    COUNT(event_id) AS total_stages_executed,
    STRING_AGG(activity, ' -> ' ORDER BY event_timestamp) AS executed_path,
    CASE 
        WHEN bool_or(status = 'FAILED') THEN 'FAILED'
        WHEN bool_or(status = 'QUARANTINED') THEN 'QUARANTINED'
        ELSE 'SUCCESS'
    END AS overall_status
FROM bpm_process_events
GROUP BY case_id
ORDER BY process_start DESC;


-- ----------------------------------------------------------------------------
-- 3. STAGE-WISE DURATION BENCHMARK (Average, Min, Max, P95)
-- ----------------------------------------------------------------------------
SELECT 
    activity,
    COUNT(event_id) AS execution_count,
    ROUND(AVG(COALESCE(duration_seconds, EXTRACT(EPOCH FROM (end_timestamp - event_timestamp))))::numeric, 2) AS avg_duration_seconds,
    ROUND(MIN(COALESCE(duration_seconds, EXTRACT(EPOCH FROM (end_timestamp - event_timestamp))))::numeric, 2) AS min_duration_seconds,
    ROUND(MAX(COALESCE(duration_seconds, EXTRACT(EPOCH FROM (end_timestamp - event_timestamp))))::numeric, 2) AS max_duration_seconds,
    SUM(records_processed) AS total_records_processed
FROM bpm_process_events
WHERE status != 'RUNNING'
GROUP BY activity
ORDER BY avg_duration_seconds DESC;


-- ----------------------------------------------------------------------------
-- 4. ACTIVE BOTTLENECK STAGES (Ranked by Latency Share)
-- ----------------------------------------------------------------------------
WITH stage_aggregates AS (
    SELECT 
        activity,
        SUM(COALESCE(duration_seconds, 0)) AS total_stage_time,
        AVG(COALESCE(duration_seconds, 0)) AS avg_stage_time,
        COUNT(event_id) AS total_runs
    FROM bpm_process_events
    GROUP BY activity
)
SELECT 
    activity AS bottleneck_stage,
    total_runs,
    ROUND(avg_stage_time::numeric, 2) AS avg_duration_seconds,
    ROUND((total_stage_time / NULLIF(SUM(total_stage_time) OVER (), 0) * 100)::numeric, 1) AS latency_share_percentage,
    DENSE_RANK() OVER (ORDER BY total_stage_time DESC) AS bottleneck_rank
FROM stage_aggregates
ORDER BY bottleneck_rank ASC;


-- ----------------------------------------------------------------------------
-- 5. SLA BREACH COUNT & COMPLIANCE PERCENTAGE (Configured SLA Benchmarks)
-- ----------------------------------------------------------------------------
WITH sla_benchmarks AS (
    SELECT 'bronze' AS activity, 30.0 AS sla_limit UNION ALL
    SELECT 'source_ingestion', 30.0 UNION ALL
    SELECT 'staging', 25.0 UNION ALL
    SELECT 'validation', 15.0 UNION ALL
    SELECT 'cdc', 15.0 UNION ALL
    SELECT 'silver', 30.0 UNION ALL
    SELECT 'silver_load', 30.0 UNION ALL
    SELECT 'gold', 25.0 UNION ALL
    SELECT 'gold_load', 25.0 UNION ALL
    SELECT 'quarantine', 10.0
)
SELECT 
    e.activity,
    s.sla_limit,
    COUNT(e.event_id) AS total_executions,
    COUNT(CASE WHEN e.duration_seconds > s.sla_limit THEN 1 END) AS breach_count,
    ROUND((COUNT(CASE WHEN e.duration_seconds <= s.sla_limit THEN 1 END)::numeric / NULLIF(COUNT(e.event_id), 0) * 100), 1) AS sla_compliance_pct,
    ROUND(AVG(e.duration_seconds)::numeric, 2) AS avg_duration_seconds
FROM bpm_process_events e
JOIN sla_benchmarks s ON LOWER(e.activity) = LOWER(s.activity)
GROUP BY e.activity, s.sla_limit
ORDER BY breach_count DESC;


-- ----------------------------------------------------------------------------
-- 6. STAGE-BY-STAGE DROP-OFF / FAILURE RATE
-- ----------------------------------------------------------------------------
SELECT 
    activity,
    COUNT(event_id) AS total_executions,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS successful_executions,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_executions,
    COUNT(CASE WHEN status = 'QUARANTINED' THEN 1 END) AS quarantined_executions,
    ROUND((COUNT(CASE WHEN status = 'FAILED' THEN 1 END)::numeric / NULLIF(COUNT(event_id), 0) * 100), 2) AS failure_rate_pct
FROM bpm_process_events
GROUP BY activity
ORDER BY total_executions DESC;


-- ----------------------------------------------------------------------------
-- 7. END-TO-END PIPELINE OPERATIONAL THROUGHPUT (Records / Second)
-- ----------------------------------------------------------------------------
SELECT 
    case_id,
    SUM(records_processed) AS total_records,
    ROUND(EXTRACT(EPOCH FROM (MAX(COALESCE(end_timestamp, event_timestamp)) - MIN(event_timestamp)))::numeric, 2) AS total_time_seconds,
    ROUND((SUM(records_processed)::numeric / NULLIF(EXTRACT(EPOCH FROM (MAX(COALESCE(end_timestamp, event_timestamp)) - MIN(event_timestamp))), 0))::numeric, 2) AS throughput_records_per_sec
FROM bpm_process_events
GROUP BY case_id
HAVING SUM(records_processed) > 0
ORDER BY total_time_seconds DESC;


-- ----------------------------------------------------------------------------
-- 8. PROCESS ANOMALY & EXCEPTION AUDIT SUMMARY
-- ----------------------------------------------------------------------------
SELECT 
    event_id,
    case_id,
    activity,
    status,
    duration_seconds,
    error_message,
    event_timestamp,
    CASE 
        WHEN status = 'FAILED' THEN 'STAGE_FAILURE'
        WHEN status = 'QUARANTINED' THEN 'QUALITY_QUARANTINE_EXCEPTION'
        WHEN duration_seconds > 45 THEN 'EXTREME_LATENCY_ANOMALY'
        ELSE 'NORMAL'
    END AS anomaly_classification
FROM bpm_process_events
WHERE status IN ('FAILED', 'QUARANTINED') OR duration_seconds > 45
ORDER BY event_timestamp DESC
LIMIT 50;


-- ----------------------------------------------------------------------------
-- 9. HISTORICAL SEASON EXECUTION LATENCY TRENDS
-- ----------------------------------------------------------------------------
SELECT 
    COALESCE(metadata->>'season', 'All') AS season,
    COUNT(DISTINCT case_id) AS total_runs,
    ROUND(AVG(duration_seconds)::numeric, 2) AS avg_stage_duration,
    SUM(records_processed) AS total_records_loaded
FROM bpm_process_events
GROUP BY metadata->>'season'
ORDER BY season DESC;
