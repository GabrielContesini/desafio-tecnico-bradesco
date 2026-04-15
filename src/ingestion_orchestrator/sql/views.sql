-- Optional operational views.
-- Replace ${catalog} and ${schema} before executing manually.

CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_ingestion_group_status AS
SELECT
  status,
  ready_reason,
  COUNT(*) AS total_groups,
  SUM(CASE WHEN incomplete_flag THEN 1 ELSE 0 END) AS incomplete_groups
FROM ${catalog}.${schema}.ops_ingestion_groups
GROUP BY status, ready_reason;

CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_ingestion_file_quality AS
SELECT
  CAST(arrived_at AS DATE) AS arrived_date,
  COUNT(*) AS total_files,
  SUM(CASE WHEN parse_error IS NOT NULL THEN 1 ELSE 0 END) AS parse_errors
FROM ${catalog}.${schema}.ops_ingestion_file_events
GROUP BY CAST(arrived_at AS DATE);

CREATE OR REPLACE VIEW ${catalog}.${schema}.vw_ingestion_operational_metrics AS
SELECT event_type, severity, COUNT(*) AS event_count
FROM ${catalog}.${schema}.ops_ingestion_audit
GROUP BY event_type, severity;
