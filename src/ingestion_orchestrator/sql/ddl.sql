-- Optional SQL bootstrap. The Python jobs already create these tables when missing.
-- Replace ${catalog} and ${schema} before executing manually.

CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema};

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.ops_ingestion_file_events (
  file_event_id STRING,
  file_path STRING,
  file_name STRING,
  system_name STRING,
  table_name STRING,
  dt_ref DATE,
  part_number INT,
  is_manifest BOOLEAN,
  expected_parts INT,
  group_key STRING,
  file_business_key STRING,
  file_checksum STRING,
  file_size BIGINT,
  source_modification_time TIMESTAMP,
  arrived_at TIMESTAMP,
  ingested_at TIMESTAMP,
  is_duplicate BOOLEAN,
  parse_error STRING,
  raw_metadata_json STRING
)
USING DELTA
PARTITIONED BY (dt_ref);

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.ops_ingestion_groups (
  group_key STRING,
  system_name STRING,
  table_name STRING,
  dt_ref DATE,
  expected_parts INT,
  received_parts INT,
  status STRING,
  ready_reason STRING,
  first_seen_at TIMESTAMP,
  last_seen_at TIMESTAMP,
  expires_at TIMESTAMP,
  triggered_at TIMESTAMP,
  incomplete_flag BOOLEAN,
  last_evaluated_at TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (dt_ref);

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.ops_ingestion_dispatch (
  dispatch_key STRING,
  group_key STRING,
  system_name STRING,
  table_name STRING,
  dt_ref DATE,
  dispatched_at TIMESTAMP,
  dispatch_reason STRING,
  expected_parts INT,
  received_parts INT,
  incomplete_flag BOOLEAN,
  files_snapshot_json STRING,
  status STRING
)
USING DELTA
PARTITIONED BY (dt_ref);

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.ops_ingestion_audit (
  audit_id STRING,
  event_type STRING,
  group_key STRING,
  file_business_key STRING,
  event_ts TIMESTAMP,
  payload_json STRING,
  severity STRING
)
USING DELTA;
