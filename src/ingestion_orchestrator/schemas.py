from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def file_events_schema() -> StructType:
    return StructType(
        [
            StructField("file_event_id", StringType(), False),
            StructField("file_path", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("system_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("dt_ref", DateType(), True),
            StructField("part_number", IntegerType(), True),
            StructField("is_manifest", BooleanType(), False),
            StructField("expected_parts", IntegerType(), True),
            StructField("group_key", StringType(), True),
            StructField("file_business_key", StringType(), False),
            StructField("file_checksum", StringType(), True),
            StructField("file_size", LongType(), True),
            StructField("source_modification_time", TimestampType(), True),
            StructField("arrived_at", TimestampType(), False),
            StructField("ingested_at", TimestampType(), False),
            StructField("is_duplicate", BooleanType(), False),
            StructField("parse_error", StringType(), True),
            StructField("raw_metadata_json", StringType(), True),
        ]
    )


def groups_schema() -> StructType:
    return StructType(
        [
            StructField("group_key", StringType(), False),
            StructField("system_name", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("dt_ref", DateType(), False),
            StructField("expected_parts", IntegerType(), True),
            StructField("received_parts", IntegerType(), False),
            StructField("status", StringType(), False),
            StructField("ready_reason", StringType(), True),
            StructField("first_seen_at", TimestampType(), True),
            StructField("last_seen_at", TimestampType(), True),
            StructField("expires_at", TimestampType(), True),
            StructField("triggered_at", TimestampType(), True),
            StructField("incomplete_flag", BooleanType(), False),
            StructField("last_evaluated_at", TimestampType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )


def dispatch_schema() -> StructType:
    return StructType(
        [
            StructField("dispatch_key", StringType(), False),
            StructField("group_key", StringType(), False),
            StructField("system_name", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("dt_ref", DateType(), False),
            StructField("dispatched_at", TimestampType(), False),
            StructField("dispatch_reason", StringType(), False),
            StructField("expected_parts", IntegerType(), True),
            StructField("received_parts", IntegerType(), False),
            StructField("incomplete_flag", BooleanType(), False),
            StructField("files_snapshot_json", StringType(), True),
            StructField("status", StringType(), False),
        ]
    )


def audit_schema() -> StructType:
    return StructType(
        [
            StructField("audit_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("group_key", StringType(), True),
            StructField("file_business_key", StringType(), True),
            StructField("event_ts", TimestampType(), False),
            StructField("payload_json", StringType(), True),
            StructField("severity", StringType(), False),
        ]
    )
