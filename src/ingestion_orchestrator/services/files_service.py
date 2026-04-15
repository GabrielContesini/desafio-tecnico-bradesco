from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion_orchestrator.audit import AuditService
from ingestion_orchestrator.config import OrchestratorConfig
from ingestion_orchestrator.hashing import spark_file_business_key_expr
from ingestion_orchestrator.parsing import with_parsed_metadata


@dataclass(frozen=True)
class FileRegistrationResult:
    raw_events: int
    unique_candidates: int
    inserted: int
    duplicates: int
    parse_errors: int


class FileEventsService:
    def __init__(self, spark: SparkSession, config: OrchestratorConfig, audit_service: AuditService) -> None:
        self.spark = spark
        self.config = config
        self.audit_service = audit_service

    def build_file_events_dataframe(self, raw_df: DataFrame) -> DataFrame:
        parsed = with_parsed_metadata(raw_df, timezone=self.config.timezone)
        checksum_column = (
            F.sha2(F.base64(F.col("content")), 256)
            if self.config.enable_file_checksum
            else F.lit(None).cast("string")
        )

        prepared = (
            parsed.withColumn("file_checksum", checksum_column)
            .withColumn("file_name", F.col("logical_file_name"))
            .withColumn("file_size", F.col("length").cast("long"))
            .withColumn("source_modification_time", F.col("modificationTime"))
            .drop(
                "path",
                "length",
                "modificationTime",
                "content",
                "dt_ref_str",
                "part_number_str",
                "manifest_json",
                "logical_file_name",
            )
        )
        return spark_file_business_key_expr(prepared).select(
            "file_event_id",
            "file_path",
            "file_name",
            "system_name",
            "table_name",
            "dt_ref",
            "part_number",
            "is_manifest",
            "expected_parts",
            "group_key",
            "file_business_key",
            "file_checksum",
            "file_size",
            "source_modification_time",
            "arrived_at",
            "ingested_at",
            "is_duplicate",
            "parse_error",
            "raw_metadata_json",
        )

    def register_batch(self, events_df: DataFrame) -> FileRegistrationResult:
        if events_df.head(1) == []:
            return FileRegistrationResult(0, 0, 0, 0, 0)

        deduped_candidates = events_df.dropDuplicates(["file_business_key"])
        existing_keys = self.spark.table(self.config.file_events_table).select("file_business_key")
        new_candidates = deduped_candidates.join(existing_keys, "file_business_key", "left_anti")
        duplicate_candidates = deduped_candidates.join(existing_keys, "file_business_key", "inner")

        delta_table = DeltaTable.forName(self.spark, self.config.file_events_table)
        delta_table.alias("t").merge(
            deduped_candidates.alias("s"),
            "t.file_business_key = s.file_business_key",
        ).whenNotMatchedInsertAll().execute()

        self._audit_new_files(new_candidates)
        self._audit_duplicates(duplicate_candidates)

        raw_events = events_df.count()
        unique_candidates = deduped_candidates.count()
        inserted = new_candidates.count()
        duplicates = raw_events - inserted
        parse_errors = new_candidates.filter(F.col("parse_error").isNotNull()).count()

        return FileRegistrationResult(
            raw_events=raw_events,
            unique_candidates=unique_candidates,
            inserted=inserted,
            duplicates=duplicates,
            parse_errors=parse_errors,
        )

    def _audit_new_files(self, new_candidates: DataFrame) -> None:
        if new_candidates.head(1) == []:
            return
        registered_events = new_candidates.select(
            F.lit("FILE_REGISTERED").alias("event_type"),
            F.col("group_key"),
            F.col("file_business_key"),
            F.to_json(
                F.struct(
                    "file_path",
                    "file_name",
                    "system_name",
                    "table_name",
                    "dt_ref",
                    "part_number",
                    "is_manifest",
                    "expected_parts",
                    "arrived_at",
                )
            ).alias("payload_json"),
            F.when(F.col("parse_error").isNotNull(), F.lit("WARN")).otherwise(F.lit("INFO")).alias("severity"),
            F.col("file_business_key").alias("dedupe_key"),
        )

        manifest_events = new_candidates.where(F.col("is_manifest")).select(
            F.lit("MANIFEST_REGISTERED").alias("event_type"),
            F.col("group_key"),
            F.col("file_business_key"),
            F.to_json(
                F.struct("file_name", "expected_parts", "arrived_at", "parse_error")
            ).alias("payload_json"),
            F.when(F.col("expected_parts").isNull(), F.lit("WARN")).otherwise(F.lit("INFO")).alias("severity"),
            F.concat_ws("|", F.col("file_business_key"), F.lit("manifest")).alias("dedupe_key"),
        )

        parse_errors = new_candidates.where(F.col("parse_error").isNotNull()).select(
            F.lit("PARSE_ERROR").alias("event_type"),
            F.col("group_key"),
            F.col("file_business_key"),
            F.to_json(F.struct("file_name", "file_path", "parse_error")).alias("payload_json"),
            F.lit("WARN").alias("severity"),
            F.concat_ws("|", F.col("file_business_key"), F.lit("parse_error")).alias("dedupe_key"),
        )

        self.audit_service.log_dataframe(registered_events.unionByName(manifest_events).unionByName(parse_errors))

    def _audit_duplicates(self, duplicates_df: DataFrame) -> None:
        if duplicates_df.head(1) == []:
            return
        events = duplicates_df.select(
            F.lit("FILE_DUPLICATE_IGNORED").alias("event_type"),
            F.col("group_key"),
            F.col("file_business_key"),
            F.to_json(
                F.struct("file_name", "file_path", "system_name", "table_name", "dt_ref", "part_number")
            ).alias("payload_json"),
            F.lit("INFO").alias("severity"),
            F.concat_ws("|", F.col("file_business_key"), F.lit("duplicate")).alias("dedupe_key"),
        )
        self.audit_service.log_dataframe(events)
