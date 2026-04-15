from __future__ import annotations

from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion_orchestrator.audit import AuditService
from ingestion_orchestrator.config import OrchestratorConfig
from ingestion_orchestrator.hashing import spark_dispatch_key_expr


@dataclass(frozen=True)
class DispatchResult:
    ready_groups: int
    dispatched: int
    skipped_already_dispatched: int


class DispatchService:
    def __init__(self, spark: SparkSession, config: OrchestratorConfig, audit_service: AuditService) -> None:
        self.spark = spark
        self.config = config
        self.audit_service = audit_service

    def dispatch_ready_groups(self, limit: int | None = None) -> DispatchResult:
        ready_groups = self.spark.table(self.config.groups_table).where(F.col("status") == "READY")
        if limit is not None and limit > 0:
            ready_groups = ready_groups.limit(limit)
        if ready_groups.head(1) == []:
            return DispatchResult(0, 0, 0)

        ready_with_key = spark_dispatch_key_expr(ready_groups)
        existing_dispatch = self.spark.table(self.config.dispatch_table).select(
            F.col("dispatch_key"),
            F.col("group_key").alias("already_dispatched_group_key"),
            F.col("dispatched_at").alias("already_dispatched_at"),
        )

        to_dispatch = ready_with_key.join(
            existing_dispatch.select("dispatch_key"),
            "dispatch_key",
            "left_anti",
        )
        skipped = (
            ready_with_key.alias("r")
            .join(
                existing_dispatch.alias("d"),
                F.col("r.dispatch_key") == F.col("d.dispatch_key"),
                "inner",
            )
            .select(
                F.col("r.dispatch_key"),
                F.col("r.group_key"),
                F.col("r.system_name"),
                F.col("r.table_name"),
                F.col("r.dt_ref"),
                F.col("r.ready_reason"),
                F.col("r.expected_parts"),
                F.col("r.received_parts"),
                F.col("r.incomplete_flag"),
                F.col("d.already_dispatched_at").alias("dispatched_at"),
            )
        )

        dispatch_payload = (
            self._attach_file_snapshot(to_dispatch)
            .withColumn("dispatched_at", F.current_timestamp())
            .withColumn("dispatch_reason", F.col("ready_reason"))
            .withColumn("status", F.lit("SENT"))
            .select(
                "dispatch_key",
                "group_key",
                "system_name",
                "table_name",
                "dt_ref",
                "dispatched_at",
                "dispatch_reason",
                "expected_parts",
                "received_parts",
                "incomplete_flag",
                "files_snapshot_json",
                "status",
            )
        )

        if dispatch_payload.head(1) != []:
            dispatch_delta = DeltaTable.forName(self.spark, self.config.dispatch_table)
            dispatch_delta.alias("t").merge(
                dispatch_payload.alias("s"),
                "t.dispatch_key = s.dispatch_key",
            ).whenNotMatchedInsertAll().execute()

        self._mark_groups_as_dispatched(dispatch_payload, skipped)
        self._audit_dispatch(dispatch_payload, skipped)

        return DispatchResult(
            ready_groups=ready_with_key.count(),
            dispatched=dispatch_payload.count(),
            skipped_already_dispatched=skipped.count(),
        )

    def _attach_file_snapshot(self, groups_df: DataFrame) -> DataFrame:
        if groups_df.head(1) == []:
            return groups_df.withColumn("files_snapshot_json", F.lit("[]"))

        files = self.spark.table(self.config.file_events_table).where(
            F.col("parse_error").isNull() & ~F.col("is_manifest")
        )
        grouped = files.groupBy("group_key").agg(
            F.array_sort(
                F.collect_list(
                    F.struct(
                        F.col("part_number"),
                        F.col("file_name"),
                        F.col("file_path"),
                        F.col("arrived_at"),
                        F.col("file_checksum"),
                    )
                )
            ).alias("parts_snapshot")
        )
        return groups_df.join(grouped, "group_key", "left").withColumn(
            "files_snapshot_json",
            F.coalesce(F.to_json(F.col("parts_snapshot")), F.lit("[]")),
        )

    def _mark_groups_as_dispatched(self, dispatched_df: DataFrame, skipped_df: DataFrame) -> None:
        updates_from_dispatched = dispatched_df.select("group_key", "dispatched_at")
        updates_from_skipped = skipped_df.select("group_key", "dispatched_at")
        updates = updates_from_dispatched.unionByName(updates_from_skipped)
        if updates.head(1) == []:
            return
        groups_delta = DeltaTable.forName(self.spark, self.config.groups_table)
        groups_delta.alias("t").merge(
            updates.alias("s"),
            "t.group_key = s.group_key",
        ).whenMatchedUpdate(
            set={
                "status": "'DISPATCHED'",
                "triggered_at": "COALESCE(t.triggered_at, s.dispatched_at)",
                "updated_at": "s.dispatched_at",
            }
        ).execute()

    def _audit_dispatch(self, dispatched_df: DataFrame, skipped_df: DataFrame) -> None:
        events: DataFrame | None = None

        if dispatched_df.head(1) != []:
            dispatched_events = dispatched_df.select(
                F.lit("GROUP_DISPATCHED").alias("event_type"),
                "group_key",
                F.lit(None).cast("string").alias("file_business_key"),
                F.to_json(
                    F.struct(
                        "dispatch_key",
                        "dispatch_reason",
                        "expected_parts",
                        "received_parts",
                        "incomplete_flag",
                        "dispatched_at",
                        "files_snapshot_json",
                    )
                ).alias("payload_json"),
                F.lit("INFO").alias("severity"),
                F.col("dispatch_key").alias("dedupe_key"),
            )
            events = dispatched_events

        if skipped_df.head(1) != []:
            skipped_events = skipped_df.select(
                F.lit("GROUP_DISPATCH_SKIPPED_ALREADY_DONE").alias("event_type"),
                "group_key",
                F.lit(None).cast("string").alias("file_business_key"),
                F.to_json(
                    F.struct(
                        "dispatch_key",
                        "ready_reason",
                        "expected_parts",
                        "received_parts",
                        "dispatched_at",
                    )
                ).alias("payload_json"),
                F.lit("INFO").alias("severity"),
                F.concat_ws("|", F.col("dispatch_key"), F.lit("already_done")).alias("dedupe_key"),
            )
            events = skipped_events if events is None else events.unionByName(skipped_events)

        if events is not None:
            self.audit_service.log_dataframe(events)
