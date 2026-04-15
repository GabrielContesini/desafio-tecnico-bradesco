from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from ingestion_orchestrator.audit import AuditService
from ingestion_orchestrator.config import OrchestratorConfig


@dataclass(frozen=True)
class GroupRefreshResult:
    groups_seen: int
    groups_created: int
    groups_updated: int
    manifest_conflicts: int


@dataclass(frozen=True)
class GroupEvaluateResult:
    ready_groups: int
    ready_all_parts: int
    ready_timeout: int


def _union_or_none(first: DataFrame | None, second: DataFrame | None) -> DataFrame | None:
    if first is None:
        return second
    if second is None:
        return first
    return first.unionByName(second)


class GroupsService:
    def __init__(self, spark: SparkSession, config: OrchestratorConfig, audit_service: AuditService) -> None:
        self.spark = spark
        self.config = config
        self.audit_service = audit_service

    def refresh_group_state(self) -> GroupRefreshResult:
        events = self.spark.table(self.config.file_events_table).where(
            F.col("parse_error").isNull() & F.col("group_key").isNotNull()
        )
        if events.head(1) == []:
            return GroupRefreshResult(0, 0, 0, 0)

        aggregated = self._aggregate_group_state(events)
        existing_before = self.spark.table(self.config.groups_table).select(
            "group_key",
            "expected_parts",
            "received_parts",
            "first_seen_at",
            "last_seen_at",
        )

        joined_before = aggregated.alias("s").join(existing_before.alias("e"), "group_key", "left")
        new_groups = joined_before.where(F.col("e.group_key").isNull()).select("s.*")
        changed_groups = joined_before.where(
            F.col("e.group_key").isNotNull()
            & (
                (F.col("s.received_parts") != F.col("e.received_parts"))
                | (F.col("s.last_seen_at") != F.col("e.last_seen_at"))
                | (
                    F.col("e.expected_parts").isNull()
                    & F.col("s.manifest_expected_parts").isNotNull()
                )
            )
        ).select("s.*")

        group_delta = DeltaTable.forName(self.spark, self.config.groups_table)
        group_delta.alias("t").merge(
            aggregated.alias("s"),
            "t.group_key = s.group_key",
        ).whenMatchedUpdate(
            set={
                "system_name": "s.system_name",
                "table_name": "s.table_name",
                "dt_ref": "s.dt_ref",
                "expected_parts": "COALESCE(t.expected_parts, s.manifest_expected_parts)",
                "received_parts": "s.received_parts",
                "first_seen_at": "s.first_seen_at",
                "last_seen_at": "s.last_seen_at",
                "expires_at": "s.expires_at",
                "status": (
                    "CASE WHEN t.status IN ('READY', 'DISPATCHED') "
                    "THEN t.status ELSE 'OPEN' END"
                ),
                "ready_reason": "t.ready_reason",
                "triggered_at": "t.triggered_at",
                "incomplete_flag": "t.incomplete_flag",
                "last_evaluated_at": "t.last_evaluated_at",
                "created_at": "t.created_at",
                "updated_at": "s.processed_at",
            }
        ).whenNotMatchedInsert(
            values={
                "group_key": "s.group_key",
                "system_name": "s.system_name",
                "table_name": "s.table_name",
                "dt_ref": "s.dt_ref",
                "expected_parts": "s.manifest_expected_parts",
                "received_parts": "s.received_parts",
                "status": "'OPEN'",
                "ready_reason": "NULL",
                "first_seen_at": "s.first_seen_at",
                "last_seen_at": "s.last_seen_at",
                "expires_at": "s.expires_at",
                "triggered_at": "NULL",
                "incomplete_flag": "false",
                "last_evaluated_at": "NULL",
                "created_at": "s.processed_at",
                "updated_at": "s.processed_at",
            }
        ).execute()

        current_groups = self.spark.table(self.config.groups_table).select("group_key", "expected_parts")
        conflicts = aggregated.alias("s").join(current_groups.alias("g"), "group_key", "inner").where(
            (F.col("s.manifest_distinct_count") > 1)
            | (
                F.col("s.manifest_expected_parts").isNotNull()
                & F.col("g.expected_parts").isNotNull()
                & (F.col("s.manifest_expected_parts") != F.col("g.expected_parts"))
            )
        )

        self._audit_group_refresh(new_groups, changed_groups, conflicts)

        return GroupRefreshResult(
            groups_seen=aggregated.count(),
            groups_created=new_groups.count(),
            groups_updated=changed_groups.count(),
            manifest_conflicts=conflicts.count(),
        )

    def evaluate_ready_groups(self, now_ts: datetime | None = None) -> GroupEvaluateResult:
        open_groups = self.spark.table(self.config.groups_table).where(F.col("status") == "OPEN")
        if open_groups.head(1) == []:
            return GroupEvaluateResult(0, 0, 0)

        now_literal = F.current_timestamp() if now_ts is None else F.lit(now_ts).cast("timestamp")
        ready_candidates = (
            open_groups.withColumn("evaluated_at", now_literal)
            .withColumn(
                "ready_reason_candidate",
                F.when(
                    F.col("expected_parts").isNotNull()
                    & (F.col("received_parts") >= F.col("expected_parts")),
                    F.lit("ALL_PARTS"),
                ).when(F.col("evaluated_at") >= F.col("expires_at"), F.lit("TIMEOUT")),
            )
            .where(F.col("ready_reason_candidate").isNotNull())
            .withColumn(
                "incomplete_flag_calc",
                F.when(
                    (F.col("ready_reason_candidate") == "TIMEOUT")
                    & F.col("expected_parts").isNotNull()
                    & (F.col("received_parts") < F.col("expected_parts")),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
        )
        if ready_candidates.head(1) == []:
            return GroupEvaluateResult(0, 0, 0)

        groups_delta = DeltaTable.forName(self.spark, self.config.groups_table)
        groups_delta.alias("t").merge(
            ready_candidates.alias("s"),
            "t.group_key = s.group_key AND t.status = 'OPEN'",
        ).whenMatchedUpdate(
            set={
                "status": "'READY'",
                "ready_reason": "s.ready_reason_candidate",
                "incomplete_flag": "s.incomplete_flag_calc",
                "last_evaluated_at": "s.evaluated_at",
                "updated_at": "s.evaluated_at",
            }
        ).execute()

        audit_events = ready_candidates.select(
            F.when(
                F.col("ready_reason_candidate") == "ALL_PARTS",
                F.lit("GROUP_READY_ALL_PARTS"),
            )
            .otherwise(F.lit("GROUP_READY_TIMEOUT"))
            .alias("event_type"),
            "group_key",
            F.lit(None).cast("string").alias("file_business_key"),
            F.to_json(
                F.struct(
                    "expected_parts",
                    "received_parts",
                    "expires_at",
                    "ready_reason_candidate",
                    "incomplete_flag_calc",
                    "evaluated_at",
                )
            ).alias("payload_json"),
            F.when(
                (F.col("ready_reason_candidate") == "TIMEOUT") & F.col("expected_parts").isNull(),
                F.lit("WARN"),
            )
            .otherwise(F.lit("INFO"))
            .alias("severity"),
            F.concat_ws("|", F.col("group_key"), F.col("ready_reason_candidate")).alias("dedupe_key"),
        )
        self.audit_service.log_dataframe(audit_events)

        ready_groups = ready_candidates.count()
        ready_all_parts = ready_candidates.where(F.col("ready_reason_candidate") == "ALL_PARTS").count()
        ready_timeout = ready_groups - ready_all_parts
        return GroupEvaluateResult(
            ready_groups=ready_groups,
            ready_all_parts=ready_all_parts,
            ready_timeout=ready_timeout,
        )

    def _aggregate_group_state(self, events: DataFrame) -> DataFrame:
        base = events.groupBy("group_key", "system_name", "table_name", "dt_ref").agg(
            F.min("arrived_at").alias("first_seen_at"),
            F.max("arrived_at").alias("last_seen_at"),
            F.countDistinct(F.when(~F.col("is_manifest"), F.col("part_number"))).alias("received_parts"),
        )

        manifest_events = events.where(F.col("is_manifest") & F.col("expected_parts").isNotNull())
        window_spec = Window.partitionBy("group_key").orderBy(
            F.col("arrived_at").asc(), F.col("file_business_key").asc()
        )
        first_manifest = (
            manifest_events.withColumn("rn", F.row_number().over(window_spec))
            .where(F.col("rn") == 1)
            .select("group_key", F.col("expected_parts").alias("manifest_expected_parts"))
        )
        manifest_stats = manifest_events.groupBy("group_key").agg(
            F.countDistinct("expected_parts").alias("manifest_distinct_count"),
            F.collect_set("expected_parts").alias("manifest_expected_values"),
        )

        aggregated = (
            base.join(first_manifest, "group_key", "left")
            .join(manifest_stats, "group_key", "left")
            .withColumn("manifest_distinct_count", F.coalesce(F.col("manifest_distinct_count"), F.lit(0)))
            .withColumn(
                "expires_at",
                F.expr(f"first_seen_at + INTERVAL {self.config.group_timeout_minutes} MINUTES"),
            )
            .withColumn("processed_at", F.current_timestamp())
        )
        return aggregated

    def _audit_group_refresh(
        self,
        new_groups: DataFrame,
        changed_groups: DataFrame,
        conflicts: DataFrame,
    ) -> None:
        events: DataFrame | None = None

        if new_groups.head(1) != []:
            created_events = new_groups.select(
                F.lit("GROUP_CREATED").alias("event_type"),
                "group_key",
                F.lit(None).cast("string").alias("file_business_key"),
                F.to_json(
                    F.struct(
                        "system_name",
                        "table_name",
                        "dt_ref",
                        "first_seen_at",
                        "last_seen_at",
                        "received_parts",
                        "manifest_expected_parts",
                    )
                ).alias("payload_json"),
                F.lit("INFO").alias("severity"),
                F.concat_ws("|", F.col("group_key"), F.col("first_seen_at").cast("string")).alias("dedupe_key"),
            )
            events = _union_or_none(events, created_events)

        if changed_groups.head(1) != []:
            updated_events = changed_groups.select(
                F.lit("GROUP_UPDATED").alias("event_type"),
                "group_key",
                F.lit(None).cast("string").alias("file_business_key"),
                F.to_json(
                    F.struct(
                        "received_parts",
                        "manifest_expected_parts",
                        "first_seen_at",
                        "last_seen_at",
                        "expires_at",
                    )
                ).alias("payload_json"),
                F.lit("INFO").alias("severity"),
                F.concat_ws(
                    "|",
                    F.col("group_key"),
                    F.col("received_parts").cast("string"),
                    F.col("last_seen_at").cast("string"),
                ).alias("dedupe_key"),
            )
            events = _union_or_none(events, updated_events)

        if conflicts.head(1) != []:
            conflict_events = conflicts.select(
                F.lit("MANIFEST_CONFLICT").alias("event_type"),
                "group_key",
                F.lit(None).cast("string").alias("file_business_key"),
                F.to_json(
                    F.struct(
                        F.col("g.expected_parts").alias("group_expected_parts"),
                        "manifest_expected_parts",
                        "manifest_expected_values",
                        "manifest_distinct_count",
                    )
                ).alias("payload_json"),
                F.lit("WARN").alias("severity"),
                F.concat_ws(
                    "|",
                    F.col("group_key"),
                    F.col("manifest_distinct_count").cast("string"),
                    F.to_json(F.col("manifest_expected_values")),
                ).alias("dedupe_key"),
            )
            events = _union_or_none(events, conflict_events)

        if events is not None:
            self.audit_service.log_dataframe(events)
