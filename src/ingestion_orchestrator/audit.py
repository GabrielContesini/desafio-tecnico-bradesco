from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion_orchestrator.config import OrchestratorConfig


@dataclass(frozen=True)
class AuditWriteResult:
    written_events: int


class AuditService:
    def __init__(self, spark: SparkSession, config: OrchestratorConfig) -> None:
        self.spark = spark
        self.config = config

    def log_dataframe(self, events_df: DataFrame) -> AuditWriteResult:
        """
        Writes audit events idempotently.

        Expected columns:
          - event_type (string)
          - group_key (string, nullable)
          - file_business_key (string, nullable)
          - payload_json (string, nullable)
          - severity (string, nullable)
          - dedupe_key (string, nullable)
        """
        if events_df.head(1) == []:
            return AuditWriteResult(written_events=0)

        prepared = (
            events_df.withColumn("severity", F.coalesce(F.col("severity"), F.lit("INFO")))
            .withColumn("event_ts", F.current_timestamp())
            .withColumn(
                "audit_id",
                F.sha2(
                    F.concat_ws(
                        "|",
                        F.col("event_type"),
                        F.coalesce(F.col("group_key"), F.lit("-")),
                        F.coalesce(F.col("file_business_key"), F.lit("-")),
                        F.coalesce(F.col("dedupe_key"), F.lit("-")),
                    ),
                    256,
                ),
            )
            .select(
                "audit_id",
                "event_type",
                "group_key",
                "file_business_key",
                "event_ts",
                "payload_json",
                "severity",
            )
        )

        delta_table = DeltaTable.forName(self.spark, self.config.audit_table)
        delta_table.alias("t").merge(
            prepared.alias("s"),
            "t.audit_id = s.audit_id",
        ).whenNotMatchedInsertAll().execute()

        return AuditWriteResult(written_events=prepared.count())


def payload_as_json(**kwargs: Any) -> str:
    import json

    return json.dumps(kwargs, default=str, ensure_ascii=True, sort_keys=True)
