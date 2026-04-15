from __future__ import annotations

from typing import Iterable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from ingestion_orchestrator.config import OrchestratorConfig
from ingestion_orchestrator.schemas import audit_schema, dispatch_schema, file_events_schema, groups_schema


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(table_name))
    except Exception:
        return False


def ensure_catalog_schema(spark: SparkSession, config: OrchestratorConfig) -> None:
    if config.catalog:
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.catalog}")
        except Exception:
            # Local Spark environments may not support Unity Catalog statements.
            pass
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema_fqn}")


def create_empty_delta_table(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    partition_by: Iterable[str] | None = None,
) -> None:
    if _table_exists(spark, table_name):
        return
    writer = spark.createDataFrame([], schema).write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_name)


def ensure_control_tables(spark: SparkSession, config: OrchestratorConfig) -> None:
    ensure_catalog_schema(spark, config)
    create_empty_delta_table(
        spark=spark,
        table_name=config.file_events_table,
        schema=file_events_schema(),
        partition_by=["dt_ref"],
    )
    create_empty_delta_table(
        spark=spark,
        table_name=config.groups_table,
        schema=groups_schema(),
        partition_by=["dt_ref"],
    )
    create_empty_delta_table(
        spark=spark,
        table_name=config.dispatch_table,
        schema=dispatch_schema(),
        partition_by=["dt_ref"],
    )
    create_empty_delta_table(
        spark=spark,
        table_name=config.audit_table,
        schema=audit_schema(),
        partition_by=None,
    )
