# Databricks notebook source
# MAGIC %run ./00_free_edition_setup

# COMMAND ----------
from ingestion_orchestrator.config import OrchestratorConfig
from ingestion_orchestrator.jobs.dispatch_groups import run_dispatch_job
from ingestion_orchestrator.jobs.evaluate_groups import run_evaluate_job
from ingestion_orchestrator.jobs.ingest_landing import run_ingest_job
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
config = OrchestratorConfig.from_env()

# COMMAND ----------
# 1) Ingest files from landing using Auto Loader (availableNow) or batch mode.
ingest_summary = run_ingest_job(
    spark,
    config,
    mode="auto_loader" if config.auto_loader_enabled else "batch",
)

# COMMAND ----------
# 2) Rebuild groups and evaluate readiness.
evaluate_summary = run_evaluate_job(spark, config)

# COMMAND ----------
# 3) Dispatch READY groups (idempotent merge by dispatch_key).
dispatch_summary = run_dispatch_job(spark, config)

# COMMAND ----------
display(
    {
        "ingest_summary": ingest_summary,
        "evaluate_summary": evaluate_summary,
        "dispatch_summary": dispatch_summary,
    }
)

# COMMAND ----------
display(spark.table(config.groups_table).orderBy("group_key"))

# COMMAND ----------
display(spark.table(config.dispatch_table).orderBy("group_key"))

# COMMAND ----------
display(
    spark.table(config.audit_table)
    .orderBy("event_ts")
    .select("event_ts", "event_type", "group_key", "severity", "payload_json")
)
