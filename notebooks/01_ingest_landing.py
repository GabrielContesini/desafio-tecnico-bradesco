# Databricks notebook source
# MAGIC %run ./00_free_edition_setup

# COMMAND ----------
from ingestion_orchestrator.jobs.ingest_landing import run_ingest_job
from ingestion_orchestrator.config import OrchestratorConfig
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
config = OrchestratorConfig.from_env()

# COMMAND ----------
summary = run_ingest_job(spark, config, mode="auto_loader" if config.auto_loader_enabled else "batch")
display(summary)
