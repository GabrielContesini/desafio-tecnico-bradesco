# Databricks notebook source
# MAGIC %run ./00_free_edition_setup

# COMMAND ----------
from ingestion_orchestrator.jobs.dispatch_groups import run_dispatch_job
from ingestion_orchestrator.config import OrchestratorConfig
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
config = OrchestratorConfig.from_env()

# COMMAND ----------
summary = run_dispatch_job(spark, config)
display(summary)
