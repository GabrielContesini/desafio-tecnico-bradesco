# Databricks notebook source
# MAGIC %run ./00_free_edition_setup

# COMMAND ----------
from ingestion_orchestrator.jobs.evaluate_groups import run_evaluate_job
from ingestion_orchestrator.config import OrchestratorConfig
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
config = OrchestratorConfig.from_env()

# COMMAND ----------
summary = run_evaluate_job(spark, config)
display(summary)
