# Databricks notebook source
# MAGIC %run ./00_free_edition_setup

# COMMAND ----------
from pathlib import Path

from ingestion_orchestrator.config import OrchestratorConfig
from ingestion_orchestrator.jobs.simulate_landing import create_demo_files


config = OrchestratorConfig.from_env()
create_demo_files(Path(config.paths.landing))

display({"demo_files_written_to": config.paths.landing})
