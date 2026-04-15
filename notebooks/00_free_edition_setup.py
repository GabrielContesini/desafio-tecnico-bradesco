# Databricks notebook source
import os
import sys

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()


def _set_default_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        if value.strip() == "":
            return default
        return value
    except Exception:
        return default


def _infer_project_root() -> str:
    try:
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        )
        if "/notebooks/" in notebook_path:
            return f"/Workspace{notebook_path.split('/notebooks/')[0]}"
    except Exception:
        pass
    return "/Workspace/Users/<your-email>/teste-tecnico-bradesco"


_set_default_widget("env", "dev")
_set_default_widget("catalog", "main")
_set_default_widget("schema", "ops_ingestion_dev")
_set_default_widget("group_timeout_minutes", "30")
_set_default_widget("project_root", "")

env = _get_widget("env", "dev")
catalog = _get_widget("catalog", "main")
schema = _get_widget("schema", "ops_ingestion_dev")
group_timeout_minutes = _get_widget("group_timeout_minutes", "30")
project_root = _get_widget("project_root", _infer_project_root())

src_path = f"{project_root.rstrip('/')}/src"
if src_path not in sys.path:
    sys.path.insert(0, src_path)

landing_path = f"/Volumes/{catalog}/{schema}/landing"
checkpoints_root = f"/Volumes/{catalog}/{schema}/checkpoints"

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`landing`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`checkpoints`")

for path in [landing_path, checkpoints_root]:
    dbutils.fs.mkdirs(path)

os.environ["INGEST_ENV"] = env
os.environ["INGEST_CATALOG"] = catalog
os.environ["INGEST_SCHEMA"] = schema
os.environ["INGEST_LANDING_PATH"] = landing_path
os.environ["INGEST_CHECKPOINTS_ROOT"] = checkpoints_root
os.environ["INGEST_GROUP_TIMEOUT_MINUTES"] = group_timeout_minutes
os.environ["INGEST_AUTO_LOADER_ENABLED"] = "true"
os.environ["INGEST_CLOUD_FILES_FORMAT"] = "binaryFile"
os.environ["INGEST_INCLUDE_EXISTING"] = "true"
os.environ["INGEST_CLOUD_FILES_SCHEMA_LOCATION"] = (
    f"{checkpoints_root.rstrip('/')}/schemas/ingest_landing"
)
os.environ["INGEST_TIMEZONE"] = "UTC"

display(
    {
        "env": env,
        "catalog": catalog,
        "schema": schema,
        "project_root": project_root,
        "src_path": src_path,
        "landing_path": landing_path,
        "checkpoints_root": checkpoints_root,
    }
)
