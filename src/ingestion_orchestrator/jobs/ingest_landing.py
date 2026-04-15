from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from ingestion_orchestrator.audit import AuditService
from ingestion_orchestrator.config import OrchestratorConfig
from ingestion_orchestrator.logging_utils import get_logger
from ingestion_orchestrator.services.files_service import FileEventsService, FileRegistrationResult
from ingestion_orchestrator.utils import ensure_control_tables


logger = get_logger(__name__)


def _append_result(accumulator: dict[str, int], result: FileRegistrationResult) -> None:
    accumulator["raw_events"] += result.raw_events
    accumulator["unique_candidates"] += result.unique_candidates
    accumulator["inserted"] += result.inserted
    accumulator["duplicates"] += result.duplicates
    accumulator["parse_errors"] += result.parse_errors


def run_ingest_job(spark: SparkSession, config: OrchestratorConfig, mode: str) -> dict[str, int]:
    ensure_control_tables(spark, config)
    audit_service = AuditService(spark, config)
    file_service = FileEventsService(spark, config, audit_service)

    summary: dict[str, int] = {
        "raw_events": 0,
        "unique_candidates": 0,
        "inserted": 0,
        "duplicates": 0,
        "parse_errors": 0,
    }

    if mode == "auto_loader":
        reader = spark.readStream.format("cloudFiles")
        for key, value in config.auto_loader_options.items():
            reader = reader.option(key, value)
        stream_df = reader.load(config.paths.landing)

        def process_batch(batch_df, batch_id: int) -> None:  # type: ignore[no-untyped-def]
            prepared = file_service.build_file_events_dataframe(batch_df)
            result = file_service.register_batch(prepared)
            _append_result(summary, result)
            logger.info("ingest_batch_processed", extra={"payload": {"batch_id": batch_id, **result.__dict__}})

        query = (
            stream_df.writeStream.trigger(availableNow=True)
            .option("checkpointLocation", config.paths.ingest_checkpoint)
            .foreachBatch(process_batch)
            .start()
        )
        query.awaitTermination()
        return summary

    try:
        raw_df = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(config.paths.landing)
    except AnalysisException:
        logger.info("ingest_landing_path_not_found", extra={"payload": {"path": config.paths.landing}})
        return summary

    prepared = file_service.build_file_events_dataframe(raw_df)
    result = file_service.register_batch(prepared)
    _append_result(summary, result)
    logger.info("ingest_batch_processed", extra={"payload": result.__dict__})
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest landing files into Delta control table.")
    parser.add_argument("--env", default=None, help="Environment name (dev/qa/prod).")
    parser.add_argument(
        "--mode",
        choices=["auto_loader", "batch"],
        default=None,
        help="Execution mode. Default uses auto_loader when enabled.",
    )
    args = parser.parse_args()

    config = OrchestratorConfig.from_env(args.env)
    mode = args.mode or ("auto_loader" if config.auto_loader_enabled else "batch")
    spark = SparkSession.builder.appName("ingestion_orchestrator_ingest_landing").getOrCreate()
    summary = run_ingest_job(spark, config, mode=mode)
    logger.info("ingest_job_finished", extra={"payload": summary})


if __name__ == "__main__":
    main()
