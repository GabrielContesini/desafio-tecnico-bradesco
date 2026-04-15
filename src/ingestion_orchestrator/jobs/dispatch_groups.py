from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from ingestion_orchestrator.audit import AuditService
from ingestion_orchestrator.config import OrchestratorConfig
from ingestion_orchestrator.logging_utils import get_logger
from ingestion_orchestrator.services.dispatch_service import DispatchService
from ingestion_orchestrator.utils import ensure_control_tables


logger = get_logger(__name__)


def run_dispatch_job(spark: SparkSession, config: OrchestratorConfig) -> dict[str, int]:
    ensure_control_tables(spark, config)
    audit_service = AuditService(spark, config)
    dispatch_service = DispatchService(spark, config, audit_service)
    result = dispatch_service.dispatch_ready_groups(limit=config.dispatch_batch_limit)

    summary = {
        "ready_groups": result.ready_groups,
        "dispatched": result.dispatched,
        "skipped_already_dispatched": result.skipped_already_dispatched,
    }
    logger.info("dispatch_job_finished", extra={"payload": summary})
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch ready ingestion groups.")
    parser.add_argument("--env", default=None, help="Environment name (dev/qa/prod).")
    args = parser.parse_args()

    config = OrchestratorConfig.from_env(args.env)
    spark = SparkSession.builder.appName("ingestion_orchestrator_dispatch_groups").getOrCreate()
    run_dispatch_job(spark, config)


if __name__ == "__main__":
    main()
