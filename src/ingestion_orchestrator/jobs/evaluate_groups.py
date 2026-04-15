from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from ingestion_orchestrator.audit import AuditService
from ingestion_orchestrator.config import OrchestratorConfig
from ingestion_orchestrator.logging_utils import get_logger
from ingestion_orchestrator.services.groups_service import GroupsService
from ingestion_orchestrator.utils import ensure_control_tables


logger = get_logger(__name__)


def run_evaluate_job(spark: SparkSession, config: OrchestratorConfig) -> dict[str, int]:
    ensure_control_tables(spark, config)
    audit_service = AuditService(spark, config)
    group_service = GroupsService(spark, config, audit_service)

    refresh = group_service.refresh_group_state()
    ready = group_service.evaluate_ready_groups()

    summary = {
        "groups_seen": refresh.groups_seen,
        "groups_created": refresh.groups_created,
        "groups_updated": refresh.groups_updated,
        "manifest_conflicts": refresh.manifest_conflicts,
        "ready_groups": ready.ready_groups,
        "ready_all_parts": ready.ready_all_parts,
        "ready_timeout": ready.ready_timeout,
    }
    logger.info("evaluate_job_finished", extra={"payload": summary})
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate ingestion groups readiness.")
    parser.add_argument("--env", default=None, help="Environment name (dev/qa/prod).")
    args = parser.parse_args()

    config = OrchestratorConfig.from_env(args.env)
    spark = SparkSession.builder.appName("ingestion_orchestrator_evaluate_groups").getOrCreate()
    run_evaluate_job(spark, config)


if __name__ == "__main__":
    main()
