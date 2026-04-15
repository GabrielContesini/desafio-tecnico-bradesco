from __future__ import annotations

import os
from dataclasses import dataclass, field


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "y", "yes", "on"}


@dataclass(frozen=True)
class TableNames:
    file_events: str = "ops_ingestion_file_events"
    groups: str = "ops_ingestion_groups"
    dispatch: str = "ops_ingestion_dispatch"
    audit: str = "ops_ingestion_audit"


@dataclass(frozen=True)
class PathConfig:
    landing: str
    checkpoints_root: str

    @property
    def ingest_checkpoint(self) -> str:
        return f"{self.checkpoints_root.rstrip('/')}/ingest_landing"

    @property
    def evaluate_checkpoint(self) -> str:
        return f"{self.checkpoints_root.rstrip('/')}/evaluate_groups"

    @property
    def dispatch_checkpoint(self) -> str:
        return f"{self.checkpoints_root.rstrip('/')}/dispatch_groups"


@dataclass(frozen=True)
class OrchestratorConfig:
    env: str
    catalog: str
    schema: str
    timezone: str
    group_timeout_minutes: int
    paths: PathConfig
    tables: TableNames = field(default_factory=TableNames)
    auto_loader_enabled: bool = True
    auto_loader_options: dict[str, str] = field(default_factory=dict)
    enable_file_checksum: bool = False
    evaluate_batch_limit: int = 10_000
    dispatch_batch_limit: int = 10_000

    @property
    def schema_fqn(self) -> str:
        if self.catalog:
            return f"{self.catalog}.{self.schema}"
        return self.schema

    def table_fqn(self, table_name: str) -> str:
        return f"{self.schema_fqn}.{table_name}"

    @property
    def file_events_table(self) -> str:
        return self.table_fqn(self.tables.file_events)

    @property
    def groups_table(self) -> str:
        return self.table_fqn(self.tables.groups)

    @property
    def dispatch_table(self) -> str:
        return self.table_fqn(self.tables.dispatch)

    @property
    def audit_table(self) -> str:
        return self.table_fqn(self.tables.audit)

    @classmethod
    def from_env(cls, env: str | None = None) -> "OrchestratorConfig":
        resolved_env = env or os.getenv("INGEST_ENV") or "dev"
        catalog = os.getenv("INGEST_CATALOG") or "main"
        schema = os.getenv("INGEST_SCHEMA") or f"ops_ingestion_{resolved_env}"
        timezone = os.getenv("INGEST_TIMEZONE") or "UTC"
        timeout_minutes = int(os.getenv("INGEST_GROUP_TIMEOUT_MINUTES", "30"))
        landing_path = os.getenv(
            "INGEST_LANDING_PATH",
            f"/Volumes/{catalog}/{schema}/landing",
        )
        checkpoints_root = os.getenv(
            "INGEST_CHECKPOINTS_ROOT",
            f"/Volumes/{catalog}/{schema}/checkpoints",
        )
        auto_loader_enabled = _as_bool(os.getenv("INGEST_AUTO_LOADER_ENABLED"), True)
        enable_file_checksum = _as_bool(os.getenv("INGEST_ENABLE_FILE_CHECKSUM"), False)
        evaluate_batch_limit = int(os.getenv("INGEST_EVALUATE_BATCH_LIMIT", "10000"))
        dispatch_batch_limit = int(os.getenv("INGEST_DISPATCH_BATCH_LIMIT", "10000"))

        options: dict[str, str] = {
            "cloudFiles.format": os.getenv("INGEST_CLOUD_FILES_FORMAT", "binaryFile"),
            "cloudFiles.includeExistingFiles": os.getenv("INGEST_INCLUDE_EXISTING", "true"),
        }
        schema_location = os.getenv("INGEST_CLOUD_FILES_SCHEMA_LOCATION") or (
            f"{checkpoints_root.rstrip('/')}/schemas/ingest_landing"
        )
        options["cloudFiles.schemaLocation"] = schema_location

        return cls(
            env=resolved_env,
            catalog=catalog,
            schema=schema,
            timezone=timezone,
            group_timeout_minutes=timeout_minutes,
            paths=PathConfig(landing=landing_path, checkpoints_root=checkpoints_root),
            auto_loader_enabled=auto_loader_enabled,
            auto_loader_options=options,
            enable_file_checksum=enable_file_checksum,
            evaluate_batch_limit=evaluate_batch_limit,
            dispatch_batch_limit=dispatch_batch_limit,
        )
