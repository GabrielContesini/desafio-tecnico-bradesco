from __future__ import annotations

import hashlib

from pyspark.sql import DataFrame

from ingestion_orchestrator.parsing import build_group_key


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def make_file_business_key(
    *,
    system_name: str | None,
    table_name: str | None,
    dt_ref: str | None,
    is_manifest: bool,
    part_number: int | None,
    expected_parts: int | None,
    file_name: str,
    parse_error: str | None = None,
) -> str:
    if parse_error:
        return sha256_hex(f"PARSE_ERROR|{file_name}")
    group_key = build_group_key(system_name or "", table_name or "", dt_ref or "")
    if is_manifest:
        manifest_value = str(expected_parts) if expected_parts is not None else "UNKNOWN"
        return sha256_hex(f"{group_key}|MANIFEST|{manifest_value}")
    if part_number is None:
        return sha256_hex(f"{group_key}|PART|MISSING")
    return sha256_hex(f"{group_key}|PART|{part_number:010d}")


def make_dispatch_key(group_key: str) -> str:
    return sha256_hex(group_key)


def make_audit_key(event_type: str, group_key: str | None, file_business_key: str | None, dedupe: str) -> str:
    return sha256_hex(f"{event_type}|{group_key or '-'}|{file_business_key or '-'}|{dedupe}")


def spark_file_business_key_expr(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    parse_error_key = F.sha2(
        F.concat_ws("|", F.lit("PARSE_ERROR"), F.coalesce(F.col("file_name"), F.col("file_path"))),
        256,
    )
    manifest_key = F.sha2(
        F.concat_ws(
            "|",
            F.col("group_key"),
            F.lit("MANIFEST"),
            F.coalesce(F.col("expected_parts").cast("string"), F.lit("UNKNOWN")),
        ),
        256,
    )
    part_key = F.sha2(
        F.concat_ws(
            "|",
            F.col("group_key"),
            F.lit("PART"),
            F.lpad(F.col("part_number").cast("string"), 10, "0"),
        ),
        256,
    )
    return (
        df.withColumn(
            "file_business_key",
            F.when(F.col("parse_error").isNotNull(), parse_error_key)
            .when(F.col("is_manifest"), manifest_key)
            .otherwise(part_key),
        )
        .withColumn("file_event_id", F.sha2(F.col("file_business_key"), 256))
        .withColumn("is_duplicate", F.lit(False))
    )


def spark_dispatch_key_expr(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return df.withColumn("dispatch_key", F.sha2(F.col("group_key"), 256))
