from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import unquote


FILE_NAME_REGEX = (
    r"^SISTEMA=([^|]+)\|TABELA=([^|]+)\|DT_REF=(\d{4}-\d{2}-\d{2})\|"
    r"(PART=(\d+)\.json|MANIFEST\.json)$"
)
_COMPILED_FILE_NAME_REGEX = re.compile(FILE_NAME_REGEX)


class ParseFileNameError(ValueError):
    """Raised when a file name does not follow the expected pattern."""


@dataclass(frozen=True)
class ParsedFileName:
    system_name: str
    table_name: str
    dt_ref: str
    part_number: int | None
    is_manifest: bool

    @property
    def group_key(self) -> str:
        return build_group_key(self.system_name, self.table_name, self.dt_ref)


def build_group_key(system_name: str, table_name: str, dt_ref: str) -> str:
    return f"{system_name}|{table_name}|{dt_ref}"


def parse_file_name(file_name: str) -> ParsedFileName:
    decoded_file_name = unquote(file_name)
    match = _COMPILED_FILE_NAME_REGEX.match(decoded_file_name)
    if not match:
        raise ParseFileNameError(f"Invalid file name pattern: {file_name}")
    system_name = match.group(1)
    table_name = match.group(2)
    dt_ref = match.group(3)
    try:
        datetime.strptime(dt_ref, "%Y-%m-%d")
    except ValueError as exc:
        raise ParseFileNameError(f"Invalid DT_REF in file name: {file_name}") from exc

    is_manifest = "MANIFEST.json" in decoded_file_name
    part_number = None
    if not is_manifest:
        part_raw = match.group(5)
        if part_raw is None:
            raise ParseFileNameError(f"Missing PART value in file name: {decoded_file_name}")
        part_number = int(part_raw)

    return ParsedFileName(
        system_name=system_name,
        table_name=table_name,
        dt_ref=dt_ref,
        part_number=part_number,
        is_manifest=is_manifest,
    )


def parse_manifest_payload(payload: bytes | str | None) -> tuple[int | None, str | None]:
    if payload is None:
        return None, "EMPTY_MANIFEST_PAYLOAD"
    raw_text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None, "INVALID_MANIFEST_JSON"
    expected = parsed.get("expected_parts")
    if expected is None:
        return None, "MISSING_EXPECTED_PARTS"
    if not isinstance(expected, int) or expected <= 0:
        return None, "INVALID_EXPECTED_PARTS"
    return expected, None


def with_parsed_metadata(raw_df: Any, timezone: str = "UTC") -> Any:
    """Attach parsed columns to a Spark DataFrame produced by binaryFile/Auto Loader."""

    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    manifest_schema = T.StructType([T.StructField("expected_parts", T.IntegerType(), True)])
    file_name_col = F.regexp_extract(F.col("path"), r"([^/\\]+)$", 1)

    parsed_df = (
        raw_df.withColumn("file_path", F.col("path"))
        .withColumn("file_name", file_name_col)
            .withColumn("logical_file_name", F.expr("url_decode(file_name)"))
            .withColumn("system_name", F.regexp_extract(F.col("logical_file_name"), FILE_NAME_REGEX, 1))
            .withColumn("table_name", F.regexp_extract(F.col("logical_file_name"), FILE_NAME_REGEX, 2))
            .withColumn("dt_ref_str", F.regexp_extract(F.col("logical_file_name"), FILE_NAME_REGEX, 3))
            .withColumn("part_number_str", F.regexp_extract(F.col("logical_file_name"), FILE_NAME_REGEX, 5))
            .withColumn("is_manifest", F.col("logical_file_name").rlike(r"MANIFEST\.json$"))
        .withColumn("dt_ref", F.to_date(F.col("dt_ref_str"), "yyyy-MM-dd"))
        .withColumn(
            "part_number",
            F.when(~F.col("is_manifest"), F.col("part_number_str").cast("int")).otherwise(F.lit(None)),
        )
        .withColumn("manifest_json", F.from_json(F.col("content").cast("string"), manifest_schema))
        .withColumn(
            "expected_parts",
            F.when(F.col("is_manifest"), F.col("manifest_json.expected_parts").cast("int")).otherwise(
                F.lit(None)
            ),
        )
        .withColumn(
            "group_key",
            F.when(
                (F.col("system_name") != "")
                & (F.col("table_name") != "")
                & F.col("dt_ref").isNotNull(),
                F.concat_ws("|", F.col("system_name"), F.col("table_name"), F.col("dt_ref_str")),
            ),
        )
        .withColumn(
            "arrived_at",
            F.to_utc_timestamp(F.current_timestamp(), timezone),
        )
        .withColumn("ingested_at", F.to_utc_timestamp(F.current_timestamp(), timezone))
        .withColumn(
            "parse_error",
            F.when(~F.col("logical_file_name").rlike(FILE_NAME_REGEX), F.lit("INVALID_FILENAME_PATTERN"))
            .when(F.col("dt_ref").isNull(), F.lit("INVALID_DT_REF"))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "raw_metadata_json",
            F.to_json(
                F.struct(
                    F.col("path"),
                    F.col("logical_file_name"),
                    F.col("length").alias("size"),
                    F.col("modificationTime").alias("source_modification_time"),
                )
            ),
        )
    )
    return parsed_df
