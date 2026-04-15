from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.parse import quote

from ingestion_orchestrator.logging_utils import get_logger


logger = get_logger(__name__)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def _to_filesystem_name(logical_file_name: str) -> str:
    if os.name != "nt":
        return logical_file_name
    # Windows does not allow "|" and other characters in file names.
    return quote(logical_file_name, safe="._-")


def create_demo_files(base_path: Path) -> None:
    # Scenario A: complete group with manifest and all parts (ALL_PARTS)
    group_a_files = [
        "SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|PART=0001.json",
        "SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|PART=0002.json",
        "SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|PART=0003.json",
    ]
    for index, file_name in enumerate(group_a_files, start=1):
        _write_json(
            base_path / "parts" / "group_a" / _to_filesystem_name(file_name),
            {"row": index, "customer_id": index, "logical_file_name": file_name},
        )
    _write_json(
        base_path
        / "manifests"
        / "group_a"
        / _to_filesystem_name("SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|MANIFEST.json"),
        {"expected_parts": 3},
    )

    # Scenario B: no manifest; closes by TIMEOUT.
    _write_json(
        base_path
        / "parts"
        / "group_b"
        / _to_filesystem_name("SISTEMA=TJSP|TABELA=processos|DT_REF=2026-04-15|PART=0001.json"),
        {
            "process_id": 1001,
            "logical_file_name": "SISTEMA=TJSP|TABELA=processos|DT_REF=2026-04-15|PART=0001.json",
        },
    )

    # Scenario C: duplicate part arriving again in another folder (same business key).
    duplicate_name = "SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|PART=0001.json"
    _write_json(
        base_path / "parts" / "dup_1" / _to_filesystem_name(duplicate_name),
        {"row": 1, "customer_id": 1, "logical_file_name": duplicate_name},
    )
    _write_json(
        base_path / "parts" / "dup_2" / _to_filesystem_name(duplicate_name),
        {"row": 1, "customer_id": 1, "logical_file_name": duplicate_name},
    )

    # Scenario D: conflicting manifest values.
    _write_json(
        base_path
        / "manifests"
        / "conflict_a"
        / _to_filesystem_name("SISTEMA=TJSP|TABELA=advogados|DT_REF=2026-04-14|MANIFEST.json"),
        {"expected_parts": 2},
    )
    _write_json(
        base_path
        / "manifests"
        / "conflict_b"
        / _to_filesystem_name("SISTEMA=TJSP|TABELA=advogados|DT_REF=2026-04-14|MANIFEST.json"),
        {"expected_parts": 3},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate demo landing files for local tests.")
    parser.add_argument(
        "--output-path",
        default="sample_data/demo_landing",
        help="Output folder for generated files.",
    )
    args = parser.parse_args()

    output_path = Path(args.output_path).resolve()
    create_demo_files(output_path)
    logger.info("demo_files_created", extra={"payload": {"output_path": str(output_path)}})


if __name__ == "__main__":
    main()
