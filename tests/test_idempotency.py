from __future__ import annotations

from ingestion_orchestrator.hashing import make_dispatch_key, make_file_business_key


def test_part_business_key_is_deterministic() -> None:
    key_1 = make_file_business_key(
        system_name="TJSP",
        table_name="clientes",
        dt_ref="2026-04-14",
        is_manifest=False,
        part_number=1,
        expected_parts=None,
        file_name="SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|PART=0001.json",
        parse_error=None,
    )
    key_2 = make_file_business_key(
        system_name="TJSP",
        table_name="clientes",
        dt_ref="2026-04-14",
        is_manifest=False,
        part_number=1,
        expected_parts=None,
        file_name="SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|PART=0001.json",
        parse_error=None,
    )
    assert key_1 == key_2


def test_manifest_key_changes_when_expected_parts_changes() -> None:
    key_1 = make_file_business_key(
        system_name="TJSP",
        table_name="clientes",
        dt_ref="2026-04-14",
        is_manifest=True,
        part_number=None,
        expected_parts=3,
        file_name="SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|MANIFEST.json",
        parse_error=None,
    )
    key_2 = make_file_business_key(
        system_name="TJSP",
        table_name="clientes",
        dt_ref="2026-04-14",
        is_manifest=True,
        part_number=None,
        expected_parts=4,
        file_name="SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|MANIFEST.json",
        parse_error=None,
    )
    assert key_1 != key_2


def test_parse_error_business_key_is_deterministic() -> None:
    key_1 = make_file_business_key(
        system_name=None,
        table_name=None,
        dt_ref=None,
        is_manifest=False,
        part_number=None,
        expected_parts=None,
        file_name="invalid-name.json",
        parse_error="INVALID_FILENAME_PATTERN",
    )
    key_2 = make_file_business_key(
        system_name=None,
        table_name=None,
        dt_ref=None,
        is_manifest=False,
        part_number=None,
        expected_parts=None,
        file_name="invalid-name.json",
        parse_error="INVALID_FILENAME_PATTERN",
    )
    assert key_1 == key_2


def test_dispatch_key_deterministic() -> None:
    group_key = "TJSP|clientes|2026-04-14"
    assert make_dispatch_key(group_key) == make_dispatch_key(group_key)
