from __future__ import annotations

import pytest

from ingestion_orchestrator.parsing import ParseFileNameError, parse_file_name, parse_manifest_payload


def test_parse_part_file_name_valid() -> None:
    parsed = parse_file_name("SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|PART=0007.json")
    assert parsed.system_name == "TJSP"
    assert parsed.table_name == "clientes"
    assert parsed.dt_ref == "2026-04-14"
    assert parsed.part_number == 7
    assert parsed.is_manifest is False
    assert parsed.group_key == "TJSP|clientes|2026-04-14"


def test_parse_manifest_file_name_valid() -> None:
    parsed = parse_file_name("SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-04-14|MANIFEST.json")
    assert parsed.is_manifest is True
    assert parsed.part_number is None


def test_parse_file_name_supports_url_encoded_form() -> None:
    encoded = "SISTEMA%3DTJSP%7CTABELA%3Dclientes%7CDT_REF%3D2026-04-14%7CPART%3D0001.json"
    parsed = parse_file_name(encoded)
    assert parsed.part_number == 1


def test_parse_file_name_invalid_pattern() -> None:
    with pytest.raises(ParseFileNameError):
        parse_file_name("clientes_2026-04-14.json")


def test_parse_file_name_invalid_date() -> None:
    with pytest.raises(ParseFileNameError):
        parse_file_name("SISTEMA=TJSP|TABELA=clientes|DT_REF=2026-14-99|PART=0001.json")


def test_parse_manifest_payload_valid() -> None:
    expected, error = parse_manifest_payload('{"expected_parts": 4}')
    assert expected == 4
    assert error is None


def test_parse_manifest_payload_invalid() -> None:
    expected, error = parse_manifest_payload('{"expected_parts": "x"}')
    assert expected is None
    assert error == "INVALID_EXPECTED_PARTS"
