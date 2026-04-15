from __future__ import annotations

from datetime import datetime, timezone

from ingestion_orchestrator.logic import GroupEvent, build_group_snapshot, resolve_manifest_expected


def test_resolve_manifest_expected_without_conflict() -> None:
    expected, conflict = resolve_manifest_expected(None, [3, 3, 3])
    assert expected == 3
    assert conflict is False


def test_resolve_manifest_expected_with_conflict() -> None:
    expected, conflict = resolve_manifest_expected(None, [3, 4])
    assert expected == 3
    assert conflict is True


def test_resolve_manifest_expected_keeps_existing_value() -> None:
    expected, conflict = resolve_manifest_expected(5, [3, 5, 7])
    assert expected == 5
    assert conflict is True


def test_group_snapshot_detects_manifest_conflict() -> None:
    ts = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    events = [
        GroupEvent(arrived_at=ts, is_manifest=True, expected_parts=2),
        GroupEvent(arrived_at=ts, is_manifest=True, expected_parts=3),
    ]
    snapshot = build_group_snapshot(events)
    assert snapshot.expected_parts == 2
    assert snapshot.manifest_conflict is True
