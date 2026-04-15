from __future__ import annotations

from datetime import datetime, timezone

from ingestion_orchestrator.logic import GroupEvent, apply_part_idempotently, build_group_snapshot


def test_apply_part_idempotently() -> None:
    seen: set[int] = set()
    assert apply_part_idempotently(seen, 1) is True
    assert apply_part_idempotently(seen, 1) is False
    assert apply_part_idempotently(seen, 2) is True
    assert seen == {1, 2}


def test_group_snapshot_counts_distinct_parts() -> None:
    base = datetime(2026, 4, 15, 10, 0, tzinfo=timezone.utc)
    events = [
        GroupEvent(arrived_at=base, is_manifest=False, part_number=1),
        GroupEvent(arrived_at=base, is_manifest=False, part_number=1),
        GroupEvent(arrived_at=base, is_manifest=False, part_number=2),
    ]
    snapshot = build_group_snapshot(events, timeout_minutes=30)
    assert snapshot.received_parts == 2
    assert snapshot.expected_parts is None
    assert snapshot.expires_at == datetime(2026, 4, 15, 10, 30, tzinfo=timezone.utc)
