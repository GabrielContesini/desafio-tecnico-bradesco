from __future__ import annotations

from datetime import datetime, timezone

from ingestion_orchestrator.logic import compute_incomplete_flag, determine_ready_reason


def test_ready_reason_all_parts_has_priority() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    expires = datetime(2026, 4, 15, 11, 0, tzinfo=timezone.utc)
    reason = determine_ready_reason(
        expected_parts=3,
        received_parts=3,
        expires_at=expires,
        now_ts=now,
    )
    assert reason == "ALL_PARTS"


def test_ready_reason_timeout_without_manifest() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    expires = datetime(2026, 4, 15, 11, 30, tzinfo=timezone.utc)
    reason = determine_ready_reason(
        expected_parts=None,
        received_parts=1,
        expires_at=expires,
        now_ts=now,
    )
    assert reason == "TIMEOUT"


def test_ready_reason_none_before_timeout() -> None:
    now = datetime(2026, 4, 15, 11, 0, tzinfo=timezone.utc)
    expires = datetime(2026, 4, 15, 11, 30, tzinfo=timezone.utc)
    reason = determine_ready_reason(
        expected_parts=None,
        received_parts=1,
        expires_at=expires,
        now_ts=now,
    )
    assert reason is None


def test_incomplete_flag_timeout_with_missing_parts() -> None:
    assert compute_incomplete_flag("TIMEOUT", expected_parts=5, received_parts=2) is True
    assert compute_incomplete_flag("TIMEOUT", expected_parts=None, received_parts=2) is False
    assert compute_incomplete_flag("ALL_PARTS", expected_parts=5, received_parts=5) is False
