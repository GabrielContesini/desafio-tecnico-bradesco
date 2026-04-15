from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence


@dataclass(frozen=True)
class GroupEvent:
    arrived_at: datetime
    is_manifest: bool
    part_number: int | None = None
    expected_parts: int | None = None


@dataclass(frozen=True)
class GroupSnapshot:
    first_seen_at: datetime
    last_seen_at: datetime
    expires_at: datetime
    received_parts: int
    expected_parts: int | None
    manifest_conflict: bool


def ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def resolve_manifest_expected(existing_expected: int | None, ordered_manifest_values: Sequence[int]) -> tuple[int | None, bool]:
    if existing_expected is not None:
        return existing_expected, any(v != existing_expected for v in ordered_manifest_values)
    if not ordered_manifest_values:
        return None, False
    first = ordered_manifest_values[0]
    conflict = any(v != first for v in ordered_manifest_values[1:])
    return first, conflict


def determine_ready_reason(
    *,
    expected_parts: int | None,
    received_parts: int,
    expires_at: datetime,
    now_ts: datetime,
) -> str | None:
    now_utc = ensure_utc(now_ts)
    expires_utc = ensure_utc(expires_at)
    if expected_parts is not None and received_parts >= expected_parts:
        return "ALL_PARTS"
    if now_utc >= expires_utc:
        return "TIMEOUT"
    return None


def compute_incomplete_flag(reason: str, expected_parts: int | None, received_parts: int) -> bool:
    if reason != "TIMEOUT":
        return False
    if expected_parts is None:
        return False
    return received_parts < expected_parts


def apply_part_idempotently(seen_parts: set[int], part_number: int) -> bool:
    if part_number in seen_parts:
        return False
    seen_parts.add(part_number)
    return True


def should_dispatch(*, status: str, already_dispatched: bool) -> bool:
    return status == "READY" and not already_dispatched


def build_group_snapshot(events: Iterable[GroupEvent], timeout_minutes: int = 30) -> GroupSnapshot:
    materialized = list(events)
    if not materialized:
        raise ValueError("events must not be empty")

    first_seen = min(ensure_utc(event.arrived_at) for event in materialized)
    last_seen = max(ensure_utc(event.arrived_at) for event in materialized)
    expires_at = first_seen + timedelta(minutes=timeout_minutes)

    seen_parts: set[int] = set()
    manifest_values: list[int] = []
    for event in sorted(materialized, key=lambda item: ensure_utc(item.arrived_at)):
        if event.is_manifest and event.expected_parts is not None:
            manifest_values.append(event.expected_parts)
            continue
        if event.part_number is None:
            continue
        apply_part_idempotently(seen_parts, event.part_number)

    expected_parts, manifest_conflict = resolve_manifest_expected(None, manifest_values)
    return GroupSnapshot(
        first_seen_at=first_seen,
        last_seen_at=last_seen,
        expires_at=expires_at,
        received_parts=len(seen_parts),
        expected_parts=expected_parts,
        manifest_conflict=manifest_conflict,
    )
