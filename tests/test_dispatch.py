from __future__ import annotations

from ingestion_orchestrator.logic import should_dispatch


def test_should_dispatch_only_when_ready_and_not_already_dispatched() -> None:
    assert should_dispatch(status="READY", already_dispatched=False) is True
    assert should_dispatch(status="READY", already_dispatched=True) is False
    assert should_dispatch(status="OPEN", already_dispatched=False) is False
    assert should_dispatch(status="DISPATCHED", already_dispatched=True) is False
