"""Tests for ReasonCode persisted label conversions."""

from __future__ import annotations

from swingmaster.core.domain.enums import ReasonCode, reason_from_persisted, reason_to_persisted


def test_reason_to_persisted_overlap_prefixed() -> None:
    assert reason_to_persisted(ReasonCode.TREND_STARTED) == "POLICY:TREND_STARTED"


def test_reason_to_persisted_non_overlap_prefixed() -> None:
    assert reason_to_persisted(ReasonCode.ENTRY_CONDITIONS_MET) == "POLICY:ENTRY_CONDITIONS_MET"


def test_reason_from_persisted_accepts_both_forms() -> None:
    assert reason_from_persisted("TREND_STARTED") == ReasonCode.TREND_STARTED
    assert reason_from_persisted("POLICY:TREND_STARTED") == ReasonCode.TREND_STARTED
