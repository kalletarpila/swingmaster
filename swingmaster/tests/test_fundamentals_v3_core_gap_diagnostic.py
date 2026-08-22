from __future__ import annotations

import sqlite3

import pytest

from swingmaster.fundamentals.v3_core_gap_diagnostic import (
    classify_period_relation,
    classify_v2_identity,
    compare_values,
    connect_readonly,
)


def _comparison(field: str, v3_value: float | None, v2_value: float | None):
    return compare_values(field, v3_value, v2_value)


def test_fyfq_candidate_alone_is_not_confirmed_same_quarter() -> None:
    result = classify_v2_identity([], "EXACT_PERIOD_END")

    assert result.classification == "INSUFFICIENT_EVIDENCE"
    assert result.comparable_fields == 0


def test_period_end_relations() -> None:
    assert classify_period_relation("2025-03-31", "2025-03-31") == "EXACT_PERIOD_END"
    assert classify_period_relation("2025-03-31", "2025-03-28") == "SMALL_KNOWN_PROVIDER_VARIANT"
    assert classify_period_relation("2025-03-31", "2025-03-02") == "KNOWN_FISCAL_CALENDAR_VARIANT"
    assert classify_period_relation("2025-03-31", "2024-12-31") == "MATERIAL_PERIOD_END_DIFFERENCE"
    assert classify_period_relation("2025-03-31", None) == "V2_PERIOD_END_MISSING"


def test_multi_field_strong_match_requires_tier_a_evidence() -> None:
    comparisons = [
        _comparison("revenue", 100_000_000, 101_000_000),
        _comparison("net_income", 10_000_000, 10_200_000),
        _comparison("operating_cashflow", 12_000_000, 11_900_000),
        _comparison("gross_profit", 45_000_000, 44_900_000),
    ]

    result = classify_v2_identity(comparisons, "EXACT_PERIOD_END")

    assert result.classification == "STRONG_MATCH"
    assert result.matching_tier_a_fields_5pct == 3


def test_one_field_only_is_insufficient() -> None:
    result = classify_v2_identity([_comparison("revenue", 100, 100)], "EXACT_PERIOD_END")

    assert result.classification == "INSUFFICIENT_EVIDENCE"


def test_identity_tolerance_threshold_columns() -> None:
    comparison = compare_values("revenue", 100_000_000.0, 104_900_000.0)

    assert not comparison.within_1pct
    assert not comparison.within_2pct
    assert comparison.within_5pct
    assert comparison.within_10pct
    assert comparison.status == "MATCH"


def test_near_zero_absolute_floor_allows_small_same_sign_noise() -> None:
    comparison = compare_values("net_income", 1_000.0, 9_000.0)

    assert comparison.within_5pct
    assert not comparison.sign_mismatch


def test_sign_mismatch_blocks_match_when_not_near_zero() -> None:
    comparison = compare_values("net_income", 1_000_000.0, -1_000_000.0)

    assert comparison.sign_mismatch
    assert comparison.status == "SIGN_MISMATCH"


def test_tier_a_conflict_prevents_strong_match() -> None:
    comparisons = [
        _comparison("revenue", 100_000_000, 150_000_000),
        _comparison("net_income", 10_000_000, 10_100_000),
        _comparison("operating_cashflow", 12_000_000, 12_100_000),
        _comparison("gross_profit", 45_000_000, 45_100_000),
        _comparison("operating_income", 20_000_000, 20_100_000),
    ]

    result = classify_v2_identity(comparisons, "EXACT_PERIOD_END")

    assert result.classification == "CONFLICT"
    assert result.tier_a_conflicts == 1


def test_material_period_mismatch_can_be_period_identity_conflict() -> None:
    comparisons = [
        _comparison("revenue", 100_000_000, 150_000_000),
        _comparison("net_income", 10_000_000, 20_000_000),
        _comparison("operating_cashflow", 12_000_000, 22_000_000),
    ]

    result = classify_v2_identity(comparisons, "MATERIAL_PERIOD_END_DIFFERENCE")

    assert result.classification == "PERIOD_IDENTITY_CONFLICT"


def test_limited_two_tier_a_match_is_separate_hold_for_review_class() -> None:
    comparisons = [
        _comparison("revenue", 100_000_000, 100_500_000),
        _comparison("cash", 50_000_000, 50_100_000),
    ]

    result = classify_v2_identity(comparisons, "EXACT_PERIOD_END")

    assert result.classification == "STRONG_MATCH_LIMITED_FIELDS"


def test_classification_is_deterministic() -> None:
    comparisons = [
        _comparison("revenue", 100_000_000, 100_500_000),
        _comparison("cash", 50_000_000, 50_100_000),
        _comparison("net_income", 5_000_000, 5_100_000),
    ]

    first = classify_v2_identity(comparisons, "EXACT_PERIOD_END")
    second = classify_v2_identity(comparisons, "EXACT_PERIOD_END")

    assert first == second


def test_readonly_connection_blocks_v2_and_v3_style_writes(tmp_path) -> None:
    db_path = tmp_path / "sample.db"
    writer = sqlite3.connect(db_path)
    writer.execute("CREATE TABLE v3_quarter(id INTEGER PRIMARY KEY)")
    writer.execute("INSERT INTO v3_quarter(id) VALUES (1)")
    writer.commit()
    writer.close()

    conn = connect_readonly(db_path)
    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 1
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO v3_quarter(id) VALUES (2)")
