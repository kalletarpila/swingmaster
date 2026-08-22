from __future__ import annotations

import sqlite3

import pytest

from swingmaster.fundamentals.v3_core_gap_diagnostic import (
    adjacent_summary,
    adjacent_quarter_q_level_results,
    build_conflict_field_matrix,
    build_primary_conflict_rows,
    classify_period_relation,
    classify_v2_identity,
    compare_values,
    connect_readonly,
    cumulative_ytd_diagnostic,
    fingerprint_result,
    scale_normalization_diagnostic,
    trusted_score,
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


def test_conflict_field_attribution_identifies_primary_revenue_conflict() -> None:
    conflict = {
        "ticker": "AAA",
        "fiscal_year": 2026,
        "fiscal_quarter": "Q1",
        "classification": "CONFLICT",
    }
    v3 = {
        ("AAA", 2026, "Q1"): {
            "ticker": "AAA",
            "fiscal_year": 2026,
            "fiscal_quarter": "Q1",
            "period_end_date": "2026-03-31",
            "revenue": 100_000_000.0,
            "gross_profit": 50_000_000.0,
            "operating_income": 10_000_000.0,
            "ebit": None,
            "ebitda": None,
            "net_income": 8_000_000.0,
            "operating_cashflow": None,
            "capex": None,
            "free_cashflow": None,
            "cash": None,
            "total_debt": None,
            "shares_outstanding": 1_000_000.0,
        }
    }
    v2 = {
        ("AAA", 2026, "Q1"): {
            "period_end_date": "2026-03-31",
            "revenue": 200_000_000.0,
            "gross_profit": 50_000_000.0,
            "operating_income": 10_000_000.0,
            "ebit": None,
            "ebitda": None,
            "net_income": 8_000_000.0,
            "operating_cashflow": None,
            "capex": None,
            "free_cashflow": None,
            "cash": None,
            "total_debt": None,
            "shares_outstanding": 1_000_000.0,
        }
    }

    matrix = build_conflict_field_matrix([conflict], v3, v2)
    primary = build_primary_conflict_rows([conflict], matrix)

    assert primary[0]["primary_conflict_field"] == "revenue"
    assert "revenue" in primary[0]["conflicting_fields"]


def test_trusted_score_ignores_semantic_risk_fields_by_default() -> None:
    v3 = {"revenue": 100.0, "net_income": 10.0, "ebitda": 1000.0}
    v2 = {"revenue": 101.0, "net_income": 10.1, "ebitda": -1000.0}

    result = trusted_score(v3, v2)

    assert result["matches"] == 2
    assert result["conflicts"] == 0


def test_adjacent_q_level_best_match_previous_and_next() -> None:
    v3_rows = [
        {"ticker": "AAA", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "revenue": 100_000_000.0, "gross_profit": 50_000_000.0, "operating_income": 10_000_000.0, "net_income": 8_000_000.0, "operating_cashflow": 9_000_000.0, "cash": 20_000_000.0, "total_debt": 5_000_000.0},
        {"ticker": "AAA", "fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end_date": "2026-06-30", "revenue": 200_000_000.0, "gross_profit": 80_000_000.0, "operating_income": 20_000_000.0, "net_income": 18_000_000.0, "operating_cashflow": 19_000_000.0, "cash": 25_000_000.0, "total_debt": 6_000_000.0},
        {"ticker": "AAA", "fiscal_year": 2026, "fiscal_quarter": "Q3", "period_end_date": "2026-09-30", "revenue": 300_000_000.0, "gross_profit": 90_000_000.0, "operating_income": 30_000_000.0, "net_income": 28_000_000.0, "operating_cashflow": 29_000_000.0, "cash": 35_000_000.0, "total_debt": 7_000_000.0},
    ]
    v2_rows = {
        ("AAA", 2026, "Q2"): {"revenue": 300_000_000.0, "gross_profit": 90_000_000.0, "operating_income": 30_000_000.0, "net_income": 28_000_000.0, "operating_cashflow": 29_000_000.0, "cash": 35_000_000.0, "total_debt": 7_000_000.0},
    }
    class_map = {("AAA", 2026, "Q2"): {"classification": "CONFLICT"}}

    rows = adjacent_quarter_q_level_results(v3_rows, v2_rows, class_map=class_map, conflict_only=True)

    assert rows[0]["best_match"] == "NEXT_Q_BEST"
    assert adjacent_summary(rows)["NEXT_Q_BEST"] == 1


def test_revenue_anchored_fingerprint_conflicts_when_revenue_disagrees() -> None:
    v3_rows = [{"ticker": "AAA", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "revenue": 100_000_000.0, "net_income": 10_000_000.0, "cash": 5_000_000.0}]
    v2_rows = {("AAA", 2026, "Q1"): {"revenue": 150_000_000.0, "net_income": 10_000_000.0, "cash": 5_000_000.0}}

    result = fingerprint_result(v3_rows, v2_rows, "REVENUE_ANCHORED", ("revenue", "net_income", "cash"), 0.05)

    assert result["CONFLICT"] == 1


def test_scale_mismatch_detection_finds_thousand_multiplier() -> None:
    matrix = [
        {
            "ticker": "AAA",
            "fiscal_year": 2026,
            "fiscal_quarter": "Q1",
            "field": "revenue",
            "comparable": 1,
            "v3_value": 1_000.0,
            "v2_value": 1_000_000.0,
        }
    ]

    rows = scale_normalization_diagnostic(matrix)

    assert rows
    assert "1000" in rows[0]["scale_or_sign_pattern"]


def test_cumulative_ytd_diagnostic_detects_ytd_like_q2_value() -> None:
    v3_rows = [
        {"ticker": "AAA", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "operating_cashflow": 100_000_000.0, "capex": -10_000_000.0, "free_cashflow": 90_000_000.0, "revenue": 1_000_000_000.0, "net_income": 50_000_000.0},
        {"ticker": "AAA", "fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end_date": "2026-06-30", "operating_cashflow": 110_000_000.0, "capex": -20_000_000.0, "free_cashflow": 90_000_000.0, "revenue": 1_200_000_000.0, "net_income": 55_000_000.0},
    ]
    v2_rows = {
        ("AAA", 2026, "Q1"): {"operating_cashflow": 100_000_000.0, "capex": -10_000_000.0, "free_cashflow": 90_000_000.0, "revenue": 1_000_000_000.0, "net_income": 50_000_000.0},
        ("AAA", 2026, "Q2"): {"operating_cashflow": 210_000_000.0, "capex": -30_000_000.0, "free_cashflow": 180_000_000.0, "revenue": 2_200_000_000.0, "net_income": 105_000_000.0},
    }

    rows = cumulative_ytd_diagnostic(v3_rows, v2_rows)
    ocf_q2 = next(row for row in rows if row["field"] == "operating_cashflow" and row["fiscal_quarter"] == "Q2")

    assert ocf_q2["v2_closer_to_v3_ytd_than_quarter"] == 1
