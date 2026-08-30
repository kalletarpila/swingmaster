from __future__ import annotations

from pathlib import Path

from swingmaster.fundamentals.v4_sharadar_aapl_shares_validation import (
    V4SharadarPaths,
    compare_latest8,
    match_v3,
    run_validation,
    semantic_summary,
    sharadar_aapl_arq,
)


def test_only_arq_used_for_primary_historical_comparison() -> None:
    rows = [
        {"ticker": "AAPL", "dimension": "ARQ", "fiscalperiod": "2026-Q3", "reportperiod": "2026-06-27", "calendardate": "2026-06-30", "date": "2026-07-31", "lastupdated": "", "sharefactor": "1", "sharesbas": "10", "shareswa": "9", "shareswadil": "8"},
        {"ticker": "AAPL", "dimension": "MRQ", "fiscalperiod": "2026-Q3", "reportperiod": "2026-06-27", "calendardate": "2026-06-30", "date": "2026-07-31", "lastupdated": "", "sharefactor": "1", "sharesbas": "99", "shareswa": "99", "shareswadil": "99"},
    ]
    assert len(sharadar_aapl_arq(rows)) == 1
    assert sharadar_aapl_arq(rows)[0]["sharesbas"] == 10


def test_shareswa_is_not_mapped_to_v4_shares_outstanding() -> None:
    summary = semantic_summary({"rows": 1}, [], [], {"matching_periods": 0, "sharesbas_same": 0, "sharesbas_different": 0})
    assert "shareswa and shareswadil remain informational" in summary["recommended_mapping"]


def test_shareswadil_is_not_mapped_to_v4_shares_outstanding() -> None:
    summary = semantic_summary({"rows": 1}, [], [], {"matching_periods": 0, "sharesbas_same": 0, "sharesbas_different": 0})
    assert "V4 shares_outstanding = Sharadar ARQ sharesbas" in summary["recommended_mapping"]


def test_seven_day_period_matching_does_not_match_neighboring_quarters() -> None:
    sharadar = {"fiscal_year": 2026, "fiscal_quarter": "Q2", "reportperiod": "2026-03-28"}
    v3_rows = [
        {"fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2025-12-31"},
        {"fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end_date": "2026-03-31"},
    ]
    matched, basis = match_v3(sharadar, v3_rows)
    assert matched["fiscal_quarter"] == "Q2"
    assert basis == "FYFQ_NEAR_PERIOD"


def test_missing_values_remain_missing() -> None:
    arq = [{"fiscal_year": 2026, "fiscal_quarter": "Q3", "fiscalperiod": "2026-Q3", "reportperiod": "2026-06-27", "calendardate": "2026-06-30", "date": "2026-07-31", "sharesbas": None, "shareswa": 1.0, "shareswadil": 1.0}]
    comparison = compare_latest8(arq, [{"quarter_id": 1, "fiscal_year": 2026, "fiscal_quarter": "Q3", "period_end_date": "2026-06-30", "shares_outstanding": 1.0}])
    assert comparison[0]["classification"] == "SHARADAR_MISSING"


def test_no_production_writes(tmp_path: Path) -> None:
    summary = run_validation(V4SharadarPaths(artifact_root=tmp_path, write_documentation=False))
    assert summary["safety"]["production_writes"] == 0


def test_no_network_calls(tmp_path: Path) -> None:
    summary = run_validation(V4SharadarPaths(artifact_root=tmp_path, write_documentation=False))
    assert summary["safety"]["network_calls"] == 0
