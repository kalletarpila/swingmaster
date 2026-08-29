from __future__ import annotations

from pathlib import Path

from swingmaster.fundamentals.v3_phase8e_apply import (
    EXPECTED_FROZEN_GROUPS,
    EXPECTED_FROZEN_ROWS,
    EXPECTED_FROZEN_TICKERS,
    QUALITY_GOOD_GAPS,
    data_quality_report_rows,
    load_final_apply_rows,
    operational_quality,
    pct,
)
from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import classify_attribution


def test_final_preapply_set_exact_match() -> None:
    rows, blockers, validation = load_final_apply_rows(
        Path("temp/fundamentals_v3_phase8e_preapply_downstream_proving/20260829T_PHASE8E_PREAPPLY"),
        Path("temp/fundamentals_v3_phase8e_rehearse_fiscal_repairs/20260828T_PHASE8E"),
    )

    assert len(rows) == EXPECTED_FROZEN_ROWS
    assert len({row["transformation_group"] for row in rows}) == EXPECTED_FROZEN_GROUPS
    assert len({row["ticker"] for row in rows}) == EXPECTED_FROZEN_TICKERS
    assert len(blockers) == 207
    assert validation["valid_for_production_apply"] is True
    assert validation["blocked_rows_promoted"] == 0


def test_percent_helper() -> None:
    assert pct(1, 4) == 25.0
    assert pct(1, 0) == 0.0


def test_quality_rows_include_current_downstream_availability() -> None:
    risk = {
        "full": {"clean": 9, "rows": 10},
        "2024plus": {"clean": 8, "rows": 10},
        "2025plus": {"clean": 7, "rows": 10},
        "latest8q": {"clean": 6, "rows": 10},
        "latest4q": {"clean": 5, "rows": 10},
    }
    active = [
        {"scope": "latest_quarter", "clean_tickers": 4, "total_active_tickers": 10, "pct": 40.0},
        {"scope": "latest4q_all_clean", "clean_tickers": 3, "total_active_tickers": 10, "pct": 30.0},
        {"scope": "latest8q_all_clean", "clean_tickers": 2, "total_active_tickers": 10, "pct": 20.0},
    ]
    downstream = [
        {"metric": "TTM_CLEAN", "available": 9, "total": 10, "pct": 90.0},
        {"metric": "SCORE_AVAILABLE", "available": 8, "total": 10, "pct": 80.0},
        {"metric": "LIFECYCLE_AVAILABLE", "available": 7, "total": 10, "pct": 70.0},
        {"metric": "VALUATION_AVAILABLE", "available": 6, "total": 10, "pct": 60.0},
        {"metric": "ALL_CURRENT_DOWNSTREAM_AVAILABLE", "available": 5, "total": 10, "pct": 50.0},
    ]

    rows = data_quality_report_rows(risk, active, downstream)

    assert rows[0]["metric"] == "Full canonical fiscal identity clean"
    assert rows[-1]["metric"] == "All current downstream layers available"


def test_operational_quality_good_with_known_gaps() -> None:
    summary = {
        "risk_after": {"latest_quarter": {"clean": 94, "rows": 100}},
        "data_quality": {"headline": [{"metric": "Current TTM clean", "pct": 80.0}]},
    }

    assert operational_quality(summary) == QUALITY_GOOD_GAPS


def test_whlr_price_only_normalization_is_not_unrelated_drift() -> None:
    row = {"layer": "valuation", "ticker": "WHLR", "change_type": "UPDATED", "changed_fields": "valuation_close_price|market_cap|enterprise_value|output_json"}

    assert classify_attribution(row, {"AAPL"}) == "EXPECTED_REBUILD_NORMALIZATION"
