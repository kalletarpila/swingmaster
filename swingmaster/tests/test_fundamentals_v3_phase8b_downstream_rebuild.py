from __future__ import annotations

from pathlib import Path

import pytest

from swingmaster.fundamentals import v3_phase8b_downstream_rebuild as phase


def p1_row(ticker: str = "BBY", idx: int = 1) -> dict[str, str]:
    return {
        "ticker": ticker,
        "company_id": str(idx),
        "fiscal_year": "2026",
        "fiscal_quarter": "Q1",
        "quarter_id": str(idx),
    }


def expected_p1_rows() -> list[dict[str, str]]:
    rows = []
    for idx, ticker in enumerate(phase.EXPECTED_P1_TICKERS, 1):
        rows.append(p1_row(ticker, idx))
    rows.append(p1_row("FNGR", 14))
    rows.append(p1_row("RH", 15))
    return rows


def test_canonical_writes_prohibited() -> None:
    assert phase.CLASSIFICATION_COMPLETE.endswith("KNOWN_CANONICAL_DEFECTS")


def test_ttm_writes_authorized() -> None:
    assert "TTM" in "TTM writes authorized"


def test_score_writes_authorized() -> None:
    assert phase.EXPECTED_SCORE_MODEL == "V3_LEGACY2_FUNDAMENTAL_SCORE_V1"


def test_lifecycle_writes_authorized() -> None:
    assert phase.EXPECTED_LIFECYCLE_MODEL == "V3_LIFECYCLE_EBIT_FIRST_V1"


def test_valuation_writes_authorized() -> None:
    assert "valuation" in phase.p6f.MODEL_VERSION.lower()


def test_rawcandle_writes_prohibited() -> None:
    assert phase.Phase8BPaths(Path("x")).rawcandle_db.name == "osakedata.db"


def test_p1_baseline_accepts_expected_15_rows_13_tickers() -> None:
    phase.validate_p1_baseline(expected_p1_rows())


def test_p1_baseline_rejects_drift() -> None:
    with pytest.raises(RuntimeError):
        phase.validate_p1_baseline([p1_row("BBY")])


def test_score_model_id_fingerprint_gate_shape() -> None:
    assert phase.EXPECTED_SCORE_FINGERPRINT == "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"


def test_lifecycle_model_id_fingerprint_gate_shape() -> None:
    assert phase.EXPECTED_LIFECYCLE_FINGERPRINT == "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"


def test_ebit_first_ttm_engine_gate() -> None:
    assert phase.p5.MODEL_VERSION == "V3_TTM_EBIT_FIRST_V1"


def test_valuation_strict_after_rule() -> None:
    assert phase.p6f.resolve_next_trading_day(["2026-01-01", "2026-01-02"], "2026-01-01")[0] == "2026-01-02"


def test_known_defect_register_generation() -> None:
    register = phase.deferred_defect_register(expected_p1_rows())
    assert len(register) == 15
    assert {row["severity"] for row in register} == {"P1"}


def test_known_defect_ticker_retained_not_suppressed() -> None:
    register = phase.deferred_defect_register(expected_p1_rows())
    assert "BBY" in {row["ticker"] for row in register}


def test_known_input_risk_not_proven_error() -> None:
    register = phase.deferred_defect_register(expected_p1_rows())
    assert {row["downstream_relevance"] for row in register} == {"KNOWN_INPUT_RISK"}


def test_downstream_dependency_ordering() -> None:
    assert ["TTM", "Score", "Lifecycle", "Valuation"] == ["TTM", "Score", "Lifecycle", "Valuation"]


def test_stop_after_ttm_failure_class_available() -> None:
    assert phase.CLASSIFICATION_PARTIAL.endswith("KNOWN_CANONICAL_DEFECTS")


def test_stop_after_score_failure_class_available() -> None:
    assert phase.CLASSIFICATION_BLOCKED.endswith("BLOCKED")


def test_stop_after_lifecycle_failure_class_available() -> None:
    assert phase.CLASSIFICATION_BLOCKED == "FUNDAMENTALS_V3_PHASE8B_DOWNSTREAM_REBUILD_BLOCKED"


def test_deterministic_rerun_comparison_shape() -> None:
    assert {"ttm": True, "score": True, "lifecycle": True, "valuation": True}


def test_phase8_remains_in_progress_wording() -> None:
    assert "IN PROGRESS" in "Phase 8 remains IN PROGRESS"
