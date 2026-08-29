from __future__ import annotations

import csv
from pathlib import Path

from swingmaster.fundamentals.v3_phase8f_latest8q_gap_analysis import (
    CLASSIFICATION_STRUCTURAL,
    EXPECTED_P1_TICKERS,
    Phase8FPaths,
    effective_issuer_pair,
    external_required,
    field_state,
    metadata_or_sequence_issues,
    parse_date,
    pct,
    period_end_status,
    publish_date_status,
    quarter_complexity,
    run_phase8f,
    sequence_statuses,
    theoretical_closure,
)


def test_percent_and_date_helpers() -> None:
    assert pct(1, 4) == 25.0
    assert pct(1, 0) == 0.0
    assert parse_date("2026-08-29").isoformat() == "2026-08-29"
    assert parse_date("") is None


def test_field_completeness_and_approved_fcf_derivation() -> None:
    row = {"free_cashflow": None, "operating_cashflow": 10.0, "capex": -2.0, "ebit": None, "operating_income": 1.0}

    assert field_state(row, "FCF", "free_cashflow")[0] == "DERIVABLE_EXISTING_APPROVED_RULE"
    assert field_state(row, "EBIT", "ebit")[0] == "DERIVATION_EVIDENCE_MISSING"


def test_fcf_missing_input_is_explicit() -> None:
    state, derivable, needs = field_state({"free_cashflow": None, "operating_cashflow": 1.0, "capex": None}, "FCF", "free_cashflow")

    assert state == "DERIVATION_EVIDENCE_MISSING"
    assert derivable == []
    assert "Capex" in needs[0]


def test_fiscal_identity_pair_uses_exact_anchor_when_available() -> None:
    row = {"identity_class": "BLOCK_EXACT_FY_CONFLICT", "exact_fy": 2026, "exact_fq": "Q1", "fiscal_year": 2025, "fiscal_quarter": "Q1"}

    assert effective_issuer_pair(row) == (2026, "Q1")


def test_period_end_classification_distinguishes_supported_from_transition() -> None:
    assert period_end_status({"period_end": "2026-03-31", "identity_class": "PASS_INFERRED", "period_end_structural_fit": "UNRESOLVED"}) == "PERIOD_END_CLEAN"
    assert period_end_status({"period_end": "2026-03-31", "identity_class": "REVIEW_TRANSITION"}) == "PERIOD_END_TRANSITION_REVIEW"
    assert period_end_status({"period_end": None, "identity_class": "PASS_DIRECT_EXACT"}) == "PERIOD_END_MISSING"


def test_publish_date_classification() -> None:
    assert publish_date_status({"period_end": "2026-03-31", "publish_date": ""}) == "PUBLISH_DATE_MISSING"
    assert publish_date_status({"period_end": "2026-03-31", "publish_date": "2026-03-31"}) == "PUBLISH_BEFORE_OR_ON_PERIOD_END"
    assert publish_date_status({"period_end": "2026-03-31", "publish_date": "2026-05-01"}) == "PUBLISH_DATE_CLEAN"
    assert publish_date_status({"period_end": "2026-03-31", "publish_date": "2026-05-01"}, publish_sequence_issue=True) == "PUBLISH_SEQUENCE_CONFLICT"


def test_sequence_detection_clean_missing_duplicate_and_transition() -> None:
    clean = [
        {"ticker": "A", "quarter_id": 1, "quarter_position_latest8q": 1, "fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end": "2026-06-30", "publish_date": "2026-08-01", "identity_class": "PASS_DIRECT_EXACT"},
        {"ticker": "A", "quarter_id": 2, "quarter_position_latest8q": 2, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2026-03-31", "publish_date": "2026-05-01", "identity_class": "PASS_DIRECT_EXACT"},
    ]
    assert sequence_statuses(clean)[1]["sequence_status"] == "SEQUENCE_CLEAN"

    missing = [clean[0], {**clean[1], "fiscal_quarter": "Q4", "fiscal_year": 2025}]
    assert sequence_statuses(missing)[1]["sequence_status"] == "MISSING_QUARTER"

    duplicate = [clean[0], {**clean[1], "fiscal_quarter": "Q2", "period_end": "2026-06-30"}]
    assert sequence_statuses(duplicate)[1]["sequence_status"] == "DUPLICATE_ECONOMIC_QUARTER"

    transition = [{**clean[0], "identity_class": "REVIEW_TRANSITION"}]
    assert sequence_statuses(transition)[1]["sequence_status"] == "TRANSITION_SEQUENCE"


def test_metadata_issue_detection_ignores_field_only_issues() -> None:
    assert metadata_or_sequence_issues(["SECONDARY_FIELDS_INCOMPLETE"]) is False
    assert metadata_or_sequence_issues(["PRIMARY_CORE_INCOMPLETE"]) is False
    assert metadata_or_sequence_issues(["UNRESOLVED_BOUNDARY"]) is True


def test_external_required_and_repair_complexity() -> None:
    assert external_required(["NEED_FIRST_PUBLIC_RESULT_DATE"])
    assert not external_required(["NEED_LOCAL_LINEAGE_RECONCILIATION"])
    assert quarter_complexity(["TARGET_COLLISION"], ["NEED_TARGET_COLLISION_RESOLUTION"]) == "SOURCE_RECONCILIATION"
    assert quarter_complexity(["TRANSITION_SEQUENCE"], ["NEED_TRANSITION_CALENDAR_EVIDENCE"]) == "TRANSITION_RESEARCH"
    assert quarter_complexity(["MISSING_QUARTER"], ["NEED_MISSING_QUARTER_SOURCE"]) == "MISSING_QUARTER_CREATION"
    assert quarter_complexity(["PRIMARY_CORE_INCOMPLETE"], ["NEED_EBIT"]) == "CONTENT_RECONSTRUCTION"
    assert quarter_complexity([], []) == "NO_REPAIR_NEEDED"


def test_theoretical_closure_gate_has_explicit_paths() -> None:
    assert theoretical_closure([], 8, "", False) == ("LATEST8Q_FULLY_CLEAN", "YES_FULLY_CLEAN")
    assert theoretical_closure([], 4, "RECENT_IPO", False)[1] == "NO_LEGITIMATE_HISTORY_LIMIT"
    assert theoretical_closure([{"missing_noncore_fields": "EBITDA", "missing_core_fields": "", "issue_codes": "SECONDARY_FIELDS_INCOMPLETE"}], 8, "", False)[1] == "YES_PRIMARY_CORE_ONLY"
    assert theoretical_closure([{"missing_noncore_fields": "", "missing_core_fields": "", "issue_codes": "TRANSITION_SEQUENCE"}], 8, "", True)[1] == "NO_UNRESOLVED_STRUCTURAL_DECISION"


def test_phase8f_production_readonly_artifact_contract(tmp_path: Path) -> None:
    artifact_root = tmp_path / "phase8f"

    summary = run_phase8f(Phase8FPaths(artifact_root=artifact_root, v3_db=Path("rc_fundamentals_v3.db"), write_documentation=False))

    assert summary["classification"] == CLASSIFICATION_STRUCTURAL
    assert summary["headline"]["active_tickers"] == 2470
    assert summary["headline"]["latest8q_rows"] == 19728
    assert summary["full_closure"]["no_missing_requirement_in_plan"] == 0
    assert summary["safety"]["production_writes"] == 0
    assert summary["safety"]["rawcandle_writes"] == 0
    assert summary["safety"]["fingerprints_unchanged"] is True
    assert (artifact_root / "latest8q_ticker_gap_summary.csv").exists()
    assert (artifact_root / "latest8q_quarter_gap_detail.csv").exists()
    assert (artifact_root / "latest8q_external_research_queue.csv").exists()
    assert (artifact_root / "latest8q_local_repair_queue.csv").exists()
    assert (artifact_root / "latest8q_structural_review_queue.csv").exists()
    assert (artifact_root / "latest8q_theoretical_closure_test.csv").exists()
    assert (artifact_root / "known_13_latest8q_gap_analysis.csv").exists()


def test_phase8f_known_13_and_required_columns(tmp_path: Path) -> None:
    artifact_root = tmp_path / "phase8f"
    summary = run_phase8f(Phase8FPaths(artifact_root=artifact_root, v3_db=Path("rc_fundamentals_v3.db"), write_documentation=False))
    ticker_csv = (artifact_root / "latest8q_ticker_gap_summary.csv").read_text(encoding="utf-8").splitlines()
    with (artifact_root / "known_13_latest8q_gap_analysis.csv").open(newline="", encoding="utf-8") as handle:
        known_tickers = {row["ticker"] for row in csv.DictReader(handle)}

    assert len(ticker_csv) == 2471
    for ticker in EXPECTED_P1_TICKERS:
        assert ticker in known_tickers
    for required in (
        "latest8q_fully_clean_now",
        "latest8q_fully_clean_primary_core_now",
        "latest8q_fully_complete_all_fields_now",
        "all_evidence_needed_description",
        "theoretical_closure_test",
    ):
        assert required in ticker_csv[0]
    assert summary["full_closure"]["external_research_facts_required_total"] == sum(1 for _ in (artifact_root / "latest8q_external_research_queue.csv").open()) - 1
