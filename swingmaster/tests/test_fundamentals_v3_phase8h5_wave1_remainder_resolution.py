from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_phase8h5_wave1_remainder_resolution import (
    CLASSIFICATION_REMAINING,
    CRITICAL_EXTERNAL_EVIDENCE,
    EXPECTED_EXTERNAL_FACTS,
    EXPECTED_EXTERNAL_TICKERS,
    EXPECTED_LOCAL_CASES,
    EXPECTED_LOCAL_TICKERS,
    EXPECTED_STRUCTURAL_DECISIONS,
    EXPECTED_STRUCTURAL_TICKERS,
    FINAL_STATES,
    LOCAL_TYPES,
    NEXT_LOCAL,
    STRUCTURAL_TYPES,
    Phase8H5Paths,
    analyze_local_cases,
    analyze_structural_cases,
    build_frozen_repair_set,
    current_rows_for_tickers,
    local_case_type,
    reassess_external,
    repair_group_summary,
    run_phase8h5,
    run_rehearsal,
    structural_subtype,
    validate_inputs,
)


def make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, market TEXT, active INTEGER);
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL,
            fiscal_year INTEGER NOT NULL,
            fiscal_quarter TEXT NOT NULL,
            period_end_date TEXT,
            publish_date TEXT,
            market_availability_date TEXT,
            q_lifecycle TEXT NOT NULL DEFAULT 'ACTIVE',
            sec_confirmation_state TEXT NOT NULL DEFAULT 'NOT_DERIVABLE',
            created_at_utc TEXT NOT NULL DEFAULT 'x',
            updated_at_utc TEXT NOT NULL DEFAULT 'x'
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY,
            revenue REAL,
            ebitda REAL,
            free_cashflow REAL,
            cash REAL,
            total_debt REAL,
            shares_outstanding REAL,
            ebit REAL,
            operating_income REAL,
            operating_cashflow REAL,
            capex REAL,
            gross_profit REAL,
            net_income REAL,
            currency TEXT,
            accepted_source_provider TEXT,
            accepted_at_utc TEXT,
            update_run_id TEXT,
            derivation_method TEXT,
            resolution_issue_id INTEGER,
            created_at_utc TEXT NOT NULL DEFAULT 'x',
            updated_at_utc TEXT NOT NULL DEFAULT 'x'
        );
        CREATE TABLE v3_ttm(company_id INTEGER, endpoint_quarter_id INTEGER, model_version TEXT, period_end TEXT, ttm_pit_ready INTEGER);
        CREATE TABLE v3_score(company_id INTEGER, as_of_quarter_id INTEGER, score_model_version TEXT, endpoint_period_end TEXT, score_ready INTEGER);
        CREATE TABLE v3_lifecycle(company_id INTEGER, endpoint_quarter_id INTEGER, lifecycle_model_version TEXT, endpoint_period_end TEXT, lifecycle_ready INTEGER);
        CREATE TABLE v3_valuation(company_id INTEGER, endpoint_quarter_id INTEGER, model_version TEXT, endpoint_period_end TEXT, valuation_ready INTEGER);
        INSERT INTO v3_company(company_id,ticker,market,active) VALUES (1,'TEST','US',1),(2,'OTHER','US',1);
        INSERT INTO v3_quarter(quarter_id,company_id,fiscal_year,fiscal_quarter,period_end_date,publish_date,market_availability_date)
        VALUES (10,1,2026,'Q1','2026-03-31','2026-05-01','2026-05-02'),
               (11,1,2025,'Q4','2025-12-31','2026-02-01','2026-02-02'),
               (20,2,2026,'Q1','2026-03-31','2026-04-01','2026-04-02');
        INSERT INTO v3_quarter_fundamentals(quarter_id,revenue,ebit,free_cashflow,cash,total_debt,shares_outstanding,accepted_source_provider,derivation_method)
        VALUES (10,1,2,3,4,5,6,'YAHOO','DIRECT'),(11,2,3,4,5,6,7,'YAHOO','DIRECT'),(20,3,4,5,6,7,8,'YAHOO','DIRECT');
        """
    )
    conn.commit()
    conn.close()


def write_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def h4_local(ticker: str) -> dict[str, str]:
    return {
        "ticker": ticker,
        "h3_final_state": "LOCAL_RECONCILIATION_REQUIRED",
        "latest4q_clean": "NO",
        "latest8q_downstream_clean": "NO",
        "latest_quarter_clean": "YES",
    }


def h4_structural(ticker: str) -> dict[str, str]:
    return {"ticker": ticker, "final_status": "STRUCTURAL_REVIEW_REQUIRED", "fact_gaps": "1", "verified_differences": "0", "repair_groups": "0"}


def fact(ticker: str = "TEST", evidence_type: str = "OFFICIAL_PERIOD_END", status: str = "VERIFIED", discrepancy: str = "MATCH", value: str = "2026-03-31") -> dict[str, str]:
    return {
        "ticker": ticker,
        "evidence_type": evidence_type,
        "requested_evidence_type": evidence_type,
        "verification_status": status,
        "discrepancy_vs_current": discrepancy,
        "verified_value": value,
        "verified_period_end": value if evidence_type == "OFFICIAL_PERIOD_END" else "2026-03-31",
        "target_column": "period_end_date",
    }


def ext(ticker: str = "TEST", evidence_type: str = "EBIT_DIRECT", status: str = "NOT_FOUND") -> dict[str, str]:
    return {
        "ticker": ticker,
        "requested_fiscal_year": "2026",
        "requested_fiscal_quarter": "Q1",
        "requested_evidence_type": evidence_type,
        "verification_status": status,
    }


def make_phase_inputs(root: Path, h3_root: Path, local_rows: list[dict[str, str]], structural_rows: list[dict[str, str]], external_rows: list[dict[str, str]], verified_rows: list[dict[str, str]]) -> None:
    write_rows(root / "wave1_postapply_local_reconciliation.csv", local_rows)
    write_rows(root / "wave1_postapply_structural_review.csv", structural_rows)
    write_rows(root / "wave1_postapply_more_external_evidence.csv", external_rows)
    write_rows(root / "wave1_210_postapply_audit.csv", [{"ticker": f"T{i}"} for i in range(210)])
    (root / "wave1_vs_wave23_duplicate_request_audit.csv").write_text("", encoding="utf-8")
    for name in [
        "wave1_verified_facts_vs_current_v3.csv",
        "wave1_structural_reconciliation.csv",
        "wave1_fy_fq_resolution.csv",
        "wave1_period_end_resolution.csv",
        "wave1_publish_date_resolution.csv",
        "wave1_ebit_resolution.csv",
        "wave1_debt_resolution.csv",
        "wave1_fcf_resolution.csv",
    ]:
        write_rows(h3_root / name, verified_rows if name == "wave1_verified_facts_vs_current_v3.csv" else [])
    (h3_root / "wave1_target_collision_analysis.csv").write_text("", encoding="utf-8")


def test_expected_input_constants_are_locked() -> None:
    assert EXPECTED_LOCAL_TICKERS == 3
    assert EXPECTED_LOCAL_CASES == 3
    assert EXPECTED_STRUCTURAL_TICKERS == 11
    assert EXPECTED_STRUCTURAL_DECISIONS == 11
    assert EXPECTED_EXTERNAL_TICKERS == 17
    assert EXPECTED_EXTERNAL_FACTS == 53


def test_allowed_local_types_cover_prompt_contract() -> None:
    assert "LOCAL_FY_FQ_RECONCILIATION" in LOCAL_TYPES
    assert "LOCAL_PERIOD_END_RECONCILIATION" in LOCAL_TYPES
    assert "LOCAL_LINEAGE_RECONCILIATION" in LOCAL_TYPES
    assert "LOCAL_TARGET_COLLISION_RECONCILIATION" in LOCAL_TYPES


def test_allowed_structural_types_cover_prompt_contract() -> None:
    assert "TARGET_COLLISION" in STRUCTURAL_TYPES
    assert "CALENDAR_TRANSITION" in STRUCTURAL_TYPES
    assert "STUB_PERIOD" in STRUCTURAL_TYPES
    assert "FISCAL_IDENTITY_BOUNDARY" in STRUCTURAL_TYPES


def test_allowed_final_states_cover_prompt_contract() -> None:
    assert "CLOSED_NO_REPAIR" in FINAL_STATES
    assert "PRODUCTION_REPAIR_READY" in FINAL_STATES
    assert "MORE_EXTERNAL_EVIDENCE_REQUIRED" in FINAL_STATES
    assert "STRUCTURAL_REVIEW_STILL_REQUIRED" in FINAL_STATES


def test_validate_inputs_accepts_exact_h4_counts() -> None:
    inputs = {
        "local": [h4_local(f"L{i}") for i in range(3)],
        "structural": [h4_structural(f"S{i}") for i in range(11)],
        "external": [ext(f"E{i % 17}") for i in range(53)],
        "postapply": [],
        "duplicates": [],
    }

    result = validate_inputs(inputs)

    assert result["valid"] is True


def test_validate_inputs_rejects_count_drift() -> None:
    inputs = {
        "local": [h4_local("L0")],
        "structural": [h4_structural(f"S{i}") for i in range(11)],
        "external": [ext(f"E{i % 17}") for i in range(53)],
        "postapply": [],
        "duplicates": [],
    }

    result = validate_inputs(inputs)

    assert result["valid"] is False
    assert result["local_cases_found"] == 1


def test_current_rows_for_tickers_reads_latest_segment(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    result = current_rows_for_tickers(db, {"TEST"})

    assert [row["quarter_id"] for row in result["TEST"]] == [10, 11]


def test_current_rows_for_tickers_ignores_unrequested(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    result = current_rows_for_tickers(db, {"TEST"})

    assert "OTHER" not in result


def test_local_case_type_detects_explicit_target_collision() -> None:
    rows = [fact(evidence_type="TARGET_COLLISION_EVIDENCE", discrepancy="PERIOD_END_DIFFERENT", value="PERIOD_END_DIFFERENT")]

    assert local_case_type(rows, []) == "LOCAL_TARGET_COLLISION_RECONCILIATION"


def test_local_case_type_detects_lineage() -> None:
    rows = [fact(evidence_type="LINEAGE_OWNERSHIP_EVIDENCE", value="CIK 1; accession 0001")]

    assert local_case_type(rows, []) == "LOCAL_LINEAGE_RECONCILIATION"


def test_local_case_type_detects_period_end() -> None:
    rows = [fact(discrepancy="PERIOD_END_DIFFERENT: current=2026-03-31; official=2025-06-30")]

    assert local_case_type(rows, []) == "LOCAL_PERIOD_END_RECONCILIATION"


def test_local_case_type_detects_fy_fq() -> None:
    rows = [fact(evidence_type="OFFICIAL_FY_FQ_IDENTITY", discrepancy="FY_FQ_DIFFERENT_AT_CURRENT_PERIOD")]

    assert local_case_type(rows, []) == "LOCAL_FY_FQ_RECONCILIATION"


def test_local_case_type_detects_sequence_from_h4_row() -> None:
    assert local_case_type([], [{"latest4q_clean": "NO"}]) == "LOCAL_SEQUENCE_RECONCILIATION"


def test_local_case_resolution_stays_local_when_not_deterministic() -> None:
    analysis, resolution = analyze_local_cases([h4_local("TEST")], {"TEST": [fact(discrepancy="PERIOD_END_DIFFERENT")]}, {"TEST": []})

    assert analysis[0]["repairability"] == "NOT_DETERMINISTIC_FOR_H5"
    assert resolution[0]["final_state"] == "LOCAL_RECONCILIATION_STILL_REQUIRED"


def test_structural_subtype_detects_target_collision() -> None:
    rows = [fact(evidence_type="TARGET_COLLISION_EVIDENCE", discrepancy="PERIOD_END_DIFFERENT", value="PERIOD_END_DIFFERENT")]

    assert structural_subtype(rows) == "TARGET_COLLISION"


def test_structural_subtype_detects_transition() -> None:
    rows = [fact(evidence_type="FISCAL_TRANSITION_EVIDENCE", value="VERIFIED_TRANSITION")]

    assert structural_subtype(rows) == "CALENDAR_TRANSITION"


def test_structural_subtype_detects_stub() -> None:
    rows = [fact(value="short stub reporting period")]

    assert structural_subtype(rows) == "STUB_PERIOD"


def test_structural_subtype_detects_restatement() -> None:
    rows = [fact(value="restatement vintage")]

    assert structural_subtype(rows) == "RESTATEMENT_VINTAGE"


def test_structural_subtype_detects_lineage() -> None:
    rows = [fact(evidence_type="LINEAGE_OWNERSHIP_EVIDENCE", value="lineage ownership")]

    assert structural_subtype(rows) == "LINEAGE_OWNERSHIP"


def test_structural_subtype_detects_source_period() -> None:
    rows = [fact(discrepancy="PERIOD_END_DIFFERENT")]

    assert structural_subtype(rows) == "SOURCE_PERIOD_OWNERSHIP"


def test_structural_subtype_detects_fiscal_boundary() -> None:
    rows = [fact(evidence_type="OFFICIAL_FY_FQ_IDENTITY", discrepancy="FY_FQ_DIFFERENT_AT_CURRENT_PERIOD")]

    assert structural_subtype(rows) == "FISCAL_IDENTITY_BOUNDARY"


def test_structural_analysis_uncertain_becomes_external() -> None:
    _analysis, _target, _transition, _lineage, final = analyze_structural_cases(
        [h4_structural("TEST")],
        {"TEST": [fact(evidence_type="OFFICIAL_FY_FQ_IDENTITY", status="UNCERTAIN", discrepancy="FY_FQ_DIFFERENT_AT_CURRENT_PERIOD")]},
        {"TEST": []},
    )

    assert final[0]["final_state"] == "MORE_EXTERNAL_EVIDENCE_REQUIRED"


def test_structural_analysis_verified_collision_stays_structural() -> None:
    _analysis, target, _transition, _lineage, final = analyze_structural_cases(
        [h4_structural("TEST")],
        {"TEST": [fact(evidence_type="TARGET_COLLISION_EVIDENCE", status="VERIFIED", discrepancy="PERIOD_END_DIFFERENT")]},
        {"TEST": []},
    )

    assert len(target) == 1
    assert final[0]["final_state"] == "STRUCTURAL_REVIEW_STILL_REQUIRED"


def test_frozen_repair_set_is_empty_for_non_deterministic_cases() -> None:
    assert build_frozen_repair_set([], []) == []


def test_repair_group_summary_handles_empty_set() -> None:
    assert repair_group_summary([]) == []


def test_rehearsal_copies_db_and_preserves_production(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)

    summary, apply_log, content, lineage, downstream, determinism = run_rehearsal(Phase8H5Paths(tmp_path / "art", v3_db=db, write_documentation=False), [])

    assert Path(summary["rehearsal_db"]).exists()
    assert apply_log == []
    assert content == []
    assert lineage == []
    assert downstream[0]["status"] == "SKIPPED_NO_REPAIR_SET"
    assert determinism["determinism_all_layers"] == "YES"
    assert summary["production_fingerprints_unchanged"] is True


def test_reassess_external_keeps_critical_gap() -> None:
    reassessed, queue, by_ticker = reassess_external([ext("A", "EBIT_DIRECT", "NOT_FOUND")], [])

    assert reassessed[0]["h5_external_decision"] == "KEEP_TRUE_EXTERNAL_FACT"
    assert queue[0]["ticker"] == "A"
    assert by_ticker[0]["fact_rows"] == 1


def test_reassess_external_removes_resolved_row() -> None:
    _reassessed, queue, _by_ticker = reassess_external([ext("A", "EBIT_DIRECT", "VERIFIED")], [])

    assert queue == []


def test_reassess_external_removes_secondary_only() -> None:
    reassessed, queue, _by_ticker = reassess_external([ext("A", "GROSS_PROFIT", "NOT_FOUND")], [])

    assert reassessed[0]["h5_external_decision"] == "REMOVE_SECONDARY_ONLY"
    assert queue == []


def test_reassess_external_removes_duplicate() -> None:
    row = ext("A", "EBIT_DIRECT", "NOT_FOUND")

    reassessed, queue, _by_ticker = reassess_external([row], [{"ticker": "A", "fiscal_year": "2026", "fiscal_quarter": "Q1", "evidence_type": "EBIT_DIRECT"}])

    assert reassessed[0]["h5_external_decision"] == "REMOVE_DUPLICATE_WAVE23"
    assert queue == []


def test_reassess_external_critical_set_contains_required_core_fields() -> None:
    assert {"EBIT_DIRECT", "TOTAL_DEBT", "FIRST_PUBLIC_PUBLISH_DATE", "OFFICIAL_FY_FQ_IDENTITY"} <= CRITICAL_EXTERNAL_EVIDENCE


def test_run_phase8h5_blocks_on_invalid_input(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)
    h4 = tmp_path / "h4"
    h3 = tmp_path / "h3"
    make_phase_inputs(h4, h3, [h4_local("L")], [], [], [])

    summary = run_phase8h5(Phase8H5Paths(tmp_path / "art", h4_root=h4, h3_root=h3, v3_db=db, write_documentation=False))

    assert summary["classification"] == "WAVE1_REMAINDER_LOCAL_STRUCTURAL_RESOLUTION_BLOCKED"


def test_run_phase8h5_writes_required_artifacts(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)
    h4 = tmp_path / "h4"
    h3 = tmp_path / "h3"
    local = [h4_local(f"L{i}") for i in range(3)]
    structural = [h4_structural(f"S{i}") for i in range(11)]
    external = [ext(f"E{i % 17}") for i in range(53)]
    verified = [fact(f"S{i}", "OFFICIAL_FY_FQ_IDENTITY", "UNCERTAIN", "FY_FQ_DIFFERENT_AT_CURRENT_PERIOD", "2026 Q2") for i in range(11)]
    make_phase_inputs(h4, h3, local, structural, external, verified)

    summary = run_phase8h5(Phase8H5Paths(tmp_path / "art", h4_root=h4, h3_root=h3, v3_db=db, write_documentation=False))

    assert summary["classification"] == CLASSIFICATION_REMAINING
    for name in [
        "h5_input_validation.json",
        "h5_local_case_analysis.csv",
        "h5_structural_case_analysis.csv",
        "wave1_h5_frozen_repair_set.csv",
        "h5_rehearsal_integrity.json",
        "wave1_final_external_followup_queue.csv",
        "phase8h5_summary.json",
        "next_action.md",
    ]:
        assert (tmp_path / "art" / name).exists()


def test_run_phase8h5_summary_counts_remaining(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    make_db(db)
    h4 = tmp_path / "h4"
    h3 = tmp_path / "h3"
    local = [h4_local(f"L{i}") for i in range(3)]
    structural = [h4_structural(f"S{i}") for i in range(11)]
    external = [ext(f"E{i % 17}") for i in range(53)]
    verified = [fact(f"S{i}", "OFFICIAL_FY_FQ_IDENTITY", "UNCERTAIN", "FY_FQ_DIFFERENT_AT_CURRENT_PERIOD", "2026 Q2") for i in range(11)]
    make_phase_inputs(h4, h3, local, structural, external, verified)

    summary = run_phase8h5(Phase8H5Paths(tmp_path / "art", h4_root=h4, h3_root=h3, v3_db=db, write_documentation=False))

    assert summary["remaining"]["local_cases"] == 3
    assert summary["remaining"]["structural_decisions"] == 11
    assert summary["remaining"]["external_fact_rows"] == 53
    assert summary["next_action"] == NEXT_LOCAL
