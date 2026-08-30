from __future__ import annotations

from pathlib import Path

from swingmaster.fundamentals.v3_phase8g_local_latest8q_repairs import (
    CLASSIFICATION_REMAINING,
    Phase8GPaths,
    closure_test,
    critical_evidence_codes,
    critical_issue_codes,
    is_material_structural,
    material_problem_rows,
    missing_quarter_reclassification,
    run_phase8g,
    secondary_gap_reclassification,
)


def base_problem(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "TEST",
        "quarter_id": "1",
        "fiscal_year": "2026",
        "fiscal_quarter": "Q1",
        "period_end": "2026-03-31",
        "issue_codes": "",
        "evidence_needed_codes": "",
        "missing_core_fields": "",
        "missing_noncore_fields": "",
        "derivation_missing_inputs": "",
        "external_research_required": "NO",
        "current_ttm_impact": "NO",
        "score_impact": "NO",
        "lifecycle_impact": "NO",
        "valuation_impact": "NO",
        "target_collision": "",
        "priority": "P3_LATEST8Q",
        "repair_complexity": "NO_REPAIR_NEEDED",
        "sequence_status": "SEQUENCE_CLEAN",
        "fiscal_identity_status": "PASS_DIRECT_EXACT",
    }
    row.update(overrides)
    return row


def test_secondary_only_gaps_are_not_downstream_critical() -> None:
    row = base_problem(issue_codes="SECONDARY_FIELDS_INCOMPLETE", missing_noncore_fields="Gross Profit|EBITDA|Net Income")

    assert critical_issue_codes(row) == []
    assert critical_evidence_codes(row) == []
    reclass = secondary_gap_reclassification([row])

    assert {item["field"] for item in reclass} == {"Gross Profit", "EBITDA", "Net Income"}
    assert {item["downstream_critical"] for item in reclass} == {"NO"}


def test_ocf_capex_and_operating_income_are_critical_only_as_derivation_inputs() -> None:
    fcf_row = base_problem(
        issue_codes="PRIMARY_CORE_INCOMPLETE",
        evidence_needed_codes="NEED_OCF|NEED_CAPEX",
        missing_core_fields="FCF",
        missing_noncore_fields="OCF|Capex",
    )
    ebit_row = base_problem(
        issue_codes="PRIMARY_CORE_INCOMPLETE",
        evidence_needed_codes="NEED_OPERATING_INCOME",
        missing_core_fields="EBIT",
        missing_noncore_fields="Operating Income",
    )

    reclass = secondary_gap_reclassification([fcf_row, ebit_row])

    assert {item["field"] for item in reclass if item["downstream_critical"] == "YES"} == {
        "OCF",
        "Capex",
        "Operating Income",
    }
    assert critical_evidence_codes(fcf_row) == ["NEED_CAPEX", "NEED_OCF"]


def test_ebit_requires_source_semantics_not_operating_income_aliasing() -> None:
    row = base_problem(
        issue_codes="PRIMARY_CORE_INCOMPLETE",
        evidence_needed_codes="NEED_OPERATING_INCOME",
        missing_core_fields="EBIT",
        derivation_missing_inputs="missing approved issuer/company-specific EBIT rule",
    )

    assert critical_evidence_codes(row) == ["NEED_SOURCE_SEMANTICS_CONFIRMATION"]


def test_false_missing_quarter_without_downstream_impact_is_reclassified_out_of_material_scope() -> None:
    row = base_problem(issue_codes="MISSING_QUARTER", external_research_required="NO")

    assert material_problem_rows([row]) == []


def test_missing_quarter_reclassification_distinguishes_external_local_and_transition_paths() -> None:
    rows = material_problem_rows(
        [
            base_problem(issue_codes="MISSING_QUARTER", external_research_required="YES"),
            base_problem(issue_codes="MISSING_QUARTER", external_research_required="NO", current_ttm_impact="YES"),
            base_problem(issue_codes="MISSING_QUARTER", external_research_required="NO", sequence_status="TRANSITION_SEQUENCE"),
        ]
    )

    assert [row["phase8g_missing_quarter_class"] for row in missing_quarter_reclassification(rows)] == [
        "TRUE_MISSING_EXTERNAL_EVIDENCE_REQUIRED",
        "TRUE_MISSING_LOCAL_EVIDENCE_AVAILABLE",
        "TRANSITION_OR_STUB",
    ]


def test_non_auto_relabel_local_identity_conflicts_are_structural_review_not_local_blockers() -> None:
    row = base_problem(
        issue_codes="FY_CONFLICT_DIRECT_EXACT",
        evidence_needed_codes="NEED_LOCAL_LINEAGE_RECONCILIATION",
        fiscal_identity_status="FY_CONFLICT_DIRECT_EXACT",
        external_research_required="NO",
    )
    material = material_problem_rows([row])

    assert len(material) == 1
    assert is_material_structural(material[0]) is True


def test_closure_test_accepts_clean_external_and_structural_paths() -> None:
    rows = [
        {"ticker": "A", "remaining_status": "DOWNSTREAM_LATEST8Q_CLEAN"},
        {"ticker": "B", "remaining_status": "DOWNSTREAM_LATEST8Q_EXTERNAL_EVIDENCE_REQUIRED"},
        {"ticker": "C", "remaining_status": "DOWNSTREAM_LATEST8Q_STRUCTURAL_REVIEW"},
    ]

    assert [row["theoretical_downstream_closure"] for row in closure_test(rows)] == [
        "YES",
        "YES_AFTER_EXTERNAL",
        "YES_AFTER_STRUCTURAL",
    ]


def test_phase8g_final_readonly_artifact_contract(tmp_path: Path) -> None:
    summary = run_phase8g(
        Phase8GPaths(
            artifact_root=tmp_path / "phase8g",
            v3_db=Path("rc_fundamentals_v3.db"),
            osakedata_db=Path("/home/kalle/projects/rawcandle/data/osakedata.db"),
            apply_production=False,
            write_documentation=False,
        )
    )

    assert summary["classification"] == CLASSIFICATION_REMAINING
    assert summary["local_critical_repair"]["local_candidates"] == 1
    assert summary["full_downstream_closure"]["NO_MISSING_REQUIREMENT"] == 0
    assert summary["safety"]["rawcandle_writes"] == 0
    assert summary["safety"]["active_guard_changes"] == 0
    assert (tmp_path / "phase8g" / "latest8q_downstream_ticker_status.csv").exists()
    assert (tmp_path / "phase8g" / "latest8q_downstream_external_research_queue.csv").exists()
    assert (tmp_path / "phase8g" / "latest8q_downstream_structural_review_queue.csv").exists()
    assert (tmp_path / "phase8g" / "latest8q_downstream_local_remaining_queue.csv").exists()
