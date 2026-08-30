from __future__ import annotations

from pathlib import Path

from swingmaster.fundamentals.v3_phase8h_external_research_packaging import (
    build_fact_rows,
    source_semantic_subtypes,
)
from swingmaster.fundamentals.v3_phase8h2_dependency_root_cause import (
    CLASSIFICATION_STRUCTURAL,
    NEXT_FIXED,
    Phase8H2Paths,
    classify_audited_fact,
    clean_task,
    closure_rows,
    reclassify_wave,
    run_phase8h2,
    semantic_category,
    wave_delta,
)
from swingmaster.fundamentals.v3_phase8f_latest8q_gap_analysis import analyze_quarter


def task(**overrides: str) -> dict[str, str]:
    row = {
        "research_task_id": "P8H-3-0001",
        "ticker": "TEST",
        "fiscal_year": "2026",
        "fiscal_quarter": "Q1",
        "current_period_end": "2026-03-31",
        "current_publish_date": "2026-05-01",
        "priority": "P3_LATEST8Q",
        "evidence_types_needed": "OFFICIAL_FY_FQ_IDENTITY|SOURCE_SEMANTICS_CONFIRMATION|TOTAL_DEBT",
        "exact_information_needed": "Verify official fiscal year / fiscal quarter identity for FY2026 Q1; Verify source semantics confirmation for FY2026 Q1; Verify Total Debt for FY2026 Q1",
        "research_request": "Research official issuer sources.",
        "existing_local_evidence_summary": "",
        "structural_warning": "",
        "closure_dependency": "OFFICIAL_FY_FQ_IDENTITY|SOURCE_SEMANTICS_CONFIRMATION|TOTAL_DEBT",
        "fact_count": "3",
    }
    row.update(overrides)
    return row


def original(**overrides: str) -> dict[str, str]:
    row = task(
        exact_information_needed="FY2026Q1: need official fiscal year start and FY/FQ identity; FY2026Q1: confirm sequence semantics; FY2026Q1: need Debt"
    )
    row.update(overrides)
    return row


def exact_local(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "identity_basis": "DIRECT_EXACT_INTERVAL",
        "fq_confidence": "DIRECT_EXACT_FQ_HIGH",
        "period_end_structural_fit": "STRUCTURAL_FIT",
        "target_collision": "",
        "break_reason": "",
        "sequence_status": "SEQUENCE_OK",
        "exact_fy": "2026",
        "exact_fq": "Q1",
    }
    row.update(overrides)
    return row


def test_exact_adjacent_anchor_prevents_external_fy_fq_dependency() -> None:
    result = classify_audited_fact(task(), "OFFICIAL_FY_FQ_IDENTITY", exact_local(), original())

    assert result["keep_external"] == "NO"
    assert result["removal_reason"] == "FY_FQ_ALREADY_RESOLVED_BY_EXACT_ANCHOR_AND_LOCAL_FQ"


def test_high_confidence_local_fq_removes_external_fq_dependency() -> None:
    result = classify_audited_fact(task(), "OFFICIAL_FY_FQ_IDENTITY", exact_local(fq_confidence="DIRECT_EXACT_FQ_HIGH"), original())

    assert result["local_resolved_FQ"] == "Q1"
    assert result["keep_external"] == "NO"


def test_stored_wrong_fy_does_not_create_external_request_when_exact_anchor_resolves() -> None:
    row = task(fiscal_year="2025")
    local = exact_local(exact_fy="2026", exact_fq="Q1")

    result = classify_audited_fact(row, "OFFICIAL_FY_FQ_IDENTITY", local, original())

    assert result["local_resolved_FY"] == "2026"
    assert result["keep_external"] == "NO"


def test_old_seed_fy_mismatch_does_not_create_external_request() -> None:
    row = task(fiscal_year="2024", fiscal_quarter="Q4")
    local = exact_local(exact_fy="2025", exact_fq="Q1")

    result = classify_audited_fact(row, "OFFICIAL_FY_FQ_IDENTITY", local, original())

    assert result["local_resolved_FY"] == "2025"
    assert result["local_resolved_FQ"] == "Q1"
    assert result["keep_external"] == "NO"


def test_unresolved_sequence_alone_does_not_emit_source_semantics_in_phase8f() -> None:
    row = {
        "ticker": "TEST",
        "company_id": 1,
        "quarter_id": 1,
        "fiscal_year": 2026,
        "fiscal_quarter": "Q1",
        "period_end": "2026-03-31",
        "publish_date": "2026-05-01",
        "quarter_position_latest8q": 1,
        "identity_class": "PASS_DIRECT_EXACT",
        "identity_basis": "DIRECT_EXACT_INTERVAL",
        "period_end_structural_fit": "STRUCTURAL_FIT",
        "target_collision": "",
        "calendar_type": "CALENDAR_YEAR",
        "local_source_hint": "v3_company_fiscal_year_calendar",
        "revenue": 1,
        "ebit": 1,
        "free_cashflow": 1,
        "cash": 1,
        "total_debt": 1,
        "shares_outstanding": 1,
        "gross_profit": 1,
        "operating_income": 1,
        "ebitda": 1,
        "net_income": 1,
        "operating_cashflow": 1,
        "capex": 1,
    }

    result = analyze_quarter(row, {"sequence_status": "UNRESOLVED_SEQUENCE"}, {})

    assert "NEED_SOURCE_SEMANTICS_CONFIRMATION" not in result["evidence_needed_codes"]


def test_generic_source_semantics_without_subtype_is_prohibited() -> None:
    result = classify_audited_fact(task(evidence_types_needed="SOURCE_SEMANTICS_CONFIRMATION"), "SOURCE_SEMANTICS_CONFIRMATION", exact_local(), original())

    assert result["semantic_category"] == "SEQUENCE_ONLY"
    assert result["keep_external"] == "NO"


def test_ytd_vs_discrete_keeps_semantics() -> None:
    row = {"exact_information_needed": "Confirm YTD versus discrete field scope"}

    assert source_semantic_subtypes(row) == ["YTD_VS_DISCRETE", "FIELD_SCOPE_AMBIGUITY"]


def test_debt_definition_keeps_semantics() -> None:
    assert source_semantic_subtypes({"exact_information_needed": "Confirm debt definition and debt scope"}) == ["DEBT_DEFINITION"]


def test_shares_semantics_keep_semantics() -> None:
    assert source_semantic_subtypes({"exact_information_needed": "Confirm shares period-end versus weighted average"}) == [
        "SHARES_PERIOD_END_VS_WEIGHTED_AVERAGE"
    ]


def test_restatement_vintage_keeps_semantics() -> None:
    assert source_semantic_subtypes({"exact_information_needed": "Confirm restatement vintage"}) == ["RESTATEMENT_VINTAGE"]


def test_structural_collision_does_not_force_redundant_fy_fq_request() -> None:
    row = task(structural_warning="STRUCTURAL_REVIEW_ALSO_REQUIRED")
    result = classify_audited_fact(row, "OFFICIAL_FY_FQ_IDENTITY", exact_local(target_collision="TARGET_EMPTY"), original())

    assert result["keep_external"] == "NO"


def test_transition_can_retain_identity_request() -> None:
    result = classify_audited_fact(task(), "OFFICIAL_FY_FQ_IDENTITY", exact_local(break_reason="CALENDAR_TRANSITION"), original())

    assert result["keep_external"] == "YES"


def test_unresolved_boundary_can_retain_identity_request() -> None:
    result = classify_audited_fact(
        task(),
        "OFFICIAL_FY_FQ_IDENTITY",
        exact_local(identity_basis="UNRESOLVED", break_reason="UNRESOLVED_BOUNDARY"),
        original(),
    )

    assert result["keep_external"] == "YES"


def test_true_missing_quarter_request_is_retained_by_non_audited_fact_cleaner() -> None:
    row = task(evidence_types_needed="MISSING_QUARTER_EXISTENCE|TOTAL_DEBT")
    clean = clean_task(row, set(), original())

    assert clean is not None
    assert "MISSING_QUARTER_EXISTENCE" in clean["evidence_types_needed"]


def test_previously_removed_missing_quarter_request_is_not_reintroduced() -> None:
    row = task(evidence_types_needed="TOTAL_DEBT")
    clean = clean_task(row, set(), original())

    assert clean is not None
    assert "MISSING_QUARTER_EXISTENCE" not in clean["evidence_types_needed"]


def test_build_fact_rows_skips_generic_source_semantics() -> None:
    raw, dedup = build_fact_rows(
        [
            {
                **task(closure_dependency="NEED_SOURCE_SEMANTICS_CONFIRMATION", exact_information_needed="FY2026Q1: confirm sequence semantics"),
                "source_row_id": 1,
            }
        ]
    )

    assert raw == []
    assert dedup == []


def test_build_fact_rows_keeps_subtyped_source_semantics() -> None:
    raw, dedup = build_fact_rows(
        [
            {
                **task(closure_dependency="NEED_SOURCE_SEMANTICS_CONFIRMATION", exact_information_needed="Need approved issuer/company-specific EBIT rule"),
                "source_row_id": 1,
            }
        ]
    )

    assert raw[0]["semantic_subtype"] == "EBIT_COMPONENT_SEMANTICS"
    assert dedup[0]["evidence_type"] == "SOURCE_SEMANTICS_CONFIRMATION"


def test_wave2_reclassification_removes_generic_semantics_and_rewrites_task_text() -> None:
    row = task(evidence_types_needed="SOURCE_SEMANTICS_CONFIRMATION|TOTAL_DEBT")
    audit, cleaned = reclassify_wave([row], {("TEST", "2026", "Q1", "2026-03-31"): exact_local()}, {("TEST", "2026", "Q1", "2026-03-31"): original()})

    assert audit[0]["keep_external"] == "NO"
    assert cleaned[0]["evidence_types_needed"] == "TOTAL_DEBT"
    assert "source semantics" not in cleaned[0]["exact_information_needed"].lower()


def test_wave_delta_reports_semantics_removal() -> None:
    row = task(evidence_types_needed="SOURCE_SEMANTICS_CONFIRMATION|TOTAL_DEBT")
    audit, cleaned = reclassify_wave([row], {("TEST", "2026", "Q1", "2026-03-31"): exact_local()}, {("TEST", "2026", "Q1", "2026-03-31"): original()})
    delta = wave_delta([row], cleaned, audit)

    assert delta["semantics_removed"] == 1
    assert delta["facts_removed"] == 1


def test_closure_remains_complete_for_h1_locally_closed_ticker() -> None:
    rows = closure_rows(
        [{"ticker": "TEST", "closure_completeness": "YES_EXTERNAL_ONLY"}],
        [{"ticker": "TEST", "cleaned_closure_status": "COMPLETE_CLOSURE_PATH"}],
        [],
        [],
        [],
        set(),
    )

    assert rows[0]["rootcause_closure_status"] == "COMPLETE_CLOSURE_PATH"


def test_semantic_category_sequence_only_without_subtype() -> None:
    assert semantic_category("confirm sequence semantics", []) == "SEQUENCE_ONLY"


def test_semantic_category_true_semantics_with_subtype() -> None:
    assert semantic_category("Need OCF and Capex to derive FCF", ["FCF_SEMANTICS"]) == "TRUE_SEMANTIC_AMBIGUITY"


def test_phase8h2_integration_regenerates_wave2_and_wave3(tmp_path: Path) -> None:
    summary = run_phase8h2(
        Phase8H2Paths(
            artifact_root=tmp_path,
            phase8h_root=Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H"),
            phase8h1_root=Path("temp/fundamentals_v3_phase8h1_wave23_cleanup/20260829T_PHASE8H1"),
            v3_db=Path("rc_fundamentals_v3.db"),
            write_documentation=False,
        )
    )

    assert summary["classification"] == CLASSIFICATION_STRUCTURAL
    assert summary["wave2"]["before_facts"] == 533
    assert summary["wave2"]["new_facts"] == 530
    assert summary["wave3"]["before_facts"] == 6073
    assert summary["wave3"]["new_facts"] == 6068
    assert (tmp_path / "latest8q_external_research_wave2_p2_latest4q_rootcause_cleaned.csv").exists()
    assert (tmp_path / "latest8q_external_research_wave3_p3_latest8q_rootcause_cleaned.csv").exists()


def test_phase8h2_integration_wave1_file_unchanged_and_diagnostic_recorded(tmp_path: Path) -> None:
    wave1_path = Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H/latest8q_external_research_wave1_p1_current.csv")
    before = wave1_path.read_bytes()
    summary = run_phase8h2(
        Phase8H2Paths(
            artifact_root=tmp_path,
            phase8h_root=Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H"),
            phase8h1_root=Path("temp/fundamentals_v3_phase8h1_wave23_cleanup/20260829T_PHASE8H1"),
            v3_db=Path("rc_fundamentals_v3.db"),
            write_documentation=False,
        )
    )

    assert wave1_path.read_bytes() == before
    assert summary["wave1_diagnostic"]["wave1_file_unchanged"] == "YES"
    assert summary["wave1_diagnostic"]["source_semantics_facts_that_would_disappear"] == 339


def test_phase8h2_integration_closure_and_safety(tmp_path: Path) -> None:
    summary = run_phase8h2(
        Phase8H2Paths(
            artifact_root=tmp_path,
            phase8h_root=Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H"),
            phase8h1_root=Path("temp/fundamentals_v3_phase8h1_wave23_cleanup/20260829T_PHASE8H1"),
            v3_db=Path("rc_fundamentals_v3.db"),
            write_documentation=False,
        )
    )

    assert summary["closure"]["MISSING_REQUIREMENT"] == 0
    assert summary["safety"]["production_writes"] == 0
    assert summary["safety"]["network_calls"] == 0
    assert summary["safety"]["downstream_writes"] == 0
    assert summary["safety"]["rawcandle_writes"] == 0
    assert summary["safety"]["active_guard_changes"] == 0
    assert summary["next_action"] == NEXT_FIXED
