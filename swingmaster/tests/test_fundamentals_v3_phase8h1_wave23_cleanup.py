from __future__ import annotations

from pathlib import Path

from swingmaster.fundamentals.v3_phase8h1_wave23_cleanup import (
    CLASSIFICATION_STRUCTURAL,
    NEXT_COMPLETE,
    Phase8H1Paths,
    build_ticker_summary,
    classify_fact,
    clean_tasks,
    closure_test,
    known_13_rows,
    local_status,
    reclassify_wave_tasks,
    rewrite_exact_information,
    run_phase8h1,
    source_semantics_is_only_identity,
    structural_boundary,
    wave_summary,
)


def task(**overrides: str) -> dict[str, str]:
    row = {
        "research_task_id": "TEST_2026_Q1",
        "ticker": "TEST",
        "fiscal_year": "2026",
        "fiscal_quarter": "Q1",
        "current_period_end": "2026-03-31",
        "current_publish_date": "2026-05-01",
        "priority": "P3_LATEST8Q",
        "evidence_types_needed": "OFFICIAL_FY_FQ_IDENTITY",
        "exact_information_needed": "Verify sequence semantics for FY2026 Q1",
        "research_request": "Research official issuer sources for FY2026 Q1.",
        "preferred_source_type": "official issuer source",
        "closure_dependency": "OFFICIAL_FY_FQ_IDENTITY",
        "existing_local_evidence_summary": "",
        "structural_warning": "",
    }
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


def test_exact_anchor_resolves_local_status() -> None:
    status = local_status(task(), exact_local())

    assert status["local_fy_status"] == "FY_RESOLVED_DIRECT_EXACT"
    assert status["local_fq_status"] == "FQ_RESOLVED_LOCAL_HIGH"
    assert status["structural_boundary_status"] == "NO_STRUCTURAL_BOUNDARY"


def test_missing_local_row_keeps_unresolved_status() -> None:
    status = local_status(task(), None)

    assert status["exact_anchor_coverage"] == "NO_LOCAL_ROW_MATCH"
    assert status["local_fy_status"] == "FY_UNRESOLVED"
    assert status["local_fq_status"] == "FQ_UNRESOLVED"


def test_structural_warning_marks_boundary() -> None:
    assert structural_boundary(task(structural_warning="TARGET_COLLISION"), exact_local())


def test_calendar_transition_marks_boundary() -> None:
    assert structural_boundary(task(), exact_local(break_reason="CALENDAR_TRANSITION"))


def test_target_collision_marks_boundary() -> None:
    assert structural_boundary(task(), exact_local(target_collision="TARGET_CONFLICTING"))


def test_official_fy_fq_identity_removed_when_exact_anchor_and_fq_are_resolved() -> None:
    result = classify_fact(task(), "OFFICIAL_FY_FQ_IDENTITY", exact_local())

    assert result["keep_external"] == "NO"
    assert result["removal_reason"] == "FY_FQ_ALREADY_RESOLVED"


def test_official_fy_fq_identity_removed_when_only_fy_is_direct_exact() -> None:
    result = classify_fact(task(), "OFFICIAL_FY_FQ_IDENTITY", exact_local(fq_confidence="LOW"))

    assert result["keep_external"] == "NO"
    assert result["removal_reason"] == "FY_ALREADY_RESOLVED_DIRECT_EXACT"


def test_unresolved_boundary_retains_fiscal_identity_request() -> None:
    result = classify_fact(task(structural_warning="UNRESOLVED_BOUNDARY"), "OFFICIAL_FY_FQ_IDENTITY", exact_local())

    assert result["keep_external"] == "YES"


def test_identity_only_source_semantics_text_is_detected() -> None:
    assert source_semantics_is_only_identity(task(exact_information_needed="Confirm sequence semantics; confirm stored FY/FQ mapping"))


def test_value_semantics_text_is_not_identity_only() -> None:
    row = task(exact_information_needed="Confirm YTD versus discrete source semantics; verify revenue")

    assert not source_semantics_is_only_identity(row)


def test_source_semantics_removed_only_when_identity_semantics_are_locally_resolved() -> None:
    row = task(evidence_types_needed="SOURCE_SEMANTICS_CONFIRMATION", exact_information_needed="Confirm sequence semantics")
    result = classify_fact(row, "SOURCE_SEMANTICS_CONFIRMATION", exact_local())

    assert result["keep_external"] == "NO"
    assert result["removal_reason"] == "SOURCE_SEMANTICS_ALREADY_RESOLVED"


def test_source_semantics_retained_for_ytd_or_discrete_value_semantics() -> None:
    row = task(evidence_types_needed="SOURCE_SEMANTICS_CONFIRMATION", exact_information_needed="Confirm YTD/discrete source semantics")
    result = classify_fact(row, "SOURCE_SEMANTICS_CONFIRMATION", exact_local())

    assert result["keep_external"] == "YES"


def test_missing_quarter_removed_when_existing_quarter_is_locally_identified() -> None:
    row = task(fiscal_year="2025", fiscal_quarter="Q4", evidence_types_needed="MISSING_QUARTER_EXISTENCE")
    result = classify_fact(row, "MISSING_QUARTER_EXISTENCE", exact_local(exact_fy="2026", exact_fq="Q1"))

    assert result["keep_external"] == "NO"
    assert result["removal_reason"] == "MISSING_QUARTER_ALREADY_IDENTIFIED_LOCALLY"


def test_missing_quarter_removed_when_same_quarter_is_locally_resolved() -> None:
    row = task(evidence_types_needed="MISSING_QUARTER_EXISTENCE")
    result = classify_fact(row, "MISSING_QUARTER_EXISTENCE", exact_local())

    assert result["keep_external"] == "NO"


def test_missing_quarter_retained_when_boundary_requires_review() -> None:
    row = task(evidence_types_needed="MISSING_QUARTER_EXISTENCE", structural_warning="TRANSITION")
    result = classify_fact(row, "MISSING_QUARTER_EXISTENCE", exact_local())

    assert result["keep_external"] == "YES"


def test_value_fact_is_retained_when_fiscal_identity_fact_is_removed() -> None:
    row = task(evidence_types_needed="OFFICIAL_FY_FQ_IDENTITY|TOTAL_DEBT")
    local = {("TEST", "2026", "Q1", "2026-03-31"): exact_local()}

    results = reclassify_wave_tasks([row], local)

    assert [r["evidence_type"] for r in results if r["keep_external"] == "YES"] == ["TOTAL_DEBT"]
    assert [r["evidence_type"] for r in results if r["keep_external"] == "NO"] == ["OFFICIAL_FY_FQ_IDENTITY"]


def test_clean_task_rewrites_retained_requirements_without_removed_fy_fq_request() -> None:
    row = task(evidence_types_needed="OFFICIAL_FY_FQ_IDENTITY|TOTAL_DEBT")
    local = {("TEST", "2026", "Q1", "2026-03-31"): exact_local()}
    reclass = reclassify_wave_tasks([row], local)
    cleaned = clean_tasks([row], reclass)

    assert cleaned[0]["evidence_types_needed"] == "TOTAL_DEBT"
    assert "fiscal" not in cleaned[0]["exact_information_needed"].lower()
    assert "Do not research other fields" in cleaned[0]["research_request"]


def test_rewrite_exact_information_is_bounded_to_retained_facts() -> None:
    text = rewrite_exact_information(task(), ["TOTAL_DEBT", "REVENUE"])

    assert "total debt" in text.lower()
    assert "revenue" in text.lower()
    assert "sequence" not in text.lower()


def test_wave_summary_counts_removed_by_type() -> None:
    old = [task(evidence_types_needed="MISSING_QUARTER_EXISTENCE|SOURCE_SEMANTICS_CONFIRMATION")]
    reclass = [
        {**classify_fact(old[0], "MISSING_QUARTER_EXISTENCE", exact_local()), "priority": "P3_LATEST8Q"},
        {**classify_fact(old[0], "SOURCE_SEMANTICS_CONFIRMATION", exact_local()), "priority": "P3_LATEST8Q"},
    ]

    summary = wave_summary(old, [], reclass)

    assert summary["facts_removed"] == 2
    assert summary["missing_quarter_requests_removed"] == 1
    assert summary["source_semantics_requests_removed"] == 1


def test_ticker_summary_marks_locally_closed_ticker() -> None:
    row = task(evidence_types_needed="MISSING_QUARTER_EXISTENCE")
    reclass = [classify_fact(row, "MISSING_QUARTER_EXISTENCE", exact_local())]
    summary = build_ticker_summary([row], [], reclass, set())

    assert summary[0]["new_wave"] == "NO_EXTERNAL_RESEARCH_NEEDED"
    assert summary[0]["missing_quarter_requests_removed"] == 1


def test_closure_allows_locally_closed_tickers() -> None:
    closure = [{"ticker": "TEST", "closure_completeness": "YES_EXTERNAL_ONLY"}]
    rows = closure_test(closure, set(), set(), {"TEST"})

    assert rows[0]["cleaned_closure_status"] == "COMPLETE_CLOSURE_PATH"
    assert rows[0]["missing_requirement"] == 0


def test_closure_flags_missing_when_not_cleaned_or_structural() -> None:
    closure = [{"ticker": "TEST", "closure_completeness": "YES_EXTERNAL_ONLY"}]
    rows = closure_test(closure, set(), set(), set())

    assert rows[0]["cleaned_closure_status"] == "MISSING_REQUIREMENT"
    assert rows[0]["missing_requirement"] == 1


def test_known_13_preserves_structural_dependency_reporting() -> None:
    rows = [{"ticker": "VTGN", "new_wave": "P2_LATEST4Q", "fy_fq_requests_removed": 0, "remaining_facts": "TOTAL_DEBT"}]
    known = known_13_rows(rows, {"VTGN"})
    vtgn = next(row for row in known if row["ticker"] == "VTGN")

    assert vtgn["current_wave"] == "P2_LATEST4Q"
    assert vtgn["remaining_structural_decision"] == "YES"


def test_phase8h1_integration_produces_expected_wave_cleanup_counts(tmp_path: Path) -> None:
    summary = run_phase8h1(
        Phase8H1Paths(
            artifact_root=tmp_path,
            phase8h_root=Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H"),
            v3_db=Path("rc_fundamentals_v3.db"),
            write_documentation=False,
        )
    )

    assert summary["classification"] == CLASSIFICATION_STRUCTURAL
    assert summary["wave2"]["old_facts"] == 548
    assert summary["wave2"]["new_facts"] == 533
    assert summary["wave3"]["old_facts"] == 7086
    assert summary["wave3"]["new_facts"] == 6073
    assert summary["combined"]["total_facts_removed"] == 1028
    assert summary["combined"]["remaining_facts"] == 6606
    assert summary["closure"]["MISSING_REQUIREMENT"] == 0


def test_phase8h1_integration_writes_cleaned_wave_files(tmp_path: Path) -> None:
    run_phase8h1(
        Phase8H1Paths(
            artifact_root=tmp_path,
            phase8h_root=Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H"),
            v3_db=Path("rc_fundamentals_v3.db"),
            write_documentation=False,
        )
    )

    assert (tmp_path / "latest8q_external_research_wave2_p2_latest4q_cleaned.csv").exists()
    assert (tmp_path / "latest8q_external_research_wave3_p3_latest8q_cleaned.csv").exists()
    assert (tmp_path / "wave23_removed_external_requests.csv").exists()


def test_phase8h1_integration_records_no_forbidden_actions(tmp_path: Path) -> None:
    summary = run_phase8h1(
        Phase8H1Paths(
            artifact_root=tmp_path,
            phase8h_root=Path("temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H"),
            v3_db=Path("rc_fundamentals_v3.db"),
            write_documentation=False,
        )
    )

    assert summary["safety"]["production_writes"] == 0
    assert summary["safety"]["network_calls"] == 0
    assert summary["safety"]["rawcandle_writes"] == 0
    assert summary["safety"]["guard_changes"] == 0
    assert summary["next_action"] == NEXT_COMPLETE
