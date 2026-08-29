from __future__ import annotations

import csv
from pathlib import Path

from swingmaster.fundamentals.v3_phase8h_external_research_packaging import (
    CLASSIFICATION_STRUCTURAL,
    EXPECTED_P1_TICKERS,
    Phase8HPaths,
    build_fact_rows,
    build_research_tasks,
    build_ticker_summary,
    clean_structural_rows,
    closure_rows,
    first_batch,
    normalized_evidence_types,
    reclassify_external_rows,
    run_phase8h,
    sanitized_exact_information,
    wave_rows,
)


def external_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "TEST",
        "fiscal_year": "2026",
        "fiscal_quarter": "Q1",
        "current_period_end": "2026-03-31",
        "current_publish_date": "2026-05-01",
        "issue": "PRIMARY_CORE_INCOMPLETE",
        "exact_information_needed": "FY2026Q1: need EBIT",
        "preferred_source_type": "official issuer source",
        "downstream_impact": "current_ttm_impact|score_impact",
        "priority": "P1_CURRENT",
        "structural_context": "CALENDAR_YEAR",
        "warning": "",
        "closure_dependency": "NEED_EBIT",
    }
    row.update(overrides)
    return row


def test_phase8g_queue_codes_are_normalized_to_stable_evidence_types() -> None:
    assert normalized_evidence_types("NEED_DEBT|NEED_FIRST_PUBLIC_RESULT_DATE|NEED_OFFICIAL_FISCAL_YEAR_START") == [
        "TOTAL_DEBT",
        "FIRST_PUBLIC_PUBLISH_DATE",
        "OFFICIAL_FY_FQ_IDENTITY",
    ]


def test_secondary_only_gross_profit_ebitda_and_ni_are_removed() -> None:
    rows = [
        external_row(ticker="GP", closure_dependency="NEED_GROSS_PROFIT"),
        external_row(ticker="EBITDA", closure_dependency="NEED_EBITDA"),
        external_row(ticker="NI", closure_dependency="NEED_NET_INCOME"),
    ]

    reclass, removed = reclassify_external_rows(rows)

    assert len(removed) == 3
    assert {row["status"] for row in reclass} == {"REMOVED"}


def test_gross_profit_only_removed_with_secondary_reason() -> None:
    _reclass, removed = reclassify_external_rows([external_row(closure_dependency="NEED_GROSS_PROFIT")])

    assert removed[0]["removal_reason"] == "SECONDARY_FIELD_ONLY"


def test_ebitda_only_removed_with_secondary_reason() -> None:
    _reclass, removed = reclassify_external_rows([external_row(closure_dependency="NEED_EBITDA")])

    assert removed[0]["removal_reason"] == "SECONDARY_FIELD_ONLY"


def test_net_income_only_removed_with_secondary_reason() -> None:
    _reclass, removed = reclassify_external_rows([external_row(closure_dependency="NEED_NET_INCOME")])

    assert removed[0]["removal_reason"] == "SECONDARY_FIELD_ONLY"


def test_operating_income_only_removed_when_not_an_ebit_requirement() -> None:
    _reclass, removed = reclassify_external_rows(
        [external_row(closure_dependency="NEED_OPERATING_INCOME", exact_information_needed="FY2026Q1: need Operating Income")]
    )

    assert removed[0]["removal_reason"] == "SECONDARY_FIELD_ONLY"


def test_operating_income_retained_for_approved_ebit_component_requirement() -> None:
    reclass, removed = reclassify_external_rows(
        [
            external_row(
                closure_dependency="NEED_OPERATING_INCOME",
                exact_information_needed="FY2026Q1: need Operating Income for approved EBIT derivation",
            )
        ]
    )

    assert removed == []
    assert reclass[0]["normalized_evidence_types"] == "EBIT_COMPONENTS"


def test_ocf_only_removed_when_fcf_is_not_the_target() -> None:
    _reclass, removed = reclassify_external_rows([external_row(closure_dependency="NEED_OCF", exact_information_needed="FY2026Q1: need OCF")])

    assert removed[0]["removal_reason"] == "SECONDARY_FIELD_ONLY"


def test_capex_only_removed_when_fcf_is_not_the_target() -> None:
    _reclass, removed = reclassify_external_rows([external_row(closure_dependency="NEED_CAPEX", exact_information_needed="FY2026Q1: need Capex")])

    assert removed[0]["removal_reason"] == "SECONDARY_FIELD_ONLY"


def test_ocf_retained_for_fcf_derivation_requirement() -> None:
    reclass, removed = reclassify_external_rows([external_row(closure_dependency="NEED_OCF", exact_information_needed="FY2026Q1: Need OCF to derive FCF")])

    assert removed == []
    assert reclass[0]["normalized_evidence_types"] == "OCF_FOR_FCF"


def test_capex_retained_for_fcf_derivation_requirement() -> None:
    reclass, removed = reclassify_external_rows([external_row(closure_dependency="NEED_CAPEX", exact_information_needed="FY2026Q1: Need Capex to derive FCF")])

    assert removed == []
    assert reclass[0]["normalized_evidence_types"] == "CAPEX_FOR_FCF"


def test_oi_ocf_and_capex_text_is_removed_when_not_required() -> None:
    text = "FY2026Q1: need Operating Income; FY2026Q1: need OCF; FY2026Q1: need Capex; FY2026Q1: need Revenue"

    assert sanitized_exact_information([text], ["REVENUE"]) == "FY2026Q1: need Revenue"


def test_ocf_and_capex_are_retained_when_required_for_fcf() -> None:
    text = "FY2026Q1: need OCF; FY2026Q1: need Capex"

    assert sanitized_exact_information([text], ["OCF_FOR_FCF", "CAPEX_FOR_FCF"]) == text


def test_ebit_components_are_retained_only_when_required() -> None:
    text = "FY2026Q1: need Operating Income; FY2026Q1: confirm sequence semantics"

    assert "Operating Income" not in sanitized_exact_information([text], ["SOURCE_SEMANTICS_CONFIRMATION"])
    assert "Operating Income" in sanitized_exact_information([text], ["EBIT_COMPONENTS"])


def test_ticker_fy_fq_evidence_dedup_collapses_duplicate_fiscal_identity_fact() -> None:
    rows = [
        external_row(closure_dependency="NEED_OFFICIAL_FISCAL_YEAR_START|NEED_OFFICIAL_FY_FQ_IDENTITY"),
    ]

    _raw, facts = build_fact_rows([{**rows[0], "source_row_id": 1}])

    assert len(facts) == 1
    assert facts[0]["evidence_type"] == "OFFICIAL_FY_FQ_IDENTITY"
    assert facts[0]["duplicate_source_rows"] == 2


def test_multi_fact_quarter_task_is_consolidated_without_overbroad_request() -> None:
    rows = [
        {**external_row(closure_dependency="NEED_REVENUE|NEED_EBIT|NEED_DEBT"), "source_row_id": 1},
    ]
    _raw, facts = build_fact_rows(rows)
    tasks = build_research_tasks(
        rows,
        facts,
        {"TEST": {"company_id": "1"}},
        set(),
        {("TEST", "2026", "Q1", "2026-03-31"): {"quarter_position_latest8q": 1}},
    )

    assert len(tasks) == 1
    assert tasks[0]["fact_count"] == 3
    assert "Do not research other fields" in tasks[0]["research_request"]
    assert "all financials" not in tasks[0]["research_request"].lower()


def test_wave_assignment_and_no_task_in_multiple_waves() -> None:
    rows = [
        {**external_row(ticker="P1", priority="P1_CURRENT"), "source_row_id": 1},
        {**external_row(ticker="P2", priority="P2_LATEST4Q"), "source_row_id": 2},
        {**external_row(ticker="P3", priority="P3_LATEST8Q"), "source_row_id": 3},
    ]
    _raw, facts = build_fact_rows(rows)
    tasks = build_research_tasks(rows, facts, {}, set(), {})

    assert len(wave_rows(tasks, "P1_CURRENT")) == 1
    assert len(wave_rows(tasks, "P2_LATEST4Q")) == 1
    assert len(wave_rows(tasks, "P3_LATEST8Q")) == 1
    assert sum(len(wave_rows(tasks, wave)) for wave in ("P1_CURRENT", "P2_LATEST4Q", "P3_LATEST8Q")) == len(tasks)


def test_wave1_sort_prefers_current_ttm_before_other_tasks() -> None:
    current = {"research_task_id": "A", "ticker": "A", "fiscal_year": "2026", "fiscal_quarter": "Q1", "priority": "P1_CURRENT", "quarter_position_latest8q": 2, "current_ttm_impact": "YES", "downstream_layer_count": 1, "fact_count": 1}
    other = {**current, "research_task_id": "B", "ticker": "B", "current_ttm_impact": "NO", "quarter_position_latest8q": 1}

    assert wave_rows([other, current], "P1_CURRENT")[0]["research_task_id"] == "A"


def test_structural_only_closure_is_not_external_task() -> None:
    rows = closure_rows([{"ticker": "S", "company_id": "1", "remaining_status": "DOWNSTREAM_LATEST8Q_STRUCTURAL_REVIEW"}], set(), {"S"})

    assert rows[0]["closure_completeness"] == "YES_STRUCTURAL_ONLY"


def test_structural_queue_is_separate_and_mixed_ticker_is_flagged() -> None:
    structural = [{"ticker": "TEST", "FY/FQ": "FY2026Q1", "issue": "TARGET_COLLISION", "current evidence": "TARGET_CONFLICTING", "exact decision needed": "resolve target", "evidence that would resolve it": "NEED_TARGET_COLLISION_RESOLUTION", "priority": "P1_CURRENT"}]

    cleaned = clean_structural_rows(structural, {"TEST"}, {"TEST": {"company_id": "1"}})

    assert cleaned[0]["status"] == "STRUCTURAL_REVIEW_SEPARATE"
    assert cleaned[0]["external_research_also_required"] == "YES"


def test_ticker_consolidated_request_is_directly_usable() -> None:
    rows = [
        {**external_row(closure_dependency="NEED_REVENUE|NEED_DEBT"), "source_row_id": 1},
    ]
    _raw, facts = build_fact_rows(rows)
    tasks = build_research_tasks(rows, facts, {"TEST": {"company_id": "1"}}, {"TEST"}, {})
    summary = build_ticker_summary(tasks, [{"ticker": "TEST", "company_id": "1", "remaining_status": "DOWNSTREAM_LATEST8Q_EXTERNAL_EVIDENCE_REQUIRED"}], {"TEST"})

    assert "For FY2026 Q1 verify" in summary[0]["consolidated_research_request"]
    assert summary[0]["structural_review_also_required"] == "YES"
    assert summary[0]["expected_status_after_external_evidence"] == "YES_EXTERNAL_PLUS_STRUCTURAL"


def test_first_batch_scoring_prefers_current_multi_layer_tasks() -> None:
    high = {"research_task_id": "H", "ticker": "H", "fiscal_year": "2026", "fiscal_quarter": "Q1", "priority": "P1_CURRENT", "quarter_position_latest8q": 1, "current_ttm_impact": "YES", "score_impact": "YES", "lifecycle_impact": "YES", "valuation_impact": "YES", "downstream_layer_count": 4, "fact_count": 2, "evidence_types_needed": "REVENUE|TOTAL_DEBT"}
    low = {**high, "research_task_id": "L", "ticker": "L", "quarter_position_latest8q": 4, "current_ttm_impact": "NO", "score_impact": "NO", "lifecycle_impact": "NO", "valuation_impact": "NO", "downstream_layer_count": 0}

    selected, scores = first_batch([high, low])

    assert [row["research_task_id"] for row in selected] == ["H"]
    assert int(scores[0]["score"]) > int(scores[-1]["score"])


def test_first_batch_excludes_wave2_even_with_high_score() -> None:
    task = {"research_task_id": "W2", "ticker": "W2", "fiscal_year": "2026", "fiscal_quarter": "Q1", "priority": "P2_LATEST4Q", "quarter_position_latest8q": 1, "current_ttm_impact": "YES", "score_impact": "YES", "lifecycle_impact": "YES", "valuation_impact": "YES", "downstream_layer_count": 4, "fact_count": 5, "evidence_types_needed": "REVENUE|TOTAL_DEBT"}

    selected, _scores = first_batch([task])

    assert selected == []


def test_closure_completeness_has_no_missing_requirement_when_every_ticker_has_path() -> None:
    status = [
        {"ticker": "A", "company_id": "1", "remaining_status": "DOWNSTREAM_LATEST8Q_CLEAN"},
        {"ticker": "B", "company_id": "2", "remaining_status": "DOWNSTREAM_LATEST8Q_EXTERNAL_EVIDENCE_REQUIRED"},
        {"ticker": "C", "company_id": "3", "remaining_status": "DOWNSTREAM_LATEST8Q_STRUCTURAL_REVIEW"},
        {"ticker": "D", "company_id": "4", "remaining_status": "DOWNSTREAM_LATEST8Q_STRUCTURAL_REVIEW"},
    ]
    rows = closure_rows(status, {"B", "D"}, {"C", "D"})

    assert {row["closure_completeness"] for row in rows} == {
        "ALREADY_CLEAN",
        "YES_EXTERNAL_ONLY",
        "YES_STRUCTURAL_ONLY",
        "YES_EXTERNAL_PLUS_STRUCTURAL",
    }
    assert sum(int(row["missing_requirement"]) for row in rows) == 0


def test_phase8h_readonly_artifact_contract(tmp_path: Path) -> None:
    summary = run_phase8h(
        Phase8HPaths(
            artifact_root=tmp_path / "phase8h",
            phase8g_root=Path("temp/fundamentals_v3_phase8g_local_latest8q_repairs/20260829T_PHASE8G_FINAL"),
            v3_db=Path("rc_fundamentals_v3.db"),
            write_documentation=False,
        )
    )

    assert summary["classification"] == CLASSIFICATION_STRUCTURAL
    assert summary["starting_queue"]["phase8g_external_facts"] == 4413
    assert summary["cleanup"]["final_downstream_critical_external_facts"] == 9491
    assert summary["closure_completeness"]["NO_MISSING_REQUIREMENT"] == 0
    assert summary["safety"] == {"production_writes": 0, "network_calls": 0, "rawcandle_writes": 0}
    for ticker in EXPECTED_P1_TICKERS:
        assert ticker in {row["ticker"] for row in summary["known_13"]}
    with (tmp_path / "phase8h" / "latest8q_external_research_wave1_p1_current.csv").open(newline="", encoding="utf-8") as handle:
        wave1 = list(csv.DictReader(handle))
    assert wave1
    assert all("Do not research other fields" in row["research_request"] for row in wave1)
    assert (tmp_path / "phase8h" / "latest8q_external_research_first_batch.csv").exists()
    assert (tmp_path / "phase8h" / "latest8q_structural_decisions_remaining.csv").exists()
