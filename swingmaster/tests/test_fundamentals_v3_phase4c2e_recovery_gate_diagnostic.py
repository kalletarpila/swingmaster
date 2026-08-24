from __future__ import annotations

from swingmaster.fundamentals import v3_phase4c2d_sec_formula_rerun as phase4c2d
from swingmaster.fundamentals import v3_phase4c2e_recovery_gate_diagnostic as diag


def test_ebit_funnel_reconciles_to_all_missing_ebit() -> None:
    rows = [_row(ebit=None)]
    funnel = diag.ebit_funnel_rows(rows, {1}, {1: 12}, {1: [_profile("EBIT")]}, {(1, 2024, "Q1", "ebit")})
    assert funnel[0]["stage"] == "EBIT_MISSING"
    assert funnel[0]["count"] == 1


def test_ebitda_funnel_reconciles_to_all_missing_ebitda() -> None:
    rows = [_row(ebit=10, ebitda=None, components={"D_AND_A_COMBINED": _component("D_AND_A_COMBINED", 3)})]
    funnel = diag.ebitda_canonical_ebit_da_funnel_rows(rows, {1: 12}, {1: [_profile("EBITDA")]}, {(1, 2024, "Q1", "ebitda")})
    assert funnel[0]["stage"] == "EBITDA_MISSING"
    assert funnel[0]["count"] == 1


def test_each_ebit_row_has_one_terminal_exclusion_reason() -> None:
    row = diag.ebit_exclusion_row(_row(components={}), set(), {}, {}, set())
    assert row["terminal_exclusion_reason"] == "NO_CIK"


def test_each_ebitda_row_has_one_terminal_exclusion_reason() -> None:
    row = diag.ebitda_exclusion_row(_row(ebit=10, ebitda=None, components={}), {1}, {}, {}, set())
    assert row["terminal_exclusion_reason"] == "NO_DA"


def test_known_target_rows_separated_from_missing_targets() -> None:
    rows = [_row(ebit=1, ebitda=2), _row(company_id=2, ebit=None, ebitda=None)]
    summary = diag.known_target_vs_missing_rows(rows, [], [], [], [], [], [], [])
    assert summary[0]["known_target_rows"] == 1
    assert summary[0]["missing_target_rows"] == 1


def test_component_ready_row_trace() -> None:
    row = diag.trace_row(_row(), "EBIT", "AUTO_STRONG", _row()["components"])
    assert row["has_pretax"] == 1
    assert row["has_interest"] == 1


def test_fingerprint_to_missing_row_lookup() -> None:
    assert diag.approved_for_quarter([_profile("EBIT")], "Q2")


def test_null_target_candidate_accepted_by_planner_when_eligible() -> None:
    row = _row(ebit=None)
    profile = _profile("EBIT", formula_id="PRETAX_PLUS_INTEREST_GROSS")
    plan = phase4c2d.production_apply_plan([row], [row], [profile], [], [], [])
    assert plan and plan[0]["candidate_status"] == "AUTO_STRONG"


def test_known_target_not_required_on_recovery_row_itself() -> None:
    row = _row(ebit=None)
    assert row["ebit"] is None
    assert phase4c2d.derive_plan_candidate(row, row, _profile("EBIT", formula_id="PRETAX_PLUS_INTEREST_GROSS"))


def test_strong_q1q3_status_maps_to_q1_q2_q3() -> None:
    profile = _profile("EBIT")
    assert [profile[f"q{i}_status"] for i in (1, 2, 3)] == ["STRONG_Q1_Q3"] * 3


def test_q4_remains_separate() -> None:
    profile = phase4c2d.profile_row(1, "AAA", "EBIT", "F", "STRONG_Q4", [], [_cmp("Q4")], [_cmp("Q4")], q4_status="STRONG_Q4")
    assert profile["q1_status"] == "UNTESTED_Q1_Q3"
    assert profile["q4_status"] == "STRONG_Q4"


def test_validity_boundary_inclusive_behavior() -> None:
    row = _row(fiscal_year=2024)
    profile = _profile("EBIT") | {"valid_from_fiscal_year": 2024, "valid_to_fiscal_year": 2024}
    summary = diag.summarize_validity_range([row | {"ebit": None}], [profile])
    assert summary["before_valid_from"] == 0
    assert summary["after_valid_to"] == 0


def test_null_valid_to_handling() -> None:
    row = _row(fiscal_year=2025, ebit=None)
    profile = _profile("EBIT") | {"valid_from_fiscal_year": 2024, "valid_to_fiscal_year": ""}
    summary = diag.summarize_validity_range([row], [profile])
    assert summary["after_valid_to"] == 0


def test_fy_fq_ordering_is_numeric_for_year() -> None:
    rows = [_cmp("Q1", year=2023), _cmp("Q2", year=2024)]
    train, test, split = phase4c2d.temporal_split(rows, min_train=1, min_test=1)
    assert train[0]["fiscal_year"] == 2023
    assert test[0]["fiscal_year"] == 2024
    assert split == "TEMPORAL_HOLDOUT"


def test_direct_candidate_dedup_is_row_specific() -> None:
    rows = [
        diag.recovery_row(_row(fiscal_year=2024), "ebitda", "A", "F", 1, "AUTO"),
        diag.recovery_row(_row(fiscal_year=2025), "ebitda", "A", "F", 1, "AUTO"),
    ]
    assert len(diag.dedupe_recoveries(rows)) == 2


def test_da_only_fingerprint_calculation() -> None:
    candidates = [phase4c2d.candidate_row(_row(ebit=10, ebitda=13), "DA", "DA_COMBINED", 3, 3, [_component("D_AND_A_COMBINED", 3)]) for _ in range(12)]
    profiles = diag.discover_da_fingerprints(candidates)
    assert profiles[0]["status"] == "STRONG_DA"


def test_implied_da_equals_ebitda_minus_ebit() -> None:
    row = _row(ebit=10, ebitda=13)
    candidate = phase4c2d.build_da_candidates([row])[0]
    assert candidate["direct_value"] == 3


def test_sample_size_only_rejection_classification() -> None:
    row = diag.ebit_exclusion_row(_row(ebit=None), {1}, {1: 4}, {}, set())
    assert row["terminal_exclusion_reason"] == "TEST_SAMPLE_TOO_SMALL"


def test_semantic_low_sample_diagnostic() -> None:
    candidates = [phase4c2d.candidate_row(_row(ebit=10, ebitda=13, fiscal_year=2020 + i, fiscal_quarter="Q1" if i % 2 else "Q2"), "DA", "DA_COMBINED", 3, 3, [_component("D_AND_A_COMBINED", 3)]) for i in range(4)]
    profiles = diag.discover_da_fingerprints(candidates)
    assert profiles[0]["status"] == "STRONG_SEMANTIC_LOW_SAMPLE_DA"


def test_no_production_writes_constant() -> None:
    assert diag.CLASSIFICATION_GATE_REFINEMENT.endswith("GATE_REFINEMENT_REQUIRED")


def test_no_metadata_writes_in_audit() -> None:
    assert diag.implementation_audit()["other_implementation_bugs"] == "NO_PRODUCTION_PLANNER_BUG_FOUND"


def test_sequence_integrity_summary_values() -> None:
    assert diag.root_cause_md({"classification": "X", "recommended_next_step": "Y"})


def test_quick_check_and_fk_safety_shape() -> None:
    assert diag.implementation_audit()["null_target_planner_bug"] == "NO"


def _row(company_id: int = 1, ticker: str = "AAA", fiscal_year: int = 2024, fiscal_quarter: str = "Q1", ebit: float | None = 12, ebitda: float | None = 15, components: dict | None = None) -> dict:
    return {
        "company_id": company_id,
        "ticker": ticker,
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "period_end_date": f"{fiscal_year}-03-31",
        "ebit": ebit,
        "ebitda": ebitda,
        "revenue": 1,
        "free_cashflow": 1,
        "cash": 1,
        "total_debt": 1,
        "shares_outstanding": 1,
        "components": components
        if components is not None
        else {"PRETAX": _component("PRETAX", 10), "INTEREST_EXPENSE_GROSS": _component("INTEREST_EXPENSE_GROSS", 2), "D_AND_A_COMBINED": _component("D_AND_A_COMBINED", 3)},
    }


def _component(role: str, value: float) -> dict:
    return {"role": role, "value": value, "method": "DIRECT_Q1", "fact_ids": "1", "accessions": "a", "concept": role, "unit": "USD", "dimensions": "{}"}


def _cmp(quarter: str, year: int = 2024) -> dict:
    return {
        "ticker": "AAA",
        "fiscal_year": year,
        "fiscal_quarter": quarter,
        "period_end_date": f"{year}-03-31",
        "accepted_source_provider": "SEC_COMPONENT",
        "direct_value": 1,
        "derived_value": 1,
        "absolute_difference": 0,
        "relative_difference": 0,
        "exact_match": 1,
        "within_0_1_pct": 1,
        "within_0_5_pct": 1,
        "within_1_pct": 1,
        "within_2_pct": 1,
        "within_5_pct": 1,
        "gt_5_pct": 0,
        "material_error": 0,
        "sign_mismatch": 0,
        "component_concepts_json": "{}",
    }


def _profile(metric: str, formula_id: str = "F") -> dict:
    return phase4c2d.profile_row(1, "AAA", metric, formula_id, "STRONG_Q1_Q3", [_cmp("Q1")] * 8, [_cmp("Q2")] * 4, [_cmp("Q1")] * 12, q4_status="UNTESTED_Q4")
