from __future__ import annotations

from datetime import date

from swingmaster.fundamentals.v3_phase8d7_historical_anchor_reanalysis import (
    build_exact_interval_map,
    classify_repairability,
    classify_row,
    d6_405_reanalysis,
    d6_513_reanalysis,
    fy_compare,
    short_or_long_inference,
    summarize_classes,
    target_collision,
)


def base_row(**overrides):
    row = {
        "company_id": 1,
        "ticker": "AAA",
        "active": 1,
        "quarter_id": 10,
        "fiscal_year": 2025,
        "fiscal_quarter": "Q1",
        "period_end": "2024-03-31",
        "publish_date": "2024-05-01",
        "revenue": 1.0,
        "operating_income": 2.0,
        "net_income": 3.0,
    }
    row.update(overrides)
    return row


def test_exact_adjacent_anchors_resolve_fy_and_stored_label_cannot_override() -> None:
    anchors = {1: {2024: date(2024, 1, 1), 2025: date(2025, 1, 1)}}
    profiles = {1: {"calendar_type": "CALENDAR_YEAR"}}
    chains = {1: {"chain_status": "COMPLETE_TO_FY1999", "break_reason": "COMPLETE_TO_FY1999"}}
    intervals = build_exact_interval_map(anchors, profiles, chains, {1: "AAA"})
    intervals_by_company = {1: intervals}

    resolved = classify_row(base_row(fiscal_year=2025), intervals_by_company, profiles, chains, anchors, {}, {})

    assert resolved["identity_basis"] == "DIRECT_EXACT_INTERVAL"
    assert resolved["exact_fy"] == 2024
    assert resolved["fy_compare"] == "FY_PLUS_ONE"
    assert resolved["identity_class"] == "BLOCK_EXACT_FY_CONFLICT"


def test_transition_unresolved_and_no_fiscal_year_boundaries_block_inference() -> None:
    anchors = {1: {2026: date(2025, 7, 1)}}
    observed = date(2023, 9, 30)

    assert short_or_long_inference(1, observed, anchors, {"break_reason": "CALENDAR_TRANSITION"}) == "UNRESOLVED"
    assert short_or_long_inference(1, observed, anchors, {"break_reason": "UNRESOLVED_BOUNDARY"}) == "UNRESOLVED"
    assert short_or_long_inference(1, observed, anchors, {"break_reason": "NO_FISCAL_YEAR"}) == "UNRESOLVED"
    assert short_or_long_inference(1, observed, anchors, {"break_reason": "SOURCE_HISTORY_EXHAUSTED"}) == "UNRESOLVED"


def test_fq_resolution_inside_exact_fy_and_label_only_repair_gate() -> None:
    anchors = {1: {2025: date(2024, 7, 1), 2026: date(2025, 7, 1)}}
    profiles = {1: {"calendar_type": "FIXED_DATE_FISCAL_YEAR"}}
    chains = {1: {"chain_status": "BROKEN_AT_FY1999", "break_reason": "SOURCE_HISTORY_EXHAUSTED"}}
    intervals = build_exact_interval_map(anchors, profiles, chains, {1: "AAA"})
    row = base_row(fiscal_year=2024, fiscal_quarter="Q1", period_end="2024-09-30", publish_date="2024-11-01")
    target_map = {}

    resolved = classify_row(row, {1: intervals}, profiles, chains, anchors, {}, target_map)

    assert resolved["exact_fy"] == 2025
    assert resolved["exact_fq"] == "Q1"
    assert resolved["fy_compare"] == "FY_MINUS_ONE"
    assert resolved["fq_compare"] == "FQ_EXACT_MATCH"
    assert resolved["period_end_structural_fit"] == "STRUCTURAL_FIT"
    assert resolved["repairability"] == "AUTO_RELABEL_READY"


def test_target_collision_and_auto_gate_reject_different_economic_quarter() -> None:
    row = {
        "company_id": 1,
        "quarter_id": 10,
        "exact_fy": 2025,
        "exact_fq": "Q1",
        "period_end": "2024-09-30",
        "revenue": 1.0,
        "operating_income": 2.0,
        "net_income": 3.0,
        "identity_class": "BLOCK_EXACT_FY_CONFLICT",
        "period_end_structural_fit": "STRUCTURAL_FIT",
        "publish_chronology": "PUBLISH_AFTER_PERIOD_END",
        "content_integrity": "CONTENT_MAPPING_REVIEW",
    }
    by_target = {(1, 2025, "Q1"): {"quarter_id": 11, "period_end": "2024-10-31", "revenue": 9.0, "operating_income": 9.0, "net_income": 9.0}}

    row["target_collision"] = target_collision(row, by_target)

    assert row["target_collision"] == "TARGET_DIFFERENT_ECONOMIC"
    assert classify_repairability(row) == "CONTENT_RECONSTRUCTION_REQUIRED"


def test_d6_cohorts_are_reclassified_by_quarter_id() -> None:
    reclass = {
        1: {"quarter_id": 1, "identity_basis": "DIRECT_EXACT_INTERVAL", "identity_class": "BLOCK_EXACT_FY_CONFLICT", "exact_fy": 2025, "exact_fq": "Q1", "fy_compare": "FY_MINUS_ONE", "fq_compare": "FQ_EXACT_MATCH", "repairability": "AUTO_RELABEL_READY", "target_collision": "TARGET_EMPTY", "content_integrity": "CONTENT_NOT_PROVEN_WRONG"},
        2: {"quarter_id": 2, "identity_basis": "LONG_INFERENCE", "identity_class": "INSUFFICIENT_HISTORY", "fy_compare": "NO_EXACT_FY"},
    }

    d405 = d6_405_reanalysis([{"quarter_id": "1"}, {"quarter_id": "2"}], reclass)
    d513 = d6_513_reanalysis([{"quarter_id": "1"}, {"quarter_id": "2"}], reclass)

    assert d405[0]["d7_reanalysis_class"] == "FY_MINUS_ONE_CONFIRMED_EXACT_ANCHOR"
    assert d405[1]["d7_reanalysis_class"] == "PRIOR_D6_CLASSIFICATION_NOT_CONFIRMED"
    assert d513[0]["d7_reanalysis_class"] == "LABEL_ONLY_ERROR_DIRECT_EXACT"
    assert d513[1]["d7_reanalysis_class"] == "NO_EXACT_HISTORICAL_SUPPORT"


def test_full_population_summary_counts_all_rows() -> None:
    data = [
        {"ticker": "A", "identity_class": "PASS_DIRECT_EXACT"},
        {"ticker": "B", "identity_class": "BLOCK_EXACT_FY_CONFLICT"},
        {"ticker": "C", "identity_class": "REVIEW_TRANSITION"},
    ]

    summary = summarize_classes(data)

    assert summary["rows"] == 3
    assert summary["clean"] == 1
    assert summary["direct_exact_fy_conflicts"] == 1
    assert summary["transition_review"] == 1


def test_fy_compare_labels_minus_one() -> None:
    assert fy_compare(2024, 2025) == "FY_MINUS_ONE"
