from __future__ import annotations

from swingmaster.fundamentals import v3_phase8a10a_special_resolution as special


def current_rows() -> list[dict[str, object]]:
    return [
        {"ticker": "FNGR", "quarter_id": 1, "fiscal_year": 2024, "fiscal_quarter": "Q2", "period_end": "2024-05-31", "publish_date": "2023-10-16", "revenue": 8373983.0, "lineage_refs": 1, "accepted_source_provider": "V2"},
        {"ticker": "IMMR", "quarter_id": 2, "fiscal_year": 2025, "fiscal_quarter": "Q1", "period_end": "2025-04-30", "publish_date": "2025-12-24", "revenue": 281376000.0, "lineage_refs": 1, "accepted_source_provider": "YAHOO"},
        {"ticker": "IMMR", "quarter_id": 3, "fiscal_year": 2025, "fiscal_quarter": "Q4", "period_end": "2026-01-31", "publish_date": "2025-07-30", "revenue": 518488000.0, "lineage_refs": 1, "accepted_source_provider": "YAHOO"},
        {"ticker": "RCAT", "quarter_id": 4, "fiscal_year": 2024, "fiscal_quarter": "Q2", "period_end": "2024-07-31", "publish_date": "2024-08-08", "revenue": 886440.0, "lineage_refs": 1, "accepted_source_provider": "LEGACY"},
        {"ticker": "RCAT", "quarter_id": 5, "fiscal_year": 2024, "fiscal_quarter": "Q3", "period_end": "2024-10-31", "publish_date": "2024-03-18", "revenue": 1534727.0, "lineage_refs": 1, "accepted_source_provider": "V2"},
    ]


def official() -> dict[tuple[str, str, str], dict[str, str]]:
    rows = [
        {"Ticker": "FNGR", "Fiscal Year": "2024", "Fiscal Q": "Q2", "Official Period End": "2023-08-31", "Publish Date": "2023-10-13", "Revenue": "8373983", "Source 1": "sec", "Confidence": "HIGH"},
        {"Ticker": "IMMR", "Fiscal Year": "2025", "Fiscal Q": "Q4", "Official Period End": "2025-04-30", "Publish Date": "2026-03-12", "Revenue": "284876000", "Source 1": "sec", "Confidence": "HIGH"},
        {"Ticker": "IMMR", "Fiscal Year": "2026", "Fiscal Q": "Q3", "Official Period End": "2026-01-31", "Publish Date": "2026-06-26", "Revenue": "518488000", "Source 1": "sec", "Confidence": "HIGH"},
        {"Ticker": "RCAT", "Fiscal Year": "2024T", "Fiscal Q": "Q1", "Official Period End": "2024-07-31", "Publish Date": "2024-08-08", "Revenue": "2776535", "Source 1": "sec", "Confidence": "HIGH"},
        {"Ticker": "RCAT", "Fiscal Year": "2024T", "Fiscal Q": "Q2", "Official Period End": "2024-10-31", "Publish Date": "2024-12-16", "Revenue": "1534727", "Source 1": "sec", "Confidence": "HIGH"},
    ]
    return special.official_by_identity(rows)


def remaps() -> list[dict[str, str]]:
    return [
        {"Ticker": "FNGR", "Current Fiscal Year": "2024", "Current Fiscal Q": "Q2", "Current Period End": "2024-05-31", "Proposed Fiscal Year": "2024", "Proposed Fiscal Q": "Q2"},
        {"Ticker": "IMMR", "Current Fiscal Year": "2025", "Current Fiscal Q": "Q1", "Current Period End": "2025-04-30", "Proposed Fiscal Year": "2025", "Proposed Fiscal Q": "Q4"},
        {"Ticker": "IMMR", "Current Fiscal Year": "2025", "Current Fiscal Q": "Q4", "Current Period End": "2026-01-31", "Proposed Fiscal Year": "2026", "Proposed Fiscal Q": "Q3"},
        {"Ticker": "RCAT", "Current Fiscal Year": "2024", "Current Fiscal Q": "Q2", "Current Period End": "2024-07-31", "Proposed Fiscal Year": "2024", "Proposed Fiscal Q": "Q1"},
        {"Ticker": "RCAT", "Current Fiscal Year": "2024", "Current Fiscal Q": "Q3", "Current Period End": "2024-10-31", "Proposed Fiscal Year": "2024", "Proposed Fiscal Q": "Q2"},
    ]


def test_exact_remaining_tickers() -> None:
    special.validate_remaining_tickers([{"ticker": "FNGR"}, {"ticker": "IMMR"}, {"ticker": "RCAT"}])


def test_fngr_sparse_history_does_not_imply_shifted_sequence() -> None:
    ops, status = special.fngr_resolution(current_rows(), official())
    assert status == "PRODUCTION_READY"
    assert {op["operation"] for op in ops} == {"UPDATE_PERIOD_END", "UPDATE_PUBLISH_DATE"}


def test_fngr_bounded_period_end_repair_guard() -> None:
    ops, _ = special.fngr_resolution(current_rows(), official())
    period = [op for op in ops if op["operation"] == "UPDATE_PERIOD_END"][0]
    assert period["current_canonical_quarter_id"] == 1
    assert period["old_value"] == "2024-05-31"
    assert period["new_value"] == "2023-08-31"


def test_immr_identity_value_coupling_detected() -> None:
    matrix = special.immr_restatement_matrix(current_rows(), official(), remaps())
    assert any(row["revenue_action"] == "UPDATE_CANONICAL_VALUE" for row in matrix)


def test_immr_restated_value_mapping() -> None:
    matrix = special.immr_restatement_matrix(current_rows(), official(), remaps())
    row = [item for item in matrix if item["actual_fy"] == "2025" and item["actual_fq"] == "Q4"][0]
    assert row["current_revenue"] == 281376000.0
    assert row["official_revenue"] == "284876000"


def test_pre_restatement_vs_restated_row_distinction() -> None:
    conflicts = special.non_null_conflicts(current_rows(), official(), remaps())
    assert any(row["ticker"] == "IMMR" and row["status"] == "CONFLICT" for row in conflicts)


def test_rcat_transition_year_detected() -> None:
    mapping = special.rcat_transition_mapping(current_rows(), official(), remaps())
    assert all(row["status"] == "TRANSITION_LABEL_POLICY_REQUIRED" for row in mapping)


def test_10kt_period_handling() -> None:
    mapping = special.rcat_transition_mapping(current_rows(), official(), remaps())
    assert {row["official_period_end"] for row in mapping} == {"2024-07-31", "2024-10-31"}


def test_rcat_revenue_1534727_remains_with_2024_10_31() -> None:
    mapping = special.rcat_transition_mapping(current_rows(), official(), remaps())
    row = [item for item in mapping if item["current_revenue"] == 1534727.0][0]
    assert row["current_period_end"] == "2024-10-31"


def test_no_normal_quarter_forcing_across_transition_year() -> None:
    mapping = special.rcat_transition_mapping(current_rows(), official(), remaps())
    assert all(row["proposed_canonical_label"] == "POLICY_REQUIRED_FOR_2024T" for row in mapping)


def test_target_collision_detection() -> None:
    collisions = special.target_collisions(current_rows(), remaps())
    assert any(row["classification"] == "DUPLICATE_ECONOMIC_QUARTER" for row in collisions)


def test_non_null_conflict_detection() -> None:
    conflicts = special.non_null_conflicts(current_rows(), official(), remaps())
    assert any(row["conflicting_fields"] == "revenue" for row in conflicts)


def test_lineage_ownership() -> None:
    ops, _ = special.fngr_resolution(current_rows(), official())
    lineage = special.lineage_ownership(current_rows(), ops, remaps())
    assert lineage[0]["lineage_action"] == "PRESERVE_QUARTER_ID_LINEAGE"


def test_atomic_transformation_grouping() -> None:
    ops, _ = special.fngr_resolution(current_rows(), official())
    groups = special.group_summary(ops)
    assert groups[0]["transformation_group_id"] == "P8A10A-SPECIAL-FNGR"


def test_no_production_writes_summary_shape() -> None:
    assert special.DERIVED_STALE == "DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR"


def test_no_derived_writes_contract() -> None:
    assert set(special.SPECIAL_TICKERS) == {"FNGR", "IMMR", "RCAT"}


def test_no_rawcandle_writes_contract() -> None:
    assert special.Phase8A10ASpecialPaths.__dataclass_fields__["rawcandle_db"].default == special.Path("/home/kalle/projects/rawcandle/data/osakedata.db")
