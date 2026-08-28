from __future__ import annotations

from swingmaster.fundamentals.v3_phase8d6_label_provenance_audit import (
    calendar_rule,
    candidate_row,
    content_integrity,
    infer_provenance,
    label_creation_paths,
    label_error_class,
    repairability,
    target_collision,
)


def test_provenance_path_inventory_includes_active_and_historical_paths() -> None:
    paths = label_creation_paths()

    assert any(row["path"] == "Yahoo raw normalization" and row["active"] == "YES" for row in paths)
    assert any(row["path"] == "V3 Yahoo canonical seed" and row["active"] == "HISTORICAL_SEED" for row in paths)


def test_fy_and_fq_provenance_classification_from_yahoo_audit() -> None:
    row = {"accepted_source_provider": "YAHOO"}
    audit = {"source": "YAHOO", "audit_type": "CANONICAL_APPLY", "source_key": "YAHOO:AAA:2026-03-31:hash"}

    result = infer_provenance(row, audit)

    assert result["fy_provenance"] == "V3_SEED_DERIVED"
    assert result["fq_provenance"] == "V3_SEED_DERIVED"


def test_manual_repair_provenance_takes_precedence() -> None:
    result = infer_provenance({"accepted_source_provider": "YAHOO"}, {"source": "YAHOO", "audit_type": "CANONICAL_IDENTITY_CORRECTION", "source_key": "x"})

    assert result["fy_provenance"] == "MANUAL_REPAIR_LABEL"


def test_detect_period_end_year_label_rule() -> None:
    row = {"fiscal_year": "2026", "period_end": "2026-04-30", "d5_fy_interval_start": "2025-02-01"}

    assert calendar_rule(row) == "STORED_FY_EQUALS_PERIOD_END_YEAR"


def test_detect_fiscal_start_year_label_rule() -> None:
    row = {"fiscal_year": "2025", "period_end": "2025-09-30", "d5_fy_interval_start": "2025-07-01"}

    assert calendar_rule(row) == "STORED_FY_EQUALS_PERIOD_END_AND_FISCAL_START_YEAR"


def test_label_error_minus_one_and_fq_correct() -> None:
    row = {"fiscal_year": "2025", "fiscal_quarter": "Q1", "d5_inferred_fiscal_year": 2026, "d5_inferred_fiscal_quarter": "Q1"}

    assert label_error_class(row) == "FY_LABEL_MINUS_ONE"


def test_label_error_fq_correct_fy_wrong_other_shift() -> None:
    row = {"fiscal_year": "2024", "fiscal_quarter": "Q2", "d5_inferred_fiscal_year": 2026, "d5_inferred_fiscal_quarter": "Q2"}

    assert label_error_class(row) == "FY_WRONG_FQ_CORRECT"


def test_fy_correct_fq_wrong_classification() -> None:
    row = {"fiscal_year": "2026", "fiscal_quarter": "Q2", "d5_inferred_fiscal_year": 2026, "d5_inferred_fiscal_quarter": "Q3"}

    assert label_error_class(row) == "FQ_LABEL_MINUS_ONE"


def test_label_only_content_integrity() -> None:
    row = {"d5_actual_slot_offset_days": "0", "publish_date": "2026-05-01", "revenue": "100", "collision_status": "TARGET_EMPTY"}

    assert content_integrity(row) == "LABEL_ONLY_ERROR_HIGH_CONFIDENCE"


def test_target_collision_empty_and_different_economic() -> None:
    row = {"company_id": "1", "quarter_id": "10", "d5_inferred_fiscal_year": "2026", "d5_inferred_fiscal_quarter": "Q1", "period_end": "2026-03-31", "revenue": "10", "operating_income": "1", "net_income": "1"}
    target = {"company_id": 1, "quarter_id": "11", "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2025-03-31", "revenue": "99", "operating_income": "9", "net_income": "9"}

    assert target_collision(row, {}) == "TARGET_EMPTY"
    assert target_collision(row, {(1, 2026, "Q1"): target}) == "TARGET_DIFFERENT_ECONOMIC"


def test_auto_label_repair_ready_requires_label_only_and_no_collision() -> None:
    row = {"content_integrity": "LABEL_ONLY_ERROR_HIGH_CONFIDENCE", "collision_status": "TARGET_EMPTY"}

    assert repairability(row) == "AUTO_LABEL_REPAIR_READY"


def test_candidate_row_exports_current_and_proposed_identity() -> None:
    row = {
        "ticker": "AAA",
        "company_id": "1",
        "quarter_id": "2",
        "fiscal_year": "2025",
        "fiscal_quarter": "Q1",
        "d5_inferred_fiscal_year": "2026",
        "d5_inferred_fiscal_quarter": "Q1",
        "period_end": "2025-09-30",
        "d5_fy_interval_start": "2025-07-01",
        "calendar_type": "FIXED_DATE_FISCAL_YEAR",
        "d5_confidence": "FY_EXACT_FQ_HIGH",
        "fy_provenance": "V3_SEED_DERIVED",
        "collision_status": "TARGET_EMPTY",
        "sequence_quality": "COHERENT",
    }

    candidate = candidate_row(row)

    assert candidate["current_fiscal_year"] == "2025"
    assert candidate["proposed_fiscal_year"] == "2026"
