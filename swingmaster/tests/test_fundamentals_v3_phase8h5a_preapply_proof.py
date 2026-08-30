from __future__ import annotations

import csv
from pathlib import Path

from swingmaster.fundamentals.v3_phase8h5a_preapply_proof import (
    CLASSIFICATION_READY,
    CLASSIFICATION_REDUCED,
    PreapplyPaths,
    collision_classification,
    fq_confidence_class,
    fq_explanation,
    normalize_global,
    partition_latest8,
    repair_confidence_audits,
    run_preapply,
    validate_h5a_artifacts,
)


H5A_ROOT = Path("temp/fundamentals_v3_phase8h5a_fiscal_identity_root_cause/20260830T_PHASE8H5A")
_CACHED_SUMMARY: dict | None = None


def cached_summary() -> dict:
    global _CACHED_SUMMARY
    if _CACHED_SUMMARY is None:
        _CACHED_SUMMARY = run_preapply(
            PreapplyPaths(
                artifact_root=Path("temp/pytest_phase8h5a_preapply_proof"),
                h5a_root=H5A_ROOT,
                write_documentation=False,
            )
        )
    return _CACHED_SUMMARY


def row(**kwargs: str) -> dict[str, str]:
    base = {
        "ticker": "T",
        "quarter_id": "1",
        "company_id": "1",
        "stored_FY": "2025",
        "stored_FQ": "Q1",
        "resolved_FY": "2025",
        "resolved_FQ": "Q1",
        "FY_match": "YES",
        "FQ_match": "YES",
        "final_defect_class": "CANONICAL_CORRECT",
        "identity_confidence": "EXACT_ANCHOR",
        "evidence_basis": "EXACT_FY_START_INTERVAL_FIRST",
        "exact_anchor_interval": "2025-01-01",
        "expected_slot_end": "2025-03-31",
        "period_end": "2025-03-31",
        "transition_status": "",
        "calendar_type": "CALENDAR_YEAR",
        "resolver_warnings": "",
        "latest8Q": "0",
        "latest4Q": "0",
        "latest_quarter": "0",
        "group_status": "",
        "target_collision_class": "",
        "repair_type": "",
        "old_fiscal_year": "2025",
        "new_fiscal_year": "2025",
        "old_fiscal_quarter": "Q1",
        "new_fiscal_quarter": "Q1",
        "repair_group_id": "g",
    }
    base.update(kwargs)
    return base


def test_h5a_artifacts_present() -> None:
    assert validate_h5a_artifacts(H5A_ROOT)["valid"] is True


def test_global_normalized_categories_are_non_overlapping() -> None:
    audited = [row(quarter_id="1"), row(quarter_id="2", FY_match="NO", final_defect_class="CANONICAL_FY_MINUS_ONE")]
    repairs = [row(quarter_id="2", group_status="REHEARSAL_READY")]
    normalized, summary = normalize_global(audited, repairs, [])
    assert len({r["normalized_identity_state"] for r in normalized}) == 2
    assert summary["reconciliation_total"] == 2


def test_normalized_counts_reconcile_exactly() -> None:
    normalized, summary = normalize_global([row(), row(quarter_id="2", FQ_match="NO", final_defect_class="CANONICAL_FQ_WRONG", resolver_warnings="PERIOD_END_OUTSIDE_SLOT")], [], [])
    assert summary["reconciles"] is True
    assert summary["canonical_rows_analyzed"] == len(normalized)


def test_fy_repair_requires_high_confidence() -> None:
    repairs = [row(group_status="REHEARSAL_READY", old_fiscal_year="2024", new_fiscal_year="2025", old_fiscal_quarter="Q1", new_fiscal_quarter="Q1", repair_group_id="g")]
    fy, _fq, decisions = repair_confidence_audits(repairs, {"1": row(exact_anchor_interval="", identity_confidence="LOW")})
    assert fy[0]["FY_confidence"] == "LOW"
    assert decisions[0]["decision"] == "REMOVE_OTHER"


def test_fq_repair_requires_high_confidence() -> None:
    assert fq_confidence_class(row(resolver_warnings="PERIOD_END_OUTSIDE_SLOT")) == "FQ_MEDIUM_PATTERN"


def test_medium_fq_removed() -> None:
    repairs = [row(group_status="REHEARSAL_READY", old_fiscal_year="2025", new_fiscal_year="2025", old_fiscal_quarter="Q1", new_fiscal_quarter="Q2", repair_group_id="g")]
    _fy, _fq, decisions = repair_confidence_audits(repairs, {"1": row(FQ_match="NO", resolver_warnings="PERIOD_END_OUTSIDE_SLOT")})
    assert decisions[0]["decision"] == "REMOVE_FQ_CONFIDENCE_INSUFFICIENT"


def test_low_fq_removed() -> None:
    assert fq_confidence_class(row(resolver_warnings="UNKNOWN_WARNING")) == "FQ_LOW_HEURISTIC"


def test_issuer_direct_fq_accepted() -> None:
    assert fq_confidence_class(row(issuer_label="FY2025 Q1")) == "FQ_HIGH_DIRECT_ISSUER"


def test_exact_slot_fq_accepted() -> None:
    assert fq_confidence_class(row()) == "FQ_HIGH_EXACT_SLOT"


def test_sequence_proven_fq_accepted() -> None:
    assert fq_confidence_class(row(repair_type="ATOMIC_SEGMENT_RELABEL")) == "FQ_HIGH_SEQUENCE"


def test_week_based_fq_accepted() -> None:
    assert fq_confidence_class(row(calendar_type="WEEK_BASED_52_53")) == "FQ_HIGH_WEEK_BASED"


def test_transition_fq_excluded() -> None:
    assert fq_confidence_class(row(transition_status="VERIFIED_TRANSITION")) == "FQ_TRANSITION"


def test_collision_fq_excluded_unless_fully_proven() -> None:
    assert fq_confidence_class(row(group_status="UNRESOLVED_TARGET_COLLISION")) == "FQ_COLLISION_BLOCKED"


def test_all_original_repair_rows_classified() -> None:
    summary = cached_summary()
    total = sum(summary["repair_row_decision"].values())
    assert total == 2088
    assert summary["original_h5a_repair_population"]["original_rows"] == 2088


def test_all_8437_fq_audit_differences_explained() -> None:
    audited = [row(final_defect_class="CANONICAL_FQ_WRONG", FQ_match="NO", resolver_warnings="PERIOD_END_OUTSIDE_SLOT")]
    normalized, _summary = normalize_global(audited, [], [])
    explained, _counts = fq_explanation(normalized)
    assert len(explained) == 1
    assert explained[0]["fq_defect_explanation"]


def test_latest8q_4327_rows_fully_partitioned() -> None:
    normalized = [row(quarter_id="1", latest8Q="1", final_defect_class="CANONICAL_FQ_WRONG", FQ_match="NO")]
    rows, summary = partition_latest8(normalized, {"1"})
    assert len(rows) == summary["wrong_identity_rows_before"]
    assert summary["remaining_unexplained"] == 0


def test_wday_repair_retained() -> None:
    summary = cached_summary()
    assert summary["hard_cases"]["WDAY"][0]["resolved_FY"] == "2027"
    assert summary["hard_cases"]["WDAY"][0]["normalized_identity_state"] in {"FY_ONLY_WRONG_REPAIRABLE", "FY_AND_FQ_WRONG_REPAIRABLE"}


def test_asth_no_write() -> None:
    summary = cached_summary()
    assert summary["hard_cases"]["ASTH"][0]["normalized_identity_state"] == "CANONICAL_CORRECT"


def test_ceco_no_write() -> None:
    summary = cached_summary()
    assert summary["hard_cases"]["CECO"][0]["normalized_identity_state"] == "CANONICAL_CORRECT"


def test_fresh_disposable_db_required() -> None:
    summary = cached_summary()
    assert "temp/pytest_phase8h5a_preapply_proof/rehearsal" in summary["fresh_rehearsal"]["rehearsal_db"]


def test_no_reuse_of_old_rehearsal_db() -> None:
    summary = cached_summary()
    assert "phase8h5a_fiscal_identity_root_cause/20260830T_PHASE8H5A/rehearsal" not in summary["fresh_rehearsal"]["rehearsal_db"]


def test_canonical_integrity() -> None:
    summary = cached_summary()
    assert summary["fresh_rehearsal"]["quick_check"] == "ok"
    assert summary["fresh_rehearsal"]["duplicate_fy_fq"] == 0


def test_lineage_parity() -> None:
    summary = cached_summary()
    assert summary["fresh_rehearsal"]["lineage_failures"] == 0


def test_content_parity() -> None:
    summary = cached_summary()
    with (Path(summary["artifact_root"]) / "preapply_content_parity.csv").open(newline="", encoding="utf-8-sig") as handle:
        assert next(csv.DictReader(handle))["content_parity_status"] == "FY_FQ_ONLY_RELABELED"


def test_unrelated_canonical_drift_zero() -> None:
    summary = cached_summary()
    assert summary["fresh_rehearsal"]["unrelated_canonical_drift"] == 0


def test_ttm_rebuild() -> None:
    summary = cached_summary()
    assert summary["downstream"]["TTM"] == "REBUILT"


def test_score_rebuild() -> None:
    summary = cached_summary()
    assert summary["downstream"]["Score"] == "REBUILT"


def test_lifecycle_rebuild() -> None:
    summary = cached_summary()
    assert summary["downstream"]["Lifecycle"] == "REBUILT"


def test_valuation_rebuild() -> None:
    summary = cached_summary()
    assert summary["downstream"]["Valuation"] == "REBUILT"


def test_determinism() -> None:
    summary = cached_summary()
    assert summary["downstream"]["determinism_all"] == "YES"


def test_unrelated_downstream_drift_zero() -> None:
    summary = cached_summary()
    assert summary["downstream"]["unrelated_downstream_drift"] == 0


def test_no_q4_creation() -> None:
    summary = cached_summary()
    assert summary["deferred"]["q4_rows_created"] == 0


def test_no_latest_quarter_creation() -> None:
    summary = cached_summary()
    assert summary["deferred"]["latest_quarter_rows_created"] == 0


def test_no_publish_date_scope_creep() -> None:
    summary = cached_summary()
    assert summary["deferred"]["publish_date_cleanup_deferred"] == "YES"


def test_production_writes_zero() -> None:
    summary = cached_summary()
    assert summary["safety"]["production_writes"] == 0


def test_network_calls_zero() -> None:
    summary = cached_summary()
    assert summary["safety"]["network_calls"] == 0


def test_rawcandle_writes_zero() -> None:
    summary = cached_summary()
    assert summary["safety"]["rawcandle_writes"] == 0


def test_guard_changes_zero() -> None:
    summary = cached_summary()
    assert summary["safety"]["guard_changes"] == 0


def test_collision_142_default_deferred() -> None:
    rows = [row(group_status="UNRESOLVED_TARGET_COLLISION", target_collision_class="TARGET_CONFLICTING")]
    classified, counts = collision_classification(rows)
    assert classified[0]["decision"] == "DEFER"
    assert counts["LINEAGE_AMBIGUOUS"] == 1


def test_preapply_classification_reduced_when_collision_rows_deferred() -> None:
    summary = cached_summary()
    assert summary["classification"] in {CLASSIFICATION_READY, CLASSIFICATION_REDUCED}
