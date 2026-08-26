from __future__ import annotations

import csv
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10f_current_downstream as phase


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def case_row(request_id: str = "REQ1", ticker: str = "CAPS", ready: str = "YES", confidence: str = "HIGH") -> dict[str, str]:
    return {
        "Request ID": request_id,
        "Priority": "P2",
        "Ticker": ticker,
        "Company ID": "1",
        "Current Fiscal Year": "2026",
        "Current Fiscal Q": "Q1",
        "Current Period End": "2026-03-31",
        "Current Publish Date": "2026-08-12",
        "Issue Type": "LONG",
        "Signals": "PUBLISH_DUPLICATE_DATE",
        "Latest Quarter Rank": "1",
        "In Latest 4Q": "1",
        "In Latest 8Q": "1",
        "Affects Current TTM": "1",
        "Affects Score": "0",
        "Affects Lifecycle": "0",
        "Affects Valuation": "0",
        "Correct Fiscal Year": "2026",
        "Correct Fiscal Q": "Q1",
        "Correct Period End": "2026-03-31",
        "Correct Publish Date": "2026-05-20",
        "Primary Root Cause": "WRONG_PUBLISH_DATE",
        "Proposed Canonical Action": "UPDATE_PUBLISH_DATE",
        "Economic Quarter Match": "YES",
        "Fundamental Value Repair Required": "NO",
        "Production Ready": ready,
        "Confidence": confidence,
        "Primary Source": "source",
        "Primary Source Type": "SEC",
        "Secondary Source": "",
        "Secondary Source Type": "",
        "Exact Explanation": "ok",
    }


def plan_row(request_id: str = "REQ1", ticker: str = "CAPS", op: str = "UPDATE_PUBLISH_DATE", old: str = "2026-08-12", new: str = "2026-05-20", ready: str = "YES") -> dict[str, str]:
    return {
        "Transformation Group": "G1",
        "Request ID": request_id,
        "Ticker": ticker,
        "Operation Order": "1",
        "Current Fiscal Year": "2026",
        "Current Fiscal Q": "Q1",
        "Current Period End": "2026-03-31",
        "Current Publish Date": old,
        "Target Fiscal Year": "2026",
        "Target Fiscal Q": "Q1",
        "Target Period End": "2026-03-31",
        "Target Publish Date": new,
        "Field": "publish_date" if op == "UPDATE_PUBLISH_DATE" else "period_end",
        "Old Value": old,
        "New Value": new,
        "Operation": op,
        "Merge/Delete/Create Target": "CURRENT_ROW",
        "Evidence": "source",
        "Confidence": "HIGH",
        "Production Ready": ready,
        "Blocking Issue": "",
        "Notes": "",
    }


def current_row(request_id: str = "REQ1", publish: str = "2026-08-12", period: str = "2026-03-31") -> dict:
    return {
        "Request ID": request_id,
        "company_id": 1,
        "ticker": "CAPS",
        "quarter_id": 10,
        "fiscal_year": 2026,
        "fiscal_quarter": "Q1",
        "period_end_date": period,
        "publish_date": publish,
        "revenue": 1.0,
        "gross_profit": 1.0,
        "operating_income": 1.0,
        "ebit": 1.0,
        "ebitda": 1.0,
        "net_income": 1.0,
        "operating_cashflow": 1.0,
        "capex": -1.0,
        "free_cashflow": 0.0,
        "cash": 1.0,
        "total_debt": 1.0,
        "shares_outstanding": 1.0,
    }


def validation_paths(tmp_path: Path) -> phase.Phase8A10FPaths:
    cases = []
    tickers = [f"T{i:02d}" for i in range(30)]
    for i in range(35):
        cases.append(case_row(f"REQ{i:02d}", tickers[i % 30], "YES" if i < 21 else "NO", "MEDIUM" if i == 0 else "HIGH"))
    timeline = [{"Ticker": tickers[i % 30], "Fiscal Year": "2026", "Fiscal Q": "Q1"} for i in range(96)]
    plan = [plan_row(f"REQ{i % 35:02d}", tickers[i % 30]) for i in range(52)]
    write_csv(tmp_path / "cases.csv", cases)
    write_csv(tmp_path / "timeline.csv", timeline)
    write_csv(tmp_path / "plan.csv", plan)
    return phase.Phase8A10FPaths(tmp_path / "art", case_resolution_csv=tmp_path / "cases.csv", official_timeline_csv=tmp_path / "timeline.csv", transformation_plan_csv=tmp_path / "plan.csv")


def test_validate_external_package_35_cases(tmp_path: Path) -> None:
    manifest, *_ = phase.validate_external_package(validation_paths(tmp_path))
    assert manifest["case_rows"] == 35


def test_validate_external_package_30_unique_tickers(tmp_path: Path) -> None:
    manifest, *_ = phase.validate_external_package(validation_paths(tmp_path))
    assert manifest["unique_tickers"] == 30


def test_validate_external_package_ready_blocked_counts(tmp_path: Path) -> None:
    manifest, *_ = phase.validate_external_package(validation_paths(tmp_path))
    assert manifest["production_ready"] == {"NO": 14, "YES": 21}


def test_validate_external_package_confidence_counts(tmp_path: Path) -> None:
    manifest, *_ = phase.validate_external_package(validation_paths(tmp_path))
    assert manifest["confidence"] == {"HIGH": 34, "MEDIUM": 1}


def test_validate_external_package_timeline_and_plan_counts(tmp_path: Path) -> None:
    manifest, *_ = phase.validate_external_package(validation_paths(tmp_path))
    assert manifest["timeline_rows"] == 96
    assert manifest["transformation_rows"] == 52


def test_state_reconciliation_exact_current_match() -> None:
    result = phase.state_reconciliation([case_row()], [current_row()])[0]
    assert result["current_reconciliation_status"] == "EXACT_CURRENT_MATCH"


def test_state_reconciliation_already_resolved_is_harmless_drift() -> None:
    result = phase.state_reconciliation([case_row()], [current_row(publish="2026-05-20")])[0]
    assert result["current_reconciliation_status"] == "ALREADY_RESOLVED"
    assert result["material_drift"] == "NO"


def test_state_reconciliation_material_drift() -> None:
    result = phase.state_reconciliation([case_row()], [current_row(publish="2026-07-01")])[0]
    assert result["current_reconciliation_status"] == "CURRENT_STATE_DRIFT"


def test_state_reconciliation_row_not_found() -> None:
    result = phase.state_reconciliation([case_row()], [])[0]
    assert result["current_reconciliation_status"] == "ROW_NOT_FOUND"


def test_p1_overlap_no_overlap() -> None:
    overlap = phase.p1_overlap([case_row()], [plan_row()], [])
    assert overlap[0]["overlap_class"] == "NO_P1_OVERLAP"


def test_p1_overlap_same_ticker_independent() -> None:
    p1 = [{"ticker": "CAPS", "fiscal_year": "2025", "fiscal_quarter": "Q1"}]
    overlap = phase.p1_overlap([case_row()], [plan_row()], p1)
    assert overlap[0]["overlap_class"] == "SAME_TICKER_INDEPENDENT_CASE"


def test_p1_overlap_blocked_same_ticker_dependency() -> None:
    p1 = [{"ticker": "CAPS", "fiscal_year": "2025", "fiscal_quarter": "Q1"}]
    blocked = case_row(ready="NO")
    overlap = phase.p1_overlap([blocked], [plan_row()], p1)
    assert overlap[0]["overlap_class"] == "P1_DEPENDENT"


def test_p1_overlap_duplicate_repair_detected() -> None:
    p1 = [{"ticker": "CAPS", "fiscal_year": "2026", "fiscal_quarter": "Q1"}]
    overlap = phase.p1_overlap([case_row()], [plan_row(op="UPDATE_PERIOD_END")], p1)
    assert overlap[0]["overlap_class"] == "SAME_CANONICAL_QUARTER_DUPLICATE_REPAIR"


def test_classify_publish_date_only_locally_ready() -> None:
    status, _ = phase.classify_operation(plan_row(), {"current_reconciliation_status": "EXACT_CURRENT_MATCH"}, current_row(), "NO_P1_OVERLAP")
    assert status == "LOCALLY_READY"


def test_classify_period_end_only_locally_ready() -> None:
    op = plan_row(op="UPDATE_PERIOD_END", old="2026-03-31", new="2026-03-28")
    status, _ = phase.classify_operation(op, {"current_reconciliation_status": "EXACT_CURRENT_MATCH"}, current_row(), "NO_P1_OVERLAP")
    assert status == "LOCALLY_READY"


def test_classify_period_publish_group_is_atomic_by_shared_group() -> None:
    ops, _ready, _blockers = phase.build_transformations(
        [case_row()],
        [plan_row(op="UPDATE_PERIOD_END", old="2026-03-31", new="2026-03-28"), plan_row()],
        [current_row()],
        [{"Request ID": "REQ1", "current_reconciliation_status": "EXACT_CURRENT_MATCH", "material_drift": "NO"}],
        [{"Request ID": "REQ1", "overlap_class": "NO_P1_OVERLAP"}],
    )
    assert len({row["transformation_group"] for row in ops}) == 1


def test_avah_fyq_collision_classified_structural() -> None:
    case = case_row(ticker="AVAH", ready="NO")
    case["Proposed Canonical Action"] = "RELABEL_FISCAL_Q"
    assert phase.local_blocker_class(case, [{"operation": "UPDATE_FQ"}], "NO_P1_OVERLAP") == "STRUCTURAL_BLOCKER_REMAINS"


def test_missing_quarter_detection_for_create_op() -> None:
    case = case_row(ticker="AVAH", ready="NO")
    assert phase.local_blocker_class(case, [{"operation": "CREATE_CANONICAL_ROW"}], "NO_P1_OVERLAP") == "MISSING_TARGET_QUARTER"


def test_restatement_field_matrix_blocks_unverified_values() -> None:
    matrix = phase.restatement_matrix([case_row(ready="NO") | {"Primary Root Cause": "RESTATEMENT", "Fundamental Value Repair Required": "YES"}], [current_row()], [])
    assert matrix[0]["field_eligibility"] == "RESTATED_VALUE_NOT_VERIFIABLE"


def test_field_eligibility_matches_restated_value() -> None:
    assert phase.field_eligibility(1.0, "1") == "CURRENT_MATCHES_RESTATED"


def test_field_eligibility_verified_difference() -> None:
    assert phase.field_eligibility(1.0, "3") == "RESTATED_VALUE_VERIFIED"


def test_global_p1_dependency_blocks_write() -> None:
    status, _ = phase.classify_operation(plan_row(), {"current_reconciliation_status": "EXACT_CURRENT_MATCH"}, current_row(), "P1_DEPENDENT")
    assert status == "BLOCKED_BY_GLOBAL_P1"


def test_unverified_restated_value_blocks_write() -> None:
    case = case_row(ready="NO")
    case["Primary Root Cause"] = "RESTATEMENT"
    assert phase.local_blocker_class(case, [], "NO_P1_OVERLAP") == "RESTATEMENT_FIELD_RECONCILIATION_REQUIRED"


def test_already_correct_is_not_write_ready() -> None:
    status, _ = phase.classify_operation(plan_row(), {"current_reconciliation_status": "ALREADY_RESOLVED"}, current_row(publish="2026-05-20"), "NO_P1_OVERLAP")
    assert status == "ALREADY_CORRECT"


def test_old_value_guard_mismatch_blocks_write() -> None:
    status, reason = phase.classify_operation(plan_row(), {"current_reconciliation_status": "EXACT_CURRENT_MATCH"}, current_row(publish="2026-07-01"), "NO_P1_OVERLAP")
    assert status == "BLOCKED_BY_CURRENT_STATE"
    assert "guard mismatch" in reason


def test_post_audit_counts_resolved_and_blocked() -> None:
    rows, summary = phase.post_audit([case_row("REQ1"), case_row("REQ2")], [{"Request ID": "REQ1"}], [{"Request ID": "REQ2"}])
    assert summary["original_35_resolved"] == 1
    assert summary["still_blocked"] == 1
    assert rows[0]["post_rehearsal_status"] == "RESOLVED_BY_REHEARSED_SAFE_REPAIR"


def test_downstream_impact_lists_all_flags() -> None:
    case = case_row()
    case["Affects Score"] = case["Affects Lifecycle"] = case["Affects Valuation"] = "1"
    assert phase.downstream_impact(case) == "TTM,Score,Lifecycle,Valuation"


def test_summary_payload_reports_no_writes() -> None:
    summary = phase.summary_payload(
        phase.CLASSIFICATION_PARTIAL,
        phase.Phase8A10FPaths(Path("art")),
        {"case_rows": 35, "production_ready": {"YES": 21, "NO": 14}},
        [{"current_reconciliation_status": "EXACT_CURRENT_MATCH", "material_drift": "NO"}],
        [{"overlap_class": "NO_P1_OVERLAP", "same_ticker_as_global_P1": 0, "same_FYFQ_as_global_P1": 0}],
        [{"local_status": "LOCALLY_READY"}],
        [],
        [],
        [{"operation": "UPDATE_PUBLISH_DATE", "transformation_group": "G1"}],
        [{"transformation_group": "G1", "rows_changed": 1}],
        {"quick_check": "ok", "duplicate_fy_fq": 0, "orphans": 0},
        {"new_current_critical_cases_introduced": 0},
        {"production_writes": 0, "ttm_writes": 0, "score_writes": 0, "lifecycle_writes": 0, "valuation_writes": 0, "rawcandle_writes": 0},
        {},
    )
    assert summary["safety"]["production_writes"] == 0


def test_classification_constants_are_stable() -> None:
    assert phase.CLASSIFICATION_PARTIAL.endswith("BLOCKERS_REMAIN")
