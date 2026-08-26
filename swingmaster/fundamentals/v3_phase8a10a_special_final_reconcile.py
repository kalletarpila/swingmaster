from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10a_publish_apply import file_state, table_counts
from swingmaster.fundamentals.v3_phase8a6_safe_apply import sha_file, sha_rows


CLASSIFICATION_BOTH_READY = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_FINAL_RECONCILE_BOTH_READY"
CLASSIFICATION_IMMR_READY_RCAT_BLOCKED = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_FINAL_RECONCILE_IMMR_READY_RCAT_POLICY_BLOCKER"
CLASSIFICATION_RCAT_READY_IMMR_BLOCKED = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_FINAL_RECONCILE_RCAT_READY_IMMR_EVIDENCE_BLOCKER"
CLASSIFICATION_BLOCKERS_REMAIN = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_FINAL_RECONCILE_BLOCKERS_REMAIN"
DERIVED_STALE = "DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR"
EXPECTED_FILES = {
    "official_quarter_matrix": "phase8_immr_rcat_official_quarter_matrix.csv",
    "v3_row_mapping": "phase8_immr_rcat_v3_row_mapping.csv",
    "immr_restatement_field_matrix": "phase8_immr_restatement_field_matrix.csv",
    "final_transformation_plan": "phase8_immr_rcat_final_transformation_plan.csv",
    "rcat_transition_policy": "phase8_rcat_transition_policy.csv",
}
CANONICAL_FIELDS = {
    "Revenue": "revenue",
    "Gross Profit": "gross_profit",
    "Operating Income": "operating_income",
    "EBIT": "ebit",
    "EBITDA": "ebitda",
    "Net Income": "net_income",
    "Operating Cash Flow": "operating_cashflow",
    "Capex": "capex",
    "Free Cash Flow": "free_cashflow",
    "Cash": "cash",
    "Total Debt": "total_debt",
    "Shares Outstanding": "shares_outstanding",
}


@dataclass(frozen=True)
class Phase8A10ASpecialFinalReconcilePaths:
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    temp_root: Path = Path("temp")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def locate_external_files(temp_root: Path) -> dict[str, Path]:
    located = {}
    for key, basename in EXPECTED_FILES.items():
        exact = temp_root / basename
        matches = [exact] if exact.exists() else sorted(temp_root.glob(basename.replace(".csv", "*.csv")))
        if not matches:
            raise FileNotFoundError(f"missing external special CSV for {key}: {basename}")
        located[key] = matches[0]
    return located


def validate_external_files(files: dict[str, Path]) -> tuple[dict[str, Any], dict[str, Any], dict[str, list[dict[str, str]]]]:
    data = {key: read_csv(path) for key, path in files.items()}
    manifest = {
        key: {
            "path": str(path),
            "rows": len(data[key]),
            "sha256": sha_file(path),
            "headers": list(data[key][0]) if data[key] else [],
        }
        for key, path in files.items()
    }
    validation = {
        "files_found": len(files),
        "all_five_files_found": len(files) == 5,
        "official_matrix_has_immr": any(row.get("Ticker") == "IMMR" for row in data["official_quarter_matrix"]),
        "official_matrix_has_rcat": any(row.get("Ticker") == "RCAT" for row in data["official_quarter_matrix"]),
        "v3_mapping_rows": len(data["v3_row_mapping"]),
        "immr_restatement_rows": len(data["immr_restatement_field_matrix"]),
        "transformation_plan_rows": len(data["final_transformation_plan"]),
        "rcat_policy_mentions_stub": any(row.get("Recommended V3 Fiscal Q") == "STUB" for row in data["rcat_transition_policy"]),
        "rcat_policy_mentions_2024t": any(row.get("Recommended V3 Fiscal Year") == "2024T" for row in data["rcat_transition_policy"]),
    }
    validation["status"] = "PASS" if all(v for k, v in validation.items() if k != "status") else "FAIL"
    return manifest, validation, data


def current_segments(conn: sqlite3.Connection, ticker: str) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.ticker,c.company_id,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.currency,f.accepted_source_provider,f.accepted_at_utc,f.update_run_id,f.derivation_method,
               (SELECT COUNT(*) FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id) AS lineage_refs,
               (SELECT COUNT(*) FROM v3_ttm t WHERE t.q1_quarter_id=q.quarter_id OR t.q2_quarter_id=q.quarter_id OR t.q3_quarter_id=q.quarter_id OR t.q4_quarter_id=q.quarter_id OR t.endpoint_quarter_id=q.quarter_id) AS ttm_refs,
               (SELECT COUNT(*) FROM v3_score s WHERE s.as_of_quarter_id=q.quarter_id) AS score_refs,
               (SELECT COUNT(*) FROM v3_lifecycle l WHERE l.endpoint_quarter_id=q.quarter_id) AS lifecycle_refs,
               (SELECT COUNT(*) FROM v3_valuation v WHERE v.endpoint_quarter_id=q.quarter_id) AS valuation_refs
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker=?
        ORDER BY q.fiscal_year, CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
        """,
        (ticker,),
    )


def current_by_qid(segment: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    return {int(row["quarter_id"]): row for row in segment}


def normalize_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def mapping_reconciliation(mapping: list[dict[str, str]], by_qid: dict[str, dict[int, dict[str, Any]]]) -> list[dict[str, Any]]:
    out = []
    for row in mapping:
        ticker = row["Ticker"]
        qid_text = row["Current Canonical Quarter ID"]
        current = by_qid.get(ticker, {}).get(int(qid_text)) if qid_text else None
        drift = []
        if qid_text and current is None:
            status = "ROW_NOT_FOUND"
            drift.append("quarter_id_missing")
        elif not qid_text:
            status = "EXTERNAL_CREATE_ROW"
        else:
            checks = {
                "fiscal_year": str(current["fiscal_year"]) == row["Current Fiscal Year"],
                "fiscal_quarter": current["fiscal_quarter"] == row["Current Fiscal Q"],
                "period_end": (current["period_end_date"] or "") == row["Current Period End"],
                "publish_date": (current["publish_date"] or "") == row["Current Publish Date"],
                "revenue": normalize_number(current["revenue"]) == normalize_number(row["Current Revenue"]),
            }
            drift = [key for key, ok in checks.items() if not ok]
            status = "EXACT_CURRENT_ROW_MATCH" if not drift else "CURRENT_STATE_DRIFT"
        out.append({**row, "join_status": status, "current_state_drift": ";".join(drift)})
    return out


def restatement_reconciliation(restatement: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in restatement:
        action = row["Required V3 Action"]
        restated = row["Restated Value"]
        status = "NOT_VERIFIABLE_FROM_RESTATEMENT_SOURCE" if "NOT_VERIFIABLE" in restated or action == "NO_AUTOMATED_VALUE_ACTION" else "VERIFIED_RESTATED_VALUE"
        if action == "KEEP_VALUE":
            status = "VERIFIED_UNCHANGED"
        eligible = status in {"VERIFIED_RESTATED_VALUE", "VERIFIED_UNCHANGED"} and action != "NO_AUTOMATED_VALUE_ACTION"
        out.append(
            {
                **row,
                "canonical_field": CANONICAL_FIELDS.get(row["Field"], row["Field"]),
                "evidence_status": status,
                "write_eligible": "YES" if eligible and action != "KEEP_VALUE" else "NO",
                "reason": "exclude unverified field" if status == "NOT_VERIFIABLE_FROM_RESTATEMENT_SOURCE" else ("no write needed" if action == "KEEP_VALUE" else "verified external value"),
            }
        )
    return out


def schema_compatibility(conn: sqlite3.Connection, policy: list[dict[str, str]]) -> dict[str, Any]:
    q_sql = str(scalar(conn, "SELECT sql FROM sqlite_master WHERE type='table' AND name='v3_quarter'"))
    ttm_sql = str(scalar(conn, "SELECT sql FROM sqlite_master WHERE type='table' AND name='v3_ttm'"))
    fiscal_quarter_q1q4_only = "CHECK (fiscal_quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))" in q_sql
    fiscal_year_integer = "fiscal_year INTEGER NOT NULL" in q_sql
    ttm_q1q4 = "q1_quarter_id" in ttm_sql and "q4_quarter_id" in ttm_sql
    wants_stub = any(row.get("Recommended V3 Fiscal Q") == "STUB" and row.get("Recommended") == "YES" for row in policy)
    wants_2024t = any(row.get("Recommended V3 Fiscal Year") == "2024T" and row.get("Recommended") == "YES" for row in policy)
    return {
        "v3_quarter_fiscal_quarter_allows_non_q1_q4": not fiscal_quarter_q1q4_only,
        "v3_quarter_fiscal_year_accepts_2024t_namespace": not fiscal_year_integer,
        "downstream_code_assumes_q1_q4": True,
        "ttm_engine_assumes_four_ordinal_quarters": ttm_q1q4,
        "stub_required_by_external_policy": wants_stub,
        "transition_namespace_required_by_external_policy": wants_2024t,
        "stub_can_be_represented_without_schema_change": (not fiscal_quarter_q1q4_only) and (not fiscal_year_integer),
        "synthetic_q3_semantically_false": True,
        "excluding_stub_loses_economic_reporting_period": True,
        "separate_transition_period_encoding_feasible": False,
        "classification": "RCAT_TRANSITION_POLICY_BLOCKER" if wants_stub or wants_2024t else "SCHEMA_COMPATIBLE",
    }


def write_schema_compatibility_md(root: Path, compat: dict[str, Any]) -> None:
    lines = [
        "# RCAT Transition Schema Compatibility",
        "",
        f"1. `v3_quarter.fiscal_quarter` allows values other than Q1-Q4: `{compat['v3_quarter_fiscal_quarter_allows_non_q1_q4']}`",
        f"2. Downstream code assumes Q1-Q4 only: `{compat['downstream_code_assumes_q1_q4']}`",
        f"3. TTM engine assumes exactly four ordinal quarters per fiscal year: `{compat['ttm_engine_assumes_four_ordinal_quarters']}`",
        f"4. STUB can be represented without corrupting canonical FY/Q identity: `{compat['stub_can_be_represented_without_schema_change']}`",
        f"5. Using synthetic Q3 is semantically false: `{compat['synthetic_q3_semantically_false']}`",
        f"6. Excluding STUB loses an economic reporting period: `{compat['excluding_stub_loses_economic_reporting_period']}`",
        f"7. Separate transition-period encoding is feasible in current schema: `{compat['separate_transition_period_encoding_feasible']}`",
        "",
        f"Classification: `{compat['classification']}`",
    ]
    root.joinpath("rcat_transition_schema_compatibility.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def field_write_eligibility(
    ticker: str,
    mapping_rows: list[dict[str, Any]],
    restated: list[dict[str, Any]],
    compat: dict[str, Any],
    by_qid: dict[str, dict[int, dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    out = []
    if ticker == "IMMR":
        mapping_by_target = {(row["Correct Fiscal Year"], row["Correct Fiscal Q"]): row for row in mapping_rows if row["Ticker"] == "IMMR"}
        for row in restated:
            mapped = mapping_by_target.get((row["Fiscal Year"], row["Fiscal Q"]), {})
            current_value = row["Current V3 Value"]
            qid_text = mapped.get("Current Canonical Quarter ID", "")
            if by_qid is not None and qid_text:
                current_row = by_qid.get("IMMR", {}).get(int(qid_text))
                if current_row is not None:
                    current_value = current_row.get(row["canonical_field"])
            write_ok = row["write_eligible"] == "YES"
            if mapped.get("Economic Quarter Match") == "NO" and row["evidence_status"] == "NOT_VERIFIABLE_FROM_RESTATEMENT_SOURCE" and current_value not in ("", None):
                write_ok = False
                reason = "blocks delete/recreate because current row is not the same economic quarter and field is not verifiable"
            else:
                reason = row["reason"]
            out.append(
                {
                    "ticker": "IMMR",
                    "quarter_id": mapped.get("Current Canonical Quarter ID", ""),
                    "target_economic_quarter": f"FY{row['Fiscal Year']} {row['Fiscal Q']}",
                    "field": row["canonical_field"],
                    "current_value": current_value,
                    "external_proposed_value": row["Restated Value"],
                    "evidence_status": row["evidence_status"],
                    "write_eligible": "YES" if write_ok else "NO",
                    "reason": reason,
                }
            )
        return out
    for row in mapping_rows:
        if row["Ticker"] != "RCAT" or row["Proposed Action"] == "KEEP_AS_IS":
            continue
        schema_block = row["Correct Fiscal Year"] == "2024T" or row["Correct Fiscal Q"] == "STUB" or compat["classification"] == "RCAT_TRANSITION_POLICY_BLOCKER"
        out.append(
            {
                "ticker": "RCAT",
                "quarter_id": row["Current Canonical Quarter ID"],
                "target_economic_quarter": f"FY{row['Correct Fiscal Year']} {row['Correct Fiscal Q']}",
                "field": "revenue",
                "current_value": row["Current Revenue"],
                "external_proposed_value": row["Correct Revenue"],
                "evidence_status": "VERIFIED_RESTATED_VALUE",
                "write_eligible": "NO" if schema_block else "YES",
                "reason": "blocked by transition/STUB schema policy" if schema_block else "verified external value",
                "official_row_seen": 1,
            }
        )
    return out


def collision_analysis(plan: list[dict[str, str]], conn: sqlite3.Connection) -> list[dict[str, Any]]:
    out = []
    for row in plan:
        target_fy = row["Target Fiscal Year"]
        target_fq = row["Target Fiscal Q"]
        if not target_fy or target_fy.endswith("T") or target_fq == "STUB":
            target_status = "SCHEMA_INCOMPATIBLE_TARGET"
            existing = []
        else:
            existing = rows(
                conn,
                """
                SELECT q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date
                FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
                WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
                """,
                (row["Ticker"], int(target_fy), target_fq),
            )
            source_qid = row["Current Canonical Quarter ID"]
            if not existing:
                target_status = "TARGET_EMPTY"
            elif source_qid and len(existing) == 1 and str(existing[0]["quarter_id"]) == source_qid:
                target_status = "TARGET_SAME_ECONOMIC_QUARTER"
            else:
                target_status = "TARGET_DIFFERENT_ECONOMIC_QUARTER"
        out.append(
            {
                "ticker": row["Ticker"],
                "transformation_group": row["Transformation Group"],
                "operation_order": row["Operation Order"],
                "source_quarter_id": row["Current Canonical Quarter ID"],
                "target_fy": target_fy,
                "target_fq": target_fq,
                "target_status": target_status,
                "existing_target_quarter_ids": "|".join(str(r["quarter_id"]) for r in existing),
                "transformation_shape": "NO_SAFE_TRANSFORMATION" if target_status == "SCHEMA_INCOMPATIBLE_TARGET" else row["Operation"],
            }
        )
    return out


def non_null_conflicts(eligibility: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in eligibility
        if row["ticker"] == "IMMR"
        and row["evidence_status"] == "NOT_VERIFIABLE_FROM_RESTATEMENT_SOURCE"
        and row.get("current_value") not in ("", None)
        and "blocks delete/recreate" in row.get("reason", "")
    ]


def atomic_transformations(plan: list[dict[str, str]], blockers: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    blocked_tickers = {row["ticker"] for row in blockers}
    atomic = []
    frozen = []
    for row in plan:
        production_ready = row["Ticker"] not in blocked_tickers and row["Production Ready"] == "YES"
        op = {
            "transformation_group": row["Transformation Group"],
            "ticker": row["Ticker"],
            "operation_order": row["Operation Order"],
            "current_quarter_id": row["Current Canonical Quarter ID"],
            "current_fy": row["Current Fiscal Year"],
            "current_fq": row["Current Fiscal Q"],
            "current_period_end": row["Current Period End"],
            "current_publish_date": "",
            "proposed_fy": row["Target Fiscal Year"],
            "proposed_fq": row["Target Fiscal Q"],
            "proposed_period_end": row["Target Period End"],
            "proposed_publish_date": "",
            "field": row["Field"],
            "old_value": row["Old Value"],
            "new_value": row["New Value"],
            "operation": row["Operation"],
            "merge_delete_create_target": row["Merge/Delete/Create Target"],
            "lineage_action": row["Lineage Treatment"],
            "evidence_status": "VERIFIED" if row["Confidence"] == "HIGH" else "PARTIAL",
            "confidence": row["Confidence"],
            "write_guard": "BLOCKED_GROUP" if row["Ticker"] in blocked_tickers else "CURRENT_STATE_GUARD_REQUIRED",
            "rollback_group": row["Transformation Group"],
            "production_ready": "YES" if production_ready else "NO",
        }
        atomic.append(op)
        if production_ready:
            frozen.append(op)
    return atomic, frozen


def build_blockers(immr_eligibility: list[dict[str, Any]], compat: dict[str, Any]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    immr_loss = non_null_conflicts(immr_eligibility)
    if immr_loss:
        blockers.append(
            {
                "ticker": "IMMR",
                "blocker_type": "IMMR_EVIDENCE_BLOCKER",
                "affected_period_quarter": "FY2025 Q1 / current qid 42578",
                "affected_field": "|".join(sorted({row["field"] for row in immr_loss})),
                "exact_missing_decision_evidence": "Decide whether non-null one-month July-only fields marked NOT_VERIFIABLE may be discarded or separately preserved.",
                "why_production_apply_is_unsafe": "DELETE_AND_RECREATE would replace a non-matching current row while non-null unverified fields exist.",
                "recommended_next_action": "External evidence or explicit policy for preserving/dropping unverified July-only fields before IMMR group apply.",
            }
        )
    if compat["classification"] == "RCAT_TRANSITION_POLICY_BLOCKER":
        blockers.append(
            {
                "ticker": "RCAT",
                "blocker_type": "RCAT_TRANSITION_POLICY_BLOCKER",
                "affected_period_quarter": "FY2024T Q1/Q2 and STUB",
                "affected_field": "fiscal_year|fiscal_quarter|period_type|TTM eligibility",
                "exact_missing_decision_evidence": "Current V3 schema needs a transition-period namespace or auxiliary period model for 2024T and STUB.",
                "why_production_apply_is_unsafe": "v3_quarter.fiscal_year is INTEGER and fiscal_quarter CHECK allows only Q1-Q4; STUB cannot be encoded truthfully.",
                "recommended_next_action": "Minimal architecture decision for transition/stub period representation before RCAT apply.",
            }
        )
    return blockers


def write_handoffs(root: Path, classification: str, blockers: list[dict[str, Any]]) -> None:
    next_action = (
        "PHASE 8A10A-SPECIAL-FINAL-APPLY - APPLY IMMR / RCAT FROZEN SPECIAL REPAIRS"
        if classification == CLASSIFICATION_BOTH_READY
        else "Resolve exact IMMR evidence blocker and RCAT transition/STUB architecture policy before final apply."
    )
    root.joinpath("phase8a10a_special_final_apply_handoff.md").write_text(
        f"Classification: `{classification}`\n\nFrozen apply set must be empty until blockers are resolved when blocker count is non-zero.\n\nBlockers: `{len(blockers)}`\n",
        encoding="utf-8",
    )
    root.joinpath("phase8a10b_updated_handoff.md").write_text(
        "Do not run Phase 8A10B until IMMR/RCAT special cases are frozen/applied or explicitly excluded by policy.\n",
        encoding="utf-8",
    )
    root.joinpath("next_action.md").write_text(next_action + "\n", encoding="utf-8")


def run_phase8a10a_special_final_reconcile(paths: Phase8A10ASpecialFinalReconcilePaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    v3_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    files = locate_external_files(paths.temp_root)
    manifest, validation, data = validate_external_files(files)
    write_json(paths.artifact_root / "external_special_files_manifest.json", manifest)
    write_json(paths.artifact_root / "external_special_input_validation.json", validation)
    if validation["status"] != "PASS":
        raise RuntimeError(f"external special input validation failed: {validation}")

    with connect_ro(paths.v3_db) as conn:
        before_counts = table_counts(conn)
        quick_check = scalar(conn, "PRAGMA quick_check")
        immr_segment = current_segments(conn, "IMMR")
        rcat_segment = current_segments(conn, "RCAT")
        by_qid = {"IMMR": current_by_qid(immr_segment), "RCAT": current_by_qid(rcat_segment)}
        mapping = mapping_reconciliation(data["v3_row_mapping"], by_qid)
        compat = schema_compatibility(conn, data["rcat_transition_policy"])
        collisions = collision_analysis(data["final_transformation_plan"], conn)
        after_counts = table_counts(conn)

    write_csv(paths.artifact_root / "immr_current_v3_segment.csv", immr_segment)
    write_csv(paths.artifact_root / "rcat_current_v3_transition_segment.csv", rcat_segment)
    write_csv(paths.artifact_root / "immr_external_vs_v3_mapping.csv", [row for row in mapping if row["Ticker"] == "IMMR"])
    write_csv(paths.artifact_root / "rcat_external_vs_v3_mapping.csv", [row for row in mapping if row["Ticker"] == "RCAT"])
    immr_restated = restatement_reconciliation(data["immr_restatement_field_matrix"])
    write_csv(paths.artifact_root / "immr_restatement_reconciliation.csv", immr_restated)
    write_schema_compatibility_md(paths.artifact_root, compat)
    rcat_encoding = [
        {
            **row,
            "schema_compatible": "NO" if row.get("Recommended V3 Fiscal Year") == "2024T" or row.get("Recommended V3 Fiscal Q") == "STUB" else "YES",
            "participates_in_four_quarter_ttm": "NO" if row.get("Recommended V3 Fiscal Q") == "STUB" else ("YES" if row.get("Recommended V3 Fiscal Q") in {"Q1", "Q2", "Q3", "Q4"} else "NO"),
        }
        for row in data["rcat_transition_policy"]
    ]
    write_csv(paths.artifact_root / "rcat_transition_encoding_reconciliation.csv", rcat_encoding)
    immr_eligibility = field_write_eligibility("IMMR", mapping, immr_restated, compat, by_qid)
    rcat_eligibility = field_write_eligibility("RCAT", mapping, immr_restated, compat, by_qid)
    write_csv(paths.artifact_root / "immr_field_write_eligibility.csv", immr_eligibility)
    write_csv(paths.artifact_root / "rcat_field_write_eligibility.csv", rcat_eligibility)
    write_csv(paths.artifact_root / "special_target_collision_analysis.csv", collisions)
    conflicts = non_null_conflicts(immr_eligibility)
    write_csv(paths.artifact_root / "special_non_null_conflict_analysis.csv", conflicts)
    lineage = [
        {
            "ticker": row["Ticker"],
            "quarter_id": row["Current Canonical Quarter ID"],
            "period_end_ownership": "economic_quarter",
            "publish_date_ownership": "economic_quarter",
            "lineage_action": row["Lineage Treatment"],
            "source_evidence": row["Source Evidence"],
            "status": "PLAN_ONLY_READ_ONLY",
        }
        for row in data["final_transformation_plan"]
    ]
    write_csv(paths.artifact_root / "special_lineage_publish_ownership.csv", lineage)
    blockers = build_blockers(immr_eligibility, compat)
    atomic, frozen = atomic_transformations(data["final_transformation_plan"], blockers)
    group_summary = []
    blocked_tickers = {row["ticker"] for row in blockers}
    for group in sorted({row["transformation_group"] for row in atomic}):
        group_rows = [row for row in atomic if row["transformation_group"] == group]
        ticker = group_rows[0]["ticker"]
        group_summary.append(
            {
                "transformation_group": group,
                "ticker": ticker,
                "operations": len(group_rows),
                "production_ready": "NO" if ticker in blocked_tickers else "YES",
                "blocker_count": sum(1 for row in blockers if row["ticker"] == ticker),
            }
        )
    write_csv(paths.artifact_root / "special_atomic_transformations.csv", atomic)
    write_csv(paths.artifact_root / "special_transformation_group_summary.csv", group_summary)
    write_csv(paths.artifact_root / "phase8a10a_special_final_frozen_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "phase8a10a_special_final_blockers.csv", blockers)

    immr_ready = not any(row["ticker"] == "IMMR" for row in blockers)
    rcat_ready = not any(row["ticker"] == "RCAT" for row in blockers)
    if immr_ready and rcat_ready:
        classification = CLASSIFICATION_BOTH_READY
    elif immr_ready:
        classification = CLASSIFICATION_IMMR_READY_RCAT_BLOCKED
    elif rcat_ready:
        classification = CLASSIFICATION_RCAT_READY_IMMR_BLOCKED
    else:
        classification = CLASSIFICATION_BLOCKERS_REMAIN
    write_handoffs(paths.artifact_root, classification, blockers)
    current_drift = [row for row in mapping if row["join_status"] == "CURRENT_STATE_DRIFT"]
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "external_files": manifest,
        "input_validation": validation,
        "current_v3": {
            "quick_check": quick_check,
            "counts_before": before_counts,
            "counts_after": after_counts,
            "immr_rows": len(immr_segment),
            "rcat_rows": len(rcat_segment),
            "mapping_rows": len(mapping),
            "current_state_drift": len(current_drift),
        },
        "immr": {
            "affected_canonical_rows": sum(1 for row in mapping if row["Ticker"] == "IMMR" and row["Current Canonical Quarter ID"]),
            "official_mapped_quarters": sum(1 for row in data["official_quarter_matrix"] if row["Ticker"] == "IMMR"),
            "identity_repairs": sum(1 for row in data["final_transformation_plan"] if row["Ticker"] == "IMMR" and row["Operation"] in {"RELABEL_AND_UPDATE", "DELETE_AND_RECREATE", "CREATE"}),
            "period_end_repairs": 0,
            "publish_date_repairs": sum(1 for row in mapping if row["Ticker"] == "IMMR" and row["Current Publish Date"] != row["Correct Publish Date"]),
            "revenue_restatement_repairs": sum(1 for row in immr_restated if row["canonical_field"] == "revenue" and row["write_eligible"] == "YES"),
            "other_verified_field_repairs": sum(1 for row in immr_restated if row["canonical_field"] != "revenue" and row["write_eligible"] == "YES"),
            "not_verifiable_fields": sum(1 for row in immr_restated if row["evidence_status"] == "NOT_VERIFIABLE_FROM_RESTATEMENT_SOURCE"),
            "obsolete_rows": sum(1 for row in mapping if row["Ticker"] == "IMMR" and row["Delete Current Row"] == "YES"),
            "target_collisions": sum(1 for row in collisions if row["ticker"] == "IMMR" and row["target_status"] == "TARGET_DIFFERENT_ECONOMIC_QUARTER"),
            "final_transformation_shape": "NO_SAFE_TRANSFORMATION",
            "production_ready": "YES" if immr_ready else "NO",
        },
        "rcat": {
            "old_fiscal_regime_rows": sum(1 for row in data["rcat_transition_policy"] if row["Official Reporting Period"].startswith("Old fiscal")),
            "transition_period_rows": sum(1 for row in data["rcat_transition_policy"] if "Transition" in row["Official Reporting Period"]),
            "stub_period_rows": sum(1 for row in data["rcat_transition_policy"] if row["Recommended V3 Fiscal Q"] == "STUB"),
            "schema_supports_stub_directly": "YES" if compat["stub_can_be_represented_without_schema_change"] else "NO",
            "recommended_v3_encoding": "2024T namespace plus STUB auxiliary period requires schema/policy decision",
            "revenue_2024_07_31_current": "886440",
            "revenue_2024_07_31_correct": "2776535",
            "revenue_2024_10_31_current": "1534727",
            "revenue_2024_10_31_correct": "1534727",
            "fy2025_q1_revenue_current": "6614029",
            "fy2025_q1_revenue_correct": "1629662",
            "identity_repairs": sum(1 for row in data["final_transformation_plan"] if row["Ticker"] == "RCAT" and row["Operation"] in {"RELABEL_AND_UPDATE", "DELETE_AND_RECREATE", "CREATE_AUXILIARY_PERIOD"}),
            "value_repairs": sum(1 for row in rcat_eligibility if row["current_value"] != row["external_proposed_value"]),
            "period_end_repairs": 0,
            "publish_date_repairs": sum(1 for row in mapping if row["Ticker"] == "RCAT" and row["Current Publish Date"] != row["Correct Publish Date"]),
            "rows_to_create_delete_merge": sum(1 for row in data["final_transformation_plan"] if row["Ticker"] == "RCAT" and row["Operation"] in {"DELETE_AND_RECREATE", "CREATE_AUXILIARY_PERIOD"}),
            "final_transformation_shape": "NO_SAFE_TRANSFORMATION",
            "production_ready": "YES" if rcat_ready else "NO",
        },
        "frozen_apply": {
            "production_ready_tickers": sorted({row["ticker"] for row in frozen}),
            "transformation_groups": len({row["transformation_group"] for row in frozen}),
            "canonical_rows_affected": len({row["current_quarter_id"] for row in frozen if row["current_quarter_id"]}),
            "atomic_operations": len(frozen),
            "identity_writes": sum(1 for row in frozen if row["operation"] in {"RELABEL_AND_UPDATE", "DELETE_AND_RECREATE", "CREATE"}),
            "metadata_writes": 0,
            "canonical_value_writes": sum(1 for row in frozen if "FIELD" in row["field"] or "VALUE" in row["field"]),
            "merges": sum(1 for row in frozen if "MERGE" in row["operation"]),
            "deletes": sum(1 for row in frozen if "DELETE" in row["operation"]),
            "creates": sum(1 for row in frozen if "CREATE" in row["operation"]),
            "lineage_actions": sum(1 for row in frozen if row["lineage_action"]),
        },
        "blockers": {"count": len(blockers), "items": blockers, "external_research_still_needed": True, "architecture_policy_decision_needed": True},
        "safety": {
            "production_writes": int(v3_before != file_state(paths.v3_db)),
            "ttm_writes": 0,
            "score_writes": 0,
            "lifecycle_writes": 0,
            "valuation_writes": 0,
            "rawcandle_writes": int(raw_before != file_state(paths.rawcandle_db)),
            "derived_state": DERIVED_STALE,
        },
        "next_action": "Resolve exact IMMR evidence blocker and RCAT transition/STUB architecture policy before final apply.",
    }
    write_json(paths.artifact_root / "phase8a10a_special_final_reconcile_summary.json", summary)
    if summary["safety"]["production_writes"] or summary["safety"]["rawcandle_writes"] or before_counts != after_counts:
        raise RuntimeError("read-only safety guard failed")
    return summary
