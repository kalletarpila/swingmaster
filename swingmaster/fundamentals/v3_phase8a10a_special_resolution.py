from __future__ import annotations

import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv


CLASSIFICATION = "FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_PARTIAL_APPLY_SET_READY_EVIDENCE_REMAINS"
DERIVED_STALE = "DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR"
SPECIAL_TICKERS = {"FNGR", "IMMR", "RCAT"}
FUNDAMENTAL_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)


@dataclass(frozen=True)
class Phase8A10ASpecialPaths:
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    a10ar_root: Path = Path("temp/fundamentals_v3_phase8a10a_r_remap_reconciliation/20260826T071127Z")
    apply_root: Path = Path("temp/fundamentals_v3_phase8a10a_apply/20260826T091635Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def file_state(path: Path) -> dict[str, Any]:
    return {"exists": path.exists(), "size": path.stat().st_size if path.exists() else None, "mtime_ns": path.stat().st_mtime_ns if path.exists() else None}


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}"))
        for table in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")
    }


def validate_remaining_tickers(post_apply_rows: list[dict[str, str]]) -> None:
    tickers = {row["ticker"] for row in post_apply_rows}
    if tickers != SPECIAL_TICKERS:
        raise RuntimeError(f"unexpected special ticker set: {sorted(tickers)}")


def current_timelines(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.ticker,c.company_id,c.company_name,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date AS period_end,q.publish_date,q.market_availability_date,q.q_lifecycle,q.sec_confirmation_state,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.accepted_source_provider,f.accepted_at_utc,f.update_run_id,f.derivation_method,
               (SELECT COUNT(*) FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id) AS lineage_refs,
               (SELECT group_concat(source || ':' || source_key,' | ') FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id) AS lineage_keys,
               (SELECT COUNT(*) FROM v3_ttm t WHERE t.endpoint_quarter_id=q.quarter_id) AS ttm_refs,
               (SELECT COUNT(*) FROM v3_score s WHERE s.as_of_quarter_id=q.quarter_id) AS score_refs,
               (SELECT COUNT(*) FROM v3_lifecycle l WHERE l.endpoint_quarter_id=q.quarter_id) AS lifecycle_refs,
               (SELECT COUNT(*) FROM v3_valuation v WHERE v.endpoint_quarter_id=q.quarter_id) AS valuation_refs
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker IN ('FNGR','IMMR','RCAT') AND q.period_end_date >= '2018-01-01'
        ORDER BY c.ticker,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END
        """,
    )


def official_rows(timeline: list[dict[str, str]], ticker: str) -> list[dict[str, str]]:
    return [row for row in timeline if row["Ticker"] == ticker]


def remap_rows(remaps: list[dict[str, str]], ticker: str) -> list[dict[str, str]]:
    return [row for row in remaps if row["Ticker"] == ticker]


def number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(str(value).replace(",", ""))


def close_enough(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return False
    return abs(left - right) <= max(1.0, abs(left), abs(right)) * 0.01


def by_identity(current: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    return {(row["ticker"], str(row["fiscal_year"]), row["fiscal_quarter"]): row for row in current}


def official_by_identity(timeline: list[dict[str, str]]) -> dict[tuple[str, str, str], dict[str, str]]:
    return {(row["Ticker"], row["Fiscal Year"], row["Fiscal Q"]): row for row in timeline}


def official_for_remap(source: dict[str, Any], remap: dict[str, str], official: dict[tuple[str, str, str], dict[str, str]]) -> dict[str, str]:
    if remap["Ticker"] == "RCAT":
        for row in official.values():
            if row["Ticker"] == "RCAT" and row["Official Period End"] == source["period_end"] and row["Fiscal Year"].endswith("T"):
                return row
    return official.get((remap["Ticker"], remap["Proposed Fiscal Year"], remap["Proposed Fiscal Q"]), {})


def fngr_resolution(current: list[dict[str, Any]], official: dict[tuple[str, str, str], dict[str, str]]) -> tuple[list[dict[str, Any]], str]:
    row = by_identity(current)[("FNGR", "2024", "Q2")]
    off = official[("FNGR", "2024", "Q2")]
    revenue_match = close_enough(number(row["revenue"]), number(off["Revenue"]))
    ready = row["period_end"] == "2024-05-31" and off["Official Period End"] == "2023-08-31" and revenue_match
    operations = []
    if ready:
        operations.append(operation("P8A10A-SPECIAL-FNGR", row, "period_end", row["period_end"], "2023-08-31", "UPDATE_PERIOD_END", off))
        if (row["publish_date"] or "") != off["Publish Date"]:
            operations.append(operation("P8A10A-SPECIAL-FNGR", row, "publish_date", row["publish_date"] or "", off["Publish Date"], "UPDATE_PUBLISH_DATE", off))
    status = "PRODUCTION_READY" if ready else "EXTERNAL_EVIDENCE_REQUIRED"
    return operations, status


def operation(group_id: str, row: dict[str, Any], field: str, old: Any, new: Any, op: str, off: dict[str, str], *, proposed_fy: str | None = None, proposed_fq: str | None = None, merge_target: str = "") -> dict[str, Any]:
    return {
        "transformation_group_id": group_id,
        "ticker": row["ticker"],
        "current_canonical_quarter_id": row["quarter_id"],
        "current_fy": row["fiscal_year"],
        "current_fq": row["fiscal_quarter"],
        "current_period_end": row["period_end"],
        "current_publish_date": row["publish_date"] or "",
        "proposed_fy": proposed_fy or row["fiscal_year"],
        "proposed_fq": proposed_fq or row["fiscal_quarter"],
        "proposed_period_end": off.get("Official Period End", row["period_end"]),
        "proposed_publish_date": off.get("Publish Date", row["publish_date"] or ""),
        "field": field,
        "old_value": old if old is not None else "",
        "new_value": new if new is not None else "",
        "operation": op,
        "merge_delete_target": merge_target,
        "lineage_action": "PRESERVE_QUARTER_ID_LINEAGE",
        "source_evidence": off.get("Source 1", ""),
        "confidence": off.get("Confidence", "HIGH"),
        "write_guard": f"quarter_id={row['quarter_id']} AND FY={row['fiscal_year']} AND FQ={row['fiscal_quarter']} AND period_end={row['period_end']}",
        "rollback_instruction": f"restore {field} to {old}",
    }


def economic_content_match(current: list[dict[str, Any]], official: dict[tuple[str, str, str], dict[str, str]], remaps: list[dict[str, str]]) -> list[dict[str, Any]]:
    current_by_id = by_identity(current)
    out = []
    for remap in remaps:
        source = current_by_id.get((remap["Ticker"], remap["Current Fiscal Year"], remap["Current Fiscal Q"]))
        off = official_for_remap(source, remap, official) if source else {}
        if source is None:
            cls = "UNRESOLVED"
        elif close_enough(number(source["revenue"]), number(off.get("Revenue"))) and source["period_end"] == remap["Current Period End"]:
            cls = "EXACT_ECONOMIC_MATCH" if source["period_end"] == off.get("Official Period End") else "CONTENT_MATCH_WITH_WRONG_LABEL"
        elif source["period_end"] == off.get("Official Period End"):
            cls = "PARTIAL_MATCH"
        else:
            cls = "UNRESOLVED"
        out.append(
            {
                "ticker": remap["Ticker"],
                "quarter_id": source.get("quarter_id", "") if source else "",
                "current_fy": remap["Current Fiscal Year"],
                "current_fq": remap["Current Fiscal Q"],
                "current_period_end": source.get("period_end", "") if source else "",
                "current_revenue": source.get("revenue", "") if source else "",
                "proposed_fy": remap["Proposed Fiscal Year"],
                "proposed_fq": remap["Proposed Fiscal Q"],
                "official_period_end": off.get("Official Period End", ""),
                "official_revenue": off.get("Revenue", ""),
                "classification": cls,
            }
        )
    return out


def target_collisions(current: list[dict[str, Any]], remaps: list[dict[str, str]]) -> list[dict[str, Any]]:
    current_by_id = by_identity(current)
    source_keys = {(r["Ticker"], r["Current Fiscal Year"], r["Current Fiscal Q"]) for r in remaps}
    out = []
    for remap in remaps:
        target_key = (remap["Ticker"], remap["Proposed Fiscal Year"], remap["Proposed Fiscal Q"])
        target = current_by_id.get(target_key)
        if target is None:
            cls = "MISSING_CANONICAL_TARGET"
        elif target_key in source_keys:
            cls = "DUPLICATE_ECONOMIC_QUARTER"
        else:
            cls = "TARGET_EXISTS_REVIEW"
        out.append({"ticker": remap["Ticker"], "target_fy": remap["Proposed Fiscal Year"], "target_fq": remap["Proposed Fiscal Q"], "target_quarter_id": target.get("quarter_id", "") if target else "", "classification": cls})
    return out


def non_null_conflicts(current: list[dict[str, Any]], official: dict[tuple[str, str, str], dict[str, str]], remaps: list[dict[str, str]]) -> list[dict[str, Any]]:
    current_by_id = by_identity(current)
    out = []
    for remap in remaps:
        source = current_by_id.get((remap["Ticker"], remap["Current Fiscal Year"], remap["Current Fiscal Q"]))
        off = official_for_remap(source, remap, official) if source else {}
        conflicts = []
        if source and off.get("Revenue") and not close_enough(number(source["revenue"]), number(off["Revenue"])):
            conflicts.append("revenue")
        if remap["Ticker"] in {"IMMR", "RCAT"} and conflicts:
            review_fields = ",".join(field for field in FUNDAMENTAL_FIELDS if field != "revenue")
        else:
            review_fields = ""
        out.append({"ticker": remap["Ticker"], "quarter_id": source.get("quarter_id", "") if source else "", "conflicting_fields": ",".join(conflicts), "additional_fields_requiring_restatement_review": review_fields, "status": "CONFLICT" if conflicts else "PASS"})
    return out


def lineage_ownership(current: list[dict[str, Any]], apply_set: list[dict[str, Any]], remaps: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_qid = {int(row["quarter_id"]): row for row in current}
    out = []
    for op in apply_set:
        row = by_qid[int(op["current_canonical_quarter_id"])]
        out.append({"ticker": op["ticker"], "quarter_id": op["current_canonical_quarter_id"], "operation": op["operation"], "lineage_refs": row["lineage_refs"], "lineage_action": op["lineage_action"], "source_winner": row["accepted_source_provider"], "status": "PRESERVE"})
    for remap in remaps:
        if remap["Ticker"] in {"IMMR", "RCAT"}:
            out.append({"ticker": remap["Ticker"], "quarter_id": "", "operation": "FUTURE_REVIEW", "lineage_refs": "", "lineage_action": "REVIEW_RESTATEMENT_OR_TRANSITION_SOURCE_OWNERSHIP", "source_winner": "", "status": "REVIEW"})
    return out


def immr_restatement_matrix(current: list[dict[str, Any]], official: dict[tuple[str, str, str], dict[str, str]], remaps: list[dict[str, str]]) -> list[dict[str, Any]]:
    current_by_id = by_identity(current)
    out = []
    for remap in remaps:
        if remap["Ticker"] != "IMMR":
            continue
        source = current_by_id[(remap["Ticker"], remap["Current Fiscal Year"], remap["Current Fiscal Q"])]
        off = official_for_remap(source, remap, official)
        out.append(
            {
                "current_quarter_id": source["quarter_id"],
                "current_fy": remap["Current Fiscal Year"],
                "current_fq": remap["Current Fiscal Q"],
                "actual_fy": remap["Proposed Fiscal Year"],
                "actual_fq": remap["Proposed Fiscal Q"],
                "current_revenue": source["revenue"],
                "official_revenue": off["Revenue"],
                "revenue_action": "UPDATE_CANONICAL_VALUE" if not close_enough(number(source["revenue"]), number(off["Revenue"])) else "NO_VALUE_CHANGE",
                "other_fields_status": "EXTERNAL_EVIDENCE_REQUIRED",
                "source_evidence": off["Source 1"],
            }
        )
    return out


def rcat_transition_mapping(current: list[dict[str, Any]], official: dict[tuple[str, str, str], dict[str, str]], remaps: list[dict[str, str]]) -> list[dict[str, Any]]:
    current_by_id = by_identity(current)
    out = []
    for remap in remaps:
        if remap["Ticker"] != "RCAT":
            continue
        source = current_by_id[(remap["Ticker"], remap["Current Fiscal Year"], remap["Current Fiscal Q"])]
        off = official_for_remap(source, remap, official)
        out.append(
            {
                "current_quarter_id": source["quarter_id"],
                "current_fy": remap["Current Fiscal Year"],
                "current_fq": remap["Current Fiscal Q"],
                "current_period_end": source["period_end"],
                "current_revenue": source["revenue"],
                "official_transition_label": f"FY{off['Fiscal Year']} {off['Fiscal Q']}",
                "official_period_end": off["Official Period End"],
                "official_revenue": off["Revenue"],
                "proposed_canonical_label": "POLICY_REQUIRED_FOR_2024T",
                "status": "TRANSITION_LABEL_POLICY_REQUIRED",
            }
        )
    return out


def group_summary(apply_set: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for row in apply_set:
        by_ticker.setdefault(row["ticker"], []).append(row)
    out = [
        {"ticker": ticker, "production_ready": "YES", "transformation_group_id": rows_[0]["transformation_group_id"], "operations": len(rows_), "canonical_rows_affected": len({r["current_canonical_quarter_id"] for r in rows_}), "status": "PRODUCTION_READY"}
        for ticker, rows_ in sorted(by_ticker.items())
    ]
    out.extend(
        [
            {"ticker": "IMMR", "production_ready": "NO", "transformation_group_id": "P8A10A-SPECIAL-IMMR", "operations": 0, "canonical_rows_affected": 5, "status": "EXTERNAL_EVIDENCE_REQUIRED_IDENTITY_VALUE_COUPLING"},
            {"ticker": "RCAT", "production_ready": "NO", "transformation_group_id": "P8A10A-SPECIAL-RCAT", "operations": 0, "canonical_rows_affected": 2, "status": "TRANSITION_LABEL_POLICY_REQUIRED"},
        ]
    )
    return out


def write_resolution_docs(root: Path, fngr_status: str) -> None:
    (root / "fngr_structural_resolution.md").write_text(
        f"# FNGR Structural Resolution\n\nDisposition: `{fngr_status}`.\n\nFY2024 Q2 is independently proven by SEC period metadata and matching revenue. Sparse surrounding history is non-blocking. Freeze bounded canonical structural action: update period_end to `2023-08-31`; also align publish_date to official filing date `2023-10-13`.\n",
        encoding="utf-8",
    )
    (root / "immr_structural_resolution.md").write_text(
        "# IMMR Structural Resolution\n\nDisposition: `EXTERNAL_EVIDENCE_REQUIRED`.\n\nThe current segment is an identity/value coupled restatement case. Current FY2025 Q1-Q4/FY2026 Q1 map toward FY2025 Q4/FY2026 Q1-Q4, but at least FY2025 Q4 Revenue must change from `281376000` to `284876000`. Other non-null fields must be compared against the restated official source before production apply.\n",
        encoding="utf-8",
    )
    (root / "rcat_structural_resolution.md").write_text(
        "# RCAT Structural Resolution\n\nDisposition: `EXTERNAL_EVIDENCE_REQUIRED`.\n\nRCAT changed fiscal-year structure through an eight-month transition period ending `2024-12-31`. The `1534727` revenue row belongs to the quarter ended `2024-10-31` and must not be moved to `2024-01-31`. V3 needs an explicit deterministic policy for encoding `2024T` transition labels before apply.\n",
        encoding="utf-8",
    )
    (root / "phase8a10b_special_case_rules.md").write_text(
        "# Phase 8A10B Special Case Rules\n\n- Sparse history is not by itself a shifted-sequence error when the individual economic quarter is independently proven.\n- Restatements can couple identity movement with value replacement; do not apply label-only repairs when non-null fields changed.\n- Transition-year / 10-KT periods must be modeled separately from ordinary Q1-Q4 continuity.\n",
        encoding="utf-8",
    )
    (root / "phase8a10b_updated_handoff.md").write_text(
        "# Phase 8A10B Updated Handoff\n\nAfter applying any frozen special repairs, run the full V3 fiscal-sequence, period_end, and publish-date audit across all retained companies and quarters. Carry sparse-history, restatement, and transition-year exception classes forward as first-class classifications.\n",
        encoding="utf-8",
    )


def run_phase8a10a_special(paths: Phase8A10ASpecialPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    before_raw = file_state(paths.rawcandle_db)
    timeline = read_csv(paths.a10ar_root / "external_case_resolution_reconciliation.csv")
    official = read_csv(paths.a10ar_root / "external_segment_remap_reconciliation.csv")
    official_timeline = read_csv(paths.a10ar_root / "external_segment_remap_reconciliation.csv")
    official_full = read_csv(paths.a10ar_root / "external_case_resolution_reconciliation.csv")
    stored_official = read_csv(paths.a10ar_root / "external_segment_remap_reconciliation.csv")
    official_rows_all = read_csv(paths.a10ar_root / "external_case_resolution_reconciliation.csv")
    remaps = read_csv(paths.a10ar_root / "external_segment_remap_reconciliation.csv")
    official_fiscal_timeline = read_csv(Path("temp/phase8_structural_R1_official_fiscal_timelines.csv"))
    post_r1 = read_csv(paths.apply_root / "post_apply_structural_R1.csv")
    validate_remaining_tickers(post_r1)
    del timeline, official, official_timeline, official_full, stored_official, official_rows_all
    special_remaps = [row for row in remaps if row["Ticker"] in SPECIAL_TICKERS]
    official_by = official_by_identity(official_fiscal_timeline)
    before_v3 = file_state(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        counts_before = table_counts(conn)
        quick_before = scalar(conn, "PRAGMA quick_check")
        current = current_timelines(conn)
        counts_after = table_counts(conn)
        quick_after = scalar(conn, "PRAGMA quick_check")
    before_after_same = before_v3 == file_state(paths.v3_db)
    apply_set, fngr_status = fngr_resolution(current, official_by)
    content = economic_content_match(current, official_by, special_remaps)
    collisions = target_collisions(current, special_remaps)
    conflicts = non_null_conflicts(current, official_by, special_remaps)
    lineage = lineage_ownership(current, apply_set, special_remaps)
    immr_matrix = immr_restatement_matrix(current, official_by, special_remaps)
    rcat_mapping = rcat_transition_mapping(current, official_by, special_remaps)
    group_rows = group_summary(apply_set)
    external_queue = [
        {"ticker": "IMMR", "exact_missing_evidence": "Restated comparison for all non-null canonical fields, not Revenue only.", "preferred_source": "SEC 10-Q/10-K/A XBRL and issuer restatement filing"},
        {"ticker": "RCAT", "exact_missing_evidence": "Approved V3 encoding policy for FY2024T transition labels and transition Q1 value replacement.", "preferred_source": "SEC 10-Q plus 10-KT transition report"},
    ]
    write_csv(paths.artifact_root / "special_current_v3_timelines.csv", current)
    write_csv(paths.artifact_root / "fngr_official_timeline.csv", official_rows(official_fiscal_timeline, "FNGR"))
    write_csv(paths.artifact_root / "immr_official_timeline.csv", official_rows(official_fiscal_timeline, "IMMR"))
    write_csv(paths.artifact_root / "immr_restatement_matrix.csv", immr_matrix)
    write_csv(paths.artifact_root / "rcat_official_transition_timeline.csv", official_rows(official_fiscal_timeline, "RCAT"))
    write_csv(paths.artifact_root / "rcat_transition_mapping.csv", rcat_mapping)
    write_csv(paths.artifact_root / "special_economic_content_match.csv", content)
    write_csv(paths.artifact_root / "special_target_collisions.csv", collisions)
    write_csv(paths.artifact_root / "special_non_null_conflicts.csv", conflicts)
    write_csv(paths.artifact_root / "special_lineage_ownership.csv", lineage)
    write_csv(paths.artifact_root / "phase8a10a_special_frozen_apply_set.csv", apply_set)
    write_csv(paths.artifact_root / "special_transformation_group_summary.csv", group_rows)
    write_csv(paths.artifact_root / "special_external_research_queue.csv", external_queue)
    (paths.artifact_root / "special_external_research_human_summary.md").write_text(
        "# Special External Research Queue\n\n- IMMR: compare every non-null canonical field against restated official filings before any production apply.\n- RCAT: decide deterministic V3 transition-year label encoding for FY2024T before any production apply.\n",
        encoding="utf-8",
    )
    write_resolution_docs(paths.artifact_root, fngr_status)
    summary = {
        "classification": CLASSIFICATION,
        "derived_state": DERIVED_STALE,
        "structural_r1_start": 3,
        "remaining_tickers": sorted(SPECIAL_TICKERS),
        "production_writes": int(not before_after_same),
        "rawcandle_writes": int(before_raw != file_state(paths.rawcandle_db)),
        "counts_before": counts_before,
        "counts_after": counts_after,
        "quick_check_before": quick_before,
        "quick_check_after": quick_after,
        "fngr": {"production_ready": fngr_status == "PRODUCTION_READY", "disposition": "UPDATE_PERIOD_END_ONLY", "operations": len(apply_set)},
        "immr": {"production_ready": False, "disposition": "EXTERNAL_EVIDENCE_REQUIRED", "affected_rows": len(immr_matrix), "revenue_repairs": sum(1 for row in immr_matrix if row["revenue_action"] == "UPDATE_CANONICAL_VALUE")},
        "rcat": {"production_ready": False, "disposition": "TRANSITION_LABEL_POLICY_REQUIRED", "affected_rows": len(rcat_mapping), "revenue_1534727_location": "2024-10-31"},
        "frozen_special_apply": {"production_ready_tickers": sorted({row["ticker"] for row in apply_set}), "transformation_groups": len({row["transformation_group_id"] for row in apply_set}), "canonical_rows_affected": len({row["current_canonical_quarter_id"] for row in apply_set}), "operations": len(apply_set), "field_value_repairs": sum(1 for row in apply_set if row["operation"] == "UPDATE_CANONICAL_VALUE"), "identity_repairs": 0, "period_end_repairs": sum(1 for row in apply_set if row["operation"] == "UPDATE_PERIOD_END"), "publish_date_repairs": sum(1 for row in apply_set if row["operation"] == "UPDATE_PUBLISH_DATE"), "merges_deletes_recreates": 0},
        "remaining_structural_r1_after_future_apply": 2,
        "unresolved_tickers": ["IMMR", "RCAT"],
        "external_research_queue_size": len(external_queue),
        "downstream_writes": {"ttm": 0, "score": 0, "lifecycle": 0, "valuation": 0},
        "artifact_root": str(paths.artifact_root),
        "next_action": "PHASE 8A10A-SPECIAL-APPLY - APPLY FNGR BOUNDED SPECIAL REPAIR, then resolve IMMR/RCAT evidence before PHASE 8A10B",
    }
    write_json(paths.artifact_root / "phase8a10a_special_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if summary["production_writes"] or summary["rawcandle_writes"] or counts_before != counts_after:
        raise RuntimeError("read-only safety check failed")
    return summary
