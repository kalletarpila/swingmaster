from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A10A_R_STRUCTURAL_APPLY_SET_READY"
CLASSIFICATION_PARTIAL = "FUNDAMENTALS_V3_PHASE8A10A_R_PARTIAL_APPLY_SET_READY_SPECIAL_CASES_REMAIN"
CLASSIFICATION_CONFLICTS = "FUNDAMENTALS_V3_PHASE8A10A_R_EXTERNAL_MAPPING_CONFLICTS_REMAIN"

SPECIAL_NO_TICKERS = {"FNGR", "IMMR", "RCAT"}
CANONICAL_FIELDS = (
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
class Phase8A10ARPaths:
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    official_timeline_csv: Path = Path("temp/phase8_structural_R1_official_fiscal_timelines.csv")
    case_resolution_csv: Path = Path("temp/phase8_structural_R1_case_resolution.csv")
    segment_remap_csv: Path = Path("temp/phase8_structural_R1_segment_remap.csv")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def file_state(path: Path) -> dict[str, Any]:
    return {
        "exists": path.exists(),
        "size": path.stat().st_size if path.exists() else None,
        "mtime_ns": path.stat().st_mtime_ns if path.exists() else None,
    }


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    tables = ["v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation"]
    return {table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}")) for table in tables}


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    return float(text)


def close_enough(left: float | None, right: float | None, *, tolerance: float = 0.01) -> bool:
    if left is None or right is None:
        return False
    return abs(left - right) <= max(1.0, abs(left), abs(right)) * tolerance


def validate_inputs(timeline: list[dict[str, str]], cases: list[dict[str, str]], remaps: list[dict[str, str]]) -> dict[str, Any]:
    ready_counts = Counter(row["Production Ready"] for row in cases)
    no_tickers = {row["Ticker"] for row in cases if row["Production Ready"] == "NO"}
    summary = {
        "official_timeline_rows": len(timeline),
        "case_resolution_rows": len(cases),
        "segment_remap_rows": len(remaps),
        "external_ready_yes": ready_counts["YES"],
        "external_ready_no": ready_counts["NO"],
        "external_no_tickers": sorted(no_tickers),
        "segment_unique_rows": len(
            {
                (
                    row["Ticker"],
                    row["Current Fiscal Year"],
                    row["Current Fiscal Q"],
                    row["Current Period End"],
                    row["Proposed Fiscal Year"],
                    row["Proposed Fiscal Q"],
                    row["Proposed Period End"],
                )
                for row in remaps
            }
        ),
    }
    expected = {
        "official_timeline_rows": 116,
        "case_resolution_rows": 15,
        "segment_remap_rows": 75,
        "external_ready_yes": 12,
        "external_ready_no": 3,
        "external_no_tickers": ["FNGR", "IMMR", "RCAT"],
        "segment_unique_rows": 75,
    }
    failures = {key: {"expected": value, "actual": summary[key]} for key, value in expected.items() if summary[key] != value}
    if failures:
        raise RuntimeError(f"external structural input validation failed: {failures}")
    return summary


def current_row(conn: sqlite3.Connection, remap: dict[str, str]) -> dict[str, Any] | None:
    found = rows(
        conn,
        """
        SELECT c.company_id,c.ticker,c.company_name,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
               q.period_end_date AS period_end,q.publish_date,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.accepted_source_provider,
               (SELECT COUNT(*) FROM v3_ttm t WHERE t.endpoint_quarter_id=q.quarter_id) AS ttm_endpoint_refs,
               (SELECT COUNT(*) FROM v3_score s WHERE s.as_of_quarter_id=q.quarter_id) AS score_refs,
               (SELECT COUNT(*) FROM v3_lifecycle l WHERE l.endpoint_quarter_id=q.quarter_id) AS lifecycle_refs,
               (SELECT COUNT(*) FROM v3_valuation v WHERE v.endpoint_quarter_id=q.quarter_id) AS valuation_refs,
               (SELECT COUNT(*) FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id) AS lineage_refs
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=? AND q.period_end_date=?
        """,
        (
            remap["Ticker"],
            int(remap["Current Fiscal Year"]),
            remap["Current Fiscal Q"],
            remap["Current Period End"],
        ),
    )
    return found[0] if len(found) == 1 else None


def target_identity_row(conn: sqlite3.Connection, ticker: str, fiscal_year: str, fiscal_quarter: str) -> dict[str, Any] | None:
    found = rows(
        conn,
        """
        SELECT c.company_id,c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date AS period_end,q.publish_date,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
        """,
        (ticker, int(fiscal_year), fiscal_quarter),
    )
    return found[0] if len(found) == 1 else None


def field_conflicts(source: dict[str, Any], target: dict[str, Any] | None) -> tuple[str, str]:
    if target is None:
        return "TARGET_EMPTY", ""
    classes = []
    conflicts = []
    for field in (*CANONICAL_FIELDS, "publish_date", "period_end"):
        left = source.get(field)
        right = target.get(field)
        if left is None and right is None:
            status = "SAME"
        elif left is None and right is not None:
            status = "NULL_VS_VALUE"
        elif left is not None and right is None:
            status = "VALUE_VS_NULL"
        elif field in {"publish_date", "period_end"}:
            status = "SAME" if str(left) == str(right) else "CONFLICT"
        else:
            status = "SAME" if close_enough(float(left), float(right)) else "CONFLICT"
        classes.append(status)
        if status == "CONFLICT":
            conflicts.append(field)
    if conflicts:
        return "CONFLICT", ",".join(conflicts)
    if "NULL_VS_VALUE" in classes or "VALUE_VS_NULL" in classes:
        return "PARTIAL_COMPLEMENT", ""
    return "SAME", ""


def official_by_key(timeline: list[dict[str, str]]) -> dict[tuple[str, str, str], dict[str, str]]:
    return {(row["Ticker"], row["Fiscal Year"], row["Fiscal Q"]): row for row in timeline}


def case_by_ticker(cases: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in cases:
        out[row["Ticker"]].append(row)
    return out


def reconcile_remaps(
    conn: sqlite3.Connection,
    remaps: list[dict[str, str]],
    official: dict[tuple[str, str, str], dict[str, str]],
    cases_by_ticker: dict[str, list[dict[str, str]]],
) -> dict[str, list[dict[str, Any]]]:
    current_rows = []
    content = []
    target_collisions = []
    non_null = []
    ownership = []
    by_ticker = defaultdict(list)
    for remap in remaps:
        by_ticker[remap["Ticker"]].append(remap)
    source_keys_by_ticker = {
        ticker: {(r["Current Fiscal Year"], r["Current Fiscal Q"], r["Current Period End"]) for r in group}
        for ticker, group in by_ticker.items()
    }
    source_identities_by_ticker = {
        ticker: {(r["Current Fiscal Year"], r["Current Fiscal Q"]) for r in group}
        for ticker, group in by_ticker.items()
    }
    for remap in remaps:
        ticker = remap["Ticker"]
        group_id = f"P8A10A-R-{ticker}"
        row = current_row(conn, remap)
        status = "EXACT_CURRENT_ROW_MATCH" if row else "CURRENT_ROW_NOT_FOUND"
        target = target_identity_row(conn, ticker, remap["Proposed Fiscal Year"], remap["Proposed Fiscal Q"])
        target_key = (remap["Proposed Fiscal Year"], remap["Proposed Fiscal Q"], remap["Proposed Period End"])
        target_identity = (remap["Proposed Fiscal Year"], remap["Proposed Fiscal Q"])
        target_in_same_group = target_key in source_keys_by_ticker[ticker] or target_identity in source_identities_by_ticker[ticker]
        official_row = official.get((ticker, remap["Proposed Fiscal Year"], remap["Proposed Fiscal Q"]), {})
        current_revenue = parse_number(row.get("revenue") if row else None)
        official_revenue = parse_number(official_row.get("Revenue"))
        revenue_match = close_enough(current_revenue, official_revenue)
        period_match = row is not None and row["period_end"] == remap["Current Period End"]
        proposed_period_match = official_row.get("Official Period End", "") in {"", remap["Proposed Period End"]}
        if row is None:
            match_conf = "ECONOMIC_QUARTER_MISMATCH"
        elif revenue_match and proposed_period_match:
            match_conf = "ECONOMIC_QUARTER_MATCH_HIGH"
        elif proposed_period_match or revenue_match:
            match_conf = "ECONOMIC_QUARTER_MATCH_MEDIUM"
        else:
            match_conf = "ECONOMIC_QUARTER_MATCH_LOW"
        collision_class = "TARGET_EMPTY"
        shape = "RELABEL_ONLY"
        if target:
            if int(target["quarter_id"]) == int(row["quarter_id"]) if row else False:
                collision_class = "TARGET_EXISTS_SAME_ECONOMIC_QUARTER"
                shape = "RELABEL_ONLY"
            elif target_in_same_group:
                collision_class = "TARGET_EXISTS_DIFFERENT_ECONOMIC_QUARTER"
                shape = "MULTI_ROW_ROTATION"
            else:
                cls, _ = field_conflicts(row or {}, target)
                collision_class = {
                    "SAME": "TARGET_EXISTS_SAME_ECONOMIC_QUARTER",
                    "PARTIAL_COMPLEMENT": "TARGET_EXISTS_PARTIAL_COMPLEMENT",
                    "CONFLICT": "TARGET_EXISTS_CONFLICTING_CONTENT",
                    "TARGET_EMPTY": "TARGET_EMPTY",
                }[cls]
                shape = "MERGE_COMPLEMENTARY" if cls == "PARTIAL_COMPLEMENT" else "NO_SAFE_TRANSFORMATION"
        cls, conflicts = field_conflicts(row or {}, target)
        current_rows.append(
            {
                **{f"remap_{k.lower().replace(' ', '_')}": v for k, v in remap.items()},
                **(row or {}),
                "match_status": status,
                "transformation_group_id": group_id,
            }
        )
        content.append(
            {
                "transformation_group_id": group_id,
                "ticker": ticker,
                "current_fy": remap["Current Fiscal Year"],
                "current_fq": remap["Current Fiscal Q"],
                "current_period_end": remap["Current Period End"],
                "proposed_fy": remap["Proposed Fiscal Year"],
                "proposed_fq": remap["Proposed Fiscal Q"],
                "proposed_period_end": remap["Proposed Period End"],
                "quarter_id": row.get("quarter_id", "") if row else "",
                "current_revenue": current_revenue if current_revenue is not None else "",
                "official_revenue": official_revenue if official_revenue is not None else "",
                "revenue_match": int(revenue_match),
                "period_match": int(period_match),
                "content_match_confidence": match_conf,
                "source_evidence": remap.get("Evidence", "") or official_row.get("Source 1", ""),
            }
        )
        target_collisions.append(
            {
                "transformation_group_id": group_id,
                "ticker": ticker,
                "source_quarter_id": row.get("quarter_id", "") if row else "",
                "target_quarter_id": target.get("quarter_id", "") if target else "",
                "target_fy": remap["Proposed Fiscal Year"],
                "target_fq": remap["Proposed Fiscal Q"],
                "target_period_end": target.get("period_end", "") if target else "",
                "target_identity_collision": collision_class,
                "target_in_same_rotation_group": int(target_in_same_group),
                "transformation_shape": shape,
            }
        )
        non_null.append(
            {
                "transformation_group_id": group_id,
                "ticker": ticker,
                "source_quarter_id": row.get("quarter_id", "") if row else "",
                "target_quarter_id": target.get("quarter_id", "") if target else "",
                "field_conflict_class": cls,
                "conflicting_fields": conflicts,
                "merge_safe": int(cls in {"SAME", "PARTIAL_COMPLEMENT", "TARGET_EMPTY"} and not conflicts),
            }
        )
        ownership.append(
            {
                "transformation_group_id": group_id,
                "ticker": ticker,
                "quarter_id": row.get("quarter_id", "") if row else "",
                "publish_date_moves_with_economic_quarter": 1,
                "period_end_moves_with_economic_quarter": 1,
                "lineage_moves_with_quarter_id": 1,
                "lineage_action": "KEEP_QUARTER_ID_AND_RELABEL_CONTENT" if shape in {"RELABEL_ONLY", "MULTI_ROW_ROTATION"} else "REVIEW_BEFORE_MERGE",
                "dependent_reference_action": "MARK_DERIVED_STALE_FOR_REBUILD",
                "current_publish_date": row.get("publish_date", "") if row else "",
                "proposed_publish_date": official_row.get("Publish Date", ""),
            }
        )
    return {
        "current_rows": current_rows,
        "content": content,
        "target_collisions": target_collisions,
        "non_null_conflicts": non_null,
        "ownership": ownership,
    }


def group_readiness(cases: list[dict[str, str]], remaps: list[dict[str, str]], analysis: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    content_by_group = defaultdict(list)
    collisions_by_group = defaultdict(list)
    conflicts_by_group = defaultdict(list)
    ownership_by_group = defaultdict(list)
    for row in analysis["content"]:
        content_by_group[row["transformation_group_id"]].append(row)
    for row in analysis["target_collisions"]:
        collisions_by_group[row["transformation_group_id"]].append(row)
    for row in analysis["non_null_conflicts"]:
        conflicts_by_group[row["transformation_group_id"]].append(row)
    for row in analysis["ownership"]:
        ownership_by_group[row["transformation_group_id"]].append(row)
    case_rows = case_by_ticker(cases)
    out = []
    for ticker in sorted({row["Ticker"] for row in remaps}):
        group_id = f"P8A10A-R-{ticker}"
        external_ready = "YES" if any(row["Production Ready"] == "YES" for row in case_rows[ticker]) else "NO"
        content = content_by_group[group_id]
        collisions = collisions_by_group[group_id]
        conflicts = conflicts_by_group[group_id]
        high_or_medium = all(row["content_match_confidence"] in {"ECONOMIC_QUARTER_MATCH_HIGH", "ECONOMIC_QUARTER_MATCH_MEDIUM"} for row in content)
        all_found = all(row["quarter_id"] != "" for row in content)
        unsafe_collisions = [
            row
            for row in collisions
            if row["target_identity_collision"] in {"TARGET_EXISTS_CONFLICTING_CONTENT", "TARGET_EXISTS_PARTIAL_COMPLEMENT"}
            or (row["target_identity_collision"] == "TARGET_EXISTS_DIFFERENT_ECONOMIC_QUARTER" and int(row["target_in_same_rotation_group"]) != 1)
        ]
        conflict_fields = [row for row in conflicts if row["conflicting_fields"] and not any(c["target_in_same_rotation_group"] for c in collisions if c["target_quarter_id"] == row["target_quarter_id"])]
        if external_ready == "YES" and high_or_medium and all_found and not unsafe_collisions and not conflict_fields:
            local = "V3_PRODUCTION_READY"
        elif external_ready == "YES":
            local = "V3_REPAIRABLE_WITH_BOUNDED_CONFLICT_RESOLUTION"
        else:
            local = "V3_NOT_READY"
        shapes = sorted({row["transformation_shape"] for row in collisions})
        out.append(
            {
                "transformation_group_id": group_id,
                "ticker": ticker,
                "external_production_ready": external_ready,
                "v3_local_classification": local,
                "transformation_shape": "MULTI_ROW_ROTATION" if "MULTI_ROW_ROTATION" in shapes else ",".join(shapes),
                "canonical_rows_affected": len(content),
                "conflict_count": len(conflict_fields),
                "lineage_handling": "keep quarter_id lineage through atomic relabel/period/publish rotation; mark derived stale",
                "final_production_ready": "YES" if local == "V3_PRODUCTION_READY" else "NO",
                "reason": "external NO special case" if external_ready == "NO" else "locally proven bounded rotation" if local == "V3_PRODUCTION_READY" else "current V3 conflicts require bounded conflict resolution",
            }
        )
    return out


def atomic_transformations(remaps: list[dict[str, str]], analysis: dict[str, list[dict[str, Any]]], group_summary: list[dict[str, Any]], official: dict[tuple[str, str, str], dict[str, str]]) -> list[dict[str, Any]]:
    ready = {row["ticker"] for row in group_summary if row["final_production_ready"] == "YES"}
    current_by_key = {
        (row["remap_ticker"], row["remap_current_fiscal_year"], row["remap_current_fiscal_q"], row["remap_current_period_end"]): row
        for row in analysis["current_rows"]
    }
    out = []
    for remap in remaps:
        if remap["Ticker"] not in ready:
            continue
        group_id = f"P8A10A-R-{remap['Ticker']}"
        current = current_by_key[(remap["Ticker"], remap["Current Fiscal Year"], remap["Current Fiscal Q"], remap["Current Period End"])]
        official_row = official.get((remap["Ticker"], remap["Proposed Fiscal Year"], remap["Proposed Fiscal Q"]), {})
        out.append(
            {
                "transformation_group_id": group_id,
                "ticker": remap["Ticker"],
                "operation_order": 1,
                "operation": "CREATE_TEMP_IDENTITY",
                "source_canonical_quarter_id": current["quarter_id"],
                "target_canonical_quarter_id": "",
                "old_fy": remap["Current Fiscal Year"],
                "old_fq": remap["Current Fiscal Q"],
                "new_fy": f"900000 + row_order sentinel for {group_id}",
                "new_fq": remap["Current Fiscal Q"],
                "old_period_end": remap["Current Period End"],
                "new_period_end": remap["Current Period End"],
                "old_publish_date": current.get("publish_date", ""),
                "new_publish_date": current.get("publish_date", ""),
                "fields_moved": "quarter row identity only",
                "lineage_action": "preserve quarter_id",
                "dependency_action": "derived tables remain stale until rebuild",
                "write_guard": "quarter_id + old FY/FQ + old period_end",
                "rollback_instruction": "restore old FY/FQ/period_end/publish_date from frozen set",
            }
        )
        out.append(
            {
                "transformation_group_id": group_id,
                "ticker": remap["Ticker"],
                "operation_order": 2,
                "operation": "FINALIZE_IDENTITY",
                "source_canonical_quarter_id": current["quarter_id"],
                "target_canonical_quarter_id": "",
                "old_fy": remap["Current Fiscal Year"],
                "old_fq": remap["Current Fiscal Q"],
                "new_fy": remap["Proposed Fiscal Year"],
                "new_fq": remap["Proposed Fiscal Q"],
                "old_period_end": remap["Current Period End"],
                "new_period_end": remap["Proposed Period End"],
                "old_publish_date": current.get("publish_date", ""),
                "new_publish_date": official_row.get("Publish Date", ""),
                "fields_moved": ",".join(CANONICAL_FIELDS),
                "lineage_action": "preserve v3_migration_audit quarter_id references with economic content",
                "dependency_action": "retain existing derived rows until explicit downstream rebuild",
                "write_guard": "sentinel identity + quarter_id + source evidence hash",
                "rollback_instruction": "restore old FY/FQ/period_end/publish_date from frozen set",
            }
        )
    return out


def frozen_apply_set(remaps: list[dict[str, str]], analysis: dict[str, list[dict[str, Any]]], group_summary: list[dict[str, Any]], official: dict[tuple[str, str, str], dict[str, str]]) -> list[dict[str, Any]]:
    ready = {row["ticker"] for row in group_summary if row["final_production_ready"] == "YES"}
    content_by_key = {
        (row["ticker"], row["current_fy"], row["current_fq"], row["current_period_end"]): row
        for row in analysis["content"]
    }
    current_by_key = {
        (row["remap_ticker"], row["remap_current_fiscal_year"], row["remap_current_fiscal_q"], row["remap_current_period_end"]): row
        for row in analysis["current_rows"]
    }
    out = []
    for remap in remaps:
        if remap["Ticker"] not in ready:
            continue
        key = (remap["Ticker"], remap["Current Fiscal Year"], remap["Current Fiscal Q"], remap["Current Period End"])
        current = current_by_key[key]
        content = content_by_key[key]
        official_row = official.get((remap["Ticker"], remap["Proposed Fiscal Year"], remap["Proposed Fiscal Q"]), {})
        out.append(
            {
                "transformation_group_id": f"P8A10A-R-{remap['Ticker']}",
                "ticker": remap["Ticker"],
                "current_canonical_quarter_id": current["quarter_id"],
                "current_fy": remap["Current Fiscal Year"],
                "current_fq": remap["Current Fiscal Q"],
                "current_period_end": remap["Current Period End"],
                "current_publish_date": current.get("publish_date", ""),
                "proposed_fy": remap["Proposed Fiscal Year"],
                "proposed_fq": remap["Proposed Fiscal Q"],
                "proposed_period_end": remap["Proposed Period End"],
                "proposed_publish_date": official_row.get("Publish Date", ""),
                "transformation_shape": remap["Action"],
                "merge_delete_target": "",
                "fields_moved": ",".join(CANONICAL_FIELDS),
                "lineage_action": "preserve quarter_id lineage with moved economic content",
                "conflict_status": "NO_UNRESOLVED_DATA_LOSS_CONFLICT",
                "old_value_guards": f"FY={remap['Current Fiscal Year']} FQ={remap['Current Fiscal Q']} period_end={remap['Current Period End']}",
                "content_match_confidence": content["content_match_confidence"],
                "source_evidence": remap["Evidence"],
                "rollback_group": f"ROLLBACK-{remap['Ticker']}",
            }
        )
    return out


def downstream_impact(group_summary: list[dict[str, Any]], analysis: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    current_by_group = defaultdict(list)
    for row in analysis["current_rows"]:
        current_by_group[row["transformation_group_id"]].append(row)
    out = []
    for group in group_summary:
        rows_ = current_by_group[group["transformation_group_id"]]
        out.append(
            {
                "transformation_group_id": group["transformation_group_id"],
                "ticker": group["ticker"],
                "canonical_rows": len(rows_),
                "ttm_endpoint_rows": sum(int(row.get("ttm_endpoint_refs") or 0) for row in rows_),
                "score_rows": sum(int(row.get("score_refs") or 0) for row in rows_),
                "lifecycle_rows": sum(int(row.get("lifecycle_refs") or 0) for row in rows_),
                "valuation_rows": sum(int(row.get("valuation_refs") or 0) for row in rows_),
                "rebuild_status": "DEFERRED",
            }
        )
    return out


def write_special_reviews(root: Path, cases: list[dict[str, str]], timeline: list[dict[str, str]]) -> list[dict[str, Any]]:
    special = []
    notes = {
        "FNGR": "Single FY2024 Q2 period_end repair appears bounded, but external file keeps Production Ready=NO because broader canonical history is sparse and has unresolved gaps. Do not over-remap.",
        "IMMR": "Current FY2025 Q4 appears to represent FY2026 Q3 and FY2025 Q4 also needs restated Revenue replacement. Label-only repair is blocked.",
        "RCAT": "Transition-year / 10-KT case. Do not move current row to 2024-01-31; current 1534727 revenue belongs to transition Q2 ending 2024-10-31. Synthetic transition labeling policy is still needed.",
    }
    filenames = {"FNGR": "fngr_special_review.md", "IMMR": "immr_special_review.md", "RCAT": "rcat_special_review.md"}
    for ticker in sorted(SPECIAL_NO_TICKERS):
        case_rows = [row for row in cases if row["Ticker"] == ticker]
        timeline_rows = [row for row in timeline if row["Ticker"] == ticker]
        text = f"# {ticker} Special Review\n\n{notes[ticker]}\n\nCases: {json.dumps(case_rows, indent=2)}\n\nOfficial timeline rows: {json.dumps(timeline_rows, indent=2)}\n"
        (root / filenames[ticker]).write_text(text, encoding="utf-8")
        special.append(
            {
                "ticker": ticker,
                "external_production_ready": "NO",
                "local_classification": "V3_NOT_READY",
                "bounded_single_row_repair_possible": "YES_WITH_SEPARATE_APPROVAL" if ticker == "FNGR" else "NO",
                "additional_evidence_needed": "segment policy / restatement value / transition labeling" if ticker != "FNGR" else "whether sparse history can be downgraded after single repair",
                "conclusion": notes[ticker],
            }
        )
    return special


def write_handoff(root: Path) -> None:
    (root / "phase8a10b_updated_audit_rules.md").write_text(
        """# Phase 8A10B Updated Audit Rules

Add detection patterns from external structural reconciliation:

- fiscal year shifted by exactly +1/-1 across a contiguous segment
- multi-quarter label rotations where every target identity is occupied by another row in the same segment
- current period_end already belonging to another canonical FY/FQ
- populated collision rows with different non-null fundamentals
- 52/53-week exact Saturday/Sunday period_end replacement vs month-end normalization
- sparse history that should not be over-remapped
- transition-year / 10-KT reporting that may require explicit transition labels
- restatement cases where identity and fundamentals both need repair
""",
        encoding="utf-8",
    )
    (root / "phase8a10b_updated_handoff.md").write_text(
        """# Phase 8A10B Updated Handoff

Do not run full-V3 audit until the frozen structural apply set and special cases are dispositioned.

The full audit must classify hard structural errors separately from review anomalies and must detect rotations as atomic groups, not individual period_end cells.
""",
        encoding="utf-8",
    )
    (root / "next_action.md").write_text(
        "PHASE 8A10A-APPLY - APPLY FROZEN STRUCTURAL QUARTER-SEQUENCE REPAIRS\n\nSPECIAL CASE RESEARCH - FNGR / IMMR / RCAT AS APPLICABLE\n",
        encoding="utf-8",
    )


def run_phase8a10a_r(paths: Phase8A10ARPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    before_v3 = file_state(paths.v3_db)
    before_raw = file_state(paths.rawcandle_db)
    official = read_csv(paths.official_timeline_csv)
    cases = read_csv(paths.case_resolution_csv)
    remaps = read_csv(paths.segment_remap_csv)
    input_summary = validate_inputs(official, cases, remaps)
    write_json(
        paths.artifact_root / "external_files_manifest.json",
        {
            "official_timeline_path": str(paths.official_timeline_csv),
            "case_resolution_path": str(paths.case_resolution_csv),
            "segment_remap_path": str(paths.segment_remap_csv),
            **input_summary,
        },
    )
    write_csv(paths.artifact_root / "external_case_resolution_reconciliation.csv", cases)
    write_csv(paths.artifact_root / "external_segment_remap_reconciliation.csv", remaps)
    with connect_ro(paths.v3_db) as conn:
        before_counts = table_counts(conn)
        analysis = reconcile_remaps(conn, remaps, official_by_key(official), case_by_ticker(cases))
        group_summary = group_readiness(cases, remaps, analysis)
        atomic = atomic_transformations(remaps, analysis, group_summary, official_by_key(official))
        frozen = frozen_apply_set(remaps, analysis, group_summary, official_by_key(official))
        impact = downstream_impact(group_summary, analysis)
        after_counts = table_counts(conn)
    special = write_special_reviews(paths.artifact_root, cases, official)
    write_csv(paths.artifact_root / "current_v3_rows_for_external_remap.csv", analysis["current_rows"])
    write_csv(paths.artifact_root / "economic_quarter_content_match.csv", analysis["content"])
    write_csv(paths.artifact_root / "target_identity_collisions.csv", analysis["target_collisions"])
    write_csv(paths.artifact_root / "canonical_non_null_conflicts.csv", analysis["non_null_conflicts"])
    write_csv(paths.artifact_root / "publish_lineage_ownership.csv", analysis["ownership"])
    write_csv(paths.artifact_root / "atomic_structural_transformations.csv", atomic)
    write_csv(paths.artifact_root / "transformation_group_summary.csv", group_summary)
    write_csv(paths.artifact_root / "phase8a10a_r_v3_frozen_structural_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "phase8a10a_r_special_cases_remaining.csv", special)
    write_csv(paths.artifact_root / "structural_repair_downstream_impact.csv", impact)
    write_handoff(paths.artifact_root)
    after_v3 = file_state(paths.v3_db)
    after_raw = file_state(paths.rawcandle_db)
    production_writes = int(before_v3 != after_v3)
    rawcandle_writes = int(before_raw != after_raw)
    local_ready = [row for row in group_summary if row["final_production_ready"] == "YES"]
    summary = {
        "classification": CLASSIFICATION_PARTIAL if local_ready and len(special) else CLASSIFICATION_READY if local_ready else CLASSIFICATION_CONFLICTS,
        "external": input_summary,
        "current_v3_reconciliation": dict(Counter(row["match_status"] for row in analysis["current_rows"])),
        "economic_quarter_matching": dict(Counter(row["content_match_confidence"] for row in analysis["content"])),
        "target_collisions": dict(Counter(row["target_identity_collision"] for row in analysis["target_collisions"])),
        "field_conflicts": {
            "merge_safe_groups": sum(1 for row in group_summary if row["final_production_ready"] == "YES"),
            "conflicting_populated_groups": sum(1 for row in group_summary if row["conflict_count"]),
            "unresolved_field_conflicts": sum(int(row["conflict_count"]) for row in group_summary),
        },
        "group_summary": group_summary,
        "frozen_apply_rows": len(frozen),
        "frozen_apply_groups": len(local_ready),
        "atomic_operations": len(atomic),
        "special_cases_remaining": [row["ticker"] for row in special],
        "downstream_impact": {
            "ttm_endpoint_rows": sum(row["ttm_endpoint_rows"] for row in impact if row["ticker"] in {g["ticker"] for g in local_ready}),
            "score_rows": sum(row["score_rows"] for row in impact if row["ticker"] in {g["ticker"] for g in local_ready}),
            "lifecycle_rows": sum(row["lifecycle_rows"] for row in impact if row["ticker"] in {g["ticker"] for g in local_ready}),
            "valuation_rows": sum(row["valuation_rows"] for row in impact if row["ticker"] in {g["ticker"] for g in local_ready}),
        },
        "counts_before": before_counts,
        "counts_after": after_counts,
        "production_writes": production_writes,
        "rawcandle_writes": rawcandle_writes,
        "derived_writes": {
            "ttm": int(before_counts["v3_ttm"] != after_counts["v3_ttm"]),
            "score": int(before_counts["v3_score"] != after_counts["v3_score"]),
            "lifecycle": int(before_counts["v3_lifecycle"] != after_counts["v3_lifecycle"]),
            "valuation": int(before_counts["v3_valuation"] != after_counts["v3_valuation"]),
        },
        "artifact_root": str(paths.artifact_root),
        "next_action": "PHASE 8A10A-APPLY - APPLY FROZEN STRUCTURAL QUARTER-SEQUENCE REPAIRS; SPECIAL CASE RESEARCH - FNGR / IMMR / RCAT AS APPLICABLE",
    }
    write_json(paths.artifact_root / "phase8a10a_r_summary.json", summary)
    if production_writes or rawcandle_writes or before_counts != after_counts:
        raise RuntimeError("read-only safety check failed")
    return summary
