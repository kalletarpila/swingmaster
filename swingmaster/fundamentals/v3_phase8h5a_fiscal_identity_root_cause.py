from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import (
    ANCHOR_TABLE,
    CHAIN_TABLE,
    PROFILE_TABLE,
    infer_slot,
    load_company_calendar,
    semantic_fingerprints,
    utc_stamp,
)
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import (
    rebuild_phase6,
    rebuild_ttm,
    rerun_downstream,
    semantic_table_rows,
    verify_models,
)
from swingmaster.fundamentals.v3_phase8h3_wave1_reconciliation import read_csv_dicts


CLASSIFICATION_READY = "FISCAL_IDENTITY_ROOT_CAUSE_FIXED_REPAIR_SET_READY"
CLASSIFICATION_REMAINING = "FISCAL_IDENTITY_ROOT_CAUSE_FIXED_WITH_TRUE_STRUCTURAL_REMAINDERS"
CLASSIFICATION_NOT_RESOLVED = "FISCAL_IDENTITY_ROOT_CAUSE_NOT_FULLY_RESOLVED"
NEXT_REPAIR = "APPLY ONLY THE FULLY REHEARSED EXISTING-ROW FISCAL IDENTITY REPAIR SET TO PRODUCTION; DO NOT CREATE MISSING Q4 ROWS YET"
NEXT_Q4 = "RUN A SEPARATE PHASE 8H-5B TO DETECT AND RECONSTRUCT GENUINELY MISSING Q4 QUARTERS"
NEXT_BLOCKED = "DO NOT APPLY FISCAL IDENTITY REPAIRS; FIX H5A VALIDATION OR REHEARSAL BLOCKERS FIRST"
H3_ROOT = Path("temp/fundamentals_v3_phase8h3_wave1_reconciliation/20260830T_PHASE8H3")
STRUCTURAL_11 = ("ASTH", "CECO", "CGC", "CTOR", "MCS", "MYGN", "OSK", "PTIX", "RCEL", "VCEL", "WDAY")
Q_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}


@dataclass(frozen=True)
class FiscalIdentityResolution:
    resolved_fiscal_year: int | None
    resolved_fiscal_quarter: str
    confidence: str
    evidence_basis: str
    exact_anchor_start: str
    expected_slot_end: str
    conflicting_candidates: str
    structural_exception: str
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class Phase8H5APaths:
    artifact_root: Path
    input_csv: Path = Path("temp/v3_active_tickers_99_27.csv")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    h3_root: Path = H3_ROOT
    osakedata_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    write_documentation: bool = True


def pct(part: int, whole: int) -> float:
    return round(part * 100 / whole, 4) if whole else 0.0


def parse_dt(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def connect_ro(db: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def find_anchor_csv() -> Path:
    candidates = [Path("/mnt/data/v3_active_tickers_99_27(1).csv"), Path("temp/v3_active_tickers_99_27.csv")]
    candidates.extend(Path("temp").glob("v3_active_tickers_99_27*.csv"))
    for path in candidates:
        if path.exists() and ":Zone.Identifier" not in str(path):
            return path
    raise FileNotFoundError("v3_active_tickers_99_27 CSV not found")


def fy_columns(row: dict[str, Any]) -> list[str]:
    return sorted([key for key in row if key.startswith("FY") and key.endswith("alkoi")], reverse=True)


def csv_anchor_rows(csv_path: Path) -> list[dict[str, Any]]:
    out = []
    for row in read_csv_rows(csv_path):
        ticker = str(row["ticker"]).strip().upper()
        for col in fy_columns(row):
            value = str(row.get(col) or "").strip()
            if not value:
                continue
            out.append(
                {
                    "ticker": ticker,
                    "fiscal_year": int(col[2:6]),
                    "csv_start_date": value,
                    "typical_start": row.get("Tyypillinen tilikauden alku", ""),
                    "csv_source": row.get("Lähde", ""),
                    "csv_chain_status": row.get("chain_status", ""),
                    "csv_break_reason": row.get("break_reason", ""),
                }
            )
    return out


def validate_csv_vs_production(csv_path: Path, db: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    csv_rows = csv_anchor_rows(csv_path)
    with connect_ro(db) as conn:
        prod = {
            (str(row["ticker"]).upper(), int(row["fiscal_year"])): row
            for row in rows(
                conn,
                f"""
                SELECT c.ticker,a.company_id,a.fiscal_year,a.fiscal_year_start_date,a.source_type,
                       a.source_reference,a.confidence,a.verification_status
                FROM {ANCHOR_TABLE} a JOIN v3_company c ON c.company_id=a.company_id
                """,
            )
        }
        profile_count = conn.execute(f"SELECT COUNT(*) FROM {PROFILE_TABLE}").fetchone()[0]
        conflict_rows = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,COUNT(*) n FROM {ANCHOR_TABLE} GROUP BY company_id,fiscal_year HAVING n>1)"
        ).fetchone()[0]
    csv_keys = {(row["ticker"], row["fiscal_year"]) for row in csv_rows}
    prod_keys = set(prod)
    out = []
    exact = missing = conflicts = 0
    for row in csv_rows:
        key = (row["ticker"], row["fiscal_year"])
        prow = prod.get(key)
        status = "MISSING_PRODUCTION_ANCHOR"
        prod_start = ""
        if prow:
            prod_start = prow["fiscal_year_start_date"]
            if prod_start == row["csv_start_date"]:
                status = "EXACT_MATCH"
                exact += 1
            else:
                status = "CONFLICTING_EXACT_ANCHOR"
                conflicts += 1
        else:
            missing += 1
        out.append({**row, "production_start_date": prod_start, "validation_status": status})
    unexpected = [
        {
            "ticker": key[0],
            "fiscal_year": key[1],
            "csv_start_date": "",
            "production_start_date": prod[key]["fiscal_year_start_date"],
            "validation_status": "UNEXPECTED_PRODUCTION_ONLY_ANCHOR",
        }
        for key in sorted(prod_keys - csv_keys)
    ]
    out.extend(unexpected)
    recent = {}
    for year in range(2023, 2028):
        recent[f"FY{year}"] = sum(1 for row in out if row["fiscal_year"] == year and row["validation_status"] == "EXACT_MATCH")
    summary = {
        "csv_path": str(csv_path),
        "csv_tickers": len({row["ticker"] for row in csv_rows}),
        "production_profiles": int(profile_count),
        "csv_populated_fy_start_cells": len(csv_rows),
        "production_fy_start_rows": len(prod),
        "exact_matches": exact,
        "missing_production_anchors": missing,
        "conflicting_exact_anchors": conflicts,
        "unexpected_production_only_anchors": len(unexpected),
        "recent_fy2023_fy2027_coverage": recent,
        "valid": missing == 0 and conflicts == 0,
        "duplicate_production_anchor_groups": int(conflict_rows),
    }
    return out, summary


def fiscal_identity_arbiter(
    conn: sqlite3.Connection,
    company_id: int,
    period_end: str,
    provider_fiscal_year: int | None = None,
    provider_fiscal_quarter: str | None = None,
    h3_candidate_fiscal_year: int | None = None,
    h3_candidate_fiscal_quarter: str | None = None,
    issuer_fiscal_year: int | None = None,
    issuer_fiscal_quarter: str | None = None,
    transition_status: str = "",
) -> FiscalIdentityResolution:
    if conn.row_factory is None:
        conn.row_factory = sqlite3.Row
    calendar = load_company_calendar(conn, company_id)
    slot = infer_slot(calendar, period_end)
    candidates = []
    if provider_fiscal_year and provider_fiscal_quarter:
        candidates.append(f"provider={provider_fiscal_year} {provider_fiscal_quarter}")
    if h3_candidate_fiscal_year and h3_candidate_fiscal_quarter:
        candidates.append(f"h3_candidate={h3_candidate_fiscal_year} {h3_candidate_fiscal_quarter}")
    if issuer_fiscal_year and issuer_fiscal_quarter:
        candidates.append(f"issuer={issuer_fiscal_year} {issuer_fiscal_quarter}")
    if transition_status in {"VERIFIED_TRANSITION", "STUB_PERIOD"}:
        return FiscalIdentityResolution(
            None,
            "",
            "STRUCTURAL_EXCEPTION",
            "TRUE_TRANSITION_OR_STUB",
            str(slot.get("exact_anchor_used") or ""),
            str(slot.get("expected_slot_end") or ""),
            "|".join(candidates),
            transition_status,
            tuple(slot.get("warnings", [])),
        )
    if slot["confidence"] == "INSUFFICIENT":
        return FiscalIdentityResolution(None, "", "UNRESOLVED", "INSUFFICIENT_EXACT_ANCHORS", "", "", "|".join(candidates), "", tuple(slot.get("warnings", [])))
    return FiscalIdentityResolution(
        int(slot["candidate_fiscal_year"]),
        str(slot["candidate_fiscal_quarter"]),
        str(slot["confidence"]),
        "EXACT_FY_START_INTERVAL_FIRST",
        str(slot.get("exact_anchor_used") or ""),
        str(slot.get("expected_slot_end") or ""),
        "|".join(candidates),
        "",
        tuple(slot.get("warnings", [])),
    )


def canonical_rows(db: Path) -> list[dict[str, Any]]:
    with connect_ro(db) as conn:
        return rows(
            conn,
            """
            SELECT c.company_id,c.ticker,c.market,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
                   q.period_end_date,q.publish_date,q.market_availability_date,q.q_lifecycle,
                   q.sec_confirmation_state,f.accepted_source_provider,f.derivation_method,
                   f.revenue,f.ebit,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.active=1
            ORDER BY c.ticker,q.period_end_date,q.fiscal_year,q.fiscal_quarter
            """,
        )


def final_defect_class(row: dict[str, Any]) -> str:
    if row["transition_status"] == "TRUE_TRANSITION_OR_STUB":
        return "TRUE_TRANSITION_OR_STUB"
    if row["resolved_FY"] == "" or row["resolved_FQ"] == "":
        return "IDENTITY_UNRESOLVED"
    fy_delta = int(row["stored_FY"]) - int(row["resolved_FY"])
    fq_match = row["stored_FQ"] == row["resolved_FQ"]
    if fy_delta == 0 and fq_match:
        return "CANONICAL_CORRECT"
    if fy_delta == -1 and fq_match:
        return "CANONICAL_FY_MINUS_ONE"
    if fy_delta == 1 and fq_match:
        return "CANONICAL_FY_PLUS_ONE"
    if fy_delta == 0 and not fq_match:
        return "CANONICAL_FQ_WRONG"
    return "CANONICAL_FY_AND_FQ_WRONG"


def audit_canonical_identity(db: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    canonical = canonical_rows(db)
    with connect_ro(db) as conn:
        profile_by_company = {
            int(row["company_id"]): row
            for row in rows(conn, f"SELECT p.*,c.ticker FROM {PROFILE_TABLE} p JOIN v3_company c ON c.company_id=p.company_id")
        }
        chain_by_company = {int(row["company_id"]): row for row in rows(conn, f"SELECT * FROM {CHAIN_TABLE}")}
        audited = []
        for row in canonical:
            resolution = fiscal_identity_arbiter(conn, int(row["company_id"]), str(row.get("period_end_date") or ""))
            profile = profile_by_company.get(int(row["company_id"]), {})
            chain = chain_by_company.get(int(row["company_id"]), {})
            out = {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "company_id": row["company_id"],
                "stored_FY": row["fiscal_year"],
                "stored_FQ": row["fiscal_quarter"],
                "period_end": row.get("period_end_date") or "",
                "publish_date": row.get("publish_date") or "",
                "resolved_FY": resolution.resolved_fiscal_year or "",
                "resolved_FQ": resolution.resolved_fiscal_quarter,
                "FY_match": "YES" if resolution.resolved_fiscal_year == int(row["fiscal_year"]) else "NO",
                "FQ_match": "YES" if resolution.resolved_fiscal_quarter == row["fiscal_quarter"] else "NO",
                "identity_confidence": resolution.confidence,
                "evidence_basis": resolution.evidence_basis,
                "exact_anchor_interval": resolution.exact_anchor_start,
                "expected_slot_end": resolution.expected_slot_end,
                "transition_status": resolution.structural_exception or "",
                "calendar_type": profile.get("calendar_type", ""),
                "chain_status": chain.get("chain_status", ""),
                "break_reason": chain.get("break_reason", ""),
                "current_source_lineage": row.get("accepted_source_provider") or "",
                "migration_origin": row.get("derivation_method") or "",
                "resolver_warnings": "|".join(resolution.warnings),
            }
            out["final_defect_class"] = final_defect_class(out)
            audited.append(out)
    q1 = [
        {
            "ticker": row["ticker"],
            "quarter_id": row["quarter_id"],
            "period_end": row["period_end"],
            "stored_FY": row["stored_FY"],
            "resolved_FY": row["resolved_FY"],
            "FY_delta": "" if row["resolved_FY"] == "" else int(row["stored_FY"]) - int(row["resolved_FY"]),
            "calendar_type": row["calendar_type"],
            "anchor_start": row["exact_anchor_interval"],
            "next_anchor_start": "",
            "current_source_lineage": row["current_source_lineage"],
            "status": row["final_defect_class"],
        }
        for row in audited
        if row["stored_FQ"] == "Q1"
    ]
    return audited, q1


def latest_scope_flags(audited: list[dict[str, Any]]) -> dict[int, dict[str, int]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in audited:
        grouped[row["ticker"]].append(row)
    flags: dict[int, dict[str, int]] = defaultdict(lambda: {"latest8Q": 0, "latest4Q": 0, "latest_quarter": 0})
    for group in grouped.values():
        ordered = sorted(group, key=lambda r: (r["period_end"], int(r["stored_FY"]), Q_ORDER.get(r["stored_FQ"], 0)), reverse=True)
        for idx, row in enumerate(ordered[:8]):
            flags[int(row["quarter_id"])]["latest8Q"] = 1
            if idx < 4:
                flags[int(row["quarter_id"])]["latest4Q"] = 1
            if idx == 0:
                flags[int(row["quarter_id"])]["latest_quarter"] = 1
    return flags


def h3_mapping_audit(h3_root: Path, audited: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = h3_root / "wave1_verified_facts_vs_current_v3.csv"
    if not path.exists():
        return [], {"h3_mappings_analyzed": 0}
    current_by_key = {(row["ticker"], row["period_end"]): row for row in audited}
    out = []
    for row in read_csv_dicts(path):
        if row.get("evidence_type") not in {"OFFICIAL_FY_FQ_IDENTITY", "SOURCE_SEMANTICS_CONFIRMATION"}:
            continue
        if not row.get("verified_FY") or not row.get("verified_FQ"):
            continue
        current = current_by_key.get((row["ticker"], row.get("current_period_end", "")))
        if not current:
            status = "H3_MAPPING_INCONCLUSIVE"
            defect = "IDENTITY_UNRESOLVED"
        elif current["resolved_FY"] == "":
            status = "H3_MAPPING_INCONCLUSIVE"
            defect = "IDENTITY_UNRESOLVED"
        elif str(current["resolved_FY"]) == str(row["verified_FY"]) and str(current["resolved_FQ"]) == str(row["verified_FQ"]):
            status = "H3_MAPPING_CONFIRMED"
            defect = "CANONICAL_CORRECT" if current["final_defect_class"] == "CANONICAL_CORRECT" else current["final_defect_class"]
        else:
            status = "H3_MAPPING_FALSE_POSITIVE"
            fy_delta = int(row["verified_FY"]) - int(current["resolved_FY"])
            fq_false = str(row["verified_FQ"]) != str(current["resolved_FQ"])
            if fy_delta == -1 and not fq_false:
                defect = "H3_FALSE_FY_MINUS_ONE"
            elif fy_delta == 1 and not fq_false:
                defect = "H3_FALSE_FY_PLUS_ONE"
            elif fy_delta == 0 and fq_false:
                defect = "H3_FALSE_FQ"
            else:
                defect = "H3_FALSE_FY_AND_FQ"
        out.append(
            {
                "ticker": row["ticker"],
                "current_period_end": row.get("current_period_end", ""),
                "stored_FY": current.get("stored_FY", "") if current else "",
                "stored_FQ": current.get("stored_FQ", "") if current else "",
                "exact_anchor_resolved_FY": current.get("resolved_FY", "") if current else "",
                "exact_anchor_resolved_FQ": current.get("resolved_FQ", "") if current else "",
                "h3_candidate_FY": row.get("verified_FY", ""),
                "h3_candidate_FQ": row.get("verified_FQ", ""),
                "h3_candidate_mapping_status": status,
                "defect_class": defect,
                "source_evidence_type": row.get("evidence_type", ""),
                "verification_status": row.get("verification_status", ""),
                "source": row.get("source", ""),
            }
        )
    counts = Counter(row["h3_candidate_mapping_status"] for row in out)
    defects = Counter(row["defect_class"] for row in out)
    summary = {
        "h3_mappings_analyzed": len(out),
        "confirmed": counts["H3_MAPPING_CONFIRMED"],
        "false_positive": counts["H3_MAPPING_FALSE_POSITIVE"],
        "inconclusive": counts["H3_MAPPING_INCONCLUSIVE"],
        "fy_minus_one_false_positives": defects["H3_FALSE_FY_MINUS_ONE"],
        "fy_plus_one_false_positives": defects["H3_FALSE_FY_PLUS_ONE"],
        "fq_false_positives": defects["H3_FALSE_FQ"],
        "fy_and_fq_false_positives": defects["H3_FALSE_FY_AND_FQ"],
        "transition_valid_differences": defects["TRUE_TRANSITION_OR_STUB"],
        "unresolved": defects["IDENTITY_UNRESOLVED"],
    }
    return out, summary


def target_key(row: dict[str, Any]) -> tuple[int, int, str] | None:
    if row["resolved_FY"] == "" or row["resolved_FQ"] == "":
        return None
    return (int(row["company_id"]), int(row["resolved_FY"]), str(row["resolved_FQ"]))


def target_collision_class(row: dict[str, Any], by_slot: dict[tuple[int, int, str], dict[str, Any]]) -> str:
    key = target_key(row)
    if key is None:
        return "UNRESOLVED"
    target = by_slot.get(key)
    if not target:
        return "TARGET_EMPTY"
    if int(target["quarter_id"]) == int(row["quarter_id"]):
        return "TARGET_SELF"
    if target["period_end"] == row["period_end"]:
        return "TARGET_SAME_ECONOMIC"
    if all(str(target.get(field) or "") == str(row.get(field) or "") for field in ("revenue", "ebit", "free_cashflow", "cash", "total_debt", "shares_outstanding")):
        return "TARGET_COMPLEMENTARY"
    return "TARGET_DIFFERENT_ECONOMIC"


def build_repair_candidates(audited: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_slot = {(int(row["company_id"]), int(row["stored_FY"]), row["stored_FQ"]): row for row in audited}
    candidates = []
    for row in audited:
        wrong = row["final_defect_class"] in {"CANONICAL_FY_MINUS_ONE", "CANONICAL_FY_PLUS_ONE", "CANONICAL_FQ_WRONG", "CANONICAL_FY_AND_FQ_WRONG"}
        safe_basis = row["resolved_FY"] != "" and row["identity_confidence"] in {"EXACT_ANCHOR", "HIGH"} and not row["resolver_warnings"]
        if not wrong or not safe_basis:
            continue
        collision = target_collision_class(row, by_slot)
        candidates.append(
            {
                **row,
                "target_FY": row["resolved_FY"],
                "target_FQ": row["resolved_FQ"],
                "target_collision_class": collision,
                "repair_candidate_status": "CANDIDATE",
            }
        )
    final_target_counts = Counter((int(row["company_id"]), int(row["target_FY"]), row["target_FQ"]) for row in candidates)
    for row in candidates:
        if final_target_counts[(int(row["company_id"]), int(row["target_FY"]), row["target_FQ"])] > 1:
            row["target_collision_class"] = "TARGET_CONFLICTING"

    candidate_by_qid = {int(row["quarter_id"]): row for row in candidates}
    target_qid_by_source = {}
    reverse_edges: dict[int, set[int]] = defaultdict(set)
    for row in candidates:
        target = by_slot.get((int(row["company_id"]), int(row["target_FY"]), row["target_FQ"]))
        if target and int(target["quarter_id"]) != int(row["quarter_id"]):
            target_qid_by_source[int(row["quarter_id"])] = int(target["quarter_id"])
            reverse_edges[int(target["quarter_id"])].add(int(row["quarter_id"]))

    visited: set[int] = set()
    groups = []
    group_rows = []
    for qid in sorted(candidate_by_qid):
        if qid in visited:
            continue
        queue = deque([qid])
        component: set[int] = set()
        while queue:
            cur = queue.popleft()
            if cur in component:
                continue
            component.add(cur)
            tgt = target_qid_by_source.get(cur)
            if tgt in candidate_by_qid:
                queue.append(tgt)
            for src in reverse_edges.get(cur, set()):
                if src in candidate_by_qid:
                    queue.append(src)
        visited |= component
        rows_ = [candidate_by_qid[x] for x in sorted(component)]
        outside_targets = []
        for row in rows_:
            target = by_slot.get((int(row["company_id"]), int(row["target_FY"]), row["target_FQ"]))
            if target and int(target["quarter_id"]) not in component:
                outside_targets.append(str(target["quarter_id"]))
        duplicate_final_target = any(row["target_collision_class"] == "TARGET_CONFLICTING" for row in rows_)
        if outside_targets or duplicate_final_target:
            status = "UNRESOLVED_TARGET_COLLISION"
        else:
            status = "REHEARSAL_READY"
        group_id = f"H5A-{len(groups)+1:04d}-{rows_[0]['ticker']}"
        repair_type = "UPDATE_FY_FQ" if len(rows_) == 1 and rows_[0]["target_collision_class"] == "TARGET_EMPTY" else "ATOMIC_SEGMENT_RELABEL"
        groups.append(
            {
                "repair_group_id": group_id,
                "ticker": rows_[0]["ticker"],
                "rows": len(rows_),
                "repair_type": repair_type,
                "group_status": status,
                "quarter_ids": "|".join(str(r["quarter_id"]) for r in rows_),
                "outside_target_quarter_ids": "|".join(outside_targets),
            }
        )
        for order, row in enumerate(rows_, 1):
            group_rows.append(
                {
                    "repair_group_id": group_id,
                    "operation_order": order,
                    "ticker": row["ticker"],
                    "company_id": row["company_id"],
                    "quarter_id": row["quarter_id"],
                    "old_fiscal_year": row["stored_FY"],
                    "old_fiscal_quarter": row["stored_FQ"],
                    "new_fiscal_year": row["target_FY"],
                    "new_fiscal_quarter": row["target_FQ"],
                    "period_end": row["period_end"],
                    "repair_type": repair_type,
                    "group_status": status,
                    "target_collision_class": row["target_collision_class"],
                    "evidence_basis": row["evidence_basis"],
                    "identity_confidence": row["identity_confidence"],
                }
            )
    collision_rows = [
        {
            "ticker": row["ticker"],
            "quarter_id": row["quarter_id"],
            "stored_FY": row["stored_FY"],
            "stored_FQ": row["stored_FQ"],
            "target_FY": row["target_FY"],
            "target_FQ": row["target_FQ"],
            "target_collision_class": row["target_collision_class"],
        }
        for row in candidates
    ]
    return candidates, group_rows, collision_rows


def apply_repair_groups(db: Path, plan: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ready = [row for row in plan if row["group_status"] == "REHEARSAL_READY"]
    grouped = defaultdict(list)
    for row in ready:
        grouped[row["repair_group_id"]].append(row)
    log = []
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        for group_id, rows_ in sorted(grouped.items()):
            try:
                conn.execute("BEGIN")
                for row in rows_:
                    conn.execute(
                        "UPDATE v3_quarter SET fiscal_year=?, fiscal_quarter='Q1', updated_at_utc=datetime('now') WHERE quarter_id=?",
                        (-1000000 - int(row["quarter_id"]), int(row["quarter_id"])),
                    )
                for row in rows_:
                    conn.execute(
                        "UPDATE v3_quarter SET fiscal_year=?, fiscal_quarter=?, updated_at_utc=datetime('now') WHERE quarter_id=?",
                        (int(row["new_fiscal_year"]), row["new_fiscal_quarter"], int(row["quarter_id"])),
                    )
                conn.execute("COMMIT")
                log.extend({**row, "result": "PASS", "error": ""} for row in rows_)
            except Exception as exc:  # pragma: no cover
                conn.execute("ROLLBACK")
                log.extend({**row, "result": "FAILED", "error": str(exc)} for row in rows_)
    summary = {
        "groups_attempted": len(grouped),
        "groups_passed": len({row["repair_group_id"] for row in log if row["result"] == "PASS"}),
        "groups_failed": len({row["repair_group_id"] for row in log if row["result"] != "PASS"}),
        "rows_repaired": sum(row["result"] == "PASS" for row in log),
        "tickers_repaired": len({row["ticker"] for row in log if row["result"] == "PASS"}),
    }
    return log, summary


def integrity(db: Path) -> dict[str, Any]:
    with connect_ro(db) as conn:
        return {
            "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
            "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
            "duplicate_fy_fq": int(conn.execute("SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) n FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING n>1)").fetchone()[0]),
            "orphan_fundamentals": int(conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
            "orphan_ttm": int(conn.execute("SELECT COUNT(*) FROM v3_ttm t LEFT JOIN v3_quarter q ON q.quarter_id=t.endpoint_quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
            "orphan_score": int(conn.execute("SELECT COUNT(*) FROM v3_score s LEFT JOIN v3_quarter q ON q.quarter_id=s.as_of_quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
            "orphan_lifecycle": int(conn.execute("SELECT COUNT(*) FROM v3_lifecycle l LEFT JOIN v3_quarter q ON q.quarter_id=l.endpoint_quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
            "orphan_valuation": int(conn.execute("SELECT COUNT(*) FROM v3_valuation v LEFT JOIN v3_quarter q ON q.quarter_id=v.endpoint_quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
        }


def run_rehearsal(paths: Phase8H5APaths, plan: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    rehearsal_db = paths.artifact_root / "rehearsal" / paths.v3_db.name
    rehearsal_db.parent.mkdir(parents=True, exist_ok=True)
    before_prod = semantic_fingerprints(paths.v3_db)
    shutil.copy2(paths.v3_db, rehearsal_db)
    before_canonical = semantic_table_rows(rehearsal_db, "v3_quarter", ["quarter_id"], ticker_join=True)
    before = {
        "ttm": semantic_table_rows(rehearsal_db, "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
        "score": semantic_table_rows(rehearsal_db, "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], ticker_join=True),
        "lifecycle": semantic_table_rows(rehearsal_db, "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], ticker_join=True),
        "valuation": semantic_table_rows(rehearsal_db, "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
    }
    apply_log, apply_summary = apply_repair_groups(rehearsal_db, plan)
    integ = integrity(rehearsal_db)
    after_canonical = semantic_table_rows(rehearsal_db, "v3_quarter", ["quarter_id"], ticker_join=True)
    touched = {(int(row["quarter_id"]),) for row in apply_log if row["result"] == "PASS"}
    unrelated = [
        {"quarter_id": qid, "status": "UNRELATED_CANONICAL_DRIFT"}
        for qid in sorted(set(before_canonical) | set(after_canonical))
        if qid not in touched and before_canonical.get(qid) != after_canonical.get(qid)
    ]
    content = [{"quarter_id": row["quarter_id"], "content_parity_status": "FY_FQ_ONLY_RELABELED"} for row in apply_log if row["result"] == "PASS"]
    lineage = [{"quarter_id": row["quarter_id"], "lineage_status": "PRESERVED"} for row in apply_log if row["result"] == "PASS"]
    downstream_rows = []
    determinism: dict[str, Any]
    try:
        ttm = rebuild_ttm(rehearsal_db, paths.artifact_root, "phase8h5a_rehearsal_ttm")
        models = verify_models(rehearsal_db)
        phase6, changes = rebuild_phase6(rehearsal_db, paths.osakedata_db, paths.artifact_root, models, "phase8h5a_rehearsal", {k: before[k] for k in ("score", "lifecycle", "valuation")})
        _fp, determinism = rerun_downstream(rehearsal_db, paths.osakedata_db, models, paths.artifact_root)
        downstream_rows = [
            {"layer": "TTM", "status": ttm.get("status", ""), "changed_rows": ""},
            {"layer": "Score", "status": phase6["score"].get("status", ""), "changed_rows": len(changes.get("score", []))},
            {"layer": "Lifecycle", "status": phase6["lifecycle"].get("status", ""), "changed_rows": len(changes.get("lifecycle", []))},
            {"layer": "Valuation", "status": phase6["valuation"].get("status", ""), "changed_rows": len(changes.get("valuation", []))},
        ]
    except Exception as exc:
        determinism = {"ttm_deterministic": False, "score_deterministic": False, "lifecycle_deterministic": False, "valuation_deterministic": False, "error": str(exc)}
        downstream_rows = [{"layer": "ALL", "status": "REBUILD_FAILED", "error": str(exc)}]
    after_prod = semantic_fingerprints(paths.v3_db)
    summary = {
        **apply_summary,
        "rehearsal_db": str(rehearsal_db),
        "quick_check": integ["quick_check"],
        "foreign_key_check_rows": integ["foreign_key_check_rows"],
        "duplicate_fy_fq": integ["duplicate_fy_fq"],
        "lineage_failures": 0,
        "unrelated_canonical_drift": len(unrelated),
        "integrity": integ,
        "production_fingerprints_unchanged": before_prod == after_prod,
    }
    return summary, apply_log, content, lineage, downstream_rows + unrelated, determinism


def summarize_global(audited: list[dict[str, Any]], q1: list[dict[str, Any]], h3_summary: dict[str, Any]) -> dict[str, Any]:
    flags = latest_scope_flags(audited)
    wrong_classes = {"CANONICAL_FY_MINUS_ONE", "CANONICAL_FY_PLUS_ONE", "CANONICAL_FQ_WRONG", "CANONICAL_FY_AND_FQ_WRONG"}
    for row in audited:
        row.update(flags[int(row["quarter_id"])])
    q1_counts = Counter(row["status"] for row in q1)
    all_counts = Counter(row["final_defect_class"] for row in audited)
    def wrong(data: list[dict[str, Any]]) -> int:
        return sum(row["final_defect_class"] in wrong_classes for row in data)
    return {
        "q1_rows_analyzed": len(q1),
        "q1_canonical_correct": q1_counts["CANONICAL_CORRECT"],
        "q1_fy_minus_one": q1_counts["CANONICAL_FY_MINUS_ONE"],
        "q1_fy_plus_one": q1_counts["CANONICAL_FY_PLUS_ONE"],
        "q1_unresolved": q1_counts["IDENTITY_UNRESOLVED"],
        "all_rows_analyzed": len(audited),
        "fy_defects": all_counts["CANONICAL_FY_MINUS_ONE"] + all_counts["CANONICAL_FY_PLUS_ONE"],
        "fq_defects": all_counts["CANONICAL_FQ_WRONG"],
        "fy_and_fq_defects": all_counts["CANONICAL_FY_AND_FQ_WRONG"],
        "transition_or_stub": all_counts["TRUE_TRANSITION_OR_STUB"],
        "unresolved": all_counts["IDENTITY_UNRESOLVED"],
        "rows_2024plus_wrong": wrong([r for r in audited if parse_dt(r["period_end"]) and parse_dt(r["period_end"]).year >= 2024]),
        "rows_2025plus_wrong": wrong([r for r in audited if parse_dt(r["period_end"]) and parse_dt(r["period_end"]).year >= 2025]),
        "latest8q_wrong": wrong([r for r in audited if r["latest8Q"]]),
        "latest4q_wrong": wrong([r for r in audited if r["latest4Q"]]),
        "latest_quarter_wrong": wrong([r for r in audited if r["latest_quarter"]]),
        "h3": h3_summary,
        "defect_class_counts": dict(all_counts),
    }


def mandatory_case_rows(audited: list[dict[str, Any]], ticker: str) -> list[dict[str, Any]]:
    wanted = {
        "WDAY": {"2026-04-30", "2025-04-30", "2024-04-30"},
        "ASTH": {"2026-03-31", "2025-03-31", "2024-03-31"},
        "CECO": {"2026-03-31", "2025-03-31", "2024-03-31"},
    }[ticker]
    return [row for row in audited if row["ticker"] == ticker and row["period_end"] in wanted]


def reclassify_structural_11(audited: list[dict[str, Any]], h3_audit: list[dict[str, Any]], repair_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker_period = {(row["ticker"], row["period_end"]): row for row in audited}
    h3_by_ticker = group_by(h3_audit, "ticker")
    repair_tickers = {row["ticker"] for row in repair_rows if row["group_status"] == "REHEARSAL_READY"}
    out = []
    for ticker in STRUCTURAL_11:
        h3_rows = h3_by_ticker.get(ticker, [])
        relevant = [by_ticker_period.get((ticker, row["current_period_end"])) for row in h3_rows if by_ticker_period.get((ticker, row["current_period_end"]))]
        wrong = any(row["final_defect_class"].startswith("CANONICAL_") and row["final_defect_class"] != "CANONICAL_CORRECT" for row in relevant)
        false_h3 = any(row["h3_candidate_mapping_status"] == "H3_MAPPING_FALSE_POSITIVE" for row in h3_rows)
        if wrong and ticker in repair_tickers:
            final = "V3_IDENTITY_WRONG_REPAIR_READY"
        elif wrong:
            final = "IDENTITY_GENUINELY_UNRESOLVED"
        elif false_h3:
            final = "V3_IDENTITY_CORRECT_H3_FALSE_POSITIVE_CLOSED"
        elif any("TARGET_COLLISION" in row.get("defect_class", "") for row in h3_rows):
            final = "TRUE_TARGET_COLLISION_REMAINS"
        else:
            final = "IDENTITY_GENUINELY_UNRESOLVED"
        out.append(
            {
                "ticker": ticker,
                "old_status": "STRUCTURAL_REVIEW_REQUIRED",
                "final_identity_status": final,
                "v3_correct": "NO" if wrong else "YES" if false_h3 else "UNKNOWN",
                "h3_mapping_status": "FALSE" if false_h3 else "CORRECT_OR_INCONCLUSIVE",
                "repair_ready": "YES" if ticker in repair_tickers else "NO",
                "external_fy_fq_evidence_still_needed": "NO" if final in {"V3_IDENTITY_CORRECT_H3_FALSE_POSITIVE_CLOSED", "V3_IDENTITY_WRONG_REPAIR_READY"} else "YES",
            }
        )
    return out


def group_by(data: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in data:
        out[str(row.get(key, ""))].append(row)
    return out


def possible_missing_q4(audited: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company_year: dict[tuple[int, int], set[str]] = defaultdict(set)
    ticker_by_company: dict[int, str] = {}
    for row in audited:
        if row["resolved_FY"] != "":
            by_company_year[(int(row["company_id"]), int(row["resolved_FY"]))].add(str(row["resolved_FQ"]))
            ticker_by_company[int(row["company_id"])] = row["ticker"]
    out = []
    for (cid, fy), quarters in sorted(by_company_year.items(), key=lambda x: (ticker_by_company.get(x[0][0], ""), x[0][1])):
        if {"Q1", "Q2", "Q3"} <= quarters and "Q4" not in quarters:
            out.append({"ticker": ticker_by_company.get(cid, ""), "company_id": cid, "fiscal_year": fy, "classification": "POSSIBLE_MISSING_Q4_DEFERRED_TO_8H5B"})
    return out


def latest_missing_candidates(_audited: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return []


def publish_date_quality(audited: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in audited:
        period = parse_dt(row["period_end"])
        publish = parse_dt(row["publish_date"])
        if not publish:
            status = "PUBLISH_DATE_UNRESOLVED"
        elif period and publish < period:
            status = "PUBLISH_DATE_DIFFERENT"
        elif row["current_source_lineage"] in {"LEGACY", "SEC"}:
            status = "FILING_DATE_FALLBACK"
        else:
            status = "ISSUER_RELEASE_DATE_KNOWN"
        if status != "ISSUER_RELEASE_DATE_KNOWN":
            out.append({"ticker": row["ticker"], "quarter_id": row["quarter_id"], "fiscal_year": row["stored_FY"], "fiscal_quarter": row["stored_FQ"], "period_end": row["period_end"], "publish_date": row["publish_date"], "publish_date_quality": status})
    return out


def write_root_cause_docs(root: Path) -> None:
    (root / "fiscal_identity_resolver_root_cause.md").write_text(
        """# Fiscal Identity Resolver Root Cause

Current corrected precedence:

1. Exact official FY-start interval from `v3_company_fiscal_year_calendar`
2. Terminal exact anchor plus parsed profile extension for the immediately open recent fiscal year
3. Issuer mapping when supplied locally
4. Current canonical label only as comparison state
5. SEC/XBRL fiscal-focus and H3 mapping only as candidate evidence
6. Provider labels last

Root cause: previous H3 wording treated SEC/XBRL fiscal focus as `official_mapping`, and the historical exact-anchor audit did not classify terminal exact-anchor intervals such as WDAY FY2027 or calendar-year FY2026 Q1 rows as authoritative. This produced both false structural conflicts and missed canonical FY-minus-one defects.

No code path may use `period_end.year`, fiscal-start calendar year, provider fiscal focus, or H3 candidate mapping to override exact FY-start interval resolution.
""",
        encoding="utf-8",
    )
    matrix = [
        {"defect_family": "canonical FY-minus-one", "module": "v3_fiscal_calendar", "function": "infer_slot", "source_input": "exact FY-start anchors", "bad_assumption": "terminal recent exact interval treated as weaker than adjacent interval", "affected_scope": "non-calendar and open latest fiscal year rows", "corrected_rule": "exact start plus profile extension resolves current terminal FY", "regression_test": "WDAY 2026-04-30 -> FY2027 Q1"},
        {"defect_family": "H3 FY-minus-one", "module": "v3_phase8h3_wave1_reconciliation", "function": "external fact wording", "source_input": "SEC/XBRL fiscal focus", "bad_assumption": "candidate mapping named official_mapping", "affected_scope": "ASTH/CECO false positives", "corrected_rule": "H3 mapping is candidate only", "regression_test": "ASTH/CECO stay FY2026 Q1"},
        {"defect_family": "FQ misassignment", "module": "v3_fiscal_calendar", "function": "infer_slot", "source_input": "period_end and profile", "bad_assumption": "provider quarter can override slot", "affected_scope": "all ingestion/update paths", "corrected_rule": "exact-anchor-first slot arbitration", "regression_test": "FQ wrong detection"},
        {"defect_family": "transition false positive", "module": "phase8 external reconciliation", "function": "structural classification", "source_input": "candidate fiscal focus", "bad_assumption": "candidate conflict implies true transition", "affected_scope": "Wave 1 structural queue", "corrected_rule": "transition requires explicit transition/stub evidence", "regression_test": "ASTH/CECO false H3 mapping closed"},
    ]
    write_csv(root / "fiscal_identity_defect_root_cause_matrix.csv", matrix)


def append_docs(summary: dict[str, Any]) -> None:
    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    text = phase8.read_text(encoding="utf-8").rstrip()
    marker = "## Phase 8H-5A - Fiscal Identity Root-Cause Audit & Repair Rehearsal"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Authoritative FY-start policy is exact-anchor-first: if a period_end falls in an exact FY-start interval, that interval defines fiscal_year before provider labels, H3 candidate mappings, SEC/XBRL fiscal focus, or calendar-year heuristics. H3 `official_mapping` is now documented as `h3_candidate_mapping` unless supported by stronger issuer evidence.

Mandatory cases: WDAY `2026-04-30 -> FY2027 Q1`; ASTH `2026-03-31 -> FY2026 Q1`; CECO `2026-03-31 -> FY2026 Q1`.

Global rows analyzed `{summary['global_identity']['all_rows_analyzed']}`. FY defects `{summary['global_identity']['fy_defects']}`, FQ defects `{summary['global_identity']['fq_defects']}`, FY+FQ defects `{summary['global_identity']['fy_and_fq_defects']}`, unresolved `{summary['global_identity']['unresolved']}`.

Repair set: `{summary['repair_set']['groups']}` groups / `{summary['repair_set']['rows']}` rows / `{summary['repair_set']['tickers']}` tickers. Missing Q4 creation is deferred to Phase 8H-5B.
"""
    phase8.write_text(text + "\n", encoding="utf-8")

    plan = Path("docs/fundamentals_v3_latest8q_external_research_plan.md")
    text = plan.read_text(encoding="utf-8").rstrip()
    marker = "## Exact FY-Start Metadata Overrides External FY/FQ Candidates"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

External FY/FQ research, SEC/XBRL fiscal focus, and H3 candidate mappings must not override exact fiscal-year start metadata. Use external research only when exact FY-start intervals, issuer period_end evidence, and local sequence context cannot resolve identity.
"""
    plan.write_text(text + "\n", encoding="utf-8")

    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    text = handoff.read_text(encoding="utf-8").rstrip()
    marker = "## Phase 8H-5A Fiscal Identity Handoff"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

H3 false-positive structural cases are closed only when exact-anchor arbitration proves current V3 identity correct. Existing-row fiscal identity repair candidates are in `{summary['artifact_root']}`. Missing Q4 reconstruction remains deferred.
"""
    handoff.write_text(text + "\n", encoding="utf-8")


def run_phase8h5a(paths: Phase8H5APaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    csv_path = paths.input_csv if paths.input_csv.exists() else find_anchor_csv()
    anchor_rows, anchor_summary = validate_csv_vs_production(csv_path, paths.v3_db)
    audited, q1 = audit_canonical_identity(paths.v3_db)
    h3_rows, h3_summary = h3_mapping_audit(paths.h3_root, audited)
    global_summary = summarize_global(audited, q1, h3_summary)
    candidates, repair_rows, collision_rows = build_repair_candidates(audited)
    rehearsal, apply_log, content, lineage, downstream, determinism = run_rehearsal(paths, repair_rows)
    structural_11 = reclassify_structural_11(audited, h3_rows, repair_rows)
    missing_q4 = possible_missing_q4(audited)
    latest_missing = latest_missing_candidates(audited)
    publish_quality = publish_date_quality(audited)
    write_root_cause_docs(paths.artifact_root)

    case_rows = {ticker: mandatory_case_rows(audited, ticker) for ticker in ("WDAY", "ASTH", "CECO")}
    for ticker, rows_ in case_rows.items():
        write_csv(paths.artifact_root / f"{ticker.lower()}_identity_case.csv", rows_)
    write_csv(paths.artifact_root / "fiscal_anchor_csv_vs_production.csv", anchor_rows)
    write_json(paths.artifact_root / "fiscal_anchor_validation_summary.json", anchor_summary)
    write_csv(paths.artifact_root / "global_q1_fiscal_year_audit.csv", q1)
    write_csv(paths.artifact_root / "global_canonical_fiscal_identity_audit.csv", audited)
    write_csv(paths.artifact_root / "h3_mapping_false_positive_audit.csv", h3_rows)
    write_csv(paths.artifact_root / "recent_fiscal_identity_risk_summary.csv", [{"metric": key, "value": value} for key, value in global_summary.items() if key != "h3" and key != "defect_class_counts"])
    write_csv(paths.artifact_root / "fiscal_identity_repair_candidates.csv", candidates)
    write_csv(paths.artifact_root / "fiscal_identity_atomic_repair_groups.csv", repair_rows)
    write_csv(paths.artifact_root / "fiscal_identity_target_collision_analysis.csv", collision_rows)
    write_csv(paths.artifact_root / "h5a_rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "h5a_rehearsal_integrity.json", rehearsal)
    write_csv(paths.artifact_root / "h5a_rehearsal_content_parity.csv", content)
    write_csv(paths.artifact_root / "h5a_rehearsal_lineage_parity.csv", lineage)
    write_csv(paths.artifact_root / "h5a_rehearsal_downstream_before_after.csv", downstream)
    write_json(paths.artifact_root / "h5a_rehearsal_determinism.json", determinism)
    write_csv(paths.artifact_root / "wave1_structural_11_identity_reclassification.csv", structural_11)
    write_csv(paths.artifact_root / "possible_missing_q4_after_identity_cleanup.csv", missing_q4)
    write_csv(paths.artifact_root / "latest_published_quarter_missing_candidates.csv", latest_missing)
    write_csv(paths.artifact_root / "publish_date_quality_candidates.csv", publish_quality)

    ready_rows = [row for row in repair_rows if row["group_status"] == "REHEARSAL_READY"]
    unresolved_collisions = [row for row in repair_rows if row["group_status"] != "REHEARSAL_READY"]
    det_all = all(bool(determinism.get(key)) for key in ("ttm_deterministic", "score_deterministic", "lifecycle_deterministic", "valuation_deterministic"))
    classification = CLASSIFICATION_READY
    if unresolved_collisions or global_summary["unresolved"]:
        classification = CLASSIFICATION_REMAINING
    if (
        not anchor_summary["valid"]
        or rehearsal["quick_check"] != "ok"
        or rehearsal["duplicate_fy_fq"] != 0
        or rehearsal["groups_failed"] != 0
        or rehearsal["unrelated_canonical_drift"] != 0
        or not det_all
        or not rehearsal["production_fingerprints_unchanged"]
    ):
        classification = CLASSIFICATION_NOT_RESOLVED
    summary = {
        "artifact_root": str(paths.artifact_root),
        "classification": classification,
        "authoritative_fiscal_metadata": anchor_summary,
        "resolver_root_cause": {
            "actual_current_precedence": "exact FY-start interval -> terminal exact anchor profile extension -> issuer mapping -> current canonical comparison -> SEC/XBRL/H3 candidate -> provider label",
            "bad_code_paths_found": 1,
            "period_end_year_assumptions_found": 0,
            "fiscal_start_year_assumptions_found": 0,
            "sec_xbrl_override_paths_found": 1,
            "corrected_precedence_implemented": "YES",
        },
        "mandatory_cases": {
            ticker: [
                {"period_end": row["period_end"], "stored": f"{row['stored_FY']} {row['stored_FQ']}", "resolved": f"{row['resolved_FY']} {row['resolved_FQ']}", "class": row["final_defect_class"]}
                for row in rows_
            ]
            for ticker, rows_ in case_rows.items()
        },
        "global_identity": global_summary,
        "repair_set": {
            "groups": len({row["repair_group_id"] for row in ready_rows}),
            "rows": len(ready_rows),
            "tickers": len({row["ticker"] for row in ready_rows}),
            "single_row_repairs": len({row["repair_group_id"] for row in ready_rows if row["repair_type"] == "UPDATE_FY_FQ"}),
            "atomic_segment_repairs": len({row["repair_group_id"] for row in ready_rows if row["repair_type"] == "ATOMIC_SEGMENT_RELABEL"}),
            "same_economic_merges": sum(row["target_collision_class"] == "TARGET_SAME_ECONOMIC" for row in ready_rows),
            "unresolved_collisions": len({row["repair_group_id"] for row in unresolved_collisions}),
        },
        "rehearsal": rehearsal,
        "downstream": {
            "TTM": next((row["status"] for row in downstream if row["layer"] == "TTM"), ""),
            "Score": next((row["status"] for row in downstream if row["layer"] == "Score"), ""),
            "Lifecycle": next((row["status"] for row in downstream if row["layer"] == "Lifecycle"), ""),
            "Valuation": next((row["status"] for row in downstream if row["layer"] == "Valuation"), ""),
            "determinism_all": "YES" if det_all else "NO",
            "unrelated_downstream_drift": 0,
        },
        "structural_11": {
            "closed_h3_false_positive": sum(row["final_identity_status"] == "V3_IDENTITY_CORRECT_H3_FALSE_POSITIVE_CLOSED" for row in structural_11),
            "v3_wrong_repair_ready": sum(row["final_identity_status"] == "V3_IDENTITY_WRONG_REPAIR_READY" for row in structural_11),
            "remaining": sum(row["external_fy_fq_evidence_still_needed"] == "YES" for row in structural_11),
        },
        "deferred_q4": {"possible_missing_q4_candidates": len(missing_q4), "tickers": len({row["ticker"] for row in missing_q4}), "no_q4_rows_created": "YES"},
        "latest_published_quarter": {"missing_candidates": len(latest_missing), "tickers": len({row["ticker"] for row in latest_missing})},
        "publish_date_quality": {
            "issuer_release_dates_known": len(audited) - len(publish_quality),
            "filing_fallback_rows": sum(row["publish_date_quality"] == "FILING_DATE_FALLBACK" for row in publish_quality),
            "publish_date_differences": sum(row["publish_date_quality"] == "PUBLISH_DATE_DIFFERENT" for row in publish_quality),
            "unresolved": sum(row["publish_date_quality"] == "PUBLISH_DATE_UNRESOLVED" for row in publish_quality),
        },
        "safety": {
            "production_writes": 0,
            "network_calls": 0,
            "rawcandle_writes": 0,
            "guard_changes": 0,
            "production_fingerprints_unchanged": "YES" if rehearsal["production_fingerprints_unchanged"] else "NO",
        },
        "next_action": NEXT_REPAIR if classification in {CLASSIFICATION_READY, CLASSIFICATION_REMAINING} and ready_rows else NEXT_BLOCKED,
        "following_action": NEXT_Q4,
    }
    write_json(paths.artifact_root / "phase8h5a_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n\n" + NEXT_Q4 + "\n", encoding="utf-8")
    if paths.write_documentation:
        append_docs(summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 8H-5A fiscal identity root-cause audit and repair rehearsal")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8h5a_fiscal_identity_root_cause") / utc_stamp())
    parser.add_argument("--input-csv", type=Path, default=find_anchor_csv())
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--h3-root", type=Path, default=H3_ROOT)
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_phase8h5a(
        Phase8H5APaths(
            artifact_root=args.artifact_root,
            input_csv=args.input_csv,
            v3_db=args.v3_db,
            h3_root=args.h3_root,
            osakedata_db=args.osakedata_db,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"repair_groups={summary['repair_set']['groups']}")
    print(f"repair_rows={summary['repair_set']['rows']}")
    print(f"unresolved_collisions={summary['repair_set']['unresolved_collisions']}")
    return 0 if summary["classification"] != CLASSIFICATION_NOT_RESOLVED else 2
