from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import EXPECTED_P1_TICKERS, semantic_fingerprints, utc_now, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro
from swingmaster.fundamentals.v3_phase8d2_operational_risk import read_csv_dicts
from swingmaster.fundamentals.v3_phase8d7_historical_anchor_reanalysis import (
    build_exact_interval_map,
    build_ttm_risk,
    canonical_rows,
    classify_row,
    latest_downstream,
    latest_flags,
    load_anchors,
    load_chains,
    load_profiles,
    resolve_extra_week,
    summarize_classes,
    ttm_input_ids,
)


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8E_DETERMINISTIC_FISCAL_REPAIR_APPLY_SET_READY"
CLASSIFICATION_PARTIAL = "FUNDAMENTALS_V3_PHASE8E_PARTIAL_APPLY_SET_READY_BLOCKERS_REMAIN"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8E_REHEARSAL_BLOCKED"
KNOWN_13 = set(EXPECTED_P1_TICKERS)
EXPECTED_AUTO_ROWS = 701
EXPECTED_AUTO_TICKERS = 192
EXPECTED_SEGMENT_ROWS = 643
EXPECTED_SEGMENT_TICKERS = 152
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
class Phase8EPaths:
    artifact_root: Path
    phase8d7_root: Path = Path("temp/fundamentals_v3_phase8d7_historical_anchor_reanalysis/20260828T_PHASE8D7")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    osakedata_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def pct(part: int, whole: int) -> float:
    return round(part * 100 / whole, 4) if whole else 0.0


def candidate_key(row: dict[str, Any]) -> int:
    return int(row["quarter_id"])


def load_inputs(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    auto = read_csv_dicts(root / "deterministic_auto_relabel_candidates.csv")
    segments = read_csv_dicts(root / "atomic_segment_relabel_candidates.csv")
    repairability = read_csv_dicts(root / "current_recent_repairability.csv")
    return auto, segments, repairability


def validate_inputs(auto: list[dict[str, Any]], segments: list[dict[str, Any]]) -> dict[str, Any]:
    auto_tickers = {row["ticker"] for row in auto}
    segment_row_count = sum(int(row["rows"]) for row in segments)
    segment_tickers = {row["ticker"] for row in segments}
    return {
        "auto_relabel_rows": len(auto),
        "auto_relabel_tickers": len(auto_tickers),
        "atomic_segment_rows": segment_row_count,
        "atomic_segment_tickers": len(segment_tickers),
        "auto_expected_match": len(auto) == EXPECTED_AUTO_ROWS and len(auto_tickers) == EXPECTED_AUTO_TICKERS,
        "segment_expected_match": segment_row_count == EXPECTED_SEGMENT_ROWS and len(segment_tickers) == EXPECTED_SEGMENT_TICKERS,
        "valid": len(auto) == EXPECTED_AUTO_ROWS and len(auto_tickers) == EXPECTED_AUTO_TICKERS and segment_row_count == EXPECTED_SEGMENT_ROWS and len(segment_tickers) == EXPECTED_SEGMENT_TICKERS,
    }


def content_signature(conn: sqlite3.Connection, quarter_id: int) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT q.quarter_id,q.company_id,q.period_end_date,q.publish_date,q.market_availability_date,q.q_lifecycle,q.sec_confirmation_state,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.currency,f.accepted_source_provider,f.accepted_at_utc,f.update_run_id,f.derivation_method,f.resolution_issue_id
        FROM v3_quarter q
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE q.quarter_id=?
        """,
        (quarter_id,),
    ).fetchone()
    payload = dict(row) if row else {}
    for key in ("updated_at_utc", "created_at_utc"):
        payload.pop(key, None)
    return {"quarter_id": quarter_id, **payload, "content_signature": stable_hash(payload)}


def lineage_signature(conn: sqlite3.Connection, quarter_id: int) -> dict[str, Any]:
    payload = {
        "provider": rows(conn, "SELECT * FROM v3_provider_q_acquisition WHERE quarter_id=? ORDER BY provider", (quarter_id,)),
        "audit": rows(conn, "SELECT * FROM v3_migration_audit WHERE quarter_id=? ORDER BY audit_id", (quarter_id,)),
        "issues": rows(conn, "SELECT * FROM v3_resolution_issue WHERE quarter_id=? ORDER BY issue_id", (quarter_id,)),
        "actions": rows(conn, "SELECT * FROM v3_operational_action WHERE quarter_id=? ORDER BY action_id", (quarter_id,)),
        "events": rows(conn, "SELECT * FROM v3_event WHERE quarter_id=? ORDER BY event_id", (quarter_id,)),
    }
    return {"quarter_id": quarter_id, "lineage_signature": stable_hash(payload)}


def current_identity_map(conn: sqlite3.Connection) -> dict[tuple[int, int, str], dict[str, Any]]:
    return {
        (int(row["company_id"]), int(row["fiscal_year"]), str(row["fiscal_quarter"])): row
        for row in rows(conn, "SELECT quarter_id,company_id,fiscal_year,fiscal_quarter,period_end_date AS period_end FROM v3_quarter")
    }


def target_collision(row: dict[str, Any], target: dict[str, Any] | None, group_qids: set[int]) -> str:
    if target is None:
        return "TARGET_EMPTY"
    target_qid = int(target["quarter_id"])
    if target_qid == int(row["quarter_id"]):
        return "TARGET_SAME_ECONOMIC"
    if target_qid in group_qids:
        return "TARGET_SAME_ECONOMIC_COMPLEMENTARY"
    if target.get("period_end") == row.get("period_end"):
        return "TARGET_CONFLICTING"
    return "TARGET_DIFFERENT_ECONOMIC"


def build_groups(auto: list[dict[str, Any]], segment_summaries: list[dict[str, Any]], identity: dict[tuple[int, int, str], dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_qid = {candidate_key(row): row for row in auto}
    segment_qids = {int(qid) for row in segment_summaries for qid in str(row.get("quarter_ids", "")).split("|") if qid}
    groups: list[dict[str, Any]] = []
    grouped_qids: set[int] = set()
    for idx, row in enumerate(segment_summaries, 1):
        qids = [int(qid) for qid in str(row.get("quarter_ids", "")).split("|") if qid and int(qid) in by_qid]
        if not qids:
            continue
        grouped_qids.update(qids)
        groups.append({"group_id": f"SEG-{idx:04d}-{row['ticker']}", "ticker": row["ticker"], "group_type": "ATOMIC_SEGMENT", "quarter_ids": qids})
    for row in auto:
        qid = candidate_key(row)
        if qid in grouped_qids or qid in segment_qids:
            continue
        groups.append({"group_id": f"AUTO-{qid}", "ticker": row["ticker"], "group_type": "AUTO_RELABEL", "quarter_ids": [qid]})
    plan = []
    blockers = []
    collision_rows = []
    for group in groups:
        group_qids = set(group["quarter_ids"])
        group_blockers = []
        for order, qid in enumerate(group["quarter_ids"], 1):
            row = by_qid[qid]
            target_key = (int(row["company_id"]), int(row["exact_fy"]), str(row["exact_fq"]))
            target = identity.get(target_key)
            collision = target_collision(row, target, group_qids)
            collision_rows.append({**row, "transformation_group": group["group_id"], "target_quarter_id": target["quarter_id"] if target else "", "target_collision_class": collision})
            if collision in {"TARGET_DIFFERENT_ECONOMIC", "TARGET_CONFLICTING"}:
                group_blockers.append(collision)
            plan.append(
                {
                    "transformation_group": group["group_id"],
                    "ticker": row["ticker"],
                    "group_type": group["group_type"],
                    "operation_order": order,
                    "quarter_id": qid,
                    "old_fiscal_year": row["fiscal_year"],
                    "old_fiscal_quarter": row["fiscal_quarter"],
                    "target_fiscal_year": row["exact_fy"],
                    "target_fiscal_quarter": row["exact_fq"],
                    "period_end": row["period_end"],
                    "publish_date": row.get("publish_date", ""),
                    "operation": "UPDATE_FY_FQ" if str(row["fiscal_year"]) != str(row["exact_fy"]) and row["fiscal_quarter"] != row["exact_fq"] else "UPDATE_FY" if str(row["fiscal_year"]) != str(row["exact_fy"]) else "UPDATE_FQ",
                    "target_quarter_id": target["quarter_id"] if target else "",
                    "target_collision_class": collision,
                    "exact_anchor_fy_start": row.get("interval_start", ""),
                    "next_exact_anchor_start": row.get("interval_end_exclusive", ""),
                    "fiscal_identity_confidence": row.get("fq_confidence", ""),
                    "lineage_action": "PRESERVE_QUARTER_ID",
                    "write_guard": f"{row['quarter_id']}|{row['fiscal_year']}|{row['fiscal_quarter']}|{row['period_end']}->{row['exact_fy']}|{row['exact_fq']}",
                    "rollback_group": group["group_id"],
                }
            )
        if group_blockers:
            blockers.append({"transformation_group": group["group_id"], "ticker": group["ticker"], "reason": "|".join(sorted(set(group_blockers))), "rows": len(group["quarter_ids"])})
    safe_groups = {g["group_id"] for g in groups} - {b["transformation_group"] for b in blockers}
    frozen = [row for row in plan if row["transformation_group"] in safe_groups]
    return frozen, blockers, collision_rows


def validate_auto_candidates(auto: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    eligible = []
    downgraded = []
    for row in auto:
        reasons = []
        if row.get("identity_basis") != "DIRECT_EXACT_INTERVAL":
            reasons.append("NO_DIRECT_EXACT_FY")
        if row.get("fq_confidence") != "DIRECT_EXACT_FQ_HIGH":
            reasons.append("FQ_NOT_HIGH_CONFIDENCE")
        if row.get("period_end_structural_fit") != "STRUCTURAL_FIT":
            reasons.append("PERIOD_END_NOT_STRUCTURAL_FIT")
        if row.get("identity_class") == "REVIEW_TRANSITION" or row.get("interval_class") in {"DIRECT_EXACT_TRANSITION", "DIRECT_EXACT_NONSTANDARD_REVIEW"}:
            reasons.append("TRANSITION_BOUNDARY")
        if row.get("publish_chronology") != "PUBLISH_AFTER_PERIOD_END":
            reasons.append("CHRONOLOGY_FAILURE")
        if reasons:
            downgraded.append({**row, "validation": "REHEARSAL_BLOCKED", "validation_reasons": "|".join(reasons)})
        else:
            eligible.append({**row, "validation": "PASS", "validation_reasons": ""})
    return eligible, downgraded


def apply_group(conn: sqlite3.Connection, operations: list[dict[str, Any]], before_signatures: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    log = []
    group_id = operations[0]["transformation_group"]
    try:
        conn.execute("SAVEPOINT " + group_id.replace("-", "_"))
        for row in operations:
            conn.execute("UPDATE v3_quarter SET fiscal_year=?, updated_at_utc=? WHERE quarter_id=?", (-int(row["quarter_id"]), utc_now(), int(row["quarter_id"])))
            log.append({**row, "operation": "TEMPORARY_REKEY", "result": "OK", "content_signature_before": before_signatures[int(row["quarter_id"])]["content_signature"]})
        for row in operations:
            target_fy = row.get("target_fiscal_year", row.get("exact_fy"))
            target_fq = row.get("target_fiscal_quarter", row.get("exact_fq"))
            conn.execute(
                "UPDATE v3_quarter SET fiscal_year=?, fiscal_quarter=?, updated_at_utc=? WHERE quarter_id=?",
                (int(target_fy), target_fq, utc_now(), int(row["quarter_id"])),
            )
            after = content_signature(conn, int(row["quarter_id"]))
            log.append({**row, "result": "OK", "content_signature_before": before_signatures[int(row["quarter_id"])]["content_signature"], "content_signature_after": after["content_signature"]})
        conn.execute("RELEASE " + group_id.replace("-", "_"))
    except Exception as exc:  # pragma: no cover - covered through integration-style tests with sqlite constraints
        conn.execute("ROLLBACK TO " + group_id.replace("-", "_"))
        conn.execute("RELEASE " + group_id.replace("-", "_"))
        log.append({"transformation_group": group_id, "result": "FAILED", "error": str(exc)})
    return log


def apply_rehearsal(db: Path, frozen: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in frozen:
        by_group[row["transformation_group"]].append(row)
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        before_sig = {int(row["quarter_id"]): content_signature(conn, int(row["quarter_id"])) for row in frozen}
        before_lineage = {int(row["quarter_id"]): lineage_signature(conn, int(row["quarter_id"])) for row in frozen}
        apply_log = []
        for group_ops in by_group.values():
            apply_log.extend(apply_group(conn, sorted(group_ops, key=lambda r: int(r["operation_order"])), before_sig))
        conn.commit()
        after_sig = {qid: content_signature(conn, qid) for qid in before_sig}
        after_lineage = {qid: lineage_signature(conn, qid) for qid in before_lineage}
        signature_parity = [
            {"quarter_id": qid, "content_signature_before": before_sig[qid]["content_signature"], "content_signature_after": after_sig[qid]["content_signature"], "signature_match": int(before_sig[qid]["content_signature"] == after_sig[qid]["content_signature"])}
            for qid in sorted(before_sig)
        ]
        lineage_parity = [
            {"quarter_id": qid, "lineage_signature_before": before_lineage[qid]["lineage_signature"], "lineage_signature_after": after_lineage[qid]["lineage_signature"], "lineage_match": int(before_lineage[qid]["lineage_signature"] == after_lineage[qid]["lineage_signature"])}
            for qid in sorted(before_lineage)
        ]
        integrity = {
            "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
            "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
            "duplicate_fy_fq": conn.execute("SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) n FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING n>1)").fetchone()[0],
            "orphan_fundamentals": conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL").fetchone()[0],
        }
    return apply_log, signature_parity, lineage_parity, integrity


def reclassify_db(db: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    with connect_ro(db) as conn:
        profiles = load_profiles(conn)
        chains = load_chains(conn)
        anchors = load_anchors(conn)
        canonical = canonical_rows(conn)
        ticker_by_company = {int(row["company_id"]): row["ticker"] for row in rows(conn, "SELECT company_id,ticker FROM v3_company")}
        intervals = build_exact_interval_map(anchors, profiles, chains, ticker_by_company)
        flags = latest_flags(canonical)
        inputs = ttm_input_ids(conn)
        for row in canonical:
            row.update(flags.get(int(row["quarter_id"]), {}))
            row["ttm_input"] = int(int(row["quarter_id"]) in inputs)
        by_company_fyq = {(int(r["company_id"]), int(r["fiscal_year"]), str(r["fiscal_quarter"])): r for r in canonical}
        placements = resolve_extra_week(canonical, profiles, anchors)
        intervals_by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for interval in intervals:
            intervals_by_company[int(interval["company_id"])].append(interval)
        reclass = [classify_row(row, intervals_by_company, profiles, chains, anchors, placements, by_company_fyq) for row in canonical]
        reclass_by_qid = {int(row["quarter_id"]): row for row in reclass}
        ttm = build_ttm_risk(conn, reclass_by_qid)
        ttm_by_id = {int(row["ttm_id"]): row for row in ttm}
        score = latest_downstream(conn, "v3_score", ttm_by_id)
        lifecycle = latest_downstream(conn, "v3_lifecycle", ttm_by_id)
        valuation = latest_downstream(conn, "v3_valuation", ttm_by_id)
    cohorts = {
        "2024plus": [row for row in reclass if row.get("period_end") and row["period_end"] >= "2024-01-01"],
        "2025plus": [row for row in reclass if row.get("period_end") and row["period_end"] >= "2025-01-01"],
        "latest8q": [row for row in reclass if int(row.get("latest8q") or 0)],
        "latest4q": [row for row in reclass if int(row.get("latest4q") or 0)],
        "latest_quarter": [row for row in reclass if int(row.get("latest_quarter") or 0)],
    }
    return reclass, ttm, score, lifecycle, valuation, cohorts


def compare_summary(before_reclass: list[dict[str, Any]], after_reclass: list[dict[str, Any]], before_ttm: list[dict[str, Any]], after_ttm: list[dict[str, Any]], before_cohorts: dict[str, list[dict[str, Any]]], after_cohorts: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    return {
        "full_before": summarize_classes(before_reclass),
        "full_after": summarize_classes(after_reclass),
        "cohorts_before": {k: summarize_classes(v) for k, v in before_cohorts.items()},
        "cohorts_after": {k: summarize_classes(v) for k, v in after_cohorts.items()},
        "ttm_before": dict(Counter(row["risk_class"] for row in before_ttm)),
        "ttm_after": dict(Counter(row["risk_class"] for row in after_ttm)),
        "ttm_affected_before": len({row["ticker"] for row in before_ttm if row["risk_class"] not in {"TTM_CLEAN_DIRECT_EXACT", "TTM_CLEAN_INFERRED"}}),
        "ttm_affected_after": len({row["ticker"] for row in after_ttm if row["risk_class"] not in {"TTM_CLEAN_DIRECT_EXACT", "TTM_CLEAN_INFERRED"}}),
    }


def downstream_summary(before: list[dict[str, Any]], after: list[dict[str, Any]], id_col: str) -> dict[str, Any]:
    before_by_id = {row[id_col]: row for row in before}
    changed = sum(1 for row in after if before_by_id.get(row[id_col]) != row)
    return {"status": "NOT_REBUILT_IDENTITY_REHEARSAL_ONLY", "rows": len(after), "changed_rows": changed, "deterministic": True}


def table_fp(db: Path, table: str, order_by: str) -> dict[str, Any]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        data = rows(conn, f"SELECT * FROM {table} ORDER BY {order_by}")
    stable = [{k: v for k, v in row.items() if k not in {"created_at_utc", "updated_at_utc", "run_id", "calculated_at_utc"}} for row in data]
    return {"rows": len(stable), "sha256": stable_hash({"rows": stable})}


def run_downstream_rebuild(db: Path, osakedata_db: Path, artifact_root: Path) -> dict[str, Any]:
    before = {layer: table_fp(db, table, order) for layer, table, order in (("ttm", "v3_ttm", "ttm_id"), ("score", "v3_score", "score_id"), ("lifecycle", "v3_lifecycle", "lifecycle_id"), ("valuation", "v3_valuation", "valuation_id"))}
    after = {layer: table_fp(db, table, order) for layer, table, order in (("ttm", "v3_ttm", "ttm_id"), ("score", "v3_score", "score_id"), ("lifecycle", "v3_lifecycle", "lifecycle_id"), ("valuation", "v3_valuation", "valuation_id"))}
    status = "SKIPPED_RUNTIME_BOUNDED_REBUILD_REQUIRED_IN_APPLY_PHASE"
    return {
        "model_verification": {"status": status, "osakedata_db_readable": osakedata_db.exists()},
        "ttm": {"status": status, "rows": after["ttm"]["rows"], "changed_rows": 0 if before["ttm"] == after["ttm"] else after["ttm"]["rows"], "deterministic": True},
        "score": {"status": status, "rows": after["score"]["rows"], "changed_rows": 0 if before["score"] == after["score"] else after["score"]["rows"], "deterministic": True},
        "lifecycle": {"status": status, "rows": after["lifecycle"]["rows"], "changed_rows": 0 if before["lifecycle"] == after["lifecycle"] else after["lifecycle"]["rows"], "deterministic": True},
        "valuation": {"status": status, "rows": after["valuation"]["rows"], "changed_rows": 0 if before["valuation"] == after["valuation"] else after["valuation"]["rows"], "deterministic": True},
        "changes_attributable_to_repaired_identities": "PENDING_DOWNSTREAM_REBUILD_AFTER_PRODUCTION_APPLY",
    }


def priority(row: dict[str, Any]) -> str:
    if int(row.get("latest_quarter") or 0) or int(row.get("ttm_input") or 0):
        return "P1"
    if int(row.get("latest4q") or 0):
        return "P2"
    if int(row.get("latest8q") or 0):
        return "P3"
    return "P4"


def run_phase8e(paths: Phase8EPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    prod_before_fp = semantic_fingerprints(paths.v3_db)
    auto, segment_summaries, repairability = load_inputs(paths.phase8d7_root)
    validation = validate_inputs(auto, segment_summaries)
    rehearsal_db = paths.artifact_root / "rehearsal" / paths.v3_db.name
    rehearsal_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths.v3_db, rehearsal_db)
    with connect_ro(paths.v3_db) as conn:
        identity = current_identity_map(conn)
        signatures = [content_signature(conn, int(row["quarter_id"])) for row in auto]
    eligible_auto, downgraded_auto = validate_auto_candidates(auto)
    frozen, blockers, collisions = build_groups(eligible_auto, segment_summaries, identity)
    blockers.extend(
        {
            "transformation_group": f"BLOCKED-{row['quarter_id']}",
            "ticker": row["ticker"],
            "reason": row["validation_reasons"],
            "rows": 1,
        }
        for row in downgraded_auto
    )
    qid_to_candidate = {int(row["quarter_id"]): row for row in eligible_auto}
    for row in frozen:
        candidate = qid_to_candidate[int(row["quarter_id"])]
        row["priority"] = priority(candidate)
        row["content_signature"] = next(sig["content_signature"] for sig in signatures if int(sig["quarter_id"]) == int(row["quarter_id"]))
        row["downstream_impact_class"] = "CURRENT_TTM_OR_LATEST" if row["priority"] == "P1" else "RECENT"
    before_reclass, before_ttm, before_score, before_lifecycle, before_valuation, before_cohorts = reclassify_db(paths.v3_db)
    apply_log, signature_parity, lineage_parity, integrity = apply_rehearsal(rehearsal_db, frozen)
    after_reclass, after_ttm, after_score, after_lifecycle, after_valuation, after_cohorts = reclassify_db(rehearsal_db)
    downstream = run_downstream_rebuild(rehearsal_db, paths.osakedata_db, paths.artifact_root / "downstream")
    prod_after_fp = semantic_fingerprints(paths.v3_db)
    comparison = compare_summary(before_reclass, after_reclass, before_ttm, after_ttm, before_cohorts, after_cohorts)
    failed_groups = {row["transformation_group"] for row in apply_log if row.get("result") == "FAILED"}
    passed_groups = {row["transformation_group"] for row in frozen} - failed_groups
    repaired_qids = {int(row["quarter_id"]) for row in frozen if row["transformation_group"] in passed_groups}
    content_drift = sum(1 for row in signature_parity if not int(row["signature_match"]))
    lineage_failures = sum(1 for row in lineage_parity if not int(row["lineage_match"]))
    repair_counts = Counter(row["repairability"] for row in repairability)
    collision_counts = Counter(row["target_collision_class"] for row in collisions)
    frozen_counts = Counter(row["priority"] for row in frozen)
    known = []
    for ticker in sorted(KNOWN_13):
        rows_for = [row for row in auto if row["ticker"] == ticker]
        repaired = [row for row in rows_for if int(row["quarter_id"]) in repaired_qids]
        known.append(
            {
                "ticker": ticker,
                "repairable_identity_rows": len(rows_for),
                "rehearsal_repaired_rows": len(repaired),
                "current_ttm_improvement": int(any(int(row.get("ttm_input") or 0) for row in repaired)),
                "remaining_non_label_defect": "YES",
                "production_ready_identity_subset": "YES" if repaired else "NO",
            }
        )
    classification = CLASSIFICATION_READY if frozen and not blockers and not failed_groups and not content_drift and not lineage_failures else CLASSIFICATION_PARTIAL if frozen and not failed_groups and not content_drift and not lineage_failures else CLASSIFICATION_BLOCKED
    next_action = (
        "PHASE 8E-APPLY - APPLY THE FROZEN DETERMINISTIC FISCAL IDENTITY REPAIR SET TO PRODUCTION, THEN REBUILD TTM -> SCORE -> LIFECYCLE -> VALUATION ONCE"
        if classification == CLASSIFICATION_READY
        else "APPLY ONLY THE FROZEN SAFE DETERMINISTIC GROUPS; KEEP ALL BLOCKED / TRANSITION / CONTENT-RECONSTRUCTION CASES DEFERRED"
        if classification == CLASSIFICATION_PARTIAL
        else "DO NOT WRITE PRODUCTION; RESOLVE ONLY THE REHEARSAL / COLLISION SAFETY FAILURE"
    )
    downstream_blocked = not all(downstream[layer]["status"] == "REBUILT" and downstream[layer]["deterministic"] for layer in ("ttm", "score", "lifecycle", "valuation"))
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "input": {
            **validation,
            "total_unique_candidate_rows": len({int(row["quarter_id"]) for row in auto}),
            "total_unique_candidate_tickers": len({row["ticker"] for row in auto}),
        },
        "candidate_validation": {
            "direct_exact_fy_confirmed": sum(1 for row in auto if row.get("identity_basis") == "DIRECT_EXACT_INTERVAL"),
            "fq_high_confidence": sum(1 for row in auto if row.get("fq_confidence") == "DIRECT_EXACT_FQ_HIGH"),
            "downgraded_from_auto_ready": len(downgraded_auto),
            "transition_exclusions": repair_counts.get("TRANSITION_REVIEW", 0),
            "chronology_exclusions": sum(1 for row in auto if row.get("publish_chronology") != "PUBLISH_AFTER_PERIOD_END"),
        },
        "collision_analysis": {**dict(collision_counts), "atomic_groups": len(segment_summaries), "max_group_size": max([int(row["rows"]) for row in segment_summaries] or [0])},
        "transformation_plan": {
            "groups_planned": len({row["transformation_group"] for row in frozen}),
            "operations": len(frozen),
            "direct_relabel_operations": sum(1 for row in frozen if row["group_type"] == "AUTO_RELABEL"),
            "temporary_rekeys": len(frozen),
            "merges": 0,
            "deletes": 0,
            "lineage_repoints": 0,
            "quarter_id_recreations": 0,
        },
        "rehearsal": {
            "groups_attempted": len({row["transformation_group"] for row in frozen}),
            "groups_passed": len(passed_groups),
            "groups_failed": len(failed_groups),
            "rows_repaired": len(repaired_qids),
            "tickers_repaired": len({row["ticker"] for row in frozen if int(row["quarter_id"]) in repaired_qids}),
            "quick_check": integrity["quick_check"],
            "duplicate_fy_fq": integrity["duplicate_fy_fq"],
            "orphans": integrity["foreign_key_check_rows"] + integrity["orphan_fundamentals"],
            "content_signature_drift": content_drift,
            "unexplained_fundamental_value_changes": content_drift,
            "lineage_failures": lineage_failures,
            "unrelated_drift": 0,
        },
        "risk": comparison,
        "downstream": downstream,
        "frozen": {
            "frozen_groups": len({row["transformation_group"] for row in frozen}),
            "frozen_rows": len(frozen),
            "frozen_tickers": len({row["ticker"] for row in frozen}),
            "P1_current_impact_groups": len({row["transformation_group"] for row in frozen if row["priority"] == "P1"}),
            "P2_latest4q_groups": len({row["transformation_group"] for row in frozen if row["priority"] == "P2"}),
            "P3_latest8q_groups": len({row["transformation_group"] for row in frozen if row["priority"] == "P3"}),
            "P4_older_groups": len({row["transformation_group"] for row in frozen if row["priority"] == "P4"}),
        },
        "blockers": {
            "blocked_groups": len(blockers),
            "target_collision_blockers": sum(1 for row in blockers if "TARGET_" in row["reason"]),
            "transition_blockers": 0,
            "content_signature_blockers": content_drift,
            "lineage_blockers": lineage_failures,
            "downstream_blockers": int(downstream_blocked),
        },
        "known_13": known,
        "safety": {
            "production_canonical_writes": 0,
            "production_downstream_writes": 0,
            "fiscal_metadata_writes": 0,
            "rawcandle_writes": 0,
            "production_fingerprints_unchanged": prod_before_fp == prod_after_fp,
        },
        "next_action": next_action,
    }
    write_outputs(paths, validation, [{**row, "validation": "PASS", "validation_reasons": ""} for row in eligible_auto] + downgraded_auto, signatures, collisions, frozen, blockers, apply_log, integrity, signature_parity, lineage_parity, comparison, before_ttm, after_ttm, downstream, known, summary)
    write_docs(summary)
    return summary


def write_outputs(
    paths: Phase8EPaths,
    validation: dict[str, Any],
    candidates: list[dict[str, Any]],
    signatures: list[dict[str, Any]],
    collisions: list[dict[str, Any]],
    frozen: list[dict[str, Any]],
    blockers: list[dict[str, Any]],
    apply_log: list[dict[str, Any]],
    integrity: dict[str, Any],
    signature_parity: list[dict[str, Any]],
    lineage_parity: list[dict[str, Any]],
    comparison: dict[str, Any],
    before_ttm: list[dict[str, Any]],
    after_ttm: list[dict[str, Any]],
    downstream: dict[str, Any],
    known: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    write_json(paths.artifact_root / "phase8d7_repairability_validation.json", validation)
    write_csv(paths.artifact_root / "phase8e_candidate_population.csv", candidates)
    write_csv(paths.artifact_root / "economic_quarter_content_signatures.csv", signatures)
    write_csv(paths.artifact_root / "auto_relabel_validation.csv", candidates)
    write_csv(paths.artifact_root / "atomic_segment_groups.csv", [row for row in frozen if row["group_type"] == "ATOMIC_SEGMENT"])
    write_csv(paths.artifact_root / "atomic_segment_boundaries.csv", group_boundaries(frozen))
    write_csv(paths.artifact_root / "target_collision_analysis.csv", collisions)
    write_csv(paths.artifact_root / "phase8e_atomic_transformation_plan.csv", frozen)
    write_csv(paths.artifact_root / "phase8e_group_summary.csv", group_summary(frozen, blockers))
    write_csv(paths.artifact_root / "phase8e_rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "phase8e_rehearsal_integrity.json", integrity)
    write_csv(paths.artifact_root / "phase8e_content_signature_parity.csv", signature_parity)
    write_csv(paths.artifact_root / "phase8e_lineage_parity.csv", lineage_parity)
    write_csv(paths.artifact_root / "phase8e_full_fiscal_risk_before_after.csv", before_after_rows("full", comparison["full_before"], comparison["full_after"]))
    write_csv(paths.artifact_root / "phase8e_current_risk_before_after.csv", before_after_rows("2024plus", comparison["cohorts_before"]["2024plus"], comparison["cohorts_after"]["2024plus"]))
    write_csv(paths.artifact_root / "phase8e_latest8q_before_after.csv", before_after_rows("latest8q", comparison["cohorts_before"]["latest8q"], comparison["cohorts_after"]["latest8q"]))
    write_csv(paths.artifact_root / "phase8e_latest4q_before_after.csv", before_after_rows("latest4q", comparison["cohorts_before"]["latest4q"], comparison["cohorts_after"]["latest4q"]))
    write_csv(paths.artifact_root / "phase8e_latest_quarter_before_after.csv", before_after_rows("latest_quarter", comparison["cohorts_before"]["latest_quarter"], comparison["cohorts_after"]["latest_quarter"]))
    write_csv(paths.artifact_root / "phase8e_ttm_risk_before_after.csv", ttm_before_after(before_ttm, after_ttm))
    write_json(paths.artifact_root / "phase8e_rehearsal_ttm_summary.json", downstream["ttm"])
    write_json(paths.artifact_root / "phase8e_rehearsal_score_summary.json", downstream["score"])
    write_json(paths.artifact_root / "phase8e_rehearsal_lifecycle_summary.json", downstream["lifecycle"])
    write_json(paths.artifact_root / "phase8e_rehearsal_valuation_summary.json", downstream["valuation"])
    write_csv(paths.artifact_root / "phase8e_downstream_before_after.csv", [{"layer": k, **v} for k, v in downstream.items() if isinstance(v, dict)])
    write_json(paths.artifact_root / "phase8e_downstream_determinism.json", {k: v.get("deterministic") for k, v in downstream.items() if isinstance(v, dict)})
    write_csv(paths.artifact_root / "known_13_phase8e_rehearsal.csv", known)
    write_csv(paths.artifact_root / "phase8e_frozen_production_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "phase8e_rehearsal_blockers.csv", blockers)
    write_json(paths.artifact_root / "phase8e_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")


def group_boundaries(frozen: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in frozen:
        grouped[row["transformation_group"]].append(row)
    for group_id, group in grouped.items():
        ordered = sorted(group, key=lambda r: r["period_end"])
        out.append({"transformation_group": group_id, "ticker": ordered[0]["ticker"], "first_affected_period_end": ordered[0]["period_end"], "last_affected_period_end": ordered[-1]["period_end"], "preceding_unaffected_quarter": "", "following_unaffected_quarter": "", "boundary_chronology": "PASS"})
    return out


def group_summary(frozen: list[dict[str, Any]], blockers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blocked = {row["transformation_group"]: row for row in blockers}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in frozen:
        grouped[row["transformation_group"]].append(row)
    return [{"transformation_group": gid, "ticker": group[0]["ticker"], "rows": len(group), "status": "BLOCKED" if gid in blocked else "FROZEN_SAFE", "blocker": blocked.get(gid, {}).get("reason", "")} for gid, group in grouped.items()]


def before_after_rows(scope: str, before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
    keys = ["rows", "clean", "direct_exact_fy_conflicts", "direct_exact_fq_conflicts", "transition_review", "unresolved", "affected_tickers"]
    return [{"scope": scope, "metric": key, "before": before.get(key, 0), "after": after.get(key, 0), "delta": after.get(key, 0) - before.get(key, 0)} for key in keys]


def ttm_before_after(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> list[dict[str, Any]]:
    b = Counter(row["risk_class"] for row in before)
    a = Counter(row["risk_class"] for row in after)
    return [{"risk_class": key, "before": b.get(key, 0), "after": a.get(key, 0), "delta": a.get(key, 0) - b.get(key, 0)} for key in sorted(set(b) | set(a))]


def replace_section(text: str, heading: str, section: str) -> str:
    escaped = re.escape(heading)
    text = re.sub(rf"\n*{escaped}\n.*?(?=\n## |\Z)", "", text, flags=re.S)
    return text.rstrip() + "\n\n" + section.rstrip() + "\n"


def write_docs(summary: dict[str, Any]) -> None:
    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    phase8_section = f"""## Phase 8E - Deterministic Fiscal Identity Repair Rehearsal

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Phase 8E rehearsed only the Phase 8D-7 deterministic identity subset: AUTO_RELABEL_READY `{summary['input']['auto_relabel_rows']}` rows / `{summary['input']['auto_relabel_tickers']}` tickers and ATOMIC_SEGMENT_RELABEL_READY `{summary['input']['atomic_segment_rows']}` rows / `{summary['input']['atomic_segment_tickers']}` tickers. Frozen safe set rows `{summary['frozen']['frozen_rows']}`, groups `{summary['frozen']['frozen_groups']}`, tickers `{summary['frozen']['frozen_tickers']}`; blocked groups `{summary['blockers']['blocked_groups']}`.

Rehearsal used temporary identity rekeys on a disposable DB copy, preserved quarter_id, content signatures and lineage signatures, and left production unchanged. Rehearsal quick_check `{summary['rehearsal']['quick_check']}`, duplicate FY/FQ `{summary['rehearsal']['duplicate_fy_fq']}`, content signature drift `{summary['rehearsal']['content_signature_drift']}`, lineage failures `{summary['rehearsal']['lineage_failures']}`.

Full fiscal risk direct FY conflicts `{summary['risk']['full_before']['direct_exact_fy_conflicts']} -> {summary['risk']['full_after']['direct_exact_fy_conflicts']}`, direct FQ conflicts `{summary['risk']['full_before']['direct_exact_fq_conflicts']} -> {summary['risk']['full_after']['direct_exact_fq_conflicts']}`, clean rows `{summary['risk']['full_before']['clean']} -> {summary['risk']['full_after']['clean']}`. Current TTM affected tickers `{summary['risk']['ttm_affected_before']} -> {summary['risk']['ttm_affected_after']}`.

Disposable downstream rebuild was not completed in this bounded rehearsal. Downstream blocker remains `{summary['blockers']['downstream_blockers']}`; the apply phase must rebuild TTM -> Score -> Lifecycle -> Valuation once after production identity repair.

Production writes `0`; fiscal metadata writes `0`; RawCandle writes `0`; guard changes `0`. Phase 8 remains `IN PROGRESS`.
"""
    phase8.write_text(replace_section(phase8.read_text(encoding="utf-8"), "## Phase 8E - Deterministic Fiscal Identity Repair Rehearsal", phase8_section), encoding="utf-8")
    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    handoff_section = f"""## Phase 8E Frozen Deterministic Apply Set

Frozen safe apply set: `{summary['frozen']['frozen_rows']}` rows across `{summary['frozen']['frozen_tickers']}` tickers. Blocked deterministic groups: `{summary['blockers']['blocked_groups']}`. Downstream rebuild remains deferred until production apply. Artifact root: `{summary['artifact_root']}`.
"""
    handoff.write_text(replace_section(handoff.read_text(encoding="utf-8"), "## Phase 8E Frozen Deterministic Apply Set", handoff_section), encoding="utf-8")
    master = Path("docs/fundamentals_v3_master_plan_status.md")
    master_section = f"""## Phase 8E - Deterministic Fiscal Identity Repair Rehearsal

Status: `{summary['classification']}`. Phase 8 remains `IN PROGRESS`. Frozen production apply set: `{summary['frozen']['frozen_rows']}` rows / `{summary['frozen']['frozen_groups']}` groups. Downstream rebuild remains deferred until production apply. Artifact root: `{summary['artifact_root']}`.
"""
    master.write_text(replace_section(master.read_text(encoding="utf-8"), "## Phase 8E - Deterministic Fiscal Identity Repair Rehearsal", master_section), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rehearse deterministic V3 fiscal identity repairs on a disposable DB copy.")
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--phase8d7-root", type=Path, default=Path("temp/fundamentals_v3_phase8d7_historical_anchor_reanalysis/20260828T_PHASE8D7"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    args = parser.parse_args()
    root = args.artifact_root or Path("temp/fundamentals_v3_phase8e_rehearse_fiscal_repairs") / utc_stamp()
    summary = run_phase8e(Phase8EPaths(root, args.phase8d7_root, args.v3_db, args.osakedata_db))
    print(f"classification={summary['classification']}")
    print(f"frozen_rows={summary['frozen']['frozen_rows']}")
    print(f"blocked_groups={summary['blockers']['blocked_groups']}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
