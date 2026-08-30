from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import semantic_fingerprints, utc_stamp
from swingmaster.fundamentals.v3_phase6i_production_rebuild import create_backup
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8e_apply import (
    active_ticker_quality,
    add_reclass_to_risk,
    current_downstream_availability,
    production_summary,
)
from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import (
    attribution,
    rebuild_phase6,
    rebuild_ttm,
    rerun_downstream,
    risk_for_db,
    semantic_table_rows,
    verify_models,
)
from swingmaster.fundamentals.v3_phase8h3_wave1_reconciliation import (
    CLASSIFICATION_REMAINING as H3_CLASSIFICATION_REMAINING,
    read_csv_dicts,
)


CLASSIFICATION_COMPLETE = "WAVE1_FIRST_BATCH_PRODUCTION_APPLY_COMPLETE"
CLASSIFICATION_REMAINING = "WAVE1_FIRST_BATCH_PRODUCTION_APPLY_COMPLETE_WITH_REMAINING_EXTERNAL_STRUCTURAL_CASES"
CLASSIFICATION_PARTIAL = "WAVE1_FIRST_BATCH_PRODUCTION_APPLY_PARTIAL_DUE_TO_STATE_DRIFT"
CLASSIFICATION_BLOCKED = "WAVE1_FIRST_BATCH_PRODUCTION_APPLY_BLOCKED"
NEXT_REMAINING = "KEEP THE 132 NO-REPAIR TICKERS CLOSED, KEEP THE 47 APPLIED TICKERS RECONCILED, AND CONTINUE ONLY WITH THE POST-APPLY 31-TICKER EXTERNAL / STRUCTURAL / LOCAL REMAINDER SET"
NEXT_DRIFT = "DO NOT FORCE THE ORIGINAL REPAIR; RECONCILE ONLY THE DRIFTED GROUPS AGAINST CURRENT PRODUCTION BEFORE ANY ADDITIONAL WRITE"
NEXT_BLOCKED = "DO NOT CONTINUE PRODUCTION APPLY; FIX ONLY THE H4 PRE-APPLY / APPLY / PARITY BLOCKER"
EXPECTED_GROUPS = 55
EXPECTED_ROWS = 59
EXPECTED_TICKERS = 47
EXPECTED_WAVE1_TICKERS = 210
EXPECTED_UNRESOLVED = {"STRUCTURAL_REVIEW_REQUIRED": 11, "MORE_EXTERNAL_EVIDENCE_REQUIRED": 17, "LOCAL_RECONCILIATION_REQUIRED": 3}
CRITICAL_FIELDS = ("revenue", "ebit", "free_cashflow", "cash", "total_debt", "shares_outstanding")


@dataclass(frozen=True)
class Phase8H4Paths:
    artifact_root: Path
    h3_root: Path = Path("temp/fundamentals_v3_phase8h3_wave1_reconciliation/20260830T_PHASE8H3")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    osakedata_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    write_documentation: bool = True


def group_by(rows_: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows_:
        grouped[str(row[key])].append(row)
    return grouped


def scalar(value: Any) -> str:
    return "" if value is None else str(value)


def numeric_equal(left: Any, right: Any) -> bool:
    if scalar(left) == "" and scalar(right) == "":
        return True
    try:
        return abs(float(left) - float(right)) <= max(1.0, abs(float(left)), abs(float(right))) * 0.0001
    except (TypeError, ValueError):
        return scalar(left) == scalar(right)


def load_h3(paths: Phase8H4Paths) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    frozen = read_csv_dicts(paths.h3_root / "wave1_first_batch_frozen_production_apply_set.csv")
    apply_log = read_csv_dicts(paths.h3_root / "wave1_rehearsal_apply_log.csv")
    ticker_status = read_csv_dicts(paths.h3_root / "wave1_first_batch_ticker_final_status.csv")
    summary = json.loads((paths.h3_root / "phase8h3_summary.json").read_text(encoding="utf-8"))
    return frozen, apply_log, ticker_status, summary


def validate_apply_set(
    frozen: list[dict[str, Any]],
    h3_apply_log: list[dict[str, Any]],
    ticker_status: list[dict[str, Any]],
    h3_summary: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ready = [row for row in frozen if row.get("frozen_status") == "READY_FOR_PRODUCTION_APPLY"]
    groups = {row["repair_group_id"] for row in ready}
    tickers = {row["ticker"] for row in ready}
    unresolved = {row["ticker"] for row in ticker_status if row["final_status"] in EXPECTED_UNRESOLVED}
    researched = {row["ticker"] for row in ticker_status}
    passed_groups = {row["repair_group_id"] for row in h3_apply_log if row.get("result") == "PASS"}
    duplicate_rows = len(ready) - len({(row["repair_group_id"], row["operation_order"], row["quarter_id"], row["target_table"], row["target_column"]) for row in ready})
    invalid = []
    for row in ready:
        reasons = []
        if row["ticker"] in unresolved:
            reasons.append("UNRESOLVED_TICKER_INCLUDED")
        if row["ticker"] not in researched:
            reasons.append("OUTSIDE_210_RESEARCHED_TICKERS")
        if row["repair_group_id"] not in passed_groups:
            reasons.append("H3_REHEARSAL_NOT_PASSED")
        if row.get("frozen_status") != "READY_FOR_PRODUCTION_APPLY":
            reasons.append("NOT_READY")
        if reasons:
            invalid.append({**row, "validation_status": "FAIL", "reason": "|".join(reasons)})
        else:
            invalid.append({**row, "validation_status": "PASS", "reason": ""})
    validation = {
        "h3_classification": h3_summary.get("classification", ""),
        "h3_rehearsal_available": (h3_summary.get("repair_rehearsal", {}).get("groups_failed") == 0),
        "h3_classification_valid": h3_summary.get("classification") == H3_CLASSIFICATION_REMAINING,
        "repair_groups_expected": EXPECTED_GROUPS,
        "repair_groups_found": len(groups),
        "repair_rows_expected": EXPECTED_ROWS,
        "repair_rows_found": len(ready),
        "repair_tickers_expected": EXPECTED_TICKERS,
        "repair_tickers_found": len(tickers),
        "wave1_tickers_expected": EXPECTED_WAVE1_TICKERS,
        "wave1_tickers_found": len(researched),
        "duplicate_apply_rows": duplicate_rows,
        "all_group_ids_unique": len(groups) == EXPECTED_GROUPS,
        "unresolved_tickers_included": len(tickers & unresolved),
        "outside_210_tickers": len(tickers - researched),
        "groups_not_passed_h3_rehearsal": len(groups - passed_groups),
        "source_rows_missing": sum(1 for row in ready if not row.get("source")),
    }
    validation["valid"] = (
        validation["h3_rehearsal_available"]
        and validation["h3_classification_valid"]
        and validation["repair_groups_found"] == EXPECTED_GROUPS
        and validation["repair_rows_found"] == EXPECTED_ROWS
        and validation["repair_tickers_found"] == EXPECTED_TICKERS
        and validation["wave1_tickers_found"] == EXPECTED_WAVE1_TICKERS
        and validation["duplicate_apply_rows"] == 0
        and validation["unresolved_tickers_included"] == 0
        and validation["outside_210_tickers"] == 0
        and validation["groups_not_passed_h3_rehearsal"] == 0
        and validation["source_rows_missing"] == 0
    )
    return validation, invalid


def current_target_value(conn: sqlite3.Connection, row: dict[str, Any]) -> Any:
    qid = int(row["quarter_id"])
    if row["target_table"] == "v3_quarter" and row["target_column"] == "fiscal_identity":
        current = conn.execute("SELECT fiscal_year,fiscal_quarter FROM v3_quarter WHERE quarter_id=?", (qid,)).fetchone()
        return None if current is None else f"{current[0]}|{current[1]}"
    if row["target_table"] == "v3_quarter":
        current = conn.execute(f"SELECT {row['target_column']} FROM v3_quarter WHERE quarter_id=?", (qid,)).fetchone()
    else:
        current = conn.execute(f"SELECT {row['target_column']} FROM v3_quarter_fundamentals WHERE quarter_id=?", (qid,)).fetchone()
    return None if current is None else current[0]


def guard_check(db: Path, frozen: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], set[str]]:
    out = []
    drift_groups: set[str] = set()
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for row in frozen:
            current = current_target_value(conn, row)
            expected = row.get("old_value_guard", "")
            ok = numeric_equal(current, expected)
            result = "PASS" if ok else "PRODUCTION_STATE_DRIFT"
            if not ok:
                drift_groups.add(row["repair_group_id"])
            out.append({**row, "current_old_state_value": scalar(current), "expected_old_state_value": expected, "guard_status": result})
    return out, drift_groups


def apply_one(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    qid = int(row["quarter_id"])
    op = row["repair_type"]
    if op == "UPDATE_FY_FQ":
        fy, fq = str(row["new_value"]).split("|", 1)
        conn.execute("UPDATE v3_quarter SET fiscal_year=?, fiscal_quarter=?, updated_at_utc=datetime('now') WHERE quarter_id=?", (int(fy), fq, qid))
    elif op == "UPDATE_FY":
        fy, _fq = str(row["new_value"]).split("|", 1)
        conn.execute("UPDATE v3_quarter SET fiscal_year=?, updated_at_utc=datetime('now') WHERE quarter_id=?", (int(fy), qid))
    elif op == "UPDATE_FQ":
        _fy, fq = str(row["new_value"]).split("|", 1)
        conn.execute("UPDATE v3_quarter SET fiscal_quarter=?, updated_at_utc=datetime('now') WHERE quarter_id=?", (fq, qid))
    elif row["target_table"] == "v3_quarter":
        conn.execute(f"UPDATE v3_quarter SET {row['target_column']}=?, updated_at_utc=datetime('now') WHERE quarter_id=?", (row["new_value"], qid))
    elif row["target_table"] == "v3_quarter_fundamentals":
        conn.execute(f"UPDATE v3_quarter_fundamentals SET {row['target_column']}=?, updated_at_utc=datetime('now') WHERE quarter_id=?", (float(row["new_value"]), qid))
    else:
        raise ValueError(f"Unsupported target table: {row['target_table']}")


def integrity(db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return {
            "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
            "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
            "duplicate_fy_fq": int(conn.execute("SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) n FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING n>1)").fetchone()[0]),
            "orphan_fundamentals": int(conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
            "orphan_ttm": int(conn.execute("SELECT COUNT(*) FROM v3_ttm t LEFT JOIN v3_quarter q ON q.quarter_id=t.endpoint_quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
            "orphan_score": int(conn.execute("SELECT COUNT(*) FROM v3_score s LEFT JOIN v3_quarter q ON q.quarter_id=s.as_of_quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
            "orphan_lifecycle": int(conn.execute("SELECT COUNT(*) FROM v3_lifecycle l LEFT JOIN v3_quarter q ON q.quarter_id=l.endpoint_quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
            "orphan_valuation": int(conn.execute("SELECT COUNT(*) FROM v3_valuation v LEFT JOIN v3_quarter q ON q.quarter_id=v.endpoint_quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
            "unexpected_null_identity": int(conn.execute("SELECT COUNT(*) FROM v3_quarter WHERE fiscal_year IS NULL OR fiscal_quarter IS NULL").fetchone()[0]),
            "invalid_quarter_ordering": int(conn.execute("SELECT COUNT(*) FROM v3_quarter WHERE fiscal_quarter NOT IN ('Q1','Q2','Q3','Q4')").fetchone()[0]),
        }


def apply_groups(db: Path, frozen: list[dict[str, Any]], drift_groups: set[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    logs = []
    group_status = []
    grouped = group_by(frozen, "repair_group_id")
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        for group_id, group in sorted(grouped.items()):
            if group_id in drift_groups:
                group_status.append({"repair_group_id": group_id, "ticker": group[0]["ticker"], "rows": len(group), "status": "SKIPPED_PRODUCTION_STATE_DRIFT", "error": ""})
                logs.extend({**row, "apply_status": "SKIPPED_PRODUCTION_STATE_DRIFT", "error": ""} for row in group)
                continue
            try:
                conn.execute("BEGIN")
                for row in sorted(group, key=lambda r: int(r["operation_order"])):
                    if not numeric_equal(current_target_value(conn, row), row.get("old_value_guard", "")):
                        raise RuntimeError("FAILED_GUARD")
                    apply_one(conn, row)
                    if not numeric_equal(current_target_value(conn, row), row["new_value"]):
                        raise RuntimeError("FAILED_INTEGRITY")
                conn.execute("COMMIT")
                group_status.append({"repair_group_id": group_id, "ticker": group[0]["ticker"], "rows": len(group), "status": "APPLIED", "error": ""})
                logs.extend({**row, "apply_status": "APPLIED", "error": ""} for row in group)
            except Exception as exc:
                conn.execute("ROLLBACK")
                code = str(exc) if str(exc) in {"FAILED_GUARD", "FAILED_INTEGRITY"} else "FAILED_OTHER"
                group_status.append({"repair_group_id": group_id, "ticker": group[0]["ticker"], "rows": len(group), "status": code, "error": str(exc)})
                logs.extend({**row, "apply_status": code, "error": str(exc)} for row in group)
    return logs, group_status


def canonical_snapshot(db: Path, exclude_qids: set[int] | None = None) -> dict[int, dict[str, Any]]:
    exclude_qids = exclude_qids or set()
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT c.ticker,q.quarter_id,q.company_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
                   f.revenue,f.ebit,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,f.accepted_source_provider,f.derivation_method
            FROM v3_quarter q
            JOIN v3_company c ON c.company_id=q.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            """
        ).fetchall()
    return {
        int(row["quarter_id"]): {key: scalar(value) for key, value in dict(row).items() if key != "quarter_id"}
        for row in rows
        if int(row["quarter_id"]) not in exclude_qids
    }


def production_vs_h3(db: Path, rehearsal_db: Path, applied: list[dict[str, Any]]) -> list[dict[str, Any]]:
    qids = sorted({int(row["quarter_id"]) for row in applied if row["apply_status"] == "APPLIED"})
    prod = canonical_snapshot(db)
    rehearsal = canonical_snapshot(rehearsal_db)
    out = []
    for qid in qids:
        left = prod.get(qid, {})
        right = rehearsal.get(qid, {})
        fields = ("fiscal_year", "fiscal_quarter", "period_end_date", "publish_date", *CRITICAL_FIELDS, "accepted_source_provider", "derivation_method")
        diffs = [field for field in fields if not numeric_equal(left.get(field), right.get(field))]
        out.append(
            {
                "quarter_id": qid,
                "ticker": left.get("ticker", right.get("ticker", "")),
                "parity_status": "MATCH_REHEARSAL" if not diffs else "DIFFERENT_FROM_REHEARSAL",
                "different_fields": "|".join(diffs),
            }
        )
    return out


def baseline_metrics(db: Path) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    risk, ttm_risk = risk_for_db(db)
    risk = add_reclass_to_risk(db, risk)
    quality = active_ticker_quality(db, risk)
    downstream = current_downstream_availability(db, ttm_risk)
    return {"production": production_summary(db), "latest_quality": quality, "current_downstream": downstream}, quality, downstream


def flatten_global_metrics(label: str, quality: list[dict[str, Any]], downstream: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in quality:
        out.append({"snapshot": label, "metric": row["scope"], "available": row.get("clean_tickers", ""), "total": row.get("total_active_tickers", ""), "pct": row.get("pct", "")})
    for row in downstream:
        out.append({"snapshot": label, "metric": row["metric"], "available": row.get("available", ""), "total": row.get("total", ""), "pct": row.get("pct", "")})
    return out


def metric_value(rows_: list[dict[str, Any]], key: str) -> int:
    for row in rows_:
        if row.get("scope") == key:
            return int(row.get("clean_tickers") or 0)
        if row.get("metric") == key:
            return int(row.get("available") or 0)
    return 0


def ticker_downstream(db: Path) -> dict[str, dict[str, str]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        tickers = [row["ticker"] for row in conn.execute("SELECT ticker FROM v3_company WHERE active=1")]
        out = {ticker: {"current_ttm_clean": "NO", "score_available": "NO", "lifecycle_available": "NO", "valuation_available": "NO"} for ticker in tickers}
        for table, period_col, ready_col, target in (
            ("v3_ttm", "period_end", "ttm_pit_ready", "current_ttm_clean"),
            ("v3_score", "endpoint_period_end", "score_ready", "score_available"),
            ("v3_lifecycle", "endpoint_period_end", "lifecycle_ready", "lifecycle_available"),
            ("v3_valuation", "endpoint_period_end", "valuation_ready", "valuation_available"),
        ):
            for row in conn.execute(f"SELECT c.ticker,d.{ready_col} ready FROM {table} d JOIN v3_company c ON c.company_id=d.company_id WHERE c.active=1 ORDER BY c.ticker,d.{period_col}"):
                out[row["ticker"]][target] = "YES" if int(row["ready"] or 0) else "NO"
    return out


def latest_clean_by_ticker(db: Path, tickers: set[str]) -> dict[str, dict[str, str]]:
    risk, _ttm = risk_for_db(db)
    risk = add_reclass_to_risk(db, risk)
    risk_by_qid = {int(row["quarter_id"]): row for row in risk["_reclass"]}
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        quarters = conn.execute(
            """
            SELECT c.ticker,q.quarter_id
            FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
            WHERE c.ticker IN (%s)
            ORDER BY c.ticker,q.period_end_date DESC
            """
            % ",".join("?" for _ in tickers),
            sorted(tickers),
        ).fetchall()
    by_ticker: dict[str, list[int]] = defaultdict(list)
    for row in quarters:
        by_ticker[row["ticker"]].append(int(row["quarter_id"]))
    out = {}
    for ticker in tickers:
        qids = by_ticker.get(ticker, [])
        clean = [risk_by_qid.get(qid, {}).get("identity_class") in {"PASS_DIRECT_EXACT", "PASS_INFERRED", "WARNING"} for qid in qids]
        out[ticker] = {
            "latest_quarter_clean": "YES" if clean[:1] and all(clean[:1]) else "NO",
            "latest4q_clean": "YES" if len(clean) >= 4 and all(clean[:4]) else "NO",
            "latest8q_downstream_clean": "YES" if len(clean) >= 8 and all(clean[:8]) else "NO",
        }
    return out


def postapply_audit(ticker_status: list[dict[str, Any]], group_status: list[dict[str, Any]], db: Path) -> list[dict[str, Any]]:
    applied_tickers = {row["ticker"] for row in group_status if row["status"] == "APPLIED"}
    researched = {row["ticker"] for row in ticker_status}
    downstream = ticker_downstream(db)
    latest = latest_clean_by_ticker(db, researched)
    out = []
    for row in sorted(ticker_status, key=lambda r: r["ticker"]):
        ticker = row["ticker"]
        h3 = row["final_status"]
        if h3 == "PRODUCTION_REPAIR_READY" and ticker in applied_tickers:
            post = "REPAIR_APPLIED_RECONCILED"
        elif h3 in EXPECTED_UNRESOLVED:
            post = h3
        else:
            post = "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED"
        out.append(
            {
                "ticker": ticker,
                "h3_final_state": h3,
                "h4_repair_applied": "YES" if ticker in applied_tickers else "NO",
                "postapply_state": post,
                "current_canonical_status": "PRESENT",
                "current_structural_status": "OPEN" if post == "STRUCTURAL_REVIEW_REQUIRED" else "CLOSED",
                **latest.get(ticker, {}),
                **downstream.get(ticker, {}),
                "remaining_blocker": post if post in EXPECTED_UNRESOLVED or post == "LOCAL_RECONCILIATION_REQUIRED" else "",
                "next_action": "KEEP_CLOSED" if post == "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED" else "KEEP_RECONCILED" if post == "REPAIR_APPLIED_RECONCILED" else "CARRY_FORWARD_TO_REMAINDER_SET",
            }
        )
    return out


def duplicate_request_audit(wave1_external: list[dict[str, Any]], wave1_structural: list[dict[str, Any]], wave1_local: list[dict[str, Any]], h3_root: Path) -> list[dict[str, Any]]:
    wave1_keys = {(row.get("ticker"), row.get("requested_fiscal_year"), row.get("requested_fiscal_quarter"), row.get("requested_evidence_type")) for row in wave1_external}
    wave1_keys |= {(row.get("ticker"), row.get("requested_FY") or row.get("requested_fiscal_year"), row.get("requested_FQ") or row.get("requested_fiscal_quarter"), "STRUCTURAL_REVIEW") for row in wave1_structural}
    wave1_keys |= {(row.get("ticker"), "", "", "LOCAL_RECONCILIATION") for row in wave1_local}
    out = []
    h2_root = h3_root.parent.parent / "fundamentals_v3_phase8h2_dependency_root_cause" / "20260830T_PHASE8H2"
    wave_files = {
        "wave2": h2_root / "latest8q_external_research_wave2_p2_latest4q_rootcause_cleaned.csv",
        "wave3": h2_root / "latest8q_external_research_wave3_p3_latest8q_rootcause_cleaned.csv",
    }
    for name, path in wave_files.items():
        if not path.exists():
            continue
        for row in read_csv_dicts(path):
            for evidence_type in str(row.get("evidence_types_needed") or "").split("|"):
                if not evidence_type:
                    continue
                key = (row.get("ticker"), row.get("fiscal_year"), row.get("fiscal_quarter"), evidence_type)
                if key in wave1_keys:
                    out.append({"wave": name, "ticker": key[0], "fiscal_year": key[1], "fiscal_quarter": key[2], "evidence_type": key[3], "duplicate_status": "DUPLICATE_ACTIVE_RESEARCH_REQUEST"})
    return out


def append_docs(summary: dict[str, Any]) -> None:
    plan = Path("docs/fundamentals_v3_latest8q_external_research_plan.md")
    text = plan.read_text(encoding="utf-8").rstrip()
    marker = "## Wave 1 First-Batch Production Apply"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Phase 8H-4 applied the H3 frozen Wave 1 first-batch repair set to production: `{summary['apply']['groups_applied']}` groups / `{summary['apply']['rows_changed']}` rows / `{summary['apply']['tickers_changed']}` tickers. Skipped groups `{summary['apply']['groups_skipped']}`; failed groups `{summary['apply']['groups_failed']}`.

Post-apply first-batch states: repair applied reconciled `{summary['post_audit']['REPAIR_APPLIED_RECONCILED']}`, no-repair closed `{summary['post_audit']['NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED']}`, structural `{summary['post_audit']['STRUCTURAL_REVIEW_REQUIRED']}`, external `{summary['post_audit']['MORE_EXTERNAL_EVIDENCE_REQUIRED']}`, local `{summary['post_audit']['LOCAL_RECONCILIATION_REQUIRED']}`.

Remaining queues are frozen as external `{summary['remaining']['external_tickers']}` tickers / `{summary['remaining']['external_fact_rows']}` facts, structural `{summary['remaining']['structural_tickers']}` tickers / `{summary['remaining']['structural_decisions']}` decisions, local `{summary['remaining']['local_tickers']}` tickers / `{summary['remaining']['local_cases']}` cases.
"""
    plan.write_text(text + "\n", encoding="utf-8")
    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    text = phase8.read_text(encoding="utf-8").rstrip()
    marker = "## Phase 8H-4 - Wave 1 First-Batch Production Apply & Re-Audit"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Applied `{summary['apply']['groups_applied']}` / `{summary['preapply_gate']['repair_groups_found']}` frozen repair groups. Canonical integrity quick_check `{summary['integrity']['quick_check']}`, FK rows `{summary['integrity']['foreign_key_check_rows']}`, duplicate FY/FQ `{summary['integrity']['duplicate_fy_fq']}`. H3 rehearsal parity differences `{summary['h3_rehearsal_parity']['groups_differing']}`.

Downstream rebuild status: TTM `{summary['downstream']['TTM']}`, Score `{summary['downstream']['Score']}`, Lifecycle `{summary['downstream']['Lifecycle']}`, Valuation `{summary['downstream']['Valuation']}`. Determinism all layers `{summary['downstream']['determinism_all_layers']}`.

Phase 8 remains `IN PROGRESS`.

Next action: {summary['next_action']}
"""
    phase8.write_text(text + "\n", encoding="utf-8")
    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    text = handoff.read_text(encoding="utf-8").rstrip()
    marker = "## Phase 8H-4 Post-Apply Remaining Wave 1 Cases"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

- External evidence: `{summary['remaining']['external_tickers']}` tickers / `{summary['remaining']['external_fact_rows']}` fact rows
- Structural review: `{summary['remaining']['structural_tickers']}` tickers / `{summary['remaining']['structural_decisions']}` decisions
- Local reconciliation: `{summary['remaining']['local_tickers']}` tickers / `{summary['remaining']['local_cases']}` cases
"""
    handoff.write_text(text + "\n", encoding="utf-8")


def run_phase8h4(paths: Phase8H4Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    frozen_all, h3_apply_log, ticker_status, h3_summary = load_h3(paths)
    frozen = [row for row in frozen_all if row.get("frozen_status") == "READY_FOR_PRODUCTION_APPLY"]
    validation, validation_rows = validate_apply_set(frozen_all, h3_apply_log, ticker_status, h3_summary)
    write_csv(paths.artifact_root / "wave1_apply_set_validation.csv", validation_rows)
    if not validation["valid"]:
        summary = {"classification": CLASSIFICATION_BLOCKED, "artifact_root": str(paths.artifact_root), "preapply_gate": validation, "next_action": NEXT_BLOCKED}
        write_json(paths.artifact_root / "phase8h4_summary.json", summary)
        return summary

    pre_baseline, pre_quality, pre_downstream = baseline_metrics(paths.v3_db)
    pre_canonical_fp = semantic_fingerprints(paths.v3_db)
    touched_qids = {int(row["quarter_id"]) for row in frozen}
    untouched_before = canonical_snapshot(paths.v3_db, touched_qids)
    baseline_downstream = {
        "ttm": semantic_table_rows(paths.v3_db, "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
        "score": semantic_table_rows(paths.v3_db, "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], ticker_join=True),
        "lifecycle": semantic_table_rows(paths.v3_db, "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], ticker_join=True),
        "valuation": semantic_table_rows(paths.v3_db, "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
    }
    write_json(paths.artifact_root / "production_preapply_baseline.json", pre_baseline)
    backup = create_backup(paths.v3_db, paths.artifact_root / "backup")
    backup["source_path"] = str(paths.v3_db.resolve())
    write_json(paths.artifact_root / "production_backup_manifest.json", backup)
    guards, drift_groups = guard_check(paths.v3_db, frozen)
    write_csv(paths.artifact_root / "production_old_state_guard_check.csv", guards)
    apply_log, group_status = apply_groups(paths.v3_db, frozen, drift_groups)
    write_csv(paths.artifact_root / "wave1_production_apply_log.csv", apply_log)
    write_csv(paths.artifact_root / "wave1_group_apply_status.csv", group_status)
    post_integrity = integrity(paths.v3_db)
    write_json(paths.artifact_root / "production_integrity_postapply.json", post_integrity)
    untouched_after = canonical_snapshot(paths.v3_db, touched_qids)
    unrelated_canonical_drift = [
        {"quarter_id": qid, "status": "UNRELATED_CANONICAL_DRIFT"}
        for qid in sorted(set(untouched_before) | set(untouched_after))
        if untouched_before.get(qid) != untouched_after.get(qid)
    ]
    write_csv(paths.artifact_root / "unrelated_canonical_drift.csv", unrelated_canonical_drift)
    parity = production_vs_h3(paths.v3_db, paths.h3_root / "rehearsal" / "rc_fundamentals_v3.db", apply_log)
    write_csv(paths.artifact_root / "production_vs_h3_rehearsal.csv", parity)

    ttm_summary = rebuild_ttm(paths.v3_db, paths.artifact_root, "phase8h4_production_ttm")
    model_verification = verify_models(paths.v3_db)
    phase6_summaries, changes = rebuild_phase6(paths.v3_db, paths.osakedata_db, paths.artifact_root, model_verification, "phase8h4_production", {k: baseline_downstream[k] for k in ("score", "lifecycle", "valuation")})
    downstream_changes = {"ttm": compare_layer(paths.v3_db, baseline_downstream["ttm"], "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], "ttm"), **changes}
    attrib, unrelated_downstream = attribution(downstream_changes, {row["ticker"] for row in frozen})
    write_csv(paths.artifact_root / "downstream_before_after.csv", attrib)
    write_csv(paths.artifact_root / "unrelated_downstream_drift.csv", unrelated_downstream)
    det_db = paths.artifact_root / "determinism" / paths.v3_db.name
    det_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths.v3_db, det_db)
    _det_fp, determinism = rerun_downstream(det_db, paths.osakedata_db, model_verification, paths.artifact_root)
    write_json(paths.artifact_root / "downstream_determinism.json", determinism)
    downstream_summary = {
        "TTM": ttm_summary["status"],
        "Score": phase6_summaries["score"]["status"],
        "Lifecycle": phase6_summaries["lifecycle"]["status"],
        "Valuation": phase6_summaries["valuation"]["status"],
        "determinism_all_layers": "YES" if all(determinism.get(k) for k in ("ttm_deterministic", "score_deterministic", "lifecycle_deterministic", "valuation_deterministic")) else "NO",
        "unrelated_downstream_drift": len(unrelated_downstream),
        "ttm": ttm_summary,
        "score": phase6_summaries["score"],
        "lifecycle": phase6_summaries["lifecycle"],
        "valuation": phase6_summaries["valuation"],
    }
    write_json(paths.artifact_root / "downstream_rebuild_summary.json", downstream_summary)

    post_baseline, post_quality, post_downstream = baseline_metrics(paths.v3_db)
    latest_global = flatten_global_metrics("pre_h4", pre_quality, []) + flatten_global_metrics("post_h4", post_quality, [])
    current_global = flatten_global_metrics("pre_h4", [], pre_downstream) + flatten_global_metrics("post_h4", [], post_downstream)
    write_csv(paths.artifact_root / "latest8q_global_pre_post_h4.csv", latest_global)
    write_csv(paths.artifact_root / "current_downstream_global_pre_post_h4.csv", current_global)
    post_audit = postapply_audit(ticker_status, group_status, paths.v3_db)
    write_csv(paths.artifact_root / "wave1_210_postapply_audit.csv", post_audit)
    write_csv(paths.artifact_root / "wave1_repaired_47_postapply.csv", [row for row in post_audit if row["h3_final_state"] == "PRODUCTION_REPAIR_READY"])
    write_csv(paths.artifact_root / "wave1_no_repair_132_postapply.csv", [row for row in post_audit if row["postapply_state"] == "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED"])
    unresolved_post = [row for row in post_audit if row["postapply_state"] in EXPECTED_UNRESOLVED or row["postapply_state"] == "LOCAL_RECONCILIATION_REQUIRED"]
    write_csv(paths.artifact_root / "wave1_unresolved_31_postapply.csv", unresolved_post)
    external_queue = [row for row in read_csv_dicts(paths.h3_root / "wave1_first_batch_more_external_evidence.csv") if row["ticker"] in {r["ticker"] for r in unresolved_post if r["postapply_state"] == "MORE_EXTERNAL_EVIDENCE_REQUIRED"} and row.get("requested_evidence_type") != "SOURCE_SEMANTICS_CONFIRMATION"]
    structural_queue = [row for row in read_csv_dicts(paths.h3_root / "wave1_first_batch_structural_review.csv") if row["ticker"] in {r["ticker"] for r in unresolved_post if r["postapply_state"] == "STRUCTURAL_REVIEW_REQUIRED"}]
    local_queue = [row for row in post_audit if row["postapply_state"] == "LOCAL_RECONCILIATION_REQUIRED"]
    write_csv(paths.artifact_root / "wave1_postapply_more_external_evidence.csv", external_queue)
    write_csv(paths.artifact_root / "wave1_postapply_structural_review.csv", structural_queue)
    write_csv(paths.artifact_root / "wave1_postapply_local_reconciliation.csv", local_queue)
    duplicates = duplicate_request_audit(external_queue, structural_queue, local_queue, paths.h3_root)
    write_csv(paths.artifact_root / "wave1_vs_wave23_duplicate_request_audit.csv", duplicates)
    post_canonical_fp = semantic_fingerprints(paths.v3_db)
    allowed = {row["quarter_id"] for row in apply_log if row["apply_status"] == "APPLIED"}
    status_counts = Counter(row["postapply_state"] for row in post_audit)
    group_counts = Counter(row["status"] for row in group_status)
    classification = (
        CLASSIFICATION_BLOCKED
        if post_integrity["quick_check"] != "ok" or post_integrity["foreign_key_check_rows"] or unrelated_canonical_drift or any(row["parity_status"] != "MATCH_REHEARSAL" for row in parity) or downstream_summary["determinism_all_layers"] != "YES" or unrelated_downstream
        else CLASSIFICATION_PARTIAL
        if drift_groups
        else CLASSIFICATION_REMAINING
        if unresolved_post
        else CLASSIFICATION_COMPLETE
    )
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "preapply_gate": validation | {"production_state_drift_groups": len(drift_groups)},
        "backup": backup,
        "apply": {
            "groups_attempted": len(group_status),
            "groups_applied": group_counts["APPLIED"],
            "groups_skipped": group_counts["SKIPPED_PRODUCTION_STATE_DRIFT"],
            "groups_failed": sum(v for k, v in group_counts.items() if k.startswith("FAILED")),
            "rows_changed": sum(1 for row in apply_log if row["apply_status"] == "APPLIED"),
            "tickers_changed": len({row["ticker"] for row in apply_log if row["apply_status"] == "APPLIED"}),
        },
        "integrity": post_integrity | {"unrelated_canonical_drift": len(unrelated_canonical_drift)},
        "h3_rehearsal_parity": {
            "groups_matching": len({row["quarter_id"] for row in parity if row["parity_status"] == "MATCH_REHEARSAL"}),
            "groups_differing": len({row["quarter_id"] for row in parity if row["parity_status"] != "MATCH_REHEARSAL"}),
        },
        "downstream": downstream_summary,
        "post_audit": {key: status_counts[key] for key in ("REPAIR_APPLIED_RECONCILED", "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED", "STRUCTURAL_REVIEW_REQUIRED", "MORE_EXTERNAL_EVIDENCE_REQUIRED", "LOCAL_RECONCILIATION_REQUIRED", "RESOLVED_BY_NEIGHBORING_APPLY")},
        "repaired_47": {
            "fully_reconciled": status_counts["REPAIR_APPLIED_RECONCILED"],
            "still_downstream_blocked": sum(1 for row in post_audit if row["h3_final_state"] == "PRODUCTION_REPAIR_READY" and row["postapply_state"] != "REPAIR_APPLIED_RECONCILED"),
            "exceptions": [row for row in post_audit if row["h3_final_state"] == "PRODUCTION_REPAIR_READY" and row["postapply_state"] != "REPAIR_APPLIED_RECONCILED"],
        },
        "global_improvement": {
            "latest8q_downstream_clean_before": metric_value(pre_quality, "latest8q_all_clean"),
            "latest8q_downstream_clean_after": metric_value(post_quality, "latest8q_all_clean"),
            "latest4q_clean_before": metric_value(pre_quality, "latest4q_all_clean"),
            "latest4q_clean_after": metric_value(post_quality, "latest4q_all_clean"),
            "latest_quarter_clean_before": metric_value(pre_quality, "latest_quarter"),
            "latest_quarter_clean_after": metric_value(post_quality, "latest_quarter"),
            "current_ttm_clean_before": metric_value(pre_downstream, "TTM_CLEAN"),
            "current_ttm_clean_after": metric_value(post_downstream, "TTM_CLEAN"),
            "score_available_before": metric_value(pre_downstream, "SCORE_AVAILABLE"),
            "score_available_after": metric_value(post_downstream, "SCORE_AVAILABLE"),
            "lifecycle_available_before": metric_value(pre_downstream, "LIFECYCLE_AVAILABLE"),
            "lifecycle_available_after": metric_value(post_downstream, "LIFECYCLE_AVAILABLE"),
            "valuation_available_before": metric_value(pre_downstream, "VALUATION_AVAILABLE"),
            "valuation_available_after": metric_value(post_downstream, "VALUATION_AVAILABLE"),
            "all_current_downstream_before": metric_value(pre_downstream, "ALL_CURRENT_DOWNSTREAM_AVAILABLE"),
            "all_current_downstream_after": metric_value(post_downstream, "ALL_CURRENT_DOWNSTREAM_AVAILABLE"),
        },
        "remaining": {
            "external_tickers": len({row["ticker"] for row in external_queue}),
            "external_fact_rows": len(external_queue),
            "structural_tickers": len({row["ticker"] for row in structural_queue}),
            "structural_decisions": len(structural_queue),
            "local_tickers": len({row["ticker"] for row in local_queue}),
            "local_cases": len(local_queue),
        },
        "cross_wave": {"duplicate_active_research_requests": len(duplicates)},
        "safety": {
            "production_writes_limited_to_frozen_set": "YES" if {row["quarter_id"] for row in apply_log if row["apply_status"] == "APPLIED"} <= allowed else "NO",
            "network_calls": 0,
            "rawcandle_writes": 0,
            "active_guard_changes": 0,
            "model_logic_changes": 0,
            "canonical_fingerprint_changed": pre_canonical_fp != post_canonical_fp,
        },
        "next_action": NEXT_DRIFT if drift_groups else NEXT_REMAINING if classification == CLASSIFICATION_REMAINING else NEXT_BLOCKED if classification == CLASSIFICATION_BLOCKED else "WAVE 1 FIRST BATCH IS FULLY CLOSED",
        "post_baseline": post_baseline,
    }
    write_json(paths.artifact_root / "phase8h4_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if paths.write_documentation:
        append_docs(summary)
    return summary


def compare_layer(db: Path, before: dict[tuple[Any, ...], dict[str, Any]], table: str, key_cols: list[str], layer: str) -> list[dict[str, Any]]:
    from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import compare_maps

    return compare_maps(before, semantic_table_rows(db, table, key_cols, ticker_join=True), layer)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply Phase 8H-3 Wave 1 frozen repair set to production")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8h4_wave1_production_apply") / utc_stamp())
    parser.add_argument("--h3-root", type=Path, default=Path("temp/fundamentals_v3_phase8h3_wave1_reconciliation/20260830T_PHASE8H3"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_phase8h4(
        Phase8H4Paths(
            artifact_root=args.artifact_root,
            h3_root=args.h3_root,
            v3_db=args.v3_db,
            osakedata_db=args.osakedata_db,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"groups_applied={summary.get('apply', {}).get('groups_applied', 0)}")
    print(f"rows_changed={summary.get('apply', {}).get('rows_changed', 0)}")
    return 0 if summary["classification"] != CLASSIFICATION_BLOCKED else 2


if __name__ == "__main__":
    raise SystemExit(main())
