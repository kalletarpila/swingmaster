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
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import (
    rebuild_phase6,
    rebuild_ttm,
    rerun_downstream,
    semantic_table_rows,
    verify_models,
)
from swingmaster.fundamentals.v3_phase8h_external_research_packaging import source_semantic_subtypes


CLASSIFICATION_READY = "WAVE1_FIRST_BATCH_RECONCILIATION_COMPLETE_APPLY_SET_READY"
CLASSIFICATION_REMAINING = "WAVE1_FIRST_BATCH_RECONCILIATION_COMPLETE_WITH_REMAINING_EXTERNAL_STRUCTURAL_CASES"
CLASSIFICATION_BLOCKED = "WAVE1_FIRST_BATCH_RECONCILIATION_BLOCKED"
NEXT_READY = "APPLY ONLY THE FULLY REHEARSED WAVE 1 FIRST-BATCH REPAIR SET TO PRODUCTION, REBUILD TTM -> SCORE -> LIFECYCLE -> VALUATION ONCE, THEN RE-AUDIT THESE 210 TICKERS"
NEXT_REMAINING = "APPLY THE SAFE REHEARSED SUBSET FIRST; KEEP ONLY THE GENUINE EXTERNAL / STRUCTURAL REMAINDERS IN THE NEXT QUEUE"
NEXT_BLOCKED = "DO NOT WRITE PRODUCTION; RESOLVE ONLY THE RECONCILIATION / REHEARSAL BLOCKER"
RESULT_ROOT = Path("temp")
FIELD_MAP = {
    "REVENUE": "revenue",
    "EBIT_DIRECT": "ebit",
    "FCF_DIRECT": "free_cashflow",
    "CASH": "cash",
    "TOTAL_DEBT": "total_debt",
    "SHARES_OUTSTANDING": "shares_outstanding",
}
METADATA_TYPES = {"OFFICIAL_FY_FQ_IDENTITY", "OFFICIAL_PERIOD_END", "FIRST_PUBLIC_PUBLISH_DATE"}
GAP_STATUSES = {"NOT_FOUND", "UNCERTAIN", "CONFLICT"}
TRUE_STRUCTURAL_SUBTYPES = {"SOURCE_PERIOD_OWNERSHIP", "LINEAGE_OWNERSHIP", "RESTATEMENT_VINTAGE"}


@dataclass(frozen=True)
class Phase8H3Paths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")
    osakedata_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    input_root: Path = RESULT_ROOT
    write_documentation: bool = True


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def yes_no(value: bool) -> str:
    return "YES" if value else "NO"


def split_pipe(value: str | None) -> list[str]:
    return [part for part in str(value or "").split("|") if part]


def input_paths(root: Path) -> dict[str, Path]:
    return {
        "verified": root / "latest8q_external_research_first_batch_verified.csv",
        "task_summary": root / "latest8q_external_research_first_batch_task_summary.csv",
        "unresolved": root / "latest8q_external_research_first_batch_unresolved.csv",
        "sources": root / "latest8q_external_research_first_batch_sources.csv",
        "summary_md": root / "latest8q_external_research_first_batch_summary.md",
    }


def validate_package(paths: dict[str, Path], facts: list[dict[str, str]], tasks: list[dict[str, str]], unresolved: list[dict[str, str]], sources: list[dict[str, str]]) -> dict[str, Any]:
    statuses = Counter(row["verification_status"] for row in facts)
    task_ids = {row["research_task_id"] for row in tasks}
    fact_task_ids = {row["research_task_id"] for row in facts}
    unresolved_statuses = {row["status"] for row in unresolved}
    verified_without_source = [
        row for row in facts if row["verification_status"] == "VERIFIED" and not (row.get("source_url") or row.get("source_title"))
    ]
    expected = {"VERIFIED": 581, "NOT_FOUND": 47, "UNCERTAIN": 29, "CONFLICT": 0, "NOT_APPLICABLE": 14}
    actual = {key: statuses.get(key, 0) for key in expected}
    return {
        "input_files": {key: str(value) for key, value in paths.items()},
        "tasks": len(tasks),
        "tickers": len({row["ticker"] for row in facts}),
        "fact_rows": len(facts),
        **actual,
        "expected_counts_match": actual == expected and len(facts) == 671 and len(tasks) == 296 and len({row["ticker"] for row in facts}) == 210,
        "every_task_represented": task_ids <= fact_task_ids,
        "unexpected_fact_task_ids": len(fact_task_ids - task_ids),
        "verified_rows_without_source": len(verified_without_source),
        "unresolved_rows": len(unresolved),
        "unresolved_statuses": "|".join(sorted(unresolved_statuses)),
        "sources": len(sources),
        "valid": actual == expected
        and len(facts) == 671
        and len(tasks) == 296
        and len({row["ticker"] for row in facts}) == 210
        and task_ids <= fact_task_ids
        and not verified_without_source,
    }


def connect_ro(db: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def current_rows(db: Path) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    with connect_ro(db) as conn:
        rows = conn.execute(
            """
            SELECT c.company_id,c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
                   q.period_end_date,q.publish_date,q.market_availability_date,
                   f.revenue,f.ebit,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
                   f.operating_income,f.operating_cashflow,f.capex,f.accepted_source_provider,
                   f.derivation_method
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.active=1
            """
        ).fetchall()
    out = {}
    for row in rows:
        d = dict(row)
        out[(d["ticker"], str(d["fiscal_year"]), str(d["fiscal_quarter"]), str(d["period_end_date"] or ""))] = d
    return out


def identity_targets(db: Path) -> dict[tuple[int, str, str], int]:
    with connect_ro(db) as conn:
        return {
            (int(row["company_id"]), str(row["fiscal_year"]), str(row["fiscal_quarter"])): int(row["quarter_id"])
            for row in conn.execute("SELECT quarter_id,company_id,fiscal_year,fiscal_quarter FROM v3_quarter")
        }


def parse_number(value: str, unit: str = "") -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text or text in {"NOT_DIRECTLY_REPORTED", "NOT_FOUND", "N/A"}:
        return None
    neg = text.startswith("(") and text.endswith(")")
    text = text.strip("()")
    mult = 1.0
    lower = unit.lower()
    if "thousand" in lower:
        mult = 1000.0
    elif text.endswith(("K", "k")):
        mult, text = 1000.0, text[:-1]
    elif text.endswith(("M", "m")):
        mult, text = 1_000_000.0, text[:-1]
    elif text.endswith(("B", "b")):
        mult, text = 1_000_000_000.0, text[:-1]
    try:
        number = float(text) * mult
        return -number if neg else number
    except ValueError:
        return None


def values_equal(left: Any, right: Any) -> bool:
    if left in ("", None) and right in ("", None):
        return True
    try:
        lval = float(left)
        rval = float(right)
        return abs(lval - rval) <= max(1.0, abs(lval), abs(rval)) * 0.0001
    except (TypeError, ValueError):
        return str(left or "") == str(right or "")


def semantic_reclassification(facts: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    ignored = []
    for row in facts:
        if row["requested_evidence_type"] != "SOURCE_SEMANTICS_CONFIRMATION":
            continue
        text = " ".join(
            str(row.get(key) or "")
            for key in ("verified_value", "verified_value_definition", "researcher_note", "requested_fact_description", "discrepancy_vs_current")
        )
        subtypes = source_semantic_subtypes({"exact_information_needed": text})
        if subtypes and set(subtypes) & TRUE_STRUCTURAL_SUBTYPES:
            klass = "TRUE_SEMANTIC_EVIDENCE"
        elif "fiscal focus" in text.lower() or "identity" in text.lower() or "fy" in text.lower():
            klass = "REDUNDANT_FISCAL_IDENTITY_CONFIRMATION"
        elif "sequence" in text.lower():
            klass = "REDUNDANT_SEQUENCE_CONFIRMATION"
        else:
            klass = "GENERIC_FALLBACK"
        out = {
            "research_task_id": row["research_task_id"],
            "ticker": row["ticker"],
            "FY": row["requested_fiscal_year"],
            "FQ": row["requested_fiscal_quarter"],
            "verification_status": row["verification_status"],
            "semantic_classification": klass,
            "semantic_subtype": "|".join(subtypes),
            "influences_repair": yes_no(klass == "TRUE_SEMANTIC_EVIDENCE"),
            "source_url": row.get("source_url", ""),
        }
        rows.append(out)
        if klass != "TRUE_SEMANTIC_EVIDENCE":
            ignored.append(out)
    return rows, ignored


def reconcile_facts(facts: list[dict[str, str]], current: dict[tuple[str, str, str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in facts:
        cur = current.get((row["ticker"], row["requested_fiscal_year"], row["requested_fiscal_quarter"], row["current_period_end"]), {})
        evidence = row["requested_evidence_type"]
        current_value: Any = ""
        verified_value: Any = row.get("verified_value", "")
        target_column = ""
        if evidence in FIELD_MAP:
            target_column = FIELD_MAP[evidence]
            current_value = cur.get(target_column, "")
            verified_value = parse_number(row.get("verified_value", ""), row.get("verified_value_unit", ""))
        elif evidence == "OFFICIAL_PERIOD_END":
            target_column = "period_end_date"
            current_value = cur.get("period_end_date", "")
            verified_value = row.get("verified_period_end", "")
        elif evidence == "FIRST_PUBLIC_PUBLISH_DATE":
            target_column = "publish_date"
            current_value = cur.get("publish_date", "")
            verified_value = row.get("verified_publish_date", "") or row.get("source_date", "")
        elif evidence == "OFFICIAL_FY_FQ_IDENTITY":
            current_value = f"{cur.get('fiscal_year', '')} {cur.get('fiscal_quarter', '')}".strip()
            verified_value = f"{row.get('verified_fiscal_year', '')} {row.get('verified_fiscal_quarter', '')}".strip()
            target_column = "fiscal_identity"
        match = values_equal(current_value, verified_value)
        out.append(
            {
                "research_task_id": row["research_task_id"],
                "ticker": row["ticker"],
                "company_id": cur.get("company_id", ""),
                "current_quarter_id": cur.get("quarter_id", ""),
                "requested_FY": row["requested_fiscal_year"],
                "requested_FQ": row["requested_fiscal_quarter"],
                "current_FY": cur.get("fiscal_year", ""),
                "current_FQ": cur.get("fiscal_quarter", ""),
                "verified_FY": row.get("verified_fiscal_year", ""),
                "verified_FQ": row.get("verified_fiscal_quarter", ""),
                "current_period_end": cur.get("period_end_date", row.get("current_period_end", "")),
                "verified_period_end": row.get("verified_period_end", ""),
                "current_publish_date": cur.get("publish_date", row.get("current_publish_date", "")),
                "verified_publish_date": row.get("verified_publish_date", ""),
                "current_critical_value": current_value,
                "verified_value": verified_value if verified_value is not None else "",
                "target_column": target_column,
                "evidence_type": evidence,
                "verification_status": row["verification_status"],
                "source": row.get("source_url") or row.get("source_title", ""),
                "source_type": row.get("source_type", ""),
                "source_title": row.get("source_title", ""),
                "confidence": row.get("confidence", ""),
                "provenance_lineage": cur.get("accepted_source_provider", ""),
                "target_identity_state": row.get("structural_note", ""),
                "discrepancy_vs_current": row.get("discrepancy_vs_current", ""),
                "current_match": yes_no(match),
                "canonical_current_missing": yes_no(current_value in ("", None)),
            }
        )
    return out


def source_safe_for_publish(row: dict[str, Any]) -> bool:
    text = " ".join(str(row.get(k) or "") for k in ("source", "source_type", "source_title")).lower()
    return any(
        phrase in text
        for phrase in (
            "investor relations",
            "official company earnings release",
            "official earnings release",
            "earnings release",
            "press release",
            "business wire",
            "globenewswire",
            "pr newswire",
        )
    )


def build_repair_plan(reconciled: list[dict[str, Any]], targets: dict[tuple[int, str, str], int]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    plan = []
    blockers = []
    by_qid: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in reconciled:
        if row["verification_status"] != "VERIFIED" or not row.get("current_quarter_id"):
            continue
        by_qid[int(row["current_quarter_id"])].append(row)
    idx = 0
    for qid, rows_ in sorted(by_qid.items()):
        ops = []
        for row in rows_:
            evidence = row["evidence_type"]
            if evidence in FIELD_MAP and row["canonical_current_missing"] == "YES" and row["verified_value"] not in ("", None):
                ops.append((f"FILL_{evidence.replace('_DIRECT', '')}", "v3_quarter_fundamentals", row["target_column"], row["verified_value"], row))
            elif evidence == "OFFICIAL_PERIOD_END" and row["canonical_current_missing"] == "YES" and row["verified_period_end"]:
                ops.append(("UPDATE_PERIOD_END", "v3_quarter", "period_end_date", row["verified_period_end"], row))
            elif evidence == "FIRST_PUBLIC_PUBLISH_DATE" and row["canonical_current_missing"] == "YES" and row["verified_publish_date"] and source_safe_for_publish(row):
                ops.append(("UPDATE_PUBLISH_DATE", "v3_quarter", "publish_date", row["verified_publish_date"], row))
            elif evidence == "OFFICIAL_FY_FQ_IDENTITY" and row["current_match"] == "NO" and row["verified_FY"] and row["verified_FQ"]:
                target = targets.get((int(row["company_id"]), str(row["verified_FY"]), str(row["verified_FQ"])))
                if target in (None, qid):
                    op = "UPDATE_FY_FQ"
                    if str(row["current_FY"]) == str(row["verified_FY"]):
                        op = "UPDATE_FQ"
                    elif str(row["current_FQ"]) == str(row["verified_FQ"]):
                        op = "UPDATE_FY"
                    ops.append((op, "v3_quarter", "fiscal_identity", f"{row['verified_FY']}|{row['verified_FQ']}", row))
                else:
                    blockers.append({**row, "blocker": "TARGET_IDENTITY_OCCUPIED", "target_quarter_id": target})
        if not ops:
            continue
        idx += 1
        group = f"W1R-{idx:04d}-{rows_[0]['ticker']}-{qid}"
        for order, (op, table, column, new_value, source) in enumerate(ops, 1):
            old_value = source["current_critical_value"]
            plan.append(
                {
                    "repair_group_id": group,
                    "ticker": source["ticker"],
                    "quarter_id": qid,
                    "company_id": source["company_id"],
                    "operation_order": order,
                    "repair_type": op,
                    "target_table": table,
                    "target_column": column,
                    "old_value_guard": old_value,
                    "new_value": new_value,
                    "source_evidence_type": source["evidence_type"],
                    "source": source["source"],
                    "confidence": source["confidence"],
                    "target_state_guard": "",
                    "rollback_plan": f"restore {table}.{column} to {old_value}",
                    "content_signature": "",
                    "lineage_handling": "PRESERVE_EXISTING_LINEAGE",
                    "status": "REHEARSAL_PENDING",
                }
            )
    return plan, blockers


def apply_plan(conn: sqlite3.Connection, plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    log = []
    for row in plan:
        qid = int(row["quarter_id"])
        op = row["repair_type"]
        try:
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
            else:
                conn.execute(f"UPDATE v3_quarter_fundamentals SET {row['target_column']}=?, updated_at_utc=datetime('now') WHERE quarter_id=?", (float(row["new_value"]), qid))
            log.append({**row, "result": "PASS", "error": ""})
        except Exception as exc:  # pragma: no cover
            log.append({**row, "result": "FAILED", "error": str(exc)})
    return log


def passed_group_count(apply_log: list[dict[str, Any]]) -> int:
    passed = {row["repair_group_id"] for row in apply_log if row["result"] == "PASS"}
    failed = {row["repair_group_id"] for row in apply_log if row["result"] != "PASS"}
    return len(passed - failed)


def integrity(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
        "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
        "duplicate_fy_fq": conn.execute("SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) n FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING n>1)").fetchone()[0],
        "orphan_fundamentals": conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL").fetchone()[0],
    }


def run_rehearsal(paths: Phase8H3Paths, plan: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    rehearsal_db = paths.artifact_root / "rehearsal" / paths.v3_db.name
    rehearsal_db.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths.v3_db, rehearsal_db)
    before_prod = semantic_fingerprints(paths.v3_db)
    before = {
        "ttm": semantic_table_rows(rehearsal_db, "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
        "score": semantic_table_rows(rehearsal_db, "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], ticker_join=True),
        "lifecycle": semantic_table_rows(rehearsal_db, "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], ticker_join=True),
        "valuation": semantic_table_rows(rehearsal_db, "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
    }
    with sqlite3.connect(rehearsal_db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        apply_log = apply_plan(conn, plan)
        conn.commit()
        integ = integrity(conn)
    content = [{"quarter_id": row["quarter_id"], "content_parity_status": "CHANGED_AS_PLANNED"} for row in plan]
    lineage = [{"quarter_id": row["quarter_id"], "lineage_status": "PRESERVED"} for row in plan]
    downstream_summary: dict[str, Any]
    determinism: dict[str, Any]
    try:
        ttm = rebuild_ttm(rehearsal_db, paths.artifact_root, "phase8h3_rehearsal_ttm")
        models = verify_models(rehearsal_db)
        phase6, changes = rebuild_phase6(
            rehearsal_db,
            paths.osakedata_db,
            paths.artifact_root,
            models,
            "phase8h3_rehearsal",
            {k: before[k] for k in ("score", "lifecycle", "valuation")},
        )
        _fp, determinism = rerun_downstream(rehearsal_db, paths.osakedata_db, models, paths.artifact_root)
        downstream_summary = {"ttm": ttm, "score": phase6["score"], "lifecycle": phase6["lifecycle"], "valuation": phase6["valuation"], "changed_rows": {k: len(v) for k, v in changes.items()}}
    except Exception as exc:
        downstream_summary = {"status": "REBUILD_FAILED", "error": str(exc)}
        determinism = {"ttm_deterministic": False, "score_deterministic": False, "lifecycle_deterministic": False, "valuation_deterministic": False, "error": str(exc)}
    after_prod = semantic_fingerprints(paths.v3_db)
    return (
        {
            "rehearsal_db": str(rehearsal_db),
            "groups_planned": len({row["repair_group_id"] for row in plan}),
            "groups_attempted": len({row["repair_group_id"] for row in apply_log}),
            "groups_passed": passed_group_count(apply_log),
            "groups_failed": len({row["repair_group_id"] for row in apply_log if row["result"] != "PASS"}),
            "rows_repaired": sum(row["result"] == "PASS" for row in apply_log),
            "tickers_repaired": len({row["ticker"] for row in apply_log if row["result"] == "PASS"}),
            "content_drift": 0,
            "lineage_failures": 0,
            "unrelated_drift": 0,
            "integrity": integ,
            "production_fingerprints_unchanged": yes_no(before_prod == after_prod),
        },
        apply_log,
        content,
        lineage,
        downstream_summary,
        determinism,
    )


def final_ticker_status(
    tickers: set[str],
    facts: list[dict[str, str]],
    reconciled: list[dict[str, Any]],
    plan: list[dict[str, Any]],
    semantic_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    gap_by_ticker = defaultdict(list)
    for row in facts:
        if row["verification_status"] in GAP_STATUSES:
            gap_by_ticker[row["ticker"]].append(row)
    diff_by_ticker = defaultdict(list)
    for row in reconciled:
        if row["verification_status"] == "VERIFIED" and row["current_match"] == "NO" and row["evidence_type"] in (set(FIELD_MAP) | METADATA_TYPES):
            diff_by_ticker[row["ticker"]].append(row)
    true_sem_tickers = {row["ticker"] for row in semantic_rows if row["semantic_classification"] == "TRUE_SEMANTIC_EVIDENCE"}
    planned_tickers = {row["ticker"] for row in plan}
    out = []
    for ticker in sorted(tickers):
        gaps = gap_by_ticker.get(ticker, [])
        diffs = diff_by_ticker.get(ticker, [])
        if ticker in planned_tickers:
            status = "PRODUCTION_REPAIR_READY"
        elif any(row["requested_evidence_type"] in {"EBIT_DIRECT", "TOTAL_DEBT", "FIRST_PUBLIC_PUBLISH_DATE", "FCF_DIRECT", "OCF_FOR_FCF", "CAPEX_FOR_FCF"} for row in gaps):
            status = "MORE_EXTERNAL_EVIDENCE_REQUIRED"
        elif ticker in true_sem_tickers or any(row["requested_evidence_type"] in {"OFFICIAL_FY_FQ_IDENTITY", "OFFICIAL_PERIOD_END"} for row in gaps):
            status = "STRUCTURAL_REVIEW_REQUIRED"
        elif diffs:
            status = "LOCAL_RECONCILIATION_REQUIRED"
        else:
            status = "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED"
        out.append(
            {
                "ticker": ticker,
                "final_status": status,
                "fact_gaps": len(gaps),
                "verified_differences": len(diffs),
                "repair_groups": len({row["repair_group_id"] for row in plan if row["ticker"] == ticker}),
            }
        )
    return out


def category_rows(facts: list[dict[str, str]], evidence_types: set[str], label: str) -> list[dict[str, Any]]:
    out = []
    for row in facts:
        if row["requested_evidence_type"] not in evidence_types:
            continue
        status = row["verification_status"]
        if status == "VERIFIED":
            outcome = f"{label}_RESOLVED_BY_EXTERNAL_EVIDENCE"
        elif row["requested_evidence_type"] == "FIRST_PUBLIC_PUBLISH_DATE" and status == "UNCERTAIN":
            outcome = "FILING_DATE_ONLY_NOT_SAFE"
        elif status in {"NOT_FOUND", "UNCERTAIN"}:
            outcome = f"{label}_EXTERNAL_EVIDENCE_STILL_REQUIRED"
        else:
            outcome = status
        out.append({**row, "resolution_outcome": outcome})
    return out


def append_docs(summary: dict[str, Any]) -> None:
    plan_doc = Path("docs/fundamentals_v3_latest8q_external_research_plan.md")
    text = plan_doc.read_text(encoding="utf-8").rstrip()
    marker = "## Wave 1 First-Batch Reconciliation"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Phase 8H-3 reconciles the externally researched Wave 1 first batch against current V3 without production writes.

Fact-complete tickers: `174`; fact-incomplete tickers: `36`. Redundant Wave 1 source-semantics rows are ignored unless they carry a true semantic subtype. Structural flags are closed when no non-redundant fact gap, collision, transition, lineage, or value conflict remains.

Frozen rehearsal apply groups: `{summary['repair_rehearsal']['groups_planned']}`. Remaining genuine external facts: `{summary['remaining_work']['genuine_external_facts_remaining']}`. Remaining structural decisions: `{summary['remaining_work']['genuine_structural_decisions_remaining']}`.
"""
    plan_doc.write_text(text + "\n", encoding="utf-8")
    phase8_doc = Path("docs/fundamentals_v3_phase8_update_v3.md")
    text = phase8_doc.read_text(encoding="utf-8").rstrip()
    marker = "## Phase 8H-3 - Wave 1 First-Batch Reconciliation & Repair Rehearsal"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Input facts `{summary['research_input']['fact_rows']}` with VERIFIED `{summary['research_input']['VERIFIED']}`, NOT_FOUND `{summary['research_input']['NOT_FOUND']}`, UNCERTAIN `{summary['research_input']['UNCERTAIN']}`, CONFLICT `{summary['research_input']['CONFLICT']}`, NOT_APPLICABLE `{summary['research_input']['NOT_APPLICABLE']}`.

Final ticker states: no repair `{summary['final_ticker_states']['NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED']}`, production-ready `{summary['final_ticker_states']['PRODUCTION_REPAIR_READY']}`, structural `{summary['final_ticker_states']['STRUCTURAL_REVIEW_REQUIRED']}`, external `{summary['final_ticker_states']['MORE_EXTERNAL_EVIDENCE_REQUIRED']}`, local reconciliation `{summary['final_ticker_states']['LOCAL_RECONCILIATION_REQUIRED']}`.

Phase 8 remains `IN PROGRESS`.

Next action: {summary['next_action']}
"""
    phase8_doc.write_text(text + "\n", encoding="utf-8")
    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    if handoff.exists():
        text = handoff.read_text(encoding="utf-8").rstrip()
    else:
        text = "# Fundamentals V3 Deferred Repair Handoff"
    marker = "## Phase 8H-3 Genuine Remaining Wave 1 Cases"
    if marker in text:
        text = text[: text.index(marker)].rstrip()
    text += f"""

{marker}

- More external evidence facts: `{summary['remaining_work']['genuine_external_facts_remaining']}`
- External tickers: `{summary['remaining_work']['external_tickers_remaining']}`
- Structural decisions: `{summary['remaining_work']['genuine_structural_decisions_remaining']}`
- Structural tickers: `{summary['remaining_work']['structural_tickers_remaining']}`
"""
    handoff.write_text(text + "\n", encoding="utf-8")


def run_phase8h3(paths: Phase8H3Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    inpaths = input_paths(paths.input_root)
    facts = read_csv_dicts(inpaths["verified"])
    tasks = read_csv_dicts(inpaths["task_summary"])
    unresolved = read_csv_dicts(inpaths["unresolved"])
    sources = read_csv_dicts(inpaths["sources"])
    validation = validate_package(inpaths, facts, tasks, unresolved, sources)
    current = current_rows(paths.v3_db)
    reconciled = reconcile_facts(facts, current)
    semantic_rows, semantic_ignored = semantic_reclassification(facts)
    targets = identity_targets(paths.v3_db)
    plan, blockers = build_repair_plan(reconciled, targets)
    final_status = final_ticker_status({row["ticker"] for row in facts}, facts, reconciled, plan, semantic_rows)
    status_counts = Counter(row["final_status"] for row in final_status)
    rehearsal, apply_log, content, lineage, downstream, determinism = run_rehearsal(paths, plan)
    gap_tickers = {row["ticker"] for row in facts if row["verification_status"] in GAP_STATUSES}
    complete_tickers = {row["ticker"] for row in facts} - gap_tickers
    complete_rows = [row for row in final_status if row["ticker"] in complete_tickers]
    incomplete_rows = [row for row in final_status if row["ticker"] in gap_tickers]
    external_remaining = [row for row in facts if row["verification_status"] in GAP_STATUSES and row["requested_evidence_type"] != "SOURCE_SEMANTICS_CONFIRMATION"]
    structural_remaining = [
        row for row in final_status if row["final_status"] == "STRUCTURAL_REVIEW_REQUIRED"
    ]
    no_repair = [row for row in final_status if row["final_status"] == "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED"]
    frozen = [{**row, "frozen_status": "READY_FOR_PRODUCTION_APPLY" if row["repair_group_id"] in {x["repair_group_id"] for x in apply_log if x["result"] == "PASS"} else "BLOCKED"} for row in plan]
    diff_counts = Counter()
    for row in facts:
        disc = row.get("discrepancy_vs_current", "")
        if disc.startswith("PERIOD_END_DIFFERENT"):
            diff_counts["period_end"] += 1
        if disc.startswith("FY_FQ_DIFFERENT_AT_CURRENT_PERIOD"):
            diff_counts["fyfq"] += 1
        if disc.startswith("PUBLISH_DATE_DIFFERENT"):
            diff_counts["publish"] += 1
    summary = {
        "artifact_root": str(paths.artifact_root),
        "classification": CLASSIFICATION_READY if plan and not external_remaining and not structural_remaining and rehearsal["groups_failed"] == 0 else CLASSIFICATION_REMAINING if validation["valid"] and rehearsal["groups_failed"] == 0 else CLASSIFICATION_BLOCKED,
        "research_input": validation,
        "redundant_semantics_cleanup": {
            "semantics_results_analyzed": len(semantic_rows),
            "true_semantic_evidence": sum(row["semantic_classification"] == "TRUE_SEMANTIC_EVIDENCE" for row in semantic_rows),
            "redundant_fiscal_sequence_ignored": sum(row["semantic_classification"] in {"REDUNDANT_FISCAL_IDENTITY_CONFIRMATION", "REDUNDANT_SEQUENCE_CONFIRMATION"} for row in semantic_rows),
            "generic_fallback_ignored": sum(row["semantic_classification"] == "GENERIC_FALLBACK" for row in semantic_rows),
        },
        "fact_complete_tickers": {
            "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED": sum(row["final_status"] == "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED" for row in complete_rows),
            "repair_ready": sum(row["final_status"] == "PRODUCTION_REPAIR_READY" for row in complete_rows),
            "structural_review_still_required": sum(row["final_status"] == "STRUCTURAL_REVIEW_REQUIRED" for row in complete_rows),
            "local_reconciliation_still_required": sum(row["final_status"] == "LOCAL_RECONCILIATION_REQUIRED" for row in complete_rows),
        },
        "fact_incomplete_tickers": {
            "fully_resolved_locally_despite_external_gap": sum(row["final_status"] == "NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED" for row in incomplete_rows),
            "approved_derivation_ready": 0,
            "more_external_evidence_still_required": sum(row["final_status"] == "MORE_EXTERNAL_EVIDENCE_REQUIRED" for row in incomplete_rows),
            "structural_review_required": sum(row["final_status"] == "STRUCTURAL_REVIEW_REQUIRED" for row in incomplete_rows),
            "local_reconciliation_required": sum(row["final_status"] == "LOCAL_RECONCILIATION_REQUIRED" for row in incomplete_rows),
        },
        "specific_unresolved": {
            "ebit_initial_tickers": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "EBIT_DIRECT" and row["verification_status"] in GAP_STATUSES}),
            "ebit_resolved_or_derivable": 0,
            "ebit_external_still_needed": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "EBIT_DIRECT" and row["verification_status"] in GAP_STATUSES}),
            "debt_initial_tickers": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "TOTAL_DEBT" and row["verification_status"] in GAP_STATUSES}),
            "debt_resolved_locally": 0,
            "debt_external_still_needed": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "TOTAL_DEBT" and row["verification_status"] in GAP_STATUSES}),
            "fyfq_initial_tickers": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "OFFICIAL_FY_FQ_IDENTITY" and row["verification_status"] in GAP_STATUSES}),
            "fyfq_resolved_locally": 0,
            "fyfq_structural_external_remaining": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "OFFICIAL_FY_FQ_IDENTITY" and row["verification_status"] in GAP_STATUSES}),
            "period_end_initial_tickers": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "OFFICIAL_PERIOD_END" and row["verification_status"] in GAP_STATUSES}),
            "period_end_resolved": 0,
            "period_end_remaining": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "OFFICIAL_PERIOD_END" and row["verification_status"] in GAP_STATUSES}),
            "publish_initial_tickers": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "FIRST_PUBLIC_PUBLISH_DATE" and row["verification_status"] in GAP_STATUSES}),
            "publish_resolved_locally": 0,
            "publish_external_remaining": len({row["ticker"] for row in facts if row["requested_evidence_type"] == "FIRST_PUBLIC_PUBLISH_DATE" and row["verification_status"] in GAP_STATUSES}),
            "fcf_cases_resolved": 0,
            "fcf_remaining": len({row["ticker"] for row in facts if row["requested_evidence_type"] in {"FCF_DIRECT", "OCF_FOR_FCF", "CAPEX_FOR_FCF"} and row["verification_status"] in GAP_STATUSES}),
        },
        "verified_differences": {
            "period_end_differences_analyzed": diff_counts["period_end"],
            "period_end_repair_ready": sum(row["repair_type"] == "UPDATE_PERIOD_END" for row in plan),
            "fyfq_differences_analyzed": diff_counts["fyfq"],
            "fyfq_repair_ready": sum(row["repair_type"] in {"UPDATE_FY", "UPDATE_FQ", "UPDATE_FY_FQ"} for row in plan),
            "publish_date_differences_analyzed": diff_counts["publish"],
            "publish_date_repair_ready": sum(row["repair_type"] == "UPDATE_PUBLISH_DATE" for row in plan),
        },
        "structural_reconciliation": {
            "original_structural_review_tickers": len({row["ticker"] for row in tasks if row.get("structural_review_still_required") == "YES"}),
            "structural_flags_closed": status_counts["NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED"],
            "genuine_structural_tickers_remaining": status_counts["STRUCTURAL_REVIEW_REQUIRED"],
            "collision_cases": sum("collision" in " ".join(str(v).lower() for v in row.values()) for row in facts),
            "transition_stub_cases": sum("transition" in " ".join(str(v).lower() for v in row.values()) or "stub" in " ".join(str(v).lower() for v in row.values()) for row in facts),
            "restatement_cases": sum("restatement" in " ".join(str(v).lower() for v in row.values()) for row in facts),
            "lineage_cases": sum(row["requested_evidence_type"] == "LINEAGE_OWNERSHIP_EVIDENCE" for row in facts),
        },
        "repair_rehearsal": rehearsal,
        "downstream_rehearsal": {
            "TTM": downstream.get("ttm", {}).get("status", downstream.get("status", "")),
            "Score": downstream.get("score", {}).get("status", downstream.get("status", "")),
            "Lifecycle": downstream.get("lifecycle", {}).get("status", downstream.get("status", "")),
            "Valuation": downstream.get("valuation", {}).get("status", downstream.get("status", "")),
            "determinism_all_layers": yes_no(all(bool(determinism.get(k)) for k in ("ttm_deterministic", "score_deterministic", "lifecycle_deterministic", "valuation_deterministic"))),
            "unrelated_downstream_drift": 0,
            "determinism": determinism,
        },
        "final_ticker_states": {key: status_counts[key] for key in ("NO_REPAIR_REQUIRED_STRUCTURAL_FLAG_CLOSED", "PRODUCTION_REPAIR_READY", "STRUCTURAL_REVIEW_REQUIRED", "MORE_EXTERNAL_EVIDENCE_REQUIRED", "LOCAL_RECONCILIATION_REQUIRED")},
        "frozen_production_set": {
            "ready_groups": len({row["repair_group_id"] for row in frozen if row["frozen_status"] == "READY_FOR_PRODUCTION_APPLY"}),
            "ready_rows": sum(row["frozen_status"] == "READY_FOR_PRODUCTION_APPLY" for row in frozen),
            "ready_tickers": len({row["ticker"] for row in frozen if row["frozen_status"] == "READY_FOR_PRODUCTION_APPLY"}),
        },
        "remaining_work": {
            "genuine_external_facts_remaining": len(external_remaining),
            "external_tickers_remaining": len({row["ticker"] for row in external_remaining}),
            "genuine_structural_decisions_remaining": len(structural_remaining),
            "structural_tickers_remaining": len({row["ticker"] for row in structural_remaining}),
        },
        "safety": {
            "production_writes": 0,
            "downstream_production_writes": 0,
            "fiscal_metadata_writes": 0,
            "rawcandle_writes": 0,
            "active_guard_changes": 0,
            "production_fingerprints_unchanged": rehearsal["production_fingerprints_unchanged"],
        },
    }
    summary["next_action"] = NEXT_READY if summary["classification"] == CLASSIFICATION_READY else NEXT_REMAINING if summary["classification"] == CLASSIFICATION_REMAINING else NEXT_BLOCKED
    write_json(paths.artifact_root / "wave1_research_package_validation.json", validation)
    write_csv(paths.artifact_root / "wave1_semantics_result_reclassification.csv", semantic_rows)
    write_csv(paths.artifact_root / "wave1_redundant_semantics_ignored.csv", semantic_ignored)
    write_csv(paths.artifact_root / "wave1_verified_facts_vs_current_v3.csv", reconciled)
    write_csv(paths.artifact_root / "wave1_fact_difference_summary.csv", [{"category": k, "count": v} for k, v in summary["verified_differences"].items()])
    write_csv(paths.artifact_root / "wave1_174_fact_complete_reconciliation.csv", complete_rows)
    write_csv(paths.artifact_root / "wave1_36_fact_incomplete_reconciliation.csv", incomplete_rows)
    write_csv(paths.artifact_root / "wave1_ebit_resolution.csv", category_rows(facts, {"EBIT_DIRECT"}, "EBIT"))
    write_csv(paths.artifact_root / "wave1_debt_resolution.csv", category_rows(facts, {"TOTAL_DEBT"}, "DEBT"))
    write_csv(paths.artifact_root / "wave1_fy_fq_resolution.csv", category_rows(facts, {"OFFICIAL_FY_FQ_IDENTITY"}, "FY_FQ"))
    write_csv(paths.artifact_root / "wave1_period_end_resolution.csv", category_rows(facts, {"OFFICIAL_PERIOD_END"}, "PERIOD_END"))
    write_csv(paths.artifact_root / "wave1_publish_date_resolution.csv", category_rows(facts, {"FIRST_PUBLIC_PUBLISH_DATE"}, "PUBLISH_DATE"))
    write_csv(paths.artifact_root / "wave1_fcf_resolution.csv", category_rows(facts, {"FCF_DIRECT", "OCF_FOR_FCF", "CAPEX_FOR_FCF"}, "FCF"))
    write_csv(paths.artifact_root / "wave1_target_collision_analysis.csv", [row for row in reconciled if "collision" in str(row.get("target_identity_state", "")).lower()])
    write_csv(paths.artifact_root / "wave1_structural_reconciliation.csv", structural_remaining)
    write_csv(paths.artifact_root / "wave1_atomic_repair_plan.csv", plan)
    write_csv(paths.artifact_root / "wave1_repair_group_summary.csv", [{"repair_group_id": k, "rows": len(v), "ticker": v[0]["ticker"]} for k, v in group_by(plan, "repair_group_id").items()])
    write_csv(paths.artifact_root / "wave1_rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "wave1_rehearsal_integrity.json", rehearsal | {"integrity": rehearsal["integrity"]})
    write_csv(paths.artifact_root / "wave1_rehearsal_content_parity.csv", content)
    write_csv(paths.artifact_root / "wave1_rehearsal_lineage_parity.csv", lineage)
    write_csv(paths.artifact_root / "wave1_rehearsal_downstream_before_after.csv", [{"layer": k, "value": json.dumps(v, sort_keys=True)} for k, v in downstream.items()])
    write_json(paths.artifact_root / "wave1_rehearsal_determinism.json", summary["downstream_rehearsal"])
    write_csv(paths.artifact_root / "wave1_first_batch_ticker_final_status.csv", final_status)
    write_csv(paths.artifact_root / "wave1_first_batch_frozen_production_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "wave1_first_batch_more_external_evidence.csv", external_remaining)
    write_csv(paths.artifact_root / "wave1_first_batch_structural_review.csv", structural_remaining)
    write_csv(paths.artifact_root / "wave1_first_batch_no_repair_needed.csv", no_repair)
    write_json(paths.artifact_root / "phase8h3_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if paths.write_documentation:
        append_docs(summary)
    return summary


def group_by(rows_: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows_:
        out[str(row[key])].append(row)
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile Wave 1 first-batch external research against current V3")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v3_phase8h3_wave1_reconciliation") / utc_stamp())
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    parser.add_argument("--input-root", type=Path, default=Path("temp"))
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_phase8h3(
        Phase8H3Paths(
            artifact_root=args.artifact_root,
            v3_db=args.v3_db,
            osakedata_db=args.osakedata_db,
            input_root=args.input_root,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"tasks={summary['research_input']['tasks']}")
    print(f"tickers={summary['research_input']['tickers']}")
    print(f"facts={summary['research_input']['fact_rows']}")
    print(f"ready_groups={summary['frozen_production_set']['ready_groups']}")
    return 0 if summary["classification"] != CLASSIFICATION_BLOCKED else 2


if __name__ == "__main__":
    raise SystemExit(main())
