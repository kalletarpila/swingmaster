from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import open_ro, rows, write_csv, write_json


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A5_PUBLISH_REPAIR_SET_READY"
CLASSIFICATION_MANUAL = "FUNDAMENTALS_V3_PHASE8A5_PUBLISH_MANUAL_REVIEW_REMAINS"
NEXT_ACTION_READY = "PHASE 8A6 - BOUNDED PUBLISH-DATE / PERIOD-END PRODUCTION APPLY & PROVING"
NEXT_ACTION_MANUAL = "USER MANUAL REVIEW - ONLY REMAINING UNRESOLVED PUBLISH/PERIOD-END ROWS"

REQUIRED_COLUMNS = {
    "Ticker",
    "Fiscal Year",
    "Fiscal Q",
    "Publish Date",
    "Period End",
    "Candidate Publish Date",
    "Status",
    "Verified Fiscal Year",
    "Verified Fiscal Q",
    "Verified Period End",
    "Evidence Basis",
    "Source 1",
}


@dataclass(frozen=True)
class Phase8A5Paths:
    verified_csv: Path
    artifact_root: Path
    v3_db: Path
    rawcandle_db: Path


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_verified_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"Verified publish CSV missing columns: {sorted(missing)}")
        return list(reader)


def trading_day_distance(conn: sqlite3.Connection, market: str, date_a: str, date_b: str) -> int:
    if date_a == date_b:
        return 0
    lo, hi = sorted([date_a, date_b])
    return int(
        conn.execute(
            "SELECT COUNT(DISTINCT pvm) FROM osakedata WHERE market=? AND pvm>? AND pvm<=?",
            (market, lo, hi),
        ).fetchone()[0]
    )


def later_date(date_a: str, date_b: str) -> str:
    return max(date_a, date_b)


def publish_evidence_status(row: dict[str, str]) -> str:
    status = row["Status"].strip().upper()
    if status == "MATCH":
        return "MATCH_CONFIRMED"
    if status not in {"DIFFERENT"}:
        return "PUBLISH_DATE_EVIDENCE_INSUFFICIENT"
    if not row.get("Candidate Publish Date") or not row.get("Source 1"):
        return "PUBLISH_DATE_EVIDENCE_INSUFFICIENT"
    basis = row.get("Evidence Basis", "").upper()
    if "RESULT" in basis or "EARNINGS" in basis or "ITEM 2.02" in basis:
        return "PUBLISH_DATE_REPAIR_CONFIRMED"
    return "PUBLISH_DATE_SEMANTICS_UNCERTAIN"


def period_end_disposition(current_period_end: str, verified_period_end: str, distance: int) -> str:
    if current_period_end == verified_period_end:
        return "EXACT_MATCH"
    if distance > 7:
        return "OUTSIDE_TOLERANCE_MANUAL_REVIEW"
    if current_period_end == later_date(current_period_end, verified_period_end):
        return "WITHIN_TOLERANCE_NO_CHANGE"
    return "WITHIN_TOLERANCE_UPDATE_TO_LATER"


def production_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "canonical": int(conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0]),
        "ttm": int(conn.execute("SELECT COUNT(*) FROM v3_ttm").fetchone()[0]),
        "valuation": int(conn.execute("SELECT COUNT(*) FROM v3_valuation").fetchone()[0]),
        "score": int(conn.execute("SELECT COUNT(*) FROM v3_score").fetchone()[0]),
        "lifecycle": int(conn.execute("SELECT COUNT(*) FROM v3_lifecycle").fetchone()[0]),
    }


def run_phase8a5(paths: Phase8A5Paths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    verified = read_verified_csv(paths.verified_csv)
    started = datetime.now(timezone.utc)
    if len(verified) != 111:
        raise RuntimeError(f"Expected 111 verified publish rows, got {len(verified)}")

    with open_ro(paths.v3_db) as conn:
        before = production_counts(conn)
        conn.execute("ATTACH DATABASE ? AS rawcandle", (f"file:{paths.rawcandle_db}?mode=ro",))
        try:
            current_recon, evidence, period_rows, publish_repairs, period_repairs, downstream, valuation_plan = analyze_rows(conn, verified)
        finally:
            try:
                conn.execute("DETACH DATABASE rawcandle")
            except sqlite3.OperationalError:
                pass
        after = production_counts(conn)

    write_csv(paths.artifact_root / "verified_publish_input_reconciliation.csv", input_reconciliation(verified))
    write_csv(paths.artifact_root / "verified_publish_current_v3_reconciliation.csv", current_recon)
    write_csv(paths.artifact_root / "publish_evidence_classification.csv", evidence)
    write_csv(paths.artifact_root / "period_end_trading_day_tolerance_analysis.csv", period_rows)
    write_csv(paths.artifact_root / "period_end_outside_tolerance_cases.csv", [r for r in period_rows if r["period_end_disposition"] == "OUTSIDE_TOLERANCE_MANUAL_REVIEW"])
    write_csv(paths.artifact_root / "publish_date_frozen_repair_set.csv", publish_repairs)
    write_csv(paths.artifact_root / "period_end_frozen_repair_set.csv", period_repairs)
    write_csv(paths.artifact_root / "publish_downstream_impact_plan.csv", downstream)
    write_csv(paths.artifact_root / "valuation_date_change_plan.csv", valuation_plan)

    summary = build_summary(started, verified, current_recon, evidence, period_rows, publish_repairs, period_repairs, downstream, valuation_plan, before, after)
    write_json(paths.artifact_root / "phase8a5_summary.json", summary)
    (paths.artifact_root / "phase8a5_next_action.md").write_text(
        f"Classification: `{summary['classification']}`\n\nNext action: `{summary['next_action']}`\n",
        encoding="utf-8",
    )
    return summary


def input_reconciliation(verified: list[dict[str, str]]) -> list[dict[str, Any]]:
    statuses = Counter(r["Status"].strip().upper() for r in verified)
    return [
        {"metric": "rows", "expected": 111, "actual": len(verified)},
        {"metric": "unique_ticker_fy_fq", "expected": 111, "actual": len({(r["Ticker"], r["Fiscal Year"], r["Fiscal Q"]) for r in verified})},
        {"metric": "MATCH", "expected": 16, "actual": statuses.get("MATCH", 0)},
        {"metric": "DIFFERENT", "expected": 95, "actual": statuses.get("DIFFERENT", 0)},
        {"metric": "UNCERTAIN_NOT_FOUND", "expected": 0, "actual": statuses.get("UNCERTAIN", 0) + statuses.get("NOT_FOUND", 0)},
        {"metric": "candidate_publish_date_complete", "expected": 111, "actual": sum(1 for r in verified if r.get("Candidate Publish Date"))},
        {"metric": "source_1_complete", "expected": 111, "actual": sum(1 for r in verified if r.get("Source 1"))},
        {"metric": "verified_fy_complete", "expected": 111, "actual": sum(1 for r in verified if r.get("Verified Fiscal Year"))},
        {"metric": "verified_fq_complete", "expected": 111, "actual": sum(1 for r in verified if r.get("Verified Fiscal Q"))},
        {"metric": "verified_period_end_complete", "expected": 111, "actual": sum(1 for r in verified if r.get("Verified Period End"))},
    ]


def analyze_rows(conn: sqlite3.Connection, verified: list[dict[str, str]]) -> tuple[list[dict[str, Any]], ...]:
    current_recon: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    period_rows: list[dict[str, Any]] = []
    publish_repairs: list[dict[str, Any]] = []
    period_repairs: list[dict[str, Any]] = []
    downstream_rows: list[dict[str, Any]] = []
    valuation_plan: list[dict[str, Any]] = []

    for idx, row in enumerate(verified, 1):
        row = dict(row)
        row["_idx"] = str(idx)
        issue_id = f"P8-PUB-{idx:03d}"
        current = rows(
            conn,
            """
            SELECT q.quarter_id,q.company_id,c.market,c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date
            FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
            WHERE c.ticker=? AND q.fiscal_year=? AND q.fiscal_quarter=?
            """,
            (row["Ticker"], int(row["Fiscal Year"]), row["Fiscal Q"]),
        )
        identity_ok = len(current) == 1
        current_state_ok = False
        if identity_ok:
            q = current[0]
            current_state_ok = q["publish_date"] == row["Publish Date"] and q["period_end_date"] == row["Period End"]
        else:
            q = {"quarter_id": "", "company_id": "", "market": "", "ticker": row["Ticker"], "fiscal_year": row["Fiscal Year"], "fiscal_quarter": row["Fiscal Q"], "period_end_date": "", "publish_date": ""}

        fy_match = str(row["Fiscal Year"]) == str(row["Verified Fiscal Year"])
        fq_match = row["Fiscal Q"] == row["Verified Fiscal Q"]
        evidence_status = publish_evidence_status(row)
        if not fy_match or not fq_match:
            evidence_status = "IDENTITY_CONFLICT_MANUAL_REVIEW"
        elif not current_state_ok:
            evidence_status = "CURRENT_STATE_CHANGED_SINCE_EXPORT"

        distance = trading_day_distance(conn, q["market"] or "usa", row["Period End"], row["Verified Period End"])
        period_disp = period_end_disposition(row["Period End"], row["Verified Period End"], distance)
        selected_period_end = later_date(row["Period End"], row["Verified Period End"]) if period_disp.startswith("WITHIN_TOLERANCE") else row["Period End"]
        publish_disp = {
            "MATCH_CONFIRMED": "MATCH_NO_CHANGE",
            "PUBLISH_DATE_REPAIR_CONFIRMED": "REPAIR_PUBLISH_DATE",
            "CURRENT_STATE_CHANGED_SINCE_EXPORT": "CURRENT_STATE_CHANGED",
        }.get(evidence_status, "MANUAL_REVIEW")

        impact = downstream_impact(conn, int(q["quarter_id"]) if q["quarter_id"] else None, int(q["company_id"]) if q["company_id"] else None)
        current_recon.append(
            {
                "issue_id": issue_id,
                "ticker": row["Ticker"],
                "company_id": q["company_id"],
                "quarter_id": q["quarter_id"],
                "fy_match": int(fy_match),
                "fq_match": int(fq_match),
                "current_identity_match": int(identity_ok),
                "current_state_match": int(current_state_ok),
                "current_publish_date": q["publish_date"],
                "input_publish_date": row["Publish Date"],
                "current_period_end": q["period_end_date"],
                "input_period_end": row["Period End"],
                "classification": evidence_status,
            }
        )
        evidence_rows.append(
            {
                "issue_id": issue_id,
                "ticker": row["Ticker"],
                "fiscal_year": row["Fiscal Year"],
                "fiscal_quarter": row["Fiscal Q"],
                "status": row["Status"],
                "candidate_publish_date": row["Candidate Publish Date"],
                "evidence_basis": row["Evidence Basis"],
                "source_1": row["Source 1"],
                "source_2": row.get("Source 2", ""),
                "review_notes": row.get("Review Notes", ""),
                "classification": evidence_status,
                "publish_disposition": publish_disp,
            }
        )
        period_rows.append(
            {
                "issue_id": issue_id,
                "ticker": row["Ticker"],
                "fiscal_year": row["Fiscal Year"],
                "fiscal_quarter": row["Fiscal Q"],
                "current_period_end": row["Period End"],
                "verified_period_end": row["Verified Period End"],
                "trading_day_distance": distance,
                "selected_period_end": selected_period_end,
                "period_end_disposition": period_disp,
            }
        )
        downstream_rows.append({"issue_id": issue_id, "ticker": row["Ticker"], "publish_disposition": publish_disp, "period_end_disposition": period_disp, **impact})

        if publish_disp == "REPAIR_PUBLISH_DATE":
            publish_repairs.append(
                {
                    "repair_id": f"P8A5-PUB-REPAIR-{len(publish_repairs)+1:03d}",
                    "issue_id": issue_id,
                    "company_id": q["company_id"],
                    "ticker": row["Ticker"],
                    "fiscal_year": row["Fiscal Year"],
                    "fiscal_quarter": row["Fiscal Q"],
                    "current_period_end": row["Period End"],
                    "selected_period_end": selected_period_end,
                    "current_publish_date": row["Publish Date"],
                    "new_publish_date": row["Candidate Publish Date"],
                    "evidence_type": row["Evidence Basis"],
                    "source_1": row["Source 1"],
                    "source_2": row.get("Source 2", ""),
                    "confidence": "HIGH",
                    "publish_disposition": publish_disp,
                    "period_end_disposition": period_disp,
                    "expected_downstream_scope": impact["expected_downstream_scope"],
                }
            )
            valuation_plan.append(valuation_change_plan(conn, q, row))
        if period_disp == "WITHIN_TOLERANCE_UPDATE_TO_LATER" and evidence_status not in {"IDENTITY_CONFLICT_MANUAL_REVIEW", "CURRENT_STATE_CHANGED_SINCE_EXPORT"}:
            period_repairs.append(
                {
                    "repair_id": f"P8A5-PERIOD-REPAIR-{len(period_repairs)+1:03d}",
                    "issue_id": issue_id,
                    "company_id": q["company_id"],
                    "ticker": row["Ticker"],
                    "fiscal_year": row["Fiscal Year"],
                    "fiscal_quarter": row["Fiscal Q"],
                    "current_period_end": row["Period End"],
                    "new_period_end": selected_period_end,
                    "trading_day_distance": distance,
                    "evidence_type": row["Evidence Basis"],
                    "source_1": row["Source 1"],
                    "source_2": row.get("Source 2", ""),
                    "confidence": "HIGH",
                }
            )
    return current_recon, evidence_rows, period_rows, publish_repairs, period_repairs, downstream_rows, valuation_plan


def downstream_impact(conn: sqlite3.Connection, quarter_id: int | None, company_id: int | None) -> dict[str, Any]:
    if quarter_id is None or company_id is None:
        return {"ttm_rows": 0, "score_rows": 0, "lifecycle_rows": 0, "valuation_rows": 0, "expected_downstream_scope": "unresolved_identity"}
    ttm_ids = [str(r["ttm_id"]) for r in rows(conn, "SELECT ttm_id FROM v3_ttm WHERE q1_quarter_id=? OR q2_quarter_id=? OR q3_quarter_id=? OR q4_quarter_id=? OR endpoint_quarter_id=?", (quarter_id, quarter_id, quarter_id, quarter_id, quarter_id))]
    in_sql = ",".join(ttm_ids) or "NULL"
    score = rows(conn, f"SELECT score_id FROM v3_score WHERE endpoint_ttm_id IN ({in_sql})")
    lifecycle = rows(conn, f"SELECT lifecycle_id FROM v3_lifecycle WHERE endpoint_ttm_id IN ({in_sql})")
    valuation = rows(conn, f"SELECT valuation_id FROM v3_valuation WHERE endpoint_ttm_id IN ({in_sql})")
    return {
        "ttm_rows": len(ttm_ids),
        "score_rows": len(score),
        "lifecycle_rows": len(lifecycle),
        "valuation_rows": len(valuation),
        "expected_downstream_scope": f"ttm={len(ttm_ids)};score={len(score)};lifecycle={len(lifecycle)};valuation={len(valuation)}",
    }


def valuation_change_plan(conn: sqlite3.Connection, q: dict[str, Any], row: dict[str, str]) -> dict[str, Any]:
    vals = rows(conn, "SELECT valuation_id,valuation_date,valuation_close_price FROM v3_valuation WHERE endpoint_quarter_id=? ORDER BY valuation_id", (q["quarter_id"],))
    new_trade = rows(
        conn,
        """
        SELECT pvm,close FROM rawcandle.osakedata
        WHERE market=? AND osake=? AND pvm>? AND close IS NOT NULL
        ORDER BY pvm LIMIT 1
        """,
        (q["market"], q["ticker"], row["Candidate Publish Date"]),
    )
    new_date = new_trade[0]["pvm"] if new_trade else ""
    new_close = new_trade[0]["close"] if new_trade else ""
    return {
        "issue_id": f"P8-PUB-{int(row.get('_idx', 0)):03d}" if row.get("_idx") else "",
        "ticker": q["ticker"],
        "quarter_id": q["quarter_id"],
        "current_publish_date": row["Publish Date"],
        "new_publish_date": row["Candidate Publish Date"],
        "old_valuation_dates": "|".join(str(v["valuation_date"]) for v in vals),
        "new_valuation_date": new_date,
        "old_closes": "|".join(str(v["valuation_close_price"]) for v in vals),
        "new_close": new_close,
        "valuation_snapshot_changes": int(any(v["valuation_date"] != new_date for v in vals)) if new_date else 0,
        "valuation_rows": len(vals),
    }


def build_summary(
    started: datetime,
    verified: list[dict[str, str]],
    current_recon: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    period_rows: list[dict[str, Any]],
    publish_repairs: list[dict[str, Any]],
    period_repairs: list[dict[str, Any]],
    downstream: list[dict[str, Any]],
    valuation_plan: list[dict[str, Any]],
    before: dict[str, int],
    after: dict[str, int],
) -> dict[str, Any]:
    status_counts = Counter(r["Status"].strip().upper() for r in verified)
    evidence_counts = Counter(r["classification"] for r in evidence)
    period_counts = Counter(r["period_end_disposition"] for r in period_rows)
    publish_manual_ids = {r["issue_id"] for r in evidence if r["publish_disposition"] == "MANUAL_REVIEW"}
    period_manual_ids = {r["issue_id"] for r in period_rows if r["period_end_disposition"] == "OUTSIDE_TOLERANCE_MANUAL_REVIEW"}
    unresolved_ids = publish_manual_ids | period_manual_ids
    valuation_rows = sum(int(r["valuation_rows"]) for r in valuation_plan)
    valuation_changes = sum(1 for r in valuation_plan if int(r["valuation_snapshot_changes"]) == 1)
    classification = CLASSIFICATION_READY if not unresolved_ids else CLASSIFICATION_MANUAL
    return {
        "classification": classification,
        "next_action": NEXT_ACTION_READY if classification == CLASSIFICATION_READY else NEXT_ACTION_MANUAL,
        "started_at_utc": started.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "completed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "rows": len(verified),
        "unique_quarters": len({(r["Ticker"], r["Fiscal Year"], r["Fiscal Q"]) for r in verified}),
        "status_counts": dict(status_counts),
        "source_1_complete": sum(1 for r in verified if r.get("Source 1")),
        "source_2_complete": sum(1 for r in verified if r.get("Source 2")),
        "fy_matches": sum(1 for r in current_recon if int(r["fy_match"]) == 1),
        "fy_mismatches": sum(1 for r in current_recon if int(r["fy_match"]) == 0),
        "fq_matches": sum(1 for r in current_recon if int(r["fq_match"]) == 1),
        "fq_mismatches": sum(1 for r in current_recon if int(r["fq_match"]) == 0),
        "current_production_identity_mismatches": sum(1 for r in current_recon if int(r["current_identity_match"]) == 0 or int(r["current_state_match"]) == 0),
        "publish_classification_counts": dict(evidence_counts),
        "period_end_disposition_counts": dict(period_counts),
        "period_end_exact_matches": period_counts.get("EXACT_MATCH", 0),
        "period_end_differences": len(period_rows) - period_counts.get("EXACT_MATCH", 0),
        "period_end_within_tolerance": period_counts.get("WITHIN_TOLERANCE_NO_CHANGE", 0) + period_counts.get("WITHIN_TOLERANCE_UPDATE_TO_LATER", 0),
        "period_end_outside_tolerance": period_counts.get("OUTSIDE_TOLERANCE_MANUAL_REVIEW", 0),
        "current_v3_already_later": period_counts.get("WITHIN_TOLERANCE_NO_CHANGE", 0),
        "verified_date_later": period_counts.get("WITHIN_TOLERANCE_UPDATE_TO_LATER", 0),
        "frozen_publish_repair_rows": len(publish_repairs),
        "frozen_period_end_repair_rows": len(period_repairs),
        "publish_manual_review_rows": len(publish_manual_ids),
        "period_end_manual_review_rows": len(period_manual_ids),
        "unresolved_rows": len(unresolved_ids),
        "ttm_rows_potentially_affected": sum(int(r["ttm_rows"]) for r in downstream if r["publish_disposition"] == "REPAIR_PUBLISH_DATE"),
        "score_rows_potentially_affected": sum(int(r["score_rows"]) for r in downstream if r["publish_disposition"] == "REPAIR_PUBLISH_DATE"),
        "lifecycle_rows_potentially_affected": sum(int(r["lifecycle_rows"]) for r in downstream if r["publish_disposition"] == "REPAIR_PUBLISH_DATE"),
        "valuation_rows_affected": valuation_rows,
        "valuation_dates_that_would_change": valuation_changes,
        "period_end_only_downstream_recompute_required": False,
        "production_counts_before": before,
        "production_counts_after": after,
        "production_writes": 0 if before == after else "COUNT_DRIFT",
        "rawcandle_writes": 0,
        "period_end_policy_persisted": True,
        "later_date_rule_persisted": True,
        "period_end_remains_metadata": True,
    }
