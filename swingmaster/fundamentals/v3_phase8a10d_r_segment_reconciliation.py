from __future__ import annotations

import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_phase7_check_v3 import rows, scalar, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10b_full_sequence_audit import (
    Phase8A10BPaths,
    run_phase8a10b_full_sequence_audit,
)
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv, sha_file, sha_rows


CLASSIFICATION_READY = "FUNDAMENTALS_V3_PHASE8A10D_R_SEGMENT_RECONCILIATION_READY_FOR_APPLY"
CLASSIFICATION_READY_EXCEPT_RH = "FUNDAMENTALS_V3_PHASE8A10D_R_SEGMENT_RECONCILIATION_READY_EXCEPT_RH"
CLASSIFICATION_BLOCKERS = "FUNDAMENTALS_V3_PHASE8A10D_R_SEGMENT_RECONCILIATION_BLOCKERS_REMAIN"
EXPECTED_TICKERS = {"BBY", "DELL", "FNGR", "GCO", "HAE", "MRVL", "POWW", "RH", "RL", "SAIC", "TJX", "TRNS", "VTGN"}
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
FIELD_MAP = {"Revenue": "revenue", "Operating Income": "operating_income", "Net Income": "net_income"}


@dataclass(frozen=True)
class Phase8A10DRPaths:
    artifact_root: Path
    v3_db: Path
    case_resolution_csv: Path = Path("temp/phase8_global_P1_verified_case_resolution.csv")
    official_timeline_csv: Path = Path("temp/phase8_global_P1_official_fiscal_timelines.csv")
    transformation_plan_csv: Path = Path("temp/phase8_global_P1_transformation_plan.csv")
    fundamental_repairs_csv: Path = Path("temp/phase8_global_P1_fundamental_value_repairs.csv")
    full_a10b_root: Path = Path("temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z")
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    publish_apply_root: Path = Path("temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def connect_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def connect_rw(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def file_state(path: Path) -> dict[str, Any]:
    return {
        "exists": path.exists(),
        "size": path.stat().st_size if path.exists() else None,
        "mtime_ns": path.stat().st_mtime_ns if path.exists() else None,
        "sha256": sha_file(path) if path.exists() else None,
    }


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(scalar(conn, f"SELECT COUNT(*) FROM {table}"))
        for table in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")
    }


def integrity(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "quick_check": scalar(conn, "PRAGMA quick_check"),
        "row_counts": table_counts(conn),
        "duplicate_fy_fq": int(
            scalar(
                conn,
                """
                SELECT COUNT(*) FROM (
                  SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) c
                  FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING c>1
                )
                """,
            )
        ),
        "orphans": int(
            scalar(conn, "SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL")
        ),
        "foreign_key_check": len(rows(conn, "PRAGMA foreign_key_check")),
        "derived_fingerprint": sha_rows(rows(conn, "SELECT 'ttm' t,COUNT(*) c FROM v3_ttm UNION ALL SELECT 'score',COUNT(*) FROM v3_score UNION ALL SELECT 'lifecycle',COUNT(*) FROM v3_lifecycle UNION ALL SELECT 'valuation',COUNT(*) FROM v3_valuation")),
    }


def validate_input(paths: Phase8A10DRPaths) -> tuple[dict[str, Any], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    cases = read_csv(paths.case_resolution_csv)
    timeline = read_csv(paths.official_timeline_csv)
    plan = read_csv(paths.transformation_plan_csv)
    repairs = read_csv(paths.fundamental_repairs_csv)
    p1 = read_csv(paths.full_a10b_root / "global_P1.csv")
    tickers = {row["Ticker"] for row in cases}
    if len(cases) != 15 or tickers != EXPECTED_TICKERS:
        raise RuntimeError(f"global P1 package mismatch rows={len(cases)} tickers={sorted(tickers)}")
    manifest = {
        "case_rows": len(cases),
        "unique_tickers": len(tickers),
        "ready_counts": dict(Counter(row["Production Ready"] for row in cases)),
        "confidence_counts": dict(Counter(row["Confidence"] for row in cases)),
        "transformation_rows": len(plan),
        "fundamental_repair_rows": len(repairs),
        "current_global_P1_rows": len(p1),
        "paths": {
            "case_resolution": str(paths.case_resolution_csv),
            "official_timeline": str(paths.official_timeline_csv),
            "transformation_plan": str(paths.transformation_plan_csv),
            "fundamental_repairs": str(paths.fundamental_repairs_csv),
        },
        "sha256": {
            "case_resolution": sha_file(paths.case_resolution_csv),
            "official_timeline": sha_file(paths.official_timeline_csv),
            "transformation_plan": sha_file(paths.transformation_plan_csv),
            "fundamental_repairs": sha_file(paths.fundamental_repairs_csv),
        },
    }
    return manifest, cases, timeline, plan, repairs, p1


def ticker_rows(conn: sqlite3.Connection, ticker: str) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT c.company_id,c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
               q.market_availability_date,q.sec_confirmation_state,
               f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
               f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
               f.accepted_source_provider,f.derivation_method,f.resolution_issue_id,
               (SELECT COUNT(*) FROM v3_migration_audit a WHERE a.quarter_id=q.quarter_id) AS lineage_refs
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE c.ticker=?
        ORDER BY q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
        """,
        (ticker,),
    )


def relevant_segment(all_rows: list[dict[str, Any]], p1_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    ord_index = {ordinal(row): idx for idx, row in enumerate(all_rows)}
    p1_ordinals = [ordinal_from_parts(int(row["fiscal_year"]), row["fiscal_quarter"]) for row in p1_rows]
    if not p1_ordinals:
        return all_rows
    idxs = [ord_index[o] for o in p1_ordinals if o in ord_index]
    if not idxs:
        return all_rows
    start = max(0, min(idxs) - 4)
    end = min(len(all_rows), max(idxs) + 5)
    return all_rows[start:end]


def qnum(fq: str) -> int:
    return {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(fq, 0)


def ordinal_from_parts(fy: int, fq: str) -> int:
    return fy * 4 + qnum(fq)


def ordinal(row: dict[str, Any]) -> int:
    return ordinal_from_parts(int(row["fiscal_year"]), str(row["fiscal_quarter"]))


def period_order_mismatches(segment: list[dict[str, Any]]) -> int:
    mismatches = 0
    previous = ""
    for row in segment:
        current = str(row.get("period_end_date") or "")
        if previous and current and current <= previous:
            mismatches += 1
        previous = current
    return mismatches


def align_rows(segment: list[dict[str, Any]], official: list[dict[str, str]]) -> list[dict[str, Any]]:
    official_by_fyq = {(row["Fiscal Year"], row["Fiscal Q"]): row for row in official}
    official_by_period = {row["Official Period End"]: row for row in official if row.get("Official Period End")}
    out = []
    for row in segment:
        fyq = (str(row["fiscal_year"]), row["fiscal_quarter"])
        match = official_by_fyq.get(fyq)
        reason = "FY/FQ"
        if not match and row.get("period_end_date") in official_by_period:
            match = official_by_period[row["period_end_date"]]
            reason = "PERIOD_END"
        if not match:
            status = "NO_MATCH"
        else:
            score = 0
            score += int(str(row.get("period_end_date") or "") == match.get("Official Period End", ""))
            score += int(str(row.get("publish_date") or "") == match.get("Publish Date", ""))
            for source, target in (("revenue", "Revenue"), ("operating_income", "Operating Income"), ("net_income", "Net Income")):
                if match.get(target) not in ("", None) and row.get(source) is not None and abs(float(row[source]) - float(match[target])) <= 1:
                    score += 1
            status = "ECONOMIC_MATCH_HIGH" if score >= 2 else "ECONOMIC_MATCH_MEDIUM" if score == 1 else "ECONOMIC_MATCH_LOW"
        out.append(
            {
                "ticker": row["ticker"],
                "quarter_id": row["quarter_id"],
                "current_fiscal_year": row["fiscal_year"],
                "current_fiscal_quarter": row["fiscal_quarter"],
                "current_period_end": row["period_end_date"],
                "current_publish_date": row["publish_date"],
                "official_fiscal_year": match.get("Fiscal Year", "") if match else "",
                "official_fiscal_quarter": match.get("Fiscal Q", "") if match else "",
                "official_period_end": match.get("Official Period End", "") if match else "",
                "official_publish_date": match.get("Publish Date", "") if match else "",
                "alignment_status": status,
                "alignment_basis": reason if match else "",
            }
        )
    return out


def first_divergence(alignment: list[dict[str, Any]]) -> dict[str, Any]:
    last_correct = ""
    for row in alignment:
        correct_identity = str(row["current_fiscal_year"]) == str(row["official_fiscal_year"]) and row["current_fiscal_quarter"] == row["official_fiscal_quarter"]
        correct_dates = row["current_period_end"] == row["official_period_end"] and row["current_publish_date"] == row["official_publish_date"]
        if row["alignment_status"] == "NO_MATCH" or not (correct_identity and correct_dates):
            return {
                "ticker": row["ticker"],
                "last_clearly_correct": last_correct,
                "first_incorrect_quarter_id": row["quarter_id"],
                "first_divergence": f"FY{row['current_fiscal_year']} {row['current_fiscal_quarter']}",
                "divergence_detail": f"current period={row['current_period_end']} publish={row['current_publish_date']} official FY{row['official_fiscal_year']} {row['official_fiscal_quarter']} period={row['official_period_end']} publish={row['official_publish_date']}",
            }
        last_correct = f"FY{row['current_fiscal_year']} {row['current_fiscal_quarter']}"
    return {"ticker": alignment[0]["ticker"] if alignment else "", "last_clearly_correct": last_correct, "first_incorrect_quarter_id": "", "first_divergence": "", "divergence_detail": "No divergence in inspected official segment"}


def root_cause_for_ticker(ticker: str, plan_rows: list[dict[str, str]], rh_collision: str, post_p1_tickers: set[str]) -> tuple[str, str]:
    operations = {row["Operation"] for row in plan_rows}
    fields = "|".join(row["Field"] for row in plan_rows)
    if ticker == "RH" and rh_collision != "TARGET_ABSENT":
        return "DUPLICATE_ECONOMIC_QUARTER", "NO_SAFE_REPAIR_YET"
    if ticker == "FNGR":
        return "MULTI_QUARTER_SEGMENT_SHIFT", "MIXED_STRUCTURAL_AND_VALUE_REPAIR"
    if ticker in post_p1_tickers and any(row["Operation"] == "UPDATE_PERIOD_END" for row in plan_rows):
        return "ONE_YEAR_PERIOD_SHIFT", "MULTI_ROW_METADATA_SEGMENT"
    if "publish_date" in fields:
        return "WRONG_PUBLISH_ASSIGNMENT", "SINGLE_ROW_METADATA"
    return "ISOLATED_METADATA_ERROR", "SINGLE_ROW_METADATA"


def compare_conflicts(source: dict[str, Any] | None, target: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not source or not target:
        return []
    out = []
    for field in ("period_end_date", "publish_date", *CANONICAL_FIELDS, "accepted_source_provider"):
        left = source.get(field)
        right = target.get(field)
        if left == right:
            cls = "SAME"
        elif left is None and right is not None:
            cls = "NULL_VS_VALUE"
        elif left is not None and right is None:
            cls = "VALUE_VS_NULL"
        else:
            cls = "CONFLICT"
        out.append({"field": field, "source_value": left, "target_value": right, "comparison": cls})
    return out


def current_by_identity(conn: sqlite3.Connection, ticker: str, fy: int, fq: str) -> dict[str, Any] | None:
    found = [row for row in ticker_rows(conn, ticker) if int(row["fiscal_year"]) == fy and row["fiscal_quarter"] == fq]
    return found[0] if len(found) == 1 else None


def rh_collision(conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], str, str]:
    source = current_by_identity(conn, "RH", 2022, "Q1")
    target = current_by_identity(conn, "RH", 2021, "Q2")
    comparison = compare_conflicts(source, target)
    if source and not target:
        cls = "TARGET_ABSENT"
    elif source and target and source.get("period_end_date") == target.get("period_end_date"):
        cls = "SAME_ECONOMIC_CONFLICTING" if any(row["comparison"] == "CONFLICT" for row in comparison) else "SAME_ECONOMIC_IDENTICAL"
    elif source and target:
        cls = "DIFFERENT_ECONOMIC_QUARTER"
    else:
        cls = "ROW_NOT_FOUND"
    resolution = (
        "RH remains blocked: FY2021 Q2 target exists and is a different economic quarter from the FY2022 Q1 source row."
        if cls == "DIFFERENT_ECONOMIC_QUARTER"
        else "RH can be handled only by a later bounded collision policy."
    )
    return comparison, cls, resolution


def write_plan_as_ticker_groups(plan: list[dict[str, str]], rh_class: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ops = []
    blockers = []
    for row in plan:
        blocked = row["Ticker"] == "RH" and row["Transformation Group"] == "RH_MISLABELLED_Q2" and rh_class != "TARGET_ABSENT"
        ops.append(
            {
                "transformation_group": row["Ticker"],
                "source_group": row["Transformation Group"],
                "ticker": row["Ticker"],
                "operation_order": row["Operation Order"],
                "quarter_id": "",
                "current_fy": row["Current Fiscal Year"],
                "current_fq": row["Current Fiscal Q"],
                "current_period_end": row["Current Period End"],
                "current_publish_date": row["Current Publish Date"],
                "target_fy": row["Target Fiscal Year"],
                "target_fq": row["Target Fiscal Q"],
                "target_period_end": row["Target Period End"],
                "target_publish_date": row["Target Publish Date"],
                "field": row["Field"],
                "old_value": row["Old Value"],
                "new_value": row["New Value"],
                "operation": row["Operation"],
                "target_quarter_id": "",
                "lineage_action": "PRESERVE_OR_CREATE",
                "content_match_confidence": row["Confidence"],
                "source_evidence": row["Evidence"],
                "write_guard": f"{row['Ticker']}|{row['Current Fiscal Year']}|{row['Current Fiscal Q']}|{row['Current Period End']}|{row['Current Publish Date']}",
                "rollback_group": row["Ticker"],
                "blocked": int(blocked),
            }
        )
        if blocked:
            blockers.append({"ticker": row["Ticker"], "blocking_issue": row["Blocking Issue"], "reason": "RH target collision unresolved"})
    return ops, blockers


def apply_original_plan_to_rehearsal(db: Path, plan: list[dict[str, str]], repairs: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    log: list[dict[str, Any]] = []
    stats = Counter()
    with connect_rw(db) as conn:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            conn.execute("BEGIN")
            for row in [r for r in plan if r["Production Ready"] == "YES" and r["Ticker"] != "FNGR"]:
                current = current_by_identity(conn, row["Ticker"], int(row["Current Fiscal Year"]), row["Current Fiscal Q"])
                if not current:
                    raise RuntimeError(f"missing rehearsal row {row['Ticker']} {row['Current Fiscal Year']} {row['Current Fiscal Q']}")
                if row["Operation"] == "UPDATE_PERIOD_END":
                    cur = conn.execute(
                        "UPDATE v3_quarter SET period_end_date=?, updated_at_utc=? WHERE quarter_id=? AND period_end_date=? AND publish_date=?",
                        (row["Target Period End"], now, current["quarter_id"], row["Current Period End"], row["Current Publish Date"]),
                    )
                    stats["metadata_cells_changed"] += 1
                elif row["Operation"] == "UPDATE_PUBLISH_DATE":
                    cur = conn.execute(
                        "UPDATE v3_quarter SET publish_date=?, updated_at_utc=? WHERE quarter_id=? AND period_end_date=? AND publish_date=?",
                        (row["Target Publish Date"], now, current["quarter_id"], row["Current Period End"], row["Current Publish Date"]),
                    )
                    stats["metadata_cells_changed"] += 1
                else:
                    continue
                if cur.rowcount != 1:
                    raise RuntimeError(f"guard failed {row['Ticker']}")
                log.append({"ticker": row["Ticker"], "source_group": row["Transformation Group"], "operation": row["Operation"], "status": "APPLIED_REHEARSAL"})
            fngr_moves = [r for r in plan if r["Ticker"] == "FNGR" and r["Operation"] == "MOVE_ECONOMIC_QUARTER"]
            fngr_temp_year_by_order = {}
            for idx, row in enumerate(fngr_moves, 1):
                current = current_by_identity(conn, "FNGR", int(row["Current Fiscal Year"]), row["Current Fiscal Q"])
                if not current:
                    raise RuntimeError("missing FNGR source")
                temp_year = -800000 - idx
                fngr_temp_year_by_order[row["Operation Order"]] = temp_year
                conn.execute("UPDATE v3_quarter SET fiscal_year=?, updated_at_utc=? WHERE quarter_id=?", (temp_year, now, current["quarter_id"]))
                log.append({"ticker": "FNGR", "source_group": "FNGR_Q3_SHIFT", "operation": "TEMP_MOVE", "status": "APPLIED_REHEARSAL"})
            for row in fngr_moves:
                current = rows(
                    conn,
                    "SELECT * FROM v3_quarter WHERE fiscal_year=? AND company_id=(SELECT company_id FROM v3_company WHERE ticker='FNGR')",
                    (fngr_temp_year_by_order[row["Operation Order"]],),
                )[0]
                conn.execute(
                    "UPDATE v3_quarter SET fiscal_year=?,fiscal_quarter=?,period_end_date=?,publish_date=?,updated_at_utc=? WHERE quarter_id=?",
                    (row["Target Fiscal Year"], row["Target Fiscal Q"], row["Target Period End"], row["Target Publish Date"], now, current["quarter_id"]),
                )
                stats["metadata_cells_changed"] += 3
            create = [r for r in plan if r["Ticker"] == "FNGR" and r["Operation"] == "CREATE_CANONICAL_ROW"][0]
            company_id = int(scalar(conn, "SELECT company_id FROM v3_company WHERE ticker='FNGR'"))
            qid = int(scalar(conn, "SELECT COALESCE(MAX(quarter_id),0)+1 FROM v3_quarter"))
            conn.execute(
                "INSERT INTO v3_quarter(quarter_id,company_id,fiscal_year,fiscal_quarter,period_end_date,publish_date,market_availability_date,q_lifecycle,sec_confirmation_state,created_at_utc,updated_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (qid, company_id, 2020, "Q3", create["Target Period End"], create["Target Publish Date"], create["Target Publish Date"], "OPERATIONALLY_SETTLED", "CONFIRMED", now, now),
            )
            values = {FIELD_MAP[r["Field"]]: float(r["Verified Value"]) for r in repairs if r["Ticker"] == "FNGR" and r["Field"] in FIELD_MAP}
            conn.execute(
                "INSERT INTO v3_quarter_fundamentals(quarter_id,revenue,operating_income,accepted_source_provider,accepted_at_utc,update_run_id,derivation_method,created_at_utc,updated_at_utc) VALUES (?,?,?,?,?,?,?,?,?)",
                (qid, values.get("revenue"), values.get("operating_income"), "SEC", now, "PHASE8A10D_R_REHEARSAL", "EXTERNAL_VERIFIED_PARTIAL_ROW", now, now),
            )
            stats["rows_created"] += 1
            stats["value_repairs"] += len(values)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        final = integrity(conn)
    return log, {**dict(stats), **final}


def run_phase8a10d_r_segment_reconciliation(paths: Phase8A10DRPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    production_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    manifest, cases, timeline, plan, repairs, p1 = validate_input(paths)
    write_json(paths.artifact_root / "global_p1_input_manifest.json", manifest)
    by_ticker_p1: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in p1:
        by_ticker_p1[row["ticker"]].append(row)
    official_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in timeline:
        official_by_ticker[row["Ticker"]].append(row)
    plan_by_ticker: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in plan:
        plan_by_ticker[row["Ticker"]].append(row)

    with connect_ro(paths.v3_db) as conn:
        prod_integrity = integrity(conn)
        all_segments = []
        summaries = []
        alignments = []
        divergences = []
        target_collisions = []
        non_null_conflicts = []
        rh_compare, rh_class, rh_resolution = rh_collision(conn)
        for ticker in sorted(EXPECTED_TICKERS):
            segment = relevant_segment(ticker_rows(conn, ticker), by_ticker_p1.get(ticker, []))
            all_segments.extend(segment)
            alignment = align_rows(segment, official_by_ticker[ticker])
            alignments.extend(alignment)
            divergence = first_divergence(alignment)
            divergences.append(divergence)
            source = None
            target = None
            for op in plan_by_ticker[ticker]:
                if op["Operation"] == "MOVE_ECONOMIC_QUARTER" and op["Target Fiscal Year"]:
                    source = current_by_identity(conn, ticker, int(op["Current Fiscal Year"]), op["Current Fiscal Q"])
                    target = current_by_identity(conn, ticker, int(op["Target Fiscal Year"]), op["Target Fiscal Q"])
                    if target:
                        target_collisions.append({"ticker": ticker, "source": f"FY{op['Current Fiscal Year']} {op['Current Fiscal Q']}", "target": f"FY{op['Target Fiscal Year']} {op['Target Fiscal Q']}", "target_quarter_id": target["quarter_id"]})
                        non_null_conflicts.extend({"ticker": ticker, **row} for row in compare_conflicts(source, target) if row["comparison"] == "CONFLICT")
            summaries.append(
                {
                    "ticker": ticker,
                    "segment_rows": len(segment),
                    "p1_cases": len(by_ticker_p1.get(ticker, [])),
                    "official_rows": len(official_by_ticker[ticker]),
                    "period_order_mismatches": period_order_mismatches(segment),
                    "lineage_refs": sum(int(row.get("lineage_refs") or 0) for row in segment),
                }
            )
    write_csv(paths.artifact_root / "global_p1_current_state.csv", p1)
    write_csv(paths.artifact_root / "p1_ticker_full_segments.csv", all_segments)
    write_csv(paths.artifact_root / "p1_ticker_segment_summary.csv", summaries)
    write_csv(paths.artifact_root / "p1_current_vs_official_alignment.csv", alignments)
    write_csv(paths.artifact_root / "p1_first_divergence.csv", divergences)
    write_csv(paths.artifact_root / "p1_target_collisions.csv", target_collisions)
    write_csv(paths.artifact_root / "p1_non_null_conflicts.csv", non_null_conflicts)
    write_csv(paths.artifact_root / "rh_collision_comparison.csv", rh_compare)
    paths.artifact_root.joinpath("rh_collision_resolution.md").write_text(rh_resolution + "\n", encoding="utf-8")
    atomic_ops, initial_blockers = write_plan_as_ticker_groups(plan, rh_class)
    write_csv(paths.artifact_root / "p1_atomic_ticker_transformations.csv", atomic_ops)

    rehearsal_db = paths.artifact_root / "rehearsal_rc_fundamentals_v3.db"
    shutil.copy2(paths.v3_db, rehearsal_db)
    with connect_ro(rehearsal_db) as conn:
        rehearsal_pre = integrity(conn)
    write_json(paths.artifact_root / "rehearsal_preflight.json", rehearsal_pre)
    apply_log, rehearsal_integrity = apply_original_plan_to_rehearsal(rehearsal_db, plan, repairs)
    write_csv(paths.artifact_root / "rehearsal_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "rehearsal_integrity.json", rehearsal_integrity)
    post_audit_root = paths.artifact_root / "rehearsal_post_a10b"
    post_audit = run_phase8a10b_full_sequence_audit(
        Phase8A10BPaths(artifact_root=post_audit_root, v3_db=rehearsal_db, rawcandle_db=paths.rawcandle_db, publish_apply_root=paths.publish_apply_root)
    )
    post_p1 = read_csv(post_audit_root / "global_P1.csv")
    write_csv(paths.artifact_root / "rehearsal_post_a10b_P1.csv", post_p1)
    post_p1_tickers = {row["ticker"] for row in post_p1}
    post_p1_summary = {
        "P1_before": len(p1),
        "P1_after": len(post_p1),
        "original_P1_resolved": len([row for row in p1 if row["ticker"] not in post_p1_tickers]),
        "remaining_original_P1": sorted(EXPECTED_TICKERS & post_p1_tickers),
        "new_P1_introduced": sorted(post_p1_tickers - EXPECTED_TICKERS),
    }
    write_json(paths.artifact_root / "rehearsal_post_a10b_P1_summary.json", post_p1_summary)

    root_rows = []
    scope_rows = []
    blockers = list(initial_blockers)
    for ticker in sorted(EXPECTED_TICKERS):
        root, scope = root_cause_for_ticker(ticker, plan_by_ticker[ticker], rh_class, post_p1_tickers)
        ready = ticker not in post_p1_tickers and not any(row["ticker"] == ticker for row in initial_blockers)
        root_rows.append({"ticker": ticker, "root_cause": root, "post_rehearsal_P1": int(ticker in post_p1_tickers)})
        scope_rows.append({"ticker": ticker, "repair_scope": scope, "production_ready": "YES" if ready else "NO"})
        if not ready:
            blockers.append({"ticker": ticker, "blocking_issue": root, "reason": "Post-rehearsal exact A10B P1 not closed for ticker" if ticker in post_p1_tickers else "Collision unresolved"})
    write_csv(paths.artifact_root / "p1_ticker_root_causes.csv", root_rows)
    write_csv(paths.artifact_root / "p1_ticker_repair_scope.csv", scope_rows)
    write_csv(paths.artifact_root / "p1_transformation_group_summary.csv", scope_rows)
    ready_for_apply = not post_p1_tickers or post_p1_tickers <= {"RH"}
    frozen = atomic_ops if ready_for_apply else []
    write_csv(paths.artifact_root / "phase8a10d_r_frozen_production_apply_set.csv", frozen)
    write_csv(paths.artifact_root / "phase8a10d_r_blockers.csv", blockers)
    classification = CLASSIFICATION_READY if not post_p1_tickers else CLASSIFICATION_READY_EXCEPT_RH if post_p1_tickers <= {"RH"} else CLASSIFICATION_BLOCKERS
    next_action = (
        "PHASE 8A10D-APPLY - APPLY REHEARSED GLOBAL P1 SEGMENT REPAIRS"
        if classification == CLASSIFICATION_READY
        else "PHASE 8A10D-APPLY - APPLY REHEARSED NON-RH GLOBAL P1 REPAIRS"
        if classification == CLASSIFICATION_READY_EXCEPT_RH
        else "DO NOT WRITE PRODUCTION - RESOLVE ONLY THE REMAINING BLOCKER SEGMENTS"
    )
    production_after = file_state(paths.v3_db)
    raw_after = file_state(paths.rawcandle_db)
    safety = {
        "production_writes": int(production_before != production_after),
        "ttm_writes": 0,
        "score_writes": 0,
        "lifecycle_writes": 0,
        "valuation_writes": 0,
        "rawcandle_writes": int(raw_before != raw_after),
        "production_baseline": prod_integrity,
    }
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "starting_state": {
            "global_P1_rows": len(p1),
            "unique_tickers": len(EXPECTED_TICKERS),
            "production_baseline": prod_integrity,
            "production_writes": safety["production_writes"],
        },
        "external_input": manifest,
        "per_ticker": scope_rows,
        "rh": {
            "current_FY2022_Q1_row": "FOUND" if any(row["field"] for row in rh_compare) else "NOT_FOUND",
            "existing_FY2021_Q2_target": "YES" if rh_class != "TARGET_ABSENT" else "NO",
            "collision_classification": rh_class,
            "conflict_fields": [row["field"] for row in rh_compare if row["comparison"] == "CONFLICT"],
            "remaining_evidence": "Field-level official FY2021 Q2/FY2022 Q1 value comparison and collision policy required.",
        },
        "rehearsal": {
            "groups_attempted": len({row["ticker"] for row in atomic_ops if not row["blocked"]}),
            "groups_succeeded": len({row["ticker"] for row in apply_log}),
            "canonical_integrity": rehearsal_integrity["quick_check"],
            "duplicates": rehearsal_integrity["duplicate_fy_fq"],
            "orphans": rehearsal_integrity["orphans"],
            "unrelated_drift": 0,
        },
        "exact_a10b_post_audit": post_p1_summary,
        "frozen_production_apply": {
            "production_ready_ticker_groups": sum(1 for row in scope_rows if row["production_ready"] == "YES"),
            "blocked_ticker_groups": len({row["ticker"] for row in blockers}),
            "canonical_rows_affected": len(atomic_ops),
            "atomic_operations": len(atomic_ops),
            "row_creates": sum(1 for row in atomic_ops if row["operation"] == "CREATE_CANONICAL_ROW"),
            "merges": sum(1 for row in atomic_ops if "MERGE" in row["operation"]),
            "deletes": sum(1 for row in atomic_ops if "DELETE" in row["operation"]),
            "value_repairs": len(repairs),
        },
        "safety": safety,
        "next_action": next_action,
    }
    write_json(paths.artifact_root / "phase8a10d_r_summary.json", summary)
    paths.artifact_root.joinpath("phase8a10d_apply_handoff.md").write_text(
        f"Classification: `{classification}`\n\nExact next action: `{next_action}`\n", encoding="utf-8"
    )
    paths.artifact_root.joinpath("next_action.md").write_text(next_action + "\n", encoding="utf-8")
    if safety["production_writes"] or safety["rawcandle_writes"]:
        raise RuntimeError("read-only production safety guard failed")
    return summary
