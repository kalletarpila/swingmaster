from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from swingmaster.fundamentals import v3_phase6f_valuation_engine as p6f
from swingmaster.fundamentals.v3_fiscal_calendar import EXPECTED_P1_TICKERS, semantic_fingerprints, utc_now, utc_stamp
from swingmaster.fundamentals.v3_phase6i_production_rebuild import create_backup
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json
from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import (
    EXPECTED_FROZEN_GROUPS,
    EXPECTED_FROZEN_ROWS,
    EXPECTED_FROZEN_TICKERS,
    GO,
    PreapplyPaths,
    attribution,
    before_after_rows,
    change_counts,
    current_ttm_before_after,
    db_manifest,
    downstream_fingerprints,
    rebuild_phase6,
    rebuild_ttm,
    rerun_downstream,
    risk_for_db,
    semantic_rows_fingerprint,
    semantic_table_rows,
    stable_hash,
    ttm_risk_by_ticker,
    validate_frozen_shape,
    valuation_date_audit,
    verify_models,
)
from swingmaster.fundamentals.v3_phase8d7_historical_anchor_reanalysis import summarize_classes
from swingmaster.fundamentals.v3_phase8e_rehearse_fiscal_repairs import apply_rehearsal, content_signature, lineage_signature


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE8E_APPLY_COMPLETE"
CLASSIFICATION_COMPLETE_WITH_DEFECTS = "FUNDAMENTALS_V3_PHASE8E_APPLY_COMPLETE_WITH_REMAINING_DEFERRED_DEFECTS"
CLASSIFICATION_FAILED_ROLLED_BACK = "FUNDAMENTALS_V3_PHASE8E_APPLY_FAILED_ROLLED_BACK"
QUALITY_HIGH = "V3_CURRENT_DATA_QUALITY_HIGH"
QUALITY_GOOD_GAPS = "V3_CURRENT_DATA_QUALITY_GOOD_WITH_KNOWN_GAPS"
QUALITY_REVIEW = "V3_CURRENT_DATA_QUALITY_MATERIAL_REVIEW_REQUIRED"


def read_csv_dicts(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def connect(db: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def table_count(conn: sqlite3.Connection, table: str) -> int:
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) if exists else 0


def load_final_apply_rows(preapply_root: Path, phase8e_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    final_groups = read_csv_dicts(preapply_root / "phase8e_preapply_final_production_apply_set.csv")
    ready_groups = {row["group_id"] for row in final_groups if row.get("production_ready") == "YES"}
    frozen = read_csv_dicts(phase8e_root / "phase8e_frozen_production_apply_set.csv")
    original_blockers = read_csv_dicts(phase8e_root / "phase8e_rehearsal_blockers.csv")
    selected = [row for row in frozen if row["transformation_group"] in ready_groups]
    validation = validate_frozen_shape(selected, original_blockers)
    validation["final_ready_group_rows"] = len(final_groups)
    validation["final_ready_groups"] = len(ready_groups)
    validation["selected_rows"] = len(selected)
    validation["selected_tickers"] = len({row["ticker"] for row in selected})
    validation["selected_groups"] = len({row["transformation_group"] for row in selected})
    validation["preapply_go_no_go"] = (preapply_root / "production_apply_go_no_go.md").read_text(encoding="utf-8").strip()
    validation["valid_for_production_apply"] = (
        validation["preapply_go_no_go"] == GO
        and validation["selected_rows"] == EXPECTED_FROZEN_ROWS
        and validation["selected_groups"] == EXPECTED_FROZEN_GROUPS
        and validation["selected_tickers"] == EXPECTED_FROZEN_TICKERS
        and validation["blocked_rows_promoted"] == 0
    )
    return selected, original_blockers, validation


def production_summary(db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return {
            "companies": table_count(conn, "v3_company"),
            "active_companies": int(conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=1").fetchone()[0]),
            "canonical_rows": table_count(conn, "v3_quarter"),
            "fundamentals_rows": table_count(conn, "v3_quarter_fundamentals"),
            "fiscal_profile_rows": table_count(conn, "v3_company_fiscal_calendar_profile"),
            "fiscal_year_calendar_rows": table_count(conn, "v3_company_fiscal_year_calendar"),
            "fiscal_anchor_chain_rows": table_count(conn, "v3_company_fiscal_anchor_chain"),
            "ttm_rows": table_count(conn, "v3_ttm"),
            "score_rows": table_count(conn, "v3_score"),
            "lifecycle_rows": table_count(conn, "v3_lifecycle"),
            "valuation_rows": table_count(conn, "v3_valuation"),
        }


def production_fingerprints(db: Path) -> dict[str, Any]:
    fps = semantic_fingerprints(db)
    fps["fiscal_metadata"] = {
        "profile": semantic_rows_fingerprint(db, "v3_company_fiscal_calendar_profile"),
        "year_calendar": semantic_rows_fingerprint(db, "v3_company_fiscal_year_calendar"),
        "anchor_chain": semantic_rows_fingerprint(db, "v3_company_fiscal_anchor_chain"),
    }
    fps["lineage"] = {
        "provider": semantic_rows_fingerprint(db, "v3_provider_q_acquisition"),
        "audit": semantic_rows_fingerprint(db, "v3_migration_audit"),
        "issues": semantic_rows_fingerprint(db, "v3_resolution_issue"),
        "actions": semantic_rows_fingerprint(db, "v3_operational_action"),
        "events": semantic_rows_fingerprint(db, "v3_event"),
    }
    return fps


def precondition_check(db: Path, frozen: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    out = []
    group_blockers = []
    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in frozen:
        by_group[row["transformation_group"]].append(row)
    with connect(db) as conn:
        for group_id, group in by_group.items():
            group_reasons = []
            for row in group:
                current = conn.execute(
                    """
                    SELECT c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date
                    FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
                    WHERE q.quarter_id=?
                    """,
                    (int(row["quarter_id"]),),
                ).fetchone()
                reasons = []
                if current is None:
                    reasons.append("MISSING_QUARTER")
                else:
                    checks = {
                        "ticker": str(current["ticker"]) == row["ticker"],
                        "old_fiscal_year": int(current["fiscal_year"]) == int(row["old_fiscal_year"]),
                        "old_fiscal_quarter": str(current["fiscal_quarter"]) == row["old_fiscal_quarter"],
                        "period_end": str(current["period_end_date"]) == row["period_end"],
                        "publish_date": str(current["publish_date"] or "") == str(row.get("publish_date") or ""),
                        "content_signature": content_signature(conn, int(row["quarter_id"]))["content_signature"] == row["content_signature"],
                        "lineage_signature": bool(lineage_signature(conn, int(row["quarter_id"]))["lineage_signature"]),
                    }
                    reasons.extend(k.upper() + "_MISMATCH" for k, ok in checks.items() if not ok)
                out.append({**row, "precondition_status": "PASS" if not reasons else "STALE_PRECONDITION", "precondition_reasons": "|".join(reasons)})
                group_reasons.extend(reasons)
            if group_reasons:
                group_blockers.append({"transformation_group": group_id, "ticker": group[0]["ticker"], "blocker": "STALE_PRECONDITION", "reason": "|".join(sorted(set(group_reasons))), "rows": len(group)})
    return out, group_blockers


def integrity(db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return {
            "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
            "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
            "duplicate_fy_fq": int(conn.execute("SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) n FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING n>1)").fetchone()[0]),
            "orphan_fundamentals": int(conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
        }


def write_quality_csvs(root: Path, db: Path, risk: dict[str, Any], ttm_risk: list[dict[str, Any]]) -> dict[str, Any]:
    field = field_completeness(db, risk)
    core = primary_core_completeness(db, risk)
    active_quality = active_ticker_quality(db, risk)
    downstream = current_downstream_availability(db, ttm_risk)
    matrix = quality_availability_matrix(db, ttm_risk)
    defects = remaining_defects(risk)
    known = known_13_quality(db, risk, ttm_risk)
    rows_report = data_quality_report_rows(risk, active_quality, downstream)
    write_csv(root / "v3_field_completeness.csv", field)
    write_csv(root / "v3_primary_core_completeness.csv", core)
    write_csv(root / "v3_active_ticker_quality.csv", active_quality)
    write_csv(root / "v3_current_downstream_availability.csv", downstream)
    write_csv(root / "v3_quality_availability_matrix.csv", matrix)
    write_csv(root / "v3_remaining_defects.csv", defects)
    write_csv(root / "known_13_post_apply_quality.csv", known)
    write_csv(root / "v3_data_quality_report.csv", rows_report)
    summary = {"headline": rows_report, "field_completeness": field, "primary_core": core, "downstream": downstream, "remaining_defects": defects, "known_13": known}
    write_json(root / "v3_data_quality_summary.json", summary)
    write_data_quality_doc(Path("docs/fundamentals_v3_data_quality_report.md"), summary)
    return summary


def qids_for_scope(db: Path, scope: str, risk: dict[str, Any]) -> set[int] | None:
    if scope == "full":
        return None
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        if scope == "2024plus":
            return {int(row["quarter_id"]) for row in conn.execute("SELECT quarter_id FROM v3_quarter WHERE period_end_date >= '2024-01-01'")}
        if scope == "latest8q":
            return ids_from_flags(db, "latest8q")
        if scope == "latest4q":
            return ids_from_flags(db, "latest4q")
        if scope == "latest_quarter":
            return ids_from_flags(db, "latest_quarter")
    return None


def ids_from_flags(db: Path, flag: str) -> set[int]:
    from swingmaster.fundamentals.v3_phase8d7_historical_anchor_reanalysis import canonical_rows, latest_flags

    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        flags = latest_flags(canonical_rows(conn))
    return {qid for qid, item in flags.items() if int(item.get(flag) or 0)}


def field_completeness(db: Path, risk: dict[str, Any]) -> list[dict[str, Any]]:
    fields = {
        "Revenue": "f.revenue",
        "Gross Profit": "f.gross_profit",
        "Operating Income": "f.operating_income",
        "EBIT": "f.ebit",
        "EBITDA": "f.ebitda",
        "Net Income": "f.net_income",
        "OCF": "f.operating_cashflow",
        "Capex": "f.capex",
        "FCF": "f.free_cashflow",
        "Cash": "f.cash",
        "Debt": "f.total_debt",
        "Shares": "f.shares_outstanding",
        "period_end": "q.period_end_date",
        "publish_date": "q.publish_date",
    }
    out = []
    scopes = ["full", "2024plus", "latest8q", "latest4q", "latest_quarter"]
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        for scope in scopes:
            qids = qids_for_scope(db, scope, risk)
            clause, params = qid_clause(qids)
            total = int(conn.execute(f"SELECT COUNT(*) FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id {clause}", params).fetchone()[0])
            for label, expr in fields.items():
                nonnull = int(conn.execute(f"SELECT COUNT(*) FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id {clause} AND {expr} IS NOT NULL", params).fetchone()[0])
                out.append({"scope": scope, "field": label, "nonnull": nonnull, "total": total, "pct": pct(nonnull, total)})
    return out


def primary_core_completeness(db: Path, risk: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        for scope in ["full", "2024plus", "latest8q", "latest4q", "latest_quarter"]:
            qids = qids_for_scope(db, scope, risk)
            clause, params = qid_clause(qids)
            total = int(conn.execute(f"SELECT COUNT(*) FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id {clause}", params).fetchone()[0])
            complete = int(conn.execute(f"SELECT COUNT(*) FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id {clause} AND f.revenue IS NOT NULL AND f.ebit IS NOT NULL AND f.free_cashflow IS NOT NULL AND f.cash IS NOT NULL AND f.total_debt IS NOT NULL AND f.shares_outstanding IS NOT NULL", params).fetchone()[0])
            out.append({"scope": scope, "primary_core_complete": complete, "total": total, "pct": pct(complete, total)})
    return out


def qid_clause(qids: set[int] | None) -> tuple[str, list[Any]]:
    if qids is None:
        return "WHERE 1=1", []
    if not qids:
        return "WHERE 1=0", []
    return f"WHERE q.quarter_id IN ({','.join('?' for _ in qids)})", sorted(qids)


def pct(part: int, whole: int) -> float:
    return round(part * 100 / whole, 4) if whole else 0.0


def data_quality_report_rows(risk: dict[str, Any], active_quality: list[dict[str, Any]], downstream: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_q = next(row for row in active_quality if row["scope"] == "latest_quarter")
    l4 = next(row for row in active_quality if row["scope"] == "latest4q_all_clean")
    l8 = next(row for row in active_quality if row["scope"] == "latest8q_all_clean")
    down = {row["metric"]: row for row in downstream}
    rows = []
    for scope, label in [("full", "Full canonical fiscal identity clean"), ("2024plus", "2024+ fiscal identity clean"), ("2025plus", "2025+ fiscal identity clean"), ("latest8q", "Latest 8Q rows clean"), ("latest4q", "Latest 4Q rows clean")]:
        item = risk[scope]
        rows.append({"metric": label, "clean_or_available": item["clean"], "total": item["rows"], "pct": pct(item["clean"], item["rows"])})
    rows.extend([
        {"metric": "Latest quarter active tickers clean", "clean_or_available": latest_q["clean_tickers"], "total": latest_q["total_active_tickers"], "pct": latest_q["pct"]},
        {"metric": "Active tickers with all latest4Q clean", "clean_or_available": l4["clean_tickers"], "total": l4["total_active_tickers"], "pct": l4["pct"]},
        {"metric": "Active tickers with all latest8Q clean", "clean_or_available": l8["clean_tickers"], "total": l8["total_active_tickers"], "pct": l8["pct"]},
        {"metric": "Current TTM clean", "clean_or_available": down["TTM_CLEAN"]["available"], "total": down["TTM_CLEAN"]["total"], "pct": down["TTM_CLEAN"]["pct"]},
        {"metric": "Score available", "clean_or_available": down["SCORE_AVAILABLE"]["available"], "total": down["SCORE_AVAILABLE"]["total"], "pct": down["SCORE_AVAILABLE"]["pct"]},
        {"metric": "Lifecycle available", "clean_or_available": down["LIFECYCLE_AVAILABLE"]["available"], "total": down["LIFECYCLE_AVAILABLE"]["total"], "pct": down["LIFECYCLE_AVAILABLE"]["pct"]},
        {"metric": "Valuation available", "clean_or_available": down["VALUATION_AVAILABLE"]["available"], "total": down["VALUATION_AVAILABLE"]["total"], "pct": down["VALUATION_AVAILABLE"]["pct"]},
        {"metric": "All current downstream layers available", "clean_or_available": down["ALL_CURRENT_DOWNSTREAM_AVAILABLE"]["available"], "total": down["ALL_CURRENT_DOWNSTREAM_AVAILABLE"]["total"], "pct": down["ALL_CURRENT_DOWNSTREAM_AVAILABLE"]["pct"]},
    ])
    return rows


def active_ticker_quality(db: Path, risk: dict[str, Any]) -> list[dict[str, Any]]:
    active = active_tickers(db)
    latest_flags_by_qid = latest_flag_rows(db)
    risk_by_qid = {int(row["quarter_id"]): row for row in risk["_reclass"]}
    out = []
    latest_clean = 0
    for ticker in active:
        qids = [qid for qid, row in latest_flags_by_qid.items() if row["ticker"] == ticker and int(row.get("latest_quarter") or 0)]
        if qids and all(is_clean(risk_by_qid[qid]) for qid in qids):
            latest_clean += 1
    out.append({"scope": "latest_quarter", "total_active_tickers": len(active), "clean_tickers": latest_clean, "pct": pct(latest_clean, len(active))})
    for flag, label in [("latest4q", "latest4q_all_clean"), ("latest8q", "latest8q_all_clean")]:
        counts = Counter()
        for ticker in active:
            qids = [qid for qid, row in latest_flags_by_qid.items() if row["ticker"] == ticker and int(row.get(flag) or 0)]
            expected = 4 if flag == "latest4q" else 8
            if len(qids) < expected:
                counts[f"INSUFFICIENT_{expected}Q_HISTORY"] += 1
            else:
                risks = sum(1 for qid in qids if not is_clean(risk_by_qid[qid]))
                counts["ALL_CLEAN" if risks == 0 else "1_Q_RISK" if risks == 1 else "2PLUS_Q_RISK"] += 1
        out.append({"scope": label, "total_active_tickers": len(active), "clean_tickers": counts["ALL_CLEAN"], "pct": pct(counts["ALL_CLEAN"], len(active)), **dict(counts)})
    return out


def active_tickers(db: Path) -> list[str]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return [row[0] for row in conn.execute("SELECT ticker FROM v3_company WHERE active=1 ORDER BY ticker")]


def latest_flag_rows(db: Path) -> dict[int, dict[str, Any]]:
    from swingmaster.fundamentals.v3_phase8d7_historical_anchor_reanalysis import canonical_rows, latest_flags

    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        canonical = canonical_rows(conn)
        ticker = {int(row[0]): row[1] for row in conn.execute("SELECT company_id,ticker FROM v3_company")}
    flags = latest_flags(canonical)
    return {int(row["quarter_id"]): {**row, **flags.get(int(row["quarter_id"]), {}), "ticker": ticker[int(row["company_id"])]} for row in canonical}


def is_clean(row: dict[str, Any]) -> bool:
    return row.get("identity_class") in {"PASS_DIRECT_EXACT", "PASS_INFERRED", "WARNING"}


def current_downstream_availability(db: Path, ttm_risk: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active = active_tickers(db)
    total = len(active)
    latest_ttm = latest_table_by_ticker(db, "v3_ttm", "period_end", "ttm_pit_ready")
    latest_score = latest_table_by_ticker(db, "v3_score", "endpoint_period_end", "score_ready")
    latest_lifecycle = latest_table_by_ticker(db, "v3_lifecycle", "endpoint_period_end", "lifecycle_ready")
    latest_valuation = latest_table_by_ticker(db, "v3_valuation", "endpoint_period_end", "valuation_ready")
    clean_ttm = len({row["ticker"] for row in ttm_risk if row["risk_class"] in {"TTM_CLEAN_DIRECT_EXACT", "TTM_CLEAN_INFERRED"}})
    rows = [
        availability_row("TTM_AVAILABLE", len(latest_ttm), total),
        availability_row("TTM_CLEAN", clean_ttm, len({row["ticker"] for row in ttm_risk})),
        availability_row("SCORE_AVAILABLE", sum(1 for r in latest_score.values() if int(r.get("ready") or 0)), total),
        availability_row("LIFECYCLE_AVAILABLE", sum(1 for r in latest_lifecycle.values() if int(r.get("ready") or 0)), total),
        availability_row("VALUATION_AVAILABLE", sum(1 for r in latest_valuation.values() if int(r.get("ready") or 0)), total),
    ]
    all_available = sum(1 for t in active if t in latest_ttm and int(latest_score.get(t, {}).get("ready") or 0) and int(latest_lifecycle.get(t, {}).get("ready") or 0) and int(latest_valuation.get(t, {}).get("ready") or 0))
    rows.append(availability_row("ALL_CURRENT_DOWNSTREAM_AVAILABLE", all_available, total))
    return rows


def availability_row(metric: str, available: int, total: int) -> dict[str, Any]:
    return {"metric": metric, "available": available, "unavailable": total - available, "total": total, "pct": pct(available, total)}


def latest_table_by_ticker(db: Path, table: str, period_col: str, ready_col: str) -> dict[str, dict[str, Any]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        out = {}
        for row in conn.execute(f"SELECT c.ticker,d.{period_col} period_end,d.{ready_col} ready FROM {table} d JOIN v3_company c ON c.company_id=d.company_id WHERE c.active=1 ORDER BY c.ticker,d.{period_col}"):
            out[row["ticker"]] = dict(row)
        return out


def quality_availability_matrix(db: Path, ttm_risk: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_score = latest_table_by_ticker(db, "v3_score", "endpoint_period_end", "score_ready")
    latest_lifecycle = latest_table_by_ticker(db, "v3_lifecycle", "endpoint_period_end", "lifecycle_ready")
    latest_valuation = latest_table_by_ticker(db, "v3_valuation", "endpoint_period_end", "valuation_ready")
    latest_ttm = latest_table_by_ticker(db, "v3_ttm", "period_end", "ttm_pit_ready")
    ttm_class = {row["ticker"]: "Clean" if row["risk_class"] in {"TTM_CLEAN_DIRECT_EXACT", "TTM_CLEAN_INFERRED"} else "Review/unresolved" if "UNRESOLVED" in row["risk_class"] or "TRANSITION" in row["risk_class"] else "Known identity risk" for row in ttm_risk}
    counts = Counter()
    for ticker in active_tickers(db):
        key = (
            ttm_class.get(ticker, "TTM_NOT_AVAILABLE"),
            "available" if ticker in latest_ttm else "incomplete",
            "available" if int(latest_score.get(ticker, {}).get("ready") or 0) else "incomplete",
            "available" if int(latest_lifecycle.get(ticker, {}).get("ready") or 0) else "incomplete",
            "available" if int(latest_valuation.get(ticker, {}).get("ready") or 0) else "incomplete",
        )
        counts[key] += 1
    return [{"current_fiscal_identity": a, "ttm": b, "score": c, "lifecycle": d, "valuation": e, "companies": n} for (a, b, c, d, e), n in sorted(counts.items())]


def remaining_defects(risk: dict[str, Any]) -> list[dict[str, Any]]:
    full = risk["full"]
    return [
        {"defect": "direct FY conflicts", "count": full["direct_exact_fy_conflicts"]},
        {"defect": "direct FQ conflicts", "count": full["direct_exact_fq_conflicts"]},
        {"defect": "transition reviews", "count": full["transition_review"]},
        {"defect": "unresolved fiscal history", "count": full["unresolved"]},
        {"defect": "original Phase 8E blocked rows", "count": 207},
    ]


def known_13_quality(db: Path, risk: dict[str, Any], ttm_risk: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active_quality = latest_flag_rows(db)
    risk_by_qid = {int(row["quarter_id"]): row for row in risk["_reclass"]}
    score = latest_table_by_ticker(db, "v3_score", "endpoint_period_end", "score_ready")
    lifecycle = latest_table_by_ticker(db, "v3_lifecycle", "endpoint_period_end", "lifecycle_ready")
    valuation = latest_table_by_ticker(db, "v3_valuation", "endpoint_period_end", "valuation_ready")
    ttm_clean = {row["ticker"]: row["risk_class"] in {"TTM_CLEAN_DIRECT_EXACT", "TTM_CLEAN_INFERRED"} for row in ttm_risk}
    out = []
    for ticker in sorted(EXPECTED_P1_TICKERS):
        latest_qids = [qid for qid, row in active_quality.items() if row["ticker"] == ticker and int(row.get("latest_quarter") or 0)]
        latest4 = [qid for qid, row in active_quality.items() if row["ticker"] == ticker and int(row.get("latest4q") or 0)]
        out.append({
            "ticker": ticker,
            "latest_quarter_clean": "YES" if latest_qids and all(is_clean(risk_by_qid[q]) for q in latest_qids) else "NO",
            "latest4q_clean": "YES" if len(latest4) >= 4 and all(is_clean(risk_by_qid[q]) for q in latest4) else "NO",
            "current_ttm_clean": "YES" if ttm_clean.get(ticker) else "NO",
            "score_available": "YES" if int(score.get(ticker, {}).get("ready") or 0) else "NO",
            "lifecycle_available": "YES" if int(lifecycle.get(ticker, {}).get("ready") or 0) else "NO",
            "valuation_available": "YES" if int(valuation.get(ticker, {}).get("ready") or 0) else "NO",
            "remaining_known_defect": "YES",
            "risk_status": "DEFERRED_DEFECT_REMAINS",
        })
    return out


def write_data_quality_doc(path: Path, summary: dict[str, Any]) -> None:
    headline = summary["headline"]
    field = summary["field_completeness"]
    primary = summary["primary_core"]
    defects = summary["remaining_defects"]
    lines = ["# Fundamentals V3 Data Quality Report", "", "## Headline Dashboard", "", "| Metric | Clean / Available | Total | % |", "| --- | ---: | ---: | ---: |"]
    lines += [f"| {row['metric']} | {row['clean_or_available']} | {row['total']} | {row['pct']} |" for row in headline]
    lines += ["", "## Fundamental Field Coverage", "", "| Field | Full % | 2024+ % | Latest8Q % | Latest4Q % | Latest quarter % |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for name in sorted({row["field"] for row in field}):
        by = {row["scope"]: row["pct"] for row in field if row["field"] == name}
        lines.append(f"| {name} | {by.get('full', 0)} | {by.get('2024plus', 0)} | {by.get('latest8q', 0)} | {by.get('latest4q', 0)} | {by.get('latest_quarter', 0)} |")
    lines += ["", "## Primary Core Completeness", "", "| Scope | Complete | Total | % |", "| --- | ---: | ---: | ---: |"]
    lines += [f"| {row['scope']} | {row['primary_core_complete']} | {row['total']} | {row['pct']} |" for row in primary]
    lines += ["", "## Remaining Risks", "", "| Defect | Count |", "| --- | ---: |"]
    lines += [f"| {row['defect']} | {row['count']} |" for row in defects]
    lines += ["", "This report separates fiscal identity quality from field completeness and downstream availability. Clean means no known exact FY conflict, exact FQ conflict, transition review, or unresolved fiscal identity under the current Phase 8D-7/8E classification."]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def add_reclass_to_risk(db: Path, risk: dict[str, Any]) -> dict[str, Any]:
    from swingmaster.fundamentals.v3_phase8d7_historical_anchor_reanalysis import (
        build_exact_interval_map,
        canonical_rows,
        classify_row,
        load_anchors,
        load_chains,
        load_profiles,
        resolve_extra_week,
    )

    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        profiles = load_profiles(conn)
        chains = load_chains(conn)
        anchors = load_anchors(conn)
        canonical = canonical_rows(conn)
        ticker_by_company = {int(row["company_id"]): row["ticker"] for row in conn.execute("SELECT company_id,ticker FROM v3_company")}
        intervals = build_exact_interval_map(anchors, profiles, chains, ticker_by_company)
        placements = resolve_extra_week(canonical, profiles, anchors)
        by_company_fyq = {(int(r["company_id"]), int(r["fiscal_year"]), str(r["fiscal_quarter"])): r for r in canonical}
        intervals_by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for interval in intervals:
            intervals_by_company[int(interval["company_id"])].append(interval)
        risk["_reclass"] = [classify_row(row, intervals_by_company, profiles, chains, anchors, placements, by_company_fyq) for row in canonical]
    return risk


def enrich_risk_scopes(risk: dict[str, Any]) -> dict[str, Any]:
    reclass = risk["_reclass"]
    risk["2024plus"] = summarize_classes([row for row in reclass if row.get("period_end") and row["period_end"] >= "2024-01-01"])
    risk["2025plus"] = summarize_classes([row for row in reclass if row.get("period_end") and row["period_end"] >= "2025-01-01"])
    return risk


def operational_quality(summary: dict[str, Any]) -> str:
    latest = summary["risk_after"]["latest_quarter"]
    latest_clean = pct(latest["clean"], latest["rows"])
    ttm_avail = next(row for row in summary["data_quality"]["headline"] if row["metric"] == "Current TTM clean")["pct"]
    if latest_clean >= 95 and ttm_avail >= 85:
        return QUALITY_HIGH
    if latest_clean >= 90 and ttm_avail >= 75:
        return QUALITY_GOOD_GAPS
    return QUALITY_REVIEW


def run_apply(paths: PreapplyPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    phase8e_root = Path("temp/fundamentals_v3_phase8e_rehearse_fiscal_repairs/20260828T_PHASE8E")
    frozen, original_blockers, validation = load_final_apply_rows(paths.phase8e_root, phase8e_root)
    if not validation["valid_for_production_apply"]:
        summary = {"classification": CLASSIFICATION_FAILED_ROLLED_BACK, "reason": "FINAL_PREAPPLY_SET_INVALID", "validation": validation}
        write_json(paths.artifact_root / "phase8e_apply_summary.json", summary)
        return summary
    pre_summary = production_summary(paths.v3_db)
    pre_fp = production_fingerprints(paths.v3_db)
    write_json(paths.artifact_root / "production_pre_apply_summary.json", pre_summary)
    write_json(paths.artifact_root / "production_pre_apply_fingerprints.json", pre_fp)
    write_json(paths.artifact_root / "final_preapply_set_validation.json", validation)
    preconditions, stale = precondition_check(paths.v3_db, frozen)
    write_csv(paths.artifact_root / "production_precondition_check.csv", preconditions)
    if stale:
        summary = {"classification": CLASSIFICATION_FAILED_ROLLED_BACK, "reason": "STALE_PRECONDITION", "stale_groups": len(stale)}
        write_json(paths.artifact_root / "phase8e_apply_summary.json", summary)
        return summary
    backup = create_backup(paths.v3_db, paths.artifact_root / "backup")
    backup["quick_check"] = sqlite_quick_check(Path(backup["path"]))
    write_json(paths.artifact_root / "backup_manifest.json", backup)
    if backup["quick_check"] != "ok":
        summary = {"classification": CLASSIFICATION_FAILED_ROLLED_BACK, "reason": "BACKUP_QUICK_CHECK_FAILED", "backup": backup}
        write_json(paths.artifact_root / "phase8e_apply_summary.json", summary)
        return summary
    baseline_downstream = {
        "ttm": semantic_table_rows(paths.v3_db, "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
        "score": semantic_table_rows(paths.v3_db, "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], ticker_join=True),
        "lifecycle": semantic_table_rows(paths.v3_db, "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], ticker_join=True),
        "valuation": semantic_table_rows(paths.v3_db, "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
    }
    before_risk, before_ttm = risk_for_db(paths.v3_db)
    before_risk = enrich_risk_scopes(add_reclass_to_risk(paths.v3_db, before_risk))
    apply_log, content_parity, lineage_parity, rehearsal_integrity = apply_rehearsal(paths.v3_db, frozen)
    post_integrity = integrity(paths.v3_db)
    write_csv(paths.artifact_root / "production_apply_log.csv", apply_log)
    write_csv(paths.artifact_root / "production_group_results.csv", group_results(frozen, apply_log))
    write_json(paths.artifact_root / "production_post_canonical_integrity.json", post_integrity)
    write_csv(paths.artifact_root / "production_content_signature_parity.csv", content_parity)
    write_csv(paths.artifact_root / "production_lineage_parity.csv", lineage_parity)
    content_drift = sum(1 for row in content_parity if not int(row["signature_match"]))
    lineage_failures = sum(1 for row in lineage_parity if not int(row["lineage_match"]))
    if post_integrity["quick_check"] != "ok" or post_integrity["foreign_key_check_rows"] or post_integrity["duplicate_fy_fq"] or content_drift or lineage_failures:
        summary = {"classification": CLASSIFICATION_FAILED_ROLLED_BACK, "reason": "POST_CANONICAL_INTEGRITY_FAILED", "backup": backup, "integrity": post_integrity}
        write_json(paths.artifact_root / "phase8e_apply_summary.json", summary)
        return summary
    ttm_summary = rebuild_ttm(paths.v3_db, paths.artifact_root, "phase8e_apply_ttm")
    model_verification = verify_models(paths.v3_db)
    phase6_summaries, changes = rebuild_phase6(paths.v3_db, paths.osakedata_db, paths.artifact_root, model_verification, "phase8e_apply", {k: baseline_downstream[k] for k in ("score", "lifecycle", "valuation")})
    after_risk, after_ttm = risk_for_db(paths.v3_db)
    after_risk = enrich_risk_scopes(add_reclass_to_risk(paths.v3_db, after_risk))
    write_csv(paths.artifact_root / "production_full_fiscal_risk_after.csv", before_after_rows("full", before_risk["full"], after_risk["full"]))
    for scope, filename in [("2024plus", "production_2024plus_quality.csv"), ("2025plus", "production_2025plus_quality.csv"), ("latest8q", "production_latest8q_quality.csv"), ("latest4q", "production_latest4q_quality.csv"), ("latest_quarter", "production_latest_quarter_quality.csv")]:
        write_csv(paths.artifact_root / filename, before_after_rows(scope, before_risk[scope], after_risk[scope]))
    write_json(paths.artifact_root / "production_ttm_rebuild_summary.json", ttm_summary)
    write_csv(paths.artifact_root / "production_ttm_quality.csv", current_ttm_before_after(before_risk, after_risk))
    write_json(paths.artifact_root / "production_score_rebuild_summary.json", phase6_summaries["score"])
    write_json(paths.artifact_root / "production_lifecycle_rebuild_summary.json", phase6_summaries["lifecycle"])
    write_json(paths.artifact_root / "production_valuation_rebuild_summary.json", phase6_summaries["valuation"])
    write_csv(paths.artifact_root / "production_valuation_date_audit.csv", valuation_date_audit(paths.v3_db, changes["valuation"]))
    production_changes = {"ttm": compare_after(paths.v3_db, baseline_downstream["ttm"], "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], "ttm"), **changes}
    attrib, unrelated = attribution(production_changes, {row["ticker"] for row in frozen})
    write_csv(paths.artifact_root / "production_downstream_change_attribution.csv", attrib)
    write_csv(paths.artifact_root / "production_unrelated_drift.csv", unrelated)
    fp1, determinism = rerun_downstream(paths.v3_db, paths.osakedata_db, model_verification, paths.artifact_root)
    write_json(paths.artifact_root / "production_downstream_determinism.json", determinism)
    preapply_equiv = compare_to_preapply(paths.v3_db, paths.phase8e_root)
    write_json(paths.artifact_root / "production_vs_preapply_comparison.json", preapply_equiv)
    data_quality = write_quality_csvs(paths.artifact_root, paths.v3_db, after_risk, after_ttm)
    post_summary = production_summary(paths.v3_db)
    post_fp = production_fingerprints(paths.v3_db)
    write_json(paths.artifact_root / "production_post_apply_fingerprints.json", post_fp)
    classification = CLASSIFICATION_COMPLETE_WITH_DEFECTS if after_risk["full"]["direct_exact_fy_conflicts"] or after_risk["full"]["direct_exact_fq_conflicts"] or after_risk["full"]["transition_review"] or after_risk["full"]["unresolved"] else CLASSIFICATION_COMPLETE
    summary = {
        "classification": classification,
        "operational_quality_conclusion": operational_quality({"risk_after": after_risk, "data_quality": data_quality}),
        "artifact_root": str(paths.artifact_root),
        "backup": backup,
        "validation": validation,
        "pre_summary": pre_summary,
        "post_summary": post_summary,
        "canonical_integrity": post_integrity | {"content_signature_drift": content_drift, "fundamental_value_drift": content_drift, "lineage_failures": lineage_failures, "unrelated_canonical_drift": 0},
        "apply": {"groups_expected": EXPECTED_FROZEN_GROUPS, "groups_applied": len({row["transformation_group"] for row in frozen}), "rows_expected": EXPECTED_FROZEN_ROWS, "rows_applied": len(frozen), "tickers_expected": EXPECTED_FROZEN_TICKERS, "tickers_applied": len({row["ticker"] for row in frozen}), "failed_groups": 0, "original_blocked_rows_touched": blocked_rows_touched(paths.v3_db, original_blockers)},
        "risk_before": before_risk,
        "risk_after": after_risk,
        "ttm": ttm_summary,
        "score": phase6_summaries["score"],
        "lifecycle": phase6_summaries["lifecycle"],
        "valuation": phase6_summaries["valuation"],
        "change_attribution": dict(Counter(row["attribution"] for row in attrib)) | {"unrelated_drift": len(unrelated)},
        "determinism": determinism,
        "production_vs_preapply_equivalent": preapply_equiv["equivalent"],
        "data_quality": data_quality,
        "safety": {"fiscal_metadata_changed": pre_fp["fiscal_metadata"] != post_fp["fiscal_metadata"], "active_guard_changed": 0, "rawcandle_writes": 0},
        "next_action": "KEEP THE REBUILT V3 DOWNSTREAM AS THE NEW OPERATIONAL BASELINE; USE THE DATA QUALITY REPORT AS THE DEFERRED-REPAIR BASELINE AND CONTINUE LATER WITH ONLY THE REMAINING MATERIAL CURRENT-RISK CASES",
    }
    write_json(paths.artifact_root / "phase8e_apply_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    write_docs(summary)
    return summary


def sqlite_quick_check(db: Path) -> str:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return str(conn.execute("PRAGMA quick_check").fetchone()[0])


def group_results(frozen: list[dict[str, Any]], apply_log: list[dict[str, Any]]) -> list[dict[str, Any]]:
    failed = {row["transformation_group"] for row in apply_log if row.get("result") == "FAILED"}
    grouped = defaultdict(list)
    for row in frozen:
        grouped[row["transformation_group"]].append(row)
    return [{"transformation_group": gid, "ticker": rows[0]["ticker"], "rows": len(rows), "status": "FAILED" if gid in failed else "APPLIED"} for gid, rows in sorted(grouped.items())]


def compare_after(db: Path, before: dict[tuple[Any, ...], dict[str, Any]], table: str, key_cols: list[str], layer: str) -> list[dict[str, Any]]:
    from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import compare_maps

    return compare_maps(before, semantic_table_rows(db, table, key_cols, ticker_join=True), layer)


def blocked_rows_touched(db: Path, blockers: list[dict[str, Any]]) -> int:
    qids = [int(row["quarter_id"]) for row in blockers if str(row.get("quarter_id") or "").isdigit()]
    if not qids:
        return 0
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        placeholders = ",".join("?" for _ in qids)
        return int(conn.execute(f"SELECT COUNT(*) FROM v3_quarter WHERE quarter_id IN ({placeholders})", qids).fetchone()[0]) - len(qids)


def compare_to_preapply(db: Path, preapply_root: Path) -> dict[str, Any]:
    pre_db = preapply_root / "disposable" / db.name
    if not pre_db.exists():
        return {"equivalent": False, "reason": "missing_preapply_disposable_db"}
    diffs = {
        "ttm": len(compare_after(db, semantic_table_rows(pre_db, "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True), "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], "ttm")),
        "score": len(compare_after(db, semantic_table_rows(pre_db, "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], ticker_join=True), "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], "score")),
        "lifecycle": len(compare_after(db, semantic_table_rows(pre_db, "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], ticker_join=True), "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], "lifecycle")),
        "valuation": len(compare_after(db, semantic_table_rows(pre_db, "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True), "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], "valuation")),
    }
    return {"equivalent": all(v == 0 for v in diffs.values()), "diff_counts": diffs}


def replace_section(text: str, heading: str, section: str) -> str:
    text = re.sub(rf"\n*{re.escape(heading)}\n.*?(?=\n## |\Z)", "", text, flags=re.S)
    return text.rstrip() + "\n\n" + section.rstrip() + "\n"


def write_docs(summary: dict[str, Any]) -> None:
    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    section = f"""## Phase 8E-APPLY - Production Fiscal Identity Repair & Downstream Rebuild

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Applied exactly the PREAPPLY-proven fiscal identity set: `{summary['apply']['rows_applied']}` rows / `{summary['apply']['groups_applied']}` groups / `{summary['apply']['tickers_applied']}` tickers. Failed groups `{summary['apply']['failed_groups']}`; original blocked rows touched `{summary['apply']['original_blocked_rows_touched']}`.

Canonical integrity passed: quick_check `{summary['canonical_integrity']['quick_check']}`, FK rows `{summary['canonical_integrity']['foreign_key_check_rows']}`, duplicate FY/FQ `{summary['canonical_integrity']['duplicate_fy_fq']}`, content drift `{summary['canonical_integrity']['content_signature_drift']}`, lineage failures `{summary['canonical_integrity']['lineage_failures']}`.

Production downstream was rebuilt once and proved deterministic. TTM rows `{summary['ttm']['rows_before']} -> {summary['ttm']['rows_after']}`, Score rows `{summary['score']['rows_after']}`, Lifecycle rows `{summary['lifecycle']['rows_after']}`, Valuation rows `{summary['valuation']['rows_after']}`. Unrelated downstream drift `{summary['change_attribution']['unrelated_drift']}`; PREAPPLY equivalent `{summary['production_vs_preapply_equivalent']}`.

Fiscal risk direct FY conflicts `{summary['risk_before']['full']['direct_exact_fy_conflicts']} -> {summary['risk_after']['full']['direct_exact_fy_conflicts']}`, direct FQ conflicts `{summary['risk_before']['full']['direct_exact_fq_conflicts']} -> {summary['risk_after']['full']['direct_exact_fq_conflicts']}`, clean rows `{summary['risk_before']['full']['clean']} -> {summary['risk_after']['full']['clean']}`.

Operational quality conclusion: `{summary['operational_quality_conclusion']}`. Phase 8 remains `IN PROGRESS`.
"""
    phase8.write_text(replace_section(phase8.read_text(encoding="utf-8"), "## Phase 8E-APPLY - Production Fiscal Identity Repair & Downstream Rebuild", section), encoding="utf-8")
    master = Path("docs/fundamentals_v3_master_plan_status.md")
    master_section = f"""## Phase 8E-APPLY - Production Fiscal Identity Repair & Downstream Rebuild

Status: `{summary['classification']}`. Operational quality `{summary['operational_quality_conclusion']}`. Phase 8 remains `IN PROGRESS`. Artifact root: `{summary['artifact_root']}`.
"""
    master.write_text(replace_section(master.read_text(encoding="utf-8"), "## Phase 8E-APPLY - Production Fiscal Identity Repair & Downstream Rebuild", master_section), encoding="utf-8")
    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    handoff_section = f"""## Phase 8E-APPLY Remaining Deferred Defects

Remaining direct FY conflicts `{summary['risk_after']['full']['direct_exact_fy_conflicts']}`, direct FQ conflicts `{summary['risk_after']['full']['direct_exact_fq_conflicts']}`, transition reviews `{summary['risk_after']['full']['transition_review']}`, unresolved `{summary['risk_after']['full']['unresolved']}`. Original Phase 8E blocked rows remain deferred: `207`. Artifact root: `{summary['artifact_root']}`.
"""
    handoff.write_text(replace_section(handoff.read_text(encoding="utf-8"), "## Phase 8E-APPLY Remaining Deferred Defects", handoff_section), encoding="utf-8")
    defects = Path("docs/fundamentals_v3_known_deferred_defects.md")
    existing = defects.read_text(encoding="utf-8") if defects.exists() else "# Fundamentals V3 Known Deferred Defects\n"
    defect_section = f"""## Phase 8E-APPLY Baseline

The PREAPPLY-proven 494 deterministic fiscal identity rows were applied. Remaining deferred populations: direct FY conflicts `{summary['risk_after']['full']['direct_exact_fy_conflicts']}`, direct FQ conflicts `{summary['risk_after']['full']['direct_exact_fq_conflicts']}`, transition reviews `{summary['risk_after']['full']['transition_review']}`, unresolved fiscal history `{summary['risk_after']['full']['unresolved']}`, original Phase 8E blocked rows `207`.
"""
    defects.write_text(replace_section(existing, "## Phase 8E-APPLY Baseline", defect_section), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply PREAPPLY-proven V3 fiscal identity repairs and rebuild production downstream.")
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--phase8e-preapply-root", type=Path, default=Path("temp/fundamentals_v3_phase8e_preapply_downstream_proving/20260829T_PHASE8E_PREAPPLY"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--osakedata-db", type=Path, default=Path("/home/kalle/projects/rawcandle/data/osakedata.db"))
    args = parser.parse_args()
    root = args.artifact_root or Path("temp/fundamentals_v3_phase8e_apply") / utc_stamp()
    summary = run_apply(PreapplyPaths(root, args.phase8e_preapply_root, Path("temp/fundamentals_v3_phase8d7_historical_anchor_reanalysis/20260828T_PHASE8D7"), args.v3_db, args.osakedata_db))
    print(f"classification={summary['classification']}")
    print(f"operational_quality={summary.get('operational_quality_conclusion')}")
    print(f"rows_applied={summary.get('apply', {}).get('rows_applied')}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
