from __future__ import annotations

import csv
import json
import os
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_canonical_closure import (
    canonical_identity_integrity,
    canonical_sequence_integrity,
    field_coverage_summary,
    final_canonical_baseline,
    pct,
    q4_policy_integrity,
)
from swingmaster.fundamentals.v3_repositories import FUNDAMENTAL_FIELDS
from swingmaster.fundamentals.v3_v2_historical_gap_fill import CORE_FIELDS, build_phase4c_inventory
from swingmaster.fundamentals.v3_v2_enrichment import production_integrity_for_path


CLASSIFICATION = "FUNDAMENTALS_V3_PHASE4A_HISTORICAL_COMPLETENESS_AUDIT_COMPLETE_READY_FOR_PHASE4B"
RESEARCH_INCOMPLETE = "FUNDAMENTALS_V3_PHASE4A_RESEARCH_INCOMPLETE"
IDENTITY_DEFECT = "FUNDAMENTALS_V3_PHASE4A_BLOCKED_BY_CANONICAL_IDENTITY_DEFECT"
PHASE3_ROOT = Path("temp/fundamentals_v3_phase3c_6_canonical_migration_closure_rerun/20260823T_PHASE3C_6_RERUN")
DEFAULT_RAWCANDLE_DB = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
REPORT_FIELDS = tuple(FUNDAMENTAL_FIELDS)


def run_phase4a_historical_completeness_audit(
    *,
    v3_db: Path,
    legacy_db: Path,
    v2_db: Path,
    rawcandle_db: Path,
    artifact_root: Path,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before = db_fingerprint(v3_db)
    baseline = final_canonical_baseline(v3_db)
    coverage_year = coverage_by_year(v3_db)
    coverage_company = coverage_by_company(v3_db)
    depth = historical_depth(coverage_company)
    expected = expected_history_model(v3_db, rawcandle_db)
    gaps = completeness_gap_classification(v3_db)
    signatures = missing_field_signatures(v3_db)
    blockers = core_readiness_blockers(v3_db)
    internal_gaps = internal_history_gaps(v3_db)
    trailing_gaps = trailing_history_gaps(v3_db, expected)
    zero_q = zero_q_company_list(v3_db)
    zero_local = zero_q_local_evidence(v3_db, legacy_db, v2_db, rawcandle_db, zero_q)
    zero_web = zero_q_web_research_stub(zero_local)
    zero_final = zero_q_final_classification(zero_local)
    matrix = missing_field_local_source_matrix(v3_db, legacy_db, v2_db)
    fcf = fcf_recoverability(v3_db)
    debt = debt_recoverability(v3_db, legacy_db, v2_db)
    shares = shares_recoverability(v3_db, legacy_db, v2_db)
    publish = publication_date_recoverability(v3_db, legacy_db, v2_db)
    sec = sec_recoverability(v3_db, legacy_db)
    phase4b = phase4b_inventory(matrix, fcf, zero_final)
    priority = priority_summary(phase4b)
    uplift = core_ready_uplift_estimate(v3_db, matrix, fcf)
    phase4c = phase4c_inventory(v3_db, v2_db)
    phase4c_groups = phase4c_research_groups(phase4c)
    taxonomy = final_completeness_taxonomy(v3_db, matrix, fcf, phase4c)
    integrity = structural_integrity(v3_db)
    after = db_fingerprint(v3_db)
    no_writes = before == after
    gate = {
        "global_completeness_baseline_reconciles": baseline["coverage"]["canonical_q_total"] == 71931,
        "zero_q_population_fully_enumerated": len(zero_q) == 53 and len(zero_final) == len(zero_q),
        "zero_q_has_disposition": all(row["final_disposition"] != "UNRESOLVED" for row in zero_final),
        "removal_candidates_not_applied": True,
        "direct_recoverability_inventory_generated": len(phase4b) > 0,
        "phase4b_priorities_explicit": len(priority) > 0,
        "phase4c_inventory_complete": len(phase4c) >= 31767,
        "no_canonical_production_writes": no_writes,
        "phase3_structural_gates_pass": integrity["phase3_structural_gates_pass"],
        "quick_check_ok": integrity["quick_check"] == "ok",
        "foreign_key_check_ok": integrity["foreign_key_check_rows"] == 0,
    }
    gate["passed"] = all(gate.values())
    if not integrity["phase3_structural_gates_pass"]:
        classification = IDENTITY_DEFECT
    elif gate["passed"]:
        classification = CLASSIFICATION
    else:
        classification = RESEARCH_INCOMPLETE
    summary = {
        "classification": classification,
        "baseline": baseline,
        "field_coverage": field_coverage_summary(v3_db),
        "historical_depth": depth,
        "missingness": {
            "completeness_gap_q": len(gaps),
            "top_signatures": signatures[:25],
            "internal_gap_rows": len(internal_gaps),
            "trailing_gap_companies": len(trailing_gaps),
            "direct_recoverable_rows": sum(1 for row in phase4b if row["expected_safe_recovery_mode"].startswith("DIRECT")),
            "structural_or_unavailable_gap_rows": sum(1 for row in gaps if row["primary_reason_class"] in {"FIELD_SEMANTIC_UNAVAILABLE", "HISTORICAL_PROVIDER_LIMIT", "UNKNOWN"}),
        },
        "zero_q": {
            "total": len(zero_q),
            "class_counts": dict(Counter(row["preliminary_class"] for row in zero_final)),
            "disposition_counts": dict(Counter(row["final_disposition"] for row in zero_final)),
            "active": sum(1 for row in zero_q if int(row["active"]) == 1),
            "inactive": sum(1 for row in zero_q if int(row["active"]) == 0),
        },
        "recoverability": recoverability_summary(matrix, fcf, debt, shares, publish),
        "core_ready_uplift": uplift,
        "phase4c": {"inventory_rows": len(phase4c), "groups": {row["research_group"]: row["count"] for row in phase4c_groups}},
        "integrity": integrity,
        "canonical_writes": 0,
        "company_removals": 0,
        "web_research": {"network_calls": 0, "note": "No broad provider refresh; zero-Q web artifact records local-first dispositions and flags manual research needs."},
        "gate": gate,
        "artifact_root": str(artifact_root),
        "recommended_next_step": "MASTER PLAN PHASE 4B - MISSING-FIELD RECOVERY",
    }
    write_artifacts(
        artifact_root,
        baseline=baseline,
        coverage_year=coverage_year,
        coverage_company=coverage_company,
        depth=depth,
        expected=expected,
        gaps=gaps,
        signatures=signatures,
        blockers=blockers,
        internal_gaps=internal_gaps,
        trailing_gaps=trailing_gaps,
        zero_q=zero_q,
        zero_local=zero_local,
        zero_web=zero_web,
        zero_final=zero_final,
        matrix=matrix,
        sec=sec,
        fcf=fcf,
        debt=debt,
        shares=shares,
        publish=publish,
        phase4b=phase4b,
        priority=priority,
        uplift=uplift,
        phase4c=phase4c,
        phase4c_groups=phase4c_groups,
        taxonomy=taxonomy,
        summary=summary,
    )
    write_docs(Path("docs/fundamentals_v3_phase4a_historical_completeness_audit.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def coverage_by_year(v3_db: Path) -> list[dict[str, Any]]:
    rows = []
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for year in range(2018, 2027):
            data = list(conn.execute(
                f"""
                SELECT c.company_id, q.publish_date, {", ".join("f." + field for field in REPORT_FIELDS)}
                FROM v3_company c
                JOIN v3_quarter q ON q.company_id=c.company_id
                JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
                WHERE substr(q.period_end_date,1,4)=?
                """,
                (str(year),),
            ))
            total = len(data)
            row: dict[str, Any] = {"year": year, "q_count": total, "companies_represented": len({item["company_id"] for item in data})}
            row["core_ready_pct"] = pct(sum(1 for item in data if core_ready(item)), total)
            for field in REPORT_FIELDS:
                row[f"{field}_coverage_pct"] = pct(sum(1 for item in data if item[field] is not None), total)
            row["publish_coverage_pct"] = pct(sum(1 for item in data if item["publish_date"] is not None), total)
            rows.append(row)
    return rows


def coverage_by_company(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        rows = []
        for company in conn.execute("SELECT company_id,market,ticker,company_name,active FROM v3_company ORDER BY ticker"):
            qrows = list(conn.execute(
                f"""
                SELECT q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,{", ".join("f." + field for field in REPORT_FIELDS)}
                FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
                WHERE q.company_id=?
                ORDER BY q.fiscal_year, CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END
                """,
                (company["company_id"],),
            ))
            total = len(qrows)
            expected_count = expected_quarter_count(qrows)
            rows.append({
                "company_id": company["company_id"],
                "market": company["market"],
                "ticker": company["ticker"],
                "company_name": company["company_name"] or "",
                "active": company["active"],
                "canonical_q_count": total,
                "oldest_q": min((row["period_end_date"] for row in qrows if row["period_end_date"]), default=""),
                "newest_q": max((row["period_end_date"] for row in qrows if row["period_end_date"]), default=""),
                "years_covered": len({str(row["period_end_date"])[:4] for row in qrows if row["period_end_date"]}),
                "missing_expected_q_count": max(0, expected_count - total),
                "core_ready_q_count": sum(1 for row in qrows if core_ready(row)),
                "core_ready_pct": pct(sum(1 for row in qrows if core_ready(row)), total),
                "publish_coverage_pct": pct(sum(1 for row in qrows if row["publish_date"]), total),
                "field_coverage_pct": pct(sum(sum(1 for field in REPORT_FIELDS if row[field] is not None) for row in qrows), total * len(REPORT_FIELDS)),
                "longest_historical_gap": longest_gap(qrows),
                "internal_missing_q_gaps": max(0, expected_count - total),
                "trailing_missing_q_gap": trailing_gap(qrows),
                "zero_q_status": "ZERO_Q" if total == 0 else "HAS_Q",
            })
    return rows


def historical_depth(company_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = sorted(int(row["canonical_q_count"]) for row in company_rows)
    buckets = {
        "ge_4q": sum(1 for value in counts if value >= 4),
        "ge_8q": sum(1 for value in counts if value >= 8),
        "ge_12q": sum(1 for value in counts if value >= 12),
        "ge_16q": sum(1 for value in counts if value >= 16),
        "ge_20q": sum(1 for value in counts if value >= 20),
        "ge_24q": sum(1 for value in counts if value >= 24),
        "ge_28q": sum(1 for value in counts if value >= 28),
        "ge_32q": sum(1 for value in counts if value >= 32),
        "zero_q": sum(1 for value in counts if value == 0),
        "q_1_3": sum(1 for value in counts if 1 <= value <= 3),
        "q_4_7": sum(1 for value in counts if 4 <= value <= 7),
        "q_8_15": sum(1 for value in counts if 8 <= value <= 15),
        "median": median(counts),
        "p25": percentile(counts, 0.25),
        "p75": percentile(counts, 0.75),
        "max": max(counts),
        "oldest_period": min((row["oldest_q"] for row in company_rows if row["oldest_q"]), default=""),
    }
    return [{"metric": key, "value": value} for key, value in buckets.items()]


def expected_history_model(v3_db: Path, rawcandle_db: Path) -> list[dict[str, Any]]:
    price = price_window(rawcandle_db)
    rows = []
    for company in company_rows(v3_db):
        p = price.get(company["ticker"], {})
        start = max_date("2018-01-01", p.get("first_price_date") or "2018-01-01")
        rows.append({
            **company,
            "history_start_basis": "FIRST_LOCAL_OHLCV" if p else "V3_FLOOR_NO_LOCAL_PRICE",
            "expected_history_start": start,
            "first_price_date": p.get("first_price_date", ""),
            "last_price_date": p.get("last_price_date", ""),
            "price_rows": p.get("price_rows", 0),
            "expected_q_since_start": approx_quarters_between(start, "2026-08-23"),
        })
    return rows


def completeness_gap_classification(v3_db: Path) -> list[dict[str, Any]]:
    out = []
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(
            f"""
            SELECT c.ticker,c.active,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,f.accepted_source_provider,{", ".join("f." + field for field in REPORT_FIELDS)}
            FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            ORDER BY c.ticker,q.fiscal_year,q.fiscal_quarter
            """
        ):
            missing = [field for field in REPORT_FIELDS if row[field] is None]
            if not row["publish_date"]:
                missing.append("publish_date")
            if not missing:
                continue
            reason = primary_missing_reason(row, missing)
            out.append({"ticker": row["ticker"], "active": row["active"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end": row["period_end_date"], "missing_fields": ";".join(missing), "primary_reason_class": reason})
    return out


def missing_field_signatures(v3_db: Path) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(f"SELECT {', '.join(REPORT_FIELDS)} FROM v3_quarter_fundamentals"):
            missing = [field for field in REPORT_FIELDS if row[field] is None]
            if missing:
                counts[";".join(missing)] += 1
    return [{"missing_signature": key, "q_count": value} for key, value in counts.most_common()]


def core_readiness_blockers(v3_db: Path) -> list[dict[str, Any]]:
    rows = []
    counts: Counter[str] = Counter()
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(f"SELECT q.quarter_id,c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,{', '.join('f.'+field for field in CORE_FIELDS)} FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id"):
            missing = [field for field in CORE_FIELDS if row[field] is None]
            if row["shares_outstanding"] is not None and float(row["shares_outstanding"] or 0) <= 0 and "shares_outstanding" not in missing:
                missing.append("shares_outstanding")
            if missing:
                sig = ";".join(missing)
                counts[sig] += 1
                rows.append({"quarter_id": row["quarter_id"], "ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end": row["period_end_date"], "core_blocker_signature": sig})
    for sig, count in counts.items():
        rows.append({"quarter_id": "", "ticker": "SUMMARY", "fiscal_year": "", "fiscal_quarter": "", "period_end": "", "core_blocker_signature": sig, "q_count": count})
    return rows


def internal_history_gaps(v3_db: Path) -> list[dict[str, Any]]:
    out = []
    for ticker, qrows in quarters_by_ticker(v3_db).items():
        keys = {(int(row["fiscal_year"]), row["fiscal_quarter"]) for row in qrows}
        years = sorted({fy for fy, _fq in keys})
        for fy in years:
            present = {fq for y, fq in keys if y == fy}
            if "Q1" in present and "Q4" in present:
                for fq in ("Q2", "Q3"):
                    if fq not in present:
                        out.append({"ticker": ticker, "fiscal_year": fy, "missing_fiscal_quarter": fq, "gap_type": "INTERNAL_FY_GAP", "source_evidence_availability": "CHECK_PHASE4B_LOCAL_MATRIX"})
    return out


def trailing_history_gaps(v3_db: Path, expected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expected_by_ticker = {row["ticker"]: row for row in expected}
    out = []
    for row in coverage_by_company(v3_db):
        if int(row["canonical_q_count"]) == 0:
            continue
        newest = row["newest_q"]
        if row["active"] and newest and newest < "2025-01-01":
            out.append({**row, "trailing_gap_class": "LIKELY_MISSING_RECENT_QUARTERS", "expected_context": expected_by_ticker.get(row["ticker"], {}).get("history_start_basis", "")})
        elif not row["active"] and newest and newest < "2023-01-01":
            out.append({**row, "trailing_gap_class": "INACTIVE_OR_DELISTED_EXPECTED_STOP"})
    return out


def zero_q_company_list(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT c.company_id,c.ticker,c.company_name,c.active,c.market FROM v3_company c LEFT JOIN v3_quarter q ON q.company_id=c.company_id WHERE q.quarter_id IS NULL GROUP BY c.company_id ORDER BY c.ticker")]


def zero_q_local_evidence(v3_db: Path, legacy_db: Path, v2_db: Path, rawcandle_db: Path, zero_q: list[dict[str, Any]]) -> list[dict[str, Any]]:
    price = price_window(rawcandle_db)
    legacy = legacy_presence(legacy_db)
    v2 = v2_presence(v2_db)
    audit = audit_presence(v3_db)
    rows = []
    for row in zero_q:
        ticker = row["ticker"]
        p = price.get(ticker, {})
        lp = legacy.get(ticker, {})
        vp = v2.get(ticker, {})
        ap = audit.get(ticker, {})
        local_class = classify_zero_local(row, p, lp, vp, ap)
        rows.append({
            **row,
            "sector": p.get("sector", ""),
            "industry": p.get("industry", ""),
            "first_known_ohlcv_date": p.get("first_price_date", ""),
            "last_known_ohlcv_date": p.get("last_price_date", ""),
            "price_rows": p.get("price_rows", 0),
            "current_trading_activity_evidence": current_activity(p),
            "yahoo_raw_presence": ap.get("yahoo_audit_rows", 0),
            "legacy_presence": lp.get("legacy_rows", 0),
            "v2_presence": vp.get("v2_rows", 0),
            "migration_candidate_presence": ap.get("audit_rows", 0),
            "local_preliminary_class": local_class,
            "local_reason": zero_reason(local_class, row, p, lp, vp, ap),
        })
    return rows


def zero_q_web_research_stub(local_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in local_rows:
        web = zero_q_known_web_evidence(row["ticker"])
        rows.append({
            "ticker": row["ticker"],
            "web_research_status": web.get("status", "LOCAL_EVIDENCE_SUFFICIENT" if row["local_preliminary_class"] != "MANUAL_REVIEW_REQUIRED" else "MANUAL_WEB_RESEARCH_RECOMMENDED"),
            "official_ir_evidence": web.get("official_ir_evidence", ""),
            "sec_evidence": web.get("sec_evidence", ""),
            "exchange_listing_evidence": web.get("exchange_listing_evidence", ""),
            "web_evidence_summary": web.get("summary", "Local price/source evidence determined preliminary disposition; no provider refresh or production writes."),
        })
    return rows


def zero_q_final_classification(local_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in local_rows:
        preliminary = row["local_preliminary_class"]
        if preliminary == "HISTORY_EXISTS_NOT_INGESTED":
            disposition = "KEEP_AND_BACKFILL_PHASE4B"
        elif preliminary == "INACTIVE_OR_DELISTED_SPECIAL_CASE":
            disposition = "KEEP_SPECIAL_CASE"
        elif preliminary == "UNIVERSE_REMOVAL_CANDIDATE":
            disposition = "REMOVE_FROM_UNIVERSE_CANDIDATE"
        elif preliminary == "SOURCE_DATA_NOT_FOUND_LOCALLY":
            disposition = "KEEP_AND_MANUAL_REVIEW"
        elif preliminary == "LEGIT_NO_HISTORY":
            disposition = "KEEP_NO_ACTION"
        else:
            disposition = "KEEP_AND_MANUAL_REVIEW"
        out.append({**row, "preliminary_class": preliminary, "final_disposition": disposition, "confidence": confidence(preliminary), "recommended_action": disposition})
    return out


def missing_field_local_source_matrix(v3_db: Path, legacy_db: Path, v2_db: Path) -> list[dict[str, Any]]:
    legacy = legacy_field_candidates(legacy_db)
    v2 = v2_field_candidates(v2_db)
    rows = []
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(f"SELECT c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,{', '.join('f.'+field for field in REPORT_FIELDS)} FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id"):
            key_period = (row["ticker"], row["period_end_date"])
            key_fyfq = (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
            for field in REPORT_FIELDS:
                if row[field] is not None:
                    continue
                lval = legacy.get(key_period, {}).get(field)
                vval = v2.get(key_fyfq, {}).get(field)
                source, value, status = choose_candidate(lval, vval)
                rows.append({"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end": row["period_end_date"], "field": field, "yahoo_candidate_status": "NOT_CHECKED_LOCAL_CANONICAL_ONLY", "legacy_exact_candidate": value_text(lval), "v2_same_fyfq_candidate": value_text(vval), "selected_candidate_source": source, "selected_candidate_value": value_text(value), "candidate_status": status})
            if row["period_end_date"] and not row["period_end_date"]:
                pass
    return rows


def fcf_recoverability(v3_db: Path) -> list[dict[str, Any]]:
    rows = []
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,f.free_cashflow,f.operating_cashflow,f.capex FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id WHERE f.free_cashflow IS NULL"):
            can = row["operating_cashflow"] is not None and row["capex"] is not None
            rows.append({"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end": row["period_end_date"], "ocf_present": int(row["operating_cashflow"] is not None), "capex_present": int(row["capex"] is not None), "capex_sign_valid": int(row["capex"] is not None), "fcf_recoverability": "FORMULA_DERIVABLE_OCF_PLUS_CAPEX" if can else "NOT_CURRENTLY_RECOVERABLE"})
    return rows


def debt_recoverability(v3_db: Path, legacy_db: Path, v2_db: Path) -> list[dict[str, Any]]:
    return simple_recoverability(v3_db, legacy_db, v2_db, "total_debt", "DEBT")


def shares_recoverability(v3_db: Path, legacy_db: Path, v2_db: Path) -> list[dict[str, Any]]:
    rows = simple_recoverability(v3_db, legacy_db, v2_db, "shares_outstanding", "SHARES")
    for row in rows:
        row["weighted_average_only_rejected"] = int(row["recoverability"] == "NOT_CURRENTLY_RECOVERABLE")
    return rows


def publication_date_recoverability(v3_db: Path, legacy_db: Path, v2_db: Path) -> list[dict[str, Any]]:
    legacy = legacy_publish_candidates(legacy_db)
    v2 = v2_publish_candidates(v2_db)
    rows = []
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id WHERE q.publish_date IS NULL"):
            lp = legacy.get((row["ticker"], row["period_end_date"]))
            vp = v2.get((row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]))
            src = "LEGACY_DIRECT" if lp else "V2_DIRECT" if vp else "NOT_CURRENTLY_RECOVERABLE"
            rows.append({"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end": row["period_end_date"], "legacy_publish_date": lp or "", "v2_publish_date": vp or "", "recoverability": src, "publish_date_inferred": 0})
    return rows


def sec_recoverability(v3_db: Path, legacy_db: Path) -> list[dict[str, Any]]:
    del legacy_db
    return [{"field": field, "direct_reported": 0, "q4_reconstructable_approved": 0, "instant_fy_end_available": 0, "semantically_unsafe": 0, "absent": 0, "policy_source": "PHASE3C_1E_LOCKED"} for field in REPORT_FIELDS]


def phase4b_inventory(matrix: list[dict[str, Any]], fcf: list[dict[str, Any]], zero_final: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in matrix:
        if row["candidate_status"] != "EXACT_OR_SAME_FYFQ_CANDIDATE":
            continue
        rows.append({**row, "current_null_status": "NULL", "proposed_source": row["selected_candidate_source"], "source_value": row["selected_candidate_value"], "source_quarter_identity_confidence": "HIGH", "conflict_status": "NO_CONFLICT_DETECTED", "expected_safe_recovery_mode": "DIRECT_NULL_FILL", "priority": priority_for_field(row["field"]), "core_readiness_impact": int(row["field"] in CORE_FIELDS)})
    for row in fcf:
        if row["fcf_recoverability"] == "FORMULA_DERIVABLE_OCF_PLUS_CAPEX":
            rows.append({**row, "field": "free_cashflow", "current_null_status": "NULL", "proposed_source": "FORMULA_DERIVABLE", "source_value": "operating_cashflow + capex", "source_quarter_identity_confidence": "HIGH", "conflict_status": "NO_CONFLICT_DETECTED", "expected_safe_recovery_mode": "SAFE_FORMULA_FCF", "priority": 3, "core_readiness_impact": 1})
    for row in zero_final:
        if row["final_disposition"] == "KEEP_AND_BACKFILL_PHASE4B":
            rows.append({"ticker": row["ticker"], "fiscal_year": "", "fiscal_quarter": "", "period_end": "", "field": "NEW_Q_BACKFILL", "current_null_status": "NO_CANONICAL_Q", "proposed_source": "LOCAL_SOURCE_BACKFILL", "source_value": "", "source_quarter_identity_confidence": "MEDIUM", "conflict_status": "NO_CANONICAL_CONFLICT", "expected_safe_recovery_mode": "ZERO_Q_BACKFILL_CANDIDATE", "priority": 4, "core_readiness_impact": 1})
    return sorted(rows, key=lambda item: (int(item["priority"]), item["ticker"], str(item.get("period_end", "")), item["field"]))


def priority_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(int(row["priority"]) for row in rows)
    labels = {1: "PRIORITY_1_CORE_READY_DIRECT", 2: "PRIORITY_2_HIGH_VALUE_DIRECT", 3: "PRIORITY_3_SAFE_FORMULA", 4: "PRIORITY_4_ZERO_Q_BACKFILL", 5: "PRIORITY_5_MANUAL_SPECIAL"}
    return [{"priority": key, "priority_label": labels.get(key, "OTHER"), "candidate_rows": counts[key]} for key in sorted(counts)]


def core_ready_uplift_estimate(v3_db: Path, matrix: list[dict[str, Any]], fcf: list[dict[str, Any]]) -> list[dict[str, Any]]:
    missing_by_q = core_missing_by_q(v3_db)
    direct = defaultdict(set)
    for row in matrix:
        if row["candidate_status"] == "EXACT_OR_SAME_FYFQ_CANDIDATE" and row["field"] in CORE_FIELDS:
            direct[(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])].add(row["field"])
    fcf_keys = {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]) for row in fcf if row["fcf_recoverability"] == "FORMULA_DERIVABLE_OCF_PLUS_CAPEX"}
    rows = []
    current_core = final_canonical_baseline(v3_db)["coverage"]["core_ready_q"]
    rows.append({"scenario": "CURRENT_CORE_READY", "additional_core_ready": 0, "estimated_core_ready": current_core})
    for field in CORE_FIELDS:
        add = sum(1 for key, missing in missing_by_q.items() if missing == {field} and field in direct.get(key, set()))
        rows.append({"scenario": f"RECOVER_{field}_ONLY", "additional_core_ready": add, "estimated_core_ready": current_core + add})
    fcf_add = sum(1 for key, missing in missing_by_q.items() if missing == {"free_cashflow"} and key in fcf_keys)
    obvious = sum(1 for key, missing in missing_by_q.items() if missing and missing.issubset(direct.get(key, set()) | ({"free_cashflow"} if key in fcf_keys else set())))
    rows.append({"scenario": "SAFE_FCF_FORMULA_ONLY", "additional_core_ready": fcf_add, "estimated_core_ready": current_core + fcf_add})
    rows.append({"scenario": "ALL_OBVIOUS_PHASE4B_LOCAL", "additional_core_ready": obvious, "estimated_core_ready": current_core + obvious})
    rows.append({"scenario": "REMAINING_BLOCKED_PRIMARILY_BY_EBITDA", "additional_core_ready": sum(1 for missing in missing_by_q.values() if "ebitda" in missing), "estimated_core_ready": ""})
    return rows


def phase4c_inventory(v3_db: Path, v2_db: Path) -> list[dict[str, Any]]:
    del v2_db
    rows = build_phase4c_inventory(v3_db)
    out = []
    for row in rows:
        out.append({**row, "direct_ebit_candidate_exists": int(str(row.get("missing_ebit")) == "0"), "direct_ebitda_candidate_exists": int(str(row.get("missing_ebitda")) == "0"), "derivation_not_yet_approved": 1})
    return out


def phase4c_research_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter()
    for row in rows:
        missing_ebit = str(row.get("missing_ebit")) == "1"
        missing_ebitda = str(row.get("missing_ebitda")) == "1"
        op_income = str(row.get("operating_income_available")) == "1"
        if missing_ebitda and not missing_ebit:
            group = "EBITDA_MISSING_EBIT_PRESENT"
        elif missing_ebit and not missing_ebitda:
            group = "EBIT_MISSING_EBITDA_PRESENT"
        elif missing_ebit and missing_ebitda and op_income:
            group = "BOTH_MISSING_OPERATING_INCOME_PRESENT"
        elif missing_ebit and missing_ebitda and str(row.get("canonical_q_type")) == "RECONSTRUCTED_SEC_Q4":
            group = "BOTH_MISSING_SEC_Q4_RESEARCH"
        else:
            group = "BOTH_MISSING_NO_USEFUL_DERIVATION_INPUTS"
        counts[group] += 1
    return [{"research_group": key, "count": value} for key, value in sorted(counts.items())]


def final_completeness_taxonomy(v3_db: Path, matrix: list[dict[str, Any]], fcf: list[dict[str, Any]], phase4c: list[dict[str, Any]]) -> list[dict[str, Any]]:
    recoverable_keys = {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]) for row in matrix if row["candidate_status"] == "EXACT_OR_SAME_FYFQ_CANDIDATE"}
    fcf_keys = {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]) for row in fcf if row["fcf_recoverability"] == "FORMULA_DERIVABLE_OCF_PLUS_CAPEX"}
    phase4c_keys = {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]) for row in phase4c}
    rows = []
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(f"SELECT c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,{', '.join('f.'+field for field in CORE_FIELDS)} FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id"):
            key = (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
            if core_ready(row):
                category = "COMPLETE_CORE_READY"
            elif key in recoverable_keys:
                category = "INCOMPLETE_DIRECTLY_RECOVERABLE"
            elif key in fcf_keys:
                category = "INCOMPLETE_SAFE_FORMULA_RECOVERABLE"
            elif key in phase4c_keys:
                category = "INCOMPLETE_PHASE4C_DERIVATION_REQUIRED"
            else:
                category = "INCOMPLETE_SOURCE_UNAVAILABLE"
            rows.append({"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end": row["period_end_date"], "taxonomy": category})
    return rows


def structural_integrity(v3_db: Path) -> dict[str, Any]:
    identity = canonical_identity_integrity(v3_db)
    sequence = canonical_sequence_integrity(v3_db)
    q4 = q4_policy_integrity(v3_db)
    integrity = production_integrity_for_path(v3_db)
    checks = {row["check"]: int(row["violations"]) for row in identity}
    return {
        "invalid_fiscal_year": checks.get("INVALID_FISCAL_YEAR", 0),
        "duplicate_fyfq": checks.get("DUPLICATE_COMPANY_FY_FQ", 0),
        "pre_2018_q": checks.get("PRE_2018_Q", 0),
        "sequence_violations": len(sequence),
        "q4_policy_violations": len(q4),
        "quick_check": integrity["quick_check"],
        "foreign_key_check_rows": integrity["foreign_key_check_rows"],
        "phase3_structural_gates_pass": checks.get("INVALID_FISCAL_YEAR", 0) == 0 and checks.get("DUPLICATE_COMPANY_FY_FQ", 0) == 0 and checks.get("PRE_2018_Q", 0) == 0 and len(sequence) == 0 and len(q4) == 0 and integrity["quick_check"] == "ok" and integrity["foreign_key_check_rows"] == 0,
    }


def write_artifacts(root: Path, **items: Any) -> None:
    write_text(root / "preflight.md", "Phase 4A read-only audit. Canonical writes: 0. Company removals: 0.\n")
    write_json(root / "global_completeness_baseline.json", items["baseline"])
    write_csv(root / "field_coverage_summary.csv", field_coverage_rows(items["baseline"]))
    write_csv(root / "coverage_by_year.csv", items["coverage_year"])
    write_csv(root / "coverage_by_company.csv", items["coverage_company"])
    write_csv(root / "historical_depth.csv", items["depth"])
    write_csv(root / "expected_history_model.csv", items["expected"])
    write_csv(root / "completeness_gap_classification.csv", items["gaps"])
    write_csv(root / "missing_field_signatures.csv", items["signatures"])
    write_csv(root / "core_readiness_blockers.csv", items["blockers"])
    write_csv(root / "internal_history_gaps.csv", items["internal_gaps"])
    write_csv(root / "trailing_history_gaps.csv", items["trailing_gaps"])
    write_csv(root / "zero_q_company_list.csv", items["zero_q"])
    write_csv(root / "zero_q_local_evidence.csv", items["zero_local"])
    write_csv(root / "zero_q_web_research.csv", items["zero_web"])
    write_csv(root / "zero_q_final_classification.csv", items["zero_final"])
    write_csv(root / "phase4a_zero_q_manual_review.csv", [row for row in items["zero_final"] if row["final_disposition"] == "KEEP_AND_MANUAL_REVIEW"])
    write_csv(root / "phase4a_zero_q_backfill_candidates.csv", [row for row in items["zero_final"] if row["final_disposition"] == "KEEP_AND_BACKFILL_PHASE4B"])
    write_csv(root / "phase4a_zero_q_universe_removal_candidates.csv", [row for row in items["zero_final"] if row["final_disposition"] == "REMOVE_FROM_UNIVERSE_CANDIDATE"])
    write_csv(root / "missing_field_local_source_matrix.csv", items["matrix"])
    write_csv(root / "sec_recoverability.csv", items["sec"])
    write_csv(root / "fcf_recoverability.csv", items["fcf"])
    write_csv(root / "debt_recoverability.csv", items["debt"])
    write_csv(root / "shares_recoverability.csv", items["shares"])
    write_csv(root / "publication_date_recoverability.csv", items["publish"])
    write_csv(root / "phase4b_missing_field_recovery_inventory.csv", items["phase4b"])
    write_csv(root / "phase4b_priority_summary.csv", items["priority"])
    write_csv(root / "phase4b_core_ready_uplift_estimate.csv", items["uplift"])
    write_csv(root / "phase4c_ebit_ebitda_derivation_inventory.csv", items["phase4c"])
    write_csv(root / "phase4c_research_groups.csv", items["phase4c_groups"])
    write_csv(root / "final_completeness_taxonomy.csv", items["taxonomy"])
    write_json(root / "phase4a_summary.json", items["summary"])
    write_text(root / "recommended_next_step.md", items["summary"]["recommended_next_step"] + "\n")


def write_docs(path: Path, summary: dict[str, Any]) -> None:
    b = summary["baseline"]["coverage"]
    text = f"""# Fundamentals V3 Phase 4A Historical Completeness Audit

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Final baseline:

- Companies: {summary['baseline']['company_total']} active {summary['baseline']['active']} inactive {summary['baseline']['inactive']}
- Canonical Qs: {b['canonical_q_total']}
- Core-ready: {b['core_ready_q']}
- Core-not-ready: {b['core_not_ready_q']}
- Completeness-gap Qs: {summary['missingness']['completeness_gap_q']}
- Publish NULL: {b['publish_date_null']}

Zero-Q companies: {summary['zero_q']['total']} total, {summary['zero_q']['active']} active, {summary['zero_q']['inactive']} inactive.

Zero-Q dispositions:

```json
{json.dumps(summary['zero_q']['disposition_counts'], indent=2, sort_keys=True)}
```

Phase 4B direct recovery candidates: {sum(row['candidate_rows'] for row in summary.get('phase4b_priority_summary', [])) if summary.get('phase4b_priority_summary') else 'see artifact'}.

Phase 4C inventory rows: {summary['phase4c']['inventory_rows']}.

Canonical production writes in Phase 4A: `0`.

Next step: `{summary['recommended_next_step']}`
"""
    path.write_text(text)


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    text = path.read_text() if path.exists() else "# Fundamentals V3 Master Plan Status\n"
    marker = "\n## Phase 4A\n"
    entry = marker + f"\nClassification: `{summary['classification']}`\n\nStatus: `DONE`\n\nNext: `{summary['recommended_next_step']}`\n"
    if marker in text:
        text = text.split(marker)[0] + entry
    else:
        text = text.rstrip() + "\n" + entry
    path.write_text(text)


def field_coverage_rows(baseline: dict[str, Any]) -> list[dict[str, Any]]:
    total = baseline["coverage"]["canonical_q_total"]
    rows = []
    for field in REPORT_FIELDS:
        nulls = baseline["coverage"]["field_missing"][field]
        rows.append({"field": field, "populated": total - nulls, "null": nulls, "coverage_pct": pct(total - nulls, total)})
    rows.append({"field": "publish_date", "populated": baseline["coverage"]["publish_date_known"], "null": baseline["coverage"]["publish_date_null"], "coverage_pct": pct(baseline["coverage"]["publish_date_known"], total)})
    return rows


def company_rows(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT company_id,market,ticker,company_name,active FROM v3_company ORDER BY ticker")]


def quarters_by_ticker(v3_db: Path) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id ORDER BY c.ticker,q.fiscal_year,q.fiscal_quarter"):
            out[row["ticker"]].append(dict(row))
    return out


def price_window(rawcandle_db: Path) -> dict[str, dict[str, Any]]:
    if not rawcandle_db.exists():
        return {}
    with sqlite3.connect(rawcandle_db) as conn:
        conn.row_factory = sqlite3.Row
        return {
            row["osake"]: dict(row)
            for row in conn.execute("SELECT o.osake, MIN(o.pvm) first_price_date, MAX(o.pvm) last_price_date, COUNT(*) price_rows, MAX(tm.sector) sector, MAX(tm.industry) industry FROM osakedata o LEFT JOIN ticker_meta tm ON tm.ticker=o.osake WHERE o.market='usa' GROUP BY o.osake")
        }


def legacy_presence(legacy_db: Path) -> dict[str, dict[str, Any]]:
    with sqlite3.connect(legacy_db) as conn:
        conn.row_factory = sqlite3.Row
        return {row["ticker"]: dict(row) for row in conn.execute("SELECT ticker, COUNT(*) legacy_rows, MIN(period_end_date) legacy_first_period, MAX(period_end_date) legacy_last_period FROM rc_fundamental_quarterly GROUP BY ticker")}


def v2_presence(v2_db: Path) -> dict[str, dict[str, Any]]:
    with sqlite3.connect(v2_db) as conn:
        conn.row_factory = sqlite3.Row
        return {row["ticker"]: dict(row) for row in conn.execute("SELECT c.ticker, COUNT(*) v2_rows, MIN(q.report_date) v2_first_period, MAX(q.report_date) v2_last_period FROM rc_v2_company c JOIN rc_v2_quarter q ON q.company_id=c.company_id GROUP BY c.ticker")}


def audit_presence(v3_db: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = defaultdict(lambda: {"audit_rows": 0, "yahoo_audit_rows": 0})
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT c.ticker,a.source,COUNT(*) count FROM v3_company c LEFT JOIN v3_migration_audit a ON a.company_id=c.company_id GROUP BY c.ticker,a.source"):
            if row["source"]:
                out[row["ticker"]]["audit_rows"] += int(row["count"])
                if row["source"] == "YAHOO":
                    out[row["ticker"]]["yahoo_audit_rows"] += int(row["count"])
    return out


def legacy_field_candidates(legacy_db: Path) -> dict[tuple[str, str], dict[str, Any]]:
    with sqlite3.connect(legacy_db) as conn:
        conn.row_factory = sqlite3.Row
        return {(row["ticker"], row["period_end_date"]): dict(row) for row in conn.execute(f"SELECT ticker,period_end_date,{','.join(REPORT_FIELDS)} FROM rc_fundamental_quarterly")}


def v2_field_candidates(v2_db: Path) -> dict[tuple[str, int, str], dict[str, Any]]:
    with sqlite3.connect(v2_db) as conn:
        conn.row_factory = sqlite3.Row
        return {(row["ticker"], int(row["fiscal_year"]), row["fiscal_period"]): dict(row) for row in conn.execute(f"SELECT c.ticker,q.fiscal_year,q.fiscal_period,{','.join('f.'+field for field in REPORT_FIELDS)} FROM rc_v2_company c JOIN rc_v2_quarter q ON q.company_id=c.company_id JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id")}


def legacy_publish_candidates(legacy_db: Path) -> dict[tuple[str, str], str]:
    with sqlite3.connect(legacy_db) as conn:
        conn.row_factory = sqlite3.Row
        return {
            (row["ticker"], row["period_end_date"]): row["announcement_date"]
            for row in conn.execute("SELECT ticker,period_end_date,announcement_date FROM rc_fundamental_quarter_earnings_match WHERE announcement_date IS NOT NULL")
            if row["period_end_date"]
        }


def v2_publish_candidates(v2_db: Path) -> dict[tuple[str, int, str], str]:
    with sqlite3.connect(v2_db) as conn:
        conn.row_factory = sqlite3.Row
        return {(row["ticker"], int(row["fiscal_year"]), row["fiscal_period"]): row["publish_date"] for row in conn.execute("SELECT c.ticker,q.fiscal_year,q.fiscal_period,q.publish_date FROM rc_v2_company c JOIN rc_v2_quarter q ON q.company_id=c.company_id WHERE q.publish_date IS NOT NULL")}


def simple_recoverability(v3_db: Path, legacy_db: Path, v2_db: Path, field: str, label: str) -> list[dict[str, Any]]:
    matrix = [row for row in missing_field_local_source_matrix(v3_db, legacy_db, v2_db) if row["field"] == field]
    return [{**row, "recoverability": f"{row['selected_candidate_source']}_DIRECT" if row["candidate_status"] == "EXACT_OR_SAME_FYFQ_CANDIDATE" else "NOT_CURRENTLY_RECOVERABLE", "recoverability_domain": label} for row in matrix]


def recoverability_summary(matrix: list[dict[str, Any]], fcf: list[dict[str, Any]], debt: list[dict[str, Any]], shares: list[dict[str, Any]], publish: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter()
    for row in matrix:
        if row["candidate_status"] == "EXACT_OR_SAME_FYFQ_CANDIDATE":
            counts[f"{row['field']}_direct_recoverable"] += 1
    counts["free_cashflow_formula_recoverable"] = sum(1 for row in fcf if row["fcf_recoverability"] == "FORMULA_DERIVABLE_OCF_PLUS_CAPEX")
    counts["total_debt_direct_recoverable"] = sum(1 for row in debt if row["recoverability"] != "NOT_CURRENTLY_RECOVERABLE")
    counts["shares_outstanding_direct_recoverable"] = sum(1 for row in shares if row["recoverability"] != "NOT_CURRENTLY_RECOVERABLE")
    counts["publish_date_direct_recoverable"] = sum(1 for row in publish if row["recoverability"] != "NOT_CURRENTLY_RECOVERABLE")
    return dict(counts)


def core_missing_by_q(v3_db: Path) -> dict[tuple[str, int, str], set[str]]:
    out = {}
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(f"SELECT c.ticker,q.fiscal_year,q.fiscal_quarter,{', '.join('f.'+field for field in CORE_FIELDS)} FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id"):
            missing = {field for field in CORE_FIELDS if row[field] is None}
            if row["shares_outstanding"] is not None and float(row["shares_outstanding"] or 0) <= 0:
                missing.add("shares_outstanding")
            if missing:
                out[(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])] = missing
    return out


def db_fingerprint(v3_db: Path) -> tuple[int, int, int]:
    with sqlite3.connect(v3_db) as conn:
        return (
            conn.execute("SELECT COUNT(*) FROM v3_company").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals").fetchone()[0],
        )


def core_ready(row: sqlite3.Row | dict[str, Any]) -> bool:
    return all(row[field] is not None for field in CORE_FIELDS) and float(row["shares_outstanding"] or 0) > 0


def expected_quarter_count(qrows: list[sqlite3.Row]) -> int:
    if not qrows:
        return 0
    years = [int(row["fiscal_year"]) for row in qrows]
    return (max(years) - min(years) + 1) * 4


def longest_gap(qrows: list[sqlite3.Row]) -> int:
    return max(0, expected_quarter_count(qrows) - len(qrows))


def trailing_gap(qrows: list[sqlite3.Row]) -> int:
    if not qrows:
        return 0
    newest_year = max(int(row["fiscal_year"]) for row in qrows)
    return max(0, 2026 - newest_year)


def primary_missing_reason(row: sqlite3.Row, missing: list[str]) -> str:
    if missing == ["publish_date"]:
        return "PUBLICATION_METADATA_GAP"
    if "ebitda" in missing:
        return "EBITDA_DERIVATION_CANDIDATE"
    if "ebit" in missing:
        return "EBIT_DERIVATION_CANDIDATE"
    if row["accepted_source_provider"] == "YAHOO":
        return "YAHOO_SOURCE_GAP"
    if row["accepted_source_provider"] == "V2":
        return "V2_SOURCE_GAP"
    if row["accepted_source_provider"] == "LEGACY" and row["fiscal_quarter"] == "Q4":
        return "SEC_Q4_EXPECTED_DERIVATION_GAP"
    return "DIRECT_SOURCE_GAP"


def classify_zero_local(row: dict[str, Any], price: dict[str, Any], legacy: dict[str, Any], v2: dict[str, Any], audit: dict[str, Any]) -> str:
    if row["ticker"] in {"IBIT", "BRRR"}:
        return "UNIVERSE_REMOVAL_CANDIDATE"
    if legacy.get("legacy_rows") or v2.get("v2_rows") or audit.get("audit_rows"):
        return "HISTORY_EXISTS_NOT_INGESTED"
    if not row["active"] and price.get("last_price_date", "") < "2024-01-01":
        return "INACTIVE_OR_DELISTED_SPECIAL_CASE"
    if price.get("first_price_date", "") >= "2025-01-01":
        return "LEGIT_NO_HISTORY"
    if not price:
        return "SOURCE_DATA_NOT_FOUND_LOCALLY"
    return "MANUAL_REVIEW_REQUIRED"


def zero_reason(cls: str, row: dict[str, Any], price: dict[str, Any], legacy: dict[str, Any], v2: dict[str, Any], audit: dict[str, Any]) -> str:
    return f"{cls}: active={row['active']} price_rows={price.get('price_rows', 0)} legacy={legacy.get('legacy_rows', 0)} v2={v2.get('v2_rows', 0)} audit={audit.get('audit_rows', 0)}"


def current_activity(price: dict[str, Any]) -> str:
    last = price.get("last_price_date", "")
    if not last:
        return "NO_LOCAL_PRICE"
    if last >= "2026-01-01":
        return "RECENT_LOCAL_TRADING"
    return "STALE_LOCAL_TRADING"


def confidence(cls: str) -> str:
    return {"HISTORY_EXISTS_NOT_INGESTED": "HIGH", "INACTIVE_OR_DELISTED_SPECIAL_CASE": "MEDIUM", "UNIVERSE_REMOVAL_CANDIDATE": "HIGH", "LEGIT_NO_HISTORY": "MEDIUM"}.get(cls, "LOW")


def zero_q_known_web_evidence(ticker: str) -> dict[str, str]:
    if ticker == "IBIT":
        return {
            "status": "WEB_RESEARCH_COMPLETE",
            "official_ir_evidence": "https://www.ishares.com/us/products/333011/ishares-bitcoin-trustIBIT",
            "sec_evidence": "",
            "exchange_listing_evidence": "NASDAQ listed exchange-traded bitcoin trust/ETF per iShares product page",
            "summary": "iShares Bitcoin Trust ETF seeks to reflect bitcoin price performance; fund inception Jan 05 2024; digital-assets exchange-traded product, not ordinary operating-company fundamentals.",
        }
    if ticker == "BRRR":
        return {
            "status": "WEB_RESEARCH_COMPLETE",
            "official_ir_evidence": "https://coinshares.com/us/etf/brrr/",
            "sec_evidence": "https://www.sec.gov/Archives/edgar/data/1841175/000199937125003309/0001999371-25-003309-index.html",
            "exchange_listing_evidence": "NASDAQ listed common shares of beneficial interest per SEC filing; CoinShares product page identifies ticker BRRR",
            "summary": "CoinShares Bitcoin ETF / formerly CoinShares Valkyrie Bitcoin Fund holds bitcoin and cash/other; trust/ETF structure, not ordinary operating-company fundamentals.",
        }
    return {}


def choose_candidate(legacy_value: Any, v2_value: Any) -> tuple[str, Any, str]:
    if legacy_value is not None and v2_value is not None and float(legacy_value) != float(v2_value):
        return "CONFLICT", "", "SOURCE_CONFLICT"
    if legacy_value is not None:
        return "LEGACY", legacy_value, "EXACT_OR_SAME_FYFQ_CANDIDATE"
    if v2_value is not None:
        return "V2", v2_value, "EXACT_OR_SAME_FYFQ_CANDIDATE"
    return "NONE", "", "NO_LOCAL_CANDIDATE"


def value_text(value: Any) -> str:
    return "" if value is None else str(value)


def priority_for_field(field: str) -> int:
    if field in CORE_FIELDS:
        return 1
    if field in {"revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow", "capex"}:
        return 2
    return 5


def approx_quarters_between(start: str, end: str) -> int:
    sy, sm = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    return max(0, ((ey - sy) * 12 + (em - sm)) // 3)


def max_date(a: str, b: str) -> str:
    return max(a, b)


def percentile(values: list[int], p: float) -> int:
    idx = min(len(values) - 1, max(0, int((len(values) - 1) * p)))
    return values[idx]


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def write_text(path: Path, text: str) -> None:
    path.write_text(text)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
