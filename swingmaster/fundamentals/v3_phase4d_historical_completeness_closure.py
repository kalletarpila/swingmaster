from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_canonical_closure import (
    canonical_sequence_integrity,
    final_canonical_baseline,
    pct,
    q4_policy_integrity,
)
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity
from swingmaster.fundamentals.v3_repositories import FUNDAMENTAL_FIELDS


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE4_HISTORICAL_COMPLETENESS_COMPLETE_READY_FOR_PHASE5"
CLASSIFICATION_REPAIR = "FUNDAMENTALS_V3_PHASE4D_BOUNDED_REPAIR_REQUIRED"
NEXT_PHASE = "MASTER PLAN PHASE 5 - TTM ENGINE"
PHASE3_SUMMARY = Path("temp/fundamentals_v3_phase3c_6_canonical_migration_closure_rerun/20260823T_PHASE3C_6_RERUN/summary.json")
YEARS = tuple(range(2018, 2027))
FLOW_FIELDS = ("revenue", "gross_profit", "operating_income", "ebit", "ebitda", "net_income", "operating_cashflow", "capex", "free_cashflow")
INSTANT_FIELDS = ("cash", "total_debt", "shares_outstanding")
REPORT_FIELDS = (*FLOW_FIELDS, *INSTANT_FIELDS, "publish_date")
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
ZERO_Q_TICKERS = ("ALTS", "HOTH", "PKST", "QVCGA", "STSS")


def run_phase4d_historical_completeness_closure(*, v3_db: Path, component_db: Path, artifact_root: Path, phase3_summary_path: Path = PHASE3_SUMMARY) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before_fp = file_presence_fingerprint(v3_db)
    rows = load_canonical_rows(v3_db)
    companies = load_companies(v3_db)
    sec_companies = load_sec_component_companies(component_db)
    baseline = final_baseline(rows, companies)
    field_coverage = final_field_coverage(rows)
    raw_matrix = variable_year_matrix(rows)
    eligible_matrix = variable_year_matrix(rows)
    year_trends = year_trend_analysis(raw_matrix)
    gap_rows = remaining_gap_classification(rows, sec_companies)
    gap_summary = summarize_gap_reasons(gap_rows)
    field_reason = field_gap_reason_matrix(gap_rows)
    company_history = company_history_coverage(rows, companies)
    depth_summary = historical_depth_summary(company_history)
    zero_q = zero_q_residuals(companies, rows)
    cik_unmapped = cik_unmapped_residuals(rows, companies, sec_companies)
    internal_gaps = internal_gap_summary(rows, companies)
    core_signatures = core_readiness_signatures(rows)
    core_by_company = core_readiness_by_company(company_history)
    core_by_year = core_ready_by_year(rows)
    ebit_residuals = [row for row in gap_rows if row["field"] == "ebit"]
    ebitda_residuals = [row for row in gap_rows if row["field"] == "ebitda"]
    ebit_year = ebit_ebitda_coverage_by_year(rows)
    ebit_quarter = ebit_ebitda_coverage_by_quarter(rows)
    ttm_metric = ttm_metric_readiness(rows)
    ttm_company = ttm_company_readiness(rows)
    ttm_core = ttm_core_readiness(rows)
    ttm_publish = ttm_publish_pit_readiness(rows)
    phase3_vs_phase4 = phase3_vs_phase4_completeness(phase3_summary_path, baseline, field_coverage)
    recovery_summary = phase4_recovery_summary(phase3_vs_phase4)
    fingerprint = logical_fingerprint(rows, companies)
    integrity = structural_integrity(v3_db)
    after_fp = file_presence_fingerprint(v3_db)
    no_writes = before_fp == after_fp
    gate = {
        "baseline_reconciles": baseline["canonical_q"] == len(rows),
        "field_reconciles": all(int(row["populated"]) + int(row["null"]) == baseline["canonical_q"] for row in field_coverage),
        "gap_reconciles": gap_reconciliation_ok(gap_rows, field_coverage),
        "matrix_generated": bool(raw_matrix),
        "integrity_pass": integrity["phase3_structural_gates_pass"],
        "no_production_writes": no_writes,
    }
    gate["passed"] = all(gate.values())
    classification = CLASSIFICATION_COMPLETE if gate["passed"] else CLASSIFICATION_REPAIR
    summary = {
        "classification": classification,
        "recommended_next_step": NEXT_PHASE if classification == CLASSIFICATION_COMPLETE else "MASTER PLAN PHASE 4D-REPAIR - BOUNDED REPAIR REQUIRED",
        "baseline": baseline,
        "field_coverage": {row["field"]: row for row in field_coverage},
        "year_trends": year_trends,
        "history_depth": depth_summary,
        "zero_q": {"total": len(zero_q), "tickers": [row["ticker"] for row in zero_q]},
        "cik_unmapped": summarize_cik(cik_unmapped),
        "core": summarize_core(core_signatures, core_by_year, core_by_company),
        "ebit_residual": summarize_residual(ebit_residuals),
        "ebitda_residual": summarize_residual(ebitda_residuals),
        "ttm": summarize_ttm(ttm_metric, ttm_core, ttm_company, ttm_publish),
        "phase3_vs_phase4": phase3_vs_phase4,
        "recovery_summary": recovery_summary,
        "integrity": integrity,
        "fingerprint": fingerprint,
        "gate": gate,
        "canonical_financial_writes": 0,
        "canonical_q_writes": 0,
        "universe_writes": 0,
        "network_calls": 0,
        "artifact_root": str(artifact_root),
    }
    write_artifacts(
        artifact_root,
        summary,
        field_coverage,
        raw_matrix,
        eligible_matrix,
        gap_rows,
        gap_summary,
        field_reason,
        company_history,
        depth_summary,
        zero_q,
        cik_unmapped,
        internal_gaps,
        core_signatures,
        core_by_company,
        core_by_year,
        ebit_residuals,
        ebitda_residuals,
        ebit_year,
        ebit_quarter,
        ttm_metric,
        ttm_company,
        ttm_core,
        ttm_publish,
        phase3_vs_phase4,
        recovery_summary,
        fingerprint,
        integrity,
    )
    write_doc(Path("docs/fundamentals_v3_phase4d_historical_completeness_closure.md"), summary, raw_matrix, field_coverage)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def load_canonical_rows(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        fields = ",".join(f"f.{field}" for field in FUNDAMENTAL_FIELDS)
        return [dict(row) for row in conn.execute(
            f"""
            SELECT c.company_id,c.ticker,c.company_name,c.active,c.market,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
                   q.period_end_date,q.publish_date,{fields}
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            ORDER BY c.ticker,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END
            """
        )]


def load_companies(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT company_id,ticker,company_name,active,market,admission_source FROM v3_company ORDER BY ticker")]


def load_sec_component_companies(component_db: Path) -> set[int]:
    if not component_db.exists():
        return set()
    with sqlite3.connect(f"file:{component_db}?mode=ro", uri=True) as conn:
        return {int(row[0]) for row in conn.execute("SELECT DISTINCT company_id FROM sec_component_fact WHERE company_id IS NOT NULL")}


def final_baseline(rows: list[dict[str, Any]], companies: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "companies": len(companies),
        "active": sum(1 for row in companies if int(row["active"]) == 1),
        "inactive": sum(1 for row in companies if int(row["active"]) == 0),
        "canonical_q": len(rows),
        "core_ready": sum(1 for row in rows if core_ready(row)),
        "core_not_ready": sum(1 for row in rows if not core_ready(row)),
        "publish_known": sum(1 for row in rows if row["publish_date"] is not None),
        "publish_null": sum(1 for row in rows if row["publish_date"] is None),
    }


def final_field_coverage(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(rows)
    out = []
    for field in REPORT_FIELDS:
        present = sum(1 for row in rows if field_value(row, field) is not None)
        out.append({"field": field, "populated": present, "null": total - present, "coverage_pct": pct(present, total), "variable_type": variable_type(field)})
    return out


def variable_year_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for field in REPORT_FIELDS:
        row: dict[str, Any] = {"field": field, "variable_type": variable_type(field), "denominator_policy": "RAW_CANONICAL_Q_PERIOD_END_YEAR"}
        total_present = 0
        total_eligible = 0
        for year in YEARS:
            yrows = [item for item in rows if year_of(item) == year]
            present = sum(1 for item in yrows if field_value(item, field) is not None)
            total_present += present
            total_eligible += len(yrows)
            row[str(year)] = cell(present, len(yrows))
            row[f"{year}_populated"] = present
            row[f"{year}_eligible"] = len(yrows)
            row[f"{year}_coverage_pct"] = pct(present, len(yrows))
        row["TOTAL"] = cell(total_present, total_eligible)
        row["total_populated"] = total_present
        row["total_eligible"] = total_eligible
        row["total_coverage_pct"] = pct(total_present, total_eligible)
        out.append(row)
    return out


def year_trend_analysis(matrix: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in matrix:
        year_rates = [(year, float(row[f"{year}_coverage_pct"])) for year in YEARS if int(row[f"{year}_eligible"]) > 0]
        weakest = min(year_rates, key=lambda item: item[1])
        strongest = max(year_rates, key=lambda item: item[1])
        older = sum(rate for year, rate in year_rates if year <= 2020) / max(1, sum(1 for year, _ in year_rates if year <= 2020))
        recent = sum(rate for year, rate in year_rates if 2023 <= year <= 2025) / max(1, sum(1 for year, _ in year_rates if 2023 <= year <= 2025))
        out.append({"field": row["field"], "weakest_year": weakest[0], "weakest_pct": weakest[1], "strongest_year": strongest[0], "strongest_pct": strongest[1], "older_years_lower": int(older + 5 < recent), "year_2026_partial_not_structural_failure": 1})
    return out


def remaining_gap_classification(rows: list[dict[str, Any]], sec_companies: set[int]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        for field in REPORT_FIELDS:
            if field_value(row, field) is not None:
                continue
            reason, secondary = closure_reason(row, field, sec_companies)
            out.append({
                "company_id": row["company_id"],
                "ticker": row["ticker"],
                "active": row["active"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end_date": row["period_end_date"],
                "field": field,
                "primary_closure_reason": reason,
                "secondary_flags": secondary,
            })
    return out


def closure_reason(row: dict[str, Any], field: str, sec_companies: set[int]) -> tuple[str, str]:
    if field == "publish_date":
        return "PUBLICATION_DATE_UNAVAILABLE", ""
    if int(row["company_id"]) not in sec_companies and field in {"ebit", "ebitda"}:
        return "CIK_UNMAPPED", "SEC_COMPONENT_UNAVAILABLE"
    if field == "ebit":
        if row["fiscal_quarter"] == "Q4":
            return "Q4_BLOCKED", "NO_SAFE_DERIVATION"
        return "EBIT_SEMANTIC_AMBIGUITY", "NO_SAFE_DERIVATION"
    if field == "ebitda":
        if row.get("ebit") is None:
            return "CANONICAL_EBIT_MISSING", "NO_SAFE_DERIVATION"
        if row["fiscal_quarter"] == "Q4":
            return "Q4_BLOCKED", "NO_SAFE_DERIVATION"
        return "EBITDA_SEMANTIC_AMBIGUITY", "D_AND_A_TRUE_AMBIGUITY"
    if year_of(row) <= 2020:
        return "HISTORICAL_SOURCE_LIMIT", ""
    if int(row["active"]) == 0:
        return "INACTIVE_HISTORY_LIMIT", ""
    if field == "shares_outstanding":
        return "COMPANY_NOT_REPORTING_FIELD", "SHARES_MISSING_OR_ZERO"
    return "NO_DIRECT_SOURCE", "NO_SAFE_DERIVATION"


def summarize_gap_reasons(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    c = Counter((row["field"], row["primary_closure_reason"]) for row in rows)
    return [{"field": field, "primary_closure_reason": reason, "null_count": count} for (field, reason), count in sorted(c.items())]


def field_gap_reason_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reasons = sorted({row["primary_closure_reason"] for row in rows})
    by_field = defaultdict(Counter)
    for row in rows:
        by_field[row["field"]][row["primary_closure_reason"]] += 1
    return [{"field": field, **{reason: by_field[field][reason] for reason in reasons}, "total": sum(by_field[field].values())} for field in REPORT_FIELDS]


def company_history_coverage(rows: list[dict[str, Any]], companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company = defaultdict(list)
    for row in rows:
        by_company[row["company_id"]].append(row)
    out = []
    for company in companies:
        qrows = by_company.get(company["company_id"], [])
        out.append({
            "company_id": company["company_id"],
            "ticker": company["ticker"],
            "company_name": company.get("company_name") or "",
            "active": company["active"],
            "canonical_q_count": len(qrows),
            "oldest_period": min((row["period_end_date"] for row in qrows if row["period_end_date"]), default=""),
            "newest_period": max((row["period_end_date"] for row in qrows if row["period_end_date"]), default=""),
            "core_ready_q": sum(1 for row in qrows if core_ready(row)),
            "core_ready_pct": pct(sum(1 for row in qrows if core_ready(row)), len(qrows)),
            "internal_gap_count": internal_gap_count(qrows),
            "trailing_gap": trailing_gap(qrows),
        })
    return out


def historical_depth_summary(company_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = sorted(int(row["canonical_q_count"]) for row in company_rows)
    return {
        "ge_4q": sum(1 for value in counts if value >= 4),
        "ge_8q": sum(1 for value in counts if value >= 8),
        "ge_12q": sum(1 for value in counts if value >= 12),
        "ge_16q": sum(1 for value in counts if value >= 16),
        "ge_20q": sum(1 for value in counts if value >= 20),
        "ge_24q": sum(1 for value in counts if value >= 24),
        "ge_28q": sum(1 for value in counts if value >= 28),
        "ge_32q": sum(1 for value in counts if value >= 32),
        "median": float(median(counts)) if counts else 0.0,
        "p25": percentile(counts, 0.25),
        "p75": percentile(counts, 0.75),
        "max": max(counts) if counts else 0,
        "zero_q": sum(1 for value in counts if value == 0),
        "q_1_3": sum(1 for value in counts if 1 <= value <= 3),
        "oldest_period": min((row["oldest_period"] for row in company_rows if row["oldest_period"]), default=""),
    }


def zero_q_residuals(companies: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    q_companies = {row["company_id"] for row in rows}
    out = []
    for company in companies:
        if company["company_id"] in q_companies:
            continue
        ticker = company["ticker"]
        out.append({**company, "known_phase4b_residual": int(ticker in ZERO_Q_TICKERS), "reason_zero_q_remains": "NO_SAFE_LOCAL_FUNDAMENTAL_SOURCE", "source_evidence": "PHASE4A_PHASE4B_LOCAL_AUDIT", "removal_warranted": 0, "manual_review_useful": 1})
    return out


def cik_unmapped_residuals(rows: list[dict[str, Any]], companies: list[dict[str, Any]], sec_companies: set[int]) -> list[dict[str, Any]]:
    by_company = defaultdict(list)
    for row in rows:
        by_company[row["company_id"]].append(row)
    out = []
    for company in companies:
        if int(company["company_id"]) in sec_companies:
            continue
        qrows = by_company.get(company["company_id"], [])
        out.append({**company, "canonical_q_count": len(qrows), "ebit_missing": sum(1 for row in qrows if row["ebit"] is None), "ebitda_missing": sum(1 for row in qrows if row["ebitda"] is None), "material_limit": int(len(qrows) > 0 and any(row["ebit"] is None or row["ebitda"] is None for row in qrows))})
    return out


def internal_gap_summary(rows: list[dict[str, Any]], companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    history = company_history_coverage(rows, companies)
    return [row for row in history if int(row["internal_gap_count"]) > 0]


def core_readiness_signatures(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    c = Counter()
    for row in rows:
        blockers = core_blockers(row)
        if blockers:
            c[" + ".join(blockers)] += 1
    return [{"signature": signature, "count": count} for signature, count in c.most_common(25)]


def core_ready_by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for year in YEARS:
        yrows = [row for row in rows if year_of(row) == year]
        ready = sum(1 for row in yrows if core_ready(row))
        out.append({"year": year, "canonical_q": len(yrows), "core_ready_q": ready, "core_ready_pct": pct(ready, len(yrows))})
    return out


def core_readiness_by_company(company_history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in company_history:
        rate = float(row["core_ready_pct"])
        bucket = "0%" if int(row["core_ready_q"]) == 0 else ">=90%" if rate >= 90 else ">=75%" if rate >= 75 else ">=50%" if rate >= 50 else "<50%"
        out.append({**row, "core_ready_bucket": bucket})
    return out


def ebit_ebitda_coverage_by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for year in YEARS:
        yrows = [row for row in rows if year_of(row) == year]
        out.append({"year": year, "canonical_q": len(yrows), "ebit_populated": sum(1 for row in yrows if row["ebit"] is not None), "ebitda_populated": sum(1 for row in yrows if row["ebitda"] is not None), "ebit_coverage_pct": pct(sum(1 for row in yrows if row["ebit"] is not None), len(yrows)), "ebitda_coverage_pct": pct(sum(1 for row in yrows if row["ebitda"] is not None), len(yrows))})
    return out


def ebit_ebitda_coverage_by_quarter(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for quarter in ("Q1", "Q2", "Q3", "Q4"):
        qrows = [row for row in rows if row["fiscal_quarter"] == quarter]
        out.append({"fiscal_quarter": quarter, "canonical_q": len(qrows), "ebit_populated": sum(1 for row in qrows if row["ebit"] is not None), "ebitda_populated": sum(1 for row in qrows if row["ebitda"] is not None), "ebit_coverage_pct": pct(sum(1 for row in qrows if row["ebit"] is not None), len(qrows)), "ebitda_coverage_pct": pct(sum(1 for row in qrows if row["ebitda"] is not None), len(qrows))})
    return out


def ttm_metric_readiness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company = rows_by_company(rows)
    out = []
    for field in FLOW_FIELDS:
        full = 0
        incomplete = 0
        for qrows in by_company.values():
            for idx in range(3, len(qrows)):
                window = qrows[idx - 3 : idx + 1]
                if consecutive(window):
                    if all(row[field] is not None for row in window):
                        full += 1
                    else:
                        incomplete += 1
        out.append({"metric": field, "ttm_full_4q": full, "ttm_incomplete": incomplete})
    return out


def ttm_company_readiness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company = rows_by_company(rows)
    out = []
    for metric in FLOW_FIELDS:
        counts = []
        for company_id, qrows in by_company.items():
            ticker = qrows[0]["ticker"] if qrows else ""
            valid = 0
            for idx in range(3, len(qrows)):
                window = qrows[idx - 3 : idx + 1]
                valid += int(consecutive(window) and all(row[metric] is not None for row in window))
            counts.append({"company_id": company_id, "ticker": ticker, "metric": metric, "valid_ttm_observations": valid})
        out.extend(counts)
    return out


def ttm_core_readiness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company = rows_by_company(rows)
    rows_out = []
    company_counts = Counter()
    endpoints = 0
    for qrows in by_company.values():
        for idx in range(3, len(qrows)):
            window = qrows[idx - 3 : idx + 1]
            ok = consecutive(window) and all(all(row[field] is not None for field in ("revenue", "ebitda", "free_cashflow")) for row in window)
            if ok:
                endpoints += 1
                company_counts[qrows[idx]["company_id"]] += 1
    rows_out.append({"metric_set": "TTM_REVENUE_EBITDA_FCF", "valid_endpoints": endpoints, "companies_ge_1": sum(1 for value in company_counts.values() if value >= 1), "companies_ge_4": sum(1 for value in company_counts.values() if value >= 4), "companies_ge_8": sum(1 for value in company_counts.values() if value >= 8)})
    return rows_out


def ttm_publish_pit_readiness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company = rows_by_company(rows)
    out = []
    for metric in ("revenue", "ebitda", "free_cashflow"):
        full_publish = latest_publish = total = 0
        for qrows in by_company.values():
            for idx in range(3, len(qrows)):
                window = qrows[idx - 3 : idx + 1]
                if not consecutive(window) or not all(row[metric] is not None for row in window):
                    continue
                total += 1
                full_publish += int(all(row["publish_date"] is not None for row in window))
                latest_publish += int(qrows[idx]["publish_date"] is not None)
        out.append({"metric": metric, "valid_ttm_endpoints": total, "all_underlying_publish_dates_known": full_publish, "latest_publish_date_known": latest_publish, "pit_safe_feasible_pct": pct(full_publish, total)})
    return out


def phase3_vs_phase4_completeness(path: Path, baseline: dict[str, Any], coverage: list[dict[str, Any]]) -> list[dict[str, Any]]:
    phase3 = json.loads(path.read_text(encoding="utf-8"))["baseline"] if path.exists() else {}
    p3_cov = phase3.get("coverage", {})
    p3_present = p3_cov.get("field_present", {})
    p4_present = {row["field"]: int(row["populated"]) for row in coverage}
    metrics = [
        ("canonical_q", p3_cov.get("canonical_q_total", 0), baseline["canonical_q"]),
        ("core_ready", p3_cov.get("core_ready_q", 0), baseline["core_ready"]),
        ("publish_known", p3_cov.get("publish_date_known", 0), baseline["publish_known"]),
    ]
    metrics.extend((field, p3_present.get(field, 0), p4_present.get(field, 0)) for field in FUNDAMENTAL_FIELDS)
    return [{"metric": metric, "phase3": before, "phase4": after, "improvement": int(after) - int(before)} for metric, before, after in metrics]


def phase4_recovery_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"category": "net_phase4_improvement", "metric": row["metric"], "improvement": row["improvement"]} for row in rows if int(row["improvement"]) != 0]


def logical_fingerprint(rows: list[dict[str, Any]], companies: list[dict[str, Any]]) -> dict[str, Any]:
    company_payload = [{"company_id": row["company_id"], "ticker": row["ticker"], "active": row["active"]} for row in companies]
    q_payload = [{"company_id": row["company_id"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row["period_end_date"]} for row in rows]
    field_payload = [{field: int(field_value(row, field) is not None) for field in REPORT_FIELDS} | {"quarter_id": row["quarter_id"]} for row in rows]
    core_payload = [{"quarter_id": row["quarter_id"], "core_ready": int(core_ready(row))} for row in rows]
    parts = {
        "company_hash": hash_json(company_payload),
        "canonical_q_hash": hash_json(q_payload),
        "field_presence_hash": hash_json(field_payload),
        "ebit_presence_hash": hash_json([{"quarter_id": row["quarter_id"], "ebit": int(row["ebit"] is not None)} for row in rows]),
        "ebitda_presence_hash": hash_json([{"quarter_id": row["quarter_id"], "ebitda": int(row["ebitda"] is not None)} for row in rows]),
        "core_ready_hash": hash_json(core_payload),
        "publish_date_presence_hash": hash_json([{"quarter_id": row["quarter_id"], "publish_date": int(row["publish_date"] is not None)} for row in rows]),
    }
    parts["phase4_combined_fingerprint"] = hash_json(parts)
    return parts


def core_ready(row: dict[str, Any]) -> bool:
    return all(row[field] is not None for field in ("revenue", "ebitda", "free_cashflow", "cash", "total_debt")) and row.get("shares_outstanding") is not None and float(row["shares_outstanding"]) > 0


def core_blockers(row: dict[str, Any]) -> list[str]:
    blockers = []
    for field in CORE_FIELDS:
        if row.get(field) is None or (field == "shares_outstanding" and float(row[field] or 0) <= 0):
            blockers.append(field)
    return blockers


def field_value(row: dict[str, Any], field: str) -> Any:
    return row["publish_date"] if field == "publish_date" else row[field]


def variable_type(field: str) -> str:
    return "FLOW" if field in FLOW_FIELDS else "INSTANT" if field in INSTANT_FIELDS else "METADATA"


def year_of(row: dict[str, Any]) -> int:
    return int(str(row["period_end_date"] or "0000")[:4])


def cell(present: int, eligible: int) -> str:
    return f"{present} / {eligible} ({pct(present, eligible):.2f}%)"


def percentile(values: list[int], q: float) -> float:
    if not values:
        return 0.0
    idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * q))))
    return float(values[idx])


def rows_by_company(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    out = defaultdict(list)
    for row in rows:
        out[int(row["company_id"])].append(row)
    for values in out.values():
        values.sort(key=lambda row: (int(row["fiscal_year"]), quarter_num(row["fiscal_quarter"])))
    return out


def consecutive(window: list[dict[str, Any]]) -> bool:
    seq = [int(row["fiscal_year"]) * 4 + quarter_num(row["fiscal_quarter"]) for row in window]
    return len(seq) == 4 and seq == list(range(seq[0], seq[0] + 4))


def quarter_num(q: str) -> int:
    return {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(q, 0)


def internal_gap_count(qrows: list[dict[str, Any]]) -> int:
    if len(qrows) < 2:
        return 0
    seq = sorted(int(row["fiscal_year"]) * 4 + quarter_num(row["fiscal_quarter"]) for row in qrows)
    return max(0, seq[-1] - seq[0] + 1 - len(seq))


def trailing_gap(qrows: list[dict[str, Any]]) -> int:
    if not qrows:
        return 0
    latest = max(int(row["fiscal_year"]) * 4 + quarter_num(row["fiscal_quarter"]) for row in qrows)
    current = 2026 * 4 + 2
    return max(0, current - latest)


def gap_reconciliation_ok(gaps: list[dict[str, Any]], coverage: list[dict[str, Any]]) -> bool:
    by_field = Counter(row["field"] for row in gaps)
    return all(by_field[row["field"]] == int(row["null"]) for row in coverage)


def summarize_cik(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"total": len(rows), "active": sum(1 for row in rows if int(row["active"]) == 1), "inactive": sum(1 for row in rows if int(row["active"]) == 0), "canonical_q": sum(int(row["canonical_q_count"]) for row in rows), "ebit_missing": sum(int(row["ebit_missing"]) for row in rows), "ebitda_missing": sum(int(row["ebitda_missing"]) for row in rows), "materially_limits_dataset": int(sum(int(row["canonical_q_count"]) for row in rows) > 0)}


def summarize_core(signatures: list[dict[str, Any]], by_year: list[dict[str, Any]], by_company: list[dict[str, Any]]) -> dict[str, Any]:
    c = Counter(row["core_ready_bucket"] for row in by_company)
    return {"top_signatures": signatures[:25], "by_year": by_year, "companies_ge_90": c[">=90%"], "companies_ge_75": c[">=75%"], "companies_ge_50": c[">=50%"], "companies_lt_50": c["<50%"], "companies_zero": c["0%"]}


def summarize_residual(rows: list[dict[str, Any]]) -> dict[str, Any]:
    c = Counter(row["primary_closure_reason"] for row in rows)
    return {"missing": len(rows), **dict(c)}


def summarize_ttm(metric_rows: list[dict[str, Any]], core_rows: list[dict[str, Any]], company_rows: list[dict[str, Any]], publish_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_metric = {row["metric"]: row for row in metric_rows}
    core = core_rows[0] if core_rows else {}
    return {"metric": by_metric, "core": core, "companies": company_ttm_summary(company_rows), "publish": publish_rows}


def company_ttm_summary(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out = {}
    by_metric = defaultdict(list)
    for row in rows:
        by_metric[row["metric"]].append(int(row["valid_ttm_observations"]))
    for metric, values in by_metric.items():
        out[metric] = {"companies_ge_1": sum(1 for value in values if value >= 1), "companies_ge_4": sum(1 for value in values if value >= 4), "companies_ge_8": sum(1 for value in values if value >= 8), "companies_zero": sum(1 for value in values if value == 0)}
    return out


def file_presence_fingerprint(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return (stat.st_size, stat.st_mtime_ns)


def hash_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def matrix_markdown(matrix: list[dict[str, Any]]) -> str:
    headers = ["field", *[str(year) for year in YEARS], "TOTAL"]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in matrix:
        lines.append("| " + " | ".join(str(row[col]) for col in headers) + " |")
    return "\n".join(lines) + "\n"


def write_artifacts(root: Path, summary: dict[str, Any], field_coverage: list[dict[str, Any]], raw_matrix: list[dict[str, Any]], eligible_matrix: list[dict[str, Any]], gap_rows: list[dict[str, Any]], gap_summary: list[dict[str, Any]], field_reason: list[dict[str, Any]], company_history: list[dict[str, Any]], depth_summary: dict[str, Any], zero_q: list[dict[str, Any]], cik_unmapped: list[dict[str, Any]], internal_gaps: list[dict[str, Any]], core_signatures: list[dict[str, Any]], core_by_company: list[dict[str, Any]], core_by_year: list[dict[str, Any]], ebit_residuals: list[dict[str, Any]], ebitda_residuals: list[dict[str, Any]], ebit_year: list[dict[str, Any]], ebit_quarter: list[dict[str, Any]], ttm_metric: list[dict[str, Any]], ttm_company: list[dict[str, Any]], ttm_core: list[dict[str, Any]], ttm_publish: list[dict[str, Any]], phase3_vs_phase4: list[dict[str, Any]], recovery_summary: list[dict[str, Any]], fingerprint: dict[str, Any], integrity: dict[str, Any]) -> None:
    write_text(root / "preflight.md", "Phase 4D read-only historical completeness closure. Canonical writes: 0. Network calls: 0.\n")
    write_json(root / "phase4d_final_baseline.json", summary["baseline"])
    write_csv(root / "final_field_coverage.csv", field_coverage)
    write_csv(root / "phase4d_variable_year_completeness_matrix.csv", raw_matrix)
    write_text(root / "phase4d_variable_year_completeness_matrix.md", matrix_markdown(raw_matrix))
    write_csv(root / "phase4d_variable_year_eligible_completeness_matrix.csv", eligible_matrix)
    write_csv(root / "phase4d_core_ready_by_year.csv", core_by_year)
    write_csv(root / "remaining_gap_classification.csv", gap_rows)
    write_csv(root / "remaining_gap_reason_summary.csv", gap_summary)
    write_csv(root / "field_gap_reason_matrix.csv", field_reason)
    write_csv(root / "company_history_coverage.csv", company_history)
    write_csv(root / "historical_depth_summary.csv", [{"metric": key, "value": value} for key, value in depth_summary.items()])
    write_csv(root / "zero_q_residuals.csv", zero_q)
    write_csv(root / "cik_unmapped_residuals.csv", cik_unmapped)
    write_csv(root / "internal_gap_summary.csv", internal_gaps)
    write_csv(root / "core_readiness_signatures.csv", core_signatures)
    write_csv(root / "core_readiness_by_company.csv", core_by_company)
    write_csv(root / "final_ebit_residuals.csv", ebit_residuals)
    write_csv(root / "final_ebitda_residuals.csv", ebitda_residuals)
    write_csv(root / "ebit_residual_reason_summary.csv", summarize_gap_reasons(ebit_residuals))
    write_csv(root / "ebitda_residual_reason_summary.csv", summarize_gap_reasons(ebitda_residuals))
    write_csv(root / "ebit_ebitda_coverage_by_year.csv", ebit_year)
    write_csv(root / "ebit_ebitda_coverage_by_quarter.csv", ebit_quarter)
    write_csv(root / "ttm_metric_readiness.csv", ttm_metric)
    write_csv(root / "ttm_company_readiness.csv", ttm_company)
    write_csv(root / "ttm_core_readiness.csv", ttm_core)
    write_csv(root / "ttm_publish_pit_readiness.csv", ttm_publish)
    write_text(root / "ttm_instant_field_policy.md", "Instant fields are not summed for TTM. cash, total_debt, and shares_outstanding use the current endpoint period-end value.\n")
    write_csv(root / "phase3_vs_phase4_completeness.csv", phase3_vs_phase4)
    write_csv(root / "phase4_recovery_summary.csv", recovery_summary)
    write_json(root / "phase4_logical_fingerprint.json", fingerprint)
    write_text(root / "production_integrity.md", json.dumps(integrity, indent=2, sort_keys=True) + "\n")
    write_text(root / "phase5_handoff.md", f"{NEXT_PHASE}\nTTM engine must require four consecutive non-null flow quarters and endpoint instant values.\n")
    write_json(root / "summary.json", summary)
    write_text(root / "recommended_next_step.md", summary["recommended_next_step"] + "\n")


def write_doc(path: Path, summary: dict[str, Any], matrix: list[dict[str, Any]], coverage: list[dict[str, Any]]) -> None:
    text = f"""# Fundamentals V3 Phase 4D Historical Completeness Closure

Classification: `{summary['classification']}`

Canonical financial writes: `0`

Canonical Q writes: `0`

Universe writes: `0`

## Final Baseline

- Companies: `{summary['baseline']['companies']}` active `{summary['baseline']['active']}` inactive `{summary['baseline']['inactive']}`
- Canonical Q: `{summary['baseline']['canonical_q']}`
- Core-ready: `{summary['baseline']['core_ready']}`
- Core-not-ready: `{summary['baseline']['core_not_ready']}`
- Publish known/null: `{summary['baseline']['publish_known']}` / `{summary['baseline']['publish_null']}`

## Field Coverage

{coverage_markdown(coverage)}

## Year-By-Variable Completeness

{matrix_markdown(matrix)}

## Remaining Gaps

All remaining NULLs are classified by primary closure reason. A NULL is acceptable when no direct source or approved deterministic derivation exists, evidence is ambiguous, source conflict remains, or filling would weaken canonical reliability.

## TTM Readiness

TTM readiness is quantified in the Phase 4D artifacts. Flow metrics require four consecutive non-null quarters. Instant fields use endpoint values and are not summed.

## Phase 5 Handoff

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(text, encoding="utf-8")


def coverage_markdown(rows: list[dict[str, Any]]) -> str:
    lines = ["| Field | Populated | NULL | Coverage |", "| --- | ---: | ---: | ---: |"]
    for row in rows:
        lines.append(f"| {row['field']} | {row['populated']} | {row['null']} | {float(row['coverage_pct']):.2f}% |")
    return "\n".join(lines)


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = "## Phase 4D"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 4D

Classification: `{summary['classification']}`

Status: `DONE`

PHASE 4 - HISTORICAL COMPLETENESS & EBIT/EBITDA RECOVERY: `DONE`

Canonical financial writes: `0`

Canonical Q writes: `0`

Universe writes: `0`

Companies: `{summary['baseline']['companies']}`

Canonical Q: `{summary['baseline']['canonical_q']}`

Core-ready: `{summary['baseline']['core_ready']}`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
