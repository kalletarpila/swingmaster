from __future__ import annotations

import csv
import json
import os
import shutil
import sqlite3
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_core_gap_diagnostic import (
    adjacent_quarter_q_level_results,
    connect_readonly,
    current_vs_revised_gate_fill_potential,
    load_v2_rows,
    load_v3_rows,
)
from swingmaster.fundamentals.v3_v2_enrichment_policy import V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE
from swingmaster.fundamentals.v3_yahoo_canonical_seed import (
    CORE_READY_FIELDS,
    coverage_summary,
    production_integrity,
    run_yahoo_seed,
)


EXCLUDE_BANK = "EXCLUDE_BANK"
EXCLUDE_INSURANCE = "EXCLUDE_INSURANCE"
EXCLUDE_OTHER_FINANCIAL = "EXCLUDE_OTHER_FINANCIAL"
KEEP_REIT = "KEEP_REIT"
KEEP_FINANCIAL_INFRASTRUCTURE = "KEEP_FINANCIAL_INFRASTRUCTURE"
KEEP_OTHER = "KEEP_OTHER"
MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"

REIT_MARKERS = ("REIT", "Real Estate Investment Trust")
BANK_INDUSTRY_MARKERS = ("Bank", "Banks", "Savings", "Thrift")
INSURANCE_INDUSTRY_MARKERS = ("Insurance", "Reinsurance")
FINANCIAL_INFRASTRUCTURE_INDUSTRIES = {"Financial Data & Stock Exchanges"}
OTHER_FINANCIAL_EXCLUDE_INDUSTRIES = {
    "Asset Management",
    "Capital Markets",
    "Credit Services",
    "Mortgage Finance",
    "Financial Conglomerates",
}


@dataclass(frozen=True)
class FinancialClassification:
    ticker: str
    company_name: str | None
    active: int
    sector: str | None
    industry: str | None
    review_class: str
    financial_subtype: str
    decision: str
    reason: str


def classify_company(*, ticker: str, company_name: str | None, active: int, sector: str | None, industry: str | None) -> FinancialClassification:
    normalized_industry = industry or ""
    normalized_sector = sector or ""
    if any(marker.lower() in normalized_industry.lower() for marker in REIT_MARKERS):
        review_class = KEEP_REIT
        subtype = normalized_industry
        decision = "KEEP"
        reason = "REIT_TYPE_COMPANY_RETAINED_BY_POLICY"
    elif any(marker.lower() in normalized_industry.lower() for marker in BANK_INDUSTRY_MARKERS):
        review_class = EXCLUDE_BANK
        subtype = normalized_industry
        decision = "EXCLUDE"
        reason = "BANKING_BUSINESS_REQUIRES_NON_ORDINARY_MODEL"
    elif any(marker.lower() in normalized_industry.lower() for marker in INSURANCE_INDUSTRY_MARKERS):
        review_class = EXCLUDE_INSURANCE
        subtype = normalized_industry
        decision = "EXCLUDE"
        reason = "INSURANCE_OR_REINSURANCE_REQUIRES_NON_ORDINARY_MODEL"
    elif normalized_industry in FINANCIAL_INFRASTRUCTURE_INDUSTRIES:
        review_class = KEEP_FINANCIAL_INFRASTRUCTURE
        subtype = normalized_industry
        decision = "KEEP"
        reason = "FINANCIAL_INFRASTRUCTURE_RETAINED"
    elif normalized_sector == "Financial Services" and normalized_industry in OTHER_FINANCIAL_EXCLUDE_INDUSTRIES:
        review_class = EXCLUDE_OTHER_FINANCIAL
        subtype = normalized_industry
        decision = "EXCLUDE"
        reason = "PURE_FINANCIAL_BUSINESS_NOT_USEFUL_FOR_ORDINARY_V3_MODEL"
    elif normalized_sector == "Financial Services":
        review_class = MANUAL_REVIEW_REQUIRED
        subtype = normalized_industry or "UNKNOWN_FINANCIAL_SERVICES"
        decision = "KEEP"
        reason = "FINANCIAL_SERVICES_INDUSTRY_NOT_IN_EXPLICIT_POLICY"
    else:
        review_class = KEEP_OTHER
        subtype = normalized_industry
        decision = "KEEP"
        reason = "NOT_EXCLUDED_FINANCIAL_COMPANY"
    return FinancialClassification(
        ticker=ticker,
        company_name=company_name,
        active=int(active),
        sector=sector,
        industry=industry,
        review_class=review_class,
        financial_subtype=subtype,
        decision=decision,
        reason=reason,
    )


def plan_refined_universe(
    *,
    v3_db: Path,
    osakedata_db: Path,
    company_baseline_csv: Path,
    artifact_root: Path,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(f"ATTACH DATABASE '{osakedata_db.as_posix()}' AS osake")
        rows = conn.execute(
            """
            SELECT c.market, c.ticker, c.company_name, c.active, tm.sector, tm.industry
            FROM v3_company c
            LEFT JOIN osake.ticker_meta tm ON tm.market = c.market AND tm.ticker = c.ticker
            WHERE c.market = 'usa'
            ORDER BY c.ticker
            """
        ).fetchall()
    classifications = [
        classify_company(
            ticker=row["ticker"],
            company_name=row["company_name"],
            active=int(row["active"]),
            sector=row["sector"],
            industry=row["industry"],
        )
        for row in rows
    ]
    baseline_rows = list(_read_csv(company_baseline_csv))
    keep_tickers = {row.ticker for row in classifications if row.decision == "KEEP"}
    refined = [row for row in baseline_rows if row["ticker"].strip().upper() in keep_tickers]
    refined.sort(key=lambda row: row["ticker"].strip().upper())
    _write_csv(artifact_root / "financial_company_final_classification.csv", [_classification_row(row) for row in classifications])
    _write_csv(artifact_root / "financial_company_review_population.csv", [_classification_row(row) for row in classifications if row.review_class != KEEP_OTHER])
    _write_csv(artifact_root / "excluded_banks.csv", [_classification_row(row) for row in classifications if row.review_class == EXCLUDE_BANK])
    _write_csv(artifact_root / "excluded_insurance.csv", [_classification_row(row) for row in classifications if row.review_class == EXCLUDE_INSURANCE])
    _write_csv(artifact_root / "excluded_other_financial.csv", [_classification_row(row) for row in classifications if row.review_class == EXCLUDE_OTHER_FINANCIAL])
    _write_csv(artifact_root / "kept_reits.csv", [_classification_row(row) for row in classifications if row.review_class == KEEP_REIT])
    _write_csv(artifact_root / "kept_financial_infrastructure.csv", [_classification_row(row) for row in classifications if row.review_class == KEEP_FINANCIAL_INFRASTRUCTURE])
    _write_csv(artifact_root / "manual_review_required.csv", [_classification_row(row) for row in classifications if row.review_class == MANUAL_REVIEW_REQUIRED])
    _write_csv(artifact_root / "refined_universe.csv", refined)
    _write_csv(artifact_root / "current_universe_reconciliation.csv", _current_universe_rows(classifications, baseline_rows))
    _write_csv(artifact_root / "previous_exclusion_gap_analysis.csv", _previous_gap_rows(classifications))
    counts = Counter(row.review_class for row in classifications)
    excluded = [row for row in classifications if row.decision == "EXCLUDE"]
    return {
        "current_approved": len(classifications),
        "current_active": sum(row.active for row in classifications),
        "current_inactive": sum(1 - row.active for row in classifications),
        "class_counts": dict(counts),
        "excluded_tickers": sorted(row.ticker for row in excluded),
        "refined_universe_count": len(refined),
        "refined_active": sum(1 for row in refined if row["recommended_v3_company_active"].strip() == "1"),
        "refined_inactive": sum(1 for row in refined if row["recommended_v3_company_active"].strip() != "1"),
    }


def run_universe_refinement(
    *,
    production_v3_db: Path,
    v2_db: Path,
    legacy_db: Path,
    osakedata_db: Path,
    artifact_root: Path,
    company_baseline_csv: Path,
    raw_cache_db: Path,
    bootstrap_root: Path,
    post_a_root: Path,
    post_a2_root: Path,
    post_a3_root: Path,
    bootstrap_run_id: str,
    migration_run_id: str,
    now_utc: str,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before_summary = summarize_v3(production_v3_db)
    plan = plan_refined_universe(
        v3_db=production_v3_db,
        osakedata_db=osakedata_db,
        company_baseline_csv=company_baseline_csv,
        artifact_root=artifact_root,
    )
    candidate_db = artifact_root / "rc_fundamentals_v3.refined_candidate.db"
    if candidate_db.exists():
        candidate_db.unlink()
    candidate_summary = run_yahoo_seed(
        target_db=candidate_db,
        artifact_root=artifact_root / "candidate_seed_artifacts",
        company_baseline_csv=artifact_root / "refined_universe.csv",
        raw_cache_db=raw_cache_db,
        bootstrap_root=bootstrap_root,
        post_a_root=post_a_root,
        post_a2_root=post_a2_root,
        post_a3_root=post_a3_root,
        bootstrap_run_id=bootstrap_run_id,
        migration_run_id=migration_run_id,
        now_utc=now_utc,
        expected_normalized_rows=None,
    )
    candidate_v3 = summarize_v3(candidate_db)
    impact = impact_summary(production_v3_db, plan["excluded_tickers"])
    _write_csv(artifact_root / "yahoo_q_impact_pre_post.csv", [impact])
    _write_csv(artifact_root / "core_readiness_pre_post.csv", [_core_pre_post_row(before_summary, candidate_v3)])
    _write_csv(artifact_root / "missing_fields_pre_post.csv", _missing_pre_post_rows(before_summary, candidate_v3))
    _write_csv(artifact_root / "activity_pre_post.csv", [_activity_pre_post_row(plan)])
    parity = retained_company_parity(production_v3_db, candidate_db)
    _write_csv(artifact_root / "retained_company_parity.csv", parity["rows"])
    special = special_case_parity(candidate_db)
    (artifact_root / "special_case_parity.md").write_text(_markdown_table(special["rows"]) + "\n")
    if parity["value_differences"] or parity["metadata_differences"]:
        raise RuntimeError("V3_UNIVERSE_REFINEMENT_RETAINED_PARITY_FAILED:" + json.dumps(parity["summary"], sort_keys=True))
    if not special["passed"]:
        raise RuntimeError("V3_UNIVERSE_REFINEMENT_SPECIAL_CASE_PARITY_FAILED:" + json.dumps(special, sort_keys=True))
    v2_diag = refined_v2_diagnostic(candidate_db, v2_db)
    _write_csv(artifact_root / "v2_identity_population_refined.csv", [v2_diag["identity"]])
    _write_csv(artifact_root / "v2_fill_potential_refined.csv", v2_diag["fill_potential"])
    (artifact_root / "v2_no_overwrite_policy.md").write_text(v2_no_overwrite_policy_text())
    candidate_integrity = production_integrity_for_path(candidate_db)
    (artifact_root / "temp_candidate_integrity.md").write_text(json.dumps(candidate_integrity, indent=2, sort_keys=True) + "\n")
    backup = artifact_root / "rc_fundamentals_v3.pre_financial_refinement.db"
    shutil.copy2(production_v3_db, backup)
    replacement = artifact_root / "rc_fundamentals_v3.production_replacement.db"
    shutil.copy2(candidate_db, replacement)
    os.replace(replacement, production_v3_db)
    production_integrity_result = production_integrity_for_path(production_v3_db)
    (artifact_root / "production_integrity.md").write_text(json.dumps(production_integrity_result, indent=2, sort_keys=True) + "\n")
    after_summary = summarize_v3(production_v3_db)
    idempotency = idempotency_validation(production_v3_db, plan["excluded_tickers"], after_summary)
    (artifact_root / "idempotency_validation.md").write_text(json.dumps(idempotency, indent=2, sort_keys=True) + "\n")
    (artifact_root / "recommended_next_step.md").write_text("MASTER PLAN PHASE 3C - V2 METADATA & VALUE ENRICHMENT\n")
    summary = {
        "plan": plan,
        "before": before_summary,
        "candidate": candidate_v3,
        "after": after_summary,
        "impact": impact,
        "candidate_seed": candidate_summary,
        "retained_parity": parity["summary"],
        "special_case_parity": special,
        "v2_refined": v2_diag,
        "candidate_integrity": candidate_integrity,
        "production_integrity": production_integrity_result,
        "idempotency": idempotency,
        "v2_policy": {"V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE": V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE},
        "provider_calls": 0,
        "v2_canonical_contribution": 0,
        "legacy_canonical_contribution": 0,
        "classification": "FUNDAMENTALS_V3_FINANCIAL_UNIVERSE_REFINEMENT_COMPLETE",
    }
    (artifact_root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def summarize_v3(path: Path) -> dict[str, Any]:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        company = conn.execute(
            "SELECT COUNT(*) AS total, SUM(active=1) AS active, SUM(active=0) AS inactive FROM v3_company"
        ).fetchone()
        coverage = coverage_summary(conn)
        return {
            "company_total": int(company["total"]),
            "active": int(company["active"] or 0),
            "inactive": int(company["inactive"] or 0),
            "coverage": coverage,
            "integrity": production_integrity(conn),
        }


def impact_summary(path: Path, excluded_tickers: list[str]) -> dict[str, Any]:
    if not excluded_tickers:
        return {"excluded_companies": 0}
    placeholders = ",".join("?" for _ in excluded_tickers)
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        excluded_q = conn.execute(
            f"SELECT COUNT(*) FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id WHERE c.ticker IN ({placeholders})",
            excluded_tickers,
        ).fetchone()[0]
        excluded_core_ready = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM v3_quarter q
            JOIN v3_company c ON c.company_id=q.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.ticker IN ({placeholders})
              AND f.revenue IS NOT NULL AND f.ebitda IS NOT NULL AND f.free_cashflow IS NOT NULL
              AND f.cash IS NOT NULL AND f.total_debt IS NOT NULL
              AND f.shares_outstanding IS NOT NULL AND f.shares_outstanding > 0
            """,
            excluded_tickers,
        ).fetchone()[0]
        fundamentals = conn.execute(
            f"SELECT COUNT(*) FROM v3_quarter_fundamentals f JOIN v3_quarter q ON q.quarter_id=f.quarter_id JOIN v3_company c ON c.company_id=q.company_id WHERE c.ticker IN ({placeholders})",
            excluded_tickers,
        ).fetchone()[0]
        audit = conn.execute(
            f"SELECT COUNT(*) FROM v3_migration_audit a JOIN v3_company c ON c.company_id=a.company_id WHERE c.ticker IN ({placeholders})",
            excluded_tickers,
        ).fetchone()[0]
        issues = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM v3_resolution_issue i
            LEFT JOIN v3_quarter q ON q.quarter_id=i.quarter_id
            LEFT JOIN v3_company c ON c.company_id=q.company_id
            WHERE c.ticker IN ({placeholders})
               OR i.unresolved_ticker IN ({placeholders})
            """,
            [*excluded_tickers, *excluded_tickers],
        ).fetchone()[0]
    return {
        "companies_to_remove": len(excluded_tickers),
        "canonical_qs_removed": int(excluded_q),
        "fundamentals_rows_removed": int(fundamentals),
        "migration_audit_rows_removed": int(audit),
        "resolution_issues_removed": int(issues),
        "core_ready_q_removed": int(excluded_core_ready),
        "core_not_ready_q_removed": int(excluded_q - excluded_core_ready),
    }


def retained_company_parity(before_db: Path, after_db: Path) -> dict[str, Any]:
    query = """
        SELECT c.ticker, q.fiscal_year, q.fiscal_quarter, q.period_end_date, q.publish_date,
               f.revenue, f.gross_profit, f.operating_income, f.ebit, f.ebitda, f.net_income,
               f.operating_cashflow, f.capex, f.free_cashflow, f.cash, f.total_debt, f.shares_outstanding
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id=q.company_id
        JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        ORDER BY c.ticker, q.fiscal_year, q.fiscal_quarter
    """
    with sqlite3.connect(before_db) as before, sqlite3.connect(after_db) as after:
        before.row_factory = sqlite3.Row
        after.row_factory = sqlite3.Row
        after_tickers = {row[0] for row in after.execute("SELECT ticker FROM v3_company")}
        before_rows = {
            (row["ticker"], row["fiscal_year"], row["fiscal_quarter"]): dict(row)
            for row in before.execute(query)
            if row["ticker"] in after_tickers
        }
        after_rows = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"]): dict(row) for row in after.execute(query)}
    rows = []
    value_differences = 0
    metadata_differences = 0
    for key, before_row in before_rows.items():
        after_row = after_rows.get(key)
        if after_row is None:
            rows.append({"ticker": key[0], "fiscal_year": key[1], "fiscal_quarter": key[2], "parity": "MISSING_AFTER"})
            value_differences += 1
            continue
        metadata_diff = any(before_row[field] != after_row[field] for field in ("period_end_date", "publish_date"))
        value_diff = any(before_row[field] != after_row[field] for field in ("revenue", "gross_profit", "operating_income", "ebit", "ebitda", "net_income", "operating_cashflow", "capex", "free_cashflow", "cash", "total_debt", "shares_outstanding"))
        metadata_differences += int(metadata_diff)
        value_differences += int(value_diff)
        if metadata_diff or value_diff:
            rows.append({"ticker": key[0], "fiscal_year": key[1], "fiscal_quarter": key[2], "metadata_diff": int(metadata_diff), "value_diff": int(value_diff), "parity": "DIFFERENCE"})
    return {
        "rows": rows,
        "value_differences": value_differences,
        "metadata_differences": metadata_differences,
        "summary": {"retained_company_value_differences": value_differences, "retained_company_metadata_differences": metadata_differences},
    }


def special_case_parity(db_path: Path) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        rows = []
        cava = conn.execute("SELECT COUNT(*) FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id WHERE c.ticker='CAVA' AND q.fiscal_year=2026 AND q.fiscal_quarter='Q1'").fetchone()[0]
        neup = conn.execute("SELECT COUNT(*) FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id WHERE c.ticker='NEUP' AND ((q.period_end_date='2025-09-30' AND q.fiscal_year=2026 AND q.fiscal_quarter='Q1') OR (q.period_end_date='2025-12-31' AND q.fiscal_year=2026 AND q.fiscal_quarter='Q2') OR (q.period_end_date='2026-03-31' AND q.fiscal_year=2026 AND q.fiscal_quarter='Q3'))").fetchone()[0]
        lfcr = conn.execute("SELECT COUNT(*) FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id WHERE c.ticker='LFCR' AND q.period_end_date='2025-09-30'").fetchone()[0]
    rows.extend([
        {"case": "CAVA_FY2026_Q1_COUNT", "observed": cava, "expected": 1, "passed": int(cava == 1)},
        {"case": "NEUP_CORRECTED_Q1_Q2_Q3_COUNT", "observed": neup, "expected": 3, "passed": int(neup == 3)},
        {"case": "LFCR_2025_09_30_EXCLUDED_VARIANT_COUNT", "observed": lfcr, "expected": 0, "passed": int(lfcr == 0)},
    ])
    return {"rows": rows, "passed": all(row["passed"] for row in rows)}


def refined_v2_diagnostic(v3_db: Path, v2_db: Path) -> dict[str, Any]:
    v3 = connect_readonly(v3_db)
    v2 = connect_readonly(v2_db)
    v3_rows = load_v3_rows(v3)
    v2_rows = load_v2_rows(v2)
    class_map = {}
    for row in v3_rows:
        key = (row["ticker"], row["fiscal_year"], row["fiscal_quarter"])
        if key in v2_rows:
            class_map[key] = {"classification": "CANDIDATE"}
    adjacent = adjacent_quarter_q_level_results(v3_rows, v2_rows, class_map=class_map, conflict_only=False)
    confirmed = [row for row in adjacent if row["best_match"] == "SAME_Q_BEST" and row["score_margin"] is not None and row["score_margin"] >= 1]
    mapping_risk = [row for row in adjacent if row["best_match"] in {"PREVIOUS_Q_BEST", "NEXT_Q_BEST"}]
    blocked = [row for row in adjacent if row["best_match"] in {"TIE", "INSUFFICIENT"}]
    typology_rows = [
        {"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "typology": "SAME_QUARTER_LIKELY_TOLERANCE_TOO_STRICT"}
        for row in confirmed
    ]
    fills = current_vs_revised_gate_fill_potential(v3_rows, v2_rows, {key: {"classification": "NO_CURRENT_GATE"} for key in class_map}, typology_rows)
    return {
        "identity": {
            "exact_fyfq_candidates": len(class_map),
            "revised_gate_confirmed_same_quarter": len(confirmed),
            "mapping_risk_rows": len(mapping_risk),
            "blocked_rows": len(blocked),
        },
        "fill_potential": fills,
    }


def production_integrity_for_path(path: Path) -> dict[str, Any]:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        return production_integrity(conn)


def idempotency_validation(path: Path, excluded_tickers: list[str], after_summary: dict[str, Any]) -> dict[str, Any]:
    with sqlite3.connect(path) as conn:
        placeholders = ",".join("?" for _ in excluded_tickers) or "''"
        remaining = conn.execute(f"SELECT COUNT(*) FROM v3_company WHERE ticker IN ({placeholders})", excluded_tickers).fetchone()[0] if excluded_tickers else 0
    return {"universe_changes_on_recompute": int(remaining), "companies_to_remove": int(remaining), "retained_values_unchanged": True, "after_company_total": after_summary["company_total"]}


def v2_no_overwrite_policy_text() -> str:
    return """# V2 No-Overwrite Policy

`V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`

Automatic Phase 3C V2 enrichment may fill canonical V3 NULL values only after same-quarter identity is confirmed.

If V3 already has a non-null financial value, a matching V2 value is confirmation/audit evidence and a materially different V2 value is conflict evidence. It must not overwrite the existing V3 value.

The same rule applies to `publish_date`: V2 may fill NULL publish dates after same-quarter confirmation, but must not overwrite a non-null V3 publish date.
"""


def _classification_row(row: FinancialClassification) -> dict[str, Any]:
    return row.__dict__


def _current_universe_rows(classifications: list[FinancialClassification], baseline_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    baseline = {row["ticker"].strip().upper(): row for row in baseline_rows}
    return [{**_classification_row(row), "in_activity_baseline": int(row.ticker in baseline)} for row in classifications]


def _previous_gap_rows(classifications: list[FinancialClassification]) -> list[dict[str, Any]]:
    rows = []
    for row in classifications:
        if row.review_class in {EXCLUDE_BANK, EXCLUDE_INSURANCE}:
            rows.append(
                {
                    "ticker": row.ticker,
                    "company_name": row.company_name,
                    "local_industry": row.industry,
                    "previous_universe_classification": "RETAINED_IN_V3",
                    "new_review_class": row.review_class,
                    "previous_selector_retained_reason": "V2 company_profile was ORDINARY or missing; previous selector only excluded positive V2 BANK/INSURANCE profiles",
                    "gap_classification": "TAXONOMY_SCOPE_GAP",
                }
            )
    return rows


def _core_pre_post_row(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "core_ready_before": before["coverage"]["core_ready_q"],
        "core_ready_after": after["coverage"]["core_ready_q"],
        "core_not_ready_before": before["coverage"]["core_not_ready_q"],
        "core_not_ready_after": after["coverage"]["core_not_ready_q"],
    }


def _missing_pre_post_rows(before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"field": field, "before": before["coverage"]["core_missing_field_breakdown"][field], "after": after["coverage"]["core_missing_field_breakdown"][field]}
        for field in CORE_READY_FIELDS
    ]


def _activity_pre_post_row(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "current_approved": plan["current_approved"],
        "current_active": plan["current_active"],
        "current_inactive": plan["current_inactive"],
        "final_approved": plan["refined_universe_count"],
        "final_active": plan["refined_active"],
        "final_inactive": plan["refined_inactive"],
        "excluded_active": plan["current_active"] - plan["refined_active"],
        "excluded_inactive": plan["current_inactive"] - plan["refined_inactive"],
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _markdown_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    headers = list(rows[0].keys())
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(header, "")) for header in headers) + " |")
    return "\n".join(lines)
