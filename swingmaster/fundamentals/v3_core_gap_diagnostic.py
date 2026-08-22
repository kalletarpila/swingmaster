from __future__ import annotations

import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any


CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
EXTENDED_FIELDS = ("gross_profit", "operating_income", "ebit", "net_income", "operating_cashflow", "capex")
ALL_FIELDS = (*CORE_FIELDS, *EXTENDED_FIELDS)
FINGERPRINT_FIELDS = (
    "revenue",
    "net_income",
    "operating_cashflow",
    "cash",
    "total_debt",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "free_cashflow",
    "shares_outstanding",
)
TIER_A = {"revenue", "net_income", "operating_cashflow", "cash", "total_debt"}
TIER_B = {"gross_profit", "operating_income"}
TIER_C = {"ebit", "ebitda", "free_cashflow", "shares_outstanding"}
SIGN_SENSITIVE = {"net_income", "operating_cashflow", "operating_income", "ebit", "ebitda", "free_cashflow"}
IDENTITY_TOLERANCE = 0.05
NEAR_ZERO_ABSOLUTE_FLOOR = 10_000.0


@dataclass(frozen=True)
class FieldComparison:
    field_name: str
    v3_value: float | None
    v2_value: float | None
    comparable: bool
    relative_difference: float | None
    absolute_difference: float | None
    within_1pct: bool
    within_2pct: bool
    within_5pct: bool
    within_10pct: bool
    sign_mismatch: bool
    tier: str
    status: str


@dataclass(frozen=True)
class IdentityClassification:
    classification: str
    comparable_fields: int
    comparable_tier_a_fields: int
    matching_fields_5pct: int
    matching_tier_a_fields_5pct: int
    tier_a_conflicts: int
    total_conflicts: int
    period_relation: str


def connect_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def compare_values(field_name: str, v3_value: Any, v2_value: Any, *, tolerance: float = IDENTITY_TOLERANCE) -> FieldComparison:
    if v3_value is None or v2_value is None:
        return FieldComparison(field_name, _float_or_none(v3_value), _float_or_none(v2_value), False, None, None, False, False, False, False, False, _tier(field_name), "NOT_COMPARABLE")
    left = float(v3_value)
    right = float(v2_value)
    abs_diff = abs(left - right)
    denom = max(abs(left), abs(right))
    rel = 0.0 if denom == 0 else abs_diff / denom
    sign_mismatch = _sign_mismatch(field_name, left, right)
    within_1 = _within_identity_tolerance(left, right, 0.01)
    within_2 = _within_identity_tolerance(left, right, 0.02)
    within_5 = _within_identity_tolerance(left, right, 0.05)
    within_10 = _within_identity_tolerance(left, right, 0.10)
    if sign_mismatch:
        status = "SIGN_MISMATCH"
    elif _within_identity_tolerance(left, right, tolerance):
        status = "MATCH"
    else:
        status = "MISMATCH"
    return FieldComparison(field_name, left, right, True, rel, abs_diff, within_1, within_2, within_5, within_10, sign_mismatch, _tier(field_name), status)


def classify_period_relation(v3_period_end: str | None, v2_period_end: str | None) -> str:
    if not v2_period_end:
        return "V2_PERIOD_END_MISSING"
    if not v3_period_end:
        return "V3_PERIOD_END_MISSING"
    left = date.fromisoformat(v3_period_end)
    right = date.fromisoformat(v2_period_end)
    diff = abs((left - right).days)
    if diff == 0:
        return "EXACT_PERIOD_END"
    if diff <= 7:
        return "SMALL_KNOWN_PROVIDER_VARIANT"
    if diff <= 35:
        return "KNOWN_FISCAL_CALENDAR_VARIANT"
    return "MATERIAL_PERIOD_END_DIFFERENCE"


def classify_v2_identity(comparisons: list[FieldComparison], period_relation: str) -> IdentityClassification:
    comparable = [item for item in comparisons if item.comparable]
    comparable_tier_a = [item for item in comparable if item.field_name in TIER_A]
    matches = [item for item in comparable if item.within_5pct and not item.sign_mismatch]
    tier_a_matches = [item for item in matches if item.field_name in TIER_A]
    conflicts = [item for item in comparable if item.status in {"MISMATCH", "SIGN_MISMATCH"}]
    tier_a_conflicts = [item for item in conflicts if item.field_name in TIER_A]
    period_compatible = period_relation in {"EXACT_PERIOD_END", "SMALL_KNOWN_PROVIDER_VARIANT", "KNOWN_FISCAL_CALENDAR_VARIANT"}
    if period_relation == "MATERIAL_PERIOD_END_DIFFERENCE" and (tier_a_conflicts or len(matches) < 3):
        classification = "PERIOD_IDENTITY_CONFLICT"
    elif tier_a_conflicts:
        classification = "CONFLICT"
    elif period_compatible and len(comparable) >= 3 and len(comparable_tier_a) >= 2 and len(tier_a_matches) >= 2 and len(matches) >= max(3, len(comparable) - 1):
        classification = "STRONG_MATCH"
    elif period_compatible and len(comparable_tier_a) == 2 and len(tier_a_matches) == 2 and len(conflicts) == 0:
        classification = "STRONG_MATCH_LIMITED_FIELDS"
    elif len(matches) >= 2 and not tier_a_conflicts:
        classification = "PROBABLE_MATCH"
    else:
        classification = "INSUFFICIENT_EVIDENCE"
    return IdentityClassification(
        classification=classification,
        comparable_fields=len(comparable),
        comparable_tier_a_fields=len(comparable_tier_a),
        matching_fields_5pct=len(matches),
        matching_tier_a_fields_5pct=len(tier_a_matches),
        tier_a_conflicts=len(tier_a_conflicts),
        total_conflicts=len(conflicts),
        period_relation=period_relation,
    )


def run_core_gap_diagnostic(*, v3_db: Path, v2_db: Path, legacy_db: Path, artifact_root: Path) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    v3 = connect_readonly(v3_db)
    v2 = connect_readonly(v2_db)
    legacy = connect_readonly(legacy_db)
    before = row_counts(v3, v2, legacy)
    baseline = reproduce_v3_baseline(v3)
    if baseline["total_canonical_q"] != 14345 or baseline["core_ready_q"] != 12344 or baseline["core_not_ready_q"] != 2001:
        raise RuntimeError("V3_CORE_DIAGNOSTIC_BASELINE_DRIFT:" + json.dumps(baseline, sort_keys=True))
    yahoo_sources = load_yahoo_sources(v3)
    issues = load_issue_map(v3)
    v3_rows = load_v3_rows(v3)
    v2_rows = load_v2_rows(v2)
    legacy_rows = load_legacy_rows(legacy)
    missing_rows = [row for row in v3_rows if any(row[field] is None or (field == "shares_outstanding" and (row[field] or 0) <= 0) for field in CORE_FIELDS)]
    missing_dataset = build_missing_dataset(missing_rows, yahoo_sources, issues, v2_rows)
    v2_identity_rows, field_agreements, tolerance_summary, classifications = analyze_v2_identity(v3_rows, v2_rows)
    adjacent_check = analyze_adjacent_quarter_false_confidence(v3_rows, v2_rows)
    legacy_recovery = analyze_legacy_recoverability(missing_rows, legacy_rows)
    field_diagnostics = diagnose_missing_fields(missing_rows, yahoo_sources, v2_rows, classifications, legacy_rows)
    overlap = recovery_source_overlap(field_diagnostics)
    potential_bugs = potential_migration_bugs(field_diagnostics)
    field_matrix = field_recovery_matrix(field_diagnostics)
    shares_validation = validate_shares(v3_rows, yahoo_sources)
    after = row_counts(v3, v2, legacy)
    readonly = {
        "v3_writes": 0 if before["v3"] == after["v3"] else "ROW_COUNT_DRIFT",
        "v2_writes": 0 if before["v2"] == after["v2"] else "ROW_COUNT_DRIFT",
        "legacy_writes": 0 if before["legacy"] == after["legacy"] else "ROW_COUNT_DRIFT",
    }
    summary = {
        "baseline": baseline,
        "missing_overlap": missing_overlap_summary(missing_dataset),
        "company_concentration": company_concentration(missing_dataset),
        "period_concentration": period_concentration(missing_dataset),
        "v2_identity": identity_summary(classifications),
        "v2_tolerance": tolerance_summary,
        "v2_field_quality": field_quality(field_agreements),
        "adjacent_quarter_false_confidence": adjacent_check["summary"],
        "v2_recoverability": v2_recoverability(field_diagnostics, classifications),
        "legacy_recoverability": legacy_recoverability_summary(legacy_recovery),
        "field_recovery_matrix": field_matrix,
        "recovery_source_overlap": overlap,
        "field_root_causes": root_cause_summary(field_diagnostics),
        "potential_migration_bugs": {"count": len(potential_bugs)},
        "shares_validation": {
            "invalid_or_missing": shares_validation["invalid_or_missing"],
            "source_quality_counts": shares_validation["source_quality_counts"],
        },
        "v2_publication_recoverability": v2_publication_summary(v2_identity_rows),
        "read_only_proof": readonly,
        "quick_check": v3.execute("PRAGMA quick_check").fetchone()[0],
        "foreign_key_check_rows": len(v3.execute("PRAGMA foreign_key_check").fetchall()),
        "classification": "YAHOO_CORE_DIAGNOSTIC_CLEAN_V2_IDENTITY_GATE_READY" if not potential_bugs else "YAHOO_CORE_DIAGNOSTIC_PHASE3B_REPAIR_REQUIRED",
    }
    write_artifacts(
        artifact_root,
        missing_dataset=missing_dataset,
        field_diagnostics=field_diagnostics,
        shares_validation=shares_validation,
        v2_identity_rows=v2_identity_rows,
        field_agreements=field_agreements,
        tolerance_summary=tolerance_summary,
        classifications=classifications,
        adjacent_check=adjacent_check,
        legacy_recovery=legacy_recovery,
        field_matrix=field_matrix,
        overlap=overlap,
        potential_bugs=potential_bugs,
        summary=summary,
    )
    return summary


def reproduce_v3_baseline(conn: sqlite3.Connection) -> dict[str, Any]:
    total = conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0]
    core_ready = conn.execute(
        """
        SELECT COUNT(*)
        FROM v3_quarter_fundamentals
        WHERE revenue IS NOT NULL AND ebitda IS NOT NULL AND free_cashflow IS NOT NULL
          AND cash IS NOT NULL AND total_debt IS NOT NULL
          AND shares_outstanding IS NOT NULL AND shares_outstanding > 0
        """
    ).fetchone()[0]
    missing = conn.execute(
        """
        SELECT SUM(revenue IS NULL) AS revenue,
               SUM(ebitda IS NULL) AS ebitda,
               SUM(free_cashflow IS NULL) AS free_cashflow,
               SUM(cash IS NULL) AS cash,
               SUM(total_debt IS NULL) AS total_debt,
               SUM(shares_outstanding IS NULL OR shares_outstanding <= 0) AS shares_outstanding
        FROM v3_quarter_fundamentals
        """
    ).fetchone()
    publish = conn.execute(
        "SELECT SUM(publish_date IS NOT NULL) AS known, SUM(publish_date IS NULL) AS nulls FROM v3_quarter"
    ).fetchone()
    return {
        "total_canonical_q": int(total),
        "core_ready_q": int(core_ready),
        "core_not_ready_q": int(total - core_ready),
        "missing": {field: int(missing[field] or 0) for field in CORE_FIELDS},
        "publish_date_known": int(publish["known"] or 0),
        "publish_date_null": int(publish["nulls"] or 0),
    }


def load_v3_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT c.market, c.ticker, c.active, q.quarter_id, q.fiscal_year, q.fiscal_quarter,
               q.period_end_date, q.publish_date,
               f.revenue, f.ebitda, f.free_cashflow, f.cash, f.total_debt, f.shares_outstanding,
               f.gross_profit, f.operating_income, f.ebit, f.net_income, f.operating_cashflow, f.capex
        FROM v3_quarter q
        JOIN v3_company c ON c.company_id = q.company_id
        JOIN v3_quarter_fundamentals f ON f.quarter_id = q.quarter_id
        ORDER BY c.ticker, q.fiscal_year, q.fiscal_quarter, q.period_end_date
        """
    ).fetchall()
    return [dict(row) for row in rows]


def load_v2_rows(conn: sqlite3.Connection) -> dict[tuple[str, int, str], dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT c.market, c.ticker, q.fiscal_year, q.fiscal_period AS fiscal_quarter,
               q.report_date AS period_end_date, q.publish_date,
               f.revenue, f.gross_profit, f.operating_income, f.ebit, f.ebitda, f.net_income,
               f.operating_cashflow, f.capex, f.free_cashflow, f.cash, f.total_debt, f.shares_outstanding,
               f.depreciation_amortization
        FROM rc_v2_company c
        JOIN rc_v2_quarter q ON q.company_id = c.company_id
        JOIN rc_v2_fundamental_quarterly f ON f.quarter_id = q.quarter_id
        WHERE c.market = 'usa'
        """
    ).fetchall()
    out: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in rows:
        out[(str(row["ticker"]).upper(), int(row["fiscal_year"]), str(row["fiscal_quarter"]).upper())] = dict(row)
    return out


def load_v2_rows_by_ticker(v2_rows: dict[tuple[str, int, str], dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (ticker, _, _), row in v2_rows.items():
        by_ticker[ticker].append(row)
    for rows in by_ticker.values():
        rows.sort(key=lambda item: str(item["period_end_date"] or ""))
    return by_ticker


def load_legacy_rows(conn: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT ticker, period_end_date, revenue, gross_profit, operating_income, ebit, ebitda, net_income,
               operating_cashflow, capex, free_cashflow, cash, total_debt, shares_outstanding
        FROM rc_fundamental_quarterly
        """
    ).fetchall()
    return {(str(row["ticker"]).upper(), str(row["period_end_date"])): dict(row) for row in rows}


def load_yahoo_sources(conn: sqlite3.Connection) -> dict[tuple[str, int, str], list[dict[str, Any]]]:
    out: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    rows = conn.execute(
        """
        SELECT a.source_key, a.migration_run_id, a.evidence_json, c.ticker, q.fiscal_year, q.fiscal_quarter
        FROM v3_migration_audit a
        LEFT JOIN v3_quarter q ON q.quarter_id = a.quarter_id
        LEFT JOIN v3_company c ON c.company_id = COALESCE(q.company_id, a.company_id)
        WHERE a.source = 'YAHOO'
        """
    ).fetchall()
    for row in rows:
        evidence = json.loads(row["evidence_json"] or "{}")
        value_meta = evidence.get("value_metadata", {})
        key = (str(row["ticker"] or _source_ticker(row["source_key"])).upper(), int(row["fiscal_year"] or _source_fy(row["source_key"]) or 0), str(row["fiscal_quarter"] or _source_fq(row["source_key"]) or "").upper())
        out[key].append(
            {
                "source_key": row["source_key"],
                "migration_run_id": row["migration_run_id"],
                "field_outcomes": evidence.get("field_outcomes", {}),
                "provider_period_end_date": _source_period(row["source_key"]),
                "disposition": value_meta.get("disposition"),
                "provider_details": value_meta.get("provider_details", {}),
                "issue_ids": evidence.get("issue_ids", []),
                "metadata_outcomes": evidence.get("metadata_outcomes", []),
            }
        )
    return out


def load_issue_map(conn: sqlite3.Connection) -> dict[tuple[str, int, str], list[str]]:
    out: dict[tuple[str, int, str], list[str]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT COALESCE(c.ticker, i.unresolved_ticker) AS ticker,
               i.unresolved_fiscal_year AS fiscal_year,
               i.unresolved_fiscal_quarter AS fiscal_quarter,
               i.issue_type
        FROM v3_resolution_issue i
        LEFT JOIN v3_quarter q ON q.quarter_id = i.quarter_id
        LEFT JOIN v3_company c ON c.company_id = q.company_id
        """
    ):
        if row["ticker"] and row["fiscal_year"] and row["fiscal_quarter"]:
            out[(str(row["ticker"]).upper(), int(row["fiscal_year"]), str(row["fiscal_quarter"]).upper())].append(str(row["issue_type"]))
    return out


def build_missing_dataset(
    missing_rows: list[dict[str, Any]],
    yahoo_sources: dict[tuple[str, int, str], list[dict[str, Any]]],
    issues: dict[tuple[str, int, str], list[str]],
    v2_rows: dict[tuple[str, int, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in missing_rows:
        key = (row["ticker"], row["fiscal_year"], row["fiscal_quarter"])
        sources = yahoo_sources.get(key, [])
        missing_flags = {f"missing_{field}": _is_missing(row, field) for field in CORE_FIELDS}
        rows.append(
            {
                **{k: row[k] for k in ("market", "ticker", "fiscal_year", "fiscal_quarter", "period_end_date", "publish_date", "active")},
                "source_class": "V2_COVERED" if key in v2_rows else "LEGACY_ONLY",
                **{field: row[field] for field in (*CORE_FIELDS, *EXTENDED_FIELDS)},
                **missing_flags,
                "yahoo_source_record_ids": ";".join(item["source_key"] for item in sources),
                "provider_period_end_dates": ";".join(str(item["provider_period_end_date"]) for item in sources),
                "candidate_dispositions": ";".join(sorted({str(item["disposition"]) for item in sources})),
                "migration_run_ids": ";".join(sorted({str(item["migration_run_id"]) for item in sources})),
                "yahoo_work_unit_count": len(sources),
                "known_duplicate_or_variant": int(any(item["disposition"] in {"COMPLEMENTARY_SAME_FISCAL_Q", "PROVIDER_PERIOD_VARIANT_EXCLUDED"} for item in sources)),
                "resolution_issue_types": ";".join(sorted(set(issues.get(key, [])))),
            }
        )
    return rows


def analyze_v2_identity(v3_rows: list[dict[str, Any]], v2_rows: dict[tuple[str, int, str], dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    agreements: list[dict[str, Any]] = []
    classes: list[dict[str, Any]] = []
    for row in v3_rows:
        key = (row["ticker"], row["fiscal_year"], row["fiscal_quarter"])
        v2 = v2_rows.get(key)
        if v2 is None:
            continue
        period_relation = classify_period_relation(row["period_end_date"], v2["period_end_date"])
        comparisons = [compare_values(field, row.get(field), v2.get(field)) for field in FINGERPRINT_FIELDS]
        classification = classify_v2_identity(comparisons, period_relation)
        candidates.append(
            {
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "v3_period_end_date": row["period_end_date"],
                "v2_period_end_date": v2["period_end_date"],
                "v3_publish_date": row["publish_date"],
                "v2_publish_date": v2["publish_date"],
                **classification.__dict__,
            }
        )
        for comparison in comparisons:
            agreements.append(
                {
                    "ticker": row["ticker"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "field": comparison.field_name,
                    "tier": comparison.tier,
                    "v3_value": comparison.v3_value,
                    "v2_value": comparison.v2_value,
                    "comparable": int(comparison.comparable),
                    "relative_difference": comparison.relative_difference,
                    "absolute_difference": comparison.absolute_difference,
                    "within_1pct": int(comparison.within_1pct),
                    "within_2pct": int(comparison.within_2pct),
                    "within_5pct": int(comparison.within_5pct),
                    "within_10pct": int(comparison.within_10pct),
                    "sign_mismatch": int(comparison.sign_mismatch),
                    "status": comparison.status,
                    "identity_classification": classification.classification,
                }
            )
        classes.append(candidates[-1])
    return candidates, agreements, tolerance_distribution(agreements), classes


def analyze_adjacent_quarter_false_confidence(v3_rows: list[dict[str, Any]], v2_rows: dict[tuple[str, int, str], dict[str, Any]]) -> dict[str, Any]:
    by_ticker = load_v2_rows_by_ticker(v2_rows)
    checks: list[dict[str, Any]] = []
    for row in v3_rows:
        key = (row["ticker"], row["fiscal_year"], row["fiscal_quarter"])
        same = v2_rows.get(key)
        if same is None or not same.get("period_end_date"):
            continue
        ticker_rows = by_ticker.get(row["ticker"], [])
        same_index = next((index for index, candidate in enumerate(ticker_rows) if candidate is same), None)
        if same_index is None:
            continue
        same_score = _identity_score(row, same)
        for offset in (-1, 1):
            adjacent_index = same_index + offset
            if adjacent_index < 0 or adjacent_index >= len(ticker_rows):
                continue
            adjacent = ticker_rows[adjacent_index]
            adjacent_score = _identity_score(row, adjacent)
            checks.append(
                {
                    "ticker": row["ticker"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "v3_period_end_date": row["period_end_date"],
                    "same_v2_period_end_date": same["period_end_date"],
                    "adjacent_v2_period_end_date": adjacent["period_end_date"],
                    "adjacent_direction": "previous" if offset < 0 else "next",
                    "same_matching_fields_5pct": same_score["matches"],
                    "same_tier_a_matches_5pct": same_score["tier_a_matches"],
                    "same_conflicts": same_score["conflicts"],
                    "adjacent_matching_fields_5pct": adjacent_score["matches"],
                    "adjacent_tier_a_matches_5pct": adjacent_score["tier_a_matches"],
                    "adjacent_conflicts": adjacent_score["conflicts"],
                    "same_score_stronger": int(
                        (same_score["tier_a_matches"], same_score["matches"], -same_score["conflicts"])
                        > (adjacent_score["tier_a_matches"], adjacent_score["matches"], -adjacent_score["conflicts"])
                    ),
                }
            )
    summary = {
        "comparisons": len(checks),
        "same_score_stronger": sum(row["same_score_stronger"] for row in checks),
        "adjacent_equal_or_stronger": sum(1 - row["same_score_stronger"] for row in checks),
    }
    summary["same_score_stronger_pct"] = round(summary["same_score_stronger"] / summary["comparisons"] * 100.0, 2) if summary["comparisons"] else 0.0
    return {"summary": summary, "rows": checks}


def _identity_score(v3_row: dict[str, Any], v2_row: dict[str, Any]) -> dict[str, int]:
    comparisons = [compare_values(field, v3_row.get(field), v2_row.get(field)) for field in FINGERPRINT_FIELDS]
    return {
        "matches": sum(1 for item in comparisons if item.within_5pct and not item.sign_mismatch),
        "tier_a_matches": sum(1 for item in comparisons if item.field_name in TIER_A and item.within_5pct and not item.sign_mismatch),
        "conflicts": sum(1 for item in comparisons if item.status in {"MISMATCH", "SIGN_MISMATCH"}),
    }


def diagnose_missing_fields(
    missing_rows: list[dict[str, Any]],
    yahoo_sources: dict[tuple[str, int, str], list[dict[str, Any]]],
    v2_rows: dict[tuple[str, int, str], dict[str, Any]],
    classifications: list[dict[str, Any]],
    legacy_rows: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    class_map = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"]): row for row in classifications}
    diagnostics = {field: [] for field in CORE_FIELDS}
    for row in missing_rows:
        key = (row["ticker"], row["fiscal_year"], row["fiscal_quarter"])
        sources = yahoo_sources.get(key, [])
        class_row = class_map.get(key)
        legacy = legacy_rows.get((row["ticker"], row["period_end_date"]))
        v2 = v2_rows.get(key)
        for field in CORE_FIELDS:
            if not _is_missing(row, field):
                continue
            yahoo_direct = any(_field_outcome(s, field) not in {"FIELD_SKIPPED_NULL", ""} for s in sources)
            yahoo_null = any(_field_outcome(s, field) == "FIELD_SKIPPED_NULL" for s in sources)
            duplicate = any(s["disposition"] in {"COMPLEMENTARY_SAME_FISCAL_Q", "PROVIDER_PERIOD_VARIANT_EXCLUDED"} for s in sources)
            v2_available = v2 is not None and v2.get(field) is not None
            strong = class_row is not None and class_row["classification"] == "STRONG_MATCH"
            legacy_available = legacy is not None and legacy.get(field) is not None
            yahoo_derivable = _yahoo_derivable(field, row)
            if yahoo_direct:
                cause = "CANONICAL_APPLY_ISSUE"
            elif duplicate:
                cause = "DUPLICATE_OR_PROVIDER_VARIANT"
            elif field == "ebitda" and yahoo_derivable:
                cause = "DERIVATION_INPUTS_MISSING"
            elif field == "free_cashflow" and yahoo_derivable:
                cause = "DIRECT_FCF_NULL_BUT_DERIVABLE"
            elif field == "total_debt" and yahoo_derivable:
                cause = "DIRECT_TOTAL_DEBT_NULL_BUT_DERIVABLE"
            elif yahoo_null and sources:
                cause = "YAHOO_FIELD_NULL" if field != "ebitda" else "YAHOO_DIRECT_EBITDA_NULL"
            elif not sources:
                cause = "NO_MATCHING_YAHOO_INCOME_ROW" if field == "revenue" else "NO_MATCHING_YAHOO_WORK_UNIT"
            else:
                cause = "OTHER_IDENTIFIED_LOCAL_CAUSE"
            diagnostics[field].append(
                {
                    "ticker": row["ticker"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end_date": row["period_end_date"],
                    "primary_cause": cause,
                    "yahoo_local_direct_recoverable": int(yahoo_direct),
                    "yahoo_local_derivable": int(yahoo_derivable),
                    "v2_value_on_identity_candidate": int(v2_available),
                    "v2_value_on_strong_match": int(v2_available and strong),
                    "legacy_recoverable": int(legacy_available),
                    "no_known_source": int(not yahoo_direct and not yahoo_derivable and not v2_available and not legacy_available),
                    "v2_identity_classification": class_row["classification"] if class_row else "NO_V2_FYFQ_MATCH",
                }
            )
    return diagnostics


def analyze_legacy_recoverability(missing_rows: list[dict[str, Any]], legacy_rows: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in missing_rows:
        legacy = legacy_rows.get((row["ticker"], row["period_end_date"]))
        for field in CORE_FIELDS:
            if _is_missing(row, field):
                out.append(
                    {
                        "ticker": row["ticker"],
                        "fiscal_year": row["fiscal_year"],
                        "fiscal_quarter": row["fiscal_quarter"],
                        "period_end_date": row["period_end_date"],
                        "field": field,
                        "legacy_value_available": int(legacy is not None and legacy.get(field) is not None),
                        "legacy_identity_method": "EXACT_TICKER_PERIOD_END" if legacy is not None else "NO_EXACT_PERIOD_MATCH",
                    }
                )
    return out


def write_artifacts(
    root: Path,
    *,
    missing_dataset: list[dict[str, Any]],
    field_diagnostics: dict[str, list[dict[str, Any]]],
    shares_validation: dict[str, Any],
    v2_identity_rows: list[dict[str, Any]],
    field_agreements: list[dict[str, Any]],
    tolerance_summary: dict[str, Any],
    classifications: list[dict[str, Any]],
    adjacent_check: dict[str, Any],
    legacy_recovery: list[dict[str, Any]],
    field_matrix: list[dict[str, Any]],
    overlap: list[dict[str, Any]],
    potential_bugs: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    write_csv(root / "missing_core_quarters.csv", missing_dataset)
    write_csv(root / "missing_field_signatures.csv", signature_rows(missing_dataset))
    for field, name in [("revenue", "revenue"), ("ebitda", "ebitda"), ("free_cashflow", "fcf"), ("cash", "cash"), ("total_debt", "total_debt")]:
        write_csv(root / f"{name}_missing_diagnostic.csv", field_diagnostics[field])
    write_csv(root / "shares_validation.csv", shares_validation["rows"])
    write_csv(root / "missing_by_company.csv", missing_by_company_rows(missing_dataset))
    write_csv(root / "missing_by_year.csv", missing_by_year_rows(missing_dataset))
    write_csv(root / "missing_by_fiscal_quarter.csv", missing_by_fq_rows(missing_dataset))
    write_csv(root / "missing_vs_resolution_issues.csv", missing_vs_issue_rows(missing_dataset))
    write_csv(root / "v2_v3_identity_candidates.csv", v2_identity_rows)
    write_csv(root / "v2_v3_field_agreement.csv", field_agreements)
    write_csv(root / "v2_v3_relative_difference_distribution.csv", distribution_rows(tolerance_summary))
    write_csv(root / "v2_quarter_identity_classification.csv", classifications)
    write_csv(root / "v2_identity_field_quality.csv", field_quality(summary_data=field_agreements))
    write_csv(root / "v2_adjacent_quarter_false_confidence.csv", adjacent_check["rows"])
    write_samples(root, field_agreements, classifications)
    write_csv(root / "v2_core_recoverability.csv", v2_core_recovery_rows(field_diagnostics))
    write_csv(root / "v2_publication_recoverability.csv", v2_publication_rows(v2_identity_rows))
    write_csv(root / "legacy_recoverability.csv", legacy_recovery)
    write_csv(root / "field_recovery_matrix.csv", field_matrix)
    write_csv(root / "recovery_source_overlap.csv", overlap)
    write_csv(root / "potential_migration_bugs.csv", potential_bugs)
    (root / "recommended_v2_same_quarter_gate.md").write_text(recommended_gate_text())
    (root / "recommended_next_step.md").write_text("MASTER PLAN PHASE 3C - V2 METADATA & VALUE ENRICHMENT\n")
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def tolerance_distribution(agreements: list[dict[str, Any]]) -> dict[str, Any]:
    by_field: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in agreements:
        if row["comparable"]:
            by_field[row["field"]].append(row)
    out: dict[str, Any] = {}
    for field, rows in by_field.items():
        rels = sorted(float(row["relative_difference"]) for row in rows if row["relative_difference"] is not None)
        out[field] = {
            "compared": len(rows),
            "le_1pct": sum(row["within_1pct"] for row in rows),
            "le_2pct": sum(row["within_2pct"] for row in rows),
            "le_5pct": sum(row["within_5pct"] for row in rows),
            "le_10pct": sum(row["within_10pct"] for row in rows),
            "gt_10pct": sum(1 for row in rows if row["relative_difference"] is not None and row["relative_difference"] > 0.10),
            "gt_25pct": sum(1 for row in rows if row["relative_difference"] is not None and row["relative_difference"] > 0.25),
            "gt_50pct": sum(1 for row in rows if row["relative_difference"] is not None and row["relative_difference"] > 0.50),
            "sign_conflicts": sum(row["sign_mismatch"] for row in rows),
            "median": _percentile(rels, 0.50),
            "p75": _percentile(rels, 0.75),
            "p90": _percentile(rels, 0.90),
            "p95": _percentile(rels, 0.95),
            "p99": _percentile(rels, 0.99),
        }
    return out


def field_quality(summary_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dist = tolerance_distribution(summary_data)
    rows = []
    for field, item in dist.items():
        conflict_rate = round((item["gt_10pct"] / item["compared"] * 100.0) if item["compared"] else 0.0, 2)
        if field in TIER_A and conflict_rate <= 10:
            recommendation = "STRONG_IDENTITY_FIELD"
        elif field in TIER_B and conflict_rate <= 15:
            recommendation = "SUPPORTING_IDENTITY_FIELD"
        else:
            recommendation = "DO_NOT_USE_AS_PRIMARY_IDENTITY_FIELD"
        rows.append({"field": field, "compared": item["compared"], "p95": item["p95"], "gt_10pct": item["gt_10pct"], "conflict_rate_pct": conflict_rate, "recommendation": recommendation})
    return sorted(rows, key=lambda row: row["field"])


def identity_summary(classes: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter(row["classification"] for row in classes)
    return {
        "exact_fyfq_candidate_matches": len(classes),
        "with_1_plus_comparable": sum(1 for row in classes if row["comparable_fields"] >= 1),
        "with_2_plus_comparable": sum(1 for row in classes if row["comparable_fields"] >= 2),
        "with_3_plus_comparable": sum(1 for row in classes if row["comparable_fields"] >= 3),
        "with_2_plus_tier_a": sum(1 for row in classes if row["comparable_tier_a_fields"] >= 2),
        "classification_counts": dict(counter),
        "period_relation_counts": dict(Counter(row["period_relation"] for row in classes)),
    }


def v2_recoverability(field_diagnostics: dict[str, list[dict[str, Any]]], classifications: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        field: {
            "v3_null": len(rows),
            "v2_value_on_identity_candidate": sum(row["v2_value_on_identity_candidate"] for row in rows),
            "v2_value_on_strong_match": sum(row["v2_value_on_strong_match"] for row in rows),
        }
        for field, rows in field_diagnostics.items()
    }


def v2_publication_rows(v2_identity_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in v2_identity_rows:
        if row["v3_publish_date"] is None:
            rows.append(
                {
                    "ticker": row["ticker"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "v2_publish_date_available": int(row["v2_publish_date"] is not None),
                    "classification": row["classification"],
                    "strong_match_with_publish": int(row["classification"] == "STRONG_MATCH" and row["v2_publish_date"] is not None),
                }
            )
    return rows


def v2_publication_summary(v2_identity_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = v2_publication_rows(v2_identity_rows)
    return {
        "v3_publish_null_with_v2_fyfq_match": len(rows),
        "v2_publish_available": sum(row["v2_publish_date_available"] for row in rows),
        "strong_match": sum(1 for row in rows if row["classification"] == "STRONG_MATCH"),
        "strong_match_with_publish": sum(row["strong_match_with_publish"] for row in rows),
        "probable_or_insufficient_or_conflict_with_publish": sum(
            1
            for row in rows
            if row["v2_publish_date_available"]
            and row["classification"] in {"PROBABLE_MATCH", "INSUFFICIENT_EVIDENCE", "CONFLICT", "PERIOD_IDENTITY_CONFLICT", "STRONG_MATCH_LIMITED_FIELDS"}
        ),
    }


def root_cause_summary(field_diagnostics: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, int]]:
    return {field: dict(Counter(row["primary_cause"] for row in rows)) for field, rows in field_diagnostics.items()}


def field_recovery_matrix(field_diagnostics: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for field, items in field_diagnostics.items():
        rows.append(
            {
                "field": field,
                "v3_null": len(items),
                "yahoo_local_direct_recoverable": sum(row["yahoo_local_direct_recoverable"] for row in items),
                "yahoo_local_derivable": sum(row["yahoo_local_derivable"] for row in items),
                "v2_value_on_identity_candidate": sum(row["v2_value_on_identity_candidate"] for row in items),
                "v2_value_on_strong_match": sum(row["v2_value_on_strong_match"] for row in items),
                "legacy_usable": sum(row["legacy_recoverable"] for row in items),
                "no_known_source": sum(row["no_known_source"] for row in items),
            }
        )
    return rows


def recovery_source_overlap(field_diagnostics: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for field, items in field_diagnostics.items():
        counter = Counter()
        for row in items:
            sources = []
            if row["yahoo_local_direct_recoverable"] or row["yahoo_local_derivable"]:
                sources.append("Yahoo")
            if row["v2_value_on_strong_match"]:
                sources.append("V2")
            if row["legacy_recoverable"]:
                sources.append("Legacy")
            counter[" + ".join(sources) if sources else "none"] += 1
        for source_combo, count in sorted(counter.items()):
            rows.append({"field": field, "source_overlap": source_combo, "count": count})
    return rows


def potential_migration_bugs(field_diagnostics: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [
        {"field": field, **row}
        for field, rows in field_diagnostics.items()
        for row in rows
        if row["primary_cause"] == "CANONICAL_APPLY_ISSUE"
    ]


def validate_shares(v3_rows: list[dict[str, Any]], yahoo_sources: dict[tuple[str, int, str], list[dict[str, Any]]]) -> dict[str, Any]:
    rows = []
    counter = Counter()
    invalid = 0
    for row in v3_rows:
        key = (row["ticker"], row["fiscal_year"], row["fiscal_quarter"])
        sources = yahoo_sources.get(key, [])
        qualities = sorted({str(item["provider_details"].get("shares_quality")) for item in sources if item["provider_details"].get("shares_quality")})
        source_names = sorted({str(item["provider_details"].get("shares_source")) for item in sources if item["provider_details"].get("shares_source")})
        if row["shares_outstanding"] is None or row["shares_outstanding"] <= 0:
            invalid += 1
        quality_key = ";".join(qualities) or "UNKNOWN"
        source_key = ";".join(source_names) or "UNKNOWN"
        counter[(source_key, quality_key)] += 1
        rows.append({"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "shares_outstanding": row["shares_outstanding"], "shares_sources": source_key, "shares_qualities": quality_key})
    return {"invalid_or_missing": invalid, "source_quality_counts": [{"shares_source": k[0], "shares_quality": k[1], "count": v} for k, v in counter.items()], "rows": rows}


def missing_overlap_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(sum(int(row[f"missing_{field}"]) for field in CORE_FIELDS) for row in rows)
    signatures = Counter(_signature(row) for row in rows)
    return {"missing_count_distribution": dict(counts), "top_signatures": dict(signatures.most_common(20))}


def company_concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_company: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = by_company.setdefault(row["ticker"], {"affected_q": 0, "missing_fields": 0, "active": row["active"], "source_class": row["source_class"]})
        item["affected_q"] += 1
        item["missing_fields"] += sum(int(row[f"missing_{field}"]) for field in CORE_FIELDS)
    return {
        "companies_affected": len(by_company),
        "active_affected": sum(1 for item in by_company.values() if item["active"]),
        "inactive_affected": sum(1 for item in by_company.values() if not item["active"]),
        "v2_covered_affected": sum(1 for item in by_company.values() if item["source_class"] == "V2_COVERED"),
        "legacy_only_affected": sum(1 for item in by_company.values() if item["source_class"] == "LEGACY_ONLY"),
        "top_30": sorted(({"ticker": ticker, **item} for ticker, item in by_company.items()), key=lambda row: (-row["missing_fields"], -row["affected_q"], row["ticker"]))[:30],
    }


def period_concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "by_fiscal_year": dict(Counter(str(row["fiscal_year"]) for row in rows)),
        "by_period_end_year": dict(Counter(str(row["period_end_date"])[:4] for row in rows)),
        "by_fiscal_quarter": dict(Counter(row["fiscal_quarter"] for row in rows)),
    }


def legacy_recoverability_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, dict[str, int]] = {}
    for field in CORE_FIELDS:
        field_rows = [row for row in rows if row["field"] == field]
        out[field] = {
            "v3_null": len(field_rows),
            "legacy_value_available_on_exact_period": sum(row["legacy_value_available"] for row in field_rows),
        }
    return out


def row_counts(v3: sqlite3.Connection, v2: sqlite3.Connection, legacy: sqlite3.Connection) -> dict[str, dict[str, int]]:
    return {
        "v3": _counts(v3, ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_migration_audit", "v3_resolution_issue")),
        "v2": _counts(v2, ("rc_v2_company", "rc_v2_quarter", "rc_v2_fundamental_quarterly")),
        "legacy": _counts(legacy, ("rc_fundamental_quarterly", "rc_fundamental_yahoo_quarterly")),
    }


def _counts(conn: sqlite3.Connection, tables: tuple[str, ...]) -> dict[str, int]:
    return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}


def _is_missing(row: dict[str, Any], field: str) -> bool:
    return row[field] is None or (field == "shares_outstanding" and row[field] <= 0)


def _field_outcome(source: dict[str, Any], field: str) -> str:
    outcomes = source.get("field_outcomes", {}).get(field, [])
    return ";".join(outcomes)


def _yahoo_derivable(field: str, row: dict[str, Any]) -> bool:
    if field == "free_cashflow":
        return row.get("operating_cashflow") is not None and row.get("capex") is not None
    return False


def _within_identity_tolerance(left: float, right: float, tolerance: float) -> bool:
    return abs(left - right) <= max(max(abs(left), abs(right)) * tolerance, NEAR_ZERO_ABSOLUTE_FLOOR)


def _sign_mismatch(field: str, left: float, right: float) -> bool:
    if field not in SIGN_SENSITIVE:
        return False
    if abs(left) <= NEAR_ZERO_ABSOLUTE_FLOOR or abs(right) <= NEAR_ZERO_ABSOLUTE_FLOOR:
        return False
    return (left < 0 < right) or (right < 0 < left)


def _tier(field: str) -> str:
    if field in TIER_A:
        return "A"
    if field in TIER_B:
        return "B"
    return "C"


def _float_or_none(value: Any) -> float | None:
    return None if value is None else float(value)


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    index = min(len(values) - 1, max(0, math.ceil(len(values) * percentile) - 1))
    return values[index]


def _signature(row: dict[str, Any]) -> str:
    return "+".join(field for field in CORE_FIELDS if row[f"missing_{field}"]) or "NONE"


def _source_ticker(source_key: str) -> str:
    return source_key.split(":")[1] if ":" in source_key else ""


def _source_period(source_key: str) -> str:
    parts = source_key.split(":")
    return parts[2] if len(parts) >= 3 else ""


def _source_fy(source_key: str) -> int | None:
    return None


def _source_fq(source_key: str) -> str | None:
    return None


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def signature_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"signature": sig, "count": count} for sig, count in Counter(_signature(row) for row in rows).most_common()]


def missing_by_company_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return company_concentration(rows)["top_30"]


def missing_by_year_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    c = Counter(str(row["fiscal_year"]) for row in rows)
    return [{"fiscal_year": key, "affected_q": value} for key, value in sorted(c.items())]


def missing_by_fq_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    c = Counter(row["fiscal_quarter"] for row in rows)
    return [{"fiscal_quarter": key, "affected_q": value} for key, value in sorted(c.items())]


def missing_vs_issue_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    c = Counter(row["resolution_issue_types"] or "NO_RESOLUTION_ISSUE" for row in rows)
    return [{"resolution_issue_types": key, "affected_q": value} for key, value in c.most_common()]


def distribution_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"field": field, **values} for field, values in sorted(summary.items())]


def v2_core_recovery_rows(field_diagnostics: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return field_recovery_matrix(field_diagnostics)


def write_samples(root: Path, agreements: list[dict[str, Any]], classes: list[dict[str, Any]]) -> None:
    by_class = {row["classification"]: [] for row in classes}
    for row in classes:
        by_class.setdefault(row["classification"], []).append(row)
    agreement_map: dict[tuple[str, int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in agreements:
        agreement_map[(row["ticker"], row["fiscal_year"], row["fiscal_quarter"])].append(row)
    mapping = {
        "STRONG_MATCH": "v2_strong_match_sample.csv",
        "PROBABLE_MATCH": "v2_probable_match_sample.csv",
        "CONFLICT": "v2_conflict_sample.csv",
        "PERIOD_IDENTITY_CONFLICT": "v2_conflict_sample.csv",
        "INSUFFICIENT_EVIDENCE": "v2_insufficient_evidence_sample.csv",
        "STRONG_MATCH_LIMITED_FIELDS": "v2_strong_match_limited_fields_sample.csv",
    }
    for class_name, filename in mapping.items():
        source = by_class.get(class_name, [])
        limit = 30 if class_name == "STRONG_MATCH" else 20
        write_csv(root / filename, _sample_detail_rows(source[:limit], agreement_map))


def _sample_detail_rows(samples: list[dict[str, Any]], agreement_map: dict[tuple[str, int, str], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sample in samples:
        key = (sample["ticker"], sample["fiscal_year"], sample["fiscal_quarter"])
        for agreement in agreement_map.get(key, []):
            rows.append(
                {
                    "ticker": sample["ticker"],
                    "fiscal_year": sample["fiscal_year"],
                    "fiscal_quarter": sample["fiscal_quarter"],
                    "v3_period_end_date": sample["v3_period_end_date"],
                    "v2_period_end_date": sample["v2_period_end_date"],
                    "period_relation": sample["period_relation"],
                    "classification": sample["classification"],
                    "field": agreement["field"],
                    "tier": agreement["tier"],
                    "v3_value": agreement["v3_value"],
                    "v2_value": agreement["v2_value"],
                    "relative_difference": agreement["relative_difference"],
                    "within_1pct": agreement["within_1pct"],
                    "within_2pct": agreement["within_2pct"],
                    "within_5pct": agreement["within_5pct"],
                    "within_10pct": agreement["within_10pct"],
                    "sign_mismatch": agreement["sign_mismatch"],
                    "status": agreement["status"],
                }
            )
    return rows


def recommended_gate_text() -> str:
    return """# Recommended V2 Same-Quarter Gate

1. Start with exact `market + ticker + fiscal_year + fiscal_quarter`.
2. Treat that as `IDENTITY_CANDIDATE_MATCH`, not proof.
3. Require period-end relation to be `EXACT_PERIOD_END`, `SMALL_KNOWN_PROVIDER_VARIANT`, or documented `KNOWN_FISCAL_CALENDAR_VARIANT`.
4. Compare Tier A fingerprint fields: revenue, net_income, operating_cashflow, cash, total_debt.
5. Use 5% identity-evidence tolerance with a 10,000 absolute near-zero floor and sign-mismatch protection for income/cashflow fields.
6. `STRONG_MATCH` requires at least three comparable fields, at least two Tier A fields, at least two Tier A matches, no Tier A conflicts, and nearly all comparable fields matching.
7. `STRONG_MATCH_LIMITED_FIELDS`, `PROBABLE_MATCH`, and `INSUFFICIENT_EVIDENCE` are hold-for-review for automatic Phase 3C writes.
8. `CONFLICT` and `PERIOD_IDENTITY_CONFLICT` block automatic V2 enrichment.
9. Even with `STRONG_MATCH`, Phase 3C may only NULL-fill; it must not overwrite existing Yahoo canonical values.
"""
