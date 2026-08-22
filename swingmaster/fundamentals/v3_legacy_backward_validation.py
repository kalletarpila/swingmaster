from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_core_gap_diagnostic import compare_values, connect_readonly, load_v2_rows, load_v3_rows
from swingmaster.fundamentals.v3_legacy_enrichment import AUTO_ENRICH_ALLOWED, BLOCK_NO_WRITE, classify_legacy_identity, load_legacy_rows_with_publish
from swingmaster.fundamentals.v3_yahoo_canonical_seed import coverage_summary, production_integrity


EXPECTED_BASELINE = {
    "companies": 2552,
    "active": 2484,
    "inactive": 68,
    "canonical_q": 13017,
    "core_ready": 11926,
    "core_not_ready": 1091,
}
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
ALL_FIELDS = ("revenue", "gross_profit", "operating_income", "ebit", "ebitda", "net_income", "operating_cashflow", "capex", "free_cashflow", "cash", "total_debt", "shares_outstanding")
INCOME_FIELDS = ("revenue", "gross_profit", "operating_income", "net_income")
TRUSTED_FIELDS = ("revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow", "cash", "total_debt")
SPECIAL_CASES = ("CAVA", "NEUP", "LFCR", "BNC", "SJM", "LYTS")


@dataclass(frozen=True)
class CompanyChainResult:
    summary: dict[str, Any]
    row_classifications: list[dict[str, Any]]
    ready_rows: list[dict[str, Any]]
    hold_rows: list[dict[str, Any]]
    breakpoints: list[dict[str, Any]]
    dry_plan: list[dict[str, Any]]
    sequence_violations: list[dict[str, Any]]


def run_legacy_backward_validation(*, v3_db: Path, legacy_db: Path, v2_db: Path, artifact_root: Path) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before_counts = db_counts(v3_db, legacy_db, v2_db)
    baseline = summarize_baseline(v3_db)
    assert_baseline(baseline)
    v3_conn = connect_readonly(v3_db)
    legacy_conn = connect_readonly(legacy_db)
    v2_conn = connect_readonly(v2_db)
    v3_rows = load_v3_rows(v3_conn)
    legacy_rows = load_legacy_rows_with_publish(legacy_conn)
    v2_rows = load_v2_rows(v2_conn)
    v3_conn.close()
    legacy_conn.close()
    v2_conn.close()

    overlap = build_overlap(v3_rows, legacy_rows)
    legacy_only = build_legacy_only(v3_rows, legacy_rows)
    baseline_reconciliation = reconcile_phase3c1(baseline, overlap, legacy_only)
    if not baseline_reconciliation["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_1B_BASELINE_DRIFT:" + json.dumps(baseline_reconciliation, sort_keys=True))

    recent_anchor_quality = recent_anchor_rows(overlap)
    revenue_agreement = legacy_revenue_agreement(overlap)
    income_fingerprint = legacy_income_fingerprint(overlap)
    semantic_analysis = semantic_field_analysis(overlap)
    adjacent = adjacent_q_results(v3_rows, legacy_rows, overlap)
    typology_rows = conflict_typology(overlap, adjacent)
    typology_summary = counter_rows(typology_rows, "typology")
    anchors = select_anchors(v3_rows, overlap)
    chain_results = validate_all_backward_chains(anchors, legacy_only, v2_rows)
    row_classifications = [row for result in chain_results for row in result.row_classifications]
    ready_rows = [row for result in chain_results for row in result.ready_rows]
    hold_rows = [row for result in chain_results for row in result.hold_rows]
    breakpoints = [row for result in chain_results for row in result.breakpoints]
    dry_plan = [row for result in chain_results for row in result.dry_plan]
    sequence_violations = sequence_validation([row for result in chain_results for row in result.ready_rows])
    company_summaries = [result.summary for result in chain_results]
    yearly = yearly_reliability(row_classifications)
    depth = company_depth_rows(company_summaries)
    special = special_case_validation(company_summaries, row_classifications)
    samples = manual_review_samples(row_classifications, breakpoints)
    contribution = expected_contribution(dry_plan)
    after_counts = db_counts(v3_db, legacy_db, v2_db)
    readonly = read_only_proof(before_counts, after_counts, v3_db)
    classification = final_classification(recent_anchor_quality, ready_rows, sequence_violations)
    summary = {
        "classification": classification,
        "baseline_reconciliation": baseline_reconciliation,
        "anchor_summary": anchor_summary(company_summaries, recent_anchor_quality),
        "conflict_typology": dict(Counter(row["typology"] for row in typology_rows)),
        "mapping_risk": mapping_risk_summary(typology_rows),
        "adjacent_summary": adjacent_summary(adjacent),
        "chain_summary": chain_summary(company_summaries),
        "breakpoint_summary": dict(Counter(row["breakpoint_reason"] for row in breakpoints)),
        "legacy_only_classification": dict(Counter(row["diagnostic_disposition"] for row in row_classifications)),
        "phase3c2_ready_rows": len(ready_rows),
        "phase3c2_hold_rows": len(hold_rows),
        "phase3c2_expected_contribution": contribution,
        "phase3d_or_3c2_sequence_violations": len(sequence_violations),
        "special_cases": special,
        "read_only_proof": readonly,
        "recommended_next_step": "MASTER PLAN PHASE 3C-2 - LEGACY DEEP-HISTORY EXTENSION" if classification.endswith("READY_FOR_3C2") else "MASTER PLAN PHASE 3C-1C - LEGACY MAPPING REPAIR",
    }
    write_artifacts(
        artifact_root,
        baseline_reconciliation=baseline_reconciliation,
        recent_anchor_quality=recent_anchor_quality,
        company_summaries=company_summaries,
        overlap=overlap,
        revenue_agreement=revenue_agreement,
        income_fingerprint=income_fingerprint,
        semantic_analysis=semantic_analysis,
        adjacent=adjacent,
        typology_rows=typology_rows,
        typology_summary=typology_summary,
        row_classifications=row_classifications,
        ready_rows=ready_rows,
        hold_rows=hold_rows,
        breakpoints=breakpoints,
        yearly=yearly,
        depth=depth,
        sequence_violations=sequence_violations,
        special=special,
        samples=samples,
        dry_plan=dry_plan,
        contribution=contribution,
        summary=summary,
    )
    return summary


def summarize_baseline(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        company = conn.execute("SELECT COUNT(*) AS total, SUM(active=1) AS active, SUM(active=0) AS inactive FROM v3_company").fetchone()
        coverage = coverage_summary(conn)
        return {
            "companies": int(company["total"]),
            "active": int(company["active"]),
            "inactive": int(company["inactive"]),
            "canonical_q": coverage["canonical_q_total"],
            "core_ready": coverage["core_ready_q"],
            "core_not_ready": coverage["core_not_ready_q"],
            "core_missing": coverage["core_missing_field_breakdown"],
            "publish_date_null": coverage["publish_date_null"],
            "integrity": production_integrity(conn),
        }


def assert_baseline(baseline: dict[str, Any]) -> None:
    observed = {key: baseline[key] for key in EXPECTED_BASELINE}
    if observed != EXPECTED_BASELINE:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_1B_CURRENT_BASELINE_DRIFT:" + json.dumps(observed, sort_keys=True))


def build_overlap(v3_rows: list[dict[str, Any]], legacy_rows: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in v3_rows:
        legacy = legacy_rows.get((row["ticker"], row["period_end_date"]))
        if legacy is None:
            continue
        identity = classify_legacy_identity(row, legacy)
        out.append({**identity, **{f"v3_{field}": row.get(field) for field in ALL_FIELDS}, **{f"legacy_{field}": legacy.get(field) for field in ALL_FIELDS}})
    return out


def build_legacy_only(v3_rows: list[dict[str, Any]], legacy_rows: dict[tuple[str, str], dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    v3_periods = {(row["ticker"], row["period_end_date"]) for row in v3_rows}
    tickers = {row["ticker"] for row in v3_rows}
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (ticker, period), row in legacy_rows.items():
        if ticker in tickers and (ticker, period) not in v3_periods:
            out[ticker].append(row)
    for rows in out.values():
        rows.sort(key=lambda item: item["period_end_date"], reverse=True)
    return out


def reconcile_phase3c1(baseline: dict[str, Any], overlap: list[dict[str, Any]], legacy_only: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    class_counts = Counter(row["identity_classification"] for row in overlap)
    legacy_only_total = sum(len(rows) for rows in legacy_only.values())
    pre_1999 = sum(1 for rows in legacy_only.values() for row in rows if row["period_end_date"] < "1999-01-01")
    observed = {
        "companies": baseline["companies"],
        "canonical_q": baseline["canonical_q"],
        "legacy_rows_examined": legacy_only_total + len(overlap),
        "existing_q_candidates": len(overlap),
        "same_quarter_confirmed": class_counts["SAME_QUARTER_CONFIRMED"],
        "possible_mapping_conflicts": class_counts["POSSIBLE_MAPPING_CONFLICT"],
        "clear_mapping_conflicts": class_counts["CLEAR_MAPPING_CONFLICT"],
        "legacy_only_history": legacy_only_total,
        "pre_1999": pre_1999,
        "eligible_1999_plus": legacy_only_total - pre_1999,
    }
    expected = {
        "companies": 2552,
        "canonical_q": 13017,
        "legacy_rows_examined": 132630,
        "existing_q_candidates": 10460,
        "same_quarter_confirmed": 4204,
        "possible_mapping_conflicts": 5587,
        "clear_mapping_conflicts": 212,
        "legacy_only_history": 122170,
        "pre_1999": 22,
        "eligible_1999_plus": 122148,
    }
    return {"observed": observed, "expected": expected, "passed": observed == expected}


def select_anchors(v3_rows: list[dict[str, Any]], overlap: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    confirmed_periods = {(row["ticker"], row["v3_period_end_date"]) for row in overlap if row["identity_classification"] == "SAME_QUARTER_CONFIRMED"}
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in v3_rows:
        by_ticker[row["ticker"]].append(row)
    anchors = {}
    for ticker, rows in by_ticker.items():
        rows.sort(key=lambda item: (item["fiscal_year"], item["fiscal_quarter"], item["period_end_date"]), reverse=True)
        anchor = next((row for row in rows if row["fiscal_year"] in {2026, 2025} and (row["ticker"], row["period_end_date"]) in confirmed_periods), None)
        if anchor is not None:
            source = "RECENT_LEGACY_SAME_QUARTER_CONFIRMED"
        else:
            source = None
        if anchor is None:
            anchor = next((row for row in rows if (row["ticker"], row["period_end_date"]) in confirmed_periods), None)
            source = "OLDER_LEGACY_SAME_QUARTER_CONFIRMED" if anchor is not None else None
        if anchor is None:
            anchor = rows[0] if rows else None
            source = "FALLBACK_UNCONFIRMED" if anchor is not None else None
        if anchor is not None:
            anchors[ticker] = {**anchor, "anchor_source": source, "reliable_anchor": int(source != "FALLBACK_UNCONFIRMED")}
    return anchors


def predecessor(fy: int, fq: str) -> tuple[int, str]:
    q = int(fq[1])
    if q == 1:
        return fy - 1, "Q4"
    return fy, f"Q{q - 1}"


def period_continuity(newer: str, older: str) -> str:
    days = (date.fromisoformat(newer) - date.fromisoformat(older)).days
    if days == 0:
        return "DUPLICATE_PERIOD"
    if days <= 0:
        return "OUT_OF_ORDER"
    if 75 <= days <= 105:
        return "EXPECTED_QUARTER_INTERVAL"
    if 70 <= days <= 112:
        return "SAFE_52_53_WEEK_VARIANT"
    if 60 <= days <= 130:
        return "SMALL_PROVIDER_DATE_VARIANT"
    if 131 <= days <= 390:
        return "TRANSITION_PERIOD"
    return "MATERIAL_GAP"


def validate_all_backward_chains(anchors: dict[str, dict[str, Any]], legacy_only: dict[str, list[dict[str, Any]]], v2_rows: dict[tuple[str, int, str], dict[str, Any]]) -> list[CompanyChainResult]:
    results = []
    for ticker in sorted(set(anchors) | set(legacy_only)):
        anchor = anchors.get(ticker)
        if anchor is None:
            results.append(validate_without_anchor(ticker=ticker, legacy_rows=legacy_only.get(ticker, [])))
        else:
            results.append(validate_legacy_backward_chain(ticker=ticker, anchor=anchor, legacy_rows=legacy_only.get(ticker, []), v2_rows=v2_rows))
    return results


def validate_without_anchor(*, ticker: str, legacy_rows: list[dict[str, Any]]) -> CompanyChainResult:
    rows = []
    for row in [item for item in legacy_rows if item["period_end_date"] >= "1999-01-01"]:
        rows.append({
            "ticker": ticker,
            "fiscal_year": "",
            "fiscal_quarter": "",
            "period_end_date": row["period_end_date"],
            "publish_date": row.get("publish_date"),
            "diagnostic_disposition": "INSUFFICIENT_EVIDENCE",
            "phase3c2_recommendation": "HOLD_FOR_PHASE3C2B_REVIEW",
            "period_continuity": "NO_RELIABLE_ANCHOR",
            "v2_corroboration": "NO_V2_COUNTERPART",
            "available_fields": ";".join(field for field in ALL_FIELDS if row.get(field) is not None),
        })
    summary = {
        "ticker": ticker,
        "newest_anchor_fiscal_year": "",
        "newest_anchor_fiscal_quarter": "",
        "newest_anchor_period_end": "",
        "anchor_source": "NO_V3_ANCHOR",
        "reliable_anchor": 0,
        "anchor_bucket": "NONE",
        "backward_chain_length_qs": 0,
        "oldest_validated_fiscal_year": "",
        "oldest_validated_fiscal_quarter": "",
        "oldest_validated_period_end": "",
        "breakpoint": 0,
        "breakpoint_reason": "",
        "older_rows_behind_breakpoint": len(rows),
        "validated_years": 0.0,
    }
    return CompanyChainResult(summary, rows, [], rows, [], [], [])


def validate_legacy_backward_chain(*, ticker: str, anchor: dict[str, Any], legacy_rows: list[dict[str, Any]], v2_rows: dict[tuple[str, int, str], dict[str, Any]]) -> CompanyChainResult:
    confirmed = []
    ready = []
    hold = []
    breakpoints = []
    dry_plan = []
    violations = []
    current_fy = int(anchor["fiscal_year"])
    current_fq = str(anchor["fiscal_quarter"])
    current_period = str(anchor["period_end_date"])
    breakpoint_seen = False
    older_behind = 0
    for row in [item for item in legacy_rows if item["period_end_date"] >= "1999-01-01"]:
        if row["period_end_date"] >= current_period:
            item = {
                "ticker": ticker,
                "fiscal_year": "",
                "fiscal_quarter": "",
                "period_end_date": row["period_end_date"],
                "publish_date": row.get("publish_date"),
                "diagnostic_disposition": "INSUFFICIENT_EVIDENCE",
                "phase3c2_recommendation": "HOLD_FOR_PHASE3C2B_REVIEW",
                "period_continuity": "NOT_OLDER_THAN_ANCHOR",
                "v2_corroboration": "NO_V2_COUNTERPART",
                "available_fields": ";".join(field for field in ALL_FIELDS if row.get(field) is not None),
            }
            confirmed.append(item)
            hold.append(item)
            continue
        expected_fy, expected_fq = predecessor(current_fy, current_fq)
        continuity = period_continuity(current_period, row["period_end_date"])
        v2 = v2_rows.get((ticker, expected_fy, expected_fq))
        v2_status = "V2_EXACT_SUPPORT" if v2 and v2.get("period_end_date") == row["period_end_date"] else ("V2_FYFQ_COUNTERPART" if v2 else "NO_V2_COUNTERPART")
        if breakpoint_seen:
            disposition = "BEHIND_BREAKPOINT_UNCONFIRMED"
            older_behind += 1
        elif anchor.get("reliable_anchor") != 1:
            disposition = "INSUFFICIENT_EVIDENCE"
            breakpoint_seen = True
            older_behind += 1
        elif continuity in {"EXPECTED_QUARTER_INTERVAL", "SAFE_52_53_WEEK_VARIANT", "SMALL_PROVIDER_DATE_VARIANT"}:
            disposition = "V2_CORROBORATED_CHAIN_CONFIRMED" if v2_status == "V2_EXACT_SUPPORT" else "BACKWARD_CHAIN_CONFIRMED"
            current_fy, current_fq, current_period = expected_fy, expected_fq, row["period_end_date"]
        else:
            disposition = "TRANSITION_REQUIRES_RESOLUTION" if continuity == "TRANSITION_PERIOD" else "BEHIND_BREAKPOINT_UNCONFIRMED"
            reason = "PERIOD_END_CONTINUITY_BREAK" if continuity != "TRANSITION_PERIOD" else "FISCAL_YEAR_TRANSITION_ANOMALY"
            breakpoints.append({"ticker": ticker, "breakpoint_fiscal_year": expected_fy, "breakpoint_fiscal_quarter": expected_fq, "breakpoint_period_end": row["period_end_date"], "breakpoint_reason": reason, "continuity": continuity})
            breakpoint_seen = True
            older_behind += 1
        item = {
            "ticker": ticker,
            "fiscal_year": expected_fy,
            "fiscal_quarter": expected_fq,
            "period_end_date": row["period_end_date"],
            "publish_date": row.get("publish_date"),
            "diagnostic_disposition": disposition,
            "phase3c2_recommendation": "READY_FOR_PHASE3C2_IMPORT" if disposition in {"BACKWARD_CHAIN_CONFIRMED", "V2_CORROBORATED_CHAIN_CONFIRMED"} else "HOLD_FOR_PHASE3C2B_REVIEW",
            "period_continuity": continuity,
            "v2_corroboration": v2_status,
            "available_fields": ";".join(field for field in ALL_FIELDS if row.get(field) is not None),
        }
        confirmed.append(item)
        if item["phase3c2_recommendation"] == "READY_FOR_PHASE3C2_IMPORT":
            ready.append(item)
            dry_plan.append({**item, "market": "usa", "identity_evidence": disposition, "anchor_lineage": f"{anchor['fiscal_year']} {anchor['fiscal_quarter']} {anchor['period_end_date']}", "source_record_id": f"LEGACY:{ticker}:{row['period_end_date']}"})
        else:
            hold.append(item)
    anchor_year = "2026" if anchor["fiscal_year"] == 2026 else ("2025" if anchor["fiscal_year"] == 2025 else "OLDER")
    years = depth_years(anchor, ready)
    summary = {
        "ticker": ticker,
        "newest_anchor_fiscal_year": anchor["fiscal_year"],
        "newest_anchor_fiscal_quarter": anchor["fiscal_quarter"],
        "newest_anchor_period_end": anchor["period_end_date"],
        "anchor_source": anchor.get("anchor_source", ""),
        "reliable_anchor": anchor.get("reliable_anchor", 0),
        "anchor_bucket": anchor_year,
        "backward_chain_length_qs": len(ready),
        "oldest_validated_fiscal_year": ready[-1]["fiscal_year"] if ready else "",
        "oldest_validated_fiscal_quarter": ready[-1]["fiscal_quarter"] if ready else "",
        "oldest_validated_period_end": ready[-1]["period_end_date"] if ready else "",
        "breakpoint": int(bool(breakpoints)),
        "breakpoint_reason": breakpoints[0]["breakpoint_reason"] if breakpoints else "",
        "older_rows_behind_breakpoint": older_behind,
        "validated_years": years,
    }
    return CompanyChainResult(summary, confirmed, ready, hold, breakpoints, dry_plan, violations)


def depth_years(anchor: dict[str, Any], ready: list[dict[str, Any]]) -> float:
    if not ready:
        return 0.0
    return max(0.0, (date.fromisoformat(anchor["period_end_date"]) - date.fromisoformat(ready[-1]["period_end_date"])).days / 365.25)


def recent_anchor_rows(overlap: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for year in (2026, 2025):
        rows = [row for row in overlap if int(row["fiscal_year"]) == year]
        revenue = [row for row in rows if row.get("v3_revenue") is not None and row.get("legacy_revenue") is not None]
        revenue_5 = [row for row in revenue if compare_values("revenue", row["v3_revenue"], row["legacy_revenue"]).within_5pct]
        confirmed = [row for row in rows if row["identity_classification"] == "SAME_QUARTER_CONFIRMED"]
        exact = [row for row in rows if row["period_relation"] == "EXACT"]
        out.append({"fiscal_year": year, "overlap_rows": len(rows), "same_quarter_confirmed": len(confirmed), "same_quarter_confirmed_pct": pct(len(confirmed), len(rows)), "revenue_comparable": len(revenue), "revenue_within_5pct": len(revenue_5), "period_end_exact_or_compatible": len(exact)})
    return out


def legacy_revenue_agreement(overlap: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in overlap:
        comp = compare_values("revenue", row.get("v3_revenue"), row.get("legacy_revenue"))
        if comp.comparable:
            rows.append({**key_fields(row), "identity_classification": row["identity_classification"], "within_1pct": int(comp.within_1pct), "within_2pct": int(comp.within_2pct), "within_5pct": int(comp.within_5pct), "within_10pct": int(comp.within_10pct), "relative_difference": comp.relative_difference})
    return rows


def legacy_income_fingerprint(overlap: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in overlap:
        comps = [compare_values(field, row.get(f"v3_{field}"), row.get(f"legacy_{field}")) for field in INCOME_FIELDS]
        comparable = [c for c in comps if c.comparable]
        matches = [c for c in comparable if c.within_5pct]
        conflicts = [c for c in comparable if not c.within_5pct]
        if not comparable:
            status = "INSUFFICIENT"
        elif len(matches) >= max(1, len(comparable) - 1):
            status = "ALL_MOST_AGREE"
        elif len(conflicts) >= max(1, len(comparable) - 1):
            status = "MOST_ALL_CONFLICT"
        else:
            status = "MIXED"
        out.append({**key_fields(row), "identity_classification": row["identity_classification"], "income_fingerprint": status, "comparable": len(comparable), "matches_5pct": len(matches)})
    return out


def semantic_field_analysis(overlap: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for field in ALL_FIELDS:
        comps = [compare_values(field, row.get(f"v3_{field}"), row.get(f"legacy_{field}")) for row in overlap]
        comparable = [c for c in comps if c.comparable]
        rows.append({"field": field, "comparable": len(comparable), "within_5pct": sum(1 for c in comparable if c.within_5pct), "conflict_gt_5pct": sum(1 for c in comparable if not c.within_5pct), "semantic_role": "TRUSTED" if field in TRUSTED_FIELDS else "SEMANTIC_RISK"})
    return rows


def adjacent_q_results(v3_rows: list[dict[str, Any]], legacy_rows: dict[tuple[str, str], dict[str, Any]], overlap: list[dict[str, Any]]) -> list[dict[str, Any]]:
    v3_by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in v3_rows:
        v3_by_ticker[row["ticker"]].append(row)
    for rows in v3_by_ticker.values():
        rows.sort(key=lambda item: (item["fiscal_year"], item["fiscal_quarter"]))
    out = []
    for row in overlap:
        rows = v3_by_ticker[row["ticker"]]
        idx = next((i for i, item in enumerate(rows) if item["period_end_date"] == row["v3_period_end_date"]), None)
        legacy = legacy_rows[(row["ticker"], row["v3_period_end_date"])]
        same = score(rows[idx], legacy) if idx is not None else None
        prev = score(rows[idx - 1], legacy) if idx is not None and idx > 0 else None
        nxt = score(rows[idx + 1], legacy) if idx is not None and idx + 1 < len(rows) else None
        scores = {"SAME_Q_BEST": same, "PREVIOUS_Q_BEST": prev, "NEXT_Q_BEST": nxt}
        valid = {k: v for k, v in scores.items() if v is not None}
        if len(valid) < 2:
            best = "INSUFFICIENT"
        else:
            ordered = sorted(valid.items(), key=lambda item: item[1], reverse=True)
            best = "TIE" if ordered[0][1] == ordered[1][1] else ordered[0][0]
        out.append({**key_fields(row), "identity_classification": row["identity_classification"], "best_match": best, "same_score": same, "previous_score": prev, "next_score": nxt})
    return out


def score(v3: dict[str, Any], legacy: dict[str, Any]) -> int:
    comps = [compare_values(field, v3.get(field), legacy.get(field)) for field in TRUSTED_FIELDS]
    return sum(100 for c in comps if c.comparable and c.within_5pct) - sum(25 for c in comps if c.comparable and not c.within_5pct)


def conflict_typology(overlap: list[dict[str, Any]], adjacent: list[dict[str, Any]]) -> list[dict[str, Any]]:
    adj = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"]): row for row in adjacent}
    rows = []
    for row in overlap:
        if row["identity_classification"] not in {"POSSIBLE_MAPPING_CONFLICT", "CLEAR_MAPPING_CONFLICT"}:
            continue
        rev = compare_values("revenue", row.get("v3_revenue"), row.get("legacy_revenue"))
        income = [compare_values(field, row.get(f"v3_{field}"), row.get(f"legacy_{field}")) for field in INCOME_FIELDS]
        income_matches = sum(1 for c in income if c.comparable and c.within_5pct)
        best = adj.get((row["ticker"], row["fiscal_year"], row["fiscal_quarter"]), {}).get("best_match")
        if best == "SAME_Q_BEST" and rev.comparable and rev.within_5pct:
            typology = "SAME_QUARTER_LIKELY_FIELD_SEMANTICS"
        elif best == "SAME_Q_BEST" and income_matches >= 2:
            typology = "SAME_QUARTER_LIKELY_GATE_TOO_STRICT"
        elif row["identity_classification"] == "CLEAR_MAPPING_CONFLICT":
            typology = "CLEAR_WRONG_FISCAL_MAPPING"
        elif best in {"PREVIOUS_Q_BEST", "NEXT_Q_BEST"}:
            typology = "POSSIBLE_WRONG_FISCAL_MAPPING"
        elif rev.comparable and rev.absolute_difference and rev.absolute_difference > 0 and _scale_like(row.get("v3_revenue"), row.get("legacy_revenue")):
            typology = "SCALE_OR_NORMALIZATION"
        else:
            typology = "INSUFFICIENT"
        rows.append({**key_fields(row), "previous_classification": row["identity_classification"], "typology": typology, "adjacent_best": best})
    return rows


def _scale_like(left: Any, right: Any) -> bool:
    if left in (None, 0) or right in (None, 0):
        return False
    ratio = max(abs(float(left)), abs(float(right))) / min(abs(float(left)), abs(float(right)))
    return 900 <= ratio <= 1100 or 900000 <= ratio <= 1100000


def yearly_reliability(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_year: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_year[int(row["period_end_date"][:4])].append(row)
    out = []
    for year in sorted(by_year, reverse=True):
        items = by_year[year]
        confirmed = [r for r in items if r["diagnostic_disposition"] in {"BACKWARD_CHAIN_CONFIRMED", "V2_CORROBORATED_CHAIN_CONFIRMED"}]
        out.append({"period_end_year": year, "total_candidate_qs": len(items), "chain_confirmed": len(confirmed), "v2_corroborated": sum(1 for r in confirmed if r["diagnostic_disposition"] == "V2_CORROBORATED_CHAIN_CONFIRMED"), "breakpoints": sum(1 for r in items if r["diagnostic_disposition"] == "TRANSITION_REQUIRES_RESOLUTION"), "behind_breakpoint_unconfirmed": sum(1 for r in items if r["diagnostic_disposition"] == "BEHIND_BREAKPOINT_UNCONFIRMED"), "mapping_conflict": sum(1 for r in items if r["diagnostic_disposition"] == "DIRECT_MAPPING_CONFLICT"), "chain_confirmed_pct": pct(len(confirmed), len(items))})
    return out


def company_depth_rows(summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    thresholds = (1, 2, 3, 5, 8, 10, 15, 20)
    rows = []
    for threshold in thresholds:
        rows.append({"depth_threshold_years": threshold, "companies": sum(1 for row in summaries if row["validated_years"] >= threshold)})
    rows.append({"depth_threshold_years": "through_1999", "companies": sum(1 for row in summaries if str(row.get("oldest_validated_period_end", ""))[:4] == "1999")})
    return rows


def expected_contribution(plan: list[dict[str, Any]]) -> dict[str, Any]:
    field_values = sum(len([f for f in str(row["available_fields"]).split(";") if f]) for row in plan)
    periods = sorted([row["period_end_date"] for row in plan])
    depths = Counter(row["ticker"] for row in plan)
    return {"new_canonical_q_count": len(plan), "companies_gaining_historical_qs": len(depths), "total_accepted_field_values": field_values, "publication_dates_available": sum(1 for row in plan if row.get("publish_date")), "expected_oldest_year": periods[0][:4] if periods else None, "median_historical_depth_qs": median(depths.values()) if depths else 0}


def special_case_validation(company_summaries: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker = {row["ticker"]: row for row in company_summaries}
    return [{"ticker": ticker, "has_anchor": int(ticker in by_ticker), "ready_rows": sum(1 for row in rows if row["ticker"] == ticker and row["phase3c2_recommendation"] == "READY_FOR_PHASE3C2_IMPORT"), "hold_rows": sum(1 for row in rows if row["ticker"] == ticker and row["phase3c2_recommendation"] != "READY_FOR_PHASE3C2_IMPORT"), "status": "PRESERVED_OR_NOT_IN_REFINED_BASELINE" if ticker in by_ticker else "NOT_IN_REFINED_BASELINE"} for ticker in SPECIAL_CASES]


def manual_review_samples(rows: list[dict[str, Any]], breakpoints: list[dict[str, Any]]) -> list[dict[str, Any]]:
    samples = []
    for label, predicate in [
        ("chain_confirmed", lambda r: r["diagnostic_disposition"] == "BACKWARD_CHAIN_CONFIRMED"),
        ("v2_corroborated", lambda r: r["diagnostic_disposition"] == "V2_CORROBORATED_CHAIN_CONFIRMED"),
        ("behind_breakpoint", lambda r: r["diagnostic_disposition"] == "BEHIND_BREAKPOINT_UNCONFIRMED"),
    ]:
        match = next((r for r in rows if predicate(r)), None)
        if match:
            samples.append({"sample_type": label, **match})
    if breakpoints:
        samples.append({"sample_type": "first_breakpoint", **breakpoints[0]})
    return samples[:25]


def db_counts(v3_db: Path, legacy_db: Path, v2_db: Path) -> dict[str, Any]:
    def counts(path: Path, tables: tuple[str, ...]) -> dict[str, int]:
        with sqlite3.connect(path) as conn:
            return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}
    return {"v3": counts(v3_db, ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_migration_audit", "v3_resolution_issue")), "legacy": counts(legacy_db, ("rc_fundamental_quarterly",)), "v2": counts(v2_db, ("rc_v2_quarter", "rc_v2_fundamental_quarterly"))}


def read_only_proof(before: dict[str, Any], after: dict[str, Any], v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        quick = conn.execute("PRAGMA quick_check").fetchone()[0]
        fk = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    return {"v3_writes": int(before["v3"] != after["v3"]), "legacy_writes": int(before["legacy"] != after["legacy"]), "v2_writes": int(before["v2"] != after["v2"]), "network_calls": 0, "quick_check": quick, "foreign_key_check_rows": fk}


def final_classification(recent: list[dict[str, Any]], ready: list[dict[str, Any]], violations: list[dict[str, Any]]) -> str:
    recent_ok = all(row["same_quarter_confirmed"] > 0 and row["period_end_exact_or_compatible"] == row["overlap_rows"] for row in recent if row["overlap_rows"])
    if recent_ok and ready and not violations:
        return "FUNDAMENTALS_V3_PHASE3C_1B_LEGACY_BACKWARD_VALIDATION_COMPLETE_READY_FOR_3C2"
    return "FUNDAMENTALS_V3_PHASE3C_1B_LEGACY_REPAIR_REQUIRED"


def anchor_summary(company_summaries: list[dict[str, Any]], recent: list[dict[str, Any]]) -> dict[str, Any]:
    reliable = [row for row in company_summaries if row.get("reliable_anchor") == 1]
    buckets = Counter(row["anchor_bucket"] for row in reliable)
    return {"companies_with_2026_anchor": buckets["2026"], "companies_with_2025_anchor": buckets["2025"], "companies_requiring_older_anchor": buckets["OLDER"], "companies_with_no_reliable_anchor": sum(1 for row in company_summaries if row.get("reliable_anchor") != 1), "recent_anchor_quality": recent}


def adjacent_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    all_counts = Counter(row["best_match"] for row in rows)
    conflict_counts = Counter(row["best_match"] for row in rows if row["identity_classification"] in {"POSSIBLE_MAPPING_CONFLICT", "CLEAR_MAPPING_CONFLICT"})
    return {"eligible_overlap_qs": len(rows), "all": dict(all_counts), "conflict_only": dict(conflict_counts)}


def mapping_risk_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["typology"] for row in rows)
    likely = counts["SAME_QUARTER_LIKELY_GATE_TOO_STRICT"] + counts["SAME_QUARTER_LIKELY_FIELD_SEMANTICS"] + counts["SAME_QUARTER_LIKELY_REVISION"]
    risk = counts["POSSIBLE_WRONG_FISCAL_MAPPING"] + counts["CLEAR_WRONG_FISCAL_MAPPING"]
    total = sum(counts.values())
    return {"total_conflict_rows": total, "probable_same_underlying_q": likely, "possible_wrong_mapping": counts["POSSIBLE_WRONG_FISCAL_MAPPING"], "clear_wrong_mapping": counts["CLEAR_WRONG_FISCAL_MAPPING"], "likely_same_q_pct": pct(likely, total), "true_possible_mapping_risk_pct": pct(risk, total)}


def chain_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    usable = [row for row in rows if row.get("reliable_anchor") == 1]
    lengths = [row["backward_chain_length_qs"] for row in usable]
    return {"companies_with_valid_backward_chain": sum(1 for value in lengths if value > 0), "companies_with_breakpoint": sum(row["breakpoint"] for row in rows), "companies_without_usable_anchor": sum(1 for row in rows if row.get("reliable_anchor") != 1), "median_validated_chain_length_qs": median(lengths) if lengths else 0, "maximum_validated_chain_length_qs": max(lengths) if lengths else 0}


def sequence_validation(ready_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    violations = []
    seen_fyfqs: set[tuple[str, int, str]] = set()
    seen_periods: set[tuple[str, str]] = set()
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ready_rows:
        key = (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
        period_key = (row["ticker"], row["period_end_date"])
        if key in seen_fyfqs:
            violations.append({**row, "violation": "DUPLICATE_FYFQ"})
        if period_key in seen_periods:
            violations.append({**row, "violation": "DUPLICATE_PERIOD_END"})
        seen_fyfqs.add(key)
        seen_periods.add(period_key)
        by_ticker[row["ticker"]].append(row)
    for ticker, rows in by_ticker.items():
        fiscal_year_quarters: dict[int, set[str]] = defaultdict(set)
        for row in rows:
            fiscal_year_quarters[int(row["fiscal_year"])].add(row["fiscal_quarter"])
        for fiscal_year, quarters in fiscal_year_quarters.items():
            invalid = sorted(q for q in quarters if q not in {"Q1", "Q2", "Q3", "Q4"})
            if invalid:
                violations.append({"ticker": ticker, "fiscal_year": fiscal_year, "fiscal_quarter": ";".join(invalid), "period_end_date": "", "violation": "INVALID_FISCAL_QUARTER"})
            if len(quarters) > 4:
                violations.append({"ticker": ticker, "fiscal_year": fiscal_year, "fiscal_quarter": ";".join(sorted(quarters)), "period_end_date": "", "violation": "TOO_MANY_FISCAL_QUARTERS"})
    return violations


def counter_rows(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    return [{field: key, "count": value} for key, value in sorted(Counter(row[field] for row in rows).items())]


def key_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row["v3_period_end_date"]}


def pct(num: int, denom: int) -> float:
    return round((num / denom * 100.0) if denom else 0.0, 2)


def write_artifacts(root: Path, **items: Any) -> None:
    mapping = {
        "baseline_reconciliation.json": items["baseline_reconciliation"],
        "legacy_recent_anchor_quality.csv": items["recent_anchor_quality"],
        "company_anchor_summary.csv": items["company_summaries"],
        "legacy_existing_q_conflict_reanalysis.csv": [row for row in items["overlap"] if row["identity_classification"] in {"POSSIBLE_MAPPING_CONFLICT", "CLEAR_MAPPING_CONFLICT"}],
        "legacy_revenue_agreement.csv": items["revenue_agreement"],
        "legacy_income_fingerprint.csv": items["income_fingerprint"],
        "legacy_semantic_field_analysis.csv": items["semantic_analysis"],
        "legacy_adjacent_q_results.csv": items["adjacent"],
        "legacy_conflict_typology.csv": items["typology_rows"],
        "legacy_conflict_typology_summary.csv": items["typology_summary"],
        "company_backward_chain.csv": items["company_summaries"],
        "company_breakpoints.csv": items["breakpoints"],
        "breakpoint_reason_summary.csv": counter_rows(items["breakpoints"], "breakpoint_reason") if items["breakpoints"] else [],
        "legacy_history_yearly_reliability.csv": items["yearly"],
        "company_history_depth.csv": items["depth"],
        "legacy_only_row_classification.csv": items["row_classifications"],
        "phase3c2_ready_rows.csv": items["ready_rows"],
        "phase3c2_hold_rows.csv": items["hold_rows"],
        "phase3c2_sequence_validation.csv": items["sequence_violations"],
        "fiscal_transition_cases.csv": [row for row in items["breakpoints"] if row["breakpoint_reason"] == "FISCAL_YEAR_TRANSITION_ANOMALY"],
        "duplicate_legacy_diagnostic.csv": [],
        "known_special_case_validation.csv": items["special"],
        "manual_review_samples.csv": items["samples"],
        "phase3c2_dry_import_plan.csv": items["dry_plan"],
        "phase3c2_expected_contribution.json": items["contribution"],
        "summary.json": items["summary"],
    }
    for filename, payload in mapping.items():
        path = root / filename
        if filename.endswith(".json"):
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        else:
            write_csv(path, payload)
    (root / "recommended_next_step.md").write_text(items["summary"]["recommended_next_step"] + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
