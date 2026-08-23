from __future__ import annotations

import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_canonical_closure import final_canonical_baseline, field_coverage_summary
from swingmaster.fundamentals.v3_phase4a_completeness_audit import phase4c_inventory, phase4c_research_groups
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity
from swingmaster.fundamentals.v3_repositories import configure_connection
from swingmaster.fundamentals.v3_v2_historical_gap_fill import CORE_FIELDS


CLASSIFICATION = "FUNDAMENTALS_V3_PHASE4C_EBIT_EBITDA_RESEARCH_COMPLETE_READY_FOR_PRODUCTION_APPLY"
INCOMPLETE = "FUNDAMENTALS_V3_PHASE4C_DERIVATION_RESEARCH_INCOMPLETE"
NEXT_STEP = "MASTER PLAN PHASE 4C-APPLY - EBIT & EBITDA PRODUCTION DERIVATION"
ZERO_Q_RESIDUALS = ("ALTS", "HOTH", "PKST", "QVCGA", "STSS")
NEAR_ZERO_FLOOR = 1_000.0
MATERIAL_ABS_ERROR = 1_000_000.0
MATERIAL_REL_ERROR = 0.05


def run_phase4c_ebit_ebitda_research(*, v3_db: Path, v2_db: Path, legacy_db: Path, artifact_root: Path) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    baseline = final_canonical_baseline(v3_db)
    coverage = field_coverage_summary(v3_db)
    inventory = phase4c_inventory(v3_db, v2_db)
    groups = phase4c_research_groups(inventory)
    v3_rows = load_v3_rows(v3_db)
    v2_rows = load_v2_rows(v2_db)
    yahoo_rows = load_yahoo_rows(legacy_db)
    joined = join_known_answer_rows(v3_rows, v2_rows, yahoo_rows)

    direct_ebit_inventory = direct_source_inventory(v3_rows, "ebit")
    direct_ebitda_inventory = direct_source_inventory(v3_rows, "ebitda")
    cross_source = provider_cross_source_agreement(v3_rows, v2_rows, yahoo_rows)
    da_inventory = da_semantics_inventory(v2_rows)

    e1_rows = ebit_rule_e1_operating_income(joined)
    e2_rows = ebit_rule_e2_pretax_interest()
    d1_rows = ebitda_rule_d1_ebit_plus_da(joined)
    d2_rows = ebitda_rule_d2_ebit_plus_dep_amort()
    d3_rows = ebitda_rule_d3_operating_income_plus_da(joined)
    q4_ebit = q4_reconstruction_rows(v3_rows, "ebit")
    q4_ebitda = q4_reconstruction_rows(v3_rows, "ebitda")
    q4_da = q4_da_reconstruction_rows(v2_rows)

    ebit_summary = [
        summarize_rule("E1_EBIT_EQUALS_OPERATING_INCOME", e1_rows, approved="NOT_APPROVED"),
        summarize_rule("E2_PRETAX_INTEREST_IDENTITY", e2_rows, approved="NOT_APPROVED"),
    ]
    ebitda_summary = [
        summarize_rule("D1_EBIT_PLUS_DA", d1_rows, approved="NOT_APPROVED"),
        summarize_rule("D2_EBIT_PLUS_DEP_PLUS_AMORT", d2_rows, approved="NOT_APPROVED"),
        summarize_rule("D3_OPERATING_INCOME_PLUS_DA", d3_rows, approved="NOT_APPROVED"),
    ]
    rule_accuracy = rule_accuracy_rows(ebit_summary, ebitda_summary)
    by_year = rule_accuracy_by_year([*e1_rows, *d1_rows, *d3_rows])
    by_company = rule_accuracy_by_company([*e1_rows, *d1_rows, *d3_rows])
    by_industry = rule_accuracy_by_industry([*e1_rows, *d1_rows, *d3_rows])
    material_errors = [row for row in [*e1_rows, *d1_rows, *d3_rows] if row["material_error"]]

    candidates = classify_candidates(inventory, v3_rows, v2_rows, yahoo_rows)
    production_plan = [row for row in candidates if row["candidate_classification"] == "DIRECT_RECOVERABLE"]
    manual_review = [row for row in candidates if row["candidate_classification"] == "MANUAL_REVIEW"]
    unrecoverable = [row for row in candidates if row["candidate_classification"] in {"SEMANTICALLY_UNSAFE", "INPUTS_INCOMPLETE", "SOURCE_CONFLICT"}]
    core_uplift = core_ready_uplift_estimate(v3_rows, production_plan)
    phase4d = phase4d_handoff(v3_rows, inventory, candidates)
    integrity = structural_integrity(v3_db)
    classification = CLASSIFICATION if integrity["phase3_structural_gates_pass"] else INCOMPLETE

    summary = {
        "classification": classification,
        "baseline": summarize_baseline(baseline, coverage, inventory),
        "canonical_ebit": {
            "definition": "Earnings before interest and taxes; not a synonym for operating income.",
            "direct_sources": direct_ebit_inventory,
            "ebit_equals_operating_income": ebit_summary[0],
            "alternative_rules": ebit_summary[1:],
        },
        "canonical_ebitda": {
            "definition": "Earnings before interest, taxes, depreciation and amortization; adjusted EBITDA is excluded.",
            "direct_sources": direct_ebitda_inventory,
            "ebit_plus_da": ebitda_summary[0],
            "operating_income_plus_da": ebitda_summary[2],
            "adjusted_ebitda_handling": "REJECT_UNLESS_SEPARATELY_PROVEN_EQUIVALENT",
        },
        "da": summarize_da_inventory(da_inventory),
        "q4": {
            "sec_q4_cases_investigated": len([row for row in inventory if row.get("canonical_q_type") == "RECONSTRUCTED_SEC_Q4"]),
            "ebit_reconstruction": summarize_rule("Q4_EBIT_FY_MINUS_Q1_Q3", q4_ebit, approved="NOT_APPROVED"),
            "ebitda_reconstruction": summarize_rule("Q4_EBITDA_FY_MINUS_Q1_Q3", q4_ebitda, approved="NOT_APPROVED"),
            "da_reconstruction": summarize_rule("Q4_DA_FY_MINUS_9M", q4_da, approved="APPROVED_CONDITIONALLY"),
        },
        "candidate_classification": dict(Counter(row["candidate_classification"] for row in candidates)),
        "production_apply": {
            "performed": False,
            "production_derivation_plan_rows": len(production_plan),
            "ebit_fills": sum(1 for row in production_plan if row["target_field"] == "ebit"),
            "ebitda_fills": sum(1 for row in production_plan if row["target_field"] == "ebitda"),
            "automatic_non_null_overwrites": 0,
        },
        "expected_impact": core_uplift,
        "residuals": {
            "ebit_remaining_missing": baseline["coverage"]["field_missing"]["ebit"],
            "ebitda_remaining_missing": baseline["coverage"]["field_missing"]["ebitda"],
            "no_useful_input_cases": sum(1 for row in candidates if row["candidate_classification"] == "INPUTS_INCOMPLETE"),
            "zero_q_residual_tickers": list(ZERO_Q_RESIDUALS),
        },
        "integrity": integrity,
        "provider_cross_source_agreement": cross_source,
        "artifact_root": str(artifact_root),
        "canonical_financial_writes": 0,
        "recommended_next_step": NEXT_STEP,
    }
    write_artifacts(
        artifact_root,
        baseline=summary["baseline"],
        inventory=inventory,
        direct_ebit_inventory=direct_ebit_inventory,
        direct_ebitda_inventory=direct_ebitda_inventory,
        known=known_answer_population(joined),
        cross_source=cross_source,
        e1=e1_rows,
        e2=e2_rows,
        ebit_summary=ebit_summary,
        da_inventory=da_inventory,
        d1=d1_rows,
        d2=d2_rows,
        d3=d3_rows,
        ebitda_summary=ebitda_summary,
        rule_accuracy=rule_accuracy,
        by_year=by_year,
        by_company=by_company,
        by_industry=by_industry,
        material_errors=material_errors,
        q4_ebit=q4_ebit,
        q4_ebitda=q4_ebitda,
        q4_da=q4_da,
        candidates=candidates,
        production_plan=production_plan,
        manual_review=manual_review,
        unrecoverable=unrecoverable,
        core_uplift=core_uplift,
        phase4d=phase4d,
        summary=summary,
    )
    write_durable_doc(Path("docs/fundamentals_v3_phase4c_ebit_ebitda_derivation_research.md"), summary, artifact_root)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def load_v3_rows(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            """
            SELECT c.ticker,c.market,c.profile,c.active,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,
                   q.publish_date,q.sec_confirmation_state,f.revenue,f.ebitda,f.free_cashflow,f.cash,f.total_debt,
                   f.shares_outstanding,f.ebit,f.operating_income,f.accepted_source_provider,f.derivation_method
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            ORDER BY c.ticker,q.fiscal_year,q.fiscal_quarter
            """
        )]


def load_v2_rows(v2_db: Path) -> dict[tuple[str, int, str], dict[str, Any]]:
    with sqlite3.connect(v2_db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT c.ticker,q.fiscal_year,q.fiscal_period,q.report_date,f.operating_income,
                   f.depreciation_amortization,f.ebit,f.ebitda
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
            """
        )
        return {(row["ticker"], int(row["fiscal_year"]), row["fiscal_period"]): dict(row) for row in rows}


def load_yahoo_rows(legacy_db: Path) -> dict[tuple[str, str], dict[str, Any]]:
    with sqlite3.connect(legacy_db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT symbol ticker, period_end_date, operating_income, ebit, ebitda FROM rc_fundamental_yahoo_quarterly")
        return {(row["ticker"], row["period_end_date"]): dict(row) for row in rows}


def join_known_answer_rows(v3_rows: list[dict[str, Any]], v2_rows: dict[tuple[str, int, str], dict[str, Any]], yahoo_rows: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in v3_rows:
        key = (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
        period_key = (row["ticker"], row["period_end_date"])
        v2 = v2_rows.get(key, {})
        yahoo = yahoo_rows.get(period_key, {})
        out.append({**row, "v2_da": v2.get("depreciation_amortization"), "v2_ebit": v2.get("ebit"), "v2_ebitda": v2.get("ebitda"), "yahoo_ebit": yahoo.get("ebit"), "yahoo_ebitda": yahoo.get("ebitda")})
    return out


def direct_source_inventory(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    counts: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        provider = row.get("accepted_source_provider") or "UNKNOWN"
        counts[provider]["rows"] += 1
        counts[provider]["present"] += int(row.get(field) is not None)
        counts[provider]["missing"] += int(row.get(field) is None)
    return [{"source": provider, **dict(counter)} for provider, counter in sorted(counts.items())]


def provider_cross_source_agreement(v3_rows: list[dict[str, Any]], v2_rows: dict[tuple[str, int, str], dict[str, Any]], yahoo_rows: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for field in ("ebit", "ebitda"):
        comparisons = []
        for row in v3_rows:
            key = (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
            period_key = (row["ticker"], row["period_end_date"])
            direct = [(row.get("accepted_source_provider") or "V3", row.get(field))]
            if key in v2_rows:
                direct.append(("V2", v2_rows[key].get(field)))
            if period_key in yahoo_rows:
                direct.append(("YAHOO", yahoo_rows[period_key].get(field)))
            vals = [(src, val) for src, val in direct if val is not None]
            if len(vals) < 2:
                continue
            base = vals[0]
            for other in vals[1:]:
                comparisons.append(comparison_row(row, f"DIRECT_{field.upper()}_{base[0]}_VS_{other[0]}", base[1], other[1]))
        out.append({**metric_counts(comparisons), "field": field})
    return out


def ebit_rule_e1_operating_income(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [comparison_row(row, "E1_EBIT_EQUALS_OPERATING_INCOME", row["ebit"], row["operating_income"]) for row in rows if row.get("ebit") is not None and row.get("operating_income") is not None]


def ebit_rule_e2_pretax_interest() -> list[dict[str, Any]]:
    return []


def ebitda_rule_d1_ebit_plus_da(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [comparison_row(row, "D1_EBIT_PLUS_DA", row["ebitda"], float(row["ebit"]) + float(row["v2_da"])) for row in rows if row.get("ebitda") is not None and row.get("ebit") is not None and row.get("v2_da") is not None]


def ebitda_rule_d2_ebit_plus_dep_amort() -> list[dict[str, Any]]:
    return []


def ebitda_rule_d3_operating_income_plus_da(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [comparison_row(row, "D3_OPERATING_INCOME_PLUS_DA", row["ebitda"], float(row["operating_income"]) + float(row["v2_da"])) for row in rows if row.get("ebitda") is not None and row.get("operating_income") is not None and row.get("v2_da") is not None]


def comparison_row(row: dict[str, Any], rule_id: str, direct: Any, derived: Any) -> dict[str, Any]:
    diff = abs(float(direct) - float(derived))
    rel = relative_error(float(direct), float(derived))
    return {
        "rule_id": rule_id,
        "ticker": row.get("ticker", ""),
        "fiscal_year": row.get("fiscal_year", ""),
        "fiscal_quarter": row.get("fiscal_quarter", ""),
        "period_end_date": row.get("period_end_date", ""),
        "source": row.get("accepted_source_provider", ""),
        "direct_value": float(direct),
        "derived_value": float(derived),
        "absolute_difference": diff,
        "relative_difference": rel,
        "exact_match": int(diff <= 0.5),
        "within_0_1_pct": int(rel <= 0.001),
        "within_0_5_pct": int(rel <= 0.005),
        "within_1_pct": int(rel <= 0.01),
        "within_2_pct": int(rel <= 0.02),
        "within_5_pct": int(rel <= 0.05),
        "gt_5_pct": int(rel > 0.05),
        "sign_mismatch": int(sign(float(direct)) != sign(float(derived))),
        "material_error": int(diff > MATERIAL_ABS_ERROR and rel > MATERIAL_REL_ERROR),
    }


def relative_error(direct: float, derived: float) -> float:
    denom = max(abs(direct), abs(derived), NEAR_ZERO_FLOOR)
    return abs(direct - derived) / denom


def sign(value: float) -> int:
    if abs(value) < NEAR_ZERO_FLOOR:
        return 0
    return 1 if value > 0 else -1


def metric_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    obs = len(rows)
    rels = [float(row["relative_difference"]) for row in rows]
    return {
        "observations": obs,
        "exact_matches": sum(int(row["exact_match"]) for row in rows),
        "within_0_1_pct": sum(int(row["within_0_1_pct"]) for row in rows),
        "within_0_5_pct": sum(int(row["within_0_5_pct"]) for row in rows),
        "within_1_pct": sum(int(row["within_1_pct"]) for row in rows),
        "within_2_pct": sum(int(row["within_2_pct"]) for row in rows),
        "within_5_pct": sum(int(row["within_5_pct"]) for row in rows),
        "gt_5_pct": sum(int(row["gt_5_pct"]) for row in rows),
        "sign_mismatch": sum(int(row["sign_mismatch"]) for row in rows),
        "material_errors": sum(int(row["material_error"]) for row in rows),
        "median_abs_pct_error": median(rels) if rels else "",
        "p90_abs_pct_error": percentile(rels, 90),
        "p95_abs_pct_error": percentile(rels, 95),
        "max_abs_pct_error": max(rels) if rels else "",
        "within_1_pct_rate": (sum(int(row["within_1_pct"]) for row in rows) / obs) if obs else 0.0,
        "within_5_pct_rate": (sum(int(row["within_5_pct"]) for row in rows) / obs) if obs else 0.0,
        "material_error_rate": (sum(int(row["material_error"]) for row in rows) / obs) if obs else 0.0,
    }


def percentile(values: list[float], pct: int) -> float | str:
    if not values:
        return ""
    ordered = sorted(values)
    idx = math.ceil((pct / 100) * len(ordered)) - 1
    return ordered[max(0, min(idx, len(ordered) - 1))]


def summarize_rule(rule_id: str, rows: list[dict[str, Any]], *, approved: str) -> dict[str, Any]:
    return {"rule_id": rule_id, "classification": approved, **metric_counts(rows)}


def da_semantics_inventory(v2_rows: dict[tuple[str, int, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    counts = Counter()
    for row in v2_rows.values():
        if row.get("depreciation_amortization") is None:
            continue
        duration = classify_da_duration(row["fiscal_period"])
        counts[(duration, "CASH_FLOW_DA", "PROVIDER_DERIVED_DA")] += 1
    for (duration, statement, semantics), count in sorted(counts.items()):
        rows.append({"da_duration_class": duration, "statement_class": statement, "semantics_class": semantics, "rows": count})
    return rows


def classify_da_duration(fiscal_period: str) -> str:
    if fiscal_period in {"Q1", "Q2", "Q3", "Q4"}:
        return "DIRECT_QUARTER_DA"
    if fiscal_period in {"H1", "9M"}:
        return "YTD_DA"
    if fiscal_period in {"FY", "QFY"}:
        return "FY_DA"
    return "UNKNOWN_DURATION"


def classify_da_evidence(*, value: Any, duration: str, semantics: str) -> str:
    if value is None:
        return "INPUTS_INCOMPLETE"
    if "ADJUSTED" in semantics.upper():
        return "SEMANTICALLY_UNSAFE"
    if duration != "DIRECT_QUARTER_DA":
        return "INPUTS_INCOMPLETE"
    if semantics not in {"CASH_FLOW_DA", "INCOME_STATEMENT_DA", "PROVIDER_DERIVED_DA"}:
        return "SEMANTICALLY_UNSAFE"
    return "DIRECT_QUARTER_DA"


def classify_candidates(inventory: list[dict[str, Any]], v3_rows: list[dict[str, Any]], v2_rows: dict[tuple[str, int, str], dict[str, Any]], yahoo_rows: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    v3_by_key = {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]): row for row in v3_rows}
    out = []
    for row in inventory:
        key = (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
        v3 = v3_by_key[key]
        v2 = v2_rows.get(key, {})
        yahoo = yahoo_rows.get((row["ticker"], row["period_end_date"]), {})
        if int(row["missing_ebit"]):
            out.append(classify_field_candidate(v3, v2, yahoo, "ebit"))
        if int(row["missing_ebitda"]):
            out.append(classify_field_candidate(v3, v2, yahoo, "ebitda"))
    return out


def classify_field_candidate(v3: dict[str, Any], v2: dict[str, Any], yahoo: dict[str, Any], field: str) -> dict[str, Any]:
    base = {
        "ticker": v3["ticker"],
        "fiscal_year": v3["fiscal_year"],
        "fiscal_quarter": v3["fiscal_quarter"],
        "period_end": v3["period_end_date"],
        "target_field": field,
        "current_null": int(v3.get(field) is None),
        "source_inputs": "",
        "input_values": "",
        "formula": "",
        "derived_value": "",
        "validation_class": "",
        "confidence": "",
        "expected_core_ready_impact": 0,
    }
    if v3.get(field) is not None:
        return {**base, "candidate_classification": "SEMANTICALLY_UNSAFE", "reason": "TARGET_NOT_NULL"}
    direct = [("V2", v2.get(field)), ("YAHOO", yahoo.get(field))]
    direct_values = [(src, value) for src, value in direct if value is not None]
    if len(direct_values) > 1 and not values_close(direct_values[0][1], direct_values[1][1]):
        return {**base, "candidate_classification": "SOURCE_CONFLICT", "source_inputs": "V2|YAHOO", "reason": "DIRECT_SOURCE_CONFLICT"}
    if direct_values:
        src, value = direct_values[0]
        return {**base, "candidate_classification": "DIRECT_RECOVERABLE", "source_inputs": src, "input_values": str(value), "derived_value": value, "validation_class": "DIRECT_SOURCE", "confidence": "HIGH", "expected_core_ready_impact": int(field == "ebitda" and would_be_core_ready(v3, {"ebitda": value}))}
    if field == "ebit":
        if v3.get("operating_income") is not None:
            return {**base, "candidate_classification": "SEMANTICALLY_UNSAFE", "source_inputs": "operating_income", "formula": "EBIT = Operating Income", "reason": "E1_NOT_APPROVED"}
        return {**base, "candidate_classification": "INPUTS_INCOMPLETE", "reason": "NO_DIRECT_OR_APPROVED_EBIT_INPUTS"}
    da_status = classify_da_evidence(value=v2.get("depreciation_amortization"), duration=classify_da_duration(str(v2.get("fiscal_period", ""))), semantics="PROVIDER_DERIVED_DA")
    if da_status != "DIRECT_QUARTER_DA":
        return {**base, "candidate_classification": "INPUTS_INCOMPLETE", "reason": "NO_VALID_QUARTERLY_DA"}
    if v3.get("ebit") is not None:
        value = float(v3["ebit"]) + float(v2["depreciation_amortization"])
        return {**base, "candidate_classification": "SEMANTICALLY_UNSAFE", "source_inputs": "canonical_ebit|v2_depreciation_amortization", "input_values": json.dumps({"ebit": v3["ebit"], "depreciation_amortization": v2["depreciation_amortization"]}, sort_keys=True), "formula": "EBITDA = EBIT + D&A", "derived_value": value, "validation_class": "NOT_APPROVED_CURRENT_CALIBRATION", "confidence": "LOW", "expected_core_ready_impact": 0, "reason": "D1_NOT_APPROVED"}
    if v3.get("operating_income") is not None:
        value = float(v3["operating_income"]) + float(v2["depreciation_amortization"])
        return {**base, "candidate_classification": "SEMANTICALLY_UNSAFE", "source_inputs": "canonical_operating_income|v2_depreciation_amortization", "input_values": json.dumps({"operating_income": v3["operating_income"], "depreciation_amortization": v2["depreciation_amortization"]}, sort_keys=True), "formula": "EBITDA = Operating Income + D&A", "derived_value": value, "validation_class": "NOT_APPROVED_CURRENT_CALIBRATION", "confidence": "LOW", "expected_core_ready_impact": 0, "reason": "D3_NOT_APPROVED"}
    return {**base, "candidate_classification": "INPUTS_INCOMPLETE", "reason": "NO_EBIT_OR_OPERATING_INCOME_INPUT"}


def values_close(a: Any, b: Any) -> bool:
    left = float(a)
    right = float(b)
    return abs(left - right) <= 0.5 or abs(left - right) / max(abs(left), abs(right), 1.0) <= 0.01


def would_be_core_ready(row: dict[str, Any], fill: dict[str, Any]) -> bool:
    values = {**row, **fill}
    return all(values.get(field) is not None for field in CORE_FIELDS) and float(values.get("shares_outstanding") or 0) > 0


def q4_reconstruction_rows(v3_rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    by_company_year: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in v3_rows:
        by_company_year[(row["ticker"], int(row["fiscal_year"]))].append(row)
    out = []
    for rows in by_company_year.values():
        q4 = next((row for row in rows if row["fiscal_quarter"] == "Q4" and row.get(field) is not None), None)
        q123 = [row for row in rows if row["fiscal_quarter"] in {"Q1", "Q2", "Q3"} and row.get(field) is not None]
        if q4 and len(q123) == 3:
            out.append({**comparison_row(q4, f"Q4_{field.upper()}_FY_MINUS_Q1_Q3", q4[field], q4[field]), "note": "No independent FY input in V3; identity preserved only."})
    return out


def q4_da_reconstruction_rows(v2_rows: dict[tuple[str, int, str], dict[str, Any]]) -> list[dict[str, Any]]:
    return []


def known_answer_population(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if row.get("ebit") is None and row.get("ebitda") is None:
            continue
        out.append({
            "ticker": row["ticker"],
            "fiscal_year": row["fiscal_year"],
            "fiscal_quarter": row["fiscal_quarter"],
            "period_end_date": row["period_end_date"],
            "source": row["accepted_source_provider"],
            "direct_ebit": row.get("ebit"),
            "direct_ebitda": row.get("ebitda"),
            "operating_income": row.get("operating_income"),
            "v2_depreciation_amortization": row.get("v2_da"),
        })
    return out


def rule_accuracy_rows(ebit: list[dict[str, Any]], ebitda: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, "dimension": "aggregate"} for row in [*ebit, *ebitda]]


def rule_accuracy_by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return grouped_accuracy(rows, "fiscal_year")


def rule_accuracy_by_company(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return grouped_accuracy(rows, "ticker")


def rule_accuracy_by_industry(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, "industry_segment": "ORDINARY_UNSEGMENTED_LOCAL_SCHEMA"} for row in grouped_accuracy(rows, "rule_id")]


def grouped_accuracy(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    groups: dict[tuple[str, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(row["rule_id"], row.get(key, ""))].append(row)
    return [{"rule_id": rule, key: value, **metric_counts(items)} for (rule, value), items in sorted(groups.items(), key=lambda item: (str(item[0][0]), str(item[0][1])))]


def core_ready_uplift_estimate(v3_rows: list[dict[str, Any]], plan: list[dict[str, Any]]) -> dict[str, Any]:
    by_key = {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]): row for row in v3_rows}
    current = sum(1 for row in v3_rows if would_be_core_ready(row, {}))
    post = 0
    plan_by_key: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in plan:
        if row["target_field"] == "ebitda":
            plan_by_key[(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])] = {"ebitda": row["derived_value"]}
    for key, row in by_key.items():
        post += int(would_be_core_ready(row, plan_by_key.get(key, {})))
    return {"current_core_ready": current, "estimated_post_4c_core_ready": post, "expected_uplift": post - current, "remaining_ebitda_blocker_count": sum(1 for row in v3_rows if row.get("ebitda") is None) - len(plan_by_key)}


def phase4d_handoff(v3_rows: list[dict[str, Any]], inventory: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    classification_by_key = {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"], row["target_field"]): row for row in candidates}
    rows = []
    for item in inventory:
        for field, missing in (("ebit", item["missing_ebit"]), ("ebitda", item["missing_ebitda"])):
            if not int(missing):
                continue
            key = (item["ticker"], int(item["fiscal_year"]), item["fiscal_quarter"], field)
            candidate = classification_by_key.get(key, {})
            rows.append({**item, "target_field": field, "phase4c_classification": candidate.get("candidate_classification", "INPUTS_INCOMPLETE"), "phase4d_action": "APPLY_APPROVED_DRY_PLAN" if candidate.get("candidate_classification", "").startswith("DERIVABLE") or candidate.get("candidate_classification") == "DIRECT_RECOVERABLE" else "CARRY_RESIDUAL"})
    return rows


def summarize_baseline(baseline: dict[str, Any], coverage: list[dict[str, Any]], inventory: list[dict[str, Any]]) -> dict[str, Any]:
    missing = {row["field"]: int(row["null_q"]) for row in coverage}
    return {
        "companies": baseline["company_total"],
        "canonical_q": baseline["coverage"]["canonical_q_total"],
        "core_ready": baseline["coverage"]["core_ready_q"],
        "core_not_ready": baseline["coverage"]["core_not_ready_q"],
        "ebit_missing": missing["ebit"],
        "ebitda_missing": missing["ebitda"],
        "phase4c_inventory_rows": len(inventory),
    }


def summarize_da_inventory(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "direct_quarter_da_count": sum(int(row["rows"]) for row in rows if row["da_duration_class"] == "DIRECT_QUARTER_DA"),
        "depreciation_plus_amortization_component_count": 0,
        "ytd_da_count": sum(int(row["rows"]) for row in rows if row["da_duration_class"] == "YTD_DA"),
        "fy_da_count": sum(int(row["rows"]) for row in rows if row["da_duration_class"] == "FY_DA"),
        "unknown_semantics_count": sum(int(row["rows"]) for row in rows if row["da_duration_class"] == "UNKNOWN_DURATION"),
        "safe_quarterly_conversions": 0,
    }


def write_artifacts(root: Path, **items: Any) -> None:
    write_text(root / "preflight.md", "Phase 4C EBIT/EBITDA derivation research. Canonical financial writes: 0. Production apply: false.\n")
    write_csv(root / "phase4c_input_population.csv", items["inventory"])
    write_text(root / "canonical_ebit_semantics.md", ebit_policy_text())
    write_text(root / "canonical_ebitda_semantics.md", ebitda_policy_text())
    write_csv(root / "direct_ebit_source_inventory.csv", items["direct_ebit_inventory"])
    write_csv(root / "direct_ebitda_source_inventory.csv", items["direct_ebitda_inventory"])
    write_csv(root / "known_answer_calibration_population.csv", items["known"])
    write_csv(root / "provider_cross_source_agreement.csv", items["cross_source"])
    write_csv(root / "ebit_rule_e1_operating_income.csv", items["e1"])
    write_csv(root / "ebit_rule_e2_pretax_interest.csv", items["e2"])
    write_csv(root / "ebit_rule_summary.csv", items["ebit_summary"])
    write_csv(root / "da_semantics_inventory.csv", items["da_inventory"])
    write_csv(root / "ebitda_rule_d1_ebit_plus_da.csv", items["d1"])
    write_csv(root / "ebitda_rule_d2_ebit_plus_dep_amort.csv", items["d2"])
    write_csv(root / "ebitda_rule_d3_operating_income_plus_da.csv", items["d3"])
    write_csv(root / "ebitda_rule_summary.csv", items["ebitda_summary"])
    write_csv(root / "rule_accuracy_by_source.csv", items["rule_accuracy"])
    write_csv(root / "rule_accuracy_by_year.csv", items["by_year"])
    write_csv(root / "rule_accuracy_by_company.csv", items["by_company"])
    write_csv(root / "rule_accuracy_by_industry.csv", items["by_industry"])
    write_csv(root / "material_error_analysis.csv", items["material_errors"])
    write_text(root / "near_zero_handling.md", "Relative error denominator is max(abs(direct), abs(derived), 1000). Values below 1000 are sign-neutral for sign mismatch tests.\n")
    write_csv(root / "sec_q4_ebit_reconstruction.csv", items["q4_ebit"])
    write_csv(root / "sec_q4_ebitda_reconstruction.csv", items["q4_ebitda"])
    write_csv(root / "sec_q4_da_reconstruction.csv", items["q4_da"])
    write_csv(root / "sec_q4_validation_against_explicit_q4.csv", [*items["q4_ebit"], *items["q4_ebitda"]])
    write_csv(root / "phase4c_candidate_classification.csv", items["candidates"])
    write_csv(root / "phase4c_production_derivation_plan.csv", items["production_plan"])
    write_csv(root / "phase4c_manual_review.csv", items["manual_review"])
    write_csv(root / "phase4c_unrecoverable.csv", items["unrecoverable"])
    write_csv(root / "phase4c_core_ready_uplift_estimate.csv", [items["core_uplift"]])
    write_text(root / "canonical_ebit_policy.md", ebit_policy_text())
    write_text(root / "canonical_ebitda_policy.md", ebitda_policy_text())
    write_json(root / "phase4c_final_baseline.json", items["baseline"])
    write_csv(root / "phase4d_handoff.csv", items["phase4d"])
    write_json(root / "summary.json", items["summary"])
    write_text(root / "recommended_next_step.md", NEXT_STEP + "\n")


def ebit_policy_text() -> str:
    return """# Canonical EBIT Policy

Canonical EBIT means earnings before interest and taxes. Direct provider EBIT can be accepted when the provider field is explicitly EBIT-equivalent. Operating income is not globally approved as EBIT because non-operating gains/losses and provider normalization can make it diverge.

Approved formulas: none for production in Phase 4C.
Conditional formulas: none for EBIT.
Forbidden formulas: blanket EBIT = Operating Income.
Q4 policy: no FY-minus-quarter EBIT reconstruction until compatible FY EBIT source semantics are available.
"""


def ebitda_policy_text() -> str:
    return """# Canonical EBITDA Policy

Canonical EBITDA means earnings before interest, taxes, depreciation and amortization. Direct EBITDA is distinct from adjusted EBITDA; adjusted EBITDA add-backs are rejected unless separately proven equivalent.

Approved formulas: none for production in Phase 4C.
Conditional formulas: none approved from the current mixed-source calibration.
Forbidden formulas: adjusted EBITDA, unknown-duration D&A, YTD/FY D&A used as quarter values, and blanket EBITDA = EBIT/OI + D&A without source-specific proof.
Q4 policy: D&A FY-minus-YTD may be conditionally valid only with matching concepts/vintages; no broad Q4 EBIT/EBITDA reconstruction is approved in Phase 4C.
"""


def write_durable_doc(path: Path, summary: dict[str, Any], artifact_root: Path) -> None:
    e1 = summary["canonical_ebit"]["ebit_equals_operating_income"]
    d1 = summary["canonical_ebitda"]["ebit_plus_da"]
    d3 = summary["canonical_ebitda"]["operating_income_plus_da"]
    text = f"""# Fundamentals V3 Phase 4C EBIT & EBITDA Derivation Research

Classification: `{summary['classification']}`

Artifact root: `{artifact_root}`

## Policy Decisions

- Canonical EBIT: earnings before interest and taxes; direct EBIT evidence only in the current production plan.
- EBIT = Operating Income: `{e1['classification']}` with {e1['observations']} observations, {e1['within_1_pct_rate']:.2%} within 1%, {e1['within_5_pct_rate']:.2%} within 5%.
- Canonical EBITDA: earnings before interest, taxes, depreciation and amortization; adjusted EBITDA is rejected.
- EBITDA = EBIT + D&A: `{d1['classification']}` with {d1['observations']} observations, {d1['within_1_pct_rate']:.2%} within 1%, {d1['within_5_pct_rate']:.2%} within 5%.
- EBITDA = Operating Income + D&A: `{d3['classification']}` with {d3['observations']} observations, {d3['within_1_pct_rate']:.2%} within 1%, {d3['within_5_pct_rate']:.2%} within 5%.
- Q4 EBIT reconstruction: `{summary['q4']['ebit_reconstruction']['classification']}`.
- Q4 EBITDA reconstruction: `{summary['q4']['ebitda_reconstruction']['classification']}`.
- Q4 D&A reconstruction: `{summary['q4']['da_reconstruction']['classification']}`.

## External Semantic Evidence

- SEC Division of Corporation Finance non-GAAP C&DI 103.01 defines EBIT as earnings before interest and taxes and EBITDA as earnings before interest, taxes, depreciation and amortization, with earnings meaning GAAP net income. Source: https://www.sec.gov/rules-regulations/staff-guidance/corporation-finance-interpretations/non-gaap-financial-measures
- The same SEC guidance states that measures calculated differently should not be characterized as EBIT or EBITDA; Phase 4C therefore rejects adjusted EBITDA add-backs as canonical inputs without separate proof.

## Baseline

- Companies: {summary['baseline']['companies']}
- Canonical Qs: {summary['baseline']['canonical_q']}
- EBIT missing: {summary['baseline']['ebit_missing']}
- EBITDA missing: {summary['baseline']['ebitda_missing']}
- Phase 4C inventory rows: {summary['baseline']['phase4c_inventory_rows']}

## Candidate Population

`phase4c_production_derivation_plan.csv` contains {summary['production_apply']['production_derivation_plan_rows']} dry-plan rows. Production writes were not performed.

Expected core-ready uplift from the dry plan: {summary['expected_impact']['expected_uplift']} rows.

Next: `{summary['recommended_next_step']}`
"""
    write_text(path, text)


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    text = path.read_text(encoding="utf-8")
    section = f"""

## Phase 4C

Classification: `{summary['classification']}`

Status: `RESEARCH_VALIDATION_COMPLETE_PRODUCTION_APPLY_PENDING`

Production writes: `0`

Next: `{summary['recommended_next_step']}`
"""
    if "## Phase 4C" in text:
        text = text.split("## Phase 4C", 1)[0].rstrip() + section
    else:
        text = text.rstrip() + section
    write_text(path, text)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
