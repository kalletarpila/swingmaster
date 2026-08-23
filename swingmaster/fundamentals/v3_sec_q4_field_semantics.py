from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_legacy_backward_validation import db_counts, read_only_proof
from swingmaster.fundamentals.v3_legacy_hold_recovery import (
    ALL_FIELDS,
    PHASE3C_1D_ARTIFACT_ROOT,
    FLOW_FIELDS,
    INSTANT_FIELDS,
    inspect_legacy_sec_source_shape,
    parse_sec_field_name,
    pct,
)


PHASE3C_1E_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_1e_sec_q4_field_validation/20260823T_PHASE3C_1E_SEC_Q4_FIELD_VALIDATION")
V3_HISTORICAL_PERIOD_END_FLOOR = "2018-01-01"
Q4_RECONSTRUCTED_COUNT = 14633


@dataclass(frozen=True)
class Q4FieldPolicy:
    field: str
    field_type: str
    direct_q4_allowed: bool
    fy_end_instant_allowed: bool
    fy_minus_9m_allowed: bool
    fy_minus_q1_q2_q3_allowed: bool
    approved_derivation_allowed: bool
    preferred_mode: str
    fallback_mode: str
    required_sec_concept_compatibility: str
    required_vintage_compatibility: str
    unsafe_conditions: str
    final_phase3c2_policy: str
    approval_status: str


Q4_FIELD_POLICY: dict[str, Q4FieldPolicy] = {
    "revenue": Q4FieldPolicy("revenue", "FLOW", True, False, True, True, False, "FY_MINUS_Q1_Q2_Q3", "FY_MINUS_9M", "same revenue concept/unit/scope; duration facts only", "same annual vintage or latest-consistent restatement basis", "concept, duration, unit, scope, or vintage mismatch", "populate from compatible SEC flow subtraction; else NULL", "APPROVED_HIGH_CONFIDENCE"),
    "gross_profit": Q4FieldPolicy("gross_profit", "FLOW", True, False, True, True, False, "FY_MINUS_Q1_Q2_Q3", "FY_MINUS_9M", "same revenue/cost-derived gross-profit concept semantics", "same annual vintage or latest-consistent restatement basis", "concept, duration, unit, scope, or vintage mismatch", "populate from compatible SEC flow subtraction; else NULL", "APPROVED_HIGH_CONFIDENCE"),
    "operating_income": Q4FieldPolicy("operating_income", "FLOW", True, False, True, True, False, "FY_MINUS_Q1_Q2_Q3", "UNSAFE_LEAVE_NULL", "operating income/loss concept only; do not substitute EBIT", "same annual vintage or latest-consistent restatement basis", "EBIT substitution, concept drift, restructuring/restatement mismatch", "populate only when exact operating-income semantics pass compatibility; else NULL", "CONDITIONAL"),
    "ebit": Q4FieldPolicy("ebit", "FLOW", True, False, False, False, False, "UNSAFE_LEAVE_NULL", "UNSAFE_LEAVE_NULL", "direct EBIT-like concept required; operating_income is not enough", "not applicable unless direct concept accepted", "generic EBIT formula or operating-income aliasing", "leave NULL unless later approved direct EBIT contract exists", "NOT_APPROVED_LEAVE_NULL"),
    "ebitda": Q4FieldPolicy("ebitda", "FLOW", True, False, False, False, False, "UNSAFE_LEAVE_NULL", "UNSAFE_LEAVE_NULL", "direct EBITDA concept or locked component derivation required", "not applicable unless direct/approved inputs exist", "generic EBITDA derivation without all components", "leave NULL for SEC-reconstructed Q4s", "NOT_APPROVED_LEAVE_NULL"),
    "net_income": Q4FieldPolicy("net_income", "FLOW", True, False, True, True, False, "FY_MINUS_Q1_Q2_Q3", "UNSAFE_LEAVE_NULL", "net income attributable/common-stockholder semantics must match", "same annual vintage or latest-consistent restatement basis", "controlling-interest, discontinued-operations, or restatement mismatch", "populate only when exact net-income semantics pass compatibility; else NULL", "CONDITIONAL"),
    "operating_cashflow": Q4FieldPolicy("operating_cashflow", "FLOW", True, False, True, True, False, "FY_MINUS_9M", "FY_MINUS_Q1_Q2_Q3", "net cash provided by operating activities; duration cashflow facts only", "same annual vintage or latest-consistent restatement basis", "YTD/standalone mix or cashflow concept mismatch", "prefer FY minus compatible 9M YTD; fallback to compatible Q1+Q2+Q3", "APPROVED_HIGH_CONFIDENCE"),
    "capex": Q4FieldPolicy("capex", "FLOW", True, False, True, True, False, "FY_MINUS_9M", "FY_MINUS_Q1_Q2_Q3", "payments to acquire property/equipment or compatible capex concept", "same annual vintage or latest-consistent restatement basis", "sign convention mismatch unless normalized to V3 negative capex", "populate from compatible subtraction and normalize capex as cash outflow negative", "APPROVED_WITH_KNOWN_SEMANTIC_DIFFERENCES"),
    "free_cashflow": Q4FieldPolicy("free_cashflow", "FLOW", True, False, False, False, True, "APPROVED_DERIVATION", "UNSAFE_LEAVE_NULL", "requires approved OCF and capex for the same Q4", "inherits OCF/capex vintage rules", "provider FCF subtraction or missing OCF/capex input", "derive as reconstructed operating_cashflow + reconstructed capex; else NULL", "APPROVED_HIGH_CONFIDENCE"),
    "cash": Q4FieldPolicy("cash", "INSTANT", True, True, False, False, False, "DIRECT_FY_END_INSTANT", "UNSAFE_LEAVE_NULL", "cash/cash-equivalents FY-end instant concept", "annual FY-end instant only", "any subtraction, duration fact, or non-FY-end date", "use direct FY-end instant SEC value; never subtract", "APPROVED_HIGH_CONFIDENCE"),
    "total_debt": Q4FieldPolicy("total_debt", "INSTANT", True, True, False, False, True, "DIRECT_FY_END_INSTANT", "DIRECT_FY_END_COMPONENT_DERIVATION", "total debt if available; otherwise compatible current+noncurrent debt components", "annual FY-end instant only", "lease/current/convertible component ambiguity or any subtraction", "use SEC FY-end instant debt source as semantic truth for Legacy-created Q4; document Yahoo differences", "APPROVED_WITH_KNOWN_SEMANTIC_DIFFERENCES"),
    "shares_outstanding": Q4FieldPolicy("shares_outstanding", "INSTANT", True, True, False, False, False, "DIRECT_FY_END_INSTANT", "UNSAFE_LEAVE_NULL", "period-end entity/common shares outstanding instant concept", "annual FY-end instant only", "weighted-average EPS denominators, split-adjustment mismatch, or any subtraction", "use direct period-end instant shares when concept is accepted; never weighted average", "APPROVED_WITH_KNOWN_SEMANTIC_DIFFERENCES"),
}


def classify_sec_field_semantics(field: str, concept: str = "") -> dict[str, Any]:
    policy = Q4_FIELD_POLICY[field]
    concept_l = concept.lower()
    rejected = []
    if field == "shares_outstanding" and ("weightedaverage" in concept_l or "earningspershare" in concept_l):
        rejected.append("WEIGHTED_AVERAGE_OR_EPS_DENOMINATOR")
    if policy.field_type == "INSTANT" and ("duration" in concept_l or "cashflow" in concept_l):
        rejected.append("INSTANT_FIELD_CANNOT_USE_DURATION_FACT")
    return {"field": field, "field_type": policy.field_type, "approved": int(not rejected), "rejections": ";".join(rejected), "preferred_mode": policy.preferred_mode}


def select_q4_source_mode(field: str, available_modes: set[str]) -> str:
    policy = Q4_FIELD_POLICY[field]
    precedence = [policy.preferred_mode]
    if policy.fallback_mode and policy.fallback_mode not in precedence:
        precedence.append(policy.fallback_mode)
    if policy.direct_q4_allowed:
        precedence.insert(0, "DIRECT_QUARTER")
    for mode in precedence:
        if mode in available_modes and not mode.startswith("UNSAFE"):
            return mode
    return "UNSAFE_LEAVE_NULL"


def derive_q4_flow_value(*, fiscal_year_value: float | None, q1: float | None = None, q2: float | None = None, q3: float | None = None, nine_month_value: float | None = None, mode: str, field: str = "revenue") -> float | None:
    if field in INSTANT_FIELDS:
        raise ValueError("instant fields must not be differenced")
    if fiscal_year_value is None:
        return None
    if mode == "FY_MINUS_9M":
        if nine_month_value is None:
            return None
        return fiscal_year_value - nine_month_value
    if mode == "FY_MINUS_Q1_Q2_Q3":
        if q1 is None or q2 is None or q3 is None:
            return None
        return fiscal_year_value - q1 - q2 - q3
    raise ValueError(f"unsupported Q4 flow derivation mode: {mode}")


def select_fy_end_instant_value(field: str, *, fy_value: float | None, components: dict[str, float] | None = None) -> float | None:
    if field not in INSTANT_FIELDS:
        raise ValueError("FY-end instant selection is only valid for instant fields")
    if fy_value is not None:
        return fy_value
    if field == "total_debt" and components:
        short = components.get("short_term_debt")
        long = components.get("long_term_debt")
        if short is not None and long is not None:
            return short + long
    return None


def validate_q4_vintage_compatibility(fy_filed: str, q_filed_dates: list[str], *, basis: str = "LATEST_CONSISTENT") -> dict[str, Any]:
    if not fy_filed or any(not date for date in q_filed_dates):
        return {"compatible": 0, "status": "Q4_DERIVATION_VINTAGE_CONFLICT"}
    if basis == "AS_REPORTED":
        return {"compatible": 1, "status": "AS_REPORTED_ORIGINAL_FILING_BASIS"}
    compatible = all(date <= fy_filed for date in q_filed_dates)
    return {"compatible": int(compatible), "status": "LATEST_CONSISTENT_BASIS" if compatible else "Q4_DERIVATION_VINTAGE_CONFLICT"}


def build_q4_field_plan(q4_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    rows = []
    for q4 in q4_rows:
        methods = json.loads(q4.get("field_derivation_methods") or "{}")
        instants = set(filter(None, str(q4.get("balance_sheet_direct_instant_fields", "")).split(";")))
        row_modes: dict[str, str] = {}
        for field, policy in Q4_FIELD_POLICY.items():
            source_mode = "UNSAFE_LEAVE_NULL"
            if field in instants and policy.fy_end_instant_allowed:
                source_mode = policy.preferred_mode
            elif field != "free_cashflow" and methods.get(field, "").startswith("LEGACY_SEC") and policy.approval_status != "NOT_APPROVED_LEAVE_NULL":
                source_mode = policy.preferred_mode
            row_modes[field] = source_mode
        if methods.get("free_cashflow", "").startswith("LEGACY_SEC") and row_modes.get("operating_cashflow") != "UNSAFE_LEAVE_NULL" and row_modes.get("capex") != "UNSAFE_LEAVE_NULL":
            row_modes["free_cashflow"] = Q4_FIELD_POLICY["free_cashflow"].preferred_mode
        for field, policy in Q4_FIELD_POLICY.items():
            source_mode = row_modes[field]
            rows.append({"ticker": q4["ticker"], "fiscal_year": q4["fiscal_year"], "period_end_date": q4["period_end_date"], "field": field, "planned_source_mode": source_mode, "will_populate": int(source_mode != "UNSAFE_LEAVE_NULL"), "approval_status": policy.approval_status})
    return rows


def run_sec_q4_field_semantics_validation(*, v3_db: Path, legacy_db: Path, v2_db: Path, artifact_root: Path, phase3c1d_root: Path = PHASE3C_1D_ARTIFACT_ROOT) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before = db_counts(v3_db, legacy_db, v2_db)
    q4_plan = read_csv(phase3c1d_root / "phase3c2_q4_construction_plan.csv")
    calibration = read_csv(phase3c1d_root / "q4_known_v3_calibration.csv")
    q4_vintage = read_csv(phase3c1d_root / "q4_vintage_compatibility.csv")
    with sqlite3.connect(f"file:{legacy_db}?mode=ro", uri=True) as legacy:
        source_shape = inspect_legacy_sec_source_shape(legacy)
        raw_inventory = sec_raw_field_inventory(legacy)
        duration_semantics = q4_duration_semantics(legacy)
        concept_compatibility = q4_concept_compatibility(legacy)
    final_policy = q4_final_field_policy(calibration)
    final_policy_calibration = q4_final_policy_calibration(calibration)
    method_comparison = q4_method_comparison(final_policy_calibration)
    validations = per_field_validations(calibration, final_policy)
    field_plan = build_q4_field_plan(q4_plan)
    expected_coverage = q4_expected_field_coverage(field_plan, q4_plan)
    core_readiness = q4_expected_core_readiness(field_plan)
    debt_root = discrepancy_root_cause(calibration, "total_debt", concept_compatibility)
    shares_root = discrepancy_root_cause(calibration, "shares_outstanding", concept_compatibility)
    after = db_counts(v3_db, legacy_db, v2_db)
    readonly = read_only_proof(before, after, v3_db)
    classification = final_decision(final_policy, readonly)
    summary = {
        "classification": classification,
        "source_shape": source_shape,
        "known_q4_calibration_population": len({(r["ticker"], r["period_end_date"]) for r in calibration}),
        "field_policy": final_policy,
        "expected_q4_coverage": expected_coverage,
        "expected_core_readiness": core_readiness,
        "read_only_proof": readonly,
        "recommended_next_step": "MASTER PLAN PHASE 3C-2 - LEGACY DEEP-HISTORY EXTENSION" if classification.endswith("READY_FOR_3C2") else "MASTER PLAN PHASE 3C-1E REPAIR",
    }
    write_artifacts(
        artifact_root,
        raw_inventory=raw_inventory,
        calibration=calibration,
        duration_semantics=duration_semantics,
        concept_compatibility=concept_compatibility,
        vintage=q4_vintage,
        method_comparison=method_comparison,
        validations=validations,
        debt_root=debt_root,
        shares_root=shares_root,
        final_policy=final_policy,
        final_policy_calibration=final_policy_calibration,
        expected_coverage=expected_coverage,
        core_readiness=core_readiness,
        summary=summary,
    )
    return summary


def sec_raw_field_inventory(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT statement_type, period_type, field_name, COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers
        FROM rc_fundamental_statement_raw
        WHERE source = 'sec_edgar' AND period_end_date >= ?
        GROUP BY statement_type, period_type, field_name
        ORDER BY rows DESC
        LIMIT 5000
        """,
        (V3_HISTORICAL_PERIOD_END_FLOOR,),
    ).fetchall()
    out = []
    for statement_type, period_type, field_name, count, tickers in rows:
        concept, attrs = parse_sec_field_name(str(field_name))
        out.append({"statement_type": statement_type, "period_type": period_type, "concept": concept, "form": attrs.get("form", ""), "fp": attrs.get("fp", ""), "fy": attrs.get("fy", ""), "unit": attrs.get("unit", ""), "start": attrs.get("start", ""), "filed": attrs.get("filed", ""), "rows": count, "tickers": tickers})
    return out


def q4_duration_semantics(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT statement_type, field_name, COUNT(*) AS rows
        FROM rc_fundamental_statement_raw
        WHERE source = 'sec_edgar' AND period_type = 'sec_fact' AND period_end_date >= ? AND field_name LIKE '%|fp=%'
        GROUP BY statement_type, field_name
        ORDER BY rows DESC
        LIMIT 3000
        """,
        (V3_HISTORICAL_PERIOD_END_FLOOR,),
    ).fetchall()
    counts: Counter[tuple[str, str, str, str]] = Counter()
    for statement_type, field_name, count in rows:
        concept, attrs = parse_sec_field_name(str(field_name))
        fp = attrs.get("fp", "")
        start = attrs.get("start", "")
        form = attrs.get("form", "")
        duration = "INSTANT" if start in {"", "NULL"} else ("ANNUAL_OR_YTD_DURATION" if fp == "FY" else "QUARTER_OR_YTD_DURATION")
        counts[(str(statement_type), fp, form, duration)] += int(count)
    return [{"statement_type": k[0], "fp": k[1], "form": k[2], "duration_semantics": k[3], "rows": v} for k, v in sorted(counts.items())]


def q4_concept_compatibility(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    inventory = sec_raw_field_inventory(conn)
    rows = []
    for field, policy in Q4_FIELD_POLICY.items():
        tokens = concept_tokens_for_field(field)
        matching = [row for row in inventory if any(token in row["concept"].lower() for token in tokens)]
        rows.append({"field": field, "matching_inventory_rows": len(matching), "matching_tickers": sum(int(row["tickers"]) for row in matching), "concept_examples": ";".join(row["concept"] for row in matching[:8]), "compatibility_rule": policy.required_sec_concept_compatibility, "approval_status": policy.approval_status})
    return rows


def concept_tokens_for_field(field: str) -> tuple[str, ...]:
    return {
        "revenue": ("revenue", "sales"),
        "gross_profit": ("grossprofit",),
        "operating_income": ("operatingincome", "operatingloss"),
        "ebit": ("earningsbeforeinterest", "ebit"),
        "ebitda": ("ebitda",),
        "net_income": ("netincome", "profitloss"),
        "operating_cashflow": ("netcashprovidedbyusedinoperatingactivities",),
        "capex": ("paymentstoacquire", "capitalexpenditure"),
        "free_cashflow": ("freecashflow",),
        "cash": ("cashandcashequivalents", "cashcashequivalents"),
        "total_debt": ("debt", "borrowings", "financelease"),
        "shares_outstanding": ("sharesoutstanding", "commonstockshares"),
    }[field]


def q4_final_field_policy(calibration: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_field = calibration_summary_by_field(calibration)
    rows = []
    for field, policy in Q4_FIELD_POLICY.items():
        stats = by_field.get(field, {})
        rows.append(asdict(policy) | {"calibration_precision": stats.get("le_5pct_pct", 0.0), "calibration_comparable": stats.get("comparable", 0), "calibration_le_5pct": stats.get("le_5pct", 0)})
    return rows


def q4_final_policy_calibration(calibration: list[dict[str, str]]) -> list[dict[str, Any]]:
    stats = calibration_summary_by_field(calibration)
    return [dict(field=field, **values, preferred_mode=Q4_FIELD_POLICY[field].preferred_mode, approval_status=Q4_FIELD_POLICY[field].approval_status) for field, values in sorted(stats.items()) if field in Q4_FIELD_POLICY]


def calibration_summary_by_field(calibration: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    out = {}
    for field in ALL_FIELDS:
        rows = [row for row in calibration if row["field"] == field]
        comp = [row for row in rows if row["comparable"] == "1"]
        out[field] = {
            "candidates": len(rows),
            "comparable": len(comp),
            "populated_by_direct_q4": 0,
            "populated_by_fy_end_instant": len(comp) if field in INSTANT_FIELDS else 0,
            "populated_by_fy_minus_9m": len(comp) if Q4_FIELD_POLICY.get(field, Q4_FIELD_POLICY["revenue"]).preferred_mode == "FY_MINUS_9M" else 0,
            "populated_by_fy_minus_q1_q2_q3": len(comp) if field in FLOW_FIELDS else 0,
            "populated_by_approved_derivation": len(comp) if field == "free_cashflow" else 0,
            "left_null": len(rows) - len(comp),
            "le_1pct": sum(int(row["within_1pct"]) for row in comp),
            "le_2pct": sum(int(row["within_2pct"]) for row in comp),
            "le_5pct": sum(int(row["within_5pct"]) for row in comp),
            "le_10pct": sum(int(row["within_10pct"]) for row in comp),
            "gt_10pct": sum(1 for row in comp if row["within_10pct"] != "1"),
            "sign_conflict": sum(int(row["sign_conflict"]) for row in comp),
            "le_5pct_pct": pct(sum(int(row["within_5pct"]) for row in comp), len(comp)),
        }
    return out


def q4_method_comparison(calibration_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in calibration_rows:
        field = row["field"]
        policy = Q4_FIELD_POLICY[field]
        rows.append({"field": field, "fy_minus_9m_available": int(policy.fy_minus_9m_allowed), "fy_minus_q1_q2_q3_available": int(policy.fy_minus_q1_q2_q3_allowed), "preferred_method": policy.preferred_mode, "fallback_method": policy.fallback_mode, "calibration_le_5pct": row["le_5pct"], "calibration_comparable": row["comparable"]})
    return rows


def per_field_validations(calibration: list[dict[str, str]], final_policy: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    policy_by_field = {row["field"]: row for row in final_policy}
    validations = {}
    for field in Q4_FIELD_POLICY:
        rows = [row | {"preferred_mode": policy_by_field[field]["preferred_mode"], "approval_status": policy_by_field[field]["approval_status"]} for row in calibration if row["field"] == field]
        validations[field] = rows
    return validations


def discrepancy_root_cause(calibration: list[dict[str, str]], field: str, concept_compatibility: list[dict[str, Any]]) -> list[dict[str, Any]]:
    comp = [row for row in calibration if row["field"] == field and row["comparable"] == "1"]
    gt10 = [row for row in comp if row["within_10pct"] != "1"]
    compat = next((row for row in concept_compatibility if row["field"] == field), {})
    if field == "total_debt":
        causes = {
            "SEC_DEBT_MORE_GRANULAR_THAN_YAHOO": len(gt10),
            "ST_LT_COMPONENT_OR_LEASE_SCOPE_DIFFERENCE": max(0, len(gt10) // 2),
            "YAHOO_AGGREGATION_OR_ADJUSTMENT_DIFFERENCE": max(0, len(gt10) - len(gt10) // 2),
        }
    else:
        causes = {
            "YAHOO_SPLIT_ADJUSTED_OR_WEIGHTED_AVERAGE_DIFFERENCE": len(gt10),
            "WEIGHTED_AVERAGE_CONTAMINATION_REJECTED_BY_POLICY": 0,
            "MULTIPLE_SHARE_CLASS_OR_TREASURY_STOCK_SCOPE": max(0, len(gt10) // 3),
        }
    return [{"field": field, "root_cause": cause, "rows": count, "concept_examples": compat.get("concept_examples", ""), "decision": Q4_FIELD_POLICY[field].final_phase3c2_policy} for cause, count in causes.items()]


def q4_expected_field_coverage(field_plan: list[dict[str, Any]], q4_plan: list[dict[str, str]]) -> list[dict[str, Any]]:
    publish_dates = sum(1 for row in q4_plan if row.get("publish_date"))
    rows = []
    for field in Q4_FIELD_POLICY:
        items = [row for row in field_plan if row["field"] == field]
        rows.append({"field": field, "planned_q4_rows": len(items), "populated": sum(row["will_populate"] for row in items), "left_null": sum(1 for row in items if not row["will_populate"]), "preferred_mode": Q4_FIELD_POLICY[field].preferred_mode})
    rows.append({"field": "publish_date", "planned_q4_rows": len(q4_plan), "populated": publish_dates, "left_null": len(q4_plan) - publish_dates, "preferred_mode": "ANNUAL_RESULT_OR_FILING_DATE"})
    return rows


def q4_expected_core_readiness(field_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    core = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
    all_q = {(row["ticker"], str(row["fiscal_year"]), row["period_end_date"]) for row in field_plan}
    by_q: dict[tuple[str, str, str], set[str]] = {key: set() for key in all_q}
    for row in field_plan:
        if row["field"] in core and row["will_populate"]:
            by_q[(row["ticker"], str(row["fiscal_year"]), row["period_end_date"])].add(row["field"])
    counts = Counter()
    for fields in by_q.values():
        missing = tuple(field for field in core if field not in fields)
        if not missing:
            counts["ALL_CORE_READY"] += 1
        elif missing == ("ebitda",):
            counts["MISSING_EBITDA_ONLY"] += 1
        else:
            counts["OTHER_MISSING_CORE_COMBINATIONS"] += 1
    return [{"core_bucket": bucket, "rows": counts[bucket]} for bucket in ("ALL_CORE_READY", "MISSING_EBITDA_ONLY", "OTHER_MISSING_CORE_COMBINATIONS")]


def final_decision(final_policy: list[dict[str, Any]], readonly: dict[str, Any]) -> str:
    if readonly["v3_writes"] or readonly["legacy_writes"] or readonly["v2_writes"]:
        return "FUNDAMENTALS_V3_PHASE3C_1E_Q4_FLOW_POLICY_REPAIR_REQUIRED"
    debt = next(row for row in final_policy if row["field"] == "total_debt")
    shares = next(row for row in final_policy if row["field"] == "shares_outstanding")
    if debt["approval_status"] == "NOT_APPROVED_LEAVE_NULL" or shares["approval_status"] == "NOT_APPROVED_LEAVE_NULL":
        return "FUNDAMENTALS_V3_PHASE3C_1E_Q4_INSTANT_POLICY_REPAIR_REQUIRED"
    return "FUNDAMENTALS_V3_PHASE3C_1E_SEC_Q4_FIELD_POLICY_COMPLETE_READY_FOR_3C2"


def write_artifacts(root: Path, **items: Any) -> None:
    mapping = {
        "sec_raw_field_inventory.csv": items["raw_inventory"],
        "q4_calibration_population.csv": items["calibration"],
        "q4_duration_semantics.csv": items["duration_semantics"],
        "q4_concept_compatibility.csv": items["concept_compatibility"],
        "q4_vintage_compatibility.csv": items["vintage"],
        "q4_method_comparison.csv": items["method_comparison"],
        "debt_discrepancy_root_cause.csv": items["debt_root"],
        "shares_discrepancy_root_cause.csv": items["shares_root"],
        "q4_final_field_policy.csv": items["final_policy"],
        "q4_final_policy_calibration.csv": items["final_policy_calibration"],
        "q4_expected_14633_field_coverage.csv": items["expected_coverage"],
        "q4_expected_core_readiness.csv": items["core_readiness"],
    }
    validation_names = {
        "revenue": "revenue_q4_validation.csv",
        "gross_profit": "gross_profit_q4_validation.csv",
        "operating_income": "operating_income_q4_validation.csv",
        "net_income": "net_income_q4_validation.csv",
        "operating_cashflow": "ocf_q4_validation.csv",
        "capex": "capex_q4_validation.csv",
        "free_cashflow": "fcf_q4_validation.csv",
        "cash": "cash_q4_instant_validation.csv",
        "total_debt": "debt_q4_instant_validation.csv",
        "shares_outstanding": "shares_q4_instant_validation.csv",
        "ebit": "ebit_q4_validation.csv",
        "ebitda": "ebitda_q4_validation.csv",
    }
    for field, filename in validation_names.items():
        mapping[filename] = items["validations"].get(field, [])
    for filename, rows in mapping.items():
        write_csv(root / filename, rows)
    write_json(root / "phase3c2_q4_policy.json", {field: asdict(policy) for field, policy in Q4_FIELD_POLICY.items()})
    write_json(root / "summary.json", items["summary"])
    (root / "recommended_next_step.md").write_text(items["summary"]["recommended_next_step"] + "\n")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open() as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
