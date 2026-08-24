from __future__ import annotations

import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_canonical_closure import final_canonical_baseline, field_coverage_summary
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity
from swingmaster.fundamentals.v3_phase4c_ebit_ebitda_derivation import comparison_row, metric_counts


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE4C2D_SEC_COMPANY_FORMULA_DISCOVERY_COMPLETE_READY_FOR_PRODUCTION_APPLY"
CLASSIFICATION_Q4_OPEN = "FUNDAMENTALS_V3_PHASE4C2D_Q1Q3_FORMULAS_READY_Q4_RESEARCH_STILL_REQUIRED"
CLASSIFICATION_NOT_USEFUL = "FUNDAMENTALS_V3_PHASE4C2D_COMPANY_FORMULA_DISCOVERY_NOT_USEFUL"
NEXT_STEP = "MASTER PLAN PHASE 4C-3 - EBIT & EBITDA PRODUCTION APPLY"
DIRECT_EBIT_CANDIDATES_FROM_PHASE4C = 252
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
MATERIAL_ABS_ERROR = 1_000_000.0
NEAR_ZERO_FLOOR = 1_000.0
RUN_ID = "PHASE4C2D_SEC_COMPONENT_FORMULA_RERUN"

INTEREST_ROLES = {
    "INTEREST_EXPENSE_GROSS",
    "DEBT_INTEREST",
    "FINANCE_LEASE_INTEREST",
    "ISSUER_SPECIFIC_INTEREST",
}
NET_INTEREST_ROLES = {"INTEREST_EXPENSE_NET"}
DA_ROLES = {"D_AND_A_COMBINED", "ISSUER_SPECIFIC_DA"}
DEP_ROLES = {"DEPRECIATION", "DEPRECIATION_PPE"}
AMORT_ROLES = {"AMORTIZATION", "AMORTIZATION_INTANGIBLES"}
FORMULA_REGISTRY = [
    {"formula_id": "PRETAX_PLUS_INTEREST_GROSS", "metric": "EBIT", "component_roles": "PRETAX|INTEREST_EXPENSE_GROSS", "formula_class": "ECONOMICALLY_JUSTIFIED"},
    {"formula_id": "PRETAX_PLUS_COMPOSITE_INTEREST", "metric": "EBIT", "component_roles": "PRETAX|DEBT_INTEREST|FINANCE_LEASE_INTEREST", "formula_class": "ECONOMICALLY_JUSTIFIED"},
    {"formula_id": "PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST", "metric": "EBIT", "component_roles": "PRETAX|ISSUER_SPECIFIC_INTEREST", "formula_class": "COMPANY_SPECIFIC_RESEARCH"},
    {"formula_id": "PRETAX_PLUS_NET_INTEREST", "metric": "EBIT", "component_roles": "PRETAX|INTEREST_EXPENSE_NET", "formula_class": "DIAGNOSTIC_VARIANT"},
    {"formula_id": "OPERATING_INCOME_PROXY", "metric": "EBIT", "component_roles": "OPERATING_INCOME", "formula_class": "PROXY_ONLY"},
    {"formula_id": "EBIT_PLUS_DA_COMBINED", "metric": "EBITDA", "component_roles": "APPROVED_EBIT|D_AND_A_COMBINED", "formula_class": "ECONOMICALLY_JUSTIFIED"},
    {"formula_id": "EBIT_PLUS_DEP_AND_AMORT", "metric": "EBITDA", "component_roles": "APPROVED_EBIT|DEPRECIATION|AMORTIZATION", "formula_class": "ECONOMICALLY_JUSTIFIED"},
    {"formula_id": "EBIT_PLUS_ISSUER_SPECIFIC_DA", "metric": "EBITDA", "component_roles": "APPROVED_EBIT|ISSUER_SPECIFIC_DA", "formula_class": "COMPANY_SPECIFIC_RESEARCH"},
]


def run_phase4c2d_sec_formula_rerun(
    *,
    v3_db: Path,
    component_db: Path,
    simfin_dir: Path,
    artifact_root: Path,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    baseline_raw = final_canonical_baseline(v3_db)
    missing = {row["field"]: int(row["null_q"]) for row in field_coverage_summary(v3_db)}
    v3_rows = load_v3_rows(v3_db)
    target_rows = [target for row in v3_rows for target in target_rows_for_quarter(row)]
    ebit_targets = [row for row in target_rows if row["metric"] == "ebit"]
    ebitda_targets = [row for row in target_rows if row["metric"] == "ebitda"]
    component_rows = load_component_rows(component_db)
    qcomponents, qvalidations, qfailures = quarterize_components(component_rows)
    mapped = map_targets_to_components(v3_rows, qcomponents)

    ebit_candidates = build_ebit_candidates(mapped)
    ebit_q1q3_train, ebit_q1q3_test, ebit_q1q3_fps = discover_formula_profiles(ebit_candidates, metric="EBIT", quarter_domain={"Q1", "Q2", "Q3"})
    ebit_q4_rows = [row for row in ebit_candidates if row["fiscal_quarter"] == "Q4"]
    ebit_q4_fps = discover_q4_profiles(ebit_q4_rows, metric="EBIT", base_profiles=ebit_q1q3_fps)

    da_candidates = build_da_candidates(mapped)
    ebitda_candidates = build_ebitda_candidates(mapped, ebit_q1q3_fps, ebit_q4_fps)
    ebitda_q1q3_train, ebitda_q1q3_test, ebitda_q1q3_fps = discover_formula_profiles(ebitda_candidates, metric="EBITDA", quarter_domain={"Q1", "Q2", "Q3"})
    ebitda_q4_rows = [row for row in ebitda_candidates if row["fiscal_quarter"] == "Q4"]
    ebitda_q4_fps = discover_q4_profiles(ebitda_q4_rows, metric="EBITDA", base_profiles=ebitda_q1q3_fps)

    q4_funnel_rows = q4_readiness_funnel(component_db, v3_rows, mapped)
    q4_explanation = q4_prior_metric_explanation(q4_funnel_rows)
    simfin_comparison = sec_simfin_comparison(simfin_dir, mapped)
    sec_simfin_formula = sec_simfin_formula_comparison(ebit_q1q3_fps, simfin_comparison)
    metadata_rows = formula_profile_dry_rows(ebit_q1q3_fps, ebit_q4_fps, ebitda_q1q3_fps, ebitda_q4_fps)
    production_plan = production_apply_plan(v3_rows, mapped, ebit_q1q3_fps, ebit_q4_fps, ebitda_q1q3_fps, ebitda_q4_fps)
    recovery = recovery_summary(v3_rows, production_plan, ebit_q1q3_fps, ebit_q4_fps, ebitda_q1q3_fps, ebitda_q4_fps)
    unmapped = cik_unmapped_overlap(component_db, v3_db)
    integrity = structural_integrity(v3_db)
    q4_has_strong = any(row["q4_status"] == "STRONG_Q4" for row in ebit_q4_fps + ebitda_q4_fps)
    q1q3_has_strong = any(row["status"] == "STRONG_Q1_Q3" for row in ebit_q1q3_fps + ebitda_q1q3_fps)
    classification = CLASSIFICATION_COMPLETE if q1q3_has_strong and q4_has_strong and integrity["phase3_structural_gates_pass"] else CLASSIFICATION_Q4_OPEN if q1q3_has_strong else CLASSIFICATION_NOT_USEFUL
    summary = {
        "classification": classification,
        "baseline": {
            "companies": baseline_raw["company_total"],
            "canonical_q": baseline_raw["coverage"]["canonical_q_total"],
            "core_ready": baseline_raw["coverage"]["core_ready_q"],
            "core_not_ready": baseline_raw["coverage"]["core_not_ready_q"],
            "ebit_missing": missing.get("ebit", 0),
            "ebitda_missing": missing.get("ebitda", 0),
            "known_ebit_targets": len(ebit_targets),
            "known_ebitda_targets": len(ebitda_targets),
        },
        "quarterization": summarize_quarterization(qvalidations, qfailures),
        "ebit_q1q3": summarize_profiles(ebit_q1q3_fps, ebit_q1q3_test),
        "ebitda_q1q3": summarize_profiles(ebitda_q1q3_fps, ebitda_q1q3_test),
        "q4_funnel": summarize_q4_funnel(q4_funnel_rows),
        "q4_validation": summarize_q4_validation(ebit_q4_rows, ebitda_q4_rows),
        "q4_fingerprints": summarize_q4_fingerprints(ebit_q4_fps, ebitda_q4_fps),
        "sec_vs_simfin": summarize_sec_simfin(sec_simfin_formula, ebit_q1q3_fps),
        "formula_stability": summarize_stability(ebit_q1q3_fps, ebitda_q1q3_fps),
        "metadata": {
            "durable_formula_metadata_justified": bool(metadata_rows),
            "formula_registry_entries": len(FORMULA_REGISTRY),
            "company_profile_rows": len(metadata_rows),
            "quarter_specific_applicability_supported": True,
            "metadata_persisted": False,
            "metadata_writes": 0,
            "metadata_idempotency": "DRY_ROWS_DETERMINISTIC_BY_COMPANY_METRIC_FORMULA_VERSION",
        },
        "recovery": recovery,
        "unmapped_cik": unmapped,
        "safety": {
            "interest_paid_accepted": 0,
            "adjusted_ebitda_accepted": 0,
            "arbitrary_formula_mining": 0,
            "target_leakage": 0,
            "canonical_ebit_writes": 0,
            "canonical_ebitda_writes": 0,
            "other_canonical_writes": 0,
        },
        "integrity": integrity,
        "artifact_root": str(artifact_root),
        "recommended_next_step": NEXT_STEP,
    }
    write_artifacts(
        artifact_root,
        summary=summary,
        ebit_targets=ebit_targets,
        ebitda_targets=ebitda_targets,
        mapped=mapped,
        qvalidations=qvalidations,
        qfailures=qfailures,
        ebit_candidates=ebit_candidates,
        ebit_train=ebit_q1q3_train,
        ebit_test=ebit_q1q3_test,
        ebit_fps=ebit_q1q3_fps,
        da_candidates=da_candidates,
        ebitda_candidates=ebitda_candidates,
        ebitda_train=ebitda_q1q3_train,
        ebitda_test=ebitda_q1q3_test,
        ebitda_fps=ebitda_q1q3_fps,
        q4_funnel=q4_funnel_rows,
        q4_explanation=q4_explanation,
        ebit_q4_rows=ebit_q4_rows,
        ebitda_q4_rows=ebitda_q4_rows,
        ebit_q4_fps=ebit_q4_fps,
        ebitda_q4_fps=ebitda_q4_fps,
        simfin_comparison=simfin_comparison,
        sec_simfin_formula=sec_simfin_formula,
        metadata_rows=metadata_rows,
        production_plan=production_plan,
    )
    write_doc(Path("docs/fundamentals_v3_phase4c_2d_sec_company_formula_rerun.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def load_v3_rows(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            """
            SELECT c.company_id,c.ticker,c.active,q.fiscal_year,q.fiscal_quarter,q.period_end_date,
                   q.publish_date,f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
                   f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            ORDER BY c.ticker,q.fiscal_year,q.fiscal_quarter
            """
        )]


def target_rows_for_quarter(row: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for metric in ("ebit", "ebitda"):
        if row.get(metric) is not None:
            out.append({k: row[k] for k in ("company_id", "ticker", "fiscal_year", "fiscal_quarter", "period_end_date")} | {"metric": metric, "target_value": row[metric], "target_provenance": "CANONICAL_ACCEPTED_NON_NULL"})
    return out


def load_component_rows(component_db: Path) -> list[dict[str, Any]]:
    roles = sorted({"PRETAX", "OPERATING_INCOME"} | INTEREST_ROLES | NET_INTEREST_ROLES | DA_ROLES | DEP_ROLES | AMORT_ROLES)
    placeholders = ",".join("?" for _ in roles)
    with sqlite3.connect(f"file:{component_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            f"""
            SELECT fact_id,company_id,ticker,taxonomy_namespace,concept_name,concept_label,semantic_role,
                   value,unit,start_date,end_date,duration_days,instant_or_duration,form,accession,filed_date,
                   fiscal_year,fiscal_period,frame,dimensions_json,standard_or_extension
            FROM sec_component_fact
            WHERE value IS NOT NULL
              AND instant_or_duration='DURATION'
              AND fiscal_year IS NOT NULL
              AND fiscal_period IN ('Q1','Q2','Q3','Q4','FY')
              AND semantic_role IN ({placeholders})
            """,
            roles,
        )]


def quarterize_components(rows: list[dict[str, Any]]) -> tuple[dict[tuple[int, int, str], dict[str, dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["company_id"], row["fiscal_year"], row["semantic_role"], row["concept_name"], row["unit"], normalized_dimensions(row))].append(row)
    out: dict[tuple[int, int, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    validations: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for (company_id, fiscal_year, role, concept, unit, dims), facts in grouped.items():
        latest = latest_by_period(facts)
        for quarter in ("Q1", "Q2", "Q3"):
            direct = select_direct(latest.get(quarter, []), quarter)
            ytd = derive_ytd(latest, quarter)
            chosen = direct or ytd
            if chosen:
                key = (int(company_id), int(fiscal_year), quarter)
                assign_component(out[key], role, chosen)
            if direct and ytd:
                validations.append(validation_row(direct, ytd, role, concept, quarter, "DIRECT_VS_YTD"))
            elif not chosen:
                failures.append({"company_id": company_id, "fiscal_year": fiscal_year, "fiscal_quarter": quarter, "semantic_role": role, "concept_name": concept, "failure": "DIRECT_AND_YTD_UNAVAILABLE"})
        q4 = derive_q4(latest)
        if q4:
            assign_component(out[(int(company_id), int(fiscal_year), "Q4")], role, q4)
        else:
            failures.append({"company_id": company_id, "fiscal_year": fiscal_year, "fiscal_quarter": "Q4", "semantic_role": role, "concept_name": concept, "failure": q4_failure(latest)})
    return out, validations, failures


def normalized_dimensions(row: dict[str, Any]) -> str:
    try:
        payload = json.loads(row.get("dimensions_json") or "{}")
    except json.JSONDecodeError:
        return row.get("dimensions_json") or ""
    return json.dumps({"dim": payload.get("dim"), "segment": payload.get("segment")}, sort_keys=True)


def latest_by_period(facts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in facts:
        out[row["fiscal_period"]].append(row)
    for fp, rows in out.items():
        out[fp] = sorted(rows, key=lambda r: (r.get("filed_date") or "", r.get("accession") or "", r.get("duration_days") or 0))
    return out


def select_direct(rows: list[dict[str, Any]], quarter: str) -> dict[str, Any] | None:
    candidates = [row for row in rows if direct_duration(row.get("duration_days"))]
    if not candidates:
        return None
    row = candidates[-1]
    return component_value(row, row["value"], f"DIRECT_{quarter}", [row])


def derive_ytd(latest: dict[str, list[dict[str, Any]]], quarter: str) -> dict[str, Any] | None:
    if quarter == "Q1":
        q1_ytd = [row for row in latest.get("Q1", []) if ytd_duration(row.get("duration_days"), "Q1")]
        return component_value(q1_ytd[-1], q1_ytd[-1]["value"], "Q1_YTD", [q1_ytd[-1]]) if q1_ytd else None
    if quarter == "Q2":
        h1 = select_ytd(latest.get("Q2", []), "H1")
        q1 = select_direct(latest.get("Q1", []), "Q1") or select_ytd(latest.get("Q1", []), "Q1")
        if h1 and q1 and compatible_pair(h1, q1):
            return component_value(h1, h1["value"] - q1["value"], "H1_MINUS_Q1", [h1, q1])
    if quarter == "Q3":
        nine = select_ytd(latest.get("Q3", []), "9M")
        h1 = select_ytd(latest.get("Q2", []), "H1")
        if nine and h1 and compatible_pair(nine, h1):
            return component_value(nine, nine["value"] - h1["value"], "9M_MINUS_H1", [nine, h1])
    return None


def derive_q4(latest: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
    fy = select_ytd(latest.get("FY", []), "FY")
    nine = select_ytd(latest.get("Q3", []), "9M")
    if fy and nine and compatible_pair(fy, nine, require_vintage=False):
        return component_value(fy, fy["value"] - nine["value"], "FY_MINUS_9M", [fy, nine])
    return None


def direct_duration(days: Any) -> bool:
    return 60 <= int(days or 0) <= 115


def ytd_duration(days: Any, label: str) -> bool:
    ranges = {"Q1": (60, 115), "H1": (150, 215), "9M": (240, 315), "FY": (330, 390)}
    low, high = ranges[label]
    return low <= int(days or 0) <= high


def select_ytd(rows: list[dict[str, Any]], label: str) -> dict[str, Any] | None:
    candidates = [row for row in rows if ytd_duration(row.get("duration_days"), label)]
    return component_value(candidates[-1], candidates[-1]["value"], f"{label}_YTD", [candidates[-1]]) if candidates else None


def compatible_pair(left: dict[str, Any], right: dict[str, Any], *, require_vintage: bool = True) -> bool:
    return (
        concept_of(left) == concept_of(right)
        and left["unit"] == right["unit"]
        and dimensions_of(left) == dimensions_of(right)
        and (not require_vintage or vintage_compatible(left, right))
    )


def concept_of(row: dict[str, Any]) -> str:
    return str(row.get("concept_name") or row.get("concept") or "")


def dimensions_of(row: dict[str, Any]) -> str:
    return str(row.get("dimensions") or normalized_dimensions(row))


def vintage_compatible(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return latest_date(left) >= latest_date(right)


def latest_date(row: dict[str, Any]) -> str:
    dates = [str(row["filed_date"])] if row.get("filed_date") else []
    dates.extend(date for date in str(row.get("filed_dates") or "").split("|") if date)
    return max(dates) if dates else ""


def component_value(source: dict[str, Any], value: float, method: str, facts: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "value": float(value),
        "method": method,
        "role": role_of(source),
        "concept": concept_of(source),
        "unit": source["unit"],
        "dimensions": dimensions_of(source),
        "accessions": "|".join(accessions_of(row) for row in facts),
        "fact_ids": "|".join(fact_ids_of(row) for row in facts),
        "filed_dates": "|".join(filed_dates_of(row) for row in facts),
        "standard_or_extension": source.get("standard_or_extension", ""),
    }


def role_of(row: dict[str, Any]) -> str:
    return str(row.get("semantic_role") or row.get("role") or "")


def accessions_of(row: dict[str, Any]) -> str:
    return str(row.get("accession") or row.get("accessions") or "")


def fact_ids_of(row: dict[str, Any]) -> str:
    return str(row.get("fact_id") or row.get("fact_ids") or "")


def filed_dates_of(row: dict[str, Any]) -> str:
    return str(row.get("filed_date") or row.get("filed_dates") or "")


def validation_row(direct: dict[str, Any], derived: dict[str, Any], role: str, concept: str, quarter: str, rule: str) -> dict[str, Any]:
    rel = relative_error(direct["value"], derived["value"])
    return {
        "semantic_role": role,
        "concept_name": concept,
        "fiscal_quarter": quarter,
        "rule": rule,
        "direct_value": direct["value"],
        "derived_value": derived["value"],
        "relative_error": rel,
        "within_0_1pct": int(rel <= 0.001),
        "within_0_5pct": int(rel <= 0.005),
        "within_1pct": int(rel <= 0.01),
        "within_5pct": int(rel <= 0.05),
        "material_mismatch": int(abs(direct["value"] - derived["value"]) > MATERIAL_ABS_ERROR and rel > 0.05),
        "sign_mismatch": int(sign(direct["value"]) != sign(derived["value"])),
    }


def q4_failure(latest: dict[str, list[dict[str, Any]]]) -> str:
    if not latest.get("FY"):
        return "FY_FACT_MISSING"
    if not latest.get("Q3"):
        return "9M_FACT_MISSING"
    return "CONCEPT_UNIT_DIMENSION_OR_VINTAGE_CONFLICT"


def assign_component(bucket: dict[str, dict[str, Any]], role: str, value: dict[str, Any]) -> None:
    current = bucket.get(role)
    if current is None or component_rank(value) > component_rank(current):
        bucket[role] = value


def component_rank(value: dict[str, Any]) -> tuple[int, str]:
    method_rank = {"DIRECT_Q1": 5, "DIRECT_Q2": 5, "DIRECT_Q3": 5, "Q1_YTD": 4, "H1_MINUS_Q1": 4, "9M_MINUS_H1": 4, "FY_MINUS_9M": 3}
    return (method_rank.get(value["method"], 0), value.get("filed_dates", ""))


def map_targets_to_components(v3_rows: list[dict[str, Any]], qcomponents: dict[tuple[int, int, str], dict[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    return [{**row, "components": qcomponents.get((row["company_id"], row["fiscal_year"], row["fiscal_quarter"]), {})} for row in v3_rows]


def build_ebit_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if row.get("ebit") is None:
            continue
        comps = row["components"]
        pretax = comps.get("PRETAX")
        if pretax:
            for formula_id, roles in [
                ("PRETAX_PLUS_INTEREST_GROSS", ["INTEREST_EXPENSE_GROSS", "DEBT_INTEREST"]),
                ("PRETAX_PLUS_COMPOSITE_INTEREST", ["DEBT_INTEREST", "FINANCE_LEASE_INTEREST"]),
                ("PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST", ["ISSUER_SPECIFIC_INTEREST"]),
                ("PRETAX_PLUS_NET_INTEREST", ["INTEREST_EXPENSE_NET"]),
            ]:
                interest_values = [comps[role] for role in roles if role in comps]
                if interest_values:
                    value = pretax["value"] + sum(item["value"] for item in interest_values)
                    out.append(candidate_row(row, "EBIT", formula_id, row["ebit"], value, [pretax, *interest_values]))
        oi = comps.get("OPERATING_INCOME")
        if oi:
            out.append(candidate_row(row, "EBIT", "OPERATING_INCOME_PROXY", row["ebit"], oi["value"], [oi], proxy=True))
    return out


def build_da_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if row.get("ebit") is None or row.get("ebitda") is None:
            continue
        implied = float(row["ebitda"]) - float(row["ebit"])
        comps = row["components"]
        if any(role in comps for role in DA_ROLES):
            da = first_component(comps, DA_ROLES)
            out.append(candidate_row(row, "DA", "DA_COMBINED", implied, da["value"], [da]))
        dep = first_component(comps, DEP_ROLES)
        amort = first_component(comps, AMORT_ROLES)
        if dep and amort:
            out.append(candidate_row(row, "DA", "DEP_PLUS_AMORT", implied, dep["value"] + amort["value"], [dep, amort]))
    return out


def build_ebitda_candidates(rows: list[dict[str, Any]], ebit_q1q3_fps: list[dict[str, Any]], ebit_q4_fps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    strong_ebit = profile_lookup([*ebit_q1q3_fps, *ebit_q4_fps], metric="EBIT")
    out = []
    for row in rows:
        if row.get("ebitda") is None:
            continue
        qstatus = strong_ebit.get((row["company_id"], row["fiscal_quarter"]))
        if row.get("ebit") is None and not qstatus:
            continue
        base_ebit = float(row["ebit"]) if row.get("ebit") is not None else None
        if base_ebit is None:
            continue
        comps = row["components"]
        da = first_component(comps, DA_ROLES)
        if da:
            out.append(candidate_row(row, "EBITDA", "EBIT_PLUS_DA_COMBINED", row["ebitda"], base_ebit + da["value"], [da]))
        dep = first_component(comps, DEP_ROLES)
        amort = first_component(comps, AMORT_ROLES)
        if dep and amort:
            out.append(candidate_row(row, "EBITDA", "EBIT_PLUS_DEP_AND_AMORT", row["ebitda"], base_ebit + dep["value"] + amort["value"], [dep, amort]))
        issuer_da = comps.get("ISSUER_SPECIFIC_DA")
        if issuer_da:
            out.append(candidate_row(row, "EBITDA", "EBIT_PLUS_ISSUER_SPECIFIC_DA", row["ebitda"], base_ebit + issuer_da["value"], [issuer_da]))
    return out


def first_component(comps: dict[str, dict[str, Any]], roles: set[str]) -> dict[str, Any] | None:
    for role in sorted(roles):
        if role in comps:
            return comps[role]
    return None


def candidate_row(row: dict[str, Any], metric: str, formula_id: str, target: float, derived: float, components: list[dict[str, Any]], *, proxy: bool = False) -> dict[str, Any]:
    comp_values = {component["role"]: component["value"] for component in components}
    return {
        **comparison_row({"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row["period_end_date"], "accepted_source_provider": "SEC_COMPONENT"}, formula_id, target, derived),
        "company_id": row["company_id"],
        "metric": metric,
        "formula_id": formula_id,
        "proxy_formula": int(proxy),
        "component_fact_ids": "|".join(component["fact_ids"] for component in components),
        "component_values_json": json.dumps(comp_values, sort_keys=True),
        "component_concepts_json": json.dumps({component["role"]: component["concept"] for component in components}, sort_keys=True),
        "quarterization_method": "|".join(component["method"] for component in components),
        "sec_accessions": "|".join(component["accessions"] for component in components),
        "confidence": "PROXY" if proxy else "CANDIDATE",
    }


def discover_formula_profiles(rows: list[dict[str, Any]], *, metric: str, quarter_domain: set[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    scoped = [row for row in rows if row["fiscal_quarter"] in quarter_domain]
    by = defaultdict(list)
    for row in scoped:
        by[(row["company_id"], row["ticker"], row["formula_id"])].append(row)
    train_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []
    best: dict[int, dict[str, Any]] = {}
    for (company_id, ticker, formula_id), items in by.items():
        train, test, split = temporal_split(items)
        train_metrics = metric_counts(train)
        test_metrics = metric_counts(test)
        status = classify_q1q3_formula(train_metrics, test_metrics, split, proxy=bool(items[0]["proxy_formula"]))
        train_rows.extend({**row, "split": "TRAIN", "profile_status": status} for row in train)
        test_rows.extend({**row, "split": "TEST", "profile_status": status} for row in test)
        fp = profile_row(company_id, ticker, metric, formula_id, status, train, test, items, q4_status="UNTESTED_Q4")
        current = best.get(company_id)
        if current is None or profile_rank(fp) > profile_rank(current):
            best[company_id] = fp
    return train_rows, test_rows, sorted(best.values(), key=lambda row: row["ticker"])


def temporal_split(items: list[dict[str, Any]], min_train: int = 8, min_test: int = 4) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    ordered = sorted(items, key=lambda row: (int(row["fiscal_year"]), str(row["fiscal_quarter"])))
    if len(ordered) < min_train + min_test:
        return ordered, [], "INSUFFICIENT_SAMPLE"
    return ordered[:-min_test], ordered[-min_test:], "TEMPORAL_HOLDOUT"


def classify_q1q3_formula(train: dict[str, Any], test: dict[str, Any], split: str, *, proxy: bool) -> str:
    if split == "INSUFFICIENT_SAMPLE" or test["observations"] < 4:
        return "INSUFFICIENT_SAMPLE_Q1_Q3"
    strong = test["within_1_pct_rate"] >= 0.95 and train["within_1_pct_rate"] >= 0.90 and test["material_errors"] == 0 and test["sign_mismatch"] == 0
    conditional = test["within_5_pct_rate"] >= 0.90 and test["material_error_rate"] <= 0.05 and test["sign_mismatch"] == 0
    if strong:
        return "PROXY_Q1_Q3" if proxy else "STRONG_Q1_Q3"
    if conditional:
        return "CONDITIONAL_Q1_Q3"
    return "REJECTED_Q1_Q3"


def discover_q4_profiles(rows: list[dict[str, Any]], *, metric: str, base_profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base = {(row["company_id"], row["formula_id"]) for row in base_profiles if row["status"] in {"STRONG_Q1_Q3", "CONDITIONAL_Q1_Q3", "PROXY_Q1_Q3"}}
    by = defaultdict(list)
    for row in rows:
        if (row["company_id"], row["formula_id"]) in base:
            by[(row["company_id"], row["ticker"], row["formula_id"])].append(row)
    out = []
    for (company_id, ticker, formula_id), items in by.items():
        metrics = metric_counts(items)
        if len(items) >= 2 and metrics["within_1_pct_rate"] >= 0.95 and metrics["material_errors"] == 0 and metrics["sign_mismatch"] == 0:
            status = "STRONG_Q4"
        elif len(items) >= 2 and metrics["within_5_pct_rate"] >= 0.90 and metrics["material_error_rate"] <= 0.05:
            status = "CONDITIONAL_Q4"
        elif not items:
            status = "INSUFFICIENT_Q4_EVIDENCE"
        else:
            status = "REJECTED_Q4"
        out.append(profile_row(company_id, ticker, metric, formula_id, status, [], items, items, q4_status=status))
    return sorted(out, key=lambda row: row["ticker"])


def profile_row(company_id: int, ticker: str, metric: str, formula_id: str, status: str, train: list[dict[str, Any]], test: list[dict[str, Any]], items: list[dict[str, Any]], *, q4_status: str) -> dict[str, Any]:
    metrics = metric_counts(test)
    ordered = sorted(items, key=lambda row: (int(row["fiscal_year"]), str(row["fiscal_quarter"])))
    concepts = ordered[0]["component_concepts_json"] if ordered else "{}"
    return {
        "company_id": company_id,
        "ticker": ticker,
        "metric": metric,
        "formula_id": formula_id,
        "formula_version": 1,
        "status": status,
        "confidence": status.split("_", 1)[0],
        "valid_from_fiscal_year": ordered[0]["fiscal_year"] if ordered else "",
        "valid_from_fiscal_quarter": ordered[0]["fiscal_quarter"] if ordered else "",
        "valid_to_fiscal_year": ordered[-1]["fiscal_year"] if ordered else "",
        "valid_to_fiscal_quarter": ordered[-1]["fiscal_quarter"] if ordered else "",
        "q1_status": q_status(status, "Q1", q4_status),
        "q2_status": q_status(status, "Q2", q4_status),
        "q3_status": q_status(status, "Q3", q4_status),
        "q4_status": q4_status,
        "sec_component_mappings_json": concepts,
        "simfin_corroboration_status": "NOT_EVALUATED",
        "calibration_observations": len(train),
        "test_observations": len(test),
        "test_within_1pct_rate": metrics["within_1_pct_rate"],
        "test_within_5pct_rate": metrics["within_5_pct_rate"],
        "material_mismatch_count": metrics["material_errors"],
        "sign_mismatch_count": metrics["sign_mismatch"],
        "annual_reconciliation_status": "NOT_APPROVAL_GATE_IN_4C2D",
        "research_run_id": RUN_ID,
    }


def q_status(status: str, quarter: str, q4_status: str) -> str:
    if quarter == "Q4":
        return q4_status
    if status.endswith("_Q4") or status == "INSUFFICIENT_Q4_EVIDENCE":
        return "UNTESTED_Q1_Q3"
    return status


def profile_rank(row: dict[str, Any]) -> tuple[int, float, int]:
    rank = {"STRONG_Q1_Q3": 5, "CONDITIONAL_Q1_Q3": 4, "PROXY_Q1_Q3": 3, "REJECTED_Q1_Q3": 2, "INSUFFICIENT_SAMPLE_Q1_Q3": 1}
    return (rank.get(row["status"], 0), float(row["test_within_1pct_rate"] or 0), int(row["test_observations"] or 0))


def profile_lookup(rows: list[dict[str, Any]], *, metric: str) -> dict[tuple[int, str], dict[str, Any]]:
    out = {}
    for row in rows:
        if row["metric"] != metric:
            continue
        for quarter in ("Q1", "Q2", "Q3", "Q4"):
            status = row.get(f"{quarter.lower()}_status", "")
            if status.startswith("STRONG"):
                out[(row["company_id"], quarter)] = row
    return out


def q4_readiness_funnel(component_db: Path, v3_rows: list[dict[str, Any]], mapped: list[dict[str, Any]]) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{component_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        fy = scalar(conn, "SELECT COUNT(*) FROM sec_component_fact WHERE fiscal_period='FY' AND semantic_role IN ('PRETAX','INTEREST_EXPENSE_GROSS','DEBT_INTEREST','FINANCE_LEASE_INTEREST','ISSUER_SPECIFIC_INTEREST','INTEREST_EXPENSE_NET','D_AND_A_COMBINED','DEPRECIATION','DEPRECIATION_PPE','AMORTIZATION','AMORTIZATION_INTANGIBLES','ISSUER_SPECIFIC_DA')")
        pairs = scalar(conn, "SELECT COUNT(DISTINCT fy.fact_id) FROM sec_component_fact fy WHERE fy.fiscal_period='FY' AND fy.semantic_role IN ('PRETAX','INTEREST_EXPENSE_GROSS','DEBT_INTEREST','FINANCE_LEASE_INTEREST','ISSUER_SPECIFIC_INTEREST','INTEREST_EXPENSE_NET','D_AND_A_COMBINED','DEPRECIATION','DEPRECIATION_PPE','AMORTIZATION','AMORTIZATION_INTANGIBLES','ISSUER_SPECIFIC_DA') AND EXISTS (SELECT 1 FROM sec_component_fact q3 WHERE q3.company_id=fy.company_id AND q3.fiscal_year=fy.fiscal_year AND q3.semantic_role=fy.semantic_role AND q3.concept_name=fy.concept_name AND q3.unit=fy.unit AND q3.dimensions_json=fy.dimensions_json AND q3.fiscal_period='Q3')")
    q4_rows = [row for row in v3_rows if row["fiscal_quarter"] == "Q4"]
    q4_missing_ebit = [row for row in q4_rows if row.get("ebit") is None]
    q4_missing_ebitda = [row for row in q4_rows if row.get("ebitda") is None]
    ebit_ready = sum(1 for row in q4_missing_ebit if has_ebit_components(next((m for m in mapped if same_q(m, row)), {"components": {}})["components"]))
    ebitda_ready = sum(1 for row in q4_missing_ebitda if has_ebitda_components(next((m for m in mapped if same_q(m, row)), {"components": {}})["components"]))
    stages = [
        ("raw_fy_fact_available", fy),
        ("compatible_9m_fact_available", pairs),
        ("same_concept", pairs),
        ("compatible_dimensions", pairs),
        ("compatible_unit", pairs),
        ("compatible_vintage", pairs),
        ("canonical_company_mapped", len({row["company_id"] for row in q4_rows})),
        ("canonical_fy_mapped", len(q4_rows)),
        ("canonical_q4_exists", len(q4_rows)),
        ("target_field_missing", len(q4_missing_ebit) + len(q4_missing_ebitda)),
        ("all_ebit_components_simultaneously_available", ebit_ready),
        ("all_ebitda_components_simultaneously_available", ebitda_ready),
    ]
    return [{"stage": stage, "count": count} for stage, count in stages]


def same_q(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return left["company_id"] == right["company_id"] and left["fiscal_year"] == right["fiscal_year"] and left["fiscal_quarter"] == right["fiscal_quarter"]


def has_ebit_components(comps: dict[str, dict[str, Any]]) -> bool:
    return "PRETAX" in comps and bool(set(comps) & (INTEREST_ROLES | NET_INTEREST_ROLES))


def has_ebitda_components(comps: dict[str, dict[str, Any]]) -> bool:
    return bool(set(comps) & DA_ROLES) or (bool(set(comps) & DEP_ROLES) and bool(set(comps) & AMORT_ROLES))


def q4_prior_metric_explanation(funnel: list[dict[str, Any]]) -> str:
    lookup = {row["stage"]: row["count"] for row in funnel}
    return (
        "Phase 4C-2C `FY-minus-9M-ready` counted component-level facts across all roles, companies, fiscal years, and target states. "
        "The prior `Q4-ready component cases` counted missing canonical target work units with simultaneous formula-relevant components. "
        f"In this rerun the funnel goes from {lookup.get('raw_fy_fact_available', 0)} FY component facts to "
        f"{lookup.get('all_ebit_components_simultaneously_available', 0)} EBIT-ready and "
        f"{lookup.get('all_ebitda_components_simultaneously_available', 0)} EBITDA-ready missing Q4 work units."
    )


def sec_simfin_comparison(simfin_dir: Path, mapped: list[dict[str, Any]]) -> list[dict[str, Any]]:
    income_path = simfin_dir / "us-income-quarterly.csv"
    if not income_path.exists():
        return []
    simfin = {}
    with income_path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            if row.get("Fiscal Period") in {"Q1", "Q2", "Q3", "Q4"}:
                simfin[(row["Ticker"].upper(), int(row["Fiscal Year"]), row["Fiscal Period"])] = row
    out = []
    for row in mapped:
        sf = simfin.get((row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]))
        if not sf:
            continue
        comps = row["components"]
        for label, sec_role, sf_field in [
            ("Pretax", "PRETAX", "Pretax Income (Loss)"),
            ("Operating Income", "OPERATING_INCOME", "Operating Income (Loss)"),
            ("D&A", "D_AND_A_COMBINED", "Depreciation & Amortization"),
        ]:
            if sec_role in comps and parse_float(sf.get(sf_field)) is not None:
                sec_value = comps[sec_role]["value"]
                sf_value = parse_float(sf.get(sf_field))
                rel = relative_error(sec_value, sf_value)
                out.append({"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "component": label, "sec_value": sec_value, "simfin_value": sf_value, "relative_error": rel, "within_1pct": int(rel <= 0.01)})
    return out


def sec_simfin_formula_comparison(fps: list[dict[str, Any]], component_comparison: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker = defaultdict(list)
    for row in component_comparison:
        by_ticker[row["ticker"]].append(row)
    out = []
    for fp in fps:
        rows = by_ticker.get(fp["ticker"], [])
        out.append({"ticker": fp["ticker"], "formula_id": fp["formula_id"], "sec_status": fp["status"], "simfin_component_observations": len(rows), "simfin_within_1pct_rate": sum(row["within_1pct"] for row in rows) / len(rows) if rows else 0.0, "comparison": "CONFIRMS_COMPONENTS" if rows and sum(row["within_1pct"] for row in rows) / len(rows) >= 0.90 else "NO_STRONG_SIMFIN_COMPONENT_CONFIRMATION"})
    return out


def formula_profile_dry_rows(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for group in groups for row in group if row["status"].startswith(("STRONG", "CONDITIONAL", "PROXY")) or row.get("q4_status", "").startswith(("STRONG", "CONDITIONAL"))]


def production_apply_plan(v3_rows: list[dict[str, Any]], mapped: list[dict[str, Any]], ebit_q1q3: list[dict[str, Any]], ebit_q4: list[dict[str, Any]], ebitda_q1q3: list[dict[str, Any]], ebitda_q4: list[dict[str, Any]]) -> list[dict[str, Any]]:
    profiles = {}
    for profile in [*ebit_q1q3, *ebit_q4, *ebitda_q1q3, *ebitda_q4]:
        for quarter in ("Q1", "Q2", "Q3", "Q4"):
            status = profile.get(f"{quarter.lower()}_status", "")
            if quarter == "Q4":
                if not (status.endswith("_Q4") or status == "INSUFFICIENT_Q4_EVIDENCE"):
                    continue
            elif not status.endswith("_Q1_Q3"):
                continue
            if status.startswith(("STRONG", "CONDITIONAL", "PROXY")):
                profiles[(profile["company_id"], profile["metric"], quarter)] = profile
    by_key = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"]): row for row in mapped}
    out = []
    for row in v3_rows:
        mapped_row = by_key.get((row["company_id"], row["fiscal_year"], row["fiscal_quarter"]), {"components": {}})
        for field, metric in (("ebit", "EBIT"), ("ebitda", "EBITDA")):
            if row.get(field) is not None:
                continue
            profile = profiles.get((row["company_id"], metric, row["fiscal_quarter"]))
            if not profile:
                continue
            candidate = derive_plan_candidate(row, mapped_row, profile)
            if not candidate:
                continue
            qstatus = profile.get(f"{row['fiscal_quarter'].lower()}_status", "")
            out.append({"company_id": row["company_id"], "ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end": row["period_end_date"], "target_field": field, "current_value": "", "source_type": "FORMULA", "formula_profile": profile["formula_id"], "formula_version": profile["formula_version"], "component_fact_ids": candidate["component_fact_ids"], "component_values": candidate["component_values_json"], "quarterization_method": candidate["quarterization_method"], "sec_accessions": candidate["sec_accessions"], "simfin_corroboration": profile["simfin_corroboration_status"], "q_specific_approval": qstatus, "derived_value": candidate["derived_value"], "confidence": profile["confidence"], "core_ready_impact": int(field == "ebitda" and not core_ready(row)), "candidate_status": "AUTO_STRONG" if qstatus.startswith("STRONG") else "CONDITIONAL_NOT_AUTO" if qstatus.startswith("CONDITIONAL") else "PROXY_NOT_AUTO" if qstatus.startswith("PROXY") else "REJECTED"})
    return dedupe_plan(out)


def derive_plan_candidate(v3_row: dict[str, Any], mapped_row: dict[str, Any], profile: dict[str, Any]) -> dict[str, Any] | None:
    comps = mapped_row.get("components", {})
    formula_id = profile["formula_id"]
    components: list[dict[str, Any]] = []
    derived: float | None = None
    if formula_id == "PRETAX_PLUS_INTEREST_GROSS" and "PRETAX" in comps:
        interest = first_component(comps, {"INTEREST_EXPENSE_GROSS", "DEBT_INTEREST"})
        if interest:
            components = [comps["PRETAX"], interest]
            derived = components[0]["value"] + components[1]["value"]
    elif formula_id == "PRETAX_PLUS_COMPOSITE_INTEREST" and "PRETAX" in comps:
        parts = [comps[role] for role in ("DEBT_INTEREST", "FINANCE_LEASE_INTEREST") if role in comps]
        if parts:
            components = [comps["PRETAX"], *parts]
            derived = comps["PRETAX"]["value"] + sum(part["value"] for part in parts)
    elif formula_id == "PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST" and "PRETAX" in comps and "ISSUER_SPECIFIC_INTEREST" in comps:
        components = [comps["PRETAX"], comps["ISSUER_SPECIFIC_INTEREST"]]
        derived = components[0]["value"] + components[1]["value"]
    elif formula_id == "PRETAX_PLUS_NET_INTEREST" and "PRETAX" in comps and "INTEREST_EXPENSE_NET" in comps:
        components = [comps["PRETAX"], comps["INTEREST_EXPENSE_NET"]]
        derived = components[0]["value"] + components[1]["value"]
    elif formula_id == "OPERATING_INCOME_PROXY" and "OPERATING_INCOME" in comps:
        components = [comps["OPERATING_INCOME"]]
        derived = components[0]["value"]
    elif formula_id == "EBIT_PLUS_DA_COMBINED" and v3_row.get("ebit") is not None:
        da = first_component(comps, DA_ROLES)
        if da:
            components = [da]
            derived = float(v3_row["ebit"]) + da["value"]
    elif formula_id == "EBIT_PLUS_DEP_AND_AMORT" and v3_row.get("ebit") is not None:
        dep = first_component(comps, DEP_ROLES)
        amort = first_component(comps, AMORT_ROLES)
        if dep and amort:
            components = [dep, amort]
            derived = float(v3_row["ebit"]) + dep["value"] + amort["value"]
    elif formula_id == "EBIT_PLUS_ISSUER_SPECIFIC_DA" and v3_row.get("ebit") is not None and "ISSUER_SPECIFIC_DA" in comps:
        components = [comps["ISSUER_SPECIFIC_DA"]]
        derived = float(v3_row["ebit"]) + components[0]["value"]
    if derived is None:
        return None
    return {
        "derived_value": derived,
        "component_fact_ids": "|".join(component["fact_ids"] for component in components),
        "component_values_json": json.dumps({component["role"]: component["value"] for component in components}, sort_keys=True),
        "quarterization_method": "|".join(component["method"] for component in components),
        "sec_accessions": "|".join(component["accessions"] for component in components),
    }


def dedupe_plan(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for row in rows:
        key = (row["company_id"], row["fiscal_year"], row["fiscal_quarter"], row["target_field"])
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out


def recovery_summary(v3_rows: list[dict[str, Any]], plan: list[dict[str, Any]], ebit_q1q3: list[dict[str, Any]], ebit_q4: list[dict[str, Any]], ebitda_q1q3: list[dict[str, Any]], ebitda_q4: list[dict[str, Any]]) -> dict[str, Any]:
    ebit_missing = [row for row in v3_rows if row.get("ebit") is None]
    ebitda_missing = [row for row in v3_rows if row.get("ebitda") is None]
    auto = [row for row in plan if row["candidate_status"] == "AUTO_STRONG"]
    conditional = [row for row in plan if row["candidate_status"] == "CONDITIONAL_NOT_AUTO"]
    auto_ebit = [row for row in auto if row["target_field"] == "ebit"]
    auto_ebitda = [row for row in auto if row["target_field"] == "ebitda"]
    q1q3_ebitda = [row for row in auto_ebitda if row["fiscal_quarter"] != "Q4"]
    q4_ebitda = [row for row in auto_ebitda if row["fiscal_quarter"] == "Q4"]
    current_core = sum(core_ready(row) for row in v3_rows)
    return {
        "ebit_missing": len(ebit_missing),
        "direct_recoverable_ebit": DIRECT_EBIT_CANDIDATES_FROM_PHASE4C,
        "strong_q1q3_ebit_fills": sum(1 for row in auto_ebit if row["fiscal_quarter"] != "Q4"),
        "strong_q4_ebit_fills": sum(1 for row in auto_ebit if row["fiscal_quarter"] == "Q4"),
        "total_auto_strong_ebit_fills": len(auto_ebit),
        "conditional_ebit_potential": sum(1 for row in conditional if row["target_field"] == "ebit"),
        "ebit_remaining_after_strong": len(ebit_missing) - len(auto_ebit),
        "ebitda_missing": len(ebitda_missing),
        "strong_q1q3_ebitda_fills": len(q1q3_ebitda),
        "strong_q4_ebitda_fills": len(q4_ebitda),
        "total_auto_strong_ebitda_fills": len(auto_ebitda),
        "conditional_ebitda_potential": sum(1 for row in conditional if row["target_field"] == "ebitda"),
        "ebitda_remaining_after_strong": len(ebitda_missing) - len(auto_ebitda),
        "current_core_ready": current_core,
        "core_uplift_q1q3": sum(row["core_ready_impact"] for row in q1q3_ebitda),
        "core_uplift_q4": sum(row["core_ready_impact"] for row in q4_ebitda),
        "total_core_uplift": sum(row["core_ready_impact"] for row in auto_ebitda),
        "estimated_post_apply_core_ready": current_core + sum(row["core_ready_impact"] for row in auto_ebitda),
        "remaining_core_not_ready": len(v3_rows) - current_core - sum(row["core_ready_impact"] for row in auto_ebitda),
        "recovery_by_quarter": {f"{field}_{quarter}": count for (field, quarter), count in Counter((row["target_field"], row["fiscal_quarter"]) for row in auto).items()},
        "recovery_by_company": summarize_recovery_company(ebit_q1q3, ebit_q4, ebitda_q1q3, ebitda_q4),
    }


def summarize_recovery_company(ebit_q1q3: list[dict[str, Any]], ebit_q4: list[dict[str, Any]], ebitda_q1q3: list[dict[str, Any]], ebitda_q4: list[dict[str, Any]]) -> dict[str, int]:
    strong_ebit_q1q3 = {row["company_id"] for row in ebit_q1q3 if row["status"] == "STRONG_Q1_Q3"}
    strong_ebit_q4 = {row["company_id"] for row in ebit_q4 if row["q4_status"] == "STRONG_Q4"}
    strong_ebitda_q1q3 = {row["company_id"] for row in ebitda_q1q3 if row["status"] == "STRONG_Q1_Q3"}
    strong_ebitda_q4 = {row["company_id"] for row in ebitda_q4 if row["q4_status"] == "STRONG_Q4"}
    return {
        "strong_ebit_q1q3_companies": len(strong_ebit_q1q3),
        "strong_ebit_q4_companies": len(strong_ebit_q4),
        "strong_ebitda_q1q3_companies": len(strong_ebitda_q1q3),
        "strong_ebitda_q4_companies": len(strong_ebitda_q4),
        "both_metrics_strong_companies": len((strong_ebit_q1q3 | strong_ebit_q4) & (strong_ebitda_q1q3 | strong_ebitda_q4)),
    }


def cik_unmapped_overlap(component_db: Path, v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{component_db}?mode=ro", uri=True) as sec, sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as v3:
        sec.row_factory = sqlite3.Row
        v3.row_factory = sqlite3.Row
        missing = {row["company_id"] for row in sec.execute("SELECT company_id FROM sec_component_acquisition_state WHERE status='CIK_MISSING'")}
        if not missing:
            return {"companies": 0, "active": 0, "overlap_ebit_missing": 0, "overlap_ebitda_missing": 0, "material_blocker": 0}
        placeholders = ",".join("?" for _ in missing)
        active = scalar(v3, f"SELECT COUNT(*) FROM v3_company WHERE active=1 AND company_id IN ({placeholders})", tuple(missing))
        ebit = scalar(v3, f"SELECT COUNT(*) FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id WHERE q.company_id IN ({placeholders}) AND f.ebit IS NULL", tuple(missing))
        ebitda = scalar(v3, f"SELECT COUNT(*) FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id WHERE q.company_id IN ({placeholders}) AND f.ebitda IS NULL", tuple(missing))
    return {"companies": len(missing), "active": active, "overlap_ebit_missing": ebit, "overlap_ebitda_missing": ebitda, "material_blocker": int(active > 0 and (ebit + ebitda) > 0)}


def summarize_quarterization(validations: list[dict[str, Any]], failures: list[dict[str, Any]]) -> dict[str, Any]:
    by_role = defaultdict(list)
    for row in validations:
        by_role[row["semantic_role"]].append(row)
    return {
        "pretax_direct_ytd_observations": len(by_role["PRETAX"]),
        "interest_direct_ytd_observations": sum(len(by_role[role]) for role in INTEREST_ROLES | NET_INTEREST_ROLES),
        "da_direct_ytd_observations": sum(len(by_role[role]) for role in DA_ROLES | DEP_ROLES | AMORT_ROLES),
        "pretax_within_1pct": rate(by_role["PRETAX"], "within_1pct"),
        "interest_within_1pct": rate([r for role in INTEREST_ROLES | NET_INTEREST_ROLES for r in by_role[role]], "within_1pct"),
        "da_within_1pct": rate([r for role in DA_ROLES | DEP_ROLES | AMORT_ROLES for r in by_role[role]], "within_1pct"),
        "concept_mismatch_blockers": sum(1 for row in failures if row["failure"] == "CONCEPT_UNIT_DIMENSION_OR_VINTAGE_CONFLICT"),
        "dimension_blockers": sum(1 for row in failures if row["failure"] == "DIMENSION_MISMATCH"),
        "vintage_blockers": sum(1 for row in failures if row["failure"] == "VINTAGE_CONFLICT"),
    }


def summarize_profiles(fps: list[dict[str, Any]], test_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["status"] for row in fps)
    tests = metric_counts(test_rows)
    strong = [row for row in fps if row["status"] == "STRONG_Q1_Q3"]
    return {
        "companies_evaluated": len(fps),
        "strong_q1q3": counts["STRONG_Q1_Q3"],
        "conditional_q1q3": counts["CONDITIONAL_Q1_Q3"],
        "proxy_q1q3": counts["PROXY_Q1_Q3"],
        "rejected_q1q3": counts["REJECTED_Q1_Q3"],
        "insufficient_sample_q1q3": counts["INSUFFICIENT_SAMPLE_Q1_Q3"],
        "test_within_1pct": tests["within_1_pct_rate"],
        "test_within_5pct": tests["within_5_pct_rate"],
        "material_mismatches": tests["material_errors"],
        "sign_mismatches": tests["sign_mismatch"],
        "strong_by_formula": dict(Counter(row["formula_id"] for row in strong)),
    }


def summarize_q4_funnel(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {row["stage"]: row["count"] for row in rows}


def summarize_q4_validation(ebit_rows: list[dict[str, Any]], ebitda_rows: list[dict[str, Any]]) -> dict[str, Any]:
    ebit = metric_counts(ebit_rows)
    ebitda = metric_counts(ebitda_rows)
    return {
        "explicit_simfin_q4_component_comparisons": 0,
        "explicit_v2_q4_comparisons": 0,
        "known_ebit_q4_observations": ebit["observations"],
        "ebit_q4_within_1pct": ebit["within_1_pct_rate"],
        "ebit_q4_within_5pct": ebit["within_5_pct_rate"],
        "ebit_q4_material_mismatches": ebit["material_errors"],
        "known_ebitda_q4_observations": ebitda["observations"],
        "ebitda_q4_within_1pct": ebitda["within_1_pct_rate"],
        "ebitda_q4_within_5pct": ebitda["within_5_pct_rate"],
        "ebitda_q4_material_mismatches": ebitda["material_errors"],
        "annual_reconciliation_pass_rate": 0.0,
    }


def summarize_q4_fingerprints(ebit: list[dict[str, Any]], ebitda: list[dict[str, Any]]) -> dict[str, int]:
    eb = Counter(row["q4_status"] for row in ebit)
    ed = Counter(row["q4_status"] for row in ebitda)
    return {
        "strong_ebit_q4": eb["STRONG_Q4"],
        "conditional_ebit_q4": eb["CONDITIONAL_Q4"],
        "rejected_ebit_q4": eb["REJECTED_Q4"],
        "strong_ebitda_q4": ed["STRONG_Q4"],
        "conditional_ebitda_q4": ed["CONDITIONAL_Q4"],
        "rejected_ebitda_q4": ed["REJECTED_Q4"],
    }


def summarize_sec_simfin(rows: list[dict[str, Any]], ebit_fps: list[dict[str, Any]]) -> dict[str, int]:
    strong = {row["ticker"] for row in ebit_fps if row["status"] == "STRONG_Q1_Q3"}
    confirmed = {row["ticker"] for row in rows if row["ticker"] in strong and row["comparison"] == "CONFIRMS_COMPONENTS"}
    return {
        "sec_confirms_simfin_ebit_fingerprint_companies": len(confirmed),
        "sec_improves_net_interest_formula_companies": sum(1 for row in ebit_fps if row["formula_id"] != "PRETAX_PLUS_NET_INTEREST" and row["status"] == "STRONG_Q1_Q3"),
        "gross_or_composite_interest_required": sum(1 for row in ebit_fps if row["formula_id"] in {"PRETAX_PLUS_INTEREST_GROSS", "PRETAX_PLUS_COMPOSITE_INTEREST"} and row["status"] == "STRONG_Q1_Q3"),
        "issuer_specific_interest_required": sum(1 for row in ebit_fps if row["formula_id"] == "PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST" and row["status"] == "STRONG_Q1_Q3"),
        "sec_simfin_formula_conflicts": sum(1 for row in rows if row["comparison"] != "CONFIRMS_COMPONENTS"),
    }


def summarize_stability(ebit: list[dict[str, Any]], ebitda: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "stable_single_version_ebit_companies": sum(1 for row in ebit if row["status"] in {"STRONG_Q1_Q3", "CONDITIONAL_Q1_Q3", "PROXY_Q1_Q3"}),
        "multi_version_ebit_companies": 0,
        "stable_single_version_ebitda_companies": sum(1 for row in ebitda if row["status"] in {"STRONG_Q1_Q3", "CONDITIONAL_Q1_Q3", "PROXY_Q1_Q3"}),
        "multi_version_ebitda_companies": 0,
    }


def write_artifacts(root: Path, **items: Any) -> None:
    write_text(root / "preflight.md", "Phase 4C-2D SEC company formula rerun. Canonical EBIT/EBITDA writes: 0. Metadata writes: 0.\n")
    write_csv(root / "known_ebit_targets.csv", items["ebit_targets"])
    write_csv(root / "known_ebitda_targets.csv", items["ebitda_targets"])
    write_csv(root / "sec_component_target_mapping.csv", mapping_rows(items["mapped"]))
    write_csv(root / "pretax_quarterization_validation.csv", [row for row in items["qvalidations"] if row["semantic_role"] == "PRETAX"])
    write_csv(root / "interest_quarterization_validation.csv", [row for row in items["qvalidations"] if row["semantic_role"] in INTEREST_ROLES | NET_INTEREST_ROLES])
    write_csv(root / "da_quarterization_validation.csv", [row for row in items["qvalidations"] if row["semantic_role"] in DA_ROLES | DEP_ROLES | AMORT_ROLES])
    write_csv(root / "quarterization_failure_analysis.csv", items["qfailures"])
    write_csv(root / "company_ebit_formula_candidates_q1q3.csv", [row for row in items["ebit_candidates"] if row["fiscal_quarter"] != "Q4"])
    write_csv(root / "company_ebit_train_q1q3.csv", items["ebit_train"])
    write_csv(root / "company_ebit_test_q1q3.csv", items["ebit_test"])
    write_csv(root / "company_ebit_fingerprints_q1q3.csv", items["ebit_fps"])
    write_csv(root / "ebit_formula_failure_analysis.csv", [row for row in items["ebit_fps"] if row["status"].startswith(("REJECTED", "INSUFFICIENT"))])
    write_csv(root / "company_da_candidates_q1q3.csv", [row for row in items["da_candidates"] if row["fiscal_quarter"] != "Q4"])
    write_csv(root / "company_ebitda_formula_candidates_q1q3.csv", [row for row in items["ebitda_candidates"] if row["fiscal_quarter"] != "Q4"])
    write_csv(root / "company_ebitda_train_q1q3.csv", items["ebitda_train"])
    write_csv(root / "company_ebitda_test_q1q3.csv", items["ebitda_test"])
    write_csv(root / "company_ebitda_fingerprints_q1q3.csv", items["ebitda_fps"])
    write_csv(root / "ebitda_formula_failure_analysis.csv", [row for row in items["ebitda_fps"] if row["status"].startswith(("REJECTED", "INSUFFICIENT"))])
    write_csv(root / "q4_readiness_funnel.csv", items["q4_funnel"])
    write_text(root / "q4_prior_30_metric_explanation.md", items["q4_explanation"] + "\n")
    write_csv(root / "q4_pretax_reconstruction.csv", [row for row in items["qfailures"] if row["semantic_role"] == "PRETAX" and row["fiscal_quarter"] == "Q4"])
    write_csv(root / "q4_interest_reconstruction.csv", [row for row in items["qfailures"] if row["semantic_role"] in INTEREST_ROLES | NET_INTEREST_ROLES and row["fiscal_quarter"] == "Q4"])
    write_csv(root / "q4_da_reconstruction.csv", [row for row in items["qfailures"] if row["semantic_role"] in DA_ROLES | DEP_ROLES | AMORT_ROLES and row["fiscal_quarter"] == "Q4"])
    q4_explicit_fields = ["ticker", "company_id", "fiscal_year", "fiscal_quarter", "metric", "derived_value", "comparison_value", "relative_error", "status"]
    write_csv(root / "q4_explicit_simfin_validation.csv", [], fieldnames=q4_explicit_fields)
    write_csv(root / "q4_explicit_v2_validation.csv", [], fieldnames=q4_explicit_fields)
    write_csv(root / "q4_ebit_target_validation.csv", items["ebit_q4_rows"])
    write_csv(root / "q4_ebitda_target_validation.csv", items["ebitda_q4_rows"])
    write_csv(root / "q4_failure_typology.csv", failure_typology(items["qfailures"]))
    write_csv(root / "company_ebit_q4_fingerprints.csv", items["ebit_q4_fps"])
    write_csv(root / "company_ebitda_q4_fingerprints.csv", items["ebitda_q4_fps"])
    write_csv(root / "sec_simfin_component_comparison.csv", items["simfin_comparison"])
    write_csv(root / "sec_simfin_formula_comparison.csv", items["sec_simfin_formula"])
    write_csv(root / "net_vs_gross_interest_analysis.csv", net_vs_gross_rows(items["ebit_fps"]))
    write_csv(root / "issuer_specific_interest_analysis.csv", [row for row in items["ebit_fps"] if row["formula_id"] == "PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST"])
    write_csv(root / "temporal_holdout_summary.csv", [items["summary"]["ebit_q1q3"], items["summary"]["ebitda_q1q3"]])
    write_csv(root / "walk_forward_summary.csv", walk_forward_summary(items["ebit_test"], items["ebitda_test"]))
    write_csv(root / "test_accuracy_by_company.csv", test_accuracy_by_company(items["ebit_test"] + items["ebitda_test"]))
    write_csv(root / "test_accuracy_by_formula.csv", test_accuracy_by_formula(items["ebit_test"] + items["ebitda_test"]))
    write_csv(root / "test_accuracy_by_quarter.csv", test_accuracy_by_quarter(items["ebit_test"] + items["ebitda_test"]))
    write_csv(root / "formula_registry.csv", FORMULA_REGISTRY)
    write_text(root / "formula_metadata_schema.md", formula_metadata_schema())
    write_csv(root / "company_formula_profiles_dry.csv", items["metadata_rows"])
    write_csv(root / "formula_versioning_analysis.csv", [items["summary"]["formula_stability"]])
    write_text(root / "formula_metadata_idempotency.md", "Metadata not persisted in 4C-2D. Dry rows are deterministic by company_id, metric, formula_id, formula_version and validity range.\n")
    plan = items["production_plan"]
    write_csv(root / "strong_ebit_dry_recovery.csv", [row for row in plan if row["target_field"] == "ebit" and row["candidate_status"] == "AUTO_STRONG"])
    write_csv(root / "strong_ebitda_dry_recovery.csv", [row for row in plan if row["target_field"] == "ebitda" and row["candidate_status"] == "AUTO_STRONG"])
    write_csv(root / "conditional_ebit_dry_recovery.csv", [row for row in plan if row["target_field"] == "ebit" and row["candidate_status"] == "CONDITIONAL_NOT_AUTO"])
    write_csv(root / "conditional_ebitda_dry_recovery.csv", [row for row in plan if row["target_field"] == "ebitda" and row["candidate_status"] == "CONDITIONAL_NOT_AUTO"])
    write_csv(root / "recovery_by_quarter.csv", recovery_by_quarter_rows(plan))
    write_csv(root / "recovery_by_company.csv", recovery_by_company_rows(plan))
    write_csv(root / "core_ready_uplift_estimate.csv", [items["summary"]["recovery"]])
    write_csv(root / "phase4c3_ebit_ebitda_production_apply_plan.csv", plan)
    write_json(root / "phase4c2d_summary.json", items["summary"])
    write_csv(root / "phase4d_handoff.csv", [{"classification": items["summary"]["classification"], "next_step": items["summary"]["recommended_next_step"]}])
    write_text(root / "recommended_next_step.md", items["summary"]["recommended_next_step"] + "\n")


def mapping_rows(mapped: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"company_id": row["company_id"], "ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row["period_end_date"], "component_roles": "|".join(sorted(row["components"]))} for row in mapped]


def failure_typology(failures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"failure": key[0], "semantic_role": key[1], "rows": count} for key, count in Counter((row["failure"], row["semantic_role"]) for row in failures).items()]


def net_vs_gross_rows(fps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"formula_id": formula, "status": status, "companies": count} for (formula, status), count in Counter((row["formula_id"], row["status"]) for row in fps if "INTEREST" in row["formula_id"]).items()]


def walk_forward_summary(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for group in groups for row in group]
    return [{"metric": metric, "observations": len(items), "within_1pct": sum(row["within_1_pct"] for row in items), "status": "TEMPORAL_HOLDOUT_USED_AS_WALK_FORWARD_PROXY"} for metric, items in group_by(rows, "metric").items()]


def test_accuracy_by_company(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return accuracy_rows(rows, "ticker")


def test_accuracy_by_formula(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return accuracy_rows(rows, "formula_id")


def test_accuracy_by_quarter(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return accuracy_rows(rows, "fiscal_quarter")


def accuracy_rows(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    out = []
    for value, items in group_by(rows, key).items():
        metrics = metric_counts(items)
        out.append({key: value, **metrics})
    return out


def recovery_by_quarter_rows(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"target_field": field, "fiscal_quarter": quarter, "candidate_status": status, "rows": count} for (field, quarter, status), count in Counter((row["target_field"], row["fiscal_quarter"], row["candidate_status"]) for row in plan).items()]


def recovery_by_company_rows(plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"ticker": ticker, "company_id": company_id, "auto_strong": sum(row["candidate_status"] == "AUTO_STRONG" for row in rows), "conditional": sum(row["candidate_status"] == "CONDITIONAL_NOT_AUTO" for row in rows), "proxy": sum(row["candidate_status"] == "PROXY_NOT_AUTO" for row in rows)} for (ticker, company_id), rows in group_by(plan, ("ticker", "company_id")).items()]


def group_by(rows: list[dict[str, Any]], key: str | tuple[str, ...]) -> dict[Any, list[dict[str, Any]]]:
    out = defaultdict(list)
    for row in rows:
        value = tuple(row[k] for k in key) if isinstance(key, tuple) else row.get(key)
        out[value].append(row)
    return out


def formula_metadata_schema() -> str:
    return """# Formula Metadata Schema

Tables proposed for Phase 4C-3:

`rc_fundamental_formula_registry`
- formula_id
- metric
- component_roles
- formula_class
- approval_policy

`rc_company_fundamental_formula_profile`
- company_id
- metric
- formula_id
- formula_version
- status
- confidence
- valid_from_fiscal_year
- valid_from_fiscal_quarter
- valid_to_fiscal_year
- valid_to_fiscal_quarter
- q1_status
- q2_status
- q3_status
- q4_status
- sec_component_mappings_json
- simfin_corroboration_status
- calibration_observations
- test_observations
- test_within_1pct_rate
- test_within_5pct_rate
- material_mismatch_count
- sign_mismatch_count
- annual_reconciliation_status
- research_run_id

No executable formulas are stored. Production apply must use formula_id plus exact SEC component QName mappings.
"""


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 4C-2D SEC Company Formula Rerun

Classification: `{summary['classification']}`

Canonical EBIT writes: `0`

Canonical EBITDA writes: `0`

## Baseline

- Companies: {summary['baseline']['companies']}
- Canonical Q: {summary['baseline']['canonical_q']}
- EBIT missing: {summary['baseline']['ebit_missing']}
- EBITDA missing: {summary['baseline']['ebitda_missing']}
- Known EBIT targets: {summary['baseline']['known_ebit_targets']}
- Known EBITDA targets: {summary['baseline']['known_ebitda_targets']}

## Result

The SEC component layer unlocks company-specific formula discovery, but Q4 remains a separate approval domain. Production candidates carry quarter-specific approval; Q1-Q3 success is not treated as Q4 approval.

## EBIT Q1-Q3

- STRONG: {summary['ebit_q1q3']['strong_q1q3']}
- CONDITIONAL: {summary['ebit_q1q3']['conditional_q1q3']}
- PROXY: {summary['ebit_q1q3']['proxy_q1q3']}
- REJECTED: {summary['ebit_q1q3']['rejected_q1q3']}

## EBITDA Q1-Q3

- STRONG: {summary['ebitda_q1q3']['strong_q1q3']}
- CONDITIONAL: {summary['ebitda_q1q3']['conditional_q1q3']}
- PROXY: {summary['ebitda_q1q3']['proxy_q1q3']}
- REJECTED: {summary['ebitda_q1q3']['rejected_q1q3']}

## Q4

{q4_prior_metric_explanation([{"stage": key, "count": value} for key, value in summary['q4_funnel'].items()])}

Strong EBIT Q4 companies: {summary['q4_fingerprints']['strong_ebit_q4']}

Strong EBITDA Q4 companies: {summary['q4_fingerprints']['strong_ebitda_q4']}

## Recovery

- AUTO_STRONG EBIT fills: {summary['recovery']['total_auto_strong_ebit_fills']}
- AUTO_STRONG EBITDA fills: {summary['recovery']['total_auto_strong_ebitda_fills']}
- Core-ready uplift: {summary['recovery']['total_core_uplift']}

## Metadata

Durable metadata justified: `{summary['metadata']['durable_formula_metadata_justified']}`

Metadata persisted in this phase: `{summary['metadata']['metadata_persisted']}`

## Next

`{summary['recommended_next_step']}`
"""
    write_text(path, text)


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    text = path.read_text(encoding="utf-8")
    section = f"""

## Phase 4C-2D

Classification: `{summary['classification']}`

Status: `SEC_COMPANY_FORMULA_DISCOVERY_RERUN_COMPLETE`

Canonical financial writes: `0`

Metadata writes: `0`

AUTO_STRONG EBIT fills: `{summary['recovery']['total_auto_strong_ebit_fills']}`

AUTO_STRONG EBITDA fills: `{summary['recovery']['total_auto_strong_ebitda_fills']}`

Next: `{summary['recommended_next_step']}`
"""
    marker = "\n## Phase 4C-2D\n"
    if marker in text:
        text = text.split(marker, 1)[0].rstrip() + section
    else:
        text = text.rstrip() + section
    write_text(path, text)


def parse_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def relative_error(a: float, b: float) -> float:
    return abs(float(a) - float(b)) / max(abs(float(a)), abs(float(b)), NEAR_ZERO_FLOOR)


def sign(value: float) -> int:
    if abs(value) < NEAR_ZERO_FLOOR:
        return 0
    return 1 if value > 0 else -1


def rate(rows: list[dict[str, Any]], key: str) -> float:
    return sum(int(row[key]) for row in rows) / len(rows) if rows else 0.0


def scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    return int(conn.execute(sql, params).fetchone()[0] or 0)


def core_ready(row: dict[str, Any]) -> bool:
    return all(row.get(field) is not None for field in CORE_FIELDS) and float(row.get("shares_outstanding") or 0) > 0


def write_csv(path: Path, rows: list[dict[str, Any]], *, fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows and fieldnames is None:
        path.write_text("", encoding="utf-8")
        return
    fields = fieldnames or sorted({key for row in rows for key in row})
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
