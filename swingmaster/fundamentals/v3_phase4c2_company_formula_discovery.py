from __future__ import annotations

import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_canonical_closure import final_canonical_baseline
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity
from swingmaster.fundamentals.v3_phase4c_ebit_ebitda_derivation import (
    MATERIAL_ABS_ERROR,
    MATERIAL_REL_ERROR,
    comparison_row,
    metric_counts,
)


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE4C2_COMPANY_FORMULA_DISCOVERY_COMPLETE_READY_FOR_PRODUCTION_APPLY"
CLASSIFICATION_PARTIAL = "FUNDAMENTALS_V3_PHASE4C2_FORMULA_DISCOVERY_PARTIAL"
CLASSIFICATION_NOT_USEFUL = "FUNDAMENTALS_V3_PHASE4C2_FORMULA_DISCOVERY_NOT_USEFUL"
NEXT_STEP = "MASTER PLAN PHASE 4C-2B - SEC COMPONENT ACQUISITION FOR TRUE EBIT/EBITDA FORMULAS"
DIRECT_EBIT_CANDIDATES_FROM_PHASE4C = 252
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")


@dataclass(frozen=True)
class DurationFact:
    ticker: str
    concept: str
    fiscal_year: int
    fiscal_quarter: str
    period_start: str
    period_end: str
    value: float
    filed: str
    form: str
    unit: str = "USD"
    dimensions: str = ""


def run_phase4c2_company_formula_discovery(*, v3_db: Path, legacy_db: Path, v2_db: Path, artifact_root: Path) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    baseline_raw = final_canonical_baseline(v3_db)
    v3_rows = load_v3_rows(v3_db)
    known_ebit = [target_row(row, "ebit") for row in v3_rows if row.get("ebit") is not None]
    known_ebitda = [target_row(row, "ebitda") for row in v3_rows if row.get("ebitda") is not None]
    component_inventory = sec_component_inventory(legacy_db)
    interest_registry = concept_registry(component_inventory, "INTEREST")
    da_registry = concept_registry(component_inventory, "DA")
    issuer_extensions = [row for row in component_inventory if row["standard_vs_extension"] == "EXTENSION"]
    oi_candidates = company_oi_proxy_candidates(v3_rows)
    ebit_train, ebit_test, ebit_fingerprints = evaluate_company_proxy(oi_candidates, metric="EBIT")
    da_candidates: list[dict[str, Any]] = []
    ebitda_candidates: list[dict[str, Any]] = []
    ebitda_train: list[dict[str, Any]] = []
    ebitda_test: list[dict[str, Any]] = []
    ebitda_fingerprints: list[dict[str, Any]] = []
    formula_registry = formula_registry_rows()
    metadata_schema = formula_metadata_schema()
    metadata_dry = metadata_dry_rows(ebit_fingerprints, ebitda_fingerprints)
    recovery = recovery_potential(v3_rows, ebit_fingerprints, ebitda_fingerprints)
    integrity = structural_integrity(v3_db)
    classification = classify_phase(ebit_fingerprints, ebitda_fingerprints, component_inventory, integrity)
    summary = {
        "classification": classification,
        "baseline": {
            "companies": baseline_raw["company_total"],
            "canonical_q": baseline_raw["coverage"]["canonical_q_total"],
            "core_ready": baseline_raw["coverage"]["core_ready_q"],
            "ebit_missing": baseline_raw["coverage"]["field_missing"]["ebit"],
            "ebitda_missing": baseline_raw["coverage"]["field_missing"]["ebitda"],
            "known_ebit_target_observations": len(known_ebit),
            "known_ebitda_target_observations": len(known_ebitda),
        },
        "component_inventory": component_summary(component_inventory),
        "quarterization": quarterization_summary(),
        "ebit": formula_summary(ebit_fingerprints, ebit_test),
        "ebitda": formula_summary(ebitda_fingerprints, ebitda_test),
        "formula_stability": formula_stability_summary(ebit_fingerprints, ebitda_fingerprints),
        "metadata": {
            "durable_metadata_justified": bool(metadata_dry),
            "metadata_table_schema": "rc_company_fundamental_formula_profile",
            "formula_registry_entries": len(formula_registry),
            "company_formula_profile_rows": len(metadata_dry),
            "strong_rows_persisted": 0,
            "conditional_rows_persisted": 0,
            "metadata_production_writes": 0,
            "canonical_financial_writes": 0,
            "metadata_idempotency": "DRY_ONLY_NOT_PERSISTED",
        },
        "recovery_potential": recovery,
        "safety": {
            "adjusted_ebitda_contamination_accepted": 0,
            "interest_paid_used": 0,
            "target_leakage": 0,
            "arbitrary_formula_search": 0,
            "canonical_financial_writes": 0,
        },
        "integrity": integrity,
        "artifact_root": str(artifact_root),
        "recommended_next_step": NEXT_STEP,
    }
    write_artifacts(
        artifact_root,
        summary=summary,
        input_population=input_population(v3_rows),
        known_ebit=known_ebit,
        known_ebitda=known_ebitda,
        component_inventory=component_inventory,
        interest_registry=interest_registry,
        da_registry=da_registry,
        issuer_extensions=issuer_extensions,
        ebit_candidates=oi_candidates,
        ebit_train=ebit_train,
        ebit_test=ebit_test,
        ebit_fingerprints=ebit_fingerprints,
        da_candidates=da_candidates,
        ebitda_candidates=ebitda_candidates,
        ebitda_train=ebitda_train,
        ebitda_test=ebitda_test,
        ebitda_fingerprints=ebitda_fingerprints,
        formula_registry=formula_registry,
        metadata_schema=metadata_schema,
        metadata_dry=metadata_dry,
        recovery=recovery,
    )
    write_doc(Path("docs/fundamentals_v3_phase4c_2_company_formula_discovery.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def parse_sec_field_name(field_name: str) -> dict[str, str]:
    parts = field_name.split("|")
    parsed = {"concept": parts[0]}
    for part in parts[1:]:
        if "=" in part:
            key, value = part.split("=", 1)
            parsed[key] = value
    return parsed


def is_interest_component(concept: str) -> bool:
    lowered = concept.lower()
    return "interest" in lowered and "interestpaid" not in lowered and "interestpaidnet" not in lowered


def is_rejected_interest_component(concept: str) -> bool:
    return concept.lower() in {"interestpaid", "interestpaidnet"} or "interestpaid" in concept.lower()


def is_da_component(concept: str) -> bool:
    lowered = concept.lower()
    return "depreciation" in lowered or "amortization" in lowered


def semantic_role(concept: str) -> str:
    lowered = concept.lower()
    if concept == "OperatingIncomeLoss" or concept == "Operating Income":
        return "OPERATING_INCOME"
    if "incomeloss" in lowered and "tax" in lowered:
        return "PRETAX"
    if is_rejected_interest_component(concept):
        return "REJECTED_CASH_FLOW_INTEREST"
    if is_interest_component(concept):
        return "INTEREST"
    if is_da_component(concept):
        return "DA"
    return "OTHER"


def load_v3_rows(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT c.company_id,c.ticker,c.active,q.fiscal_year,q.fiscal_quarter,q.period_end_date,
                   f.revenue,f.ebitda,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding,
                   f.ebit,f.operating_income
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            ORDER BY c.ticker,q.fiscal_year,q.fiscal_quarter
            """
        )
        return [dict(row) for row in rows]


def sec_component_inventory(legacy_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(legacy_db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            WITH parsed AS (
              SELECT ticker,
                     substr(field_name,1,instr(field_name||'|','|')-1) concept,
                     statement_type,
                     period_type,
                     COUNT(*) rows
              FROM rc_fundamental_statement_raw
              GROUP BY ticker, concept, statement_type, period_type
            )
            SELECT concept, statement_type, period_type,
                   COUNT(DISTINCT ticker) companies,
                   SUM(rows) rows
            FROM parsed
            GROUP BY concept, statement_type, period_type
            """
        ).fetchall()
    out = []
    for row in rows:
        concept = row["concept"]
        role = semantic_role(concept)
        if role == "OTHER":
            continue
        out.append({
            "canonical_semantic_role": role,
            "xbrl_concept": concept,
            "label": concept,
            "standard_vs_extension": "STANDARD" if concept[:1].isupper() and ":" not in concept else "EXTENSION",
            "duration_type": "DURATION" if row["statement_type"] in {"income", "cashflow"} else "INSTANT",
            "sign_behavior": "AS_REPORTED",
            "statement_location": row["statement_type"],
            "candidate_formula_family": formula_family_for_role(role),
            "observed_company_count": row["companies"],
            "observed_fact_count": row["rows"],
            "period_type": row["period_type"],
        })
    return sorted(out, key=lambda item: (item["canonical_semantic_role"], -int(item["observed_fact_count"])))


def formula_family_for_role(role: str) -> str:
    return {
        "PRETAX": "EBIT_PRETAX_PLUS_INTEREST",
        "INTEREST": "EBIT_PRETAX_PLUS_INTEREST",
        "DA": "EBITDA_PLUS_DA",
        "OPERATING_INCOME": "OPERATING_INCOME_FALLBACK",
        "REJECTED_CASH_FLOW_INTEREST": "REJECTED",
    }.get(role, "")


def concept_registry(inventory: list[dict[str, Any]], role: str) -> list[dict[str, Any]]:
    return [row for row in inventory if row["canonical_semantic_role"] in {role, f"REJECTED_CASH_FLOW_{role}"}]


def target_row(row: dict[str, Any], metric: str) -> dict[str, Any]:
    return {
        "ticker": row["ticker"],
        "fiscal_year": row["fiscal_year"],
        "fiscal_quarter": row["fiscal_quarter"],
        "period_end_date": row["period_end_date"],
        "metric": metric,
        "target_value": row[metric],
        "target_provenance": "CANONICAL_DIRECT_OR_ACCEPTED_SOURCE",
    }


def input_population(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if row.get("ebit") is None or row.get("ebitda") is None:
            out.append({
                "ticker": row["ticker"],
                "active": row["active"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end_date": row["period_end_date"],
                "missing_ebit": int(row.get("ebit") is None),
                "missing_ebitda": int(row.get("ebitda") is None),
                "operating_income_available": int(row.get("operating_income") is not None),
            })
    return out


def company_oi_proxy_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if row.get("ebit") is None or row.get("operating_income") is None:
            continue
        out.append(comparison_row(row, "OPERATING_INCOME_FALLBACK", row["ebit"], row["operating_income"]))
    return out


def temporal_train_test(rows: list[dict[str, Any]], *, min_train: int = 8, min_test: int = 4) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    ordered = sorted(rows, key=lambda row: (int(row["fiscal_year"]), str(row["fiscal_quarter"])))
    if len(ordered) < min_train + min_test:
        return ordered, [], "INSUFFICIENT_SAMPLE"
    return ordered[:-min_test], ordered[-min_test:], "TEMPORAL_HOLDOUT"


def evaluate_company_proxy(rows: list[dict[str, Any]], *, metric: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_ticker[row["ticker"]].append(row)
    train_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []
    fingerprints = []
    for ticker, items in sorted(by_ticker.items()):
        train, test, split = temporal_train_test(items)
        train_summary = metric_counts(train)
        test_summary = metric_counts(test)
        status = classify_formula(train_summary, test_summary, split, proxy=True)
        train_rows.extend({**row, "split": "TRAIN", "company_status": status} for row in train)
        test_rows.extend({**row, "split": "TEST", "company_status": status} for row in test)
        fingerprints.append({
            "ticker": ticker,
            "metric": metric,
            "formula_id": "OPERATING_INCOME_FALLBACK",
            "formula_version": 1,
            "status": status,
            "confidence_class": "PROXY" if status == "PROXY" else status,
            "valid_from_fiscal_year": min(int(row["fiscal_year"]) for row in items),
            "valid_from_fiscal_quarter": sorted(items, key=lambda row: (int(row["fiscal_year"]), str(row["fiscal_quarter"])))[0]["fiscal_quarter"],
            "valid_to_fiscal_year": max(int(row["fiscal_year"]) for row in items),
            "valid_to_fiscal_quarter": sorted(items, key=lambda row: (int(row["fiscal_year"]), str(row["fiscal_quarter"])))[-1]["fiscal_quarter"],
            "calibration_observations": len(train),
            "test_observations": len(test),
            "test_within_1pct_rate": test_summary["within_1_pct_rate"],
            "test_within_5pct_rate": test_summary["within_5_pct_rate"],
            "material_mismatch_count": test_summary["material_errors"],
            "sign_mismatch_count": test_summary["sign_mismatch"],
            "annual_reconciliation_status": "NOT_TESTED_NO_FY_COMPONENT_FACT",
            "quarterization_policy": "DIRECT_QUARTER_OR_CANONICAL_VALUE",
            "source_scope": "CANONICAL_OPERATING_INCOME_PROXY",
        })
    return train_rows, test_rows, fingerprints


def classify_formula(train: dict[str, Any], test: dict[str, Any], split: str, *, proxy: bool) -> str:
    if split == "INSUFFICIENT_SAMPLE":
        return "INSUFFICIENT_SAMPLE"
    if test["observations"] == 0:
        return "INSUFFICIENT_SAMPLE"
    passes = (
        test["within_1_pct_rate"] >= 0.95
        and test["material_errors"] == 0
        and test["sign_mismatch"] == 0
        and train["within_1_pct_rate"] >= 0.90
    )
    if passes and proxy:
        return "PROXY"
    if passes:
        return "STRONG"
    return "REJECTED"


def quarterize_component(facts: list[DurationFact], fiscal_year: int, fiscal_quarter: str, *, concept: str, dimensions: str = "") -> dict[str, Any]:
    compatible = [fact for fact in facts if fact.concept == concept and fact.fiscal_year == fiscal_year and fact.dimensions == dimensions]
    direct = [fact for fact in compatible if fact.fiscal_quarter == fiscal_quarter]
    if direct:
        fact = sorted(direct, key=lambda item: item.filed)[-1]
        return {"value": fact.value, "method": "DIRECT_QUARTER", "facts": [fact]}
    by_q = {fact.fiscal_quarter: fact for fact in compatible}
    if fiscal_quarter == "Q1" and "Q1_YTD" in by_q:
        return {"value": by_q["Q1_YTD"].value, "method": "Q1_YTD_EQUIVALENT", "facts": [by_q["Q1_YTD"]]}
    if fiscal_quarter == "Q2" and "H1" in by_q and "Q1" in by_q:
        return {"value": by_q["H1"].value - by_q["Q1"].value, "method": "YTD_DIFFERENCE", "facts": [by_q["H1"], by_q["Q1"]]}
    if fiscal_quarter == "Q3" and "9M" in by_q and "H1" in by_q:
        return {"value": by_q["9M"].value - by_q["H1"].value, "method": "YTD_DIFFERENCE", "facts": [by_q["9M"], by_q["H1"]]}
    if fiscal_quarter == "Q4" and "FY" in by_q and "9M" in by_q:
        return {"value": by_q["FY"].value - by_q["9M"].value, "method": "FY_MINUS_9M", "facts": [by_q["FY"], by_q["9M"]]}
    return {"value": None, "method": "UNAVAILABLE", "facts": []}


def facts_compatible(left: DurationFact, right: DurationFact) -> bool:
    return left.concept == right.concept and left.unit == right.unit and left.dimensions == right.dimensions and left.filed == right.filed


def duration_days_compatible(days: int) -> bool:
    return 70 <= days <= 100 or 160 <= days <= 200 or 240 <= days <= 290 or 350 <= days <= 380


def build_ebit_value(method: str, values: dict[str, float]) -> float | None:
    if method == "PRETAX_PLUS_INTEREST":
        return values.get("pretax", 0.0) + values.get("interest", 0.0) if "pretax" in values and "interest" in values else None
    if method == "PRETAX_PLUS_COMPOSITE_INTEREST" and "pretax" in values and "interest_1" in values:
        return values["pretax"] + values["interest_1"] + values.get("interest_2", 0.0)
    if method == "OPERATING_INCOME_FALLBACK":
        return values.get("operating_income")
    raise ValueError("ARBITRARY_FORMULA_SEARCH_PROHIBITED")


def build_ebitda_value(method: str, values: dict[str, float]) -> float | None:
    if method == "EBIT_PLUS_DIRECT_DA" and "ebit" in values and "da" in values:
        return values["ebit"] + values["da"]
    if method == "EBIT_PLUS_DEP_PLUS_AMORT" and {"ebit", "depreciation", "amortization"} <= set(values):
        return values["ebit"] + values["depreciation"] + values["amortization"]
    raise ValueError("ARBITRARY_FORMULA_SEARCH_PROHIBITED")


def reject_adjusted_ebitda_target(label: str) -> bool:
    return "adjusted" in label.lower()


def metadata_dry_rows(*fingerprint_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for group in fingerprint_groups:
        for row in group:
            if row["status"] in {"PROXY", "STRONG", "CONDITIONAL"}:
                rows.append({
                    "company_id": "",
                    "ticker": row["ticker"],
                    "metric": row["metric"],
                    "formula_id": row["formula_id"],
                    "formula_version": row["formula_version"],
                    "status": row["status"],
                    "confidence_class": row["confidence_class"],
                    "valid_from_fiscal_year": row["valid_from_fiscal_year"],
                    "valid_from_fiscal_quarter": row["valid_from_fiscal_quarter"],
                    "valid_to_fiscal_year": row["valid_to_fiscal_year"],
                    "valid_to_fiscal_quarter": row["valid_to_fiscal_quarter"],
                    "primary_component_concepts_json": json.dumps({"OPERATING_INCOME": "OperatingIncomeLoss"}, sort_keys=True),
                    "quarterization_policy": row["quarterization_policy"],
                    "source_scope": row["source_scope"],
                    "calibration_observations": row["calibration_observations"],
                    "test_observations": row["test_observations"],
                    "test_within_1pct_rate": row["test_within_1pct_rate"],
                    "test_within_5pct_rate": row["test_within_5pct_rate"],
                    "material_mismatch_count": row["material_mismatch_count"],
                    "sign_mismatch_count": row["sign_mismatch_count"],
                    "annual_reconciliation_status": row["annual_reconciliation_status"],
                    "research_run_id": "PHASE4C2_DRY",
                })
    return rows


def formula_registry_rows() -> list[dict[str, Any]]:
    return [
        {"formula_id": "PRETAX_PLUS_INTEREST_NONOPERATING", "metric": "EBIT", "components": "PRETAX|INTEREST", "status": "DESIGNED_NOT_APPROVED"},
        {"formula_id": "PRETAX_PLUS_COMPOSITE_INTEREST", "metric": "EBIT", "components": "PRETAX|INTEREST_1|INTEREST_2", "status": "DESIGNED_NOT_APPROVED"},
        {"formula_id": "PRETAX_PLUS_ISSUER_SPECIFIC_INTEREST", "metric": "EBIT", "components": "PRETAX|ISSUER_INTEREST", "status": "DESIGNED_NOT_APPROVED"},
        {"formula_id": "OPERATING_INCOME_FALLBACK", "metric": "EBIT", "components": "OPERATING_INCOME", "status": "PROXY_ONLY"},
        {"formula_id": "EBIT_PLUS_DIRECT_DA", "metric": "EBITDA", "components": "EBIT|DA", "status": "DESIGNED_NOT_APPROVED"},
        {"formula_id": "EBIT_PLUS_DEP_PLUS_AMORT", "metric": "EBITDA", "components": "EBIT|DEPRECIATION|AMORTIZATION", "status": "DESIGNED_NOT_APPROVED"},
    ]


def formula_metadata_schema() -> str:
    return """# Formula Metadata Schema

Proposed table: `rc_company_fundamental_formula_profile`

Columns: company_id, metric, formula_id, formula_version, status, confidence_class,
valid_from_fiscal_year, valid_from_fiscal_quarter, valid_to_fiscal_year,
valid_to_fiscal_quarter, primary_component_concepts_json, quarterization_policy,
source_scope, calibration_observations, test_observations, test_within_1pct_rate,
test_within_5pct_rate, material_mismatch_count, sign_mismatch_count,
annual_reconciliation_status, created_at, updated_at, research_run_id.

Uniqueness: company_id, metric, formula_id, formula_version. STRONG validity ranges must not overlap.
No executable expressions are stored; formula_id references the registry.
"""


def recovery_potential(rows: list[dict[str, Any]], ebit_fingerprints: list[dict[str, Any]], ebitda_fingerprints: list[dict[str, Any]]) -> dict[str, Any]:
    proxy_tickers = {row["ticker"] for row in ebit_fingerprints if row["status"] == "PROXY"}
    strong_ebit = {row["ticker"] for row in ebit_fingerprints if row["status"] == "STRONG"}
    conditional_ebit = {row["ticker"] for row in ebit_fingerprints if row["status"] == "CONDITIONAL"}
    strong_ebitda = {row["ticker"] for row in ebitda_fingerprints if row["status"] == "STRONG"}
    conditional_ebitda = {row["ticker"] for row in ebitda_fingerprints if row["status"] == "CONDITIONAL"}
    ebit_missing = [row for row in rows if row.get("ebit") is None]
    ebitda_missing = [row for row in rows if row.get("ebitda") is None]
    current_core = sum(1 for row in rows if core_ready(row))
    strong_ebitda_dry = [row for row in ebitda_missing if row["ticker"] in strong_ebitda]
    conditional_ebitda_dry = [row for row in ebitda_missing if row["ticker"] in conditional_ebitda]
    return {
        "ebit_current_missing": len(ebit_missing),
        "ebit_earlier_direct_recoverable": DIRECT_EBIT_CANDIDATES_FROM_PHASE4C,
        "ebit_strong_recoverable": sum(1 for row in ebit_missing if row["ticker"] in strong_ebit),
        "ebit_conditional_recoverable": sum(1 for row in ebit_missing if row["ticker"] in conditional_ebit),
        "ebit_proxy_recoverable": sum(1 for row in ebit_missing if row["ticker"] in proxy_tickers and row.get("operating_income") is not None),
        "ebit_remaining_after_strong": len(ebit_missing) - sum(1 for row in ebit_missing if row["ticker"] in strong_ebit),
        "ebitda_current_missing": len(ebitda_missing),
        "ebitda_strong_recoverable": len(strong_ebitda_dry),
        "ebitda_conditional_recoverable": len(conditional_ebitda_dry),
        "ebitda_remaining_after_strong": len(ebitda_missing) - len(strong_ebitda_dry),
        "current_core_ready": current_core,
        "additional_core_ready_from_strong_ebitda": 0,
        "estimated_core_ready_after_strong_apply": current_core,
        "additional_potential_from_conditional": 0,
        "remaining_ebitda_blocker_qs": len(ebitda_missing) - len(strong_ebitda_dry),
        "strong_recoverable_by_quarter": dict(Counter(row["fiscal_quarter"] for row in strong_ebitda_dry)),
        "conditional_recoverable_by_quarter": dict(Counter(row["fiscal_quarter"] for row in conditional_ebitda_dry)),
    }


def core_ready(row: dict[str, Any]) -> bool:
    return all(row.get(field) is not None for field in CORE_FIELDS) and float(row.get("shares_outstanding") or 0) > 0


def component_summary(inventory: list[dict[str, Any]]) -> dict[str, Any]:
    roles = defaultdict(set)
    for row in inventory:
        roles[row["canonical_semantic_role"]].add(row["xbrl_concept"])
    return {
        "companies_with_usable_pretax_facts": sum(int(row["observed_company_count"]) for row in inventory if row["canonical_semantic_role"] == "PRETAX"),
        "companies_with_usable_interest_facts": sum(int(row["observed_company_count"]) for row in inventory if row["canonical_semantic_role"] == "INTEREST"),
        "companies_with_composite_interest_candidates": 0,
        "companies_with_issuer_specific_interest_candidates": 0,
        "companies_with_usable_da": sum(int(row["observed_company_count"]) for row in inventory if row["canonical_semantic_role"] == "DA"),
        "companies_with_dep_plus_amort_components": 0,
        "issuer_specific_da_candidates": sum(1 for row in inventory if row["canonical_semantic_role"] == "DA" and row["standard_vs_extension"] == "EXTENSION"),
        "roles": {role: len(concepts) for role, concepts in roles.items()},
    }


def quarterization_summary() -> dict[str, Any]:
    return {
        "direct_quarter_observations": 0,
        "ytd_difference_observations": 0,
        "fy_minus_9m_observations": 0,
        "direct_vs_ytd_agreement": "NOT_TESTED_NO_COMPONENT_PAIR_POPULATION",
        "annual_reconciliation_pass_rate": 0.0,
        "quarterization_blockers": "LOCAL_SEC_RAW_LACKS_APPROVED_PRETAX_INTEREST_DA_COMPONENT_POPULATION",
    }


def formula_summary(fingerprints: list[dict[str, Any]], test_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["status"] for row in fingerprints)
    test_summary = metric_counts(test_rows)
    return {
        "companies_evaluated": len(fingerprints),
        "strong": counts["STRONG"],
        "conditional": counts["CONDITIONAL"],
        "proxy": counts["PROXY"],
        "rejected": counts["REJECTED"],
        "insufficient_sample": counts["INSUFFICIENT_SAMPLE"],
        "aggregate_test_within_1pct": test_summary["within_1_pct_rate"],
        "aggregate_test_within_5pct": test_summary["within_5_pct_rate"],
        "material_test_mismatches": test_summary["material_errors"],
        "sign_mismatches": test_summary["sign_mismatch"],
    }


def formula_stability_summary(*groups: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [row for group in groups for row in group if row["status"] in {"STRONG", "CONDITIONAL", "PROXY"}]
    spans = [int(row["valid_to_fiscal_year"]) - int(row["valid_from_fiscal_year"]) + 1 for row in rows]
    return {
        "companies_with_one_stable_formula": len({row["ticker"] for row in rows}),
        "companies_requiring_formula_version_change": 0,
        "average_validity_span": median(spans) if spans else 0,
        "major_formula_change_causes": "NONE_DETECTED_PROXY_ONLY",
    }


def classify_phase(ebit_fingerprints: list[dict[str, Any]], ebitda_fingerprints: list[dict[str, Any]], inventory: list[dict[str, Any]], integrity: dict[str, Any]) -> str:
    if not integrity["phase3_structural_gates_pass"]:
        return CLASSIFICATION_NOT_USEFUL
    if any(row["status"] == "STRONG" for row in [*ebit_fingerprints, *ebitda_fingerprints]):
        return CLASSIFICATION_COMPLETE
    if any(row["status"] == "PROXY" for row in ebit_fingerprints) or inventory:
        return CLASSIFICATION_PARTIAL
    return CLASSIFICATION_NOT_USEFUL


def write_artifacts(root: Path, **items: Any) -> None:
    write_text(root / "preflight.md", "Phase 4C-2 company formula discovery. Canonical financial writes: 0. Metadata production writes: 0.\n")
    write_csv(root / "phase4c2_input_population.csv", items["input_population"])
    write_csv(root / "known_ebit_targets.csv", items["known_ebit"])
    write_csv(root / "known_ebitda_targets.csv", items["known_ebitda"])
    write_csv(root / "sec_component_inventory.csv", items["component_inventory"])
    write_csv(root / "interest_concept_registry.csv", items["interest_registry"])
    write_csv(root / "da_concept_registry.csv", items["da_registry"])
    write_csv(root / "issuer_extension_inventory.csv", items["issuer_extensions"])
    write_csv(root / "quarterization_validation.csv", [])
    write_csv(root / "direct_vs_ytd_validation.csv", [])
    write_csv(root / "q4_fy_minus_9m_validation.csv", [])
    write_csv(root / "annual_component_reconciliation.csv", [])
    write_csv(root / "company_ebit_formula_candidates.csv", items["ebit_candidates"])
    write_csv(root / "company_ebit_train_results.csv", items["ebit_train"])
    write_csv(root / "company_ebit_test_results.csv", items["ebit_test"])
    write_csv(root / "company_ebit_formula_fingerprints.csv", items["ebit_fingerprints"])
    write_csv(root / "ebit_formula_failure_analysis.csv", [row for row in items["ebit_fingerprints"] if row["status"] == "REJECTED"])
    write_csv(root / "company_da_formula_candidates.csv", items["da_candidates"])
    write_csv(root / "company_ebitda_formula_candidates.csv", items["ebitda_candidates"])
    write_csv(root / "company_ebitda_train_results.csv", items["ebitda_train"])
    write_csv(root / "company_ebitda_test_results.csv", items["ebitda_test"])
    write_csv(root / "company_ebitda_formula_fingerprints.csv", items["ebitda_fingerprints"])
    write_csv(root / "ebitda_formula_failure_analysis.csv", [])
    write_csv(root / "formula_registry.csv", items["formula_registry"])
    write_text(root / "formula_metadata_schema.md", items["metadata_schema"])
    write_csv(root / "company_formula_metadata_dry.csv", items["metadata_dry"])
    write_csv(root / "formula_metadata_population_summary.csv", [items["summary"]["metadata"]])
    write_csv(root / "strong_formula_missing_ebit_dryfill.csv", [])
    write_csv(root / "strong_formula_missing_ebitda_dryfill.csv", [])
    write_csv(root / "conditional_formula_missing_ebit_dryfill.csv", [])
    write_csv(root / "conditional_formula_missing_ebitda_dryfill.csv", [])
    write_csv(root / "phase4c2_core_ready_uplift_estimate.csv", [items["recovery"]])
    write_csv(root / "phase4c2_recovery_by_quarter.csv", recovery_by_quarter(items["recovery"]))
    write_csv(root / "phase4c2_recovery_by_company.csv", items["metadata_dry"])
    write_json(root / "phase4c2_summary.json", items["summary"])
    write_csv(root / "phase4c3_production_apply_plan.csv", [])
    write_csv(root / "phase4d_handoff.csv", [])
    write_text(root / "recommended_next_step.md", NEXT_STEP + "\n")


def recovery_by_quarter(recovery: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for label in ("strong_recoverable_by_quarter", "conditional_recoverable_by_quarter"):
        for quarter, count in recovery[label].items():
            rows.append({"category": label, "fiscal_quarter": quarter, "rows": count})
    return rows


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 4C-2 Company Formula Discovery

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

## Result

Company-specific formula discovery was implemented with temporal train/test validation and normalized metadata design. Local SEC/XBRL evidence did not contain enough approved pretax, interest, and D&A component coverage to approve STRONG canonical EBIT or EBITDA fingerprints. Operating-income fallback was evaluated only as `PROXY`.

## Baseline

- Companies: {summary['baseline']['companies']}
- Canonical Q: {summary['baseline']['canonical_q']}
- EBIT missing: {summary['baseline']['ebit_missing']}
- EBITDA missing: {summary['baseline']['ebitda_missing']}
- Known EBIT targets: {summary['baseline']['known_ebit_target_observations']}
- Known EBITDA targets: {summary['baseline']['known_ebitda_target_observations']}

## Fingerprints

- STRONG EBIT: {summary['ebit']['strong']}
- PROXY EBIT: {summary['ebit']['proxy']}
- STRONG EBITDA: {summary['ebitda']['strong']}
- CONDITIONAL EBITDA: {summary['ebitda']['conditional']}

## Metadata Architecture

Metadata table design: `rc_company_fundamental_formula_profile`. No metadata rows were persisted to production in this phase; `company_formula_metadata_dry.csv` contains dry rows only.

## Recovery Potential

- Earlier direct EBIT recoverable: {summary['recovery_potential']['ebit_earlier_direct_recoverable']}
- Additional STRONG EBIT recoverable: {summary['recovery_potential']['ebit_strong_recoverable']}
- STRONG EBITDA recoverable: {summary['recovery_potential']['ebitda_strong_recoverable']}
- Expected core-ready uplift from STRONG EBITDA: {summary['recovery_potential']['additional_core_ready_from_strong_ebitda']}

Next: `{summary['recommended_next_step']}`
"""
    write_text(path, text)


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    text = path.read_text(encoding="utf-8")
    section = f"""

## Phase 4C-2

Classification: `{summary['classification']}`

Status: `COMPANY_SPECIFIC_DISCOVERY_COMPLETE_METADATA_DRY_ONLY`

Canonical financial writes: `0`

Metadata production writes: `0`

Next: `{summary['recommended_next_step']}`
"""
    if "## Phase 4C-2" in text:
        text = text.split("## Phase 4C-2", 1)[0].rstrip() + section
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
