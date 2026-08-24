from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE6B_SCORE_LIFECYCLE_CALIBRATION_DESIGN_COMPLETE_READY_FOR_PHASE6C"
NEXT_PHASE = "MASTER PLAN PHASE 6C - SCORE DISTRIBUTIONS & POINT CALIBRATION"
CALIBRATION_START = "2021-01-01"
CALIBRATION_END = "2025-12-31"
OOS_START = "2026-01-01"
STRESS_START = "2020-01-01"
STRESS_END = "2020-12-31"
NEAR_ZERO_EPSILON = 1e-9


@dataclass(frozen=True)
class DomainResult:
    state: str
    score_floor: int | None = None


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def full_scale_values(min_score: int, max_score: int) -> list[int]:
    if max_score < min_score:
        raise ValueError("INVALID_SCORE_RANGE")
    return list(range(min_score, max_score + 1))


def validate_full_scale(reachable_values: set[int], *, min_score: int, max_score: int) -> dict[str, Any]:
    expected = set(full_scale_values(min_score, max_score))
    missing = sorted(expected - reachable_values)
    extra = sorted(reachable_values - expected)
    return {
        "valid": not missing and not extra,
        "expected_count": len(expected),
        "reachable_count": len(reachable_values & expected),
        "dead_values": missing,
        "out_of_range_values": extra,
    }


def scalar_scale_contract(component_id: str, max_score: int = 15) -> dict[str, Any]:
    values = full_scale_values(0, max_score)
    return {
        "component_id": component_id,
        "min_score": 0,
        "max_score": max_score,
        "integer_only": 1,
        "reachable_values": "|".join(str(value) for value in values),
        "reachable_score_count": len(values),
        "expected_score_count": max_score + 1,
        "every_integer_reachable": 1,
    }


def classify_positive_only_good(value: float | None) -> DomainResult:
    if value is None:
        return DomainResult("MISSING_DATA")
    if value <= 0:
        return DomainResult("BAD_ECONOMIC_VALUE", 0)
    return DomainResult("POSITIVE_SCORE_DOMAIN")


def classify_multiple_denominator(denominator: float | None) -> DomainResult:
    if denominator is None:
        return DomainResult("MISSING_DATA")
    if abs(float(denominator)) <= NEAR_ZERO_EPSILON:
        return DomainResult("NOT_MEANINGFUL")
    if denominator <= 0:
        return DomainResult("NOT_MEANINGFUL")
    return DomainResult("MEANINGFUL_POSITIVE_DENOMINATOR")


def classify_net_debt(value: float | None) -> DomainResult:
    if value is None:
        return DomainResult("MISSING_DATA")
    if value < 0:
        return DomainResult("NET_CASH_FAVORABLE")
    if value == 0:
        return DomainResult("ZERO_NET_DEBT")
    return DomainResult("NET_DEBT_POSITIVE")


def classify_signed_transition(previous: float | None, current: float | None) -> str:
    if previous is None or current is None:
        return "MISSING_DATA"
    if previous < 0 and current < previous:
        return "NEGATIVE_AND_DETERIORATING"
    if previous < 0 and current < 0 and current >= previous:
        return "NEGATIVE_BUT_IMPROVING"
    if previous <= 0 < current:
        return "CROSSING_TO_POSITIVE"
    if previous > 0 and current > previous:
        return "POSITIVE_AND_GROWING"
    if previous > 0 and current > 0 and current <= previous:
        return "POSITIVE_AND_DECLINING"
    if previous > 0 and current <= 0:
        return "POSITIVE_TURNING_NEGATIVE"
    return "FLAT_ZERO_REGION"


def calibration_windows() -> dict[str, Any]:
    return {
        "calibration": {"start": CALIBRATION_START, "end": CALIBRATION_END, "fit_thresholds": 1},
        "oos": {"start": OOS_START, "end": "YTD", "fit_thresholds": 0},
        "stress": {"start": STRESS_START, "end": STRESS_END, "fit_thresholds": 0},
        "context_only": {"start": "2018-01-01", "end": "2019-12-31", "fit_thresholds": 0},
    }


def existing_score_component_inventory() -> list[dict[str, Any]]:
    return [
        existing_score("growth_component", 15, "0|5|6|9|12|15", "revenue_growth_ttm_yoy", "higher is better", "fixed thresholds", "None -> 6"),
        existing_score("margin_component", 15, "0|4|8|12|15", "ebitda_margin_ttm", "higher is better", "fixed EBITDA thresholds", "None -> 0"),
        existing_score("margin_trend_component", 15, "2|6|10|15", "ebitda_margin_trend_4q", "higher is better", "fixed EBITDA thresholds", "None -> 6"),
        existing_score("fcf_component", 15, "0|4|8|12|15", "fcf_margin_ttm", "higher is better", "fixed thresholds", "None -> 0"),
        existing_score("leverage_component", 15, "0|4|8|12|15", "net_debt_to_ebitda then net_debt_to_ebit", "lower is better", "fixed thresholds", "None -> 8"),
        existing_score("dilution_component", 10, "0|2|5|8|10", "share_dilution_yoy", "lower dilution is better", "fixed thresholds", "None/outlier -> 5"),
        existing_score("lifecycle_component", 5, "-10|-5|0|2|4|5", "lifecycle_class", "state adjustment", "categorical", "unknown -> 0"),
        existing_score("consistency_component", 10, "0|2|4|6|8|10", "CV of revenue growth, EBITDA margin, FCF margin", "lower CV is better", "fixed thresholds", "insufficient history -> 0"),
    ]


def existing_score(name: str, max_score: int, reachable: str, inputs: str, direction: str, thresholds: str, missing: str) -> dict[str, Any]:
    values = {int(value) for value in reachable.split("|")}
    validation = validate_full_scale(values, min_score=0, max_score=max_score)
    return {
        "component_id": name,
        "max_score": max_score,
        "reachable_values": reachable,
        "full_0_to_max_scale_used": int(not validate_full_scale(values, min_score=0, max_score=max_score)["dead_values"]),
        "dead_values": "|".join(str(value) for value in validate_full_scale(values, min_score=0, max_score=max_score)["dead_values"]),
        "scale_status": "SPARSE_SCALE_REQUIRES_RECALIBRATION" if validation["dead_values"] else "OK",
        "inputs": inputs,
        "direction": direction,
        "threshold_structure": thresholds,
        "missing_policy": missing,
        "recommendation": "REVISE_TO_FULL_SCALE" if "SPARSE" in ("SPARSE_SCALE_REQUIRES_RECALIBRATION" if validation["dead_values"] else "OK") else "KEEP",
    }


def existing_lifecycle_inventory() -> list[dict[str, Any]]:
    return [
        {"state": "DISTRESSED", "inputs": "ebitda_margin_ttm,fcf_margin_ttm", "entry_conditions": "EBITDA margin < -30% and FCF margin < -20%", "old_ebitda_dependency": 1, "issue": "EBITDA primary dependency; no hysteresis"},
        {"state": "STARTUP", "inputs": "revenue_growth_ttm_yoy,ebitda_margin_ttm,fcf_margin_ttm", "entry_conditions": "revenue growth >30%, EBITDA margin <0, FCF margin <0", "old_ebitda_dependency": 1, "issue": "state mixes growth and poor profitability without transition persistence"},
        {"state": "GROWTH", "inputs": "revenue_growth_ttm_yoy,ebitda_margin_ttm", "entry_conditions": "revenue growth >20%, EBITDA margin <15%", "old_ebitda_dependency": 1, "issue": "EBITDA threshold; no signed EBIT transition"},
        {"state": "SCALING", "inputs": "revenue_growth_ttm_yoy,ebitda_margin_trend_4q,ebitda_margin_ttm", "entry_conditions": "revenue growth >10%, EBITDA margin trend >0, EBITDA margin >=0", "old_ebitda_dependency": 1, "issue": "EBITDA trend dependency"},
        {"state": "MATURE", "inputs": "revenue_growth_ttm_yoy,ebitda_margin_ttm,fcf_margin_ttm", "entry_conditions": "EBITDA margin >=25%, FCF margin >=5%, revenue growth >=-5% or missing", "old_ebitda_dependency": 1, "issue": "missing revenue can pass maturity gate"},
        {"state": "TRANSITION", "inputs": "revenue_growth_ttm_yoy,ebitda_margin_ttm,ebitda_margin_trend_4q,fcf_margin_ttm", "entry_conditions": "nonnegative EBITDA and FCF with modest growth/trend guard", "old_ebitda_dependency": 1, "issue": "transition state is level-based, not transition-aware"},
        {"state": "DECLINING", "inputs": "revenue_growth_ttm_yoy,ebitda_margin_trend_4q", "entry_conditions": "revenue growth <-5% or EBITDA margin trend <-7%", "old_ebitda_dependency": 1, "issue": "no persistence or churn control"},
        {"state": "UNCLASSIFIED", "inputs": "all lifecycle inputs", "entry_conditions": "fallback", "old_ebitda_dependency": 1, "issue": "fallback not explicitly actionable"},
    ]


def proposed_score_components() -> list[dict[str, Any]]:
    return [
        component("REVENUE_GROWTH", "GROWTH", "ttm_revenue vs prior-year ttm_revenue", "HIGHER_IS_BETTER", "HYBRID", 15, "PRIMARY"),
        component("EBIT_GROWTH_TRANSITION", "GROWTH", "signed transition of ttm_ebit and calibrated positive-domain growth", "SIGNED_TRANSITION_METRIC", "HYBRID", 15, "PRIMARY"),
        component("FCF_GROWTH_TRANSITION", "GROWTH", "signed transition of ttm_fcf and calibrated positive-domain growth", "SIGNED_TRANSITION_METRIC", "HYBRID", 10, "PRIMARY"),
        component("EBIT_MARGIN", "PROFITABILITY_QUALITY", "ttm_ebit / ttm_revenue", "POSITIVE_ONLY_GOOD", "HYBRID", 15, "PRIMARY"),
        component("FCF_MARGIN", "PROFITABILITY_QUALITY", "ttm_fcf / ttm_revenue", "POSITIVE_ONLY_GOOD", "HYBRID", 15, "PRIMARY"),
        component("EV_EBIT", "VALUATION", "enterprise_value / ttm_ebit", "MULTIPLE_REQUIRES_POSITIVE_DENOMINATOR", "HYBRID", 15, "PRIMARY"),
        component("FCF_YIELD", "VALUATION", "ttm_fcf / market_cap", "POSITIVE_ONLY_GOOD", "HYBRID", 15, "PRIMARY"),
        component("EV_SALES", "VALUATION", "enterprise_value / ttm_revenue", "LOWER_IS_BETTER", "HYBRID", 10, "PRIMARY"),
        component("NET_DEBT_POSITION", "BALANCE_SHEET_RISK", "total_debt - cash", "NEGATIVE_CAN_BE_GOOD", "HYBRID", 10, "PRIMARY"),
        component("NET_DEBT_TO_EBIT", "BALANCE_SHEET_RISK", "net_debt / ttm_ebit", "MULTIPLE_REQUIRES_POSITIVE_DENOMINATOR", "HYBRID", 10, "PRIMARY_OPTIONAL"),
    ]


def component(metric_id: str, group: str, formula: str, domain: str, method: str, max_score: int, role: str) -> dict[str, Any]:
    return {
        "metric_id": metric_id,
        "group": group,
        "formula": formula,
        "data_inputs": formula,
        "economic_domain": domain,
        "direction": "LOWER_IS_BETTER" if "EV_" in metric_id or "DEBT_TO" in metric_id else "HIGHER_IS_BETTER",
        "max_score": max_score,
        "calibration_method_candidate": method,
        "missing_policy": "MISSING_DATA; exclude component and require minimum component coverage",
        "denominator_guard": "1e-9 and positive denominator where required",
        "outlier_candidates": "P1/P99 winsorization or piecewise caps to be tested in Phase 6C",
        "redundancy_group": group,
        "role": role,
    }


def economic_domain_policies() -> list[dict[str, Any]]:
    rows = []
    for item in proposed_score_components():
        metric = item["metric_id"]
        zero_boundary = "special" if item["economic_domain"] in {"POSITIVE_ONLY_GOOD", "MULTIPLE_REQUIRES_POSITIVE_DENOMINATOR", "SIGNED_TRANSITION_METRIC"} else "context"
        rows.append({
            "metric_id": metric,
            "economic_domain_policy": item["economic_domain"],
            "better_direction": item["direction"],
            "hard_boundaries": "denominator > 0" if "MULTIPLE" in item["economic_domain"] else "metric <= 0 floors level score" if item["economic_domain"] == "POSITIVE_ONLY_GOOD" else "transition regimes across zero",
            "meaningful_denominator_conditions": item["denominator_guard"],
            "sign_handling": "transition-aware" if "TRANSITION" in metric else "negative can be favorable" if metric == "NET_DEBT_POSITION" else "negative is bad/floor" if item["economic_domain"] == "POSITIVE_ONLY_GOOD" else "normal signed distribution",
            "transition_handling": "required" if item["economic_domain"] == "SIGNED_TRANSITION_METRIC" else "not primary",
            "zero_is_special": int(zero_boundary == "special"),
            "missing_policy": item["missing_policy"],
            "outlier_policy": item["outlier_candidates"],
            "expected_score_scale": f"0..{item['max_score']} full integer scale",
        })
    return rows


def score_redundancy_analysis() -> list[dict[str, Any]]:
    return [
        {"redundancy_group": "OPERATING_PROFITABILITY", "metrics": "EBIT_MARGIN|OPERATING_MARGIN|EBITDA_MARGIN", "primary": "EBIT_MARGIN", "secondary_diagnostic": "OPERATING_MARGIN|EBITDA_MARGIN", "retire_from_score": "OPERATING_MARGIN|EBITDA_MARGIN"},
        {"redundancy_group": "OPERATING_VALUATION", "metrics": "EV_EBIT|EBIT_YIELD", "primary": "EV_EBIT", "secondary_diagnostic": "EBIT_YIELD", "retire_from_score": "EBIT_YIELD if EV_EBIT retained"},
        {"redundancy_group": "FCF_VALUATION", "metrics": "FCF_YIELD|PRICE_FCF", "primary": "FCF_YIELD", "secondary_diagnostic": "PRICE_FCF", "retire_from_score": "PRICE_FCF"},
        {"redundancy_group": "EARNINGS_GROWTH", "metrics": "EBIT_GROWTH|OPERATING_INCOME_GROWTH|EBITDA_GROWTH", "primary": "EBIT_GROWTH_TRANSITION", "secondary_diagnostic": "OPERATING_INCOME_GROWTH|EBITDA_GROWTH", "retire_from_score": "EBITDA_GROWTH unless justified"},
    ]


def lifecycle_feature_design() -> list[dict[str, Any]]:
    return [
        {"feature": "revenue_growth", "calculation": "TTM revenue growth vs prior-year TTM", "sign_interpretation": "higher growth, but negative revenue growth is contraction", "smoothing": "2-quarter confirmation candidate", "state_relevance": "growth/recovery/mature/decline", "calibration_population": "2021-2025", "missing_policy": "feature missing, do not infer state alone"},
        {"feature": "ebit_signed_transition", "calculation": "previous TTM EBIT to current TTM EBIT regime", "sign_interpretation": "negative deteriorating/improving/crossing positive/positive declining/growing", "smoothing": "confirmation unless crossing is large", "state_relevance": "distress/recovery/profitable growth/decline", "calibration_population": "2021-2025", "missing_policy": "UNCLASSIFIED_OR_LOW_CONFIDENCE"},
        {"feature": "fcf_signed_transition", "calculation": "previous TTM FCF to current TTM FCF regime", "sign_interpretation": "same transition states as EBIT", "smoothing": "2-quarter confirmation candidate", "state_relevance": "cash generation and quality", "calibration_population": "2021-2025", "missing_policy": "secondary missing allowed"},
        {"feature": "ebit_margin_level", "calculation": "TTM EBIT / TTM Revenue", "sign_interpretation": "<=0 unprofitable; positive domain calibrated", "smoothing": "state thresholds with hysteresis band", "state_relevance": "startup/recovery/profitable/mature", "calibration_population": "2021-2025", "missing_policy": "state confidence reduced"},
        {"feature": "margin_expansion", "calculation": "EBIT margin change vs prior-year TTM margin", "sign_interpretation": "expansion/contraction", "smoothing": "confirmation", "state_relevance": "scaling/decelerating/declining", "calibration_population": "2021-2025", "missing_policy": "no transition inference"},
    ]


def lifecycle_states_md() -> str:
    return """# Lifecycle State Design

Lifecycle is descriptive, not an attractiveness score. Proposed states for Phase 6D calibration:

- DISTRESS_CONTRACTION: negative profitability and deteriorating EBIT/FCF or revenue contraction.
- RECOVERY: negative but improving EBIT/FCF or early positive inflection.
- EMERGING_GROWTH: strong revenue growth with still-low or unstable profitability.
- PROFITABLE_GROWTH: positive EBIT with revenue growth and margin expansion.
- MATURE_GROWTH: positive EBIT/FCF, moderate growth, stable margins.
- MATURE_STABLE: positive profitability, low growth, stable cash generation.
- DECELERATING: positive but weakening growth/margins.
- DECLINING: sustained contraction or positive-to-negative profitability transition.
- UNCLASSIFIED_LOW_CONFIDENCE: insufficient feature coverage.
"""


def run_phase6b_design(*, v3_db: Path, artifact_root: Path, write_durable_docs: bool = True) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before = production_counts(v3_db)
    write_artifacts(artifact_root)
    after = production_counts(v3_db)
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "recommended_next_step": NEXT_PHASE,
        "run_at_utc": utc_now(),
        "artifact_root": str(artifact_root),
        "score_components_found": len(existing_score_component_inventory()),
        "sparse_scale_components": sum(1 for row in existing_score_component_inventory() if row["scale_status"] == "SPARSE_SCALE_REQUIRES_RECALIBRATION"),
        "lifecycle_states_found": len(existing_lifecycle_inventory()),
        "proposed_primary_score_components": len([row for row in proposed_score_components() if row["role"].startswith("PRIMARY")]),
        "calibration_window": f"{CALIBRATION_START} through {CALIBRATION_END}",
        "oos_window": "2026 YTD",
        "stress_window": "2020",
        "production_writes": {
            "score": after["score"] - before["score"],
            "lifecycle": 0,
            "valuation": after["valuation"] - before["valuation"],
            "ttm": after["ttm"] - before["ttm"],
            "canonical": after["canonical"] - before["canonical"],
        },
        "integrity": structural_checks(v3_db),
    }
    write_json(artifact_root / "phase6b_summary.json", summary)
    if write_durable_docs:
        write_doc(Path("docs/fundamentals_v3_phase6b_score_lifecycle_calibration_design.md"), summary)
        update_phase6a_doc(Path("docs/fundamentals_v3_phase6_score_valuation_engine_design.md"))
        update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def production_counts(v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {
            "score": count(conn, "v3_score"),
            "valuation": count(conn, "v3_valuation"),
            "ttm": count(conn, "v3_ttm"),
            "canonical": count(conn, "v3_quarter") + count(conn, "v3_quarter_fundamentals"),
        }


def structural_checks(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {"quick_check": conn.execute("PRAGMA quick_check").fetchone()[0], "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall())}


def count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def write_artifacts(root: Path) -> None:
    write_csv(root / "existing_score_component_inventory.csv", existing_score_component_inventory())
    write_csv(root / "existing_score_scale_usage.csv", existing_score_component_inventory())
    write_csv(root / "existing_lifecycle_inventory.csv", existing_lifecycle_inventory())
    write_csv(root / "existing_lifecycle_transition_rules.csv", [{"rule": "none", "hysteresis": "none", "minimum_state_age": "none", "confirmation": "none", "issue": "current lifecycle is stateless per-row classification"}])
    write_csv(root / "proposed_score_component_architecture.csv", proposed_score_components())
    write_csv(root / "score_redundancy_analysis.csv", score_redundancy_analysis())
    write_text(root / "score_group_design.md", score_group_design_md())
    write_text(root / "score_weight_design_options.md", score_weight_design_md())
    write_csv(root / "economic_domain_policies.csv", economic_domain_policies())
    write_text(root / "negative_value_policy.md", negative_value_policy_md())
    write_text(root / "missing_value_policy.md", missing_value_policy_md())
    write_text(root / "not_meaningful_policy.md", not_meaningful_policy_md())
    write_text(root / "outlier_policy_design.md", outlier_policy_design_md())
    write_csv(root / "score_scale_contract.csv", [scalar_scale_contract(row["metric_id"], int(row["max_score"])) for row in proposed_score_components()])
    write_text(root / "full_scale_utilization_rules.md", full_scale_rules_md())
    write_csv(root / "score_calibration_method_design.csv", proposed_score_components())
    write_text(root / "year_by_year_distribution_requirements.md", distribution_requirements_md())
    write_csv(root / "phase6c_input_contract.csv", proposed_score_components())
    write_csv(root / "lifecycle_feature_design.csv", lifecycle_feature_design())
    write_text(root / "lifecycle_state_design.md", lifecycle_states_md())
    write_text(root / "lifecycle_transition_design.md", lifecycle_transition_design_md())
    write_text(root / "lifecycle_hysteresis_design.md", lifecycle_hysteresis_design_md())
    write_csv(root / "phase6d_input_contract.csv", lifecycle_feature_design())
    write_text(root / "phase6e_oos_validation_contract.md", phase6e_oos_md())
    write_text(root / "phase6e_stress_validation_contract.md", phase6e_stress_md())
    write_text(root / "score_scale_validation_contract.md", score_scale_validation_md())
    write_text(root / "updated_phase6_master_plan.md", phase6_master_plan_md())
    write_text(root / "recommended_next_step.md", NEXT_PHASE + "\n")


def score_group_design_md() -> str:
    return "Groups: Growth, Profitability/Quality, Valuation, Balance Sheet/Risk. Weights must sum to 100% in Phase 6C. No group may require EBITDA. Secondary diagnostics are stored/reported outside the primary score.\n"


def score_weight_design_md() -> str:
    return "Phase 6B does not lock final numeric weights. Allowed designs for Phase 6C: equal group weights, business-prior weights, or capped component weights inside each group. Do not optimize weights against 2026 returns.\n"


def negative_value_policy_md() -> str:
    return "Negative values are metric-specific: negative EBIT margin and negative FCF margin are bad economic values; negative net debt means net cash and is favorable; negative denominators in valuation/leverage ratios are NOT_MEANINGFUL.\n"


def missing_value_policy_md() -> str:
    return "MISSING_DATA is distinct from score 0. Primary score should use metric-specific readiness and minimum component coverage; available components may be renormalized only under explicit Phase 6C rules.\n"


def not_meaningful_policy_md() -> str:
    return "NOT_MEANINGFUL means the ratio is not ranked or treated as cheap/expensive. It is distinct from missing input and from bad economic values. Near-zero denominator guard is 1e-9.\n"


def outlier_policy_design_md() -> str:
    return "Phase 6C must compute min/max, P1/P5/P10/P25/P50/P75/P90/P95/P99 and skewness by year and pooled 2021-2025. Outlier handling is metric-specific: winsorization, percentile caps, log transform, piecewise thresholds, or no transform.\n"


def full_scale_rules_md() -> str:
    return "For scalar component range 0..N, every integer 0,1,...,N must be defined and reachable. Validator: reachable_score_count == max_score - min_score + 1 and no dead values. Hard-zero plus positive-domain 1..N is allowed.\n"


def distribution_requirements_md() -> str:
    return "Phase 6C must calculate each candidate score variable for 2021, 2022, 2023, 2024, 2025 and pooled 2021-2025. Required stats: observations, median, P10, P25, P75, P90, P95, plus P1/P5/P99/min/max/skewness for outlier review.\n"


def lifecycle_transition_design_md() -> str:
    return "Phase 6D must produce a state_t -> state_t+1 transition matrix over 2021-2025, including stay rate, one-step transitions, multi-step jumps, reversals, and average state duration. Direct jumps are allowed only for strong signed inflections or severe deterioration.\n"


def lifecycle_hysteresis_design_md() -> str:
    return "Use hysteresis bands and confirmation rules to avoid quarter-to-quarter oscillation. Candidate default: require two consecutive quarters for normal state change, allow one-quarter immediate transition for crossing-to-positive or positive-turning-negative events above calibrated magnitude thresholds. Missing features reduce confidence rather than forcing a state.\n"


def phase6e_oos_md() -> str:
    return "2026 YTD is out-of-sample validation only. Report score distribution, bucket utilization, saturation, lifecycle distribution, churn, outliers, missing/readiness, and company coverage. Do not tune thresholds on 2026 without documenting structural failure.\n"


def phase6e_stress_md() -> str:
    return "2020 is stress/robustness only. Apply locked score and lifecycle mappings from 2021-2025 calibration. Check score collapse/explosion, denominator guards, negative-growth handling, lifecycle transitions and recovery/decline behavior.\n"


def score_scale_validation_md() -> str:
    return "For each component and for calibration/OOS/stress windows, report counts for every integer score value, unused scores, overconcentration, score-0 saturation and max-score saturation.\n"


def phase6_master_plan_md() -> str:
    return """- Phase 6A - Downstream Inventory & Policy Lock: DONE
- Phase 6B - Score & Lifecycle Calibration Design: THIS PHASE
- Phase 6C - Score Distributions & Point Calibration
- Phase 6D - Lifecycle Recalibration
- Phase 6E - Out-of-Sample & Stress Validation
- Phase 6F - Valuation Engine Implementation
- Phase 6G - Score Engine Implementation
- Phase 6H - Lifecycle Engine Implementation
- Phase 6I - Production Rebuild & Proving
- Phase 6J - Phase 6 Closure
"""


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(
        f"""# Fundamentals V3 Phase 6B Score & Lifecycle Calibration Design

Classification: `{summary['classification']}`

Phase 6B locks the score and lifecycle calibration framework before production score, valuation, or lifecycle writes.

## Windows

- Calibration: `2021-01-01 through 2025-12-31`
- OOS validation: `2026 YTD`
- Stress/robustness: `2020`
- 2018-2019: context only, not threshold fitting

## Existing Model

Existing score components found: `{summary['score_components_found']}`.

Sparse-scale components requiring recalibration: `{summary['sparse_scale_components']}`.

Existing lifecycle states found: `{summary['lifecycle_states_found']}`. Current lifecycle is EBITDA-dependent and stateless, with no hysteresis or transition matrix.

## Locked Design

Primary score groups are Growth, Profitability/Quality, Valuation, and Balance Sheet/Risk. EBIT is the primary earnings metric. EBITDA is preserved only as secondary diagnostics.

Every scalar score component must use the full integer 0..N scale. Missing data, bad economic values, and not-meaningful ratios are distinct states.

Lifecycle is descriptive and trajectory-oriented. It is not an attractiveness score and must not be collapsed into the score.

## Safety

Production writes: `{summary['production_writes']}`.

## Authoritative Phase 6 Plan

{phase6_master_plan_md()}

Next: `{summary['recommended_next_step']}`
""",
        encoding="utf-8",
    )


def update_phase6a_doc(path: Path) -> None:
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8")
    old = "Next: `MASTER PLAN PHASE 6B - SCORE & VALUATION PRODUCTION IMPLEMENTATION`"
    new = "Next: `MASTER PLAN PHASE 6B - SCORE & LIFECYCLE CALIBRATION DESIGN`"
    path.write_text(text.replace(old, new), encoding="utf-8")


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = "## Phase 6"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 6

Authoritative structure:

{phase6_master_plan_md()}
## Phase 6A

Status: `DONE`

Classification: `FUNDAMENTALS_V3_PHASE6_SCORE_VALUATION_DESIGN_COMPLETE_READY_FOR_PRODUCTION_IMPLEMENTATION`

## Phase 6B

Classification: `{summary['classification']}`

Status: `DESIGN_COMPLETE_READY_FOR_PHASE6C`

Production score writes: `{summary['production_writes']['score']}`

Lifecycle writes: `{summary['production_writes']['lifecycle']}`

Production valuation writes: `{summary['production_writes']['valuation']}`

TTM writes: `{summary['production_writes']['ttm']}`

Canonical writes: `{summary['production_writes']['canonical']}`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition.rstrip() + "\n", encoding="utf-8")


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


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
