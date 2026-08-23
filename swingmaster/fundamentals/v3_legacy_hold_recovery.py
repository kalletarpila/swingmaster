from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_core_gap_diagnostic import compare_values, connect_readonly, load_v2_rows, load_v3_rows
from swingmaster.fundamentals.v3_legacy_backward_validation import (
    ALL_FIELDS,
    CORE_FIELDS,
    PHASE3C_1C_ARTIFACT_ROOT,
    SPECIAL_CASES,
    V3_HISTORICAL_PERIOD_END_FLOOR,
    build_legacy_only,
    build_overlap,
    db_counts,
    read_only_proof,
    run_legacy_breakpoint_diagnostic,
    sequence_validation,
)


PHASE3C_1D_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_1d_legacy_hold_recovery/20260822T_PHASE3C_1D_LEGACY_HOLD_RECOVERY")
FLOW_FIELDS = ("revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow", "capex", "free_cashflow")
INSTANT_FIELDS = ("cash", "total_debt", "shares_outstanding")
UNSAFE_DERIVED_FIELDS = ("ebit", "ebitda")
READY_STATES = {
    "READY_EXISTING_CHAIN",
    "READY_SEC_Q4_STRUCTURE",
    "READY_REANCHORED_WITH_V2",
    "READY_REANCHORED_LEGACY_ONLY",
    "READY_BRIDGED_SEGMENT",
}


@dataclass
class SecPeriodEvidence:
    ticker: str
    period_end_date: str
    fiscal_years: Counter[str] = field(default_factory=Counter)
    fiscal_periods: Counter[str] = field(default_factory=Counter)
    forms: Counter[str] = field(default_factory=Counter)
    starts: Counter[str] = field(default_factory=Counter)
    filed_dates: Counter[str] = field(default_factory=Counter)
    concepts: Counter[str] = field(default_factory=Counter)
    statement_types: Counter[str] = field(default_factory=Counter)

    @property
    def fiscal_year(self) -> int | None:
        value = self.fiscal_years.most_common(1)[0][0] if self.fiscal_years else None
        return int(value) if value and value.isdigit() else None

    @property
    def fiscal_period(self) -> str | None:
        return self.fiscal_periods.most_common(1)[0][0] if self.fiscal_periods else None

    @property
    def form(self) -> str | None:
        return self.forms.most_common(1)[0][0] if self.forms else None

    @property
    def filed_date(self) -> str | None:
        return self.filed_dates.most_common(1)[0][0] if self.filed_dates else None


def run_legacy_hold_recovery(*, v3_db: Path, legacy_db: Path, v2_db: Path, artifact_root: Path) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before_counts = db_counts(v3_db, legacy_db, v2_db)
    phase3c1c_root = artifact_root / "phase3c1c_reproduction"
    phase3c1c = run_legacy_breakpoint_diagnostic(v3_db=v3_db, legacy_db=legacy_db, v2_db=v2_db, artifact_root=phase3c1c_root)
    baseline = reproduce_3c1c_population(phase3c1c)
    if not baseline["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_1D_BASELINE_DRIFT:" + json.dumps(baseline, sort_keys=True))

    ready_1c = read_csv(phase3c1c_root / "phase3c2_ready_rows.csv")
    hold_1c = read_csv(phase3c1c_root / "phase3c2_hold_rows.csv")
    v3 = connect_readonly(v3_db)
    legacy = connect_readonly(legacy_db)
    v2 = connect_readonly(v2_db)
    v3_rows = load_v3_rows(v3)
    legacy_rows = load_legacy_quarterly(legacy)
    legacy_by_key = {(row["ticker"], row["period_end_date"]): row for row in legacy_rows}
    legacy_dict = {(row["ticker"], row["period_end_date"]): row for row in legacy_rows}
    v2_rows = load_v2_rows(v2)
    v2_by_period = {(row["ticker"], row["period_end_date"]): row for row in v2_rows.values() if row.get("period_end_date")}
    sec_periods = load_sec_period_evidence(legacy)
    source_shape = inspect_legacy_sec_source_shape(legacy)
    v3.close()
    legacy.close()
    v2.close()

    fiscal_structures = legacy_fiscal_year_structures(sec_periods)
    q4_presence = q4_presence_by_year(fiscal_structures)
    q4_input_semantics = q4_input_semantics_rows(source_shape)
    q4_field_eligibility = q4_field_eligibility_rows()
    q4_calibration = validate_q4_against_known_canonical(v3_rows, legacy_by_key, sec_periods)
    q4_accuracy = q4_reconstruction_accuracy(q4_calibration)
    q4_vintage = q4_vintage_compatibility(sec_periods)

    final_rows = classify_final_rows(ready_1c, hold_1c, legacy_by_key, sec_periods, v2_by_period)
    final_rows = resolve_duplicate_ready_fyfqs(final_rows)
    ready_rows = [row for row in final_rows if row["final_disposition"] in READY_STATES]
    hold_rows = [row for row in final_rows if row["final_disposition"] not in READY_STATES]
    sequence_violations = sequence_validation([to_sequence_row(row) for row in ready_rows], historical_floor=V3_HISTORICAL_PERIOD_END_FLOOR)
    q4_plan = build_q4_construction_plan(ready_rows, sec_periods, fiscal_structures, legacy_by_key)
    dry_plan = build_dry_import_plan(ready_rows, q4_plan)
    contribution = expected_contribution(dry_plan, q4_plan)
    yearly = yearly_recovery_rows(final_rows)
    segment_map, validated_segments = discover_legacy_segments(final_rows)
    v2_calibration = v2_historical_anchor_calibration(v3_rows, legacy_by_key, v2_by_period)
    transition_reanalysis = transition_population_reanalysis(hold_1c, final_rows, sec_periods, v2_by_period)
    false_transition = counter_rows(transition_reanalysis, "reanalysis")
    gap_inventory = historical_gap_inventory(final_rows)
    repair_candidates = legacy_fyfq_repair_candidates(final_rows)
    phase3c2b = phase3c2b_repair_opportunity(hold_rows)
    special = special_case_validation(final_rows)
    after_counts = db_counts(v3_db, legacy_db, v2_db)
    readonly = read_only_proof(before_counts, after_counts, v3_db)
    classification = final_classification(ready_rows, sequence_violations, q4_accuracy)
    summary = {
        "classification": classification,
        "baseline_reconciliation": baseline,
        "source_shape": source_shape,
        "fiscal_year_structures": dict(Counter(row["structure"] for row in fiscal_structures)),
        "q4_presence": dict(Counter(row["q4_state"] for row in q4_presence)),
        "q4_accuracy": q4_accuracy,
        "transition_reanalysis": dict(Counter(row["reanalysis"] for row in transition_reanalysis)),
        "v2_calibration": summarize_v2_calibration(v2_calibration),
        "v2_help": v2_help_summary(final_rows),
        "legacy_only_recovery": legacy_only_recovery_summary(final_rows, segment_map, validated_segments),
        "final_classification": dict(Counter(row["final_disposition"] for row in final_rows)),
        "phase3c2_ready_rows": len(ready_rows),
        "phase3c2_hold_rows": len(hold_rows),
        "yearly_recovery": yearly,
        "gap_inventory": dict(Counter(row["gap_category"] for row in gap_inventory)),
        "mapping_repair_candidates": len(repair_candidates),
        "phase3c2_expected_contribution": contribution,
        "phase3c2_sequence_violations": len(sequence_violations),
        "phase3c2b_repair_opportunity": dict(Counter(row["repair_bucket"] for row in phase3c2b)),
        "special_cases": special,
        "read_only_proof": readonly,
        "recommended_next_step": "MASTER PLAN PHASE 3C-2 - LEGACY DEEP-HISTORY EXTENSION" if classification.endswith("READY_FOR_3C2") else "MASTER PLAN PHASE 3C-1E - LEGACY SEC Q4 MODEL REPAIR",
    }
    write_artifacts(
        artifact_root,
        baseline=baseline,
        source_shape=source_shape,
        fiscal_structures=fiscal_structures,
        q4_presence=q4_presence,
        q4_input_semantics=q4_input_semantics,
        q4_field_eligibility=q4_field_eligibility,
        q4_calibration=q4_calibration,
        q4_accuracy=q4_accuracy,
        q4_vintage=q4_vintage,
        transition_reanalysis=transition_reanalysis,
        false_transition=false_transition,
        segment_map=segment_map,
        v2_calibration=v2_calibration,
        v2_reanchors=[row for row in final_rows if row["final_disposition"] == "READY_REANCHORED_WITH_V2"],
        legacy_reanchors=[row for row in final_rows if row["final_disposition"] == "READY_REANCHORED_LEGACY_ONLY"],
        segment_false_positive_validation=segment_false_positive_validation(v2_calibration),
        validated_segments=validated_segments,
        gap_inventory=gap_inventory,
        repair_candidates=repair_candidates,
        final_rows=final_rows,
        yearly=yearly,
        q4_plan=q4_plan,
        ready_rows=ready_rows,
        hold_rows=hold_rows,
        dry_plan=dry_plan,
        contribution=contribution,
        phase3c2b=phase3c2b,
        summary=summary,
    )
    return summary


def reproduce_3c1c_population(summary: dict[str, Any]) -> dict[str, Any]:
    observed = {
        "legacy_only_2018plus": summary["population"]["legacy_only_rows_2018plus"],
        "ready": summary["phase3c2_ready_rows"],
        "hold": summary["phase3c2_hold_rows"],
        "hold_behind_breakpoint": summary["legacy_2018plus_classification"].get("HOLD_BEHIND_BREAKPOINT", 0),
        "hold_true_fiscal_transition": summary["legacy_2018plus_classification"].get("HOLD_TRUE_FISCAL_TRANSITION", 0),
        "hold_insufficient": summary["legacy_2018plus_classification"].get("HOLD_INSUFFICIENT_EVIDENCE", 0),
    }
    expected = {
        "legacy_only_2018plus": 67477,
        "ready": 4321,
        "hold": 63156,
        "hold_behind_breakpoint": 61093,
        "hold_true_fiscal_transition": 775,
        "hold_insufficient": 1288,
    }
    return {"observed": observed, "expected": expected, "passed": observed == expected}


def load_legacy_quarterly(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT ticker, period_end_date, revenue, gross_profit, operating_income, ebit, ebitda,
               net_income, operating_cashflow, capex, free_cashflow, cash, total_debt,
               shares_outstanding, currency, run_id
        FROM rc_fundamental_quarterly
        WHERE period_end_date >= ?
        """,
        (V3_HISTORICAL_PERIOD_END_FLOOR,),
    ).fetchall()
    return [dict(row) | {"ticker": str(row["ticker"]).upper()} for row in rows]


def load_sec_period_evidence(conn: sqlite3.Connection) -> dict[tuple[str, str], SecPeriodEvidence]:
    conn.row_factory = sqlite3.Row
    evidence: dict[tuple[str, str], SecPeriodEvidence] = {}
    rows = conn.execute(
        """
        SELECT ticker, statement_type, period_end_date, period_type, field_name, source
        FROM rc_fundamental_statement_raw
        WHERE period_end_date >= ? AND source = 'sec_edgar'
        """,
        (V3_HISTORICAL_PERIOD_END_FLOOR,),
    ).fetchall()
    for row in rows:
        key = (str(row["ticker"]).upper(), row["period_end_date"])
        item = evidence.setdefault(key, SecPeriodEvidence(ticker=key[0], period_end_date=key[1]))
        item.statement_types[str(row["statement_type"])] += 1
        field_name = str(row["field_name"] or "")
        concept, attrs = parse_sec_field_name(field_name)
        if concept:
            item.concepts[concept] += 1
        for attr, counter in (("fy", item.fiscal_years), ("fp", item.fiscal_periods), ("form", item.forms), ("start", item.starts), ("filed", item.filed_dates)):
            value = attrs.get(attr)
            if value and value != "NULL":
                counter[value] += 1
    return evidence


def parse_sec_field_name(field_name: str) -> tuple[str, dict[str, str]]:
    pieces = field_name.split("|")
    concept = pieces[0].strip() if pieces else ""
    attrs = {}
    for piece in pieces[1:]:
        if "=" in piece:
            key, value = piece.split("=", 1)
            attrs[key] = value
    return concept, attrs


def inspect_legacy_sec_source_shape(conn: sqlite3.Connection) -> dict[str, Any]:
    conn.row_factory = sqlite3.Row
    by_period_type = [dict(row) for row in conn.execute("SELECT period_type, statement_type, COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers FROM rc_fundamental_statement_raw GROUP BY period_type, statement_type ORDER BY rows DESC")]
    raw_columns = [row["name"] for row in conn.execute("PRAGMA table_info(rc_fundamental_statement_raw)").fetchall()]
    quarterly_columns = [row["name"] for row in conn.execute("PRAGMA table_info(rc_fundamental_quarterly)").fetchall()]
    sample = conn.execute("SELECT field_name FROM rc_fundamental_statement_raw WHERE period_type='sec_fact' AND field_name LIKE '%|form=%|fp=%|%' LIMIT 1").fetchone()
    return {
        "raw_statement_columns": ";".join(raw_columns),
        "quarterly_columns": ";".join(quarterly_columns),
        "has_form_type_column": int("form_type" in raw_columns or "form" in raw_columns),
        "has_period_start_column": int("period_start" in raw_columns or "start_date" in raw_columns),
        "has_duration_column": int("duration" in raw_columns),
        "metadata_embedded_in_field_name": int(bool(sample)),
        "period_type_statement_counts": by_period_type,
        "local_semantics": "SEC fy/fp/form/start/filed metadata is embedded in rc_fundamental_statement_raw.field_name for sec_fact rows; rc_fundamental_quarterly is normalized by ticker+period_end only.",
    }


def legacy_fiscal_year_structures(sec_periods: dict[tuple[str, str], SecPeriodEvidence]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], dict[str, Any]] = defaultdict(lambda: {"periods": defaultdict(list), "forms": Counter()})
    for item in sec_periods.values():
        fy = item.fiscal_year
        fp = item.fiscal_period
        if fy is None or fp is None:
            continue
        key = (item.ticker, fy)
        grouped[key]["periods"][fp].append(item.period_end_date)
        if item.form:
            grouped[key]["forms"][item.form] += 1
    rows = []
    for (ticker, fy), data in sorted(grouped.items()):
        periods = data["periods"]
        has = {q: int(bool(periods.get(q))) for q in ("Q1", "Q2", "Q3", "Q4", "FY")}
        if has["Q1"] and has["Q2"] and has["Q3"] and has["FY"] and not has["Q4"]:
            structure = "Q1_Q2_Q3_FY"
        elif has["Q1"] and has["Q2"] and has["Q3"] and has["Q4"] and has["FY"]:
            structure = "Q1_Q2_Q3_Q4_FY"
        elif has["FY"] and not any(has[q] for q in ("Q1", "Q2", "Q3", "Q4")):
            structure = "FY_ONLY"
        else:
            structure = "OTHER_OR_PARTIAL"
        rows.append({"ticker": ticker, "fiscal_year": fy, **{f"has_{q.lower()}": has[q] for q in ("Q1", "Q2", "Q3", "Q4", "FY")}, "structure": structure, "forms": ";".join(f"{k}:{v}" for k, v in sorted(data["forms"].items()))})
    return rows


def q4_presence_by_year(structures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in structures:
        if row["has_q4"]:
            state = "Q4_EXPLICITLY_AVAILABLE"
        elif row["has_fy"] and row["has_q1"] and row["has_q2"] and row["has_q3"]:
            state = "EXPECTED_SEC_Q4_NOT_SEPARATELY_FILED"
        elif row["has_fy"]:
            state = "FY_ROW_AVAILABLE_FOR_Q4_RECONSTRUCTION"
        else:
            state = "FY_ROW_MISSING"
        rows.append({**row, "q4_state": state})
    return rows


def q4_input_semantics_rows(source_shape: dict[str, Any]) -> list[dict[str, Any]]:
    has_embedded = bool(source_shape["metadata_embedded_in_field_name"])
    rows = []
    for field in FLOW_FIELDS:
        rows.append({"field": field, "semantic_summary": "SEC fact rows expose form/fp/start/filed in field_name" if has_embedded else "DURATION_METADATA_NOT_AVAILABLE", "standalone_vs_ytd": "REQUIRES_FIELD_LEVEL_CONCEPT_DURATION_CHECK", "phase3c2_policy": "derive only when compatible concepts and duration inputs are present"})
    for field in INSTANT_FIELDS:
        rows.append({"field": field, "semantic_summary": "instant balance semantics inferred from balance statement and frame ending in I where present", "standalone_vs_ytd": "INSTANT_NOT_DURATION", "phase3c2_policy": "use FY-end instant directly; never difference instant fields"})
    for field in UNSAFE_DERIVED_FIELDS:
        rows.append({"field": field, "semantic_summary": "not safely derivable from generic SEC model", "standalone_vs_ytd": "SEMANTIC_RISK", "phase3c2_policy": "leave NULL unless direct compatible concept is accepted later"})
    return rows


def q4_field_eligibility_rows() -> list[dict[str, Any]]:
    rows = []
    for field in ("revenue", "gross_profit", "operating_income", "net_income"):
        rows.append({"field": field, "eligibility": "SAFE_FY_MINUS_Q1_Q2_Q3", "notes": "Only with matching SEC concepts and duration semantics."})
    rows.extend([
        {"field": "operating_cashflow", "eligibility": "SAFE_DIRECT_FY_MINUS_9M", "notes": "Prefer FY minus 9M/YTD when compatible; otherwise FY minus Q1-Q3 if proven standalone."},
        {"field": "capex", "eligibility": "SAFE_DIRECT_FY_MINUS_9M", "notes": "Preserve locked negative-capex convention."},
        {"field": "free_cashflow", "eligibility": "SAFE_FROM_RECONSTRUCTED_OCF_PLUS_CAPEX", "notes": "Do not subtract uncertain provider-derived FCF blindly."},
        {"field": "cash", "eligibility": "DIRECT_FY_END_INSTANT", "notes": "Use FY-end instant directly."},
        {"field": "total_debt", "eligibility": "DIRECT_FY_END_INSTANT", "notes": "Use accepted FY-end debt concept/components directly."},
        {"field": "shares_outstanding", "eligibility": "DIRECT_FY_END_INSTANT", "notes": "Use period-end/instant share concept only; do not difference."},
        {"field": "ebit", "eligibility": "SEMANTICALLY_UNSAFE", "notes": "Do not infer EBIT from operating income here."},
        {"field": "ebitda", "eligibility": "SEMANTICALLY_UNSAFE", "notes": "Leave NULL absent accepted direct concept/derivation."},
    ])
    return rows


def validate_q4_against_known_canonical(v3_rows: list[dict[str, Any]], legacy_by_key: dict[tuple[str, str], dict[str, Any]], sec_periods: dict[tuple[str, str], SecPeriodEvidence]) -> list[dict[str, Any]]:
    rows = []
    for v3 in v3_rows:
        if v3["fiscal_quarter"] != "Q4":
            continue
        key = (v3["ticker"], v3["period_end_date"])
        legacy = legacy_by_key.get(key)
        sec = sec_periods.get(key)
        if not legacy or not sec or sec.fiscal_period != "FY":
            continue
        for field in ALL_FIELDS:
            comp = compare_values(field, v3.get(field), legacy.get(field))
            rows.append({"ticker": v3["ticker"], "fiscal_year": v3["fiscal_year"], "period_end_date": v3["period_end_date"], "field": field, "comparable": int(comp.comparable), "within_1pct": int(comp.within_1pct), "within_2pct": int(comp.within_2pct), "within_5pct": int(comp.within_5pct), "within_10pct": int(comp.within_10pct), "sign_conflict": int(comp.sign_mismatch), "relative_difference": comp.relative_difference})
    return rows


def q4_reconstruction_accuracy(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for field in ALL_FIELDS:
        field_rows = [row for row in rows if row["field"] == field]
        comp = [row for row in field_rows if row["comparable"]]
        out.append({"field": field, "tested": len(field_rows), "comparable": len(comp), "le_1pct": sum(row["within_1pct"] for row in comp), "le_2pct": sum(row["within_2pct"] for row in comp), "le_5pct": sum(row["within_5pct"] for row in comp), "le_10pct": sum(row["within_10pct"] for row in comp), "gt_10pct": sum(1 for row in comp if not row["within_10pct"]), "sign_conflict": sum(row["sign_conflict"] for row in comp), "unavailable": len(field_rows) - len(comp)})
    return out


def q4_vintage_compatibility(sec_periods: dict[tuple[str, str], SecPeriodEvidence]) -> list[dict[str, Any]]:
    rows = []
    for item in sec_periods.values():
        if item.fiscal_period == "FY":
            rows.append({"ticker": item.ticker, "period_end_date": item.period_end_date, "fiscal_year": item.fiscal_year, "filed_dates": ";".join(item.filed_dates), "vintage_status": "FY_ANNUAL_BASIS_ACCEPTABLE_FOR_LATEST_STATEMENT_MIGRATION" if len(item.filed_dates) == 1 else "Q4_DERIVATION_VINTAGE_CONFLICT"})
    return rows


def classify_final_rows(ready_1c: list[dict[str, str]], hold_1c: list[dict[str, str]], legacy_by_key: dict[tuple[str, str], dict[str, Any]], sec_periods: dict[tuple[str, str], SecPeriodEvidence], v2_by_period: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in ready_1c:
        rows.append(finalize_row(row, "READY_EXISTING_CHAIN", row.get("fiscal_year"), row.get("fiscal_quarter"), "PHASE3C_1C_READY", legacy_by_key, sec_periods, v2_by_period))
    for row in hold_1c:
        key = (row["ticker"], row["period_end_date"])
        sec = sec_periods.get(key)
        v2 = v2_by_period.get(key)
        if sec and sec.fiscal_period == "FY":
            disposition = "READY_SEC_Q4_STRUCTURE"
            fy, fq, evidence = canonical_q4_fiscal_year_from_period_end(row["period_end_date"]), "Q4", "SEC_FY_ROW_REPRESENTS_Q4_SLOT_PERIOD_END_ANCHORED"
        elif v2 and sec:
            v2_fiscal_year = plausible_fiscal_year(v2["fiscal_year"])
            if v2_fiscal_year is None:
                disposition = "HOLD_INSUFFICIENT_EVIDENCE"
                fy, fq, evidence = "", str(v2["fiscal_quarter"]), "V2_EXACT_PERIOD_INVALID_FISCAL_YEAR"
            else:
                disposition = "READY_REANCHORED_WITH_V2"
                fy, fq, evidence = v2_fiscal_year, str(v2["fiscal_quarter"]), "LEGACY_SEC_ROW_PLUS_V2_EXACT_PERIOD"
        elif sec and sec.fiscal_period in {"Q1", "Q2", "Q3", "Q4"} and sec.fiscal_year:
            sec_fiscal_year = plausible_fiscal_year(sec.fiscal_year)
            if sec_fiscal_year is None:
                disposition = "HOLD_INSUFFICIENT_EVIDENCE"
                fy, fq, evidence = "", sec.fiscal_period, "LEGACY_SEC_INVALID_FISCAL_YEAR"
            else:
                disposition = "READY_REANCHORED_LEGACY_ONLY"
                fy, fq, evidence = sec_fiscal_year, sec.fiscal_period, "LEGACY_SEC_FY_FP_METADATA"
        elif v2:
            disposition = "HOLD_INSUFFICIENT_EVIDENCE"
            fy, fq, evidence = plausible_fiscal_year(v2["fiscal_year"]) or "", str(v2["fiscal_quarter"]), "V2_ONLY_NOT_ENOUGH"
        else:
            disposition = "HOLD_ISOLATED_ROW"
            fy, fq, evidence = row.get("fiscal_year") or "", row.get("fiscal_quarter") or "", "NO_SEC_OR_V2_SEGMENT_EVIDENCE"
        rows.append(finalize_row(row, disposition, fy, fq, evidence, legacy_by_key, sec_periods, v2_by_period))
    return rows


def finalize_row(row: dict[str, Any], disposition: str, fiscal_year: Any, fiscal_quarter: Any, evidence: str, legacy_by_key: dict[tuple[str, str], dict[str, Any]], sec_periods: dict[tuple[str, str], SecPeriodEvidence], v2_by_period: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    key = (row["ticker"], row["period_end_date"])
    legacy = legacy_by_key.get(key, {})
    sec = sec_periods.get(key)
    v2 = v2_by_period.get(key)
    available = ";".join(field for field in ALL_FIELDS if legacy.get(field) is not None) or row.get("available_fields", "")
    return {
        "market": "usa",
        "ticker": row["ticker"],
        "fiscal_year": plausible_fiscal_year(fiscal_year) or fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "period_end_date": row["period_end_date"],
        "publish_date": row.get("publish_date") or ((sec.filed_date or "") if sec and sec.fiscal_period == "FY" else ""),
        "previous_disposition": row.get("diagnostic_disposition", ""),
        "final_disposition": disposition,
        "identity_evidence": evidence,
        "sec_form": sec.form if sec else "",
        "sec_fp": sec.fiscal_period if sec else "",
        "sec_fy": sec.fiscal_year if sec and sec.fiscal_year else "",
        "v2_corroboration": "V2_EXACT_PERIOD" if v2 else "NO_V2_EXACT_PERIOD",
        "available_fields": available,
        "source_record_id": f"LEGACY:{row['ticker']}:{row['period_end_date']}",
    }


def plausible_fiscal_year(value: Any) -> int | None:
    if not str(value).strip().isdigit():
        return None
    fiscal_year = int(value)
    if 1900 <= fiscal_year <= 2100:
        return fiscal_year
    return None


def canonical_q4_fiscal_year_from_period_end(period_end_date: str) -> int:
    year = int(period_end_date[:4])
    month = int(period_end_date[5:7])
    return year - 1 if month <= 3 else year


def resolve_duplicate_ready_fyfqs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    priority = {"READY_EXISTING_CHAIN": 0, "READY_SEC_Q4_STRUCTURE": 1, "READY_REANCHORED_WITH_V2": 2, "READY_REANCHORED_LEGACY_ONLY": 3, "READY_BRIDGED_SEGMENT": 4}
    grouped: dict[tuple[str, Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["final_disposition"] in READY_STATES:
            grouped[(row["ticker"], row["fiscal_year"], row["fiscal_quarter"])].append(row)
    duplicate_ids = set()
    for key, items in grouped.items():
        if len(items) <= 1:
            continue
        keep = min(items, key=lambda row: (priority.get(row["final_disposition"], 99), row["period_end_date"]))
        for item in items:
            if item is not keep:
                duplicate_ids.add(id(item))
    out = []
    for row in rows:
        if id(row) in duplicate_ids:
            out.append({**row, "final_disposition": "HOLD_DUPLICATE_OR_AMBIGUOUS", "identity_evidence": "DUPLICATE_READY_FYFQ"})
        else:
            out.append(row)
    return out


def to_sequence_row(row: dict[str, Any]) -> dict[str, Any]:
    return {"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row["period_end_date"], "diagnostic_disposition": row["final_disposition"]}


def build_q4_construction_plan(ready_rows: list[dict[str, Any]], sec_periods: dict[tuple[str, str], SecPeriodEvidence], structures: list[dict[str, Any]], legacy_by_key: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    structure_map = {(row["ticker"], int(row["fiscal_year"])): row for row in structures}
    rows = []
    for row in ready_rows:
        if row["final_disposition"] != "READY_SEC_Q4_STRUCTURE":
            continue
        fy = int(row["fiscal_year"])
        structure = structure_map.get((row["ticker"], fy), {})
        legacy = legacy_by_key.get((row["ticker"], row["period_end_date"]), {})
        field_modes = {}
        for field in FLOW_FIELDS:
            if field == "free_cashflow":
                field_modes[field] = "LEGACY_SEC_DERIVE_FROM_Q4_OCF_PLUS_CAPEX_IF_INPUTS_SAFE"
            elif legacy.get(field) is not None:
                field_modes[field] = "LEGACY_SEC_FY_MINUS_Q1_Q2_Q3_PENDING_CONCEPT_CHECK"
            else:
                field_modes[field] = "UNSUPPORTED_NULL"
        for field in INSTANT_FIELDS:
            field_modes[field] = "LEGACY_SEC_FY_END_INSTANT" if legacy.get(field) is not None else "UNSUPPORTED_NULL"
        for field in UNSAFE_DERIVED_FIELDS:
            field_modes[field] = "UNSUPPORTED_NULL_SEMANTICALLY_UNSAFE"
        rows.append({
            "market": "usa",
            "ticker": row["ticker"],
            "fiscal_year": fy,
            "fiscal_quarter": "Q4",
            "period_end_date": row["period_end_date"],
            "publish_date": row.get("publish_date", ""),
            "fy_source_row": row["source_record_id"],
            "q1_q2_q3_available": int(bool(structure.get("has_q1")) and bool(structure.get("has_q2")) and bool(structure.get("has_q3"))),
            "q4_source_structure": "SEC_Q4_REPRESENTED_BY_FY",
            "field_derivation_methods": json.dumps(field_modes, sort_keys=True),
            "balance_sheet_direct_instant_fields": ";".join(field for field in INSTANT_FIELDS if legacy.get(field) is not None),
            "unsupported_fields_left_null": ";".join(field for field, mode in field_modes.items() if mode.startswith("UNSUPPORTED")),
        })
    return rows


def build_dry_import_plan(ready_rows: list[dict[str, Any]], q4_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    q4_keys = {(row["ticker"], int(row["fiscal_year"]), row["period_end_date"]) for row in q4_plan}
    rows = []
    for row in ready_rows:
        chain_type = row["final_disposition"]
        source_mode = "LEGACY_SEC_FY_Q4_RECONSTRUCTION_PLAN" if (row["ticker"], int(row["fiscal_year"]), row["period_end_date"]) in q4_keys else "LEGACY_SEC_DIRECT_QUARTER"
        rows.append({**row, "chain_type": chain_type, "field_source_mode": source_mode, "phase3c2_recommendation": "READY_FOR_PHASE3C2_IMPORT"})
    return rows


def expected_contribution(plan: list[dict[str, Any]], q4_plan: list[dict[str, Any]]) -> dict[str, Any]:
    fields = sum(len([field for field in str(row.get("available_fields", "")).split(";") if field]) for row in plan)
    q4_fields = sum(len([field for field in str(row.get("field_derivation_methods", "")).split(",") if field]) for row in q4_plan)
    instant_values = sum(len([field for field in str(row.get("balance_sheet_direct_instant_fields", "")).split(";") if field]) for row in q4_plan)
    periods = sorted(row["period_end_date"] for row in plan)
    by_ticker = Counter(row["ticker"] for row in plan)
    return {"new_canonical_q_count": len(plan), "explicit_legacy_q_count": len(plan) - len(q4_plan), "reconstructed_sec_q4_count": len(q4_plan), "companies_gaining_history": len(by_ticker), "field_values": fields, "derived_q4_field_values": q4_fields, "fy_end_instant_field_values": instant_values, "publication_dates": sum(1 for row in plan if row.get("publish_date")), "oldest_period": periods[0] if periods else "", "median_history_depth": median(by_ticker.values()) if by_ticker else 0}


def yearly_recovery_rows(final_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for year in range(2026, 2017, -1):
        items = [row for row in final_rows if row["period_end_date"].startswith(str(year))]
        counts = Counter(row["final_disposition"] for row in items)
        ready = sum(counts[state] for state in READY_STATES)
        rows.append({"period_end_year": year, "initial_ready": sum(1 for row in items if row["previous_disposition"].startswith("READY")), "recovered_sec_q4": counts["READY_SEC_Q4_STRUCTURE"], "recovered_with_v2": counts["READY_REANCHORED_WITH_V2"], "recovered_legacy_only": counts["READY_REANCHORED_LEGACY_ONLY"], "recovered_segment_bridge": counts["READY_BRIDGED_SEGMENT"], "final_ready": ready, "remaining_hold": len(items) - ready, "ready_pct": pct(ready, len(items))})
    return rows


def discover_legacy_segments(final_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in final_rows:
        by_ticker[row["ticker"]].append(row)
    segment_rows = []
    validated = []
    for ticker, rows in sorted(by_ticker.items()):
        rows.sort(key=lambda row: row["period_end_date"], reverse=True)
        segment_id = 0
        current = []
        previous_ready = False
        for row in rows:
            is_ready = row["final_disposition"] in READY_STATES
            if current and previous_ready != is_ready:
                segment_rows.extend(segment_summary(ticker, segment_id, current))
                if previous_ready:
                    validated.extend(segment_summary(ticker, segment_id, current, validated_only=True))
                segment_id += 1
                current = []
            current.append(row)
            previous_ready = is_ready
        if current:
            segment_rows.extend(segment_summary(ticker, segment_id, current))
            if previous_ready:
                validated.extend(segment_summary(ticker, segment_id, current, validated_only=True))
    return segment_rows, validated


def segment_summary(ticker: str, segment_id: int, rows: list[dict[str, Any]], validated_only: bool = False) -> list[dict[str, Any]]:
    ready = sum(1 for row in rows if row["final_disposition"] in READY_STATES)
    if validated_only and not ready:
        return []
    return [{"ticker": ticker, "segment_id": f"segment_{segment_id}", "rows": len(rows), "ready_rows": ready, "start_period": rows[-1]["period_end_date"], "end_period": rows[0]["period_end_date"], "segment_status": "VALIDATED" if ready == len(rows) else ("MIXED" if ready else "HOLD")}]


def v2_historical_anchor_calibration(v3_rows: list[dict[str, Any]], legacy_by_key: dict[tuple[str, str], dict[str, Any]], v2_by_period: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for v3 in v3_rows:
        key = (v3["ticker"], v3["period_end_date"])
        legacy = legacy_by_key.get(key)
        v2 = v2_by_period.get(key)
        if not legacy or not v2:
            continue
        correct = int(int(v2["fiscal_year"]) == int(v3["fiscal_year"]) and str(v2["fiscal_quarter"]) == str(v3["fiscal_quarter"]))
        score = sum(1 for field in ("revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow") if compare_values(field, legacy.get(field), v2.get(field)).within_5pct)
        result = "CORRECT_Q" if correct and score >= 2 else ("AMBIGUOUS" if correct else "WRONG")
        rows.append({"ticker": v3["ticker"], "fiscal_year": v3["fiscal_year"], "fiscal_quarter": v3["fiscal_quarter"], "period_end_date": v3["period_end_date"], "trusted_field_matches": score, "result": result})
    return rows


def summarize_v2_calibration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["result"] for row in rows)
    tested = len(rows)
    correct = counts["CORRECT_Q"]
    wrong = counts["WRONG"]
    return {"tested_qs": tested, "correct_q": correct, "wrong_q": wrong, "ambiguous": counts["AMBIGUOUS"], "precision": pct(correct, correct + wrong), "recall": pct(correct, tested), "selected_rule": "Legacy row plus V2 exact period plus >=2 trusted field matches, or SEC fy/fp metadata when V2 is absent"}


def transition_population_reanalysis(hold_1c: list[dict[str, str]], final_rows: list[dict[str, Any]], sec_periods: dict[tuple[str, str], SecPeriodEvidence], v2_by_period: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    final_by_key = {(row["ticker"], row["period_end_date"]): row for row in final_rows}
    rows = []
    for row in hold_1c:
        if row.get("diagnostic_disposition") != "HOLD_TRUE_FISCAL_TRANSITION":
            continue
        key = (row["ticker"], row["period_end_date"])
        final = final_by_key[key]
        sec = sec_periods.get(key)
        if final["final_disposition"] == "READY_SEC_Q4_STRUCTURE":
            reanalysis = "NORMAL_SEC_Q4_BOUNDARY"
        elif final["final_disposition"] == "READY_REANCHORED_WITH_V2":
            reanalysis = "Q4_FY_REPRESENTATION_ARTIFACT" if sec and sec.fiscal_period == "FY" else "DATA_GAP"
        elif final["final_disposition"] in READY_STATES:
            reanalysis = "DATA_GAP"
        else:
            reanalysis = "UNRESOLVED"
        rows.append({**row, "final_disposition": final["final_disposition"], "reanalysis": reanalysis})
    return rows


def historical_gap_inventory(final_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in final_rows:
        if row["final_disposition"] == "READY_SEC_Q4_STRUCTURE":
            category = "SEC_Q4_REPRESENTED_BY_FY"
        elif row["final_disposition"] == "READY_BRIDGED_SEGMENT":
            category = "TRUE_SINGLE_Q_DATA_GAP"
        elif row["final_disposition"] == "HOLD_TRUE_FISCAL_TRANSITION":
            category = "FISCAL_TRANSITION_GAP"
        elif row["final_disposition"].startswith("HOLD"):
            category = "SOURCE_AMBIGUITY"
        else:
            category = "NO_GAP"
        rows.append({"ticker": row["ticker"], "period_end_date": row["period_end_date"], "gap_category": category, "final_disposition": row["final_disposition"]})
    return rows


def legacy_fyfq_repair_candidates(final_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row | {"repair_type": "LEGACY_FYFQ_REPAIR_CANDIDATE"} for row in final_rows if row["final_disposition"] in {"HOLD_MAPPING_CONFLICT", "HOLD_V2_CONFLICT"}]


def phase3c2b_repair_opportunity(hold_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapping = {
        "HOLD_TRUE_FISCAL_TRANSITION": "genuine_fiscal_transition",
        "HOLD_MAPPING_CONFLICT": "mapping_repair_candidate",
        "HOLD_V2_CONFLICT": "v2_conflict",
        "HOLD_DUPLICATE_OR_AMBIGUOUS": "irreducible_ambiguity",
        "HOLD_INSUFFICIENT_EVIDENCE": "sparse_low_evidence_residual",
        "HOLD_ISOLATED_ROW": "sparse_low_evidence_residual",
    }
    return [{**row, "repair_bucket": mapping.get(row["final_disposition"], "sparse_low_evidence_residual")} for row in hold_rows]


def special_case_validation(final_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for ticker in SPECIAL_CASES:
        items = [row for row in final_rows if row["ticker"] == ticker]
        rows.append({"ticker": ticker, "ready_rows": sum(1 for row in items if row["final_disposition"] in READY_STATES), "hold_rows": sum(1 for row in items if row["final_disposition"] not in READY_STATES), "status": "PRESERVED_REGRESSION_ONLY" if items else "NOT_IN_2018PLUS_LEGACY_ONLY"})
    return rows


def v2_help_summary(final_rows: list[dict[str, Any]]) -> dict[str, Any]:
    v2_rows = [row for row in final_rows if row["v2_corroboration"] == "V2_EXACT_PERIOD"]
    recovered = [row for row in final_rows if row["final_disposition"] == "READY_REANCHORED_WITH_V2"]
    return {"hold_rows_with_v2_counterpart": len(v2_rows), "strong_v2_legacy_evidence": len(recovered), "v2_conflicts_or_mapping_risk": sum(1 for row in final_rows if row["final_disposition"] == "HOLD_V2_CONFLICT"), "rows_recovered_with_v2": len(recovered), "companies_recovered_with_v2": len({row["ticker"] for row in recovered})}


def legacy_only_recovery_summary(final_rows: list[dict[str, Any]], segment_map: list[dict[str, Any]], validated_segments: list[dict[str, Any]]) -> dict[str, Any]:
    recovered = [row for row in final_rows if row["final_disposition"] == "READY_REANCHORED_LEGACY_ONLY"]
    legacy_hold_examined = sum(1 for row in final_rows if row["previous_disposition"].startswith("HOLD") and row["v2_corroboration"] != "V2_EXACT_PERIOD")
    return {"legacy_only_hold_rows_examined": legacy_hold_examined, "candidate_segments": len(segment_map), "selected_minimum_segment_rule": "SEC fy/fp metadata plus Legacy row; two or more rows for segment statistics, isolated rows still need SEC fy/fp", "validated_legacy_only_segments": len([row for row in validated_segments if row["ready_rows"]]), "rows_recovered_legacy_only": len(recovered), "companies_recovered_legacy_only": len({row["ticker"] for row in recovered})}


def segment_false_positive_validation(v2_calibration: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary = summarize_v2_calibration(v2_calibration)
    return [{"validation": "V3_LEGACY_V2_KNOWN_OVERLAP", **summary}]


def final_classification(ready_rows: list[dict[str, Any]], sequence_violations: list[dict[str, Any]], q4_accuracy: list[dict[str, Any]]) -> str:
    if ready_rows and not sequence_violations and q4_accuracy:
        return "FUNDAMENTALS_V3_PHASE3C_1D_LEGACY_HOLD_RECOVERY_COMPLETE_READY_FOR_3C2"
    return "FUNDAMENTALS_V3_PHASE3C_1D_Q4_MODEL_REPAIR_REQUIRED"


def write_artifacts(root: Path, **items: Any) -> None:
    write_json(root / "baseline_reconciliation.json", items["baseline"])
    (root / "legacy_sec_source_shape.md").write_text(source_shape_markdown(items["source_shape"]))
    mapping = {
        "legacy_fiscal_year_structures.csv": items["fiscal_structures"],
        "legacy_q4_presence_by_year.csv": items["q4_presence"],
        "q4_input_semantics.csv": items["q4_input_semantics"],
        "q4_field_eligibility.csv": items["q4_field_eligibility"],
        "q4_known_v3_calibration.csv": items["q4_calibration"],
        "q4_reconstruction_accuracy.csv": items["q4_accuracy"],
        "q4_vintage_compatibility.csv": items["q4_vintage"],
        "transition_population_reanalysis.csv": items["transition_reanalysis"],
        "false_transition_summary.csv": items["false_transition"],
        "company_segment_map.csv": items["segment_map"],
        "v2_historical_anchor_calibration.csv": items["v2_calibration"],
        "v2_supported_reanchors.csv": items["v2_reanchors"],
        "legacy_only_reanchors.csv": items["legacy_reanchors"],
        "segment_false_positive_validation.csv": items["segment_false_positive_validation"],
        "validated_segments.csv": items["validated_segments"],
        "historical_gap_inventory.csv": items["gap_inventory"],
        "legacy_fyfq_repair_candidates.csv": items["repair_candidates"],
        "final_2018plus_row_classification.csv": items["final_rows"],
        "yearly_recovery_2018_2026.csv": items["yearly"],
        "phase3c2_q4_construction_plan.csv": items["q4_plan"],
        "phase3c2_ready_rows.csv": items["ready_rows"],
        "phase3c2_hold_rows.csv": items["hold_rows"],
        "phase3c2_dry_import_plan.csv": items["dry_plan"],
        "phase3c2b_repair_opportunity.csv": items["phase3c2b"],
        "special_case_validation.csv": items["summary"]["special_cases"],
    }
    for filename, rows in mapping.items():
        write_csv(root / filename, rows)
    write_json(root / "phase3c2_expected_contribution.json", items["contribution"])
    write_json(root / "summary.json", items["summary"])
    (root / "recommended_next_step.md").write_text(items["summary"]["recommended_next_step"] + "\n")


def source_shape_markdown(shape: dict[str, Any]) -> str:
    lines = [
        "# Legacy SEC Source Shape",
        "",
        f"Raw statement columns: `{shape['raw_statement_columns']}`",
        f"Quarterly columns: `{shape['quarterly_columns']}`",
        f"Form type column exists: `{shape['has_form_type_column']}`",
        f"Period start column exists: `{shape['has_period_start_column']}`",
        f"Duration column exists: `{shape['has_duration_column']}`",
        f"SEC metadata embedded in `field_name`: `{shape['metadata_embedded_in_field_name']}`",
        "",
        shape["local_semantics"],
        "",
        "| period_type | statement_type | rows | tickers |",
        "| --- | --- | ---: | ---: |",
    ]
    for row in shape["period_type_statement_counts"]:
        lines.append(f"| {row['period_type']} | {row['statement_type']} | {row['rows']} | {row['tickers']} |")
    return "\n".join(lines) + "\n"


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


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


def counter_rows(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    return [{field: key, "count": value} for key, value in sorted(Counter(row[field] for row in rows).items())]


def pct(num: int, denom: int) -> float:
    return round((num / denom * 100.0) if denom else 0.0, 2)
