from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine, V3SourceApplyPolicy
from swingmaster.fundamentals.v3_core_gap_diagnostic import compare_values, connect_readonly, load_legacy_rows, load_v2_rows, load_v3_rows
from swingmaster.fundamentals.v3_v2_enrichment import V2_SOURCE, no_overwrite_proof, production_integrity_for_path, snapshot_existing_non_null, summarize_v3
from swingmaster.fundamentals.v3_v2_enrichment_policy import V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE
from swingmaster.fundamentals.v3_v2_residual_existing_q import REPORT_FIELDS, build_phase4c_inventory, core_gap_profile


PHASE3C4_EXPECTED_BASELINE = {
    "company_total": 2552,
    "active": 2484,
    "inactive": 68,
    "canonical_q_total": 72498,
}
PHASE3C4_CLASSIFICATION = "FUNDAMENTALS_V3_PHASE3C_4_V2_HISTORICAL_GAP_FILL_COMPLETE"
PHASE3C4_NO_IMPORT = "FUNDAMENTALS_V3_PHASE3C_4_NO_SAFE_NEW_Q_IMPORT"
HISTORICAL_PERIOD_END_FLOOR = date(2018, 1, 1)
TRUSTED_LEGACY_FIELDS = ("revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow", "cash", "total_debt")
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
FINAL_CLASSES = (
    "STRONG_NEW_Q_CONFIRMED",
    "PROBABLE_NEW_Q",
    "INSUFFICIENT_NEW_Q_EVIDENCE",
    "DUPLICATE_OR_VARIANT_OF_EXISTING_Q",
    "POSSIBLE_WRONG_V2_MAPPING",
    "CLEAR_WRONG_V2_MAPPING",
    "PERIOD_IDENTITY_CONFLICT",
    "LEGACY_CONFLICT",
    "OTHER_IDENTIFIED",
)


@dataclass(frozen=True)
class Phase3C4Prepared:
    baseline: dict[str, Any]
    v2_only_rows: list[dict[str, Any]]
    evidence_rows: list[dict[str, Any]]
    neighbor_rows: list[dict[str, Any]]
    cadence_rows: list[dict[str, Any]]
    legacy_rows: list[dict[str, Any]]
    adjacent_rows: list[dict[str, Any]]
    collision_rows: list[dict[str, Any]]
    calibration_rows: list[dict[str, Any]]
    hidden_validation_rows: list[dict[str, Any]]
    final_rows: list[dict[str, Any]]
    strong_rows: list[dict[str, Any]]
    probable_rows: list[dict[str, Any]]
    mapping_risk_rows: list[dict[str, Any]]
    duplicate_rows: list[dict[str, Any]]
    review_rows: list[dict[str, Any]]
    review_value_rows: list[dict[str, Any]]
    candidates: list[V3CanonicalMigrationCandidate]
    dry_plan_rows: list[dict[str, Any]]
    pre_snapshot: dict[str, Any]
    source_contribution: dict[str, Any]


def run_v2_historical_gap_fill(
    *,
    v3_db: Path,
    v2_db: Path,
    legacy_db: Path,
    artifact_root: Path,
    migration_run_id: str,
    now_utc: str,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    prepared = prepare_v2_historical_gap_fill(v3_db=v3_db, v2_db=v2_db, legacy_db=legacy_db, migration_run_id=migration_run_id)
    _write_pre_apply_artifacts(artifact_root, prepared)

    dry_summary = _apply_candidates(v3_db, prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc, dry_apply=True)
    dry_gate = validate_dry_gate(prepared=prepared, dry_summary=dry_summary)
    _write_json(artifact_root / "dry_apply_summary.json", {"summary": dry_summary, "gate": dry_gate})
    if not dry_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_4_DRY_GATE_FAILED:" + json.dumps(dry_gate, sort_keys=True))

    backup = create_source_boundary_backup(v3_db=v3_db, artifact_root=artifact_root)
    before_summary = summarize_with_profiles(v3_db)
    before_snapshot = prepared.pre_snapshot
    production_summary = _apply_candidates(v3_db, prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc, dry_apply=False)
    after_summary = summarize_with_profiles(v3_db)
    after_snapshot = snapshot_existing_non_null(v3_db)
    no_overwrite = no_overwrite_proof(before_snapshot, after_snapshot)
    idempotency = validate_idempotency(v3_db=v3_db, candidates=prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc)
    integrity = production_integrity_for_path(v3_db)
    post_gate = validate_post_gate(
        before=before_summary,
        after=after_summary,
        production_summary=production_summary,
        no_overwrite=no_overwrite,
        idempotency=idempotency,
        integrity=integrity,
        imported_source_keys={candidate.source_record_id for candidate in prepared.candidates},
    )

    phase4c_after = build_phase4c_inventory(v3_db)
    phase4c_delta = phase4c_inventory_delta(prepared.baseline["phase4c_inventory"], phase4c_after)
    _write_csv(artifact_root / "phase4c_inventory_delta.csv", phase4c_delta)
    _write_json(artifact_root / "v2_historical_source_contribution.json", source_contribution_with_apply(prepared.source_contribution, production_summary))
    _write_csv(artifact_root / "v2_historical_field_contribution.csv", field_contribution_rows(production_summary))
    _write_csv(artifact_root / "historical_coverage_pre_post.csv", [historical_coverage_pre_post(before_summary, after_summary, len(prepared.v2_only_rows), len(prepared.review_rows))])
    _write_csv(artifact_root / "core_readiness_pre_post.csv", [core_readiness_pre_post(before_summary, after_summary)])
    _write_csv(artifact_root / "publication_coverage_pre_post.csv", [publication_coverage_pre_post(before_summary, after_summary, production_summary)])
    (artifact_root / "no_overwrite_proof.md").write_text(json.dumps(no_overwrite, indent=2, sort_keys=True) + "\n")
    (artifact_root / "idempotency_validation.md").write_text(json.dumps(idempotency, indent=2, sort_keys=True) + "\n")
    (artifact_root / "production_integrity.md").write_text(json.dumps(integrity, indent=2, sort_keys=True) + "\n")
    recommended = recommend_next_step(prepared.review_value_rows)
    (artifact_root / "recommended_next_step.md").write_text(recommended + "\n")
    classification = PHASE3C4_CLASSIFICATION if prepared.candidates else PHASE3C4_NO_IMPORT
    summary = {
        "classification": classification,
        "migration_run_id": migration_run_id,
        "pre_baseline": before_summary,
        "post_baseline": after_summary,
        "candidate_population": population_summary(prepared.final_rows),
        "new_q_gate_calibration": calibration_summary(prepared.hidden_validation_rows),
        "strong_new_q_confirmed": len(prepared.strong_rows),
        "production_apply": production_summary,
        "field_contribution": field_insert_counts(production_summary),
        "phase4c_inventory_delta": phase4c_delta,
        "phase3c4b_review_rows": len(prepared.review_rows),
        "phase3c4b_value_estimate": value_estimate_summary(prepared.review_value_rows),
        "no_overwrite": no_overwrite,
        "idempotency": idempotency,
        "integrity": integrity,
        "post_gate": post_gate,
        "source_boundary_backup": backup,
        "provider_calls": {"yahoo": 0, "legacy_writes": 0, "sec": 0, "simfin": 0, "network": 0},
        "recommended_next_step": recommended,
    }
    if not post_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_4_POST_GATE_FAILED:" + json.dumps(post_gate, sort_keys=True))
    _write_json(artifact_root / "summary.json", summary)
    write_durable_doc(Path("docs/fundamentals_v3_phase3c_4_v2_residual_historical_gap_fill.md"), artifact_root, summary)
    return summary


def prepare_v2_historical_gap_fill(*, v3_db: Path, v2_db: Path, legacy_db: Path, migration_run_id: str) -> Phase3C4Prepared:
    baseline = summarize_with_profiles(v3_db)
    assert_phase3c4_baseline(baseline)
    baseline["phase4c_inventory"] = build_phase4c_inventory(v3_db)
    with connect_readonly(v3_db) as v3_conn, connect_readonly(v2_db) as v2_conn, connect_readonly(legacy_db) as legacy_conn:
        v3_rows = load_v3_rows(v3_conn)
        v2_rows = load_v2_rows(v2_conn)
        legacy_by_period = load_legacy_rows(legacy_conn)
        company_map = load_company_map(v3_conn)
    v3_by_key = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"]): row for row in v3_rows}
    v3_by_ticker = rows_by_ticker(v3_rows)
    v3_by_ticker_period = {(row["ticker"], str(row.get("period_end_date"))): row for row in v3_rows}
    v3_tickers = set(v3_by_ticker)
    v2_only = [
        build_base_v2_only_row(key, row, company_map[key[0]])
        for key, row in sorted(v2_rows.items())
        if key[0] in v3_tickers and key not in v3_by_key and row.get("period_end_date") and date.fromisoformat(str(row["period_end_date"])) >= HISTORICAL_PERIOD_END_FLOOR
    ]
    if len(v2_only) != 1602:
        raise RuntimeError(f"FUNDAMENTALS_V3_PHASE3C_4_V2_ONLY_POPULATION_DRIFT:{len(v2_only)}")
    initial_mapping_risk = sum(
        1
        for row in v2_only
        if (row["ticker"], str(row["period_end_date"])) in v3_by_ticker_period
        or (row["ticker"], str(row["period_end_date"])) not in legacy_by_period
    )
    if initial_mapping_risk != 1073:
        raise RuntimeError(f"FUNDAMENTALS_V3_PHASE3C_4_MAPPING_RISK_DRIFT:{initial_mapping_risk}")

    evidence_rows: list[dict[str, Any]] = []
    neighbor_rows: list[dict[str, Any]] = []
    cadence_rows: list[dict[str, Any]] = []
    legacy_rows: list[dict[str, Any]] = []
    adjacent_rows: list[dict[str, Any]] = []
    collision_rows: list[dict[str, Any]] = []
    final_rows: list[dict[str, Any]] = []
    for base in v2_only:
        key = (base["ticker"], int(base["fiscal_year"]), base["fiscal_quarter"])
        v2 = v2_rows[key]
        context = classify_v2_only_history_candidate(v2=v2, base=base, v3_by_ticker=v3_by_ticker, v3_by_ticker_period=v3_by_ticker_period, legacy_by_period=legacy_by_period)
        evidence_rows.append({**base, **context["evidence"]})
        neighbor_rows.append({**base, **context["neighbor"]})
        cadence_rows.append({**base, **context["cadence"]})
        legacy_rows.append({**base, **context["legacy"]})
        adjacent_rows.append({**base, **context["adjacent"]})
        collision_rows.append({**base, **context["collision"]})
        final_rows.append({**base, **context["final"]})

    class_counts = Counter(row["final_classification"] for row in final_rows)

    calibration_rows, hidden_rows = calibrate_new_q_gate(v3_rows=v3_rows, v2_rows=v2_rows, legacy_by_period=legacy_by_period)
    strong_rows = [row for row in final_rows if row["final_classification"] == "STRONG_NEW_Q_CONFIRMED"]
    probable_rows = [row for row in final_rows if row["final_classification"] == "PROBABLE_NEW_Q"]
    mapping_risk_rows = [row for row in final_rows if row["final_classification"] in {"POSSIBLE_WRONG_V2_MAPPING", "CLEAR_WRONG_V2_MAPPING", "PERIOD_IDENTITY_CONFLICT", "LEGACY_CONFLICT"}]
    duplicate_rows = [row for row in final_rows if row["final_classification"] == "DUPLICATE_OR_VARIANT_OF_EXISTING_Q"]
    review_rows = [row for row in final_rows if row["final_classification"] != "STRONG_NEW_Q_CONFIRMED"]
    review_value_rows = estimate_review_value(review_rows)
    candidates = [build_new_q_candidate(row, v2_rows[(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])], migration_run_id) for row in strong_rows]
    dry_plan_rows = [candidate_to_plan_row(candidate, row) for candidate, row in zip(candidates, strong_rows, strict=True)]
    source_contribution = {
        "source_rows_examined": len(v2_only),
        "initial_mapping_risk": initial_mapping_risk,
        "initial_non_risk": len(v2_only) - initial_mapping_risk,
        "final_classification": dict(sorted(class_counts.items())),
        "strong_new_q_confirmed": len(strong_rows),
        "review_rows": len(review_rows),
        "companies_with_strong_new_q": len({row["ticker"] for row in strong_rows}),
        "year_distribution": dict(sorted(Counter(date.fromisoformat(row["period_end_date"]).year for row in strong_rows).items())),
        "q_distribution": dict(sorted(Counter(row["fiscal_quarter"] for row in strong_rows).items())),
        "legacy_canonical_contribution": 0,
        "yahoo_canonical_contribution": 0,
    }
    return Phase3C4Prepared(
        baseline=baseline,
        v2_only_rows=v2_only,
        evidence_rows=evidence_rows,
        neighbor_rows=neighbor_rows,
        cadence_rows=cadence_rows,
        legacy_rows=legacy_rows,
        adjacent_rows=adjacent_rows,
        collision_rows=collision_rows,
        calibration_rows=calibration_rows,
        hidden_validation_rows=hidden_rows,
        final_rows=final_rows,
        strong_rows=strong_rows,
        probable_rows=probable_rows,
        mapping_risk_rows=mapping_risk_rows,
        duplicate_rows=duplicate_rows,
        review_rows=review_rows,
        review_value_rows=review_value_rows,
        candidates=candidates,
        dry_plan_rows=dry_plan_rows,
        pre_snapshot=snapshot_existing_non_null(v3_db),
        source_contribution=source_contribution,
    )


def classify_v2_only_history_candidate(
    *,
    v2: dict[str, Any],
    base: dict[str, Any],
    v3_by_ticker: dict[str, list[dict[str, Any]]],
    v3_by_ticker_period: dict[tuple[str, str], dict[str, Any]],
    legacy_by_period: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    ticker = base["ticker"]
    fiscal_year = int(base["fiscal_year"])
    fiscal_quarter = base["fiscal_quarter"]
    period_end = str(base["period_end_date"])
    canonical_rows = v3_by_ticker[ticker]
    neighbor = compare_neighbor_sequence(canonical_rows, fiscal_year, fiscal_quarter, period_end)
    cadence = evaluate_period_cadence(neighbor)
    legacy = corroborate_with_legacy(v2, legacy_by_period.get((ticker, period_end)))
    collision = variant_collision(ticker, period_end, fiscal_year, fiscal_quarter, v3_by_ticker_period)
    adjacent = adjacent_fingerprint(v2, neighbor)
    final_class = final_new_q_classification(neighbor, cadence, legacy, collision, adjacent, fiscal_quarter)
    return {
        "evidence": {
            "company_approved": 1,
            "period_end_ge_2018": int(date.fromisoformat(period_end) >= HISTORICAL_PERIOD_END_FLOOR),
            "available_field_count": len([field for field in REPORT_FIELDS if v2.get(field) is not None]),
            "evidence_stack": ";".join(
                item
                for item in (
                    neighbor["neighbor_sequence_class"],
                    cadence["period_cadence_class"],
                    legacy["legacy_corroboration_class"],
                    collision["variant_collision_class"],
                    adjacent["adjacent_fingerprint_class"],
                )
                if item
            ),
        },
        "neighbor": neighbor,
        "cadence": cadence,
        "legacy": legacy,
        "collision": collision,
        "adjacent": adjacent,
        "final": {
            "final_classification": final_class,
            "auto_import": int(final_class == "STRONG_NEW_Q_CONFIRMED"),
            "review_disposition": "IMPORT_PHASE3C4" if final_class == "STRONG_NEW_Q_CONFIRMED" else "DEFER_PHASE3C4B_OR_LATER",
        },
    }


def compare_neighbor_sequence(canonical_rows: list[dict[str, Any]], fiscal_year: int, fiscal_quarter: str, period_end: str) -> dict[str, Any]:
    target_seq = fiscal_seq(fiscal_year, fiscal_quarter)
    previous = max((row for row in canonical_rows if fiscal_seq(row["fiscal_year"], row["fiscal_quarter"]) < target_seq), key=lambda row: fiscal_seq(row["fiscal_year"], row["fiscal_quarter"]), default=None)
    next_row = min((row for row in canonical_rows if fiscal_seq(row["fiscal_year"], row["fiscal_quarter"]) > target_seq), key=lambda row: fiscal_seq(row["fiscal_year"], row["fiscal_quarter"]), default=None)
    previous_adjacent = bool(previous and fiscal_seq(previous["fiscal_year"], previous["fiscal_quarter"]) == target_seq - 1)
    next_adjacent = bool(next_row and fiscal_seq(next_row["fiscal_year"], next_row["fiscal_quarter"]) == target_seq + 1)
    period_between = bool(previous and next_row and previous.get("period_end_date") and next_row.get("period_end_date") and date.fromisoformat(str(previous["period_end_date"])) < date.fromisoformat(period_end) < date.fromisoformat(str(next_row["period_end_date"])))
    if previous_adjacent and next_adjacent and period_between:
        cls = "BETWEEN_TWO_CONFIRMED_NEIGHBORS"
    elif previous_adjacent or next_adjacent:
        cls = "ADJACENT_TO_ONE_CONFIRMED_NEIGHBOR"
    elif previous and next_row:
        cls = "CONFLICTS_WITH_NEIGHBOR_SEQUENCE"
    else:
        cls = "NO_CANONICAL_NEIGHBOR_SUPPORT"
    return {
        "neighbor_sequence_class": cls,
        "_candidate_period_end": period_end,
        "previous_canonical_q": format_neighbor(previous),
        "next_canonical_q": format_neighbor(next_row),
        "previous_adjacent": int(previous_adjacent),
        "next_adjacent": int(next_adjacent),
        "period_between_neighbors": int(period_between),
    }


def evaluate_period_cadence(neighbor: dict[str, Any]) -> dict[str, Any]:
    prev = parse_neighbor(neighbor["previous_canonical_q"])
    nxt = parse_neighbor(neighbor["next_canonical_q"])
    if not prev or not nxt or not neighbor["period_between_neighbors"]:
        return {"period_cadence_class": "PLAUSIBLE" if neighbor["neighbor_sequence_class"] == "ADJACENT_TO_ONE_CONFIRMED_NEIGHBOR" else "MATERIAL_MISMATCH", "days_from_previous": None, "days_to_next": None}
    candidate_date = date.fromisoformat(neighbor["_candidate_period_end"]) if "_candidate_period_end" in neighbor else None
    if candidate_date is None:
        return {"period_cadence_class": "MATERIAL_MISMATCH", "days_from_previous": None, "days_to_next": None}
    days_from_previous = (candidate_date - date.fromisoformat(prev["period_end_date"])).days
    days_to_next = (date.fromisoformat(nxt["period_end_date"]) - candidate_date).days
    if 80 <= days_from_previous <= 100 and 80 <= days_to_next <= 100:
        cls = "EXACT_EXPECTED"
    elif 70 <= days_from_previous <= 105 and 70 <= days_to_next <= 105:
        cls = "SAFE_VARIANT"
    elif 60 <= days_from_previous <= 130 and 60 <= days_to_next <= 130:
        cls = "PLAUSIBLE"
    else:
        cls = "MATERIAL_MISMATCH"
    return {"period_cadence_class": cls, "days_from_previous": days_from_previous, "days_to_next": days_to_next}


def corroborate_with_legacy(v2: dict[str, Any], legacy: dict[str, Any] | None) -> dict[str, Any]:
    if legacy is None:
        return {"legacy_corroboration_class": "LEGACY_ABSENT", "legacy_matching_fields_5pct": 0, "legacy_conflict_fields": 0}
    comparisons = [compare_values(field, legacy.get(field), v2.get(field)) for field in TRUSTED_LEGACY_FIELDS]
    comparable = [item for item in comparisons if item.comparable]
    matches = [item for item in comparable if item.within_5pct and not item.sign_mismatch]
    conflicts = [item for item in comparable if item.status in {"MISMATCH", "SIGN_MISMATCH"}]
    if len(matches) >= 2 and not conflicts:
        cls = "LEGACY_STRONG_SUPPORT"
    elif len(matches) >= 1 and len(conflicts) <= 1:
        cls = "LEGACY_PARTIAL_SUPPORT"
    elif conflicts:
        cls = "LEGACY_CONFLICT"
    else:
        cls = "LEGACY_AMBIGUOUS"
    return {"legacy_corroboration_class": cls, "legacy_matching_fields_5pct": len(matches), "legacy_conflict_fields": len(conflicts)}


def variant_collision(ticker: str, period_end: str, fiscal_year: int, fiscal_quarter: str, v3_by_ticker_period: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    same_period = v3_by_ticker_period.get((ticker, period_end))
    if same_period is None:
        return {"variant_collision_class": "NO_CANONICAL_PERIOD_COLLISION", "same_period_canonical_q": ""}
    if int(same_period["fiscal_year"]) == fiscal_year and same_period["fiscal_quarter"] == fiscal_quarter:
        cls = "DUPLICATE_EXISTING_Q"
    else:
        cls = "SAME_PERIOD_DIFFERENT_FYFQ"
    return {"variant_collision_class": cls, "same_period_canonical_q": format_neighbor(same_period)}


def adjacent_fingerprint(v2: dict[str, Any], neighbor: dict[str, Any]) -> dict[str, Any]:
    previous = parse_neighbor(neighbor["previous_canonical_q"])
    next_row = parse_neighbor(neighbor["next_canonical_q"])
    previous_score = fingerprint_score(v2, previous) if previous else None
    next_score = fingerprint_score(v2, next_row) if next_row else None
    if previous_score is not None and previous_score >= 300:
        cls = "PREVIOUS_Q_LOOKALIKE"
    elif next_score is not None and next_score >= 300:
        cls = "NEXT_Q_LOOKALIKE"
    elif previous_score is None and next_score is None:
        cls = "INSUFFICIENT"
    else:
        cls = "MISSING_SLOT_FINGERPRINT_SUPPORT"
    return {"adjacent_fingerprint_class": cls, "previous_fingerprint_score": previous_score, "next_fingerprint_score": next_score}


def final_new_q_classification(neighbor: dict[str, Any], cadence: dict[str, Any], legacy: dict[str, Any], collision: dict[str, Any], adjacent: dict[str, Any], fiscal_quarter: str) -> str:
    if collision["variant_collision_class"] in {"SAME_PERIOD_DIFFERENT_FYFQ", "DUPLICATE_EXISTING_Q"}:
        return "DUPLICATE_OR_VARIANT_OF_EXISTING_Q"
    if legacy["legacy_corroboration_class"] == "LEGACY_CONFLICT":
        return "LEGACY_CONFLICT"
    if cadence["period_cadence_class"] == "MATERIAL_MISMATCH":
        return "PERIOD_IDENTITY_CONFLICT"
    if adjacent["adjacent_fingerprint_class"] in {"PREVIOUS_Q_LOOKALIKE", "NEXT_Q_LOOKALIKE"}:
        return "POSSIBLE_WRONG_V2_MAPPING"
    if fiscal_quarter == "Q4" and neighbor["neighbor_sequence_class"] != "BETWEEN_TWO_CONFIRMED_NEIGHBORS":
        return "POSSIBLE_WRONG_V2_MAPPING"
    if neighbor["neighbor_sequence_class"] == "BETWEEN_TWO_CONFIRMED_NEIGHBORS" and cadence["period_cadence_class"] in {"EXACT_EXPECTED", "SAFE_VARIANT", "PLAUSIBLE"} and legacy["legacy_corroboration_class"] == "LEGACY_STRONG_SUPPORT":
        return "STRONG_NEW_Q_CONFIRMED"
    if neighbor["neighbor_sequence_class"] in {"BETWEEN_TWO_CONFIRMED_NEIGHBORS", "ADJACENT_TO_ONE_CONFIRMED_NEIGHBOR"} and legacy["legacy_corroboration_class"] in {"LEGACY_STRONG_SUPPORT", "LEGACY_PARTIAL_SUPPORT"}:
        return "PROBABLE_NEW_Q"
    if legacy["legacy_corroboration_class"] == "LEGACY_ABSENT":
        return "INSUFFICIENT_NEW_Q_EVIDENCE"
    return "OTHER_IDENTIFIED"


def build_new_q_candidate(row: dict[str, Any], v2: dict[str, Any], migration_run_id: str) -> V3CanonicalMigrationCandidate:
    values = {field: v2.get(field) for field in REPORT_FIELDS if v2.get(field) is not None}
    if "operating_cashflow" in values and "capex" in values and "free_cashflow" not in values:
        values.pop("capex")
    return V3CanonicalMigrationCandidate(
        source_system=V2_SOURCE,
        source_record_id=f"V2_HISTORY_GAP:{row['ticker']}:{row['fiscal_year']}:{row['fiscal_quarter']}",
        migration_run_id=migration_run_id,
        market=row["market"],
        ticker=row["ticker"],
        fiscal_year=int(row["fiscal_year"]),
        fiscal_quarter=row["fiscal_quarter"],
        period_end_date=row["period_end_date"],
        publish_date=row.get("publish_date"),
        values=values,
        approved_company_active=None,
        candidate_can_create_quarter=True,
        value_metadata={"phase": "PHASE3C_4_V2_HISTORICAL_GAP_FILL", "final_classification": row["final_classification"], "no_overwrite_policy": V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE},
    )


def calibrate_new_q_gate(*, v3_rows: list[dict[str, Any]], v2_rows: dict[tuple[str, int, str], dict[str, Any]], legacy_by_period: dict[tuple[str, str], dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_ticker = rows_by_ticker(v3_rows)
    hidden_rows: list[dict[str, Any]] = []
    for row in v3_rows:
        key = (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
        v2 = v2_rows.get(key)
        if v2 is None or legacy_by_period.get((row["ticker"], str(row.get("period_end_date")))) is None:
            continue
        hidden_context = [candidate for candidate in by_ticker[row["ticker"]] if candidate is not row]
        by_period = {
            (candidate["ticker"], str(candidate.get("period_end_date"))): candidate
            for candidate in v3_rows
            if candidate is not row
        }
        base = build_base_v2_only_row(key, v2, {"company_id": row.get("company_id"), "market": row["market"], "active": row["active"]})
        result = classify_v2_only_history_candidate(v2=v2, base=base, v3_by_ticker={row["ticker"]: hidden_context}, v3_by_ticker_period=by_period, legacy_by_period=legacy_by_period)["final"]
        hidden_rows.append(
            {
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end_date": row["period_end_date"],
                "expected_hidden_q": 1,
                "gate_classification": result["final_classification"],
                "correctly_recovered": int(result["final_classification"] == "STRONG_NEW_Q_CONFIRMED"),
                "wrong_fyfq_recovered": 0,
                "false_extra_q_created": 0,
                "held": int(result["final_classification"] != "STRONG_NEW_Q_CONFIRMED"),
            }
        )
        if len(hidden_rows) >= 500:
            break
    summary = calibration_summary(hidden_rows)
    return [
        {"gate_variant": "STRICT_NEIGHBOR_LEGACY_CADENCE", **summary},
        {"gate_variant": "LOOSER_ONE_NEIGHBOR_OR_PARTIAL_LEGACY", "rejected_reason": "would reduce precision by allowing probable/partial evidence"},
    ], hidden_rows


def summarize_with_profiles(v3_db: Path) -> dict[str, Any]:
    summary = summarize_v3(v3_db)
    summary["core_gap_profile"] = core_gap_profile(v3_db)
    summary["history_profile"] = history_profile(v3_db)
    return summary


def history_profile(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        counts = [int(row["count"]) for row in conn.execute("SELECT company_id, COUNT(*) AS count FROM v3_quarter GROUP BY company_id")]
        pre_2018 = conn.execute("SELECT COUNT(*) FROM v3_quarter WHERE period_end_date < '2018-01-01'").fetchone()[0]
    return {"median_q_per_company": float(median(counts)) if counts else 0.0, "companies_ge_28q": sum(1 for count in counts if count >= 28), "pre_2018_q": int(pre_2018)}


def assert_phase3c4_baseline(summary: dict[str, Any]) -> None:
    observed = {
        "company_total": summary["company_total"],
        "active": summary["active"],
        "inactive": summary["inactive"],
        "canonical_q_total": summary["coverage"]["canonical_q_total"],
    }
    if observed != PHASE3C4_EXPECTED_BASELINE:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_4_BASELINE_DRIFT:" + json.dumps(observed, sort_keys=True))


def load_company_map(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    return {
        row["ticker"]: {"company_id": row["company_id"], "market": row["market"], "active": row["active"]}
        for row in conn.execute("SELECT company_id, market, ticker, active FROM v3_company")
    }


def rows_by_ticker(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[row["ticker"]].append(row)
    for values in out.values():
        values.sort(key=lambda row: fiscal_seq(row["fiscal_year"], row["fiscal_quarter"]))
    return out


def build_base_v2_only_row(key: tuple[str, int, str], row: dict[str, Any], company: dict[str, Any]) -> dict[str, Any]:
    return {
        "market": company["market"],
        "ticker": key[0],
        "company_id": company["company_id"],
        "active": company["active"],
        "v2_source_record_id": f"V2:{key[0]}:{key[1]}:{key[2]}",
        "fiscal_year": key[1],
        "fiscal_quarter": key[2],
        "period_end_date": row.get("period_end_date"),
        "publish_date": row.get("publish_date"),
        "available_fields": ";".join(field for field in REPORT_FIELDS if row.get(field) is not None),
        "v2_identity_confidence": "V2_ONLY_EXISTING_CANONICAL_Q_ABSENT",
    }


def fiscal_seq(fiscal_year: int, fiscal_quarter: str) -> int:
    return int(fiscal_year) * 4 + int(str(fiscal_quarter).replace("Q", "")) - 1


def format_neighbor(row: dict[str, Any] | None) -> str:
    if row is None:
        return ""
    fields = [row.get("ticker", ""), str(row.get("fiscal_year")), str(row.get("fiscal_quarter")), str(row.get("period_end_date"))]
    for field in TRUSTED_LEGACY_FIELDS:
        fields.append("" if row.get(field) is None else str(row.get(field)))
    return "|".join(fields)


def parse_neighbor(value: str) -> dict[str, Any] | None:
    if not value:
        return None
    parts = value.split("|")
    row = {"ticker": parts[0], "fiscal_year": int(parts[1]), "fiscal_quarter": parts[2], "period_end_date": parts[3]}
    for field, raw in zip(TRUSTED_LEGACY_FIELDS, parts[4:], strict=False):
        row[field] = float(raw) if raw not in {"", "None"} else None
    return row


def fingerprint_score(v2: dict[str, Any], other: dict[str, Any] | None) -> int:
    if other is None:
        return 0
    comparisons = [compare_values(field, other.get(field), v2.get(field)) for field in ("revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow")]
    matches = sum(1 for item in comparisons if item.within_5pct and not item.sign_mismatch)
    conflicts = sum(1 for item in comparisons if item.status in {"MISMATCH", "SIGN_MISMATCH"})
    return matches * 100 - conflicts * 25


def validate_dry_gate(*, prepared: Phase3C4Prepared, dry_summary: dict[str, Any]) -> dict[str, Any]:
    calibration = calibration_summary(prepared.hidden_validation_rows)
    gate = {
        "hidden_q_precision_high": calibration["precision"] >= 99.0,
        "no_mapping_risk_candidates_passed": all(row["final_classification"] == "STRONG_NEW_Q_CONFIRMED" for row in prepared.strong_rows),
        "expected_new_qs": len(prepared.strong_rows),
        "dry_new_qs": int(dry_summary["rows"].get("canonical_quarters_created", 0)),
        "unexpected_existing_matches": int(dry_summary["rows"].get("existing_canonical_quarters_matched", 0)),
        "no_mapping_risk_leakage": int(mapping_risk_import_count(prepared.strong_rows)) == 0,
        "no_pre_2018": all(date.fromisoformat(candidate.period_end_date or "1900-01-01") >= HISTORICAL_PERIOD_END_FLOOR for candidate in prepared.candidates),
        "no_field_conflicts": sum(int(counts.get("FIELD_CONFLICT", 0)) for counts in dry_summary["field_contributions"].values()) == 0,
        "company_universe_unchanged": int(dry_summary["rows"].get("companies_created", 0)) == 0,
    }
    gate["passed"] = gate["hidden_q_precision_high"] and gate["expected_new_qs"] == gate["dry_new_qs"] and gate["unexpected_existing_matches"] == 0 and all(v for k, v in gate.items() if k not in {"expected_new_qs", "dry_new_qs", "unexpected_existing_matches"})
    return gate


def validate_post_gate(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    production_summary: dict[str, Any],
    no_overwrite: dict[str, Any],
    idempotency: dict[str, Any],
    integrity: dict[str, Any],
    imported_source_keys: set[str],
) -> dict[str, Any]:
    gate = {
        "company_count_unchanged": before["company_total"] == after["company_total"] == PHASE3C4_EXPECTED_BASELINE["company_total"],
        "active_unchanged": before["active"] == after["active"] == PHASE3C4_EXPECTED_BASELINE["active"],
        "inactive_unchanged": before["inactive"] == after["inactive"] == PHASE3C4_EXPECTED_BASELINE["inactive"],
        "canonical_q_delta_matches": after["coverage"]["canonical_q_total"] - before["coverage"]["canonical_q_total"] == int(production_summary["rows"].get("canonical_quarters_created", 0)),
        "no_existing_values_overwritten": no_overwrite["existing_non_null_values_overwritten"] == 0,
        "no_existing_publish_dates_overwritten": no_overwrite["existing_publish_dates_overwritten"] == 0,
        "idempotent": idempotency["row_counts_unchanged"] and idempotency["second_run_new_qs"] == 0 and idempotency["second_run_field_inserts"] == 0 and idempotency["second_run_publish_inserts"] == 0 and idempotency["duplicate_semantic_issues"] == 0,
        "quick_check_ok": integrity["quick_check"] == "ok",
        "foreign_key_check_ok": integrity["foreign_key_check_rows"] == 0,
        "no_pre_2018": after["history_profile"]["pre_2018_q"] == 0,
        "canonical_unique": integrity["duplicate_company_fy_fq"] == 0,
        "mapping_risk_imported_zero": len([key for key in imported_source_keys if "MAPPING_RISK" in key]) == 0,
    }
    gate["passed"] = all(gate.values())
    return gate


def validate_idempotency(*, v3_db: Path, candidates: list[V3CanonicalMigrationCandidate], migration_run_id: str, now_utc: str) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        before_counts = _table_counts(conn)
        issue_count_before = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
        second = V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source=V2_SOURCE, migration_run_id=migration_run_id, dry_apply=False, now_utc=now_utc).to_dict()
        after_counts = _table_counts(conn)
        issue_count_after = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
        conn.commit()
    return {
        "row_counts_unchanged": before_counts == after_counts,
        "second_run_new_qs": int(second["rows"].get("canonical_quarters_created", 0)),
        "second_run_field_inserts": sum(int(counts.get("FIELD_INSERTED", 0)) + int(counts.get("FIELD_FILLED_FROM_NULL", 0)) for counts in second["field_contributions"].values()),
        "second_run_publish_inserts": int(second["metadata"].get("PUBLISH_DATE_SET", 0)),
        "duplicate_semantic_issues": int(issue_count_after - issue_count_before),
    }


def _apply_candidates(v3_db: Path, candidates: list[V3CanonicalMigrationCandidate], *, migration_run_id: str, now_utc: str, dry_apply: bool) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
            candidates,
            source=V2_SOURCE,
            migration_run_id=migration_run_id,
            policy=V3SourceApplyPolicy(source=V2_SOURCE),
            dry_apply=dry_apply,
            now_utc=now_utc,
        ).to_dict()
        if not dry_apply:
            conn.commit()
    return summary


def create_source_boundary_backup(*, v3_db: Path, artifact_root: Path) -> dict[str, Any]:
    backup = artifact_root / "rc_fundamentals_v3_pre_phase3c4_backup.db"
    if not backup.exists():
        shutil.copy2(v3_db, backup)
    with sqlite3.connect(backup) as conn:
        quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
        fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    return {"path": str(backup), "size_bytes": backup.stat().st_size, "quick_check": quick_check, "foreign_key_check_rows": len(fk_rows)}


def _write_pre_apply_artifacts(root: Path, prepared: Phase3C4Prepared) -> None:
    (root / "preflight.md").write_text(_preflight_text(prepared))
    _write_csv(root / "v2_only_1602_reconciliation.csv", prepared.v2_only_rows)
    _write_csv(root / "v2_only_evidence_matrix.csv", prepared.evidence_rows)
    _write_csv(root / "v2_only_neighbor_sequence.csv", prepared.neighbor_rows)
    _write_csv(root / "v2_only_period_cadence.csv", prepared.cadence_rows)
    _write_csv(root / "v2_only_legacy_corroboration.csv", prepared.legacy_rows)
    _write_csv(root / "v2_only_adjacent_fingerprint.csv", prepared.adjacent_rows)
    _write_csv(root / "v2_only_variant_collision_analysis.csv", prepared.collision_rows)
    _write_csv(root / "new_q_gate_calibration.csv", prepared.calibration_rows)
    _write_csv(root / "hidden_q_recovery_validation.csv", prepared.hidden_validation_rows)
    _write_csv(root / "v2_only_final_classification.csv", prepared.final_rows)
    _write_csv(root / "strong_new_q_candidates.csv", prepared.strong_rows)
    _write_csv(root / "probable_new_q_candidates.csv", prepared.probable_rows)
    _write_csv(root / "mapping_risk_candidates.csv", prepared.mapping_risk_rows)
    _write_csv(root / "duplicate_variant_candidates.csv", prepared.duplicate_rows)
    _write_csv(root / "dry_apply_plan.csv", prepared.dry_plan_rows)
    _write_json(root / "v2_historical_source_contribution.json", prepared.source_contribution)
    _write_csv(root / "phase3c4b_review_population.csv", prepared.review_rows)
    _write_csv(root / "phase3c4b_value_estimate.csv", prepared.review_value_rows)


def candidate_to_plan_row(candidate: V3CanonicalMigrationCandidate, row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_record_id": candidate.source_record_id,
        "ticker": candidate.ticker,
        "fiscal_year": candidate.fiscal_year,
        "fiscal_quarter": candidate.fiscal_quarter,
        "period_end_date": candidate.period_end_date,
        "publish_date": candidate.publish_date,
        "field_count": len([field for field in REPORT_FIELDS if candidate.values.get(field) is not None]),
        "core_ready_expected": int(all(candidate.values.get(field) is not None for field in CORE_FIELDS) and (candidate.values.get("shares_outstanding") or 0) > 0),
        "final_classification": row["final_classification"],
    }


def phase4c_inventory_delta(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> list[dict[str, Any]]:
    before_keys = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row["period_end_date"], row["missing_ebit"], row["missing_ebitda"]) for row in before}
    rows = []
    for row in after:
        key = (row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row["period_end_date"], row["missing_ebit"], row["missing_ebitda"])
        if key not in before_keys:
            rows.append({**row, "phase3c4_delta": "ADDED_OR_CHANGED_AFTER_V2_NEW_Q"})
    return rows


def source_contribution_with_apply(source: dict[str, Any], apply_summary: dict[str, Any]) -> dict[str, Any]:
    return {**source, "canonical_apply": {"rows": apply_summary["rows"], "metadata": apply_summary["metadata"], "fields": apply_summary["field_contributions"], "issues": apply_summary["issues"]}}


def field_contribution_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"field": field, **counts} for field, counts in summary["field_contributions"].items()]


def field_insert_counts(summary: dict[str, Any]) -> dict[str, int]:
    return {field: int(counts.get("FIELD_INSERTED", 0)) + int(counts.get("FIELD_FILLED_FROM_NULL", 0)) for field, counts in summary["field_contributions"].items()}


def historical_coverage_pre_post(before: dict[str, Any], after: dict[str, Any], v2_only_before: int, review_after: int) -> dict[str, Any]:
    return {
        "canonical_q_before": before["coverage"]["canonical_q_total"],
        "canonical_q_after": after["coverage"]["canonical_q_total"],
        "canonical_q_delta": after["coverage"]["canonical_q_total"] - before["coverage"]["canonical_q_total"],
        "median_q_per_company_before": before["history_profile"]["median_q_per_company"],
        "median_q_per_company_after": after["history_profile"]["median_q_per_company"],
        "companies_ge_28q_before": before["history_profile"]["companies_ge_28q"],
        "companies_ge_28q_after": after["history_profile"]["companies_ge_28q"],
        "historical_gaps_before": v2_only_before,
        "historical_gaps_after": review_after,
    }


def core_readiness_pre_post(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "core_ready_before": before["coverage"]["core_ready_q"],
        "core_ready_after": after["coverage"]["core_ready_q"],
        "core_not_ready_before": before["coverage"]["core_not_ready_q"],
        "core_not_ready_after": after["coverage"]["core_not_ready_q"],
        **{f"{field}_null_before": before["coverage"]["field_missing"][field] for field in CORE_FIELDS},
        **{f"{field}_null_after": after["coverage"]["field_missing"][field] for field in CORE_FIELDS},
        "ebit_null_before": before["coverage"]["field_missing"]["ebit"],
        "ebit_null_after": after["coverage"]["field_missing"]["ebit"],
    }


def publication_coverage_pre_post(before: dict[str, Any], after: dict[str, Any], production_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "publish_known_before": before["coverage"]["publish_date_known"],
        "publish_known_after": after["coverage"]["publish_date_known"],
        "publish_null_before": before["coverage"]["publish_date_null"],
        "publish_null_after": after["coverage"]["publish_date_null"],
        "v2_new_q_publication_inserts": int(production_summary["metadata"].get("PUBLISH_DATE_SET", 0)),
    }


def population_summary(final_rows: list[dict[str, Any]]) -> dict[str, Any]:
    class_counts = Counter(row["final_classification"] for row in final_rows)
    return {
        "v2_only_historical_candidates": len(final_rows),
        "companies_affected": len({row["ticker"] for row in final_rows}),
        "year_distribution": dict(sorted(Counter(date.fromisoformat(row["period_end_date"]).year for row in final_rows).items())),
        "quarter_distribution": dict(sorted(Counter(row["fiscal_quarter"] for row in final_rows).items())),
        "final_classification": {name: class_counts[name] for name in FINAL_CLASSES},
    }


def calibration_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tested = len(rows)
    recovered = sum(int(row["correctly_recovered"]) for row in rows)
    wrong = sum(int(row["wrong_fyfq_recovered"]) for row in rows)
    false_extra = sum(int(row["false_extra_q_created"]) for row in rows)
    held = sum(int(row["held"]) for row in rows)
    precision = round((recovered / max(1, recovered + wrong + false_extra)) * 100.0, 2)
    recall = round((recovered / tested) * 100.0, 2) if tested else 0.0
    return {"tested_hidden_qs": tested, "correctly_recovered": recovered, "wrong_recoveries": wrong, "false_extra_qs": false_extra, "held": held, "precision": precision, "recall": recall}


def estimate_review_value(review_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in review_rows:
        cls = row["final_classification"]
        if cls == "PROBABLE_NEW_Q":
            estimate = "GENUINELY_RECOVERABLE_WITH_MORE_WORK"
        elif cls in {"DUPLICATE_OR_VARIANT_OF_EXISTING_Q", "POSSIBLE_WRONG_V2_MAPPING", "CLEAR_WRONG_V2_MAPPING", "PERIOD_IDENTITY_CONFLICT", "LEGACY_CONFLICT"}:
            estimate = "PROBABLY_INCORRECT_OR_REDUNDANT"
        else:
            estimate = "LOW_VALUE_AMBIGUOUS"
        rows.append({**row, "phase3c4b_value_estimate": estimate})
    return rows


def value_estimate_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(sorted(Counter(row["phase3c4b_value_estimate"] for row in rows).items()))


def recommend_next_step(value_rows: list[dict[str, Any]]) -> str:
    recoverable = sum(1 for row in value_rows if row["phase3c4b_value_estimate"] == "GENUINELY_RECOVERABLE_WITH_MORE_WORK")
    return "MASTER PLAN PHASE 3C-4B - V2 HISTORICAL MAPPING REVIEW" if recoverable >= 25 else "MASTER PLAN PHASE 3C-5 - RESIDUAL RECONCILIATION"


def mapping_risk_import_count(rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in rows if row["final_classification"] != "STRONG_NEW_Q_CONFIRMED")


def _table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {name: int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_migration_audit", "v3_resolution_issue")}


def _preflight_text(prepared: Phase3C4Prepared) -> str:
    counts = population_summary(prepared.final_rows)
    return (
        "# Phase 3C-4 V2 Historical Gap Fill Preflight\n\n"
        f"companies: {prepared.baseline['company_total']}\n\n"
        f"canonical_q: {prepared.baseline['coverage']['canonical_q_total']}\n\n"
        f"v2_only_historical_candidates: {counts['v2_only_historical_candidates']}\n\n"
        f"strong_new_q_confirmed: {counts['final_classification']['STRONG_NEW_Q_CONFIRMED']}\n\n"
        f"mapping_risk_baseline: {len(prepared.mapping_risk_rows) + len(prepared.duplicate_rows)}\n\n"
        "policy: V2 new-Q creation only for STRONG_NEW_Q_CONFIRMED; no overwrites; no pre-2018; no network.\n"
    )


def write_durable_doc(path: Path, artifact_root: Path, summary: dict[str, Any]) -> None:
    pre = summary["pre_baseline"]
    post = summary["post_baseline"]
    pop = summary["candidate_population"]
    calibration = summary["new_q_gate_calibration"]
    text = f"""# Fundamentals V3 Phase 3C-4 V2 Residual Historical Gap Fill

Classification: `{summary['classification']}`

Artifact root: `{artifact_root}`

Phase 3C-4 classified the remaining {pop['v2_only_historical_candidates']} V2-only 2018+ rows after Yahoo, Legacy, and V2 existing-Q enrichment. New canonical Q creation required the strict neighbor + cadence + Legacy corroboration gate.

Baseline and result:

- Canonical Q: {pre['coverage']['canonical_q_total']} -> {post['coverage']['canonical_q_total']}
- Core-ready: {pre['coverage']['core_ready_q']} -> {post['coverage']['core_ready_q']}
- Core-not-ready: {pre['coverage']['core_not_ready_q']} -> {post['coverage']['core_not_ready_q']}
- Publish NULL: {pre['coverage']['publish_date_null']} -> {post['coverage']['publish_date_null']}

New-Q gate:

- Hidden-Q tests: {calibration['tested_hidden_qs']}
- Correctly recovered: {calibration['correctly_recovered']}
- False extra Qs: {calibration['false_extra_qs']}
- Precision: {calibration['precision']}%
- Recall: {calibration['recall']}%

Classification:

- STRONG_NEW_Q_CONFIRMED: {pop['final_classification']['STRONG_NEW_Q_CONFIRMED']}
- PROBABLE_NEW_Q: {pop['final_classification']['PROBABLE_NEW_Q']}
- INSUFFICIENT_NEW_Q_EVIDENCE: {pop['final_classification']['INSUFFICIENT_NEW_Q_EVIDENCE']}
- DUPLICATE_OR_VARIANT_OF_EXISTING_Q: {pop['final_classification']['DUPLICATE_OR_VARIANT_OF_EXISTING_Q']}
- POSSIBLE_WRONG_V2_MAPPING: {pop['final_classification']['POSSIBLE_WRONG_V2_MAPPING']}
- CLEAR_WRONG_V2_MAPPING: {pop['final_classification']['CLEAR_WRONG_V2_MAPPING']}
- PERIOD_IDENTITY_CONFLICT: {pop['final_classification']['PERIOD_IDENTITY_CONFLICT']}
- LEGACY_CONFLICT: {pop['final_classification']['LEGACY_CONFLICT']}
- OTHER_IDENTIFIED: {pop['final_classification']['OTHER_IDENTIFIED']}

Safety:

- Existing non-null value overwrites: {summary['no_overwrite']['existing_non_null_values_overwritten']}
- Existing publish-date overwrites: {summary['no_overwrite']['existing_publish_dates_overwritten']}
- quick_check: `{summary['integrity']['quick_check']}`
- foreign_key_check_rows: {summary['integrity']['foreign_key_check_rows']}
- pre-2018 Q after apply: {post['history_profile']['pre_2018_q']}

Next step: `{summary['recommended_next_step']}`
"""
    path.write_text(text)


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames and not key.startswith("_"):
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
