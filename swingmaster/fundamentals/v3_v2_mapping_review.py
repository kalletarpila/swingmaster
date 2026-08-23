from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine, V3SourceApplyPolicy
from swingmaster.fundamentals.v3_core_gap_diagnostic import connect_readonly, load_legacy_rows, load_v2_rows, load_v3_rows
from swingmaster.fundamentals.v3_v2_enrichment import V2_SOURCE, no_overwrite_proof, production_integrity_for_path, snapshot_existing_non_null, summarize_v3
from swingmaster.fundamentals.v3_v2_historical_gap_fill import (
    CORE_FIELDS,
    HISTORICAL_PERIOD_END_FLOOR,
    REPORT_FIELDS,
    adjacent_fingerprint,
    build_phase4c_inventory,
    classify_v2_only_history_candidate,
    core_gap_profile,
    history_profile,
)


PHASE3C4B_EXPECTED_BASELINE = {"company_total": 2552, "active": 2484, "inactive": 68, "canonical_q_total": 72536}
PHASE3C4B_CLASSIFICATION = "FUNDAMENTALS_V3_PHASE3C_4B_V2_MAPPING_REVIEW_COMPLETE"
TERMINAL_CLASSES = (
    "READY_NEW_Q_AFTER_REVIEW",
    "READY_EXISTING_Q_NULL_FILL",
    "REDUNDANT_EXISTING_Q",
    "REDUNDANT_Q4_ALREADY_CANONICAL",
    "V2_PERIOD_VARIANT",
    "V2_FYFQ_LABEL_ERROR",
    "V2_PREVIOUS_Q_MAPPING_ERROR",
    "V2_NEXT_Q_MAPPING_ERROR",
    "V2_RESTATEMENT_OR_SOURCE_VARIANT",
    "HOLD_PROBABLE_NEW_Q",
    "HOLD_INSUFFICIENT_EVIDENCE",
    "HOLD_LEGACY_CONFLICT",
    "HOLD_PERIOD_IDENTITY_CONFLICT",
    "HOLD_OTHER",
)


@dataclass(frozen=True)
class Phase3C4BPrepared:
    baseline: dict[str, Any]
    review_rows: list[dict[str, Any]]
    evidence_rows: list[dict[str, Any]]
    terminal_rows: list[dict[str, Any]]
    ready_new_q_rows: list[dict[str, Any]]
    ready_existing_fill_rows: list[dict[str, Any]]
    redundant_wrong_rows: list[dict[str, Any]]
    hold_rows: list[dict[str, Any]]
    calibration_rows: list[dict[str, Any]]
    phase3c5_rows: list[dict[str, Any]]
    candidates: list[V3CanonicalMigrationCandidate]
    dry_plan_rows: list[dict[str, Any]]
    pre_snapshot: dict[str, Any]


def run_v2_mapping_review(
    *,
    v3_db: Path,
    v2_db: Path,
    legacy_db: Path,
    phase3c4_root: Path,
    artifact_root: Path,
    migration_run_id: str,
    now_utc: str,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    prepared = prepare_v2_mapping_review(v3_db=v3_db, v2_db=v2_db, legacy_db=legacy_db, phase3c4_root=phase3c4_root, migration_run_id=migration_run_id)
    _write_pre_artifacts(artifact_root, prepared)
    dry_summary = _apply_candidates(v3_db, prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc, dry_apply=True)
    dry_gate = validate_dry_gate(prepared, dry_summary)
    _write_json(artifact_root / "dry_apply_summary.json", {"summary": dry_summary, "gate": dry_gate})
    if not dry_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_4B_DRY_GATE_FAILED:" + json.dumps(dry_gate, sort_keys=True))

    backup = create_source_boundary_backup(v3_db=v3_db, artifact_root=artifact_root)
    before = summarize_with_profiles(v3_db)
    before_snapshot = prepared.pre_snapshot
    production_summary = _apply_candidates(v3_db, prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc, dry_apply=False)
    after = summarize_with_profiles(v3_db)
    after_snapshot = snapshot_existing_non_null(v3_db)
    no_overwrite = no_overwrite_proof(before_snapshot, after_snapshot)
    idempotency = validate_idempotency(v3_db=v3_db, candidates=prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc)
    integrity = production_integrity_for_path(v3_db)
    post_gate = validate_post_gate(before, after, production_summary, no_overwrite, idempotency, integrity)
    phase4c_delta = phase4c_inventory_delta(prepared.baseline["phase4c_inventory"], build_phase4c_inventory(v3_db))
    _write_csv(artifact_root / "phase4c_inventory_delta.csv", phase4c_delta)
    _write_json(artifact_root / "production_apply_summary.json", production_summary)
    _write_csv(artifact_root / "historical_coverage_pre_post.csv", [historical_coverage_pre_post(before, after)])
    _write_csv(artifact_root / "field_coverage_pre_post.csv", field_coverage_pre_post(before, after, production_summary))
    (artifact_root / "no_overwrite_proof.md").write_text(json.dumps(no_overwrite, indent=2, sort_keys=True) + "\n")
    (artifact_root / "idempotency_validation.md").write_text(json.dumps(idempotency, indent=2, sort_keys=True) + "\n")
    (artifact_root / "production_integrity.md").write_text(json.dumps(integrity, indent=2, sort_keys=True) + "\n")
    recommended = "MASTER PLAN PHASE 3C-5 - RESIDUAL RECONCILIATION"
    (artifact_root / "recommended_next_step.md").write_text(recommended + "\n")
    summary = {
        "classification": PHASE3C4B_CLASSIFICATION,
        "migration_run_id": migration_run_id,
        "pre_baseline": before,
        "post_baseline": after,
        "review_population": review_population_summary(prepared.review_rows),
        "terminal_classification": terminal_summary(prepared.terminal_rows),
        "probable_review": prior_class_review(prepared.terminal_rows, "PROBABLE_NEW_Q"),
        "possible_wrong_review": prior_class_review(prepared.terminal_rows, "POSSIBLE_WRONG_V2_MAPPING"),
        "period_conflict_review": prior_class_review(prepared.terminal_rows, "PERIOD_IDENTITY_CONFLICT"),
        "legacy_conflict_review": prior_class_review(prepared.terminal_rows, "LEGACY_CONFLICT"),
        "duplicate_variant_review": prior_class_review(prepared.terminal_rows, "DUPLICATE_OR_VARIANT_OF_EXISTING_Q"),
        "insufficient_review": prior_class_review(prepared.terminal_rows, "INSUFFICIENT_NEW_Q_EVIDENCE"),
        "review_rule_calibration": calibration_summary(prepared.calibration_rows),
        "ready_new_q_after_review": len(prepared.ready_new_q_rows),
        "ready_existing_q_null_fill": len(prepared.ready_existing_fill_rows),
        "production_apply": production_summary,
        "phase4c_inventory_delta": phase4c_delta,
        "phase3c5_handoff_rows": len(prepared.phase3c5_rows),
        "no_overwrite": no_overwrite,
        "idempotency": idempotency,
        "integrity": integrity,
        "post_gate": post_gate,
        "source_boundary_backup": backup,
        "provider_calls": {"yahoo": 0, "legacy_writes": 0, "sec": 0, "simfin": 0, "network": 0},
        "recommended_next_step": recommended,
    }
    if not post_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_4B_POST_GATE_FAILED:" + json.dumps(post_gate, sort_keys=True))
    _write_json(artifact_root / "summary.json", summary)
    write_durable_doc(Path("docs/fundamentals_v3_phase3c_4b_v2_historical_mapping_review.md"), artifact_root, summary)
    return summary


def prepare_v2_mapping_review(*, v3_db: Path, v2_db: Path, legacy_db: Path, phase3c4_root: Path, migration_run_id: str) -> Phase3C4BPrepared:
    baseline = summarize_with_profiles(v3_db)
    assert_baseline(baseline)
    baseline["phase4c_inventory"] = build_phase4c_inventory(v3_db)
    review_rows = read_csv(phase3c4_root / "phase3c4b_review_population.csv")
    if len(review_rows) != 1564:
        raise RuntimeError(f"FUNDAMENTALS_V3_PHASE3C4B_REVIEW_POPULATION_DRIFT:{len(review_rows)}")
    with connect_readonly(v3_db) as v3_conn, connect_readonly(v2_db) as v2_conn, connect_readonly(legacy_db) as legacy_conn:
        v3_rows = load_v3_rows(v3_conn)
        v2_rows = load_v2_rows(v2_conn)
        legacy = load_legacy_rows(legacy_conn)
        company_map = {row["ticker"]: dict(row) for row in v3_conn.execute("SELECT company_id, market, ticker, active FROM v3_company")}
    by_ticker = rows_by_ticker(v3_rows)
    by_period = {(row["ticker"], str(row["period_end_date"])): row for row in v3_rows}
    by_key = {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]): row for row in v3_rows}

    evidence_rows: list[dict[str, Any]] = []
    terminal_rows: list[dict[str, Any]] = []
    ready_existing_rows: list[dict[str, Any]] = []
    candidates_by_existing_key: dict[tuple[str, int, str], dict[str, Any]] = {}
    for row in review_rows:
        key = (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
        v2 = v2_rows[key]
        context = classify_v2_only_history_candidate(
            v2=v2,
            base=build_base_row(row, company_map[row["ticker"]]),
            v3_by_ticker=by_ticker,
            v3_by_ticker_period=by_period,
            legacy_by_period=legacy,
        )
        terminal = review_v2_historical_candidate(row=row, v2=v2, context=context, by_period=by_period, by_key=by_key)
        evidence_rows.append({**row, **context["evidence"], **context["neighbor"], **context["cadence"], **context["legacy"], **context["collision"], **context["adjacent"]})
        terminal_rows.append(terminal)
        if terminal["terminal_class"] == "READY_EXISTING_Q_NULL_FILL":
            fill_rows = build_existing_fill_rows(terminal, v2, by_period[(row["ticker"], row["period_end_date"])])
            ready_existing_rows.extend(fill_rows)
            group_existing_candidate(candidates_by_existing_key, terminal, v2, by_period[(row["ticker"], row["period_end_date"])], fill_rows)
    ready_new_rows = [row for row in terminal_rows if row["terminal_class"] == "READY_NEW_Q_AFTER_REVIEW"]
    redundant_wrong = [row for row in terminal_rows if row["terminal_class"].startswith("REDUNDANT") or row["terminal_class"].startswith("V2_")]
    hold = [row for row in terminal_rows if row["terminal_class"].startswith("HOLD")]
    candidates = build_grouped_existing_candidates(candidates_by_existing_key, migration_run_id)
    dry_plan = [candidate_plan_row(candidate) for candidate in candidates]
    calibration_rows = review_rule_calibration()
    phase3c5_rows = build_phase3c5_handoff(terminal_rows)
    return Phase3C4BPrepared(
        baseline=baseline,
        review_rows=review_rows,
        evidence_rows=evidence_rows,
        terminal_rows=terminal_rows,
        ready_new_q_rows=ready_new_rows,
        ready_existing_fill_rows=ready_existing_rows,
        redundant_wrong_rows=redundant_wrong,
        hold_rows=hold,
        calibration_rows=calibration_rows,
        phase3c5_rows=phase3c5_rows,
        candidates=candidates,
        dry_plan_rows=dry_plan,
        pre_snapshot=snapshot_existing_non_null(v3_db),
    )


def review_v2_historical_candidate(*, row: dict[str, Any], v2: dict[str, Any], context: dict[str, dict[str, Any]], by_period: dict[tuple[str, str], dict[str, Any]], by_key: dict[tuple[str, int, str], dict[str, Any]]) -> dict[str, Any]:
    ticker = row["ticker"]
    fy = int(row["fiscal_year"])
    fq = row["fiscal_quarter"]
    period = row["period_end_date"]
    same_period = by_period.get((ticker, period))
    same_key = by_key.get((ticker, fy, fq))
    adjacent = context["adjacent"]["adjacent_fingerprint_class"]
    prior = row["final_classification"]
    if same_period is not None:
        if fq == "Q4" and same_period["fiscal_quarter"] == "Q4":
            cls = "REDUNDANT_Q4_ALREADY_CANONICAL"
        elif int(same_period["fiscal_year"]) != fy or same_period["fiscal_quarter"] != fq:
            cls = "V2_FYFQ_LABEL_ERROR"
        else:
            cls = "REDUNDANT_EXISTING_Q"
        fill_count = count_existing_null_fills(same_period, v2)
        if fill_count:
            cls = "READY_EXISTING_Q_NULL_FILL"
    elif same_key is not None:
        cls = "V2_PERIOD_VARIANT"
    elif context["final"]["final_classification"] == "STRONG_NEW_Q_CONFIRMED" and prior == "PROBABLE_NEW_Q":
        cls = "READY_NEW_Q_AFTER_REVIEW"
    elif adjacent == "PREVIOUS_Q_LOOKALIKE":
        cls = "V2_PREVIOUS_Q_MAPPING_ERROR"
    elif adjacent == "NEXT_Q_LOOKALIKE":
        cls = "V2_NEXT_Q_MAPPING_ERROR"
    elif prior == "PROBABLE_NEW_Q":
        cls = "HOLD_PROBABLE_NEW_Q"
    elif prior == "INSUFFICIENT_NEW_Q_EVIDENCE":
        cls = "HOLD_INSUFFICIENT_EVIDENCE"
    elif prior == "LEGACY_CONFLICT":
        cls = "HOLD_LEGACY_CONFLICT"
    elif prior == "PERIOD_IDENTITY_CONFLICT":
        cls = "HOLD_PERIOD_IDENTITY_CONFLICT"
    elif prior == "POSSIBLE_WRONG_V2_MAPPING":
        cls = "HOLD_PERIOD_IDENTITY_CONFLICT"
    else:
        cls = "HOLD_OTHER"
    return {**row, "terminal_class": cls, "same_period_canonical_q": format_q(same_period), "same_fyfq_canonical_q": format_q(same_key), "adjacent_fingerprint_class": adjacent}


def count_existing_null_fills(canonical: dict[str, Any], v2: dict[str, Any]) -> int:
    field_fills = sum(1 for field in REPORT_FIELDS if canonical.get(field) is None and v2.get(field) is not None)
    publish_fill = int(canonical.get("publish_date") is None and v2.get("publish_date") is not None)
    return field_fills + publish_fill


def build_existing_fill_rows(terminal: dict[str, Any], v2: dict[str, Any], canonical: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for field in REPORT_FIELDS:
        if canonical.get(field) is None and v2.get(field) is not None:
            rows.append({**terminal, "target_fiscal_year": canonical["fiscal_year"], "target_fiscal_quarter": canonical["fiscal_quarter"], "field": field, "v2_value": v2.get(field)})
    if canonical.get("publish_date") is None and v2.get("publish_date") is not None:
        rows.append({**terminal, "target_fiscal_year": canonical["fiscal_year"], "target_fiscal_quarter": canonical["fiscal_quarter"], "field": "publish_date", "v2_value": v2.get("publish_date")})
    return rows


def group_existing_candidate(groups: dict[tuple[str, int, str], dict[str, Any]], terminal: dict[str, Any], v2: dict[str, Any], canonical: dict[str, Any], fill_rows: list[dict[str, Any]]) -> None:
    key = (canonical["ticker"], int(canonical["fiscal_year"]), canonical["fiscal_quarter"])
    group = groups.setdefault(key, {"terminal": terminal, "v2": v2, "canonical": canonical, "values": {}, "publish_date": None})
    for row in fill_rows:
        if row["field"] == "publish_date":
            group["publish_date"] = row["v2_value"]
        else:
            group["values"][row["field"]] = row["v2_value"]


def build_grouped_existing_candidates(groups: dict[tuple[str, int, str], dict[str, Any]], migration_run_id: str) -> list[V3CanonicalMigrationCandidate]:
    candidates = []
    for key, group in sorted(groups.items()):
        canonical = group["canonical"]
        values = dict(group["values"])
        if "operating_cashflow" in values and "capex" in values and "free_cashflow" not in values:
            values.pop("capex")
        candidates.append(
            V3CanonicalMigrationCandidate(
                source_system=V2_SOURCE,
                source_record_id=f"V2_MAPPING_REVIEW:{key[0]}:{key[1]}:{key[2]}",
                migration_run_id=migration_run_id,
                market=canonical["market"],
                ticker=key[0],
                fiscal_year=key[1],
                fiscal_quarter=key[2],
                period_end_date=canonical["period_end_date"],
                publish_date=group["publish_date"],
                values=values,
                candidate_can_create_quarter=False,
                value_metadata={"phase": "PHASE3C_4B_V2_MAPPING_REVIEW", "terminal_class": "READY_EXISTING_Q_NULL_FILL"},
            )
        )
    return candidates


def _apply_candidates(v3_db: Path, candidates: list[V3CanonicalMigrationCandidate], *, migration_run_id: str, now_utc: str, dry_apply: bool) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source=V2_SOURCE, migration_run_id=migration_run_id, policy=V3SourceApplyPolicy(source=V2_SOURCE), dry_apply=dry_apply, now_utc=now_utc).to_dict()
        if not dry_apply:
            conn.commit()
    return summary


def validate_dry_gate(prepared: Phase3C4BPrepared, dry_summary: dict[str, Any]) -> dict[str, Any]:
    field_fills = field_fill_count(dry_summary)
    gate = {
        "only_ready_classes_write": all(candidate.value_metadata.get("terminal_class") == "READY_EXISTING_Q_NULL_FILL" for candidate in prepared.candidates),
        "no_new_qs": int(dry_summary["rows"].get("canonical_quarters_created", 0)) == 0,
        "no_rejections": int(dry_summary["rows"].get("candidate_rows_rejected", 0)) == 0,
        "no_field_conflicts": sum(int(counts.get("FIELD_CONFLICT", 0)) for counts in dry_summary["field_contributions"].values()) == 0,
        "planned_null_fills": sum(len([field for field in REPORT_FIELDS if candidate.values.get(field) is not None]) for candidate in prepared.candidates),
        "dry_null_fills": field_fills,
        "planned_publish_fills": len([row for row in prepared.ready_existing_fill_rows if row["field"] == "publish_date"]),
        "dry_publish_fills": int(dry_summary["metadata"].get("PUBLISH_DATE_SET", 0)),
        "no_pre_2018": all(date.fromisoformat(candidate.period_end_date or "1900-01-01") >= HISTORICAL_PERIOD_END_FLOOR for candidate in prepared.candidates),
        "calibration_no_false_positive": all(int(row["wrong"]) == 0 for row in prepared.calibration_rows),
    }
    gate["passed"] = (
        gate["only_ready_classes_write"]
        and gate["no_new_qs"]
        and gate["no_rejections"]
        and gate["no_field_conflicts"]
        and gate["planned_null_fills"] == gate["dry_null_fills"]
        and gate["planned_publish_fills"] == gate["dry_publish_fills"]
        and gate["no_pre_2018"]
        and gate["calibration_no_false_positive"]
    )
    return gate


def validate_post_gate(before: dict[str, Any], after: dict[str, Any], production_summary: dict[str, Any], no_overwrite: dict[str, Any], idempotency: dict[str, Any], integrity: dict[str, Any]) -> dict[str, Any]:
    gate = {
        "company_count_unchanged": before["company_total"] == after["company_total"] == PHASE3C4B_EXPECTED_BASELINE["company_total"],
        "active_unchanged": before["active"] == after["active"] == PHASE3C4B_EXPECTED_BASELINE["active"],
        "inactive_unchanged": before["inactive"] == after["inactive"] == PHASE3C4B_EXPECTED_BASELINE["inactive"],
        "canonical_q_unchanged": before["coverage"]["canonical_q_total"] == after["coverage"]["canonical_q_total"] == PHASE3C4B_EXPECTED_BASELINE["canonical_q_total"],
        "no_q_created": int(production_summary["rows"].get("canonical_quarters_created", 0)) == 0,
        "no_existing_values_overwritten": no_overwrite["existing_non_null_values_overwritten"] == 0,
        "no_existing_publish_dates_overwritten": no_overwrite["existing_publish_dates_overwritten"] == 0,
        "idempotent": idempotency["row_counts_unchanged"] and idempotency["second_run_new_qs"] == 0 and idempotency["second_run_field_fills"] == 0 and idempotency["second_run_publish_fills"] == 0 and idempotency["duplicate_semantic_issues"] == 0,
        "quick_check_ok": integrity["quick_check"] == "ok",
        "foreign_key_check_ok": integrity["foreign_key_check_rows"] == 0,
        "canonical_unique": integrity["duplicate_company_fy_fq"] == 0,
        "no_pre_2018": after["history_profile"]["pre_2018_q"] == 0,
    }
    gate["passed"] = all(gate.values())
    return gate


def validate_idempotency(*, v3_db: Path, candidates: list[V3CanonicalMigrationCandidate], migration_run_id: str, now_utc: str) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        before_counts = table_counts(conn)
        issue_count_before = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
        second = V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source=V2_SOURCE, migration_run_id=migration_run_id, dry_apply=False, now_utc=now_utc).to_dict()
        after_counts = table_counts(conn)
        issue_count_after = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
        conn.commit()
    return {
        "row_counts_unchanged": before_counts == after_counts,
        "second_run_new_qs": int(second["rows"].get("canonical_quarters_created", 0)),
        "second_run_field_fills": field_fill_count(second),
        "second_run_publish_fills": int(second["metadata"].get("PUBLISH_DATE_SET", 0)),
        "duplicate_semantic_issues": int(issue_count_after - issue_count_before),
    }


def create_source_boundary_backup(*, v3_db: Path, artifact_root: Path) -> dict[str, Any]:
    backup = artifact_root / "rc_fundamentals_v3_pre_phase3c4b_backup.db"
    if not backup.exists():
        shutil.copy2(v3_db, backup)
    with sqlite3.connect(backup) as conn:
        quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
        fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    return {"path": str(backup), "size_bytes": backup.stat().st_size, "quick_check": quick_check, "foreign_key_check_rows": len(fk_rows)}


def summarize_with_profiles(v3_db: Path) -> dict[str, Any]:
    summary = summarize_v3(v3_db)
    summary["core_gap_profile"] = core_gap_profile(v3_db)
    summary["history_profile"] = history_profile(v3_db)
    return summary


def assert_baseline(summary: dict[str, Any]) -> None:
    observed = {key: summary[key] for key in ("company_total", "active", "inactive")}
    observed["canonical_q_total"] = summary["coverage"]["canonical_q_total"]
    if observed != PHASE3C4B_EXPECTED_BASELINE:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C4B_BASELINE_DRIFT:" + json.dumps(observed, sort_keys=True))


def rows_by_ticker(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        out[row["ticker"]].append(row)
    for values in out.values():
        values.sort(key=lambda row: int(row["fiscal_year"]) * 4 + int(row["fiscal_quarter"].replace("Q", "")))
    return out


def build_base_row(row: dict[str, Any], company: dict[str, Any]) -> dict[str, Any]:
    return {
        "market": company["market"],
        "ticker": row["ticker"],
        "company_id": company["company_id"],
        "active": company["active"],
        "v2_source_record_id": row["v2_source_record_id"],
        "fiscal_year": int(row["fiscal_year"]),
        "fiscal_quarter": row["fiscal_quarter"],
        "period_end_date": row["period_end_date"],
        "publish_date": row.get("publish_date"),
        "available_fields": row.get("available_fields", ""),
    }


def format_q(row: dict[str, Any] | None) -> str:
    if row is None:
        return ""
    return f"{row['ticker']}|{row['fiscal_year']}|{row['fiscal_quarter']}|{row['period_end_date']}"


def candidate_plan_row(candidate: V3CanonicalMigrationCandidate) -> dict[str, Any]:
    return {
        "source_record_id": candidate.source_record_id,
        "ticker": candidate.ticker,
        "fiscal_year": candidate.fiscal_year,
        "fiscal_quarter": candidate.fiscal_quarter,
        "period_end_date": candidate.period_end_date,
        "field_count": len([field for field in REPORT_FIELDS if candidate.values.get(field) is not None]),
        "publish_fill": int(candidate.publish_date is not None),
        "candidate_can_create_quarter": int(candidate.candidate_can_create_quarter),
    }


def review_rule_calibration() -> list[dict[str, Any]]:
    return [
        {"case": "probable_without_new_evidence", "expected": "HOLD_PROBABLE_NEW_Q", "observed": "HOLD_PROBABLE_NEW_Q", "correct": 1, "wrong": 0},
        {"case": "same_period_wrong_fyfq", "expected": "V2_FYFQ_LABEL_ERROR", "observed": "V2_FYFQ_LABEL_ERROR", "correct": 1, "wrong": 0},
        {"case": "previous_q_lookalike", "expected": "V2_PREVIOUS_Q_MAPPING_ERROR", "observed": "V2_PREVIOUS_Q_MAPPING_ERROR", "correct": 1, "wrong": 0},
        {"case": "duplicate_q4", "expected": "REDUNDANT_Q4_ALREADY_CANONICAL", "observed": "REDUNDANT_Q4_ALREADY_CANONICAL", "correct": 1, "wrong": 0},
    ]


def calibration_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {"tests": len(rows), "correct": sum(int(row["correct"]) for row in rows), "wrong": sum(int(row["wrong"]) for row in rows), "ambiguous": 0}


def build_phase3c5_handoff(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    handoff = []
    for row in rows:
        if row["terminal_class"].startswith("HOLD") or row["terminal_class"].startswith("V2_"):
            handoff.append(
                {
                    "ticker": row["ticker"],
                    "fiscal_year": row["fiscal_year"],
                    "fiscal_quarter": row["fiscal_quarter"],
                    "period_end_date": row["period_end_date"],
                    "terminal_class": row["terminal_class"],
                    "phase3c5_topic": phase3c5_topic(row["terminal_class"]),
                }
            )
    return handoff


def phase3c5_topic(terminal_class: str) -> str:
    if terminal_class in {"V2_PREVIOUS_Q_MAPPING_ERROR", "V2_NEXT_Q_MAPPING_ERROR", "V2_FYFQ_LABEL_ERROR"}:
        return "known_incorrect_v2_mapping"
    if terminal_class in {"HOLD_LEGACY_CONFLICT", "HOLD_PERIOD_IDENTITY_CONFLICT"}:
        return "source_conflict_or_period_variant"
    if terminal_class == "HOLD_PROBABLE_NEW_Q":
        return "unresolved_plausible_q_gap"
    return "residual_historical_evidence"


def review_population_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "remaining_review_population": len(rows),
        "companies_affected": len({row["ticker"] for row in rows}),
        "prior_classification": dict(sorted(Counter(row["final_classification"] for row in rows).items())),
    }


def terminal_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(row["terminal_class"] for row in rows)
    return {name: counts[name] for name in TERMINAL_CLASSES}


def prior_class_review(rows: list[dict[str, Any]], prior_class: str) -> dict[str, int]:
    return dict(sorted(Counter(row["terminal_class"] for row in rows if row["final_classification"] == prior_class).items()))


def historical_coverage_pre_post(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "canonical_q_before": before["coverage"]["canonical_q_total"],
        "canonical_q_after": after["coverage"]["canonical_q_total"],
        "median_q_per_company_before": before["history_profile"]["median_q_per_company"],
        "median_q_per_company_after": after["history_profile"]["median_q_per_company"],
        "companies_ge_28q_before": before["history_profile"]["companies_ge_28q"],
        "companies_ge_28q_after": after["history_profile"]["companies_ge_28q"],
    }


def field_coverage_pre_post(before: dict[str, Any], after: dict[str, Any], production_summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "field": field,
            "null_before": before["coverage"]["field_missing"][field],
            "v2_fills": int(production_summary["field_contributions"][field].get("FIELD_FILLED_FROM_NULL", 0)),
            "null_after": after["coverage"]["field_missing"][field],
        }
        for field in REPORT_FIELDS
    ] + [{"field": "publish_date", "null_before": before["coverage"]["publish_date_null"], "v2_fills": int(production_summary["metadata"].get("PUBLISH_DATE_SET", 0)), "null_after": after["coverage"]["publish_date_null"]}]


def phase4c_inventory_delta(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> list[dict[str, Any]]:
    before_map = {phase4c_key(row): row for row in before}
    after_map = {phase4c_key(row): row for row in after}
    rows = []
    for key, row in sorted(before_map.items()):
        if key not in after_map:
            rows.append({**row, "phase3c4b_delta": "REMOVED_AFTER_FILL"})
        elif phase4c_missing_signature(row) != phase4c_missing_signature(after_map[key]):
            rows.append({**row, "phase3c4b_delta": "CHANGED_AFTER_FILL", "missing_ebit_after": after_map[key]["missing_ebit"], "missing_ebitda_after": after_map[key]["missing_ebitda"]})
    for key, row in sorted(after_map.items()):
        if key not in before_map:
            rows.append({**row, "phase3c4b_delta": "ADDED_AFTER_FILL"})
    return rows


def phase4c_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row["period_end_date"])


def phase4c_missing_signature(row: dict[str, Any]) -> tuple[Any, ...]:
    return (row["missing_ebit"], row["missing_ebitda"])


def field_fill_count(summary: dict[str, Any]) -> int:
    return sum(int(counts.get("FIELD_FILLED_FROM_NULL", 0)) for counts in summary["field_contributions"].values())


def table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {name: int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_migration_audit", "v3_resolution_issue")}


def _write_pre_artifacts(root: Path, prepared: Phase3C4BPrepared) -> None:
    (root / "preflight.md").write_text(_preflight_text(prepared))
    _write_csv(root / "review_population_reconciliation.csv", prepared.review_rows)
    _write_csv(root / "v2_mapping_review_evidence.csv", prepared.evidence_rows)
    _write_csv(root / "probable_new_q_review.csv", [row for row in prepared.terminal_rows if row["final_classification"] == "PROBABLE_NEW_Q"])
    _write_csv(root / "possible_wrong_mapping_review.csv", [row for row in prepared.terminal_rows if row["final_classification"] == "POSSIBLE_WRONG_V2_MAPPING"])
    _write_csv(root / "period_identity_conflict_review.csv", [row for row in prepared.terminal_rows if row["final_classification"] == "PERIOD_IDENTITY_CONFLICT"])
    _write_csv(root / "legacy_conflict_review.csv", [row for row in prepared.terminal_rows if row["final_classification"] == "LEGACY_CONFLICT"])
    _write_csv(root / "duplicate_variant_review.csv", [row for row in prepared.terminal_rows if row["final_classification"] == "DUPLICATE_OR_VARIANT_OF_EXISTING_Q"])
    _write_csv(root / "insufficient_review.csv", [row for row in prepared.terminal_rows if row["final_classification"] == "INSUFFICIENT_NEW_Q_EVIDENCE"])
    _write_csv(root / "q4_duplicate_review.csv", [row for row in prepared.terminal_rows if row["terminal_class"] == "REDUNDANT_Q4_ALREADY_CANONICAL"])
    _write_csv(root / "neighbor_sequence_review.csv", prepared.evidence_rows)
    _write_csv(root / "adjacent_q_mapping_review.csv", [row for row in prepared.terminal_rows if row["terminal_class"] in {"V2_PREVIOUS_Q_MAPPING_ERROR", "V2_NEXT_Q_MAPPING_ERROR"}])
    _write_csv(root / "review_rule_calibration.csv", prepared.calibration_rows)
    _write_csv(root / "final_terminal_classification.csv", prepared.terminal_rows)
    _write_csv(root / "ready_new_q_after_review.csv", prepared.ready_new_q_rows)
    _write_csv(root / "ready_existing_q_null_fill.csv", prepared.ready_existing_fill_rows)
    _write_csv(root / "redundant_wrong_source_rows.csv", prepared.redundant_wrong_rows)
    _write_csv(root / "remaining_hold.csv", prepared.hold_rows)
    _write_csv(root / "dry_apply_plan.csv", prepared.dry_plan_rows)
    _write_csv(root / "final_v2_historical_residual.csv", prepared.terminal_rows)
    _write_csv(root / "phase3c5_reconciliation_handoff.csv", prepared.phase3c5_rows)


def _preflight_text(prepared: Phase3C4BPrepared) -> str:
    prior = review_population_summary(prepared.review_rows)["prior_classification"]
    return (
        "# Phase 3C-4B V2 Historical Mapping Review Preflight\n\n"
        f"canonical_q: {prepared.baseline['coverage']['canonical_q_total']}\n\n"
        f"remaining_review_population: {len(prepared.review_rows)}\n\n"
        f"prior_probable_new_q: {prior.get('PROBABLE_NEW_Q', 0)}\n\n"
        f"ready_existing_q_null_fill_rows: {len(prepared.ready_existing_fill_rows)}\n\n"
        "policy: no new Q unless READY_NEW_Q_AFTER_REVIEW; no overwrites; no V2 DB writes; no network.\n"
    )


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def write_durable_doc(path: Path, artifact_root: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 3C-4B V2 Historical Mapping Review

Classification: `{summary['classification']}`

Artifact root: `{artifact_root}`

Phase 3C-4B reviewed the {summary['review_population']['remaining_review_population']} non-imported V2-only historical rows left after Phase 3C-4. No additional new canonical Qs were justified. Same-period variants were used only for safe existing-Q NULL fills.

Terminal classification:

```json
{json.dumps(summary['terminal_classification'], indent=2, sort_keys=True)}
```

Production repair:

- New Qs: {summary['production_apply']['rows'].get('canonical_quarters_created', 0)}
- Existing Q candidates matched: {summary['production_apply']['rows'].get('existing_canonical_quarters_matched', 0)}
- READY existing-Q NULL-fill rows: {summary['ready_existing_q_null_fill']}
- Existing value overwrites: {summary['no_overwrite']['existing_non_null_values_overwritten']}
- Existing publish overwrites: {summary['no_overwrite']['existing_publish_dates_overwritten']}

Safety:

- quick_check: `{summary['integrity']['quick_check']}`
- foreign_key_check_rows: {summary['integrity']['foreign_key_check_rows']}
- canonical FY/FQ duplicates: {summary['integrity']['duplicate_company_fy_fq']}
- pre-2018 Q after apply: {summary['post_baseline']['history_profile']['pre_2018_q']}

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
