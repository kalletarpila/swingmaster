from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_canonical_migration import (
    CANONICAL_FIELD_NAMES,
    V3CanonicalMigrationCandidate,
    V3CanonicalMigrationEngine,
    V3SourceApplyPolicy,
)
from swingmaster.fundamentals.v3_core_gap_diagnostic import (
    FINGERPRINT_FIELDS,
    IDENTITY_TOLERANCE,
    compare_values,
    classify_period_relation,
    classify_v2_identity,
    connect_readonly,
    load_legacy_rows,
    load_v2_rows,
    load_v2_rows_by_ticker,
    load_v3_rows,
)
from swingmaster.fundamentals.v3_v2_enrichment_policy import (
    V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE,
    decide_v2_publish_date_action,
    decide_v2_value_action,
)
from swingmaster.fundamentals.v3_yahoo_canonical_seed import CORE_READY_FIELDS, coverage_summary, production_integrity


PHASE3C_EXPECTED_BASELINE = {
    "company_total": 2552,
    "active": 2484,
    "inactive": 68,
    "canonical_q_total": 13017,
    "core_ready_q": 11907,
    "core_not_ready_q": 1110,
}
V2_SOURCE = "V2"
V2_SEMANTICALLY_ACCEPTED_FIELDS = tuple(CANONICAL_FIELD_NAMES)
V2_REJECTED_SEMANTIC_SUBSTITUTIONS = ("weighted_average_shares_basic", "weighted_average_shares_diluted")
TRUSTED_IDENTITY_FIELDS = ("revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow", "cash", "total_debt")
SEMANTIC_RISK_FIELDS = ("ebit", "ebitda", "free_cashflow", "shares_outstanding")
AUTO_ENRICH_ALLOWED = "AUTO_ENRICH_ALLOWED"
HOLD_NO_WRITE = "HOLD_NO_WRITE"
BLOCK_NO_WRITE = "BLOCK_NO_WRITE"


@dataclass(frozen=True)
class Phase3CV2Prepared:
    baseline: dict[str, Any]
    identity_rows: list[dict[str, Any]]
    mapping_risk_rows: list[dict[str, Any]]
    v2_only_rows: list[dict[str, Any]]
    legacy_crosscheck_rows: list[dict[str, Any]]
    safe_null_fill_plan: list[dict[str, Any]]
    safe_publish_fill_plan: list[dict[str, Any]]
    non_null_agreement_rows: list[dict[str, Any]]
    non_null_conflict_rows: list[dict[str, Any]]
    candidates: list[V3CanonicalMigrationCandidate]
    pre_snapshot: dict[str, Any]
    source_contribution: dict[str, Any]


def run_v2_enrichment(
    *,
    v3_db: Path,
    v2_db: Path,
    legacy_db: Path,
    artifact_root: Path,
    migration_run_id: str,
    now_utc: str,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    prepared = prepare_v2_enrichment(v3_db=v3_db, v2_db=v2_db, legacy_db=legacy_db, artifact_root=artifact_root, migration_run_id=migration_run_id)
    _write_json(artifact_root / "refined_v3_baseline.json", prepared.baseline)
    (artifact_root / "preflight.md").write_text(_preflight_text(prepared.baseline))
    _write_csv(artifact_root / "v2_identity_classification.csv", prepared.identity_rows)
    _write_csv(artifact_root / "v2_mapping_risk.csv", prepared.mapping_risk_rows)
    _write_csv(artifact_root / "v2_only_historical_q_candidates.csv", prepared.v2_only_rows)
    _write_csv(artifact_root / "v2_only_history_legacy_crosscheck.csv", prepared.legacy_crosscheck_rows)
    _write_csv(artifact_root / "safe_null_fill_plan.csv", prepared.safe_null_fill_plan)
    _write_csv(artifact_root / "safe_publish_fill_plan.csv", prepared.safe_publish_fill_plan)
    _write_csv(artifact_root / "v2_non_null_agreement.csv", prepared.non_null_agreement_rows)
    _write_csv(artifact_root / "v2_non_null_conflicts.csv", prepared.non_null_conflict_rows)
    _write_json(artifact_root / "v2_source_contribution.json", prepared.source_contribution)

    dry_conn = sqlite3.connect(v3_db)
    dry_conn.row_factory = sqlite3.Row
    dry_summary = V3CanonicalMigrationEngine(dry_conn).apply_source_batch(
        prepared.candidates,
        source=V2_SOURCE,
        migration_run_id=migration_run_id,
        policy=V3SourceApplyPolicy(source=V2_SOURCE),
        dry_apply=True,
        now_utc=now_utc,
    ).to_dict()
    dry_conn.close()
    dry_gate = validate_dry_gate(prepared=prepared, dry_summary=dry_summary)
    _write_json(artifact_root / "dry_apply_summary.json", {"summary": dry_summary, "gate": dry_gate})
    if not dry_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_DRY_GATE_FAILED:" + json.dumps(dry_gate, sort_keys=True))

    backup_status = source_boundary_status(artifact_root)
    before_snapshot = prepared.pre_snapshot
    before_summary = summarize_v3(v3_db)
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        production_summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
            prepared.candidates,
            source=V2_SOURCE,
            migration_run_id=migration_run_id,
            policy=V3SourceApplyPolicy(source=V2_SOURCE),
            now_utc=now_utc,
        ).to_dict()
        conn.commit()
    after_summary = summarize_v3(v3_db)
    after_snapshot = snapshot_existing_non_null(v3_db)
    no_overwrite = no_overwrite_proof(before_snapshot, after_snapshot)
    idempotency = run_idempotency(v3_db=v3_db, candidates=prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc, after_summary=after_summary)
    integrity = production_integrity_for_path(v3_db)
    post_gate = validate_post_gate(
        before=before_summary,
        after=after_summary,
        production_summary=production_summary,
        no_overwrite=no_overwrite,
        idempotency=idempotency,
        integrity=integrity,
    )
    _write_json(artifact_root / "v2_source_contribution.json", source_contribution_with_apply(prepared.source_contribution, production_summary))
    _write_csv(artifact_root / "v2_field_contribution.csv", field_contribution_rows(production_summary))
    _write_csv(artifact_root / "v2_metadata_contribution.csv", metadata_contribution_rows(production_summary))
    _write_csv(artifact_root / "core_readiness_pre_post.csv", [core_pre_post_row(before_summary, after_summary)])
    _write_csv(artifact_root / "missing_fields_pre_post.csv", missing_pre_post_rows(before_summary, after_summary, prepared.safe_null_fill_plan))
    _write_csv(artifact_root / "publication_coverage_pre_post.csv", [publication_pre_post_row(before_summary, after_summary, prepared.safe_publish_fill_plan)])
    (artifact_root / "no_overwrite_proof.md").write_text(json.dumps(no_overwrite, indent=2, sort_keys=True) + "\n")
    (artifact_root / "idempotency_validation.md").write_text(json.dumps(idempotency, indent=2, sort_keys=True) + "\n")
    (artifact_root / "production_v3_integrity.md").write_text(json.dumps(integrity, indent=2, sort_keys=True) + "\n")
    _write_json(artifact_root / "phase3d_baseline.json", after_summary)
    (artifact_root / "recommended_next_step.md").write_text("MASTER PLAN PHASE 3D - LEGACY DEEP-HISTORY ENRICHMENT\n")
    summary = {
        "classification": "FUNDAMENTALS_V3_PHASE3C_V2_ENRICHMENT_COMPLETE",
        "migration_run_id": migration_run_id,
        "pre_baseline": before_summary,
        "post_baseline": after_summary,
        "identity": prepared.source_contribution["identity"],
        "v2_only_historical_q_candidates": len(prepared.v2_only_rows),
        "safe_null_fills_planned": len(prepared.safe_null_fill_plan),
        "safe_publish_fills_planned": len(prepared.safe_publish_fill_plan),
        "production_apply": production_summary,
        "no_overwrite": no_overwrite,
        "idempotency": idempotency,
        "integrity": integrity,
        "post_gate": post_gate,
        "source_boundary": backup_status,
        "provider_calls": 0,
        "legacy_canonical_contribution": 0,
    }
    if not post_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_POST_GATE_FAILED:" + json.dumps(post_gate, sort_keys=True))
    _write_json(artifact_root / "summary.json", summary)
    return summary


def prepare_v2_enrichment(*, v3_db: Path, v2_db: Path, legacy_db: Path, artifact_root: Path, migration_run_id: str) -> Phase3CV2Prepared:
    baseline = summarize_v3(v3_db)
    assert_refined_baseline(baseline)
    v3_conn = connect_readonly(v3_db)
    v2_conn = connect_readonly(v2_db)
    legacy_conn = connect_readonly(legacy_db)
    v3_rows = load_v3_rows(v3_conn)
    v2_rows = load_v2_rows(v2_conn)
    legacy_rows = load_legacy_rows(legacy_conn)
    v3_conn.close()
    v2_conn.close()
    legacy_conn.close()
    v3_by_key = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"]): row for row in v3_rows}
    refined_tickers = {row["ticker"] for row in v3_rows}
    v2_refined = {key: row for key, row in v2_rows.items() if key[0] in refined_tickers}
    v2_by_ticker = load_v2_rows_by_ticker(v2_refined)
    identity_rows: list[dict[str, Any]] = []
    mapping_risk_rows: list[dict[str, Any]] = []
    safe_null_fill_plan: list[dict[str, Any]] = []
    safe_publish_fill_plan: list[dict[str, Any]] = []
    agreement_rows: list[dict[str, Any]] = []
    conflict_rows: list[dict[str, Any]] = []
    candidates: list[V3CanonicalMigrationCandidate] = []
    class_counts = Counter()
    apply_counts = Counter()
    for key, v3 in sorted(v3_by_key.items()):
        v2 = v2_refined.get(key)
        if v2 is None:
            continue
        identity = classify_phase3c_identity(v3, v2, v2_by_ticker.get(key[0], []))
        class_counts[identity["identity_classification"]] += 1
        apply_counts[identity["apply_state"]] += 1
        identity_rows.append(identity)
        if identity["identity_classification"] in {"MAPPING_RISK", "CLEAR_WRONG_QUARTER"}:
            mapping_risk_rows.append(identity)
        candidate_values: dict[str, Any] = {}
        field_actions = {}
        for field_name in CANONICAL_FIELD_NAMES:
            if field_name in V2_REJECTED_SEMANTIC_SUBSTITUTIONS:
                continue
            current = v3.get(field_name)
            incoming = v2.get(field_name)
            if identity["apply_state"] != AUTO_ENRICH_ALLOWED:
                continue
            if incoming is None:
                continue
            equivalent = _strict_equivalent(current, incoming) if current is not None else False
            action = decide_v2_value_action(
                field_name=field_name,
                existing_v3_value=current,
                v2_value=incoming,
                same_quarter_confirmed=True,
                value_equivalent=equivalent,
            )
            field_actions[field_name] = action
            if action == "FILL_NULL":
                candidate_values[field_name] = incoming
                safe_null_fill_plan.append(_fill_row(v3, v2, identity, field_name, incoming))
            elif action == "CONFIRM_ONLY":
                agreement_rows.append(_agreement_row(v3, v2, identity, field_name, current, incoming, equivalent))
            elif action == "CONFLICT_NO_OVERWRITE":
                conflict_rows.append(_conflict_row(v3, v2, identity, field_name, current, incoming))
        publish_date = None
        if identity["apply_state"] == AUTO_ENRICH_ALLOWED:
            publish_action = decide_v2_publish_date_action(
                existing_publish_date=v3.get("publish_date"),
                v2_publish_date=v2.get("publish_date"),
                same_quarter_confirmed=True,
            )
            if publish_action in {"FILL_NULL", "CONFIRM_ONLY"}:
                publish_date = v2.get("publish_date")
                if publish_action == "FILL_NULL":
                    safe_publish_fill_plan.append(_publish_fill_row(v3, v2, identity))
            elif publish_action == "CONFLICT_NO_OVERWRITE":
                conflict_rows.append(_conflict_row(v3, v2, identity, "publish_date", v3.get("publish_date"), v2.get("publish_date")))
        if identity["apply_state"] == AUTO_ENRICH_ALLOWED and (candidate_values or publish_date is not None):
            candidates.append(
                V3CanonicalMigrationCandidate(
                    source_system=V2_SOURCE,
                    source_record_id=f"V2:{key[0]}:{key[1]}:{key[2]}",
                    migration_run_id=migration_run_id,
                    market=v3["market"],
                    ticker=key[0],
                    fiscal_year=key[1],
                    fiscal_quarter=key[2],
                    period_end_date=v2.get("period_end_date"),
                    publish_date=publish_date,
                    values=candidate_values,
                    candidate_can_create_quarter=False,
                    period_date_policy="SAFE_VARIANT" if identity["period_relation"] != "EXACT_PERIOD_END" else "CONFLICT",
                    value_metadata={
                        "phase": "PHASE3C_V2_ENRICHMENT",
                        "identity_classification": identity["identity_classification"],
                        "apply_state": identity["apply_state"],
                        "identity_score": identity,
                        "field_actions": field_actions,
                        "no_overwrite_policy": V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE,
                    },
                )
            )
    v2_only_rows, legacy_crosscheck = v2_only_inventory(v2_refined, v3_by_key, legacy_rows)
    source_contribution = {
        "identity": {
            "v2_source_quarters_examined": len(v2_refined),
            "exact_ticker_fy_fq_candidates": len(identity_rows),
            "identity_classes": dict(sorted(class_counts.items())),
            "apply_states": dict(sorted(apply_counts.items())),
            "same_quarter_confirmed": class_counts["SAME_QUARTER_CONFIRMED"],
            "probable_ambiguous": class_counts["PROBABLE_SAME_QUARTER"] + class_counts["AMBIGUOUS"],
            "mapping_risk": class_counts["MAPPING_RISK"],
            "clear_wrong_quarter": class_counts["CLEAR_WRONG_QUARTER"],
            "insufficient_evidence": class_counts["INSUFFICIENT_EVIDENCE"],
            "period_identity_conflicts": class_counts["PERIOD_IDENTITY_CONFLICT"],
        },
        "planned_field_fills": dict(Counter(row["field"] for row in safe_null_fill_plan)),
        "planned_publish_fills": len(safe_publish_fill_plan),
        "non_null_conflicts": dict(Counter(row["field"] for row in conflict_rows)),
        "non_null_agreements": dict(Counter(row["field"] for row in agreement_rows)),
        "semantic_rejections": {field: "WEIGHTED_AVERAGE_SHARES_NOT_CANONICAL_SHARES_OUTSTANDING" for field in V2_REJECTED_SEMANTIC_SUBSTITUTIONS},
    }
    return Phase3CV2Prepared(
        baseline=baseline,
        identity_rows=identity_rows,
        mapping_risk_rows=mapping_risk_rows,
        v2_only_rows=v2_only_rows,
        legacy_crosscheck_rows=legacy_crosscheck,
        safe_null_fill_plan=safe_null_fill_plan,
        safe_publish_fill_plan=safe_publish_fill_plan,
        non_null_agreement_rows=agreement_rows,
        non_null_conflict_rows=conflict_rows,
        candidates=candidates,
        pre_snapshot=snapshot_existing_non_null(v3_db),
        source_contribution=source_contribution,
    )


def classify_phase3c_identity(v3: dict[str, Any], v2: dict[str, Any], ticker_v2_rows: list[dict[str, Any]]) -> dict[str, Any]:
    period_relation = classify_period_relation(v3.get("period_end_date"), v2.get("period_end_date"))
    comparisons = [compare_values(field, v3.get(field), v2.get(field), tolerance=IDENTITY_TOLERANCE) for field in FINGERPRINT_FIELDS]
    base = classify_v2_identity(comparisons, period_relation)
    trusted = [item for item in comparisons if item.field_name in TRUSTED_IDENTITY_FIELDS and item.comparable]
    trusted_matches = [item for item in trusted if item.within_5pct and not item.sign_mismatch]
    trusted_conflicts = [item for item in trusted if item.status in {"MISMATCH", "SIGN_MISMATCH"}]
    revenue = next((item for item in comparisons if item.field_name == "revenue"), None)
    revenue_opposite_sign = bool(
        revenue
        and revenue.comparable
        and revenue.v3_value is not None
        and revenue.v2_value is not None
        and float(revenue.v3_value) * float(revenue.v2_value) < 0
    )
    revenue_conflict = bool(revenue and revenue.comparable and (not revenue.within_5pct or revenue.sign_mismatch or revenue_opposite_sign))
    adjacent = adjacent_margin(v3, v2, ticker_v2_rows)
    period_compatible = period_relation in {"EXACT_PERIOD_END", "SMALL_KNOWN_PROVIDER_VARIANT", "KNOWN_FISCAL_CALENDAR_VARIANT"}
    same_q_margin_support = adjacent["best_match"] == "SAME_Q_BEST" and adjacent["score_margin"] is not None and adjacent["score_margin"] >= 1
    revenue_supports = bool(revenue and revenue.comparable and revenue.within_5pct and not revenue.sign_mismatch)
    revenue_unavailable = bool(revenue and not revenue.comparable)
    if not period_compatible:
        classification = "PERIOD_IDENTITY_CONFLICT"
        apply_state = BLOCK_NO_WRITE
    elif revenue_conflict:
        classification = "MAPPING_RISK" if len(trusted_matches) >= 2 else "CLEAR_WRONG_QUARTER"
        apply_state = BLOCK_NO_WRITE
    elif adjacent["best_match"] in {"PREVIOUS_Q_BEST", "NEXT_Q_BEST"}:
        classification = "CLEAR_WRONG_QUARTER" if revenue_conflict or len(trusted_matches) < 2 else "MAPPING_RISK"
        apply_state = BLOCK_NO_WRITE
    elif adjacent["best_match"] == "TIE":
        classification = "AMBIGUOUS"
        apply_state = HOLD_NO_WRITE
    elif base.classification in {"STRONG_MATCH", "STRONG_MATCH_LIMITED_FIELDS"}:
        classification = "SAME_QUARTER_CONFIRMED"
        apply_state = AUTO_ENRICH_ALLOWED
    elif revenue_supports and same_q_margin_support:
        classification = "SAME_QUARTER_CONFIRMED"
        apply_state = AUTO_ENRICH_ALLOWED
    elif revenue_supports and len(trusted_matches) >= 3:
        classification = "SAME_QUARTER_CONFIRMED"
        apply_state = AUTO_ENRICH_ALLOWED
    elif revenue_unavailable and same_q_margin_support and len(trusted_matches) >= 2 and len(trusted_conflicts) <= 1:
        classification = "SAME_QUARTER_CONFIRMED"
        apply_state = AUTO_ENRICH_ALLOWED
    elif base.classification == "PROBABLE_MATCH":
        classification = "PROBABLE_SAME_QUARTER"
        apply_state = HOLD_NO_WRITE
    else:
        classification = "INSUFFICIENT_EVIDENCE"
        apply_state = HOLD_NO_WRITE
    return {
        "ticker": v3["ticker"],
        "fiscal_year": v3["fiscal_year"],
        "fiscal_quarter": v3["fiscal_quarter"],
        "active": v3["active"],
        "v3_period_end_date": v3.get("period_end_date"),
        "v2_period_end_date": v2.get("period_end_date"),
        "v3_publish_date": v3.get("publish_date"),
        "v2_publish_date": v2.get("publish_date"),
        "period_relation": period_relation,
        "diag2_base_classification": base.classification,
        "identity_classification": classification,
        "apply_state": apply_state,
        "comparable_fields": base.comparable_fields,
        "trusted_comparable_fields": len(trusted),
        "trusted_matches_5pct": len(trusted_matches),
        "trusted_conflicts": len(trusted_conflicts),
        "semantic_risk_conflicts": sum(1 for item in comparisons if item.field_name in SEMANTIC_RISK_FIELDS and item.status in {"MISMATCH", "SIGN_MISMATCH"}),
        "revenue_conflict": int(revenue_conflict),
        "revenue_opposite_sign": int(revenue_opposite_sign),
        **adjacent,
    }


def adjacent_margin(v3: dict[str, Any], same_v2: dict[str, Any], ticker_v2_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not ticker_v2_rows:
        return {"best_match": "INSUFFICIENT", "same_score": None, "previous_score": None, "next_score": None, "score_margin": None}
    same_index = next((index for index, candidate in enumerate(ticker_v2_rows) if candidate is same_v2), None)
    if same_index is None:
        return {"best_match": "INSUFFICIENT", "same_score": None, "previous_score": None, "next_score": None, "score_margin": None}
    same_score = identity_score(v3, same_v2)
    scores = {"SAME_Q_BEST": same_score}
    previous_score = None
    next_score = None
    if same_index > 0:
        previous_score = identity_score(v3, ticker_v2_rows[same_index - 1])
        scores["PREVIOUS_Q_BEST"] = previous_score
    if same_index + 1 < len(ticker_v2_rows):
        next_score = identity_score(v3, ticker_v2_rows[same_index + 1])
        scores["NEXT_Q_BEST"] = next_score
    if len(scores) == 1:
        return {"best_match": "SAME_Q_BEST", "same_score": same_score, "previous_score": previous_score, "next_score": next_score, "score_margin": None}
    ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    if len(ordered) > 1 and ordered[0][1] == ordered[1][1]:
        best = "TIE"
    else:
        best = ordered[0][0]
    margin = ordered[0][1] - ordered[1][1] if len(ordered) > 1 else None
    return {"best_match": best, "same_score": same_score, "previous_score": previous_score, "next_score": next_score, "score_margin": margin}


def identity_score(v3: dict[str, Any], v2: dict[str, Any]) -> int:
    comparisons = [compare_values(field, v3.get(field), v2.get(field), tolerance=IDENTITY_TOLERANCE) for field in FINGERPRINT_FIELDS]
    trusted_matches = sum(1 for item in comparisons if item.field_name in TRUSTED_IDENTITY_FIELDS and item.within_5pct and not item.sign_mismatch)
    all_matches = sum(1 for item in comparisons if item.within_5pct and not item.sign_mismatch)
    trusted_conflicts = sum(1 for item in comparisons if item.field_name in TRUSTED_IDENTITY_FIELDS and item.status in {"MISMATCH", "SIGN_MISMATCH"})
    return trusted_matches * 100 + all_matches * 10 - trusted_conflicts * 25


def assert_refined_baseline(summary: dict[str, Any]) -> None:
    observed = {
        "company_total": summary["company_total"],
        "active": summary["active"],
        "inactive": summary["inactive"],
        "canonical_q_total": summary["coverage"]["canonical_q_total"],
        "core_ready_q": summary["coverage"]["core_ready_q"],
        "core_not_ready_q": summary["coverage"]["core_not_ready_q"],
    }
    if observed != PHASE3C_EXPECTED_BASELINE:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_BASELINE_DRIFT:" + json.dumps(observed, sort_keys=True))


def summarize_v3(path: Path) -> dict[str, Any]:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        company = conn.execute("SELECT COUNT(*) AS total, SUM(active=1) AS active, SUM(active=0) AS inactive FROM v3_company").fetchone()
        return {
            "company_total": int(company["total"]),
            "active": int(company["active"] or 0),
            "inactive": int(company["inactive"] or 0),
            "coverage": coverage_summary(conn),
            "integrity": production_integrity(conn),
        }


def production_integrity_for_path(path: Path) -> dict[str, Any]:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        integrity = production_integrity(conn)
        v2_created_q = conn.execute(
            """
            SELECT COUNT(*)
            FROM v3_migration_audit
            WHERE source='V2' AND evidence_json LIKE '%QUARTER_CREATED%'
            """
        ).fetchone()[0]
        integrity["v2_created_q"] = int(v2_created_q)
        return integrity


def snapshot_existing_non_null(path: Path) -> dict[str, Any]:
    fields = ", ".join(f"f.{field}" for field in CANONICAL_FIELD_NAMES)
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT c.ticker, q.fiscal_year, q.fiscal_quarter, q.publish_date, {fields}
            FROM v3_quarter q
            JOIN v3_company c ON c.company_id=q.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            ORDER BY c.ticker, q.fiscal_year, q.fiscal_quarter
            """
        ).fetchall()
    values = {}
    publish = {}
    for row in rows:
        key = f"{row['ticker']}|{row['fiscal_year']}|{row['fiscal_quarter']}"
        if row["publish_date"] is not None:
            publish[key] = row["publish_date"]
        for field in CANONICAL_FIELD_NAMES:
            if row[field] is not None:
                values[f"{key}|{field}"] = row[field]
    return {"values": values, "publish_dates": publish}


def no_overwrite_proof(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    overwritten_values = [
        {"key": key, "before": value, "after": after["values"].get(key)}
        for key, value in before["values"].items()
        if key in after["values"] and after["values"][key] != value
    ]
    overwritten_publish = [
        {"key": key, "before": value, "after": after["publish_dates"].get(key)}
        for key, value in before["publish_dates"].items()
        if key in after["publish_dates"] and after["publish_dates"][key] != value
    ]
    return {
        "existing_non_null_values_checked": len(before["values"]),
        "existing_non_null_values_overwritten": len(overwritten_values),
        "existing_publish_dates_checked": len(before["publish_dates"]),
        "existing_publish_dates_overwritten": len(overwritten_publish),
        "value_overwrite_examples": overwritten_values[:20],
        "publish_overwrite_examples": overwritten_publish[:20],
    }


def run_idempotency(*, v3_db: Path, candidates: list[V3CanonicalMigrationCandidate], migration_run_id: str, now_utc: str, after_summary: dict[str, Any]) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        before_counts = _table_counts(conn)
        issue_count_before = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
        second = V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source=V2_SOURCE, migration_run_id=migration_run_id, dry_apply=False, now_utc=now_utc).to_dict()
        after_counts = _table_counts(conn)
        issue_count_after = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
        conn.commit()
    fills = sum(int(counts.get("FIELD_FILLED_FROM_NULL", 0)) + int(counts.get("FIELD_INSERTED", 0)) + int(counts.get("FIELD_DERIVED", 0)) for counts in second["field_contributions"].values())
    return {
        "row_counts_unchanged": before_counts == after_counts,
        "second_run_q_creations": int(second["rows"].get("canonical_quarters_created", 0)),
        "new_null_fills": int(fills),
        "new_publish_fills": int(second["metadata"].get("PUBLISH_DATE_SET", 0)),
        "duplicate_semantic_issues": int(issue_count_after - issue_count_before),
        "after_company_total": after_summary["company_total"],
    }


def validate_dry_gate(*, prepared: Phase3CV2Prepared, dry_summary: dict[str, Any]) -> dict[str, Any]:
    field_fills = sum(int(counts.get("FIELD_FILLED_FROM_NULL", 0)) + int(counts.get("FIELD_DERIVED", 0)) for counts in dry_summary["field_contributions"].values())
    gate = {
        "only_confirmed_candidates_passed": all(row.value_metadata.get("identity_classification") == "SAME_QUARTER_CONFIRMED" for row in prepared.candidates),
        "no_mapping_risk_candidates_passed": all(row.value_metadata.get("apply_state") == AUTO_ENRICH_ALLOWED for row in prepared.candidates),
        "no_v2_only_q_created": int(dry_summary["rows"].get("canonical_quarters_created", 0)) == 0,
        "no_candidate_rejected": int(dry_summary["rows"].get("candidate_rows_rejected", 0)) == 0,
        "expected_field_fills": len(prepared.safe_null_fill_plan),
        "dry_field_fills": int(field_fills),
        "expected_publish_fills": len(prepared.safe_publish_fill_plan),
        "dry_publish_fills": int(dry_summary["metadata"].get("PUBLISH_DATE_SET", 0)),
        "candidate_count": len(prepared.candidates),
    }
    gate["passed"] = (
        gate["only_confirmed_candidates_passed"]
        and gate["no_mapping_risk_candidates_passed"]
        and gate["no_v2_only_q_created"]
        and gate["no_candidate_rejected"]
        and gate["expected_field_fills"] == gate["dry_field_fills"]
        and gate["expected_publish_fills"] == gate["dry_publish_fills"]
    )
    return gate


def validate_post_gate(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    production_summary: dict[str, Any],
    no_overwrite: dict[str, Any],
    idempotency: dict[str, Any],
    integrity: dict[str, Any],
) -> dict[str, Any]:
    gate = {
        "company_count_unchanged": before["company_total"] == after["company_total"] == PHASE3C_EXPECTED_BASELINE["company_total"],
        "active_unchanged": before["active"] == after["active"] == PHASE3C_EXPECTED_BASELINE["active"],
        "inactive_unchanged": before["inactive"] == after["inactive"] == PHASE3C_EXPECTED_BASELINE["inactive"],
        "no_q_created": int(production_summary["rows"].get("canonical_quarters_created", 0)) == 0,
        "no_existing_values_overwritten": no_overwrite["existing_non_null_values_overwritten"] == 0,
        "no_existing_publish_dates_overwritten": no_overwrite["existing_publish_dates_overwritten"] == 0,
        "idempotent": idempotency["row_counts_unchanged"] and idempotency["new_null_fills"] == 0 and idempotency["new_publish_fills"] == 0,
        "quick_check_ok": integrity["quick_check"] == "ok",
        "foreign_key_check_ok": integrity["foreign_key_check_rows"] == 0,
        "v2_created_q_zero": integrity["v2_created_q"] == 0,
    }
    gate["passed"] = all(gate.values())
    return gate


def v2_only_inventory(
    v2_refined: dict[tuple[str, int, str], dict[str, Any]],
    v3_by_key: dict[tuple[str, int, str], dict[str, Any]],
    legacy_rows: dict[tuple[str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    out = []
    cross = []
    active_by_ticker = {row["ticker"]: row["active"] for row in v3_by_key.values()}
    for key, row in sorted(v2_refined.items()):
        if key in v3_by_key:
            continue
        available_fields = [field for field in CANONICAL_FIELD_NAMES if row.get(field) is not None]
        legacy = legacy_rows.get((key[0], str(row.get("period_end_date"))))
        item = {
            "ticker": key[0],
            "fiscal_year": key[1],
            "fiscal_quarter": key[2],
            "v2_period_end_date": row.get("period_end_date"),
            "publish_date": row.get("publish_date"),
            "available_fields": ";".join(available_fields),
            "active": active_by_ticker.get(key[0]),
            "reason_not_imported": "PHASE3C_V2_CANNOT_CREATE_CANONICAL_Q",
        }
        out.append(item)
        cross.append(
            {
                **item,
                "legacy_crosscheck": "V2_ONLY_LEGACY_APPARENT_MATCH" if legacy is not None else "V2_ONLY_WITHOUT_LEGACY_EXACT_PERIOD_MATCH",
                "legacy_available_fields": ";".join(field for field in CANONICAL_FIELD_NAMES if legacy is not None and legacy.get(field) is not None),
            }
        )
    return out, cross


def source_boundary_status(artifact_root: Path) -> dict[str, Any]:
    existing = Path("temp/fundamentals_v3_phase3b_universe_refinement/20260822T_PHASE3B_UNIVERSE_REFINEMENT/rc_fundamentals_v3.pre_financial_refinement.db")
    return {"pre_v2_checkpoint_created": False, "reused_refined_checkpoint": str(existing), "exists": existing.exists()}


def source_contribution_with_apply(source: dict[str, Any], apply_summary: dict[str, Any]) -> dict[str, Any]:
    return {**source, "canonical_apply": {"rows": apply_summary["rows"], "metadata": apply_summary["metadata"], "fields": apply_summary["field_contributions"], "issues": apply_summary["issues"]}}


def field_contribution_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"field": field, **counts} for field, counts in summary["field_contributions"].items()]


def metadata_contribution_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"metadata_outcome": key, "count": value} for key, value in sorted(summary["metadata"].items())]


def core_pre_post_row(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "core_ready_before": before["coverage"]["core_ready_q"],
        "core_ready_after": after["coverage"]["core_ready_q"],
        "core_ready_increase": after["coverage"]["core_ready_q"] - before["coverage"]["core_ready_q"],
        "core_not_ready_before": before["coverage"]["core_not_ready_q"],
        "core_not_ready_after": after["coverage"]["core_not_ready_q"],
        "core_not_ready_decrease": before["coverage"]["core_not_ready_q"] - after["coverage"]["core_not_ready_q"],
    }


def missing_pre_post_rows(before: dict[str, Any], after: dict[str, Any], fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fill_counts = Counter(row["field"] for row in fills)
    return [
        {"field": field, "null_before": before["coverage"]["core_missing_field_breakdown"].get(field, before["coverage"]["field_missing"].get(field)), "v2_fills": fill_counts[field], "null_after": after["coverage"]["core_missing_field_breakdown"].get(field, after["coverage"]["field_missing"].get(field))}
        for field in CORE_READY_FIELDS
    ]


def publication_pre_post_row(before: dict[str, Any], after: dict[str, Any], fills: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "publish_known_before": before["coverage"]["publish_date_known"],
        "publish_null_before": before["coverage"]["publish_date_null"],
        "v2_publish_fills": len(fills),
        "publish_known_after": after["coverage"]["publish_date_known"],
        "publish_null_after": after["coverage"]["publish_date_null"],
    }


def _strict_equivalent(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    diff = abs(float(left) - float(right))
    return diff <= max(1.0, max(abs(float(left)), abs(float(right))) * 0.000001)


def _fill_row(v3: dict[str, Any], v2: dict[str, Any], identity: dict[str, Any], field_name: str, incoming: Any) -> dict[str, Any]:
    return {**_identity_key(v3), "field": field_name, "v3_value_before": v3.get(field_name), "v2_value": incoming, "identity_classification": identity["identity_classification"], "apply_state": identity["apply_state"]}


def _publish_fill_row(v3: dict[str, Any], v2: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    return {**_identity_key(v3), "v3_publish_date_before": v3.get("publish_date"), "v2_publish_date": v2.get("publish_date"), "identity_classification": identity["identity_classification"], "apply_state": identity["apply_state"]}


def _agreement_row(v3: dict[str, Any], v2: dict[str, Any], identity: dict[str, Any], field_name: str, current: Any, incoming: Any, equivalent: bool) -> dict[str, Any]:
    return {**_identity_key(v3), "field": field_name, "v3_value": current, "v2_value": incoming, "agreement_class": "STRICT_EQUIVALENT" if equivalent else "EXACT_SAME", "identity_classification": identity["identity_classification"]}


def _conflict_row(v3: dict[str, Any], v2: dict[str, Any], identity: dict[str, Any], field_name: str, current: Any, incoming: Any) -> dict[str, Any]:
    if field_name == "publish_date" or current is None or incoming is None:
        abs_diff = None
        rel_diff = None
    else:
        abs_diff = abs(float(current) - float(incoming))
        rel_diff = 0.0 if max(abs(float(current)), abs(float(incoming))) == 0 else abs_diff / max(abs(float(current)), abs(float(incoming)))
    return {**_identity_key(v3), "field": field_name, "v3_value": current, "v2_value": incoming, "absolute_difference": abs_diff, "relative_difference": rel_diff, "identity_classification": identity["identity_classification"], "disposition": "EVIDENCE_ONLY_NO_OVERWRITE"}


def _identity_key(row: dict[str, Any]) -> dict[str, Any]:
    return {"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row.get("period_end_date")}


def _table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        name: int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
        for name in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_migration_audit", "v3_resolution_issue")
    }


def _preflight_text(baseline: dict[str, Any]) -> str:
    return (
        "# Phase 3C Preflight\n\n"
        f"companies: {baseline['company_total']}\n\n"
        f"active: {baseline['active']}\n\n"
        f"inactive: {baseline['inactive']}\n\n"
        f"canonical_q: {baseline['coverage']['canonical_q_total']}\n\n"
        f"core_ready: {baseline['coverage']['core_ready_q']}\n\n"
        f"core_not_ready: {baseline['coverage']['core_not_ready_q']}\n"
    )


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
