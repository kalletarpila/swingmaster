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

from swingmaster.fundamentals.v3_canonical_migration import (
    CANONICAL_FIELD_NAMES,
    V3CanonicalMigrationCandidate,
    V3CanonicalMigrationEngine,
    V3SourceApplyPolicy,
)
from swingmaster.fundamentals.v3_core_gap_diagnostic import (
    connect_readonly,
    load_legacy_rows,
    load_v2_rows,
    load_v2_rows_by_ticker,
    load_v3_rows,
)
from swingmaster.fundamentals.v3_v2_enrichment import (
    AUTO_ENRICH_ALLOWED,
    BLOCK_NO_WRITE,
    HOLD_NO_WRITE,
    V2_REJECTED_SEMANTIC_SUBSTITUTIONS,
    V2_SOURCE,
    _agreement_row,
    _conflict_row,
    _strict_equivalent,
    classify_phase3c_identity,
    no_overwrite_proof,
    production_integrity_for_path,
    snapshot_existing_non_null,
    summarize_v3,
)
from swingmaster.fundamentals.v3_v2_enrichment_policy import (
    V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE,
    decide_v2_publish_date_action,
    decide_v2_value_action,
)


PHASE3C3_EXPECTED_BASELINE = {
    "company_total": 2552,
    "active": 2484,
    "inactive": 68,
    "canonical_q_total": 72498,
}
PHASE3C3_CLASSIFICATION = "FUNDAMENTALS_V3_PHASE3C_3_V2_RESIDUAL_EXISTING_Q_ENRICHMENT_COMPLETE"
HISTORICAL_PERIOD_END_FLOOR = date(2018, 1, 1)
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
REPORT_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)
Q_TYPE_SEC_Q4 = "RECONSTRUCTED_SEC_Q4"
Q_TYPE_YAHOO = "YAHOO_SEEDED_Q"
Q_TYPE_LEGACY = "LEGACY_EXPLICIT_HISTORICAL_Q"
Q_TYPE_V2 = "PREVIOUSLY_ENRICHED_BY_V2"
Q_TYPE_UNKNOWN = "UNKNOWN_CANONICAL_Q_TYPE"


@dataclass(frozen=True)
class Phase3C3Prepared:
    baseline: dict[str, Any]
    identity_rows: list[dict[str, Any]]
    mapping_risk_rows: list[dict[str, Any]]
    safe_null_fill_plan: list[dict[str, Any]]
    safe_publish_fill_plan: list[dict[str, Any]]
    non_null_agreement_rows: list[dict[str, Any]]
    non_null_conflict_rows: list[dict[str, Any]]
    candidates: list[V3CanonicalMigrationCandidate]
    v2_only_rows: list[dict[str, Any]]
    v2_only_mapping_risk_rows: list[dict[str, Any]]
    year_overlap_rows: list[dict[str, Any]]
    q4_rows: list[dict[str, Any]]
    phase4c_inventory_rows: list[dict[str, Any]]
    source_contribution: dict[str, Any]
    q_type_by_quarter_id: dict[int, str]
    pre_snapshot: dict[str, Any]


def run_v2_residual_existing_q(
    *,
    v3_db: Path,
    v2_db: Path,
    legacy_db: Path,
    artifact_root: Path,
    migration_run_id: str,
    now_utc: str,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    prepared = prepare_v2_residual_existing_q(
        v3_db=v3_db,
        v2_db=v2_db,
        legacy_db=legacy_db,
        migration_run_id=migration_run_id,
    )
    _write_pre_apply_artifacts(artifact_root, prepared)

    dry_summary = _apply_candidates(v3_db, prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc, dry_apply=True)
    dry_gate = validate_dry_gate(prepared=prepared, dry_summary=dry_summary)
    _write_json(artifact_root / "dry_apply_summary.json", {"summary": dry_summary, "gate": dry_gate})
    if not dry_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_3_DRY_GATE_FAILED:" + json.dumps(dry_gate, sort_keys=True))

    backup = create_source_boundary_backup(v3_db=v3_db, artifact_root=artifact_root)
    before_summary = summarize_v3(v3_db)
    before_summary["core_gap_profile"] = core_gap_profile(v3_db)
    before_snapshot = prepared.pre_snapshot
    production_summary = _apply_candidates(v3_db, prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc, dry_apply=False)
    after_summary = summarize_v3(v3_db)
    after_summary["core_gap_profile"] = core_gap_profile(v3_db)
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
    )

    contribution = source_contribution_with_apply(prepared.source_contribution, production_summary)
    _write_json(artifact_root / "v2_residual_source_contribution.json", contribution)
    _write_csv(artifact_root / "v2_field_contribution.csv", field_contribution_rows(production_summary))
    _write_csv(artifact_root / "v2_contribution_by_q_type.csv", contribution_by_q_type_rows(prepared.safe_null_fill_plan, prepared.safe_publish_fill_plan))
    _write_csv(artifact_root / "v2_metadata_contribution.csv", metadata_contribution_rows(production_summary, prepared.safe_publish_fill_plan))
    _write_csv(artifact_root / "core_readiness_pre_post.csv", [core_readiness_pre_post(before_summary, after_summary)])
    _write_csv(artifact_root / "field_nulls_pre_post.csv", field_nulls_pre_post(before_summary, after_summary, prepared.safe_null_fill_plan))
    _write_csv(artifact_root / "ebit_ebitda_pre_post.csv", ebit_ebitda_pre_post(before_summary, after_summary, prepared.safe_null_fill_plan))
    _write_csv(artifact_root / "phase4c_ebit_ebitda_derivation_inventory.csv", build_phase4c_inventory(v3_db))
    _write_json(artifact_root / "phase3c4_baseline.json", phase3c4_baseline(after_summary, prepared.v2_only_rows, prepared.v2_only_mapping_risk_rows))
    (artifact_root / "no_overwrite_proof.md").write_text(json.dumps(no_overwrite, indent=2, sort_keys=True) + "\n")
    (artifact_root / "idempotency_validation.md").write_text(json.dumps(idempotency, indent=2, sort_keys=True) + "\n")
    (artifact_root / "production_integrity.md").write_text(json.dumps(integrity, indent=2, sort_keys=True) + "\n")
    (artifact_root / "recommended_next_step.md").write_text("MASTER PLAN PHASE 3C-4 - V2 RESIDUAL HISTORICAL GAP FILL\n")
    summary = {
        "classification": PHASE3C3_CLASSIFICATION,
        "migration_run_id": migration_run_id,
        "pre_baseline": before_summary,
        "post_baseline": after_summary,
        "identity": prepared.source_contribution["identity"],
        "year_overlap": prepared.year_overlap_rows,
        "safe_null_fills_planned": len(prepared.safe_null_fill_plan),
        "safe_publish_fills_planned": len(prepared.safe_publish_fill_plan),
        "v2_only_historical_q_candidates": len(prepared.v2_only_rows),
        "v2_only_historical_mapping_risk": len(prepared.v2_only_mapping_risk_rows),
        "production_apply": production_summary,
        "source_contribution": contribution,
        "no_overwrite": no_overwrite,
        "idempotency": idempotency,
        "integrity": integrity,
        "post_gate": post_gate,
        "source_boundary_backup": backup,
        "legacy_canonical_contribution": 0,
        "provider_calls": {"yahoo": 0, "sec": 0, "simfin": 0, "network": 0},
    }
    if not post_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_3_POST_GATE_FAILED:" + json.dumps(post_gate, sort_keys=True))
    _write_json(artifact_root / "summary.json", summary)
    write_durable_doc(Path("docs/fundamentals_v3_phase3c_3_v2_residual_existing_q_enrichment.md"), artifact_root, summary)
    return summary


def prepare_v2_residual_existing_q(*, v3_db: Path, v2_db: Path, legacy_db: Path, migration_run_id: str) -> Phase3C3Prepared:
    baseline = summarize_v3(v3_db)
    baseline["core_gap_profile"] = core_gap_profile(v3_db)
    assert_expanded_baseline(baseline)
    with connect_readonly(v3_db) as v3_conn, connect_readonly(v2_db) as v2_conn, connect_readonly(legacy_db) as legacy_conn:
        v3_rows = load_v3_rows(v3_conn)
        v2_rows = load_v2_rows(v2_conn)
        legacy_rows = load_legacy_rows(legacy_conn)
        q_type_by_quarter_id = load_q_types(v3_conn)
    v3_by_key = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"]): row for row in v3_rows}
    refined_tickers = {row["ticker"] for row in v3_rows}
    v2_refined = {
        key: row
        for key, row in v2_rows.items()
        if key[0] in refined_tickers and not _is_pre_2018(row.get("period_end_date"))
    }
    v2_by_ticker = load_v2_rows_by_ticker(v2_refined)

    identity_rows: list[dict[str, Any]] = []
    mapping_risk_rows: list[dict[str, Any]] = []
    safe_null_fill_plan: list[dict[str, Any]] = []
    safe_publish_fill_plan: list[dict[str, Any]] = []
    agreement_rows: list[dict[str, Any]] = []
    conflict_rows: list[dict[str, Any]] = []
    candidate_groups: dict[tuple[str, int, str], dict[str, Any]] = {}
    class_counts: Counter[str] = Counter()
    apply_counts: Counter[str] = Counter()

    for key, v3 in sorted(v3_by_key.items()):
        v2 = v2_refined.get(key)
        if v2 is None:
            continue
        q_type = q_type_by_quarter_id.get(int(v3["quarter_id"]), Q_TYPE_UNKNOWN)
        identity = classify_phase3c_identity(v3, v2, v2_by_ticker.get(key[0], []))
        identity["canonical_q_type"] = q_type
        class_counts[identity["identity_classification"]] += 1
        apply_counts[identity["apply_state"]] += 1
        identity_rows.append(identity)
        if identity["apply_state"] == BLOCK_NO_WRITE or identity["identity_classification"] in {"MAPPING_RISK", "CLEAR_WRONG_QUARTER", "PERIOD_IDENTITY_CONFLICT"}:
            mapping_risk_rows.append(identity)
        if identity["apply_state"] != AUTO_ENRICH_ALLOWED:
            continue
        for field_name in REPORT_FIELDS:
            if field_name in V2_REJECTED_SEMANTIC_SUBSTITUTIONS:
                continue
            current = v3.get(field_name)
            incoming = v2.get(field_name)
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
            if action == "FILL_NULL":
                fill = _fill_plan_row(v3, v2, identity, field_name, incoming, q_type)
                safe_null_fill_plan.append(fill)
                _group_field_candidate(candidate_groups, v3, v2, identity, field_name, incoming, q_type)
            elif action == "CONFIRM_ONLY":
                row = _agreement_row(v3, v2, identity, field_name, current, incoming, equivalent)
                row["canonical_q_type"] = q_type
                agreement_rows.append(row)
            elif action == "CONFLICT_NO_OVERWRITE":
                row = _conflict_row(v3, v2, identity, field_name, current, incoming)
                row["canonical_q_type"] = q_type
                conflict_rows.append(row)
        publish_action = decide_v2_publish_date_action(
            existing_publish_date=v3.get("publish_date"),
            v2_publish_date=v2.get("publish_date"),
            same_quarter_confirmed=True,
        )
        if publish_action == "FILL_NULL":
            publish_row = _publish_plan_row(v3, v2, identity, q_type)
            safe_publish_fill_plan.append(publish_row)
            _group_publish_candidate(candidate_groups, v3, v2, identity, q_type)
        elif publish_action == "CONFLICT_NO_OVERWRITE":
            row = _conflict_row(v3, v2, identity, "publish_date", v3.get("publish_date"), v2.get("publish_date"))
            row["canonical_q_type"] = q_type
            conflict_rows.append(row)

    v2_only_rows, v2_only_mapping_risk_rows = v2_only_inventory(v2_refined, v3_by_key, legacy_rows)
    candidates = build_grouped_candidates(candidate_groups, migration_run_id)
    year_overlap_rows = year_overlap(v3_rows, identity_rows)
    q4_rows = q4_v2_enrichment_rows(identity_rows, safe_null_fill_plan, q_type_by_quarter_id)
    phase4c_inventory_rows = build_phase4c_inventory(v3_db)
    source_contribution = {
        "identity": identity_summary(v2_refined, identity_rows, class_counts, apply_counts),
        "planned_field_fills": dict(sorted(Counter(row["field"] for row in safe_null_fill_plan).items())),
        "planned_publish_fills": len(safe_publish_fill_plan),
        "non_null_conflicts": dict(sorted(Counter(row["field"] for row in conflict_rows).items())),
        "non_null_agreements": dict(sorted(Counter(row["field"] for row in agreement_rows).items())),
        "blocked_total": apply_counts[BLOCK_NO_WRITE] + apply_counts[HOLD_NO_WRITE],
        "semantic_rejections": {field: "WEIGHTED_AVERAGE_SHARES_NOT_CANONICAL_SHARES_OUTSTANDING" for field in V2_REJECTED_SEMANTIC_SUBSTITUTIONS},
        "legacy_canonical_contribution": 0,
    }
    return Phase3C3Prepared(
        baseline=baseline,
        identity_rows=identity_rows,
        mapping_risk_rows=mapping_risk_rows,
        safe_null_fill_plan=safe_null_fill_plan,
        safe_publish_fill_plan=safe_publish_fill_plan,
        non_null_agreement_rows=agreement_rows,
        non_null_conflict_rows=conflict_rows,
        candidates=candidates,
        v2_only_rows=v2_only_rows,
        v2_only_mapping_risk_rows=v2_only_mapping_risk_rows,
        year_overlap_rows=year_overlap_rows,
        q4_rows=q4_rows,
        phase4c_inventory_rows=phase4c_inventory_rows,
        source_contribution=source_contribution,
        q_type_by_quarter_id=q_type_by_quarter_id,
        pre_snapshot=snapshot_existing_non_null(v3_db),
    )


def assert_expanded_baseline(summary: dict[str, Any]) -> None:
    observed = {
        "company_total": summary["company_total"],
        "active": summary["active"],
        "inactive": summary["inactive"],
        "canonical_q_total": summary["coverage"]["canonical_q_total"],
    }
    if observed != PHASE3C3_EXPECTED_BASELINE:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_3_BASELINE_DRIFT:" + json.dumps(observed, sort_keys=True))


def load_q_types(conn: sqlite3.Connection) -> dict[int, str]:
    rows = conn.execute(
        """
        SELECT q.quarter_id,
               MAX(CASE WHEN a.source = 'YAHOO' THEN 1 ELSE 0 END) AS has_yahoo,
               MAX(CASE WHEN a.source = 'LEGACY' THEN 1 ELSE 0 END) AS has_legacy,
               MAX(CASE WHEN a.source = 'V2' THEN 1 ELSE 0 END) AS has_v2,
               MAX(CASE WHEN a.source = 'LEGACY' AND a.evidence_json LIKE '%SEC_Q4%' THEN 1 ELSE 0 END) AS has_sec_q4
        FROM v3_quarter q
        LEFT JOIN v3_migration_audit a ON a.quarter_id = q.quarter_id
        GROUP BY q.quarter_id
        """
    ).fetchall()
    out: dict[int, str] = {}
    for row in rows:
        if row["has_sec_q4"]:
            q_type = Q_TYPE_SEC_Q4
        elif row["has_yahoo"]:
            q_type = Q_TYPE_YAHOO
        elif row["has_legacy"]:
            q_type = Q_TYPE_LEGACY
        elif row["has_v2"]:
            q_type = Q_TYPE_V2
        else:
            q_type = Q_TYPE_UNKNOWN
        out[int(row["quarter_id"])] = q_type
    return out


def _group_field_candidate(
    groups: dict[tuple[str, int, str], dict[str, Any]],
    v3: dict[str, Any],
    v2: dict[str, Any],
    identity: dict[str, Any],
    field_name: str,
    incoming: Any,
    q_type: str,
) -> None:
    group = _candidate_group(groups, v3, v2, identity, q_type)
    group["values"][field_name] = incoming


def _group_publish_candidate(
    groups: dict[tuple[str, int, str], dict[str, Any]],
    v3: dict[str, Any],
    v2: dict[str, Any],
    identity: dict[str, Any],
    q_type: str,
) -> None:
    group = _candidate_group(groups, v3, v2, identity, q_type)
    group["publish_date"] = v2.get("publish_date")


def _candidate_group(
    groups: dict[tuple[str, int, str], dict[str, Any]],
    v3: dict[str, Any],
    v2: dict[str, Any],
    identity: dict[str, Any],
    q_type: str,
) -> dict[str, Any]:
    key = (v3["ticker"], v3["fiscal_year"], v3["fiscal_quarter"])
    if key not in groups:
        groups[key] = {"v3": v3, "v2": v2, "identity": identity, "q_type": q_type, "values": {}, "publish_date": None}
    return groups[key]


def build_grouped_candidates(groups: dict[tuple[str, int, str], dict[str, Any]], migration_run_id: str) -> list[V3CanonicalMigrationCandidate]:
    candidates: list[V3CanonicalMigrationCandidate] = []
    for key, group in sorted(groups.items()):
        del key
        values = dict(group["values"])
        publish_date = group["publish_date"]
        semantic_rejections = ()
        if "operating_cashflow" in values and "capex" in values and "free_cashflow" not in values:
            semantic_rejections = ("free_cashflow",)
        candidates.append(
            _candidate(
                group["v3"],
                group["v2"],
                group["identity"],
                values=values,
                publish_date=publish_date,
                q_type=group["q_type"],
                source_record_suffix=_group_suffix(values, publish_date),
                migration_run_id=migration_run_id,
                field_semantic_differences=semantic_rejections,
            )
        )
    return candidates


def _group_suffix(values: dict[str, Any], publish_date: str | None) -> str:
    parts = sorted(values)
    if publish_date is not None:
        parts.append("publish_date")
    return "fields_" + "_".join(parts)


def _candidate(
    v3: dict[str, Any],
    v2: dict[str, Any],
    identity: dict[str, Any],
    *,
    values: dict[str, Any],
    publish_date: str | None,
    q_type: str,
    source_record_suffix: str,
    migration_run_id: str,
    field_semantic_differences: tuple[str, ...] = (),
) -> V3CanonicalMigrationCandidate:
    return V3CanonicalMigrationCandidate(
        source_system=V2_SOURCE,
        source_record_id=f"V2_RESIDUAL:{v3['ticker']}:{v3['fiscal_year']}:{v3['fiscal_quarter']}:{source_record_suffix}",
        migration_run_id=migration_run_id,
        market=v3["market"],
        ticker=v3["ticker"],
        fiscal_year=v3["fiscal_year"],
        fiscal_quarter=v3["fiscal_quarter"],
        period_end_date=v3.get("period_end_date"),
        publish_date=publish_date,
        values=values,
        candidate_can_create_quarter=False,
        period_date_policy="CONFLICT",
        field_semantic_differences=field_semantic_differences,
        value_metadata={
            "phase": "PHASE3C_3_V2_RESIDUAL_EXISTING_Q",
            "identity_classification": identity["identity_classification"],
            "apply_state": identity["apply_state"],
            "canonical_q_type": q_type,
            "v2_period_end_date": v2.get("period_end_date"),
            "field_or_metadata": source_record_suffix,
            "no_overwrite_policy": V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE,
        },
    )


def v2_only_inventory(
    v2_refined: dict[tuple[str, int, str], dict[str, Any]],
    v3_by_key: dict[tuple[str, int, str], dict[str, Any]],
    legacy_rows: dict[tuple[str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    v3_by_ticker_period = {(row["ticker"], str(row.get("period_end_date"))): row for row in v3_by_key.values()}
    active_by_ticker = {row["ticker"]: row["active"] for row in v3_by_key.values()}
    candidates: list[dict[str, Any]] = []
    risk: list[dict[str, Any]] = []
    for key, row in sorted(v2_refined.items()):
        if key in v3_by_key:
            continue
        legacy = legacy_rows.get((key[0], str(row.get("period_end_date"))))
        same_period_v3 = v3_by_ticker_period.get((key[0], str(row.get("period_end_date"))))
        risk_class = "ACTUAL_MISSING_CANONICAL_Q"
        if same_period_v3 is not None:
            risk_class = "DUPLICATE_OR_PERIOD_VARIANT_OF_EXISTING_CANONICAL_Q"
        elif legacy is None:
            risk_class = "INSUFFICIENT_LOCAL_CORROBORATION"
        item = {
            "ticker": key[0],
            "fiscal_year": key[1],
            "fiscal_quarter": key[2],
            "v2_period_end_date": row.get("period_end_date"),
            "publish_date": row.get("publish_date"),
            "available_fields": ";".join(field for field in REPORT_FIELDS if row.get(field) is not None),
            "active": active_by_ticker.get(key[0]),
            "v2_identity_confidence": "NOT_EVALUATED_EXISTING_Q_ABSENT",
            "legacy_counterpart_status": "LEGACY_EXACT_PERIOD_MATCH" if legacy is not None else "NO_LEGACY_EXACT_PERIOD_MATCH",
            "canonical_gap_classification": risk_class,
            "reason_canonical_q_absent": "PHASE3C3_EXISTING_Q_ONLY_NO_CREATE",
        }
        candidates.append(item)
        if risk_class != "ACTUAL_MISSING_CANONICAL_Q":
            risk.append(item)
    return candidates, risk


def year_overlap(v3_rows: list[dict[str, Any]], identity_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    canonical = Counter(_year(row.get("period_end_date")) for row in v3_rows if _year(row.get("period_end_date")) is not None)
    candidates = Counter(_year(row.get("v3_period_end_date")) for row in identity_rows if _year(row.get("v3_period_end_date")) is not None)
    confirmed = Counter(_year(row.get("v3_period_end_date")) for row in identity_rows if row["identity_classification"] == "SAME_QUARTER_CONFIRMED" and _year(row.get("v3_period_end_date")) is not None)
    rows = []
    for year in range(2018, 2027):
        overlap = candidates[year]
        rows.append(
            {
                "year": year,
                "canonical_qs": canonical[year],
                "v2_identity_candidates": overlap,
                "same_quarter_confirmed": confirmed[year],
                "identity_confirmation_pct": round((confirmed[year] / overlap * 100.0) if overlap else 0.0, 2),
            }
        )
    return rows


def identity_summary(v2_refined: dict[tuple[str, int, str], dict[str, Any]], identity_rows: list[dict[str, Any]], class_counts: Counter[str], apply_counts: Counter[str]) -> dict[str, Any]:
    return {
        "v2_source_quarters_examined": len(v2_refined),
        "exact_ticker_fy_fq_candidates": len(identity_rows),
        "period_compatible_matches": sum(1 for row in identity_rows if row["period_relation"] in {"EXACT_PERIOD_END", "SMALL_KNOWN_PROVIDER_VARIANT", "KNOWN_FISCAL_CALENDAR_VARIANT"}),
        "same_quarter_confirmed": class_counts["SAME_QUARTER_CONFIRMED"],
        "probable_ambiguous": class_counts["PROBABLE_SAME_QUARTER"] + class_counts["AMBIGUOUS"],
        "insufficient_evidence": class_counts["INSUFFICIENT_EVIDENCE"],
        "possible_wrong_quarter": class_counts["MAPPING_RISK"],
        "clear_wrong_quarter": class_counts["CLEAR_WRONG_QUARTER"],
        "period_identity_conflict": class_counts["PERIOD_IDENTITY_CONFLICT"],
        "blocked_total": apply_counts[BLOCK_NO_WRITE] + apply_counts[HOLD_NO_WRITE],
        "identity_classes": dict(sorted(class_counts.items())),
        "apply_states": dict(sorted(apply_counts.items())),
    }


def q4_v2_enrichment_rows(identity_rows: list[dict[str, Any]], fills: list[dict[str, Any]], q_type_by_quarter_id: dict[int, str]) -> list[dict[str, Any]]:
    del q_type_by_quarter_id
    fill_counts = Counter((row["ticker"], row["fiscal_year"], row["fiscal_quarter"], row["field"]) for row in fills if row["canonical_q_type"] == Q_TYPE_SEC_Q4)
    rows = []
    for row in identity_rows:
        if row.get("canonical_q_type") != Q_TYPE_SEC_Q4:
            continue
        rows.append(
            {
                "ticker": row["ticker"],
                "fiscal_year": row["fiscal_year"],
                "fiscal_quarter": row["fiscal_quarter"],
                "period_end_date": row["v3_period_end_date"],
                "identity_classification": row["identity_classification"],
                "v2_direct_ebit_fill": fill_counts[(row["ticker"], row["fiscal_year"], row["fiscal_quarter"], "ebit")],
                "v2_direct_ebitda_fill": fill_counts[(row["ticker"], row["fiscal_year"], row["fiscal_quarter"], "ebitda")],
            }
        )
    return rows


def build_phase4c_inventory(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        q_types = load_q_types(conn)
        rows = conn.execute(
            """
            SELECT q.quarter_id, c.ticker, q.fiscal_year, q.fiscal_quarter, q.period_end_date,
                   f.ebit, f.ebitda, f.operating_income, f.accepted_source_provider
            FROM v3_quarter q
            JOIN v3_company c ON c.company_id = q.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id = q.quarter_id
            WHERE f.ebit IS NULL OR f.ebitda IS NULL
            ORDER BY c.ticker, q.fiscal_year, q.fiscal_quarter
            """
        ).fetchall()
    return [
        {
            "ticker": row["ticker"],
            "fiscal_year": row["fiscal_year"],
            "fiscal_quarter": row["fiscal_quarter"],
            "period_end_date": row["period_end_date"],
            "missing_ebit": int(row["ebit"] is None),
            "missing_ebitda": int(row["ebitda"] is None),
            "operating_income_available": int(row["operating_income"] is not None),
            "canonical_q_type": q_types.get(int(row["quarter_id"]), Q_TYPE_UNKNOWN),
            "accepted_source_provider": row["accepted_source_provider"],
            "phase4c_action": "RESEARCH_DERIVATION_ONLY_NO_PHASE3C3_DERIVATION",
        }
        for row in rows
    ]


def validate_dry_gate(*, prepared: Phase3C3Prepared, dry_summary: dict[str, Any]) -> dict[str, Any]:
    field_fills = _field_fill_total(dry_summary)
    gate = {
        "only_confirmed_candidates_passed": all(candidate.value_metadata.get("identity_classification") == "SAME_QUARTER_CONFIRMED" for candidate in prepared.candidates),
        "no_mapping_risk_candidates_passed": all(candidate.value_metadata.get("apply_state") == AUTO_ENRICH_ALLOWED for candidate in prepared.candidates),
        "no_v2_only_q_created": int(dry_summary["rows"].get("canonical_quarters_created", 0)) == 0,
        "no_candidate_rejected": int(dry_summary["rows"].get("candidate_rows_rejected", 0)) == 0,
        "no_field_conflicts": sum(int(counts.get("FIELD_CONFLICT", 0)) for counts in dry_summary["field_contributions"].values()) == 0,
        "expected_field_fills": len(prepared.safe_null_fill_plan),
        "dry_field_fills": field_fills,
        "expected_publish_fills": len(prepared.safe_publish_fill_plan),
        "dry_publish_fills": int(dry_summary["metadata"].get("PUBLISH_DATE_SET", 0)),
        "no_pre_2018_candidates": all(not _is_pre_2018(candidate.period_end_date) for candidate in prepared.candidates),
        "candidate_count": len(prepared.candidates),
    }
    gate["passed"] = all(value for key, value in gate.items() if key not in {"expected_field_fills", "dry_field_fills", "expected_publish_fills", "dry_publish_fills", "candidate_count"}) and gate["expected_field_fills"] == gate["dry_field_fills"] and gate["expected_publish_fills"] == gate["dry_publish_fills"]
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
        "company_count_unchanged": before["company_total"] == after["company_total"] == PHASE3C3_EXPECTED_BASELINE["company_total"],
        "active_unchanged": before["active"] == after["active"] == PHASE3C3_EXPECTED_BASELINE["active"],
        "inactive_unchanged": before["inactive"] == after["inactive"] == PHASE3C3_EXPECTED_BASELINE["inactive"],
        "canonical_q_unchanged": before["coverage"]["canonical_q_total"] == after["coverage"]["canonical_q_total"] == PHASE3C3_EXPECTED_BASELINE["canonical_q_total"],
        "no_q_created": int(production_summary["rows"].get("canonical_quarters_created", 0)) == 0,
        "no_existing_values_overwritten": no_overwrite["existing_non_null_values_overwritten"] == 0,
        "no_existing_publish_dates_overwritten": no_overwrite["existing_publish_dates_overwritten"] == 0,
        "idempotent": idempotency["row_counts_unchanged"] and idempotency["new_null_fills"] == 0 and idempotency["new_publish_fills"] == 0 and idempotency["duplicate_semantic_issues"] == 0,
        "quick_check_ok": integrity["quick_check"] == "ok",
        "foreign_key_check_ok": integrity["foreign_key_check_rows"] == 0,
        "v2_created_q_zero": integrity["v2_created_q"] == 0,
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
        "second_run_q_creations": int(second["rows"].get("canonical_quarters_created", 0)),
        "new_null_fills": _field_fill_total(second),
        "new_publish_fills": int(second["metadata"].get("PUBLISH_DATE_SET", 0)),
        "duplicate_semantic_issues": int(issue_count_after - issue_count_before),
    }


def create_source_boundary_backup(*, v3_db: Path, artifact_root: Path) -> dict[str, Any]:
    backup = artifact_root / "rc_fundamentals_v3_pre_phase3c3_backup.db"
    if not backup.exists():
        shutil.copy2(v3_db, backup)
    with sqlite3.connect(backup) as conn:
        quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
        fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    return {
        "path": str(backup),
        "size_bytes": backup.stat().st_size,
        "quick_check": quick_check,
        "foreign_key_check_rows": len(fk_rows),
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


def _write_pre_apply_artifacts(root: Path, prepared: Phase3C3Prepared) -> None:
    _write_json(root / "v3_pre_v2_residual_baseline.json", prepared.baseline)
    (root / "preflight.md").write_text(_preflight_text(prepared.baseline, prepared.source_contribution["identity"]))
    _write_csv(root / "v2_existing_q_identity_classification.csv", prepared.identity_rows)
    _write_csv(root / "v2_mapping_risk.csv", prepared.mapping_risk_rows)
    _write_csv(root / "safe_null_fill_plan.csv", prepared.safe_null_fill_plan)
    _write_csv(root / "safe_publish_fill_plan.csv", prepared.safe_publish_fill_plan)
    _write_json(root / "v2_residual_source_contribution.json", prepared.source_contribution)
    _write_csv(root / "v2_non_null_agreement.csv", prepared.non_null_agreement_rows)
    _write_csv(root / "v2_non_null_conflicts.csv", prepared.non_null_conflict_rows)
    _write_csv(root / "q4_v2_enrichment.csv", prepared.q4_rows)
    _write_csv(root / "phase4c_ebit_ebitda_derivation_inventory.csv", prepared.phase4c_inventory_rows)
    _write_csv(root / "v2_only_historical_q_candidates.csv", prepared.v2_only_rows)
    _write_csv(root / "v2_only_historical_mapping_risk.csv", prepared.v2_only_mapping_risk_rows)
    _write_json(root / "phase3c4_baseline.json", phase3c4_baseline(prepared.baseline, prepared.v2_only_rows, prepared.v2_only_mapping_risk_rows))


def _fill_plan_row(v3: dict[str, Any], v2: dict[str, Any], identity: dict[str, Any], field_name: str, incoming: Any, q_type: str) -> dict[str, Any]:
    return {
        "ticker": v3["ticker"],
        "fiscal_year": v3["fiscal_year"],
        "fiscal_quarter": v3["fiscal_quarter"],
        "period_end_date": v3.get("period_end_date"),
        "v2_period_end_date": v2.get("period_end_date"),
        "field": field_name,
        "v3_value_before": v3.get(field_name),
        "v2_value": incoming,
        "identity_classification": identity["identity_classification"],
        "apply_state": identity["apply_state"],
        "canonical_q_type": q_type,
    }


def _publish_plan_row(v3: dict[str, Any], v2: dict[str, Any], identity: dict[str, Any], q_type: str) -> dict[str, Any]:
    return {
        "ticker": v3["ticker"],
        "fiscal_year": v3["fiscal_year"],
        "fiscal_quarter": v3["fiscal_quarter"],
        "period_end_date": v3.get("period_end_date"),
        "v2_period_end_date": v2.get("period_end_date"),
        "v3_publish_date_before": v3.get("publish_date"),
        "v2_publish_date": v2.get("publish_date"),
        "identity_classification": identity["identity_classification"],
        "apply_state": identity["apply_state"],
        "canonical_q_type": q_type,
    }


def source_contribution_with_apply(source: dict[str, Any], apply_summary: dict[str, Any]) -> dict[str, Any]:
    return {**source, "canonical_apply": {"rows": apply_summary["rows"], "metadata": apply_summary["metadata"], "fields": apply_summary["field_contributions"], "issues": apply_summary["issues"]}}


def field_contribution_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"field": field, **counts} for field, counts in summary["field_contributions"].items()]


def contribution_by_q_type_rows(field_fills: list[dict[str, Any]], publish_fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str]] = Counter()
    for row in field_fills:
        counts[(row["canonical_q_type"], row["field"], "FIELD_FILLED_FROM_NULL")] += 1
    for row in publish_fills:
        counts[(row["canonical_q_type"], "publish_date", "PUBLISH_DATE_SET")] += 1
    return [{"canonical_q_type": q_type, "field_or_metadata": field, "outcome": outcome, "count": count} for (q_type, field, outcome), count in sorted(counts.items())]


def metadata_contribution_rows(summary: dict[str, Any], publish_fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [{"metadata_outcome": key, "count": value, "canonical_q_type": "ALL"} for key, value in sorted(summary["metadata"].items())]
    by_type = Counter(row["canonical_q_type"] for row in publish_fills)
    rows.extend({"metadata_outcome": "PUBLISH_DATE_SET", "count": count, "canonical_q_type": q_type} for q_type, count in sorted(by_type.items()))
    return rows


def core_readiness_pre_post(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "core_ready_before": before["coverage"]["core_ready_q"],
        "core_ready_after": after["coverage"]["core_ready_q"],
        "core_ready_increase": after["coverage"]["core_ready_q"] - before["coverage"]["core_ready_q"],
        "core_not_ready_before": before["coverage"]["core_not_ready_q"],
        "core_not_ready_after": after["coverage"]["core_not_ready_q"],
        "missing_ebitda_only_before": before["core_gap_profile"]["missing_ebitda_only"],
        "missing_ebitda_only_after": after["core_gap_profile"]["missing_ebitda_only"],
        "other_core_not_ready_after": after["core_gap_profile"]["other_core_not_ready"],
    }


def field_nulls_pre_post(before: dict[str, Any], after: dict[str, Any], fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fill_counts = Counter(row["field"] for row in fills)
    return [
        {
            "field": field,
            "null_before": before["coverage"]["field_missing"].get(field),
            "v2_fills": fill_counts[field],
            "null_after": after["coverage"]["field_missing"].get(field),
        }
        for field in REPORT_FIELDS
    ] + [
        {
            "field": "publish_date",
            "null_before": before["coverage"]["publish_date_null"],
            "v2_fills": None,
            "null_after": after["coverage"]["publish_date_null"],
        }
    ]


def ebit_ebitda_pre_post(before: dict[str, Any], after: dict[str, Any], fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((row["field"], row["canonical_q_type"]) for row in fills)
    return [
        {
            "field": field,
            "null_before": before["coverage"]["field_missing"][field],
            "v2_direct_fills": sum(counts[(field, q_type)] for q_type in (Q_TYPE_SEC_Q4, Q_TYPE_YAHOO, Q_TYPE_LEGACY, Q_TYPE_V2, Q_TYPE_UNKNOWN)),
            "null_after": after["coverage"]["field_missing"][field],
            "reconstructed_sec_q4_fills": counts[(field, Q_TYPE_SEC_Q4)],
            "legacy_explicit_historical_q_fills": counts[(field, Q_TYPE_LEGACY)],
            "yahoo_seeded_q_fills": counts[(field, Q_TYPE_YAHOO)],
        }
        for field in ("ebit", "ebitda")
    ]


def phase3c4_baseline(summary: dict[str, Any], v2_only_rows: list[dict[str, Any]], v2_only_mapping_risk_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "canonical_q_total": summary["coverage"]["canonical_q_total"],
        "core_ready_q": summary["coverage"]["core_ready_q"],
        "core_not_ready_q": summary["coverage"]["core_not_ready_q"],
        "v2_only_historical_q_candidates": len(v2_only_rows),
        "v2_only_historical_mapping_risk": len(v2_only_mapping_risk_rows),
        "historical_period_end_floor": HISTORICAL_PERIOD_END_FLOOR.isoformat(),
    }


def core_gap_profile(v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        missing_ebitda_only = conn.execute(
            """
            SELECT COUNT(*)
            FROM v3_quarter_fundamentals
            WHERE revenue IS NOT NULL
              AND ebitda IS NULL
              AND free_cashflow IS NOT NULL
              AND cash IS NOT NULL
              AND total_debt IS NOT NULL
              AND shares_outstanding IS NOT NULL
              AND shares_outstanding > 0
            """
        ).fetchone()[0]
        core_not_ready = conn.execute(
            """
            SELECT COUNT(*)
            FROM v3_quarter_fundamentals
            WHERE NOT (
                revenue IS NOT NULL
                AND ebitda IS NOT NULL
                AND free_cashflow IS NOT NULL
                AND cash IS NOT NULL
                AND total_debt IS NOT NULL
                AND shares_outstanding IS NOT NULL
                AND shares_outstanding > 0
            )
            """
        ).fetchone()[0]
    return {
        "missing_ebitda_only": int(missing_ebitda_only),
        "other_core_not_ready": int(core_not_ready - missing_ebitda_only),
    }


def _field_fill_total(summary: dict[str, Any]) -> int:
    return sum(int(counts.get("FIELD_FILLED_FROM_NULL", 0)) + int(counts.get("FIELD_DERIVED", 0)) for counts in summary["field_contributions"].values())


def _table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        name: int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
        for name in ("v3_company", "v3_quarter", "v3_quarter_fundamentals", "v3_migration_audit", "v3_resolution_issue")
    }


def _is_pre_2018(value: Any) -> bool:
    if not value:
        return False
    return date.fromisoformat(str(value)) < HISTORICAL_PERIOD_END_FLOOR


def _year(value: Any) -> int | None:
    if not value:
        return None
    return date.fromisoformat(str(value)).year


def _preflight_text(baseline: dict[str, Any], identity: dict[str, Any]) -> str:
    return (
        "# Phase 3C-3 V2 Residual Existing-Q Preflight\n\n"
        f"companies: {baseline['company_total']}\n\n"
        f"active: {baseline['active']}\n\n"
        f"inactive: {baseline['inactive']}\n\n"
        f"canonical_q: {baseline['coverage']['canonical_q_total']}\n\n"
        f"core_ready: {baseline['coverage']['core_ready_q']}\n\n"
        f"core_not_ready: {baseline['coverage']['core_not_ready_q']}\n\n"
        f"publish_null: {baseline['coverage']['publish_date_null']}\n\n"
        f"v2_source_rows_examined: {identity['v2_source_quarters_examined']}\n\n"
        f"same_quarter_confirmed: {identity['same_quarter_confirmed']}\n\n"
        "policy: V2 null-fill only; no canonical Q creation; no non-null overwrite; no network.\n"
    )


def write_durable_doc(path: Path, artifact_root: Path, summary: dict[str, Any]) -> None:
    pre = summary["pre_baseline"]
    post = summary["post_baseline"]
    identity = summary["identity"]
    text = f"""# Fundamentals V3 Phase 3C-3 V2 Residual Existing-Q Enrichment

Classification: `{summary['classification']}`

Artifact root: `{artifact_root}`

Phase 3C-3 re-ran V2 after Legacy deep history expanded canonical V3 to {pre['coverage']['canonical_q_total']} quarters. V2 was used only as a confidence-gated residual source against existing canonical quarters.

Hard policy:

- `V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = {V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE}`
- V2 canonical quarter creation: `0`
- Legacy canonical contribution in this phase: `0`
- Provider/network calls: `0`

Baseline and result:

- Companies: {pre['company_total']} -> {post['company_total']}
- Active/inactive: {pre['active']}/{pre['inactive']} -> {post['active']}/{post['inactive']}
- Canonical Q: {pre['coverage']['canonical_q_total']} -> {post['coverage']['canonical_q_total']}
- Core-ready: {pre['coverage']['core_ready_q']} -> {post['coverage']['core_ready_q']}
- Core-not-ready: {pre['coverage']['core_not_ready_q']} -> {post['coverage']['core_not_ready_q']}
- Publish NULL: {pre['coverage']['publish_date_null']} -> {post['coverage']['publish_date_null']}

V2 identity:

- V2 source rows examined: {identity['v2_source_quarters_examined']}
- Exact ticker/FY/FQ candidates: {identity['exact_ticker_fy_fq_candidates']}
- Same-quarter confirmed: {identity['same_quarter_confirmed']}
- Blocked total: {identity['blocked_total']}

Contribution:

- Planned field NULL fills: {summary['safe_null_fills_planned']}
- Planned publish-date fills: {summary['safe_publish_fills_planned']}
- V2-only historical Q candidates for Phase 3C-4: {summary['v2_only_historical_q_candidates']}
- V2-only mapping-risk rows: {summary['v2_only_historical_mapping_risk']}

Integrity:

- quick_check: `{summary['integrity']['quick_check']}`
- foreign_key_check_rows: {summary['integrity']['foreign_key_check_rows']}
- existing non-null value overwrites: {summary['no_overwrite']['existing_non_null_values_overwritten']}
- existing publish-date overwrites: {summary['no_overwrite']['existing_publish_dates_overwritten']}

Next step: `MASTER PLAN PHASE 3C-4 - V2 RESIDUAL HISTORICAL GAP FILL`
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
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
