from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_canonical_migration import CANONICAL_FIELD_NAMES, V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine, V3SourceApplyPolicy
from swingmaster.fundamentals.v3_core_gap_diagnostic import IDENTITY_TOLERANCE, compare_values, connect_readonly, load_v2_rows, load_v3_rows
from swingmaster.fundamentals.v3_v2_enrichment import (
    PHASE3C_EXPECTED_BASELINE,
    _strict_equivalent,
    _table_counts,
    core_pre_post_row,
    missing_pre_post_rows,
    no_overwrite_proof,
    production_integrity_for_path,
    publication_pre_post_row,
    snapshot_existing_non_null,
    summarize_v3,
)
from swingmaster.fundamentals.v3_v2_enrichment_policy import V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE
from swingmaster.fundamentals.v3_yahoo_canonical_seed import CORE_READY_FIELDS


LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = False
LEGACY_CAN_CREATE_NEW_CANONICAL_Q = False
LEGACY_SOURCE = "LEGACY"
AUTO_ENRICH_ALLOWED = "AUTO_ENRICH_ALLOWED"
HOLD_NO_WRITE = "HOLD_NO_WRITE"
BLOCK_NO_WRITE = "BLOCK_NO_WRITE"
TRUSTED_IDENTITY_FIELDS = ("revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow", "cash", "total_debt")
SEMANTIC_RISK_FIELDS = ("ebit", "ebitda", "free_cashflow", "shares_outstanding")
LEGACY_EXPECTED_BASELINE = {
    **PHASE3C_EXPECTED_BASELINE,
    "core_ready_q": 11923,
    "core_not_ready_q": 1094,
}
DEFAULT_V2_SAFE_FILL_PLAN = Path("temp/fundamentals_v3_phase3c_v2_enrichment/20260822T_PHASE3C_V2_ENRICHMENT/safe_null_fill_plan.csv")


@dataclass(frozen=True)
class LegacyPrepared:
    baseline: dict[str, Any]
    candidates: list[V3CanonicalMigrationCandidate]
    existing_q_candidates: list[dict[str, Any]]
    identity_rows: list[dict[str, Any]]
    mapping_risk_rows: list[dict[str, Any]]
    safe_null_fill_plan: list[dict[str, Any]]
    safe_publish_fill_plan: list[dict[str, Any]]
    agreement_rows: list[dict[str, Any]]
    conflict_rows: list[dict[str, Any]]
    v2_counterfactual_rows: list[dict[str, Any]]
    v2_overlap_summary_rows: list[dict[str, Any]]
    legacy_only_rows: list[dict[str, Any]]
    legacy_only_v2_crosscheck_rows: list[dict[str, Any]]
    phase3d_summary: dict[str, Any]
    pre_snapshot: dict[str, Any]
    source_contribution: dict[str, Any]


def run_legacy_existing_q_enrichment(
    *,
    v3_db: Path,
    legacy_db: Path,
    v2_db: Path,
    artifact_root: Path,
    migration_run_id: str,
    now_utc: str,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    prepared = prepare_legacy_enrichment(v3_db=v3_db, legacy_db=legacy_db, v2_db=v2_db, migration_run_id=migration_run_id)
    _write_json(artifact_root / "current_v3_baseline.json", prepared.baseline)
    (artifact_root / "preflight.md").write_text(_preflight_text(prepared.baseline))
    _write_csv(artifact_root / "legacy_existing_q_candidates.csv", prepared.existing_q_candidates)
    _write_csv(artifact_root / "legacy_identity_classification.csv", prepared.identity_rows)
    _write_csv(artifact_root / "legacy_mapping_risk.csv", prepared.mapping_risk_rows)
    _write_csv(artifact_root / "legacy_safe_null_fill_plan.csv", prepared.safe_null_fill_plan)
    _write_csv(artifact_root / "legacy_safe_publish_fill_plan.csv", prepared.safe_publish_fill_plan)
    _write_csv(artifact_root / "legacy_non_null_agreement.csv", prepared.agreement_rows)
    _write_csv(artifact_root / "legacy_non_null_conflicts.csv", prepared.conflict_rows)
    _write_csv(artifact_root / "v2_fill_counterfactual_audit.csv", prepared.v2_counterfactual_rows)
    _write_csv(artifact_root / "v2_fill_legacy_overlap_summary.csv", prepared.v2_overlap_summary_rows)
    _write_csv(artifact_root / "legacy_only_historical_q_candidates.csv", prepared.legacy_only_rows)
    _write_csv(artifact_root / "legacy_only_history_v2_crosscheck.csv", prepared.legacy_only_v2_crosscheck_rows)
    _write_json(artifact_root / "phase3d_history_candidate_summary.json", prepared.phase3d_summary)
    _write_json(artifact_root / "legacy_source_contribution.json", prepared.source_contribution)

    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        dry_summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
            prepared.candidates,
            source=LEGACY_SOURCE,
            migration_run_id=migration_run_id,
            policy=V3SourceApplyPolicy(source=LEGACY_SOURCE),
            dry_apply=True,
            now_utc=now_utc,
        ).to_dict()
    dry_gate = validate_dry_gate(prepared, dry_summary)
    _write_json(artifact_root / "dry_apply_summary.json", {"summary": dry_summary, "gate": dry_gate})
    if not dry_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_LEGACY_DRY_GATE_FAILED:" + json.dumps(dry_gate, sort_keys=True))

    before_summary = summarize_v3(v3_db)
    before_snapshot = prepared.pre_snapshot
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        production_summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
            prepared.candidates,
            source=LEGACY_SOURCE,
            migration_run_id=migration_run_id,
            policy=V3SourceApplyPolicy(source=LEGACY_SOURCE),
            now_utc=now_utc,
        ).to_dict()
        conn.commit()
    after_summary = summarize_v3(v3_db)
    after_snapshot = snapshot_existing_non_null(v3_db)
    no_overwrite = no_overwrite_proof(before_snapshot, after_snapshot)
    idempotency = run_idempotency(v3_db=v3_db, candidates=prepared.candidates, migration_run_id=migration_run_id, now_utc=now_utc, after_summary=after_summary)
    integrity = production_integrity_for_path(v3_db)
    post_gate = validate_post_gate(before_summary, after_summary, production_summary, no_overwrite, idempotency, integrity)
    _write_csv(artifact_root / "legacy_field_contribution.csv", field_contribution_rows(production_summary))
    _write_csv(artifact_root / "legacy_metadata_contribution.csv", metadata_contribution_rows(production_summary))
    _write_csv(artifact_root / "core_readiness_pre_post.csv", [core_pre_post_row(before_summary, after_summary)])
    _write_csv(artifact_root / "missing_fields_pre_post.csv", missing_pre_post_rows(before_summary, after_summary, prepared.safe_null_fill_plan))
    _write_csv(artifact_root / "publication_coverage_pre_post.csv", [publication_pre_post_row(before_summary, after_summary, prepared.safe_publish_fill_plan)])
    (artifact_root / "no_overwrite_proof.md").write_text(json.dumps(no_overwrite, indent=2, sort_keys=True) + "\n")
    (artifact_root / "idempotency_validation.md").write_text(json.dumps(idempotency, indent=2, sort_keys=True) + "\n")
    (artifact_root / "production_v3_integrity.md").write_text(json.dumps(integrity, indent=2, sort_keys=True) + "\n")
    (artifact_root / "recommended_next_step.md").write_text("MASTER PLAN PHASE 3D - LEGACY DEEP-HISTORY EXTENSION\n")
    summary = {
        "classification": "FUNDAMENTALS_V3_PHASE3C_LEGACY_EXISTING_Q_ENRICHMENT_COMPLETE",
        "migration_run_id": migration_run_id,
        "pre_baseline": before_summary,
        "post_baseline": after_summary,
        "identity": prepared.source_contribution["identity"],
        "legacy_only_historical_q_candidates": len(prepared.legacy_only_rows),
        "safe_null_fills_planned": len(prepared.safe_null_fill_plan),
        "safe_publish_fills_planned": len(prepared.safe_publish_fill_plan),
        "production_apply": production_summary,
        "no_overwrite": no_overwrite,
        "idempotency": idempotency,
        "integrity": integrity,
        "post_gate": post_gate,
        "counterfactual": counterfactual_summary(prepared.v2_counterfactual_rows),
        "phase3d_history_candidate_summary": prepared.phase3d_summary,
        "provider_calls": 0,
        "yahoo_canonical_contribution": 0,
        "v2_canonical_contribution": 0,
        "legacy_policy": {"LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE": LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE},
        "v2_policy": {"V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE": V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE},
    }
    if not post_gate["passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_LEGACY_POST_GATE_FAILED:" + json.dumps(post_gate, sort_keys=True))
    _write_json(artifact_root / "summary.json", summary)
    return summary


def prepare_legacy_enrichment(*, v3_db: Path, legacy_db: Path, v2_db: Path, migration_run_id: str) -> LegacyPrepared:
    baseline = summarize_v3(v3_db)
    assert_legacy_baseline(baseline)
    v3_conn = connect_readonly(v3_db)
    legacy_conn = connect_readonly(legacy_db)
    v2_conn = connect_readonly(v2_db)
    v3_rows = load_v3_rows(v3_conn)
    legacy_rows = load_legacy_rows_with_publish(legacy_conn)
    v2_rows = load_v2_rows(v2_conn)
    v2_fill_rows = load_v2_filled_fields(v3_conn)
    v3_conn.close()
    legacy_conn.close()
    v2_conn.close()
    refined_tickers = {row["ticker"] for row in v3_rows}
    v3_by_period = {(row["ticker"], row["period_end_date"]): row for row in v3_rows if row.get("period_end_date")}
    v3_by_key = {(row["ticker"], row["fiscal_year"], row["fiscal_quarter"]): row for row in v3_rows}
    existing_q_candidates = []
    identity_rows = []
    mapping_risk = []
    safe_fills = []
    publish_fills = []
    agreements = []
    conflicts = []
    candidates = []
    class_counts = Counter()
    apply_counts = Counter()
    for key, legacy in sorted(legacy_rows.items()):
        v3 = v3_by_period.get(key)
        if v3 is None:
            continue
        identity = classify_legacy_identity(v3, legacy)
        existing_q_candidates.append(_candidate_row(v3, legacy))
        identity_rows.append(identity)
        class_counts[identity["identity_classification"]] += 1
        apply_counts[identity["apply_state"]] += 1
        if identity["apply_state"] == BLOCK_NO_WRITE:
            mapping_risk.append(identity)
            continue
        if identity["apply_state"] != AUTO_ENRICH_ALLOWED:
            continue
        values = {}
        for field in CANONICAL_FIELD_NAMES:
            incoming = legacy.get(field)
            current = v3.get(field)
            if incoming is None:
                continue
            if current is None:
                values[field] = incoming
                safe_fills.append(_fill_row(v3, legacy, identity, field))
            elif _strict_equivalent(current, incoming):
                agreements.append(_agreement_row(v3, legacy, identity, field, "STRICT_EQUIVALENT"))
            else:
                conflicts.append(_conflict_row(v3, legacy, identity, field))
        publish_date = None
        if legacy.get("publish_date"):
            if v3.get("publish_date") is None:
                publish_date = legacy["publish_date"]
                publish_fills.append(_publish_fill_row(v3, legacy, identity))
            elif v3.get("publish_date") == legacy.get("publish_date"):
                pass
            else:
                conflicts.append(_conflict_row(v3, legacy, identity, "publish_date"))
        if values or publish_date:
            candidates.append(
                V3CanonicalMigrationCandidate(
                    source_system=LEGACY_SOURCE,
                    source_record_id=f"LEGACY:{v3['ticker']}:{v3['period_end_date']}",
                    migration_run_id=migration_run_id,
                    market=v3["market"],
                    ticker=v3["ticker"],
                    fiscal_year=v3["fiscal_year"],
                    fiscal_quarter=v3["fiscal_quarter"],
                    period_end_date=legacy["period_end_date"],
                    publish_date=publish_date,
                    values=values,
                    candidate_can_create_quarter=False,
                    value_metadata={"phase": "PHASE3C_LEGACY_EXISTING_Q_ENRICHMENT", "identity_classification": identity["identity_classification"], "apply_state": identity["apply_state"], "no_overwrite_policy": LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE},
                )
            )
    legacy_only, legacy_v2_cross, phase3d = legacy_only_inventory(legacy_rows, v3_by_period, refined_tickers, v2_rows)
    counterfactual = v2_counterfactual(v2_fill_rows, v3_by_key, legacy_rows)
    overlap = counterfactual_overlap_rows(counterfactual)
    source = {
        "identity": {
            "legacy_rows_examined": sum(1 for (ticker, _) in legacy_rows if ticker in refined_tickers),
            "existing_q_candidates": len(existing_q_candidates),
            "same_quarter_confirmed": class_counts["SAME_QUARTER_CONFIRMED"],
            "probable_ambiguous": class_counts["PROBABLE_SAME_QUARTER"] + class_counts["AMBIGUOUS"],
            "insufficient": class_counts["INSUFFICIENT_EVIDENCE"],
            "possible_mapping_conflicts": class_counts["POSSIBLE_MAPPING_CONFLICT"],
            "clear_mapping_conflicts": class_counts["CLEAR_MAPPING_CONFLICT"],
            "period_identity_conflicts": class_counts["PERIOD_IDENTITY_CONFLICT"],
            "blocked_total": apply_counts[BLOCK_NO_WRITE],
            "identity_classes": dict(sorted(class_counts.items())),
            "apply_states": dict(sorted(apply_counts.items())),
        },
        "planned_field_fills": dict(Counter(row["field"] for row in safe_fills)),
        "planned_publish_fills": len(publish_fills),
        "non_null_agreements": dict(Counter(row["field"] for row in agreements)),
        "non_null_conflicts": dict(Counter(row["field"] for row in conflicts)),
    }
    return LegacyPrepared(
        baseline=baseline,
        candidates=candidates,
        existing_q_candidates=existing_q_candidates,
        identity_rows=identity_rows,
        mapping_risk_rows=mapping_risk,
        safe_null_fill_plan=safe_fills,
        safe_publish_fill_plan=publish_fills,
        agreement_rows=agreements,
        conflict_rows=conflicts,
        v2_counterfactual_rows=counterfactual,
        v2_overlap_summary_rows=overlap,
        legacy_only_rows=legacy_only,
        legacy_only_v2_crosscheck_rows=legacy_v2_cross,
        phase3d_summary=phase3d,
        pre_snapshot=snapshot_existing_non_null(v3_db),
        source_contribution=source,
    )


def load_legacy_rows_with_publish(conn: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    publish = {
        (str(row["ticker"]).upper(), str(row["period_end_date"])): dict(row)
        for row in conn.execute(
            """
            SELECT ticker, period_end_date, announcement_date AS publish_date,
                   matching_status, matching_confidence
            FROM rc_fundamental_quarter_earnings_match
            WHERE market='usa'
            """
        )
    }
    rows = conn.execute(
        """
        SELECT ticker, period_end_date, revenue, gross_profit, operating_income, ebit,
               ebitda, net_income, operating_cashflow, capex, free_cashflow, cash,
               total_debt, shares_outstanding, currency, run_id
        FROM rc_fundamental_quarterly q
        """
    ).fetchall()
    out = {}
    for row in rows:
        key = (str(row["ticker"]).upper(), str(row["period_end_date"]))
        item = dict(row)
        item.update(publish.get(key, {"publish_date": None, "matching_status": None, "matching_confidence": None}))
        out[key] = item
    return out


def classify_legacy_identity(v3: dict[str, Any], legacy: dict[str, Any]) -> dict[str, Any]:
    comparisons = [compare_values(field, v3.get(field), legacy.get(field), tolerance=IDENTITY_TOLERANCE) for field in (*TRUSTED_IDENTITY_FIELDS, *SEMANTIC_RISK_FIELDS)]
    trusted = [item for item in comparisons if item.field_name in TRUSTED_IDENTITY_FIELDS and item.comparable]
    trusted_matches = [item for item in trusted if item.within_5pct and not _opposite_sign(item)]
    trusted_conflicts = [item for item in trusted if item.status in {"MISMATCH", "SIGN_MISMATCH"} or _opposite_sign(item)]
    period_relation = "EXACT" if v3.get("period_end_date") == legacy.get("period_end_date") else "MATERIAL_MISMATCH"
    if period_relation == "MATERIAL_MISMATCH":
        classification = "PERIOD_IDENTITY_CONFLICT"
        apply = BLOCK_NO_WRITE
    elif trusted_conflicts and len(trusted_matches) < 2:
        classification = "CLEAR_MAPPING_CONFLICT"
        apply = BLOCK_NO_WRITE
    elif trusted_conflicts:
        classification = "POSSIBLE_MAPPING_CONFLICT"
        apply = BLOCK_NO_WRITE
    elif len(trusted_matches) >= 2:
        classification = "SAME_QUARTER_CONFIRMED"
        apply = AUTO_ENRICH_ALLOWED
    elif len(trusted_matches) == 1:
        classification = "PROBABLE_SAME_QUARTER"
        apply = HOLD_NO_WRITE
    else:
        classification = "INSUFFICIENT_EVIDENCE"
        apply = HOLD_NO_WRITE
    return {
        "ticker": v3["ticker"],
        "fiscal_year": v3["fiscal_year"],
        "fiscal_quarter": v3["fiscal_quarter"],
        "active": v3["active"],
        "v3_period_end_date": v3.get("period_end_date"),
        "legacy_period_end_date": legacy.get("period_end_date"),
        "v3_publish_date": v3.get("publish_date"),
        "legacy_publish_date": legacy.get("publish_date"),
        "period_relation": period_relation,
        "identity_classification": classification,
        "apply_state": apply,
        "trusted_comparable_fields": len(trusted),
        "trusted_matches_5pct": len(trusted_matches),
        "trusted_conflicts": len(trusted_conflicts),
        "semantic_risk_conflicts": sum(1 for item in comparisons if item.field_name in SEMANTIC_RISK_FIELDS and item.status in {"MISMATCH", "SIGN_MISMATCH"}),
    }


def _opposite_sign(item: Any) -> bool:
    return bool(item.comparable and item.v3_value is not None and item.v2_value is not None and float(item.v3_value) * float(item.v2_value) < 0)


def assert_legacy_baseline(summary: dict[str, Any]) -> None:
    observed = {
        "company_total": summary["company_total"],
        "active": summary["active"],
        "inactive": summary["inactive"],
        "canonical_q_total": summary["coverage"]["canonical_q_total"],
        "core_ready_q": summary["coverage"]["core_ready_q"],
        "core_not_ready_q": summary["coverage"]["core_not_ready_q"],
    }
    if observed != LEGACY_EXPECTED_BASELINE:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_LEGACY_BASELINE_DRIFT:" + json.dumps(observed, sort_keys=True))


def load_v2_filled_fields(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = []
    for row in conn.execute(
        """
        SELECT c.ticker, q.fiscal_year, q.fiscal_quarter, q.period_end_date, a.evidence_json
        FROM v3_migration_audit a
        JOIN v3_quarter q ON q.quarter_id=a.quarter_id
        JOIN v3_company c ON c.company_id=q.company_id
        WHERE a.source='V2'
        """
    ):
        evidence = json.loads(row["evidence_json"] or "{}")
        for field, outcomes in evidence.get("field_outcomes", {}).items():
            if "FIELD_FILLED_FROM_NULL" in outcomes:
                rows.append({"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row["period_end_date"], "field": field})
    if rows:
        return rows
    if DEFAULT_V2_SAFE_FILL_PLAN.exists():
        with DEFAULT_V2_SAFE_FILL_PLAN.open(newline="") as handle:
            return [
                {
                    "ticker": row["ticker"].strip().upper(),
                    "fiscal_year": int(row["fiscal_year"]),
                    "fiscal_quarter": row["fiscal_quarter"].strip().upper(),
                    "period_end_date": row["period_end_date"],
                    "field": row["field"],
                }
                for row in csv.DictReader(handle)
            ]
    return rows


def v2_counterfactual(v2_fills: list[dict[str, Any]], v3_by_key: dict[tuple[str, int, str], dict[str, Any]], legacy_rows: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for item in v2_fills:
        v3 = v3_by_key[(item["ticker"], item["fiscal_year"], item["fiscal_quarter"])]
        legacy = legacy_rows.get((item["ticker"], item["period_end_date"]))
        if legacy is None or legacy.get(item["field"]) is None:
            status = "LEGACY_NOT_AVAILABLE"
            legacy_value = None
        else:
            identity = classify_legacy_identity(v3, legacy)
            legacy_value = legacy.get(item["field"])
            if identity["apply_state"] != AUTO_ENRICH_ALLOWED:
                status = "LEGACY_IDENTITY_NOT_CONFIRMED"
            elif v3.get(item["field"]) == legacy_value:
                status = "LEGACY_AVAILABLE_SAME_VALUE"
            elif _strict_equivalent(v3.get(item["field"]), legacy_value):
                status = "LEGACY_AVAILABLE_ROUNDING_EQUIVALENT"
            else:
                status = "LEGACY_AVAILABLE_DIFFERENT_VALUE"
        out.append({**item, "current_v3_value": v3.get(item["field"]), "legacy_value": legacy_value, "counterfactual_classification": status})
    return out


def counterfactual_overlap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((row["field"], row["counterfactual_classification"]) for row in rows)
    fields = sorted({row["field"] for row in rows})
    classes = ["LEGACY_NOT_AVAILABLE", "LEGACY_AVAILABLE_SAME_VALUE", "LEGACY_AVAILABLE_ROUNDING_EQUIVALENT", "LEGACY_AVAILABLE_DIFFERENT_VALUE", "LEGACY_IDENTITY_NOT_CONFIRMED"]
    return [{"field": field, **{name: counts[(field, name)] for name in classes}, "total": sum(counts[(field, name)] for name in classes)} for field in fields]


def counterfactual_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(row["counterfactual_classification"] for row in rows)
    return {"prior_v2_fills_audited": len(rows), **dict(sorted(counts.items()))}


def legacy_only_inventory(legacy_rows: dict[tuple[str, str], dict[str, Any]], v3_by_period: dict[tuple[str, str], dict[str, Any]], refined_tickers: set[str], v2_rows: dict[tuple[str, int, str], dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    v2_by_period = {(row["ticker"], row["period_end_date"]): row for row in v2_rows.values()}
    out = []
    cross = []
    classes = Counter()
    v2_cross = Counter()
    for key, legacy in sorted(legacy_rows.items()):
        ticker, period_end = key
        if ticker not in refined_tickers or key in v3_by_period:
            continue
        available = [field for field in CANONICAL_FIELD_NAMES if legacy.get(field) is not None]
        try:
            pre_2018 = date.fromisoformat(period_end) < date(2018, 1, 1)
        except ValueError:
            pre_2018 = False
        classification = "PRE_2018_EXCLUDED" if pre_2018 else "READY_FOR_DEEP_HISTORY_IDENTITY_VALIDATION"
        classes[classification] += 1
        v2 = v2_by_period.get(key)
        v2_status = "V2_EXACT_PERIOD_COUNTERPART" if v2 else "NO_V2_COUNTERPART"
        v2_cross[v2_status] += 1
        row = {"ticker": ticker, "fiscal_year": "", "fiscal_quarter": "", "period_end_date": period_end, "publish_date": legacy.get("publish_date"), "available_fields": ";".join(available), "active": "", "identity_confidence": classification}
        out.append(row)
        cross.append({**row, "v2_crosscheck": v2_status})
    summary = {
        "legacy_only_historical_q_candidates": len(out),
        "ready_for_phase3d_identity_validation": classes["READY_FOR_DEEP_HISTORY_IDENTITY_VALIDATION"],
        "identity_ambiguous": classes["IDENTITY_AMBIGUOUS"],
        "duplicate_source_rows": classes["DUPLICATE_SOURCE_ROWS"],
        "pre_2018_excluded": classes["PRE_2018_EXCLUDED"],
        "other_review": classes["OTHER_REVIEW"],
        "with_v2_counterpart": v2_cross["V2_EXACT_PERIOD_COUNTERPART"],
        "without_v2_counterpart": v2_cross["NO_V2_COUNTERPART"],
    }
    return out, cross, summary


def validate_dry_gate(prepared: LegacyPrepared, dry_summary: dict[str, Any]) -> dict[str, Any]:
    fills = sum(int(counts.get("FIELD_FILLED_FROM_NULL", 0)) + int(counts.get("FIELD_DERIVED", 0)) for counts in dry_summary["field_contributions"].values())
    gate = {
        "only_confirmed_candidates_passed": all(c.value_metadata.get("identity_classification") == "SAME_QUARTER_CONFIRMED" for c in prepared.candidates),
        "no_legacy_q_created": int(dry_summary["rows"].get("canonical_quarters_created", 0)) == 0,
        "expected_field_fills": len(prepared.safe_null_fill_plan),
        "dry_field_fills": int(fills),
        "expected_publish_fills": len(prepared.safe_publish_fill_plan),
        "dry_publish_fills": int(dry_summary["metadata"].get("PUBLISH_DATE_SET", 0)),
        "candidate_rows_rejected": int(dry_summary["rows"].get("candidate_rows_rejected", 0)),
        "counterfactual_v2_fills": len(prepared.v2_counterfactual_rows),
    }
    gate["passed"] = gate["only_confirmed_candidates_passed"] and gate["no_legacy_q_created"] and gate["expected_field_fills"] == gate["dry_field_fills"] and gate["expected_publish_fills"] == gate["dry_publish_fills"] and gate["candidate_rows_rejected"] == 0 and gate["counterfactual_v2_fills"] == 319
    return gate


def validate_post_gate(before: dict[str, Any], after: dict[str, Any], production: dict[str, Any], no_overwrite: dict[str, Any], idempotency: dict[str, Any], integrity: dict[str, Any]) -> dict[str, Any]:
    gate = {
        "company_count_unchanged": before["company_total"] == after["company_total"] == 2552,
        "active_unchanged": before["active"] == after["active"] == 2484,
        "inactive_unchanged": before["inactive"] == after["inactive"] == 68,
        "q_count_unchanged": before["coverage"]["canonical_q_total"] == after["coverage"]["canonical_q_total"] == 13017,
        "no_q_created": int(production["rows"].get("canonical_quarters_created", 0)) == 0,
        "no_existing_values_overwritten": no_overwrite["existing_non_null_values_overwritten"] == 0,
        "no_existing_publish_dates_overwritten": no_overwrite["existing_publish_dates_overwritten"] == 0,
        "idempotent": idempotency["row_counts_unchanged"] and idempotency["new_null_fills"] == 0 and idempotency["new_publish_fills"] == 0,
        "quick_check_ok": integrity["quick_check"] == "ok",
        "foreign_key_check_ok": integrity["foreign_key_check_rows"] == 0,
    }
    gate["passed"] = all(gate.values())
    return gate


def run_idempotency(*, v3_db: Path, candidates: list[V3CanonicalMigrationCandidate], migration_run_id: str, now_utc: str, after_summary: dict[str, Any]) -> dict[str, Any]:
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        before_counts = _table_counts(conn)
        issue_count_before = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
        second = V3CanonicalMigrationEngine(conn).apply_source_batch(candidates, source=LEGACY_SOURCE, migration_run_id=migration_run_id, dry_apply=False, now_utc=now_utc).to_dict()
        after_counts = _table_counts(conn)
        issue_count_after = conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0]
        conn.commit()
    fills = sum(int(counts.get("FIELD_FILLED_FROM_NULL", 0)) + int(counts.get("FIELD_INSERTED", 0)) + int(counts.get("FIELD_DERIVED", 0)) for counts in second["field_contributions"].values())
    return {"row_counts_unchanged": before_counts == after_counts, "second_run_q_creations": int(second["rows"].get("canonical_quarters_created", 0)), "new_null_fills": int(fills), "new_publish_fills": int(second["metadata"].get("PUBLISH_DATE_SET", 0)), "duplicate_semantic_issues": int(issue_count_after - issue_count_before), "after_company_total": after_summary["company_total"]}


def field_contribution_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"field": field, **counts} for field, counts in summary["field_contributions"].items()]


def metadata_contribution_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"metadata_outcome": key, "count": value} for key, value in sorted(summary["metadata"].items())]


def _candidate_row(v3: dict[str, Any], legacy: dict[str, Any]) -> dict[str, Any]:
    return {"ticker": v3["ticker"], "fiscal_year": v3["fiscal_year"], "fiscal_quarter": v3["fiscal_quarter"], "v3_period_end_date": v3["period_end_date"], "legacy_period_end_date": legacy["period_end_date"]}


def _fill_row(v3: dict[str, Any], legacy: dict[str, Any], identity: dict[str, Any], field: str) -> dict[str, Any]:
    return {"ticker": v3["ticker"], "fiscal_year": v3["fiscal_year"], "fiscal_quarter": v3["fiscal_quarter"], "period_end_date": v3["period_end_date"], "field": field, "v3_value_before": v3.get(field), "legacy_value": legacy.get(field), "identity_classification": identity["identity_classification"], "apply_state": identity["apply_state"]}


def _publish_fill_row(v3: dict[str, Any], legacy: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    return {"ticker": v3["ticker"], "fiscal_year": v3["fiscal_year"], "fiscal_quarter": v3["fiscal_quarter"], "period_end_date": v3["period_end_date"], "v3_publish_date_before": v3.get("publish_date"), "legacy_publish_date": legacy.get("publish_date"), "identity_classification": identity["identity_classification"], "apply_state": identity["apply_state"]}


def _agreement_row(v3: dict[str, Any], legacy: dict[str, Any], identity: dict[str, Any], field: str, agreement_class: str) -> dict[str, Any]:
    return {"ticker": v3["ticker"], "fiscal_year": v3["fiscal_year"], "fiscal_quarter": v3["fiscal_quarter"], "period_end_date": v3["period_end_date"], "field": field, "v3_value": v3.get(field), "legacy_value": legacy.get(field), "agreement_class": agreement_class, "identity_classification": identity["identity_classification"]}


def _conflict_row(v3: dict[str, Any], legacy: dict[str, Any], identity: dict[str, Any], field: str) -> dict[str, Any]:
    current = v3.get(field if field != "publish_date" else "publish_date")
    incoming = legacy.get(field if field != "publish_date" else "publish_date")
    abs_diff = None if field == "publish_date" or current is None or incoming is None else abs(float(current) - float(incoming))
    rel_diff = None if abs_diff is None else (0.0 if max(abs(float(current)), abs(float(incoming))) == 0 else abs_diff / max(abs(float(current)), abs(float(incoming))))
    return {"ticker": v3["ticker"], "fiscal_year": v3["fiscal_year"], "fiscal_quarter": v3["fiscal_quarter"], "period_end_date": v3["period_end_date"], "field": field, "v3_value": current, "legacy_value": incoming, "absolute_difference": abs_diff, "relative_difference": rel_diff, "identity_classification": identity["identity_classification"], "disposition": "EVIDENCE_ONLY_NO_OVERWRITE"}


def _preflight_text(baseline: dict[str, Any]) -> str:
    return "# Phase 3C Legacy Existing-Q Preflight\n\n" + json.dumps(baseline, indent=2, sort_keys=True) + "\n"


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
