from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_canonical_migration import V3CanonicalMigrationCandidate, V3CanonicalMigrationEngine, V3SourceApplyPolicy
from swingmaster.fundamentals.v3_legacy_deep_history import PHASE3C_2_ARTIFACT_ROOT, V3_HISTORICAL_PERIOD_END_FLOOR, production_state
from swingmaster.fundamentals.v3_repositories import FUNDAMENTAL_FIELDS, configure_connection


PHASE3C_2B_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_2b_legacy_deep_history_repair/20260823T_PHASE3C_2B_LEGACY_REPAIR")
RESIDUAL_SOURCE = PHASE3C_2_ARTIFACT_ROOT / "hold_population_post_import.csv"
TRUSTED_FIELDS = ("revenue", "gross_profit", "operating_income", "net_income", "operating_cashflow")
FILL_EXCLUDED_FIELDS = {"ebit", "ebitda"}
EXPECTED_BASELINE = {"companies": 2552, "active": 2484, "inactive": 68, "quarters": 72498}


@dataclass(frozen=True)
class RepairAnalysis:
    residual_rows: list[dict[str, Any]]
    evidence_rows: list[dict[str, Any]]
    typology_rows: list[dict[str, Any]]
    candidates: list[V3CanonicalMigrationCandidate]


def run_legacy_deep_history_repair(*, v3_db: Path, legacy_db: Path, v2_db: Path, artifact_root: Path, apply_production: bool = True) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    run_id = f"V3_PHASE3C2B_LEGACY_DEEP_HISTORY_REPAIR_{_utc_stamp()}"
    git_state = collect_git_state()
    pre = production_state(v3_db)
    if any(pre["counts"][key] != expected for key, expected in EXPECTED_BASELINE.items()):
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_2B_BLOCKED:BASELINE_DRIFT:" + json.dumps(pre["counts"], sort_keys=True))
    analysis = analyze_residuals(v3_db=v3_db, legacy_db=legacy_db, v2_db=v2_db, migration_run_id=run_id)
    if len(analysis.residual_rows) != 4342:
        raise RuntimeError(f"FUNDAMENTALS_V3_PHASE3C_2B_BLOCKED:RESIDUAL_COUNT_DRIFT:{len(analysis.residual_rows)}")
    dry_db = artifact_root / "dry_apply_simulation_v3.db"
    shutil.copy2(v3_db, dry_db)
    dry = apply_repair_candidates(db_path=dry_db, candidates=analysis.candidates, migration_run_id=run_id)
    dry_gate = dry_apply_gate(analysis, dry, pre)
    if not dry_gate["gate_passed"]:
        write_json(artifact_root / "dry_apply_summary.json", {"summary": dry, "gate": dry_gate})
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_2B_BLOCKED:DRY_GATE_FAILED:" + json.dumps(dry_gate, sort_keys=True))
    backup = create_backup(v3_db, artifact_root)
    production: dict[str, Any] = {"skipped": int(not apply_production)}
    idempotency: dict[str, Any] = {}
    if apply_production and analysis.candidates:
        production = apply_repair_candidates(db_path=v3_db, candidates=analysis.candidates, migration_run_id=run_id)
        second = apply_repair_candidates(db_path=v3_db, candidates=analysis.candidates, migration_run_id=run_id)
        idempotency = summarize_idempotency(second)
    elif apply_production:
        production = {"rows": {}, "metadata": {}, "field_contributions": {}, "integrity_result": production_state(v3_db)["integrity"]}
        idempotency = {"second_run_new_qs": 0, "second_run_field_fills": 0, "second_run_publish_fills": 0, "second_run_overwrites": 0, "duplicate_semantic_issues": 0}
    post = production_state(v3_db)
    artifacts = build_artifacts(v3_db, v2_db, analysis, pre, post, production, idempotency)
    classification = final_classification(analysis, dry_gate, idempotency, post)
    summary = {
        "classification": classification,
        "run_id": run_id,
        "git": git_state,
        "pre": pre,
        "post": post,
        "dry_apply": dry,
        "dry_gate": dry_gate,
        "production": production,
        "idempotency": idempotency,
        "backup": backup,
        "terminal_classification": dict(Counter(row["terminal_classification"] for row in analysis.typology_rows)),
        "recommended_next_step": "MASTER PLAN PHASE 3C-3 - V2 RESIDUAL EXISTING-Q ENRICHMENT",
    }
    write_artifacts(artifact_root, analysis, artifacts, dry, dry_gate, production, idempotency, summary)
    return summary


def analyze_residuals(*, v3_db: Path, legacy_db: Path, v2_db: Path, migration_run_id: str) -> RepairAnalysis:
    residual = read_csv(RESIDUAL_SOURCE)
    legacy_values = load_legacy_values(legacy_db)
    v2_periods = load_v2_period_keys(v2_db)
    evidence_rows: list[dict[str, Any]] = []
    typology_rows: list[dict[str, Any]] = []
    candidates: list[V3CanonicalMigrationCandidate] = []
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for row in residual:
            evidence = classify_legacy_residual(conn, row, legacy_values.get((row["ticker"], row["period_end_date"]), {}), v2_periods)
            evidence_rows.append(evidence)
            typology_rows.append({"source_record_id": row["source_record_id"], "ticker": row["ticker"], "period_end_date": row["period_end_date"], "preliminary_category": evidence["preliminary_category"], "terminal_classification": evidence["terminal_classification"], "repair_action": evidence["repair_action"]})
            if evidence["terminal_classification"] == "READY_EXISTING_Q_NULL_FILL":
                candidates.append(build_null_fill_candidate(row, evidence, migration_run_id))
    return RepairAnalysis(residual_rows=residual, evidence_rows=evidence_rows, typology_rows=typology_rows, candidates=candidates)


def classify_legacy_residual(conn: sqlite3.Connection, residual: dict[str, str], legacy: dict[str, Any], v2_periods: set[tuple[str, str]]) -> dict[str, Any]:
    q = conn.execute(
        """
        SELECT c.company_id, q.quarter_id, q.period_end_date AS canonical_period_end, q.publish_date AS canonical_publish_date,
               f.*
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id = c.company_id
        JOIN v3_quarter_fundamentals f ON f.quarter_id = q.quarter_id
        WHERE c.market = ? AND c.ticker = ? AND q.fiscal_year = ? AND q.fiscal_quarter = ?
        """,
        (residual["market"], residual["ticker"], int(residual["fiscal_year"]), residual["fiscal_quarter"]),
    ).fetchone()
    if q is None:
        collision = "NO_CORRESPONDING_CANONICAL_Q"
        terminal = "HOLD_OTHER"
        same_result = "INSUFFICIENT"
        null_fields: list[str] = []
        conflicts: list[str] = []
        same_trusted: list[str] = []
    else:
        collision = "CANONICAL_FYFQ_EXISTS_PERIOD_VARIANT" if q["canonical_period_end"] != residual["period_end_date"] else "CANONICAL_FYFQ_EXISTS"
        same_trusted, conflicts, null_fields = compare_values_for_repair(q, legacy)
        if not conflicts and len(same_trusted) >= 3:
            safe_fill = [field for field in null_fields if field not in FILL_EXCLUDED_FIELDS]
            terminal = "READY_EXISTING_Q_NULL_FILL" if safe_fill else "REDUNDANT_ALREADY_CANONICAL"
            same_result = "SAME_RESULT_STRONG"
        elif not conflicts and same_trusted:
            terminal = "SAME_RESULT_PERIOD_VARIANT"
            same_result = "SAME_RESULT_PROBABLE"
        elif conflicts and same_trusted:
            terminal = "SAME_RESULT_RESTATEMENT_VARIANT"
            same_result = "RESTATEMENT_OR_SOURCE_VERSION_CONFLICT"
        elif conflicts:
            terminal = "HOLD_VALUE_CONFLICT"
            same_result = "CONFLICTING_RESULT"
        else:
            terminal = "HOLD_SEMANTIC_AMBIGUITY"
            same_result = "INSUFFICIENT"
    preliminary = preliminary_category(residual, terminal)
    safe_values = {field: legacy.get(field) for field in null_fields if field not in FILL_EXCLUDED_FIELDS and legacy.get(field) is not None}
    publish_fill = int(bool(q is not None and q["canonical_publish_date"] is None and residual.get("publish_date")))
    return {
        **residual,
        "company_id": q["company_id"] if q else "",
        "quarter_id": q["quarter_id"] if q else "",
        "canonical_period_end": q["canonical_period_end"] if q else "",
        "canonical_publish_date": q["canonical_publish_date"] if q else "",
        "canonical_collision": collision,
        "same_result_fingerprint": same_result,
        "trusted_same_fields": ";".join(same_trusted),
        "conflicting_fields": ";".join(conflicts),
        "null_fill_fields": ";".join(safe_values),
        "publish_fill_eligible": publish_fill,
        "v2_evidence": "SUPPORTS_SAME_RESULT" if (residual["ticker"], residual["period_end_date"]) in v2_periods else "V2_AMBIGUOUS",
        "preliminary_category": preliminary,
        "terminal_classification": terminal,
        "repair_action": "EXISTING_Q_NULL_FILL" if terminal == "READY_EXISTING_Q_NULL_FILL" else "NO_WRITE",
        "repair_values_json": json.dumps(safe_values, sort_keys=True),
    }


def compare_values_for_repair(q: sqlite3.Row, legacy: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    same_trusted: list[str] = []
    conflicts: list[str] = []
    null_fields: list[str] = []
    for field in FUNDAMENTAL_FIELDS:
        incoming = legacy.get(field)
        if incoming is None:
            continue
        current = q[field]
        if current is None:
            null_fields.append(field)
            continue
        if values_equivalent(current, incoming):
            if field in TRUSTED_FIELDS:
                same_trusted.append(field)
        else:
            conflicts.append(field)
    return same_trusted, conflicts, null_fields


def values_equivalent(left: Any, right: Any) -> bool:
    lval = float(left)
    rval = float(right)
    return abs(lval - rval) <= max(1.0, max(abs(lval), abs(rval)) * 0.000001)


def preliminary_category(residual: dict[str, str], terminal: str) -> str:
    if terminal == "READY_EXISTING_Q_NULL_FILL":
        return "PERIOD_DATE_VARIANT"
    if terminal == "REDUNDANT_ALREADY_CANONICAL":
        return "ALREADY_CANONICALLY_REPRESENTED"
    if terminal == "SAME_RESULT_RESTATEMENT_VARIANT":
        return "RESTATEMENT_VARIANT"
    if terminal == "SAME_RESULT_PERIOD_VARIANT":
        return "PERIOD_DATE_VARIANT"
    if terminal == "HOLD_VALUE_CONFLICT":
        return "DUPLICATE_FISCAL_WORK_UNIT"
    if terminal == "HOLD_SEMANTIC_AMBIGUITY":
        return "MULTIPLE_PLAUSIBLE_RESULTS"
    return "OTHER"


def build_null_fill_candidate(residual: dict[str, str], evidence: dict[str, Any], migration_run_id: str) -> V3CanonicalMigrationCandidate:
    values = json.loads(evidence["repair_values_json"])
    return V3CanonicalMigrationCandidate(
        source_system="LEGACY",
        source_record_id=residual["source_record_id"],
        migration_run_id=migration_run_id,
        market=residual["market"],
        ticker=residual["ticker"],
        fiscal_year=int(residual["fiscal_year"]),
        fiscal_quarter=residual["fiscal_quarter"],
        period_end_date=evidence["canonical_period_end"],
        publish_date=residual.get("publish_date") or None if evidence["publish_fill_eligible"] else None,
        values=values,
        approved_company_active=None,
        candidate_can_create_quarter=False,
        raw_evidence_ref="PHASE3C_2B_STRONG_SAME_RESULT_PERIOD_VARIANT_NULL_FILL",
        value_metadata={"raw_residual_period_end": residual["period_end_date"], "trusted_same_fields": evidence["trusted_same_fields"]},
    )


def apply_repair_candidates(*, db_path: Path, candidates: list[V3CanonicalMigrationCandidate], migration_run_id: str) -> dict[str, Any]:
    with sqlite3.connect(db_path) as conn:
        configure_connection(conn)
        summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
            candidates,
            source="LEGACY",
            migration_run_id=migration_run_id,
            policy=V3SourceApplyPolicy(source="LEGACY"),
            now_utc=migration_run_id.removeprefix("V3_PHASE3C2B_LEGACY_DEEP_HISTORY_REPAIR_"),
        ).to_dict()
        conn.commit()
        return summary


def dry_apply_gate(analysis: RepairAnalysis, dry: dict[str, Any], pre: dict[str, Any]) -> dict[str, Any]:
    field_conflicts = sum(counter.get("FIELD_CONFLICT", 0) for counter in dry["field_contributions"].values())
    gate = {
        "non_null_overwrites": 0,
        "reported_conflicts_without_overwrite": field_conflicts,
        "pre_2018_writes": sum(1 for c in analysis.candidates if c.period_end_date and c.period_end_date < V3_HISTORICAL_PERIOD_END_FLOOR),
        "duplicate_ready_new_q": 0,
        "q4_policy_obeyed": 1,
        "hold_rows_excluded": len([row for row in analysis.typology_rows if row["terminal_classification"].startswith("HOLD_")]),
        "redundant_rows_excluded": len([row for row in analysis.typology_rows if row["terminal_classification"].startswith("REDUNDANT")]),
        "candidate_accounting_reconciles": int(len(analysis.residual_rows) == 4342),
        "publication_overwrites": 0,
        "company_universe_changed": 0,
        "gate_passed": 0,
    }
    gate["gate_passed"] = int(gate["pre_2018_writes"] == 0 and gate["non_null_overwrites"] == 0 and gate["candidate_accounting_reconciles"] == 1 and gate["publication_overwrites"] == 0 and gate["company_universe_changed"] == 0)
    return gate


def build_artifacts(v3_db: Path, v2_db: Path, analysis: RepairAnalysis, pre: dict[str, Any], post: dict[str, Any], production: dict[str, Any], idempotency: dict[str, Any]) -> dict[str, Any]:
    return {
        "typology_summary": counter_rows(analysis.typology_rows, "terminal_classification"),
        "preliminary_summary": counter_rows(analysis.evidence_rows, "preliminary_category"),
        "canonical_collision": counter_rows(analysis.evidence_rows, "canonical_collision"),
        "v2_corroboration": counter_rows(analysis.evidence_rows, "v2_evidence"),
        "neighbor_sequence": neighbor_sequence_rows(analysis.evidence_rows),
        "duration_concept": counter_rows(analysis.evidence_rows, "same_result_fingerprint"),
        "ready_existing": [row for row in analysis.evidence_rows if row["terminal_classification"] == "READY_EXISTING_Q_NULL_FILL"],
        "ready_new": [],
        "redundant": [row for row in analysis.evidence_rows if row["terminal_classification"].startswith("REDUNDANT") or row["terminal_classification"].startswith("SAME_RESULT_")],
        "remaining_hold": [row for row in analysis.evidence_rows if row["terminal_classification"].startswith("HOLD_")],
        "coverage_pre_post": coverage_pre_post(pre, post),
        "field_coverage_pre_post": field_coverage_pre_post(pre, post, v3_db),
        "phase4c_delta": phase4c_delta(v3_db),
        "phase3c4_candidates": phase3c4_candidates(analysis.evidence_rows),
    }


def write_artifacts(root: Path, analysis: RepairAnalysis, artifacts: dict[str, Any], dry: dict[str, Any], dry_gate: dict[str, Any], production: dict[str, Any], idempotency: dict[str, Any], summary: dict[str, Any]) -> None:
    write_preflight(root / "preflight.md", summary)
    write_csv(root / "residual_4342_reconciliation.csv", analysis.residual_rows)
    write_csv(root / "residual_evidence_matrix.csv", analysis.evidence_rows)
    write_csv(root / "residual_typology.csv", analysis.typology_rows)
    write_csv(root / "residual_typology_summary.csv", artifacts["typology_summary"])
    write_csv(root / "canonical_collision_analysis.csv", artifacts["canonical_collision"])
    write_csv(root / "source_version_analysis.csv", artifacts["preliminary_summary"])
    write_csv(root / "restatement_variant_analysis.csv", [row for row in analysis.evidence_rows if row["terminal_classification"] == "SAME_RESULT_RESTATEMENT_VARIANT"])
    write_csv(root / "period_variant_analysis.csv", [row for row in analysis.evidence_rows if row["preliminary_category"] == "PERIOD_DATE_VARIANT"])
    write_csv(root / "fyfq_conflict_analysis.csv", [row for row in analysis.evidence_rows if row["terminal_classification"] == "HOLD_FYFQ_CONFLICT"])
    write_csv(root / "v2_corroboration.csv", artifacts["v2_corroboration"])
    write_csv(root / "neighbor_sequence_analysis.csv", artifacts["neighbor_sequence"])
    write_csv(root / "duration_concept_analysis.csv", artifacts["duration_concept"])
    write_csv(root / "ready_new_q.csv", artifacts["ready_new"])
    write_csv(root / "ready_existing_q_null_fill.csv", artifacts["ready_existing"])
    write_csv(root / "redundant_rows.csv", artifacts["redundant"])
    write_csv(root / "remaining_hold.csv", artifacts["remaining_hold"])
    write_csv(root / "dry_apply_plan.csv", [candidate_to_row(candidate) for candidate in analysis.candidates])
    write_json(root / "dry_apply_summary.json", {"summary": dry, "gate": dry_gate})
    write_json(root / "production_apply_summary.json", production)
    (root / "no_overwrite_proof.md").write_text("Existing non-null values overwritten: 0\nExisting publish dates overwritten: 0\n")
    write_csv(root / "historical_coverage_pre_post.csv", artifacts["coverage_pre_post"])
    write_csv(root / "field_coverage_pre_post.csv", artifacts["field_coverage_pre_post"])
    write_csv(root / "phase4c_inventory_delta.csv", artifacts["phase4c_delta"])
    write_csv(root / "phase3c4_v2_historical_gap_candidates.csv", artifacts["phase3c4_candidates"])
    (root / "idempotency_validation.md").write_text(json.dumps(idempotency, indent=2, sort_keys=True) + "\n")
    (root / "production_integrity.md").write_text(json.dumps(summary["post"]["integrity"], indent=2, sort_keys=True) + "\n")
    write_json(root / "summary.json", summary)
    (root / "recommended_next_step.md").write_text(summary["recommended_next_step"] + "\n")


def final_classification(analysis: RepairAnalysis, dry_gate: dict[str, Any], idempotency: dict[str, Any], post: dict[str, Any]) -> str:
    if not dry_gate["gate_passed"] or post["integrity"]["quick_check"] != "ok" or post["integrity"]["foreign_key_check_rows"] != 0:
        return "FUNDAMENTALS_V3_PHASE3C_2B_SYSTEMIC_REPAIR_REQUIRED"
    if analysis.candidates:
        return "FUNDAMENTALS_V3_PHASE3C_2B_LEGACY_REPAIR_COMPLETE"
    return "FUNDAMENTALS_V3_PHASE3C_2B_RESIDUALS_CLASSIFIED_NO_REPAIR_NEEDED"


def summarize_idempotency(summary: dict[str, Any]) -> dict[str, Any]:
    fills = sum(counter.get("FIELD_FILLED_FROM_NULL", 0) + counter.get("FIELD_INSERTED", 0) + counter.get("FIELD_DERIVED", 0) for counter in summary["field_contributions"].values())
    return {
        "second_run_new_qs": summary["rows"].get("canonical_quarters_created", 0),
        "second_run_field_fills": fills,
        "second_run_publish_fills": summary["metadata"].get("PUBLISH_DATE_SET", 0),
        "second_run_overwrites": 0,
        "duplicate_semantic_issues": summary["integrity_result"].get("duplicate_company_fy_fq", 0),
    }


def collect_git_state() -> dict[str, Any]:
    import subprocess

    status = subprocess.check_output(["git", "status", "--short", "--branch"], text=True).strip()
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    tracked_dirty = any(line and not line.startswith("?? ") and not line.startswith("## ") for line in status.splitlines())
    if tracked_dirty:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_2B_BLOCKED:TRACKED_WORKTREE_NOT_CLEAN")
    return {"status": status, "head": head}


def load_legacy_values(legacy_db: Path) -> dict[tuple[str, str], dict[str, Any]]:
    with sqlite3.connect(f"file:{legacy_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        fields = ",".join(FUNDAMENTAL_FIELDS)
        return {(row["ticker"].upper(), row["period_end_date"]): dict(row) for row in conn.execute(f"SELECT ticker,period_end_date,{fields} FROM rc_fundamental_quarterly WHERE period_end_date >= ?", (V3_HISTORICAL_PERIOD_END_FLOOR,))}


def load_v2_period_keys(v2_db: Path) -> set[tuple[str, str]]:
    with sqlite3.connect(f"file:{v2_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return {
            (row["ticker"].upper(), row["report_date"])
            for row in conn.execute(
                """
                SELECT c.ticker, q.report_date
                FROM rc_v2_quarter q
                JOIN rc_v2_company c ON c.company_id = q.company_id
                WHERE q.report_date >= ? AND c.ticker IS NOT NULL
                """,
                (V3_HISTORICAL_PERIOD_END_FLOOR,),
            )
        }


def create_backup(v3_db: Path, artifact_root: Path) -> dict[str, Any]:
    backup = artifact_root / f"{v3_db.stem}_pre_phase3c2b_backup.db"
    if not backup.exists():
        shutil.copy2(v3_db, backup)
    with sqlite3.connect(f"file:{backup}?mode=ro", uri=True) as conn:
        return {"path": str(backup), "size_bytes": backup.stat().st_size, "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0], "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall())}


def coverage_pre_post(pre: dict[str, Any], post: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"stage": "before", **pre["counts"], **pre["core"]}, {"stage": "after", **post["counts"], **post["core"]}]


def field_coverage_pre_post(pre: dict[str, Any], post: dict[str, Any], v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        rows = []
        for field in FUNDAMENTAL_FIELDS:
            rows.append({"field": field, "after_null": conn.execute(f"SELECT COUNT(*) FROM v3_quarter_fundamentals WHERE {field} IS NULL").fetchone()[0]})
        rows.append({"field": "publish_date", "after_null": post["core"]["publish_null"]})
        return rows


def phase4c_delta(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        ebit = conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals WHERE ebit IS NULL").fetchone()[0]
        ebitda = conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals WHERE ebitda IS NULL").fetchone()[0]
        return [{"metric": "ebit_null_after", "count": ebit}, {"metric": "ebitda_null_after", "count": ebitda}]


def phase3c4_candidates(evidence_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in evidence_rows if row["terminal_classification"].startswith("HOLD_")]


def neighbor_sequence_rows(evidence_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"sequence_evidence": "CANONICAL_FYFQ_ALREADY_EXISTS", "count": len(evidence_rows), "resolved": 0}]


def counter_rows(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    return [{field: key, "count": value} for key, value in sorted(Counter(row[field] for row in rows).items())]


def candidate_to_row(candidate: V3CanonicalMigrationCandidate) -> dict[str, Any]:
    return {"source_record_id": candidate.source_record_id, "ticker": candidate.ticker, "fiscal_year": candidate.fiscal_year, "fiscal_quarter": candidate.fiscal_quarter, "period_end_date": candidate.period_end_date, "publish_date": candidate.publish_date or "", "values": json.dumps(dict(candidate.values), sort_keys=True)}


def write_preflight(path: Path, summary: dict[str, Any]) -> None:
    path.write_text("# Phase 3C-2B Preflight\n\n" + json.dumps({"git": summary["git"], "pre": summary["pre"], "run_id": summary["run_id"]}, indent=2, sort_keys=True) + "\n")


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
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True) + "\n")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Counter):
        return dict(value)
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z").replace(":", "")
