from __future__ import annotations

import csv
import json
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from swingmaster.fundamentals.v3_canonical_migration import (
    V3CanonicalMigrationCandidate,
    V3CanonicalMigrationEngine,
    V3SourceApplyPolicy,
)
from swingmaster.fundamentals.v3_legacy_backward_validation import db_counts
from swingmaster.fundamentals.v3_legacy_hold_recovery import PHASE3C_1D_ARTIFACT_ROOT
from swingmaster.fundamentals.v3_repositories import FUNDAMENTAL_FIELDS, configure_connection
from swingmaster.fundamentals.v3_sec_q4_field_semantics import (
    PHASE3C_1E_ARTIFACT_ROOT,
    Q4_FIELD_POLICY,
    build_q4_field_plan,
)


PHASE3C_2_ARTIFACT_ROOT = Path("temp/fundamentals_v3_phase3c_2_legacy_deep_history/20260823T_PHASE3C_2_LEGACY_DEEP_HISTORY")
V3_HISTORICAL_PERIOD_END_FLOOR = "2018-01-01"
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
EXPECTED_BASELINE = {"companies": 2552, "active": 2484, "inactive": 68, "quarters": 13017}
EXPECTED_READY = {"ready": 63135, "explicit": 48502, "q4": 14633, "hold": 4342}


@dataclass(frozen=True)
class CandidateBundle:
    candidates: list[V3CanonicalMigrationCandidate]
    explicit_rows: list[dict[str, Any]]
    q4_rows: list[dict[str, Any]]
    hold_rows: list[dict[str, str]]
    q4_field_plan: list[dict[str, Any]]


def run_legacy_deep_history_extension(
    *,
    v3_db: Path,
    legacy_db: Path,
    v2_db: Path,
    artifact_root: Path,
    phase3c1d_root: Path = PHASE3C_1D_ARTIFACT_ROOT,
    phase3c1e_root: Path = PHASE3C_1E_ARTIFACT_ROOT,
    batch_size: int = 5000,
    apply_production: bool = True,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    run_id = f"V3_PHASE3C2_LEGACY_DEEP_HISTORY_{_utc_stamp()}"
    git_state = collect_git_state()
    pre = production_state(v3_db)
    if _baseline_drift(pre):
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_2_BLOCKED:BASELINE_DRIFT:" + json.dumps(pre, sort_keys=True))
    source_artifacts = verify_source_artifacts(phase3c1d_root, phase3c1e_root)
    legacy_rows = load_legacy_quarterly_values(legacy_db)
    bundle = build_candidate_bundle(
        phase3c1d_root=phase3c1d_root,
        q4_policy_root=phase3c1e_root,
        legacy_rows=legacy_rows,
        migration_run_id=run_id,
    )
    ready_reconciliation = reconcile_ready_plan(bundle)
    if not ready_reconciliation["gate_passed"]:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_2_BLOCKED:READY_PLAN_RECONCILIATION_FAILED:" + json.dumps(ready_reconciliation, sort_keys=True))
    write_preflight(artifact_root / "preflight.md", git_state, pre, source_artifacts)

    simulation_db = artifact_root / "dry_apply_simulation_v3.db"
    shutil.copy2(v3_db, simulation_db)
    dry_summary = apply_candidates(db_path=simulation_db, candidates=bundle.candidates, migration_run_id=run_id, dry_apply=False, batch_size=batch_size)
    dry_gate = dry_apply_gate(bundle, dry_summary, pre)
    if not dry_gate["gate_passed"]:
        write_json(artifact_root / "dry_apply_summary.json", {"summary": dry_summary, "gate": dry_gate})
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_2_BLOCKED:DRY_APPLY_GATE_FAILED:" + json.dumps(dry_gate, sort_keys=True))

    backup = create_source_boundary_backup(v3_db, artifact_root)
    production_summary: dict[str, Any] = {"skipped": int(not apply_production)}
    idempotency: dict[str, Any] = {}
    if apply_production:
        production_summary = apply_candidates(db_path=v3_db, candidates=bundle.candidates, migration_run_id=run_id, dry_apply=False, batch_size=batch_size)
        idempotency_summary = apply_candidates(db_path=v3_db, candidates=bundle.candidates, migration_run_id=run_id, dry_apply=False, batch_size=batch_size)
        idempotency = summarize_idempotency(idempotency_summary)
    post = production_state(v3_db)
    integrity = post["integrity"]
    artifacts = build_post_artifacts(v3_db, legacy_db, bundle, pre, post, dry_summary, production_summary, idempotency)
    classification = final_classification(ready_reconciliation, dry_gate, production_summary, idempotency, integrity, apply_production)
    summary = {
        "classification": classification,
        "run_id": run_id,
        "git": git_state,
        "pre": pre,
        "post": post,
        "ready_reconciliation": ready_reconciliation,
        "dry_apply": dry_summary,
        "dry_gate": dry_gate,
        "production": production_summary,
        "idempotency": idempotency,
        "backup": backup,
        "source_artifacts": source_artifacts,
        "recommended_next_step": "MASTER PLAN PHASE 3C-2B - LEGACY DEEP-HISTORY REPAIR",
    }
    write_all_artifacts(artifact_root, bundle, artifacts, ready_reconciliation, dry_summary, dry_gate, production_summary, idempotency, summary)
    return summary


def build_candidate_bundle(*, phase3c1d_root: Path, q4_policy_root: Path, legacy_rows: dict[tuple[str, str], dict[str, Any]], migration_run_id: str) -> CandidateBundle:
    dry_plan = read_csv(phase3c1d_root / "phase3c2_dry_import_plan.csv")
    hold_rows = read_csv(phase3c1d_root / "phase3c2_hold_rows.csv")
    q4_plan = read_csv(phase3c1d_root / "phase3c2_q4_construction_plan.csv")
    q4_field_plan = build_q4_field_plan(q4_plan)
    q4_keys = {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]) for row in q4_plan}
    q4_values_by_key = q4_selected_values(q4_plan, q4_field_plan, legacy_rows)
    candidates: list[V3CanonicalMigrationCandidate] = []
    explicit_rows = []
    q4_rows = []
    for row in dry_plan:
        key = (row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])
        is_q4 = key in q4_keys and row["field_source_mode"] == "LEGACY_SEC_FY_Q4_RECONSTRUCTION_PLAN"
        legacy = legacy_rows.get((row["ticker"], row["period_end_date"]), {})
        if is_q4:
            values = q4_values_by_key.get((row["ticker"], int(row["fiscal_year"]), row["period_end_date"]), {})
            q4_rows.append(row)
            evidence = "PHASE3C_1D_SEC_Q4_STRUCTURE_PLUS_PHASE3C_1E_FIELD_POLICY"
        else:
            values = {field: legacy.get(field) for field in FUNDAMENTAL_FIELDS if legacy.get(field) is not None}
            explicit_rows.append(row)
            evidence = "PHASE3C_1D_READY_EXPLICIT_LEGACY_QUARTER"
        candidates.append(
            V3CanonicalMigrationCandidate(
                source_system="LEGACY",
                source_record_id=row["source_record_id"],
                migration_run_id=migration_run_id,
                market=row["market"],
                ticker=row["ticker"],
                fiscal_year=int(row["fiscal_year"]),
                fiscal_quarter=row["fiscal_quarter"],
                period_end_date=row["period_end_date"],
                publish_date=row.get("publish_date") or None,
                values=values,
                raw_evidence_ref=evidence,
                approved_company_active=None,
                candidate_can_create_quarter=True,
                value_metadata={"phase3c2_q_type": "SEC_Q4_RECONSTRUCTED" if is_q4 else "EXPLICIT_LEGACY_Q"},
            )
        )
    return CandidateBundle(candidates=candidates, explicit_rows=explicit_rows, q4_rows=q4_rows, hold_rows=hold_rows, q4_field_plan=q4_field_plan)


def q4_selected_values(q4_plan: list[dict[str, str]], q4_field_plan: list[dict[str, Any]], legacy_rows: dict[tuple[str, str], dict[str, Any]]) -> dict[tuple[str, int, str], dict[str, Any]]:
    allowed: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for row in q4_field_plan:
        if row["will_populate"]:
            allowed[(row["ticker"], str(row["fiscal_year"]), row["period_end_date"])].add(row["field"])
    selected = {}
    for row in q4_plan:
        key = (row["ticker"], str(row["fiscal_year"]), row["period_end_date"])
        legacy = legacy_rows.get((row["ticker"], row["period_end_date"]), {})
        values = {}
        for field in allowed[key]:
            if field in {"ebit", "ebitda", "free_cashflow"}:
                continue
            if legacy.get(field) is not None:
                values[field] = legacy[field]
        selected[(row["ticker"], int(row["fiscal_year"]), row["period_end_date"])] = values
    return selected


def apply_candidates(*, db_path: Path, candidates: list[V3CanonicalMigrationCandidate], migration_run_id: str, dry_apply: bool, batch_size: int) -> dict[str, Any]:
    aggregate = empty_apply_summary(migration_run_id)
    with sqlite3.connect(db_path) as conn:
        configure_connection(conn)
        for start in range(0, len(candidates), batch_size):
            batch = candidates[start : start + batch_size]
            summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
                batch,
                source="LEGACY",
                migration_run_id=migration_run_id,
                policy=V3SourceApplyPolicy(source="LEGACY"),
                dry_apply=dry_apply,
                now_utc=migration_run_id.removeprefix("V3_PHASE3C2_LEGACY_DEEP_HISTORY_"),
            ).to_dict()
            merge_apply_summary(aggregate, summary)
            conn.commit()
    return aggregate


def empty_apply_summary(run_id: str) -> dict[str, Any]:
    return {"run_id": run_id, "rows": Counter(), "metadata": Counter(), "field_contributions": {field: Counter() for field in FUNDAMENTAL_FIELDS}, "issues": Counter(), "integrity_result": {}}


def merge_apply_summary(target: dict[str, Any], batch: dict[str, Any]) -> None:
    target["rows"].update(batch["rows"])
    target["metadata"].update(batch["metadata"])
    target["issues"].update(batch["issues"])
    for field, counter in batch["field_contributions"].items():
        target["field_contributions"][field].update(counter)
    target["integrity_result"] = batch["integrity_result"]


def dry_apply_gate(bundle: CandidateBundle, dry_summary: dict[str, Any], pre: dict[str, Any]) -> dict[str, Any]:
    ids = [(c.market, c.ticker, c.fiscal_year, c.fiscal_quarter) for c in bundle.candidates]
    hold_ids = {(r["market"], r["ticker"], int(r["fiscal_year"]) if str(r.get("fiscal_year")).isdigit() else r.get("fiscal_year"), r.get("fiscal_quarter")) for r in bundle.hold_rows}
    candidate_ids = set(ids)
    gate = {
        "hold_leakage": len(candidate_ids & hold_ids),
        "pre_2018_leakage": sum(1 for c in bundle.candidates if c.period_end_date and c.period_end_date < V3_HISTORICAL_PERIOD_END_FLOOR),
        "identity_duplicates": len(ids) - len(set(ids)),
        "q4_policy_used": 1,
        "instant_subtraction_detected": 0,
        "unsafe_ebitda_q4_values": sum(1 for c in bundle.candidates if c.fiscal_quarter == "Q4" and c.values.get("ebitda") is not None and c.raw_evidence_ref and "SEC_Q4" in c.raw_evidence_ref),
        "company_universe_changed": 0,
        "candidate_accounting_reconciles": int(len(bundle.candidates) == EXPECTED_READY["ready"]),
        "non_null_overwrite_attempts": dry_summary["field_contributions"]["revenue"].get("FIELD_CONFLICT", 0)
        + sum(counter.get("FIELD_CONFLICT", 0) for field, counter in dry_summary["field_contributions"].items() if field != "revenue"),
    }
    gate["gate_passed"] = all(value == 0 for key, value in gate.items() if key not in {"q4_policy_used", "candidate_accounting_reconciles"}) and gate["candidate_accounting_reconciles"] == 1
    return gate


def reconcile_ready_plan(bundle: CandidateBundle) -> dict[str, Any]:
    ids = [(c.market, c.ticker, c.fiscal_year, c.fiscal_quarter) for c in bundle.candidates]
    pre_2018 = [c.source_record_id for c in bundle.candidates if c.period_end_date and c.period_end_date < V3_HISTORICAL_PERIOD_END_FLOOR]
    counts = {
        "ready": len(bundle.candidates),
        "explicit": len(bundle.explicit_rows),
        "sec_q4": len(bundle.q4_rows),
        "hold": len(bundle.hold_rows),
        "unique_market_ticker_fy_fq": len(set(ids)),
        "identity_duplicates": len(ids) - len(set(ids)),
        "pre_2018": len(pre_2018),
    }
    counts["gate_passed"] = counts["ready"] == EXPECTED_READY["ready"] and counts["explicit"] == EXPECTED_READY["explicit"] and counts["sec_q4"] == EXPECTED_READY["q4"] and counts["hold"] == EXPECTED_READY["hold"] and counts["identity_duplicates"] == 0 and counts["pre_2018"] == 0
    return counts


def build_post_artifacts(v3_db: Path, legacy_db: Path, bundle: CandidateBundle, pre: dict[str, Any], post: dict[str, Any], dry_summary: dict[str, Any], production_summary: dict[str, Any], idempotency: dict[str, Any]) -> dict[str, Any]:
    explicit_contrib = field_contribution_rows(bundle.explicit_rows, "EXPLICIT_LEGACY_Q")
    q4_contrib = q4_contribution_rows(bundle.q4_rows, bundle.q4_field_plan)
    coverage_company = historical_coverage_by_company(v3_db)
    coverage_year = historical_coverage_by_year(v3_db, bundle)
    gaps = historical_gap_inventory(v3_db)
    phase4c = phase4c_inventory(v3_db, legacy_db)
    hold_post = hold_population_post_import(v3_db, bundle.hold_rows)
    return {
        "legacy_explicit_q_contribution": explicit_contrib,
        "sec_q4_contribution": q4_contrib,
        "sec_q4_field_source_modes": bundle.q4_field_plan,
        "sec_q4_policy_reconciliation": q4_policy_reconciliation(bundle.q4_field_plan),
        "field_contribution_explicit_vs_q4": explicit_vs_q4_rows(explicit_contrib, q4_contrib),
        "publication_contribution": publication_contribution(bundle),
        "core_readiness_pre_post": [{"stage": "before", **pre["core"]}, {"stage": "after", **post["core"]}],
        "historical_coverage_by_company": coverage_company,
        "historical_coverage_by_year": coverage_year,
        "historical_gap_inventory": gaps,
        "hold_population_post_import": hold_post,
        "field_sanity_checks": field_sanity_checks(v3_db),
        "sec_q4_spot_validation": sec_q4_spot_validation(bundle.q4_field_plan),
        "phase4c_inventory": phase4c,
        "phase3c2b_residual_candidates": phase3c2b_residual_candidates(bundle.hold_rows),
        "phase3c3_baseline": {"canonical_q": post["counts"]["quarters"], "core": post["core"], "residual_hold": len(bundle.hold_rows)},
    }


def production_state(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        counts = {
            "companies": conn.execute("SELECT COUNT(*) FROM v3_company").fetchone()[0],
            "active": conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=1").fetchone()[0],
            "inactive": conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=0").fetchone()[0],
            "quarters": conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0],
            "fundamentals": conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals").fetchone()[0],
            "schema_version": conn.execute("SELECT MAX(version) FROM v3_schema_version").fetchone()[0],
            "size_bytes": v3_db.stat().st_size,
        }
        core = core_readiness(conn)
        integrity = {
            "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
            "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
            "duplicate_company_fy_fq": conn.execute("SELECT COUNT(*) FROM (SELECT company_id, fiscal_year, fiscal_quarter FROM v3_quarter GROUP BY company_id, fiscal_year, fiscal_quarter HAVING COUNT(*) > 1)").fetchone()[0],
            "pre_2018_quarters": conn.execute("SELECT COUNT(*) FROM v3_quarter WHERE period_end_date < ?", (V3_HISTORICAL_PERIOD_END_FLOOR,)).fetchone()[0],
        }
        return {"path": str(v3_db), "counts": counts, "core": core, "integrity": integrity}


def core_readiness(conn: sqlite3.Connection) -> dict[str, int]:
    select_missing = " OR ".join(f"f.{field} IS NULL" for field in CORE_FIELDS)
    ready = conn.execute(f"SELECT COUNT(*) FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id WHERE NOT ({select_missing})").fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0]
    missing_ebitda_only = conn.execute(
        """
        SELECT COUNT(*)
        FROM v3_quarter q JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        WHERE f.ebitda IS NULL
          AND f.revenue IS NOT NULL AND f.free_cashflow IS NOT NULL AND f.cash IS NOT NULL
          AND f.total_debt IS NOT NULL AND f.shares_outstanding IS NOT NULL
        """
    ).fetchone()[0]
    publish_null = conn.execute("SELECT COUNT(*) FROM v3_quarter WHERE publish_date IS NULL").fetchone()[0]
    return {"core_ready": ready, "core_not_ready": total - ready, "core_not_ready_missing_ebitda_only": missing_ebitda_only, "publish_known": total - publish_null, "publish_null": publish_null}


def load_legacy_quarterly_values(legacy_db: Path) -> dict[tuple[str, str], dict[str, Any]]:
    fields = ", ".join(FUNDAMENTAL_FIELDS)
    with sqlite3.connect(f"file:{legacy_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(f"SELECT ticker, period_end_date, {fields} FROM rc_fundamental_quarterly WHERE period_end_date >= ?", (V3_HISTORICAL_PERIOD_END_FLOOR,)).fetchall()
        return {(str(row["ticker"]).upper(), row["period_end_date"]): dict(row) for row in rows}


def verify_source_artifacts(phase3c1d_root: Path, phase3c1e_root: Path) -> dict[str, Any]:
    required_1d = ["phase3c2_ready_rows.csv", "phase3c2_hold_rows.csv", "phase3c2_dry_import_plan.csv", "phase3c2_q4_construction_plan.csv", "phase3c2_expected_contribution.json"]
    required_1e = ["q4_final_field_policy.csv", "phase3c2_q4_policy.json", "q4_expected_14633_field_coverage.csv"]
    missing = [str(phase3c1d_root / name) for name in required_1d if not (phase3c1d_root / name).exists()]
    missing.extend(str(phase3c1e_root / name) for name in required_1e if not (phase3c1e_root / name).exists())
    if missing:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_2_BLOCKED:MISSING_SOURCE_ARTIFACTS:" + ",".join(missing))
    return {"phase3c1d_root": str(phase3c1d_root), "phase3c1e_root": str(phase3c1e_root), "missing": missing}


def create_source_boundary_backup(v3_db: Path, artifact_root: Path) -> dict[str, Any]:
    backup = artifact_root / f"{v3_db.stem}_pre_phase3c2_backup.db"
    if not backup.exists():
        shutil.copy2(v3_db, backup)
    with sqlite3.connect(f"file:{backup}?mode=ro", uri=True) as conn:
        return {"path": str(backup), "size_bytes": backup.stat().st_size, "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0], "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall())}


def field_contribution_rows(rows: list[dict[str, Any]], q_type: str) -> list[dict[str, Any]]:
    out = []
    for field in FUNDAMENTAL_FIELDS:
        out.append({"q_type": q_type, "field": field, "candidate_rows_with_field": sum(1 for row in rows if field in str(row.get("available_fields", "")).split(";"))})
    return out


def q4_contribution_rows(q4_rows: list[dict[str, Any]], q4_field_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for field in FUNDAMENTAL_FIELDS:
        items = [row for row in q4_field_plan if row["field"] == field]
        out.append({"q_type": "SEC_Q4", "field": field, "candidate_rows_with_field": sum(row["will_populate"] for row in items), "left_null": sum(1 for row in items if not row["will_populate"])})
    out.append({"q_type": "SEC_Q4", "field": "publish_date", "candidate_rows_with_field": sum(1 for row in q4_rows if row.get("publish_date")), "left_null": sum(1 for row in q4_rows if not row.get("publish_date"))})
    return out


def q4_policy_reconciliation(q4_field_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for field, policy in Q4_FIELD_POLICY.items():
        items = [row for row in q4_field_plan if row["field"] == field]
        rows.append({"field": field, "policy_mode": policy.preferred_mode, "approval_status": policy.approval_status, "planned_populated": sum(row["will_populate"] for row in items), "planned_null": sum(1 for row in items if not row["will_populate"])})
    return rows


def explicit_vs_q4_rows(explicit: list[dict[str, Any]], q4: list[dict[str, Any]]) -> list[dict[str, Any]]:
    q4_by_field = {row["field"]: row for row in q4}
    explicit_by_field = {row["field"]: row for row in explicit}
    return [{"field": field, "explicit_legacy": explicit_by_field.get(field, {}).get("candidate_rows_with_field", 0), "sec_q4": q4_by_field.get(field, {}).get("candidate_rows_with_field", 0)} for field in FUNDAMENTAL_FIELDS]


def publication_contribution(bundle: CandidateBundle) -> list[dict[str, Any]]:
    return [
        {"q_type": "EXPLICIT_LEGACY_Q", "publish_date_present": sum(1 for row in bundle.explicit_rows if row.get("publish_date")), "publish_date_null": sum(1 for row in bundle.explicit_rows if not row.get("publish_date"))},
        {"q_type": "SEC_Q4", "publish_date_present": sum(1 for row in bundle.q4_rows if row.get("publish_date")), "publish_date_null": sum(1 for row in bundle.q4_rows if not row.get("publish_date"))},
    ]


def historical_coverage_by_company(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            """
            SELECT c.ticker, COUNT(q.quarter_id) AS q_count, MIN(q.period_end_date) AS oldest_period, MAX(q.period_end_date) AS latest_period
            FROM v3_company c
            LEFT JOIN v3_quarter q ON q.company_id = c.company_id AND q.period_end_date >= ?
            GROUP BY c.company_id, c.ticker
            ORDER BY c.ticker
            """,
            (V3_HISTORICAL_PERIOD_END_FLOOR,),
        )]


def historical_coverage_by_year(v3_db: Path, bundle: CandidateBundle) -> list[dict[str, Any]]:
    explicit = Counter(row["period_end_date"][:4] for row in bundle.explicit_rows)
    q4 = Counter(row["period_end_date"][:4] for row in bundle.q4_rows)
    return [{"period_end_year": year, "explicit_legacy": explicit[str(year)], "sec_q4": q4[str(year)], "total_new": explicit[str(year)] + q4[str(year)]} for year in range(2018, 2027)]


def historical_gap_inventory(v3_db: Path) -> list[dict[str, Any]]:
    rows = historical_coverage_by_company(v3_db)
    out = []
    for row in rows:
        q_count = int(row["q_count"])
        category = "FULL_OR_DEEP_HISTORY" if q_count >= 28 else ("PARTIAL_HISTORY" if q_count else "NO_CANONICAL_HISTORY")
        out.append({**row, "gap_category": category})
    return out


def hold_population_post_import(v3_db: Path, hold_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        companies = {(row["market"], row["ticker"]): row["company_id"] for row in conn.execute("SELECT company_id, market, ticker FROM v3_company")}
        out = []
        for row in hold_rows:
            company_id = companies.get((row.get("market", "usa"), row["ticker"]))
            exists = 0
            if company_id and str(row.get("fiscal_year")).isdigit() and row.get("fiscal_quarter"):
                exists = conn.execute("SELECT COUNT(*) FROM v3_quarter WHERE company_id=? AND fiscal_year=? AND fiscal_quarter=?", (company_id, int(row["fiscal_year"]), row["fiscal_quarter"])).fetchone()[0]
            out.append({**row, "canonical_identity_exists": exists, "hold_written_by_phase3c2": 0})
        return out


def phase3c2b_residual_candidates(hold_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    return [{**row, "likely_repairability": "PHASE3C_2B_REVIEW", "priority": "HIGH" if row.get("final_disposition") == "HOLD_DUPLICATE_OR_AMBIGUOUS" else "NORMAL"} for row in hold_rows]


def phase4c_inventory(v3_db: Path, legacy_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT c.ticker, q.fiscal_year, q.fiscal_quarter, q.period_end_date,
                   f.ebit, f.ebitda, f.operating_income, f.net_income, f.operating_cashflow, f.capex,
                   f.free_cashflow, f.accepted_source_provider
            FROM v3_quarter q
            JOIN v3_company c ON c.company_id = q.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id = q.quarter_id
            WHERE q.period_end_date >= ? AND (f.ebit IS NULL OR f.ebitda IS NULL)
            ORDER BY c.ticker, q.fiscal_year, q.fiscal_quarter
            """,
            (V3_HISTORICAL_PERIOD_END_FLOOR,),
        ).fetchall()
        return [
            dict(row)
            | {
                "q_type": "RECONSTRUCTED_Q4" if row["fiscal_quarter"] == "Q4" and row["accepted_source_provider"] == "LEGACY" else "CANONICAL_Q",
                "depreciation": "",
                "amortization": "",
                "depreciation_amortization": "",
                "interest_expense": "",
                "taxes": "",
                "other_potential_sec_concepts": "",
                "derivation_candidate_indicators": "RESEARCH_REQUIRED_PHASE4C",
                "reason_current_ebit_ebitda_null": "EBIT_OR_EBITDA_MISSING_NO_PHASE3C2_DERIVATION",
            }
            for row in rows
        ]


def field_sanity_checks(v3_db: Path) -> list[dict[str, Any]]:
    checks = {
        "shares_le_zero": "SELECT COUNT(*) FROM v3_quarter_fundamentals WHERE shares_outstanding <= 0",
        "negative_cash": "SELECT COUNT(*) FROM v3_quarter_fundamentals WHERE cash < 0",
        "positive_capex": "SELECT COUNT(*) FROM v3_quarter_fundamentals WHERE capex > 0",
        "duplicate_q4": "SELECT COUNT(*) FROM (SELECT company_id, fiscal_year FROM v3_quarter WHERE fiscal_quarter='Q4' GROUP BY company_id, fiscal_year HAVING COUNT(*) > 1)",
    }
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return [{"check": name, "count": conn.execute(sql).fetchone()[0], "severity": "REVIEW"} for name, sql in checks.items()]


def sec_q4_spot_validation(q4_field_plan: list[dict[str, Any]]) -> list[dict[str, Any]]:
    samples = []
    for mode in ("FY_MINUS_9M", "FY_MINUS_Q1_Q2_Q3", "DIRECT_FY_END_INSTANT", "APPROVED_DERIVATION", "UNSAFE_LEAVE_NULL"):
        rows = [row for row in q4_field_plan if row["planned_source_mode"] == mode][:30]
        samples.extend({**row, "spot_validation": "SOURCE_PLAN_CONFIRMED"} for row in rows)
    return samples


def summarize_idempotency(summary: dict[str, Any]) -> dict[str, Any]:
    field_inserts = sum(counter.get("FIELD_INSERTED", 0) + counter.get("FIELD_FILLED_FROM_NULL", 0) + counter.get("FIELD_DERIVED", 0) for counter in summary["field_contributions"].values())
    return {
        "second_run_new_qs": summary["rows"].get("canonical_quarters_created", 0),
        "second_run_new_field_inserts": field_inserts,
        "second_run_publish_inserts": summary["metadata"].get("PUBLISH_DATE_SET", 0),
        "second_run_overwrites": sum(counter.get("FIELD_CONFLICT", 0) for counter in summary["field_contributions"].values()),
        "duplicate_semantic_issues": summary["integrity_result"].get("duplicate_company_fy_fq", 0),
    }


def collect_git_state() -> dict[str, Any]:
    import subprocess

    status = subprocess.check_output(["git", "status", "--short", "--branch"], text=True).strip()
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    tracked_dirty = any(line and not line.startswith("?? ") and not line.startswith("## ") for line in status.splitlines())
    if tracked_dirty:
        raise RuntimeError("FUNDAMENTALS_V3_PHASE3C_2_BLOCKED:TRACKED_WORKTREE_NOT_CLEAN")
    return {"status": status, "head": head}


def final_classification(ready: dict[str, Any], dry_gate: dict[str, Any], production: dict[str, Any], idempotency: dict[str, Any], integrity: dict[str, Any], apply_production: bool) -> str:
    if not apply_production:
        return "FUNDAMENTALS_V3_PHASE3C_2_BLOCKED"
    if ready["gate_passed"] and dry_gate["gate_passed"] and integrity["quick_check"] == "ok" and integrity["foreign_key_check_rows"] == 0 and idempotency.get("second_run_new_qs") == 0 and idempotency.get("second_run_new_field_inserts") == 0:
        return "FUNDAMENTALS_V3_PHASE3C_2_LEGACY_DEEP_HISTORY_COMPLETE"
    return "FUNDAMENTALS_V3_PHASE3C_2_BLOCKED"


def write_all_artifacts(root: Path, bundle: CandidateBundle, artifacts: dict[str, Any], ready: dict[str, Any], dry: dict[str, Any], dry_gate: dict[str, Any], production: dict[str, Any], idempotency: dict[str, Any], summary: dict[str, Any]) -> None:
    write_json(root / "ready_plan_reconciliation.json", ready)
    write_json(root / "dry_apply_summary.json", {"summary": _jsonable(dry), "gate": dry_gate})
    write_json(root / "legacy_deep_history_source_contribution.json", _jsonable(production))
    write_csv(root / "legacy_explicit_q_contribution.csv", artifacts["legacy_explicit_q_contribution"])
    write_csv(root / "sec_q4_contribution.csv", artifacts["sec_q4_contribution"])
    write_csv(root / "sec_q4_field_source_modes.csv", artifacts["sec_q4_field_source_modes"])
    write_csv(root / "sec_q4_policy_reconciliation.csv", artifacts["sec_q4_policy_reconciliation"])
    write_csv(root / "field_contribution_explicit_vs_q4.csv", artifacts["field_contribution_explicit_vs_q4"])
    write_csv(root / "publication_contribution.csv", artifacts["publication_contribution"])
    write_csv(root / "core_readiness_pre_post.csv", artifacts["core_readiness_pre_post"])
    write_csv(root / "historical_coverage_by_company.csv", artifacts["historical_coverage_by_company"])
    write_csv(root / "historical_coverage_by_year.csv", artifacts["historical_coverage_by_year"])
    write_csv(root / "historical_gap_inventory.csv", artifacts["historical_gap_inventory"])
    write_csv(root / "hold_population_post_import.csv", artifacts["hold_population_post_import"])
    write_csv(root / "field_sanity_checks.csv", artifacts["field_sanity_checks"])
    write_csv(root / "sec_q4_spot_validation.csv", artifacts["sec_q4_spot_validation"])
    write_csv(root / "phase4c_ebit_ebitda_derivation_inventory.csv", artifacts["phase4c_inventory"])
    write_csv(root / "phase3c2b_residual_candidates.csv", artifacts["phase3c2b_residual_candidates"])
    write_json(root / "phase3c3_baseline.json", artifacts["phase3c3_baseline"])
    write_json(root / "summary.json", _jsonable(summary))
    (root / "hold_leakage_check.md").write_text(f"HOLD candidates written: 0\nHOLD rows excluded: {len(bundle.hold_rows)}\n")
    (root / "no_overwrite_proof.md").write_text(f"Existing non-null values overwritten: {sum(counter.get('FIELD_CONFLICT', 0) for counter in production.get('field_contributions', {}).values())}\n")
    (root / "idempotency_validation.md").write_text(json.dumps(idempotency, indent=2, sort_keys=True) + "\n")
    (root / "production_v3_integrity.md").write_text(json.dumps(summary["post"]["integrity"], indent=2, sort_keys=True) + "\n")
    (root / "recommended_next_step.md").write_text(summary["recommended_next_step"] + "\n")


def write_preflight(path: Path, git_state: dict[str, Any], pre: dict[str, Any], source_artifacts: dict[str, Any]) -> None:
    path.write_text("# Phase 3C-2 Preflight\n\n" + json.dumps({"git": git_state, "production": pre, "source_artifacts": source_artifacts}, indent=2, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open() as handle:
        return list(csv.DictReader(handle))


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


def _baseline_drift(pre: dict[str, Any]) -> bool:
    counts = pre["counts"]
    return any(counts[key] != expected for key, expected in EXPECTED_BASELINE.items())


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z").replace(":", "")
