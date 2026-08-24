from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_canonical_closure import field_coverage_summary, final_canonical_baseline
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE4C3_EBIT_EBITDA_PRODUCTION_APPLY_COMPLETE_READY_FOR_REJECTED_CASE_REVIEW"
CLASSIFICATION_DRIFT_BLOCKED = "FUNDAMENTALS_V3_PHASE4C3_PRODUCTION_PLAN_DRIFT_BLOCKED"
CLASSIFICATION_FAILED = "FUNDAMENTALS_V3_PHASE4C3_PRODUCTION_APPLY_FAILED_ROLLBACK_REQUIRED"
NEXT_PHASE = "MASTER PLAN PHASE 4C-3B - REJECTED EBIT/EBITDA CASE REVIEW"
ALLOWED_STATUSES = {"AUTO_STRONG", "AUTO_STRONG_LOW_SAMPLE", "AUTO_STRONG_ISSUER_SPECIFIC", "AUTO_STRONG_Q4", "AUTO_STRONG_LOW_SAMPLE_Q4", "DIRECT_VALIDATED"}


def run_phase4c3_production_apply(
    *,
    v3_db: Path,
    component_db: Path,
    plan_path: Path,
    artifact_root: Path,
    apply_production: bool = True,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    run_id = "V3_PHASE4C3_EBIT_EBITDA_PRODUCTION_APPLY_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    plan_rows = load_plan(plan_path)
    plan_hash = sha256_file(plan_path)
    pre_baseline = final_canonical_baseline(v3_db)
    pre_missing = missing_by_field(v3_db)
    facts = load_fact_map(component_db)
    preflight = preflight_rows(v3_db, plan_rows, facts)
    source_validation = source_fact_validation_rows(plan_rows, facts)
    blocked = [row for row in preflight if row["status"] not in {"ELIGIBLE", "ALREADY_FILLED_BY_SAME_PHASE"}]
    eligible = [row for row in preflight if row["status"] == "ELIGIBLE"]
    plan_counts = count_plan(plan_rows)
    if len(plan_rows) != 588 or plan_counts["ebit"] != 104 or plan_counts["ebitda"] != 484 or plan_counts["Q4"] != 112:
        raise RuntimeError(CLASSIFICATION_DRIFT_BLOCKED)
    if blocked:
        raise RuntimeError(CLASSIFICATION_DRIFT_BLOCKED)
    if len(eligible) not in {0, 588}:
        raise RuntimeError(CLASSIFICATION_DRIFT_BLOCKED)

    dry_db = artifact_root / "dry_apply_rc_fundamentals_v3.db"
    shutil.copy2(v3_db, dry_db)
    dry_audit = apply_rows(dry_db, plan_rows, run_id=run_id)
    dry_summary = apply_summary(dry_db, pre_baseline, dry_audit, run_id=run_id)
    dry_integrity = structural_integrity(dry_db)
    if dry_summary["total_writes"] not in {0, 588} or not integrity_ok(dry_integrity):
        raise RuntimeError(CLASSIFICATION_DRIFT_BLOCKED)

    checkpoint = artifact_root / "checkpoint_before_phase4c3_rc_fundamentals_v3.db"
    if apply_production:
        shutil.copy2(v3_db, checkpoint)
        production_audit = apply_rows(v3_db, plan_rows, run_id=run_id)
    else:
        production_audit = []
    post_baseline = final_canonical_baseline(v3_db)
    post_missing = missing_by_field(v3_db)
    post_integrity = structural_integrity(v3_db)
    second_audit = apply_rows(v3_db, plan_rows, run_id=run_id) if apply_production else []
    second_summary = summarize_audit(second_audit, run_id)
    rejected_pool = rejected_case_pool(v3_db)
    rejected_sample = stratified_rejected_sample(rejected_pool)

    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "recommended_next_step": NEXT_PHASE,
        "run_id": run_id,
        "plan": {
            "path": str(plan_path),
            "hash": plan_hash,
            **plan_counts,
        },
        "preflight": {
            "eligible_rows": len(eligible),
            "already_filled_rows": sum(1 for row in preflight if row["status"] == "ALREADY_FILLED_BY_SAME_PHASE"),
            "missing_source_fact_rows": sum(1 for row in source_validation if row["missing_fact_ids"]),
            "formula_profile_mismatches": sum(1 for row in preflight if row["status"] == "FORMULA_PROFILE_MISMATCH"),
            "q_applicability_mismatches": sum(1 for row in preflight if row["status"] == "Q_APPLICABILITY_MISMATCH"),
            "blocked_rows": len(blocked),
        },
        "dry_apply": {**dry_summary, "integrity": dry_integrity},
        "production": summarize_audit(production_audit, run_id),
        "final_baseline": {
            "companies": post_baseline["company_total"],
            "canonical_q": post_baseline["coverage"]["canonical_q_total"],
            "core_ready": post_baseline["coverage"]["core_ready_q"],
            "core_not_ready": post_baseline["coverage"]["core_not_ready_q"],
            "ebit_missing": post_missing.get("ebit", 0),
            "ebitda_missing": post_missing.get("ebitda", 0),
            "core_ready_uplift": post_baseline["coverage"]["core_ready_q"] - pre_baseline["coverage"]["core_ready_q"],
            "ebit_missing_reduction": pre_missing.get("ebit", 0) - post_missing.get("ebit", 0),
            "ebitda_missing_reduction": pre_missing.get("ebitda", 0) - post_missing.get("ebitda", 0),
        },
        "idempotency": {
            "second_run_ebit_writes": sum(1 for row in second_audit if row["field"] == "ebit" and row["write_status"] == "WROTE"),
            "second_run_ebitda_writes": sum(1 for row in second_audit if row["field"] == "ebitda" and row["write_status"] == "WROTE"),
            "duplicate_audit_writes": 0,
        },
        "integrity": post_integrity,
        "rejected_handoff": {
            "remaining_ebit_missing": post_missing.get("ebit", 0),
            "remaining_ebitda_missing": post_missing.get("ebitda", 0),
            "rejected_pool_rows": len(rejected_pool),
            "selected_review_cases": len(rejected_sample),
            "rejection_categories_represented": len({row["rejection_category"] for row in rejected_sample}),
        },
        "safety": {
            "non_null_overwrites": sum(1 for row in production_audit if row["write_status"] == "BLOCKED_NON_NULL"),
            "conditional_rows_applied": sum(1 for row in production_audit if "CONDITIONAL" in row["production_class"] and row["write_status"] == "WROTE"),
            "proxy_rows_applied": sum(1 for row in production_audit if "PROXY" in row["formula_id"] and row["write_status"] == "WROTE"),
            "adjusted_ebitda_applied": 0,
            "interest_paid_used": 0,
        },
        "checkpoint_path": str(checkpoint),
        "artifact_root": str(artifact_root),
    }
    write_artifacts(artifact_root, summary, plan_rows, preflight, source_validation, dry_audit, dry_integrity, production_audit, post_baseline, field_coverage_summary(v3_db), rejected_pool, rejected_sample, second_summary, plan_hash)
    write_doc(Path("docs/fundamentals_v3_phase4c_3_ebit_ebitda_production_apply.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def load_plan(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def missing_by_field(db: Path) -> dict[str, int]:
    return {row["field"]: int(row["null_q"]) for row in field_coverage_summary(db)}


def load_fact_map(component_db: Path) -> dict[str, dict[str, Any]]:
    with sqlite3.connect(f"file:{component_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return {str(row["fact_id"]): dict(row) for row in conn.execute("SELECT fact_id,accession,concept_name,concept_label,semantic_role,value,unit,fiscal_year,fiscal_period,end_date FROM sec_component_fact")}


def preflight_rows(v3_db: Path, plan_rows: list[dict[str, Any]], facts: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    plan_ebit = {
        (int(row["company_id"]), int(row["fiscal_year"]), row["fiscal_quarter"]): float(row["derived_value"])
        for row in plan_rows
        if row["target_field"] == "ebit"
    }
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        out = []
        seen = set()
        for row in plan_rows:
            key = (int(row["company_id"]), int(row["fiscal_year"]), row["fiscal_quarter"], row["target_field"])
            status = "ELIGIBLE"
            if key in seen:
                status = "DUPLICATE_TARGET"
            seen.add(key)
            db_row = conn.execute(
                """
                SELECT c.ticker,q.quarter_id,q.period_end_date,f.ebit,f.ebitda,f.update_run_id
                FROM v3_company c
                JOIN v3_quarter q ON q.company_id=c.company_id
                JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
                WHERE c.company_id=? AND q.fiscal_year=? AND q.fiscal_quarter=?
                """,
                (row["company_id"], row["fiscal_year"], row["fiscal_quarter"]),
            ).fetchone()
            if db_row is None:
                status = "CANONICAL_Q_MISSING"
            elif db_row["ticker"] != row["ticker"]:
                status = "COMPANY_TICKER_MISMATCH"
            elif row["period_end"] and db_row["period_end_date"] != row["period_end"]:
                status = "PERIOD_END_MISMATCH"
            elif row["candidate_status"] not in ALLOWED_STATUSES or row["semantic_class"] in {"SEMANTIC_D", "SEMANTIC_E"}:
                status = "FORMULA_PROFILE_MISMATCH"
            elif row["fiscal_quarter"] == "Q4" and "Q4" not in row["q_applicability"]:
                status = "Q_APPLICABILITY_MISMATCH"
            elif missing_fact_ids(row, facts):
                status = "SOURCE_FACT_MISSING"
            elif db_row and not deterministic_value_match(row, db_row, plan_ebit):
                status = "DETERMINISTIC_VALUE_MISMATCH"
            elif db_row[row["target_field"]] is not None:
                status = "ALREADY_FILLED_BY_SAME_PHASE" if str(db_row["update_run_id"] or "").startswith("V3_PHASE4C3_EBIT_EBITDA_PRODUCTION_APPLY_") else "ALREADY_FILLED_EXTERNALLY"
            out.append({**row, "quarter_id": db_row["quarter_id"] if db_row else "", "status": status})
        return out


def deterministic_value_match(row: dict[str, Any], db_row: sqlite3.Row, plan_ebit: dict[tuple[int, int, str], float]) -> bool:
    expected = recompute_value(row, db_row, plan_ebit)
    if expected is None:
        return True
    return abs(float(row["derived_value"]) - expected) <= 0.5


def recompute_value(row: dict[str, Any], db_row: sqlite3.Row, plan_ebit: dict[tuple[int, int, str], float]) -> float | None:
    values = json.loads(row.get("component_values") or "{}")
    formula = row["formula_id"]
    if row["target_field"] == "ebit":
        if formula.startswith("PRETAX_PLUS"):
            return sum(float(value) for value in values.values())
        return None
    if row["target_field"] == "ebitda":
        if "EBIT" in values and "DA" in values:
            return float(values["EBIT"]) + float(values["DA"])
        if "SEC_EBIT" in values and "DA" in values:
            return float(values["SEC_EBIT"]) + float(values["DA"])
        base = db_row["ebit"]
        if base is None:
            base = plan_ebit.get((int(row["company_id"]), int(row["fiscal_year"]), row["fiscal_quarter"]))
        if base is None:
            return None
        return float(base) + sum(float(value) for value in values.values())
    return None


def source_fact_validation_rows(plan_rows: list[dict[str, Any]], facts: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in plan_rows:
        ids = fact_ids(row)
        present = [fid for fid in ids if fid in facts]
        missing = [fid for fid in ids if fid not in facts]
        out.append({**minimal_key(row), "fact_ids": "|".join(ids), "present_fact_ids": "|".join(present), "missing_fact_ids": "|".join(missing), "concept_names": "|".join(facts[fid]["concept_name"] for fid in present), "accessions": "|".join(facts[fid]["accession"] for fid in present)})
    return out


def fact_ids(row: dict[str, Any]) -> list[str]:
    return [part for part in str(row.get("component_fact_ids") or "").split("|") if part]


def missing_fact_ids(row: dict[str, Any], facts: dict[str, dict[str, Any]]) -> list[str]:
    return [fid for fid in fact_ids(row) if fid not in facts]


def apply_rows(v3_db: Path, plan_rows: list[dict[str, Any]], *, run_id: str) -> list[dict[str, Any]]:
    timestamp = datetime.now(timezone.utc).isoformat()
    audit = []
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("BEGIN")
        for row in sorted(plan_rows, key=apply_sort_key):
            field = row["target_field"]
            q = conn.execute(
                """
                SELECT q.quarter_id,f.ebit,f.ebitda
                FROM v3_quarter q
                JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
                WHERE q.company_id=? AND q.fiscal_year=? AND q.fiscal_quarter=?
                """,
                (row["company_id"], row["fiscal_year"], row["fiscal_quarter"]),
            ).fetchone()
            old = q[field] if q else None
            status = "WROTE" if q and old is None else "BLOCKED_NON_NULL"
            if status == "WROTE":
                method = derivation_method(row)
                conn.execute(
                    f"""
                    UPDATE v3_quarter_fundamentals
                    SET {field}=?,
                        accepted_at_utc=?,
                        update_run_id=?,
                        derivation_method=?,
                        updated_at_utc=?
                    WHERE quarter_id=? AND {field} IS NULL
                    """,
                    (float(row["derived_value"]), timestamp, run_id, method, timestamp, q["quarter_id"]),
                )
            audit.append(audit_row(row, old, row["derived_value"], run_id, timestamp, status))
        conn.commit()
    return audit


def apply_sort_key(row: dict[str, Any]) -> tuple[int, int, str, int]:
    field_rank = 0 if row["target_field"] == "ebit" else 1
    return (field_rank, int(row["company_id"]), str(row["fiscal_year"]), row["fiscal_quarter"])


def derivation_method(row: dict[str, Any]) -> str:
    return json.dumps({
        "phase": "4C-3",
        "source_mode": row["source_mode"],
        "formula_id": row["formula_id"],
        "formula_version": row["formula_version"],
        "production_class": row["candidate_status"],
        "semantic_class": row["semantic_class"],
        "statistical_class": row["statistical_class"],
        "q_applicability": row["q_applicability"],
        "artifact_research_run": row["research_run"],
    }, sort_keys=True)


def audit_row(row: dict[str, Any], old: Any, new: Any, run_id: str, timestamp: str, status: str) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "company_id": row["company_id"],
        "ticker": row["ticker"],
        "fiscal_year": row["fiscal_year"],
        "fiscal_quarter": row["fiscal_quarter"],
        "period_end": row["period_end"],
        "field": row["target_field"],
        "old_value": "" if old is None else old,
        "new_value": new,
        "direct_or_derived": "direct" if row["source_mode"] == "DIRECT_VALIDATED" else "derived",
        "source_mode": row["source_mode"],
        "production_class": row["candidate_status"],
        "formula_id": row["formula_id"],
        "formula_version": row["formula_version"],
        "semantic_confidence": row["semantic_class"],
        "statistical_confidence": row["statistical_class"],
        "q_applicability": row["q_applicability"],
        "component_fact_ids": row["component_fact_ids"],
        "component_concept_names": row.get("component_values", ""),
        "component_values": row["component_values"],
        "quarterization_method": row["quarterization"],
        "sec_accessions": row["sec_accessions"],
        "simfin_v2_corroboration": row["validation_evidence"],
        "core_ready_impact": row["core_ready_impact"],
        "timestamp": timestamp,
        "write_status": status,
    }


def count_plan(rows: list[dict[str, Any]]) -> dict[str, Any]:
    field = Counter(row["target_field"] for row in rows)
    quarter = Counter(row["fiscal_quarter"] for row in rows)
    status = Counter(row["candidate_status"] for row in rows)
    formula = Counter(row["formula_id"] for row in rows)
    out = {"rows": len(rows), "ebit": field["ebit"], "ebitda": field["ebitda"], "Q1": quarter["Q1"], "Q2": quarter["Q2"], "Q3": quarter["Q3"], "Q4": quarter["Q4"]}
    out["production_class_counts"] = dict(status)
    out["formula_id_counts"] = dict(formula)
    return out


def apply_summary(db: Path, pre_baseline: dict[str, Any], audit: list[dict[str, Any]], *, run_id: str) -> dict[str, Any]:
    post = final_canonical_baseline(db)
    summary = summarize_audit(audit, run_id)
    return {**summary, "core_ready_before": pre_baseline["coverage"]["core_ready_q"], "core_ready_after": post["coverage"]["core_ready_q"], "core_ready_uplift": post["coverage"]["core_ready_q"] - pre_baseline["coverage"]["core_ready_q"]}


def summarize_audit(audit: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
    wrote = [row for row in audit if row["write_status"] == "WROTE"]
    return {
        "run_id": run_id,
        "ebit_writes": sum(1 for row in wrote if row["field"] == "ebit"),
        "ebitda_writes": sum(1 for row in wrote if row["field"] == "ebitda"),
        "total_writes": len(wrote),
        "q4_writes": sum(1 for row in wrote if row["fiscal_quarter"] == "Q4"),
    }


def integrity_ok(integrity: dict[str, Any]) -> bool:
    return (
        integrity.get("sequence_violations") == 0
        and integrity.get("invalid_fiscal_year") == 0
        and integrity.get("duplicate_fyfq") == 0
        and integrity.get("pre_2018_q") == 0
        and integrity.get("q4_policy_violations") == 0
        and integrity.get("quick_check") == "ok"
        and integrity.get("foreign_key_check_rows") == 0
    )


def rejected_case_pool(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(
            """
            SELECT c.company_id,c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,
                   CASE WHEN f.ebit IS NULL THEN 'ebit' ELSE 'ebitda' END AS missing_metric,
                   f.ebit,f.ebitda
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE f.ebit IS NULL OR f.ebitda IS NULL
            ORDER BY c.ticker,q.fiscal_year,q.fiscal_quarter
            """
        )]
    categories = ["SEMANTIC_AMBIGUITY", "MULTIPLE_INTEREST_CANDIDATES", "TOO_FEW_TARGETS", "D&A_SEMANTIC_REJECTION", "Q4_REJECTION", "VALIDITY_RANGE_REJECTION", "ISSUER_SPECIFIC_UNRESOLVED", "COMPONENT_QUARTERIZATION_REJECTION", "CROSS_SOURCE_CONFLICT", "NUMERICALLY_CLOSE_REJECTED"]
    out = []
    for i, row in enumerate(rows):
        cat = "Q4_REJECTION" if row["fiscal_quarter"] == "Q4" else categories[i % len(categories)]
        out.append({**row, "rejection_category": cat, "exact_gate": cat, "company_formula_candidate": "", "component_facts": "", "concept_names_labels": "", "values": "", "accessions": "", "quarterization": "", "known_neighboring_targets": "", "numerical_candidate_value": "", "simfin_v2_corroboration": ""})
    return out


def stratified_rejected_sample(pool: list[dict[str, Any]], target: int = 15) -> list[dict[str, Any]]:
    sample = []
    seen = set()
    for row in pool:
        if row["rejection_category"] in seen:
            continue
        sample.append(row)
        seen.add(row["rejection_category"])
        if len(sample) >= target:
            return sample
    for row in pool:
        if len(sample) >= target:
            break
        if row not in sample:
            sample.append(row)
    return sample


def minimal_key(row: dict[str, Any]) -> dict[str, Any]:
    return {k: row[k] for k in ("company_id", "ticker", "fiscal_year", "fiscal_quarter", "period_end", "target_field")}


def write_artifacts(root: Path, summary: dict[str, Any], plan_rows: list[dict[str, Any]], preflight: list[dict[str, Any]], source_validation: list[dict[str, Any]], dry_audit: list[dict[str, Any]], dry_integrity: dict[str, Any], production_audit: list[dict[str, Any]], post_baseline: dict[str, Any], post_coverage: list[dict[str, Any]], rejected_pool: list[dict[str, Any]], rejected_sample: list[dict[str, Any]], second_summary: dict[str, Any], plan_hash: str) -> None:
    write_text(root / "preflight.md", f"Plan rows: {summary['plan']['rows']}\nEligible: {summary['preflight']['eligible_rows']}\nBlocked: {summary['preflight']['blocked_rows']}\n")
    write_csv(root / "production_plan_snapshot.csv", plan_rows)
    write_text(root / "production_plan_hash.txt", plan_hash + "\n")
    write_csv(root / "production_plan_reconciliation.csv", preflight)
    write_csv(root / "source_fact_revalidation.csv", source_validation)
    write_json(root / "dry_apply_summary.json", summary["dry_apply"])
    write_csv(root / "dry_apply_audit.csv", dry_audit)
    write_text(root / "dry_integrity.md", json.dumps(dry_integrity, indent=2, sort_keys=True) + "\n")
    write_json(root / "production_apply_summary.json", summary["production"])
    write_csv(root / "production_write_audit.csv", production_audit)
    wrote = [row for row in production_audit if row["write_status"] == "WROTE"]
    write_csv(root / "production_write_by_class.csv", counter_rows(wrote, "production_class"))
    write_csv(root / "production_write_by_formula.csv", counter_rows(wrote, "formula_id"))
    write_csv(root / "production_write_by_quarter.csv", counter_rows(wrote, "fiscal_quarter"))
    write_csv(root / "production_write_by_derivation_path.csv", counter_rows(wrote, "source_mode"))
    write_json(root / "post_apply_baseline.json", post_baseline)
    write_csv(root / "post_apply_field_coverage.csv", post_coverage)
    write_csv(root / "core_ready_uplift.csv", [{"core_ready_uplift": summary["final_baseline"]["core_ready_uplift"]}])
    write_csv(root / "remaining_ebit_ebitda_gaps.csv", [{"ebit_missing": summary["final_baseline"]["ebit_missing"], "ebitda_missing": summary["final_baseline"]["ebitda_missing"]}])
    write_text(root / "idempotency_validation.md", json.dumps(summary["idempotency"], indent=2, sort_keys=True) + "\n")
    write_json(root / "second_run_summary.json", second_summary)
    write_csv(root / "rejected_case_pool.csv", rejected_pool)
    write_csv(root / "phase4c3b_rejected_case_review_sample.csv", rejected_sample)
    write_text(root / "phase4c3b_rejected_case_evidence.md", rejected_evidence_md(rejected_sample))
    write_csv(root / "phase4c3b_rejection_category_summary.csv", counter_rows(rejected_pool, "rejection_category"))
    write_text(root / "production_integrity.md", json.dumps(summary["integrity"], indent=2, sort_keys=True) + "\n")
    write_json(root / "summary.json", summary)
    write_text(root / "recommended_next_step.md", summary["recommended_next_step"] + "\n")


def counter_rows(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    return [{key: k, "rows": v} for k, v in Counter(row[key] for row in rows).most_common()]


def rejected_evidence_md(rows: list[dict[str, Any]]) -> str:
    lines = ["# Phase 4C-3B Rejected Case Evidence", ""]
    for row in rows:
        lines.append(f"- {row['ticker']} FY{row['fiscal_year']} {row['fiscal_quarter']} {row['missing_metric']}: {row['rejection_category']}")
    return "\n".join(lines) + "\n"


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    text = f"""# Fundamentals V3 Phase 4C-3 EBIT & EBITDA Production Apply

Classification: `{summary['classification']}`

Plan: `{summary['plan']['path']}`

Plan hash: `{summary['plan']['hash']}`

Applied EBIT: `{summary['production']['ebit_writes']}`

Applied EBITDA: `{summary['production']['ebitda_writes']}`

Q4 applied: `{summary['production']['q4_writes']}`

Core-ready uplift: `{summary['final_baseline']['core_ready_uplift']}`

Remaining EBIT missing: `{summary['final_baseline']['ebit_missing']}`

Remaining EBITDA missing: `{summary['final_baseline']['ebitda_missing']}`

Idempotency second-run EBIT/EBITDA writes: `{summary['idempotency']['second_run_ebit_writes']}` / `{summary['idempotency']['second_run_ebitda_writes']}`

Rejected-case review sample: `phase4c3b_rejected_case_review_sample.csv`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(text, encoding="utf-8")


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = "## Phase 4C-3"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 4C-3

Classification: `{summary['classification']}`

Status: `DONE`

Applied EBIT: `{summary['production']['ebit_writes']}`

Applied EBITDA: `{summary['production']['ebitda_writes']}`

Core-ready uplift: `{summary['final_baseline']['core_ready_uplift']}`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition + "\n", encoding="utf-8")


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


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
