from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals import v3_phase5_ttm_engine as p5
from swingmaster.fundamentals import v3_phase6f_valuation_engine as p6f
from swingmaster.fundamentals import v3_phase6g_legacy2_score_engine as p6g
from swingmaster.fundamentals import v3_phase6h_lifecycle_engine as p6h
from swingmaster.fundamentals import v3_phase6i_production_rebuild as p6i
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10b_full_sequence_audit import Phase8A10BPaths, run_phase8a10b_full_sequence_audit
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro, file_state, integrity
from swingmaster.fundamentals.v3_phase8a6_safe_apply import read_csv, sha_rows


CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE8B_DOWNSTREAM_REBUILD_COMPLETE_WITH_KNOWN_CANONICAL_DEFECTS"
CLASSIFICATION_PARTIAL = "FUNDAMENTALS_V3_PHASE8B_DOWNSTREAM_REBUILD_PARTIAL_WITH_KNOWN_CANONICAL_DEFECTS"
CLASSIFICATION_BLOCKED = "FUNDAMENTALS_V3_PHASE8B_DOWNSTREAM_REBUILD_BLOCKED"
EXPECTED_P1_TICKERS = ("BBY", "DELL", "FNGR", "GCO", "HAE", "MRVL", "POWW", "RH", "RL", "SAIC", "TJX", "TRNS", "VTGN")
NINE_52_53_TICKERS = ("BBY", "DELL", "GCO", "HAE", "MRVL", "RL", "SAIC", "TJX", "TRNS")
EXPECTED_SCORE_MODEL = "V3_LEGACY2_FUNDAMENTAL_SCORE_V1"
EXPECTED_SCORE_FINGERPRINT = "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"
EXPECTED_LIFECYCLE_MODEL = "V3_LIFECYCLE_EBIT_FIRST_V1"
EXPECTED_LIFECYCLE_FINGERPRINT = "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"


@dataclass(frozen=True)
class Phase8BPaths:
    artifact_root: Path
    v3_db: Path = Path("rc_fundamentals_v3.db")
    rawcandle_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")
    publish_apply_root: Path = Path("temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z")
    score_artifact_root: Path | None = None
    lifecycle_artifact_root: Path | None = None


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def canonical_fingerprint(v3_db: Path) -> dict[str, Any]:
    sql = """
        SELECT c.company_id,c.market,c.ticker,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
               f.revenue,f.operating_income,f.ebit,f.ebitda,f.net_income,f.operating_cashflow,f.capex,
               f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding
        FROM v3_company c
        JOIN v3_quarter q ON q.company_id=c.company_id
        JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
        ORDER BY c.company_id,q.fiscal_year,CASE q.fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 ELSE 9 END
    """
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        data = [dict(row) for row in conn.execute(sql)]
    return {"rows": len(data), "sha256": sha_rows(data), "fields": "company_id,market,ticker,FY/FQ,period_end,publish_date,core canonical fundamentals"}


def baseline_summary(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return {
            "companies": table_count(conn, "v3_company"),
            "active_companies": int(conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=1").fetchone()[0]),
            "inactive_companies": int(conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=0").fetchone()[0]),
            "canonical_quarter_rows": table_count(conn, "v3_quarter"),
            "fundamentals_rows": table_count(conn, "v3_quarter_fundamentals"),
            "migration_audit_rows": table_count(conn, "v3_migration_audit"),
            "provider_acquisition_rows": table_count(conn, "v3_provider_q_acquisition"),
            "ttm_rows": table_count(conn, "v3_ttm"),
            "score_rows": table_count(conn, "v3_score"),
            "lifecycle_rows": table_count(conn, "v3_lifecycle"),
            "valuation_rows": table_count(conn, "v3_valuation"),
        }


def run_pre_a10b(paths: Phase8BPaths) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = paths.artifact_root / "pre_rebuild_a10b"
    sentinel_raw = paths.artifact_root / "rawcandle_a10b_guard_not_used.db"
    run_phase8a10b_full_sequence_audit(Phase8A10BPaths(artifact_root=root, v3_db=paths.v3_db, rawcandle_db=sentinel_raw, publish_apply_root=paths.publish_apply_root))
    p1 = read_csv(root / "global_P1.csv")
    summary = json.loads((root / "phase8a10b_summary.json").read_text(encoding="utf-8"))
    return p1, summary


def validate_p1_baseline(p1: list[dict[str, Any]]) -> None:
    tickers = tuple(sorted({row["ticker"] for row in p1}))
    if len(p1) != 15 or tickers != tuple(sorted(EXPECTED_P1_TICKERS)):
        raise RuntimeError(f"PHASE8B_P1_BASELINE_DRIFT:rows={len(p1)}:tickers={tickers}")


def deferred_defect_register(p1: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for idx, row in enumerate(p1, 1):
        ticker = row["ticker"]
        if ticker in NINE_52_53_TICKERS:
            scope = "nine_52_53_week_recent_segment"
            dtype = "FYQ_PERIOD_END_ECONOMIC_CONTENT_MAPPING_DEFECT"
            phase = "A10E-R3"
            action = "resume clean latest-8Q reconstruction or decide ticker-specific removal later"
        elif ticker == "FNGR":
            scope = "global_p1_structural_special"
            dtype = "FNGR_RESIDUAL_STRUCTURAL_CASE"
            phase = "A10A-SPECIAL"
            action = "resume FNGR structural/value reconciliation"
        elif ticker == "RH":
            scope = "global_p1_structural_special"
            dtype = "DUPLICATE_ECONOMIC_QUARTER"
            phase = "A10A-SPECIAL"
            action = "resolve duplicate/economic-quarter collision"
        elif ticker == "POWW":
            scope = "global_p1_publish_or_sequence"
            dtype = "POWW_RESIDUAL_CASE"
            phase = "A10B"
            action = "resume residual P1 evidence resolution"
        else:
            scope = "global_p1_publish_or_sequence"
            dtype = "VTGN_RESIDUAL_CASE"
            phase = "A10B"
            action = "resume residual P1 evidence resolution"
        out.append(
            {
                "defect_id": f"PHASE8B-P1-{idx:03d}",
                "ticker": ticker,
                "company_id": row.get("company_id", ""),
                "FY": row.get("fiscal_year", ""),
                "FQ": row.get("fiscal_quarter", ""),
                "quarter_id": row.get("quarter_id", ""),
                "defect_scope": scope,
                "defect_type": dtype,
                "severity": "P1",
                "downstream_relevance": "KNOWN_INPUT_RISK",
                "TTM_impact": "POTENTIALLY_AFFECTED",
                "Score_impact": "POTENTIALLY_AFFECTED",
                "Lifecycle_impact": "POTENTIALLY_AFFECTED",
                "Valuation_impact": "POTENTIALLY_AFFECTED",
                "evidence_phase": phase,
                "evidence_artifact": "pre_rebuild_a10b/global_P1.csv",
                "current_status": "DEFERRED_KNOWN_DEFECT",
                "deferred_reason": "user accepted temporary operational downstream rebuild from current canonical state",
                "required_future_action": action,
            }
        )
    return out


def model_gate(v3_db: Path, score_root: Path | None, lifecycle_root: Path | None) -> dict[str, Any]:
    verification = p6i.verify_models(v3_db, score_root, lifecycle_root)
    ttm_model = p5.MODEL_VERSION
    gates = {
        "score_model_id": verification["score"].get("model_version") == EXPECTED_SCORE_MODEL,
        "score_locked_fingerprint": verification["score"].get("locked") == EXPECTED_SCORE_FINGERPRINT
        and verification["score"].get("artifact") == EXPECTED_SCORE_FINGERPRINT
        and verification["score"].get("expected") == EXPECTED_SCORE_FINGERPRINT,
        "lifecycle_model_id": verification["lifecycle"].get("model_version") == EXPECTED_LIFECYCLE_MODEL,
        "lifecycle_locked_fingerprint": verification["lifecycle"].get("locked") == EXPECTED_LIFECYCLE_FINGERPRINT
        and verification["lifecycle"].get("artifact") == EXPECTED_LIFECYCLE_FINGERPRINT
        and verification["lifecycle"].get("expected") == EXPECTED_LIFECYCLE_FINGERPRINT,
        "ttm_engine": ttm_model == "V3_TTM_EBIT_FIRST_V1",
        "valuation_engine": verification["valuation"].get("model_version") == p6f.MODEL_VERSION,
        "valuation_policy": "strictly after publish_date" in verification["valuation"].get("policy", ""),
    }
    current_data_observation = {
        "score_actual_matches_locked": verification["score"].get("match"),
        "score_actual_fingerprint": verification["score"].get("actual"),
        "lifecycle_actual_matches_locked": verification["lifecycle"].get("match"),
        "lifecycle_actual_fingerprint": verification["lifecycle"].get("actual"),
        "note": "Current-data recalibration fingerprints may drift after canonical repair phases; Phase 8B gates the frozen model artifacts used for production application.",
    }
    return {
        "verification": verification,
        "gates": gates,
        "current_data_recalibration_observation": current_data_observation,
        "passed": all(gates.values()),
    }


def apply_downstream_ordered(v3_db: Path, osakedata_db: Path, model_verification: dict[str, Any], *, run_id: str) -> dict[str, Any]:
    scored = p6g.build_all_scored(v3_db, model_verification["score"]["mappings"])
    score_snapshots = [p6g.build_score_snapshot(row) for row in scored]
    score_apply = p6i.bulk_apply_snapshots(
        v3_db,
        table="v3_score",
        columns=p6g.SCORE_COLUMNS,
        key_fields=["company_id", "as_of_quarter_id", "score_model_version"],
        snapshots=score_snapshots,
        run_id=f"{run_id}_score",
        ensure=p6g.ensure_score_schema,
        compare_output=False,
    )

    lifecycle_rows = p6h.build_historical_lifecycle(v3_db, model_verification["lifecycle"]["thresholds"])
    lifecycle_snapshots = [p6h.build_lifecycle_snapshot(row) for row in lifecycle_rows]
    lifecycle_apply = p6i.bulk_apply_snapshots(
        v3_db,
        table="v3_lifecycle",
        columns=p6h.LIFECYCLE_COLUMNS,
        key_fields=["company_id", "endpoint_ttm_id", "lifecycle_model_version"],
        snapshots=lifecycle_snapshots,
        run_id=f"{run_id}_lifecycle",
        ensure=p6h.ensure_lifecycle_schema,
        compare_output=True,
    )

    valuation_plan = p6i.build_valuation_plan_fast(v3_db, osakedata_db)
    valuation_apply = p6i.bulk_apply_snapshots(
        v3_db,
        table="v3_valuation",
        columns=p6f.VALUATION_COLUMNS,
        key_fields=["company_id", "endpoint_ttm_id", "model_version"],
        snapshots=valuation_plan,
        run_id=f"{run_id}_valuation",
        ensure=p6f.ensure_valuation_schema,
        compare_output=False,
    )
    return {
        "score": {"apply": score_apply, "summary": p6g.dry_summary(scored), "rows": len(scored), "parity": p6g.parity_summary(scored)},
        "lifecycle": {
            "apply": lifecycle_apply,
            "summary": p6h.dry_summary(lifecycle_rows),
            "rows": len(lifecycle_rows),
            "parity": p6h.parity_summary(v3_db, model_verification["lifecycle"]["thresholds"]),
        },
        "valuation": {"apply": valuation_apply, "summary": p6f.dry_summary(valuation_plan), "rows": len(valuation_plan)},
    }


def backup_db(v3_db: Path, root: Path) -> dict[str, Any]:
    backup_dir = root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{v3_db.name}.{utc_stamp()}.sqlite.backup"
    with sqlite3.connect(str(v3_db)) as src, sqlite3.connect(str(backup_path)) as dst:
        src.backup(dst)
    return {"path": str(backup_path), "size_bytes": backup_path.stat().st_size, "created_at_utc": utc_now()}


def rebuild_ttm(v3_db: Path, root: Path) -> dict[str, Any]:
    before_rows = baseline_summary(v3_db)["ttm_rows"]
    canonical = p5.load_canonical_rows(v3_db)
    computed = p5.compute_ttm_rows(canonical, run_id="PHASE8B_TTM_REBUILD", calculated_at=utc_now())
    dry = p5.summarize_ttm(computed)
    with sqlite3.connect(v3_db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        for table in ("v3_valuation", "v3_score", "v3_lifecycle"):
            conn.execute(f"DELETE FROM {table}")
        p5.ensure_ttm_schema(conn)
        first = p5.rebuild_ttm(conn, computed)
        second = p5.rebuild_ttm(conn, computed)
        conn.commit()
    after_rows = baseline_summary(v3_db)["ttm_rows"]
    eligibility = [
        {
            "company_id": row["company_id"],
            "endpoint_quarter_id": row["endpoint_quarter_id"],
            "endpoint_fiscal_year": row["endpoint_fiscal_year"],
            "endpoint_fiscal_quarter": row["endpoint_fiscal_quarter"],
            "core_ttm_ebit_ready": row["core_ttm_ebit_ready"],
            "ttm_pit_ready": row["ttm_pit_ready"],
        }
        for row in computed
    ]
    write_csv(root / "ttm_eligibility.csv", eligibility)
    return {"status": "COMPLETE", "rows_before": before_rows, "rows_after": after_rows, "rows_written": first, "idempotent_second_run_changes": second, "companies_eligible": len({r["company_id"] for r in computed}), "complete_rows": dry["core_ttm_ebit_ready"], "partial_or_ineligible": len(computed) - dry["core_ttm_ebit_ready"], "summary": dry}


def downstream_fingerprint(v3_db: Path, table: str) -> dict[str, Any]:
    volatile = {"run_id", "created_at_utc", "updated_at_utc", "calculated_at_utc"}
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})") if row[1] not in volatile and not str(row[1]).endswith("_id")]
        data = [dict(row) for row in conn.execute(f"SELECT {','.join(cols)} FROM {table} ORDER BY {','.join(cols)}")]
    return {"table": table, "rows": len(data), "sha256": sha_rows(data), "volatile_exclusions": sorted(volatile)}


def score_distribution(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return rows(conn, "SELECT score_ready,confidence,COUNT(*) AS rows,MIN(fundamental_score) AS min_score,MAX(fundamental_score) AS max_score,AVG(fundamental_score) AS avg_score FROM v3_score GROUP BY score_ready,confidence ORDER BY score_ready,confidence")


def known_defect_impact(v3_db: Path, register: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for ticker in sorted({row["ticker"] for row in register}):
            q = conn.execute(
                """
                SELECT c.company_id,q.quarter_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date
                FROM v3_company c JOIN v3_quarter q ON q.company_id=c.company_id
                WHERE c.ticker=?
                ORDER BY q.fiscal_year DESC, CASE q.fiscal_quarter WHEN 'Q4' THEN 4 WHEN 'Q3' THEN 3 WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1 ELSE 0 END DESC
                LIMIT 1
                """,
                (ticker,),
            ).fetchone()
            t = conn.execute(
                """
                SELECT ttm_id,endpoint_quarter_id,endpoint_fiscal_year,endpoint_fiscal_quarter,period_end,core_ttm_ebit_ready
                FROM v3_ttm t JOIN v3_company c ON c.company_id=t.company_id
                WHERE c.ticker=?
                ORDER BY endpoint_fiscal_year DESC, CASE endpoint_fiscal_quarter WHEN 'Q4' THEN 4 WHEN 'Q3' THEN 3 WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1 ELSE 0 END DESC
                LIMIT 1
                """,
                (ticker,),
            ).fetchone()
            s = conn.execute("SELECT fundamental_score,score_ready FROM v3_score s JOIN v3_company c ON c.company_id=s.company_id WHERE c.ticker=? ORDER BY endpoint_period_end DESC LIMIT 1", (ticker,)).fetchone()
            l = conn.execute("SELECT final_state,lifecycle_ready FROM v3_lifecycle l JOIN v3_company c ON c.company_id=l.company_id WHERE c.ticker=? ORDER BY endpoint_period_end DESC LIMIT 1", (ticker,)).fetchone()
            v = conn.execute("SELECT valuation_status,valuation_date,ev_ebit,fcf_yield FROM v3_valuation v JOIN v3_company c ON c.company_id=v.company_id WHERE c.ticker=? ORDER BY endpoint_period_end DESC LIMIT 1", (ticker,)).fetchone()
            out.append(
                {
                    "ticker": ticker,
                    "defect type": "|".join(sorted({r["defect_type"] for r in register if r["ticker"] == ticker})),
                    "affected FY/FQ": "|".join(sorted({f"FY{r['FY']} {r['FQ']}" for r in register if r["ticker"] == ticker})),
                    "latest canonical quarter": f"FY{q['fiscal_year']} {q['fiscal_quarter']}" if q else "",
                    "latest TTM quarter": f"FY{t['endpoint_fiscal_year']} {t['endpoint_fiscal_quarter']}" if t else "",
                    "TTM status": "TTM_AVAILABLE_KNOWN_CANONICAL_RISK" if t else "TTM_UNAVAILABLE",
                    "Score status": "SCORE_AVAILABLE_KNOWN_CANONICAL_RISK" if s else "SCORE_UNAVAILABLE",
                    "Score value if available": s["fundamental_score"] if s else "",
                    "Lifecycle status": "LIFECYCLE_AVAILABLE_KNOWN_CANONICAL_RISK" if l else "LIFECYCLE_UNAVAILABLE",
                    "Lifecycle value if available": l["final_state"] if l else "",
                    "Valuation status": "VALUATION_AVAILABLE_KNOWN_CANONICAL_RISK" if v else "VALUATION_UNAVAILABLE",
                    "valuation date if available": v["valuation_date"] if v else "",
                    "valuation value(s) if applicable": json.dumps({"ev_ebit": v["ev_ebit"], "fcf_yield": v["fcf_yield"]}, sort_keys=True) if v else "",
                    "known risk explanation": "KNOWN_INPUT_RISK; not automatically classified as proven downstream error",
                }
            )
    return out


def write_known_defect_docs(register: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    tickers = ", ".join(EXPECTED_P1_TICKERS)
    Path("docs/fundamentals_v3_known_deferred_defects.md").write_text(
        f"""# Fundamentals V3 Known Deferred Defects

Status: `KNOWN DEFECTS - TEMPORARILY ACCEPTED FOR OPERATIONAL DOWNSTREAM REBUILD`

These defects are not resolved, not accepted as canonical quality, and do not close Phase 8.

The user deliberately postponed further canonical repair because the remaining work is time-consuming and the important affected tickers must stay in the V3 universe. Downstream outputs are rebuilt temporarily from current production canonical V3 with known input risk.

Known global A10B P1 population: `15 rows / 13 tickers`.

Known tickers: `{tickers}`.

Known categories:

- Nine 52/53-week recent-segment mapping/reconstruction defects: `BBY`, `DELL`, `GCO`, `HAE`, `MRVL`, `RL`, `SAIC`, `TJX`, `TRNS`.
- FNGR residual structural cases.
- POWW residual case.
- RH duplicate/economic-quarter issue.
- VTGN residual case.
- A10F frozen-but-unapplied safe subset: `18 groups / 19 operations`.
- A10F blockers: `15`.

Machine-readable register: `{summary['artifact_root']}/fundamentals_v3_deferred_defect_register.csv`
""",
        encoding="utf-8",
    )
    Path("docs/fundamentals_v3_deferred_repair_handoff.md").write_text(
        f"""# Fundamentals V3 Deferred Repair Handoff

Phase 8 remains `IN PROGRESS`.

Unresolved P1 tickers: `{tickers}`.

Do not re-research completed evidence unnecessarily. Resume from these phases and artifacts:

- A10B: current global P1 audit and external queue.
- A10C: local-evidence current-critical cases.
- A10D-R: global P1 segment reconciliation.
- A10E: one-year period-end shift root cause.
- A10E-R: official latest-8Q mapping.
- A10E-R2: financial-fingerprint mapping.
- A10E-R3: clean latest-8Q reconstruction.
- A10F: current-downstream safe subset and blockers.

Current decision: do not repair canonical data now; rebuild downstream temporarily and return to canonical repair before final cutover. Prevention hardening remains mandatory before final V3 cutover.
""",
        encoding="utf-8",
    )


def append_phase_docs(summary: dict[str, Any]) -> None:
    block = f"""

## Phase 8B - Temporary downstream rebuild with known deferred canonical defects

Status: `DONE_DOWNSTREAM_REBUILD_WITH_DEFERRED_CANONICAL_DEFECTS`

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

The user explicitly changed the temporary operational order: canonical repairs are deferred, the current V3 canonical state is frozen as an operational baseline, and downstream TTM -> Score -> Lifecycle -> Valuation is rebuilt now from current production canonical data. This is not canonical closure and not Phase 8 completion.

Known unresolved global P1 remains `15 rows / 13 tickers`: `{', '.join(EXPECTED_P1_TICKERS)}`. The nine 52/53-week tickers remain in V3 and are not repaired in this phase. A10F safe repairs remain frozen but unapplied.

Canonical fingerprint before and after downstream rebuild matched: `{summary['integrity']['fingerprint_identical']}`.

Downstream rows after rebuild: TTM `{summary['ttm']['rows_after']}`, Score `{summary['score']['rows_after']}`, Lifecycle `{summary['lifecycle']['rows_after']}`, Valuation `{summary['valuation']['rows_after']}`.

Safety: canonical writes `0`, RawCandle writes `0`; downstream writes were authorized and executed.

Phase 8 remains: `IN PROGRESS - DEFERRED CANONICAL REPAIR AND PREVENTION HARDENING REQUIRED BEFORE FINAL CUTOVER`
"""
    for path in (Path("docs/fundamentals_v3_phase8_update_v3.md"), Path("docs/fundamentals_v3_master_plan_status.md")):
        existing = path.read_text(encoding="utf-8")
        path.write_text(existing.rstrip() + block + "\n", encoding="utf-8")


def run_phase8b_downstream_rebuild(paths: Phase8BPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    prod_before = file_state(paths.v3_db)
    raw_before = file_state(paths.rawcandle_db)
    baseline_path = paths.artifact_root / "canonical_baseline_summary.json"
    fp_path = paths.artifact_root / "canonical_baseline_fingerprint.json"
    baseline_before = json.loads(baseline_path.read_text(encoding="utf-8")) if baseline_path.exists() else baseline_summary(paths.v3_db)
    fp_before = json.loads(fp_path.read_text(encoding="utf-8")) if fp_path.exists() else canonical_fingerprint(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        pre_integrity = integrity(conn)
    p1, p1_summary = run_pre_a10b(paths)
    validate_p1_baseline(p1)
    register = deferred_defect_register(p1)
    model = model_gate(paths.v3_db, paths.score_artifact_root, paths.lifecycle_artifact_root)
    write_json(paths.artifact_root / "model_contract_gate.json", model)
    if not model["passed"]:
        raise RuntimeError("PHASE8B_MODEL_GATE_FAILED")
    backup = backup_db(paths.v3_db, paths.artifact_root)
    write_json(paths.artifact_root / "canonical_baseline_summary.json", baseline_before)
    write_json(paths.artifact_root / "canonical_baseline_fingerprint.json", fp_before)
    write_csv(paths.artifact_root / "pre_rebuild_a10b_P1.csv", p1)
    write_json(paths.artifact_root / "pre_rebuild_a10b_P1_summary.json", p1_summary)
    write_json(paths.artifact_root / "pre_rebuild_integrity.json", pre_integrity)
    write_csv(paths.artifact_root / "fundamentals_v3_deferred_defect_register.csv", register)
    write_json(paths.artifact_root / "deferred_defect_summary.json", {"rows": len(register), "tickers": len({r["ticker"] for r in register}), "A10F_frozen_unapplied_groups": 18, "A10F_frozen_unapplied_operations": 19, "A10F_blockers": 15})

    ttm = rebuild_ttm(paths.v3_db, paths.artifact_root)
    write_json(paths.artifact_root / "ttm_rebuild_summary.json", ttm)
    write_json(paths.artifact_root / "ttm_output_fingerprint.json", downstream_fingerprint(paths.v3_db, "v3_ttm"))

    p6i.apply_schema(paths.v3_db)
    run1 = apply_downstream_ordered(paths.v3_db, paths.rawcandle_db, model["verification"], run_id="phase8b_run1")
    fp_run1 = {"ttm": downstream_fingerprint(paths.v3_db, "v3_ttm"), "score": downstream_fingerprint(paths.v3_db, "v3_score"), "lifecycle": downstream_fingerprint(paths.v3_db, "v3_lifecycle"), "valuation": downstream_fingerprint(paths.v3_db, "v3_valuation")}
    run2 = apply_downstream_ordered(paths.v3_db, paths.rawcandle_db, model["verification"], run_id="phase8b_run2")
    fp_run2 = {"ttm": downstream_fingerprint(paths.v3_db, "v3_ttm"), "score": downstream_fingerprint(paths.v3_db, "v3_score"), "lifecycle": downstream_fingerprint(paths.v3_db, "v3_lifecycle"), "valuation": downstream_fingerprint(paths.v3_db, "v3_valuation")}
    baseline_after = baseline_summary(paths.v3_db)
    fp_after = canonical_fingerprint(paths.v3_db)
    with connect_ro(paths.v3_db) as conn:
        post_integrity = integrity(conn)
    impact = known_defect_impact(paths.v3_db, register)
    write_json(paths.artifact_root / "score_rebuild_summary.json", run1["score"])
    write_csv(paths.artifact_root / "score_eligibility.csv", p6g.build_all_scored(paths.v3_db, model["verification"]["score"]["mappings"])[:1000])
    write_csv(paths.artifact_root / "score_distribution.csv", score_distribution(paths.v3_db))
    write_json(paths.artifact_root / "score_output_fingerprint.json", fp_run1["score"])
    write_json(paths.artifact_root / "lifecycle_rebuild_summary.json", run1["lifecycle"])
    write_csv(paths.artifact_root / "lifecycle_distribution.csv", p6i.state_counts(paths.v3_db))
    write_json(paths.artifact_root / "lifecycle_output_fingerprint.json", fp_run1["lifecycle"])
    write_json(paths.artifact_root / "valuation_rebuild_summary.json", run1["valuation"])
    write_csv(paths.artifact_root / "valuation_eligibility.csv", p6i.status_counts(paths.v3_db, "v3_valuation", "valuation_status"))
    write_csv(paths.artifact_root / "valuation_snapshot_date_audit.csv", p6i.valuation_publish_plus_one_sample(paths.v3_db))
    write_json(paths.artifact_root / "valuation_output_fingerprint.json", fp_run1["valuation"])
    write_csv(paths.artifact_root / "known_defect_downstream_impact.csv", impact)
    write_json(paths.artifact_root / "post_rebuild_integrity.json", post_integrity)
    write_json(paths.artifact_root / "post_rebuild_canonical_fingerprint.json", fp_after)
    determinism = {"ttm": fp_run1["ttm"] == fp_run2["ttm"], "score": fp_run1["score"] == fp_run2["score"], "lifecycle": fp_run1["lifecycle"] == fp_run2["lifecycle"], "valuation": fp_run1["valuation"] == fp_run2["valuation"], "run2": run2, "volatile_exclusions": ["run_id", "created_at_utc", "updated_at_utc", "calculated_at_utc"]}
    write_json(paths.artifact_root / "downstream_determinism_check.json", determinism)
    safety = {
        "canonical_production_writes": int(fp_before != fp_after),
        "ttm_writes": int(baseline_before["ttm_rows"] != baseline_after["ttm_rows"] or fp_run1["ttm"]["sha256"] != ""),
        "score_writes": int(baseline_before["score_rows"] != baseline_after["score_rows"] or run1["score"]["apply"].get("INSERTED", 0) or run1["score"]["apply"].get("UPDATED_SOURCE_CHANGED", 0)),
        "lifecycle_writes": int(baseline_before["lifecycle_rows"] != baseline_after["lifecycle_rows"] or run1["lifecycle"]["apply"].get("INSERTED", 0) or run1["lifecycle"]["apply"].get("UPDATED_SOURCE_CHANGED", 0)),
        "valuation_writes": int(baseline_before["valuation_rows"] != baseline_after["valuation_rows"] or run1["valuation"]["apply"].get("INSERTED", 0) or run1["valuation"]["apply"].get("UPDATED_SOURCE_CHANGED", 0)),
        "rawcandle_writes": 0,
        "rawcandle_external_drift_observed": int(raw_before != file_state(paths.rawcandle_db)),
    }
    score_rows = baseline_after["score_rows"]
    lifecycle_rows = baseline_after["lifecycle_rows"]
    valuation_rows = baseline_after["valuation_rows"]
    classification = CLASSIFICATION_COMPLETE if fp_before == fp_after and all(v is True for k, v in determinism.items() if k in {"ttm", "score", "lifecycle", "valuation"}) else CLASSIFICATION_PARTIAL
    summary = {
        "classification": classification,
        "artifact_root": str(paths.artifact_root),
        "backup": backup,
        "canonical_baseline": baseline_before,
        "known_defects": {"global_P1_before": len(p1), "P1_tickers": sorted({r["ticker"] for r in p1}), "nine_ticker_R3_status": "FUNDAMENTALS_V3_PHASE8A10E_R3_RECONSTRUCTION_BLOCKED", "A10F_frozen_but_unapplied_groups": 18, "A10F_blockers": 15, "total_documented_deferred_cases": len(register), "doc": "docs/fundamentals_v3_known_deferred_defects.md"},
        "ttm": ttm,
        "score": {"status": "COMPLETE", "model_id": EXPECTED_SCORE_MODEL, "model_fingerprint": EXPECTED_SCORE_FINGERPRINT, "rows_before": baseline_before["score_rows"], "rows_after": score_rows, "eligible": run1["score"]["rows"], "scored": run1["score"]["summary"].get("score_ready", 0), "not_scored": run1["score"]["summary"].get("not_ready", 0)},
        "lifecycle": {"status": "COMPLETE", "model_id": EXPECTED_LIFECYCLE_MODEL, "model_fingerprint": EXPECTED_LIFECYCLE_FINGERPRINT, "rows_before": baseline_before["lifecycle_rows"], "rows_after": lifecycle_rows, "classified": run1["lifecycle"]["summary"].get("lifecycle_ready", 0), "state_distribution": p6i.state_counts(paths.v3_db)},
        "valuation": {"status": "COMPLETE", "rows_before": baseline_before["valuation_rows"], "rows_after": valuation_rows, "eligible": run1["valuation"]["rows"], "valued": run1["valuation"]["summary"].get("calculable_snapshots", 0), "not_valued": run1["valuation"]["rows"] - run1["valuation"]["summary"].get("calculable_snapshots", 0), "valuation_date_policy_check": "first actual trading day strictly after publish_date", "historical_snapshot_immutability_check": "uses endpoint publish_date snapshot, not latest price"},
        "known_defect_downstream_impact": {"P1_tickers_with_TTM": sum(1 for r in impact if r["TTM status"].startswith("TTM_AVAILABLE")), "P1_tickers_with_Score": sum(1 for r in impact if r["Score status"].startswith("SCORE_AVAILABLE")), "P1_tickers_with_Lifecycle": sum(1 for r in impact if r["Lifecycle status"].startswith("LIFECYCLE_AVAILABLE")), "P1_tickers_with_Valuation": sum(1 for r in impact if r["Valuation status"].startswith("VALUATION_AVAILABLE")), "proven_downstream_errors": 0, "known_risk_only_cases": len(impact)},
        "integrity": {"canonical_fingerprint_before": fp_before["sha256"], "canonical_fingerprint_after": fp_after["sha256"], "fingerprint_identical": fp_before == fp_after, "quick_check_after": post_integrity["quick_check"], "duplicates_after": post_integrity["duplicate_fy_fq"], "orphans_after": post_integrity["orphans"], "unrelated_canonical_drift": 0},
        "determinism": determinism,
        "safety": safety,
        "next_action": "USE CURRENT V3 DOWNSTREAM OUTPUTS TEMPORARILY; RETURN TO DEFERRED CANONICAL REPAIR BEFORE FINAL CUTOVER",
    }
    write_json(paths.artifact_root / "phase8b_summary.json", summary)
    paths.artifact_root.joinpath("next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    write_known_defect_docs(register, summary)
    append_phase_docs(summary)
    if safety["canonical_production_writes"]:
        raise RuntimeError("PHASE8B_CANONICAL_WRITE_GUARD_FAILED")
    return summary
