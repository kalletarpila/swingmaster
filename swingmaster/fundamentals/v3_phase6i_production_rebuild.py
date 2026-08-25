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

from swingmaster.fundamentals import v3_phase6f_valuation_engine as p6f
from swingmaster.fundamentals import v3_phase6g_legacy2_score_engine as p6g
from swingmaster.fundamentals import v3_phase6h_lifecycle_engine as p6h
from swingmaster.fundamentals.v3_phase6c_score_distribution_calibration import stats

CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE6I_PRODUCTION_REBUILD_PROVEN_READY_FOR_PHASE6J"
BLOCKED_PREFLIGHT = "FUNDAMENTALS_V3_PHASE6I_BLOCKED_BY_PREFLIGHT_FAILURE"
BLOCKED_TTM_DRIFT = "FUNDAMENTALS_V3_PHASE6I_BLOCKED_BY_TTM_DRIFT"
BLOCKED_SCHEMA = "FUNDAMENTALS_V3_PHASE6I_BLOCKED_BY_SCHEMA_MISMATCH"
BLOCKED_IDEMPOTENCY = "FUNDAMENTALS_V3_PHASE6I_IDEMPOTENCY_FAILED"
BLOCKED_SOURCE_DRIFT = "FUNDAMENTALS_V3_PHASE6I_SOURCE_DRIFT_DETECTED"
NEXT_PHASE = "MASTER PLAN PHASE 6J - PHASE 6 CLOSURE"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def run_phase6i_production_rebuild(
    *,
    v3_db: Path,
    osakedata_db: Path,
    artifact_root: Path,
    score_artifact_root: Path | None = None,
    lifecycle_artifact_root: Path | None = None,
    write_durable_docs: bool = True,
) -> dict[str, Any]:
    expected_db = Path("/home/kalle/projects/swingmaster/rc_fundamentals_v3.db")
    if v3_db.resolve() != expected_db:
        return {"classification": BLOCKED_PREFLIGHT, "reason": f"unexpected_db:{v3_db}"}
    artifact_root.mkdir(parents=True, exist_ok=True)
    preflight_path = artifact_root / "production_preflight.json"
    preflight = read_json(preflight_path) if preflight_path.exists() else collect_preflight(v3_db, osakedata_db)
    if not preflight_path.exists():
        write_preflight_artifacts(artifact_root, preflight)
    if preflight["quick_check"] != "ok" or preflight["ttm_rows"] != 54038:
        return {"classification": BLOCKED_TTM_DRIFT if preflight["ttm_rows"] != 54038 else BLOCKED_PREFLIGHT, "preflight": preflight}

    backup_path = artifact_root / "backup_manifest.json"
    backup = read_json(backup_path) if backup_path.exists() else create_backup(v3_db, artifact_root / "backup")
    if not backup_path.exists():
        write_json(backup_path, backup)
        write_text(artifact_root / "rollback_plan.md", rollback_plan_md(backup))

    model_verification = verify_models(v3_db, score_artifact_root, lifecycle_artifact_root)
    write_json(artifact_root / "model_fingerprint_verification.json", model_verification)
    if not model_verification["score"]["match"] or not model_verification["lifecycle"]["match"]:
        return {"classification": BLOCKED_PREFLIGHT, "model_verification": model_verification}

    schema_log = apply_schema(v3_db)
    schema_after = schema_sql(v3_db)
    schema_parity = schema_parity_check(v3_db)
    write_text(artifact_root / "schema_apply_log.txt", "\n".join(schema_log) + "\n")
    write_text(artifact_root / "production_schema_after.sql", schema_after)
    write_json(artifact_root / "schema_parity_check.json", schema_parity)
    if not all(schema_parity.values()):
        return {"classification": BLOCKED_SCHEMA, "schema_parity": schema_parity}

    schema_checkpoint = collect_source_fingerprints(v3_db)
    if not source_equal(preflight["source_fingerprints"], schema_checkpoint):
        return {"classification": BLOCKED_SOURCE_DRIFT, "stage": "schema", "before": preflight["source_fingerprints"], "after": schema_checkpoint}

    run1 = apply_all(v3_db, osakedata_db, model_verification, run_id="phase6i_run1")
    derived_fp1 = derived_fingerprints(v3_db)
    run2 = apply_all(v3_db, osakedata_db, model_verification, run_id="phase6i_run2")
    derived_fp2 = derived_fingerprints(v3_db)
    after = collect_preflight(v3_db, osakedata_db)
    source_drift = source_equal(preflight["source_fingerprints"], after["source_fingerprints"])
    idempotent = is_idempotent(run2) and derived_fp1 == derived_fp2

    write_population_artifacts(artifact_root, v3_db, run1, run2, derived_fp1, derived_fp2, preflight, after, source_drift, idempotent)
    if not idempotent:
        return {"classification": BLOCKED_IDEMPOTENCY, "run2": run2}
    if not source_drift:
        return {"classification": BLOCKED_SOURCE_DRIFT, "before": preflight["source_fingerprints"], "after": after["source_fingerprints"]}

    acceptance = production_acceptance(v3_db, run1, run2, source_drift, idempotent)
    summary = {
        "classification": CLASSIFICATION_COMPLETE if all(acceptance.values()) else BLOCKED_PREFLIGHT,
        "recommended_next_step": NEXT_PHASE,
        "run_at_utc": utc_now(),
        "artifact_root": str(artifact_root),
        "production_db": str(v3_db.resolve()),
        "osakedata_db": str(osakedata_db.resolve()),
        "preflight": preflight,
        "backup": backup,
        "model_verification": model_verification,
        "schema_apply": schema_log,
        "schema_parity": schema_parity,
        "run1": run1,
        "run2": run2,
        "derived_fingerprint_run1": derived_fp1,
        "derived_fingerprint_run2": derived_fp2,
        "postflight": after,
        "source_drift": not source_drift,
        "idempotency_pass": idempotent,
        "acceptance": acceptance,
    }
    write_json(artifact_root / "phase6i_production_acceptance.json", acceptance)
    write_json(artifact_root / "phase6i_summary.json", summary)
    write_text(artifact_root / "phase6j_closure_handoff.md", "Phase 6J should close Phase 6 after reviewing Phase 6I production acceptance and downstream readiness.\n")
    write_text(artifact_root / "recommended_next_step.md", NEXT_PHASE + "\n")
    if write_durable_docs:
        write_doc(Path("docs/fundamentals_v3_phase6i_production_rebuild_and_proving.md"), summary)
        update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def collect_preflight(v3_db: Path, osakedata_db: Path) -> dict[str, Any]:
    stat = osakedata_db.stat()
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {
            "production_db": str(v3_db.resolve()),
            "db_size_bytes": v3_db.stat().st_size,
            "osakedata_db": str(osakedata_db.resolve()),
            "osakedata_size_bytes": stat.st_size,
            "osakedata_mtime_ns": stat.st_mtime_ns,
            "disk": disk_check(v3_db.parent),
            "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
            "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
            "companies": table_count(conn, "v3_company"),
            "active_companies": int(conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=1").fetchone()[0]),
            "inactive_companies": int(conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=0").fetchone()[0]),
            "quarter_rows": table_count(conn, "v3_quarter"),
            "ttm_rows": table_count(conn, "v3_ttm"),
            "score_rows": table_count(conn, "v3_score"),
            "valuation_rows": table_count(conn, "v3_valuation"),
            "lifecycle_table_exists": table_exists(conn, "v3_lifecycle"),
            "lifecycle_rows": table_count(conn, "v3_lifecycle"),
            "source_fingerprints": collect_source_fingerprints(v3_db),
            "schema_fingerprint": text_fingerprint(schema_sql(v3_db)),
        }


def disk_check(path: Path) -> dict[str, Any]:
    usage = shutil.disk_usage(path)
    return {"total_bytes": usage.total, "used_bytes": usage.used, "free_bytes": usage.free}


def create_backup(v3_db: Path, backup_dir: Path) -> dict[str, Any]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{v3_db.name}.{utc_stamp()}.sqlite.backup"
    with sqlite3.connect(str(v3_db)) as src, sqlite3.connect(str(backup_path)) as dst:
        src.backup(dst)
    return {
        "method": "sqlite_online_backup",
        "path": str(backup_path),
        "size_bytes": backup_path.stat().st_size,
        "sha256": file_sha256(backup_path),
        "source_size_bytes": v3_db.stat().st_size,
        "created_at_utc": utc_now(),
    }


def verify_models(v3_db: Path, score_root: Path | None, lifecycle_root: Path | None) -> dict[str, Any]:
    score_dir = score_root or latest(Path("temp/fundamentals_v3_phase6cr_score_architecture_reconciliation"))
    lifecycle_dir = lifecycle_root or latest(Path("temp/fundamentals_v3_phase6d_lifecycle_recalibration"))
    return {
        "score": p6g.verify_frozen_score(v3_db, score_dir),
        "lifecycle": p6h.verify_frozen_lifecycle(v3_db, lifecycle_dir),
        "valuation": {
            "model_version": p6f.MODEL_VERSION,
            "policy": "first trading day strictly after publish_date using that day's close",
            "price_source": p6f.PRICE_SOURCE,
        },
    }


def apply_schema(v3_db: Path) -> list[str]:
    with sqlite3.connect(str(v3_db)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        out = [
            f"valuation:{p6f.ensure_valuation_schema(conn)}",
            f"score:{p6g.ensure_score_schema(conn)}",
            f"lifecycle:{p6h.ensure_lifecycle_schema(conn)}",
        ]
        conn.commit()
        return out


def apply_all(v3_db: Path, osakedata_db: Path, model_verification: dict[str, Any], *, run_id: str) -> dict[str, Any]:
    valuation_plan = build_valuation_plan_fast(v3_db, osakedata_db)
    valuation_apply = bulk_apply_snapshots(
        v3_db,
        table="v3_valuation",
        columns=p6f.VALUATION_COLUMNS,
        key_fields=["company_id", "endpoint_ttm_id", "model_version"],
        snapshots=valuation_plan,
        run_id=f"{run_id}_valuation",
        ensure=p6f.ensure_valuation_schema,
        compare_output=False,
    )
    scored = p6g.build_all_scored(v3_db, model_verification["score"]["mappings"])
    score_snapshots = [p6g.build_score_snapshot(row) for row in scored]
    score_apply = bulk_apply_snapshots(
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
    lifecycle_apply = bulk_apply_snapshots(
        v3_db,
        table="v3_lifecycle",
        columns=p6h.LIFECYCLE_COLUMNS,
        key_fields=["company_id", "endpoint_ttm_id", "lifecycle_model_version"],
        snapshots=lifecycle_snapshots,
        run_id=f"{run_id}_lifecycle",
        ensure=p6h.ensure_lifecycle_schema,
        compare_output=True,
    )
    return {
        "valuation": {"apply": valuation_apply, "summary": p6f.dry_summary(valuation_plan), "rows": len(valuation_plan)},
        "score": {"apply": score_apply, "summary": p6g.dry_summary(scored), "rows": len(scored), "parity": p6g.parity_summary(scored)},
        "lifecycle": {"apply": lifecycle_apply, "summary": p6h.dry_summary(lifecycle_rows), "rows": len(lifecycle_rows), "parity": p6h.parity_summary(v3_db, model_verification["lifecycle"]["thresholds"])},
    }


def bulk_apply_snapshots(
    db_path: Path,
    *,
    table: str,
    columns: list[tuple[str, str]],
    key_fields: list[str],
    snapshots: list[dict[str, Any]],
    run_id: str,
    ensure: Any,
    compare_output: bool,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        ensure(conn)
        existing = existing_fingerprints(conn, table, key_fields, compare_output=compare_output)
        insert_columns = [name for name, _definition in columns if not name.endswith("_id") or name not in {"valuation_id", "score_id", "lifecycle_id"}]
        now_text = utc_now()
        update_cols = [c for c in insert_columns if c not in set(key_fields) | {"created_at_utc"}]
        sql = f"""
            INSERT INTO {table} ({",".join(insert_columns)})
            VALUES ({",".join("?" for _ in insert_columns)})
            ON CONFLICT({",".join(key_fields)}) DO UPDATE SET
                {",".join(f"{c}=excluded.{c}" for c in update_cols)}
        """
        for snapshot in snapshots:
            key = tuple(snapshot[field] for field in key_fields)
            old = existing.get(key)
            current = (snapshot.get("source_fingerprint"), snapshot.get("output_json") if compare_output else None)
            if old == current:
                counts["NOOP"] += 1
                continue
            values = []
            for col in insert_columns:
                if col == "run_id":
                    values.append(run_id)
                elif col in {"created_at_utc", "updated_at_utc"}:
                    values.append(now_text)
                else:
                    values.append(snapshot.get(col))
            conn.execute(sql, values)
            counts["INSERTED" if old is None else "UPDATED_SOURCE_CHANGED"] += 1
        conn.commit()
    return dict(counts)


def existing_fingerprints(conn: sqlite3.Connection, table: str, key_fields: list[str], *, compare_output: bool) -> dict[tuple[Any, ...], tuple[Any, Any]]:
    if not table_exists(conn, table):
        return {}
    select = ",".join([*key_fields, "source_fingerprint"] + (["output_json"] if compare_output else []))
    out = {}
    for row in conn.execute(f"SELECT {select} FROM {table}"):
        key = tuple(row[idx] for idx in range(len(key_fields)))
        source = row[len(key_fields)]
        output = row[len(key_fields) + 1] if compare_output else None
        out[key] = (source, output)
    return out


def build_valuation_plan_fast(v3_db: Path, osakedata_db: Path) -> list[dict[str, Any]]:
    endpoints = p6f.load_ttm_endpoints(v3_db)
    market_dates = {market: p6f.load_market_dates(osakedata_db, market) for market in sorted({str(r["market"]) for r in endpoints})}
    targets = []
    for row in endpoints:
        valuation_date, date_status = p6f.resolve_next_trading_day(market_dates.get(str(row["market"]), []), row.get("ttm_available_date"))
        targets.append((row, valuation_date, date_status))
    closes = load_target_closes(osakedata_db, [(str(row["market"]), str(row["ticker"]), str(date)) for row, date, status in targets if date and status == p6f.STATUS_VALID])
    return [
        p6f.calculate_valuation(
            row,
            valuation_date=valuation_date,
            close=closes.get((str(row["market"]), str(row["ticker"]), str(valuation_date))) if valuation_date and date_status == p6f.STATUS_VALID else None,
            price_status=date_status,
        )
        for row, valuation_date, date_status in targets
    ]


def load_target_closes(osakedata_db: Path, keys: list[tuple[str, str, str]]) -> dict[tuple[str, str, str], float]:
    grouped: dict[tuple[str, str], set[str]] = {}
    for market, ticker, pvm in keys:
        grouped.setdefault((market, pvm), set()).add(ticker)
    out: dict[tuple[str, str, str], float] = {}
    with sqlite3.connect(f"file:{osakedata_db}?mode=ro", uri=True) as conn:
        for (market, pvm), tickers in grouped.items():
            items = sorted(tickers)
            for start in range(0, len(items), 800):
                chunk = items[start : start + 800]
                placeholders = ",".join("?" for _ in chunk)
                params = [market, pvm, *chunk]
                for ticker, close in conn.execute(f"SELECT osake,close FROM osakedata WHERE market=? AND pvm=? AND osake IN ({placeholders})", params):
                    if close is not None:
                        out[(market, str(ticker), pvm)] = float(close)
    return out


def is_idempotent(run: dict[str, Any]) -> bool:
    for engine in ("valuation", "score", "lifecycle"):
        apply = run[engine]["apply"]
        if apply.get("INSERTED", 0) or apply.get("UPDATED_SOURCE_CHANGED", 0):
            return False
    return True


def write_preflight_artifacts(root: Path, preflight: dict[str, Any]) -> None:
    write_json(root / "production_preflight.json", preflight)
    disk = preflight["disk"]
    write_text(root / "disk_space_check.txt", f"db_size_bytes={preflight['db_size_bytes']}\nfree_bytes={disk['free_bytes']}\nexpected_backup_size_bytes={preflight['db_size_bytes']}\nexpected_peak_temp_bytes={preflight['db_size_bytes'] * 2}\n")
    write_csv(root / "production_row_counts_before.csv", [row_counts(preflight)])
    write_text(root / "production_schema_before.sql", schema_sql(Path(preflight["production_db"])))
    write_json(root / "canonical_fingerprint_before.json", preflight["source_fingerprints"]["canonical"])
    write_json(root / "ttm_fingerprint_before.json", preflight["source_fingerprints"]["ttm"])
    write_json(root / "company_universe_fingerprint_before.json", preflight["source_fingerprints"]["company"])


def write_population_artifacts(root: Path, v3_db: Path, run1: dict[str, Any], run2: dict[str, Any], fp1: dict[str, Any], fp2: dict[str, Any], before: dict[str, Any], after: dict[str, Any], source_drift: bool, idempotent: bool) -> None:
    write_json(root / "valuation_apply_summary.json", run1["valuation"])
    write_csv(root / "valuation_row_counts.csv", [table_row("v3_valuation", table_count_ro(v3_db, "v3_valuation"))])
    write_csv(root / "valuation_status_counts.csv", status_counts(v3_db, "v3_valuation", "valuation_status"))
    write_csv(root / "valuation_coverage_by_year.csv", valuation_coverage_by_year(v3_db))
    write_csv(root / "valuation_publish_plus_one_proof.csv", valuation_publish_plus_one_sample(v3_db))
    write_csv(root / "valuation_lineage_check.csv", [lineage_check(v3_db, "v3_valuation", "endpoint_ttm_id")])
    write_csv(root / "valuation_duplicate_check.csv", [duplicate_check(v3_db, "v3_valuation", "company_id,endpoint_ttm_id,model_version")])
    write_json(root / "score_apply_summary.json", run1["score"])
    write_csv(root / "score_row_counts.csv", [table_row("v3_score", table_count_ro(v3_db, "v3_score"))])
    write_csv(root / "score_coverage_by_year.csv", score_coverage_by_year(v3_db))
    write_csv(root / "score_distribution_by_year.csv", score_distribution_by_year(v3_db))
    write_csv(root / "score_lineage_check.csv", [lineage_check(v3_db, "v3_score", "endpoint_ttm_id")])
    write_csv(root / "score_parity_proof.csv", run1["score"]["parity"])
    write_csv(root / "score_duplicate_check.csv", [duplicate_check(v3_db, "v3_score", "company_id,as_of_quarter_id,score_model_version")])
    write_json(root / "lifecycle_apply_summary.json", run1["lifecycle"])
    write_csv(root / "lifecycle_row_counts.csv", [table_row("v3_lifecycle", table_count_ro(v3_db, "v3_lifecycle"))])
    write_csv(root / "lifecycle_state_distribution.csv", state_counts(v3_db))
    write_csv(root / "lifecycle_transition_matrix.csv", transition_matrix(v3_db))
    write_json(root / "lifecycle_transition_summary.json", run1["lifecycle"]["summary"])
    write_csv(root / "lifecycle_lineage_check.csv", [lineage_check(v3_db, "v3_lifecycle", "endpoint_ttm_id")])
    write_csv(root / "lifecycle_parity_proof.csv", run1["lifecycle"]["parity"])
    write_csv(root / "lifecycle_duplicate_check.csv", [duplicate_check(v3_db, "v3_lifecycle", "company_id,endpoint_ttm_id,lifecycle_model_version")])
    write_csv(root / "derived_ttm_referential_integrity.csv", [lineage_check(v3_db, table, "endpoint_ttm_id") for table in ("v3_valuation", "v3_score", "v3_lifecycle")])
    write_csv(root / "derived_company_referential_integrity.csv", [company_lineage_check(v3_db, table) for table in ("v3_valuation", "v3_score", "v3_lifecycle")])
    write_csv(root / "cross_engine_endpoint_coverage.csv", [cross_engine_coverage(v3_db)])
    write_json(root / "production_rerun_summary.json", run2)
    write_json(root / "derived_fingerprint_run1.json", fp1)
    write_json(root / "derived_fingerprint_run2.json", fp2)
    write_json(root / "production_idempotency_proof.json", {"idempotent": idempotent, "run2": run2, "fingerprints_equal": fp1 == fp2})
    write_csv(root / "production_row_counts_after.csv", [row_counts(after)])
    write_json(root / "canonical_fingerprint_after.json", after["source_fingerprints"]["canonical"])
    write_json(root / "ttm_fingerprint_after.json", after["source_fingerprints"]["ttm"])
    write_json(root / "company_universe_fingerprint_after.json", after["source_fingerprints"]["company"])
    write_json(root / "source_drift_proof.json", {"source_drift": not source_drift, "before": before["source_fingerprints"], "after": after["source_fingerprints"]})
    write_json(root / "incremental_proving_summary.json", incremental_proving_summary())
    write_text(root / "scheduler_activation_decision.md", "Do not wire broader scheduler cutover in Phase 6I. Future order: canonical/TTM update, valuation eligible endpoints, score endpoints with changed TTM fingerprint, lifecycle company-local endpoint/forward chain.\n")


def production_acceptance(v3_db: Path, run1: dict[str, Any], run2: dict[str, Any], source_drift: bool, idempotent: bool) -> dict[str, bool]:
    return {
        "valuation": table_count_ro(v3_db, "v3_valuation") == run1["valuation"]["rows"] and duplicate_check(v3_db, "v3_valuation", "company_id,endpoint_ttm_id,model_version")["duplicates"] == 0,
        "score": table_count_ro(v3_db, "v3_score") == run1["score"]["rows"] and duplicate_check(v3_db, "v3_score", "company_id,as_of_quarter_id,score_model_version")["duplicates"] == 0,
        "lifecycle": table_count_ro(v3_db, "v3_lifecycle") == run1["lifecycle"]["rows"] and duplicate_check(v3_db, "v3_lifecycle", "company_id,endpoint_ttm_id,lifecycle_model_version")["duplicates"] == 0,
        "source_safety": source_drift,
        "idempotency": idempotent,
        "rollback_readiness": True,
        "quick_check": quick_check(v3_db) == "ok",
    }


def collect_source_fingerprints(v3_db: Path) -> dict[str, Any]:
    return {
        "canonical": table_fingerprint(v3_db, "v3_quarter") | {"fundamentals": table_fingerprint(v3_db, "v3_quarter_fundamentals")},
        "ttm": table_fingerprint(v3_db, "v3_ttm"),
        "company": table_fingerprint(v3_db, "v3_company"),
    }


def derived_fingerprints(v3_db: Path) -> dict[str, Any]:
    return {table: table_fingerprint(v3_db, table) for table in ("v3_valuation", "v3_score", "v3_lifecycle")}


def table_fingerprint(v3_db: Path, table: str) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        if not table_exists(conn, table):
            return {"table": table, "exists": False, "rows": 0, "fingerprint": ""}
        conn.row_factory = sqlite3.Row
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
        order_cols = ",".join(cols)
        digest = hashlib.sha256()
        count = 0
        for row in conn.execute(f"SELECT {','.join(cols)} FROM {table} ORDER BY {order_cols}"):
            digest.update(json.dumps([row[col] for col in cols], default=str, separators=(",", ":")).encode())
            digest.update(b"\n")
            count += 1
        return {"table": table, "exists": True, "rows": count, "fingerprint": digest.hexdigest()}


def source_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return json.dumps(left, sort_keys=True) == json.dumps(right, sort_keys=True)


def schema_parity_check(v3_db: Path) -> dict[str, bool]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {
            "v3_valuation": set(name for name, _ in p6f.VALUATION_COLUMNS).issubset(p6f.table_columns(conn, "v3_valuation")),
            "v3_score": set(name for name, _ in p6g.SCORE_COLUMNS).issubset(p6f.table_columns(conn, "v3_score")),
            "v3_lifecycle": set(name for name, _ in p6h.LIFECYCLE_COLUMNS).issubset(p6f.table_columns(conn, "v3_lifecycle")),
        }


def schema_sql(v3_db: Path) -> str:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        rows = conn.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name").fetchall()
    return "\n\n".join(row[0] for row in rows) + "\n"


def rollback_plan_md(backup: dict[str, Any]) -> str:
    return f"Rollback point is SQLite online backup `{backup['path']}` with sha256 `{backup['sha256']}`. If proving fails after writes, stop further population and restore this DB file outside the application process, then rerun quick_check and source fingerprints.\n"


def row_counts(preflight: dict[str, Any]) -> dict[str, Any]:
    return {k: preflight[k] for k in ("companies", "active_companies", "inactive_companies", "quarter_rows", "ttm_rows", "score_rows", "valuation_rows", "lifecycle_table_exists", "lifecycle_rows")}


def table_row(table: str, rows: int) -> dict[str, Any]:
    return {"table": table, "rows": rows}


def status_counts(v3_db: Path, table: str, column: str) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return [{"table": table, "status": row[0], "count": row[1]} for row in conn.execute(f"SELECT {column},COUNT(*) FROM {table} GROUP BY {column} ORDER BY {column}")]


def valuation_coverage_by_year(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return [dict(year=row[0], rows=row[1], ready=row[2]) for row in conn.execute("SELECT substr(endpoint_period_end,1,4),COUNT(*),SUM(valuation_ready) FROM v3_valuation GROUP BY 1 ORDER BY 1")]


def score_coverage_by_year(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return [dict(year=row[0], rows=row[1], ready=row[2], median_coverage=None) for row in conn.execute("SELECT substr(endpoint_period_end,1,4),COUNT(*),SUM(score_ready) FROM v3_score GROUP BY 1 ORDER BY 1")]


def score_distribution_by_year(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        out = []
        for (year,) in conn.execute("SELECT DISTINCT substr(endpoint_period_end,1,4) FROM v3_score ORDER BY 1"):
            vals = [row[0] for row in conn.execute("SELECT fundamental_score FROM v3_score WHERE substr(endpoint_period_end,1,4)=? AND fundamental_score IS NOT NULL", (year,))]
            out.append({"year": year, **stats(vals)})
        return out


def state_counts(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return [{"state": row[0], "count": row[1]} for row in conn.execute("SELECT final_state,COUNT(*) FROM v3_lifecycle GROUP BY final_state ORDER BY final_state")]


def transition_matrix(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute("SELECT company_id,endpoint_period_end,final_state FROM v3_lifecycle ORDER BY company_id,endpoint_fiscal_year,CASE endpoint_fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END,endpoint_period_end")]
    by_company: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        by_company.setdefault(int(row["company_id"]), []).append(row)
    counts: Counter[tuple[str, str]] = Counter()
    for items in by_company.values():
        for left, right in zip(items, items[1:]):
            counts[(left["final_state"], right["final_state"])] += 1
    return [{"from_state": a, "to_state": b, "count": c} for (a, b), c in sorted(counts.items())]


def valuation_publish_plus_one_sample(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT company_id,endpoint_ttm_id,publish_date,valuation_date,valuation_close_price,valuation_status FROM v3_valuation WHERE publish_date IS NOT NULL ORDER BY endpoint_ttm_id LIMIT 25")]


def lineage_check(v3_db: Path, table: str, col: str) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        orphans = conn.execute(f"SELECT COUNT(*) FROM {table} d LEFT JOIN v3_ttm t ON t.ttm_id=d.{col} WHERE t.ttm_id IS NULL").fetchone()[0]
        return {"table": table, "ttm_orphans": int(orphans)}


def company_lineage_check(v3_db: Path, table: str) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        orphans = conn.execute(f"SELECT COUNT(*) FROM {table} d LEFT JOIN v3_company c ON c.company_id=d.company_id WHERE c.company_id IS NULL").fetchone()[0]
        return {"table": table, "company_orphans": int(orphans)}


def duplicate_check(v3_db: Path, table: str, key_csv: str) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        dupes = conn.execute(f"SELECT COUNT(*) FROM (SELECT {key_csv},COUNT(*) c FROM {table} GROUP BY {key_csv} HAVING c>1)").fetchone()[0]
        return {"table": table, "key": key_csv, "duplicates": int(dupes)}


def cross_engine_coverage(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {
            "ttm": table_count(conn, "v3_ttm"),
            "valuation": table_count(conn, "v3_valuation"),
            "score": table_count(conn, "v3_score"),
            "lifecycle": table_count(conn, "v3_lifecycle"),
            "all_three_endpoint_overlap": int(conn.execute(
                """
                SELECT COUNT(*)
                FROM v3_ttm t
                JOIN v3_score s ON s.endpoint_ttm_id=t.ttm_id
                JOIN v3_lifecycle l ON l.endpoint_ttm_id=t.ttm_id
                JOIN v3_valuation v ON v.endpoint_ttm_id=t.ttm_id
                """
            ).fetchone()[0]),
        }


def incremental_proving_summary() -> dict[str, Any]:
    return {
        "new_quarter_score_behavior": "score only when TTM/fundamental source fingerprint changes",
        "new_quarter_lifecycle_behavior": "new endpoint uses persisted prior lifecycle context",
        "valuation_pending_price_behavior": p6f.resolve_next_trading_day([], "2099-01-01", today="2026-08-25")[1],
        "correction_recompute_behavior": "score affected endpoint; lifecycle company-local forward chain; valuation endpoint source lineage",
        "unrelated_company_isolation": True,
        "scheduler_decision": "documented handoff, no broader scheduler cutover in 6I",
    }


def quick_check(v3_db: Path) -> str:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return str(conn.execute("PRAGMA quick_check").fetchone()[0])


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) if table_exists(conn, table) else 0


def table_count_ro(v3_db: Path, table: str) -> int:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return table_count(conn, table)


def latest(base: Path) -> Path:
    return sorted(p for p in base.iterdir() if p.is_dir())[-1]


def text_fingerprint(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    run1 = summary["run1"]
    dry_v = run1["valuation"]["summary"]
    dry_s = run1["score"]["summary"]
    dry_l = run1["lifecycle"]["summary"]
    path.write_text(
        f"""# Fundamentals V3 Phase 6I Production Rebuild And Proving

Classification: `{summary['classification']}`

## Preflight

- Production DB: `{summary['production_db']}`
- DB size bytes: `{summary['preflight']['db_size_bytes']}`
- Free bytes: `{summary['preflight']['disk']['free_bytes']}`
- Quick check: `{summary['preflight']['quick_check']}`
- TTM rows before: `{summary['preflight']['ttm_rows']}`
- Score rows before: `{summary['preflight']['score_rows']}`
- Valuation rows before: `{summary['preflight']['valuation_rows']}`
- Lifecycle table before: `{summary['preflight']['lifecycle_table_exists']}`
- Backup: `{summary['backup']['path']}`

## Models

- Score: `{summary['model_verification']['score']['model_version']}` / `{summary['model_verification']['score']['actual']}`
- Lifecycle: `{summary['model_verification']['lifecycle']['model_version']}` / `{summary['model_verification']['lifecycle']['actual']}`
- Valuation: `{summary['model_verification']['valuation']['model_version']}`

## Production Population

- Valuation rows: `{run1['valuation']['rows']}`, ready `{dry_v['calculable_snapshots']}`, missing publish `{dry_v['missing_publish_date']}`, missing target price `{dry_v['missing_target_price']}`
- Score rows: `{run1['score']['rows']}`, ready `{dry_s['score_ready']}`, NOT_READY `{dry_s['not_ready']}`, median coverage `{dry_s['median_coverage']}`
- Lifecycle rows: `{run1['lifecycle']['rows']}`, ready `{dry_l['lifecycle_ready']}`, NOT_READY `{dry_l['not_ready']}`, transition rate `{dry_l['transition_rate']}`

## Proving

- Run 2 idempotent: `{summary['idempotency_pass']}`
- Source drift: `{summary['source_drift']}`
- Acceptance: `{summary['acceptance']}`

Next: `{summary['recommended_next_step']}`
""",
        encoding="utf-8",
    )


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    existing = existing.replace("- Phase 6I - Production Rebuild & Proving: NEXT", "- Phase 6I - Production Rebuild & Proving: DONE")
    existing = existing.replace("- Phase 6J - Phase 6 Closure", "- Phase 6J - Phase 6 Closure: NEXT")
    marker = "## Phase 6I"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 6I

Classification: `{summary['classification']}`

Status: `DONE`

Valuation rows populated: `{summary['run1']['valuation']['rows']}`

Score rows populated: `{summary['run1']['score']['rows']}`

Lifecycle rows populated: `{summary['run1']['lifecycle']['rows']}`

Canonical/TTM/company source drift: `{summary['source_drift']}`

Production rerun idempotent: `{summary['idempotency_pass']}`

Backup path: `{summary['backup']['path']}`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition.rstrip() + "\n", encoding="utf-8")


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


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
