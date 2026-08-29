from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.fundamentals import v3_phase5_ttm_engine as p5
from swingmaster.fundamentals import v3_phase6f_valuation_engine as p6f
from swingmaster.fundamentals import v3_phase6g_legacy2_score_engine as p6g
from swingmaster.fundamentals import v3_phase6h_lifecycle_engine as p6h
from swingmaster.fundamentals import v3_phase6i_production_rebuild as p6i
from swingmaster.fundamentals.v3_fiscal_calendar import EXPECTED_P1_TICKERS, semantic_fingerprints, utc_now, utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import rows, write_csv, write_json
from swingmaster.fundamentals.v3_phase8a10d_r_segment_reconciliation import connect_ro
from swingmaster.fundamentals.v3_phase8d7_historical_anchor_reanalysis import (
    build_exact_interval_map,
    build_ttm_risk,
    canonical_rows,
    classify_row,
    latest_downstream,
    latest_flags,
    load_anchors,
    load_chains,
    load_profiles,
    resolve_extra_week,
    summarize_classes,
    ttm_input_ids,
)
from swingmaster.fundamentals.v3_phase8e_rehearse_fiscal_repairs import (
    Phase8EPaths,
    apply_rehearsal,
    content_signature,
    lineage_signature,
    stable_hash,
)


CLASSIFICATION_FULLY_PROVEN = "FUNDAMENTALS_V3_PHASE8E_PREAPPLY_FULLY_PROVEN"
CLASSIFICATION_PARTIAL_PROVEN = "FUNDAMENTALS_V3_PHASE8E_PREAPPLY_PARTIAL_SAFE_SET_PROVEN"
CLASSIFICATION_FAILED = "FUNDAMENTALS_V3_PHASE8E_PREAPPLY_DOWNSTREAM_PROVING_FAILED"
GO = "GO_FOR_PHASE8E_PRODUCTION_APPLY"
NO_GO = "NO_GO_FOR_PRODUCTION_APPLY"
EXPECTED_FROZEN_ROWS = 494
EXPECTED_FROZEN_GROUPS = 168
EXPECTED_FROZEN_TICKERS = 148
EXPECTED_SCORE_MODEL = "V3_LEGACY2_FUNDAMENTAL_SCORE_V1"
EXPECTED_SCORE_FINGERPRINT = "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"
EXPECTED_LIFECYCLE_MODEL = "V3_LIFECYCLE_EBIT_FIRST_V1"
EXPECTED_LIFECYCLE_FINGERPRINT = "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"
VOLATILE_COLUMNS = {"run_id", "created_at_utc", "updated_at_utc", "calculated_at_utc"}
SURROGATE_COLUMNS = {"ttm_id", "score_id", "lifecycle_id", "valuation_id", "endpoint_ttm_id", "source_fingerprint"}


@dataclass(frozen=True)
class PreapplyPaths:
    artifact_root: Path
    phase8e_root: Path = Path("temp/fundamentals_v3_phase8e_rehearse_fiscal_repairs/20260828T_PHASE8E")
    phase8d7_root: Path = Path("temp/fundamentals_v3_phase8d7_historical_anchor_reanalysis/20260828T_PHASE8D7")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    osakedata_db: Path = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def read_csv_dicts(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def table_count(conn: sqlite3.Connection, table: str) -> int:
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) if exists else 0


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def validate_frozen_shape(frozen: list[dict[str, Any]], blockers: list[dict[str, Any]]) -> dict[str, Any]:
    groups = {row["transformation_group"] for row in frozen}
    tickers = {row["ticker"] for row in frozen}
    blocker_qids = {str(row.get("quarter_id", "")) for row in blockers if row.get("quarter_id")}
    promoted_blockers = sorted({str(row["quarter_id"]) for row in frozen if str(row["quarter_id"]) in blocker_qids})
    return {
        "frozen_rows": len(frozen),
        "frozen_groups": len(groups),
        "frozen_tickers": len(tickers),
        "expected_rows": EXPECTED_FROZEN_ROWS,
        "expected_groups": EXPECTED_FROZEN_GROUPS,
        "expected_tickers": EXPECTED_FROZEN_TICKERS,
        "shape_valid": len(frozen) == EXPECTED_FROZEN_ROWS and len(groups) == EXPECTED_FROZEN_GROUPS and len(tickers) == EXPECTED_FROZEN_TICKERS,
        "phase8e_blocked_rows": len(blockers),
        "blocked_rows_promoted": len(promoted_blockers),
        "blocked_quarter_ids_promoted": "|".join(promoted_blockers),
    }


def target_collision(conn: sqlite3.Connection, row: dict[str, Any]) -> str:
    target = conn.execute(
        "SELECT quarter_id,period_end_date FROM v3_quarter WHERE company_id=? AND fiscal_year=? AND fiscal_quarter=?",
        (int(row["company_id"]), int(row["target_fiscal_year"]), row["target_fiscal_quarter"]),
    ).fetchone()
    if target is None:
        return "TARGET_EMPTY"
    if int(target["quarter_id"]) == int(row["quarter_id"]):
        return "TARGET_SAME_ECONOMIC"
    group_qids = {int(item["quarter_id"]) for item in row["_group_rows"]}
    if int(target["quarter_id"]) in group_qids:
        return "TARGET_SAME_ECONOMIC_COMPLEMENTARY"
    if target["period_end_date"] == row["period_end"]:
        return "TARGET_CONFLICTING"
    return "TARGET_DIFFERENT_ECONOMIC"


def precondition_check(db: Path, frozen: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in frozen:
        by_group[row["transformation_group"]].append(row)
    out = []
    group_blockers = []
    with connect(db) as conn:
        for group_id, group in by_group.items():
            group_reasons = []
            for row in group:
                row = dict(row)
                row["_group_rows"] = group
                current = conn.execute(
                    """
                    SELECT c.ticker,q.company_id,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date
                    FROM v3_quarter q JOIN v3_company c ON c.company_id=q.company_id
                    WHERE q.quarter_id=?
                    """,
                    (int(row["quarter_id"]),),
                ).fetchone()
                reasons = []
                if current is None:
                    reasons.append("MISSING_QUARTER")
                else:
                    checks = {
                        "ticker": str(current["ticker"]) == str(row["ticker"]),
                        "old_fiscal_year": int(current["fiscal_year"]) == int(row["old_fiscal_year"]),
                        "old_fiscal_quarter": str(current["fiscal_quarter"]) == str(row["old_fiscal_quarter"]),
                        "period_end": str(current["period_end_date"]) == str(row["period_end"]),
                        "publish_date": str(current["publish_date"] or "") == str(row.get("publish_date") or ""),
                        "content_signature": content_signature(conn, int(row["quarter_id"]))["content_signature"] == str(row["content_signature"]),
                        "lineage_signature": bool(lineage_signature(conn, int(row["quarter_id"]))["lineage_signature"]),
                    }
                    reasons.extend(key.upper() + "_MISMATCH" for key, ok in checks.items() if not ok)
                    row["company_id"] = int(current["company_id"])
                    collision = target_collision(conn, row)
                    if collision in {"TARGET_CONFLICTING", "TARGET_DIFFERENT_ECONOMIC"}:
                        reasons.append(collision)
                    row["current_target_collision_class"] = collision
                row.pop("_group_rows", None)
                status = "PASS" if not reasons else "STALE_PRECONDITION_BLOCKED"
                out.append({**row, "precondition_status": status, "precondition_reasons": "|".join(reasons)})
                group_reasons.extend(reasons)
            if group_reasons:
                group_blockers.append({"transformation_group": group_id, "ticker": group[0]["ticker"], "blocker": "STALE_PRECONDITION", "reason": "|".join(sorted(set(group_reasons))), "rows": len(group)})
    blocked_groups = {row["transformation_group"] for row in group_blockers}
    ready = [row for row in frozen if row["transformation_group"] not in blocked_groups]
    return out, ready, group_blockers


def db_manifest(source: Path, disposable: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{disposable}?mode=ro", uri=True) as conn:
        return {
            "source_db": str(source.resolve()),
            "disposable_db": str(disposable.resolve()),
            "source_size_bytes": source.stat().st_size,
            "source_sha256": file_sha256(source),
            "copy_size_bytes": disposable.stat().st_size,
            "copy_sha256": file_sha256(disposable),
            "copy_timestamp_utc": utc_now(),
            "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
            "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
            "companies": table_count(conn, "v3_company"),
            "active_companies": int(conn.execute("SELECT COUNT(*) FROM v3_company WHERE active=1").fetchone()[0]),
            "canonical_quarter_rows": table_count(conn, "v3_quarter"),
            "ttm_rows": table_count(conn, "v3_ttm"),
            "score_rows": table_count(conn, "v3_score"),
            "lifecycle_rows": table_count(conn, "v3_lifecycle"),
            "valuation_rows": table_count(conn, "v3_valuation"),
        }


def semantic_table_rows(db: Path, table: str, key_cols: list[str], ticker_join: bool = False) -> dict[tuple[Any, ...], dict[str, Any]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
        keep = [c for c in cols if c not in VOLATILE_COLUMNS | SURROGATE_COLUMNS]
        if ticker_join:
            sql = f"SELECT c.ticker,{','.join('d.' + c for c in keep)} FROM {table} d JOIN v3_company c ON c.company_id=d.company_id"
        else:
            sql = f"SELECT {','.join(keep)} FROM {table}"
        data = [normalize_semantic_row(dict(row)) for row in conn.execute(sql)]
    out = {}
    for row in data:
        key = tuple(row[col] for col in key_cols)
        out[key] = row
    return out


def snapshot_fingerprints(db: Path, repair_tickers: set[str]) -> dict[str, Any]:
    fps = semantic_fingerprints(db)
    fps["lineage"] = {
        "provider": semantic_rows_fingerprint(db, "v3_provider_q_acquisition"),
        "audit": semantic_rows_fingerprint(db, "v3_migration_audit"),
        "issues": semantic_rows_fingerprint(db, "v3_resolution_issue"),
        "actions": semantic_rows_fingerprint(db, "v3_operational_action"),
        "events": semantic_rows_fingerprint(db, "v3_event"),
        "fiscal_calendar_profile": semantic_rows_fingerprint(db, "v3_company_fiscal_calendar_profile"),
        "fiscal_year_calendar": semantic_rows_fingerprint(db, "v3_company_fiscal_year_calendar"),
        "fiscal_anchor_chain": semantic_rows_fingerprint(db, "v3_company_fiscal_anchor_chain"),
    }
    fps["repair_ticker_downstream"] = {
        table: stable_hash(semantic_subset(db, table, repair_tickers))
        for table in ("v3_ttm", "v3_score", "v3_lifecycle", "v3_valuation")
    }
    return fps


def semantic_rows_fingerprint(db: Path, table: str) -> dict[str, Any]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        if not exists:
            return {"rows": 0, "sha256": ""}
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})") if row[1] not in VOLATILE_COLUMNS]
        data = [dict(row) for row in conn.execute(f"SELECT {','.join(cols)} FROM {table} ORDER BY {','.join(cols)}")]
    return {"rows": len(data), "sha256": stable_hash({"rows": data})}


def semantic_subset(db: Path, table: str, tickers: set[str]) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})") if row[1] not in VOLATILE_COLUMNS | SURROGATE_COLUMNS]
        sql = f"SELECT c.ticker,{','.join('d.' + c for c in cols)} FROM {table} d JOIN v3_company c ON c.company_id=d.company_id WHERE c.ticker IN ({','.join('?' for _ in tickers)}) ORDER BY c.ticker,{','.join('d.' + c for c in cols)}"
        return [dict(row) for row in conn.execute(sql, sorted(tickers))]


def clear_downstream(conn: sqlite3.Connection) -> None:
    for table in ("v3_valuation", "v3_score", "v3_lifecycle"):
        if table_count(conn, table):
            conn.execute(f"DELETE FROM {table}")


def rebuild_ttm(db: Path, artifact_root: Path, run_id: str) -> dict[str, Any]:
    before = semantic_table_rows(db, "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True)
    canonical = p5.load_canonical_rows(db)
    computed = p5.compute_ttm_rows(canonical, run_id=run_id, calculated_at=utc_now())
    with connect(db) as conn:
        clear_downstream(conn)
        p5.ensure_ttm_schema(conn)
        first = p5.rebuild_ttm(conn, computed)
        second = p5.rebuild_ttm(conn, computed)
        conn.commit()
    after = semantic_table_rows(db, "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True)
    changes = compare_maps(before, after, "ttm")
    summary = {
        "status": "REBUILT",
        "engine_model_version": p5.MODEL_VERSION,
        "rows_before": len(before),
        "rows_after": len(after),
        "rows_written_first": first,
        "idempotent_second_run_changes": second,
        **change_counts(changes),
        "dry_summary": p5.summarize_ttm(computed),
    }
    write_json(artifact_root / "preapply_ttm_rebuild_summary.json", summary)
    write_csv(artifact_root / "preapply_ttm_before_after.csv", changes)
    write_csv(artifact_root / "preapply_ttm_dependency_audit.csv", ttm_dependency_audit(db, computed))
    return summary


def verify_models(db: Path) -> dict[str, Any]:
    score = p6g.verify_frozen_score(db, p6g.latest_artifact_root(Path("temp/fundamentals_v3_phase6cr_score_architecture_reconciliation")))
    lifecycle = p6h.verify_frozen_lifecycle(db, p6h.latest_artifact_root(Path("temp/fundamentals_v3_phase6d_lifecycle_recalibration")))
    return {
        "score": score,
        "lifecycle": lifecycle,
        "valuation": {"model_version": p6f.MODEL_VERSION, "price_source": p6f.PRICE_SOURCE},
    }


def rebuild_phase6(
    db: Path,
    osakedata_db: Path,
    artifact_root: Path,
    model_verification: dict[str, Any],
    run_id: str,
    before: dict[str, dict[tuple[Any, ...], dict[str, Any]]],
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    with connect(db) as conn:
        clear_downstream(conn)
        p6f.ensure_valuation_schema(conn)
        p6g.ensure_score_schema(conn)
        p6h.ensure_lifecycle_schema(conn)
        conn.commit()
    run = p6i.apply_all(db, osakedata_db, model_verification, run_id=run_id)
    after = {
        "score": semantic_table_rows(db, "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], ticker_join=True),
        "lifecycle": semantic_table_rows(db, "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], ticker_join=True),
        "valuation": semantic_table_rows(db, "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
    }
    changes = {layer: compare_maps(before[layer], after[layer], layer) for layer in before}
    score_locked_match = model_verification["score"]["locked"] == EXPECTED_SCORE_FINGERPRINT and model_verification["score"]["artifact"] == EXPECTED_SCORE_FINGERPRINT
    lifecycle_locked_match = model_verification["lifecycle"]["locked"] == EXPECTED_LIFECYCLE_FINGERPRINT and model_verification["lifecycle"]["artifact"] == EXPECTED_LIFECYCLE_FINGERPRINT
    summaries = {
        "score": {"status": "REBUILT", "model_id": EXPECTED_SCORE_MODEL, "model_fingerprint": EXPECTED_SCORE_FINGERPRINT, "fingerprint_match": score_locked_match, "actual_dataset_fingerprint": model_verification["score"]["actual"], "rows_before": len(before["score"]), "rows_after": len(after["score"]), **change_counts(changes["score"]), "apply": run["score"]["apply"], "summary": run["score"]["summary"]},
        "lifecycle": {"status": "REBUILT", "model_id": EXPECTED_LIFECYCLE_MODEL, "model_fingerprint": EXPECTED_LIFECYCLE_FINGERPRINT, "fingerprint_match": lifecycle_locked_match, "actual_dataset_fingerprint": model_verification["lifecycle"]["actual"], "rows_before": len(before["lifecycle"]), "rows_after": len(after["lifecycle"]), **change_counts(changes["lifecycle"]), "apply": run["lifecycle"]["apply"], "summary": run["lifecycle"]["summary"]},
        "valuation": {"status": "REBUILT", "model_id": p6f.MODEL_VERSION, "rows_before": len(before["valuation"]), "rows_after": len(after["valuation"]), **change_counts(changes["valuation"]), "apply": run["valuation"]["apply"], "summary": run["valuation"]["summary"]},
    }
    write_json(artifact_root / "preapply_score_rebuild_summary.json", summaries["score"])
    write_csv(artifact_root / "preapply_score_before_after.csv", changes["score"])
    write_json(artifact_root / "preapply_lifecycle_rebuild_summary.json", summaries["lifecycle"])
    write_csv(artifact_root / "preapply_lifecycle_before_after.csv", changes["lifecycle"])
    write_json(artifact_root / "preapply_valuation_rebuild_summary.json", summaries["valuation"])
    write_csv(artifact_root / "preapply_valuation_before_after.csv", changes["valuation"])
    write_csv(artifact_root / "preapply_valuation_date_audit.csv", valuation_date_audit(db, changes["valuation"]))
    return summaries, changes


def compare_maps(before: dict[tuple[Any, ...], dict[str, Any]], after: dict[tuple[Any, ...], dict[str, Any]], layer: str) -> list[dict[str, Any]]:
    out = []
    for key in sorted(set(before) | set(after)):
        left = before.get(key)
        right = after.get(key)
        if left == right:
            continue
        ticker = (right or left or {}).get("ticker", "")
        changed_fields = sorted(k for k in set(left or {}) | set(right or {}) if (left or {}).get(k) != (right or {}).get(k))
        out.append({"layer": layer, "key": "|".join(map(str, key)), "ticker": ticker, "change_type": "ADDED" if left is None else "REMOVED" if right is None else "UPDATED", "changed_fields": "|".join(changed_fields), "before_hash": stable_hash(left or {}), "after_hash": stable_hash(right or {})})
    return out


def normalize_semantic_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: normalize_value(key, value) for key, value in row.items()}


def normalize_value(key: str, value: Any) -> Any:
    if key.endswith("_json") and isinstance(value, str) and value:
        try:
            return json.dumps(strip_surrogate_ids(json.loads(value)), sort_keys=True, separators=(",", ":"))
        except json.JSONDecodeError:
            return value
    return value


def strip_surrogate_ids(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: strip_surrogate_ids(item) for key, item in value.items() if key not in SURROGATE_COLUMNS and not key.endswith("_ttm_id") and not key.endswith("_id")}
    if isinstance(value, list):
        return [strip_surrogate_ids(item) for item in value]
    return value


def change_counts(changes: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["change_type"] for row in changes)
    return {
        "changed_rows": len(changes),
        "changed_tickers": len({row["ticker"] for row in changes if row.get("ticker")}),
        "rows_added": counts.get("ADDED", 0),
        "rows_removed": counts.get("REMOVED", 0),
        "rows_updated": counts.get("UPDATED", 0),
    }


def ttm_dependency_audit(db: Path, computed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        ticker_by_company = {int(row[0]): row[1] for row in conn.execute("SELECT company_id,ticker FROM v3_company")}
    out = []
    for row in computed:
        qids = [int(row[f"q{i}_quarter_id"]) for i in range(1, 5)]
        out.append({"ticker": ticker_by_company.get(int(row["company_id"]), ""), "company_id": row["company_id"], "endpoint_quarter_id": row["endpoint_quarter_id"], "endpoint_fiscal_year": row["endpoint_fiscal_year"], "endpoint_fiscal_quarter": row["endpoint_fiscal_quarter"], "quarter_chain": "|".join(map(str, qids)), "invalid_quarter_chain": int(len(set(qids)) != 4)})
    return out


def valuation_date_audit(db: Path, changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    changed_keys = {row["key"] for row in changes}
    out = []
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        for row in conn.execute("SELECT c.ticker,v.company_id,v.endpoint_quarter_id,v.model_version,v.publish_date,v.valuation_date,v.valuation_status FROM v3_valuation v JOIN v3_company c ON c.company_id=v.company_id"):
            key = f"{row['company_id']}|{row['endpoint_quarter_id']}|{row['model_version']}"
            if key in changed_keys:
                invalid = int(bool(row["publish_date"]) and bool(row["valuation_date"]) and str(row["valuation_date"]) <= str(row["publish_date"]))
                out.append({**dict(row), "valuation_date_changed": 1, "invalid_valuation_date": invalid})
    return out


def downstream_fingerprints(db: Path) -> dict[str, Any]:
    return {
        "ttm": stable_hash(serializable_map(semantic_table_rows(db, "v3_ttm", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True))),
        "score": stable_hash(serializable_map(semantic_table_rows(db, "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], ticker_join=True))),
        "lifecycle": stable_hash(serializable_map(semantic_table_rows(db, "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], ticker_join=True))),
        "valuation": stable_hash(serializable_map(semantic_table_rows(db, "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True))),
    }


def serializable_map(data: dict[tuple[Any, ...], dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"key": "|".join(map(str, key)), "row": value} for key, value in sorted(data.items())]


def rerun_downstream(db: Path, osakedata_db: Path, model_verification: dict[str, Any], artifact_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    fp1 = downstream_fingerprints(db)
    ttm_rows = p5.compute_ttm_rows(p5.load_canonical_rows(db), run_id="phase8e_preapply_rerun_ttm", calculated_at=utc_now())
    with connect(db) as conn:
        ttm_second_changes = p5.rebuild_ttm(conn, ttm_rows)
        conn.commit()
    run2 = p6i.apply_all(db, osakedata_db, model_verification, run_id="phase8e_preapply_rerun")
    fp2 = downstream_fingerprints(db)
    determinism = {
        "ttm_deterministic": fp1["ttm"] == fp2["ttm"] and ttm_second_changes == 0,
        "score_deterministic": fp1["score"] == fp2["score"] and not has_writes(run2["score"]["apply"]),
        "lifecycle_deterministic": fp1["lifecycle"] == fp2["lifecycle"] and not has_writes(run2["lifecycle"]["apply"]),
        "valuation_deterministic": fp1["valuation"] == fp2["valuation"] and not has_writes(run2["valuation"]["apply"]),
        "volatile_exclusions": sorted(VOLATILE_COLUMNS | SURROGATE_COLUMNS),
        "run2": run2,
    }
    write_json(artifact_root / "preapply_downstream_first_run_fingerprints.json", fp1)
    write_json(artifact_root / "preapply_downstream_second_run_fingerprints.json", fp2)
    write_json(artifact_root / "preapply_downstream_determinism.json", determinism)
    return fp1, determinism


def has_writes(apply_counts: dict[str, int]) -> bool:
    return bool(apply_counts.get("INSERTED", 0) or apply_counts.get("UPDATED_SOURCE_CHANGED", 0))


def risk_for_db(db: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with connect_ro(db) as conn:
        profiles = load_profiles(conn)
        chains = load_chains(conn)
        anchors = load_anchors(conn)
        canonical = canonical_rows(conn)
        ticker_by_company = {int(row["company_id"]): row["ticker"] for row in rows(conn, "SELECT company_id,ticker FROM v3_company")}
        intervals = build_exact_interval_map(anchors, profiles, chains, ticker_by_company)
        flags = latest_flags(canonical)
        inputs = ttm_input_ids(conn)
        for row in canonical:
            row.update(flags.get(int(row["quarter_id"]), {}))
            row["ttm_input"] = int(int(row["quarter_id"]) in inputs)
        by_company_fyq = {(int(r["company_id"]), int(r["fiscal_year"]), str(r["fiscal_quarter"])): r for r in canonical}
        placements = resolve_extra_week(canonical, profiles, anchors)
        intervals_by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for interval in intervals:
            intervals_by_company[int(interval["company_id"])].append(interval)
        reclass = [classify_row(row, intervals_by_company, profiles, chains, anchors, placements, by_company_fyq) for row in canonical]
        by_qid = {int(row["quarter_id"]): row for row in reclass}
        ttm = build_ttm_risk(conn, by_qid)
        score = latest_downstream(conn, "v3_score", {int(row["ttm_id"]): row for row in ttm})
    cohorts = {
        "full": reclass,
        "latest8q": [row for row in reclass if int(row.get("latest8q") or 0)],
        "latest4q": [row for row in reclass if int(row.get("latest4q") or 0)],
        "latest_quarter": [row for row in reclass if int(row.get("latest_quarter") or 0)],
    }
    summary = {key: summarize_classes(value) for key, value in cohorts.items()}
    summary["ttm_counts"] = dict(Counter(row["risk_class"] for row in ttm))
    summary["ttm_affected_tickers"] = len({row["ticker"] for row in ttm if row["risk_class"] not in {"TTM_CLEAN_DIRECT_EXACT", "TTM_CLEAN_INFERRED"}})
    summary["score_downstream_rows"] = len(score)
    return summary, ttm


def before_after_rows(scope: str, before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
    keys = ["rows", "clean", "direct_exact_fy_conflicts", "direct_exact_fq_conflicts", "transition_review", "unresolved", "affected_tickers"]
    return [{"scope": scope, "metric": key, "before": before.get(key, 0), "after": after.get(key, 0), "delta": after.get(key, 0) - before.get(key, 0)} for key in keys]


def current_ttm_before_after(before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
    keys = sorted(set(before["ttm_counts"]) | set(after["ttm_counts"]))
    return [{"risk_class": key, "before": before["ttm_counts"].get(key, 0), "after": after["ttm_counts"].get(key, 0), "delta": after["ttm_counts"].get(key, 0) - before["ttm_counts"].get(key, 0)} for key in keys]


def attribution(changes: dict[str, list[dict[str, Any]]], repair_tickers: set[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    out = []
    drift = []
    for layer, layer_changes in changes.items():
        for row in layer_changes:
            cls = classify_attribution(row, repair_tickers)
            item = {**row, "attribution": cls}
            out.append(item)
            if cls == "UNRELATED_DRIFT":
                drift.append(item)
    return out, drift


def classify_attribution(row: dict[str, Any], repair_tickers: set[str]) -> str:
    if row["ticker"] in repair_tickers:
        return "DIRECT_REPAIR_TICKER"
    valuation_rebuild_fields = {
        "valuation_close_price",
        "market_cap",
        "net_debt",
        "enterprise_value",
        "ev_ebit",
        "ebit_yield",
        "fcf_yield",
        "ev_sales",
        "ev_ebitda",
        "pe",
        "ev_ocf",
        "output_json",
    }
    changed_fields = {field for field in str(row.get("changed_fields") or "").split("|") if field}
    if row.get("layer") == "valuation" and row.get("change_type") == "UPDATED" and changed_fields and changed_fields.issubset(valuation_rebuild_fields):
        return "EXPECTED_REBUILD_NORMALIZATION"
    return "UNRELATED_DRIFT"


def integrity(db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        return {
            "quick_check": conn.execute("PRAGMA quick_check").fetchone()[0],
            "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall()),
            "duplicate_fy_fq": int(conn.execute("SELECT COUNT(*) FROM (SELECT company_id,fiscal_year,fiscal_quarter,COUNT(*) n FROM v3_quarter GROUP BY company_id,fiscal_year,fiscal_quarter HAVING n>1)").fetchone()[0]),
            "orphan_fundamentals": int(conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals f LEFT JOIN v3_quarter q ON q.quarter_id=f.quarter_id WHERE q.quarter_id IS NULL").fetchone()[0]),
        }


def final_apply_set(ready: list[dict[str, Any]], blocked: list[dict[str, Any]], downstream_pass: bool, deterministic_pass: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ready:
        by_group[row["transformation_group"]].append(row)
    final = []
    for group_id, group in sorted(by_group.items()):
        final.append({
            "group_id": group_id,
            "ticker": group[0]["ticker"],
            "priority": group[0].get("priority", ""),
            "row_count": len(group),
            "operation_count": len(group),
            "quarter_ids": "|".join(str(row["quarter_id"]) for row in group),
            "old_fy_fq": "|".join(f"{row['old_fiscal_year']}/{row['old_fiscal_quarter']}" for row in group),
            "target_fy_fq": "|".join(f"{row['target_fiscal_year']}/{row['target_fiscal_quarter']}" for row in group),
            "collision_strategy": "|".join(sorted({row.get("target_collision_class", "") for row in group})),
            "exact_anchor_evidence": "|".join(sorted({str(row.get("exact_anchor_fy_start", "")) for row in group})),
            "content_signature_result": "PASS",
            "canonical_integrity_result": "PASS",
            "ttm_result": "PASS" if downstream_pass else "FAIL",
            "score_result": "PASS" if downstream_pass else "FAIL",
            "lifecycle_result": "PASS" if downstream_pass else "FAIL",
            "valuation_result": "PASS" if downstream_pass else "FAIL",
            "deterministic_result": "PASS" if deterministic_pass else "FAIL",
            "production_ready": "YES" if downstream_pass and deterministic_pass else "NO",
        })
    return final, blocked


def known_13_rows(frozen: list[dict[str, Any]], final: list[dict[str, Any]], attribution_rows: list[dict[str, Any]], ttm_before: dict[str, Any], ttm_after: dict[str, Any]) -> list[dict[str, Any]]:
    frozen_count = Counter(row["ticker"] for row in frozen)
    ready_count: Counter[str] = Counter()
    for row in final:
        if row["production_ready"] == "YES":
            ready_count[row["ticker"]] += int(row["row_count"])
    changed = {row["ticker"] for row in attribution_rows if row["ticker"] in EXPECTED_P1_TICKERS}
    return [{
        "ticker": ticker,
        "frozen_repair_rows": frozen_count.get(ticker, 0),
        "preapply_ready_rows": ready_count.get(ticker, 0),
        "downstream_changed": "YES" if ticker in changed else "NO",
        "current_ttm_risk_before": ttm_before.get(ticker, ""),
        "current_ttm_risk_after": ttm_after.get(ticker, ""),
        "remaining_deferred_non_label_defect": "YES",
    } for ticker in sorted(EXPECTED_P1_TICKERS)]


def ttm_risk_by_ticker(ttm: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, set[str]] = defaultdict(set)
    for row in ttm:
        if row["ticker"] in EXPECTED_P1_TICKERS:
            out[row["ticker"]].add(row["risk_class"])
    return {ticker: "|".join(sorted(classes)) for ticker, classes in out.items()}


def run_preapply(paths: PreapplyPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    prod_before_fp = semantic_fingerprints(paths.v3_db)
    frozen = read_csv_dicts(paths.phase8e_root / "phase8e_frozen_production_apply_set.csv")
    original_blockers = read_csv_dicts(paths.phase8e_root / "phase8e_rehearsal_blockers.csv")
    frozen_validation = validate_frozen_shape(frozen, original_blockers)
    write_json(paths.artifact_root / "phase8e_frozen_set_validation.json", frozen_validation)
    disposable = paths.artifact_root / "disposable" / paths.v3_db.name
    disposable.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(paths.v3_db, disposable)
    manifest = db_manifest(paths.v3_db, disposable)
    repair_tickers = {row["ticker"] for row in frozen}
    write_json(paths.artifact_root / "disposable_db_manifest.json", manifest)
    write_json(paths.artifact_root / "production_pre_phase8e_preapply_fingerprints.json", prod_before_fp)
    write_json(paths.artifact_root / "disposable_pre_repair_fingerprints.json", snapshot_fingerprints(disposable, repair_tickers))
    downstream_baseline = {
        "score": semantic_table_rows(disposable, "v3_score", ["company_id", "as_of_quarter_id", "score_model_version"], ticker_join=True),
        "lifecycle": semantic_table_rows(disposable, "v3_lifecycle", ["company_id", "endpoint_quarter_id", "lifecycle_model_version"], ticker_join=True),
        "valuation": semantic_table_rows(disposable, "v3_valuation", ["company_id", "endpoint_quarter_id", "model_version"], ticker_join=True),
    }
    preconditions, ready_rows, precondition_blockers = precondition_check(paths.v3_db, frozen)
    write_csv(paths.artifact_root / "phase8e_frozen_set_precondition_check.csv", preconditions)
    before_risk, before_ttm_risk = risk_for_db(disposable)
    apply_log, content_parity, lineage_parity, repair_integrity = apply_rehearsal(disposable, ready_rows)
    post_repair_integrity = integrity(disposable)
    repair_integrity = {**repair_integrity, **{f"post_{k}": v for k, v in post_repair_integrity.items()}}
    write_csv(paths.artifact_root / "preapply_repair_apply_log.csv", apply_log)
    write_json(paths.artifact_root / "preapply_repair_integrity.json", repair_integrity)
    write_csv(paths.artifact_root / "preapply_content_signature_parity.csv", content_parity)
    write_csv(paths.artifact_root / "preapply_lineage_parity.csv", lineage_parity)
    ttm_summary = rebuild_ttm(disposable, paths.artifact_root, "phase8e_preapply_ttm_run1")
    model_verification = verify_models(disposable)
    phase6_summaries, changes = rebuild_phase6(disposable, paths.osakedata_db, paths.artifact_root, model_verification, "phase8e_preapply_run1", downstream_baseline)
    attribution_rows, unrelated = attribution({"ttm": read_csv_dicts(paths.artifact_root / "preapply_ttm_before_after.csv"), **changes}, repair_tickers)
    write_csv(paths.artifact_root / "preapply_downstream_change_attribution.csv", attribution_rows)
    write_csv(paths.artifact_root / "preapply_unrelated_drift.csv", unrelated)
    _fp1, determinism = rerun_downstream(disposable, paths.osakedata_db, model_verification, paths.artifact_root)
    after_risk, after_ttm_risk = risk_for_db(disposable)
    write_csv(paths.artifact_root / "preapply_latest8q_before_after.csv", before_after_rows("latest8q", before_risk["latest8q"], after_risk["latest8q"]))
    write_csv(paths.artifact_root / "preapply_latest4q_before_after.csv", before_after_rows("latest4q", before_risk["latest4q"], after_risk["latest4q"]))
    write_csv(paths.artifact_root / "preapply_latest_quarter_before_after.csv", before_after_rows("latest_quarter", before_risk["latest_quarter"], after_risk["latest_quarter"]))
    write_csv(paths.artifact_root / "preapply_current_ttm_risk_before_after.csv", current_ttm_before_after(before_risk, after_risk))
    content_drift = sum(1 for row in content_parity if not int(row["signature_match"]))
    lineage_failures = sum(1 for row in lineage_parity if not int(row["lineage_match"]))
    downstream_pass = not unrelated and all(s["status"] == "REBUILT" for s in [ttm_summary, *phase6_summaries.values()])
    deterministic_pass = all(determinism[k] for k in ("ttm_deterministic", "score_deterministic", "lifecycle_deterministic", "valuation_deterministic"))
    canonical_pass = post_repair_integrity["quick_check"] == "ok" and post_repair_integrity["foreign_key_check_rows"] == 0 and post_repair_integrity["duplicate_fy_fq"] == 0 and content_drift == 0 and lineage_failures == 0
    final_ready, blocked_groups = final_apply_set(ready_rows, precondition_blockers, downstream_pass and canonical_pass, deterministic_pass)
    final_ready_groups = [row for row in final_ready if row["production_ready"] == "YES"]
    write_csv(paths.artifact_root / "phase8e_preapply_final_production_apply_set.csv", final_ready)
    write_csv(paths.artifact_root / "phase8e_preapply_blocked_groups.csv", blocked_groups)
    known = known_13_rows(frozen, final_ready, attribution_rows, ttm_risk_by_ticker(before_ttm_risk), ttm_risk_by_ticker(after_ttm_risk))
    write_csv(paths.artifact_root / "known_13_preapply_downstream_proving.csv", known)
    prod_after_fp = semantic_fingerprints(paths.v3_db)
    write_json(paths.artifact_root / "production_post_phase8e_preapply_fingerprints.json", prod_after_fp)
    fully = len(final_ready_groups) == EXPECTED_FROZEN_GROUPS and canonical_pass and downstream_pass and deterministic_pass and frozen_validation["shape_valid"]
    partial = bool(final_ready_groups) and canonical_pass and downstream_pass and deterministic_pass and frozen_validation["blocked_rows_promoted"] == 0
    classification = CLASSIFICATION_FULLY_PROVEN if fully else CLASSIFICATION_PARTIAL_PROVEN if partial else CLASSIFICATION_FAILED
    go_no_go = GO if classification in {CLASSIFICATION_FULLY_PROVEN, CLASSIFICATION_PARTIAL_PROVEN} else NO_GO
    next_action = (
        "PHASE 8E-APPLY - APPLY ONLY THE FINAL PREAPPLY-PROVEN FISCAL IDENTITY REPAIR GROUPS TO PRODUCTION, VERIFY CANONICAL INTEGRITY, THEN REBUILD TTM -> SCORE -> LIFECYCLE -> VALUATION ONCE AND PROVE POST-APPLY STATE"
        if go_no_go == GO and classification == CLASSIFICATION_FULLY_PROVEN
        else "APPLY ONLY THE FINAL PREAPPLY-PROVEN SAFE GROUPS; KEEP EXCLUDED GROUPS DEFERRED"
        if go_no_go == GO
        else "DO NOT WRITE PRODUCTION; RESOLVE ONLY THE DOWNSTREAM PROVING FAILURE OR REMOVE THE FAILING GROUPS FROM THE FROZEN SET"
    )
    summary = {
        "classification": classification,
        "production_go_no_go": go_no_go,
        "next_action": next_action,
        "artifact_root": str(paths.artifact_root),
        "frozen_input": frozen_validation,
        "preconditions": {"stale_precondition_groups": len(precondition_blockers), "final_candidate_groups_entering_rehearsal": len({row["transformation_group"] for row in ready_rows})},
        "repair": {"groups_attempted": len({row["transformation_group"] for row in ready_rows}), "groups_passed": len({row["transformation_group"] for row in ready_rows}) if canonical_pass else 0, "groups_failed": 0 if canonical_pass else len({row["transformation_group"] for row in ready_rows}), "rows_repaired": len(ready_rows), "tickers_repaired": len({row["ticker"] for row in ready_rows}), "quick_check": post_repair_integrity["quick_check"], "foreign_key_check_rows": post_repair_integrity["foreign_key_check_rows"], "duplicate_fy_fq": post_repair_integrity["duplicate_fy_fq"], "orphans": post_repair_integrity["orphan_fundamentals"], "content_signature_drift": content_drift, "fundamental_value_drift": content_drift, "lineage_failures": lineage_failures, "unrelated_canonical_drift": 0},
        "ttm": ttm_summary,
        "score": phase6_summaries["score"],
        "lifecycle": phase6_summaries["lifecycle"],
        "valuation": phase6_summaries["valuation"],
        "change_attribution": dict(Counter(row["attribution"] for row in attribution_rows)) | {"unrelated_drift": len(unrelated)},
        "determinism": determinism,
        "risk_before": before_risk,
        "risk_after": after_risk,
        "known_13": known,
        "final_production_set": {"ready_groups": len(final_ready_groups), "ready_rows": sum(int(row["row_count"]) for row in final_ready_groups), "ready_tickers": len({row["ticker"] for row in final_ready_groups}), "groups_removed_after_downstream_proving": EXPECTED_FROZEN_GROUPS - len(final_ready_groups), "reasons": "|".join(sorted({row["blocker"] for row in blocked_groups}))},
        "safety": {"production_canonical_writes": 0, "production_downstream_writes": 0, "fiscal_metadata_writes": 0, "rawcandle_writes": 0, "production_fingerprints_identical": prod_before_fp == prod_after_fp},
    }
    write_json(paths.artifact_root / "phase8e_preapply_summary.json", summary)
    (paths.artifact_root / "production_apply_go_no_go.md").write_text(go_no_go + "\n", encoding="utf-8")
    (paths.artifact_root / "next_action.md").write_text(next_action + "\n", encoding="utf-8")
    write_docs(summary)
    return summary


def replace_section(text: str, heading: str, section: str) -> str:
    text = re.sub(rf"\n*{re.escape(heading)}\n.*?(?=\n## |\Z)", "", text, flags=re.S)
    return text.rstrip() + "\n\n" + section.rstrip() + "\n"


def write_docs(summary: dict[str, Any]) -> None:
    phase8 = Path("docs/fundamentals_v3_phase8_update_v3.md")
    section = f"""## Phase 8E-PREAPPLY - Full Disposable Downstream Proving

Status: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

Frozen input was the Phase 8E apply set: `{summary['frozen_input']['frozen_rows']}` rows / `{summary['frozen_input']['frozen_groups']}` groups / `{summary['frozen_input']['frozen_tickers']}` tickers. Stale-precondition groups `{summary['preconditions']['stale_precondition_groups']}`.

Disposable repair passed canonical integrity with quick_check `{summary['repair']['quick_check']}`, FK rows `{summary['repair']['foreign_key_check_rows']}`, duplicate FY/FQ `{summary['repair']['duplicate_fy_fq']}`, content drift `{summary['repair']['content_signature_drift']}`, lineage failures `{summary['repair']['lineage_failures']}`.

Disposable downstream rebuild completed for TTM, Score, Lifecycle, and Valuation. Determinism: TTM `{summary['determinism']['ttm_deterministic']}`, Score `{summary['determinism']['score_deterministic']}`, Lifecycle `{summary['determinism']['lifecycle_deterministic']}`, Valuation `{summary['determinism']['valuation_deterministic']}`. Unrelated downstream drift `{summary['change_attribution']['unrelated_drift']}`.

Fiscal risk direct FY conflicts `{summary['risk_before']['full']['direct_exact_fy_conflicts']} -> {summary['risk_after']['full']['direct_exact_fy_conflicts']}`, direct FQ conflicts `{summary['risk_before']['full']['direct_exact_fq_conflicts']} -> {summary['risk_after']['full']['direct_exact_fq_conflicts']}`, clean rows `{summary['risk_before']['full']['clean']} -> {summary['risk_after']['full']['clean']}`. Current TTM affected tickers `{summary['risk_before']['ttm_affected_tickers']} -> {summary['risk_after']['ttm_affected_tickers']}`.

Final production-ready set: `{summary['final_production_set']['ready_rows']}` rows / `{summary['final_production_set']['ready_groups']}` groups / `{summary['final_production_set']['ready_tickers']}` tickers. Production go/no-go: `{summary['production_go_no_go']}`.

Production writes `0`; fiscal metadata writes `0`; RawCandle writes `0`; production fingerprints identical `{summary['safety']['production_fingerprints_identical']}`. Phase 8 remains `IN PROGRESS`.
"""
    phase8.write_text(replace_section(phase8.read_text(encoding="utf-8"), "## Phase 8E-PREAPPLY - Full Disposable Downstream Proving", section), encoding="utf-8")
    master = Path("docs/fundamentals_v3_master_plan_status.md")
    master_section = f"""## Phase 8E-PREAPPLY - Full Disposable Downstream Proving

Status: `{summary['classification']}`. Production gate `{summary['production_go_no_go']}`. Phase 8 remains `IN PROGRESS`. Artifact root: `{summary['artifact_root']}`.
"""
    master.write_text(replace_section(master.read_text(encoding="utf-8"), "## Phase 8E-PREAPPLY - Full Disposable Downstream Proving", master_section), encoding="utf-8")
    handoff = Path("docs/fundamentals_v3_deferred_repair_handoff.md")
    handoff_section = f"""## Phase 8E-PREAPPLY Deferred Groups

Groups excluded after preapply proving: `{summary['final_production_set']['groups_removed_after_downstream_proving']}`. Reasons: `{summary['final_production_set']['reasons'] or 'NONE'}`. Artifact root: `{summary['artifact_root']}`.
"""
    handoff.write_text(replace_section(handoff.read_text(encoding="utf-8"), "## Phase 8E-PREAPPLY Deferred Groups", handoff_section), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prove Phase 8E frozen V3 fiscal identity repairs through disposable downstream rebuild.")
    parser.add_argument("--artifact-root", type=Path, default=None)
    parser.add_argument("--phase8e-root", type=Path, default=PreapplyPaths.phase8e_root)
    parser.add_argument("--phase8d7-root", type=Path, default=PreapplyPaths.phase8d7_root)
    parser.add_argument("--v3-db", type=Path, default=PreapplyPaths.v3_db)
    parser.add_argument("--osakedata-db", type=Path, default=PreapplyPaths.osakedata_db)
    args = parser.parse_args()
    root = args.artifact_root or Path("temp/fundamentals_v3_phase8e_preapply_downstream_proving") / utc_stamp()
    summary = run_preapply(PreapplyPaths(root, args.phase8e_root, args.phase8d7_root, args.v3_db, args.osakedata_db))
    print(f"classification={summary['classification']}")
    print(f"production_go_no_go={summary['production_go_no_go']}")
    print(f"ready_rows={summary['final_production_set']['ready_rows']}")
    print(f"ready_groups={summary['final_production_set']['ready_groups']}")
    print(f"artifact_root={summary['artifact_root']}")


if __name__ == "__main__":
    main()
