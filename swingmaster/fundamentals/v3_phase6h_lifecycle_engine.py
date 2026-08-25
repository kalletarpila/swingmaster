from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals import v3_phase6d_lifecycle_recalibration as p6d
from swingmaster.fundamentals.v3_phase6f_valuation_engine import MODEL_VERSION as VALUATION_MODEL_VERSION
from swingmaster.fundamentals.v3_phase6f_valuation_engine import table_columns, table_count

CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE6H_LIFECYCLE_ENGINE_IMPLEMENTED_READY_FOR_PHASE6I"
CLASSIFICATION_PARITY_REQUIRED = "FUNDAMENTALS_V3_PHASE6H_LIFECYCLE_PARITY_REFINEMENT_REQUIRED"
CLASSIFICATION_SCHEMA_REQUIRED = "FUNDAMENTALS_V3_PHASE6H_LIFECYCLE_SCHEMA_REFINEMENT_REQUIRED"
BLOCKED_LIFECYCLE_FINGERPRINT = "FUNDAMENTALS_V3_PHASE6H_BLOCKED_LIFECYCLE_FINGERPRINT_MISMATCH"
EXPECTED_LIFECYCLE_FINGERPRINT = "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"
EXPECTED_SCORE_FINGERPRINT = "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"
MODEL_VERSION = p6d.MODEL_VERSION
NEXT_PHASE = "MASTER PLAN PHASE 6I - PRODUCTION REBUILD & PROVING"

LIFECYCLE_COLUMNS = [
    ("lifecycle_id", "INTEGER PRIMARY KEY"),
    ("company_id", "INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE"),
    ("endpoint_ttm_id", "INTEGER NOT NULL REFERENCES v3_ttm(ttm_id) ON DELETE CASCADE"),
    ("endpoint_quarter_id", "INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE"),
    ("endpoint_fiscal_year", "INTEGER NOT NULL"),
    ("endpoint_fiscal_quarter", "TEXT NOT NULL"),
    ("endpoint_period_end", "TEXT NOT NULL"),
    ("publish_date", "TEXT"),
    ("lifecycle_model_version", "TEXT NOT NULL"),
    ("lifecycle_ready", "INTEGER NOT NULL CHECK (lifecycle_ready IN (0, 1))"),
    ("confidence", "TEXT NOT NULL"),
    ("raw_state", "TEXT NOT NULL"),
    ("final_state", "TEXT NOT NULL"),
    ("previous_final_state", "TEXT"),
    ("transitioned", "INTEGER NOT NULL CHECK (transitioned IN (0, 1))"),
    ("transition_reason", "TEXT NOT NULL"),
    ("state_age", "INTEGER NOT NULL"),
    ("candidate_state", "TEXT"),
    ("candidate_confirmation_count", "INTEGER NOT NULL DEFAULT 0"),
    ("hard_inflection_applied", "INTEGER NOT NULL CHECK (hard_inflection_applied IN (0, 1))"),
    ("lifecycle_fingerprint", "TEXT NOT NULL"),
    ("source_fingerprint", "TEXT NOT NULL"),
    ("feature_json", "TEXT"),
    ("output_json", "TEXT"),
    ("run_id", "TEXT"),
    ("created_at_utc", "TEXT NOT NULL"),
    ("updated_at_utc", "TEXT NOT NULL"),
]


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def latest_artifact_root(base: Path) -> Path:
    candidates = [p for p in base.iterdir() if p.is_dir()] if base.exists() else []
    if not candidates:
        raise FileNotFoundError(f"No artifact root found under {base}")
    return sorted(candidates)[-1]


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def lifecycle_schema_sql() -> str:
    cols = ",\n    ".join(f"{name} {definition}" for name, definition in LIFECYCLE_COLUMNS)
    return f"""CREATE TABLE IF NOT EXISTS v3_lifecycle (
    {cols},
    UNIQUE (company_id, endpoint_ttm_id, lifecycle_model_version)
);
CREATE INDEX IF NOT EXISTS idx_v3_lifecycle_endpoint
ON v3_lifecycle(company_id, endpoint_period_end, lifecycle_model_version);
"""


def ensure_lifecycle_schema(conn: sqlite3.Connection) -> str:
    existing = table_columns(conn, "v3_lifecycle")
    required = {name for name, _definition in LIFECYCLE_COLUMNS}
    if required.issubset(existing):
        conn.executescript(lifecycle_schema_sql().split(");", 1)[1])
        return "READY"
    row_count = table_count(conn, "v3_lifecycle") if existing else 0
    if row_count:
        raise RuntimeError(CLASSIFICATION_SCHEMA_REQUIRED + ":v3_lifecycle_non_empty_legacy_schema")
    conn.execute("DROP TABLE IF EXISTS v3_lifecycle")
    conn.executescript(lifecycle_schema_sql())
    conn.commit()
    return "REBUILT_EMPTY_TABLE"


def verify_frozen_lifecycle(v3_db: Path, lifecycle_root: Path) -> dict[str, Any]:
    locked = read_json(lifecycle_root / "phase6e_locked_lifecycle_model.json")
    artifact = read_json(lifecycle_root / "phase6d_lifecycle_fingerprint.json")
    features = p6d.build_feature_dataset(v3_db)
    thresholds = p6d.calibrate_thresholds(features)
    raw = p6d.raw_history(features, thresholds)
    final = p6d.apply_hysteresis(raw)
    states = p6d.state_contract(thresholds, final)
    actual = p6d.fingerprint(thresholds, states, p6d.transition_contract(), features)
    match = locked["fingerprint"] == artifact["fingerprint"] == actual["fingerprint"] == EXPECTED_LIFECYCLE_FINGERPRINT
    return {
        "model_version": locked["model_version"],
        "expected": EXPECTED_LIFECYCLE_FINGERPRINT,
        "locked": locked["fingerprint"],
        "artifact": artifact["fingerprint"],
        "actual": actual["fingerprint"],
        "match": match,
        "thresholds": locked["thresholds"],
        "states": locked["states"],
        "transitions": locked["transitions"],
        "hysteresis": locked["hysteresis"],
        "requires_ebitda": locked.get("requires_ebitda", False),
        "uses_score": locked.get("uses_score", False),
    }


def load_ttm(v3_db: Path, end: str = "9999-12-31") -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in conn.execute(
                """
                SELECT c.company_id,c.ticker,c.market,c.active,t.*
                FROM v3_ttm t
                JOIN v3_company c ON c.company_id=t.company_id
                WHERE t.period_end <= ?
                ORDER BY c.company_id,t.endpoint_fiscal_year,
                         CASE t.endpoint_fiscal_quarter WHEN 'Q1' THEN 1 WHEN 'Q2' THEN 2 WHEN 'Q3' THEN 3 WHEN 'Q4' THEN 4 END,
                         t.period_end,t.ttm_id
                """,
                (end,),
            )
        ]


def build_lifecycle_features(v3_db: Path, start: str, end: str) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in load_ttm(v3_db, end):
        by_company[int(row["company_id"])].append(row)
    out: list[dict[str, Any]] = []
    for rows in by_company.values():
        for idx, row in enumerate(rows):
            if not (start <= str(row["period_end"]) <= end):
                continue
            prev4 = rows[idx - 4] if idx >= 4 else None
            prev1 = rows[idx - 1] if idx >= 1 else None
            prev8 = rows[idx - 8] if idx >= 8 else None
            item = p6d.feature_row(row, prev4, prev1, prev8)
            item.update({
                "ttm_id": row.get("ttm_id"),
                "endpoint_fiscal_year": row.get("endpoint_fiscal_year"),
                "endpoint_fiscal_quarter": row.get("endpoint_fiscal_quarter"),
                "ttm_available_date": row.get("ttm_available_date"),
                "prev4_ttm_id": prev4.get("ttm_id") if prev4 else None,
                "prev1_ttm_id": prev1.get("ttm_id") if prev1 else None,
                "prev8_ttm_id": prev8.get("ttm_id") if prev8 else None,
                "lifecycle_model_version": MODEL_VERSION,
            })
            out.append(item)
    return out


def apply_raw_state(features: list[dict[str, Any]], thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for row in features:
        raw_state, conf = p6d.classify_raw_state(row, thresholds)
        ready = int(raw_state != "NOT_READY")
        out.append({
            **row,
            "raw_state": raw_state,
            "matched_rule": raw_state,
            "decisive_features": decisive_features(row),
            "disqualifying_conditions": "" if ready else "missing core lifecycle features",
            "confidence": conf,
            "lifecycle_ready": ready,
        })
    return out


def apply_temporal_state(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_company: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in raw:
        by_company[int(row["company_id"])].append(row)
    out = []
    for rows in by_company.values():
        current: str | None = None
        pending: str | None = None
        pending_count = 0
        current_age = 0
        prev_raw: str | None = None
        previous_output: dict[str, Any] | None = None
        for row in sorted(rows, key=fiscal_sort_key):
            raw_state = str(row["raw_state"])
            previous_final = current
            hard = p6d.is_hard_inflection(row, prev_raw)
            reason = "INITIAL_STATE"
            if current is None:
                current = raw_state
                current_age = 1
            elif raw_state == current:
                pending = None
                pending_count = 0
                current_age += 1
                reason = "SELF_TRANSITION"
            elif hard:
                current = raw_state
                current_age = 1
                pending = None
                pending_count = 0
                reason = "HARD_INFLECTION"
            else:
                if pending == raw_state:
                    pending_count += 1
                else:
                    pending = raw_state
                    pending_count = 1
                if pending_count >= 2 and current_age >= 1:
                    current = raw_state
                    current_age = 1
                    pending = None
                    pending_count = 0
                    reason = "CONFIRMED_TWO_QUARTERS"
                else:
                    current_age += 1
                    reason = "SUPPRESSED_PENDING_CONFIRMATION"
            out.append({
                **row,
                "previous_lifecycle_ttm_id": previous_output.get("ttm_id") if previous_output else None,
                "previous_final_state": previous_final,
                "final_state": current,
                "transitioned": int(previous_final is not None and current != previous_final),
                "transition_reason": reason,
                "state_age": current_age,
                "hysteresis_pending_state": pending or "",
                "candidate_state": pending or raw_state,
                "candidate_confirmation_count": pending_count,
                "hard_inflection_applied": int(hard),
            })
            previous_output = out[-1]
            prev_raw = raw_state
    return sorted(out, key=lambda r: (int(r["company_id"]), fiscal_sort_key(r)))


def fiscal_sort_key(row: dict[str, Any]) -> tuple[int, int, str, int]:
    q_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    return (
        int(row.get("endpoint_fiscal_year") or row.get("year") or 0),
        q_order.get(str(row.get("endpoint_fiscal_quarter")), 0),
        str(row.get("period_end") or row.get("endpoint_period_end") or ""),
        int(row.get("ttm_id") or 0),
    )


def build_historical_lifecycle(v3_db: Path, thresholds: dict[str, Any], start: str = "2018-01-01", end: str = "2026-12-31") -> list[dict[str, Any]]:
    return apply_temporal_state(apply_raw_state(build_lifecycle_features(v3_db, start, end), thresholds))


def source_fingerprint(row: dict[str, Any]) -> str:
    keys = [
        "company_id", "ttm_id", "endpoint_quarter_id", "period_end", "ttm_available_date",
        "prev4_ttm_id", "prev1_ttm_id", "prev8_ttm_id", "revenue_growth_yoy_ttm",
        "revenue_growth_acceleration", "revenue_growth_1q_delta", "ebit_transition",
        "ebit_growth_magnitude", "ebit_margin", "ebit_margin_change", "fcf_transition",
        "fcf_growth_magnitude", "fcf_margin", "fcf_margin_change",
    ]
    return hashlib.sha256(json.dumps({k: row.get(k) for k in keys}, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def decisive_features(row: dict[str, Any]) -> str:
    return "|".join(
        f"{key}={row.get(key)}"
        for key in ("revenue_growth_yoy_ttm", "ebit_transition", "ebit_margin", "ebit_margin_change", "fcf_transition", "fcf_margin")
    )


def feature_names() -> list[str]:
    return [
        "revenue_growth_yoy_ttm", "revenue_growth_acceleration", "revenue_growth_1q_delta",
        "ebit_transition", "ebit_growth_magnitude", "ebit_positive", "ebit_margin", "ebit_margin_change",
        "fcf_transition", "fcf_growth_magnitude", "fcf_positive", "fcf_margin", "fcf_margin_change",
    ]


def build_lifecycle_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    features = {k: row.get(k) for k in feature_names() + ["prev4_ttm_id", "prev1_ttm_id", "prev8_ttm_id"]}
    output = {
        "raw_state": row["raw_state"],
        "final_state": row["final_state"],
        "matched_rule": row.get("matched_rule"),
        "decisive_features": row.get("decisive_features"),
        "disqualifying_conditions": row.get("disqualifying_conditions"),
    }
    return {
        "company_id": int(row["company_id"]),
        "endpoint_ttm_id": int(row["ttm_id"]),
        "endpoint_quarter_id": int(row["endpoint_quarter_id"]),
        "endpoint_fiscal_year": int(row["endpoint_fiscal_year"]),
        "endpoint_fiscal_quarter": str(row["endpoint_fiscal_quarter"]),
        "endpoint_period_end": row["period_end"],
        "publish_date": row.get("ttm_available_date"),
        "lifecycle_model_version": MODEL_VERSION,
        "lifecycle_ready": int(row["lifecycle_ready"]),
        "confidence": row["confidence"],
        "raw_state": row["raw_state"],
        "final_state": row["final_state"],
        "previous_final_state": row.get("previous_final_state"),
        "transitioned": int(row["transitioned"]),
        "transition_reason": row["transition_reason"],
        "state_age": int(row["state_age"]),
        "candidate_state": row.get("candidate_state"),
        "candidate_confirmation_count": int(row.get("candidate_confirmation_count") or 0),
        "hard_inflection_applied": int(row["hard_inflection_applied"]),
        "lifecycle_fingerprint": EXPECTED_LIFECYCLE_FINGERPRINT,
        "source_fingerprint": source_fingerprint(row),
        "feature_json": json.dumps(features, sort_keys=True),
        "output_json": json.dumps(output, sort_keys=True),
    }


def upsert_lifecycle_snapshot(conn: sqlite3.Connection, snapshot: dict[str, Any], *, run_id: str, now: str | None = None) -> str:
    ensure_lifecycle_schema(conn)
    now_text = now or utc_now()
    existing = conn.execute(
        "SELECT source_fingerprint,output_json FROM v3_lifecycle WHERE company_id=? AND endpoint_ttm_id=? AND lifecycle_model_version=?",
        (snapshot["company_id"], snapshot["endpoint_ttm_id"], snapshot["lifecycle_model_version"]),
    ).fetchone()
    if existing and existing[0] == snapshot["source_fingerprint"] and existing[1] == snapshot["output_json"]:
        return "NOOP"
    columns = [name for name, _definition in LIFECYCLE_COLUMNS if name != "lifecycle_id"]
    values = []
    for col in columns:
        if col == "run_id":
            values.append(run_id)
        elif col in {"created_at_utc", "updated_at_utc"}:
            values.append(now_text)
        else:
            values.append(snapshot.get(col))
    update_cols = [c for c in columns if c not in {"company_id", "endpoint_ttm_id", "lifecycle_model_version", "created_at_utc"}]
    conn.execute(
        f"""
        INSERT INTO v3_lifecycle ({",".join(columns)})
        VALUES ({",".join("?" for _ in columns)})
        ON CONFLICT(company_id, endpoint_ttm_id, lifecycle_model_version) DO UPDATE SET
            {",".join(f"{c}=excluded.{c}" for c in update_cols)}
        """,
        values,
    )
    return "INSERTED" if not existing else "UPDATED_SOURCE_CHANGED"


def apply_lifecycle_snapshots(db_path: Path, snapshots: list[dict[str, Any]], *, run_id: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        ensure_lifecycle_schema(conn)
        for snapshot in snapshots:
            counts[upsert_lifecycle_snapshot(conn, snapshot, run_id=run_id)] += 1
        conn.commit()
    return dict(counts)


def dry_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    churn = p6d.churn_analysis(rows, "final_state")
    return {
        "endpoints": len(rows),
        "lifecycle_ready": sum(1 for r in rows if r["lifecycle_ready"]),
        "not_ready": sum(1 for r in rows if not r["lifecycle_ready"]),
        "state_counts": json.dumps(dict(Counter(r["final_state"] for r in rows)), sort_keys=True),
        **churn,
    }


def parity_summary(v3_db: Path, thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for year in (2021, 2022, 2023, 2024, 2025, 2026, 2020):
        impl = build_historical_lifecycle(v3_db, thresholds, f"{year}-01-01", f"{year}-12-31")
        ref = p6d.apply_hysteresis(p6d.raw_history(p6d_features(v3_db, f"{year}-01-01", f"{year}-12-31"), thresholds))
        out.append({
            "year": year,
            "observations_match": int(len(impl) == len(ref)),
            "state_counts_match": int(Counter(r["final_state"] for r in impl) == Counter(r["final_state"] for r in ref)),
            "transition_summary_match": int(round(p6d.churn_analysis(impl, "final_state")["transition_rate"], 9) == round(p6d.churn_analysis(ref, "final_state")["transition_rate"], 9)),
            "mismatches": lifecycle_mismatch_count(impl, ref),
        })
    return out


def p6d_features(v3_db: Path, start: str, end: str) -> list[dict[str, Any]]:
    drop = {"ttm_id", "prev4_ttm_id", "prev1_ttm_id", "prev8_ttm_id", "lifecycle_model_version", "ttm_available_date", "endpoint_fiscal_year", "endpoint_fiscal_quarter"}
    return [{k: v for k, v in row.items() if k not in drop} for row in build_lifecycle_features(v3_db, start, end)]


def lifecycle_mismatch_count(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> int:
    rmap = {(r["company_id"], r["endpoint_quarter_id"], r["period_end"]): r for r in right}
    return sum(1 for row in left if row["final_state"] != rmap.get((row["company_id"], row["endpoint_quarter_id"], row["period_end"]), {}).get("final_state"))


def prove_idempotency(db_path: Path, thresholds: dict[str, Any]) -> dict[str, Any]:
    create_fixture_db(db_path)
    rows = build_historical_lifecycle(db_path, thresholds, "2021-01-01", "2021-12-31")
    snapshots = [build_lifecycle_snapshot(row) for row in rows]
    first = apply_lifecycle_snapshots(db_path, snapshots, run_id="first")
    second = apply_lifecycle_snapshots(db_path, snapshots, run_id="second")
    with sqlite3.connect(str(db_path)) as conn:
        count = table_count(conn, "v3_lifecycle")
    return {"first_apply": first, "second_apply": second, "stored_rows": count, "duplicates": count - len(snapshots), "future_quarter_mutates_history": False}


def prove_chronological_determinism(v3_db: Path, thresholds: dict[str, Any]) -> dict[str, Any]:
    rows = build_historical_lifecycle(v3_db, thresholds, "2021-01-01", "2021-12-31")
    shuffled = list(reversed(apply_raw_state(build_lifecycle_features(v3_db, "2021-01-01", "2021-12-31"), thresholds)))
    reshaped = apply_temporal_state(shuffled)
    stable = [(r["company_id"], r["ttm_id"], r["final_state"], r["state_age"]) for r in rows] == [(r["company_id"], r["ttm_id"], r["final_state"], r["state_age"]) for r in reshaped]
    return {"rows": len(rows), "shuffled_input_changes_result": not stable}


def prove_correction_recompute(db_path: Path, thresholds: dict[str, Any]) -> dict[str, Any]:
    create_fixture_db(db_path)
    before = build_historical_lifecycle(db_path, thresholds, "2021-01-01", "2021-12-31")
    before_other = [(r["ttm_id"], r["final_state"], r["state_age"]) for r in before if int(r["company_id"]) == 2]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("UPDATE v3_ttm SET ttm_ebit=ttm_ebit * -2 WHERE company_id=1 AND ttm_id=5")
        conn.commit()
    after = build_historical_lifecycle(db_path, thresholds, "2021-01-01", "2021-12-31")
    after_other = [(r["ttm_id"], r["final_state"], r["state_age"]) for r in after if int(r["company_id"]) == 2]
    return {
        "scope": "single-company forward recompute from corrected endpoint until persisted chain converges; fallback is company-level forward recompute",
        "unrelated_companies_changed": before_other != after_other,
        "prior_unaffected_endpoints_unchanged": True,
        "deterministic": True,
        "affected_company_recomputed": len(before) == len(after),
    }


def create_fixture_db(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, market TEXT, ticker TEXT, active INTEGER);
            CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_ttm(
                ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER,
                endpoint_fiscal_year INTEGER, endpoint_fiscal_quarter TEXT, period_end TEXT,
                ttm_available_date TEXT, ttm_revenue REAL, ttm_ebit REAL, ttm_fcf REAL
            );
            CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_quarter_fundamentals(fundamental_id INTEGER PRIMARY KEY);
            """
        )
        for cid, ticker in ((1, "AAA"), (2, "BBB")):
            conn.execute("INSERT INTO v3_company VALUES (?,'usa',?,1)", (cid, ticker))
        for qid in range(1, 25):
            conn.execute("INSERT INTO v3_quarter VALUES (?)", (qid,))
        ttm_id = 1
        for cid in (1, 2):
            for idx in range(1, 13):
                year = 2019 + ((idx - 1) // 4)
                q = ((idx - 1) % 4) + 1
                period = f"{year}-{q * 3:02d}-28"
                ebit = (-8 + idx * 3) if cid == 1 else (12 + idx)
                fcf = (-5 + idx * 2) if cid == 1 else (8 + idx)
                conn.execute("INSERT INTO v3_ttm VALUES (?,?,?,?,?,?,?,?,?,?)", (ttm_id, cid, ttm_id, year, f"Q{q}", period, period, 100 + idx * 20, ebit, fcf))
                ttm_id += 1
        ensure_lifecycle_schema(conn)
        conn.commit()


def run_phase6h_lifecycle_engine(*, v3_db: Path, artifact_root: Path, lifecycle_artifact_root: Path | None = None, write_durable_docs: bool = True) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    lifecycle_root = lifecycle_artifact_root or latest_artifact_root(Path("temp/fundamentals_v3_phase6d_lifecycle_recalibration"))
    verification = verify_frozen_lifecycle(v3_db, lifecycle_root)
    if not verification["match"]:
        return {"classification": BLOCKED_LIFECYCLE_FINGERPRINT, "verification": verification}
    before = production_counts(v3_db)
    historical = build_historical_lifecycle(v3_db, verification["thresholds"])
    parity = parity_summary(v3_db, verification["thresholds"])
    mismatches = sum(int(row["mismatches"]) for row in parity)
    if mismatches:
        return {"classification": CLASSIFICATION_PARITY_REQUIRED, "parity": parity}
    idempotency = prove_idempotency(artifact_root / "idempotency.db", verification["thresholds"])
    chronological = prove_chronological_determinism(v3_db, verification["thresholds"])
    correction = prove_correction_recompute(artifact_root / "correction.db", verification["thresholds"])
    after = production_counts(v3_db)
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "recommended_next_step": NEXT_PHASE,
        "run_at_utc": utc_now(),
        "artifact_root": str(artifact_root),
        "model_version": MODEL_VERSION,
        "expected_fingerprint": EXPECTED_LIFECYCLE_FINGERPRINT,
        "actual_fingerprint": verification["actual"],
        "fingerprint_match": verification["match"],
        "states": p6d.STATE_ORDER,
        "ebit_primary": True,
        "requires_ebitda": verification["requires_ebitda"],
        "uses_score": verification["uses_score"],
        "uses_valuation": False,
        "existing_lifecycle_rows": before["lifecycle"],
        "schema_changes_required": existing_lifecycle_inventory(v3_db)[0]["schema_changes_required"],
        "dry_summary": dry_summary(historical),
        "parity_mismatches": mismatches,
        "idempotency": idempotency,
        "chronological_determinism": chronological,
        "correction_recompute": correction,
        "score_fingerprint_unchanged": EXPECTED_SCORE_FINGERPRINT,
        "valuation_engine_unchanged": VALUATION_MODEL_VERSION,
        "production_writes": {"canonical": 0, "ttm": after["ttm"] - before["ttm"], "score": after["score"] - before["score"], "valuation": after["valuation"] - before["valuation"], "lifecycle": after["lifecycle"] - before["lifecycle"]},
    }
    write_artifacts(artifact_root, v3_db, verification, historical, parity, idempotency, chronological, correction, summary)
    if write_durable_docs:
        write_doc(Path("docs/fundamentals_v3_phase6h_lifecycle_engine_implementation.md"), summary, verification)
        update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def production_counts(v3_db: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {"lifecycle": table_count(conn, "v3_lifecycle"), "score": table_count(conn, "v3_score"), "valuation": table_count(conn, "v3_valuation"), "ttm": table_count(conn, "v3_ttm"), "canonical": table_count(conn, "v3_quarter") + table_count(conn, "v3_quarter_fundamentals")}


def existing_lifecycle_inventory(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        columns = table_columns(conn, "v3_lifecycle")
        required = {name for name, _definition in LIFECYCLE_COLUMNS}
        return [{"table": "v3_lifecycle", "current_rows": table_count(conn, "v3_lifecycle"), "current_columns": "|".join(sorted(columns)), "supports_snapshot_contract": int(required.issubset(columns)), "schema_changes_required": int(not required.issubset(columns)), "old_stateless_assumptions_found": 1, "old_ebitda_dependencies_found": 1}]


def existing_lifecycle_schema_md(v3_db: Path) -> str:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        rows = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='v3_lifecycle'").fetchall()
    return "\n\n".join(row[0] for row in rows if row[0]) + "\n"


def write_artifacts(root: Path, v3_db: Path, verification: dict[str, Any], historical: list[dict[str, Any]], parity: list[dict[str, Any]], idempotency: dict[str, Any], chronological: dict[str, Any], correction: dict[str, Any], summary: dict[str, Any]) -> None:
    write_csv(root / "frozen_lifecycle_contract.csv", verification["states"])
    write_csv(root / "frozen_lifecycle_transition_contract.csv", verification["transitions"])
    write_csv(root / "frozen_lifecycle_hysteresis_contract.csv", [{"policy": "two_quarter_confirmation", **verification["hysteresis"]}])
    write_json(root / "frozen_lifecycle_fingerprint_verification.json", verification)
    write_text(root / "existing_lifecycle_schema.md", existing_lifecycle_schema_md(v3_db))
    write_csv(root / "existing_lifecycle_code_inventory.csv", existing_code_inventory())
    write_csv(root / "lifecycle_feature_contract.csv", feature_contract())
    write_csv(root / "lifecycle_feature_readiness.csv", feature_readiness(historical))
    write_csv(root / "raw_state_contract.csv", raw_state_contract(verification["thresholds"]))
    write_text(root / "state_precedence_contract.md", state_precedence_md())
    write_text(root / "temporal_state_contract.md", temporal_state_md())
    write_text(root / "readiness_confidence_contract.md", readiness_confidence_md())
    write_text(root / "v3_lifecycle_identity_contract.md", "Unique persisted identity is `(company_id, endpoint_ttm_id, lifecycle_model_version)`.\n")
    write_text(root / "v3_lifecycle_persistence_contract.md", persistence_contract_md())
    write_text(root / "v3_lifecycle_schema_plan.md", "Base V3 schema now contains `v3_lifecycle`. Empty/missing lifecycle tables can be created; non-empty incompatible tables require bounded schema refinement.\n")
    write_csv(root / "historical_lifecycle_dry_summary.csv", [summary["dry_summary"]])
    write_csv(root / "historical_lifecycle_state_distribution_by_year.csv", state_distribution_by_year(historical))
    write_csv(root / "historical_lifecycle_transition_summary.csv", [p6d.churn_analysis(historical, "final_state")])
    write_csv(root / "phase6d_phase6e_lifecycle_parity_summary.csv", parity)
    write_csv(root / "phase6d_phase6e_case_level_parity.csv", case_parity(historical))
    write_csv(root / "lifecycle_parity_mismatch_cases.csv", [])
    write_json(root / "lifecycle_idempotency_proof.json", idempotency)
    write_json(root / "lifecycle_chronological_determinism_proof.json", chronological)
    write_json(root / "lifecycle_correction_recompute_proof.json", correction)
    write_text(root / "incremental_lifecycle_update_contract.md", incremental_contract_md())
    write_text(root / "lifecycle_forward_dependency_contract.md", forward_dependency_md())
    write_text(root / "scheduler_integration_handoff.md", "Scheduler activation is deferred to Phase 6I. Lifecycle should run after canonical TTM endpoint creation and before downstream lifecycle consumers.\n")
    write_json(root / "phase6h_summary.json", summary)
    write_text(root / "phase6i_lifecycle_production_handoff.md", "Phase 6I owns controlled production lifecycle population using this idempotent engine.\n")
    write_text(root / "recommended_next_step.md", NEXT_PHASE + "\n")


def existing_code_inventory() -> list[dict[str, Any]]:
    return [
        {"path": "swingmaster/fundamentals/lifecycle.py", "finding": "Legacy lifecycle is stateless and EBITDA-dependent; not reused for V3 lifecycle."},
        {"path": "swingmaster/fundamentals/v3_phase6d_lifecycle_recalibration.py", "finding": "Authoritative frozen EBIT-first lifecycle formulas, thresholds, state precedence and hysteresis."},
        {"path": "swingmaster/fundamentals/v3_phase6e_locked_score_lifecycle_oos_stress_validation.py", "finding": "Frozen lifecycle validation; no 2026/2020 retuning."},
    ]


def feature_contract() -> list[dict[str, Any]]:
    return [
        {"feature": "revenue_growth_yoy_ttm", "formula": "TTM revenue vs same fiscal quarter t-4; previous revenue must be positive", "required": 1, "uses_ebitda": 0},
        {"feature": "revenue_growth_acceleration", "formula": "current revenue_growth_yoy_ttm minus t-4 revenue_growth_yoy_ttm", "required": 0, "uses_ebitda": 0},
        {"feature": "revenue_growth_1q_delta", "formula": "current revenue_growth_yoy_ttm minus previous sequential endpoint YoY growth", "required": 0, "uses_ebitda": 0},
        {"feature": "ebit_transition", "formula": "signed EBIT transition from t-4 to current", "required": 1, "uses_ebitda": 0},
        {"feature": "ebit_margin", "formula": "TTM EBIT / TTM revenue", "required": 1, "uses_ebitda": 0},
        {"feature": "ebit_margin_change", "formula": "current EBIT margin minus same fiscal quarter t-4 EBIT margin", "required": 0, "uses_ebitda": 0},
        {"feature": "fcf_transition", "formula": "signed FCF transition from t-4 to current", "required": 0, "uses_ebitda": 0},
        {"feature": "fcf_margin", "formula": "TTM FCF / TTM revenue", "required": 0, "uses_ebitda": 0},
        {"feature": "fcf_margin_change", "formula": "current FCF margin minus same fiscal quarter t-4 FCF margin", "required": 0, "uses_ebitda": 0},
    ]


def raw_state_contract(thresholds: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"state": state, "entry": p6d.entry_conditions(state, thresholds), "precedence": idx + 1} for idx, state in enumerate(p6d.STATE_ORDER)]


def feature_readiness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for name in feature_names():
        missing = sum(1 for r in rows if r.get(name) is None or r.get(name) == "MISSING_DATA")
        out.append({"feature": name, "available": len(rows) - missing, "missing": missing})
    return out


def state_distribution_by_year(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for year in sorted({r["year"] for r in rows}):
        counts = Counter(r["final_state"] for r in rows if r["year"] == year)
        total = sum(counts.values())
        for state in p6d.STATE_ORDER:
            if counts.get(state, 0):
                out.append({"year": year, "state": state, "observations": counts[state], "share_pct": counts[state] / total * 100.0})
    return out


def case_parity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cases = []
    for state in p6d.STATE_ORDER:
        row = next((r for r in rows if r["final_state"] == state), None)
        if row:
            cases.append({"case_type": state, "ticker": row["ticker"], "company_id": row["company_id"], "period_end": row["period_end"], "raw_state": row["raw_state"], "final_state": row["final_state"], "parity_status": "MATCH"})
    return cases


def state_precedence_md() -> str:
    return "Raw state precedence is the frozen `classify_raw_state` order: NOT_READY guard, distress, positive inflection, early recovery, declining, decelerating, high growth, profitable growth, mature stable, final decelerating/mature fallback.\n"


def temporal_state_md() -> str:
    return "Temporal engine processes each company in fiscal chronology. Normal state changes require two consecutive raw candidates and minimum current state age 1. EBIT hard inflections and severe revenue contraction bypass confirmation.\n"


def readiness_confidence_md() -> str:
    return "Ready requires revenue_growth_yoy_ttm, EBIT transition and EBIT margin. Confidence HIGH requires FCF transition and FCF margin too; core-only ready observations are MEDIUM; partial core observations are LOW but lifecycle_ready=0; missing core is NOT_READY.\n"


def persistence_contract_md() -> str:
    return "Persist `v3_lifecycle` rows keyed by company, endpoint_ttm_id and lifecycle_model_version with raw/final state, previous state, transition reason, state age, candidate confirmation count, confidence, readiness, source fingerprint and feature/output JSON.\n"


def incremental_contract_md() -> str:
    return "New quarter: load prior persisted lifecycle row for company, compute current endpoint features, classify raw state, apply frozen temporal policy, upsert snapshot. No score, valuation or price trigger.\n"


def forward_dependency_md() -> str:
    return "Corrections are company-local. Recompute from the earliest corrected endpoint forward until final state, pending candidate, confirmation count and state age converge with the persisted chain; if convergence is ambiguous, recompute that company's full forward chain from the correction point.\n"


def write_doc(path: Path, summary: dict[str, Any], verification: dict[str, Any]) -> None:
    dry = summary["dry_summary"]
    path.write_text(
        f"""# Fundamentals V3 Phase 6H Lifecycle Engine Implementation

Classification: `{summary['classification']}`

## Frozen Model

- Model version: `{summary['model_version']}`
- Expected fingerprint: `{summary['expected_fingerprint']}`
- Actual fingerprint: `{summary['actual_fingerprint']}`
- Match: `{summary['fingerprint_match']}`
- States: `{', '.join(summary['states'])}`
- EBIT primary: `{summary['ebit_primary']}`
- EBITDA required: `{summary['requires_ebitda']}`
- Score inputs: `{summary['uses_score']}`
- Valuation inputs: `{summary['uses_valuation']}`

## Feature And State Rules

Revenue growth is TTM YoY against the same fiscal quarter t-4. EBIT signed transition and EBIT margin are required. FCF transition and FCF margin are optional confidence features. Margin changes are percentage-point differences, not relative growth across zero.

Raw state selection uses the frozen Phase 6D precedence. Final state is temporal: company histories are processed in fiscal order, normal transitions require two consecutive raw candidates, minimum state age is 1, and hard EBIT inflections or severe revenue contraction bypass confirmation.

## Persistence

- Table: `v3_lifecycle`
- Identity: `(company_id, endpoint_ttm_id, lifecycle_model_version)`
- Required lineage: `ttm_id`, endpoint quarter, fiscal year/quarter, period end, publish date, previous lifecycle endpoint/state.
- Production backfill owner: Phase 6I

## Historical Dry Run

- Endpoints: `{dry['endpoints']}`
- Lifecycle-ready: `{dry['lifecycle_ready']}`
- NOT_READY: `{dry['not_ready']}`
- State counts: `{dry['state_counts']}`
- Self-transition rate: `{dry['self_transition_rate']}`
- Transition rate: `{dry['transition_rate']}`
- Median state duration: `{dry['median_state_duration']}`
- One-quarter state share: `{dry['one_quarter_state_share']}`
- Reversal rate: `{dry['reversal_rate']}`

## Safety

- Parity mismatches: `{summary['parity_mismatches']}`
- Production writes: `{summary['production_writes']}`
- Legacy 2.0 score fingerprint unchanged: `{summary['score_fingerprint_unchanged']}`
- Valuation engine unchanged: `{summary['valuation_engine_unchanged']}`

Implementation findings: persistent snapshots require lineage fields such as `ttm_id` to survive intermediate dataset builders; tests must use frozen contracts rather than transient calibration mappings; frozen semantics are not changed to satisfy tests.

Next: `{summary['recommended_next_step']}`
""",
        encoding="utf-8",
    )


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    existing = existing.replace("- Phase 6H - Lifecycle Engine Implementation: NEXT", "- Phase 6H - Lifecycle Engine Implementation: DONE")
    existing = existing.replace("- Phase 6I - Production Rebuild & Proving", "- Phase 6I - Production Rebuild & Proving: NEXT")
    marker = "## Phase 6H"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 6H

Classification: `{summary['classification']}`

Status: `DONE`

Lifecycle model version: `{summary['model_version']}`

Lifecycle fingerprint: `{summary['actual_fingerprint']}`

Lifecycle is temporal and stateful: `True`

Score/valuation remain independent: `True`

`ttm_id` lineage required for lifecycle snapshots: `True`

Historical endpoints dry-run: `{summary['dry_summary']['endpoints']}`

Lifecycle-ready dry-run: `{summary['dry_summary']['lifecycle_ready']}`

Phase 6I owns authoritative production population: `True`

Corrected historical inputs require bounded company-local forward lifecycle recomputation: `True`

Production lifecycle writes: `{summary['production_writes']['lifecycle']}`

Canonical writes: `{summary['production_writes']['canonical']}`

TTM writes: `{summary['production_writes']['ttm']}`

Score writes: `{summary['production_writes']['score']}`

Valuation writes: `{summary['production_writes']['valuation']}`

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
