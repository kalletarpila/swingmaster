from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase6i_production_rebuild as p6i
from swingmaster.fundamentals import v3_phase6h_lifecycle_engine as p6h


def test_exact_success_classification() -> None:
    assert p6i.CLASSIFICATION_COMPLETE == "FUNDAMENTALS_V3_PHASE6I_PRODUCTION_REBUILD_PROVEN_READY_FOR_PHASE6J"


def test_next_phase_constant() -> None:
    assert p6i.NEXT_PHASE == "MASTER PLAN PHASE 6J - PHASE 6 CLOSURE"


def test_idempotent_detector_rejects_insert() -> None:
    assert not p6i.is_idempotent({"valuation": {"apply": {"INSERTED": 1}}, "score": {"apply": {}}, "lifecycle": {"apply": {}}})


def test_idempotent_detector_accepts_noops() -> None:
    assert p6i.is_idempotent({"valuation": {"apply": {"NOOP": 1}}, "score": {"apply": {"NOOP": 1}}, "lifecycle": {"apply": {"NOOP": 1}}})


def test_source_equal() -> None:
    left = {"a": {"rows": 1, "fingerprint": "x"}}
    assert p6i.source_equal(left, dict(left))


def test_table_fingerprint_missing_table(tmp_path: Path) -> None:
    db = tmp_path / "empty.db"
    sqlite3.connect(db).close()
    assert p6i.table_fingerprint(db, "missing") == {"table": "missing", "exists": False, "rows": 0, "fingerprint": ""}


def test_schema_parity_after_ensure(tmp_path: Path) -> None:
    db = tmp_path / "schema.db"
    p6h.create_fixture_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS v3_quarter_fundamentals(fundamental_id INTEGER PRIMARY KEY)")
        conn.commit()
    p6i.apply_schema(db)
    assert all(p6i.schema_parity_check(db).values())


def test_duplicate_check(tmp_path: Path) -> None:
    db = tmp_path / "dupes.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE x(a INTEGER, b TEXT)")
        conn.executemany("INSERT INTO x VALUES (?,?)", [(1, "a"), (1, "a"), (2, "b")])
        conn.commit()
    assert p6i.duplicate_check(db, "x", "a,b")["duplicates"] == 1


def test_rollback_plan_records_backup_path() -> None:
    text = p6i.rollback_plan_md({"path": "/tmp/backup.db", "sha256": "abc"})
    assert "/tmp/backup.db" in text
    assert "abc" in text


def test_incremental_proving_keeps_pending_price_policy() -> None:
    assert p6i.incremental_proving_summary()["valuation_pending_price_behavior"] == "PENDING_PRICE_DATE"
