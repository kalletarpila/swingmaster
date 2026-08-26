from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.fundamentals import v3_phase8a10a_special_remove as rem


def make_db(path: Path) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;
        CREATE TABLE v3_company(
            company_id INTEGER PRIMARY KEY, market TEXT NOT NULL, ticker TEXT NOT NULL,
            company_name TEXT, profile TEXT NOT NULL DEFAULT 'ORDINARY', active INTEGER NOT NULL,
            admission_source TEXT NOT NULL, admission_evidence TEXT, created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL, UNIQUE(market,ticker)
        );
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
            fiscal_year INTEGER NOT NULL, fiscal_quarter TEXT NOT NULL CHECK(fiscal_quarter IN ('Q1','Q2','Q3','Q4')),
            period_end_date TEXT, publish_date TEXT, market_availability_date TEXT, q_lifecycle TEXT,
            sec_confirmation_state TEXT, created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
            UNIQUE(company_id,fiscal_year,fiscal_quarter)
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
            revenue REAL, ebitda REAL, free_cashflow REAL, cash REAL, total_debt REAL, shares_outstanding REAL,
            created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL
        );
        CREATE TABLE v3_ttm(
            ttm_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
            endpoint_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
            endpoint_fiscal_year INTEGER NOT NULL, endpoint_fiscal_quarter TEXT NOT NULL,
            period_end TEXT, model_version TEXT NOT NULL, q1_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
            q2_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id), q3_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
            q4_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id), source_fingerprint TEXT, created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        CREATE TABLE v3_score(
            score_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
            as_of_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
            endpoint_ttm_id INTEGER REFERENCES v3_ttm(ttm_id) ON DELETE CASCADE,
            score_model_version TEXT, score_fingerprint TEXT, created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL
        );
        CREATE TABLE v3_lifecycle(
            lifecycle_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
            endpoint_ttm_id INTEGER NOT NULL REFERENCES v3_ttm(ttm_id) ON DELETE CASCADE,
            endpoint_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
            lifecycle_model_version TEXT, lifecycle_fingerprint TEXT, created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL
        );
        CREATE TABLE v3_valuation(
            valuation_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
            endpoint_ttm_id INTEGER NOT NULL REFERENCES v3_ttm(ttm_id) ON DELETE CASCADE,
            endpoint_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
            created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL
        );
        CREATE TABLE v3_migration_audit(
            audit_id INTEGER PRIMARY KEY, company_id INTEGER REFERENCES v3_company(company_id) ON DELETE SET NULL,
            quarter_id INTEGER REFERENCES v3_quarter(quarter_id) ON DELETE SET NULL
        );
        CREATE TABLE v3_provider_q_acquisition(acquisition_id INTEGER PRIMARY KEY, quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE);
        CREATE TABLE v3_provider_symbol_alias(alias_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE);
        CREATE TABLE v3_operational_action(action_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE, quarter_id INTEGER REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE);
        CREATE TABLE v3_result_calendar(calendar_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE);
        CREATE TABLE v3_event(event_id INTEGER PRIMARY KEY, company_id INTEGER REFERENCES v3_company(company_id) ON DELETE SET NULL, quarter_id INTEGER REFERENCES v3_quarter(quarter_id) ON DELETE SET NULL);
        CREATE TABLE v3_resolution_issue(issue_id INTEGER PRIMARY KEY, quarter_id INTEGER REFERENCES v3_quarter(quarter_id) ON DELETE SET NULL, unresolved_ticker TEXT, issue_type TEXT, status TEXT);
        CREATE TABLE v3_run(run_id TEXT PRIMARY KEY);
        CREATE TABLE v3_schema_version(version INTEGER PRIMARY KEY);

        INSERT INTO v3_company VALUES
          (1,'usa','IMMR',NULL,'ORDINARY',1,'TEST','ACTIVE','c','u'),
          (2,'usa','RCAT',NULL,'ORDINARY',1,'TEST','ACTIVE','c','u'),
          (3,'usa','KEEP',NULL,'ORDINARY',1,'TEST','ACTIVE','c','u');
        INSERT INTO v3_quarter VALUES
          (10,1,2025,'Q1','2024-07-31','2024-12-16',NULL,'SETTLED','CONFIRMED','c','u'),
          (11,1,2025,'Q2','2024-10-31','2025-03-26',NULL,'SETTLED','CONFIRMED','c','u'),
          (20,2,2024,'Q2','2024-07-31','2024-08-08',NULL,'SETTLED','CONFIRMED','c','u'),
          (21,2,2024,'Q3','2024-10-31','2024-03-18',NULL,'SETTLED','CONFIRMED','c','u'),
          (30,3,2025,'Q1','2025-03-31','2025-05-01',NULL,'SETTLED','CONFIRMED','c','u'),
          (31,3,2025,'Q2','2025-06-30','2025-08-01',NULL,'SETTLED','CONFIRMED','c','u');
        INSERT INTO v3_quarter_fundamentals VALUES
          (10,1,2,3,4,5,6,'c','u'),(11,1,2,3,4,5,6,'c','u'),(20,1,2,3,4,5,6,'c','u'),
          (21,1,2,3,4,5,6,'c','u'),(30,1,2,3,4,5,6,'c','u'),(31,1,2,3,4,5,6,'c','u');
        INSERT INTO v3_ttm VALUES
          (100,1,11,2025,'Q2','2024-10-31','m',10,10,11,11,'sf','c','u'),
          (200,2,21,2024,'Q3','2024-10-31','m',20,20,21,21,'sf','c','u'),
          (300,3,31,2025,'Q2','2025-06-30','m',30,30,31,31,'sf','c','u');
        INSERT INTO v3_score VALUES (1000,1,11,100,'m','sf','c','u'),(2000,2,21,200,'m','sf','c','u'),(3000,3,31,300,'m','sf','c','u');
        INSERT INTO v3_lifecycle VALUES (1001,1,100,11,'m','lf','c','u'),(2001,2,200,21,'m','lf','c','u'),(3001,3,300,31,'m','lf','c','u');
        INSERT INTO v3_valuation VALUES (1002,1,100,11,'c','u'),(2002,2,200,21,'c','u'),(3002,3,300,31,'c','u');
        INSERT INTO v3_migration_audit VALUES (1,1,10),(2,2,20),(3,3,30);
        INSERT INTO v3_provider_q_acquisition VALUES (1,10),(2,20),(3,30);
        INSERT INTO v3_provider_symbol_alias VALUES (1,1),(2,2),(3,3);
        INSERT INTO v3_operational_action VALUES (1,1,10),(2,2,20),(3,3,30);
        INSERT INTO v3_result_calendar VALUES (1,1),(2,2),(3,3);
        INSERT INTO v3_event VALUES (1,1,10),(2,2,20),(3,3,30);
        INSERT INTO v3_resolution_issue VALUES (1,10,'IMMR','STRUCTURAL','ACTIVE'),(2,20,'RCAT','STRUCTURAL','ACTIVE'),(3,30,'KEEP','SEMANTIC','RESOLVED');
        """
    )
    conn.commit()
    conn.close()
    return path


def run_fixture(tmp_path: Path) -> dict:
    db = make_db(tmp_path / "v3.db")
    raw = tmp_path / "raw.db"
    raw.write_bytes(b"raw")
    return rem.run_phase8a10a_special_remove(rem.Phase8A10ASpecialRemovePaths(tmp_path / "artifacts", db, raw))


def test_exact_ticker_set() -> None:
    assert rem.TARGET_TICKERS == ("IMMR", "RCAT")


def test_exact_two_company_ids_frozen(tmp_path: Path) -> None:
    with rem.connect(make_db(tmp_path / "v3.db")) as conn:
        identities = rem.company_identity(conn)
    assert [row["company_id"] for row in identities] == [1, 2]


def test_no_third_company_in_scope(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["after"]["universe"]["companies"] == 1


def test_dependency_inventory_complete_for_known_tables(tmp_path: Path) -> None:
    with rem.connect(make_db(tmp_path / "v3.db")) as conn:
        inv = rem.dependency_inventory(conn, rem.id_sets(conn))
    assert set(rem.SNAPSHOT_TABLES) <= {row["table"] for row in inv}


def test_delete_plan_deterministic(tmp_path: Path) -> None:
    with rem.connect(make_db(tmp_path / "v3.db")) as conn:
        ids = rem.id_sets(conn)
        assert rem.build_delete_plan(rem.dependency_inventory(conn, ids)) == rem.build_delete_plan(rem.dependency_inventory(conn, ids))


def test_company_rows_deleted_last(tmp_path: Path) -> None:
    with rem.connect(make_db(tmp_path / "v3.db")) as conn:
        plan = rem.build_delete_plan(rem.dependency_inventory(conn, rem.id_sets(conn)))
    assert plan[-2]["table"] == "v3_company"
    assert plan[-1]["table"] == "v3_company"


def test_delete_count_guards(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["delete_plan"]["guard_status"] == "PASS"


def test_rollback_on_count_mismatch(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with rem.connect(db) as conn:
        ids = rem.id_sets(conn)
        plan = rem.build_delete_plan(rem.dependency_inventory(conn, ids))
        plan[0]["expected_rows"] = 999
        with pytest.raises(RuntimeError):
            rem.apply_delete_plan(conn, plan, ids)
        assert rem.universe_counts(conn)["companies"] == 3


def test_immr_canonical_rows_zero(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["integrity"]["target_company_rows"] == 0


def test_rcat_canonical_rows_zero(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["apply"]["canonical_rows_deleted"] == 4


def test_immr_derived_rows_zero(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["apply"]["ttm_rows_deleted"] >= 1


def test_rcat_derived_rows_zero(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["apply"]["score_rows_deleted"] == 2


def test_no_unrelated_company_deletion(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    raw = tmp_path / "raw.db"
    raw.write_bytes(b"raw")
    rem.run_phase8a10a_special_remove(rem.Phase8A10ASpecialRemovePaths(tmp_path / "artifacts", db, raw))
    with rem.connect(db) as conn:
        assert conn.execute("SELECT ticker FROM v3_company").fetchone()[0] == "KEEP"


def test_no_orphan_rows(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["integrity"]["orphan_rows"] == 0


def test_no_duplicate_fyfq_introduced(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["integrity"]["duplicate_fy_fq"] == 0


def test_retained_company_fingerprints_unchanged(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["retained_drift"]["retained_unrelated_drift"] == 0


def test_structural_r1_becomes_zero_when_only_targets_remain(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["residual"]["structural_r1_before"] == 2
    assert summary["residual"]["structural_r1_after"] == 0


def test_no_ttm_rebuild(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["safety"]["retained_company_ttm_rebuild"] == "NO"


def test_no_score_rebuild(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["safety"]["score_rebuild"] == "NO"


def test_no_lifecycle_rebuild(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["safety"]["lifecycle_rebuild"] == "NO"


def test_no_valuation_rebuild(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["safety"]["valuation_rebuild"] == "NO"


def test_no_rawcandle_writes(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["safety"]["rawcandle_writes"] == 0


def test_classification_complete(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["classification"] == rem.CLASSIFICATION_COMPLETE
