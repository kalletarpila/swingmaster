from __future__ import annotations

import csv
import sqlite3

import pytest

from swingmaster.fundamentals.v3_phase8_update_v3 import (
    CLASSIFICATION_MANUAL,
    Phase8Paths,
    age_bucket,
    priority,
    run_phase8_diagnosis,
)


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def make_db(path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE v3_company (
            company_id INTEGER PRIMARY KEY,
            market TEXT NOT NULL,
            ticker TEXT NOT NULL,
            company_name TEXT,
            profile TEXT NOT NULL DEFAULT 'ORDINARY',
            active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE v3_quarter (
            quarter_id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL,
            fiscal_year INTEGER NOT NULL,
            fiscal_quarter TEXT NOT NULL,
            period_end_date TEXT,
            publish_date TEXT,
            market_availability_date TEXT
        );
        CREATE TABLE v3_quarter_fundamentals (
            quarter_id INTEGER PRIMARY KEY,
            revenue REAL,
            cash REAL,
            total_debt REAL,
            shares_outstanding REAL,
            accepted_source_provider TEXT,
            derivation_method TEXT,
            currency TEXT
        );
        CREATE TABLE v3_migration_audit (
            audit_id INTEGER PRIMARY KEY,
            source TEXT,
            audit_type TEXT,
            decision TEXT,
            quarter_id INTEGER,
            evidence_json TEXT,
            created_at_utc TEXT
        );
        CREATE TABLE v3_ttm (
            ttm_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            endpoint_quarter_id INTEGER,
            period_end TEXT,
            q1_quarter_id INTEGER,
            q2_quarter_id INTEGER,
            q3_quarter_id INTEGER,
            q4_quarter_id INTEGER,
            source_fingerprint TEXT
        );
        CREATE TABLE v3_score (
            score_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            as_of_quarter_id INTEGER,
            endpoint_ttm_id INTEGER,
            source_fingerprint TEXT,
            score_model_version TEXT,
            score_fingerprint TEXT
        );
        CREATE TABLE v3_lifecycle (
            lifecycle_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            endpoint_ttm_id INTEGER,
            endpoint_quarter_id INTEGER,
            endpoint_period_end TEXT,
            source_fingerprint TEXT,
            lifecycle_model_version TEXT,
            lifecycle_fingerprint TEXT
        );
        CREATE TABLE v3_valuation (
            valuation_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            endpoint_ttm_id INTEGER,
            endpoint_quarter_id INTEGER,
            valuation_date TEXT,
            source_fingerprint TEXT
        );
        INSERT INTO v3_company VALUES (1,'usa','TST','Test Inc','ORDINARY',1);
        INSERT INTO v3_company VALUES (2,'usa','OLD','Old Inc','ORDINARY',0);
        INSERT INTO v3_quarter VALUES (1,1,2025,'Q1','2025-03-31','2025-02-01',NULL);
        INSERT INTO v3_quarter VALUES (2,1,2026,'Q1','2026-03-31','2026-02-01',NULL);
        INSERT INTO v3_quarter VALUES (3,2,2020,'Q1','2020-03-31','2020-04-30',NULL);
        INSERT INTO v3_quarter_fundamentals VALUES (1,-1,10,0,100,'V2','DIRECT','USD');
        INSERT INTO v3_quarter_fundamentals VALUES (2,1,10,0,100,'V2','DIRECT','USD');
        INSERT INTO v3_quarter_fundamentals VALUES (3,-1,10,0,100,'V2','DIRECT','USD');
        INSERT INTO v3_migration_audit VALUES (1,'V2','FIELD','ACCEPTED',1,'{"field":"publish_date"}','2026-01-01');
        INSERT INTO v3_ttm VALUES (1,1,1,'2025-03-31',1,1,1,1,'t');
        INSERT INTO v3_score VALUES (1,1,1,1,'s','V3_LEGACY2_FUNDAMENTAL_SCORE_V1','8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0');
        INSERT INTO v3_lifecycle VALUES (1,1,1,1,'2025-03-31','l','V3_LIFECYCLE_EBIT_FIRST_V1','18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e');
        INSERT INTO v3_valuation VALUES (1,1,1,1,'2025-04-01','v');
        """
    )
    conn.commit()
    conn.close()


def test_priority_buckets_cover_phase8_policy() -> None:
    assert age_bucket("2020-12-31") == "<=2020"
    assert age_bucket("2021-12-31") == "2021-2022"
    assert age_bucket("2026-03-31") == "2026"
    assert priority("2025", latest_impact=False, derived_impact=True) == "P1_CURRENT_MATERIAL"
    assert priority("2021-2022", latest_impact=False, derived_impact=True) == "P2_HISTORICAL_MATERIAL"
    assert priority("2026", latest_impact=True, derived_impact=True) == "P3_RECENT_UNCERTAIN"
    assert priority("<=2020", latest_impact=False, derived_impact=False) == "P4_LOW_CURRENT_MATERIALITY"
    assert priority("<=2020", latest_impact=True, derived_impact=False) == "P1_CURRENT_MATERIAL"


def test_phase8_diagnosis_maps_exact_identity_and_blocks_apply(tmp_path) -> None:
    phase7 = tmp_path / "phase7"
    out = tmp_path / "phase8"
    phase7.mkdir()
    db = tmp_path / "v3.db"
    raw = tmp_path / "raw.db"
    sqlite3.connect(raw).execute("CREATE TABLE osakedata(osake TEXT,pvm TEXT,close REAL,market TEXT)").connection.close()
    make_db(db)
    write_csv(
        phase7 / "canonical_publish_date_anomalies.csv",
        [{"fiscal_quarter": "Q1", "fiscal_year": "2025", "market_availability_date": "", "period_end_date": "2025-03-31", "publish_date": "2025-02-01", "quarter_id": "1", "ticker": "TST"}],
    )
    write_csv(
        phase7 / "field_semantic_outliers.csv",
        [{"cash": "10", "fiscal_quarter": "Q1", "fiscal_year": "2025", "period_end_date": "2025-03-31", "revenue": "-1", "shares_outstanding": "100", "ticker": "TST", "total_debt": "0"}],
    )
    summary = run_phase8_diagnosis(Phase8Paths(phase7, out, db, raw))
    assert summary["classification"] == CLASSIFICATION_MANUAL
    assert summary["phase7_publish_date_anomalies_ingested"] == 1
    assert summary["phase7_semantic_outliers_ingested"] == 1
    master = (out / "phase8_master_anomaly_table.csv").read_text(encoding="utf-8")
    assert "TST" in master
    assert "2025" in master
    assert (out / "phase8_frozen_repair_set.csv").read_text(encoding="utf-8").strip() == ""
    assert "Production apply is blocked" in (out / "manual_review_summary.md").read_text(encoding="utf-8")


def test_ambiguous_semantic_quarter_is_rejected(tmp_path) -> None:
    phase7 = tmp_path / "phase7"
    out = tmp_path / "phase8"
    phase7.mkdir()
    db = tmp_path / "v3.db"
    raw = tmp_path / "raw.db"
    sqlite3.connect(raw).execute("CREATE TABLE osakedata(osake TEXT,pvm TEXT,close REAL,market TEXT)").connection.close()
    make_db(db)
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO v3_quarter VALUES (4,1,2025,'Q1','2025-03-31','2025-02-01',NULL)")
    conn.commit()
    conn.close()
    write_csv(
        phase7 / "canonical_publish_date_anomalies.csv",
        [{"fiscal_quarter": "Q1", "fiscal_year": "2025", "market_availability_date": "", "period_end_date": "2025-03-31", "publish_date": "2025-02-01", "quarter_id": "1", "ticker": "TST"}],
    )
    write_csv(
        phase7 / "field_semantic_outliers.csv",
        [{"cash": "10", "fiscal_quarter": "Q1", "fiscal_year": "2025", "period_end_date": "2025-03-31", "revenue": "-1", "shares_outstanding": "100", "ticker": "TST", "total_debt": "0"}],
    )
    with pytest.raises(ValueError, match="ambiguous semantic finding"):
        run_phase8_diagnosis(Phase8Paths(phase7, out, db, raw))


def test_no_broad_rebuild_or_backup_before_manual_gate(tmp_path) -> None:
    phase7 = tmp_path / "phase7"
    out = tmp_path / "phase8"
    phase7.mkdir()
    db = tmp_path / "v3.db"
    raw = tmp_path / "raw.db"
    sqlite3.connect(raw).execute("CREATE TABLE osakedata(osake TEXT,pvm TEXT,close REAL,market TEXT)").connection.close()
    make_db(db)
    write_csv(
        phase7 / "canonical_publish_date_anomalies.csv",
        [{"fiscal_quarter": "Q1", "fiscal_year": "2020", "market_availability_date": "", "period_end_date": "2020-03-31", "publish_date": "2020-04-30", "quarter_id": "3", "ticker": "OLD"}],
    )
    write_csv(
        phase7 / "field_semantic_outliers.csv",
        [{"cash": "10", "fiscal_quarter": "Q1", "fiscal_year": "2020", "period_end_date": "2020-03-31", "revenue": "-1", "shares_outstanding": "100", "ticker": "OLD", "total_debt": "0"}],
    )
    summary = run_phase8_diagnosis(Phase8Paths(phase7, out, db, raw))
    assert summary["production_apply_performed"] is False
    backup = (out / "backup_manifest.json").read_text(encoding="utf-8")
    assert '"backup_created": false' in backup
    scope = (out / "phase8_repair_scope_summary.json").read_text(encoding="utf-8")
    assert '"expected_canonical_update_rows": 0' in scope

