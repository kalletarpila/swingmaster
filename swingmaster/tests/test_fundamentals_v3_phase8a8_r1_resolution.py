from __future__ import annotations

import csv
import sqlite3

from swingmaster.fundamentals.v3_phase8a8_r1_resolution import (
    FOUR_REPAIRS,
    apply_four,
    external_queue,
    identity_guard,
    rebuild_residuals,
)


def make_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY,ticker TEXT);
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,company_id INTEGER,fiscal_year INTEGER,fiscal_quarter TEXT,period_end_date TEXT,publish_date TEXT,updated_at_utc TEXT);
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY,revenue REAL,cash REAL,accepted_source_provider TEXT,accepted_at_utc TEXT,update_run_id TEXT,updated_at_utc TEXT);
        INSERT INTO v3_company VALUES (1,'POWW'),(2,'RH'),(3,'VTGN'),(4,'TBLA');
        INSERT INTO v3_quarter VALUES (10,1,2025,'Q1','2025-03-31','2025-06-16','t0');
        INSERT INTO v3_quarter VALUES (11,1,2024,'Q2','2024-06-30','2024-08-08','t0');
        INSERT INTO v3_quarter VALUES (20,2,2021,'Q4','2021-05-01','2021-06-09','t0');
        INSERT INTO v3_quarter VALUES (30,3,2025,'Q1','2025-03-31','2025-06-17','t0');
        INSERT INTO v3_quarter VALUES (40,4,2022,'Q3','2022-09-30','2022-11-09','t0');
        INSERT INTO v3_quarter_fundamentals VALUES (10,-42159090,30227796,'YAHOO',NULL,NULL,'t0');
        INSERT INTO v3_quarter_fundamentals VALUES (11,30953550,1,'YAHOO',NULL,NULL,'t0');
        INSERT INTO v3_quarter_fundamentals VALUES (20,-7453000,229527000,'YAHOO',NULL,NULL,'t0');
        INSERT INTO v3_quarter_fundamentals VALUES (30,-15000,67131000,'YAHOO',NULL,NULL,'t0');
        INSERT INTO v3_quarter_fundamentals VALUES (40,332462000,-445000,'YAHOO',NULL,NULL,'t0');
        """
    )
    conn.commit()
    return conn


def write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def test_four_verified_repairs_pass_identity_and_apply(tmp_path) -> None:
    conn = make_db(tmp_path / "v3.db")
    guarded = [identity_guard(conn, repair) for repair in FOUR_REPAIRS]
    assert [row["guard_ok"] for row in guarded] == [1, 1, 1, 1]
    assert guarded[0]["identity_guard"] == "PASS_WITH_PERIOD_COLLISION_OBSERVED"
    audit = apply_four(conn, guarded, "TEST")
    assert sum(1 for row in audit if row["apply_status"] == "APPLIED") == 4
    assert conn.execute("SELECT period_end_date FROM v3_quarter WHERE quarter_id=10").fetchone()[0] == "2024-06-30"
    assert conn.execute("SELECT revenue FROM v3_quarter_fundamentals WHERE quarter_id=20").fetchone()[0] == 902741000
    assert conn.execute("SELECT cash FROM v3_quarter_fundamentals WHERE quarter_id=40").fetchone()[0] == 188477000


def test_old_value_guard_blocks_stale_case(tmp_path) -> None:
    conn = make_db(tmp_path / "v3.db")
    conn.execute("UPDATE v3_quarter_fundamentals SET revenue=1 WHERE quarter_id=10")
    guarded = identity_guard(conn, FOUR_REPAIRS[0])
    assert guarded["guard_ok"] == 0
    assert guarded["identity_guard"] == "FAIL_OLD_VALUE_OR_PERIOD_MISMATCH"


def test_rebuild_residuals_removes_repaired_cases_and_excludes_r2_r3_from_external_queue(tmp_path) -> None:
    root = tmp_path / "a7"
    root.mkdir()
    write_csv(
        root / "residual_R1.csv",
        [
            {"ticker": "POWW", "fiscal_year": "2025", "fiscal_quarter": "Q1", "field": "revenue", "post_a7_classification": "FISCAL_IDENTITY_CONFLICT"},
            {"ticker": "AMST", "fiscal_year": "2024", "fiscal_quarter": "Q4", "residual_type": "PERIOD_END", "current_period_end": "2024-12-31", "verified_period_end": "2024-06-30", "post_a7_classification": "PERIOD_END_OUTSIDE_TOLERANCE_RETAINED"},
            {"ticker": "FNGR", "fiscal_year": "2020", "fiscal_quarter": "Q3", "residual_type": "PERIOD_END", "current_period_end": "2020-11-30", "verified_period_end": "2019-11-30", "post_a7_classification": "PERIOD_END_OUTSIDE_TOLERANCE_RETAINED"},
        ],
    )
    write_csv(root / "residual_R2.csv", [{"ticker": "R2", "fiscal_year": "2025", "fiscal_quarter": "Q1"}])
    write_csv(root / "residual_R3.csv", [{"ticker": "R3", "fiscal_year": "2020", "fiscal_quarter": "Q1"}])
    r1, r2, r3, _ = rebuild_residuals(root, [{"ticker": "POWW", "fiscal_year": 2025, "fiscal_quarter": "Q1", "field": "revenue", "apply_status": "APPLIED"}])
    assert [row["ticker"] for row in r1] == ["AMST"]
    assert any(row["ticker"] == "FNGR" for row in r3)
    queue = external_queue(r1)
    assert len(queue) == 1
    assert queue[0]["Ticker"] == "AMST"
