from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from swingmaster.fundamentals.v3_phase7_check_v3 import (
    EXPECTED_LIFECYCLE_FINGERPRINT,
    EXPECTED_LIFECYCLE_MODEL,
    EXPECTED_SCORE_FINGERPRINT,
    EXPECTED_SCORE_MODEL,
    Issue,
    audit_canonical,
    audit_lifecycle,
    audit_score,
    audit_ttm,
    audit_valuation,
    issue_rows,
    open_ro,
)


def make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
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
            gross_profit REAL,
            operating_income REAL,
            ebit REAL,
            ebitda REAL,
            net_income REAL,
            operating_cashflow REAL,
            capex REAL,
            free_cashflow REAL,
            cash REAL,
            total_debt REAL,
            shares_outstanding REAL
        );
        CREATE TABLE v3_ttm (
            ttm_id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL,
            endpoint_quarter_id INTEGER NOT NULL,
            endpoint_fiscal_year INTEGER NOT NULL,
            endpoint_fiscal_quarter TEXT NOT NULL,
            period_end TEXT,
            model_version TEXT NOT NULL,
            ttm_revenue REAL,
            ttm_gross_profit REAL,
            ttm_operating_income REAL,
            ttm_ebit REAL,
            ttm_ebitda REAL,
            ttm_net_income REAL,
            ttm_ocf REAL,
            ttm_capex REAL,
            ttm_fcf REAL,
            cash REAL,
            total_debt REAL,
            shares_outstanding REAL,
            core_ttm_ebit_ready INTEGER,
            core_ttm_ebitda_ready INTEGER,
            ttm_available_date TEXT,
            ttm_pit_ready INTEGER,
            underlying_publish_dates_complete INTEGER,
            q1_quarter_id INTEGER,
            q2_quarter_id INTEGER,
            q3_quarter_id INTEGER,
            q4_quarter_id INTEGER
        );
        CREATE TABLE v3_score (
            score_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            as_of_quarter_id INTEGER,
            endpoint_ttm_id INTEGER,
            score_model_version TEXT,
            score_ready INTEGER,
            fundamental_score REAL,
            total_max_score INTEGER,
            coverage_pct REAL,
            confidence TEXT,
            applicability TEXT,
            score_fingerprint TEXT
        );
        CREATE TABLE v3_lifecycle (
            lifecycle_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            endpoint_ttm_id INTEGER,
            endpoint_quarter_id INTEGER,
            endpoint_fiscal_year INTEGER,
            endpoint_fiscal_quarter TEXT,
            endpoint_period_end TEXT,
            lifecycle_model_version TEXT,
            lifecycle_ready INTEGER,
            confidence TEXT,
            raw_state TEXT,
            final_state TEXT,
            previous_final_state TEXT,
            transitioned INTEGER,
            transition_reason TEXT,
            state_age INTEGER,
            lifecycle_fingerprint TEXT
        );
        CREATE TABLE v3_valuation (
            valuation_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            endpoint_ttm_id INTEGER,
            endpoint_quarter_id INTEGER,
            endpoint_fiscal_year INTEGER,
            endpoint_fiscal_quarter TEXT,
            endpoint_period_end TEXT,
            publish_date TEXT,
            valuation_date TEXT,
            valuation_close_price REAL,
            price_source TEXT,
            shares_outstanding REAL,
            market_cap REAL,
            cash REAL,
            total_debt REAL,
            net_debt REAL,
            enterprise_value REAL,
            ttm_revenue REAL,
            ttm_ebit REAL,
            ttm_fcf REAL,
            valuation_ready INTEGER,
            valuation_status TEXT,
            model_version TEXT
        );
        """
    )
    return conn


def seed_company_quarters(conn: sqlite3.Connection) -> None:
    conn.execute("INSERT INTO v3_company(company_id,market,ticker,company_name) VALUES (1,'usa','TST','Test Inc')")
    quarters = [
        (1, 1, 2025, "Q1", "2024-03-31", "2024-05-01", "2024-05-02"),
        (2, 1, 2025, "Q2", "2024-06-30", "2024-08-01", "2024-08-02"),
        (3, 1, 2025, "Q3", "2024-09-30", "2024-11-01", "2024-11-04"),
        (4, 1, 2025, "Q4", "2024-12-31", "2025-02-01", "2025-02-03"),
    ]
    conn.executemany("INSERT INTO v3_quarter VALUES (?,?,?,?,?,?,?)", quarters)
    conn.executemany(
        "INSERT INTO v3_quarter_fundamentals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(qid, 10.0, 6.0, 3.0, 2.0, 2.5, 1.0, 4.0, -1.0, 3.0, 20.0, 5.0, 100.0) for qid in range(1, 5)],
    )


def test_open_ro_rejects_write(tmp_path) -> None:
    db = tmp_path / "audit.db"
    sqlite3.connect(db).execute("CREATE TABLE x(id INTEGER)").connection.close()
    conn = open_ro(db)
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO x VALUES (1)")


def test_issue_ids_are_deterministic() -> None:
    issues = [
        Issue("HIGH", "score", "MODEL", "bad", 1),
        Issue("HIGH", "score", "MODEL", "bad again", 2),
        Issue("LOW", "universe", "REVIEW", "manual", 3),
    ]
    assert [row["issue_id"] for row in issue_rows(issues)] == ["P7-MODEL-001", "P7-MODEL-002", "P7-REVIEW-001"]


def test_canonical_audit_detects_duplicate_and_publish_anomaly(tmp_path) -> None:
    conn = make_conn()
    seed_company_quarters(conn)
    conn.execute("INSERT INTO v3_quarter VALUES (5,1,2025,'Q4','2025-12-31','2025-01-01','2025-01-02')")
    issues: list[Issue] = []
    audit_canonical(conn, tmp_path, issues, date(2026, 8, 25))
    assert {issue.code for issue in issues} >= {"DUPLICATE_FY_FQ", "PUBLISH_DATE_ANOMALY"}


def test_ttm_audit_detects_flow_instant_and_pit_failures(tmp_path) -> None:
    conn = make_conn()
    seed_company_quarters(conn)
    conn.execute(
        """
        INSERT INTO v3_ttm VALUES (
            1,1,4,2025,'Q4','2024-12-31','TEST',
            99,24,12,8,10,4,16,-4,12,
            21,5,100,1,1,'2024-01-01',1,1,1,2,3,4
        )
        """
    )
    issues: list[Issue] = []
    audit_ttm(conn, tmp_path, issues)
    assert {issue.code for issue in issues} >= {"TTM_FLOW_PARITY", "TTM_INSTANT_PARITY", "TTM_PIT_DATE"}


def test_score_audit_detects_fingerprint_bounds_and_lineage(tmp_path) -> None:
    conn = make_conn()
    seed_company_quarters(conn)
    conn.execute("INSERT INTO v3_score VALUES (1,1,999,999,'BAD',1,101,101,120,'HIGH','ORDINARY','BAD')")
    issues: list[Issue] = []
    audit_score(conn, tmp_path, issues)
    assert {issue.code for issue in issues} >= {"SCORE_MODEL_FINGERPRINT", "SCORE_BOUNDS", "SCORE_LINEAGE"}


def test_lifecycle_audit_detects_domain_and_previous_state_failure(tmp_path) -> None:
    conn = make_conn()
    seed_company_quarters(conn)
    conn.executemany(
        "INSERT INTO v3_lifecycle VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (1, 1, 1, 1, 2025, "Q1", "2024-03-31", EXPECTED_LIFECYCLE_MODEL, 1, "HIGH", "MATURE_STABLE", "MATURE_STABLE", None, 0, "INITIAL", 1, EXPECTED_LIFECYCLE_FINGERPRINT),
            (2, 1, 2, 2, 2025, "Q2", "2024-06-30", EXPECTED_LIFECYCLE_MODEL, 1, "HIGH", "BAD", "BAD", "DECLINING", 1, "TEST", 1, EXPECTED_LIFECYCLE_FINGERPRINT),
        ],
    )
    issues: list[Issue] = []
    audit_lifecycle(conn, tmp_path, issues)
    assert {issue.code for issue in issues} >= {"LIFECYCLE_STATE_DOMAIN", "LIFECYCLE_PREVIOUS_STATE"}


def test_valuation_audit_detects_publish_price_formula_and_lineage(tmp_path) -> None:
    conn = make_conn()
    seed_company_quarters(conn)
    conn.execute(
        """
        INSERT INTO v3_ttm VALUES (
            1,1,4,2025,'Q4','2024-12-31','TEST',
            40,24,12,8,10,4,16,-4,12,
            20,5,100,1,1,'2025-02-01',1,1,1,2,3,4
        )
        """
    )
    conn.execute(
        """
        INSERT INTO v3_valuation VALUES (
            1,1,1,4,2025,'Q4','2024-12-31','2025-02-01','2025-02-04',
            9.0,'RAWCANDLE',100.0,901.0,20.0,5.0,-14.0,888.0,40.0,8.0,12.0,1,'READY','TEST'
        )
        """
    )
    raw = tmp_path / "raw.db"
    raw_conn = sqlite3.connect(raw)
    raw_conn.execute("CREATE TABLE osakedata(osake TEXT,pvm TEXT,close REAL,market TEXT)")
    raw_conn.executemany(
        "INSERT INTO osakedata VALUES (?,?,?,?)",
        [("TST", "2025-02-03", 8.0, "usa"), ("TST", "2025-02-04", 7.0, "usa")],
    )
    raw_conn.commit()
    raw_conn.close()
    issues: list[Issue] = []
    audit_valuation(conn, raw, tmp_path, issues)
    assert {issue.code for issue in issues} >= {
        "VALUATION_PUBLISH_PLUS_ONE",
        "VALUATION_PRICE_PARITY",
        "VALUATION_FORMULA_PARITY",
    }


def test_score_good_model_has_no_model_issue(tmp_path) -> None:
    conn = make_conn()
    seed_company_quarters(conn)
    conn.execute(
        "INSERT INTO v3_ttm VALUES (1,1,4,2025,'Q4','2024-12-31','TEST',40,24,12,8,10,4,16,-4,12,20,5,100,1,1,'2025-02-01',1,1,1,2,3,4)"
    )
    conn.execute(
        "INSERT INTO v3_score VALUES (1,1,4,1,?,1,55,100,90,'HIGH','ORDINARY',?)",
        (EXPECTED_SCORE_MODEL, EXPECTED_SCORE_FINGERPRINT),
    )
    issues: list[Issue] = []
    audit_score(conn, tmp_path, issues)
    assert "SCORE_MODEL_FINGERPRINT" not in {issue.code for issue in issues}
