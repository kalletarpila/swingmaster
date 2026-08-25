from __future__ import annotations

import sqlite3

from swingmaster.fundamentals.v3_phase8a7_canonical_closure import (
    FINANCIAL_SUBTYPES,
    FIVE_REVENUE_REPAIRS,
    company_inventory,
    delete_company_set,
    dependency_counts,
    financial_uncertain_rows,
    five_revenue_repairs,
    revenue_systemic_scan,
)


def make_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, market TEXT, company_name TEXT, active INTEGER);
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY, company_id INTEGER REFERENCES v3_company(company_id) ON DELETE CASCADE, fiscal_year INTEGER, fiscal_quarter TEXT, period_end_date TEXT, publish_date TEXT);
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE, revenue REAL, accepted_source_provider TEXT, accepted_at_utc TEXT, update_run_id TEXT, updated_at_utc TEXT);
        CREATE TABLE v3_event(event_id INTEGER PRIMARY KEY, company_id INTEGER, quarter_id INTEGER);
        CREATE TABLE v3_migration_audit(audit_id INTEGER PRIMARY KEY, company_id INTEGER, quarter_id INTEGER);
        CREATE TABLE v3_resolution_issue(issue_id INTEGER PRIMARY KEY, quarter_id INTEGER);
        CREATE TABLE v3_operational_action(action_id INTEGER PRIMARY KEY, company_id INTEGER, quarter_id INTEGER);
        CREATE TABLE v3_provider_q_acquisition(acquisition_id INTEGER PRIMARY KEY, quarter_id INTEGER);
        CREATE TABLE v3_provider_symbol_alias(alias_id INTEGER PRIMARY KEY, company_id INTEGER);
        CREATE TABLE v3_result_calendar(calendar_id INTEGER PRIMARY KEY, company_id INTEGER);
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER, q1_quarter_id INTEGER, q2_quarter_id INTEGER, q3_quarter_id INTEGER, q4_quarter_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, company_id INTEGER, as_of_quarter_id INTEGER, endpoint_ttm_id INTEGER, endpoint_period_end TEXT);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER, endpoint_ttm_id INTEGER, endpoint_period_end TEXT);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER, endpoint_ttm_id INTEGER);
        INSERT INTO v3_company VALUES (1,'GDC','usa','GDC',1),(2,'AOMR','usa','AOMR',1),(3,'KEEP','usa','KeepCo',1);
        INSERT INTO v3_quarter VALUES (10,1,2020,'Q3','2020-09-30','2020-11-01'),(20,2,2026,'Q1','2026-03-31','2026-05-01'),(30,3,2026,'Q1','2026-03-31','2026-05-01');
        INSERT INTO v3_quarter_fundamentals VALUES (10,-45759,'YAHOO',NULL,NULL,'t0'),(20,-1,'YAHOO',NULL,NULL,'t0'),(30,100,'YAHOO',NULL,NULL,'t0');
        INSERT INTO v3_event VALUES (1,2,20);
        INSERT INTO v3_migration_audit VALUES (1,2,20);
        INSERT INTO v3_resolution_issue VALUES (1,20);
        INSERT INTO v3_operational_action VALUES (1,2,20);
        INSERT INTO v3_provider_q_acquisition VALUES (1,20);
        INSERT INTO v3_provider_symbol_alias VALUES (1,2);
        INSERT INTO v3_result_calendar VALUES (1,2);
        INSERT INTO v3_ttm VALUES (1,2,20,20,20,20,20),(2,3,30,30,30,30,30);
        INSERT INTO v3_score VALUES (1,2,20,1,'2026-03-31'),(2,3,30,2,'2026-03-31');
        INSERT INTO v3_lifecycle VALUES (1,2,20,1,'2026-03-31'),(2,3,30,2,'2026-03-31');
        INSERT INTO v3_valuation VALUES (1,2,20,1),(2,3,30,2);
        """
    )
    conn.commit()
    return conn


def semantic_row(ticker="GDC", fy="2020", fq="Q3", status="VALID_BUT_DIFFERENT_SEMANTICS", **overrides):
    row = {
        "Ticker": ticker,
        "Fiscal Year": fy,
        "Fiscal Q": fq,
        "Field": "revenue",
        "Current Value": "-45759",
        "Period End": "2020-09-30",
        "Verified Value": "2465765",
        "Status": status,
        "Confidence": "HIGH",
        "Primary Source": "https://example.test",
        "Notes": "Current value matches RevenueNotFromContractWithCustomerOther.",
    }
    row.update(overrides)
    return row


def test_five_revenue_repairs_are_exact_and_guarded(tmp_path) -> None:
    conn = make_db(tmp_path / "v3.db")
    repairs = five_revenue_repairs(conn, [semantic_row()])
    gdc = next(row for row in repairs if row["ticker"] == "GDC")
    assert len(repairs) == len(FIVE_REVENUE_REPAIRS)
    assert gdc["old_revenue"] == "-45759"
    assert gdc["new_revenue"] == "2465765"
    assert gdc["write_guard_ok"] == 1
    conn.execute("UPDATE v3_quarter_fundamentals SET revenue=-1 WHERE quarter_id=10")
    assert next(row for row in five_revenue_repairs(conn, [semantic_row()]) if row["ticker"] == "GDC")["classification"] == "STALE_REPAIR_GUARD_FAILED"


def test_financial_uncertain_rows_and_unique_inventory(tmp_path) -> None:
    conn = make_db(tmp_path / "v3.db")
    semantic = [semantic_row(ticker="AOMR", fy="2026", fq="Q1", status="UNCERTAIN") for _ in range(24)]
    assert len(financial_uncertain_rows(semantic)) == 24
    inv = company_inventory(conn, ["AOMR"], financial_uncertain_rows(semantic))
    assert inv[0]["eligibility"] == "REMOVE_FROM_V3"
    assert "mortgage REIT" in FINANCIAL_SUBTYPES["AOMR"][0]
    assert inv[0]["affected_uncertain_rows"] == 24


def test_delete_company_set_removes_only_frozen_company(tmp_path) -> None:
    conn = make_db(tmp_path / "v3.db")
    candidate = company_inventory(conn, ["AOMR"], [semantic_row(ticker="AOMR", fy="2026", fq="Q1", status="UNCERTAIN")])[0]
    dep = dependency_counts(conn, [candidate])[0]
    assert dep["canonical_quarters"] == 1
    audit = delete_company_set(conn, [candidate])
    assert audit[0]["apply_status"] == "REMOVED_FROM_V3"
    assert conn.execute("SELECT COUNT(*) FROM v3_company WHERE ticker='AOMR'").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM v3_company WHERE ticker='KEEP'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM v3_ttm WHERE company_id=3").fetchone()[0] == 1


def test_systemic_scan_flags_wrong_concepts_only(tmp_path) -> None:
    conn = make_db(tmp_path / "v3.db")
    out = revenue_systemic_scan(conn, [semantic_row(), semantic_row(ticker="KEEP", status="MATCH", **{"Notes": "ordinary match"})])
    assert len(out) == 1
    assert out[0]["concept_class"] == "REVENUE_SUBCOMPONENT"
