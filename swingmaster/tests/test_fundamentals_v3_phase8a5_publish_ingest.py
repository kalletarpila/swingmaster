from __future__ import annotations

import csv
import sqlite3

from swingmaster.fundamentals.v3_phase8a5_publish_ingest import (
    CLASSIFICATION_MANUAL,
    Phase8A5Paths,
    later_date,
    period_end_disposition,
    publish_evidence_status,
    run_phase8a5,
    trading_day_distance,
)


HEADER = [
    "Ticker",
    "Fiscal Year",
    "Fiscal Q",
    "Publish Date",
    "Period End",
    "Candidate Publish Date",
    "Status",
    "Verified Fiscal Year",
    "Verified Fiscal Q",
    "Verified Period End",
    "Evidence Basis",
    "Source 1",
    "Source 2",
    "Review Notes",
]


def row(**overrides):
    base = {
        "Ticker": "TST",
        "Fiscal Year": "2025",
        "Fiscal Q": "Q1",
        "Publish Date": "2025-02-01",
        "Period End": "2025-03-31",
        "Candidate Publish Date": "2025-05-01",
        "Status": "DIFFERENT",
        "Verified Fiscal Year": "2025",
        "Verified Fiscal Q": "Q1",
        "Verified Period End": "2025-03-31",
        "Evidence Basis": "SEC Item 2.02 8-K (results release)",
        "Source 1": "https://example.test/source",
        "Source 2": "",
        "Review Notes": "",
    }
    base.update(overrides)
    return base


def write_verified(path, rows):
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER)
        writer.writeheader()
        writer.writerows(rows)


def make_dbs(v3, raw) -> None:
    conn = sqlite3.connect(v3)
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY,market TEXT,ticker TEXT);
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,company_id INTEGER,fiscal_year INTEGER,fiscal_quarter TEXT,period_end_date TEXT,publish_date TEXT);
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY,q1_quarter_id INTEGER,q2_quarter_id INTEGER,q3_quarter_id INTEGER,q4_quarter_id INTEGER,endpoint_quarter_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY,endpoint_ttm_id INTEGER);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY,endpoint_ttm_id INTEGER);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY,endpoint_ttm_id INTEGER,endpoint_quarter_id INTEGER,valuation_date TEXT,valuation_close_price REAL);
        INSERT INTO v3_company VALUES (1,'usa','TST');
        INSERT INTO v3_quarter VALUES (1,1,2025,'Q1','2025-03-31','2025-02-01');
        INSERT INTO v3_ttm VALUES (1,1,1,1,1,1);
        INSERT INTO v3_score VALUES (1,1);
        INSERT INTO v3_lifecycle VALUES (1,1);
        INSERT INTO v3_valuation VALUES (1,1,1,'2025-02-03',10.0);
        """
    )
    conn.commit()
    conn.close()
    raw_conn = sqlite3.connect(raw)
    raw_conn.execute("CREATE TABLE osakedata(osake TEXT,pvm TEXT,close REAL,market TEXT)")
    raw_conn.executemany(
        "INSERT INTO osakedata VALUES (?,?,?,?)",
        [
            ("TST", "2025-03-28", 10.0, "usa"),
            ("TST", "2025-03-31", 11.0, "usa"),
            ("TST", "2025-04-01", 12.0, "usa"),
            ("TST", "2025-05-02", 13.0, "usa"),
        ],
    )
    raw_conn.commit()
    raw_conn.close()


def test_evidence_status_requires_candidate_source_and_result_semantics() -> None:
    assert publish_evidence_status(row(Status="MATCH")) == "MATCH_CONFIRMED"
    assert publish_evidence_status(row()) == "PUBLISH_DATE_REPAIR_CONFIRMED"
    assert publish_evidence_status(row(**{"Candidate Publish Date": ""})) == "PUBLISH_DATE_EVIDENCE_INSUFFICIENT"
    assert publish_evidence_status(row(**{"Source 1": ""})) == "PUBLISH_DATE_EVIDENCE_INSUFFICIENT"
    assert publish_evidence_status(row(**{"Evidence Basis": "SEC 10-Q filing date"})) == "PUBLISH_DATE_SEMANTICS_UNCERTAIN"


def test_period_end_trading_day_tolerance_and_later_selection(tmp_path) -> None:
    raw = tmp_path / "raw.db"
    v3 = tmp_path / "v3.db"
    make_dbs(v3, raw)
    conn = sqlite3.connect(raw)
    assert trading_day_distance(conn, "usa", "2025-03-28", "2025-03-31") == 1
    assert period_end_disposition("2025-03-31", "2025-03-31", 0) == "EXACT_MATCH"
    assert period_end_disposition("2025-03-31", "2025-03-28", 1) == "WITHIN_TOLERANCE_NO_CHANGE"
    assert period_end_disposition("2025-03-28", "2025-03-31", 1) == "WITHIN_TOLERANCE_UPDATE_TO_LATER"
    assert period_end_disposition("2025-03-01", "2025-03-31", 8) == "OUTSIDE_TOLERANCE_MANUAL_REVIEW"
    assert later_date("2025-03-28", "2025-03-31") == "2025-03-31"


def test_run_phase8a5_retains_rows_blocks_identity_and_writes_zero(tmp_path) -> None:
    v3 = tmp_path / "v3.db"
    raw = tmp_path / "raw.db"
    verified = tmp_path / "verified.csv"
    out = tmp_path / "out"
    make_dbs(v3, raw)
    data = [row() for _ in range(111)]
    for i, item in enumerate(data):
        item["Ticker"] = "TST"
        item["Fiscal Year"] = "2025"
        item["Fiscal Q"] = "Q1"
        item["Status"] = "MATCH" if i < 16 else "DIFFERENT"
    data[20]["Verified Fiscal Q"] = "Q2"
    data[21]["Publish Date"] = "2025-02-02"
    write_verified(verified, data)
    summary = run_phase8a5(Phase8A5Paths(verified, out, v3, raw))
    assert summary["classification"] == CLASSIFICATION_MANUAL
    assert summary["rows"] == 111
    assert summary["status_counts"]["MATCH"] == 16
    assert summary["status_counts"]["DIFFERENT"] == 95
    assert summary["fq_mismatches"] == 1
    assert summary["current_production_identity_mismatches"] == 1
    assert summary["production_counts_before"] == summary["production_counts_after"]
    assert summary["production_writes"] == 0
    assert (out / "publish_date_frozen_repair_set.csv").exists()
