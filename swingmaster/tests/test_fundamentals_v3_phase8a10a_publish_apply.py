from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10a_publish_apply as apply


HEADERS = [
    "Ticker",
    "Fiscal Year",
    "Fiscal Q",
    "Period End",
    "Current Publish Date",
    "Prior Verified/Candidate Publish Date",
    "Prior External Status",
    "Current Residual Reason",
    "Current Residual Queue",
    "Current Canonical Quarter ID",
    "Latest-State Impact",
    "Downstream Impact",
    "Source 1",
    "Source 2",
    "Notes",
    "Verified Publish Date",
    "Status",
    "Confidence",
    "Source Count",
    "Primary Source",
    "Primary Source Type",
    "Secondary Source",
    "Secondary Source Type",
    "Earnings Release Date",
    "SEC Filing Date",
    "Verification Method",
]
CASE_ROWS = [
    ("ABVC", 2019, "Q3", "2019-09-30", "2019-08-18", "2019-11-18", 1, "", "2019-11-18", "DIRECT_SEC_FILING_DATE"),
    ("BCTX", 2026, "Q1", "2025-10-31", "2025-10-11", "2025-12-11", 1, "", "2025-12-11", "DIRECT_SEC_FILING_DATE"),
    ("BJDX", 2024, "Q4", "2024-12-31", "2024-03-28", "2025-03-31", 1, "", "2025-03-31", "DIRECT_SEC_FILING_DATE"),
    ("BOC", 2025, "Q4", "2025-12-31", "2025-03-28", "2026-03-30", 2, "2026-03-30", "2026-03-30", "DIRECT_ISSUER_EARNINGS_RELEASE"),
    ("BRTX", 2020, "Q2", "2020-06-30", "2020-01-11", "2021-04-12", 1, "", "2021-04-12", "DIRECT_SEC_FILING_DATE"),
    ("BRTX", 2020, "Q3", "2020-09-30", "2020-04-12", "2021-04-12", 1, "", "2021-04-12", "DIRECT_SEC_FILING_DATE"),
    ("KLRS", 2024, "Q4", "2024-12-31", "2024-03-15", "2025-03-07", 1, "", "2025-03-07", "DIRECT_SEC_FILING_DATE"),
    ("LWLG", 2025, "Q4", "2025-12-31", "2025-03-18", "2026-03-05", 2, "2026-03-05", "2026-03-06", "FIRST_PUBLIC_DISCLOSURE"),
    ("NWTG", 2024, "Q4", "2024-12-31", "2024-03-18", "2025-03-31", 2, "2025-03-31", "2025-04-04", "FIRST_PUBLIC_DISCLOSURE"),
    ("OLB", 2024, "Q4", "2024-12-31", "2024-04-15", "2025-04-15", 1, "", "2025-04-15", "DIRECT_SEC_FILING_DATE"),
    ("OMEX", 2024, "Q4", "2024-12-31", "2024-05-17", "2025-03-31", 1, "", "2025-03-31", "DIRECT_SEC_FILING_DATE"),
    ("ORBS", 2024, "Q4", "2024-12-31", "2024-04-02", "2025-04-15", 2, "2025-04-15", "2025-04-15", "DIRECT_SEC_FILING_DATE"),
    ("PROP", 2024, "Q4", "2024-12-31", "2024-03-19", "2025-03-06", 1, "", "2025-03-06", "DIRECT_SEC_FILING_DATE"),
    ("RIME", 2024, "Q4", "2024-12-31", "2023-07-14", "2025-04-15", 2, "2025-04-16", "2025-04-15", "FIRST_PUBLIC_DISCLOSURE"),
    ("RNAZ", 2024, "Q4", "2024-12-31", "2024-04-01", "2025-04-15", 1, "", "2025-04-15", "DIRECT_SEC_FILING_DATE"),
    ("SLXN", 2025, "Q4", "2025-12-31", "2025-03-18", "2026-03-17", 2, "2026-03-17", "2026-03-17", "DIRECT_SEC_FILING_DATE"),
    ("TELO", 2025, "Q4", "2025-12-31", "2025-02-04", "2026-03-17", 1, "", "2026-03-17", "DIRECT_SEC_FILING_DATE"),
]


def verified_rows() -> list[dict[str, str]]:
    out = []
    for qid, (ticker, fy, fq, period_end, old_publish, verified_publish, sources, earnings, sec, method) in enumerate(CASE_ROWS, 1):
        out.append(
            {
                "Ticker": ticker,
                "Fiscal Year": str(fy),
                "Fiscal Q": fq,
                "Period End": period_end,
                "Current Publish Date": old_publish,
                "Prior Verified/Candidate Publish Date": verified_publish,
                "Prior External Status": "DIFFERENT",
                "Current Residual Reason": "PUBLISH_DATE_SEMANTICS_UNCERTAIN;NEEDS_SECONDARY_SOURCE",
                "Current Residual Queue": "NEEDS_SECONDARY_SOURCE",
                "Current Canonical Quarter ID": str(qid),
                "Latest-State Impact": "0",
                "Downstream Impact": "ttm=0;score=0;lifecycle=0;valuation=0",
                "Source 1": f"https://example.test/{ticker}/1",
                "Source 2": f"https://example.test/{ticker}/2" if sources >= 2 else "",
                "Notes": "verified",
                "Verified Publish Date": verified_publish,
                "Status": "DIFFERENT",
                "Confidence": "HIGH",
                "Source Count": str(sources),
                "Primary Source": f"https://example.test/{ticker}/primary",
                "Primary Source Type": "SEC_10K",
                "Secondary Source": f"https://example.test/{ticker}/secondary" if sources >= 2 else "",
                "Secondary Source Type": "ISSUER_EARNINGS_RELEASE" if sources >= 2 else "",
                "Earnings Release Date": earnings,
                "SEC Filing Date": sec,
                "Verification Method": method,
            }
        )
    return out


def write_verified_csv(path: Path, data: list[dict[str, str]] | None = None) -> Path:
    data = data or verified_rows()
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(data)
    return path


def make_db(path: Path) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, active INTEGER);
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_quarter TEXT,
            period_end_date TEXT, publish_date TEXT, market_availability_date TEXT, q_lifecycle TEXT,
            sec_confirmation_state TEXT, created_at_utc TEXT, updated_at_utc TEXT,
            UNIQUE(company_id,fiscal_year,fiscal_quarter)
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY, revenue REAL, gross_profit REAL, operating_income REAL, ebit REAL,
            ebitda REAL, net_income REAL, operating_cashflow REAL, capex REAL, free_cashflow REAL, cash REAL,
            total_debt REAL, shares_outstanding REAL, currency TEXT, accepted_source_provider TEXT,
            accepted_at_utc TEXT, update_run_id TEXT, derivation_method TEXT, resolution_issue_id INTEGER,
            created_at_utc TEXT, updated_at_utc TEXT
        );
        CREATE TABLE v3_migration_audit(audit_id INTEGER PRIMARY KEY, quarter_id INTEGER);
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, endpoint_ttm_id INTEGER, score_model_version TEXT, score_fingerprint TEXT);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, endpoint_ttm_id INTEGER, lifecycle_model_version TEXT, lifecycle_fingerprint TEXT);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        """
    )
    for qid, row in enumerate(verified_rows(), 1):
        company_id = qid
        conn.execute("INSERT INTO v3_company VALUES (?,?,1)", (company_id, row["Ticker"]))
        conn.execute(
            "INSERT INTO v3_quarter VALUES (?,?,?,?,?,?,NULL,'OPERATIONALLY_SETTLED','CONFIRMED','c','u')",
            (qid, company_id, int(row["Fiscal Year"]), row["Fiscal Q"], row["Period End"], row["Current Publish Date"]),
        )
        conn.execute(
            """
            INSERT INTO v3_quarter_fundamentals VALUES (
              ?,100,50,10,10,12,8,9,-1,8,20,5,10,'USD','TEST','a','r','d',NULL,'c','u'
            )
            """,
            (qid,),
        )
        conn.execute("INSERT INTO v3_migration_audit VALUES (?,?)", (qid, qid))
    conn.execute("INSERT INTO v3_ttm VALUES (1,1)")
    conn.execute("INSERT INTO v3_score VALUES (1,1,'M','S')")
    conn.execute("INSERT INTO v3_lifecycle VALUES (1,1,'M','L')")
    conn.execute("INSERT INTO v3_valuation VALUES (1,1)")
    conn.commit()
    conn.close()
    return path


def run_apply(tmp_path: Path) -> dict:
    db_path = make_db(tmp_path / "v3.db")
    csv_path = write_verified_csv(tmp_path / "phase8_publish_date_residual_17_verified.csv")
    raw_path = tmp_path / "osakedata.db"
    raw_path.write_bytes(b"raw")
    return apply.run_phase8a10a_publish_apply(
        apply.Phase8A10APublishApplyPaths(
            artifact_root=tmp_path / "artifacts",
            v3_db=db_path,
            verified_csv=csv_path,
            rawcandle_db=raw_path,
        )
    )


def test_verified_input_row_count_17(tmp_path: Path) -> None:
    manifest, _ = apply.validate_verified_input(verified_rows(), write_verified_csv(tmp_path / "v.csv"))
    assert manifest["rows"] == 17


def test_verified_file_found(tmp_path: Path, monkeypatch) -> None:
    write_verified_csv(tmp_path / "phase8_publish_date_residual_17_verified.csv")
    monkeypatch.chdir(tmp_path)
    assert apply.find_verified_csv(Path(".")) == Path("phase8_publish_date_residual_17_verified.csv")


def test_different_count_17(tmp_path: Path) -> None:
    manifest, _ = apply.validate_verified_input(verified_rows(), write_verified_csv(tmp_path / "v.csv"))
    assert manifest["status_counts"]["DIFFERENT"] == 17


def test_high_confidence_17(tmp_path: Path) -> None:
    manifest, _ = apply.validate_verified_input(verified_rows(), write_verified_csv(tmp_path / "v.csv"))
    assert manifest["confidence_counts"]["HIGH"] == 17


def test_no_uncertain_not_found_identity_conflict(tmp_path: Path) -> None:
    _, recon = apply.validate_verified_input(verified_rows(), write_verified_csv(tmp_path / "v.csv"))
    assert all(row["actual"] == 0 for row in recon if row["metric"] in {"UNCERTAIN", "NOT_FOUND", "IDENTITY_CONFLICT"})


def test_unique_identity_17(tmp_path: Path) -> None:
    manifest, _ = apply.validate_verified_input(verified_rows(), write_verified_csv(tmp_path / "v.csv"))
    assert manifest["unique_ticker_fy_fq"] == 17


def test_old_publish_date_guard_passes(tmp_path: Path) -> None:
    db_path = make_db(tmp_path / "v3.db")
    with apply.connect(db_path) as conn:
        guards, _, _ = apply.reconcile_input_to_current(conn, verified_rows())
    assert all(row["old_value_guard"] == "PASS" for row in guards)


def test_period_end_unchanged_after_apply(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["integrity"]["counts_before"] == summary["integrity"]["counts_after"]


def test_fy_fq_unchanged_after_apply(tmp_path: Path) -> None:
    run_apply(tmp_path)
    parity = list(csv.DictReader((tmp_path / "artifacts" / "publish_apply_parity.csv").open()))
    assert all(row["fy_fq_unchanged"] == "1" for row in parity)


def test_only_publish_date_changes(tmp_path: Path) -> None:
    run_apply(tmp_path)
    parity = list(csv.DictReader((tmp_path / "artifacts" / "publish_apply_parity.csv").open()))
    assert all(row["only_publish_date_changed"] == "1" for row in parity)


def test_changed_cells_exactly_17(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["apply"]["changed_publish_cells"] == 17


def test_brtx_duplicate_publish_date_accepted(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["chronology"]["brtx_duplicate_publish_date_exception_accepted"]


def test_rime_first_public_date_policy() -> None:
    rime = [row for row in verified_rows() if row["Ticker"] == "RIME"][0]
    assert rime["Verified Publish Date"] == min(rime["Earnings Release Date"], rime["SEC Filing Date"])


def test_klrs_ticker_name_history_accepted(tmp_path: Path) -> None:
    db_path = make_db(tmp_path / "v3.db")
    with apply.connect(db_path) as conn:
        guards, _, _ = apply.reconcile_input_to_current(conn, verified_rows())
    assert [row for row in guards if row["ticker"] == "KLRS"][0]["old_value_guard"] == "PASS"


def test_orbs_ticker_name_history_accepted(tmp_path: Path) -> None:
    db_path = make_db(tmp_path / "v3.db")
    with apply.connect(db_path) as conn:
        guards, _, _ = apply.reconcile_input_to_current(conn, verified_rows())
    assert [row for row in guards if row["ticker"] == "ORBS"][0]["old_value_guard"] == "PASS"


def test_bctx_fiscal_year_identity_accepted(tmp_path: Path) -> None:
    db_path = make_db(tmp_path / "v3.db")
    with apply.connect(db_path) as conn:
        guards, _, _ = apply.reconcile_input_to_current(conn, verified_rows())
    bctx = [row for row in guards if row["ticker"] == "BCTX"][0]
    assert bctx["fiscal_year"] == 2026 and bctx["current_period_end"] == "2025-10-31"


def test_bctx_market_availability_flag_is_r2_not_publish_r1() -> None:
    row = {
        "ticker": "BCTX",
        "quarter_id": 1551,
        "period_end_date": "2025-10-31",
        "publish_date": "2025-12-11",
        "market_availability_date": "2025-10-11",
    }
    assert apply.publish_residual_tier(row, today=apply.date(2026, 8, 26)) == "R2_MARKET_AVAILABILITY_STALE_AFTER_PUBLISH_REPAIR"


def test_no_immr_in_verified_input() -> None:
    assert "IMMR" not in {row["Ticker"] for row in verified_rows()}


def test_no_rcat_in_verified_input() -> None:
    assert "RCAT" not in {row["Ticker"] for row in verified_rows()}


def test_all_fundamentals_unchanged(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["integrity"]["fundamental_fields_changed"] == 0


def test_lineage_unchanged(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["integrity"]["lineage_rows_changed"] == 0


def test_no_ttm_writes(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["downstream_writes"]["ttm"] == 0


def test_no_score_writes(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["downstream_writes"]["score"] == 0


def test_no_lifecycle_writes(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["downstream_writes"]["lifecycle"] == 0


def test_no_valuation_writes(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["downstream_writes"]["valuation"] == 0


def test_rawcandle_unchanged(tmp_path: Path) -> None:
    summary = run_apply(tmp_path)
    assert summary["rawcandle"]["writes"] == 0


def test_rollback_on_stale_guard(tmp_path: Path) -> None:
    db_path = make_db(tmp_path / "v3.db")
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE v3_quarter SET publish_date='2099-01-01' WHERE quarter_id=1")
    conn.commit()
    conn.close()
    csv_path = write_verified_csv(tmp_path / "phase8_publish_date_residual_17_verified.csv")
    raw_path = tmp_path / "osakedata.db"
    raw_path.write_bytes(b"raw")
    summary = apply.run_phase8a10a_publish_apply(
        apply.Phase8A10APublishApplyPaths(tmp_path / "artifacts", db_path, csv_path, raw_path)
    )
    assert summary["classification"] == apply.CLASSIFICATION_STALE
    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT publish_date FROM v3_quarter WHERE quarter_id=2").fetchone()[0] == "2025-10-11"
    conn.close()


def test_immr_rcat_structural_blocker_respected(tmp_path: Path) -> None:
    data = verified_rows()
    data[0]["Ticker"] = "IMMR"
    db_path = make_db(tmp_path / "v3.db")
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE v3_company SET ticker='IMMR' WHERE company_id=1")
    conn.commit()
    conn.close()
    with apply.connect(db_path) as conn:
        guards, _, frozen = apply.reconcile_input_to_current(conn, data)
    assert [row for row in guards if row["ticker"] == "IMMR"][0]["structural_identity_guard"] == apply.CLASSIFICATION_STRUCTURAL_BLOCK
    assert len(frozen) == 16
