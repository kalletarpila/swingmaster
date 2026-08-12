from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from swingmaster.fundamentals_v2.simfin_seed import create_schema
from swingmaster.fundamentals_v2.simfin_specialized import (
    BANK_PROVIDER,
    INSURANCE_PROVIDER,
    import_simfin_specialized,
    validate_v2_db_target,
)


def test_imports_bank_and_insurance_without_ordinary_semantic_pollution(tmp_path: Path) -> None:
    simfin_dir = _write_specialized_fixture(tmp_path / "simfin")
    db = _write_v2_db(tmp_path / "rc_fundamentals_v2.db")
    legacy = _write_legacy_db(tmp_path / "legacy.db", ["BANKX", "INSX"])

    result = import_simfin_specialized(
        db_path=db,
        simfin_dir=simfin_dir,
        artifact_dir=tmp_path / "artifacts",
        legacy_db=legacy,
    )

    assert result["profile_conflict_count"] == 0
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        profiles = dict(conn.execute("SELECT company_profile, COUNT(*) FROM rc_v2_company GROUP BY company_profile"))
        assert profiles == {"BANK": 1, "INSURANCE": 1}
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_quarter").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_quarterly").fetchone()[0] == 0

        bank = conn.execute(
            """
            SELECT b.revenue, b.provision_for_loan_losses, b.net_loans, b.total_deposits,
                   b.short_term_debt, b.long_term_debt, b.weighted_average_shares_basic
            FROM rc_v2_fundamental_bank_quarterly b
            JOIN rc_v2_quarter q ON q.quarter_id=b.quarter_id
            JOIN rc_v2_company c ON c.company_id=q.company_id
            WHERE c.ticker='BANKX'
            """
        ).fetchone()
        assert bank["revenue"] == 100.0
        assert bank["provision_for_loan_losses"] == -5.0
        assert bank["net_loans"] == 700.0
        assert bank["total_deposits"] == 800.0
        assert bank["short_term_debt"] == 10.0
        assert bank["long_term_debt"] == 20.0
        assert bank["weighted_average_shares_basic"] == 1000.0

        insurance = conn.execute(
            """
            SELECT i.revenue, i.total_claims_losses, i.insurance_reserves, i.total_assets,
                   i.short_term_debt, i.long_term_debt, i.weighted_average_shares_diluted
            FROM rc_v2_fundamental_insurance_quarterly i
            JOIN rc_v2_quarter q ON q.quarter_id=i.quarter_id
            JOIN rc_v2_company c ON c.company_id=q.company_id
            WHERE c.ticker='INSX'
            """
        ).fetchone()
        assert insurance["revenue"] == 200.0
        assert insurance["total_claims_losses"] == -120.0
        assert insurance["insurance_reserves"] == 900.0
        assert insurance["total_assets"] == 1500.0
        assert insurance["short_term_debt"] == 15.0
        assert insurance["long_term_debt"] == 25.0
        assert insurance["weighted_average_shares_diluted"] == 2100.0

        providers = {row["provider"] for row in conn.execute("SELECT DISTINCT provider FROM rc_v2_fundamental_field_source")}
        assert BANK_PROVIDER in providers
        assert INSURANCE_PROVIDER in providers
        source = conn.execute(
            """
            SELECT provider, provider_field, source_dataset, transformation
            FROM rc_v2_fundamental_field_source
            WHERE field_name='bank_net_loans'
            """
        ).fetchone()
        assert source["provider"] == BANK_PROVIDER
        assert source["provider_field"] == "Net Loans"
        assert source["source_dataset"] == "bank_balance"
        assert source["transformation"] == "DIRECT"


def test_idempotent_replay_creates_no_churn(tmp_path: Path) -> None:
    simfin_dir = _write_specialized_fixture(tmp_path / "simfin")
    db = _write_v2_db(tmp_path / "rc_fundamentals_v2.db")
    import_simfin_specialized(db_path=db, simfin_dir=simfin_dir, artifact_dir=tmp_path / "a1")
    before = _counts(db)
    import_simfin_specialized(db_path=db, simfin_dir=simfin_dir, artifact_dir=tmp_path / "a2")
    assert _counts(db) == before


def test_profile_conflict_rejected_without_switching_existing_company(tmp_path: Path) -> None:
    simfin_dir = _write_specialized_fixture(tmp_path / "simfin")
    db = _write_v2_db(tmp_path / "rc_fundamentals_v2.db")
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "INSERT INTO rc_v2_company VALUES (1,'usa','BANKX',1,'Bank X','ORDINARY',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
        )
        conn.commit()

    result = import_simfin_specialized(db_path=db, simfin_dir=simfin_dir, artifact_dir=tmp_path / "artifacts", profiles=("BANK",))
    assert result["profile_conflict_count"] == 1
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT company_profile FROM rc_v2_company WHERE simfin_id=1").fetchone()[0] == "ORDINARY"
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_bank_quarterly").fetchone()[0] == 0


def test_dry_run_uses_copy_and_legacy_db_target_rejected(tmp_path: Path) -> None:
    simfin_dir = _write_specialized_fixture(tmp_path / "simfin")
    db = _write_v2_db(tmp_path / "rc_fundamentals_v2.db")
    import_simfin_specialized(db_path=db, simfin_dir=simfin_dir, artifact_dir=tmp_path / "dry", dry_run=True)
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_company").fetchone()[0] == 0
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='rc_v2_fundamental_bank_quarterly'").fetchone() is None
    with pytest.raises(ValueError, match="REFUSES_LEGACY"):
        validate_v2_db_target(tmp_path / "fundamentals_usa.db")


def _write_v2_db(path: Path) -> Path:
    with sqlite3.connect(str(path)) as conn:
        create_schema(conn)
        conn.commit()
    return path


def _write_legacy_db(path: Path, tickers: list[str]) -> Path:
    with sqlite3.connect(str(path)) as conn:
        conn.execute("CREATE TABLE rc_fundamental_quarterly (ticker TEXT)")
        conn.executemany("INSERT INTO rc_fundamental_quarterly VALUES (?)", [(ticker,) for ticker in tickers])
        conn.commit()
    return path


def _counts(db: Path) -> tuple[int, int, int, int, int]:
    with sqlite3.connect(str(db)) as conn:
        return (
            conn.execute("SELECT COUNT(*) FROM rc_v2_company").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM rc_v2_quarter").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_bank_quarterly").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_insurance_quarterly").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source").fetchone()[0],
        )


def _write_specialized_fixture(root: Path) -> Path:
    root.mkdir(parents=True)
    common_bank = {
        "Ticker": "BANKX",
        "SimFinId": "1",
        "Currency": "USD",
        "Fiscal Year": "2026",
        "Fiscal Period": "Q1",
        "Report Date": "2026-03-31",
        "Publish Date": "2026-04-20",
        "Restated Date": "2026-04-21",
        "Shares (Basic)": "1000",
        "Shares (Diluted)": "1100",
    }
    common_ins = {
        "Ticker": "INSX",
        "SimFinId": "2",
        "Currency": "USD",
        "Fiscal Year": "2026",
        "Fiscal Period": "Q1",
        "Report Date": "2026-03-31",
        "Publish Date": "2026-04-22",
        "Restated Date": "2026-04-23",
        "Shares (Basic)": "2000",
        "Shares (Diluted)": "2100",
    }
    _write_csv(root / "us-income-banks-quarterly.csv", [{**common_bank, "Revenue": "100", "Provision for Loan Losses": "-5", "Net Revenue after Provisions": "95", "Total Non-Interest Expense": "-40", "Operating Income (Loss)": "55", "Non-Operating Income (Loss)": "1", "Pretax Income (Loss)": "56", "Income Tax (Expense) Benefit, Net": "-10", "Income (Loss) from Continuing Operations": "46", "Net Extraordinary Gains (Losses)": "", "Net Income": "46", "Net Income (Common)": "44"}])
    _write_csv(root / "us-balance-banks-quarterly.csv", [{**common_bank, "Cash, Cash Equivalents & Short Term Investments": "50", "Interbank Assets": "60", "Short & Long Term Investments": "300", "Accounts & Notes Receivable": "", "Net Loans": "700", "Net Fixed Assets": "", "Total Assets": "1200", "Total Deposits": "800", "Short Term Debt": "10", "Long Term Debt": "20", "Total Liabilities": "1000", "Preferred Equity": "2", "Share Capital & Additional Paid-In Capital": "", "Treasury Stock": "", "Retained Earnings": "", "Total Equity": "200", "Total Liabilities & Equity": "1200"}])
    _write_csv(root / "us-cashflow-banks-quarterly.csv", [{**common_bank, "Net Income/Starting Line": "46", "Depreciation & Amortization": "3", "Provision for Loan Losses": "-5", "Non-Cash Items": "", "Change in Working Capital": "", "Net Cash from Operating Activities": "70", "Change in Fixed Assets & Intangibles": "-4", "Net Change in Loans & Interbank": "", "Net Cash from Acquisitions & Divestitures": "", "Net Cash from Investing Activities": "", "Dividends Paid": "", "Cash from (Repayment of) Debt": "", "Cash from (Repurchase of) Equity": "", "Net Cash from Financing Activities": "", "Effect of Foreign Exchange Rates": "", "Net Change in Cash": ""}])
    _write_csv(root / "us-income-insurance-quarterly.csv", [{**common_ins, "Revenue": "200", "Total Claims & Losses": "-120", "Operating Income (Loss)": "30", "Pretax Income (Loss)": "32", "Income Tax (Expense) Benefit, Net": "-6", "Income (Loss) from Affiliates, Net of Taxes": "1", "Income (Loss) from Continuing Operations": "27", "Net Extraordinary Gains (Losses)": "", "Net Income": "27", "Net Income (Common)": "26"}])
    _write_csv(root / "us-balance-insurance-quarterly.csv", [{**common_ins, "Total Investments": "1000", "Cash, Cash Equivalents & Short Term Investments": "100", "Accounts & Notes Receivable": "80", "Property, Plant & Equipment, Net": "40", "Total Assets": "1500", "Insurance Reserves": "900", "Short Term Debt": "15", "Long Term Debt": "25", "Total Liabilities": "1100", "Preferred Equity": "", "Policyholders Equity": "100", "Share Capital & Additional Paid-In Capital": "", "Treasury Stock": "", "Retained Earnings": "", "Total Equity": "400", "Total Liabilities & Equity": "1500"}])
    _write_csv(root / "us-cashflow-insurance-quarterly.csv", [{**common_ins, "Net Income/Starting Line": "27", "Depreciation & Amortization": "2", "Non-Cash Items": "", "Net Cash from Operating Activities": "50", "Change in Fixed Assets & Intangibles": "-3", "Net Change in Investments": "", "Net Cash from Investing Activities": "", "Dividends Paid": "", "Cash from (Repayment of) Debt": "", "Cash from (Repurchase of) Equity": "", "Net Cash from Financing Activities": "", "Effect of Foreign Exchange Rates": "", "Net Change in Cash": ""}])
    return root


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
