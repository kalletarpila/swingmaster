from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from swingmaster.fundamentals_v2.simfin_seed import (
    build_rc_fundamentals_v2_from_simfin,
    canonical_content_hash,
    validate_output_path,
)


def test_builds_clean_v2_with_fiscal_identity_mapping_provenance_and_determinism(tmp_path: Path) -> None:
    simfin_dir = _write_fixture_simfin_dir(tmp_path / "simfin")
    db1 = tmp_path / "rc_fundamentals_v2_a.db"
    db2 = tmp_path / "rc_fundamentals_v2_b.db"
    result1 = build_rc_fundamentals_v2_from_simfin(
        simfin_dir=simfin_dir,
        output_db=db1,
        artifact_dir=tmp_path / "artifacts1",
        rebuild=False,
        legacy_db=None,
    )
    result2 = build_rc_fundamentals_v2_from_simfin(
        simfin_dir=simfin_dir,
        output_db=db2,
        artifact_dir=tmp_path / "artifacts2",
        rebuild=False,
        legacy_db=None,
    )

    assert result1["integrity_check"] == "ok"
    assert result1["duplicate_canonical_quarters"] == 0
    assert result1["content_hash"] == result2["content_hash"]
    assert canonical_content_hash(db1) == canonical_content_hash(db2)

    with sqlite3.connect(str(db1)) as conn:
        conn.row_factory = sqlite3.Row
        aapl = conn.execute(
            """
            SELECT c.ticker, c.company_profile, q.fiscal_year, q.fiscal_period, q.report_date,
                   f.revenue, f.operating_income, f.depreciation_amortization, f.ebit,
                   f.ebitda, f.operating_cashflow, f.capex, f.free_cashflow, f.cash,
                   f.total_debt, f.shares_outstanding, f.weighted_average_shares_basic,
                   f.weighted_average_shares_diluted, f.seed_status
            FROM rc_v2_fundamental_quarterly f
            JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id
            JOIN rc_v2_company c ON c.company_id=q.company_id
            WHERE c.ticker='AAPL'
            """
        ).fetchone()
        assert aapl["company_profile"] == "ORDINARY"
        assert aapl["fiscal_year"] == 2026
        assert aapl["fiscal_period"] == "Q1"
        assert aapl["report_date"] == "2025-12-27"
        assert aapl["revenue"] == 100.0
        assert aapl["operating_income"] == 20.0
        assert aapl["depreciation_amortization"] == 5.0
        assert aapl["ebitda"] == 25.0
        assert aapl["ebit"] is None
        assert aapl["operating_cashflow"] == 30.0
        assert aapl["capex"] == -7.0
        assert aapl["free_cashflow"] == 23.0
        assert aapl["total_debt"] == 15.0
        assert aapl["shares_outstanding"] is None
        assert aapl["weighted_average_shares_basic"] == 1000.0
        assert aapl["weighted_average_shares_diluted"] == 1100.0
        assert aapl["seed_status"] == "SEED_STRONG"

        sources = {
            row["field_name"]: row
            for row in conn.execute(
                """
                SELECT field_name, provider, provider_field, transformation
                FROM rc_v2_fundamental_field_source
                JOIN rc_v2_quarter USING (quarter_id)
                JOIN rc_v2_company USING (company_id)
                WHERE ticker='AAPL'
                """
            )
        }
        assert sources["revenue"]["provider"] == "SIMFIN"
        assert sources["ebitda"]["provider"] == "SIMFIN_DERIVED"
        assert sources["ebitda"]["transformation"] == "operating_income + depreciation_amortization"
        assert sources["free_cashflow"]["transformation"] == "operating_cashflow + capex"
        assert "ebit" not in sources


def test_missing_ebitda_input_keeps_ebitda_null_and_partial_statements_supported(tmp_path: Path) -> None:
    simfin_dir = _write_fixture_simfin_dir(tmp_path / "simfin", missing_da=True, partial_balance=True)
    db = tmp_path / "rc_fundamentals_v2.db"
    build_rc_fundamentals_v2_from_simfin(
        simfin_dir=simfin_dir,
        output_db=db,
        artifact_dir=tmp_path / "artifacts",
        rebuild=False,
        legacy_db=None,
    )
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT c.ticker, q.has_income, q.has_balance, q.has_cashflow,
                   f.operating_income, f.depreciation_amortization, f.ebitda, f.seed_status
            FROM rc_v2_fundamental_quarterly f
            JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id
            JOIN rc_v2_company c ON c.company_id=q.company_id
            WHERE c.ticker='MISSDA'
            """
        ).fetchone()
        assert row["has_income"] == 1
        assert row["has_balance"] == 0
        assert row["has_cashflow"] == 1
        assert row["operating_income"] == -10.0
        assert row["depreciation_amortization"] is None
        assert row["ebitda"] is None
        assert row["seed_status"] == "SEED_PARTIAL"


def test_safety_rejects_legacy_target_and_existing_output_without_rebuild(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="RC_V2_REFUSES_LEGACY_DB_TARGET"):
        validate_output_path(tmp_path / "fundamentals_usa.db", rebuild=True)
    existing = tmp_path / "rc_fundamentals_v2.db"
    existing.write_text("already here", encoding="utf-8")
    with pytest.raises(FileExistsError, match="REBUILD_REQUIRED"):
        validate_output_path(existing, rebuild=False)


def test_bank_and_insurance_are_not_forced_through_ordinary_import_when_files_absent(tmp_path: Path) -> None:
    simfin_dir = _write_fixture_simfin_dir(tmp_path / "simfin")
    db = tmp_path / "rc_fundamentals_v2.db"
    artifact_dir = tmp_path / "artifacts"
    build_rc_fundamentals_v2_from_simfin(
        simfin_dir=simfin_dir,
        output_db=db,
        artifact_dir=artifact_dir,
        rebuild=False,
        legacy_db=None,
    )

    with sqlite3.connect(str(db)) as conn:
        profiles = dict(conn.execute("SELECT company_profile, COUNT(*) FROM rc_v2_company GROUP BY company_profile"))
    assert profiles == {"ORDINARY": 2}

    gap_text = (artifact_dir / "bank_insurance_gap_analysis.csv").read_text(encoding="utf-8")
    assert "BANK,0,PENDING_DEDICATED_IMPORT" in gap_text
    assert "INSURANCE,0,PENDING_DEDICATED_IMPORT" in gap_text


def _write_fixture_simfin_dir(root: Path, *, missing_da: bool = False, partial_balance: bool = False) -> Path:
    root.mkdir(parents=True)
    company_rows = [
        {
            "Ticker": "AAPL",
            "SimFinId": "111",
            "Company Name": "Apple Inc.",
            "IndustryId": "",
            "ISIN": "",
            "End of financial year (month)": "9",
            "Number Employees": "",
            "Business Summary": "",
            "Market": "us",
            "CIK": "320193",
            "Main Currency": "USD",
        },
        {
            "Ticker": "MISSDA",
            "SimFinId": "222",
            "Company Name": "Missing DA Corp",
            "IndustryId": "",
            "ISIN": "",
            "End of financial year (month)": "12",
            "Number Employees": "",
            "Business Summary": "",
            "Market": "us",
            "CIK": "",
            "Main Currency": "USD",
        },
    ]
    income_rows = [
        {
            "Ticker": "AAPL",
            "SimFinId": "111",
            "Currency": "USD",
            "Fiscal Year": "2026",
            "Fiscal Period": "Q1",
            "Report Date": "2025-12-27",
            "Publish Date": "2026-01-30",
            "Restated Date": "2026-01-30",
            "Shares (Basic)": "1000",
            "Shares (Diluted)": "1100",
            "Revenue": "100",
            "Gross Profit": "40",
            "Operating Income (Loss)": "20",
            "Net Income": "15",
        },
        {
            "Ticker": "MISSDA",
            "SimFinId": "222",
            "Currency": "USD",
            "Fiscal Year": "2026",
            "Fiscal Period": "Q1",
            "Report Date": "2026-03-31",
            "Publish Date": "2026-04-30",
            "Restated Date": "2026-04-30",
            "Shares (Basic)": "2000",
            "Shares (Diluted)": "2100",
            "Revenue": "50",
            "Gross Profit": "10",
            "Operating Income (Loss)": "-10",
            "Net Income": "-12",
        },
    ]
    balance_rows = [
        {
            "Ticker": "AAPL",
            "SimFinId": "111",
            "Currency": "USD",
            "Fiscal Year": "2026",
            "Fiscal Period": "Q1",
            "Report Date": "2025-12-27",
            "Publish Date": "2026-01-30",
            "Restated Date": "2026-01-30",
            "Shares (Basic)": "1000",
            "Shares (Diluted)": "1100",
            "Cash, Cash Equivalents & Short Term Investments": "25",
            "Short Term Debt": "5",
            "Long Term Debt": "10",
        }
    ]
    if not partial_balance:
        balance_rows.append(
            {
                "Ticker": "MISSDA",
                "SimFinId": "222",
                "Currency": "USD",
                "Fiscal Year": "2026",
                "Fiscal Period": "Q1",
                "Report Date": "2026-03-31",
                "Publish Date": "2026-04-30",
                "Restated Date": "2026-04-30",
                "Shares (Basic)": "2000",
                "Shares (Diluted)": "2100",
                "Cash, Cash Equivalents & Short Term Investments": "5",
                "Short Term Debt": "",
                "Long Term Debt": "",
            }
        )
    cashflow_rows = [
        {
            "Ticker": "AAPL",
            "SimFinId": "111",
            "Currency": "USD",
            "Fiscal Year": "2026",
            "Fiscal Period": "Q1",
            "Report Date": "2025-12-27",
            "Publish Date": "2026-01-30",
            "Restated Date": "2026-01-30",
            "Shares (Basic)": "1000",
            "Shares (Diluted)": "1100",
            "Depreciation & Amortization": "5",
            "Net Cash from Operating Activities": "30",
            "Change in Fixed Assets & Intangibles": "-7",
        },
        {
            "Ticker": "MISSDA",
            "SimFinId": "222",
            "Currency": "USD",
            "Fiscal Year": "2026",
            "Fiscal Period": "Q1",
            "Report Date": "2026-03-31",
            "Publish Date": "2026-04-30",
            "Restated Date": "2026-04-30",
            "Shares (Basic)": "2000",
            "Shares (Diluted)": "2100",
            "Depreciation & Amortization": "" if missing_da else "3",
            "Net Cash from Operating Activities": "4",
            "Change in Fixed Assets & Intangibles": "-1",
        },
    ]
    _write_csv(root / "us-companies.csv", company_rows)
    _write_csv(root / "us-income-quarterly.csv", income_rows)
    _write_csv(root / "us-balance-quarterly.csv", balance_rows)
    _write_csv(root / "us-cashflow-quarterly.csv", cashflow_rows)
    return root


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
