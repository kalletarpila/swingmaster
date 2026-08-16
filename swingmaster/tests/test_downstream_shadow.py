from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.downstream_shadow import (
    LegacyFundamentalReader,
    SourceMode,
    V2FundamentalReader,
    run_shadow_for_ticker,
)


def test_v2_reader_maps_exact_quarter_identity_and_result_date(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _create_v2_db(db)
    reader = V2FundamentalReader(db)

    rows = reader.load_quarters("AAA")

    assert rows[-1].ticker == "AAA"
    assert rows[-1].fiscal_year == 2026
    assert rows[-1].fiscal_period == "Q1"
    assert rows[-1].period_end_date == "2026-03-31"
    assert rows[-1].result_publication_date == "2026-04-20"
    assert rows[-1].shares_outstanding == 100.0


def test_shadow_mode_returns_legacy_output_and_v2_shadow(tmp_path: Path) -> None:
    legacy_db = tmp_path / "legacy.db"
    v2_db = tmp_path / "v2.db"
    _create_legacy_db(legacy_db)
    _create_v2_db(v2_db)

    result = run_shadow_for_ticker(
        LegacyFundamentalReader(legacy_db),
        V2FundamentalReader(v2_db),
        ticker="AAA",
        mode=SourceMode.LEGACY_WITH_V2_SHADOW,
        close_price=10.0,
        valuation_date="2026-04-21",
    )

    assert result.production_source == "LEGACY"
    assert result.legacy_status == "OK"
    assert result.v2_status == "OK"
    assert result.production_output is not None
    assert result.v2_shadow_output is not None
    assert result.production_output["valuation"]["shares_outstanding"] == 100.0
    assert result.v2_shadow_output["valuation"]["shares_outstanding"] == 100.0


def test_shadow_v2_failure_does_not_break_legacy(tmp_path: Path) -> None:
    legacy_db = tmp_path / "legacy.db"
    missing_v2 = tmp_path / "missing_v2.db"
    _create_legacy_db(legacy_db)

    result = run_shadow_for_ticker(
        LegacyFundamentalReader(legacy_db),
        V2FundamentalReader(missing_v2),
        ticker="AAA",
        mode=SourceMode.LEGACY_WITH_V2_SHADOW,
        close_price=10.0,
        valuation_date="2026-04-21",
    )

    assert result.production_source == "LEGACY"
    assert result.legacy_status == "OK"
    assert result.production_output is not None
    assert result.v2_status == "FAILED"
    assert result.shadow_error is not None


def test_legacy_only_mode_has_no_v2_shadow(tmp_path: Path) -> None:
    legacy_db = tmp_path / "legacy.db"
    v2_db = tmp_path / "v2.db"
    _create_legacy_db(legacy_db)
    _create_v2_db(v2_db)

    result = run_shadow_for_ticker(
        LegacyFundamentalReader(legacy_db),
        V2FundamentalReader(v2_db),
        ticker="AAA",
        mode=SourceMode.LEGACY_ONLY,
        close_price=10.0,
        valuation_date="2026-04-21",
    )

    assert result.production_source == "LEGACY"
    assert result.v2_status is None
    assert result.v2_shadow_output is None


def test_v2_only_diagnostic_mode_works(tmp_path: Path) -> None:
    legacy_db = tmp_path / "legacy.db"
    v2_db = tmp_path / "v2.db"
    _create_legacy_db(legacy_db)
    _create_v2_db(v2_db)

    result = run_shadow_for_ticker(
        LegacyFundamentalReader(legacy_db),
        V2FundamentalReader(v2_db),
        ticker="AAA",
        mode=SourceMode.V2_ONLY,
        close_price=10.0,
        valuation_date="2026-04-21",
    )

    assert result.production_source == "V2_DIAGNOSTIC"
    assert result.legacy_status == "NOT_RUN"
    assert result.v2_status == "OK"


def _create_legacy_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE rc_fundamental_quarterly(
          ticker TEXT NOT NULL,
          period_end_date TEXT NOT NULL,
          revenue REAL,
          gross_profit REAL,
          operating_income REAL,
          ebit REAL,
          ebitda REAL,
          operating_cashflow REAL,
          capex REAL,
          free_cashflow REAL,
          cash REAL,
          total_debt REAL,
          shares_outstanding REAL
        )
        """
    )
    for period in ("2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"):
        conn.execute(
            "INSERT INTO rc_fundamental_quarterly VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("AAA", period, 100.0, 50.0, 30.0, 30.0, 40.0, 25.0, -5.0, 20.0, 10.0, 30.0, 100.0),
        )
    conn.commit()
    conn.close()


def _create_v2_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE rc_v2_company(company_id INTEGER PRIMARY KEY, ticker TEXT, company_profile TEXT, active INTEGER);
        CREATE TABLE rc_v2_quarter(quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_period TEXT, report_date TEXT, publish_date TEXT);
        CREATE TABLE rc_v2_fundamental_quarterly(
          quarter_id INTEGER PRIMARY KEY,
          revenue REAL,
          gross_profit REAL,
          operating_income REAL,
          ebit REAL,
          ebitda REAL,
          operating_cashflow REAL,
          capex REAL,
          free_cashflow REAL,
          cash REAL,
          total_debt REAL,
          shares_outstanding REAL
        );
        INSERT INTO rc_v2_company VALUES (1, 'AAA', 'ORDINARY', 1);
        """
    )
    rows = [
        (1, 2025, "Q2", "2025-06-30", "2025-07-20"),
        (2, 2025, "Q3", "2025-09-30", "2025-10-20"),
        (3, 2025, "Q4", "2025-12-31", "2026-01-20"),
        (4, 2026, "Q1", "2026-03-31", "2026-04-20"),
    ]
    for quarter_id, fy, fq, report_date, publish_date in rows:
        conn.execute("INSERT INTO rc_v2_quarter VALUES (?,?,?,?,?,?)", (quarter_id, 1, fy, fq, report_date, publish_date))
        conn.execute(
            "INSERT INTO rc_v2_fundamental_quarterly VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (quarter_id, 100.0, 50.0, 30.0, 30.0, 40.0, 25.0, -5.0, 20.0, 10.0, 30.0, 100.0),
        )
    conn.commit()
    conn.close()
