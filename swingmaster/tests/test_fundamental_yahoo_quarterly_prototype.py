from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

from swingmaster.cli import run_fundamental_yahoo_quarterly_prototype
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_yahoo_audit import canonical_json_dumps


def _insert_yahoo_raw_row(
    db_path: Path,
    *,
    symbol: str,
    info: dict,
    fast_info: dict,
    income: dict,
    balance: dict,
    cashflow: dict,
    run_id: str = "RAW1",
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_yahoo_raw (
                market,
                provider,
                symbol,
                info_json,
                fast_info_json,
                quarterly_income_stmt_json,
                quarterly_balance_sheet_json,
                quarterly_cashflow_json,
                payload_hash,
                status,
                error_message,
                loaded_at_utc,
                run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "fin",
                "yahoo",
                symbol,
                canonical_json_dumps(info),
                canonical_json_dumps(fast_info),
                canonical_json_dumps(income),
                canonical_json_dumps(balance),
                canonical_json_dumps(cashflow),
                "hash-1",
                "OK",
                None,
                "2026-05-03T10:23:06+00:00",
                run_id,
            ),
        )
        conn.commit()


def test_build_rows_prefers_ordinary_then_issued_minus_treasury_then_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "prototype_priority.db"
    run_migration(db_path)
    _insert_yahoo_raw_row(
        db_path,
        symbol="NOKIA.HE",
        info={"sharesOutstanding": 999.0},
        fast_info={"shares": 888.0},
        income={
            "index": ["Total Revenue", "Gross Profit", "Operating Income", "EBIT", "EBITDA", "Net Income"],
            "columns": ["2024-12-31", "2025-03-31", "2025-06-30"],
            "data": [
                [1000.0, 1100.0, 1200.0],
                [400.0, 420.0, 430.0],
                [200.0, 210.0, 220.0],
                [190.0, 205.0, 215.0],
                [250.0, 265.0, 275.0],
                [150.0, 160.0, 170.0],
            ],
        },
        balance={
            "index": ["Ordinary Shares Number", "Share Issued", "Treasury Shares Number", "Cash And Cash Equivalents", "Total Debt"],
            "columns": ["2024-12-31", "2025-03-31", "2025-06-30"],
            "data": [
                [100.0, None, None],
                [150.0, 160.0, None],
                [25.0, 30.0, None],
                [50.0, 55.0, 60.0],
                [300.0, 290.0, 280.0],
            ],
        },
        cashflow={
            "index": ["Operating Cash Flow", "Capital Expenditure"],
            "columns": ["2024-12-31", "2025-03-31", "2025-06-30"],
            "data": [
                [80.0, 85.0, 90.0],
                [-20.0, -25.0, -30.0],
            ],
        },
    )

    result = run_fundamental_yahoo_quarterly_prototype.run_yahoo_quarterly_prototype(db_path, "NOKIA.HE")

    assert result["rows"] == [
        {
            "period_end_date": "2024-12-31",
            "revenue": 1000.0,
            "gross_profit": 400.0,
            "operating_income": 200.0,
            "ebit": 190.0,
            "ebitda": 250.0,
            "net_income": 150.0,
            "operating_cashflow": 80.0,
            "capex": -20.0,
            "free_cashflow": 60.0,
            "cash": 50.0,
            "total_debt": 300.0,
            "shares_outstanding": 100.0,
            "shares_source": "ordinary_shares_number",
            "shares_quality": "OK",
        },
        {
            "period_end_date": "2025-03-31",
            "revenue": 1100.0,
            "gross_profit": 420.0,
            "operating_income": 210.0,
            "ebit": 205.0,
            "ebitda": 265.0,
            "net_income": 160.0,
            "operating_cashflow": 85.0,
            "capex": -25.0,
            "free_cashflow": 60.0,
            "cash": 55.0,
            "total_debt": 290.0,
            "shares_outstanding": 130.0,
            "shares_source": "issued_minus_treasury",
            "shares_quality": "REVIEW",
        },
        {
            "period_end_date": "2025-06-30",
            "revenue": 1200.0,
            "gross_profit": 430.0,
            "operating_income": 220.0,
            "ebit": 215.0,
            "ebitda": 275.0,
            "net_income": 170.0,
            "operating_cashflow": 90.0,
            "capex": -30.0,
            "free_cashflow": 60.0,
            "cash": 60.0,
            "total_debt": 280.0,
            "shares_outstanding": 999.0,
            "shares_source": "snapshot",
            "shares_quality": "REVIEW",
        },
    ]


def test_missing_when_no_valid_value_exists(tmp_path: Path) -> None:
    db_path = tmp_path / "prototype_missing.db"
    run_migration(db_path)
    _insert_yahoo_raw_row(
        db_path,
        symbol="NOKIA.HE",
        info={},
        fast_info={},
        income={"index": [], "columns": ["2024-12-31"], "data": []},
        balance={
            "index": ["Ordinary Shares Number", "Share Issued", "Treasury Shares Number"],
            "columns": ["2024-12-31"],
            "data": [[None], [0.0], [None]],
        },
        cashflow={"index": [], "columns": [], "data": []},
    )

    result = run_fundamental_yahoo_quarterly_prototype.run_yahoo_quarterly_prototype(db_path, "NOKIA.HE")

    assert result["rows"] == [
        {
            "period_end_date": "2024-12-31",
            "revenue": None,
            "gross_profit": None,
            "operating_income": None,
            "ebit": None,
            "ebitda": None,
            "net_income": None,
            "operating_cashflow": None,
            "capex": None,
            "free_cashflow": None,
            "cash": None,
            "total_debt": None,
            "shares_outstanding": None,
            "shares_source": "",
            "shares_quality": "MISSING",
        }
    ]


def test_burl_style_direct_ebit_and_ebitda_are_distinct_from_operating_income(tmp_path: Path) -> None:
    db_path = tmp_path / "prototype_burl_direct_ebit.db"
    run_migration(db_path)
    _insert_yahoo_raw_row(
        db_path,
        symbol="BURL",
        info={},
        fast_info={},
        income={
            "index": ["Operating Income", "EBIT", "EBITDA", "Net Income"],
            "columns": ["2026-04-30"],
            "data": [[167676000.0], [159164000.0], [263771000.0], [114744000.0]],
        },
        balance={"index": [], "columns": ["2026-04-30"], "data": []},
        cashflow={"index": [], "columns": ["2026-04-30"], "data": []},
    )

    result = run_fundamental_yahoo_quarterly_prototype.run_yahoo_quarterly_prototype(db_path, "BURL")

    row = result["rows"][0]
    assert row["operating_income"] == 167676000.0
    assert row["ebit"] == 159164000.0
    assert row["ebitda"] == 263771000.0


def test_operating_income_does_not_silently_populate_ebit_when_direct_ebit_absent(tmp_path: Path) -> None:
    db_path = tmp_path / "prototype_no_ebit_proxy.db"
    run_migration(db_path)
    _insert_yahoo_raw_row(
        db_path,
        symbol="AAPL",
        info={},
        fast_info={},
        income={
            "index": ["Operating Income", "Net Income"],
            "columns": ["2026-03-31"],
            "data": [[321.0], [123.0]],
        },
        balance={"index": [], "columns": ["2026-03-31"], "data": []},
        cashflow={"index": [], "columns": ["2026-03-31"], "data": []},
    )

    result = run_fundamental_yahoo_quarterly_prototype.run_yahoo_quarterly_prototype(db_path, "AAPL")

    row = result["rows"][0]
    assert row["operating_income"] == 321.0
    assert row["ebit"] is None
    assert row["ebitda"] is None


def test_qoq_change_over_25_percent_is_review(tmp_path: Path) -> None:
    db_path = tmp_path / "prototype_review.db"
    run_migration(db_path)
    _insert_yahoo_raw_row(
        db_path,
        symbol="NOKIA.HE",
        info={},
        fast_info={},
        income={"index": [], "columns": ["2024-12-31", "2025-03-31"], "data": []},
        balance={
            "index": ["Ordinary Shares Number"],
            "columns": ["2024-12-31", "2025-03-31"],
            "data": [[100.0, 140.0]],
        },
        cashflow={"index": [], "columns": [], "data": []},
    )

    result = run_fundamental_yahoo_quarterly_prototype.run_yahoo_quarterly_prototype(db_path, "NOKIA.HE")

    assert result["rows"][0]["shares_quality"] == "OK"
    assert result["rows"][1]["shares_quality"] == "REVIEW"


def test_free_cashflow_uses_direct_value_when_present(tmp_path: Path) -> None:
    db_path = tmp_path / "prototype_fcf_direct.db"
    run_migration(db_path)
    _insert_yahoo_raw_row(
        db_path,
        symbol="NOKIA.HE",
        info={},
        fast_info={},
        income={"index": [], "columns": ["2024-12-31"], "data": []},
        balance={"index": [], "columns": ["2024-12-31"], "data": []},
        cashflow={
            "index": ["Operating Cash Flow", "Capital Expenditure", "Free Cash Flow"],
            "columns": ["2024-12-31"],
            "data": [[100.0], [-40.0], [70.0]],
        },
    )

    result = run_fundamental_yahoo_quarterly_prototype.run_yahoo_quarterly_prototype(db_path, "NOKIA.HE")

    assert result["rows"][0]["operating_cashflow"] == 100.0
    assert result["rows"][0]["capex"] == -40.0
    assert result["rows"][0]["free_cashflow"] == 70.0


def test_cli_summary_and_output(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "prototype_cli.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_yahoo_quarterly_prototype,
        "parse_args",
        lambda: Namespace(db=str(db_path), symbol="NOKIA.HE"),
    )
    monkeypatch.setattr(
        run_fundamental_yahoo_quarterly_prototype,
        "run_yahoo_quarterly_prototype",
        lambda _db_path, _symbol: {
            "symbol": "NOKIA.HE",
            "source_run_id": "RAW1",
            "periods_total": 2,
            "rows_normalized": 2,
            "ok_count": 1,
            "review_count": 1,
            "missing_count": 0,
            "rows": [
                {
                    "period_end_date": "2024-12-31",
                    "revenue": 1000.0,
                    "gross_profit": 400.0,
                    "operating_income": 200.0,
                    "ebit": 190.0,
                    "ebitda": 250.0,
                    "net_income": 150.0,
                    "operating_cashflow": 80.0,
                    "capex": -20.0,
                    "free_cashflow": 60.0,
                    "cash": 50.0,
                    "total_debt": 300.0,
                    "shares_outstanding": 100.0,
                    "shares_source": "ordinary_shares_number",
                    "shares_quality": "OK",
                },
                {
                    "period_end_date": "2025-03-31",
                    "revenue": 1100.0,
                    "gross_profit": 420.0,
                    "operating_income": 210.0,
                    "ebit": 205.0,
                    "ebitda": 265.0,
                    "net_income": 160.0,
                    "operating_cashflow": 85.0,
                    "capex": -25.0,
                    "free_cashflow": 60.0,
                    "cash": 55.0,
                    "total_debt": 290.0,
                    "shares_outstanding": 140.0,
                    "shares_source": "ordinary_shares_number",
                    "shares_quality": "REVIEW",
                },
            ],
        },
    )

    run_fundamental_yahoo_quarterly_prototype.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY symbol=NOKIA.HE",
        "SUMMARY source_run_id=RAW1",
        "SUMMARY periods_total=2",
        "SUMMARY rows_normalized=2",
        "SUMMARY ok_count=1",
        "SUMMARY review_count=1",
        "SUMMARY missing_count=0",
        "period_end_date\trevenue\tgross_profit\toperating_income\tebit\tebitda\tnet_income\toperating_cashflow\tcapex\tfree_cashflow\tcash\ttotal_debt\tshares_outstanding\tshares_source\tshares_quality",
        "2024-12-31\t1000.0\t400.0\t200.0\t190.0\t250.0\t150.0\t80.0\t-20.0\t60.0\t50.0\t300.0\t100.0\tordinary_shares_number\tOK",
        "2025-03-31\t1100.0\t420.0\t210.0\t205.0\t265.0\t160.0\t85.0\t-25.0\t60.0\t55.0\t290.0\t140.0\tordinary_shares_number\tREVIEW",
    ]
