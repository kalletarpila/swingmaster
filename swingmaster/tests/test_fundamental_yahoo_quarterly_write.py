from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

from swingmaster.cli import run_fundamental_yahoo_quarterly_write
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


def _base_fixture() -> dict:
    return {
        "info": {"sharesOutstanding": 5582534171.0},
        "fast_info": {"shares": 5582534171.0},
        "income": {
            "index": ["Total Revenue", "Gross Profit", "Operating Income", "Net Income"],
            "columns": ["2024-12-31", "2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"],
            "data": [
                [None, 4390000000.0, 4546000000.0, 4828000000.0, 6125000000.0, 4497000000.0],
                [None, 1824000000.0, 1971000000.0, 2110000000.0, 2754000000.0, 1988000000.0],
                [None, -21000000.0, 80000000.0, 239000000.0, 437000000.0, 63000000.0],
                [None, -59000000.0, 90000000.0, 78000000.0, 542000000.0, 86000000.0],
            ],
        },
        "balance": {
            "index": ["Ordinary Shares Number", "Cash And Cash Equivalents", "Total Debt"],
            "columns": ["2024-12-31", "2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"],
            "data": [
                [None, 5380831000.0, 5379095249.0, 5413481372.0, 5582534171.0, 5582534171.0],
                [None, 5543000000.0, 4797000000.0, 4892000000.0, 5462000000.0, 4951000000.0],
                [None, 5065000000.0, 4100000000.0, 4065000000.0, 4413000000.0, 3325000000.0],
            ],
        },
        "cashflow": {
            "index": ["Operating Cash Flow", "Capital Expenditure", "Free Cash Flow"],
            "columns": ["2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"],
            "data": [
                [890000000.0, 209000000.0, 597000000.0, 375000000.0, 783000000.0],
                [-169000000.0, -121000000.0, -168000000.0, -148000000.0, -154000000.0],
                [721000000.0, 88000000.0, 429000000.0, 227000000.0, 629000000.0],
            ],
        },
    }


def test_dry_run_writes_zero_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_write_dry.db"
    run_migration(db_path)
    fixture = _base_fixture()
    _insert_yahoo_raw_row(db_path, symbol="NOKIA.HE", run_id="RAW1", **fixture)

    result = run_fundamental_yahoo_quarterly_write.run_yahoo_quarterly_write(
        db_path=db_path,
        market="fin",
        symbol="NOKIA.HE",
        run_id="WRITE1",
        dry_run=True,
        replace_symbol=False,
    )

    assert result["rows_written"] == 0
    assert result["rows_skipped"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_yahoo_quarterly").fetchone()[0]
    assert count == 0


def test_writer_skips_snapshot_only_row_and_writes_five_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_write_rows.db"
    run_migration(db_path)
    fixture = _base_fixture()
    _insert_yahoo_raw_row(db_path, symbol="NOKIA.HE", run_id="RAW1", **fixture)

    result = run_fundamental_yahoo_quarterly_write.run_yahoo_quarterly_write(
        db_path=db_path,
        market="fin",
        symbol="NOKIA.HE",
        run_id="WRITE2",
        dry_run=False,
        replace_symbol=False,
    )

    assert result["periods_total"] == 6
    assert result["rows_normalized"] == 6
    assert result["rows_skipped"] == 1
    assert result["rows_written"] == 5
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT period_end_date, shares_source, shares_quality
            FROM rc_fundamental_yahoo_quarterly
            ORDER BY period_end_date
            """
        ).fetchall()
    assert rows == [
        ("2025-03-31", "ordinary_shares_number", "OK"),
        ("2025-06-30", "ordinary_shares_number", "OK"),
        ("2025-09-30", "ordinary_shares_number", "OK"),
        ("2025-12-31", "ordinary_shares_number", "OK"),
        ("2026-03-31", "ordinary_shares_number", "OK"),
    ]


def test_replace_symbol_deletes_existing_rows_before_insert(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_write_replace.db"
    run_migration(db_path)
    fixture = _base_fixture()
    _insert_yahoo_raw_row(db_path, symbol="NOKIA.HE", run_id="RAW1", **fixture)

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_yahoo_quarterly (
                market, symbol, period_end_date, shares_outstanding, shares_source, shares_quality, source_run_id, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("fin", "NOKIA.HE", "2025-03-31", 1.0, "snapshot", "REVIEW", "OLDRAW", "OLDRUN", "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()

    result = run_fundamental_yahoo_quarterly_write.run_yahoo_quarterly_write(
        db_path=db_path,
        market="fin",
        symbol="NOKIA.HE",
        run_id="WRITE3",
        dry_run=False,
        replace_symbol=True,
    )

    assert result["rows_deleted"] == 1
    assert result["rows_written"] == 5
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_yahoo_quarterly WHERE market='fin' AND symbol='NOKIA.HE'"
        ).fetchone()[0]
    assert count == 5


def test_cli_summary_output(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_write_cli.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_yahoo_quarterly_write,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="fin",
            symbol="NOKIA.HE",
            run_id="WRITE4",
            dry_run=True,
            replace_symbol=False,
        ),
    )
    monkeypatch.setattr(
        run_fundamental_yahoo_quarterly_write,
        "run_yahoo_quarterly_write",
        lambda **kwargs: {
            "market": "fin",
            "symbol": "NOKIA.HE",
            "source_run_id": "RAW1",
            "periods_total": 6,
            "rows_normalized": 6,
            "rows_skipped": 1,
            "rows_deleted": 0,
            "rows_written": 0,
            "dry_run": "true",
            "replace_symbol": "false",
            "run_id": "WRITE4",
            "rows": [],
        },
    )

    run_fundamental_yahoo_quarterly_write.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY market=fin",
        "SUMMARY symbol=NOKIA.HE",
        "SUMMARY source_run_id=RAW1",
        "SUMMARY periods_total=6",
        "SUMMARY rows_normalized=6",
        "SUMMARY rows_skipped=1",
        "SUMMARY rows_deleted=0",
        "SUMMARY rows_written=0",
        "SUMMARY dry_run=true",
        "SUMMARY replace_symbol=false",
        "SUMMARY run_id=WRITE4",
    ]
