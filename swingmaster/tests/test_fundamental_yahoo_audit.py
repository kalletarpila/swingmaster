from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

import pandas as pd

from swingmaster.cli import run_fundamental_yahoo_audit
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.providers import yahoo


def test_deterministic_symbol_sorting() -> None:
    assert run_fundamental_yahoo_audit.normalize_symbols("KNEBV.HE,NOKIA.HE,AAK.HE") == [
        "AAK.HE",
        "KNEBV.HE",
        "NOKIA.HE",
    ]


def test_payload_hash_is_stable_for_different_key_order() -> None:
    payload_left = {
        "info": {"sector": "Tech", "currency": "EUR"},
        "fast_info": {"currency": "EUR"},
        "quarterly_income_stmt": {"index": ["Revenue"], "columns": ["2024-12-31"], "data": [[1.0]]},
        "quarterly_balance_sheet": {"index": [], "columns": [], "data": []},
        "quarterly_cashflow": {"index": [], "columns": [], "data": []},
    }
    payload_right = {
        "quarterly_cashflow": {"columns": [], "data": [], "index": []},
        "quarterly_balance_sheet": {"data": [], "columns": [], "index": []},
        "fast_info": {"currency": "EUR"},
        "quarterly_income_stmt": {"data": [[1.0]], "index": ["Revenue"], "columns": ["2024-12-31"]},
        "info": {"currency": "EUR", "sector": "Tech"},
    }

    assert run_fundamental_yahoo_audit.compute_payload_hash(payload_left) == (
        run_fundamental_yahoo_audit.compute_payload_hash(payload_right)
    )


def test_provider_get_raw_payload_serializes_statements_and_info(monkeypatch) -> None:
    income = pd.DataFrame({"2024-12-31": [100.0]}, index=["Total Revenue"])
    balance = pd.DataFrame({"2024-12-31": [50.0]}, index=["Cash And Cash Equivalents"])
    cashflow = pd.DataFrame({"2024-12-31": [20.0]}, index=["Operating Cash Flow"])

    class _FakeTicker:
        quarterly_income_stmt = income
        quarterly_balance_sheet = balance
        quarterly_cashflow = cashflow
        info = {"currency": "EUR", "longName": "Nokia Oyj"}
        fast_info = {"currency": "EUR", "market_cap": 123}

    class _FakeYFinanceModule:
        @staticmethod
        def Ticker(symbol: str) -> _FakeTicker:
            assert symbol == "NOKIA.HE"
            return _FakeTicker()

    monkeypatch.setattr(yahoo, "_get_yfinance_module", lambda: _FakeYFinanceModule())

    client = yahoo.YahooFinanceClient()
    payload = client.get_raw_payload("NOKIA.HE")

    assert payload["info"] == {"currency": "EUR", "longName": "Nokia Oyj"}
    assert payload["fast_info"] == {"currency": "EUR", "market_cap": 123}
    assert payload["quarterly_income_stmt"] == {
        "index": ["Total Revenue"],
        "columns": ["2024-12-31"],
        "data": [[100.0]],
    }


def test_status_classification_ok_empty_error_and_dry_run(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_audit.db"
    run_migration(db_path)

    class _FakeYahooFinanceClient:
        def get_raw_payload(self, symbol: str) -> dict:
            if symbol == "AAA.HE":
                return {
                    "info": {},
                    "fast_info": {},
                    "quarterly_income_stmt": {"index": ["Revenue"], "columns": ["2024-12-31"], "data": [[10.0]]},
                    "quarterly_balance_sheet": {"index": [], "columns": [], "data": []},
                    "quarterly_cashflow": {"index": [], "columns": [], "data": []},
                }
            if symbol == "BBB.HE":
                return {
                    "info": {},
                    "fast_info": {},
                    "quarterly_income_stmt": {"index": [], "columns": [], "data": []},
                    "quarterly_balance_sheet": {"index": [], "columns": [], "data": []},
                    "quarterly_cashflow": {"index": [], "columns": [], "data": []},
                }
            raise RuntimeError(f"FETCH_FAILED:{symbol}")

    monkeypatch.setattr(run_fundamental_yahoo_audit, "YahooFinanceClient", lambda: _FakeYahooFinanceClient())

    summary = run_fundamental_yahoo_audit.run_yahoo_audit(
        db_path=db_path,
        market="omxh",
        exchange="HE",
        symbols_arg="BBB.HE,CCC.HE,AAA.HE",
        limit=None,
        run_id="RUN1",
        dry_run=True,
    )

    assert summary == {
        "market": "omxh",
        "exchange": "HE",
        "symbols_total": 3,
        "symbols_processed": 3,
        "ok_count": 1,
        "empty_count": 1,
        "error_count": 1,
        "rows_written": 0,
        "dry_run": "true",
        "run_id": "RUN1",
    }

    with sqlite3.connect(str(db_path)) as conn:
        row_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_yahoo_raw").fetchone()[0]
    assert row_count == 0


def test_status_classification_persists_rows(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_audit_persist.db"
    run_migration(db_path)

    class _FakeYahooFinanceClient:
        def get_raw_payload(self, symbol: str) -> dict:
            assert symbol == "NOKIA.HE"
            return {
                "info": {"currency": "EUR"},
                "fast_info": {"currency": "EUR"},
                "quarterly_income_stmt": {"index": ["Revenue"], "columns": ["2024-12-31"], "data": [[10.0]]},
                "quarterly_balance_sheet": {"index": [], "columns": [], "data": []},
                "quarterly_cashflow": {"index": [], "columns": [], "data": []},
            }

    monkeypatch.setattr(run_fundamental_yahoo_audit, "YahooFinanceClient", lambda: _FakeYahooFinanceClient())

    summary = run_fundamental_yahoo_audit.run_yahoo_audit(
        db_path=db_path,
        market="omxh",
        exchange="HE",
        symbols_arg=None,
        limit=None,
        run_id="RUN2",
        dry_run=False,
    )

    assert summary["symbols_total"] == 1
    assert summary["symbols_processed"] == 1
    assert summary["ok_count"] == 1
    assert summary["rows_written"] == 1

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT symbol, status, run_id, info_json, error_message
            FROM rc_fundamental_yahoo_raw
            ORDER BY id
            """
        ).fetchall()
    assert rows == [
        (
            "NOKIA.HE",
            "OK",
            "RUN2",
            run_fundamental_yahoo_audit.canonical_json_dumps({"currency": "EUR"}),
            None,
        )
    ]


def test_cli_summary_output(monkeypatch, capsys, tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo_audit_cli.db"
    run_migration(db_path)
    monkeypatch.setattr(
        run_fundamental_yahoo_audit,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="omxh",
            exchange="HE",
            symbols=None,
            limit=None,
            run_id="RUN3",
            dry_run=True,
        ),
    )
    monkeypatch.setattr(
        run_fundamental_yahoo_audit,
        "run_yahoo_audit",
        lambda **kwargs: {
            "market": "omxh",
            "exchange": "HE",
            "symbols_total": 1,
            "symbols_processed": 1,
            "ok_count": 0,
            "empty_count": 1,
            "error_count": 0,
            "rows_written": 0,
            "dry_run": "true",
            "run_id": "RUN3",
        },
    )

    run_fundamental_yahoo_audit.main()
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [
        "SUMMARY market=omxh",
        "SUMMARY exchange=HE",
        "SUMMARY symbols_total=1",
        "SUMMARY symbols_processed=1",
        "SUMMARY ok_count=0",
        "SUMMARY empty_count=1",
        "SUMMARY error_count=0",
        "SUMMARY rows_written=0",
        "SUMMARY dry_run=true",
        "SUMMARY run_id=RUN3",
    ]
