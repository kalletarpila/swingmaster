from __future__ import annotations

import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_yahoo_raw_load
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_quarterly_ticker(db_path: Path, ticker: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                run_id
            ) VALUES (?, ?, ?)
            """,
            (ticker, "2025-12-31", "FIXTURE"),
        )
        conn.commit()


def test_universe_loading_uses_quarterly_and_excludes_he(tmp_path: Path) -> None:
    db_path = tmp_path / "raw_load_universe.db"
    run_migration(db_path)
    _insert_quarterly_ticker(db_path, "AAPL")
    _insert_quarterly_ticker(db_path, "MSFT")
    _insert_quarterly_ticker(db_path, "NOKIA.HE")

    assert run_fundamental_yahoo_raw_load.load_usa_ticker_universe(db_path) == ["AAPL", "MSFT"]


def test_ticker_overrides_universe(tmp_path: Path) -> None:
    db_path = tmp_path / "raw_load_ticker_override.db"
    run_migration(db_path)
    _insert_quarterly_ticker(db_path, "AAPL")

    assert run_fundamental_yahoo_raw_load.resolve_tickers(
        db_path=db_path,
        market="usa",
        ticker="lrcx",
        tickers_arg=None,
        limit_tickers=None,
    ) == ["LRCX"]


def test_tickers_list_is_normalized_and_sorted_deterministically() -> None:
    assert run_fundamental_yahoo_raw_load.normalize_tickers("msft,LRCX,aapl,MSFT") == ["AAPL", "LRCX", "MSFT"]


def test_limit_tickers_applies_after_sorting(tmp_path: Path) -> None:
    db_path = tmp_path / "raw_load_limit.db"
    run_migration(db_path)
    _insert_quarterly_ticker(db_path, "MSFT")
    _insert_quarterly_ticker(db_path, "AAPL")
    _insert_quarterly_ticker(db_path, "LRCX")

    assert run_fundamental_yahoo_raw_load.resolve_tickers(
        db_path=db_path,
        market="usa",
        ticker=None,
        tickers_arg=None,
        limit_tickers=2,
    ) == ["AAPL", "LRCX"]


def test_batching_splits_deterministically() -> None:
    assert run_fundamental_yahoo_raw_load.chunk_tickers(["AAPL", "LRCX", "MSFT", "NVDA", "TSLA"], 2) == [
        ["AAPL", "LRCX"],
        ["MSFT", "NVDA"],
        ["TSLA"],
    ]


def test_child_run_id_derivation_is_correct() -> None:
    assert run_fundamental_yahoo_raw_load.derive_batch_run_id("USA_YAHOO_RAW_20260505", 1) == (
        "USA_YAHOO_RAW_20260505__RAW__B0001"
    )
    assert run_fundamental_yahoo_raw_load.derive_batch_run_id("USA_YAHOO_RAW_20260505", 12) == (
        "USA_YAHOO_RAW_20260505__RAW__B0012"
    )


def test_dry_run_does_not_call_underlying_raw_loader(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "raw_load_dry_run.db"
    run_migration(db_path)
    _insert_quarterly_ticker(db_path, "AAPL")
    calls: list[tuple[str, ...]] = []

    def _fake_run_batch(**kwargs):
        calls.append(tuple(kwargs["tickers"]))
        return {}

    monkeypatch.setattr(run_fundamental_yahoo_raw_load, "run_batch", _fake_run_batch)

    summary = run_fundamental_yahoo_raw_load.run_fundamental_yahoo_raw_load(
        db_path=db_path,
        market="usa",
        run_id="BASE",
        ticker=None,
        tickers_arg=None,
        limit_tickers=None,
        batch_size=100,
        dry_run=True,
    )
    out = capsys.readouterr().out

    assert calls == []
    assert "BATCH 1/1 tickers=1 run_id=BASE__RAW__B0001" in out
    assert summary["dry_run"] == 1
    assert summary["tickers_processed"] == 0


def test_batch_failure_propagates_and_stops_execution(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "raw_load_failure.db"
    run_migration(db_path)
    _insert_quarterly_ticker(db_path, "AAPL")
    _insert_quarterly_ticker(db_path, "LRCX")
    calls: list[tuple[str, ...]] = []

    def _fake_run_batch(**kwargs):
        calls.append(tuple(kwargs["tickers"]))
        if kwargs["batch_run_id"].endswith("B0002"):
            raise RuntimeError("BATCH_BROKE")
        return {
            "ok_count": 1,
            "empty_count": 0,
            "error_count": 0,
            "rows_written": 1,
        }

    monkeypatch.setattr(run_fundamental_yahoo_raw_load, "run_batch", _fake_run_batch)

    with pytest.raises(RuntimeError, match="^BATCH_BROKE$"):
        run_fundamental_yahoo_raw_load.run_fundamental_yahoo_raw_load(
            db_path=db_path,
            market="usa",
            run_id="BASE",
            ticker=None,
            tickers_arg=None,
            limit_tickers=None,
            batch_size=1,
            dry_run=False,
        )
    out = capsys.readouterr().out

    assert calls == [("AAPL",), ("LRCX",)]
    assert "BATCH 2=FAILED" in out
    assert "ERROR batch_run_id=BASE__RAW__B0002" in out


def test_batch_summaries_aggregate_correctly(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "raw_load_aggregate.db"
    run_migration(db_path)
    _insert_quarterly_ticker(db_path, "AAPL")
    _insert_quarterly_ticker(db_path, "LRCX")
    _insert_quarterly_ticker(db_path, "MSFT")

    def _fake_run_batch(**kwargs):
        if kwargs["batch_run_id"].endswith("B0001"):
            return {
                "ok_count": 1,
                "empty_count": 1,
                "error_count": 0,
                "rows_written": 2,
            }
        return {
            "ok_count": 0,
            "empty_count": 0,
            "error_count": 1,
            "rows_written": 1,
        }

    monkeypatch.setattr(run_fundamental_yahoo_raw_load, "run_batch", _fake_run_batch)

    summary = run_fundamental_yahoo_raw_load.run_fundamental_yahoo_raw_load(
        db_path=db_path,
        market="usa",
        run_id="BASE",
        ticker=None,
        tickers_arg=None,
        limit_tickers=None,
        batch_size=2,
        dry_run=False,
    )

    assert summary == {
        "market": "usa",
        "tickers_total": 3,
        "tickers_processed": 3,
        "batch_size": 2,
        "batches_total": 2,
        "batches_executed": 2,
        "ok_count": 1,
        "empty_count": 1,
        "error_count": 1,
        "rows_written": 3,
        "dry_run": 0,
        "run_id": "BASE",
    }


def test_main_rejects_non_usa_market(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "raw_load_market.db"
    monkeypatch.setattr(
        run_fundamental_yahoo_raw_load,
        "parse_args",
        lambda: Namespace(
            db=str(db_path),
            market="omxh",
            run_id="BASE",
            ticker=None,
            tickers=None,
            limit_tickers=None,
            batch_size=100,
            dry_run=False,
        ),
    )

    with pytest.raises(SystemExit, match="YAHOO_RAW_LOAD_UNSUPPORTED_MARKET:omxh"):
        run_fundamental_yahoo_raw_load.main()
