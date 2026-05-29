from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_ttm_batch
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_quarterly_row(conn: sqlite3.Connection, ticker: str, period_end_date: str) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            gross_profit,
            operating_income,
            ebit,
            ebitda,
            net_income,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            shares_outstanding,
            run_id
        ) VALUES (?, ?, 100.0, 30.0, 20.0, 20.0, NULL, 10.0, 15.0, -5.0, 10.0, 50.0, 20.0, 1000.0, 'QTRFIX')
        """,
        (ticker, period_end_date),
    )


def _insert_four_quarters(conn: sqlite3.Connection, ticker: str) -> None:
    for period_end_date in ("2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31"):
        _insert_quarterly_row(conn, ticker, period_end_date)


def _insert_true_semiannual_periods(conn: sqlite3.Connection, ticker: str) -> None:
    for period_end_date in ("2025-01-31", "2025-07-31"):
        _insert_quarterly_row(conn, ticker, period_end_date)


def _insert_quarterly_missing_source_periods(conn: sqlite3.Connection, ticker: str) -> None:
    for period_end_date in ("2024-12-31", "2025-03-31", "2025-06-30", "2025-12-31", "2026-03-31"):
        _insert_quarterly_row(conn, ticker, period_end_date)


def test_loads_deterministic_ticker_universe_from_quarterly(tmp_path: Path) -> None:
    db_path = tmp_path / "ttm_batch_universe.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "MSFT")
        _insert_four_quarters(conn, "AAPL")
        _insert_four_quarters(conn, "LRCX")
        conn.commit()

    tickers = run_fundamental_ttm_batch.resolve_ticker_universe(
        db_path=db_path,
        market=None,
        ticker=None,
        tickers_arg=None,
        limit=None,
    )
    assert tickers == ["AAPL", "LRCX", "MSFT"]


def test_market_usa_excludes_he_tickers(tmp_path: Path) -> None:
    db_path = tmp_path / "ttm_batch_usa.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "AAPL")
        _insert_four_quarters(conn, "NOKIA.HE")
        conn.commit()

    tickers = run_fundamental_ttm_batch.resolve_ticker_universe(
        db_path=db_path,
        market="usa",
        ticker=None,
        tickers_arg=None,
        limit=None,
    )
    assert tickers == ["AAPL"]


def test_market_omxh_includes_only_he_tickers(tmp_path: Path) -> None:
    db_path = tmp_path / "ttm_batch_omxh.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "AAPL")
        _insert_four_quarters(conn, "NOKIA.HE")
        conn.commit()

    tickers = run_fundamental_ttm_batch.resolve_ticker_universe(
        db_path=db_path,
        market="omxh",
        ticker=None,
        tickers_arg=None,
        limit=None,
    )
    assert tickers == ["NOKIA.HE"]


def test_ticker_overrides_tickers_arg_and_market_mismatch_fails(tmp_path: Path) -> None:
    db_path = tmp_path / "ttm_batch_ticker_override.db"
    run_migration(db_path)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_TTM_BATCH_MARKET_TICKER_MISMATCH:AAPL"):
        run_fundamental_ttm_batch.resolve_ticker_universe(
            db_path=db_path,
            market="omxh",
            ticker="AAPL",
            tickers_arg="NOKIA.HE",
            limit=None,
        )


def test_tickers_list_is_normalized_and_sorted(tmp_path: Path) -> None:
    db_path = tmp_path / "ttm_batch_tickers.db"
    run_migration(db_path)

    tickers = run_fundamental_ttm_batch.resolve_ticker_universe(
        db_path=db_path,
        market=None,
        ticker=None,
        tickers_arg="msft,aapl,MSFT,lrcx",
        limit=None,
    )
    assert tickers == ["AAPL", "LRCX", "MSFT"]


def test_limit_applies_after_sorting(tmp_path: Path) -> None:
    db_path = tmp_path / "ttm_batch_limit.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "MSFT")
        _insert_four_quarters(conn, "AAPL")
        _insert_four_quarters(conn, "LRCX")
        conn.commit()

    tickers = run_fundamental_ttm_batch.resolve_ticker_universe(
        db_path=db_path,
        market=None,
        ticker=None,
        tickers_arg=None,
        limit=2,
    )
    assert tickers == ["AAPL", "LRCX"]


def test_successful_rebuild_calls_existing_ttm_logic_and_writes_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_success.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "AAPL")
        conn.commit()

    calls: list[tuple[str, bool, bool, str]] = []

    def _fake_run_quarterly_to_ttm(**kwargs):
        calls.append((kwargs["ticker"], kwargs["dry_run"], kwargs["replace_ticker"], kwargs["run_id"]))
        return {"rows_written": 2}

    monkeypatch.setattr(run_fundamental_ttm_batch, "run_quarterly_to_ttm", _fake_run_quarterly_to_ttm)

    summary = run_fundamental_ttm_batch.run_fundamental_ttm_batch(
        db_path=db_path,
        run_id="BASE",
        market=None,
        ticker=None,
        tickers_arg=None,
        limit=None,
        replace_ticker=True,
        dry_run=False,
    )

    assert calls == [("AAPL", False, True, "BASE__TTM")]
    assert summary["tickers_succeeded"] == 1
    assert summary["rows_written"] == 2


def test_omxh_quarterly_ticker_is_allowed_to_proceed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_omxh_quarterly_ok.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "NOKIA.HE")
        conn.commit()

    calls: list[str] = []

    def _fake_run_quarterly_to_ttm(**kwargs):
        calls.append(kwargs["ticker"])
        return {"rows_written": 1}

    monkeypatch.setattr(run_fundamental_ttm_batch, "run_quarterly_to_ttm", _fake_run_quarterly_to_ttm)

    summary = run_fundamental_ttm_batch.run_fundamental_ttm_batch(
        db_path=db_path,
        run_id="BASE",
        market="omxh",
        ticker=None,
        tickers_arg=None,
        limit=None,
        replace_ticker=False,
        dry_run=False,
    )

    assert calls == ["NOKIA.HE"]
    assert summary["reporting_frequency_quarterly_count"] == 1
    assert summary["tickers_succeeded"] == 1


def test_omxh_true_semiannual_ticker_is_skipped(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_omxh_semiannual_skip.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_true_semiannual_periods(conn, "PUUILO.HE")
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_ttm_batch,
        "run_quarterly_to_ttm",
        lambda **kwargs: pytest.fail("quarterly TTM should not run for TRUE_SEMIANNUAL"),
    )

    summary = run_fundamental_ttm_batch.run_fundamental_ttm_batch(
        db_path=db_path,
        run_id="BASE",
        market="omxh",
        ticker=None,
        tickers_arg=None,
        limit=None,
        replace_ticker=False,
        dry_run=False,
    )
    out = capsys.readouterr().out

    assert "STEP ttm=SKIPPED_SEMIANNUAL_TTM_NOT_IMPLEMENTED" in out
    assert summary["reporting_frequency_true_semiannual_skipped_count"] == 1
    assert summary["tickers_succeeded"] == 0


def test_omxh_quarterly_missing_source_period_ticker_is_skipped(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_omxh_missing_source_skip.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_missing_source_periods(conn, "TIETO.HE")
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_ttm_batch,
        "run_quarterly_to_ttm",
        lambda **kwargs: pytest.fail("quarterly TTM should not run for QUARTERLY_MISSING_SOURCE_PERIOD"),
    )

    summary = run_fundamental_ttm_batch.run_fundamental_ttm_batch(
        db_path=db_path,
        run_id="BASE",
        market="omxh",
        ticker=None,
        tickers_arg=None,
        limit=None,
        replace_ticker=False,
        dry_run=False,
    )
    out = capsys.readouterr().out

    assert "STEP ttm=SKIPPED_QUARTERLY_MISSING_SOURCE_PERIOD" in out
    assert summary["reporting_frequency_quarterly_missing_source_period_skipped_count"] == 1
    assert summary["tickers_succeeded"] == 0


def test_omxh_malformed_dates_are_skipped(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_omxh_unknown_skip.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_quarterly_row(conn, "BROKE.HE", "2025-03-31")
        _insert_quarterly_row(conn, "BROKE.HE", "2025-06-30")
        _insert_quarterly_row(conn, "BROKE.HE", "bad-date")
        conn.commit()

    monkeypatch.setattr(
        run_fundamental_ttm_batch,
        "run_quarterly_to_ttm",
        lambda **kwargs: pytest.fail("quarterly TTM should not run for UNKNOWN"),
    )

    summary = run_fundamental_ttm_batch.run_fundamental_ttm_batch(
        db_path=db_path,
        run_id="BASE",
        market="omxh",
        ticker=None,
        tickers_arg=None,
        limit=None,
        replace_ticker=False,
        dry_run=False,
    )
    out = capsys.readouterr().out

    assert "STEP ttm=SKIPPED_UNKNOWN" in out
    assert summary["reporting_frequency_other_skipped_count"] == 1
    assert summary["tickers_succeeded"] == 0


def test_usa_behavior_is_not_blocked_by_reporting_frequency_gate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_usa_gate_noop.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "AAPL")
        conn.commit()

    calls: list[str] = []

    def _fake_run_quarterly_to_ttm(**kwargs):
        calls.append(kwargs["ticker"])
        return {"rows_written": 2}

    monkeypatch.setattr(run_fundamental_ttm_batch, "run_quarterly_to_ttm", _fake_run_quarterly_to_ttm)

    summary = run_fundamental_ttm_batch.run_fundamental_ttm_batch(
        db_path=db_path,
        run_id="BASE",
        market="usa",
        ticker=None,
        tickers_arg=None,
        limit=None,
        replace_ticker=False,
        dry_run=False,
    )

    assert calls == ["AAPL"]
    assert summary["reporting_frequency_quarterly_count"] == 0
    assert summary["tickers_succeeded"] == 1


def test_insufficient_rows_does_not_fail_batch(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_skip.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "AAPL")
        _insert_quarterly_row(conn, "DGXX", "2025-12-31")
        conn.commit()

    def _fake_run_quarterly_to_ttm(**kwargs):
        if kwargs["ticker"] == "DGXX":
            raise RuntimeError("FUNDAMENTAL_TTM_INSUFFICIENT_ROWS:DGXX")
        return {"rows_written": 2}

    monkeypatch.setattr(run_fundamental_ttm_batch, "run_quarterly_to_ttm", _fake_run_quarterly_to_ttm)

    summary = run_fundamental_ttm_batch.run_fundamental_ttm_batch(
        db_path=db_path,
        run_id="BASE",
        market=None,
        ticker=None,
        tickers_arg=None,
        limit=None,
        replace_ticker=False,
        dry_run=False,
    )
    out = capsys.readouterr().out

    assert "TICKER DGXX" in out
    assert "STEP ttm=SKIPPED_INSUFFICIENT_ROWS" in out
    assert summary["tickers_skipped_insufficient_rows"] == 1
    assert summary["tickers_succeeded"] == 1


def test_unexpected_ttm_error_fails_batch_immediately(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_fail.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "AAPL")
        _insert_four_quarters(conn, "MSFT")
        conn.commit()

    calls: list[str] = []

    def _fake_run_quarterly_to_ttm(**kwargs):
        calls.append(kwargs["ticker"])
        if kwargs["ticker"] == "AAPL":
            raise RuntimeError("FUNDAMENTAL_TTM_BROKE")
        return {"rows_written": 2}

    monkeypatch.setattr(run_fundamental_ttm_batch, "run_quarterly_to_ttm", _fake_run_quarterly_to_ttm)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_TTM_BROKE"):
        run_fundamental_ttm_batch.run_fundamental_ttm_batch(
            db_path=db_path,
            run_id="BASE",
            market=None,
            ticker=None,
            tickers_arg=None,
            limit=None,
            replace_ticker=False,
            dry_run=False,
        )
    out = capsys.readouterr().out

    assert calls == ["AAPL"]
    assert "TICKER AAPL=FAILED" in out
    assert "ERROR ticker=AAPL step=ttm" in out


def test_dry_run_writes_nothing_and_disables_replace_delete(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_dry.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "AAPL")
        conn.commit()

    calls: list[tuple[bool, bool]] = []

    def _fake_run_quarterly_to_ttm(**kwargs):
        calls.append((kwargs["dry_run"], kwargs["replace_ticker"]))
        return {"rows_written": 0}

    monkeypatch.setattr(run_fundamental_ttm_batch, "run_quarterly_to_ttm", _fake_run_quarterly_to_ttm)

    summary = run_fundamental_ttm_batch.run_fundamental_ttm_batch(
        db_path=db_path,
        run_id="BASE",
        market=None,
        ticker=None,
        tickers_arg=None,
        limit=None,
        replace_ticker=True,
        dry_run=True,
    )

    assert calls == [(True, False)]
    assert summary["dry_run"] == 1
    assert summary["rows_written"] == 0


def test_child_run_id_derivation_is_correct() -> None:
    assert run_fundamental_ttm_batch.derive_child_run_id("USA_TTM_REBUILD_20260505") == "USA_TTM_REBUILD_20260505__TTM"


def test_replace_ticker_is_passed_through_correctly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "ttm_batch_replace.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_four_quarters(conn, "AAPL")
        conn.commit()

    seen: list[bool] = []

    def _fake_run_quarterly_to_ttm(**kwargs):
        seen.append(kwargs["replace_ticker"])
        return {"rows_written": 2}

    monkeypatch.setattr(run_fundamental_ttm_batch, "run_quarterly_to_ttm", _fake_run_quarterly_to_ttm)

    run_fundamental_ttm_batch.run_fundamental_ttm_batch(
        db_path=db_path,
        run_id="BASE",
        market=None,
        ticker=None,
        tickers_arg=None,
        limit=None,
        replace_ticker=True,
        dry_run=False,
    )
    assert seen == [True]
