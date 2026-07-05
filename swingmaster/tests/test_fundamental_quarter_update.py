from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_state_row(
    db_path: Path,
    ticker: str,
    market: str,
    latest_db_period_end_date: str | None,
    detected_source_period_end_date: str | None,
    new_quarter_available: int,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_state (
                ticker,
                market,
                primary_source,
                latest_db_period_end_date,
                detected_source_period_end_date,
                new_quarter_available,
                last_updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                market,
                "yahoo" if market == "omxh" else "sec_edgar",
                latest_db_period_end_date,
                detected_source_period_end_date,
                new_quarter_available,
                "2026-05-05T00:00:00+00:00",
            ),
        )
        conn.commit()


def _insert_quarterly_row(db_path: Path, ticker: str, period_end_date: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (ticker, period_end_date, run_id)
            VALUES (?, ?, ?)
            """,
            (ticker, period_end_date, "FIXTURE"),
        )
        conn.commit()


def _insert_yahoo_quarterly_row(
    db_path: Path,
    *,
    market: str,
    symbol: str,
    period_end_date: str,
    revenue: float | None = None,
    gross_profit: float | None = None,
    operating_income: float | None = None,
    net_income: float | None = None,
    operating_cashflow: float | None = None,
    capex: float | None = None,
    free_cashflow: float | None = None,
    cash: float | None = None,
    total_debt: float | None = None,
    shares_outstanding: float | None = None,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_yahoo_quarterly (
                market, symbol, period_end_date, revenue, gross_profit, operating_income, net_income,
                operating_cashflow, capex, free_cashflow, cash, total_debt, shares_outstanding,
                shares_source, shares_quality, source_run_id, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market,
                symbol,
                period_end_date,
                revenue,
                gross_profit,
                operating_income,
                net_income,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt,
                shares_outstanding,
                "yahoo",
                "OK",
                "YRAW",
                "YRUN",
                "2026-05-03T00:00:00+00:00",
            ),
        )
        conn.commit()


def _mock_usa_valuation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "resolve_latest_close_as_of_date",
        lambda *_args, **_kwargs: "2026-05-08",
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_fundamental_valuation",
        lambda **_kwargs: {"rows_written": 0},
    )


def test_loads_only_flagged_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_flags.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_state_row(db_path, "MSFT", "usa", "2025-12-31", "2026-03-31", 0)

    rows = run_fundamental_quarter_update.load_eligible_rows(db_path, None, None, None)
    assert [str(row["ticker"]) for row in rows] == ["AAPL"]


def test_market_filter_works(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_market.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_state_row(db_path, "NOKIA.HE", "omxh", "2025-12-31", "2026-03-31", 1)

    rows = run_fundamental_quarter_update.load_eligible_rows(db_path, "usa", None, None)
    assert [str(row["ticker"]) for row in rows] == ["AAPL"]


def test_ticker_filter_works(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_ticker.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_state_row(db_path, "MSFT", "usa", "2025-12-31", "2026-03-31", 1)

    rows = run_fundamental_quarter_update.load_eligible_rows(db_path, None, "MSFT", None)
    assert [str(row["ticker"]) for row in rows] == ["MSFT"]


def test_limit_works_after_deterministic_sorting(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_limit.db"
    run_migration(db_path)
    _insert_state_row(db_path, "MSFT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_state_row(db_path, "LRCX", "usa", "2025-12-31", "2026-03-31", 1)

    rows = run_fundamental_quarter_update.load_eligible_rows(db_path, None, None, 2)
    assert [str(row["ticker"]) for row in rows] == ["AAPL", "LRCX"]


def test_dry_run_runs_nothing_and_writes_nothing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_dry.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", lambda **kwargs: (_ for _ in ()).throw(AssertionError()))

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=None,
        run_id="BASE",
        market=None,
        ticker=None,
        limit=None,
        dry_run=True,
        skip_ack=False,
    )
    out = capsys.readouterr().out

    assert "TICKER AAPL market=usa detected_period=2026-03-31" in out
    assert summary["dry_run"] == 1
    assert summary["tickers_processed"] == 0
    assert summary["tickers_succeeded"] == 0
    assert summary["tickers_failed"] == 0


def test_successful_run_executes_usa_steps_in_order_and_acknowledges(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_success.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "AAPL", "2026-03-31")
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_quarterly_refresh",
        lambda **kwargs: calls.append("quarterly_refresh") or {"mode": "enrich", "sec_refresh_required": False},
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_quarterly_to_ttm",
        lambda **kwargs: calls.append("ttm") or {"rows_written": 1},
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_lifecycle_step",
        lambda **kwargs: calls.append("lifecycle") or 1,
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_score_step",
        lambda **kwargs: calls.append("score") or 1,
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "acknowledge_ticker",
        lambda **kwargs: calls.append("ack") or 1,
    )
    _mock_usa_valuation(monkeypatch)

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "osakedata.db",
        run_id="BASE",
        market="usa",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
    )

    assert calls == ["quarterly_refresh", "ttm", "lifecycle", "score", "ack"]
    assert summary["tickers_processed"] == 1
    assert summary["tickers_succeeded"] == 1
    assert summary["tickers_failed"] == 0


def test_skip_ack_leaves_state_unchanged(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_skip_ack.db"
    run_migration(db_path)
    _insert_state_row(db_path, "NOKIA.HE", "omxh", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "NOKIA.HE", "2026-03-31")

    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_refresh", lambda **kwargs: {"mode": "yahoo_refresh"})
    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_to_ttm", lambda **kwargs: {"rows_written": 1})
    monkeypatch.setattr(run_fundamental_quarter_update, "run_lifecycle_step", lambda **kwargs: 1)
    monkeypatch.setattr(run_fundamental_quarter_update, "run_score_step", lambda **kwargs: 1)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=None,
        run_id="BASE",
        market="omxh",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=True,
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT new_quarter_available, detected_source_period_end_date FROM rc_fundamental_quarter_state WHERE ticker='NOKIA.HE'"
        ).fetchone()
    assert row == (1, "2026-03-31")


def test_single_ticker_failure_stops_processing_and_leaves_state_unchanged(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_fail.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_state_row(db_path, "MSFT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "AAPL", "2026-03-31")
    _insert_quarterly_row(db_path, "MSFT", "2026-03-31")
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_quarterly_refresh",
        lambda **kwargs: {"mode": "enrich", "sec_refresh_required": False},
    )

    def _fake_ttm(**kwargs):
        calls.append(kwargs["ticker"])
        if kwargs["ticker"] == "AAPL":
            raise RuntimeError("FUNDAMENTAL_TTM_BROKE")
        return {"rows_written": 1}

    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_to_ttm", _fake_ttm)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_TTM_BROKE"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="BASE",
            market=None,
            ticker="AAPL",
            limit=None,
            dry_run=False,
            skip_ack=False,
        )
    out = capsys.readouterr().out

    assert calls == ["AAPL"]
    assert "TICKER AAPL=FAILED" in out
    assert "ERROR ticker=AAPL step=ttm message=FUNDAMENTAL_TTM_BROKE" in out
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT new_quarter_available FROM rc_fundamental_quarter_state WHERE ticker='AAPL'"
        ).fetchone()
    assert row == (1,)


def test_batch_failure_continues_to_next_ticker_and_raises_final_batch_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_batch_continue.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_state_row(db_path, "MSFT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "AAPL", "2026-03-31")
    _insert_quarterly_row(db_path, "MSFT", "2026-03-31")
    calls: list[str] = []

    def _fake_process_ticker(**kwargs):
        ticker = str(kwargs["row"]["ticker"]).upper()
        calls.append(ticker)
        if ticker == "AAPL":
            raise RuntimeError("FUNDAMENTAL_TTM_BROKE")
        return {"score_rows_written": 1}

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _fake_process_ticker)
    _mock_usa_valuation(monkeypatch)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed=1"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=tmp_path / "osakedata.db",
            run_id="BASE",
            market="usa",
            ticker=None,
            limit=None,
            dry_run=False,
            skip_ack=False,
        )
    out = capsys.readouterr().out

    assert calls == ["AAPL", "MSFT"]
    assert "TICKER AAPL=FAILED" in out
    assert "ERROR ticker=AAPL step=ttm message=FUNDAMENTAL_TTM_BROKE" in out
    assert "SUMMARY tickers_total=2" in out
    assert "SUMMARY tickers_processed=2" in out
    assert "SUMMARY tickers_succeeded=1" in out
    assert "SUMMARY tickers_failed=1" in out


def test_batch_all_success_exits_cleanly_with_summary_counts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_batch_success.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_state_row(db_path, "MSFT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "AAPL", "2026-03-31")
    _insert_quarterly_row(db_path, "MSFT", "2026-03-31")
    calls: list[str] = []

    def _fake_process_ticker(**kwargs):
        ticker = str(kwargs["row"]["ticker"]).upper()
        calls.append(ticker)
        return {"score_rows_written": 1}

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _fake_process_ticker)
    _mock_usa_valuation(monkeypatch)

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "osakedata.db",
        run_id="BASE",
        market="usa",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
    )

    assert calls == ["AAPL", "MSFT"]
    assert summary["tickers_total"] == 2
    assert summary["tickers_processed"] == 2
    assert summary["tickers_succeeded"] == 2
    assert summary["tickers_failed"] == 0


def test_batch_multiple_failures_preserve_deterministic_processing_order(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_batch_multi_fail.db"
    run_migration(db_path)
    for ticker in ("AAPL", "MSFT", "NVDA"):
        _insert_state_row(db_path, ticker, "usa", "2025-12-31", "2026-03-31", 1)
        _insert_quarterly_row(db_path, ticker, "2026-03-31")
    calls: list[str] = []

    def _fake_process_ticker(**kwargs):
        ticker = str(kwargs["row"]["ticker"]).upper()
        calls.append(ticker)
        if ticker in {"AAPL", "MSFT"}:
            raise RuntimeError("FUNDAMENTAL_TTM_BROKE")
        return {"score_rows_written": 1}

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _fake_process_ticker)
    _mock_usa_valuation(monkeypatch)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed=2"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=tmp_path / "osakedata.db",
            run_id="BASE",
            market="usa",
            ticker=None,
            limit=None,
            dry_run=False,
            skip_ack=False,
        )
    out = capsys.readouterr().out

    assert calls == ["AAPL", "MSFT", "NVDA"]
    assert "SUMMARY tickers_processed=3" in out
    assert "SUMMARY tickers_succeeded=1" in out
    assert "SUMMARY tickers_failed=2" in out


def test_child_run_id_derivation_is_correct() -> None:
    assert run_fundamental_quarter_update.derive_child_run_ids("USA_QUARTER_UPDATE_20260505") == {
        "raw": "USA_QUARTER_UPDATE_20260505__RAW",
        "yqtr": "USA_QUARTER_UPDATE_20260505__YQTR",
        "qbridge": "USA_QUARTER_UPDATE_20260505__QBRIDGE",
        "ttm": "USA_QUARTER_UPDATE_20260505__TTM",
        "lifecycle": "USA_QUARTER_UPDATE_20260505__LIFECYCLE",
        "score": "USA_QUARTER_UPDATE_20260505__SCORE",
        "valuation": "USA_QUARTER_UPDATE_20260505__VALUATION",
        "ack": "USA_QUARTER_UPDATE_20260505__ACK",
        "enrich": "USA_QUARTER_UPDATE_20260505__ENRICH",
        "sec_raw": "USA_QUARTER_UPDATE_20260505__SEC_RAW",
        "sec_reconstruct": "USA_QUARTER_UPDATE_20260505__SEC_QUARTERLY_RECON",
        "quarterly": "USA_QUARTER_UPDATE_20260505__QUARTERLY",
    }


def test_invalid_state_missing_detected_date_fails(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_invalid_state.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", None, 1)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_DETECTED_DATE_MISSING:AAPL"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="BASE",
            market=None,
            ticker="AAPL",
            limit=None,
            dry_run=False,
            skip_ack=False,
        )


def test_non_usa_processing_uses_quarterly_refresh(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_nonusa.db"
    run_migration(db_path)
    _insert_state_row(db_path, "NOKIA.HE", "omxh", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "NOKIA.HE", "2026-03-31")
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_quarterly_refresh",
        lambda **kwargs: calls.append("quarterly_refresh") or {"mode": "yahoo_refresh", "sec_refresh_required": False},
    )
    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_to_ttm", lambda **kwargs: calls.append("ttm") or {"rows_written": 1})
    monkeypatch.setattr(run_fundamental_quarter_update, "run_lifecycle_step", lambda **kwargs: calls.append("lifecycle") or 1)
    monkeypatch.setattr(run_fundamental_quarter_update, "run_score_step", lambda **kwargs: calls.append("score") or 1)
    monkeypatch.setattr(run_fundamental_quarter_update, "acknowledge_ticker", lambda **kwargs: calls.append("ack") or 1)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=None,
        run_id="BASE",
        market="omxh",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
    )
    assert calls == ["quarterly_refresh", "ttm", "lifecycle", "score", "ack"]


def test_run_quarterly_refresh_usa_uses_enrichment_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_usa_refresh.db"
    run_migration(db_path)
    calls: list[str] = []
    _insert_state_row(db_path, "AAPL", "usa", "2026-03-28", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "AAPL", "2026-03-28")

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_fallback_enrich",
        lambda **kwargs: calls.append("enrich") or {"fields_filled": 0},
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_audit",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("raw should not run for usa")),
    )

    summary = run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="AAPL",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )
    assert calls == ["enrich"]
    assert summary["mode"] == "enrich"
    assert summary["sec_refresh_required"] is False


def test_run_quarterly_refresh_non_usa_runs_raw_write_bridge(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_nonusa_refresh.db"
    run_migration(db_path)
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_audit",
        lambda **kwargs: calls.append("raw") or {"ok_count": 1, "empty_count": 0, "error_count": 0, "rows_written": 1},
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_quarterly_write",
        lambda **kwargs: calls.append("yqtr") or {"rows_written": 5},
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_to_quarterly",
        lambda **kwargs: calls.append("qbridge") or {"rows_written": 5},
    )

    summary = run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="NOKIA.HE",
        market="omxh",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )
    assert calls == ["raw", "yqtr", "qbridge"]
    assert summary["mode"] == "yahoo_refresh"


def test_quarterly_refresh_failure_stops_processing_immediately(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_refresh_fail.db"
    run_migration(db_path)
    _insert_state_row(db_path, "NOKIA.HE", "omxh", "2025-12-31", "2026-03-31", 1)

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_quarterly_refresh",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_RAW_NOT_USABLE:NOKIA.HE")),
    )

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_RAW_NOT_USABLE:NOKIA.HE"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="BASE",
            market=None,
            ticker="NOKIA.HE",
            limit=None,
            dry_run=False,
            skip_ack=False,
        )
    out = capsys.readouterr().out
    assert "TICKER NOKIA.HE=FAILED" in out
    assert "ERROR ticker=NOKIA.HE step=quarterly_refresh" in out


def test_ack_safety_rule_still_fails_if_quarterly_max_date_below_detected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_ack_safety.db"
    run_migration(db_path)
    _insert_state_row(db_path, "NOKIA.HE", "omxh", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "NOKIA.HE", "2025-12-31")

    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_refresh", lambda **kwargs: {"mode": "yahoo_refresh"})
    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_to_ttm", lambda **kwargs: {"rows_written": 1})
    monkeypatch.setattr(run_fundamental_quarter_update, "run_lifecycle_step", lambda **kwargs: 1)
    monkeypatch.setattr(run_fundamental_quarter_update, "run_score_step", lambda **kwargs: 1)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_ACK_PERIOD_MISMATCH:NOKIA.HE"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="BASE",
            market=None,
            ticker="NOKIA.HE",
            limit=None,
            dry_run=False,
            skip_ack=False,
        )


def test_run_quarterly_refresh_usa_runs_sec_refresh_when_needed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_sec_needed.db"
    run_migration(db_path)
    _insert_state_row(db_path, "LRCX", "usa", "2025-12-28", "2026-03-29", 1)
    _insert_quarterly_row(db_path, "LRCX", "2025-12-28")
    calls: list[str] = []

    def _fake_sec_raw(**kwargs):
        calls.append("sec_raw")
        return "0000707549", [{"ticker": "LRCX"}]

    def _fake_sec_quarterly_build(**kwargs):
        calls.append("quarterly")
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                "INSERT INTO rc_fundamental_quarterly (ticker, period_end_date, run_id) VALUES (?, ?, ?)",
                ("LRCX", "2026-03-29", "SEC_QUARTERLY"),
            )
            conn.commit()
        return 1, 1

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_raw_bootstrap", _fake_sec_raw)
    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_quarterly_build_step", _fake_sec_quarterly_build)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_fallback_enrich",
        lambda **kwargs: calls.append("enrich") or {"fields_filled": 0},
    )

    summary = run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="LRCX",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )
    assert calls == ["sec_raw", "quarterly", "enrich"]
    assert summary["sec_refresh_required"] is True


def test_run_quarterly_refresh_usa_sec_miss_still_runs_enrich_and_succeeds(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_sec_missing.db"
    run_migration(db_path)
    _insert_state_row(db_path, "LRCX", "usa", "2025-12-28", "2026-03-29", 1)
    _insert_quarterly_row(db_path, "LRCX", "2025-12-28")
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_raw_bootstrap",
        lambda **kwargs: calls.append("sec_raw") or ("0000707549", []),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_quarterly_build_step",
        lambda **kwargs: calls.append("quarterly") or (0, 0),
    )
    original_enrich = run_fundamental_quarter_update.run_yahoo_fallback_enrich

    def _wrapped_enrich(**kwargs):
        calls.append("enrich")
        return original_enrich(**kwargs)

    monkeypatch.setattr(run_fundamental_quarter_update, "run_yahoo_fallback_enrich", _wrapped_enrich)
    _insert_yahoo_quarterly_row(
        db_path,
        market="usa",
        symbol="LRCX",
        period_end_date="2026-03-29",
        revenue=100.0,
        net_income=10.0,
        operating_cashflow=20.0,
        cash=30.0,
        total_debt=40.0,
        shares_outstanding=50.0,
    )

    summary = run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="LRCX",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )
    out = capsys.readouterr().out

    assert calls == ["sec_raw", "quarterly", "enrich"]
    assert (
        "WARN ticker=LRCX step=quarterly_refresh_sec "
        "message=FUNDAMENTAL_QUARTER_UPDATE_SEC_REFRESH_MISSING_DETECTED:"
        "LRCX:expected_detected_period=2026-03-29:latest_quarter_after_sec_refresh=2025-12-28"
    ) in out
    assert summary["sec_refresh_required"] is True
    with sqlite3.connect(str(db_path)) as conn:
        inserted_row = conn.execute(
            """
            SELECT revenue, net_income, operating_cashflow, cash, total_debt, shares_outstanding, run_id
            FROM rc_fundamental_quarterly
            WHERE ticker = 'LRCX' AND period_end_date = '2026-03-29'
            """
        ).fetchone()
    assert inserted_row == (100.0, 10.0, 20.0, 30.0, 40.0, 50.0, "BASE__ENRICH")


def test_run_quarterly_refresh_usa_sec_miss_and_enrich_miss_fails_after_enrich(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_enrich_missing.db"
    run_migration(db_path)
    _insert_state_row(db_path, "LRCX", "usa", "2025-12-28", "2026-03-29", 1)
    _insert_quarterly_row(db_path, "LRCX", "2025-12-28")
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_raw_bootstrap",
        lambda **kwargs: calls.append("sec_raw") or ("0000707549", []),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_quarterly_build_step",
        lambda **kwargs: calls.append("quarterly") or (0, 0),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_fallback_enrich",
        lambda **kwargs: calls.append("enrich") or {"fields_filled": 0},
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "FUNDAMENTAL_QUARTER_UPDATE_ENRICH_MISSING_DETECTED:LRCX:"
            "expected_detected_period=2026-03-29:latest_quarter_after_enrich=2025-12-28"
        ),
    ):
        run_fundamental_quarter_update.run_quarterly_refresh(
            db_path=db_path,
            ticker="LRCX",
            market="usa",
            child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
        )
    out = capsys.readouterr().out

    assert calls == ["sec_raw", "quarterly", "enrich"]
    assert (
        "WARN ticker=LRCX step=quarterly_refresh_sec "
        "message=FUNDAMENTAL_QUARTER_UPDATE_SEC_REFRESH_MISSING_DETECTED:"
        "LRCX:expected_detected_period=2026-03-29:latest_quarter_after_sec_refresh=2025-12-28"
    ) in out


def test_run_quarterly_refresh_usa_exact_yahoo_match_bridges_missing_quarter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_exact_match.db"
    run_migration(db_path)
    _insert_state_row(db_path, "ALKT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "ALKT", "2025-12-31")
    _insert_yahoo_quarterly_row(
        db_path,
        market="usa",
        symbol="ALKT",
        period_end_date="2026-03-31",
        revenue=126138000.0,
        net_income=-9963000.0,
        operating_cashflow=-4800000.0,
        cash=40412000.0,
        total_debt=358211000.0,
        shares_outstanding=107019174.0,
    )

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_raw_bootstrap", lambda **kwargs: ("0000707549", []))
    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_quarterly_build_step", lambda **kwargs: (0, 0))

    summary = run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="ALKT",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )

    assert summary["sec_refresh_required"] is True
    with sqlite3.connect(str(db_path)) as conn:
        inserted_row = conn.execute(
            """
            SELECT period_end_date, revenue, net_income, operating_cashflow, cash, total_debt, shares_outstanding, run_id
            FROM rc_fundamental_quarterly
            WHERE ticker = 'ALKT' AND period_end_date = '2026-03-31'
            """
        ).fetchone()
    assert inserted_row == (
        "2026-03-31",
        126138000.0,
        -9963000.0,
        -4800000.0,
        40412000.0,
        358211000.0,
        107019174.0,
        "BASE__ENRICH",
    )


def test_run_quarterly_refresh_usa_same_quarter_tolerance_yahoo_match_bridges_missing_quarter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_tolerance_match.db"
    run_migration(db_path)
    _insert_state_row(db_path, "ALKT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "ALKT", "2025-12-31")
    _insert_yahoo_quarterly_row(
        db_path,
        market="usa",
        symbol="ALKT",
        period_end_date="2026-03-29",
        revenue=111.0,
    )

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_raw_bootstrap", lambda **kwargs: ("0000707549", []))
    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_quarterly_build_step", lambda **kwargs: (0, 0))

    run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="ALKT",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )
    with sqlite3.connect(str(db_path)) as conn:
        inserted_periods = conn.execute(
            """
            SELECT period_end_date
            FROM rc_fundamental_quarterly
            WHERE ticker = 'ALKT'
            ORDER BY period_end_date DESC
            """
        ).fetchall()
    assert inserted_periods[0][0] == "2026-03-29"


def test_run_quarterly_refresh_usa_outside_tolerance_yahoo_match_still_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_outside_tolerance.db"
    run_migration(db_path)
    _insert_state_row(db_path, "ALKT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "ALKT", "2025-12-31")
    _insert_yahoo_quarterly_row(db_path, market="usa", symbol="ALKT", period_end_date="2026-03-21", revenue=111.0)

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_raw_bootstrap", lambda **kwargs: ("0000707549", []))
    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_quarterly_build_step", lambda **kwargs: (0, 0))

    with pytest.raises(
        RuntimeError,
        match=(
            "FUNDAMENTAL_QUARTER_UPDATE_ENRICH_MISSING_DETECTED:ALKT:"
            "expected_detected_period=2026-03-31:latest_quarter_after_enrich=2025-12-31"
        ),
    ):
        run_fundamental_quarter_update.run_quarterly_refresh(
            db_path=db_path,
            ticker="ALKT",
            market="usa",
            child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
        )
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_quarterly WHERE ticker = 'ALKT' AND period_end_date = '2026-03-21'"
        ).fetchone()[0]
    assert count == 0


def test_run_quarterly_refresh_usa_does_not_insert_duplicate_when_exact_row_already_exists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_no_duplicate.db"
    run_migration(db_path)
    _insert_state_row(db_path, "ALKT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "ALKT", "2026-03-31")
    _insert_yahoo_quarterly_row(db_path, market="usa", symbol="ALKT", period_end_date="2026-03-31", revenue=111.0)

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_raw_bootstrap",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("sec raw should be skipped")),
    )

    run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="ALKT",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_quarterly WHERE ticker = 'ALKT' AND period_end_date = '2026-03-31'"
        ).fetchone()[0]
    assert count == 1


def test_run_quarterly_refresh_usa_does_not_insert_when_generic_quarterly_already_satisfies_detected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_already_satisfied.db"
    run_migration(db_path)
    _insert_state_row(db_path, "ALKT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "ALKT", "2026-03-29")
    _insert_yahoo_quarterly_row(db_path, market="usa", symbol="ALKT", period_end_date="2026-03-31", revenue=111.0)

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_raw_bootstrap",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("sec raw should be skipped")),
    )

    run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="ALKT",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )
    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM rc_fundamental_quarterly WHERE ticker = 'ALKT'"
        ).fetchone()[0]
    assert count == 1


def test_run_quarterly_refresh_usa_skips_sec_when_quarterly_already_satisfies_detected_even_if_state_stale(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_state_stale.db"
    run_migration(db_path)
    _insert_state_row(db_path, "LRCX", "usa", "2025-12-28", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "LRCX", "2026-03-29")
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_raw_bootstrap",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("sec raw should be skipped")),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_fallback_enrich",
        lambda **kwargs: calls.append("enrich") or {"fields_filled": 0},
    )

    summary = run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="LRCX",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )
    out = capsys.readouterr().out
    assert calls == ["enrich"]
    assert summary["sec_refresh_required"] is False
    assert "WARN ticker=LRCX step=quarterly_refresh_sec" not in out


def test_quarterly_refresh_enrich_missing_maps_to_quarterly_refresh_step(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_enrich_missing_maps.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "AAPL", "2025-12-31")

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_quarterly_refresh",
        lambda **kwargs: (_ for _ in ()).throw(
            RuntimeError(
                "FUNDAMENTAL_QUARTER_UPDATE_ENRICH_MISSING_DETECTED:"
                "AAPL:expected_detected_period=2026-03-31:latest_quarter_after_enrich=2025-12-31"
            )
        ),
    )

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_ENRICH_MISSING_DETECTED:AAPL"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="BASE",
            market=None,
            ticker="AAPL",
            limit=None,
            dry_run=False,
            skip_ack=False,
        )
    out = capsys.readouterr().out

    assert (
        "ERROR ticker=AAPL step=quarterly_refresh "
        "message=FUNDAMENTAL_QUARTER_UPDATE_ENRICH_MISSING_DETECTED:"
        "AAPL:expected_detected_period=2026-03-31:latest_quarter_after_enrich=2025-12-31"
    ) in out


def test_run_quarterly_refresh_non_usa_fails_if_raw_returns_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_nonusa_raw_empty.db"
    run_migration(db_path)

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_audit",
        lambda **kwargs: {"ok_count": 0, "empty_count": 1, "error_count": 0, "rows_written": 1},
    )

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_RAW_NOT_USABLE:NOKIA.HE"):
        run_fundamental_quarter_update.run_quarterly_refresh(
            db_path=db_path,
            ticker="NOKIA.HE",
            market="omxh",
            child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
        )
