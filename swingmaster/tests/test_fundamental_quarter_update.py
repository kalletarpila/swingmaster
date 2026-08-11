from __future__ import annotations

import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.result_check import PLAN_VERSION, candidate_hash
from swingmaster.fundamentals.quarter_refresh_decision import (
    DECISION_NO_ACTION_COMPLETE,
    DECISION_REFRESH_SEC_CONFIRMATION,
    DECISION_RETRY_FETCH_FAILED,
    DECISION_RETRY_PARTIAL_QUARTER,
    DECISION_REVIEW_HISTORICAL_PARTIAL,
    build_quarter_refresh_decisions,
    open_readonly_db,
)


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


def _capture_usa_valuation(monkeypatch: pytest.MonkeyPatch, calls: list[dict]) -> None:
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "resolve_latest_close_as_of_date",
        lambda *_args, **_kwargs: "2026-05-08",
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_fundamental_valuation",
        lambda **kwargs: calls.append(kwargs) or {"rows_written": 7},
    )


def _mock_downstream(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_to_ttm", lambda **_kwargs: {"rows_written": 1})
    monkeypatch.setattr(run_fundamental_quarter_update, "run_lifecycle_step", lambda **_kwargs: 1)
    monkeypatch.setattr(run_fundamental_quarter_update, "run_score_step", lambda **_kwargs: 1)


def _write_plan(
    name: str,
    db_path: Path,
    candidates: list[dict],
    *,
    check_status: str = "SUCCESS",
    created_at_utc: str | None = None,
    decision_date: str = "2026-08-07",
) -> Path:
    plan_path = Path.cwd() / "temp" / name / "plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "plan_version": PLAN_VERSION,
        "created_at_utc": created_at_utc
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "decision_date": decision_date,
        "fundamentals_db": str(db_path.resolve()),
        "ohlcv_db": str((Path.cwd() / "temp" / name / "osakedata.db").resolve()),
        "ohlcv_stale_days": 14,
        "candidate_count": len(candidates),
        "candidate_hash": candidate_hash(candidates),
        "check_status": check_status,
        "stages": [{"stage": "fixture", "status": check_status}],
        "candidates": candidates,
    }
    plan_path.write_text(json.dumps(payload), encoding="utf-8")
    return plan_path


def _candidate(ticker: str = "AAPL", decision: str = "FETCH_NEW_QUARTER") -> dict:
    return {
        "market": "usa",
        "ticker": ticker,
        "decision": decision,
        "priority": "P1_FETCH_NOW",
        "fundamental_fetch_enabled": 1,
        "target_period_end_date": "2026-06-30",
        "planned_action": "PLAN_FETCH_QUARTERLY_FUNDAMENTALS",
        "eligible_for_execution": 1,
    }


def _seed_earnings_context(db_path: Path, ticker: str, *, event_id: int = 7, period: str = "2026-06-30") -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO rc_fundamental_quarterly(ticker, period_end_date, run_id)
            VALUES (?, '2026-03-31', 'FIXTURE')
            """,
            (ticker,),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO rc_earnings_calendar (
                market, ticker, estimated_announcement_at, estimated_announcement_date, estimated_session,
                calendar_status, source, source_observed_at_utc, first_observed_at_utc, last_observed_at_utc,
                date_change_count, created_at_utc, updated_at_utc
            ) VALUES (
                'usa', ?, '2026-08-07T16:00:00-04:00', '2026-08-07', 'UNKNOWN',
                'DUE_TODAY', 'YAHOO_FINANCE', '2026-08-07T00:00:00Z',
                '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z',
                0, '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z'
            )
            """,
            (ticker,),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO rc_earnings_event (
                id, market, ticker, announcement_at, announcement_date, announcement_session,
                is_reported, reported_eps, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES (?, 'usa', ?, '2026-08-07T16:00:00-04:00', '2026-08-07',
                'UNKNOWN', 1, 1.23, 'YAHOO_FINANCE', '2026-08-07T00:00:00Z',
                'America/New_York', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """,
            (event_id, ticker),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO rc_fundamental_quarter_earnings_match (
                market, ticker, period_end_date, earnings_event_id, announcement_at, announcement_date,
                announcement_session, effective_date_status, reporting_delay_days, matching_status,
                matching_confidence, matching_method, candidate_count, availability_policy, matcher_version,
                created_at_utc, updated_at_utc
            ) VALUES ('usa', ?, ?, ?, '2026-08-07T16:00:00-04:00', '2026-08-07',
                'UNKNOWN', 'RESOLVED', 1, 'MATCHED', 'HIGH', 'nearest', 1,
                'event_effective_date', 'test', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """,
            (ticker, period, event_id),
        )
        conn.commit()


def _seed_ohlcv(path: Path, tickers: list[str]) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS osakedata (
                id INTEGER PRIMARY KEY,
                osake TEXT,
                pvm TEXT,
                close REAL,
                volume INTEGER,
                market TEXT NOT NULL DEFAULT 'usa'
            )
            """
        )
        for ticker in tickers:
            conn.execute(
                "INSERT INTO osakedata(osake, pvm, close, volume, market) VALUES (?, '2026-08-07', 1, 100, 'usa')",
                (ticker,),
            )
        conn.commit()


def _next_decision(db_path: Path, ticker: str) -> str:
    ohlcv_path = db_path.with_name(f"{ticker.lower()}_osakedata.db")
    _seed_ohlcv(ohlcv_path, [ticker])
    with open_readonly_db(db_path) as conn:
        with open_readonly_db(ohlcv_path) as ohlcv_conn:
            rows = build_quarter_refresh_decisions(
                conn,
                ohlcv_conn=ohlcv_conn,
                tickers=[ticker],
                decision_date="2026-08-07",
                ohlcv_stale_days=14,
            )
    assert len(rows) == 1
    return str(rows[0].decision)


def _status_row(db_path: Path, ticker: str, period: str = "2026-06-30") -> dict:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT *
            FROM rc_fundamental_quarter_ingestion_status
            WHERE market = 'usa'
              AND ticker = ?
              AND period_end_date = ?
            """,
            (ticker, period),
        ).fetchone()
    assert row is not None
    return dict(row)


def _insert_assessment_quarter(db_path: Path, ticker: str, *, complete: bool) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        if complete:
            conn.execute(
                """
                INSERT OR REPLACE INTO rc_fundamental_quarterly (
                    ticker, period_end_date, revenue, ebit, operating_cashflow, capex,
                    free_cashflow, cash, total_debt, shares_outstanding, run_id
                ) VALUES (?, '2026-06-30', 100, 10, 8, -3, 5, 20, 2, 1000, 'UPDATED')
                """,
                (ticker,),
            )
        else:
            conn.execute(
                """
                INSERT OR REPLACE INTO rc_fundamental_quarterly (
                    ticker, period_end_date, revenue, run_id
                ) VALUES (?, '2026-06-30', 100, 'UPDATED')
                """,
                (ticker,),
            )
        conn.commit()


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


def test_usa_valuation_gate_requires_usa_market_and_material_change() -> None:
    assert run_fundamental_quarter_update.should_run_usa_valuation(
        "usa",
        material_fundamentals_change_count=1,
    )
    assert run_fundamental_quarter_update.should_run_usa_valuation(
        None,
        material_fundamentals_change_count=1,
    )
    assert not run_fundamental_quarter_update.should_run_usa_valuation(
        "usa",
        material_fundamentals_change_count=0,
    )
    assert not run_fundamental_quarter_update.should_run_usa_valuation(
        "omxh",
        material_fundamentals_change_count=1,
    )


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


def test_plan_mode_runs_usa_candidate_without_quarter_state_flag(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_plan.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 0)
    plan_path = _write_plan("pytest_quarter_update_plan", db_path, [_candidate()])
    captured: list[dict] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "load_eligible_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("quarter_state must not be read")),
    )

    def _fake_process_ticker(**kwargs):
        captured.append(kwargs)
        return {"post_update_result": "UPDATED_COMPLETE"}

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
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    assert len(captured) == 1
    assert captured[0]["target_period_end_date"] == "2026-06-30"
    assert captured[0]["execution_source"] == "quarter_refresh_plan"
    assert captured[0]["skip_ack"] is True
    assert summary["execution_mode"] == "quarter_refresh_plan"
    assert summary["plan_candidate_count"] == 1
    assert summary["updated_complete_count"] == 1


def test_plan_mode_retry_partial_quarter_forces_provider_refresh(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_plan_force_refresh.db"
    run_migration(db_path)
    plan_path = _write_plan(
        "pytest_quarter_update_plan_force_refresh",
        db_path,
        [_candidate("AAPL", DECISION_RETRY_PARTIAL_QUARTER)],
    )
    captured: list[dict] = []

    def _fake_quarterly_refresh(**kwargs):
        captured.append(kwargs)
        return {
            "mode": "enrich",
            "sec_refresh_required": True,
            "sec_target_available": True,
            "summary": {"rows_inserted": 0, "rows_updated": 0, "fields_filled": 0},
            "sec_refresh_summary": {"rows_written": 0},
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_refresh", _fake_quarterly_refresh)
    _mock_downstream(monkeypatch)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "reassess_target_quarter",
        lambda **_kwargs: {"post_update_result": "UPDATED_COMPLETE"},
    )
    _mock_usa_valuation(monkeypatch)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "osakedata.db",
        run_id="BASE",
        market="usa",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    assert captured[0]["allow_live_yahoo_fast_ingest"] is True
    assert captured[0]["force_provider_refresh"] is True


def test_plan_mode_does_not_require_quarter_state_row(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_no_state.db"
    run_migration(db_path)
    plan_path = _write_plan("pytest_quarter_update_plan_no_state", db_path, [_candidate("MSFT")])
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **kwargs: calls.append(str(kwargs["row"]["ticker"])) or {"post_update_result": "NO_NEW_DATA"},
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
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    assert calls == ["MSFT"]
    assert summary["no_new_data_count"] == 1


def test_plan_mode_zero_candidates_skips_usa_valuation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_zero_candidates.db"
    run_migration(db_path)
    plan_path = _write_plan("pytest_quarter_update_zero_candidates", db_path, [])
    valuation_calls: list[dict] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("no candidate should be processed")),
    )
    _capture_usa_valuation(monkeypatch, valuation_calls)

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=None,
        run_id="BASE",
        market="usa",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    assert valuation_calls == []
    assert summary["tickers_total"] == 0
    assert summary["material_fundamentals_change_count"] == 0
    assert summary["valuation_status"] == "SKIPPED"
    assert summary["valuation_rows_written"] == 0


def test_plan_mode_all_failed_candidates_skip_usa_valuation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_failed_candidates.db"
    run_migration(db_path)
    plan_path = _write_plan("pytest_quarter_update_failed_candidates", db_path, [_candidate("AAPL"), _candidate("MSFT")])
    valuation_calls: list[dict] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_RAW_NOT_USABLE")),
    )
    _capture_usa_valuation(monkeypatch, valuation_calls)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed=2"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="BASE",
            market="usa",
            ticker=None,
            limit=None,
            dry_run=False,
            skip_ack=False,
            quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
        )

    assert valuation_calls == []


def test_plan_mode_all_partial_without_writes_skips_usa_valuation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_partial_no_writes.db"
    run_migration(db_path)
    plan_path = _write_plan("pytest_quarter_update_partial_no_writes", db_path, [_candidate("AAPL")])
    valuation_calls: list[dict] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **kwargs: {
            "target_period_end_date": "2026-06-30",
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 0,
            "target_ttm_input_complete": 0,
            "target_score_history_complete": 0,
            "post_update_result": "UPDATED_PARTIAL",
            "quarterly_rows_written": 0,
            "ttm_rows_written": 0,
        },
    )
    _capture_usa_valuation(monkeypatch, valuation_calls)

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=None,
        run_id="BASE",
        market="usa",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    assert valuation_calls == []
    assert summary["updated_partial_count"] == 1
    assert summary["material_fundamentals_change_count"] == 0
    assert summary["valuation_status"] == "SKIPPED"


def test_plan_mode_material_quarterly_update_runs_usa_valuation_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_material_quarterly.db"
    run_migration(db_path)
    plan_path = _write_plan("pytest_quarter_update_material_quarterly", db_path, [_candidate("AAPL")])
    valuation_calls: list[dict] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **kwargs: {
            "target_period_end_date": "2026-06-30",
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 0,
            "target_ttm_input_complete": 0,
            "target_score_history_complete": 0,
            "post_update_result": "UPDATED_PARTIAL",
            "quarterly_rows_written": 1,
            "ttm_rows_written": 0,
        },
    )
    _capture_usa_valuation(monkeypatch, valuation_calls)

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "osakedata.db",
        run_id="BASE",
        market="usa",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    assert len(valuation_calls) == 1
    assert summary["material_fundamentals_change_count"] == 1
    assert summary["valuation_status"] == "SUCCESS"
    assert summary["valuation_rows_written"] == 7


def test_plan_mode_mixed_batch_runs_usa_valuation_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_mixed_valuation.db"
    run_migration(db_path)
    plan_path = _write_plan("pytest_quarter_update_mixed_valuation", db_path, [_candidate("AAPL"), _candidate("MSFT")])
    valuation_calls: list[dict] = []

    def _process_ticker(**kwargs):
        ticker = str(kwargs["row"]["ticker"])
        if ticker == "AAPL":
            return {
                "target_period_end_date": "2026-06-30",
                "target_quarter_exists": 1,
                "target_quarter_basic_complete": 1,
                "target_ttm_input_complete": 1,
                "target_score_history_complete": 1,
                "post_update_result": "UPDATED_COMPLETE",
                "quarterly_rows_written": 1,
                "ttm_rows_written": 1,
            }
        return {
            "target_period_end_date": "2026-06-30",
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 0,
            "target_ttm_input_complete": 0,
            "target_score_history_complete": 0,
            "post_update_result": "UPDATED_PARTIAL",
            "quarterly_rows_written": 0,
            "ttm_rows_written": 0,
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _process_ticker)
    _capture_usa_valuation(monkeypatch, valuation_calls)

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "osakedata.db",
        run_id="BASE",
        market="usa",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    assert len(valuation_calls) == 1
    assert summary["updated_complete_count"] == 1
    assert summary["updated_partial_count"] == 1
    assert summary["material_fundamentals_change_count"] == 1


def test_plan_mode_accepts_old_created_at_on_same_decision_date(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_old_created_same_day.db"
    run_migration(db_path)
    old_created_at = datetime(2026, 8, 7, 5, 0, tzinfo=timezone.utc)
    plan_path = _write_plan(
        "pytest_quarter_update_plan_old_created_same_day",
        db_path,
        [_candidate()],
        created_at_utc=old_created_at.isoformat().replace("+00:00", "Z"),
        decision_date="2026-08-07",
    )

    plan, rows = run_fundamental_quarter_update.load_plan_rows(
        plan_path=plan_path,
        db_path=db_path,
        ticker=None,
        limit=None,
        execution_decision_date="2026-08-07",
    )

    assert plan["decision_date"] == "2026-08-07"
    assert [row["ticker"] for row in rows] == ["AAPL"]


def test_plan_mode_rejects_old_decision_date_plan(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_old_decision_date.db"
    run_migration(db_path)
    plan_path = _write_plan(
        "pytest_quarter_update_plan_old_decision_date",
        db_path,
        [_candidate()],
        decision_date="2026-08-06",
    )

    with pytest.raises(RuntimeError, match="STALE_RESULT_CHECK_PLAN"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=tmp_path / "osakedata.db",
            run_id="BASE",
            market="usa",
            ticker=None,
            limit=None,
            dry_run=True,
            skip_ack=False,
            quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
        )


def test_plan_mode_rejects_non_executable_decision(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_bad_decision.db"
    run_migration(db_path)
    row = _candidate(decision="REVIEW_NO_CALENDAR_ESTIMATE")
    row["planned_action"] = "MANUAL_REVIEW_SOURCE_COVERAGE"
    plan_path = _write_plan("pytest_quarter_update_plan_bad_decision", db_path, [row])

    with pytest.raises(RuntimeError, match="RESULT_CHECK_PLAN_NON_EXECUTABLE_DECISION"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=tmp_path / "osakedata.db",
            run_id="BASE",
            market="usa",
            ticker=None,
            limit=None,
            dry_run=True,
            skip_ack=False,
            quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
        )


def test_plan_mode_rejects_duplicate_ticker_target(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_duplicate.db"
    run_migration(db_path)
    plan_path = _write_plan("pytest_quarter_update_plan_duplicate", db_path, [_candidate(), _candidate()])

    with pytest.raises(RuntimeError, match="RESULT_CHECK_PLAN_DUPLICATE_CANDIDATE"):
        run_fundamental_quarter_update.load_plan_rows(
            plan_path=plan_path,
            db_path=db_path,
            ticker=None,
            limit=None,
            execution_decision_date="2026-08-07",
        )


def test_plan_mode_rejects_wrong_db(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_right.db"
    other_db_path = tmp_path / "quarter_update_plan_wrong.db"
    run_migration(db_path)
    run_migration(other_db_path)
    plan_path = _write_plan("pytest_quarter_update_plan_wrong_db", other_db_path, [_candidate()])

    with pytest.raises(RuntimeError, match="RESULT_CHECK_PLAN_DB_MISMATCH"):
        run_fundamental_quarter_update.load_plan_rows(
            plan_path=plan_path,
            db_path=db_path,
            ticker=None,
            limit=None,
            execution_decision_date="2026-08-07",
        )


def test_plan_mode_rejects_inactive_row(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_inactive.db"
    run_migration(db_path)
    row = _candidate()
    row["fundamental_fetch_enabled"] = 0
    plan_path = _write_plan("pytest_quarter_update_plan_inactive", db_path, [row])

    with pytest.raises(RuntimeError, match="RESULT_CHECK_PLAN_INACTIVE_ROW"):
        run_fundamental_quarter_update.load_plan_rows(
            plan_path=plan_path,
            db_path=db_path,
            ticker=None,
            limit=None,
            execution_decision_date="2026-08-07",
        )


def test_plan_mode_rejects_missing_target_period(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_missing_target.db"
    run_migration(db_path)
    row = _candidate()
    row["target_period_end_date"] = None
    plan_path = _write_plan("pytest_quarter_update_plan_missing_target", db_path, [row])

    with pytest.raises(RuntimeError, match="RESULT_CHECK_PLAN_TARGET_PERIOD_REQUIRED"):
        run_fundamental_quarter_update.load_plan_rows(
            plan_path=plan_path,
            db_path=db_path,
            ticker=None,
            limit=None,
            execution_decision_date="2026-08-07",
        )


def test_plan_mode_accepts_retry_decisions(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_retry.db"
    run_migration(db_path)
    plan_path = _write_plan(
        "pytest_quarter_update_plan_retry",
        db_path,
        [_candidate("AAPL", "RETRY_PARTIAL_QUARTER"), _candidate("MSFT", "RETRY_FETCH_FAILED")],
    )

    _plan, rows = run_fundamental_quarter_update.load_plan_rows(
        plan_path=plan_path,
        db_path=db_path,
        ticker=None,
        limit=None,
        execution_decision_date="2026-08-07",
    )

    assert [row["ticker"] for row in rows] == ["AAPL", "MSFT"]
    assert [row["detected_source_period_end_date"] for row in rows] == ["2026-06-30", "2026-06-30"]


def test_plan_mode_persists_fetch_failed_for_next_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_plan_fetch_failed_status.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    plan_path = _write_plan("pytest_quarter_update_plan_fetch_failed_status", db_path, [_candidate("AAPL")])

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("SEC_FETCH_FAILED:https://example.test:Timeout:boom")),
    )

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed=1"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="RUN",
            market=None,
            ticker=None,
            limit=None,
            dry_run=False,
            skip_ack=False,
            quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
        )

    status = _status_row(db_path, "AAPL")
    assert status["ingestion_status"] == "FETCH_FAILED"
    assert status["ingestion_evidence_type"] == "MANAGED_UPDATE_ATTEMPT"
    assert _next_decision(db_path, "AAPL") == DECISION_RETRY_FETCH_FAILED


def test_plan_mode_persists_partial_for_next_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_plan_partial_status.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    plan_path = _write_plan("pytest_quarter_update_plan_partial_status", db_path, [_candidate("AAPL")])

    def fake_process(**_kwargs: object) -> dict[str, object]:
        _insert_assessment_quarter(db_path, "AAPL", complete=False)
        return {
            "target_period_end_date": "2026-06-30",
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 0,
            "post_update_result": "UPDATED_PARTIAL",
            "quarterly_rows_written": 0,
            "ttm_rows_written": 0,
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", fake_process)
    calls: list[dict] = []
    _capture_usa_valuation(monkeypatch, calls)

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "unused_osakedata.db",
        run_id="RUN",
        market=None,
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    status = _status_row(db_path, "AAPL")
    assert summary["tickers_succeeded"] == 1
    assert status["ingestion_status"] == "FUNDAMENTALS_PARTIAL"
    assert status["quarter_basic_complete"] == 0
    assert _next_decision(db_path, "AAPL") == DECISION_RETRY_PARTIAL_QUARTER
    assert calls == []


def test_plan_mode_partial_then_complete_updates_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_plan_partial_then_complete.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    _insert_assessment_quarter(db_path, "AAPL", complete=False)
    run_fundamental_quarter_update.persist_managed_update_ingestion_status(
        db_path=db_path,
        ticker="AAPL",
        market="usa",
        target_period_end_date="2026-06-30",
        run_id="OLD",
        post_update_result="UPDATED_PARTIAL",
    )
    plan_path = _write_plan("pytest_quarter_update_plan_partial_then_complete", db_path, [_candidate("AAPL", "RETRY_PARTIAL_QUARTER")])

    def fake_process(**_kwargs: object) -> dict[str, object]:
        _insert_assessment_quarter(db_path, "AAPL", complete=True)
        return {
            "target_period_end_date": "2026-06-30",
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 1,
            "post_update_result": "UPDATED_COMPLETE",
            "quarterly_rows_written": 1,
            "ttm_rows_written": 0,
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", fake_process)
    _mock_usa_valuation(monkeypatch)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "unused_osakedata.db",
        run_id="RUN",
        market=None,
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    assert _status_row(db_path, "AAPL")["ingestion_status"] == "QUARTER_BASIC_COMPLETE"
    assert _next_decision(db_path, "AAPL") == DECISION_NO_ACTION_COMPLETE


def test_plan_mode_fetch_failed_then_complete_updates_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_plan_fetch_failed_then_complete.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    run_fundamental_quarter_update.persist_managed_update_ingestion_status(
        db_path=db_path,
        ticker="AAPL",
        market="usa",
        target_period_end_date="2026-06-30",
        run_id="OLD",
        post_update_result="FETCH_FAILED",
        failure_step="quarterly_refresh",
        error_message="SEC_FETCH_FAILED:timeout",
    )
    plan_path = _write_plan("pytest_quarter_update_plan_fetch_failed_then_complete", db_path, [_candidate("AAPL", "RETRY_FETCH_FAILED")])

    def fake_process(**_kwargs: object) -> dict[str, object]:
        _insert_assessment_quarter(db_path, "AAPL", complete=True)
        return {
            "target_period_end_date": "2026-06-30",
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 1,
            "post_update_result": "UPDATED_COMPLETE",
            "quarterly_rows_written": 1,
            "ttm_rows_written": 0,
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", fake_process)
    _mock_usa_valuation(monkeypatch)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "unused_osakedata.db",
        run_id="RUN",
        market=None,
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    assert _status_row(db_path, "AAPL")["ingestion_status"] == "QUARTER_BASIC_COMPLETE"
    assert _next_decision(db_path, "AAPL") == DECISION_NO_ACTION_COMPLETE


def test_plan_mode_does_not_downgrade_complete_on_transient_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_plan_complete_preserved.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    _insert_assessment_quarter(db_path, "AAPL", complete=True)
    run_fundamental_quarter_update.persist_managed_update_ingestion_status(
        db_path=db_path,
        ticker="AAPL",
        market="usa",
        target_period_end_date="2026-06-30",
        run_id="OLD",
        post_update_result="UPDATED_COMPLETE",
    )
    plan_path = _write_plan("pytest_quarter_update_plan_complete_preserved", db_path, [_candidate("AAPL", "RETRY_FETCH_FAILED")])
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("SEC_FETCH_FAILED:https://example.test:Timeout:boom")),
    )

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed=1"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="RUN",
            market=None,
            ticker=None,
            limit=None,
            dry_run=False,
            skip_ack=False,
            quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
        )

    assert _status_row(db_path, "AAPL")["ingestion_status"] == "QUARTER_BASIC_COMPLETE"
    assert _next_decision(db_path, "AAPL") == DECISION_REFRESH_SEC_CONFIRMATION


def test_plan_mode_historical_unknown_unaffected_without_managed_attempt(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_historical_unknown.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    _insert_assessment_quarter(db_path, "AAPL", complete=False)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_ingestion_status (
                market, ticker, period_end_date, ingestion_status, basic_status,
                quarter_basic_complete, ttm_input_complete, score_history_complete,
                valuation_input_ready, historical_research_ready, available_basic_field_count,
                missing_basic_fields, missing_core_fields_json, missing_ttm_fields_json,
                missing_score_fields_json, data_quality_warnings_json, retry_recommendation,
                last_checked_at_utc, assessment_policy_version, ingestion_evidence_type,
                run_id, assessed_at_utc, created_at_utc, updated_at_utc
            ) VALUES (
                'usa', 'AAPL', '2026-06-30', 'UNKNOWN_HISTORICAL_INGEST_COMPLETENESS',
                'BASIC_PARTIAL', 0, 0, 0, 0, 1, 1, '["ebit"]', '["ebit"]',
                '[]', '[]', '[]', 'MANUAL_REVIEW', '2026-08-07T00:00:00Z',
                'test', 'CURRENT_DB_STATE_ONLY', 'HIST', '2026-08-07T00:00:00Z',
                '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z'
            )
            """
        )
        conn.commit()

    assert _next_decision(db_path, "AAPL") == DECISION_REVIEW_HISTORICAL_PARTIAL


def test_plan_mode_managed_status_upsert_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_plan_idempotent_status.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")

    for run_id in ("RUN1", "RUN2"):
        run_fundamental_quarter_update.persist_managed_update_ingestion_status(
            db_path=db_path,
            ticker="AAPL",
            market="usa",
            target_period_end_date="2026-06-30",
            run_id=run_id,
            post_update_result="FETCH_FAILED",
            failure_step="quarterly_refresh",
            error_message="SEC_FETCH_FAILED:timeout",
        )

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarter_ingestion_status").fetchone()[0]
    status = _status_row(db_path, "AAPL")
    assert count == 1
    assert status["ingestion_status"] == "FETCH_FAILED"
    assert status["run_id"] == "RUN2"


def test_plan_mode_mixed_outcomes_persist_matching_statuses(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_plan_mixed_status.db"
    run_migration(db_path)
    tickers = ["COMP", "PART", "FAIL", "NODATA"]
    for index, ticker in enumerate(tickers, start=10):
        _seed_earnings_context(db_path, ticker, event_id=index)
    plan_path = _write_plan("pytest_quarter_update_plan_mixed_status", db_path, [_candidate(ticker) for ticker in tickers])

    def fake_process(**kwargs: object) -> dict[str, object]:
        row = kwargs["row"]
        ticker = str(row["ticker"])
        if ticker == "FAIL":
            raise RuntimeError("SEC_FETCH_FAILED:https://example.test:Timeout:boom")
        if ticker == "NODATA":
            return {
                "target_period_end_date": "2026-06-30",
                "target_quarter_exists": 0,
                "target_quarter_basic_complete": 0,
                "post_update_result": "NO_NEW_DATA",
                "quarterly_rows_written": 0,
                "ttm_rows_written": 0,
            }
        _insert_assessment_quarter(db_path, ticker, complete=ticker == "COMP")
        return {
            "target_period_end_date": "2026-06-30",
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 1 if ticker == "COMP" else 0,
            "post_update_result": "UPDATED_COMPLETE" if ticker == "COMP" else "UPDATED_PARTIAL",
            "quarterly_rows_written": 1 if ticker == "COMP" else 0,
            "ttm_rows_written": 0,
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", fake_process)
    _mock_usa_valuation(monkeypatch)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed=1"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=tmp_path / "unused_osakedata.db",
            run_id="RUN",
            market=None,
            ticker=None,
            limit=None,
            dry_run=False,
            skip_ack=False,
            quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
        )

    statuses = {ticker: _status_row(db_path, ticker)["ingestion_status"] for ticker in tickers}
    assert statuses == {
        "COMP": "QUARTER_BASIC_COMPLETE",
        "PART": "FUNDAMENTALS_PARTIAL",
        "FAIL": "FETCH_FAILED",
        "NODATA": "PUBLISHED_DATA_NOT_FETCHED",
    }


def test_plan_mode_sec_first_success_skips_live_yahoo_and_confirms_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_sec_first.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    plan_path = _write_plan("pytest_quarter_update_sec_first", db_path, [_candidate("AAPL")])
    calls: list[str] = []

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_raw_bootstrap", lambda **_kwargs: ("0000320193", 10))

    def fake_sec_build(**_kwargs: object) -> tuple[int, int]:
        calls.append("sec_build")
        _insert_assessment_quarter(db_path, "AAPL", complete=True)
        return 1, 1

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_quarterly_build_step", fake_sec_build)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_audit",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("live Yahoo should not run after SEC target success")),
    )
    _mock_downstream(monkeypatch)
    _mock_usa_valuation(monkeypatch)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "unused_osakedata.db",
        run_id="RUN",
        market=None,
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    status = _status_row(db_path, "AAPL")
    assert calls == ["sec_build"]
    assert status["ingestion_status"] == "QUARTER_BASIC_COMPLETE"
    assert status["source_confirmation_status"] == "SEC_CONFIRMED"
    assert _next_decision(db_path, "AAPL") == DECISION_NO_ACTION_COMPLETE


def test_plan_mode_sec_missing_yahoo_complete_fast_ingest_creates_sec_followup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_yahoo_fast_ingest.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    plan_path = _write_plan("pytest_quarter_update_yahoo_fast_ingest", db_path, [_candidate("AAPL")])

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_raw_bootstrap", lambda **_kwargs: ("0000320193", 10))
    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_quarterly_build_step", lambda **_kwargs: (0, 0))
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_audit",
        lambda **_kwargs: {"ok_count": 1, "empty_count": 0, "error_count": 0, "rows_written": 1},
    )

    def fake_yahoo_write(**_kwargs: object) -> dict[str, object]:
        _insert_yahoo_quarterly_row(
            db_path,
            market="usa",
            symbol="AAPL",
            period_end_date="2026-06-30",
            revenue=100,
            operating_income=10,
            operating_cashflow=8,
            capex=-3,
            free_cashflow=5,
            cash=20,
            total_debt=2,
            shares_outstanding=1000,
        )
        return {"rows_written": 1, "rows_normalized": 1, "rows_skipped": 0, "source_run_id": "YRAW"}

    monkeypatch.setattr(run_fundamental_quarter_update, "run_yahoo_quarterly_write", fake_yahoo_write)
    _mock_downstream(monkeypatch)
    _mock_usa_valuation(monkeypatch)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "unused_osakedata.db",
        run_id="RUN",
        market=None,
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    with sqlite3.connect(str(db_path)) as conn:
        quarter = conn.execute(
            "SELECT revenue, ebit, run_id FROM rc_fundamental_quarterly WHERE ticker='AAPL' AND period_end_date='2026-06-30'"
        ).fetchone()
    status = _status_row(db_path, "AAPL")
    assert quarter == (100.0, 10.0, "RUN__ENRICH")
    assert status["ingestion_status"] == "QUARTER_BASIC_COMPLETE"
    assert status["source_confirmation_status"] == "YAHOO_BACKED_SEC_PENDING"
    assert _next_decision(db_path, "AAPL") == DECISION_REFRESH_SEC_CONFIRMATION


def test_plan_mode_yahoo_complete_then_later_sec_confirmation_overwrites_sec_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_yahoo_then_sec.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    _insert_assessment_quarter(db_path, "AAPL", complete=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "UPDATE rc_fundamental_quarterly SET revenue = 100, ebit = 10, run_id = 'YAHOO_FAST' WHERE ticker='AAPL' AND period_end_date='2026-06-30'"
        )
        conn.commit()
    run_fundamental_quarter_update.persist_managed_update_ingestion_status(
        db_path=db_path,
        ticker="AAPL",
        market="usa",
        target_period_end_date="2026-06-30",
        run_id="OLD",
        post_update_result="UPDATED_COMPLETE",
        source_confirmation={"source_confirmation_status": "YAHOO_BACKED_SEC_PENDING", "source_confirmation_source": "yahoo"},
    )
    plan_path = _write_plan("pytest_quarter_update_yahoo_then_sec", db_path, [_candidate("AAPL", "REFRESH_SEC_CONFIRMATION")])

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_raw_bootstrap", lambda **_kwargs: ("0000320193", 10))

    def fake_sec_build(**_kwargs: object) -> tuple[int, int]:
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO rc_fundamental_quarterly (
                    ticker, period_end_date, revenue, ebit, operating_cashflow, capex,
                    free_cashflow, cash, total_debt, shares_outstanding, run_id
                ) VALUES ('AAPL', '2026-06-30', 111, 11, 8, -3, 5, 20, 2, 1000, 'RUN__QUARTERLY')
                """
            )
            conn.commit()
        return 1, 1

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_quarterly_build_step", fake_sec_build)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_audit",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("live Yahoo should not run after SEC target success")),
    )
    _mock_downstream(monkeypatch)
    _mock_usa_valuation(monkeypatch)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "unused_osakedata.db",
        run_id="RUN",
        market=None,
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    with sqlite3.connect(str(db_path)) as conn:
        quarter = conn.execute(
            "SELECT revenue, ebit, run_id FROM rc_fundamental_quarterly WHERE ticker='AAPL' AND period_end_date='2026-06-30'"
        ).fetchone()
    status = _status_row(db_path, "AAPL")
    assert quarter == (111.0, 11.0, "RUN__QUARTERLY")
    assert status["source_confirmation_status"] == "SEC_CONFIRMED"
    assert _next_decision(db_path, "AAPL") == DECISION_NO_ACTION_COMPLETE


def test_plan_mode_sec_error_after_yahoo_complete_keeps_completeness_and_retries_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_sec_error_after_yahoo.db"
    run_migration(db_path)
    _seed_earnings_context(db_path, "AAPL")
    _insert_assessment_quarter(db_path, "AAPL", complete=True)
    run_fundamental_quarter_update.persist_managed_update_ingestion_status(
        db_path=db_path,
        ticker="AAPL",
        market="usa",
        target_period_end_date="2026-06-30",
        run_id="OLD",
        post_update_result="UPDATED_COMPLETE",
        source_confirmation={"source_confirmation_status": "YAHOO_BACKED_SEC_PENDING", "source_confirmation_source": "yahoo"},
    )
    plan_path = _write_plan("pytest_quarter_update_sec_error_after_yahoo", db_path, [_candidate("AAPL", "REFRESH_SEC_CONFIRMATION")])
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_raw_bootstrap",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("SEC_FETCH_FAILED:timeout")),
    )
    monkeypatch.setattr(run_fundamental_quarter_update, "run_yahoo_audit", lambda **_kwargs: {"ok_count": 1})
    monkeypatch.setattr(run_fundamental_quarter_update, "run_yahoo_quarterly_write", lambda **_kwargs: {"rows_written": 0})
    _mock_downstream(monkeypatch)
    calls: list[dict] = []
    _capture_usa_valuation(monkeypatch, calls)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "unused_osakedata.db",
        run_id="RUN",
        market=None,
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
        execution_decision_date="2026-08-07",
    )

    status = _status_row(db_path, "AAPL")
    assert status["ingestion_status"] == "QUARTER_BASIC_COMPLETE"
    assert status["source_confirmation_status"] == "SEC_CONFIRMATION_FAILED_RETRYABLE"
    assert _next_decision(db_path, "AAPL") == DECISION_REFRESH_SEC_CONFIRMATION
    assert calls


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


def test_run_quarterly_refresh_forced_plan_refresh_runs_yahoo_before_sec(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_usa_forced_plan_refresh.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "AAPL", "2026-03-31")
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_audit",
        lambda **kwargs: calls.append("yahoo_audit")
        or {"ok_count": 1, "empty_count": 0, "error_count": 0, "rows_written": 1},
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_quarterly_write",
        lambda **kwargs: calls.append("yahoo_write")
        or {"rows_written": 1, "rows_normalized": 1, "rows_skipped": 0, "source_run_id": "YRAW"},
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_raw_bootstrap",
        lambda **kwargs: calls.append("sec_raw") or ("0000320193", [{"ticker": "AAPL"}]),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_quarterly_build_step",
        lambda **kwargs: calls.append("quarterly") or (1, 0),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_fallback_enrich",
        lambda **kwargs: calls.append("enrich") or {"fields_filled": 0, "rows_inserted": 0, "rows_updated": 0},
    )

    summary = run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="AAPL",
        market="usa",
        target_period_end_date="2026-03-31",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
        allow_live_yahoo_fast_ingest=True,
        force_provider_refresh=True,
    )

    assert calls == ["yahoo_audit", "yahoo_write", "sec_raw", "quarterly", "enrich"]
    assert summary["sec_refresh_required"] is True
    assert summary["yahoo_live_refresh_attempted"] is True


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
