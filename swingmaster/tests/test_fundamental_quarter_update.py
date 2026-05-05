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


def test_successful_run_executes_steps_in_order_and_acknowledges(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_success.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "AAPL", "2026-03-31")
    calls: list[str] = []

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_fallback_enrich",
        lambda **kwargs: calls.append("enrich") or {"fields_filled": 0},
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

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        run_id="BASE",
        market=None,
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
    )

    assert calls == ["enrich", "ttm", "lifecycle", "score", "ack"]
    assert summary["tickers_processed"] == 1


def test_skip_ack_leaves_state_unchanged(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_skip_ack.db"
    run_migration(db_path)
    _insert_state_row(db_path, "NOKIA.HE", "omxh", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "NOKIA.HE", "2026-03-31")

    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_to_ttm", lambda **kwargs: {"rows_written": 1})
    monkeypatch.setattr(run_fundamental_quarter_update, "run_lifecycle_step", lambda **kwargs: 1)
    monkeypatch.setattr(run_fundamental_quarter_update, "run_score_step", lambda **kwargs: 1)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        run_id="BASE",
        market=None,
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


def test_failure_stops_processing_and_leaves_state_unchanged(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_fail.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_state_row(db_path, "MSFT", "usa", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "AAPL", "2026-03-31")
    _insert_quarterly_row(db_path, "MSFT", "2026-03-31")
    calls: list[str] = []

    monkeypatch.setattr(run_fundamental_quarter_update, "run_yahoo_fallback_enrich", lambda **kwargs: {"fields_filled": 0})

    def _fake_ttm(**kwargs):
        calls.append(kwargs["ticker"])
        if kwargs["ticker"] == "AAPL":
            raise RuntimeError("FUNDAMENTAL_TTM_BROKE")
        return {"rows_written": 1}

    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_to_ttm", _fake_ttm)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_TTM_BROKE"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            run_id="BASE",
            market=None,
            ticker=None,
            limit=None,
            dry_run=False,
            skip_ack=False,
        )
    out = capsys.readouterr().out

    assert calls == ["AAPL"]
    assert "TICKER AAPL=FAILED" in out
    assert "ERROR ticker=AAPL step=ttm" in out
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT new_quarter_available FROM rc_fundamental_quarter_state WHERE ticker='AAPL'"
        ).fetchone()
    assert row == (1,)


def test_child_run_id_derivation_is_correct() -> None:
    assert run_fundamental_quarter_update.derive_child_run_ids("USA_QUARTER_UPDATE_20260505") == {
        "ttm": "USA_QUARTER_UPDATE_20260505__TTM",
        "lifecycle": "USA_QUARTER_UPDATE_20260505__LIFECYCLE",
        "score": "USA_QUARTER_UPDATE_20260505__SCORE",
        "ack": "USA_QUARTER_UPDATE_20260505__ACK",
        "enrich": "USA_QUARTER_UPDATE_20260505__ENRICH",
    }


def test_invalid_state_missing_detected_date_fails(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_invalid_state.db"
    run_migration(db_path)
    _insert_state_row(db_path, "AAPL", "usa", "2025-12-31", None, 1)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_DETECTED_DATE_MISSING:AAPL"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            run_id="BASE",
            market=None,
            ticker=None,
            limit=None,
            dry_run=False,
            skip_ack=False,
        )


def test_non_usa_processing_does_not_run_enrichment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_nonusa.db"
    run_migration(db_path)
    _insert_state_row(db_path, "NOKIA.HE", "omxh", "2025-12-31", "2026-03-31", 1)
    _insert_quarterly_row(db_path, "NOKIA.HE", "2026-03-31")

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_fallback_enrich",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not enrich non-usa")),
    )
    monkeypatch.setattr(run_fundamental_quarter_update, "run_quarterly_to_ttm", lambda **kwargs: {"rows_written": 1})
    monkeypatch.setattr(run_fundamental_quarter_update, "run_lifecycle_step", lambda **kwargs: 1)
    monkeypatch.setattr(run_fundamental_quarter_update, "run_score_step", lambda **kwargs: 1)
    monkeypatch.setattr(run_fundamental_quarter_update, "acknowledge_ticker", lambda **kwargs: 1)

    run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        run_id="BASE",
        market=None,
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
    )
