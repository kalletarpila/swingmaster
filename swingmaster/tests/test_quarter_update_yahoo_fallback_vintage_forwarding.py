from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_state_row(db_path: Path, ticker: str = "AAPL") -> None:
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
            ) VALUES (?, 'usa', 'sec_edgar', '2025-12-31', '2026-03-31', 1, ?)
            """,
            (ticker, "2026-05-05T00:00:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (ticker, period_end_date, run_id)
            VALUES (?, '2025-12-31', 'FIXTURE')
            """,
            (ticker,),
        )
        conn.commit()


def _vintage_kwargs(mode: str = "yahoo_fallback_only") -> dict[str, object]:
    return {
        "write_vintage": True,
        "vintage_market": "usa",
        "vintage_available_at_utc": "2026-05-05T12:00:00Z",
        "vintage_ingested_at_utc": "2026-05-05T12:05:00Z",
        "vintage_run_id": "YAHOO_FALLBACK_VINTAGE_RUN",
        "vintage_normalization_run_id": "YAHOO_FALLBACK_NORM_RUN",
        "vintage_mode": mode,
    }


def _mock_downstream(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_quarterly_to_ttm",
        lambda **_kwargs: {"rows_written": 0},
    )
    monkeypatch.setattr(run_fundamental_quarter_update, "run_lifecycle_step", lambda **_kwargs: 0)
    monkeypatch.setattr(run_fundamental_quarter_update, "run_score_step", lambda **_kwargs: 0)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "resolve_latest_close_as_of_date",
        lambda *_args, **_kwargs: "2026-05-05",
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_fundamental_valuation",
        lambda **_kwargs: {"rows_written": 0},
    )


def test_yahoo_fallback_only_requires_write_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "fallback_mode_requires_write.db"
    run_migration(db_path)

    with pytest.raises(
        RuntimeError,
        match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_WRITE_REQUIRED_FOR_MODE:yahoo_fallback_only",
    ):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="BASE",
            market="usa",
            ticker=None,
            limit=None,
            dry_run=True,
            skip_ack=False,
            write_vintage=False,
            vintage_mode="yahoo_fallback_only",
        )


def test_validation_only_still_does_not_pass_vintage_to_child(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "validation_only_no_fallback_forward.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    seen_kwargs: dict[str, object] = {}

    def _fake_process_ticker(**kwargs: object) -> dict[str, int]:
        seen_kwargs.update(kwargs)
        return {"score_rows_written": 1}

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _fake_process_ticker)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "resolve_latest_close_as_of_date",
        lambda *_args, **_kwargs: "2026-05-05",
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_fundamental_valuation",
        lambda **_kwargs: {"rows_written": 0},
    )

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "osakedata.db",
        run_id="BASE",
        market="usa",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=True,
        **_vintage_kwargs(mode="validation_only"),
    )

    assert summary["vintage_execution_enabled"] is False
    assert "yahoo_fallback_vintage_options" not in seen_kwargs


def test_yahoo_fallback_only_forwards_fallback_metadata_and_not_sec(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "fallback_forwarding.db"
    run_migration(db_path)
    _insert_state_row(db_path, ticker="AAPL")
    _mock_downstream(monkeypatch)
    sec_reconstruct_called = False
    yahoo_fallback_kwargs: dict[str, object] = {}

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_raw_bootstrap",
        lambda **_kwargs: ("0000320193", [{"ticker": "AAPL"}]),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_sec_reconstruct_step",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("sec reconstruct should not receive fallback mode")),
    )

    def _fake_sec_quarterly_build(**_kwargs: object) -> tuple[int, int]:
        nonlocal sec_reconstruct_called
        sec_reconstruct_called = False
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO rc_fundamental_quarterly (ticker, period_end_date, run_id)
                VALUES ('AAPL', '2026-03-31', 'SEC_QUARTERLY')
                """
            )
            conn.commit()
        return 1, 1

    def _fake_yahoo_fallback(**kwargs: object) -> dict[str, int]:
        yahoo_fallback_kwargs.update(kwargs)
        return {"fields_filled": 0}

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_quarterly_build_step", _fake_sec_quarterly_build)
    monkeypatch.setattr(run_fundamental_quarter_update, "run_yahoo_fallback_enrich", _fake_yahoo_fallback)

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "osakedata.db",
        run_id="BASE",
        market="usa",
        ticker="AAPL",
        limit=None,
        dry_run=False,
        skip_ack=True,
        **_vintage_kwargs(),
    )

    assert sec_reconstruct_called is False
    assert yahoo_fallback_kwargs["write_vintage"] is True
    assert yahoo_fallback_kwargs["vintage_market"] == "usa"
    assert yahoo_fallback_kwargs["vintage_available_at_utc"] == "2026-05-05T12:00:00Z"
    assert yahoo_fallback_kwargs["vintage_ingested_at_utc"] == "2026-05-05T12:05:00Z"
    assert yahoo_fallback_kwargs["vintage_run_id"] == "YAHOO_FALLBACK_VINTAGE_RUN"
    assert yahoo_fallback_kwargs["vintage_normalization_run_id"] == "YAHOO_FALLBACK_NORM_RUN"
    assert summary["vintage_requested"] is True
    assert summary["vintage_execution_enabled"] is True
    assert summary["vintage_mode"] == "yahoo_fallback_only"
    assert summary["vintage_sec_reconstruct_requested"] is False
    assert summary["vintage_yahoo_bridge_requested"] is False
    assert summary["vintage_yahoo_fallback_requested"] is True
    assert summary["vintage_rows_inserted"] is None
    assert summary["vintage_provenance_rows_inserted"] is None
    assert summary["vintage_count_status"] == "not_reported_by_child"


def test_yahoo_fallback_only_missing_metadata_fails_before_child_steps(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "fallback_forwarding_validation_fail.db"
    kwargs = _vintage_kwargs()
    kwargs["vintage_market"] = None

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "load_eligible_rows",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("load_eligible_rows should not run")),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_fallback_enrich",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("fallback should not run")),
    )

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MARKET_REQUIRED"):
        run_fundamental_quarter_update.run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=None,
            run_id="BASE",
            market="usa",
            ticker=None,
            limit=None,
            dry_run=False,
            skip_ack=True,
            **kwargs,
        )
