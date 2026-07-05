from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_state_row(db_path: Path, ticker: str = "AAPL", market: str = "usa") -> None:
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
                "sec_edgar" if market == "usa" else "yahoo",
                "2025-12-31",
                "2026-03-31",
                1,
                "2026-05-05T00:00:00+00:00",
            ),
        )
        conn.commit()


def _vintage_kwargs() -> dict[str, object]:
    return {
        "write_vintage": True,
        "vintage_market": "usa",
        "vintage_available_at_utc": "2026-05-05T12:00:00Z",
        "vintage_ingested_at_utc": "2026-05-05T12:05:00Z",
        "vintage_run_id": "VINTAGE_RUN",
        "vintage_normalization_run_id": None,
        "vintage_mode": "validation_only",
    }


def _run_update(db_path: Path, **overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "db_path": db_path,
        "osakedata_db_path": None,
        "run_id": "BASE",
        "market": "omxh",
        "ticker": None,
        "limit": None,
        "dry_run": True,
        "skip_ack": False,
    }
    kwargs.update(overrides)
    return run_fundamental_quarter_update.run_fundamental_quarter_update(**kwargs)


def test_default_dry_run_summary_omits_vintage_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_default.db"
    run_migration(db_path)
    _insert_state_row(db_path, ticker="NOKIA.HE", market="omxh")

    summary = _run_update(db_path)

    assert "vintage_requested" not in summary
    assert "vintage_execution_enabled" not in summary


def test_write_vintage_requires_vintage_market(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_missing_market.db"
    run_migration(db_path)
    kwargs = _vintage_kwargs()
    kwargs["vintage_market"] = None

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MARKET_REQUIRED"):
        _run_update(db_path, **kwargs)


def test_write_vintage_requires_available_at_utc(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_missing_available.db"
    run_migration(db_path)
    kwargs = _vintage_kwargs()
    kwargs["vintage_available_at_utc"] = None

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_AVAILABLE_AT_UTC_REQUIRED"):
        _run_update(db_path, **kwargs)


def test_write_vintage_requires_ingested_at_utc(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_missing_ingested.db"
    run_migration(db_path)
    kwargs = _vintage_kwargs()
    kwargs["vintage_ingested_at_utc"] = None

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_INGESTED_AT_UTC_REQUIRED"):
        _run_update(db_path, **kwargs)


def test_write_vintage_requires_vintage_run_id(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_missing_run_id.db"
    run_migration(db_path)
    kwargs = _vintage_kwargs()
    kwargs["vintage_run_id"] = None

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_RUN_ID_REQUIRED"):
        _run_update(db_path, **kwargs)


def test_write_vintage_requires_validation_only_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_missing_mode.db"
    run_migration(db_path)
    kwargs = _vintage_kwargs()
    kwargs["vintage_mode"] = None

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MODE_REQUIRED"):
        _run_update(db_path, **kwargs)


def test_invalid_vintage_mode_fails(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_invalid_mode.db"
    run_migration(db_path)
    kwargs = _vintage_kwargs()
    kwargs["vintage_mode"] = "sec_only"

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MODE_UNSUPPORTED:sec_only"):
        _run_update(db_path, **kwargs)


def test_parse_args_rejects_invalid_vintage_mode() -> None:
    with pytest.raises(SystemExit):
        run_fundamental_quarter_update.parse_args(
            [
                "--db",
                "fundamentals.db",
                "--run-id",
                "BASE",
                "--write-vintage",
                "--vintage-mode",
                "sec_only",
            ]
        )


def test_validation_failure_happens_before_child_or_db_steps(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_preflight_fail.db"

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "load_eligible_rows",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("load_eligible_rows should not run")),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("process_ticker should not run")),
    )
    kwargs = _vintage_kwargs()
    kwargs["vintage_available_at_utc"] = None

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_AVAILABLE_AT_UTC_REQUIRED"):
        _run_update(db_path, **kwargs)


def test_validation_only_success_sets_zero_vintage_summary(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_vintage_summary.db"
    run_migration(db_path)
    _insert_state_row(db_path, ticker="NOKIA.HE", market="omxh")

    summary = _run_update(db_path, **_vintage_kwargs())

    assert summary["vintage_requested"] is True
    assert summary["vintage_mode"] == "validation_only"
    assert summary["vintage_execution_enabled"] is False
    assert summary["vintage_validation_status"] == "OK"
    assert summary["vintage_rows_inserted"] == 0
    assert summary["vintage_provenance_rows_inserted"] == 0
    assert summary["vintage_rows_skipped_noop"] == 0
    assert summary["vintage_rows_failed"] == 0
    assert summary["vintage_error_summary"] is None


def test_validation_only_does_not_pass_vintage_kwargs_to_ticker_processing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "quarter_update_no_subpath_flags.db"
    run_migration(db_path)
    _insert_state_row(db_path, ticker="NOKIA.HE", market="omxh")
    seen_kwargs: dict[str, object] = {}

    def _fake_process_ticker(**kwargs: object) -> dict[str, int]:
        seen_kwargs.update(kwargs)
        return {"score_rows_written": 1}

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _fake_process_ticker)

    summary = _run_update(db_path, dry_run=False, **_vintage_kwargs())

    assert summary["tickers_processed"] == 1
    assert not any(key.startswith("vintage_") for key in seen_kwargs)
    assert "write_vintage" not in seen_kwargs
