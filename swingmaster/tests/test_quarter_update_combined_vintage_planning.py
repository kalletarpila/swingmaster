from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


def _insert_state_row(db_path: Path, ticker: str = "NOKIA.HE", market: str = "omxh") -> None:
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
            ) VALUES (?, ?, ?, '2025-12-31', '2026-03-31', 1, ?)
            """,
            (
                ticker,
                market,
                "sec_edgar" if market == "usa" else "yahoo",
                "2026-05-05T00:00:00+00:00",
            ),
        )
        conn.commit()


def _planning_kwargs() -> dict[str, object]:
    return {
        "write_vintage": True,
        "vintage_market": "usa",
        "vintage_available_at_utc": "2026-05-05T12:00:00Z",
        "vintage_ingested_at_utc": "2026-05-05T12:05:00Z",
        "vintage_run_id": "COMBINED_PLAN_RUN",
        "vintage_normalization_run_id": "COMBINED_PLAN_NORM_RUN",
        "vintage_mode": "sec_plus_yahoo_fallback_planning",
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
    db_path = tmp_path / "combined_planning_default.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    summary = _run_update(db_path)

    assert "vintage_requested" not in summary
    assert "vintage_mode" not in summary


def test_combined_planning_requires_write_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "combined_planning_requires_write.db"
    run_migration(db_path)

    with pytest.raises(
        RuntimeError,
        match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_WRITE_REQUIRED_FOR_MODE:sec_plus_yahoo_fallback_planning",
    ):
        _run_update(db_path, write_vintage=False, vintage_mode="sec_plus_yahoo_fallback_planning")


@pytest.mark.parametrize(
    ("missing_key", "expected_error"),
    [
        ("vintage_market", "FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MARKET_REQUIRED"),
        ("vintage_available_at_utc", "FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_AVAILABLE_AT_UTC_REQUIRED"),
        ("vintage_ingested_at_utc", "FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_INGESTED_AT_UTC_REQUIRED"),
        ("vintage_run_id", "FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_RUN_ID_REQUIRED"),
    ],
)
def test_combined_planning_requires_explicit_metadata(
    tmp_path: Path,
    missing_key: str,
    expected_error: str,
) -> None:
    db_path = tmp_path / f"combined_planning_missing_{missing_key}.db"
    run_migration(db_path)
    kwargs = _planning_kwargs()
    kwargs[missing_key] = None

    with pytest.raises(RuntimeError, match=expected_error):
        _run_update(db_path, **kwargs)


def test_combined_planning_validation_fails_before_child_steps(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "combined_planning_preflight_fail.db"
    kwargs = _planning_kwargs()
    kwargs["vintage_available_at_utc"] = None

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

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_AVAILABLE_AT_UTC_REQUIRED"):
        _run_update(db_path, **kwargs)


def test_combined_planning_summary_marks_both_subpaths_and_no_execution(tmp_path: Path) -> None:
    db_path = tmp_path / "combined_planning_summary.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    summary = _run_update(db_path, **_planning_kwargs())

    assert summary["vintage_requested"] is True
    assert summary["vintage_mode"] == "sec_plus_yahoo_fallback_planning"
    assert summary["vintage_execution_enabled"] is False
    assert summary["vintage_planning_only"] is True
    assert summary["vintage_validation_status"] == "OK"
    assert summary["vintage_sec_reconstruct_requested"] is True
    assert summary["vintage_yahoo_fallback_requested"] is True
    assert summary["vintage_yahoo_bridge_requested"] is False
    assert summary["vintage_final_mixed_planned"] is True
    assert summary["vintage_final_mixed_written"] is False
    assert summary["vintage_rows_inserted"] == 0
    assert summary["vintage_provenance_rows_inserted"] == 0
    assert summary["vintage_rows_skipped_noop"] == 0
    assert summary["vintage_rows_failed"] == 0
    assert summary["vintage_count_status"] == "planning_only_no_execution"


def test_combined_planning_does_not_pass_vintage_metadata_to_children(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "combined_planning_no_child_forwarding.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    seen_kwargs: dict[str, object] = {}

    def _fake_process_ticker(**kwargs: object) -> dict[str, int]:
        seen_kwargs.update(kwargs)
        return {"score_rows_written": 1}

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _fake_process_ticker)

    summary = _run_update(db_path, dry_run=False, **_planning_kwargs())

    assert summary["tickers_processed"] == 1
    assert summary["vintage_planning_only"] is True
    assert "sec_vintage_options" not in seen_kwargs
    assert "yahoo_fallback_vintage_options" not in seen_kwargs
    assert not any(key.startswith("vintage_") for key in seen_kwargs)
    assert "write_vintage" not in seen_kwargs
