from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


def test_default_quarter_update_behavior_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_wiring_default.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    summary = _run_update(db_path)

    assert "vintage_requested" not in summary
    assert "vintage_final_mixed_rows_inserted" not in summary


def test_final_mixed_mode_requires_write_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_wiring_requires_write.db"
    run_migration(db_path)

    with pytest.raises(
        RuntimeError,
        match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_WRITE_REQUIRED_FOR_MODE:sec_plus_yahoo_fallback_final_mixed",
    ):
        _run_update(db_path, write_vintage=False, vintage_mode="sec_plus_yahoo_fallback_final_mixed")


@pytest.mark.parametrize(
    ("missing_key", "expected_error"),
    [
        ("vintage_market", "FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MARKET_REQUIRED"),
        ("vintage_available_at_utc", "FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_AVAILABLE_AT_UTC_REQUIRED"),
        ("vintage_ingested_at_utc", "FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_INGESTED_AT_UTC_REQUIRED"),
        ("vintage_run_id", "FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_RUN_ID_REQUIRED"),
    ],
)
def test_final_mixed_mode_requires_pit_metadata(
    tmp_path: Path,
    missing_key: str,
    expected_error: str,
) -> None:
    db_path = tmp_path / f"final_mixed_wiring_missing_{missing_key}.db"
    kwargs = _final_mixed_kwargs()
    kwargs[missing_key] = None

    with pytest.raises(RuntimeError, match=expected_error):
        _run_update(db_path, **kwargs)


def test_validation_failure_happens_before_child_steps(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_wiring_validation_preflight.db"
    kwargs = _final_mixed_kwargs()
    kwargs["vintage_available_at_utc"] = None

    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "load_eligible_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("load_eligible_rows should not run")),
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("process_ticker should not run")),
    )

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_AVAILABLE_AT_UTC_REQUIRED"):
        _run_update(db_path, **kwargs)


def test_final_mixed_mode_marks_sec_yahoo_and_execution_requested(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_wiring_dry_summary.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    summary = _run_update(db_path, **_final_mixed_kwargs())

    assert summary["vintage_mode"] == "sec_plus_yahoo_fallback_final_mixed"
    assert summary["vintage_execution_enabled"] is True
    assert summary["vintage_planning_only"] is False
    assert summary["vintage_sec_reconstruct_requested"] is True
    assert summary["vintage_yahoo_fallback_requested"] is True
    assert summary["vintage_final_mixed_planned"] is True


def test_mocked_final_mixed_helper_success_updates_summary_counts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "final_mixed_wiring_success.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _patch_process_ticker(monkeypatch)

    summary = _run_update(
        db_path,
        market="omxh",
        dry_run=False,
        final_mixed_execution_runner=_success_runner,
        **_final_mixed_kwargs(),
    )

    assert summary["vintage_final_mixed_written"] is True
    assert summary["vintage_final_mixed_rows_inserted"] == 1
    assert summary["vintage_final_mixed_provenance_rows_inserted"] == 5
    assert summary["vintage_rows_skipped_noop"] == 0
    assert summary["vintage_rows_failed"] == 0
    assert summary["vintage_error_summary"] is None


def test_mocked_final_mixed_helper_noop_updates_summary_counts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "final_mixed_wiring_noop.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _patch_process_ticker(monkeypatch)

    summary = _run_update(
        db_path,
        market="omxh",
        dry_run=False,
        final_mixed_execution_runner=_noop_runner,
        **_final_mixed_kwargs(),
    )

    assert summary["vintage_final_mixed_written"] is False
    assert summary["vintage_final_mixed_rows_inserted"] == 0
    assert summary["vintage_final_mixed_provenance_rows_inserted"] == 0
    assert summary["vintage_rows_skipped_noop"] == 1
    assert summary["vintage_rows_failed"] == 0
    assert summary["vintage_count_status"] == "final_mixed_execution"


def test_mocked_final_mixed_helper_failure_updates_error_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "final_mixed_wiring_failure.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _patch_process_ticker(monkeypatch)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed=1"):
        _run_update(
            db_path,
            market="omxh",
            dry_run=False,
            final_mixed_execution_runner=_failing_runner,
            **_final_mixed_kwargs(),
        )


def test_final_mixed_mode_requires_runner_before_child_steps(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_wiring_runner_required.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("process_ticker should not run")),
    )

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_RUNNER_REQUIRED"):
        _run_update(db_path, market="omxh", dry_run=False, **_final_mixed_kwargs())


def test_no_provider_functions_are_called(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_wiring_no_provider.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _patch_process_ticker(monkeypatch)
    for function_name in (
        "run_sec_raw_bootstrap",
        "run_sec_reconstruct_quarterly",
        "run_yahoo_audit",
        "run_yahoo_fallback_enrich",
        "run_yahoo_quarterly_write",
        "run_yahoo_to_quarterly",
    ):
        monkeypatch.setattr(
            run_fundamental_quarter_update,
            function_name,
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("provider function should not run")),
        )

    summary = _run_update(
        db_path,
        market="omxh",
        dry_run=False,
        final_mixed_execution_runner=_success_runner,
        **_final_mixed_kwargs(),
    )

    assert summary["vintage_final_mixed_written"] is True


def test_runner_receives_temp_db_path_row_and_vintage_options(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "final_mixed_wiring_runner_args.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _patch_process_ticker(monkeypatch)
    seen: dict[str, object] = {}

    def _recording_runner(**kwargs: object) -> dict[str, object]:
        seen.update(kwargs)
        return _success_runner(**kwargs)

    _run_update(
        db_path,
        market="omxh",
        dry_run=False,
        final_mixed_execution_runner=_recording_runner,
        **_final_mixed_kwargs(),
    )

    assert seen["db_path"] == db_path
    assert seen["row"]["ticker"] == "NOKIA.HE"  # type: ignore[index]
    assert seen["vintage_options"] == {
        "market": "usa",
        "available_at_utc": "2026-05-05T12:00:00Z",
        "ingested_at_utc": "2026-05-05T12:05:00Z",
        "run_id": "FINAL_MIXED_RUN",
        "normalization_run_id": "FINAL_MIXED_NORM_RUN",
    }


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


def _final_mixed_kwargs() -> dict[str, object]:
    return {
        "write_vintage": True,
        "vintage_market": "usa",
        "vintage_available_at_utc": "2026-05-05T12:00:00Z",
        "vintage_ingested_at_utc": "2026-05-05T12:05:00Z",
        "vintage_run_id": "FINAL_MIXED_RUN",
        "vintage_normalization_run_id": "FINAL_MIXED_NORM_RUN",
        "vintage_mode": "sec_plus_yahoo_fallback_final_mixed",
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


def _patch_process_ticker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **_kwargs: {
            "quarterly_refresh_mode": 1,
            "ttm_rows_written": 1,
            "lifecycle_rows_written": 1,
            "score_rows_written": 1,
            "ack_rows_written": 1,
        },
    )


def _success_runner(**_kwargs: object) -> dict[str, object]:
    return {
        "final_mixed_written": True,
        "statement_vintage_id": "mixed_sec_yahoo:usa:AAPL:2026-03-31:abc123",
        "source_hash": "abc123",
        "vintage_rows_inserted": 1,
        "provenance_rows_inserted": 5,
        "provenance_field_count": 5,
        "skipped_noop": 0,
        "already_known": 0,
        "error": None,
    }


def _noop_runner(**_kwargs: object) -> dict[str, object]:
    return {
        "final_mixed_written": False,
        "statement_vintage_id": None,
        "source_hash": None,
        "vintage_rows_inserted": 0,
        "provenance_rows_inserted": 0,
        "provenance_field_count": 0,
        "skipped_noop": 1,
        "already_known": 0,
        "error": None,
    }


def _failing_runner(**_kwargs: object) -> dict[str, object]:
    raise RuntimeError("FINAL_MIXED_MOCK_FAILURE")
