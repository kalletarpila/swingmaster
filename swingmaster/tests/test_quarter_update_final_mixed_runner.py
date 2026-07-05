from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_vintage_reader import get_pit_quarterly_vintage


def test_default_quarter_update_behavior_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_runner_default.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    summary = _run_update(db_path)

    assert "vintage_requested" not in summary
    assert "vintage_final_mixed_rows_inserted" not in summary


def test_final_mixed_mode_requires_write_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_runner_requires_write.db"

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_WRITE_REQUIRED_FOR_MODE"):
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
def test_final_mixed_mode_requires_pit_metadata(tmp_path: Path, missing_key: str, expected_error: str) -> None:
    db_path = tmp_path / f"final_mixed_runner_missing_{missing_key}.db"
    kwargs = _final_mixed_kwargs()
    kwargs[missing_key] = None

    with pytest.raises(RuntimeError, match=expected_error):
        _run_update(db_path, **kwargs)


def test_final_mixed_mode_fails_if_inputs_are_not_available_before_child_steps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "final_mixed_runner_inputs_required.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "process_ticker",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("process_ticker should not run")),
    )

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_INPUTS_REQUIRED"):
        _run_update(db_path, dry_run=False, **_final_mixed_kwargs())


def test_injected_runner_success_updates_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_runner_injected_success.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _patch_process_ticker(monkeypatch)

    summary = _run_update(
        db_path,
        dry_run=False,
        final_mixed_execution_runner=lambda **_kwargs: _execution_summary(vintage_rows_inserted=1),
        **_final_mixed_kwargs(),
    )

    assert summary["vintage_final_mixed_written"] is True
    assert summary["vintage_final_mixed_rows_inserted"] == 1
    assert summary["vintage_final_mixed_provenance_rows_inserted"] == 5


def test_injected_runner_noop_updates_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_runner_injected_noop.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _patch_process_ticker(monkeypatch)

    summary = _run_update(
        db_path,
        dry_run=False,
        final_mixed_execution_runner=lambda **_kwargs: _execution_summary(vintage_rows_inserted=0, skipped_noop=1),
        **_final_mixed_kwargs(),
    )

    assert summary["vintage_final_mixed_written"] is False
    assert summary["vintage_rows_skipped_noop"] == 1


def test_injected_runner_failure_surfaces_controlled_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_runner_injected_failure.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _patch_process_ticker(monkeypatch)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed=1"):
        _run_update(
            db_path,
            dry_run=False,
            final_mixed_execution_runner=lambda **_kwargs: (_ for _ in ()).throw(
                RuntimeError("FINAL_MIXED_TEST_FAILURE")
            ),
            **_final_mixed_kwargs(),
        )


def test_production_safe_runner_writes_final_mixed_row_in_temp_db(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_runner_direct_write.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        summary = run_fundamental_quarter_update.run_final_mixed_vintage_execution_for_ticker(
            conn,
            ticker="AAPL",
            market="usa",
            normalized_row=_normalized_row(),
            sec_field_source_map=_sec_source_map(),
            yahoo_field_source_map=_yahoo_source_map(),
            fallback_audit_rows=_audit_rows(),
            available_at_utc="2026-05-03T10:30:00Z",
            ingested_at_utc="2026-05-03T10:31:00Z",
            run_id="FINAL_MIXED_RUN",
            normalization_run_id="FINAL_MIXED_NORM",
        )

    assert summary["final_mixed_written"] is True
    assert summary["vintage_rows_inserted"] == 1
    assert summary["provenance_rows_inserted"] == 5


def test_production_safe_runner_returns_pit_readable_row_in_temp_db(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_runner_pit.db"
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        summary = run_fundamental_quarter_update.run_final_mixed_vintage_execution_for_ticker(
            conn,
            ticker="AAPL",
            market="usa",
            normalized_row=_normalized_row(),
            sec_field_source_map=_sec_source_map(),
            yahoo_field_source_map=_yahoo_source_map(),
            fallback_audit_rows=_audit_rows(),
            available_at_utc="2026-05-03T10:30:00Z",
            ingested_at_utc="2026-05-03T10:31:00Z",
            run_id="FINAL_MIXED_RUN",
            normalization_run_id="FINAL_MIXED_NORM",
        )
        pit_row = get_pit_quarterly_vintage(
            conn,
            "AAPL",
            "2026-03-31",
            "2026-05-03T10:30:00Z",
            market="usa",
        )

    assert pit_row is not None
    assert pit_row["statement_vintage_id"] == summary["statement_vintage_id"]


def test_production_safe_runner_does_not_open_db_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_runner_no_open.db"
    run_migration(db_path)
    conn = sqlite3.connect(str(db_path))
    monkeypatch.setattr(
        run_fundamental_quarter_update.sqlite3,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("sqlite3.connect should not run")),
    )
    try:
        summary = run_fundamental_quarter_update.run_final_mixed_vintage_execution_for_ticker(
            conn,
            ticker="AAPL",
            market="usa",
            normalized_row=_normalized_row(),
            sec_field_source_map=_sec_source_map(),
            yahoo_field_source_map=_yahoo_source_map(),
            fallback_audit_rows=_audit_rows(),
            available_at_utc="2026-05-03T10:30:00Z",
            ingested_at_utc="2026-05-03T10:31:00Z",
            run_id="FINAL_MIXED_RUN",
            normalization_run_id="FINAL_MIXED_NORM",
        )
    finally:
        conn.close()

    assert summary["final_mixed_written"] is True


def test_no_provider_functions_are_used(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_runner_no_provider.db"
    run_migration(db_path)
    _insert_state_row(db_path, ticker="AAPL")
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
        dry_run=False,
        final_mixed_inputs_by_key={("AAPL", "2026-03-31"): _final_mixed_inputs()},
        **_final_mixed_kwargs(),
    )

    assert summary["vintage_final_mixed_written"] is True
    assert summary["vintage_final_mixed_rows_inserted"] == 1


def _insert_state_row(db_path: Path, ticker: str = "AAPL", market: str = "omxh") -> None:
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


def _final_mixed_kwargs() -> dict[str, object]:
    return {
        "write_vintage": True,
        "vintage_market": "usa",
        "vintage_available_at_utc": "2026-05-03T10:30:00Z",
        "vintage_ingested_at_utc": "2026-05-03T10:31:00Z",
        "vintage_run_id": "FINAL_MIXED_RUN",
        "vintage_normalization_run_id": "FINAL_MIXED_NORM",
        "vintage_mode": "sec_plus_yahoo_fallback_final_mixed",
    }


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


def _execution_summary(*, vintage_rows_inserted: int, skipped_noop: int = 0) -> dict[str, object]:
    return {
        "final_mixed_written": vintage_rows_inserted > 0,
        "statement_vintage_id": "mixed_sec_yahoo:usa:AAPL:2026-03-31:abc123",
        "source_hash": "abc123",
        "vintage_rows_inserted": vintage_rows_inserted,
        "provenance_rows_inserted": 5 if vintage_rows_inserted > 0 else 0,
        "provenance_field_count": 5 if vintage_rows_inserted > 0 else 0,
        "skipped_noop": skipped_noop,
        "already_known": 0,
        "error": None,
    }


def _final_mixed_inputs() -> dict[str, object]:
    return {
        "normalized_row": _normalized_row(),
        "sec_field_source_map": _sec_source_map(),
        "yahoo_field_source_map": _yahoo_source_map(),
        "fallback_audit_rows": _audit_rows(),
    }


def _normalized_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": 100.0,
        "gross_profit": None,
        "operating_income": None,
        "ebit": None,
        "ebitda": None,
        "net_income": 25.0,
        "operating_cashflow": None,
        "capex": None,
        "free_cashflow": 30.0,
        "cash": 80.0,
        "total_debt": 20.0,
        "shares_outstanding": None,
        "currency": "USD",
        "run_id": "LATEST_RUN1",
    }
    row.update(overrides)
    return row


def _sec_source_map() -> dict[str, dict[str, object]]:
    return {
        "revenue": _sec_source("revenue"),
        "cash": _sec_source("cash"),
    }


def _sec_source(field_name: str) -> dict[str, object]:
    return {
        "source_provider": "sec_edgar",
        "source_table": "rc_fundamental_statement_raw",
        "source_row_ref": f"sec:{field_name}:AAPL:2026-03-31",
        "source_document_id": "sec_doc_1",
        "source_hash": f"sec_hash_{field_name}",
        "provenance_role": "PRIMARY_REPORTED",
        "merge_action": "SEC_RETAINED",
    }


def _yahoo_source_map() -> dict[str, dict[str, object]]:
    return {
        "free_cashflow": _yahoo_source("free_cashflow", 30.0),
        "total_debt": _yahoo_source("total_debt", 20.0),
    }


def _yahoo_source(field_name: str, new_value: float) -> dict[str, object]:
    return {
        "source_provider": "yahoo",
        "source_table": "rc_fundamental_quarterly_enrichment_audit",
        "source_row_ref": f"AAPL:2026-03-31:{field_name}:2026-03-31:EXACT",
        "source_hash": f"yahoo_hash_{field_name}",
        "provenance_role": "FALLBACK_REPORTED",
        "merge_action": "YAHOO_FILLED_MISSING",
        "old_value": None,
        "new_value": new_value,
        "available_at_utc": "2026-05-03T10:30:00Z",
        "created_at_utc": "2026-05-03T10:30:00Z",
        "run_id": "ENRICH_RUN1",
        "enrichment_run_id": "ENRICH_RUN1",
    }


def _audit_rows() -> list[dict[str, object]]:
    return [_audit_row("free_cashflow", 30.0), _audit_row("total_debt", 20.0)]


def _audit_row(field_name: str, new_value: float) -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "field_name": field_name,
        "old_value": None,
        "new_value": new_value,
        "primary_source": "sec_edgar",
        "fallback_source": "yahoo",
        "enrichment_status": "FILLED_FROM_YAHOO",
        "matched_yahoo_period_end_date": "2026-03-31",
        "match_method": "EXACT",
        "run_id": "ENRICH_RUN1",
        "created_at_utc": "2026-05-03T10:30:00Z",
    }
