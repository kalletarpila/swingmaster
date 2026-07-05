from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


def _state(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_state (
                ticker, market, primary_source, latest_db_period_end_date,
                detected_source_period_end_date, new_quarter_available, last_updated_at_utc
            ) VALUES ('AAPL', 'usa', 'sec_edgar', '2025-12-31', '2026-03-31', 1, '2026-05-05T00:00:00+00:00')
            """
        )
        conn.commit()


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": 100.0,
        "operating_income": 20.0,
        "ebit": 20.0,
        "currency": "USD",
        "run_id": "BASE__QUARTERLY",
    }
    row.update(overrides)
    return row


def _sec(field: str) -> dict[str, object]:
    return {
        "source_provider": "sec_edgar",
        "source_table": "rc_fundamental_statement_raw",
        "source_row_ref": field,
        "source_hash": f"sec_{field}",
        "provenance_role": "PRIMARY_REPORTED",
        "merge_action": "SEC_RETAINED",
    }


def _yahoo_audit(field: str = "operating_income") -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "field_name": field,
        "old_value": None,
        "new_value": 20.0,
        "fallback_source": "yahoo",
        "enrichment_status": "FILLED_FROM_YAHOO",
        "matched_yahoo_period_end_date": "2026-03-31",
        "match_method": "EXACT",
        "run_id": "BASE__ENRICH",
        "created_at_utc": "2026-05-05T12:00:00Z",
    }


def _yahoo_row() -> dict[str, object]:
    return {
        "market": "usa",
        "symbol": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": 100.0,
        "operating_income": 20.0,
        "source_run_id": "YAHOO_SOURCE",
        "run_id": "YAHOO_QTR",
        "created_at_utc": "2026-05-05T11:00:00Z",
    }


def _final_mixed_candidate() -> dict[tuple[str, str], dict[str, object]]:
    return {
        ("AAPL", "2026-03-31"): {
            "normalized_row": _row(run_id="VINTAGE_RUN"),
            "sec_field_source_map": {"revenue": _sec("revenue"), "ebit": _sec("ebit")},
            "yahoo_field_source_map": run_fundamental_quarter_update._yahoo_audit_field_source_map([_yahoo_audit()]),
            "fallback_audit_rows": [_yahoo_audit()],
        }
    }


def _yahoo_candidate() -> dict[tuple[str, str], dict[str, object]]:
    return {
        ("AAPL", "2026-03-31"): {
            "normalized_row": _row(run_id="VINTAGE_RUN"),
            "yahoo_quarterly_row": _yahoo_row(),
        }
    }


def _execute(
    conn: sqlite3.Connection,
    *,
    plan_status: str,
    final_candidates: dict[tuple[str, str], dict[str, object]] | None = None,
    yahoo_candidates: dict[tuple[str, str], dict[str, object]] | None = None,
) -> dict[str, object]:
    return run_fundamental_quarter_update.execute_quarter_update_yahoo_aware_vintage_plan(
        conn,
        plan={"vintage_yahoo_aware_planning_status": plan_status},
        final_mixed_candidates_by_key=final_candidates or {},
        yahoo_vintage_candidates_by_key=yahoo_candidates or {},
        market="usa",
        available_at_utc="2026-05-05T12:00:00Z",
        ingested_at_utc="2026-05-05T12:05:00Z",
        vintage_run_id="VINTAGE_RUN",
    )


def test_default_without_vintage_flags_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "default.db"
    run_migration(db_path)
    _state(db_path)
    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", lambda **_kwargs: {})
    monkeypatch.setattr(run_fundamental_quarter_update, "resolve_latest_close_as_of_date", lambda *_args, **_kwargs: "2026-05-05")
    monkeypatch.setattr(run_fundamental_quarter_update, "run_fundamental_valuation", lambda **_kwargs: {"rows_written": 0})

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "osakedata.db",
        run_id="BASE",
        market="usa",
        ticker="AAPL",
        limit=None,
        dry_run=False,
        skip_ack=True,
    )

    assert "vintage_yahoo_aware_execution_status" not in summary


def test_plan_only_does_not_execute_yahoo_aware_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "plan_only.db"
    run_migration(db_path)
    _state(db_path)

    def _fake_process_ticker(**kwargs: object) -> dict[str, object]:
        assert kwargs["vintage_yahoo_aware_action"] == "plan_only"
        return {
            "sec_latest_writer_vintage_summary": {"vintage_rows_inserted": 0, "provenance_rows_inserted": 0},
            "vintage_post_run_guard_summary": {
                "vintage_yahoo_aware_execution_status": "NOT_REQUESTED",
            },
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _fake_process_ticker)
    monkeypatch.setattr(run_fundamental_quarter_update, "resolve_latest_close_as_of_date", lambda *_args, **_kwargs: "2026-05-05")
    monkeypatch.setattr(run_fundamental_quarter_update, "run_fundamental_valuation", lambda **_kwargs: {"rows_written": 0})

    summary = run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=tmp_path / "osakedata.db",
        run_id="BASE",
        market="usa",
        ticker="AAPL",
        limit=None,
        dry_run=False,
        skip_ack=True,
        write_vintage=True,
        vintage_market="usa",
        vintage_available_at_utc="2026-05-05T12:00:00Z",
        vintage_ingested_at_utc="2026-05-05T12:05:00Z",
        vintage_run_id="VINTAGE_RUN",
        vintage_mode="sec_latest_writer",
    )

    assert summary["vintage_yahoo_aware_execution_status"] == "NOT_REQUESTED"


def test_write_action_requires_sec_latest_writer_mode() -> None:
    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_YAHOO_AWARE_WRITE_REQUIRES_SEC_LATEST_WRITER"):
        run_fundamental_quarter_update.validate_vintage_options(
            write_vintage=True,
            vintage_market="usa",
            vintage_available_at_utc="2026-05-05T12:00:00Z",
            vintage_ingested_at_utc="2026-05-05T12:05:00Z",
            vintage_run_id="VINTAGE_RUN",
            vintage_mode="validation_only",
            vintage_yahoo_aware_action="write",
        )


def test_final_mixed_plan_writes_vintage_and_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        summary = _execute(conn, plan_status="FINAL_MIXED_PLAN_READY", final_candidates=_final_mixed_candidate())
        conn.commit()
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        providers = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT source_provider FROM rc_fundamental_quarterly_field_provenance"
            ).fetchall()
        }

    assert summary["vintage_yahoo_aware_execution_status"] == "EXECUTION_COMPLETED"
    assert summary["vintage_yahoo_aware_final_mixed_rows_written"] == 1
    assert summary["vintage_yahoo_aware_provenance_rows_written"] == 3
    assert vintage_count == 1
    assert providers == {"sec_edgar", "yahoo"}


def test_unknown_non_null_field_blocks_before_write(tmp_path: Path) -> None:
    db_path = tmp_path / "blocked.db"
    run_migration(db_path)
    bad_candidate = _final_mixed_candidate()
    bad_candidate[("AAPL", "2026-03-31")]["normalized_row"]["cash"] = 50.0
    with sqlite3.connect(str(db_path)) as conn:
        summary = _execute(conn, plan_status="FINAL_MIXED_PLAN_READY", final_candidates=bad_candidate)
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]

    assert summary["vintage_yahoo_aware_execution_status"] == "EXECUTION_BLOCKED"
    assert "UNKNOWN_PROVENANCE_FIELDS" in str(summary["vintage_yahoo_aware_error"])
    assert vintage_count == 0


def test_yahoo_inserted_missing_quarter_writes_yahoo_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "yahoo.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        summary = _execute(conn, plan_status="YAHOO_VINTAGE_PLAN_READY", yahoo_candidates=_yahoo_candidate())
        conn.commit()
        provider = conn.execute("SELECT source_provider FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        merge_actions = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT merge_action FROM rc_fundamental_quarterly_field_provenance"
            ).fetchall()
        }

    assert summary["vintage_yahoo_aware_execution_status"] == "EXECUTION_COMPLETED"
    assert summary["vintage_yahoo_aware_yahoo_vintage_rows_written"] == 1
    assert provider == "yahoo"
    assert "YAHOO_BRIDGED" in merge_actions


def test_missing_yahoo_candidate_blocks() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        summary = _execute(conn, plan_status="YAHOO_VINTAGE_PLAN_READY", yahoo_candidates={})
    finally:
        conn.close()
    assert summary["vintage_yahoo_aware_execution_status"] == "EXECUTION_BLOCKED"
    assert summary["vintage_yahoo_aware_error"] == "YAHOO_VINTAGE_CANDIDATES_REQUIRED"


def test_duplicate_existing_vintage_blocks_without_replace(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicate.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        first = _execute(conn, plan_status="FINAL_MIXED_PLAN_READY", final_candidates=_final_mixed_candidate())
        second = _execute(conn, plan_status="FINAL_MIXED_PLAN_READY", final_candidates=_final_mixed_candidate())
        conn.commit()
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]

    assert first["vintage_yahoo_aware_execution_status"] == "EXECUTION_COMPLETED"
    assert second["vintage_yahoo_aware_execution_status"] == "EXECUTION_BLOCKED"
    assert second["vintage_yahoo_aware_rows_skipped"] == 1
    assert vintage_count == 1


def test_summary_fields_reflect_blocked_counts() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        summary = _execute(conn, plan_status="PLAN_BLOCKED")
    finally:
        conn.close()
    assert summary["vintage_yahoo_aware_execution_status"] == "EXECUTION_BLOCKED"
    assert summary["vintage_yahoo_aware_error"] == "PLAN_NOT_EXECUTABLE"
