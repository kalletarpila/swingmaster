from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


def _parity_summary(
    *,
    status: str = "OK",
    latest_without_vintage: int = 0,
    vintage_without_latest: int = 0,
    value_mismatch: int = 0,
    duplicate_vintage_ids: int = 0,
) -> dict[str, object]:
    return {
        "vintage_post_run_parity_status": status,
        "vintage_post_run_latest_without_vintage_count": latest_without_vintage,
        "vintage_post_run_vintage_without_latest_count": vintage_without_latest,
        "vintage_post_run_value_mismatch_count": value_mismatch,
        "vintage_post_run_duplicate_statement_vintage_id_count": duplicate_vintage_ids,
    }


def _yahoo_summary(
    *,
    status: str = "NO_YAHOO_IMPACT_DETECTED",
    fallback_rows: int = 0,
    inserted_rows: int = 0,
    filled_fields: int = 0,
    audit_rows: int = 0,
) -> dict[str, object]:
    return {
        "vintage_yahoo_impact_status": status,
        "vintage_yahoo_fallback_rows_detected": fallback_rows,
        "vintage_yahoo_inserted_missing_quarter_rows_detected": inserted_rows,
        "vintage_yahoo_filled_field_rows_detected": filled_fields,
        "vintage_yahoo_audit_rows_detected": audit_rows,
        "vintage_yahoo_can_create_post_sec_vintage_drift": status == "YAHOO_IMPACT_DETECTED",
    }


def _classify(
    parity: dict[str, object] | None = None,
    yahoo: dict[str, object] | None = None,
    value_parity: dict[str, object] | None = None,
) -> dict[str, object]:
    return run_fundamental_quarter_update.classify_quarter_update_vintage_completion(
        parity_summary=parity or _parity_summary(),
        yahoo_impact_summary=yahoo or _yahoo_summary(),
        value_parity_summary=value_parity,
    )


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
        conn.commit()


def test_completion_gate_sec_vintage_sufficient_when_parity_ok_and_no_yahoo_impact() -> None:
    summary = _classify()

    assert summary["vintage_completion_status"] == "SEC_VINTAGE_SUFFICIENT"
    assert summary["vintage_next_required_action"] == "NONE"
    assert summary["vintage_sec_only_sufficient"] is True
    assert summary["vintage_final_mixed_required"] is False


def test_completion_gate_latest_without_vintage_blocks_post_run_drift() -> None:
    summary = _classify(parity=_parity_summary(status="DRIFT", latest_without_vintage=1))

    assert summary["vintage_completion_status"] == "BLOCKED_POST_RUN_DRIFT"
    assert summary["vintage_completion_reason"] == "latest_rows_without_vintage"
    assert summary["vintage_next_required_action"] == "INVESTIGATE_DRIFT"
    assert summary["vintage_blocked_post_run_drift"] is True


def test_completion_gate_yahoo_filled_fields_requires_final_mixed() -> None:
    summary = _classify(
        yahoo=_yahoo_summary(
            status="YAHOO_IMPACT_DETECTED",
            fallback_rows=1,
            filled_fields=2,
            audit_rows=2,
        )
    )

    assert summary["vintage_completion_status"] == "FINAL_MIXED_REQUIRED"
    assert summary["vintage_completion_reason"] == "yahoo_filled_fields_on_sec_backed_latest"
    assert summary["vintage_next_required_action"] == "CREATE_FINAL_MIXED_VINTAGE"
    assert summary["vintage_final_mixed_required"] is True


def test_completion_gate_yahoo_inserted_missing_quarter_requires_yahoo_or_final_mixed() -> None:
    summary = _classify(
        parity=_parity_summary(status="DRIFT", latest_without_vintage=1),
        yahoo=_yahoo_summary(
            status="YAHOO_IMPACT_DETECTED",
            inserted_rows=1,
        ),
    )

    assert summary["vintage_completion_status"] == "YAHOO_VINTAGE_REQUIRED"
    assert summary["vintage_completion_reason"] == "yahoo_inserted_missing_quarter"
    assert summary["vintage_next_required_action"] == "CREATE_YAHOO_OR_FINAL_MIXED_VINTAGE"
    assert summary["vintage_yahoo_vintage_required"] is True


def test_completion_gate_unknown_run_linkage_returns_unknown() -> None:
    summary = _classify(
        parity=_parity_summary(status="UNKNOWN_RUN_LINKAGE"),
        yahoo=_yahoo_summary(status="UNKNOWN_RUN_LINKAGE"),
    )

    assert summary["vintage_completion_status"] == "UNKNOWN"
    assert summary["vintage_completion_reason"] == "run_linkage_unknown"
    assert summary["vintage_next_required_action"] == "IMPROVE_RUN_LINKAGE"


def test_completion_gate_value_mismatch_with_exact_yahoo_audit_requires_final_mixed() -> None:
    summary = _classify(
        parity=_parity_summary(status="DRIFT", value_mismatch=1),
        yahoo=_yahoo_summary(
            status="YAHOO_IMPACT_DETECTED",
            audit_rows=1,
            filled_fields=1,
        ),
        value_parity={
            "vintage_post_run_yahoo_explained_mismatch_count": 1,
            "vintage_post_run_unexplained_mismatch_count": 0,
        },
    )

    assert summary["vintage_completion_status"] == "FINAL_MIXED_REQUIRED"
    assert summary["vintage_completion_reason"] == "value_mismatch_exactly_explained_by_yahoo_audit"
    assert summary["vintage_final_mixed_required"] is True


def test_completion_gate_value_mismatch_with_only_aggregate_yahoo_audit_blocks_drift() -> None:
    summary = _classify(
        parity=_parity_summary(status="DRIFT", value_mismatch=1),
        yahoo=_yahoo_summary(
            status="YAHOO_IMPACT_DETECTED",
            audit_rows=99,
            filled_fields=99,
        ),
    )

    assert summary["vintage_completion_status"] == "BLOCKED_POST_RUN_DRIFT"
    assert summary["vintage_completion_reason"] == "unexplained_value_mismatch"
    assert summary["vintage_final_mixed_required"] is False


def test_completion_gate_value_mismatch_without_explanation_blocks_drift() -> None:
    summary = _classify(parity=_parity_summary(status="DRIFT", value_mismatch=1))

    assert summary["vintage_completion_status"] == "BLOCKED_POST_RUN_DRIFT"
    assert summary["vintage_completion_reason"] == "unexplained_value_mismatch"
    assert summary["vintage_blocked_post_run_drift"] is True


def test_quarter_update_sec_latest_writer_summary_surfaces_completion_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "completion_summary.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    def _fake_process_ticker(**_kwargs: object) -> dict[str, object]:
        guard_summary = {
            **_parity_summary(),
            **_yahoo_summary(
                status="YAHOO_IMPACT_DETECTED",
                fallback_rows=1,
                filled_fields=1,
                audit_rows=1,
            ),
            "vintage_recommendation": "phase_4k3_final_mixed_or_post_run_parity_apply",
        }
        guard_summary.update(
            run_fundamental_quarter_update.classify_quarter_update_vintage_completion(
                parity_summary=guard_summary,
                yahoo_impact_summary=guard_summary,
            )
        )
        return {
            "sec_latest_writer_vintage_summary": {
                "vintage_rows_inserted": 1,
                "provenance_rows_inserted": 7,
                "skipped_already_had_vintage": 0,
                "blocked_rows": 0,
            },
            "vintage_post_run_guard_summary": guard_summary,
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

    assert summary["vintage_completion_status"] == "FINAL_MIXED_REQUIRED"
    assert summary["vintage_next_required_action"] == "CREATE_FINAL_MIXED_VINTAGE"
    assert summary["vintage_final_mixed_required"] is True


def test_default_without_vintage_flags_omits_completion_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "completion_default.db"
    run_migration(db_path)
    _insert_state_row(db_path)

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

    assert "vintage_completion_status" not in summary
    assert "vintage_next_required_action" not in summary
