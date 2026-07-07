from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


def _completion(status: str) -> dict[str, object]:
    return {"vintage_completion_status": status}


def _latest_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "revenue": 100.0,
        "operating_income": 20.0,
        "ebit": 20.0,
        "operating_cashflow": None,
        "capex": None,
        "free_cashflow": None,
        "cash": None,
        "total_debt": None,
        "currency": "USD",
        "run_id": "BASE__QUARTERLY",
    }
    row.update(overrides)
    return row


def _sec_source(field_name: str) -> dict[str, object]:
    return {
        "source_provider": "sec_edgar",
        "source_table": "rc_fundamental_statement_raw",
        "source_row_ref": field_name,
        "source_hash": f"sec_hash_{field_name}",
        "provenance_role": "PRIMARY_REPORTED",
        "merge_action": "SEC_RETAINED",
    }


def _yahoo_audit(field_name: str, new_value: float) -> dict[str, object]:
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
        "net_income": None,
        "operating_cashflow": None,
        "capex": None,
        "free_cashflow": None,
        "cash": None,
        "total_debt": None,
        "shares_outstanding": None,
        "source_run_id": "YAHOO_SOURCE",
        "run_id": "YAHOO_QTR",
        "created_at_utc": "2026-05-05T11:00:00Z",
    }


def _plan(
    *,
    completion_status: str,
    latest_rows: list[dict[str, object]] | None = None,
    sec_provenance_by_key: dict[tuple[str, str], dict[str, dict[str, object]]] | None = None,
    yahoo_audit_rows_by_key: dict[tuple[str, str], list[dict[str, object]]] | None = None,
    yahoo_rows_by_key: dict[tuple[str, str], dict[str, object]] | None = None,
    final_mixed_scope_keys: list[tuple[str, str]] | None = None,
    yahoo_vintage_scope_keys: list[tuple[str, str]] | None = None,
) -> dict[str, object]:
    return run_fundamental_quarter_update.plan_quarter_update_yahoo_aware_vintage(
        completion_summary=_completion(completion_status),
        latest_rows=latest_rows or [],
        sec_provenance_by_key=sec_provenance_by_key or {},
        yahoo_audit_rows_by_key=yahoo_audit_rows_by_key or {},
        yahoo_rows_by_key=yahoo_rows_by_key or {},
        market="usa",
        available_at_utc="2026-05-05T12:00:00Z",
        ingested_at_utc="2026-05-05T12:05:00Z",
        vintage_run_id="VINTAGE_RUN",
        final_mixed_scope_keys=final_mixed_scope_keys,
        yahoo_vintage_scope_keys=yahoo_vintage_scope_keys,
    )


def _insert_state_row(db_path: Path) -> None:
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
            ) VALUES ('AAPL', 'usa', 'sec_edgar', '2025-12-31', '2026-03-31', 1, '2026-05-05T00:00:00+00:00')
            """
        )
        conn.commit()


def test_sec_vintage_sufficient_returns_no_action() -> None:
    summary = _plan(completion_status="SEC_VINTAGE_SUFFICIENT")

    assert summary["vintage_yahoo_aware_planning_status"] == "NO_ACTION_REQUIRED"
    assert summary["vintage_yahoo_aware_next_action"] == "NONE"
    assert summary["vintage_planned_final_mixed_rows"] == 0


def test_final_mixed_required_with_sec_and_yahoo_sources_is_plan_ready() -> None:
    key = ("AAPL", "2026-03-31")
    summary = _plan(
        completion_status="FINAL_MIXED_REQUIRED",
        latest_rows=[_latest_row(revenue=100.0, operating_income=20.0)],
        sec_provenance_by_key={key: {"revenue": _sec_source("revenue"), "ebit": _sec_source("ebit")}},
        yahoo_audit_rows_by_key={key: [_yahoo_audit("operating_income", 20.0)]},
        final_mixed_scope_keys=[key],
    )

    assert summary["vintage_yahoo_aware_planning_status"] == "FINAL_MIXED_PLAN_READY"
    assert summary["vintage_planned_final_mixed_rows"] == 1
    assert summary["vintage_planned_yahoo_aware_provenance_rows"] == 3
    assert str(summary["vintage_yahoo_aware_sample_statement_vintage_ids"]).startswith("mixed_sec_yahoo:usa:AAPL")


def test_final_mixed_plan_uses_final_latest_row_values_for_hash() -> None:
    key = ("AAPL", "2026-03-31")
    base = _plan(
        completion_status="FINAL_MIXED_REQUIRED",
        latest_rows=[_latest_row(revenue=100.0, operating_income=20.0)],
        sec_provenance_by_key={key: {"revenue": _sec_source("revenue"), "ebit": _sec_source("ebit")}},
        yahoo_audit_rows_by_key={key: [_yahoo_audit("operating_income", 20.0)]},
        final_mixed_scope_keys=[key],
    )
    changed = _plan(
        completion_status="FINAL_MIXED_REQUIRED",
        latest_rows=[_latest_row(revenue=101.0, operating_income=20.0)],
        sec_provenance_by_key={key: {"revenue": _sec_source("revenue"), "ebit": _sec_source("ebit")}},
        yahoo_audit_rows_by_key={key: [_yahoo_audit("operating_income", 20.0)]},
        final_mixed_scope_keys=[key],
    )

    assert base["vintage_yahoo_aware_sample_source_hashes"] != changed["vintage_yahoo_aware_sample_source_hashes"]


def test_yahoo_filled_fields_get_fallback_plan_provenance() -> None:
    source_map = run_fundamental_quarter_update._yahoo_audit_field_source_map([_yahoo_audit("operating_income", 20.0)])

    assert source_map["operating_income"]["source_provider"] == "yahoo"
    assert source_map["operating_income"]["provenance_role"] == "FALLBACK_REPORTED"
    assert source_map["operating_income"]["merge_action"] == "YAHOO_FILLED_MISSING"


def test_sec_retained_fields_keep_sec_provenance_in_final_mixed_plan() -> None:
    key = ("AAPL", "2026-03-31")
    summary = _plan(
        completion_status="FINAL_MIXED_REQUIRED",
        latest_rows=[_latest_row(revenue=100.0, operating_income=20.0)],
        sec_provenance_by_key={key: {"revenue": _sec_source("revenue"), "ebit": _sec_source("ebit")}},
        yahoo_audit_rows_by_key={key: [_yahoo_audit("operating_income", 20.0)]},
        final_mixed_scope_keys=[key],
    )

    assert summary["vintage_yahoo_aware_planning_status"] == "FINAL_MIXED_PLAN_READY"
    assert summary["vintage_yahoo_aware_unknown_provenance_fields"] == ""


def test_unknown_non_null_field_blocks_final_mixed_plan() -> None:
    key = ("AAPL", "2026-03-31")
    summary = _plan(
        completion_status="FINAL_MIXED_REQUIRED",
        latest_rows=[_latest_row(revenue=100.0, operating_income=20.0, cash=50.0)],
        sec_provenance_by_key={key: {"revenue": _sec_source("revenue"), "ebit": _sec_source("ebit")}},
        yahoo_audit_rows_by_key={key: [_yahoo_audit("operating_income", 20.0)]},
        final_mixed_scope_keys=[key],
    )

    assert summary["vintage_yahoo_aware_planning_status"] == "PLAN_BLOCKED"
    assert "AAPL:2026-03-31:cash" in str(summary["vintage_yahoo_aware_unknown_provenance_fields"])


def test_yahoo_inserted_missing_quarter_plans_yahoo_vintage() -> None:
    key = ("AAPL", "2026-03-31")
    summary = _plan(
        completion_status="YAHOO_VINTAGE_REQUIRED",
        latest_rows=[_latest_row(run_id="BASE__ENRICH")],
        yahoo_rows_by_key={key: _yahoo_row()},
        yahoo_vintage_scope_keys=[key],
    )

    assert summary["vintage_yahoo_aware_planning_status"] == "YAHOO_VINTAGE_PLAN_READY"
    assert summary["vintage_planned_yahoo_vintage_rows"] == 1
    assert str(summary["vintage_yahoo_aware_sample_statement_vintage_ids"]).startswith(
        "yahoo:yahoo_missing_quarter_insert:usa:AAPL"
    )


def test_missing_yahoo_run_linkage_blocks_yahoo_plan() -> None:
    summary = _plan(
        completion_status="YAHOO_VINTAGE_REQUIRED",
        latest_rows=[_latest_row(run_id="BASE__ENRICH")],
        yahoo_rows_by_key={},
    )

    assert summary["vintage_yahoo_aware_planning_status"] == "PLAN_BLOCKED"
    assert summary["vintage_yahoo_aware_block_reason"] == "INSUFFICIENT_YAHOO_RUN_LINKAGE"


def test_quarter_update_summary_surfaces_yahoo_aware_plan_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "yahoo_aware_summary.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    def _fake_process_ticker(**_kwargs: object) -> dict[str, object]:
        return {
            "sec_latest_writer_vintage_summary": {
                "vintage_rows_inserted": 1,
                "provenance_rows_inserted": 2,
                "skipped_already_had_vintage": 0,
                "blocked_rows": 0,
            },
            "vintage_post_run_guard_summary": {
                "vintage_completion_status": "FINAL_MIXED_REQUIRED",
                "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
                "vintage_yahoo_aware_next_action": "CREATE_FINAL_MIXED_VINTAGE",
                "vintage_planned_final_mixed_rows": 1,
                "vintage_planned_yahoo_vintage_rows": 0,
                "vintage_planned_yahoo_aware_provenance_rows": 2,
                "vintage_yahoo_aware_blocked_rows": 0,
                "vintage_yahoo_aware_unknown_provenance_fields": "",
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

    assert summary["vintage_yahoo_aware_planning_status"] == "FINAL_MIXED_PLAN_READY"
    assert summary["vintage_planned_final_mixed_rows"] == 1
    assert summary["vintage_planned_yahoo_aware_provenance_rows"] == 2


def test_default_without_vintage_flags_omits_yahoo_aware_plan_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "yahoo_aware_default.db"
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

    assert "vintage_yahoo_aware_planning_status" not in summary
