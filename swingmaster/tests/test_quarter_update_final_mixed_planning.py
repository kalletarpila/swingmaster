from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


def test_default_quarter_update_behavior_unchanged(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_planning_default.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    summary = _run_update(db_path)

    assert "vintage_requested" not in summary
    assert "vintage_final_mixed_plan_available" not in summary


def test_combined_planning_remains_no_execution(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_planning_no_execution.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    summary = _run_update(db_path, **_planning_kwargs())

    assert summary["vintage_execution_enabled"] is False
    assert summary["vintage_planning_only"] is True
    assert summary["vintage_final_mixed_planned"] is True
    assert summary["vintage_final_mixed_written"] is False
    assert summary["vintage_count_status"] == "planning_only_no_execution"


def test_combined_planning_does_not_write_vintage_db_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_planning_no_db_write.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    _run_update(db_path, **_planning_kwargs())

    with sqlite3.connect(str(db_path)) as conn:
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert vintage_count == 0
    assert provenance_count == 0


def test_combined_planning_does_not_pass_vintage_write_flags_to_child_paths(
    monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "final_mixed_planning_no_child_flags.db"
    run_migration(db_path)
    _insert_state_row(db_path, ticker="NOKIA.HE", market="omxh")
    seen_kwargs: dict[str, object] = {}

    def _fake_process_ticker(**kwargs: object) -> dict[str, int]:
        seen_kwargs.update(kwargs)
        return {"score_rows_written": 1}

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _fake_process_ticker)

    summary = _run_update(db_path, market="omxh", dry_run=False, **_planning_kwargs())

    assert summary["tickers_processed"] == 1
    assert "sec_vintage_options" not in seen_kwargs
    assert "yahoo_fallback_vintage_options" not in seen_kwargs
    assert "write_vintage" not in seen_kwargs
    assert not any(key.startswith("vintage_") for key in seen_kwargs)


def test_planning_helper_computes_final_mixed_statement_vintage_id() -> None:
    plan = _final_mixed_plan()

    assert str(plan["vintage_final_mixed_statement_vintage_id"]).startswith(
        "mixed_sec_yahoo:usa:AAPL:2026-03-31:"
    )


def test_planning_helper_computes_final_mixed_source_hash() -> None:
    plan = _final_mixed_plan()
    changed_plan = _final_mixed_plan(normalized_row=_normalized_row(revenue=101.0))

    assert isinstance(plan["vintage_final_mixed_source_hash"], str)
    assert len(str(plan["vintage_final_mixed_source_hash"])) == 64
    assert plan["vintage_final_mixed_source_hash"] != changed_plan["vintage_final_mixed_source_hash"]


def test_planning_helper_counts_planned_provenance_fields() -> None:
    plan = _final_mixed_plan()

    assert plan["vintage_final_mixed_provenance_field_count"] == 5


def test_planning_helper_preserves_sec_yahoo_and_unknown_provenance_roles() -> None:
    plan = _final_mixed_plan()
    source_map = plan["vintage_final_mixed_field_source_map"]
    assert isinstance(source_map, dict)

    assert source_map["revenue"]["source_provider"] == "sec_edgar"
    assert source_map["revenue"]["provenance_role"] == "PRIMARY_REPORTED"
    assert source_map["free_cashflow"]["source_provider"] == "yahoo"
    assert source_map["free_cashflow"]["provenance_role"] == "FALLBACK_REPORTED"
    assert source_map["net_income"]["source_provider"] == "unknown"
    assert source_map["net_income"]["merge_action"] == "SOURCE_NOT_PROVIDED"


def test_summary_fields_are_stable_when_no_per_period_plan_input_exists(tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_planning_null_fields.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    summary = _run_update(db_path, **_planning_kwargs())

    assert summary["vintage_final_mixed_plan_available"] is False
    assert summary["vintage_final_mixed_statement_vintage_id"] is None
    assert summary["vintage_final_mixed_source_hash"] is None
    assert summary["vintage_final_mixed_provenance_field_count"] is None


def test_final_mixed_planning_does_not_call_provider_functions(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "final_mixed_planning_no_provider_calls.db"
    run_migration(db_path)
    _insert_state_row(db_path)

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

    summary = _run_update(db_path, **_planning_kwargs())

    assert summary["dry_run"] == 1
    assert summary["vintage_planning_only"] is True


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
            ) VALUES (?, ?, 'sec_edgar', '2025-12-31', '2026-03-31', 1, ?)
            """,
            (ticker, market, "2026-05-05T00:00:00+00:00"),
        )
        conn.commit()


def _planning_kwargs() -> dict[str, object]:
    return {
        "write_vintage": True,
        "vintage_market": "usa",
        "vintage_available_at_utc": "2026-05-05T12:00:00Z",
        "vintage_ingested_at_utc": "2026-05-05T12:05:00Z",
        "vintage_run_id": "FINAL_MIXED_PLAN_RUN",
        "vintage_normalization_run_id": "FINAL_MIXED_PLAN_NORM_RUN",
        "vintage_mode": "sec_plus_yahoo_fallback_planning",
    }


def _run_update(db_path: Path, **overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "db_path": db_path,
        "osakedata_db_path": None,
        "run_id": "BASE",
        "market": "usa",
        "ticker": None,
        "limit": None,
        "dry_run": True,
        "skip_ack": False,
    }
    kwargs.update(overrides)
    return run_fundamental_quarter_update.run_fundamental_quarter_update(**kwargs)


def _final_mixed_plan(**overrides: object) -> dict[str, object]:
    normalized_row = overrides.get("normalized_row", _normalized_row())
    assert isinstance(normalized_row, dict)
    return run_fundamental_quarter_update.build_final_mixed_vintage_plan_summary(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-03-31",
        normalized_row=normalized_row,
        sec_field_source_map=_sec_source_map(),
        yahoo_field_source_map=_yahoo_source_map(),
        fallback_audit_rows=[_audit_row("free_cashflow", 30.0), _audit_row("total_debt", 20.0)],
    )


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
        "revenue": {
            "source_provider": "sec_edgar",
            "source_table": "rc_fundamental_statement_raw",
            "source_row_ref": "sec:revenue:AAPL:2026-03-31",
            "source_document_id": "sec_doc_1",
            "source_hash": "sec_hash_revenue",
            "provenance_role": "PRIMARY_REPORTED",
            "merge_action": "SEC_RETAINED",
        },
        "cash": {
            "source_provider": "sec_edgar",
            "source_table": "rc_fundamental_statement_raw",
            "source_row_ref": "sec:cash:AAPL:2026-03-31",
            "source_document_id": "sec_doc_1",
            "source_hash": "sec_hash_cash",
            "provenance_role": "PRIMARY_REPORTED",
            "merge_action": "SEC_RETAINED",
        },
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
