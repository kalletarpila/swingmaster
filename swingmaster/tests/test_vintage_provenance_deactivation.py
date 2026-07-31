from __future__ import annotations

import sqlite3
import inspect
from pathlib import Path
from uuid import uuid4

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.cli.run_fundamental_quarter_update import (
    execute_quarter_update_yahoo_aware_vintage_plan,
    run_final_mixed_vintage_execution_for_ticker,
    run_fundamental_quarter_update,
    run_sec_latest_writer_vintage_side_write,
    validate_vintage_options,
)
from swingmaster.cli import run_fundamental_ticker_snapshot, run_fundamental_valuation
from swingmaster.cli.run_fundamental_sec_reconstruct_quarterly import run_sec_reconstruct_quarterly
from swingmaster.cli.run_fundamental_yahoo_fallback_enrich import run_yahoo_fallback_enrich
from swingmaster.cli.run_fundamental_yahoo_to_quarterly import run_yahoo_to_quarterly
from swingmaster.fundamentals.build_ttm import build_and_insert_ttm_rows
from swingmaster.fundamentals import score, score_percentile
from swingmaster.fundamentals.reported_quarterly_dual_write import (
    write_normalized_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_sec_dual_write_adapter import (
    write_sec_reconstructed_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_vintage_policy import VINTAGE_DISABLED_STATUS
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)
from swingmaster.fundamentals.reported_yahoo_dual_write_adapter import (
    write_yahoo_fallback_enriched_rows_with_optional_vintage,
    write_yahoo_quarterly_rows_with_optional_vintage,
)
from ui_fundamental_pipeline.command_builder import UsaQuarterUpdateVintageOptions, build_usa_update_command
from ui_fundamental_pipeline.vintage_status import (
    should_apply_sec_vintage_recovery,
    should_apply_yahoo_aware_recovery,
    should_auto_apply_yahoo_aware_vintage,
    should_enable_yahoo_aware_apply,
    should_plan_sec_vintage_recovery,
)


def test_latest_adapter_noops_explicit_vintage_and_preserves_latest_write() -> None:
    db_path = _db_path("adapter_noop")
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        result = write_normalized_quarterly_rows_with_optional_vintage(
            conn,
            [_latest_row()],
            write_vintage=True,
            vintage_metadata_by_key={("AAPL", "2026-03-31"): _metadata()},
        )
        counts = _counts(conn)
        latest = conn.execute("SELECT ticker, period_end_date, revenue, run_id FROM rc_fundamental_quarterly").fetchone()

    assert result["latest_rows_written"] == 1
    assert result["vintage_rows_written"] == 0
    assert result["field_provenance_rows_written"] == 0
    assert result["vintage_status"] == VINTAGE_DISABLED_STATUS
    assert counts == {"vintage": 0, "field_provenance": 0}
    assert latest == ("AAPL", "2026-03-31", 100.0, "RUN1")


def test_sec_yahoo_and_fallback_adapters_do_not_build_provenance_when_disabled() -> None:
    db_path = _db_path("adapter_sources")
    run_migration(db_path)
    row = _latest_row()

    with sqlite3.connect(str(db_path)) as conn:
        sec_result = write_sec_reconstructed_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=[row],
            write_vintage=True,
        )
        yahoo_result = write_yahoo_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=[_latest_row(period_end_date="2026-06-30")],
            write_vintage=True,
        )
        fallback_result = write_yahoo_fallback_enriched_rows_with_optional_vintage(
            conn,
            normalized_rows=[_latest_row(period_end_date="2026-09-30")],
            write_vintage=True,
        )
        counts = _counts(conn)

    for result in (sec_result, yahoo_result, fallback_result):
        assert result["latest_rows_written"] == 1
        assert result["vintage_rows_written"] == 0
        assert result["field_provenance_rows_written"] == 0
        assert result["vintage_status"] == VINTAGE_DISABLED_STATUS
    assert counts == {"vintage": 0, "field_provenance": 0}


def test_direct_vintage_and_provenance_inserts_are_rejected() -> None:
    db_path = _db_path("direct_reject")
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(RuntimeError, match="VINTAGE_PROVENANCE_WRITES_DISABLED"):
            insert_quarterly_vintage_row(conn, _vintage_row())
        with pytest.raises(RuntimeError, match="VINTAGE_PROVENANCE_WRITES_DISABLED"):
            insert_quarterly_field_provenance_rows(conn, [_field_provenance_row()])
        assert _counts(conn) == {"vintage": 0, "field_provenance": 0}


def test_quarter_update_default_summary_disables_vintage_and_explicit_flag_rejects_before_db_use() -> None:
    db_path = _db_path("quarter_update")
    run_migration(db_path)

    summary = run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=None,
        run_id="RUN_Q",
        market="usa",
        ticker=None,
        limit=0,
        dry_run=True,
        skip_ack=True,
    )

    assert summary["vintage_execution_enabled"] is False
    assert summary["vintage_status"] == VINTAGE_DISABLED_STATUS
    assert summary["vintage_rows_inserted"] == 0
    assert summary["vintage_provenance_rows_inserted"] == 0

    with pytest.raises(RuntimeError, match="VINTAGE_PROVENANCE_WRITES_DISABLED"):
        validate_vintage_options(
            write_vintage=True,
            vintage_market="usa",
            vintage_available_at_utc="2026-07-31T00:00:00Z",
            vintage_ingested_at_utc="2026-07-31T00:00:00Z",
            vintage_run_id="VINTAGE_RUN",
            vintage_mode="sec_latest_writer",
        )


def test_standalone_cli_functions_reject_retired_vintage_flags_before_writes() -> None:
    db_path = _db_path("cli_reject")
    run_migration(db_path)

    with pytest.raises(RuntimeError, match="VINTAGE_PROVENANCE_WRITES_DISABLED"):
        run_sec_reconstruct_quarterly(db_path, "AAPL", "RUN", "2026-07-31T00:00:00Z", True, write_vintage=True)
    with pytest.raises(RuntimeError, match="VINTAGE_PROVENANCE_WRITES_DISABLED"):
        run_yahoo_to_quarterly(db_path, "usa", "AAPL", "RUN", True, False, write_vintage=True)
    with pytest.raises(RuntimeError, match="VINTAGE_PROVENANCE_WRITES_DISABLED"):
        run_yahoo_fallback_enrich(db_path, "usa", "AAPL", "RUN", True, False, write_vintage=True)

    with sqlite3.connect(str(db_path)) as conn:
        assert _counts(conn) == {"vintage": 0, "field_provenance": 0}


def test_direct_quarter_update_vintage_execution_helpers_reject_before_work() -> None:
    db_path = _db_path("quarter_update_direct_reject")
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(RuntimeError, match="VINTAGE_PROVENANCE_WRITES_DISABLED"):
            run_final_mixed_vintage_execution_for_ticker(
                conn,
                ticker="AAPL",
                market="usa",
                normalized_row={"ticker": "AAPL", "period_end_date": "2026-03-31"},
                sec_field_source_map={},
                yahoo_field_source_map={},
                fallback_audit_rows=[],
                available_at_utc="2026-07-31T00:00:00Z",
                ingested_at_utc="2026-07-31T00:00:00Z",
                run_id="RUN",
            )
        with pytest.raises(RuntimeError, match="VINTAGE_PROVENANCE_WRITES_DISABLED"):
            execute_quarter_update_yahoo_aware_vintage_plan(
                conn,
                plan={"vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY"},
                final_mixed_candidates_by_key={},
                yahoo_vintage_candidates_by_key={},
                market="usa",
                available_at_utc="2026-07-31T00:00:00Z",
                ingested_at_utc="2026-07-31T00:00:00Z",
                vintage_run_id="RUN",
            )
        assert _counts(conn) == {"vintage": 0, "field_provenance": 0}

    with pytest.raises(RuntimeError, match="VINTAGE_PROVENANCE_WRITES_DISABLED"):
        run_sec_latest_writer_vintage_side_write(
            db_path,
            ticker="AAPL",
            latest_run_id="LATEST",
            source_run_id="SOURCE",
            market="usa",
            available_at_utc="2026-07-31T00:00:00Z",
            ingested_at_utc="2026-07-31T00:00:00Z",
            vintage_run_id="RUN",
        )


def test_ui_and_scheduler_helpers_cannot_enable_vintage_writes() -> None:
    command = build_usa_update_command(
        "RUN",
        vintage_options=UsaQuarterUpdateVintageOptions(
            launch_timestamp_utc="2026-07-31T00:00:00Z",
            vintage_run_id="VINTAGE_RUN",
        ),
    )

    assert "--write-vintage" not in command
    assert should_enable_yahoo_aware_apply({"vintage_completion_status": "FINAL_MIXED_REQUIRED"})[0] is False
    assert should_auto_apply_yahoo_aware_vintage({}, user_enabled_vintage=True)[0] is False
    assert should_plan_sec_vintage_recovery({"overall_status": "PARITY_DRIFT"}) == (False, "RECOVERY_DISABLED")
    assert should_apply_sec_vintage_recovery(preflight_summary={}, dry_run_summary={"overall_status": "DRY_RUN_READY"})[0] is False
    assert should_apply_yahoo_aware_recovery(preflight_summary={}, plan_summary={"source_run_id": "RUN"})[0] is False


def test_ttm_consumer_reads_latest_quarterly_without_vintage_or_provenance() -> None:
    db_path = _db_path("ttm_latest")
    run_migration(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        for period, revenue in [
            ("2025-06-30", 100.0),
            ("2025-09-30", 110.0),
            ("2025-12-31", 120.0),
            ("2026-03-31", 130.0),
        ]:
            write_normalized_quarterly_rows_with_optional_vintage(
                conn,
                [_latest_row(period_end_date=period, revenue=revenue)],
                write_vintage=True,
                vintage_metadata_by_key={("AAPL", period): _metadata(period_end_date=period)},
            )
        build_and_insert_ttm_rows(conn=conn, ticker="AAPL", run_id="TTM_RUN", dry_run=False)
        ttm_revenue = conn.execute("SELECT revenue_ttm FROM rc_fundamental_ttm WHERE ticker='AAPL'").fetchone()[0]
        counts = _counts(conn)

    assert ttm_revenue == 460.0
    assert counts == {"vintage": 0, "field_provenance": 0}


def test_latest_state_consumers_do_not_require_vintage_or_provenance_tables() -> None:
    consumer_sources = {
        "score": inspect.getsource(score),
        "score_percentile": inspect.getsource(score_percentile),
        "valuation": inspect.getsource(run_fundamental_valuation),
        "snapshot": inspect.getsource(run_fundamental_ticker_snapshot),
    }

    assert "rc_fundamental_ttm" in consumer_sources["score"]
    assert "rc_fundamental_ttm" in consumer_sources["score_percentile"]
    assert "rc_fundamental_ttm" in consumer_sources["valuation"]
    assert "rc_fundamental_quarterly" in consumer_sources["valuation"]
    assert "rc_fundamental_ttm" in consumer_sources["snapshot"]
    assert "rc_fundamental_quarterly" in consumer_sources["snapshot"]

    for source in consumer_sources.values():
        assert "rc_fundamental_quarterly_vintage" not in source
        assert "rc_fundamental_quarterly_field_provenance" not in source


def test_temp_runtime_path_policy_for_deactivation_artifacts() -> None:
    path = _db_path("runtime_policy")
    root = Path.cwd() / "temp" / "vintage_provenance_deactivation"

    assert path.resolve().is_relative_to(root.resolve())


def _db_path(label: str) -> Path:
    root = Path.cwd() / "temp" / "vintage_provenance_deactivation" / "tests"
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{label}_{uuid4().hex}.db"


def _counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "vintage": int(conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]),
        "field_provenance": int(
            conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
        ),
    }


def _latest_row(period_end_date: str = "2026-03-31", revenue: float = 100.0) -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "period_end_date": period_end_date,
        "revenue": revenue,
        "gross_profit": 50.0,
        "operating_income": 20.0,
        "ebit": 20.0,
        "ebitda": 25.0,
        "net_income": 15.0,
        "operating_cashflow": 18.0,
        "capex": -4.0,
        "free_cashflow": 14.0,
        "cash": 80.0,
        "total_debt": 10.0,
        "shares_outstanding": 1000.0,
        "currency": "USD",
        "run_id": "RUN1",
    }


def _metadata(period_end_date: str = "2026-03-31") -> dict[str, object]:
    return {
        "market": "usa",
        "statement_vintage_id": f"AAPL_{period_end_date}_V1",
        "source_provider": "sec_edgar",
        "source_hash": f"hash_{period_end_date}",
        "revision_number": 1,
        "is_restated": 0,
        "availability_quality": "PROVIDER_FILED_OR_OBSERVED",
        "available_at_utc": "2026-07-31T00:00:00Z",
        "ingested_at_utc": "2026-07-31T00:00:00Z",
        "run_id": "VINTAGE_RUN",
        "created_at_utc": "2026-07-31T00:00:00Z",
    }


def _vintage_row() -> dict[str, object]:
    row = dict(_latest_row())
    row.update(_metadata())
    return row


def _field_provenance_row() -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "market": "usa",
        "period_end_date": "2026-03-31",
        "statement_vintage_id": "AAPL_2026-03-31_V1",
        "field_name": "revenue",
        "field_value": 100.0,
        "source_provider": "sec_edgar",
        "provenance_role": "PRIMARY_REPORTED",
        "merge_action": "SEC_RETAINED",
        "available_at_utc": "2026-07-31T00:00:00Z",
        "created_at_utc": "2026-07-31T00:00:00Z",
    }
