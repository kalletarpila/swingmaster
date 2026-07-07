from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


SOURCE_RUN_ID = "BASE__QUARTERLY"
ENRICH_RUN_ID = "BASE__ENRICH"
VINTAGE_RUN_ID = "BASE__SEC_LATEST_WRITER_VINTAGE"


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _insert_latest(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    period_end_date: str = "2026-03-31",
    total_debt: float = 200.0,
    run_id: str = SOURCE_RUN_ID,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            total_debt,
            currency,
            run_id
        ) VALUES (?, ?, 10.0, ?, 'USD', ?)
        """,
        (ticker, period_end_date, total_debt, run_id),
    )


def _insert_vintage(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    period_end_date: str = "2026-03-31",
    total_debt: float = 100.0,
    statement_vintage_id: str = "vintage:AAPL:2026-03-31",
    run_id: str = VINTAGE_RUN_ID,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_vintage (
            ticker,
            market,
            period_end_date,
            statement_vintage_id,
            source_provider,
            source_hash,
            available_at_utc,
            ingested_at_utc,
            run_id,
            revenue,
            total_debt,
            currency,
            created_at_utc
        ) VALUES (?, 'usa', ?, ?, 'sec_edgar', 'hash', '2026-05-05T12:00:00Z', '2026-05-05T12:05:00Z', ?, 10.0, ?, 'USD', '2026-05-05T12:05:00Z')
        """,
        (ticker, period_end_date, statement_vintage_id, run_id, total_debt),
    )


def _insert_sec_provenance(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    period_end_date: str = "2026-03-31",
    field_name: str = "revenue",
    statement_vintage_id: str = "vintage:AAPL:2026-03-31",
    run_id: str = VINTAGE_RUN_ID,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_field_provenance (
            ticker,
            market,
            period_end_date,
            statement_vintage_id,
            field_name,
            field_value,
            source_provider,
            source_table,
            source_row_ref,
            source_hash,
            provenance_role,
            merge_action,
            available_at_utc,
            created_at_utc,
            run_id
        ) VALUES (?, 'usa', ?, ?, ?, 10.0, 'sec_edgar', 'rc_fundamental_statement_raw', ?, 'sec_hash', 'PRIMARY_REPORTED', 'SEC_RETAINED', '2026-05-05T12:00:00Z', '2026-05-05T12:05:00Z', ?)
        """,
        (ticker, period_end_date, statement_vintage_id, field_name, field_name, run_id),
    )


def _insert_yahoo_audit(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    period_end_date: str = "2026-03-31",
    field_name: str = "total_debt",
    run_id: str = ENRICH_RUN_ID,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_enrichment_audit (
            ticker,
            period_end_date,
            field_name,
            old_value,
            new_value,
            primary_source,
            fallback_source,
            enrichment_status,
            matched_yahoo_period_end_date,
            match_method,
            run_id,
            created_at_utc
        ) VALUES (?, ?, ?, NULL, 200.0, 'sec_edgar', 'yahoo', 'FILLED_FROM_YAHOO', ?, 'EXACT', ?, '2026-05-05T12:00:00Z')
        """,
        (ticker, period_end_date, field_name, period_end_date, run_id),
    )


def _summary(conn: sqlite3.Connection) -> dict[str, object]:
    return run_fundamental_quarter_update.build_quarter_update_vintage_post_run_guard_summary(
        conn,
        market="usa",
        source_run_id=SOURCE_RUN_ID,
        vintage_run_id=VINTAGE_RUN_ID,
        enrich_run_id=ENRICH_RUN_ID,
        enrich_summary=None,
        available_at_utc="2026-05-05T12:00:00Z",
        ingested_at_utc="2026-05-05T12:05:00Z",
    )


def test_exact_current_run_yahoo_audit_explains_mismatch_and_scopes_plan(tmp_path: Path) -> None:
    db_path = tmp_path / "exact_scope.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        _insert_sec_provenance(conn)
        _insert_yahoo_audit(conn)
        conn.commit()

        summary = _summary(conn)

    assert summary["vintage_post_run_value_mismatch_count"] == 1
    assert summary["vintage_post_run_yahoo_explained_mismatch_count"] == 1
    assert summary["vintage_post_run_unexplained_mismatch_count"] == 0
    assert summary["vintage_completion_status"] == "FINAL_MIXED_REQUIRED"
    assert summary["vintage_completion_reason"] == "value_mismatch_exactly_explained_by_yahoo_audit"
    assert summary["vintage_yahoo_aware_planning_status"] == "FINAL_MIXED_PLAN_READY"
    assert summary["vintage_yahoo_aware_planner_scope_count"] == 1
    assert summary["vintage_yahoo_aware_planner_scope_source"] == "post_run_yahoo_explained_mismatches"
    assert summary["vintage_planned_final_mixed_rows"] == 1


def test_same_period_different_field_audit_does_not_explain_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "different_field.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        _insert_sec_provenance(conn)
        _insert_yahoo_audit(conn, field_name="cash")
        conn.commit()

        summary = _summary(conn)

    assert summary["vintage_post_run_yahoo_explained_mismatch_count"] == 0
    assert summary["vintage_post_run_unexplained_mismatch_count"] == 1
    assert summary["vintage_completion_status"] == "BLOCKED_POST_RUN_DRIFT"
    assert summary["vintage_completion_reason"] == "unexplained_value_mismatch"
    assert summary["vintage_yahoo_aware_planning_status"] == "PLAN_BLOCKED"
    assert summary["vintage_yahoo_aware_planner_scope_count"] == 0
    assert summary["vintage_yahoo_aware_unknown_provenance_fields"] == ""


def test_same_field_older_run_audit_does_not_explain_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "older_run.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        _insert_sec_provenance(conn)
        _insert_yahoo_audit(conn, run_id="OLDER__ENRICH")
        conn.commit()

        summary = _summary(conn)

    assert summary["vintage_yahoo_audit_rows_detected"] == 0
    assert summary["vintage_post_run_yahoo_explained_mismatch_count"] == 0
    assert summary["vintage_completion_status"] == "BLOCKED_POST_RUN_DRIFT"
    assert summary["vintage_yahoo_aware_planner_scope_count"] == 0


def test_no_yahoo_audit_does_not_explain_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "no_audit.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        _insert_sec_provenance(conn)
        conn.commit()

        summary = _summary(conn)

    assert summary["vintage_post_run_unexplained_mismatch_count"] == 1
    assert summary["vintage_completion_status"] == "BLOCKED_POST_RUN_DRIFT"
    assert summary["vintage_yahoo_aware_planning_status"] == "PLAN_BLOCKED"


def test_historical_yahoo_audit_rows_are_not_planned_when_current_mismatch_unexplained(tmp_path: Path) -> None:
    db_path = tmp_path / "historical_not_planned.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        _insert_sec_provenance(conn)
        for index in range(30):
            ticker = f"H{index:02d}"
            _insert_latest(conn, ticker=ticker, total_debt=200.0)
            _insert_yahoo_audit(conn, ticker=ticker, field_name="cash")
        conn.commit()

        summary = _summary(conn)

    assert summary["vintage_yahoo_audit_rows_detected"] == 30
    assert summary["vintage_post_run_value_mismatch_count"] == 1
    assert summary["vintage_completion_status"] == "BLOCKED_POST_RUN_DRIFT"
    assert summary["vintage_yahoo_aware_planner_scope_count"] == 0
    assert summary["vintage_yahoo_aware_unknown_provenance_fields"] == ""
