from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.cli.diagnose_quarter_update_vintage_scope import _exit_code, main, run_diagnostic
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
    ticker: str = "GIS",
    period_end_date: str = "2025-05-25",
    revenue: float = 10.0,
    total_debt: float = 14878600000.0,
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
        ) VALUES (?, ?, ?, ?, 'USD', ?)
        """,
        (ticker, period_end_date, revenue, total_debt, run_id),
    )


def _insert_vintage(
    conn: sqlite3.Connection,
    *,
    ticker: str = "GIS",
    period_end_date: str = "2025-05-25",
    revenue: float = 10.0,
    total_debt: float = 677000000.0,
    statement_vintage_id: str = "legacy:usa:GIS:2025-05-25:9441b8313c7894e3",
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
        ) VALUES (?, 'usa', ?, ?, 'sec_edgar', 'hash', '2026-05-05T12:00:00Z', '2026-05-05T12:05:00Z', ?, ?, ?, 'USD', '2026-05-05T12:05:00Z')
        """,
        (ticker, period_end_date, statement_vintage_id, run_id, revenue, total_debt),
    )


def _insert_sec_provenance(
    conn: sqlite3.Connection,
    *,
    ticker: str = "GIS",
    period_end_date: str = "2025-05-25",
    field_name: str = "revenue",
    statement_vintage_id: str = "legacy:usa:GIS:2025-05-25:9441b8313c7894e3",
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
    ticker: str = "GIS",
    period_end_date: str = "2025-05-25",
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
        ) VALUES (?, ?, ?, NULL, 14878600000.0, 'sec_edgar', 'yahoo', 'FILLED_FROM_YAHOO', ?, 'EXACT', ?, '2026-05-05T12:00:00Z')
        """,
        (ticker, period_end_date, field_name, period_end_date, run_id),
    )


def _diagnose(db_path: Path, *, source_run_id: str = SOURCE_RUN_ID, sample_limit: int = 20) -> dict[str, object]:
    return run_diagnostic(
        fundamentals_db=db_path,
        market="usa",
        source_run_id=source_run_id,
        enrich_run_id=ENRICH_RUN_ID,
        sample_limit=sample_limit,
    )


def _base_db(db_path: Path) -> None:
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        _insert_sec_provenance(conn)
        conn.commit()


def test_exact_yahoo_explained_mismatch_has_scope_one(tmp_path: Path) -> None:
    db_path = tmp_path / "exact.db"
    _base_db(db_path)
    with _connect(db_path) as conn:
        _insert_yahoo_audit(conn)
        conn.commit()

    summary = _diagnose(db_path)

    assert summary["value_mismatch_count"] == 1
    assert summary["yahoo_explained_mismatch_count"] == 1
    assert summary["unexplained_mismatch_count"] == 0
    assert summary["completion_status"] == "FINAL_MIXED_REQUIRED"
    assert summary["planner_scope_count"] == 1


def test_gis_like_unexplained_mismatch_is_blocked_narrow(tmp_path: Path) -> None:
    db_path = tmp_path / "gis_unexplained.db"
    _base_db(db_path)

    summary = _diagnose(db_path)

    assert summary["value_mismatch_count"] == 1
    assert summary["yahoo_explained_mismatch_count"] == 0
    assert summary["unexplained_mismatch_count"] == 1
    assert "GIS:2025-05-25:total_debt" in str(summary["value_mismatch_sample"])
    assert summary["completion_status"] == "BLOCKED_POST_RUN_DRIFT"
    assert summary["completion_reason"] == "unexplained_value_mismatch"
    assert summary["planner_scope_count"] == 0
    assert summary["planner_blocked_rows"] == 0
    assert summary["overall_diagnostic_status"] == "SCOPE_FIX_VERIFIED_BLOCKED_NARROW"


def test_many_historical_audit_rows_do_not_expand_scope(tmp_path: Path) -> None:
    db_path = tmp_path / "historical.db"
    _base_db(db_path)
    with _connect(db_path) as conn:
        for index in range(50):
            ticker = f"H{index:02d}"
            _insert_latest(conn, ticker=ticker, period_end_date="2025-03-31", total_debt=100.0)
            _insert_vintage(
                conn,
                ticker=ticker,
                period_end_date="2025-03-31",
                total_debt=100.0,
                statement_vintage_id=f"vintage:{ticker}:2025-03-31",
            )
            _insert_yahoo_audit(conn, ticker=ticker, period_end_date="2025-03-31", field_name="cash")
        conn.commit()

    summary = _diagnose(db_path, sample_limit=5)

    assert summary["planner_scope_count"] == 0
    assert summary["planner_blocked_rows"] == 0
    assert summary["unknown_provenance_field_sample"] == ""


def test_latest_without_vintage_returns_parity_drift(tmp_path: Path) -> None:
    db_path = tmp_path / "latest_without_vintage.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn)
        conn.commit()

    summary = _diagnose(db_path)

    assert summary["latest_without_vintage_count"] == 1
    assert summary["overall_diagnostic_status"] == "PARITY_DRIFT"


def test_duplicate_statement_id_returns_parity_drift(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicate.db"
    _base_db(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn, ticker="MSFT", period_end_date="2026-03-31", total_debt=50.0)
        _insert_vintage(
            conn,
            ticker="MSFT",
            period_end_date="2026-03-31",
            total_debt=50.0,
            statement_vintage_id="legacy:usa:GIS:2025-05-25:9441b8313c7894e3",
        )
        conn.commit()

    summary = _diagnose(db_path)

    assert summary["duplicate_statement_vintage_id_count"] == 1
    assert summary["overall_diagnostic_status"] == "PARITY_DRIFT"


def test_no_mismatch_main_returns_zero(tmp_path: Path) -> None:
    db_path = tmp_path / "no_mismatch.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn, total_debt=14878600000.0)
        _insert_vintage(conn, total_debt=14878600000.0)
        _insert_sec_provenance(conn, field_name="total_debt")
        conn.commit()

    rc = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--source-run-id",
            SOURCE_RUN_ID,
            "--enrich-run-id",
            ENRICH_RUN_ID,
            "--format",
            "json",
        ]
    )

    assert rc == 0
    assert _diagnose(db_path)["overall_diagnostic_status"] == "NO_MISMATCH"


def test_post_apply_like_no_mismatch_summary_returns_zero(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "post_apply_like.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn, total_debt=14878600000.0)
        _insert_vintage(
            conn,
            total_debt=14878600000.0,
            statement_vintage_id="sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0",
            run_id="USA_QUARTER_UPDATE_2026-07-07__GIS_TOTAL_DEBT_PROVIDER_VINTAGE_APPLY",
        )
        _insert_sec_provenance(
            conn,
            field_name="total_debt",
            statement_vintage_id="sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0",
            run_id="USA_QUARTER_UPDATE_2026-07-07__GIS_TOTAL_DEBT_PROVIDER_VINTAGE_APPLY",
        )
        conn.commit()

    rc = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--source-run-id",
            SOURCE_RUN_ID,
            "--enrich-run-id",
            ENRICH_RUN_ID,
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["summary"]["overall_diagnostic_status"] == "NO_MISMATCH"
    assert payload["summary"]["value_mismatch_count"] == 0


def test_parity_drift_main_returns_nonzero(tmp_path: Path) -> None:
    db_path = tmp_path / "parity_exit.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn)
        conn.commit()

    rc = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--source-run-id",
            SOURCE_RUN_ID,
            "--enrich-run-id",
            ENRICH_RUN_ID,
            "--format",
            "json",
        ]
    )

    assert rc == 1


def test_duplicate_statement_id_main_returns_nonzero(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicate_exit.db"
    _base_db(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn, ticker="MSFT", period_end_date="2026-03-31", total_debt=50.0)
        _insert_vintage(
            conn,
            ticker="MSFT",
            period_end_date="2026-03-31",
            total_debt=50.0,
            statement_vintage_id="legacy:usa:GIS:2025-05-25:9441b8313c7894e3",
        )
        conn.commit()

    rc = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--source-run-id",
            SOURCE_RUN_ID,
            "--enrich-run-id",
            ENRICH_RUN_ID,
            "--format",
            "json",
        ]
    )

    assert rc == 1


def test_scope_fix_failed_still_broad_returns_nonzero() -> None:
    assert _exit_code({"overall_diagnostic_status": "SCOPE_FIX_FAILED_STILL_BROAD"}) == 1


def test_blocked_narrow_main_returns_zero_as_successful_diagnostic_verification(tmp_path: Path) -> None:
    db_path = tmp_path / "blocked_narrow_exit.db"
    _base_db(db_path)

    rc = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--source-run-id",
            SOURCE_RUN_ID,
            "--enrich-run-id",
            ENRICH_RUN_ID,
            "--format",
            "json",
        ]
    )

    assert rc == 0
    assert _diagnose(db_path)["overall_diagnostic_status"] == "SCOPE_FIX_VERIFIED_BLOCKED_NARROW"


def test_json_output_includes_bounded_samples(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "json.db"
    _base_db(db_path)

    rc = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--source-run-id",
            SOURCE_RUN_ID,
            "--enrich-run-id",
            ENRICH_RUN_ID,
            "--format",
            "json",
            "--sample-limit",
            "1",
        ]
    )

    assert rc == 0
    output = json.loads(capsys.readouterr().out)
    assert output["summary"]["unknown_provenance_field_sample"] == ""
    assert "GIS:2025-05-25:total_debt" in output["summary"]["value_mismatch_sample"]


def test_source_run_id_with_missing_suffix_is_resolved_read_only(tmp_path: Path) -> None:
    db_path = tmp_path / "resolved.db"
    _base_db(db_path)

    summary = _diagnose(db_path, source_run_id="BASE")

    assert summary["source_run_id_used"] == SOURCE_RUN_ID
    assert summary["source_run_id_resolution"] == "appended_quarterly_suffix"
    assert summary["value_mismatch_count"] == 1


def test_diagnostic_does_not_write_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "readonly.db"
    _base_db(db_path)
    with _connect(db_path) as conn:
        before = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]

    _diagnose(db_path)

    with _connect(db_path) as conn:
        after = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
    assert after == before


def test_invalid_db_path_exits_nonzero(tmp_path: Path, capsys) -> None:
    rc = main(
        [
            "--fundamentals-db",
            str(tmp_path / "missing.db"),
            "--market",
            "usa",
            "--source-run-id",
            SOURCE_RUN_ID,
            "--enrich-run-id",
            ENRICH_RUN_ID,
            "--format",
            "json",
        ]
    )

    assert rc == 2
    assert "ERROR:" in capsys.readouterr().err
