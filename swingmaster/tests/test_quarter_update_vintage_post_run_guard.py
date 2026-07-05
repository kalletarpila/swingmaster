from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration


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


def _insert_latest(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    period_end_date: str = "2026-03-31",
    run_id: str = "BASE__QUARTERLY",
    revenue: float = 100.0,
    operating_income: float = 20.0,
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            operating_income,
            ebit,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            currency,
            run_id
        ) VALUES (?, ?, ?, ?, ?, 30, -5, 25, 40, 20, 'USD', ?)
        """,
        (ticker, period_end_date, revenue, operating_income, operating_income, run_id),
    )


def _insert_vintage(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    period_end_date: str = "2026-03-31",
    market: str = "usa",
    statement_vintage_id: str = "vintage:AAPL:2026-03-31",
    run_id: str = "VINTAGE_RUN",
    revenue: float = 100.0,
    operating_income: float = 20.0,
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
            operating_income,
            ebit,
            operating_cashflow,
            capex,
            free_cashflow,
            cash,
            total_debt,
            currency,
            created_at_utc
        ) VALUES (?, ?, ?, ?, 'sec_edgar', 'hash', '2026-05-05T12:00:00Z', '2026-05-05T12:05:00Z', ?, ?, ?, ?, 30, -5, 25, 40, 20, 'USD', '2026-05-05T12:05:00Z')
        """,
        (ticker, market, period_end_date, statement_vintage_id, run_id, revenue, operating_income, operating_income),
    )


def _parity(conn: sqlite3.Connection) -> dict[str, object]:
    return run_fundamental_quarter_update.check_quarter_update_vintage_parity_for_run(
        conn,
        market="usa",
        source_run_id="BASE__QUARTERLY",
        vintage_run_id="VINTAGE_RUN",
    )


def test_post_run_parity_ok_returns_ok(tmp_path: Path) -> None:
    db_path = tmp_path / "guard_ok.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        summary = _parity(conn)

    assert summary["vintage_post_run_parity_status"] == "OK"
    assert summary["vintage_post_run_latest_without_vintage_count"] == 0
    assert summary["vintage_post_run_value_mismatch_count"] == 0


def test_post_run_parity_detects_latest_without_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "guard_latest_without_vintage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn)
        summary = _parity(conn)

    assert summary["vintage_post_run_parity_status"] == "DRIFT"
    assert summary["vintage_post_run_latest_without_vintage_count"] == 1


def test_post_run_parity_detects_additional_latest_run_without_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "guard_additional_latest_without_vintage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        _insert_latest(conn, ticker="MSFT", run_id="BASE__ENRICH")
        summary = run_fundamental_quarter_update.check_quarter_update_vintage_parity_for_run(
            conn,
            market="usa",
            source_run_id="BASE__QUARTERLY",
            vintage_run_id="VINTAGE_RUN",
            additional_latest_run_ids=["BASE__ENRICH"],
        )

    assert summary["vintage_post_run_parity_status"] == "DRIFT"
    assert summary["vintage_post_run_latest_without_vintage_count"] == 1


def test_post_run_parity_detects_vintage_without_latest_when_vintage_run_linked(tmp_path: Path) -> None:
    db_path = tmp_path / "guard_vintage_without_latest.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        _insert_vintage(
            conn,
            ticker="MSFT",
            period_end_date="2026-03-31",
            statement_vintage_id="vintage:MSFT:2026-03-31",
        )
        summary = _parity(conn)

    assert summary["vintage_post_run_parity_status"] == "DRIFT"
    assert summary["vintage_post_run_vintage_without_latest_count"] == 1


def test_post_run_parity_detects_value_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "guard_value_mismatch.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn, revenue=101.0)
        _insert_vintage(conn, revenue=100.0)
        summary = _parity(conn)

    assert summary["vintage_post_run_parity_status"] == "DRIFT"
    assert summary["vintage_post_run_value_mismatch_count"] == 1


def test_post_run_parity_detects_duplicate_statement_vintage_ids_if_present(tmp_path: Path) -> None:
    db_path = tmp_path / "guard_duplicate_statement_id.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn)
        _insert_vintage(conn, statement_vintage_id="duplicate:vintage")
        _insert_latest(conn, ticker="MSFT", period_end_date="2026-06-30")
        _insert_vintage(conn, ticker="MSFT", period_end_date="2026-06-30", statement_vintage_id="duplicate:vintage")
        summary = _parity(conn)

    assert summary["vintage_post_run_parity_status"] == "DRIFT"
    assert summary["vintage_post_run_duplicate_statement_vintage_id_count"] == 1


def test_yahoo_enrichment_audit_presence_is_detected(tmp_path: Path) -> None:
    db_path = tmp_path / "guard_yahoo_audit.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
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
            ) VALUES ('AAPL', '2026-03-31', 'revenue', NULL, 100, 'sec_edgar', 'yahoo', 'FILLED_FROM_YAHOO', '2026-03-31', 'EXACT', 'BASE__ENRICH', '2026-05-05T12:00:00Z')
            """
        )
        summary = run_fundamental_quarter_update.detect_yahoo_quarter_update_impact_for_run(
            conn,
            enrich_run_id="BASE__ENRICH",
            enrich_summary={"rows_inserted": 0, "rows_updated": 1},
        )

    assert summary["vintage_yahoo_impact_status"] == "YAHOO_IMPACT_DETECTED"
    assert summary["vintage_yahoo_audit_rows_detected"] == 1
    assert summary["vintage_yahoo_filled_field_rows_detected"] == 1
    assert summary["vintage_yahoo_can_create_post_sec_vintage_drift"] is True


def test_missing_run_linkage_returns_unknown_without_guessing(tmp_path: Path) -> None:
    db_path = tmp_path / "guard_unknown_linkage.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        parity = run_fundamental_quarter_update.check_quarter_update_vintage_parity_for_run(
            conn,
            market="usa",
            source_run_id=None,
        )
        yahoo = run_fundamental_quarter_update.detect_yahoo_quarter_update_impact_for_run(
            conn,
            enrich_run_id=None,
        )

    assert parity["vintage_post_run_parity_status"] == "UNKNOWN_RUN_LINKAGE"
    assert yahoo["vintage_yahoo_impact_status"] == "UNKNOWN_RUN_LINKAGE"


def test_quarter_update_sec_latest_writer_surfaces_guard_summary_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "guard_quarter_update_summary.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    def _fake_process_ticker(**_kwargs: object) -> dict[str, object]:
        return {
            "sec_latest_writer_vintage_summary": {
                "vintage_rows_inserted": 1,
                "provenance_rows_inserted": 7,
                "skipped_already_had_vintage": 0,
                "blocked_rows": 0,
            },
            "vintage_post_run_guard_summary": {
                "vintage_post_run_parity_status": "DRIFT",
                "vintage_post_run_latest_without_vintage_count": 1,
                "vintage_post_run_vintage_without_latest_count": 0,
                "vintage_post_run_value_mismatch_count": 0,
                "vintage_post_run_duplicate_statement_vintage_id_count": 0,
                "vintage_yahoo_impact_status": "YAHOO_IMPACT_DETECTED",
                "vintage_yahoo_fallback_rows_detected": 1,
                "vintage_yahoo_inserted_missing_quarter_rows_detected": 0,
                "vintage_yahoo_filled_field_rows_detected": 1,
                "vintage_yahoo_audit_rows_detected": 1,
                "vintage_yahoo_can_create_post_sec_vintage_drift": True,
                "vintage_recommendation": "phase_4k3_final_mixed_or_post_run_parity_apply",
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

    assert summary["vintage_post_run_parity_status"] == "DRIFT"
    assert summary["vintage_yahoo_impact_status"] == "YAHOO_IMPACT_DETECTED"
    assert summary["vintage_recommendation"] == "phase_4k3_final_mixed_or_post_run_parity_apply"


def test_default_without_vintage_flags_omits_guard_summary_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "guard_default_no_vintage_fields.db"
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

    assert "vintage_post_run_parity_status" not in summary
    assert "vintage_yahoo_impact_status" not in summary
