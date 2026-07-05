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


def _insert_sec_fact(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    statement_type: str,
    field_name: str,
    field_value: float,
    run_id: str = "BASE__SEC_RAW",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_statement_raw (
            ticker,
            statement_type,
            period_end_date,
            period_type,
            field_name,
            field_value,
            currency,
            source,
            retrieved_at_utc,
            run_id
        ) VALUES (?, ?, '2026-03-31', 'sec_fact', ?, ?, 'USD', 'sec_edgar', ?, ?)
        """,
        (ticker, statement_type, field_name, field_value, "2026-05-05T10:00:00Z", run_id),
    )


def _insert_sec_facts(db_path: Path, *, ticker: str = "AAPL", run_id: str = "BASE__SEC_RAW") -> None:
    with sqlite3.connect(str(db_path)) as conn:
        _insert_sec_fact(
            conn,
            ticker=ticker,
            statement_type="income",
            field_name="RevenueFromContractWithCustomerExcludingAssessedTax|form=10-Q|unit=USD|fy=2026|fp=Q2",
            field_value=100.0,
            run_id=run_id,
        )
        _insert_sec_fact(
            conn,
            ticker=ticker,
            statement_type="income",
            field_name="OperatingIncomeLoss|form=10-Q|unit=USD|fy=2026|fp=Q2",
            field_value=20.0,
            run_id=run_id,
        )
        _insert_sec_fact(
            conn,
            ticker=ticker,
            statement_type="cashflow",
            field_name="NetCashProvidedByUsedInOperatingActivities|form=10-Q|unit=USD|fy=2026|fp=Q2",
            field_value=30.0,
            run_id=run_id,
        )
        _insert_sec_fact(
            conn,
            ticker=ticker,
            statement_type="cashflow",
            field_name="PaymentsToAcquirePropertyPlantAndEquipment|form=10-Q|unit=USD|fy=2026|fp=Q2",
            field_value=-5.0,
            run_id=run_id,
        )
        _insert_sec_fact(
            conn,
            ticker=ticker,
            statement_type="balance",
            field_name="CashAndCashEquivalentsAtCarryingValue|form=10-Q|unit=USD|fy=2026|fp=Q2",
            field_value=40.0,
            run_id=run_id,
        )
        _insert_sec_fact(
            conn,
            ticker=ticker,
            statement_type="balance",
            field_name="LongTermDebtCurrent|form=10-Q|unit=USD|fy=2026|fp=Q2",
            field_value=7.0,
            run_id=run_id,
        )
        _insert_sec_fact(
            conn,
            ticker=ticker,
            statement_type="balance",
            field_name="LongTermDebtNoncurrent|form=10-Q|unit=USD|fy=2026|fp=Q2",
            field_value=13.0,
            run_id=run_id,
        )
        conn.commit()


def _mock_sec_refresh(monkeypatch: pytest.MonkeyPatch, db_path: Path) -> None:
    def _fake_sec_raw_bootstrap(**kwargs: object) -> tuple[str, list[dict[str, object]]]:
        _insert_sec_facts(db_path, run_id=str(kwargs["run_id"]))
        return "0000320193", [{"ticker": "AAPL"}]

    monkeypatch.setattr(run_fundamental_quarter_update, "run_sec_raw_bootstrap", _fake_sec_raw_bootstrap)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_yahoo_fallback_enrich",
        lambda **_kwargs: {"fields_filled": 0},
    )


def _vintage_options() -> dict[str, object]:
    return {
        "market": "usa",
        "available_at_utc": "2026-05-05T12:00:00Z",
        "ingested_at_utc": "2026-05-05T12:05:00Z",
        "vintage_run_id": "VINTAGE_RUN",
    }


def test_run_quarterly_refresh_default_off_writes_latest_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_sec_latest_default_off.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _mock_sec_refresh(monkeypatch, db_path)

    summary = run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="AAPL",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
    )

    assert summary["sec_refresh_summary"]["sec_latest_writer_vintage_summary"] is None
    with sqlite3.connect(str(db_path)) as conn:
        latest_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
    assert latest_count == 1
    assert vintage_count == 0


def test_run_quarterly_refresh_sec_latest_writer_opt_in_writes_vintage_from_latest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_sec_latest_writer.db"
    run_migration(db_path)
    _insert_state_row(db_path)
    _mock_sec_refresh(monkeypatch, db_path)

    summary = run_fundamental_quarter_update.run_quarterly_refresh(
        db_path=db_path,
        ticker="AAPL",
        market="usa",
        child_run_ids=run_fundamental_quarter_update.derive_child_run_ids("BASE"),
        sec_latest_writer_vintage_options=_vintage_options(),
    )

    vintage_summary = summary["sec_refresh_summary"]["sec_latest_writer_vintage_summary"]
    assert vintage_summary["vintage_rows_inserted"] == 1
    assert vintage_summary["provenance_rows_inserted"] >= 6
    with sqlite3.connect(str(db_path)) as conn:
        vintage_row = conn.execute(
            """
            SELECT
                market,
                run_id,
                provider_run_id,
                revenue,
                operating_income,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt,
                source_provider
            FROM rc_fundamental_quarterly_vintage
            WHERE ticker='AAPL' AND period_end_date='2026-03-31'
            """
        ).fetchone()
        unknown_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_fundamental_quarterly_field_provenance
            WHERE source_provider='unknown'
            """
        ).fetchone()[0]

    assert vintage_row == ("usa", "VINTAGE_RUN", "BASE__SEC_RAW", 100.0, 20.0, 30.0, -5.0, 25.0, 40.0, 20.0, "sec_edgar")
    assert unknown_count == 0


def test_sec_latest_writer_side_write_skips_existing_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_sec_latest_writer_noop.db"
    run_migration(db_path)
    _insert_sec_facts(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                revenue,
                operating_income,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt,
                run_id
            ) VALUES ('AAPL', '2026-03-31', 100, 20, 30, -5, 25, 40, 20, 'BASE__QUARTERLY')
            """
        )
        conn.commit()

    first = run_fundamental_quarter_update.run_sec_latest_writer_vintage_side_write(
        db_path,
        ticker="AAPL",
        latest_run_id="BASE__QUARTERLY",
        source_run_id="BASE__SEC_RAW",
        **_vintage_options(),
    )
    second = run_fundamental_quarter_update.run_sec_latest_writer_vintage_side_write(
        db_path,
        ticker="AAPL",
        latest_run_id="BASE__QUARTERLY",
        source_run_id="BASE__SEC_RAW",
        **_vintage_options(),
    )

    assert first["vintage_rows_inserted"] == 1
    assert second["vintage_rows_inserted"] == 0
    assert second["skipped_already_had_vintage"] == 1


def test_sec_latest_writer_side_write_blocks_unknown_provenance_without_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "quarter_update_sec_latest_writer_unknown.db"
    run_migration(db_path)
    _insert_sec_facts(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                revenue,
                operating_income,
                run_id
            ) VALUES ('AAPL', '2026-03-31', 101, 20, 'BASE__QUARTERLY')
            """
        )
        conn.commit()

    with pytest.raises(
        RuntimeError,
        match="FUNDAMENTAL_QUARTER_UPDATE_SEC_LATEST_WRITER_UNKNOWN_PROVENANCE:AAPL,2026-03-31:revenue",
    ):
        run_fundamental_quarter_update.run_sec_latest_writer_vintage_side_write(
            db_path,
            ticker="AAPL",
            latest_run_id="BASE__QUARTERLY",
            source_run_id="BASE__SEC_RAW",
            **_vintage_options(),
        )

    with sqlite3.connect(str(db_path)) as conn:
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
    assert vintage_count == 0
    assert provenance_count == 0


def test_sec_latest_writer_mode_requires_explicit_write_vintage() -> None:
    with pytest.raises(
        RuntimeError,
        match="FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_WRITE_REQUIRED_FOR_MODE:sec_latest_writer",
    ):
        run_fundamental_quarter_update.validate_vintage_options(
            write_vintage=False,
            vintage_market=None,
            vintage_available_at_utc=None,
            vintage_ingested_at_utc=None,
            vintage_run_id=None,
            vintage_mode="sec_latest_writer",
        )


def test_quarter_update_summary_merges_sec_latest_writer_counts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarter_update_sec_latest_writer_summary.db"
    run_migration(db_path)
    _insert_state_row(db_path)

    def _fake_process_ticker(**_kwargs: object) -> dict[str, object]:
        return {
            "sec_latest_writer_vintage_summary": {
                "vintage_rows_inserted": 1,
                "provenance_rows_inserted": 7,
                "skipped_already_had_vintage": 0,
                "blocked_rows": 0,
            }
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _fake_process_ticker)
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "resolve_latest_close_as_of_date",
        lambda *_args, **_kwargs: "2026-05-05",
    )
    monkeypatch.setattr(
        run_fundamental_quarter_update,
        "run_fundamental_valuation",
        lambda **_kwargs: {"rows_written": 0},
    )

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

    assert summary["vintage_rows_inserted"] == 1
    assert summary["vintage_provenance_rows_inserted"] == 7
    assert summary["vintage_count_status"] == "sec_latest_writer_execution"
