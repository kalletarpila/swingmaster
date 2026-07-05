from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.cli import preflight_quarter_update_vintage_readiness as preflight
from swingmaster.cli.run_fundamental_migrations import run_migration


def _migrated_db(tmp_path: Path, name: str = "fundamentals.db") -> Path:
    db_path = tmp_path / name
    run_migration(db_path)
    return db_path


def _insert_latest(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    period_end_date: str = "2026-03-31",
    run_id: str = "RUN",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker, period_end_date, revenue, currency, run_id
        ) VALUES (?, ?, 100.0, 'USD', ?)
        """,
        (ticker, period_end_date, run_id),
    )


def _insert_vintage(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    market: str = "usa",
    period_end_date: str = "2026-03-31",
    statement_vintage_id: str = "sec:usa:AAPL:2026-03-31:1",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_vintage (
            ticker, market, period_end_date, statement_vintage_id,
            source_provider, source_hash, revision_number, is_restated,
            availability_quality, available_at_utc, ingested_at_utc,
            run_id, revenue, currency, created_at_utc
        ) VALUES (
            ?, ?, ?, ?, 'sec_edgar', 'hash', 1, 0,
            'ESTIMATED', '2026-07-05T00:00:00Z', '2026-07-05T00:00:00Z',
            'RUN', 100.0, 'USD', '2026-07-05T00:00:00Z'
        )
        """,
        (ticker, market, period_end_date, statement_vintage_id),
    )


def _insert_provenance(
    conn: sqlite3.Connection,
    *,
    ticker: str = "AAPL",
    market: str = "usa",
    period_end_date: str = "2026-03-31",
    statement_vintage_id: str = "sec:usa:AAPL:2026-03-31:1",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly_field_provenance (
            ticker, market, period_end_date, statement_vintage_id,
            field_name, field_value, source_provider, source_table,
            source_row_ref, source_hash, provenance_role, merge_action,
            available_at_utc, created_at_utc, run_id
        ) VALUES (
            ?, ?, ?, ?, 'revenue', 100.0, 'sec_edgar',
            'rc_fundamental_statement_raw', 'revenue', 'hash',
            'PRIMARY_REPORTED', 'SEC_RETAINED',
            '2026-07-05T00:00:00Z', '2026-07-05T00:00:00Z', 'RUN'
        )
        """,
        (ticker, market, period_end_date, statement_vintage_id),
    )


def test_parity_ok_returns_ready_noop(tmp_path: Path) -> None:
    db_path = _migrated_db(tmp_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        _insert_provenance(conn)
        conn.commit()

    result = preflight.run_preflight(fundamentals_db=db_path, market="usa")

    assert result["overall_status"] == "READY_NOOP"
    assert result["quick_check"] == "ok"
    assert result["query_only"] == 1
    assert result["latest_without_vintage_count"] == 0
    assert result["vintage_without_latest_count"] == 0
    assert result["yahoo_aware_pending_action_count"] == 0


def test_latest_without_vintage_returns_parity_drift(tmp_path: Path) -> None:
    db_path = _migrated_db(tmp_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn)
        conn.commit()

    result = preflight.run_preflight(fundamentals_db=db_path, market="usa")

    assert result["overall_status"] == "PARITY_DRIFT"
    assert result["latest_without_vintage_count"] == 1
    assert result["sec_missing_latest_candidates"] == 1


def test_vintage_without_latest_returns_parity_drift(tmp_path: Path) -> None:
    db_path = _migrated_db(tmp_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_vintage(conn)
        conn.commit()

    result = preflight.run_preflight(fundamentals_db=db_path, market="usa")

    assert result["overall_status"] == "PARITY_DRIFT"
    assert result["vintage_without_latest_count"] == 1


def test_duplicate_statement_vintage_id_returns_duplicate_vintage(tmp_path: Path) -> None:
    db_path = _migrated_db(tmp_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn, ticker="AAPL")
        _insert_latest(conn, ticker="MSFT")
        _insert_vintage(conn, ticker="AAPL", statement_vintage_id="duplicate")
        _insert_vintage(conn, ticker="MSFT", statement_vintage_id="duplicate")
        conn.commit()

    result = preflight.run_preflight(fundamentals_db=db_path, market="usa")

    assert result["overall_status"] == "DUPLICATE_VINTAGE"
    assert result["duplicate_statement_vintage_id_count"] == 1


def test_json_output_includes_key_counts(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    db_path = _migrated_db(tmp_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        conn.commit()

    exit_code = preflight.main(["--fundamentals-db", str(db_path), "--market", "usa", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["latest_row_count"] == 1
    assert payload["vintage_row_count"] == 1
    assert payload["provenance_row_count"] == 0
    assert payload["overall_status"] == "READY_NOOP"


def test_invalid_db_path_exits_nonzero(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    exit_code = preflight.main(["--fundamentals-db", str(tmp_path / "missing.db"), "--market", "usa"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "FUNDAMENTALS_DB_NOT_FOUND" in captured.err


def test_read_only_behavior_does_not_write(tmp_path: Path) -> None:
    db_path = _migrated_db(tmp_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_latest(conn)
        _insert_vintage(conn)
        before = conn.execute("SELECT COUNT(*) FROM rc_fundamental_run").fetchone()[0]
        conn.commit()

    result = preflight.run_preflight(fundamentals_db=db_path, market="usa")

    with sqlite3.connect(str(db_path)) as conn:
        after = conn.execute("SELECT COUNT(*) FROM rc_fundamental_run").fetchone()[0]
    assert result["overall_status"] == "READY_NOOP"
    assert after == before
