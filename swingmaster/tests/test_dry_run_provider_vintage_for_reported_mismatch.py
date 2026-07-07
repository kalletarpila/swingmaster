from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.cli.dry_run_provider_vintage_for_reported_mismatch import main, run_dry_run
from swingmaster.cli.run_fundamental_migrations import run_migration


AVAILABLE_AT = "2026-07-07T20:04:15Z"
INGESTED_AT = "2026-07-07T20:04:15Z"
VINTAGE_RUN_ID = "TEST_PROVIDER_VINTAGE_DRY_RUN"


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _insert_latest(conn: sqlite3.Connection, *, total_debt: float = 100.0, cash: float | None = 5.0) -> None:
    conn.execute(
        """
        INSERT INTO rc_fundamental_quarterly (
            ticker,
            period_end_date,
            revenue,
            cash,
            total_debt,
            currency,
            run_id
        ) VALUES ('GIS', '2025-05-25', 10.0, ?, ?, 'USD', 'LATEST_RUN')
        """,
        (cash, total_debt),
    )


def _insert_visible_vintage(conn: sqlite3.Connection, *, total_debt: float = 40.0) -> None:
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
            cash,
            total_debt,
            currency,
            created_at_utc
        ) VALUES ('GIS', 'usa', '2025-05-25', 'legacy:GIS:2025-05-25', 'UNKNOWN_LEGACY', 'legacy_hash', '2026-06-19T00:00:00Z', '2026-06-19T00:05:00Z', 'LEGACY_RUN', 10.0, 5.0, ?, 'USD', '2026-06-19T00:05:00Z')
        """,
        (total_debt,),
    )


def _insert_sec_fact(conn: sqlite3.Connection, field_name: str, value: float, statement_type: str = "balance") -> None:
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
        ) VALUES ('GIS', ?, '2025-05-25', 'sec_fact', ?, ?, 'USD', 'sec_edgar', '2026-07-07T20:04:15Z', 'SEC_RAW_RUN')
        """,
        (statement_type, field_name, value),
    )


def _insert_complete_sec_evidence(conn: sqlite3.Connection) -> None:
    _insert_sec_fact(conn, "RevenueFromContractWithCustomerExcludingAssessedTax|form=10-K|unit=USD|fy=2026|fp=FY|frame=NULL|start=NULL|filed=2026-07-01", 10.0, "income")
    _insert_sec_fact(conn, "CashAndCashEquivalentsAtCarryingValue|form=10-K|unit=USD|fy=2026|fp=FY|frame=NULL|start=NULL|filed=2026-07-01", 5.0)
    _insert_sec_fact(conn, "LongTermDebtCurrent|form=10-K|unit=USD|fy=2026|fp=FY|frame=NULL|start=NULL|filed=2026-07-01", 10.0)
    _insert_sec_fact(conn, "LongTermDebtNoncurrent|form=10-K|unit=USD|fy=2026|fp=FY|frame=NULL|start=NULL|filed=2026-07-01", 50.0)
    _insert_sec_fact(conn, "ShortTermBorrowings|form=10-K|unit=USD|fy=2026|fp=FY|frame=NULL|start=NULL|filed=2026-07-01", 40.0)


def _base_db(db_path: Path, *, complete_sec: bool = True, latest: bool = True) -> None:
    run_migration(db_path)
    with _connect(db_path) as conn:
        if latest:
            _insert_latest(conn)
        _insert_visible_vintage(conn)
        if complete_sec:
            _insert_complete_sec_evidence(conn)
        conn.commit()


def _run(db_path: Path, *, vintage_run_id: str = VINTAGE_RUN_ID) -> dict[str, object]:
    return run_dry_run(
        fundamentals_db=db_path,
        market="usa",
        ticker="GIS",
        period_end_date="2025-05-25",
        available_at_utc=AVAILABLE_AT,
        ingested_at_utc=INGESTED_AT,
        vintage_run_id=vintage_run_id,
        sample_limit=20,
    )


def test_supported_latest_builds_ready_candidate(tmp_path: Path) -> None:
    db_path = tmp_path / "ready.db"
    _base_db(db_path)

    summary = _run(db_path)

    assert summary["overall_status"] == "DRY_RUN_READY"
    assert summary["planned_vintage_rows"] == 1
    assert summary["planned_provenance_rows"] == 3
    assert summary["candidate_source_provider"] == "sec_edgar"
    assert str(summary["candidate_statement_vintage_id"]).startswith("sec_edgar:usa:GIS:2025-05-25")


def test_visible_legacy_lower_value_is_described(tmp_path: Path) -> None:
    db_path = tmp_path / "mismatch.db"
    _base_db(db_path)

    summary = _run(db_path)

    assert "total_debt" in summary["mismatched_fields"]
    assert summary["latest_value_total_debt"] == 100.0
    assert summary["visible_vintage_value_total_debt"] == 40.0
    assert summary["candidate_value_total_debt"] == 100.0


def test_total_debt_component_sum_is_reported(tmp_path: Path) -> None:
    db_path = tmp_path / "component_sum.db"
    _base_db(db_path)

    summary = _run(db_path)

    assert summary["total_debt_component_sum"] == 100.0
    assert summary["total_debt_component_values"] == {
        "LongTermDebtCurrent": 10.0,
        "LongTermDebtNoncurrent": 50.0,
        "ShortTermBorrowings": 40.0,
    }


def test_missing_latest_row_blocks(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_latest.db"
    _base_db(db_path, latest=False)

    summary = _run(db_path)

    assert summary["overall_status"] == "DRY_RUN_BLOCKED_NO_LATEST_ROW"
    assert summary["planned_vintage_rows"] == 0


def test_missing_sec_evidence_blocks(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_sec.db"
    _base_db(db_path, complete_sec=False)

    summary = _run(db_path)

    assert summary["overall_status"] == "DRY_RUN_BLOCKED_NO_SEC_EVIDENCE"


def test_incomplete_provenance_is_ready_with_unknown(tmp_path: Path) -> None:
    db_path = tmp_path / "unknown.db"
    run_migration(db_path)
    with _connect(db_path) as conn:
        _insert_latest(conn, total_debt=100.0, cash=5.0)
        _insert_visible_vintage(conn, total_debt=40.0)
        _insert_sec_fact(conn, "LongTermDebtCurrent|form=10-K|unit=USD|fy=2026|fp=FY|frame=NULL|start=NULL|filed=2026-07-01", 10.0)
        _insert_sec_fact(conn, "LongTermDebtNoncurrent|form=10-K|unit=USD|fy=2026|fp=FY|frame=NULL|start=NULL|filed=2026-07-01", 50.0)
        _insert_sec_fact(conn, "ShortTermBorrowings|form=10-K|unit=USD|fy=2026|fp=FY|frame=NULL|start=NULL|filed=2026-07-01", 40.0)
        conn.commit()

    summary = _run(db_path)

    assert summary["overall_status"] == "DRY_RUN_READY_WITH_UNKNOWN_PROVENANCE"
    assert "revenue" in summary["unknown_provenance_fields"]
    assert "cash" in summary["unknown_provenance_fields"]


def test_duplicate_candidate_statement_id_blocks(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicate.db"
    _base_db(db_path)
    first = _run(db_path)
    with _connect(db_path) as conn:
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
            ) VALUES ('GIS', 'usa', '2025-05-25', ?, 'sec_edgar', 'hash', '2026-07-07T20:04:15Z', '2026-07-07T20:04:15Z', 'EXISTING', 10.0, 100.0, 'USD', '2026-07-07T20:04:15Z')
            """,
            (first["candidate_statement_vintage_id"],),
        )
        conn.commit()

    summary = _run(db_path)

    assert summary["overall_status"] == "DRY_RUN_BLOCKED_DUPLICATE_VINTAGE"


def test_json_output_includes_statement_id_and_source_hash(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "json.db"
    _base_db(db_path)

    rc = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--ticker",
            "GIS",
            "--period-end-date",
            "2025-05-25",
            "--available-at-utc",
            AVAILABLE_AT,
            "--ingested-at-utc",
            INGESTED_AT,
            "--vintage-run-id",
            VINTAGE_RUN_ID,
            "--format",
            "json",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["candidate_statement_vintage_id"]
    assert payload["summary"]["candidate_source_hash"]


def test_dry_run_does_not_write_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "readonly.db"
    _base_db(db_path)
    with _connect(db_path) as conn:
        before = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]

    _run(db_path)

    with _connect(db_path) as conn:
        after = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
    assert after == before


def test_invalid_db_path_exits_nonzero(tmp_path: Path, capsys) -> None:
    rc = main(
        [
            "--fundamentals-db",
            str(tmp_path / "missing.db"),
            "--market",
            "usa",
            "--ticker",
            "GIS",
            "--period-end-date",
            "2025-05-25",
            "--available-at-utc",
            AVAILABLE_AT,
            "--ingested-at-utc",
            INGESTED_AT,
            "--vintage-run-id",
            VINTAGE_RUN_ID,
            "--format",
            "json",
        ]
    )

    assert rc == 2
    assert "ERROR:" in capsys.readouterr().err
