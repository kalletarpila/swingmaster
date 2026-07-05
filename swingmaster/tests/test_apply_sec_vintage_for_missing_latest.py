from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli.apply_sec_vintage_for_missing_latest import APPROVAL_TOKEN, main
from swingmaster.cli.run_fundamental_migrations import run_migration


AVAILABLE_AT_UTC = "2026-04-30T00:00:00Z"
INGESTED_AT_UTC = "2026-04-30T01:00:00Z"
LATEST_RUN_ID = "LATEST_RUN1"
VINTAGE_RUN_ID = "SEC_LATEST_WRITER_VINTAGE_RUN1"


def test_apply_refuses_without_exact_approval_token(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)

    exit_code = main(_args(db_path))

    assert exit_code == 0
    assert _counts(db_path) == (1, 0, 0)


def test_apply_refuses_if_expected_count_mismatch(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)

    exit_code = main([*_args(db_path), "--approval-token", APPROVAL_TOKEN, "--expected-count", "2"])

    assert exit_code == 2
    assert _counts(db_path) == (1, 0, 0)


def test_apply_writes_only_missing_vintage_rows_in_temp_db(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)

    exit_code = main([*_args(db_path), "--approval-token", APPROVAL_TOKEN, "--expected-count", "1"])

    assert exit_code == 0
    assert _counts(db_path) == (1, 1, 6)
    assert db_path.with_suffix(db_path.suffix + ".sec_latest_writer_vintage_apply.bak").exists()


def test_apply_second_run_is_blocked_not_replace(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)

    first = main([*_args(db_path), "--approval-token", APPROVAL_TOKEN, "--expected-count", "1"])
    second = main([*_args(db_path), "--approval-token", APPROVAL_TOKEN, "--expected-count", "1"])

    assert first == 0
    assert second == 2
    assert _counts(db_path) == (1, 1, 6)


def test_apply_does_not_import_provider_modules(tmp_path: Path) -> None:
    import sys

    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)
    main(_args(db_path))

    assert "yfinance" not in sys.modules
    assert "urllib.request" not in sys.modules


def _args(db_path: Path) -> list[str]:
    return [
        "--fundamentals-db",
        str(db_path),
        "--market",
        "usa",
        "--source-run-id",
        LATEST_RUN_ID,
        "--candidate-mode",
        "latest_writer",
        "--available-at-utc",
        AVAILABLE_AT_UTC,
        "--ingested-at-utc",
        INGESTED_AT_UTC,
        "--vintage-run-id",
        VINTAGE_RUN_ID,
    ]


def _db_with_schema(tmp_path: Path) -> Path:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    return db_path


def _insert_latest_and_raw(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                revenue,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt,
                run_id
            ) VALUES ('AAPL', '2026-03-31', 100.0, 35.0, 5.0, 40.0, 80.0, 20.0, ?)
            """,
            (LATEST_RUN_ID,),
        )
        conn.executemany(
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
            ) VALUES ('AAPL', ?, '2026-03-31', 'sec_fact', ?, ?, 'USD', 'sec_edgar', '2026-04-30T00:30:00Z', 'SEC_RAW_RUN1')
            """,
            [
                ("income", _field("Revenues"), 100.0),
                ("cashflow", _field("NetCashProvidedByUsedInOperatingActivities"), 35.0),
                ("cashflow", _field("PaymentsToAcquirePropertyPlantAndEquipment"), 5.0),
                ("balance", _field("CashAndCashEquivalentsAtCarryingValue", start="NULL"), 80.0),
                ("balance", _field("LongTermDebtCurrent", start="NULL"), 5.0),
                ("balance", _field("LongTermDebtNoncurrent", start="NULL"), 15.0),
            ],
        )


def _counts(db_path: Path) -> tuple[int, int, int]:
    with sqlite3.connect(str(db_path)) as conn:
        latest = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        vintage = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
    return latest, vintage, provenance


def _field(tag: str, *, start: str = "2026-01-01") -> str:
    return f"{tag}|form=10-Q|unit=USD|fy=2026|fp=Q1|frame=CY2026Q1|start={start}|filed=2026-04-29"
