from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.cli.dry_run_sec_vintage_for_missing_latest import main, run_dry_run
from swingmaster.cli.run_fundamental_migrations import run_migration


AVAILABLE_AT_UTC = "2026-04-30T00:00:00Z"
INGESTED_AT_UTC = "2026-04-30T01:00:00Z"
LATEST_RUN_ID = "LATEST_RUN1"
VINTAGE_RUN_ID = "SEC_VINTAGE_DRY_RUN1"


def test_ready_candidate_with_sec_raw_plans_vintage_and_provenance(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)

    result = _run(db_path)

    assert result["summary"]["overall_status"] == "DRY_RUN_READY"
    assert result["summary"]["latest_missing_vintage_rows"] == 1
    assert result["summary"]["ready_rows"] == 1
    assert result["summary"]["planned_vintage_rows"] == 1
    assert result["summary"]["planned_provenance_rows"] == 7
    assert result["samples"][0]["status"] == "READY"
    assert result["samples"][0]["provenance_field_count"] == 7


def test_row_with_no_sec_raw_is_blocked(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest(db_path)

    result = _run(db_path)

    assert result["summary"]["overall_status"] == "DRY_RUN_BLOCKED"
    assert result["summary"]["no_sec_raw_rows"] == 1
    assert result["samples"][0]["status"] == "BLOCKED_NO_SEC_RAW"


def test_reconstruction_mismatch_is_blocked(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path, latest_revenue=101.0)

    result = _run(db_path)

    assert result["summary"]["overall_status"] == "DRY_RUN_BLOCKED"
    assert result["summary"]["reconstruction_mismatch_rows"] == 1
    assert result["samples"][0]["status"] == "BLOCKED_RECONSTRUCTION_MISMATCH"
    assert "revenue" in str(result["samples"][0]["reason"])


def test_latest_writer_candidate_mode_returns_ready_with_unknown_for_latest_only_sec_raw(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path, latest_revenue=101.0)

    result = _run(db_path, candidate_mode="latest_writer")

    assert result["summary"]["overall_status"] == "DRY_RUN_READY_WITH_UNKNOWN_PROVENANCE"
    assert result["summary"]["planned_vintage_rows"] == 1
    assert result["summary"]["planned_provenance_rows"] == 7
    assert result["summary"]["ready_with_unknown_provenance_rows"] == 1
    assert result["summary"]["unknown_provenance_rows"] == 1
    assert result["samples"][0]["status"] == "READY_WITH_UNKNOWN_PROVENANCE"
    assert "revenue" in result["samples"][0]["unknown_provenance_fields"]


def test_row_with_existing_vintage_is_skipped(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)
    _insert_existing_vintage(db_path)

    result = run_dry_run(
        fundamentals_db=str(db_path),
        market="usa",
        source_run_id=LATEST_RUN_ID,
        available_at_utc=AVAILABLE_AT_UTC,
        ingested_at_utc=INGESTED_AT_UTC,
        vintage_run_id=VINTAGE_RUN_ID,
        ticker="AAPL",
    )

    assert result["summary"]["latest_missing_vintage_rows"] == 0
    assert result["summary"]["ready_rows"] == 0
    assert result["summary"]["skipped_rows"] == 1
    assert result["samples"][0]["status"] == "SKIPPED_ALREADY_HAS_VINTAGE"


def test_statement_vintage_id_and_source_hash_are_deterministic(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)

    first = _run(db_path)
    second = _run(db_path)

    assert first["samples"][0]["statement_vintage_id"] == second["samples"][0]["statement_vintage_id"]
    assert first["samples"][0]["source_hash"] == second["samples"][0]["source_hash"]


def test_json_output_includes_summary_and_samples(tmp_path: Path, capsys) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)

    exit_code = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--source-run-id",
            LATEST_RUN_ID,
            "--available-at-utc",
            AVAILABLE_AT_UTC,
            "--ingested-at-utc",
            INGESTED_AT_UTC,
            "--vintage-run-id",
            VINTAGE_RUN_ID,
            "--format",
            "json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["summary"]["ready_rows"] == 1
    assert payload["summary"]["candidate_mode"] == "sec_reconstruct"
    assert payload["samples"][0]["ticker"] == "AAPL"


def test_read_only_dry_run_does_not_write(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)
    before = _counts(db_path)

    _run(db_path)

    assert _counts(db_path) == before


def test_invalid_db_path_exits_nonzero(tmp_path: Path, capsys) -> None:
    exit_code = main(
        [
            "--fundamentals-db",
            str(tmp_path / "missing.db"),
            "--market",
            "usa",
            "--available-at-utc",
            AVAILABLE_AT_UTC,
            "--ingested-at-utc",
            INGESTED_AT_UTC,
            "--vintage-run-id",
            VINTAGE_RUN_ID,
        ]
    )

    assert exit_code == 2
    assert "FUNDAMENTALS_DB_NOT_FOUND" in capsys.readouterr().err


def test_fail_if_blocked_exits_nonzero_when_blocked_rows_exist(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest(db_path)

    exit_code = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--source-run-id",
            LATEST_RUN_ID,
            "--available-at-utc",
            AVAILABLE_AT_UTC,
            "--ingested-at-utc",
            INGESTED_AT_UTC,
            "--vintage-run-id",
            VINTAGE_RUN_ID,
            "--format",
            "json",
            "--fail-if-blocked",
        ]
    )

    assert exit_code == 1


def _run(db_path: Path, *, candidate_mode: str = "sec_reconstruct") -> dict[str, object]:
    return _run_with_mode(db_path, candidate_mode)


def _run_with_mode(db_path: Path, candidate_mode: str) -> dict[str, object]:
    return run_dry_run(
        fundamentals_db=str(db_path),
        market="usa",
        source_run_id=LATEST_RUN_ID,
        available_at_utc=AVAILABLE_AT_UTC,
        ingested_at_utc=INGESTED_AT_UTC,
        vintage_run_id=VINTAGE_RUN_ID,
        candidate_mode=candidate_mode,
    )


def _db_with_schema(tmp_path: Path) -> Path:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    return db_path


def _insert_latest_and_raw(db_path: Path, *, latest_revenue: float = 100.0) -> None:
    _insert_latest(db_path, revenue=latest_revenue)
    with sqlite3.connect(str(db_path)) as conn:
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
            ) VALUES (?, ?, ?, 'sec_fact', ?, ?, 'USD', 'sec_edgar', ?, 'SEC_RAW_RUN1')
            """,
            [
                ("AAPL", "income", "2026-03-31", _field("Revenues"), 100.0, INGESTED_AT_UTC),
                ("AAPL", "income", "2026-03-31", _field("NetIncomeLoss"), 25.0, INGESTED_AT_UTC),
                (
                    "AAPL",
                    "cashflow",
                    "2026-03-31",
                    _field("NetCashProvidedByUsedInOperatingActivities"),
                    35.0,
                    INGESTED_AT_UTC,
                ),
                (
                    "AAPL",
                    "cashflow",
                    "2026-03-31",
                    _field("PaymentsToAcquirePropertyPlantAndEquipment"),
                    5.0,
                    INGESTED_AT_UTC,
                ),
                (
                    "AAPL",
                    "balance",
                    "2026-03-31",
                    _field("CashAndCashEquivalentsAtCarryingValue", start="NULL"),
                    80.0,
                    INGESTED_AT_UTC,
                ),
                (
                    "AAPL",
                    "balance",
                    "2026-03-31",
                    _field("LongTermDebtCurrent", start="NULL"),
                    5.0,
                    INGESTED_AT_UTC,
                ),
                (
                    "AAPL",
                    "balance",
                    "2026-03-31",
                    _field("LongTermDebtNoncurrent", start="NULL"),
                    15.0,
                    INGESTED_AT_UTC,
                ),
            ],
        )


def _insert_latest(db_path: Path, *, revenue: float = 100.0) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                revenue,
                net_income,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt,
                run_id
            ) VALUES ('AAPL', '2026-03-31', ?, 25.0, 35.0, -5.0, 30.0, 80.0, 20.0, ?)
            """,
            (revenue, LATEST_RUN_ID),
        )


def _insert_existing_vintage(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
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
                created_at_utc
            ) VALUES (
                'AAPL',
                'usa',
                '2026-03-31',
                'existing:vintage',
                'sec_edgar',
                'existing_hash',
                ?,
                ?,
                'EXISTING_RUN',
                100.0,
                ?
            )
            """,
            (AVAILABLE_AT_UTC, INGESTED_AT_UTC, INGESTED_AT_UTC),
        )


def _counts(db_path: Path) -> tuple[int, int, int]:
    with sqlite3.connect(str(db_path)) as conn:
        latest = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        vintage = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
    return latest, vintage, provenance


def _field(tag: str, *, start: str = "2026-01-01") -> str:
    return f"{tag}|form=10-Q|unit=USD|fy=2026|fp=Q1|frame=CY2026Q1|start={start}|filed=2026-04-29"
