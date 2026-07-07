from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.cli.apply_provider_vintage_for_reported_mismatch import APPROVAL_TOKEN, main
from swingmaster.cli.dry_run_provider_vintage_for_reported_mismatch import run_dry_run
from swingmaster.cli.run_fundamental_migrations import run_migration


AVAILABLE_AT = "2026-07-07T20:04:15Z"
INGESTED_AT = "2026-07-07T20:04:15Z"
VINTAGE_RUN_ID = "TEST_PROVIDER_VINTAGE_APPLY"


def test_refuses_without_approval_token(tmp_path: Path) -> None:
    db_path = _ready_db(tmp_path)
    summary = _dry_run(db_path)

    rc = main([*_args(db_path, summary), "--approval-token", "WRONG"])

    assert rc == 2
    assert _counts(db_path) == (1, 0, 0)


def test_refuses_if_dry_run_not_ready(tmp_path: Path) -> None:
    db_path = _ready_db(tmp_path, total_debt=999.0)
    summary = _dry_run(db_path)

    rc = main([*_args(db_path, summary), "--approval-token", APPROVAL_TOKEN])

    assert rc == 2
    assert _counts(db_path) == (1, 0, 0)


def test_refuses_if_expected_statement_id_mismatches(tmp_path: Path) -> None:
    db_path = _ready_db(tmp_path)
    summary = _dry_run(db_path)

    rc = main(
        [
            *_args(db_path, summary, expected_statement_vintage_id="sec_edgar:usa:GIS:2025-05-25:wrong"),
            "--approval-token",
            APPROVAL_TOKEN,
        ]
    )

    assert rc == 2
    assert _counts(db_path) == (1, 0, 0)


def test_refuses_if_expected_vintage_count_mismatches(tmp_path: Path) -> None:
    db_path = _ready_db(tmp_path)
    summary = _dry_run(db_path)

    rc = main([*_args(db_path, summary, expected_vintage_count=2), "--approval-token", APPROVAL_TOKEN])

    assert rc == 2
    assert _counts(db_path) == (1, 0, 0)


def test_refuses_if_expected_provenance_count_mismatches(tmp_path: Path) -> None:
    db_path = _ready_db(tmp_path)
    summary = _dry_run(db_path)

    rc = main([*_args(db_path, summary, expected_provenance_count=4), "--approval-token", APPROVAL_TOKEN])

    assert rc == 2
    assert _counts(db_path) == (1, 0, 0)


def test_writes_one_vintage_and_provenance_rows(tmp_path: Path, capsys) -> None:
    db_path = _ready_db(tmp_path)
    summary = _dry_run(db_path)

    rc = main([*_args(db_path, summary), "--approval-token", APPROVAL_TOKEN, "--format", "json"])
    output = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert _counts(db_path) == (1, 1, 3)
    assert output["apply"]["vintage_rows_inserted"] == 1
    assert output["apply"]["provenance_rows_inserted"] == 3
    assert Path(output["apply"]["backup_path"]).exists()


def test_duplicate_second_run_fails_without_replace(tmp_path: Path) -> None:
    db_path = _ready_db(tmp_path)
    summary = _dry_run(db_path)

    first = main([*_args(db_path, summary), "--approval-token", APPROVAL_TOKEN])
    second = main([*_args(db_path, summary), "--approval-token", APPROVAL_TOKEN])

    assert first == 0
    assert second == 2
    assert _counts(db_path) == (1, 1, 3)


def test_latest_table_remains_unchanged(tmp_path: Path) -> None:
    db_path = _ready_db(tmp_path)
    summary = _dry_run(db_path)
    before = _latest_row(db_path)

    main([*_args(db_path, summary), "--approval-token", APPROVAL_TOKEN])

    assert _latest_row(db_path) == before


def test_json_output_includes_backup_path_and_inserted_counts(tmp_path: Path, capsys) -> None:
    db_path = _ready_db(tmp_path)
    summary = _dry_run(db_path)

    rc = main([*_args(db_path, summary), "--approval-token", APPROVAL_TOKEN, "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["apply"]["backup_path"]
    assert payload["count_deltas"] == {"latest": 0, "vintage": 1, "provenance": 3}


def test_invalid_db_path_exits_nonzero(tmp_path: Path) -> None:
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
            "--expected-statement-vintage-id",
            "sec_edgar:usa:GIS:2025-05-25:missing",
            "--expected-vintage-count",
            "1",
            "--expected-provenance-count",
            "3",
            "--approval-token",
            APPROVAL_TOKEN,
        ]
    )

    assert rc == 2


def _ready_db(tmp_path: Path, *, total_debt: float = 100.0) -> Path:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                revenue,
                cash,
                total_debt,
                run_id
            ) VALUES ('GIS', '2025-05-25', 10.0, 5.0, ?, 'LATEST_RUN')
            """,
            (total_debt,),
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
            ) VALUES ('GIS', ?, '2025-05-25', 'sec_fact', ?, ?, 'USD', 'sec_edgar', '2026-07-07T20:04:15Z', 'SEC_RAW')
            """,
            [
                ("income", _field("RevenueFromContractWithCustomerExcludingAssessedTax"), 10.0),
                ("balance", _field("CashAndCashEquivalentsAtCarryingValue", start="NULL"), 5.0),
                ("balance", _field("LongTermDebtCurrent", start="NULL"), 10.0),
                ("balance", _field("LongTermDebtNoncurrent", start="NULL"), 50.0),
                ("balance", _field("ShortTermBorrowings", start="NULL"), 40.0),
            ],
        )
        conn.commit()
    return db_path


def _dry_run(db_path: Path) -> dict[str, object]:
    return run_dry_run(
        fundamentals_db=db_path,
        market="usa",
        ticker="GIS",
        period_end_date="2025-05-25",
        available_at_utc=AVAILABLE_AT,
        ingested_at_utc=INGESTED_AT,
        vintage_run_id=VINTAGE_RUN_ID,
        sample_limit=20,
    )


def _args(
    db_path: Path,
    summary: dict[str, object],
    *,
    expected_statement_vintage_id: str | None = None,
    expected_vintage_count: int = 1,
    expected_provenance_count: int = 3,
) -> list[str]:
    return [
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
        "--expected-statement-vintage-id",
        expected_statement_vintage_id or str(summary["candidate_statement_vintage_id"]),
        "--expected-vintage-count",
        str(expected_vintage_count),
        "--expected-provenance-count",
        str(expected_provenance_count),
    ]


def _counts(db_path: Path) -> tuple[int, int, int]:
    with sqlite3.connect(str(db_path)) as conn:
        latest = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        vintage = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
    return latest, vintage, provenance


def _latest_row(db_path: Path) -> tuple[object, ...]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT ticker, period_end_date, revenue, cash, total_debt, run_id
            FROM rc_fundamental_quarterly
            WHERE ticker='GIS' AND period_end_date='2025-05-25'
            """
        ).fetchone()


def _field(tag: str, *, start: str = "2026-01-01") -> str:
    return f"{tag}|form=10-K|unit=USD|fy=2026|fp=FY|frame=NULL|start={start}|filed=2026-07-01"
