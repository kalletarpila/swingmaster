from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.preflight_final_mixed_single_ticker import main, run_preflight
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.reported_final_mixed_vintage import (
    build_final_mixed_source_hash,
    build_final_mixed_statement_vintage_id,
)
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)


def test_finds_candidate_row_and_returns_readable_summary(tmp_path: Path) -> None:
    db_path = _db_with_candidate(tmp_path)

    result = _run(db_path)

    assert result["ticker"] == "A"
    assert result["period_end_date"] == "2026-03-31"
    assert result["provenance_rows"] == 2
    assert result["query_only"] == 1


def test_missing_latest_quarterly_row_returns_no_source_row(tmp_path: Path) -> None:
    db_path = tmp_path / "preflight_no_source.db"
    run_migration(db_path)

    result = _run(db_path)

    assert result["status"] == "NO_SOURCE_ROW"


def test_missing_legacy_vintage_returns_no_legacy_vintage(tmp_path: Path) -> None:
    db_path = tmp_path / "preflight_no_legacy.db"
    run_migration(db_path)
    _insert_latest_row(db_path)

    result = _run(db_path)

    assert result["status"] == "NO_SOURCE_ROW"


def test_missing_provenance_returns_no_provenance(tmp_path: Path) -> None:
    db_path = tmp_path / "preflight_no_provenance.db"
    run_migration(db_path)
    _insert_latest_row(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(conn, _vintage_row())

    result = _run(db_path)

    assert result["status"] == "NO_PROVENANCE"


def test_duplicate_final_mixed_vintage_is_detected(tmp_path: Path) -> None:
    db_path = _db_with_candidate(tmp_path)
    duplicate_id = _candidate_statement_vintage_id()
    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(conn, _vintage_row(statement_vintage_id=duplicate_id, source_provider="mixed_sec_yahoo"))

    result = _run(db_path)

    assert result["status"] == "DUPLICATE_FINAL_MIXED"
    assert result["duplicate_final_mixed"] is True


def test_legacy_baseline_only_is_not_provider_ready_final_mixed(tmp_path: Path) -> None:
    db_path = _db_with_candidate(tmp_path)

    result = _run(db_path)

    assert result["status"] == "INPUTS_INCOMPLETE_FOR_TRUE_FINAL_MIXED"
    assert result["legacy_baseline_only"] is True


def test_json_output_includes_candidate_hash_and_statement_id(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    db_path = _db_with_candidate(tmp_path)

    exit_code = main(
        [
            "--fundamentals-db",
            str(db_path),
            "--market",
            "usa",
            "--ticker",
            "A",
            "--as-of-date",
            "2026-06-19",
            "--available-at-utc",
            "2026-06-19T00:00:00Z",
            "--format",
            "json",
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["source_hash"] == _candidate_source_hash()
    assert output["statement_vintage_id"] == _candidate_statement_vintage_id()


def test_read_only_behavior_does_not_write(tmp_path: Path) -> None:
    db_path = _db_with_candidate(tmp_path)
    before_counts = _counts(db_path)

    _run(db_path)

    assert _counts(db_path) == before_counts


def test_invalid_db_path_raises_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _run(tmp_path / "missing.db")


def _run(db_path: Path) -> dict[str, object]:
    return run_preflight(
        fundamentals_db=db_path,
        market="usa",
        ticker="A",
        as_of_date="2026-06-19",
        available_at_utc="2026-06-19T00:00:00Z",
    )


def _db_with_candidate(tmp_path: Path) -> Path:
    db_path = tmp_path / "preflight_candidate.db"
    run_migration(db_path)
    _insert_latest_row(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        insert_quarterly_vintage_row(conn, _vintage_row())
        insert_quarterly_field_provenance_rows(
            conn,
            [
                _provenance_row("revenue", 100.0),
                _provenance_row("net_income", 25.0),
            ],
        )
    return db_path


def _insert_latest_row(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                revenue,
                net_income,
                currency,
                run_id
            ) VALUES ('A', '2026-03-31', 100.0, 25.0, 'USD', 'LATEST_RUN1')
            """
        )
        conn.commit()


def _vintage_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ticker": "A",
        "market": "usa",
        "period_end_date": "2026-03-31",
        "statement_vintage_id": "legacy:A:2026-03-31",
        "source_provider": "legacy_baseline",
        "source_hash": "legacy_hash",
        "revision_number": 1,
        "is_restated": 0,
        "availability_quality": "LEGACY_BASELINE",
        "available_at_utc": "2026-05-01T00:00:00Z",
        "ingested_at_utc": "2026-05-01T00:00:00Z",
        "created_at_utc": "2026-05-01T00:00:00Z",
        "run_id": "LEGACY_RUN1",
        "revenue": 100.0,
        "net_income": 25.0,
        "currency": "USD",
    }
    row.update(overrides)
    return row


def _provenance_row(field_name: str, field_value: float) -> dict[str, object]:
    return {
        "ticker": "A",
        "market": "usa",
        "period_end_date": "2026-03-31",
        "statement_vintage_id": "legacy:A:2026-03-31",
        "field_name": field_name,
        "field_value": field_value,
        "source_provider": "legacy_baseline",
        "source_table": "rc_fundamental_quarterly",
        "source_row_ref": f"A:2026-03-31:{field_name}",
        "source_hash": f"legacy_hash_{field_name}",
        "provenance_role": "LEGACY_BASELINE",
        "merge_action": "LEGACY_RETAINED",
        "created_at_utc": "2026-05-01T00:00:00Z",
        "run_id": "LEGACY_RUN1",
    }


def _candidate_source_hash() -> str:
    return build_final_mixed_source_hash(
        market="usa",
        ticker="A",
        period_end_date="2026-03-31",
        normalized_row={
            "ticker": "A",
            "period_end_date": "2026-03-31",
            "currency": "USD",
            "run_id": "LATEST_RUN1",
            "revenue": 100.0,
            "gross_profit": None,
            "operating_income": None,
            "ebit": None,
            "ebitda": None,
            "net_income": 25.0,
            "operating_cashflow": None,
            "capex": None,
            "free_cashflow": None,
            "cash": None,
            "total_debt": None,
            "shares_outstanding": None,
        },
        sec_field_source_map={
            "revenue": {
                "source_provider": "legacy_baseline",
                "source_table": "rc_fundamental_quarterly",
                "source_row_ref": "A:2026-03-31:revenue",
                "source_document_id": None,
                "source_hash": "legacy_hash_revenue",
                "provenance_role": "LEGACY_BASELINE",
                "merge_action": "LEGACY_RETAINED",
                "old_value": None,
                "new_value": None,
                "available_at_utc": None,
                "created_at_utc": "2026-05-01T00:00:00Z",
                "run_id": "LEGACY_RUN1",
                "enrichment_run_id": None,
            },
            "net_income": {
                "source_provider": "legacy_baseline",
                "source_table": "rc_fundamental_quarterly",
                "source_row_ref": "A:2026-03-31:net_income",
                "source_document_id": None,
                "source_hash": "legacy_hash_net_income",
                "provenance_role": "LEGACY_BASELINE",
                "merge_action": "LEGACY_RETAINED",
                "old_value": None,
                "new_value": None,
                "available_at_utc": None,
                "created_at_utc": "2026-05-01T00:00:00Z",
                "run_id": "LEGACY_RUN1",
                "enrichment_run_id": None,
            },
        },
        yahoo_field_source_map={},
        fallback_audit_rows=[],
    )


def _candidate_statement_vintage_id() -> str:
    return build_final_mixed_statement_vintage_id(
        market="usa",
        ticker="A",
        period_end_date="2026-03-31",
        source_hash=_candidate_source_hash(),
    )


def _counts(db_path: Path) -> tuple[int, int, int]:
    with sqlite3.connect(str(db_path)) as conn:
        return (
            conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0],
        )
