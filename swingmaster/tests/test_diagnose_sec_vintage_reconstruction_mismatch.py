from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.cli.diagnose_sec_vintage_reconstruction_mismatch import main, run_diagnostics
from swingmaster.cli.run_fundamental_migrations import run_migration


LATEST_RUN_ID = "LATEST_RUN1"


def test_all_fields_match_reports_matched_row(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)

    result = _run(db_path)

    assert result["summary"]["matched_rows"] == 1
    assert result["summary"]["mismatched_rows"] == 0
    assert result["samples"][0]["mismatched_fields"] == []


def test_latest_has_value_recon_null_is_classified(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path, latest_ebitda=12.0)

    result = _run(db_path)

    sample = result["samples"][0]
    assert sample["field_comparisons"]["ebitda"]["status"] == "LATEST_HAS_VALUE_RECON_NULL"
    assert result["summary"]["mismatched_field_counts"]["ebitda"] == 1


def test_recon_has_value_latest_null_is_classified(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path, latest_net_income=None)

    result = _run(db_path)

    sample = result["samples"][0]
    assert sample["field_comparisons"]["net_income"]["status"] == "RECON_HAS_VALUE_LATEST_NULL"
    assert result["summary"]["mismatched_field_counts"]["net_income"] == 1


def test_numeric_value_diff_is_classified(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path, latest_revenue=101.0)

    result = _run(db_path)

    sample = result["samples"][0]
    assert sample["field_comparisons"]["revenue"]["status"] == "VALUE_DIFF"
    assert sample["field_comparisons"]["revenue"]["numeric_diff"] == -1.0


def test_aggregates_mismatched_field_counts(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path, ticker="AAPL", latest_revenue=101.0)
    _insert_latest_and_raw(db_path, ticker="MSFT", latest_revenue=102.0)

    result = _run(db_path)

    assert result["summary"]["candidate_count"] == 2
    assert result["summary"]["mismatched_rows"] == 2
    assert result["summary"]["mismatched_field_counts"]["revenue"] == 2


def test_yahoo_evidence_presence_is_reported_not_sec_proof(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path, latest_revenue=101.0)
    _insert_yahoo_and_audit_evidence(db_path)

    result = _run(db_path)

    assert result["summary"]["sec_raw_evidence_count"] == 1
    assert result["summary"]["yahoo_evidence_count"] == 1
    assert result["summary"]["enrichment_audit_count"] == 1
    assert result["summary"]["likely_cause"] == "LATEST_CONTAINS_NON_SEC_VALUES"
    assert result["samples"][0]["yahoo_quarterly_present"] is True
    assert result["samples"][0]["enrichment_audit_present"] is True


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
            "--format",
            "json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["summary"]["candidate_count"] == 1
    assert payload["samples"][0]["ticker"] == "AAPL"


def test_invalid_db_path_exits_nonzero(tmp_path: Path, capsys) -> None:
    exit_code = main(["--fundamentals-db", str(tmp_path / "missing.db"), "--market", "usa"])

    assert exit_code == 2
    assert "FUNDAMENTALS_DB_NOT_FOUND" in capsys.readouterr().err


def test_read_only_diagnostics_does_not_write(tmp_path: Path) -> None:
    db_path = _db_with_schema(tmp_path)
    _insert_latest_and_raw(db_path)
    before = _counts(db_path)

    _run(db_path)

    assert _counts(db_path) == before


def _run(db_path: Path) -> dict[str, object]:
    return run_diagnostics(
        fundamentals_db=str(db_path),
        market="usa",
        source_run_id=LATEST_RUN_ID,
    )


def _db_with_schema(tmp_path: Path) -> Path:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    return db_path


def _insert_latest_and_raw(
    db_path: Path,
    *,
    ticker: str = "AAPL",
    latest_revenue: float | None = 100.0,
    latest_net_income: float | None = 25.0,
    latest_ebitda: float | None = None,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker,
                period_end_date,
                revenue,
                ebitda,
                net_income,
                operating_cashflow,
                capex,
                free_cashflow,
                cash,
                total_debt,
                run_id
            ) VALUES (?, '2026-03-31', ?, ?, ?, 35.0, -5.0, 30.0, 80.0, 20.0, ?)
            """,
            (ticker, latest_revenue, latest_ebitda, latest_net_income, LATEST_RUN_ID),
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
            ) VALUES (?, ?, '2026-03-31', 'sec_fact', ?, ?, 'USD', 'sec_edgar', '2026-04-30T00:30:00Z', 'SEC_RAW_RUN1')
            """,
            [
                (ticker, "income", _field("Revenues"), 100.0),
                (ticker, "income", _field("NetIncomeLoss"), 25.0),
                (ticker, "cashflow", _field("NetCashProvidedByUsedInOperatingActivities"), 35.0),
                (ticker, "cashflow", _field("PaymentsToAcquirePropertyPlantAndEquipment"), 5.0),
                (ticker, "balance", _field("CashAndCashEquivalentsAtCarryingValue", start="NULL"), 80.0),
                (ticker, "balance", _field("LongTermDebtCurrent", start="NULL"), 5.0),
                (ticker, "balance", _field("LongTermDebtNoncurrent", start="NULL"), 15.0),
            ],
        )


def _insert_yahoo_and_audit_evidence(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_yahoo_quarterly (
                market,
                symbol,
                period_end_date,
                revenue,
                source_run_id,
                run_id,
                created_at_utc
            ) VALUES ('usa', 'AAPL', '2026-03-31', 101.0, 'YRAW1', 'YQTR1', '2026-04-30T01:00:00Z')
            """
        )
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
                run_id,
                created_at_utc
            ) VALUES ('AAPL', '2026-03-31', 'revenue', 100.0, 101.0, 'sec_edgar', 'yahoo', 'updated', 'ENRICH1', '2026-04-30T01:00:00Z')
            """
        )


def _counts(db_path: Path) -> tuple[int, int, int]:
    with sqlite3.connect(str(db_path)) as conn:
        latest = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
        vintage = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
    return latest, vintage, provenance


def _field(tag: str, *, start: str = "2026-01-01") -> str:
    return f"{tag}|form=10-Q|unit=USD|fy=2026|fp=Q1|frame=CY2026Q1|start={start}|filed=2026-04-29"
