from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.build_quarterly import build_quarterly_rows
from swingmaster.fundamentals.reported_sec_dual_write_adapter import (
    write_sec_reconstructed_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_sec_vintage_metadata import build_sec_vintage_metadata
from swingmaster.fundamentals.reported_vintage_reader import (
    get_pit_quarterly_vintage,
    get_quarterly_field_provenance,
)
from swingmaster.fundamentals.sec_reconstruct_quarterly import reconstruct_quarterly_rows
from swingmaster.fundamentals.sec_reconstruction_provenance import (
    reconstruct_quarterly_rows_with_provenance,
)


AVAILABLE_AT_UTC = "2026-04-30T00:00:00Z"
INGESTED_AT_UTC = "2026-04-30T01:00:00Z"
RECON_RUN_ID = "SEC_RECON_RUN1"
VINTAGE_RUN_ID = "SEC_VINTAGE_RUN1"
NORMALIZATION_RUN_ID = "SEC_NORM_RUN1"


def test_sec_reconstruction_to_vintage_temp_db_flow(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruction_to_vintage.db"
    run_migration(db_path)
    raw_facts = _raw_facts()
    reconstructed_rows = reconstruct_quarterly_rows(raw_facts, "AAPL", RECON_RUN_ID, INGESTED_AT_UTC)
    helper_rows, provenance_by_key = reconstruct_quarterly_rows_with_provenance(
        raw_facts,
        "AAPL",
        RECON_RUN_ID,
        INGESTED_AT_UTC,
    )
    normalized_rows = build_quarterly_rows(reconstructed_rows, NORMALIZATION_RUN_ID)
    normalized_row = normalized_rows[0]

    assert helper_rows == reconstructed_rows
    assert _reconstructed_values(reconstructed_rows) == {
        ("balance", "Cash And Cash Equivalents"): 80.0,
        ("balance", "Total Debt"): 20.0,
        ("cashflow", "Capital Expenditure"): -5.0,
        ("cashflow", "Operating Cash Flow"): 35.0,
        ("income", "Net Income"): 25.0,
        ("income", "Total Revenue"): 100.0,
    }
    assert normalized_row["revenue"] == 100.0
    assert normalized_row["net_income"] == 25.0
    assert normalized_row["cash"] == 80.0
    assert normalized_row["operating_cashflow"] == 35.0
    assert normalized_row["capex"] == -5.0
    assert normalized_row["free_cashflow"] == 30.0
    assert normalized_row["total_debt"] == 20.0

    metadata = _metadata_for(normalized_row, provenance_by_key)
    with sqlite3.connect(str(db_path)) as conn:
        result = write_sec_reconstructed_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=normalized_rows,
            contributing_facts_by_key=provenance_by_key,
            write_vintage=True,
            available_at_utc=AVAILABLE_AT_UTC,
            ingested_at_utc=INGESTED_AT_UTC,
            run_id=VINTAGE_RUN_ID,
            normalization_run_id=NORMALIZATION_RUN_ID,
        )
        latest_row = conn.execute(
            """
            SELECT ticker, period_end_date, revenue, net_income, free_cashflow, total_debt, run_id
            FROM rc_fundamental_quarterly
            """
        ).fetchone()
        vintage_row = conn.execute(
            """
            SELECT statement_vintage_id, source_provider, source_hash, revenue, free_cashflow, total_debt
            FROM rc_fundamental_quarterly_vintage
            """
        ).fetchone()
        before = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-04-29T23:59:59Z", market="usa")
        at_available = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", AVAILABLE_AT_UTC, market="usa")
        after = get_pit_quarterly_vintage(conn, "AAPL", "2026-03-31", "2026-04-30T00:00:01Z", market="usa")
        provenance_rows = get_quarterly_field_provenance(conn, str(vintage_row[0]))

    assert result["latest_rows_written"] == 1
    assert result["vintage_rows_written"] == 1
    assert result["field_provenance_rows_written"] == len(provenance_rows)
    assert latest_row == ("AAPL", "2026-03-31", 100.0, 25.0, 30.0, 20.0, VINTAGE_RUN_ID)
    assert vintage_row[0] == metadata["statement_vintage_id"]
    assert vintage_row[1] == "sec_edgar"
    assert vintage_row[2] == metadata["source_hash"]
    assert vintage_row[3:] == (100.0, 30.0, 20.0)
    assert before is None
    assert at_available is not None
    assert after is not None
    assert at_available["statement_vintage_id"] == metadata["statement_vintage_id"]

    by_field = {row["field_name"]: row for row in provenance_rows}
    for field_name in ("revenue", "net_income", "cash", "operating_cashflow", "capex", "free_cashflow", "total_debt"):
        assert by_field[field_name]["source_provider"] == "sec_edgar"
        assert by_field[field_name]["provenance_role"] == "PRIMARY_REPORTED"
        assert by_field[field_name]["merge_action"] == "SEC_RETAINED"
    assert "Revenues|form=10-Q" in by_field["revenue"]["source_row_ref"]


def test_sec_reconstruction_to_vintage_source_identity_is_stable_and_changes_with_fact_value() -> None:
    raw_facts = _raw_facts()
    first_row, first_provenance = _normalized_row_and_provenance(raw_facts)
    second_row, second_provenance = _normalized_row_and_provenance(list(reversed(raw_facts)))
    changed_row, changed_provenance = _normalized_row_and_provenance(_raw_facts(revenue=101.0))

    first_metadata = _metadata_for(first_row, first_provenance)
    second_metadata = _metadata_for(second_row, second_provenance)
    changed_metadata = _metadata_for(changed_row, changed_provenance)

    assert first_metadata["source_hash"] == second_metadata["source_hash"]
    assert first_metadata["statement_vintage_id"] == second_metadata["statement_vintage_id"]
    assert first_metadata["source_hash"] != changed_metadata["source_hash"]
    assert first_metadata["statement_vintage_id"] != changed_metadata["statement_vintage_id"]


def test_sec_reconstruction_to_vintage_derived_field_provenance_uses_component_facts() -> None:
    normalized_row, provenance_by_key = _normalized_row_and_provenance(_raw_facts())
    field_map = provenance_by_key[_key()]

    assert normalized_row["free_cashflow"] == 30.0
    assert _fact_tags(field_map["free_cashflow"]) == [
        "NetCashProvidedByUsedInOperatingActivities",
        "PaymentsToAcquirePropertyPlantAndEquipment",
    ]
    assert _fact_tags(field_map["total_debt"]) == [
        "LongTermDebtCurrent",
        "LongTermDebtNoncurrent",
    ]


def test_sec_reconstruction_to_vintage_missing_contributing_facts_fail_safely(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruction_to_vintage_missing_facts.db"
    run_migration(db_path)
    normalized_row, _ = _normalized_row_and_provenance(_raw_facts())

    with sqlite3.connect(str(db_path)) as conn:
        with pytest.raises(ValueError, match="REPORTED_SEC_DUAL_WRITE_CONTRIBUTING_FACTS_MISSING:AAPL,2026-03-31"):
            write_sec_reconstructed_quarterly_rows_with_optional_vintage(
                conn,
                normalized_rows=[normalized_row],
                contributing_facts_by_key={_key(): {}},
                write_vintage=True,
                available_at_utc=AVAILABLE_AT_UTC,
                ingested_at_utc=INGESTED_AT_UTC,
                run_id=VINTAGE_RUN_ID,
                normalization_run_id=NORMALIZATION_RUN_ID,
            )


def _normalized_row_and_provenance(
    raw_facts: list[dict[str, object]],
) -> tuple[dict[str, object], dict[tuple[str, str], dict[str, list[dict[str, object]]]]]:
    reconstructed_rows, provenance_by_key = reconstruct_quarterly_rows_with_provenance(
        raw_facts,
        "AAPL",
        RECON_RUN_ID,
        INGESTED_AT_UTC,
    )
    normalized_rows = build_quarterly_rows(reconstructed_rows, NORMALIZATION_RUN_ID)
    return normalized_rows[0], provenance_by_key


def _metadata_for(
    normalized_row: dict[str, object],
    provenance_by_key: dict[tuple[str, str], dict[str, list[dict[str, object]]]],
) -> dict[str, object]:
    facts = [fact for field_name in sorted(provenance_by_key[_key()]) for fact in provenance_by_key[_key()][field_name]]
    return build_sec_vintage_metadata(
        market="usa",
        ticker=str(normalized_row["ticker"]),
        period_end_date=str(normalized_row["period_end_date"]),
        normalized_row=normalized_row,
        contributing_facts=facts,
        available_at_utc=AVAILABLE_AT_UTC,
        ingested_at_utc=INGESTED_AT_UTC,
        run_id=VINTAGE_RUN_ID,
        normalization_run_id=NORMALIZATION_RUN_ID,
    )


def _reconstructed_values(rows: list[dict[str, object]]) -> dict[tuple[str, str], float]:
    return {
        (str(row["statement_type"]), str(row["field_name"])): float(row["field_value"])
        for row in rows
    }


def _fact_tags(facts: list[dict[str, object]]) -> list[str]:
    return [str(fact["field_name"]).split("|", 1)[0] for fact in facts]


def _key() -> tuple[str, str]:
    return "AAPL", "2026-03-31"


def _raw_facts(*, revenue: float = 100.0, filed: str = "2026-04-29") -> list[dict[str, object]]:
    return [
        _fact("Revenues", revenue, "income", filed=filed),
        _fact("NetIncomeLoss", 25.0, "income", filed=filed),
        _fact("NetCashProvidedByUsedInOperatingActivities", 35.0, "cashflow", filed=filed),
        _fact("PaymentsToAcquirePropertyPlantAndEquipment", 5.0, "cashflow", filed=filed),
        _fact("CashAndCashEquivalentsAtCarryingValue", 80.0, "balance", start="NULL", filed=filed),
        _fact("LongTermDebtCurrent", 5.0, "balance", start="NULL", filed=filed),
        _fact("LongTermDebtNoncurrent", 15.0, "balance", start="NULL", filed=filed),
    ]


def _fact(
    tag: str,
    value: float,
    statement_type: str,
    *,
    start: str = "2026-01-01",
    filed: str = "2026-04-29",
) -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "statement_type": statement_type,
        "period_end_date": "2026-03-31",
        "field_name": (
            f"{tag}|form=10-Q|unit=USD|fy=2026|fp=Q1|frame=CY2026Q1|start={start}|filed={filed}"
        ),
        "field_value": value,
        "currency": "USD",
        "source": "sec_edgar",
        "retrieved_at_utc": "2026-04-30T00:30:00Z",
        "run_id": "SEC_RAW_RUN1",
    }
