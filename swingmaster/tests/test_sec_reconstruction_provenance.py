from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.build_quarterly import build_quarterly_rows
from swingmaster.fundamentals.reported_sec_dual_write_adapter import (
    write_sec_reconstructed_quarterly_rows_with_optional_vintage,
)
from swingmaster.fundamentals.reported_sec_vintage_metadata import (
    build_sec_field_source_map,
    extract_sec_filed_date,
)
from swingmaster.fundamentals.sec_reconstruct_quarterly import reconstruct_quarterly_rows
from swingmaster.fundamentals.sec_reconstruction_provenance import (
    build_sec_contributing_facts_by_field,
    reconstruct_quarterly_rows_with_provenance,
)


def test_provenance_map_returns_contributing_fact_for_revenue() -> None:
    rows, provenance = _reconstruct_with_provenance(_raw_facts())

    assert _values(rows, "Total Revenue") == [("2026-03-31", 100.0)]
    revenue_facts = provenance[_key()]["revenue"]
    assert [fact["field_name"].split("|", 1)[0] for fact in revenue_facts] == ["Revenues"]
    assert revenue_facts[0]["field_value"] == 100.0


def test_provenance_map_returns_contributing_fact_for_net_income() -> None:
    _, provenance = _reconstruct_with_provenance(_raw_facts())

    net_income_facts = provenance[_key()]["net_income"]
    assert [fact["field_name"].split("|", 1)[0] for fact in net_income_facts] == ["NetIncomeLoss"]
    assert net_income_facts[0]["field_value"] == 25.0


def test_provenance_map_returns_snapshot_fact_for_cash() -> None:
    _, provenance = _reconstruct_with_provenance(_raw_facts())

    cash_facts = provenance[_key()]["cash"]
    assert [fact["field_name"].split("|", 1)[0] for fact in cash_facts] == [
        "CashAndCashEquivalentsAtCarryingValue"
    ]
    assert cash_facts[0]["statement_type"] == "balance"


def test_provenance_map_returns_all_debt_component_facts_for_total_debt() -> None:
    _, provenance = _reconstruct_with_provenance(_raw_facts())

    debt_facts = provenance[_key()]["total_debt"]
    assert [fact["field_name"].split("|", 1)[0] for fact in debt_facts] == [
        "LongTermDebtCurrent",
        "LongTermDebtNoncurrent",
    ]
    assert sum(float(fact["field_value"]) for fact in debt_facts) == 20.0


def test_provenance_map_returns_operating_cashflow_and_capex_for_free_cashflow() -> None:
    reconstructed_rows, provenance = _reconstruct_with_provenance(_raw_facts())
    normalized_row = build_quarterly_rows(reconstructed_rows, "NORM_RUN1")[0]

    assert normalized_row["operating_cashflow"] == 35.0
    assert normalized_row["capex"] == -5.0
    assert normalized_row["free_cashflow"] == 30.0
    fcf_facts = provenance[_key()]["free_cashflow"]
    assert [fact["field_name"].split("|", 1)[0] for fact in fcf_facts] == [
        "NetCashProvidedByUsedInOperatingActivities",
        "PaymentsToAcquirePropertyPlantAndEquipment",
    ]


def test_provenance_includes_encoded_field_name_metadata_with_filed_date() -> None:
    _, provenance = _reconstruct_with_provenance(_raw_facts(filed="2026-04-29"))

    assert extract_sec_filed_date(provenance[_key()]["revenue"]) == "2026-04-29"
    assert "filed=2026-04-29" in provenance[_key()]["revenue"][0]["field_name"]


def test_provenance_is_deterministic_regardless_of_raw_fact_input_order() -> None:
    raw_facts = _raw_facts()
    rows_a, provenance_a = _reconstruct_with_provenance(raw_facts)
    rows_b, provenance_b = _reconstruct_with_provenance(list(reversed(raw_facts)))

    assert rows_a == rows_b
    assert provenance_a == provenance_b


def test_provenance_helper_does_not_change_existing_reconstructed_row_values() -> None:
    raw_facts = _raw_facts()

    existing_rows = reconstruct_quarterly_rows(raw_facts, "AAPL", "RECON_RUN1", "2026-04-30T00:30:00Z")
    helper_rows, provenance = reconstruct_quarterly_rows_with_provenance(
        raw_facts,
        "AAPL",
        "RECON_RUN1",
        "2026-04-30T00:30:00Z",
    )

    assert helper_rows == existing_rows
    assert provenance[_key()]["revenue"][0]["field_value"] == 100.0


def test_missing_contributing_fact_yields_empty_map_not_guessed_fact() -> None:
    row_without_supported_mapping = {
        "ticker": "AAPL",
        "period_end_date": "2026-03-31",
        "field_name": "EBITDA",
    }

    assert build_sec_contributing_facts_by_field(
        reconstructed_row=row_without_supported_mapping,
        raw_fact_rows=_raw_facts(),
    ) == {}


def test_output_can_feed_sec_field_source_map() -> None:
    reconstructed_rows, provenance = _reconstruct_with_provenance(_raw_facts())
    normalized_row = build_quarterly_rows(reconstructed_rows, "NORM_RUN1")[0]

    source_map = build_sec_field_source_map(
        normalized_row=normalized_row,
        field_to_contributing_facts=provenance[_key()],
    )

    assert source_map["revenue"]["source_provider"] == "sec_edgar"
    assert source_map["revenue"]["merge_action"] == "SEC_RETAINED"
    assert source_map["free_cashflow"]["source_provider"] == "sec_edgar"
    assert "ebitda" not in source_map


def test_output_can_feed_sec_dual_write_scaffold_in_temp_db(tmp_path: Path) -> None:
    db_path = tmp_path / "sec_reconstruction_provenance_dual_write.db"
    run_migration(db_path)
    reconstructed_rows, provenance = _reconstruct_with_provenance(_raw_facts())
    normalized_rows = build_quarterly_rows(reconstructed_rows, "NORM_RUN1")

    with sqlite3.connect(str(db_path)) as conn:
        result = write_sec_reconstructed_quarterly_rows_with_optional_vintage(
            conn,
            normalized_rows=normalized_rows,
            contributing_facts_by_key=provenance,
            write_vintage=True,
            available_at_utc="2026-04-30T00:00:00Z",
            ingested_at_utc="2026-04-30T01:00:00Z",
            run_id="SEC_DUAL_RUN1",
            normalization_run_id="NORM_RUN1",
        )
        vintage_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]

    assert result["latest_rows_written"] == 1
    assert vintage_count == 1
    assert provenance_count >= 7


def test_provenance_helper_does_not_import_provider_or_network_modules() -> None:
    import sys

    assert "swingmaster.fundamentals.sec_edgar" not in sys.modules
    assert "swingmaster.fundamentals.providers.yahoo" not in sys.modules
    assert "yfinance" not in sys.modules
    assert "urllib.request" not in sys.modules


def _reconstruct_with_provenance(
    raw_facts: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[tuple[str, str], dict[str, list[dict[str, object]]]]]:
    return reconstruct_quarterly_rows_with_provenance(
        raw_facts,
        "AAPL",
        "RECON_RUN1",
        "2026-04-30T00:30:00Z",
    )


def _key() -> tuple[str, str]:
    return "AAPL", "2026-03-31"


def _values(rows: list[dict[str, object]], field_name: str) -> list[tuple[str, float]]:
    return [
        (str(row["period_end_date"]), float(row["field_value"]))
        for row in rows
        if row["field_name"] == field_name
    ]


def _raw_facts(*, filed: str = "2026-04-29") -> list[dict[str, object]]:
    return [
        _fact("Revenues", 100.0, "income", filed=filed),
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
