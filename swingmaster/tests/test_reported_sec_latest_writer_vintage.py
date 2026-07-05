from __future__ import annotations

from swingmaster.fundamentals.reported_sec_latest_writer_vintage import (
    build_latest_writer_sec_vintage_candidate,
)


def test_latest_writer_candidate_identity_is_deterministic() -> None:
    first = _candidate()
    second = _candidate()

    assert first["source_hash"] == second["source_hash"]
    assert first["statement_vintage_id"] == second["statement_vintage_id"]


def test_clear_sec_evidence_produces_sec_provenance() -> None:
    candidate = _candidate()

    revenue = _provenance_by_field(candidate)["revenue"]
    assert revenue["source_provider"] == "sec_edgar"
    assert revenue["provenance_role"] == "PRIMARY_REPORTED"
    assert revenue["merge_action"] == "SEC_RETAINED"


def test_non_null_field_without_matching_sec_value_gets_unknown_provenance() -> None:
    candidate = _candidate(revenue=101.0)

    revenue = _provenance_by_field(candidate)["revenue"]
    assert revenue["source_provider"] == "unknown"
    assert revenue["provenance_role"] == "UNKNOWN_RETAINED"
    assert revenue["merge_action"] == "SOURCE_NOT_PROVIDED"


def test_no_yahoo_provenance_is_invented() -> None:
    candidate = _candidate()

    assert {row["source_provider"] for row in candidate["provenance_rows"]} == {"sec_edgar"}


def test_candidate_uses_latest_row_values_not_sec_reconstruct_output() -> None:
    candidate = _candidate(revenue=101.0)

    assert candidate["vintage_row"]["revenue"] == 101.0
    assert _provenance_by_field(candidate)["revenue"]["source_provider"] == "unknown"


def _candidate(*, revenue: float = 100.0) -> dict[str, object]:
    return build_latest_writer_sec_vintage_candidate(
        latest_row={
            "ticker": "AAPL",
            "period_end_date": "2026-03-31",
            "revenue": revenue,
            "operating_cashflow": 35.0,
            "capex": 5.0,
            "free_cashflow": 40.0,
            "cash": 80.0,
            "total_debt": 20.0,
            "run_id": "LATEST_RUN1",
        },
        sec_raw_rows=_raw_facts(),
        market="usa",
        available_at_utc="2026-04-30T00:00:00Z",
        ingested_at_utc="2026-04-30T01:00:00Z",
        vintage_run_id="VINTAGE_RUN1",
        source_run_id="SEC_RAW_RUN1",
    )


def _provenance_by_field(candidate: dict[str, object]) -> dict[str, dict[str, object]]:
    return {str(row["field_name"]): row for row in candidate["provenance_rows"]}  # type: ignore[index]


def _raw_facts() -> list[dict[str, object]]:
    return [
        _fact("Revenues", 100.0, "income"),
        _fact("NetCashProvidedByUsedInOperatingActivities", 35.0, "cashflow"),
        _fact("PaymentsToAcquirePropertyPlantAndEquipment", 5.0, "cashflow"),
        _fact("CashAndCashEquivalentsAtCarryingValue", 80.0, "balance", start="NULL"),
        _fact("LongTermDebtCurrent", 5.0, "balance", start="NULL"),
        _fact("LongTermDebtNoncurrent", 15.0, "balance", start="NULL"),
    ]


def _fact(tag: str, value: float, statement_type: str, *, start: str = "2026-01-01") -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "statement_type": statement_type,
        "period_end_date": "2026-03-31",
        "period_type": "sec_fact",
        "field_name": f"{tag}|form=10-Q|unit=USD|fy=2026|fp=Q1|frame=CY2026Q1|start={start}|filed=2026-04-29",
        "field_value": value,
        "currency": "USD",
        "source": "sec_edgar",
        "retrieved_at_utc": "2026-04-30T00:30:00Z",
        "run_id": "SEC_RAW_RUN1",
    }
