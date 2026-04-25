from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any


FLOW_TAG_TO_FIELD = {
    "Revenues": "Total Revenue",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "Total Revenue",
    "GrossProfit": "Gross Profit",
    "OperatingIncomeLoss": "Operating Income",
    "NetIncomeLoss": "Net Income",
    "NetCashProvidedByUsedInOperatingActivities": "Operating Cash Flow",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": "Operating Cash Flow",
    "PaymentsToAcquirePropertyPlantAndEquipment": "Capital Expenditure",
    "PaymentsToAcquireProductiveAssets": "Capital Expenditure",
}
SNAPSHOT_TAG_TO_FIELD = {
    "CashAndCashEquivalentsAtCarryingValue": "Cash And Cash Equivalents",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents": "Cash And Cash Equivalents",
    "LongTermDebtCurrent": "Total Debt",
    "LongTermDebtNoncurrent": "Total Debt",
    "LongTermDebtAndFinanceLeaseObligationsCurrent": "Total Debt",
    "LongTermDebtAndFinanceLeaseObligationsNoncurrent": "Total Debt",
    "EntityCommonStockSharesOutstanding": "Ordinary Shares Number",
    "WeightedAverageNumberOfDilutedSharesOutstanding": "Ordinary Shares Number",
    "WeightedAverageNumberOfSharesOutstandingBasic": "Ordinary Shares Number",
}
FIELD_TO_STATEMENT_TYPE = {
    "Total Revenue": "income",
    "Gross Profit": "income",
    "Operating Income": "income",
    "Net Income": "income",
    "Operating Cash Flow": "cashflow",
    "Capital Expenditure": "cashflow",
    "Cash And Cash Equivalents": "balance",
    "Total Debt": "balance",
    "Ordinary Shares Number": "balance",
}
FIELD_TAG_PRIORITY = {
    "Total Revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"],
    "Gross Profit": ["GrossProfit"],
    "Operating Income": ["OperatingIncomeLoss"],
    "Net Income": ["NetIncomeLoss"],
    "Operating Cash Flow": [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ],
    "Capital Expenditure": [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
    ],
    "Cash And Cash Equivalents": [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ],
    "Ordinary Shares Number": [
        "EntityCommonStockSharesOutstanding",
        "WeightedAverageNumberOfDilutedSharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingBasic",
    ],
}
DEBT_GROUPS = [
    [
        "LongTermDebtAndFinanceLeaseObligationsCurrent",
        "LongTermDebtAndFinanceLeaseObligationsNoncurrent",
    ],
    ["LongTermDebtCurrent", "LongTermDebtNoncurrent"],
]
SUPPORTED_FP = {"Q1", "Q2", "Q3", "FY"}


def load_sec_fact_rows(conn: sqlite3.Connection, ticker: str) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                ticker,
                statement_type,
                period_end_date,
                field_name,
                field_value,
                currency
            FROM rc_fundamental_statement_raw
            WHERE ticker = ?
              AND source = 'sec_edgar'
              AND period_type = 'sec_fact'
            ORDER BY ticker ASC, statement_type ASC, period_end_date ASC, field_name ASC
            """,
            (ticker.upper(),),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    if not rows:
        raise RuntimeError(f"SEC_FACT_ROWS_NOT_FOUND:{ticker.upper()}")
    return rows


def parse_sec_field_name(field_name: str) -> dict[str, str] | None:
    parts = field_name.split("|")
    if len(parts) != 8:
        return None
    tag = parts[0]
    metadata: dict[str, str] = {"tag": tag}
    for part in parts[1:]:
        if "=" not in part:
            return None
        key, value = part.split("=", 1)
        metadata[key] = value
    required = {"form", "unit", "fy", "fp", "frame", "start", "filed"}
    if not required.issubset(metadata):
        return None
    return metadata


def reconstruct_quarterly_rows(
    sec_fact_rows: list[sqlite3.Row],
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
) -> list[dict[str, Any]]:
    parsed_rows = []
    for row in sec_fact_rows:
        parsed = parse_sec_field_name(str(row["field_name"]))
        if parsed is None:
            continue
        if parsed["form"] not in ("10-Q", "10-K"):
            continue
        if parsed["fp"] not in SUPPORTED_FP:
            continue
        tag = parsed["tag"]
        if tag not in FLOW_TAG_TO_FIELD and tag not in SNAPSHOT_TAG_TO_FIELD:
            continue
        parsed_rows.append(
            {
                "ticker": ticker.upper(),
                "statement_type": str(row["statement_type"]),
                "period_end_date": str(row["period_end_date"]),
                "field_value": row["field_value"],
                "currency": row["currency"],
                **parsed,
            }
        )

    selected_facts = _select_best_facts(parsed_rows)
    reconstructed = []
    reconstructed.extend(_reconstruct_flow_fields(selected_facts, ticker, run_id, retrieved_at_utc))
    reconstructed.extend(_reconstruct_snapshot_fields(selected_facts, ticker, run_id, retrieved_at_utc))
    reconstructed.extend(_reconstruct_total_debt(selected_facts, ticker, run_id, retrieved_at_utc))
    if not reconstructed:
        raise RuntimeError(f"SEC_QUARTERLY_ROWS_NOT_RECONSTRUCTED:{ticker.upper()}")
    return sorted(
        reconstructed,
        key=lambda row: (row["ticker"], row["statement_type"], row["period_end_date"], row["field_name"]),
    )


def _select_best_facts(parsed_rows: list[dict[str, Any]]) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in parsed_rows:
        grouped[(row["tag"], row["currency"], row["fy"], row["fp"])].append(row)
    selected: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for key, rows in grouped.items():
        selected[key] = sorted(rows, key=_fact_priority_key, reverse=True)[0]
    return selected


def _fact_priority_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        1 if row["frame"] != "NULL" else 0,
        row["filed"],
        row["start"],
        row["period_end_date"],
        float(row["field_value"]) if row["field_value"] is not None else float("-inf"),
        "".join(chr(255 - ord(ch)) for ch in str(row["tag"])) if False else "",
    )


def _field_name_tiebreak(row: dict[str, Any]) -> str:
    return (
        f"{row['tag']}|form={row['form']}|unit={row['unit']}|fy={row['fy']}|fp={row['fp']}"
        f"|frame={row['frame']}|start={row['start']}|filed={row['filed']}"
    )


def _fact_priority_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        1 if row["frame"] != "NULL" else 0,
        row["filed"],
        row["start"],
        row["period_end_date"],
        float(row["field_value"]) if row["field_value"] is not None else float("-inf"),
        tuple(-ord(char) for char in _field_name_tiebreak(row)),
    )


def _reconstruct_flow_fields(
    selected_facts: dict[tuple[str, str, str, str], dict[str, Any]],
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field_name, tag_priority in FIELD_TAG_PRIORITY.items():
        if field_name not in ("Total Revenue", "Gross Profit", "Operating Income", "Net Income", "Operating Cash Flow", "Capital Expenditure"):
            continue
        available_units = sorted(
            {
                key[1]
                for key in selected_facts
                if key[0] in tag_priority
            }
        )
        if not available_units:
            continue
        unit = _select_unit(field_name, available_units)
        fiscal_years = sorted({key[2] for key in selected_facts if key[0] in tag_priority and key[1] == unit})
        for fy in fiscal_years:
            series = {fp: _pick_tag_fact(selected_facts, tag_priority, unit, fy, fp) for fp in SUPPORTED_FP}
            q1 = _row_value(series["Q1"])
            q2_ytd = _row_value(series["Q2"])
            q3_ytd = _row_value(series["Q3"])
            fy_ytd = _row_value(series["FY"])
            if series["Q1"] is not None and q1 is not None:
                rows.append(_build_output_row(ticker, field_name, series["Q1"]["period_end_date"], q1, unit, run_id, retrieved_at_utc))
            if series["Q2"] is not None and q1 is not None and q2_ytd is not None:
                rows.append(_build_output_row(ticker, field_name, series["Q2"]["period_end_date"], q2_ytd - q1, unit, run_id, retrieved_at_utc))
            if series["Q3"] is not None and q2_ytd is not None and q3_ytd is not None:
                rows.append(_build_output_row(ticker, field_name, series["Q3"]["period_end_date"], q3_ytd - q2_ytd, unit, run_id, retrieved_at_utc))
            if all(series[fp] is not None for fp in ("Q1", "Q2", "Q3", "FY")) and None not in (q1, q2_ytd, q3_ytd, fy_ytd):
                q2 = q2_ytd - q1
                q3 = q3_ytd - q2_ytd
                q4 = fy_ytd - q1 - q2 - q3
                rows.append(_build_output_row(ticker, field_name, series["FY"]["period_end_date"], q4, unit, run_id, retrieved_at_utc))
    return rows


def _reconstruct_snapshot_fields(
    selected_facts: dict[tuple[str, str, str, str], dict[str, Any]],
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    snapshot_fields = ("Cash And Cash Equivalents", "Ordinary Shares Number")
    for field_name in snapshot_fields:
        tag_priority = FIELD_TAG_PRIORITY[field_name]
        available_units = sorted({key[1] for key in selected_facts if key[0] in tag_priority})
        if not available_units:
            continue
        unit = _select_unit(field_name, available_units)
        fiscal_years = sorted({key[2] for key in selected_facts if key[0] in tag_priority and key[1] == unit})
        for fy in fiscal_years:
            for fp in ("Q1", "Q2", "Q3", "FY"):
                fact = _pick_tag_fact(selected_facts, tag_priority, unit, fy, fp)
                value = _row_value(fact)
                if fact is not None and value is not None:
                    rows.append(_build_output_row(ticker, field_name, fact["period_end_date"], value, unit, run_id, retrieved_at_utc))
    return rows


def _reconstruct_total_debt(
    selected_facts: dict[tuple[str, str, str, str], dict[str, Any]],
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    debt_tags = {tag for group in DEBT_GROUPS for tag in group}
    available_units = sorted({key[1] for key in selected_facts if key[0] in debt_tags})
    if not available_units:
        return rows
    unit = _select_unit("Total Debt", available_units)
    fiscal_years = sorted({key[2] for key in selected_facts if key[0] in debt_tags and key[1] == unit})
    for fy in fiscal_years:
        for fp in ("Q1", "Q2", "Q3", "FY"):
            value = None
            period_end_date = None
            for tag_group in DEBT_GROUPS:
                facts = [_pick_specific_fact(selected_facts, tag, unit, fy, fp) for tag in tag_group]
                present = [fact for fact in facts if fact is not None and _row_value(fact) is not None]
                if present:
                    value = sum(_row_value(fact) for fact in present if _row_value(fact) is not None)
                    period_end_date = present[0]["period_end_date"]
                    break
            if value is not None and period_end_date is not None:
                rows.append(_build_output_row(ticker, "Total Debt", period_end_date, value, unit, run_id, retrieved_at_utc))
    return rows


def _pick_tag_fact(
    selected_facts: dict[tuple[str, str, str, str], dict[str, Any]],
    tag_priority: list[str],
    unit: str,
    fy: str,
    fp: str,
) -> dict[str, Any] | None:
    for tag in tag_priority:
        fact = _pick_specific_fact(selected_facts, tag, unit, fy, fp)
        if fact is not None and _row_value(fact) is not None:
            return fact
    return None


def _pick_specific_fact(
    selected_facts: dict[tuple[str, str, str, str], dict[str, Any]],
    tag: str,
    unit: str,
    fy: str,
    fp: str,
) -> dict[str, Any] | None:
    return selected_facts.get((tag, unit, fy, fp))


def _row_value(fact: dict[str, Any] | None) -> float | None:
    if fact is None:
        return None
    value = fact["field_value"]
    return None if value is None else float(value)


def _select_unit(field_name: str, units: list[str]) -> str:
    preferred_unit = "shares" if field_name == "Ordinary Shares Number" else "USD"
    if preferred_unit in units:
        return preferred_unit
    return sorted(units)[0]


def _build_output_row(
    ticker: str,
    field_name: str,
    period_end_date: str,
    value: float,
    currency: str,
    run_id: str,
    retrieved_at_utc: str,
) -> dict[str, Any]:
    if field_name == "Capital Expenditure":
        value = -abs(value)
    return {
        "ticker": ticker.upper(),
        "statement_type": FIELD_TO_STATEMENT_TYPE[field_name],
        "period_end_date": period_end_date,
        "period_type": "quarterly",
        "field_name": field_name,
        "field_value": float(value),
        "currency": currency,
        "source": "sec_edgar",
        "retrieved_at_utc": retrieved_at_utc,
        "run_id": run_id,
    }


def insert_reconstructed_quarterly_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    conn.executemany(
        """
        INSERT OR REPLACE INTO rc_fundamental_statement_raw (
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["ticker"],
                row["statement_type"],
                row["period_end_date"],
                row["period_type"],
                row["field_name"],
                row["field_value"],
                row["currency"],
                row["source"],
                row["retrieved_at_utc"],
                row["run_id"],
            )
            for row in rows
        ],
    )
    return len(rows)
