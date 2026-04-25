from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import date
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
    "CommonStocksIncludingAdditionalPaidInCapitalSharesOutstanding": "Ordinary Shares Number",
    "CommonStockSharesOutstanding": "Ordinary Shares Number",
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
        "CommonStocksIncludingAdditionalPaidInCapitalSharesOutstanding",
        "CommonStockSharesOutstanding",
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
QUARTERLY_DURATION_MIN_DAYS = 70
QUARTERLY_DURATION_MAX_DAYS = 110
WEIGHTED_AVERAGE_SHARE_TAGS = {
    "WeightedAverageNumberOfDilutedSharesOutstanding",
    "WeightedAverageNumberOfSharesOutstandingBasic",
}
SHARE_TAGS = set(FIELD_TAG_PRIORITY["Ordinary Shares Number"])


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
                "encoded_field_name": str(row["field_name"]),
                **parsed,
            }
        )

    flow_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in parsed_rows:
        if row["tag"] in FLOW_TAG_TO_FIELD:
            flow_groups[(row["tag"], str(row["currency"]), row["fy"])].append(row)
    for row in parsed_rows:
        row["flow_group_rows"] = flow_groups.get((row["tag"], str(row["currency"]), row["fy"]), [])

    selected_snapshot_facts = _select_best_snapshot_facts(parsed_rows)
    selected_share_facts = _select_best_share_period_facts(parsed_rows)
    selected_flow_facts = _select_best_flow_facts(parsed_rows)
    reconstructed = []
    reconstructed.extend(_reconstruct_flow_fields(selected_flow_facts, ticker, run_id, retrieved_at_utc))
    reconstructed.extend(_reconstruct_snapshot_fields(selected_snapshot_facts, selected_share_facts, ticker, run_id, retrieved_at_utc))
    reconstructed.extend(_reconstruct_total_debt(selected_snapshot_facts, ticker, run_id, retrieved_at_utc))
    if not reconstructed:
        raise RuntimeError(f"SEC_QUARTERLY_ROWS_NOT_RECONSTRUCTED:{ticker.upper()}")
    return sorted(
        reconstructed,
        key=lambda row: (row["ticker"], row["statement_type"], row["period_end_date"], row["field_name"]),
    )


def _select_best_snapshot_facts(parsed_rows: list[dict[str, Any]]) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in parsed_rows:
        if row["tag"] not in SNAPSHOT_TAG_TO_FIELD:
            continue
        if row["tag"] in SHARE_TAGS:
            continue
        grouped[(row["tag"], row["currency"], row["fy"], row["fp"])].append(row)
    return {key: _pick_best_snapshot_fact(rows) for key, rows in grouped.items()}


def _select_best_share_period_facts(parsed_rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in parsed_rows:
        if row["tag"] not in SHARE_TAGS:
            continue
        grouped[(row["period_end_date"], row["tag"], row["currency"])].append(row)
    return {key: _pick_best_share_period_fact(rows) for key, rows in grouped.items()}


def _select_best_flow_facts(
    parsed_rows: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str, str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in parsed_rows:
        if row["tag"] not in FLOW_TAG_TO_FIELD:
            continue
        duration_type = _classify_flow_duration_type(row)
        grouped[(row["tag"], row["currency"], row["fy"], row["fp"], row["period_end_date"], duration_type)].append(row)
    return {key: _pick_best_flow_fact(rows) for key, rows in grouped.items()}


def _pick_best_fact(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(rows, key=_fact_priority_key)[0]


def _pick_best_snapshot_fact(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if rows and rows[0]["tag"] in WEIGHTED_AVERAGE_SHARE_TAGS:
        return sorted(rows, key=_weighted_share_snapshot_priority_key)[0]
    return _pick_best_fact(rows)


def _pick_best_share_period_fact(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if rows and rows[0]["tag"] in WEIGHTED_AVERAGE_SHARE_TAGS:
        return sorted(rows, key=_weighted_share_snapshot_priority_key)[0]
    return _pick_best_fact(rows)


def _pick_best_flow_fact(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(rows, key=_flow_fact_priority_key)[0]


def _fact_priority_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        0 if row["frame"] != "NULL" else 1,
        _sort_desc_text(row["filed"]),
        _sort_desc_text(row["start"]),
        _sort_desc_text(row["period_end_date"]),
        _sort_desc_number(_row_value(row)),
        row["encoded_field_name"],
    )


def _flow_fact_priority_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _duration_sort_key(row),
        0 if row["frame"] != "NULL" else 1,
        _sort_desc_text(row["filed"]),
        _sort_desc_text(row["start"]),
        _sort_desc_text(row["period_end_date"]),
        _sort_desc_number(_row_value(row)),
        row["encoded_field_name"],
    )


def _weighted_share_snapshot_priority_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        0 if row["frame"] != "NULL" else 1,
        _sort_desc_text(row["filed"]),
        _sort_desc_text(row["frame"]),
        _sort_desc_number(_row_value(row)),
        row["encoded_field_name"],
    )


def _duration_sort_key(row: dict[str, Any]) -> tuple[int, int]:
    duration_days = _duration_days(row)
    if duration_days is None:
        return (1, 0)
    return (0, duration_days)


def _sort_desc_text(value: str | None) -> tuple[int, str]:
    if value in (None, "NULL"):
        return (1, "")
    return (0, "".join(chr(255 - ord(ch)) for ch in str(value)))


def _sort_desc_number(value: float | None) -> tuple[int, float]:
    if value is None:
        return (1, 0.0)
    return (0, -float(value))


def _classify_flow_duration_type(row: dict[str, Any]) -> str:
    start_date = _parse_iso_date(row["start"])
    end_date = _parse_iso_date(row["period_end_date"])
    if end_date is None:
        return "unknown_duration"
    if (
        row["form"] == "10-Q"
        and row["fp"] in ("Q1", "Q2", "Q3")
        and row["start"] == "NULL"
    ):
        return "quarterly_fact"
    if start_date is None:
        return "unknown_duration"
    duration_days = (end_date - start_date).days
    if QUARTERLY_DURATION_MIN_DAYS <= duration_days <= QUARTERLY_DURATION_MAX_DAYS:
        return "quarterly_fact"
    if row["fp"] == "FY":
        return "annual_fact"
    if duration_days > QUARTERLY_DURATION_MAX_DAYS:
        return "ytd_fact"
    return "unknown_duration"


def _parse_iso_date(value: str | None) -> date | None:
    if value in (None, "NULL"):
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _duration_days(row: dict[str, Any]) -> int | None:
    start_date = _parse_iso_date(row["start"])
    end_date = _parse_iso_date(row["period_end_date"])
    if start_date is None or end_date is None:
        return None
    return (end_date - start_date).days


def _reconstruct_flow_fields(
    selected_flow_facts: dict[tuple[str, str, str, str, str, str], dict[str, Any]],
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    flow_fields = (
        "Total Revenue",
        "Gross Profit",
        "Operating Income",
        "Net Income",
        "Operating Cash Flow",
        "Capital Expenditure",
    )
    for field_name in flow_fields:
        tag_priority = FIELD_TAG_PRIORITY[field_name]
        available_units = sorted(
            {
                row["currency"]
                for row in selected_flow_facts.values()
                if row["tag"] in tag_priority
            }
        )
        if not available_units:
            continue
        unit = _select_unit(field_name, available_units)
        fiscal_years = sorted(
            {
                row["fy"]
                for row in selected_flow_facts.values()
                if row["tag"] in tag_priority and row["currency"] == unit
            }
        )
        for fy in fiscal_years:
            chain_keys = _build_flow_chain_keys(selected_flow_facts, tag_priority, unit, fy)
            for chain_key in chain_keys:
                quarter_rows = _build_flow_rows_for_field(
                    selected_flow_facts=selected_flow_facts,
                    tag_priority=tag_priority,
                    unit=unit,
                    fy=fy,
                    chain_key=chain_key,
                    ticker=ticker,
                    field_name=field_name,
                    run_id=run_id,
                    retrieved_at_utc=retrieved_at_utc,
                )
                rows.extend(quarter_rows)
    return rows


def _build_flow_rows_for_field(
    selected_flow_facts: dict[tuple[str, str, str, str, str, str], dict[str, Any]],
    tag_priority: list[str],
    unit: str,
    fy: str,
    chain_key: str,
    ticker: str,
    field_name: str,
    run_id: str,
    retrieved_at_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    quarter_order = ("Q1", "Q2", "Q3", "FY")
    quarter_values_by_tag = {
        tag: _build_quarter_values_for_tag(selected_flow_facts, tag, unit, fy, chain_key)
        for tag in tag_priority
    }
    for fp in quarter_order:
        selected = None
        for tag in tag_priority:
            selected = quarter_values_by_tag[tag].get(fp)
            if selected is not None:
                break
        if selected is None:
            continue
        rows.append(
            _build_output_row(
                ticker=ticker,
                field_name=field_name,
                period_end_date=selected["period_end_date"],
                value=selected["field_value"],
                currency=unit,
                run_id=run_id,
                retrieved_at_utc=retrieved_at_utc,
            )
        )
    return rows


def _build_flow_chain_keys(
    selected_flow_facts: dict[tuple[str, str, str, str, str, str], dict[str, Any]],
    tag_priority: list[str],
    unit: str,
    fy: str,
) -> list[str]:
    chain_keys = {
        row["period_end_date"]
        for row in selected_flow_facts.values()
        if row["tag"] in tag_priority and row["currency"] == unit and row["fy"] == fy and row["fp"] == "FY"
    }
    return sorted(chain_keys)


def _build_quarter_values_for_tag(
    selected_flow_facts: dict[tuple[str, str, str, str, str, str], dict[str, Any]],
    tag: str,
    unit: str,
    fy: str,
    chain_key: str,
) -> dict[str, dict[str, Any]]:
    quarterly_q1 = _pick_flow_fact(selected_flow_facts, tag, unit, fy, "Q1", "quarterly_fact", chain_key)
    quarterly_q2 = _pick_flow_fact(selected_flow_facts, tag, unit, fy, "Q2", "quarterly_fact", chain_key)
    quarterly_q3 = _pick_flow_fact(selected_flow_facts, tag, unit, fy, "Q3", "quarterly_fact", chain_key)
    quarterly_fy = _pick_flow_fact(selected_flow_facts, tag, unit, fy, "FY", "quarterly_fact", chain_key)

    ytd_q1 = _pick_flow_fact(selected_flow_facts, tag, unit, fy, "Q1", "ytd_fact", chain_key)
    ytd_q2 = _pick_flow_fact(selected_flow_facts, tag, unit, fy, "Q2", "ytd_fact", chain_key)
    ytd_q3 = _pick_flow_fact(selected_flow_facts, tag, unit, fy, "Q3", "ytd_fact", chain_key)
    annual_fy = _pick_flow_fact(selected_flow_facts, tag, unit, fy, "FY", "annual_fact", chain_key)

    quarter_values: dict[str, dict[str, Any]] = {}

    if quarterly_q1 is not None and _row_value(quarterly_q1) is not None:
        quarter_values["Q1"] = {
            "period_end_date": quarterly_q1["period_end_date"],
            "field_value": _row_value(quarterly_q1),
        }
    elif ytd_q1 is not None and _row_value(ytd_q1) is not None:
        quarter_values["Q1"] = {
            "period_end_date": ytd_q1["period_end_date"],
            "field_value": _row_value(ytd_q1),
        }

    if quarterly_q2 is not None and _row_value(quarterly_q2) is not None:
        quarter_values["Q2"] = {
            "period_end_date": quarterly_q2["period_end_date"],
            "field_value": _row_value(quarterly_q2),
        }
    elif ytd_q2 is not None:
        q2_ytd = _row_value(ytd_q2)
        q1_baseline = _row_value(ytd_q1) if ytd_q1 is not None else _quarter_value(quarter_values, "Q1")
        if q2_ytd is not None and q1_baseline is not None:
            quarter_values["Q2"] = {
                "period_end_date": ytd_q2["period_end_date"],
                "field_value": q2_ytd - q1_baseline,
            }

    if quarterly_q3 is not None and _row_value(quarterly_q3) is not None:
        quarter_values["Q3"] = {
            "period_end_date": quarterly_q3["period_end_date"],
            "field_value": _row_value(quarterly_q3),
        }
    elif ytd_q3 is not None and ytd_q2 is not None:
        q3_ytd = _row_value(ytd_q3)
        q2_ytd = _row_value(ytd_q2)
        if q3_ytd is not None and q2_ytd is not None:
            quarter_values["Q3"] = {
                "period_end_date": ytd_q3["period_end_date"],
                "field_value": q3_ytd - q2_ytd,
            }

    if quarterly_fy is not None and _row_value(quarterly_fy) is not None:
        quarter_values["FY"] = {
            "period_end_date": quarterly_fy["period_end_date"],
            "field_value": _row_value(quarterly_fy),
        }
    elif annual_fy is not None and _row_value(annual_fy) is not None:
        q1 = _quarter_value(quarter_values, "Q1")
        q2 = _quarter_value(quarter_values, "Q2")
        q3 = _quarter_value(quarter_values, "Q3")
        fy_value = _row_value(annual_fy)
        if None not in (q1, q2, q3, fy_value):
            quarter_values["FY"] = {
                "period_end_date": annual_fy["period_end_date"],
                "field_value": fy_value - q1 - q2 - q3,
            }

    return quarter_values


def _pick_flow_fact(
    selected_flow_facts: dict[tuple[str, str, str, str, str, str], dict[str, Any]],
    tag: str,
    unit: str,
    fy: str,
    fp: str,
    duration_type: str,
    chain_key: str,
) -> dict[str, Any] | None:
    candidates = [
        row
        for row in selected_flow_facts.values()
        if row["tag"] == tag
        and row["currency"] == unit
        and row["fy"] == fy
        and row["fp"] == fp
        and _classify_flow_duration_type(row) == duration_type
        and _row_assigned_to_flow_chain(row, chain_key)
    ]
    if not candidates:
        return None
    return _pick_best_flow_fact(candidates)


def _row_assigned_to_flow_chain(row: dict[str, Any], chain_key: str) -> bool:
    if row["fp"] == "FY":
        return row["period_end_date"] == chain_key
    if row["fp"] not in ("Q1", "Q2", "Q3"):
        return False
    row_end_date = _parse_iso_date(row["period_end_date"])
    chain_end_date = _parse_iso_date(chain_key)
    if row_end_date is None or chain_end_date is None or row_end_date >= chain_end_date:
        return False
    return _assigned_chain_key_for_row(row) == chain_key


def _assigned_chain_key_for_row(row: dict[str, Any]) -> str | None:
    row_end_date = _parse_iso_date(row["period_end_date"])
    if row_end_date is None:
        return None
    flow_group_fy_dates = sorted(
        {
            candidate["period_end_date"]
            for candidate in row["flow_group_rows"]
            if candidate["fp"] == "FY"
        }
    )
    for fy_period_end_date in flow_group_fy_dates:
        fy_end_date = _parse_iso_date(fy_period_end_date)
        if fy_end_date is not None and fy_end_date > row_end_date:
            return fy_period_end_date
    return None


def _quarter_value(quarter_values: dict[str, dict[str, Any]], fp: str) -> float | None:
    quarter_value = quarter_values.get(fp)
    if quarter_value is None:
        return None
    return float(quarter_value["field_value"])


def _reconstruct_snapshot_fields(
    selected_facts: dict[tuple[str, str, str, str], dict[str, Any]],
    selected_share_facts: dict[tuple[str, str, str], dict[str, Any]],
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    snapshot_fields = ("Cash And Cash Equivalents",)
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
    rows.extend(_reconstruct_shares_by_period(selected_share_facts, ticker, run_id, retrieved_at_utc))
    return rows


def _reconstruct_shares_by_period(
    selected_share_facts: dict[tuple[str, str, str], dict[str, Any]],
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    period_end_dates = sorted({key[0] for key in selected_share_facts})
    tag_priority = FIELD_TAG_PRIORITY["Ordinary Shares Number"]
    for period_end_date in period_end_dates:
        available_units = sorted({key[2] for key in selected_share_facts if key[0] == period_end_date and key[1] in tag_priority})
        if not available_units:
            continue
        unit = _select_unit("Ordinary Shares Number", available_units)
        fact = _pick_share_period_fact(selected_share_facts, tag_priority, period_end_date, unit)
        value = _row_value(fact)
        if fact is not None and value is not None:
            rows.append(_build_output_row(ticker, "Ordinary Shares Number", period_end_date, value, unit, run_id, retrieved_at_utc))
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


def _pick_share_period_fact(
    selected_share_facts: dict[tuple[str, str, str], dict[str, Any]],
    tag_priority: list[str],
    period_end_date: str,
    unit: str,
) -> dict[str, Any] | None:
    for tag in tag_priority:
        fact = selected_share_facts.get((period_end_date, tag, unit))
        if fact is not None and _row_value(fact) is not None:
            return fact
    return None


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
