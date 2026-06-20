from __future__ import annotations

import sqlite3
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from swingmaster.fundamentals.sec_reconstruct_quarterly import (
    DEBT_GROUPS,
    FIELD_TAG_PRIORITY,
    FIELD_TO_STATEMENT_TYPE,
    FLOW_TAG_TO_FIELD,
    SNAPSHOT_TAG_TO_FIELD,
    SUPPORTED_FP,
    _build_flow_chain_keys,
    _pick_flow_fact,
    _pick_share_period_fact,
    _pick_specific_fact,
    _pick_tag_fact,
    _row_value,
    _select_best_flow_facts,
    _select_best_share_period_facts,
    _select_best_snapshot_facts,
    _select_unit,
    parse_sec_field_name,
    reconstruct_quarterly_rows,
)


RECONSTRUCTED_FIELD_TO_NORMALIZED_FIELD = {
    "Total Revenue": "revenue",
    "Gross Profit": "gross_profit",
    "Operating Income": "operating_income",
    "Net Income": "net_income",
    "Operating Cash Flow": "operating_cashflow",
    "Capital Expenditure": "capex",
    "Cash And Cash Equivalents": "cash",
    "Total Debt": "total_debt",
    "Ordinary Shares Number": "shares_outstanding",
}


def reconstruct_quarterly_rows_with_provenance(
    sec_fact_rows: Sequence[Mapping[str, Any] | sqlite3.Row],
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], dict[str, list[dict[str, Any]]]]]:
    """Return current SEC reconstructed rows plus additive field provenance.

    This intentionally delegates value reconstruction to the existing function
    so callers can assert the legacy output is unchanged.
    """
    rows = reconstruct_quarterly_rows(list(sec_fact_rows), ticker, run_id, retrieved_at_utc)
    provenance = build_sec_contributing_facts_by_reconstructed_rows(
        reconstructed_rows=rows,
        raw_fact_rows=sec_fact_rows,
    )
    return rows, provenance


def build_sec_contributing_facts_by_reconstructed_rows(
    *,
    reconstructed_rows: Sequence[Mapping[str, Any]],
    raw_fact_rows: Sequence[Mapping[str, Any] | sqlite3.Row],
) -> dict[tuple[str, str], dict[str, list[dict[str, Any]]]]:
    by_key = _build_direct_reconstruction_provenance(raw_fact_rows)
    _add_free_cashflow_provenance(by_key)

    output: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
    for row in reconstructed_rows:
        ticker = _normalize_ticker(row.get("ticker"))
        period_end_date = _require_text(row.get("period_end_date"), "period_end_date")
        reconstructed_field = _require_text(row.get("field_name"), "field_name")
        normalized_field = RECONSTRUCTED_FIELD_TO_NORMALIZED_FIELD.get(reconstructed_field)
        if ticker is None or normalized_field is None:
            continue
        row_key = (ticker, period_end_date)
        field_facts = by_key.get(row_key, {}).get(normalized_field, [])
        output.setdefault(row_key, {})[normalized_field] = _dedupe_and_sort_facts(field_facts)

    for row_key, field_map in list(output.items()):
        fcf_facts = by_key.get(row_key, {}).get("free_cashflow", [])
        if fcf_facts:
            field_map["free_cashflow"] = _dedupe_and_sort_facts(fcf_facts)

    return output


def build_sec_contributing_facts_by_field(
    *,
    reconstructed_row: Mapping[str, Any],
    raw_fact_rows: Sequence[Mapping[str, Any] | sqlite3.Row],
) -> dict[str, list[dict[str, Any]]]:
    provenance = build_sec_contributing_facts_by_reconstructed_rows(
        reconstructed_rows=[reconstructed_row],
        raw_fact_rows=raw_fact_rows,
    )
    ticker = _normalize_ticker(reconstructed_row.get("ticker"))
    period_end_date = _require_text(reconstructed_row.get("period_end_date"), "period_end_date")
    if ticker is None:
        return {}
    return provenance.get((ticker, period_end_date), {})


def _build_direct_reconstruction_provenance(
    raw_fact_rows: Sequence[Mapping[str, Any] | sqlite3.Row],
) -> dict[tuple[str, str], dict[str, list[dict[str, Any]]]]:
    parsed_rows = _parse_sec_fact_rows(raw_fact_rows)
    for row in parsed_rows:
        row["flow_group_rows"] = []

    flow_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in parsed_rows:
        if row["tag"] in FLOW_TAG_TO_FIELD:
            flow_groups[(row["tag"], str(row["currency"]), row["fy"])].append(row)
    for row in parsed_rows:
        row["flow_group_rows"] = flow_groups.get((row["tag"], str(row["currency"]), row["fy"]), [])

    selected_snapshot_facts = _select_best_snapshot_facts(parsed_rows)
    selected_share_facts = _select_best_share_period_facts(parsed_rows)
    selected_flow_facts = _select_best_flow_facts(parsed_rows)

    provenance: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = defaultdict(dict)
    _add_flow_provenance(provenance, selected_flow_facts)
    _add_snapshot_provenance(provenance, selected_snapshot_facts)
    _add_share_provenance(provenance, selected_share_facts)
    _add_total_debt_provenance(provenance, selected_snapshot_facts)
    return {row_key: dict(field_map) for row_key, field_map in provenance.items()}


def _parse_sec_fact_rows(raw_fact_rows: Sequence[Mapping[str, Any] | sqlite3.Row]) -> list[dict[str, Any]]:
    parsed_rows = []
    for raw_row in raw_fact_rows:
        row = _row_to_dict(raw_row)
        parsed = parse_sec_field_name(str(row.get("field_name")))
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
                "ticker": _normalize_ticker(row.get("ticker")),
                "statement_type": str(row.get("statement_type")),
                "period_end_date": str(row.get("period_end_date")),
                "field_value": row.get("field_value"),
                "currency": row.get("currency"),
                "encoded_field_name": str(row.get("field_name")),
                "source": row.get("source", "sec_edgar"),
                "retrieved_at_utc": row.get("retrieved_at_utc"),
                "run_id": row.get("run_id"),
                **parsed,
            }
        )
    return parsed_rows


def _add_flow_provenance(
    provenance: dict[tuple[str, str], dict[str, list[dict[str, Any]]]],
    selected_flow_facts: dict[tuple[str, str, str, str, str, str], dict[str, Any]],
) -> None:
    flow_fields = (
        "Total Revenue",
        "Gross Profit",
        "Operating Income",
        "Net Income",
        "Operating Cash Flow",
        "Capital Expenditure",
    )
    for reconstructed_field in flow_fields:
        normalized_field = RECONSTRUCTED_FIELD_TO_NORMALIZED_FIELD[reconstructed_field]
        tag_priority = FIELD_TAG_PRIORITY[reconstructed_field]
        available_units = sorted(
            {row["currency"] for row in selected_flow_facts.values() if row["tag"] in tag_priority}
        )
        if not available_units:
            continue
        unit = _select_unit(reconstructed_field, available_units)
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
                field_provenance = _build_flow_provenance_for_field(
                    selected_flow_facts=selected_flow_facts,
                    tag_priority=tag_priority,
                    unit=unit,
                    fy=fy,
                    chain_key=chain_key,
                )
                for fp, selected in field_provenance.items():
                    if selected["value"] is None:
                        continue
                    row_key = (
                        _require_text(selected["ticker"], "ticker"),
                        _require_text(selected["period_end_date"], "period_end_date"),
                    )
                    provenance[row_key][normalized_field] = _dedupe_and_sort_facts(selected["facts"])


def _build_flow_provenance_for_field(
    *,
    selected_flow_facts: dict[tuple[str, str, str, str, str, str], dict[str, Any]],
    tag_priority: list[str],
    unit: str,
    fy: str,
    chain_key: str,
) -> dict[str, dict[str, Any]]:
    quarter_order = ("Q1", "Q2", "Q3", "FY")
    quarter_values_by_tag = {
        tag: _build_quarter_provenance_for_tag(selected_flow_facts, tag, unit, fy, chain_key)
        for tag in tag_priority
    }
    selected_by_quarter: dict[str, dict[str, Any]] = {}
    for fp in quarter_order:
        for tag in tag_priority:
            selected = quarter_values_by_tag[tag].get(fp)
            if selected is not None:
                selected_by_quarter[fp] = selected
                break
    return selected_by_quarter


def _build_quarter_provenance_for_tag(
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
        quarter_values["Q1"] = _selected_flow_value(quarterly_q1, _row_value(quarterly_q1), [quarterly_q1])
    elif ytd_q1 is not None and _row_value(ytd_q1) is not None:
        quarter_values["Q1"] = _selected_flow_value(ytd_q1, _row_value(ytd_q1), [ytd_q1])

    if quarterly_q2 is not None and _row_value(quarterly_q2) is not None:
        quarter_values["Q2"] = _selected_flow_value(quarterly_q2, _row_value(quarterly_q2), [quarterly_q2])
    elif ytd_q2 is not None:
        q2_ytd = _row_value(ytd_q2)
        q1_baseline = _row_value(ytd_q1) if ytd_q1 is not None else _flow_selected_value(quarter_values, "Q1")
        if q2_ytd is not None and q1_baseline is not None:
            q1_facts = [ytd_q1] if ytd_q1 is not None else list(quarter_values["Q1"]["facts"])
            quarter_values["Q2"] = _selected_flow_value(ytd_q2, q2_ytd - q1_baseline, [ytd_q2, *q1_facts])

    if quarterly_q3 is not None and _row_value(quarterly_q3) is not None:
        quarter_values["Q3"] = _selected_flow_value(quarterly_q3, _row_value(quarterly_q3), [quarterly_q3])
    elif ytd_q3 is not None and ytd_q2 is not None:
        q3_ytd = _row_value(ytd_q3)
        q2_ytd = _row_value(ytd_q2)
        if q3_ytd is not None and q2_ytd is not None:
            quarter_values["Q3"] = _selected_flow_value(ytd_q3, q3_ytd - q2_ytd, [ytd_q3, ytd_q2])

    if quarterly_fy is not None and _row_value(quarterly_fy) is not None:
        quarter_values["FY"] = _selected_flow_value(quarterly_fy, _row_value(quarterly_fy), [quarterly_fy])
    elif annual_fy is not None and _row_value(annual_fy) is not None:
        q1 = _flow_selected_value(quarter_values, "Q1")
        q2 = _flow_selected_value(quarter_values, "Q2")
        q3 = _flow_selected_value(quarter_values, "Q3")
        fy_value = _row_value(annual_fy)
        if None not in (q1, q2, q3, fy_value):
            facts = [annual_fy, *quarter_values["Q1"]["facts"], *quarter_values["Q2"]["facts"], *quarter_values["Q3"]["facts"]]
            quarter_values["FY"] = _selected_flow_value(annual_fy, fy_value - q1 - q2 - q3, facts)

    return quarter_values


def _selected_flow_value(source_fact: Mapping[str, Any], value: float | None, facts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "ticker": source_fact.get("ticker"),
        "period_end_date": source_fact.get("period_end_date"),
        "value": value,
        "facts": [_fact_payload(fact) for fact in facts],
    }


def _flow_selected_value(quarter_values: Mapping[str, Mapping[str, Any]], fp: str) -> float | None:
    value = quarter_values.get(fp, {}).get("value")
    return None if value is None else float(value)


def _add_snapshot_provenance(
    provenance: dict[tuple[str, str], dict[str, list[dict[str, Any]]]],
    selected_facts: dict[tuple[str, str, str, str], dict[str, Any]],
) -> None:
    reconstructed_field = "Cash And Cash Equivalents"
    normalized_field = RECONSTRUCTED_FIELD_TO_NORMALIZED_FIELD[reconstructed_field]
    tag_priority = FIELD_TAG_PRIORITY[reconstructed_field]
    available_units = sorted({key[1] for key in selected_facts if key[0] in tag_priority})
    if not available_units:
        return
    unit = _select_unit(reconstructed_field, available_units)
    fiscal_years = sorted({key[2] for key in selected_facts if key[0] in tag_priority and key[1] == unit})
    for fy in fiscal_years:
        for fp in ("Q1", "Q2", "Q3", "FY"):
            fact = _pick_tag_fact(selected_facts, tag_priority, unit, fy, fp)
            value = _row_value(fact)
            if fact is not None and value is not None:
                row_key = (_require_text(fact.get("ticker"), "ticker"), _require_text(fact.get("period_end_date"), "period_end_date"))
                provenance[row_key][normalized_field] = [_fact_payload(fact)]


def _add_share_provenance(
    provenance: dict[tuple[str, str], dict[str, list[dict[str, Any]]]],
    selected_share_facts: dict[tuple[str, str, str], dict[str, Any]],
) -> None:
    normalized_field = "shares_outstanding"
    tag_priority = FIELD_TAG_PRIORITY["Ordinary Shares Number"]
    period_end_dates = sorted({key[0] for key in selected_share_facts})
    for period_end_date in period_end_dates:
        available_units = sorted(
            {key[2] for key in selected_share_facts if key[0] == period_end_date and key[1] in tag_priority}
        )
        if not available_units:
            continue
        unit = _select_unit("Ordinary Shares Number", available_units)
        fact = _pick_share_period_fact(selected_share_facts, tag_priority, period_end_date, unit)
        value = _row_value(fact)
        if fact is not None and value is not None:
            row_key = (_require_text(fact.get("ticker"), "ticker"), _require_text(fact.get("period_end_date"), "period_end_date"))
            provenance[row_key][normalized_field] = [_fact_payload(fact)]


def _add_total_debt_provenance(
    provenance: dict[tuple[str, str], dict[str, list[dict[str, Any]]]],
    selected_facts: dict[tuple[str, str, str, str], dict[str, Any]],
) -> None:
    debt_tags = {tag for group in DEBT_GROUPS for tag in group}
    available_units = sorted({key[1] for key in selected_facts if key[0] in debt_tags})
    if not available_units:
        return
    unit = _select_unit("Total Debt", available_units)
    fiscal_years = sorted({key[2] for key in selected_facts if key[0] in debt_tags and key[1] == unit})
    for fy in fiscal_years:
        for fp in ("Q1", "Q2", "Q3", "FY"):
            for tag_group in DEBT_GROUPS:
                facts = [_pick_specific_fact(selected_facts, tag, unit, fy, fp) for tag in tag_group]
                present = [fact for fact in facts if fact is not None and _row_value(fact) is not None]
                if not present:
                    continue
                row_key = (
                    _require_text(present[0].get("ticker"), "ticker"),
                    _require_text(present[0].get("period_end_date"), "period_end_date"),
                )
                provenance[row_key]["total_debt"] = _dedupe_and_sort_facts([_fact_payload(fact) for fact in present])
                break


def _add_free_cashflow_provenance(
    by_key: dict[tuple[str, str], dict[str, list[dict[str, Any]]]],
) -> None:
    for field_map in by_key.values():
        operating_facts = field_map.get("operating_cashflow", [])
        capex_facts = field_map.get("capex", [])
        if operating_facts and capex_facts:
            field_map["free_cashflow"] = _dedupe_and_sort_facts([*operating_facts, *capex_facts])


def _fact_payload(fact: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ticker": _normalize_ticker(fact.get("ticker")),
        "statement_type": fact.get("statement_type") or FIELD_TO_STATEMENT_TYPE.get(str(fact.get("field_name"))),
        "period_end_date": fact.get("period_end_date"),
        "field_name": fact.get("field_name", fact.get("encoded_field_name")),
        "field_value": fact.get("field_value"),
        "currency": fact.get("currency"),
        "source": fact.get("source", "sec_edgar"),
        "retrieved_at_utc": fact.get("retrieved_at_utc"),
        "run_id": fact.get("run_id"),
    }


def _dedupe_and_sort_facts(facts: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    unique = {_fact_identity(fact): dict(fact) for fact in facts}
    return [unique[key] for key in sorted(unique)]


def _fact_identity(fact: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(fact.get("ticker")),
        str(fact.get("statement_type")),
        str(fact.get("period_end_date")),
        str(fact.get("field_name", fact.get("encoded_field_name"))),
        str(fact.get("field_value")),
    )


def _row_to_dict(row: Mapping[str, Any] | sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _normalize_ticker(value: Any) -> str | None:
    if value is None:
        return None
    ticker = str(value).strip().upper()
    if not ticker:
        return None
    return ticker


def _require_text(value: Any, field_name: str) -> str:
    if value is None or not str(value).strip():
        raise ValueError(f"SEC_RECONSTRUCTION_PROVENANCE_REQUIRED_FIELD_MISSING:{field_name}")
    return str(value).strip()
