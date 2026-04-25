from __future__ import annotations

import gzip
import json
from typing import Any
from urllib.request import Request, urlopen


SEC_USER_AGENT = "SwingMaster fundamentals research contact@example.com"
SEC_TICKER_CIK_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANYFACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SEC_TAGS = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "GrossProfit",
    "OperatingIncomeLoss",
    "NetIncomeLoss",
    "EBITDA",
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets",
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    "AssetsCurrent",
    "LiabilitiesCurrent",
    "ShortTermBorrowings",
    "LongTermDebtCurrent",
    "LongTermDebtNoncurrent",
    "LongTermDebtAndFinanceLeaseObligationsCurrent",
    "LongTermDebtAndFinanceLeaseObligationsNoncurrent",
    "EntityCommonStockSharesOutstanding",
    "WeightedAverageNumberOfDilutedSharesOutstanding",
    "WeightedAverageNumberOfSharesOutstandingBasic",
]
SEC_STATEMENT_TYPE_BY_TAG = {
    "Revenues": "income",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "income",
    "GrossProfit": "income",
    "OperatingIncomeLoss": "income",
    "NetIncomeLoss": "income",
    "EBITDA": "income",
    "NetCashProvidedByUsedInOperatingActivities": "cashflow",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": "cashflow",
    "PaymentsToAcquirePropertyPlantAndEquipment": "cashflow",
    "PaymentsToAcquireProductiveAssets": "cashflow",
    "CashAndCashEquivalentsAtCarryingValue": "balance",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents": "balance",
    "AssetsCurrent": "balance",
    "LiabilitiesCurrent": "balance",
    "ShortTermBorrowings": "balance",
    "LongTermDebtCurrent": "balance",
    "LongTermDebtNoncurrent": "balance",
    "LongTermDebtAndFinanceLeaseObligationsCurrent": "balance",
    "LongTermDebtAndFinanceLeaseObligationsNoncurrent": "balance",
    "EntityCommonStockSharesOutstanding": "balance",
    "WeightedAverageNumberOfDilutedSharesOutstanding": "balance",
    "WeightedAverageNumberOfSharesOutstandingBasic": "balance",
}


def fetch_json(url: str, user_agent: str) -> dict:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        },
    )
    try:
        with urlopen(request) as response:
            payload = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                payload = gzip.decompress(payload)
            return json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"SEC_FETCH_FAILED:{url}:{type(exc).__name__}:{exc}") from exc


def load_ticker_cik_map(user_agent: str) -> dict[str, str]:
    payload = fetch_json(SEC_TICKER_CIK_URL, user_agent)
    ticker_map: dict[str, str] = {}
    for row in payload.values():
        ticker = str(row["ticker"]).upper()
        cik = str(row["cik_str"]).zfill(10)
        ticker_map[ticker] = cik
    return ticker_map


def resolve_cik(ticker: str, user_agent: str) -> str:
    normalized_ticker = ticker.upper()
    ticker_map = load_ticker_cik_map(user_agent)
    if normalized_ticker not in ticker_map:
        raise RuntimeError(f"SEC_TICKER_NOT_FOUND:{normalized_ticker}")
    return ticker_map[normalized_ticker]


def fetch_companyfacts(cik: str, user_agent: str) -> dict:
    url = SEC_COMPANYFACTS_URL_TEMPLATE.format(cik=cik)
    return fetch_json(url, user_agent)


def inspect_companyfacts_tags(companyfacts: dict, tags: list[str]) -> list[dict]:
    us_gaap_facts = companyfacts.get("facts", {}).get("us-gaap", {})
    inspections: list[dict] = []
    for tag in tags:
        tag_payload = us_gaap_facts.get(tag)
        if tag_payload is None:
            inspections.append(
                {
                    "tag": tag,
                    "namespace": "us-gaap",
                    "found": False,
                    "unit_count": 0,
                    "fact_count": 0,
                    "form_count_10q": 0,
                    "form_count_10k": 0,
                    "first_end_date": None,
                    "last_end_date": None,
                }
            )
            continue

        units = tag_payload.get("units", {})
        facts = [fact for unit_facts in units.values() for fact in unit_facts]
        end_dates = sorted(fact["end"] for fact in facts if "end" in fact)
        inspections.append(
            {
                "tag": tag,
                "namespace": "us-gaap",
                "found": True,
                "unit_count": len(units),
                "fact_count": len(facts),
                "form_count_10q": sum(1 for fact in facts if fact.get("form") == "10-Q"),
                "form_count_10k": sum(1 for fact in facts if fact.get("form") == "10-K"),
                "first_end_date": end_dates[0] if end_dates else None,
                "last_end_date": end_dates[-1] if end_dates else None,
            }
        )
    return inspections


def extract_companyfacts_raw_rows(
    ticker: str,
    companyfacts: dict,
    run_id: str,
    retrieved_at_utc: str,
) -> list[dict[str, Any]]:
    us_gaap_facts = companyfacts.get("facts", {}).get("us-gaap", {})
    extracted_rows: list[dict[str, Any]] = []
    normalized_ticker = ticker.upper()

    for tag in SEC_TAGS:
        tag_payload = us_gaap_facts.get(tag)
        if tag_payload is None:
            continue
        units = tag_payload.get("units", {})
        for unit in sorted(units):
            facts = sorted(
                units[unit],
                key=lambda fact: (
                    str(fact.get("end", "")),
                    str(fact.get("form", "")),
                    str(fact.get("fy", "")),
                    str(fact.get("fp", "")),
                    str(fact.get("frame", "")),
                    str(fact.get("start", "")),
                    str(fact.get("filed", "")),
                ),
            )
            for fact in facts:
                form = fact.get("form")
                end = fact.get("end")
                if form not in ("10-Q", "10-K") or end is None:
                    continue
                extracted_rows.append(
                    {
                        "ticker": normalized_ticker,
                        "statement_type": SEC_STATEMENT_TYPE_BY_TAG[tag],
                        "period_end_date": str(end),
                        "period_type": "sec_fact",
                        "field_name": _build_sec_field_name(tag, unit, fact),
                        "field_value": _normalize_sec_field_value(fact.get("val")),
                        "currency": unit,
                        "source": "sec_edgar",
                        "retrieved_at_utc": retrieved_at_utc,
                        "run_id": run_id,
                    }
                )

    return extracted_rows


def _build_sec_field_name(tag: str, unit: str, fact: dict[str, Any]) -> str:
    return (
        f"{tag}"
        f"|form={_fact_component(fact.get('form'))}"
        f"|unit={_fact_component(unit)}"
        f"|fy={_fact_component(fact.get('fy'))}"
        f"|fp={_fact_component(fact.get('fp'))}"
        f"|frame={_fact_component(fact.get('frame'))}"
        f"|start={_fact_component(fact.get('start'))}"
        f"|filed={_fact_component(fact.get('filed'))}"
    )


def _fact_component(value: Any) -> str:
    if value is None:
        return "NULL"
    return str(value)


def _normalize_sec_field_value(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
