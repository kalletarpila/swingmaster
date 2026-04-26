from __future__ import annotations

import argparse

from swingmaster.fundamentals.sec_edgar import (
    SEC_TAGS,
    SEC_USER_AGENT,
    fetch_companyfacts,
    inspect_companyfacts_tags,
    resolve_cik,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect SEC EDGAR companyfacts availability for selected US-GAAP tags")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--user-agent", default=SEC_USER_AGENT, help="Optional SEC User-Agent header")
    return parser.parse_args()


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def main() -> None:
    args = parse_args()
    ticker = args.ticker.upper()
    cik = resolve_cik(ticker, args.user_agent)
    companyfacts = fetch_companyfacts(cik, args.user_agent)
    inspections = inspect_companyfacts_tags(companyfacts, SEC_TAGS)

    print("SEC EDGAR FUNDAMENTAL DIAGNOSTIC")
    print(f"ticker={ticker}")
    print(f"cik={cik}")
    for item in inspections:
        print(
            " ".join(
                [
                    f"tag={item['tag']}",
                    f"found={str(item['found']).lower()}",
                    f"units={item['unit_count']}",
                    f"facts={item['fact_count']}",
                    f"forms_10q={item['form_count_10q']}",
                    f"forms_10k={item['form_count_10k']}",
                    f"first_end={item['first_end_date'] if item['first_end_date'] is not None else 'NULL'}",
                    f"last_end={item['last_end_date'] if item['last_end_date'] is not None else 'NULL'}",
                ]
            )
        )

    _summary(ticker=ticker)
    _summary(cik=cik)
    _summary(tags_found=sum(1 for item in inspections if item["found"]))
    _summary(tags_missing=sum(1 for item in inspections if not item["found"]))
    _summary(total_facts=sum(int(item["fact_count"]) for item in inspections))
    _summary(total_10q_facts=sum(int(item["form_count_10q"]) for item in inspections))
    _summary(total_10k_facts=sum(int(item["form_count_10k"]) for item in inspections))
    _summary(status="ok")


if __name__ == "__main__":
    main()
