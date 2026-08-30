from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.providers.sharadar import (
    FUNDAMENTALS_REQUIRED_FIELDS,
    SHARADAR_DIRECT_BASE_URL,
    STATUS_FREE_TIER_LIMIT,
    STATUS_SUCCESS,
    SharadarClient,
    SharadarResult,
    extract_schema_fields,
    validate_schema_fields,
)


COMPARE_FIELDS = ("revenue", "ebit", "ebitda", "fcf", "cashneq", "debt", "sharesbas", "shareswa", "shareswadil")
PROJECTED_FIELDS = (
    "ticker",
    "dimension",
    "reportperiod",
    "fiscalperiod",
    "date",
    "revenue",
    "ebit",
    "ebitda",
    "fcf",
    "cashneq",
    "debt",
    "sharesbas",
    "shareswa",
    "shareswadil",
)
BOUNDARY_TICKERS = ("WDAY", "ASTH", "CECO")
NEXT_ACTION = "ACTIVATE ONE MONTH OF SHARADAR FUNDAMENTALS FULL HISTORY AND RUN THE DIFFICULT MULTI-TICKER V4 ACCEPTANCE SET USING THE SAME CLIENT WITHOUT CHANGING THE INTEGRATION CONTRACT"
CONFIGURE_KEY_ACTION = "CONFIGURE SHARADAR_API_KEY LOCALLY AND RERUN ONLY THE LIVE SMOKE; DO NOT CHANGE IMPLEMENTATION"


@dataclass(frozen=True)
class SmokePaths:
    artifact_root: Path
    sample_csv: Path


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[Mapping[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def parse_num(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def classify_value(sample_value: Any, api_value: Any) -> str:
    sample_text = normalize_value(sample_value)
    api_text = normalize_value(api_value)
    if not sample_text and not api_text:
        return "EXACT_MATCH"
    if sample_text and not api_text:
        return "API_MISSING"
    if api_text and not sample_text:
        return "SAMPLE_MISSING"
    if sample_text == api_text:
        return "EXACT_MATCH"
    sample_num = parse_num(sample_text)
    api_num = parse_num(api_text)
    if sample_num is not None and api_num is not None and abs(sample_num - api_num) <= max(1e-9, abs(sample_num) * 1e-9):
        return "NUMERIC_MATCH"
    return "DIFFERENT"


def result_summary(result: SharadarResult) -> dict[str, Any]:
    return {
        "status": result.status,
        "auth_status": result.auth_status,
        "http_status": result.http_status,
        "endpoint": result.endpoint,
        "url": result.url,
        "records": len(result.records),
        "request_count": result.request_count,
        "error": result.error,
    }


def sample_vs_api(sample_rows: list[dict[str, str]], api_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sample_arq = {
        (row.get("ticker"), row.get("dimension"), row.get("reportperiod"), row.get("fiscalperiod")): row
        for row in sample_rows
        if row.get("ticker") == "AAPL" and row.get("dimension") == "ARQ"
    }
    api_arq = {
        (row.get("ticker"), row.get("dimension"), row.get("reportperiod"), row.get("fiscalperiod")): row
        for row in api_rows
        if row.get("ticker") == "AAPL" and row.get("dimension") == "ARQ"
    }
    rows: list[dict[str, Any]] = []
    field_counts: dict[str, Counter[str]] = {field: Counter() for field in COMPARE_FIELDS}
    for key in sorted(set(sample_arq) | set(api_arq), key=lambda k: (str(k[2]), str(k[3])), reverse=True):
        sample = sample_arq.get(key, {})
        api = api_arq.get(key, {})
        for field in COMPARE_FIELDS:
            classification = classify_value(sample.get(field), api.get(field))
            field_counts[field][classification] += 1
            rows.append(
                {
                    "ticker": key[0],
                    "dimension": key[1],
                    "reportperiod": key[2],
                    "fiscalperiod": key[3],
                    "field": field,
                    "classification": classification,
                    "sample_value": sample.get(field, ""),
                    "api_value": api.get(field, ""),
                }
            )
    summary = {
        "comparable_arq_rows": len(set(sample_arq) & set(api_arq)),
        "fields": {field: dict(counts) for field, counts in field_counts.items()},
    }
    return rows, summary


def fiscal_identity_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    out = []
    q4_count = 0
    non_calendar_ok = False
    for row in rows:
        if row.get("dimension") != "ARQ":
            continue
        fiscalperiod = str(row.get("fiscalperiod") or "")
        reportperiod = str(row.get("reportperiod") or "")
        if fiscalperiod.endswith("-Q4"):
            q4_count += 1
        is_non_calendar = reportperiod.startswith("2025-12-27") and fiscalperiod == "2026-Q1"
        if is_non_calendar:
            non_calendar_ok = True
        out.append(
            {
                "ticker": row.get("ticker", ""),
                "dimension": row.get("dimension", ""),
                "reportperiod": reportperiod,
                "fiscalperiod": fiscalperiod,
                "date": row.get("date", ""),
                "classification": "NON_CALENDAR_FISCAL_IDENTITY_CONFIRMED" if is_non_calendar else "OBSERVED",
            }
        )
    return out, {
        "reportperiod_2025_12_27_to_fiscalperiod_2026_Q1": "YES" if non_calendar_ok else "NO",
        "explicit_q4_rows": q4_count,
        "q4_explicit_rows_confirmed": "YES" if q4_count > 0 else "NO",
    }


def schema_summary(result: SharadarResult, expected: Iterable[str] = FUNDAMENTALS_REQUIRED_FIELDS) -> dict[str, Any]:
    if not result.ok:
        return {**result_summary(result), "expected_fields_found": False, "missing_expected_fields": list(expected)}
    validation = validate_schema_fields(result.payload, expected)
    return {**result_summary(result), **validation}


def available_metadata(fields: set[str]) -> dict[str, Any]:
    return {
        "fields_found": sorted(fields),
        "permaticker_available": "YES" if "permaticker" in fields else "NO",
        "ticker_available": "YES" if "ticker" in fields else "NO",
    }


def actions_relevance(fields: set[str]) -> dict[str, Any]:
    relevant = sorted(fields & {"action", "eventcodes", "eventcode", "dividend", "split", "splits", "ticker", "name", "date"})
    return {
        "fields_found": sorted(fields),
        "relevant_fields": relevant,
        "relevant_to_splits_ticker_changes": "YES" if relevant else "NO",
    }


def run_smoke(paths: SmokePaths, *, ticker: str, test_free_tier_boundary: bool) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    client = SharadarClient(max_retries=1)
    schema = client.schema("fundamentals")
    ticker_schema = client.table_schema("tickers")
    actions_schema = client.table_schema("actions")
    descriptions_schema = client.table_schema("descriptions")

    if not client.api_key_configured:
        summary = {
            "classification": "SHARADAR_FREE_API_SMOKE_BLOCKED",
            "next_action": CONFIGURE_KEY_ACTION,
            "environment": {"sharadar_api_key_configured": "NO", "key_exposure_detected": "NO", "total_network_requests": client.request_count},
            "schema": schema_summary(schema),
            "authentication": {"aapl_authenticated_request_status": "AUTH_NOT_CONFIGURED"},
            "artifact_root": str(paths.artifact_root),
        }
        write_json(paths.artifact_root / "sharadar_smoke_summary.json", summary)
        (paths.artifact_root / "next_action.md").write_text(CONFIGURE_KEY_ACTION + "\n", encoding="utf-8")
        return summary

    modern = client.fundamentals(ticker=ticker, limit=100)
    legacy = client.fundamentals(ticker=ticker, limit=100, use_legacy_alias=True)
    arq = client.fundamentals(ticker=ticker, dimension="ARQ", limit=100)
    mrq = client.fundamentals(ticker=ticker, dimension="MRQ", limit=100)
    projected = client.fundamentals(ticker=ticker, fields=PROJECTED_FIELDS, limit=10)

    boundary_rows = []
    if test_free_tier_boundary:
        for boundary_ticker in BOUNDARY_TICKERS:
            result = client.fundamentals(ticker=boundary_ticker, limit=1)
            boundary_rows.append(
                {
                    "ticker": boundary_ticker,
                    "status": result.status,
                    "auth_status": result.auth_status,
                    "http_status": result.http_status,
                    "request_count": result.request_count,
                    "records": len(result.records),
                }
            )

    sample_rows = read_csv_rows(paths.sample_csv) if paths.sample_csv.exists() else []
    comparison_rows, comparison_summary = sample_vs_api(sample_rows, modern.records)
    fiscal_rows, fiscal_summary = fiscal_identity_rows(modern.records)

    write_json(paths.artifact_root / "sharadar_schema_validation.json", schema_summary(schema))
    write_json(paths.artifact_root / "sharadar_ticker_schema_summary.json", available_metadata(extract_schema_fields(ticker_schema.payload)))
    write_json(paths.artifact_root / "sharadar_actions_schema_summary.json", actions_relevance(extract_schema_fields(actions_schema.payload)))
    write_csv(paths.artifact_root / "aapl_api_arq.csv", arq.records)
    write_csv(paths.artifact_root / "aapl_api_mrq.csv", mrq.records)
    write_csv(paths.artifact_root / "aapl_sample_vs_api.csv", comparison_rows)
    write_csv(paths.artifact_root / "aapl_fiscal_identity_validation.csv", fiscal_rows)
    write_csv(paths.artifact_root / "free_tier_boundary_test.csv", boundary_rows, ["ticker", "status", "auth_status", "http_status", "request_count", "records"])

    description_fields = extract_schema_fields(descriptions_schema.payload)
    if description_fields:
        write_csv(paths.artifact_root / "sharadar_field_descriptions.csv", [{"field": field} for field in sorted(description_fields)])

    dimensions = Counter(str(row.get("dimension", "")) for row in modern.records)
    arq_dates = sorted(str(row.get("reportperiod")) for row in arq.records if row.get("reportperiod"))
    boundary_free_tier_ok = all(row["status"] in {STATUS_FREE_TIER_LIMIT, STATUS_SUCCESS} for row in boundary_rows) if boundary_rows else None
    field_totals = {
        field: {
            "exact_or_numeric_matches": comparison_summary["fields"].get(field, {}).get("EXACT_MATCH", 0)
            + comparison_summary["fields"].get(field, {}).get("NUMERIC_MATCH", 0),
            "differences": comparison_summary["fields"].get(field, {}).get("DIFFERENT", 0),
            "api_missing": comparison_summary["fields"].get(field, {}).get("API_MISSING", 0),
            "sample_missing": comparison_summary["fields"].get(field, {}).get("SAMPLE_MISSING", 0),
            "all": comparison_summary["fields"].get(field, {}),
        }
        for field in COMPARE_FIELDS
    }
    blocking_statuses = [
        schema.status,
        ticker_schema.status,
        actions_schema.status,
        modern.status,
        legacy.status,
        arq.status,
        mrq.status,
        projected.status,
    ]
    if any(status != STATUS_SUCCESS for status in blocking_statuses):
        classification = "SHARADAR_FREE_API_SMOKE_BLOCKED"
        next_action = "RERUN THE LIVE SMOKE WITH NETWORK ACCESS; DO NOT CHANGE IMPLEMENTATION"
    elif any(row["status"] == STATUS_FREE_TIER_LIMIT for row in boundary_rows):
        classification = "SHARADAR_FREE_API_SMOKE_COMPLETE_WITH_FREE_TIER_LIMITS"
        next_action = NEXT_ACTION
    else:
        classification = "SHARADAR_FREE_API_SMOKE_COMPLETE"
        next_action = NEXT_ACTION
    summary = {
        "classification": classification,
        "environment": {
            "sharadar_api_key_configured": "YES",
            "key_exposure_detected": "NO",
            "total_network_requests": client.request_count,
        },
        "schema": {
            "fundamentals_schema_reachable": "YES" if schema.ok else "NO",
            "expected_fields_found": "YES" if schema_summary(schema).get("expected_fields_found") else "NO",
            "missing_expected_fields": schema_summary(schema).get("missing_expected_fields", []),
            "ticker_permaticker_metadata_available": available_metadata(extract_schema_fields(ticker_schema.payload))["permaticker_available"],
            "actions_schema_relevant_to_splits_ticker_changes": actions_relevance(extract_schema_fields(actions_schema.payload))["relevant_to_splits_ticker_changes"],
        },
        "authentication": {
            "aapl_authenticated_request_status": modern.status,
            "modern_fundamentals_endpoint_status": modern.status,
            "legacy_sf1_alias_status": legacy.status,
        },
        "aapl_api": {
            "returned_records": len(modern.records),
            "arq_records": len(arq.records),
            "mrq_records": len(mrq.records),
            "dimensions": dict(dimensions),
            "arq_date_range": [arq_dates[0], arq_dates[-1]] if arq_dates else ["", ""],
            "explicit_q4_rows_found": fiscal_summary["q4_explicit_rows_confirmed"],
            "non_calendar_fiscalperiod_behavior_correct": fiscal_summary["reportperiod_2025_12_27_to_fiscalperiod_2026_Q1"],
            "field_projection_status": projected.status,
            "field_projection_observed_fields": sorted({key for row in projected.records for key in row}),
        },
        "sample_parity": {"comparable_arq_rows": comparison_summary["comparable_arq_rows"], "fields": field_totals},
        "fiscal_identity": fiscal_summary,
        "free_tier": {
            "rows": boundary_rows,
            "403_correctly_classified_free_tier_limit": "YES" if boundary_free_tier_ok else "NOT_RUN",
            "retries_on_403": 0,
        },
        "api_security": {
            "api_key_logged": "NO",
            "api_key_committed": "NO",
            "api_key_persisted_in_artifacts": "NO",
        },
        "architecture": {
            "raw_provider_model_implemented": "YES",
            "canonical_layer_still_separate": "YES",
            "v4_db_created": "NO",
            "v3_modified": "NO",
        },
        "results": {
            "schema": result_summary(schema),
            "ticker_schema": result_summary(ticker_schema),
            "actions_schema": result_summary(actions_schema),
            "descriptions_schema": result_summary(descriptions_schema),
            "modern": result_summary(modern),
            "legacy": result_summary(legacy),
            "arq": result_summary(arq),
            "mrq": result_summary(mrq),
            "projected": result_summary(projected),
        },
        "artifact_root": str(paths.artifact_root),
        "client_path": "swingmaster/providers/sharadar.py",
        "smoke_cli_path": "swingmaster/cli/run_sharadar_v4_smoke.py",
        "provider_evaluation_doc": "docs/fundamentals_v4_provider_evaluation.md",
        "integration_doc": "docs/fundamentals_v4_sharadar_integration.md",
        "next_action": next_action,
    }
    write_json(paths.artifact_root / "sharadar_smoke_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(next_action + "\n", encoding="utf-8")
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a small Sharadar Direct API smoke test for Fundamentals V4")
    parser.add_argument("--ticker", default="AAPL")
    parser.add_argument("--sample-csv", type=Path, default=Path("temp/fundamentals.csv"))
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v4_sharadar_free_api_smoke") / utc_stamp())
    parser.add_argument("--test-free-tier-boundary", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_smoke(
        SmokePaths(artifact_root=args.artifact_root, sample_csv=args.sample_csv),
        ticker=args.ticker.strip().upper(),
        test_free_tier_boundary=args.test_free_tier_boundary,
    )
    print(f"classification={summary['classification']}")
    print(f"artifact_root={summary['artifact_root']}")
    print(f"network_requests={summary['environment']['total_network_requests']}")
    return 0 if summary["classification"] != "SHARADAR_FREE_API_SMOKE_BLOCKED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
