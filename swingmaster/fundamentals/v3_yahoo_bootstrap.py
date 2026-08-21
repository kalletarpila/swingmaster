from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_yahoo_audit import (
    build_audit_row,
    payload_has_usable_statement_data,
)
from swingmaster.cli.run_fundamental_yahoo_quarterly_prototype import build_normalized_rows, should_persist_row
from swingmaster.fundamentals.providers.yahoo import YahooFinanceClient
from swingmaster.fundamentals.v3_helpers import make_v3_work_unit_key, normalize_market, normalize_ticker
from swingmaster.fundamentals.v3_repositories import V3RawCacheRepository, configure_connection, utc_now_text


YAHOO_PROVIDER = "YAHOO"
YAHOO_BOOTSTRAP_DERIVATION_METHOD = "YAHOO_DIRECT_OR_PROVIDER_NORMALIZED"
YAHOO_BOOTSTRAP_CANDIDATE_VERSION = "fundamentals_v3_yahoo_bootstrap_candidate_v1"
YAHOO_BOOTSTRAP_DEFAULT_DELAY_SECONDS = 0.5
V3_YAHOO_VALUE_FIELDS = (
    "revenue",
    "gross_profit",
    "ebit",
    "ebitda",
    "net_income",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)


@dataclass(frozen=True)
class ApprovedV3Company:
    company_id: int
    market: str
    ticker: str
    provider_symbol: str


@dataclass(frozen=True)
class YahooRawCacheResult:
    company: ApprovedV3Company
    fetch_run_id: str
    payload_hash: str
    payload_json: str
    status: str
    error_message: str | None
    observed_at_utc: str


@dataclass(frozen=True)
class YahooNormalizedQuarter:
    company: ApprovedV3Company
    fetch_run_id: str
    payload_hash: str
    period_end_date: str
    values: dict[str, float]
    provider_details: dict[str, Any]


@dataclass(frozen=True)
class QuarterMetadata:
    fiscal_year: int
    fiscal_quarter: str
    period_end_date: str
    publish_date: str
    market_availability_date: str
    fiscal_identity_source: str
    publish_date_source: str


@dataclass(frozen=True)
class V3YahooMigrationCandidate:
    company_id: int
    market: str
    ticker: str
    provider_symbol: str
    fiscal_year: int
    fiscal_quarter: str
    period_end_date: str
    publish_date: str
    market_availability_date: str
    values: dict[str, float]
    provider_details: dict[str, Any]
    source_provider: str
    provider_cache_ref: str
    fetch_run_id: str
    payload_hash: str
    work_unit_key: str
    candidate_key: str
    derivation_method: str
    candidate_version: str


@dataclass(frozen=True)
class YahooCandidateRejection:
    market: str
    ticker: str
    provider_symbol: str
    period_end_date: str | None
    reason: str
    details: dict[str, Any]


class _RawRowAdapter:
    def __init__(self, row: Mapping[str, Any]) -> None:
        self._row = row

    def __getitem__(self, key: str) -> Any:
        return self._row[key]


def load_approved_v3_companies(conn: sqlite3.Connection, *, market: str = "usa", limit: int | None = None) -> list[ApprovedV3Company]:
    conn = configure_connection(conn)
    params: list[Any] = [normalize_market(market)]
    limit_clause = ""
    if limit is not None:
        if limit < 0:
            raise ValueError(f"V3_YAHOO_INVALID_LIMIT:{limit}")
        limit_clause = "LIMIT ?"
        params.append(int(limit))
    rows = conn.execute(
        f"""
        SELECT c.company_id, c.market, c.ticker, COALESCE(a.provider_symbol, c.ticker) AS provider_symbol
        FROM v3_company c
        LEFT JOIN v3_provider_symbol_alias a
          ON a.company_id = c.company_id
         AND a.provider = 'YAHOO'
        WHERE c.market = ?
          AND c.active = 1
        ORDER BY c.ticker
        {limit_clause}
        """,
        tuple(params),
    ).fetchall()
    return [
        ApprovedV3Company(
            company_id=int(row["company_id"]),
            market=str(row["market"]),
            ticker=str(row["ticker"]),
            provider_symbol=str(row["provider_symbol"]),
        )
        for row in rows
    ]


def select_approved_v3_yahoo_companies(
    conn: sqlite3.Connection,
    *,
    market: str = "usa",
    ticker: str | None = None,
    tickers: str | None = None,
    limit: int | None = None,
) -> list[ApprovedV3Company]:
    return _select_companies(
        load_approved_v3_companies(conn, market=market, limit=limit),
        ticker=ticker,
        tickers=tickers,
    )


def fetch_yahoo_to_v3_raw_cache(
    company: ApprovedV3Company,
    *,
    raw_cache_repo: V3RawCacheRepository,
    fetch_run_id: str,
    client: Any | None = None,
    observed_at_utc: str | None = None,
) -> YahooRawCacheResult:
    result = build_yahoo_raw_cache_result(
        company,
        fetch_run_id=fetch_run_id,
        client=client,
        observed_at_utc=observed_at_utc,
    )
    payload_hash = raw_cache_repo.put_payload(
        provider=YAHOO_PROVIDER,
        provider_symbol=company.provider_symbol,
        fetch_run_id=fetch_run_id,
        payload_json=result.payload_json,
        status=result.status,
        error_message=result.error_message,
        observed_at_utc=result.observed_at_utc,
    )
    if payload_hash == result.payload_hash:
        return result
    return YahooRawCacheResult(
        company=result.company,
        fetch_run_id=result.fetch_run_id,
        payload_hash=payload_hash,
        payload_json=result.payload_json,
        status=result.status,
        error_message=result.error_message,
        observed_at_utc=result.observed_at_utc,
    )


def build_yahoo_raw_cache_result(
    company: ApprovedV3Company,
    *,
    fetch_run_id: str,
    client: Any | None = None,
    observed_at_utc: str | None = None,
) -> YahooRawCacheResult:
    yahoo_client = client or YahooFinanceClient()
    observed = observed_at_utc or utc_now_text()
    try:
        payload = yahoo_client.get_raw_payload(company.provider_symbol)
        status = "OK" if payload_has_usable_statement_data(payload) else "EMPTY"
        error_message = None
    except Exception as exc:
        payload = {}
        status = "ERROR"
        error_message = str(exc)
    audit_row = build_audit_row(
        market=company.market,
        symbol=company.provider_symbol,
        payload=payload,
        status=status,
        error_message=error_message,
        loaded_at_utc=observed,
        run_id=fetch_run_id,
    )
    payload_json = _payload_json_from_audit_row(audit_row)
    return YahooRawCacheResult(
        company=company,
        fetch_run_id=fetch_run_id,
        payload_hash=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        payload_json=payload_json,
        status=status,
        error_message=error_message,
        observed_at_utc=observed,
    )


def load_yahoo_raw_cache_result(
    *,
    raw_cache_repo: V3RawCacheRepository,
    company: ApprovedV3Company,
    fetch_run_id: str | None = None,
    payload_hash: str | None = None,
) -> YahooRawCacheResult | None:
    with raw_cache_repo.connect() as conn:
        params: list[Any] = [YAHOO_PROVIDER, company.provider_symbol]
        clauses = ["provider = ?", "provider_symbol = ?"]
        if fetch_run_id is not None:
            clauses.append("fetch_run_id = ?")
            params.append(fetch_run_id)
        if payload_hash is not None:
            clauses.append("payload_hash = ?")
            params.append(payload_hash)
        row = conn.execute(
            f"""
            SELECT provider_symbol, fetch_run_id, payload_hash, payload_json,
                   status, error_message, observed_at_utc
            FROM v3_raw_cache_entry
            WHERE {" AND ".join(clauses)}
            ORDER BY raw_cache_id DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    if row is None:
        return None
    return YahooRawCacheResult(
        company=company,
        fetch_run_id=str(row["fetch_run_id"]),
        payload_hash=str(row["payload_hash"]),
        payload_json=str(row["payload_json"]),
        status=str(row["status"]),
        error_message=None if row["error_message"] is None else str(row["error_message"]),
        observed_at_utc=str(row["observed_at_utc"]),
    )


def normalize_yahoo_raw_cache_result(raw_result: YahooRawCacheResult) -> list[YahooNormalizedQuarter]:
    if raw_result.status != "OK":
        return []
    payload = json.loads(raw_result.payload_json)
    raw_row = _RawRowAdapter(
        {
            "info_json": _canonical_json(payload.get("info", {})),
            "fast_info_json": _canonical_json(payload.get("fast_info", {})),
            "quarterly_income_stmt_json": _canonical_json(payload.get("quarterly_income_stmt", _empty_statement())),
            "quarterly_balance_sheet_json": _canonical_json(payload.get("quarterly_balance_sheet", _empty_statement())),
            "quarterly_cashflow_json": _canonical_json(payload.get("quarterly_cashflow", _empty_statement())),
            "run_id": raw_result.fetch_run_id,
        }
    )
    normalized: list[YahooNormalizedQuarter] = []
    for row in build_normalized_rows(raw_row):  # type: ignore[arg-type]
        if not should_persist_row(row):
            continue
        values = {
            field_name: float(row[field_name])
            for field_name in V3_YAHOO_VALUE_FIELDS
            if row.get(field_name) is not None
        }
        normalized.append(
            YahooNormalizedQuarter(
                company=raw_result.company,
                fetch_run_id=raw_result.fetch_run_id,
                payload_hash=raw_result.payload_hash,
                period_end_date=str(row["period_end_date"]),
                values=values,
                provider_details={
                    "operating_income": row.get("operating_income"),
                    "shares_source": row.get("shares_source"),
                    "shares_quality": row.get("shares_quality"),
                },
            )
        )
    return normalized


class YahooMetadataEnricher:
    def __init__(self, *, v2_conn: sqlite3.Connection | None = None, legacy_conn: sqlite3.Connection | None = None) -> None:
        self.v2_conn = configure_connection(v2_conn) if v2_conn is not None else None
        self.legacy_conn = configure_connection(legacy_conn) if legacy_conn is not None else None

    def enrich(self, *, market: str, ticker: str, period_end_date: str) -> QuarterMetadata | None:
        identity = self._resolve_v2_identity(market=market, ticker=ticker, period_end_date=period_end_date)
        if identity is None:
            identity = self._resolve_provider_observation_identity(
                market=market,
                ticker=ticker,
                period_end_date=period_end_date,
            )
        if identity is None:
            return None

        publish = identity.get("publish_date")
        publish_source = identity.get("publish_date_source")
        market_availability = identity.get("market_availability_date")
        if not publish:
            result_event = self._resolve_result_event_publish_date(
                market=market,
                ticker=ticker,
                period_end_date=period_end_date,
            )
            if result_event is not None:
                publish = result_event["publish_date"]
                publish_source = result_event["publish_date_source"]
                market_availability = result_event["market_availability_date"]
        if not publish:
            return None

        return QuarterMetadata(
            fiscal_year=int(identity["fiscal_year"]),
            fiscal_quarter=str(identity["fiscal_quarter"]).upper(),
            period_end_date=period_end_date,
            publish_date=str(publish),
            market_availability_date=str(market_availability or publish),
            fiscal_identity_source=str(identity["fiscal_identity_source"]),
            publish_date_source=str(publish_source or identity["fiscal_identity_source"]),
        )

    def _resolve_v2_identity(self, *, market: str, ticker: str, period_end_date: str) -> dict[str, Any] | None:
        if self.v2_conn is None or not _table_exists(self.v2_conn, "rc_v2_company") or not _table_exists(self.v2_conn, "rc_v2_quarter"):
            return None
        company_columns = _table_columns(self.v2_conn, "rc_v2_company")
        market_clause = "AND LOWER(c.market) = ?" if "market" in company_columns else ""
        params: list[Any] = [normalize_ticker(ticker), period_end_date]
        if market_clause:
            params.insert(1, normalize_market(market))
        rows = self.v2_conn.execute(
            f"""
            SELECT q.fiscal_year, q.fiscal_period, q.report_date, q.publish_date
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id = c.company_id
            WHERE UPPER(c.ticker) = ?
              {market_clause}
              AND q.report_date = ?
            ORDER BY q.fiscal_year, q.fiscal_period, q.report_date
            """,
            tuple(params),
        ).fetchall()
        distinct = {
            (
                int(row["fiscal_year"]),
                str(row["fiscal_period"]).upper(),
                str(row["report_date"]),
                None if row["publish_date"] is None else str(row["publish_date"]),
            )
            for row in rows
        }
        if len(distinct) != 1:
            return None
        fiscal_year, fiscal_quarter, report_date, publish_date = next(iter(distinct))
        return {
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "period_end_date": report_date,
            "publish_date": publish_date,
            "market_availability_date": publish_date,
            "fiscal_identity_source": "V2_EXACT_REPORT_DATE",
            "publish_date_source": "V2_PUBLISH_DATE" if publish_date else None,
        }

    def _resolve_provider_observation_identity(self, *, market: str, ticker: str, period_end_date: str) -> dict[str, Any] | None:
        conn = self.legacy_conn or self.v2_conn
        if conn is None or not _table_exists(conn, "rc_fundamental_provider_observation_content"):
            return None
        rows = conn.execute(
            """
            SELECT canonical_fiscal_year, canonical_fiscal_quarter, provider_reported_at_utc
            FROM rc_fundamental_provider_observation_content
            WHERE LOWER(market) = ?
              AND UPPER(ticker) = ?
              AND period_end_date = ?
              AND canonical_fiscal_year IS NOT NULL
              AND canonical_fiscal_quarter IS NOT NULL
            ORDER BY canonical_fiscal_year, canonical_fiscal_quarter
            """,
            (normalize_market(market), normalize_ticker(ticker), period_end_date),
        ).fetchall()
        distinct = {
            (
                int(row["canonical_fiscal_year"]),
                str(row["canonical_fiscal_quarter"]).upper(),
                _date_part(row["provider_reported_at_utc"]),
            )
            for row in rows
        }
        if len(distinct) != 1:
            return None
        fiscal_year, fiscal_quarter, provider_reported_date = next(iter(distinct))
        return {
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "period_end_date": period_end_date,
            "publish_date": provider_reported_date,
            "market_availability_date": provider_reported_date,
            "fiscal_identity_source": "PROVIDER_OBSERVATION_EXACT_PERIOD_END",
            "publish_date_source": "PROVIDER_REPORTED_AT" if provider_reported_date else None,
        }

    def _resolve_result_event_publish_date(self, *, market: str, ticker: str, period_end_date: str) -> dict[str, str] | None:
        conn = self.legacy_conn or self.v2_conn
        if conn is None or not _table_exists(conn, "rc_fundamental_quarter_earnings_match"):
            return None
        rows = conn.execute(
            """
            SELECT announcement_date, effective_trading_date
            FROM rc_fundamental_quarter_earnings_match
            WHERE LOWER(market) = ?
              AND UPPER(ticker) = ?
              AND period_end_date = ?
              AND announcement_date IS NOT NULL
            ORDER BY announcement_date
            """,
            (normalize_market(market), normalize_ticker(ticker), period_end_date),
        ).fetchall()
        distinct = {(str(row["announcement_date"]), row["effective_trading_date"]) for row in rows}
        if len(distinct) != 1:
            return None
        announcement_date, effective_trading_date = next(iter(distinct))
        return {
            "publish_date": announcement_date,
            "market_availability_date": str(effective_trading_date or announcement_date),
            "publish_date_source": "RESULT_EVENT_EXACT_PERIOD_MATCH",
        }


def build_v3_yahoo_migration_candidates(
    normalized_rows: Iterable[YahooNormalizedQuarter],
    *,
    metadata_enricher: YahooMetadataEnricher,
) -> tuple[list[V3YahooMigrationCandidate], list[YahooCandidateRejection]]:
    candidates: list[V3YahooMigrationCandidate] = []
    rejections: list[YahooCandidateRejection] = []
    for row in normalized_rows:
        metadata = metadata_enricher.enrich(
            market=row.company.market,
            ticker=row.company.ticker,
            period_end_date=row.period_end_date,
        )
        if metadata is None:
            rejections.append(
                YahooCandidateRejection(
                    market=row.company.market,
                    ticker=row.company.ticker,
                    provider_symbol=row.company.provider_symbol,
                    period_end_date=row.period_end_date,
                    reason="METADATA_NOT_RESOLVED",
                    details={"required": ["fiscal_year", "fiscal_quarter", "publish_date"]},
                )
            )
            continue
        work_unit_key = make_v3_work_unit_key(
            market=row.company.market,
            ticker=row.company.ticker,
            fiscal_year=metadata.fiscal_year,
            fiscal_quarter=metadata.fiscal_quarter,
        )
        provider_cache_ref = f"v3_raw_cache_entry:YAHOO:{row.company.provider_symbol}:{row.fetch_run_id}:{row.payload_hash}"
        candidates.append(
            V3YahooMigrationCandidate(
                company_id=row.company.company_id,
                market=row.company.market,
                ticker=row.company.ticker,
                provider_symbol=row.company.provider_symbol,
                fiscal_year=metadata.fiscal_year,
                fiscal_quarter=metadata.fiscal_quarter,
                period_end_date=metadata.period_end_date,
                publish_date=metadata.publish_date,
                market_availability_date=metadata.market_availability_date,
                values=dict(row.values),
                provider_details={
                    **row.provider_details,
                    "fiscal_identity_source": metadata.fiscal_identity_source,
                    "publish_date_source": metadata.publish_date_source,
                },
                source_provider=YAHOO_PROVIDER,
                provider_cache_ref=provider_cache_ref,
                fetch_run_id=row.fetch_run_id,
                payload_hash=row.payload_hash,
                work_unit_key=work_unit_key,
                candidate_key=f"{work_unit_key}|YAHOO|{row.payload_hash}",
                derivation_method=YAHOO_BOOTSTRAP_DERIVATION_METHOD,
                candidate_version=YAHOO_BOOTSTRAP_CANDIDATE_VERSION,
            )
        )
    candidates.sort(key=lambda candidate: candidate.candidate_key)
    rejections.sort(key=lambda rejection: (rejection.market, rejection.ticker, rejection.period_end_date or ""))
    return candidates, rejections


def run_v3_yahoo_bootstrap_adapter(
    *,
    companies: Iterable[ApprovedV3Company],
    raw_cache_repo: V3RawCacheRepository,
    metadata_enricher: YahooMetadataEnricher,
    fetch_run_id: str,
    client: Any | None = None,
    observed_at_utc: str | None = None,
    delay_seconds: float = YAHOO_BOOTSTRAP_DEFAULT_DELAY_SECONDS,
    sleep_fn: Callable[[float], None] = time.sleep,
    dry_run: bool = False,
) -> dict[str, Any]:
    raw_results: list[YahooRawCacheResult] = []
    normalized_rows: list[YahooNormalizedQuarter] = []
    companies_list = list(companies)
    for index, company in enumerate(companies_list):
        if index > 0 and delay_seconds > 0:
            sleep_fn(delay_seconds)
        if dry_run:
            raw_result = build_yahoo_raw_cache_result(
                company,
                fetch_run_id=fetch_run_id,
                client=client,
                observed_at_utc=observed_at_utc,
            )
        else:
            raw_result = fetch_yahoo_to_v3_raw_cache(
                company,
                raw_cache_repo=raw_cache_repo,
                fetch_run_id=fetch_run_id,
                client=client,
                observed_at_utc=observed_at_utc,
            )
        raw_results.append(raw_result)
        normalized_rows.extend(normalize_yahoo_raw_cache_result(raw_result))
    candidates, rejections = build_v3_yahoo_migration_candidates(
        normalized_rows,
        metadata_enricher=metadata_enricher,
    )
    return {
        "fetch_run_id": fetch_run_id,
        "dry_run": 1 if dry_run else 0,
        "companies_total": len(companies_list),
        "raw_ok": sum(1 for result in raw_results if result.status == "OK"),
        "raw_empty": sum(1 for result in raw_results if result.status == "EMPTY"),
        "raw_error": sum(1 for result in raw_results if result.status == "ERROR"),
        "normalized_rows": len(normalized_rows),
        "migration_candidates": len(candidates),
        "metadata_rejections": len(rejections),
        "candidates": candidates,
        "rejections": rejections,
    }


def replay_v3_yahoo_bootstrap_from_raw_cache(
    *,
    companies: Iterable[ApprovedV3Company],
    raw_cache_repo: V3RawCacheRepository,
    metadata_enricher: YahooMetadataEnricher,
    fetch_run_id: str | None = None,
) -> dict[str, Any]:
    raw_results: list[YahooRawCacheResult] = []
    normalized_rows: list[YahooNormalizedQuarter] = []
    cache_miss_rejections: list[YahooCandidateRejection] = []
    companies_list = list(companies)
    for company in companies_list:
        raw_result = load_yahoo_raw_cache_result(
            raw_cache_repo=raw_cache_repo,
            company=company,
            fetch_run_id=fetch_run_id,
        )
        if raw_result is None:
            cache_miss_rejections.append(
                YahooCandidateRejection(
                    market=company.market,
                    ticker=company.ticker,
                    provider_symbol=company.provider_symbol,
                    period_end_date=None,
                    reason="RAW_CACHE_NOT_FOUND",
                    details={"fetch_run_id": fetch_run_id},
                )
            )
            continue
        raw_results.append(raw_result)
        normalized_rows.extend(normalize_yahoo_raw_cache_result(raw_result))
    candidates, metadata_rejections = build_v3_yahoo_migration_candidates(
        normalized_rows,
        metadata_enricher=metadata_enricher,
    )
    rejections = [*metadata_rejections, *cache_miss_rejections]
    rejections.sort(key=lambda rejection: (rejection.market, rejection.ticker, rejection.period_end_date or ""))
    return {
        "fetch_run_id": fetch_run_id,
        "dry_run": 1,
        "companies_total": len(companies_list),
        "raw_ok": sum(1 for result in raw_results if result.status == "OK"),
        "raw_empty": sum(1 for result in raw_results if result.status == "EMPTY"),
        "raw_error": sum(1 for result in raw_results if result.status == "ERROR"),
        "normalized_rows": len(normalized_rows),
        "migration_candidates": len(candidates),
        "metadata_rejections": len(metadata_rejections),
        "raw_cache_misses": len(cache_miss_rejections),
        "candidates": candidates,
        "rejections": rejections,
    }


def _payload_json_from_audit_row(row: Mapping[str, Any]) -> str:
    return _canonical_json(
        {
            "info": json.loads(str(row["info_json"])),
            "fast_info": json.loads(str(row["fast_info_json"])),
            "quarterly_income_stmt": json.loads(str(row["quarterly_income_stmt_json"])),
            "quarterly_balance_sheet": json.loads(str(row["quarterly_balance_sheet_json"])),
            "quarterly_cashflow": json.loads(str(row["quarterly_cashflow_json"])),
        }
    )


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _empty_statement() -> dict[str, list[Any]]:
    return {"index": [], "columns": [], "data": []}


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def _date_part(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text:
        return None
    return text[:10]


def _candidate_to_dict(candidate: V3YahooMigrationCandidate) -> dict[str, Any]:
    return {
        "candidate_key": candidate.candidate_key,
        "work_unit_key": candidate.work_unit_key,
        "company_id": candidate.company_id,
        "market": candidate.market,
        "ticker": candidate.ticker,
        "provider_symbol": candidate.provider_symbol,
        "fiscal_year": candidate.fiscal_year,
        "fiscal_quarter": candidate.fiscal_quarter,
        "period_end_date": candidate.period_end_date,
        "publish_date": candidate.publish_date,
        "market_availability_date": candidate.market_availability_date,
        "values": candidate.values,
        "provider_details": candidate.provider_details,
        "source_provider": candidate.source_provider,
        "provider_cache_ref": candidate.provider_cache_ref,
        "fetch_run_id": candidate.fetch_run_id,
        "payload_hash": candidate.payload_hash,
        "derivation_method": candidate.derivation_method,
        "candidate_version": candidate.candidate_version,
    }


def _rejection_to_dict(rejection: YahooCandidateRejection) -> dict[str, Any]:
    return {
        "market": rejection.market,
        "ticker": rejection.ticker,
        "provider_symbol": rejection.provider_symbol,
        "period_end_date": rejection.period_end_date,
        "reason": rejection.reason,
        "details": rejection.details,
    }


def _parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Fundamentals V3 Yahoo raw-cache and metadata adapter")
    parser.add_argument("--v3-db", required=True, help="Canonical V3 SQLite DB used only to read approved companies")
    parser.add_argument("--raw-cache-db", required=True, help="External V3 raw-cache SQLite DB")
    parser.add_argument("--v2-db", default=None, help="Optional V2 SQLite DB for fiscal identity and publish date enrichment")
    parser.add_argument("--legacy-db", default=None, help="Optional legacy SQLite DB for result-event publication metadata")
    parser.add_argument("--market", default="usa")
    parser.add_argument("--ticker", default=None)
    parser.add_argument("--tickers", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--delay-seconds", type=float, default=YAHOO_BOOTSTRAP_DEFAULT_DELAY_SECONDS)
    parser.add_argument("--dry-run", action="store_true", help="Fetch and build candidates without writing raw cache")
    parser.add_argument("--replay-raw-cache", action="store_true", help="Build candidates from existing raw cache without Yahoo calls")
    parser.add_argument("--json-output", action="store_true", help="Print deterministic candidate/rejection JSON")
    return parser.parse_args()


def _select_companies(companies: list[ApprovedV3Company], *, ticker: str | None, tickers: str | None) -> list[ApprovedV3Company]:
    if ticker and tickers:
        raise ValueError("V3_YAHOO_TICKER_AND_TICKERS_MUTUALLY_EXCLUSIVE")
    selected = companies
    allowed: set[str] | None = None
    if ticker:
        allowed = {normalize_ticker(ticker)}
    if tickers:
        allowed = {normalize_ticker(item) for item in tickers.split(",") if item.strip()}
    if allowed is not None:
        selected = [company for company in selected if company.ticker in allowed]
        found = {company.ticker for company in selected}
        missing = sorted(allowed - found)
        if missing:
            raise ValueError("V3_YAHOO_TICKER_NOT_APPROVED:" + ",".join(missing))
    return selected


def main() -> None:
    args = _parse_cli_args()
    with sqlite3.connect(str(Path(args.v3_db))) as v3_conn:
        companies = select_approved_v3_yahoo_companies(
            v3_conn,
            market=args.market,
            ticker=args.ticker,
            tickers=args.tickers,
            limit=args.limit,
        )
    v2_conn = sqlite3.connect(str(Path(args.v2_db))) if args.v2_db else None
    legacy_conn = sqlite3.connect(str(Path(args.legacy_db))) if args.legacy_db else None
    try:
        raw_cache_repo = V3RawCacheRepository(Path(args.raw_cache_db))
        metadata_enricher = YahooMetadataEnricher(v2_conn=v2_conn, legacy_conn=legacy_conn)
        if args.replay_raw_cache:
            summary = replay_v3_yahoo_bootstrap_from_raw_cache(
                companies=companies,
                raw_cache_repo=raw_cache_repo,
                metadata_enricher=metadata_enricher,
                fetch_run_id=args.run_id,
            )
        else:
            summary = run_v3_yahoo_bootstrap_adapter(
                companies=companies,
                raw_cache_repo=raw_cache_repo,
                metadata_enricher=metadata_enricher,
                fetch_run_id=args.run_id,
                delay_seconds=args.delay_seconds,
                dry_run=args.dry_run,
            )
    finally:
        if v2_conn is not None:
            v2_conn.close()
        if legacy_conn is not None:
            legacy_conn.close()
    print(f"SUMMARY fetch_run_id={summary['fetch_run_id']}")
    print(f"SUMMARY companies_total={summary['companies_total']}")
    print(f"SUMMARY raw_ok={summary['raw_ok']}")
    print(f"SUMMARY raw_empty={summary['raw_empty']}")
    print(f"SUMMARY raw_error={summary['raw_error']}")
    print(f"SUMMARY normalized_rows={summary['normalized_rows']}")
    print(f"SUMMARY migration_candidates={summary['migration_candidates']}")
    print(f"SUMMARY metadata_rejections={summary['metadata_rejections']}")
    if args.json_output:
        print(
            _canonical_json(
                {
                    "candidates": [_candidate_to_dict(candidate) for candidate in summary["candidates"]],
                    "rejections": [_rejection_to_dict(rejection) for rejection in summary["rejections"]],
                }
            )
        )


if __name__ == "__main__":
    main()
