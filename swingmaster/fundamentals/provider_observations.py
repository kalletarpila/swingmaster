from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping


OBS_KIND_YAHOO_CALENDAR_STATUS = "YAHOO_CALENDAR_STATUS"
OBS_KIND_YAHOO_RESULT_DETECTED_PRESENT = "YAHOO_RESULT_DETECTED_PRESENT"
OBS_KIND_SEC_FILING_CONFIRMED = "SEC_FILING_CONFIRMED"
OBS_KIND_SEC_FILING_NOT_AVAILABLE = "SEC_FILING_NOT_AVAILABLE"
OBS_KIND_SEC_CONFIRMATION_RECHECK = "SEC_CONFIRMATION_RECHECK"
OBS_KIND_PROVIDER_ERROR_RETRY = "PROVIDER_ERROR_RETRY"

TIMESTAMP_PROVIDER_REPORTED = "PROVIDER_REPORTED"
TIMESTAMP_OBSERVED_AT_ONLY = "OBSERVED_AT_ONLY"
TIMESTAMP_NOT_AVAILABLE = "NOT_AVAILABLE"


@dataclass(frozen=True)
class WorkUnitIdentity:
    market: str
    company_key: str
    ticker: str
    fiscal_year: int
    fiscal_quarter: str
    report_date: str
    work_unit_key: str


@dataclass(frozen=True)
class ProviderObservation:
    provider: str
    market: str
    ticker: str
    company_key: str
    observation_kind: str
    source_endpoint: str
    outcome: str
    observed_at_utc: str
    run_id: str
    canonical_fiscal_year: int | None = None
    canonical_fiscal_quarter: str | None = None
    period_end_date: str | None = None
    provider_reported_at_utc: str | None = None
    timestamp_quality: str = TIMESTAMP_NOT_AVAILABLE
    field_presence_fingerprint: str | None = None
    payload_hash: str | None = None
    source_reference: str | None = None


def work_unit_identity(*, market: str, ticker: str, period_end_date: str) -> WorkUnitIdentity:
    parsed = date.fromisoformat(period_end_date)
    quarter = ((parsed.month - 1) // 3) + 1
    fiscal_quarter = f"Q{quarter}"
    normalized_market = str(market).lower()
    normalized_ticker = str(ticker).upper()
    company_key = f"{normalized_market}:{normalized_ticker}"
    fiscal_year = parsed.year
    return WorkUnitIdentity(
        market=normalized_market,
        company_key=company_key,
        ticker=normalized_ticker,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        report_date=period_end_date,
        work_unit_key=f"{company_key}:{fiscal_year}:{fiscal_quarter}",
    )


def observation_content_key(observation: ProviderObservation) -> str:
    payload = {
        "company_key": observation.company_key,
        "canonical_fiscal_quarter": observation.canonical_fiscal_quarter,
        "canonical_fiscal_year": observation.canonical_fiscal_year,
        "field_presence_fingerprint": observation.field_presence_fingerprint,
        "market": observation.market,
        "observation_kind": observation.observation_kind,
        "outcome": observation.outcome,
        "payload_hash": observation.payload_hash,
        "period_end_date": observation.period_end_date,
        "provider": observation.provider,
        "provider_reported_at_utc": observation.provider_reported_at_utc,
        "source_endpoint": observation.source_endpoint,
        "source_reference": observation.source_reference,
        "ticker": observation.ticker,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def fingerprint_mapping(payload: Mapping[str, Any]) -> str:
    present = sorted(key for key, value in payload.items() if value is not None and value != "")
    return hashlib.sha256(json.dumps(present, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def hash_mapping(payload: Mapping[str, Any]) -> str:
    normalized = {str(key): payload[key] for key in sorted(payload)}
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8", errors="replace")).hexdigest()


def record_provider_observations(
    conn: sqlite3.Connection,
    observations: list[ProviderObservation],
) -> dict[str, int]:
    summary = {
        "content_observation_count": 0,
        "content_inserted_count": 0,
        "content_reused_count": 0,
        "seen_event_inserted_count": 0,
    }
    for observation in observations:
        key = observation_content_key(observation)
        existing = conn.execute(
            """
            SELECT id
            FROM rc_fundamental_provider_observation_content
            WHERE content_key = ?
            """,
            (key,),
        ).fetchone()
        if existing is None:
            cursor = conn.execute(
                """
                INSERT INTO rc_fundamental_provider_observation_content (
                    content_key, provider, market, ticker, company_key,
                    canonical_fiscal_year, canonical_fiscal_quarter, period_end_date,
                    observation_kind, source_endpoint, provider_reported_at_utc,
                    timestamp_quality, field_presence_fingerprint, payload_hash,
                    source_reference, outcome, first_observed_at_utc, last_observed_at_utc,
                    first_run_id, last_run_id, poll_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    key,
                    observation.provider,
                    observation.market,
                    observation.ticker,
                    observation.company_key,
                    observation.canonical_fiscal_year,
                    observation.canonical_fiscal_quarter,
                    observation.period_end_date,
                    observation.observation_kind,
                    observation.source_endpoint,
                    observation.provider_reported_at_utc,
                    observation.timestamp_quality,
                    observation.field_presence_fingerprint,
                    observation.payload_hash,
                    observation.source_reference,
                    observation.outcome,
                    observation.observed_at_utc,
                    observation.observed_at_utc,
                    observation.run_id,
                    observation.run_id,
                ),
            )
            content_id = int(cursor.lastrowid)
            summary["content_inserted_count"] += 1
        else:
            content_id = int(existing[0])
            conn.execute(
                """
                UPDATE rc_fundamental_provider_observation_content
                SET last_observed_at_utc = ?,
                    last_run_id = ?,
                    poll_count = poll_count + 1
                WHERE id = ?
                """,
                (observation.observed_at_utc, observation.run_id, content_id),
            )
            summary["content_reused_count"] += 1
        conn.execute(
            """
            INSERT INTO rc_fundamental_provider_observation_seen (
                content_id, observed_at_utc, run_id, outcome, created_at_utc
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (content_id, observation.observed_at_utc, observation.run_id, observation.outcome, observation.observed_at_utc),
        )
        summary["content_observation_count"] += 1
        summary["seen_event_inserted_count"] += 1
    return summary
