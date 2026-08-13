from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from swingmaster.fundamentals_v2.simfin_api_statements import RequestStartRateLimiter, safe_rate_headers, statement_tickers
from swingmaster.fundamentals_v2.simfin_seed import load_active_tickers_readonly, parse_float, read_csv_rows, write_csv


SIMFIN_API_SHARES_PROVIDER = "SIMFIN_API_SHARES"
SHARES_ENDPOINT = "https://prod.simfin.com/api/v3/companies/common-shares-outstanding"
SHARES_ENDPOINT_NAME = "/api/v3/companies/common-shares-outstanding"
SOURCE_DATASET = "common-shares-outstanding"
SOURCE_FILE = "SIMFIN_API_SHARES_RAW"

DATE_COLUMNS = (
    "date",
    "end date",
    "enddate",
    "publish date",
    "report date",
    "period end date",
    "as of date",
    "as-of date",
)
SHARES_COLUMNS = (
    "common shares outstanding",
    "commonsharesoutstanding",
    "value",
    "shares outstanding",
    "shares",
)


@dataclass(frozen=True)
class ShareObservation:
    ticker: str
    simfin_id: int | None
    observation_date: str
    shares_outstanding: float
    provider_field: str
    source_value: Any


@dataclass(frozen=True)
class ShareMatch:
    quarter_id: int
    ticker: str
    report_date: str
    observation_date: str
    shares_outstanding: float
    provider_field: str
    source_value: Any
    match_type: str
    age_days: int


class SimFinSharesClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        rate_limiter: RequestStartRateLimiter | None = None,
        opener: Callable[[Request, float], Any] | None = None,
        timeout_seconds: float = 60.0,
    ) -> None:
        resolved = api_key or os.environ.get("SIMFIN_API_KEY")
        if not resolved:
            raise RuntimeError("SIMFIN_API_KEY_MISSING")
        self._api_key = resolved
        self._rate_limiter = rate_limiter or RequestStartRateLimiter()
        self._opener = opener or self._default_open
        self._timeout_seconds = timeout_seconds

    def fetch_ticker(self, ticker: str) -> dict[str, Any]:
        ticker = ticker.strip().upper()
        if not ticker or "," in ticker:
            raise ValueError("SIMFIN_API_ONE_TICKER_ONLY")
        self._rate_limiter.wait_for_next_start()
        started = utc_now()
        request = Request(
            SHARES_ENDPOINT + "?" + urlencode({"ticker": ticker}),
            headers={
                "Authorization": self._api_key,
                "accept": "application/json",
                "User-Agent": "swingmaster-rc-v2-simfin-api-shares",
            },
        )
        try:
            response = self._opener(request, self._timeout_seconds)
            status = int(response.status)
            body = response.read().decode("utf-8")
            headers = dict(response.headers.items())
        except HTTPError as exc:
            status = int(exc.code)
            body = exc.read().decode("utf-8", errors="replace")
            headers = dict(exc.headers.items())
            return fetch_result(ticker, started, status, body, headers)
        except Exception as exc:
            return {
                "ticker": ticker,
                "retrieved_at_utc": started,
                "http_status": 0,
                "provider_status": "RETRYABLE_ERROR",
                "payload_json": json.dumps({"error": type(exc).__name__, "message": str(exc)[:300]}, sort_keys=True),
                "safe_headers_json": "{}",
            }
        return fetch_result(ticker, started, status, body, headers)

    @staticmethod
    def _default_open(request: Request, timeout_seconds: float) -> Any:
        return urlopen(request, timeout=timeout_seconds)


def fetch_result(ticker: str, retrieved_at_utc: str, status: int, body: str, headers: Mapping[str, str]) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "retrieved_at_utc": retrieved_at_utc,
        "http_status": status,
        "provider_status": classify_http_status(status, body),
        "payload_json": body,
        "safe_headers_json": json.dumps(safe_rate_headers(headers), sort_keys=True),
    }


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def classify_http_status(status: int, body: str) -> str:
    if status == 200:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return "MALFORMED_RESPONSE"
        return "SUCCESS" if parse_share_observations(payload) else "NO_DATA"
    if status == 429:
        return "RATE_LIMITED"
    if status in {401, 403}:
        return "AUTH_ERROR"
    if status == 404:
        return "NO_DATA"
    if status >= 500:
        return "RETRYABLE_ERROR"
    return "RETRYABLE_ERROR"


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS rc_v2_simfin_api_shares_raw (
            raw_id INTEGER PRIMARY KEY,
            market TEXT NOT NULL,
            ticker TEXT NOT NULL,
            simfin_id INTEGER,
            endpoint TEXT NOT NULL,
            retrieved_at_utc TEXT NOT NULL,
            http_status INTEGER NOT NULL,
            provider_status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_sha256 TEXT NOT NULL,
            safe_headers_json TEXT NOT NULL DEFAULT '{}',
            run_id TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_rc_v2_simfin_api_shares_raw_ticker_status
        ON rc_v2_simfin_api_shares_raw (market, ticker, endpoint, provider_status, retrieved_at_utc);

        CREATE TABLE IF NOT EXISTS rc_v2_simfin_api_shares_fetch_state (
            market TEXT NOT NULL,
            ticker TEXT NOT NULL,
            last_attempt_at_utc TEXT,
            last_success_at_utc TEXT,
            last_status TEXT NOT NULL,
            last_http_status INTEGER,
            failure_count INTEGER NOT NULL DEFAULT 0,
            last_run_id TEXT,
            retry_after_utc TEXT,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (market, ticker)
        );
        """
    )


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone() is not None


def persist_fetch_result(conn: sqlite3.Connection, *, market: str, run_id: str, result: Mapping[str, Any]) -> int | None:
    ensure_schema(conn)
    now = utc_now()
    payload = str(result["payload_json"])
    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    observations = parse_share_observations(json.loads(payload)) if result["provider_status"] == "SUCCESS" else []
    simfin_id = observations[0].simfin_id if observations else extract_first_simfin_id(payload)
    raw_id: int | None = None
    if result["provider_status"] == "SUCCESS":
        cur = conn.execute(
            """
            INSERT INTO rc_v2_simfin_api_shares_raw (
                market, ticker, simfin_id, endpoint, retrieved_at_utc, http_status, provider_status,
                payload_json, payload_sha256, safe_headers_json, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market,
                str(result["ticker"]).upper(),
                simfin_id,
                SHARES_ENDPOINT_NAME,
                result["retrieved_at_utc"],
                result["http_status"],
                result["provider_status"],
                payload,
                payload_hash,
                result.get("safe_headers_json", "{}"),
                run_id,
                now,
            ),
        )
        raw_id = int(cur.lastrowid)
    previous = conn.execute(
        "SELECT failure_count FROM rc_v2_simfin_api_shares_fetch_state WHERE market=? AND ticker=?",
        (market, str(result["ticker"]).upper()),
    ).fetchone()
    failure_count = 0 if result["provider_status"] == "SUCCESS" else ((int(previous[0]) if previous else 0) + 1)
    conn.execute(
        """
        INSERT INTO rc_v2_simfin_api_shares_fetch_state (
            market, ticker, last_attempt_at_utc, last_success_at_utc, last_status, last_http_status,
            failure_count, last_run_id, retry_after_utc, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(market, ticker) DO UPDATE SET
            last_attempt_at_utc=excluded.last_attempt_at_utc,
            last_success_at_utc=CASE WHEN excluded.last_status='SUCCESS' THEN excluded.last_success_at_utc ELSE rc_v2_simfin_api_shares_fetch_state.last_success_at_utc END,
            last_status=excluded.last_status,
            last_http_status=excluded.last_http_status,
            failure_count=excluded.failure_count,
            last_run_id=excluded.last_run_id,
            retry_after_utc=excluded.retry_after_utc,
            updated_at_utc=excluded.updated_at_utc
        """,
        (
            market,
            str(result["ticker"]).upper(),
            result["retrieved_at_utc"],
            result["retrieved_at_utc"] if result["provider_status"] == "SUCCESS" else None,
            result["provider_status"],
            result["http_status"],
            failure_count,
            run_id,
            _retry_after(result.get("safe_headers_json", "{}")),
            now,
        ),
    )
    return raw_id


def _retry_after(safe_headers_json: object) -> str:
    try:
        headers = json.loads(str(safe_headers_json or "{}"))
    except json.JSONDecodeError:
        return ""
    return str(headers.get("Retry-After") or headers.get("retry-after") or "")


def latest_successful_raw(
    conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str,
    create_schema_if_missing: bool = True,
) -> sqlite3.Row | None:
    if create_schema_if_missing:
        ensure_schema(conn)
    elif not table_exists(conn, "rc_v2_simfin_api_shares_raw"):
        return None
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT *
        FROM rc_v2_simfin_api_shares_raw
        WHERE market=? AND ticker=? AND endpoint=? AND provider_status='SUCCESS'
        ORDER BY retrieved_at_utc DESC, raw_id DESC
        LIMIT 1
        """,
        (market, ticker.upper(), SHARES_ENDPOINT_NAME),
    ).fetchone()


def latest_terminal_no_data_state(
    conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str,
    create_schema_if_missing: bool = True,
) -> sqlite3.Row | None:
    if create_schema_if_missing:
        ensure_schema(conn)
    elif not table_exists(conn, "rc_v2_simfin_api_shares_fetch_state"):
        return None
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT *
        FROM rc_v2_simfin_api_shares_fetch_state
        WHERE market=? AND ticker=? AND last_status='NO_DATA'
        LIMIT 1
        """,
        (market, ticker.upper()),
    ).fetchone()


def acquire_simfin_api_shares(
    *,
    db_path: Path,
    tickers: Iterable[str],
    run_id: str,
    market: str = "usa",
    dry_run: bool = False,
    force_refresh: bool = False,
    max_tickers: int | None = None,
    min_interval_seconds: float = 2.1,
    client: SimFinSharesClient | None = None,
) -> dict[str, Any]:
    ordered = deterministic_tickers(tickers)
    if max_tickers is not None:
        ordered = ordered[:max_tickers]
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = []
    try:
        if not dry_run:
            ensure_schema(conn)
        active_client = client
        if active_client is None and not dry_run:
            active_client = SimFinSharesClient(rate_limiter=RequestStartRateLimiter(min_interval_seconds))
        for ticker in ordered:
            cached = latest_successful_raw(conn, market=market, ticker=ticker, create_schema_if_missing=not dry_run)
            if cached is not None and not force_refresh:
                rows.append({"ticker": ticker, "action": "CACHE_HIT", "status": "SUCCESS", "raw_id": cached["raw_id"]})
                continue
            no_data = latest_terminal_no_data_state(conn, market=market, ticker=ticker, create_schema_if_missing=not dry_run)
            if no_data is not None and not force_refresh:
                rows.append({"ticker": ticker, "action": "NO_DATA_CACHE_HIT", "status": "NO_DATA", "raw_id": ""})
                continue
            if dry_run:
                rows.append({"ticker": ticker, "action": "NETWORK_REQUIRED", "status": "DRY_RUN", "raw_id": ""})
                continue
            result = active_client.fetch_ticker(ticker)
            raw_id = persist_fetch_result(conn, market=market, run_id=run_id, result=result)
            conn.commit()
            rows.append({"ticker": ticker, "action": "FETCHED", "status": result["provider_status"], "raw_id": raw_id or ""})
            if result["provider_status"] == "RATE_LIMITED":
                return {"status": "SIMFIN_RATE_LIMITED", "rows": rows}
            if result["provider_status"] == "AUTH_ERROR":
                return {"status": "SIMFIN_AUTH_ERROR", "rows": rows}
        conn.commit()
        return {"status": "OK", "rows": rows}
    finally:
        conn.close()


def deterministic_tickers(tickers: Iterable[str]) -> list[str]:
    return sorted({ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()})


def parse_share_observations(payload: Any) -> list[ShareObservation]:
    observations: list[ShareObservation] = []
    for company in _companies(payload):
        ticker = str(company.get("ticker") or company.get("Ticker") or "").upper()
        simfin_id = _optional_int(company.get("id") or company.get("SimFinId") or company.get("simfin_id") or company.get("pid"))
        if _is_direct_observation(company):
            observation = _row_to_observation(company, ticker=ticker, simfin_id=simfin_id)
            if observation is not None:
                observations.append(observation)
        for row in _share_rows(company):
            observation = _row_to_observation(row, ticker=ticker, simfin_id=simfin_id)
            if observation is not None:
                observations.append(observation)
    return sorted(
        observations,
        key=lambda item: (item.ticker, item.observation_date, item.provider_field, item.shares_outstanding),
    )


def _companies(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        nested = payload.get("companies")
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, Mapping)]
        return [payload]
    return []


def _is_direct_observation(row: Mapping[str, Any]) -> bool:
    return _first_matching_key(row, DATE_COLUMNS) is not None and _first_matching_key(row, SHARES_COLUMNS) is not None


def _share_rows(company: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for key in ("shares", "commonShares", "commonSharesOutstanding", "common_shares_outstanding", "data"):
        value = company.get(key)
        rows.extend(_rows_from_value(value, parent_columns=company.get("columns")))
    for block in company.get("statements") or []:
        if isinstance(block, Mapping):
            rows.extend(_rows_from_value(block.get("data"), parent_columns=block.get("columns")))
    return rows


def _rows_from_value(value: Any, *, parent_columns: Any = None) -> list[Mapping[str, Any]]:
    if not value:
        return []
    if isinstance(value, list):
        if value and all(isinstance(item, Mapping) for item in value):
            return [item for item in value if isinstance(item, Mapping)]
        columns = [str(column) for column in parent_columns or []]
        if columns and all(isinstance(item, list) for item in value):
            return [dict(zip(columns, item)) for item in value if isinstance(item, list)]
    if isinstance(value, Mapping):
        nested = value.get("data")
        columns = value.get("columns") or parent_columns
        return _rows_from_value(nested, parent_columns=columns)
    return []


def _row_to_observation(row: Mapping[str, Any], *, ticker: str, simfin_id: int | None) -> ShareObservation | None:
    date_key = _first_matching_key(row, DATE_COLUMNS)
    shares_key = _first_matching_key(row, SHARES_COLUMNS)
    if date_key is None or shares_key is None:
        return None
    observation_date = _parse_iso_date(row.get(date_key))
    shares = parse_float(row.get(shares_key))
    if observation_date is None or shares is None:
        return None
    return ShareObservation(
        ticker=ticker,
        simfin_id=simfin_id or _optional_int(row.get("SimFinId") or row.get("simfin_id") or row.get("pid")),
        observation_date=observation_date,
        shares_outstanding=shares,
        provider_field=str(shares_key),
        source_value=row.get(shares_key),
    )


def _first_matching_key(row: Mapping[str, Any], candidates: Iterable[str]) -> str | None:
    normalized = {_normalize_key(key): key for key in row}
    for candidate in candidates:
        key = normalized.get(_normalize_key(candidate))
        if key is not None:
            return str(key)
    return None


def _normalize_key(key: object) -> str:
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _parse_iso_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()[:10]
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _optional_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    return int(float(str(value)))


def extract_first_simfin_id(payload_json: str) -> int | None:
    try:
        companies = _companies(json.loads(payload_json))
    except json.JSONDecodeError:
        return None
    for company in companies:
        value = company.get("id") or company.get("SimFinId") or company.get("simfin_id") or company.get("pid")
        if value is not None:
            return _optional_int(value)
    return None


def match_observation_for_report_date(
    observations: Iterable[ShareObservation],
    *,
    ticker: str,
    report_date: str,
    max_age_days: int | None = None,
) -> ShareMatch | None:
    report = date.fromisoformat(report_date)
    candidates = [
        observation
        for observation in observations
        if observation.ticker in {"", ticker.upper()} and date.fromisoformat(observation.observation_date) <= report
    ]
    if not candidates:
        return None
    latest = max(candidates, key=lambda observation: date.fromisoformat(observation.observation_date))
    age_days = (report - date.fromisoformat(latest.observation_date)).days
    if max_age_days is not None and age_days > max_age_days:
        return None
    return ShareMatch(
        quarter_id=-1,
        ticker=ticker.upper(),
        report_date=report_date,
        observation_date=latest.observation_date,
        shares_outstanding=latest.shares_outstanding,
        provider_field=latest.provider_field,
        source_value=latest.source_value,
        match_type="EXACT_DATE" if latest.observation_date == report_date else "PRIOR_OBSERVATION",
        age_days=age_days,
    )


def apply_simfin_api_shares(
    *,
    db_path: Path,
    tickers: Iterable[str],
    run_id: str,
    market: str = "usa",
    dry_run: bool = False,
    max_age_days: int | None = None,
) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = []
    try:
        if not dry_run:
            ensure_schema(conn)
            ensure_api_import_run(conn, run_id, market)
        for ticker in deterministic_tickers(tickers):
            raw = latest_successful_raw(conn, market=market, ticker=ticker, create_schema_if_missing=not dry_run)
            if raw is None:
                rows.append({"ticker": ticker, "status": "NO_CACHE", "updated": 0, "conflicts": 0, "unmatched": 0})
                continue
            result = apply_one_raw(conn, raw=raw, run_id=run_id, market=market, dry_run=dry_run, max_age_days=max_age_days)
            rows.append({"ticker": ticker, **result})
        if not dry_run:
            conn.commit()
        return {"status": "OK", "rows": rows}
    finally:
        conn.close()


def apply_one_raw(
    conn: sqlite3.Connection,
    *,
    raw: sqlite3.Row,
    run_id: str,
    market: str,
    dry_run: bool,
    max_age_days: int | None,
) -> dict[str, Any]:
    try:
        observations = parse_share_observations(json.loads(raw["payload_json"]))
    except json.JSONDecodeError:
        return {"status": "MALFORMED_RESPONSE", "updated": 0, "conflicts": 0, "unmatched": 0}
    ticker = str(raw["ticker"]).upper()
    quarter_rows = ordinary_quarters_for_ticker(conn, market=market, ticker=ticker)
    updated = conflicts = unchanged = unmatched = 0
    matches: list[ShareMatch] = []
    for quarter in quarter_rows:
        base_match = match_observation_for_report_date(observations, ticker=ticker, report_date=quarter["report_date"], max_age_days=max_age_days)
        if base_match is None:
            unmatched += 1
            continue
        match = ShareMatch(quarter_id=int(quarter["quarter_id"]), **{k: getattr(base_match, k) for k in base_match.__dataclass_fields__ if k != "quarter_id"})
        current = quarter["shares_outstanding"]
        if current is None:
            updated += 1
            matches.append(match)
        elif float(current) == float(match.shares_outstanding):
            unchanged += 1
        else:
            conflicts += 1
    if not dry_run:
        for match in matches:
            fill_share_match(conn, match, raw=raw, run_id=run_id)
    return {
        "status": "APPLIED_DRY_RUN" if dry_run else "APPLIED",
        "observations": len(observations),
        "quarters": len(quarter_rows),
        "updated": updated,
        "unchanged": unchanged,
        "conflicts": conflicts,
        "unmatched": unmatched,
    }


def ordinary_quarters_for_ticker(conn: sqlite3.Connection, *, market: str, ticker: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT c.ticker, q.quarter_id, q.report_date, f.shares_outstanding
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
            WHERE c.market=? AND c.ticker=? AND c.company_profile='ORDINARY' AND c.active=1
            ORDER BY q.report_date
            """,
            (market, ticker.upper()),
        )
    )


def fill_share_match(conn: sqlite3.Connection, match: ShareMatch, *, raw: sqlite3.Row, run_id: str) -> None:
    now = utc_now()
    conn.execute(
        "UPDATE rc_v2_fundamental_quarterly SET shares_outstanding=?, updated_at_utc=? WHERE quarter_id=? AND shares_outstanding IS NULL",
        (match.shares_outstanding, now, match.quarter_id),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
            quarter_id, field_name, provider, provider_field, source_dataset, source_file,
            source_file_sha256, transformation, source_value, import_run_id, created_at_utc
        ) VALUES (?, 'shares_outstanding', ?, ?, ?, ?, ?, 'none', ?, ?, ?)
        """,
        (
            match.quarter_id,
            SIMFIN_API_SHARES_PROVIDER,
            match.provider_field,
            SOURCE_DATASET,
            SOURCE_FILE,
            raw["payload_sha256"],
            json.dumps(
                {
                    "shares_outstanding": match.shares_outstanding,
                    "source_observation_date": match.observation_date,
                    "quarter_report_date": match.report_date,
                    "age_days": match.age_days,
                    "match_type": match.match_type,
                    "source_value": match.source_value,
                },
                sort_keys=True,
            ),
            run_id,
            now,
        ),
    )


def ensure_api_import_run(conn: sqlite3.Connection, run_id: str, market: str) -> None:
    if conn.execute("SELECT 1 FROM rc_v2_import_run WHERE import_run_id=?", (run_id,)).fetchone():
        return
    now = utc_now()
    conn.execute(
        "INSERT INTO rc_v2_import_run VALUES (?, ?, ?, ?, ?, NULL)",
        (run_id, market, SOURCE_FILE, "rc_v2_simfin_api_shares_v1", now),
    )


def build_candidate_inventory(*, v2_db: Path, market: str = "usa") -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{v2_db.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        cached = cached_share_tickers(conn, market=market)
        no_data = no_data_share_tickers(conn, market=market)
        rows = list(
            conn.execute(
                """
                SELECT c.ticker,
                       c.simfin_id,
                       COUNT(q.quarter_id) AS quarters_needing_shares,
                       MIN(q.report_date) AS first_missing_report_date,
                       MAX(q.report_date) AS latest_missing_report_date,
                       SUM(CASE WHEN strftime('%m-%d', q.report_date) NOT IN ('03-31', '06-30', '09-30', '12-31') THEN 1 ELSE 0 END) AS non_calendar_quarters
                FROM rc_v2_company c
                JOIN rc_v2_quarter q ON q.company_id=c.company_id
                JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
                WHERE c.market=? AND c.active=1 AND c.company_profile='ORDINARY' AND f.shares_outstanding IS NULL
                GROUP BY c.ticker, c.simfin_id
                ORDER BY c.ticker
                """,
                (market,),
            )
        )
        zero_coverage_companies = conn.execute(
            """
            SELECT COUNT(*)
            FROM rc_v2_company c
            WHERE c.market=? AND c.active=1 AND c.company_profile='ORDINARY'
              AND NOT EXISTS (
                  SELECT 1
                  FROM rc_v2_quarter q
                  JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
                  WHERE q.company_id=c.company_id AND f.shares_outstanding IS NOT NULL
              )
            """,
            (market,),
        ).fetchone()[0]
    finally:
        conn.close()
    inventory_rows = []
    for row in rows:
        ticker = row["ticker"]
        cache_status = "CACHE_HIT" if ticker in cached else ("NO_DATA_CACHE_HIT" if ticker in no_data else "NETWORK_REQUIRED")
        inventory_rows.append(
            {
                "ticker": ticker,
                "simfin_id": row["simfin_id"],
                "company_profile": "ORDINARY",
                "quarters_needing_shares": row["quarters_needing_shares"],
                "first_missing_report_date": row["first_missing_report_date"],
                "latest_missing_report_date": row["latest_missing_report_date"],
                "non_calendar_quarters": row["non_calendar_quarters"],
                "cache_status": cache_status,
                "request_model": "one_ticker_per_request",
                "min_start_interval_seconds": 2.1,
                "stop_on_429": 1,
            }
        )
    return {
        "rows": inventory_rows,
        "summary": {
            "ordinary_companies": len(inventory_rows),
            "ordinary_quarters": sum(int(row["quarters_needing_shares"]) for row in inventory_rows),
            "ordinary_tickers_requiring_shares_request": len(inventory_rows),
            "quarters_needing_shares": sum(int(row["quarters_needing_shares"]) for row in inventory_rows),
            "companies_with_zero_shares_coverage": int(zero_coverage_companies),
            "tickers_needing_latest_quarter_shares": len(inventory_rows),
            "historical_quarters_needing_shares": sum(max(int(row["quarters_needing_shares"]) - 1, 0) for row in inventory_rows),
            "cached_shares_tickers": len(cached),
            "terminal_no_data_shares_tickers": len(no_data),
            "network_required_shares_tickers": sum(1 for row in inventory_rows if row["cache_status"] == "NETWORK_REQUIRED"),
            "estimated_runtime_seconds_at_2_1": round(sum(1 for row in inventory_rows if row["cache_status"] == "NETWORK_REQUIRED") * 2.1, 1),
        },
    }


def cached_share_tickers(conn: sqlite3.Connection, *, market: str) -> set[str]:
    if not table_exists(conn, "rc_v2_simfin_api_shares_raw"):
        return set()
    return {
        str(row[0]).upper()
        for row in conn.execute(
            "SELECT DISTINCT ticker FROM rc_v2_simfin_api_shares_raw WHERE market=? AND endpoint=? AND provider_status='SUCCESS'",
            (market, SHARES_ENDPOINT_NAME),
        )
    }


def no_data_share_tickers(conn: sqlite3.Connection, *, market: str) -> set[str]:
    if not table_exists(conn, "rc_v2_simfin_api_shares_fetch_state"):
        return set()
    return {
        str(row[0]).upper()
        for row in conn.execute(
            "SELECT ticker FROM rc_v2_simfin_api_shares_fetch_state WHERE market=? AND last_status='NO_DATA'",
            (market,),
        )
    }


def select_first_shares_batch(inventory_rows: list[Mapping[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    candidates = [dict(row) for row in inventory_rows if row["cache_status"] == "NETWORK_REQUIRED"]
    selected: list[dict[str, Any]] = []
    aapl = next((row for row in candidates if row["ticker"] == "AAPL"), None)
    if aapl:
        selected.append(aapl)
    non_calendar = next((row for row in candidates if row["ticker"] != "AAPL" and int(row["non_calendar_quarters"] or 0) > 0), None)
    if non_calendar:
        selected.append(non_calendar)
    used = {row["ticker"] for row in selected}
    for row in candidates:
        if len(selected) >= limit:
            break
        if row["ticker"] not in used:
            selected.append(row)
            used.add(row["ticker"])
    return [
        {
            **row,
            "batch_order": idx + 1,
            "max_shares_requests": 1,
            "yahoo": 0,
            "sec": 0,
            "execute_now": 0,
        }
        for idx, row in enumerate(selected[:limit])
    ]


def write_dry_run_artifacts(
    *,
    v2_db: Path,
    artifact_dir: Path,
    market: str = "usa",
    legacy_db: Path | None = None,
    simfin_dir: Path | None = None,
) -> dict[str, Any]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    inventory = build_candidate_inventory(v2_db=v2_db, market=market)
    write_csv(artifact_dir / "shares_candidate_inventory.csv", inventory["rows"])
    write_csv(artifact_dir / "first_shares_batch.csv", select_first_shares_batch(inventory["rows"]))
    write_csv(
        artifact_dir / "shares_matching_audit.csv",
        [
            {
                "audit_scope": "PRODUCTION_DRY_RUN",
                "cached_shares_tickers": inventory["summary"]["cached_shares_tickers"],
                "projected_matched_quarters": 0,
                "projected_unmatched_quarters": inventory["summary"]["quarters_needing_shares"],
                "reason": "NO_CACHED_SHARES_OBSERVATIONS",
            }
        ],
    )
    write_residual_reconciliation(v2_db=v2_db, artifact_dir=artifact_dir, market=market, legacy_db=legacy_db, simfin_dir=simfin_dir)
    write_schema_doc(artifact_dir / "schema.md")
    write_endpoint_schema_doc(artifact_dir / "shares_endpoint_schema.md")
    summary = {
        **inventory["summary"],
        "artifact_dir": str(artifact_dir),
        "simfin_live_api_calls": 0,
        "production_db_writes": 0,
        "yahoo_calls": 0,
        "sec_calls": 0,
    }
    (artifact_dir / "dry_run_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (artifact_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def write_residual_reconciliation(
    *,
    v2_db: Path,
    artifact_dir: Path,
    market: str,
    legacy_db: Path | None,
    simfin_dir: Path | None,
) -> None:
    active: set[str] | None = None
    metadata_by_ticker: dict[str, Mapping[str, str]] = {}
    if legacy_db is not None and legacy_db.exists():
        active = load_active_tickers_readonly(legacy_db)
    if simfin_dir is not None and (simfin_dir / "us-companies.csv").exists():
        metadata_by_ticker = {str(row.get("Ticker") or "").upper(): row for row in read_csv_rows(simfin_dir / "us-companies.csv") if row.get("Ticker")}
    local_ordinary = set()
    local_banks = set()
    local_insurance = set()
    if simfin_dir is not None:
        local_ordinary = statement_tickers(simfin_dir, ["us-income-quarterly.csv", "us-balance-quarterly.csv", "us-cashflow-quarterly.csv"])
        local_banks = statement_tickers(simfin_dir, ["us-income-banks-quarterly.csv", "us-balance-banks-quarterly.csv", "us-cashflow-banks-quarterly.csv"])
        local_insurance = statement_tickers(simfin_dir, ["us-income-insurance-quarterly.csv", "us-balance-insurance-quarterly.csv", "us-cashflow-insurance-quarterly.csv"])
    conn = sqlite3.connect(f"file:{v2_db.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        v2_active = {str(row[0]).upper() for row in conn.execute("SELECT ticker FROM rc_v2_company WHERE market=? AND active=1", (market,))}
        no_data = {
            str(row[0]).upper()
            for row in conn.execute("SELECT ticker FROM rc_v2_simfin_api_fetch_state WHERE market=? AND last_status='NO_DATA'", (market,))
        }
        fetch_state_by_ticker = {
            str(row["ticker"]).upper(): dict(row)
            for row in conn.execute(
                """
                SELECT ticker, last_status, last_http_status, last_run_id, last_attempt_at_utc, last_success_at_utc
                FROM rc_v2_simfin_api_fetch_state
                WHERE market=?
                """,
                (market,),
            )
        }
        raw_state_by_ticker = {
            str(row["ticker"]).upper(): dict(row)
            for row in conn.execute(
                """
                SELECT ticker, COUNT(*) AS raw_success_rows, MAX(retrieved_at_utc) AS latest_raw_success_at,
                       MAX(simfin_id) AS raw_simfin_id, MAX(length(payload_json)) AS max_payload_bytes
                FROM rc_v2_simfin_api_raw
                WHERE market=? AND provider_status='SUCCESS'
                GROUP BY ticker
                """,
                (market,),
            )
        }
        v2_company_by_ticker = {
            str(row["ticker"]).upper(): dict(row)
            for row in conn.execute(
                """
                SELECT ticker, company_id, company_profile, active
                FROM rc_v2_company
                WHERE market=?
                """,
                (market,),
            )
        }
        canonical_by_ticker = {
            str(row["ticker"]).upper(): dict(row)
            for row in conn.execute(
                """
                SELECT c.ticker, COUNT(q.quarter_id) AS quarters, COUNT(f.quarter_id) AS fundamentals
                FROM rc_v2_company c
                LEFT JOIN rc_v2_quarter q ON q.company_id=c.company_id
                LEFT JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
                WHERE c.market=?
                GROUP BY c.ticker
                """,
                (market,),
            )
        }
        empty_success = {
            str(row[0]).upper()
            for row in conn.execute(
                """
                SELECT r.ticker
                FROM rc_v2_simfin_api_raw r
                LEFT JOIN rc_v2_company c ON c.market=r.market AND c.ticker=r.ticker
                WHERE r.market=? AND r.provider_status='SUCCESS' AND c.company_id IS NULL
                  AND r.payload_json LIKE '%"statements":[]%'
                """,
                (market,),
            )
        }
    finally:
        conn.close()
    residual = sorted((active or set()) - v2_active) if active is not None else sorted(empty_success)
    rows = []
    for ticker in residual:
        if ticker in empty_success:
            classification = "STALE_EMPTY_STATEMENT_SUCCESS_CACHE"
            evidence = "statement fetch_state SUCCESS/raw HTTP 200 but payload has statements=[] and no rc_v2_company row"
        elif ticker in no_data and ticker in metadata_by_ticker:
            classification = "ORDINARY_API_NO_DATA"
            evidence = "statement fetch_state terminal NO_DATA with SimFin company metadata"
        elif metadata_by_ticker and ticker not in metadata_by_ticker:
            classification = "NO_SIMFIN_COMPANY_METADATA"
            evidence = "active legacy ticker absent from SimFin us-companies metadata"
        elif ticker in no_data:
            classification = "API_NO_DATA"
            evidence = "statement fetch_state terminal NO_DATA"
        else:
            classification = "UNCLASSIFIED_RESIDUAL"
            evidence = "active ticker missing from rc_v2_company"
        rows.append(
            {
                "ticker": ticker,
                "active_universe_status": "ACTIVE" if active is None or ticker in active else "NOT_ACTIVE",
                "v2_company_row": "YES" if ticker in v2_company_by_ticker else "NO",
                "company_profile": v2_company_by_ticker.get(ticker, {}).get("company_profile", ""),
                "local_ordinary_membership": int(ticker in local_ordinary),
                "bank_membership": int(ticker in local_banks),
                "insurance_membership": int(ticker in local_insurance),
                "simfin_company_metadata": "YES" if ticker in metadata_by_ticker else "NO",
                "classification": classification,
                "simfin_id": metadata_by_ticker.get(ticker, {}).get("SimFinId", ""),
                "company_name": metadata_by_ticker.get(ticker, {}).get("Company Name", ""),
                "api_fetch_state": json.dumps(fetch_state_by_ticker.get(ticker, {}), sort_keys=True),
                "raw_api_state": json.dumps(raw_state_by_ticker.get(ticker, {}), sort_keys=True),
                "canonical_quarter_fundamental_state": json.dumps(canonical_by_ticker.get(ticker, {"quarters": 0, "fundamentals": 0}), sort_keys=True),
                "evidence": evidence,
            }
        )
    write_csv(artifact_dir / "residual_coverage_reconciliation.csv", rows)


def write_schema_doc(path: Path) -> None:
    path.write_text(
        """
# SimFin API Shares Schema

Production migration required before live acquire/apply:

```sql
CREATE TABLE rc_v2_simfin_api_shares_raw (
    raw_id INTEGER PRIMARY KEY,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    simfin_id INTEGER,
    endpoint TEXT NOT NULL,
    retrieved_at_utc TEXT NOT NULL,
    http_status INTEGER NOT NULL,
    provider_status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    safe_headers_json TEXT NOT NULL DEFAULT '{}',
    run_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL
);

CREATE INDEX idx_rc_v2_simfin_api_shares_raw_ticker_status
ON rc_v2_simfin_api_shares_raw (market, ticker, endpoint, provider_status, retrieved_at_utc);

CREATE TABLE rc_v2_simfin_api_shares_fetch_state (
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    last_attempt_at_utc TEXT,
    last_success_at_utc TEXT,
    last_status TEXT NOT NULL,
    last_http_status INTEGER,
    failure_count INTEGER NOT NULL DEFAULT 0,
    last_run_id TEXT,
    retry_after_utc TEXT,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (market, ticker)
);
```

These tables are intentionally separate from `rc_v2_simfin_api_raw` and
`rc_v2_simfin_api_fetch_state`; statement cache state is not shares cache state.
""".strip()
        + "\n",
        encoding="utf-8",
    )


def write_endpoint_schema_doc(path: Path) -> None:
    path.write_text(
        """
# SimFin Shares Endpoint Schema

Endpoint: `/api/v3/companies/common-shares-outstanding`.

Saved evidence from `temp/simfin_api_capability_audit/20260812T173450Z` shows:

- one ticker per request is the only confirmed shares request shape;
- an AAPL-only request returned HTTP 200;
- the previous parser wrote zero rows and no retained raw payload exists in the artifacts;
- a two-ticker request returned HTTP 429.

Current public client documentation for this endpoint maps response rows to
`id`, `Date`, and `Common Shares Outstanding`. The V2 parser accepts that
compact-style `columns` + `data` shape and row-dict shapes, but only emits
observations when both a parseable date column and a parseable
common-shares-outstanding value column exist.

Observed/assumed schema for the checkpoint:

- top level: one company object or list of company objects;
- ticker: company-level `ticker`;
- SimFinId: company-level `id`/`SimFinId`;
- observation date: row `Date`;
- shares value field: row `Common Shares Outstanding`;
- units: raw share count, no currency conversion;
- ordering: not trusted by importer; observations are sorted by date before matching;
- multiple share classes: endpoint name is common shares outstanding; no retained
  evidence of separate class rows, so first checkpoint must inspect raw payload;
- frequency: sparse point-in-time observations, not per-quarter statement rows;
- null behavior: rows with null/blank shares or unparsable dates are ignored.

Canonical semantic: `shares_outstanding` is point-in-time shares applicable at
the canonical quarter `report_date`. It is not weighted-average basic/diluted
shares from income statements.

Matching rule: exact observation date preferred; otherwise latest prior
observation `<= report_date`. Future observations are never used. No age
threshold is configured until endpoint evidence supports one.
""".strip()
        + "\n",
        encoding="utf-8",
    )
