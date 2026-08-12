from __future__ import annotations

import csv
import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from swingmaster.fundamentals_v2.simfin_seed import (
    QUARTERLY_PERIODS,
    load_active_tickers_readonly,
    parse_float,
    read_csv_rows,
    write_csv,
)


SIMFIN_API_STATEMENTS_PROVIDER = "SIMFIN_API_STATEMENTS"
SIMFIN_API_DERIVED_PROVIDER = "SIMFIN_API_DERIVED"
STATEMENT_ENDPOINT = "https://prod.simfin.com/api/v3/companies/statements/compact"
STATEMENT_ENDPOINT_NAME = "/api/v3/companies/statements/compact"
REQUIRED_START = (2020, "Q4")
REQUIRED_PERIOD_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}


@dataclass(frozen=True)
class CandidateInventory:
    candidate_rows: list[dict[str, Any]]
    excluded_rows: list[dict[str, Any]]
    cache_rows: list[dict[str, Any]]
    network_rows: list[dict[str, Any]]
    summary: dict[str, Any]


class RequestStartRateLimiter:
    def __init__(
        self,
        min_interval_seconds: float = 2.1,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if min_interval_seconds < 2.0:
            raise ValueError("SIMFIN_RATE_LIMIT_INTERVAL_TOO_LOW")
        self.min_interval_seconds = min_interval_seconds
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._last_start: float | None = None

    def wait_for_next_start(self) -> None:
        if self._last_start is not None:
            elapsed = self._monotonic() - self._last_start
            wait_for = self.min_interval_seconds - elapsed
            if wait_for > 0:
                self._sleeper(wait_for)
        self._last_start = self._monotonic()


class SimFinStatementClient:
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
        params = {
            "ticker": ticker,
            "statements": "pl,bs,cf",
            "period": "q1,q2,q3,q4",
        }
        request = Request(
            STATEMENT_ENDPOINT + "?" + urlencode(params),
            headers={
                "Authorization": self._api_key,
                "accept": "application/json",
                "User-Agent": "swingmaster-rc-v2-simfin-api-statements",
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
            return {
                "ticker": ticker,
                "retrieved_at_utc": started,
                "http_status": status,
                "provider_status": classify_http_status(status, body),
                "payload_json": body,
                "safe_headers_json": json.dumps(safe_rate_headers(headers), sort_keys=True),
            }
        except Exception as exc:
            return {
                "ticker": ticker,
                "retrieved_at_utc": started,
                "http_status": 0,
                "provider_status": "RETRYABLE_ERROR",
                "payload_json": json.dumps({"error": type(exc).__name__, "message": str(exc)[:300]}, sort_keys=True),
                "safe_headers_json": "{}",
            }
        provider_status = classify_http_status(status, body)
        return {
            "ticker": ticker,
            "retrieved_at_utc": started,
            "http_status": status,
            "provider_status": provider_status,
            "payload_json": body,
            "safe_headers_json": json.dumps(safe_rate_headers(headers), sort_keys=True),
        }

    @staticmethod
    def _default_open(request: Request, timeout_seconds: float) -> Any:
        return urlopen(request, timeout=timeout_seconds)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def classify_http_status(status: int, body: str) -> str:
    if status == 200:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return "MALFORMED_RESPONSE"
        return "SUCCESS" if payload_has_statement_rows(payload) else "NO_DATA"
    if status == 429:
        return "RATE_LIMITED"
    if status in {401, 403}:
        return "AUTH_ERROR"
    if status == 404:
        return "NO_DATA"
    if status >= 500:
        return "RETRYABLE_ERROR"
    return "RETRYABLE_ERROR"


def safe_rate_headers(headers: Mapping[str, str]) -> dict[str, str]:
    allowed = {"retry-after", "x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset"}
    return {key: value for key, value in headers.items() if key.lower() in allowed}


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS rc_v2_simfin_api_raw (
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

        CREATE INDEX IF NOT EXISTS idx_rc_v2_simfin_api_raw_ticker_status
        ON rc_v2_simfin_api_raw (market, ticker, endpoint, provider_status, retrieved_at_utc);

        CREATE TABLE IF NOT EXISTS rc_v2_simfin_api_fetch_state (
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


def persist_fetch_result(
    conn: sqlite3.Connection,
    *,
    market: str,
    run_id: str,
    result: Mapping[str, Any],
) -> int | None:
    ensure_schema(conn)
    now = utc_now()
    payload = str(result["payload_json"])
    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    simfin_id = extract_first_simfin_id(payload)
    raw_id: int | None = None
    if result["provider_status"] == "SUCCESS":
        cur = conn.execute(
            """
            INSERT INTO rc_v2_simfin_api_raw (
                market, ticker, simfin_id, endpoint, retrieved_at_utc, http_status, provider_status,
                payload_json, payload_sha256, safe_headers_json, run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                market,
                result["ticker"],
                simfin_id,
                STATEMENT_ENDPOINT_NAME,
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
        "SELECT failure_count FROM rc_v2_simfin_api_fetch_state WHERE market=? AND ticker=?",
        (market, result["ticker"]),
    ).fetchone()
    failure_count = 0 if result["provider_status"] == "SUCCESS" else ((int(previous[0]) if previous else 0) + 1)
    retry_after = ""
    try:
        retry_after = json.loads(str(result.get("safe_headers_json", "{}"))).get("Retry-After", "")
    except json.JSONDecodeError:
        retry_after = ""
    conn.execute(
        """
        INSERT INTO rc_v2_simfin_api_fetch_state (
            market, ticker, last_attempt_at_utc, last_success_at_utc, last_status, last_http_status,
            failure_count, last_run_id, retry_after_utc, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(market, ticker) DO UPDATE SET
            last_attempt_at_utc=excluded.last_attempt_at_utc,
            last_success_at_utc=CASE WHEN excluded.last_status='SUCCESS' THEN excluded.last_success_at_utc ELSE rc_v2_simfin_api_fetch_state.last_success_at_utc END,
            last_status=excluded.last_status,
            last_http_status=excluded.last_http_status,
            failure_count=excluded.failure_count,
            last_run_id=excluded.last_run_id,
            retry_after_utc=excluded.retry_after_utc,
            updated_at_utc=excluded.updated_at_utc
        """,
        (
            market,
            result["ticker"],
            result["retrieved_at_utc"],
            result["retrieved_at_utc"] if result["provider_status"] == "SUCCESS" else None,
            result["provider_status"],
            result["http_status"],
            failure_count,
            run_id,
            retry_after,
            now,
        ),
    )
    return raw_id


def extract_first_simfin_id(payload_json: str) -> int | None:
    try:
        companies = parse_companies(json.loads(payload_json))
    except json.JSONDecodeError:
        return None
    for company in companies:
        value = company.get("id")
        if value is not None:
            return int(value)
    return None


def latest_successful_raw(
    conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str,
    create_schema_if_missing: bool = True,
) -> sqlite3.Row | None:
    if create_schema_if_missing:
        ensure_schema(conn)
    elif not table_exists(conn, "rc_v2_simfin_api_raw"):
        return None
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT *
        FROM rc_v2_simfin_api_raw
        WHERE market=? AND ticker=? AND endpoint=? AND provider_status='SUCCESS'
        ORDER BY retrieved_at_utc DESC, raw_id DESC
        LIMIT 1
        """,
        (market, ticker.upper(), STATEMENT_ENDPOINT_NAME),
    ).fetchone()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone() is not None


def acquire_simfin_api_statements(
    *,
    db_path: Path,
    tickers: Iterable[str],
    run_id: str,
    market: str = "usa",
    dry_run: bool = False,
    force_refresh: bool = False,
    max_tickers: int | None = None,
    min_interval_seconds: float = 2.1,
    client: SimFinStatementClient | None = None,
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
        for ticker in ordered:
            cached = latest_successful_raw(conn, market=market, ticker=ticker, create_schema_if_missing=not dry_run)
            if cached is not None and not force_refresh:
                rows.append({"ticker": ticker, "action": "CACHE_HIT", "status": "SUCCESS", "raw_id": cached["raw_id"]})
                continue
            if dry_run:
                rows.append({"ticker": ticker, "action": "NETWORK_REQUIRED", "status": "DRY_RUN", "raw_id": ""})
                continue
            active_client = client or SimFinStatementClient(rate_limiter=RequestStartRateLimiter(min_interval_seconds))
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


def parse_companies(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        return [payload]
    return []


def payload_has_statement_rows(payload: Any) -> bool:
    for company in parse_companies(payload):
        for statement in company.get("statements") or []:
            if statement.get("data"):
                return True
    return False


def flatten_statement_company(company: Mapping[str, Any]) -> list[dict[str, Any]]:
    base = {
        "ticker": str(company.get("ticker") or "").upper(),
        "simfin_id": int(company["id"]) if company.get("id") is not None else None,
        "company_name": company.get("name"),
        "currency": company.get("currency"),
    }
    rows = []
    for statement in company.get("statements") or []:
        columns = statement.get("columns") or []
        statement_code = str(statement.get("statement") or "").upper()
        for values in statement.get("data") or []:
            row = dict(base)
            row["statement"] = statement_code
            for column, value in zip(columns, values):
                row[column] = value
            rows.append(row)
    return rows


def statement_key(row: Mapping[str, Any]) -> tuple[int, int, str, str]:
    return (
        int(row["simfin_id"]),
        int(row["Fiscal Year"]),
        str(row["Fiscal Period"]).upper(),
        str(row["Report Date"]),
    )


def row_by_statement_key(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[tuple[int, int, str, str], Mapping[str, Any]]]:
    grouped: dict[str, dict[tuple[int, int, str, str], Mapping[str, Any]]] = {"PL": {}, "BS": {}, "CF": {}}
    for row in rows:
        statement = str(row.get("statement") or "").upper()
        if statement in grouped and row.get("simfin_id") and row.get("Fiscal Year") and row.get("Fiscal Period") and row.get("Report Date"):
            grouped[statement][statement_key(row)] = row
    return grouped


def is_required_window(fiscal_year: int, fiscal_period: str) -> bool:
    period = fiscal_period.upper()
    if period not in REQUIRED_PERIOD_ORDER:
        return False
    return (fiscal_year, REQUIRED_PERIOD_ORDER[period]) >= (REQUIRED_START[0], REQUIRED_PERIOD_ORDER[REQUIRED_START[1]])


def map_api_ordinary_fields(rows: Mapping[str, Mapping[str, Any] | None]) -> dict[str, float | None]:
    income = rows.get("PL") or {}
    balance = rows.get("BS") or {}
    cashflow = rows.get("CF") or {}
    operating_income = parse_float(income.get("Operating Income (Loss)"))
    depreciation_amortization = parse_float(cashflow.get("Depreciation & Amortization"))
    operating_cashflow = parse_float(cashflow.get("Cash from Operating Activities")) or parse_float(cashflow.get("Net Cash from Operating Activities"))
    capex = parse_float(cashflow.get("Change in Fixed Assets & Intangibles"))
    short_debt = parse_float(balance.get("Short Term Debt"))
    long_debt = parse_float(balance.get("Long Term Debt"))
    total_debt = None if short_debt is None and long_debt is None else (short_debt or 0.0) + (long_debt or 0.0)
    return {
        "revenue": parse_float(income.get("Revenue")),
        "gross_profit": parse_float(income.get("Gross Profit")),
        "operating_income": operating_income,
        "depreciation_amortization": depreciation_amortization,
        "ebit": None,
        "ebitda": None if operating_income is None or depreciation_amortization is None else operating_income + depreciation_amortization,
        "net_income": parse_float(income.get("Net Income")),
        "operating_cashflow": operating_cashflow,
        "capex": capex,
        "free_cashflow": None if operating_cashflow is None or capex is None else operating_cashflow + capex,
        "cash": parse_float(balance.get("Cash, Cash Equivalents & Short Term Investments")),
        "total_debt": total_debt,
        "shares_outstanding": None,
        "weighted_average_shares_basic": None,
        "weighted_average_shares_diluted": None,
    }


def apply_simfin_api_statements(
    *,
    db_path: Path,
    tickers: Iterable[str],
    run_id: str,
    market: str = "usa",
    dry_run: bool = False,
) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = []
    try:
        ensure_schema(conn)
        for ticker in deterministic_tickers(tickers):
            raw = latest_successful_raw(conn, market=market, ticker=ticker)
            if raw is None:
                rows.append({"ticker": ticker, "status": "NO_CACHE"})
                continue
            result = apply_one_raw(conn, raw=raw, run_id=run_id, market=market, dry_run=dry_run)
            rows.append({"ticker": ticker, **result})
        if not dry_run:
            conn.commit()
        return {"status": "OK", "rows": rows}
    finally:
        conn.close()


def apply_one_raw(conn: sqlite3.Connection, *, raw: sqlite3.Row, run_id: str, market: str, dry_run: bool) -> dict[str, Any]:
    try:
        companies = parse_companies(json.loads(raw["payload_json"]))
    except json.JSONDecodeError:
        return {"status": "MALFORMED_RESPONSE"}
    if not dry_run:
        ensure_api_import_run(conn, run_id, market)
    inserted_companies = inserted_quarters = inserted_fundamentals = filled_fields = conflicts = 0
    for company in companies:
        flat_rows = flatten_statement_company(company)
        if not flat_rows:
            continue
        simfin_id = int(company["id"])
        ticker = str(company.get("ticker") or raw["ticker"]).upper()
        company_existed = company_exists(conn, simfin_id)
        company_id = get_or_create_company(
            conn,
            market=market,
            ticker=ticker,
            simfin_id=simfin_id,
            company_name=company.get("name"),
            dry_run=dry_run,
        )
        if company_id == -1:
            inserted_companies += 1
            continue
        if company_id < 0:
            continue
        if not company_existed:
            inserted_companies += 1
        grouped = row_by_statement_key(flat_rows)
        for key in sorted(set().union(*(set(values) for values in grouped.values()))):
            _sid, fiscal_year, fiscal_period, report_date = key
            if fiscal_period not in QUARTERLY_PERIODS or not is_required_window(fiscal_year, fiscal_period):
                continue
            quarter_rows = {"PL": grouped["PL"].get(key), "BS": grouped["BS"].get(key), "CF": grouped["CF"].get(key)}
            if dry_run:
                inserted_quarters += int(not quarter_exists(conn, company_id, fiscal_year, fiscal_period, report_date))
                continue
            quarter_id, quarter_inserted = get_or_create_quarter(conn, company_id, key, quarter_rows)
            inserted_quarters += int(quarter_inserted)
            values = map_api_ordinary_fields(quarter_rows)
            fund_inserted, field_fills, field_conflicts = upsert_fundamentals_with_conflict_policy(conn, quarter_id, values)
            inserted_fundamentals += int(fund_inserted)
            filled_fields += field_fills
            conflicts += field_conflicts
            insert_api_provenance(conn, quarter_id, values, raw, run_id)
    return {
        "status": "APPLIED_DRY_RUN" if dry_run else "APPLIED",
        "inserted_companies": inserted_companies,
        "inserted_quarters": inserted_quarters,
        "inserted_fundamentals": inserted_fundamentals,
        "filled_fields": filled_fields,
        "conflicts": conflicts,
    }


def get_or_create_company(
    conn: sqlite3.Connection,
    *,
    market: str,
    ticker: str,
    simfin_id: int,
    company_name: str | None,
    dry_run: bool,
) -> int:
    existing = conn.execute("SELECT company_id FROM rc_v2_company WHERE simfin_id=?", (simfin_id,)).fetchone()
    if existing:
        return int(existing["company_id"])
    if dry_run:
        return -1
    now = utc_now()
    cur = conn.execute(
        """
        INSERT INTO rc_v2_company (market, ticker, simfin_id, company_name, company_profile, active, created_at_utc, updated_at_utc)
        VALUES (?, ?, ?, ?, 'ORDINARY', 1, ?, ?)
        """,
        (market, ticker, simfin_id, company_name, now, now),
    )
    return int(cur.lastrowid)


def ensure_api_import_run(conn: sqlite3.Connection, run_id: str, market: str) -> None:
    if conn.execute("SELECT 1 FROM rc_v2_import_run WHERE import_run_id=?", (run_id,)).fetchone():
        return
    now = utc_now()
    conn.execute(
        "INSERT INTO rc_v2_import_run VALUES (?, ?, ?, ?, ?, NULL)",
        (run_id, market, "SIMFIN_API_RAW", "rc_v2_simfin_api_statements_v1", now),
    )


def company_exists(conn: sqlite3.Connection, simfin_id: int) -> bool:
    return conn.execute("SELECT 1 FROM rc_v2_company WHERE simfin_id=?", (simfin_id,)).fetchone() is not None


def quarter_exists(conn: sqlite3.Connection, company_id: int, fiscal_year: int, fiscal_period: str, report_date: str) -> bool:
    return (
        conn.execute(
            """
            SELECT 1 FROM rc_v2_quarter
            WHERE company_id=? AND fiscal_year=? AND fiscal_period=? AND report_date=?
            """,
            (company_id, fiscal_year, fiscal_period, report_date),
        ).fetchone()
        is not None
    )


def get_or_create_quarter(
    conn: sqlite3.Connection,
    company_id: int,
    key: tuple[int, int, str, str],
    rows: Mapping[str, Mapping[str, Any] | None],
) -> tuple[int, bool]:
    _sid, fiscal_year, fiscal_period, report_date = key
    existing = conn.execute(
        """
        SELECT quarter_id FROM rc_v2_quarter
        WHERE company_id=? AND fiscal_year=? AND fiscal_period=? AND report_date=?
        """,
        (company_id, fiscal_year, fiscal_period, report_date),
    ).fetchone()
    if existing:
        return int(existing["quarter_id"]), False
    now = utc_now()
    publish_date = first_available(rows.values(), "Publish Date")
    restated_date = first_available(rows.values(), "Restated")
    cur = conn.execute(
        """
        INSERT INTO rc_v2_quarter (
            company_id, fiscal_year, fiscal_period, report_date, publish_date, restated_date,
            quarter_identity_source, has_income, has_balance, has_cashflow, created_at_utc, updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, 'SIMFIN_API_FISCAL_PERIOD_REPORT_DATE', ?, ?, ?, ?, ?)
        """,
        (
            company_id,
            fiscal_year,
            fiscal_period,
            report_date,
            publish_date,
            restated_date,
            int(rows["PL"] is not None),
            int(rows["BS"] is not None),
            int(rows["CF"] is not None),
            now,
            now,
        ),
    )
    return int(cur.lastrowid), True


def first_available(rows: Iterable[Mapping[str, Any] | None], field: str) -> str | None:
    values = sorted({str(row.get(field)) for row in rows if row and row.get(field)})
    return values[-1] if values else None


def upsert_fundamentals_with_conflict_policy(
    conn: sqlite3.Connection,
    quarter_id: int,
    values: Mapping[str, float | None],
) -> tuple[bool, int, int]:
    fields = list(values)
    existing = conn.execute("SELECT * FROM rc_v2_fundamental_quarterly WHERE quarter_id=?", (quarter_id,)).fetchone()
    now = utc_now()
    if existing is None:
        missing = [field for field, value in values.items() if field not in {"ebit", "shares_outstanding"} and value is None]
        available = sum(1 for value in values.values() if value is not None)
        conn.execute(
            """
            INSERT INTO rc_v2_fundamental_quarterly (
                quarter_id, revenue, gross_profit, operating_income, depreciation_amortization, ebit, ebitda,
                net_income, operating_cashflow, capex, free_cashflow, cash, total_debt, shares_outstanding,
                weighted_average_shares_basic, weighted_average_shares_diluted, available_canonical_field_count,
                has_income, has_balance, has_cashflow, seed_status, missing_seed_fields_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 1, ?, ?, ?, ?)
            """,
            (
                quarter_id,
                values["revenue"],
                values["gross_profit"],
                values["operating_income"],
                values["depreciation_amortization"],
                values["ebit"],
                values["ebitda"],
                values["net_income"],
                values["operating_cashflow"],
                values["capex"],
                values["free_cashflow"],
                values["cash"],
                values["total_debt"],
                values["shares_outstanding"],
                values["weighted_average_shares_basic"],
                values["weighted_average_shares_diluted"],
                available,
                "API_STRONG",
                json.dumps(missing, sort_keys=True),
                now,
                now,
            ),
        )
        return True, available, 0
    fills = conflicts = 0
    updates = {}
    for field in fields:
        incoming = values[field]
        if incoming is None:
            continue
        current = existing[field]
        if current is None:
            updates[field] = incoming
            fills += 1
        elif float(current) != float(incoming):
            conflicts += 1
    if updates:
        assignments = ", ".join(f"{field}=?" for field in updates)
        conn.execute(
            f"UPDATE rc_v2_fundamental_quarterly SET {assignments}, updated_at_utc=? WHERE quarter_id=?",
            (*updates.values(), now, quarter_id),
        )
    return False, fills, conflicts


PROVENANCE = {
    "revenue": ("PL", "Revenue", SIMFIN_API_STATEMENTS_PROVIDER, "DIRECT"),
    "gross_profit": ("PL", "Gross Profit", SIMFIN_API_STATEMENTS_PROVIDER, "DIRECT"),
    "operating_income": ("PL", "Operating Income (Loss)", SIMFIN_API_STATEMENTS_PROVIDER, "DIRECT"),
    "depreciation_amortization": ("CF", "Depreciation & Amortization", SIMFIN_API_STATEMENTS_PROVIDER, "DIRECT"),
    "net_income": ("PL", "Net Income", SIMFIN_API_STATEMENTS_PROVIDER, "DIRECT"),
    "operating_cashflow": ("CF", "Cash from Operating Activities", SIMFIN_API_STATEMENTS_PROVIDER, "DIRECT"),
    "capex": ("CF", "Change in Fixed Assets & Intangibles", SIMFIN_API_STATEMENTS_PROVIDER, "DIRECT"),
    "cash": ("BS", "Cash, Cash Equivalents & Short Term Investments", SIMFIN_API_STATEMENTS_PROVIDER, "DIRECT"),
}


def insert_api_provenance(
    conn: sqlite3.Connection,
    quarter_id: int,
    values: Mapping[str, float | None],
    raw: sqlite3.Row,
    run_id: str,
) -> None:
    now = utc_now()
    for field, value in values.items():
        if value is None:
            continue
        if field == "ebitda":
            provider = SIMFIN_API_DERIVED_PROVIDER
            provider_field = "Operating Income (Loss)+Depreciation & Amortization"
            source_dataset = "PL+CF"
            transformation = "operating_income + depreciation_amortization"
            source_value = json.dumps({"operating_income": values["operating_income"], "depreciation_amortization": values["depreciation_amortization"]}, sort_keys=True)
        elif field == "free_cashflow":
            provider = SIMFIN_API_DERIVED_PROVIDER
            provider_field = "Cash from Operating Activities+Change in Fixed Assets & Intangibles"
            source_dataset = "CF"
            transformation = "operating_cashflow + capex"
            source_value = json.dumps({"operating_cashflow": values["operating_cashflow"], "capex": values["capex"]}, sort_keys=True)
        elif field == "total_debt":
            provider = SIMFIN_API_DERIVED_PROVIDER
            provider_field = "Short Term Debt+Long Term Debt"
            source_dataset = "BS"
            transformation = "short_term_debt + long_term_debt"
            source_value = str(value)
        else:
            source_dataset, provider_field, provider, transformation = PROVENANCE[field]
            source_value = str(value)
        conn.execute(
            """
            INSERT OR IGNORE INTO rc_v2_fundamental_field_source (
                quarter_id, field_name, provider, provider_field, source_dataset, source_file,
                source_file_sha256, transformation, source_value, import_run_id, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                quarter_id,
                field,
                provider,
                provider_field,
                source_dataset,
                "SIMFIN_API_RAW",
                raw["payload_sha256"],
                transformation,
                source_value,
                run_id,
                now,
            ),
        )


def build_candidate_inventory(
    *,
    legacy_db: Path,
    v2_db: Path,
    simfin_dir: Path,
    market: str = "usa",
) -> CandidateInventory:
    active = load_active_tickers_readonly(legacy_db)
    company_rows = read_csv_rows(simfin_dir / "us-companies.csv")
    metadata_by_ticker = {str(row.get("Ticker") or "").upper(): row for row in company_rows if row.get("Ticker")}
    ordinary_tickers = statement_tickers(simfin_dir, ["us-income-quarterly.csv", "us-balance-quarterly.csv", "us-cashflow-quarterly.csv"])
    bank_tickers = statement_tickers(simfin_dir, ["us-income-banks-quarterly.csv", "us-balance-banks-quarterly.csv", "us-cashflow-banks-quarterly.csv"])
    insurance_tickers = statement_tickers(simfin_dir, ["us-income-insurance-quarterly.csv", "us-balance-insurance-quarterly.csv", "us-cashflow-insurance-quarterly.csv"])
    conn = sqlite3.connect(f"file:{v2_db.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only=ON")
        cache_rows = cache_inventory(conn, active, market=market)
    finally:
        conn.close()
    cached = {row["ticker"] for row in cache_rows if row["cache_status"] == "CACHE_HIT"}
    candidate_rows = []
    excluded_rows = []
    for ticker in sorted(active):
        meta = metadata_by_ticker.get(ticker)
        if ticker in ordinary_tickers:
            excluded_rows.append(profile_row(ticker, "ORDINARY_LOCAL_COVERED", meta))
        elif ticker in bank_tickers:
            excluded_rows.append(profile_row(ticker, "BANK", meta))
        elif ticker in insurance_tickers:
            excluded_rows.append(profile_row(ticker, "INSURANCE", meta))
        elif meta is None:
            excluded_rows.append(profile_row(ticker, "NO_SIMFIN_COMPANY_METADATA", None))
        else:
            candidate_rows.append(profile_row(ticker, "ORDINARY_API_GAP", meta))
    network_rows = [
        {**row, "request_number": idx + 1, "request_model": "one_ticker_per_request", "min_start_interval_seconds": 2.1}
        for idx, row in enumerate(row for row in candidate_rows if row["ticker"] not in cached)
    ]
    counts = count_by_class(candidate_rows + excluded_rows)
    summary = {
        "active_tickers": len(active),
        "ordinary_api_gap_count": len(candidate_rows),
        "bank_exclusions": counts.get("BANK", 0),
        "insurance_exclusions": counts.get("INSURANCE", 0),
        "ordinary_local_exclusions": counts.get("ORDINARY_LOCAL_COVERED", 0),
        "no_metadata_count": counts.get("NO_SIMFIN_COMPANY_METADATA", 0),
        "network_required_count": len(network_rows),
        "cache_hit_count": len(cached),
        "estimated_runtime_seconds_at_2_1": round(len(network_rows) * 2.1, 1),
        "estimated_runtime_minutes_at_2_1": round(len(network_rows) * 2.1 / 60, 2),
    }
    return CandidateInventory(candidate_rows, excluded_rows, cache_rows, network_rows, summary)


def statement_tickers(simfin_dir: Path, filenames: Iterable[str]) -> set[str]:
    tickers = set()
    for filename in filenames:
        path = simfin_dir / filename
        if not path.exists():
            continue
        for row in read_csv_rows(path):
            ticker = str(row.get("Ticker") or "").upper()
            if ticker:
                tickers.add(ticker)
    return tickers


def cache_inventory(conn: sqlite3.Connection, active: set[str], *, market: str) -> list[dict[str, Any]]:
    rows = []
    for ticker in sorted(active):
        cached = latest_successful_raw(conn, market=market, ticker=ticker, create_schema_if_missing=False)
        rows.append(
            {
                "ticker": ticker,
                "cache_status": "CACHE_HIT" if cached else "NETWORK_REQUIRED",
                "raw_id": cached["raw_id"] if cached else "",
                "retrieved_at_utc": cached["retrieved_at_utc"] if cached else "",
                "payload_sha256": cached["payload_sha256"] if cached else "",
            }
        )
    return rows


def profile_row(ticker: str, classification: str, meta: Mapping[str, str] | None) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "classification": classification,
        "simfin_id": meta.get("SimFinId") if meta else "",
        "company_name": meta.get("Company Name") if meta else "",
        "industry_id": meta.get("IndustryId") if meta else "",
        "fiscal_year_end_month": meta.get("End of financial year (month)") if meta else "",
    }


def count_by_class(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["classification"])
        counts[key] = counts.get(key, 0) + 1
    return counts


def write_dry_run_artifacts(inventory: CandidateInventory, artifact_dir: Path) -> dict[str, Any]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    write_csv(artifact_dir / "candidate_inventory.csv", inventory.candidate_rows)
    write_csv(artifact_dir / "excluded_profiles.csv", inventory.excluded_rows)
    write_csv(artifact_dir / "cache_inventory.csv", inventory.cache_rows)
    write_csv(artifact_dir / "network_request_plan.csv", inventory.network_rows)
    first_batch = select_first_production_batch(inventory.candidate_rows)
    write_csv(artifact_dir / "first_production_batch.csv", first_batch)
    write_schema_doc(artifact_dir / "schema.md")
    write_rate_doc(artifact_dir / "rate_limit_policy.md")
    summary = {
        **inventory.summary,
        "first_production_batch": [row["ticker"] for row in first_batch],
        "simfin_api_calls": 0,
        "shares_calls": 0,
        "yahoo_calls": 0,
        "sec_calls": 0,
        "v2_production_writes": 0,
    }
    (artifact_dir / "dry_run_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (artifact_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def select_first_production_batch(candidate_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    avoid = {"AAPL", "ADM", "AAP", "AFL", "ACGL"}
    preferred = [row for row in candidate_rows if row["ticker"] not in avoid]
    selected = []
    non_calendar = next((row for row in preferred if str(row.get("fiscal_year_end_month") or "") not in {"", "12"}), None)
    if non_calendar:
        selected.append(non_calendar)
    used_tickers = {row["ticker"] for row in selected}
    used_industries = {row.get("industry_id") for row in selected if row.get("industry_id")}
    for row in preferred:
        if len(selected) >= 6:
            break
        if row["ticker"] in used_tickers:
            continue
        industry = row.get("industry_id")
        if industry and industry in used_industries and len(preferred) - len(selected) > 6:
            continue
        selected.append(row)
        used_tickers.add(row["ticker"])
        if industry:
            used_industries.add(industry)
    for row in preferred:
        if len(selected) >= 6:
            break
        if row["ticker"] not in used_tickers:
            selected.append(row)
            used_tickers.add(row["ticker"])
    return [
        {
            **row,
            "request_model": "one_ticker_per_request",
            "max_statement_requests": 1,
            "min_start_interval_seconds": 2.1,
            "stop_on_429": 1,
            "shares_api": 0,
            "yahoo": 0,
            "sec": 0,
        }
        for row in selected
    ]


def write_schema_doc(path: Path) -> None:
    path.write_text(
        """
# SimFin API Statement Schema

Required migration before production acquisition:

```sql
CREATE TABLE rc_v2_simfin_api_raw (
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

CREATE INDEX idx_rc_v2_simfin_api_raw_ticker_status
ON rc_v2_simfin_api_raw (market, ticker, endpoint, provider_status, retrieved_at_utc);

CREATE TABLE rc_v2_simfin_api_fetch_state (
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

Raw success payloads are append-only. Failed attempts update fetch-state only and do not delete previous successes.
""".strip()
        + "\n",
        encoding="utf-8",
    )


def write_rate_doc(path: Path) -> None:
    path.write_text(
        """
# SimFin API Rate Policy

Initial production default:

- one ticker per request
- `statements=pl,bs,cf`
- `period=q1,q2,q3,q4`
- serial execution only
- minimum request-start interval: 2.1 seconds

If HTTP 429 occurs:

- persist safe fetch-state as `RATE_LIMITED`
- stop the run immediately
- do not retry the ticker in the same run
- do not call subsequent tickers
- preserve all previous successful raw snapshots
""".strip()
        + "\n",
        encoding="utf-8",
    )
