from __future__ import annotations

import csv
import io
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
from urllib.request import Request, urlopen


SHARADAR_DIRECT_BASE_URL = "https://api.sharadar.com/v1.0"
SHARADAR_API_KEY_ENV = "SHARADAR_API_KEY"
USER_AGENT = "swingmaster-v4-sharadar-smoke/0.1"

STATUS_SUCCESS = "SUCCESS"
STATUS_AUTH_NOT_CONFIGURED = "AUTH_NOT_CONFIGURED"
STATUS_AUTH_FAILED = "AUTH_FAILED"
STATUS_FREE_TIER_LIMIT = "FREE_TIER_LIMIT"
STATUS_RATE_LIMITED = "RATE_LIMITED"
STATUS_TRANSIENT_FAILURE = "TRANSIENT_FAILURE"
STATUS_INVALID_RESPONSE = "INVALID_RESPONSE"
STATUS_SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
STATUS_REQUEST_FAILED = "REQUEST_FAILED"

AUTH_OK = "AUTH_OK"
AUTH_FAILED = "AUTH_FAILED"
FREE_TIER_ACCESS_LIMIT = "FREE_TIER_ACCESS_LIMIT"
REQUEST_FAILED = "REQUEST_FAILED"

TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}
NO_RETRY_STATUS_CODES = {400, 401, 403, 404}
FUNDAMENTALS_REQUIRED_FIELDS = (
    "ticker",
    "dimension",
    "calendardate",
    "reportperiod",
    "fiscalperiod",
    "date",
    "revenue",
    "gp",
    "opinc",
    "ebit",
    "ebitda",
    "netinc",
    "ncfo",
    "capex",
    "fcf",
    "cashneq",
    "debt",
    "debtc",
    "debtnc",
    "sharesbas",
    "shareswa",
    "shareswadil",
)


@dataclass(frozen=True)
class SharadarRawRecord:
    provider: str
    table: str
    fields: Mapping[str, Any]


@dataclass(frozen=True)
class SharadarResult:
    status: str
    auth_status: str
    http_status: int
    endpoint: str
    url: str
    request_count: int
    records: list[dict[str, Any]]
    payload: Any = None
    error: str = ""

    @property
    def ok(self) -> bool:
        return self.status == STATUS_SUCCESS


def resolve_api_key(api_key: str | None = None, env: Mapping[str, str] | None = None) -> str:
    resolved = api_key or (env or os.environ).get(SHARADAR_API_KEY_ENV)
    if not resolved:
        raise RuntimeError('SHARADAR_API_KEY_NOT_CONFIGURED; set it with: export SHARADAR_API_KEY="YOUR_KEY_HERE"')
    return resolved


def redact_secret(text: str, secrets: Iterable[str | None]) -> str:
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "***")
    redacted = redacted.replace("api_key=", "api_key=***")
    return redacted


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        query.append((key, "***" if key.lower() in {"api_key", "apikey", "key"} else value))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def classify_http_status(status: int, body: str) -> tuple[str, str]:
    body_l = body.lower()
    if 200 <= status < 300:
        return STATUS_SUCCESS, AUTH_OK
    if status == 401:
        return STATUS_AUTH_FAILED, AUTH_FAILED
    if status == 403 and "free tier" in body_l:
        return STATUS_FREE_TIER_LIMIT, FREE_TIER_ACCESS_LIMIT
    if status == 403:
        return STATUS_AUTH_FAILED, AUTH_FAILED
    if status == 429:
        return STATUS_RATE_LIMITED, REQUEST_FAILED
    if status >= 500:
        return STATUS_TRANSIENT_FAILURE, REQUEST_FAILED
    return STATUS_REQUEST_FAILED, REQUEST_FAILED


def parse_payload(body: str, content_type: str = "") -> Any:
    if "json" in content_type.lower():
        return json.loads(body)
    text = body.lstrip()
    if text.startswith("{") or text.startswith("["):
        return json.loads(body)
    if text.startswith("--") or text.upper().startswith("CREATE TABLE"):
        return body
    reader = csv.DictReader(io.StringIO(body))
    return list(reader)


def payload_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "rows", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        if payload and all(not isinstance(v, (list, dict)) for v in payload.values()):
            return [payload]
    return []


class SharadarClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = SHARADAR_DIRECT_BASE_URL,
        timeout_seconds: float = 30.0,
        max_retries: int = 1,
        retry_sleep_seconds: float = 1.0,
        opener: Callable[[Request, float], Any] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self._api_key = api_key if api_key is not None else (env or os.environ).get(SHARADAR_API_KEY_ENV)
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._retry_sleep_seconds = retry_sleep_seconds
        self._opener = opener or self._default_open
        self._sleeper = sleeper
        self.request_count = 0

    @property
    def api_key_configured(self) -> bool:
        return bool(self._api_key)

    def schema(self, table: str = "fundamentals") -> SharadarResult:
        return self._request("GET", f"/schema/{table}", {}, auth=False)

    def table_schema(self, table: str) -> SharadarResult:
        return self._request("GET", f"/schema/{table}", {}, auth=False)

    def fundamentals(
        self,
        *,
        ticker: str | None = None,
        dimension: str | None = None,
        fields: Iterable[str] | None = None,
        date: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        use_legacy_alias: bool = False,
    ) -> SharadarResult:
        params: dict[str, str] = {}
        if ticker:
            params["ticker"] = ticker.strip().upper()
        if dimension:
            params["dimension"] = dimension.strip().upper()
        if fields:
            params["fields"] = ",".join(fields)
        if date:
            params["date"] = date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if limit is not None:
            params["limit"] = str(limit)
        endpoint = "/data/SF1" if use_legacy_alias else "/data/fundamentals"
        result = self._request("GET", endpoint, params, auth=True)
        if result.ok and dimension:
            wanted = dimension.strip().upper()
            filtered = [row for row in result.records if str(row.get("dimension", "")).upper() == wanted]
            result = SharadarResult(
                status=result.status,
                auth_status=result.auth_status,
                http_status=result.http_status,
                endpoint=result.endpoint,
                url=result.url,
                request_count=result.request_count,
                records=filtered,
                payload=result.payload,
                error=result.error,
            )
        return result

    def raw_records(self, table: str, records: Iterable[Mapping[str, Any]]) -> list[SharadarRawRecord]:
        return [SharadarRawRecord(provider="SHARADAR", table=table, fields=dict(row)) for row in records]

    def _request(self, method: str, path: str, params: Mapping[str, str], *, auth: bool) -> SharadarResult:
        if auth and not self._api_key:
            return SharadarResult(
                status=STATUS_AUTH_NOT_CONFIGURED,
                auth_status=AUTH_FAILED,
                http_status=0,
                endpoint=path,
                url=self._safe_url(path, params),
                request_count=0,
                records=[],
                error='SHARADAR_API_KEY_NOT_CONFIGURED; set it with: export SHARADAR_API_KEY="YOUR_KEY_HERE"',
            )
        url = self._url(path, params)
        safe_url = redact_url(url)
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if auth and self._api_key:
            headers["x-api-key"] = self._api_key
        request = Request(url, headers=headers, method=method)

        attempts = self._max_retries + 1
        last_error = ""
        for attempt in range(attempts):
            try:
                self.request_count += 1
                response = self._opener(request, self._timeout_seconds)
                status = int(getattr(response, "status", response.getcode()))
                body = response.read().decode("utf-8", errors="replace")
                headers_map = dict(getattr(response, "headers", {}).items())
            except HTTPError as exc:
                status = int(exc.code)
                body = exc.read().decode("utf-8", errors="replace")
                headers_map = dict(exc.headers.items()) if exc.headers else {}
            except (TimeoutError, URLError, OSError) as exc:
                last_error = redact_secret(f"{type(exc).__name__}:{exc}", [self._api_key])
                if attempt < self._max_retries:
                    self._sleeper(self._retry_sleep_seconds)
                    continue
                return SharadarResult(
                    status=STATUS_TRANSIENT_FAILURE,
                    auth_status=REQUEST_FAILED,
                    http_status=0,
                    endpoint=path,
                    url=safe_url,
                    request_count=self.request_count,
                    records=[],
                    error=last_error,
                )

            status_name, auth_status = classify_http_status(status, body)
            if status_name in {STATUS_RATE_LIMITED, STATUS_TRANSIENT_FAILURE} and attempt < self._max_retries:
                retry_after = _retry_after_seconds(headers_map) or self._retry_sleep_seconds
                self._sleeper(retry_after)
                continue
            if status_name != STATUS_SUCCESS:
                return SharadarResult(
                    status=status_name,
                    auth_status=auth_status,
                    http_status=status,
                    endpoint=path,
                    url=safe_url,
                    request_count=self.request_count,
                    records=[],
                    error=redact_secret(body[:500], [self._api_key]),
                )
            try:
                payload = parse_payload(body, headers_map.get("Content-Type", ""))
            except (csv.Error, json.JSONDecodeError, ValueError) as exc:
                return SharadarResult(
                    status=STATUS_INVALID_RESPONSE,
                    auth_status=auth_status,
                    http_status=status,
                    endpoint=path,
                    url=safe_url,
                    request_count=self.request_count,
                    records=[],
                    error=redact_secret(f"{type(exc).__name__}:{exc}", [self._api_key]),
                )
            return SharadarResult(
                status=STATUS_SUCCESS,
                auth_status=auth_status,
                http_status=status,
                endpoint=path,
                url=safe_url,
                request_count=self.request_count,
                records=payload_records(payload),
                payload=payload,
            )

        return SharadarResult(
            status=STATUS_TRANSIENT_FAILURE,
            auth_status=REQUEST_FAILED,
            http_status=0,
            endpoint=path,
            url=safe_url,
            request_count=self.request_count,
            records=[],
            error=last_error,
        )

    def _url(self, path: str, params: Mapping[str, str]) -> str:
        query = urlencode({key: value for key, value in params.items() if value not in ("", None)})
        return f"{self._base_url}{path}" + (f"?{query}" if query else "")

    def _safe_url(self, path: str, params: Mapping[str, str]) -> str:
        return redact_url(self._url(path, params))

    @staticmethod
    def _default_open(request: Request, timeout_seconds: float) -> Any:
        return urlopen(request, timeout=timeout_seconds)


def _retry_after_seconds(headers: Mapping[str, str]) -> float | None:
    for key, value in headers.items():
        if key.lower() == "retry-after":
            try:
                return max(0.0, min(float(value), 60.0))
            except ValueError:
                return None
    return None


def filter_dimension(records: Iterable[Mapping[str, Any]], dimension: str) -> list[dict[str, Any]]:
    wanted = dimension.upper()
    return [dict(row) for row in records if str(row.get("dimension", "")).upper() == wanted]


def validate_schema_fields(schema_payload: Any, expected_fields: Iterable[str] = FUNDAMENTALS_REQUIRED_FIELDS) -> dict[str, Any]:
    fields = extract_schema_fields(schema_payload)
    missing = [field for field in expected_fields if field not in fields]
    return {
        "status": STATUS_SUCCESS if not missing else STATUS_SCHEMA_MISMATCH,
        "fields_found": sorted(fields),
        "expected_fields": list(expected_fields),
        "missing_expected_fields": missing,
        "expected_fields_found": not missing,
    }


def extract_schema_fields(payload: Any) -> set[str]:
    fields: set[str] = set()
    if isinstance(payload, str):
        fields.update(extract_postgres_schema_fields(payload))
    elif isinstance(payload, dict):
        for key, value in payload.items():
            if key in {"name", "field", "column"} and isinstance(value, str):
                fields.add(value)
            elif key in {"fields", "columns"}:
                fields.update(extract_schema_fields(value))
            elif isinstance(value, (dict, list)):
                fields.update(extract_schema_fields(value))
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, str):
                fields.add(item)
            elif isinstance(item, (dict, list)):
                fields.update(extract_schema_fields(item))
    return fields


def extract_postgres_schema_fields(sql: str) -> set[str]:
    fields: set[str] = set()
    in_table = False
    for raw_line in sql.splitlines():
        line = raw_line.strip()
        upper = line.upper()
        if upper.startswith("CREATE TABLE"):
            in_table = True
            continue
        if not in_table:
            continue
        if line.startswith(");"):
            break
        if not line or line.startswith("--") or upper.startswith(("PRIMARY ", "UNIQUE ", "CONSTRAINT ", "FOREIGN ", "CHECK ", "CREATE INDEX")):
            continue
        match = re.match(r'"?([A-Za-z_][A-Za-z0-9_]*)"?\s+', line.rstrip(","))
        if match:
            fields.add(match.group(1))
    return fields
