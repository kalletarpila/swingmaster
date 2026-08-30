from __future__ import annotations

import json
from io import BytesIO
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request

from swingmaster.providers.sharadar import (
    AUTH_OK,
    FREE_TIER_ACCESS_LIMIT,
    SHARADAR_DIRECT_BASE_URL,
    STATUS_AUTH_FAILED,
    STATUS_AUTH_NOT_CONFIGURED,
    STATUS_FREE_TIER_LIMIT,
    STATUS_RATE_LIMITED,
    STATUS_SUCCESS,
    STATUS_TRANSIENT_FAILURE,
    SharadarClient,
    SharadarRawRecord,
    extract_schema_fields,
    redact_secret,
    redact_url,
    validate_schema_fields,
)


class FakeResponse:
    def __init__(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self.headers = headers or {"Content-Type": "application/json"}
        self._payload = payload

    def read(self) -> bytes:
        if isinstance(self._payload, bytes):
            return self._payload
        if isinstance(self._payload, str):
            return self._payload.encode("utf-8")
        return json.dumps(self._payload).encode("utf-8")

    def getcode(self) -> int:
        return self.status


class FakeOpener:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.requests: list[Request] = []

    def __call__(self, request: Request, timeout: float) -> Any:
        del timeout
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def http_error(status: int, body: str) -> HTTPError:
    return HTTPError("https://api.sharadar.com/v1.0/data/fundamentals", status, "error", {}, BytesIO(body.encode("utf-8")))


def test_missing_api_key_classified_without_network() -> None:
    client = SharadarClient(env={}, opener=FakeOpener([]))
    result = client.fundamentals(ticker="AAPL")
    assert result.status == STATUS_AUTH_NOT_CONFIGURED
    assert result.request_count == 0
    assert 'export SHARADAR_API_KEY="YOUR_KEY_HERE"' in result.error


def test_api_key_loaded_from_env_and_sent_as_header() -> None:
    opener = FakeOpener([FakeResponse(200, [{"ticker": "AAPL", "dimension": "ARQ"}])])
    client = SharadarClient(env={"SHARADAR_API_KEY": "secret-key"}, opener=opener)
    result = client.fundamentals(ticker="AAPL")
    assert result.status == STATUS_SUCCESS
    assert opener.requests[0].headers["X-api-key"] == "secret-key"
    assert "secret-key" not in result.url


def test_api_key_never_logged_or_returned_in_errors() -> None:
    opener = FakeOpener([URLError("secret-key unavailable")])
    client = SharadarClient(api_key="secret-key", opener=opener, max_retries=0)
    result = client.fundamentals(ticker="AAPL")
    assert "secret-key" not in result.error
    assert "secret-key" not in result.url
    assert redact_secret("api_key=secret-key", ["secret-key"]) == "api_key=******"


def test_modern_endpoint_url_correct() -> None:
    opener = FakeOpener([FakeResponse(200, [])])
    client = SharadarClient(api_key="key", opener=opener)
    client.fundamentals(ticker="AAPL")
    assert opener.requests[0].full_url == f"{SHARADAR_DIRECT_BASE_URL}/data/fundamentals?ticker=AAPL"


def test_legacy_sf1_alias_supported_for_smoke() -> None:
    opener = FakeOpener([FakeResponse(200, [])])
    client = SharadarClient(api_key="key", opener=opener)
    result = client.fundamentals(ticker="AAPL", use_legacy_alias=True)
    assert result.status == STATUS_SUCCESS
    assert opener.requests[0].full_url.endswith("/data/SF1?ticker=AAPL")


def test_aapl_fundamentals_parsed() -> None:
    opener = FakeOpener([FakeResponse(200, [{"ticker": "AAPL", "dimension": "ARQ", "revenue": 1}])])
    result = SharadarClient(api_key="key", opener=opener).fundamentals(ticker="AAPL")
    assert result.records == [{"ticker": "AAPL", "dimension": "ARQ", "revenue": 1}]


def test_arq_filtering() -> None:
    opener = FakeOpener([FakeResponse(200, [{"dimension": "ARQ"}, {"dimension": "MRQ"}])])
    result = SharadarClient(api_key="key", opener=opener).fundamentals(ticker="AAPL", dimension="ARQ")
    assert result.records == [{"dimension": "ARQ"}]


def test_mrq_filtering() -> None:
    opener = FakeOpener([FakeResponse(200, [{"dimension": "ARQ"}, {"dimension": "MRQ"}])])
    result = SharadarClient(api_key="key", opener=opener).fundamentals(ticker="AAPL", dimension="MRQ")
    assert result.records == [{"dimension": "MRQ"}]


def test_field_projection_query() -> None:
    opener = FakeOpener([FakeResponse(200, [{"ticker": "AAPL", "revenue": 1}])])
    SharadarClient(api_key="key", opener=opener).fundamentals(ticker="AAPL", fields=["ticker", "revenue"])
    assert "fields=ticker%2Crevenue" in opener.requests[0].full_url


def test_401_classified_auth_failed() -> None:
    opener = FakeOpener([http_error(401, "invalid api key")])
    result = SharadarClient(api_key="key", opener=opener).fundamentals(ticker="AAPL")
    assert result.status == STATUS_AUTH_FAILED


def test_403_exceeds_free_tier_classified() -> None:
    opener = FakeOpener([http_error(403, "Exceeds free tier")])
    result = SharadarClient(api_key="key", opener=opener).fundamentals(ticker="WDAY")
    assert result.status == STATUS_FREE_TIER_LIMIT
    assert result.auth_status == FREE_TIER_ACCESS_LIMIT


def test_429_retry_policy() -> None:
    opener = FakeOpener([http_error(429, "rate limit"), FakeResponse(200, [{"ticker": "AAPL"}])])
    result = SharadarClient(api_key="key", opener=opener, retry_sleep_seconds=0, sleeper=lambda _: None).fundamentals(ticker="AAPL")
    assert result.status == STATUS_SUCCESS
    assert len(opener.requests) == 2


def test_transient_5xx_retry() -> None:
    opener = FakeOpener([http_error(503, "try later"), FakeResponse(200, [{"ticker": "AAPL"}])])
    result = SharadarClient(api_key="key", opener=opener, retry_sleep_seconds=0, sleeper=lambda _: None).fundamentals(ticker="AAPL")
    assert result.status == STATUS_SUCCESS
    assert len(opener.requests) == 2


def test_no_retry_on_403() -> None:
    opener = FakeOpener([http_error(403, "Exceeds free tier"), FakeResponse(200, [])])
    result = SharadarClient(api_key="key", opener=opener).fundamentals(ticker="WDAY")
    assert result.status == STATUS_FREE_TIER_LIMIT
    assert len(opener.requests) == 1


def test_schema_field_validation() -> None:
    schema = [{"name": "ticker"}, {"name": "dimension"}, {"name": "revenue"}]
    validation = validate_schema_fields(schema, ["ticker", "dimension"])
    assert validation["expected_fields_found"] is True


def test_postgres_schema_text_field_validation() -> None:
    schema = """
    CREATE TABLE IF NOT EXISTS fundamentals (
      ticker text NOT NULL,
      dimension text NOT NULL,
      calendardate date,
      reportperiod date NOT NULL,
      fiscalperiod text,
      revenue bigint
    );
    """
    assert {"ticker", "dimension", "calendardate", "reportperiod", "fiscalperiod", "revenue"} <= extract_schema_fields(schema)


def test_provider_record_stays_separate_from_canonical_model() -> None:
    client = SharadarClient(api_key="key", opener=FakeOpener([]))
    records = client.raw_records("fundamentals", [{"ticker": "AAPL", "sharesbas": 1}])
    assert records == [SharadarRawRecord(provider="SHARADAR", table="fundamentals", fields={"ticker": "AAPL", "sharesbas": 1})]
    assert "shares_outstanding" not in records[0].fields


def test_no_production_writes_contract() -> None:
    client = SharadarClient(api_key="key", opener=FakeOpener([]))
    assert not hasattr(client, "persist")
    assert not hasattr(client, "write_v3")
    assert not hasattr(client, "write_rawcandle")


def test_redact_url_query_string_key() -> None:
    assert redact_url("https://x.test/path?api_key=secret&ticker=AAPL") == "https://x.test/path?api_key=%2A%2A%2A&ticker=AAPL"
