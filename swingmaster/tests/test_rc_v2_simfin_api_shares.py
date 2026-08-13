from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from urllib.request import Request

import pytest

from swingmaster.fundamentals_v2.simfin_api_shares import (
    RequestStartRateLimiter,
    acquire_simfin_api_shares,
    apply_simfin_api_shares,
    classify_http_status,
    ensure_schema,
    match_observation_for_report_date,
    parse_share_observations,
    persist_fetch_result,
)
from swingmaster.fundamentals_v2.simfin_seed import create_schema


def test_parse_compact_shares_payload_and_match_latest_prior() -> None:
    observations = parse_share_observations(_shares_payload("AAPL"))

    assert len(observations) == 3
    assert observations[0].ticker == "AAPL"
    assert observations[0].simfin_id == 111052
    assert observations[0].provider_field == "Common Shares Outstanding"
    exact = match_observation_for_report_date(observations, ticker="AAPL", report_date="2026-03-31")
    prior = match_observation_for_report_date(observations, ticker="AAPL", report_date="2026-06-30")
    no_future = match_observation_for_report_date(observations, ticker="AAPL", report_date="2025-12-31")

    assert exact is not None
    assert exact.match_type == "EXACT_DATE"
    assert exact.observation_date == "2026-03-31"
    assert prior is not None
    assert prior.match_type == "PRIOR_OBSERVATION"
    assert prior.observation_date == "2026-06-15"
    assert prior.age_days == 15
    assert no_future is None


def test_parse_live_shares_endpoint_pid_enddate_value_rows() -> None:
    observations = parse_share_observations(
        [
            {"pid": 111052, "endDate": "2026-03-31", "value": 15000000000},
            {"pid": 111052, "endDate": "2026-06-15", "value": "14900000000"},
        ]
    )

    assert [(obs.simfin_id, obs.observation_date, obs.shares_outstanding, obs.provider_field) for obs in observations] == [
        (111052, "2026-03-31", 15000000000.0, "value"),
        (111052, "2026-06-15", 14900000000.0, "value"),
    ]


def test_no_lookahead_prefers_prior_observation_over_future() -> None:
    observations = parse_share_observations(
        [
            {
                "ticker": "LOOK",
                "id": 7,
                "columns": ["Date", "Common Shares Outstanding"],
                "data": [["2025-03-15", "100"], ["2025-04-10", "200"]],
            }
        ]
    )

    match = match_observation_for_report_date(observations, ticker="LOOK", report_date="2025-03-31")

    assert match is not None
    assert match.observation_date == "2025-03-15"
    assert match.shares_outstanding == 100.0


def test_exact_date_wins_over_older_prior_observation() -> None:
    observations = parse_share_observations(
        [
            {
                "ticker": "EXACT",
                "id": 8,
                "columns": ["Date", "Common Shares Outstanding"],
                "data": [["2025-03-15", "100"], ["2025-03-31", "150"]],
            }
        ]
    )

    match = match_observation_for_report_date(observations, ticker="EXACT", report_date="2025-03-31")

    assert match is not None
    assert match.match_type == "EXACT_DATE"
    assert match.shares_outstanding == 150.0


def test_null_and_malformed_shares_rows_are_not_successful() -> None:
    payload = [{"ticker": "NULLS", "id": 9, "columns": ["Date", "Common Shares Outstanding"], "data": [["2025-03-31", None]]}]

    assert parse_share_observations(payload) == []
    assert classify_http_status(200, json.dumps(payload)) == "NO_DATA"
    assert classify_http_status(200, "{") == "MALFORMED_RESPONSE"


def test_age_threshold_blocks_stale_prior_when_configured() -> None:
    observations = parse_share_observations(
        [{"ticker": "OLD", "id": 10, "columns": ["Date", "Common Shares Outstanding"], "data": [["2024-01-01", "100"]]}]
    )

    assert match_observation_for_report_date(observations, ticker="OLD", report_date="2025-03-31", max_age_days=None) is not None
    assert match_observation_for_report_date(observations, ticker="OLD", report_date="2025-03-31", max_age_days=90) is None


def test_acquire_uses_separate_shares_cache_and_stops_on_429(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    calls = []

    class Client:
        def fetch_ticker(self, ticker: str) -> dict[str, object]:
            calls.append(ticker)
            if ticker == "AAPL":
                return _fetch_result("AAPL", "SUCCESS", 200)
            return _fetch_result(ticker, "RATE_LIMITED", 429)

    result = acquire_simfin_api_shares(db_path=db, tickers=["AAPL", "MSFT", "NVDA"], run_id="RUN1", client=Client())

    assert result["status"] == "SIMFIN_RATE_LIMITED"
    assert calls == ["AAPL", "MSFT"]
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_shares_raw WHERE ticker='AAPL'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_shares_raw WHERE ticker='MSFT'").fetchone()[0] == 0
        assert conn.execute("SELECT last_status FROM rc_v2_simfin_api_shares_fetch_state WHERE ticker='MSFT'").fetchone()[0] == "RATE_LIMITED"
        assert "rc_v2_simfin_api_raw" not in {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

    calls.clear()
    result = acquire_simfin_api_shares(db_path=db, tickers=["AAPL"], run_id="RUN2", client=Client())
    assert result["rows"][0]["action"] == "CACHE_HIT"
    assert calls == []


def test_apply_fills_only_null_shares_and_records_match_metadata(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        persist_fetch_result(conn, market="usa", run_id="RAW1", result=_fetch_result("AAPL", "SUCCESS", 200))
        _insert_company_quarter(conn, "AAPL", "2026-03-31", None)
        _insert_company_quarter(conn, "AAPL", "2026-06-30", 999.0)
        conn.commit()

    result = apply_simfin_api_shares(db_path=db, tickers=["AAPL"], run_id="APPLY1")

    assert result["rows"][0]["updated"] == 1
    assert result["rows"][0]["conflicts"] == 1
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        rows = {
            row["report_date"]: row
            for row in conn.execute(
                """
                SELECT q.report_date, f.shares_outstanding
                FROM rc_v2_fundamental_quarterly f
                JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id
                ORDER BY q.report_date
                """
            )
        }
        assert rows["2026-03-31"]["shares_outstanding"] == 15000000000.0
        assert rows["2026-06-30"]["shares_outstanding"] == 999.0
        source = conn.execute(
            """
            SELECT provider, provider_field, source_dataset, transformation, source_value
            FROM rc_v2_fundamental_field_source
            WHERE field_name='shares_outstanding'
            """
        ).fetchone()
        assert source["provider"] == "SIMFIN_API_SHARES"
        assert source["source_dataset"] == "common-shares-outstanding"
        assert source["transformation"] == "none"
        source_value = json.loads(source["source_value"])
        assert source_value["match_type"] == "EXACT_DATE"
        assert source_value["source_observation_date"] == "2026-03-31"


def test_apply_replay_is_idempotent_without_provenance_churn(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        persist_fetch_result(conn, market="usa", run_id="RAW1", result=_fetch_result("AAPL", "SUCCESS", 200))
        _insert_company_quarter(conn, "AAPL", "2026-03-31", None)
        conn.commit()

    first = apply_simfin_api_shares(db_path=db, tickers=["AAPL"], run_id="APPLY1")
    second = apply_simfin_api_shares(db_path=db, tickers=["AAPL"], run_id="APPLY2")

    assert first["rows"][0]["updated"] == 1
    assert second["rows"][0]["updated"] == 0
    assert second["rows"][0]["unchanged"] == 1
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source WHERE field_name='shares_outstanding'").fetchone()[0] == 1


def test_no_data_state_is_terminal_cache_hit(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    with sqlite3.connect(str(db)) as conn:
        persist_fetch_result(conn, market="usa", run_id="RUN1", result=_fetch_result("EMPTY", "NO_DATA", 200))
        conn.commit()

    class Client:
        def fetch_ticker(self, ticker: str) -> dict[str, object]:
            raise AssertionError("NO_DATA state should not be refetched without force_refresh")

    result = acquire_simfin_api_shares(db_path=db, tickers=["EMPTY"], run_id="RUN2", client=Client())
    assert result["rows"] == [{"ticker": "EMPTY", "action": "NO_DATA_CACHE_HIT", "status": "NO_DATA", "raw_id": ""}]


def test_default_client_reuses_rate_limiter_across_tickers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    clock = FakeClock()
    sleeps = []
    calls = []
    monkeypatch.setenv("SIMFIN_API_KEY", "test-key")
    monkeypatch.setattr("swingmaster.fundamentals_v2.simfin_api_shares.urlencode", lambda params: f"ticker={params['ticker']}")

    class InstrumentedRateLimiter(RequestStartRateLimiter):
        def __init__(self, min_interval_seconds: float = 2.1) -> None:
            super().__init__(
                min_interval_seconds,
                monotonic=clock.monotonic,
                sleeper=lambda seconds: sleeps.append(seconds) or clock.advance(seconds),
            )

    monkeypatch.setattr("swingmaster.fundamentals_v2.simfin_api_shares.RequestStartRateLimiter", InstrumentedRateLimiter)

    def opener(request: Request, timeout_seconds: float) -> object:
        ticker = request.full_url.rsplit("=", 1)[-1]
        calls.append(ticker)
        clock.advance(0.1)

        class Response:
            status = 200
            headers = {}

            def read(self) -> bytes:
                return json.dumps(_shares_payload(ticker), sort_keys=True).encode("utf-8")

        return Response()

    monkeypatch.setattr("swingmaster.fundamentals_v2.simfin_api_shares.SimFinSharesClient._default_open", staticmethod(opener))

    result = acquire_simfin_api_shares(db_path=db, tickers=["AAPL", "MSFT", "NVDA"], run_id="RUN1", min_interval_seconds=2.1)
    assert result["status"] == "OK"
    assert calls == ["AAPL", "MSFT", "NVDA"]
    assert sleeps == [pytest.approx(2.0), pytest.approx(2.0)]


def test_classify_http_status_requires_parseable_shares_rows() -> None:
    assert classify_http_status(200, json.dumps(_shares_payload("AAPL"))) == "SUCCESS"
    assert classify_http_status(200, json.dumps([{"ticker": "AAPL", "id": 111052, "data": []}])) == "NO_DATA"
    assert classify_http_status(429, "quota") == "RATE_LIMITED"


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _write_v2_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        create_schema(conn)
        conn.commit()


def _insert_company_quarter(conn: sqlite3.Connection, ticker: str, report_date: str, shares: float | None) -> None:
    now = "2026-08-13T00:00:00Z"
    company = conn.execute(
        """
        INSERT INTO rc_v2_company (market, ticker, simfin_id, company_name, company_profile, active, created_at_utc, updated_at_utc)
        VALUES ('usa', ?, 111052, ?, 'ORDINARY', 1, ?, ?)
        ON CONFLICT(simfin_id) DO UPDATE SET ticker=excluded.ticker
        RETURNING company_id
        """,
        (ticker, ticker, now, now),
    ).fetchone()
    company_id = company[0]
    quarter = conn.execute(
        """
        INSERT INTO rc_v2_quarter (
            company_id, fiscal_year, fiscal_period, report_date, quarter_identity_source,
            has_income, has_balance, has_cashflow, created_at_utc, updated_at_utc
        ) VALUES (?, 2026, 'Q1', ?, 'fixture', 1, 1, 1, ?, ?)
        RETURNING quarter_id
        """,
        (company_id, report_date, now, now),
    ).fetchone()
    conn.execute(
        """
        INSERT INTO rc_v2_fundamental_quarterly (
            quarter_id, shares_outstanding, available_canonical_field_count, has_income, has_balance,
            has_cashflow, seed_status, missing_seed_fields_json, created_at_utc, updated_at_utc
        ) VALUES (?, ?, 0, 1, 1, 1, 'fixture', '[]', ?, ?)
        """,
        (quarter[0], shares, now, now),
    )


def _fetch_result(ticker: str, status: str, http_status: int) -> dict[str, object]:
    payload = _shares_payload(ticker) if status == "SUCCESS" else [{"ticker": ticker, "id": 1, "data": []}]
    return {
        "ticker": ticker,
        "retrieved_at_utc": "2026-08-13T00:00:00Z",
        "http_status": http_status,
        "provider_status": status,
        "payload_json": json.dumps(payload, sort_keys=True),
        "safe_headers_json": "{}",
    }


def _shares_payload(ticker: str) -> list[dict[str, object]]:
    return [
        {
            "ticker": ticker,
            "id": 111052,
            "currency": "USD",
            "columns": ["Date", "Common Shares Outstanding"],
            "data": [
                ["2026-03-31", "15000000000"],
                ["2026-06-15", "14900000000"],
                ["2026-07-01", "14800000000"],
            ],
        }
    ]
