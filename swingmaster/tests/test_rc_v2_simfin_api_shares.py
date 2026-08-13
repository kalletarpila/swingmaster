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
        (111052, "2026-03-31", 15000000000.0, "Common Shares Outstanding"),
        (111052, "2026-06-15", 14900000000.0, "Common Shares Outstanding"),
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


def test_default_120_day_boundary_for_prior_matches() -> None:
    for age in (0, 1, 30, 90, 120):
        observations = parse_share_observations(
            [{"ticker": "AGE", "id": 1, "columns": ["Date", "Common Shares Outstanding"], "data": [[_date_days_before("2025-06-30", age), "100"]]}]
        )
        assert match_observation_for_report_date(observations, ticker="AGE", report_date="2025-06-30") is not None
    for age in (121, 153, 181, 184):
        observations = parse_share_observations(
            [{"ticker": "AGE", "id": 1, "columns": ["Date", "Common Shares Outstanding"], "data": [[_date_days_before("2025-06-30", age), "100"]]}]
        )
        assert match_observation_for_report_date(observations, ticker="AGE", report_date="2025-06-30") is None


def test_acquire_uses_separate_shares_cache_and_stops_on_second_429(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"AAPL": _test_simfin_id("AAPL"), "MSFT": _test_simfin_id("MSFT"), "NVDA": _test_simfin_id("NVDA")})
    calls = []
    sleeps = []

    class Client:
        def fetch_ticker(self, ticker: str) -> dict[str, object]:
            calls.append(ticker)
            if ticker == "AAPL":
                return _fetch_result("AAPL", "SUCCESS", 200)
            return _fetch_result(ticker, "RATE_LIMITED", 429)

    result = acquire_simfin_api_shares(
        db_path=db,
        tickers=["AAPL", "MSFT", "NVDA"],
        run_id="RUN1",
        client=Client(),
        request_batch_size=1,
        rate_limit_retry_sleeper=lambda seconds: sleeps.append(seconds),
    )

    assert result["status"] == "SIMFIN_RATE_LIMITED_AFTER_RETRY"
    assert calls == ["AAPL", "MSFT", "MSFT"]
    assert sleeps == [120.0]
    assert result["rows"][1]["http_requests_made"] == 2
    assert result["rows"][1]["first_http_status"] == 429
    assert result["rows"][1]["first_429_detected"] == 1
    assert result["rows"][1]["retry_http_status"] == 429
    assert result["rows"][1]["stopped_after_second_429"] == 1
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 2,
        "http_requests_made": 3,
        "first_429_count": 1,
        "recovered_429_count": 0,
        "second_429_stop_count": 1,
    }
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_shares_raw WHERE ticker='AAPL'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_shares_raw WHERE ticker='MSFT'").fetchone()[0] == 0
        assert conn.execute("SELECT last_status FROM rc_v2_simfin_api_shares_fetch_state WHERE ticker='MSFT'").fetchone()[0] == "RATE_LIMITED"
        assert "rc_v2_simfin_api_raw" not in {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

    calls.clear()
    result = acquire_simfin_api_shares(db_path=db, tickers=["AAPL"], run_id="RUN2", client=Client(), request_batch_size=1)
    assert result["rows"][0]["action"] == "CACHE_HIT"
    assert calls == []


def test_acquire_shares_retries_first_429_once_and_recovers_on_success(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"MSFT": _test_simfin_id("MSFT"), "NVDA": _test_simfin_id("NVDA")})
    calls = []
    sleeps = []

    class Client:
        def fetch_ticker(self, ticker: str) -> dict[str, object]:
            calls.append(ticker)
            if ticker == "MSFT" and calls.count("MSFT") == 1:
                return _fetch_result("MSFT", "RATE_LIMITED", 429)
            return _fetch_result(ticker, "SUCCESS", 200)

    result = acquire_simfin_api_shares(
        db_path=db,
        tickers=["MSFT", "NVDA"],
        run_id="RUN1",
        client=Client(),
        request_batch_size=1,
        rate_limit_retry_delay_seconds=0.5,
        rate_limit_retry_sleeper=lambda seconds: sleeps.append(seconds),
    )

    assert result["status"] == "OK"
    assert calls == ["MSFT", "MSFT", "NVDA"]
    assert sleeps == [0.5]
    assert result["rows"][0]["status"] == "SUCCESS"
    assert result["rows"][0]["recovered_after_429"] == 1
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 2,
        "http_requests_made": 3,
        "first_429_count": 1,
        "recovered_429_count": 1,
        "second_429_stop_count": 0,
    }
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT last_status FROM rc_v2_simfin_api_shares_fetch_state WHERE ticker='MSFT'").fetchone()[0] == "SUCCESS"
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_shares_raw WHERE ticker='MSFT'").fetchone()[0] == 1


def test_acquire_shares_retries_first_429_once_and_continues_on_no_data(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"EMPTY": _test_simfin_id("EMPTY"), "NVDA": _test_simfin_id("NVDA")})
    calls = []

    class Client:
        def fetch_ticker(self, ticker: str) -> dict[str, object]:
            calls.append(ticker)
            if ticker == "EMPTY" and calls.count("EMPTY") == 1:
                return _fetch_result("EMPTY", "RATE_LIMITED", 429)
            if ticker == "EMPTY":
                return _fetch_result("EMPTY", "NO_DATA", 200)
            return _fetch_result(ticker, "SUCCESS", 200)

    result = acquire_simfin_api_shares(
        db_path=db,
        tickers=["EMPTY", "NVDA"],
        run_id="RUN1",
        client=Client(),
        request_batch_size=1,
        rate_limit_retry_sleeper=lambda seconds: None,
    )

    assert result["status"] == "OK"
    assert calls == ["EMPTY", "EMPTY", "NVDA"]
    assert result["rows"][0]["status"] == "NO_DATA"
    assert result["rows"][0]["recovered_after_429"] == 1
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 2,
        "http_requests_made": 3,
        "first_429_count": 1,
        "recovered_429_count": 1,
        "second_429_stop_count": 0,
    }
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT last_status FROM rc_v2_simfin_api_shares_fetch_state WHERE ticker='EMPTY'").fetchone()[0] == "NO_DATA"
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_shares_raw WHERE ticker='EMPTY'").fetchone()[0] == 0


def test_pair_acquire_batches_four_tickers_into_two_http_requests_and_demultiplexes_pid_rows(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"AAA": 101, "BBB": 202, "CCC": 303, "DDD": 404})
    calls = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tuple(tickers))
            payload = []
            for ticker in reversed(tickers):
                simfin_id = {"AAA": 101, "BBB": 202, "CCC": 303, "DDD": 404}[ticker]
                payload.extend(
                    [
                        {"pid": simfin_id, "endDate": "2026-06-30", "value": simfin_id * 10},
                        {"pid": simfin_id, "endDate": "2026-03-31", "value": simfin_id * 9},
                    ]
                )
            return _group_result(tickers, "SUCCESS", 200, payload)

    result = acquire_simfin_api_shares(db_path=db, tickers=["AAA", "BBB", "CCC", "DDD"], run_id="RUN1", client=Client())

    assert result["status"] == "OK"
    assert calls == [("AAA", "BBB"), ("CCC", "DDD")]
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 4,
        "http_requests_made": 2,
        "first_429_count": 0,
        "recovered_429_count": 0,
        "second_429_stop_count": 0,
    }
    with sqlite3.connect(str(db)) as conn:
        for ticker in ("AAA", "BBB", "CCC", "DDD"):
            raw = conn.execute("SELECT payload_json FROM rc_v2_simfin_api_shares_raw WHERE ticker=?", (ticker,)).fetchone()[0]
            observations = parse_share_observations(json.loads(raw))
            assert {obs.simfin_id for obs in observations} == {{"AAA": 101, "BBB": 202, "CCC": 303, "DDD": 404}[ticker]}


def test_pair_acquire_batches_odd_final_ticker(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"AAA": 101, "BBB": 202, "CCC": 303, "DDD": 404, "EEE": 505})
    calls = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tuple(tickers))
            return _group_result(tickers, "SUCCESS", 200, [{"pid": _pid(ticker), "endDate": "2026-03-31", "value": 1} for ticker in tickers])

    result = acquire_simfin_api_shares(db_path=db, tickers=["AAA", "BBB", "CCC", "DDD", "EEE"], run_id="RUN1", client=Client())

    assert result["status"] == "OK"
    assert calls == [("AAA", "BBB"), ("CCC", "DDD"), ("EEE",)]
    assert result["request_accounting"]["http_requests_made"] == 3


def test_pair_acquire_rejects_batch_size_above_two(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)

    with pytest.raises(ValueError, match="SIMFIN_API_SHARES_REQUEST_BATCH_SIZE_MAX_2"):
        acquire_simfin_api_shares(db_path=db, tickers=["AAA", "BBB", "CCC"], run_id="RUN1", request_batch_size=3)


def test_pair_acquire_unknown_pid_is_malformed_not_silent_assignment(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"AAA": 101, "BBB": 202})

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            return _group_result(tickers, "SUCCESS", 200, [{"pid": 999, "endDate": "2026-03-31", "value": 1}])

    result = acquire_simfin_api_shares(db_path=db, tickers=["AAA", "BBB"], run_id="RUN1", client=Client())

    assert result["status"] == "SIMFIN_SHARES_PAIR_RESPONSE_MAPPING_FAILURE"
    assert [row["status"] for row in result["rows"]] == ["MALFORMED_RESPONSE", "MALFORMED_RESPONSE"]
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_shares_raw").fetchone()[0] == 0


def test_pair_acquire_one_cached_one_actionable_requests_only_actionable(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"AAA": 101, "BBB": 202})
    with sqlite3.connect(str(db)) as conn:
        ensure_schema(conn)
        persist_fetch_result(conn, market="usa", run_id="RAW1", result=_fetch_result("AAA", "SUCCESS", 200))
        conn.commit()
    calls = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tuple(tickers))
            return _group_result(tickers, "SUCCESS", 200, [{"pid": 202, "endDate": "2026-03-31", "value": 1}])

    result = acquire_simfin_api_shares(db_path=db, tickers=["AAA", "BBB"], run_id="RUN1", client=Client())

    assert [row["action"] for row in result["rows"]] == ["CACHE_HIT", "FETCHED"]
    assert calls == [("BBB",)]


def test_pair_429_retries_same_pair_once_then_success(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"AAA": 101, "BBB": 202})
    calls = []
    sleeps = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tuple(tickers))
            if len(calls) == 1:
                return _group_result(tickers, "RATE_LIMITED", 429, {"error": "quota"})
            return _group_result(tickers, "SUCCESS", 200, [{"pid": _pid(ticker), "endDate": "2026-03-31", "value": 1} for ticker in tickers])

    result = acquire_simfin_api_shares(
        db_path=db,
        tickers=["AAA", "BBB"],
        run_id="RUN1",
        client=Client(),
        rate_limit_retry_sleeper=lambda seconds: sleeps.append(seconds),
    )

    assert result["status"] == "OK"
    assert calls == [("AAA", "BBB"), ("AAA", "BBB")]
    assert sleeps == [120.0]
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 2,
        "http_requests_made": 2,
        "first_429_count": 1,
        "recovered_429_count": 1,
        "second_429_stop_count": 0,
    }


def test_pair_429_twice_stops_without_later_pair_and_preserves_first_pair(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"AAA": 101, "BBB": 202, "CCC": 303, "DDD": 404, "EEE": 505, "FFF": 606})
    calls = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tuple(tickers))
            if tuple(tickers) == ("AAA", "BBB"):
                return _group_result(tickers, "SUCCESS", 200, [{"pid": _pid(ticker), "endDate": "2026-03-31", "value": 1} for ticker in tickers])
            return _group_result(tickers, "RATE_LIMITED", 429, {"error": "quota"})

    result = acquire_simfin_api_shares(
        db_path=db,
        tickers=["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"],
        run_id="RUN1",
        client=Client(),
        rate_limit_retry_sleeper=lambda seconds: None,
    )

    assert result["status"] == "SIMFIN_RATE_LIMITED_AFTER_RETRY"
    assert calls == [("AAA", "BBB"), ("CCC", "DDD"), ("CCC", "DDD")]
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 4,
        "http_requests_made": 3,
        "first_429_count": 1,
        "recovered_429_count": 0,
        "second_429_stop_count": 1,
    }
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_shares_raw WHERE ticker IN ('AAA','BBB')").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_shares_fetch_state WHERE ticker IN ('EEE','FFF')").fetchone()[0] == 0


def test_pair_cache_replay_makes_zero_provider_requests(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"AAA": 101, "BBB": 202})
    with sqlite3.connect(str(db)) as conn:
        ensure_schema(conn)
        persist_fetch_result(conn, market="usa", run_id="RAW1", result=_fetch_result("AAA", "SUCCESS", 200))
        persist_fetch_result(conn, market="usa", run_id="RAW1", result=_fetch_result("BBB", "NO_DATA", 200))
        conn.commit()

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            raise AssertionError("cache replay should not call provider")

    result = acquire_simfin_api_shares(db_path=db, tickers=["AAA", "BBB"], run_id="RUN2", client=Client())

    assert result["status"] == "OK"
    assert [row["action"] for row in result["rows"]] == ["CACHE_HIT", "NO_DATA_CACHE_HIT"]
    assert result["request_accounting"]["http_requests_made"] == 0


def test_pair_batch_size_one_still_works(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    _insert_companies(db, {"AAA": 101, "BBB": 202})
    calls = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tuple(tickers))
            return _group_result(tickers, "SUCCESS", 200, [{"pid": _pid(ticker), "endDate": "2026-03-31", "value": 1} for ticker in tickers])

    result = acquire_simfin_api_shares(db_path=db, tickers=["AAA", "BBB"], run_id="RUN1", client=Client(), request_batch_size=1)

    assert result["status"] == "OK"
    assert calls == [("AAA",), ("BBB",)]
    assert result["request_accounting"]["http_requests_made"] == 2


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
        weighted = conn.execute(
            "SELECT weighted_average_shares_basic, weighted_average_shares_diluted FROM rc_v2_fundamental_quarterly WHERE shares_outstanding=15000000000.0"
        ).fetchone()
        assert weighted["weighted_average_shares_basic"] == 111.0
        assert weighted["weighted_average_shares_diluted"] == 222.0
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
        assert source_value["quarter_report_date"] == "2026-03-31"
        assert source_value["age_days"] == 0


def test_apply_default_rejects_stale_121_day_prior(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        persist_fetch_result(
            conn,
            market="usa",
            run_id="RAW1",
            result={
                "ticker": "AAPL",
                "retrieved_at_utc": "2026-08-13T00:00:00Z",
                "http_status": 200,
                "provider_status": "SUCCESS",
                "payload_json": json.dumps([{"pid": 111052, "endDate": "2026-01-01", "value": 15000000000}], sort_keys=True),
                "safe_headers_json": "{}",
            },
        )
        _insert_company_quarter(conn, "AAPL", "2026-05-02", None)
        conn.commit()

    result = apply_simfin_api_shares(db_path=db, tickers=["AAPL"], run_id="APPLY1")

    assert result["rows"][0]["updated"] == 0
    assert result["rows"][0]["unmatched"] == 1
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT shares_outstanding FROM rc_v2_fundamental_quarterly").fetchone()[0] is None
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source WHERE field_name='shares_outstanding'").fetchone()[0] == 0


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
    _insert_companies(db, {"AAPL": _test_simfin_id("AAPL"), "MSFT": _test_simfin_id("MSFT"), "NVDA": _test_simfin_id("NVDA")})
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
        ticker_param = request.full_url.rsplit("=", 1)[-1]
        calls.append(ticker_param)
        clock.advance(0.1)

        class Response:
            status = 200
            headers = {}

            def read(self) -> bytes:
                payload = []
                for ticker in ticker_param.split(","):
                    payload.extend(_shares_payload(ticker))
                return json.dumps(payload, sort_keys=True).encode("utf-8")

        return Response()

    monkeypatch.setattr("swingmaster.fundamentals_v2.simfin_api_shares.SimFinSharesClient._default_open", staticmethod(opener))

    result = acquire_simfin_api_shares(db_path=db, tickers=["AAPL", "MSFT", "NVDA"], run_id="RUN1", min_interval_seconds=2.1)
    assert result["status"] == "OK"
    assert calls == ["AAPL,MSFT", "NVDA"]
    assert sleeps == [pytest.approx(2.0)]


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


def _insert_companies(path: Path, simfin_ids: dict[str, int]) -> None:
    now = "2026-08-13T00:00:00Z"
    with sqlite3.connect(str(path)) as conn:
        for ticker, simfin_id in simfin_ids.items():
            conn.execute(
                """
                INSERT INTO rc_v2_company (market, ticker, simfin_id, company_name, company_profile, active, created_at_utc, updated_at_utc)
                VALUES ('usa', ?, ?, ?, 'ORDINARY', 1, ?, ?)
                ON CONFLICT(simfin_id) DO UPDATE SET ticker=excluded.ticker
                """,
                (ticker, simfin_id, ticker, now, now),
            )
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
            quarter_id, shares_outstanding, weighted_average_shares_basic, weighted_average_shares_diluted,
            available_canonical_field_count, has_income, has_balance,
            has_cashflow, seed_status, missing_seed_fields_json, created_at_utc, updated_at_utc
        ) VALUES (?, ?, 111.0, 222.0, 0, 1, 1, 1, 'fixture', '[]', ?, ?)
        """,
        (quarter[0], shares, now, now),
    )


def _date_days_before(report_date: str, days: int) -> str:
    from datetime import date, timedelta

    return (date.fromisoformat(report_date) - timedelta(days=days)).isoformat()


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


def _group_result(tickers: list[str], status: str, http_status: int, payload: object) -> dict[str, object]:
    return {
        "tickers": tickers,
        "retrieved_at_utc": "2026-08-13T00:00:00Z",
        "http_status": http_status,
        "provider_status": status,
        "payload_json": json.dumps(payload, sort_keys=True),
        "safe_headers_json": "{}",
    }


def _pid(ticker: str) -> int:
    return {"AAA": 101, "BBB": 202, "CCC": 303, "DDD": 404, "EEE": 505, "FFF": 606}[ticker]


def _shares_payload(ticker: str) -> list[dict[str, object]]:
    return [
        {
            "ticker": ticker,
            "id": _test_simfin_id(ticker),
            "currency": "USD",
            "columns": ["Date", "Common Shares Outstanding"],
            "data": [
                ["2026-03-31", "15000000000"],
                ["2026-06-15", "14900000000"],
                ["2026-07-01", "14800000000"],
            ],
        }
    ]


def _test_simfin_id(ticker: str) -> int:
    return {"AAPL": 111052, "MSFT": 59265, "NVDA": 477647, "EMPTY": 1}.get(ticker, 111052)
