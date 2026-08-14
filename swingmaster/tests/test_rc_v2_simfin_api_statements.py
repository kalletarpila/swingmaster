from __future__ import annotations

import json
import csv
import sqlite3
from pathlib import Path
from urllib.request import Request

import pytest

from swingmaster.fundamentals_v2.simfin_api_statements import (
    RequestStartRateLimiter,
    acquire_simfin_api_statements,
    apply_simfin_api_statements,
    build_candidate_inventory,
    classify_http_status,
    ensure_schema,
    map_api_ordinary_fields,
    persist_fetch_result,
)
from swingmaster.fundamentals_v2.simfin_seed import create_schema


def test_candidate_inventory_excludes_local_profiles_and_selects_ordinary_gap(tmp_path: Path) -> None:
    simfin = _fixture_simfin_dir(tmp_path / "simfin")
    legacy = tmp_path / "legacy.db"
    _write_legacy_db(legacy, ["LOCAL", "BANK", "INS", "AAPL", "NOMETA"])
    v2 = tmp_path / "v2.db"
    _write_v2_db(v2)

    inventory = build_candidate_inventory(legacy_db=legacy, v2_db=v2, simfin_dir=simfin)

    assert [row["ticker"] for row in inventory.candidate_rows] == ["AAPL"]
    excluded = {row["ticker"]: row["classification"] for row in inventory.excluded_rows}
    assert excluded["LOCAL"] == "ORDINARY_LOCAL_COVERED"
    assert excluded["BANK"] == "BANK"
    assert excluded["INS"] == "INSURANCE"
    assert excluded["NOMETA"] == "NO_SIMFIN_COMPANY_METADATA"
    assert inventory.summary["network_required_count"] == 1


def test_rate_limiter_uses_request_start_spacing_and_default_margin() -> None:
    clock = FakeClock()
    sleeps = []
    limiter = RequestStartRateLimiter(monotonic=clock.monotonic, sleeper=lambda seconds: sleeps.append(seconds) or clock.advance(seconds))
    assert limiter.min_interval_seconds == 2.1
    limiter.wait_for_next_start()
    clock.advance(0.4)
    limiter.wait_for_next_start()
    assert sleeps == [pytest.approx(1.7)]


def test_acquire_default_client_reuses_rate_limiter_across_tickers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    clock = FakeClock()
    sleeps = []
    calls = []
    monkeypatch.setenv("SIMFIN_API_KEY", "test-key")
    class InstrumentedRateLimiter(RequestStartRateLimiter):
        def __init__(self, min_interval_seconds: float = 2.1) -> None:
            super().__init__(
                min_interval_seconds,
                monotonic=clock.monotonic,
                sleeper=lambda seconds: sleeps.append(seconds) or clock.advance(seconds),
            )

    monkeypatch.setattr(
        "swingmaster.fundamentals_v2.simfin_api_statements.RequestStartRateLimiter",
        InstrumentedRateLimiter,
    )

    def opener(request: Request, timeout_seconds: float) -> object:
        query = request.full_url.split("?", 1)[1]
        ticker_value = dict(part.split("=", 1) for part in query.split("&"))["ticker"]
        calls.append(ticker_value)
        clock.advance(0.1)

        class Response:
            status = 200
            headers = {}

            def read(self) -> bytes:
                return json.dumps([_payload_company(ticker) for ticker in ticker_value.split("%2C")], sort_keys=True).encode("utf-8")

        return Response()

    monkeypatch.setattr(
        "swingmaster.fundamentals_v2.simfin_api_statements.SimFinStatementClient._default_open",
        staticmethod(opener),
    )

    result = acquire_simfin_api_statements(db_path=db, tickers=["AAPL", "MSFT", "NVDA"], run_id="RUN1", min_interval_seconds=2.1)
    assert result["status"] == "OK"
    assert calls == ["AAPL%2CMSFT", "NVDA"]
    assert sleeps == [pytest.approx(2.0)]


def test_acquire_cache_first_and_second_429_stops_without_erasing_success(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    calls = []
    sleeps = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(",".join(tickers))
            if tickers == ["AAPL", "MSFT"]:
                return _group_fetch_result(tickers, "SUCCESS", 200)
            return _group_fetch_result(tickers, "RATE_LIMITED", 429)

    result = acquire_simfin_api_statements(
        db_path=db,
        tickers=["AAPL", "MSFT", "NVDA"],
        run_id="RUN1",
        client=Client(),
        rate_limit_retry_sleeper=lambda seconds: sleeps.append(seconds),
    )
    assert result["status"] == "SIMFIN_RATE_LIMITED_AFTER_RETRY"
    assert calls == ["AAPL,MSFT", "NVDA", "NVDA"]
    assert sleeps == [300.0]
    assert result["rows"][2]["http_requests_made"] == 2
    assert result["rows"][2]["first_http_status"] == 429
    assert result["rows"][2]["first_429_detected"] == 1
    assert result["rows"][2]["retry_http_status"] == 429
    assert result["rows"][2]["stopped_after_second_429"] == 1
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 3,
        "http_requests_made": 3,
        "first_429_count": 1,
        "recovered_429_count": 0,
        "second_429_stop_count": 1,
    }

    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        raw_count = conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_raw WHERE ticker='AAPL'").fetchone()[0]
        msft_raw = conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_raw WHERE ticker='MSFT'").fetchone()[0]
        state = conn.execute("SELECT last_status FROM rc_v2_simfin_api_fetch_state WHERE ticker='NVDA'").fetchone()[0]
    assert raw_count == 1
    assert msft_raw == 1
    assert state == "RATE_LIMITED"

    calls.clear()
    result = acquire_simfin_api_statements(db_path=db, tickers=["AAPL"], run_id="RUN2", client=Client())
    assert result["rows"][0]["action"] == "CACHE_HIT"
    assert calls == []


def test_acquire_retries_first_429_once_and_recovers_on_success(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    calls = []
    sleeps = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            group = ",".join(tickers)
            calls.append(group)
            if tickers == ["MSFT", "NVDA"] and calls.count(group) == 1:
                return _group_fetch_result(tickers, "RATE_LIMITED", 429)
            return _group_fetch_result(tickers, "SUCCESS", 200)

    result = acquire_simfin_api_statements(
        db_path=db,
        tickers=["MSFT", "NVDA"],
        run_id="RUN1",
        client=Client(),
        rate_limit_retry_delay_seconds=0.5,
        rate_limit_retry_sleeper=lambda seconds: sleeps.append(seconds),
    )

    assert result["status"] == "OK"
    assert calls == ["MSFT,NVDA", "MSFT,NVDA"]
    assert sleeps == [0.5]
    assert result["rows"][0]["status"] == "SUCCESS"
    assert result["rows"][0]["recovered_after_429"] == 1
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 2,
        "http_requests_made": 2,
        "first_429_count": 1,
        "recovered_429_count": 1,
        "second_429_stop_count": 0,
    }
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT last_status FROM rc_v2_simfin_api_fetch_state WHERE ticker='MSFT'").fetchone()[0] == "SUCCESS"
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_raw WHERE ticker='MSFT'").fetchone()[0] == 1


def test_acquire_retries_first_429_once_and_continues_on_no_data(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    calls = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            group = ",".join(tickers)
            calls.append(group)
            if tickers == ["EMPTY", "NVDA"] and calls.count(group) == 1:
                return _group_fetch_result(tickers, "RATE_LIMITED", 429)
            return _group_fetch_result(["NVDA"], "SUCCESS", 200)

    result = acquire_simfin_api_statements(
        db_path=db,
        tickers=["EMPTY", "NVDA"],
        run_id="RUN1",
        client=Client(),
        rate_limit_retry_sleeper=lambda seconds: None,
    )

    assert result["status"] == "OK"
    assert calls == ["EMPTY,NVDA", "EMPTY,NVDA"]
    assert result["rows"][0]["status"] == "NO_DATA"
    assert result["rows"][0]["recovered_after_429"] == 1
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 2,
        "http_requests_made": 2,
        "first_429_count": 1,
        "recovered_429_count": 1,
        "second_429_stop_count": 0,
    }
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT last_status FROM rc_v2_simfin_api_fetch_state WHERE ticker='EMPTY'").fetchone()[0] == "NO_DATA"
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_raw WHERE ticker='EMPTY'").fetchone()[0] == 0


def test_acquire_no_data_state_is_terminal_cache_hit(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        persist_fetch_result(conn, market="usa", run_id="RUN1", result=_fetch_result("EMPTY", "NO_DATA", 200))
        conn.commit()

    class Client:
        def fetch_ticker(self, ticker: str) -> dict[str, object]:
            raise AssertionError("NO_DATA state should not be refetched without force_refresh")

    result = acquire_simfin_api_statements(db_path=db, tickers=["EMPTY"], run_id="RUN2", client=Client())
    assert result["status"] == "OK"
    assert result["rows"] == [{"ticker": "EMPTY", "action": "NO_DATA_CACHE_HIT", "status": "NO_DATA", "raw_id": ""}]


def test_statement_pair_batching_two_tickers_one_http_request(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    calls = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tickers)
            return _group_fetch_result(tickers, "SUCCESS", 200)

    result = acquire_simfin_api_statements(db_path=db, tickers=["AAPL", "MSFT"], run_id="RUN1", client=Client())

    assert result["status"] == "OK"
    assert calls == [["AAPL", "MSFT"]]
    assert result["request_accounting"]["logical_tickers_attempted"] == 2
    assert result["request_accounting"]["http_requests_made"] == 1


def test_statement_pair_batching_odd_count_uses_three_http_requests(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    calls = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tickers)
            return _group_fetch_result(tickers, "SUCCESS", 200)

    result = acquire_simfin_api_statements(db_path=db, tickers=["A", "B", "C", "D", "E"], run_id="RUN1", client=Client())

    assert result["status"] == "OK"
    assert calls == [["A", "B"], ["C", "D"], ["E"]]
    assert result["request_accounting"]["logical_tickers_attempted"] == 5
    assert result["request_accounting"]["http_requests_made"] == 3


def test_statement_batch_size_above_two_rejected(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)

    with pytest.raises(ValueError, match="SIMFIN_API_STATEMENT_REQUEST_BATCH_SIZE_MAX_2"):
        acquire_simfin_api_statements(db_path=db, tickers=["A", "B", "C"], run_id="RUN1", request_batch_size=3)


def test_statement_pair_demux_uses_identity_not_response_order(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            return _group_fetch_result(["MSFT", "AAPL"], "SUCCESS", 200)

    result = acquire_simfin_api_statements(db_path=db, tickers=["AAPL", "MSFT"], run_id="RUN1", client=Client())

    assert result["status"] == "OK"
    assert [row["ticker"] for row in result["rows"]] == ["AAPL", "MSFT"]
    with sqlite3.connect(str(db)) as conn:
        payloads = {
            row[0]: json.loads(row[1])[0]["ticker"]
            for row in conn.execute("SELECT ticker, payload_json FROM rc_v2_simfin_api_raw ORDER BY ticker")
        }
    assert payloads == {"AAPL": "AAPL", "MSFT": "MSFT"}


def test_statement_pair_unknown_identity_is_hard_mapping_failure(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            return _group_fetch_result(["AAPL", "ZZZ"], "SUCCESS", 200)

    result = acquire_simfin_api_statements(db_path=db, tickers=["AAPL", "MSFT"], run_id="RUN1", client=Client())

    assert result["status"] == "SIMFIN_STATEMENT_PAIR_RESPONSE_MAPPING_FAILURE"
    assert {row["status"] for row in result["rows"]} == {"MALFORMED_RESPONSE"}


def test_statement_pair_missing_requested_company_is_no_data(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            return _group_fetch_result(["AAPL"], "SUCCESS", 200)

    result = acquire_simfin_api_statements(db_path=db, tickers=["AAPL", "EMPTY"], run_id="RUN1", client=Client())

    assert result["status"] == "OK"
    assert {row["ticker"]: row["status"] for row in result["rows"]} == {"AAPL": "SUCCESS", "EMPTY": "NO_DATA"}


def test_statement_pair_one_cached_one_network_required_requests_only_actionable(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        persist_fetch_result(conn, market="usa", run_id="RAW1", result=_fetch_result("AAPL", "SUCCESS", 200))
        conn.commit()
    calls = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tickers)
            return _group_fetch_result(tickers, "SUCCESS", 200)

    result = acquire_simfin_api_statements(db_path=db, tickers=["AAPL", "MSFT"], run_id="RUN1", client=Client())

    assert calls == [["MSFT"]]
    assert [row["action"] for row in result["rows"]] == ["CACHE_HIT", "FETCHED"]


def test_statement_pair_429_uses_300_second_retry_and_stops_later_groups(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    calls = []
    sleeps = []

    class Client:
        def fetch_tickers(self, tickers: list[str]) -> dict[str, object]:
            calls.append(tickers)
            if tickers == ["AAPL", "MSFT"]:
                return _group_fetch_result(tickers, "SUCCESS", 200)
            return _group_fetch_result(tickers, "RATE_LIMITED", 429)

    result = acquire_simfin_api_statements(
        db_path=db,
        tickers=["AAPL", "MSFT", "NVDA", "TSLA", "ZZZ"],
        run_id="RUN1",
        client=Client(),
        rate_limit_retry_sleeper=lambda seconds: sleeps.append(seconds),
    )

    assert result["status"] == "SIMFIN_RATE_LIMITED_AFTER_RETRY"
    assert calls == [["AAPL", "MSFT"], ["NVDA", "TSLA"], ["NVDA", "TSLA"]]
    assert sleeps == [300.0]
    assert result["request_accounting"] == {
        "logical_tickers_attempted": 4,
        "http_requests_made": 3,
        "first_429_count": 1,
        "recovered_429_count": 0,
        "second_429_stop_count": 1,
    }
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_simfin_api_fetch_state WHERE ticker='ZZZ'").fetchone()[0] == 0


def test_mapping_and_apply_preserve_fiscal_identity_conflicts_and_provenance(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        persist_fetch_result(conn, market="usa", run_id="RAW1", result=_fetch_result("AAPL", "SUCCESS", 200))
        conn.commit()

    result = apply_simfin_api_statements(db_path=db, tickers=["AAPL"], run_id="APPLY1")
    assert result["rows"][0]["status"] == "APPLIED"

    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT c.ticker, c.simfin_id, q.fiscal_year, q.fiscal_period, q.report_date,
                   f.revenue, f.operating_income, f.depreciation_amortization, f.ebitda,
                   f.ebit, f.operating_cashflow, f.capex, f.free_cashflow, f.total_debt,
                   f.shares_outstanding
            FROM rc_v2_fundamental_quarterly f
            JOIN rc_v2_quarter q ON q.quarter_id=f.quarter_id
            JOIN rc_v2_company c ON c.company_id=q.company_id
            WHERE c.ticker='AAPL'
            """
        ).fetchone()
        assert row["simfin_id"] == 111052
        assert row["fiscal_year"] == 2026
        assert row["fiscal_period"] == "Q3"
        assert row["report_date"] == "2026-06-30"
        assert row["ebitda"] == 95.0
        assert row["ebit"] is None
        assert row["free_cashflow"] == 17.0
        assert row["total_debt"] == 15.0
        assert row["shares_outstanding"] is None
        providers = {
            field: provider
            for field, provider in conn.execute("SELECT field_name, provider FROM rc_v2_fundamental_field_source")
        }
        assert providers["revenue"] == "SIMFIN_API_STATEMENTS"
        assert providers["ebitda"] == "SIMFIN_API_DERIVED"

    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        quarter_id = conn.execute("SELECT quarter_id FROM rc_v2_quarter").fetchone()[0]
        conn.execute("UPDATE rc_v2_fundamental_quarterly SET revenue=999 WHERE quarter_id=?", (quarter_id,))
        conn.commit()
    result = apply_simfin_api_statements(db_path=db, tickers=["AAPL"], run_id="APPLY2")
    assert result["rows"][0]["conflicts"] >= 1
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT revenue FROM rc_v2_fundamental_quarterly").fetchone()[0] == 999


def test_replay_apply_does_not_duplicate_quarters(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    _write_v2_db(db)
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        ensure_schema(conn)
        persist_fetch_result(conn, market="usa", run_id="RAW1", result=_fetch_result("AAPL", "SUCCESS", 200))
        conn.commit()
    apply_simfin_api_statements(db_path=db, tickers=["AAPL"], run_id="APPLY1")
    apply_simfin_api_statements(db_path=db, tickers=["AAPL"], run_id="APPLY2")
    with sqlite3.connect(str(db)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_quarter").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_quarterly").fetchone()[0] == 1


def test_api_key_never_accepted_as_required_function_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIMFIN_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="SIMFIN_API_KEY_MISSING"):
        from swingmaster.fundamentals_v2.simfin_api_statements import SimFinStatementClient

        SimFinStatementClient(api_key=None)


def test_map_api_ordinary_fields_uses_shared_semantics() -> None:
    values = map_api_ordinary_fields(
        {
            "PL": {"Revenue": "10", "Gross Profit": "5", "Operating Income (Loss)": "3", "Net Income": "2"},
            "BS": {"Cash, Cash Equivalents & Short Term Investments": "7", "Short Term Debt": "1", "Long Term Debt": "4"},
            "CF": {"Cash from Operating Activities": "8", "Change in Fixed Assets & Intangibles": "-6", "Depreciation & Amortization": "2"},
        }
    )
    assert values["ebitda"] == 5
    assert values["ebit"] is None
    assert values["free_cashflow"] == 2
    assert values["total_debt"] == 5
    assert values["shares_outstanding"] is None


def test_http_200_company_wrapper_without_statement_rows_is_no_data() -> None:
    payload = [{"id": 123, "ticker": "EMPTY", "statements": []}]
    assert classify_http_status(200, json.dumps(payload)) == "NO_DATA"


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def _write_v2_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        create_schema(conn)
        conn.execute(
            "INSERT INTO rc_v2_import_run VALUES ('SEED','usa','fixture','test','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
        )
        conn.commit()


def _write_legacy_db(path: Path, tickers: list[str]) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute("CREATE TABLE rc_fundamental_quarterly (ticker TEXT)")
        conn.executemany("INSERT INTO rc_fundamental_quarterly VALUES (?)", [(ticker,) for ticker in tickers])


def _fixture_simfin_dir(path: Path) -> Path:
    path.mkdir()
    _write_csv(path / "us-companies.csv", [
        {"Ticker": "LOCAL", "SimFinId": "1", "Company Name": "Local Co", "IndustryId": "10"},
        {"Ticker": "BANK", "SimFinId": "2", "Company Name": "Bank Co", "IndustryId": "20"},
        {"Ticker": "INS", "SimFinId": "3", "Company Name": "Insurance Co", "IndustryId": "30"},
        {"Ticker": "AAPL", "SimFinId": "111052", "Company Name": "Apple Inc", "IndustryId": "40"},
    ])
    _write_statement_csv(path / "us-income-quarterly.csv", "LOCAL", "1")
    _write_statement_csv(path / "us-balance-quarterly.csv", "LOCAL", "1")
    _write_statement_csv(path / "us-cashflow-quarterly.csv", "LOCAL", "1")
    _write_statement_csv(path / "us-income-banks-quarterly.csv", "BANK", "2")
    _write_statement_csv(path / "us-balance-banks-quarterly.csv", "BANK", "2")
    _write_statement_csv(path / "us-cashflow-banks-quarterly.csv", "BANK", "2")
    _write_statement_csv(path / "us-income-insurance-quarterly.csv", "INS", "3")
    _write_statement_csv(path / "us-balance-insurance-quarterly.csv", "INS", "3")
    _write_statement_csv(path / "us-cashflow-insurance-quarterly.csv", "INS", "3")
    return path


def _write_statement_csv(path: Path, ticker: str, simfin_id: str) -> None:
    _write_csv(path, [{"Ticker": ticker, "SimFinId": simfin_id, "Fiscal Year": "2026", "Fiscal Period": "Q1", "Report Date": "2026-03-31"}])


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def _fetch_result(ticker: str, status: str, http_status: int) -> dict[str, object]:
    payload = _payload(ticker) if status == "SUCCESS" else {"error": status}
    return {
        "ticker": ticker,
        "retrieved_at_utc": "2026-08-12T00:00:00Z",
        "http_status": http_status,
        "provider_status": status,
        "payload_json": json.dumps(payload, sort_keys=True),
        "safe_headers_json": "{}",
    }


def _group_fetch_result(tickers: list[str], status: str, http_status: int) -> dict[str, object]:
    payload = [_payload_company(ticker) for ticker in tickers] if status == "SUCCESS" else {"error": status}
    return {
        "ticker": ",".join(tickers),
        "requested_tickers": tickers,
        "retrieved_at_utc": "2026-08-12T00:00:00Z",
        "http_status": http_status,
        "provider_status": status,
        "payload_json": json.dumps(payload, sort_keys=True),
        "safe_headers_json": "{}",
    }


def _payload(ticker: str) -> list[dict[str, object]]:
    return [_payload_company(ticker)]


def _payload_company(ticker: str) -> dict[str, object]:
    simfin_id = {
        "A": 100001,
        "AAPL": 111052,
        "B": 100002,
        "C": 100003,
        "D": 100004,
        "E": 100005,
        "EMPTY": 100006,
        "MSFT": 59265,
        "NVDA": 59266,
        "TSLA": 59267,
        "ZZZ": 999999,
    }.get(ticker, 111052)
    return [
        {
            "id": simfin_id,
            "ticker": ticker,
            "name": f"{ticker} INC",
            "currency": "USD",
            "statements": [
                {
                    "statement": "PL",
                    "columns": ["Fiscal Period", "Fiscal Year", "Report Date", "Publish Date", "Revenue", "Gross Profit", "Operating Income (Loss)", "Net Income"],
                    "data": [["Q3", 2026, "2026-06-30", "2026-07-30", 100, 40, 90, 70]],
                },
                {
                    "statement": "BS",
                    "columns": ["Fiscal Period", "Fiscal Year", "Report Date", "Publish Date", "Cash, Cash Equivalents & Short Term Investments", "Short Term Debt", "Long Term Debt"],
                    "data": [["Q3", 2026, "2026-06-30", "2026-07-30", 20, 5, 10]],
                },
                {
                    "statement": "CF",
                    "columns": ["Fiscal Period", "Fiscal Year", "Report Date", "Publish Date", "Cash from Operating Activities", "Change in Fixed Assets & Intangibles", "Depreciation & Amortization"],
                    "data": [["Q3", 2026, "2026-06-30", "2026-07-30", 30, -13, 5]],
                },
            ],
        }
    ][0]
