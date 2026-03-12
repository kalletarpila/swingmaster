from __future__ import annotations

import sqlite3
import sys
import types
from pathlib import Path
from urllib.error import URLError

from swingmaster.cli import run_macro_ingest
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.macro.raw_ingest import (
    MacroIngestSummary,
    SOURCE_DEFINITIONS,
    ingest_macro_raw,
    parse_cboe_equity_put_call_csv,
    parse_fred_observations,
)


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    return conn


def _fred_payload(series_id: str, value: str = "1.23") -> dict[str, object]:
    return {
        "realtime_start": "2026-01-01",
        "realtime_end": "2026-01-01",
        "observations": [
            {"date": "2026-01-01", "value": value, "series_id": series_id},
            {"date": "2026-01-02", "value": ".", "series_id": series_id},
        ],
    }


def _fred_fetcher(series_id: str, date_from: str, date_to: str, api_key: str):  # type: ignore[no-untyped-def]
    del date_from
    del date_to
    del api_key
    return _fred_payload(series_id), f"https://fred.test/{series_id}"


def _cboe_fetcher() -> tuple[str, str]:
    csv_text = "\n".join(
        [
            "Generated: 2026-01-03",
            "Date,Total Put/Call Ratio,Equity Put/Call Ratio,Index Put/Call Ratio",
            "2026-01-01,0.95,0.61,1.34",
            "2026-01-02,1.01,0.73,1.40",
            "Footer line",
        ]
    )
    return csv_text, "https://cboe.test/equity.csv"


def test_macro_raw_migration_creates_table_and_uniqueness() -> None:
    conn = _new_conn()
    cols = {
        str(row[1]): str(row[2])
        for row in conn.execute("PRAGMA table_info(rc_macro_source_raw)")
    }
    assert cols["source_key"] == "TEXT"
    assert cols["vendor"] == "TEXT"
    assert cols["external_series_id"] == "TEXT"
    assert cols["observation_date"] == "TEXT"
    assert cols["raw_value"] == "REAL"
    assert cols["loaded_at_utc"] == "TEXT"
    assert cols["run_id"] == "TEXT"

    conn.execute(
        """
        INSERT INTO rc_macro_source_raw (
          source_key, vendor, external_series_id, observation_date,
          raw_value, raw_value_text, source_url, loaded_at_utc, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "BTC_USD_CBBTCUSD",
            "FRED",
            "CBBTCUSD",
            "2026-01-01",
            1.0,
            "1.0",
            "https://fred.test/CBBTCUSD",
            "2026-01-03T00:00:00+00:00",
            "run1",
        ),
    )
    conn.commit()

    try:
        conn.execute(
            """
            INSERT INTO rc_macro_source_raw (
              source_key, vendor, external_series_id, observation_date,
              raw_value, raw_value_text, source_url, loaded_at_utc, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "BTC_USD_CBBTCUSD",
                "FRED",
                "CBBTCUSD",
                "2026-01-01",
                2.0,
                "2.0",
                "https://fred.test/CBBTCUSD",
                "2026-01-04T00:00:00+00:00",
                "run2",
            ),
        )
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError("expected unique constraint on (source_key, observation_date)")

    score_table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name='rc_risk_appetite_daily'
        LIMIT 1
        """
    ).fetchone()
    assert score_table_exists is not None
    score_count = conn.execute("SELECT COUNT(*) FROM rc_risk_appetite_daily").fetchone()
    assert score_count is not None and int(score_count[0]) == 0


def test_parse_fred_observations_is_deterministic() -> None:
    rows = parse_fred_observations(
        _fred_payload("CBBTCUSD", value="12345.67"),
        source_key="BTC_USD_CBBTCUSD",
        external_series_id="CBBTCUSD",
        source_url="https://fred.test/CBBTCUSD",
    )
    assert len(rows) == 2
    assert rows[0].observation_date == "2026-01-01"
    assert rows[0].raw_value == 12345.67
    assert rows[0].raw_value_text == "12345.67"
    assert rows[1].observation_date == "2026-01-02"
    assert rows[1].raw_value is None
    assert rows[1].raw_value_text == "."


def test_fred_fetch_uses_fredapi_client_and_returns_observations(monkeypatch) -> None:
    from swingmaster.macro import raw_ingest

    calls: dict[str, object] = {}

    class _FakeSeries:
        def items(self):  # type: ignore[no-untyped-def]
            return [
                ("2026-01-01", 10.5),
                ("2026-01-02", float("nan")),
            ]

    class _FakeFred:
        def __init__(self, *, api_key: str) -> None:
            calls["api_key"] = api_key

        def get_series(self, series_id: str, *, observation_start: str, observation_end: str):  # type: ignore[no-untyped-def]
            calls["series_id"] = series_id
            calls["observation_start"] = observation_start
            calls["observation_end"] = observation_end
            return _FakeSeries()

    fake_module = types.ModuleType("fredapi")
    setattr(fake_module, "Fred", _FakeFred)
    monkeypatch.setitem(sys.modules, "fredapi", fake_module)

    payload, source_url = raw_ingest.fetch_fred_series_observations(
        "WALCL",
        date_from="2026-01-01",
        date_to="2026-01-31",
        fred_api_key="abc123",
    )
    assert calls["api_key"] == "abc123"
    assert calls["series_id"] == "WALCL"
    assert calls["observation_start"] == "2026-01-01"
    assert calls["observation_end"] == "2026-01-31"
    assert source_url == "https://api.stlouisfed.org/fred/series/observations?series_id=WALCL"
    assert payload == {
        "observations": [
            {"date": "2026-01-01", "value": "10.5"},
            {"date": "2026-01-02", "value": "."},
        ]
    }


def test_fred_fetch_raises_when_fredapi_missing(monkeypatch) -> None:
    from swingmaster.macro import raw_ingest

    monkeypatch.delitem(sys.modules, "fredapi", raising=False)
    import builtins
    real_import = builtins.__import__

    def _fake_import(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if name == "fredapi":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    try:
        raw_ingest.fetch_fred_series_observations(
            "WALCL",
            date_from="2026-01-01",
            date_to="2026-01-31",
            fred_api_key="abc123",
        )
    except RuntimeError as exc:
        assert str(exc) == "FREDAPI_REQUIRED_FOR_FRED_INGEST"
    else:
        raise AssertionError("expected missing fredapi error")


def test_cboe_fetch_retries_transient_then_succeeds(monkeypatch) -> None:
    from swingmaster.macro import raw_ingest

    calls = {"n": 0}
    sleep_calls: list[float] = []

    class _Resp:
        def read(self) -> bytes:
            return b"Date,Equity Put/Call Ratio\n2026-01-01,0.70\n"

        def __enter__(self) -> "_Resp":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
            del exc_type, exc, tb

    def _fake_urlopen(url: str, timeout: int):  # type: ignore[no-untyped-def]
        del url, timeout
        calls["n"] += 1
        if calls["n"] == 1:
            raise URLError("temporary")
        return _Resp()

    monkeypatch.setattr(raw_ingest, "urlopen", _fake_urlopen)
    monkeypatch.setattr(raw_ingest, "_sleep", lambda s: sleep_calls.append(float(s)))
    monkeypatch.setattr(raw_ingest, "HTTP_MIN_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(raw_ingest, "_LAST_HTTP_REQUEST_TS", None)

    csv_text, _ = raw_ingest.fetch_cboe_equity_put_call_csv()
    assert "2026-01-01,0.70" in csv_text
    assert calls["n"] == 2
    assert sleep_calls == [1.0]


def test_fred_fetch_wraps_client_failure_with_context(monkeypatch) -> None:
    from swingmaster.macro import raw_ingest

    class _FakeFred:
        def __init__(self, *, api_key: str) -> None:
            del api_key

        def get_series(self, series_id: str, *, observation_start: str, observation_end: str):  # type: ignore[no-untyped-def]
            del series_id, observation_start, observation_end
            raise ValueError("boom")

    fake_module = types.ModuleType("fredapi")
    setattr(fake_module, "Fred", _FakeFred)
    monkeypatch.setitem(sys.modules, "fredapi", fake_module)

    try:
        raw_ingest.fetch_fred_series_observations(
            "WALCL",
            date_from="2026-01-01",
            date_to="2026-01-31",
            fred_api_key="abc123",
        )
    except RuntimeError as exc:
        assert str(exc) == "FRED_FETCH_FAILED:WALCL"
    else:
        raise AssertionError("expected wrapped fred failure")


def test_timeout_constant_is_passed_to_http_calls(monkeypatch) -> None:
    from swingmaster.macro import raw_ingest

    timeouts: list[int] = []

    class _Resp:
        def read(self) -> bytes:
            return b'{"observations":[]}'

        def __enter__(self) -> "_Resp":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
            del exc_type, exc, tb

    def _fake_urlopen(url: str, timeout: int):  # type: ignore[no-untyped-def]
        del url
        timeouts.append(int(timeout))
        return _Resp()

    monkeypatch.setattr(raw_ingest, "urlopen", _fake_urlopen)
    monkeypatch.setattr(raw_ingest, "_sleep", lambda s: None)
    monkeypatch.setattr(raw_ingest, "HTTP_MIN_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(raw_ingest, "_LAST_HTTP_REQUEST_TS", None)

    raw_ingest.fetch_cboe_equity_put_call_csv()
    assert timeouts == [raw_ingest.HTTP_TIMEOUT_SECONDS]


def test_rate_limit_sleep_is_deterministic_between_consecutive_requests(monkeypatch) -> None:
    from swingmaster.macro import raw_ingest

    monotonic_values = iter([100.0, 100.2, 100.2])
    sleep_calls: list[float] = []

    class _Resp:
        def read(self) -> bytes:
            return b'{"observations":[]}'

        def __enter__(self) -> "_Resp":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
            del exc_type, exc, tb

    def _fake_urlopen(url: str, timeout: int):  # type: ignore[no-untyped-def]
        del url, timeout
        return _Resp()

    monkeypatch.setattr(raw_ingest, "urlopen", _fake_urlopen)
    monkeypatch.setattr(raw_ingest, "_sleep", lambda s: sleep_calls.append(float(s)))
    monkeypatch.setattr(raw_ingest, "_monotonic", lambda: float(next(monotonic_values)))
    monkeypatch.setattr(raw_ingest, "HTTP_MIN_INTERVAL_SECONDS", 0.5)
    monkeypatch.setattr(raw_ingest, "_LAST_HTTP_REQUEST_TS", None)

    raw_ingest.fetch_cboe_equity_put_call_csv()
    raw_ingest.fetch_cboe_equity_put_call_csv()
    assert len(sleep_calls) == 1
    assert abs(sleep_calls[0] - 0.3) < 1e-9


def test_parse_cboe_csv_is_deterministic() -> None:
    csv_text, url = _cboe_fetcher()
    rows = parse_cboe_equity_put_call_csv(
        csv_text,
        source_key="PCR_EQUITY_CBOE",
        source_url=url,
    )
    assert [row.observation_date for row in rows] == ["2026-01-01", "2026-01-02"]
    assert [row.raw_value for row in rows] == [0.61, 0.73]
    assert all(row.vendor == "CBOE" for row in rows)


def test_ingest_insert_missing_is_idempotent() -> None:
    conn = _new_conn()
    first = ingest_macro_raw(
        conn,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="insert-missing",
        computed_at="2026-01-03T00:00:00+00:00",
        fred_fetcher=_fred_fetcher,
        cboe_fetcher=_cboe_fetcher,
    )
    assert first.rows_inserted == 10
    assert first.rows_skipped == 0

    second = ingest_macro_raw(
        conn,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="insert-missing",
        computed_at="2026-01-03T00:00:00+00:00",
        fred_fetcher=_fred_fetcher,
        cboe_fetcher=_cboe_fetcher,
    )
    assert second.rows_inserted == 0
    assert second.rows_updated == 0
    assert second.rows_skipped == 10

    total = conn.execute("SELECT COUNT(*) FROM rc_macro_source_raw").fetchone()
    assert total is not None and int(total[0]) == 10


def test_ingest_uses_explicit_computed_at_for_loaded_at_utc() -> None:
    conn = _new_conn()
    ingest_macro_raw(
        conn,
        date_from="2026-01-01",
        date_to="2026-01-01",
        mode="upsert",
        computed_at="2026-01-03T12:34:56+00:00",
        fred_fetcher=_fred_fetcher,
        cboe_fetcher=_cboe_fetcher,
    )
    row = conn.execute(
        """
        SELECT loaded_at_utc
        FROM rc_macro_source_raw
        WHERE source_key='BTC_USD_CBBTCUSD' AND observation_date='2026-01-01'
        """
    ).fetchone()
    assert row is not None
    assert str(row[0]) == "2026-01-03T12:34:56+00:00"


def test_ingest_default_loaded_at_utc_is_deterministic_without_computed_at() -> None:
    conn = _new_conn()
    first = ingest_macro_raw(
        conn,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        fred_fetcher=_fred_fetcher,
        cboe_fetcher=_cboe_fetcher,
    )
    assert first.rows_inserted == 10
    loaded_first = conn.execute(
        """
        SELECT DISTINCT loaded_at_utc
        FROM rc_macro_source_raw
        ORDER BY loaded_at_utc
        """
    ).fetchall()
    assert loaded_first == [("2026-01-02T00:00:00+00:00",)]

    second = ingest_macro_raw(
        conn,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        fred_fetcher=_fred_fetcher,
        cboe_fetcher=_cboe_fetcher,
    )
    assert second.rows_updated == 10
    loaded_second = conn.execute(
        """
        SELECT DISTINCT loaded_at_utc
        FROM rc_macro_source_raw
        ORDER BY loaded_at_utc
        """
    ).fetchall()
    assert loaded_second == [("2026-01-02T00:00:00+00:00",)]


def test_ingest_upsert_updates_existing_rows() -> None:
    conn = _new_conn()
    ingest_macro_raw(
        conn,
        date_from="2026-01-01",
        date_to="2026-01-01",
        mode="upsert",
        computed_at="2026-01-03T00:00:00+00:00",
        fred_fetcher=_fred_fetcher,
        cboe_fetcher=_cboe_fetcher,
    )

    def _fred_fetcher_changed(series_id: str, date_from: str, date_to: str, api_key: str):  # type: ignore[no-untyped-def]
        del date_from
        del date_to
        del api_key
        return _fred_payload(series_id, value="9.99"), f"https://fred.test/{series_id}"

    out = ingest_macro_raw(
        conn,
        date_from="2026-01-01",
        date_to="2026-01-01",
        mode="upsert",
        computed_at="2026-01-04T00:00:00+00:00",
        fred_fetcher=_fred_fetcher_changed,
        cboe_fetcher=_cboe_fetcher,
    )
    assert out.rows_inserted == 0
    assert out.rows_updated == len(SOURCE_DEFINITIONS)

    updated = conn.execute(
        """
        SELECT raw_value
        FROM rc_macro_source_raw
        WHERE source_key='BTC_USD_CBBTCUSD' AND observation_date='2026-01-01'
        """
    ).fetchone()
    assert updated is not None and float(updated[0]) == 9.99


def test_ingest_replace_all_is_bounded_to_requested_scope() -> None:
    conn = _new_conn()
    ingest_macro_raw(
        conn,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        computed_at="2026-01-03T00:00:00+00:00",
        fred_fetcher=_fred_fetcher,
        cboe_fetcher=_cboe_fetcher,
    )
    conn.execute(
        """
        INSERT INTO rc_macro_source_raw (
          source_key, vendor, external_series_id, observation_date,
          raw_value, raw_value_text, source_url, loaded_at_utc, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "BTC_USD_CBBTCUSD",
            "FRED",
            "CBBTCUSD",
            "2026-02-01",
            11.0,
            "11.0",
            "https://fred.test/CBBTCUSD",
            "2026-01-05T00:00:00+00:00",
            "seed",
        ),
    )
    conn.execute(
        """
        INSERT INTO rc_macro_source_raw (
          source_key, vendor, external_series_id, observation_date,
          raw_value, raw_value_text, source_url, loaded_at_utc, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "UNRELATED_SOURCE",
            "OTHER",
            "OTHER1",
            "2026-01-01",
            7.0,
            "7.0",
            "https://other.test/value",
            "2026-01-05T00:00:00+00:00",
            "seed",
        ),
    )
    conn.commit()

    out = ingest_macro_raw(
        conn,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="replace-all",
        computed_at="2026-01-06T00:00:00+00:00",
        fred_fetcher=_fred_fetcher,
        cboe_fetcher=_cboe_fetcher,
    )
    assert out.rows_deleted == 10
    assert out.rows_inserted == 10
    remaining_out_of_range = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_macro_source_raw
        WHERE source_key='BTC_USD_CBBTCUSD' AND observation_date='2026-02-01'
        """
    ).fetchone()
    assert remaining_out_of_range is not None and int(remaining_out_of_range[0]) == 1
    remaining_unrelated = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_macro_source_raw
        WHERE source_key='UNRELATED_SOURCE'
        """
    ).fetchone()
    assert remaining_unrelated is not None and int(remaining_unrelated[0]) == 1


def test_cli_emits_summary_lines(monkeypatch, tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "rc.db"
    expected = MacroIngestSummary(
        sources_requested=5,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        rows_inserted=10,
        rows_updated=0,
        rows_deleted=0,
        rows_skipped=0,
        distinct_sources_loaded=5,
        run_id="MACRO_RAW_INGEST_V1_20260101_20260102_deadbeef00",
        summary_status="OK",
    )

    monkeypatch.setattr(
        run_macro_ingest,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db_path": str(db_path),
                "start_date": "2026-01-01",
                "end_date": "2026-01-02",
                "mode": "upsert",
                "computed_at": "2026-01-03T00:00:00+00:00",
                "fred_api_key": None,
            },
        )(),
    )
    seen: dict[str, object] = {}

    def _fake_ingest(*args, **kwargs):  # type: ignore[no-untyped-def]
        del args
        seen["fred_api_key"] = kwargs.get("fred_api_key")
        return expected

    monkeypatch.setattr(run_macro_ingest, "ingest_macro_raw", _fake_ingest)
    monkeypatch.setenv("FRED_API_KEY", "env-key")

    run_macro_ingest.main()
    output = capsys.readouterr().out
    assert seen["fred_api_key"] == "env-key"
    assert "SUMMARY status=OK" in output
    assert "SUMMARY sources_requested=5" in output
    assert "SUMMARY rows_inserted=10" in output
    assert "SUMMARY distinct_sources_loaded=5" in output
    assert "SUMMARY summary_status=OK" in output


def test_cli_prefers_explicit_fred_api_key_over_env(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "rc.db"
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        run_macro_ingest,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db_path": str(db_path),
                "start_date": "2026-01-01",
                "end_date": "2026-01-02",
                "mode": "upsert",
                "computed_at": "2026-01-03T00:00:00+00:00",
                "fred_api_key": "cli-key",
            },
        )(),
    )
    monkeypatch.setenv("FRED_API_KEY", "env-key")

    expected = MacroIngestSummary(
        sources_requested=5,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        rows_inserted=10,
        rows_updated=0,
        rows_deleted=0,
        rows_skipped=0,
        distinct_sources_loaded=5,
        run_id="MACRO_RAW_INGEST_V1_20260101_20260102_deadbeef00",
        summary_status="OK",
    )

    def _fake_ingest(*args, **kwargs):  # type: ignore[no-untyped-def]
        del args
        seen["fred_api_key"] = kwargs.get("fred_api_key")
        return expected

    monkeypatch.setattr(run_macro_ingest, "ingest_macro_raw", _fake_ingest)
    run_macro_ingest.main()
    assert seen["fred_api_key"] == "cli-key"


def test_cli_raises_when_fred_key_missing(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "rc.db"
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    monkeypatch.setattr(
        run_macro_ingest,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "db_path": str(db_path),
                "start_date": "2026-01-01",
                "end_date": "2026-01-02",
                "mode": "upsert",
                "computed_at": "2026-01-03T00:00:00+00:00",
                "fred_api_key": None,
            },
        )(),
    )
    try:
        run_macro_ingest.main()
    except RuntimeError as exc:
        assert str(exc) == "FRED_API_KEY_MISSING"
    else:
        raise AssertionError("expected FRED_API_KEY_MISSING")
