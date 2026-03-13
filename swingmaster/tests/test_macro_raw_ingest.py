from __future__ import annotations

import sqlite3
import sys
import types
from pathlib import Path

from swingmaster.cli import run_macro_ingest
from swingmaster.infra.sqlite.migrator import apply_macro_migrations
from swingmaster.macro.raw_ingest import (
    MacroIngestSummary,
    RawObservation,
    SOURCE_DEFINITIONS,
    ingest_macro_raw,
    parse_cboe_local_put_call_csv,
    parse_fred_observations,
)


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    apply_macro_migrations(conn)
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


def _cboe_fetcher() -> list[RawObservation]:
    csv_text = "\n".join(
        [
            "date,total_put_call_ratio,index_put_call_ratio,equity_put_call_ratio,status,fetched_at_utc",
            "2026-01-01,0.95,1.34,0.61,ok,2026-03-13T00:00:00Z",
            "2026-01-02,1.01,1.40,0.73,ok,2026-03-13T00:00:00Z",
        ]
    )
    return parse_cboe_local_put_call_csv(csv_text, source_url="/tmp/cboe.csv")


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
    rc_state_table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name='rc_state_daily'
        LIMIT 1
        """
    ).fetchone()
    assert rc_state_table_exists is None


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
    assert source_url == "https://fred.stlouisfed.org/series/WALCL"
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
        assert str(exc).startswith("FRED_FETCH_FAILED:WALCL:ImportError:")
    else:
        raise AssertionError("expected missing fredapi error")


def test_cboe_fetch_reads_local_csv_files_and_maps_all_series(monkeypatch, tmp_path: Path) -> None:
    from swingmaster.macro import raw_ingest

    first = tmp_path / "cboe_pcr_2026-01-01_2026-01-02_20260313T010000Z.csv"
    first.write_text(
        "\n".join(
            [
                "date,total_put_call_ratio,index_put_call_ratio,equity_put_call_ratio,status,fetched_at_utc",
                "2026-01-01,0.95,1.34,0.61,ok,2026-03-13T01:00:00Z",
                "2026-01-02,0.80,1.20,0.55,http_403,2026-03-13T01:00:00Z",
            ]
        ),
        encoding="utf-8",
    )
    second = tmp_path / "cboe_pcr_2026-01-01_2026-01-02_20260313T020000Z.csv"
    second.write_text(
        "\n".join(
            [
                "date,total_put_call_ratio,index_put_call_ratio,equity_put_call_ratio,status,fetched_at_utc",
                "2026-01-02,1.01,1.40,0.73,ok,2026-03-13T02:00:00Z",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(raw_ingest, "CBOE_LOCAL_PCR_DIR", tmp_path)
    rows = raw_ingest.fetch_cboe_put_call_observations()
    assert {(row.source_key, row.observation_date, row.raw_value_text) for row in rows} == {
        ("PCR_EQUITY_CBOE", "2026-01-01", "0.61"),
        ("PCR_EQUITY_CBOE", "2026-01-02", "0.73"),
        ("PCR_TOTAL_CBOE", "2026-01-01", "0.95"),
        ("PCR_TOTAL_CBOE", "2026-01-02", "1.01"),
        ("PCR_INDEX_CBOE", "2026-01-01", "1.34"),
        ("PCR_INDEX_CBOE", "2026-01-02", "1.40"),
    }
    assert all(str(tmp_path) in row.source_url for row in rows)


def test_cboe_fetch_failure_is_deterministic_when_local_dir_missing(tmp_path: Path) -> None:
    from swingmaster.macro import raw_ingest

    missing = tmp_path / "missing-dir"
    try:
        raw_ingest.fetch_cboe_put_call_observations(cboe_csv_url=str(missing))
    except RuntimeError as exc:
        assert str(exc) == f"CBOE_FETCH_FAILED:PCR_EQUITY_CBOE:PATH_NOT_FOUND:{missing}"
    else:
        raise AssertionError("expected deterministic missing-path failure")


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
        assert str(exc) == "FRED_FETCH_FAILED:WALCL:ValueError:boom"
    else:
        raise AssertionError("expected wrapped fred failure")

def test_parse_cboe_local_csv_is_deterministic() -> None:
    csv_text = "\n".join(
        [
            "date,total_put_call_ratio,index_put_call_ratio,equity_put_call_ratio,status,fetched_at_utc",
            "2026-01-01,0.90,1.10,0.64,ok,2026-03-13T00:00:00Z",
            "2026-01-02,0.95,1.20,0.61,ok,2026-03-13T00:00:00Z",
        ]
    )
    rows = parse_cboe_local_put_call_csv(
        csv_text,
        source_url="/tmp/cboe.csv",
    )
    assert [row.source_key for row in rows] == [
        "PCR_EQUITY_CBOE",
        "PCR_TOTAL_CBOE",
        "PCR_INDEX_CBOE",
        "PCR_EQUITY_CBOE",
        "PCR_TOTAL_CBOE",
        "PCR_INDEX_CBOE",
    ]
    assert [row.observation_date for row in rows] == [
        "2026-01-01",
        "2026-01-01",
        "2026-01-01",
        "2026-01-02",
        "2026-01-02",
        "2026-01-02",
    ]
    assert [row.raw_value_text for row in rows] == ["0.64", "0.90", "1.10", "0.61", "0.95", "1.20"]


def test_parse_cboe_local_csv_preserves_equity_source_key_for_downstream() -> None:
    csv_text = "\n".join(
        [
            "date,total_put_call_ratio,index_put_call_ratio,equity_put_call_ratio,status,fetched_at_utc",
            "2026-01-01,0.95,1.10,0.61,ok,2026-03-13T00:00:00Z",
        ]
    )
    rows = parse_cboe_local_put_call_csv(
        csv_text,
        source_url="/tmp/cboe.csv",
    )
    equity_rows = [row for row in rows if row.source_key == "PCR_EQUITY_CBOE"]
    assert len(equity_rows) == 1
    assert equity_rows[0].external_series_id == "EQUITY_PUT_CALL_RATIO"
    assert equity_rows[0].raw_value == 0.61


def test_parse_cboe_local_csv_raises_when_required_column_missing() -> None:
    csv_text = "\n".join(
        [
            "date,total_put_call_ratio,equity_put_call_ratio,status,fetched_at_utc",
            "2026-01-01,0.95,0.61,ok,2026-03-13T00:00:00Z",
        ]
    )
    try:
        parse_cboe_local_put_call_csv(
            csv_text,
            source_url="/tmp/cboe.csv",
        )
    except RuntimeError as exc:
        assert str(exc) == "CBOE_FETCH_FAILED:PCR_INDEX_CBOE:MISSING_COLUMN:index_put_call_ratio"
    else:
        raise AssertionError("expected deterministic parse failure when a required column is missing")


def test_ingest_persists_all_three_cboe_raw_series() -> None:
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
    rows = conn.execute(
        """
        SELECT source_key, external_series_id, raw_value_text
        FROM rc_macro_source_raw
        WHERE observation_date='2026-01-01' AND vendor='CBOE'
        ORDER BY source_key
        """
    ).fetchall()
    assert rows == [
        ("PCR_EQUITY_CBOE", "EQUITY_PUT_CALL_RATIO", "0.61"),
        ("PCR_INDEX_CBOE", "INDEX_PUT_CALL_RATIO", "1.34"),
        ("PCR_TOTAL_CBOE", "TOTAL_PUT_CALL_RATIO", "0.95"),
    ]


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
    assert first.rows_inserted == 14
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
    assert second.rows_skipped == 14

    total = conn.execute("SELECT COUNT(*) FROM rc_macro_source_raw").fetchone()
    assert total is not None and int(total[0]) == 14


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
    assert first.rows_inserted == 14
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
    assert second.rows_updated == 14
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
    assert out.rows_deleted == 14
    assert out.rows_inserted == 14
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
        sources_requested=7,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        rows_inserted=14,
        rows_updated=0,
        rows_deleted=0,
        rows_skipped=0,
        distinct_sources_loaded=7,
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
                "cboe_csv_url": None,
            },
        )(),
    )
    seen: dict[str, object] = {}

    def _fake_ingest(*args, **kwargs):  # type: ignore[no-untyped-def]
        del args
        seen["fred_api_key"] = kwargs.get("fred_api_key")
        seen["cboe_csv_url"] = kwargs.get("cboe_csv_url")
        return expected

    monkeypatch.setattr(run_macro_ingest, "ingest_macro_raw", _fake_ingest)
    monkeypatch.setenv("FRED_API_KEY", "env-key")

    run_macro_ingest.main()
    output = capsys.readouterr().out
    assert seen["fred_api_key"] == "env-key"
    assert seen["cboe_csv_url"] is None
    assert "SUMMARY status=OK" in output
    assert "SUMMARY sources_requested=7" in output
    assert "SUMMARY rows_inserted=14" in output
    assert "SUMMARY distinct_sources_loaded=7" in output
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
                "cboe_csv_url": None,
            },
        )(),
    )
    monkeypatch.setenv("FRED_API_KEY", "env-key")

    expected = MacroIngestSummary(
        sources_requested=7,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        rows_inserted=14,
        rows_updated=0,
        rows_deleted=0,
        rows_skipped=0,
        distinct_sources_loaded=7,
        run_id="MACRO_RAW_INGEST_V1_20260101_20260102_deadbeef00",
        summary_status="OK",
    )

    def _fake_ingest(*args, **kwargs):  # type: ignore[no-untyped-def]
        del args
        seen["fred_api_key"] = kwargs.get("fred_api_key")
        seen["cboe_csv_url"] = kwargs.get("cboe_csv_url")
        return expected

    monkeypatch.setattr(run_macro_ingest, "ingest_macro_raw", _fake_ingest)
    run_macro_ingest.main()
    assert seen["fred_api_key"] == "cli-key"
    assert seen["cboe_csv_url"] is None


def test_cli_passes_cboe_csv_url_override(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "rc.db"
    cboe_dir = tmp_path / "cboe"
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
                "cboe_csv_url": str(cboe_dir),
            },
        )(),
    )

    expected = MacroIngestSummary(
        sources_requested=7,
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        rows_inserted=14,
        rows_updated=0,
        rows_deleted=0,
        rows_skipped=0,
        distinct_sources_loaded=7,
        run_id="MACRO_RAW_INGEST_V1_20260101_20260102_deadbeef00",
        summary_status="OK",
    )

    def _fake_ingest(*args, **kwargs):  # type: ignore[no-untyped-def]
        del args
        seen["cboe_csv_url"] = kwargs.get("cboe_csv_url")
        return expected

    monkeypatch.setattr(run_macro_ingest, "ingest_macro_raw", _fake_ingest)
    run_macro_ingest.main()
    assert seen["cboe_csv_url"] == str(cboe_dir)


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
                "cboe_csv_url": None,
            },
        )(),
    )
    try:
        run_macro_ingest.main()
    except RuntimeError as exc:
        assert str(exc) == "FRED_API_KEY_MISSING"
    else:
        raise AssertionError("expected FRED_API_KEY_MISSING")
