from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli import run_macro_normalize
from swingmaster.infra.sqlite.migrator import apply_migrations
from swingmaster.macro.normalize import MacroNormalizeSummary, normalize_macro_sources


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    return conn


def _insert_raw(
    conn: sqlite3.Connection,
    *,
    source_key: str,
    obs_date: str,
    raw_value: float | None,
    raw_value_text: str | None,
    loaded_at_utc: str = "2026-01-10T00:00:00+00:00",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_macro_source_raw (
          source_key, vendor, external_series_id, observation_date,
          raw_value, raw_value_text, source_url, loaded_at_utc, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_key,
            "FRED" if source_key != "PCR_EQUITY_CBOE" else "CBOE",
            source_key,
            obs_date,
            raw_value,
            raw_value_text,
            "https://example.test/source",
            loaded_at_utc,
            "raw_run_1",
        ),
    )


def _seed_fixture_raw(conn: sqlite3.Connection) -> None:
    _insert_raw(conn, source_key="BTC_USD_CBBTCUSD", obs_date="2026-01-03", raw_value=100.0, raw_value_text="100")
    _insert_raw(conn, source_key="HY_OAS_BAMLH0A0HYM2", obs_date="2026-01-02", raw_value=5.5, raw_value_text="5.5")
    _insert_raw(conn, source_key="FED_WALCL", obs_date="2026-01-01", raw_value=9000.0, raw_value_text="9000")
    _insert_raw(conn, source_key="USD_BROAD_DTWEXBGS", obs_date="2026-01-01", raw_value=120.0, raw_value_text="120")
    _insert_raw(conn, source_key="PCR_EQUITY_CBOE", obs_date="2026-01-02", raw_value=0.7, raw_value_text="0.7")
    _insert_raw(conn, source_key="PCR_EQUITY_CBOE", obs_date="2026-01-03", raw_value=None, raw_value_text=".")
    conn.commit()


def test_macro_source_daily_migration_creates_table_and_pk() -> None:
    conn = _new_conn()
    cols = {
        str(row[1]): str(row[2])
        for row in conn.execute("PRAGMA table_info(macro_source_daily)")
    }
    assert cols["as_of_date"] == "TEXT"
    assert cols["source_code"] == "TEXT"
    assert cols["source_value"] == "REAL"
    assert cols["source_value_raw_text"] == "TEXT"
    assert cols["is_forward_filled"] == "INTEGER"
    assert cols["source_frequency"] == "TEXT"
    assert cols["published_at_utc"] == "TEXT"
    assert cols["retrieved_at_utc"] == "TEXT"
    assert cols["revision_tag"] == "TEXT"
    assert cols["run_id"] == "TEXT"

    pk_cols = {
        int(row[5]): str(row[1])
        for row in conn.execute("PRAGMA table_info(macro_source_daily)")
        if int(row[5]) > 0
    }
    assert pk_cols == {1: "as_of_date", 2: "source_code"}


def test_normalization_reads_only_from_raw_table() -> None:
    conn = _new_conn()
    _seed_fixture_raw(conn)
    conn.execute(
        """
        INSERT INTO macro_source_daily (
          as_of_date, source_code, source_value, source_value_raw_text, source_frequency,
          published_at_utc, retrieved_at_utc, revision_tag, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "2026-01-01",
            "BTC_USD_CBBTCUSD",
            999.0,
            "999",
            "DAILY_7D",
            "2000-01-01T00:00:00+00:00",
            "2000-01-01T00:00:00+00:00",
            None,
            "seed",
        ),
    )
    conn.commit()

    out = normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="replace-all",
        computed_at="2026-01-05T00:00:00+00:00",
    )
    assert out.summary_status == "OK"
    row = conn.execute(
        """
        SELECT source_value
        FROM macro_source_daily
        WHERE as_of_date='2026-01-03' AND source_code='BTC_USD_CBBTCUSD'
        """
    ).fetchone()
    assert row is not None
    assert float(row[0]) == 100.0


def test_normalization_never_uses_future_observations() -> None:
    conn = _new_conn()
    _insert_raw(conn, source_key="BTC_USD_CBBTCUSD", obs_date="2026-01-04", raw_value=101.0, raw_value_text="101")
    conn.commit()
    normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="upsert",
        computed_at="2026-01-05T00:00:00+00:00",
    )
    row = conn.execute(
        """
        SELECT 1
        FROM macro_source_daily
        WHERE as_of_date='2026-01-03' AND source_code='BTC_USD_CBBTCUSD'
        """
    ).fetchone()
    assert row is None


def test_normalization_skips_null_raw_values() -> None:
    conn = _new_conn()
    _insert_raw(conn, source_key="PCR_EQUITY_CBOE", obs_date="2026-01-03", raw_value=None, raw_value_text=".")
    conn.commit()
    normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="upsert",
        computed_at="2026-01-05T00:00:00+00:00",
    )
    row = conn.execute(
        """
        SELECT 1
        FROM macro_source_daily
        WHERE as_of_date='2026-01-03' AND source_code='PCR_EQUITY_CBOE'
        """
    ).fetchone()
    assert row is None


def test_normalization_respects_source_forward_fill_limits() -> None:
    conn = _new_conn()
    _seed_fixture_raw(conn)
    normalize_macro_sources(
        conn,
        date_from="2026-01-04",
        date_to="2026-01-04",
        mode="upsert",
        computed_at="2026-01-05T00:00:00+00:00",
    )
    btc = conn.execute(
        "SELECT source_value FROM macro_source_daily WHERE as_of_date='2026-01-04' AND source_code='BTC_USD_CBBTCUSD'"
    ).fetchone()
    hy = conn.execute(
        "SELECT source_value FROM macro_source_daily WHERE as_of_date='2026-01-04' AND source_code='HY_OAS_BAMLH0A0HYM2'"
    ).fetchone()
    pcr = conn.execute(
        "SELECT source_value FROM macro_source_daily WHERE as_of_date='2026-01-04' AND source_code='PCR_EQUITY_CBOE'"
    ).fetchone()
    usd = conn.execute(
        "SELECT source_value FROM macro_source_daily WHERE as_of_date='2026-01-04' AND source_code='USD_BROAD_DTWEXBGS'"
    ).fetchone()
    walcl = conn.execute(
        "SELECT source_value FROM macro_source_daily WHERE as_of_date='2026-01-04' AND source_code='FED_WALCL'"
    ).fetchone()
    assert btc is not None and float(btc[0]) == 100.0
    assert hy is not None and float(hy[0]) == 5.5
    assert pcr is not None and float(pcr[0]) == 0.7
    assert usd is not None and float(usd[0]) == 120.0
    assert walcl is not None and float(walcl[0]) == 9000.0

    normalize_macro_sources(
        conn,
        date_from="2026-01-06",
        date_to="2026-01-06",
        mode="upsert",
        computed_at="2026-01-07T00:00:00+00:00",
    )
    btc_late = conn.execute(
        "SELECT 1 FROM macro_source_daily WHERE as_of_date='2026-01-06' AND source_code='BTC_USD_CBBTCUSD'"
    ).fetchone()
    hy_late = conn.execute(
        "SELECT 1 FROM macro_source_daily WHERE as_of_date='2026-01-06' AND source_code='HY_OAS_BAMLH0A0HYM2'"
    ).fetchone()
    pcr_late = conn.execute(
        "SELECT 1 FROM macro_source_daily WHERE as_of_date='2026-01-06' AND source_code='PCR_EQUITY_CBOE'"
    ).fetchone()
    usd_late = conn.execute(
        "SELECT source_value FROM macro_source_daily WHERE as_of_date='2026-01-06' AND source_code='USD_BROAD_DTWEXBGS'"
    ).fetchone()
    walcl_late = conn.execute(
        "SELECT source_value FROM macro_source_daily WHERE as_of_date='2026-01-06' AND source_code='FED_WALCL'"
    ).fetchone()
    assert btc_late is None
    assert hy_late is None
    assert pcr_late is None
    assert usd_late is not None and float(usd_late[0]) == 120.0
    assert walcl_late is not None and float(walcl_late[0]) == 9000.0

    normalize_macro_sources(
        conn,
        date_from="2026-01-20",
        date_to="2026-01-20",
        mode="upsert",
        computed_at="2026-01-21T00:00:00+00:00",
    )
    walcl_very_late = conn.execute(
        "SELECT source_value FROM macro_source_daily WHERE as_of_date='2026-01-20' AND source_code='FED_WALCL'"
    ).fetchone()
    usd_very_late = conn.execute(
        "SELECT 1 FROM macro_source_daily WHERE as_of_date='2026-01-20' AND source_code='USD_BROAD_DTWEXBGS'"
    ).fetchone()
    assert walcl_very_late is not None and float(walcl_very_late[0]) == 9000.0
    assert usd_very_late is None


def test_normalization_writes_expected_aligned_values() -> None:
    conn = _new_conn()
    _seed_fixture_raw(conn)
    out = normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="upsert",
        computed_at="2026-01-05T00:00:00+00:00",
    )
    assert out.raw_rows_scanned >= 6
    assert out.distinct_sources_normalized == 5

    rows = conn.execute(
        """
        SELECT source_code, source_value, source_frequency, published_at_utc, retrieved_at_utc, revision_tag
        FROM macro_source_daily
        WHERE as_of_date='2026-01-03'
        ORDER BY source_code
        """
    ).fetchall()
    assert rows == [
        ("BTC_USD_CBBTCUSD", 100.0, "DAILY_7D", "2026-01-10T00:00:00+00:00", "2026-01-05T00:00:00+00:00", None),
        ("FED_WALCL", 9000.0, "WEEKLY", "2026-01-10T00:00:00+00:00", "2026-01-05T00:00:00+00:00", None),
        ("HY_OAS_BAMLH0A0HYM2", 5.5, "DAILY_CLOSE", "2026-01-10T00:00:00+00:00", "2026-01-05T00:00:00+00:00", None),
        ("PCR_EQUITY_CBOE", 0.7, "DAILY", "2026-01-10T00:00:00+00:00", "2026-01-05T00:00:00+00:00", None),
        ("USD_BROAD_DTWEXBGS", 120.0, "DAILY", "2026-01-10T00:00:00+00:00", "2026-01-05T00:00:00+00:00", None),
    ]


def test_normalization_sets_is_forward_filled_same_day_vs_carried() -> None:
    conn = _new_conn()
    _seed_fixture_raw(conn)
    normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="upsert",
        computed_at="2026-01-05T00:00:00+00:00",
    )
    btc = conn.execute(
        """
        SELECT is_forward_filled
        FROM macro_source_daily
        WHERE as_of_date='2026-01-03' AND source_code='BTC_USD_CBBTCUSD'
        """
    ).fetchone()
    walcl = conn.execute(
        """
        SELECT is_forward_filled
        FROM macro_source_daily
        WHERE as_of_date='2026-01-03' AND source_code='FED_WALCL'
        """
    ).fetchone()
    assert btc is not None and int(btc[0]) == 0
    assert walcl is not None and int(walcl[0]) == 1


def test_normalization_insert_missing_is_deterministic() -> None:
    conn = _new_conn()
    _seed_fixture_raw(conn)
    first = normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="insert-missing",
    )
    second = normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="insert-missing",
    )
    assert first.normalized_rows_inserted == 5
    assert second.normalized_rows_inserted == 0
    assert second.normalized_rows_skipped == 5


def test_normalization_upsert_is_deterministic() -> None:
    conn = _new_conn()
    _seed_fixture_raw(conn)
    first = normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="upsert",
    )
    second = normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="upsert",
    )
    assert first.normalized_rows_inserted == 5
    assert second.normalized_rows_inserted == 0
    assert second.normalized_rows_updated == 5


def test_normalization_replace_all_is_bounded_to_requested_scope() -> None:
    conn = _new_conn()
    _seed_fixture_raw(conn)
    normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="upsert",
    )
    conn.execute(
        """
        INSERT INTO macro_source_daily (
          as_of_date, source_code, source_value, source_value_raw_text, source_frequency,
          published_at_utc, retrieved_at_utc, revision_tag, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "2026-02-01",
            "BTC_USD_CBBTCUSD",
            111.0,
            "111",
            "DAILY_7D",
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00+00:00",
            None,
            "seed",
        ),
    )
    conn.execute(
        """
        INSERT INTO macro_source_daily (
          as_of_date, source_code, source_value, source_value_raw_text, source_frequency,
          published_at_utc, retrieved_at_utc, revision_tag, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "2026-01-03",
            "UNRELATED_SOURCE",
            999.0,
            "999",
            "OTHER",
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00+00:00",
            None,
            "seed",
        ),
    )
    conn.commit()

    out = normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="replace-all",
    )
    assert out.normalized_rows_deleted == 5
    remain_outside = conn.execute(
        "SELECT COUNT(*) FROM macro_source_daily WHERE as_of_date='2026-02-01' AND source_code='BTC_USD_CBBTCUSD'"
    ).fetchone()
    remain_unrelated = conn.execute(
        "SELECT COUNT(*) FROM macro_source_daily WHERE as_of_date='2026-01-03' AND source_code='UNRELATED_SOURCE'"
    ).fetchone()
    assert remain_outside is not None and int(remain_outside[0]) == 1
    assert remain_unrelated is not None and int(remain_unrelated[0]) == 1


def test_cli_emits_summary_lines(monkeypatch, tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "rc.db"
    expected = MacroNormalizeSummary(
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        raw_rows_scanned=20,
        normalized_rows_inserted=10,
        normalized_rows_updated=0,
        normalized_rows_deleted=0,
        normalized_rows_skipped=0,
        distinct_sources_normalized=5,
        summary_status="OK",
    )
    monkeypatch.setattr(
        run_macro_normalize,
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
            },
        )(),
    )
    monkeypatch.setattr(run_macro_normalize, "normalize_macro_sources", lambda *args, **kwargs: expected)
    run_macro_normalize.main()
    output = capsys.readouterr().out
    assert "SUMMARY status=OK" in output
    assert "SUMMARY date_from=2026-01-01" in output
    assert "SUMMARY raw_rows_scanned=20" in output
    assert "SUMMARY normalized_rows_inserted=10" in output
    assert "SUMMARY summary_status=OK" in output


def test_phase2_does_not_create_score_tables() -> None:
    conn = _new_conn()
    _seed_fixture_raw(conn)
    normalize_macro_sources(
        conn,
        date_from="2026-01-03",
        date_to="2026-01-03",
        mode="upsert",
    )
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
