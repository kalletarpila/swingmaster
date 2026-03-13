from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

from swingmaster.cli import run_risk_appetite_scorecard
from swingmaster.infra.sqlite.migrator import apply_macro_migrations
from swingmaster.macro.scorecard import (
    ScorecardSummary,
    _bucket_bitcoin,
    _bucket_credit,
    _bucket_dxy,
    _bucket_liquidity,
    _bucket_pcr,
    _confirm_regime,
    _map_regime,
    compute_and_store_risk_appetite_scorecard,
)


def _new_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    apply_macro_migrations(conn)
    return conn


def _insert_norm(
    conn: sqlite3.Connection,
    *,
    day: str,
    source: str,
    value: float,
    is_forward_filled: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO macro_source_daily (
          as_of_date, source_code, source_value, source_value_raw_text,
          is_forward_filled, source_frequency, published_at_utc, retrieved_at_utc, revision_tag, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            day,
            source,
            value,
            str(value),
            is_forward_filled,
            "DAILY",
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00+00:00",
            None,
            "norm_seed",
        ),
    )


def _seed_long_normalized(conn: sqlite3.Connection, *, days: int = 260) -> tuple[str, str]:
    start_dt = date(2025, 1, 1)
    start_date = start_dt.isoformat()
    for i in range(days):
        day = (start_dt + timedelta(days=i)).isoformat()
        # BTC: strong late surge => btc_mom > 0.40 near the end.
        btc = 200.0 if i >= 215 else 100.0
        # HY: low spread => top credit bucket.
        hy = 3.0
        # PCR: low => 90 bucket.
        pcr = 0.59
        # WALCL: uptrend => positive 13w change.
        walcl = 100.0 + (0.2 * i)
        # DXY: late drop => negative diff and high score.
        dxy = 80.0 if i >= 195 else 100.0

        _insert_norm(conn, day=day, source="BTC_USD_CBBTCUSD", value=btc)
        _insert_norm(conn, day=day, source="HY_OAS_BAMLH0A0HYM2", value=hy)
        _insert_norm(conn, day=day, source="PCR_EQUITY_CBOE", value=pcr)
        _insert_norm(conn, day=day, source="FED_WALCL", value=walcl)
        _insert_norm(conn, day=day, source="USD_BROAD_DTWEXBGS", value=dxy)
    conn.commit()
    end_date = (start_dt + timedelta(days=days - 1)).isoformat()
    return start_date, end_date


def test_migration_creates_rc_risk_appetite_daily() -> None:
    conn = _new_conn()
    cols = {
        str(row[1]): str(row[2])
        for row in conn.execute("PRAGMA table_info(rc_risk_appetite_daily)")
    }
    assert cols["as_of_date"] == "TEXT"
    assert cols["risk_score_raw"] == "REAL"
    assert cols["risk_score_final"] == "REAL"
    assert cols["regime_label"] == "TEXT"
    assert cols["regime_label_confirmed"] == "TEXT"
    assert cols["data_quality_status"] == "TEXT"
    pk_cols = [str(row[1]) for row in conn.execute("PRAGMA table_info(rc_risk_appetite_daily)") if int(row[5]) == 1]
    assert pk_cols == ["as_of_date"]
    rc_state_table_exists = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name='rc_state_daily'
        LIMIT 1
        """
    ).fetchone()
    assert rc_state_table_exists is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.41, 100.0),
        (0.26, 80.0),
        (0.11, 60.0),
        (0.01, 50.0),
        (-0.05, 40.0),
        (-0.10, 20.0),
    ],
)
def test_bitcoin_bucket_boundaries(value: float, expected: float) -> None:
    assert _bucket_bitcoin(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (3.49, 100.0),
        (3.50, 80.0),
        (4.49, 80.0),
        (4.50, 60.0),
        (5.49, 60.0),
        (5.50, 40.0),
        (6.99, 40.0),
        (7.00, 20.0),
        (8.99, 20.0),
        (9.00, 0.0),
    ],
)
def test_credit_bucket_boundaries(value: float, expected: float) -> None:
    assert _bucket_credit(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.59, 90.0),
        (0.60, 70.0),
        (0.79, 70.0),
        (0.80, 50.0),
        (0.99, 50.0),
        (1.00, 30.0),
        (1.19, 30.0),
        (1.20, 10.0),
    ],
)
def test_pcr_bucket_boundaries(value: float, expected: float) -> None:
    assert _bucket_pcr(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0.051, 100.0),
        (0.050, 80.0),
        (0.021, 80.0),
        (0.020, 60.0),
        (0.001, 60.0),
        (0.000, 40.0),
        (-0.010, 40.0),
        (-0.020, 20.0),
        (-0.049, 20.0),
        (-0.050, 0.0),
    ],
)
def test_liquidity_bucket_boundaries(value: float, expected: float) -> None:
    assert _bucket_liquidity(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (-0.051, 100.0),
        (-0.050, 80.0),
        (-0.021, 80.0),
        (-0.020, 60.0),
        (-0.001, 60.0),
        (0.000, 40.0),
        (0.029, 40.0),
        (0.030, 20.0),
    ],
)
def test_dxy_bucket_boundaries(value: float, expected: float) -> None:
    assert _bucket_dxy(value) == expected


def test_weighted_raw_score_and_3day_smoothing() -> None:
    conn = _new_conn()
    start, end = _seed_long_normalized(conn, days=260)
    out = compute_and_store_risk_appetite_scorecard(
        conn,
        date_from=start,
        date_to=end,
        mode="upsert",
        computed_at="2026-03-13T00:00:00+00:00",
    )
    assert out.summary_status == "OK"

    day = conn.execute("SELECT date(?, '+220 days')", (start,)).fetchone()[0]
    row = conn.execute(
        """
        SELECT bitcoin_score, credit_score, pcr_score, liquidity_score, dxy_score,
               risk_score_raw, risk_score_final, data_quality_status
        FROM rc_risk_appetite_daily
        WHERE as_of_date=?
        """,
        (day,),
    ).fetchone()
    assert row is not None
    assert float(row[0]) == 100.0
    assert float(row[1]) == 100.0
    assert float(row[2]) == 90.0
    assert float(row[3]) == 100.0
    assert float(row[4]) == 100.0
    assert float(row[5]) == 98.5
    assert float(row[6]) == 98.5
    assert str(row[7]) == "OK"


def test_data_quality_partial_forward_fill_when_any_input_is_carried() -> None:
    conn = _new_conn()
    start, end = _seed_long_normalized(conn, days=260)
    target = conn.execute("SELECT date(?, '+220 days')", (start,)).fetchone()[0]
    # Mark one actually used normalized input row as carried.
    conn.execute(
        """
        UPDATE macro_source_daily
        SET is_forward_filled=1
        WHERE as_of_date=? AND source_code='HY_OAS_BAMLH0A0HYM2'
        """,
        (target,),
    )
    conn.commit()

    compute_and_store_risk_appetite_scorecard(
        conn,
        date_from=start,
        date_to=end,
        mode="upsert",
        computed_at="2026-03-13T00:00:00+00:00",
    )
    row = conn.execute(
        """
        SELECT data_quality_status
        FROM rc_risk_appetite_daily
        WHERE as_of_date=?
        """,
        (target,),
    ).fetchone()
    assert row is not None
    assert str(row[0]) == "PARTIAL_FORWARD_FILL"


def test_regime_mapping_boundaries() -> None:
    assert _map_regime(0.0) == "RISK_OFF"
    assert _map_regime(29.99) == "RISK_OFF"
    assert _map_regime(30.0) == "DEFENSIVE"
    assert _map_regime(44.99) == "DEFENSIVE"
    assert _map_regime(45.0) == "NEUTRAL"
    assert _map_regime(59.99) == "NEUTRAL"
    assert _map_regime(60.0) == "RISK_ON"
    assert _map_regime(74.99) == "RISK_ON"
    assert _map_regime(75.0) == "EUPHORIC"


def test_regime_confirmation_logic() -> None:
    assert _confirm_regime("RISK_ON", "NEUTRAL", "RISK_ON") == "RISK_ON"
    assert _confirm_regime("RISK_ON", "RISK_ON", "NEUTRAL") == "RISK_ON"
    assert _confirm_regime("RISK_ON", "NEUTRAL", "DEFENSIVE") == "DEFENSIVE"
    assert _confirm_regime("NEUTRAL", None, None) is None
    assert _confirm_regime("NEUTRAL", "RISK_ON", "NEUTRAL") == "NEUTRAL"


def test_first_valid_row_sets_confirmed_equal_candidate() -> None:
    conn = _new_conn()
    start, _ = _seed_long_normalized(conn, days=260)
    date_from = conn.execute("SELECT date(?, '+199 days')", (start,)).fetchone()[0]
    date_to = conn.execute("SELECT date(?, '+201 days')", (start,)).fetchone()[0]
    compute_and_store_risk_appetite_scorecard(conn, date_from=date_from, date_to=date_to, mode="upsert")
    first_valid_day = date_to
    row = conn.execute(
        """
        SELECT regime_label, regime_label_confirmed, data_quality_status
        FROM rc_risk_appetite_daily
        WHERE as_of_date=?
        """,
        (first_valid_day,),
    ).fetchone()
    assert row is not None
    assert str(row[2]) == "OK"
    assert row[0] == row[1]


def test_missing_component_behavior() -> None:
    conn = _new_conn()
    start, _ = _seed_long_normalized(conn, days=260)
    # Remove one required source entirely up to target day, forcing missing component.
    target = conn.execute("SELECT date(?, '+220 days')", (start,)).fetchone()[0]
    conn.execute(
        "DELETE FROM macro_source_daily WHERE as_of_date<=? AND source_code='PCR_EQUITY_CBOE'",
        (target,),
    )
    conn.commit()

    compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=target, mode="upsert")
    row = conn.execute(
        """
        SELECT data_quality_status, component_count, risk_score_final
        FROM rc_risk_appetite_daily
        WHERE as_of_date=?
        """,
        (target,),
    ).fetchone()
    assert row is not None
    assert str(row[0]) == "MISSING_COMPONENT"
    assert int(row[1]) < 5
    assert row[2] is None


def test_missing_component_overrides_partial_forward_fill() -> None:
    conn = _new_conn()
    start, _ = _seed_long_normalized(conn, days=260)
    target = conn.execute("SELECT date(?, '+220 days')", (start,)).fetchone()[0]
    conn.execute(
        "UPDATE macro_source_daily SET is_forward_filled=1 WHERE as_of_date=? AND source_code='HY_OAS_BAMLH0A0HYM2'",
        (target,),
    )
    conn.execute(
        "DELETE FROM macro_source_daily WHERE as_of_date<=? AND source_code='PCR_EQUITY_CBOE'",
        (target,),
    )
    conn.commit()
    compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=target, mode="upsert")
    row = conn.execute(
        "SELECT data_quality_status FROM rc_risk_appetite_daily WHERE as_of_date=?",
        (target,),
    ).fetchone()
    assert row is not None
    assert str(row[0]) == "MISSING_COMPONENT"


def test_invalid_source_value_behavior() -> None:
    conn = _new_conn()
    start, _ = _seed_long_normalized(conn, days=260)
    # Make walcl_13w_ago denominator invalid (<=0) for target day.
    anchor = conn.execute("SELECT date(?, '+129 days')", (start,)).fetchone()[0]
    conn.execute(
        "UPDATE macro_source_daily SET source_value=0 WHERE as_of_date=? AND source_code='FED_WALCL'",
        (anchor,),
    )
    conn.commit()
    target = conn.execute("SELECT date(?, '+220 days')", (start,)).fetchone()[0]
    compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=target, mode="upsert")
    row = conn.execute(
        """
        SELECT data_quality_status, risk_score_final
        FROM rc_risk_appetite_daily
        WHERE as_of_date=?
        """,
        (target,),
    ).fetchone()
    assert row is not None
    assert str(row[0]) == "INVALID_SOURCE_VALUE"
    assert row[1] is None


def test_invalid_source_value_overrides_partial_forward_fill() -> None:
    conn = _new_conn()
    start, _ = _seed_long_normalized(conn, days=260)
    target = conn.execute("SELECT date(?, '+220 days')", (start,)).fetchone()[0]
    anchor = conn.execute("SELECT date(?, '+129 days')", (start,)).fetchone()[0]
    conn.execute(
        "UPDATE macro_source_daily SET is_forward_filled=1 WHERE as_of_date=? AND source_code='HY_OAS_BAMLH0A0HYM2'",
        (target,),
    )
    conn.execute(
        "UPDATE macro_source_daily SET source_value=0 WHERE as_of_date=? AND source_code='FED_WALCL'",
        (anchor,),
    )
    conn.commit()
    compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=target, mode="upsert")
    row = conn.execute(
        "SELECT data_quality_status FROM rc_risk_appetite_daily WHERE as_of_date=?",
        (target,),
    ).fetchone()
    assert row is not None
    assert str(row[0]) == "INVALID_SOURCE_VALUE"


def test_insert_missing_is_deterministic() -> None:
    conn = _new_conn()
    start, end = _seed_long_normalized(conn, days=220)
    first = compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=end, mode="insert-missing")
    second = compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=end, mode="insert-missing")
    assert first.score_rows_inserted > 0
    assert second.score_rows_inserted == 0
    assert second.score_rows_skipped == first.score_rows_inserted


def test_upsert_is_deterministic() -> None:
    conn = _new_conn()
    start, end = _seed_long_normalized(conn, days=220)
    first = compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=end, mode="upsert")
    second = compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=end, mode="upsert")
    assert first.score_rows_inserted > 0
    assert second.score_rows_inserted == 0
    assert second.score_rows_updated == first.score_rows_inserted


def test_replace_all_is_bounded() -> None:
    conn = _new_conn()
    start, end = _seed_long_normalized(conn, days=220)
    compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=end, mode="upsert")
    conn.execute(
        """
        INSERT INTO rc_risk_appetite_daily (
          as_of_date, data_quality_status, component_count, run_id, created_at_utc
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ("2028-01-01", "MISSING_COMPONENT", 0, "seed", "2028-01-01T00:00:00+00:00"),
    )
    conn.commit()
    out = compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=end, mode="replace-all")
    assert out.score_rows_deleted > 0
    remain = conn.execute("SELECT COUNT(*) FROM rc_risk_appetite_daily WHERE as_of_date='2028-01-01'").fetchone()
    assert remain is not None and int(remain[0]) == 1


def test_cli_emits_summary_lines(monkeypatch, tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "rc.db"
    expected = ScorecardSummary(
        date_from="2026-01-01",
        date_to="2026-01-02",
        mode="upsert",
        normalized_rows_scanned=100,
        score_rows_inserted=2,
        score_rows_updated=0,
        score_rows_deleted=0,
        score_rows_skipped=0,
        valid_rows_published=1,
        missing_component_rows=1,
        summary_status="OK",
    )
    monkeypatch.setattr(
        run_risk_appetite_scorecard,
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
    monkeypatch.setattr(
        run_risk_appetite_scorecard,
        "compute_and_store_risk_appetite_scorecard",
        lambda *args, **kwargs: expected,
    )
    run_risk_appetite_scorecard.main()
    output = capsys.readouterr().out
    assert "SUMMARY status=OK" in output
    assert "SUMMARY normalized_rows_scanned=100" in output
    assert "SUMMARY score_rows_inserted=2" in output
    assert "SUMMARY valid_rows_published=1" in output
    assert "SUMMARY summary_status=OK" in output


def test_compute_reads_only_normalized_table_no_raw_dependency() -> None:
    conn = _new_conn()
    start, end = _seed_long_normalized(conn, days=220)
    # Ensure raw table is empty: compute must still work from macro_source_daily.
    conn.execute("DELETE FROM rc_macro_source_raw")
    conn.commit()
    out = compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=end, mode="upsert")
    assert out.summary_status == "OK"
    count = conn.execute("SELECT COUNT(*) FROM rc_risk_appetite_daily").fetchone()
    assert count is not None and int(count[0]) > 0


def test_summary_valid_rows_matches_persisted_ok_and_partial_rows() -> None:
    conn = _new_conn()
    start, end = _seed_long_normalized(conn, days=260)
    target = conn.execute("SELECT date(?, '+220 days')", (start,)).fetchone()[0]
    conn.execute(
        """
        UPDATE macro_source_daily
        SET is_forward_filled=1
        WHERE as_of_date=? AND source_code='HY_OAS_BAMLH0A0HYM2'
        """,
        (target,),
    )
    conn.commit()

    out = compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=end, mode="upsert")
    persisted_valid = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_risk_appetite_daily
        WHERE as_of_date BETWEEN ? AND ?
          AND risk_score_final IS NOT NULL
          AND regime_label IS NOT NULL
          AND data_quality_status IN ('OK', 'PARTIAL_FORWARD_FILL')
        """,
        (start, end),
    ).fetchone()
    assert persisted_valid is not None
    assert out.valid_rows_published == int(persisted_valid[0])


def test_summary_valid_rows_excludes_missing_and_invalid_and_missing_count_matches() -> None:
    conn = _new_conn()
    start, end = _seed_long_normalized(conn, days=260)
    target = conn.execute("SELECT date(?, '+220 days')", (start,)).fetchone()[0]
    anchor = conn.execute("SELECT date(?, '+129 days')", (start,)).fetchone()[0]
    conn.execute(
        "DELETE FROM macro_source_daily WHERE as_of_date<=? AND source_code='PCR_EQUITY_CBOE'",
        (target,),
    )
    conn.execute(
        "UPDATE macro_source_daily SET source_value=0 WHERE as_of_date=? AND source_code='FED_WALCL'",
        (anchor,),
    )
    conn.commit()

    out = compute_and_store_risk_appetite_scorecard(conn, date_from=start, date_to=end, mode="upsert")
    persisted_valid = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_risk_appetite_daily
        WHERE as_of_date BETWEEN ? AND ?
          AND risk_score_final IS NOT NULL
          AND regime_label IS NOT NULL
          AND data_quality_status IN ('OK', 'PARTIAL_FORWARD_FILL')
        """,
        (start, end),
    ).fetchone()
    persisted_missing = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_risk_appetite_daily
        WHERE as_of_date BETWEEN ? AND ?
          AND data_quality_status='MISSING_COMPONENT'
        """,
        (start, end),
    ).fetchone()
    assert persisted_valid is not None
    assert persisted_missing is not None
    assert out.valid_rows_published == int(persisted_valid[0])
    assert out.missing_component_rows == int(persisted_missing[0])
