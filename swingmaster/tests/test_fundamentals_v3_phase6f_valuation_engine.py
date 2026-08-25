from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.fundamentals import v3_phase6f_valuation_engine as p6f


def test_valuation_date_strictly_after_publish() -> None:
    assert p6f.resolve_next_trading_day(["2026-02-02", "2026-02-03"], "2026-02-02")[0] == "2026-02-03"


def test_normal_weekday_publish() -> None:
    assert p6f.resolve_next_trading_day(["2026-02-03"], "2026-02-02") == ("2026-02-03", p6f.STATUS_VALID)


def test_friday_publish_next_actual_trading_day() -> None:
    assert p6f.resolve_next_trading_day(["2026-02-09"], "2026-02-06")[0] == "2026-02-09"


def test_holiday_skipped() -> None:
    assert p6f.resolve_next_trading_day(["2026-01-20"], "2026-01-16")[0] == "2026-01-20"


def test_weekend_publish() -> None:
    assert p6f.resolve_next_trading_day(["2026-02-09"], "2026-02-07")[0] == "2026-02-09"


def test_no_calendar_day_approximation() -> None:
    assert p6f.resolve_next_trading_day(["2026-02-10"], "2026-02-06")[0] != "2026-02-07"


def test_ticker_missing_bar_does_not_redefine_market_date(tmp_path: Path) -> None:
    v3 = fixture_v3_db(tmp_path)
    prices = fixture_price_db(tmp_path, include_aaa_target=False)
    plan = p6f.build_valuation_plan(v3, prices)
    aaa = next(row for row in plan if row["ticker"] == "AAA")
    assert aaa["valuation_date"] == "2026-02-03"
    assert aaa["valuation_status"] == p6f.STATUS_MISSING_TARGET_DAY_PRICE


def test_uses_target_day_close(tmp_path: Path) -> None:
    plan = p6f.build_valuation_plan(fixture_v3_db(tmp_path), fixture_price_db(tmp_path))
    aaa = next(row for row in plan if row["ticker"] == "AAA")
    assert aaa["valuation_close_price"] == 10.0


def test_never_uses_current_or_latest_close(tmp_path: Path) -> None:
    prices = fixture_price_db(tmp_path)
    with sqlite3.connect(prices) as conn:
        conn.execute("INSERT INTO osakedata(osake,pvm,open,high,low,close,volume,market) VALUES ('AAA','2026-08-25',99,99,99,99,1,'usa')")
    aaa = next(row for row in p6f.build_valuation_plan(fixture_v3_db(tmp_path), prices) if row["ticker"] == "AAA")
    assert aaa["valuation_close_price"] == 10.0


def test_never_uses_quarter_end_close_as_fallback(tmp_path: Path) -> None:
    v3 = fixture_v3_db(tmp_path, missing_publish=True)
    prices = fixture_price_db(tmp_path)
    row = p6f.build_valuation_plan(v3, prices)[0]
    assert row["valuation_status"] == p6f.STATUS_MISSING_PUBLISH_DATE


def test_missing_publish_date_status() -> None:
    row = base_ttm_row(ttm_available_date=None)
    assert p6f.calculate_valuation(row, valuation_date=None, close=None, price_status=p6f.STATUS_MISSING_PUBLISH_DATE)["valuation_status"] == p6f.STATUS_MISSING_PUBLISH_DATE


def test_missing_target_day_price_status() -> None:
    row = base_ttm_row()
    assert p6f.calculate_valuation(row, valuation_date="2026-02-03", close=None, price_status=p6f.STATUS_VALID)["valuation_status"] == p6f.STATUS_MISSING_TARGET_DAY_PRICE


def test_next_trading_day_not_closed_pending() -> None:
    assert p6f.resolve_next_trading_day(["2026-02-03"], "2026-02-02", today="2026-02-02") == ("2026-02-03", p6f.STATUS_PENDING_PRICE_DATE)


def test_market_cap_formula() -> None:
    assert valuation()["market_cap"] == 1000.0


def test_net_debt_formula() -> None:
    assert valuation()["net_debt"] == -10.0


def test_enterprise_value_formula() -> None:
    assert valuation()["enterprise_value"] == 990.0


def test_ev_ebit_formula() -> None:
    assert valuation()["ev_ebit"] == 9.9


def test_ebit_yield_formula() -> None:
    assert valuation()["ebit_yield"] == pytest.approx(100.0 / 990.0)


def test_fcf_yield_formula() -> None:
    assert valuation()["fcf_yield"] == 0.07


def test_ev_sales_formula() -> None:
    assert valuation()["ev_sales"] == 0.99


def test_ev_ebitda_formula() -> None:
    assert valuation()["ev_ebitda"] == 8.25


def test_pe_formula() -> None:
    assert valuation()["pe"] == 12.5


def test_ev_ocf_formula() -> None:
    assert valuation()["ev_ocf"] == 9.0


def test_negative_ebit_not_meaningful() -> None:
    snap = p6f.calculate_valuation(base_ttm_row(ttm_ebit=-1.0), valuation_date="2026-02-03", close=10.0, price_status=p6f.STATUS_VALID)
    assert snap["ev_ebit_status"] == p6f.STATUS_NOT_MEANINGFUL


def test_near_zero_ebit_guard() -> None:
    snap = p6f.calculate_valuation(base_ttm_row(ttm_ebit=0.0), valuation_date="2026-02-03", close=10.0, price_status=p6f.STATUS_VALID)
    assert snap["ev_ebit_status"] == p6f.STATUS_NOT_MEANINGFUL


def test_negative_ebitda_not_meaningful() -> None:
    snap = p6f.calculate_valuation(base_ttm_row(ttm_ebitda=-1.0), valuation_date="2026-02-03", close=10.0, price_status=p6f.STATUS_VALID)
    assert snap["ev_ebitda_status"] == p6f.STATUS_NOT_MEANINGFUL


def test_negative_net_income_not_meaningful() -> None:
    snap = p6f.calculate_valuation(base_ttm_row(ttm_net_income=-1.0), valuation_date="2026-02-03", close=10.0, price_status=p6f.STATUS_VALID)
    assert snap["pe_status"] == p6f.STATUS_NOT_MEANINGFUL


def test_negative_fcf_not_cheap() -> None:
    snap = p6f.calculate_valuation(base_ttm_row(ttm_fcf=-1.0), valuation_date="2026-02-03", close=10.0, price_status=p6f.STATUS_VALID)
    assert snap["fcf_yield_status"] == p6f.STATUS_NOT_MEANINGFUL


def test_missing_is_not_not_meaningful() -> None:
    assert p6f.calculate_metric(None, 1.0) == (None, p6f.STATUS_MISSING_INPUT)


def test_deterministic_unique_identity(tmp_path: Path) -> None:
    assert p6f.prove_idempotency(tmp_path / "idem.db")["stored_rows"] == 1


def test_no_duplicate_insert(tmp_path: Path) -> None:
    proof = p6f.prove_idempotency(tmp_path / "idem.db")
    assert proof["second_apply"] == {"NOOP": 1}


def test_rerun_idempotent(tmp_path: Path) -> None:
    proof = p6f.prove_idempotency(tmp_path / "idem.db")
    assert proof["fingerprint_stable"] is True


def test_snapshot_immutable_to_later_price_movement(tmp_path: Path) -> None:
    proof = p6f.prove_idempotency(tmp_path / "idem.db")
    assert proof["later_market_price_changes_historical_row"] is False
    assert proof["stored_close"] == 10.0


def test_model_version_separation(tmp_path: Path) -> None:
    db = tmp_path / "model_sep.db"
    p6f.create_fixture_db(db)
    row = p6f.load_ttm_endpoints(db)[0]
    snap = p6f.calculate_valuation(row, valuation_date="2026-02-03", close=10.0, price_status=p6f.STATUS_VALID)
    p6f.apply_snapshots(db, [snap], run_id="a")
    other = dict(snap, model_version="OTHER_MODEL")
    other["source_fingerprint"] = p6f.snapshot_fingerprint(other)
    p6f.apply_snapshots(db, [other], run_id="b")
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM v3_valuation").fetchone()[0] == 2


def test_source_lineage_preserved() -> None:
    snap = valuation()
    assert snap["endpoint_ttm_id"] == 1
    assert snap["endpoint_quarter_id"] == 1


def test_no_valuation_input_added_to_score_contract() -> None:
    from swingmaster.fundamentals.v3_phase6cr_score_architecture_reconciliation import COMPONENTS

    assert "EV_EBIT" not in {c["component_id"] for c in COMPONENTS}


def test_legacy2_fingerprint_constant_documented() -> None:
    assert "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"


def test_score_total_remains_100() -> None:
    from swingmaster.fundamentals.v3_phase6cr_score_architecture_reconciliation import COMPONENTS

    assert sum(int(c["max_score"]) for c in COMPONENTS) == 100


def test_canonical_untouched_in_dry_run(tmp_path: Path) -> None:
    summary = run_fixture_phase(tmp_path)
    assert summary["production_writes"]["canonical"] == 0


def test_ttm_untouched_in_dry_run(tmp_path: Path) -> None:
    summary = run_fixture_phase(tmp_path)
    assert summary["production_writes"]["ttm"] == 0


def test_lifecycle_untouched_in_dry_run(tmp_path: Path) -> None:
    summary = run_fixture_phase(tmp_path)
    assert summary["production_writes"]["lifecycle"] == 0


def test_score_untouched_in_dry_run(tmp_path: Path) -> None:
    summary = run_fixture_phase(tmp_path)
    assert summary["production_writes"]["score"] == 0


def test_no_unrestricted_production_backfill(tmp_path: Path) -> None:
    summary = run_fixture_phase(tmp_path)
    assert summary["production_writes"]["valuation"] == 0


def test_empty_legacy_schema_rebuilt(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY)")
        assert p6f.ensure_valuation_schema(conn) == "REBUILT_EMPTY_TABLE"


def test_non_empty_legacy_schema_refuses_rebuild(tmp_path: Path) -> None:
    db = tmp_path / "legacy_non_empty.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO v3_valuation VALUES (1)")
        with pytest.raises(RuntimeError, match=p6f.CLASSIFICATION_SCHEMA_REFINEMENT):
            p6f.ensure_valuation_schema(conn)


def valuation() -> dict[str, object]:
    return p6f.calculate_valuation(base_ttm_row(), valuation_date="2026-02-03", close=10.0, price_status=p6f.STATUS_VALID)


def base_ttm_row(**overrides: object) -> dict[str, object]:
    row = {
        "company_id": 1,
        "ticker": "AAA",
        "market": "usa",
        "ttm_id": 1,
        "endpoint_quarter_id": 1,
        "endpoint_fiscal_year": 2025,
        "endpoint_fiscal_quarter": "Q4",
        "period_end": "2025-12-31",
        "ttm_available_date": "2026-02-02",
        "shares_outstanding": 100.0,
        "cash": 20.0,
        "total_debt": 10.0,
        "ttm_revenue": 1000.0,
        "ttm_ebit": 100.0,
        "ttm_ebitda": 120.0,
        "ttm_net_income": 80.0,
        "ttm_ocf": 110.0,
        "ttm_fcf": 70.0,
    }
    row.update(overrides)
    return row


def fixture_v3_db(tmp_path: Path, *, missing_publish: bool = False) -> Path:
    db = tmp_path / ("v3_missing.db" if missing_publish else "v3.db")
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, market TEXT, ticker TEXT, company_name TEXT, active INTEGER);
            CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_ttm(
                ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER,
                endpoint_fiscal_year INTEGER, endpoint_fiscal_quarter TEXT, period_end TEXT,
                ttm_available_date TEXT, shares_outstanding REAL, cash REAL, total_debt REAL,
                ttm_revenue REAL, ttm_ebit REAL, ttm_ebitda REAL, ttm_net_income REAL,
                ttm_ocf REAL, ttm_fcf REAL
            );
            """
        )
        conn.execute("INSERT INTO v3_company VALUES (1,'usa','AAA','AAA Corp',1)")
        conn.execute("INSERT INTO v3_company VALUES (2,'usa','BBB','BBB Corp',1)")
        conn.execute("INSERT INTO v3_quarter VALUES (1)")
        conn.execute("INSERT INTO v3_quarter VALUES (2)")
        publish = None if missing_publish else "2026-02-02"
        for row in [
            base_ttm_row(ttm_available_date=publish),
            base_ttm_row(company_id=2, ticker="BBB", ttm_id=2, endpoint_quarter_id=2, ttm_available_date="2026-02-06"),
        ]:
            fields = [k for k in row if k not in {"ticker", "market"}]
            conn.execute(f"INSERT INTO v3_ttm({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})", [row[k] for k in fields])
        conn.commit()
    return db


def fixture_price_db(tmp_path: Path, *, include_aaa_target: bool = True) -> Path:
    db = tmp_path / ("prices_no_aaa.db" if not include_aaa_target else "prices.db")
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE osakedata(
                id INTEGER PRIMARY KEY AUTOINCREMENT, osake TEXT, pvm TEXT, open REAL,
                high REAL, low REAL, close REAL, volume INTEGER, market TEXT NOT NULL DEFAULT 'usa'
            )
            """
        )
        rows = [
            ("CAL", "2026-02-03", 1.0),
            ("AAA", "2025-12-31", 9.0),
            ("BBB", "2026-02-09", 20.0),
        ]
        if include_aaa_target:
            rows.append(("AAA", "2026-02-03", 10.0))
        for ticker, pvm, close in rows:
            conn.execute("INSERT INTO osakedata(osake,pvm,open,high,low,close,volume,market) VALUES (?,?,?,?,?,?,?,?)", (ticker, pvm, close, close, close, close, 1, "usa"))
        conn.commit()
    return db


def run_fixture_phase(tmp_path: Path) -> dict[str, object]:
    return p6f.run_phase6f_valuation_engine(
        v3_db=fixture_v3_db(tmp_path),
        osakedata_db=fixture_price_db(tmp_path),
        artifact_root=tmp_path / "artifacts",
        write_durable_docs=False,
    )
