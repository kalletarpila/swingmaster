from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals_v2.legacy_yahoo_ebit import (
    build_identity_audit,
    parse_yahoo_ebit_from_income_payload,
    run_legacy_yahoo_ebit_import,
)


def test_direct_yahoo_ebit_fills_null_and_preserves_provenance_and_scope(tmp_path: Path) -> None:
    v2 = tmp_path / "v2.db"
    legacy = tmp_path / "legacy.db"
    _create_v2(v2)
    _create_legacy(legacy)
    _insert_v2_quarter(v2, ticker="AAPL", report_date="2026-03-31", ebit=None, operating_income=90, ebitda=120, revenue=1000)
    _insert_yahoo_raw(legacy, symbol="AAPL", columns=["2026-03-31"], fields={"EBIT": [111.0], "Operating Income": [90.0]})

    summary = run_legacy_yahoo_ebit_import(
        v2_db=v2,
        legacy_db=legacy,
        artifact_dir=tmp_path / "artifacts",
        run_id="RUN1",
        dry_run=False,
        apply=True,
        create_backup=False,
    )

    with sqlite3.connect(v2) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT ebit, operating_income, ebitda, revenue FROM rc_v2_fundamental_quarterly").fetchone()
        provenance = conn.execute("SELECT provider, provider_field, transformation, source_dataset, source_value FROM rc_v2_fundamental_field_source").fetchone()
    assert row["ebit"] == 111.0
    assert row["operating_income"] == 90
    assert row["ebitda"] == 120
    assert row["revenue"] == 1000
    assert provenance["provider"] == "YAHOO"
    assert provenance["provider_field"] == "EBIT"
    assert provenance["transformation"] == "none"
    assert provenance["source_dataset"] == "legacy_yahoo_raw"
    assert json.loads(provenance["source_value"])["provider_field"] == "EBIT"
    assert summary["production_ebit_fills"] == 1
    assert summary["scope_audit"]["ebit_rows_changed"] == 1
    assert summary["scope_audit"]["operating_income_changes"] == 0
    assert summary["scope_audit"]["ebitda_changes"] == 0
    assert summary["scope_audit"]["revenue_changes"] == 0


def test_operating_income_does_not_populate_ebit(tmp_path: Path) -> None:
    v2 = tmp_path / "v2.db"
    legacy = tmp_path / "legacy.db"
    _create_v2(v2)
    _create_legacy(legacy)
    _insert_v2_quarter(v2, ticker="AAPL", report_date="2026-03-31", ebit=None)
    _insert_yahoo_raw(legacy, symbol="AAPL", columns=["2026-03-31"], fields={"Operating Income": [90.0]})

    summary = run_legacy_yahoo_ebit_import(
        v2_db=v2,
        legacy_db=legacy,
        artifact_dir=tmp_path / "artifacts",
        run_id="RUN1",
        dry_run=True,
        apply=False,
    )

    assert summary["eligible_rows"] == 0
    assert summary["dry_run_fills"] == 0


def test_exact_report_date_identity_required(tmp_path: Path) -> None:
    v2 = tmp_path / "v2.db"
    legacy = tmp_path / "legacy.db"
    _create_v2(v2)
    _create_legacy(legacy)
    _insert_v2_quarter(v2, ticker="AAPL", report_date="2026-03-30", ebit=None)
    _insert_yahoo_raw(legacy, symbol="AAPL", columns=["2026-03-31"], fields={"EBIT": [111.0]})

    with sqlite3.connect(v2) as v2_conn, sqlite3.connect(legacy) as legacy_conn:
        v2_conn.row_factory = sqlite3.Row
        legacy_conn.row_factory = sqlite3.Row
        eligible, audit, _conflicts = build_identity_audit(v2_conn=v2_conn, legacy_conn=legacy_conn)

    assert eligible == []
    assert audit[0]["classification"] == "NO_V2_ORDINARY_EXACT_REPORT_DATE"


def test_ambiguous_v2_identity_rejected(tmp_path: Path) -> None:
    v2 = tmp_path / "v2.db"
    legacy = tmp_path / "legacy.db"
    _create_v2(v2)
    _create_legacy(legacy)
    _insert_v2_quarter(v2, ticker="AAPL", report_date="2026-03-31", ebit=None)
    _insert_v2_quarter(v2, ticker="AAPL", report_date="2026-03-31", ebit=None, company_id=2)
    _insert_yahoo_raw(legacy, symbol="AAPL", columns=["2026-03-31"], fields={"EBIT": [111.0]})

    with sqlite3.connect(v2) as v2_conn, sqlite3.connect(legacy) as legacy_conn:
        v2_conn.row_factory = sqlite3.Row
        legacy_conn.row_factory = sqlite3.Row
        eligible, audit, _conflicts = build_identity_audit(v2_conn=v2_conn, legacy_conn=legacy_conn)

    assert eligible == []
    assert audit[0]["classification"] == "AMBIGUOUS_V2_TICKER_REPORT_DATE"


def test_existing_same_noop_existing_different_conflict_and_replay_delta_zero(tmp_path: Path) -> None:
    v2 = tmp_path / "v2.db"
    legacy = tmp_path / "legacy.db"
    _create_v2(v2)
    _create_legacy(legacy)
    _insert_v2_quarter(v2, ticker="SAME", report_date="2026-03-31", ebit=10.0)
    _insert_v2_quarter(v2, ticker="DIFF", report_date="2026-03-31", ebit=20.0, company_id=2)
    _insert_v2_quarter(v2, ticker="NULL", report_date="2026-03-31", ebit=None, company_id=3)
    _insert_yahoo_raw(legacy, symbol="SAME", columns=["2026-03-31"], fields={"EBIT": [10.0]})
    _insert_yahoo_raw(legacy, symbol="DIFF", columns=["2026-03-31"], fields={"EBIT": [21.0]})
    _insert_yahoo_raw(legacy, symbol="NULL", columns=["2026-03-31"], fields={"EBIT": [30.0]})

    summary = run_legacy_yahoo_ebit_import(
        v2_db=v2,
        legacy_db=legacy,
        artifact_dir=tmp_path / "artifacts",
        run_id="RUN1",
        dry_run=False,
        apply=True,
        create_backup=False,
    )
    replay = run_legacy_yahoo_ebit_import(
        v2_db=v2,
        legacy_db=legacy,
        artifact_dir=tmp_path / "artifacts2",
        run_id="RUN1",
        dry_run=True,
        apply=False,
        create_backup=False,
    )

    assert summary["production_ebit_fills"] == 1
    assert summary["conflicts"] == 1
    assert replay["dry_run_fills"] == 0
    assert replay["replay"]["ebit_delta"] == 0


def test_duplicate_ebit_index_is_ambiguous() -> None:
    payload = json.dumps({"columns": ["2026-03-31"], "index": ["EBIT", "EBIT"], "data": [[1.0], [2.0]]})
    try:
        parse_yahoo_ebit_from_income_payload(payload)
    except ValueError as exc:
        assert "AMBIGUOUS" in str(exc)
    else:
        raise AssertionError("duplicate EBIT index should reject")


def _create_legacy(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE rc_fundamental_yahoo_raw (
                id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                provider TEXT NOT NULL,
                symbol TEXT NOT NULL,
                info_json TEXT NOT NULL,
                fast_info_json TEXT NOT NULL,
                quarterly_income_stmt_json TEXT NOT NULL,
                quarterly_balance_sheet_json TEXT NOT NULL,
                quarterly_cashflow_json TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                loaded_at_utc TEXT NOT NULL,
                run_id TEXT NOT NULL
            )
            """
        )


def _create_v2(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE rc_v2_company (
                company_id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                ticker TEXT,
                simfin_id INTEGER,
                company_name TEXT,
                company_profile TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            CREATE TABLE rc_v2_import_run (
                import_run_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                simfin_dir TEXT NOT NULL,
                builder_version TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT
            );
            CREATE TABLE rc_v2_quarter (
                quarter_id INTEGER PRIMARY KEY,
                company_id INTEGER NOT NULL,
                fiscal_year INTEGER NOT NULL,
                fiscal_period TEXT NOT NULL,
                report_date TEXT NOT NULL,
                publish_date TEXT,
                restated_date TEXT,
                quarter_identity_source TEXT NOT NULL,
                has_income INTEGER NOT NULL DEFAULT 0,
                has_balance INTEGER NOT NULL DEFAULT 0,
                has_cashflow INTEGER NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            CREATE TABLE rc_v2_fundamental_quarterly (
                quarter_id INTEGER PRIMARY KEY,
                revenue REAL,
                gross_profit REAL,
                operating_income REAL,
                depreciation_amortization REAL,
                ebit REAL,
                ebitda REAL,
                net_income REAL,
                operating_cashflow REAL,
                capex REAL,
                free_cashflow REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL,
                weighted_average_shares_basic REAL,
                weighted_average_shares_diluted REAL,
                available_canonical_field_count INTEGER NOT NULL,
                has_income INTEGER NOT NULL,
                has_balance INTEGER NOT NULL,
                has_cashflow INTEGER NOT NULL,
                seed_status TEXT NOT NULL,
                missing_seed_fields_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            CREATE TABLE rc_v2_fundamental_field_source (
                quarter_id INTEGER NOT NULL,
                field_name TEXT NOT NULL,
                provider TEXT NOT NULL,
                provider_field TEXT NOT NULL,
                source_dataset TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_file_sha256 TEXT NOT NULL,
                transformation TEXT NOT NULL,
                source_value TEXT,
                import_run_id TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                PRIMARY KEY (quarter_id, field_name, provider)
            );
            """
        )


def _insert_v2_quarter(
    path: Path,
    *,
    ticker: str,
    report_date: str,
    ebit: float | None,
    company_id: int = 1,
    operating_income: float | None = None,
    ebitda: float | None = None,
    revenue: float | None = None,
) -> None:
    quarter_id = company_id
    with sqlite3.connect(path) as conn:
        conn.execute(
            "INSERT INTO rc_v2_company VALUES (?, 'usa', ?, ?, ?, 'ORDINARY', 1, 'now', 'now')",
            (company_id, ticker, company_id, ticker),
        )
        conn.execute(
            "INSERT INTO rc_v2_quarter VALUES (?, ?, 2026, 'Q1', ?, NULL, NULL, 'fixture', 1, 1, 1, 'now', 'now')",
            (quarter_id, company_id, report_date),
        )
        conn.execute(
            """
            INSERT INTO rc_v2_fundamental_quarterly (
                quarter_id, revenue, gross_profit, operating_income, depreciation_amortization,
                ebit, ebitda, net_income, operating_cashflow, capex, free_cashflow, cash,
                total_debt, shares_outstanding, weighted_average_shares_basic,
                weighted_average_shares_diluted, available_canonical_field_count, has_income,
                has_balance, has_cashflow, seed_status, missing_seed_fields_json,
                created_at_utc, updated_at_utc
            ) VALUES (?, ?, NULL, ?, NULL, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 1, 1, 1, 'fixture', '[]', 'now', 'now')
            """,
            (quarter_id, revenue, operating_income, ebit, ebitda),
        )


def _insert_yahoo_raw(path: Path, *, symbol: str, columns: list[str], fields: dict[str, list[float | None]]) -> None:
    payload = json.dumps(
        {
            "columns": columns,
            "index": list(fields),
            "data": list(fields.values()),
        },
        sort_keys=True,
    )
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_yahoo_raw (
                market, provider, symbol, info_json, fast_info_json, quarterly_income_stmt_json,
                quarterly_balance_sheet_json, quarterly_cashflow_json, payload_hash, status,
                error_message, loaded_at_utc, run_id
            ) VALUES ('usa', 'yahoo', ?, '{}', '{}', ?, '{}', '{}', ?, 'OK', NULL, '2026-05-05T00:00:00Z', 'YRAW')
            """,
            (symbol, payload, f"hash-{symbol}"),
        )
