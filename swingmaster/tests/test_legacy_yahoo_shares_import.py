from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals_v2.legacy_yahoo_shares import (
    parse_ordinary_shares_from_balance_payload,
    run_legacy_yahoo_shares_import,
)


def test_company_validated_exact_date_yahoo_shares_fill_and_replay(tmp_path: Path) -> None:
    v2 = tmp_path / "v2.db"
    legacy = tmp_path / "legacy.db"
    _create_v2(v2)
    _create_legacy(legacy)

    _seed_company(v2, "GOOD", [("2026-03-31", 1000.0), ("2026-06-30", 1005.0), ("2026-09-30", 1010.0), ("2026-12-31", None)])
    _seed_company(v2, "LIMIT", [("2026-03-31", 1000.0), ("2026-06-30", 1005.0), ("2026-09-30", 1010.0), ("2026-12-31", None)])
    _seed_company(v2, "DIV", [("2026-03-31", 1000.0), ("2026-06-30", 1000.0), ("2026-09-30", 1000.0), ("2026-12-31", None)])
    _seed_company(v2, "SCALE", [("2026-03-31", 1000.0), ("2026-06-30", 1000.0), ("2026-09-30", 1000.0), ("2026-12-31", None)])
    _seed_company(v2, "PRIOR", [("2026-03-31", 1000.0), ("2026-06-30", 1005.0), ("2026-09-30", 1010.0), ("2026-12-31", None)])
    _seed_company(v2, "FUTURE", [("2026-03-31", 1000.0), ("2026-06-30", 1005.0), ("2026-09-30", 1010.0), ("2026-12-31", None)])
    _seed_company(v2, "EXIST", [("2026-03-31", 1000.0), ("2026-06-30", 1005.0), ("2026-09-30", 1010.0), ("2026-12-31", 2000.0)])
    _seed_company(v2, "WRONG", [("2026-03-31", 1000.0), ("2026-06-30", 1005.0), ("2026-09-30", 1010.0), ("2026-12-31", None)])

    _seed_yahoo(legacy, "GOOD", {"2026-03-31": 1000.0, "2026-06-30": 1006.0, "2026-09-30": 1012.0, "2026-12-31": 1020.0})
    _seed_yahoo(legacy, "LIMIT", {"2026-03-31": 1000.0, "2026-06-30": 1030.0, "2026-09-30": 1010.0, "2026-12-31": 1020.0})
    _seed_yahoo(legacy, "DIV", {"2026-03-31": 1300.0, "2026-06-30": 1300.0, "2026-09-30": 1300.0, "2026-12-31": 1300.0})
    _seed_yahoo(legacy, "SCALE", {"2026-03-31": 10000.0, "2026-06-30": 10000.0, "2026-09-30": 10000.0, "2026-12-31": 10000.0})
    _seed_yahoo(legacy, "PRIOR", {"2026-03-31": 1000.0, "2026-06-30": 1006.0, "2026-09-30": 1012.0, "2026-12-30": 1020.0})
    _seed_yahoo(legacy, "FUTURE", {"2026-03-31": 1000.0, "2026-06-30": 1006.0, "2026-09-30": 1012.0, "2027-01-01": 1020.0})
    _seed_yahoo(legacy, "EXIST", {"2026-03-31": 1000.0, "2026-06-30": 1006.0, "2026-09-30": 1012.0, "2026-12-31": 2020.0})
    _seed_yahoo(legacy, "WRONG", {"2026-03-31": 1000.0, "2026-06-30": 1006.0, "2026-09-30": 1012.0})
    _insert_yahoo_quarterly(legacy, "WRONG", "2026-12-31", 1020.0, shares_source="WeightedAverageNumberOfSharesOutstandingBasic")

    summary = run_legacy_yahoo_shares_import(
        v2_db=v2,
        legacy_db=legacy,
        artifact_dir=tmp_path / "artifacts",
        run_id="RUN1",
        dry_run=False,
        apply=True,
        create_backup=False,
    )
    replay = run_legacy_yahoo_shares_import(
        v2_db=v2,
        legacy_db=legacy,
        artifact_dir=tmp_path / "replay",
        run_id="RUN1",
        dry_run=True,
        apply=False,
        create_backup=False,
    )

    with sqlite3.connect(v2) as conn:
        conn.row_factory = sqlite3.Row
        values = dict(conn.execute(
            """
            SELECT c.ticker, f.shares_outstanding
            FROM rc_v2_company c
            JOIN rc_v2_quarter q ON q.company_id=c.company_id
            JOIN rc_v2_fundamental_quarterly f ON f.quarter_id=q.quarter_id
            WHERE q.report_date='2026-12-31'
            """
        ))
        provenance = conn.execute("SELECT provider, provider_field, transformation, source_dataset, source_value FROM rc_v2_fundamental_field_source WHERE provider='YAHOO'").fetchone()
        weighted = conn.execute("SELECT SUM(weighted_average_shares_basic IS NOT NULL), SUM(weighted_average_shares_diluted IS NOT NULL) FROM rc_v2_fundamental_quarterly").fetchone()
    assert values["GOOD"] == 1020.0
    assert values["LIMIT"] is None
    assert values["DIV"] is None
    assert values["SCALE"] is None
    assert values["PRIOR"] is None
    assert values["FUTURE"] is None
    assert values["EXIST"] == 2000.0
    assert values["WRONG"] is None
    assert provenance["provider"] == "YAHOO"
    assert provenance["provider_field"] == "Ordinary Shares Number"
    assert provenance["transformation"] == "none"
    assert provenance["source_dataset"] == "legacy_yahoo_quarterly"
    source_value = json.loads(provenance["source_value"])
    assert source_value["age_days"] == 0
    assert source_value["match_type"] == "EXACT_DATE"
    assert source_value["validation_mode"] == "company_validated_yahoo_shares_fallback"
    assert tuple(weighted) == (0, 0)
    assert summary["shares_fills"] == 1
    assert summary["scope_audit"]["shares_outstanding_changes"] == 1
    assert summary["scope_audit"]["unrelated_field_writes"] == 0
    assert summary["bad_provenance"] == 0
    assert replay["dry_run_fills"] == 0
    assert replay["replay"]["shares_delta"] == 0


def test_raw_payload_must_contain_exact_ordinary_shares_field(tmp_path: Path) -> None:
    v2 = tmp_path / "v2.db"
    legacy = tmp_path / "legacy.db"
    _create_v2(v2)
    _create_legacy(legacy)
    _seed_company(v2, "GOOD", [("2026-03-31", 1000.0), ("2026-06-30", 1005.0), ("2026-09-30", 1010.0), ("2026-12-31", None)])
    _seed_yahoo(legacy, "GOOD", {"2026-03-31": 1000.0, "2026-06-30": 1006.0, "2026-09-30": 1012.0})
    _insert_yahoo_quarterly(legacy, "GOOD", "2026-12-31", 1020.0, shares_source="ordinary_shares_number", raw_field="Common Stock Shares Outstanding")

    summary = run_legacy_yahoo_shares_import(
        v2_db=v2,
        legacy_db=legacy,
        artifact_dir=tmp_path / "artifacts",
        run_id="RUN1",
        dry_run=True,
        apply=False,
    )

    assert summary["eligible_rows"] == 0
    assert summary["dry_run_fills"] == 0


def test_duplicate_ordinary_shares_raw_field_rejects() -> None:
    payload = json.dumps({"columns": ["2026-03-31"], "index": ["Ordinary Shares Number", "Ordinary Shares Number"], "data": [[1.0], [2.0]]})
    try:
        parse_ordinary_shares_from_balance_payload(payload)
    except ValueError as exc:
        assert "AMBIGUOUS" in str(exc)
    else:
        raise AssertionError("duplicate Ordinary Shares Number should reject")


def _create_legacy(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
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
            );
            CREATE TABLE rc_fundamental_yahoo_quarterly (
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                shares_outstanding REAL,
                shares_source TEXT,
                shares_quality TEXT,
                source_run_id TEXT,
                run_id TEXT,
                created_at_utc TEXT,
                PRIMARY KEY (market, symbol, period_end_date)
            );
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


def _seed_company(path: Path, ticker: str, rows: list[tuple[str, float | None]]) -> None:
    company_id = abs(hash(ticker)) % 100000
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO rc_v2_company VALUES (?, 'usa', ?, ?, ?, 'ORDINARY', 1, 'now', 'now')", (company_id, ticker, company_id, ticker))
        for idx, (report_date, shares) in enumerate(rows, start=1):
            quarter_id = company_id * 10 + idx
            conn.execute("INSERT INTO rc_v2_quarter VALUES (?, ?, 2026, ?, ?, NULL, NULL, 'fixture', 1, 1, 1, 'now', 'now')", (quarter_id, company_id, f"Q{idx}", report_date))
            conn.execute(
                """
                INSERT INTO rc_v2_fundamental_quarterly (
                    quarter_id, revenue, gross_profit, operating_income, depreciation_amortization,
                    ebit, ebitda, net_income, operating_cashflow, capex, free_cashflow, cash,
                    total_debt, shares_outstanding, weighted_average_shares_basic,
                    weighted_average_shares_diluted, available_canonical_field_count, has_income,
                    has_balance, has_cashflow, seed_status, missing_seed_fields_json,
                    created_at_utc, updated_at_utc
                ) VALUES (?, 100, NULL, NULL, NULL, NULL, 20, NULL, NULL, NULL, 10, 5, 1, ?, NULL, NULL, ?, 1, 1, 1, 'fixture', '[]', 'now', 'now')
                """,
                (quarter_id, shares, 6 if shares is not None else 5),
            )
            if shares is not None:
                conn.execute(
                    """
                    INSERT INTO rc_v2_fundamental_field_source VALUES (
                        ?, 'shares_outstanding', 'SIMFIN_API_SHARES', 'Common Shares Outstanding',
                        'common-shares-outstanding', 'SIMFIN_API_SHARES_RAW', 'hash', 'none', ?, 'SIMFIN_RUN', 'now'
                    )
                    """,
                    (quarter_id, json.dumps({"shares_outstanding": shares, "source_observation_date": report_date, "age_days": 0, "match_type": "EXACT_DATE"})),
                )


def _seed_yahoo(path: Path, symbol: str, values: dict[str, float]) -> None:
    for period, value in values.items():
        _insert_yahoo_quarterly(path, symbol, period, value)


def _insert_yahoo_quarterly(
    path: Path,
    symbol: str,
    period: str,
    value: float,
    *,
    shares_source: str = "ordinary_shares_number",
    raw_field: str = "Ordinary Shares Number",
) -> None:
    payload = json.dumps({"columns": [period], "index": [raw_field], "data": [[value]]}, sort_keys=True)
    run_id = f"YRAW_{symbol}_{period}"
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_yahoo_raw (
                market, provider, symbol, info_json, fast_info_json, quarterly_income_stmt_json,
                quarterly_balance_sheet_json, quarterly_cashflow_json, payload_hash, status,
                error_message, loaded_at_utc, run_id
            ) VALUES ('usa', 'yahoo', ?, '{}', '{}', '{}', ?, '{}', ?, 'OK', NULL, '2026-05-05T00:00:00Z', ?)
            """,
            (symbol, payload, f"hash-{symbol}-{period}", run_id),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO rc_fundamental_yahoo_quarterly (
                market, symbol, period_end_date, shares_outstanding, shares_source, shares_quality,
                source_run_id, run_id, created_at_utc
            ) VALUES ('usa', ?, ?, ?, ?, 'OK', ?, 'YQ', 'now')
            """,
            (symbol, period, value, shares_source, run_id),
        )
