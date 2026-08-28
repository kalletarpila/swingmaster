from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_fiscal_calendar import ANCHOR_TABLE, CHAIN_TABLE
from swingmaster.fundamentals.v3_phase8c_ext_historical_anchors import (
    chain_rows,
    fy_columns,
    import_metadata,
    interval_distribution,
    normalized_anchors,
    validate_input,
)
from swingmaster.fundamentals.v3_schema import V3_REQUIRED_TABLES, apply_v3_schema


NOW = "2026-08-28T00:00:00Z"


def make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    apply_v3_schema(conn)
    conn.execute(
        """
        INSERT INTO v3_company(company_id, market, ticker, company_name, active, admission_source, created_at_utc, updated_at_utc)
        VALUES (1, 'usa', 'AAA', 'AAA Inc.', 1, 'TEST', ?, ?),
               (2, 'usa', 'BBB', 'BBB Inc.', 1, 'TEST', ?, ?)
        """,
        (NOW, NOW, NOW, NOW),
    )
    conn.execute(
        """
        INSERT INTO v3_company_fiscal_calendar_profile(
            company_id, calendar_type, start_basis, reference_month, reference_day, anchor_weekday,
            relative_position_rule, supports_52_53_week, fiscal_year_label_convention,
            typical_start_description_raw, profile_parse_status, source_type, source_reference,
            confidence, source_fingerprint, created_at_utc, updated_at_utc
        )
        VALUES
            (1, 'FIXED_DATE_FISCAL_YEAR', 'FIXED_DATE', 1, 1, NULL, NULL, 0, 'YEAR_OF_FISCAL_PERIOD_END',
             '1. tammikuuta', 'PARSED', 'OFFICIAL_SOURCE', 'src', 'VERIFIED', 'fp1', ?, ?),
            (2, 'WEEK_BASED_52_53', 'WEEKDAY_NEAR_DATE', 2, 1, 'SUNDAY', 'NEAREST', 1, 'YEAR_OF_FISCAL_PERIOD_END',
             'Sunnuntai lähella helmikuun alkua', 'PARSED', 'OFFICIAL_SOURCE', 'src', 'VERIFIED', 'fp2', ?, ?)
        """,
        (NOW, NOW, NOW, NOW),
    )
    conn.commit()
    return conn


def input_rows() -> list[dict[str, str]]:
    return [
        {
            "ticker": "AAA",
            "FY1999 alkoi": "",
            "FY2000 alkoi": "1999-01-01",
            "FY2001 alkoi": "2000-01-01",
            "FY2026 alkoi": "2025-01-01",
            "FY2027 alkoi": "2026-01-01",
            "chain_status": "BROKEN_AT_FY1999",
            "break_reason": "SOURCE_HISTORY_EXHAUSTED",
            "Lähde": "https://example.com/investors/aaa",
        },
        {
            "ticker": "BBB",
            "FY1999 alkoi": "",
            "FY2000 alkoi": "",
            "FY2001 alkoi": "",
            "FY2026 alkoi": "2025-02-02",
            "FY2027 alkoi": "2026-02-01",
            "chain_status": "COMPLETE_TO_FY1999",
            "break_reason": "",
            "Lähde": "https://data.sec.gov/api/xbrl/companyfacts/CIK0000000002.json",
        },
    ]


def test_schema_creates_anchor_chain_table() -> None:
    conn = make_db()
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    assert CHAIN_TABLE in names
    assert CHAIN_TABLE in V3_REQUIRED_TABLES


def test_validation_and_normalization_do_not_infer_blank_anchors() -> None:
    rows = input_rows()
    columns = fy_columns(list(rows[0]))
    active = {"AAA": 1, "BBB": 2}

    validation, ticker_recon = validate_input(rows, columns, active)
    anchors = normalized_anchors(rows, columns, active)
    chains = chain_rows(rows, columns, active)

    assert validation["rows"] == 2
    assert validation["unique_tickers"] == 2
    assert validation["ticker_set_match"] is True
    assert validation["populated_annual_cells"] == 6
    assert validation["blank_annual_cells"] == 4
    assert not validation["invalid_rows"]
    assert len(ticker_recon) == 2
    assert len(anchors) == 6
    assert not any(row["ticker"] == "BBB" and row["fiscal_year"] == 2000 for row in anchors)
    assert any(row["ticker"] == "AAA" and row["fiscal_year"] == 2000 and row["fiscal_year_start_date"] == "1999-01-01" for row in anchors)
    assert {row["confidence"] for row in anchors} == {"VERIFIED_EXACT"}
    assert chains[1]["break_reason"] == "COMPLETE_TO_FY1999"


def test_import_is_idempotent_and_does_not_overwrite_existing_anchor() -> None:
    conn = make_db()
    rows = input_rows()
    columns = fy_columns(list(rows[0]))
    active = {"AAA": 1, "BBB": 2}
    anchors = normalized_anchors(rows, columns, active)
    chains = chain_rows(rows, columns, active)

    first = import_metadata(conn, anchors, chains, NOW)
    second = import_metadata(conn, anchors, chains, NOW)
    changed = [dict(row) for row in anchors]
    changed[0]["fiscal_year_start_date"] = "1999-01-02"
    conflict = import_metadata(conn, changed, chains, NOW)

    assert first["counts"]["anchor_inserted"] == 6
    assert first["counts"]["chain_inserted"] == 2
    assert second["counts"]["anchor_exact_match"] == 6
    assert second["counts"]["chain_exact_match"] == 2
    assert conflict["counts"]["anchor_conflict"] == 1
    assert conn.execute(f"SELECT COUNT(*) FROM {ANCHOR_TABLE}").fetchone()[0] == 6
    assert conn.execute(f"SELECT COUNT(*) FROM {CHAIN_TABLE}").fetchone()[0] == 2


def test_interval_distribution_respects_calendar_type() -> None:
    rows = input_rows()
    columns = fy_columns(list(rows[0]))
    active = {"AAA": 1, "BBB": 2}
    anchors = normalized_anchors(rows, columns, active)
    profiles = {
        1: {"calendar_type": "FIXED_DATE_FISCAL_YEAR"},
        2: {"calendar_type": "WEEK_BASED_52_53"},
    }

    dist, nonstandard = interval_distribution(anchors, profiles)

    buckets = {row["interval_bucket"]: row["rows"] for row in dist}
    assert buckets["FIXED_DATE_NORMAL_YEAR"] == 2
    assert buckets["NORMAL_52_WEEK"] == 1
    assert len(nonstandard) == 1
    assert nonstandard[0]["bucket"] == "LONG_TRANSITION_OR_REVIEW"
