from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_fiscal_calendar import (
    ANCHOR_TABLE,
    PROFILE_TABLE,
    build_anchors,
    build_profiles,
    ensure_fiscal_calendar_schema,
    import_metadata,
    infer_slot,
    parse_profile_description,
    validate_canonical_row,
    validate_input_csv,
)
from swingmaster.fundamentals.v3_schema import V3_REQUIRED_TABLES, apply_v3_schema


NOW = "2026-08-27T00:00:00Z"


def make_db(tmp_path: Path, tickers: list[str]) -> Path:
    db = tmp_path / "v3.db"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        apply_v3_schema(conn)
        for idx, ticker in enumerate(tickers, 1):
            conn.execute(
                """
                INSERT INTO v3_company(company_id,market,ticker,company_name,active,admission_source,created_at_utc,updated_at_utc)
                VALUES (?, 'usa', ?, ?, 1, 'TEST', ?, ?)
                """,
                (idx, ticker, ticker, NOW, NOW),
            )
        conn.commit()
    return db


def write_input(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["ticker", "FY2027 alkoi", "FY2026 alkoi", "Tyypillinen tilikauden alku", "Lähde"])
        writer.writeheader()
        writer.writerows(rows)


def sample_rows() -> list[dict[str, str]]:
    return [
        {"ticker": "AAA", "FY2027 alkoi": "2027-01-01", "FY2026 alkoi": "2026-01-01", "Tyypillinen tilikauden alku": "1. tammikuuta (kalenterivuosi)", "Lähde": "https://data.sec.gov/api/xbrl/companyfacts/CIK0000000001.json"},
        {"ticker": "BBB", "FY2027 alkoi": "", "FY2026 alkoi": "2025-07-01", "Tyypillinen tilikauden alku": "1. heinäkuuta", "Lähde": "https://example.com/ir"},
        {"ticker": "WEEK", "FY2027 alkoi": "2026-02-01", "FY2026 alkoi": "2025-02-02", "Tyypillinen tilikauden alku": "Sunnuntai lähellä helmikuun alkua", "Lähde": "https://example.com/investors"},
    ]


def test_schema_tables_are_required_and_created() -> None:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert PROFILE_TABLE in names
    assert ANCHOR_TABLE in names
    assert PROFILE_TABLE in V3_REQUIRED_TABLES
    assert ANCHOR_TABLE in V3_REQUIRED_TABLES


def test_input_reconciliation_and_required_fields(tmp_path: Path) -> None:
    db = make_db(tmp_path, ["AAA", "BBB", "WEEK"])
    input_csv = tmp_path / "input.csv"
    write_input(input_csv, sample_rows())

    rows, summary, reconciliation = validate_input_csv(input_csv, db)

    assert len(rows) == 3
    assert summary["unique_tickers"] == 3
    assert summary["matched_active_v3_tickers"] == 3
    assert summary["csv_only_tickers"] == []
    assert summary["v3_only_active_tickers"] == []
    assert summary["FY2026_populated"] == 3
    assert summary["FY2027_populated"] == 2
    assert summary["source_populated"] == 3
    assert summary["invalid_rows"] == 0
    assert len(reconciliation) == 3


def test_input_rejects_duplicates_bad_dates_and_v3_only(tmp_path: Path) -> None:
    db = make_db(tmp_path, ["AAA", "MISSING"])
    input_csv = tmp_path / "input.csv"
    write_input(
        input_csv,
        [
            {"ticker": "AAA", "FY2027 alkoi": "", "FY2026 alkoi": "bad", "Tyypillinen tilikauden alku": "1. tammikuuta", "Lähde": "src"},
            {"ticker": "AAA", "FY2027 alkoi": "", "FY2026 alkoi": "2026-01-01", "Tyypillinen tilikauden alku": "1. tammikuuta", "Lähde": "src"},
        ],
    )

    _rows, summary, _reconciliation = validate_input_csv(input_csv, db)

    assert summary["duplicate_tickers"] == ["AAA"]
    assert summary["invalid_rows"] == 1
    assert summary["v3_only_active_tickers"] == ["MISSING"]
    assert summary["material_difference"] is True


def test_profile_parser_classifies_calendar_fixed_week_and_unparsed() -> None:
    assert parse_profile_description("1. tammikuuta (kalenterivuosi)", "2026-01-01", None)["calendar_type"] == "CALENDAR_YEAR"
    fixed = parse_profile_description("1. heinäkuuta", "2025-07-01", None)
    assert fixed["calendar_type"] == "FIXED_DATE_FISCAL_YEAR"
    assert fixed["start_basis"] == "FIXED_DATE"
    week = parse_profile_description("Sunnuntai lähellä helmikuun alkua", "2025-02-02", "2026-02-01")
    assert week["calendar_type"] == "WEEK_BASED_52_53"
    assert week["anchor_weekday"] == "SUNDAY"
    unparsed = parse_profile_description("Vaihtelee; havaitut alut 1.1.–31.12.", "2026-01-01", None)
    assert unparsed["profile_parse_status"] == "UNPARSED"


def test_build_profiles_preserves_raw_description_and_source() -> None:
    profiles = build_profiles(sample_rows(), {"AAA": 1, "BBB": 2, "WEEK": 3})

    assert len(profiles) == 3
    assert profiles[0]["typical_start_description_raw"] == "1. tammikuuta (kalenterivuosi)"
    assert profiles[0]["source_reference"].startswith("https://data.sec.gov")
    assert profiles[0]["source_type"] == "SEC_COMPANYFACTS"
    assert profiles[1]["source_type"] == "ISSUER_IR"


def test_build_anchors_uses_issuer_fiscal_year_label_and_does_not_infer_missing_fy2027() -> None:
    anchors = build_anchors(sample_rows(), {"AAA": 1, "BBB": 2, "WEEK": 3})

    assert len(anchors) == 5
    assert {"fiscal_year": 2026, "fiscal_year_start_date": "2025-07-01"}.items() <= anchors[2].items()
    assert not any(a["ticker"] == "BBB" and a["fiscal_year"] == 2027 for a in anchors)


def test_import_metadata_is_idempotent_and_detects_anchor_conflict(tmp_path: Path) -> None:
    db = make_db(tmp_path, ["AAA"])
    profiles = build_profiles([sample_rows()[0]], {"AAA": 1})
    anchors = build_anchors([sample_rows()[0]], {"AAA": 1})
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        first = import_metadata(conn, profiles, anchors, NOW)
        second = import_metadata(conn, profiles, anchors, NOW)
        changed = [dict(anchors[0]), dict(anchors[1])]
        changed[0]["fiscal_year_start_date"] = "2026-01-02"
        conflict = import_metadata(conn, profiles, changed, NOW)
        conn.commit()
        anchor_count = conn.execute(f"SELECT COUNT(*) FROM {ANCHOR_TABLE}").fetchone()[0]

    assert first["counts"]["profile_inserted"] == 1
    assert first["counts"]["anchor_inserted"] == 2
    assert second["counts"]["profile_exact_match"] == 1
    assert second["counts"]["anchor_exact_match"] == 2
    assert conflict["counts"]["anchor_conflict_review_required"] == 1
    assert anchor_count == 2


def test_slot_inference_supports_exact_364_day_interval_and_13_week_slot() -> None:
    calendar = {
        "profile": {"calendar_type": "WEEK_BASED_52_53"},
        "anchors": [
            {"fiscal_year": 2026, "fiscal_year_start_date": "2025-02-02"},
            {"fiscal_year": 2027, "fiscal_year_start_date": "2026-02-01"},
        ],
    }

    slot = infer_slot(calendar, "2025-08-02")

    assert slot["candidate_fiscal_year"] == 2026
    assert slot["candidate_fiscal_quarter"] == "Q2"
    assert slot["confidence"] == "EXACT_ANCHOR"
    assert slot["expected_slot_length_days"] == 91


def test_slot_inference_supports_371_day_interval_and_14_week_q4() -> None:
    calendar = {
        "profile": {"calendar_type": "WEEK_BASED_52_53"},
        "anchors": [
            {"fiscal_year": 2026, "fiscal_year_start_date": "2025-02-02"},
            {"fiscal_year": 2027, "fiscal_year_start_date": "2026-02-08"},
        ],
    }

    slot = infer_slot(calendar, "2026-02-07")

    assert slot["candidate_fiscal_quarter"] == "Q4"
    assert slot["expected_slot_length_days"] == 98


def test_slot_inference_supports_backward_and_forward_inference_without_storing_truth() -> None:
    calendar = {
        "profile": {"calendar_type": "FIXED_DATE_FISCAL_YEAR"},
        "anchors": [{"fiscal_year": 2027, "fiscal_year_start_date": "2026-02-01"}],
    }

    backward = infer_slot(calendar, "2025-08-02")
    forward = infer_slot(calendar, "2027-05-01")

    assert backward["candidate_fiscal_year"] == 2026
    assert forward["candidate_fiscal_year"] == 2028


def test_validator_detects_fy_shifts_fq_mismatch_period_outside_publish_and_reverse_sequence() -> None:
    calendar = {
        "profile": {"calendar_type": "WEEK_BASED_52_53"},
        "anchors": [
            {"fiscal_year": 2026, "fiscal_year_start_date": "2025-02-02"},
            {"fiscal_year": 2027, "fiscal_year_start_date": "2026-02-01"},
        ],
    }
    row = {"fiscal_year": 2027, "fiscal_quarter": "Q4", "period_end_date": "2025-08-31", "publish_date": "2025-08-01"}
    previous = {"period_end_date": "2025-09-30"}

    result = validate_canonical_row(calendar, row, previous)

    assert result["status"] == "REVIEW"
    assert "FY_SHIFT_PLUS_ONE" in result["reason_codes"]
    assert "FQ_SLOT_MISMATCH" in result["reason_codes"]
    assert "PERIOD_END_OUTSIDE_SLOT" in result["reason_codes"]
    assert "PUBLISH_SEQUENCE_MISMATCH" in result["reason_codes"]
    assert "REVERSE_SEQUENCE" in result["reason_codes"]


def test_validator_detects_minus_one_shift_and_month_end_warning() -> None:
    calendar = {
        "profile": {"calendar_type": "WEEK_BASED_52_53"},
        "anchors": [
            {"fiscal_year": 2026, "fiscal_year_start_date": "2025-02-02"},
            {"fiscal_year": 2027, "fiscal_year_start_date": "2026-02-01"},
        ],
    }
    row = {"fiscal_year": 2025, "fiscal_quarter": "Q2", "period_end_date": "2025-08-31", "publish_date": "2025-09-20"}

    result = validate_canonical_row(calendar, row)

    assert "FY_SHIFT_MINUS_ONE" in result["reason_codes"]
    assert "MONTH_END_NORMALIZATION_SUSPECT" in result["reason_codes"]


def test_validator_handles_insufficient_metadata_and_clean_calendar_year_pass() -> None:
    assert validate_canonical_row(None, {"fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31"})["status"] == "PASS_WITH_WARNING"
    calendar = {
        "profile": {"calendar_type": "CALENDAR_YEAR"},
        "anchors": [
            {"fiscal_year": 2026, "fiscal_year_start_date": "2026-01-01"},
            {"fiscal_year": 2027, "fiscal_year_start_date": "2027-01-01"},
        ],
    }
    result = validate_canonical_row(calendar, {"fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end_date": "2026-03-31", "publish_date": "2026-04-20"})
    assert result["status"] == "PASS"


def test_weekend_period_end_is_preserved_not_normalized() -> None:
    calendar = {
        "profile": {"calendar_type": "WEEK_BASED_52_53"},
        "anchors": [
            {"fiscal_year": 2026, "fiscal_year_start_date": "2025-02-02"},
            {"fiscal_year": 2027, "fiscal_year_start_date": "2026-02-01"},
        ],
    }

    result = validate_canonical_row(calendar, {"fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end_date": "2025-08-02", "publish_date": "2025-08-28"})

    assert result["status"] == "PASS"
    assert result["slot"]["expected_slot_end"] == "2025-08-02"
