from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_fiscal_calendar import ANCHOR_TABLE, PROFILE_TABLE, ensure_fiscal_calendar_schema
from swingmaster.fundamentals.v3_phase8d3_quarter_slot_calibration import (
    build_calibration_population,
    chronology_summary,
    coverage,
    distribution,
    estimate_fy_start,
    expected_slot,
    latest_six_candidates,
    percentile,
    sequence_quality,
    simulated_decision,
    simulate,
    week_based_analysis,
    window_needed,
)
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository, V3FundamentalsRepository, V3QuarterRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-28T00:00:00Z"


def setup_db(path: Path, ticker: str = "AAA", calendar_type: str = "CALENDAR_YEAR") -> tuple[sqlite3.Connection, int]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    apply_v3_schema(conn)
    ensure_fiscal_calendar_schema(conn)
    cid = V3CompanyRepository(conn).admit_company(market="usa", ticker=ticker, now_utc=NOW)
    start = "2026-01-01" if calendar_type != "WEEK_BASED_52_53" else "2025-02-02"
    next_start = "2027-01-01" if calendar_type != "WEEK_BASED_52_53" else "2026-02-08"
    conn.execute(
        f"""
        INSERT INTO {PROFILE_TABLE} (
          company_id,calendar_type,start_basis,reference_month,reference_day,anchor_weekday,relative_position_rule,
          supports_52_53_week,fiscal_year_label_convention,typical_start_description_raw,profile_parse_status,
          source_type,source_reference,confidence,source_fingerprint,created_at_utc,updated_at_utc
        ) VALUES (?, ?, 'FIXED_DATE', 1, 1, NULL, NULL, ?, 'ISSUER_LABEL_YEAR',
          'fixture', 'PARSED', 'OTHER_OFFICIAL', 'fixture', 'HIGH', 'fp', ?, ?)
        """,
        (cid, calendar_type, 1 if calendar_type == "WEEK_BASED_52_53" else 0, NOW, NOW),
    )
    conn.execute(
        f"""
        INSERT INTO {ANCHOR_TABLE} (
          company_id,fiscal_year,fiscal_year_start_date,source_type,source_reference,confidence,
          verification_status,import_state,source_fingerprint,created_at_utc,updated_at_utc
        ) VALUES
        (?, 2026, ?, 'OTHER_OFFICIAL', 'fixture', 'VERIFIED', 'VERIFIED_EXACT_ANCHOR', 'EXACT_MATCH', 'a', ?, ?),
        (?, 2027, ?, 'OTHER_OFFICIAL', 'fixture', 'VERIFIED', 'VERIFIED_EXACT_ANCHOR', 'EXACT_MATCH', 'b', ?, ?)
        """,
        (cid, start, NOW, NOW, cid, next_start, NOW, NOW),
    )
    conn.commit()
    return conn, cid


def add_quarter(conn: sqlite3.Connection, cid: int, fy: int, fq: str, period: str, provider: str = "YAHOO") -> int:
    qid = V3QuarterRepository(conn).upsert_quarter(company_id=cid, fiscal_year=fy, fiscal_quarter=fq, period_end_date=period, publish_date="2026-04-20", now_utc=NOW, enforce_fiscal_calendar_guard=False)
    V3FundamentalsRepository(conn).write_null_preserving_fields(
        quarter_id=qid,
        values={"revenue": 1.0, "operating_income": 2.0, "net_income": 3.0},
        accepted_source_provider=provider,
        accepted_at_utc=NOW,
    )
    conn.commit()
    return qid


def test_latest_six_selection_and_known_p1_exclusion(tmp_path: Path) -> None:
    conn, cid = setup_db(tmp_path / "v3.db", "BBY")
    qids = [add_quarter(conn, cid, 2025 + idx // 4, f"Q{idx % 4 + 1}", f"202{idx}-03-31") for idx in range(7)]
    audit = {qid: {"guard_decision": "BLOCK", "reason_codes": "FQ_SLOT_MISMATCH"} for qid in qids}

    selected = latest_six_candidates(conn)
    population = build_calibration_population(conn, audit)

    assert len(selected) == 6
    assert {row["calibration_confidence"] for row in population} == {"EXCLUDED_STRUCTURAL_RISK"}


def test_expected_slot_and_offsets_for_exact_anchor(tmp_path: Path) -> None:
    conn, cid = setup_db(tmp_path / "v3.db")
    row = {"company_id": cid, "fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end": "2026-07-01"}
    profile = dict(conn.execute(f"SELECT * FROM {PROFILE_TABLE} WHERE company_id=?", (cid,)).fetchone())
    anchors = {2026: __import__("datetime").date(2026, 1, 1), 2027: __import__("datetime").date(2027, 1, 1)}

    slot = expected_slot(row, profile, anchors)

    assert slot["expected_quarter_start"] == "2026-04-02"
    assert slot["expected_quarter_end"] == "2026-07-01"
    assert estimate_fy_start(2025, profile, anchors)[1] == "ONE_ANCHOR_INFERRED"


def test_distribution_coverage_and_windows() -> None:
    values = [-10, -5, 0, 5, 10]

    assert percentile(values, 50) == 0
    assert distribution(values)["median"] == 0
    assert distribution(values)["abs_p90"] == 10
    assert coverage(values, 5)["rows_inside"] == 3
    assert coverage(values, 14)["inside_pct"] == 100.0
    assert window_needed(values, 95) == 10


def test_sequence_and_publish_chronology() -> None:
    rows = [
        {"fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2026-03-31"},
        {"fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end": "2026-06-30"},
    ]
    high = [{
        "calibration_confidence": "KNOWN_GOOD_HIGH",
        "publish_date": "2026-04-20",
        "reporting_lag_days": 20,
        "publish_chronology": "PUBLISH_CHRONOLOGY_STRONG_PASS",
        "publish_before_next_q_end_plus7": 1,
        "publish_before_next_q_end_plus14": 1,
    }]

    assert sequence_quality(rows) == "COHERENT"
    assert chronology_summary(high)["both_pct"] == 100.0


def test_simulated_window_model_removes_slot_reasons_only() -> None:
    row = {"period_end_offset_days": 6, "current_guard_reasons": "FQ_SLOT_MISMATCH|PERIOD_END_OUTSIDE_SLOT", "current_guard_decision": "BLOCK"}
    hard = {"period_end_offset_days": 6, "current_guard_reasons": "FY_SHIFT_MINUS_ONE|FQ_SLOT_MISMATCH", "current_guard_decision": "BLOCK"}

    assert simulated_decision(row, 7) == "PASS"
    assert simulated_decision(hard, 7) == "BLOCK"


def test_simulation_preserves_known_p1_review_or_block() -> None:
    population = [
        {"ticker": "AAA", "calibration_confidence": "KNOWN_GOOD_HIGH", "period_end_offset_days": 6, "current_guard_reasons": "FQ_SLOT_MISMATCH", "current_guard_decision": "BLOCK"},
        {"ticker": "BBY", "calibration_confidence": "EXCLUDED_STRUCTURAL_RISK", "period_end_offset_days": 6, "current_guard_reasons": "FY_SHIFT_MINUS_ONE|FQ_SLOT_MISMATCH", "current_guard_decision": "BLOCK"},
    ]
    audit = [{**population[0], "latest4q": 1, "latest8q": 1, "latest_quarter": 1, "ttm_input": 1}]

    result = simulate(population, audit, 7)

    assert result["known_good_BLOCK"] == 0
    assert result["known_P1_BLOCK_or_REVIEW"] == 1
    assert result["known_P1_incorrect_PASS"] == 0


def test_week_based_371_day_year_and_14_week_placement(tmp_path: Path) -> None:
    conn, cid = setup_db(tmp_path / "v3.db", "WEEK", "WEEK_BASED_52_53")
    population = [
        {"ticker": "WEEK", "company_id": cid, "fiscal_year": 2026, "fiscal_quarter": "Q1", "period_end": "2025-05-03", "calendar_type": "WEEK_BASED_52_53", "calibration_confidence": "KNOWN_GOOD_HIGH", "expected_fy_start": "2025-02-02", "verified_year_length_days": 371},
        {"ticker": "WEEK", "company_id": cid, "fiscal_year": 2026, "fiscal_quarter": "Q2", "period_end": "2025-08-02", "calendar_type": "WEEK_BASED_52_53", "calibration_confidence": "KNOWN_GOOD_HIGH", "expected_fy_start": "2025-02-02", "verified_year_length_days": 371},
        {"ticker": "WEEK", "company_id": cid, "fiscal_year": 2026, "fiscal_quarter": "Q3", "period_end": "2025-11-01", "calendar_type": "WEEK_BASED_52_53", "calibration_confidence": "KNOWN_GOOD_HIGH", "expected_fy_start": "2025-02-02", "verified_year_length_days": 371},
        {"ticker": "WEEK", "company_id": cid, "fiscal_year": 2026, "fiscal_quarter": "Q4", "period_end": "2026-02-07", "calendar_type": "WEEK_BASED_52_53", "calibration_confidence": "KNOWN_GOOD_HIGH", "expected_fy_start": "2025-02-02", "verified_year_length_days": 371},
    ]

    lengths, placement = week_based_analysis(population)

    assert any(row["length_bucket"] == "14 weeks" and row["fiscal_quarter"] == "Q4" for row in lengths)
    assert placement[0]["extra_week_placement"] == "Q4"
