from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from swingmaster.fundamentals.v3_fiscal_calendar import (
    ANCHOR_TABLE,
    PROFILE_TABLE,
    FiscalCalendarTransitionEvidence,
    FiscalCalendarWriteCandidate,
    ensure_fiscal_calendar_schema,
    validate_canonical_write_candidate,
)
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository, V3QuarterRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-27T00:00:00Z"


def setup_conn() -> tuple[sqlite3.Connection, int]:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    apply_v3_schema(conn)
    cid = V3CompanyRepository(conn).admit_company(market="usa", ticker="BBY", now_utc=NOW)
    ensure_fiscal_calendar_schema(conn)
    conn.execute(
        f"""
        INSERT INTO {PROFILE_TABLE} (
          company_id,calendar_type,start_basis,reference_month,reference_day,anchor_weekday,relative_position_rule,
          supports_52_53_week,fiscal_year_label_convention,typical_start_description_raw,profile_parse_status,
          source_type,source_reference,confidence,source_fingerprint,created_at_utc,updated_at_utc
        ) VALUES (?, 'WEEK_BASED_52_53', 'WEEKDAY_NEAR_DATE', 2, 1, 'SUNDAY', 'NEAR_BEGINNING',
          1, 'ISSUER_LABEL_YEAR', 'Sunnuntai lähellä helmikuun alkua', 'PARSED',
          'OTHER_OFFICIAL', 'fixture', 'HIGH', 'fp', ?, ?)
        """,
        (cid, NOW, NOW),
    )
    conn.execute(
        f"""
        INSERT INTO {ANCHOR_TABLE} (
          company_id,fiscal_year,fiscal_year_start_date,source_type,source_reference,confidence,
          verification_status,import_state,source_fingerprint,created_at_utc,updated_at_utc
        ) VALUES
        (?, 2026, '2025-02-02', 'OTHER_OFFICIAL', 'fixture', 'VERIFIED', 'VERIFIED_EXACT_ANCHOR', 'EXACT_MATCH', 'a', ?, ?),
        (?, 2027, '2026-02-01', 'OTHER_OFFICIAL', 'fixture', 'VERIFIED', 'VERIFIED_EXACT_ANCHOR', 'EXACT_MATCH', 'b', ?, ?)
        """,
        (cid, NOW, NOW, cid, NOW, NOW),
    )
    conn.commit()
    return conn, cid


def decision(conn: sqlite3.Connection, cid: int, fy: int, fq: str, period: str, **kwargs: object):
    return validate_canonical_write_candidate(conn, FiscalCalendarWriteCandidate(cid, fy, fq, period, **kwargs))


def test_exact_fy2026_and_fy2027_anchors_are_authoritative() -> None:
    conn, cid = setup_conn()

    assert decision(conn, cid, 2026, "Q1", "2025-05-03").decision == "PASS"
    bad = decision(conn, cid, 2025, "Q1", "2025-05-03")

    assert bad.decision == "BLOCK"
    assert "FY_SHIFT_MINUS_ONE" in bad.reason_codes


def test_current_or_provider_label_cannot_override_exact_anchor() -> None:
    conn, cid = setup_conn()

    bad = validate_canonical_write_candidate(
        conn,
        FiscalCalendarWriteCandidate(
            cid,
            2027,
            "Q2",
            "2025-08-02",
            provider_fiscal_year=2027,
            provider_fiscal_quarter="Q2",
        ),
    )

    assert bad.decision == "BLOCK"
    assert "FY_SHIFT_PLUS_ONE" in bad.reason_codes


def test_backward_inference_week_based_52_week_and_13_week_slot() -> None:
    conn, cid = setup_conn()

    result = decision(conn, cid, 2025, "Q2", "2024-08-03")

    assert result.decision == "PASS"
    assert result.inferred_fiscal_year == 2025
    assert result.inferred_fiscal_quarter == "Q2"
    assert result.slot_confidence == "HIGH"


def test_53_week_and_14_week_q4_are_allowed() -> None:
    conn, cid = setup_conn()
    conn.execute(f"UPDATE {ANCHOR_TABLE} SET fiscal_year_start_date='2026-02-08' WHERE fiscal_year=2027")

    result = decision(conn, cid, 2026, "Q4", "2026-02-07")

    assert result.decision == "PASS"
    assert result.inferred_fiscal_quarter == "Q4"


def test_weekday_weekend_period_end_is_preserved() -> None:
    conn, cid = setup_conn()

    result = decision(conn, cid, 2026, "Q2", "2025-08-02")

    assert result.decision == "PASS"
    assert result.exact_anchor_used == "2025-02-02"


def test_impossible_fq_and_month_end_normalization_block_or_review() -> None:
    conn, cid = setup_conn()

    result = decision(conn, cid, 2026, "Q4", "2025-08-31")

    assert result.decision == "BLOCK"
    assert "FQ_SLOT_MISMATCH" in result.reason_codes
    assert "MONTH_END_NORMALIZATION_SUSPECT" in result.reason_codes


def test_possible_transition_and_stub_return_review_not_block() -> None:
    conn, cid = setup_conn()

    transition = validate_canonical_write_candidate(
        conn,
        FiscalCalendarWriteCandidate(
            cid,
            2027,
            "Q2",
            "2025-08-02",
            transition_evidence=FiscalCalendarTransitionEvidence("POSSIBLE_TRANSITION", "fixture"),
        ),
    )
    stub = validate_canonical_write_candidate(conn, FiscalCalendarWriteCandidate(cid, 2026, "Q2", "2025-08-02", stub_period=True))

    assert transition.decision == "REVIEW"
    assert "POSSIBLE_FISCAL_CALENDAR_TRANSITION" in transition.reason_codes
    assert stub.decision == "REVIEW"


def test_financial_fingerprint_conflict_reviews() -> None:
    conn, cid = setup_conn()

    result = decision(conn, cid, 2026, "Q2", "2025-08-02", financial_fingerprint_state="CONFLICT")

    assert result.decision == "REVIEW"
    assert "FINANCIAL_FINGERPRINT_CONFLICT" in result.reason_codes


def test_target_collision_and_reverse_sequence_block() -> None:
    conn, cid = setup_conn()
    repo = V3QuarterRepository(conn)
    repo.upsert_quarter(company_id=cid, fiscal_year=2026, fiscal_quarter="Q1", period_end_date="2025-05-03", now_utc=NOW)
    repo.upsert_quarter(company_id=cid, fiscal_year=2026, fiscal_quarter="Q3", period_end_date="2025-11-01", now_utc=NOW)

    collision = decision(conn, cid, 2026, "Q1", "2025-05-10")
    reverse = decision(conn, cid, 2026, "Q2", "2025-04-01")

    assert collision.decision == "BLOCK"
    assert "TARGET_IDENTITY_COLLISION" in collision.reason_codes
    assert reverse.decision == "BLOCK"
    assert "REVERSE_SEQUENCE" in reverse.reason_codes


def test_repository_guard_runs_before_canonical_mutation_for_block_and_review() -> None:
    conn, cid = setup_conn()
    repo = V3QuarterRepository(conn)

    with pytest.raises(RuntimeError, match="V3_FISCAL_CALENDAR_GUARD_REJECTED:BLOCK"):
        repo.upsert_quarter(company_id=cid, fiscal_year=2027, fiscal_quarter="Q4", period_end_date="2025-08-31", now_utc=NOW)

    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 0


def test_repository_pass_preserves_existing_write_semantics() -> None:
    conn, cid = setup_conn()

    qid = V3QuarterRepository(conn).upsert_quarter(company_id=cid, fiscal_year=2026, fiscal_quarter="Q2", period_end_date="2025-08-02", publish_date="2025-08-28", now_utc=NOW)

    row = conn.execute("SELECT quarter_id,period_end_date,publish_date FROM v3_quarter WHERE quarter_id=?", (qid,)).fetchone()
    assert row["period_end_date"] == "2025-08-02"
    assert row["publish_date"] == "2025-08-28"


def test_guard_can_be_disabled_for_bounded_manual_repair_tools() -> None:
    conn, cid = setup_conn()

    qid = V3QuarterRepository(conn).upsert_quarter(
        company_id=cid,
        fiscal_year=2027,
        fiscal_quarter="Q4",
        period_end_date="2025-08-31",
        now_utc=NOW,
        enforce_fiscal_calendar_guard=False,
    )

    assert qid > 0
