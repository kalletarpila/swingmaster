from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_fiscal_calendar import ANCHOR_TABLE, PROFILE_TABLE, ensure_fiscal_calendar_schema
from swingmaster.fundamentals.v3_phase8d1_full_fiscal_audit import (
    Phase8D1Paths,
    _breakdown,
    _bucket_distance,
    _evidence_class,
    _latest_quarter_ids,
    classify_row,
    enumerate_canonical_rows,
    run_phase8d1,
)
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository, V3QuarterRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-27T00:00:00Z"


def fixture_db(path: Path) -> tuple[sqlite3.Connection, int, int]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    apply_v3_schema(conn)
    ensure_fiscal_calendar_schema(conn)
    companies = V3CompanyRepository(conn)
    quarters = V3QuarterRepository(conn)
    active_cid = companies.admit_company(market="usa", ticker="AAA", now_utc=NOW)
    inactive_cid = companies.admit_company(market="usa", ticker="OLD", now_utc=NOW)
    conn.execute("UPDATE v3_company SET active=0 WHERE company_id=?", (inactive_cid,))
    for cid in (active_cid, inactive_cid):
        conn.execute(
            f"""
            INSERT INTO {PROFILE_TABLE} (
              company_id,calendar_type,start_basis,reference_month,reference_day,supports_52_53_week,
              fiscal_year_label_convention,typical_start_description_raw,profile_parse_status,
              source_type,source_reference,confidence,source_fingerprint,created_at_utc,updated_at_utc
            ) VALUES (?, 'CALENDAR_YEAR', 'FIXED_DATE', 1, 1, 0, 'ISSUER_LABEL_YEAR',
              'Kalenterivuosi alkaa 1. tammikuuta', 'PARSED', 'OTHER_OFFICIAL', 'fixture', 'HIGH', ?, ?, ?)
            """,
            (cid, f"fp-{cid}", NOW, NOW),
        )
        conn.execute(
            f"""
            INSERT INTO {ANCHOR_TABLE} (
              company_id,fiscal_year,fiscal_year_start_date,source_type,source_reference,confidence,
              verification_status,import_state,source_fingerprint,created_at_utc,updated_at_utc
            ) VALUES
            (?, 2026, '2026-01-01', 'OTHER_OFFICIAL', 'fixture', 'VERIFIED', 'VERIFIED_EXACT_ANCHOR', 'EXACT_MATCH', ?, ?, ?),
            (?, 2027, '2027-01-01', 'OTHER_OFFICIAL', 'fixture', 'VERIFIED', 'VERIFIED_EXACT_ANCHOR', 'EXACT_MATCH', ?, ?, ?)
            """,
            (cid, f"a-{cid}", NOW, NOW, cid, f"b-{cid}", NOW, NOW),
        )
    quarters.upsert_quarter(company_id=active_cid, fiscal_year=2026, fiscal_quarter="Q1", period_end_date="2026-03-31", publish_date="2026-04-20", now_utc=NOW)
    quarters.upsert_quarter(company_id=active_cid, fiscal_year=2025, fiscal_quarter="Q1", period_end_date="2025-03-31", publish_date="2025-04-20", now_utc=NOW)
    quarters.upsert_quarter(company_id=active_cid, fiscal_year=2027, fiscal_quarter="Q1", period_end_date="2026-03-31", publish_date="2026-04-20", now_utc=NOW, enforce_fiscal_calendar_guard=False)
    quarters.upsert_quarter(company_id=inactive_cid, fiscal_year=2026, fiscal_quarter="Q1", period_end_date="2026-03-31", publish_date="2026-04-20", now_utc=NOW)
    conn.commit()
    return conn, active_cid, inactive_cid


def test_full_population_enumeration_is_not_sampled_and_includes_inactive(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    conn, _, _ = fixture_db(db)

    enumerated = enumerate_canonical_rows(conn)

    assert len(enumerated) == 4
    assert {row["ticker"] for row in enumerated} == {"AAA", "OLD"}


def test_evidence_distance_and_exact_anchor_conflict_classes(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    conn, _, _ = fixture_db(db)
    downstream = {"ttm": set(), "score": set(), "lifecycle": set(), "valuation": set()}
    all_rows = enumerate_canonical_rows(conn)
    latest8 = _latest_quarter_ids(all_rows, 8)
    classified = [classify_row(conn, row, downstream, latest8, _latest_quarter_ids(all_rows, 4)) for row in all_rows]

    exact_bad = next(row for row in classified if row["ticker"] == "AAA" and row["fiscal_year"] == 2027)
    backward_ok = next(row for row in classified if row["ticker"] == "AAA" and row["fiscal_year"] == 2025)

    assert exact_bad["guard_decision"] == "BLOCK"
    assert exact_bad["block_kind"] == "EXACT_ANCHOR_PROVEN_CONFLICT"
    assert exact_bad["block_confidence"] == "PROVEN_HIGH"
    assert backward_ok["inference_distance_bucket"] == "1 year"
    assert backward_ok["structural_evidence_class"] == "BACKWARD_INFERENCE_1FY"


def test_breakdowns_cover_calendar_year_and_company_inputs() -> None:
    data = [
        {"calendar_type": "CALENDAR_YEAR", "guard_decision": "PASS", "reason_codes": ""},
        {"calendar_type": "CALENDAR_YEAR", "guard_decision": "BLOCK", "reason_codes": "FY_SHIFT_PLUS_ONE"},
    ]

    breakdown = _breakdown(data, "calendar_type")

    assert breakdown == [{
        "calendar_type": "CALENDAR_YEAR",
        "rows": 2,
        "PASS": 1,
        "PASS_WITH_WARNING": 0,
        "REVIEW": 0,
        "BLOCK": 1,
        "block_pct": 50.0,
        "major_reason_codes": "FY_SHIFT_PLUS_ONE",
    }]


def test_distance_and_inference_taxonomy() -> None:
    assert _bucket_distance(0, "EXACT") == "0 exact/current anchor interval"
    assert _bucket_distance(8, "BACKWARD") == "6-10 years"
    assert _bucket_distance(1, "FORWARD") == "forward"
    assert _evidence_class(12, "BACKWARD", False, False, "WEEK_BASED_52_53", "PARSED", "") == "BACKWARD_INFERENCE_GT10FY"
    assert _evidence_class(None, "UNKNOWN", False, False, "UNKNOWN", "MISSING", "") == "INSUFFICIENT_METADATA"
    assert _evidence_class(1, "BACKWARD", False, False, "CALENDAR_YEAR", "PARSED", "POSSIBLE_FISCAL_CALENDAR_TRANSITION") == "TRANSITION_AWARE"


def test_full_audit_writes_artifacts_without_changing_fingerprints(tmp_path: Path) -> None:
    db = tmp_path / "v3.db"
    conn, _, _ = fixture_db(db)
    before_rows = conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0]
    conn.close()

    summary = run_phase8d1(Phase8D1Paths(artifact_root=tmp_path / "artifacts", v3_db=db))

    with sqlite3.connect(db) as after:
        assert after.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == before_rows
    assert summary["rows_audited"] == 4
    assert summary["safety"]["canonical_changed"] == 0
    assert (tmp_path / "artifacts" / "full_canonical_fiscal_guard_audit.csv").exists()
    assert (tmp_path / "artifacts" / "current_ttm_input_guard_analysis.csv").exists()
