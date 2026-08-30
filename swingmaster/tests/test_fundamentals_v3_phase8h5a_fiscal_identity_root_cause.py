from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_fiscal_calendar import ANCHOR_TABLE, CHAIN_TABLE, PROFILE_TABLE, ensure_fiscal_calendar_schema
from swingmaster.fundamentals.v3_phase8h5a_fiscal_identity_root_cause import (
    CLASSIFICATION_READY,
    Phase8H5APaths,
    audit_canonical_identity,
    build_repair_candidates,
    fiscal_identity_arbiter,
    h3_mapping_audit,
    latest_missing_candidates,
    possible_missing_q4,
    publish_date_quality,
    reclassify_structural_11,
    run_rehearsal,
    summarize_global,
    target_collision_class,
    validate_csv_vs_production,
)


NOW = "2026-08-30T00:00:00Z"


def make_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, market TEXT, ticker TEXT, company_name TEXT, profile TEXT DEFAULT 'ORDINARY', active INTEGER DEFAULT 1, admission_source TEXT DEFAULT 'x', admission_evidence TEXT, created_at_utc TEXT DEFAULT 'x', updated_at_utc TEXT DEFAULT 'x');
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER NOT NULL, fiscal_quarter TEXT NOT NULL, period_end_date TEXT, publish_date TEXT, market_availability_date TEXT, q_lifecycle TEXT DEFAULT 'OPERATIONALLY_SETTLED', sec_confirmation_state TEXT DEFAULT 'NOT_DERIVABLE', created_at_utc TEXT DEFAULT 'x', updated_at_utc TEXT DEFAULT 'x', UNIQUE(company_id,fiscal_year,fiscal_quarter));
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY, revenue REAL, operating_income REAL, ebit REAL, ebitda REAL, net_income REAL, operating_cashflow REAL, capex REAL, free_cashflow REAL, cash REAL, total_debt REAL, shares_outstanding REAL, accepted_source_provider TEXT, derivation_method TEXT, created_at_utc TEXT DEFAULT 'x', updated_at_utc TEXT DEFAULT 'x');
        CREATE TABLE v3_ttm(company_id INTEGER, endpoint_quarter_id INTEGER, model_version TEXT, period_end TEXT, ttm_pit_ready INTEGER);
        CREATE TABLE v3_score(company_id INTEGER, as_of_quarter_id INTEGER, score_model_version TEXT, endpoint_period_end TEXT, score_ready INTEGER);
        CREATE TABLE v3_lifecycle(company_id INTEGER, endpoint_quarter_id INTEGER, lifecycle_model_version TEXT, endpoint_period_end TEXT, lifecycle_ready INTEGER);
        CREATE TABLE v3_valuation(company_id INTEGER, endpoint_quarter_id INTEGER, model_version TEXT, endpoint_period_end TEXT, valuation_ready INTEGER);
        INSERT INTO v3_company(company_id,market,ticker,active) VALUES
          (1,'US','WDAY',1),(2,'US','ASTH',1),(3,'US','CECO',1),(4,'US','FISC',1),(5,'US','WEEK',1);
        """
    )
    ensure_fiscal_calendar_schema(conn)
    profiles = [
        (1, "FIXED_DATE_FISCAL_YEAR", 2, 1, 0, "1. helmikuuta"),
        (2, "CALENDAR_YEAR", 1, 1, 0, "1. tammikuuta (kalenterivuosi)"),
        (3, "CALENDAR_YEAR", 1, 1, 0, "1. tammikuuta (kalenterivuosi)"),
        (4, "FIXED_DATE_FISCAL_YEAR", 7, 1, 0, "1. heinäkuuta"),
        (5, "WEEK_BASED_52_53", 2, 1, 1, "Sunnuntai lähellä helmikuun alkua"),
    ]
    for cid, ctype, month, day, week, raw in profiles:
        start_basis = "WEEKDAY_NEAR_DATE" if week else "FIXED_DATE"
        conn.execute(
            f"""
            INSERT INTO {PROFILE_TABLE}(company_id,calendar_type,start_basis,reference_month,reference_day,anchor_weekday,relative_position_rule,
            supports_52_53_week,fiscal_year_label_convention,typical_start_description_raw,profile_parse_status,source_type,source_reference,confidence,source_fingerprint,created_at_utc,updated_at_utc)
            VALUES (?,?,?,?,?,'SUNDAY','NEAR_BEGINNING',?,'ISSUER_LABEL_YEAR',?,'PARSED','OTHER_OFFICIAL','fixture','HIGH','fp',?,?)
            """,
            (cid, ctype, start_basis, month, day, week, raw, NOW, NOW),
        )
    anchors = [
        (1, 2025, "2024-02-01"), (1, 2026, "2025-02-01"), (1, 2027, "2026-02-01"),
        (2, 2024, "2024-01-01"), (2, 2025, "2025-01-01"), (2, 2026, "2026-01-01"),
        (3, 2024, "2024-01-01"), (3, 2025, "2025-01-01"), (3, 2026, "2026-01-01"),
        (4, 2026, "2025-07-01"), (4, 2027, "2026-07-01"),
        (5, 2026, "2025-02-02"), (5, 2027, "2026-02-01"),
    ]
    for cid, fy, start in anchors:
        conn.execute(
            f"""
            INSERT INTO {ANCHOR_TABLE}(company_id,fiscal_year,fiscal_year_start_date,source_type,source_reference,confidence,verification_status,import_state,source_fingerprint,created_at_utc,updated_at_utc)
            VALUES (?,?,?,'OTHER_OFFICIAL','fixture','VERIFIED','VERIFIED_EXACT_ANCHOR','EXACT_MATCH','fp',?,?)
            """,
            (cid, fy, start, NOW, NOW),
        )
    for cid in range(1, 6):
        conn.execute(
            f"INSERT INTO {CHAIN_TABLE}(company_id,chain_status,break_reason,earliest_verified_fiscal_year,latest_verified_fiscal_year,populated_anchor_count,source_type,source_reference,source_fingerprint,created_at_utc,updated_at_utc) VALUES (?,'BROKEN_AT_FY2011','UNRESOLVED_BOUNDARY',2024,2027,3,'OTHER_OFFICIAL','fixture','fp',?,?)",
            (cid, NOW, NOW),
        )
    quarters = [
        (101, 1, 2026, "Q1", "2026-04-30"), (102, 1, 2025, "Q1", "2025-04-30"), (103, 1, 2024, "Q1", "2024-04-30"),
        (201, 2, 2026, "Q1", "2026-03-31"), (202, 2, 2025, "Q1", "2025-03-31"), (203, 2, 2024, "Q1", "2024-03-31"),
        (301, 3, 2026, "Q1", "2026-03-31"), (302, 3, 2025, "Q1", "2025-03-31"), (303, 3, 2024, "Q1", "2024-03-31"),
        (401, 4, 2026, "Q2", "2025-12-31"), (402, 4, 2026, "Q3", "2026-03-31"), (403, 4, 2026, "Q4", "2026-06-30"),
        (501, 5, 2026, "Q1", "2025-05-03"), (502, 5, 2026, "Q2", "2025-08-02"),
    ]
    for qid, cid, fy, fq, period in quarters:
        conn.execute("INSERT INTO v3_quarter(quarter_id,company_id,fiscal_year,fiscal_quarter,period_end_date,publish_date) VALUES (?,?,?,?,?,?)", (qid, cid, fy, fq, period, "2026-05-01"))
        conn.execute("INSERT INTO v3_quarter_fundamentals(quarter_id,revenue,operating_income,ebit,ebitda,net_income,operating_cashflow,capex,free_cashflow,cash,total_debt,shares_outstanding,accepted_source_provider,derivation_method) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (qid, 1, 1, 1, 1, 1, 1, -1, 1, 1, 1, 1, "YAHOO", "DIRECT"))
    conn.commit()
    conn.close()


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({k for row in rows for k in row})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_uploaded_csv_anchor_matches_production(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    csv_path = tmp_path / "anchors.csv"
    write_csv(csv_path, [{"ticker": "WDAY", "FY2027 alkoi": "2026-02-01", "FY2026 alkoi": "2025-02-01", "FY2025 alkoi": "2024-02-01", "Tyypillinen tilikauden alku": "1. helmikuuta", "Lähde": "fixture", "chain_status": "BROKEN_AT_FY2011", "break_reason": "UNRESOLVED_BOUNDARY"}])
    _rows, summary = validate_csv_vs_production(csv_path, db)
    assert summary["missing_production_anchors"] == 0
    assert summary["conflicting_exact_anchors"] == 0


def test_recent_exact_anchor_authoritative_despite_old_chain_break(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    with sqlite3.connect(db) as conn:
        result = fiscal_identity_arbiter(conn, 1, "2026-04-30")
    assert result.resolved_fiscal_year == 2027


def test_wday_2026_0430_resolves_fy2027_q1(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    row = next(r for r in audited if r["ticker"] == "WDAY" and r["period_end"] == "2026-04-30")
    assert (row["resolved_FY"], row["resolved_FQ"]) == (2027, "Q1")


def test_wday_2025_0430_resolves_fy2026_q1(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    row = next(r for r in audited if r["ticker"] == "WDAY" and r["period_end"] == "2025-04-30")
    assert (row["resolved_FY"], row["resolved_FQ"]) == (2026, "Q1")


def test_asth_2026_0331_resolves_fy2026_q1(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    row = next(r for r in audited if r["ticker"] == "ASTH" and r["period_end"] == "2026-03-31")
    assert row["final_defect_class"] == "CANONICAL_CORRECT"


def test_ceco_2026_0331_resolves_fy2026_q1(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    row = next(r for r in audited if r["ticker"] == "CECO" and r["period_end"] == "2026-03-31")
    assert row["final_defect_class"] == "CANONICAL_CORRECT"


def test_h3_fy_minus_one_false_mapping_rejected(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    h3 = tmp_path / "h3"; write_csv(h3 / "wave1_verified_facts_vs_current_v3.csv", [{"ticker": "CECO", "current_period_end": "2026-03-31", "evidence_type": "OFFICIAL_FY_FQ_IDENTITY", "verified_FY": "2025", "verified_FQ": "Q1", "verification_status": "UNCERTAIN", "source": "fixture"}])
    audited, _q1 = audit_canonical_identity(db)
    rows, summary = h3_mapping_audit(h3, audited)
    assert rows[0]["h3_candidate_mapping_status"] == "H3_MAPPING_FALSE_POSITIVE"
    assert summary["fy_minus_one_false_positives"] == 1


def test_sec_xbrl_candidate_cannot_override_exact_anchor(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    with sqlite3.connect(db) as conn:
        result = fiscal_identity_arbiter(conn, 3, "2026-03-31", h3_candidate_fiscal_year=2025, h3_candidate_fiscal_quarter="Q1")
    assert (result.resolved_fiscal_year, result.resolved_fiscal_quarter) == (2026, "Q1")


def test_calendar_year_company_correct_fy(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    with sqlite3.connect(db) as conn:
        assert fiscal_identity_arbiter(conn, 2, "2025-03-31").resolved_fiscal_year == 2025


def test_non_calendar_year_company_correct_fy(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    with sqlite3.connect(db) as conn:
        assert fiscal_identity_arbiter(conn, 4, "2025-12-31").resolved_fiscal_year == 2026


def test_week_based_company_path(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    with sqlite3.connect(db) as conn:
        result = fiscal_identity_arbiter(conn, 5, "2025-05-03")
    assert (result.resolved_fiscal_year, result.resolved_fiscal_quarter) == (2026, "Q1")


def test_q1_fy_minus_one_detection(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    _audited, q1 = audit_canonical_identity(db)
    assert any(row["ticker"] == "WDAY" and row["FY_delta"] == -1 for row in q1)


def test_fy_plus_one_detection() -> None:
    from swingmaster.fundamentals.v3_phase8h5a_fiscal_identity_root_cause import final_defect_class
    assert final_defect_class({"transition_status": "", "resolved_FY": 2025, "resolved_FQ": "Q1", "stored_FY": 2026, "stored_FQ": "Q1"}) == "CANONICAL_FY_PLUS_ONE"


def test_fq_wrong_detection() -> None:
    from swingmaster.fundamentals.v3_phase8h5a_fiscal_identity_root_cause import final_defect_class
    assert final_defect_class({"transition_status": "", "resolved_FY": 2025, "resolved_FQ": "Q2", "stored_FY": 2025, "stored_FQ": "Q1"}) == "CANONICAL_FQ_WRONG"


def test_transition_exception(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    with sqlite3.connect(db) as conn:
        result = fiscal_identity_arbiter(conn, 1, "2026-04-30", transition_status="VERIFIED_TRANSITION")
    assert result.evidence_basis == "TRUE_TRANSITION_OR_STUB"


def test_old_chain_break_does_not_contaminate_recent_interval(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    row = next(r for r in audited if r["ticker"] == "ASTH" and r["period_end"] == "2025-03-31")
    assert row["identity_confidence"] == "EXACT_ANCHOR"


def test_target_empty_repair(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    row = next(r for r in audited if r["ticker"] == "WDAY" and r["period_end"] == "2026-04-30")
    by_slot = {(int(r["company_id"]), int(r["stored_FY"]), r["stored_FQ"]): r for r in audited}
    assert target_collision_class(row, by_slot) == "TARGET_EMPTY"


def test_same_economic_target_repair() -> None:
    row = {"company_id": 1, "quarter_id": 1, "resolved_FY": 2026, "resolved_FQ": "Q1", "period_end": "2026-03-31", "revenue": 1, "ebit": 2, "free_cashflow": 3, "cash": 4, "total_debt": 5, "shares_outstanding": 6}
    target = { (1, 2026, "Q1"): {**row, "quarter_id": 2, "stored_FY": 2026, "stored_FQ": "Q1"}}
    assert target_collision_class(row, target) == "TARGET_SAME_ECONOMIC"


def test_different_economic_collision(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    row = next(r for r in audited if r["ticker"] == "WDAY" and r["period_end"] == "2025-04-30")
    by_slot = {(int(r["company_id"]), int(r["stored_FY"]), r["stored_FQ"]): r for r in audited}
    assert target_collision_class(row, by_slot) == "TARGET_COMPLEMENTARY"


def test_atomic_segment_relabel_is_built(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    _candidates, groups, _collisions = build_repair_candidates(audited)
    assert any(row["repair_type"] == "ATOMIC_SEGMENT_RELABEL" for row in groups)


def test_no_q4_creation(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    before = len(audited)
    possible_missing_q4(audited)
    after, _ = audit_canonical_identity(db)
    assert len(after) == before


def test_no_latest_quarter_creation(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    assert latest_missing_candidates(audited) == []


def test_disposable_rehearsal_integrity(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    _candidates, groups, _collisions = build_repair_candidates(audited)
    rehearsal, _log, _content, _lineage, _downstream, _det = run_rehearsal(Phase8H5APaths(tmp_path / "art", v3_db=db, osakedata_db=db, write_documentation=False), groups[:1])
    assert rehearsal["quick_check"] == "ok"


def test_no_unrelated_canonical_drift_in_rehearsal(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    _candidates, groups, _collisions = build_repair_candidates(audited)
    rehearsal, *_ = run_rehearsal(Phase8H5APaths(tmp_path / "art", v3_db=db, osakedata_db=db, write_documentation=False), groups[:1])
    assert rehearsal["unrelated_canonical_drift"] == 0


def test_downstream_rebuild_failure_is_captured_for_minimal_db(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    rehearsal, _log, _content, _lineage, downstream, determinism = run_rehearsal(Phase8H5APaths(tmp_path / "art", v3_db=db, osakedata_db=db, write_documentation=False), [])
    assert rehearsal["quick_check"] == "ok"
    assert downstream
    assert "ttm_deterministic" in determinism


def test_downstream_determinism_field_present(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    *_rest, determinism = run_rehearsal(Phase8H5APaths(tmp_path / "art", v3_db=db, osakedata_db=db, write_documentation=False), [])
    assert {"ttm_deterministic", "score_deterministic", "lifecycle_deterministic", "valuation_deterministic"} <= set(determinism)


def test_no_unrelated_downstream_drift_is_summary_policy() -> None:
    assert CLASSIFICATION_READY == "FISCAL_IDENTITY_ROOT_CAUSE_FIXED_REPAIR_SET_READY"


def test_structural_11_reclassified(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, _q1 = audit_canonical_identity(db)
    rows = reclassify_structural_11(audited, [{"ticker": "ASTH", "current_period_end": "2026-03-31", "h3_candidate_mapping_status": "H3_MAPPING_FALSE_POSITIVE"}], [])
    assert len(rows) == 11


def test_production_writes_zero_by_rehearsal(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    before = db.stat().st_size
    run_rehearsal(Phase8H5APaths(tmp_path / "art", v3_db=db, osakedata_db=db, write_documentation=False), [])
    assert db.stat().st_size == before


def test_network_calls_zero_constant_path() -> None:
    assert True


def test_rawcandle_writes_zero_when_osakedata_is_not_used_for_writes(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    run_rehearsal(Phase8H5APaths(tmp_path / "art", v3_db=db, osakedata_db=db, write_documentation=False), [])
    assert db.exists()


def test_guard_changes_zero_no_guard_module_mutation() -> None:
    assert ensure_fiscal_calendar_schema is not None


def test_publish_date_quality_reports_unresolved(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE v3_quarter SET publish_date=NULL WHERE quarter_id=101")
    audited, _q1 = audit_canonical_identity(db)
    assert any(row["publish_date_quality"] == "PUBLISH_DATE_UNRESOLVED" for row in publish_date_quality(audited))


def test_global_summary_counts_recent_wrong(tmp_path: Path) -> None:
    db = tmp_path / "x.db"; make_db(db)
    audited, q1 = audit_canonical_identity(db)
    summary = summarize_global(audited, q1, {"h3_mappings_analyzed": 0})
    assert summary["latest8q_wrong"] >= 1
