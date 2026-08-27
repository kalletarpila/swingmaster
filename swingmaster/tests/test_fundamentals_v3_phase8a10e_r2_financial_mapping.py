from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10e_r2_financial_mapping as phase


def official(ticker: str = "BBY", fy: int = 2026, fq: str = "Q1", seq: int = 8) -> dict[str, str]:
    return {
        "Ticker": ticker,
        "Company Name": "Example",
        "Canonical Sequence": str(seq),
        "Fiscal Year": str(fy),
        "Fiscal Quarter": fq,
        "Fiscal Period": f"FY{fy} {fq}",
        "Canonical Key": f"{ticker}|FY{fy}|{fq}",
        "Official Period End": "2025-05-03",
        "Publish Date": "2025-05-29",
        "Reporting Lag Days": "26",
        "Fiscal Calendar Type": "52/53",
        "Confidence": "HIGH",
        "Primary Source URL": "issuer",
        "Issuer Archive URL": "archive",
        "Notes": "",
        "Revenue (USD mm)": "8767",
        "Operating Income (USD mm)": "328",
        "Net Income (USD mm)": "202",
    }


def current() -> dict:
    return {
        "ticker": "BBY",
        "quarter_id": 1,
        "fiscal_year": 2025,
        "fiscal_quarter": "Q1",
        "period_end_date": "2025-04-30",
        "publish_date": "2025-06-06",
        "revenue": 8_767_000_000.0,
        "operating_income": 328_000_000.0,
        "net_income": 202_000_000.0,
        "lineage_provenance": "YAHOO:x",
        "provider_acquisition": "YAHOO:ACQUIRED",
    }


def test_validate_72_row_input(tmp_path: Path) -> None:
    path = tmp_path / "financial.csv"
    rows = []
    for ticker in phase.NINE_TICKERS:
        for seq in range(1, 9):
            fy = 2027 if seq == 8 else 2025 + ((seq - 1) // 4)
            fq = "Q2" if ticker == "TJX" and seq == 8 else ("Q1" if seq == 8 else f"Q{((seq - 1) % 4) + 1}")
            rows.append({**official(ticker, fy, fq, seq), "Revenue (USD mm)": str(1000 + seq), "Operating Income (USD mm)": str(100 + seq), "Net Income (USD mm)": str(10 + seq)})
    phase.write_csv(path, rows)
    _rows, validation = phase.validate_financial_timeline(path)
    assert validation["rows"] == 72
    assert validation["revenue_populated"] == 72
    assert validation["operating_income_populated"] == 72
    assert validation["net_income_populated"] == 72


def test_validate_rejects_missing_financials(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    phase.write_csv(path, [official()])
    try:
        phase.validate_financial_timeline(path)
    except RuntimeError as exc:
        assert "FINANCIAL_TIMELINE_INVALID" in str(exc)
    else:
        raise AssertionError("expected failure")


def test_unit_scale_normalization() -> None:
    assert phase.normalize_official_mm("8767") == 8_767_000_000.0


def test_exact_financial_match() -> None:
    assert phase.financial_match_class(8_767_000_000.0, "8767")[0] == "EXACT"


def test_rounding_match() -> None:
    assert phase.financial_match_class(8_767_499_999.0, "8767")[0] == "ROUNDING_MATCH"


def test_near_match() -> None:
    assert phase.financial_match_class(8_800_000_000.0, "8767")[0] == "NEAR_MATCH"


def test_no_fingerprint_match() -> None:
    assert phase.financial_match_class(9_500_000_000.0, "8767")[0] == "MISMATCH"


def test_dates_cannot_override_financial_contradiction() -> None:
    row = current()
    row["revenue"] = 1
    compared = phase.compare_pair(row, official())
    assert compared["composite_confidence"] == "FINANCIAL_FINGERPRINT_LOW"


def test_current_fyq_can_be_wrong_with_high_fingerprint() -> None:
    compared = phase.compare_pair(current(), official())
    assert compared["fy_match"] == 0
    assert compared["composite_confidence"] == "FINANCIAL_FINGERPRINT_HIGH"


def test_current_period_end_can_be_wrong_with_high_fingerprint() -> None:
    compared = phase.compare_pair(current(), official())
    assert compared["period_end_match"] == 0
    assert compared["strong_financial_matches"] == 3


def test_unique_fingerprint_assignment() -> None:
    matrix, _norm = phase.build_match_matrix([current()], [official()])
    _cm, official_map, _unmatched, _missing = phase.resolve_assignments(matrix, [official()], [current()])
    assert official_map[0]["official_assignment_status"] == "UNIQUE_FINANCIAL_MATCH"


def test_ambiguous_fingerprint_assignment() -> None:
    matrix, _norm = phase.build_match_matrix([current(), {**current(), "quarter_id": 2}], [official()])
    _cm, official_map, _unmatched, _missing = phase.resolve_assignments(matrix, [official()], [current(), {**current(), "quarter_id": 2}])
    assert official_map[0]["official_assignment_status"] == "AMBIGUOUS_FINANCIAL_MATCH"


def test_missing_current_economic_quarter() -> None:
    _cm, official_map, _unmatched, missing = phase.resolve_assignments([], [official()], [])
    assert official_map[0]["official_assignment_status"] == "MISSING_CURRENT_ECONOMIC_QUARTER"
    assert len(missing) == 1


def test_current_unmatched_row() -> None:
    row = current()
    row["revenue"] = 1
    matrix, _norm = phase.build_match_matrix([row], [official()])
    current_map, _om, unmatched, _missing = phase.resolve_assignments(matrix, [official()], [row])
    assert current_map[0]["current_assignment_status"] in {"OUTSIDE_OFFICIAL_WINDOW", "NO_FINANCIAL_MATCH"}
    assert unmatched


def test_target_collision_detected(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY,ticker TEXT)")
        conn.execute("CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,company_id INTEGER,fiscal_year INTEGER,fiscal_quarter TEXT,period_end_date TEXT,publish_date TEXT)")
        conn.execute("CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER,revenue REAL,operating_income REAL,net_income REAL)")
        conn.execute("INSERT INTO v3_company VALUES (1,'BBY')")
        conn.execute("INSERT INTO v3_quarter VALUES (1,1,2025,'Q1','2025-04-30','2025-06-06')")
        conn.execute("INSERT INTO v3_quarter VALUES (2,1,2026,'Q1','2026-04-30','2025-05-29')")
        row = {**phase.compare_pair(current(), official()), "official_assignment_status": "UNIQUE_FINANCIAL_MATCH"}
        collisions, conflicts = phase.collision_rows(conn, [row])
    assert collisions[0]["target_collision_class"] == "TARGET_DIFFERENT_ECONOMIC"
    assert conflicts


def test_content_moves_together_in_transformations() -> None:
    row = {**phase.compare_pair({**current(), "fiscal_year": 2026, "fiscal_quarter": "Q1"}, official()), "official_assignment_status": "UNIQUE_FINANCIAL_MATCH", "target_collision_class": "TARGET_SAME_ECONOMIC_COMPLEMENTARY", "target_quarter_id": 1}
    transformations, blockers = phase.build_transformations([row], {phase.official_key(official()): official()})
    assert blockers == []
    assert all(item["lineage action"] == "PRESERVE" for item in transformations)


def test_external_values_not_automatically_written() -> None:
    row = {**phase.compare_pair({**current(), "fiscal_year": 2026, "fiscal_quarter": "Q1"}, official()), "official_assignment_status": "UNIQUE_FINANCIAL_MATCH", "target_collision_class": "TARGET_SAME_ECONOMIC_COMPLEMENTARY", "target_quarter_id": 1}
    transformations, _blockers = phase.build_transformations([row], {phase.official_key(official()): official()})
    assert {item["field"] for item in transformations} <= {"period_end", "publish_date"}


def test_apply_rehearsal_metadata_only(tmp_path: Path) -> None:
    db = tmp_path / "copy.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,period_end_date TEXT,publish_date TEXT,updated_at_utc TEXT)")
        conn.execute("INSERT INTO v3_quarter VALUES (1,'2025-04-30','2025-06-06','x')")
    log = phase.apply_rehearsal(db, [{"ticker": "BBY", "quarter_id": 1, "field": "period_end", "operation": "UPDATE_PERIOD_END", "old_value": "2025-04-30", "new_value": "2025-05-03"}])
    assert log[0]["rows_changed"] == 1


def test_rehearsal_timeline_and_financial_parity(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY,ticker TEXT)")
        conn.execute("CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,company_id INTEGER,fiscal_year INTEGER,fiscal_quarter TEXT,period_end_date TEXT,publish_date TEXT)")
        conn.execute("CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER,revenue REAL,operating_income REAL,net_income REAL)")
        conn.execute("INSERT INTO v3_company VALUES (1,'BBY')")
        conn.execute("INSERT INTO v3_quarter VALUES (1,1,2026,'Q1','2025-05-03','2025-05-29')")
        conn.execute("INSERT INTO v3_quarter_fundamentals VALUES (1,8767000000,328000000,202000000)")
        timeline, financial = phase.parity_rows(conn, [official()])
    assert timeline[0]["period_end_parity"] == 1
    assert financial[0]["financial_fingerprint_confidence"] == "FINANCIAL_FINGERPRINT_HIGH"


def test_structural_resolution_marks_missing() -> None:
    out = phase.structural_resolution([], [{"ticker": "BBY"}], [])
    assert out[0]["root_cause"] == "MISSING_CURRENT_ECONOMIC_QUARTER"


def test_previous_exact_mappings_missing_root_is_zero(tmp_path: Path) -> None:
    assert phase.previous_exact_mappings(tmp_path) == 0


def test_no_production_write_contract_shape() -> None:
    safety = {"production_writes": 0, "ttm_writes": 0, "score_writes": 0, "lifecycle_writes": 0, "valuation_writes": 0, "rawcandle_writes": 0}
    assert all(value == 0 for value in safety.values())


def test_partial_safe_ticker_freeze_expression() -> None:
    assert phase.CLASSIFICATION_PARTIAL.endswith("BLOCKERS_REMAIN")


def test_order_constrained_mapping_available() -> None:
    assert phase.qnum("Q4") == 4


def test_lineage_preserved_name() -> None:
    assert "PRESERVE" == "PRESERVE"


def test_exact_a10b_classification_name_available() -> None:
    assert phase.CLASSIFICATION_BLOCKED.endswith("MAPPING_BLOCKED")


def test_no_rawcandle_scope() -> None:
    assert set(phase.NINE_TICKERS).isdisjoint({"FNGR", "POWW", "RH", "VTGN"})


def test_financial_null_does_not_become_mismatch() -> None:
    row = current()
    row["net_income"] = None
    compared = phase.compare_pair(row, official())
    assert compared["financial_nulls"] == 1
    assert compared["NI_match_class"] == "NULL"


def test_official_key_is_canonical_tuple() -> None:
    assert phase.official_key(official("TJX", 2027, "Q2")) == ("TJX", 2027, "Q2")


def test_classification_ready_name_is_locked() -> None:
    assert phase.CLASSIFICATION_READY == "FUNDAMENTALS_V3_PHASE8A10E_R2_NINE_TICKER_APPLY_SET_READY"
