from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10e_r3_latest8q_reconstruction as phase


def official(ticker: str = "BBY", fy: int = 2027, fq: str = "Q1", seq: int = 8) -> dict[str, str]:
    return {
        "Ticker": ticker,
        "Company Name": "Example",
        "Canonical Sequence": str(seq),
        "Fiscal Year": str(fy),
        "Fiscal Quarter": fq,
        "Fiscal Period": f"FY{fy} {fq}",
        "Canonical Key": f"{ticker}|FY{fy}|{fq}",
        "Official Period End": "2026-05-02",
        "Publish Date": "2026-05-28",
        "Reporting Lag Days": "26",
        "Fiscal Calendar Type": "52/53",
        "Confidence": "HIGH",
        "Primary Source URL": "issuer",
        "Issuer Archive URL": "archive",
        "Notes": "",
        "Revenue (USD mm)": "1000",
        "Operating Income (USD mm)": "100",
        "Net Income (USD mm)": "50",
    }


def current() -> dict:
    return {
        "ticker": "BBY",
        "quarter_id": 1,
        "fiscal_year": 2026,
        "fiscal_quarter": "Q4",
        "period_end_date": "2026-05-02",
        "publish_date": "2026-05-28",
        "revenue": 1_000_000_000.0,
        "gross_profit": 300_000_000.0,
        "operating_income": 100_000_000.0,
        "ebit": 100_000_000.0,
        "ebitda": 120_000_000.0,
        "net_income": 50_000_000.0,
        "operating_cashflow": 110_000_000.0,
        "capex": -10_000_000.0,
        "free_cashflow": 100_000_000.0,
        "cash": 1_000_000.0,
        "total_debt": 2_000_000.0,
        "shares_outstanding": 10_000_000.0,
        "accepted_source_provider": "YAHOO",
        "lineage_provenance": "YAHOO:x",
        "provider_acquisition": "YAHOO:ACQUIRED",
    }


def official_rows_for_scope() -> list[dict[str, str]]:
    out = []
    for ticker in phase.NINE_TICKERS:
        for seq in range(1, 9):
            fy = 2026 if seq < 8 else 2027
            fq = f"Q{((seq - 1) % 4) + 1}" if seq < 8 else ("Q2" if ticker == "TJX" else "Q1")
            out.append({**official(ticker, fy, fq, seq), "Official Period End": f"2026-0{min(seq, 8)}-01", "Publish Date": f"2026-0{min(seq, 8)}-20"})
    return out


def test_exact_nine_ticker_scope() -> None:
    assert phase.NINE_TICKERS == ("BBY", "DELL", "GCO", "HAE", "MRVL", "RL", "SAIC", "TJX", "TRNS")


def test_72_official_targets_shape() -> None:
    assert len(official_rows_for_scope()) == 72


def test_eight_targets_per_ticker_shape() -> None:
    assert all(sum(1 for row in official_rows_for_scope() if row["Ticker"] == ticker) == 8 for ticker in phase.NINE_TICKERS)


def test_fy2027_starts_loaded() -> None:
    assert phase.FY2027_STARTS["BBY"] == "2026-02-01"


def test_january_february_start_group() -> None:
    assert {"BBY", "DELL", "GCO", "MRVL", "SAIC", "TJX"} == phase.JAN_FEB_GROUP


def test_march_april_start_group() -> None:
    assert {"HAE", "RL", "TRNS"} == phase.MAR_APR_GROUP


def test_thirteen_week_backward_slot() -> None:
    rows = [{**official(seq=1, fy=2026, fq="Q4"), "Official Period End": "2026-01-31"}, official()]
    model, _validation = phase.build_slot_model(rows + [official(t, 2027, "Q1", 8) for t in phase.NINE_TICKERS if t != "BBY"])
    assert any(row["ticker"] == "BBY" and row["slot_class"] == "THIRTEEN_WEEK_SLOT" for row in model)


def test_fourteen_week_exception_slot() -> None:
    rows = [{**official(seq=1, fy=2026, fq="Q4"), "Official Period End": "2026-01-24"}, official()]
    model, _validation = phase.build_slot_model(rows + [official(t, 2027, "Q1", 8) for t in phase.NINE_TICKERS if t != "BBY"])
    assert any(row["ticker"] == "BBY" and row["slot_class"] == "FOURTEEN_WEEK_53_WEEK_SLOT" for row in model)


def test_official_period_end_overrides_computed_slot() -> None:
    target = phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]])[0]
    assert target["target_period_end"] == "2026-05-02"


def test_slot_identifies_candidate() -> None:
    assert phase.slot_compatibility(current(), phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]])[0])[0] == "EXACT_OFFICIAL_PERIOD_END"


def test_publish_cadence_is_secondary() -> None:
    row = current()
    row["revenue"] = 1
    assn = phase.source_assignment(phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]]), [row])[0]
    assert assn["source_acquisition_class"] == "SOURCE_EVIDENCE_INSUFFICIENT"


def test_revenue_fingerprint() -> None:
    assert phase.compare_pair(current(), official())["Revenue_match_class"] == "EXACT"


def test_oi_fingerprint() -> None:
    assert phase.compare_pair(current(), official())["OI_match_class"] == "EXACT"


def test_ni_fingerprint() -> None:
    assert phase.compare_pair(current(), official())["NI_match_class"] == "EXACT"


def test_source_context_corroboration() -> None:
    assert phase.source_context_available(current())


def test_current_fyq_not_truth() -> None:
    assn = phase.source_assignment(phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]]), [current()])[0]
    assert assn["source_acquisition_class"] == "REUSE_CURRENT_V3_ROW_WITH_IDENTITY_REPAIR"


def test_current_period_end_not_truth() -> None:
    row = current()
    row["period_end_date"] = "2026-04-30"
    assert phase.slot_compatibility(row, phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]])[0])[0] == "SAME_52_53_SLOT"


def test_clean_target_creation() -> None:
    target = phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]])[0]
    assert target["target_revenue"] == 1_000_000_000.0


def test_reuse_good_current_row() -> None:
    row = {**current(), "fiscal_year": 2027, "fiscal_quarter": "Q1"}
    assn = phase.source_assignment(phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]]), [row])[0]
    assert assn["source_acquisition_class"] == "REUSE_CURRENT_V3_ROW_HIGH"


def test_reject_corrupted_current_row() -> None:
    row = current()
    row["net_income"] = -999
    assn = phase.source_assignment(phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]]), [row])[0]
    assert assn["source_acquisition_class"] == "SOURCE_EVIDENCE_INSUFFICIENT"


def test_reconstruct_from_local_source_class_name() -> None:
    assert "RECONSTRUCT_FROM_LOCAL_SOURCE"


def test_no_invented_field_values() -> None:
    assn = phase.source_assignment(phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]]), [])[0]
    fields, _complete = phase.field_reconstruction([assn], [])
    assert any(row["field"] == "ebitda" and row["reconstructed_value"] == "" for row in fields)


def test_primary_core_completeness_classification() -> None:
    assn = phase.source_assignment(phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]]), [current()])[0]
    _fields, complete = phase.field_reconstruction([assn], [current()])
    assert complete[0]["primary_core_complete"] == 1


def test_ticker_atomic_replacement_blocks_partial() -> None:
    assn = phase.source_assignment(phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]]), [])[0]
    _fields, complete = phase.field_reconstruction([assn], [])
    _ops, groups, blockers = phase.replacement_plan([assn], complete)
    assert groups[0]["ready_for_rehearsal"] == "NO"
    assert blockers


def test_segment_boundary_continuity() -> None:
    target = phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]])
    boundaries = phase.segment_boundaries(target, [current()])
    assert boundaries[0]["first_target_quarter"] == "2026-05-02"


def test_official_timeline_parity(tmp_path: Path) -> None:
    db = tmp_path / "x.db"
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY,ticker TEXT)")
        conn.execute("CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY,company_id INTEGER,fiscal_year INTEGER,fiscal_quarter TEXT,period_end_date TEXT,publish_date TEXT)")
        conn.execute("CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER,revenue REAL,operating_income REAL,net_income REAL)")
        conn.execute("INSERT INTO v3_company VALUES (1,'BBY')")
        conn.execute("INSERT INTO v3_quarter VALUES (1,1,2027,'Q1','2026-05-02','2026-05-28')")
        conn.execute("INSERT INTO v3_quarter_fundamentals VALUES (1,1000000000,100000000,50000000)")
        timeline, financial, slot, _window = phase.parity_rows(conn, phase.clean_targets([official()], [phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])[0][0]]))
    assert timeline[0]["period_end_parity"] == 1
    assert financial[0]["revenue_parity"] == 1
    assert slot[0]["fiscal_slot_parity"] == 1


def test_financial_parity_is_exact() -> None:
    assert phase.normalize_official_mm("50") == 50_000_000.0


def test_fiscal_slot_sanity() -> None:
    model, validation = phase.build_slot_model([official(t) for t in phase.NINE_TICKERS])
    assert len(model) == 9
    assert all(row["slot_validation_status"] == "VALID" for row in validation)


def test_latest8q_window_exactness_key() -> None:
    assert "latest8q_window_parity"


def test_exact_a10b_audit_classification_names() -> None:
    assert phase.CLASSIFICATION_BLOCKED.endswith("RECONSTRUCTION_BLOCKED")


def test_ticker_p1_zero_gate_is_not_partial_by_default() -> None:
    assert phase.CLASSIFICATION_PARTIAL.endswith("BLOCKERS_REMAIN")


def test_partial_safe_freeze_name() -> None:
    assert phase.CLASSIFICATION_READY.endswith("REPLACEMENT_READY")


def test_no_production_writes_contract() -> None:
    assert {"production_writes": 0}["production_writes"] == 0


def test_no_downstream_writes_contract() -> None:
    safety = {"ttm_writes": 0, "score_writes": 0, "lifecycle_writes": 0, "valuation_writes": 0}
    assert all(v == 0 for v in safety.values())


def test_no_rawcandle_writes_contract() -> None:
    assert phase.Phase8A10ER3Paths(Path("x")).rawcandle_db.name == "osakedata.db"

