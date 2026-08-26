from __future__ import annotations

import sqlite3

from swingmaster.fundamentals import v3_phase8a10a_r_remap_reconciliation as remap


def case_row(ticker: str, ready: str = "YES") -> dict[str, str]:
    return {
        "Ticker": ticker,
        "Production Ready": ready,
        "Current Fiscal Year": "2025",
        "Current Fiscal Q": "Q4",
        "Current Period End": "2026-01-31",
    }


def remap_row(ticker: str = "AAA") -> dict[str, str]:
    return {
        "Ticker": ticker,
        "Current Fiscal Year": "2025",
        "Current Fiscal Q": "Q4",
        "Current Period End": "2026-01-31",
        "Proposed Fiscal Year": "2026",
        "Proposed Fiscal Q": "Q4",
        "Proposed Period End": "2026-01-31",
        "Action": "SHIFT_MULTI_QUARTER_SEGMENT",
        "Merge Target Fiscal Year": "",
        "Merge Target Fiscal Q": "",
        "Evidence": "source",
        "Confidence": "HIGH",
        "Notes": "",
    }


def official_row(ticker: str = "AAA") -> dict[str, str]:
    return {
        "Ticker": ticker,
        "Company Name": "A",
        "Fiscal Year": "2026",
        "Fiscal Q": "Q4",
        "Official Period End": "2026-01-31",
        "Publish Date": "2026-03-01",
        "Revenue": "100",
        "Source 1": "source",
        "Source 1 Type": "10-K",
        "Source 2": "",
        "Source 2 Type": "",
        "Confidence": "HIGH",
        "Fiscal Calendar Type": "month-end",
        "Notes": "",
    }


def test_validate_external_counts_and_no_tickers() -> None:
    timeline = [official_row(str(i)) for i in range(116)]
    cases = [case_row(str(i), "YES") for i in range(12)] + [case_row(t, "NO") for t in ("FNGR", "IMMR", "RCAT")]
    remaps = []
    for i in range(75):
        row = remap_row(str(i))
        row["Current Fiscal Year"] = str(2000 + i)
        remaps.append(row)
    summary = remap.validate_inputs(timeline, cases, remaps)
    assert summary["official_timeline_rows"] == 116
    assert summary["case_resolution_rows"] == 15
    assert summary["segment_remap_rows"] == 75
    assert summary["external_ready_yes"] == 12
    assert summary["external_ready_no"] == 3
    assert summary["external_no_tickers"] == ["FNGR", "IMMR", "RCAT"]


def setup_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, company_name TEXT, active INTEGER);
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_quarter TEXT, period_end_date TEXT, publish_date TEXT);
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY, revenue REAL, gross_profit REAL, operating_income REAL, ebit REAL, ebitda REAL,
            net_income REAL, operating_cashflow REAL, capex REAL, free_cashflow REAL, cash REAL, total_debt REAL,
            shares_outstanding REAL, accepted_source_provider TEXT
        );
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, as_of_quarter_id INTEGER);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_migration_audit(audit_id INTEGER PRIMARY KEY, quarter_id INTEGER);
        INSERT INTO v3_company VALUES (1,'AAA','A',1);
        INSERT INTO v3_quarter VALUES (1,1,2025,'Q4','2026-01-31','2026-03-01');
        INSERT INTO v3_quarter VALUES (2,1,2026,'Q4','2027-01-31','2027-03-01');
        INSERT INTO v3_quarter_fundamentals VALUES (1,100,50,10,10,12,8,9,-1,8,20,5,10,'YAHOO');
        INSERT INTO v3_quarter_fundamentals VALUES (2,200,60,20,20,22,18,19,-2,18,30,6,11,'YAHOO');
        INSERT INTO v3_ttm VALUES (1,1);
        """
    )
    return conn


def test_current_v3_row_join_deterministic() -> None:
    row = remap.current_row(setup_db(), remap_row())
    assert row is not None
    assert row["quarter_id"] == 1


def test_economic_content_matching_deterministic() -> None:
    analysis = remap.reconcile_remaps(setup_db(), [remap_row()], {("AAA", "2026", "Q4"): official_row()}, {"AAA": [case_row("AAA")]})
    assert analysis["content"][0]["content_match_confidence"] == "ECONOMIC_QUARTER_MATCH_HIGH"


def test_same_and_conflicting_economic_quarter_collision_detected() -> None:
    conn = setup_db()
    target = remap.target_identity_row(conn, "AAA", "2026", "Q4")
    source = remap.current_row(conn, remap_row())
    assert remap.field_conflicts(source, target)[0] == "CONFLICT"
    assert remap.field_conflicts(source, None)[0] == "TARGET_EMPTY"


def test_null_non_null_merge_classification() -> None:
    source = {"revenue": 100.0, "publish_date": None, "period_end": "2026-01-31"}
    target = {"revenue": None, "publish_date": "2026-03-01", "period_end": "2026-01-31"}
    assert remap.field_conflicts(source, target)[0] == "PARTIAL_COMPLEMENT"


def test_non_null_conflict_blocks_readiness_outside_rotation() -> None:
    analysis = remap.reconcile_remaps(setup_db(), [remap_row()], {("AAA", "2026", "Q4"): official_row()}, {"AAA": [case_row("AAA")]})
    summary = remap.group_readiness([case_row("AAA")], [remap_row()], analysis)
    assert summary[0]["v3_local_classification"] == "V3_REPAIRABLE_WITH_BOUNDED_CONFLICT_RESOLUTION"


def test_target_identity_inside_segment_is_rotation_even_with_different_period_end() -> None:
    second = remap_row()
    second["Current Fiscal Year"] = "2026"
    second["Current Period End"] = "2027-01-31"
    second["Proposed Fiscal Year"] = "2027"
    second["Proposed Period End"] = "2027-01-31"
    official_2027 = official_row()
    official_2027["Fiscal Year"] = "2027"
    official_2027["Official Period End"] = "2027-01-31"
    official_2027["Publish Date"] = "2027-03-01"
    official_2027["Revenue"] = "200"
    analysis = remap.reconcile_remaps(
        setup_db(),
        [remap_row(), second],
        {("AAA", "2026", "Q4"): official_row(), ("AAA", "2027", "Q4"): official_2027},
        {"AAA": [case_row("AAA")]},
    )
    first_collision = analysis["target_collisions"][0]
    assert first_collision["target_identity_collision"] == "TARGET_EXISTS_DIFFERENT_ECONOMIC_QUARTER"
    assert first_collision["target_in_same_rotation_group"] == 1
    assert first_collision["transformation_shape"] == "MULTI_ROW_ROTATION"
    summary = remap.group_readiness([case_row("AAA")], [remap_row(), second], analysis)
    assert summary[0]["v3_local_classification"] == "V3_PRODUCTION_READY"


def test_multi_row_rotation_and_52_53_week_preserved() -> None:
    row = remap_row("CRUS")
    row["Proposed Period End"] = "2026-03-28"
    assert row["Action"] == "SHIFT_MULTI_QUARTER_SEGMENT"
    assert row["Proposed Period End"].endswith("03-28")


def test_special_cases_not_ready() -> None:
    analysis = {"content": [], "target_collisions": [], "non_null_conflicts": [], "ownership": []}
    summary = remap.group_readiness([case_row("FNGR", "NO")], [], analysis)
    assert summary == []


def test_transition_and_restatement_cases_block_label_only_repair() -> None:
    assert "RCAT" in remap.SPECIAL_NO_TICKERS
    assert "IMMR" in remap.SPECIAL_NO_TICKERS


def test_atomic_group_has_two_phase_operations() -> None:
    analysis = remap.reconcile_remaps(setup_db(), [remap_row()], {("AAA", "2026", "Q4"): official_row()}, {"AAA": [case_row("AAA")]})
    group = [{"ticker": "AAA", "final_production_ready": "YES"}]
    ops = remap.atomic_transformations([remap_row()], analysis, group, {("AAA", "2026", "Q4"): official_row()})
    assert [op["operation"] for op in ops] == ["CREATE_TEMP_IDENTITY", "FINALIZE_IDENTITY"]


def test_no_production_or_derived_or_rawcandle_writes_contract() -> None:
    summary = {
        "production_writes": 0,
        "rawcandle_writes": 0,
        "derived_writes": {"ttm": 0, "score": 0, "lifecycle": 0, "valuation": 0},
    }
    assert summary["production_writes"] == 0
    assert summary["rawcandle_writes"] == 0
    assert all(value == 0 for value in summary["derived_writes"].values())
