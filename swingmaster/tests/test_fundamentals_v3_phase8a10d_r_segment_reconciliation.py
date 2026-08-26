from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from swingmaster.fundamentals import v3_phase8a10d_r_segment_reconciliation as rec


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def segment_row(ticker: str, fy: int, fq: str, period: str, publish: str, qid: int) -> dict:
    return {
        "company_id": 1,
        "ticker": ticker,
        "quarter_id": qid,
        "fiscal_year": fy,
        "fiscal_quarter": fq,
        "period_end_date": period,
        "publish_date": publish,
        "market_availability_date": publish,
        "sec_confirmation_state": "CONFIRMED",
        "revenue": 100.0,
        "gross_profit": 50.0,
        "operating_income": 10.0,
        "ebit": 10.0,
        "ebitda": 12.0,
        "net_income": 7.0,
        "operating_cashflow": 8.0,
        "capex": -1.0,
        "free_cashflow": 7.0,
        "cash": 20.0,
        "total_debt": 5.0,
        "shares_outstanding": 1000.0,
        "accepted_source_provider": "SEC",
        "derivation_method": "SOURCE",
        "resolution_issue_id": "",
        "lineage_refs": 0,
    }


def make_db(path: Path) -> Path:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, market TEXT, ticker TEXT, company_name TEXT, active INTEGER);
            CREATE TABLE v3_quarter(
                quarter_id INTEGER PRIMARY KEY,
                company_id INTEGER,
                fiscal_year INTEGER,
                fiscal_quarter TEXT,
                period_end_date TEXT,
                publish_date TEXT,
                market_availability_date TEXT,
                q_lifecycle TEXT,
                sec_confirmation_state TEXT,
                created_at_utc TEXT,
                updated_at_utc TEXT
            );
            CREATE TABLE v3_quarter_fundamentals(
                quarter_id INTEGER PRIMARY KEY,
                revenue REAL,
                gross_profit REAL,
                operating_income REAL,
                ebit REAL,
                ebitda REAL,
                net_income REAL,
                operating_cashflow REAL,
                capex REAL,
                free_cashflow REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL,
                accepted_source_provider TEXT,
                accepted_at_utc TEXT,
                update_run_id TEXT,
                derivation_method TEXT,
                resolution_issue_id TEXT,
                created_at_utc TEXT,
                updated_at_utc TEXT
            );
            CREATE TABLE v3_migration_audit(audit_id INTEGER PRIMARY KEY, quarter_id INTEGER);
            CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER);
            CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, company_id INTEGER, as_of_quarter_id INTEGER);
            CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER);
            CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER);
            INSERT INTO v3_company VALUES (1,'usa','FNGR','FNGR',1),(2,'usa','BBY','BBY',1),(3,'usa','RH','RH',1);
            INSERT INTO v3_quarter VALUES
                (10,1,2023,'Q3','2023-11-30','2024-01-16','2024-01-16','OPERATIONALLY_SETTLED','CONFIRMED','x','x'),
                (11,1,2022,'Q3','2022-11-30','2022-12-17','2022-12-17','OPERATIONALLY_SETTLED','CONFIRMED','x','x'),
                (12,2,2026,'Q1','2026-04-30','2025-05-29','2025-05-29','OPERATIONALLY_SETTLED','CONFIRMED','x','x'),
                (13,3,2022,'Q1','2021-07-31','2021-09-08','2021-09-08','OPERATIONALLY_SETTLED','CONFIRMED','x','x'),
                (14,3,2021,'Q2','2021-05-01','2021-06-09','2021-06-09','OPERATIONALLY_SETTLED','CONFIRMED','x','x');
            INSERT INTO v3_quarter_fundamentals(quarter_id,revenue,gross_profit,operating_income,ebit,ebitda,net_income,operating_cashflow,capex,free_cashflow,cash,total_debt,shares_outstanding,accepted_source_provider,accepted_at_utc,update_run_id,derivation_method,resolution_issue_id,created_at_utc,updated_at_utc)
            VALUES
                (10,1,2,3,4,5,6,7,8,9,10,11,12,'SEC','x','x','SOURCE','','x','x'),
                (11,1,2,3,4,5,6,7,8,9,10,11,12,'SEC','x','x','SOURCE','','x','x'),
                (12,1,2,3,4,5,6,7,8,9,10,11,12,'YAHOO','x','x','SOURCE','','x','x'),
                (13,100,2,3,4,5,6,7,8,9,10,11,12,'SEC','x','x','SOURCE','','x','x'),
                (14,200,2,30,4,5,60,7,8,9,10,11,12,'SEC','x','x','SOURCE','','x','x');
            """
        )
    return path


@pytest.mark.parametrize("ticker", sorted(rec.EXPECTED_TICKERS))
def test_expected_global_p1_ticker_scope_is_locked(ticker: str) -> None:
    assert ticker in rec.EXPECTED_TICKERS


@pytest.mark.parametrize(
    ("fq", "expected"),
    [("Q1", 1), ("Q2", 2), ("Q3", 3), ("Q4", 4), ("FY", 0)],
)
def test_qnum_mapping(fq: str, expected: int) -> None:
    assert rec.qnum(fq) == expected


def test_ordinal_orders_fiscal_quarters() -> None:
    assert rec.ordinal_from_parts(2026, "Q1") < rec.ordinal_from_parts(2026, "Q2")


def test_relevant_segment_includes_four_before_and_four_after() -> None:
    data = [segment_row("BBY", 2024 + i // 4, f"Q{(i % 4) + 1}", f"202{i}-01-31", f"202{i}-03-01", i) for i in range(12)]
    p1 = [{"fiscal_year": "2025", "fiscal_quarter": "Q3"}]
    segment = rec.relevant_segment(data, p1)
    assert len(segment) == 9


def test_relevant_segment_returns_all_when_anchor_missing() -> None:
    data = [segment_row("BBY", 2025, "Q1", "2025-03-31", "2025-05-01", 1)]
    assert rec.relevant_segment(data, [{"fiscal_year": "2030", "fiscal_quarter": "Q1"}]) == data


def test_period_order_mismatch_detected() -> None:
    data = [segment_row("BBY", 2025, "Q1", "2025-06-30", "2025-08-01", 1), segment_row("BBY", 2025, "Q2", "2025-03-31", "2025-05-01", 2)]
    assert rec.period_order_mismatches(data) == 1


def test_period_order_mismatch_ignores_missing_dates() -> None:
    data = [segment_row("BBY", 2025, "Q1", "", "2025-08-01", 1), segment_row("BBY", 2025, "Q2", "2025-03-31", "2025-05-01", 2)]
    assert rec.period_order_mismatches(data) == 0


def test_align_rows_high_match_by_identity_and_dates() -> None:
    row = segment_row("BBY", 2026, "Q1", "2025-05-03", "2025-05-29", 1)
    official = [{"Fiscal Year": "2026", "Fiscal Q": "Q1", "Official Period End": "2025-05-03", "Publish Date": "2025-05-29", "Revenue": "100", "Operating Income": "10", "Net Income": "7"}]
    assert rec.align_rows([row], official)[0]["alignment_status"] == "ECONOMIC_MATCH_HIGH"


def test_align_rows_medium_match_by_period_only() -> None:
    row = segment_row("BBY", 2026, "Q1", "2025-05-03", "2025-01-01", 1)
    official = [{"Fiscal Year": "2026", "Fiscal Q": "Q1", "Official Period End": "2025-05-03", "Publish Date": "2025-05-29", "Revenue": "", "Operating Income": "", "Net Income": ""}]
    assert rec.align_rows([row], official)[0]["alignment_status"] == "ECONOMIC_MATCH_MEDIUM"


def test_align_rows_low_match_by_identity_only() -> None:
    row = segment_row("BBY", 2026, "Q1", "2026-04-30", "2025-01-01", 1)
    official = [{"Fiscal Year": "2026", "Fiscal Q": "Q1", "Official Period End": "2025-05-03", "Publish Date": "2025-05-29", "Revenue": "", "Operating Income": "", "Net Income": ""}]
    assert rec.align_rows([row], official)[0]["alignment_status"] == "ECONOMIC_MATCH_LOW"


def test_align_rows_no_match_when_no_identity_or_period() -> None:
    row = segment_row("BBY", 2026, "Q1", "2026-04-30", "2025-01-01", 1)
    assert rec.align_rows([row], [])[0]["alignment_status"] == "NO_MATCH"


def test_first_divergence_identifies_first_wrong_row() -> None:
    alignment = [
        {"ticker": "BBY", "quarter_id": 1, "current_fiscal_year": 2025, "current_fiscal_quarter": "Q4", "current_period_end": "2025-02-01", "current_publish_date": "2025-03-04", "official_fiscal_year": "2025", "official_fiscal_quarter": "Q4", "official_period_end": "2025-02-01", "official_publish_date": "2025-03-04", "alignment_status": "ECONOMIC_MATCH_HIGH"},
        {"ticker": "BBY", "quarter_id": 2, "current_fiscal_year": 2026, "current_fiscal_quarter": "Q1", "current_period_end": "2026-04-30", "current_publish_date": "2025-05-29", "official_fiscal_year": "2026", "official_fiscal_quarter": "Q1", "official_period_end": "2025-05-03", "official_publish_date": "2025-05-29", "alignment_status": "ECONOMIC_MATCH_HIGH"},
    ]
    assert rec.first_divergence(alignment)["first_incorrect_quarter_id"] == 2


def test_first_divergence_reports_clean_segment() -> None:
    alignment = [{"ticker": "BBY", "quarter_id": 1, "current_fiscal_year": 2025, "current_fiscal_quarter": "Q4", "current_period_end": "2025-02-01", "current_publish_date": "2025-03-04", "official_fiscal_year": "2025", "official_fiscal_quarter": "Q4", "official_period_end": "2025-02-01", "official_publish_date": "2025-03-04", "alignment_status": "ECONOMIC_MATCH_HIGH"}]
    assert rec.first_divergence(alignment)["first_divergence"] == ""


def test_compare_conflicts_detects_conflicting_non_null_values() -> None:
    conflicts = rec.compare_conflicts({"period_end_date": "2025-01-01"}, {"period_end_date": "2025-02-01"})
    assert conflicts[0]["comparison"] == "CONFLICT"


def test_compare_conflicts_detects_null_vs_value() -> None:
    conflicts = rec.compare_conflicts({"period_end_date": None}, {"period_end_date": "2025-02-01"})
    assert conflicts[0]["comparison"] == "NULL_VS_VALUE"


def test_compare_conflicts_detects_same_values() -> None:
    conflicts = rec.compare_conflicts({"period_end_date": "2025-02-01"}, {"period_end_date": "2025-02-01"})
    assert conflicts[0]["comparison"] == "SAME"


def test_compare_conflicts_empty_without_source_or_target() -> None:
    assert rec.compare_conflicts(None, {"period_end_date": "2025-02-01"}) == []


def test_root_cause_fngr_preserves_segment_shift_classification() -> None:
    assert rec.root_cause_for_ticker("FNGR", [], "TARGET_ABSENT", {"FNGR"}) == ("MULTI_QUARTER_SEGMENT_SHIFT", "MIXED_STRUCTURAL_AND_VALUE_REPAIR")


def test_root_cause_rh_collision_blocks_apply() -> None:
    assert rec.root_cause_for_ticker("RH", [], "DIFFERENT_ECONOMIC_QUARTER", set()) == ("DUPLICATE_ECONOMIC_QUARTER", "NO_SAFE_REPAIR_YET")


def test_root_cause_period_shift_when_post_rehearsal_still_p1() -> None:
    plan = [{"Operation": "UPDATE_PERIOD_END", "Field": "period_end"}]
    assert rec.root_cause_for_ticker("BBY", plan, "TARGET_ABSENT", {"BBY"}) == ("ONE_YEAR_PERIOD_SHIFT", "MULTI_ROW_METADATA_SEGMENT")


def test_root_cause_publish_only_update() -> None:
    plan = [{"Operation": "UPDATE_PUBLISH_DATE", "Field": "publish_date"}]
    assert rec.root_cause_for_ticker("POWW", plan, "TARGET_ABSENT", set()) == ("WRONG_PUBLISH_ASSIGNMENT", "SINGLE_ROW_METADATA")


def test_write_plan_blocks_rh_collision_only() -> None:
    plan = [{"Ticker": "RH", "Transformation Group": "RH_MISLABELLED_Q2", "Operation Order": "1", "Current Fiscal Year": "2022", "Current Fiscal Q": "Q1", "Current Period End": "2021-07-31", "Current Publish Date": "2021-09-08", "Target Fiscal Year": "2021", "Target Fiscal Q": "Q2", "Target Period End": "2021-07-31", "Target Publish Date": "2021-09-08", "Field": "canonical_identity", "Old Value": "", "New Value": "", "Operation": "MOVE_ECONOMIC_QUARTER", "Confidence": "MEDIUM", "Evidence": "official", "Blocking Issue": "collision"}]
    ops, blockers = rec.write_plan_as_ticker_groups(plan, "DIFFERENT_ECONOMIC_QUARTER")
    assert ops[0]["blocked"] == 1
    assert blockers[0]["ticker"] == "RH"


def test_write_plan_preserves_retail_52_53_week_period_end() -> None:
    plan = [{"Ticker": "BBY", "Transformation Group": "BBY_FY2026Q1_METADATA", "Operation Order": "1", "Current Fiscal Year": "2026", "Current Fiscal Q": "Q1", "Current Period End": "2026-04-30", "Current Publish Date": "2025-05-29", "Target Fiscal Year": "2026", "Target Fiscal Q": "Q1", "Target Period End": "2025-05-03", "Target Publish Date": "2025-05-29", "Field": "period_end", "Old Value": "", "New Value": "", "Operation": "UPDATE_PERIOD_END", "Confidence": "HIGH", "Evidence": "official", "Blocking Issue": ""}]
    ops, _ = rec.write_plan_as_ticker_groups(plan, "TARGET_ABSENT")
    assert ops[0]["target_period_end"] == "2025-05-03"


def test_ticker_rows_reads_canonical_and_fundamental_fields(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with rec.connect_ro(db) as conn:
        rows = rec.ticker_rows(conn, "BBY")
    assert rows[0]["accepted_source_provider"] == "YAHOO"


def test_current_by_identity_finds_exact_quarter(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with rec.connect_ro(db) as conn:
        assert rec.current_by_identity(conn, "BBY", 2026, "Q1")["quarter_id"] == 12


def test_current_by_identity_returns_none_for_missing(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with rec.connect_ro(db) as conn:
        assert rec.current_by_identity(conn, "BBY", 2026, "Q2") is None


def test_rh_collision_classifies_existing_different_target(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with rec.connect_ro(db) as conn:
        _compare, cls, resolution = rec.rh_collision(conn)
    assert cls == "DIFFERENT_ECONOMIC_QUARTER"
    assert "blocked" in resolution


def test_integrity_counts_are_reported(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    with rec.connect_ro(db) as conn:
        result = rec.integrity(conn)
    assert result["quick_check"] == "ok"
    assert result["row_counts"]["v3_quarter"] == 5


def test_apply_original_plan_rehearses_fngr_create_and_values(tmp_path: Path) -> None:
    db = make_db(tmp_path / "v3.db")
    plan = [
        {"Ticker": "FNGR", "Operation": "MOVE_ECONOMIC_QUARTER", "Production Ready": "YES", "Current Fiscal Year": "2023", "Current Fiscal Q": "Q3", "Current Period End": "2023-11-30", "Current Publish Date": "2024-01-16", "Target Fiscal Year": "2024", "Target Fiscal Q": "Q3", "Target Period End": "2023-11-30", "Target Publish Date": "2024-01-16", "Operation Order": "1", "Transformation Group": "FNGR_Q3_SHIFT"},
        {"Ticker": "FNGR", "Operation": "MOVE_ECONOMIC_QUARTER", "Production Ready": "YES", "Current Fiscal Year": "2022", "Current Fiscal Q": "Q3", "Current Period End": "2022-11-30", "Current Publish Date": "2022-12-17", "Target Fiscal Year": "2023", "Target Fiscal Q": "Q3", "Target Period End": "2022-11-30", "Target Publish Date": "2023-01-17", "Operation Order": "2", "Transformation Group": "FNGR_Q3_SHIFT"},
        {"Ticker": "FNGR", "Operation": "CREATE_CANONICAL_ROW", "Production Ready": "YES", "Current Fiscal Year": "", "Current Fiscal Q": "", "Current Period End": "", "Current Publish Date": "", "Target Fiscal Year": "2020", "Target Fiscal Q": "Q3", "Target Period End": "2019-11-30", "Target Publish Date": "2020-01-21", "Operation Order": "3", "Transformation Group": "FNGR_Q3_SHIFT"},
    ]
    repairs = [
        {"Ticker": "FNGR", "Field": "Revenue", "Verified Value": "2692734"},
        {"Ticker": "FNGR", "Field": "Operating Income", "Verified Value": "-564770"},
    ]
    _log, integrity = rec.apply_original_plan_to_rehearsal(db, plan, repairs)
    with sqlite3.connect(db) as conn:
        created = conn.execute(
            "SELECT f.revenue,f.operating_income FROM v3_quarter q JOIN v3_quarter_fundamentals f USING(quarter_id) WHERE q.fiscal_year=2020 AND q.fiscal_quarter='Q3'"
        ).fetchone()
    assert integrity["rows_created"] == 1
    assert created == (2692734.0, -564770.0)


def test_validate_input_accepts_exact_15_case_13_ticker_package(tmp_path: Path) -> None:
    case_rows = [
        {"Case ID": str(idx), "Ticker": ticker, "Production Ready": "YES", "Confidence": "HIGH"}
        for idx, ticker in enumerate(sorted(rec.EXPECTED_TICKERS), 1)
    ]
    case_rows.extend(
        [
            {"Case ID": "14", "Ticker": "FNGR", "Production Ready": "YES", "Confidence": "HIGH"},
            {"Case ID": "15", "Ticker": "FNGR", "Production Ready": "YES", "Confidence": "HIGH"},
        ]
    )
    timeline_rows = [{"Ticker": "BBY", "Fiscal Year": "2026", "Fiscal Q": "Q1", "Official Period End": "2025-05-03", "Publish Date": "2025-05-29"}]
    plan_rows = [{"Ticker": "BBY", "Operation": "UPDATE_PERIOD_END"}]
    repair_rows = [{"Ticker": "FNGR", "Field": "Revenue"}]
    p1_root = tmp_path / "a10b"
    write_csv(tmp_path / "cases.csv", case_rows)
    write_csv(tmp_path / "timeline.csv", timeline_rows)
    write_csv(tmp_path / "plan.csv", plan_rows)
    write_csv(tmp_path / "repairs.csv", repair_rows)
    write_csv(p1_root / "global_P1.csv", [{"ticker": "BBY"} for _ in range(15)])
    manifest, *_ = rec.validate_input(
        rec.Phase8A10DRPaths(
            artifact_root=tmp_path / "art",
            v3_db=tmp_path / "v3.db",
            case_resolution_csv=tmp_path / "cases.csv",
            official_timeline_csv=tmp_path / "timeline.csv",
            transformation_plan_csv=tmp_path / "plan.csv",
            fundamental_repairs_csv=tmp_path / "repairs.csv",
            full_a10b_root=p1_root,
        )
    )
    assert manifest["case_rows"] == 15


def test_validate_input_rejects_wrong_ticker_scope(tmp_path: Path) -> None:
    write_csv(tmp_path / "cases.csv", [{"Ticker": "BAD", "Production Ready": "YES", "Confidence": "HIGH"} for _ in range(15)])
    write_csv(tmp_path / "timeline.csv", [{"Ticker": "BAD"}])
    write_csv(tmp_path / "plan.csv", [{"Ticker": "BAD"}])
    write_csv(tmp_path / "repairs.csv", [{"Ticker": "BAD"}])
    write_csv(tmp_path / "a10b" / "global_P1.csv", [{"ticker": "BAD"}])
    with pytest.raises(RuntimeError):
        rec.validate_input(
            rec.Phase8A10DRPaths(
                artifact_root=tmp_path / "art",
                v3_db=tmp_path / "v3.db",
                case_resolution_csv=tmp_path / "cases.csv",
                official_timeline_csv=tmp_path / "timeline.csv",
                transformation_plan_csv=tmp_path / "plan.csv",
                fundamental_repairs_csv=tmp_path / "repairs.csv",
                full_a10b_root=tmp_path / "a10b",
            )
        )
