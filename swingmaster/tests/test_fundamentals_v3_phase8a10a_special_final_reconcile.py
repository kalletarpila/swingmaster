from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10a_special_final_reconcile as rec


def write_csv(path: Path, rows: list[dict[str, str]]) -> Path:
    fieldnames = list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def seed_files(root: Path) -> None:
    write_csv(
        root / "phase8_immr_rcat_official_quarter_matrix.csv",
        [
            official("IMMR", "2025", "Q1", "QUARTER", "2024-07-31", "2024-12-16", "183489000"),
            official("RCAT", "2024T", "Q1", "TRANSITION_QUARTER_3M", "2024-07-31", "2024-09-23", "2776535"),
            official("RCAT", "2024T", "Q2", "TRANSITION_QUARTER_3M", "2024-10-31", "2024-12-16", "1534727"),
            official("RCAT", "2024T", "STUB", "TRANSITION_STUB_2M", "2024-12-31", "2025-03-31", "539042"),
            official("RCAT", "2025", "Q1", "QUARTER", "2025-03-31", "2025-05-14", "1629662"),
        ],
    )
    write_csv(
        root / "phase8_immr_rcat_v3_row_mapping.csv",
        [
            mapping("IMMR", "42578", "2024", "Q2", "2024-07-31", "2024-08-20", "89943000", "2025", "Q1", "2024-07-31", "2024-12-16", "183489000", "NO", "YES", "DELETE_AND_RECREATE", "YES"),
            mapping("RCAT", "59126", "2024", "Q2", "2024-07-31", "2024-08-08", "886440", "2024T", "Q1", "2024-07-31", "2024-09-23", "2776535", "YES", "NO", "DELETE_AND_RECREATE", "CONDITIONAL"),
            mapping("RCAT", "59125", "2024", "Q3", "2024-10-31", "2024-03-18", "1534727", "2024T", "Q2", "2024-10-31", "2024-12-16", "1534727", "YES", "NO", "RELABEL_AND_UPDATE", "CONDITIONAL"),
            mapping("RCAT", "9812", "2025", "Q1", "2025-03-31", "2025-05-14", "6614029", "2025", "Q1", "2025-03-31", "2025-05-14", "1629662", "NO", "NO", "DELETE_AND_RECREATE", "YES"),
        ],
    )
    write_csv(
        root / "phase8_immr_restatement_field_matrix.csv",
        [
            restatement("2025", "Q1", "Revenue", "89943000", "183489000", "REPLACE_WITH_RESTATED_VALUE", "HIGH"),
            restatement("2025", "Q1", "Gross Profit", "147158000", "NOT_VERIFIABLE_FROM_RESTATEMENT_SOURCE", "NO_AUTOMATED_VALUE_ACTION", "LOW"),
            restatement("2025", "Q1", "Shares Outstanding", "31970", "NOT_VERIFIABLE_FROM_RESTATEMENT_SOURCE", "NO_AUTOMATED_VALUE_ACTION", "LOW"),
        ],
    )
    write_csv(
        root / "phase8_immr_rcat_final_transformation_plan.csv",
        [
            plan("IMMR_CALENDAR_REBASE", "IMMR", "10", "42578", "2024", "Q2", "2024-07-31", "2025", "Q1", "2024-07-31", "ALL_VERIFIED_FIELDS", "JULY_ONLY_ROW", "FULL_MAY_JULY_QUARTER", "DELETE_AND_RECREATE", "FY2025_Q1", "YES", ""),
            plan("RCAT_TRANSITION", "RCAT", "110", "59126", "2024", "Q2", "2024-07-31", "2024T", "Q1", "2024-07-31", "ALL_VERIFIED_FIELDS", "Revenue 886440", "Revenue 2776535", "DELETE_AND_RECREATE", "FY2024T_Q1", "CONDITIONAL", "Schema must support transition fiscal-year namespace 2024T"),
            plan("RCAT_TRANSITION", "RCAT", "130", "", "", "", "", "2024T", "STUB", "2024-12-31", "REVENUE_AND_PERIOD_METADATA", "MISSING", "Revenue 539042", "CREATE_AUXILIARY_PERIOD", "FY2024T_STUB", "CONDITIONAL", "Schema must support non-quarter period type"),
        ],
    )
    write_csv(
        root / "phase8_rcat_transition_policy.csv",
        [
            policy("Transition quarter 1", "Transition Q1", "2024-05-01", "2024-07-31", "2024T", "Q1", "YES"),
            policy("Transition quarter 2", "Transition Q2", "2024-08-01", "2024-10-31", "2024T", "Q2", "YES"),
            policy("Transition stub", "Transition stub (2 months)", "2024-11-01", "2024-12-31", "2024T", "STUB", "YES"),
            policy("Rejected encoding", "FY2024 Q3 for stub", "2024-11-01", "2024-12-31", "2024", "Q3", "NO"),
        ],
    )


def official(ticker: str, fy: str, fq: str, period_type: str, period_end: str, publish: str, revenue: str) -> dict[str, str]:
    return {
        "Ticker": ticker,
        "Company Name": ticker,
        "Fiscal Year": fy,
        "Fiscal Q": fq,
        "Fiscal Period Type": period_type,
        "Official Period Start": "",
        "Official Period End": period_end,
        "Publish Date": publish,
        "Revenue": revenue,
        "Gross Profit": "",
        "Operating Income": "",
        "EBIT": "",
        "EBITDA": "",
        "Net Income": "",
        "Operating Cash Flow": "",
        "Capex": "",
        "Free Cash Flow": "",
        "Cash": "",
        "Total Debt": "",
        "Shares Outstanding": "",
        "Original Or Restated": "RESTATED",
        "Restatement Filing": "",
        "Primary Source": "source",
        "Primary Source Type": "SEC",
        "Secondary Source": "",
        "Secondary Source Type": "",
        "Confidence": "HIGH",
        "Notes": "",
    }


def mapping(ticker: str, qid: str, cfy: str, cfq: str, cperiod: str, cpub: str, crev: str, tfy: str, tfq: str, tperiod: str, tpub: str, trev: str, economic: str, restated: str, action: str, ready: str) -> dict[str, str]:
    return {
        "Ticker": ticker,
        "Current Canonical Quarter ID": qid,
        "Current Fiscal Year": cfy,
        "Current Fiscal Q": cfq,
        "Current Period End": cperiod,
        "Current Publish Date": cpub,
        "Current Revenue": crev,
        "Correct Fiscal Year": tfy,
        "Correct Fiscal Q": tfq,
        "Correct Period End": tperiod,
        "Correct Publish Date": tpub,
        "Correct Revenue": trev,
        "Economic Quarter Match": economic,
        "Restatement Required": restated,
        "Primary Root Cause": "ROOT",
        "Proposed Action": action,
        "Merge Target Quarter ID": "",
        "Delete Current Row": "YES" if action == "DELETE_AND_RECREATE" else "NO",
        "Production Ready": ready,
        "Confidence": "HIGH",
        "Primary Evidence": "source",
        "Secondary Evidence": "",
        "Exact Explanation": "explanation",
    }


def restatement(fy: str, fq: str, field: str, current: str, restated_value: str, action: str, confidence: str) -> dict[str, str]:
    return {
        "Ticker": "IMMR",
        "Fiscal Year": fy,
        "Fiscal Q": fq,
        "Period End": "2024-07-31",
        "Field": field,
        "Old Reported Value": current,
        "Restated Value": restated_value,
        "Changed": "UNKNOWN",
        "Current V3 Value": current,
        "V3 Matches Old": "NO",
        "V3 Matches Restated": "NO",
        "Required V3 Action": action,
        "Primary Source": "source",
        "Confidence": confidence,
        "Notes": "",
    }


def plan(group: str, ticker: str, order: str, qid: str, cfy: str, cfq: str, cperiod: str, tfy: str, tfq: str, tperiod: str, field: str, old: str, new: str, operation: str, target: str, ready: str, blocker: str) -> dict[str, str]:
    return {
        "Transformation Group": group,
        "Ticker": ticker,
        "Operation Order": order,
        "Current Canonical Quarter ID": qid,
        "Current Fiscal Year": cfy,
        "Current Fiscal Q": cfq,
        "Current Period End": cperiod,
        "Target Fiscal Year": tfy,
        "Target Fiscal Q": tfq,
        "Target Period End": tperiod,
        "Field": field,
        "Old Value": old,
        "New Value": new,
        "Operation": operation,
        "Merge/Delete/Create Target": target,
        "Lineage Treatment": "Preserve lineage",
        "Source Evidence": "source",
        "Confidence": "HIGH",
        "Production Ready": ready,
        "Blocking Issue": blocker,
        "Notes": "",
    }


def policy(period: str, label: str, start: str, end: str, fy: str, fq: str, recommended: str) -> dict[str, str]:
    return {
        "Official Reporting Period": period,
        "Official Label": label,
        "Period Start": start,
        "Period End": end,
        "Recommended V3 Fiscal Year": fy,
        "Recommended V3 Fiscal Q": fq,
        "Reason": "reason",
        "Alternative Encoding": "",
        "Recommended": recommended,
        "Confidence": "HIGH",
        "Source": "source",
    }


def make_db(path: Path) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT);
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL, fiscal_year INTEGER NOT NULL,
            fiscal_quarter TEXT NOT NULL CHECK (fiscal_quarter IN ('Q1', 'Q2', 'Q3', 'Q4')),
            period_end_date TEXT, publish_date TEXT, market_availability_date TEXT, q_lifecycle TEXT,
            sec_confirmation_state TEXT, created_at_utc TEXT, updated_at_utc TEXT,
            UNIQUE(company_id,fiscal_year,fiscal_quarter)
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY, revenue REAL, gross_profit REAL, operating_income REAL, ebit REAL,
            ebitda REAL, net_income REAL, operating_cashflow REAL, capex REAL, free_cashflow REAL, cash REAL,
            total_debt REAL, shares_outstanding REAL, currency TEXT, accepted_source_provider TEXT,
            accepted_at_utc TEXT, update_run_id TEXT, derivation_method TEXT, resolution_issue_id INTEGER,
            created_at_utc TEXT, updated_at_utc TEXT
        );
        CREATE TABLE v3_migration_audit(audit_id INTEGER PRIMARY KEY, quarter_id INTEGER);
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER, q1_quarter_id INTEGER, q2_quarter_id INTEGER, q3_quarter_id INTEGER, q4_quarter_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, as_of_quarter_id INTEGER);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        INSERT INTO v3_company VALUES (1,'IMMR'),(2,'RCAT');
        INSERT INTO v3_quarter VALUES (42578,1,2024,'Q2','2024-07-31','2024-08-20',NULL,'OPERATIONALLY_SETTLED','CONFIRMED','c','u');
        INSERT INTO v3_quarter VALUES (59126,2,2024,'Q2','2024-07-31','2024-08-08',NULL,'OPERATIONALLY_SETTLED','CONFIRMED','c','u');
        INSERT INTO v3_quarter VALUES (59125,2,2024,'Q3','2024-10-31','2024-03-18',NULL,'OPERATIONALLY_SETTLED','CONFIRMED','c','u');
        INSERT INTO v3_quarter VALUES (9812,2,2025,'Q1','2025-03-31','2025-05-14',NULL,'OPERATIONALLY_SETTLED','CONFIRMED','c','u');
        INSERT INTO v3_quarter_fundamentals VALUES (42578,89943000,147158000,-9642000,-9642000,97442000,NULL,-114373000,2808000,-111565000,57653000,NULL,31970,'USD','TEST','a','r','d',NULL,'c','u');
        INSERT INTO v3_quarter_fundamentals VALUES (59126,886440,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'USD','TEST','a','r','d',NULL,'c','u');
        INSERT INTO v3_quarter_fundamentals VALUES (59125,1534727,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'USD','TEST','a','r','d',NULL,'c','u');
        INSERT INTO v3_quarter_fundamentals VALUES (9812,6614029,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'USD','TEST','a','r','d',NULL,'c','u');
        INSERT INTO v3_migration_audit VALUES (1,42578);
        """
    )
    conn.commit()
    conn.close()
    return path


def run_fixture(tmp_path: Path) -> dict:
    seed_files(tmp_path)
    db = make_db(tmp_path / "v3.db")
    raw = tmp_path / "raw.db"
    raw.write_bytes(b"raw")
    return rec.run_phase8a10a_special_final_reconcile(
        rec.Phase8A10ASpecialFinalReconcilePaths(tmp_path / "artifacts", db, raw, tmp_path)
    )


def test_all_five_external_files_located(tmp_path: Path) -> None:
    seed_files(tmp_path)
    assert set(rec.locate_external_files(tmp_path)) == set(rec.EXPECTED_FILES)


def test_immr_rows_map_deterministically(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["current_v3"]["current_state_drift"] == 0


def test_rcat_rows_map_deterministically(tmp_path: Path) -> None:
    run_fixture(tmp_path)
    rows = list(csv.DictReader((tmp_path / "artifacts" / "rcat_external_vs_v3_mapping.csv").open()))
    assert all(row["join_status"] == "EXACT_CURRENT_ROW_MATCH" for row in rows)


def test_immr_known_revenue_restatement_detected() -> None:
    out = rec.restatement_reconciliation([restatement("2025", "Q4", "Revenue", "281376000", "284876000", "REPLACE_WITH_RESTATED_VALUE", "HIGH")])
    assert out[0]["write_eligible"] == "YES"


def test_not_verifiable_field_excluded_from_write() -> None:
    out = rec.restatement_reconciliation([restatement("2025", "Q1", "Gross Profit", "1", "NOT_VERIFIABLE_FROM_RESTATEMENT_SOURCE", "NO_AUTOMATED_VALUE_ACTION", "LOW")])
    assert out[0]["write_eligible"] == "NO"


def test_no_guessed_restatement_values(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["immr"]["not_verifiable_fields"] == 2


def test_rcat_886440_rejected_as_total_revenue(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["rcat"]["revenue_2024_07_31_current"] == "886440"


def test_rcat_2776535_recognized_for_2024_07_31(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["rcat"]["revenue_2024_07_31_correct"] == "2776535"


def test_rcat_1534727_remains_with_2024_10_31(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["rcat"]["revenue_2024_10_31_correct"] == "1534727"


def test_rcat_fy2025_q1_1629662_recognized(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["rcat"]["fy2025_q1_revenue_correct"] == "1629662"


def test_rcat_6614029_rejected_for_fy2025_q1(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["rcat"]["fy2025_q1_revenue_current"] == "6614029"


def test_stub_not_silently_encoded_as_q3(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["rcat"]["schema_supports_stub_directly"] == "NO"


def test_schema_compatibility_check(tmp_path: Path) -> None:
    seed_files(tmp_path)
    db = make_db(tmp_path / "v3.db")
    with rec.connect_ro(db) as conn:
        compat = rec.schema_compatibility(conn, rec.read_csv(tmp_path / "phase8_rcat_transition_policy.csv"))
    assert compat["classification"] == "RCAT_TRANSITION_POLICY_BLOCKER"


def test_target_collision_detection(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["blockers"]["architecture_policy_decision_needed"]


def test_atomic_group_all_or_nothing(tmp_path: Path) -> None:
    summary = run_fixture(tmp_path)
    assert summary["frozen_apply"]["atomic_operations"] == 0


def test_lineage_ownership_preserved_in_plan(tmp_path: Path) -> None:
    run_fixture(tmp_path)
    rows = list(csv.DictReader((tmp_path / "artifacts" / "special_lineage_publish_ownership.csv").open()))
    assert all(row["status"] == "PLAN_ONLY_READ_ONLY" for row in rows)


def test_no_production_writes(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["safety"]["production_writes"] == 0


def test_no_derived_writes(tmp_path: Path) -> None:
    safety = run_fixture(tmp_path)["safety"]
    assert safety["ttm_writes"] == safety["score_writes"] == safety["lifecycle_writes"] == safety["valuation_writes"] == 0


def test_no_rawcandle_writes(tmp_path: Path) -> None:
    assert run_fixture(tmp_path)["safety"]["rawcandle_writes"] == 0
