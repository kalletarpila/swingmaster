from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_phase8e_rehearse_fiscal_repairs import (
    apply_rehearsal,
    build_groups,
    content_signature,
    target_collision,
    validate_inputs,
)
from swingmaster.fundamentals.v3_phase8d7_historical_anchor_reanalysis import classify_repairability


NOW = "2026-08-28T00:00:00Z"


def make_db(tmp_path: Path) -> Path:
    db = tmp_path / "v3.db"
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT NOT NULL);
            CREATE TABLE v3_quarter(
                quarter_id INTEGER PRIMARY KEY,
                company_id INTEGER NOT NULL REFERENCES v3_company(company_id),
                fiscal_year INTEGER NOT NULL,
                fiscal_quarter TEXT NOT NULL,
                period_end_date TEXT,
                publish_date TEXT,
                market_availability_date TEXT,
                q_lifecycle TEXT NOT NULL,
                sec_confirmation_state TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                UNIQUE(company_id,fiscal_year,fiscal_quarter)
            );
            CREATE TABLE v3_quarter_fundamentals(
                quarter_id INTEGER PRIMARY KEY REFERENCES v3_quarter(quarter_id),
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
                currency TEXT,
                accepted_source_provider TEXT,
                accepted_at_utc TEXT,
                update_run_id TEXT,
                derivation_method TEXT,
                resolution_issue_id INTEGER,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            CREATE TABLE v3_provider_q_acquisition(acquisition_id INTEGER PRIMARY KEY, quarter_id INTEGER REFERENCES v3_quarter(quarter_id), provider TEXT);
            CREATE TABLE v3_migration_audit(audit_id INTEGER PRIMARY KEY, quarter_id INTEGER REFERENCES v3_quarter(quarter_id), evidence_json TEXT);
            CREATE TABLE v3_resolution_issue(issue_id INTEGER PRIMARY KEY, quarter_id INTEGER REFERENCES v3_quarter(quarter_id));
            CREATE TABLE v3_operational_action(action_id INTEGER PRIMARY KEY, quarter_id INTEGER REFERENCES v3_quarter(quarter_id));
            CREATE TABLE v3_event(event_id INTEGER PRIMARY KEY, quarter_id INTEGER REFERENCES v3_quarter(quarter_id));
            """
        )
        conn.execute("INSERT INTO v3_company VALUES (1,'AAA')")
        for qid, fy, fq, end in [(1, 2024, "Q1", "2024-09-30"), (2, 2025, "Q1", "2025-09-30")]:
            conn.execute(
                """
                INSERT INTO v3_quarter VALUES (?,?,?,?,?,?,?,'SETTLED','NOT_DERIVABLE',?,?)
                """,
                (qid, 1, fy, fq, end, "2024-11-01", None, NOW, NOW),
            )
            conn.execute(
                """
                INSERT INTO v3_quarter_fundamentals(
                    quarter_id,revenue,gross_profit,operating_income,ebit,ebitda,net_income,
                    operating_cashflow,capex,free_cashflow,cash,total_debt,shares_outstanding,
                    currency,accepted_source_provider,accepted_at_utc,update_run_id,derivation_method,
                    resolution_issue_id,created_at_utc,updated_at_utc
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (qid, 10.0 + qid, 1, 2, 3, 4, 5, 6, -1, 5, 7, 8, 9, "USD", "YAHOO", NOW, "run", "method", None, NOW, NOW),
            )
        conn.commit()
    return db


def candidate(qid: int, old_fy: int, target_fy: int) -> dict[str, str]:
    return {
        "quarter_id": str(qid),
        "company_id": "1",
        "ticker": "AAA",
        "fiscal_year": str(old_fy),
        "fiscal_quarter": "Q1",
        "exact_fy": str(target_fy),
        "exact_fq": "Q1",
        "period_end": "2024-09-30" if qid == 1 else "2025-09-30",
        "publish_date": "2024-11-01",
        "interval_start": "2024-07-01",
        "interval_end_exclusive": "2025-07-01",
        "fq_confidence": "DIRECT_EXACT_FQ_HIGH",
        "publish_chronology": "PUBLISH_AFTER_PERIOD_END",
    }


def test_input_validation_counts_expected_populations() -> None:
    auto = [dict(candidate(i, 2020, 2021), ticker=f"T{i}") for i in range(701)]
    segments = [{"ticker": f"S{i}", "rows": "4", "quarter_ids": "1|2|3|4"} for i in range(151)] + [{"ticker": "S151", "rows": "39", "quarter_ids": "1"}]

    result = validate_inputs(auto, segments)

    assert result["auto_relabel_rows"] == 701
    assert result["auto_relabel_tickers"] == 701
    assert result["atomic_segment_rows"] == 643
    assert result["segment_expected_match"] is True
    assert result["valid"] is False


def test_collision_classes() -> None:
    row = candidate(1, 2024, 2025)

    assert target_collision(row, None, {1}) == "TARGET_EMPTY"
    assert target_collision(row, {"quarter_id": 1, "period_end": row["period_end"]}, {1}) == "TARGET_SAME_ECONOMIC"
    assert target_collision(row, {"quarter_id": 2, "period_end": "2025-09-30"}, {1, 2}) == "TARGET_SAME_ECONOMIC_COMPLEMENTARY"
    assert target_collision(row, {"quarter_id": 3, "period_end": row["period_end"]}, {1}) == "TARGET_CONFLICTING"
    assert target_collision(row, {"quarter_id": 3, "period_end": "2025-09-30"}, {1}) == "TARGET_DIFFERENT_ECONOMIC"


def test_build_groups_blocks_external_different_economic_collision() -> None:
    auto = [candidate(1, 2024, 2025)]
    identity = {(1, 2025, "Q1"): {"quarter_id": 2, "period_end": "2025-09-30"}}

    frozen, blockers, collisions = build_groups(auto, [], identity)

    assert frozen == []
    assert blockers[0]["reason"] == "TARGET_DIFFERENT_ECONOMIC"
    assert collisions[0]["target_collision_class"] == "TARGET_DIFFERENT_ECONOMIC"


def test_rehearsal_temp_rekey_preserves_signature_and_quarter_id(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    frozen = [
        {**candidate(1, 2024, 2025), "transformation_group": "G1", "operation_order": 1, "group_type": "ATOMIC_SEGMENT", "operation": "UPDATE_FY", "target_collision_class": "TARGET_SAME_ECONOMIC_COMPLEMENTARY"},
        {**candidate(2, 2025, 2026), "transformation_group": "G1", "operation_order": 2, "group_type": "ATOMIC_SEGMENT", "operation": "UPDATE_FY", "target_collision_class": "TARGET_EMPTY"},
    ]
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        before = content_signature(conn, 1)["content_signature"]

    log, parity, lineage, integrity = apply_rehearsal(db, frozen)

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        after = content_signature(conn, 1)["content_signature"]
        qids = [row[0] for row in conn.execute("SELECT quarter_id FROM v3_quarter ORDER BY quarter_id")]

    assert before == after
    assert all(row["signature_match"] for row in parity)
    assert all(row["lineage_match"] for row in lineage)
    assert integrity["quick_check"] == "ok"
    assert integrity["duplicate_fy_fq"] == 0
    assert qids == [1, 2]
    assert {row["result"] for row in log} == {"OK"}


def test_repairability_blocks_metadata_and_content_failures() -> None:
    assert classify_repairability({"identity_class": "PASS_DIRECT_EXACT"}) == "NO_REPAIR_NEEDED"
    assert classify_repairability({"identity_class": "REVIEW_TRANSITION"}) == "TRANSITION_REVIEW"
    assert classify_repairability({"identity_class": "BLOCK_EXACT_FY_CONFLICT", "period_end_structural_fit": "STRUCTURAL_REVIEW"}) == "METADATA_REPAIR_REQUIRED"
    assert classify_repairability({"identity_class": "BLOCK_EXACT_FY_CONFLICT", "period_end_structural_fit": "STRUCTURAL_FIT", "content_integrity": "CONTENT_MAPPING_REVIEW"}) == "CONTENT_RECONSTRUCTION_REQUIRED"
