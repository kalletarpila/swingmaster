from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_phase8e_preapply_downstream_proving import (
    EXPECTED_FROZEN_GROUPS,
    EXPECTED_FROZEN_ROWS,
    EXPECTED_FROZEN_TICKERS,
    attribution,
    classify_attribution,
    has_writes,
    precondition_check,
    validate_frozen_shape,
)
from swingmaster.fundamentals.v3_phase8e_rehearse_fiscal_repairs import content_signature


NOW = "2026-08-29T00:00:00Z"


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
        conn.execute("INSERT INTO v3_quarter VALUES (1,1,2024,'Q1','2024-09-30','2024-11-01',NULL,'SETTLED','NOT_DERIVABLE',?,?)", (NOW, NOW))
        conn.execute("INSERT INTO v3_quarter VALUES (2,1,2025,'Q2','2025-12-31','2026-02-01',NULL,'SETTLED','NOT_DERIVABLE',?,?)", (NOW, NOW))
        for qid in (1, 2):
            conn.execute(
                """
                INSERT INTO v3_quarter_fundamentals VALUES (
                    ?,10,1,2,3,4,5,6,-1,5,7,8,9,'USD','YAHOO',?,'run','method',NULL,?,?
                )
                """,
                (qid, NOW, NOW, NOW),
            )
        conn.commit()
    return db


def frozen_row(db: Path, *, quarter_id: int = 1, group: str = "G1") -> dict[str, str]:
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        sig = content_signature(conn, quarter_id)["content_signature"]
    return {
        "transformation_group": group,
        "ticker": "AAA",
        "quarter_id": str(quarter_id),
        "old_fiscal_year": "2024",
        "old_fiscal_quarter": "Q1",
        "target_fiscal_year": "2025",
        "target_fiscal_quarter": "Q1",
        "period_end": "2024-09-30",
        "publish_date": "2024-11-01",
        "content_signature": sig,
        "target_collision_class": "TARGET_EMPTY",
        "priority": "P1",
        "exact_anchor_fy_start": "2024-07-01",
    }


def test_frozen_shape_requires_exact_phase8e_population() -> None:
    frozen = [{"transformation_group": f"G{i}", "ticker": f"T{i}", "quarter_id": str(i)} for i in range(EXPECTED_FROZEN_ROWS)]
    for idx, row in enumerate(frozen):
        row["ticker"] = f"T{idx % EXPECTED_FROZEN_TICKERS}"
        row["transformation_group"] = f"G{idx % EXPECTED_FROZEN_GROUPS}"

    result = validate_frozen_shape(frozen, [])

    assert result["shape_valid"] is True
    assert result["blocked_rows_promoted"] == 0


def test_blocked_phase8e_row_is_not_promoted() -> None:
    result = validate_frozen_shape([{"transformation_group": "G1", "ticker": "AAA", "quarter_id": "1"}], [{"quarter_id": "1"}])

    assert result["blocked_rows_promoted"] == 1
    assert result["shape_valid"] is False


def test_stale_precondition_rejects_whole_group(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    good = frozen_row(db, quarter_id=1, group="G1")
    stale = {**frozen_row(db, quarter_id=1, group="G1"), "quarter_id": "2", "old_fiscal_year": "2024", "old_fiscal_quarter": "Q1", "period_end": "2024-09-30"}

    checks, ready, blockers = precondition_check(db, [good, stale])

    assert {row["precondition_status"] for row in checks} == {"PASS", "STALE_PRECONDITION_BLOCKED"}
    assert ready == []
    assert blockers[0]["blocker"] == "STALE_PRECONDITION"


def test_attribution_flags_unrelated_drift() -> None:
    rows, drift = attribution({"ttm": [{"layer": "ttm", "ticker": "AAA"}, {"layer": "ttm", "ticker": "BBB"}]}, {"AAA"})

    assert [row["attribution"] for row in rows] == ["DIRECT_REPAIR_TICKER", "UNRELATED_DRIFT"]
    assert drift[0]["ticker"] == "BBB"


def test_valuation_price_only_rebuild_change_is_expected_normalization() -> None:
    row = {"layer": "valuation", "ticker": "BBB", "change_type": "UPDATED", "changed_fields": "valuation_close_price|market_cap|enterprise_value|output_json"}

    assert classify_attribution(row, {"AAA"}) == "EXPECTED_REBUILD_NORMALIZATION"


def test_has_writes_only_counts_real_upserts() -> None:
    assert has_writes({"NOOP": 10}) is False
    assert has_writes({"UPDATED_SOURCE_CHANGED": 1}) is True
    assert has_writes({"INSERTED": 1}) is True
