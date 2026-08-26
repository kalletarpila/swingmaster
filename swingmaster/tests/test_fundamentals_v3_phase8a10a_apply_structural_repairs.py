from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase8a10a_apply_structural_repairs as apply


COUNTS = {"CRUS": 6, "DOMO": 9, "EEFT": 9, "INBS": 5, "MNR": 5, "MNRO": 6, "NCNO": 9, "RBC": 6, "SKY": 6, "VIVS": 6}


def frozen_rows() -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    qid = 1
    for ticker, count in COUNTS.items():
        for idx in range(count):
            fq = f"Q{idx % 4 + 1}"
            out.append(
                {
                    "ticker": ticker,
                    "transformation_group_id": f"P8A10A-R-{ticker}",
                    "current_canonical_quarter_id": str(qid),
                    "current_fy": str(2024 + idx // 4),
                    "current_fq": fq,
                    "current_period_end": f"202{idx % 6}-03-31",
                    "current_publish_date": f"202{idx % 6}-05-01",
                    "proposed_fy": str(2025 + idx // 4),
                    "proposed_fq": fq,
                    "proposed_period_end": f"202{idx % 6}-03-29",
                    "proposed_publish_date": f"202{idx % 6}-05-01",
                    "transformation_shape": "SHIFT_MULTI_QUARTER_SEGMENT",
                }
            )
            qid += 1
    return out


def atomic_rows(frozen: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {"source_canonical_quarter_id": row["current_canonical_quarter_id"], "operation": op}
        for row in frozen
        for op in ("CREATE_TEMP_IDENTITY", "FINALIZE_IDENTITY")
    ]


def group_summary() -> list[dict[str, str]]:
    return [
        {"ticker": ticker, "transformation_group_id": f"P8A10A-R-{ticker}", "final_production_ready": "YES"}
        for ticker in sorted(COUNTS)
    ]


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, company_name TEXT);
        CREATE TABLE v3_quarter(
            quarter_id INTEGER PRIMARY KEY, company_id INTEGER, fiscal_year INTEGER, fiscal_quarter TEXT,
            period_end_date TEXT, publish_date TEXT, market_availability_date TEXT, q_lifecycle TEXT,
            sec_confirmation_state TEXT, created_at_utc TEXT, updated_at_utc TEXT,
            UNIQUE(company_id,fiscal_year,fiscal_quarter)
        );
        CREATE TABLE v3_quarter_fundamentals(
            quarter_id INTEGER PRIMARY KEY, revenue REAL, gross_profit REAL, operating_income REAL, ebit REAL,
            ebitda REAL, net_income REAL, operating_cashflow REAL, capex REAL, free_cashflow REAL, cash REAL,
            total_debt REAL, shares_outstanding REAL, currency TEXT, accepted_source_provider TEXT,
            accepted_at_utc TEXT, update_run_id TEXT, derivation_method TEXT, resolution_issue_id INTEGER
        );
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, as_of_quarter_id INTEGER);
        CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, endpoint_quarter_id INTEGER);
        CREATE TABLE v3_migration_audit(audit_id INTEGER PRIMARY KEY, quarter_id INTEGER);
        INSERT INTO v3_company VALUES (1,'AAA','A');
        INSERT INTO v3_quarter VALUES (1,1,2024,'Q4','2025-01-31','2025-03-01',NULL,'OPERATIONALLY_SETTLED','CONFIRMED','c','u');
        INSERT INTO v3_quarter VALUES (2,1,2025,'Q4','2026-01-31','2026-03-01',NULL,'OPERATIONALLY_SETTLED','CONFIRMED','c','u');
        INSERT INTO v3_quarter_fundamentals VALUES (1,100,50,10,10,12,8,9,-1,8,20,5,10,'USD','YAHOO','a','r','d',NULL);
        INSERT INTO v3_quarter_fundamentals VALUES (2,200,60,20,20,22,18,19,-2,18,30,6,11,'USD','YAHOO','a','r','d',NULL);
        INSERT INTO v3_migration_audit VALUES (1,1);
        """
    )
    return conn


def two_row_rotation() -> list[dict[str, str]]:
    return [
        {
            "ticker": "AAA",
            "transformation_group_id": "P8A10A-R-AAA",
            "current_canonical_quarter_id": "1",
            "current_fy": "2024",
            "current_fq": "Q4",
            "current_period_end": "2025-01-31",
            "current_publish_date": "2025-03-01",
            "proposed_fy": "2025",
            "proposed_fq": "Q4",
            "proposed_period_end": "2025-01-31",
            "proposed_publish_date": "2025-03-01",
        },
        {
            "ticker": "AAA",
            "transformation_group_id": "P8A10A-R-AAA",
            "current_canonical_quarter_id": "2",
            "current_fy": "2025",
            "current_fq": "Q4",
            "current_period_end": "2026-01-31",
            "current_publish_date": "2026-03-01",
            "proposed_fy": "2026",
            "proposed_fq": "Q4",
            "proposed_period_end": "2026-01-31",
            "proposed_publish_date": "2026-03-01",
        },
    ]


def test_frozen_group_count_10() -> None:
    apply.validate_scope(frozen_rows(), atomic_rows(frozen_rows()), group_summary())
    assert len(apply.group_rows(frozen_rows())) == 10


def test_frozen_canonical_rows_67() -> None:
    assert len(frozen_rows()) == 67


def test_ticker_set_exact() -> None:
    assert {row["ticker"] for row in frozen_rows()} == apply.EXPECTED_TICKERS


def test_fngr_excluded() -> None:
    assert "FNGR" not in {row["ticker"] for row in frozen_rows()}


def test_immr_excluded() -> None:
    assert "IMMR" not in {row["ticker"] for row in frozen_rows()}


def test_rcat_excluded() -> None:
    assert "RCAT" not in {row["ticker"] for row in frozen_rows()}


def test_old_state_guard_passes() -> None:
    guards = apply.write_guards(db(), two_row_rotation())
    assert [row["status"] for row in guards] == ["PASS", "PASS"]


def test_multi_row_rotation_simulation() -> None:
    conn = db()
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    assert conn.execute("SELECT fiscal_year FROM v3_quarter WHERE quarter_id=1").fetchone()[0] == 2025


def test_target_exists_different_economic_quarter_safe_rotation() -> None:
    conn = db()
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    assert apply.integrity_counts(conn)["duplicate_fy_fq"] == 0


def test_same_economic_quarter_target_handling() -> None:
    plan = two_row_rotation()[0]
    plan["proposed_fy"] = "2024"
    conn = db()
    apply.apply_group(conn, [plan], sentinel_base=900000, applied_at="test")
    assert conn.execute("SELECT fiscal_year FROM v3_quarter WHERE quarter_id=1").fetchone()[0] == 2024


def test_temporary_identity_avoids_unique_collision() -> None:
    conn = db()
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"


def test_group_atomicity_rollback_on_bad_guard() -> None:
    conn = db()
    bad = two_row_rotation()
    bad[1]["current_period_end"] = "BAD"
    try:
        apply.apply_group(conn, bad, sentinel_base=900000, applied_at="test")
    except RuntimeError:
        pass
    assert conn.execute("SELECT fiscal_year FROM v3_quarter WHERE quarter_id=1").fetchone()[0] == 2024


def test_content_signatures_preserved_after_period_publish_move() -> None:
    conn = db()
    before = apply.write_guards(conn, two_row_rotation())
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    parity, sigs, _ = apply.final_parity(conn, two_row_rotation(), {int(row["quarter_id"]): row for row in before})
    assert all(row["status"] == "PASS" for row in parity)
    assert all(row["fundamental_content_signature_preserved"] for row in sigs)


def test_period_end_moves_with_economic_quarter() -> None:
    conn = db()
    before = apply.write_guards(conn, two_row_rotation())
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    _, sigs, _ = apply.final_parity(conn, two_row_rotation(), {int(row["quarter_id"]): row for row in before})
    assert all(row["period_end_moved_with_economic_quarter"] for row in sigs)


def test_publish_date_moves_with_economic_quarter() -> None:
    conn = db()
    before = apply.write_guards(conn, two_row_rotation())
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    _, sigs, _ = apply.final_parity(conn, two_row_rotation(), {int(row["quarter_id"]): row for row in before})
    assert all(row["publish_date_moved_with_economic_quarter"] for row in sigs)


def test_fundamentals_move_with_economic_quarter() -> None:
    conn = db()
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    assert conn.execute("SELECT revenue FROM v3_quarter_fundamentals WHERE quarter_id=1").fetchone()[0] == 100


def test_lineage_remains_intact() -> None:
    conn = db()
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    assert apply.lineage_integrity(conn, two_row_rotation())[0]["migration_audit_refs"] == 1


def test_52_53_week_official_dates_preserved() -> None:
    assert apply.WEEK_52_53_TICKERS == {"CRUS", "MNRO", "RBC", "SKY"}


def test_vivs_segment_coherent() -> None:
    assert COUNTS["VIVS"] == 6


def test_unrelated_canonical_unchanged() -> None:
    conn = db()
    before = apply.canonical_snapshot(conn, {1}, invert=True)
    plan = two_row_rotation()[0]
    plan["proposed_fy"] = "2027"
    apply.apply_group(conn, [plan], sentinel_base=900000, applied_at="test")
    assert apply.unrelated_drift(before, conn, {1})["unrelated_canonical_drift"] == 0


def test_company_count_unchanged() -> None:
    conn = db()
    before = apply.table_counts(conn)
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    assert before["v3_company"] == apply.table_counts(conn)["v3_company"]


def test_canonical_row_count_unchanged() -> None:
    conn = db()
    before = apply.table_counts(conn)
    apply.apply_group(conn, two_row_rotation(), sentinel_base=900000, applied_at="test")
    assert before["v3_quarter"] == apply.table_counts(conn)["v3_quarter"]


def test_no_ttm_writes() -> None:
    assert apply.table_counts(db())["v3_ttm"] == 0


def test_no_score_writes() -> None:
    assert apply.table_counts(db())["v3_score"] == 0


def test_no_lifecycle_writes() -> None:
    assert apply.table_counts(db())["v3_lifecycle"] == 0


def test_no_valuation_writes() -> None:
    assert apply.table_counts(db())["v3_valuation"] == 0


def test_no_rawcandle_writes_contract() -> None:
    assert apply.Phase8A10AApplyPaths(Path("x"), Path("db")).rawcandle_db == Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def test_rollback_simulation() -> None:
    conn = db()
    bad = two_row_rotation()
    bad[0]["current_publish_date"] = "BAD"
    try:
        apply.apply_group(conn, bad, sentinel_base=900000, applied_at="test")
    except RuntimeError:
        pass
    assert conn.execute("SELECT publish_date FROM v3_quarter WHERE quarter_id=1").fetchone()[0] == "2025-03-01"
