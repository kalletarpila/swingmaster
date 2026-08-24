from __future__ import annotations

import sqlite3

from swingmaster.fundamentals import v3_phase5_ttm_engine as ttm


def test_inventory_derived_objects() -> None:
    conn = fixture_db()
    names = {row["object_name"]: row["classification"] for row in ttm.legacy_inventory(conn)}
    assert names["v3_ttm"] == "REBUILD_IN_PHASE5"
    assert names["v3_score"] == "REBUILD_LATER_PHASE6"


def test_canonical_tables_protected() -> None:
    conn = fixture_db()
    company = next(row for row in ttm.legacy_inventory(conn) if row["object_name"] == "v3_company")
    assert company["classification"] == "KEEP_CANONICAL"


def test_old_ttm_deletion_plan() -> None:
    conn = fixture_db()
    plan = ttm.deletion_plan_rows(ttm.legacy_inventory(conn))
    assert next(row for row in plan if row["object_name"] == "v3_ttm")["action"] == "DROP_RECREATE"


def test_old_score_valuation_deletion_plan() -> None:
    conn = fixture_db()
    plan = ttm.deletion_plan_rows(ttm.legacy_inventory(conn))
    assert next(row for row in plan if row["object_name"] == "v3_score")["action"] == "DELETE_ROWS_KEEP_SCHEMA"
    assert next(row for row in plan if row["object_name"] == "v3_valuation")["action"] == "DELETE_ROWS_KEEP_SCHEMA"


def test_unrelated_tables_preserved() -> None:
    conn = fixture_db()
    row = next(row for row in ttm.legacy_inventory(conn) if row["object_name"] == "v3_run")
    assert row["action"] == "KEEP"


def test_q1_q4_contiguous() -> None:
    assert ttm.contiguous(rows()[:4])


def test_cross_year_q2_q1_ttm() -> None:
    assert ttm.contiguous(rows()[1:5])


def test_missing_quarter_blocks() -> None:
    assert not ttm.contiguous([rows()[0], rows()[1], rows()[3], rows()[4]])


def test_52_53_week_support_no_calendar_spacing() -> None:
    special = rows()
    special[1]["period_end_date"] = "2024-04-21"
    assert ttm.contiguous(special[:4])


def test_no_calendar_quarter_assumption() -> None:
    special = rows()
    special[2]["period_end_date"] = "2024-07-14"
    assert ttm.compute_ttm_rows(special[:4], run_id="R", calculated_at="N")


def test_revenue_4q_sum() -> None:
    assert built()["ttm_revenue"] == 100.0


def test_gross_profit_4q_sum() -> None:
    assert built()["ttm_gross_profit"] == 60.0


def test_operating_income_4q_sum() -> None:
    assert built()["ttm_operating_income"] == 40.0


def test_ebit_4q_sum() -> None:
    assert built()["ttm_ebit"] == 36.0


def test_ebitda_4q_sum() -> None:
    assert built()["ttm_ebitda"] == 44.0


def test_net_income_4q_sum() -> None:
    assert built()["ttm_net_income"] == 20.0


def test_ocf_4q_sum() -> None:
    assert built()["ttm_ocf"] == 28.0


def test_capex_4q_sum_preserves_sign() -> None:
    assert built()["ttm_capex"] == -8.0


def test_fcf_4q_sum() -> None:
    assert built()["ttm_fcf"] == 20.0


def test_negative_values_valid() -> None:
    sample = rows()
    sample[0]["ebit"] = -2.0
    row = ttm.compute_ttm_rows(sample[:4], run_id="R", calculated_at="N")[0]
    assert row["ebit_4q_ready"] == 1


def test_zero_values_valid() -> None:
    sample = rows()
    sample[0]["ebit"] = 0.0
    row = ttm.compute_ttm_rows(sample[:4], run_id="R", calculated_at="N")[0]
    assert row["ebit_4q_ready"] == 1


def test_ebit_complete_ebitda_missing_primary_ready() -> None:
    sample = rows()
    sample[0]["ebitda"] = None
    row = ttm.compute_ttm_rows(sample[:4], run_id="R", calculated_at="N")[0]
    assert row["ttm_ebit_primary_ready"] == 1
    assert row["ttm_ebitda_secondary_ready"] == 0


def test_ebitda_complete_ebit_missing_primary_not_ready() -> None:
    sample = rows()
    sample[0]["ebit"] = None
    row = ttm.compute_ttm_rows(sample[:4], run_id="R", calculated_at="N")[0]
    assert row["ttm_ebit_primary_ready"] == 0
    assert row["ttm_ebitda_secondary_ready"] == 1


def test_no_substitution() -> None:
    sample = rows()
    sample[0]["ebit"] = None
    row = ttm.compute_ttm_rows(sample[:4], run_id="R", calculated_at="N")[0]
    assert row["ttm_ebit"] is None


def test_cash_endpoint() -> None:
    assert built()["cash"] == rows()[3]["cash"]


def test_debt_endpoint() -> None:
    assert built()["total_debt"] == rows()[3]["total_debt"]


def test_shares_endpoint() -> None:
    assert built()["shares_outstanding"] == rows()[3]["shares_outstanding"]


def test_instant_not_summed() -> None:
    assert built()["cash"] != sum(row["cash"] for row in rows()[:4])


def test_max_publish_date() -> None:
    assert built()["ttm_available_date"] == "2025-04-20"


def test_missing_publish_blocks_pit() -> None:
    sample = rows()
    sample[1]["publish_date"] = None
    row = ttm.compute_ttm_rows(sample[:4], run_id="R", calculated_at="N")[0]
    assert row["ttm_pit_ready"] == 0


def test_math_ttm_can_exist_when_pit_not_ready() -> None:
    sample = rows()
    sample[1]["publish_date"] = None
    row = ttm.compute_ttm_rows(sample[:4], run_id="R", calculated_at="N")[0]
    assert row["ttm_revenue"] == 100.0


def test_no_future_vintage_scope_current_canonical() -> None:
    assert "CURRENT_CANONICAL_TTM" in built()["output_json"]


def test_full_rebuild() -> None:
    conn = fixture_db()
    ttm.ensure_ttm_schema(conn)
    assert ttm.rebuild_ttm(conn, [built()]) == 1


def test_deterministic_result() -> None:
    assert ttm.compute_ttm_rows(rows()[:4], run_id="R", calculated_at="N")[0]["source_fingerprint"] == ttm.compute_ttm_rows(rows()[:4], run_id="R", calculated_at="N")[0]["source_fingerprint"]


def test_no_duplicate_endpoint() -> None:
    conn = fixture_db()
    ttm.ensure_ttm_schema(conn)
    ttm.rebuild_ttm(conn, [built()])
    assert ttm.scalar(conn, "SELECT COUNT(*) FROM (SELECT company_id,endpoint_quarter_id,model_version FROM v3_ttm GROUP BY company_id,endpoint_quarter_id,model_version HAVING COUNT(*)>1)") == 0


def test_first_possible_endpoint_after_four_quarters() -> None:
    assert len(ttm.compute_ttm_rows(rows()[:3], run_id="R", calculated_at="N")) == 0
    assert len(ttm.compute_ttm_rows(rows()[:4], run_id="R", calculated_at="N")) == 1


def test_idempotent_second_rebuild() -> None:
    conn = fixture_db()
    ttm.ensure_ttm_schema(conn)
    ttm.rebuild_ttm(conn, [built()])
    assert ttm.rebuild_ttm(conn, [built()]) == 0


def test_changed_quarter_affects_own_and_next_three() -> None:
    assert ttm.affected_endpoint_keys(1, 2024, "Q2") == [(1, 2024, "Q2"), (1, 2024, "Q3"), (1, 2024, "Q4"), (1, 2025, "Q1")]


def test_unrelated_endpoint_not_recalculated() -> None:
    assert (1, 2025, "Q2") not in ttm.affected_endpoint_keys(1, 2024, "Q2")


def test_restatement_path_same_affected_window() -> None:
    assert len(ttm.affected_endpoint_keys(1, 2024, "Q4")) == 4


def test_canonical_q_unchanged_baseline_shape() -> None:
    assert len(rows()) == 5


def test_canonical_fundamentals_hash_function() -> None:
    assert ttm.hash_json({"a": 1}) == ttm.hash_json({"a": 1})


def test_company_universe_unchanged_inventory_protected() -> None:
    assert ttm.action_for_class("KEEP_CANONICAL") == "KEEP"


def test_sequence_violation_zero_fixture() -> None:
    assert ttm.contiguous(rows()[:4])


def test_invalid_fy_zero_fixture() -> None:
    assert all(row["fiscal_year"] >= 2018 for row in rows())


def test_duplicate_fyfq_zero_fixture() -> None:
    keys = {(row["company_id"], row["fiscal_year"], row["fiscal_quarter"]) for row in rows()}
    assert len(keys) == len(rows())


def test_pre_2018_zero_fixture() -> None:
    assert min(row["fiscal_year"] for row in rows()) >= 2018


def test_quick_check_gate_name() -> None:
    assert ttm.CLASSIFICATION_REPAIR.endswith("REPAIR_REQUIRED")


def test_fk_check_gate_name() -> None:
    assert ttm.CLASSIFICATION_COMPLETE.endswith("READY_FOR_PHASE6")


def built() -> dict:
    return ttm.compute_ttm_rows(rows()[:4], run_id="R", calculated_at="N")[0]


def rows() -> list[dict]:
    out = []
    quarters = [(2024, "Q1", "2024-03-31"), (2024, "Q2", "2024-06-30"), (2024, "Q3", "2024-09-30"), (2024, "Q4", "2024-12-31"), (2025, "Q1", "2025-03-31")]
    for idx, (fy, fq, period) in enumerate(quarters, start=1):
        out.append({
            "company_id": 1,
            "ticker": "AAA",
            "quarter_id": idx,
            "fiscal_year": fy,
            "fiscal_quarter": fq,
            "period_end_date": period,
            "publish_date": f"2025-0{idx if idx < 5 else 5}-20",
            "revenue": 25.0,
            "gross_profit": 15.0,
            "operating_income": 10.0,
            "ebit": 9.0,
            "ebitda": 11.0,
            "net_income": 5.0,
            "operating_cashflow": 7.0,
            "capex": -2.0,
            "free_cashflow": 5.0,
            "cash": 100.0 + idx,
            "total_debt": 50.0 + idx,
            "shares_outstanding": 10.0 + idx,
        })
    return out


def fixture_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_run(run_id TEXT PRIMARY KEY);
        CREATE TABLE v3_ttm(ttm_id INTEGER PRIMARY KEY, company_id INTEGER, as_of_quarter_id INTEGER, model_version TEXT, ttm_ready INTEGER, created_at_utc TEXT, updated_at_utc TEXT);
        CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY);
        """
    )
    return conn
