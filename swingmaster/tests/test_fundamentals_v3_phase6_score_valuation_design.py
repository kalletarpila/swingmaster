from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase6_score_valuation_design as p6


def test_downstream_formula_inventory() -> None:
    ids = {row["formula_id"] for row in p6.formula_inventory()}
    assert {"EV_EBIT", "EV_EBITDA", "SCORE_MARGIN", "SCORE_LEVERAGE"}.issubset(ids)


def test_ebitda_first_dependency_detection() -> None:
    ids = {row["formula_id"] for row in p6.old_ebitda_first_inventory()}
    assert "V2_PRIMARY_EV_MULTIPLE_SELECTOR" in ids
    assert "SCORE_MARGIN" in ids


def test_ebit_primary_classification() -> None:
    row = next(item for item in p6.formula_inventory() if item["formula_id"] == "SCORE_MARGIN")
    assert row["recommendation"] == "CHANGE_TO_EBIT"


def test_ebitda_secondary_retained() -> None:
    row = next(item for item in p6.formula_inventory() if item["formula_id"] == "EV_EBITDA")
    assert row["recommendation"] == "KEEP_EBITDA_SECONDARY"


def test_all_12_fields_retained() -> None:
    assert p6.ALL_FUNDAMENTAL_FIELDS == (
        "revenue",
        "ebit",
        "free_cashflow",
        "cash",
        "total_debt",
        "shares_outstanding",
        "gross_profit",
        "operating_income",
        "ebitda",
        "net_income",
        "operating_cashflow",
        "capex",
    )


def test_ev_formula() -> None:
    assert p6.enterprise_value(100.0, 30.0, 10.0) == 120.0


def test_market_cap_formula() -> None:
    assert p6.market_cap(12.5, 4.0) == 50.0


def test_net_debt_formula() -> None:
    assert p6.net_debt(30.0, 45.0) == -15.0


def test_negative_ebit_meaningfulness_policy() -> None:
    out = p6.positive_denominator_ratio(100.0, -5.0)
    assert out.status == "NOT_MEANINGFUL_NEGATIVE_DENOMINATOR"
    assert out.value is None


def test_negative_ebitda_meaningfulness() -> None:
    out = p6.positive_denominator_ratio(100.0, -1.0)
    assert out.status == "NOT_MEANINGFUL_NEGATIVE_DENOMINATOR"


def test_null_not_treated_as_zero() -> None:
    assert p6.market_cap(10.0, None) is None
    assert p6.enterprise_value(100.0, None, 10.0) is None


def test_ratio_near_zero_denominator_guard() -> None:
    out = p6.positive_denominator_ratio(100.0, 0.0)
    assert out.status == "NOT_MEANINGFUL_NEAR_ZERO_DENOMINATOR"


def test_readiness_metric_specific() -> None:
    row = sample_ttm_row()
    row["ttm_ebitda"] = None
    row["ebitda_4q_ready"] = 0
    ready = p6.score_readiness(row)
    assert ready["primary_score_ready"] == 1
    assert ready["secondary_ebitda_available"] == 0


def test_primary_score_readiness() -> None:
    assert p6.score_readiness(sample_ttm_row())["primary_score_ready"] == 1


def test_secondary_metric_does_not_block_primary() -> None:
    row = sample_ttm_row()
    row["ebitda_4q_ready"] = 0
    assert p6.score_readiness(row)["secondary_blocks_primary"] == 0


def test_model_versioning() -> None:
    assert p6.TTM_MODEL_VERSION == "V3_TTM_EBIT_FIRST_V1"
    assert p6.SCORE_MODEL_VERSION.startswith("V3_SCORE_EBIT_FIRST")


def test_no_production_score_writes(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    before = p6.table_count(db, "v3_score")
    p6.run_phase6_design(v3_db=db, osakedata_db=None, artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert p6.table_count(db, "v3_score") == before


def test_no_production_valuation_writes(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    before = p6.table_count(db, "v3_valuation")
    p6.run_phase6_design(v3_db=db, osakedata_db=None, artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert p6.table_count(db, "v3_valuation") == before


def test_no_ttm_writes(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    before = p6.table_count(db, "v3_ttm")
    p6.run_phase6_design(v3_db=db, osakedata_db=None, artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert p6.table_count(db, "v3_ttm") == before


def test_no_canonical_writes(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    before = p6.production_write_counts(db)["canonical"]
    p6.run_phase6_design(v3_db=db, osakedata_db=None, artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert p6.production_write_counts(db)["canonical"] == before


def test_quick_check(tmp_path: Path) -> None:
    assert p6.structural_checks(fixture_db(tmp_path))["quick_check"] == "ok"


def test_fk_check(tmp_path: Path) -> None:
    assert p6.structural_checks(fixture_db(tmp_path))["foreign_key_check_rows"] == 0


def sample_ttm_row() -> dict[str, float | int | str | None]:
    return {
        "ticker": "AAA",
        "company_id": 1,
        "period_end": "2025-12-31",
        "revenue_4q_ready": 1,
        "ebit_4q_ready": 1,
        "ebitda_4q_ready": 1,
        "fcf_4q_ready": 1,
        "ttm_revenue": 100.0,
        "ttm_ebit": 20.0,
        "ttm_ebitda": 25.0,
        "ttm_fcf": 12.0,
        "ttm_net_income": 10.0,
        "cash": 5.0,
        "total_debt": 15.0,
        "shares_outstanding": 10.0,
    }


def fixture_db(tmp_path: Path) -> Path:
    db = tmp_path / "v3.db"
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT NOT NULL);
            CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id));
            CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY REFERENCES v3_quarter(quarter_id), revenue REAL);
            CREATE TABLE v3_ttm(
                ttm_id INTEGER PRIMARY KEY,
                company_id INTEGER NOT NULL REFERENCES v3_company(company_id),
                endpoint_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
                endpoint_fiscal_year INTEGER NOT NULL,
                endpoint_fiscal_quarter TEXT NOT NULL,
                period_end TEXT,
                model_version TEXT NOT NULL,
                ttm_revenue REAL,
                ttm_gross_profit REAL,
                ttm_operating_income REAL,
                ttm_ebit REAL,
                ttm_ebitda REAL,
                ttm_net_income REAL,
                ttm_ocf REAL,
                ttm_capex REAL,
                ttm_fcf REAL,
                cash REAL,
                total_debt REAL,
                shares_outstanding REAL,
                revenue_4q_ready INTEGER NOT NULL,
                gross_profit_4q_ready INTEGER NOT NULL,
                operating_income_4q_ready INTEGER NOT NULL,
                ebit_4q_ready INTEGER NOT NULL,
                ebitda_4q_ready INTEGER NOT NULL,
                net_income_4q_ready INTEGER NOT NULL,
                ocf_4q_ready INTEGER NOT NULL,
                capex_4q_ready INTEGER NOT NULL,
                fcf_4q_ready INTEGER NOT NULL,
                ttm_ebit_primary_ready INTEGER NOT NULL,
                ttm_ebitda_secondary_ready INTEGER NOT NULL,
                core_ttm_ebit_ready INTEGER NOT NULL,
                core_ttm_ebitda_ready INTEGER NOT NULL,
                ttm_available_date TEXT,
                ttm_pit_ready INTEGER NOT NULL,
                underlying_publish_dates_complete INTEGER NOT NULL,
                q1_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
                q2_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
                q3_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
                q4_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id),
                calculation_version TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                output_json TEXT,
                run_id TEXT NOT NULL,
                calculated_at_utc TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id), as_of_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id), score_model_version TEXT NOT NULL, score_ready INTEGER NOT NULL, fundamental_score REAL, output_json TEXT, run_id TEXT, created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL);
            CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES v3_company(company_id), valuation_date TEXT NOT NULL, model_version TEXT NOT NULL, valuation_ready INTEGER NOT NULL, output_json TEXT, run_id TEXT, created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL);
            INSERT INTO v3_company(company_id,ticker) VALUES (1,'AAA');
            INSERT INTO v3_quarter(quarter_id,company_id) VALUES (10,1),(11,1),(12,1),(13,1);
            INSERT INTO v3_quarter_fundamentals(quarter_id,revenue) VALUES (10,1.0);
            INSERT INTO v3_ttm(company_id,endpoint_quarter_id,endpoint_fiscal_year,endpoint_fiscal_quarter,period_end,model_version,ttm_revenue,ttm_ebit,ttm_ebitda,ttm_net_income,ttm_fcf,cash,total_debt,shares_outstanding,revenue_4q_ready,gross_profit_4q_ready,operating_income_4q_ready,ebit_4q_ready,ebitda_4q_ready,net_income_4q_ready,ocf_4q_ready,capex_4q_ready,fcf_4q_ready,ttm_ebit_primary_ready,ttm_ebitda_secondary_ready,core_ttm_ebit_ready,core_ttm_ebitda_ready,ttm_pit_ready,underlying_publish_dates_complete,q1_quarter_id,q2_quarter_id,q3_quarter_id,q4_quarter_id,calculation_version,source_fingerprint,run_id,calculated_at_utc,created_at_utc,updated_at_utc)
            VALUES (1,13,2025,'Q4','2025-12-31','V3_TTM_EBIT_FIRST_V1',100,20,25,10,12,5,15,10,1,0,0,1,1,1,0,0,1,1,1,1,1,1,1,10,11,12,13,'V3_TTM_EBIT_FIRST_V1','fp','R','N','N','N');
            """
        )
    return db
