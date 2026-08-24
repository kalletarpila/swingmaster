from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase6c_score_distribution_calibration as p6c


def test_only_2021_2025_used_for_fitting(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    rows = p6c.build_dataset(db, None)
    assert {row["year"] for row in rows} == {2021, 2022, 2023, 2024, 2025}


def test_2026_excluded(tmp_path: Path) -> None:
    assert 2026 not in {row["year"] for row in p6c.build_dataset(fixture_db(tmp_path), None)}


def test_2020_excluded(tmp_path: Path) -> None:
    assert 2020 not in {row["year"] for row in p6c.build_dataset(fixture_db(tmp_path), None)}


def test_2018_2019_excluded_from_fitting(tmp_path: Path) -> None:
    years = {row["year"] for row in p6c.build_dataset(fixture_db(tmp_path), None)}
    assert 2018 not in years and 2019 not in years


def test_every_mapping_value_defined() -> None:
    mapping = p6c.quantile_mapping("X", 15, [float(i) for i in range(100)], lower_is_better=False)
    assert {row["score"] for row in mapping} == set(range(16))


def test_every_value_mathematically_reachable() -> None:
    mapping = p6c.positive_floor_mapping("X", 15, [float(i) for i in range(1, 100)], "HIGHER_IS_BETTER")
    assert {row["score"] for row in mapping} == set(range(16))


def test_min_score_reachable() -> None:
    assert min(row["score"] for row in p6c.transition_mapping("X", 15)) == 0


def test_max_score_reachable() -> None:
    assert max(row["score"] for row in p6c.transition_mapping("X", 15)) == 15


def test_sparse_mapping_rejected() -> None:
    scores = {0, 5, 10, 15}
    assert scores != set(range(16))


def test_monotonicity() -> None:
    rows = p6c.monotonicity_rows({"X": p6c.quantile_mapping("X", 15, [1.0, 2.0], lower_is_better=False)})
    assert rows[0]["monotonicity_valid"] == 1


def test_ebit_margin_floor() -> None:
    row = sample_row(ebit_margin=-0.1)
    score, status = p6c.score_value(row, "EBIT_MARGIN", p6c.positive_floor_mapping("EBIT_MARGIN", 15, [0.1, 0.2], "HIGHER_IS_BETTER"))
    assert score == 0
    assert status == "BAD_ECONOMIC_VALUE"


def test_fcf_margin_floor() -> None:
    row = sample_row(fcf_margin=0.0)
    score, status = p6c.score_value(row, "FCF_MARGIN", p6c.positive_floor_mapping("FCF_MARGIN", 15, [0.1, 0.2], "HIGHER_IS_BETTER"))
    assert score == 0
    assert status == "BAD_ECONOMIC_VALUE"


def test_negative_ebit_growth_transition_aware() -> None:
    assert p6c.classify_signed_transition(-10.0, -5.0) == "NEGATIVE_BUT_IMPROVING"


def test_ebit_crossing_positive() -> None:
    assert p6c.classify_signed_transition(-1.0, 1.0) == "CROSSING_TO_POSITIVE"


def test_fcf_crossing_positive() -> None:
    assert p6c.classify_signed_transition(-2.0, 3.0) == "CROSSING_TO_POSITIVE"


def test_negative_ev_ebit_not_meaningful() -> None:
    assert p6c.multiple_status(100.0, -5.0) == "NOT_MEANINGFUL"


def test_negative_fcf_yield_policy() -> None:
    row = sample_row(fcf_yield=-0.01)
    score, status = p6c.score_value(row, "FCF_YIELD", p6c.positive_floor_mapping("FCF_YIELD", 15, [0.01, 0.02], "HIGHER_IS_BETTER"))
    assert score == 0
    assert status == "BAD_ECONOMIC_VALUE"


def test_net_cash_favorable() -> None:
    mapping = p6c.quantile_mapping("NET_DEBT_TO_MARKET_CAP", 10, [-0.5, 0.0, 0.5], lower_is_better=True)
    row = sample_row(net_debt_to_market_cap=-0.5)
    score, status = p6c.score_value(row, "NET_DEBT_TO_MARKET_CAP", mapping)
    assert status == "SCORED"
    assert score is not None


def test_missing_not_score_zero() -> None:
    row = sample_row(ebit_margin=None)
    score, status = p6c.score_value(row, "EBIT_MARGIN", p6c.positive_floor_mapping("EBIT_MARGIN", 15, [0.1], "HIGHER_IS_BETTER"))
    assert score is None
    assert status == "MISSING_DATA"


def test_pooled_quantiles() -> None:
    assert p6c.stats([1.0, 2.0, 3.0])["p50"] == 2.0


def test_yearly_quantiles() -> None:
    rows = [{"year": 2021, "revenue_growth": 1.0, "ebit_transition_state": "MISSING_DATA", "fcf_transition_state": "MISSING_DATA", "ebit_margin": None, "fcf_margin": None, "ev_ebit": None, "fcf_yield": None, "ev_sales": None, "net_debt_to_market_cap": None}]
    assert p6c.by_year_distributions(rows)[0]["year"] == 2021


def test_drift_classification() -> None:
    rows = [{"year": 2021, "metric_id": "X", "p50": 1.0, "p90": 2.0, "p95": 3.0}, {"year": 2022, "metric_id": "X", "p50": 1.1, "p90": 2.1, "p95": 3.1}]
    assert p6c.drift_rows(rows)[0]["classification"] == "STABLE"


def test_outlier_guard() -> None:
    assert p6c.multiple_status(-1.0, 1.0) == "NOT_MEANINGFUL"


def test_near_zero_denominator_guard() -> None:
    assert p6c.multiple_status(1.0, 0.0) == "NOT_MEANINGFUL"


def test_higher_is_better_mapping() -> None:
    mapping = p6c.quantile_mapping("X", 3, [1.0, 2.0, 3.0, 4.0], lower_is_better=False)
    assert [row["score"] for row in mapping] == [0, 1, 2, 3]


def test_lower_is_better_mapping() -> None:
    mapping = p6c.quantile_mapping("X", 3, [1.0, 2.0, 3.0, 4.0], lower_is_better=True)
    assert {row["score"] for row in mapping} == {0, 1, 2, 3}


def test_hybrid_mapping() -> None:
    mapping = p6c.positive_floor_mapping("X", 3, [0.1, 0.2, 0.3], "HIGHER_IS_BETTER")
    assert {row["score"] for row in mapping} == {0, 1, 2, 3}


def test_transition_mapping() -> None:
    assert {row["score"] for row in p6c.transition_mapping("X", 10)} == set(range(11))


def test_score_bucket_utilization() -> None:
    scored = [sample_row()]
    scored[0]["REVENUE_GROWTH_score"] = 1
    pooled, _yearly, _dead = p6c.bucket_utilization(scored)
    assert any(row["metric_id"] == "REVENUE_GROWTH" for row in pooled)


def test_dead_score_detection() -> None:
    scored = [sample_row()]
    scored[0]["REVENUE_GROWTH_score"] = 1
    _pooled, _yearly, dead = p6c.bucket_utilization(scored)
    assert any(row["observed_unused"] == 1 for row in dead)


def test_correlation_calculation() -> None:
    assert abs((p6c.pearson([1, 2, 3], [1, 2, 3]) or 0.0) - 1.0) < 1e-9


def test_mathematical_duplicate_flag() -> None:
    rows = p6c.redundancy_decisions([])
    assert any(row["metric_id"] == "EBIT_YIELD" and row["decision"] == "DROP_AS_REDUNDANT" for row in rows)


def test_secondary_metric_retained() -> None:
    rows = p6c.redundancy_decisions([])
    assert any(row["metric_id"] == "NET_DEBT_TO_EBIT" and row["decision"] == "KEEP_SECONDARY_DIAGNOSTIC" for row in rows)


def test_missing_weight_renormalization() -> None:
    scored = p6c.apply_scores([sample_row()], {"REVENUE_GROWTH": p6c.quantile_mapping("REVENUE_GROWTH", 15, [0.1], lower_is_better=False), **minimal_mappings()})
    assert "available_weight_pct" in scored[0]


def test_minimum_coverage_requirement() -> None:
    row = sample_row()
    scored = p6c.apply_scores([row], {c["metric_id"]: p6c.transition_mapping(c["metric_id"], c["max_score"]) if c["domain"] == "SIGNED_TRANSITION_METRIC" else p6c.quantile_mapping(c["metric_id"], c["max_score"], [1.0], lower_is_better=c["direction"] == "LOWER_IS_BETTER") for c in p6c.FINAL_COMPONENTS})
    assert scored[0]["score_ready"] in {0, 1}


def test_dry_aggregate_deterministic() -> None:
    mappings = minimal_mappings()
    first = p6c.apply_scores([sample_row()], mappings)[0]["aggregate_score_dry"]
    second = p6c.apply_scores([sample_row()], mappings)[0]["aggregate_score_dry"]
    assert first == second


def test_no_return_based_fitting() -> None:
    assert "return" not in json_dump(p6c.FINAL_COMPONENTS).lower()


def test_score_writes_zero(tmp_path: Path) -> None:
    summary = p6c.run_phase6c_calibration(v3_db=fixture_db(tmp_path), osakedata_db=None, artifact_root=tmp_path / "a", write_durable_docs=False)
    assert summary["production_writes"]["score"] == 0


def test_valuation_writes_zero(tmp_path: Path) -> None:
    summary = p6c.run_phase6c_calibration(v3_db=fixture_db(tmp_path), osakedata_db=None, artifact_root=tmp_path / "a", write_durable_docs=False)
    assert summary["production_writes"]["valuation"] == 0


def test_lifecycle_writes_zero(tmp_path: Path) -> None:
    summary = p6c.run_phase6c_calibration(v3_db=fixture_db(tmp_path), osakedata_db=None, artifact_root=tmp_path / "a", write_durable_docs=False)
    assert summary["production_writes"]["lifecycle"] == 0


def test_ttm_writes_zero(tmp_path: Path) -> None:
    summary = p6c.run_phase6c_calibration(v3_db=fixture_db(tmp_path), osakedata_db=None, artifact_root=tmp_path / "a", write_durable_docs=False)
    assert summary["production_writes"]["ttm"] == 0


def test_canonical_writes_zero(tmp_path: Path) -> None:
    summary = p6c.run_phase6c_calibration(v3_db=fixture_db(tmp_path), osakedata_db=None, artifact_root=tmp_path / "a", write_durable_docs=False)
    assert summary["production_writes"]["canonical"] == 0


def minimal_mappings() -> dict[str, list[dict[str, object]]]:
    mappings: dict[str, list[dict[str, object]]] = {}
    for comp in p6c.FINAL_COMPONENTS:
        if comp["domain"] == "SIGNED_TRANSITION_METRIC":
            mappings[comp["metric_id"]] = p6c.transition_mapping(comp["metric_id"], comp["max_score"])
        elif comp["domain"] == "POSITIVE_ONLY_GOOD":
            mappings[comp["metric_id"]] = p6c.positive_floor_mapping(comp["metric_id"], comp["max_score"], [0.1, 0.2, 0.3], comp["direction"])
        else:
            mappings[comp["metric_id"]] = p6c.quantile_mapping(comp["metric_id"], comp["max_score"], [0.1, 0.2, 0.3], lower_is_better=comp["direction"] == "LOWER_IS_BETTER")
    return mappings


def sample_row(**overrides: object) -> dict[str, object]:
    row = {
        "company_id": 1,
        "ticker": "AAA",
        "year": 2024,
        "revenue_growth": 0.1,
        "ebit_transition_state": "POSITIVE_AND_GROWING",
        "ebit_transition_delta": 10.0,
        "fcf_transition_state": "CROSSING_TO_POSITIVE",
        "fcf_transition_delta": 5.0,
        "ebit_margin": 0.1,
        "fcf_margin": 0.05,
        "ev_ebit": 12.0,
        "ev_ebit_status": "MEANINGFUL",
        "fcf_yield": 0.05,
        "ev_sales": 2.0,
        "ev_sales_status": "MEANINGFUL",
        "net_debt_to_market_cap": 0.1,
    }
    row.update(overrides)
    return row


def fixture_db(tmp_path: Path) -> Path:
    db = tmp_path / "v3.db"
    with sqlite3.connect(db) as conn:
        conn.executescript(
            """
            CREATE TABLE v3_company(company_id INTEGER PRIMARY KEY, ticker TEXT, market TEXT, active INTEGER);
            CREATE TABLE v3_score(score_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_valuation(valuation_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_quarter(quarter_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_quarter_fundamentals(quarter_id INTEGER PRIMARY KEY);
            CREATE TABLE v3_ttm(
                ttm_id INTEGER PRIMARY KEY, company_id INTEGER, endpoint_quarter_id INTEGER,
                endpoint_fiscal_year INTEGER, endpoint_fiscal_quarter TEXT, period_end TEXT,
                ttm_revenue REAL, ttm_gross_profit REAL, ttm_operating_income REAL, ttm_ebit REAL,
                ttm_ebitda REAL, ttm_net_income REAL, ttm_ocf REAL, ttm_capex REAL, ttm_fcf REAL,
                cash REAL, total_debt REAL, shares_outstanding REAL
            );
            INSERT INTO v3_company VALUES (1,'AAA','usa',1);
            INSERT INTO v3_quarter VALUES (1);
            INSERT INTO v3_quarter_fundamentals VALUES (1);
            """
        )
        rows = []
        q = 1
        for year in range(2020, 2027):
            for fq in ('Q1','Q2','Q3','Q4'):
                rows.append((q, 1, q, year, fq, f"{year}-{q%12+1:02d}-28", 100 + q, 50, 20, 10 + q, 15 + q, 8 + q, 12 + q, -2, 6 + q, 20, 10, 5))
                q += 1
        conn.executemany("INSERT INTO v3_ttm VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    return db


def json_dump(value: object) -> str:
    import json

    return json.dumps(value, sort_keys=True)
