from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals import v3_phase6cr_score_architecture_reconciliation as p6cr


def test_component_weights_sum_to_100() -> None:
    assert sum(int(c["max_score"]) for c in p6cr.COMPONENTS) == 100


def test_removed_valuation_components_are_not_in_legacy2_contract() -> None:
    component_ids = {c["component_id"] for c in p6cr.COMPONENTS}
    assert {"EV_EBIT", "FCF_YIELD", "EV_SALES", "NET_DEBT_TO_MARKET_CAP"}.isdisjoint(component_ids)


def test_no_component_uses_market_price() -> None:
    assert all("market" not in c["metric"].lower() and "price" not in c["metric"].lower() for c in p6cr.COMPONENTS)


def test_dataset_excludes_2020_and_2026(tmp_path: Path) -> None:
    years = {row["year"] for row in p6cr.build_dataset(fixture_db(tmp_path))}
    assert 2020 not in years
    assert 2026 not in years
    assert years == {2021, 2022, 2023, 2024, 2025}


def test_dataset_split_contract(tmp_path: Path) -> None:
    splits = {row["sample_split"] for row in p6cr.build_dataset(fixture_db(tmp_path))}
    assert splits == {"DEVELOPMENT_2021_2023", "VALIDATION_2024", "OOS_2025_LOCKED"}


def test_fit_uses_development_window_only(tmp_path: Path) -> None:
    dataset = p6cr.build_dataset(fixture_db(tmp_path))
    mappings = p6cr.fit_mappings(dataset)
    before = json.dumps(mappings, sort_keys=True, default=str)
    for row in dataset:
        if row["sample_split"] != "DEVELOPMENT_2021_2023":
            row["revenue_growth"] = 999.0
            row["ebit_margin"] = 999.0
    assert json.dumps(p6cr.fit_mappings(dataset), sort_keys=True, default=str) == before


def test_legacy2_scores_are_0_to_100(tmp_path: Path) -> None:
    dataset = p6cr.build_dataset(fixture_db(tmp_path))
    scored = p6cr.apply_model(dataset, p6cr.fit_mappings(dataset))
    values = [r["legacy2_score"] for r in scored if r["legacy2_score"] is not None]
    assert values
    assert min(values) >= 0
    assert max(values) <= 100


def test_cash_quality_combines_cash_conversion_ratios() -> None:
    assert p6cr.cash_quality(ocf=80.0, fcf=60.0, ebit=100.0) == 0.775


def test_cash_quality_ignores_negative_ebit_denominator() -> None:
    assert p6cr.cash_quality(ocf=80.0, fcf=60.0, ebit=-100.0) == 0.75


def test_balance_metric_uses_net_debt_without_market_cap() -> None:
    low_debt = p6cr.balance_metric(cash=50.0, debt=100.0, revenue=400.0, ebit=100.0, fcf=20.0)
    high_debt = p6cr.balance_metric(cash=0.0, debt=300.0, revenue=400.0, ebit=100.0, fcf=20.0)
    assert low_debt is not None and high_debt is not None
    assert low_debt > high_debt


def test_loss_making_balance_metric_uses_cash_runway() -> None:
    assert p6cr.balance_metric(cash=120.0, debt=0.0, revenue=100.0, ebit=-10.0, fcf=-40.0) == 3.0


def test_consistency_requires_history() -> None:
    assert p6cr.consistency_metric([base_ttm_row(1, 1, "2021-03-31")]) is None


def test_consistency_scores_positive_persistence() -> None:
    history = [base_ttm_row(1, idx, f"2021-{month:02d}-28", revenue=100 + idx, ebit=20 + idx) for idx, month in enumerate([3, 6, 9, 12], 1)]
    assert p6cr.consistency_metric(history) is not None


def test_dilution_lower_is_better(tmp_path: Path) -> None:
    dataset = p6cr.build_dataset(fixture_db(tmp_path))
    mappings = p6cr.fit_mappings(dataset)
    comp = next(c for c in p6cr.COMPONENTS if c["component_id"] == "DILUTION")
    low, _ = p6cr.score_component({**dataset[8], "share_change_12m": 0.0}, comp, mappings["DILUTION"])
    high, _ = p6cr.score_component({**dataset[8], "share_change_12m": 0.50}, comp, mappings["DILUTION"])
    assert low is not None and high is not None
    assert low >= high


def test_positive_only_margin_scores_negative_as_zero(tmp_path: Path) -> None:
    dataset = p6cr.build_dataset(fixture_db(tmp_path))
    mappings = p6cr.fit_mappings(dataset)
    comp = next(c for c in p6cr.COMPONENTS if c["component_id"] == "EBIT_MARGIN")
    score, status = p6cr.score_component({**dataset[8], "ebit_margin": -0.1}, comp, mappings["EBIT_MARGIN"])
    assert score == 0
    assert status == "BAD_ECONOMIC_VALUE"


def test_applicability_flags_reit_name() -> None:
    assert p6cr.applicability({"profile": "ORDINARY", "company_name": "Example REIT"}) == "STANDARD_MODEL_WITH_LIMITATIONS_REIT_REVIEW"


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    summary = p6cr.run_phase6cr_reconciliation(v3_db=fixture_db(tmp_path), artifact_root=root, write_durable_docs=False)
    assert summary["classification"] == p6cr.CLASSIFICATION_COMPLETE
    for name in [
        "legacy_score_inventory.csv",
        "valuation_removal_decision.md",
        "legacy2_final_component_contract.csv",
        "phase6cr_score_fingerprint.json",
        "phase6e_locked_legacy2_score_model.json",
        "phase6cr_summary.json",
    ]:
        assert (root / name).exists()


def test_run_has_zero_production_writes(tmp_path: Path) -> None:
    summary = p6cr.run_phase6cr_reconciliation(v3_db=fixture_db(tmp_path), artifact_root=tmp_path / "artifacts", write_durable_docs=False)
    assert summary["production_writes"] == {"score": 0, "valuation": 0, "lifecycle": 0, "ttm": 0, "canonical": 0}


def test_locked_model_declares_no_2026_or_market_price(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    p6cr.run_phase6cr_reconciliation(v3_db=fixture_db(tmp_path), artifact_root=root, write_durable_docs=False)
    model = json.loads((root / "phase6e_locked_legacy2_score_model.json").read_text())
    assert model["uses_market_price"] is False
    assert model["uses_2026"] is False
    assert model["uses_2020"] is False


def test_fingerprint_is_deterministic(tmp_path: Path) -> None:
    db = fixture_db(tmp_path)
    first = p6cr.run_phase6cr_reconciliation(v3_db=db, artifact_root=tmp_path / "a", write_durable_docs=False)["fingerprint"]
    second = p6cr.run_phase6cr_reconciliation(v3_db=db, artifact_root=tmp_path / "b", write_durable_docs=False)["fingerprint"]
    assert first == second


def fixture_db(tmp_path: Path) -> Path:
    db = tmp_path / "v3.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE v3_company (
            company_id INTEGER PRIMARY KEY,
            market TEXT NOT NULL,
            ticker TEXT NOT NULL,
            company_name TEXT,
            profile TEXT NOT NULL,
            active INTEGER NOT NULL,
            admission_source TEXT NOT NULL,
            admission_evidence TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        CREATE TABLE v3_score (score_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_valuation (valuation_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_quarter (quarter_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_quarter_fundamentals (fundamentals_id INTEGER PRIMARY KEY);
        CREATE TABLE v3_ttm (
            ttm_id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL,
            endpoint_quarter_id INTEGER NOT NULL,
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
            q1_quarter_id INTEGER NOT NULL,
            q2_quarter_id INTEGER NOT NULL,
            q3_quarter_id INTEGER NOT NULL,
            q4_quarter_id INTEGER NOT NULL,
            calculation_version TEXT NOT NULL,
            source_fingerprint TEXT NOT NULL,
            output_json TEXT,
            run_id TEXT NOT NULL,
            calculated_at_utc TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO v3_company VALUES (1,'us','AAA','AAA Corp','ORDINARY',1,'test',NULL,'now','now')"
    )
    conn.execute(
        "INSERT INTO v3_company VALUES (2,'us','BBB','BBB Corp','ORDINARY',1,'test',NULL,'now','now')"
    )
    quarter_id = 1
    for company_id, revenue_base, ebit_base, fcf_base, shares_base in [(1, 100.0, 10.0, 5.0, 100.0), (2, 80.0, -8.0, -4.0, 50.0)]:
        for year in range(2020, 2027):
            for q, date in enumerate(["03-31", "06-30", "09-30", "12-31"], 1):
                growth = (year - 2020) * 4 + q
                revenue = revenue_base + growth * (2 if company_id == 1 else 1)
                ebit = ebit_base + growth * (1.5 if company_id == 1 else 0.8)
                fcf = fcf_base + growth * (0.8 if company_id == 1 else 0.4)
                insert_ttm(conn, base_ttm_row(company_id, quarter_id, f"{year}-{date}", revenue=revenue, ebit=ebit, fcf=fcf, shares=shares_base + growth))
                quarter_id += 1
    conn.commit()
    conn.close()
    return db


def base_ttm_row(
    company_id: int,
    quarter_id: int,
    period_end: str,
    *,
    revenue: float = 100.0,
    ebit: float = 10.0,
    fcf: float = 5.0,
    shares: float = 100.0,
) -> dict[str, object]:
    year = int(period_end[:4])
    quarter = {"03-31": "Q1", "06-30": "Q2", "09-30": "Q3", "12-31": "Q4"}.get(period_end[5:], "Q4")
    return {
        "ttm_id": quarter_id,
        "company_id": company_id,
        "endpoint_quarter_id": quarter_id,
        "endpoint_fiscal_year": year,
        "endpoint_fiscal_quarter": quarter,
        "period_end": period_end,
        "model_version": "test",
        "ttm_revenue": revenue,
        "ttm_gross_profit": revenue * 0.5,
        "ttm_operating_income": ebit,
        "ttm_ebit": ebit,
        "ttm_ebitda": ebit + 2,
        "ttm_net_income": ebit * 0.7,
        "ttm_ocf": max(fcf + 4, 0.1),
        "ttm_capex": -4.0,
        "ttm_fcf": fcf,
        "cash": 40.0,
        "total_debt": 20.0,
        "shares_outstanding": shares,
        "revenue_4q_ready": 1,
        "gross_profit_4q_ready": 1,
        "operating_income_4q_ready": 1,
        "ebit_4q_ready": 1,
        "ebitda_4q_ready": 1,
        "net_income_4q_ready": 1,
        "ocf_4q_ready": 1,
        "capex_4q_ready": 1,
        "fcf_4q_ready": 1,
        "ttm_ebit_primary_ready": 1,
        "ttm_ebitda_secondary_ready": 1,
        "core_ttm_ebit_ready": 1,
        "core_ttm_ebitda_ready": 1,
        "ttm_available_date": period_end,
        "ttm_pit_ready": 1,
        "underlying_publish_dates_complete": 1,
        "q1_quarter_id": quarter_id,
        "q2_quarter_id": quarter_id,
        "q3_quarter_id": quarter_id,
        "q4_quarter_id": quarter_id,
        "calculation_version": "test",
        "source_fingerprint": "test",
        "output_json": "{}",
        "run_id": "test",
        "calculated_at_utc": "now",
        "created_at_utc": "now",
        "updated_at_utc": "now",
    }


def insert_ttm(conn: sqlite3.Connection, row: dict[str, object]) -> None:
    fields = list(row)
    conn.execute(
        f"INSERT INTO v3_ttm ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",
        [row[f] for f in fields],
    )
