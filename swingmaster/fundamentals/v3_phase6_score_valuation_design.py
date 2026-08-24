from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CLASSIFICATION_COMPLETE = "FUNDAMENTALS_V3_PHASE6_SCORE_VALUATION_DESIGN_COMPLETE_READY_FOR_PRODUCTION_IMPLEMENTATION"
NEXT_PHASE = "MASTER PLAN PHASE 6B - SCORE & VALUATION PRODUCTION IMPLEMENTATION"
TTM_MODEL_VERSION = "V3_TTM_EBIT_FIRST_V1"
VALUATION_MODEL_VERSION = "V3_VALUATION_EBIT_FIRST_V1"
SCORE_MODEL_VERSION = "V3_SCORE_EBIT_FIRST_V1"
NEAR_ZERO_EPSILON = 1e-9
CORE_FIELDS = ("revenue", "ebit", "free_cashflow", "cash", "total_debt", "shares_outstanding")
SECONDARY_FIELDS = ("gross_profit", "operating_income", "ebitda", "net_income", "operating_cashflow", "capex")
ALL_FUNDAMENTAL_FIELDS = CORE_FIELDS + SECONDARY_FIELDS


@dataclass(frozen=True)
class RatioMeaning:
    status: str
    value: float | None


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def market_cap(price: float | None, shares_outstanding: float | None) -> float | None:
    if price is None or shares_outstanding is None or shares_outstanding <= 0:
        return None
    return float(price * shares_outstanding)


def net_debt(total_debt: float | None, cash: float | None) -> float | None:
    if total_debt is None or cash is None:
        return None
    return float(total_debt - cash)


def enterprise_value(market_cap_value: float | None, total_debt: float | None, cash: float | None) -> float | None:
    debt = net_debt(total_debt, cash)
    if market_cap_value is None or debt is None:
        return None
    return float(market_cap_value + debt)


def positive_denominator_ratio(numerator: float | None, denominator: float | None) -> RatioMeaning:
    if numerator is None or denominator is None:
        return RatioMeaning("MISSING_INPUT", None)
    if abs(float(denominator)) <= NEAR_ZERO_EPSILON:
        return RatioMeaning("NOT_MEANINGFUL_NEAR_ZERO_DENOMINATOR", None)
    if denominator <= 0:
        return RatioMeaning("NOT_MEANINGFUL_NEGATIVE_DENOMINATOR", None)
    return RatioMeaning("MEANINGFUL", float(numerator / denominator))


def signed_ratio(numerator: float | None, denominator: float | None) -> RatioMeaning:
    if numerator is None or denominator is None:
        return RatioMeaning("MISSING_INPUT", None)
    if abs(float(denominator)) <= NEAR_ZERO_EPSILON:
        return RatioMeaning("NOT_MEANINGFUL_NEAR_ZERO_DENOMINATOR", None)
    return RatioMeaning("MEANINGFUL", float(numerator / denominator))


def formula_inventory() -> list[dict[str, Any]]:
    return [
        inv("TTM_REVENUE", "swingmaster/fundamentals/v3_phase5_ttm_engine.py", "ttm_revenue", "sum 4 contiguous quarters revenue", "revenue", "KEEP_AS_IS"),
        inv("TTM_EBIT", "swingmaster/fundamentals/v3_phase5_ttm_engine.py", "ttm_ebit", "sum 4 contiguous quarters EBIT", "ebit", "KEEP_AS_IS"),
        inv("TTM_EBITDA", "swingmaster/fundamentals/v3_phase5_ttm_engine.py", "ttm_ebitda", "sum 4 contiguous quarters EBITDA", "ebitda", "KEEP_EBITDA_SECONDARY"),
        inv("TTM_FCF", "swingmaster/fundamentals/v3_phase5_ttm_engine.py", "ttm_fcf", "sum 4 contiguous quarters FCF", "free_cashflow", "KEEP_AS_IS"),
        inv("MARKET_CAP", "swingmaster/cli/run_fundamental_valuation.py", "market_cap", "close price * endpoint shares_outstanding", "price,shares_outstanding", "KEEP_AS_IS"),
        inv("ENTERPRISE_VALUE", "swingmaster/cli/run_fundamental_valuation.py", "enterprise_value", "market_cap + total_debt - cash", "price,shares_outstanding,total_debt,cash", "KEEP_AS_IS"),
        inv("EV_EBIT", "swingmaster/cli/run_fundamental_valuation.py", "valuation_ev_ebit", "enterprise_value / TTM EBIT when EBIT > 0", "ebit", "KEEP_AS_IS"),
        inv("EV_EBITDA", "swingmaster/cli/run_fundamental_valuation.py", "valuation_ev_ebitda", "enterprise_value / TTM EBITDA when EBITDA > 0", "ebitda", "KEEP_EBITDA_SECONDARY"),
        inv("FCF_YIELD", "swingmaster/cli/run_fundamental_valuation.py", "valuation_fcf_yield", "TTM FCF / market_cap", "free_cashflow,price,shares_outstanding", "KEEP_AS_IS"),
        inv("V2_PRIMARY_EV_MULTIPLE_SELECTOR", "swingmaster/cli/run_fundamental_valuation.py", "valuation_bucket", "uses EV/EBITDA first, then EV/EBIT fallback", "ebitda,ebit", "CHANGE_TO_EBIT"),
        inv("SCORE_GROWTH", "swingmaster/fundamentals/score.py", "growth_component", "revenue_growth_ttm_yoy thresholds", "revenue", "KEEP_AS_IS"),
        inv("SCORE_MARGIN", "swingmaster/fundamentals/score.py", "margin_component", "EBITDA margin thresholds", "ebitda", "CHANGE_TO_EBIT"),
        inv("SCORE_MARGIN_TREND", "swingmaster/fundamentals/score.py", "margin_trend_component", "EBITDA margin trend thresholds", "ebitda", "CHANGE_TO_EBIT"),
        inv("SCORE_FCF", "swingmaster/fundamentals/score.py", "fcf_component", "FCF margin thresholds", "free_cashflow,revenue", "KEEP_AS_IS"),
        inv("SCORE_LEVERAGE", "swingmaster/fundamentals/score.py", "leverage_component", "net_debt_to_ebitda preferred then net_debt_to_ebit fallback", "ebitda,ebit,cash,total_debt", "CHANGE_TO_EBIT"),
        inv("SCORE_CONSISTENCY", "swingmaster/fundamentals/score.py", "consistency_component", "CV of revenue growth, EBITDA margin, FCF margin", "revenue,ebitda,free_cashflow", "CHANGE_TO_EBIT"),
        inv("SCORE_DILUTION", "swingmaster/fundamentals/score.py", "dilution_component", "share dilution YoY thresholds", "shares_outstanding", "KEEP_AS_IS"),
        inv("SCORE_LIFECYCLE_SCALING", "swingmaster/fundamentals/score.py", "fundamental_score_lifecycle", "component multipliers by lifecycle", "score components", "REVISE_FORMULA"),
    ]


def inv(formula_id: str, path: str, output: str, formula: str, inputs: str, rec: str) -> dict[str, Any]:
    return {
        "formula_id": formula_id,
        "path": path,
        "output_field": output,
        "inputs": inputs,
        "formula": formula,
        "ttm_dependency": int("ttm" in formula.lower() or "margin" in output or "yield" in output),
        "ebit_dependency": int("ebit" in inputs.split(",")),
        "ebitda_dependency": int("ebitda" in inputs.split(",")),
        "revenue_dependency": int("revenue" in inputs.split(",")),
        "fcf_dependency": int("free_cashflow" in inputs.split(",")),
        "net_income_dependency": int("net_income" in inputs.split(",")),
        "price_dependency": int("price" in inputs.split(",")),
        "cash_debt_shares_dependency": int(any(x in inputs.split(",") for x in ("cash", "total_debt", "shares_outstanding"))),
        "old_availability_driven_assumption": "EBITDA-first" if "EBITDA" in formula or rec == "CHANGE_TO_EBIT" else "",
        "economically_intended": "EBIT primary; EBITDA retained only where explicitly secondary" if rec == "CHANGE_TO_EBIT" else "Preserve with locked role",
        "production_active": int(path.startswith("swingmaster/")),
        "recommendation": rec,
    }


def old_ebitda_first_inventory() -> list[dict[str, Any]]:
    return [row for row in formula_inventory() if row["old_availability_driven_assumption"]]


def score_readiness(row: dict[str, Any]) -> dict[str, int]:
    revenue_ready = row.get("revenue_4q_ready") == 1
    ebit_ready = row.get("ebit_4q_ready") == 1
    fcf_ready = row.get("fcf_4q_ready") == 1
    instant_ready = row.get("cash") is not None and row.get("total_debt") is not None and (row.get("shares_outstanding") or 0) > 0
    secondary_ebitda_ready = row.get("ebitda_4q_ready") == 1
    return {
        "primary_score_ready": int(revenue_ready and ebit_ready and fcf_ready and instant_ready),
        "secondary_ebitda_available": int(secondary_ebitda_ready),
        "secondary_blocks_primary": 0,
    }


def load_ttm_rows(v3_db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            """
            SELECT t.*, c.ticker
            FROM v3_ttm t
            JOIN v3_company c ON c.company_id = t.company_id
            ORDER BY t.company_id, t.endpoint_fiscal_year, t.endpoint_fiscal_quarter
            """
        )]


def schema_inventory(v3_db: Path, table: str) -> list[dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return [{"table_name": table, "row_count": count, "cid": row[0], "column_name": row[1], "type": row[2], "notnull": row[3], "default_value": row[4], "pk": row[5]} for row in rows]


def table_count(v3_db: Path, table: str) -> int:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def structural_checks(v3_db: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        return {"quick_check": conn.execute("PRAGMA quick_check").fetchone()[0], "foreign_key_check_rows": len(conn.execute("PRAGMA foreign_key_check").fetchall())}


def load_price_map(osakedata_db: Path | None, rows: list[dict[str, Any]]) -> dict[tuple[str, str], float]:
    if osakedata_db is None or not osakedata_db.exists():
        return {}
    by_ticker: dict[str, set[str]] = {}
    for row in rows:
        by_ticker.setdefault(str(row["ticker"]).upper(), set()).add(str(row["period_end"]))
    out: dict[tuple[str, str], float] = {}
    with sqlite3.connect(f"file:{osakedata_db}?mode=ro", uri=True) as conn:
        for ticker, dates in by_ticker.items():
            for period_end in dates:
                price = conn.execute(
                    "SELECT close FROM osakedata WHERE osake=? AND market='usa' AND pvm<=? ORDER BY pvm DESC LIMIT 1",
                    (ticker, period_end),
                ).fetchone()
                if price is not None and price[0] is not None:
                    out[(ticker, period_end)] = float(price[0])
    return out


def dry_coverage(ttm_rows: list[dict[str, Any]], price_map: dict[tuple[str, str], float]) -> dict[str, Any]:
    rows = []
    for row in ttm_rows:
        price = price_map.get((str(row["ticker"]).upper(), str(row["period_end"])))
        cap = market_cap(price, row.get("shares_outstanding"))
        ev = enterprise_value(cap, row.get("total_debt"), row.get("cash"))
        primary_ready = score_readiness(row)["primary_score_ready"] == 1
        rows.append({
            **row,
            "price_available": int(price is not None),
            "market_cap_available": int(cap is not None),
            "enterprise_value_available": int(ev is not None),
            "primary_score_ready": int(primary_ready),
            "ev_ebit_computable": int(ev is not None and row.get("ttm_ebit") is not None),
            "ev_ebit_meaningful": int(positive_denominator_ratio(ev, row.get("ttm_ebit")).status == "MEANINGFUL"),
            "ev_ebitda_computable": int(ev is not None and row.get("ttm_ebitda") is not None),
            "ev_ebitda_meaningful": int(positive_denominator_ratio(ev, row.get("ttm_ebitda")).status == "MEANINGFUL"),
            "fcf_yield_computable": int(cap is not None and row.get("ttm_fcf") is not None),
            "ev_sales_computable": int(ev is not None and row.get("ttm_revenue") is not None),
            "ev_sales_meaningful": int(positive_denominator_ratio(ev, row.get("ttm_revenue")).status == "MEANINGFUL"),
            "pe_computable": int(cap is not None and row.get("ttm_net_income") is not None),
            "pe_meaningful": int(positive_denominator_ratio(cap, row.get("ttm_net_income")).status == "MEANINGFUL"),
        })
    return {"rows": rows, "summary": summarize_coverage(rows)}


def summarize_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    latest_by_company: dict[int, dict[str, Any]] = {}
    for row in rows:
        latest_by_company[int(row["company_id"])] = row
    summary = {"total_endpoints": len(rows)}
    for key in ("price_available", "ev_ebit_computable", "ev_ebit_meaningful", "ev_ebitda_computable", "ev_ebitda_meaningful", "fcf_yield_computable", "ev_sales_computable", "ev_sales_meaningful", "pe_computable", "pe_meaningful", "primary_score_ready"):
        summary[key] = sum(int(row[key]) for row in rows)
    summary["companies_latest_primary_score_ready"] = sum(int(row["primary_score_ready"]) for row in latest_by_company.values())
    summary["ebit_primary_coverage_gain_vs_ebitda"] = summary["ev_ebit_computable"] - summary["ev_ebitda_computable"]
    return summary


def run_phase6_design(
    *,
    v3_db: Path,
    artifact_root: Path,
    osakedata_db: Path | None = Path("/home/kalle/projects/rawcandle/data/osakedata.db"),
    write_durable_docs: bool = True,
) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    before = production_write_counts(v3_db)
    ttm_rows = load_ttm_rows(v3_db)
    price_map = load_price_map(osakedata_db, ttm_rows)
    coverage = dry_coverage(ttm_rows, price_map)
    after = production_write_counts(v3_db)
    summary = {
        "classification": CLASSIFICATION_COMPLETE,
        "recommended_next_step": NEXT_PHASE,
        "run_at_utc": utc_now(),
        "artifact_root": str(artifact_root),
        "formula_inventory_rows": len(formula_inventory()),
        "old_ebitda_first_formulas": len(old_ebitda_first_inventory()),
        "score_schema_rows": table_count(v3_db, "v3_score"),
        "valuation_schema_rows": table_count(v3_db, "v3_valuation"),
        "production_writes": {
            "score": after["v3_score"] - before["v3_score"],
            "valuation": after["v3_valuation"] - before["v3_valuation"],
            "ttm": after["v3_ttm"] - before["v3_ttm"],
            "canonical": after["canonical"] - before["canonical"],
        },
        "coverage": coverage["summary"],
        "integrity": structural_checks(v3_db),
    }
    write_artifacts(artifact_root, v3_db, osakedata_db, coverage, summary)
    if write_durable_docs:
        write_doc(Path("docs/fundamentals_v3_phase6_score_valuation_engine_design.md"), summary)
        update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def production_write_counts(v3_db: Path) -> dict[str, int]:
    return {"v3_score": table_count(v3_db, "v3_score"), "v3_valuation": table_count(v3_db, "v3_valuation"), "v3_ttm": table_count(v3_db, "v3_ttm"), "canonical": table_count(v3_db, "v3_quarter") + table_count(v3_db, "v3_quarter_fundamentals")}


def write_artifacts(root: Path, v3_db: Path, osakedata_db: Path | None, coverage: dict[str, Any], summary: dict[str, Any]) -> None:
    inventory = formula_inventory()
    write_csv(root / "downstream_formula_inventory.csv", inventory)
    write_text(root / "downstream_dependency_graph.md", dependency_graph_md())
    write_csv(root / "old_ebitda_first_logic_inventory.csv", old_ebitda_first_inventory())
    write_csv(root / "score_schema_inventory.csv", schema_inventory(v3_db, "v3_score"))
    write_csv(root / "valuation_schema_inventory.csv", schema_inventory(v3_db, "v3_valuation"))
    write_text(root / "ebit_vs_ebitda_policy.md", ebit_policy_md())
    write_text(root / "valuation_contract.md", valuation_contract_md())
    write_text(root / "score_contract.md", score_contract_md())
    write_text(root / "readiness_policy.md", readiness_policy_md())
    write_text(root / "negative_denominator_policy.md", negative_policy_md())
    write_text(root / "null_handling_policy.md", null_policy_md())
    write_text(root / "outlier_policy.md", outlier_policy_md())
    write_text(root / "market_data_source_audit.md", market_audit_md(osakedata_db))
    write_text(root / "price_alignment_policy.md", price_alignment_md())
    write_text(root / "current_vs_historical_valuation_policy.md", current_historical_md())
    write_text(root / "pit_limitation.md", pit_limitation_md())
    rows = coverage["rows"]
    write_csv(root / "ev_ebit_dry_coverage.csv", select_cols(rows, "ticker", "period_end", "ev_ebit_computable", "ev_ebit_meaningful", "price_available"))
    write_csv(root / "ev_ebitda_dry_coverage.csv", select_cols(rows, "ticker", "period_end", "ev_ebitda_computable", "ev_ebitda_meaningful", "price_available"))
    write_csv(root / "fcf_yield_dry_coverage.csv", select_cols(rows, "ticker", "period_end", "fcf_yield_computable", "price_available"))
    write_csv(root / "ev_sales_dry_coverage.csv", select_cols(rows, "ticker", "period_end", "ev_sales_computable", "ev_sales_meaningful", "price_available"))
    write_csv(root / "pe_dry_coverage.csv", select_cols(rows, "ticker", "period_end", "pe_computable", "pe_meaningful", "price_available"))
    write_csv(root / "primary_score_dry_coverage.csv", select_cols(rows, "ticker", "period_end", "primary_score_ready"))
    write_csv(root / "ebit_vs_ebitda_coverage_comparison.csv", [{"metric": k, "value": v} for k, v in coverage["summary"].items()])
    write_text(root / "valuation_schema_design.md", valuation_schema_design_md())
    write_text(root / "score_schema_design.md", score_schema_design_md())
    write_text(root / "model_versioning_design.md", model_versioning_md())
    write_text(root / "phase6_production_implementation_plan.md", implementation_plan_md())
    write_json(root / "phase6_summary.json", summary)
    write_text(root / "recommended_next_step.md", NEXT_PHASE + "\n")


def select_cols(rows: list[dict[str, Any]], *cols: str) -> list[dict[str, Any]]:
    return [{col: row.get(col) for col in cols} for row in rows]


def dependency_graph_md() -> str:
    return """# Downstream Dependency Graph

- `v3_ttm` -> `v3_valuation`: TTM Revenue, EBIT, EBITDA, Net Income, OCF, FCF, endpoint cash/debt/shares.
- `v3_ttm` -> `v3_score`: primary score components use Revenue, EBIT, FCF, cash/debt/shares; secondary analytics keep all other fields.
- `rawcandle.osakedata` -> `v3_valuation`: latest close at or before selected valuation date.
- `v3_score` and `v3_valuation` are Phase 6B production outputs and remain empty in Phase 6 design.
"""


def ebit_policy_md() -> str:
    return "Primary operating earnings metric is TTM EBIT. EBITDA is retained as a secondary analytics and valuation metric and must not block EV/EBIT or primary score readiness.\n"


def valuation_contract_md() -> str:
    return """# Valuation Contract

- Market cap: `close_price * endpoint shares_outstanding`.
- Net debt: `total_debt - cash`.
- Enterprise value: `market_cap + total_debt - cash`.
- Primary: EV/EBIT, EBIT yield (`TTM EBIT / EV`), FCF yield (`TTM FCF / market_cap`), EV/Sales.
- Secondary: EV/EBITDA, P/E, EV/OCF where OCF is available.
- EV limitations: preferred stock, minority interest, lease adjustments and authoritative vendor market cap are not present in V3 inputs.
"""


def score_contract_md() -> str:
    return """# Score Contract

Primary score inputs are TTM Revenue growth, TTM EBIT margin/trend, TTM FCF margin, net debt/EBIT, share dilution, and lifecycle component. Secondary analytics retain gross/operating/EBITDA/net/OCF/capex metrics. Existing component thresholds can be reused in Phase 6B with EBIT substituted only where the old rule was EBITDA-first for availability.
"""


def readiness_policy_md() -> str:
    return "Readiness is metric-specific: EV_EBIT_READY, EV_EBITDA_READY, FCF_YIELD_READY, EV_SALES_READY, PE_READY, PRIMARY_SCORE_READY. Secondary EBITDA availability never blocks primary EBIT score or valuation readiness.\n"


def negative_policy_md() -> str:
    return "For EV/EBIT, EV/EBITDA, P/E, leverage and cash-conversion ratios, denominator <= 0 is NOT_MEANINGFUL for ranking/cheapness. Negative accounting values remain valid stored inputs. Near-zero denominators use epsilon 1e-9 and are excluded from ratio ranking.\n"


def null_policy_md() -> str:
    return "NULL inputs are missing, not zero. Metric output is NULL/unavailable unless its own contract explicitly allows computation. Score implementation should use partial normalized weights only when documented per component; no silent zero-fill.\n"


def outlier_policy_md() -> str:
    return "Phase 6B should apply deterministic per-metric winsorization/ranking after NOT_MEANINGFUL filtering. Existing absolute score component caps 0..100 are retained; no z-score reinterpretation is introduced in design.\n"


def market_audit_md(osakedata_db: Path | None) -> str:
    return f"Existing V2 valuation CLI reads OHLCV close prices from an external osakedata SQLite database. Detected path: `{osakedata_db}`. Table: `osakedata(osake,pvm,close,market,sector,industry)`. This remains read-only input owned by RawCandle.\n"


def price_alignment_md() -> str:
    return "Historical dry coverage uses latest trading close on or before the TTM endpoint `period_end`. Production current valuation should use latest close with latest available TTM. Full historical PIT valuation requires `ttm_available_date <= valuation_date` and price on or before valuation date.\n"


def current_historical_md() -> str:
    return "Current valuation and historical PIT valuation are separate modes. Current mode combines latest market price with latest available V3 TTM. Historical mode must not use future-published fundamentals and should persist the valuation date and selected price date.\n"


def pit_limitation_md() -> str:
    return "Phase 5 provides current canonical TTM rows plus publish-date availability metadata. It does not provide full historical-vintage restatement replay, so Phase 6 must label historical outputs as current-canonical-with-PIT-availability unless a later vintage layer is added.\n"


def valuation_schema_design_md() -> str:
    return "Use a rebuilt `v3_valuation` keyed by `(company_id, endpoint_quarter_id, valuation_date, valuation_model_version)` with explicit columns for price, price_date, market_cap, enterprise_value, net_debt, EV/EBIT, EV/EBITDA, FCF yield, EBIT yield, EV/Sales, P/E, readiness flags, denominator statuses, TTM model version, provenance JSON and timestamps.\n"


def score_schema_design_md() -> str:
    return "Use a rebuilt `v3_score` keyed by `(company_id, endpoint_quarter_id, score_model_version)` with explicit component columns, total score, readiness flags, missing component flags, TTM model version, score model version, output/provenance JSON and timestamps. Do not store only opaque JSON.\n"


def model_versioning_md() -> str:
    return f"TTM rows reference `{TTM_MODEL_VERSION}`. New valuation rows use `{VALUATION_MODEL_VERSION}` and score rows use `{SCORE_MODEL_VERSION}`. Formula changes require new model versions, not reinterpretation of old rows.\n"


def implementation_plan_md() -> str:
    return """# Phase 6B Implementation Plan

1. Rebuild empty `v3_valuation` and `v3_score` schemas with explicit columns.
2. Implement pure formula helpers and metric-specific readiness.
3. Implement read-only price lookup from RawCandle osakedata.
4. Add dry-run and apply CLIs with clean rebuild semantics.
5. Validate idempotency, row counts, quick_check/FK, and no canonical/TTM writes.
"""


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    c = summary["coverage"]
    path.write_text(
        f"""# Fundamentals V3 Phase 6 Score & Valuation Engine Design

Classification: `{summary['classification']}`

Phase 6 locks an EBIT-first downstream contract without writing production score, valuation, TTM or canonical rows.

## Existing Downstream

The existing V2 score path is EBITDA-first for margin, margin trend, consistency and leverage fallback. The existing V2 valuation path computes both EV/EBIT and EV/EBITDA, but bucket selection prefers EV/EBITDA before falling back to EV/EBIT.

## Locked Policy

Primary fields: `{', '.join(CORE_FIELDS)}`.

Secondary fields retained: `{', '.join(SECONDARY_FIELDS)}`.

All 12 fundamental variables remain retained. EBIT is primary operating earnings. EBITDA is secondary and does not block primary readiness.

## Valuation

Market cap is `price * shares_outstanding`; net debt is `total_debt - cash`; EV is `market_cap + total_debt - cash`. Primary metrics are EV/EBIT, EBIT yield, FCF yield and EV/Sales. EV/EBITDA and P/E are retained as secondary metrics.

## Coverage

- EV/EBIT computable endpoints: `{c['ev_ebit_computable']}`
- EV/EBIT meaningful endpoints: `{c['ev_ebit_meaningful']}`
- EV/EBITDA computable endpoints: `{c['ev_ebitda_computable']}`
- EV/EBITDA meaningful endpoints: `{c['ev_ebitda_meaningful']}`
- FCF yield computable endpoints: `{c['fcf_yield_computable']}`
- EV/Sales computable endpoints: `{c['ev_sales_computable']}`
- P/E computable endpoints: `{c['pe_computable']}`
- Primary score-ready endpoints: `{c['primary_score_ready']}`
- Latest companies primary score-ready: `{c['companies_latest_primary_score_ready']}`
- EBIT-primary coverage gain vs EBITDA-primary: `{c['ebit_primary_coverage_gain_vs_ebitda']}`

## Safety

Production writes: `{summary['production_writes']}`.

Next: `{summary['recommended_next_step']}`
""",
        encoding="utf-8",
    )


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    marker = "## Phase 6"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip() + "\n"
    addition = f"""

## Phase 6

Classification: `{summary['classification']}`

Status: `DESIGN_COMPLETE_READY_FOR_PRODUCTION_IMPLEMENTATION`

Production score writes: `{summary['production_writes']['score']}`

Production valuation writes: `{summary['production_writes']['valuation']}`

TTM writes: `{summary['production_writes']['ttm']}`

Canonical writes: `{summary['production_writes']['canonical']}`

Primary score-ready endpoints dry estimate: `{summary['coverage']['primary_score_ready']}`

Next: `{summary['recommended_next_step']}`
"""
    path.write_text(existing.rstrip() + addition.rstrip() + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
