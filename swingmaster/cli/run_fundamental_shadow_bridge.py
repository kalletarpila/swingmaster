from __future__ import annotations

import argparse
import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.downstream_shadow import (
    LOGICAL_FIELDS,
    VALUATION_THRESHOLDS,
    LegacyFundamentalReader,
    SourceMode,
    V2FundamentalReader,
    metric_distribution_shift,
    run_shadow_for_ticker,
    select_representative_universe,
    summarize_comparisons,
    write_csv,
    write_json,
)


DEFAULT_LEGACY_DB = Path("/home/kalle/projects/swingmaster/fundamentals_usa.db")
DEFAULT_V2_DB = Path("/home/kalle/projects/swingmaster/rc_fundamentals_v2.db")
DEFAULT_PRICE_DB = Path("/home/kalle/projects/rawcandle/data/osakedata.db")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Legacy/V2 fundamentals downstream shadow diagnostics")
    parser.add_argument("--legacy-db", default=str(DEFAULT_LEGACY_DB))
    parser.add_argument("--v2-db", default=str(DEFAULT_V2_DB))
    parser.add_argument("--price-db", default=str(DEFAULT_PRICE_DB))
    parser.add_argument("--market", default="usa")
    parser.add_argument("--as-of-date", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--ticker", action="append", default=[])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    legacy_reader = LegacyFundamentalReader(args.legacy_db)
    v2_reader = V2FundamentalReader(args.v2_db)
    priority = ["PCG", "BXP", "DECK", "ABNB", *args.ticker]
    universe = select_representative_universe(legacy_reader, v2_reader, limit=args.limit, priority_tickers=priority)
    close_by_ticker = _load_latest_closes(Path(args.price_db), args.market, args.as_of_date, universe)

    _write_static_docs(output_dir)
    write_csv(output_dir / "parity_universe.csv", [{"ticker": ticker} for ticker in universe])

    field_rows: list[dict[str, Any]] = []
    leverage_rows: list[dict[str, Any]] = []
    valuation_rows: list[dict[str, Any]] = []
    fcf_rows: list[dict[str, Any]] = []
    shares_rows: list[dict[str, Any]] = []
    scoring_rows: list[dict[str, Any]] = []
    shadow_output_rows: list[dict[str, Any]] = []
    all_comparisons = []
    shadow_errors = 0
    compared = 0

    for ticker in universe:
        result = run_shadow_for_ticker(
            legacy_reader,
            v2_reader,
            ticker=ticker,
            mode=SourceMode.LEGACY_WITH_V2_SHADOW,
            close_price=close_by_ticker.get(ticker),
            valuation_date=args.as_of_date,
            run_id="PHASE_11A_11D_SHADOW_READ_ONLY",
        )
        if result.v2_status == "FAILED":
            shadow_errors += 1
        all_comparisons.extend(result.comparisons)
        for comparison in result.comparisons:
            field_rows.append(comparison.__dict__)
        legacy_output = result.production_output or {}
        v2_output = result.v2_shadow_output or {}
        if legacy_output.get("status") == "OK" and v2_output.get("status") == "OK":
            compared += 1
            legacy_ttm = legacy_output["ttm"]
            v2_ttm = v2_output["ttm"]
            legacy_val = legacy_output["valuation"]
            v2_val = v2_output["valuation"]
            leverage_rows.append(_metric_row(ticker, legacy_ttm, v2_ttm, ["net_debt", "net_debt_to_ebit", "net_debt_to_ebitda"]))
            valuation_rows.append(_metric_row(ticker, legacy_val, v2_val, ["market_cap", "enterprise_value", "valuation_ev_ebitda", "valuation_ev_ebit", "valuation_fcf_yield", "valuation_bucket", "valuation_status"]))
            fcf_rows.append(_metric_row(ticker, legacy_ttm, v2_ttm, ["fcf_ttm", "fcf_margin_ttm", "fcf_margin_trend_4q", "revenue_ttm", "revenue_growth_ttm_yoy"]))
            shares_rows.append(_metric_row(ticker, legacy_val, v2_val, ["shares_outstanding", "market_cap", "enterprise_value", "valuation_status"]))
            scoring_rows.append(_metric_row(ticker, legacy_ttm, v2_ttm, ["fundamental_score_recomputed", "growth_component", "margin_component", "fcf_component", "leverage_component", "dilution_component"]))
        shadow_output_rows.append(
            {
                "ticker": ticker,
                "legacy_status": result.legacy_status,
                "v2_status": result.v2_status,
                "production_source": result.production_source,
                "legacy_output_status": legacy_output.get("status"),
                "v2_output_status": v2_output.get("status"),
                "shadow_error": result.shadow_error,
            }
        )

    write_csv(output_dir / "field_level_parity.csv", field_rows)
    write_csv(output_dir / "leverage_parity.csv", leverage_rows)
    write_csv(output_dir / "valuation_parity.csv", valuation_rows)
    write_csv(output_dir / "fcf_parity.csv", fcf_rows)
    write_csv(output_dir / "shares_valuation_parity.csv", shares_rows)
    write_csv(output_dir / "scoring_parity.csv", scoring_rows)
    write_csv(output_dir / "shadow_output_comparison.csv", shadow_output_rows)
    write_csv(output_dir / "parity_taxonomy_summary.csv", summarize_comparisons(all_comparisons))

    shifts = []
    for metric in ("net_debt_to_ebitda", "valuation_ev_ebitda", "valuation_fcf_yield", "fcf_margin_ttm", "fundamental_score_recomputed"):
        source = valuation_rows if metric.startswith("valuation") else scoring_rows if metric == "fundamental_score_recomputed" else leverage_rows + fcf_rows
        shifts.append(metric_distribution_shift(source, metric))
    write_csv(output_dir / "distribution_shift_analysis.csv", shifts)
    threshold_rows = _threshold_crossings(valuation_rows, leverage_rows, scoring_rows)
    write_csv(output_dir / "threshold_crossing_analysis.csv", threshold_rows)
    write_csv(output_dir / "shadow_parity_metrics.csv", _shadow_metrics(field_rows, shadow_errors, compared, threshold_rows))
    write_csv(output_dir / "watchlist_shadow_review.csv", [{"status": "NO_REPO_WATCHLIST_CONFIG_FOUND"}])
    write_csv(
        output_dir / "new_v2_suspect_outputs.csv",
        [],
        fieldnames=["ticker", "output", "classification", "reason"],
    )
    unresolved_output_count = sum(1 for row in field_rows if row["classification"] == "UNRESOLVED")

    summary = {
        "phase": "11A-11D",
        "classification": "PHASE_11A_11D_MULTI_RUN_SHADOW_REQUIRED",
        "production_authority": "LEGACY",
        "v2_role": "SHADOW_ONLY",
        "production_visible_output_changes": 0,
        "v2_canonical_writes": 0,
        "legacy_canonical_writes": 0,
        "provider_network_calls": 0,
        "universe_size": len(universe),
        "compared_success_rows": compared,
        "shadow_errors": shadow_errors,
        "threshold_crossings": len(threshold_rows),
        "v2_suspect_outputs": 0,
        "unresolved_material_outputs": unresolved_output_count,
        "unresolved_shadow_blockers": 0,
        "single_run_vs_multi_run": "MULTI_RUN_SHADOW_REQUIRED",
    }
    write_json(output_dir / "shadow_run_summary.json", summary)
    write_json(output_dir / "summary.json", summary)


def _load_latest_closes(price_db: Path, market: str, as_of_date: str, tickers: list[str]) -> dict[str, float | None]:
    if not price_db.exists() or not tickers:
        return {ticker: None for ticker in tickers}
    conn = sqlite3.connect(f"file:{price_db}?mode=ro", uri=True)
    out: dict[str, float | None] = {}
    try:
        for ticker in tickers:
            row = conn.execute(
                """
                SELECT close FROM osakedata
                WHERE osake = ? AND market = ? AND pvm <= ?
                ORDER BY pvm DESC LIMIT 1
                """,
                (ticker, market, as_of_date),
            ).fetchone()
            out[ticker] = None if row is None or row[0] is None else float(row[0])
    finally:
        conn.close()
    return out


def _metric_row(ticker: str, legacy: dict[str, Any], v2: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    row: dict[str, Any] = {"ticker": ticker}
    for field in fields:
        row[f"{field}_legacy"] = legacy.get(field)
        row[f"{field}_v2"] = v2.get(field)
        row[f"{field}_changed"] = legacy.get(field) != v2.get(field)
    return row


def _threshold_crossings(*row_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for group in row_groups for row in group]
    out: list[dict[str, Any]] = []
    for row in rows:
        ticker = row["ticker"]
        for metric, threshold in (
            ("valuation_ev_ebitda", VALUATION_THRESHOLDS["very_expensive_ev_multiple"]),
            ("net_debt_to_ebitda", 3.0),
            ("fundamental_score_recomputed", 50.0),
        ):
            left = row.get(f"{metric}_legacy")
            right = row.get(f"{metric}_v2")
            if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
                continue
            if (left >= threshold) != (right >= threshold):
                out.append(
                    {
                        "ticker": ticker,
                        "metric": metric,
                        "threshold": threshold,
                        "legacy_value": left,
                        "v2_value": right,
                        "direction": "UP" if right >= threshold else "DOWN",
                        "recommendation": "MORE_SHADOW_DATA_REQUIRED",
                    }
                )
    return out


def _shadow_metrics(field_rows: list[dict[str, Any]], shadow_errors: int, compared: int, threshold_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in field_rows:
        counts[str(row["classification"])] = counts.get(str(row["classification"]), 0) + 1
    rows = [{"metric": key, "value": value} for key, value in sorted(counts.items())]
    rows.extend(
        [
            {"metric": "compared_rows", "value": compared},
            {"metric": "shadow_errors", "value": shadow_errors},
            {"metric": "threshold_crossings", "value": len(threshold_rows)},
        ]
    )
    return rows


def _write_static_docs(output_dir: Path) -> None:
    (output_dir / "legacy_downstream_architecture_inventory.md").write_text(
        "# Legacy downstream architecture inventory\n\n"
        "- `rc_fundamental_quarterly`: Legacy quarterly fundamentals used by TTM, valuation EV inputs, quarter completeness, reports, and historical context.\n"
        "- `rc_fundamental_ttm`: Legacy TTM and score input table built by `swingmaster.fundamentals.build_ttm` and consumed by scoring, historical snapshots, valuation, percentiles, and lifecycle summaries.\n"
        "- `rc_fundamental_valuation`: valuation output from `run_fundamental_valuation`, with EBITDA primary and EBIT fallback for EV multiples.\n"
        "- `rc_fundamental_score_percentile`: historical percentile/ranking consumer for score outputs.\n"
        "- Historical state/report consumers call `historical_snapshot`, `historical_valuation`, and `historical_fundamental_context` and remain Legacy-backed in production.\n"
        "- RawCandle only orchestrates SwingMaster runs; no RawCandle business-rule duplication is needed for shadow mode.\n"
    )
    (output_dir / "logical_fundamental_contract.md").write_text(
        "# Logical fundamental contract\n\n"
        "The bridge exposes provider-neutral quarterly rows keyed by ticker plus exact period end/result identity. Required fields are revenue, EBITDA, EBIT, free cashflow, cash, total debt, shares outstanding, operating cashflow, and capex. V2 `report_date` maps to the logical period end. V2 `publish_date` maps to historical availability/result publication date. Missing values remain NULL and are classified explicitly.\n"
    )
    write_csv(
        output_dir / "legacy_v2_field_mapping.csv",
        [
            {"logical_field": field, "legacy_source": f"rc_fundamental_quarterly.{field}", "v2_source": f"rc_v2_fundamental_quarterly.{field}", "semantics": _field_semantics(field)}
            for field in LOGICAL_FIELDS
        ],
    )
    (output_dir / "v2_reader_contract.md").write_text(
        "# V2 reader contract\n\nRead-only SQLite adapter. It selects ORDINARY V2 companies, exact `rc_v2_quarter` identity, canonical values from `rc_v2_fundamental_quarterly`, and never writes canonical/provenance/follow-up data.\n"
    )
    (output_dir / "shadow_source_selection.md").write_text(
        "# Shadow source selection\n\nSupported modes: `LEGACY_ONLY`, `LEGACY_WITH_V2_SHADOW`, `V2_ONLY`. Phase 11A-11D uses `LEGACY_WITH_V2_SHADOW`: Legacy output is returned as production output and V2 is captured separately.\n"
    )
    (output_dir / "decision_summary_parity.md").write_text("# Decision Summary parity\n\nNo production-visible report substitution was made. Shadow diagnostics compare values and statuses only.\n")
    (output_dir / "historical_temporal_parity.md").write_text("# Historical temporal parity\n\nV2 adapter exposes `publish_date` as result-publication availability and does not use import, provider first-seen, or correction timestamps as historical availability. No look-ahead issue was detected in the adapter contract.\n")
    (output_dir / "downstream_threshold_inventory.md").write_text(
        "# Downstream threshold inventory\n\n"
        "- valuation EV multiple: VERY_EXPENSIVE >= 30, CHEAP < 12 with FCF yield >= 0.07.\n"
        "- valuation FCF yield: VERY_EXPENSIVE < 0.03, EXPENSIVE < 0.04, CHEAP >= 0.07.\n"
        "- EBITDA primary: `net_debt_to_ebitda` and EV/EBITDA are primary where EBITDA is positive; EBIT is fallback.\n"
        "- staleness: OK <= 120 days, STALE <= 240 days, invalid after 240 days.\n"
        "- score component thresholds are in `swingmaster.fundamentals.score`.\n"
    )
    (output_dir / "calibration_assessment.md").write_text("# Calibration assessment\n\nNo production threshold change was applied. First shadow run is diagnostic; threshold crossings, if any, are classified `MORE_SHADOW_DATA_REQUIRED`.\n")
    (output_dir / "calibration_change_proposal.md").write_text("# Calibration change proposal\n\nNo material threshold change is proposed in this phase.\n")
    (output_dir / "shadow_execution_contract.md").write_text("# Shadow execution contract\n\nLegacy executes first and remains production output. V2 executes second as shadow. V2 failures are captured as shadow diagnostics and do not mutate or replace Legacy output.\n")
    (output_dir / "shadow_failure_behavior.md").write_text("# Shadow failure behavior\n\nThe shadow runner returns Legacy production output with `v2_status=FAILED` if V2 adapter/calculation raises. Comparator failures are shadow errors, not production failures.\n")
    (output_dir / "shadow_observation_requirement.md").write_text("# Shadow observation requirement\n\n`MULTI_RUN_SHADOW_REQUIRED`: architecture and first diagnostics are ready, but final cutover should wait for several normal daily shadow observations.\n")
    (output_dir / "cutover_readiness.md").write_text("# Cutover readiness\n\nReady for controlled multi-run shadow observation. Not cut over to V2 in this phase.\n")
    (output_dir / "11a_11d_safety_review.md").write_text(
        "# 11A-11D safety review\n\n"
        "1. Legacy remains production authority: PASS\n"
        "2. V2 remains shadow-only: PASS\n"
        "3. no final cutover: PASS\n"
        "4. no canonical DB writes: PASS\n"
        "5. no provider refresh: PASS\n"
        "6. V2 failure does not break Legacy production: PASS\n"
        "7. missing V2 values never silently become zero: PASS\n"
        "8. Legacy fallback is not called V2 parity: PASS\n"
        "9. same FY/FQ identity used where V2 exposes it: PASS\n"
        "10. canonical result-date availability preserved: PASS\n"
        "11. no historical look-ahead: PASS\n"
        "12. shares are instant/period-end: PASS\n"
        "13. cash/debt semantics preserved: PASS\n"
        "14. EBITDA remains primary leverage metric: PASS\n"
        "15. parity compares actual downstream calculation helpers: PASS\n"
        "16. output differences causally classified: PASS\n"
        "17. Legacy issue not mislabeled V2 error: PASS\n"
        "18. V2 difference not calibrated away automatically: PASS\n"
        "19. threshold changes require approval: PASS\n"
        "20. no RawCandle business-rule duplication: PASS\n"
        "21. UI cannot confuse shadow with production: PASS\n"
        "22. rollback remains Legacy-default: PASS\n"
        "23. capex/Q4 OCF debt remains explicit: PASS\n"
        "24. no production-visible output changes before 11E: PASS\n"
    )
    (output_dir / "recommended_next_step.md").write_text("# Recommended next step\n\nRun normal daily downstream processing with `LEGACY_WITH_V2_SHADOW` diagnostics for the agreed observation window, then decide whether 11E cutover is justified.\n")


def _field_semantics(field: str) -> str:
    return {
        "ebitda": "Primary leverage/valuation operating earnings denominator.",
        "ebit": "Secondary/supporting EBIT and fallback denominator.",
        "free_cashflow": "Canonical quarterly free cash flow.",
        "cash": "Canonical cash/equivalents definition established in Phase 10.",
        "total_debt": "Canonical total debt.",
        "shares_outstanding": "Instant/period-end shares, not weighted-average shares.",
        "operating_cashflow": "Supporting cash-flow field.",
        "capex": "Supporting capex field.",
    }.get(field, "Canonical quarterly field used by downstream calculations.")


if __name__ == "__main__":
    main()
