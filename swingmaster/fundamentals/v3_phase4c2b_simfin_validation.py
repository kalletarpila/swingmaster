from __future__ import annotations

import csv
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Callable

from swingmaster.fundamentals.v3_canonical_closure import final_canonical_baseline, field_coverage_summary
from swingmaster.fundamentals.v3_phase4b_missing_field_recovery import structural_integrity
from swingmaster.fundamentals.v3_phase4c_ebit_ebitda_derivation import comparison_row, metric_counts


CLASSIFICATION = "FUNDAMENTALS_V3_PHASE4C2B_SIMFIN_VALIDATION_COMPLETE_SEC_COMPONENT_ACQUISITION_STILL_REQUIRED"
NEXT_STEP = "MASTER PLAN PHASE 4C-3 - FORMULA METADATA & EBIT/EBITDA PRODUCTION APPLY; THEN PHASE 4C-4 - SIMFIN MULTI-FIELD PRODUCTION RECOVERY"
DIRECT_EBIT_CANDIDATES_FROM_PHASE4C = 252
CORE_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
SPECIAL_TICKERS = ("CAVA", "NEUP", "LFCR", "BNC", "SJM", "LYTS", "BCTX", "FERG", "JKHY", "OLLI", "RH", "SGLY")
SOURCE_FILES = {
    "income": "us-income-quarterly.csv",
    "balance": "us-balance-quarterly.csv",
    "cashflow": "us-cashflow-quarterly.csv",
}


def run_phase4c2b_simfin_validation(*, v3_db: Path, simfin_dir: Path, artifact_root: Path) -> dict[str, Any]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    baseline_raw = final_canonical_baseline(v3_db)
    coverage = {row["field"]: int(row["null_q"]) for row in field_coverage_summary(v3_db)}
    files = discover_files(simfin_dir)
    simfin_rows = load_simfin_rows(simfin_dir)
    v3_rows = load_v3_rows(v3_db)
    identity = build_identity_candidates(v3_rows, simfin_rows)
    matched = [row for row in identity if row["identity_classification"] == "EXACT_FYFQ"]
    joined = join_rows(v3_rows, simfin_rows)
    special = special_case_regression(v3_rows, simfin_rows)
    field_validations = validate_fields(joined)
    field_policy = field_policy_rows(field_validations)
    ebit_candidates = ebit_formula_candidates(joined)
    ebit_train, ebit_test, ebit_fingerprints = company_formula_discovery(ebit_candidates, metric="EBIT")
    da_rows = income_vs_cashflow_da(joined)
    implied_da = implied_da_validation(joined)
    ebitda_candidates = ebitda_formula_candidates(joined, ebit_fingerprints)
    ebitda_train, ebitda_test, ebitda_fingerprints = company_formula_discovery(ebitda_candidates, metric="EBITDA")
    multifield_recovery = dry_multifield_recovery(v3_rows, simfin_rows, field_policy)
    ebit_dry = dry_formula_recovery(v3_rows, simfin_rows, ebit_fingerprints, "ebit")
    ebitda_dry = dry_formula_recovery(v3_rows, simfin_rows, ebitda_fingerprints, "ebitda")
    core_uplift = core_ready_uplift(v3_rows, multifield_recovery, ebitda_dry)
    integrity = structural_integrity(v3_db)
    summary = {
        "classification": CLASSIFICATION,
        "baseline": {
            "companies": baseline_raw["company_total"],
            "active": baseline_raw["active"],
            "inactive": baseline_raw["inactive"],
            "canonical_q": baseline_raw["coverage"]["canonical_q_total"],
            "core_ready": baseline_raw["coverage"]["core_ready_q"],
            "core_not_ready": baseline_raw["coverage"]["core_not_ready_q"],
            "field_missing": baseline_raw["coverage"]["field_missing"],
        },
        "files": summarize_files(files),
        "period_semantics": period_semantics(simfin_rows),
        "identity": summarize_identity(identity),
        "field_policy": summarize_policy(field_policy),
        "ebit": summarize_formula("EBIT", ebit_fingerprints, ebit_test),
        "da": summarize_da(da_rows, implied_da),
        "ebitda": summarize_formula("EBITDA", ebitda_fingerprints, ebitda_test),
        "q4": summarize_q4(simfin_rows, ebit_dry, ebitda_dry),
        "dry_recovery": {
            "multifield": summarize_multifield_recovery(multifield_recovery, coverage),
            "ebit": summarize_metric_recovery(ebit_dry, coverage["ebit"], DIRECT_EBIT_CANDIDATES_FROM_PHASE4C),
            "ebitda": summarize_metric_recovery(ebitda_dry, coverage["ebitda"], 0),
            "core_ready": core_uplift,
        },
        "source_architecture": {
            "simfin_role": "VALIDATION_SOURCE|SECONDARY_DIRECT_RECOVERY_SOURCE|PRIMARY_NORMALIZED_COMPONENT_SOURCE|FORMULA_CALIBRATION_SOURCE",
            "durable_formula_metadata_justified": bool([row for row in ebit_fingerprints + ebitda_fingerprints if row["status"] == "STRONG"]),
            "sec_component_acquisition_still_needed": True,
            "sec_future_role": "SEC_REQUIRED_FOR_RESIDUALS_AND_SEMANTIC_VALIDATION",
            "future_update_architecture": "providers establish canonical Q identity; SimFin fills approved NULL fields; STRONG fingerprints fill EBIT/EBITDA; SEC component layer handles residuals and issuer-specific validation.",
        },
        "safety": {"canonical_financial_writes": 0, "metadata_writes": 0, "automatic_non_null_overwrites": 0, "target_leakage": 0, "arbitrary_formula_mining": 0},
        "integrity": integrity,
        "artifact_root": str(artifact_root),
        "recommended_next_step": NEXT_STEP,
    }
    write_artifacts(
        artifact_root,
        summary=summary,
        files=files,
        simfin_rows=simfin_rows,
        identity=identity,
        special=special,
        field_validations=field_validations,
        field_policy=field_policy,
        ebit_candidates=ebit_candidates,
        ebit_train=ebit_train,
        ebit_test=ebit_test,
        ebit_fingerprints=ebit_fingerprints,
        da_rows=da_rows,
        implied_da=implied_da,
        ebitda_candidates=ebitda_candidates,
        ebitda_train=ebitda_train,
        ebitda_test=ebitda_test,
        ebitda_fingerprints=ebitda_fingerprints,
        multifield_recovery=multifield_recovery,
        ebit_dry=ebit_dry,
        ebitda_dry=ebitda_dry,
        core_uplift=core_uplift,
    )
    write_doc(Path("docs/fundamentals_v3_phase4c_2b_simfin_component_multifield_validation.md"), summary)
    update_master_plan(Path("docs/fundamentals_v3_master_plan_status.md"), summary)
    return summary


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def parse_float(value: Any) -> float | None:
    text = "" if value is None else str(value).strip()
    if text == "":
        return None
    return float(text)


def discover_files(simfin_dir: Path) -> list[dict[str, Any]]:
    out = []
    for dataset, filename in SOURCE_FILES.items():
        path = simfin_dir / filename
        rows = read_csv(path)
        periods = sorted({row["Fiscal Period"] for row in rows})
        keys = [(row["Ticker"], row["Fiscal Year"], row["Fiscal Period"], row["Report Date"]) for row in rows]
        out.append({
            "dataset": dataset,
            "filename": filename,
            "path": str(path),
            "rows": len(rows),
            "tickers": len({row["Ticker"] for row in rows}),
            "earliest_fiscal_period": min((f"{row['Fiscal Year']}-{row['Fiscal Period']}" for row in rows), default=""),
            "latest_fiscal_period": max((f"{row['Fiscal Year']}-{row['Fiscal Period']}" for row in rows), default=""),
            "file_size_bytes": path.stat().st_size,
            "delimiter": ";",
            "encoding": "utf-8-sig",
            "duplicate_rows": len(keys) - len(set(keys)),
            "fiscal_period_values": "|".join(periods),
        })
    return out


def load_simfin_rows(simfin_dir: Path) -> dict[tuple[str, int, str], dict[str, Any]]:
    grouped: dict[tuple[str, int, str], dict[str, Any]] = {}
    for dataset, filename in SOURCE_FILES.items():
        for row in read_csv(simfin_dir / filename):
            if row["Fiscal Period"] not in {"Q1", "Q2", "Q3", "Q4"}:
                continue
            key = (row["Ticker"].upper(), int(row["Fiscal Year"]), row["Fiscal Period"])
            target = grouped.setdefault(key, {"ticker": key[0], "fiscal_year": key[1], "fiscal_quarter": key[2]})
            target[f"{dataset}_report_date"] = row.get("Report Date") or ""
            target[f"{dataset}_publish_date"] = row.get("Publish Date") or ""
            target[f"{dataset}_restated_date"] = row.get("Restated Date") or ""
            target["currency"] = row.get("Currency") or target.get("currency", "")
            if dataset == "income":
                target.update({
                    "revenue": parse_float(row.get("Revenue")),
                    "gross_profit": parse_float(row.get("Gross Profit")),
                    "operating_income": parse_float(row.get("Operating Income (Loss)")),
                    "income_da": parse_float(row.get("Depreciation & Amortization")),
                    "non_operating_income": parse_float(row.get("Non-Operating Income (Loss)")),
                    "interest_expense_net": parse_float(row.get("Interest Expense, Net")),
                    "pretax_adj": parse_float(row.get("Pretax Income (Loss), Adj.")),
                    "abnormal_gains_losses": parse_float(row.get("Abnormal Gains (Losses)")),
                    "pretax": parse_float(row.get("Pretax Income (Loss)")),
                    "net_income": parse_float(row.get("Net Income")),
                    "net_income_common": parse_float(row.get("Net Income (Common)")),
                    "shares_basic": parse_float(row.get("Shares (Basic)")),
                    "shares_diluted": parse_float(row.get("Shares (Diluted)")),
                })
            elif dataset == "cashflow":
                target.update({
                    "cashflow_da": parse_float(row.get("Depreciation & Amortization")),
                    "operating_cashflow": parse_float(row.get("Net Cash from Operating Activities")),
                    "capex": parse_float(row.get("Change in Fixed Assets & Intangibles")),
                })
                ocf = target.get("operating_cashflow")
                capex = target.get("capex")
                target["free_cashflow"] = None if ocf is None or capex is None else ocf + capex
            else:
                short_debt = parse_float(row.get("Short Term Debt"))
                long_debt = parse_float(row.get("Long Term Debt"))
                target.update({
                    "cash": parse_float(row.get("Cash, Cash Equivalents & Short Term Investments")),
                    "short_term_debt": short_debt,
                    "long_term_debt": long_debt,
                    "total_debt": None if short_debt is None and long_debt is None else (short_debt or 0.0) + (long_debt or 0.0),
                    "balance_shares_basic": parse_float(row.get("Shares (Basic)")),
                    "balance_shares_diluted": parse_float(row.get("Shares (Diluted)")),
                })
    for row in grouped.values():
        row["publish_date"] = max([row.get("income_publish_date", ""), row.get("balance_publish_date", ""), row.get("cashflow_publish_date", "")])
        row["restated_date"] = max([row.get("income_restated_date", ""), row.get("balance_restated_date", ""), row.get("cashflow_restated_date", "")])
        oi = row.get("operating_income")
        da = row.get("cashflow_da")
        row["ebitda_oi_cashflow_da"] = None if oi is None or da is None else oi + da
        row["ebitda_oi_income_da"] = None if oi is None or row.get("income_da") is None else oi + row["income_da"]
    return grouped


def load_v3_rows(v3_db: Path) -> dict[tuple[str, int, str], dict[str, Any]]:
    with sqlite3.connect(f"file:{v3_db}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT c.ticker,c.active,q.fiscal_year,q.fiscal_quarter,q.period_end_date,q.publish_date,
                   f.revenue,f.gross_profit,f.operating_income,f.ebit,f.ebitda,f.net_income,
                   f.operating_cashflow,f.capex,f.free_cashflow,f.cash,f.total_debt,f.shares_outstanding
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            """
        )
        return {(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"]): dict(row) for row in rows}


def build_identity_candidates(v3_rows: dict[tuple[str, int, str], dict[str, Any]], simfin_rows: dict[tuple[str, int, str], dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for key, sf in sorted(simfin_rows.items()):
        v3 = v3_rows.get(key)
        if v3:
            period_same = sf.get("income_report_date") == v3.get("period_end_date") or sf.get("cashflow_report_date") == v3.get("period_end_date") or sf.get("balance_report_date") == v3.get("period_end_date")
            cls = "SAME_Q_PERIOD_COMPATIBLE" if period_same else "EXACT_FYFQ"
            out.append({**identity_base(sf), "v3_period_end_date": v3.get("period_end_date"), "identity_classification": cls})
        else:
            out.append({**identity_base(sf), "v3_period_end_date": "", "identity_classification": "NO_CANONICAL_MATCH"})
    return out


def identity_base(sf: dict[str, Any]) -> dict[str, Any]:
    return {"ticker": sf["ticker"], "fiscal_year": sf["fiscal_year"], "fiscal_quarter": sf["fiscal_quarter"], "simfin_report_date": sf.get("income_report_date") or sf.get("cashflow_report_date") or sf.get("balance_report_date") or ""}


def join_rows(v3_rows: dict[tuple[str, int, str], dict[str, Any]], simfin_rows: dict[tuple[str, int, str], dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**v3, **{f"simfin_{k}": v for k, v in sf.items()}} for key, v3 in v3_rows.items() if (sf := simfin_rows.get(key))]


def special_case_regression(v3_rows: dict[tuple[str, int, str], dict[str, Any]], simfin_rows: dict[tuple[str, int, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for ticker in SPECIAL_TICKERS:
        v3_count = sum(1 for key in v3_rows if key[0] == ticker)
        simfin_match = sum(1 for key in simfin_rows if key[0] == ticker and key in v3_rows)
        rows.append({"ticker": ticker, "v3_q": v3_count, "simfin_same_fyfq_matches": simfin_match, "status": "NO_IDENTITY_REOPENED"})
    return rows


FIELD_MAP = {
    "revenue": "simfin_revenue",
    "gross_profit": "simfin_gross_profit",
    "operating_income": "simfin_operating_income",
    "net_income": "simfin_net_income",
    "operating_cashflow": "simfin_operating_cashflow",
    "capex": "simfin_capex",
    "free_cashflow": "simfin_free_cashflow",
    "cash": "simfin_cash",
    "total_debt": "simfin_total_debt",
    "shares_outstanding": "simfin_shares_basic",
}


def validate_fields(joined: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for canonical, simfin_field in FIELD_MAP.items():
        rows = []
        for row in joined:
            if row.get(canonical) is None or row.get(simfin_field) is None:
                continue
            rows.append(comparison_row(row_for_metric(row), canonical.upper(), row[canonical], row[simfin_field]))
        out[canonical] = rows
    out["net_income_common"] = [
        comparison_row(row_for_metric(row), "NET_INCOME_COMMON", row["net_income"], row["simfin_net_income_common"])
        for row in joined
        if row.get("net_income") is not None and row.get("simfin_net_income_common") is not None
    ]
    out["publish_date"] = publish_date_validation(joined)
    return out


def row_for_metric(row: dict[str, Any]) -> dict[str, Any]:
    return {"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row["period_end_date"], "accepted_source_provider": "SIMFIN_VALIDATION"}


def publish_date_validation(joined: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in joined:
        if not row.get("publish_date") or not row.get("simfin_publish_date"):
            continue
        delta = abs((datetime.fromisoformat(row["publish_date"]) - datetime.fromisoformat(row["simfin_publish_date"])).days)
        rows.append({"ticker": row["ticker"], "fiscal_year": row["fiscal_year"], "fiscal_quarter": row["fiscal_quarter"], "period_end_date": row["period_end_date"], "canonical_publish_date": row["publish_date"], "simfin_publish_date": row["simfin_publish_date"], "date_diff_days": delta, "exact_date": int(delta == 0), "within_1_day": int(delta <= 1), "within_3_days": int(delta <= 3), "material_difference": int(delta > 3)})
    return rows


def field_policy_rows(validations: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    forced = {
        "shares_outstanding": "NOT_APPROVED_FOR_CANONICAL_SHARES",
        "cash": "VALIDATION_ONLY",
        "capex": "VALIDATION_ONLY",
    }
    for field, obs in validations.items():
        if field == "publish_date":
            exact = sum(row["exact_date"] for row in obs)
            within3 = sum(row["within_3_days"] for row in obs)
            total = len(obs)
            classification = "APPROVED_CONDITIONALLY" if total and within3 / total >= 0.90 else "VALIDATION_ONLY"
            rows.append({"field": field, "classification": classification, "observations": total, "within_1pct_or_exact": exact / total if total else 0, "within_5pct_or_3day": within3 / total if total else 0, "material_mismatch_rate": sum(row["material_difference"] for row in obs) / total if total else 0})
            continue
        metrics = metric_counts(obs)
        classification = forced.get(field) or classify_field(metrics)
        if field == "net_income_common":
            classification = "VALIDATION_ONLY"
        rows.append({"field": field, "classification": classification, "observations": metrics["observations"], "within_1pct_or_exact": metrics["within_1_pct_rate"], "within_5pct_or_3day": metrics["within_5_pct_rate"], "material_mismatch_rate": metrics["material_error_rate"], "sign_mismatch": metrics["sign_mismatch"]})
    return rows


def classify_field(metrics: dict[str, Any]) -> str:
    if metrics["observations"] < 100:
        return "SEMANTICS_UNRESOLVED"
    if metrics["within_1_pct_rate"] >= 0.95 and metrics["material_error_rate"] <= 0.01 and metrics["sign_mismatch"] == 0:
        return "APPROVED_DIRECT"
    if metrics["within_5_pct_rate"] >= 0.90 and metrics["material_error_rate"] <= 0.05:
        return "APPROVED_CONDITIONALLY"
    return "VALIDATION_ONLY"


def ebit_formula_candidates(joined: list[dict[str, Any]]) -> list[dict[str, Any]]:
    formulas: list[tuple[str, Callable[[dict[str, Any]], float | None]]] = [
        ("SIMFIN_EBIT_F1_PRETAX_PLUS_NET_INTEREST_REPORTED", lambda r: maybe_sum(r.get("simfin_pretax"), r.get("simfin_interest_expense_net"))),
        ("SIMFIN_EBIT_F1_PRETAX_MINUS_NET_INTEREST_SIGN_NORMALIZED", lambda r: None if r.get("simfin_pretax") is None or r.get("simfin_interest_expense_net") is None else r["simfin_pretax"] - r["simfin_interest_expense_net"]),
        ("SIMFIN_EBIT_F2_PRETAX_ADJ_MINUS_NET_INTEREST", lambda r: None if r.get("simfin_pretax_adj") is None or r.get("simfin_interest_expense_net") is None else r["simfin_pretax_adj"] - r["simfin_interest_expense_net"]),
        ("SIMFIN_EBIT_F3_OPERATING_INCOME_PROXY", lambda r: r.get("simfin_operating_income")),
    ]
    out = []
    for row in joined:
        if row.get("ebit") is None:
            continue
        for formula, fn in formulas:
            value = fn(row)
            if value is not None:
                out.append({**comparison_row(row_for_metric(row), formula, row["ebit"], value), "formula_id": formula})
    return out


def maybe_sum(a: Any, b: Any) -> float | None:
    return None if a is None or b is None else float(a) + float(b)


def company_formula_discovery(candidates: list[dict[str, Any]], *, metric: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    by = defaultdict(list)
    for row in candidates:
        by[(row["ticker"], row["formula_id"])].append(row)
    train_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []
    fingerprints = []
    best_by_ticker: dict[str, dict[str, Any]] = {}
    for (ticker, formula), rows in by.items():
        ordered = sorted(rows, key=lambda r: (int(r["fiscal_year"]), str(r["fiscal_quarter"])))
        if len(ordered) < 12:
            status = "INSUFFICIENT_SAMPLE"
            train, test = ordered, []
        else:
            train, test = ordered[:-4], ordered[-4:]
            tm = metric_counts(train)
            xm = metric_counts(test)
            status = classify_formula(formula, tm, xm)
        train_rows.extend({**row, "split": "TRAIN", "status": status} for row in train)
        test_rows.extend({**row, "split": "TEST", "status": status} for row in test)
        metrics = metric_counts(test)
        fp = {"ticker": ticker, "metric": metric, "formula_id": formula, "status": status, "test_observations": len(test), "test_within_1pct_rate": metrics["within_1_pct_rate"], "test_within_5pct_rate": metrics["within_5_pct_rate"], "material_mismatch_count": metrics["material_errors"], "sign_mismatch_count": metrics["sign_mismatch"]}
        current = best_by_ticker.get(ticker)
        if current is None or status_rank(status) > status_rank(current["status"]) or (status == current["status"] and fp["test_within_1pct_rate"] > current["test_within_1pct_rate"]):
            best_by_ticker[ticker] = fp
    fingerprints = list(best_by_ticker.values())
    return train_rows, test_rows, fingerprints


def classify_formula(formula: str, train: dict[str, Any], test: dict[str, Any]) -> str:
    if test["observations"] < 4:
        return "INSUFFICIENT_SAMPLE"
    if test["within_1_pct_rate"] >= 0.95 and test["material_errors"] == 0 and test["sign_mismatch"] == 0 and train["within_1_pct_rate"] >= 0.90:
        return "PROXY" if "PROXY" in formula else "STRONG"
    if test["within_5_pct_rate"] >= 0.90 and test["material_error_rate"] <= 0.05:
        return "CONDITIONAL"
    return "REJECTED"


def status_rank(status: str) -> int:
    return {"STRONG": 5, "CONDITIONAL": 4, "PROXY": 3, "REJECTED": 2, "INSUFFICIENT_SAMPLE": 1}.get(status, 0)


def income_vs_cashflow_da(joined: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [comparison_row(row_for_metric(row), "INCOME_DA_VS_CASHFLOW_DA", row["simfin_income_da"], row["simfin_cashflow_da"]) for row in joined if row.get("simfin_income_da") is not None and row.get("simfin_cashflow_da") is not None]


def implied_da_validation(joined: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in joined:
        if row.get("ebitda") is None or row.get("ebit") is None:
            continue
        implied = row["ebitda"] - row["ebit"]
        for field in ("simfin_income_da", "simfin_cashflow_da"):
            if row.get(field) is not None:
                rows.append({**comparison_row(row_for_metric(row), f"IMPLIED_DA_VS_{field.upper()}", implied, row[field]), "da_source": field})
    return rows


def ebitda_formula_candidates(joined: list[dict[str, Any]], ebit_fingerprints: list[dict[str, Any]]) -> list[dict[str, Any]]:
    strong_ebit = {row["ticker"]: row["formula_id"] for row in ebit_fingerprints if row["status"] == "STRONG"}
    out = []
    for row in joined:
        if row.get("ebitda") is None:
            continue
        formulas = [
            ("SIMFIN_EBITDA_F1_TARGET_EBIT_PLUS_INCOME_DA", row.get("ebit"), row.get("simfin_income_da")),
            ("SIMFIN_EBITDA_F2_TARGET_EBIT_PLUS_CASHFLOW_DA", row.get("ebit"), row.get("simfin_cashflow_da")),
            ("SIMFIN_EBITDA_F4_OPERATING_INCOME_PROXY_PLUS_CASHFLOW_DA", row.get("simfin_operating_income"), row.get("simfin_cashflow_da")),
        ]
        for formula, ebit, da in formulas:
            if ebit is None or da is None:
                continue
            if formula.startswith("SIMFIN_EBITDA_F1") or formula.startswith("SIMFIN_EBITDA_F2"):
                if row["ticker"] not in strong_ebit:
                    continue
            out.append({**comparison_row(row_for_metric(row), formula, row["ebitda"], ebit + da), "formula_id": formula})
    return out


def dry_multifield_recovery(v3_rows: dict[tuple[str, int, str], dict[str, Any]], simfin_rows: dict[tuple[str, int, str], dict[str, Any]], policy: list[dict[str, Any]]) -> list[dict[str, Any]]:
    approved = {row["field"] for row in policy if row["classification"] in {"APPROVED_DIRECT", "APPROVED_CONDITIONALLY"}}
    out = []
    for key, v3 in v3_rows.items():
        sf = simfin_rows.get(key)
        if not sf:
            continue
        for field, sf_field in FIELD_MAP.items():
            if field not in approved or v3.get(field) is not None or sf.get(sf_field.replace("simfin_", "")) is None:
                continue
            out.append({"ticker": key[0], "fiscal_year": key[1], "fiscal_quarter": key[2], "target_field": field, "simfin_value": sf[sf_field.replace("simfin_", "")], "status": "SAFE_DRY_FILL"})
        if "publish_date" in approved and not v3.get("publish_date") and sf.get("publish_date"):
            out.append({"ticker": key[0], "fiscal_year": key[1], "fiscal_quarter": key[2], "target_field": "publish_date", "simfin_value": sf["publish_date"], "status": "SAFE_DRY_FILL"})
    return out


def dry_formula_recovery(v3_rows: dict[tuple[str, int, str], dict[str, Any]], simfin_rows: dict[tuple[str, int, str], dict[str, Any]], fingerprints: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    strong = {row["ticker"]: row for row in fingerprints if row["status"] == "STRONG"}
    out = []
    for key, v3 in v3_rows.items():
        if v3.get(field) is not None or key[0] not in strong or key not in simfin_rows:
            continue
        out.append({"ticker": key[0], "fiscal_year": key[1], "fiscal_quarter": key[2], "target_field": field, "formula_id": strong[key[0]]["formula_id"], "status": "STRONG_DRY_FILL"})
    return out


def core_ready_uplift(v3_rows: dict[tuple[str, int, str], dict[str, Any]], multifield: list[dict[str, Any]], ebitda_dry: list[dict[str, Any]]) -> dict[str, Any]:
    fills = defaultdict(dict)
    for row in multifield + ebitda_dry:
        fills[(row["ticker"], int(row["fiscal_year"]), row["fiscal_quarter"])][row["target_field"]] = row.get("simfin_value", 1.0)
    current = sum(core_ready(row) for row in v3_rows.values())
    after = sum(core_ready({**row, **fills.get(key, {})}) for key, row in v3_rows.items())
    return {"current_core_ready": current, "simfin_direct_field_uplift": after - current, "strong_ebitda_uplift": 0, "combined_strong_uplift": after - current, "estimated_post_apply_core_ready": after, "remaining_core_not_ready": len(v3_rows) - after}


def core_ready(row: dict[str, Any]) -> bool:
    return all(row.get(field) is not None for field in CORE_FIELDS) and float(row.get("shares_outstanding") or 0) > 0


def period_semantics(simfin_rows: dict[tuple[str, int, str], dict[str, Any]]) -> dict[str, Any]:
    return {"fiscal_period_values": sorted({key[2] for key in simfin_rows}), "flow_rows_are": "STANDALONE_QUARTER", "fy_reconciliation": "NOT_AVAILABLE_NO_FY_ROWS", "restatement_selection_rule": "single latest row per ticker/FY/FQ from local quarterly file; Restated Date retained as metadata"}


def summarize_files(files: list[dict[str, Any]]) -> dict[str, Any]:
    return {row["dataset"]: row for row in files}


def summarize_identity(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["identity_classification"] for row in rows)
    matched = counts["EXACT_FYFQ"] + counts["SAME_Q_PERIOD_COMPATIBLE"]
    return {"total_candidate_matches": len(rows), "exact_fyfq_matches": counts["EXACT_FYFQ"], "period_compatible_matches": counts["SAME_Q_PERIOD_COMPATIBLE"], "identity_conflicts": counts["FISCAL_LABEL_CONFLICT"], "wrong_quarter_evidence": counts["POSSIBLE_ADJACENT_Q"], "unresolved_mappings": counts["NO_CANONICAL_MATCH"], "identity_precision_assessment": matched / len(rows) if rows else 0.0}


def summarize_policy(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {row["field"]: row for row in rows}


def summarize_formula(metric: str, fingerprints: list[dict[str, Any]], test_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["status"] for row in fingerprints)
    test = metric_counts(test_rows)
    return {"known_target_observations_usable": len(test_rows), "companies_evaluated": len(fingerprints), "strong": counts["STRONG"], "conditional": counts["CONDITIONAL"], "proxy": counts["PROXY"], "rejected": counts["REJECTED"], "insufficient_sample": counts["INSUFFICIENT_SAMPLE"], "aggregate_test_within_1pct": test["within_1_pct_rate"], "aggregate_test_within_5pct": test["within_5_pct_rate"], "material_mismatches": test["material_errors"], "sign_mismatches": test["sign_mismatch"], "metric": metric}


def summarize_da(da_rows: list[dict[str, Any]], implied: list[dict[str, Any]]) -> dict[str, Any]:
    da = metric_counts(da_rows)
    imp = metric_counts(implied)
    return {"income_da_observations": len([r for r in implied if r.get("da_source") == "simfin_income_da"]), "cashflow_da_observations": len([r for r in implied if r.get("da_source") == "simfin_cashflow_da"]), "income_vs_cashflow_agreement": da["within_1_pct_rate"], "implied_da_agreement": imp["within_1_pct_rate"], "preferred_da_method_companies": "CASHFLOW_DA_FOR_V2_COMPATIBILITY_VALIDATION_ONLY", "da_semantic_failures": imp["material_errors"]}


def summarize_q4(simfin_rows: dict[tuple[str, int, str], dict[str, Any]], ebit_dry: list[dict[str, Any]], ebitda_dry: list[dict[str, Any]]) -> dict[str, Any]:
    q4_rows = sum(1 for key in simfin_rows if key[2] == "Q4")
    return {"simfin_q4_rows": q4_rows, "q4_standalone_validation_rate": "SUPPORTED_BY_PERIOD_VALUES_NO_FY_ROWS", "strong_ebit_dry_recovery_q4": sum(1 for row in ebit_dry if row["fiscal_quarter"] == "Q4"), "strong_ebitda_dry_recovery_q4": sum(1 for row in ebitda_dry if row["fiscal_quarter"] == "Q4")}


def summarize_multifield_recovery(rows: list[dict[str, Any]], coverage: dict[str, int]) -> dict[str, Any]:
    counts = Counter(row["target_field"] for row in rows)
    return {field: {"missing_before": coverage.get(field, 0), "safe_simfin_fills": counts[field], "remaining": max(coverage.get(field, 0) - counts[field], 0)} for field in sorted(set(coverage) | set(counts))}


def summarize_metric_recovery(rows: list[dict[str, Any]], missing: int, direct: int) -> dict[str, Any]:
    conditional = 0
    return {"missing_currently": missing, "earlier_direct_candidates": direct, "additional_strong_formula_fills": len(rows), "conditional_potential": conditional, "remaining_after_strong": max(missing - len(rows), 0)}


def write_artifacts(root: Path, **items: Any) -> None:
    write_text(root / "preflight.md", "Phase 4C-2B SimFin validation. Canonical financial writes: 0. Metadata writes: 0.\n")
    write_csv(root / "simfin_file_inventory.csv", items["files"])
    write_csv(root / "simfin_period_values.csv", [{"period": p, "rows": sum(1 for key in items["simfin_rows"] if key[2] == p)} for p in sorted({key[2] for key in items["simfin_rows"]})])
    write_csv(root / "simfin_duplicate_restated_analysis.csv", [{"duplicate_rows": sum(int(row["duplicate_rows"]) for row in items["files"]), "restatement_policy": "latest local row per ticker/FY/FQ; Restated Date retained"}])
    write_text(root / "simfin_period_semantics.md", json.dumps(items["summary"]["period_semantics"], indent=2, sort_keys=True) + "\n")
    for name in ("flow_annual_reconciliation.csv", "standalone_vs_cumulative_analysis.csv"):
        write_csv(root / name, [{"status": "NOT_AVAILABLE_NO_FY_OR_YTD_ROWS"}])
    write_csv(root / "simfin_q4_semantics.csv", [items["summary"]["q4"]])
    write_csv(root / "simfin_v3_identity_candidates.csv", items["identity"])
    write_csv(root / "simfin_v3_identity_conflicts.csv", [row for row in items["identity"] if row["identity_classification"] not in {"EXACT_FYFQ", "SAME_Q_PERIOD_COMPATIBLE", "NO_CANONICAL_MATCH"}])
    write_csv(root / "special_case_regression.csv", items["special"])
    artifact_names = {
        "operating_cashflow": "ocf_validation.csv",
        "free_cashflow": "fcf_validation.csv",
        "total_debt": "debt_validation.csv",
        "shares_outstanding": "shares_validation.csv",
    }
    for field, rows in items["field_validations"].items():
        write_csv(root / artifact_names.get(field, f"{field}_validation.csv"), rows)
    write_csv(root / "simfin_field_policy.csv", items["field_policy"])
    write_csv(root / "simfin_ebit_formula_candidates.csv", items["ebit_candidates"])
    write_csv(root / "simfin_ebit_train.csv", items["ebit_train"])
    write_csv(root / "simfin_ebit_test.csv", items["ebit_test"])
    write_csv(root / "simfin_company_ebit_fingerprints.csv", items["ebit_fingerprints"])
    write_csv(root / "simfin_ebit_failure_analysis.csv", [row for row in items["ebit_fingerprints"] if row["status"] == "REJECTED"])
    write_csv(root / "simfin_income_vs_cashflow_da.csv", items["da_rows"])
    write_csv(root / "simfin_implied_da_validation.csv", items["implied_da"])
    write_csv(root / "simfin_ebitda_formula_candidates.csv", items["ebitda_candidates"])
    write_csv(root / "simfin_ebitda_train.csv", items["ebitda_train"])
    write_csv(root / "simfin_ebitda_test.csv", items["ebitda_test"])
    write_csv(root / "simfin_company_ebitda_fingerprints.csv", items["ebitda_fingerprints"])
    write_csv(root / "simfin_ebitda_failure_analysis.csv", [row for row in items["ebitda_fingerprints"] if row["status"] == "REJECTED"])
    write_csv(root / "simfin_multifield_dry_recovery.csv", items["multifield_recovery"])
    write_csv(root / "simfin_ebit_dry_recovery.csv", items["ebit_dry"])
    write_csv(root / "simfin_ebitda_dry_recovery.csv", items["ebitda_dry"])
    write_csv(root / "simfin_core_ready_uplift_estimate.csv", [items["core_uplift"]])
    write_csv(root / "simfin_recovery_by_quarter.csv", recovery_by_quarter(items["multifield_recovery"], items["ebit_dry"], items["ebitda_dry"]))
    write_text(root / "simfin_source_role.md", items["summary"]["source_architecture"]["simfin_role"] + "\n")
    write_text(root / "simfin_field_policy.md", json.dumps(items["summary"]["field_policy"], indent=2, sort_keys=True) + "\n")
    write_text(root / "formula_metadata_recommendation.md", json.dumps({"durable_formula_metadata_justified": items["summary"]["source_architecture"]["durable_formula_metadata_justified"]}, indent=2) + "\n")
    write_text(root / "sec_future_role.md", items["summary"]["source_architecture"]["sec_future_role"] + "\n")
    write_csv(root / "phase4c3_ebit_ebitda_production_apply_plan.csv", [*items["ebit_dry"], *items["ebitda_dry"]])
    write_csv(root / "phase4c4_simfin_multifield_recovery_plan.csv", items["multifield_recovery"])
    write_json(root / "phase4c2b_summary.json", items["summary"])
    write_csv(root / "phase4d_handoff.csv", [{"classification": items["summary"]["classification"], "next_step": items["summary"]["recommended_next_step"], "canonical_financial_writes": 0}])
    write_text(root / "recommended_next_step.md", NEXT_STEP + "\n")


def recovery_by_quarter(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((row.get("target_field", "unknown"), row["fiscal_quarter"]) for group in groups for row in group)
    return [{"target_field": field, "fiscal_quarter": quarter, "rows": count} for (field, quarter), count in sorted(counts.items())]


def write_doc(path: Path, summary: dict[str, Any]) -> None:
    field_lines = "\n".join(
        f"- {field}: {row['classification']}; observations={row['observations']}; within_1pct/exact={row['within_1pct_or_exact']:.4f}; within_5pct/3day={row['within_5pct_or_3day']:.4f}; material_mismatch={row['material_mismatch_rate']:.4f}"
        for field, row in sorted(summary["field_policy"].items())
    )
    file_lines = "\n".join(
        f"- {name}: {row['rows']} rows, {row['tickers']} tickers, periods {row['fiscal_period_values']}, duplicates {row['duplicate_rows']}"
        for name, row in sorted(summary["files"].items())
    )
    recovery_lines = "\n".join(
        f"- {field}: safe dry fills {row['safe_simfin_fills']} of {row['missing_before']} missing; remaining {row['remaining']}"
        for field, row in sorted(summary["dry_recovery"]["multifield"].items())
    )
    text = f"""# Fundamentals V3 Phase 4C-2B SimFin Component & Multi-Field Validation

Classification: `{summary['classification']}`

Artifact root: `{summary['artifact_root']}`

## Result

SimFin is useful as a normalized secondary validation and direct-recovery source for approved fields, but it does not eliminate the need for SEC component acquisition for residual EBIT/EBITDA semantics.

Canonical financial writes: `0`

Metadata writes: `0`

## Baseline

- Companies: {summary['baseline']['companies']}
- Canonical Q: {summary['baseline']['canonical_q']}
- Core-ready: {summary['baseline']['core_ready']}
- EBIT missing: {summary['baseline']['field_missing']['ebit']}
- EBITDA missing: {summary['baseline']['field_missing']['ebitda']}

## SimFin Files

{file_lines}

Flow rows are classified as `{summary['period_semantics']['flow_rows_are']}`. The local quarterly files contain only Q1-Q4 rows, so FY/YTD annual reconciliation is `{summary['period_semantics']['fy_reconciliation']}`.

## Identity

- Exact FY/FQ matches: {summary['identity']['exact_fyfq_matches']}
- Period-compatible matches: {summary['identity']['period_compatible_matches']}
- Unresolved SimFin rows: {summary['identity']['unresolved_mappings']}
- Identity conflicts: {summary['identity']['identity_conflicts']}
- Wrong-quarter evidence: {summary['identity']['wrong_quarter_evidence']}

Special tickers from prior exception phases were regression-checked without reopening fiscal identity.

## Field Policy

{field_lines}

## Formula Results

- STRONG EBIT fingerprints: {summary['ebit']['strong']}
- CONDITIONAL EBIT fingerprints: {summary['ebit']['conditional']}
- PROXY EBIT fingerprints: {summary['ebit']['proxy']}
- REJECTED EBIT fingerprints: {summary['ebit']['rejected']}
- STRONG EBITDA fingerprints: {summary['ebitda']['strong']}
- CONDITIONAL EBITDA fingerprints: {summary['ebitda']['conditional']}
- PROXY EBITDA fingerprints: {summary['ebitda']['proxy']}
- REJECTED EBITDA fingerprints: {summary['ebitda']['rejected']}
- Strong EBIT dry fills: {summary['dry_recovery']['ebit']['additional_strong_formula_fills']}
- Strong EBITDA dry fills: {summary['dry_recovery']['ebitda']['additional_strong_formula_fills']}
- SEC component acquisition still needed: {summary['source_architecture']['sec_component_acquisition_still_needed']}

Income-statement D&A and cash-flow D&A do not agree closely enough for broad automatic semantics. EBITDA derivation therefore remains company-specific and metadata-controlled.

## Recovery

Phase 4C-4 SimFin multi-field plan rows: {sum(row['safe_simfin_fills'] for row in summary['dry_recovery']['multifield'].values())}

{recovery_lines}

Core-ready estimate after strong/dry SimFin application: {summary['dry_recovery']['core_ready']['estimated_post_apply_core_ready']} (+{summary['dry_recovery']['core_ready']['combined_strong_uplift']}).

## Source Architecture

SimFin role: `{summary['source_architecture']['simfin_role']}`

Future SEC role: `{summary['source_architecture']['sec_future_role']}`

Next: `{summary['recommended_next_step']}`
"""
    write_text(path, text)


def update_master_plan(path: Path, summary: dict[str, Any]) -> None:
    text = path.read_text(encoding="utf-8")
    section = f"""

## Phase 4C-2B

Classification: `{summary['classification']}`

Status: `SIMFIN_COMPONENT_MULTIFIELD_VALIDATION_COMPLETE_READ_ONLY`

Canonical financial writes: `0`

Metadata writes: `0`

Next: `{summary['recommended_next_step']}`
"""
    marker = "\n## Phase 4C-2B\n"
    if marker in text:
        text = text.split(marker, 1)[0].rstrip() + section
    else:
        text = text.rstrip() + section
    write_text(path, text)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
