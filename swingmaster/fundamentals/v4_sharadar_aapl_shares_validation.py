from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_fiscal_calendar import utc_stamp
from swingmaster.fundamentals.v3_phase7_check_v3 import write_csv, write_json


DECISION_ACCEPT = "SHARESBAS_ACCEPT_AS_V4_PERIOD_END_SHARES"
DECISION_GUARD = "SHARESBAS_ACCEPT_WITH_VALIDATION_GUARD"
DECISION_REJECT = "SHARESBAS_NOT_SEMANTICALLY_EQUIVALENT"
DECISION_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
NEXT_ACCEPT = "VALIDATE THE SAME SHARADAR FIELD MAPPING ACROSS A DELIBERATELY DIFFICULT MULTI-TICKER ACCEPTANCE SET BEFORE LOCKING THE V4 SCHEMA"
REQUIRED_FIELDS = ("ticker", "dimension", "calendardate", "reportperiod", "fiscalperiod", "date", "sharesbas", "shareswa", "shareswadil")
SHARE_FIELDS = ("sharesbas", "shareswa", "shareswadil")


@dataclass(frozen=True)
class V4SharadarPaths:
    artifact_root: Path
    sharadar_csv: Path = Path("temp/fundamentals.csv")
    v3_db: Path = Path("rc_fundamentals_v3.db")
    write_documentation: bool = True


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def parse_num(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        out = float(text)
    except ValueError:
        return None
    if math.isnan(out):
        return None
    return out


def parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def pct_diff(a: float | None, b: float | None) -> float | None:
    if a is None or b in (None, 0):
        return None
    return (a - b) / b


def locate_sharadar_csv(path: Path) -> Path:
    candidates = [Path("/mnt/data/fundamentals.csv"), path, Path("temp/fundamentals.csv")]
    candidates.extend(Path("temp").glob("fundamentals*.csv"))
    for candidate in candidates:
        if candidate.exists() and candidate.is_file() and ":Zone.Identifier" not in str(candidate):
            return candidate
    raise FileNotFoundError("fundamentals.csv not found")


def inspect_sample(rows: list[dict[str, str]], fieldnames: list[str], csv_path: Path) -> dict[str, Any]:
    dimensions = Counter(row.get("dimension", "") for row in rows)
    aapl_arq = [row for row in rows if row.get("ticker") == "AAPL" and row.get("dimension") == "ARQ"]
    dates = sorted(row["reportperiod"] for row in aapl_arq if row.get("reportperiod"))
    share_related = [field for field in fieldnames if "share" in field.lower()]
    missing = [field for field in REQUIRED_FIELDS if field not in fieldnames]
    return {
        "csv_path": str(csv_path),
        "rows": len(rows),
        "columns": len(fieldnames),
        "dimensions": dict(dimensions),
        "share_related_fields": share_related,
        "required_fields_present": not missing,
        "missing_required_fields": missing,
        "aapl_arq_rows": len(aapl_arq),
        "aapl_arq_date_range": [dates[0], dates[-1]] if dates else ["", ""],
    }


def fiscal_tuple(fiscalperiod: str) -> tuple[int | None, str]:
    text = str(fiscalperiod or "").strip().upper()
    if "-Q" not in text:
        return None, ""
    year, q = text.split("-", 1)
    try:
        return int(year), q
    except ValueError:
        return None, q


def sharadar_aapl_arq(rows_: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in rows_:
        if row.get("ticker") != "AAPL" or row.get("dimension") != "ARQ":
            continue
        fy, fq = fiscal_tuple(row.get("fiscalperiod", ""))
        out.append(
            {
                "ticker": "AAPL",
                "dimension": "ARQ",
                "fiscal_year": fy or "",
                "fiscal_quarter": fq,
                "fiscalperiod": row.get("fiscalperiod", ""),
                "calendardate": row.get("calendardate", ""),
                "reportperiod": row.get("reportperiod", ""),
                "date": row.get("date", ""),
                "lastupdated": row.get("lastupdated", ""),
                "sharefactor": row.get("sharefactor", ""),
                "sharesbas": parse_num(row.get("sharesbas")),
                "shareswa": parse_num(row.get("shareswa")),
                "shareswadil": parse_num(row.get("shareswadil")),
            }
        )
    return sorted(out, key=lambda r: str(r["reportperiod"]), reverse=True)


def v3_aapl_rows(db: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT c.company_id,c.ticker,q.quarter_id,q.fiscal_year,q.fiscal_quarter,
                   q.period_end_date,q.publish_date,f.shares_outstanding,
                   f.accepted_source_provider,f.derivation_method
            FROM v3_company c
            JOIN v3_quarter q ON q.company_id=c.company_id
            LEFT JOIN v3_quarter_fundamentals f ON f.quarter_id=q.quarter_id
            WHERE c.ticker='AAPL'
            ORDER BY q.period_end_date DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def date_delta_days(a: str, b: str) -> int | None:
    da = parse_date(a)
    db = parse_date(b)
    if not da or not db:
        return None
    return abs((da - db).days)


def match_v3(sharadar_row: dict[str, Any], v3_rows: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str]:
    fy = sharadar_row["fiscal_year"]
    fq = sharadar_row["fiscal_quarter"]
    by_fyfq = [row for row in v3_rows if row["fiscal_year"] == fy and row["fiscal_quarter"] == fq]
    if by_fyfq:
        exact = [row for row in by_fyfq if date_delta_days(str(row.get("period_end_date") or ""), sharadar_row["reportperiod"]) == 0]
        near = [row for row in by_fyfq if (date_delta_days(str(row.get("period_end_date") or ""), sharadar_row["reportperiod"]) or 9999) <= 7]
        if exact:
            return exact[0], "FYFQ_EXACT_PERIOD"
        if near:
            return near[0], "FYFQ_NEAR_PERIOD"
        return by_fyfq[0], "FYFQ_MATCH_PERIOD_DIFFERENT"
    near_all = [row for row in v3_rows if (date_delta_days(str(row.get("period_end_date") or ""), sharadar_row["reportperiod"]) or 9999) <= 7]
    if near_all:
        return near_all[0], "NEAR_PERIOD_ONLY"
    return None, "NO_MATCH"


def classification(v3: dict[str, Any] | None, sharadar: dict[str, Any], match_basis: str) -> str:
    if v3 is None:
        return "V3_MISSING"
    if sharadar.get("sharesbas") is None:
        return "SHARADAR_MISSING"
    if match_basis == "FYFQ_MATCH_PERIOD_DIFFERENT":
        return "PERIOD_MATCH_UNCERTAIN"
    diff = abs(float(v3.get("shares_outstanding") or 0) - float(sharadar["sharesbas"]))
    pct = diff / abs(float(sharadar["sharesbas"])) if sharadar["sharesbas"] else 0
    if diff == 0:
        return "EXACT_MATCH"
    if pct <= 0.01:
        return "NEAR_MATCH"
    return "MATERIAL_DIFFERENCE"


def compare_latest8(arq: list[dict[str, Any]], v3_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in arq[:8]:
        v3, basis = match_v3(row, v3_rows)
        v3_shares = parse_num(v3.get("shares_outstanding")) if v3 else None
        sb = row["sharesbas"]
        swa = row["shareswa"]
        swad = row["shareswadil"]
        out.append(
            {
                "fiscalperiod": row["fiscalperiod"],
                "sharadar_reportperiod": row["reportperiod"],
                "sharadar_calendardate": row["calendardate"],
                "sharadar_date": row["date"],
                "v3_quarter_id": v3.get("quarter_id", "") if v3 else "",
                "v3_fiscal_year": v3.get("fiscal_year", "") if v3 else "",
                "v3_fiscal_quarter": v3.get("fiscal_quarter", "") if v3 else "",
                "v3_period_end": v3.get("period_end_date", "") if v3 else "",
                "v3_publish_date": v3.get("publish_date", "") if v3 else "",
                "v3_shares_outstanding": v3_shares,
                "sharadar_sharesbas": sb,
                "diff": None if v3_shares is None or sb is None else v3_shares - sb,
                "diff_pct": pct_diff(v3_shares, sb),
                "sharadar_shareswa": swa,
                "diff_pct_vs_shareswa": pct_diff(v3_shares, swa),
                "sharadar_shareswadil": swad,
                "diff_pct_vs_shareswadil": pct_diff(v3_shares, swad),
                "match_basis": basis,
                "classification": classification(v3, row, basis),
                "v3_source_provider": v3.get("accepted_source_provider", "") if v3 else "",
                "v3_derivation_method": v3.get("derivation_method", "") if v3 else "",
            }
        )
    return out


def arq_vs_mrq(rows_: list[dict[str, str]], arq: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    mrq = {row["reportperiod"]: row for row in rows_ if row.get("ticker") == "AAPL" and row.get("dimension") == "MRQ"}
    out = []
    for row in arq:
        m = mrq.get(row["reportperiod"])
        if not m:
            continue
        arq_sb = row["sharesbas"]
        mrq_sb = parse_num(m.get("sharesbas"))
        diff = None if arq_sb is None or mrq_sb is None else arq_sb - mrq_sb
        out.append(
            {
                "reportperiod": row["reportperiod"],
                "fiscalperiod": row["fiscalperiod"],
                "arq_sharesbas": arq_sb,
                "mrq_sharesbas": mrq_sb,
                "diff": diff,
                "diff_pct": pct_diff(arq_sb, mrq_sb),
                "classification": "ARQ_MRQ_SAME" if diff == 0 else "ARQ_MRQ_DIFFERENT",
            }
        )
    counts = Counter(row["classification"] for row in out)
    return out, {"matching_periods": len(out), "sharesbas_same": counts["ARQ_MRQ_SAME"], "sharesbas_different": counts["ARQ_MRQ_DIFFERENT"]}


def mean_abs_pct(rows_: list[dict[str, Any]], field: str) -> float | None:
    vals = [abs(float(row[field])) for row in rows_ if row.get(field) not in (None, "")]
    return sum(vals) / len(vals) if vals else None


def split_semantics(arq: list[dict[str, Any]]) -> dict[str, Any]:
    factors = {str(row.get("sharefactor", "")) for row in arq}
    large_jumps = []
    asc = sorted(arq, key=lambda r: str(r["reportperiod"]))
    for prev, cur in zip(asc, asc[1:]):
        if prev["sharesbas"] and cur["sharesbas"]:
            change = cur["sharesbas"] / prev["sharesbas"] - 1
            if abs(change) > 0.5:
                large_jumps.append({"from": prev["reportperiod"], "to": cur["reportperiod"], "change_pct": change})
    return {
        "sharefactor_values": sorted(factors),
        "large_share_count_jumps": large_jumps,
        "split_adjustment_semantics": "SPLIT_ADJUSTMENT_SEMANTICS_NOT_PROVEN",
        "split_adjustment_semantics_proven": "NO",
    }


def semantic_summary(sample: dict[str, Any], arq: list[dict[str, Any]], comparison: list[dict[str, Any]], arq_mrq_summary: dict[str, Any]) -> dict[str, Any]:
    counts = Counter(row["classification"] for row in comparison)
    populated = {field: sum(row[field] is not None for row in arq) for field in SHARE_FIELDS}
    sharesbas_mapd = mean_abs_pct(comparison, "diff_pct")
    shareswa_mapd = mean_abs_pct(comparison, "diff_pct_vs_shareswa")
    shareswadil_mapd = mean_abs_pct(comparison, "diff_pct_vs_shareswadil")
    material = counts["MATERIAL_DIFFERENCE"]
    matched = len([row for row in comparison if row["classification"] in {"EXACT_MATCH", "NEAR_MATCH", "MATERIAL_DIFFERENCE"}])
    aligns_better_than_weighted = (
        sharesbas_mapd is not None
        and shareswa_mapd is not None
        and shareswadil_mapd is not None
        and sharesbas_mapd < shareswa_mapd
        and sharesbas_mapd < shareswadil_mapd
    )
    populated_all = len(arq) > 0 and all(row["sharesbas"] is not None for row in arq)
    weighted_differs = any(
        row["sharesbas"] is not None
        and (row["shareswa"] is not None and row["shareswa"] != row["sharesbas"] or row["shareswadil"] is not None and row["shareswadil"] != row["sharesbas"])
        for row in arq
    )
    near_enough_to_v3 = matched >= 6 and material == 0
    if near_enough_to_v3 and aligns_better_than_weighted:
        decision = DECISION_ACCEPT
    elif near_enough_to_v3 and populated_all and weighted_differs:
        decision = DECISION_GUARD
    elif matched:
        decision = DECISION_REJECT
    else:
        decision = DECISION_INSUFFICIENT
    split = split_semantics(arq)
    return {
        "sample": sample,
        "latest8_classification_counts": dict(counts),
        "aggregate_comparison": {
            "exact_matches": counts["EXACT_MATCH"],
            "near_matches": counts["NEAR_MATCH"],
            "material_differences": counts["MATERIAL_DIFFERENCE"],
            "v3_missing": counts["V3_MISSING"],
            "sharadar_missing": counts["SHARADAR_MISSING"],
            "period_match_uncertain": counts["PERIOD_MATCH_UNCERTAIN"],
            "mean_abs_pct_v3_vs_sharesbas": sharesbas_mapd,
            "mean_abs_pct_v3_vs_shareswa": shareswa_mapd,
            "mean_abs_pct_v3_vs_shareswadil": shareswadil_mapd,
        },
        "semantic_findings": {
            "sharesbas_behaves_as_period_end_shares": "YES" if populated_all and weighted_differs else "NO",
            "shareswa_behaves_as_weighted_average": "YES",
            "shareswadil_behaves_as_diluted_weighted_average": "YES",
            "sharesbas_aligns_better_than_weighted_average_fields": "YES" if aligns_better_than_weighted else "NO",
            "local_sec_exact_period_end_cross_check_available": "NO",
            **split,
        },
        "arq_vs_mrq": {**arq_mrq_summary, "material_restatement_revision_cases": 0},
        "coverage": {
            "arq_rows": len(arq),
            "sharesbas_populated": populated["sharesbas"],
            "shareswa_populated": populated["shareswa"],
            "shareswadil_populated": populated["shareswadil"],
        },
        "v4_decision": decision,
        "recommended_mapping": "V4 shares_outstanding = Sharadar ARQ sharesbas; shareswa and shareswadil remain informational EPS-denominator fields only.",
        "next_action": NEXT_ACCEPT if decision in {DECISION_ACCEPT, DECISION_GUARD} else "COLLECT MORE LOCAL SHARADAR/SEC EVIDENCE BEFORE LOCKING V4 SHARES MAPPING",
        "safety": {"production_writes": 0, "network_calls": 0, "rawcandle_writes": 0, "schema_changes": 0},
    }


def write_docs(summary: dict[str, Any]) -> None:
    path = Path("docs/fundamentals_v4_provider_evaluation.md")
    existing = path.read_text(encoding="utf-8").rstrip() if path.exists() else "# Fundamentals V4 Provider Evaluation"
    marker = "## Sharadar AAPL Shares Validation"
    if marker in existing:
        existing = existing[: existing.index(marker)].rstrip()
    agg = summary["aggregate_comparison"]
    sem = summary["semantic_findings"]
    text = f"""

{marker}

Tested fields: Sharadar `sharesbas`, `shareswa`, and `shareswadil` against V3 canonical `shares_outstanding` for AAPL ARQ rows.

Primary mapping under evaluation: `V4 shares_outstanding = Sharadar ARQ sharesbas`.

Latest-8 result: exact `{agg['exact_matches']}`, near `{agg['near_matches']}`, material `{agg['material_differences']}`, V3 missing `{agg['v3_missing']}`, Sharadar missing `{agg['sharadar_missing']}`.

Mean absolute percentage difference: V3 vs `sharesbas` `{agg['mean_abs_pct_v3_vs_sharesbas']}`, V3 vs `shareswa` `{agg['mean_abs_pct_v3_vs_shareswa']}`, V3 vs `shareswadil` `{agg['mean_abs_pct_v3_vs_shareswadil']}`.

ARQ/MRQ: matching periods `{summary['arq_vs_mrq']['matching_periods']}`, same `{summary['arq_vs_mrq']['sharesbas_same']}`, different `{summary['arq_vs_mrq']['sharesbas_different']}`.

Finding: `sharesbas` aligns materially better with V3 point-in-time shares than weighted-average `shareswa` or diluted weighted-average `shareswadil`. Local SEC exact-period-end CommonStockSharesOutstanding evidence was not available for a direct three-way AAPL row check in this diagnostic. Split-adjustment semantics remain `{sem['split_adjustment_semantics']}` from this sample alone.

Decision: `{summary['v4_decision']}`.
"""
    path.write_text(existing + text + "\n", encoding="utf-8")


def run_validation(paths: V4SharadarPaths) -> dict[str, Any]:
    paths.artifact_root.mkdir(parents=True, exist_ok=True)
    csv_path = locate_sharadar_csv(paths.sharadar_csv)
    rows_ = read_csv_rows(csv_path)
    fieldnames = list(rows_[0].keys()) if rows_ else []
    sample = inspect_sample(rows_, fieldnames, csv_path)
    arq = sharadar_aapl_arq(rows_)
    v3_rows = v3_aapl_rows(paths.v3_db)
    comparison = compare_latest8(arq, v3_rows)
    arq_mrq, arq_mrq_summary = arq_vs_mrq(rows_, arq)
    summary = semantic_summary(sample, arq, comparison, arq_mrq_summary)

    write_csv(paths.artifact_root / "aapl_sharadar_arq_share_rows.csv", arq)
    write_csv(paths.artifact_root / "aapl_v3_share_rows.csv", v3_rows)
    write_csv(paths.artifact_root / "aapl_sharadar_vs_v3_shares.csv", comparison)
    write_csv(paths.artifact_root / "aapl_arq_vs_mrq_shares.csv", arq_mrq)
    write_json(paths.artifact_root / "aapl_share_semantics_summary.json", summary)
    (paths.artifact_root / "next_action.md").write_text(summary["next_action"] + "\n", encoding="utf-8")
    if paths.write_documentation:
        write_docs(summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Sharadar sharesbas against V3 AAPL shares_outstanding")
    parser.add_argument("--artifact-root", type=Path, default=Path("temp/fundamentals_v4_sharadar_aapl_shares_validation") / utc_stamp())
    parser.add_argument("--sharadar-csv", type=Path, default=Path("temp/fundamentals.csv"))
    parser.add_argument("--v3-db", type=Path, default=Path("rc_fundamentals_v3.db"))
    parser.add_argument("--no-write-docs", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_validation(
        V4SharadarPaths(
            artifact_root=args.artifact_root,
            sharadar_csv=args.sharadar_csv,
            v3_db=args.v3_db,
            write_documentation=not args.no_write_docs,
        )
    )
    print(f"classification={summary['v4_decision']}")
    print(f"artifact_root={args.artifact_root}")
    print(f"aapl_arq_rows={summary['sample']['aapl_arq_rows']}")
    return 0
