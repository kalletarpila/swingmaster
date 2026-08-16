from __future__ import annotations

import csv
import json
import sqlite3
import traceback
from collections import Counter
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping

from swingmaster.cli.run_fundamental_valuation import build_valuation_row
from swingmaster.fundamentals.build_ttm import build_ttm_rows
from swingmaster.fundamentals.earnings_events import normalize_ticker
from swingmaster.fundamentals.score import explain_score_components


LOGICAL_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "operating_cashflow",
    "capex",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)
REQUIRED_TTM_FIELDS = ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding")
VALUATION_THRESHOLDS = {
    "very_expensive_ev_multiple": 30.0,
    "cheap_ev_multiple": 12.0,
    "cheap_fcf_yield": 0.07,
    "expensive_fcf_yield": 0.04,
    "very_expensive_fcf_yield": 0.03,
    "stale_fundamentals_days": 120,
    "too_stale_fundamentals_days": 240,
}


class SourceMode(str, Enum):
    LEGACY_ONLY = "LEGACY_ONLY"
    LEGACY_WITH_V2_SHADOW = "LEGACY_WITH_V2_SHADOW"
    V2_ONLY = "V2_ONLY"


@dataclass(frozen=True)
class LogicalFundamentalQuarter:
    source: str
    ticker: str
    company_id: int | None
    company_profile: str | None
    fiscal_year: int | None
    fiscal_period: str | None
    period_end_date: str
    result_publication_date: str | None
    quarter_id: int | None
    revenue: float | None
    gross_profit: float | None
    operating_income: float | None
    ebit: float | None
    ebitda: float | None
    operating_cashflow: float | None
    capex: float | None
    free_cashflow: float | None
    cash: float | None
    total_debt: float | None
    shares_outstanding: float | None

    def __getitem__(self, key: str) -> Any:
        if key == "period_end_date":
            return self.period_end_date
        return getattr(self, key)

    def to_ttm_row(self) -> dict[str, Any]:
        payload = {field: getattr(self, field) for field in LOGICAL_FIELDS}
        payload["ticker"] = self.ticker
        payload["period_end_date"] = self.period_end_date
        return payload


@dataclass(frozen=True)
class ShadowComparison:
    ticker: str
    period_end_date: str
    field_name: str
    legacy_value: float | None
    v2_value: float | None
    absolute_difference: float | None
    relative_difference: float | None
    missing_state: str
    classification: str


@dataclass(frozen=True)
class ShadowRunResult:
    ticker: str
    mode: SourceMode
    legacy_status: str
    v2_status: str | None
    production_source: str
    production_output: dict[str, Any] | None
    v2_shadow_output: dict[str, Any] | None
    comparisons: list[ShadowComparison]
    shadow_error: str | None = None


class DownstreamFundamentalReader:
    source_name = "ABSTRACT"

    def load_quarters(self, ticker: str) -> list[LogicalFundamentalQuarter]:
        raise NotImplementedError

    def load_latest_population(self) -> list[LogicalFundamentalQuarter]:
        raise NotImplementedError


class LegacyFundamentalReader(DownstreamFundamentalReader):
    source_name = "LEGACY"

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    def load_quarters(self, ticker: str) -> list[LogicalFundamentalQuarter]:
        normalized = normalize_ticker(ticker)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT ticker, period_end_date, revenue, gross_profit, operating_income, ebit,
                       ebitda, operating_cashflow, capex, free_cashflow, cash, total_debt,
                       shares_outstanding
                FROM rc_fundamental_quarterly
                WHERE ticker = ?
                ORDER BY period_end_date ASC
                """,
                (normalized,),
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def load_latest_population(self) -> list[LogicalFundamentalQuarter]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT q.*
                FROM rc_fundamental_quarterly q
                JOIN (
                    SELECT ticker, MAX(period_end_date) AS period_end_date
                    FROM rc_fundamental_quarterly
                    GROUP BY ticker
                ) latest
                  ON latest.ticker = q.ticker
                 AND latest.period_end_date = q.period_end_date
                ORDER BY q.ticker
                """
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def _from_row(self, row: sqlite3.Row) -> LogicalFundamentalQuarter:
        return LogicalFundamentalQuarter(
            source=self.source_name,
            ticker=normalize_ticker(str(row["ticker"])),
            company_id=None,
            company_profile="ORDINARY",
            fiscal_year=None,
            fiscal_period=None,
            period_end_date=str(row["period_end_date"]),
            result_publication_date=str(row["period_end_date"]),
            quarter_id=None,
            revenue=_float_or_none(row["revenue"]),
            gross_profit=_float_or_none(row["gross_profit"]),
            operating_income=_float_or_none(row["operating_income"]),
            ebit=_float_or_none(row["ebit"]),
            ebitda=_float_or_none(row["ebitda"]),
            operating_cashflow=_float_or_none(row["operating_cashflow"]),
            capex=_float_or_none(row["capex"]),
            free_cashflow=_float_or_none(row["free_cashflow"]),
            cash=_float_or_none(row["cash"]),
            total_debt=_float_or_none(row["total_debt"]),
            shares_outstanding=_float_or_none(row["shares_outstanding"]),
        )


class V2FundamentalReader(DownstreamFundamentalReader):
    source_name = "V2"

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    def load_quarters(self, ticker: str) -> list[LogicalFundamentalQuarter]:
        normalized = normalize_ticker(ticker)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT c.company_id, c.ticker, c.company_profile, q.quarter_id, q.fiscal_year,
                       q.fiscal_period, q.report_date, q.publish_date, f.revenue, f.gross_profit,
                       f.operating_income, f.ebit, f.ebitda, f.operating_cashflow, f.capex,
                       f.free_cashflow, f.cash, f.total_debt, f.shares_outstanding
                FROM rc_v2_company c
                JOIN rc_v2_quarter q ON q.company_id = c.company_id
                JOIN rc_v2_fundamental_quarterly f ON f.quarter_id = q.quarter_id
                WHERE c.ticker = ?
                  AND c.company_profile = 'ORDINARY'
                ORDER BY q.report_date ASC, q.quarter_id ASC
                """,
                (normalized,),
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def load_latest_population(self) -> list[LogicalFundamentalQuarter]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT c.company_id, c.ticker, c.company_profile, q.quarter_id, q.fiscal_year,
                       q.fiscal_period, q.report_date, q.publish_date, f.revenue, f.gross_profit,
                       f.operating_income, f.ebit, f.ebitda, f.operating_cashflow, f.capex,
                       f.free_cashflow, f.cash, f.total_debt, f.shares_outstanding
                FROM rc_v2_company c
                JOIN rc_v2_quarter q ON q.company_id = c.company_id
                JOIN rc_v2_fundamental_quarterly f ON f.quarter_id = q.quarter_id
                WHERE c.company_profile = 'ORDINARY'
                  AND c.active = 1
                  AND q.report_date = (
                    SELECT MAX(q2.report_date)
                    FROM rc_v2_quarter q2
                    WHERE q2.company_id = c.company_id
                  )
                ORDER BY c.ticker
                """
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def _from_row(self, row: sqlite3.Row) -> LogicalFundamentalQuarter:
        return LogicalFundamentalQuarter(
            source=self.source_name,
            ticker=normalize_ticker(str(row["ticker"])),
            company_id=int(row["company_id"]),
            company_profile=str(row["company_profile"]),
            fiscal_year=int(row["fiscal_year"]),
            fiscal_period=str(row["fiscal_period"]),
            period_end_date=str(row["report_date"]),
            result_publication_date=_str_or_none(row["publish_date"]) or str(row["report_date"]),
            quarter_id=int(row["quarter_id"]),
            revenue=_float_or_none(row["revenue"]),
            gross_profit=_float_or_none(row["gross_profit"]),
            operating_income=_float_or_none(row["operating_income"]),
            ebit=_float_or_none(row["ebit"]),
            ebitda=_float_or_none(row["ebitda"]),
            operating_cashflow=_float_or_none(row["operating_cashflow"]),
            capex=_float_or_none(row["capex"]),
            free_cashflow=_float_or_none(row["free_cashflow"]),
            cash=_float_or_none(row["cash"]),
            total_debt=_float_or_none(row["total_debt"]),
            shares_outstanding=_float_or_none(row["shares_outstanding"]),
        )


def build_shadow_output(
    quarters: list[LogicalFundamentalQuarter],
    *,
    close_price: float | None,
    valuation_date: str,
    run_id: str,
) -> dict[str, Any]:
    if len(quarters) < 4:
        return {"status": "INSUFFICIENT_QUARTERS", "quarter_count": len(quarters)}
    ttm_rows = build_ttm_rows([quarter.to_ttm_row() for quarter in quarters], run_id=run_id)
    if not ttm_rows:
        return {"status": "NO_TTM_ROWS", "quarter_count": len(quarters)}
    latest_ttm = ttm_rows[-1]
    history = ttm_rows[-4:]
    score_components = explain_score_components(latest_ttm, history)
    latest_ttm = {**latest_ttm, **score_components}
    latest_quarter = quarters[-1].to_ttm_row()
    valuation = build_valuation_row(
        valuation_date=valuation_date,
        ttm_row=latest_ttm,
        quarterly_row=latest_quarter,
        close_price=close_price,
        run_id=run_id,
        created_at_utc="SHADOW_READ_ONLY",
    )
    missing_required = [field for field in REQUIRED_TTM_FIELDS if latest_quarter.get(field) is None]
    return {
        "status": "OK",
        "quarter_count": len(quarters),
        "latest_period_end_date": latest_ttm["latest_period_end_date"],
        "ttm": latest_ttm,
        "score": score_components,
        "valuation": valuation,
        "missing_required_fields": missing_required,
    }


def compare_quarter_contracts(
    legacy: LogicalFundamentalQuarter | None,
    v2: LogicalFundamentalQuarter | None,
) -> list[ShadowComparison]:
    if legacy is None and v2 is None:
        return []
    ticker = (legacy or v2).ticker  # type: ignore[union-attr]
    period_end_date = (legacy or v2).period_end_date  # type: ignore[union-attr]
    comparisons: list[ShadowComparison] = []
    for field in LOGICAL_FIELDS:
        left = None if legacy is None else getattr(legacy, field)
        right = None if v2 is None else getattr(v2, field)
        missing_state = _missing_state(left, right)
        abs_diff = None
        rel_diff = None
        if left is not None and right is not None:
            abs_diff = abs(float(right) - float(left))
            rel_diff = None if left == 0 else abs_diff / abs(float(left))
        comparisons.append(
            ShadowComparison(
                ticker=ticker,
                period_end_date=period_end_date,
                field_name=field,
                legacy_value=left,
                v2_value=right,
                absolute_difference=abs_diff,
                relative_difference=rel_diff,
                missing_state=missing_state,
                classification=_classify_field_difference(field, left, right, abs_diff, rel_diff, missing_state),
            )
        )
    return comparisons


def run_shadow_for_ticker(
    legacy_reader: DownstreamFundamentalReader,
    v2_reader: DownstreamFundamentalReader,
    *,
    ticker: str,
    mode: SourceMode = SourceMode.LEGACY_WITH_V2_SHADOW,
    close_price: float | None = None,
    valuation_date: str,
    run_id: str = "DOWNSTREAM_SHADOW_READ_ONLY",
) -> ShadowRunResult:
    normalized = normalize_ticker(ticker)
    if mode == SourceMode.V2_ONLY:
        v2_quarters = v2_reader.load_quarters(normalized)
        v2_output = build_shadow_output(v2_quarters, close_price=close_price, valuation_date=valuation_date, run_id=run_id)
        return ShadowRunResult(normalized, mode, "NOT_RUN", "OK", "V2_DIAGNOSTIC", v2_output, v2_output, [])

    legacy_quarters = legacy_reader.load_quarters(normalized)
    legacy_output = build_shadow_output(
        legacy_quarters,
        close_price=close_price,
        valuation_date=valuation_date,
        run_id=run_id,
    )
    if mode == SourceMode.LEGACY_ONLY:
        return ShadowRunResult(normalized, mode, "OK", None, "LEGACY", legacy_output, None, [])

    try:
        v2_quarters = v2_reader.load_quarters(normalized)
        v2_output = build_shadow_output(
            v2_quarters,
            close_price=close_price,
            valuation_date=valuation_date,
            run_id=run_id,
        )
        legacy_latest_by_date = {q.period_end_date: q for q in legacy_quarters}
        v2_latest_by_date = {q.period_end_date: q for q in v2_quarters}
        compared_dates = sorted(set(legacy_latest_by_date).intersection(v2_latest_by_date))
        latest_date = compared_dates[-1] if compared_dates else None
        comparisons = [] if latest_date is None else compare_quarter_contracts(
            legacy_latest_by_date.get(latest_date),
            v2_latest_by_date.get(latest_date),
        )
        return ShadowRunResult(normalized, mode, "OK", "OK", "LEGACY", legacy_output, v2_output, comparisons)
    except Exception as exc:  # pragma: no cover - exact traceback text is environment-dependent
        return ShadowRunResult(
            normalized,
            mode,
            "OK",
            "FAILED",
            "LEGACY",
            legacy_output,
            None,
            [],
            shadow_error=f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc()}",
        )


def select_representative_universe(
    legacy_reader: DownstreamFundamentalReader,
    v2_reader: DownstreamFundamentalReader,
    *,
    limit: int = 80,
    priority_tickers: Iterable[str] = (),
) -> list[str]:
    priority = [normalize_ticker(ticker) for ticker in priority_tickers]
    legacy_latest = {row.ticker: row for row in legacy_reader.load_latest_population()}
    v2_latest = {row.ticker: row for row in v2_reader.load_latest_population()}
    both = sorted(set(legacy_latest).intersection(v2_latest))
    v2_missing_shares = [ticker for ticker, row in sorted(v2_latest.items()) if row.shares_outstanding is None]
    large_diffs: list[tuple[float, str]] = []
    for ticker in both:
        legacy = legacy_latest[ticker]
        v2 = v2_latest[ticker]
        for field in ("revenue", "ebitda", "free_cashflow", "cash", "total_debt", "shares_outstanding"):
            left = getattr(legacy, field)
            right = getattr(v2, field)
            if left is None or right is None or left == 0:
                continue
            large_diffs.append((abs(float(right) - float(left)) / abs(float(left)), ticker))
    ordered = _unique([*priority, *v2_missing_shares[:20], *[ticker for _diff, ticker in sorted(large_diffs, reverse=True)[:40]], *both])
    return ordered[:limit]


def summarize_comparisons(comparisons: Iterable[ShadowComparison]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str]] = Counter()
    for comparison in comparisons:
        key = (comparison.field_name, comparison.classification, comparison.missing_state)
        counts[key] += 1
    return [
        {"field_name": field, "classification": classification, "missing_state": missing_state, "count": count}
        for (field, classification, missing_state), count in sorted(counts.items())
    ]


def metric_distribution_shift(rows: list[dict[str, Any]], metric: str) -> dict[str, Any]:
    diffs = [
        float(row[f"{metric}_v2"]) - float(row[f"{metric}_legacy"])
        for row in rows
        if row.get(f"{metric}_legacy") is not None and row.get(f"{metric}_v2") is not None
    ]
    if not diffs:
        return {"metric": metric, "count": 0, "median_shift": None, "p95_abs_shift": None}
    abs_sorted = sorted(abs(value) for value in diffs)
    p95_index = min(len(abs_sorted) - 1, int(round((len(abs_sorted) - 1) * 0.95)))
    return {
        "metric": metric,
        "count": len(diffs),
        "median_shift": median(diffs),
        "p95_abs_shift": abs_sorted[p95_index],
    }


def write_csv(path: Path, rows: Iterable[Mapping[str, Any]], fieldnames: list[str] | None = None) -> None:
    materialized = [dict(row) for row in rows]
    if fieldnames is None:
        fieldnames = []
        for row in materialized:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _missing_state(left: Any, right: Any) -> str:
    if left is None and right is None:
        return "BOTH_MISSING"
    if left is None:
        return "LEGACY_MISSING"
    if right is None:
        return "V2_MISSING"
    return "BOTH_PRESENT"


def _classify_field_difference(
    field: str,
    left: float | None,
    right: float | None,
    abs_diff: float | None,
    rel_diff: float | None,
    missing_state: str,
) -> str:
    if missing_state != "BOTH_PRESENT":
        return "MISSINGNESS_DIFFERENCE" if missing_state in {"LEGACY_MISSING", "V2_MISSING"} else "EXACT"
    if abs_diff is None or abs_diff == 0:
        return "EXACT"
    if abs_diff <= 1.0 or (rel_diff is not None and rel_diff <= 0.0001):
        return "ROUNDING_ONLY"
    if field in {"revenue", "ebitda", "free_cashflow", "cash", "total_debt"}:
        return "EXPECTED_SEMANTIC_DIFFERENCE"
    if field == "shares_outstanding":
        return "LEGACY_SUSPECT"
    return "UNRESOLVED"


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _unique(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_ticker(value)
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out
