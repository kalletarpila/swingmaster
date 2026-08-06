from __future__ import annotations

import csv
import json
import random
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker, repository_root


ASSESSMENT_POLICY_VERSION = "fundamental_quarter_readiness_v2"
DEFAULT_MARKET = "usa"

IDENTITY_AND_PERIOD_FIELDS = ("ticker", "period_end_date")
INCOME_STATEMENT_CORE_FIELDS = ("revenue", "ebit")
BALANCE_SHEET_CORE_FIELDS = ("cash", "total_debt")
CASH_FLOW_CORE_FIELDS = ("operating_cashflow", "capex", "free_cashflow")
SHARE_AND_EPS_CORE_FIELDS = ("shares_outstanding",)
DERIVED_OR_OPTIONAL_FIELDS = ("ebitda",)
SOURCE_OR_OPERATIONAL_METADATA_FIELDS = ("currency", "run_id")
CORE_FIELDS = (
    *INCOME_STATEMENT_CORE_FIELDS,
    *CASH_FLOW_CORE_FIELDS,
    *BALANCE_SHEET_CORE_FIELDS,
    *SHARE_AND_EPS_CORE_FIELDS,
)
FINANCIAL_FIELDS = (*CORE_FIELDS, *DERIVED_OR_OPTIONAL_FIELDS)
TTM_CONSUMER_FIELDS = ("revenue", "ebit", "free_cashflow", "operating_cashflow", "capex", "cash", "total_debt", "shares_outstanding")
SCORE_HISTORY_TTM_FIELDS = (
    "revenue_growth_ttm_yoy",
    "ebit_margin_ttm",
    "ebit_margin_trend_4q",
    "fcf_margin_ttm",
    "fcf_margin_trend_4q",
    "net_debt_to_ebit",
    "share_dilution_yoy",
)
VALUATION_QUARTER_FIELDS = ("shares_outstanding", "cash", "total_debt")
SOURCE_FIELD_IMPORTANCE = {
    "revenue": "core_ttm_score_snapshot",
    "gross_profit": "optional_ttm_margin_trend",
    "operating_income": "core_normalization_fallback",
    "ebit": "core_ttm_score_valuation",
    "ebitda": "deprecated_optional_not_used_for_active_leverage",
    "net_income": "core_research_context",
    "operating_cashflow": "core_fcf_derivation",
    "capex": "core_fcf_derivation",
    "free_cashflow": "core_ttm_score_valuation",
    "cash": "valuation_ev_optional_zero_assumption",
    "total_debt": "valuation_ev_optional_zero_assumption",
    "shares_outstanding": "valuation_market_cap_and_dilution",
    "currency": "operational_metadata",
    "run_id": "operational_metadata",
}


@dataclass(frozen=True)
class QuarterAssessment:
    market: str
    ticker: str
    period_end_date: str | None
    earnings_event_id: int | None
    announcement_date: str | None
    effective_trading_date: str | None
    quarter_basic_complete: bool
    ttm_input_complete: bool
    score_history_complete: bool
    valuation_input_ready: bool
    historical_research_ready: bool
    core_field_count: int
    available_core_field_count: int
    missing_core_fields: list[str]
    missing_ttm_fields: list[str]
    missing_score_fields: list[str]
    missing_valuation_fields: list[str]
    data_quality_warnings: list[str]
    retry_recommendation: str
    assessment_policy_version: str = ASSESSMENT_POLICY_VERSION


@dataclass(frozen=True)
class CalendarTransition:
    calendar_status: str
    reason: str


@dataclass(frozen=True)
class IngestionTransition:
    ingestion_status: str
    reason: str


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def temp_root() -> Path:
    return repository_root() / "temp"


def validate_temp_path(path: Path, *, must_exist: bool = False) -> Path:
    resolved = path.expanduser().resolve()
    root = temp_root().resolve()
    if must_exist and not resolved.exists():
        raise ValueError(f"PATH_DOES_NOT_EXIST:{resolved}")
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"RUNTIME_PATH_OUTSIDE_TEMP:{resolved}") from exc
    return resolved


def open_readonly_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path.expanduser().resolve().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def assess_quarter_completeness(row: Mapping[str, Any], *, market: str = DEFAULT_MARKET) -> QuarterAssessment:
    ticker = _optional_text(_mapping_value(row, "ticker"))
    period = _optional_text(_mapping_value(row, "period_end_date"))
    warnings: list[str] = []
    if ticker is None:
        warnings.append("MISSING_TICKER")
    if period is None or not _valid_date(period):
        warnings.append("INVALID_PERIOD_END_DATE")

    available = [field for field in CORE_FIELDS if _is_present(_mapping_value(row, field))]
    missing_core = [field for field in CORE_FIELDS if not _is_present(_mapping_value(row, field))]
    missing_ttm = [field for field in TTM_CONSUMER_FIELDS if not _quarter_ttm_field_available(row, field)]
    missing_score = list(missing_ttm)
    missing_valuation = [field for field in VALUATION_QUARTER_FIELDS if not _is_present(_mapping_value(row, field))]

    if all(_is_nullish(_mapping_value(row, field)) for field in FINANCIAL_FIELDS):
        empty = True
    else:
        empty = all(_zeroish(_mapping_value(row, field)) for field in FINANCIAL_FIELDS if not _is_nullish(_mapping_value(row, field)))
    if empty:
        warnings.append("NO_MEANINGFUL_FINANCIAL_VALUES")
    if _mapping_value(row, "shares_outstanding") is not None and not _positive(_mapping_value(row, "shares_outstanding")):
        warnings.append("INVALID_SHARES_OUTSTANDING")
    if _mapping_value(row, "currency") in ("", None):
        warnings.append("MISSING_CURRENCY")

    quarter_basic_complete = (
        ticker is not None
        and period is not None
        and "INVALID_PERIOD_END_DATE" not in warnings
        and _is_present(_mapping_value(row, "revenue"))
        and _is_present(_mapping_value(row, "ebit"))
        and (
            _is_present(_mapping_value(row, "free_cashflow"))
            or (
                _is_present(_mapping_value(row, "operating_cashflow"))
                and _is_present(_mapping_value(row, "capex"))
            )
        )
        and _is_present(_mapping_value(row, "cash"))
        and _is_present(_mapping_value(row, "total_debt"))
        and _is_present(_mapping_value(row, "shares_outstanding"))
    )
    valuation_ready = _positive(_mapping_value(row, "shares_outstanding"))
    research_ready = bool(available) and "INVALID_PERIOD_END_DATE" not in warnings and not empty

    if ticker is None or "INVALID_PERIOD_END_DATE" in warnings or empty:
        quarter_basic_complete = False
        valuation_ready = False
        research_ready = False

    return QuarterAssessment(
        market=market,
        ticker=ticker or "",
        period_end_date=period,
        earnings_event_id=_optional_int(_mapping_value(row, "earnings_event_id")),
        announcement_date=_optional_text(_mapping_value(row, "announcement_date")),
        effective_trading_date=_optional_text(_mapping_value(row, "effective_trading_date")),
        quarter_basic_complete=quarter_basic_complete,
        ttm_input_complete=False,
        score_history_complete=False,
        valuation_input_ready=valuation_ready,
        historical_research_ready=research_ready,
        core_field_count=len(CORE_FIELDS),
        available_core_field_count=len(available),
        missing_core_fields=missing_core,
        missing_ttm_fields=missing_ttm,
        missing_score_fields=missing_score,
        missing_valuation_fields=missing_valuation,
        data_quality_warnings=warnings,
        retry_recommendation=_recommend_retry(row, quarter_basic_complete, missing_core, bool(_mapping_value(row, "earnings_event_id"))),
    )


def apply_history_readiness(
    assessments: list[QuarterAssessment],
    ttm_metrics_by_key: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[QuarterAssessment]:
    by_ticker: dict[str, list[QuarterAssessment]] = defaultdict(list)
    for assessment in assessments:
        by_ticker[assessment.ticker].append(assessment)
    result: list[QuarterAssessment] = []
    for _ticker, rows in by_ticker.items():
        ordered = sorted(rows, key=lambda item: item.period_end_date or "")
        for index, row in enumerate(ordered):
            current_4q = ordered[index - 3 : index + 1] if index >= 3 else []
            previous_4q = ordered[index - 7 : index - 3] if index >= 7 else []
            ttm_input_complete = len(current_4q) == 4 and all(item.quarter_basic_complete for item in current_4q)
            score_window_complete = (
                len(current_4q) == 4
                and len(previous_4q) == 4
                and all(item.quarter_basic_complete for item in (*previous_4q, *current_4q))
            )
            ttm_metric_row = ttm_metrics_by_key.get((row.ticker, row.period_end_date or ""))
            ttm_metric_complete = bool(
                ttm_metric_row
                and all(_is_present(_mapping_value(ttm_metric_row, field)) for field in SCORE_HISTORY_TTM_FIELDS)
            )
            result.append(
                replace(
                    row,
                    ttm_input_complete=ttm_input_complete,
                    score_history_complete=score_window_complete and ttm_metric_complete,
                )
            )
    return sorted(result, key=lambda item: (item.ticker, item.period_end_date or ""))


def assess_ticker_quarter_history(assessments: list[QuarterAssessment]) -> dict[str, Any]:
    ordered = sorted(assessments, key=lambda item: item.period_end_date or "")
    ticker = ordered[0].ticker if ordered else ""
    latest = ordered[-1] if ordered else None
    recent = ordered[-4:] if len(ordered) >= 4 else ordered
    older = ordered[:-4]
    latest_bad = latest is not None and not latest.quarter_basic_complete
    recent_bad = sum(1 for row in recent if not row.quarter_basic_complete)
    older_bad = sum(1 for row in older if not row.quarter_basic_complete)
    usable_count = sum(1 for row in ordered if row.quarter_basic_complete)
    if not ordered or usable_count == 0:
        classification = "NO_USABLE_HISTORY"
    elif len(ordered) < 4:
        classification = "SPARSE_HISTORY"
    elif latest_bad:
        classification = "LATEST_QUARTER_INCOMPLETE"
    elif recent_bad >= 2:
        classification = "MULTIPLE_RECENT_INCOMPLETE"
    elif older_bad > 0 and recent_bad == 0:
        classification = "RECENT_HISTORY_USABLE_OLD_GAPS"
    elif older_bad == 0 and recent_bad == 0:
        classification = "ALL_HISTORY_USABLE"
    else:
        classification = "MANUAL_REVIEW"
    return {
        "ticker": ticker,
        "quarter_count": len(ordered),
        "quarter_basic_complete_count": sum(1 for row in ordered if row.quarter_basic_complete),
        "ttm_input_complete_count": sum(1 for row in ordered if row.ttm_input_complete),
        "score_history_complete_count": sum(1 for row in ordered if row.score_history_complete),
        "incomplete_count": sum(1 for row in ordered if not row.quarter_basic_complete),
        "latest_quarter_basic_complete": latest.quarter_basic_complete if latest else False,
        "latest_period_end": latest.period_end_date if latest else None,
        "latest_earnings_announcement_date": latest.announcement_date if latest else None,
        "latest_ttm_input_complete": latest.ttm_input_complete if latest else False,
        "latest_score_history_complete": latest.score_history_complete if latest else False,
        "latest_valuation_ready": latest.valuation_input_ready if latest else False,
        "old_history_gap_count": older_bad,
        "recent_history_gap_count": recent_bad,
        "retry_candidate_count": sum(1 for row in ordered if row.retry_recommendation not in {"NO_ACTION", "NOT_RETRYABLE"}),
        "classification": classification,
    }


def summarize_quarter_completeness(assessments: list[QuarterAssessment], ticker_rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(assessments)
    retries = Counter(row.retry_recommendation for row in assessments)
    return {
        "assessment_policy_version": ASSESSMENT_POLICY_VERSION,
        "total_quarter_rows": total,
        "distinct_tickers": len({row.ticker for row in assessments}),
        "quarter_basic_complete_count": sum(1 for row in assessments if row.quarter_basic_complete),
        "quarter_basic_incomplete_count": sum(1 for row in assessments if not row.quarter_basic_complete),
        "ttm_input_complete_count": sum(1 for row in assessments if row.ttm_input_complete),
        "score_history_complete_count": sum(1 for row in assessments if row.score_history_complete),
        "valuation_input_ready_count": sum(1 for row in assessments if row.valuation_input_ready),
        "historical_research_ready_count": sum(1 for row in assessments if row.historical_research_ready),
        "retry_yahoo_count": retries["RETRY_YAHOO"],
        "retry_sec_count": retries["RETRY_SEC"],
        "retry_both_count": retries["RETRY_YAHOO_AND_SEC"],
        "manual_review_count": retries["MANUAL_REVIEW"],
        "not_retryable_count": retries["NOT_RETRYABLE"],
        "no_action_count": retries["NO_ACTION"],
        "percentages": _percentage_payload(total, retries, assessments),
        "ticker_classification_counts": dict(Counter(str(row["classification"]) for row in ticker_rows)),
    }


def audit_quarter_completeness(
    db_path: Path,
    *,
    market: str = DEFAULT_MARKET,
    tickers: list[str] | None = None,
    first_n: int | None = None,
    sample_size: int | None = None,
    random_seed: int = 0,
    period_from: str | None = None,
    period_to: str | None = None,
) -> dict[str, Any]:
    with open_readonly_db(db_path) as conn:
        pre_counts = database_counts(conn)
        selected = select_tickers(conn, tickers=tickers, first_n=first_n, sample_size=sample_size, random_seed=random_seed)
        rows = load_quarters(conn, selected, period_from=period_from, period_to=period_to)
        assessments = [assess_quarter_completeness(row, market=market) for row in rows]
        ttm_metrics_by_key = load_ttm_score_metric_rows(conn, selected, period_from=period_from, period_to=period_to)
        assessments = apply_history_readiness(assessments, ttm_metrics_by_key)
        by_ticker: dict[str, list[QuarterAssessment]] = defaultdict(list)
        for assessment in assessments:
            by_ticker[assessment.ticker].append(assessment)
        ticker_rows = [assess_ticker_quarter_history(by_ticker[ticker]) for ticker in sorted(by_ticker)]
        field_rows = field_completeness(conn, selected, period_from=period_from, period_to=period_to)
        earnings = earnings_relationship(conn, assessments, selected)
        summary = summarize_quarter_completeness(assessments, ticker_rows)
        summary["database_counts"] = pre_counts
        summary["database_content_unchanged"] = pre_counts == database_counts(conn)
        summary["earnings_relationship"] = earnings
        summary["age_analysis"] = age_analysis(assessments)
    return {
        "summary": summary,
        "all_quarters": [_csv_ready(asdict(row)) for row in assessments],
        "ticker_summary": ticker_rows,
        "field_completeness": field_rows,
        "retry_candidates": [_csv_ready(asdict(row)) for row in assessments if row.retry_recommendation not in {"NO_ACTION", "NOT_RETRYABLE"}],
        "latest_quarter_issues": [row for row in ticker_rows if row["classification"] in {"LATEST_QUARTER_INCOMPLETE", "MULTIPLE_RECENT_INCOMPLETE", "NO_USABLE_HISTORY"}],
    }


def select_tickers(
    conn: sqlite3.Connection,
    *,
    tickers: list[str] | None,
    first_n: int | None,
    sample_size: int | None,
    random_seed: int,
) -> list[str]:
    if tickers:
        selected = sorted(dict.fromkeys(normalize_ticker(ticker) for ticker in tickers))
    else:
        selected = [
            str(row[0]).upper()
            for row in conn.execute("SELECT DISTINCT ticker FROM rc_fundamental_quarterly ORDER BY ticker ASC")
        ]
    if first_n is not None:
        selected = selected[:first_n]
    if sample_size is not None:
        rng = random.Random(random_seed)
        selected = sorted(rng.sample(selected, min(sample_size, len(selected))))
    return selected


def load_quarters(
    conn: sqlite3.Connection,
    tickers: list[str],
    *,
    period_from: str | None,
    period_to: str | None,
) -> list[sqlite3.Row]:
    if not tickers:
        return []
    clauses = [f"q.ticker IN ({', '.join('?' for _ in tickers)})"]
    params: list[Any] = list(tickers)
    if period_from is not None:
        clauses.append("date(q.period_end_date) >= date(?)")
        params.append(period_from)
    if period_to is not None:
        clauses.append("date(q.period_end_date) <= date(?)")
        params.append(period_to)
    where = " AND ".join(clauses)
    return conn.execute(
        f"""
        SELECT q.*,
               m.earnings_event_id,
               m.announcement_date,
               m.effective_trading_date
        FROM rc_fundamental_quarterly q
        LEFT JOIN rc_fundamental_quarter_earnings_match m
          ON m.ticker = q.ticker
         AND m.period_end_date = q.period_end_date
        WHERE {where}
        ORDER BY q.ticker ASC, q.period_end_date ASC
        """,
        params,
    ).fetchall()


def load_ttm_score_metric_rows(
    conn: sqlite3.Connection,
    tickers: list[str],
    *,
    period_from: str | None,
    period_to: str | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    if not tickers or not _table_exists(conn, "rc_fundamental_ttm"):
        return {}
    existing_columns = {
        str(row[1])
        for row in conn.execute(
            """
            PRAGMA table_info(rc_fundamental_ttm)
            """
        )
    }
    if not set(SCORE_HISTORY_TTM_FIELDS).issubset(existing_columns):
        return {}
    clauses = [f"ticker IN ({', '.join('?' for _ in tickers)})"]
    params: list[Any] = list(tickers)
    if period_from is not None:
        clauses.append("date(as_of_date) >= date(?)")
        params.append(period_from)
    if period_to is not None:
        clauses.append("date(as_of_date) <= date(?)")
        params.append(period_to)
    rows = conn.execute(
        f"""
        SELECT ticker, as_of_date, {', '.join(SCORE_HISTORY_TTM_FIELDS)}
        FROM rc_fundamental_ttm
        WHERE {' AND '.join(clauses)}
        """,
        params,
    ).fetchall()
    return {(str(row["ticker"]), str(row["as_of_date"])): dict(row) for row in rows}


def field_completeness(
    conn: sqlite3.Connection,
    tickers: list[str],
    *,
    period_from: str | None,
    period_to: str | None,
) -> list[dict[str, Any]]:
    if not tickers:
        return []
    clauses = [f"ticker IN ({', '.join('?' for _ in tickers)})"]
    params: list[Any] = list(tickers)
    if period_from is not None:
        clauses.append("date(period_end_date) >= date(?)")
        params.append(period_from)
    if period_to is not None:
        clauses.append("date(period_end_date) <= date(?)")
        params.append(period_to)
    where = " AND ".join(clauses)
    total = int(conn.execute(f"SELECT COUNT(*) FROM rc_fundamental_quarterly WHERE {where}", params).fetchone()[0])
    rows = []
    for field in (*FINANCIAL_FIELDS, "currency"):
        row = conn.execute(
            f"""
            SELECT
              SUM(CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count,
              SUM(CASE WHEN {field} IS NULL THEN 1 ELSE 0 END) AS null_count,
              SUM(CASE WHEN {field} = 0 THEN 1 ELSE 0 END) AS zero_count,
              MIN(CASE WHEN {field} IS NOT NULL THEN period_end_date END) AS first_period_present,
              MAX(CASE WHEN {field} IS NOT NULL THEN period_end_date END) AS last_period_present
            FROM rc_fundamental_quarterly
            WHERE {where}
            """,
            params,
        ).fetchone()
        null_count = int(row["null_count"] or 0)
        rows.append(
            {
                "field_name": field,
                "non_null_count": int(row["non_null_count"] or 0),
                "null_count": null_count,
                "zero_count": int(row["zero_count"] or 0),
                "missing_pct": round(null_count / total * 100, 4) if total else 0.0,
                "first_period_present": row["first_period_present"],
                "last_period_present": row["last_period_present"],
                "consumer_importance": SOURCE_FIELD_IMPORTANCE.get(field, "metadata"),
            }
        )
    return rows


def earnings_relationship(conn: sqlite3.Connection, assessments: list[QuarterAssessment], tickers: list[str]) -> dict[str, Any]:
    incomplete_matched = [row for row in assessments if row.earnings_event_id is not None and not row.quarter_basic_complete]
    placeholders = ", ".join("?" for _ in tickers) if tickers else "NULL"
    matched_missing = 0
    latest_missing: list[dict[str, Any]] = []
    if tickers:
        matched_missing = int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM rc_fundamental_quarter_earnings_match m
                LEFT JOIN rc_fundamental_quarterly q
                  ON q.ticker = m.ticker AND q.period_end_date = m.period_end_date
                WHERE m.ticker IN ({placeholders})
                  AND q.ticker IS NULL
                """,
                tickers,
            ).fetchone()[0]
        )
        latest_missing = [
            dict(row)
            for row in conn.execute(
                f"""
                WITH ranked AS (
                    SELECT e.*, ROW_NUMBER() OVER (PARTITION BY e.ticker ORDER BY e.announcement_date DESC, e.id DESC) rn
                    FROM rc_earnings_event e
                    WHERE e.ticker IN ({placeholders}) AND e.is_reported = 1
                )
                SELECT r.ticker, r.id AS earnings_event_id, r.announcement_date
                FROM ranked r
                LEFT JOIN rc_fundamental_quarter_earnings_match m ON m.earnings_event_id = r.id
                WHERE r.rn = 1 AND m.id IS NULL
                ORDER BY r.ticker
                """,
                tickers,
            ).fetchall()
        ]
    delays = []
    for row in assessments:
        if row.announcement_date and row.effective_trading_date and _valid_date(row.announcement_date) and _valid_date(row.effective_trading_date):
            delays.append((date.fromisoformat(row.effective_trading_date) - date.fromisoformat(row.announcement_date)).days)
    return {
        "matched_earnings_events_with_no_quarterly_row": matched_missing,
        "quarterly_rows_with_earnings_match_but_incomplete": len(incomplete_matched),
        "latest_reported_earnings_event_without_quarter_match": len(latest_missing),
        "latest_reported_earnings_event_without_quarter_match_examples": latest_missing[:50],
        "median_announcement_to_effective_days": _median(delays),
    }


def age_analysis(assessments: list[QuarterAssessment]) -> dict[str, Any]:
    by_ticker: dict[str, list[QuarterAssessment]] = defaultdict(list)
    for row in assessments:
        by_ticker[row.ticker].append(row)
    latest_bad = []
    last_four_bad = []
    older_only_bad = []
    for ticker, rows in by_ticker.items():
        ordered = sorted(rows, key=lambda item: item.period_end_date or "")
        recent = ordered[-4:]
        older = ordered[:-4]
        if ordered and not ordered[-1].quarter_basic_complete:
            latest_bad.append(ticker)
        if any(not row.quarter_basic_complete for row in recent):
            last_four_bad.append(ticker)
        if older and any(not row.quarter_basic_complete for row in older) and all(row.quarter_basic_complete for row in recent):
            older_only_bad.append(ticker)
    return {
        "latest_quarter_incomplete_tickers": sorted(latest_bad),
        "last_four_quarters_incomplete_tickers": sorted(last_four_bad),
        "older_only_incomplete_tickers": sorted(older_only_bad),
    }


def calendar_status_transition(
    *,
    estimated_announcement_date: str | None,
    today: str,
    completed_event_found: bool,
    previous_estimated_announcement_date: str | None = None,
) -> CalendarTransition:
    if completed_event_found:
        return CalendarTransition("COMPLETED_EVENT_FOUND", "completed reported EPS event exists")
    if estimated_announcement_date is None:
        return CalendarTransition("NO_CURRENT_ESTIMATE", "source returned no future estimate")
    if previous_estimated_announcement_date and previous_estimated_announcement_date != estimated_announcement_date:
        return CalendarTransition("DATE_CHANGED", "estimate changed since prior observation")
    if estimated_announcement_date < today:
        return CalendarTransition("DATE_PASSED_EVENT_NOT_FOUND", "estimated date passed without completed event")
    if estimated_announcement_date == today:
        return CalendarTransition("DUE_TODAY", "estimated date is today")
    return CalendarTransition("UPCOMING", "estimated date is in the future")


def ingestion_status_transition(
    *,
    published: bool,
    fetched: bool,
    fetch_failed: bool,
    assessment: QuarterAssessment | None,
    source_compare_complete: bool = False,
    historical: bool = False,
) -> IngestionTransition:
    if historical and source_compare_complete:
        return IngestionTransition("UNKNOWN_HISTORICAL_INGEST_COMPLETENESS", "historical source response was not preserved")
    if fetch_failed:
        return IngestionTransition("FETCH_FAILED", "latest fetch failed")
    if not published:
        return IngestionTransition("NOT_PUBLISHED", "no completed earnings event")
    if not fetched or assessment is None:
        return IngestionTransition("PUBLISHED_DATA_NOT_FETCHED", "published event exists but no quarter fetch has been stored")
    if not assessment.ticker or assessment.period_end_date is None or not _valid_date(assessment.period_end_date):
        return IngestionTransition("NOT_ASSESSABLE", "quarter cannot be assessed")
    if source_compare_complete:
        return IngestionTransition("INGEST_COMPLETE", "latest source non-null supported values matched persisted values")
    if assessment.quarter_basic_complete:
        return IngestionTransition("QUARTER_BASIC_COMPLETE", "quarter has the essential raw inputs for current TTM and score use")
    return IngestionTransition("FUNDAMENTALS_PARTIAL", "stored quarter is useful but incomplete")


def upsert_quarter_ingestion_status(
    conn: sqlite3.Connection,
    assessments: Iterable[QuarterAssessment],
    *,
    run_id: str,
    assessed_at_utc: str | None = None,
) -> int:
    assessed_at = assessed_at_utc or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    rows = [
        (
            row.ticker,
            row.market,
            row.period_end_date,
            row.earnings_event_id,
            row.announcement_date,
            row.effective_trading_date,
            _historical_ingestion_status(row),
            _basic_status(row),
            int(row.quarter_basic_complete),
            int(row.ttm_input_complete),
            int(row.score_history_complete),
            int(row.valuation_input_ready),
            int(row.historical_research_ready),
            len(CORE_FIELDS) - len(row.missing_core_fields),
            json.dumps(row.missing_core_fields, sort_keys=True, separators=(",", ":")),
            json.dumps(row.missing_core_fields, sort_keys=True, separators=(",", ":")),
            json.dumps(row.missing_ttm_fields, sort_keys=True, separators=(",", ":")),
            json.dumps(row.missing_score_fields, sort_keys=True, separators=(",", ":")),
            json.dumps(row.data_quality_warnings, sort_keys=True, separators=(",", ":")),
            None,
            None,
            None,
            row.retry_recommendation,
            None,
            None,
            None,
            assessed_at,
            row.assessment_policy_version,
            "CURRENT_DB_STATE_ONLY",
            run_id,
            assessed_at,
            assessed_at,
            assessed_at,
        )
        for row in assessments
        if row.period_end_date is not None
    ]
    conn.executemany(
        """
        INSERT INTO rc_fundamental_quarter_ingestion_status (
            ticker,
            market,
            period_end_date,
            earnings_event_id,
            announcement_date,
            effective_trading_date,
            ingestion_status,
            basic_status,
            quarter_basic_complete,
            ttm_input_complete,
            score_history_complete,
            valuation_input_ready,
            historical_research_ready,
            available_basic_field_count,
            missing_basic_fields,
            missing_core_fields_json,
            missing_ttm_fields_json,
            missing_score_fields_json,
            data_quality_warnings_json,
            supported_source_field_count,
            source_non_null_field_count,
            persisted_matching_field_count,
            retry_recommendation,
            last_fetch_status,
            last_fetch_source,
            last_source_observed_at_utc,
            last_checked_at_utc,
            assessment_policy_version,
            ingestion_evidence_type,
            run_id,
            assessed_at_utc,
            created_at_utc,
            updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(market, ticker, period_end_date) DO UPDATE SET
            earnings_event_id = excluded.earnings_event_id,
            announcement_date = excluded.announcement_date,
            effective_trading_date = excluded.effective_trading_date,
            ingestion_status = excluded.ingestion_status,
            basic_status = excluded.basic_status,
            quarter_basic_complete = excluded.quarter_basic_complete,
            ttm_input_complete = excluded.ttm_input_complete,
            score_history_complete = excluded.score_history_complete,
            valuation_input_ready = excluded.valuation_input_ready,
            historical_research_ready = excluded.historical_research_ready,
            available_basic_field_count = excluded.available_basic_field_count,
            missing_basic_fields = excluded.missing_basic_fields,
            missing_core_fields_json = excluded.missing_core_fields_json,
            missing_ttm_fields_json = excluded.missing_ttm_fields_json,
            missing_score_fields_json = excluded.missing_score_fields_json,
            data_quality_warnings_json = excluded.data_quality_warnings_json,
            supported_source_field_count = excluded.supported_source_field_count,
            source_non_null_field_count = excluded.source_non_null_field_count,
            persisted_matching_field_count = excluded.persisted_matching_field_count,
            retry_recommendation = excluded.retry_recommendation,
            last_fetch_status = excluded.last_fetch_status,
            last_fetch_source = excluded.last_fetch_source,
            last_source_observed_at_utc = excluded.last_source_observed_at_utc,
            last_checked_at_utc = excluded.last_checked_at_utc,
            assessment_policy_version = excluded.assessment_policy_version,
            ingestion_evidence_type = excluded.ingestion_evidence_type,
            run_id = excluded.run_id,
            assessed_at_utc = excluded.assessed_at_utc,
            updated_at_utc = excluded.updated_at_utc
        """,
        rows,
    )
    return len(rows)


def _basic_status(row: QuarterAssessment) -> str:
    if not row.ticker or row.period_end_date is None or not _valid_date(row.period_end_date):
        return "NOT_ASSESSABLE"
    if "NO_MEANINGFUL_FINANCIAL_VALUES" in row.data_quality_warnings:
        return "EMPTY_OR_PLACEHOLDER"
    if row.quarter_basic_complete:
        return "BASIC_COMPLETE"
    if row.historical_research_ready:
        return "BASIC_PARTIAL"
    return "BASIC_INCOMPLETE"


def _historical_ingestion_status(row: QuarterAssessment) -> str:
    if _basic_status(row) == "NOT_ASSESSABLE":
        return "NOT_ASSESSABLE"
    return "UNKNOWN_HISTORICAL_INGEST_COMPLETENESS"


def write_audit_artifacts(payload: Mapping[str, Any], output_paths: Mapping[str, Path]) -> None:
    for path in output_paths.values():
        validate_temp_path(path).parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(output_paths["summary_json"], payload["summary"])
    _write_csv(output_paths["output_csv"], payload["all_quarters"])
    _write_csv(output_paths["ticker_csv"], payload["ticker_summary"])
    _write_csv(output_paths["field_csv"], payload["field_completeness"])
    _write_csv(output_paths["retry_csv"], payload["retry_candidates"])
    _write_csv(output_paths["latest_csv"], payload["latest_quarter_issues"])
    if "checkpoint_json" in output_paths:
        _write_json_atomic(output_paths["checkpoint_json"], payload)


def database_counts(conn: sqlite3.Connection) -> dict[str, int]:
    tables = ("rc_fundamental_quarterly", "rc_fundamental_ttm", "rc_earnings_event", "rc_fundamental_quarter_earnings_match")
    return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables}


def _recommend_retry(row: Mapping[str, Any], quarter_basic_complete: bool, missing_core: list[str], has_match: bool) -> str:
    period = _optional_text(_mapping_value(row, "period_end_date"))
    if period is None or not _valid_date(period):
        return "MANUAL_REVIEW"
    if quarter_basic_complete:
        return "NO_ACTION"
    old = bool(period and _valid_date(period) and date.fromisoformat(period) < date(2018, 1, 1))
    if old and not has_match:
        return "NOT_RETRYABLE"
    income_missing = any(field in missing_core for field in ("revenue", "ebit"))
    cashflow_missing = any(field in missing_core for field in ("operating_cashflow", "capex", "free_cashflow"))
    balance_missing = any(field in missing_core for field in ("cash", "total_debt", "shares_outstanding"))
    run_id = str(_mapping_value(row, "run_id") or "").upper()
    if income_missing and cashflow_missing and balance_missing:
        return "RETRY_YAHOO_AND_SEC"
    if "SEC" in run_id and (income_missing or cashflow_missing):
        return "RETRY_YAHOO"
    if balance_missing or cashflow_missing:
        return "RETRY_SEC"
    return "RETRY_YAHOO" if has_match else "MANUAL_REVIEW"


def _mapping_value(row: Mapping[str, Any], key: str) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError):
        return None


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _is_nullish(value: Any) -> bool:
    return value is None or value == ""


def _zeroish(value: Any) -> bool:
    try:
        return float(value) == 0.0
    except (TypeError, ValueError):
        return False


def _positive(value: Any) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def _has_meaningful_value(value: Any) -> bool:
    if _is_nullish(value):
        return False
    return not _zeroish(value)


def _is_present(value: Any) -> bool:
    return not _is_nullish(value)


def _quarter_ttm_field_available(row: Mapping[str, Any], field: str) -> bool:
    if field == "free_cashflow":
        return _is_present(_mapping_value(row, "free_cashflow")) or (
            _is_present(_mapping_value(row, "operating_cashflow"))
            and _is_present(_mapping_value(row, "capex"))
        )
    if field in {"operating_cashflow", "capex"} and _is_present(_mapping_value(row, "free_cashflow")):
        return True
    return _is_present(_mapping_value(row, field))


def _valid_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _percentage_payload(total: int, retries: Counter[str], assessments: list[QuarterAssessment]) -> dict[str, float]:
    def pct(value: int) -> float:
        return round(value / total * 100, 4) if total else 0.0

    return {
        "quarter_basic_complete_pct": pct(sum(1 for row in assessments if row.quarter_basic_complete)),
        "ttm_input_complete_pct": pct(sum(1 for row in assessments if row.ttm_input_complete)),
        "score_history_complete_pct": pct(sum(1 for row in assessments if row.score_history_complete)),
        "valuation_input_ready_pct": pct(sum(1 for row in assessments if row.valuation_input_ready)),
        "retry_yahoo_pct": pct(retries["RETRY_YAHOO"]),
        "retry_sec_pct": pct(retries["RETRY_SEC"]),
        "retry_both_pct": pct(retries["RETRY_YAHOO_AND_SEC"]),
    }


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='table' AND name=?
            """,
            (table_name,),
        ).fetchone()
        is not None
    )


def _csv_ready(row: dict[str, Any]) -> dict[str, Any]:
    return {key: json.dumps(value, sort_keys=True, separators=(",", ":")) if isinstance(value, list) else value for key, value in row.items()}


def _median(values: list[int]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2)


def _write_json_atomic(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    tmp = resolved.with_suffix(resolved.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    tmp.replace(resolved)


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    resolved = validate_temp_path(path)
    materialized = list(rows)
    with resolved.open("w", encoding="utf-8", newline="") as handle:
        if not materialized:
            handle.write("")
            return
        writer = csv.DictWriter(handle, fieldnames=list(materialized[0].keys()))
        writer.writeheader()
        writer.writerows(materialized)
