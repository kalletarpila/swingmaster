from __future__ import annotations

import csv
import json
import random
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker, repository_root


ASSESSMENT_POLICY_VERSION = "fundamental_quarter_completeness_v1"
DEFAULT_MARKET = "usa"

IDENTITY_AND_PERIOD_FIELDS = ("ticker", "period_end_date")
INCOME_STATEMENT_CORE_FIELDS = ("revenue", "gross_profit", "operating_income", "ebit", "net_income")
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
TTM_CONSUMER_FIELDS = ("revenue", "ebit", "free_cashflow", "gross_profit", "cash", "total_debt", "shares_outstanding")
SCORE_CRITICAL_QUARTER_FIELDS = ("revenue", "ebit", "free_cashflow")
SCORE_USEFUL_QUARTER_FIELDS = ("gross_profit", "cash", "total_debt", "shares_outstanding")
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
    basic_status: str
    ttm_ready: bool
    score_input_ready: bool
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

    available = [field for field in CORE_FIELDS if _has_meaningful_value(_mapping_value(row, field))]
    missing_core = [field for field in CORE_FIELDS if not _has_meaningful_value(_mapping_value(row, field))]
    missing_ttm = [field for field in TTM_CONSUMER_FIELDS if not _has_meaningful_value(_mapping_value(row, field))]
    missing_score = [
        field
        for field in (*SCORE_CRITICAL_QUARTER_FIELDS, *SCORE_USEFUL_QUARTER_FIELDS)
        if not _has_meaningful_value(_mapping_value(row, field))
    ]
    missing_valuation = [field for field in VALUATION_QUARTER_FIELDS if not _has_meaningful_value(_mapping_value(row, field))]

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

    ttm_ready = all(_has_meaningful_value(_mapping_value(row, field)) for field in SCORE_CRITICAL_QUARTER_FIELDS)
    score_ready = ttm_ready
    valuation_ready = _positive(_mapping_value(row, "shares_outstanding"))
    research_ready = bool(available) and "INVALID_PERIOD_END_DATE" not in warnings and not empty

    if ticker is None or "INVALID_PERIOD_END_DATE" in warnings:
        status = "NOT_ASSESSABLE"
    elif empty:
        status = "EMPTY_OR_PLACEHOLDER"
    elif (
        _has_meaningful_value(_mapping_value(row, "revenue"))
        and any(_has_meaningful_value(_mapping_value(row, field)) for field in ("gross_profit", "operating_income", "ebit", "net_income"))
        and _has_meaningful_value(_mapping_value(row, "free_cashflow"))
        and _positive(_mapping_value(row, "shares_outstanding"))
    ):
        status = "BASIC_COMPLETE"
    elif research_ready and (ttm_ready or len(available) >= 4):
        status = "BASIC_PARTIAL"
    elif research_ready:
        status = "BASIC_INCOMPLETE"
    else:
        status = "EMPTY_OR_PLACEHOLDER"

    if status in {"EMPTY_OR_PLACEHOLDER", "NOT_ASSESSABLE"}:
        ttm_ready = False
        score_ready = False
        valuation_ready = False
        research_ready = False

    return QuarterAssessment(
        market=market,
        ticker=ticker or "",
        period_end_date=period,
        earnings_event_id=_optional_int(_mapping_value(row, "earnings_event_id")),
        announcement_date=_optional_text(_mapping_value(row, "announcement_date")),
        effective_trading_date=_optional_text(_mapping_value(row, "effective_trading_date")),
        basic_status=status,
        ttm_ready=ttm_ready,
        score_input_ready=score_ready,
        valuation_input_ready=valuation_ready,
        historical_research_ready=research_ready,
        core_field_count=len(CORE_FIELDS),
        available_core_field_count=len(available),
        missing_core_fields=missing_core,
        missing_ttm_fields=missing_ttm,
        missing_score_fields=missing_score,
        missing_valuation_fields=missing_valuation,
        data_quality_warnings=warnings,
        retry_recommendation=_recommend_retry(row, status, missing_core, bool(_mapping_value(row, "earnings_event_id"))),
    )


def assess_ticker_quarter_history(assessments: list[QuarterAssessment]) -> dict[str, Any]:
    ordered = sorted(assessments, key=lambda item: item.period_end_date or "")
    ticker = ordered[0].ticker if ordered else ""
    latest = ordered[-1] if ordered else None
    recent = ordered[-4:] if len(ordered) >= 4 else ordered
    older = ordered[:-4]
    usable = {"BASIC_COMPLETE", "BASIC_PARTIAL"}
    latest_bad = latest is not None and latest.basic_status not in usable
    recent_bad = sum(1 for row in recent if row.basic_status not in usable)
    older_bad = sum(1 for row in older if row.basic_status not in usable)
    usable_count = sum(1 for row in ordered if row.basic_status in usable)
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
    counts = Counter(row.basic_status for row in ordered)
    return {
        "ticker": ticker,
        "quarter_count": len(ordered),
        "basic_complete_count": counts["BASIC_COMPLETE"],
        "partial_count": counts["BASIC_PARTIAL"],
        "incomplete_count": counts["BASIC_INCOMPLETE"] + counts["EMPTY_OR_PLACEHOLDER"] + counts["NOT_ASSESSABLE"],
        "latest_quarter_status": latest.basic_status if latest else None,
        "latest_period_end": latest.period_end_date if latest else None,
        "latest_earnings_announcement_date": latest.announcement_date if latest else None,
        "latest_ttm_ready": latest.ttm_ready if latest else False,
        "latest_score_ready": latest.score_input_ready if latest else False,
        "latest_valuation_ready": latest.valuation_input_ready if latest else False,
        "old_history_gap_count": older_bad,
        "recent_history_gap_count": recent_bad,
        "retry_candidate_count": sum(1 for row in ordered if row.retry_recommendation not in {"NO_ACTION", "NOT_RETRYABLE"}),
        "classification": classification,
    }


def summarize_quarter_completeness(assessments: list[QuarterAssessment], ticker_rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(assessments)
    counts = Counter(row.basic_status for row in assessments)
    retries = Counter(row.retry_recommendation for row in assessments)
    return {
        "assessment_policy_version": ASSESSMENT_POLICY_VERSION,
        "total_quarter_rows": total,
        "distinct_tickers": len({row.ticker for row in assessments}),
        "basic_complete_count": counts["BASIC_COMPLETE"],
        "basic_partial_count": counts["BASIC_PARTIAL"],
        "basic_incomplete_count": counts["BASIC_INCOMPLETE"],
        "empty_or_placeholder_count": counts["EMPTY_OR_PLACEHOLDER"],
        "not_assessable_count": counts["NOT_ASSESSABLE"],
        "ttm_ready_count": sum(1 for row in assessments if row.ttm_ready),
        "score_input_ready_count": sum(1 for row in assessments if row.score_input_ready),
        "valuation_input_ready_count": sum(1 for row in assessments if row.valuation_input_ready),
        "historical_research_ready_count": sum(1 for row in assessments if row.historical_research_ready),
        "retry_yahoo_count": retries["RETRY_YAHOO"],
        "retry_sec_count": retries["RETRY_SEC"],
        "retry_both_count": retries["RETRY_YAHOO_AND_SEC"],
        "manual_review_count": retries["MANUAL_REVIEW"],
        "not_retryable_count": retries["NOT_RETRYABLE"],
        "no_action_count": retries["NO_ACTION"],
        "percentages": _percentage_payload(total, counts, retries, assessments),
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
    incomplete_matched = [row for row in assessments if row.earnings_event_id is not None and row.basic_status not in {"BASIC_COMPLETE", "BASIC_PARTIAL"}]
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
    usable = {"BASIC_COMPLETE", "BASIC_PARTIAL"}
    for ticker, rows in by_ticker.items():
        ordered = sorted(rows, key=lambda item: item.period_end_date or "")
        recent = ordered[-4:]
        older = ordered[:-4]
        if ordered and ordered[-1].basic_status not in usable:
            latest_bad.append(ticker)
        if any(row.basic_status not in usable for row in recent):
            last_four_bad.append(ticker)
        if older and any(row.basic_status not in usable for row in older) and all(row.basic_status in usable for row in recent):
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
    if assessment.basic_status == "NOT_ASSESSABLE":
        return IngestionTransition("NOT_ASSESSABLE", "quarter cannot be assessed")
    if source_compare_complete:
        return IngestionTransition("INGEST_COMPLETE", "latest source non-null supported values matched persisted values")
    if assessment.basic_status == "BASIC_COMPLETE":
        return IngestionTransition("BASIC_COMPLETE", "quarter is complete for normal research use")
    return IngestionTransition("FUNDAMENTALS_PARTIAL", "stored quarter is useful but incomplete")


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


def _recommend_retry(row: Mapping[str, Any], status: str, missing_core: list[str], has_match: bool) -> str:
    if status == "NOT_ASSESSABLE":
        return "MANUAL_REVIEW"
    if status == "BASIC_COMPLETE":
        return "NO_ACTION"
    period = _optional_text(_mapping_value(row, "period_end_date"))
    old = bool(period and _valid_date(period) and date.fromisoformat(period) < date(2018, 1, 1))
    if old and not has_match:
        return "NOT_RETRYABLE"
    income_missing = any(field in missing_core for field in ("revenue", "gross_profit", "operating_income", "ebit", "net_income"))
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


def _valid_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _percentage_payload(total: int, counts: Counter[str], retries: Counter[str], assessments: list[QuarterAssessment]) -> dict[str, float]:
    def pct(value: int) -> float:
        return round(value / total * 100, 4) if total else 0.0

    return {
        "basic_complete_pct": pct(counts["BASIC_COMPLETE"]),
        "basic_partial_pct": pct(counts["BASIC_PARTIAL"]),
        "basic_incomplete_pct": pct(counts["BASIC_INCOMPLETE"]),
        "empty_or_placeholder_pct": pct(counts["EMPTY_OR_PLACEHOLDER"]),
        "not_assessable_pct": pct(counts["NOT_ASSESSABLE"]),
        "ttm_ready_pct": pct(sum(1 for row in assessments if row.ttm_ready)),
        "score_input_ready_pct": pct(sum(1 for row in assessments if row.score_input_ready)),
        "valuation_input_ready_pct": pct(sum(1 for row in assessments if row.valuation_input_ready)),
        "retry_yahoo_pct": pct(retries["RETRY_YAHOO"]),
        "retry_sec_pct": pct(retries["RETRY_SEC"]),
        "retry_both_pct": pct(retries["RETRY_YAHOO_AND_SEC"]),
    }


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
