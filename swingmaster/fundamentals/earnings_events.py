from __future__ import annotations

import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd


DEFAULT_MARKET = "usa"
DEFAULT_SAFETY_MARGIN_DAYS = 120
DEFAULT_LIMIT_BUFFER_EVENTS = 8
YFINANCE_EARNINGS_DATES_LIMIT_CAP = 100
YFINANCE_EARNINGS_DATES_LIMIT_CAP_SOURCE = "yfinance 1.5.2 TickerBase.get_earnings_dates raises ValueError above 100"
FUNDAMENTALS_SOURCE_TABLE = "rc_fundamental_quarterly"
FUNDAMENTALS_PERIOD_END_COLUMN = "period_end_date"
YAHOO_SOURCE = "YAHOO_FINANCE"
YAHOO_TIMEZONE = "America/New_York"
REQUIRED_YAHOO_COLUMNS = ("Reported EPS", "EPS Estimate")
OPTIONAL_YAHOO_COLUMNS = ("Surprise(%)",)


class YahooEarningsError(RuntimeError):
    pass


class YahooEarningsParseError(YahooEarningsError):
    pass


@dataclass(frozen=True)
class EarningsRangePlan:
    ticker: str
    status: str
    oldest_required_period_end_date: str | None
    safety_margin_days: int
    fetch_lower_bound: str | None
    source_table: str
    source_period_end_column: str
    qualifying_fundamentals_row_count: int
    manual_start_date: str | None = None
    range_overridden: bool = False


@dataclass(frozen=True)
class LimitPlan:
    fetch_lower_bound: str
    as_of_date: str
    estimated_quarters: int
    buffer_events: int
    requested_limit: int
    uncapped_requested_limit: int
    cap: int
    capped: bool
    cap_source: str
    manual_limit: int | None = None
    limit_overridden: bool = False


@dataclass(frozen=True)
class EarningsEventRecord:
    market: str
    ticker: str
    announcement_at: str
    announcement_date: str
    announcement_session: str
    is_reported: bool
    reported_eps: float | None
    estimated_eps: float | None
    surprise_pct: float | None
    source: str
    source_observed_at_utc: str
    source_timezone: str


@dataclass(frozen=True)
class YahooParseDiagnostics:
    actual_columns: tuple[str, ...]
    raw_row_count: int
    rows_before_lower_bound: int
    completed_qualifying_count: int
    unreported_count: int
    duplicate_count: int
    invalid_count: int
    missing_required_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class CoverageAssessment:
    oldest_required_period_end_date: str | None
    fetch_lower_bound: str | None
    oldest_returned_completed_announcement_date: str | None
    newest_returned_completed_announcement_date: str | None
    covers_oldest_fundamentals_period: bool
    covers_fetch_lower_bound: bool
    coverage_status: str


@dataclass(frozen=True)
class YahooEarningsResult:
    ticker: str
    normalized_ticker: str
    requested_limit: int
    source_observed_at_utc: str
    records: tuple[EarningsEventRecord, ...]
    diagnostics: YahooParseDiagnostics
    coverage: CoverageAssessment
    status: str
    error_message: str | None = None


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_fundamentals_usa_db_path() -> Path:
    return repository_root() / "fundamentals_usa.db"


def normalize_ticker(ticker: str) -> str:
    normalized = str(ticker).strip().upper()
    if not normalized:
        raise ValueError("TICKER_REQUIRED")
    return normalized


def open_readonly_db(db_path: Path) -> sqlite3.Connection:
    resolved = db_path.expanduser().resolve()
    conn = sqlite3.connect(f"file:{resolved.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def plan_earnings_history_range(
    conn: sqlite3.Connection,
    ticker: str,
    *,
    safety_margin_days: int = DEFAULT_SAFETY_MARGIN_DAYS,
    manual_start_date: str | date | None = None,
) -> EarningsRangePlan:
    normalized_ticker = normalize_ticker(ticker)
    if safety_margin_days < 0:
        raise ValueError("SAFETY_MARGIN_DAYS_MUST_BE_NON_NEGATIVE")

    row = conn.execute(
        """
        SELECT COUNT(*) AS row_count, MIN(date(period_end_date)) AS oldest_period_end
        FROM rc_fundamental_quarterly
        WHERE UPPER(ticker) = ?
          AND period_end_date IS NOT NULL
          AND date(period_end_date) IS NOT NULL
        """,
        (normalized_ticker,),
    ).fetchone()
    row_count = int(row["row_count"] or 0) if row is not None else 0
    oldest_text = str(row["oldest_period_end"]) if row is not None and row["oldest_period_end"] is not None else None

    manual_text = _date_to_text(manual_start_date) if manual_start_date is not None else None
    if manual_text is not None:
        return EarningsRangePlan(
            ticker=normalized_ticker,
            status="OK",
            oldest_required_period_end_date=oldest_text,
            safety_margin_days=safety_margin_days,
            fetch_lower_bound=manual_text,
            source_table=FUNDAMENTALS_SOURCE_TABLE,
            source_period_end_column=FUNDAMENTALS_PERIOD_END_COLUMN,
            qualifying_fundamentals_row_count=row_count,
            manual_start_date=manual_text,
            range_overridden=True,
        )

    if oldest_text is None:
        return EarningsRangePlan(
            ticker=normalized_ticker,
            status="NO_FUNDAMENTALS_HISTORY",
            oldest_required_period_end_date=None,
            safety_margin_days=safety_margin_days,
            fetch_lower_bound=None,
            source_table=FUNDAMENTALS_SOURCE_TABLE,
            source_period_end_column=FUNDAMENTALS_PERIOD_END_COLUMN,
            qualifying_fundamentals_row_count=row_count,
        )

    oldest = date.fromisoformat(oldest_text)
    lower_bound = oldest - timedelta(days=safety_margin_days)
    return EarningsRangePlan(
        ticker=normalized_ticker,
        status="OK",
        oldest_required_period_end_date=oldest.isoformat(),
        safety_margin_days=safety_margin_days,
        fetch_lower_bound=lower_bound.isoformat(),
        source_table=FUNDAMENTALS_SOURCE_TABLE,
        source_period_end_column=FUNDAMENTALS_PERIOD_END_COLUMN,
        qualifying_fundamentals_row_count=row_count,
    )


def plan_yahoo_earnings_limit(
    fetch_lower_bound: str | date,
    *,
    as_of_date: str | date | None = None,
    buffer_events: int = DEFAULT_LIMIT_BUFFER_EVENTS,
    cap: int = YFINANCE_EARNINGS_DATES_LIMIT_CAP,
    manual_limit: int | None = None,
) -> LimitPlan:
    lower = _coerce_date(fetch_lower_bound)
    current = _coerce_date(as_of_date) if as_of_date is not None else datetime.now(timezone.utc).date()
    if buffer_events < 0:
        raise ValueError("BUFFER_EVENTS_MUST_BE_NON_NEGATIVE")
    if cap <= 0:
        raise ValueError("CAP_MUST_BE_POSITIVE")
    if manual_limit is not None:
        if manual_limit <= 0:
            raise ValueError("MANUAL_LIMIT_MUST_BE_POSITIVE")
        requested = min(manual_limit, cap)
        return LimitPlan(
            fetch_lower_bound=lower.isoformat(),
            as_of_date=current.isoformat(),
            estimated_quarters=0,
            buffer_events=buffer_events,
            requested_limit=requested,
            uncapped_requested_limit=manual_limit,
            cap=cap,
            capped=manual_limit > cap,
            cap_source=YFINANCE_EARNINGS_DATES_LIMIT_CAP_SOURCE,
            manual_limit=manual_limit,
            limit_overridden=True,
        )

    day_count = max((current - lower).days, 0)
    estimated_quarters = math.ceil(day_count / 365.25 * 4)
    uncapped = max(1, estimated_quarters + buffer_events)
    requested = min(uncapped, cap)
    return LimitPlan(
        fetch_lower_bound=lower.isoformat(),
        as_of_date=current.isoformat(),
        estimated_quarters=estimated_quarters,
        buffer_events=buffer_events,
        requested_limit=requested,
        uncapped_requested_limit=uncapped,
        cap=cap,
        capped=uncapped > cap,
        cap_source=YFINANCE_EARNINGS_DATES_LIMIT_CAP_SOURCE,
    )


def classify_announcement_session(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    try:
        timestamp = pd.Timestamp(value)
    except Exception:
        return "UNKNOWN"
    if pd.isna(timestamp):
        return "UNKNOWN"
    current_time = timestamp.timetz().replace(tzinfo=None)
    if current_time < time(9, 30):
        return "BEFORE_MARKET"
    if current_time <= time(16, 0):
        return "DURING_MARKET"
    return "AFTER_MARKET"


def parse_yahoo_earnings_dates(
    dataframe: Any,
    *,
    ticker: str,
    fetch_lower_bound: str | date,
    source_observed_at_utc: str | datetime,
    include_future: bool = False,
    market: str = DEFAULT_MARKET,
) -> tuple[tuple[EarningsEventRecord, ...], YahooParseDiagnostics]:
    normalized_ticker = normalize_ticker(ticker)
    lower_bound = _coerce_date(fetch_lower_bound)
    observed = _datetime_to_utc_text(source_observed_at_utc)
    normalized_market = str(market).strip().lower()
    if dataframe is None:
        return (
            (),
            YahooParseDiagnostics(
                actual_columns=(),
                raw_row_count=0,
                rows_before_lower_bound=0,
                completed_qualifying_count=0,
                unreported_count=0,
                duplicate_count=0,
                invalid_count=0,
            ),
        )
    if not isinstance(dataframe, pd.DataFrame):
        raise YahooEarningsParseError(f"YAHOO_EARNINGS_NOT_DATAFRAME:{type(dataframe).__name__}")

    actual_columns = tuple(str(column) for column in dataframe.columns)
    missing = tuple(column for column in REQUIRED_YAHOO_COLUMNS if column not in dataframe.columns)
    if missing:
        raise YahooEarningsParseError(
            f"YAHOO_EARNINGS_REQUIRED_COLUMNS_MISSING:{','.join(missing)} actual_columns={list(actual_columns)}"
        )

    records: list[EarningsEventRecord] = []
    seen_keys: set[tuple[str, str, str, str]] = set()
    rows_before_lower_bound = 0
    completed_count = 0
    unreported_count = 0
    duplicate_count = 0
    invalid_count = 0
    if dataframe.empty:
        return (
            (),
            YahooParseDiagnostics(
                actual_columns=actual_columns,
                raw_row_count=0,
                rows_before_lower_bound=0,
                completed_qualifying_count=0,
                unreported_count=0,
                duplicate_count=0,
                invalid_count=0,
            ),
        )

    for index_value, row in dataframe.iterrows():
        timestamp = _normalize_yahoo_timestamp(index_value)
        if timestamp is None:
            invalid_count += 1
            continue
        announcement_date = timestamp.date()
        reported_eps = _normalize_optional_float(row["Reported EPS"])
        estimated_eps = _normalize_optional_float(row["EPS Estimate"])
        surprise_pct = _normalize_optional_float(row["Surprise(%)"]) if "Surprise(%)" in dataframe.columns else None
        is_reported = reported_eps is not None
        if announcement_date < lower_bound:
            rows_before_lower_bound += 1
            continue
        if not is_reported:
            unreported_count += 1
            if not include_future:
                continue

        announcement_at = timestamp.isoformat()
        unique_key = (normalized_market, normalized_ticker, announcement_at, YAHOO_SOURCE)
        if unique_key in seen_keys:
            duplicate_count += 1
            continue
        seen_keys.add(unique_key)
        if is_reported:
            completed_count += 1
        records.append(
            EarningsEventRecord(
                market=normalized_market,
                ticker=normalized_ticker,
                announcement_at=announcement_at,
                announcement_date=announcement_date.isoformat(),
                announcement_session=classify_announcement_session(timestamp),
                is_reported=is_reported,
                reported_eps=reported_eps,
                estimated_eps=estimated_eps,
                surprise_pct=surprise_pct,
                source=YAHOO_SOURCE,
                source_observed_at_utc=observed,
                source_timezone=YAHOO_TIMEZONE,
            )
        )

    records.sort(key=lambda item: (item.announcement_at, item.ticker))
    return (
        tuple(records),
        YahooParseDiagnostics(
            actual_columns=actual_columns,
            raw_row_count=len(dataframe),
            rows_before_lower_bound=rows_before_lower_bound,
            completed_qualifying_count=completed_count,
            unreported_count=unreported_count,
            duplicate_count=duplicate_count,
            invalid_count=invalid_count,
        ),
    )


def assess_earnings_coverage(
    records: tuple[EarningsEventRecord, ...] | list[EarningsEventRecord],
    *,
    range_plan: EarningsRangePlan,
    source_failed: bool = False,
    parse_failed: bool = False,
) -> CoverageAssessment:
    if source_failed:
        status = "SOURCE_FAILED"
    elif parse_failed:
        status = "PARSE_FAILED"
    elif range_plan.status == "NO_FUNDAMENTALS_HISTORY":
        status = "NO_FUNDAMENTALS_HISTORY"
    else:
        completed_dates = sorted(
            record.announcement_date for record in records if record.is_reported
        )
        if not completed_dates:
            status = "NO_YAHOO_ROWS"
        else:
            oldest = completed_dates[0]
            covers_oldest = (
                range_plan.oldest_required_period_end_date is not None
                and oldest <= range_plan.oldest_required_period_end_date
            )
            covers_lower = range_plan.fetch_lower_bound is not None and oldest <= range_plan.fetch_lower_bound
            if covers_lower:
                status = "COVERAGE_OK"
            elif covers_oldest:
                status = "COVERAGE_PARTIAL"
            else:
                status = "COVERAGE_PARTIAL"
    completed_dates = sorted(record.announcement_date for record in records if record.is_reported)
    oldest_completed = completed_dates[0] if completed_dates else None
    newest_completed = completed_dates[-1] if completed_dates else None
    covers_oldest = (
        oldest_completed is not None
        and range_plan.oldest_required_period_end_date is not None
        and oldest_completed <= range_plan.oldest_required_period_end_date
    )
    covers_lower = (
        oldest_completed is not None
        and range_plan.fetch_lower_bound is not None
        and oldest_completed <= range_plan.fetch_lower_bound
    )
    return CoverageAssessment(
        oldest_required_period_end_date=range_plan.oldest_required_period_end_date,
        fetch_lower_bound=range_plan.fetch_lower_bound,
        oldest_returned_completed_announcement_date=oldest_completed,
        newest_returned_completed_announcement_date=newest_completed,
        covers_oldest_fundamentals_period=covers_oldest,
        covers_fetch_lower_bound=covers_lower,
        coverage_status=status,
    )


class YahooEarningsDateClient:
    def __init__(self, yf_module: Any | None = None) -> None:
        self._yf = yf_module if yf_module is not None else self._get_yfinance_module()

    def get_earnings_dates(self, ticker: str, *, limit: int) -> Any:
        return self._yf.Ticker(ticker).get_earnings_dates(limit=limit)

    def _get_yfinance_module(self) -> Any:
        import yfinance as yf

        return yf


def fetch_yahoo_earnings_events(
    *,
    ticker: str,
    range_plan: EarningsRangePlan,
    limit_plan: LimitPlan,
    include_future: bool = False,
    client: YahooEarningsDateClient | None = None,
    observed_at_utc: str | datetime | None = None,
) -> YahooEarningsResult:
    normalized_ticker = normalize_ticker(ticker)
    observed = _datetime_to_utc_text(observed_at_utc or datetime.now(timezone.utc))
    if range_plan.fetch_lower_bound is None:
        coverage = assess_earnings_coverage((), range_plan=range_plan)
        return YahooEarningsResult(
            ticker=ticker,
            normalized_ticker=normalized_ticker,
            requested_limit=limit_plan.requested_limit,
            source_observed_at_utc=observed,
            records=(),
            diagnostics=YahooParseDiagnostics((), 0, 0, 0, 0, 0, 0),
            coverage=coverage,
            status=coverage.coverage_status,
        )
    source_client = client if client is not None else YahooEarningsDateClient()
    try:
        dataframe = source_client.get_earnings_dates(normalized_ticker, limit=limit_plan.requested_limit)
    except Exception as exc:
        coverage = assess_earnings_coverage((), range_plan=range_plan, source_failed=True)
        return YahooEarningsResult(
            ticker=ticker,
            normalized_ticker=normalized_ticker,
            requested_limit=limit_plan.requested_limit,
            source_observed_at_utc=observed,
            records=(),
            diagnostics=YahooParseDiagnostics((), 0, 0, 0, 0, 0, 0),
            coverage=coverage,
            status="SOURCE_FAILED",
            error_message=str(exc),
        )
    try:
        records, diagnostics = parse_yahoo_earnings_dates(
            dataframe,
            ticker=normalized_ticker,
            fetch_lower_bound=range_plan.fetch_lower_bound,
            source_observed_at_utc=observed,
            include_future=include_future,
        )
    except YahooEarningsParseError as exc:
        coverage = assess_earnings_coverage((), range_plan=range_plan, parse_failed=True)
        actual_columns = tuple(str(column) for column in getattr(dataframe, "columns", ()))
        return YahooEarningsResult(
            ticker=ticker,
            normalized_ticker=normalized_ticker,
            requested_limit=limit_plan.requested_limit,
            source_observed_at_utc=observed,
            records=(),
            diagnostics=YahooParseDiagnostics(actual_columns, 0, 0, 0, 0, 0, 0),
            coverage=coverage,
            status="PARSE_FAILED",
            error_message=str(exc),
        )
    coverage = assess_earnings_coverage(records, range_plan=range_plan)
    return YahooEarningsResult(
        ticker=ticker,
        normalized_ticker=normalized_ticker,
        requested_limit=limit_plan.requested_limit,
        source_observed_at_utc=observed,
        records=records,
        diagnostics=diagnostics,
        coverage=coverage,
        status=coverage.coverage_status,
    )


def dataclass_to_dict(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    return value


def _coerce_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _date_to_text(value: str | date) -> str:
    return _coerce_date(value).isoformat()


def _datetime_to_utc_text(value: str | datetime) -> str:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        parsed = value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return float(value)


def _normalize_yahoo_timestamp(value: Any) -> pd.Timestamp | None:
    try:
        timestamp = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is None:
        return None
    return timestamp.tz_convert(ZoneInfo(YAHOO_TIMEZONE))
