from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker, repository_root
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


EFFECTIVE_DATE_POLICY = "MAX_COMPONENT_QUARTER_EFFECTIVE_DATE"
STATUS_RESOLVED = "RESOLVED"
STATUS_MISSING_QUARTER_MATCH = "MISSING_QUARTER_MATCH"
STATUS_NULL_COMPONENT_EFFECTIVE_DATE = "NULL_COMPONENT_EFFECTIVE_DATE"
STATUS_INSUFFICIENT_COMPONENT_QUARTERS = "INSUFFICIENT_COMPONENT_QUARTERS"
TTM_EFFECTIVE_COLUMNS = (
    "effective_trading_date",
    "effective_date_status",
    "effective_date_policy",
    "effective_date_source_period_end",
    "effective_date_match_confidence",
    "effective_date_component_count",
)
FINANCIAL_HASH_COLUMNS = (
    "revenue_ttm",
    "revenue_growth_ttm_yoy",
    "ebit_ttm",
    "ebit_growth_ttm_yoy",
    "ebit_margin_ttm",
    "ebit_margin_trend_4q",
    "gross_margin_trend_4q",
    "fcf_ttm",
    "fcf_margin_ttm",
    "fcf_margin_trend_4q",
    "net_debt",
    "net_debt_to_ebitda",
    "share_dilution_yoy",
    "lifecycle_class",
    "fundamental_score",
    "growth_component",
    "margin_component",
    "margin_trend_component",
    "fcf_component",
    "leverage_component",
    "dilution_component",
    "lifecycle_component",
    "consistency_component",
    "score_rule",
    "fundamental_score_lifecycle",
    "score_rule_lifecycle",
    "growth_component_lifecycle",
    "margin_component_lifecycle",
    "margin_trend_component_lifecycle",
    "fcf_component_lifecycle",
    "leverage_component_lifecycle",
    "dilution_component_lifecycle",
    "lifecycle_component_lifecycle",
    "consistency_component_lifecycle",
)


@dataclass(frozen=True)
class TtmEffectiveDateRow:
    ticker: str
    as_of_date: str
    latest_period_end_date: str
    effective_trading_date: str | None
    effective_date_status: str
    effective_date_policy: str
    effective_date_source_period_end: str | None
    effective_date_match_confidence: str | None
    effective_date_component_count: int
    component_period_ends: str
    component_effective_dates: str


@dataclass(frozen=True)
class TtmSelectionResult:
    found: bool
    reason: str
    row: dict[str, Any] | None


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def infer_market(ticker: str) -> str:
    return "omxh" if normalize_ticker(ticker).endswith(".HE") else "usa"


def ensure_ttm_effective_date_schema(conn: sqlite3.Connection) -> None:
    existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
    for column in TTM_EFFECTIVE_COLUMNS:
        if column not in existing:
            column_type = "INTEGER" if column == "effective_date_component_count" else "TEXT"
            conn.execute(f"ALTER TABLE rc_fundamental_ttm ADD COLUMN {column} {column_type}")
            existing.add(column)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamental_ttm_ticker_effective_date
        ON rc_fundamental_ttm(ticker, effective_trading_date)
        """
    )


def load_target_ttm_rows(conn: sqlite3.Connection, tickers: list[str] | None = None) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    if tickers:
        placeholders = ", ".join("?" for _ in tickers)
        return conn.execute(
            f"""
            SELECT ticker, as_of_date, latest_period_end_date
            FROM rc_fundamental_ttm
            WHERE ticker IN ({placeholders})
            ORDER BY ticker ASC, as_of_date ASC
            """,
            tickers,
        ).fetchall()
    return conn.execute(
        """
        SELECT ticker, as_of_date, latest_period_end_date
        FROM rc_fundamental_ttm
        ORDER BY ticker ASC, as_of_date ASC
        """
    ).fetchall()


def compute_ttm_effective_dates(conn: sqlite3.Connection, tickers: list[str] | None = None) -> list[TtmEffectiveDateRow]:
    rows = load_target_ttm_rows(conn, tickers)
    quarterly_by_ticker = load_quarterly_periods(conn, sorted({str(row["ticker"]) for row in rows}))
    matches = load_quarter_matches(conn, sorted({str(row["ticker"]) for row in rows}))
    return [compute_row(row, quarterly_by_ticker.get(str(row["ticker"]), []), matches) for row in rows]


def load_quarterly_periods(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, list[str]]:
    if not tickers:
        return {}
    placeholders = ", ".join("?" for _ in tickers)
    result: dict[str, list[str]] = {ticker: [] for ticker in tickers}
    for row in conn.execute(
        f"""
        SELECT ticker, period_end_date
        FROM rc_fundamental_quarterly
        WHERE ticker IN ({placeholders})
          AND period_end_date IS NOT NULL
        ORDER BY ticker ASC, period_end_date ASC
        """,
        tickers,
    ):
        result.setdefault(str(row["ticker"]), []).append(str(row["period_end_date"]))
    return result


def load_quarter_matches(conn: sqlite3.Connection, tickers: list[str]) -> dict[tuple[str, str, str], sqlite3.Row]:
    if not tickers:
        return {}
    placeholders = ", ".join("?" for _ in tickers)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"""
        SELECT market, ticker, period_end_date, effective_trading_date, matching_confidence
        FROM rc_fundamental_quarter_earnings_match
        WHERE ticker IN ({placeholders})
        ORDER BY market ASC, ticker ASC, period_end_date ASC
        """,
        tickers,
    ).fetchall()
    return {
        (str(row["market"]), str(row["ticker"]), str(row["period_end_date"])): row
        for row in rows
    }


def compute_row(
    ttm_row: sqlite3.Row,
    quarterly_periods: list[str],
    matches: Mapping[tuple[str, str, str], sqlite3.Row],
) -> TtmEffectiveDateRow:
    ticker = str(ttm_row["ticker"])
    as_of_date = str(ttm_row["as_of_date"])
    latest_period_end = str(ttm_row["latest_period_end_date"])
    components = [period for period in quarterly_periods if period <= latest_period_end][-4:]
    if len(components) < 4:
        return _result(ttm_row, components, [], STATUS_INSUFFICIENT_COMPONENT_QUARTERS, None, None)
    market = infer_market(ticker)
    component_matches = [matches.get((market, ticker, period)) for period in components]
    if any(match is None for match in component_matches):
        return _result(ttm_row, components, [], STATUS_MISSING_QUARTER_MATCH, None, None)
    effective_dates = [
        str(match["effective_trading_date"])
        for match in component_matches
        if match is not None and match["effective_trading_date"] is not None
    ]
    if len(effective_dates) != 4:
        return _result(ttm_row, components, effective_dates, STATUS_NULL_COMPONENT_EFFECTIVE_DATE, None, None)
    confidence = _combined_confidence([str(match["matching_confidence"]) for match in component_matches if match is not None])
    return _result(ttm_row, components, effective_dates, STATUS_RESOLVED, max(effective_dates), confidence)


def _result(
    ttm_row: sqlite3.Row,
    components: list[str],
    effective_dates: list[str],
    status: str,
    effective_date: str | None,
    confidence: str | None,
) -> TtmEffectiveDateRow:
    return TtmEffectiveDateRow(
        ticker=str(ttm_row["ticker"]),
        as_of_date=str(ttm_row["as_of_date"]),
        latest_period_end_date=str(ttm_row["latest_period_end_date"]),
        effective_trading_date=effective_date,
        effective_date_status=status,
        effective_date_policy=EFFECTIVE_DATE_POLICY,
        effective_date_source_period_end=str(ttm_row["latest_period_end_date"]) if status == STATUS_RESOLVED else None,
        effective_date_match_confidence=confidence,
        effective_date_component_count=len(components),
        component_period_ends=",".join(components),
        component_effective_dates=",".join(effective_dates),
    )


def _combined_confidence(values: list[str]) -> str | None:
    if not values:
        return None
    if any(value == "LOW" for value in values):
        return "LOW"
    if any(value == "MEDIUM" for value in values):
        return "MEDIUM"
    return "HIGH"


def summarize(rows: list[TtmEffectiveDateRow]) -> dict[str, Any]:
    resolved = [row for row in rows if row.effective_date_status == STATUS_RESOLVED]
    delays = [
        days_between(row.latest_period_end_date, row.effective_trading_date)
        for row in resolved
        if row.effective_trading_date is not None
    ]
    delays = [delay for delay in delays if delay is not None]
    by_status: dict[str, int] = {}
    for row in rows:
        by_status[row.effective_date_status] = by_status.get(row.effective_date_status, 0) + 1
    return {
        "total_ttm_rows": len(rows),
        "resolved_effective_date_rows": by_status.get(STATUS_RESOLVED, 0),
        "missing_quarter_match_rows": by_status.get(STATUS_MISSING_QUARTER_MATCH, 0),
        "null_component_effective_date_rows": by_status.get(STATUS_NULL_COMPONENT_EFFECTIVE_DATE, 0),
        "insufficient_component_quarter_rows": by_status.get(STATUS_INSUFFICIENT_COMPONENT_QUARTERS, 0),
        "rows_whose_period_end_precedes_effective_date": sum(
            1 for row in resolved if row.effective_trading_date is not None and row.latest_period_end_date < row.effective_trading_date
        ),
        "median_ttm_availability_delay_days": int(median(delays)) if delays else None,
        "p95_ttm_availability_delay_days": percentile(delays, 0.95),
        "historically_unavailable_rows": len(rows) - by_status.get(STATUS_RESOLVED, 0),
        "status_counts": dict(sorted(by_status.items())),
    }


def apply_effective_date_rows(conn: sqlite3.Connection, rows: list[TtmEffectiveDateRow]) -> dict[str, int]:
    ensure_ttm_effective_date_schema(conn)
    updates = 0
    unchanged = 0
    for row in rows:
        existing = conn.execute(
            """
            SELECT effective_trading_date, effective_date_status, effective_date_policy,
                   effective_date_source_period_end, effective_date_match_confidence,
                   effective_date_component_count
            FROM rc_fundamental_ttm
            WHERE ticker = ? AND as_of_date = ?
            """,
            (row.ticker, row.as_of_date),
        ).fetchone()
        desired = (
            row.effective_trading_date,
            row.effective_date_status,
            row.effective_date_policy,
            row.effective_date_source_period_end,
            row.effective_date_match_confidence,
            row.effective_date_component_count,
        )
        if existing is not None and tuple(existing) == desired:
            unchanged += 1
            continue
        conn.execute(
            """
            UPDATE rc_fundamental_ttm
            SET effective_trading_date = ?,
                effective_date_status = ?,
                effective_date_policy = ?,
                effective_date_source_period_end = ?,
                effective_date_match_confidence = ?,
                effective_date_component_count = ?
            WHERE ticker = ? AND as_of_date = ?
            """,
            (*desired, row.ticker, row.as_of_date),
        )
        updates += 1
    return {
        "inserted": 0,
        "financial_value_updates": 0,
        "effective_date_updates": updates,
        "unchanged": unchanged,
    }


def select_latest_ttm_current(conn: sqlite3.Connection, ticker: str) -> TtmSelectionResult:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT *
        FROM rc_fundamental_ttm
        WHERE ticker = ?
        ORDER BY as_of_date DESC, latest_period_end_date DESC, rowid DESC
        LIMIT 1
        """,
        (normalize_ticker(ticker),),
    ).fetchone()
    return TtmSelectionResult(row is not None, "OK" if row is not None else "NO_CURRENT_TTM", dict(row) if row is not None else None)


def select_latest_ttm_as_of(conn: sqlite3.Connection, ticker: str, as_of_date: str) -> TtmSelectionResult:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT *
        FROM rc_fundamental_ttm
        WHERE ticker = ?
          AND effective_trading_date IS NOT NULL
          AND effective_trading_date <= ?
        ORDER BY effective_trading_date DESC, latest_period_end_date DESC, as_of_date DESC, rowid DESC
        LIMIT 1
        """,
        (normalize_ticker(ticker), as_of_date),
    ).fetchone()
    return TtmSelectionResult(row is not None, "OK" if row is not None else "NO_AVAILABLE_TTM", dict(row) if row is not None else None)


def create_sqlite_backup(source_db: Path, backup_path: Path) -> dict[str, Any]:
    resolved = validate_temp_path(backup_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    if resolved.exists():
        raise RuntimeError(f"BACKUP_ALREADY_EXISTS:{resolved}")
    with sqlite3.connect(str(source_db)) as source, sqlite3.connect(str(resolved)) as target:
        source.backup(target)
    with sqlite3.connect(str(resolved)) as conn:
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        counts = invariant_counts(conn)
    return {
        "backup_path": str(resolved),
        "backup_size_bytes": resolved.stat().st_size,
        "quick_check": quick_check,
        "counts": counts,
    }


def invariant_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "rc_fundamental_ttm": int(conn.execute("SELECT COUNT(*) FROM rc_fundamental_ttm").fetchone()[0]),
        "rc_fundamental_quarterly": int(conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]),
        "rc_fundamental_quarter_earnings_match": int(conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarter_earnings_match").fetchone()[0]),
    }


def verification_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
    has_effective = set(TTM_EFFECTIVE_COLUMNS).issubset(columns)
    duplicate_keys = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT ticker, as_of_date, COUNT(*) AS n
                FROM rc_fundamental_ttm
                GROUP BY ticker, as_of_date
                HAVING n > 1
            )
            """
        ).fetchone()[0]
    )
    if not has_effective:
        return {"has_effective_date_schema": False, "duplicate_ttm_natural_keys": duplicate_keys}
    return {
        "has_effective_date_schema": True,
        "duplicate_ttm_natural_keys": duplicate_keys,
        "invalid_policy_rows": int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM rc_fundamental_ttm
                WHERE effective_date_status IS NOT NULL
                  AND effective_date_policy <> ?
                """,
                (EFFECTIVE_DATE_POLICY,),
            ).fetchone()[0]
        ),
        "effective_date_earlier_than_ttm_period_end": int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM rc_fundamental_ttm
                WHERE effective_trading_date IS NOT NULL
                  AND effective_trading_date < latest_period_end_date
                """
            ).fetchone()[0]
        ),
    }


def effective_fields_hash(conn: sqlite3.Connection) -> str:
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
    if not set(TTM_EFFECTIVE_COLUMNS).issubset(columns):
        return "SCHEMA_MISSING"
    digest = hashlib.sha256()
    for row in conn.execute(
        """
        SELECT ticker, as_of_date, effective_trading_date, effective_date_status,
               effective_date_policy, effective_date_source_period_end,
               effective_date_match_confidence, effective_date_component_count
        FROM rc_fundamental_ttm
        ORDER BY ticker, as_of_date
        """
    ):
        digest.update(json.dumps(tuple(row), sort_keys=True, default=str).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def financial_fields_hash(conn: sqlite3.Connection) -> str:
    existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
    columns = [column for column in FINANCIAL_HASH_COLUMNS if column in existing]
    digest = hashlib.sha256()
    for row in conn.execute(
        f"""
        SELECT ticker, as_of_date, {", ".join(columns)}
        FROM rc_fundamental_ttm
        ORDER BY ticker, as_of_date
        """
    ):
        digest.update(json.dumps(tuple(row), sort_keys=True, default=str).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def write_json_atomic(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_name(resolved.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(resolved)


def write_csv_atomic(path: Path, rows: list[Mapping[str, Any]]) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(TtmEffectiveDateRow.__dataclass_fields__)
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(resolved)


def days_between(start: str, end: str | None) -> int | None:
    if end is None:
        return None
    return (datetime.strptime(end[:10], "%Y-%m-%d") - datetime.strptime(start[:10], "%Y-%m-%d")).days


def percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    index = int(round((len(values) - 1) * fraction))
    return sorted(values)[index]


def rows_to_dicts(rows: Iterable[TtmEffectiveDateRow]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]
