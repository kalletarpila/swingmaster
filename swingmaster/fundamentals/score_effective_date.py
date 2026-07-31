from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


SCORE_EFFECTIVE_DATE_POLICY = "SOURCE_TTM_EFFECTIVE_DATE"
STATUS_RESOLVED = "RESOLVED"
STATUS_SOURCE_TTM_NOT_FOUND = "SOURCE_TTM_NOT_FOUND"
STATUS_SOURCE_TTM_EFFECTIVE_DATE_NULL = "SOURCE_TTM_EFFECTIVE_DATE_NULL"
STATUS_SOURCE_TTM_AMBIGUOUS = "SOURCE_TTM_AMBIGUOUS"
SCORE_EFFECTIVE_COLUMNS = (
    "score_effective_trading_date",
    "score_effective_date_status",
    "score_effective_date_policy",
    "score_effective_date_source_ttm_as_of_date",
)
SCORE_VALUE_COLUMNS = (
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
class ScoreEffectiveDateRow:
    ticker: str
    as_of_date: str
    source_ttm_as_of_date: str | None
    source_ttm_effective_trading_date: str | None
    score_effective_trading_date: str | None
    score_effective_date_status: str
    score_effective_date_policy: str
    fundamental_score: float | None
    fundamental_score_lifecycle: float | None
    score_rule: str | None
    score_rule_lifecycle: str | None


@dataclass(frozen=True)
class ScoreSelectionResult:
    found: bool
    reason: str
    row: dict[str, Any] | None


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def ensure_score_effective_date_schema(conn: sqlite3.Connection) -> None:
    existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
    for column in SCORE_EFFECTIVE_COLUMNS:
        if column not in existing:
            conn.execute(f"ALTER TABLE rc_fundamental_ttm ADD COLUMN {column} TEXT")
            existing.add(column)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamental_ttm_score_effective_date
        ON rc_fundamental_ttm(ticker, score_effective_trading_date)
        """
    )


def score_row_predicate() -> str:
    return """
    (
        fundamental_score IS NOT NULL
        OR fundamental_score_lifecycle IS NOT NULL
        OR score_rule IS NOT NULL
        OR score_rule_lifecycle IS NOT NULL
    )
    """


def load_score_rows(conn: sqlite3.Connection, tickers: list[str] | None = None) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    if tickers:
        placeholders = ", ".join("?" for _ in tickers)
        return conn.execute(
            f"""
            SELECT ticker, as_of_date, effective_trading_date, fundamental_score,
                   fundamental_score_lifecycle, score_rule, score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker IN ({placeholders})
              AND {score_row_predicate()}
            ORDER BY ticker ASC, as_of_date ASC
            """,
            tickers,
        ).fetchall()
    return conn.execute(
        f"""
        SELECT ticker, as_of_date, effective_trading_date, fundamental_score,
               fundamental_score_lifecycle, score_rule, score_rule_lifecycle
        FROM rc_fundamental_ttm
        WHERE {score_row_predicate()}
        ORDER BY ticker ASC, as_of_date ASC
        """
    ).fetchall()


def source_ttm_status_by_key(conn: sqlite3.Connection, tickers: list[str] | None = None) -> dict[tuple[str, str], tuple[str, str | None]]:
    conn.row_factory = sqlite3.Row
    params: list[str] = []
    ticker_filter = ""
    if tickers:
        placeholders = ", ".join("?" for _ in tickers)
        ticker_filter = f"WHERE ticker IN ({placeholders})"
        params = tickers
    rows = conn.execute(
        f"""
        SELECT ticker, as_of_date, COUNT(*) AS row_count, MAX(effective_trading_date) AS effective_trading_date
        FROM rc_fundamental_ttm
        {ticker_filter}
        GROUP BY ticker, as_of_date
        ORDER BY ticker ASC, as_of_date ASC
        """,
        params,
    ).fetchall()
    result: dict[tuple[str, str], tuple[str, str | None]] = {}
    for row in rows:
        key = (str(row["ticker"]), str(row["as_of_date"]))
        if int(row["row_count"]) > 1:
            result[key] = (STATUS_SOURCE_TTM_AMBIGUOUS, None)
        elif row["effective_trading_date"] is None:
            result[key] = (STATUS_SOURCE_TTM_EFFECTIVE_DATE_NULL, None)
        else:
            result[key] = (STATUS_RESOLVED, str(row["effective_trading_date"]))
    return result


def compute_score_effective_dates(conn: sqlite3.Connection, tickers: list[str] | None = None) -> list[ScoreEffectiveDateRow]:
    rows = load_score_rows(conn, tickers)
    source_map = source_ttm_status_by_key(conn, sorted({str(row["ticker"]) for row in rows}))
    return [compute_score_row(row, source_map) for row in rows]


def compute_score_row(
    score_row: Mapping[str, Any],
    source_map: Mapping[tuple[str, str], tuple[str, str | None]],
) -> ScoreEffectiveDateRow:
    ticker = str(score_row["ticker"])
    as_of_date = str(score_row["as_of_date"])
    status, source_effective = source_map.get((ticker, as_of_date), (STATUS_SOURCE_TTM_NOT_FOUND, None))
    score_effective = source_effective if status == STATUS_RESOLVED else None
    source_ttm_as_of = as_of_date if status in {STATUS_RESOLVED, STATUS_SOURCE_TTM_EFFECTIVE_DATE_NULL} else None
    return ScoreEffectiveDateRow(
        ticker=ticker,
        as_of_date=as_of_date,
        source_ttm_as_of_date=source_ttm_as_of,
        source_ttm_effective_trading_date=source_effective,
        score_effective_trading_date=score_effective,
        score_effective_date_status=status,
        score_effective_date_policy=SCORE_EFFECTIVE_DATE_POLICY,
        fundamental_score=_optional_float(_mapping_value(score_row, "fundamental_score")),
        fundamental_score_lifecycle=_optional_float(_mapping_value(score_row, "fundamental_score_lifecycle")),
        score_rule=_optional_str(_mapping_value(score_row, "score_rule")),
        score_rule_lifecycle=_optional_str(_mapping_value(score_row, "score_rule_lifecycle")),
    )


def apply_score_effective_date_rows(conn: sqlite3.Connection, rows: list[ScoreEffectiveDateRow]) -> dict[str, int]:
    ensure_score_effective_date_schema(conn)
    updates = 0
    unchanged = 0
    for row in rows:
        existing = conn.execute(
            """
            SELECT score_effective_trading_date, score_effective_date_status,
                   score_effective_date_policy, score_effective_date_source_ttm_as_of_date
            FROM rc_fundamental_ttm
            WHERE ticker = ? AND as_of_date = ?
            """,
            (row.ticker, row.as_of_date),
        ).fetchone()
        desired = (
            row.score_effective_trading_date,
            row.score_effective_date_status,
            row.score_effective_date_policy,
            row.source_ttm_as_of_date,
        )
        if existing is not None and tuple(existing) == desired:
            unchanged += 1
            continue
        conn.execute(
            """
            UPDATE rc_fundamental_ttm
            SET score_effective_trading_date = ?,
                score_effective_date_status = ?,
                score_effective_date_policy = ?,
                score_effective_date_source_ttm_as_of_date = ?
            WHERE ticker = ? AND as_of_date = ?
            """,
            (*desired, row.ticker, row.as_of_date),
        )
        updates += 1
    return {"inserted": 0, "score_value_updates": 0, "score_effective_date_updates": updates, "unchanged": unchanged}


def select_latest_score_current(conn: sqlite3.Connection, ticker: str) -> ScoreSelectionResult:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        f"""
        SELECT *
        FROM rc_fundamental_ttm
        WHERE ticker = ?
          AND {score_row_predicate()}
        ORDER BY as_of_date DESC, latest_period_end_date DESC, rowid DESC
        LIMIT 1
        """,
        (normalize_ticker(ticker),),
    ).fetchone()
    return ScoreSelectionResult(row is not None, "OK" if row is not None else "NO_CURRENT_SCORE", dict(row) if row is not None else None)


def select_latest_score_as_of(conn: sqlite3.Connection, ticker: str, as_of_date: str) -> ScoreSelectionResult:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        f"""
        SELECT *
        FROM rc_fundamental_ttm
        WHERE ticker = ?
          AND score_effective_trading_date IS NOT NULL
          AND score_effective_trading_date <= ?
          AND {score_row_predicate()}
        ORDER BY score_effective_trading_date DESC, as_of_date DESC, latest_period_end_date DESC, rowid DESC
        LIMIT 1
        """,
        (normalize_ticker(ticker), as_of_date),
    ).fetchone()
    return ScoreSelectionResult(row is not None, "OK" if row is not None else "NO_AVAILABLE_SCORE", dict(row) if row is not None else None)


def summarize(rows: list[ScoreEffectiveDateRow]) -> dict[str, Any]:
    resolved = [row for row in rows if row.score_effective_date_status == STATUS_RESOLVED]
    delays = [
        days_between(row.as_of_date, row.score_effective_trading_date)
        for row in resolved
        if row.score_effective_trading_date is not None
    ]
    delays = [delay for delay in delays if delay is not None]
    by_status: dict[str, int] = {}
    for row in rows:
        by_status[row.score_effective_date_status] = by_status.get(row.score_effective_date_status, 0) + 1
    return {
        "total_score_rows": len(rows),
        "resolved_score_effective_date_rows": by_status.get(STATUS_RESOLVED, 0),
        "source_ttm_not_found_rows": by_status.get(STATUS_SOURCE_TTM_NOT_FOUND, 0),
        "source_ttm_effective_date_null_rows": by_status.get(STATUS_SOURCE_TTM_EFFECTIVE_DATE_NULL, 0),
        "source_ttm_ambiguous_rows": by_status.get(STATUS_SOURCE_TTM_AMBIGUOUS, 0),
        "rows_whose_score_period_precedes_effective_date": sum(
            1
            for row in resolved
            if row.score_effective_trading_date is not None and row.as_of_date < row.score_effective_trading_date
        ),
        "median_score_availability_delay_days": int(median(delays)) if delays else None,
        "p95_score_availability_delay_days": percentile(delays, 0.95),
        "historically_unavailable_score_rows": len(rows) - by_status.get(STATUS_RESOLVED, 0),
        "status_counts": dict(sorted(by_status.items())),
    }


def invariant_counts(conn: sqlite3.Connection) -> dict[str, int]:
    counts = {
        "rc_fundamental_ttm": _table_count(conn, "rc_fundamental_ttm"),
        "score_rows_on_rc_fundamental_ttm": int(
            conn.execute(f"SELECT COUNT(*) FROM rc_fundamental_ttm WHERE {score_row_predicate()}").fetchone()[0]
        ),
        "rc_fundamental_quarterly": _table_count(conn, "rc_fundamental_quarterly"),
        "rc_fundamental_quarter_earnings_match": _table_count(conn, "rc_fundamental_quarter_earnings_match"),
        "rc_fundamental_score_percentile": _table_count(conn, "rc_fundamental_score_percentile"),
    }
    if _table_exists(conn, "rc_fundamental_quarterly_vintage"):
        counts["rc_fundamental_quarterly_vintage"] = _table_count(conn, "rc_fundamental_quarterly_vintage")
    if _table_exists(conn, "rc_fundamental_quarterly_field_provenance"):
        counts["rc_fundamental_quarterly_field_provenance"] = _table_count(conn, "rc_fundamental_quarterly_field_provenance")
    return counts


def verification_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
    has_schema = set(SCORE_EFFECTIVE_COLUMNS).issubset(columns)
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
    if not has_schema:
        return {"has_score_effective_date_schema": False, "duplicate_score_natural_keys": duplicate_keys}
    return {
        "has_score_effective_date_schema": True,
        "duplicate_score_natural_keys": duplicate_keys,
        "invalid_policy_rows": int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM rc_fundamental_ttm
                WHERE score_effective_date_status IS NOT NULL
                  AND score_effective_date_policy <> ?
                """,
                (SCORE_EFFECTIVE_DATE_POLICY,),
            ).fetchone()[0]
        ),
        "effective_date_mismatch_to_source_ttm": int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM rc_fundamental_ttm
                WHERE score_effective_date_status = ?
                  AND score_effective_trading_date IS NOT effective_trading_date
                """,
                (STATUS_RESOLVED,),
            ).fetchone()[0]
        ),
        "score_effective_date_before_source_ttm_effective_date": int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM rc_fundamental_ttm
                WHERE score_effective_date_status = ?
                  AND score_effective_trading_date < effective_trading_date
                """,
                (STATUS_RESOLVED,),
            ).fetchone()[0]
        ),
    }


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
    return {"backup_path": str(resolved), "backup_size_bytes": resolved.stat().st_size, "quick_check": quick_check, "counts": counts}


def score_effective_fields_hash(conn: sqlite3.Connection) -> str:
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
    if not set(SCORE_EFFECTIVE_COLUMNS).issubset(columns):
        return "SCHEMA_MISSING"
    digest = hashlib.sha256()
    for row in conn.execute(
        """
        SELECT ticker, as_of_date, score_effective_trading_date, score_effective_date_status,
               score_effective_date_policy, score_effective_date_source_ttm_as_of_date
        FROM rc_fundamental_ttm
        ORDER BY ticker, as_of_date
        """
    ):
        digest.update(json.dumps(tuple(row), sort_keys=True, default=str).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def score_value_fields_hash(conn: sqlite3.Connection) -> str:
    existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
    columns = [column for column in SCORE_VALUE_COLUMNS if column in existing]
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
    fieldnames = list(ScoreEffectiveDateRow.__dataclass_fields__)
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(resolved)


def rows_to_dicts(rows: Iterable[ScoreEffectiveDateRow]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]


def days_between(start: str, end: str | None) -> int | None:
    if end is None:
        return None
    return (datetime.strptime(end[:10], "%Y-%m-%d") - datetime.strptime(start[:10], "%Y-%m-%d")).days


def median(values: list[int]) -> float:
    sorted_values = sorted(values)
    count = len(sorted_values)
    midpoint = count // 2
    if count % 2:
        return float(sorted_values[midpoint])
    return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2


def percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    index = int(round((len(values) - 1) * fraction))
    return sorted(values)[index]


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _mapping_value(row: Mapping[str, Any], key: str) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError):
        return None


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone() is not None


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
