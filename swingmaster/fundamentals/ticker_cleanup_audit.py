from __future__ import annotations

import csv
import json
import random
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.fundamentals.earnings_events import normalize_ticker, repository_root


AUDIT_VERSION = "fundamentals_ticker_cleanup_audit_v1"
DEFAULT_MARKET = "usa"
USABLE_QUARTER_MIN_CORE_FIELDS = 2
CORE_QUARTERLY_FIELDS = (
    "revenue",
    "net_income",
    "operating_income",
    "ebit",
    "ebitda",
    "operating_cashflow",
    "free_cashflow",
    "cash",
    "total_debt",
    "shares_outstanding",
)
ACTIVE_TABLES = (
    "rc_fundamental_quarterly",
    "rc_fundamental_ttm",
    "rc_fundamental_score_percentile",
    "rc_fundamental_valuation",
    "rc_earnings_event",
    "rc_fundamental_quarter_earnings_match",
    "rc_fundamental_quarter_state",
    "rc_fundamental_quarterly_enrichment_audit",
    "rc_fundamental_statement_raw",
    "rc_fundamental_yahoo_raw",
    "rc_fundamental_yahoo_quarterly",
)
ARCHIVE_TABLES = (
    "rc_fundamental_quarterly_vintage",
    "rc_fundamental_quarterly_field_provenance",
)
OUTPUT_CSV_NAMES = {
    "all": "all_tickers.csv",
    "safe_remove": "safe_remove_candidates.csv",
    "manual_review": "manual_review_candidates.csv",
    "keep": "keep_candidates.csv",
}


@dataclass(frozen=True)
class TickerInventoryRow:
    ticker: str
    market: str
    company_name: str | None
    exchange: str | None
    security_type: str | None
    asset_type: str | None
    sector: str | None
    industry: str | None
    active_status: bool | None
    delisted_status: bool
    delisted_date: str | None
    first_seen_date: str | None
    last_seen_date: str | None
    quarterly_row_count: int
    usable_quarterly_rows: int
    placeholder_or_empty_rows: int
    oldest_usable_period: str | None
    newest_usable_period: str | None
    ttm_row_count: int
    score_row_count: int
    percentile_row_count: int
    valuation_row_count: int
    snapshot_row_count: int
    earnings_event_count: int
    quarter_earnings_match_count: int
    active_dependency_count: int
    archive_dependency_count: int
    classification: str
    cleanup_scope: str
    reason: str
    evidence: str


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
    conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def audit_ticker_cleanup(
    db_path: Path,
    *,
    tickers: list[str] | None = None,
    first_n: int | None = None,
    sample_size: int | None = None,
    random_seed: int = 0,
) -> dict[str, Any]:
    with open_readonly_db(db_path) as conn:
        pre_counts = database_immutability_counts(conn)
        tables = inspect_ticker_tables(conn)
        selected = select_tickers(conn, tickers=tickers, first_n=first_n, sample_size=sample_size, random_seed=random_seed)
        metadata = load_yahoo_metadata(conn)
        dependencies = dependency_details_bulk(conn, selected, tables)
        quarter_stats_by_ticker = quarterly_stats_bulk(conn, selected)
        first_last_by_ticker = first_last_seen_bulk(conn, selected, tables)
        rows = [
            classify_ticker_from_inventory(
                ticker=ticker,
                metadata=metadata.get(ticker),
                quarter_stats=quarter_stats_by_ticker[ticker],
                counts=dependencies[ticker],
                first_last=first_last_by_ticker.get(ticker, (None, None)),
                table_info=tables,
            )
            for ticker in selected
        ]
        summary = aggregate_inventory(rows, tables=tables, database_path=db_path, dependency_counts=dependencies)
        post_counts = database_immutability_counts(conn)
    return {
        "audit_version": AUDIT_VERSION,
        "database_path": str(db_path.resolve()),
        "pre_counts": pre_counts,
        "post_counts": post_counts,
        "database_content_unchanged": pre_counts == post_counts,
        "table_inventory": tables,
        "summary": summary,
        "all_tickers": [asdict(row) for row in rows],
        "dependency_details": dependencies,
    }


def inspect_ticker_tables(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    tables = [str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    result: dict[str, dict[str, Any]] = {}
    for table in tables:
        columns = [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")]
        ticker_column = _ticker_column(columns)
        if ticker_column is None:
            continue
        result[table] = {
            "row_count": _count(conn, table),
            "ticker_column": ticker_column,
            "columns": columns,
            "active_dependency": table in ACTIVE_TABLES,
            "archive_dependency": table in ARCHIVE_TABLES,
        }
    return result


def select_tickers(
    conn: sqlite3.Connection,
    *,
    tickers: list[str] | None = None,
    first_n: int | None = None,
    sample_size: int | None = None,
    random_seed: int = 0,
) -> list[str]:
    if tickers:
        selected = sorted(dict.fromkeys(normalize_ticker(ticker) for ticker in tickers))
    else:
        selected = load_all_tickers(conn)
    if first_n is not None:
        if first_n < 0:
            raise ValueError("FIRST_N_MUST_BE_NON_NEGATIVE")
        selected = selected[:first_n]
    if sample_size is not None:
        if sample_size < 0:
            raise ValueError("SAMPLE_SIZE_MUST_BE_NON_NEGATIVE")
        rng = random.Random(random_seed)
        selected = sorted(rng.sample(selected, min(sample_size, len(selected))))
    return selected


def load_all_tickers(conn: sqlite3.Connection) -> list[str]:
    tables = inspect_ticker_tables(conn)
    tickers: set[str] = set()
    for table, info in tables.items():
        column = str(info["ticker_column"])
        for row in conn.execute(f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"):
            value = str(row[0]).strip()
            if value:
                tickers.add(normalize_ticker(value))
    return sorted(tickers)


def classify_ticker(
    conn: sqlite3.Connection,
    ticker: str,
    metadata: Mapping[str, Any] | None,
    tables: Mapping[str, Mapping[str, Any]] | None = None,
) -> TickerInventoryRow:
    table_info = tables or inspect_ticker_tables(conn)
    normalized = normalize_ticker(ticker)
    quarter_stats = quarterly_stats(conn, normalized)
    counts = row_counts_by_table(conn, normalized, table_info)
    first_seen, last_seen = first_last_seen(conn, normalized, table_info)
    return classify_ticker_from_inventory(
        ticker=normalized,
        metadata=metadata,
        quarter_stats=quarter_stats,
        counts=counts,
        first_last=(first_seen, last_seen),
        table_info=table_info,
    )


def classify_ticker_from_inventory(
    *,
    ticker: str,
    metadata: Mapping[str, Any] | None,
    quarter_stats: Mapping[str, Any],
    counts: Mapping[str, int],
    first_last: tuple[str | None, str | None],
    table_info: Mapping[str, Mapping[str, Any]],
) -> TickerInventoryRow:
    normalized = normalize_ticker(ticker)
    active_dependency_count = sum(
        count
        for table, count in counts.items()
        if table_info.get(table, {}).get("active_dependency")
        and table not in {"rc_fundamental_quarterly", "rc_fundamental_yahoo_raw"}
    )
    archive_dependency_count = sum(
        count
        for table, count in counts.items()
        if table_info.get(table, {}).get("archive_dependency")
    )
    metadata = metadata or {}
    identity = identity_from_metadata(metadata)
    symbol_evidence = symbol_pattern_evidence(normalized)
    instrument = detect_instrument(normalized, identity)
    active_status = infer_active_status(metadata, counts)
    delisted_status, delisted_date = infer_delisted_status(metadata)
    if delisted_status:
        active_status = False
    first_seen, last_seen = first_last

    if quarter_stats["usable_quarterly_rows"] > 0:
        if delisted_status:
            classification = "KEEP_DELISTED_HISTORICAL_COMPANY"
            cleanup_scope = "EXCLUDE_FROM_ACTIVE_UNIVERSE"
            reason = "Delisted operating company has usable historical quarterly fundamentals; retain to avoid survivorship bias."
        else:
            classification = "KEEP_USABLE_QUARTERLY_HISTORY"
            cleanup_scope = "KEEP"
            reason = "Ticker has at least one usable quarterly fundamentals row."
    elif active_dependency_count > 0:
        classification = "KEEP_ACTIVE_DEPENDENCY"
        cleanup_scope = "KEEP"
        reason = "Ticker has active non-quarterly dependencies despite no usable quarterly row."
    elif quarter_stats["quarterly_row_count"] > 0 and quarter_stats["usable_quarterly_rows"] == 0:
        if instrument["category"] == "ETF_OR_FUND":
            classification = "REMOVE_ETF_OR_FUND"
            cleanup_scope = "DELETE_ONLY_EMPTY_FUNDAMENTAL_ROWS"
            reason = "Pooled instrument evidence and only empty or unusable quarterly placeholders."
        elif instrument["category"] == "INDEX_OR_BENCHMARK":
            classification = "REMOVE_INDEX_OR_BENCHMARK"
            cleanup_scope = "DELETE_ONLY_EMPTY_FUNDAMENTAL_ROWS"
            reason = "Index/benchmark evidence and only empty or unusable quarterly placeholders."
        elif instrument["category"] == "WARRANT_RIGHT_UNIT_OR_PREFERRED":
            classification = "REMOVE_WARRANT_RIGHT_UNIT_OR_PREFERRED"
            cleanup_scope = "DELETE_ONLY_EMPTY_FUNDAMENTAL_ROWS"
            reason = "Instrument variant evidence and only empty or unusable quarterly placeholders."
        elif instrument["category"] == "UNSUPPORTED_INSTRUMENT":
            classification = "REMOVE_UNSUPPORTED_INSTRUMENT"
            cleanup_scope = "DELETE_ONLY_EMPTY_FUNDAMENTAL_ROWS"
            reason = "Unsupported instrument evidence and only empty or unusable quarterly placeholders."
        elif quarter_stats["placeholder_or_empty_rows"] == quarter_stats["quarterly_row_count"]:
            classification = "REMOVE_EMPTY_PLACEHOLDER_ONLY"
            cleanup_scope = "DELETE_ONLY_EMPTY_FUNDAMENTAL_ROWS"
            reason = "Quarterly rows exist, but all are empty placeholders by the usable-quarter rule."
        else:
            classification = "KEEP_MANUAL_REVIEW"
            cleanup_scope = "MARK_INACTIVE"
            reason = "Quarterly rows exist but are not usable; evidence is insufficient for automatic removal."
    elif instrument["category"] == "ETF_OR_FUND":
        classification = "REMOVE_ETF_OR_FUND"
        cleanup_scope = "DELETE_TICKER_ENTIRELY"
        reason = "Pooled investment instrument evidence and no usable fundamentals/dependencies."
    elif instrument["category"] == "INDEX_OR_BENCHMARK":
        classification = "REMOVE_INDEX_OR_BENCHMARK"
        cleanup_scope = "DELETE_TICKER_ENTIRELY"
        reason = "Index or benchmark evidence and no usable fundamentals/dependencies."
    elif instrument["category"] == "WARRANT_RIGHT_UNIT_OR_PREFERRED":
        classification = "REMOVE_WARRANT_RIGHT_UNIT_OR_PREFERRED"
        cleanup_scope = "DELETE_TICKER_ENTIRELY"
        reason = "Warrant, right, unit, or preferred-share evidence and no usable fundamentals/dependencies."
    elif instrument["category"] == "UNSUPPORTED_INSTRUMENT":
        classification = "REMOVE_UNSUPPORTED_INSTRUMENT"
        cleanup_scope = "DELETE_TICKER_ENTIRELY"
        reason = "Unsupported instrument evidence and no usable fundamentals/dependencies."
    elif symbol_evidence:
        classification = "UNKNOWN_SECURITY_TYPE_REVIEW"
        cleanup_scope = "MARK_INACTIVE"
        reason = "Symbol pattern suggests unsupported instrument, but pattern alone is insufficient for removal."
    else:
        classification = "KEEP_MANUAL_REVIEW"
        cleanup_scope = "MARK_INACTIVE"
        reason = "No usable quarterly rows or active dependencies were found, but evidence does not prove an unsupported instrument or empty placeholder."

    evidence_parts = [
        f"usable_quarters={quarter_stats['usable_quarterly_rows']}",
        f"quarterly_rows={quarter_stats['quarterly_row_count']}",
        f"active_dependency_rows={active_dependency_count}",
        f"archive_dependency_rows={archive_dependency_count}",
        f"instrument={instrument['category']}",
    ]
    if instrument["evidence"]:
        evidence_parts.append(str(instrument["evidence"]))
    if symbol_evidence:
        evidence_parts.append("symbol_pattern=" + ",".join(symbol_evidence))

    return TickerInventoryRow(
        ticker=normalized,
        market=str(metadata.get("market") or metadata.get("exchange_market") or DEFAULT_MARKET),
        company_name=identity.get("company_name"),
        exchange=identity.get("exchange"),
        security_type=identity.get("security_type"),
        asset_type=identity.get("asset_type"),
        sector=identity.get("sector"),
        industry=identity.get("industry"),
        active_status=active_status,
        delisted_status=delisted_status,
        delisted_date=delisted_date,
        first_seen_date=first_seen,
        last_seen_date=last_seen,
        quarterly_row_count=quarter_stats["quarterly_row_count"],
        usable_quarterly_rows=quarter_stats["usable_quarterly_rows"],
        placeholder_or_empty_rows=quarter_stats["placeholder_or_empty_rows"],
        oldest_usable_period=quarter_stats["oldest_usable_period"],
        newest_usable_period=quarter_stats["newest_usable_period"],
        ttm_row_count=counts.get("rc_fundamental_ttm", 0),
        score_row_count=0,
        percentile_row_count=counts.get("rc_fundamental_score_percentile", 0),
        valuation_row_count=counts.get("rc_fundamental_valuation", 0),
        snapshot_row_count=0,
        earnings_event_count=counts.get("rc_earnings_event", 0),
        quarter_earnings_match_count=counts.get("rc_fundamental_quarter_earnings_match", 0),
        active_dependency_count=active_dependency_count,
        archive_dependency_count=archive_dependency_count,
        classification=classification,
        cleanup_scope=cleanup_scope,
        reason=reason,
        evidence="; ".join(evidence_parts),
    )


def quarterly_stats(conn: sqlite3.Connection, ticker: str) -> dict[str, Any]:
    field_expr = " + ".join(f"CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END" for field in CORE_QUARTERLY_FIELDS)
    zero_expr = " + ".join(f"CASE WHEN COALESCE({field}, 0) != 0 THEN 1 ELSE 0 END" for field in CORE_QUARTERLY_FIELDS)
    rows = conn.execute(
        f"""
        SELECT period_end_date,
               ({field_expr}) AS core_count,
               ({zero_expr}) AS non_zero_count
        FROM rc_fundamental_quarterly
        WHERE UPPER(ticker) = ?
        """,
        (ticker,),
    ).fetchall()
    usable_periods = [
        str(row["period_end_date"])
        for row in rows
        if _valid_date(row["period_end_date"])
        and int(row["core_count"] or 0) >= USABLE_QUARTER_MIN_CORE_FIELDS
        and int(row["non_zero_count"] or 0) > 0
    ]
    placeholder_count = len(rows) - len(usable_periods)
    return {
        "quarterly_row_count": len(rows),
        "usable_quarterly_rows": len(usable_periods),
        "placeholder_or_empty_rows": placeholder_count,
        "oldest_usable_period": min(usable_periods) if usable_periods else None,
        "newest_usable_period": max(usable_periods) if usable_periods else None,
    }


def quarterly_stats_bulk(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, dict[str, Any]]:
    selected = set(tickers)
    stats = {
        ticker: {
            "quarterly_row_count": 0,
            "usable_quarterly_rows": 0,
            "placeholder_or_empty_rows": 0,
            "oldest_usable_period": None,
            "newest_usable_period": None,
        }
        for ticker in tickers
    }
    if not _table_exists(conn, "rc_fundamental_quarterly"):
        return stats
    field_expr = " + ".join(f"CASE WHEN {field} IS NOT NULL THEN 1 ELSE 0 END" for field in CORE_QUARTERLY_FIELDS)
    zero_expr = " + ".join(f"CASE WHEN COALESCE({field}, 0) != 0 THEN 1 ELSE 0 END" for field in CORE_QUARTERLY_FIELDS)
    rows = conn.execute(
        f"""
        SELECT UPPER(ticker) AS ticker,
               period_end_date,
               ({field_expr}) AS core_count,
               ({zero_expr}) AS non_zero_count
        FROM rc_fundamental_quarterly
        WHERE ticker IS NOT NULL
        """
    )
    for row in rows:
        ticker = normalize_ticker(str(row["ticker"]))
        if ticker not in selected:
            continue
        item = stats[ticker]
        item["quarterly_row_count"] += 1
        usable = (
            _valid_date(row["period_end_date"])
            and int(row["core_count"] or 0) >= USABLE_QUARTER_MIN_CORE_FIELDS
            and int(row["non_zero_count"] or 0) > 0
        )
        if usable:
            item["usable_quarterly_rows"] += 1
            period = str(row["period_end_date"])
            if item["oldest_usable_period"] is None or period < str(item["oldest_usable_period"]):
                item["oldest_usable_period"] = period
            if item["newest_usable_period"] is None or period > str(item["newest_usable_period"]):
                item["newest_usable_period"] = period
    for item in stats.values():
        item["placeholder_or_empty_rows"] = item["quarterly_row_count"] - item["usable_quarterly_rows"]
    return stats


def row_counts_by_table(
    conn: sqlite3.Connection,
    ticker: str,
    tables: Mapping[str, Mapping[str, Any]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table, info in tables.items():
        column = str(info["ticker_column"])
        counts[table] = int(
            conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE UPPER({column}) = ?",
                (ticker,),
            ).fetchone()[0]
        )
    return counts


def dependency_details_bulk(
    conn: sqlite3.Connection,
    tickers: list[str],
    tables: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, int]]:
    selected = set(tickers)
    details = {ticker: {table: 0 for table in tables} for ticker in tickers}
    for table, info in tables.items():
        column = str(info["ticker_column"])
        rows = conn.execute(
            f"""
            SELECT UPPER({column}) AS ticker, COUNT(*) AS row_count
            FROM {table}
            WHERE {column} IS NOT NULL
            GROUP BY UPPER({column})
            """
        )
        for row in rows:
            ticker = normalize_ticker(str(row["ticker"]))
            if ticker in selected:
                details[ticker][table] = int(row["row_count"])
    return details


def load_yahoo_metadata(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not _table_exists(conn, "rc_fundamental_yahoo_raw"):
        return {}
    rows = conn.execute(
        """
        SELECT symbol, market, info_json, fast_info_json, status, loaded_at_utc
        FROM rc_fundamental_yahoo_raw
        ORDER BY symbol, loaded_at_utc
        """
    ).fetchall()
    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = normalize_ticker(str(row["symbol"]))
        info = _json_object(row["info_json"])
        fast = _json_object(row["fast_info_json"])
        if info or fast or ticker not in metadata:
            combined = {**fast, **info}
            combined["market"] = str(row["market"])
            combined["source_status"] = str(row["status"])
            combined["loaded_at_utc"] = str(row["loaded_at_utc"])
            metadata[ticker] = combined
    return metadata


def identity_from_metadata(metadata: Mapping[str, Any]) -> dict[str, str | None]:
    return {
        "company_name": _first_text(metadata, "longName", "shortName", "displayName"),
        "exchange": _first_text(metadata, "exchange", "fullExchangeName"),
        "security_type": _first_text(metadata, "quoteType", "typeDisp"),
        "asset_type": _first_text(metadata, "typeDisp", "quoteType"),
        "sector": _first_text(metadata, "sector", "sectorDisp"),
        "industry": _first_text(metadata, "industry", "industryDisp"),
    }


def detect_instrument(ticker: str, identity: Mapping[str, str | None]) -> dict[str, str]:
    text = " ".join(
        str(value or "")
        for value in (
            ticker,
            identity.get("company_name"),
            identity.get("security_type"),
            identity.get("asset_type"),
        )
    ).upper()
    quote = str(identity.get("security_type") or "").upper()
    name = str(identity.get("company_name") or "").upper()
    symbol_hits = symbol_pattern_evidence(ticker)
    if quote in {"ETF", "MUTUALFUND", "FUND"} or any(term in text for term in (" ETF", " EXCHANGE TRADED FUND", " MUTUAL FUND", " CLOSED END FUND")):
        return {"category": "ETF_OR_FUND", "evidence": "metadata/name indicates pooled fund"}
    if quote in {"INDEX"} or name.startswith("^") or " INDEX" in text or "BENCHMARK" in text:
        return {"category": "INDEX_OR_BENCHMARK", "evidence": "metadata/name indicates index or benchmark"}
    variant_terms = (" WARRANT", " RIGHT", " UNIT", " PREFERRED", " PFD", " DEPOSITARY SHARE")
    if any(term in text for term in variant_terms) or (
        symbol_hits and any(hit in {"warrant_suffix", "right_suffix", "unit_suffix", "preferred_suffix"} for hit in symbol_hits)
    ):
        if any(term in text for term in variant_terms):
            return {"category": "WARRANT_RIGHT_UNIT_OR_PREFERRED", "evidence": "metadata/name indicates instrument variant"}
        return {"category": "UNKNOWN_PATTERN_ONLY", "evidence": "symbol pattern only"}
    if "SPAC" in text or "ACQUISITION CORP" in text:
        return {"category": "UNSUPPORTED_INSTRUMENT", "evidence": "metadata/name indicates SPAC-related instrument"}
    return {"category": "OPERATING_COMPANY_OR_UNKNOWN", "evidence": ""}


def symbol_pattern_evidence(ticker: str) -> list[str]:
    hits: list[str] = []
    upper = ticker.upper()
    if upper.endswith((".W", ".WS", "-W", "-WS", "W.WS")):
        hits.append("warrant_suffix")
    if upper.endswith((".R", "-R", ".RT", "-RT")):
        hits.append("right_suffix")
    if upper.endswith((".U", "-U")):
        hits.append("unit_suffix")
    if any(token in upper for token in (".PR", "-PR", ".PRA", "-PRA", ".P", "-P")):
        hits.append("preferred_suffix")
    if upper.startswith("^"):
        hits.append("index_prefix")
    return hits


def infer_active_status(metadata: Mapping[str, Any], counts: Mapping[str, int]) -> bool | None:
    if metadata:
        quote_type = str(metadata.get("quoteType") or "").upper()
        if quote_type:
            return True
    if counts.get("rc_fundamental_quarter_state", 0) > 0:
        return True
    return None


def infer_delisted_status(metadata: Mapping[str, Any]) -> tuple[bool, str | None]:
    quote_type = str(metadata.get("quoteType") or "").upper()
    status = str(metadata.get("source_status") or "").upper()
    name = str(metadata.get("longName") or metadata.get("shortName") or "").upper()
    delisted = "DELIST" in name or "DELIST" in status or quote_type in {"NONE"}
    delisted_date = _first_text(metadata, "delistedDate", "delisted_date")
    return delisted, delisted_date


def first_last_seen(
    conn: sqlite3.Connection,
    ticker: str,
    tables: Mapping[str, Mapping[str, Any]],
) -> tuple[str | None, str | None]:
    dates: list[str] = []
    for table, date_column in {
        "rc_fundamental_quarterly": "period_end_date",
        "rc_fundamental_ttm": "as_of_date",
        "rc_earnings_event": "announcement_date",
        "rc_fundamental_quarter_earnings_match": "announcement_date",
        "rc_fundamental_yahoo_raw": "loaded_at_utc",
    }.items():
        if table not in tables:
            continue
        ticker_column = str(tables[table]["ticker_column"])
        rows = conn.execute(
            f"SELECT MIN({date_column}), MAX({date_column}) FROM {table} WHERE UPPER({ticker_column}) = ?",
            (ticker,),
        ).fetchone()
        for value in rows:
            if value:
                dates.append(str(value)[:10])
    return (min(dates), max(dates)) if dates else (None, None)


def first_last_seen_bulk(
    conn: sqlite3.Connection,
    tickers: list[str],
    tables: Mapping[str, Mapping[str, Any]],
) -> dict[str, tuple[str | None, str | None]]:
    selected = set(tickers)
    dates_by_ticker: dict[str, list[str]] = {ticker: [] for ticker in tickers}
    for table, date_column in {
        "rc_fundamental_quarterly": "period_end_date",
        "rc_fundamental_ttm": "as_of_date",
        "rc_earnings_event": "announcement_date",
        "rc_fundamental_quarter_earnings_match": "announcement_date",
        "rc_fundamental_yahoo_raw": "loaded_at_utc",
    }.items():
        if table not in tables or date_column not in tables[table]["columns"]:
            continue
        ticker_column = str(tables[table]["ticker_column"])
        rows = conn.execute(
            f"""
            SELECT UPPER({ticker_column}) AS ticker,
                   MIN({date_column}) AS first_seen,
                   MAX({date_column}) AS last_seen
            FROM {table}
            WHERE {ticker_column} IS NOT NULL
            GROUP BY UPPER({ticker_column})
            """
        )
        for row in rows:
            ticker = normalize_ticker(str(row["ticker"]))
            if ticker not in selected:
                continue
            for value in (row["first_seen"], row["last_seen"]):
                if value:
                    dates_by_ticker[ticker].append(str(value)[:10])
    return {
        ticker: ((min(dates), max(dates)) if dates else (None, None))
        for ticker, dates in dates_by_ticker.items()
    }


def aggregate_inventory(
    rows: list[TickerInventoryRow],
    *,
    tables: Mapping[str, Mapping[str, Any]],
    database_path: Path,
    dependency_counts: Mapping[str, Mapping[str, int]] | None = None,
) -> dict[str, Any]:
    category_counts: dict[str, int] = {}
    for row in rows:
        category_counts[row.classification] = category_counts.get(row.classification, 0) + 1
    safe_remove_classes = {
        "REMOVE_ETF_OR_FUND",
        "REMOVE_INDEX_OR_BENCHMARK",
        "REMOVE_WARRANT_RIGHT_UNIT_OR_PREFERRED",
        "REMOVE_UNSUPPORTED_INSTRUMENT",
        "REMOVE_EMPTY_PLACEHOLDER_ONLY",
    }
    manual_classes = {"KEEP_MANUAL_REVIEW", "UNKNOWN_SECURITY_TYPE_REVIEW"}
    projected: dict[str, int] = {}
    safe_rows = [row for row in rows if row.classification in safe_remove_classes]
    for table in tables:
        if dependency_counts is not None:
            projected[table] = sum(int(dependency_counts.get(row.ticker, {}).get(table, 0)) for row in safe_rows)
        else:
            field_name = _row_count_field(table)
            projected[table] = sum(int(getattr(row, field_name, 0)) if hasattr(row, field_name) else 0 for row in safe_rows)
    return {
        "database_path": str(database_path.resolve()),
        "total_distinct_tickers": len(rows),
        "tickers_with_quarterly_rows": sum(1 for row in rows if row.quarterly_row_count > 0),
        "tickers_with_usable_quarters": sum(1 for row in rows if row.usable_quarterly_rows > 0),
        "tickers_with_only_empty_placeholder_rows": sum(
            1 for row in rows if row.quarterly_row_count > 0 and row.usable_quarterly_rows == 0
        ),
        "tickers_without_quarterly_rows": sum(1 for row in rows if row.quarterly_row_count == 0),
        "tickers_with_active_dependencies_but_no_quarters": sum(
            1 for row in rows if row.quarterly_row_count == 0 and row.active_dependency_count > 0
        ),
        "etf_or_fund_candidates": category_counts.get("REMOVE_ETF_OR_FUND", 0),
        "index_candidates": category_counts.get("REMOVE_INDEX_OR_BENCHMARK", 0),
        "warrant_right_unit_preferred_candidates": category_counts.get("REMOVE_WARRANT_RIGHT_UNIT_OR_PREFERRED", 0),
        "unsupported_instrument_candidates": category_counts.get("REMOVE_UNSUPPORTED_INSTRUMENT", 0),
        "safe_remove_candidates": sum(1 for row in safe_rows),
        "manual_review_candidates": sum(1 for row in rows if row.classification in manual_classes),
        "delisted_historical_companies_kept": category_counts.get("KEEP_DELISTED_HISTORICAL_COMPANY", 0),
        "category_counts": dict(sorted(category_counts.items())),
        "projected_rows_affected_by_table": projected,
        "recommended_cleanup_policy": "EXCLUDE_OR_MARK_INACTIVE_FIRST;DELETE_ONLY_UNSUPPORTED_OR_EMPTY_AFTER_REVIEW",
    }


def dependency_details(
    conn: sqlite3.Connection,
    tickers: list[str],
    tables: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, int]]:
    return {
        ticker: row_counts_by_table(conn, ticker, tables)
        for ticker in tickers
    }


def database_immutability_counts(conn: sqlite3.Connection) -> dict[str, Any]:
    quick = conn.execute("PRAGMA quick_check").fetchone()[0]
    return {
        "quick_check": str(quick),
        "rc_fundamental_quarterly": _count_if_exists(conn, "rc_fundamental_quarterly"),
        "rc_fundamental_ttm": _count_if_exists(conn, "rc_fundamental_ttm"),
        "rc_earnings_event": _count_if_exists(conn, "rc_earnings_event"),
        "rc_fundamental_quarter_earnings_match": _count_if_exists(conn, "rc_fundamental_quarter_earnings_match"),
    }


def write_audit_artifacts(payload: Mapping[str, Any], root: Path) -> dict[str, str]:
    resolved_root = validate_temp_path(root)
    resolved_root.mkdir(parents=True, exist_ok=True)
    all_rows = list(payload["all_tickers"])
    safe_classes = {
        "REMOVE_ETF_OR_FUND",
        "REMOVE_INDEX_OR_BENCHMARK",
        "REMOVE_WARRANT_RIGHT_UNIT_OR_PREFERRED",
        "REMOVE_UNSUPPORTED_INSTRUMENT",
        "REMOVE_EMPTY_PLACEHOLDER_ONLY",
    }
    manual_classes = {"KEEP_MANUAL_REVIEW", "UNKNOWN_SECURITY_TYPE_REVIEW"}
    outputs = {
        "summary_json": resolved_root / "summary.json",
        "dependency_details_json": resolved_root / "dependency_details.json",
        "all_tickers_csv": resolved_root / OUTPUT_CSV_NAMES["all"],
        "safe_remove_candidates_csv": resolved_root / OUTPUT_CSV_NAMES["safe_remove"],
        "manual_review_candidates_csv": resolved_root / OUTPUT_CSV_NAMES["manual_review"],
        "keep_candidates_csv": resolved_root / OUTPUT_CSV_NAMES["keep"],
    }
    write_json_atomic(outputs["summary_json"], {key: payload[key] for key in ("audit_version", "database_path", "pre_counts", "post_counts", "database_content_unchanged", "summary", "table_inventory")})
    write_json_atomic(outputs["dependency_details_json"], payload["dependency_details"])
    write_csv_atomic(outputs["all_tickers_csv"], all_rows)
    write_csv_atomic(outputs["safe_remove_candidates_csv"], [row for row in all_rows if row["classification"] in safe_classes])
    write_csv_atomic(outputs["manual_review_candidates_csv"], [row for row in all_rows if row["classification"] in manual_classes])
    write_csv_atomic(outputs["keep_candidates_csv"], [row for row in all_rows if row["classification"].startswith("KEEP_")])
    return {key: str(path) for key, path in outputs.items()}


def write_json_atomic(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_name(resolved.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(resolved)


def write_csv_atomic(path: Path, rows: list[Mapping[str, Any]]) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(TickerInventoryRow.__dataclass_fields__)
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(resolved)


def _ticker_column(columns: Iterable[str]) -> str | None:
    lowered = {column.lower(): column for column in columns}
    if "ticker" in lowered:
        return lowered["ticker"]
    if "symbol" in lowered:
        return lowered["symbol"]
    return None


def _row_count_field(table: str) -> str:
    return {
        "rc_fundamental_quarterly": "quarterly_row_count",
        "rc_fundamental_ttm": "ttm_row_count",
        "rc_fundamental_score_percentile": "percentile_row_count",
        "rc_fundamental_valuation": "valuation_row_count",
        "rc_earnings_event": "earnings_event_count",
        "rc_fundamental_quarter_earnings_match": "quarter_earnings_match_count",
    }.get(table, "active_dependency_count")


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _count_if_exists(conn: sqlite3.Connection, table: str) -> int | None:
    if not _table_exists(conn, table):
        return None
    return _count(conn, table)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _json_object(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_text(metadata: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = metadata.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _valid_date(value: Any) -> bool:
    if value is None:
        return False
    try:
        datetime.strptime(str(value)[:10], "%Y-%m-%d")
    except ValueError:
        return False
    return True
