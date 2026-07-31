from __future__ import annotations

import csv
import json
import random
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker, repository_root
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


AUDIT_VERSION = "fundamental_effective_date_audit_v1"
DEFAULT_REPRESENTATIVE_TICKERS = ("AAPL", "MSFT", "JPM", "XOM", "NVDA")
CONSERVATIVE_MISSING_MATCH_POLICY = "EXCLUDE_FROM_RETROSPECTIVE_USE"


@dataclass(frozen=True)
class SelectionComparison:
    ticker: str
    as_of_date: str
    current_selected_period_end: str | None
    safe_selected_period_end: str | None
    current_selected_effective_date: str | None
    safe_selected_effective_date: str | None
    period_selection_differs: bool
    lookahead_days: int | None
    matching_confidence: str | None
    effective_date_status: str | None
    matching_status: str | None
    availability_policy: str | None


@dataclass(frozen=True)
class ConsumerAuditRow:
    module_file: str
    function_or_cli: str
    input_tables: str
    output_tables: str
    workflow_type: str
    date_selection_logic: str
    current_availability_assumption: str
    possible_lookahead_risk: str
    recommended_future_action: str
    implementation_scope: str
    classification: str
    severity: str


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def open_readonly_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def default_output_root() -> Path:
    return repository_root() / "temp" / "fundamental_effective_date_audit" / utc_timestamp()


def audit_effective_date_usage(
    fundamentals_db: Path | None = None,
    *,
    tickers: list[str] | None = None,
    as_of_date: str | None = None,
    first_n: int | None = None,
    sample_size: int | None = None,
    random_seed: int = 0,
) -> dict[str, Any]:
    db_path = fundamentals_db or default_fundamentals_usa_db_path()
    selected_as_of_date = as_of_date or date.today().isoformat()
    with open_readonly_db(db_path) as conn:
        pre_counts = database_immutability_counts(conn)
        selected_tickers = select_tickers(
            conn,
            tickers=tickers,
            first_n=first_n,
            sample_size=sample_size,
            random_seed=random_seed,
        )
        comparisons = [compare_selection(conn, ticker, selected_as_of_date) for ticker in selected_tickers]
        representative = representative_comparisons(conn)
        impact = universe_impact(conn)
        consumers = consumer_audit_rows()
        post_counts = database_immutability_counts(conn)
    return {
        "audit_version": AUDIT_VERSION,
        "database_path": str(db_path.resolve()),
        "as_of_date": selected_as_of_date,
        "pre_counts": pre_counts,
        "post_counts": post_counts,
        "database_content_unchanged": pre_counts == post_counts,
        "selected_tickers": selected_tickers,
        "comparisons": [asdict(row) for row in comparisons],
        "representative_comparisons": [asdict(row) for row in representative],
        "universe_impact": impact,
        "consumer_dependency_map": [asdict(row) for row in consumers],
        "missing_match_policy_recommendation": CONSERVATIVE_MISSING_MATCH_POLICY,
        "architecture_recommendation": "LIGHTWEIGHT_HYBRID",
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
        selected = sorted(dict.fromkeys(normalize_ticker(ticker) for ticker in tickers if ticker.strip()))
    else:
        selected = usa_quarterly_universe(conn)
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


def usa_quarterly_universe(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT UPPER(ticker) AS ticker
        FROM rc_fundamental_quarterly
        WHERE ticker IS NOT NULL
          AND UPPER(ticker) NOT LIKE '%.HE'
          AND date(period_end_date) IS NOT NULL
        ORDER BY UPPER(ticker)
        """
    ).fetchall()
    return [str(row["ticker"]) for row in rows]


def compare_selection(conn: sqlite3.Connection, ticker: str, as_of_date: str) -> SelectionComparison:
    normalized = normalize_ticker(ticker)
    current = current_selected_quarter(conn, normalized, as_of_date)
    safe = safe_selected_quarter(conn, normalized, as_of_date)
    current_period = str(current["period_end_date"]) if current is not None else None
    safe_period = str(safe["period_end_date"]) if safe is not None else None
    current_match = match_for_period(conn, normalized, current_period) if current_period is not None else None
    current_effective = str(current_match["effective_trading_date"]) if current_match is not None and current_match["effective_trading_date"] is not None else None
    lookahead_days = None
    if current_effective is not None and current_effective > as_of_date:
        lookahead_days = (date.fromisoformat(current_effective) - date.fromisoformat(as_of_date)).days
    return SelectionComparison(
        ticker=normalized,
        as_of_date=as_of_date,
        current_selected_period_end=current_period,
        safe_selected_period_end=safe_period,
        current_selected_effective_date=current_effective,
        safe_selected_effective_date=str(safe["effective_trading_date"]) if safe is not None and safe["effective_trading_date"] is not None else None,
        period_selection_differs=current_period != safe_period,
        lookahead_days=lookahead_days,
        matching_confidence=str(current_match["matching_confidence"]) if current_match is not None else None,
        effective_date_status=str(current_match["effective_date_status"]) if current_match is not None else None,
        matching_status=str(current_match["matching_status"]) if current_match is not None else None,
        availability_policy=str(current_match["availability_policy"]) if current_match is not None else None,
    )


def current_selected_quarter(conn: sqlite3.Connection, ticker: str, as_of_date: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT ticker, period_end_date
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
          AND date(period_end_date) <= date(?)
        ORDER BY date(period_end_date) DESC, period_end_date DESC
        LIMIT 1
        """,
        (normalize_ticker(ticker), as_of_date),
    ).fetchone()


def safe_selected_quarter(conn: sqlite3.Connection, ticker: str, as_of_date: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT q.ticker, q.period_end_date, m.effective_trading_date, m.matching_confidence,
               m.effective_date_status, m.matching_status, m.availability_policy
        FROM rc_fundamental_quarterly q
        JOIN rc_fundamental_quarter_earnings_match m
          ON m.ticker = q.ticker
         AND m.period_end_date = q.period_end_date
        WHERE q.ticker = ?
          AND m.effective_trading_date IS NOT NULL
          AND date(m.effective_trading_date) <= date(?)
        ORDER BY date(q.period_end_date) DESC, q.period_end_date DESC
        LIMIT 1
        """,
        (normalize_ticker(ticker), as_of_date),
    ).fetchone()


def match_for_period(conn: sqlite3.Connection, ticker: str, period_end_date: str | None) -> sqlite3.Row | None:
    if period_end_date is None:
        return None
    return conn.execute(
        """
        SELECT period_end_date, effective_trading_date, matching_confidence,
               effective_date_status, matching_status, availability_policy
        FROM rc_fundamental_quarter_earnings_match
        WHERE ticker = ?
          AND period_end_date = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (normalize_ticker(ticker), period_end_date),
    ).fetchone()


def representative_comparisons(conn: sqlite3.Connection) -> list[SelectionComparison]:
    tickers = list(DEFAULT_REPRESENTATIVE_TICKERS)
    tickers.extend(find_representative_edge_tickers(conn))
    rows: list[SelectionComparison] = []
    seen: set[tuple[str, str]] = set()
    for ticker in tickers:
        for as_of_date in representative_dates_for_ticker(conn, ticker):
            key = (ticker, as_of_date)
            if key in seen:
                continue
            seen.add(key)
            rows.append(compare_selection(conn, ticker, as_of_date))
    return rows


def representative_dates_for_ticker(conn: sqlite3.Connection, ticker: str) -> list[str]:
    row = conn.execute(
        """
        SELECT period_end_date, effective_trading_date
        FROM rc_fundamental_quarter_earnings_match
        WHERE ticker = ?
          AND effective_trading_date IS NOT NULL
        ORDER BY date(period_end_date) DESC
        LIMIT 1
        """,
        (normalize_ticker(ticker),),
    ).fetchone()
    older = conn.execute(
        """
        SELECT effective_trading_date
        FROM rc_fundamental_quarter_earnings_match
        WHERE ticker = ?
          AND effective_trading_date IS NOT NULL
        ORDER BY date(period_end_date) ASC
        LIMIT 1
        """,
        (normalize_ticker(ticker),),
    ).fetchone()
    if row is None:
        return []
    period_end = date.fromisoformat(str(row["period_end_date"])[:10])
    effective = date.fromisoformat(str(row["effective_trading_date"])[:10])
    values = {
        (period_end + (effective - period_end) // 2).isoformat(),
        (effective.fromordinal(effective.toordinal() - 1)).isoformat(),
        effective.isoformat(),
        (effective.fromordinal(effective.toordinal() + 3)).isoformat(),
    }
    if older is not None:
        values.add(str(older["effective_trading_date"])[:10])
    return sorted(values)


def find_representative_edge_tickers(conn: sqlite3.Connection) -> list[str]:
    queries = [
        """
        SELECT ticker FROM rc_fundamental_quarter_earnings_match
        WHERE effective_trading_date IS NOT NULL
        GROUP BY ticker
        ORDER BY MAX(reporting_delay_days) DESC, ticker
        LIMIT 2
        """,
        """
        SELECT q.ticker
        FROM rc_fundamental_quarterly q
        LEFT JOIN rc_fundamental_quarter_earnings_match m
          ON m.ticker = q.ticker AND m.period_end_date = q.period_end_date
        WHERE q.ticker NOT LIKE '%.HE'
          AND m.id IS NULL
        GROUP BY q.ticker
        ORDER BY q.ticker
        LIMIT 2
        """,
        """
        SELECT ticker FROM rc_fundamental_quarter_earnings_match
        WHERE effective_trading_date IS NULL
        GROUP BY ticker
        ORDER BY ticker
        LIMIT 2
        """,
    ]
    tickers: list[str] = []
    for query in queries:
        for row in conn.execute(query):
            ticker = normalize_ticker(str(row["ticker"]))
            if ticker not in tickers:
                tickers.append(ticker)
    return tickers


def ttm_component_effective_date(conn: sqlite3.Connection, ticker: str, latest_period_end_date: str) -> str | None:
    rows = conn.execute(
        """
        SELECT q.period_end_date, m.effective_trading_date
        FROM rc_fundamental_quarterly q
        LEFT JOIN rc_fundamental_quarter_earnings_match m
          ON m.ticker = q.ticker
         AND m.period_end_date = q.period_end_date
        WHERE q.ticker = ?
          AND date(q.period_end_date) <= date(?)
        ORDER BY date(q.period_end_date) DESC, q.period_end_date DESC
        LIMIT 4
        """,
        (normalize_ticker(ticker), latest_period_end_date),
    ).fetchall()
    if len(rows) < 4 or any(row["effective_trading_date"] is None for row in rows):
        return None
    return max(str(row["effective_trading_date"]) for row in rows)


def _legacy_universe_impact_join(conn: sqlite3.Connection) -> dict[str, Any]:
    total_tickers = len(usa_quarterly_universe(conn))
    total_matched = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarter_earnings_match m
        JOIN rc_fundamental_quarterly q
          ON UPPER(q.ticker) = UPPER(m.ticker) AND q.period_end_date = m.period_end_date
        WHERE UPPER(q.ticker) NOT LIKE '%.HE'
        """,
    )
    positive_delay = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarter_earnings_match m
        JOIN rc_fundamental_quarterly q
          ON UPPER(q.ticker) = UPPER(m.ticker) AND q.period_end_date = m.period_end_date
        WHERE UPPER(q.ticker) NOT LIKE '%.HE'
          AND m.reporting_delay_days > 0
        """,
    )
    delays = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT m.reporting_delay_days
            FROM rc_fundamental_quarter_earnings_match m
            JOIN rc_fundamental_quarterly q
              ON UPPER(q.ticker) = UPPER(m.ticker) AND q.period_end_date = m.period_end_date
            WHERE UPPER(q.ticker) NOT LIKE '%.HE'
              AND m.reporting_delay_days IS NOT NULL
            ORDER BY m.reporting_delay_days
            """
        )
    ]
    unmatched = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly q
        LEFT JOIN rc_fundamental_quarter_earnings_match m
          ON UPPER(m.ticker) = UPPER(q.ticker) AND m.period_end_date = q.period_end_date
        WHERE UPPER(q.ticker) NOT LIKE '%.HE'
          AND m.id IS NULL
        """
    )
    null_effective = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarter_earnings_match m
        JOIN rc_fundamental_quarterly q
          ON UPPER(q.ticker) = UPPER(m.ticker) AND q.period_end_date = m.period_end_date
        WHERE UPPER(q.ticker) NOT LIKE '%.HE'
          AND m.effective_trading_date IS NULL
        """
    )
    ambiguous = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarter_earnings_match m
        JOIN rc_fundamental_quarterly q
          ON UPPER(q.ticker) = UPPER(m.ticker) AND q.period_end_date = m.period_end_date
        WHERE UPPER(q.ticker) NOT LIKE '%.HE'
          AND (m.matching_confidence <> 'HIGH' OR m.matching_status <> 'MATCHED_HIGH_CONFIDENCE')
        """
    )
    affected_ticker_count = _scalar(
        conn,
        """
        SELECT COUNT(DISTINCT q.ticker)
        FROM rc_fundamental_quarterly q
        JOIN rc_fundamental_quarter_earnings_match m
          ON UPPER(m.ticker) = UPPER(q.ticker) AND m.period_end_date = q.period_end_date
        WHERE UPPER(q.ticker) NOT LIKE '%.HE'
          AND m.effective_trading_date IS NOT NULL
          AND date(m.effective_trading_date) > date(q.period_end_date)
        """
    )
    affected_ttm_rows = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_ttm t
        JOIN rc_fundamental_quarter_earnings_match m
          ON UPPER(m.ticker) = UPPER(t.ticker) AND m.period_end_date = t.latest_period_end_date
        WHERE UPPER(t.ticker) NOT LIKE '%.HE'
          AND m.effective_trading_date IS NOT NULL
          AND date(m.effective_trading_date) > date(t.as_of_date)
        """
    )
    affected_percentile_rows = _count_if_exists(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_score_percentile p
        JOIN rc_fundamental_quarter_earnings_match m
          ON UPPER(m.ticker) = UPPER(p.ticker) AND m.period_end_date = p.as_of_date
        WHERE UPPER(p.ticker) NOT LIKE '%.HE'
          AND m.effective_trading_date IS NOT NULL
          AND date(m.effective_trading_date) > date(p.target_date)
        """,
        table="rc_fundamental_score_percentile",
    )
    affected_valuation_rows = _count_if_exists(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_valuation v
        JOIN rc_fundamental_quarter_earnings_match m
          ON UPPER(m.ticker) = UPPER(v.ticker) AND m.period_end_date = v.valuation_fundamental_as_of_date
        WHERE UPPER(v.ticker) NOT LIKE '%.HE'
          AND m.effective_trading_date IS NOT NULL
          AND date(m.effective_trading_date) > date(v.as_of_date)
        """,
        table="rc_fundamental_valuation",
    )
    return {
        "total_tickers": total_tickers,
        "total_matched_quarters": total_matched,
        "quarters_with_positive_reporting_delay": positive_delay,
        "median_reporting_delay_days": percentile(delays, 0.50),
        "p95_reporting_delay_days": percentile(delays, 0.95),
        "total_period_end_to_effective_date_days": sum(delays),
        "ticker_periods_where_current_and_safe_selection_can_differ": positive_delay,
        "historical_output_rows_potentially_affected": {
            "rc_fundamental_ttm": affected_ttm_rows,
            "rc_fundamental_score": affected_ttm_rows,
            "rc_fundamental_score_percentile": affected_percentile_rows,
            "rc_fundamental_valuation": affected_valuation_rows,
        },
        "affected_ticker_count": affected_ticker_count,
        "null_effective_date_match_count": null_effective,
        "unmatched_quarter_count": unmatched,
        "ambiguous_quarter_count": ambiguous,
    }


def consumer_audit_rows() -> list[ConsumerAuditRow]:
    rows = [
        ConsumerAuditRow("swingmaster/fundamentals/build_ttm.py", "load_quarterly_rows/build_ttm_rows", "rc_fundamental_quarterly", "rc_fundamental_ttm", "historical derived", "ORDER BY period_end_date; TTM as_of_date equals latest_period_end_date", "period_end_date", "TTM row exists before announcement effective date", "Add TTM availability date as max component effective_trading_date for historical use", "derived table rebuild policy", "DERIVED_TABLE_NEEDS_REBUILD_POLICY", "MATERIAL_HISTORICAL_LOOKAHEAD"),
        ConsumerAuditRow("swingmaster/fundamentals/score.py", "run_fundamental_scoring", "rc_fundamental_ttm", "rc_fundamental_ttm", "historical derived", "scores every TTM row ordered by as_of_date", "TTM as_of_date/period_end_date", "inherits TTM early availability", "Recompute historical scores after effective-dated TTM or filter query-time", "derived table rebuild policy", "DERIVED_TABLE_NEEDS_REBUILD_POLICY", "MATERIAL_HISTORICAL_LOOKAHEAD"),
        ConsumerAuditRow("swingmaster/fundamentals/score_percentile.py", "load_latest_percentile_snapshot/build_percentile_rows", "rc_fundamental_ttm, ticker_meta", "rc_fundamental_score_percentile", "historical cross-section", "MAX(t.as_of_date) <= target_date per ticker", "TTM as_of_date/period_end_date", "peer rows can include quarters not yet announced for that peer", "Historical percentiles must select each peer by effective availability", "query-time filter plus rebuild historical ranks", "HISTORICAL_RESEARCH_NEEDS_EFFECTIVE_DATE", "MATERIAL_HISTORICAL_LOOKAHEAD"),
        ConsumerAuditRow("swingmaster/cli/run_fundamental_valuation.py", "load_ttm_rows/build_valuation_row", "rc_fundamental_ttm, rc_fundamental_quarterly, osakedata", "rc_fundamental_valuation", "current and historical valuation", "latest TTM as_of_date <= valuation date; price pvm <= valuation date", "TTM as_of_date/period_end_date", "historical valuation can pair price date with later-announced fundamentals", "Keep current valuation unchanged; historical valuation should use effective-dated TTM", "query-time filter/rebuild historical valuations", "HISTORICAL_RESEARCH_NEEDS_EFFECTIVE_DATE", "MATERIAL_HISTORICAL_LOOKAHEAD"),
        ConsumerAuditRow("swingmaster/cli/run_fundamental_ticker_snapshot.py", "build_snapshot_matrix/load_latest_valuation_snapshot", "rc_fundamental_ttm, rc_fundamental_quarterly, rc_fundamental_score_percentile, rc_fundamental_valuation", "CSV/stdout", "display/report", "latest TTM rows by as_of_date desc; stored percentiles/valuations by as_of_date", "stored latest/period dates", "displaying latest is current-state; historical snapshot rows inherit upstream issue", "Do not change current latest snapshot; add metadata if historical snapshots become research inputs", "display only plus optional historical metadata", "DISPLAY_ONLY", "CURRENT_STATE_ONLY"),
        ConsumerAuditRow("swingmaster/cli/run_fundamental_quarter_state.py", "sync_from_quarterly", "rc_fundamental_quarterly", "rc_fundamental_quarter_state", "current state", "MAX(period_end_date) per ticker", "latest database period", "not a retrospective research boundary", "No effective-date change for current state", "none", "CURRENT_ONLY_NO_CHANGE", "CURRENT_STATE_ONLY"),
        ConsumerAuditRow("swingmaster/cli/run_fundamental_quarter_update.py", "quarter update orchestration", "provider data, rc_fundamental_quarter_state, quarterly/TTM/score/valuation", "current latest fundamentals tables", "current update", "detected/latest provider period and update time", "current provider availability", "not a defect for latest-state outputs", "Leave current update workflow unchanged; downstream historical rebuild policy separate", "none for current", "CURRENT_ONLY_NO_CHANGE", "CURRENT_STATE_ONLY"),
        ConsumerAuditRow("swingmaster/cli/run_range_universe.py and research scoring CLIs", "historical range/backtest/training", "state/feature tables, score inputs", "research datasets/model scores", "historical research", "historical signal/as_of dates", "depends on persisted features", "fundamental-derived features can be early if sourced from period-end tables", "Use effective-date-safe derived fundamentals when fundamentals enter research features", "historical research integration", "HISTORICAL_RESEARCH_NEEDS_EFFECTIVE_DATE", "UNDETERMINED"),
        ConsumerAuditRow("analysis/*_reader.py technical snapshot readers", "read_*_raw_export", "technical/ohlcv analysis tables", "snapshot sections", "historical technical", "confirmed signal dates/as_of_date", "technical confirmation date", "no fundamentals exposure", "No fundamentals effective-date change", "none", "DISPLAY_ONLY", "NO_LOOKAHEAD"),
        ConsumerAuditRow("swingmaster/cli/backfill_yahoo_earnings_events.py and matching CLIs", "earnings event backfill/match persistence", "rc_earnings_event, rc_fundamental_quarterly", "rc_fundamental_quarter_earnings_match", "source/match maintenance", "announcement/effective trading date", "explicit effective date", "source for future fix, not consumer issue", "Keep as canonical association", "none", "DIAGNOSTIC_ONLY", "NO_LOOKAHEAD"),
    ]
    return rows


def write_artifacts(payload: Mapping[str, Any], root: Path) -> dict[str, str]:
    resolved_root = validate_temp_path(root)
    resolved_root.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary_json": resolved_root / "summary.json",
        "comparisons_csv": resolved_root / "comparisons.csv",
        "consumer_map_csv": resolved_root / "consumer_dependency_map.csv",
        "representative_comparisons_csv": resolved_root / "representative_comparisons.csv",
    }
    summary = {key: payload[key] for key in ("audit_version", "database_path", "as_of_date", "pre_counts", "post_counts", "database_content_unchanged", "universe_impact", "missing_match_policy_recommendation", "architecture_recommendation")}
    write_json_atomic(paths["summary_json"], summary)
    write_csv_atomic(paths["comparisons_csv"], list(payload["comparisons"]))
    write_csv_atomic(paths["consumer_map_csv"], list(payload["consumer_dependency_map"]))
    write_csv_atomic(paths["representative_comparisons_csv"], list(payload["representative_comparisons"]))
    return {key: str(path) for key, path in paths.items()}


def write_json_atomic(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_name(resolved.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(resolved)


def write_csv_atomic(path: Path, rows: list[Mapping[str, Any]]) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(resolved)


def _legacy_universe_impact_scan(conn: sqlite3.Connection) -> dict[str, Any]:
    total_tickers = len(usa_quarterly_universe(conn))
    quarterly_keys = {
        (normalize_ticker(str(row["ticker"])), str(row["period_end_date"]))
        for row in conn.execute(
            """
            SELECT ticker, period_end_date
            FROM rc_fundamental_quarterly
            WHERE ticker IS NOT NULL
              AND UPPER(ticker) NOT LIKE '%.HE'
              AND date(period_end_date) IS NOT NULL
            """
        )
    }
    match_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    delays: list[int] = []
    positive_delay = 0
    null_effective = 0
    ambiguous = 0
    affected_tickers: set[str] = set()
    for row in conn.execute(
        """
        SELECT ticker, period_end_date, effective_trading_date, reporting_delay_days,
               matching_confidence, matching_status
        FROM rc_fundamental_quarter_earnings_match
        WHERE ticker IS NOT NULL
        """
    ):
        key = (normalize_ticker(str(row["ticker"])), str(row["period_end_date"]))
        if key not in quarterly_keys:
            continue
        delay = int(row["reporting_delay_days"]) if row["reporting_delay_days"] is not None else None
        effective = str(row["effective_trading_date"]) if row["effective_trading_date"] is not None else None
        match_by_key[key] = {"effective_trading_date": effective, "reporting_delay_days": delay}
        if delay is not None:
            delays.append(delay)
            if delay > 0:
                positive_delay += 1
        if effective is None:
            null_effective += 1
        if row["matching_confidence"] != "HIGH" or row["matching_status"] != "MATCHED_HIGH_CONFIDENCE":
            ambiguous += 1
        if effective is not None and effective > key[1]:
            affected_tickers.add(key[0])
    delays.sort()
    affected_ttm_rows = count_rows_early_by_match(
        conn,
        table="rc_fundamental_ttm",
        ticker_column="ticker",
        period_column="latest_period_end_date",
        decision_date_column="as_of_date",
        match_by_key=match_by_key,
    )
    affected_percentile_rows = (
        count_rows_early_by_match(
            conn,
            table="rc_fundamental_score_percentile",
            ticker_column="ticker",
            period_column="as_of_date",
            decision_date_column="target_date",
            match_by_key=match_by_key,
        )
        if _table_exists(conn, "rc_fundamental_score_percentile")
        else None
    )
    affected_valuation_rows = (
        count_rows_early_by_match(
            conn,
            table="rc_fundamental_valuation",
            ticker_column="ticker",
            period_column="valuation_fundamental_as_of_date",
            decision_date_column="as_of_date",
            match_by_key=match_by_key,
        )
        if _table_exists(conn, "rc_fundamental_valuation")
        else None
    )
    return {
        "total_tickers": total_tickers,
        "total_matched_quarters": len(match_by_key),
        "quarters_with_positive_reporting_delay": positive_delay,
        "median_reporting_delay_days": percentile(delays, 0.50),
        "p95_reporting_delay_days": percentile(delays, 0.95),
        "total_period_end_to_effective_date_days": sum(delays),
        "ticker_periods_where_current_and_safe_selection_can_differ": positive_delay,
        "historical_output_rows_potentially_affected": {
            "rc_fundamental_ttm": affected_ttm_rows,
            "rc_fundamental_score": affected_ttm_rows,
            "rc_fundamental_score_percentile": affected_percentile_rows,
            "rc_fundamental_valuation": affected_valuation_rows,
        },
        "affected_ticker_count": len(affected_tickers),
        "null_effective_date_match_count": null_effective,
        "unmatched_quarter_count": len(quarterly_keys - set(match_by_key)),
        "ambiguous_quarter_count": ambiguous,
    }


def count_rows_early_by_match(
    conn: sqlite3.Connection,
    *,
    table: str,
    ticker_column: str,
    period_column: str,
    decision_date_column: str,
    match_by_key: Mapping[tuple[str, str], Mapping[str, Any]],
) -> int:
    count = 0
    for row in conn.execute(
        f"""
        SELECT {ticker_column}, {period_column}, {decision_date_column}
        FROM {table}
        WHERE {ticker_column} IS NOT NULL
          AND UPPER({ticker_column}) NOT LIKE '%.HE'
          AND {period_column} IS NOT NULL
          AND {decision_date_column} IS NOT NULL
        """
    ):
        key = (normalize_ticker(str(row[0])), str(row[1]))
        effective = match_by_key.get(key, {}).get("effective_trading_date")
        if effective is not None and str(effective) > str(row[2]):
            count += 1
    return count


def database_immutability_counts(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "quick_check": str(conn.execute("PRAGMA quick_check").fetchone()[0]),
        "rc_fundamental_quarterly": _table_count(conn, "rc_fundamental_quarterly"),
        "rc_earnings_event": _table_count(conn, "rc_earnings_event"),
        "rc_fundamental_quarter_earnings_match": _table_count(conn, "rc_fundamental_quarter_earnings_match"),
    }


def universe_impact(conn: sqlite3.Connection) -> dict[str, Any]:
    total_tickers = len(usa_quarterly_universe(conn))
    total_matched = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarter_earnings_match m
        JOIN rc_fundamental_quarterly q
          ON q.ticker = m.ticker AND q.period_end_date = m.period_end_date
        WHERE q.ticker NOT LIKE '%.HE'
        """,
    )
    positive_delay = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarter_earnings_match m
        JOIN rc_fundamental_quarterly q
          ON q.ticker = m.ticker AND q.period_end_date = m.period_end_date
        WHERE q.ticker NOT LIKE '%.HE'
          AND m.reporting_delay_days > 0
        """,
    )
    delays = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT m.reporting_delay_days
            FROM rc_fundamental_quarter_earnings_match m
            JOIN rc_fundamental_quarterly q
              ON q.ticker = m.ticker AND q.period_end_date = m.period_end_date
            WHERE q.ticker NOT LIKE '%.HE'
              AND m.reporting_delay_days IS NOT NULL
            ORDER BY m.reporting_delay_days
            """
        )
    ]
    unmatched = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly q
        LEFT JOIN rc_fundamental_quarter_earnings_match m
          ON m.ticker = q.ticker AND m.period_end_date = q.period_end_date
        WHERE q.ticker NOT LIKE '%.HE'
          AND m.id IS NULL
        """
    )
    null_effective = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarter_earnings_match m
        JOIN rc_fundamental_quarterly q
          ON q.ticker = m.ticker AND q.period_end_date = m.period_end_date
        WHERE q.ticker NOT LIKE '%.HE'
          AND m.effective_trading_date IS NULL
        """
    )
    ambiguous = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarter_earnings_match m
        JOIN rc_fundamental_quarterly q
          ON q.ticker = m.ticker AND q.period_end_date = m.period_end_date
        WHERE q.ticker NOT LIKE '%.HE'
          AND (m.matching_confidence <> 'HIGH' OR m.matching_status <> 'MATCHED_HIGH_CONFIDENCE')
        """
    )
    affected_ticker_count = _scalar(
        conn,
        """
        SELECT COUNT(DISTINCT q.ticker)
        FROM rc_fundamental_quarterly q
        JOIN rc_fundamental_quarter_earnings_match m
          ON m.ticker = q.ticker AND m.period_end_date = q.period_end_date
        WHERE q.ticker NOT LIKE '%.HE'
          AND m.effective_trading_date IS NOT NULL
          AND m.effective_trading_date > q.period_end_date
        """
    )
    affected_ttm_rows = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_ttm t
        JOIN rc_fundamental_quarter_earnings_match m
          ON m.ticker = t.ticker AND m.period_end_date = t.latest_period_end_date
        WHERE t.ticker NOT LIKE '%.HE'
          AND m.effective_trading_date IS NOT NULL
          AND m.effective_trading_date > t.as_of_date
        """
    )
    affected_percentile_rows = None
    affected_valuation_rows = _count_if_exists(
        conn,
        """
        SELECT COUNT(*)
        FROM rc_fundamental_valuation v
        JOIN rc_fundamental_quarter_earnings_match m
          ON m.ticker = v.ticker AND m.period_end_date = v.valuation_fundamental_as_of_date
        WHERE v.ticker NOT LIKE '%.HE'
          AND m.effective_trading_date IS NOT NULL
          AND m.effective_trading_date > v.as_of_date
        """,
        table="rc_fundamental_valuation",
    )
    return {
        "total_tickers": total_tickers,
        "total_matched_quarters": total_matched,
        "quarters_with_positive_reporting_delay": positive_delay,
        "median_reporting_delay_days": percentile(delays, 0.50),
        "p95_reporting_delay_days": percentile(delays, 0.95),
        "total_period_end_to_effective_date_days": sum(delays),
        "ticker_periods_where_current_and_safe_selection_can_differ": positive_delay,
        "historical_output_rows_potentially_affected": {
            "rc_fundamental_ttm": affected_ttm_rows,
            "rc_fundamental_score": affected_ttm_rows,
            "rc_fundamental_score_percentile": affected_percentile_rows,
            "rc_fundamental_valuation": affected_valuation_rows,
        },
        "historical_output_rows_potentially_affected_notes": {
            "rc_fundamental_score_percentile": "exact count skipped because direct comparison over 4.5M rows is too costly for this read-only audit; selection logic is still materially exposed",
        },
        "affected_ticker_count": affected_ticker_count,
        "null_effective_date_match_count": null_effective,
        "unmatched_quarter_count": unmatched,
        "ambiguous_quarter_count": ambiguous,
    }


def classify_consumer(workflow_type: str, reads_fundamentals: bool, historical: bool) -> str:
    if not reads_fundamentals:
        return "DISPLAY_ONLY"
    if not historical:
        return "CURRENT_ONLY_NO_CHANGE"
    if "derived" in workflow_type:
        return "DERIVED_TABLE_NEEDS_REBUILD_POLICY"
    return "HISTORICAL_RESEARCH_NEEDS_EFFECTIVE_DATE"


def classify_severity(classification: str) -> str:
    return {
        "CURRENT_ONLY_NO_CHANGE": "CURRENT_STATE_ONLY",
        "DISPLAY_ONLY": "NO_LOOKAHEAD",
        "DIAGNOSTIC_ONLY": "NO_LOOKAHEAD",
        "DERIVED_TABLE_NEEDS_REBUILD_POLICY": "MATERIAL_HISTORICAL_LOOKAHEAD",
        "HISTORICAL_RESEARCH_NEEDS_EFFECTIVE_DATE": "MATERIAL_HISTORICAL_LOOKAHEAD",
    }.get(classification, "UNDETERMINED")


def percentile(values: list[int], fraction: float) -> int | None:
    if not values:
        return None
    index = int(round((len(values) - 1) * fraction))
    return values[index]


def _scalar(conn: sqlite3.Connection, query: str) -> int:
    return int(conn.execute(query).fetchone()[0])


def _count_if_exists(conn: sqlite3.Connection, query: str, *, table: str) -> int | None:
    if not _table_exists(conn, table):
        return None
    return _scalar(conn, query)


def _table_count(conn: sqlite3.Connection, table: str) -> int | None:
    if not _table_exists(conn, table):
        return None
    return _scalar(conn, f"SELECT COUNT(*) FROM {table}")


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None
