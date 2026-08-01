from __future__ import annotations

import csv
import json
import random
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker, repository_root
from swingmaster.fundamentals.historical_snapshot import (
    DEFAULT_PRICE_DB,
    build_historical_fundamental_snapshot,
)
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


DEFAULT_STATE_DB = repository_root() / "swingmaster_rc.db"
STATE_FUNDAMENTAL_POLICY_RECOMMENDATION = "OPTION_B_REPORT_CONTEXT_ONLY"
STATE_READS_FUNDAMENTALS = False
STATE_DEPENDENCY_CLASSIFICATION = "STATE_CORE_NO_FUNDAMENTALS"


@dataclass(frozen=True)
class StateDependencyMapRow:
    module_file: str
    function_or_cli: str
    input_tables_helpers: str
    fundamental_fields_consumed: str
    current_or_historical_purpose: str
    signal_as_of_date_handling: str
    state_decision_impact: str
    reason_output_impact: str
    possible_lookahead_exposure: str
    recommended_future_action: str
    classification: str


@dataclass(frozen=True)
class StateFundamentalAuditSummary:
    ticker_date_rows_evaluated: int
    rows_with_different_fundamental_context: int
    state_classification_difference_count: int
    reason_difference_count: int
    ranking_difference_count: int
    report_context_difference_count: int
    rows_with_no_safe_snapshot: int
    rows_with_partial_safe_snapshot: int
    state_reads_fundamentals: bool
    recommended_policy: str


def state_dependency_map() -> list[StateDependencyMapRow]:
    return [
        StateDependencyMapRow(
            module_file="swingmaster/core/engine/evaluator.py",
            function_or_cli="evaluate_step",
            input_tables_helpers="prev StateAttrs, SignalSet, TransitionPolicy",
            fundamental_fields_consumed="none",
            current_or_historical_purpose="state decision",
            signal_as_of_date_handling="caller passes as_of_date to policy for history windows",
            state_decision_impact="hard",
            reason_output_impact="hard",
            possible_lookahead_exposure="none from fundamentals",
            recommended_future_action="keep fundamentals out of hard state decision",
            classification="STATE_CORE_NO_FUNDAMENTALS",
        ),
        StateDependencyMapRow(
            module_file="swingmaster/core/policy/rule_v1/policy.py; swingmaster/core/policy/rule_v2/policy.py; swingmaster/core/policy/rule_v3/policy.py",
            function_or_cli="RuleBasedTransitionPolicy*.decide",
            input_tables_helpers="SignalSet plus StateHistoryPortSqlite recent state/signal rows",
            fundamental_fields_consumed="none",
            current_or_historical_purpose="state transition rules and reason codes",
            signal_as_of_date_handling="history port filters state rows date <= as_of_date",
            state_decision_impact="hard",
            reason_output_impact="hard",
            possible_lookahead_exposure="none from fundamentals",
            recommended_future_action="do not add hard fundamental gates without separate product decision",
            classification="STATE_CORE_NO_FUNDAMENTALS",
        ),
        StateDependencyMapRow(
            module_file="swingmaster/app_api/providers/osakedata_signal_provider_v2.py; swingmaster/app_api/providers/osakedata_signal_provider_v3.py",
            function_or_cli="get_signals",
            input_tables_helpers="osakedata OHLCV via OsakeDataReader",
            fundamental_fields_consumed="none",
            current_or_historical_purpose="technical signal generation",
            signal_as_of_date_handling="queries market rows on or before requested date; require-row-on-date can be enforced",
            state_decision_impact="hard through emitted signals",
            reason_output_impact="indirect through policy reasons",
            possible_lookahead_exposure="none from fundamentals",
            recommended_future_action="keep technical signal generation independent",
            classification="STATE_CORE_NO_FUNDAMENTALS",
        ),
        StateDependencyMapRow(
            module_file="swingmaster/cli/run_range_universe.py",
            function_or_cli="main/app.run_daily/post phases",
            input_tables_helpers="osakedata, rc_state_daily, rc_transition, rc_pipeline_episode, optional EW/dual score tables",
            fundamental_fields_consumed="none",
            current_or_historical_purpose="historical range execution and post-run reporting",
            signal_as_of_date_handling="iterates trading days; app.run_daily writes one state row per ticker/day",
            state_decision_impact="hard for state writes; none from fundamentals",
            reason_output_impact="hard for persisted policy reasons; none from fundamentals",
            possible_lookahead_exposure="no fundamental lookahead because fundamentals are not read",
            recommended_future_action="optionally attach historical fundamental context in reports only",
            classification="STATE_CORE_NO_FUNDAMENTALS",
        ),
        StateDependencyMapRow(
            module_file="swingmaster/cli/run_fundamental_ticker_snapshot.py",
            function_or_cli="build_snapshot_matrix/load_latest_valuation_snapshot",
            input_tables_helpers="rc_fundamental_ttm, rc_fundamental_quarterly, rc_fundamental_score_percentile, rc_fundamental_valuation",
            fundamental_fields_consumed="TTM, score, percentile, valuation, quarterly fields",
            current_or_historical_purpose="current/latest display snapshot",
            signal_as_of_date_handling="not part of state signal flow",
            state_decision_impact="none",
            reason_output_impact="none",
            possible_lookahead_exposure="display-only if reused for historical research",
            recommended_future_action="use historical snapshot helper for historical reports",
            classification="CONTEXT_ONLY_HISTORICAL_NEEDS_SAFE_SNAPSHOT",
        ),
        StateDependencyMapRow(
            module_file="swingmaster/ew_score/compute.py; swingmaster/dual_score/production.py; daily reports",
            function_or_cli="compute_and_store_ew_scores/compute_and_store_dual_scores_production/report CLIs",
            input_tables_helpers="rc_state_daily, rc_transition, rc_pipeline_episode, osakedata, transactions",
            fundamental_fields_consumed="none",
            current_or_historical_purpose="ranking/reporting after state generation",
            signal_as_of_date_handling="date-filtered state and market rows",
            state_decision_impact="none",
            reason_output_impact="none",
            possible_lookahead_exposure="none from fundamentals",
            recommended_future_action="optional report enrichment only",
            classification="CONTEXT_ONLY_CURRENT",
        ),
        StateDependencyMapRow(
            module_file="earnings event/match CLIs",
            function_or_cli="earnings event maintenance and quarter matching",
            input_tables_helpers="rc_earnings_event, rc_fundamental_quarter_earnings_match",
            fundamental_fields_consumed="announcement/effective trading date metadata",
            current_or_historical_purpose="source maintenance and availability metadata",
            signal_as_of_date_handling="not wired into state blackout in audited paths",
            state_decision_impact="none",
            reason_output_impact="none",
            possible_lookahead_exposure="none in state because not consumed",
            recommended_future_action="keep blackout separate from fundamental availability",
            classification="DIAGNOSTIC_ONLY",
        ),
    ]


def audit_state_fundamental_usage(
    fundamentals_conn: sqlite3.Connection,
    state_conn: sqlite3.Connection,
    price_conn: sqlite3.Connection,
    *,
    tickers: list[str],
    dates: list[str],
    market: str,
    include_percentiles: bool = False,
    include_valuation: bool = False,
) -> tuple[StateFundamentalAuditSummary, list[dict[str, Any]]]:
    detail_rows: list[dict[str, Any]] = []
    for date in dates:
        for ticker in tickers:
            normalized = normalize_ticker(ticker)
            state_row = load_state_row(state_conn, ticker=normalized, signal_date=date)
            safe = build_historical_fundamental_snapshot(
                fundamentals_conn,
                price_conn,
                ticker=normalized,
                as_of_date=date,
                market=market,
                include_percentiles=include_percentiles,
                include_valuation=include_valuation,
                include_current_comparison=True,
            )
            comparison = safe.comparison or {}
            context_differs = bool(comparison.get("ttm_selection_differs") or comparison.get("score_selection_differs"))
            report_context_differs = context_differs or bool(safe.missing_components)
            detail_rows.append(
                {
                    "ticker": normalized,
                    "signal_date": date,
                    "state": None if state_row is None else state_row.get("state"),
                    "reason_codes": "" if state_row is None else ",".join(state_row.get("reason_codes", [])),
                    "current_context_ttm_as_of_date": comparison.get("current_ttm_as_of_date"),
                    "safe_context_ttm_as_of_date": safe.source_ttm_as_of_date,
                    "current_context_score_as_of_date": comparison.get("current_score_as_of_date"),
                    "safe_context_score_as_of_date": safe.source_score_as_of_date,
                    "fundamental_context_differs": context_differs,
                    "state_reads_fundamentals": STATE_READS_FUNDAMENTALS,
                    "state_would_change_if_context_replaced": False,
                    "report_context_would_change": report_context_differs,
                    "missing_safe_context_sections": ",".join(safe.missing_components),
                    "safe_snapshot_status": safe.snapshot_status,
                    "state_row_found": state_row is not None,
                    "classification": STATE_DEPENDENCY_CLASSIFICATION,
                }
            )
    summary = StateFundamentalAuditSummary(
        ticker_date_rows_evaluated=len(detail_rows),
        rows_with_different_fundamental_context=sum(1 for row in detail_rows if row["fundamental_context_differs"]),
        state_classification_difference_count=0,
        reason_difference_count=0,
        ranking_difference_count=0,
        report_context_difference_count=sum(1 for row in detail_rows if row["report_context_would_change"]),
        rows_with_no_safe_snapshot=sum(1 for row in detail_rows if row["safe_snapshot_status"] == "NO_AVAILABLE_TTM"),
        rows_with_partial_safe_snapshot=sum(1 for row in detail_rows if row["safe_snapshot_status"] == "PARTIAL"),
        state_reads_fundamentals=STATE_READS_FUNDAMENTALS,
        recommended_policy=STATE_FUNDAMENTAL_POLICY_RECOMMENDATION,
    )
    return summary, detail_rows


def load_state_row(conn: sqlite3.Connection, *, ticker: str, signal_date: str) -> dict[str, Any] | None:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT ticker, date, state, reasons_json, run_id, state_attrs_json
            FROM rc_state_daily
            WHERE ticker = ?
              AND date = ?
            ORDER BY run_id DESC
            LIMIT 1
            """,
            (normalize_ticker(ticker), signal_date),
        ).fetchone()
    except sqlite3.Error:
        return None
    finally:
        conn.row_factory = previous_row_factory
    if row is None:
        return None
    return {
        "ticker": str(row["ticker"]),
        "date": str(row["date"]),
        "state": str(row["state"]),
        "reason_codes": _parse_reason_codes(row["reasons_json"]),
        "run_id": row["run_id"],
        "state_attrs_json": row["state_attrs_json"],
    }


def resolve_dates(state_conn: sqlite3.Connection, *, date: str | None, date_from: str | None, date_to: str | None) -> list[str]:
    if date:
        return [date]
    if not date_from or not date_to:
        raise ValueError("Either --date or both --date-from/--date-to are required")
    rows = state_conn.execute(
        """
        SELECT DISTINCT date
        FROM rc_state_daily
        WHERE date >= ?
          AND date <= ?
        ORDER BY date ASC
        """,
        (date_from, date_to),
    ).fetchall()
    return [str(row[0]) for row in rows]


def resolve_tickers(
    state_conn: sqlite3.Connection,
    *,
    tickers: list[str],
    tickers_file: Path | None,
    dates: list[str],
    first_n: int | None,
    sample_size: int | None,
    random_seed: int,
) -> list[str]:
    resolved = [normalize_ticker(ticker) for ticker in tickers]
    if tickers_file is not None:
        path = validate_temp_path(tickers_file, must_exist=True)
        resolved.extend(normalize_ticker(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    if not resolved:
        placeholders = ", ".join("?" for _ in dates)
        params: list[Any] = [*dates]
        sql = f"""
            SELECT DISTINCT ticker
            FROM rc_state_daily
            WHERE date IN ({placeholders})
            ORDER BY ticker ASC
        """
        if first_n is not None:
            sql += " LIMIT ?"
            params.append(first_n)
        rows = state_conn.execute(sql, params).fetchall()
        resolved = [normalize_ticker(row[0]) for row in rows]
    elif first_n is not None:
        resolved = resolved[:first_n]
    unique = sorted(dict.fromkeys(resolved))
    if sample_size is not None and sample_size < len(unique):
        rng = random.Random(random_seed)
        unique = sorted(rng.sample(unique, sample_size))
    return unique


def default_output_root() -> Path:
    return repository_root() / "temp" / "historical_fundamental_state_integration_audit" / utc_timestamp()


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


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


def asdict_audit(value: StateFundamentalAuditSummary | StateDependencyMapRow) -> dict[str, Any]:
    return asdict(value)


def representative_boundary_dates() -> list[str]:
    return ["2022-01-03", "2026-04-29", "2026-04-30", "2026-07-31"]


def _parse_reason_codes(raw: Any) -> list[str]:
    if raw is None:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if isinstance(item, str)]
