from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.cli.run_fundamental_yahoo_raw_cache_reconstruct import (
    RECONSTRUCT_REASON_NO_MAPPED_VALUE_AT_TARGET_PERIOD,
    _analyze_row_for_period,
)
from swingmaster.fundamentals.historical_backfill_source_policy import (
    BACKFILL_FIELDS,
    SEC_PROVIDER,
    YAHOO_PROVIDER,
    missing_core_fields,
    non_null_supported_fields,
)


DEFAULT_MARKET = "usa"
DEFAULT_YAHOO_RECENT_TARGETS = 8
TARGET_DETERMINISTIC = "TARGET_DETERMINISTIC"
TARGET_IDENTITY_REVIEW = "TARGET_IDENTITY_REVIEW"

ACTION_NO_ACTION_COMPLETE = "NO_ACTION_COMPLETE"
ACTION_OFFLINE_MERGE_AVAILABLE = "OFFLINE_MERGE_AVAILABLE"
ACTION_OFFLINE_YAHOO_RAW_RECONSTRUCTABLE = "OFFLINE_YAHOO_RAW_RECONSTRUCTABLE"
ACTION_NEEDS_YAHOO_RECENT_ENRICHMENT = "NEEDS_YAHOO_RECENT_ENRICHMENT"
ACTION_NEEDS_SEC_HISTORY_REFRESH = "NEEDS_SEC_HISTORY_REFRESH"
ACTION_NEEDS_SEC_AND_YAHOO = "NEEDS_SEC_AND_YAHOO"
ACTION_PARTIAL_BEST_AVAILABLE = "PARTIAL_BEST_AVAILABLE"
ACTION_SOURCE_NOT_AVAILABLE = "SOURCE_NOT_AVAILABLE"
ACTION_RETRYABLE_FAILURE = "RETRYABLE_FAILURE"
ACTION_TARGET_IDENTITY_REVIEW = "TARGET_IDENTITY_REVIEW"

RESULT_COMPLETE = "COMPLETE"
RESULT_PARTIAL_BEST_AVAILABLE = "PARTIAL_BEST_AVAILABLE"
RESULT_SOURCE_NOT_AVAILABLE = "SOURCE_NOT_AVAILABLE"
RESULT_RETRYABLE_FAILURE = "RETRYABLE_FAILURE"
RESULT_TARGET_IDENTITY_REVIEW = "TARGET_IDENTITY_REVIEW"

SEC_TAG_TO_FIELD = {
    "Revenues": "revenue",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "revenue",
    "GrossProfit": "gross_profit",
    "OperatingIncomeLoss": "operating_income",
    "NetIncomeLoss": "net_income",
    "NetCashProvidedByUsedInOperatingActivities": "operating_cashflow",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": "operating_cashflow",
    "PaymentsToAcquirePropertyPlantAndEquipment": "capex",
    "PaymentsToAcquireProductiveAssets": "capex",
    "CashAndCashEquivalentsAtCarryingValue": "cash",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents": "cash",
    "LongTermDebtCurrent": "total_debt",
    "LongTermDebtNoncurrent": "total_debt",
    "LongTermDebtAndFinanceLeaseObligationsCurrent": "total_debt",
    "LongTermDebtAndFinanceLeaseObligationsNoncurrent": "total_debt",
    "EntityCommonStockSharesOutstanding": "shares_outstanding",
    "CommonStocksIncludingAdditionalPaidInCapitalSharesOutstanding": "shares_outstanding",
    "CommonStockSharesOutstanding": "shares_outstanding",
    "WeightedAverageNumberOfDilutedSharesOutstanding": "shares_outstanding",
    "WeightedAverageNumberOfSharesOutstandingBasic": "shares_outstanding",
}


@dataclass(frozen=True)
class TargetInventoryRow:
    market: str
    ticker: str
    target_period_end_date: str
    fiscal_year: str
    fiscal_quarter: str
    target_identity_source: str
    target_identity_confidence: str
    target_identity_status: str


def open_readonly_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path.expanduser().resolve().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone() is not None


def build_target_inventory(
    conn: sqlite3.Connection,
    *,
    market: str = DEFAULT_MARKET,
    tickers: Iterable[str] | None = None,
) -> list[TargetInventoryRow]:
    selected = {ticker.upper() for ticker in tickers} if tickers is not None else None
    targets: dict[tuple[str, str], dict[str, Any]] = {}

    for row in conn.execute(
        """
        SELECT DISTINCT ticker, period_end_date
        FROM rc_fundamental_quarterly
        WHERE period_end_date IS NOT NULL
          AND date(period_end_date) IS NOT NULL
          AND ticker NOT LIKE '%.HE'
        ORDER BY ticker, period_end_date
        """
    ):
        ticker = str(row["ticker"]).upper()
        if selected is not None and ticker not in selected:
            continue
        period = str(row["period_end_date"])
        targets[(ticker, period)] = {
            "market": market,
            "ticker": ticker,
            "target_period_end_date": period,
            "sources": {"NORMALIZED_QUARTERLY"},
            "fiscal_year": "",
            "fiscal_quarter": "",
            "status": TARGET_DETERMINISTIC,
        }

    if table_exists(conn, "rc_fundamental_statement_raw"):
        for row in conn.execute(
            """
            SELECT ticker, period_end_date, field_name
            FROM rc_fundamental_statement_raw
            WHERE source='sec_edgar'
              AND period_type='sec_fact'
              AND period_end_date IS NOT NULL
              AND date(period_end_date) IS NOT NULL
            ORDER BY ticker, period_end_date, field_name
            """
        ):
            ticker = str(row["ticker"]).upper()
            if selected is not None and ticker not in selected:
                continue
            if ticker.endswith(".HE"):
                continue
            period = str(row["period_end_date"])
            parsed = parse_sec_field_metadata(str(row["field_name"]))
            key = (ticker, period)
            item = targets.setdefault(
                key,
                {
                    "market": market,
                    "ticker": ticker,
                    "target_period_end_date": period,
                    "sources": set(),
                    "fiscal_year": "",
                    "fiscal_quarter": "",
                    "status": TARGET_DETERMINISTIC,
                },
            )
            item["sources"].add("SEC_FACT")
            if parsed:
                if item["fiscal_year"] and item["fiscal_year"] != parsed.get("fy", ""):
                    item["status"] = TARGET_IDENTITY_REVIEW
                if item["fiscal_quarter"] and item["fiscal_quarter"] != parsed.get("fp", ""):
                    item["status"] = TARGET_IDENTITY_REVIEW
                item["fiscal_year"] = item["fiscal_year"] or parsed.get("fy", "")
                item["fiscal_quarter"] = item["fiscal_quarter"] or parsed.get("fp", "")

    if table_exists(conn, "rc_fundamental_quarter_earnings_match"):
        for row in conn.execute(
            """
            SELECT ticker, period_end_date, matching_confidence
            FROM rc_fundamental_quarter_earnings_match
            WHERE period_end_date IS NOT NULL
              AND date(period_end_date) IS NOT NULL
            ORDER BY ticker, period_end_date
            """
        ):
            ticker = str(row["ticker"]).upper()
            if selected is not None and ticker not in selected:
                continue
            if ticker.endswith(".HE"):
                continue
            period = str(row["period_end_date"])
            key = (ticker, period)
            match_only_target = key not in targets
            item = targets.setdefault(
                key,
                {
                    "market": market,
                    "ticker": ticker,
                    "target_period_end_date": period,
                    "sources": set(),
                    "fiscal_year": "",
                    "fiscal_quarter": "",
                    "status": TARGET_DETERMINISTIC,
                },
            )
            confidence = str(row["matching_confidence"] or "")
            item["sources"].add(f"EARNINGS_MATCH:{confidence or 'UNKNOWN'}")
            if match_only_target and confidence.upper() not in {"HIGH", "EXACT", "DETERMINISTIC"}:
                item["status"] = TARGET_IDENTITY_REVIEW

    output: list[TargetInventoryRow] = []
    for item in targets.values():
        source = "+".join(sorted(item["sources"])) if item["sources"] else "UNKNOWN"
        output.append(
            TargetInventoryRow(
                market=str(item["market"]),
                ticker=str(item["ticker"]),
                target_period_end_date=str(item["target_period_end_date"]),
                fiscal_year=str(item["fiscal_year"]),
                fiscal_quarter=str(item["fiscal_quarter"]),
                target_identity_source=source,
                target_identity_confidence="HIGH" if item["status"] == TARGET_DETERMINISTIC else "REVIEW",
                target_identity_status=str(item["status"]),
            )
        )
    return sorted(output, key=lambda row: (row.ticker, row.target_period_end_date))


def parse_sec_field_metadata(field_name: str) -> dict[str, str] | None:
    parts = field_name.split("|")
    if len(parts) < 2:
        return None
    metadata = {"tag": parts[0]}
    for part in parts[1:]:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        metadata[key] = value
    return metadata


def _date_distance(left: str, right: str) -> int:
    return abs((date.fromisoformat(left) - date.fromisoformat(right)).days)


def _same_calendar_quarter(left: str, right: str) -> bool:
    ldate = date.fromisoformat(left)
    rdate = date.fromisoformat(right)
    return ldate.year == rdate.year and ((ldate.month - 1) // 3) == ((rdate.month - 1) // 3)


def _target_compatible_period(source_period: str, target_period: str, *, tolerance_days: int = 7) -> bool:
    return _same_calendar_quarter(source_period, target_period) and _date_distance(source_period, target_period) <= tolerance_days


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return None if row is None else {key: row[key] for key in row.keys()}


def _load_normalized(conn: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    fields = ", ".join(BACKFILL_FIELDS)
    rows = conn.execute(
        f"""
        SELECT ticker, period_end_date, {fields}
        FROM rc_fundamental_quarterly
        WHERE ticker NOT LIKE '%.HE'
        """
    ).fetchall()
    return {(str(row["ticker"]).upper(), str(row["period_end_date"])): _row_to_dict(row) or {} for row in rows}


def _load_status(conn: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    if not table_exists(conn, "rc_fundamental_quarter_ingestion_status"):
        return {}
    return {
        (str(row["ticker"]).upper(), str(row["period_end_date"])): _row_to_dict(row) or {}
        for row in conn.execute("SELECT * FROM rc_fundamental_quarter_ingestion_status WHERE market='usa'")
    }


def _load_ledger(conn: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    if not table_exists(conn, "rc_fundamental_historical_backfill_result"):
        return {}
    return {
        (str(row["ticker"]).upper(), str(row["target_period_end_date"])): _row_to_dict(row) or {}
        for row in conn.execute("SELECT * FROM rc_fundamental_historical_backfill_result WHERE market='usa'")
    }


def _load_sec_evidence(conn: sqlite3.Connection) -> dict[tuple[str, str], set[str]]:
    evidence: dict[tuple[str, str], set[str]] = defaultdict(set)
    if not table_exists(conn, "rc_fundamental_statement_raw"):
        return evidence
    for row in conn.execute(
        """
        SELECT ticker, period_end_date, field_name
        FROM rc_fundamental_statement_raw
        WHERE source='sec_edgar'
          AND period_type='sec_fact'
        """
    ):
        parsed = parse_sec_field_metadata(str(row["field_name"]))
        tag = str(row["field_name"]).split("|", 1)[0]
        field = SEC_TAG_TO_FIELD.get(tag)
        if field:
            evidence[(str(row["ticker"]).upper(), str(row["period_end_date"]))].add(field)
    for key, fields in list(evidence.items()):
        if "operating_cashflow" in fields and "capex" in fields:
            fields.add("free_cashflow")
    return evidence


def _load_yahoo_cache(conn: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    if not table_exists(conn, "rc_fundamental_yahoo_quarterly"):
        return {}
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_yahoo_quarterly)")}
    selected = [field for field in BACKFILL_FIELDS if field in columns]
    rows = conn.execute(
        f"""
        SELECT market, symbol, period_end_date, {', '.join(selected)}
        FROM rc_fundamental_yahoo_quarterly
        WHERE market='usa'
        ORDER BY symbol, period_end_date
        """
    ).fetchall()
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        result[str(row["symbol"]).upper()].append(_row_to_dict(row) or {})
    return result


def _load_yahoo_raw(conn: sqlite3.Connection) -> dict[str, list[sqlite3.Row]]:
    if not table_exists(conn, "rc_fundamental_yahoo_raw"):
        return {}
    rows = conn.execute(
        """
        SELECT *
        FROM rc_fundamental_yahoo_raw
        WHERE market='usa'
          AND provider='yahoo'
          AND status='OK'
        ORDER BY symbol, loaded_at_utc DESC, id DESC
        """
    ).fetchall()
    result: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        result[str(row["symbol"]).upper()].append(row)
    return result


def _matching_yahoo_cache_row(rows: list[dict[str, Any]], target_period: str) -> dict[str, Any] | None:
    exact = [row for row in rows if str(row["period_end_date"]) == target_period]
    if exact:
        return exact[0]
    candidates = [
        row
        for row in rows
        if _target_compatible_period(str(row["period_end_date"]), target_period)
    ]
    if not candidates:
        return None
    return sorted(candidates, key=lambda row: (_date_distance(str(row["period_end_date"]), target_period), str(row["period_end_date"])))[0]


def _yahoo_raw_state(rows: list[sqlite3.Row], target_period: str) -> tuple[str, int]:
    marker_present = False
    for row in rows:
        try:
            analysis = _analyze_row_for_period(row, target_period)
        except Exception:
            continue
        if analysis["persistable"]:
            return "RECONSTRUCTABLE", 1
        if analysis["period_marker_present"]:
            marker_present = True
    if marker_present:
        return RECONSTRUCT_REASON_NO_MAPPED_VALUE_AT_TARGET_PERIOD, 0
    return "NOT_PRESENT", 0


def _recent_eligible_by_ticker(inventory: list[TargetInventoryRow], recent_targets: int) -> set[tuple[str, str]]:
    eligible: set[tuple[str, str]] = set()
    by_ticker: dict[str, list[TargetInventoryRow]] = defaultdict(list)
    for row in inventory:
        if row.target_identity_status == TARGET_DETERMINISTIC:
            by_ticker[row.ticker].append(row)
    for ticker, rows in by_ticker.items():
        for row in sorted(rows, key=lambda item: item.target_period_end_date, reverse=True)[:recent_targets]:
            eligible.add((ticker, row.target_period_end_date))
    return eligible


def build_historical_backfill_plan(
    conn: sqlite3.Connection,
    *,
    market: str = DEFAULT_MARKET,
    yahoo_recent_targets: int = DEFAULT_YAHOO_RECENT_TARGETS,
    tickers: Iterable[str] | None = None,
) -> dict[str, Any]:
    inventory = build_target_inventory(conn, market=market, tickers=tickers)
    normalized = _load_normalized(conn)
    statuses = _load_status(conn)
    ledger = _load_ledger(conn)
    sec_evidence = _load_sec_evidence(conn)
    yahoo_cache = _load_yahoo_cache(conn)
    yahoo_raw = _load_yahoo_raw(conn)
    recent_eligible = _recent_eligible_by_ticker(inventory, yahoo_recent_targets)

    quarter_rows: list[dict[str, Any]] = []
    for target in inventory:
        key = (target.ticker, target.target_period_end_date)
        current = normalized.get(key)
        status = statuses.get(key, {})
        prior = ledger.get(key)
        current_complete = int(status.get("quarter_basic_complete") or 0)
        if current is not None and not status:
            missing = missing_core_fields(current)
            current_complete = int(not missing)
        else:
            missing = tuple(json.loads(status.get("missing_core_fields_json") or status.get("missing_basic_fields") or "[]")) if status else tuple(BACKFILL_FIELDS)

        sec_fields = sorted(sec_evidence.get(key, set()))
        yahoo_row = _matching_yahoo_cache_row(yahoo_cache.get(target.ticker, []), target.target_period_end_date)
        yahoo_fields = sorted(non_null_supported_fields(yahoo_row, YAHOO_PROVIDER))
        raw_state, raw_reconstructable = _yahoo_raw_state(yahoo_raw.get(target.ticker, []), target.target_period_end_date)
        recent = key in recent_eligible
        ingestion_status = str(status.get("ingestion_status") or "")
        source_confirmation = str(status.get("source_confirmation_status") or "")

        action = ACTION_SOURCE_NOT_AVAILABLE
        proposed_result = RESULT_SOURCE_NOT_AVAILABLE
        reasons: list[str] = []
        if target.target_identity_status != TARGET_DETERMINISTIC:
            action = ACTION_TARGET_IDENTITY_REVIEW
            proposed_result = RESULT_TARGET_IDENTITY_REVIEW
            reasons.append("target identity requires review")
        elif current_complete:
            action = ACTION_NO_ACTION_COMPLETE
            proposed_result = RESULT_COMPLETE
            reasons.append("current quarter_basic_complete")
        elif ingestion_status == "FETCH_FAILED" or (prior and str(prior.get("result_status")) == RESULT_RETRYABLE_FAILURE):
            action = ACTION_RETRYABLE_FAILURE
            proposed_result = RESULT_RETRYABLE_FAILURE
            reasons.append("prior or current retryable failure")
        elif sec_fields and yahoo_fields:
            action = ACTION_OFFLINE_MERGE_AVAILABLE
            proposed_result = ""
            reasons.append("persisted SEC and Yahoo evidence available")
        elif yahoo_fields and current is not None:
            action = ACTION_OFFLINE_MERGE_AVAILABLE
            proposed_result = ""
            reasons.append("persisted Yahoo cache can fill normalized NULL fields")
        elif raw_reconstructable:
            action = ACTION_OFFLINE_YAHOO_RAW_RECONSTRUCTABLE
            proposed_result = ""
            reasons.append("persisted Yahoo raw can reconstruct cache")
        elif recent and not yahoo_fields and not raw_reconstructable:
            if sec_fields:
                action = ACTION_NEEDS_YAHOO_RECENT_ENRICHMENT
                proposed_result = ""
                reasons.append("recent target lacks Yahoo enrichment evidence")
            else:
                action = ACTION_NEEDS_SEC_AND_YAHOO
                proposed_result = ""
                reasons.append("recent target lacks SEC and Yahoo evidence")
        elif not sec_fields:
            action = ACTION_NEEDS_SEC_HISTORY_REFRESH
            proposed_result = ""
            reasons.append("historical target lacks SEC evidence")
        elif current is not None or sec_fields or yahoo_fields:
            action = ACTION_PARTIAL_BEST_AVAILABLE
            proposed_result = RESULT_PARTIAL_BEST_AVAILABLE
            reasons.append("safe available evidence does not close remaining unsupported gaps")

        quarter_rows.append(
            {
                **asdict(target),
                "current_row_exists": int(current is not None),
                "current_quarter_basic_complete": current_complete,
                "current_ingestion_status": ingestion_status,
                "current_source_confirmation_status": source_confirmation,
                "current_missing_core_fields": ",".join(missing),
                "sec_evidence_state": "PRESENT" if sec_fields else "NOT_PRESENT",
                "sec_fields_available": ",".join(sec_fields),
                "yahoo_recent_eligibility": "YAHOO_RECENT_ELIGIBLE" if recent else "YAHOO_OUTSIDE_RECENT_WINDOW",
                "yahoo_cache_evidence_state": "PRESENT" if yahoo_fields else "NOT_PRESENT",
                "yahoo_cache_fields_available": ",".join(yahoo_fields),
                "yahoo_raw_reconstructability": raw_state,
                "prior_historical_result": "" if prior is None else str(prior.get("result_status") or ""),
                "proposed_quarter_action": action,
                "proposed_historical_result": proposed_result,
                "reason": "|".join(reasons),
            }
        )

    ticker_rows = _build_ticker_plan(quarter_rows)
    summary = _build_summary(inventory, quarter_rows, ticker_rows, yahoo_recent_targets)
    return {
        "target_inventory": [asdict(row) for row in inventory],
        "quarter_plan": quarter_rows,
        "ticker_provider_plan": ticker_rows,
        "summary": summary,
    }


def _build_ticker_plan(quarter_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in quarter_rows:
        by_ticker[str(row["ticker"])].append(row)
    output = []
    for ticker, rows in sorted(by_ticker.items()):
        yahoo_actions = {ACTION_NEEDS_YAHOO_RECENT_ENRICHMENT, ACTION_NEEDS_SEC_AND_YAHOO}
        sec_actions = {ACTION_NEEDS_SEC_HISTORY_REFRESH, ACTION_NEEDS_SEC_AND_YAHOO}
        yahoo_count = sum(1 for row in rows if row["proposed_quarter_action"] in yahoo_actions)
        sec_count = sum(1 for row in rows if row["proposed_quarter_action"] in sec_actions)
        output.append(
            {
                "market": DEFAULT_MARKET,
                "ticker": ticker,
                "target_count": len(rows),
                "yahoo_fetch_needed": int(yahoo_count > 0),
                "yahoo_reason_count": yahoo_count,
                "sec_fetch_needed": int(sec_count > 0),
                "sec_reason_count": sec_count,
                "affected_target_count": sum(1 for row in rows if row["proposed_quarter_action"] != ACTION_NO_ACTION_COMPLETE),
                "offline_only_target_count": sum(
                    1
                    for row in rows
                    if row["proposed_quarter_action"] in {ACTION_OFFLINE_MERGE_AVAILABLE, ACTION_OFFLINE_YAHOO_RAW_RECONSTRUCTABLE}
                ),
                "complete_target_count": sum(1 for row in rows if row["proposed_quarter_action"] == ACTION_NO_ACTION_COMPLETE),
                "best_available_target_count": sum(1 for row in rows if row["proposed_quarter_action"] == ACTION_PARTIAL_BEST_AVAILABLE),
                "review_target_count": sum(1 for row in rows if row["proposed_quarter_action"] == ACTION_TARGET_IDENTITY_REVIEW),
            }
        )
    return output


def _build_summary(
    inventory: list[TargetInventoryRow],
    quarter_rows: list[dict[str, Any]],
    ticker_rows: list[dict[str, Any]],
    yahoo_recent_targets: int,
) -> dict[str, Any]:
    by_ticker_depth = Counter()
    for row in ticker_rows:
        by_ticker_depth[int(row["target_count"])] += 1
    return {
        "planner_version": "generic_historical_quarterly_backfill_planner_v1",
        "yahoo_recent_targets": yahoo_recent_targets,
        "total_active_tickers": len(ticker_rows),
        "deterministic_target_quarters": sum(1 for row in inventory if row.target_identity_status == TARGET_DETERMINISTIC),
        "target_identity_reviews": sum(1 for row in inventory if row.target_identity_status == TARGET_IDENTITY_REVIEW),
        "currently_complete": sum(1 for row in quarter_rows if int(row["current_quarter_basic_complete"]) == 1),
        "currently_partial": sum(1 for row in quarter_rows if row["current_row_exists"] and int(row["current_quarter_basic_complete"]) == 0),
        "unknown_historical": sum(1 for row in quarter_rows if row["current_ingestion_status"] == "UNKNOWN_HISTORICAL_INGEST_COMPLETENESS"),
        "offline_only_actionable": sum(
            1
            for row in quarter_rows
            if row["proposed_quarter_action"] in {ACTION_OFFLINE_MERGE_AVAILABLE, ACTION_OFFLINE_YAHOO_RAW_RECONSTRUCTABLE}
        ),
        "tickers_needing_yahoo": sum(int(row["yahoo_fetch_needed"]) for row in ticker_rows),
        "tickers_needing_sec": sum(int(row["sec_fetch_needed"]) for row in ticker_rows),
        "tickers_needing_both": sum(1 for row in ticker_rows if row["yahoo_fetch_needed"] and row["sec_fetch_needed"]),
        "targets_best_available": sum(1 for row in quarter_rows if row["proposed_quarter_action"] == ACTION_PARTIAL_BEST_AVAILABLE),
        "retryable_failures": sum(1 for row in quarter_rows if row["proposed_quarter_action"] == ACTION_RETRYABLE_FAILURE),
        "quarter_action_counts": dict(Counter(str(row["proposed_quarter_action"]) for row in quarter_rows)),
        "provider_action_counts": dict(
            Counter(f"yahoo={row['yahoo_fetch_needed']},sec={row['sec_fetch_needed']}" for row in ticker_rows)
        ),
        "historical_result_counts": dict(Counter(str(row["proposed_historical_result"] or "ACTIONABLE") for row in quarter_rows)),
        "fiscal_history_depth_counts": dict(sorted(by_ticker_depth.items())),
    }


def write_planner_artifacts(plan: Mapping[str, Any], output_dir: Path, metadata: Mapping[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "target_inventory": output_dir / "target_inventory.csv",
        "quarter_plan": output_dir / "quarter_plan.csv",
        "ticker_provider_plan": output_dir / "ticker_provider_plan.csv",
        "summary": output_dir / "summary.json",
        "metadata": output_dir / "planner_run_metadata.json",
        "plan": output_dir / "plan.json",
    }
    _write_csv(paths["target_inventory"], plan["target_inventory"])
    _write_csv(paths["quarter_plan"], plan["quarter_plan"])
    _write_csv(paths["ticker_provider_plan"], plan["ticker_provider_plan"])
    paths["summary"].write_text(json.dumps(plan["summary"], indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["metadata"].write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["plan"].write_text(json.dumps(plan, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def plan_content_hash(plan: Mapping[str, Any]) -> str:
    payload = {
        "target_inventory": plan["target_inventory"],
        "quarter_plan": plan["quarter_plan"],
        "ticker_provider_plan": plan["ticker_provider_plan"],
        "summary": plan["summary"],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _write_csv(path: Path, rows: Any) -> None:
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
