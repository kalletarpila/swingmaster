from __future__ import annotations

import argparse
import sqlite3
from collections.abc import Callable, Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from swingmaster.cli.run_fundamental_bootstrap_sec_raw import SEC_USER_AGENT, run_sec_raw_bootstrap
from swingmaster.cli.run_fundamental_sec_reconstruct_quarterly import run_sec_reconstruct_quarterly
from swingmaster.fundamentals.build_quarterly import build_and_insert_quarterly_rows
from swingmaster.cli.run_fundamental_quarter_state import acknowledge_ingested, load_latest_quarter_rows
from swingmaster.cli.run_fundamental_quarterly_to_ttm import run_quarterly_to_ttm
from swingmaster.cli.run_fundamental_valuation import run_fundamental_valuation
from swingmaster.cli.run_fundamental_yahoo_audit import run_yahoo_audit
from swingmaster.cli.run_fundamental_yahoo_fallback_enrich import run_yahoo_fallback_enrich
from swingmaster.cli.run_fundamental_yahoo_quarterly_write import run_yahoo_quarterly_write
from swingmaster.cli.run_fundamental_yahoo_to_quarterly import run_yahoo_to_quarterly
from swingmaster.fundamentals.reported_quarterly_dual_write import REPORTED_FINANCIAL_FIELDS
from swingmaster.fundamentals.reported_final_mixed_execution import execute_final_mixed_vintage_write
from swingmaster.fundamentals.reported_final_mixed_vintage import (
    build_final_mixed_source_hash,
    build_final_mixed_statement_vintage_id,
    merge_final_mixed_field_source_maps,
)
from swingmaster.fundamentals.reported_sec_latest_writer_vintage import (
    build_latest_writer_sec_vintage_candidate,
)
from swingmaster.fundamentals.reported_vintage_writer import (
    insert_quarterly_field_provenance_rows,
    insert_quarterly_vintage_row,
)
from swingmaster.fundamentals.lifecycle import run_lifecycle_classification
from swingmaster.fundamentals.score import run_fundamental_scoring

DEFAULT_EXCHANGE = "HE"
VINTAGE_MODE_VALIDATION_ONLY = "validation_only"
VINTAGE_MODE_SEC_RECONSTRUCT_ONLY = "sec_reconstruct_only"
VINTAGE_MODE_SEC_LATEST_WRITER = "sec_latest_writer"
VINTAGE_MODE_YAHOO_FALLBACK_ONLY = "yahoo_fallback_only"
VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_PLANNING = "sec_plus_yahoo_fallback_planning"
VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_FINAL_MIXED = "sec_plus_yahoo_fallback_final_mixed"
VINTAGE_MODE_CHOICES = [
    VINTAGE_MODE_VALIDATION_ONLY,
    VINTAGE_MODE_SEC_RECONSTRUCT_ONLY,
    VINTAGE_MODE_SEC_LATEST_WRITER,
    VINTAGE_MODE_YAHOO_FALLBACK_ONLY,
    VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_PLANNING,
    VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_FINAL_MIXED,
]
VINTAGE_PARITY_OK = "OK"
VINTAGE_PARITY_DRIFT = "DRIFT"
VINTAGE_PARITY_UNKNOWN_RUN_LINKAGE = "UNKNOWN_RUN_LINKAGE"
VINTAGE_YAHOO_IMPACT_NONE = "NO_YAHOO_IMPACT_DETECTED"
VINTAGE_YAHOO_IMPACT_DETECTED = "YAHOO_IMPACT_DETECTED"
VINTAGE_COMPLETION_SEC_SUFFICIENT = "SEC_VINTAGE_SUFFICIENT"
VINTAGE_COMPLETION_FINAL_MIXED_REQUIRED = "FINAL_MIXED_REQUIRED"
VINTAGE_COMPLETION_YAHOO_REQUIRED = "YAHOO_VINTAGE_REQUIRED"
VINTAGE_COMPLETION_BLOCKED_DRIFT = "BLOCKED_POST_RUN_DRIFT"
VINTAGE_COMPLETION_UNKNOWN = "UNKNOWN"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process flagged quarter-state tickers through score")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--run-id", required=True, help="Deterministic base run identifier")
    parser.add_argument("--market", default=None, help="Optional market filter")
    parser.add_argument("--ticker", default=None, help="Optional single ticker filter")
    parser.add_argument("--limit", type=int, default=None, help="Optional ticker limit after deterministic ordering")
    parser.add_argument(
        "--osakedata-db",
        default=None,
        help="OHLCV SQLite database path used for final USA valuation step (required when market is usa or omitted)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Read-only preview without running write paths")
    parser.add_argument("--skip-ack", action="store_true", help="Run steps but do not acknowledge quarter state")
    parser.add_argument(
        "--write-vintage",
        action="store_true",
        help="Validate future reported vintage metadata without executing vintage writes",
    )
    parser.add_argument("--vintage-market", default=None, help="Required market for future vintage writes")
    parser.add_argument(
        "--vintage-available-at-utc",
        default=None,
        help="Required explicit PIT availability timestamp for future vintage writes",
    )
    parser.add_argument(
        "--vintage-ingested-at-utc",
        default=None,
        help="Required explicit ingestion timestamp for future vintage writes",
    )
    parser.add_argument("--vintage-run-id", default=None, help="Required explicit run id for future vintage writes")
    parser.add_argument(
        "--vintage-normalization-run-id",
        default=None,
        help="Optional normalization run id for future vintage writes",
    )
    parser.add_argument(
        "--vintage-mode",
        default=None,
        choices=VINTAGE_MODE_CHOICES,
        help=(
            "Vintage mode. Supports validation_only, sec_reconstruct_only, "
            "sec_latest_writer, yahoo_fallback_only, sec_plus_yahoo_fallback_planning, "
            "or sec_plus_yahoo_fallback_final_mixed"
        ),
    )
    return parser.parse_args(argv)


def _summary(**items: object) -> None:
    for key, value in items.items():
        print(f"SUMMARY {key}={value}")


def build_final_mixed_vintage_plan_summary(
    *,
    market: str,
    ticker: str,
    period_end_date: str,
    normalized_row: dict[str, Any],
    sec_field_source_map: dict[str, dict[str, Any]] | None = None,
    yahoo_field_source_map: dict[str, dict[str, Any]] | None = None,
    fallback_audit_rows: list[dict[str, Any]] | None = None,
) -> dict[str, object]:
    """Build a mocked final mixed vintage plan without writing vintage rows."""
    source_hash = build_final_mixed_source_hash(
        market=market,
        ticker=ticker,
        period_end_date=period_end_date,
        normalized_row=normalized_row,
        sec_field_source_map=sec_field_source_map,
        yahoo_field_source_map=yahoo_field_source_map,
        fallback_audit_rows=fallback_audit_rows,
    )
    field_source_map = merge_final_mixed_field_source_maps(
        normalized_row=normalized_row,
        sec_field_source_map=sec_field_source_map,
        yahoo_field_source_map=yahoo_field_source_map,
    )
    return {
        "vintage_final_mixed_plan_available": True,
        "vintage_final_mixed_statement_vintage_id": build_final_mixed_statement_vintage_id(
            market=market,
            ticker=ticker,
            period_end_date=period_end_date,
            source_hash=source_hash,
        ),
        "vintage_final_mixed_source_hash": source_hash,
        "vintage_final_mixed_provenance_field_count": len(field_source_map),
        "vintage_final_mixed_field_source_map": field_source_map,
    }


def build_final_mixed_vintage_options(
    *,
    vintage_market: str | None,
    vintage_available_at_utc: str | None,
    vintage_ingested_at_utc: str | None,
    vintage_run_id: str | None,
    vintage_normalization_run_id: str | None,
) -> dict[str, object]:
    return {
        "market": str(vintage_market),
        "available_at_utc": str(vintage_available_at_utc),
        "ingested_at_utc": str(vintage_ingested_at_utc),
        "run_id": str(vintage_run_id),
        "normalization_run_id": vintage_normalization_run_id,
    }


def merge_final_mixed_execution_summary(
    summary: dict[str, object],
    execution_summary: dict[str, object],
) -> None:
    summary["vintage_final_mixed_written"] = bool(execution_summary.get("final_mixed_written"))
    summary["vintage_final_mixed_rows_inserted"] = int(execution_summary.get("vintage_rows_inserted", 0) or 0)
    summary["vintage_final_mixed_provenance_rows_inserted"] = int(
        execution_summary.get("provenance_rows_inserted", 0) or 0
    )
    summary["vintage_rows_skipped_noop"] = int(execution_summary.get("skipped_noop", 0) or 0)
    summary["vintage_final_mixed_rows_already_known"] = int(execution_summary.get("already_known", 0) or 0)
    summary["vintage_rows_failed"] = 1 if execution_summary.get("error") else 0
    summary["vintage_count_status"] = "final_mixed_execution"
    summary["vintage_error_summary"] = execution_summary.get("error")


def merge_sec_latest_writer_vintage_summary(
    summary: dict[str, object],
    execution_summary: Mapping[str, object] | None,
) -> None:
    if execution_summary is None:
        return
    summary["vintage_rows_inserted"] = int(summary.get("vintage_rows_inserted", 0) or 0) + int(
        execution_summary.get("vintage_rows_inserted", 0) or 0
    )
    summary["vintage_provenance_rows_inserted"] = int(
        summary.get("vintage_provenance_rows_inserted", 0) or 0
    ) + int(execution_summary.get("provenance_rows_inserted", 0) or 0)
    summary["vintage_rows_skipped_noop"] = int(summary.get("vintage_rows_skipped_noop", 0) or 0) + int(
        execution_summary.get("skipped_already_had_vintage", 0) or 0
    )
    summary["vintage_rows_failed"] = int(summary.get("vintage_rows_failed", 0) or 0) + int(
        execution_summary.get("blocked_rows", 0) or 0
    )
    summary["vintage_count_status"] = "sec_latest_writer_execution"
    summary["vintage_error_summary"] = None


def merge_sec_latest_writer_post_run_guard_summary(
    summary: dict[str, object],
    guard_summary: Mapping[str, object] | None,
) -> None:
    if guard_summary is None:
        return
    summary.update(
        {
            "vintage_post_run_parity_status": guard_summary.get("vintage_post_run_parity_status"),
            "vintage_post_run_latest_without_vintage_count": guard_summary.get(
                "vintage_post_run_latest_without_vintage_count"
            ),
            "vintage_post_run_vintage_without_latest_count": guard_summary.get(
                "vintage_post_run_vintage_without_latest_count"
            ),
            "vintage_post_run_value_mismatch_count": guard_summary.get("vintage_post_run_value_mismatch_count"),
            "vintage_post_run_duplicate_statement_vintage_id_count": guard_summary.get(
                "vintage_post_run_duplicate_statement_vintage_id_count"
            ),
            "vintage_yahoo_impact_status": guard_summary.get("vintage_yahoo_impact_status"),
            "vintage_yahoo_fallback_rows_detected": guard_summary.get("vintage_yahoo_fallback_rows_detected"),
            "vintage_yahoo_inserted_missing_quarter_rows_detected": guard_summary.get(
                "vintage_yahoo_inserted_missing_quarter_rows_detected"
            ),
            "vintage_yahoo_filled_field_rows_detected": guard_summary.get(
                "vintage_yahoo_filled_field_rows_detected"
            ),
            "vintage_yahoo_audit_rows_detected": guard_summary.get("vintage_yahoo_audit_rows_detected"),
            "vintage_yahoo_can_create_post_sec_vintage_drift": guard_summary.get(
                "vintage_yahoo_can_create_post_sec_vintage_drift"
            ),
            "vintage_recommendation": guard_summary.get("vintage_recommendation"),
            "vintage_completion_status": guard_summary.get("vintage_completion_status"),
            "vintage_completion_reason": guard_summary.get("vintage_completion_reason"),
            "vintage_next_required_action": guard_summary.get("vintage_next_required_action"),
            "vintage_sec_only_sufficient": guard_summary.get("vintage_sec_only_sufficient"),
            "vintage_final_mixed_required": guard_summary.get("vintage_final_mixed_required"),
            "vintage_yahoo_vintage_required": guard_summary.get("vintage_yahoo_vintage_required"),
            "vintage_blocked_post_run_drift": guard_summary.get("vintage_blocked_post_run_drift"),
        }
    )


def run_final_mixed_vintage_execution_for_ticker(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    market: str,
    normalized_row: Mapping[str, Any],
    sec_field_source_map: Mapping[str, Mapping[str, Any]] | None,
    yahoo_field_source_map: Mapping[str, Mapping[str, Any]] | None,
    fallback_audit_rows: list[Mapping[str, Any]] | None,
    available_at_utc: str,
    ingested_at_utc: str,
    run_id: str,
    normalization_run_id: str | None = None,
) -> dict[str, Any]:
    normalized_ticker = _require_text(ticker, "ticker").upper()
    row = dict(normalized_row)
    row_ticker = _require_text(row.get("ticker"), "normalized_row.ticker").upper()
    if row_ticker != normalized_ticker:
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_TICKER_MISMATCH")
    _require_text(row.get("period_end_date"), "normalized_row.period_end_date")
    return execute_final_mixed_vintage_write(
        conn,
        normalized_row=row,
        market=_require_text(market, "market"),
        available_at_utc=_require_text(available_at_utc, "available_at_utc"),
        ingested_at_utc=_require_text(ingested_at_utc, "ingested_at_utc"),
        run_id=_require_text(run_id, "run_id"),
        sec_field_source_map=sec_field_source_map,
        yahoo_field_source_map=yahoo_field_source_map,
        fallback_audit_rows=fallback_audit_rows,
        normalization_run_id=normalization_run_id,
    )


def _resolve_final_mixed_inputs(
    row: sqlite3.Row,
    final_mixed_inputs_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]],
) -> Mapping[str, Any]:
    ticker = str(row["ticker"]).upper()
    period_end_date = row["detected_source_period_end_date"]
    market = str(row["market"]).lower()
    key_candidates = (
        (market, ticker, period_end_date),
        (ticker, period_end_date),
    )
    for key in key_candidates:
        inputs = final_mixed_inputs_by_key.get(key)
        if inputs is not None:
            return inputs
    raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_INPUTS_MISSING:{ticker},{period_end_date}")


def _run_final_mixed_execution_from_inputs(
    db_path: Path,
    row: sqlite3.Row,
    vintage_options: Mapping[str, object],
    final_mixed_inputs_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]],
) -> dict[str, Any]:
    inputs = _resolve_final_mixed_inputs(row, final_mixed_inputs_by_key)
    normalized_row = inputs.get("normalized_row")
    if not isinstance(normalized_row, Mapping):
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_NORMALIZED_ROW_REQUIRED")
    with sqlite3.connect(str(db_path)) as conn:
        return run_final_mixed_vintage_execution_for_ticker(
            conn,
            ticker=str(row["ticker"]),
            market=str(vintage_options["market"]),
            normalized_row=normalized_row,
            sec_field_source_map=_optional_mapping(inputs.get("sec_field_source_map")),
            yahoo_field_source_map=_optional_mapping(inputs.get("yahoo_field_source_map")),
            fallback_audit_rows=_optional_sequence(inputs.get("fallback_audit_rows")),
            available_at_utc=str(vintage_options["available_at_utc"]),
            ingested_at_utc=str(vintage_options["ingested_at_utc"]),
            run_id=str(vintage_options["run_id"]),
            normalization_run_id=_optional_text(vintage_options.get("normalization_run_id")),
        )


def _optional_mapping(value: Any) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_INPUT_MAPPING_INVALID")
    return value


def _optional_sequence(value: Any) -> list[Mapping[str, Any]] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_INPUT_SEQUENCE_INVALID")
    return value


def _optional_text(value: Any) -> str | None:
    if value is None or not str(value).strip():
        return None
    return str(value).strip()


def _require_text(value: Any, field_name: str) -> str:
    if value is None or not str(value).strip():
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_REQUIRED_FIELD_MISSING:{field_name}")
    return str(value).strip()


def _validate_vintage_timestamp(value: str, field_name: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_INVALID_TIMESTAMP:{field_name}") from exc


def validate_vintage_options(
    *,
    write_vintage: bool,
    vintage_market: str | None,
    vintage_available_at_utc: str | None,
    vintage_ingested_at_utc: str | None,
    vintage_run_id: str | None,
    vintage_mode: str | None,
) -> dict[str, object]:
    if not write_vintage:
        if vintage_mode in {
            VINTAGE_MODE_SEC_RECONSTRUCT_ONLY,
            VINTAGE_MODE_SEC_LATEST_WRITER,
            VINTAGE_MODE_YAHOO_FALLBACK_ONLY,
            VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_PLANNING,
            VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_FINAL_MIXED,
        }:
            raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_WRITE_REQUIRED_FOR_MODE:{vintage_mode}")
        return {}
    if vintage_market is None or vintage_market.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MARKET_REQUIRED")
    if vintage_available_at_utc is None or vintage_available_at_utc.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_AVAILABLE_AT_UTC_REQUIRED")
    if vintage_ingested_at_utc is None or vintage_ingested_at_utc.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_INGESTED_AT_UTC_REQUIRED")
    if vintage_run_id is None or vintage_run_id.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_RUN_ID_REQUIRED")
    if vintage_mode is None or vintage_mode.strip() == "":
        raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MODE_REQUIRED")
    if vintage_mode not in VINTAGE_MODE_CHOICES:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_MODE_UNSUPPORTED:{vintage_mode}")
    execution_enabled = vintage_mode in {
        VINTAGE_MODE_SEC_RECONSTRUCT_ONLY,
        VINTAGE_MODE_SEC_LATEST_WRITER,
        VINTAGE_MODE_YAHOO_FALLBACK_ONLY,
        VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_FINAL_MIXED,
    }
    planning_only = vintage_mode == VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_PLANNING
    _validate_vintage_timestamp(vintage_available_at_utc, "vintage_available_at_utc")
    _validate_vintage_timestamp(vintage_ingested_at_utc, "vintage_ingested_at_utc")
    if planning_only:
        return {
            "vintage_requested": True,
            "vintage_mode": vintage_mode,
            "vintage_execution_enabled": False,
            "vintage_planning_only": True,
            "vintage_validation_status": "OK",
            "vintage_sec_reconstruct_requested": True,
            "vintage_yahoo_bridge_requested": False,
            "vintage_yahoo_fallback_requested": True,
            "vintage_final_mixed_planned": True,
            "vintage_final_mixed_written": False,
            "vintage_final_mixed_plan_available": False,
            "vintage_final_mixed_statement_vintage_id": None,
            "vintage_final_mixed_source_hash": None,
            "vintage_final_mixed_provenance_field_count": None,
            "vintage_rows_inserted": 0,
            "vintage_provenance_rows_inserted": 0,
            "vintage_rows_skipped_noop": 0,
            "vintage_rows_failed": 0,
            "vintage_count_status": "planning_only_no_execution",
            "vintage_error_summary": None,
        }
    if vintage_mode == VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_FINAL_MIXED:
        return {
            "vintage_requested": True,
            "vintage_mode": vintage_mode,
            "vintage_execution_enabled": True,
            "vintage_planning_only": False,
            "vintage_validation_status": "OK",
            "vintage_sec_reconstruct_requested": True,
            "vintage_yahoo_bridge_requested": False,
            "vintage_yahoo_fallback_requested": True,
            "vintage_final_mixed_planned": True,
            "vintage_final_mixed_written": False,
            "vintage_final_mixed_rows_inserted": 0,
            "vintage_final_mixed_provenance_rows_inserted": 0,
            "vintage_rows_skipped_noop": 0,
            "vintage_final_mixed_rows_already_known": 0,
            "vintage_rows_failed": 0,
            "vintage_count_status": "final_mixed_execution_not_run",
            "vintage_error_summary": None,
        }
    return {
        "vintage_requested": True,
        "vintage_mode": vintage_mode,
        "vintage_execution_enabled": execution_enabled,
        "vintage_planning_only": False,
        "vintage_validation_status": "OK",
        "vintage_sec_reconstruct_requested": vintage_mode == VINTAGE_MODE_SEC_RECONSTRUCT_ONLY,
        "vintage_sec_latest_writer_requested": vintage_mode == VINTAGE_MODE_SEC_LATEST_WRITER,
        "vintage_yahoo_bridge_requested": False,
        "vintage_yahoo_fallback_requested": vintage_mode == VINTAGE_MODE_YAHOO_FALLBACK_ONLY,
        "vintage_final_mixed_planned": False,
        "vintage_final_mixed_written": False,
        "vintage_rows_inserted": None if execution_enabled else 0,
        "vintage_provenance_rows_inserted": None if execution_enabled else 0,
        "vintage_count_status": "not_reported_by_child" if execution_enabled else "zero_validation_only",
        "vintage_rows_skipped_noop": 0,
        "vintage_rows_failed": 0,
        "vintage_error_summary": None,
    }


def build_sec_reconstruct_vintage_options(
    *,
    write_vintage: bool,
    vintage_market: str | None,
    vintage_available_at_utc: str | None,
    vintage_ingested_at_utc: str | None,
    vintage_run_id: str | None,
    vintage_normalization_run_id: str | None,
    vintage_mode: str | None,
) -> dict[str, object] | None:
    if not write_vintage or vintage_mode != VINTAGE_MODE_SEC_RECONSTRUCT_ONLY:
        return None
    return {
        "write_vintage": True,
        "vintage_market": str(vintage_market),
        "vintage_available_at_utc": str(vintage_available_at_utc),
        "vintage_ingested_at_utc": str(vintage_ingested_at_utc),
        "vintage_run_id": str(vintage_run_id),
        "vintage_normalization_run_id": vintage_normalization_run_id,
    }


def build_yahoo_fallback_vintage_options(
    *,
    write_vintage: bool,
    vintage_market: str | None,
    vintage_available_at_utc: str | None,
    vintage_ingested_at_utc: str | None,
    vintage_run_id: str | None,
    vintage_normalization_run_id: str | None,
    vintage_mode: str | None,
) -> dict[str, object] | None:
    if not write_vintage or vintage_mode != VINTAGE_MODE_YAHOO_FALLBACK_ONLY:
        return None
    return {
        "write_vintage": True,
        "vintage_market": str(vintage_market),
        "vintage_available_at_utc": str(vintage_available_at_utc),
        "vintage_ingested_at_utc": str(vintage_ingested_at_utc),
        "vintage_run_id": str(vintage_run_id),
        "vintage_normalization_run_id": vintage_normalization_run_id,
    }


def build_sec_latest_writer_vintage_options(
    *,
    write_vintage: bool,
    vintage_market: str | None,
    vintage_available_at_utc: str | None,
    vintage_ingested_at_utc: str | None,
    vintage_run_id: str | None,
    vintage_mode: str | None,
) -> dict[str, object] | None:
    if not write_vintage or vintage_mode != VINTAGE_MODE_SEC_LATEST_WRITER:
        return None
    return {
        "market": str(vintage_market),
        "available_at_utc": str(vintage_available_at_utc),
        "ingested_at_utc": str(vintage_ingested_at_utc),
        "vintage_run_id": str(vintage_run_id),
    }


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def resolve_optional_db_path(db_arg: str | None) -> Path | None:
    if db_arg is None:
        return None
    return Path(db_arg).expanduser().resolve()


def derive_child_run_ids(base_run_id: str) -> dict[str, str]:
    return {
        "raw": f"{base_run_id}__RAW",
        "yqtr": f"{base_run_id}__YQTR",
        "qbridge": f"{base_run_id}__QBRIDGE",
        "sec_raw": f"{base_run_id}__SEC_RAW",
        "sec_reconstruct": f"{base_run_id}__SEC_QUARTERLY_RECON",
        "quarterly": f"{base_run_id}__QUARTERLY",
        "ttm": f"{base_run_id}__TTM",
        "lifecycle": f"{base_run_id}__LIFECYCLE",
        "score": f"{base_run_id}__SCORE",
        "valuation": f"{base_run_id}__VALUATION",
        "ack": f"{base_run_id}__ACK",
        "enrich": f"{base_run_id}__ENRICH",
    }


def resolve_latest_close_as_of_date(osakedata_db_path: Path, market: str) -> str:
    with sqlite3.connect(str(osakedata_db_path)) as conn:
        row = conn.execute(
            """
            SELECT MAX(pvm)
            FROM osakedata
            WHERE market = ?
              AND close IS NOT NULL
            """,
            (market,),
        ).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_VALUATION_AS_OF_DATE_NOT_FOUND:{market}")
    return str(row[0])


def should_run_usa_valuation(market: str | None) -> bool:
    if market is None:
        return True
    return market.strip().lower() == "usa"


def load_eligible_rows(
    db_path: Path,
    market: str | None,
    ticker: str | None,
    limit: int | None,
) -> list[sqlite3.Row]:
    with sqlite3.connect(str(db_path)) as conn:
        previous_row_factory = conn.row_factory
        conn.row_factory = sqlite3.Row
        try:
            sql = """
                SELECT
                    ticker,
                    market,
                    latest_db_period_end_date,
                    detected_source_period_end_date,
                    new_quarter_available
                FROM rc_fundamental_quarter_state
                WHERE new_quarter_available = 1
            """
            params: list[object] = []
            if market is not None:
                sql += " AND market = ?"
                params.append(market.strip().lower())
            if ticker is not None:
                sql += " AND ticker = ?"
                params.append(ticker.strip().upper())
            sql += " ORDER BY ticker ASC"
            if limit is not None:
                sql += " LIMIT ?"
                params.append(limit)
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.row_factory = previous_row_factory
    return rows


def run_lifecycle_step(db_path: Path, ticker: str, dry_run: bool) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        rows_classified, _class_counts = run_lifecycle_classification(
            conn=conn,
            ticker=ticker.upper(),
            dry_run=dry_run,
        )
    return 0 if dry_run else rows_classified


def run_score_step(db_path: Path, ticker: str, dry_run: bool) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        rows_scored, _min_score, _max_score, _avg_score = run_fundamental_scoring(
            conn=conn,
            ticker=ticker.upper(),
            dry_run=dry_run,
        )
    return 0 if dry_run else rows_scored


def latest_quarter_meets_detected(conn: sqlite3.Connection, ticker: str, detected_source_period_end_date: str) -> bool:
    row = conn.execute(
        """
        SELECT MAX(period_end_date)
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
        """,
        (ticker.upper(),),
    ).fetchone()
    if row is None or row[0] is None:
        return False
    return str(row[0]) >= detected_source_period_end_date


def latest_quarter_period_end_date(conn: sqlite3.Connection, ticker: str) -> str | None:
    row = conn.execute(
        """
        SELECT MAX(period_end_date)
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
        """,
        (ticker.upper(),),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def _build_sec_missing_detected_message(ticker: str, detected_source_period_end_date: str, latest_quarter: str | None) -> str:
    return (
        "FUNDAMENTAL_QUARTER_UPDATE_SEC_REFRESH_MISSING_DETECTED:"
        f"{ticker}:expected_detected_period={detected_source_period_end_date}:"
        f"latest_quarter_after_sec_refresh={latest_quarter or 'NONE'}"
    )


def _build_enrich_missing_detected_message(
    ticker: str,
    detected_source_period_end_date: str,
    latest_quarter: str | None,
) -> str:
    return (
        "FUNDAMENTAL_QUARTER_UPDATE_ENRICH_MISSING_DETECTED:"
        f"{ticker}:expected_detected_period={detected_source_period_end_date}:"
        f"latest_quarter_after_enrich={latest_quarter or 'NONE'}"
    )


def _calendar_quarter(value: date) -> int:
    return ((value.month - 1) // 3) + 1


def usa_quarter_satisfies_detected(conn: sqlite3.Connection, ticker: str, detected_source_period_end_date: str) -> bool:
    detected_date = date.fromisoformat(detected_source_period_end_date)
    detected_quarter = _calendar_quarter(detected_date)
    rows = conn.execute(
        """
        SELECT period_end_date
        FROM rc_fundamental_quarterly
        WHERE ticker = ?
        ORDER BY period_end_date ASC
        """,
        (ticker.upper(),),
    ).fetchall()
    for row in rows:
        period_end_date = row[0]
        if period_end_date is None:
            continue
        quarter_date = date.fromisoformat(str(period_end_date))
        if quarter_date.year != detected_date.year:
            continue
        if _calendar_quarter(quarter_date) != detected_quarter:
            continue
        if abs((quarter_date - detected_date).days) <= 7:
            return True
    return False


def acknowledge_ticker(db_path: Path, ticker: str, run_id: str) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        rows = load_latest_quarter_rows(conn, ticker.upper())
        rows_updated = acknowledge_ingested(
            conn,
            rows,
            run_id=run_id,
            updated_at_utc=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        )
        conn.commit()
    return rows_updated


def run_sec_quarterly_build_step(db_path: Path, ticker: str, run_id: str, dry_run: bool) -> tuple[int, int]:
    with sqlite3.connect(str(db_path)) as conn:
        return build_and_insert_quarterly_rows(
            conn=conn,
            ticker=ticker.upper(),
            run_id=run_id,
            dry_run=dry_run,
        )


def run_sec_latest_writer_vintage_side_write(
    db_path: Path,
    *,
    ticker: str,
    latest_run_id: str,
    source_run_id: str,
    market: str,
    available_at_utc: str,
    ingested_at_utc: str,
    vintage_run_id: str,
    allow_unknown_provenance: bool = False,
) -> dict[str, object]:
    normalized_ticker = ticker.upper()
    candidates = []
    skipped_already_has_vintage = 0
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        latest_rows = conn.execute(
            """
            SELECT *
            FROM rc_fundamental_quarterly
            WHERE ticker = ?
              AND run_id = ?
            ORDER BY period_end_date ASC
            """,
            (normalized_ticker, latest_run_id),
        ).fetchall()
        raw_rows = conn.execute(
            """
            SELECT
                ticker,
                statement_type,
                period_end_date,
                period_type,
                field_name,
                field_value,
                currency,
                source,
                retrieved_at_utc,
                run_id
            FROM rc_fundamental_statement_raw
            WHERE ticker = ?
              AND source = 'sec_edgar'
              AND period_type = 'sec_fact'
            ORDER BY ticker ASC, statement_type ASC, period_end_date ASC, field_name ASC
            """,
            (normalized_ticker,),
        ).fetchall()
        for latest_row in latest_rows:
            existing = conn.execute(
                """
                SELECT 1
                FROM rc_fundamental_quarterly_vintage
                WHERE ticker = ?
                  AND period_end_date = ?
                  AND market = ?
                LIMIT 1
                """,
                (normalized_ticker, latest_row["period_end_date"], market),
            ).fetchone()
            if existing is not None:
                skipped_already_has_vintage += 1
                continue
            candidate = build_latest_writer_sec_vintage_candidate(
                latest_row={key: latest_row[key] for key in latest_row.keys()},
                sec_raw_rows=raw_rows,
                market=market,
                available_at_utc=available_at_utc,
                ingested_at_utc=ingested_at_utc,
                vintage_run_id=vintage_run_id,
                source_run_id=source_run_id,
            )
            if candidate["unknown_provenance_count"] and not allow_unknown_provenance:
                fields = ",".join(candidate["unknown_provenance_fields"])
                raise RuntimeError(
                    "FUNDAMENTAL_QUARTER_UPDATE_SEC_LATEST_WRITER_UNKNOWN_PROVENANCE:"
                    f"{normalized_ticker},{latest_row['period_end_date']}:{fields}"
                )
            candidates.append(candidate)

        vintage_rows_inserted = 0
        provenance_rows_inserted = 0
        for candidate in candidates:
            vintage_rows_inserted += insert_quarterly_vintage_row(conn, candidate["vintage_row"])
            provenance_rows_inserted += insert_quarterly_field_provenance_rows(conn, candidate["provenance_rows"])
        conn.commit()

    return {
        "latest_rows_considered": len(candidates) + skipped_already_has_vintage,
        "vintage_rows_inserted": vintage_rows_inserted,
        "provenance_rows_inserted": provenance_rows_inserted,
        "skipped_already_had_vintage": skipped_already_has_vintage,
        "blocked_rows": 0,
        "unknown_provenance_fields": {},
        "status": "ok",
    }


def check_quarter_update_vintage_parity_for_run(
    conn: sqlite3.Connection,
    *,
    market: str,
    source_run_id: str | None,
    vintage_run_id: str | None = None,
    additional_latest_run_ids: list[str] | None = None,
) -> dict[str, object]:
    if source_run_id is None or not str(source_run_id).strip():
        return {
            "vintage_post_run_parity_status": VINTAGE_PARITY_UNKNOWN_RUN_LINKAGE,
            "vintage_post_run_latest_without_vintage_count": None,
            "vintage_post_run_vintage_without_latest_count": None,
            "vintage_post_run_value_mismatch_count": None,
            "vintage_post_run_duplicate_statement_vintage_id_count": None,
        }

    normalized_market = str(market).strip().lower()
    normalized_source_run_id = str(source_run_id).strip()
    latest_run_ids = [normalized_source_run_id, *[run_id for run_id in (additional_latest_run_ids or []) if str(run_id).strip()]]
    latest_rows = _latest_rows_for_run_ids(conn, latest_run_ids)
    latest_without_vintage_count = 0
    value_mismatch_count = 0
    for latest_row in latest_rows:
        vintage_rows = _matching_vintage_rows(conn, normalized_market, str(latest_row["ticker"]), str(latest_row["period_end_date"]))
        if not vintage_rows:
            latest_without_vintage_count += 1
            continue
        if not any(_latest_and_vintage_values_match(latest_row, vintage_row) for vintage_row in vintage_rows):
            value_mismatch_count += 1

    vintage_without_latest_count: int | None = None
    if vintage_run_id is not None and str(vintage_run_id).strip():
        vintage_without_latest_count = _vintage_without_latest_count(
            conn,
            market=normalized_market,
            vintage_run_id=str(vintage_run_id).strip(),
            source_run_id=normalized_source_run_id,
        )

    duplicate_statement_vintage_id_count = _duplicate_statement_vintage_id_count(conn, normalized_market, vintage_run_id)
    drift_detected = latest_without_vintage_count > 0 or value_mismatch_count > 0 or duplicate_statement_vintage_id_count > 0
    if vintage_without_latest_count is not None:
        drift_detected = drift_detected or vintage_without_latest_count > 0

    return {
        "vintage_post_run_parity_status": VINTAGE_PARITY_DRIFT if drift_detected else VINTAGE_PARITY_OK,
        "vintage_post_run_latest_without_vintage_count": latest_without_vintage_count,
        "vintage_post_run_vintage_without_latest_count": vintage_without_latest_count,
        "vintage_post_run_value_mismatch_count": value_mismatch_count,
        "vintage_post_run_duplicate_statement_vintage_id_count": duplicate_statement_vintage_id_count,
    }


def detect_yahoo_quarter_update_impact_for_run(
    conn: sqlite3.Connection,
    *,
    enrich_run_id: str | None,
    enrich_summary: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if enrich_run_id is None or not str(enrich_run_id).strip():
        return {
            "vintage_yahoo_impact_status": VINTAGE_PARITY_UNKNOWN_RUN_LINKAGE,
            "vintage_yahoo_fallback_rows_detected": None,
            "vintage_yahoo_inserted_missing_quarter_rows_detected": None,
            "vintage_yahoo_filled_field_rows_detected": None,
            "vintage_yahoo_audit_rows_detected": None,
            "vintage_yahoo_can_create_post_sec_vintage_drift": None,
        }

    normalized_run_id = str(enrich_run_id).strip()
    audit_rows_detected = _yahoo_audit_rows_for_run(conn, normalized_run_id)
    filled_field_rows_detected = _yahoo_filled_field_rows_for_run(conn, normalized_run_id)
    inserted_missing_quarter_rows_detected = _int_summary_value(enrich_summary, "rows_inserted")
    if inserted_missing_quarter_rows_detected is None:
        inserted_missing_quarter_rows_detected = _latest_rows_count_for_run(conn, normalized_run_id)
    fallback_rows_detected = _int_summary_value(enrich_summary, "rows_updated")
    if fallback_rows_detected is None:
        fallback_rows_detected = _yahoo_fallback_period_count_for_run(conn, normalized_run_id)

    can_drift = (
        audit_rows_detected > 0
        or filled_field_rows_detected > 0
        or inserted_missing_quarter_rows_detected > 0
        or fallback_rows_detected > 0
    )
    return {
        "vintage_yahoo_impact_status": VINTAGE_YAHOO_IMPACT_DETECTED if can_drift else VINTAGE_YAHOO_IMPACT_NONE,
        "vintage_yahoo_fallback_rows_detected": fallback_rows_detected,
        "vintage_yahoo_inserted_missing_quarter_rows_detected": inserted_missing_quarter_rows_detected,
        "vintage_yahoo_filled_field_rows_detected": filled_field_rows_detected,
        "vintage_yahoo_audit_rows_detected": audit_rows_detected,
        "vintage_yahoo_can_create_post_sec_vintage_drift": can_drift,
    }


def build_quarter_update_vintage_post_run_guard_summary(
    conn: sqlite3.Connection,
    *,
    market: str,
    source_run_id: str | None,
    vintage_run_id: str | None,
    enrich_run_id: str | None,
    enrich_summary: Mapping[str, object] | None,
) -> dict[str, object]:
    parity = check_quarter_update_vintage_parity_for_run(
        conn,
        market=market,
        source_run_id=source_run_id,
        vintage_run_id=vintage_run_id,
        additional_latest_run_ids=[enrich_run_id] if enrich_run_id is not None else None,
    )
    yahoo_impact = detect_yahoo_quarter_update_impact_for_run(
        conn,
        enrich_run_id=enrich_run_id,
        enrich_summary=enrich_summary,
    )
    output = {**parity, **yahoo_impact}
    output["vintage_recommendation"] = _quarter_update_vintage_recommendation(output)
    output.update(
        classify_quarter_update_vintage_completion(
            parity_summary=parity,
            yahoo_impact_summary=yahoo_impact,
        )
    )
    return output


def classify_quarter_update_vintage_completion(
    *,
    parity_summary: Mapping[str, object],
    yahoo_impact_summary: Mapping[str, object],
    value_parity_summary: Mapping[str, object] | None = None,
) -> dict[str, object]:
    combined_summary = {**parity_summary, **yahoo_impact_summary}
    if value_parity_summary is not None:
        combined_summary.update(value_parity_summary)

    if (
        combined_summary.get("vintage_post_run_parity_status") == VINTAGE_PARITY_UNKNOWN_RUN_LINKAGE
        or combined_summary.get("vintage_yahoo_impact_status") == VINTAGE_PARITY_UNKNOWN_RUN_LINKAGE
    ):
        return _vintage_completion_result(
            VINTAGE_COMPLETION_UNKNOWN,
            reason="run_linkage_unknown",
            next_action="IMPROVE_RUN_LINKAGE",
        )

    latest_without_vintage = _int_guard_value(combined_summary, "vintage_post_run_latest_without_vintage_count")
    vintage_without_latest = _int_guard_value(combined_summary, "vintage_post_run_vintage_without_latest_count")
    value_mismatch = _int_guard_value(combined_summary, "vintage_post_run_value_mismatch_count")
    duplicate_vintage_ids = _int_guard_value(
        combined_summary,
        "vintage_post_run_duplicate_statement_vintage_id_count",
    )
    yahoo_filled_fields = _int_guard_value(combined_summary, "vintage_yahoo_filled_field_rows_detected")
    yahoo_inserted_rows = _int_guard_value(
        combined_summary,
        "vintage_yahoo_inserted_missing_quarter_rows_detected",
    )
    yahoo_audit_rows = _int_guard_value(combined_summary, "vintage_yahoo_audit_rows_detected")

    if duplicate_vintage_ids > 0 or vintage_without_latest > 0:
        return _vintage_completion_result(
            VINTAGE_COMPLETION_BLOCKED_DRIFT,
            reason="duplicate_or_inconsistent_vintage_state",
            next_action="INVESTIGATE_DRIFT",
        )
    if latest_without_vintage > 0 and yahoo_inserted_rows <= 0:
        return _vintage_completion_result(
            VINTAGE_COMPLETION_BLOCKED_DRIFT,
            reason="latest_rows_without_vintage",
            next_action="INVESTIGATE_DRIFT",
        )
    if value_mismatch > 0 and yahoo_audit_rows <= 0 and yahoo_filled_fields <= 0:
        return _vintage_completion_result(
            VINTAGE_COMPLETION_BLOCKED_DRIFT,
            reason="unexplained_value_mismatch",
            next_action="INVESTIGATE_DRIFT",
        )
    if value_mismatch > 0 and (yahoo_audit_rows > 0 or yahoo_filled_fields > 0):
        return _vintage_completion_result(
            VINTAGE_COMPLETION_FINAL_MIXED_REQUIRED,
            reason="value_mismatch_explained_by_yahoo_audit",
            next_action="CREATE_FINAL_MIXED_VINTAGE",
        )
    if yahoo_filled_fields > 0:
        return _vintage_completion_result(
            VINTAGE_COMPLETION_FINAL_MIXED_REQUIRED,
            reason="yahoo_filled_fields_on_sec_backed_latest",
            next_action="CREATE_FINAL_MIXED_VINTAGE",
        )
    if yahoo_inserted_rows > 0:
        return _vintage_completion_result(
            VINTAGE_COMPLETION_YAHOO_REQUIRED,
            reason="yahoo_inserted_missing_quarter",
            next_action="CREATE_YAHOO_OR_FINAL_MIXED_VINTAGE",
        )
    if combined_summary.get("vintage_post_run_parity_status") == VINTAGE_PARITY_OK:
        return _vintage_completion_result(
            VINTAGE_COMPLETION_SEC_SUFFICIENT,
            reason="post_run_parity_ok_no_yahoo_impact",
            next_action="NONE",
        )
    return _vintage_completion_result(
        VINTAGE_COMPLETION_UNKNOWN,
        reason="insufficient_guard_data",
        next_action="IMPROVE_RUN_LINKAGE",
    )


def _vintage_completion_result(status: str, *, reason: str, next_action: str) -> dict[str, object]:
    return {
        "vintage_completion_status": status,
        "vintage_completion_reason": reason,
        "vintage_next_required_action": next_action,
        "vintage_sec_only_sufficient": status == VINTAGE_COMPLETION_SEC_SUFFICIENT,
        "vintage_final_mixed_required": status == VINTAGE_COMPLETION_FINAL_MIXED_REQUIRED,
        "vintage_yahoo_vintage_required": status == VINTAGE_COMPLETION_YAHOO_REQUIRED,
        "vintage_blocked_post_run_drift": status == VINTAGE_COMPLETION_BLOCKED_DRIFT,
    }


def _int_guard_value(summary: Mapping[str, object], key: str) -> int:
    value = summary.get(key)
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _latest_rows_for_run(conn: sqlite3.Connection, source_run_id: str) -> list[sqlite3.Row]:
    return _latest_rows_for_run_ids(conn, [source_run_id])


def _latest_rows_for_run_ids(conn: sqlite3.Connection, run_ids: list[str]) -> list[sqlite3.Row]:
    normalized_run_ids = [str(run_id).strip() for run_id in run_ids if str(run_id).strip()]
    if not normalized_run_ids:
        return []
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ", ".join("?" for _ in normalized_run_ids)
        return conn.execute(
            f"""
            SELECT *
            FROM rc_fundamental_quarterly
            WHERE run_id IN ({placeholders})
            ORDER BY ticker ASC, period_end_date ASC
            """,
            normalized_run_ids,
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory


def _matching_vintage_rows(
    conn: sqlite3.Connection,
    market: str,
    ticker: str,
    period_end_date: str,
) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            """
            SELECT *
            FROM rc_fundamental_quarterly_vintage
            WHERE market = ?
              AND ticker = ?
              AND period_end_date = ?
            ORDER BY available_at_utc DESC, statement_vintage_id ASC
            """,
            (market, ticker.upper(), period_end_date),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory


def _latest_and_vintage_values_match(latest_row: sqlite3.Row, vintage_row: sqlite3.Row) -> bool:
    for field_name in REPORTED_FINANCIAL_FIELDS:
        if not _values_equal_for_guard(latest_row[field_name], vintage_row[field_name]):
            return False
    return _values_equal_for_guard(latest_row["currency"], vintage_row["currency"])


def _values_equal_for_guard(left: object, right: object) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def _vintage_without_latest_count(
    conn: sqlite3.Connection,
    *,
    market: str,
    vintage_run_id: str,
    source_run_id: str,
) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_vintage AS vintage
        LEFT JOIN rc_fundamental_quarterly AS latest
          ON latest.ticker = vintage.ticker
         AND latest.period_end_date = vintage.period_end_date
         AND latest.run_id = ?
        WHERE vintage.market = ?
          AND vintage.run_id = ?
          AND latest.ticker IS NULL
        """,
        (source_run_id, market, vintage_run_id),
    ).fetchone()
    return int(row[0] or 0)


def _duplicate_statement_vintage_id_count(
    conn: sqlite3.Connection,
    market: str,
    vintage_run_id: str | None,
) -> int:
    params: list[object] = [market]
    run_filter = ""
    if vintage_run_id is not None and str(vintage_run_id).strip():
        run_filter = "AND run_id = ?"
        params.append(str(vintage_run_id).strip())
    rows = conn.execute(
        f"""
        SELECT statement_vintage_id
        FROM rc_fundamental_quarterly_vintage
        WHERE market = ?
          {run_filter}
        GROUP BY statement_vintage_id
        HAVING COUNT(*) > 1
        """,
        params,
    ).fetchall()
    return len(rows)


def _yahoo_audit_rows_for_run(conn: sqlite3.Connection, run_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_enrichment_audit
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    return int(row[0] or 0)


def _yahoo_filled_field_rows_for_run(conn: sqlite3.Connection, run_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly_enrichment_audit
        WHERE run_id = ?
          AND fallback_source = 'yahoo'
          AND enrichment_status = 'FILLED_FROM_YAHOO'
        """,
        (run_id,),
    ).fetchone()
    return int(row[0] or 0)


def _yahoo_fallback_period_count_for_run(conn: sqlite3.Connection, run_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT ticker, period_end_date
            FROM rc_fundamental_quarterly_enrichment_audit
            WHERE run_id = ?
              AND fallback_source = 'yahoo'
              AND enrichment_status = 'FILLED_FROM_YAHOO'
        )
        """,
        (run_id,),
    ).fetchone()
    return int(row[0] or 0)


def _latest_rows_count_for_run(conn: sqlite3.Connection, run_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM rc_fundamental_quarterly
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    return int(row[0] or 0)


def _int_summary_value(summary: Mapping[str, object] | None, key: str) -> int | None:
    if summary is None or key not in summary:
        return None
    value = summary.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _quarter_update_vintage_recommendation(summary: Mapping[str, object]) -> str:
    if summary.get("vintage_yahoo_can_create_post_sec_vintage_drift") is True:
        return "phase_4k3_final_mixed_or_post_run_parity_apply"
    if summary.get("vintage_post_run_parity_status") == VINTAGE_PARITY_DRIFT:
        return "phase_4k3_post_run_parity_apply"
    if summary.get("vintage_post_run_parity_status") == VINTAGE_PARITY_UNKNOWN_RUN_LINKAGE:
        return "phase_4k3_fix_run_linkage_before_apply"
    return "phase_4k3_can_prioritize_final_mixed_policy"


def run_sec_reconstruct_step(
    db_path: Path,
    ticker: str,
    run_id: str,
    retrieved_at_utc: str,
    dry_run: bool,
    sec_vintage_options: dict[str, object] | None,
) -> tuple[int, list[dict[str, Any]]]:
    options = sec_vintage_options or {}
    return run_sec_reconstruct_quarterly(
        db_path=db_path,
        ticker=ticker,
        run_id=run_id,
        retrieved_at_utc=retrieved_at_utc,
        dry_run=dry_run,
        **options,
    )


def run_quarterly_refresh(
    db_path: Path,
    ticker: str,
    market: str,
    child_run_ids: dict[str, str],
    sec_vintage_options: dict[str, object] | None = None,
    yahoo_fallback_vintage_options: dict[str, object] | None = None,
    sec_latest_writer_vintage_options: dict[str, object] | None = None,
) -> dict[str, Any]:
    if market == "usa":
        retrieved_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        with sqlite3.connect(str(db_path)) as conn:
            state_row = conn.execute(
                """
                SELECT detected_source_period_end_date
                FROM rc_fundamental_quarter_state
                WHERE ticker = ?
                """,
                (ticker.upper(),),
            ).fetchone()
            detected_source_period_end_date = str(state_row[0]) if state_row is not None and state_row[0] is not None else None
            if detected_source_period_end_date is None:
                raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_DETECTED_DATE_MISSING:{ticker}")
            sec_refresh_required = not usa_quarter_satisfies_detected(conn, ticker, detected_source_period_end_date)

        sec_refresh_summary: dict[str, Any] | None = None
        if sec_refresh_required:
            cik, rows = run_sec_raw_bootstrap(
                db_path=db_path,
                ticker=ticker,
                run_id=child_run_ids["sec_raw"],
                retrieved_at_utc=retrieved_at_utc,
                user_agent=SEC_USER_AGENT,
                dry_run=False,
            )
            sec_reconstruct_summary: dict[str, Any] | None = None
            if sec_vintage_options is not None:
                sec_fact_rows_read, reconstructed_rows = run_sec_reconstruct_step(
                    db_path=db_path,
                    ticker=ticker,
                    run_id=child_run_ids["sec_reconstruct"],
                    retrieved_at_utc=retrieved_at_utc,
                    dry_run=False,
                    sec_vintage_options=sec_vintage_options,
                )
                sec_reconstruct_summary = {
                    "sec_fact_rows_read": sec_fact_rows_read,
                    "quarterly_rows_reconstructed": len(reconstructed_rows),
                    "vintage_requested": True,
                }
            periods_detected, rows_written = run_sec_quarterly_build_step(
                db_path=db_path,
                ticker=ticker,
                run_id=child_run_ids["quarterly"],
                dry_run=False,
            )
            sec_latest_writer_vintage_summary = None
            if sec_latest_writer_vintage_options is not None:
                sec_latest_writer_vintage_summary = run_sec_latest_writer_vintage_side_write(
                    db_path=db_path,
                    ticker=ticker,
                    latest_run_id=child_run_ids["quarterly"],
                    source_run_id=child_run_ids["sec_raw"],
                    market=str(sec_latest_writer_vintage_options["market"]),
                    available_at_utc=str(sec_latest_writer_vintage_options["available_at_utc"]),
                    ingested_at_utc=str(sec_latest_writer_vintage_options["ingested_at_utc"]),
                    vintage_run_id=str(sec_latest_writer_vintage_options["vintage_run_id"]),
                )
            sec_refresh_summary = {
                "cik": cik,
                "rows": rows,
                "periods_detected": periods_detected,
                "rows_written": rows_written,
                "sec_reconstruct_summary": sec_reconstruct_summary,
                "sec_latest_writer_vintage_summary": sec_latest_writer_vintage_summary,
            }
            with sqlite3.connect(str(db_path)) as conn:
                if not usa_quarter_satisfies_detected(conn, ticker, detected_source_period_end_date):
                    latest_quarter = latest_quarter_period_end_date(conn, ticker)
                    print(
                        f"WARN ticker={ticker.upper()} step=quarterly_refresh_sec "
                        f"message={_build_sec_missing_detected_message(ticker, detected_source_period_end_date, latest_quarter)}"
                    )

        enrich_summary = run_yahoo_fallback_enrich(
            db_path=db_path,
            market=market,
            ticker=ticker,
            run_id=child_run_ids["enrich"],
            dry_run=False,
            replace_audit_for_run=False,
            detected_source_period_end_date=detected_source_period_end_date,
            **(yahoo_fallback_vintage_options or {}),
        )
        with sqlite3.connect(str(db_path)) as conn:
            if not usa_quarter_satisfies_detected(conn, ticker, detected_source_period_end_date):
                latest_quarter = latest_quarter_period_end_date(conn, ticker)
                raise RuntimeError(
                    _build_enrich_missing_detected_message(ticker, detected_source_period_end_date, latest_quarter)
                )
        return {
            "mode": "enrich",
            "sec_refresh_required": sec_refresh_required,
            "sec_refresh_summary": sec_refresh_summary,
            "summary": enrich_summary,
        }

    raw_summary = run_yahoo_audit(
        db_path=db_path,
        market=market,
        exchange=DEFAULT_EXCHANGE,
        symbols_arg=ticker,
        limit=None,
        run_id=child_run_ids["raw"],
        dry_run=False,
    )
    if int(raw_summary["ok_count"]) <= 0:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_RAW_NOT_USABLE:{ticker}")

    yahoo_quarterly_summary = run_yahoo_quarterly_write(
        db_path=db_path,
        market=market,
        symbol=ticker,
        run_id=child_run_ids["yqtr"],
        dry_run=False,
        replace_symbol=True,
    )
    quarterly_bridge_summary = run_yahoo_to_quarterly(
        db_path=db_path,
        market=market,
        symbol=ticker,
        run_id=child_run_ids["qbridge"],
        dry_run=False,
        replace_symbol=True,
    )
    return {
        "mode": "yahoo_refresh",
        "raw_summary": raw_summary,
        "yahoo_quarterly_summary": yahoo_quarterly_summary,
        "quarterly_bridge_summary": quarterly_bridge_summary,
    }


def process_ticker(
    db_path: Path,
    row: sqlite3.Row,
    child_run_ids: dict[str, str],
    skip_ack: bool,
    sec_vintage_options: dict[str, object] | None = None,
    yahoo_fallback_vintage_options: dict[str, object] | None = None,
    sec_latest_writer_vintage_options: dict[str, object] | None = None,
) -> dict[str, object]:
    ticker = str(row["ticker"]).upper()
    market = str(row["market"]).lower()
    detected_source_period_end_date = row["detected_source_period_end_date"]
    if detected_source_period_end_date is None:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_DETECTED_DATE_MISSING:{ticker}")

    print(f"TICKER {ticker} market={market} detected_period={detected_source_period_end_date}")
    quarterly_refresh_summary = run_quarterly_refresh(
        db_path=db_path,
        ticker=ticker,
        market=market,
        child_run_ids=child_run_ids,
        sec_vintage_options=sec_vintage_options,
        yahoo_fallback_vintage_options=yahoo_fallback_vintage_options,
        sec_latest_writer_vintage_options=sec_latest_writer_vintage_options,
    )
    print("STEP quarterly_refresh=OK")

    ttm_summary = run_quarterly_to_ttm(
        db_path=db_path,
        ticker=ticker,
        run_id=child_run_ids["ttm"],
        dry_run=False,
        replace_ticker=True,
    )
    print("STEP ttm=OK")

    lifecycle_rows_written = run_lifecycle_step(
        db_path=db_path,
        ticker=ticker,
        dry_run=False,
    )
    print("STEP lifecycle=OK")

    score_rows_written = run_score_step(
        db_path=db_path,
        ticker=ticker,
        dry_run=False,
    )
    print("STEP score=OK")

    ack_rows_written = 0
    if skip_ack:
        print("STEP ack=SKIPPED")
    else:
        with sqlite3.connect(str(db_path)) as conn:
            detected_period_text = str(detected_source_period_end_date)
            if market == "usa":
                ack_allowed = usa_quarter_satisfies_detected(conn, ticker, detected_period_text)
            else:
                ack_allowed = latest_quarter_meets_detected(conn, ticker, detected_period_text)
            if not ack_allowed:
                raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_ACK_PERIOD_MISMATCH:{ticker}")
        ack_rows_written = acknowledge_ticker(
            db_path=db_path,
            ticker=ticker,
            run_id=child_run_ids["ack"],
        )
        print("STEP ack=OK")

    sec_latest_writer_vintage_summary = _extract_sec_latest_writer_vintage_summary(quarterly_refresh_summary)
    post_run_guard_summary = None
    if sec_latest_writer_vintage_options is not None:
        with sqlite3.connect(str(db_path)) as conn:
            post_run_guard_summary = build_quarter_update_vintage_post_run_guard_summary(
                conn,
                market=market,
                source_run_id=child_run_ids["quarterly"],
                vintage_run_id=str(sec_latest_writer_vintage_options["vintage_run_id"]),
                enrich_run_id=child_run_ids["enrich"],
                enrich_summary=_extract_enrich_summary(quarterly_refresh_summary),
            )
    return {
        "quarterly_refresh_mode": 1 if quarterly_refresh_summary else 0,
        "ttm_rows_written": int(ttm_summary["rows_written"]),
        "lifecycle_rows_written": lifecycle_rows_written,
        "score_rows_written": score_rows_written,
        "ack_rows_written": ack_rows_written,
        "sec_latest_writer_vintage_summary": sec_latest_writer_vintage_summary,
        "vintage_post_run_guard_summary": post_run_guard_summary,
    }


def _extract_sec_latest_writer_vintage_summary(summary: Mapping[str, Any]) -> Mapping[str, object] | None:
    sec_refresh_summary = summary.get("sec_refresh_summary")
    if not isinstance(sec_refresh_summary, Mapping):
        return None
    latest_writer_summary = sec_refresh_summary.get("sec_latest_writer_vintage_summary")
    if isinstance(latest_writer_summary, Mapping):
        return latest_writer_summary
    return None


def _extract_enrich_summary(summary: Mapping[str, Any]) -> Mapping[str, object] | None:
    enrich_summary = summary.get("summary")
    if isinstance(enrich_summary, Mapping):
        return enrich_summary
    return None


def run_fundamental_quarter_update(
    db_path: Path,
    osakedata_db_path: Path | None,
    run_id: str,
    market: str | None,
    ticker: str | None,
    limit: int | None,
    dry_run: bool,
    skip_ack: bool,
    write_vintage: bool = False,
    vintage_market: str | None = None,
    vintage_available_at_utc: str | None = None,
    vintage_ingested_at_utc: str | None = None,
    vintage_run_id: str | None = None,
    vintage_normalization_run_id: str | None = None,
    vintage_mode: str | None = None,
    final_mixed_execution_runner: Callable[..., dict[str, object]] | None = None,
    final_mixed_inputs_by_key: Mapping[tuple[Any, ...], Mapping[str, Any]] | None = None,
) -> dict[str, object]:
    vintage_summary = validate_vintage_options(
        write_vintage=write_vintage,
        vintage_market=vintage_market,
        vintage_available_at_utc=vintage_available_at_utc,
        vintage_ingested_at_utc=vintage_ingested_at_utc,
        vintage_run_id=vintage_run_id,
        vintage_mode=vintage_mode,
    )
    sec_vintage_options = build_sec_reconstruct_vintage_options(
        write_vintage=write_vintage,
        vintage_market=vintage_market,
        vintage_available_at_utc=vintage_available_at_utc,
        vintage_ingested_at_utc=vintage_ingested_at_utc,
        vintage_run_id=vintage_run_id,
        vintage_normalization_run_id=vintage_normalization_run_id,
        vintage_mode=vintage_mode,
    )
    yahoo_fallback_vintage_options = build_yahoo_fallback_vintage_options(
        write_vintage=write_vintage,
        vintage_market=vintage_market,
        vintage_available_at_utc=vintage_available_at_utc,
        vintage_ingested_at_utc=vintage_ingested_at_utc,
        vintage_run_id=vintage_run_id,
        vintage_normalization_run_id=vintage_normalization_run_id,
        vintage_mode=vintage_mode,
    )
    sec_latest_writer_vintage_options = build_sec_latest_writer_vintage_options(
        write_vintage=write_vintage,
        vintage_market=vintage_market,
        vintage_available_at_utc=vintage_available_at_utc,
        vintage_ingested_at_utc=vintage_ingested_at_utc,
        vintage_run_id=vintage_run_id,
        vintage_mode=vintage_mode,
    )
    final_mixed_vintage_options = None
    if write_vintage and vintage_mode == VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_FINAL_MIXED:
        final_mixed_vintage_options = build_final_mixed_vintage_options(
            vintage_market=vintage_market,
            vintage_available_at_utc=vintage_available_at_utc,
            vintage_ingested_at_utc=vintage_ingested_at_utc,
            vintage_run_id=vintage_run_id,
            vintage_normalization_run_id=vintage_normalization_run_id,
        )
        if not dry_run and final_mixed_execution_runner is None and final_mixed_inputs_by_key is None:
            raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_INPUTS_REQUIRED")
    rows = load_eligible_rows(db_path, market, ticker, limit)
    market_label = market.strip().lower() if market is not None else "ALL"
    strict_single_ticker_mode = ticker is not None
    if dry_run:
        for row in rows:
            print(
                f"TICKER {str(row['ticker']).upper()} market={str(row['market']).lower()} "
                f"detected_period={row['detected_source_period_end_date']}"
            )
        summary = {
            "tickers_total": len(rows),
            "tickers_processed": 0,
            "tickers_succeeded": 0,
            "tickers_failed": 0,
            "market": market_label,
            "dry_run": 1,
            "skip_ack": 1 if skip_ack else 0,
            "run_id": run_id,
        }
        summary.update(vintage_summary)
        _summary(**summary)
        return summary

    child_run_ids = derive_child_run_ids(run_id)
    tickers_processed = 0
    tickers_succeeded = 0
    tickers_failed = 0
    for row in rows:
        current_ticker = str(row["ticker"]).upper()
        tickers_processed += 1
        try:
            process_kwargs: dict[str, object] = {
                "db_path": db_path,
                "row": row,
                "child_run_ids": child_run_ids,
                "skip_ack": skip_ack,
            }
            if sec_vintage_options is not None:
                process_kwargs["sec_vintage_options"] = sec_vintage_options
            if yahoo_fallback_vintage_options is not None:
                process_kwargs["yahoo_fallback_vintage_options"] = yahoo_fallback_vintage_options
            if sec_latest_writer_vintage_options is not None:
                process_kwargs["sec_latest_writer_vintage_options"] = sec_latest_writer_vintage_options
            process_summary = process_ticker(**process_kwargs)
            if vintage_mode == VINTAGE_MODE_SEC_LATEST_WRITER:
                merge_sec_latest_writer_vintage_summary(
                    vintage_summary,
                    _optional_mapping(process_summary.get("sec_latest_writer_vintage_summary")),
                )
                merge_sec_latest_writer_post_run_guard_summary(
                    vintage_summary,
                    _optional_mapping(process_summary.get("vintage_post_run_guard_summary")),
                )
            if final_mixed_vintage_options is not None:
                if final_mixed_execution_runner is not None:
                    execution_summary = final_mixed_execution_runner(
                        db_path=db_path,
                        row=row,
                        vintage_options=final_mixed_vintage_options,
                    )
                elif final_mixed_inputs_by_key is not None:
                    execution_summary = _run_final_mixed_execution_from_inputs(
                        db_path,
                        row,
                        final_mixed_vintage_options,
                        final_mixed_inputs_by_key,
                    )
                else:
                    raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_INPUTS_REQUIRED")
                merge_final_mixed_execution_summary(vintage_summary, execution_summary)
        except Exception as exc:
            step_name = "unknown"
            message = str(exc)
            if "DETECTED_DATE_MISSING" in message:
                step_name = "state"
            elif "ACK_PERIOD_MISMATCH" in message:
                step_name = "ack"
            elif "RAW_NOT_USABLE" in message:
                step_name = "quarterly_refresh"
            elif "SEC_REFRESH" in message:
                step_name = "quarterly_refresh"
            elif "FUNDAMENTAL_TTM" in message:
                step_name = "ttm"
            elif "LIFECYCLE" in message:
                step_name = "lifecycle"
            elif "SCORE" in message:
                step_name = "score"
            elif "YAHOO" in message or "ENRICH" in message:
                step_name = "quarterly_refresh"
            elif "SEC_LATEST_WRITER" in message:
                step_name = "sec_latest_writer_vintage"
            elif "FINAL_MIXED" in message:
                step_name = "final_mixed_vintage"
            print(f"TICKER {current_ticker}=FAILED")
            print(f"ERROR ticker={current_ticker} step={step_name} message={message}")
            tickers_failed += 1
            if vintage_mode == VINTAGE_MODE_SEC_LATEST_WRITER:
                vintage_summary["vintage_rows_failed"] = int(vintage_summary.get("vintage_rows_failed", 0) or 0) + 1
                vintage_summary["vintage_error_summary"] = message
            if vintage_mode == VINTAGE_MODE_SEC_PLUS_YAHOO_FALLBACK_FINAL_MIXED:
                vintage_summary["vintage_rows_failed"] = int(vintage_summary.get("vintage_rows_failed", 0) or 0) + 1
                vintage_summary["vintage_error_summary"] = message
            if strict_single_ticker_mode:
                raise
            continue
        tickers_succeeded += 1

    valuation_as_of_date = ""
    valuation_rows_written = 0
    if should_run_usa_valuation(market):
        if osakedata_db_path is None:
            raise RuntimeError("FUNDAMENTAL_QUARTER_UPDATE_OSAKEDATA_DB_REQUIRED_FOR_USA_VALUATION")
        valuation_as_of_date = resolve_latest_close_as_of_date(osakedata_db_path, market="usa")
        valuation_summary = run_fundamental_valuation(
            db_path=db_path,
            osakedata_db_path=osakedata_db_path,
            market="usa",
            as_of_date=valuation_as_of_date,
            ticker=None,
            run_id=child_run_ids["valuation"],
            dry_run=False,
            replace=True,
        )
        valuation_rows_written = int(valuation_summary["rows_written"])
        print(f"STEP valuation=OK as_of_date={valuation_as_of_date} rows_written={valuation_rows_written}")

    summary = {
        "tickers_total": len(rows),
        "tickers_processed": tickers_processed,
        "tickers_succeeded": tickers_succeeded,
        "tickers_failed": tickers_failed,
        "market": market_label,
        "dry_run": 0,
        "skip_ack": 1 if skip_ack else 0,
        "valuation_as_of_date": valuation_as_of_date,
        "valuation_rows_written": valuation_rows_written,
        "run_id": run_id,
    }
    summary.update(vintage_summary)
    _summary(**summary)
    if not strict_single_ticker_mode and tickers_failed > 0:
        raise RuntimeError(f"FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed={tickers_failed}")
    return summary


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    osakedata_db_path = resolve_optional_db_path(args.osakedata_db)
    try:
        run_fundamental_quarter_update(
            db_path=db_path,
            osakedata_db_path=osakedata_db_path,
            run_id=args.run_id,
            market=args.market,
            ticker=args.ticker,
            limit=args.limit,
            dry_run=args.dry_run,
            skip_ack=args.skip_ack,
            write_vintage=args.write_vintage,
            vintage_market=args.vintage_market,
            vintage_available_at_utc=args.vintage_available_at_utc,
            vintage_ingested_at_utc=args.vintage_ingested_at_utc,
            vintage_run_id=args.vintage_run_id,
            vintage_normalization_run_id=args.vintage_normalization_run_id,
            vintage_mode=args.vintage_mode,
        )
    except Exception as exc:
        raise SystemExit(str(exc))


if __name__ == "__main__":
    main()
