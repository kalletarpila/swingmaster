from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_events import default_fundamentals_usa_db_path, normalize_ticker, repository_root
from swingmaster.fundamentals.score import (
    FUND_SCORE_RULE_V1_1,
    FUND_SCORE_RULE_V2_LIFECYCLE_SCALING_PRE,
    compute_lifecycle_score_components,
    explain_score_components,
)
from swingmaster.fundamentals.ticker_cleanup_audit import validate_temp_path


MIGRATION_POLICY = "NET_DEBT_TO_EBIT_MIGRATION_V1"
DEFAULT_TOLERANCE = 1e-9
SCORE_COLUMNS = (
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
    "growth_component_lifecycle",
    "margin_component_lifecycle",
    "margin_trend_component_lifecycle",
    "fcf_component_lifecycle",
    "leverage_component_lifecycle",
    "dilution_component_lifecycle",
    "lifecycle_component_lifecycle",
    "consistency_component_lifecycle",
    "score_rule_lifecycle",
)
EFFECTIVE_DATE_COLUMNS = (
    "effective_trading_date",
    "effective_date_status",
    "effective_date_policy",
    "effective_date_source_period_end",
    "effective_date_match_confidence",
    "effective_date_component_count",
    "score_effective_trading_date",
    "score_effective_date_status",
    "score_effective_date_policy",
    "score_effective_date_source_ttm_as_of_date",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Guarded rebuild of active net_debt_to_ebit leverage metric")
    parser.add_argument("--fundamentals-db", default=str(default_fundamentals_usa_db_path()))
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--backup-path", default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE)
    parser.add_argument("--representative-ticker", action="append", default=None)
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.apply and args.dry_run:
        raise ValueError("APPLY_AND_DRY_RUN_ARE_MUTUALLY_EXCLUSIVE")
    apply_mode = bool(args.apply)
    output_root = _resolve_output_root(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    db_path = Path(args.fundamentals_db).expanduser().resolve()
    backup_path = _resolve_backup_path(args.backup_path, output_root, db_path)
    representative = args.representative_ticker or ["AAPL", "MSFT", "JPM", "XOM", "NVDA", "GIS", "LMT", "BBY", "ARWR", "DGXX", "AVNS"]
    payload = rebuild_net_debt_to_ebit(
        db_path,
        output_root=output_root,
        backup_path=backup_path,
        apply_mode=apply_mode,
        tolerance=args.tolerance,
        representative_tickers=representative,
    )
    _write_json_atomic(output_root / ("apply_summary.json" if apply_mode else "dry_run_summary.json"), payload)
    _write_csv_atomic(output_root / "representative_rows.csv", payload["representative_rows"])
    _write_csv_atomic(output_root / "distribution" / "distribution_rows.csv", payload["distribution_rows"])
    if args.json_output:
        print(json.dumps(payload["summary"], sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    else:
        for key, value in payload["summary"].items():
            if not isinstance(value, (dict, list)):
                print(f"SUMMARY {key}={value}")
        print(f"ARTIFACT output_root={output_root}")
        if payload["backup"] is not None:
            print(f"ARTIFACT backup_path={payload['backup']['backup_path']}")
    return 0


def rebuild_net_debt_to_ebit(
    db_path: Path,
    *,
    output_root: Path,
    backup_path: Path,
    apply_mode: bool,
    tolerance: float = DEFAULT_TOLERANCE,
    representative_tickers: list[str] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    backup = None
    if apply_mode:
        backup = ensure_backup(db_path, backup_path)
        run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        pre_counts = invariant_counts(conn)
        pre_effective_hash = effective_date_hash(conn)
        pre_deprecated_hash = deprecated_metric_hash(conn)
        if apply_mode:
            conn.execute("BEGIN")
        rows = load_calculation_rows(conn)
        calculations = build_calculations(rows)
        old_score_updates = build_score_updates(calculations, ratio_key="old_net_debt_to_ebitda")
        score_updates = build_score_updates(calculations, ratio_key="new_net_debt_to_ebit")
        summary = summarize_changes(conn, calculations, old_score_updates, score_updates, tolerance=tolerance)
        distribution_rows = build_distribution_rows(calculations)
        representative_rows = representative_checks(calculations, score_updates, representative_tickers or [])
        if apply_mode:
            metric_updates = apply_metric_updates(conn, calculations, tolerance=tolerance)
            impacted_score_updates = [
                new_update
                for old_update, new_update in zip(old_score_updates, score_updates)
                if not _same(old_update["leverage_component"], new_update["leverage_component"], tolerance)
                or not _same(old_update["fundamental_score_recomputed"], new_update["fundamental_score_recomputed"], tolerance)
                or not _same(old_update["fundamental_score_lifecycle"], new_update["fundamental_score_lifecycle"], tolerance)
            ]
            score_update_count = apply_score_updates(conn, impacted_score_updates, tolerance=tolerance)
            conn.commit()
        else:
            metric_updates = 0
            score_update_count = 0
        post_counts = invariant_counts(conn)
        post_effective_hash = effective_date_hash(conn)
        post_deprecated_hash = deprecated_metric_hash(conn)
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
    summary.update(
        {
            "mode": "apply" if apply_mode else "dry-run",
            "metric_updates": metric_updates,
            "score_updates": score_update_count,
            "quick_check": quick_check,
            "quarterly_row_count_unchanged": _count_unchanged(pre_counts, post_counts, "rc_fundamental_quarterly"),
            "ttm_row_count_unchanged": _count_unchanged(pre_counts, post_counts, "rc_fundamental_ttm"),
            "earnings_event_row_count_unchanged": _count_unchanged(pre_counts, post_counts, "rc_earnings_event"),
            "quarter_match_row_count_unchanged": _count_unchanged(pre_counts, post_counts, "rc_fundamental_quarter_earnings_match"),
            "effective_date_metadata_unchanged": pre_effective_hash == post_effective_hash,
            "deprecated_metric_unchanged": pre_deprecated_hash == post_deprecated_hash,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
    )
    return {
        "migration_policy": MIGRATION_POLICY,
        "database_path": str(db_path),
        "output_root": str(output_root),
        "backup": backup,
        "pre_counts": pre_counts,
        "post_counts": post_counts,
        "summary": summary,
        "distribution_rows": distribution_rows,
        "representative_rows": representative_rows,
    }


def load_calculation_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    stored_new_expr = "t.net_debt_to_ebit" if _has_column(conn, "rc_fundamental_ttm", "net_debt_to_ebit") else "NULL"
    rows = conn.execute(
        f"""
        SELECT
            t.ticker,
            t.as_of_date,
            t.latest_period_end_date,
            t.revenue_growth_ttm_yoy,
            t.ebit_ttm,
            t.ebit_margin_ttm,
            t.ebit_margin_trend_4q,
            t.fcf_margin_ttm,
            t.net_debt AS stored_net_debt,
            t.net_debt_to_ebitda AS old_net_debt_to_ebitda,
            {stored_new_expr} AS stored_net_debt_to_ebit,
            t.share_dilution_yoy,
            t.lifecycle_class,
            t.fundamental_score AS old_fundamental_score,
            t.fundamental_score_lifecycle AS old_fundamental_score_lifecycle,
            t.leverage_component AS old_leverage_component,
            t.leverage_component_lifecycle AS old_leverage_component_lifecycle,
            q.cash,
            q.total_debt,
            q.ebitda AS quarterly_ebitda
        FROM rc_fundamental_ttm t
        LEFT JOIN rc_fundamental_quarterly q
          ON q.ticker = t.ticker
         AND q.period_end_date = t.latest_period_end_date
        ORDER BY t.ticker ASC, t.as_of_date ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def build_calculations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        cash = _optional_float(row.get("cash"))
        total_debt = _optional_float(row.get("total_debt"))
        ebit_ttm = _optional_float(row.get("ebit_ttm"))
        net_debt = None if cash is None or total_debt is None else float(total_debt - cash)
        ratio = _safe_divide(net_debt, ebit_ttm)
        result.append({**row, "new_net_debt": net_debt, "new_net_debt_to_ebit": ratio})
    return result


def build_score_updates(calculations: list[dict[str, Any]], *, ratio_key: str) -> list[dict[str, Any]]:
    by_ticker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    updates: list[dict[str, Any]] = []
    for row in calculations:
        scoring_row = dict(row)
        scoring_row["net_debt_to_ebit"] = row[ratio_key]
        ticker_history = by_ticker[str(row["ticker"])]
        ticker_history.append(scoring_row)
        baseline = explain_score_components(scoring_row, ticker_history)
        lifecycle = compute_lifecycle_score_components(scoring_row, baseline)
        updates.append(
            {
                "ticker": row["ticker"],
                "as_of_date": row["as_of_date"],
                **baseline,
                **lifecycle,
                "score_rule": FUND_SCORE_RULE_V1_1,
            }
        )
    return updates


def summarize_changes(
    conn: sqlite3.Connection,
    calculations: list[dict[str, Any]],
    old_score_updates: list[dict[str, Any]],
    score_updates: list[dict[str, Any]],
    *,
    tolerance: float,
) -> dict[str, Any]:
    ratios = [row["new_net_debt_to_ebit"] for row in calculations if row["new_net_debt_to_ebit"] is not None]
    old_new = list(zip(calculations, old_score_updates, score_updates))
    scored_count = sum(1 for row, _old_update, _new_update in old_new if row["old_fundamental_score"] is not None)
    lifecycle_scored_count = sum(
        1 for row, _old_update, _new_update in old_new if row["old_fundamental_score_lifecycle"] is not None
    )
    score_changes = [
        abs(float(new_update["fundamental_score_recomputed"]) - float(old_update["fundamental_score_recomputed"]))
        for row, old_update, new_update in old_new
        if not _same(old_update["fundamental_score_recomputed"], new_update["fundamental_score_recomputed"], tolerance)
    ]
    lifecycle_changes = [
        abs(float(new_update["fundamental_score_lifecycle"]) - float(old_update["fundamental_score_lifecycle"]))
        for row, old_update, new_update in old_new
        if not _same(old_update["fundamental_score_lifecycle"], new_update["fundamental_score_lifecycle"], tolerance)
    ]
    return {
        "threshold_decision": "KEEP_EXISTING_THRESHOLDS",
        "total_ttm_rows": len(calculations),
        "new_ratio_non_null_count": len(ratios),
        "new_ratio_null_count": len(calculations) - len(ratios),
        "rows_with_missing_debt": sum(1 for row in calculations if row["total_debt"] is None),
        "rows_with_missing_cash": sum(1 for row in calculations if row["cash"] is None),
        "rows_with_null_or_zero_ebit": sum(1 for row in calculations if row["ebit_ttm"] is None or float(row["ebit_ttm"]) == 0),
        "rows_with_negative_ebit": sum(1 for row in calculations if row["ebit_ttm"] is not None and float(row["ebit_ttm"]) < 0),
        "rows_with_negative_net_debt": sum(1 for row in calculations if row["new_net_debt"] is not None and float(row["new_net_debt"]) < 0),
        "old_vs_new_match_count": sum(1 for row in calculations if _same(row["old_net_debt_to_ebitda"], row["new_net_debt_to_ebit"], tolerance)),
        "old_vs_new_difference_count": sum(1 for row in calculations if row["old_net_debt_to_ebitda"] is not None and row["new_net_debt_to_ebit"] is not None and not _same(row["old_net_debt_to_ebitda"], row["new_net_debt_to_ebit"], tolerance)),
        "old_null_new_value_count": sum(1 for row in calculations if row["old_net_debt_to_ebitda"] is None and row["new_net_debt_to_ebit"] is not None),
        "old_value_new_null_count": sum(1 for row in calculations if row["old_net_debt_to_ebitda"] is not None and row["new_net_debt_to_ebit"] is None),
        "rows_likely_calculated_with_actual_ebitda": sum(1 for row in calculations if row["quarterly_ebitda"] is not None),
        "leverage_component_change_count": sum(1 for _row, old_update, new_update in old_new if not _same(old_update["leverage_component"], new_update["leverage_component"], tolerance)),
        "total_score_change_count": len(score_changes),
        "lifecycle_weighted_score_change_count": len(lifecycle_changes),
        "score_change_buckets": _score_change_buckets(score_changes, total_scored=scored_count),
        "lifecycle_score_change_buckets": _score_change_buckets(lifecycle_changes, total_scored=lifecycle_scored_count),
        "percentile_source_value_change_count": sum(1 for row in calculations if row["old_net_debt_to_ebitda"] is not None and row["new_net_debt_to_ebit"] is not None and not _same(row["old_net_debt_to_ebitda"], row["new_net_debt_to_ebit"], tolerance)),
        "valuation_rebuild_required": False,
        "percentile_rebuild_required": bool(score_changes or lifecycle_changes),
        "distribution": distribution(ratios),
        "leverage_component_distribution": dict(Counter(int(update["leverage_component"]) for update in score_updates)),
        "schema_has_net_debt_to_ebit": _has_column(conn, "rc_fundamental_ttm", "net_debt_to_ebit"),
    }


def apply_metric_updates(conn: sqlite3.Connection, calculations: list[dict[str, Any]], *, tolerance: float) -> int:
    updates = []
    for row in calculations:
        if _same(row["stored_net_debt_to_ebit"], row["new_net_debt_to_ebit"], tolerance) and _same(row["stored_net_debt"], row["new_net_debt"], tolerance):
            continue
        updates.append((row["new_net_debt"], row["new_net_debt_to_ebit"], row["ticker"], row["as_of_date"]))
    conn.executemany(
        """
        UPDATE rc_fundamental_ttm
        SET net_debt = ?,
            net_debt_to_ebit = ?
        WHERE ticker = ? AND as_of_date = ?
        """,
        updates,
    )
    return len(updates)


def apply_score_updates(conn: sqlite3.Connection, score_updates: list[dict[str, Any]], *, tolerance: float) -> int:
    updates = []
    for update in score_updates:
        existing = conn.execute(
            """
            SELECT fundamental_score, growth_component, margin_component, margin_trend_component,
                   fcf_component, leverage_component, dilution_component, lifecycle_component,
                   consistency_component, score_rule, fundamental_score_lifecycle,
                   growth_component_lifecycle, margin_component_lifecycle,
                   margin_trend_component_lifecycle, fcf_component_lifecycle,
                   leverage_component_lifecycle, dilution_component_lifecycle,
                   lifecycle_component_lifecycle, consistency_component_lifecycle,
                   score_rule_lifecycle
            FROM rc_fundamental_ttm
            WHERE ticker = ? AND as_of_date = ?
            """,
            (update["ticker"], update["as_of_date"]),
        ).fetchone()
        desired = (
            update["fundamental_score_recomputed"],
            update["growth_component"],
            update["margin_component"],
            update["margin_trend_component"],
            update["fcf_component"],
            update["leverage_component"],
            update["dilution_component"],
            update["lifecycle_component"],
            update["consistency_component"],
            update["score_rule"],
            update["fundamental_score_lifecycle"],
            update["growth_component_lifecycle"],
            update["margin_component_lifecycle"],
            update["margin_trend_component_lifecycle"],
            update["fcf_component_lifecycle"],
            update["leverage_component_lifecycle"],
            update["dilution_component_lifecycle"],
            update["lifecycle_component_lifecycle"],
            update["consistency_component_lifecycle"],
            update["score_rule_lifecycle"],
        )
        if _row_same(existing, desired, tolerance):
            continue
        updates.append((*desired, update["ticker"], update["as_of_date"]))
    conn.executemany(
        """
        UPDATE rc_fundamental_ttm
        SET fundamental_score = ?,
            growth_component = ?,
            margin_component = ?,
            margin_trend_component = ?,
            fcf_component = ?,
            leverage_component = ?,
            dilution_component = ?,
            lifecycle_component = ?,
            consistency_component = ?,
            score_rule = ?,
            fundamental_score_lifecycle = ?,
            growth_component_lifecycle = ?,
            margin_component_lifecycle = ?,
            margin_trend_component_lifecycle = ?,
            fcf_component_lifecycle = ?,
            leverage_component_lifecycle = ?,
            dilution_component_lifecycle = ?,
            lifecycle_component_lifecycle = ?,
            consistency_component_lifecycle = ?,
            score_rule_lifecycle = ?
        WHERE ticker = ? AND as_of_date = ?
        """,
        updates,
    )
    return len(updates)


def representative_checks(
    calculations: list[dict[str, Any]],
    score_updates: list[dict[str, Any]],
    tickers: list[str],
) -> list[dict[str, Any]]:
    normalized = {normalize_ticker(ticker) for ticker in tickers}
    score_by_key = {(row["ticker"], row["as_of_date"]): row for row in score_updates}
    latest_by_ticker: dict[str, dict[str, Any]] = {}
    for row in calculations:
        ticker = str(row["ticker"])
        if ticker not in normalized:
            continue
        latest_by_ticker[ticker] = row
    rows = []
    for ticker in sorted(normalized):
        row = latest_by_ticker.get(ticker)
        if row is None:
            continue
        score_row = score_by_key.get((row["ticker"], row["as_of_date"]), {})
        rows.append(
            {
                "ticker": ticker,
                "as_of_date": row["as_of_date"],
                "cash": row["cash"],
                "total_debt": row["total_debt"],
                "net_debt": row["new_net_debt"],
                "ebit_ttm": row["ebit_ttm"],
                "ebitda_ttm": None,
                "old_net_debt_to_ebitda": row["old_net_debt_to_ebitda"],
                "new_net_debt_to_ebit": row["new_net_debt_to_ebit"],
                "old_leverage_component": row["old_leverage_component"],
                "old_total_score": row["old_fundamental_score"],
                "old_lifecycle_score": row["old_fundamental_score_lifecycle"],
                "new_leverage_component": score_row.get("leverage_component"),
                "new_total_score": score_row.get("fundamental_score_recomputed"),
                "new_lifecycle_score": score_row.get("fundamental_score_lifecycle"),
            }
        )
    return rows


def build_distribution_rows(calculations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = {
        "all_non_null": [row for row in calculations if row["new_net_debt_to_ebit"] is not None],
        "positive_ebit_positive_net_debt": [row for row in calculations if _gt(row["ebit_ttm"], 0) and _gt(row["new_net_debt"], 0)],
        "positive_ebit_net_cash": [row for row in calculations if _gt(row["ebit_ttm"], 0) and _lt(row["new_net_debt"], 0)],
        "zero_or_negative_ebit": [row for row in calculations if row["ebit_ttm"] is None or float(row["ebit_ttm"]) <= 0],
    }
    return [{"group": name, **distribution([row["new_net_debt_to_ebit"] for row in rows if row["new_net_debt_to_ebit"] is not None])} for name, rows in groups.items()]


def distribution(values: list[float]) -> dict[str, float | int | None]:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return {"count": 0, "minimum": None, "p01": None, "p05": None, "p10": None, "p25": None, "median": None, "p75": None, "p90": None, "p95": None, "p99": None, "maximum": None}
    return {
        "count": len(ordered),
        "minimum": ordered[0],
        "p01": _percentile(ordered, 0.01),
        "p05": _percentile(ordered, 0.05),
        "p10": _percentile(ordered, 0.10),
        "p25": _percentile(ordered, 0.25),
        "median": _percentile(ordered, 0.50),
        "p75": _percentile(ordered, 0.75),
        "p90": _percentile(ordered, 0.90),
        "p95": _percentile(ordered, 0.95),
        "p99": _percentile(ordered, 0.99),
        "maximum": ordered[-1],
    }


def ensure_backup(source_db: Path, backup_path: Path) -> dict[str, Any]:
    resolved = validate_temp_path(backup_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    if not resolved.exists():
        with sqlite3.connect(str(source_db)) as source, sqlite3.connect(str(resolved)) as target:
            source.backup(target)
    with sqlite3.connect(str(resolved)) as conn:
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        counts = invariant_counts(conn)
        duplicate_ttm = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT ticker, as_of_date, COUNT(*) c
                    FROM rc_fundamental_ttm
                    GROUP BY ticker, as_of_date
                    HAVING c > 1
                )
                """
            ).fetchone()[0]
        )
    return {
        "backup_path": str(resolved),
        "backup_size_bytes": resolved.stat().st_size,
        "quick_check": quick_check,
        "counts": counts,
        "duplicate_ttm_natural_keys": duplicate_ttm,
    }


def invariant_counts(conn: sqlite3.Connection) -> dict[str, int]:
    tables = (
        "rc_fundamental_quarterly",
        "rc_fundamental_ttm",
        "rc_fundamental_score_percentile",
        "rc_fundamental_valuation",
        "rc_earnings_event",
        "rc_fundamental_quarter_earnings_match",
        "rc_fundamental_quarterly_vintage",
        "rc_fundamental_quarterly_field_provenance",
    )
    return {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in tables if _table_exists(conn, table)}


def _count_unchanged(pre_counts: dict[str, int], post_counts: dict[str, int], table: str) -> bool:
    return pre_counts.get(table, 0) == post_counts.get(table, 0)


def effective_date_hash(conn: sqlite3.Connection) -> str:
    existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(rc_fundamental_ttm)")}
    columns = [column for column in EFFECTIVE_DATE_COLUMNS if column in existing]
    if not columns:
        return "NO_EFFECTIVE_COLUMNS"
    return _query_hash(conn, f"SELECT ticker, as_of_date, {', '.join(columns)} FROM rc_fundamental_ttm ORDER BY ticker, as_of_date")


def deprecated_metric_hash(conn: sqlite3.Connection) -> str:
    if not _has_column(conn, "rc_fundamental_ttm", "net_debt_to_ebitda"):
        return "NO_DEPRECATED_COLUMN"
    return _query_hash(conn, "SELECT ticker, as_of_date, net_debt_to_ebitda FROM rc_fundamental_ttm ORDER BY ticker, as_of_date")


def _query_hash(conn: sqlite3.Connection, sql: str) -> str:
    digest = hashlib.sha256()
    for row in conn.execute(sql):
        digest.update(json.dumps(tuple(row), sort_keys=True, default=str).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _resolve_output_root(value: str | None) -> Path:
    root = Path(value) if value else Path("temp") / "net_debt_to_ebit_migration" / utc_timestamp()
    return validate_temp_path(root)


def _resolve_backup_path(value: str | None, output_root: Path, db_path: Path) -> Path:
    if value:
        return validate_temp_path(Path(value))
    return validate_temp_path(output_root / "backups" / f"{db_path.name}.pre_net_debt_to_ebit.bak")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return float(numerator / denominator)


def _same(left: Any, right: Any, tolerance: float) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    if isinstance(left, str) or isinstance(right, str):
        return str(left) == str(right)
    return abs(float(left) - float(right)) <= tolerance


def _row_same(existing: sqlite3.Row | None, desired: tuple[Any, ...], tolerance: float) -> bool:
    if existing is None:
        return False
    return all(_same(existing[index], desired[index], tolerance) for index in range(len(desired)))


def _score_change_buckets(values: list[float], *, total_scored: int) -> dict[str, int]:
    return {
        "unchanged": total_scored - len(values),
        "absolute_change_1_to_2_points": sum(1 for value in values if 1 <= value <= 2),
        "absolute_change_3_to_5_points": sum(1 for value in values if 3 <= value <= 5),
        "absolute_change_gt_5_points": sum(1 for value in values if value > 5),
        "absolute_change_lt_1_point": sum(1 for value in values if 0 < value < 1),
    }


def _percentile(ordered: list[float], fraction: float) -> float:
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return float(ordered[lower] * (1 - weight) + ordered[upper] * weight)


def _gt(value: Any, threshold: float) -> bool:
    return value is not None and float(value) > threshold


def _lt(value: Any, threshold: float) -> bool:
    return value is not None and float(value) < threshold


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone() is not None


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    return column in {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _write_json_atomic(path: Path, payload: Any) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_name(resolved.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    tmp.replace(resolved)


def _write_csv_atomic(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    resolved = validate_temp_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    materialized = list(rows)
    tmp = resolved.with_name(resolved.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        if materialized:
            writer = csv.DictWriter(handle, fieldnames=list(materialized[0].keys()))
            writer.writeheader()
            writer.writerows(materialized)
    tmp.replace(resolved)


if __name__ == "__main__":
    raise SystemExit(main())
