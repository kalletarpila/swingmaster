from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from pathlib import Path

from swingmaster.fundamentals.score_percentile import FUND_SCORE_PERCENTILE_V2_PRE, compute_percentiles


FACTOR_SPECS = (
    ("growth_pct", "revenue_growth_ttm_yoy", True),
    ("margin_pct", "ebit_margin_ttm", True),
    ("margin_trend_pct", "ebit_margin_trend_4q", True),
    ("fcf_pct", "fcf_margin_ttm", True),
    ("leverage_pct", "net_debt_to_ebitda", False),
    ("dilution_pct", "share_dilution_yoy", False),
)

METRIC_ORDER = (
    "quarter",
    "lifecycle_class",
    "fundamental_score_v1",
    "fundamental_score_v2_lifecycle",
    "growth_pct",
    "margin_pct",
    "margin_trend_pct",
    "fcf_pct",
    "leverage_pct",
    "dilution_pct",
    "score_pct_global_v2_pre",
    "score_pct_sector_v2_pre",
    "score_pct_industry_v2_pre",
    "score_pct_blended_v2_pre",
    "score_pct_global_v2_lifecycle_weighted",
    "score_pct_sector_v2_lifecycle_weighted",
    "score_pct_industry_v2_lifecycle_weighted",
    "score_pct_blended_v2_lifecycle_weighted",
    "sector_rank_position",
    "industry_rank_position",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print multi-quarter ticker snapshot with scores and percentiles")
    parser.add_argument("--db", required=True, help="Fundamentals SQLite database path")
    parser.add_argument("--ticker", required=True, help="Ticker symbol")
    parser.add_argument("--quarters", type=int, default=4, help="Number of latest quarters to print")
    parser.add_argument(
        "--rule-id",
        default=FUND_SCORE_PERCENTILE_V2_PRE,
        help="Stored percentile rule identifier used for percentile snapshot rows",
    )
    parser.add_argument(
        "--percentile-target-date",
        default=None,
        help="Optional stored percentile target_date; defaults to latest available for the rule",
    )
    return parser.parse_args()


def resolve_db_path(db_arg: str) -> Path:
    return Path(db_arg).expanduser().resolve()


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _format_optional_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def _format_rank_position(rank: int | None, size: int | None) -> str:
    if rank is None or size is None:
        return ""
    return f"{rank}/{size}"


def load_latest_quarter_rows(
    conn: sqlite3.Connection,
    ticker: str,
    quarters: int,
) -> list[sqlite3.Row]:
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                ticker,
                as_of_date,
                lifecycle_class,
                fundamental_score,
                fundamental_score_lifecycle,
                revenue_growth_ttm_yoy,
                ebit_margin_ttm,
                ebit_margin_trend_4q,
                fcf_margin_ttm,
                net_debt_to_ebitda,
                share_dilution_yoy
            FROM rc_fundamental_ttm
            WHERE ticker = ?
            ORDER BY as_of_date DESC
            LIMIT ?
            """,
            (ticker.upper(), quarters),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    if not rows:
        raise RuntimeError(f"FUNDAMENTAL_TTM_NOT_FOUND:{ticker.upper()}")
    return list(reversed(rows))


def load_peer_rows_by_date(
    conn: sqlite3.Connection,
    as_of_dates: list[str],
) -> dict[str, list[sqlite3.Row]]:
    placeholders = ", ".join("?" for _ in as_of_dates)
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT
                ticker,
                as_of_date,
                revenue_growth_ttm_yoy,
                ebit_margin_ttm,
                ebit_margin_trend_4q,
                fcf_margin_ttm,
                net_debt_to_ebitda,
                share_dilution_yoy
            FROM rc_fundamental_ttm
            WHERE as_of_date IN ({placeholders})
            ORDER BY as_of_date ASC, ticker ASC
            """,
            as_of_dates,
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory

    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        grouped[str(row["as_of_date"])].append(row)
    return grouped


def compute_factor_percentiles_by_date(
    peer_rows_by_date: dict[str, list[sqlite3.Row]],
) -> dict[str, dict[str, dict[str, float]]]:
    result: dict[str, dict[str, dict[str, float]]] = {}
    for as_of_date, peer_rows in peer_rows_by_date.items():
        factor_scores: dict[str, dict[str, float]] = {}
        for metric_name, column_name, higher_is_better in FACTOR_SPECS:
            values: list[tuple[str, float]] = []
            for row in peer_rows:
                value = _coerce_optional_float(row[column_name])
                if value is None:
                    continue
                values.append((str(row["ticker"]), value))
            factor_scores[metric_name] = compute_percentiles(values, higher_is_better=higher_is_better)
        result[as_of_date] = factor_scores
    return result


def resolve_percentile_target_date(
    conn: sqlite3.Connection,
    rule_id: str,
    explicit_target_date: str | None,
) -> str | None:
    if explicit_target_date is not None:
        return explicit_target_date
    row = conn.execute(
        """
        SELECT MAX(target_date)
        FROM rc_fundamental_score_percentile
        WHERE rule_id = ?
        """,
        (rule_id,),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def load_stored_percentile_rows(
    conn: sqlite3.Connection,
    rule_id: str,
    target_date: str | None,
) -> dict[tuple[str, str], sqlite3.Row]:
    if target_date is None:
        return {}
    previous_row_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                p.*,
                RANK() OVER (
                    PARTITION BY p.target_date, p.sector
                    ORDER BY p.fundamental_score_percentile_blended_lifecycle_weighted DESC
                ) AS sector_rank,
                RANK() OVER (
                    PARTITION BY p.target_date, p.industry
                    ORDER BY p.fundamental_score_percentile_blended_lifecycle_weighted DESC
                ) AS industry_rank
            FROM rc_fundamental_score_percentile p
            WHERE p.rule_id = ?
              AND p.target_date = ?
            ORDER BY p.ticker ASC, p.as_of_date ASC
            """,
            (rule_id, target_date),
        ).fetchall()
    finally:
        conn.row_factory = previous_row_factory
    return {(str(row["ticker"]), str(row["as_of_date"])): row for row in rows}


def build_snapshot_matrix(
    conn: sqlite3.Connection,
    ticker: str,
    quarters: int,
    rule_id: str,
    percentile_target_date: str | None,
) -> list[dict[str, str]]:
    quarter_rows = load_latest_quarter_rows(conn, ticker, quarters)
    as_of_dates = [str(row["as_of_date"]) for row in quarter_rows]
    peer_rows_by_date = load_peer_rows_by_date(conn, as_of_dates)
    factor_percentiles_by_date = compute_factor_percentiles_by_date(peer_rows_by_date)
    stored_percentiles = load_stored_percentile_rows(conn, rule_id, percentile_target_date)

    matrix_rows: list[dict[str, str]] = []
    for row in quarter_rows:
        as_of_date = str(row["as_of_date"])
        key = (str(row["ticker"]), as_of_date)
        stored_row = stored_percentiles.get(key)
        factor_percentiles = factor_percentiles_by_date.get(as_of_date, {})

        matrix_rows.append(
            {
                "quarter": as_of_date,
                "lifecycle_class": str(row["lifecycle_class"]) if row["lifecycle_class"] is not None else "",
                "fundamental_score_v1": _format_optional_float(_coerce_optional_float(row["fundamental_score"])),
                "fundamental_score_v2_lifecycle": _format_optional_float(
                    _coerce_optional_float(row["fundamental_score_lifecycle"])
                ),
                "growth_pct": _format_optional_float(factor_percentiles.get("growth_pct", {}).get(ticker.upper())),
                "margin_pct": _format_optional_float(factor_percentiles.get("margin_pct", {}).get(ticker.upper())),
                "margin_trend_pct": _format_optional_float(
                    factor_percentiles.get("margin_trend_pct", {}).get(ticker.upper())
                ),
                "fcf_pct": _format_optional_float(factor_percentiles.get("fcf_pct", {}).get(ticker.upper())),
                "leverage_pct": _format_optional_float(factor_percentiles.get("leverage_pct", {}).get(ticker.upper())),
                "dilution_pct": _format_optional_float(factor_percentiles.get("dilution_pct", {}).get(ticker.upper())),
                "score_pct_global_v2_pre": _format_optional_float(
                    _coerce_optional_float(
                        stored_row["fundamental_score_percentile_global"] if stored_row is not None else None
                    )
                ),
                "score_pct_sector_v2_pre": _format_optional_float(
                    _coerce_optional_float(
                        stored_row["fundamental_score_percentile_sector"] if stored_row is not None else None
                    )
                ),
                "score_pct_industry_v2_pre": _format_optional_float(
                    _coerce_optional_float(
                        stored_row["fundamental_score_percentile_industry"] if stored_row is not None else None
                    )
                ),
                "score_pct_blended_v2_pre": _format_optional_float(
                    _coerce_optional_float(
                        stored_row["fundamental_score_percentile_blended"] if stored_row is not None else None
                    )
                ),
                "score_pct_global_v2_lifecycle_weighted": _format_optional_float(
                    _coerce_optional_float(
                        stored_row["fundamental_score_percentile_global_lifecycle_weighted"]
                        if stored_row is not None
                        else None
                    )
                ),
                "score_pct_sector_v2_lifecycle_weighted": _format_optional_float(
                    _coerce_optional_float(
                        stored_row["fundamental_score_percentile_sector_lifecycle_weighted"]
                        if stored_row is not None
                        else None
                    )
                ),
                "score_pct_industry_v2_lifecycle_weighted": _format_optional_float(
                    _coerce_optional_float(
                        stored_row["fundamental_score_percentile_industry_lifecycle_weighted"]
                        if stored_row is not None
                        else None
                    )
                ),
                "score_pct_blended_v2_lifecycle_weighted": _format_optional_float(
                    _coerce_optional_float(
                        stored_row["fundamental_score_percentile_blended_lifecycle_weighted"]
                        if stored_row is not None
                        else None
                    )
                ),
                "sector_rank_position": _format_rank_position(
                    int(stored_row["sector_rank"]) if stored_row is not None and stored_row["sector_rank"] is not None else None,
                    int(stored_row["sector_size"]) if stored_row is not None and stored_row["sector_size"] is not None else None,
                ),
                "industry_rank_position": _format_rank_position(
                    int(stored_row["industry_rank"])
                    if stored_row is not None and stored_row["industry_rank"] is not None
                    else None,
                    int(stored_row["industry_size"])
                    if stored_row is not None and stored_row["industry_size"] is not None
                    else None,
                ),
            }
        )
    return matrix_rows


def format_snapshot_matrix(matrix_rows: list[dict[str, str]]) -> str:
    lines: list[str] = []
    for metric in METRIC_ORDER:
        row_values = [row[metric] for row in matrix_rows]
        lines.append("|".join([metric, *row_values]))
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    ticker = args.ticker.upper()

    with sqlite3.connect(str(db_path)) as conn:
        percentile_target_date = resolve_percentile_target_date(conn, args.rule_id, args.percentile_target_date)
        matrix_rows = build_snapshot_matrix(
            conn=conn,
            ticker=ticker,
            quarters=args.quarters,
            rule_id=args.rule_id,
            percentile_target_date=percentile_target_date,
        )

    print(format_snapshot_matrix(matrix_rows))


if __name__ == "__main__":
    main()
