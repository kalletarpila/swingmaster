from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path


_IDENT_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_identifier(name: str) -> str:
    if not _IDENT_PATTERN.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name}")
    return name


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run forward-edge research analysis")
    parser.add_argument("--osakedata-db", required=True, help="Path to osakedata.db")
    parser.add_argument("--analysis-db", required=True, help="Path to analysis.db")
    parser.add_argument("--out-db", required=True, help="Path to research output sqlite db")
    parser.add_argument("--ohlcv-table", required=True, help="OHLCV table name")
    parser.add_argument("--ticker-col", default="ticker", help="OHLCV ticker column name")
    parser.add_argument("--date-col", default="date", help="OHLCV date column name")
    parser.add_argument("--close-col", default="close", help="OHLCV close column name")
    parser.add_argument("--start-date", help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end-date", help="Inclusive end date YYYY-MM-DD")
    parser.add_argument("--market", help="Optional market filter value")
    parser.add_argument(
        "--horizons",
        nargs="+",
        type=int,
        default=[5, 10, 20],
        help="Forward horizons in trading rows (default: 5 10 20)",
    )
    parser.add_argument(
        "--top-pcts",
        nargs="+",
        type=float,
        default=[0.90, 0.95],
        help="Top percentile thresholds for top10 and top5 (default: 0.90 0.95)",
    )
    return parser.parse_args(argv)


def _validate_dates(start_date: str | None, end_date: str | None) -> None:
    if start_date is not None:
        date.fromisoformat(start_date)
    if end_date is not None:
        date.fromisoformat(end_date)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("start-date must be <= end-date")


def _validate_numeric_args(horizons: list[int], top_pcts: list[float]) -> None:
    if any(h <= 0 for h in horizons):
        raise ValueError("all horizons must be > 0")
    if len(top_pcts) != 2:
        raise ValueError("top-pcts must provide exactly two values: top10 top5")
    if any(p <= 0.0 or p > 1.0 for p in top_pcts):
        raise ValueError("all top-pcts values must be in (0, 1]")


def _table_exists(conn: sqlite3.Connection, schema: str, table_name: str) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, schema: str, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA {schema}.table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _resolve_date_column(columns: set[str]) -> str | None:
    if "as_of_date" in columns:
        return "as_of_date"
    if "date" in columns:
        return "date"
    return None


def _prepare_output_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_forward_edge_candidates (
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            close_t REAL,
            close_t_plus_n REAL,
            fwd_return REAL NOT NULL,
            cume_dist REAL NOT NULL,
            is_top10 INTEGER NOT NULL,
            is_top5 INTEGER NOT NULL,
            state TEXT NULL,
            signals TEXT NULL,
            PRIMARY KEY (ticker, as_of_date, horizon)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_candidates_horizon ON research_forward_edge_candidates(horizon)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_candidates_state ON research_forward_edge_candidates(state)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_forward_edge_summary (
            horizon INTEGER,
            n_total INTEGER,
            n_top10 INTEGER,
            n_top5 INTEGER,
            avg_all REAL,
            avg_top10 REAL,
            avg_top5 REAL,
            start_date TEXT,
            end_date TEXT,
            created_at TEXT
        )
        """
    )


def _prepare_enrichment_tables(conn: sqlite3.Connection) -> tuple[bool, bool]:
    conn.execute("DROP TABLE IF EXISTS temp.state_enrichment")
    conn.execute("DROP TABLE IF EXISTS temp.signal_enrichment")
    conn.execute(
        """
        CREATE TEMP TABLE state_enrichment (
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            state TEXT NULL,
            PRIMARY KEY (ticker, as_of_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE signal_enrichment (
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            signals TEXT NULL,
            PRIMARY KEY (ticker, as_of_date)
        )
        """
    )

    state_loaded = False
    signal_loaded = False

    if _table_exists(conn, "anl", "rc_state_daily"):
        state_cols = _table_columns(conn, "anl", "rc_state_daily")
        state_date_col = _resolve_date_column(state_cols)
        if {"ticker", "state"}.issubset(state_cols) and state_date_col is not None:
            conn.execute(
                f"""
                INSERT OR REPLACE INTO temp.state_enrichment(ticker, as_of_date, state)
                SELECT ticker, {state_date_col} AS as_of_date, MAX(state) AS state
                FROM anl.rc_state_daily
                GROUP BY ticker, {state_date_col}
                """
            )
            state_loaded = True

    if _table_exists(conn, "anl", "rc_signal_daily"):
        signal_cols = _table_columns(conn, "anl", "rc_signal_daily")
        signal_date_col = _resolve_date_column(signal_cols)
        if "ticker" in signal_cols and signal_date_col is not None:
            aggregated_candidates = ["signals", "signal_keys_json", "signal_keys", "signal_keys_csv"]
            row_signal_candidates = ["signal_key", "signal", "signal_name", "key"]

            aggregated_col = next((col for col in aggregated_candidates if col in signal_cols), None)
            row_signal_col = next((col for col in row_signal_candidates if col in signal_cols), None)

            if aggregated_col is not None:
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO temp.signal_enrichment(ticker, as_of_date, signals)
                    SELECT ticker, {signal_date_col} AS as_of_date, MAX({aggregated_col}) AS signals
                    FROM anl.rc_signal_daily
                    GROUP BY ticker, {signal_date_col}
                    """
                )
                signal_loaded = True
            elif row_signal_col is not None:
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO temp.signal_enrichment(ticker, as_of_date, signals)
                    SELECT d.ticker,
                           d.as_of_date,
                           (
                             SELECT GROUP_CONCAT(x.signal_key, ',')
                             FROM (
                               SELECT s2.{row_signal_col} AS signal_key
                               FROM anl.rc_signal_daily s2
                               WHERE s2.ticker = d.ticker
                                 AND s2.{signal_date_col} = d.as_of_date
                               ORDER BY s2.{row_signal_col}
                             ) x
                           ) AS signals
                    FROM (
                      SELECT DISTINCT ticker, {signal_date_col} AS as_of_date
                      FROM anl.rc_signal_daily
                    ) d
                    """
                )
                signal_loaded = True

    return state_loaded, signal_loaded


def _build_insert_candidates_sql(
    ohlcv_table: str,
    ticker_col: str,
    date_col: str,
    close_col: str,
    horizons: list[int],
) -> str:
    base_ctes: list[str] = []
    filtered_ctes: list[str] = []
    union_parts: list[str] = []

    for idx, horizon in enumerate(horizons):
        base_name = f"base_{idx}"
        filtered_name = f"cand_{idx}"

        base_ctes.append(
            f"""
            {base_name} AS (
                SELECT
                    ticker,
                    date AS as_of_date,
                    close AS close_t,
                    LEAD(close, {horizon}) OVER (
                        PARTITION BY ticker
                        ORDER BY date
                    ) AS close_t_plus_n
                FROM (
                    SELECT
                        {ticker_col} AS ticker,
                        {date_col} AS date,
                        {close_col} AS close
                    FROM osk.{ohlcv_table}
                    WHERE (? IS NULL OR market = ?)
                ) ohlcv
            )
            """
        )

        filtered_ctes.append(
            f"""
            {filtered_name} AS (
                SELECT
                    ticker,
                    as_of_date,
                    {horizon} AS horizon,
                    close_t,
                    close_t_plus_n,
                    CASE
                        WHEN close_t IS NOT NULL
                         AND close_t_plus_n IS NOT NULL
                         AND close_t != 0
                        THEN (close_t_plus_n / close_t) - 1.0
                        ELSE NULL
                    END AS fwd_return
                FROM {base_name}
                WHERE close_t_plus_n IS NOT NULL
                  AND (? IS NULL OR as_of_date >= ?)
                  AND (? IS NULL OR as_of_date <= ?)
            )
            """
        )

        union_parts.append(
            f"SELECT ticker, as_of_date, horizon, close_t, close_t_plus_n, fwd_return FROM {filtered_name}"
        )

    ctes = ",\n".join(base_ctes + filtered_ctes)
    unioned = "\nUNION ALL\n".join(union_parts)

    return f"""
        WITH
        {ctes},
        all_candidates AS (
            {unioned}
        ),
        valid_candidates AS (
            SELECT
                ticker,
                as_of_date,
                horizon,
                close_t,
                close_t_plus_n,
                fwd_return
            FROM all_candidates
            WHERE fwd_return IS NOT NULL
        ),
        ranked AS (
            SELECT
                ticker,
                as_of_date,
                horizon,
                close_t,
                close_t_plus_n,
                fwd_return,
                CUME_DIST() OVER (
                    PARTITION BY horizon
                    ORDER BY fwd_return
                ) AS cume_dist
            FROM valid_candidates
        ),
        flagged AS (
            SELECT
                ticker,
                as_of_date,
                horizon,
                close_t,
                close_t_plus_n,
                fwd_return,
                cume_dist,
                CASE WHEN cume_dist >= ? THEN 1 ELSE 0 END AS is_top10,
                CASE WHEN cume_dist >= ? THEN 1 ELSE 0 END AS is_top5
            FROM ranked
        )
        INSERT INTO research_forward_edge_candidates (
            ticker,
            as_of_date,
            horizon,
            close_t,
            close_t_plus_n,
            fwd_return,
            cume_dist,
            is_top10,
            is_top5,
            state,
            signals
        )
        SELECT
            f.ticker,
            f.as_of_date,
            f.horizon,
            f.close_t,
            f.close_t_plus_n,
            f.fwd_return,
            f.cume_dist,
            f.is_top10,
            f.is_top5,
            se.state,
            sige.signals
        FROM flagged f
        LEFT JOIN temp.state_enrichment se
          ON se.ticker = f.ticker
         AND se.as_of_date = f.as_of_date
        LEFT JOIN temp.signal_enrichment sige
          ON sige.ticker = f.ticker
         AND sige.as_of_date = f.as_of_date
    """


def _insert_summary(
    conn: sqlite3.Connection,
    start_date: str | None,
    end_date: str | None,
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO research_forward_edge_summary (
            horizon,
            n_total,
            n_top10,
            n_top5,
            avg_all,
            avg_top10,
            avg_top5,
            start_date,
            end_date,
            created_at
        )
        SELECT
            horizon,
            COUNT(*) AS n_total,
            SUM(is_top10) AS n_top10,
            SUM(is_top5) AS n_top5,
            AVG(fwd_return) AS avg_all,
            AVG(CASE WHEN is_top10 = 1 THEN fwd_return END) AS avg_top10,
            AVG(CASE WHEN is_top5 = 1 THEN fwd_return END) AS avg_top5,
            ?,
            ?,
            ?
        FROM research_forward_edge_candidates
        GROUP BY horizon
        ORDER BY horizon
        """,
        (start_date, end_date, created_at),
    )


def _count_universe_tickers(
    conn: sqlite3.Connection,
    ohlcv_table: str,
    ticker_col: str,
    market: str | None,
) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(DISTINCT {ticker_col})
        FROM osk.{ohlcv_table}
        WHERE (? IS NULL OR market = ?)
        """,
        (market, market),
    ).fetchone()
    return int(row[0] or 0)


def _print_console_summary(conn: sqlite3.Connection, state_loaded: bool) -> None:
    rows = conn.execute(
        """
        SELECT horizon, n_total, n_top10, n_top5, avg_top10, avg_top5
        FROM research_forward_edge_summary
        ORDER BY horizon
        """
    ).fetchall()

    for row in rows:
        print(
            f"horizon={row[0]} n_total={row[1]} n_top10={row[2]} n_top5={row[3]} "
            f"avg_top10={row[4]} avg_top5={row[5]}"
        )

    if state_loaded:
        entry_stats = conn.execute(
            """
            SELECT
                COUNT(*) AS n_top10,
                SUM(CASE WHEN state = 'ENTRY_WINDOW' THEN 1 ELSE 0 END) AS n_entry_window
            FROM research_forward_edge_candidates
            WHERE is_top10 = 1
            """
        ).fetchone()

        n_top10 = int(entry_stats[0] or 0)
        n_entry_window = int(entry_stats[1] or 0)
        pct = (100.0 * n_entry_window / n_top10) if n_top10 > 0 else 0.0
        print(f"top10_entry_window_pct={pct:.6f}")

        state_rows = conn.execute(
            """
            SELECT state, COUNT(*) AS cnt
            FROM research_forward_edge_candidates
            WHERE is_top10 = 1
              AND state IS NOT NULL
            GROUP BY state
            ORDER BY cnt DESC, state ASC
            LIMIT 6
            """
        ).fetchall()

        for row in state_rows:
            print(f"top10_state state={row[0]} count={row[1]}")


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    try:
        _validate_dates(args.start_date, args.end_date)
        _validate_numeric_args(args.horizons, args.top_pcts)
        ohlcv_table = _validate_identifier(args.ohlcv_table)
        ticker_col = _validate_identifier(args.ticker_col)
        date_col = _validate_identifier(args.date_col)
        close_col = _validate_identifier(args.close_col)

        osakedata_path = Path(args.osakedata_db)
        analysis_path = Path(args.analysis_db)
        if not osakedata_path.exists():
            raise FileNotFoundError(f"osakedata-db not found: {args.osakedata_db}")
        if not analysis_path.exists():
            raise FileNotFoundError(f"analysis-db not found: {args.analysis_db}")

        out_uri = f"file:{args.out_db}?mode=rwc"
        out_conn = sqlite3.connect(out_uri, uri=True)
        out_conn.row_factory = sqlite3.Row

        try:
            out_conn.execute("PRAGMA foreign_keys=ON")
            out_conn.execute("PRAGMA temp_store=MEMORY")
            out_conn.execute("ATTACH DATABASE ? AS osk", (f"file:{args.osakedata_db}?mode=ro",))
            out_conn.execute("ATTACH DATABASE ? AS anl", (f"file:{args.analysis_db}?mode=ro",))

            if not _table_exists(out_conn, "osk", ohlcv_table):
                raise ValueError(f"OHLCV table does not exist: {ohlcv_table}")

            ohlcv_cols = _table_columns(out_conn, "osk", ohlcv_table)
            required_cols = {ticker_col, date_col, close_col}
            if not required_cols.issubset(ohlcv_cols):
                missing = sorted(required_cols - ohlcv_cols)
                raise ValueError(f"OHLCV table missing required columns: {', '.join(missing)}")
            if args.market is not None and "market" not in ohlcv_cols:
                raise ValueError("OHLCV table missing required column for --market: market")

            universe_tickers = _count_universe_tickers(
                out_conn,
                ohlcv_table=ohlcv_table,
                ticker_col=ticker_col,
                market=args.market,
            )

            out_conn.execute("BEGIN")
            _prepare_output_schema(out_conn)
            out_conn.execute("DELETE FROM research_forward_edge_candidates")
            out_conn.execute("DELETE FROM research_forward_edge_summary")

            state_loaded, _signal_loaded = _prepare_enrichment_tables(out_conn)

            insert_sql = _build_insert_candidates_sql(
                ohlcv_table=ohlcv_table,
                ticker_col=ticker_col,
                date_col=date_col,
                close_col=close_col,
                horizons=args.horizons,
            )
            params: list[object] = []
            for _ in args.horizons:
                params.extend([args.market, args.market])
            for _ in args.horizons:
                params.extend([args.start_date, args.start_date, args.end_date, args.end_date])
            params.extend([args.top_pcts[0], args.top_pcts[1]])
            out_conn.execute(insert_sql, params)

            created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            _insert_summary(out_conn, args.start_date, args.end_date, created_at)

            out_conn.commit()
            print(f"market_filter={args.market if args.market is not None else 'none'} universe_tickers={universe_tickers}")
            _print_console_summary(out_conn, state_loaded)
            return 0
        finally:
            out_conn.close()
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
