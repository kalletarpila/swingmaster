from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import squareform


PAIR_SORT_COLS = [
    "share_rolling_residual_corr_gt_050",
    "rolling_residual_corr_mean",
    "sector_residual_correlation",
    "ticker_1",
    "ticker_2",
]
PAIR_SORT_ASC = [False, False, False, True, True]

ROLLING_OUTPUT_COLS = [
    "ticker_1",
    "ticker_2",
    "raw_correlation",
    "sector_residual_correlation",
    "correlation_delta_vs_raw",
    "rolling_residual_corr_mean",
    "rolling_residual_corr_median",
    "rolling_residual_corr_min",
    "rolling_residual_corr_max",
    "rolling_residual_corr_latest",
    "rolling_residual_valid_obs",
    "share_rolling_residual_corr_gt_020",
    "share_rolling_residual_corr_gt_030",
    "share_rolling_residual_corr_gt_050",
    "share_rolling_residual_corr_gt_070",
    "sector_1",
    "industry_1",
    "sector_2",
    "industry_2",
    "same_sector",
    "same_industry",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find stocks with similar sector-neutral residual return behaviour."
    )
    parser.add_argument("--db", required=True, help="Path to usa_close_change.db")
    parser.add_argument("--start-date", default="2024-01-01", help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end-date", default="2026-12-31", help="Inclusive end date YYYY-MM-DD")
    parser.add_argument("--min-obs", type=int, default=200, help="Min valid raw observations per ticker")
    parser.add_argument(
        "--min-sector-peers",
        type=int,
        default=5,
        help="Min same-sector peers needed for sector residual adjustment",
    )
    parser.add_argument(
        "--rolling-window", type=int, default=60, help="Rolling residual correlation window"
    )
    parser.add_argument(
        "--rolling-min-periods",
        type=int,
        default=40,
        help="Min periods for rolling residual correlation",
    )
    parser.add_argument(
        "--top-pairs",
        type=int,
        default=300,
        help="Top N residual-correlation pairs for rolling analysis",
    )
    parser.add_argument(
        "--top-n-report", type=int, default=200, help="Max rows for filtered report files"
    )
    parser.add_argument(
        "--min-residual-correlation",
        type=float,
        default=0.30,
        help="Min full-period sector residual correlation for reports",
    )
    parser.add_argument(
        "--min-rolling-residual-mean",
        type=float,
        default=0.20,
        help="Min rolling residual correlation mean for reports",
    )
    parser.add_argument(
        "--min-share-rolling-residual-gt-030",
        type=float,
        default=0.50,
        help="Min share of rolling residual correlation > 0.30 for reports",
    )
    parser.add_argument(
        "--cluster-threshold",
        type=float,
        default=0.70,
        help="Distance threshold for hierarchical clustering",
    )
    parser.add_argument("--output-dir", required=True, help="Output directory")
    return parser.parse_args()


def validate_database(db_path: Path) -> None:
    if not db_path.exists():
        raise SystemExit(f"ERROR: database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        }
        for required in ("instruments", "price_change_daily"):
            if required not in tables:
                raise SystemExit(f"ERROR: required table '{required}' not found in database")

        required_cols = {
            "instruments": {"ticker", "sector", "industry"},
            "price_change_daily": {"ticker", "trade_date", "close_change"},
        }
        for table, cols in required_cols.items():
            existing = {
                row[1] for row in conn.execute(f"PRAGMA table_info({table});").fetchall()
            }
            missing = cols - existing
            if missing:
                raise SystemExit(
                    f"ERROR: table '{table}' is missing columns: {', '.join(sorted(missing))}"
                )
    finally:
        conn.close()


def load_data(db_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            """
            SELECT
                p.trade_date,
                p.ticker,
                p.close_change,
                i.sector,
                i.industry
            FROM price_change_daily p
            JOIN instruments i
                ON i.ticker = p.ticker
            WHERE p.trade_date BETWEEN ? AND ?
            ORDER BY p.trade_date, p.ticker
            """,
            conn,
            params=(start_date, end_date),
        )
    finally:
        conn.close()

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["sector"] = df["sector"].fillna("").astype(str).str.strip()
    df["industry"] = df["industry"].fillna("").astype(str).str.strip()
    return df


def build_raw_wide_matrix(
    df: pd.DataFrame,
    min_obs: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    meta = (
        df.groupby("ticker", sort=True)
        .agg(
            obs_count_raw=("close_change", "count"),
            first_date=("trade_date", "min"),
            last_date=("trade_date", "max"),
            sector=("sector", "first"),
            industry=("industry", "first"),
        )
        .reset_index()
    )

    valid_tickers = sorted(meta.loc[meta["obs_count_raw"] >= min_obs, "ticker"].tolist())
    filtered_df = df[df["ticker"].isin(valid_tickers)].copy()

    wide = filtered_df.pivot(index="trade_date", columns="ticker", values="close_change")
    wide.columns.name = None
    wide = wide.reindex(columns=valid_tickers)

    meta = meta[meta["ticker"].isin(valid_tickers)].copy()
    meta = meta.sort_values("ticker").reset_index(drop=True)
    return filtered_df, wide, meta


def compute_residual_returns(
    df: pd.DataFrame,
    raw_wide: pd.DataFrame,
    meta: pd.DataFrame,
    min_sector_peers: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, int, int]:
    enriched = df.copy()

    daily = (
        enriched.groupby("trade_date")
        .agg(
            daily_sum_return=("close_change", "sum"),
            daily_count=("close_change", "count"),
            raw_equal_weight_market_return=("close_change", "mean"),
        )
        .reset_index()
    )
    enriched = enriched.merge(daily, on="trade_date", how="left")

    enriched["market_return_ex_ticker"] = np.where(
        enriched["daily_count"] > 1,
        (enriched["daily_sum_return"] - enriched["close_change"]) / (enriched["daily_count"] - 1),
        np.nan,
    )
    enriched["market_residual"] = enriched["close_change"] - enriched["market_return_ex_ticker"]

    sector_stats = (
        enriched.groupby(["trade_date", "sector"])
        .agg(
            daily_sector_sum_market_residual=("market_residual", "sum"),
            daily_sector_count=("market_residual", "count"),
        )
        .reset_index()
    )
    enriched = enriched.merge(sector_stats, on=["trade_date", "sector"], how="left")

    has_sector_adjustment = (
        enriched["sector"].ne("")
        & enriched["market_residual"].notna()
        & ((enriched["daily_sector_count"] - 1) >= min_sector_peers)
    )

    enriched["sector_market_residual_ex_ticker"] = np.where(
        has_sector_adjustment,
        (enriched["daily_sector_sum_market_residual"] - enriched["market_residual"])
        / (enriched["daily_sector_count"] - 1),
        np.nan,
    )

    enriched["sector_neutral_residual"] = np.where(
        enriched["sector_market_residual_ex_ticker"].notna(),
        enriched["market_residual"] - enriched["sector_market_residual_ex_ticker"],
        enriched["market_residual"],
    )

    # When market residual itself is unavailable, leave basis empty so diagnostics count only usable rows.
    enriched["residual_basis"] = np.select(
        [
            enriched["sector_market_residual_ex_ticker"].notna(),
            enriched["market_residual"].notna(),
        ],
        ["MARKET_PLUS_SECTOR", "MARKET_ONLY"],
        default="",
    )

    residual_wide = enriched.pivot(
        index="trade_date", columns="ticker", values="sector_neutral_residual"
    )
    residual_wide.columns.name = None
    residual_wide = residual_wide.reindex(columns=raw_wide.columns.tolist())

    coverage_extra = (
        enriched.groupby("ticker")
        .agg(
            obs_count_residual=("sector_neutral_residual", "count"),
            days_market_plus_sector=(
                "residual_basis",
                lambda s: int((s == "MARKET_PLUS_SECTOR").sum()),
            ),
            days_market_only=("residual_basis", lambda s: int((s == "MARKET_ONLY").sum())),
        )
        .reset_index()
    )

    coverage = meta.merge(coverage_extra, on="ticker", how="left")
    for col in ("obs_count_residual", "days_market_plus_sector", "days_market_only"):
        coverage[col] = coverage[col].fillna(0).astype(int)
    coverage["share_market_plus_sector"] = np.where(
        coverage["obs_count_residual"] > 0,
        coverage["days_market_plus_sector"] / coverage["obs_count_residual"],
        np.nan,
    )

    daily_diagnostics = (
        enriched.groupby("trade_date")
        .agg(
            raw_ticker_count=("close_change", "count"),
            sector_count=("sector", lambda s: len({v for v in s if str(v).strip() != ""})),
            raw_equal_weight_market_return=("close_change", "mean"),
            market_residual_mean=("market_residual", "mean"),
            sector_neutral_residual_mean=("sector_neutral_residual", "mean"),
            sector_neutral_residual_std=("sector_neutral_residual", "std"),
            rows_market_plus_sector=(
                "residual_basis",
                lambda s: int((s == "MARKET_PLUS_SECTOR").sum()),
            ),
            rows_market_only=("residual_basis", lambda s: int((s == "MARKET_ONLY").sum())),
        )
        .reset_index()
    )

    rows_market_plus_sector = int((enriched["residual_basis"] == "MARKET_PLUS_SECTOR").sum())
    rows_market_only = int((enriched["residual_basis"] == "MARKET_ONLY").sum())

    return (
        enriched,
        residual_wide,
        coverage,
        daily_diagnostics,
        rows_market_plus_sector,
        rows_market_only,
    )


def write_residual_ticker_coverage(coverage: pd.DataFrame, out_dir: Path) -> None:
    out = coverage[
        [
            "ticker",
            "obs_count_raw",
            "obs_count_residual",
            "first_date",
            "last_date",
            "sector",
            "industry",
            "days_market_plus_sector",
            "days_market_only",
            "share_market_plus_sector",
        ]
    ].copy()
    out["first_date"] = out["first_date"].dt.strftime("%Y-%m-%d")
    out["last_date"] = out["last_date"].dt.strftime("%Y-%m-%d")
    out = out.sort_values(["obs_count_residual", "ticker"], ascending=[False, True]).reset_index(
        drop=True
    )
    out.to_csv(out_dir / "residual_ticker_coverage.csv", index=False)


def write_residual_daily_diagnostics(diagnostics: pd.DataFrame, out_dir: Path) -> None:
    out = diagnostics[
        [
            "trade_date",
            "raw_ticker_count",
            "sector_count",
            "raw_equal_weight_market_return",
            "market_residual_mean",
            "sector_neutral_residual_mean",
            "sector_neutral_residual_std",
            "rows_market_plus_sector",
            "rows_market_only",
        ]
    ].copy()
    out["trade_date"] = out["trade_date"].dt.strftime("%Y-%m-%d")
    out = out.sort_values("trade_date").reset_index(drop=True)
    out.to_csv(out_dir / "residual_daily_diagnostics.csv", index=False)


def compute_residual_pair_correlations(
    raw_wide: pd.DataFrame,
    residual_wide: pd.DataFrame,
    meta: pd.DataFrame,
    min_obs: int,
) -> pd.DataFrame:
    raw_corr = raw_wide.corr(method="pearson", min_periods=min_obs)
    residual_corr = residual_wide.corr(method="pearson", min_periods=min_obs)

    sec_map = meta.set_index("ticker")["sector"].to_dict()
    ind_map = meta.set_index("ticker")["industry"].to_dict()

    rows = []
    tickers = residual_corr.columns.tolist()
    for i in range(len(tickers)):
        for j in range(i + 1, len(tickers)):
            ticker_1 = tickers[i]
            ticker_2 = tickers[j]
            raw_correlation = raw_corr.iloc[i, j]
            sector_residual_correlation = residual_corr.iloc[i, j]
            if pd.isna(raw_correlation) or pd.isna(sector_residual_correlation):
                continue

            sector_1 = sec_map.get(ticker_1, "")
            sector_2 = sec_map.get(ticker_2, "")
            industry_1 = ind_map.get(ticker_1, "")
            industry_2 = ind_map.get(ticker_2, "")

            rows.append(
                {
                    "ticker_1": ticker_1,
                    "ticker_2": ticker_2,
                    "raw_correlation": raw_correlation,
                    "sector_residual_correlation": sector_residual_correlation,
                    "abs_sector_residual_correlation": abs(sector_residual_correlation),
                    "correlation_delta_vs_raw": sector_residual_correlation - raw_correlation,
                    "sector_1": sector_1,
                    "industry_1": industry_1,
                    "sector_2": sector_2,
                    "industry_2": industry_2,
                    "same_sector": bool(sector_1 and sector_2 and sector_1 == sector_2),
                    "same_industry": bool(industry_1 and industry_2 and industry_1 == industry_2),
                }
            )

    columns = [
        "ticker_1",
        "ticker_2",
        "raw_correlation",
        "sector_residual_correlation",
        "abs_sector_residual_correlation",
        "correlation_delta_vs_raw",
        "sector_1",
        "industry_1",
        "sector_2",
        "industry_2",
        "same_sector",
        "same_industry",
    ]
    pairs = pd.DataFrame(rows, columns=columns)
    if pairs.empty:
        return pairs

    pairs = pairs.sort_values(
        ["sector_residual_correlation", "ticker_1", "ticker_2"],
        ascending=[False, True, True],
    ).reset_index(drop=True)
    return pairs


def compute_residual_rolling_pairs(
    residual_wide: pd.DataFrame,
    pairs: pd.DataFrame,
    top_pairs: int,
    rolling_window: int,
    rolling_min_periods: int,
) -> pd.DataFrame:
    top = pairs[pairs["sector_residual_correlation"] > 0].head(top_pairs).copy()
    if top.empty:
        return pd.DataFrame(columns=ROLLING_OUTPUT_COLS)

    rows = []
    for _, row in top.iterrows():
        ticker_1 = row["ticker_1"]
        ticker_2 = row["ticker_2"]
        rolling = (
            residual_wide[ticker_1]
            .rolling(window=rolling_window, min_periods=rolling_min_periods)
            .corr(residual_wide[ticker_2])
        )
        valid = rolling.dropna()

        if valid.empty:
            rolling_valid_obs = 0
            rolling_mean = np.nan
            rolling_median = np.nan
            rolling_min = np.nan
            rolling_max = np.nan
            rolling_latest = np.nan
            share_gt_020 = np.nan
            share_gt_030 = np.nan
            share_gt_050 = np.nan
            share_gt_070 = np.nan
        else:
            rolling_valid_obs = int(len(valid))
            rolling_mean = valid.mean()
            rolling_median = valid.median()
            rolling_min = valid.min()
            rolling_max = valid.max()
            rolling_latest = valid.iloc[-1]
            share_gt_020 = float((valid > 0.20).sum() / rolling_valid_obs)
            share_gt_030 = float((valid > 0.30).sum() / rolling_valid_obs)
            share_gt_050 = float((valid > 0.50).sum() / rolling_valid_obs)
            share_gt_070 = float((valid > 0.70).sum() / rolling_valid_obs)

        rows.append(
            {
                "ticker_1": ticker_1,
                "ticker_2": ticker_2,
                "raw_correlation": row["raw_correlation"],
                "sector_residual_correlation": row["sector_residual_correlation"],
                "correlation_delta_vs_raw": row["correlation_delta_vs_raw"],
                "rolling_residual_corr_mean": rolling_mean,
                "rolling_residual_corr_median": rolling_median,
                "rolling_residual_corr_min": rolling_min,
                "rolling_residual_corr_max": rolling_max,
                "rolling_residual_corr_latest": rolling_latest,
                "rolling_residual_valid_obs": rolling_valid_obs,
                "share_rolling_residual_corr_gt_020": share_gt_020,
                "share_rolling_residual_corr_gt_030": share_gt_030,
                "share_rolling_residual_corr_gt_050": share_gt_050,
                "share_rolling_residual_corr_gt_070": share_gt_070,
                "sector_1": row["sector_1"],
                "industry_1": row["industry_1"],
                "sector_2": row["sector_2"],
                "industry_2": row["industry_2"],
                "same_sector": row["same_sector"],
                "same_industry": row["same_industry"],
            }
        )

    result = pd.DataFrame(rows, columns=ROLLING_OUTPUT_COLS)
    if result.empty:
        return result

    result = result.sort_values(PAIR_SORT_COLS, ascending=PAIR_SORT_ASC).reset_index(drop=True)
    return result


def write_residual_pair_reports(
    rolling_pairs: pd.DataFrame,
    out_dir: Path,
    top_n_report: int,
    min_residual_correlation: float,
    min_rolling_residual_mean: float,
    min_share_rolling_residual_gt_030: float,
) -> dict[str, int]:
    counts: dict[str, int] = {}

    overall = rolling_pairs[
        (rolling_pairs["sector_residual_correlation"] >= min_residual_correlation)
        & (rolling_pairs["rolling_residual_corr_mean"] >= min_rolling_residual_mean)
        & (
            rolling_pairs["share_rolling_residual_corr_gt_030"]
            >= min_share_rolling_residual_gt_030
        )
    ].copy()
    overall = overall.sort_values(PAIR_SORT_COLS, ascending=PAIR_SORT_ASC).head(top_n_report)
    overall[ROLLING_OUTPUT_COLS].to_csv(out_dir / "residual_top_pairs_overall.csv", index=False)
    counts["rows_residual_top_pairs_overall"] = len(overall)

    same_industry = rolling_pairs[
        (rolling_pairs["same_industry"] == True)
        & (rolling_pairs["sector_residual_correlation"] >= min_residual_correlation)
        & (rolling_pairs["rolling_residual_corr_mean"] >= min_rolling_residual_mean)
    ].copy()
    same_industry = same_industry.sort_values(PAIR_SORT_COLS, ascending=PAIR_SORT_ASC).head(
        top_n_report
    )
    same_industry[ROLLING_OUTPUT_COLS].to_csv(
        out_dir / "residual_top_pairs_same_industry.csv", index=False
    )
    counts["rows_residual_top_pairs_same_industry"] = len(same_industry)

    same_sector_cross_industry = rolling_pairs[
        (rolling_pairs["same_sector"] == True)
        & (rolling_pairs["same_industry"] == False)
        & (rolling_pairs["sector_residual_correlation"] >= min_residual_correlation)
        & (rolling_pairs["rolling_residual_corr_mean"] >= min_rolling_residual_mean)
    ].copy()
    same_sector_cross_industry = same_sector_cross_industry.sort_values(
        PAIR_SORT_COLS, ascending=PAIR_SORT_ASC
    ).head(top_n_report)
    same_sector_cross_industry[ROLLING_OUTPUT_COLS].to_csv(
        out_dir / "residual_top_pairs_same_sector_cross_industry.csv", index=False
    )
    counts["rows_residual_top_pairs_same_sector_cross_industry"] = len(
        same_sector_cross_industry
    )

    cross_sector = rolling_pairs[
        (rolling_pairs["same_sector"] == False)
        & (rolling_pairs["sector_residual_correlation"] >= min_residual_correlation)
        & (rolling_pairs["rolling_residual_corr_mean"] >= min_rolling_residual_mean)
    ].copy()
    cross_sector = cross_sector.sort_values(PAIR_SORT_COLS, ascending=PAIR_SORT_ASC).head(
        top_n_report
    )
    cross_sector[ROLLING_OUTPUT_COLS].to_csv(
        out_dir / "residual_top_pairs_cross_sector.csv", index=False
    )
    counts["rows_residual_top_pairs_cross_sector"] = len(cross_sector)

    unusual = rolling_pairs[
        (rolling_pairs["same_sector"] == False)
        & (rolling_pairs["same_industry"] == False)
        & (rolling_pairs["sector_1"].astype(str).str.strip() != "")
        & (rolling_pairs["sector_2"].astype(str).str.strip() != "")
        & (rolling_pairs["industry_1"].astype(str).str.strip() != "")
        & (rolling_pairs["industry_2"].astype(str).str.strip() != "")
        & (rolling_pairs["sector_residual_correlation"] >= min_residual_correlation)
        & (rolling_pairs["rolling_residual_corr_mean"] >= min_rolling_residual_mean)
        & (
            rolling_pairs["share_rolling_residual_corr_gt_030"]
            >= min_share_rolling_residual_gt_030
        )
    ].copy()
    unusual["residual_sync_score"] = (
        0.40 * unusual["rolling_residual_corr_mean"]
        + 0.25 * unusual["share_rolling_residual_corr_gt_030"]
        + 0.20 * unusual["share_rolling_residual_corr_gt_050"]
        + 0.10 * unusual["sector_residual_correlation"]
        + 0.05 * unusual["correlation_delta_vs_raw"].clip(lower=0)
    )
    unusual = unusual.sort_values(
        [
            "residual_sync_score",
            "rolling_residual_corr_mean",
            "sector_residual_correlation",
            "ticker_1",
            "ticker_2",
        ],
        ascending=[False, False, False, True, True],
    ).head(top_n_report)
    unusual[
        ROLLING_OUTPUT_COLS + ["residual_sync_score"]
    ].to_csv(out_dir / "residual_top_pairs_unusual_sync.csv", index=False)
    counts["rows_residual_top_pairs_unusual_sync"] = len(unusual)

    return counts


def compute_residual_clusters(
    residual_wide: pd.DataFrame,
    coverage: pd.DataFrame,
    min_obs: int,
    cluster_threshold: float,
    out_dir: Path,
) -> tuple[int, int]:
    corr_matrix = residual_wide.corr(method="pearson", min_periods=min_obs)
    corr_filled = corr_matrix.fillna(0.0)
    np.fill_diagonal(corr_filled.values, 1.0)

    distance_matrix = 1.0 - corr_filled
    np.fill_diagonal(distance_matrix.values, 0.0)

    condensed = squareform(distance_matrix.values, checks=False)
    condensed = np.clip(condensed, 0.0, None)
    linkage_matrix = linkage(condensed, method="average")
    labels = fcluster(linkage_matrix, t=cluster_threshold, criterion="distance")

    obs_map = coverage.set_index("ticker")["obs_count_residual"].to_dict()
    sector_map = coverage.set_index("ticker")["sector"].to_dict()
    industry_map = coverage.set_index("ticker")["industry"].to_dict()
    tickers = corr_filled.columns.tolist()

    cluster_df = pd.DataFrame(
        {
            "ticker": tickers,
            "cluster": labels,
            "sector": [sector_map.get(ticker, "") for ticker in tickers],
            "industry": [industry_map.get(ticker, "") for ticker in tickers],
            "obs_count_residual": [obs_map.get(ticker, 0) for ticker in tickers],
        }
    )
    cluster_df = cluster_df.sort_values(
        ["cluster", "sector", "industry", "ticker"],
        ascending=[True, True, True, True],
    ).reset_index(drop=True)
    cluster_df.to_csv(out_dir / "residual_clusters.csv", index=False)

    def _sorted_csv_list(values: pd.Series) -> str:
        items = sorted(
            {
                str(value).strip()
                for value in values
                if value is not None and not (isinstance(value, float) and np.isnan(value))
                and str(value).strip() != ""
            }
        )
        return ", ".join(items)

    summary = (
        cluster_df.groupby("cluster")
        .agg(
            ticker_count=("ticker", "count"),
            sectors=("sector", _sorted_csv_list),
            industries=("industry", _sorted_csv_list),
            tickers=("ticker", _sorted_csv_list),
        )
        .reset_index()
    )
    summary = summary.sort_values(["ticker_count", "cluster"], ascending=[False, True]).reset_index(
        drop=True
    )
    summary.to_csv(out_dir / "residual_cluster_summary.csv", index=False)

    def _sector_count(value: str) -> int:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return 0
        parts = [part.strip() for part in str(value).split(",") if part.strip()]
        return len(set(parts))

    cross_sector = summary[summary["ticker_count"] >= 3].copy()
    cross_sector["sector_count"] = cross_sector["sectors"].apply(_sector_count)
    cross_sector = cross_sector[cross_sector["sector_count"] >= 2].copy()
    cross_sector = cross_sector.sort_values(
        ["sector_count", "ticker_count", "cluster"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    cross_sector[
        ["cluster", "ticker_count", "sector_count", "sectors", "industries", "tickers"]
    ].to_csv(out_dir / "residual_cross_sector_clusters.csv", index=False)

    return int(cluster_df["cluster"].nunique()), int(len(cross_sector))


def write_residual_readme(
    out_dir: Path,
    db_path: Path,
    start_date: str,
    end_date: str,
    min_obs: int,
    min_sector_peers: int,
    rolling_window: int,
    rolling_min_periods: int,
    top_pairs: int,
    top_n_report: int,
    min_residual_correlation: float,
    min_rolling_residual_mean: float,
    min_share_rolling_residual_gt_030: float,
    cluster_threshold: float,
) -> None:
    lines = [
        "# Residual Similar Stocks",
        "",
        f"Input database: `{db_path}`",
        f"Output directory: `{out_dir}`",
        f"Date range: `{start_date}` to `{end_date}`",
        "",
        "## Thresholds",
        "",
        f"- min_obs: `{min_obs}`",
        f"- min_sector_peers: `{min_sector_peers}`",
        f"- rolling_window: `{rolling_window}`",
        f"- rolling_min_periods: `{rolling_min_periods}`",
        f"- top_pairs: `{top_pairs}`",
        f"- top_n_report: `{top_n_report}`",
        f"- min_residual_correlation: `{min_residual_correlation}`",
        f"- min_rolling_residual_mean: `{min_rolling_residual_mean}`",
        f"- min_share_rolling_residual_gt_030: `{min_share_rolling_residual_gt_030}`",
        f"- cluster_threshold: `{cluster_threshold}`",
        "",
        "## Concepts",
        "",
        "- Raw correlation uses the original daily `close_change` values as daily percentage returns.",
        "- Market residual removes the same-day equal-weight market move excluding the ticker itself.",
        "- Sector-neutral residual removes the same-day sector residual mean excluding the ticker when enough sector peers are available; otherwise it falls back to market residual only.",
        "- Residual correlation can differ from raw correlation because broad market and sector co-movement have been removed before correlation is measured.",
        "",
        "## Report Types",
        "",
        "- Same-industry pairs: both tickers share the same non-empty industry.",
        "- Same-sector cross-industry pairs: both tickers share the same non-empty sector but different industries.",
        "- Cross-sector pairs: tickers come from different sectors or one sector is empty.",
        "- Unusual residual sync pairs: cross-sector and cross-industry pairs that still move together after market and sector neutralization.",
        "",
        "Residual correlation is a statistical similarity measure only. It is not evidence of causality or a stable trading relationship.",
    ]
    (out_dir / "residual_readme.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    out_dir = Path(args.output_dir)

    validate_database(db_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_data(db_path, args.start_date, args.end_date)
    raw_rows = int(len(df))
    raw_tickers = int(df["ticker"].nunique()) if not df.empty else 0

    if df.empty:
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY start_date={args.start_date}")
        print(f"SUMMARY end_date={args.end_date}")
        print("SUMMARY status=NO_USABLE_DATA")
        print("SUMMARY reason=no_rows_in_date_range")
        return

    filtered_df, raw_wide, meta = build_raw_wide_matrix(df, args.min_obs)
    filtered_tickers = int(len(raw_wide.columns))
    date_rows = int(len(raw_wide.index))

    if filtered_tickers < 2:
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY start_date={args.start_date}")
        print(f"SUMMARY end_date={args.end_date}")
        print(f"SUMMARY raw_rows={raw_rows}")
        print(f"SUMMARY raw_tickers={raw_tickers}")
        print(f"SUMMARY filtered_tickers={filtered_tickers}")
        print("SUMMARY status=NO_USABLE_DATA")
        print("SUMMARY reason=fewer_than_2_tickers_after_min_obs_filter")
        return

    (
        residual_long,
        residual_wide,
        coverage,
        daily_diagnostics,
        rows_market_plus_sector,
        rows_market_only,
    ) = compute_residual_returns(filtered_df, raw_wide, meta, args.min_sector_peers)

    usable_residual_rows = int(residual_long["sector_neutral_residual"].count())
    if usable_residual_rows == 0:
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY start_date={args.start_date}")
        print(f"SUMMARY end_date={args.end_date}")
        print(f"SUMMARY raw_rows={raw_rows}")
        print(f"SUMMARY raw_tickers={raw_tickers}")
        print(f"SUMMARY filtered_tickers={filtered_tickers}")
        print(f"SUMMARY date_rows={date_rows}")
        print(f"SUMMARY min_obs={args.min_obs}")
        print(f"SUMMARY min_sector_peers={args.min_sector_peers}")
        print("SUMMARY status=NO_USABLE_DATA")
        print("SUMMARY reason=no_residual_observations_after_neutralization")
        return

    write_residual_ticker_coverage(coverage, out_dir)
    write_residual_daily_diagnostics(daily_diagnostics, out_dir)

    pairs = compute_residual_pair_correlations(raw_wide, residual_wide, meta, args.min_obs)
    pairs.to_csv(out_dir / "residual_pairs_full_period.csv", index=False)

    rolling_pairs = compute_residual_rolling_pairs(
        residual_wide,
        pairs,
        args.top_pairs,
        args.rolling_window,
        args.rolling_min_periods,
    )
    rolling_pairs.to_csv(out_dir / "residual_pairs_rolling.csv", index=False)

    report_counts = write_residual_pair_reports(
        rolling_pairs,
        out_dir,
        args.top_n_report,
        args.min_residual_correlation,
        args.min_rolling_residual_mean,
        args.min_share_rolling_residual_gt_030,
    )

    residual_cluster_count, rows_residual_cross_sector_clusters = compute_residual_clusters(
        residual_wide,
        coverage,
        args.min_obs,
        args.cluster_threshold,
        out_dir,
    )

    write_residual_readme(
        out_dir,
        db_path,
        args.start_date,
        args.end_date,
        args.min_obs,
        args.min_sector_peers,
        args.rolling_window,
        args.rolling_min_periods,
        args.top_pairs,
        args.top_n_report,
        args.min_residual_correlation,
        args.min_rolling_residual_mean,
        args.min_share_rolling_residual_gt_030,
        args.cluster_threshold,
    )

    print(f"SUMMARY db={db_path}")
    print(f"SUMMARY start_date={args.start_date}")
    print(f"SUMMARY end_date={args.end_date}")
    print(f"SUMMARY raw_rows={raw_rows}")
    print(f"SUMMARY raw_tickers={raw_tickers}")
    print(f"SUMMARY filtered_tickers={filtered_tickers}")
    print(f"SUMMARY date_rows={date_rows}")
    print(f"SUMMARY min_obs={args.min_obs}")
    print(f"SUMMARY min_sector_peers={args.min_sector_peers}")
    print(f"SUMMARY rows_market_plus_sector={rows_market_plus_sector}")
    print(f"SUMMARY rows_market_only={rows_market_only}")
    print(f"SUMMARY residual_pair_rows={len(pairs)}")
    print(f"SUMMARY residual_rolling_pair_rows={len(rolling_pairs)}")
    print(
        f"SUMMARY rows_residual_top_pairs_overall={report_counts['rows_residual_top_pairs_overall']}"
    )
    print(
        "SUMMARY rows_residual_top_pairs_same_industry="
        f"{report_counts['rows_residual_top_pairs_same_industry']}"
    )
    print(
        "SUMMARY rows_residual_top_pairs_same_sector_cross_industry="
        f"{report_counts['rows_residual_top_pairs_same_sector_cross_industry']}"
    )
    print(
        f"SUMMARY rows_residual_top_pairs_cross_sector={report_counts['rows_residual_top_pairs_cross_sector']}"
    )
    print(
        f"SUMMARY rows_residual_top_pairs_unusual_sync={report_counts['rows_residual_top_pairs_unusual_sync']}"
    )
    print(f"SUMMARY residual_cluster_count={residual_cluster_count}")
    print(f"SUMMARY rows_residual_cross_sector_clusters={rows_residual_cross_sector_clusters}")
    print(f"SUMMARY output_dir={out_dir}")
    print("SUMMARY status=OK")


if __name__ == "__main__":
    main()