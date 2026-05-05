from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find US stocks with similar daily close-change behaviour."
    )
    parser.add_argument("--db", required=True, help="Path to usa_close_change.db")
    parser.add_argument("--start-date", default="2024-01-01", help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end-date", default="2026-12-31", help="Inclusive end date YYYY-MM-DD")
    parser.add_argument("--min-obs", type=int, default=200, help="Min valid observations per ticker")
    parser.add_argument("--rolling-window", type=int, default=60, help="Rolling correlation window (trading days)")
    parser.add_argument("--rolling-min-periods", type=int, default=40, help="Min periods for rolling corr")
    parser.add_argument("--top-pairs", type=int, default=200, help="Top N full-period pairs for rolling analysis")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_database(db_path: Path) -> None:
    if not db_path.exists():
        raise SystemExit(f"ERROR: database not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row[0] for row in cur.fetchall()}
        for required in ("instruments", "price_change_daily"):
            if required not in tables:
                raise SystemExit(f"ERROR: required table '{required}' not found in database")

        # Check required columns
        required_cols = {
            "instruments": {"ticker", "sector", "industry"},
            "price_change_daily": {"ticker", "trade_date", "close_change"},
        }
        for table, cols in required_cols.items():
            cur = conn.execute(f"PRAGMA table_info({table});")
            existing = {row[1] for row in cur.fetchall()}
            missing = cols - existing
            if missing:
                raise SystemExit(f"ERROR: table '{table}' is missing columns: {missing}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

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
            JOIN instruments i ON i.ticker = p.ticker
            WHERE p.trade_date BETWEEN ? AND ?
            ORDER BY p.trade_date, p.ticker
            """,
            conn,
            params=(start_date, end_date),
        )
    finally:
        conn.close()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


# ---------------------------------------------------------------------------
# Wide matrix
# ---------------------------------------------------------------------------

def build_wide_matrix(df: pd.DataFrame, min_obs: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (wide, ticker_meta).
    wide: index=trade_date, columns=ticker, values=close_change.
    ticker_meta: per-ticker sector/industry/obs_count/first_date/last_date.
    """
    wide = df.pivot(index="trade_date", columns="ticker", values="close_change")
    wide.columns.name = None

    # Ticker metadata before filtering
    meta = (
        df.groupby("ticker")
        .agg(
            obs_count=("close_change", "count"),
            first_date=("trade_date", "min"),
            last_date=("trade_date", "max"),
            sector=("sector", "first"),
            industry=("industry", "first"),
        )
        .reset_index()
    )

    # Filter tickers
    valid_tickers = meta.loc[meta["obs_count"] >= min_obs, "ticker"].tolist()
    wide = wide[sorted(valid_tickers)]
    meta_filtered = meta[meta["ticker"].isin(valid_tickers)].copy()

    return wide, meta_filtered


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def write_ticker_coverage(meta: pd.DataFrame, out_dir: Path) -> None:
    out = meta[["ticker", "obs_count", "first_date", "last_date", "sector", "industry"]].copy()
    out["first_date"] = out["first_date"].dt.strftime("%Y-%m-%d")
    out["last_date"] = out["last_date"].dt.strftime("%Y-%m-%d")
    out = out.sort_values(["obs_count", "ticker"], ascending=[False, True]).reset_index(drop=True)
    out.to_csv(out_dir / "ticker_coverage.csv", index=False)


# ---------------------------------------------------------------------------
# Pair correlations
# ---------------------------------------------------------------------------

def compute_pair_correlations(
    wide: pd.DataFrame,
    meta: pd.DataFrame,
    min_obs: int,
) -> pd.DataFrame:
    corr_matrix = wide.corr(method="pearson", min_periods=min_obs)

    # Sector/industry lookup
    sec = meta.set_index("ticker")["sector"].to_dict()
    ind = meta.set_index("ticker")["industry"].to_dict()

    tickers = corr_matrix.columns.tolist()
    rows = []
    for i in range(len(tickers)):
        for j in range(i + 1, len(tickers)):
            t1, t2 = tickers[i], tickers[j]
            c = corr_matrix.iloc[i, j]
            if pd.isna(c):
                continue
            s1, s2 = sec.get(t1), sec.get(t2)
            i1, i2 = ind.get(t1), ind.get(t2)
            rows.append(
                {
                    "ticker_1": t1,
                    "ticker_2": t2,
                    "correlation": c,
                    "abs_correlation": abs(c),
                    "sector_1": s1,
                    "industry_1": i1,
                    "sector_2": s2,
                    "industry_2": i2,
                    "same_sector": s1 == s2 if (s1 and s2) else False,
                    "same_industry": i1 == i2 if (i1 and i2) else False,
                }
            )

    pairs = pd.DataFrame(rows)
    if pairs.empty:
        return pairs
    pairs = pairs.sort_values(
        ["correlation", "ticker_1", "ticker_2"], ascending=[False, True, True]
    ).reset_index(drop=True)
    return pairs


# ---------------------------------------------------------------------------
# Rolling pair summary
# ---------------------------------------------------------------------------

def compute_rolling_pair_summary(
    wide: pd.DataFrame,
    pairs: pd.DataFrame,
    top_pairs: int,
    rolling_window: int,
    rolling_min_periods: int,
) -> pd.DataFrame:
    # Take top N by positive correlation
    top = pairs[pairs["correlation"] > 0].head(top_pairs).copy()
    if top.empty:
        return pd.DataFrame()

    rows = []
    for _, row in top.iterrows():
        t1, t2 = row["ticker_1"], row["ticker_2"]
        if t1 not in wide.columns or t2 not in wide.columns:
            continue
        rolling = (
            wide[t1]
            .rolling(window=rolling_window, min_periods=rolling_min_periods)
            .corr(wide[t2])
        )
        valid = rolling.dropna()
        if valid.empty:
            rolling_valid = 0
            rolling_mean = np.nan
            rolling_median = np.nan
            rolling_min = np.nan
            rolling_max = np.nan
            rolling_latest = np.nan
            gt030 = np.nan
            gt050 = np.nan
            gt070 = np.nan
        else:
            rolling_valid = len(valid)
            rolling_mean = valid.mean()
            rolling_median = valid.median()
            rolling_min = valid.min()
            rolling_max = valid.max()
            rolling_latest = valid.iloc[-1]
            gt030 = (valid > 0.30).sum() / rolling_valid
            gt050 = (valid > 0.50).sum() / rolling_valid
            gt070 = (valid > 0.70).sum() / rolling_valid

        rows.append(
            {
                "ticker_1": t1,
                "ticker_2": t2,
                "full_period_correlation": row["correlation"],
                "rolling_corr_mean": rolling_mean,
                "rolling_corr_median": rolling_median,
                "rolling_corr_min": rolling_min,
                "rolling_corr_max": rolling_max,
                "rolling_corr_latest": rolling_latest,
                "rolling_valid_obs": rolling_valid,
                "share_rolling_corr_gt_030": gt030,
                "share_rolling_corr_gt_050": gt050,
                "share_rolling_corr_gt_070": gt070,
                "sector_1": row["sector_1"],
                "industry_1": row["industry_1"],
                "sector_2": row["sector_2"],
                "industry_2": row["industry_2"],
                "same_sector": row["same_sector"],
                "same_industry": row["same_industry"],
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result = result.sort_values(
        [
            "share_rolling_corr_gt_070",
            "rolling_corr_mean",
            "full_period_correlation",
            "ticker_1",
            "ticker_2",
        ],
        ascending=[False, False, False, True, True],
    ).reset_index(drop=True)
    return result


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------

def compute_clusters(
    wide: pd.DataFrame,
    meta: pd.DataFrame,
    min_obs: int,
    out_dir: Path,
) -> int:
    corr_matrix = wide.corr(method="pearson", min_periods=min_obs)
    # Fill missing with 0, set diagonal to 1 → distance = 1 - corr
    corr_filled = corr_matrix.fillna(0)
    np.fill_diagonal(corr_filled.values, 1.0)
    distance_matrix = 1.0 - corr_filled

    tickers = corr_filled.columns.tolist()
    # Condense distance matrix for linkage (upper triangle)
    from scipy.spatial.distance import squareform
    condensed = squareform(distance_matrix.values, checks=False)
    condensed = np.clip(condensed, 0, None)  # numerical safety

    Z = linkage(condensed, method="average")
    labels = fcluster(Z, t=0.50, criterion="distance")

    obs_map = meta.set_index("ticker")["obs_count"].to_dict()
    sec_map = meta.set_index("ticker")["sector"].to_dict()
    ind_map = meta.set_index("ticker")["industry"].to_dict()

    cluster_df = pd.DataFrame(
        {
            "ticker": tickers,
            "cluster": labels,
            "sector": [sec_map.get(t) for t in tickers],
            "industry": [ind_map.get(t) for t in tickers],
            "obs_count": [obs_map.get(t) for t in tickers],
        }
    )
    cluster_df = cluster_df.sort_values(
        ["cluster", "sector", "industry", "ticker"]
    ).reset_index(drop=True)
    cluster_df.to_csv(out_dir / "clusters.csv", index=False)

    # Cluster summary
    def _join_sorted(series: pd.Series) -> str:
        return ", ".join(sorted(series.dropna().unique().tolist()))

    summary = (
        cluster_df.groupby("cluster")
        .agg(
            ticker_count=("ticker", "count"),
            sectors=("sector", _join_sorted),
            industries=("industry", _join_sorted),
            tickers=("ticker", lambda s: ", ".join(sorted(s.tolist()))),
        )
        .reset_index()
    )
    summary = summary.sort_values(
        ["ticker_count", "cluster"], ascending=[False, True]
    ).reset_index(drop=True)
    summary.to_csv(out_dir / "cluster_summary.csv", index=False)

    return int(cluster_df["cluster"].nunique())


# ---------------------------------------------------------------------------
# PCA
# ---------------------------------------------------------------------------

def compute_pca(
    wide: pd.DataFrame,
    meta: pd.DataFrame,
    out_dir: Path,
) -> int:
    # Fill missing with column median for PCA only
    wide_filled = wide.copy()
    for col in wide_filled.columns:
        median_val = wide_filled[col].median()
        wide_filled[col] = wide_filled[col].fillna(median_val)

    # Standardize
    scaler = StandardScaler()
    X = scaler.fit_transform(wide_filled.values)  # shape: (dates, tickers)
    # PCA over tickers: transpose so tickers are observations
    X_T = X.T  # shape: (tickers, dates)

    n_components = min(10, X_T.shape[0], X_T.shape[1])
    pca = PCA(n_components=n_components, random_state=42)
    loadings = pca.fit_transform(X_T)  # shape: (tickers, n_components)

    tickers = wide.columns.tolist()
    sec_map = meta.set_index("ticker")["sector"].to_dict()
    ind_map = meta.set_index("ticker")["industry"].to_dict()

    factor_cols = [f"factor_{i + 1}" for i in range(n_components)]
    loadings_df = pd.DataFrame(loadings, columns=factor_cols)
    loadings_df.insert(0, "ticker", tickers)
    loadings_df.insert(1, "sector", [sec_map.get(t) for t in tickers])
    loadings_df.insert(2, "industry", [ind_map.get(t) for t in tickers])
    loadings_df = loadings_df.sort_values("ticker").reset_index(drop=True)
    loadings_df.to_csv(out_dir / "pca_loadings.csv", index=False)

    # Explained variance
    evr = pca.explained_variance_ratio_
    ev_df = pd.DataFrame(
        {
            "factor": [f"factor_{i + 1}" for i in range(n_components)],
            "explained_variance_ratio": evr,
            "cumulative_explained_variance_ratio": np.cumsum(evr),
        }
    )
    ev_df.to_csv(out_dir / "pca_explained_variance.csv", index=False)

    # Top tickers per factor
    rows = []
    for fi in range(n_components):
        factor_name = f"factor_{fi + 1}"
        col_vals = loadings_df[["ticker", "sector", "industry", factor_name]].copy()
        col_vals = col_vals.rename(columns={factor_name: "loading"})

        # Positive side: top 20 by loading descending
        pos = col_vals.nlargest(20, "loading").reset_index(drop=True)
        for rank_idx, r in pos.iterrows():
            rows.append(
                {
                    "factor": factor_name,
                    "side": "POSITIVE",
                    "rank": rank_idx + 1,
                    "ticker": r["ticker"],
                    "loading": r["loading"],
                    "sector": r["sector"],
                    "industry": r["industry"],
                }
            )
        # Negative side: top 20 by loading ascending (most negative)
        neg = col_vals.nsmallest(20, "loading").reset_index(drop=True)
        for rank_idx, r in neg.iterrows():
            rows.append(
                {
                    "factor": factor_name,
                    "side": "NEGATIVE",
                    "rank": rank_idx + 1,
                    "ticker": r["ticker"],
                    "loading": r["loading"],
                    "sector": r["sector"],
                    "industry": r["industry"],
                }
            )

    top_df = pd.DataFrame(rows)
    top_df = top_df.sort_values(["factor", "side", "rank"]).reset_index(drop=True)
    top_df.to_csv(out_dir / "pca_top_tickers_by_factor.csv", index=False)

    return n_components


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    out_dir = Path(args.output_dir)

    validate_database(db_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_data(db_path, args.start_date, args.end_date)
    raw_rows = len(df)
    raw_tickers = df["ticker"].nunique()

    if df.empty:
        print(f"SUMMARY db={db_path}")
        print(f"SUMMARY start_date={args.start_date}")
        print(f"SUMMARY end_date={args.end_date}")
        print("SUMMARY status=NO_USABLE_DATA")
        print("SUMMARY reason=no_rows_in_date_range")
        return

    wide, meta = build_wide_matrix(df, args.min_obs)
    filtered_tickers = len(wide.columns)
    date_rows = len(wide)

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

    write_ticker_coverage(meta, out_dir)

    pairs = compute_pair_correlations(wide, meta, args.min_obs)
    pairs.to_csv(out_dir / "similar_pairs_full_period.csv", index=False)
    pair_rows = len(pairs)

    rolling_summary = compute_rolling_pair_summary(
        wide,
        pairs,
        args.top_pairs,
        args.rolling_window,
        args.rolling_min_periods,
    )
    rolling_summary.to_csv(out_dir / "similar_pairs_rolling.csv", index=False)
    rolling_pair_rows = len(rolling_summary)

    cluster_count = compute_clusters(wide, meta, args.min_obs, out_dir)

    pca_factors = compute_pca(wide, meta, out_dir)

    print(f"SUMMARY db={db_path}")
    print(f"SUMMARY start_date={args.start_date}")
    print(f"SUMMARY end_date={args.end_date}")
    print(f"SUMMARY raw_rows={raw_rows}")
    print(f"SUMMARY raw_tickers={raw_tickers}")
    print(f"SUMMARY filtered_tickers={filtered_tickers}")
    print(f"SUMMARY date_rows={date_rows}")
    print(f"SUMMARY min_obs={args.min_obs}")
    print(f"SUMMARY pair_rows={pair_rows}")
    print(f"SUMMARY rolling_pair_rows={rolling_pair_rows}")
    print(f"SUMMARY cluster_count={cluster_count}")
    print(f"SUMMARY pca_factors={pca_factors}")
    print(f"SUMMARY output_dir={out_dir}")
    print("SUMMARY status=OK")


if __name__ == "__main__":
    main()
