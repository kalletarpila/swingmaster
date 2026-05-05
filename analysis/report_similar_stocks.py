from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create second-level similarity reports from find_similar_stocks.py output."
    )
    parser.add_argument("--input-dir", required=True, help="Directory containing similarity CSV files")
    parser.add_argument("--output-dir", required=True, help="Directory where report CSV files are written")
    parser.add_argument("--top-n", type=int, default=200, help="Max rows per report")
    parser.add_argument("--min-correlation", type=float, default=0.50, help="Min full-period correlation")
    parser.add_argument("--min-rolling-mean", type=float, default=0.40, help="Min rolling_corr_mean")
    parser.add_argument("--min-share-rolling-gt-050", type=float, default=0.50,
                        help="Min share_rolling_corr_gt_050")
    parser.add_argument("--min-cluster-size", type=int, default=3, help="Min cluster ticker_count")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

REQUIRED_FILES = [
    "ticker_coverage.csv",
    "similar_pairs_full_period.csv",
    "similar_pairs_rolling.csv",
    "clusters.csv",
    "cluster_summary.csv",
    "pca_loadings.csv",
    "pca_explained_variance.csv",
    "pca_top_tickers_by_factor.csv",
]


def validate_input_files(input_dir: Path) -> None:
    if not input_dir.exists():
        raise SystemExit(f"ERROR: input directory not found: {input_dir}")
    missing = [f for f in REQUIRED_FILES if not (input_dir / f).exists()]
    if missing:
        raise SystemExit(f"ERROR: missing input files: {', '.join(missing)}")


# ---------------------------------------------------------------------------
# Load inputs
# ---------------------------------------------------------------------------

def _to_bool_series(series: pd.Series) -> pd.Series:
    """Normalize bool columns that may arrive as True/False/true/false/1/0 strings."""
    if pd.api.types.is_bool_dtype(series):
        return series
    return series.map(lambda v: str(v).strip().lower() in ("true", "1"))


def load_inputs(input_dir: Path) -> dict[str, pd.DataFrame]:
    data: dict[str, pd.DataFrame] = {}
    for fname in REQUIRED_FILES:
        data[fname] = pd.read_csv(input_dir / fname)

    # Normalize booleans in rolling pairs
    rolling = data["similar_pairs_rolling.csv"]
    for col in ("same_sector", "same_industry"):
        if col in rolling.columns:
            rolling[col] = _to_bool_series(rolling[col])
    data["similar_pairs_rolling.csv"] = rolling

    return data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PAIR_SORT_COLS = [
    "share_rolling_corr_gt_070",
    "rolling_corr_mean",
    "full_period_correlation",
    "ticker_1",
    "ticker_2",
]
_PAIR_SORT_ASC = [False, False, False, True, True]

_PAIR_OUTPUT_COLS = [
    "ticker_1",
    "ticker_2",
    "full_period_correlation",
    "rolling_corr_mean",
    "rolling_corr_median",
    "rolling_corr_latest",
    "rolling_valid_obs",
    "share_rolling_corr_gt_030",
    "share_rolling_corr_gt_050",
    "share_rolling_corr_gt_070",
    "sector_1",
    "industry_1",
    "sector_2",
    "industry_2",
    "same_sector",
    "same_industry",
]


def _sorted_csv_list(values: pd.Series) -> str:
    """Comma-separated sorted unique non-empty string values."""
    items = sorted({
        str(v).strip()
        for v in values
        if v is not None and not (isinstance(v, float) and np.isnan(v)) and str(v).strip() != ""
    })
    return ", ".join(items)


# ---------------------------------------------------------------------------
# Pair reports
# ---------------------------------------------------------------------------

def write_top_pair_reports(
    data: dict[str, pd.DataFrame],
    out_dir: Path,
    top_n: int,
    min_correlation: float,
    min_rolling_mean: float,
    min_share_rolling_gt_050: float,
) -> dict[str, int]:
    rolling = data["similar_pairs_rolling.csv"]
    counts: dict[str, int] = {}

    # 1. top_pairs_overall.csv
    mask = (
        (rolling["full_period_correlation"] >= min_correlation)
        & (rolling["rolling_corr_mean"] >= min_rolling_mean)
        & (rolling["share_rolling_corr_gt_050"] >= min_share_rolling_gt_050)
    )
    df = (
        rolling[mask]
        .sort_values(_PAIR_SORT_COLS, ascending=_PAIR_SORT_ASC)
        .head(top_n)
        .reset_index(drop=True)
    )
    df[_PAIR_OUTPUT_COLS].to_csv(out_dir / "top_pairs_overall.csv", index=False)
    counts["top_pairs_overall"] = len(df)

    # 2. top_pairs_same_industry.csv
    mask2 = (
        (rolling["same_industry"] == True)
        & (rolling["full_period_correlation"] >= min_correlation)
        & (rolling["rolling_corr_mean"] >= min_rolling_mean)
    )
    df2 = (
        rolling[mask2]
        .sort_values(_PAIR_SORT_COLS, ascending=_PAIR_SORT_ASC)
        .head(top_n)
        .reset_index(drop=True)
    )
    df2[_PAIR_OUTPUT_COLS].to_csv(out_dir / "top_pairs_same_industry.csv", index=False)
    counts["top_pairs_same_industry"] = len(df2)

    # 3. top_pairs_same_sector_cross_industry.csv
    mask3 = (
        (rolling["same_sector"] == True)
        & (rolling["same_industry"] == False)
        & (rolling["full_period_correlation"] >= min_correlation)
        & (rolling["rolling_corr_mean"] >= min_rolling_mean)
    )
    df3 = (
        rolling[mask3]
        .sort_values(_PAIR_SORT_COLS, ascending=_PAIR_SORT_ASC)
        .head(top_n)
        .reset_index(drop=True)
    )
    df3[_PAIR_OUTPUT_COLS].to_csv(out_dir / "top_pairs_same_sector_cross_industry.csv", index=False)
    counts["top_pairs_same_sector_cross_industry"] = len(df3)

    # 4. top_pairs_cross_sector.csv
    mask4 = (
        (rolling["same_sector"] == False)
        & (rolling["full_period_correlation"] >= min_correlation)
        & (rolling["rolling_corr_mean"] >= min_rolling_mean)
    )
    df4 = (
        rolling[mask4]
        .sort_values(_PAIR_SORT_COLS, ascending=_PAIR_SORT_ASC)
        .head(top_n)
        .reset_index(drop=True)
    )
    df4[_PAIR_OUTPUT_COLS].to_csv(out_dir / "top_pairs_cross_sector.csv", index=False)
    counts["top_pairs_cross_sector"] = len(df4)

    # 5. top_pairs_unusual_sync.csv
    mask5 = (
        (rolling["same_sector"] == False)
        & (rolling["same_industry"] == False)
        & (rolling["full_period_correlation"] >= min_correlation)
        & (rolling["rolling_corr_mean"] >= min_rolling_mean)
        & (rolling["share_rolling_corr_gt_050"] >= min_share_rolling_gt_050)
    )
    df5 = rolling[mask5].copy()
    df5["sync_score"] = (
        0.40 * df5["rolling_corr_mean"]
        + 0.30 * df5["share_rolling_corr_gt_050"]
        + 0.20 * df5["share_rolling_corr_gt_070"]
        + 0.10 * df5["full_period_correlation"]
    )
    df5 = (
        df5
        .sort_values(
            ["sync_score", "rolling_corr_mean", "full_period_correlation", "ticker_1", "ticker_2"],
            ascending=[False, False, False, True, True],
        )
        .head(top_n)
        .reset_index(drop=True)
    )
    out_cols5 = _PAIR_OUTPUT_COLS + ["sync_score"]
    df5[out_cols5].to_csv(out_dir / "top_pairs_unusual_sync.csv", index=False)
    counts["top_pairs_unusual_sync"] = len(df5)

    return counts


# ---------------------------------------------------------------------------
# Cluster reports
# ---------------------------------------------------------------------------

def write_cluster_reports(
    data: dict[str, pd.DataFrame],
    out_dir: Path,
    min_cluster_size: int,
) -> dict[str, int]:
    clusters = data["clusters.csv"]
    summary = data["cluster_summary.csv"]
    counts: dict[str, int] = {}

    # Filter summary by size
    large_summary = summary[summary["ticker_count"] >= min_cluster_size].copy()

    # 6. largest_clusters_detailed.csv
    large_cluster_ids = set(large_summary["cluster"].tolist())
    detail = clusters[clusters["cluster"].isin(large_cluster_ids)].copy()

    # Merge cluster-level info
    cluster_meta = large_summary[["cluster", "ticker_count", "sectors", "industries"]].rename(
        columns={
            "ticker_count": "cluster_ticker_count",
            "sectors": "cluster_sectors",
            "industries": "cluster_industries",
        }
    )
    detail = detail.merge(cluster_meta, on="cluster", how="left")
    detail = detail.sort_values(
        ["cluster_ticker_count", "cluster", "sector", "industry", "ticker"],
        ascending=[False, True, True, True, True],
    ).reset_index(drop=True)
    detail[
        ["cluster", "ticker", "sector", "industry", "obs_count",
         "cluster_ticker_count", "cluster_sectors", "cluster_industries"]
    ].to_csv(out_dir / "largest_clusters_detailed.csv", index=False)
    counts["largest_clusters_detailed"] = len(detail)

    # 7. largest_clusters_summary.csv
    large_summary_sorted = large_summary.sort_values(
        ["ticker_count", "cluster"], ascending=[False, True]
    ).reset_index(drop=True)
    large_summary_sorted[["cluster", "ticker_count", "sectors", "industries", "tickers"]].to_csv(
        out_dir / "largest_clusters_summary.csv", index=False
    )
    counts["largest_clusters_summary"] = len(large_summary_sorted)

    # 8. cross_sector_clusters.csv
    def _count_unique_sectors(sectors_str: str) -> int:
        if not sectors_str or (isinstance(sectors_str, float) and np.isnan(sectors_str)):
            return 0
        parts = [s.strip() for s in str(sectors_str).split(",") if s.strip()]
        return len(set(parts))

    large_summary["sector_count"] = large_summary["sectors"].apply(_count_unique_sectors)
    cross = large_summary[large_summary["sector_count"] >= 2].copy()
    cross = cross.sort_values(
        ["sector_count", "ticker_count", "cluster"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    cross[["cluster", "ticker_count", "sector_count", "sectors", "industries", "tickers"]].to_csv(
        out_dir / "cross_sector_clusters.csv", index=False
    )
    counts["cross_sector_clusters"] = len(cross)

    return counts


# ---------------------------------------------------------------------------
# PCA report
# ---------------------------------------------------------------------------

def write_pca_report(
    data: dict[str, pd.DataFrame],
    out_dir: Path,
) -> int:
    ev = data["pca_explained_variance.csv"]
    top_tickers = data["pca_top_tickers_by_factor.csv"]

    rows = []
    for _, ev_row in ev.iterrows():
        factor = ev_row["factor"]
        factor_rows = top_tickers[top_tickers["factor"] == factor]

        pos = factor_rows[factor_rows["side"] == "POSITIVE"].nsmallest(10, "rank")
        neg = factor_rows[factor_rows["side"] == "NEGATIVE"].nsmallest(10, "rank")

        rows.append(
            {
                "factor": factor,
                "explained_variance_ratio": ev_row["explained_variance_ratio"],
                "cumulative_explained_variance_ratio": ev_row["cumulative_explained_variance_ratio"],
                "top_positive_tickers": ", ".join(pos["ticker"].tolist()),
                "top_negative_tickers": ", ".join(neg["ticker"].tolist()),
                "top_positive_sectors": _sorted_csv_list(pos["sector"]),
                "top_negative_sectors": _sorted_csv_list(neg["sector"]),
                "top_positive_industries": _sorted_csv_list(pos["industry"]),
                "top_negative_industries": _sorted_csv_list(neg["industry"]),
            }
        )

    result = pd.DataFrame(rows)
    result.to_csv(out_dir / "pca_factor_overview.csv", index=False)
    return len(result)


# ---------------------------------------------------------------------------
# README
# ---------------------------------------------------------------------------

def write_readme(
    input_dir: Path,
    out_dir: Path,
    args: argparse.Namespace,
) -> None:
    content = f"""# Similar Stocks Report

## Parameters

- **input_dir**: `{input_dir}`
- **output_dir**: `{out_dir}`
- **top_n**: {args.top_n}
- **min_correlation**: {args.min_correlation}
- **min_rolling_mean**: {args.min_rolling_mean}
- **min_share_rolling_gt_050**: {args.min_share_rolling_gt_050}
- **min_cluster_size**: {args.min_cluster_size}

## Correlation concepts

**Full-period correlation** is the Pearson correlation computed over the entire date range
using each ticker's daily percentage close change. A value of 1.0 means the two stocks
moved in perfect lockstep every day; 0.0 means no linear relationship.

**Rolling correlation** is computed in a sliding window (default 60 trading days).
It shows whether the similarity is stable over time or only present in certain periods.
The summary includes the mean, median, min, max, and latest rolling correlation value,
as well as the share of windows where correlation exceeded 0.30, 0.50, and 0.70.

## Report files

### top_pairs_overall.csv
All pairs meeting the minimum correlation, rolling mean, and share thresholds, sorted by
consistency of rolling correlation. Not filtered by sector.

### top_pairs_same_industry.csv
Pairs from the same industry. These are expected to be similar and serve as a baseline
for understanding what high correlation looks like within the same business segment.

### top_pairs_same_sector_cross_industry.csv
Pairs from the same sector but different industries. Captures broader sector co-movement
without requiring identical business models.

### top_pairs_cross_sector.csv
Pairs from different sectors. These similarities are less obvious by definition and may
reflect macro factor exposure, shared customer base, or index membership.

### top_pairs_unusual_sync.csv
Cross-sector, cross-industry pairs that still show strong and consistent rolling correlation.
The `sync_score` column ranks pairs by a weighted combination of rolling mean, share of
high-correlation windows, and full-period correlation. These are intended to highlight
non-obvious synchronized behavior worth investigating further.

### largest_clusters_detailed.csv
One row per ticker in clusters with at least `min_cluster_size` members.

### largest_clusters_summary.csv
One summary row per qualifying cluster with ticker lists and sector/industry coverage.

### cross_sector_clusters.csv
Clusters that contain tickers from at least two distinct sectors. These clusters suggest
groups of stocks that move together despite being in different parts of the economy.

### pca_factor_overview.csv
Each row is one PCA statistical factor. Factors are ordered by how much of the total
variance in daily returns they explain.

**Important**: PCA factors are purely statistical components derived from the covariance
structure of returns. They are not automatically tied to economic themes or sectors.
The top positive and negative loading tickers indicate which stocks have the strongest
association with each factor, but naming or interpreting factors requires separate analysis.

## Notes

- All correlation calculations use `close_change` which is already a daily percentage return.
  No additional conversion is applied.
- Missing values are not filled for correlation calculations.
  For PCA, missing values were filled with column medians before fitting.
"""
    (out_dir / "report_readme.md").write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)

    validate_input_files(input_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = load_inputs(input_dir)

    pair_counts = write_top_pair_reports(
        data,
        out_dir,
        top_n=args.top_n,
        min_correlation=args.min_correlation,
        min_rolling_mean=args.min_rolling_mean,
        min_share_rolling_gt_050=args.min_share_rolling_gt_050,
    )

    cluster_counts = write_cluster_reports(data, out_dir, min_cluster_size=args.min_cluster_size)

    pca_rows = write_pca_report(data, out_dir)

    write_readme(input_dir, out_dir, args)

    print(f"SUMMARY input_dir={input_dir}")
    print(f"SUMMARY output_dir={out_dir}")
    print(f"SUMMARY top_n={args.top_n}")
    print(f"SUMMARY min_correlation={args.min_correlation}")
    print(f"SUMMARY min_rolling_mean={args.min_rolling_mean}")
    print(f"SUMMARY min_share_rolling_gt_050={args.min_share_rolling_gt_050}")
    print(f"SUMMARY min_cluster_size={args.min_cluster_size}")
    print(f"SUMMARY rows_top_pairs_overall={pair_counts['top_pairs_overall']}")
    print(f"SUMMARY rows_top_pairs_same_industry={pair_counts['top_pairs_same_industry']}")
    print(f"SUMMARY rows_top_pairs_same_sector_cross_industry={pair_counts['top_pairs_same_sector_cross_industry']}")
    print(f"SUMMARY rows_top_pairs_cross_sector={pair_counts['top_pairs_cross_sector']}")
    print(f"SUMMARY rows_top_pairs_unusual_sync={pair_counts['top_pairs_unusual_sync']}")
    print(f"SUMMARY rows_largest_clusters_detailed={cluster_counts['largest_clusters_detailed']}")
    print(f"SUMMARY rows_largest_clusters_summary={cluster_counts['largest_clusters_summary']}")
    print(f"SUMMARY rows_cross_sector_clusters={cluster_counts['cross_sector_clusters']}")
    print(f"SUMMARY rows_pca_factor_overview={pca_rows}")
    print("SUMMARY status=OK")


if __name__ == "__main__":
    main()
