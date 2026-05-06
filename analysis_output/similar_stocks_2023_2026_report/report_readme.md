# Similar Stocks Report

## Parameters

- **input_dir**: `analysis_output/similar_stocks_2023_2026`
- **output_dir**: `analysis_output/similar_stocks_2023_2026_report`
- **top_n**: 2000
- **min_correlation**: 0.4
- **min_rolling_mean**: 0.3
- **min_share_rolling_gt_050**: 0.3
- **min_cluster_size**: 3

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
