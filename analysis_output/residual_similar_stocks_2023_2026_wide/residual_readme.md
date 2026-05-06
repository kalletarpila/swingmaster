# Residual Similar Stocks

Input database: `usa_close_change.db`
Output directory: `analysis_output/residual_similar_stocks_2023_2026_wide`
Date range: `2023-01-01` to `2026-12-31`

## Thresholds

- min_obs: `600`
- min_sector_peers: `5`
- rolling_window: `60`
- rolling_min_periods: `40`
- top_pairs: `7000`
- top_n_report: `1000`
- min_residual_correlation: `0.2`
- min_rolling_residual_mean: `0.15`
- min_share_rolling_residual_gt_030: `0.3`
- cluster_threshold: `0.7`

## Concepts

- Raw correlation uses the original daily `close_change` values as daily percentage returns.
- Market residual removes the same-day equal-weight market move excluding the ticker itself.
- Sector-neutral residual removes the same-day sector residual mean excluding the ticker when enough sector peers are available; otherwise it falls back to market residual only.
- Residual correlation can differ from raw correlation because broad market and sector co-movement have been removed before correlation is measured.

## Report Types

- Same-industry pairs: both tickers share the same non-empty industry.
- Same-sector cross-industry pairs: both tickers share the same non-empty sector but different industries.
- Cross-sector pairs: tickers come from different sectors or one sector is empty.
- Unusual residual sync pairs: cross-sector and cross-industry pairs that still move together after market and sector neutralization.

Residual correlation is a statistical similarity measure only. It is not evidence of causality or a stable trading relationship.
