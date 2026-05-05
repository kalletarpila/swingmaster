# V5b: Broad Lead-Lag Analysis

Input database: `usa_close_change.db`
Raw candidate directory: `analysis_output/similar_stocks_report`
Residual candidate directory: `analysis_output/residual_similar_stocks_2024_2026`
Output directory: `analysis_output/lead_lag_pairs_broad_2024_2026`
Date range: `2024-01-01` to `2026-12-31`
Max lag: `5` trading days
Min overlap: `250` aligned observations
Rolling window: `60` days, min_periods `40`
Min non-zero lag correlation: `0.15`
Min non-zero edge vs same-day: `0.03`
Min non-zero edge vs opposite lag: `0.03`
Min stability share (rolling > 0.15): `0.4`

## Difference Between V5 and V5b

V5 (`find_lead_lag_pairs.py`) tests V4 residual-similar candidate pairs for
true lead-lag behavior. V5 found all pairs to be same-day dominant, which is
a valid result: V4 candidates were selected for strong same-day residual correlation.

V5b (`find_lead_lag_pairs_broad.py`) broadens the candidate universe by reading
both raw similarity outputs and residual similarity outputs across all report
files. V5b explicitly reports the best non-zero lag for every pair even when
same-day correlation remains stronger, allowing research into episodic or
weak lead-lag patterns not captured by V5.

## Residual Return Construction

Residual returns are recomputed from the SQLite database using same-day
cross-sectional leave-one-out neutralization identical to V4.
Raw return is the daily percentage close change from `price_change_daily`.
Market residual removes the equal-weight market return of all other tickers
on the same day (leave-one-out). Sector-neutral residual further removes the
same-day mean market residual of same-sector peers (leave-one-out).

## Lag Interpretation

Lags are tested from `-5` to `+5` trading days.

- Positive lag k: `canonical_ticker_1` leads `canonical_ticker_2` by k days.
  Correlation is computed between residual(ticker_1, t) and residual(ticker_2, t+k).
- Negative lag k: `canonical_ticker_2` leads `canonical_ticker_1` by abs(k) days.
  Correlation is computed between residual(ticker_1, t+abs(k)) and residual(ticker_2, t).
- Lag 0: same-day correlation baseline.

## Key Metrics

- `same_day_correlation`: baseline lag=0 Pearson correlation.
- `best_nonzero_lag_correlation`: highest correlation among non-zero lags.
- `nonzero_edge_vs_same_day`: best non-zero lag corr minus same-day corr.
  Positive value means the lagged relationship is stronger than same-day.
- `nonzero_edge_vs_opposite_lag`: best non-zero lag corr minus its mirror lag.
  Positive value indicates directional asymmetry (one direction leads the other).

## Why Broad Lead-Lag Candidates Are Not Trading Rules

Lead-lag correlation is a statistical measure only. It is not evidence of causality.
A detected lead-lag pattern may reflect shared information sources, sector dynamics
not fully removed by neutralization, common macro sensitivity, or statistical noise.
Results may be episodic: a pattern detected in one period may not persist.
Filtered and interesting candidate files are starting points for further research,
not ready-to-use trading rules. All results require independent validation.
