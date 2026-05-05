# Lead-Lag Pair Analysis

Input database: `usa_close_change.db`
Candidate directory: `analysis_output/residual_similar_stocks_2024_2026`
Output directory: `analysis_output/lead_lag_pairs_2024_2026`
Date range: `2024-01-01` to `2026-12-31`
Max lag: `5` trading days
Min overlap: `250` aligned observations
Rolling window: `60` days, min_periods `40`
Min best-lag correlation: `0.2`
Min lead-lag edge: `0.05`
Min stability share (rolling > 0.20): `0.5`

## Residual Return Construction

Market- and sector-neutral residual returns are recomputed from the SQLite database
using the same cross-sectional same-day leave-one-out neutralization as V4.
Raw return is the daily percentage close change from `price_change_daily`.
Market residual removes the equal-weight market return of all other tickers on the same day.
Sector-neutral residual further removes the same-day mean market residual of same-sector peers.

## Lag Interpretation

Lags are tested from `-max_lag` to `+max_lag` trading days.

- Positive lag k: `canonical_ticker_1` leads `canonical_ticker_2` by k days.
  Correlation is computed between residual(ticker_1, t) and residual(ticker_2, t+k).
- Negative lag k: `canonical_ticker_2` leads `canonical_ticker_1` by abs(k) days.
  Correlation is computed between residual(ticker_1, t+abs(k)) and residual(ticker_2, t).
- Lag 0: same-day correlation baseline.

## Lead-Lag Edge

lead_lag_edge = best_lag_correlation - same_day_correlation.
A meaningful edge requires lead_lag_edge >= 0.05.
If best_lag == 0, edge is defined as 0.0.

## Signal Classes

- POSSIBLE_LEAD_LAG: best_lag != 0, best_lag_correlation >= threshold, edge >= threshold.
- POSSIBLE_BUT_LOW_CORRELATION: best_lag != 0, edge >= threshold, correlation < threshold.
- WEAK_EDGE: best_lag != 0, but edge below threshold.
- SAME_DAY_DOMINANT: best_lag == 0.
- INSUFFICIENT_DATA: no valid lag correlations available.

## Important Caveats

Lead-lag correlation is a statistical measure only. It is not evidence of causality.
A detected lead-lag pattern may reflect shared information sources, sector dynamics
not fully removed by neutralization, common macro sensitivity, or statistical noise.
Filtered signals are candidates for further research, not ready-to-use trading rules.
Past lead-lag patterns are not guaranteed to persist in the future.
