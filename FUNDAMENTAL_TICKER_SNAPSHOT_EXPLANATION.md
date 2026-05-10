# SwingMaster Fundamental + Price Behavior Snapshot

This document explains the current `run_fundamental_ticker_snapshot.py` report as it is implemented now.

The goal is to help an external reader understand:

1. what each section means
2. which metrics are absolute vs relative
3. how the metrics are calculated at a high level
4. which optional technical append sections exist now
5. how the snapshot can be used as a BUY background-check tool
6. the exact calculation rules currently used in code

This document intentionally corrects several earlier misunderstandings:

- `fundamental_score_v1` is not percentile-based and not cross-sectional. It is a rule-based point score.
- `lifecycle_class` does not currently use the labels `STABILIZING` or `WEAKENING`.
- the implemented lifecycle classes are:
  - `STARTUP`
  - `GROWTH`
  - `SCALING`
  - `MATURE`
  - `TRANSITION`
  - `DECLINING`
  - `DISTRESSED`
  - `UNCLASSIFIED`
- the `price_behavior_snapshot` block is a single latest-market snapshot, not a quarter-column series.
- the `valuation_snapshot` block is also a single latest snapshot, not a quarter-column series.

---

# 1. What the Snapshot Is

The ticker snapshot combines these layers:

1. a multi-quarter fundamental history controlled by `--quarters` (always included)
2. a latest valuation snapshot based on the most recent stored valuation row for the ticker (always included)
3. an optional latest price-behavior snapshot

It can also append optional raw technical row sets:

4. Dow structure context and event rows
5. candlestick event rows
6. divergence context and event rows
7. moving-average stock and benchmark rows

When `--output-dir` is used, each ticker snapshot is written to:

- `{TICKER}_{print-date}_{print-date}.csv`

Example:

- `FORTUM.HE_2026-05-10_2026-05-10.csv`

It is not a price target model.

It is mainly a structured decision-support report for questions like:

- Is the business quality strong?
- Is the quality improving or weakening?
- How does the company rank vs peers?
- Does market behavior confirm or reject the fundamentals?

---

# 2. Report Structure

The report currently has these sections, in this order:

1. header rows
2. fundamental scores
3. baseline score components
4. raw TTM profitability and balance-sheet factors
5. percentile scores
6. global factor percentiles
7. selected quarterly raw values
8. 4Q delta rows
9. QoQ delta rows
10. 4Q summary rows
11. sector and industry ranks
12. optional `price_behavior_snapshot` block
13. latest `valuation_snapshot` block
14. optional `section;dow_context_snapshot`
15. optional `section;dow_recent_events_60td`
16. optional `section;candlestick_events_60td`
17. optional `section;divergence_context_snapshot`
18. optional `section;divergence_signals_60td`
19. optional `section;moving_averages_60td`

The first 11 sections are quarter-column based.

The number of displayed quarter columns is controlled by `--quarters`. It is often `4`, but the implementation is not fixed to exactly four.

The optional `price_behavior_snapshot` block is a single latest snapshot, one value per row.

The `valuation_snapshot` block is also a single latest snapshot, one value per row. It does not use the displayed quarter columns.

All technical append sections after `valuation_snapshot` are export-style row blocks.

They are included only when their corresponding CLI flags are enabled.

The CLI also supports multiple tickers in one run.

In that case:

- each ticker gets its own full snapshot
- the snapshots are printed in the same ticker order given by the user
- there is exactly one blank line between ticker snapshots

---

# 3. Lifecycle Class

Variable:

- `lifecycle_class`

Current possible values:

- `STARTUP`
- `GROWTH`
- `SCALING`
- `MATURE`
- `TRANSITION`
- `DECLINING`
- `DISTRESSED`
- `UNCLASSIFIED`

Lifecycle is a rule-based stage label derived from:

- revenue growth
- EBIT margin
- EBIT margin trend
- FCF margin

It is not a machine-learning label and not a percentile score.

Practical interpretation:

- `STARTUP`: very fast growth, but still structurally unprofitable
- `GROWTH`: strong growth, profitability still modest
- `SCALING`: still growing, profitability improving
- `MATURE`: high-quality and already financially efficient
- `TRANSITION`: profitable, but not yet fully mature
- `DECLINING`: growth or margin trend weakening
- `DISTRESSED`: both operating margin and cash flow badly negative
- `UNCLASSIFIED`: none of the explicit rules matched

Important:

- lifecycle is chosen by ordered rules
- the first matching rule wins
- this means rule order matters

---

# 4. Fundamental Scores

Variables:

- `fundamental_score_v1`
- `fundamental_score_v2_lifecycle`

These are not percentile scores.

They are point-based composite scores built from:

- growth
- margin
- margin trend
- free cash flow margin
- leverage
- dilution
- lifecycle bonus/penalty
- consistency

Interpretation:

- higher = stronger rule-based quality
- lower = weaker rule-based quality

Difference between the two:

- `fundamental_score_v1` = baseline point score
- `fundamental_score_v2_lifecycle` = lifecycle-weighted overlay score

So `v2_lifecycle` is not a separate raw model. It is a lifecycle-adjusted version of the same baseline component structure.

---

# 5. Score Components

Variables:

- `growth_component`
- `margin_component`
- `margin_trend_component`
- `fcf_component`
- `consistency_component`
- `leverage_component`
- `dilution_component`

These are the baseline component scores used in `fundamental_score_v1`.

They are point buckets, not percentiles.

Example:

- `growth_component = 12`
- `margin_component = 15`

means the company matched certain predefined score thresholds.

Important:

- these rows are absolute rules
- they do not depend on other companies

---

# 6. Raw TTM Factors

Variables:

- `revenue_growth_ttm_yoy`
- `ebit_margin_ttm`
- `ebit_margin_trend_4q`
- `fcf_margin_ttm`
- `fcf_margin_trend_4q`
- `net_debt_to_ebitda`
- `share_dilution_yoy`

These are direct inputs or near-direct inputs into scoring.

They act as a sanity-check layer.

If you see a strong percentile but weak raw economics, that tension matters.

Examples:

- high `revenue_growth_ttm_yoy` = strong top-line expansion
- high `ebit_margin_ttm` = stronger operating profitability
- positive `ebit_margin_trend_4q` = margins improving
- high `fcf_margin_ttm` = earnings converting to cash
- lower `net_debt_to_ebitda` is better
- lower or negative `share_dilution_yoy` is better

---

# 7. Percentile Scores

Variables:

- `fundamental_score_percentile_global`
- `fundamental_score_percentile_sector`
- `fundamental_score_percentile_industry`
- `fundamental_score_percentile_blended`
- `fundamental_score_percentile_blended_lifecycle_weighted`

These are cross-sectional scores on a `0..100` scale.

Meaning:

- `80` means the company is better than about 80% of the comparison set for that level

Levels:

- `global`: all eligible companies in the snapshot universe
- `sector`: same sector, if sector size is large enough
- `industry`: same industry, if industry size is large enough
- `blended`: weighted combination of available levels

Important:

- these are relative, not absolute
- this is where peer comparison actually happens

Also important:

- if sector or industry groups are too small, those percentiles become empty
- the blended score then renormalizes over the levels that remain available

---

# 8. Percentile Rank Bucket

Variable:

- `percentile_rank_bucket`

This is a label derived from `fundamental_score_percentile_blended_lifecycle_weighted`.

Current mapping:

- `>= 90`: `Top 10%`
- `>= 80`: `Top 20%`
- `>= 70`: `Top 30%`
- `>= 60`: `Top 40%`
- `>= 50`: `Above median`
- `>= 40`: `Neutral`
- `>= 30`: `Weak`
- `>= 20`: `Very weak`
- `< 20`: `Bottom bucket`

This is only a convenience label, not a separate score.

---

# 9. Global Factor Percentiles

Variables:

- `growth_pct_global`
- `margin_pct_global`
- `margin_trend_pct_global`
- `fcf_pct_global`
- `consistency_pct_global`
- `leverage_pct_global`
- `dilution_pct_global`

These explain where the percentile strength comes from.

They are global cross-sectional percentiles for the factor families.

Interpretation:

- `growth_pct_global`: relative revenue growth strength
- `margin_pct_global`: relative EBIT margin strength
- `margin_trend_pct_global`: relative EBIT margin improvement strength
- `fcf_pct_global`: relative FCF margin strength
- `consistency_pct_global`: relative stability strength
- `leverage_pct_global`: relative balance-sheet conservatism
- `dilution_pct_global`: relative friendliness to shareholders on share count

Note:

- for `leverage` and `dilution`, lower raw values are better, but the percentile is still expressed so that higher percentile means better

---

# 10. Quarterly Raw Values

Variables:

- `revenue`
- `operating_income`
- `free_cashflow`
- `shares_outstanding`
- `total_debt`

These come from the quarterly table, not from TTM.

They help the reader inspect the quarter-level pattern behind the TTM metrics.

Typical use:

- is revenue still rising?
- is operating income expanding or stalling?
- is free cash flow volatile?
- is share count drifting up?
- is debt rising?

---

# 11. 4Q Delta Rows

Variables:

- `margin_trend_delta_4q`
- `fcf_margin_trend_delta_4q`
- `shares_outstanding_delta_4q`
- `net_debt_to_ebitda_delta_4q`
- `percentile_delta_4q`
- `score_delta_4q`
- `lifecycle_transition_4q`

These are long-horizon change indicators.

Meaning:

- `margin_trend_delta_4q`: latest minus earliest `ebit_margin_trend_4q`
- `fcf_margin_trend_delta_4q`: latest minus earliest `fcf_margin_trend_4q`
- `shares_outstanding_delta_4q`: latest minus earliest quarterly share count
- `net_debt_to_ebitda_delta_4q`: latest minus earliest leverage ratio
- `percentile_delta_4q`: latest lifecycle-weighted blended percentile minus earliest baseline blended percentile
- `score_delta_4q`: latest lifecycle score minus earliest lifecycle score
- `lifecycle_transition_4q`: first lifecycle label to latest lifecycle label

Use:

- QoQ tells the short-term slope
- 4Q tells the more structural shift

---

# 12. QoQ Delta Rows

Variables:

- `score_delta_qoq`
- `percentile_delta_qoq`
- `margin_trend_delta_qoq`
- `fcf_margin_trend_delta_qoq`
- `consistency_delta_qoq`
- `growth_pct_global_delta_qoq`

These are quarter-to-quarter deltas across the displayed columns.

Format rule:

- first quarter column is empty
- each later column = current quarter minus previous quarter

Example:

- `score_delta_qoq;;-6.40;-0.30;-3.30`

means:

- Q1: no previous quarter inside the displayed window
- Q2: `Q2 - Q1 = -6.40`
- Q3: `Q3 - Q2 = -0.30`
- Q4: `Q4 - Q3 = -3.30`

This is useful for detecting near-term acceleration or weakening.

---

# 13. Sector and Industry Rank

Variables:

- `sector_rank_position`
- `industry_rank_position`

Example:

- `Sijalla 3/27`

means the company ranks third within a group of 27.

Current implementation appends the group name only to the latest displayed quarter.

Use:

- industry rank is often the more important micro-comparison
- sector rank is the broader relative context

---

# 14. Price Behavior Snapshot

This block appears only when `--price-behavior-snapshot` is enabled together with `--ohlcv-db`.

Important:

- it is not quarter-column based
- it is a latest market snapshot block
- each row has a single current value

Variables:

- `price_behavior_as_of_date`
- `price_return_3m_pct`
- `price_return_6m_pct`
- `price_return_12m_pct`
- `distance_from_52w_high_pct`
- `relative_strength_6m_vs_sp500_pct`
- `price_return_since_last_report_pct`
- `relative_return_vs_sp500_since_last_report_pct`
- `earnings_reaction_1d_pct`
- `earnings_reaction_3d_pct`
- `post_earnings_drift_20d_pct`
- `volume_ratio_since_last_report_vs_3m_avg`

This layer answers:

- is the market confirming the story?
- did the stock outperform or underperform?
- how did price behave around the last report?

Important current market logic:

- for most tickers, the OHLCV lookup uses `market='usa'` with benchmark `^GSPC`
- for `.HE` tickers, the lookup uses `market='omxh'` with benchmark `^OMXH25`

So the field names still say `...vs_sp500...`, but for OMXH tickers the implemented benchmark is currently `^OMXH25`

---

# 14.1 Moving Average Benchmark Default Logic

The `moving_averages_60td` section appears only when `--moving-average-snapshot` is enabled.

Current default benchmark behavior:

- for `.HE` tickers: benchmark defaults to `^OMXH25` with market `omxh`
- for other tickers: benchmark defaults to `^GSPC` with market `usa`

Override behavior:

- if `--moving-average-benchmark-ticker` and/or `--moving-average-benchmark-market` is provided, explicit values override defaults
- if only one of them is provided, the other side is inferred by current CLI fallback rules

---

# 15. Practical Interpretation Framework

The snapshot is strongest when all three line up:

1. quality is high
2. quality is improving
3. market behavior confirms it

Examples:

## Strong candidate

- high percentile
- `SCALING` or strong `MATURE`
- positive QoQ or 4Q trend
- positive relative strength

## Quality watchlist candidate

- high score / percentile
- lifecycle acceptable
- price confirmation still weak or mixed

## Avoid / low-priority candidate

- weak percentile
- `DECLINING` or `DISTRESSED`
- negative deltas
- weak price behavior

This is a decision-support framework, not a hard-coded buy rule.

---

# 16. Important Corrections vs Earlier Draft

The earlier draft was incomplete or partially incorrect in these key ways:

## 16.1 Lifecycle labels

The implementation does not use:

- `STABILIZING`
- `WEAKENING`

The actual implemented classes are:

- `STARTUP`
- `GROWTH`
- `SCALING`
- `MATURE`
- `TRANSITION`
- `DECLINING`
- `DISTRESSED`
- `UNCLASSIFIED`

## 16.2 Fundamental score is not relative

`fundamental_score_v1` and `fundamental_score_v2_lifecycle` are rule-based point scores.

They are not percentile ranks.

The relative layer starts at the percentile block.

## 16.3 Price behavior snapshot is latest-only

The current implementation does not calculate the price-behavior block separately for each quarter column.

It calculates a single latest snapshot as of the latest OHLCV date available for the ticker.

## 16.4 Quarter-based valuation rows still exist

The main quarter-column table still contains the older quarter-based valuation rows:

- `valuation_date`
- `valuation_fundamental_as_of_date`
- `valuation_fundamental_staleness_days`
- `valuation_ev_ebit`
- `valuation_fcf_yield`
- `valuation_ebit_margin`
- `valuation_bucket`
- `valuation_status`
- `valuation_model_version`

Those rows are still looked up by exact quarter/as-of-date match and remain part of the quarter-column matrix.

The separate `valuation_snapshot` block is added in addition to them and is usually the more useful current valuation view.

## 16.5 Some interpretation labels were not implemented features

Terms like:

- `leader`
- `follower`
- `confirmed leader`
- `momentum without quality`

can be useful analytical language, but they are not stored classification outputs in the current codebase.

They should be treated as analyst interpretation, not native engine fields.

---

# 17. Full Calculation Rules

This section lists the current implementation rules.

## 17.1 Lifecycle Classification Rules

Lifecycle is evaluated in this order. The first match wins.

### `DISTRESSED`

If:

- `ebit_margin_ttm < -0.20`
- and `fcf_margin_ttm < -0.20`

### `STARTUP`

If:

- `revenue_growth_ttm_yoy > 0.30`
- and `ebit_margin_ttm < -0.05`
- and `fcf_margin_ttm < 0`

### `GROWTH`

If:

- `revenue_growth_ttm_yoy > 0.20`
- and `ebit_margin_ttm < 0.10`

### `SCALING`

If:

- `revenue_growth_ttm_yoy > 0.10`
- and `ebit_margin_trend_4q > 0`
- and `ebit_margin_ttm >= 0`

### `MATURE`

If:

- `ebit_margin_ttm >= 0.15`
- and `fcf_margin_ttm >= 0.05`
- and `revenue_growth_ttm_yoy >= -0.05` or `NULL`

### `TRANSITION`

If:

- `0 <= ebit_margin_ttm < 0.15`
- and `fcf_margin_ttm >= 0`
- and `revenue_growth_ttm_yoy >= -0.05` or `NULL`
- and `ebit_margin_trend_4q >= -0.05` or `NULL`

### `DECLINING`

If:

- `revenue_growth_ttm_yoy < -0.05`
- or `ebit_margin_trend_4q < -0.05`

### `UNCLASSIFIED`

Fallback if none of the above matched.

---

## 17.2 Baseline Fundamental Score Component Rules

### Growth component

Based on `revenue_growth_ttm_yoy`:

- `NULL` -> `6`
- `>= 0.30` -> `15`
- `>= 0.20` -> `12`
- `>= 0.10` -> `9`
- `>= 0.00` -> `5`
- else -> `0`

### Margin component

Based on `ebit_margin_ttm`:

- `NULL` -> `0`
- `>= 0.25` -> `15`
- `>= 0.15` -> `12`
- `>= 0.08` -> `8`
- `>= 0.00` -> `4`
- else -> `0`

### Margin trend component

Based on `ebit_margin_trend_4q`:

- `NULL` -> `6`
- `>= 0.05` -> `15`
- `>= 0.02` -> `10`
- `>= 0.00` -> `6`
- else -> `2`

### FCF component

Based on `fcf_margin_ttm`:

- `NULL` -> `0`
- `>= 0.20` -> `15`
- `>= 0.10` -> `12`
- `>= 0.05` -> `8`
- `>= 0.00` -> `4`
- else -> `0`

### Leverage component

Based on `net_debt_to_ebitda`:

- `NULL` -> `8`
- `<= 0` -> `15`
- `<= 1` -> `12`
- `<= 2` -> `8`
- `<= 3` -> `4`
- else -> `0`

### Dilution component

First, if `abs(share_dilution_yoy) > 0.50`, treat it as `NULL` for scoring only.

Then:

- `NULL` -> `5`
- `<= -0.02` -> `10`
- `<= 0.00` -> `8`
- `<= 0.02` -> `5`
- `<= 0.05` -> `2`
- else -> `0`

### Lifecycle component

Based on `lifecycle_class`:

- `STARTUP` -> `-5`
- `GROWTH` -> `2`
- `SCALING` -> `4`
- `MATURE` -> `5`
- `DECLINING` -> `-5`
- `DISTRESSED` -> `-10`
- all others -> `0`

### Consistency component

Uses the latest up to 4 historical values for:

- `revenue_growth_ttm_yoy`
- `ebit_margin_ttm`
- `fcf_margin_ttm`

Needs at least 3 non-null observations for each metric or returns `0`.

For each metric:

- compute population standard deviation
- divide by absolute mean
- this gives coefficient of variation

Take the average coefficient of variation across the 3 metric families.

Then:

- `<= 0.05` -> `10`
- `<= 0.10` -> `8`
- `<= 0.15` -> `6`
- `<= 0.20` -> `4`
- `<= 0.30` -> `2`
- else -> `0`

### Baseline score formula

`fundamental_score_v1` =

- growth component
- + margin component
- + margin trend component
- + fcf component
- + leverage component
- + dilution component
- + lifecycle component
- + consistency component

Then clamp to:

- minimum `0`
- maximum `100`

---

## 17.3 Lifecycle-Weighted Fundamental Score Rules

`fundamental_score_v2_lifecycle` uses the same baseline components, but multiplies them by lifecycle-specific weights.

### `SCALING`

- growth `x 1.25`
- margin `x 0.90`
- margin trend `x 1.25`
- fcf `x 0.90`
- leverage `x 1.00`
- dilution `x 1.00`
- lifecycle `x 1.00`
- consistency `x 1.25`

### `STARTUP`

- growth `x 1.40`
- margin `x 0.60`
- margin trend `x 0.90`
- fcf `x 0.60`
- leverage `x 0.70`
- dilution `x 1.00`
- lifecycle `x 1.00`
- consistency `x 1.15`

### `DISTRESSED`

- growth `x 0.70`
- margin `x 0.60`
- margin trend `x 0.75`
- fcf `x 1.25`
- leverage `x 1.40`
- dilution `x 1.10`
- lifecycle `x 1.00`
- consistency `x 1.20`
- then subtract `4`

### `TRANSITION`

- growth `x 1.15`
- margin `x 1.05`
- margin trend `x 1.35`
- fcf `x 1.00`
- leverage `x 1.00`
- dilution `x 1.00`
- lifecycle `x 1.00`
- consistency `x 1.20`

### `DECLINING`

- growth `x 0.65`
- margin `x 0.85`
- margin trend `x 0.70`
- fcf `x 1.00`
- leverage `x 1.10`
- dilution `x 1.10`
- lifecycle `x 1.00`
- consistency `x 0.80`
- then subtract `3`

### `GROWTH`

- growth `x 1.10`
- margin `x 1.05`
- margin trend `x 1.10`
- fcf `x 1.00`
- leverage `x 1.00`
- dilution `x 1.00`
- lifecycle `x 1.00`
- consistency `x 1.10`

### `MATURE`

- growth `x 0.95`
- margin `x 1.10`
- margin trend `x 1.00`
- fcf `x 1.15`
- leverage `x 1.05`
- dilution `x 1.10`
- lifecycle `x 1.00`
- consistency `x 1.15`

### Others

All factors stay unchanged.

Then sum the lifecycle-weighted components and clamp to `0..100`.

No extra integer rounding is applied.

---

## 17.4 Percentile Score Rules

The percentile system operates on the latest available fundamental snapshot per ticker as of a chosen target date.

### Factor inputs

- `growth` -> `revenue_growth_ttm_yoy`
- `margin` -> `ebit_margin_ttm`
- `margin_trend` -> `ebit_margin_trend_4q`
- `fcf` -> `fcf_margin_ttm`
- `consistency` -> `consistency_component_lifecycle`
- `leverage` -> `net_debt_to_ebitda`
- `dilution` -> `share_dilution_yoy`

### Higher-is-better factors

- `growth`
- `margin`
- `margin_trend`
- `fcf`
- `consistency`

### Lower-is-better factors

- `leverage`
- `dilution`

### Percentile method

For each factor:

- sort values ascending
- ties get average rank
- if `n == 1`, percentile = `100`
- higher-is-better uses direct percentile
- lower-is-better uses inverted percentile

### Factor weights

- growth = `20.0`
- margin = `15.0`
- margin trend = `10.0`
- fcf = `20.0`
- consistency = `20.0`
- leverage = `7.5`
- dilution = `7.5`

### Minimum factor count

Need at least `4` available factor percentiles.

If fewer than 4 are available, that level score is `NULL`.

### Level scores

The system computes:

- `global`
- `sector`
- `industry`

with renormalization over available factors only.

### Minimum group sizes

- sector score valid only if sector size `>= 10`
- industry score valid only if industry size `>= 10`

### Blended percentile

Blended weights:

- global = `0.40`
- sector = `0.35`
- industry = `0.25`

If one level is missing, weights renormalize across the remaining levels.

---

## 17.5 Lifecycle-Weighted Percentile Rules

The lifecycle-weighted percentile score uses the same factor percentiles, but modifies factor weights by lifecycle class.

### `SCALING`

- growth `x 1.15`
- margin `x 0.95`
- margin trend `x 1.20`
- fcf `x 1.05`
- consistency `x 1.10`
- leverage `x 1.00`
- dilution `x 1.00`
- adjustment `0`

### `MATURE`

- growth `x 0.90`
- margin `x 1.15`
- margin trend `x 1.00`
- fcf `x 1.20`
- consistency `x 1.15`
- leverage `x 1.05`
- dilution `x 1.05`
- adjustment `0`

### `GROWTH`

- growth `x 1.15`
- margin `x 1.00`
- margin trend `x 1.10`
- fcf `x 1.00`
- consistency `x 1.10`
- leverage `x 1.00`
- dilution `x 1.00`
- adjustment `0`

### `TRANSITION`

- growth `x 1.05`
- margin `x 1.05`
- margin trend `x 1.10`
- fcf `x 1.15`
- consistency `x 1.20`
- leverage `x 1.00`
- dilution `x 1.05`
- adjustment `0`

### `STARTUP`

- growth `x 1.25`
- margin `x 0.70`
- margin trend `x 1.00`
- fcf `x 0.70`
- consistency `x 1.15`
- leverage `x 0.90`
- dilution `x 1.00`
- adjustment `0`

### `DECLINING`

- growth `x 0.75`
- margin `x 0.90`
- margin trend `x 0.75`
- fcf `x 1.00`
- consistency `x 0.85`
- leverage `x 1.05`
- dilution `x 1.05`
- adjustment `-3`

### `DISTRESSED`

- growth `x 0.70`
- margin `x 0.75`
- margin trend `x 0.70`
- fcf `x 1.10`
- consistency `x 0.85`
- leverage `x 1.15`
- dilution `x 1.10`
- adjustment `-4`

### `UNCLASSIFIED`

All multipliers `1.00`, adjustment `0`.

### Formula

For each available factor:

- effective weight = base factor weight x lifecycle multiplier

Then:

- weighted average percentile over available factors
- plus lifecycle adjustment
- clamp to `0..100`

The blended lifecycle-weighted percentile then combines:

- global lifecycle-weighted score
- sector lifecycle-weighted score
- industry lifecycle-weighted score

using the same `0.40 / 0.35 / 0.25` weights with renormalization.

---

## 17.6 Snapshot QoQ Rules

For displayed quarter columns:

- first column = empty
- each later column = current displayed value minus previous displayed value

This currently applies to:

- `score_delta_qoq` from `fundamental_score_v2_lifecycle`
- `percentile_delta_qoq` from `fundamental_score_percentile_blended_lifecycle_weighted`
- `margin_trend_delta_qoq` from `ebit_margin_trend_4q`
- `fcf_margin_trend_delta_qoq` from `fcf_margin_trend_4q`
- `consistency_delta_qoq` from `consistency_pct_global`
- `growth_pct_global_delta_qoq` from `growth_pct_global`

If either adjacent value is missing, the delta is empty.

---

## 17.7 Price Behavior Snapshot Rules

This block is computed only if:

- `--price-behavior-snapshot`
- and `--ohlcv-db`

are provided.

### OHLCV source

- database table: `osakedata`
- ticker normalized to uppercase
- market is resolved from ticker:
  - `.HE` -> `omxh`
  - otherwise default `usa`
- benchmark ticker is resolved from market:
  - `omxh` -> `^OMXH25`
  - `usa` -> `^GSPC`

### Current anchor

Current anchor = latest OHLCV row available for the ticker.

### Metrics

#### `price_return_3m_pct`

`100 * (anchor_close / close_63_trading_days_ago - 1)`

#### `price_return_6m_pct`

`100 * (anchor_close / close_126_trading_days_ago - 1)`

#### `price_return_12m_pct`

`100 * (anchor_close / close_252_trading_days_ago - 1)`

#### `distance_from_52w_high_pct`

Over the last 252 trading days ending at anchor:

`100 * (anchor_close / max_high_252d - 1)`

#### `relative_strength_6m_vs_sp500_pct`

Ticker 6M return minus benchmark 6M return.

#### `price_return_since_last_report_pct`

Use the latest displayed quarter date as report date proxy.

Report anchor = latest ticker trading day `<= latest quarter date`.

Then:

`100 * (current_anchor_close / report_anchor_close - 1)`

#### `relative_return_vs_sp500_since_last_report_pct`

Ticker since-report return minus benchmark since-report return.

#### `earnings_reaction_1d_pct`

Event anchor = report anchor.

Reaction day 1 = next trading day after event anchor.

`100 * (close_day_1 / close_event_anchor - 1)`

#### `earnings_reaction_3d_pct`

Event anchor = report anchor.

Reaction day 3 = third trading day after event anchor.

`100 * (close_day_3 / close_event_anchor - 1)`

#### `post_earnings_drift_20d_pct`

Event anchor = report anchor.

Drift end = 20 trading days after event anchor.

`100 * (close_20td_after_event / close_event_anchor - 1)`

This is descriptive/reporting only and not safe as a production signal without explicit as-of controls.

#### `volume_ratio_since_last_report_vs_3m_avg`

Numerator:

- average volume from first trading day after report anchor through current anchor

Denominator:

- average volume over the 63 trading days ending at report anchor

Formula:

- numerator average / denominator average

### Missing data behavior

If required data is missing or insufficient:

- output is empty for that metric

The report should not fail just because one price-behavior metric is unavailable.

---

# 18. Valuation Snapshot

This block is appended after `price_behavior_snapshot` if price behavior is enabled.

If price behavior is not enabled, `valuation_snapshot` is appended directly after the quarter-based rows.

Important:

- it is not quarter-column based
- it uses the latest stored row from `rc_fundamental_valuation` for the ticker
- current implementation looks up:

`SELECT * FROM rc_fundamental_valuation WHERE ticker = ? ORDER BY as_of_date DESC LIMIT 1`

- the valuation date is currently stored in `rc_fundamental_valuation.as_of_date`

Variables:

- `valuation_date`
- `valuation_fundamental_as_of_date`
- `valuation_fundamental_staleness_days`
- `valuation_ev_ebit`
- `valuation_fcf_yield`
- `valuation_ebit_margin`
- `adjusted_expensive_threshold`
- `valuation_debt_assumed_zero`
- `valuation_cash_assumed_zero`
- `valuation_bucket`
- `valuation_status`
- `valuation_model_version`

What the block means:

- `valuation_date`: the market-price anchor used by the valuation run
- `valuation_fundamental_as_of_date`: the latest TTM row used, constrained to `<= valuation_date`
- `valuation_fundamental_staleness_days`: `valuation_date - valuation_fundamental_as_of_date`
- `valuation_ev_ebit`: EV / EBIT using the stored valuation logic
- `valuation_fcf_yield`: `fcf_ttm / market_cap` as a decimal, not percent
- `valuation_ebit_margin`: EBIT margin used by the valuation model
- `adjusted_expensive_threshold`: EBIT-margin-based expensive threshold from the valuation model
- `valuation_debt_assumed_zero`: `1` if missing `total_debt` was assumed as zero in Valuation V2.2
- `valuation_cash_assumed_zero`: `1` if missing `cash` was assumed as zero in Valuation V2.2
- `valuation_bucket`: current deterministic valuation label
- `valuation_status`: current data-quality / validity status
- `valuation_model_version`: printed exactly as stored in the valuation table

Important:

- the snapshot code does not reinterpret or normalize this field
- if the stored value is `V2`, the snapshot prints `V2` even if later valuation logic has evolved

Current EV input handling in Valuation V2.2:

- missing `total_debt` is assumed as `0`
- missing `cash` is assumed as `0`
- these assumptions are audited via:
  - `valuation_debt_assumed_zero`
  - `valuation_cash_assumed_zero`

Important:

- missing debt or cash no longer makes valuation invalid by itself
- missing `shares_outstanding` still makes valuation invalid with `valuation_status = MISSING_SHARES`
- missing valuation row does not fail the snapshot
- in that case the block is still printed, but all values are empty

This block answers:

- what is the latest stored valuation view for the ticker?
- how stale is the fundamental anchor behind that valuation?
- did valuation rely on zero-assumption fallback for debt or cash?

This block does not compute valuation itself.

It only reads the latest already-stored valuation row from `rc_fundamental_valuation`.

---

# 19. Dow Structure Snapshot

These sections appear only when `--dow-structure-snapshot` is enabled together with:

- `--dow-analysis-db`
- `--ohlcv-db`

The output sections are:

- `section;dow_context_snapshot`
- `section;dow_recent_events_60td`

Important implementation facts:

- this is raw Dow structure data only
- event availability uses `confirmed_as_of_date <= as_of_date`
- `event_date <= as_of_date` alone is not sufficient
- freshness comes from `stock_dow_structure_status.calculated_through_date`
- freshness is compared against the latest valid close date where `close IS NOT NULL`
- this block does not produce a recommendation, confidence score, or technical verdict

`dow_context_snapshot` is a one-row context output for the ticker and analysis date.

It includes:

- identity fields like `ticker`, `market`, `as_of_date`, `price_source`, `pivot_radius`
- coverage fields like `coverage_status`, `coverage_reason`, `calculated_through_date`
- latest confirmed event fields like `latest_event_type`, `latest_event_date`, `latest_confirmed_as_of_date`
- raw structure fields copied from the latest confirmed event
- warning flags in `dow_warning_flags`

`dow_recent_events_60td` is an event-sequence export.

It contains one row per Dow event inside the recent ticker-specific 60 valid trading-day window.

The rows are ordered by:

- `confirmed_as_of_date ASC`
- `id ASC`

This block answers:

- what was the latest confirmed Dow structure context?
- what exact event sequence led into the current structure state?

It does not answer:

- whether the stock is a buy
- whether the trend is strong enough by itself
- whether the setup is confirmed by other tools

---

# 20. Candlestick Snapshot

This section appears only when `--candlestick-snapshot` is enabled together with:

- `--candlestick-analysis-db`
- `--ohlcv-db`

The output section is:

- `section;candlestick_events_60td`

Important implementation facts:

- the source table is `analysis_findings`
- this is an event table, not a coverage table
- the reader includes only the allowed basic candlestick patterns in V1
- BullDiv combo patterns are excluded
- divergence-based combo patterns are excluded
- no-lookahead uses `date <= as_of_date`

The section contains one row per basic candlestick finding inside the recent ticker-specific 60 valid trading-day window.

Current raw columns are:

- identity and window metadata
- `finding_id`
- `signal_date`
- `pattern`
- `pattern_group`
- `signal_strength`
- `rsi14`
- `created_at`

`pattern_group` is a raw grouping only:

- `BULLISH_CANDLE`
- `BEARISH_CANDLE`

This section answers:

- which basic candlestick events occurred recently?
- on which dates did they occur?

It does not answer:

- whether those events form a combined setup
- whether divergence confirmed them
- whether they imply a buy or sell decision

---

# 21. Divergence Snapshot

These sections appear only when `--divergence-snapshot` is enabled together with:

- `--divergence-analysis-db`
- `--ohlcv-db`

The output sections are:

- `section;divergence_context_snapshot`
- `section;divergence_signals_60td`

Important implementation facts:

- the source table is `divergence_data`
- `divergence_data` stores one processed row per ticker/date, not only event rows
- coverage is based on latest daily `divergence_data.date` versus latest valid close date
- signal availability uses `divergence_data.date <= as_of_date`
- `pivot2_date_r2` and `pivot2_date_r3` are not signal availability dates
- `divergence_signals_60td` includes only confirmed R2/R3 event rows
- strength-only rows do not appear in `divergence_signals_60td`

`divergence_context_snapshot` is a one-row context output for the ticker and analysis date.

It includes:

- window metadata
- coverage fields like `divergence_coverage_status`
- latest daily divergence row fields
- latest R2/R3 event fields like:
  - `latest_signal_date`
  - `latest_signal_pattern`
  - `latest_signal_group`
  - `latest_signal_variant`
  - `latest_signal_direction`
  - `latest_signal_radius`
  - `latest_signal_source_flag`

`divergence_signals_60td` is event-style output.

One row means one actual R2/R3 divergence event.

If one daily `divergence_data` row contains multiple R2/R3 flags, the export contains multiple rows for that date.

Current raw event fields are:

- `signal_date`
- `divergence_pattern`
- `divergence_group`
- `divergence_variant`
- `divergence_direction`
- `divergence_radius`
- `signal_strength`
- `rsi`
- `pivot_gap`
- `pivot_drop_pct`
- `pivot2_date`
- `source_flag`

This block answers:

- what was the latest confirmed divergence event?
- what exact divergence events occurred during the recent 60 trading-day window?

It does not answer:

- whether the divergence is enough by itself
- whether it aligns with fundamentals
- whether it should be traded automatically

---

# 22. Moving Average Snapshot

This section appears only when `--moving-average-snapshot` is enabled together with:

- `--ohlcv-db`

The output section is:

- `section;moving_averages_60td`

Important implementation facts:

- the source database is only `osakedata.db`
- stock output uses the latest 60 valid stock close rows on or before `as_of_date`
- stock moving averages use only stock close rows where `pvm <= trade_date`
- benchmark data defaults to:
  - `benchmark_ticker = ^GSPC`
  - `benchmark_market = usa`
- benchmark close on each stock date uses:
  - same-date `^GSPC` close if available
  - otherwise latest valid `^GSPC` close on or before that stock trade date
- all moving averages are simple moving averages, not exponential
- only `close IS NOT NULL` rows are used

Current raw columns are:

- identity and window metadata
- `trade_date`
- `stock_close`
- `stock_volume`
- `stock_ma50`
- `stock_ma200`
- `benchmark_ticker`
- `benchmark_trade_date`
- `benchmark_close`
- `benchmark_ma50`
- `benchmark_ma200`

This section is raw stock and index context only.

It does not compute:

- trend labels
- crossover labels
- price-above/below flags
- distance from moving averages
- relative strength versus index

This section answers:

- what were the latest 60 valid stock trading days?
- what were stock close, stock volume, stock MA50 and MA200 on those days?
- what were the matching S&P 500 close, MA50 and MA200 values on those same dates?

---

# 23. Recommended Reading Order

A practical reading sequence is:

1. `lifecycle_class`
2. `fundamental_score_v2_lifecycle`
3. `fundamental_score_percentile_blended_lifecycle_weighted`
4. `growth_pct_global`, `margin_pct_global`, `fcf_pct_global`, `consistency_pct_global`
5. QoQ and 4Q delta rows
6. `sector_rank_position` and `industry_rank_position`
7. `price_behavior_snapshot`
8. `valuation_snapshot`
9. optional Dow / candlestick / divergence / moving-average append blocks, if enabled

This gives:

- stage
- quality
- relative standing
- trend
- market confirmation
- current valuation context
- raw technical context when explicitly requested

---

# 24. Bottom Line

The snapshot is best understood as a layered decision-support report:

1. absolute business quality rules
2. relative peer ranking
3. time-direction signals
4. optional market-confirmation signals
5. latest stored valuation context
6. optional raw technical append context

The highest-conviction situations are usually those where:

- lifecycle is favorable
- percentile rank is strong
- QoQ / 4Q momentum is improving
- price behavior does not contradict the fundamental picture
- valuation context is at least understandable and not driven by missing-core-input failures

If technical append sections are enabled, they should be read as additional raw context, not as automatic decision outputs.
