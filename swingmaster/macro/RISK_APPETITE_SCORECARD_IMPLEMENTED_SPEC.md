# Risk Appetite Scorecard 2.0

## Implemented Functional Specification for SwingMaster

## 1. Purpose

This document defines the implemented behavior of the SwingMaster macro risk regime layer as it exists in the current codebase.

The layer produces one global risk score for each `as_of_date` in the range `0..100` and derives a regime class from that score.

The purpose of the layer is to:

- describe broad market risk appetite
- function as a separate metadata and regime layer
- support reporting and research workflows
- remain deterministic and auditable
- stay independent from ticker-level signal and policy logic

This layer does not modify:

- signal logic
- policy logic
- state machine logic
- episode logic
- existing BUY and SELL rules

It is an additive, read-only metadata layer.

## 2. Official Implemented Structure

Risk Appetite Scorecard 2.0 consists of five components:

1. Bitcoin Momentum, weight 30%
2. Credit Spreads (HY), weight 25%
3. Put/Call Ratio (PCR), weight 15%
4. Global Liquidity Proxy, weight 15%
5. Broad U.S. Dollar Index, weight 15%

The weights sum to exactly `100%`.

The raw score is:

```text
risk_score_raw =
    0.30 * bitcoin_score +
    0.25 * credit_score +
    0.15 * pcr_score +
    0.15 * liquidity_score +
    0.15 * dxy_score
```

The published score is:

```text
risk_score_final = 3-day simple moving average of risk_score_raw
```

Both `risk_score_raw` and `risk_score_final` are rounded to two decimals before persistence.

## 3. Official Implemented Data Sources

The implemented model uses five normalized downstream source codes:

- `BTC_USD_CBBTCUSD`
- `HY_OAS_BAMLH0A0HYM2`
- `FED_WALCL`
- `USD_BROAD_DTWEXBGS`
- `PCR_EQUITY_CBOE`

### 3.1 Bitcoin

Canonical raw source:

- FRED series `CBBTCUSD`

Normalized source code:

- `BTC_USD_CBBTCUSD`

### 3.2 Credit Spreads

Canonical raw source:

- FRED series `BAMLH0A0HYM2`

Normalized source code:

- `HY_OAS_BAMLH0A0HYM2`

### 3.3 Global Liquidity Proxy

Canonical raw source:

- FRED series `WALCL`

Normalized source code:

- `FED_WALCL`

### 3.4 Broad U.S. Dollar Index

Canonical raw source:

- FRED series `DTWEXBGS`

Normalized source code:

- `USD_BROAD_DTWEXBGS`

### 3.5 Put/Call Ratio

Implemented raw source:

- local CBOE CSV files under `/home/kalle/projects/swingmaster/cboe`

The local files contain:

- `total_put_call_ratio`
- `index_put_call_ratio`
- `equity_put_call_ratio`

Three raw source keys are persisted:

- `PCR_EQUITY_CBOE`
- `PCR_TOTAL_CBOE`
- `PCR_INDEX_CBOE`

Only the following source key is used downstream in normalization and scorecard computation:

- `PCR_EQUITY_CBOE`

`PCR_TOTAL_CBOE` and `PCR_INDEX_CBOE` are raw-only in the current implementation.

### 3.6 What Is Not Used Downstream

The current implementation does not use these downstream:

- `PCR_TOTAL_CBOE`
- `PCR_INDEX_CBOE`

The current implementation also does not use `yfinance`.

## 4. SwingMaster Integration Principles

The implemented flow is:

1. raw ingest
2. normalized daily source table
3. derived metric calculation
4. final daily score table
5. reporting and research read path

The layer is global, not ticker-specific.

### 4.1 Scope

This implemented specification defines:

- data retrieval
- raw persistence
- daily normalization
- score computation
- persistence rules
- deterministic behavior
- implemented test coverage

### 4.2 Out of Scope

This implementation does not define:

- trading rules based on the score
- position sizing changes
- BUY and SELL rule changes
- portfolio allocation logic
- UI visualization

## 5. Implemented Data Model

The implemented macro database contains three macro tables:

- `rc_macro_source_raw`
- `macro_source_daily`
- `rc_risk_appetite_daily`

### 5.1 `rc_macro_source_raw`

This is the raw macro ingest table.

```sql
CREATE TABLE rc_macro_source_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_key TEXT NOT NULL,
  vendor TEXT NOT NULL,
  external_series_id TEXT NOT NULL,
  observation_date TEXT NOT NULL,
  raw_value REAL,
  raw_value_text TEXT,
  source_url TEXT NOT NULL,
  loaded_at_utc TEXT NOT NULL,
  run_id TEXT NOT NULL,
  UNIQUE (source_key, observation_date)
);
```

Purpose:

- persist one raw observation per source and date
- preserve source identity
- support deterministic upsert behavior
- support raw auditability

### 5.2 `macro_source_daily`

This is the normalized daily staging table.

```sql
CREATE TABLE macro_source_daily (
  as_of_date TEXT NOT NULL,
  source_code TEXT NOT NULL,
  source_value REAL NOT NULL,
  source_value_raw_text TEXT,
  source_frequency TEXT NOT NULL,
  published_at_utc TEXT NOT NULL,
  retrieved_at_utc TEXT NOT NULL,
  revision_tag TEXT,
  run_id TEXT NOT NULL,
  PRIMARY KEY (as_of_date, source_code)
);
```

The implementation also adds:

```sql
is_forward_filled INTEGER NOT NULL DEFAULT 0
```

Allowed downstream `source_code` values are exactly:

- `BTC_USD_CBBTCUSD`
- `HY_OAS_BAMLH0A0HYM2`
- `FED_WALCL`
- `USD_BROAD_DTWEXBGS`
- `PCR_EQUITY_CBOE`

Purpose:

- persist one aligned daily observation per source and day
- preserve forward-fill status
- separate aligned inputs from derived scores

### 5.3 `rc_risk_appetite_daily`

This is the final daily score table.

```sql
CREATE TABLE rc_risk_appetite_daily (
  as_of_date TEXT PRIMARY KEY,
  btc_ref_5d REAL,
  btc_ma90 REAL,
  btc_mom REAL,
  bitcoin_score REAL,
  hy_spread_5d REAL,
  credit_score REAL,
  pcr_10d REAL,
  pcr_score REAL,
  walcl_latest REAL,
  walcl_13w_ago REAL,
  liquidity_change_13w REAL,
  liquidity_score REAL,
  dxy_ref_5d REAL,
  dxy_ma200 REAL,
  dxy_diff REAL,
  dxy_score REAL,
  risk_score_raw REAL,
  risk_score_final REAL,
  regime_label TEXT,
  regime_label_confirmed TEXT,
  data_quality_status TEXT NOT NULL,
  component_count INTEGER NOT NULL,
  run_id TEXT NOT NULL,
  created_at_utc TEXT NOT NULL
);
```

Allowed `regime_label` and `regime_label_confirmed` values when present:

- `RISK_OFF`
- `DEFENSIVE`
- `NEUTRAL`
- `RISK_ON`
- `EUPHORIC`

Allowed `data_quality_status` values:

- `OK`
- `PARTIAL_FORWARD_FILL`
- `MISSING_COMPONENT`
- `INVALID_SOURCE_VALUE`

Implemented note:

- the score table intentionally allows `NULL` in derived metric columns and regime columns for staging, missing-data, invalid-data, and pre-smoothing rows

## 6. Calendar and Time Alignment

### 6.1 Canonical Computation Day

`as_of_date` is the day for which the score is computed.

The score can be computed for any requested calendar day in the CLI range.

### 6.2 Lookahead Prohibition

No source observation with date greater than `as_of_date` may be used.

This rule is implemented in normalization and preserved in score computation.

### 6.3 Forward-Fill Rules

Normalization uses the latest known observation on or before `as_of_date`, subject to source-specific maximum ages:

- `FED_WALCL`: unbounded forward fill
- `USD_BROAD_DTWEXBGS`: max 5 calendar days
- `HY_OAS_BAMLH0A0HYM2`: max 3 calendar days
- `PCR_EQUITY_CBOE`: max 3 calendar days
- `BTC_USD_CBBTCUSD`: max 2 calendar days

If the maximum age is exceeded, no normalized row is created for that source and date.

## 7. Component Calculations

### 7.1 Bitcoin Momentum

Input:

- `BTC_USD_CBBTCUSD`

Computed values:

```text
btc_ref_5d = 5-day simple moving average
btc_ma90   = 90-day simple moving average
btc_mom    = (btc_ref_5d / btc_ma90) - 1
```

Buckets:

```text
if btc_mom >  0.40 -> 100
elif btc_mom > 0.25 -> 80
elif btc_mom > 0.10 -> 60
elif btc_mom > 0.00 -> 50
elif btc_mom > -0.10 -> 40
else -> 20
```

Output:

- `bitcoin_score in {20, 40, 50, 60, 80, 100}`

### 7.2 Credit Spreads

Input:

- `HY_OAS_BAMLH0A0HYM2`

Computed value:

```text
hy_spread_5d = 5-day simple moving average
```

Buckets:

```text
if hy_spread_5d < 3.5 -> 100
elif hy_spread_5d < 4.5 -> 80
elif hy_spread_5d < 5.5 -> 60
elif hy_spread_5d < 7.0 -> 40
elif hy_spread_5d < 9.0 -> 20
else -> 0
```

Output:

- `credit_score in {0, 20, 40, 60, 80, 100}`

### 7.3 Put/Call Ratio

Input:

- `PCR_EQUITY_CBOE`

Computed value:

```text
pcr_10d = 10-day simple moving average
```

Buckets:

```text
if pcr_10d < 0.60 -> 90
elif pcr_10d < 0.80 -> 70
elif pcr_10d < 1.00 -> 50
elif pcr_10d < 1.20 -> 30
else -> 10
```

Output:

- `pcr_score in {10, 30, 50, 70, 90}`

### 7.4 Global Liquidity Proxy

Input:

- `FED_WALCL`

Computed values:

```text
walcl_latest          = latest known WALCL on or before as_of_date
walcl_13w_ago         = latest known WALCL on or before (as_of_date - 91 days)
liquidity_change_13w  = (walcl_latest / walcl_13w_ago) - 1
```

Buckets:

```text
if liquidity_change_13w >  0.05 -> 100
elif liquidity_change_13w > 0.02 -> 80
elif liquidity_change_13w > 0.00 -> 60
elif liquidity_change_13w > -0.02 -> 40
elif liquidity_change_13w > -0.05 -> 20
else -> 0
```

Output:

- `liquidity_score in {0, 20, 40, 60, 80, 100}`

### 7.5 Broad U.S. Dollar Index

Input:

- `USD_BROAD_DTWEXBGS`

Computed values:

```text
dxy_ref_5d = 5-day simple moving average
dxy_ma200  = 200-day simple moving average
dxy_diff   = (dxy_ref_5d / dxy_ma200) - 1
```

Buckets:

```text
if dxy_diff < -0.05 -> 100
elif dxy_diff < -0.02 -> 80
elif dxy_diff <  0.00 -> 60
elif dxy_diff <  0.03 -> 40
else -> 20
```

Output:

- `dxy_score in {20, 40, 60, 80, 100}`

## 8. Final Score

### 8.1 Raw Score

The raw score is computed exactly as:

```text
risk_score_raw =
    0.30 * bitcoin_score +
    0.25 * credit_score +
    0.15 * pcr_score +
    0.15 * liquidity_score +
    0.15 * dxy_score
```

### 8.2 Precision

`risk_score_raw` and `risk_score_final` are rounded to two decimals.

### 8.3 Final Score Smoothing

`risk_score_final` is the 3-day simple moving average of `risk_score_raw`.

Rules:

- no shorter smoothing window is used
- the first two valid raw-score days do not publish a final score
- before a 3-day valid raw-score history exists, the row remains non-published from a final-score perspective

Implemented behavior:

- rows before the first 3-day valid window are persisted with `data_quality_status = MISSING_COMPONENT`
- `risk_score_final`, `regime_label`, and `regime_label_confirmed` remain `NULL` on those rows

## 9. Regime Classification

`risk_score_final` maps to `regime_label` as follows:

```text
0  <= score < 30 -> RISK_OFF
30 <= score < 45 -> DEFENSIVE
45 <= score < 60 -> NEUTRAL
60 <= score < 75 -> RISK_ON
75 <= score      -> EUPHORIC
```

## 10. Regime Confirmation Rule

The implementation also publishes `regime_label_confirmed`.

Rule:

```text
candidate_today = regime_label(as_of_date)
candidate_yday  = regime_label(as_of_date - 1)
confirmed_yday  = regime_label_confirmed(as_of_date - 1)

if candidate_today == confirmed_yday:
    confirmed_today = confirmed_yday
elif candidate_today == candidate_yday:
    confirmed_today = candidate_today
else:
    confirmed_today = confirmed_yday
```

For the first valid final-score day:

```text
regime_label_confirmed = regime_label
```

## 11. Data Quality Rules

### 11.1 Minimum Requirement

All five component scores must exist to compute `risk_score_raw`.

### 11.2 Missing Components

If any component is missing:

- `risk_score_raw` is not published
- `risk_score_final` is not published
- `data_quality_status = MISSING_COMPONENT`
- `component_count < 5`

### 11.3 Partial Forward Fill

If the row is otherwise valid, but at least one contributing normalized input row has `is_forward_filled = 1`, then:

- `data_quality_status = PARTIAL_FORWARD_FILL`

Otherwise:

- `data_quality_status = OK`

### 11.4 Invalid Source Values

If a required denominator is non-positive in a ratio calculation, the row is marked invalid.

Current implemented invalid cases include:

- `btc_ma90 <= 0`
- `walcl_13w_ago <= 0`
- `dxy_ma200 <= 0`

On invalid input:

- `data_quality_status = INVALID_SOURCE_VALUE`
- `risk_score_final` is not published

## 12. Ingest Specification

### 12.1 FRED

FRED ingest uses `fredapi` and series observations by `series_id`, with deterministic date bounds:

- `observation_start = date_from`
- `observation_end = date_to`

FRED source mappings:

- `CBBTCUSD` -> `BTC_USD_CBBTCUSD`
- `BAMLH0A0HYM2` -> `HY_OAS_BAMLH0A0HYM2`
- `WALCL` -> `FED_WALCL`
- `DTWEXBGS` -> `USD_BROAD_DTWEXBGS`

### 12.2 CBOE PCR

The implemented ingest does not call the old remote archive path directly.

Instead it reads local CSV files from:

- `/home/kalle/projects/swingmaster/cboe`

Expected file columns:

- `date`
- `total_put_call_ratio`
- `index_put_call_ratio`
- `equity_put_call_ratio`
- optional `status`
- optional `fetched_at_utc`

Implemented raw mappings:

- `equity_put_call_ratio` -> `PCR_EQUITY_CBOE`
- `total_put_call_ratio` -> `PCR_TOTAL_CBOE`
- `index_put_call_ratio` -> `PCR_INDEX_CBOE`

Implemented raw `external_series_id` values:

- `EQUITY_PUT_CALL_RATIO`
- `TOTAL_PUT_CALL_RATIO`
- `INDEX_PUT_CALL_RATIO`

Rows with `status` present and not equal to `ok` are skipped.

### 12.3 Raw Persistence Contract

All ingested raw observations are persisted with:

- `source_key`
- `vendor`
- `external_series_id`
- `observation_date`
- `raw_value`
- `raw_value_text`
- `source_url`
- `loaded_at_utc`
- `run_id`

## 13. Implemented Architecture in SwingMaster

The current implementation is organized as:

```text
swingmaster/
  macro/
    raw_ingest.py
    normalize.py
    scorecard.py
  cli/
    run_macro_ingest.py
    run_macro_normalize.py
    run_risk_appetite_scorecard.py
```

### 13.1 CLI 1: Raw Ingest

```bash
PYTHONPATH=. python3 -m swingmaster.cli.run_macro_ingest \
  --db-path <db> \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --mode <mode>
```

Task:

- fetch raw source series
- persist raw rows into `rc_macro_source_raw`

### 13.2 CLI 2: Normalize

```bash
PYTHONPATH=. python3 -m swingmaster.cli.run_macro_normalize \
  --db-path <db> \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --mode <mode>
```

Task:

- read `rc_macro_source_raw`
- build aligned daily rows in `macro_source_daily`

### 13.3 CLI 3: Score Compute

```bash
PYTHONPATH=. python3 -m swingmaster.cli.run_risk_appetite_scorecard \
  --db-path <db> \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  --mode <mode>
```

Task:

- read `macro_source_daily`
- calculate derived metrics
- calculate daily score rows
- upsert `rc_risk_appetite_daily`

## 14. Implemented Test Specification

### 14.1 Bucket Unit Tests

The implemented tests cover bucket boundaries for:

- Bitcoin
- Credit
- PCR
- Liquidity
- DXY

Boundary comparisons follow the implemented operators exactly.

### 14.2 Moving Average Unit Tests

The implementation uses exact full-window SMAs:

- no partial windows
- current day included
- insufficient history returns `None`

### 14.3 Alignment and Forward-Fill Tests

The implemented tests verify:

1. normalization never uses future observations
2. source-specific forward-fill limits are respected
3. same-day vs carried rows set `is_forward_filled` correctly
4. missing normalized inputs produce missing score rows

### 14.4 Final Score Tests

The implemented tests verify:

- weighted raw score calculation
- 3-day smoothing behavior
- two-decimal persistence behavior

### 14.5 Regime Mapping Tests

The implemented tests verify the regime boundaries:

- `29.99 -> RISK_OFF`
- `30.00 -> DEFENSIVE`
- `44.99 -> DEFENSIVE`
- `45.00 -> NEUTRAL`
- `59.99 -> NEUTRAL`
- `60.00 -> RISK_ON`
- `74.99 -> RISK_ON`
- `75.00 -> EUPHORIC`

### 14.6 Regime Confirmation Tests

The implemented tests verify:

- same-as-confirmed path
- two-consecutive-days candidate change path
- revert-to-confirmed path
- first valid row behavior

### 14.7 Integration-Level Coverage

The implemented tests include seeded long normalized datasets and verify:

- deterministic `insert-missing`
- deterministic `upsert`
- bounded `replace-all`
- summary counters vs persisted rows
- compute path dependency on normalized data only

### 14.8 Regression Coverage

The current implementation does not maintain a separate golden-output fixture file.

Regression protection currently relies on:

- deterministic unit tests
- deterministic seeded integration-style tests
- explicit assertions on persisted values and statuses

## 15. Determinism Rules

The implementation follows these deterministic rules:

1. no randomness
2. fixed component weights
3. fixed bucket thresholds
4. fixed moving-average windows
5. no z-score normalization
6. no market-specific parameter variation
7. one global score per day
8. no lookahead
9. deterministic `run_id` calculation by engine version, date bounds, mode, and source set

## 16. Versioning

Implemented scorecard model identifier:

- `RISK_APPETITE_SCORECARD_V1`

Related engine versions:

- raw ingest: `MACRO_RAW_INGEST_V1`
- normalize: `MACRO_NORMALIZE_V1`
- scorecard: `RISK_APPETITE_SCORECARD_V1`

If any of these change materially, the implementation should version the behavior explicitly rather than mutate historical meaning.

## 17. Acceptance Criteria for the Current Implementation

The implementation is considered aligned with this document when:

1. raw sources ingest successfully for the requested date range
2. `rc_macro_source_raw` persists deterministic raw rows
3. `macro_source_daily` contains one aligned row per source and day when allowed by fill rules
4. no future observation is used
5. score buckets match the implemented thresholds
6. `risk_score_raw` and `risk_score_final` are deterministic
7. regime mapping and confirmation are deterministic
8. missing data prevents published final-score rows
9. invalid source values produce `INVALID_SOURCE_VALUE`
10. forward-fill only operates within implemented source limits

## 18. Practical Use in SwingMaster

The implemented layer is intended to remain separate from:

- signal generation
- policy transitions
- state machine transitions
- direct trade execution logic

It can be consumed by:

- reporting
- research queries
- later ranking or filter experiments

without modifying the current trading core.

## 19. Final Implementation Statement

This document defines the currently implemented production-style version of Risk Appetite Scorecard 2.0 in SwingMaster.

It is:

- simple
- deterministic
- auditable
- testable
- separated from ticker-level trading logic

The most important current implementation choices are:

- downstream score computation uses exactly five normalized source codes
- only `PCR_EQUITY_CBOE` flows into normalization and score computation
- `PCR_TOTAL_CBOE` and `PCR_INDEX_CBOE` are persisted only in raw ingest
- missing and invalid rows are persisted explicitly for auditability
- final published rows require a full valid 3-day raw-score history
