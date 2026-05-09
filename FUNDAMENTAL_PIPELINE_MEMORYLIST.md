# Fundamental Pipeline Memory List

This is an updated practical memory list for the current fundamentals pipeline in this repository.

It reflects the current CLI and reader behavior in code, including:
- ticker-level fundamentals pipeline
- market-level practical batch flows
- percentile scoring as a separate cross-sectional step
- current ticker snapshot behavior, including optional technical append sections

## 1. Ticker-level pipeline: raw fetch to score

For one ticker, the current end-to-end pipeline entrypoint is:

- `swingmaster/cli/run_fundamental_pipeline.py`

The pipeline stages are:

1. Raw fetch
   - `--source sec_edgar`:
     - fetches SEC CompanyFacts raw facts into `rc_fundamental_statement_raw`
     - implemented through `run_fundamental_bootstrap_sec_raw.py`
   - `--source yfinance`:
     - fetches Yahoo quarterly statement frames and converts them into raw statement rows

2. SEC quarterly reconstruction
   - only on the `sec_edgar` path
   - reconstructs SEC quarterly statement rows from stored SEC raw facts
   - handled by `run_fundamental_sec_reconstruct_quarterly.py`

3. Quarterly build
   - builds normalized quarterly rows into `rc_fundamental_quarterly`
   - for the pipeline this is done through `build_and_insert_quarterly_rows(...)`

4. TTM build
   - builds TTM rows from normalized quarterly rows into `rc_fundamental_ttm`
   - for the pipeline this is done through `build_and_insert_ttm_rows(...)`

5. Lifecycle classification
   - applies lifecycle classification to TTM rows
   - writes `lifecycle_class` into `rc_fundamental_ttm`

6. Fundamental scoring
   - applies rule-based fundamental scoring to TTM rows
   - writes `fundamental_score` into `rc_fundamental_ttm`

7. Optional score explain
   - if `--explain-score` is enabled and the run is not `--dry-run`
   - prints score explain rows after the score step

Important boundary:
- this ticker-level pipeline does not compute cross-sectional percentile scores
- percentile scoring is still a separate explicit step

## 2. Separate percentile scoring step

Cross-sectional percentile scoring is handled by:

- `swingmaster/cli/run_fundamental_score_percentile.py`

It reads:
- fundamentals DB
- `osakedata` DB

It writes:
- `rc_fundamental_score_percentile`

This step is date-specific and market-specific:
- `--as-of-date YYYY-MM-DD`
- `--market usa` or `--market omxh`

This is the step that must exist before the ticker snapshot can read stored percentile rows directly.

## 3. Ticker snapshot build

Ticker snapshot output is built by:

- `swingmaster/cli/run_fundamental_ticker_snapshot.py`

The current snapshot builder combines:
- latest TTM rows
- quarterly base rows
- stored percentile rows from `rc_fundamental_score_percentile`
- same-date peer factor percentiles computed from peer rows loaded from the DB
- latest valuation snapshot if available

The core builder is:
- `build_snapshot_matrix(...)`

## 4. Optional snapshot append sections

The ticker snapshot CLI can optionally append raw technical sections at the end of the snapshot output:

1. `price_behavior_snapshot`
   - requires `--ohlcv-db`

2. Dow structure
   - `section;dow_context_snapshot`
   - `section;dow_recent_events_60td`
   - requires:
     - `--dow-structure-snapshot`
     - `--dow-analysis-db`
     - `--ohlcv-db`

3. Candlestick
   - `section;candlestick_events_60td`
   - requires:
     - `--candlestick-snapshot`
     - `--candlestick-analysis-db`
     - `--ohlcv-db`

4. Divergence
   - `section;divergence_context_snapshot`
   - `section;divergence_signals_60td`
   - requires:
     - `--divergence-snapshot`
     - `--divergence-analysis-db`
     - `--ohlcv-db`

5. Moving averages
   - `section;moving_averages_60td`
   - requires:
     - `--moving-average-snapshot`
     - `--ohlcv-db`

These are raw append sections only.
They do not add recommendation, confidence, score, or interpretation layers.

## 5. Current snapshot output behavior

Current behavior is:

1. Single ticker without `--output-dir`
   - prints the snapshot to stdout

2. Single ticker with `--output-dir`
   - writes one file:
     - `<TICKER>_<RUN_DATE>.csv`
   - does not print the full snapshot to stdout

3. Multiple tickers with `--output-dir`
   - writes one file per ticker
   - each file contains only that ticker snapshot

4. Multiple tickers without `--output-dir`
   - fails clearly

Ticker parsing currently supports:
- single ticker
- comma-separated tickers
- space-separated tickers
- mixed comma and whitespace input

## 6. Practical CLI map by stage

The main current CLIs by stage are:

- raw SEC facts:
  - `run_fundamental_bootstrap_sec_raw.py`

- SEC quarterly reconstruction:
  - `run_fundamental_sec_reconstruct_quarterly.py`

- quarterly build:
  - `run_fundamental_build_quarterly.py`

- TTM build:
  - `run_fundamental_build_ttm.py`
  - or bulk:
  - `run_fundamental_ttm_batch.py`

- lifecycle:
  - `run_fundamental_lifecycle.py`

- score:
  - `run_fundamental_score.py`

- percentile:
  - `run_fundamental_score_percentile.py`

- ticker snapshot:
  - `run_fundamental_ticker_snapshot.py`

## 7. Practical command examples

### 7.1 One USA ticker from SEC raw to score

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_pipeline.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --ticker AMZN \
  --run-id USA_AMZN_2026Q2 \
  --source sec_edgar \
  --retrieved-at-utc 2026-05-09T00:00:00Z
```

### 7.2 One USA ticker snapshot after percentile rows already exist

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_ticker_snapshot.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --ticker AMZN \
  --quarters 4
```

### 7.3 One USA ticker snapshot with all optional technical sections

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_ticker_snapshot.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --ticker AMZN \
  --quarters 4 \
  --ohlcv-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --price-behavior-snapshot \
  --dow-structure-snapshot \
  --dow-analysis-db /home/kalle/projects/rawcandle/data/analysis.db \
  --candlestick-snapshot \
  --candlestick-analysis-db /home/kalle/projects/rawcandle/data/analysis.db \
  --divergence-snapshot \
  --divergence-analysis-db /home/kalle/projects/rawcandle/data/analysis.db \
  --moving-average-snapshot
```

## 8. Market-level examples to percentile score

These are practical examples for running one market up to percentile scoring.

### 8.1 USA market: practical path to percentile score

This is the practical current USA batch path:

1. Yahoo raw load + quarterly normalization + fallback enrichment
2. quarter-state driven refresh through TTM, lifecycle, and score
3. percentile scoring for one target date

Example:

```bash
PYTHONPATH=. python3 swingmaster/cli/run_usa_enrichment_batch.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --run-id USA_BATCH_2026Q2
```

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_quarter_update.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --run-id USA_UPDATE_2026Q2 \
  --market usa
```

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_score_percentile.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --osakedata-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --market usa \
  --as-of-date 2026-05-08 \
  --run-id USA_PCT_2026-05-08
```

Notes:
- this is the cleanest current market-scoped path in the repo
- percentile scoring is still its own explicit final step

### 8.2 USA market: rebuild from already normalized quarterly rows to percentile score

If `rc_fundamental_quarterly` is already populated and you want to rebuild downstream rows:

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_ttm_batch.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --run-id USA_TTM_2026Q2 \
  --market usa \
  --replace-ticker
```

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_lifecycle.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --run-id USA_LIFECYCLE_2026Q2
```

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_score.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --run-id USA_SCORE_2026Q2
```

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_score_percentile.py \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --osakedata-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --market usa \
  --as-of-date 2026-05-08 \
  --run-id USA_PCT_2026-05-08
```

Important note:
- `run_fundamental_lifecycle.py` and `run_fundamental_score.py` are not market-filtered CLIs
- if your DB contains multiple markets, those commands operate on all eligible rows unless you use more targeted flows

### 8.3 OMXH market to percentile score

For OMXH, the current batch path already goes through score:

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_yahoo_batch_fin.py \
  --db /home/kalle/projects/swingmaster/fundamentals_fin.db \
  --osakedata-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --run-id OMXH_BATCH_2026Q2 \
  --replace-symbol
```

Then compute percentile rows:

```bash
PYTHONPATH=. python3 swingmaster/cli/run_fundamental_score_percentile.py \
  --db /home/kalle/projects/swingmaster/fundamentals_fin.db \
  --osakedata-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --market omxh \
  --as-of-date 2026-05-08 \
  --run-id OMXH_PCT_2026-05-08
```

## 9. Short memory version

If you want the shortest operational memory list:

1. Raw facts or raw statements in
2. Quarterly normalized rows build
3. TTM rows build
4. Lifecycle classification
5. Fundamental score
6. Percentile score in a separate market/date step
7. Ticker snapshot reads those stored rows and optionally appends raw technical sections

