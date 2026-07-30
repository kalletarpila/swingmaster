# Yahoo Earnings Full Universe Coverage Audit

## Scope

This report documents the read-only Yahoo earnings-date coverage audit run on July 30, 2026 for the canonical USA fundamentals universe in `fundamentals_usa.db`.

No earnings events, fundamentals rows, migrations, schedulers, refresh jobs, backfills, or database tables were written by this audit.

## Environment

- Repository: `/home/kalle/projects/swingmaster`
- SwingMaster Python executable: `/home/kalle/projects/swingmaster/.venv/bin/python`
- Python version: `3.10.12`
- yfinance installed version: `1.5.2`
- Newest stable yfinance available from pip: `1.5.2`
- Dependency declaration inspected: `pyproject.toml`
- yfinance dependency declaration: unpinned `yfinance`

Because the installed yfinance version already matched the newest pip stable release, no package upgrade and no dependency-file change were needed.

## Initial Database State

- `PRAGMA quick_check`: `ok`
- `rc_earnings_event` rows: `383`
- `rc_earnings_event` tickers: `5`
- Event tickers and counts: `AAPL=80`, `JPM=76`, `MSFT=78`, `NVDA=70`, `XOM=79`
- Non-pilot event rows: `0`
- Duplicate event natural keys: `0`
- Canonical USA fundamentals universe: `2936` distinct tickers
- Deterministic first/last ticker: `A` / `ZYME`
- Fundamentals period coverage: `1980-07-21` through `2026-07-04`

## Smoke Test

The live AAPL `get_earnings_dates(limit=12)` call completed without exception and returned a non-empty pandas dataframe.

- Type: `pandas.core.frame.DataFrame`
- Rows returned: `25`
- Columns: `EPS Estimate`, `Reported EPS`, `Surprise(%)`
- Index: `DatetimeIndex`, name `Earnings Date`, dtype `datetime64[ns, America/New_York]`
- Timezone: `America/New_York`
- Date coverage: `2020-07-30 16:00:00-04:00` through `2026-07-30 16:00:00-04:00`
- Reported EPS and estimated EPS fields were present.

The live AAPL `get_earnings_dates(limit=40)` source-capability call returned `50` rows.

- Oldest returned index: `2014-04-23 16:00:00-04:00`
- Newest returned index: `2026-07-30 16:00:00-04:00`
- Columns: `EPS Estimate`, `Reported EPS`, `Surprise(%)`
- Index timezone: `America/New_York`

Yahoo appears technically capable of supplying AAPL historical earnings dates back to 2020 when enough rows are requested.

## Preflight And Resume

Artifacts were written under:

```text
temp/yahoo_earnings_coverage_audit/20260730T161944Z/
```

The 20-ticker preflight completed with:

- processed: `20`
- source successful: `20`
- actual history available: `20`
- source failures: `0`
- parse failures: `0`

Resume behavior was verified with a copied temp artifact where one row was marked failed. On resume, that failed ticker was retried live and the other 19 successful tickers were skipped from the previous artifact.

## Full Audit Run

The full audit was run sequentially with:

- inter-ticker jitter: `0.8` to `1.4` seconds
- average target delay: about `1` second
- max retries: `2`, meaning `3` total attempts per ticker
- rate-limit backoff sequence: `30`, `60`, `120` seconds
- progress interval: every `50` tickers
- atomic checkpoint writes after each ticker

Output artifacts:

- `temp/yahoo_earnings_coverage_audit/20260730T161944Z/full.json`
- `temp/yahoo_earnings_coverage_audit/20260730T161944Z/full.csv`
- `temp/yahoo_earnings_coverage_audit/20260730T161944Z/summary.json`
- `temp/yahoo_earnings_coverage_audit/20260730T161944Z/full_progress.log`

Final aggregate result:

- processed tickers: `2936`
- source-successful tickers: `2930`
- source failures: `6`
- parse failures: `0`
- tickers with actual historical rows: `2802`
- tickers with no actual historical rows: `134`
- no-Yahoo-row tickers: `128`
- capped tickers: `21`
- total completed events available: `134732`
- completed events min/median/max: `0` / `47.0` / `93`
- oldest Yahoo completed announcement date across successful tickers: `2000-05-05`
- newest Yahoo completed announcement date across successful tickers: `2026-07-30`

Planning classifications:

- `BACKFILL_READY_PARTIAL_MARGIN_ONLY`: `1234`
- `BACKFILL_PARTIAL_ACTUAL_HISTORY`: `1568`
- `BACKFILL_NO_YAHOO_ROWS`: `128`
- `BACKFILL_SOURCE_FAILED`: `6`
- `BACKFILL_READY_FULL_HISTORY`: `0`
- `BACKFILL_PARSE_FAILED`: `0`

Coverage percentages:

- covers oldest fundamentals period: `42.03%`
- covers fetch lower bound: `0.0%`
- no Yahoo rows: `4.36%`
- source failed: `0.2%`
- parse failed: `0.0%`
- capped by request limit: `0.72%`

## Failures

Six tickers failed after three total attempts:

| Ticker | Source status | Error type | Attempts | Error message |
| --- | --- | --- | ---: | --- |
| BE | `SOURCE_FAILED` | `YAHOO_SOURCE_ERROR` | 3 | `['Earnings Date']` |
| BOOM | `SOURCE_FAILED` | `YAHOO_SOURCE_ERROR` | 3 | `['Earnings Date']` |
| CYCN | `SOURCE_FAILED` | `YAHOO_SOURCE_ERROR` | 3 | `['Earnings Date']` |
| ITRI | `SOURCE_FAILED` | `YAHOO_SOURCE_ERROR` | 3 | `['Earnings Date']` |
| PPL | `SOURCE_FAILED` | `YAHOO_SOURCE_ERROR` | 3 | `['Earnings Date']` |
| SNBR | `SOURCE_FAILED` | `YAHOO_SOURCE_ERROR` | 3 | `['Earnings Date']` |

There were no parse failures recorded by the audit.

## Cap Analysis

The yfinance request limit is capped at `100` by the repository guard. Twenty-one tickers hit that cap:

```text
ARWR, BGMS, CHEF, CLRB, CLSK, CORT, CPRX, CRDF, CYTK, DCTH, FOLD, GCTK, HROW, IRD, LITS, MNKD, SRPT, SVRA, TGTX, VTGN, WKSP
```

For capped tickers:

- maximum uncovered actual days: `10304`
- median uncovered actual days: `449.5`
- maximum uncovered actual quarters: `112`
- oldest capped fetch lower bound: `1980-03-23`

The capped set needs separate planning before any future full-history backfill because the current guarded yfinance limit can materially truncate older history.

## Anomalies

Aggregate anomaly flags:

- `ACTUAL_HISTORY_GAP`: `1568`
- `CAPPED_LIMIT`: `21`
- `DUPLICATE_YAHOO_TIMESTAMPS`: `37`
- `LOW_COMPLETED_ROWS_FOR_LONG_HISTORY`: `144`
- `NO_YAHOO_ROWS`: `128`
- `SOURCE_FAILED`: `6`

The duplicate timestamp flag comes from duplicate Yahoo rows dropped during normalization; it does not indicate duplicate rows in `rc_earnings_event`.

## Post-Run Database State

- `PRAGMA quick_check`: `ok`
- `rc_earnings_event` rows: `383`
- `rc_earnings_event` tickers: `5`
- Event tickers and counts: `AAPL=80`, `JPM=76`, `MSFT=78`, `NVDA=70`, `XOM=79`
- Non-pilot event rows: `0`
- Duplicate event natural keys: `0`

The before/after logical checks match, so the audit did not change earnings-event database content.

## Backup Policy For Later Backfill

This phase did not run a backfill and did not create any new database backup.

For a later full backfill phase, use the existing guarded apply pattern:

- require explicit operator approval for real database writes;
- create a timestamped SQLite backup under `temp/` before opening a write transaction;
- verify the backup exists and has nonzero size;
- run `PRAGMA integrity_check` or `PRAGMA quick_check` against the backup;
- apply in bounded batches with post-batch verification;
- preserve the backup and all generated logs/artifacts outside Git staging;
- restore from the verified SQLite backup if rollback is required.
