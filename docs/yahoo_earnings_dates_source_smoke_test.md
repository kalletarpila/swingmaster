# Yahoo Earnings Dates Source Smoke Test

Test date: 2026-07-30T14:15:45Z

## Environment

- Python executable: `/home/kalle/projects/swingmaster/.venv/bin/python`
- Python version: 3.10.12
- pandas: 2.3.3
- yfinance: 1.5.2
- lxml: 6.1.1

Repository note: `pyproject.toml` declares `requires-python = ">=3.12"`, while the current SwingMaster `.venv` uses Python 3.10.12. This smoke test does not resolve that mismatch.

## Yahoo Method

The test used `yfinance.Ticker(<ticker>).get_earnings_dates(limit=...)`.

No returned data was written to a database or file.

## Observed Dataframe Shape

For the tested tickers, yfinance returned a `pandas.core.frame.DataFrame` with:

- Columns: `['EPS Estimate', 'Reported EPS', 'Surprise(%)']`
- Index type: `pandas.core.indexes.datetimes.DatetimeIndex`
- Index name: `Earnings Date`
- Index timezone: `America/New_York`

The returned index contains timezone-aware earnings timestamps. Observed timestamps included both after-market times such as `16:00:00-04:00` / `16:00:00-05:00` and pre-market times such as `06:00:00-04:00` or `08:00:00-04:00`, depending on ticker and event.

`Reported EPS` and `EPS Estimate` were present for all tested tickers. Future events may have `NaN` values for reported EPS and surprise.

## AAPL Checks

`AAPL` with `limit=12` returned 25 rows, not 12. The dataframe was non-empty.

- Oldest returned date: `2020-07-30 16:00:00-04:00`
- Newest returned date: `2026-07-30 16:00:00-04:00`
- Columns: `['EPS Estimate', 'Reported EPS', 'Surprise(%)']`
- Index type: `pandas.core.indexes.datetimes.DatetimeIndex`
- Index name: `Earnings Date`
- Index timezone: `America/New_York`

`AAPL` with `limit=40` returned 50 rows.

- Oldest returned date: `2014-04-23 16:00:00-04:00`
- Newest returned date: `2026-07-30 16:00:00-04:00`
- Rows dated 2020 or later: 27
- Oldest 2020-or-later date: `2020-01-28 16:00:00-05:00`

## Six-Ticker Sample

Each ticker was tested independently with `limit=40`. All succeeded on the first attempt.

| Ticker | Rows | Oldest returned | Newest returned | Rows from 2020 | Columns | Index timezone | Reported EPS | EPS Estimate |
| --- | ---: | --- | --- | ---: | --- | --- | --- | --- |
| AAPL | 50 | `2014-04-23 16:00:00-04:00` | `2026-07-30 16:00:00-04:00` | 27 | `['EPS Estimate', 'Reported EPS', 'Surprise(%)']` | `America/New_York` | yes | yes |
| MSFT | 50 | `2014-07-22 16:00:00-04:00` | `2026-10-28 16:00:00-04:00` | 28 | `['EPS Estimate', 'Reported EPS', 'Surprise(%)']` | `America/New_York` | yes | yes |
| AMZN | 50 | `2014-04-24 16:00:00-04:00` | `2026-07-30 16:00:00-04:00` | 27 | `['EPS Estimate', 'Reported EPS', 'Surprise(%)']` | `America/New_York` | yes | yes |
| NVDA | 50 | `2014-05-08 16:00:00-04:00` | `2026-08-26 16:00:00-04:00` | 27 | `['EPS Estimate', 'Reported EPS', 'Surprise(%)']` | `America/New_York` | yes | yes |
| JPM | 50 | `2014-07-15 06:00:00-04:00` | `2026-10-13 08:00:00-04:00` | 28 | `['EPS Estimate', 'Reported EPS', 'Surprise(%)']` | `America/New_York` | yes | yes |
| XOM | 50 | `2014-05-01 08:00:00-04:00` | `2026-07-31 08:00:00-04:00` | 27 | `['EPS Estimate', 'Reported EPS', 'Surprise(%)']` | `America/New_York` | yes | yes |

## Coverage Assessment

For the tested established U.S. companies, Yahoo via yfinance appears usable for historical earnings dates back to at least 2020. The observed `limit=40` calls returned 50 rows for every tested ticker, with oldest dates in 2014 and at least 27 rows dated 2020 or later.

This is observed Yahoo/yfinance behavior at test time, not a guaranteed service contract. Yahoo may change page structure, coverage, rate limits, timestamp conventions, or availability without notice.

## Gaps And Failures

No ticker-specific failures, rate-limit responses, or parsing failures occurred after installing `lxml`.
