# Yahoo Earnings Event Source And Schema

## Purpose

`rc_earnings_event` is the canonical table for quarterly earnings announcement events observed from external sources. It is separate from quarterly financial statement rows because Yahoo earnings-date output describes announcement timestamps and EPS fields, not a trustworthy fiscal `period_end_date`. Matching an announcement to an exact fiscal quarter belongs in a later phase.

This phase adds the schema, read-only planning, Yahoo source parsing, diagnostics, and tests. It does not write earnings events to the real `fundamentals_usa.db`.

## Source Method

The source call is:

```python
yfinance.Ticker(ticker).get_earnings_dates(limit=...)
```

Observed yfinance 1.5.2 dataframe schema:

- Columns: `EPS Estimate`, `Reported EPS`, `Surprise(%)`
- Index type: `pandas.core.indexes.datetimes.DatetimeIndex`
- Index name: `Earnings Date`
- Index timezone: `America/New_York`

The parser requires `Reported EPS` and `EPS Estimate` by column name. `Surprise(%)` is optional and is normalized to `None` when absent. Missing required columns are a parse failure and include actual columns in diagnostics.

Yahoo may return more rows than requested. Future estimated rows may be present. This behavior is observed source behavior, not a guaranteed service contract.

## Completed Events

Historical completed events are defined only by non-null `Reported EPS`. A row is not treated as completed merely because its timestamp is in the past. Future or otherwise unreported rows can be included in diagnostics with `--include-future`, but they remain `is_reported = false`.

All pandas and NumPy nulls are normalized to Python `None`.

## Timestamp Policy

Yahoo timestamps are preserved as timezone-aware `America/New_York` timestamps in `announcement_at`. `announcement_date` is derived from the New York-local calendar date, not from UTC conversion.

Session classification is deterministic:

- before `09:30`: `BEFORE_MARKET`
- from `09:30` through `16:00`, inclusive: `DURING_MARKET`
- after `16:00`: `AFTER_MARKET`
- missing or unusable time: `UNKNOWN`

Exactly `16:00` is classified as `DURING_MARKET`. This is a deterministic boundary, not proof that the release occurred before the regular close. Yahoo commonly reports standardized timestamps, so SwingMaster must not infer greater precision than the source provides.

## Table Schema

Migration `029_rc_earnings_event.sql` creates:

```text
rc_earnings_event
```

Columns:

- `id`
- `market`
- `ticker`
- `announcement_at`
- `announcement_date`
- `announcement_session`
- `is_reported`
- `reported_eps`
- `estimated_eps`
- `surprise_pct`
- `source`
- `source_observed_at_utc`
- `source_timezone`
- `created_at_utc`
- `updated_at_utc`

The natural uniqueness key is:

```text
(market, ticker, announcement_at, source)
```

This prevents duplicate Yahoo events for the same ticker and exact source timestamp while allowing another source to report its own event observation independently.

Indexes support ticker/date lookup, announcement-date scans, reported-event scans, and source filtering.

`period_end_date` is intentionally absent in this phase. Yahoo earnings-date output is not sufficient to populate it safely.

## History Range Planning

The production/default lower bound is ticker-specific and comes from current quarterly fundamentals already present in `fundamentals_usa.db`.

Canonical source selection:

```text
rc_fundamental_quarterly.period_end_date
```

`rc_fundamental_quarterly` is used because it is the current canonical one-row-per-`(ticker, period_end_date)` quarterly table. The vintage table can contain multiple statement vintages per period, which would be the wrong source for a simple oldest-history requirement.

The rule is:

```text
oldest_required_period_end_date =
    MIN(date(period_end_date))
    from rc_fundamental_quarterly
    for the normalized ticker
```

Then:

```text
fetch_lower_bound =
    oldest_required_period_end_date - safety_margin_days
```

The default `safety_margin_days` is `120`. This avoids missing an announcement near the beginning of the available fundamentals history and leaves room for irregular fiscal calendars and later matching logic.

There is no fixed `2020-01-01` production lower bound. A diagnostic `--start-date` override exists for tests and manual inspection, and output explicitly marks the range as overridden.

## Dynamic Limit Planning

The default Yahoo request limit is derived from the ticker-specific lower bound:

```text
estimated_quarters = ceil(number_of_days / 365.25 * 4)
requested_limit = estimated_quarters + buffer_events
```

The default buffer is `8` events.

The installed yfinance 1.5.2 implementation was inspected. `TickerBase.get_earnings_dates()` raises above `100`, and `_get_earnings_dates_using_scrape()` documents and enforces `limit <= 100`. SwingMaster therefore caps the requested limit at `100` and reports whether the calculated request was capped.

The cap is a yfinance implementation constraint observed in the installed version. It is not a Yahoo service guarantee.

## Coverage Statuses

Coverage assessment compares completed reported Yahoo events against both:

- `oldest_required_period_end_date`
- the wider `fetch_lower_bound`

Statuses:

- `COVERAGE_OK`: oldest completed returned announcement date reaches or predates `fetch_lower_bound`
- `COVERAGE_PARTIAL`: rows exist but do not reach the safety-margin lower bound, including the case where they reach only the oldest fundamentals period end
- `NO_FUNDAMENTALS_HISTORY`: no qualifying fundamentals rows exist for the ticker
- `NO_YAHOO_ROWS`: Yahoo returned no completed qualifying rows after filtering
- `SOURCE_FAILED`: yfinance/source call failed
- `PARSE_FAILED`: dataframe parsing failed

Diagnostics report both `covers_oldest_fundamentals_period` and `covers_fetch_lower_bound`.

## CLI

The read-only CLI is:

```bash
python -m swingmaster.cli.inspect_yahoo_earnings_dates --ticker AAPL
```

Important arguments:

- `--fundamentals-db`
- `--start-date`
- `--safety-margin-days`
- `--limit`
- `--include-future`
- `--json`

When `--start-date` is absent, the CLI derives the range from `rc_fundamental_quarterly` in the fundamentals database. When it is present, the CLI treats it as an explicit diagnostic lower-bound override. The CLI performs no database writes.

## Source Limitations

Yahoo can change page structure, row availability, timestamp conventions, EPS fields, rate limits, and blocking behavior without notice. Future Yahoo rows are estimates and are not treated as confirmed history.

The repository still has an unresolved Python mismatch: `pyproject.toml` declares `requires-python = ">=3.12"`, while the current `.venv` uses Python 3.10.12. This phase does not fix it.

## Next Phase

Later work should add:

- guarded write/apply path for `rc_earnings_event`
- full ticker-universe backfill
- coverage audit across the universe
- later matching from announcement events to quarterly fundamentals rows
