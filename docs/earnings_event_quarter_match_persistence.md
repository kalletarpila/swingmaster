# Earnings Event Quarter Match Persistence

Date: 2026-07-31

## Product Policy

SwingMaster persists a lightweight retrospective research-data association between quarterly fundamentals periods and Yahoo earnings announcement events.

The active fundamentals table remains:

```text
rc_fundamental_quarterly
```

The announcement effective trading date is treated as the assumed market-availability date for the quarter's basic fundamentals in retrospective research. SwingMaster ingestion timestamps are operational metadata only and do not determine historical market availability.

This is intentionally not field-level PIT reconstruction. Vintage and field-provenance writes remain disabled, and this table does not depend on vintage/provenance data.

## Table

Migration:

```text
030_rc_fundamental_quarter_earnings_match.sql
```

Table:

```text
rc_fundamental_quarter_earnings_match
```

Columns:

- `id`
- `market`
- `ticker`
- `period_end_date`
- `earnings_event_id`
- `announcement_at`
- `announcement_date`
- `announcement_session`
- `effective_trading_date`
- `effective_date_status`
- `reporting_delay_days`
- `matching_status`
- `matching_confidence`
- `matching_method`
- `candidate_count`
- `availability_policy`
- `matcher_version`
- `created_at_utc`
- `updated_at_utc`

Natural uniqueness:

```text
(market, ticker, period_end_date)
(market, ticker, earnings_event_id)
```

Indexes support ticker/period lookup, effective trading-date scans, earnings-event joins, and status/confidence filtering.

## Match Policy

Source tables:

```text
rc_fundamental_quarterly
rc_earnings_event
```

Persisted statuses:

```text
MATCHED_HIGH_CONFIDENCE
MATCHED_MEDIUM_CONFIDENCE
MATCHED_LOW_CONFIDENCE
```

Unmatched and ambiguous statuses remain in dry-run/audit artifacts only and are not stored as positive rows.

Default production apply includes low-confidence matches because they come from the deterministic matcher. The rebuild CLI also supports `--exclude-low-confidence` for diagnostics.

Policy markers:

```text
availability_policy = EARNINGS_EFFECTIVE_DATE_ASSUMED
matcher_version = earnings_event_quarter_match_v1
matching_method = SEQUENTIAL_NEXT_REPORTED_EVENT_V1
```

## Effective Trading Date

Session rules:

- `BEFORE_MARKET`: same USA trading day
- `DURING_MARKET`: same USA trading day
- `AFTER_MARKET`: next valid USA trading day
- `UNKNOWN`: `NULL`

Exactly `16:00` remains `DURING_MARKET` based on the Yahoo event parser.

The resolver uses a deterministic USA/NYSE-style holiday calendar, including New Year, MLK Day, Presidents Day, Good Friday, Memorial Day, Juneteenth from 2022, Independence Day, Labor Day, Thanksgiving, and Christmas. If a same-day session falls on a non-trading date, no date is guessed; `effective_trading_date` remains `NULL` and `effective_date_status` is `NO_TRADING_CALENDAR_DATE`.

## Rebuild CLI

CLI:

```bash
python -m swingmaster.cli.rebuild_earnings_event_matches
```

Important arguments:

- `--fundamentals-db`
- `--max-delay-days`
- `--include-low-confidence`
- `--exclude-low-confidence`
- `--dry-run`
- `--apply`
- `--backup`
- `--checkpoint-json`
- `--summary-json`
- `--output-csv`
- `--json`

Default mode is dry-run. Apply requires one verified SQLite backup under repository `temp/`. All runtime artifacts are validated to stay under:

```text
temp/earnings_event_match_persistence/
```

The apply path builds the full desired materialized set, validates uniqueness and invariants, diffs against existing rows, and applies inserts/updates/deletes inside one transaction. Existing source rows are not changed.

## Real DB Results

Database:

```text
fundamentals_usa.db
```

Dry-run artifact:

```text
temp/earnings_event_match_persistence/20260731T144136Z/dry_run/summary.json
```

Dry-run counts:

- quarterly periods: `155571`
- earnings events: `135055`
- matched high: `122757`
- matched medium: `2771`
- matched low: `26`
- persisted candidate matches: `125554`
- unmatched: `24412`
- ambiguous: `5605`

These match the prior investigation exactly.

Production apply artifact:

```text
temp/earnings_event_match_persistence/20260731T144157Z/apply/summary.json
```

Backup:

```text
temp/earnings_event_match_persistence/20260731T144157Z/backups/fundamentals_usa.db.20260731T144157Z.bak
```

Backup verification:

- `PRAGMA quick_check`: `ok`
- file size: `6619226112`
- quarterly rows: `155571`
- earnings-event rows: `135055`
- pre-existing match rows: `0`
- duplicate match period keys: `0`
- duplicate match event keys: `0`

Apply counts:

- inserted: `125554`
- updated: `0`
- deleted obsolete: `0`
- unchanged: `0`
- final table rows: `125554`
- transaction status: `COMMITTED`

After tightening the non-trading-date policy for same-day sessions, the same guarded rebuild updated `101` effective-date rows to `NULL`/`NO_TRADING_CALENDAR_DATE` rather than guessing a next trading day.

Verification:

- duplicate period keys: `0`
- duplicate event keys: `0`
- unmatched/ambiguous rows persisted: `0`
- bad availability policy rows: `0`
- bad matcher version rows: `0`
- bad reporting delay rows: `0`
- announcement not after period rows: `0`
- effective-date mismatch rows: `0`

Final effective-date counts:

- resolved effective dates: `125453`
- unknown/no-calendar effective dates: `101`

Final idempotency artifact:

```text
temp/earnings_event_match_persistence/20260731T144157Z/apply/idempotency_final.json
```

Second rebuild result:

- inserted: `0`
- updated: `0`
- deleted obsolete: `0`
- unchanged: `125554`
- content hash unchanged: `true`

## Representative Tickers

Persisted match rows:

| Ticker | Rows | First Period | Last Period | Low Confidence | Null Effective Dates |
| --- | ---: | --- | --- | ---: | ---: |
| AAPL | 72 | 2008-06-28 | 2026-03-28 | 0 | 0 |
| ARWR | 54 | 2010-12-31 | 2026-03-31 | 0 | 0 |
| BBY | 72 | 2008-08-30 | 2026-05-02 | 0 | 0 |
| DGXX | 3 | 2024-12-31 | 2026-03-31 | 0 | 0 |
| GIS | 73 | 2008-05-25 | 2026-05-31 | 0 | 0 |
| JPM | 72 | 2008-06-30 | 2026-03-31 | 0 | 0 |
| LMT | 73 | 2008-06-29 | 2026-06-28 | 0 | 0 |
| MSFT | 76 | 2007-06-30 | 2026-03-31 | 0 | 0 |
| NVDA | 63 | 2008-10-26 | 2026-04-26 | 0 | 0 |
| XOM | 72 | 2008-06-30 | 2026-03-31 | 0 | 0 |

`AVNS` has no persisted match rows in the representative check.

## Rollback

Restore the verified backup above to return the real database to its pre-migration/pre-materialization state. Code rollback is a normal revert of the implementation commit.

## Limitations

- This table assumes basic quarterly fundamentals became market-available on the earnings announcement effective trading date.
- It does not prove every detailed field was present in the original announcement.
- It does not reconstruct field-level first-seen timestamps.
- The USA trading calendar is deterministic and holiday-aware for normal NYSE holidays, but it does not model extraordinary closures.
- Matching quality remains bounded by Yahoo historical event availability and the deterministic sequence matcher.

## Downstream Use

Downstream retrospective research can join `rc_fundamental_quarterly` to `rc_fundamental_quarter_earnings_match` by `(market, ticker, period_end_date)` and use `effective_trading_date` as the assumed basic-fundamentals availability date.
