# Yahoo Earnings Event Apply And Coverage Audit

## Scope

This phase adds a guarded write path for normalized Yahoo earnings announcement events and a read-only coverage audit for the U.S. fundamentals universe.

It does not run a production backfill and does not write earnings events to the real `fundamentals_usa.db`.

## Repository Upsert Semantics

The persistence layer writes to:

```text
rc_earnings_event
```

The natural key is immutable:

```text
(market, ticker, announcement_at, source)
```

For a missing natural key, the repository inserts a new row. For an existing natural key, it compares mutable source fields and updates only when at least one persisted value differs. Existing rows are never deleted simply because Yahoo does not return them in a later fetch.

Mutable fields:

- `announcement_date`
- `announcement_session`
- `is_reported`
- `reported_eps`
- `estimated_eps`
- `surprise_pct`
- `source_observed_at_utc`
- `source_timezone`

Immutable identity fields:

- `market`
- `ticker`
- `announcement_at`
- `source`

`created_at_utc` is preserved on update. `updated_at_utc` changes only on insert or material update.

## Transaction Boundary

Single-ticker apply uses one explicit SQLite transaction for the ticker. If any record in that ticker apply fails validation or persistence, the full ticker transaction is rolled back.

The summary reports:

- fetched record count
- eligible completed records
- inserted, updated, unchanged, skipped, and duplicate counts
- transaction status
- source observation timestamp

Future or otherwise unreported Yahoo rows are skipped by default and are not persisted in this phase.

## Apply CLI

CLI:

```bash
python -m swingmaster.cli.apply_yahoo_earnings_events --ticker AAPL
```

Important arguments:

- `--ticker`
- `--fundamentals-db`
- `--start-date`
- `--safety-margin-days`
- `--limit`
- `--include-future`
- `--dry-run`
- `--apply`
- `--json`
- `--backup`

Default mode is dry-run. This follows the repository pattern of supporting dry-run safety while requiring explicit write intent for guarded operations. Writes are allowed only with `--apply`. Supplying both `--dry-run` and `--apply` is rejected.

Dry-run opens the database read-only, verifies that `rc_earnings_event` exists, derives the ticker-specific range, fetches and normalizes Yahoo rows, and calculates insert/update/unchanged/skipped counts without writing.

Apply mode creates a SQLite backup before opening the write transaction. By default the backup is written next to the database using a timestamped `.bak` filename. `--backup` can provide a target file or directory. Apply returns nonzero on backup, source, parse, transaction, or verification failure.

## Universe Coverage Audit

CLI:

```bash
python -m swingmaster.cli.audit_yahoo_earnings_coverage
```

The default universe is:

```text
DISTINCT UPPER(ticker)
FROM rc_fundamental_quarterly
WHERE date(period_end_date) IS NOT NULL
```

The audit never writes to the database. It derives, per ticker:

- oldest fundamentals period end
- 120-day safety-margin lower bound
- dynamic Yahoo request limit
- coverage status

Per-ticker output includes:

- `ticker`
- `fundamentals_row_count`
- `oldest_required_period_end_date`
- `fetch_lower_bound`
- `calculated_limit`
- `requested_limit`
- `limit_was_capped`
- `raw_yahoo_row_count`
- `completed_qualifying_count`
- `oldest_completed_announcement_date`
- `newest_completed_announcement_date`
- `covers_oldest_fundamentals_period`
- `covers_fetch_lower_bound`
- `coverage_status`
- `source_status`
- `error_type`
- `error_message`
- `elapsed_seconds`

Aggregate output includes counts for coverage statuses, capped limits, source/parse failures, and oldest observed dates across the audited universe.

The main business metric is `covers_oldest_fundamentals_period`. A ticker can be `COVERAGE_PARTIAL` because it does not cover the wider 120-day safety margin while still covering the actual oldest fundamentals period. These booleans are reported separately.

## Retry And Throttle Policy

Yahoo requests are sequential. There are no parallel Yahoo calls.

Defaults:

- `--sleep-seconds 0.5`
- `--max-retries 1`

Retries are bounded and use a simple increasing backoff based on `sleep_seconds`. Rate-limit and HTTP/network-like failures are classified from available exception text when possible. The implementation does not modify yfinance internals and does not cache Yahoo responses in SQLite.

## Resume Artifacts

JSON artifacts use:

```text
artifact_schema_version = 1
```

`--resume-from-json` loads a prior artifact, verifies the schema version and database path identity, preserves deterministic ticker order, skips previously successful tickers, and retries failed tickers.

Artifacts can be written as JSON and CSV using:

```bash
--output-json path
--output-csv path
```

`--no-network` performs planning-only audit output without Yahoo calls.

## WAL/SHM Investigation

The real `fundamentals_usa.db-shm` and `fundamentals_usa.db-wal` files were already present at the start of this phase. They were not deleted.

A focused temporary-database check showed that opening a WAL-mode SQLite database with `mode=ro` can still involve WAL/SHM sidecar files when they already exist. Trying `immutable=1` against a WAL database with uncheckpointed state failed with a malformed-database error in the local check. Because `immutable=1` can ignore or mishandle WAL state unless the database is known to be immutable and checkpointed, this phase keeps the existing read-only URI strategy and uses `PRAGMA query_only=ON`.

Tests verify that read-only planning/audit paths do not change table row counts or apply migrations.

## Next Phase

Recommended next steps:

- apply migration 029 to a controlled production copy
- run a small pilot apply against a copied or explicitly approved database
- run a full-universe guarded backfill
- verify post-backfill coverage and row counts
- implement later fiscal-quarter matching
