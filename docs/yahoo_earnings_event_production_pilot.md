# Yahoo Earnings Event Production Pilot

Execution date: 2026-07-30 UTC

Environment:

- Repository: `/home/kalle/projects/swingmaster`
- Database: `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- Python executable: `/home/kalle/projects/swingmaster/.venv/bin/python`
- Python version: 3.10.12

The repository still declares `requires-python = ">=3.12"`. This pilot did not address that mismatch.

## Pre-Migration State

Initial real database state:

- Size: `6571732992` bytes
- Journal mode: `wal`
- Page count: `1604427`
- Freelist count: `0`
- `rc_fundamental_quarterly` rows: `155571`
- Distinct normalized tickers with valid period end dates: `2936`
- `rc_fundamental_schema_version` rows: `1`
- Recorded schema version: `1`, applied at `2026-04-25 08:31:33`
- `rc_earnings_event`: absent
- `PRAGMA quick_check`: `ok`

The migration system does not record one row per SQL migration file. It records the fundamentals schema version and uses idempotent SQL migrations. Migration 029 was therefore verified by table/schema presence rather than a per-migration ledger row.

Full `integrity_check` was not run because `quick_check` on the 6.57 GB database took about 103 seconds. `quick_check` passed before and after migration.

## Pre-Migration Backup

Backup path:

```text
/home/kalle/projects/swingmaster/snapshots/yahoo_earnings_event_pilot/fundamentals_usa.pre_migration_029_earnings_event.20260730T154752Z.db
```

Backup verification:

- Backup size: `6571732992` bytes
- `PRAGMA quick_check`: `ok`
- `rc_fundamental_quarterly` rows: `155571`
- Distinct normalized tickers: `2936`
- `rc_earnings_event`: absent
- Schema version row: version `1`, applied at `2026-04-25 08:31:33`

The backup was created with SQLite's backup API before migration 029 was applied.

## Migration 029

Command:

```bash
.venv/bin/python -m swingmaster.cli.run_fundamental_migrations --db fundamentals_usa.db
```

Result:

- Migration runner status: `ok`
- `tables_created`: `17`
- `schema_version`: `1`
- `rc_earnings_event`: present
- Expected columns: present
- Natural uniqueness constraint: present as SQLite autoindex for `(market, ticker, announcement_at, source)`
- Expected indexes: present
- Initial `rc_earnings_event` rows: `0`
- Post-migration `rc_fundamental_quarterly` rows: `155571`
- Post-migration distinct normalized tickers: `2936`
- Post-migration `PRAGMA quick_check`: `ok`

The migration was additive for the checked existing fundamentals tables.

## Pilot Tickers

Pilot set:

```text
AAPL
MSFT
JPM
XOM
NVDA
```

All five were present in `rc_fundamental_quarterly`:

| Ticker | Fundamentals rows | Oldest period end | Newest period end |
| --- | ---: | --- | --- |
| AAPL | 74 | `2006-09-30` | `2026-03-28` |
| MSFT | 76 | `2007-06-30` | `2026-03-31` |
| JPM | 73 | `2007-12-31` | `2026-03-31` |
| XOM | 74 | `2006-12-31` | `2026-03-31` |
| NVDA | 77 | `2007-01-28` | `2026-04-26` |

## Dry-Run Results

All dry-runs succeeded and wrote zero rows. `rc_earnings_event` remained at `0` rows after all dry-runs.

| Ticker | Oldest required period end | Fetch lower bound | Requested limit | Raw Yahoo rows | Eligible completed | Would insert | Would update | Would unchanged | Oldest completed | Newest completed | Covers oldest fundamentals | Covers safety lower bound | Coverage |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- |
| AAPL | `2006-09-30` | `2006-06-02` | 89 | 100 | 80 | 80 | 0 | 0 | `2006-07-19` | `2026-04-30` | yes | no | `COVERAGE_PARTIAL` |
| MSFT | `2007-06-30` | `2007-03-02` | 86 | 100 | 78 | 78 | 0 | 0 | `2007-04-26` | `2026-07-29` | yes | no | `COVERAGE_PARTIAL` |
| JPM | `2007-12-31` | `2007-09-02` | 84 | 100 | 76 | 76 | 0 | 0 | `2007-10-17` | `2026-07-14` | yes | no | `COVERAGE_PARTIAL` |
| XOM | `2006-12-31` | `2006-09-02` | 88 | 100 | 79 | 79 | 0 | 0 | `2006-10-26` | `2026-05-01` | yes | no | `COVERAGE_PARTIAL` |
| NVDA | `2007-01-28` | `2006-09-30` | 88 | 100 | 70 | 70 | 0 | 0 | `2006-11-09` | `2026-05-20` | yes | no | `COVERAGE_PARTIAL` |

For all pilot tickers, Yahoo covered actual available fundamentals history. It did not cover the optional 120-day safety margin.

## First Apply Results

Each ticker was applied with:

```bash
.venv/bin/python -m swingmaster.cli.apply_yahoo_earnings_events --ticker <TICKER> --apply --json
```

The CLI created one timestamped backup per ticker and preserved all backups.

| Ticker | Fetched | Eligible | Inserted | Updated | Unchanged | Skipped | Duplicates | Transaction | Backup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| AAPL | 80 | 80 | 80 | 0 | 0 | 0 | 0 | `COMMITTED` | `fundamentals_usa.db.20260730T154955Z.bak` |
| MSFT | 78 | 78 | 78 | 0 | 0 | 0 | 0 | `COMMITTED` | `fundamentals_usa.db.20260730T155012Z.bak` |
| JPM | 76 | 76 | 76 | 0 | 0 | 0 | 0 | `COMMITTED` | `fundamentals_usa.db.20260730T155029Z.bak` |
| XOM | 79 | 79 | 79 | 0 | 0 | 0 | 0 | `COMMITTED` | `fundamentals_usa.db.20260730T155046Z.bak` |
| NVDA | 70 | 70 | 70 | 0 | 0 | 0 | 0 | `COMMITTED` | `fundamentals_usa.db.20260730T155105Z.bak` |

## Persisted Rows

Persisted row counts:

| Ticker | Rows | Oldest announcement | Newest announcement |
| --- | ---: | --- | --- |
| AAPL | 80 | `2006-07-19` | `2026-04-30` |
| MSFT | 78 | `2007-04-26` | `2026-07-29` |
| JPM | 76 | `2007-10-17` | `2026-07-14` |
| XOM | 79 | `2006-10-26` | `2026-05-01` |
| NVDA | 70 | `2006-11-09` | `2026-05-20` |

Total persisted rows: `383`.

Invariant checks:

- `market = 'usa'`: all rows
- `source = 'YAHOO_FINANCE'`: all rows
- `source_timezone = 'America/New_York'`: all rows
- `is_reported = 1`: all rows
- `reported_eps IS NOT NULL`: all rows
- unreported/future rows persisted: `0`
- duplicate natural key groups: `0`
- bad required metadata rows: `0`
- `announcement_date` differing from the local date encoded in `announcement_at`: `0`
- invalid session enum rows: `0`
- non-pilot ticker rows: `0`
- rows before ticker-specific fetch lower bound: `0`

Existing `rc_fundamental_quarterly` row count remained `155571`.

## Timestamp And Session Spot Checks

Representative persisted rows:

- `XOM 2006-10-26T08:00:00-04:00`: `BEFORE_MARKET`
- `JPM 2007-10-17T06:00:00-04:00`: `BEFORE_MARKET`
- `AAPL 2006-07-19T16:00:00-04:00`: `DURING_MARKET`
- `NVDA 2006-11-09T16:00:00-05:00`: `DURING_MARKET`

Exactly `16:00` persisted as `DURING_MARKET`. Announcement dates matched the New York-local date portion of `announcement_at`; they were not shifted through UTC or Helsinki conversion.

## Idempotency Rerun

The same explicit apply command was run again for every pilot ticker.

| Ticker | Eligible | Inserted | Updated | Unchanged |
| --- | ---: | ---: | ---: | ---: |
| AAPL | 80 | 0 | 0 | 80 |
| MSFT | 78 | 0 | 0 | 78 |
| JPM | 76 | 0 | 0 | 76 |
| XOM | 79 | 0 | 0 | 79 |
| NVDA | 70 | 0 | 0 | 70 |

After the rerun:

- Total rows remained `383`
- Duplicate natural key groups remained `0`
- Rows with `created_at_utc <> updated_at_utc`: `0`
- `source_observed_at_utc` remained the first material observation timestamp per ticker

Observation timestamp changes alone did not cause write churn. This required a narrow code correction before the real pilot: `source_observed_at_utc` is ignored for material-change comparison and only changes on insert or when another mutable source field changes.

## Post-Pilot Coverage Audit

The read-only coverage audit was run for the five pilot tickers. All five succeeded.

| Ticker | Coverage | Covers actual fundamentals history | Covers safety margin |
| --- | --- | --- | --- |
| AAPL | `COVERAGE_PARTIAL` | yes | no |
| MSFT | `COVERAGE_PARTIAL` | yes | no |
| JPM | `COVERAGE_PARTIAL` | yes | no |
| XOM | `COVERAGE_PARTIAL` | yes | no |
| NVDA | `COVERAGE_PARTIAL` | yes | no |

No ticker failed actual-history coverage. The partial status reflects only the optional 120-day margin.

## Restore Readiness

The original pre-migration backup was copied with SQLite's backup API to:

```text
/tmp/swingmaster_restore_readiness/fundamentals_usa.pre_migration_029_earnings_event.20260730T154752Z.restore_check.db
```

Restore-readiness checks:

- Isolated copy `quick_check` before migration: `ok`
- `rc_earnings_event` before migration: absent
- `rc_fundamental_quarterly` rows before migration: `155571`
- Migration runner on isolated copy: succeeded
- Isolated copy `quick_check` after migration: `ok`
- `rc_earnings_event` after migration: present
- Event rows after migration: `0`
- Synthetic isolated event insert: `1`
- Event rows after synthetic insert: `1`

The restore-readiness artifact was preserved.

## Backfill Readiness

The system is ready for the next guarded step: a broader coverage audit and then a controlled full-universe backfill plan.

Remaining cautions:

- The active `.venv` is Python 3.10.12 while `pyproject.toml` declares `>=3.12`.
- Yahoo still returns only up to the yfinance 100-row cap; established pilot tickers covered actual fundamentals history but not the wider 120-day margin.
- Future full-universe backfill should be sequential, resumable, and monitored for Yahoo source/rate-limit failures.
