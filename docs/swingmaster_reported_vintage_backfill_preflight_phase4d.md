# SwingMaster Reported Vintage Backfill Preflight Phase 4D

## Purpose

Phase 4D adds a read-only preflight for estimating how existing `rc_fundamental_quarterly` latest rows could later be converted into `rc_fundamental_quarterly_vintage` rows.

It answers:

- whether the fundamentals DB has the required latest, vintage, and provenance tables
- how many latest quarterly rows exist for the requested market/as-of scope
- how many matching vintage rows already exist
- which latest rows are candidate legacy backfill rows
- which required PIT/vintage metadata cannot be safely inferred from legacy latest rows

## Command Examples

JSON:

```bash
python3 -m swingmaster.cli.preflight_reported_vintage_backfill \
  --fundamentals-db /path/to/fundamentals.db \
  --market usa \
  --as-of-date 2026-12-31 \
  --format json
```

Text for selected tickers:

```bash
python3 -m swingmaster.cli.preflight_reported_vintage_backfill \
  --fundamentals-db /path/to/fundamentals.db \
  --market usa \
  --as-of-date 2026-12-31 \
  --tickers AAPL,MSFT,NVDA \
  --format text
```

Fail if schema is blocked:

```bash
python3 -m swingmaster.cli.preflight_reported_vintage_backfill \
  --fundamentals-db /path/to/fundamentals.db \
  --market usa \
  --as-of-date 2026-12-31 \
  --fail-if-blocked
```

## Status Meanings

- `OK_READY_FOR_BACKFILL_DESIGN`: schema and rows are present and no metadata gaps were detected. This is unlikely for legacy latest rows.
- `PARTIAL_METADATA_REQUIRED`: latest rows exist and schema is present, but required PIT/vintage metadata cannot be safely inferred.
- `BLOCKED_MISSING_SCHEMA`: one or both vintage/provenance tables are missing.
- `NO_SOURCE_ROWS`: no `rc_fundamental_quarterly` rows matched the requested scope.
- `UNKNOWN`: reserved for unexpected or future states.

## What It Checks

The preflight reads:

- `sqlite_master` table names
- `PRAGMA table_info(...)`
- current latest rows from `rc_fundamental_quarterly`
- matching rows from `rc_fundamental_quarterly_vintage`

Matching vintage rows are detected by `ticker`, `period_end_date`, and `market` when the vintage schema supports `market`.

## Explicit Non-Goals

This phase does not:

- write to any DB
- run migrations
- backfill data
- call SEC/Yahoo/yfinance/Finnhub or any network provider
- run refresh jobs or scheduler jobs
- wire vintage tables into current production readers
- implement ESS integration

## Legacy Metadata Gaps

Legacy `rc_fundamental_quarterly` rows do not carry enough PIT metadata to reconstruct true historical availability.

Not safely inferable from latest rows:

- `filed_at_utc`
- `available_at_utc`
- `ingested_at_utc`
- `provider_observed_at_utc`
- `source_provider`
- `source_document_id`
- `source_hash`
- `provider_run_id`
- `normalization_run_id`

The preflight may propose synthetic legacy values, but it does not apply them.

## Future Guarded Backfill Recommendation

A later guarded backfill should require:

- explicit DB path
- explicit backup confirmation
- dry-run summary reviewed before writes
- synthetic `statement_vintage_id` policy for legacy baseline rows
- conservative `availability_quality`, such as `LEGACY_ESTIMATED`
- a clear policy for `available_at_utc` because true historical availability is not reconstructable from latest rows alone
- post-backfill validation using the vintage reader

## Safety Rules

The CLI requires an explicit `--fundamentals-db`; it does not silently use production DB paths.

The DB is opened with SQLite `mode=ro`, and `PRAGMA query_only=ON` is set before inspection.
