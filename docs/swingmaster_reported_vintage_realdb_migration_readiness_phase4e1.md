# SwingMaster Reported Vintage Real DB Migration Readiness Phase 4E1

## Scope

This is a readiness and backup plan for a later real DB schema migration.

Phase 4E2 status: [SwingMaster Reported Vintage Real DB Migration Apply Phase 4E2](swingmaster_reported_vintage_realdb_migration_apply_phase4e2.md) documents the later schema-only apply result. The vintage/provenance tables were created, no backfill was run, and post-checks passed.

- No real DB writes were done.
- No migration was applied to `/home/kalle/projects/swingmaster/fundamentals_usa.db`.
- No backfill was run.
- No providers were called.
- No refresh or scheduler jobs were run.
- No production write paths or readers were changed.

This document is based on repo evidence, temp-DB migration tests, and read-only schema inspection.

## Current Blocker

Phase 4D2 reported the real USA fundamentals DB status as `BLOCKED_MISSING_SCHEMA`.

Current real DB summary:

- `fundamentals_db`: `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- `market`: `usa`
- `as_of_date`: `2026-06-19`
- `latest_quarterly_row_count`: `155331`
- `existing_vintage_row_count`: `0`
- `eligible_latest_rows`: `155331`
- `already_backfilled_rows`: `0`
- `missing_vintage_table`: `true`
- `missing_provenance_table`: `true`

Read-only table inspection confirmed that the DB contains current fundamentals tables such as `rc_fundamental_quarterly`, `rc_fundamental_statement_raw`, `rc_fundamental_ttm`, `rc_fundamental_valuation`, and `rc_fundamental_score_percentile`, but not:

- `rc_fundamental_quarterly_vintage`
- `rc_fundamental_quarterly_field_provenance`

Backfill cannot proceed before the schema exists.

## Migration Source Of Truth

The additive reported-vintage schema is defined in:

- `swingmaster/infra/sqlite/migrations/028_rc_fundamental_quarterly_vintage.sql`

The migration creates:

- `rc_fundamental_quarterly_vintage`
- `rc_fundamental_quarterly_field_provenance`

The migration runner includes this migration through `get_quarterly_vintage_migration_file_path()` in:

- `swingmaster/cli/run_fundamental_migrations.py`

Both new tables are included in `REQUIRED_TABLES`, and the runner validates required columns through:

- `QUARTERLY_VINTAGE_REQUIRED_COLUMNS`
- `QUARTERLY_FIELD_PROVENANCE_REQUIRED_COLUMNS`
- `validate_fundamental_schema(...)`

### Vintage Table

Expected table: `rc_fundamental_quarterly_vintage`

Key columns:

- `ticker`
- `market`
- `period_end_date`
- `statement_vintage_id`
- `source_provider`
- `source_document_id`
- `source_hash`
- `revision_number`
- `is_restated`
- `supersedes_vintage_id`
- `availability_quality`
- `filed_at_utc`
- `available_at_utc`
- `ingested_at_utc`
- `provider_observed_at_utc`
- run lineage columns
- reported financial fields
- audit timestamps

Primary key:

- `(ticker, period_end_date, statement_vintage_id)`

Important indexes:

- `idx_fundamental_quarterly_vintage_ticker_period`
- `idx_fundamental_quarterly_vintage_ticker_available`
- `idx_fundamental_quarterly_vintage_ticker_period_available`
- `idx_fundamental_quarterly_vintage_market_ticker_period`
- `idx_fundamental_quarterly_vintage_source_hash`

### Field Provenance Table

Expected table: `rc_fundamental_quarterly_field_provenance`

Key columns:

- `ticker`
- `market`
- `period_end_date`
- `statement_vintage_id`
- `field_name`
- `field_value`
- `source_provider`
- source row/document/hash fields
- `provenance_role`
- `merge_action`
- `old_value`
- `new_value`
- `available_at_utc`
- `created_at_utc`
- run lineage columns

Primary key:

- `(ticker, period_end_date, statement_vintage_id, field_name, source_provider, provenance_role)`

Important indexes:

- `idx_fundamental_quarterly_field_prov_vintage`
- `idx_fundamental_quarterly_field_prov_source_hash`
- `idx_fundamental_quarterly_field_prov_run_id`

### Additive Behavior

The migration is additive:

- it uses `CREATE TABLE IF NOT EXISTS`
- it creates new indexes only for the new tables
- it does not alter `rc_fundamental_quarterly`
- it does not rename existing tables or fields
- it does not backfill rows
- it preserves current latest-table reader compatibility

## Existing Temp-DB Verification

Existing migration tests are in:

- `swingmaster/tests/test_fundamental_migrations.py`

Relevant tests verify:

- migration creates both vintage/provenance tables and is idempotent
- both tables are present in `REQUIRED_TABLES`
- required vintage/provenance columns exist
- duplicate vintage identity is blocked by the vintage primary key
- multiple vintages for the same ticker/period are allowed
- SEC primary and Yahoo fallback provenance rows can coexist for the same field/vintage
- important indexes exist

Targeted test command:

```bash
PYTHONPATH=. pytest -q swingmaster/tests/test_fundamental_migrations.py
```

What these tests prove:

- the schema can be created in temp DBs
- the required table/column/index contracts are enforced by tests
- the vintage PK supports history without silent overwrite

What they do not prove:

- applying the migration to `/home/kalle/projects/swingmaster/fundamentals_usa.db` is safe in the current operational environment
- disk space, locks, WAL state, or external process state are safe
- a legacy backfill policy is correct
- provider lineage or true PIT availability can be reconstructed from legacy rows

## Real DB Pre-Migration Checklist

Before any later real DB write phase:

- obtain explicit user approval for real DB migration
- confirm the DB path is exactly `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- ensure no refresh, provider, scheduler, UI, or other process is using the DB
- close UI/processes using the DB if needed
- inspect `git status --short`
- confirm no DB/WAL/SHM/backup/temp/log/generated files will be staged
- inspect existing WAL/SHM state and document it
- create a timestamped SQLite backup under a safe backup path
- verify backup file exists and has nonzero size
- run `PRAGMA integrity_check` on the source DB before migration
- inspect current table list read-only
- confirm `rc_fundamental_quarterly_vintage` is absent before migration
- confirm `rc_fundamental_quarterly_field_provenance` is absent before migration
- record row counts for key tables: `rc_fundamental_quarterly`, `rc_fundamental_statement_raw`, `rc_fundamental_ttm`, `rc_fundamental_valuation`, `rc_fundamental_score_percentile`, and `rc_fundamental_quarter_state`
- do not run backfill in the same step

Suggested read-only pre-check commands for a later phase:

```bash
sqlite3 "file:/home/kalle/projects/swingmaster/fundamentals_usa.db?mode=ro" "PRAGMA integrity_check;"
sqlite3 "file:/home/kalle/projects/swingmaster/fundamentals_usa.db?mode=ro" \
  "SELECT name FROM sqlite_master WHERE type IN ('table','view') ORDER BY name;"
```

## Real DB Migration Apply Plan For Later Phase

Do not run these commands in Phase 4E1.

Future guarded apply sequence:

1. Confirm user approval for real DB schema migration.
2. Confirm no provider/refresh/scheduler/UI process is using the DB.
3. Create and verify a timestamped backup.
4. Run source `PRAGMA integrity_check`.
5. Record pre-migration table list and row counts.
6. Apply only schema migration, not backfill.
7. Stop on error.
8. Do not commit partial manual changes.
9. Do not modify latest rows.
10. Do not run provider, refresh, scheduler, or backfill jobs.

The existing CLI that applies fundamentals migrations is:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.run_fundamental_migrations \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db
```

This CLI applies the full fundamentals migration set known to the repo. A later Phase 4E2 prompt should decide whether this exact CLI is acceptable for the real DB migration, or whether a narrower guarded migration-apply wrapper is required.

## Post-Migration Verification Checklist

After a later schema-only migration:

- run `PRAGMA integrity_check`
- verify `rc_fundamental_quarterly_vintage` exists
- verify `rc_fundamental_quarterly_field_provenance` exists
- verify expected columns with `PRAGMA table_info`
- verify expected indexes with `PRAGMA index_list`
- verify `rc_fundamental_quarterly` row count is unchanged
- verify key table row counts are unchanged for `rc_fundamental_statement_raw`, `rc_fundamental_ttm`, `rc_fundamental_valuation`, `rc_fundamental_score_percentile`, and `rc_fundamental_quarter_state`
- run `preflight_reported_vintage_backfill` again
- verify preflight status changes away from `BLOCKED_MISSING_SCHEMA`
- verify `existing_vintage_row_count` remains `0` before backfill
- run `PYTHONPATH=. pytest -q swingmaster/tests/test_fundamental_migrations.py` if appropriate
- document final status

## Rollback Plan

If migration fails or post-checks fail:

- restore from the timestamped SQLite backup
- verify restored DB with `PRAGMA integrity_check`
- rerun the read-only preflight
- do not attempt manual table surgery as first-line rollback
- keep a failed DB copy only if needed for diagnosis
- do not stage failed DB copies, WAL/SHM files, backups, temp files, or logs

## Backfill Remains Separate

Adding the vintage tables is not the same as backfill.

Legacy `rc_fundamental_quarterly` rows still lack trusted PIT metadata such as true `available_at_utc`, `filed_at_utc`, source document identity, provider observation time, and source hashes.

After schema migration, the next step should be another read-only `preflight_reported_vintage_backfill` run. Only after that should a guarded legacy backfill dry-run be designed.

## Recommended Next Step

Recommended next phase: Phase 4E2, backup-confirmed real DB schema migration apply, no backfill.

Phase 4E2 should be a separate explicitly approved write phase with backup, integrity checks, pre/post row counts, rollback plan, and no provider or backfill activity.
