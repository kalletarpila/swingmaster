# SwingMaster Reported Vintage Backfill Real DB Preflight Phase 4D2

## Scope

This was a read-only preflight against the current USA fundamentals DB.

- No DB writes were performed.
- No backfill was performed.
- No providers were called.
- No refresh or scheduler jobs were run.
- No runtime code was changed.

The target DB was opened by the existing preflight CLI with SQLite `mode=ro` and `PRAGMA query_only=ON`.

Operational sidecar note: after the read-only SQLite run, `fundamentals_usa.db-shm` and `fundamentals_usa.db-wal` appeared as untracked workspace files. They were not staged, edited, removed, or copied in this task. The WAL file was zero bytes at the post-run check.

## Commands Run

Repo status:

```bash
git status --short
```

Target DB existence check:

```bash
test -f /home/kalle/projects/swingmaster/fundamentals_usa.db && ls -l /home/kalle/projects/swingmaster/fundamentals_usa.db
```

Post-run sidecar check:

```bash
ls -l /home/kalle/projects/swingmaster/fundamentals_usa.db \
  /home/kalle/projects/swingmaster/fundamentals_usa.db-shm \
  /home/kalle/projects/swingmaster/fundamentals_usa.db-wal
```

Read-only JSON preflight:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.preflight_reported_vintage_backfill \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-06-19 \
  --format json
```

Text output was not run because the full candidate list is very large; the JSON run already produced the required summary, metadata gap, and policy fields.

## Summary Result

The real DB file exists:

```text
/home/kalle/projects/swingmaster/fundamentals_usa.db
size: 5376380928 bytes
mtime: 2026-06-19 08:04
```

CLI summary:

| Field | Value |
| --- | --- |
| `fundamentals_db` | `/home/kalle/projects/swingmaster/fundamentals_usa.db` |
| `market` | `usa` |
| `as_of_date` | `2026-06-19` |
| `overall_status` | `BLOCKED_MISSING_SCHEMA` |
| `ticker_count_checked` | `2936` |
| `latest_quarterly_row_count` | `155331` |
| `existing_vintage_row_count` | `0` |
| `eligible_latest_rows` | `155331` |
| `already_backfilled_rows` | `0` |
| `missing_vintage_table` | `true` |
| `missing_provenance_table` | `true` |
| `blocked_reason_count` | `2` |
| `warning_count` | `310662` |

Blocked reasons:

- `MISSING_TABLE:rc_fundamental_quarterly_vintage`
- `MISSING_TABLE:rc_fundamental_quarterly_field_provenance`

## Interpretation

`BLOCKED_MISSING_SCHEMA` means this real DB has current latest quarterly rows, but it has not been migrated with the additive reported-fundamentals vintage tables.

The preflight found `155331` current `rc_fundamental_quarterly` rows across `2936` tickers as of `2026-06-19`. All of those rows are structurally candidate legacy backfill rows, but no actual backfill can be planned for this DB until the missing schema is addressed in a separate, backup-confirmed step.

This result does not mean a backfill was attempted. It only means the current DB is not yet structurally ready for a guarded legacy-vintage backfill.

## Metadata Gaps

The preflight confirms that legacy `rc_fundamental_quarterly` rows do not safely provide the full PIT/vintage metadata needed for true historical reconstruction.

Not safely inferable from current legacy latest rows:

- `available_at_utc`
- `filed_at_utc`
- `statement_vintage_id`
- `source_provider`
- `source_document_id`
- `source_hash`
- `provider_observed_at_utc`
- `provider_run_id`
- `normalization_run_id`
- `supersedes_vintage_id`
- `availability_quality`

Available or conservatively baseline-able only by policy:

- `market`: supplied by preflight argument
- `ticker`: present in latest rows
- `period_end_date`: present in latest rows
- `run_id`: present in latest rows when populated
- `revision_number`: could be policy baseline `1`
- `is_restated`: could be policy baseline `0`, but that is not provider truth

The preflight proposes synthetic legacy policy values such as `UNKNOWN_LEGACY` and `LEGACY_ESTIMATED`, but it does not apply them.

## Recommendation

Because `overall_status` is `BLOCKED_MISSING_SCHEMA`, the next action should be a separate backup-confirmed real DB migration readiness step before any backfill design or write attempt.

Phase 4E1 readiness plan: [SwingMaster Reported Vintage Real DB Migration Readiness Phase 4E1](swingmaster_reported_vintage_realdb_migration_readiness_phase4e1.md) documents the schema source of truth, backup requirements, pre-checks, post-checks, and rollback plan for that later migration step.

Phase 4E2 schema apply: [SwingMaster Reported Vintage Real DB Migration Apply Phase 4E2](swingmaster_reported_vintage_realdb_migration_apply_phase4e2.md) documents the later schema-only migration result. After that apply, the read-only preflight status moved to `PARTIAL_METADATA_REQUIRED`.

Do not proceed directly to backfill. A later step should first verify that applying the existing additive vintage schema to this DB is safe, explicitly backed up, and reviewed.

## Safety Note

Any future write/backfill phase must require:

- explicit backup
- integrity check
- dry-run first
- row-count preview
- transaction plan
- rollback plan
- user approval before writing

The known unrelated untracked files at the time of this run were:

- `failed_yahoo_batch_FIN_YAHOO_BATCH_2026-05-11.txt`
- `failed_yahoo_batch_FIN_YAHOO_BATCH_2026-05-16.txt`
- `pipeline_execution.log`
