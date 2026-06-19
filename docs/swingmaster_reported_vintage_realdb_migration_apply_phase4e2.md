# SwingMaster Reported Vintage Real DB Migration Apply Phase 4E2

## Scope

This phase applied the already-designed reported vintage schema to the real USA fundamentals DB.

- Schema-only migration was applied.
- No backfill was run.
- No provider, refresh, or scheduler jobs were run.
- No runtime behavior was changed.
- No production write path or reader wiring was changed.
- No legacy latest rows were intentionally modified.

Target DB:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`

## Critical Review Result

The full migration runner `swingmaster.cli.run_fundamental_migrations` was not used for the real DB write because it applies the full fundamentals migration set known to the repo, not only the reported-vintage migration.

For Phase 4E2 the allowed real DB change was narrower: create only:

- `rc_fundamental_quarterly_vintage`
- `rc_fundamental_quarterly_field_provenance`

Therefore the apply step used only:

- `swingmaster/infra/sqlite/migrations/028_rc_fundamental_quarterly_vintage.sql`

That migration is additive and uses `CREATE TABLE IF NOT EXISTS` plus indexes for the new tables.

## Backup

Backup path:

- `/home/kalle/projects/swingmaster/temp/fundamentals_usa__before_reported_vintage_schema__20260619T105401Z.sqlite`

Backup command:

```bash
sqlite3 "file:/home/kalle/projects/swingmaster/fundamentals_usa.db?mode=ro" \
  ".backup '/home/kalle/projects/swingmaster/temp/fundamentals_usa__before_reported_vintage_schema__20260619T105401Z.sqlite'"
```

Backup verification:

- backup file exists
- backup size: `5376380928` bytes
- backup `PRAGMA integrity_check`: `ok`
- backup table spot-check showed `rc_fundamental_quarterly` present and vintage/provenance tables absent

The backup is intentionally untracked and was not staged.

## Pre-Migration Baseline

Pre-migration `PRAGMA integrity_check`:

- `ok`

Pre-migration vintage/provenance table presence:

- `rc_fundamental_quarterly_vintage`: absent
- `rc_fundamental_quarterly_field_provenance`: absent

Pre-migration core row counts:

| Table | Row count |
| --- | ---: |
| `rc_fundamental_quarterly` | `155331` |
| `rc_fundamental_statement_raw` | `5204869` |
| `rc_fundamental_ttm` | `146448` |
| `rc_fundamental_valuation` | `32286` |
| `rc_fundamental_score_percentile` | `4502178` |
| `rc_fundamental_quarter_state` | `2936` |

Schema bookkeeping:

- `rc_fundamental_schema_version.version`: `1`
- `applied_at_utc`: `2026-04-25 08:31:33`

## Migration Apply

Command used:

```bash
sqlite3 /home/kalle/projects/swingmaster/fundamentals_usa.db \
  -cmd "BEGIN IMMEDIATE;" \
  -cmd ".read /home/kalle/projects/swingmaster/swingmaster/infra/sqlite/migrations/028_rc_fundamental_quarterly_vintage.sql" \
  "COMMIT;"
```

Applied migration:

- `028_rc_fundamental_quarterly_vintage.sql`

Created tables:

- `rc_fundamental_quarterly_vintage`
- `rc_fundamental_quarterly_field_provenance`

No backfill command was run.

## Post-Migration Verification

Post-migration `PRAGMA integrity_check`:

- `ok`

Post-migration table presence:

- `rc_fundamental_quarterly_vintage`: present
- `rc_fundamental_quarterly_field_provenance`: present

Verified key columns:

- `ticker`
- `market`
- `period_end_date`
- `statement_vintage_id`
- `source_provider`
- `source_hash`
- `revision_number`
- `is_restated`
- `availability_quality`
- `filed_at_utc`
- `available_at_utc`
- `ingested_at_utc`
- `created_at_utc`

Verified key indexes and constraints:

- `sqlite_autoindex_rc_fundamental_quarterly_vintage_1` from the vintage primary key
- `idx_fundamental_quarterly_vintage_ticker_period`
- `idx_fundamental_quarterly_vintage_ticker_available`
- `idx_fundamental_quarterly_vintage_ticker_period_available`
- `idx_fundamental_quarterly_vintage_market_ticker_period`
- `idx_fundamental_quarterly_vintage_source_hash`
- `sqlite_autoindex_rc_fundamental_quarterly_field_provenance_1` from the provenance primary key
- `idx_fundamental_quarterly_field_prov_vintage`
- `idx_fundamental_quarterly_field_prov_source_hash`
- `idx_fundamental_quarterly_field_prov_run_id`

Post-migration core row counts:

| Table | Pre count | Post count | Changed |
| --- | ---: | ---: | --- |
| `rc_fundamental_quarterly` | `155331` | `155331` | no |
| `rc_fundamental_statement_raw` | `5204869` | `5204869` | no |
| `rc_fundamental_ttm` | `146448` | `146448` | no |
| `rc_fundamental_valuation` | `32286` | `32286` | no |
| `rc_fundamental_score_percentile` | `4502178` | `4502178` | no |
| `rc_fundamental_quarter_state` | `2936` | `2936` | no |

New table row counts:

| Table | Row count |
| --- | ---: |
| `rc_fundamental_quarterly_vintage` | `0` |
| `rc_fundamental_quarterly_field_provenance` | `0` |

This is expected because no backfill was run.

## Post-Migration Preflight

Command:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.preflight_reported_vintage_backfill \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-06-19 \
  --format json
```

Summary:

| Field | Value |
| --- | --- |
| `overall_status` | `PARTIAL_METADATA_REQUIRED` |
| `ticker_count_checked` | `2936` |
| `latest_quarterly_row_count` | `155331` |
| `existing_vintage_row_count` | `0` |
| `eligible_latest_rows` | `155331` |
| `already_backfilled_rows` | `0` |
| `missing_vintage_table` | `false` |
| `missing_provenance_table` | `false` |
| `blocked_reason_count` | `0` |
| `warning_count` | `310662` |

Interpretation:

- The schema blocker moved away from `BLOCKED_MISSING_SCHEMA`.
- The DB is structurally ready for a later backfill dry-run design.
- Legacy PIT metadata is still incomplete, so no actual backfill should be run without an explicit placeholder policy.

Unavailable metadata reported by the preflight:

- `statement_vintage_id`
- `source_provider`
- `source_document_id`
- `source_hash`
- `filed_at_utc`
- `available_at_utc`
- `ingested_at_utc`
- `provider_observed_at_utc`
- `provider_run_id`
- `normalization_run_id`
- `supersedes_vintage_id`
- `availability_quality`

## Safety And Rollback

Rollback source:

- `/home/kalle/projects/swingmaster/temp/fundamentals_usa__before_reported_vintage_schema__20260619T105401Z.sqlite`

No rollback was needed because post-checks passed.

If rollback is later required, restore from the SQLite backup and verify with `PRAGMA integrity_check`. Do not manually drop tables as first-line rollback unless a separate rollback task is approved.

## Checks Run

```bash
PYTHONPATH=. pytest -q swingmaster/tests/test_fundamental_migrations.py
PYTHONPATH=. pytest -q swingmaster/tests/test_preflight_reported_vintage_backfill.py
PYTHONPATH=. pytest -q swingmaster/tests/test_reported_vintage_writer.py
PYTHONPATH=. pytest -q swingmaster/tests/test_reported_vintage_reader.py
git diff --check
```

Results:

- `test_fundamental_migrations.py`: `11 passed`
- `test_preflight_reported_vintage_backfill.py`: `11 passed`
- `test_reported_vintage_writer.py`: `12 passed`
- `test_reported_vintage_reader.py`: `13 passed`
- `git diff --check`: passed

## Recommended Next Phase

Recommended next phase: Phase 4F1, guarded legacy vintage backfill dry-run design, no writes.

Phase 4F1 should define a placeholder policy for legacy PIT metadata, expected row counts, dry-run output, transaction boundaries, and rollback expectations before any real backfill is considered.
