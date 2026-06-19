# SwingMaster Reported Vintage Backfill Apply Phase 4G2

## Scope

Phase 4G2 applied the guarded legacy reported-vintage baseline backfill to the real USA fundamentals DB.

Real DB changed:

- inserted rows into `rc_fundamental_quarterly_vintage`
- inserted rows into `rc_fundamental_quarterly_field_provenance`

This phase did not:

- modify `rc_fundamental_quarterly`
- modify raw, TTM, valuation, percentile, score, or quarter-state tables
- call providers
- run refresh jobs or schedulers
- run TTM/scoring/valuation/percentile recalculation
- change UI
- implement ESS integration
- wire provider/refresh paths to the vintage writer
- run `VACUUM`

Target DB:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`

## Approval Gate

The prompt contained the required approval line:

```text
USER_APPROVES_REAL_DB_BACKFILL_APPLY=YES
```

Real DB writes were limited to the approved legacy baseline vintage/provenance backfill.

## Backup

Backup path:

- `/home/kalle/projects/swingmaster/temp/fundamentals_usa__before_reported_vintage_backfill__20260619T142733Z.sqlite`

Backup size:

- `5376434176` bytes

Backup integrity:

- `PRAGMA integrity_check`: `ok`

The backup is intentionally untracked and was not staged.

## Pre-Backfill Baseline

Sidecars observed before apply:

- `fundamentals_usa.db-shm`: present
- `fundamentals_usa.db-wal`: present, size `0` at precheck time

Pre-backfill source integrity:

- `PRAGMA integrity_check`: `ok`

Pre-backfill table presence:

- `rc_fundamental_quarterly`: present
- `rc_fundamental_quarterly_vintage`: present
- `rc_fundamental_quarterly_field_provenance`: present

Pre-backfill counts:

| Table | Pre count |
| --- | ---: |
| `rc_fundamental_quarterly` | `155331` |
| `rc_fundamental_quarterly_vintage` | `0` |
| `rc_fundamental_quarterly_field_provenance` | `0` |
| `rc_fundamental_statement_raw` | `5204869` |
| `rc_fundamental_ttm` | `146448` |
| `rc_fundamental_valuation` | `32286` |
| `rc_fundamental_score_percentile` | `4502178` |
| `rc_fundamental_quarter_state` | `2936` |

## Immediate Pre-Apply Dry-Run

Command:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_reported_vintage_backfill \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-06-19 \
  --format json \
  --legacy-availability-policy live_safe_legacy_baseline \
  --legacy-available-at-utc 2026-06-19T00:00:00Z
```

Result:

| Field | Value |
| --- | ---: |
| `overall_status` | `DRY_RUN_READY` |
| `total_latest_rows` | `155331` |
| `candidate_rows` | `155331` |
| `planned_vintage_rows` | `155331` |
| `planned_provenance_rows` | `1306388` |
| `already_has_vintage_rows` | `0` |
| `skipped_rows` | `0` |
| `blocked_rows` | `0` |
| `requires_policy_decision_rows` | `0` |

## Apply CLI

New guarded CLI:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.apply_reported_vintage_backfill
```

Safety behavior:

- requires `--confirm-write`
- requires explicit expected vintage and provenance row counts
- runs dry-run before write
- refuses apply if dry-run is not `DRY_RUN_READY`
- refuses apply if planned counts differ from expected counts
- uses plain `INSERT`
- does not use `INSERT OR REPLACE`
- wraps writes in a transaction and rolls back on exception
- does not call providers, refresh jobs, scheduler jobs, or network APIs

## Apply Command

```bash
PYTHONPATH=. python3 -m swingmaster.cli.apply_reported_vintage_backfill \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-06-19 \
  --legacy-availability-policy live_safe_legacy_baseline \
  --legacy-available-at-utc 2026-06-19T00:00:00Z \
  --confirm-write \
  --expected-vintage-rows 155331 \
  --expected-provenance-rows 1306388
```

## Apply Summary

Result:

- `status`: `APPLY_COMPLETE`
- `run_id`: `reported-vintage-legacy-backfill:usa:2026-06-19:2026-06-19T14:28:43Z`
- `vintage_rows_written`: `155331`
- `provenance_rows_written`: `1306388`

Policy applied:

- `legacy_availability_policy`: `live_safe_legacy_baseline`
- `legacy_available_at_utc`: `2026-06-19T00:00:00Z`
- `availability_quality`: `LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL`
- `source_provider`: `UNKNOWN_LEGACY`
- `provenance_role`: `LEGACY_BASELINE`
- `merge_action`: `LEGACY_BACKFILL_BASELINE`

## Post-Backfill Counts

Post-backfill source integrity:

- `PRAGMA integrity_check`: `ok`

| Table | Pre count | Post count | Changed |
| --- | ---: | ---: | --- |
| `rc_fundamental_quarterly` | `155331` | `155331` | no |
| `rc_fundamental_quarterly_vintage` | `0` | `155331` | yes |
| `rc_fundamental_quarterly_field_provenance` | `0` | `1306388` | yes |
| `rc_fundamental_statement_raw` | `5204869` | `5204869` | no |
| `rc_fundamental_ttm` | `146448` | `146448` | no |
| `rc_fundamental_valuation` | `32286` | `32286` | no |
| `rc_fundamental_score_percentile` | `4502178` | `4502178` | no |
| `rc_fundamental_quarter_state` | `2936` | `2936` | no |

Vintage quality spot-check:

- count: `155331`
- min `available_at_utc`: `2026-06-19T00:00:00Z`
- max `available_at_utc`: `2026-06-19T00:00:00Z`
- distinct `availability_quality` count: `1`

## Post-Apply Dry-Run No-Op Result

The same dry-run command after apply returned:

| Field | Value |
| --- | ---: |
| `overall_status` | `DRY_RUN_READY` |
| `total_latest_rows` | `155331` |
| `candidate_rows` | `0` |
| `planned_vintage_rows` | `0` |
| `planned_provenance_rows` | `0` |
| `already_has_vintage_rows` | `155331` |
| `skipped_rows` | `155331` |
| `blocked_rows` | `0` |
| `requires_policy_decision_rows` | `0` |

Post-apply preflight summary:

- `existing_vintage_row_count`: `155331`
- `already_backfilled_rows`: `155331`
- `eligible_latest_rows`: `0`
- `overall_status`: `PARTIAL_METADATA_REQUIRED`

The preflight command produced very large per-candidate JSON output after the full apply; only the summary is relevant here.

## PIT Reader Sample

Sample PIT checks used:

- ticker `A`, period `2006-10-31`
- ticker `A`, period `2007-10-31`
- ticker `A`, period `2008-10-31`

Result:

| Ticker | Period | Cutoff `2026-06-19T00:00:00Z` | Cutoff `2026-06-18T23:59:59Z` | Available at |
| --- | --- | --- | --- | --- |
| `A` | `2006-10-31` | row found | none | `2026-06-19T00:00:00Z` |
| `A` | `2007-10-31` | row found | none | `2026-06-19T00:00:00Z` |
| `A` | `2008-10-31` | row found | none | `2026-06-19T00:00:00Z` |

This matches the live-safe policy: legacy rows are visible at or after the backfill timestamp, not before it.

## Tests And Checks

Commands run:

```bash
PYTHONPATH=. pytest -q swingmaster/tests/test_apply_reported_vintage_backfill.py
PYTHONPATH=. pytest -q swingmaster/tests/test_apply_reported_vintage_backfill.py swingmaster/tests/test_dry_run_reported_vintage_backfill.py swingmaster/tests/test_preflight_reported_vintage_backfill.py swingmaster/tests/test_reported_quarterly_dual_write.py swingmaster/tests/test_reported_vintage_writer.py swingmaster/tests/test_reported_vintage_reader.py swingmaster/tests/test_fundamental_migrations.py
python3 -m py_compile swingmaster/cli/apply_reported_vintage_backfill.py
git diff --check
```

Results:

- new apply tests: `12 passed`
- targeted suite: `96 passed`
- `py_compile`: passed
- `git diff --check`: passed

## Rollback Note

Rollback source:

- `/home/kalle/projects/swingmaster/temp/fundamentals_usa__before_reported_vintage_backfill__20260619T142733Z.sqlite`

If rollback is required later, restore from this backup and verify with `PRAGMA integrity_check`.

Do not manually delete vintage/provenance rows as first-line rollback unless a separate rollback task is approved.

The backup, DB, WAL/SHM sidecars, logs, and temp files were not staged.

## Next Recommendation

Recommended next phase:

```text
Phase 4H1: reported-vintage read integration planning, no production wiring yet
```

Do not add provider integration, ESS integration, or production dual-write until the read integration plan is reviewed.
