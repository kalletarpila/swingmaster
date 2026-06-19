# SwingMaster Reported Vintage Full Real DB Dry Run Phase 4F3

## Scope

Phase 4F3 ran and documented a full real DB read-only dry-run for reported-vintage legacy backfill planning.

This phase did not:

- backfill data
- write to `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- insert into `rc_fundamental_quarterly_vintage`
- insert into `rc_fundamental_quarterly_field_provenance`
- call providers
- run refresh jobs or schedulers
- change runtime code
- change provider, normalization, TTM, scoring, valuation, percentile, UI, or ESS behavior

## Commands Run

Git status check:

```bash
git status --short
```

Unrelated untracked files observed and not staged:

- `failed_yahoo_batch_FIN_YAHOO_BATCH_2026-05-11.txt`
- `failed_yahoo_batch_FIN_YAHOO_BATCH_2026-05-16.txt`
- `fundamentals_usa.db-shm`
- `fundamentals_usa.db-wal`
- `pipeline_execution.log`
- `temp/`

DB existence check:

```bash
test -f /home/kalle/projects/swingmaster/fundamentals_usa.db
```

Pre-dry-run read-only counts:

```bash
sqlite3 -readonly /home/kalle/projects/swingmaster/fundamentals_usa.db \
  "SELECT 'rc_fundamental_quarterly', COUNT(*) FROM rc_fundamental_quarterly UNION ALL SELECT 'rc_fundamental_quarterly_vintage', COUNT(*) FROM rc_fundamental_quarterly_vintage UNION ALL SELECT 'rc_fundamental_quarterly_field_provenance', COUNT(*) FROM rc_fundamental_quarterly_field_provenance;"
```

Pre-dry-run integrity check:

```bash
sqlite3 -readonly /home/kalle/projects/swingmaster/fundamentals_usa.db \
  "PRAGMA integrity_check;"
```

Full dry-run command:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_reported_vintage_backfill \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-06-19 \
  --format json \
  --legacy-availability-policy live_safe_legacy_baseline \
  --legacy-available-at-utc 2026-06-19T00:00:00Z
```

Bounded sample command:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_reported_vintage_backfill \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-06-19 \
  --max-rows 5 \
  --include-sample-rows 5 \
  --format json \
  --legacy-availability-policy live_safe_legacy_baseline \
  --legacy-available-at-utc 2026-06-19T00:00:00Z
```

Post-dry-run counts and integrity check used the same read-only SQLite commands as the pre-check.

## Policy Used

Policy:

- `live_safe_legacy_baseline`

Timestamp:

- `legacy_available_at_utc = 2026-06-19T00:00:00Z`

This policy is live/forward safe because legacy rows become available only from the explicit backfill timestamp onward. It is not a true historical publication, filing, or provider availability timestamp.

Historical PIT backtests before `2026-06-19T00:00:00Z` will not see these legacy vintages under this policy.

## Full Dry-Run Result

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
| `warning_count` | `310662` |

The full dry-run completed successfully with summary-sized JSON output because no sample rows were requested.

## Sample Candidate Interpretation

The bounded sample run inspected five rows and returned five planned candidates.

Sample rows were for ticker `A` with period end dates:

- `2006-10-31`
- `2007-10-31`
- `2008-10-31`
- `2009-01-31`
- `2009-04-30`

Observed candidate properties:

- `statement_vintage_id` format: `legacy:usa:{ticker}:{period_end_date}:{source_hash_prefix_16}`
- `source_hash` was present for each sample row.
- `availability_quality`: `LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL`
- `available_at_utc`: `2026-06-19T00:00:00Z`
- `planned_field_provenance_count`: `1`, `7`, `8`, `7`, `7`

Each sample row warned that historical backtests before the backfill timestamp will not see these vintages.

## Pre/Post DB Verification

Pre-dry-run `PRAGMA integrity_check`:

- `ok`

Post-dry-run `PRAGMA integrity_check`:

- `ok`

| Table | Pre count | Post count | Changed |
| --- | ---: | ---: | --- |
| `rc_fundamental_quarterly` | `155331` | `155331` | no |
| `rc_fundamental_quarterly_vintage` | `0` | `0` | no |
| `rc_fundamental_quarterly_field_provenance` | `0` | `0` | no |

## Interpretation

From a mechanics perspective, the dry-run is ready under the explicit `live_safe_legacy_baseline` policy: the full latest table can be converted into candidate vintage rows without schema blockers or policy-decision blockers.

A real write backfill remains a separate approval task. This document only proves dry-run feasibility and expected scale.

The full-scale planned vintage row count is plausible because it matches the current `rc_fundamental_quarterly` row count.

The planned provenance row count is materially larger than the vintage row count and should be considered in write planning, transaction sizing, runtime monitoring, and rollback planning.

`EXTERNALLY_VERIFIED_RELEASE_DATE` remains the better policy for historical PIT/backtest quality, but it requires verified release-date data with preserved source metadata.

## Recommended Next Action

Recommended next phase:

```text
Phase 4G1: guarded real DB backfill apply plan, no execution yet
```

That phase should design the write plan, transaction boundaries, pre/post verification, rollback plan, operational limits, and explicit approval gate before any real DB backfill is executed.
