# SwingMaster Reported Vintage Backfill Apply Plan Phase 4G1

## Scope

Phase 4G1 is an apply plan only.

No DB writes were performed.

No backfill was run.

No providers were called.

No refresh or scheduler jobs were run.

No production provider write path was changed.

No runtime code was changed.

## Current Readiness Summary

The reported-vintage schema exists in the real USA fundamentals DB:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`

Current target tables are empty:

- `rc_fundamental_quarterly_vintage`: `0`
- `rc_fundamental_quarterly_field_provenance`: `0`

Phase 4F3 full read-only dry-run status:

- `overall_status`: `DRY_RUN_READY`
- planned vintage rows: `155331`
- planned provenance rows: `1306388`

Chosen policy for the planned legacy baseline:

- `live_safe_legacy_baseline`
- `legacy_available_at_utc = 2026-06-19T00:00:00Z`

This policy is live/forward-safe because legacy rows become visible only from the backfill timestamp onward. It is not true historical publication, filing, or provider availability timing.

## Backfill Policy To Apply Later

Later apply should materialize the same policy dry-run already validated:

- `source_provider = UNKNOWN_LEGACY`
- `availability_quality = LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL`
- `available_at_utc = 2026-06-19T00:00:00Z`
- deterministic `statement_vintage_id`
- deterministic `source_hash`
- `revision_number = 1`
- `is_restated = 0`
- `source_document_id = NULL`
- `filed_at_utc = NULL`
- `provider_observed_at_utc = NULL`
- `provider_run_id = NULL`
- `supersedes_vintage_id = NULL`

Provenance policy:

- one provenance row per non-null financial field
- `source_provider = UNKNOWN_LEGACY`
- `provenance_role = LEGACY_BASELINE`
- `merge_action = LEGACY_BACKFILL_BASELINE`

The apply phase must not modify `rc_fundamental_quarterly`; it should only populate the vintage and provenance tables.

## Required Safety Gates Before Phase 4G2 Execution

Before any future real DB write:

- Get explicit user approval for Phase 4G2 execution.
- Confirm target DB path is `/home/kalle/projects/swingmaster/fundamentals_usa.db`.
- Confirm no refresh, provider, scheduler, UI, or other process is writing the DB.
- Run `git status --short`.
- Do not stage unrelated files.
- Record WAL/SHM sidecars if present.
- Create a timestamped SQLite backup under `/home/kalle/projects/swingmaster/temp/`.
- Verify backup file exists and size is greater than `0`.
- Run `PRAGMA integrity_check` on the backup.
- Run `PRAGMA integrity_check` on the source DB before write.
- Collect pre-backfill row counts for all key tables.
- Rerun the live-safe dry-run immediately before apply.

Key pre-backfill row counts:

- `rc_fundamental_quarterly`
- `rc_fundamental_quarterly_vintage`
- `rc_fundamental_quarterly_field_provenance`
- `rc_fundamental_statement_raw`
- `rc_fundamental_ttm`
- `rc_fundamental_valuation`
- `rc_fundamental_score_percentile`
- `rc_fundamental_quarter_state`

Immediate pre-apply dry-run must confirm:

- `overall_status = DRY_RUN_READY`
- planned vintage rows match `155331`, or the difference is explained before apply
- planned provenance rows match `1306388`, or the difference is explained before apply
- `already_has_vintage_rows = 0`, unless the difference is explained as an intentional prior partial/complete apply
- `blocked_rows = 0`
- `requires_policy_decision_rows = 0`

## Apply Strategy For Later Phase 4G2

Design only; do not execute in Phase 4G1.

Recommended strategy:

1. Open the real DB for write only after explicit approval and backup verification.
2. Start a single transaction if feasible.
3. Read source rows from `rc_fundamental_quarterly`.
4. Build deterministic candidate vintage rows using the dry-run policy.
5. Insert all `rc_fundamental_quarterly_vintage` rows.
6. Insert all `rc_fundamental_quarterly_field_provenance` rows.
7. Verify inserted counts inside the transaction if feasible.
8. Commit only after all expected counts match.

Insertion order:

- Insert vintage rows first.
- Insert provenance rows second.

Reason:

- Provenance rows reference a statement vintage id and are only meaningful after the corresponding vintage row exists.
- If the provenance insert fails, the transaction can still roll back both vintage and provenance inserts together.

Implementation rules:

- Use plain `INSERT`, not `INSERT OR REPLACE`.
- Stop on first integrity error.
- Do not modify `rc_fundamental_quarterly`.
- Do not modify TTM, valuation, rank, percentile, or quarter-state tables.
- Do not run `VACUUM`.
- Do not run providers, refresh jobs, schedulers, UI jobs, or ESS jobs.
- Do not perform partial manual fixes inside the transaction.
- Use batched inserts if needed for memory or runtime, but keep the transaction atomic if feasible.
- If a single transaction is not feasible, stop and document a revised chunked transaction plan before writing.

## Expected Post-Backfill Counts

Expected after successful Phase 4G2:

| Table | Expected count |
| --- | ---: |
| `rc_fundamental_quarterly` | `155331` |
| `rc_fundamental_quarterly_vintage` | `155331` |
| `rc_fundamental_quarterly_field_provenance` | approximately `1306388` |

The exact provenance count should be taken from the immediately preceding dry-run. Phase 4F3 measured `1306388`.

## Required Post-Checks After Future Apply

After any future apply:

- Run `PRAGMA integrity_check`.
- Collect row counts for all key tables.
- Verify `rc_fundamental_quarterly` is unchanged.
- Verify vintage row count matches the immediate dry-run plan.
- Verify provenance row count matches the immediate dry-run plan.
- Run `preflight_reported_vintage_backfill`.
- Run `dry_run_reported_vintage_backfill` again with the same live-safe policy.
- Run targeted tests.

Expected post-apply dry-run behavior:

- `already_has_vintage_rows` should increase to `155331`.
- planned new rows should become `0`, or the report should otherwise show an equivalent no-op state.
- blocked rows should remain `0`.

Sample PIT reader checks:

- Select a few tickers and periods from the inserted vintage rows.
- Verify `get_pit_quarterly_vintage` returns the legacy baseline at or after `2026-06-19T00:00:00Z`.
- Verify `get_pit_quarterly_vintage` returns `None` before `2026-06-19T00:00:00Z` under the live-safe policy.
- Verify field provenance rows exist for non-null financial fields for the selected vintages.

Targeted tests:

- migration tests
- dry-run tests
- vintage writer tests
- vintage reader tests

## Rollback Plan

Rollback source is the timestamped SQLite backup created before Phase 4G2.

If apply fails before commit, transaction rollback should leave the DB unchanged.

If post-check fails after commit, restore the backup.

After restore:

- run `PRAGMA integrity_check`
- collect the key row counts again
- verify vintage/provenance tables returned to the expected pre-apply state

Do not manually delete vintage or provenance rows as first-line rollback unless a separate rollback task is approved.

Do not stage backup files, DB files, WAL/SHM files, temp files, generated files, or logs.

## Risks

- `1306388` planned provenance rows is large but expected.
- The live-safe availability timestamp is not true historical publication timing.
- Backfilled legacy rows become visible only from `2026-06-19T00:00:00Z`.
- Future externally verified release dates may supersede or complement this legacy baseline.
- Duplicate insert risk exists if the backfill is rerun without no-op detection.
- WAL/SHM sidecars need explicit observation and must not be committed.
- A long transaction may have runtime, disk, lock, or rollback implications.
- A partially implemented chunking strategy could create ambiguity; if chunking is needed, design it before writing.

## Recommendation

Recommended next phase:

```text
Phase 4G2: implement and run guarded real DB backfill apply only after explicit user approval
```

Do not add provider integration yet.

Do not add ESS integration yet.

Do not wire production dual-write until the legacy baseline is safely loaded or intentionally skipped.

Phase 4G2 result: the guarded legacy baseline backfill was applied and documented in [Reported Vintage Backfill Apply Phase 4G2](swingmaster_reported_vintage_backfill_apply_phase4g2.md). The apply inserted `155331` vintage rows and `1306388` provenance rows.
