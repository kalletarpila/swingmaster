# SwingMaster GIS Provider Vintage Apply Phase 4M6

Date: 2026-07-07

## Purpose

Phase 4M6 applied exactly one provider-derived SEC/latest-writer vintage row for:

- market: `usa`
- ticker: `GIS`
- period_end_date: `2025-05-25`
- mismatch field: `total_debt`

The write supersedes the visible legacy baseline for PIT reads at and after `2026-07-07T20:04:15Z` without deleting or replacing the legacy vintage.

No quarter_update, provider calls, scheduler, refresh jobs, recovery apply, broad backfill, or Yahoo-aware apply were run.

## Pre-Apply Gates

Read-only preflight result:

- quick_check: `ok`
- latest rows: `155377`
- vintage rows: `155377`
- provenance rows: `1306713`
- latest `GIS:2025-05-25 total_debt=14878600000.0`
- visible PIT vintage `total_debt=677000000.0`
- visible PIT vintage id: `legacy:usa:GIS:2025-05-25:9441b8313c7894e3`
- candidate statement_vintage_id count: `0`
- existing provider-derived SEC vintage with same source hash: `0`
- duplicate statement_vintage_id count: `0`

Immediate pre-apply dry-run command:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_provider_vintage_for_reported_mismatch \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --ticker GIS \
  --period-end-date 2025-05-25 \
  --available-at-utc 2026-07-07T20:04:15Z \
  --ingested-at-utc 2026-07-07T20:04:15Z \
  --vintage-run-id USA_QUARTER_UPDATE_2026-07-07__GIS_TOTAL_DEBT_PROVIDER_VINTAGE_APPLY \
  --format json
```

Dry-run gate result:

- overall_status: `DRY_RUN_READY`
- planned_vintage_rows: `1`
- planned_provenance_rows: `11`
- unknown_provenance_count: `0`
- candidate statement_vintage_id: `sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0`
- candidate `total_debt=14878600000.0`
- component sum: `14878600000.0`

## Apply

Added and used:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.apply_provider_vintage_for_reported_mismatch \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --ticker GIS \
  --period-end-date 2025-05-25 \
  --available-at-utc 2026-07-07T20:04:15Z \
  --ingested-at-utc 2026-07-07T20:04:15Z \
  --vintage-run-id USA_QUARTER_UPDATE_2026-07-07__GIS_TOTAL_DEBT_PROVIDER_VINTAGE_APPLY \
  --expected-statement-vintage-id sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0 \
  --expected-vintage-count 1 \
  --expected-provenance-count 11 \
  --approval-token USER_APPROVES_GIS_PROVIDER_VINTAGE_APPLY \
  --format json
```

Backup path:

`/home/kalle/projects/swingmaster/fundamentals_usa.db.provider_vintage_apply.USA_QUARTER_UPDATE_2026-07-07__GIS_TOTAL_DEBT_PROVIDER_VINTAGE_APPLY.bak`

Apply result:

- applied: `true`
- vintage_rows_inserted: `1`
- provenance_rows_inserted: `11`
- latest count delta: `0`
- vintage count delta: `1`
- provenance count delta: `11`

## Post-Apply Verification

Post-apply quick_check:

- `ok`

Counts:

- latest rows: `155377`
- vintage rows: `155378`
- provenance rows: `1306724`

Inserted row:

- statement_vintage_id: `sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0`
- statement_vintage_id count: `1`
- source_provider: `sec_edgar`
- run_id: `USA_QUARTER_UPDATE_2026-07-07__GIS_TOTAL_DEBT_PROVIDER_VINTAGE_APPLY`
- available_at_utc: `2026-07-07T20:04:15Z`
- `total_debt=14878600000.0`
- provenance rows: `11`

Latest table:

- unchanged count: `155377`
- `GIS:2025-05-25 total_debt=14878600000.0`
- run_id remains `USA_QUARTER_UPDATE_2026-07-07__QUARTERLY__QUARTERLY`

PIT visibility:

- at `2026-07-07T20:04:14Z`: `legacy:usa:GIS:2025-05-25:9441b8313c7894e3`, `total_debt=677000000.0`
- at `2026-07-07T20:04:15Z`: `sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0`, `total_debt=14878600000.0`
- at `2026-07-08T00:00:00Z`: `sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0`, `total_debt=14878600000.0`

Duplicate statement_vintage_id count remains `0`.

## Scope Diagnostic

Command:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.diagnose_quarter_update_vintage_scope \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --source-run-id USA_QUARTER_UPDATE_2026-07-07__QUARTERLY \
  --enrich-run-id USA_QUARTER_UPDATE_2026-07-07__QUARTERLY__ENRICH \
  --format json \
  --sample-limit 20
```

The CLI returned exit code `1`, but its JSON summary reports the post-apply target state:

- overall_diagnostic_status: `NO_MISMATCH`
- value_mismatch_count: `0`
- unexplained_mismatch_count: `0`
- value_mismatch_sample: empty
- latest_without_vintage_count: `0`
- vintage_without_latest_count: `0`
- duplicate_statement_vintage_id_count: `0`
- planner_scope_count: `0`
- planner_blocked_rows: `0`

## Recommended Next Phase

No GIS total debt drift remains. The next phase should review the remaining `FINAL_MIXED_REQUIRED` state separately:

- completion_reason: `yahoo_filled_fields_on_sec_backed_latest`
- next_action: `CREATE_FINAL_MIXED_VINTAGE`

That should remain a separate guarded phase. Do not run broad Yahoo-aware apply or recovery as part of this GIS correction.

## Verification

Checks run:

```bash
python3 -m py_compile swingmaster/cli/apply_provider_vintage_for_reported_mismatch.py
PYTHONPATH=. pytest -q swingmaster/tests/test_apply_provider_vintage_for_reported_mismatch.py swingmaster/tests/test_dry_run_provider_vintage_for_reported_mismatch.py
```

Result:

- `21 passed`
