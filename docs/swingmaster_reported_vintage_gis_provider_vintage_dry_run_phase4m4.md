# SwingMaster GIS Provider Vintage Dry Run Phase 4M4

Date: 2026-07-07

## Purpose

Phase 4M4 adds a dry-run-only provider-derived vintage candidate check for the single known mismatch:

- market: `usa`
- ticker: `GIS`
- period_end_date: `2025-05-25`
- field: `total_debt`

The phase does not run quarter_update, providers, scheduler jobs, refresh jobs, recovery, Yahoo-aware apply, or any real DB write.

## CLI

Added:

```bash
python3 -m swingmaster.cli.dry_run_provider_vintage_for_reported_mismatch
```

The CLI:

- opens the fundamentals DB with SQLite URI `mode=ro`
- sets `PRAGMA query_only=ON`
- reads the latest reported row, PIT-visible vintage row, and existing SEC raw rows
- builds a candidate with the existing SEC latest-writer vintage helper
- reports candidate vintage/provenance rows without inserting them
- returns a blocked status for missing latest rows, missing SEC evidence, and duplicate candidate vintage IDs

## Real DB Dry Run

Command run:

```bash
python3 -m swingmaster.cli.dry_run_provider_vintage_for_reported_mismatch \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --ticker GIS \
  --period-end-date 2025-05-25 \
  --available-at-utc 2026-07-07T20:04:15Z \
  --ingested-at-utc 2026-07-07T20:04:15Z \
  --vintage-run-id USA_QUARTER_UPDATE_2026-07-07__GIS_TOTAL_DEBT_PROVIDER_VINTAGE_DRY_RUN \
  --format json
```

Result:

- overall_status: `DRY_RUN_READY_WITH_UNKNOWN_PROVENANCE`
- planned_vintage_rows: `1`
- planned_provenance_rows: `11`
- duplicate_candidate_statement_vintage_id_count: `0`
- sec_raw_fact_count: `69`

Latest/current reported value:

- `total_debt=14878600000.0`

PIT-visible legacy vintage value:

- statement_vintage_id: `legacy:usa:GIS:2025-05-25:9441b8313c7894e3`
- run_id: `reported-vintage-legacy-backfill:usa:2026-06-19:2026-06-19T14:28:43Z`
- available_at_utc: `2026-06-19T00:00:00Z`
- `total_debt=677000000.0`

Candidate value:

- statement_vintage_id: `sec_edgar:usa:GIS:2025-05-25:dc65e8c864b43e37`
- source_provider: `sec_edgar`
- source_hash: `dc65e8c864b43e37bac1e80488707c4b77ea768d0bfe47ccda81223e0eb13f0b`
- filed_at_utc: `2026-07-01`
- provider_run_id: `USA_QUARTER_UPDATE_2026-07-07__QUARTERLY__QUARTERLY`
- `total_debt=14878600000.0`

SEC component evidence:

- `LongTermDebtCurrent=1528400000.0`
- `LongTermDebtNoncurrent=12673200000.0`
- `ShortTermBorrowings=677000000.0`
- component sum: `14878600000.0`

The candidate therefore matches the current latest reported value and fixes the visible legacy value mismatch at the row/value level.

## Important Blocker

The dry-run candidate is not clean enough for a writing phase yet:

- unknown_provenance_count: `1`
- unknown_provenance_fields: `total_debt`

The existing SEC latest-writer helper builds `total_debt=14878600000.0`, but its provenance row is:

- source_provider: `unknown`
- source_table: `null`
- source_row_ref: `null`
- merge_action: `SOURCE_NOT_PROVIDED`
- provenance_role: `UNKNOWN_RETAINED`

This means the next writing phase should not simply apply the candidate as-is. It should first make the total debt provenance explicit from the three SEC debt components, then rerun this dry-run and require `DRY_RUN_READY`.

## Phase 4M5 Follow-Up

Phase 4M5 fixed the SEC latest-writer helper so derived `total_debt` can receive SEC component provenance when the latest value equals a supported same-period debt component sum.

Rerunning the GIS dry-run changed the result to:

- overall_status: `DRY_RUN_READY`
- unknown_provenance_count: `0`
- unknown_provenance_fields: `[]`
- planned_vintage_rows: `1`
- planned_provenance_rows: `11`
- candidate statement_vintage_id: `sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0`
- candidate `total_debt=14878600000.0`

The `total_debt` provenance now uses:

- source_provider: `sec_edgar`
- provenance_role: `PRIMARY_REPORTED`
- merge_action: `SEC_RETAINED`
- source_row_ref containing `LongTermDebtCurrent`, `LongTermDebtNoncurrent`, and `ShortTermBorrowings`

See `docs/swingmaster_reported_vintage_sec_derived_total_debt_provenance_phase4m5.md`.

## Original Recommendation

At the end of Phase 4M4, the next phase was expected to be narrow and GIS-only:

1. Enhance SEC latest-writer provenance for derived `total_debt` so it references `LongTermDebtCurrent`, `LongTermDebtNoncurrent`, and `ShortTermBorrowings`.
2. Keep the dry-run status blocked/review-required while any candidate field has unknown provenance.
3. Rerun the Phase 4M4 dry-run and require:
   - `planned_vintage_rows=1`
   - `planned_provenance_rows>0`
   - `unknown_provenance_count=0`
   - `duplicate_candidate_statement_vintage_id_count=0`
4. Only after that, consider a separate explicit approval/write phase for this single GIS vintage supersession.

Do not use Yahoo-aware final-mixed apply, broad recovery, scheduler, refresh, or provider calls for this mismatch.

After Phase 4M5, items 1-3 are complete and the current recommendation is a separate guarded one-row apply phase for GIS only, with backup and post-write verification.

## Verification

Checks run:

```bash
python3 -m py_compile swingmaster/cli/dry_run_provider_vintage_for_reported_mismatch.py
python3 -m pytest -q swingmaster/tests/test_dry_run_provider_vintage_for_reported_mismatch.py
```

Result:

- `10 passed`
