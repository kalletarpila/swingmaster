# SwingMaster SEC Derived Total Debt Provenance Phase 4M5

Date: 2026-07-07

## Purpose

Phase 4M5 fixes SEC latest-writer vintage provenance for derived `total_debt` values. The immediate target is the GIS mismatch:

- ticker: `GIS`
- period_end_date: `2025-05-25`
- latest `total_debt=14878600000.0`
- visible legacy vintage `total_debt=677000000.0`

No quarter_update, providers, scheduler, refresh job, recovery, apply, backfill, Yahoo-aware apply, or real DB write was run.

## Policy

The latest-writer vintage helper can now prove `total_debt` from same-period SEC component facts when the latest value equals a supported deterministic component sum.

Priority order:

1. `LongTermDebtCurrent + LongTermDebtNoncurrent + ShortTermBorrowings`
2. Existing SEC reconstruction debt groups from `DEBT_GROUPS`

The first priority handles GIS, where current debt, noncurrent debt, and short-term borrowings together explain the latest `total_debt`.

If no supported component group exactly matches the latest value, the field remains unknown. The helper does not change latest values, does not change debt calculation semantics, and does not invent Yahoo provenance.

## Implementation

Updated:

- `swingmaster/fundamentals/reported_sec_latest_writer_vintage.py`

Added helper:

- `build_sec_component_provenance_for_derived_field(...)`

The helper:

- supports `total_debt`
- uses SEC facts already filtered to the same ticker, period, and provider
- groups facts by ticker, period, unit, fiscal year, and fiscal period
- chooses component groups deterministically
- tolerates older short SEC field-name strings used by existing tests
- returns `None` when the component evidence does not exactly explain the latest value

The schema does not have a separate `SEC_COMPONENT_SUM` provenance enum. The provenance row therefore uses existing SEC field provenance fields:

- source_provider: `sec_edgar`
- provenance_role: `PRIMARY_REPORTED`
- merge_action: `SEC_RETAINED`
- source_table: `rc_fundamental_statement_raw`
- source_row_ref: joined SEC component fact names

## Real DB Dry Run

Command run:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_provider_vintage_for_reported_mismatch \
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

- overall_status: `DRY_RUN_READY`
- planned_vintage_rows: `1`
- planned_provenance_rows: `11`
- unknown_provenance_count: `0`
- unknown_provenance_fields: `[]`
- duplicate_candidate_statement_vintage_id_count: `0`
- sec_raw_fact_count: `69`

Candidate:

- statement_vintage_id: `sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0`
- source_hash: `9a81f59e8511cac04d5872635a7b11a10f97aaacbb8e715d858a2737b4388521`
- source_provider: `sec_edgar`
- `total_debt=14878600000.0`

SEC component evidence:

- `LongTermDebtCurrent=1528400000.0`
- `LongTermDebtNoncurrent=12673200000.0`
- `ShortTermBorrowings=677000000.0`
- component sum: `14878600000.0`

The `total_debt` provenance row is now SEC-backed:

- source_provider: `sec_edgar`
- provenance_role: `PRIMARY_REPORTED`
- merge_action: `SEC_RETAINED`
- source_row_ref includes:
  - `LongTermDebtCurrent|form=10-K|unit=USD|fy=2026|fp=FY|frame=CY2025Q2I|start=NULL|filed=2026-07-01`
  - `LongTermDebtNoncurrent|form=10-K|unit=USD|fy=2026|fp=FY|frame=CY2025Q2I|start=NULL|filed=2026-07-01`
  - `ShortTermBorrowings|form=10-K|unit=USD|fy=2026|fp=FY|frame=CY2025Q2I|start=NULL|filed=2026-07-01`

## Status Change

Before Phase 4M5:

- `DRY_RUN_READY_WITH_UNKNOWN_PROVENANCE`
- unknown field: `total_debt`

After Phase 4M5:

- `DRY_RUN_READY`
- unknown fields: none

## Recommended Next Phase

The next phase can be a guarded one-row real DB apply for GIS provider-derived vintage, with:

- explicit approval token
- real DB backup
- insert of exactly one `rc_fundamental_quarterly_vintage` row
- insert of its field provenance rows
- post-write parity and duplicate verification
- no broad Yahoo-aware apply or recovery

Do not run a write/apply phase without a separate explicit prompt.

## Phase 4M6 Follow-Up

Phase 4M6 completed the guarded one-row real DB apply for GIS:

- inserted vintage rows: `1`
- inserted provenance rows: `11`
- statement_vintage_id: `sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0`
- post-apply PIT at `2026-07-07T20:04:15Z`: new SEC provider-derived vintage
- post-apply scope diagnostic: `NO_MISMATCH`

See `docs/swingmaster_reported_vintage_gis_provider_vintage_apply_phase4m6.md`.

## Verification

Checks run:

```bash
python3 -m py_compile swingmaster/fundamentals/reported_sec_latest_writer_vintage.py swingmaster/cli/dry_run_provider_vintage_for_reported_mismatch.py
python3 -m pytest -q swingmaster/tests/test_reported_sec_latest_writer_vintage.py
python3 -m pytest -q swingmaster/tests/test_dry_run_provider_vintage_for_reported_mismatch.py
python3 -m pytest -q swingmaster/tests/test_quarter_update_sec_latest_writer_vintage.py
```

Results:

- `9 passed`
- `11 passed`
- `6 passed`
