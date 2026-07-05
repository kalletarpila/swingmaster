# SwingMaster Reported Vintage Quarter Update Combined Planning Phase 4I10

## Purpose

Phase 4I10 adds a default-off planning-only quarter_update mode for combined SEC + Yahoo fallback vintage orchestration.

The mode establishes validation and summary semantics for a future combined flow, but does not execute vintage writes and does not create final mixed vintage rows.

## New Mode

`run_fundamental_quarter_update.py` now accepts:

```text
--vintage-mode sec_plus_yahoo_fallback_planning
```

The existing modes remain supported:

- `validation_only`
- `sec_reconstruct_only`
- `yahoo_fallback_only`

## Default Behavior

Default behavior is unchanged. Without `--write-vintage`, quarter_update omits vintage summary fields and does not pass vintage metadata to child steps.

The new planning mode is valid only with `--write-vintage`.

## Required Metadata

`sec_plus_yahoo_fallback_planning` requires explicit:

- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`

`--vintage-normalization-run-id` remains optional. Availability is not inferred from `period_end_date`, and required vintage timestamps are not read from the current clock.

Validation happens before eligible rows are loaded and before ticker child steps can run.

## Planning Summary Fields

For the planning mode, summary includes:

- `vintage_requested=True`
- `vintage_mode=sec_plus_yahoo_fallback_planning`
- `vintage_execution_enabled=False`
- `vintage_planning_only=True`
- `vintage_validation_status=OK`
- `vintage_sec_reconstruct_requested=True`
- `vintage_yahoo_fallback_requested=True`
- `vintage_yahoo_bridge_requested=False`
- `vintage_final_mixed_planned=True`
- `vintage_final_mixed_written=False`
- `vintage_rows_inserted=0`
- `vintage_provenance_rows_inserted=0`
- `vintage_rows_skipped_noop=0`
- `vintage_rows_failed=0`
- `vintage_count_status=planning_only_no_execution`

## Vintage Execution Status

No vintage execution happens in Phase 4I10.

The planning mode does not pass vintage metadata to:

- SEC reconstruct
- Yahoo fallback enrich
- Yahoo bridge
- TTM
- lifecycle
- scoring
- ack
- valuation

It does not write final mixed vintage rows.

## Why Final Mixed Vintage Remains Later

Final mixed vintage creation still needs a dedicated implementation phase because it must define:

- deterministic final mixed `statement_vintage_id`
- final mixed source hash inputs
- SEC-retained field provenance
- Yahoo-filled field provenance
- missing-quarter insert policy
- no-op fallback behavior
- duplicate prevention and failure behavior

## Recommended Next Phase

Recommended Phase 4I11: temp-DB final mixed vintage creation design or mocked implementation tests, still default-off and without providers, scheduler runs, or real DB writes.

## Phase 4I11 Builder Reference

Phase 4I11 implements the recommended test-only final mixed builder contract in [Reported Vintage Final Mixed Builder Phase 4I11](swingmaster_reported_vintage_final_mixed_builder_phase4i11.md).

The planning mode remains unchanged: it still writes no vintage rows and does not call the final mixed builder.

## Phase 4I12 Planning Reference

Phase 4I12 adds planning-only final mixed builder integration in [Reported Vintage Quarter Update Final Mixed Planning Phase 4I12](swingmaster_reported_vintage_quarter_update_final_mixed_planning_phase4i12.md).

The planning mode remains no-execution. Its live summary exposes null final mixed plan details until real per-period plan input is available.

## Phase 4I15 Wiring Reference

Phase 4I15 adds a separate default-off final mixed execution mode, documented in [Reported Vintage Quarter Update Final Mixed Execution Wiring Phase 4I15](swingmaster_reported_vintage_quarter_update_final_mixed_execution_wiring_phase4i15.md).

The existing `sec_plus_yahoo_fallback_planning` mode remains planning-only and unchanged.
