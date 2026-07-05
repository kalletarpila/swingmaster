# SwingMaster Reported Vintage Final Mixed Execution Helper Phase 4I14

Phase 4I14 adds a temp-DB/mocked execution helper for writing one final mixed SEC + Yahoo fallback vintage through existing reported vintage contracts.

## Purpose

The helper proves that the Phase 4I11 final mixed builder output can be applied through the existing opt-in quarterly dual-write adapter.

This is not production quarter_update wiring. The helper requires a caller-supplied SQLite connection and is tested only against temp DBs.

## Helper Behavior

`execute_final_mixed_vintage_write(...)`:

- accepts one normalized quarterly row
- accepts explicit market, availability timestamp, ingestion timestamp, and run id
- accepts SEC and Yahoo field source maps plus optional fallback audit rows
- computes final mixed source hash
- computes deterministic `mixed_sec_yahoo` statement vintage id
- builds final mixed metadata
- merges SEC/Yahoo/unknown field provenance
- calls `write_normalized_quarterly_rows_with_optional_vintage(...)`
- returns an execution summary

The helper does not open DB paths, call providers, run schedulers, or infer PIT timestamps.

## Required Inputs

Required:

- caller-supplied `sqlite3.Connection`
- normalized row with `ticker` and `period_end_date`
- `market`
- `available_at_utc`
- `ingested_at_utc`
- `run_id`

Optional:

- SEC field source map
- Yahoo field source map
- fallback audit rows
- normalization run id

Missing explicit PIT metadata raises `ValueError` through the final mixed metadata builder.

## Summary Output

The helper returns:

```text
final_mixed_written
statement_vintage_id
source_hash
vintage_rows_inserted
provenance_rows_inserted
provenance_field_count
skipped_noop
already_known
error
```

Phase 4I14 also adds a small pure `build_final_mixed_execution_summary(...)` helper for future quarter_update summary shaping tests.

## PIT Behavior

Temp-DB tests verify that the reported vintage PIT reader:

- returns the final mixed vintage at or after `available_at_utc`
- returns `None` before `available_at_utc`

Field provenance rows preserve SEC-retained, Yahoo-filled, and unknown non-null fields.

## Duplicate And No-Op Limitations

This phase does not implement no-op or already-known detection.

If the same deterministic `statement_vintage_id` is inserted twice, SQLite raises `sqlite3.IntegrityError`. The helper does not use `INSERT OR REPLACE` and does not silently replace vintage rows.

## Production Wiring Status

Not wired.

Phase 4I14 does not modify quarter_update production execution, vintage modes, SEC/Yahoo CLIs, provider jobs, scheduler behavior, TTM, scoring, valuation, UI, or ESS.

## Recommended Next Phase

Recommended Phase 4I15: mocked quarter_update integration around the execution helper summary and data collection contract, still without real DB/provider production execution.

## Phase 4I15 Wiring Reference

Phase 4I15 adds default-off mocked quarter_update wiring for final mixed execution in [Reported Vintage Quarter Update Final Mixed Execution Wiring Phase 4I15](swingmaster_reported_vintage_quarter_update_final_mixed_execution_wiring_phase4i15.md).

The wiring requires an injected runner in tests and remains unavailable for production CLI execution.
