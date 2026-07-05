# SwingMaster Reported Vintage Quarter Update Final Mixed Runner Phase 4J0

Phase 4J0 adds a production-safe runner function for quarter_update final mixed vintage execution.

## Purpose

The runner closes the gap left by Phase 4I15: quarter_update can now use a real final mixed runner when complete final mixed inputs are supplied by an internal/test seam.

This phase does not run the mode against the real DB.

## What The Runner Does

`run_final_mixed_vintage_execution_for_ticker(...)`:

- requires a caller-supplied `sqlite3.Connection`
- requires ticker, market, normalized row, explicit PIT timestamps, and run id
- accepts SEC field source map, Yahoo field source map, and fallback audit rows
- validates ticker and required normalized row fields before write
- delegates the actual write to `execute_final_mixed_vintage_write(...)`
- returns the existing final mixed execution summary shape

The runner does not open DB paths and does not call providers.

## Quarter_Update Behavior

`sec_plus_yahoo_fallback_final_mixed` remains default-off and requires `--write-vintage` plus explicit PIT metadata.

For non-dry execution:

- if a test-injected `final_mixed_execution_runner` is supplied, quarter_update uses it
- otherwise quarter_update can use the production-safe runner only when `final_mixed_inputs_by_key` is supplied
- if neither runner nor inputs are available, quarter_update fails before child steps with `FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_INPUTS_REQUIRED`

This prevents accidental provider execution or partial writes when real final mixed inputs are not yet collected by the live flow.

## Required Inputs

The input seam must provide per ticker/period:

- `normalized_row`
- `sec_field_source_map`
- `yahoo_field_source_map`
- `fallback_audit_rows`

The key can be either:

- `(market, ticker, period_end_date)`
- `(ticker, period_end_date)`

## Summary Behavior

The runner output is merged into quarter_update summary fields:

- `vintage_final_mixed_written`
- `vintage_final_mixed_rows_inserted`
- `vintage_final_mixed_provenance_rows_inserted`
- `vintage_rows_skipped_noop`
- `vintage_final_mixed_rows_already_known`
- `vintage_rows_failed`
- `vintage_count_status`
- `vintage_error_summary`

Mocked success, no-op, and failure paths remain supported.

## Why No Real DB Run Was Done

The live quarter_update flow still does not collect complete SEC/Yahoo/fallback provenance inputs from real child steps.

Running against `/home/kalle/projects/swingmaster/fundamentals_usa.db` would risk an incomplete final mixed row or provider-side effects. This phase therefore uses only temp DBs and mocks.

## Criteria For Next Single-Ticker Real Run

Before a guarded single-ticker real DB test, the next phase must prove:

- final mixed inputs can be collected from real child step outputs without provider/network surprises
- the exact ticker/period is explicitly selected
- dry-run/preflight shows the row and provenance that would be written
- backup/rollback procedure is documented
- duplicate/no-op behavior is understood for the target ticker/period

## Phase 4J1 Preflight Reference

Phase 4J1 adds a read-only single-ticker preflight in [Reported Vintage Single Ticker Final Mixed Preflight Phase 4J1](swingmaster_reported_vintage_single_ticker_final_mixed_preflight_phase4j1.md).

The first real DB smoke classified ticker `A` as `INPUTS_INCOMPLETE_FOR_TRUE_FINAL_MIXED`, so no guarded write is recommended yet.
