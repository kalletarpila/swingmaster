# SwingMaster Reported Vintage Quarter Update Final Mixed Execution Wiring Phase 4I15

Phase 4I15 adds a default-off quarter_update vintage mode and mocked execution seam for future final mixed vintage execution.

## Purpose

The purpose is to prove that quarter_update can validate and summarize a future final mixed execution path without running real providers or writing the real DB.

This phase does not make production final mixed execution available.

## New Mode

New explicit mode:

```text
sec_plus_yahoo_fallback_final_mixed
```

The mode:

- requires `--write-vintage`
- requires explicit vintage market, availability timestamp, ingestion timestamp, and run id
- keeps default quarter_update behavior unchanged
- is not selected unless explicitly requested
- does not infer availability from period end date or current clock

## What Is Wired

quarter_update now recognizes the new mode and prepares final mixed summary fields.

For non-dry mocked tests, the mode requires an injected `final_mixed_execution_runner`. This runner seam lets tests prove summary wiring without real provider calls or production DB writes.

Without that injected runner, non-dry final mixed mode fails before child steps with:

```text
FUNDAMENTAL_QUARTER_UPDATE_FINAL_MIXED_RUNNER_REQUIRED
```

## What Is Still Mocked/Test-Only

The live quarter_update flow still does not collect real final mixed inputs:

- final normalized row after SEC + Yahoo fallback
- SEC field provenance
- Yahoo fallback audit/provenance rows
- missing-quarter insert metadata

The injected runner is used only by tests. CLI execution has no production runner.

## Summary Behavior

The new mode reports:

- `vintage_requested=True`
- `vintage_mode=sec_plus_yahoo_fallback_final_mixed`
- `vintage_execution_enabled=True`
- `vintage_planning_only=False`
- `vintage_sec_reconstruct_requested=True`
- `vintage_yahoo_fallback_requested=True`
- `vintage_final_mixed_planned=True`
- `vintage_final_mixed_written`
- `vintage_final_mixed_rows_inserted`
- `vintage_final_mixed_provenance_rows_inserted`
- `vintage_rows_skipped_noop`
- `vintage_final_mixed_rows_already_known`
- `vintage_rows_failed`
- `vintage_count_status`
- `vintage_error_summary`

Mocked success and no-op runner outputs update these fields in tests. Mocked failures are surfaced as controlled ticker failures.

## Real DB And Provider Status

No real DB writes are performed by this phase.

No SEC, Yahoo, yfinance, Finnhub, paid-provider, scheduler, or refresh job is run by the tests. The production CLI still has no final mixed execution runner.

## Recommended Next Phase

Recommended Phase 4I16: implement mocked data collection contracts for final normalized rows and provenance handoff, still without real provider execution.

Real production final mixed execution should wait until quarter_update can collect complete SEC/Yahoo/fallback inputs and duplicate/no-op behavior is proven end to end.
