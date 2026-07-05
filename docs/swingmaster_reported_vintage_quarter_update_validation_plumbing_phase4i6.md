# SwingMaster Reported Vintage Quarter Update Validation Plumbing Phase 4I6

## Purpose

Phase 4I6 adds default-off vintage option parsing, metadata validation, and summary plumbing to `run_fundamental_quarter_update.py`.

This phase does not execute vintage writes. It does not pass vintage flags to SEC, Yahoo bridge, Yahoo fallback, provider, scheduler, TTM, scoring, valuation, UI, or ESS paths.

## Flags Added

The quarter update CLI now accepts:

- `--write-vintage`
- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`
- `--vintage-normalization-run-id`
- `--vintage-mode validation_only`

Only `validation_only` is supported in Phase 4I6.

## Validation Behavior

When `--write-vintage` is absent, existing quarter update behavior and summary output remain compatible.

When `--write-vintage` is present, validation requires:

- non-empty `--vintage-market`
- non-empty `--vintage-available-at-utc`
- non-empty `--vintage-ingested-at-utc`
- non-empty `--vintage-run-id`
- `--vintage-mode validation_only`

The availability and ingestion timestamps must use explicit UTC text in `YYYY-MM-DDTHH:MM:SSZ` form. The code does not infer `available_at_utc` from `period_end_date` and does not use the current clock for vintage metadata.

Validation runs before eligible rows are loaded and before any ticker child step can run. A validation failure exits with a clear `FUNDAMENTAL_QUARTER_UPDATE_VINTAGE_*` error.

## Summary Fields

When `--write-vintage` validates successfully, the summary includes:

- `vintage_requested=True`
- `vintage_mode=validation_only`
- `vintage_execution_enabled=False`
- `vintage_validation_status=OK`
- `vintage_rows_inserted=0`
- `vintage_provenance_rows_inserted=0`
- `vintage_rows_skipped_noop=0`
- `vintage_rows_failed=0`
- `vintage_error_summary=None`

When `--write-vintage` is absent, these fields are omitted to preserve existing output compatibility.

## Vintage Execution Status

Vintage execution remains disabled. Quarter update still does not pass vintage arguments to:

- SEC raw bootstrap
- SEC quarterly build
- Yahoo audit
- Yahoo quarterly write
- Yahoo-to-generic bridge
- Yahoo fallback enrich

No vintage rows or provenance rows are written by quarter update in this phase.

## Recommended Next Phase

Recommended Phase 4I7: add mocked SEC-only vintage handoff design or implementation tests, still default-off and still without real providers, scheduler runs, or real DB writes.

Full SEC + Yahoo fallback mixed-vintage orchestration should wait until duplicate, no-op, final-vs-intermediate, and availability timestamp semantics are proven.

## Phase 4I7 Status

Phase 4I7 adds default-off SEC-only forwarding, documented in [Reported Vintage Quarter Update SEC Forwarding Phase 4I7](swingmaster_reported_vintage_quarter_update_sec_forwarding_phase4i7.md).

The `validation_only` behavior remains unchanged. The new `sec_reconstruct_only` mode forwards validated metadata only to the SEC reconstruct helper and still does not forward anything to Yahoo bridge or Yahoo fallback paths.

## Phase 4I8 Status

Phase 4I8 adds default-off Yahoo fallback forwarding, documented in [Reported Vintage Quarter Update Yahoo Fallback Forwarding Phase 4I8](swingmaster_reported_vintage_quarter_update_yahoo_fallback_forwarding_phase4i8.md).

The `validation_only` behavior remains unchanged. The new `yahoo_fallback_only` mode forwards validated metadata only to `run_yahoo_fallback_enrich(...)` and does not forward anything to SEC reconstruct or Yahoo bridge paths.

## Phase 4I10 Status

Phase 4I10 adds a planning-only combined mode, documented in [Reported Vintage Quarter Update Combined Planning Phase 4I10](swingmaster_reported_vintage_quarter_update_combined_planning_phase4i10.md).

The mode validates explicit PIT metadata and marks SEC + Yahoo fallback planning in summary, but does not forward vintage metadata to child paths or write vintage rows.
