# SwingMaster Reported Vintage Quarter Update Yahoo Fallback Forwarding Phase 4I8

## Purpose

Phase 4I8 adds a default-off quarter_update forwarding path for Yahoo fallback vintage metadata.

The scope is intentionally narrow:

- Yahoo fallback enrich only
- mocked quarter_update tests only
- no Yahoo bridge forwarding
- no combined SEC + Yahoo mode
- no final mixed-vintage orchestration beyond forwarding metadata
- no scheduler, UI, ESS, valuation, TTM, scoring, or percentile changes

## New Mode

`run_fundamental_quarter_update.py` now accepts:

```text
--vintage-mode yahoo_fallback_only
```

The existing `validation_only` and `sec_reconstruct_only` modes remain supported.

`yahoo_fallback_only` is valid only with `--write-vintage`. It still requires explicit:

- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`

`--vintage-normalization-run-id` remains optional.

## Default Behavior

Default behavior is unchanged. Without `--write-vintage`, quarter_update does not include vintage summary fields and does not pass vintage metadata to child steps.

`validation_only` keeps execution disabled. `sec_reconstruct_only` remains SEC-only.

## Yahoo Fallback Forwarding Behavior

When `--write-vintage --vintage-mode yahoo_fallback_only` is used, quarter_update validates explicit PIT metadata and forwards Yahoo fallback metadata to `run_yahoo_fallback_enrich(...)`:

- `write_vintage=True`
- `vintage_market`
- `vintage_available_at_utc`
- `vintage_ingested_at_utc`
- `vintage_run_id`
- `vintage_normalization_run_id`

The existing child run id remains:

```text
BASE__ENRICH
```

Quarter_update uses the existing Python helper path, not a subprocess CLI call.

## What Is Not Forwarded

Vintage metadata is not forwarded to:

- SEC reconstruct
- Yahoo audit
- Yahoo quarterly write
- Yahoo-to-generic bridge
- TTM
- lifecycle
- scoring
- ack
- valuation

No combined SEC + Yahoo mode and no final mixed-vintage orchestration is implemented in this phase.

## Summary Fields

For `yahoo_fallback_only`, summary includes:

- `vintage_requested=True`
- `vintage_execution_enabled=True`
- `vintage_mode=yahoo_fallback_only`
- `vintage_validation_status=OK`
- `vintage_sec_reconstruct_requested=False`
- `vintage_yahoo_bridge_requested=False`
- `vintage_yahoo_fallback_requested=True`
- `vintage_rows_inserted=None`
- `vintage_provenance_rows_inserted=None`
- `vintage_count_status=not_reported_by_child`

Counts remain unavailable because the fallback child summary currently reports fallback rows/fields, not vintage/provenance insert counts.

## Test And Production Status

The quarter_update Yahoo fallback forwarding behavior is covered by mocked tests. The tests assert that fallback metadata is forwarded, SEC reconstruct receives no fallback-mode vintage metadata, validation failures happen before child steps, and default/validation-only behavior remains compatible.

This phase does not run providers, scheduler jobs, refresh jobs against real data, or real DB writes. Production execution remains explicit opt-in and should not be enabled operationally until a later preflight validates the behavior safely.

## Recommended Next Phase

Recommended Phase 4I9: design or test an explicit combined-mode policy before adding any SEC + Yahoo fallback orchestration.

The next phase should decide whether quarter_update should write separate provider-stage vintages, one final mixed vintage, or keep SEC and Yahoo fallback modes mutually exclusive.
