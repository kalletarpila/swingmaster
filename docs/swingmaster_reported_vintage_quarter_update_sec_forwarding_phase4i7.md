# SwingMaster Reported Vintage Quarter Update SEC Forwarding Phase 4I7

## Purpose

Phase 4I7 adds the first default-off quarter_update forwarding path for reported vintage metadata.

The scope is intentionally narrow:

- SEC reconstruct only
- mocked quarter_update tests only
- no Yahoo bridge forwarding
- no Yahoo fallback forwarding
- no final mixed SEC + Yahoo vintage orchestration
- no scheduler, UI, ESS, valuation, TTM, scoring, or percentile changes

## New Mode

`run_fundamental_quarter_update.py` now accepts:

```text
--vintage-mode sec_reconstruct_only
```

The existing `validation_only` mode remains supported.

`sec_reconstruct_only` is valid only with `--write-vintage`. It still requires explicit:

- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`

`--vintage-normalization-run-id` remains optional.

## Default Behavior

Default behavior is unchanged. Without `--write-vintage`, quarter_update does not include vintage summary fields and does not pass vintage metadata to child steps.

`validation_only` also keeps execution disabled and does not pass vintage metadata to SEC or Yahoo child steps.

## SEC Forwarding Behavior

When `--write-vintage --vintage-mode sec_reconstruct_only` is used, quarter_update validates the explicit PIT metadata and builds SEC-only forwarding metadata:

- `write_vintage=True`
- `vintage_market`
- `vintage_available_at_utc`
- `vintage_ingested_at_utc`
- `vintage_run_id`
- `vintage_normalization_run_id`

If the USA SEC refresh path is required, quarter_update forwards this metadata to the SEC reconstruct helper through `run_sec_reconstruct_step(...)`.

Quarter_update does not call the SEC reconstruct CLI by subprocess. It forwards to the repo's SEC reconstruct Python function path through a small helper. The child run id uses the existing pipeline-style convention:

```text
BASE__SEC_QUARTERLY_RECON
```

The existing SEC raw bootstrap and generic quarterly build calls remain in place.

## What Is Not Forwarded

Vintage metadata is not forwarded to:

- Yahoo audit
- Yahoo quarterly write
- Yahoo-to-generic bridge
- Yahoo fallback enrich
- TTM
- lifecycle
- scoring
- ack
- valuation

No final mixed SEC + Yahoo vintage row is created in this phase.

## Summary Fields

For `sec_reconstruct_only`, summary includes:

- `vintage_requested=True`
- `vintage_execution_enabled=True`
- `vintage_mode=sec_reconstruct_only`
- `vintage_validation_status=OK`
- `vintage_sec_reconstruct_requested=True`
- `vintage_yahoo_bridge_requested=False`
- `vintage_yahoo_fallback_requested=False`
- `vintage_rows_inserted=None`
- `vintage_provenance_rows_inserted=None`
- `vintage_count_status=not_reported_by_child`

Counts remain unavailable because the SEC reconstruct child function currently returns reconstructed-row information, not vintage/provenance insert counts.

## Test And Production Status

The quarter_update forwarding behavior is covered by mocked tests. The tests assert that SEC metadata is forwarded, Yahoo fallback receives no vintage metadata, validation failures happen before child steps, and default/validation-only behavior remains compatible.

This phase does not run providers, scheduler jobs, or real DB refreshes. Real production execution remains default-off and should not be enabled until a later phase validates operational behavior explicitly.

## Recommended Next Phase

Recommended Phase 4I8: add a temp-DB integration preflight for the SEC-only quarter_update path, still without providers, scheduler, or real DB writes.

Yahoo fallback and final mixed-vintage orchestration should remain separate until duplicate, no-op, and final-vs-intermediate semantics are explicitly tested.

## Phase 4I8 Status

Phase 4I8 adds default-off Yahoo fallback forwarding, documented in [Reported Vintage Quarter Update Yahoo Fallback Forwarding Phase 4I8](swingmaster_reported_vintage_quarter_update_yahoo_fallback_forwarding_phase4i8.md).

`sec_reconstruct_only` remains unchanged and SEC-only. `yahoo_fallback_only` is a separate mode; no combined SEC + Yahoo or final mixed-vintage orchestration is implemented.
