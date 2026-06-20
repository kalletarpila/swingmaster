# SwingMaster Reported Vintage Yahoo Fallback To Vintage Integration Phase 4H10

## Purpose

Phase 4H10 adds temp-DB integration coverage for the Yahoo bridge and Yahoo fallback-to-vintage contracts.

The phase proves the test-only Yahoo chain can run end to end without wiring Yahoo production CLIs, provider refresh, fallback production execution, quarter-update orchestration, real DB writes, TTM, scoring, valuation, UI, or ESS.

## Chain Tested

The integration test covers:

```text
Yahoo quarterly fixture rows / fallback audit fixtures
-> build_yahoo_source_hash(...)
-> build_yahoo_vintage_metadata(...)
-> write_yahoo_quarterly_rows_with_optional_vintage(...)
-> write_yahoo_fallback_enriched_rows_with_optional_vintage(...)
-> get_pit_quarterly_vintage(...)
-> get_quarterly_field_provenance(...)
```

All DB writes happen only in pytest temp DBs.

## Yahoo Bridge Behavior

The bridge test proves a Yahoo-derived normalized quarterly row can write:

- latest-compatible `rc_fundamental_quarterly`
- one `rc_fundamental_quarterly_vintage` row
- field provenance rows for non-null Yahoo fields

Bridge provenance uses:

- `source_provider = yahoo`
- `provenance_role = PROVIDER_REPORTED`
- `merge_action = YAHOO_BRIDGED`

## Yahoo Fallback Enrichment Behavior

The fallback test proves a mixed row can write latest, vintage, and field provenance with fixture fallback audit rows.

Audit-confirmed Yahoo-filled fields use:

- `source_provider = yahoo`
- `provenance_role = FALLBACK_REPORTED`
- `merge_action = YAHOO_FILLED_MISSING`

Explicit SEC-retained fields remain SEC-retained. Non-null fields without explicit source metadata are marked `unknown` by the current scaffold contract, not silently Yahoo.

## What Was Proven

The test proves:

- Yahoo bridge source hashes and statement vintage ids are deterministic
- Yahoo bridge source hashes and statement vintage ids change when payload hash changes
- fallback source hashes and statement vintage ids are deterministic
- fallback source hashes and statement vintage ids change when fallback-filled values change
- PIT reads return no row before `available_at_utc`
- PIT reads return the Yahoo vintage at and after `available_at_utc`
- missing fallback audit metadata fails safely with `ValueError`

## What Remains Not Wired

Production wiring is still not done.

This phase does not change:

- Yahoo raw audit defaults
- Yahoo quarterly write defaults
- Yahoo-to-generic bridge defaults
- Yahoo fallback enrich defaults
- `insert_quarterly_rows(...)`
- quarter update
- provider refresh
- real DB writes
- downstream readers, TTM, scoring, valuation, UI, or ESS

## Limitations

The integration test uses fixtures, not live Yahoo or yfinance provider calls.

`available_at_utc` remains explicit test input. Production must still define the real availability policy before any live Yahoo write path can be enabled.

The test validates representative bridge and fallback fixtures. Broader provider variability remains a later production-readiness concern.

## Recommended Next Phase

The next phase can add a narrow, explicitly opt-in Yahoo CLI experiment if it:

- remains default-off
- requires explicit availability inputs
- carries payload/source hash through the write boundary
- carries fallback audit rows and retained-field provenance through the write boundary
- writes only to temp or explicitly approved test DBs first
- does not wire full quarter-update orchestration
