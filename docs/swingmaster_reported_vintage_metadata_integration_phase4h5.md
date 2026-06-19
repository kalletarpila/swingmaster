# SwingMaster Reported Vintage Metadata Integration Phase 4H5

## Purpose

Phase 4H5 verifies that the test-only SEC and Yahoo metadata contracts can feed the opt-in reported quarterly vintage write adapter.

This phase is integration-test only. It does not wire SEC, Yahoo, fallback enrichment, refresh jobs, quarter-update orchestration, readers, UI, scoring, valuation, or ESS production paths to vintage writes.

## What Was Tested

The new temp-DB integration tests combine:

- SEC metadata contract helpers
- Yahoo/fallback metadata contract helpers
- `write_normalized_quarterly_rows_with_optional_vintage`
- reported vintage writer semantics
- reported vintage PIT reader
- field provenance reader

All tests use temporary SQLite DBs created through the existing migration helper.

## SEC-Only Integration Result

The SEC-only scenario builds a normalized quarterly row and fixture SEC contributing facts with encoded `field_name` metadata including `filed`.

The test verifies:

- SEC metadata builds a deterministic source hash and statement vintage id.
- Adapter writes latest, vintage, and field provenance rows.
- Latest-compatible `rc_fundamental_quarterly` is populated.
- PIT reader returns no row before `available_at_utc`.
- PIT reader returns the vintage row at/after `available_at_utc`.
- SEC provenance rows use `source_provider = sec_edgar`.
- SEC provenance rows use `provenance_role = PRIMARY_REPORTED`.
- SEC provenance rows use `merge_action = SEC_RETAINED`.

## Yahoo Bridge Integration Result

The Yahoo bridge scenario builds a Yahoo-derived normalized quarterly row and Yahoo quarterly staging fixture metadata.

The test verifies:

- Yahoo metadata builds a deterministic source hash and statement vintage id.
- The statement vintage id includes the `yahoo_to_generic_bridge` mode.
- Adapter writes latest, vintage, and Yahoo provenance rows.
- Yahoo provenance rows use `source_provider = yahoo`.
- Yahoo bridge provenance uses `merge_action = YAHOO_BRIDGED`.
- PIT reader returns the vintage at the explicit Yahoo availability cutoff.

## Mixed Fallback Integration Result

The mixed SEC + Yahoo fallback scenario builds a normalized row where SEC retains some fields and Yahoo fills missing fields through enrichment audit fixtures.

The test verifies:

- SEC-retained fields remain `source_provider = sec_edgar`.
- Yahoo-filled fields are marked `source_provider = yahoo`.
- Yahoo-filled fields use `merge_action = YAHOO_FILLED_MISSING`.
- SEC-retained fields are not incorrectly marked as Yahoo.
- PIT reader can read the mixed-source vintage row.

## PIT Read Behavior

Phase 4H5 explicitly verifies point-in-time behavior:

- before `available_at_utc`, no vintage is returned
- at or after `available_at_utc`, the expected vintage is returned

This confirms that the metadata contracts and adapter produce rows that the current vintage reader can consume with decision cutoffs.

## Adapter Safety Behavior

The integration tests also verify:

- incomplete metadata from helper output is rejected by the adapter
- missing `available_at_utc` raises `ValueError`
- missing `statement_vintage_id` raises `ValueError`
- duplicate `statement_vintage_id` raises SQLite integrity error
- default adapter mode remains latest-only when `write_vintage=False`

## Production Wiring Status

Production provider paths are still not wired.

The following paths remain unchanged:

- SEC raw bootstrap
- SEC reconstruction
- normalized quarterly builder
- Yahoo raw audit
- Yahoo quarterly staging
- Yahoo-to-generic bridge
- Yahoo fallback enrichment
- quarter-update orchestration
- TTM/scoring/valuation/readers/UI/ESS

## Recommended Next Phase

The next phase should decide whether to add a narrow, explicitly opt-in production wiring experiment.

Recommended constraints for that later phase:

- start with a single narrow path, not full quarter-update orchestration
- require explicit `available_at_utc` policy
- require selected-fact/source metadata at the write boundary
- preserve latest-table compatibility
- keep provider refresh, scoring, valuation, and ESS wiring out of scope unless separately approved
