# SwingMaster Reported Vintage Yahoo Metadata Contract Phase 4H4

## Purpose

Phase 4H4 defines a pure, test-only Yahoo/fallback metadata contract for reported quarterly vintage writes.

It does not wire Yahoo audit, Yahoo quarterly staging, Yahoo-to-generic bridge, fallback enrichment, refresh jobs, quarter-update orchestration, TTM, scoring, valuation, readers, UI, or ESS to reported-vintage writes.

The implementation lives in `swingmaster/fundamentals/reported_yahoo_vintage_metadata.py` and is intended as future input for the Phase 4H2 opt-in adapter.

## Current Yahoo/Fallback Write Paths

Current repo evidence shows these Yahoo-related paths:

- `run_fundamental_yahoo_audit.py` fetches provider payloads and writes `rc_fundamental_yahoo_raw`.
- `run_fundamental_yahoo_quarterly_write.py` normalizes latest raw Yahoo payload into `rc_fundamental_yahoo_quarterly`.
- `run_fundamental_yahoo_to_quarterly.py` bridges Yahoo quarterly staging rows into latest-compatible `rc_fundamental_quarterly`.
- `run_fundamental_yahoo_fallback_enrich.py` fills missing fields in existing generic quarterly rows and writes `rc_fundamental_quarterly_enrichment_audit`.
- `run_usa_enrichment_batch.py` and `run_fundamental_quarter_update.py` orchestrate those steps but remain unwired to vintage writes.

## Yahoo Metadata Available Today

Yahoo raw audit rows include:

- `market`
- `provider = yahoo`
- `symbol`
- statement JSON payloads
- `payload_hash`
- status and error message
- `loaded_at_utc`
- `run_id`

Yahoo quarterly staging rows include:

- `market`
- `symbol`
- `period_end_date`
- normalized quarterly values
- `shares_source`
- `shares_quality`
- `source_run_id`
- `run_id`
- `created_at_utc`

Fallback enrichment audit rows include:

- `ticker`
- `period_end_date`
- `field_name`
- `old_value`
- `new_value`
- `primary_source`
- `fallback_source`
- `enrichment_status`
- `matched_yahoo_period_end_date`
- `match_method`
- `run_id`
- `created_at_utc`

## Missing Metadata

Yahoo data does not necessarily provide the true company filing/publication timestamp.

Current paths also do not provide a stable company document id equivalent to SEC accession. `source_document_id` can remain `NULL` unless future code has a stable external document/reference id.

## Source Provider

The chosen provider value is:

```text
yahoo
```

This matches existing raw, bridge, and fallback code naming.

## Source Hash Policy

`build_yahoo_source_hash` computes a deterministic SHA-256 hash from:

- market
- normalized ticker
- period end date
- payload hash when available
- normalized row values
- Yahoo quarterly staging metadata when provided
- fallback enrichment audit rows when provided

The hash is stable under enrichment audit row ordering differences.

The hash changes when:

- `payload_hash` changes
- normalized financial values change
- Yahoo quarterly metadata changes
- fallback-enriched audit values change

## Statement Vintage ID Policy

`build_yahoo_statement_vintage_id` creates deterministic ids:

```text
yahoo:<mode>:<market>:<TICKER>:<period_end_date>:<source_hash_prefix>
```

Supported modes are:

- `yahoo_quarterly_staging`
- `yahoo_to_generic_bridge`
- `yahoo_fallback_enrichment`
- `yahoo_missing_quarter_insert`

No random UUIDs are used.

## Available-At Policy

`available_at_utc` is required as an explicit input to `build_yahoo_vintage_metadata`.

The helper rejects `available_at_utc == period_end_date` to prevent accidental period-date availability. Future production wiring can choose a policy based on provider observed, raw `loaded_at_utc`, staging `created_at_utc`, or externally verified timing. Phase 4H4 does not choose that production policy.

## Fallback Enrichment Provenance Policy

`build_yahoo_field_source_map` supports two cases.

Yahoo-only or bridge rows:

- caller passes `yahoo_fields`
- non-null fields are marked `source_provider = yahoo`
- `provenance_role` defaults to `FALLBACK_REPORTED`, or can be set to `PROVIDER_REPORTED`
- `merge_action = YAHOO_BRIDGED`

Fallback enrichment rows:

- caller passes `enrichment_audit_rows`
- only `fallback_source = yahoo` and `enrichment_status = FILLED_FROM_YAHOO` rows create Yahoo provenance
- `provenance_role = FALLBACK_REPORTED`
- `merge_action = YAHOO_FILLED_MISSING`
- `old_value`, `new_value`, matched period, match method, run id, and created timestamp are carried into provenance metadata

Fields retained from SEC are not marked as Yahoo unless the caller explicitly passes them as Yahoo fields. Null normalized fields do not produce provenance entries.

## Yahoo Bridge vs Yahoo Fallback Fill

Yahoo bridge means a Yahoo quarterly staging row is the primary source for the generic quarterly row. In that case all known non-null Yahoo-provided fields can be marked as Yahoo provenance.

Yahoo fallback fill means an existing SEC-derived generic quarterly row was retained, and Yahoo only filled missing fields. In that case only audit-confirmed filled fields should be marked as Yahoo provenance.

## Limitations

- No provider/network calls are made.
- No DB connection is required.
- No real DB writes are performed.
- Yahoo production write paths are not wired.
- Fallback behavior is unchanged.
- `available_at_utc` remains explicit input.
- Yahoo does not provide a reliable company-publication timestamp in current stored rows.
- `source_document_id` may remain `NULL`.
- SEC-retained field provenance must be supplied separately; this helper does not infer SEC provenance.

## Why Production Wiring Is Still Not Done

Production wiring should wait until Yahoo raw/staging/fallback paths can pass:

- explicit availability policy
- payload hash and loaded/created/provider-observed timestamps
- field-level source maps for Yahoo-only and mixed SEC/Yahoo rows
- a safe way to combine SEC metadata contract and Yahoo fallback provenance for mixed rows

Wiring before that would risk creating vintage rows with ambiguous availability and over-broad Yahoo provenance.

## Recommended Next Phase

Phase 4H5 should add a test-only combined metadata bridge that can assemble:

- SEC metadata from Phase 4H3
- Yahoo/fallback metadata from Phase 4H4
- mixed field provenance maps
- the Phase 4H2 opt-in adapter

Production provider wiring should still remain separate and opt-in until availability and mixed-source policies are end-to-end tested.
