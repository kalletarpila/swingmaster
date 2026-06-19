# SwingMaster Reported Vintage SEC Metadata Contract Phase 4H3

## Purpose

Phase 4H3 defines a pure, test-only metadata contract for SEC-derived reported quarterly rows.

It does not wire SEC refresh, SEC reconstruction, provider CLIs, quarter-update orchestration, TTM, scoring, valuation, readers, UI, or ESS to reported-vintage writes.

The implementation lives in `swingmaster/fundamentals/reported_sec_vintage_metadata.py` and is intended as a future input contract for the Phase 4H2 opt-in adapter.

## SEC Metadata Available Today

Current SEC raw extraction stores selected companyfacts into `rc_fundamental_statement_raw` with:

- `ticker`
- `statement_type`
- `period_end_date`
- `period_type = sec_fact`
- `field_name`
- `field_value`
- `currency`
- `source = sec_edgar`
- `retrieved_at_utc`
- `run_id`

The SEC fact metadata currently available for the contract is encoded inside `field_name`:

```text
<tag>|form=<form>|unit=<unit>|fy=<fy>|fp=<fp>|frame=<frame>|start=<start>|filed=<filed>
```

`parse_sec_field_name` can extract:

- SEC tag
- form
- unit
- fiscal year
- fiscal period
- frame
- start date
- filed date

The `filed` value is date-only. The current stored raw shape does not include SEC accepted timestamp, accession number, document URL, or a full filing document id unless a future caller provides it separately in fixture/source fact metadata.

## Missing Metadata

The current SEC raw/reconstruction path does not yet provide:

- SEC accepted timestamp
- exact first-public availability timestamp
- accession/document id in the stored raw row shape
- selected-fact lineage emitted by production reconstruction
- provider-observed timestamp separate from `retrieved_at_utc`

Because of this, `available_at_utc` remains an explicit caller input. The contract must not infer it from `period_end_date`.

## Source Provider

The chosen provider value is:

```text
sec_edgar
```

This matches existing raw rows and current provider naming in the repository.

## Source Hash Policy

`build_sec_source_hash` computes a deterministic SHA-256 hash from:

- normalized ticker
- period end date
- normalized financial row values
- contributing SEC fact payloads
- encoded SEC fact metadata parsed from `field_name` or `encoded_field_name`
- raw fact value, currency, source, retrieved timestamp, and run id when present

The hash is stable under contributing-fact input ordering differences.

The hash changes when:

- a relevant SEC fact value changes
- selected contributing fact metadata changes
- normalized reported row values change

## Statement Vintage ID Policy

`build_sec_statement_vintage_id` creates deterministic ids:

```text
sec_edgar:<market>:<TICKER>:<period_end_date>:<source_hash_prefix>
```

Ticker is uppercased. Market is lowercased. The source-hash prefix makes the id change when contributing facts or normalized values change.

No random UUIDs are used.

## Filed Date Handling

`filed` is extracted from encoded SEC fact metadata when available.

The contract uses the latest contributing filed date as `filed_at_utc`, but the value remains date-only. It also emits:

```text
filed_at_utc_precision = date_only
```

This intentionally does not pretend to know precise filing time.

## Available-At Policy

`available_at_utc` is required as an explicit input to `build_sec_vintage_metadata`.

The helper rejects `available_at_utc == period_end_date` to prevent accidental lookahead-prone period-date availability.

Future production wiring can choose a policy based on SEC accepted timestamp, provider observed time, or conservative ingestion time after those values are available. Phase 4H3 does not choose that production policy.

## Source Document ID Policy

If a contributing fact fixture provides `source_document_id`, `accession`, `accession_number`, or `adsh`, the helper uses it.

Current stored SEC raw facts do not include these fields. When no explicit document id exists, the helper uses a deterministic placeholder:

```text
sec_edgar:<TICKER>:<period_end_date>:<source_hash_prefix>
```

This placeholder is stable and safe for tests, but it is not an audit-grade SEC filing accession id.

## Field Provenance Policy

`build_sec_field_source_map` emits one provenance entry per non-null normalized financial field when contributing fact data is provided for that field.

Each entry uses:

- `source_provider = sec_edgar`
- `source_table = rc_fundamental_statement_raw`
- `provenance_role = PRIMARY_REPORTED`
- `merge_action = SEC_RETAINED`
- deterministic `source_hash`
- deterministic `source_row_ref`

Null normalized fields do not produce provenance rows.

## Limitations

- No provider/network calls are made.
- No DB connection is required.
- No real DB writes are performed.
- SEC production write paths are not wired.
- SEC reconstruction rules are unchanged.
- Available-at remains explicit input.
- Current raw rows do not contain accepted timestamp or accession id.
- Field-level selected-fact lineage is represented by fixture/caller-provided contributing facts, not emitted automatically by production reconstruction yet.

## Why Production Wiring Is Still Not Done

Production wiring should wait until SEC refresh/reconstruction can pass:

- selected contributing facts per normalized field
- an accepted/observed/ingested availability policy
- an audit-grade source document id or accession id when available
- deterministic source hashes through the provider write boundary

Wiring before that would create reported-vintage rows with weak availability and lineage semantics.

## Recommended Next Phase

Phase 4H4 should add a test-only bridge that combines:

- normalized quarterly rows
- SEC metadata from this contract
- field source maps
- the Phase 4H2 opt-in adapter

That phase should still avoid production provider wiring unless SEC selected-fact lineage and available-at policy are explicitly provided and tested.
