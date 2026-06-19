# SwingMaster Reported Vintage SEC Dual-Write Scaffold Phase 4H6

## Purpose

Phase 4H6 adds the first SEC-specific opt-in scaffold for reported-vintage dual writes.

The scaffold lives in `swingmaster/fundamentals/reported_sec_dual_write_adapter.py` and is intentionally not wired into SEC CLIs, provider refresh, quarter-update orchestration, TTM, scoring, valuation, UI, or ESS.

## What The Scaffold Does

`write_sec_reconstructed_quarterly_rows_with_optional_vintage` accepts:

- a SQLite connection supplied by the caller
- normalized quarterly rows
- SEC contributing facts keyed by ticker and period
- explicit `available_at_utc`
- explicit `ingested_at_utc`
- explicit `run_id`
- optional `normalization_run_id`

When `write_vintage=False`, it preserves latest-only behavior by delegating to the existing opt-in adapter default path.

When `write_vintage=True`, it builds SEC metadata and field provenance using the Phase 4H3 SEC metadata contract, then writes latest, vintage, and field provenance rows through the Phase 4H2 opt-in adapter.

## What It Does Not Do

The scaffold does not:

- call SEC
- call Yahoo
- run providers
- run refresh jobs
- run scheduler
- open DB paths by itself
- write to the real fundamentals DB by itself
- change `insert_quarterly_rows`
- change SEC reconstruction rules
- change default SEC CLI behavior
- wire `run_fundamental_quarter_update.py`
- wire Yahoo/fallback paths
- change TTM/scoring/valuation/readers/UI/ESS

## Required SEC Metadata

Vintage mode requires:

- normalized quarterly row with `ticker` and `period_end_date`
- contributing SEC facts for that ticker and period
- explicit `available_at_utc`
- explicit `ingested_at_utc`
- explicit `run_id`

Contributing facts are passed as a field-level mapping. The scaffold flattens them for source-hash and statement-vintage metadata, and passes the field-level mapping to the SEC provenance builder.

## Available-At Policy

The scaffold never derives availability from `period_end_date`.

`available_at_utc` is required from the caller. This preserves the Phase 4H3 policy that production wiring must decide between accepted timestamp, provider observed timestamp, ingestion timestamp, or externally verified availability before enabling real production writes.

## Provenance Behavior

For non-null normalized fields with contributing SEC facts, field provenance uses:

- `source_provider = sec_edgar`
- `source_table = rc_fundamental_statement_raw`
- `provenance_role = PRIMARY_REPORTED`
- `merge_action = SEC_RETAINED`

Fields without contributing facts are not given SEC provenance by the scaffold.

## Why Production SEC CLI Is Not Wired Yet

The current SEC reconstruction path does not emit selected-fact lineage directly through the production write boundary.

Production wiring should wait until the caller can provide:

- selected contributing facts per normalized field
- explicit availability policy
- source document/accession policy where available
- deterministic source-hash inputs

Wiring the SEC CLI now would risk creating vintage rows with incomplete lineage or ambiguous availability.

## Recommended Next Phase

The next phase can add a narrow, explicitly opt-in SEC CLI experiment if it:

- remains default-off
- requires explicit availability inputs
- passes selected fact lineage through the write boundary
- uses temp-DB and mocked/fixture tests first
- does not wire full quarter-update orchestration
