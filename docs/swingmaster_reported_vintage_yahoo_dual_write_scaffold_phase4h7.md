# SwingMaster Reported Vintage Yahoo Dual-Write Scaffold Phase 4H7

## Purpose

Phase 4H7 adds an explicit Yahoo/fallback opt-in scaffold for reported-vintage dual writes.

The scaffold lives in `swingmaster/fundamentals/reported_yahoo_dual_write_adapter.py` and is intentionally not wired into Yahoo CLIs, provider refresh, fallback production execution, quarter-update orchestration, TTM, scoring, valuation, UI, or ESS.

## What The Scaffold Does

`write_yahoo_quarterly_rows_with_optional_vintage` accepts normalized quarterly rows plus caller-provided Yahoo staging/source metadata. With `write_vintage=True`, it builds Yahoo metadata and Yahoo field provenance using the Phase 4H4 Yahoo metadata contract, then writes latest, vintage, and field provenance rows through the Phase 4H2 opt-in adapter.

`write_yahoo_fallback_enriched_rows_with_optional_vintage` accepts normalized rows plus caller-provided fallback enrichment audit rows. It can also accept an explicit existing field-source map, such as SEC-retained field provenance, and merges Yahoo-filled field provenance into it.

When `write_vintage=False`, both helpers preserve latest-only behavior by delegating to the existing opt-in adapter default path. They do not write vintage or field-provenance rows.

## What It Does Not Do

The scaffold does not:

- call Yahoo
- call SEC
- run providers
- run refresh jobs
- run scheduler
- open DB paths by itself
- write to the real fundamentals DB by itself
- change `insert_quarterly_rows`
- change Yahoo staging, bridge, or fallback logic
- change default Yahoo CLI behavior
- wire `run_fundamental_quarter_update.py`
- change TTM/scoring/valuation/readers/UI/ESS

## Required Metadata

Yahoo bridge vintage mode requires:

- normalized quarterly row with `ticker` and `period_end_date`
- Yahoo quarterly staging/source row for that ticker and period
- explicit `available_at_utc`
- explicit `ingested_at_utc`
- explicit `run_id`
- optional payload hash keyed by ticker and period

Yahoo fallback vintage mode requires:

- normalized quarterly row with `ticker` and `period_end_date`
- enrichment audit rows keyed by ticker and period
- explicit `available_at_utc`
- explicit `ingested_at_utc`
- explicit `run_id`
- optional Yahoo quarterly staging/source row
- optional payload hash
- optional explicit field-source map for non-Yahoo retained fields

The scaffold does not invent availability timestamps, source hashes, provider run ids, or retained-field provenance.

## Available-At Policy

The scaffold never derives availability from `period_end_date`.

`available_at_utc` is required from the caller for vintage writes. For the current scaffold, Yahoo provider observed or loaded time can be supplied by the caller, but that is not the same as a verified company report publication timestamp. Production wiring must decide and document the availability-quality policy before enabling real DB writes.

## PIT And Provenance Behavior

Vintage rows use the existing PIT contract: future reads select rows where `available_at_utc <= decision_cutoff_utc`.

Yahoo bridge provenance marks non-null financial fields as:

- `source_provider = yahoo`
- `provenance_role = PROVIDER_REPORTED`
- `merge_action = YAHOO_BRIDGED`

Yahoo fallback provenance marks audit-confirmed Yahoo-filled fields as:

- `source_provider = yahoo`
- `provenance_role = FALLBACK_REPORTED`
- `merge_action = YAHOO_FILLED_MISSING`

If fallback rows include non-null fields that are not present in the Yahoo audit rows and not present in an explicit field-source map, the scaffold marks those fields as:

- `source_provider = unknown`
- `provenance_role = UNSPECIFIED`
- `merge_action = SOURCE_NOT_PROVIDED`

This prevents retained SEC or otherwise unknown fields from being silently labeled as Yahoo. If the caller wants SEC-retained provenance, it must pass an explicit field-source map, typically from the SEC metadata contract.

Conflicting explicit and Yahoo fallback provenance for the same field raises `ValueError` instead of silently choosing one source.

## Why Production Yahoo CLI Is Not Wired Yet

The current Yahoo bridge and fallback production paths do not pass all PIT metadata and field-level source maps through their write boundaries.

Production wiring should wait until the caller can provide:

- explicit availability policy
- payload hash or equivalent source hash
- provider observed or ingested timestamp policy
- raw/staging source references
- explicit field-source map for retained non-Yahoo fields in mixed SEC+Yahoo rows

Wiring the Yahoo CLIs now would risk creating vintage rows with incomplete lineage, ambiguous availability, or incorrect Yahoo attribution for retained fields.

## Recommended Next Phase

The next phase can add a narrow, explicitly opt-in Yahoo CLI experiment if it:

- remains default-off
- requires explicit availability inputs
- carries Yahoo payload/source hash through the write boundary
- carries fallback audit rows and retained-field provenance through the write boundary
- uses temp-DB and mocked/fixture tests first
- does not wire full quarter-update orchestration

## Phase 4H10 Reference

Phase 4H10 adds temp-DB integration coverage for the Yahoo bridge and Yahoo fallback-to-vintage chains, documented in [Reported Vintage Yahoo Fallback To Vintage Integration Phase 4H10](swingmaster_reported_vintage_yahoo_fallback_to_vintage_integration_phase4h10.md).

The integration test proves the Phase 4H7 scaffold can write latest, vintage, and field provenance rows from fixture Yahoo source rows and fallback audit rows, with PIT reader checks and without production wiring.
