# SwingMaster Reported Vintage SEC Reconstruction To Vintage Integration Phase 4H9

## Purpose

Phase 4H9 adds temp-DB integration coverage for the SEC reconstruction-to-vintage contract.

The phase proves the test-only chain can run end to end without wiring SEC production CLIs, provider refresh, quarter-update orchestration, real DB writes, TTM, scoring, valuation, UI, or ESS.

## Chain Tested

The integration test covers:

```text
SEC raw fact fixtures
-> reconstruct_quarterly_rows(...)
-> reconstruct_quarterly_rows_with_provenance(...)
-> build_quarterly_rows(...)
-> build_sec_vintage_metadata(...)
-> write_sec_reconstructed_quarterly_rows_with_optional_vintage(..., write_vintage=True)
-> get_pit_quarterly_vintage(...)
-> get_quarterly_field_provenance(...)
```

All DB writes happen only in pytest temp DBs.

## What Was Proven

The test proves:

- current SEC reconstruction produces expected reconstructed statement rows
- current normalized quarterly builder produces expected latest-compatible quarterly values
- SEC reconstruction provenance maps normalized fields to contributing encoded SEC facts
- SEC metadata source hashes and statement vintage ids are deterministic
- source hash and statement vintage id change when a contributing SEC fact value changes
- the default-off SEC scaffold can write latest, vintage, and field provenance rows in a temp DB
- PIT reads return no row before `available_at_utc`
- PIT reads return the SEC vintage at and after `available_at_utc`
- field provenance rows use `sec_edgar`, `PRIMARY_REPORTED`, and `SEC_RETAINED`

## Derived Field Coverage

The fixture covers direct and derived provenance:

- `free_cashflow` provenance includes operating cashflow and capex facts
- `total_debt` provenance includes the selected debt component facts

## What Remains Not Wired

Production wiring is still not done.

This phase does not change:

- SEC raw bootstrap defaults
- SEC reconstruction CLI defaults
- `reconstruct_quarterly_rows(...)`
- `build_quarterly_rows(...)`
- `insert_quarterly_rows(...)`
- quarter update
- provider refresh
- real DB writes
- downstream readers, TTM, scoring, valuation, UI, or ESS

## Limitations

The integration test uses fixtures, not live SEC provider calls.

`available_at_utc` remains explicit test input. Production must still define the real availability policy before any live write path can be enabled.

The test validates one representative one-period SEC fixture. Broader ticker/provider variability remains a later production-readiness concern.

## Recommended Next Phase

The next phase can add a narrow, explicitly opt-in SEC CLI experiment if it:

- remains default-off
- uses the Phase 4H8 provenance output
- requires explicit availability inputs
- writes only to temp or explicitly approved test DBs first
- does not wire full quarter-update orchestration
