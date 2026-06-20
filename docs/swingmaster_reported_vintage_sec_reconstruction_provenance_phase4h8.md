# SwingMaster Reported Vintage SEC Reconstruction Provenance Phase 4H8

## Purpose

Phase 4H8 adds a pure/test-only helper for extracting field-level SEC contributing facts from the current reconstruction logic.

The helper lives in `swingmaster/fundamentals/sec_reconstruction_provenance.py`. It is not wired into SEC CLIs, provider refresh, quarter-update orchestration, vintage production writes, TTM, scoring, valuation, UI, or ESS.

## What Provenance Extraction Does

`reconstruct_quarterly_rows_with_provenance(...)` returns:

- the existing `reconstruct_quarterly_rows(...)` output
- a mapping keyed by `(ticker, period_end_date)`
- per-row field maps keyed by normalized quarterly fields such as `revenue`, `net_income`, `cash`, `total_debt`, `operating_cashflow`, `capex`, and `free_cashflow`
- contributing SEC fact rows for each field where the current reconstruction selection can be determined

The helper delegates reconstructed value output to the existing reconstruction function. This is deliberate: Phase 4H8 does not change reconstruction values or heuristics.

## Metadata Extracted

Contributing fact entries preserve the fields needed by the existing SEC vintage metadata contract:

- `ticker`
- `statement_type`
- `period_end_date`
- encoded SEC `field_name`
- `field_value`
- `currency`
- `source`
- `retrieved_at_utc`
- `run_id`

The encoded `field_name` remains intact, so `extract_sec_filed_date(...)` can still parse `filed=...` metadata.

## Field-Level Representation

The output shape is:

```text
(ticker, period_end_date) -> normalized_field -> list[contributing SEC fact rows]
```

This shape can feed:

- `build_sec_field_source_map(...)`
- `write_sec_reconstructed_quarterly_rows_with_optional_vintage(...)`

## Derived Field Handling

Flow fields use the same selected-fact logic as current SEC reconstruction.

When a quarterly value comes directly from a quarterly SEC fact, the helper returns that fact.

When a quarterly value is derived from YTD values, the helper returns the YTD facts used in the subtraction. For example, Q2 derived from Q2 YTD minus Q1 baseline includes the Q2 YTD fact and the Q1 baseline fact.

When FY/Q4 is derived from annual FY minus Q1-Q3, the helper returns the annual fact plus the contributing facts for Q1, Q2, and Q3.

`free_cashflow` is not directly emitted by SEC reconstruction. The helper derives its provenance when both `operating_cashflow` and `capex` contributing facts are available, and returns the union of those facts.

`total_debt` provenance includes all selected debt component facts used by the current reconstruction logic. If current and noncurrent debt components are summed, both component facts are returned.

## Limitations

The helper only reports facts that can be matched to the current reconstruction selection logic.

If a reconstructed field has no supported mapping, the helper returns no guessed provenance for that field.

The helper does not invent first-class SEC accession/document metadata. It preserves any such fields only if already present in input fact rows.

The helper does not decide `available_at_utc`; production wiring must still provide an explicit availability policy.

## Production Output Status

Phase 4H8 does not change production SEC output.

Regression tests compare `reconstruct_quarterly_rows(...)` output with `reconstruct_quarterly_rows_with_provenance(...)[0]`, proving the helper is additive for current fixtures.

## Production Wiring Status

Production wiring is still not done.

The SEC dual-write scaffold can later receive real reconstruction provenance, but SEC CLIs, provider refresh, and quarter-update remain default-off/unwired for vintage writes.

## Recommended Next Phase

The next phase can add a narrow, explicitly opt-in SEC CLI experiment if it:

- remains default-off
- passes Phase 4H8 contributing-facts provenance through the write boundary
- requires explicit `available_at_utc`
- uses temp-DB and fixture tests first
- does not wire full quarter-update orchestration
