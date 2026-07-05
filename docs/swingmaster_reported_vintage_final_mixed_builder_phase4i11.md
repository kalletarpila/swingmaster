# SwingMaster Reported Vintage Final Mixed Builder Phase 4I11

Phase 4I11 adds a pure, test-only helper contract for building final mixed SEC + Yahoo fallback reported quarterly vintage metadata and field provenance.

## Purpose

The helper defines how a future quarter_update combined path can construct one final mixed vintage after SEC reconstruction and Yahoo fallback enrichment have completed.

This phase does not wire the builder into quarter_update execution. It does not call providers, write the real DB, run scheduler jobs, or change default behavior.

## Final Mixed Vintage Meaning

A final mixed vintage represents the resulting normalized quarterly row after SEC-retained reported fields and Yahoo fallback-filled fields have been merged.

It is distinct from:

- SEC-only vintages
- Yahoo bridge vintages
- Yahoo fallback-only vintages
- planning-only quarter_update summaries

## Row-Level Metadata Policy

Row-level metadata uses `source_provider=mixed_sec_yahoo`.

The metadata builder requires explicit:

- `available_at_utc`
- `ingested_at_utc`
- `run_id`

Availability is not inferred from `period_end_date`, and `period_end_date` is rejected as the availability timestamp.

## Source Hash Policy

`build_final_mixed_source_hash(...)` hashes a canonical JSON payload containing:

- normalized market, ticker, and period
- normalized financial fields from the final row
- SEC retained field provenance
- Yahoo fallback field provenance
- fallback audit rows sorted by canonical content

The hash is deterministic, stable under fallback audit row ordering differences, and changes when final financial values, SEC provenance, Yahoo provenance, or fallback audit row content changes.

## Statement Vintage ID Policy

`build_final_mixed_statement_vintage_id(...)` creates deterministic ids in this format:

```text
mixed_sec_yahoo:<market>:<TICKER>:<period_end_date>:<source_hash_prefix>
```

The prefix prevents collisions with SEC-only and Yahoo-only vintage ids.

## Field Provenance Merge Rules

`merge_final_mixed_field_source_maps(...)` builds the final field source map for non-null reported financial fields:

- SEC-retained fields preserve their SEC provenance.
- Yahoo-filled fields preserve Yahoo fallback provenance.
- If both maps claim the same field, Yahoo may replace only when its merge action is `YAHOO_FILLED_MISSING`; otherwise the helper raises `ValueError`.
- Non-null unmapped fields use the existing unknown contract: `source_provider=unknown`, `provenance_role=UNSPECIFIED`, `merge_action=SOURCE_NOT_PROVIDED`.
- Null fields do not create provenance entries.

## PIT Behavior

Temp-DB tests write the final mixed row through `write_normalized_quarterly_rows_with_optional_vintage(...)`.

The PIT reader returns the final mixed vintage at or after `available_at_utc` and returns `None` before `available_at_utc`.

## Production Wiring Status

Not wired.

Phase 4I11 adds only:

- `swingmaster/fundamentals/reported_final_mixed_vintage.py`
- temp-DB tests
- documentation

It does not modify `run_fundamental_quarter_update.py`, production provider flows, schedulers, migrations, TTM, scoring, valuation, UI, or ESS.

## Recommended Next Phase

Recommended Phase 4I12: add mocked/default-off quarter_update handoff planning for the final mixed builder, still without real DB writes or provider calls.

Before production execution, define duplicate/no-op policy, supersession semantics, and whether existing SEC-only or Yahoo-only vintages should be treated as intermediate rows.

## Phase 4I12 Planning Reference

Phase 4I12 adds planning-only quarter_update integration for this builder in [Reported Vintage Quarter Update Final Mixed Planning Phase 4I12](swingmaster_reported_vintage_quarter_update_final_mixed_planning_phase4i12.md).

The integration uses mocked inputs to compute planned source hash, statement id, and provenance count. Live quarter_update still writes no final mixed vintage rows.
