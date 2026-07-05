# Reported Vintage Yahoo-Aware Plan Phase 4K4

Phase 4K4 adds temp-tested planning for the correct final vintage candidate after quarter_update completion-gate outcomes.

## Purpose

Phase 4K1 can write a SEC latest-writer vintage before Yahoo fallback enrichment. Phase 4K2 detects post-run Yahoo impact and parity drift. Phase 4K3 classifies whether SEC-only vintage is sufficient, final mixed is required, Yahoo-aware vintage is required, or the state is blocked/unknown.

Phase 4K4 does not write final mixed or Yahoo vintages. It prepares candidate metadata and summary counts so a later phase can implement the actual default-off write path safely.

## When Final Mixed Is Required

`FINAL_MIXED_REQUIRED` means the final latest row should be represented as mixed provenance:

- SEC-retained fields keep SEC provenance from `rc_fundamental_quarterly_field_provenance`
- Yahoo-filled fields use enrichment audit rows from `rc_fundamental_quarterly_enrichment_audit`
- final latest row values are used as the normalized candidate values
- any non-null field without SEC or Yahoo evidence blocks the plan

The helper computes deterministic final mixed candidate ids and source hashes using the existing final mixed metadata helpers.

## When Yahoo Vintage Is Required

`YAHOO_VINTAGE_REQUIRED` means Yahoo inserted a missing quarter that is not represented by SEC vintage.

The planner requires Yahoo quarterly staging evidence for the ticker and period. If the Yahoo row is available, it prepares a Yahoo missing-quarter candidate using the existing Yahoo metadata helper mode `yahoo_missing_quarter_insert`. If linkage is missing, it returns `PLAN_BLOCKED` with `INSUFFICIENT_YAHOO_RUN_LINKAGE`.

## Provenance Policy

- SEC retained: `source_provider=sec_edgar`, `provenance_role=PRIMARY_REPORTED`, `merge_action=SEC_RETAINED`
- Yahoo filled: `source_provider=yahoo`, `provenance_role=FALLBACK_REPORTED`, `merge_action=YAHOO_FILLED_MISSING`
- Yahoo inserted quarter: provider-reported Yahoo metadata with missing-quarter insert mode
- Unknown non-null fields: block the plan rather than inventing provenance

## Summary Fields

For explicit `--write-vintage --vintage-mode sec_latest_writer`, quarter_update can surface:

- `vintage_yahoo_aware_planning_status`
- `vintage_yahoo_aware_next_action`
- `vintage_planned_final_mixed_rows`
- `vintage_planned_yahoo_vintage_rows`
- `vintage_planned_yahoo_aware_provenance_rows`
- `vintage_yahoo_aware_blocked_rows`
- `vintage_yahoo_aware_unknown_provenance_fields`

Default behavior without vintage flags is unchanged.

## Temp-Tested Only

This phase only plans. It does not:

- write final mixed vintage rows
- write Yahoo-aware vintage rows
- run providers
- run refresh jobs
- run schedulers
- write the real DB
- change TTM, scoring, valuation, UI, or ESS behavior

Real DB/provider status: not run.

## Recommended Next Phase

Phase 4K5 should add a default-off write executor for planned final mixed and Yahoo-aware candidates, still temp-tested first. Real DB apply should remain a separate preflight/apply phase after row counts, duplicate policy, and rollback are documented.
