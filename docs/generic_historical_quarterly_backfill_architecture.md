# Generic Historical Quarterly Backfill Foundation

This phase provides planning and policy primitives only. It does not execute provider fetches, mutate production fundamentals, rebuild percentiles, or run scheduler jobs.

## Source Semantics

SEC data remains authoritative only for fields that have validated SEC tag semantics. `OperatingIncomeLoss` is mapped to `operating_income`, not `ebit`. Generic SEC backfill does not treat SEC facts as a source for `ebit` or `ebitda`.

Yahoo is the direct source for `ebit` and `ebitda`. For shared fields, the merge policy keeps existing values unless SEC has a supported value, and lets Yahoo fill remaining NULL fields.

## Result Ledger

`rc_fundamental_historical_backfill_result` is the durable quarter-level ledger for future executor attempts. It is keyed by `(market, ticker, target_period_end_date)` and stores result status, retry/exhaustion state, evidence states, latest run id, and timestamps.

The migration is staged as schema migration `036`. It is not applied to production by the planner.

## Target Inventory

The planner builds a generic target-quarter inventory from existing normalized quarterly rows, SEC fact rows, and persisted earnings-event matches. SEC fact metadata preserves fiscal year and fiscal quarter from field-name metadata such as `fy=2026|fp=Q1`, so fiscal-quarter identity is not inferred only from calendar dates.

Targets from ambiguous earnings-event matches are marked `TARGET_IDENTITY_REVIEW` and are not auto-classified for provider execution.

## Planner Semantics

The CLI opens SQLite with `mode=ro` and `PRAGMA query_only=ON`. Its output is a deterministic artifact set:

- `target_inventory.csv`
- `quarter_plan.csv`
- `ticker_provider_plan.csv`
- `summary.json`
- `planner_run_metadata.json`
- `plan.json`

The planner classifies each quarter before any executor exists:

- `NO_ACTION_COMPLETE`
- `OFFLINE_MERGE_AVAILABLE`
- `OFFLINE_YAHOO_RAW_RECONSTRUCTABLE`
- `NEEDS_YAHOO_RECENT_ENRICHMENT`
- `NEEDS_SEC_HISTORY_REFRESH`
- `NEEDS_SEC_AND_YAHOO`
- `PARTIAL_BEST_AVAILABLE`
- `RETRYABLE_FAILURE`
- `TARGET_IDENTITY_REVIEW`
- `SOURCE_NOT_AVAILABLE`

Yahoo enrichment is ticker-scoped and recent-window limited by the latest deterministic target quarters per ticker. SEC history refresh is also ticker-scoped: many missing historical quarters produce one SEC provider action for that ticker.

## Executor Boundary

A future executor should consume the planner output and write one ledger row per attempted target quarter. Provider calls should be aggregated by ticker from `ticker_provider_plan`, while quarter outcomes remain durable in the result ledger.

Before enabling production writes, apply migration `036` intentionally and add executor-specific guards that reject plans whose content hash differs from the reviewed planner artifact.
