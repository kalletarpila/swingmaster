# Phase 9H1 Result Check Contract

This contract extends Check for New Results for future V2 quarterly refresh work units. It does not authorize canonical V2 financial writes.

## Plan Schema

`fundamental_result_check_plan_v2` preserves the existing executable-plan rule:

- `SUCCESS` plans may contain executable candidates.
- `PARTIAL` and `FAILED` plans contain no executable candidates.
- `candidate_hash` remains based on market, ticker, decision, target period, and planned action.

Each executable candidate now includes:

- `work_unit_key`
- `canonical_fiscal_year`
- `canonical_fiscal_quarter`
- `canonical_report_date`
- `selector_group`
- `providers_due`
- nested `work_unit`

The work-unit key is deterministic:

`market:ticker:canonical_fiscal_year:canonical_fiscal_quarter`

Provider date offsets inside the same fiscal quarter must not create duplicate work units.

## Timing Observations

Provider timing is stored separately from canonical provenance:

- `rc_fundamental_provider_observation_content` stores semantic provider evidence/content.
- `rc_fundamental_provider_observation_seen` stores poll/seen events for that content.

`observed_at_utc` alone does not create a new semantic content observation. Repeated identical payload/content reuses the content row and appends a seen event.

9H1 records lightweight Check observations only. It does not write `rc_fundamental_quarterly_field_provenance` and does not write V2 canonical financial facts.

## JSON Summary

Check JSON/stdout summary preserves existing fields and adds:

- `plan_version`
- `executable_work_unit_count`
- `work_unit_count`
- `partial_follow_up_count`
- `provider_timing_observation_count`
- `provider_timing_content_inserted_count`
- `provider_timing_content_reused_count`
- `provider_timing_seen_event_inserted_count`
- `provider_call_counts_json`

RawCandle can continue using `check_status`, `candidate_count`, and `plan_json`; the 9F2 SUCCESS gate remains valid.
