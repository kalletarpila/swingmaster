# SwingMaster Yahoo-Aware Vintage Scope Phase 4M1

Date: 2026-07-07

Phase 4M1 fixes the Yahoo-aware/final-mixed completion gate and planner scope after the first real UI-triggered USA PIT/vintage run.

## Problem

The first real run had one post-run value mismatch:

- `GIS:2025-05-25:total_debt`

No current-run Yahoo enrichment audit row existed for that exact ticker, period, and field. The previous completion gate still returned `FINAL_MIXED_REQUIRED` because it treated aggregate current-run Yahoo audit counts as enough evidence.

That let the Yahoo-aware planner scan the wider current enrich-run audit population and produce a large unrelated blocked set.

## New Gate Contract

Post-run parity now includes bounded mismatch detail:

- `vintage_post_run_value_mismatch_count`
- `vintage_post_run_value_mismatch_sample`
- `vintage_post_run_yahoo_explained_mismatch_count`
- `vintage_post_run_unexplained_mismatch_count`
- `vintage_post_run_yahoo_explained_mismatch_sample`
- `vintage_post_run_unexplained_mismatch_sample`

A value mismatch is Yahoo-explained only when the audit row matches exactly:

- ticker
- period_end_date
- field_name
- current enrich run id
- `fallback_source='yahoo'`
- `enrichment_status='FILLED_FROM_YAHOO'`

Aggregate `vintage_yahoo_audit_rows_detected` and `vintage_yahoo_filled_field_rows_detected` no longer explain a mismatch by themselves.

## Completion Behavior

If value mismatches exist and every mismatched field is exactly explained by current-run Yahoo audit, completion returns:

- `vintage_completion_status=FINAL_MIXED_REQUIRED`
- `vintage_completion_reason=value_mismatch_exactly_explained_by_yahoo_audit`

If any mismatched field lacks exact current-run Yahoo audit evidence, completion returns:

- `vintage_completion_status=BLOCKED_POST_RUN_DRIFT`
- `vintage_completion_reason=unexplained_value_mismatch`
- `vintage_next_required_action=INVESTIGATE_DRIFT`

This prevents the GIS mismatch case from becoming a broad final-mixed apply candidate.

## Planner Scope

Yahoo-aware final-mixed planning now receives explicit scoped keys from the post-run guard. For final mixed rows, the scope source is:

- `post_run_yahoo_explained_mismatches`

The planner summary includes:

- `vintage_yahoo_aware_planner_scope_count`
- `vintage_yahoo_aware_planner_scope_source`

The planner no longer scans unrelated current-run Yahoo audit keys when completion is blocked. Unknown provenance samples are bounded to avoid huge UI/log lines.

The execution candidate builder also respects the same scope, so provider-free apply cannot re-expand a narrowed plan.

## Safety

This phase only changes planner/completion logic and temp-DB unit tests. It does not require real provider calls, scheduler runs, refresh jobs, recovery apply, Yahoo-aware apply, or real DB writes.

## Phase 4M2 Verification

Phase 4M2 replayed the fixed guard/planner logic against the current real USA fundamentals DB in read-only mode. The GIS mismatch remained unexplained by exact current-run Yahoo audit evidence, completion returned `BLOCKED_POST_RUN_DRIFT`, and planner scope stayed at `0` with `0` blocked rows.

See [SwingMaster Yahoo-Aware Vintage Scope Verification Phase 4M2](swingmaster_reported_vintage_yahoo_aware_scope_verification_phase4m2.md).
