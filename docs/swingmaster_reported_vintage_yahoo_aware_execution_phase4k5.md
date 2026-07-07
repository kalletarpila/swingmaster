# Reported Vintage Yahoo-Aware Execution Phase 4K5

Phase 4K5 adds a default-off, temp-tested execution layer for quarter_update Yahoo-aware and final mixed vintage plans.

## Purpose

Phase 4K1 writes SEC latest-writer vintage rows in explicit mode. Phase 4K2 detects post-SEC Yahoo impact. Phase 4K3 classifies whether SEC-only vintage is sufficient. Phase 4K4 plans final mixed or Yahoo-aware candidates.

Phase 4K5 proves that those plans can be executed through existing vintage writers in temp DBs. It does not run providers and does not apply to the real DB.

## Explicit Action

quarter_update now accepts:

```text
--vintage-yahoo-aware-action plan_only|write
```

Default is `plan_only`. `write` is valid only with:

- `--write-vintage`
- `--vintage-mode sec_latest_writer`
- valid explicit PIT metadata

Without `write`, Yahoo-aware/final mixed vintage rows are not written.

## Final Mixed Execution

For `FINAL_MIXED_PLAN_READY`, execution writes a separate final mixed vintage row using the existing final mixed execution helper.

Policy:

- final latest row values are used
- SEC-retained fields keep SEC provenance
- Yahoo-filled fields use Yahoo fallback provenance
- unknown non-null fields block before writes
- existing statement vintage ids block instead of replacing

## Yahoo-Only Execution

For `YAHOO_VINTAGE_PLAN_READY`, execution writes Yahoo-derived missing-quarter vintage rows through the existing Yahoo dual-write adapter.

Policy:

- Yahoo row/source evidence is required
- provenance is Yahoo, not SEC
- existing statement vintage ids block instead of replacing
- missing linkage blocks

## Summary Fields

When explicit write action is used, quarter_update can surface:

- `vintage_yahoo_aware_execution_status`
- `vintage_yahoo_aware_final_mixed_rows_written`
- `vintage_yahoo_aware_yahoo_vintage_rows_written`
- `vintage_yahoo_aware_provenance_rows_written`
- `vintage_yahoo_aware_rows_blocked`
- `vintage_yahoo_aware_rows_skipped`
- `vintage_yahoo_aware_error`

In `plan_only`, execution status is `NOT_REQUESTED` when the explicit `sec_latest_writer` path reaches the planning guard.

## Temp-Tested Only

This phase does not:

- run SEC/Yahoo/yfinance/Finnhub/provider APIs
- run refresh jobs or schedulers
- write `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- change default behavior
- change latest-writer, SEC reconstruction, Yahoo fallback, TTM, scoring, valuation, UI, or ESS behavior

Real DB/provider status: not run.

## Recommended Next Phase

Phase 4K6 should be a read-only real-DB preflight for the execution path before any real apply. It should estimate candidate counts, duplicate risks, blocked unknown provenance, and rollback requirements without writing.

## Phase 4K6 Follow-Up

Phase 4K6 adds the read-only real DB readiness/no-op smoke in [Reported Vintage Quarter Update Readiness No-Op Phase 4K6](swingmaster_reported_vintage_quarter_update_readiness_noop_phase4k6.md).

The real USA DB smoke reported `READY_NOOP`, latest/vintage parity 155373/155373, zero latest-without-vintage rows, zero vintage-without-latest rows, zero duplicate `statement_vintage_id` groups, and zero Yahoo-aware pending actions. No providers, schedulers, refresh jobs, quarter_update provider paths, or real DB writes were run.

## Phase 4L4 UI Apply Follow-Up

Phase 4L4 adds a separate UI-driven apply command for planned Yahoo-aware/final mixed corrections without rerunning provider update paths. The primary quarter update UI path remains `plan_only`; the apply action requires explicit approval and is documented in [SwingMaster Quarter Update UI Yahoo-Aware Apply Phase 4L4](swingmaster_quarter_update_ui_yahoo_aware_apply_phase4l4.md).
