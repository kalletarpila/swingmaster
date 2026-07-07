# SwingMaster Yahoo-Aware Vintage Scope Verification Phase 4M2

Date: 2026-07-07

## Purpose

Phase 4M2 verifies the Phase 4M1 Yahoo-aware/final-mixed planner scope fix against the current real USA fundamentals DB in read-only mode.

No provider calls, scheduler runs, refresh jobs, recovery apply, Yahoo-aware apply, or real DB writes were run.

## Command

```bash
python3 -m swingmaster.cli.diagnose_quarter_update_vintage_scope \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --source-run-id USA_QUARTER_UPDATE_2026-07-07__QUARTERLY \
  --enrich-run-id USA_QUARTER_UPDATE_2026-07-07__QUARTERLY__ENRICH \
  --format json \
  --sample-limit 20
```

The diagnostic CLI opens the DB using SQLite URI `mode=ro` and sets `PRAGMA query_only=ON`.

The requested source run id resolved to the actual latest-row run id:

- requested: `USA_QUARTER_UPDATE_2026-07-07__QUARTERLY`
- used: `USA_QUARTER_UPDATE_2026-07-07__QUARTERLY__QUARTERLY`
- resolution: `appended_quarterly_suffix`

## Real DB Result

Overall diagnostic status:

- `SCOPE_FIX_VERIFIED_BLOCKED_NARROW`

Parity:

- latest rows: `155377`
- vintage rows: `155377`
- provenance rows: `1306713`
- latest-without-vintage: `0`
- vintage-without-latest: `0`
- duplicate statement_vintage_id count: `0`

Mismatch:

- value mismatch count: `1`
- Yahoo-explained mismatch count: `0`
- unexplained mismatch count: `1`
- sample: `GIS:2025-05-25:total_debt:latest=14878600000.0:vintage=677000000.0:statement_vintage_id=legacy:usa:GIS:2025-05-25:9441b8313c7894e3`

Completion under fixed logic:

- `completion_status=BLOCKED_POST_RUN_DRIFT`
- `completion_reason=unexplained_value_mismatch`
- `next_action=INVESTIGATE_DRIFT`

Planner:

- `planner_status=PLAN_BLOCKED`
- `planner_scope_count=0`
- `planner_scope_source=none_blocked_completion`
- `planner_blocked_rows=0`
- unknown provenance field sample count: `0`
- unknown provenance field sample: empty

Execution:

- `yahoo_aware_execution_status=NOT_REQUESTED`

## Conclusion

The Phase 4M1 scope fix is verified against the current real DB state.

The system no longer classifies the GIS mismatch as `FINAL_MIXED_REQUIRED` based on aggregate Yahoo audit counts. It no longer expands the planner to the 184/188 unrelated historical Yahoo-audit blocked rows. The planner remains narrow with scope count `0` and blocked rows `0`.

## Recommended Next Action

Do not run Yahoo-aware apply or recovery for this state.

Create a separate diagnostic/fix phase for the exact GIS mismatch:

- determine whether the legacy visible vintage for `GIS:2025-05-25:total_debt` should be superseded by a provider-derived vintage
- or determine whether the latest `total_debt=14878600000.0` value is incorrect

The 93 ticker failures from the first UI run remain separate from this scope verification.

## Phase 4M3 Follow-Up

Phase 4M3 diagnosed the exact GIS mismatch read-only. The latest `total_debt=14878600000.0` is supported by SEC raw debt components:

- `LongTermDebtCurrent=1528400000.0`
- `LongTermDebtNoncurrent=12673200000.0`
- `ShortTermBorrowings=677000000.0`

The visible legacy vintage value `677000000.0` equals only `ShortTermBorrowings`. Diagnosis: `DEBT_COMPONENT_POLICY_DIFF`.

See [SwingMaster GIS Total Debt Mismatch Phase 4M3](swingmaster_reported_vintage_gis_total_debt_mismatch_phase4m3.md).

## Phase 4M6 Follow-Up

Phase 4M6 applied the guarded one-row GIS provider-derived vintage. The same scope diagnostic now reports:

- overall diagnostic status: `NO_MISMATCH`
- value mismatch count: `0`
- unexplained mismatch count: `0`
- latest-without-vintage: `0`
- vintage-without-latest: `0`
- duplicate statement_vintage_id count: `0`

The GIS `total_debt` drift is no longer blocking this diagnostic. The remaining `FINAL_MIXED_REQUIRED` completion state should be handled separately from the GIS correction.
