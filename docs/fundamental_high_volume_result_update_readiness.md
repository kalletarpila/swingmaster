# USA Fundamentals High-Volume Update Readiness

## Scope

This review covers the manual USA path:

```text
Check for New Results
-> temp/fundamental_result_check/.../plan.json
-> Update Fundamentals
-> run_fundamental_quarter_update.py --quarter-refresh-plan-json ...
```

FIN/OMXH remains on the legacy `rc_fundamental_quarter_state.new_quarter_available` path. RawCandle is not part of this workflow.

## Execution Model

Plan execution is sequential. `load_plan_rows()` validates the whole plan before provider work starts, sorts candidate rows by ticker and target period, and creates execution rows with:

```text
ticker
market = usa
detected_source_period_end_date = target_period_end_date
```

The main loop in `run_fundamental_quarter_update()` processes one row at a time. There is no cross-ticker batch transaction in this loop. Each provider/persistence sub-step opens its own SQLite connection or delegates to existing single-ticker functions.

For USA, `process_ticker()` passes the explicit target period into `run_quarterly_refresh()`. The refresh path then:

```text
checks whether the explicit target is already satisfied
optionally runs SEC raw/bootstrap and SEC quarterly reconstruction
runs Yahoo fallback enrichment with detected_source_period_end_date = explicit target
verifies the explicit target again
runs TTM, lifecycle, score
skips quarter-state ack in plan mode
reassesses the explicit target quarter
```

The executor does not use `new_quarter_available` in plan mode and does not require a quarter-state row.

## Plan Safety

The plan validator rejects:

```text
non-temp paths
wrong plan_version
non-SUCCESS check_status
wrong fundamentals_db
plans whose decision_date is older than the current operational decision date
invalid decision_date
candidate_count/hash mismatch
duplicate ticker + target period
non-USA rows
non-executable decisions
fundamental_fetch_enabled != 1
eligible_for_execution != 1
missing target_period_end_date
```

This prevents duplicate executable work inside one plan and prevents target-period drift from "latest available" state.

## Failure Isolation

Batch mode catches per-ticker `Exception` failures, records them, continues to later candidates, prints per-ticker error lines, and raises a final batch error only after the loop completes. Single-ticker mode still raises immediately.

The final summary now includes:

```text
candidate_results
failed_candidates
candidate_results_json
failed_candidates_json
```

Each failed candidate includes:

```text
ticker
market
target_period_end_date
failure_step
error_message
retry_recommendation
```

This lets the operator identify which ticker/period rows failed after a large run.

## Retry And Replay

The supported safe procedure after a partial failure is:

1. Inspect `failed_candidates_json` and the console errors.
2. If still on the same operational decision date, rerun the same plan or rerun with `--ticker` for selected failed tickers.
3. If the plan decision date is stale, run `Check for New Results` again and execute the newly generated plan.
4. Do not manually edit plan rows to invent target periods.

Replay within the freshness window is deterministic: the same candidate set is validated by the same hash and processed in sorted order. Already completed target quarters are checked against the explicit target before SEC refresh, and SQLite primary keys prevent duplicate quarterly rows.

## Provider Load

A plan with 100-300 executable USA candidates means sequential per-ticker provider activity:

```text
0 or 1 SEC CompanyFacts/raw fetch per ticker if the explicit target is not already satisfied
1 Yahoo fallback enrichment attempt per ticker
TTM/lifecycle/score per ticker
one final USA valuation pass for the market
```

There is no concurrency and no burst behavior in the quarter-update executor. This is safe but can be slow. One slow provider request can delay the run because processing is sequential; timeout and retry behavior is inherited from the called provider functions.

Do not add concurrency unless there is a measured operational need and a bounded provider-load policy.

## Tested Batch Sizes

The high-volume harness uses temporary SQLite databases and monkeypatched provider execution. It does not touch production data and does not call real providers.

Covered scenarios:

```text
200 successful candidates
120 mixed candidates with provider failure, unexpected exception, partial target, and no-new-data target
duplicate candidate rejection
100-candidate replay/idempotency
100-candidate partial-run restart after interruption
```

The tests prove candidate ordering, exact once-per-plan attempts, explicit target preservation, failure isolation, summary reconciliation, duplicate rejection, and safe replay behavior for completed target rows.

## Limitations

The 2026-08-07 production `Check for New Results` produced zero executable candidates, so it did not exercise real provider throughput for 100-300 quarterly updates. The high-volume tests prove control-flow correctness under volume, not provider speed or provider availability.

Plan freshness is checked at start of execution against the operational decision date. A long run is allowed to finish even if wall-clock time crosses into a later moment while it is running. This is intentional; the stale-plan guard prevents starting from stale decision-date evidence, not completing a run already started from valid evidence.

If the process is interrupted by `KeyboardInterrupt`, the top-level loop does not emit a final summary because that is not an `Exception`. Previously completed ticker writes remain committed by their individual sub-steps. The safe restart procedure is to rerun the fresh plan or run a new check if the plan is stale.
