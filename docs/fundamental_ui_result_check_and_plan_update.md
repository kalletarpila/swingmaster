# Manual USA Fundamentals Result Check

## Previous Flow

The old USA quarter update UI launched `run_fundamental_quarter_update.py` directly. Its default selection came from `rc_fundamental_quarter_state` where `new_quarter_available = 1`.

That table remains in place for FIN, diagnostics, rollback, and legacy CLI usage. RawCandle is not changed by this workflow.

## New Manual UI Flow

The USA panel now starts with:

```text
Check for New Results
-> inspect result-check artifacts and executable candidates
-> Update Fundamentals
-> Update Percentiles
-> Ticker Snapshot
```

This is manual only. There is no scheduler, cron, systemd timer, or unattended overnight update behavior.

FIN/OMXH keeps its existing quarter-state-driven behavior.

## Result-Check Stages

`swingmaster.fundamentals.result_check.run_manual_result_check` is the reusable application layer used by the CLI and UI.

The check performs these stages:

```text
OHLCV activity assessment
earnings calendar refresh for fetch-enabled USA tickers
completed-event candidate selection
completed earnings-event refresh for targeted candidates
earnings-event/quarter match rebuild when event refresh succeeds
quarter-refresh decision rebuild
plan/artifact write under temp/
```

OHLCV activity reuses the existing operational rule:

```text
latest_ohlcv_date exists and age <= ohlcv_stale_days -> fundamental_fetch_enabled = 1
otherwise -> fundamental_fetch_enabled = 0
```

The default threshold is 14 calendar days. `osakedata.db` is read only.

## Completed-Event Candidates

The check does not refresh completed events for all USA tickers. It targets active/fetch-enabled tickers whose calendar state is:

```text
DUE_TODAY
DATE_PASSED_EVENT_NOT_FOUND within --event-watch-days-after of decision_date
```

Far-future `UPCOMING`, stale/inactive tickers, and old `NO_CURRENT_ESTIMATE` rows are not completed-event refresh candidates in this phase.

## Result-Check Backup Scope

`run_manual_result_check` creates one verified run-level SQLite backup before the first result-check database mutation:

```text
temp/fundamental_result_check/<UTC_TIMESTAMP>/backups/fundamentals_usa.db.pre_result_check.<UTC_TIMESTAMP>.bak
```

Calendar refresh, completed-event refresh, earnings-event match rebuild, and provider timing observation writes are all protected by that one backup during the orchestrated result-check run. The substeps receive an internal verified-backup context and do not create additional full database backups inside result-check. Standalone `refresh_yahoo_earnings_calendar --apply`, `apply_yahoo_earnings_events --apply`, and `rebuild_earnings_event_matches --apply` keep their independent backup behavior.

## Plan Contract

Artifacts are written under:

```text
temp/fundamental_result_check/<UTC_TIMESTAMP>/
```

The directory contains:

```text
plan.json
candidates.csv
manual_review.csv
summary.json
calendar_refresh_summary.json
completed_event_refresh_summary.json
```

`plan.json` includes:

```text
plan_version
created_at_utc
decision_date
fundamentals_db
ohlcv_db
ohlcv_stale_days
candidate_count
candidate_hash
check_status
candidates
```

Executable candidate decisions are limited to:

```text
FETCH_NEW_QUARTER
RETRY_PARTIAL_QUARTER
RETRY_FETCH_FAILED
```

Manual-review, watch, and no-action rows are never executable.

## Target Period Safety

An executable candidate must have a non-null deterministic `target_period_end_date`. The preferred evidence is `rc_fundamental_quarter_earnings_match`.

The workflow does not infer fiscal quarter ends from approximate announcement-date math. If a completed event cannot be safely mapped to a period, the row remains manual review with no executable action.

## Failure Policy

If the material calendar refresh stage fails, the check returns:

```text
check_status = FAILED
candidate_count = 0
```

If a bounded completed-event subset has failures, the check returns:

```text
check_status = PARTIAL
candidate_count = 0
```

The UI keeps `Update Fundamentals` clickable, but `PARTIAL` plans do not become executable update inputs. The user must run a successful check before a plan-based update can proceed.

## Plan-Mode Executor

`run_fundamental_quarter_update.py` supports USA-only plan execution:

```bash
.venv/bin/python swingmaster/cli/run_fundamental_quarter_update.py \
  --db fundamentals_usa.db \
  --market usa \
  --run-id USA_QUARTER_UPDATE_... \
  --quarter-refresh-plan-json temp/fundamental_result_check/.../plan.json
```

Before provider calls or DB writes, plan mode validates:

```text
path is under repository temp/
plan_version
check_status = SUCCESS
fundamentals_db matches
decision_date equals the current operational USA decision date
created_at_utc parses
candidate_count and candidate_hash match rows
market = usa
decision is executable
fundamental_fetch_enabled = 1
eligible_for_execution = 1
target_period_end_date is non-null
no duplicate ticker/target rows
```

Plan mode does not call `load_eligible_rows()` and does not require `rc_fundamental_quarter_state.new_quarter_available = 1`. It also does not acknowledge/reset quarter-state rows.

## Raw Scope vs Normalized Scope

Provider payload scope and normalized repair scope are intentionally separate.

SEC `companyfacts` and Yahoo quarterly raw/cache ingestion may remain ticker-wide because the provider APIs naturally return multi-period payloads and the repair may need earlier raw facts in memory to derive the target quarter correctly. This is provider caching/input scope, not a license to rewrite all normalized historical periods.

For `--quarter-refresh-plan-json` execution, the normalized repair target is the explicit:

```text
ticker + target_period_end_date
```

The SEC quarterly builder still reconstructs all available provider periods in memory, then writes only the normalized period compatible with the target date using the existing same-calendar-quarter and seven-day tolerance rule. Unrelated `rc_fundamental_quarterly` periods are left untouched. A new `run_id` alone is not a material quarterly change and must not replace an otherwise identical normalized row.

Yahoo fallback follows the same normalized target scope in plan mode. It may read ticker-wide Yahoo cache rows, but it only inserts/enriches the target-compatible normalized quarter. Existing SEC values remain authoritative; Yahoo fills only missing normalized fields.

Full-history workflows remain available by omitting the target-scope option. Initial bootstrap, explicit historical backfill, and legacy full reconstruction can continue to write all reconstructed periods.

## Downstream Scope

Plan-mode TTM recalculation is target-dependent rather than ticker-wide. For a repaired quarter `Q`, the candidate TTM rows are the `as_of_date` rows whose rolling four-quarter input window contains `Q`. The builder may read the full quarterly series to compute those rows correctly, including YoY/trend fields, but it writes only new or materially changed TTM rows. A `run_id`-only TTM replacement is ignored.

Lifecycle and score calculations still read the ticker history needed for correct classification, scoring, and consistency components. Lifecycle write scope is limited to the TTM rows materially written by the target repair. Score write scope starts with those TTM rows and expands by up to three later TTM `as_of_date` rows because the consistency component uses the current row plus recent history. Unchanged lifecycle/score values are not rewritten.

## Update Behavior

All executable plan decisions use the existing USA source precedence:

```text
SEC fetch/reconstruction
-> generic quarterly
-> Yahoo fallback insert/enrich
-> target-quarter verification
-> TTM
-> lifecycle
-> score
-> valuation, only when at least one candidate materially changed quarterly/TTM fundamentals inputs
```

`FETCH_NEW_QUARTER` fills a missing target quarter. `RETRY_PARTIAL_QUARTER` reruns the same safe fill path without deleting existing values. `RETRY_FETCH_FAILED` uses the same explicit target and provider path.

The quarter-update executor is not the independent daily price-driven valuation refresh path. Its USA valuation step runs once at batch end only when the batch records a material fundamentals change, such as a target quarterly value insert/update, a dependent TTM value insert/update, or a lifecycle/score value update caused by those fundamentals. Status-only and `run_id`-only changes do not trigger valuation. Zero-candidate plans, all-failed batches, and no-material-change replay batches skip valuation.

After provider work, the executor reassesses:

```text
quarter_basic_complete
ttm_input_complete
score_history_complete
```

`ttm_input_complete` and `score_history_complete` remain quality metadata, not hard gates.

## UI Controls

The USA panel shows:

```text
Check for New Results
compact status/count summary
first executable candidates
Update Fundamentals
Run USA Score Percentile
Generate USA Snapshots
```

`Update Fundamentals` remains clickable so manual operation is not tied to a just-completed UI session. The click handler first uses a valid UI-session plan when present; otherwise it searches `temp/fundamental_result_check/*/plan.json` for the newest successful USA plan whose `decision_date` matches the current USA latest-close decision date and whose basic structure, database path, candidate count, and candidate hash are valid. If no valid same-date plan exists it asks for a fresh check, and if the latest valid plan has zero executable candidates it reports that there is nothing to update. Backend plan validation remains authoritative for stale or invalid plans. The UI stores only:

```text
latest_plan_path
latest_plan_created_at
latest_candidate_count
latest_candidate_hash
```

The plan JSON remains the canonical execution input.

Percentiles remain a separate manual step and are not run automatically by `Update Fundamentals`.

## Rollback

To roll back operationally, do not use `Check for New Results` in the USA UI and continue using legacy CLI mode without `--quarter-refresh-plan-json`. FIN is already unchanged.

The quarter-state table and `new_quarter_available` column are intentionally retained until a later RawCandle flag retirement phase.
