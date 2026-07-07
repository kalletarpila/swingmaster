# SwingMaster Quarter Update UI Yahoo-Aware Apply Phase 4L4

Date: 2026-07-07

## Purpose

Phase 4L4 adds UI support for a second explicit action after a USA quarter update vintage opt-in run reports that Yahoo-aware or final mixed vintage corrections are required.

The primary USA quarter update remains planning-only for Yahoo-aware execution. Phase 4L5 can run a gated automatic follow-up apply after a vintage opt-in primary run, but the primary command itself still never writes Yahoo-aware corrections.

## Why A Second Action Is Needed

The UI is the production operator entry point. If the first USA quarter update returns:

- `FINAL_MIXED_REQUIRED`
- `YAHOO_VINTAGE_REQUIRED`

the operator needs a UI-driven way to apply the already-planned corrections without rerunning provider update paths.

## Primary Quarter Update Policy

The primary quarter update still uses:

```text
--vintage-yahoo-aware-action plan_only
```

It never uses:

```text
--vintage-yahoo-aware-action write
```

Yahoo-aware/final mixed apply is a separate provider-free command path. In Phase 4L4 it is exposed by an explicit UI button; in Phase 4L5 the same command can also run automatically after the primary plan-only summary passes the same gate.

## Gating Rules

The apply action is disabled by default.

It is enabled only when the latest USA quarter update summary includes:

- `vintage_completion_status=FINAL_MIXED_REQUIRED` with `vintage_yahoo_aware_planning_status=FINAL_MIXED_PLAN_READY`
- or `vintage_completion_status=YAHOO_VINTAGE_REQUIRED` with `vintage_yahoo_aware_planning_status=YAHOO_VINTAGE_PLAN_READY`
- source `run_id`
- positive planned row count for the relevant apply type
- zero blocked rows
- no unknown provenance fields
- no duplicate/inconsistent post-run vintage counts

It is disabled for:

- `SEC_VINTAGE_SUFFICIENT`
- `BLOCKED_POST_RUN_DRIFT`
- `UNKNOWN`
- blocked rows
- unknown provenance fields
- missing planner counts
- missing source run id

## Command Path

The UI builds a provider-free apply command:

```text
<PROJECT_ROOT>/.venv/bin/python
-m
swingmaster.cli.apply_quarter_update_yahoo_aware_vintage
--fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db
--market usa
--source-run-id USA_QUARTER_UPDATE_YYYY-MM-DD__QUARTERLY
--vintage-run-id USA_QUARTER_UPDATE_YYYY-MM-DD__YAHOO_AWARE_VINTAGE
--available-at-utc <UTC apply timestamp>
--ingested-at-utc <same UTC apply timestamp>
--approval-token USER_APPROVES_YAHOO_AWARE_VINTAGE_APPLY
```

The command does not call `run_fundamental_quarter_update.py` and does not include provider flags.

## Confirmation And Approval Policy

The standalone apply CLI defaults to no-write behavior unless the approval token is exact:

```text
USER_APPROVES_YAHOO_AWARE_VINTAGE_APPLY
```

The UI adds this token only for the explicit apply button action or the Phase 4L5 gated auto-apply follow-up. The primary quarter update command never adds it.

## Parser And Status Display

The UI parser already supports `SUMMARY key=value`. Phase 4L4 extends status mapping for apply summaries such as:

- `vintage_yahoo_aware_execution_status`
- `vintage_yahoo_aware_final_mixed_rows_written`
- `vintage_yahoo_aware_yahoo_vintage_rows_written`
- `vintage_yahoo_aware_provenance_rows_written`
- `vintage_yahoo_aware_rows_blocked`
- `vintage_yahoo_aware_error`

Mapping:

- `EXECUTION_COMPLETED` with zero blocked rows and no error -> success
- `NO_ACTION_REQUIRED` -> review
- `EXECUTION_BLOCKED`, errors, or blocked rows -> stop

## Real DB And Provider Status

Phase 4L4 did not run a real quarter update, provider call, scheduler, refresh job, or real apply.

Phase 4L4 did not write:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- `/home/kalle/projects/rawcandle/data/osakedata.db`

Tests use mocks and temp DBs only.

## Phase 4L5 Follow-Up

Phase 4L5 adds a safe automatic follow-up after the USA PIT/vintage checkbox workflow. The auto path uses the same provider-free apply CLI and the same Phase 4L4 gate, records `vintage_yahoo_aware_auto_apply_attempted` and `vintage_yahoo_aware_auto_apply_reason`, and disables the manual apply button after successful auto execution.

See [SwingMaster Quarter Update UI Yahoo-Aware Auto Apply Phase 4L5](swingmaster_quarter_update_ui_yahoo_aware_auto_apply_phase4l5.md).

## Next Real Apply

Before any real UI apply:

- review the first quarter update summary and planned counts
- confirm no blocked rows or unknown provenance fields
- verify a DB backup exists
- run the apply command only through explicit operator action
- preserve the complete apply summary output
