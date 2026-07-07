# SwingMaster Quarter Update UI Yahoo-Aware Auto Apply Phase 4L5

Date: 2026-07-07

## Purpose

Phase 4L5 adds a safe automatic follow-up apply step to the USA UI vintage workflow.

When the operator starts USA quarter update with the PIT/vintage checkbox enabled, the UI still runs:

```text
preflight_quarter_update_vintage_readiness
quarter_update with --vintage-yahoo-aware-action plan_only
```

Only after the primary quarter update summary is parsed can the UI decide whether to run the provider-free Yahoo-aware apply CLI.

## Auto Apply Gate

Auto apply is allowed only when both conditions are true:

- the user enabled the USA PIT/vintage checkbox for the primary run
- the existing Phase 4L4 apply gate says the latest USA summary is safe

The safe summaries are:

- `FINAL_MIXED_REQUIRED` with `FINAL_MIXED_PLAN_READY` and positive planned final mixed rows
- `YAHOO_VINTAGE_REQUIRED` with `YAHOO_VINTAGE_PLAN_READY` and positive planned Yahoo vintage rows

Auto apply is disabled for:

- `SEC_VINTAGE_SUFFICIENT`
- `BLOCKED_POST_RUN_DRIFT`
- `UNKNOWN`
- missing source run id
- missing planned row counts
- blocked rows
- unknown provenance fields
- duplicate or inconsistent post-run vintage counts
- post-run parity statuses that block the Phase 4L4 gate
- primary runs where the PIT/vintage checkbox was off

## Command Policy

The primary quarter update remains planning-only for Yahoo-aware execution:

```text
--vintage-yahoo-aware-action plan_only
```

The auto follow-up uses the same standalone provider-free apply CLI added in Phase 4L4:

```text
python -m swingmaster.cli.apply_quarter_update_yahoo_aware_vintage
```

The auto apply command includes the explicit approval token because it is built only after the UI gate passes. It does not call `run_fundamental_quarter_update.py` and does not include provider flags.

## UI Status Fields

The UI records the auto decision in the summary panel:

- `vintage_yahoo_aware_auto_apply_attempted`
- `vintage_yahoo_aware_auto_apply_reason`

Phase 4L6 smoke coverage verifies that these fields are preserved in the final combined UI summary after a successful auto apply.

Apply execution summaries continue to map as:

- `EXECUTION_COMPLETED` -> success
- `NO_ACTION_REQUIRED` -> review
- `EXECUTION_BLOCKED`, errors, or blocked rows -> stop

After a successful auto apply, the manual apply button is disabled with an `Auto apply completed.` tooltip. If auto apply is not run but the Phase 4L4 manual gate is safe, the manual button can remain enabled.

## Real DB And Provider Status

Phase 4L5 did not run a real quarter update, provider call, scheduler, refresh job, or real apply.

Phase 4L5 did not write:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- `/home/kalle/projects/rawcandle/data/osakedata.db`

Tests use fake executors, mocks, and temp DB paths only.

## Phase 4L6 Smoke Coverage

Phase 4L6 adds full UI workflow smoke tests for SEC-sufficient, final mixed auto apply, Yahoo vintage auto apply, blocked/unsafe summaries, preflight failure, checkbox-disabled default behavior, and user stop behavior.

See [SwingMaster Quarter Update UI Vintage Full Workflow Smoke Phase 4L6](swingmaster_quarter_update_ui_vintage_full_workflow_smoke_phase4l6.md).
