# SwingMaster Quarter Update UI Vintage Smoke Phase 4L3

Date: 2026-07-07

## Purpose

Phase 4L3 verifies the USA quarter update UI vintage opt-in path without running a real quarter update or any provider job.

## What Was Verified

The smoke tests verify:

- checkbox off uses the default single-command path
- checkbox off omits all vintage flags
- checkbox on builds a two-command chain
- preflight is first
- quarter update is second
- the quarter update command includes the required `sec_latest_writer` vintage flags
- the UI never emits `--vintage-yahoo-aware-action write`
- preflight failure stops before the quarter update command
- `SUMMARY key=value` vintage fields parse into the UI summary dict
- vintage completion statuses map to `success`, `review`, `stop`, or `unknown`

## Command-Chain Behavior

Default unchecked USA path:

```text
quarter_update only
```

Vintage opt-in USA path:

```text
preflight_quarter_update_vintage_readiness
quarter_update with explicit vintage metadata
```

The preflight command is built in module form through the UI Python executable:

```text
<PROJECT_ROOT>/.venv/bin/python
-m
swingmaster.cli.preflight_quarter_update_vintage_readiness
--fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db
--market usa
--format json
```

The quarter update command keeps:

```text
--db /home/kalle/projects/swingmaster/fundamentals_usa.db
--osakedata-db /home/kalle/projects/rawcandle/data/osakedata.db
--run-id USA_QUARTER_UPDATE_YYYY-MM-DD__QUARTERLY
--market usa
```

and opt-in adds:

```text
--write-vintage
--vintage-mode sec_latest_writer
--vintage-market usa
--vintage-available-at-utc <UTC launch timestamp>
--vintage-ingested-at-utc <same UTC launch timestamp>
--vintage-run-id USA_QUARTER_UPDATE_YYYY-MM-DD__SEC_LATEST_WRITER_VINTAGE
--vintage-yahoo-aware-action plan_only
```

## Preflight Failure Behavior

The smoke test uses a fake executor where preflight returns non-zero. The command chain stops immediately after the preflight command, the quarter update command is not executed, and the USA status badge is set to red failure.

## Summary Parser Behavior

The parser was verified with quarter update style lines:

```text
SUMMARY vintage_completion_status=SEC_VINTAGE_SUFFICIENT
SUMMARY vintage_next_required_action=NONE
SUMMARY vintage_post_run_latest_without_vintage_count=0
SUMMARY vintage_yahoo_aware_planning_status=NO_ACTION_REQUIRED
```

It also parses review statuses such as:

```text
SUMMARY vintage_completion_status=FINAL_MIXED_REQUIRED
SUMMARY vintage_next_required_action=CREATE_FINAL_MIXED_VINTAGE
```

## Severity Mapping

Smoke coverage verifies:

- `SEC_VINTAGE_SUFFICIENT` with zero drift maps to `success`
- `FINAL_MIXED_REQUIRED` maps to `review`
- `YAHOO_VINTAGE_REQUIRED` maps to `review`
- `BLOCKED_POST_RUN_DRIFT` maps to `stop`
- `UNKNOWN` maps to `stop`
- missing vintage fields map to `unknown`

The UI status badge uses the same severity mapping when parsed vintage summary fields are available.

## No-Provider And No-DB-Write Status

Phase 4L3 did not run:

- real `quarter_update`
- SEC, Yahoo, yfinance, Finnhub, or paid-provider calls
- scheduler jobs
- refresh jobs

The smoke tests patch process execution so no real subprocess is started.

Phase 4L3 did not write:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- `/home/kalle/projects/rawcandle/data/osakedata.db`

## Next Real UI Run Recommendation

Before the first real UI-triggered vintage opt-in run:

- confirm the USA checkbox is intentionally enabled
- verify an operator-managed backup of `fundamentals_usa.db`
- run readiness preflight and require `READY_NOOP`
- keep Yahoo-aware execution write out of the UI
- preserve the full terminal/UI summary output after the run

Phase 4L4 adds a separate explicit no-provider apply action for planned Yahoo-aware/final mixed corrections. The primary quarter update path remains planning-only for Yahoo-aware execution.
