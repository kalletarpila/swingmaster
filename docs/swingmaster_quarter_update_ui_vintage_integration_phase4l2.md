# SwingMaster Quarter Update UI Vintage Integration Phase 4L2

Date: 2026-07-07

## What Changed

Phase 4L2 adds the first UI-side support for launching the USA quarter update with explicit PIT/vintage opt-in metadata.

The CLI remains default-off. No default CLI behavior was changed.

## UI Option Behavior

The USA market panel now has a checkbox:

```text
Enable PIT/vintage write for USA quarterly update
```

It is initially unchecked. It is only present on the USA panel and only affects the USA quarter update button. FIN/non-USA quarter update behavior is unchanged.

When the UI is locked for a run, the checkbox is disabled together with the action buttons.

## Command Flags Added

When the USA checkbox is enabled, the UI adds these flags to the existing quarter update command:

```text
--write-vintage
--vintage-mode sec_latest_writer
--vintage-market usa
--vintage-available-at-utc <UTC launch timestamp>
--vintage-ingested-at-utc <same UTC launch timestamp>
--vintage-run-id USA_QUARTER_UPDATE_YYYY-MM-DD__SEC_LATEST_WRITER_VINTAGE
--vintage-yahoo-aware-action plan_only
```

The UI never sets:

```text
--vintage-yahoo-aware-action write
```

The USA OHLCV path remains:

```text
/home/kalle/projects/rawcandle/data/osakedata.db
```

## Timestamp And Run-ID Convention

The UI generates one UTC launch timestamp and reuses it for:

- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`

The USA source run ID now follows the UI integration convention:

```text
USA_QUARTER_UPDATE_YYYY-MM-DD__QUARTERLY
```

The vintage run ID is derived from it:

```text
USA_QUARTER_UPDATE_YYYY-MM-DD__SEC_LATEST_WRITER_VINTAGE
```

## Preflight Status

Preflight is implemented for the opt-in path. When the USA vintage checkbox is enabled, the UI runs this read-only preflight command before the quarter update:

```text
<PROJECT_ROOT>/.venv/bin/python
-m
swingmaster.cli.preflight_quarter_update_vintage_readiness
--fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db
--market usa
--format json
```

The UI uses the existing command-chain execution path, so a non-zero preflight exit stops the chain before the quarter update command.

## Parser Fix

`ProcessExecutor` now supports both summary output formats:

```text
SUMMARY:
key=value
```

and:

```text
SUMMARY key=value
SUMMARY other_key=other_value
```

This allows the UI summary panel to display quarter update vintage fields emitted by `run_fundamental_quarter_update.py`.

## Severity Mapping

Phase 4L2 adds a pure UI mapping function:

```python
map_vintage_completion_status_to_ui_severity(summary: dict) -> str
```

It returns:

- `success` for `SEC_VINTAGE_SUFFICIENT` with OK parity and zero blocking counts.
- `review` for `FINAL_MIXED_REQUIRED`, `YAHOO_VINTAGE_REQUIRED`, or planned follow-up rows.
- `stop` for `BLOCKED_POST_RUN_DRIFT`, `UNKNOWN`, drift counts, duplicate statement vintage IDs, blocked rows, or unknown provenance fields.
- `unknown` when no vintage summary fields are present.

The USA status badge includes the parsed vintage completion status and severity when those fields are available.

## Real DB And Provider Status

Phase 4L2 did not run a real quarter update, scheduler, refresh job, provider call, or network call.

Phase 4L2 did not write:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- `/home/kalle/projects/rawcandle/data/osakedata.db`

## Next Phase

Recommended next phase:

- run the Phase 4L3 no-provider UI smoke before any real provider run
- manually inspect the UI in a browser if desired
- run a controlled dry command-builder review
- perform the production run only after operator confirmation and DB backup
- keep Yahoo-aware execution write out of the UI until the first opt-in run summary is reviewed

Phase 4L3 no-provider smoke coverage is documented in [SwingMaster Quarter Update UI Vintage Smoke Phase 4L3](swingmaster_quarter_update_ui_vintage_smoke_phase4l3.md).

Phase 4L4 adds a separate explicit UI action for planned Yahoo-aware/final mixed vintage apply, documented in [SwingMaster Quarter Update UI Yahoo-Aware Apply Phase 4L4](swingmaster_quarter_update_ui_yahoo_aware_apply_phase4l4.md). The primary quarter update remains `plan_only`.
