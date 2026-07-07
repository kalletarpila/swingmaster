# SwingMaster Quarter Update UI Vintage Integration Investigation

## Scope

This is an investigation-only note for wiring the existing SwingMaster fundamentals UI to the default-off quarter update vintage flags. No runtime behavior was changed.

## Critical Review Result

The requested direction is technically coherent, but implementation should not start before addressing two integration details:

- The UI currently spawns `run_fundamental_quarter_update.py` as a script path, not `python -m swingmaster.cli.run_fundamental_quarter_update`. That is compatible with the current UI executor because it sets `PYTHONPATH` to the project root.
- The UI summary parser expects a `SUMMARY:` block, while `run_fundamental_quarter_update.py` prints `SUMMARY key=value` lines. Unless this is fixed or intentionally supported, new vintage completion fields may run successfully but not appear in the UI summary panel.

Open questions for the implementation phase:

- Should USA PIT/vintage be checked by default in the UI after the first validated production run, or remain an explicit operator toggle?
- Should `available_at_utc` be a UI-entered operator decision timestamp, or should the first UI implementation use one explicit run timestamp for both `available_at_utc` and `ingested_at_utc`?
- Should the preflight block the run on any non-`READY_NOOP` status, or allow an operator override for known transitional states?

## Current UI Call Path

The USA quarter update starts from the Flet UI in `ui_fundamental_pipeline`.

```text
MarketPanel quarter update button
-> MarketPanel._on_quarter_update_click
-> SwingMasterApp._run_usa_update
-> build_usa_update_command
-> SwingMasterApp._execute_single_command in a Flet background thread
-> ProcessExecutor.execute
-> subprocess.Popen([.../run_fundamental_quarter_update.py, args...])
-> run_fundamental_quarter_update.py
-> stdout/stderr streamed into ExecutionOutputPanel
```

Layer details:

- `ui_fundamental_pipeline/components/market_panel.py`
  - `MarketPanel.__init__` creates `quarter_update_btn`.
  - `_on_quarter_update_click` calls `on_lock(True)` and then `on_quarter_update()`.
- `ui_fundamental_pipeline/main.py`
  - `SwingMasterApp.__init__` wires the USA panel with `on_quarter_update=self._run_usa_update`.
  - `_run_usa_update` calls `get_run_id_usa()`, builds the command, and schedules `_execute_single_command(...)` through `page.run_thread`.
  - `_execute_single_command` clears output, runs the executor, sets the market status badge to green/red based on exit code, and unlocks the UI.
- `ui_fundamental_pipeline/command_builder.py`
  - `build_usa_update_command(run_id)` builds the exact CLI command list.
- `ui_fundamental_pipeline/executor.py`
  - `ProcessExecutor.execute` calls `subprocess.Popen`, captures stdout/stderr on daemon threads, queues lines to the UI, and parses summaries.
- `ui_fundamental_pipeline/components/execution_output.py`
  - `add_line` displays the rolling log.
  - `set_summary` renders parsed summary key/value pairs.

The path is subprocess-based, not a direct Python function call.

## Current Arguments

`build_usa_update_command(run_id)` currently passes:

```text
<PROJECT_ROOT>/.venv/bin/python
<PROJECT_ROOT>/swingmaster/cli/run_fundamental_quarter_update.py
--db <PROJECT_ROOT>/fundamentals_usa.db
--osakedata-db /home/kalle/projects/rawcandle/data/osakedata.db
--run-id <run_id>
--market usa
```

`build_fin_update_command(run_id)` uses the same quarter update CLI with:

```text
--db <PROJECT_ROOT>/fundamentals_fin.db
--osakedata-db /home/kalle/projects/rawcandle/data/osakedata.db
--run-id <run_id>
--market omxh
```

No current UI quarter update command passes:

- `--write-vintage`
- `--vintage-mode`
- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`
- `--vintage-yahoo-aware-action`
- `--ticker`
- `--limit`
- `--dry-run`
- `--skip-ack`

## Current Config And State Findings

Static UI configuration lives in `ui_fundamental_pipeline/config.py`.

- `FUNDAMENTALS_USA_DB = PROJECT_ROOT / "fundamentals_usa.db"`
- `FUNDAMENTALS_FIN_DB = PROJECT_ROOT / "fundamentals_fin.db"`
- `OSAKEDATA_DB = Path("/home/kalle/projects/rawcandle/data/osakedata.db")`
- `ANALYSIS_DB = Path("/home/kalle/projects/rawcandle/data/analysis.db")`
- `PYTHON_EXECUTABLE = PROJECT_ROOT / ".venv" / "bin" / "python"`
- `CLI_QUARTER_UPDATE = PROJECT_ROOT / "swingmaster" / "cli" / "run_fundamental_quarter_update.py"`

Run IDs are generated in `config.py`:

- USA quarter update: `USA_QUARTER_UPDATE_YYYY-MM-DD`
- FIN quarter update: `FIN_YAHOO_BATCH_YYYY-MM-DD`
- FIN classification/TTM/recovery use separate `OMXH_*_YYYY_MM_DD` IDs.

The UI does not currently have a persisted fundamentals update settings model. It has in-memory Flet component state for ticker inputs and button state, plus static constants in `config.py`.

Ticker filters exist for snapshot generation only. Quarter update has no UI ticker/limit controls.

Logs and summaries are held in memory by `ExecutionOutputPanel`. The export button writes `pipeline_execution.log` only when clicked.

## CLI Vintage Surface

`run_fundamental_quarter_update.py` already supports the needed default-off vintage arguments:

- `--write-vintage`
- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`
- `--vintage-normalization-run-id`
- `--vintage-mode`
- `--vintage-yahoo-aware-action`

For the requested production path, the relevant mode is:

```text
--write-vintage
--vintage-mode sec_latest_writer
--vintage-market usa
--vintage-yahoo-aware-action plan_only
```

`preflight_quarter_update_vintage_readiness.py` is a read-only preflight CLI. It accepts:

```text
--fundamentals-db <path>
--market <market>
--source-run-id <optional>
--format json|text
```

It opens SQLite with `mode=ro` and `PRAGMA query_only=ON`.

## Recommended Minimal UI Change

Keep CLI vintage behavior default-off. Add the opt-in at the UI command-building layer.

Recommended implementation shape:

- Add a small options object or dataclass in `ui_fundamental_pipeline/command_builder.py`, for example `QuarterUpdateVintageOptions`.
- Extend `build_usa_update_command(...)` with an optional `vintage_options` argument.
- Keep `build_fin_update_command(...)` unchanged unless a future FIN vintage design is explicitly added.
- Add a USA-only checkbox/toggle in `MarketPanel`, labelled close to `Enable PIT/vintage write for USA quarterly update`.
- In `SwingMasterApp._run_usa_update`, read that option, generate timestamps/run IDs once, optionally run the read-only preflight command first, then run quarter update.
- Use `_execute_command_chain(...)` for preflight + quarter update when vintage is enabled.
- Do not use `--vintage-yahoo-aware-action write` in the first UI implementation.

Initial command when enabled:

```text
--write-vintage
--vintage-mode sec_latest_writer
--vintage-market usa
--vintage-available-at-utc <run_timestamp_utc>
--vintage-ingested-at-utc <run_timestamp_utc>
--vintage-run-id <vintage_run_id>
--vintage-yahoo-aware-action plan_only
```

The preflight command should be:

```text
<python> <PROJECT_ROOT>/swingmaster/cli/preflight_quarter_update_vintage_readiness.py
--fundamentals-db <PROJECT_ROOT>/fundamentals_usa.db
--market usa
--format json
```

If the preflight supports a meaningful `--source-run-id` for this use case later, pass it explicitly. For the current "before update" check, omitting it is simpler and avoids filtering for a run ID that does not exist yet.

## Toggle Default Recommendation

Use an explicit UI toggle initially, default unchecked.

Reasoning:

- The CLI intentionally keeps vintage writes default-off.
- The first UI integration should prove command construction, preflight sequencing, and summary rendering without silently changing an operator workflow.
- After one manual production validation, the default can be revisited and potentially changed to checked for USA only.

## Timestamp And Run ID Recommendation

Generate one timezone-aware UTC timestamp when the operator launches the USA quarter update with vintage enabled.

Recommended source run ID:

```text
USA_QUARTER_UPDATE_YYYY-MM-DD__QUARTERLY
```

Recommended vintage run ID:

```text
USA_QUARTER_UPDATE_YYYY-MM-DD__SEC_LATEST_WRITER_VINTAGE
```

Recommended timestamps:

```text
available_at_utc = explicit operator decision timestamp, ISO UTC
ingested_at_utc = same explicit launch timestamp for the first UI implementation
```

Do not derive `available_at_utc` from `period_end_date`.

Current `get_run_id_usa()` returns `USA_QUARTER_UPDATE_YYYY-MM-DD`. A minimal implementation can either preserve this for compatibility or update it with tests to the `__QUARTERLY` suffix. If changing the run ID format risks downstream assumptions, keep the current source run ID and only derive the vintage run ID as:

```text
<source_run_id>__SEC_LATEST_WRITER_VINTAGE
```

## Summary And Status Display Recommendation

First fix or extend summary parsing so UI can read quarter update's current `SUMMARY key=value` output as well as the existing `SUMMARY:` block format. Without this, the summary panel may miss all quarter update summary fields.

Surface these fields prominently:

- `vintage_requested`
- `vintage_mode`
- `vintage_execution_enabled`
- `vintage_sec_latest_writer_status`
- `vintage_post_run_parity_status`
- `vintage_completion_status`
- `vintage_next_required_action`
- `vintage_yahoo_aware_planning_status`
- `vintage_yahoo_aware_execution_status`
- `vintage_latest_without_vintage_count`
- `vintage_blocked_rows`
- unknown provenance / missing provenance counts where present

Recommended UI severity mapping:

- Green/success: `vintage_completion_status=SEC_VINTAGE_SUFFICIENT` and `vintage_post_run_parity_status=OK`.
- Yellow/review: `FINAL_MIXED_REQUIRED` or `YAHOO_VINTAGE_REQUIRED`.
- Red/stop: `BLOCKED_POST_RUN_DRIFT`, `UNKNOWN`, blocked rows, parity drift, duplicate vintage IDs, or unknown run linkage.

The current market status badge only shows `exit=<code>`. The implementation should keep that but add a vintage-specific status line or badge derived from parsed summary fields.

## Proposed Implementation Tests

Add pure unit tests only. Do not call providers, network APIs, scheduler jobs, or real DB writes.

Recommended tests:

- `build_usa_update_command` omits vintage flags by default.
- `build_usa_update_command` includes all required vintage flags when options are enabled.
- USA quarter update command still uses `/home/kalle/projects/rawcandle/data/osakedata.db`.
- `SwingMasterApp._run_usa_update` uses a command chain of preflight then quarter update when the USA vintage toggle is enabled.
- Vintage preflight command uses `preflight_quarter_update_vintage_readiness.py`, `--fundamentals-db`, `--market usa`, and `--format json`.
- The UI does not add `--vintage-yahoo-aware-action write`.
- Run timestamp generation is stable within one launch: source command and vintage flags use the same generated timestamp/run IDs.
- `ProcessExecutor._parse_summary_block` or a renamed parser handles both `SUMMARY:` blocks and `SUMMARY key=value` lines.
- `ExecutionOutputPanel` or a small presentation helper maps `SEC_VINTAGE_SUFFICIENT`, `FINAL_MIXED_REQUIRED`, `YAHOO_VINTAGE_REQUIRED`, `BLOCKED_POST_RUN_DRIFT`, and `UNKNOWN` to the intended display severities.
- Existing default FIN and USA command builder tests remain unchanged for non-vintage behavior.

## Risks

- Summary parser mismatch can make the UI appear blind to successful vintage outcomes.
- A checked-by-default toggle would change production behavior even though CLI vintage is default-off.
- A preflight that blocks on transitional existing DB state may prevent the quarter update that would repair that state. The exact blocking policy needs an operator decision.
- Current run IDs are date-only, so repeated runs on the same day reuse IDs. That may be intentional, but vintage runs should make duplicate/no-op behavior explicit in UI status.
- The UI runs scripts by path. This currently works because the executor injects `PYTHONPATH`, but tests should preserve that behavior.

## Implementation Plan

1. Add unit-tested command builders for vintage options and the read-only preflight command.
2. Add a USA-only toggle in `MarketPanel`; keep it default unchecked.
3. Generate launch timestamp, source run ID, and vintage run ID in `SwingMasterApp._run_usa_update`.
4. When enabled, execute preflight then quarter update through the existing command-chain path.
5. Extend summary parsing for quarter update's `SUMMARY key=value` output.
6. Add a vintage summary/status presentation helper.
7. Run only UI unit tests and targeted pure quarter update parser/command tests.

## Verification

No runtime commands, provider calls, scheduler jobs, refresh jobs, or real DB writes were run for this investigation.

## Phase 4L2 Follow-Up

Phase 4L2 implemented the first UI-side vintage opt-in support described by this investigation:

- USA-only checkbox, initially unchecked.
- Command-builder support for `sec_latest_writer` vintage flags.
- Read-only vintage readiness preflight before opt-in USA quarter update.
- Summary parser support for `SUMMARY key=value` output.
- Pure UI severity mapping for vintage completion statuses.

See [SwingMaster Quarter Update UI Vintage Integration Phase 4L2](swingmaster_quarter_update_ui_vintage_integration_phase4l2.md).
