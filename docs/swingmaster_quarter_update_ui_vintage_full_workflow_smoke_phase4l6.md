# SwingMaster Quarter Update UI Vintage Full Workflow Smoke Phase 4L6

Date: 2026-07-07

## Purpose

Phase 4L6 adds full UI workflow smoke coverage for the USA PIT/vintage quarter update path before the first real UI-triggered Q run.

The tests execute the UI controller with fake command execution only. They do not start subprocesses, call providers, run schedulers, run refresh jobs, or write real databases.

## Scenarios Tested

The smoke suite covers:

- SEC-sufficient primary summary
- final mixed required with successful auto apply
- Yahoo vintage required with successful auto apply
- blocked or unsafe primary summaries
- unknown provenance fields
- missing source run id
- `BLOCKED_POST_RUN_DRIFT`
- `UNKNOWN`
- preflight failure
- USA checkbox disabled default path
- user stop before command execution

## Command Ordering Verified

With the USA PIT/vintage checkbox enabled, the UI must execute commands in this order:

```text
preflight_quarter_update_vintage_readiness
run_fundamental_quarter_update.py ... --vintage-yahoo-aware-action plan_only
optional provider-free Yahoo-aware apply
```

The primary quarter update command is verified to use:

```text
--vintage-yahoo-aware-action plan_only
```

It is also verified not to use:

```text
--vintage-yahoo-aware-action write
```

## Auto-Apply Safety Verified

The auto-apply command runs only after a safe primary summary. It uses:

```text
python -m swingmaster.cli.apply_quarter_update_yahoo_aware_vintage
```

The auto-apply command is verified to:

- include the explicit approval token only in the gated apply path
- omit `run_fundamental_quarter_update.py`
- omit provider flags
- omit `--vintage-yahoo-aware-action`

The UI preserves these final summary fields after auto apply:

- `vintage_yahoo_aware_auto_apply_attempted`
- `vintage_yahoo_aware_auto_apply_reason`
- apply execution fields such as `vintage_yahoo_aware_execution_status`

After successful auto apply, the manual apply button is disabled.

## No-Provider And No-Real-DB Status

Phase 4L6 did not run:

- real `quarter_update`
- SEC, Yahoo, yfinance, Finnhub, paid-provider, or network APIs
- scheduler jobs
- refresh jobs
- real apply

Phase 4L6 did not write:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- `/home/kalle/projects/rawcandle/data/osakedata.db`

The tests use mocked subprocess protection and fake executor responses.

## Remaining Step

The remaining operational step is the first real UI-triggered USA quarter update with the PIT/vintage checkbox intentionally enabled, after operator-managed DB backup and readiness review.

Phase 4L7 adds a UI recovery action for the opposite case: a USA quarter update was run without the PIT/vintage checkbox and latest rows are missing vintage rows. See [SwingMaster Quarter Update UI Vintage Recovery Phase 4L7](swingmaster_quarter_update_ui_vintage_recovery_phase4l7.md).
