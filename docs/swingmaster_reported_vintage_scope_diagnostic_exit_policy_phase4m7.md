# SwingMaster Vintage Scope Diagnostic Exit Policy Phase 4M7

Date: 2026-07-07

## Purpose

Phase 4M7 fixes `swingmaster.cli.diagnose_quarter_update_vintage_scope` exit-code behavior after the GIS provider-derived vintage apply.

Before this phase, the CLI produced a correct JSON summary:

- overall_diagnostic_status: `NO_MISMATCH`
- value_mismatch_count: `0`
- unexplained_mismatch_count: `0`

but still returned exit code `1`. That was wrong for UI and automation.

## Exit Policy

Exit code `0`:

- `NO_MISMATCH`
- `SCOPE_FIX_VERIFIED_BLOCKED_NARROW`

Exit code nonzero:

- `PARITY_DRIFT`
- `SCOPE_FIX_FAILED_STILL_BROAD`
- `UNKNOWN`
- invalid DB path
- schema/internal errors

`SCOPE_FIX_VERIFIED_BLOCKED_NARROW` remains a successful diagnostic verification state because it means the planner scope fix is behaving narrowly and no broad apply should run.

## Real DB Read-Only Verification

Command:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.diagnose_quarter_update_vintage_scope \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --source-run-id USA_QUARTER_UPDATE_2026-07-07__QUARTERLY \
  --enrich-run-id USA_QUARTER_UPDATE_2026-07-07__QUARTERLY__ENRICH \
  --format json \
  --sample-limit 20
```

Result:

- process exit code: `0`
- overall_diagnostic_status: `NO_MISMATCH`
- value_mismatch_count: `0`
- unexplained_mismatch_count: `0`
- latest_without_vintage_count: `0`
- vintage_without_latest_count: `0`
- duplicate_statement_vintage_id_count: `0`

No real DB writes, quarter_update, providers, scheduler, refresh, recovery, backfill, or apply were run.

## Verification

Checks run:

```bash
PYTHONPATH=. pytest -q swingmaster/tests/test_diagnose_quarter_update_vintage_scope.py
python3 -m py_compile swingmaster/cli/diagnose_quarter_update_vintage_scope.py
```

Result:

- `15 passed`
