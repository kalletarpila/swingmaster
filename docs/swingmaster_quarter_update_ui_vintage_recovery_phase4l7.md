# SwingMaster Quarter Update UI Vintage Recovery Phase 4L7

Date: 2026-07-07

## Purpose

Phase 4L7 adds a USA-only UI action for repairing missing PIT/vintage rows when USA quarter update was accidentally run without the PIT/vintage checkbox.

The UI action is:

```text
Repair missing PIT/vintage rows
```

It is designed so the operator does not need to remember recovery commands weeks after the original mistake.

## Why Recovery Is Needed

If USA quarter update runs without the PIT/vintage checkbox, new rows can be written to `rc_fundamental_quarterly` without matching rows in `rc_fundamental_quarterly_vintage`. That creates latest/vintage parity drift that should be repaired without rerunning the provider update.

## UI Button Behavior

The button is visible only in the USA panel. It is safe to click anytime because the first command is read-only readiness preflight.

The button is disabled while another UI run is active. It does not change the default quarter update checkbox state.

## No-Provider Recovery Design

The recovery workflow never reruns `run_fundamental_quarter_update.py` and does not call SEC, Yahoo, yfinance, Finnhub, paid-provider, scheduler, refresh, or network paths.

The sequence is:

```text
preflight_quarter_update_vintage_readiness
dry_run_sec_vintage_for_missing_latest
apply_sec_vintage_for_missing_latest
preflight_quarter_update_vintage_readiness
```

The dry-run/apply commands use `candidate-mode latest_writer`.

## SEC Latest-Writer Recovery Path

If readiness preflight returns `READY_NOOP`, recovery stops successfully with `RECOVERY_NOOP`.

If readiness preflight detects latest rows missing vintage rows and no duplicate/vintage-without-latest blockers, the UI runs the SEC latest-writer dry-run.

Apply is allowed only when dry-run proves:

- `overall_status=DRY_RUN_READY`
- known `source_run_id`
- `blocked_rows=0`
- `unknown_provenance_rows=0`
- planned vintage rows > 0
- planned provenance rows > 0
- planned vintage rows match the readiness missing count
- candidates checked match planned vintage rows

If the operator did not provide `source_run_id`, the dry-run CLI may infer it only when every missing latest row has the same non-empty latest row `run_id`. If missing rows span multiple run ids, `source_run_id` remains unset and UI recovery stops with `SOURCE_RUN_ID_REQUIRED`.

The apply command includes:

```text
--approval-token USER_APPROVES_SEC_LATEST_WRITER_VINTAGE_APPLY
```

only after those gates pass.

## Yahoo/Final Mixed Handling Policy

Phase 4L7 does not blindly apply Yahoo/final mixed recovery. If readiness or planning cannot prove SEC latest-writer recovery is safe, the UI stops with a manual-review reason.

The explicit policy is:

```text
Manual review required: Yahoo/final mixed recovery cannot be proven safe
```

Future work can add a dedicated Yahoo/final mixed recovery planner if the missing rows can be classified safely.

## Statuses And Severity

Recovery status fields:

- `RECOVERY_NOOP` -> success
- `RECOVERY_APPLIED` -> success
- `RECOVERY_READY` -> review
- `RECOVERY_BLOCKED` -> stop
- `RECOVERY_UNKNOWN` -> stop/unknown

The UI summary also surfaces counts and reasons from readiness, dry-run, apply, and post-apply readiness where available.

## Safety Gates

Recovery is blocked by:

- duplicate statement vintage ids
- vintage rows without latest rows
- dry-run blocked rows
- unknown provenance
- missing or ambiguous source run id
- planned row count mismatch
- no planned vintage/provenance rows
- post-apply readiness not returning `READY_NOOP`

## Real DB And Provider Status

Phase 4L7 did not run a real quarter update, provider call, scheduler, refresh job, or real recovery apply.

Phase 4L7 did not write:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- `/home/kalle/projects/rawcandle/data/osakedata.db`

Tests use fake executors, mocked subprocess protection, and no real DB writes.

## Next Phase

Before real recovery, confirm an operator-managed backup exists and review readiness/dry-run output. A later phase may add a dedicated Yahoo/final mixed recovery planner if SEC latest-writer recovery is insufficient.
