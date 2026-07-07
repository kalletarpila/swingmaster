# SwingMaster Quarter Update UI Yahoo-Aware Vintage Recovery Phase 4L8

Date: 2026-07-07

## Purpose

Phase 4L8 extends the USA `Repair missing PIT/vintage rows` UI workflow so it can recover Yahoo/final mixed missing vintage rows when existing DB and audit evidence is sufficient.

This is for the case where USA quarter update was accidentally run without the PIT/vintage checkbox and the missing vintage representation cannot be repaired by SEC latest-writer recovery alone.

## No-Provider Recovery Design

The recovery workflow still does not rerun:

- `run_fundamental_quarter_update.py`
- SEC refresh
- Yahoo fallback
- yfinance, Finnhub, paid-provider, scheduler, refresh, or network paths

Yahoo-aware recovery reuses the provider-free CLI:

```text
python -m swingmaster.cli.apply_quarter_update_yahoo_aware_vintage
```

The UI first runs it in dry-run JSON mode, then runs apply only if all gates pass.

## Recovery Order

The UI action follows this order:

```text
readiness preflight
SEC latest-writer dry-run
SEC latest-writer apply if safe
Yahoo-aware dry-run if SEC is not applicable
Yahoo-aware apply if safe
post-apply readiness preflight
```

`READY_NOOP` still stops successfully without apply.

## SEC Recovery Path

The Phase 4L7 SEC latest-writer recovery path remains the first repair path. A successful SEC recovery now reports:

```text
SEC_RECOVERY_APPLIED
```

The previous `RECOVERY_APPLIED` severity behavior remains compatible, but the UI now identifies the recovery mode more explicitly.

## Yahoo-Aware Recovery Path

Yahoo-aware dry-run uses:

```text
--dry-run
--format json
```

Apply uses expected row gates:

```text
--expected-final-mixed-count <N>
--expected-yahoo-vintage-count <M>
--approval-token USER_APPROVES_YAHOO_AWARE_VINTAGE_APPLY
```

The command does not include provider flags and does not call `run_fundamental_quarter_update.py`.

## Safety Gates

Yahoo-aware apply is allowed only when:

- source run id is known
- planning status is `FINAL_MIXED_PLAN_READY` or `YAHOO_VINTAGE_PLAN_READY`
- planned final mixed rows or planned Yahoo vintage rows are positive
- planned provenance rows are positive
- blocked rows are zero
- unknown provenance fields are empty
- duplicate statement vintage id count is zero
- vintage-without-latest count is zero
- planned row count matches the readiness missing count
- apply expected-count arguments match the dry-run plan

After apply, readiness preflight must return:

- `READY_NOOP`
- `latest_without_vintage_count=0`
- `duplicate_statement_vintage_id_count=0`

## Statuses And Severity

Recovery statuses:

- `RECOVERY_NOOP` -> success
- `SEC_RECOVERY_APPLIED` -> success
- `YAHOO_AWARE_RECOVERY_APPLIED` -> success
- `RECOVERY_BLOCKED` -> stop
- `RECOVERY_UNKNOWN` -> stop/unknown

The UI summary records recovery mode:

- `sec_latest_writer`
- `yahoo_aware_final_mixed`
- `none`

## Manual Review Fallback

The UI still stops without apply when Yahoo-aware recovery cannot be proven safe:

```text
Manual review required: Yahoo/final mixed recovery cannot be proven safe
```

Blocked dry-run rows, unknown provenance, source-run ambiguity, duplicate vintage ids, planned-row mismatches, and failed post-checks all keep recovery in manual-review territory.

## Real DB And Provider Status

Phase 4L8 did not run a real quarter update, provider call, scheduler, refresh job, or real recovery apply.

Phase 4L8 did not write:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- `/home/kalle/projects/rawcandle/data/osakedata.db`

Tests use fake executors, mocked subprocess protection, monkeypatched CLI planning, and temp DBs only.

## Next Phase

Before real recovery, create and verify an operator-managed DB backup, then inspect readiness, dry-run, apply, and post-check summaries from the UI output.
