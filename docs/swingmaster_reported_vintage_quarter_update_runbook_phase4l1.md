# Reported Vintage Quarter Update Runbook Phase 4L1

Date: 2026-07-07

Scope: production runbook for the next real USA `quarter_update` with explicit reported-vintage opt-in.

This document is operational guidance only. Phase 4L1 did not run the real `quarter_update`, did not call providers, did not run schedulers or refresh jobs, and did not write `/home/kalle/projects/swingmaster/fundamentals_usa.db`.

## 1. Pre-Run Readiness

For UI-triggered runs, Phase 4L3 adds a no-provider smoke test that verifies the UI command chain, vintage flags, preflight failure stop behavior, `SUMMARY key=value` parsing, and severity mapping without executing provider commands. See [SwingMaster Quarter Update UI Vintage Smoke Phase 4L3](swingmaster_quarter_update_ui_vintage_smoke_phase4l3.md).

If the UI-triggered run returns `FINAL_MIXED_REQUIRED` or `YAHOO_VINTAGE_REQUIRED`, the Phase 4L5 UI can run a gated automatic provider-free apply follow-up when the PIT/vintage checkbox was enabled and the Phase 4L4 apply gate passes. If auto apply is not attempted but the gate is safe, use the separate explicit Phase 4L4 apply action after reviewing planned counts and blockers. Do not rerun the provider update to apply Yahoo-aware/final mixed corrections. See [SwingMaster Quarter Update UI Yahoo-Aware Apply Phase 4L4](swingmaster_quarter_update_ui_yahoo_aware_apply_phase4l4.md), [SwingMaster Quarter Update UI Yahoo-Aware Auto Apply Phase 4L5](swingmaster_quarter_update_ui_yahoo_aware_auto_apply_phase4l5.md), and [SwingMaster Quarter Update UI Vintage Full Workflow Smoke Phase 4L6](swingmaster_quarter_update_ui_vintage_full_workflow_smoke_phase4l6.md).

If USA quarter update was already run without the PIT/vintage checkbox and latest rows are missing vintage rows, use the Phase 4L7 UI recovery action instead of rerunning quarter_update. See [SwingMaster Quarter Update UI Vintage Recovery Phase 4L7](swingmaster_quarter_update_ui_vintage_recovery_phase4l7.md).

Phase 4L8 extends that recovery action to Yahoo/final mixed recovery when the provider-free Yahoo-aware dry-run proves the plan safe. It still must not rerun quarter_update, Yahoo fallback, SEC refresh, or providers. See [SwingMaster Quarter Update UI Yahoo-Aware Vintage Recovery Phase 4L8](swingmaster_quarter_update_ui_vintage_recovery_phase4l8.md).

Run the read-only readiness smoke before any real Q update:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.preflight_quarter_update_vintage_readiness \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --format json
```

Required result:

| Field | Required value |
|---|---:|
| `overall_status` | `READY_NOOP` |
| `quick_check` | `ok` |
| `latest_without_vintage_count` | 0 |
| `vintage_without_latest_count` | 0 |
| `duplicate_statement_vintage_id_count` | 0 |
| `sec_missing_latest_candidates` | 0 |

Phase 4L1 read-only result:

| Field | Value |
|---|---:|
| `quick_check` | `ok` |
| `query_only` | 1 |
| `latest_row_count` | 155373 |
| `vintage_row_count` | 155373 |
| `provenance_row_count` | 1306683 |
| `latest_without_vintage_count` | 0 |
| `vintage_without_latest_count` | 0 |
| `duplicate_statement_vintage_id_count` | 0 |
| `sec_missing_latest_candidates` | 0 |
| `yahoo_aware_pending_action_count` | 0 |
| `overall_status` | `READY_NOOP` |

Stop before the real Q update if any required readiness value differs.

## 2. Main Quarter Update Command Template

Current CLI help shows `--db` and `--run-id` are required. For USA valuation, `--osakedata-db` is also required when the run reaches the final valuation step.

Use this template for the first controlled real run:

```bash
RUN_DATE=YYYY-MM-DD
AVAILABLE_AT_UTC=YYYY-MM-DDTHH:MM:SSZ
INGESTED_AT_UTC=YYYY-MM-DDTHH:MM:SSZ
RUN_ID=USA_QUARTER_UPDATE_${RUN_DATE}
VINTAGE_RUN_ID=USA_QUARTER_UPDATE_${RUN_DATE}__SEC_LATEST_WRITER_VINTAGE

PYTHONPATH=. python3 -m swingmaster.cli.run_fundamental_quarter_update \
  --db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --run-id "${RUN_ID}" \
  --market usa \
  --osakedata-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --write-vintage \
  --vintage-mode sec_latest_writer \
  --vintage-market usa \
  --vintage-available-at-utc "${AVAILABLE_AT_UTC}" \
  --vintage-ingested-at-utc "${INGESTED_AT_UTC}" \
  --vintage-run-id "${VINTAGE_RUN_ID}" \
  --vintage-yahoo-aware-action plan_only
```

The operator-confirmed OHLCV SQLite DB for USA valuation is `/home/kalle/projects/rawcandle/data/osakedata.db`. Phase 4L1 validated that the path exists, but did not open it for data inspection and did not write to it.

Do not add `--dry-run` to the real command. `--dry-run` is useful for command shape testing, but it is not the real update.

Do not use `--skip-ack` for the normal production run unless intentionally reprocessing without acknowledging quarter-state rows.

In the UI, the Phase 4L5 auto-apply follow-up must not change this primary command. The primary command remains `plan_only`; any apply is a separate no-provider command after summary parsing and gating.

## 3. Timestamp And Run-ID Convention

Use explicit UTC timestamps. Do not infer them from the quarter period end date and do not rely on an implicit current clock.

Recommended names:

| Concept | Convention |
|---|---|
| Base run id | `USA_QUARTER_UPDATE_YYYY-MM-DD` |
| Quarterly child run id emitted by code | `USA_QUARTER_UPDATE_YYYY-MM-DD__QUARTERLY` |
| SEC latest-writer vintage run id | `USA_QUARTER_UPDATE_YYYY-MM-DD__SEC_LATEST_WRITER_VINTAGE` |
| `available_at_utc` | explicit operational decision timestamp |
| `ingested_at_utc` | explicit run ingestion timestamp |

`available_at_utc` should represent when this reported vintage is considered point-in-time available to SwingMaster. `ingested_at_utc` should represent when the run ingested/wrote it. They may be equal only if that is the intended operational fact.

## 4. Yahoo-Aware Action Policy

The first real run should use:

```text
--vintage-yahoo-aware-action plan_only
```

This keeps the post-Yahoo final mixed/Yahoo-aware execution default-off. It still allows the completion gate and planner summaries to report whether a second controlled action is required.

Use `--vintage-yahoo-aware-action write` only after reviewing the first run summary and planned counts. It is valid only with:

- `--write-vintage`
- `--vintage-mode sec_latest_writer`
- valid explicit PIT metadata

Do not rerun blindly after `FINAL_MIXED_REQUIRED` or `YAHOO_VINTAGE_REQUIRED`. First inspect planned rows, provenance counts, blocked rows, unknown provenance fields, and duplicate risks.

## 5. Success, Warning, And Stop Conditions

Success for the first real run:

- `vintage_completion_status=SEC_VINTAGE_SUFFICIENT`
- `vintage_post_run_parity_status=OK`
- `vintage_post_run_latest_without_vintage_count=0`
- `vintage_post_run_vintage_without_latest_count=0`
- `vintage_post_run_duplicate_statement_vintage_id_count=0`
- post-run readiness preflight returns `READY_NOOP`
- post-run SEC latest-writer dry-run returns `NO_CANDIDATES`

Review, do not blindly rerun:

- `vintage_completion_status=FINAL_MIXED_REQUIRED`
- `vintage_completion_status=YAHOO_VINTAGE_REQUIRED`
- `vintage_yahoo_aware_planning_status=FINAL_MIXED_PLAN_READY`
- `vintage_yahoo_aware_planning_status=YAHOO_VINTAGE_PLAN_READY`
- `vintage_planned_final_mixed_rows > 0`
- `vintage_planned_yahoo_vintage_rows > 0`

Stop and investigate:

- `vintage_completion_status=BLOCKED_POST_RUN_DRIFT`
- `vintage_completion_status=UNKNOWN`
- `vintage_post_run_parity_status=DRIFT`
- `vintage_post_run_parity_status=UNKNOWN_RUN_LINKAGE`
- `vintage_post_run_latest_without_vintage_count > 0`
- `vintage_post_run_vintage_without_latest_count > 0`
- `vintage_post_run_duplicate_statement_vintage_id_count > 0`
- `vintage_yahoo_aware_planning_status=PLAN_BLOCKED`
- `vintage_yahoo_aware_blocked_rows > 0`
- `vintage_yahoo_aware_unknown_provenance_fields` is non-empty unless explicitly accepted after review
- `vintage_yahoo_aware_execution_status=EXECUTION_BLOCKED`
- any provider, DB, valuation, scoring, or ack error in the main command

## 6. Completion Status Playbook

`SEC_VINTAGE_SUFFICIENT`:

Accept the run if post-run readiness and SEC dry-run no-op checks also pass. No Yahoo-aware write action is needed.

`FINAL_MIXED_REQUIRED`:

Do not rerun the whole provider update blindly. Review `vintage_planned_final_mixed_rows`, `vintage_planned_yahoo_aware_provenance_rows`, unknown provenance fields, and blocked counts. If the plan is understood and accepted, prepare a second controlled command with the same explicit PIT metadata policy and `--vintage-yahoo-aware-action write`.

`YAHOO_VINTAGE_REQUIRED`:

Review Yahoo staging/source linkage and `vintage_planned_yahoo_vintage_rows`. Do not write until the Yahoo candidate source evidence is understood. If accepted, use the second controlled write action, not an unreviewed provider rerun.

`BLOCKED_POST_RUN_DRIFT`:

Stop. Investigate latest/vintage parity, duplicate statement vintage ids, and value mismatches. Do not run Yahoo-aware write to paper over drift.

`UNKNOWN`:

Stop. Improve run linkage or summary observability before taking write action. Unknown means the gate cannot prove the state safely.

## 7. Post-Run Verification

Run readiness preflight again:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.preflight_quarter_update_vintage_readiness \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --format json
```

Run the aligned SEC latest-writer dry-run no-op check. Replace `YYYY-MM-DD` and timestamps with the same operational values used in the real run:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_sec_vintage_for_missing_latest \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --candidate-mode latest_writer \
  --available-at-utc "${AVAILABLE_AT_UTC}" \
  --ingested-at-utc "${INGESTED_AT_UTC}" \
  --vintage-run-id "USA_QUARTER_UPDATE_${RUN_DATE}__SEC_LATEST_WRITER_VINTAGE_NOOP_SMOKE" \
  --format json \
  --sample-limit 20
```

Required dry-run result:

- `overall_status=NO_CANDIDATES`
- `latest_missing_vintage_rows=0`
- `candidates_checked=0`
- `planned_vintage_rows=0`
- `planned_provenance_rows=0`
- `blocked_rows=0`

Also preserve the quarter_update terminal summary output. The current CLI emits `SUMMARY key=value` lines; there is no documented built-in summary log file in this path.

## 8. Rollback Note

This runbook does not identify automatic backup/restore behavior in `run_fundamental_quarter_update.py`. Before a real production run, create and verify an operator-managed backup of `/home/kalle/projects/swingmaster/fundamentals_usa.db`.

If rollback is required, restore from that verified backup rather than attempting ad hoc deletes or replacements. Do not use destructive Git commands for DB rollback; the SQLite DB is operational data, not a Git-tracked source artifact.

## 9. What Phase 4L1 Did Not Do

Phase 4L1 did not:

- run the actual USA quarter_update
- call SEC, Yahoo, yfinance, Finnhub, or other providers
- run the scheduler
- run broad refresh jobs
- write the real fundamentals DB
- change CLI defaults
- add runtime code
