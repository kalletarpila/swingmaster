# SwingMaster First UI USA PIT/Vintage Run Diagnostics

Date: 2026-07-07

Scope: investigation only. No provider calls, scheduler runs, refresh jobs, apply/recovery writes, or real DB writes were run during this diagnostic pass. The real USA fundamentals DB was inspected read-only with SQLite URI `mode=ro` and `PRAGMA query_only=ON`.

## Run Summary

The first UI-triggered USA `quarter_update` with PIT/vintage enabled did not complete cleanly.

Observed summary:

- `vintage_rows_inserted=4`
- `vintage_provenance_rows_inserted=30`
- `vintage_rows_skipped_noop=278`
- `vintage_rows_failed=93`
- `vintage_post_run_parity_status=DRIFT`
- `vintage_post_run_latest_without_vintage_count=0`
- `vintage_post_run_vintage_without_latest_count=0`
- `vintage_post_run_value_mismatch_count=1`
- `vintage_post_run_duplicate_statement_vintage_id_count=0`
- `vintage_completion_status=FINAL_MIXED_REQUIRED`
- `vintage_completion_reason=value_mismatch_explained_by_yahoo_audit`
- `vintage_yahoo_aware_planning_status=PLAN_BLOCKED`
- `vintage_yahoo_aware_blocked_rows=184`

The important distinction is that the DB is not missing PIT/vintage rows. The drift is a value mismatch, and the Yahoo-aware planner then expands into a much wider Yahoo-audit scope.

## Current DB Parity

Read-only DB inspection showed:

- `rc_fundamental_quarterly` latest rows: `155377`
- USA `rc_fundamental_quarterly_vintage` rows: `155377`
- USA `rc_fundamental_quarterly_field_provenance` rows: `1306713`
- latest rows without any USA vintage row: `0`
- USA vintage rows without latest row: `0`
- duplicate USA `statement_vintage_id`: `0`

This is structurally acceptable parity. It does not justify running missing PIT/vintage recovery.

## Exact Value Mismatch

The single post-run value mismatch is:

- ticker: `GIS`
- period_end_date: `2025-05-25`
- mismatched field: `total_debt`
- latest value: `14878600000.0`
- visible/current vintage value: `677000000.0`
- visible/current vintage id: `legacy:usa:GIS:2025-05-25:9441b8313c7894e3`
- visible/current vintage run id: `reported-vintage-legacy-backfill:usa:2026-06-19:2026-06-19T14:28:43Z`
- visible/current vintage available_at_utc: `2026-06-19T00:00:00Z`
- visible/current vintage ingested_at_utc: `2026-06-19T14:28:43Z`

No current-run Yahoo enrichment audit row was found for `GIS:2025-05-25`. Therefore the specific mismatch is not directly explained by a current-run `FILLED_FROM_YAHOO` audit row for the same ticker/period/field.

## Yahoo Audit Scope

The pasted UI summary reported `vintage_yahoo_filled_field_rows_detected=302`. Current read-only DB state has `306` current-run `FILLED_FROM_YAHOO` audit rows for run id:

`USA_QUARTER_UPDATE_2026-07-07__QUARTERLY__ENRICH`

Those rows cover:

- `306` filled fields
- `189` ticker/period keys
- `54` tickers

The small 302 vs 306 discrepancy is not material to the root cause. The planner behavior is the same: it uses the whole current enrichment audit key set rather than the single value-mismatch key.

## Why Planner Blocked Many Rows

The relevant code path is in `swingmaster/cli/run_fundamental_quarter_update.py`:

- `build_quarter_update_vintage_post_run_guard_summary(...)`
- `plan_quarter_update_yahoo_aware_vintage(...)`
- `_plan_final_mixed_vintage(...)`

For `FINAL_MIXED_REQUIRED`, the planner receives:

- all latest rows from the source run id
- SEC provenance only from the `sec_latest_writer` vintage run
- Yahoo audit rows for the enrich run id

`_plan_final_mixed_vintage(...)` iterates all latest rows and, for every ticker/period that has a Yahoo audit row, tries to build a final-mixed row. For each non-null reported field it requires either SEC field provenance or Yahoo audit provenance. Most historical Yahoo-filled rows do not have SEC provenance in the current `sec_latest_writer` vintage run, so they become unknown-provenance blockers.

The diagnostic reproduced the same mechanism on current DB state:

- current-run Yahoo audit keys matched by latest rows: `189`
- blocked rows using the same provenance rule: `188`
- rows with enough provenance before global block: `1` (`HNI:2024-12-31`)
- reproduced unknown field count: `1752`

The pasted UI log showed `184` blocked rows; current DB state reproduces the same failure mode with `188`.

Conclusion: the planner scope is too broad for this run. It is effectively planning final-mixed correction for the current enrich-run Yahoo audit population, not just for the post-run drift row(s).

## 93 Failure Summary

The log contains exactly `93` ticker errors:

- `86` x `FUNDAMENTAL_QUARTER_UPDATE_ENRICH_MISSING_DETECTED`
- `7` x `SEC_TICKER_NOT_FOUND`

The most common enrich-missing patterns were:

- `35` tickers: expected `2026-03-31`, latest after enrich `2026-04-04`
- `23` tickers: expected `2026-03-31`, latest after enrich `2026-04-03`
- `9` tickers: expected `2026-05-31`, latest after enrich `2026-02-28`
- `4` tickers: expected `2026-03-31`, latest after enrich `2026-04-05`

Sample enrich-missing tickers include `ARMK`, `ARW`, `ATRO`, `AVAH`, `AZO`, `BC`, `BGS`, `C`, `CAVA`, `CGNX`, `COKE`, `COST`, `DAR`, `FC`, `JEF`, `KBH`, `NKE`, `PAYX`, `WAT`, and `ZBRA`.

The `SEC_TICKER_NOT_FOUND` tickers were:

- `ALTS`
- `MASI`
- `MEG`
- `PSTG`
- `QVCGA`
- `SNBR`
- `TIVC`

The 93 failures are mostly the same class as the ZBRA summary error: expected detected fiscal/calendar period differs from the latest period after SEC refresh and Yahoo enrich. They are not the same issue as the single `GIS` value mismatch, and they are not missing-vintage recovery candidates.

## Recommendation

Recommendation: `FIX_YAHOO_AWARE_PLANNER_SCOPE`.

The planner should not use the whole current Yahoo audit population when the completion reason is `value_mismatch_explained_by_yahoo_audit`. It should first derive explicit target keys from the post-run guard:

- current source run id
- current post-run value-mismatch ticker/period keys
- exact mismatched field names
- current-run Yahoo audit rows only if they match those keys/fields, or a clearly declared touched-row set

If the mismatch key has no matching current-run Yahoo audit, the correct result should be blocked with a narrow reason such as:

`VALUE_MISMATCH_NOT_EXPLAINED_BY_CURRENT_RUN_YAHOO_AUDIT`

not a broad final-mixed plan over 184+ unrelated Yahoo-audited historical rows.

Secondary fix: the completion gate should not classify `value_mismatch_explained_by_yahoo_audit` using only aggregate `yahoo_audit_rows > 0`. It should require key-level evidence that the mismatched ticker/period/field is explained by current-run Yahoo audit. In this run, `GIS:2025-05-25:total_debt` had no such current-run audit row.

The failure-handling question is separate. The 86 enrich-missing errors are probably expected-period detection or fiscal-period tolerance issues, not a PIT/vintage apply issue. They should be investigated under SEC/Yahoo quarter detection and should not be fixed by broadening the Yahoo-aware vintage apply.
