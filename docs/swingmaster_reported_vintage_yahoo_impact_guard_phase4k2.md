# Reported Vintage Yahoo Impact Guard Phase 4K2

Phase 4K2 adds temp-tested, read-only guardrails for detecting post-SEC latest/vintage drift risk in `run_fundamental_quarter_update.py`.

## Purpose

Phase 4K1 added the default-off `sec_latest_writer` vintage side-write. That side-write runs after the SEC latest writer has produced `rc_fundamental_quarterly` rows. The open risk is that quarter_update then runs Yahoo fallback enrichment, which can modify or insert latest rows after the SEC vintage row was already written.

This phase does not implement full Yahoo vintage production wiring. It adds guard helpers and summary plumbing so the drift can be detected in mocked/temp-DB tests and surfaced by explicit `sec_latest_writer` quarter_update runs.

## Actual Quarter Update Ordering

For USA tickers, repo evidence shows this order in `run_fundamental_quarter_update.py`:

1. Read quarter-state `detected_source_period_end_date`.
2. If the current quarterly table does not satisfy the detected USA quarter, run SEC raw bootstrap.
3. Optionally run SEC reconstruct vintage path if `sec_reconstruct_only` mode is enabled.
4. Run the latest-writer SEC quarterly build.
5. If `sec_latest_writer` mode is enabled, write SEC latest-writer-aligned vintage/provenance rows.
6. Run `run_yahoo_fallback_enrich(...)`.
7. Fail if the detected quarter is still not satisfied.
8. Run TTM, lifecycle, score, ack, and final USA valuation.
9. Emit summary fields.

The key point is that Yahoo fallback enrichment runs after the 4K1 SEC latest-writer vintage side-write.

## Yahoo Risk Analysis

Repo evidence from `run_fundamental_yahoo_fallback_enrich.py` indicates:

- Yahoo fallback can insert a missing quarterly row when the detected quarter is still absent and a matching Yahoo quarterly row exists.
- Yahoo fallback can fill NULL fields in an existing quarterly row and writes `rc_fundamental_quarterly_enrichment_audit` rows with `FILLED_FROM_YAHOO`.
- Yahoo fallback does not appear to overwrite non-null quarterly values, because `build_field_updates(...)` skips fields where the existing quarterly value is non-null.
- Yahoo fallback has run-id linkage through `run_id` in the enrichment audit table and through inserted missing-quarter latest rows.

Therefore a SEC-only vintage written before Yahoo fallback can drift from final latest rows if Yahoo inserts a missing row or fills missing fields afterward.

## Added Guard

The new read-only helpers are in `run_fundamental_quarter_update.py`:

- `check_quarter_update_vintage_parity_for_run(...)`
- `detect_yahoo_quarter_update_impact_for_run(...)`
- `build_quarter_update_vintage_post_run_guard_summary(...)`

The guard checks local SQLite tables only. It does not call providers, jobs, schedulers, or network APIs.

It detects:

- latest rows for the current latest-writer run without matching vintage rows
- vintage rows for a linked vintage run without matching latest rows for the latest-writer run
- value mismatches between latest rows and matching vintage rows
- duplicate `statement_vintage_id` values when feasible
- Yahoo fallback audit rows for the enrichment run
- Yahoo-filled field audit rows
- Yahoo inserted missing-quarter latest rows, using summary rows first and local run-id linkage as fallback

If required run-id linkage is missing, helpers return `UNKNOWN_RUN_LINKAGE` instead of guessing.

## Summary Fields

When `--write-vintage --vintage-mode sec_latest_writer` is explicitly enabled, quarter_update can now surface:

- `vintage_post_run_parity_status`
- `vintage_post_run_latest_without_vintage_count`
- `vintage_post_run_vintage_without_latest_count`
- `vintage_post_run_value_mismatch_count`
- `vintage_post_run_duplicate_statement_vintage_id_count`
- `vintage_yahoo_impact_status`
- `vintage_yahoo_fallback_rows_detected`
- `vintage_yahoo_inserted_missing_quarter_rows_detected`
- `vintage_yahoo_filled_field_rows_detected`
- `vintage_yahoo_audit_rows_detected`
- `vintage_yahoo_can_create_post_sec_vintage_drift`
- `vintage_recommendation`

Without `--write-vintage`, default summary behavior remains unchanged and these fields are omitted.

## What This Does Not Detect

The guard is intentionally conservative:

- it does not prove real production provider behavior
- it does not run against `fundamentals_usa.db`
- it does not infer missing run linkage
- it does not implement Yahoo-only vintage writes
- it does not implement final mixed vintage writes
- it does not change TTM, scoring, valuation, UI, ESS, scheduler, or provider paths

## Verification

Temp-DB tests cover the guard in `swingmaster/tests/test_quarter_update_vintage_post_run_guard.py`:

- parity OK
- latest without vintage
- vintage without latest when vintage run linkage exists
- value mismatch
- duplicate statement vintage id detection
- Yahoo enrichment audit detection
- missing run linkage returns `UNKNOWN_RUN_LINKAGE`
- quarter_update summary can surface guard fields in a mocked `sec_latest_writer` flow
- default no-vintage summary omits guard fields

Targeted verification command:

```bash
PYTHONPATH=. pytest -q swingmaster/tests/test_quarter_update_vintage_post_run_guard.py
PYTHONPATH=. pytest -q swingmaster/tests/test_quarter_update_sec_latest_writer_vintage.py
PYTHONPATH=. pytest -q swingmaster/tests/test_quarter_update_yahoo_fallback_vintage_forwarding.py
PYTHONPATH=. pytest -q swingmaster/tests/test_quarter_update_vintage_flags.py
PYTHONPATH=. pytest -q swingmaster/tests/test_fundamental_quarter_update.py
python3 -m py_compile swingmaster/cli/run_fundamental_quarter_update.py
git diff --check
```

Real DB/provider status: not run.

## Phase 4K3 Recommendation

Phase 4K3 should not silently rely on SEC-only vintage when Yahoo fallback can change final latest rows. The safest next implementation is one of:

- final mixed SEC + Yahoo fallback vintage after Yahoo enrichment completes
- post-run parity apply that writes a final vintage for the final latest row after all latest mutations
- Yahoo-only vintage only for cases where the row is entirely Yahoo-inserted and no SEC latest row exists

For final production policy, prefer a final mixed vintage after the full quarter_update latest row is stable.

## Phase 4K3 Follow-Up

Phase 4K3 adds the decision gate recommended here in [Reported Vintage Completion Gate Phase 4K3](swingmaster_reported_vintage_completion_gate_phase4k3.md).

The gate classifies explicit `sec_latest_writer` quarter_update runs as `SEC_VINTAGE_SUFFICIENT`, `FINAL_MIXED_REQUIRED`, `YAHOO_VINTAGE_REQUIRED`, `BLOCKED_POST_RUN_DRIFT`, or `UNKNOWN` using the Phase 4K2 parity/Yahoo-impact summaries. It still does not write final mixed or Yahoo-aware vintages.

Phase 4K4 adds temp-tested planning for the final mixed or Yahoo-aware candidate when this gate says SEC-only vintage is not sufficient. It remains planning-only and does not write vintage rows.
