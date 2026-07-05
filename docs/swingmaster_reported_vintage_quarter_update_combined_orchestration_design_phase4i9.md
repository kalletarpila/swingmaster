# SwingMaster Reported Vintage Quarter Update Combined Orchestration Design Phase 4I9

## Scope

Phase 4I9 is a documentation-only design for a future default-off combined SEC + Yahoo fallback vintage mode in `run_fundamental_quarter_update.py`.

This phase changes no runtime code, no tests, no migrations, and no DB data. It does not call providers, network APIs, refresh jobs, schedulers, or the real `fundamentals_usa.db`. It does not implement a new quarter_update mode, combined SEC + Yahoo execution, UI changes, ESS integration, TTM/scoring/valuation changes, or production wiring.

## Current State

Quarter update currently supports three vintage modes:

- `validation_only`: validates explicit PIT metadata and adds opt-in summary fields, but forwards no vintage metadata to child paths.
- `sec_reconstruct_only`: forwards validated metadata only to `run_sec_reconstruct_step(...)` when the USA SEC refresh path is required.
- `yahoo_fallback_only`: forwards validated metadata only to `run_yahoo_fallback_enrich(...)`.

Default behavior without `--write-vintage` remains unchanged and omits vintage summary fields.

Current opt-in summary behavior reports:

- `vintage_requested`
- `vintage_mode`
- `vintage_execution_enabled`
- `vintage_validation_status`
- per-subpath requested flags
- `vintage_rows_inserted` / `vintage_provenance_rows_inserted` as `None` for forwarding modes
- `vintage_count_status=not_reported_by_child` for forwarding modes

Production status remains default-off and narrow. SEC-only and Yahoo-fallback-only forwarding are covered by mocked tests, but no mode coordinates both in one vintage-aware quarter_update run.

## Combined Mode Problem

The combined SEC + Yahoo fallback case is materially different from the isolated forwarding modes.

SEC reconstruct can represent primary reported data for a quarter. Yahoo fallback can then fill missing fields in that same generic quarterly row or insert a missing quarter from Yahoo if no generic row satisfies the detected period.

If both substeps independently write vintages for the same ticker and period, the history can contain:

- a pre-fallback SEC-only vintage
- a later mixed SEC + Yahoo fallback vintage
- duplicate or confusing source hashes for the same period

ESS and backtests need an unambiguous row for a decision cutoff. A consumer should not accidentally treat a pre-fallback SEC-only row as the final accepted quarter_update context when the same run later filled fields from Yahoo.

## Policy Options

### Option 1: Substep Vintages Only

SEC reconstruct writes a SEC vintage. Yahoo fallback writes a separate fallback/mixed vintage if fields changed.

Pros:

- reuses existing SEC and Yahoo fallback opt-in paths
- preserves provider-stage history
- relatively small runtime change

Cons:

- consumers can accidentally read the SEC substep vintage as final
- no current schema-level role/status marker distinguishes intermediate from final
- duplicate-vintage and supersession semantics are unclear
- no-op fallback still leaves the meaning of the SEC-only row ambiguous

### Option 2: Final Mixed Vintage Only

Quarter_update waits until SEC and Yahoo fallback have completed, then writes one final accepted vintage per affected ticker/period.

Pros:

- easiest PIT contract for ESS/backtests
- avoids treating pre-fallback SEC output as final
- provenance can mark SEC-retained fields separately from Yahoo-filled fields
- no-op fallback can avoid an unnecessary second row

Cons:

- requires a final-mixed writer or orchestration layer that can combine SEC provenance and fallback audit provenance
- needs careful source hash and statement vintage id policy
- needs tests for missing-quarter insert, no-op fallback, duplicates, and retained-field provenance

### Option 3: Both Substep And Final Vintages With Explicit Role/Status

SEC writes an intermediate/provider-stage vintage, and quarter_update writes a final accepted mixed vintage after fallback completes.

Pros:

- preserves detailed provider-stage history and final accepted context
- can support richer audit trails later

Cons:

- likely requires schema or metadata support for `intermediate` vs `final`
- consumers must filter correctly
- broader than current phase unless existing schema can clearly encode role/status

### Option 4: Keep Individual Modes Only

Continue with `sec_reconstruct_only` and `yahoo_fallback_only`, without a combined quarter_update mode.

Pros:

- simplest and safest near-term behavior
- avoids creating ambiguous final history

Cons:

- incomplete for production quarter_update
- operators must choose isolated subpath behavior manually
- does not solve final accepted context for SEC + Yahoo fallback runs

## Recommended Policy

For eventual production, prefer one final mixed vintage per affected ticker/period after fallback completes.

Rules:

- Phase 4I10 should not write final mixed vintage rows yet.
- Phase 4I10 should add a planning mode only, such as `sec_plus_yahoo_fallback_planning`.
- The planning mode should validate explicit metadata, exercise mocked/request planning for both SEC and Yahoo fallback, and produce a summary plan without writing vintage rows.
- Phase 4I11 can later implement temp-DB final mixed vintage creation with mocks.
- Real DB or provider execution should come much later.

Eventual final-row policy:

- If fallback fills fields after SEC, write one final mixed vintage after fallback completes.
- SEC-retained fields remain SEC when provenance is available.
- SEC-retained fields with no provenance remain `unknown`; never relabel them as Yahoo.
- Yahoo-filled fields use `source_provider=yahoo`, `provenance_role=FALLBACK_REPORTED`, and `merge_action=YAHOO_FILLED_MISSING`.
- A Yahoo inserted missing quarter is Yahoo-sourced final vintage, not SEC-retained fallback.
- A no-op fallback should not create a new mixed vintage beyond the SEC result unless a later restatement/revision policy explicitly requires it.

## Proposed Future CLI Mode

Future mode:

```text
--vintage-mode sec_plus_yahoo_fallback_planning
```

Rules:

- default-off
- requires `--write-vintage`
- requires explicit `--vintage-market`
- requires explicit `--vintage-available-at-utc`
- requires explicit `--vintage-ingested-at-utc`
- requires explicit `--vintage-run-id`
- does not infer availability from `period_end_date`
- does not use the current clock implicitly
- first implementation writes no final mixed vintage rows
- first implementation only produces a validated orchestration plan and summary fields

## Required Metadata And Run IDs

Required identifiers:

- parent quarter_update run id: existing `--run-id`
- SEC raw child run id: existing `BASE__SEC_RAW`
- SEC reconstruct child run id: existing `BASE__SEC_QUARTERLY_RECON`
- generic quarterly build child run id: existing `BASE__QUARTERLY`
- Yahoo fallback child run id: existing `BASE__ENRICH`
- final mixed vintage run id: explicit `--vintage-run-id`, or a later tested deterministic child id such as `BASE__FINAL_MIXED_VINTAGE`
- normalization run id: explicit optional `--vintage-normalization-run-id`, otherwise a documented deterministic fallback in the future implementation

Future final mixed source hash should include:

- resulting normalized row after SEC and fallback
- SEC contributing facts or retained-field provenance where available
- Yahoo fallback audit rows for filled fields
- matched Yahoo quarterly source row metadata
- missing-quarter insert metadata when applicable
- explicit availability and ingestion metadata

Final `statement_vintage_id` should be deterministic from market, ticker, period, mode, final source hash, and final run metadata. It should not collide with SEC-only or Yahoo-only substep vintage ids.

## Summary Design

Future combined planning summary should include:

```text
vintage_requested
vintage_mode
vintage_execution_enabled
vintage_planning_only
vintage_sec_reconstruct_requested
vintage_yahoo_fallback_requested
vintage_final_mixed_planned
vintage_final_mixed_written
vintage_rows_inserted
vintage_provenance_rows_inserted
vintage_rows_skipped_noop
vintage_rows_failed
vintage_count_status
vintage_error_summary
```

For Phase 4I10 planning-only:

- `vintage_execution_enabled=false` or `vintage_planning_only=true`
- inserted counts are `0`
- planned counts may be `None` unless mocked child summaries expose them
- both SEC and Yahoo fallback planning/request flags should be visible
- unsupported paths such as Yahoo bridge should remain false

## Required Tests For Phase 4I10

Phase 4I10 should test:

1. default quarter_update unchanged
2. combined planning mode requires `--write-vintage`
3. combined planning mode requires PIT metadata
4. validation fails before child steps
5. planning mode calls or marks both SEC and Yahoo fallback planning/request flags in mocks
6. no vintage writes are executed
7. summary shows planning-only and both subpaths requested
8. no vintage flags are sent to unsupported paths
9. no-op fallback planning is represented
10. existing `sec_reconstruct_only` and `yahoo_fallback_only` tests still pass

## Risks And Open Questions

- Existing schema may not clearly mark intermediate vs final vintages.
- Duplicate final vintages need deterministic source hash and statement vintage id policy.
- If a SEC-only vintage exists, it is unclear whether a final mixed vintage should supersede it without explicit metadata.
- Legacy baseline vintages and new final mixed vintages need a consistent ordering and availability policy.
- Provider corrections or restatements after a mixed row exists need revision/supersession rules.
- TTM, valuation, and ESS should later define whether they read latest-compatible rows or PIT-vintage rows, but that is out of scope for this phase.
- Quarter-state ack should remain independent from combined vintage success until failure semantics are explicit.

## Recommended Next Phase

Recommended Phase 4I10: quarter_update combined SEC + Yahoo fallback planning mode, no vintage execution.

Phase 4I10 should use mocked tests only, avoid real DB/provider/scheduler runs, and preserve default quarter_update behavior.

## Phase 4I10 Implementation Reference

Phase 4I10 implements the recommended planning-only combined mode in [Reported Vintage Quarter Update Combined Planning Phase 4I10](swingmaster_reported_vintage_quarter_update_combined_planning_phase4i10.md).

The implementation adds `sec_plus_yahoo_fallback_planning`, validates explicit metadata, and reports combined planning summary fields without forwarding vintage metadata to SEC/Yahoo children or writing final mixed vintage rows.

## Phase 4I11 Builder Reference

Phase 4I11 adds the pure final mixed builder contract in [Reported Vintage Final Mixed Builder Phase 4I11](swingmaster_reported_vintage_final_mixed_builder_phase4i11.md).

The builder defines deterministic `mixed_sec_yahoo` source hash, statement vintage id, and SEC/Yahoo/unknown field provenance merge behavior in temp-DB tests only. It is not wired into quarter_update execution.
