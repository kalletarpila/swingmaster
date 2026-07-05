# SwingMaster Reported Vintage Quarter Update Final Mixed Execution Design Phase 4I13

Phase 4I13 designs a future quarter_update execution phase for final mixed SEC + Yahoo fallback reported vintages.

## Scope

This is a design-only phase.

No runtime code changed. No tests changed. No DB writes, provider calls, network calls, scheduler runs, UI changes, ESS integration, migrations, or final mixed execution mode are implemented here.

## Current Implemented Pieces

The repo now has the following reported-vintage building blocks:

- SEC metadata and field provenance contract for reconstructed quarterly rows.
- Yahoo bridge and Yahoo fallback metadata/provenance contracts.
- SEC and Yahoo dual-write scaffolds that are opt-in and require explicit PIT metadata.
- SEC and Yahoo CLI vintage opt-ins for narrow paths.
- quarter_update vintage validation and isolated forwarding modes: `validation_only`, `sec_reconstruct_only`, and `yahoo_fallback_only`.
- `sec_plus_yahoo_fallback_planning`, a combined planning-only quarter_update mode with execution disabled.
- `reported_final_mixed_vintage.py`, a pure final mixed builder for source hash, statement id, metadata, and field source map merging.
- `build_final_mixed_vintage_plan_summary(...)`, a mocked planning helper that computes planned final mixed details without DB writes.

No production quarter_update path currently writes final mixed vintages.

## Recommended Execution Policy

Future combined quarter_update execution should write one final mixed vintage per affected ticker/period after SEC reconstruction and Yahoo fallback have completed.

Recommended policy:

- SEC-retained fields keep SEC provenance when SEC provenance is available.
- Yahoo-filled fields use Yahoo fallback provenance.
- Retained fields without explicit provenance remain `unknown`; never relabel them as Yahoo.
- No-op fallback should not create a new final mixed vintage unless SEC output itself changed and that output is intentionally accepted as final.
- Yahoo missing-quarter insert creates one Yahoo-source final vintage rather than a SEC-retained mixed vintage.
- Substep SEC-only vintages should not be written as final in combined mode unless schema or metadata later adds a clear intermediate-vintage role.

The core invariant is that consumers should see one accepted final context for a ticker/period from a combined quarter_update run.

## Required Data Inputs

From the SEC step, quarter_update must collect:

- normalized quarterly row or rows produced by SEC reconstruction
- SEC contributing facts by ticker/period/field
- SEC source hash or enough source references to reproduce it
- SEC field source map
- SEC filed date when available
- SEC child run id

From the Yahoo fallback step, quarter_update must collect:

- affected ticker/periods
- resulting normalized row after fallback enrichment
- enrichment audit rows
- fields filled from Yahoo
- missing-quarter insert indicator
- Yahoo source references and payload hash when available
- fallback child run id

From quarter_update itself, the final mixed write needs:

- parent run id
- explicit vintage run id
- explicit `available_at_utc`
- explicit `ingested_at_utc`
- market
- final mixed execution mode

Availability must not be inferred from period end date or current clock.

## Future CLI Mode

Recommended future mode name:

```text
sec_plus_yahoo_fallback_final_mixed
```

Rules:

- default-off
- requires `--write-vintage`
- requires market, availability timestamp, ingestion timestamp, and vintage run id
- must not run under `validation_only`
- must not infer availability
- must not use the current clock implicitly
- must fail before writes if SEC/Yahoo/final mixed inputs cannot be collected

## Execution Sequence

Recommended sequence:

1. Validate vintage mode and explicit PIT metadata.
2. Run the SEC path as currently required.
3. Run Yahoo fallback as currently required.
4. Collect affected ticker/period rows after fallback.
5. For each affected period, build final mixed row metadata and field provenance.
6. Insert final mixed vintage row through the reported vintage writer path.
7. Insert field provenance rows.
8. Do not modify latest rows beyond current quarter_update behavior.
9. Continue downstream TTM, scoring, valuation, and ack as current flow does.
10. Summarize final mixed vintage results.

Final mixed vintage writing should happen after fallback has completed. Prefer writing before downstream TTM/scoring if the final mixed row documents the latest-compatible row already accepted by quarter_update. Vintage writing itself must not change TTM/scoring behavior in the first execution phase.

No ESS behavior changes are included.

## Duplicate And No-Op Policy

Recommended early implementation policy:

- If the exact final mixed `statement_vintage_id` already exists, classify it as already-known/no-op or fail clearly; do not replace.
- Prefer explicit `already_known` counting over silent ignore.
- If normalized row and final source hash are unchanged and vintage exists, report no-op.
- If fallback is no-op and SEC output is unchanged, write no final mixed vintage.
- If fallback fills one or more fields, write one final mixed vintage.
- If Yahoo inserts a missing quarter, write one Yahoo-source final vintage.
- If a different final mixed hash exists for the same ticker/period, require an explicit revision/supersession policy before replacing or superseding anything.

Early Phase 4I14 can choose fail-fast for ambiguous duplicates because it is still temp-DB/mocked.

## Failure Policy

If SEC succeeds but Yahoo fallback fails, quarter_update should not write a final mixed vintage for that ticker/period in combined final mode. It should report a clear final mixed failure and preserve existing child-step failure behavior.

If Yahoo fallback cannot provide audit/provenance for changed fields, final mixed execution should fail before writes rather than write fields with misleading Yahoo attribution.

If only SEC provenance is available and fallback made no changes, the execution helper should classify the period as no-op unless final SEC acceptance is explicitly requested.

## Summary Fields

Future summary should include:

```text
vintage_requested
vintage_mode
vintage_execution_enabled
vintage_final_mixed_requested
vintage_final_mixed_written
vintage_final_mixed_rows_inserted
vintage_final_mixed_provenance_rows_inserted
vintage_final_mixed_rows_skipped_noop
vintage_final_mixed_rows_already_known
vintage_final_mixed_rows_failed
vintage_count_status
vintage_error_summary
```

Child-level status should also be visible:

- SEC status
- Yahoo fallback status
- final mixed status

For batch runs, counts should distinguish inserted, skipped no-op, already-known, and failed rows.

## Phase 4I14 Test Plan

Recommended next implementation phase:

```text
Phase 4I14: final mixed execution helper for quarter_update, temp-DB/mocked tests only
```

Initial tests should cover:

1. final mixed execution helper writes one final mixed vintage in temp DB
2. no-op fallback writes no final mixed vintage
3. Yahoo-filled field appears as Yahoo fallback
4. SEC-retained field remains SEC or unknown
5. duplicate final mixed vintage is handled deterministically
6. missing required provenance fails clearly
7. summary counts are correct
8. no real DB/provider calls
9. default quarter_update remains unchanged

Do not start with real provider execution.

## Risks And Open Questions

- Current schema may not explicitly mark final vs intermediate role.
- Substep vintages versus final-only policy needs a durable metadata decision.
- Existing legacy baseline visibility must stay clear when final mixed rows are introduced.
- Externally verified release dates may later refine availability semantics.
- TTM and valuation may eventually need PIT-vintage readers instead of latest tables.
- Restatements or provider corrections after a final mixed vintage exists require revision and supersession rules.
- Missing-quarter inserts may need a distinct source provider or final-vintage role to avoid implying SEC retention.

## Recommendation

Phase 4I14 should implement a pure/temp-DB final mixed execution helper and mocked quarter_update summary tests only.

Real DB and production quarter_update final mixed execution should remain later, after duplicate/no-op, supersession, and final-vs-intermediate semantics are tested.

## Phase 4I14 Helper Reference

Phase 4I14 adds the temp-DB final mixed execution helper in [Reported Vintage Final Mixed Execution Helper Phase 4I14](swingmaster_reported_vintage_final_mixed_execution_helper_phase4i14.md).

It proves the final mixed builder can write latest-compatible, vintage, and field provenance rows through the existing opt-in adapter, but it is still not wired into production quarter_update execution.
