# SwingMaster Reported Vintage Quarter Update Opt-In Design Phase 4I5

## Scope

Phase 4I5 is a design-only phase for a future default-off vintage opt-in in `run_fundamental_quarter_update.py`.

This phase changes no runtime code, no tests, no migrations, and no DB data. It does not call providers, network APIs, refresh jobs, schedulers, or the real `fundamentals_usa.db`. It does not wire quarter update, SEC, Yahoo, UI, scoring, valuation, TTM, or ESS readers to vintage writes.

## Current Quarter Update Behavior

The entry point is `run_fundamental_quarter_update.py`. It parses:

- `--db`
- `--run-id`
- optional `--market`
- optional `--ticker`
- optional `--limit`
- optional `--osakedata-db`
- `--dry-run`
- `--skip-ack`

Eligible rows are loaded from `rc_fundamental_quarter_state` where `new_quarter_available = 1`, optionally filtered by market, ticker, and limit. The run uses deterministic child run ids from the base run id:

- `__RAW`
- `__YQTR`
- `__QBRIDGE`
- `__SEC_RAW`
- `__QUARTERLY`
- `__TTM`
- `__LIFECYCLE`
- `__SCORE`
- `__VALUATION`
- `__ACK`
- `__ENRICH`

For each ticker, `process_ticker(...)` calls `run_quarterly_refresh(...)`, then TTM, lifecycle, score, and quarter-state acknowledgement unless `--skip-ack` is set.

For USA tickers, `run_quarterly_refresh(...)` checks `detected_source_period_end_date` from quarter state. If current generic quarterly rows do not satisfy the detected quarter, it calls:

- `run_sec_raw_bootstrap(...)`
- `run_sec_quarterly_build_step(...)`

After the SEC path, it always calls `run_yahoo_fallback_enrich(...)` with `detected_source_period_end_date`. If the detected quarter is still not satisfied after fallback enrich, quarter update fails for the ticker.

For non-USA tickers, `run_quarterly_refresh(...)` calls:

- `run_yahoo_audit(...)`
- `run_yahoo_quarterly_write(...)`
- `run_yahoo_to_quarterly(...)`

After ticker processing, `run_fundamental_quarter_update(...)` optionally runs USA valuation when the market is `usa` or unspecified. Valuation requires `--osakedata-db` and resolves the latest USA OHLCV close date.

Quarter-state acknowledgement checks that the detected period is represented before acknowledging:

- USA uses the same calendar-quarter/tolerance satisfaction rule.
- non-USA requires latest generic quarter period to be greater than or equal to the detected source period.

The current summary includes ticker counts, market, dry-run, skip-ack, valuation date/rows, and run id. Per-step progress is printed with `STEP ...=OK`, warnings, and error messages.

## Existing Vintage-Capable Subpaths

### SEC Reconstruct CLI

`run_fundamental_sec_reconstruct_quarterly.py` has default-off vintage opt-in.

Required flags:

- `--write-vintage`
- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`

Optional:

- `--vintage-normalization-run-id`

Default behavior remains raw reconstructed quarterly inserts only. Vintage mode writes latest-compatible normalized rows, SEC statement vintages, and SEC field provenance through the SEC dual-write scaffold. It is covered by temp-DB tests. Quarter update does not currently call this CLI path; its current USA SEC path calls `run_sec_raw_bootstrap(...)` and `run_sec_quarterly_build_step(...)`.

### Yahoo-To-Generic Bridge CLI

`run_fundamental_yahoo_to_quarterly.py` has default-off vintage opt-in.

Required flags:

- `--write-vintage`
- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`

Optional:

- `--vintage-normalization-run-id`

Default behavior remains latest-only bridge writes. Vintage mode writes latest-compatible rows, Yahoo statement vintages, and Yahoo bridge provenance. It is covered by temp-DB tests. Quarter update currently calls this path only for non-USA Yahoo refresh.

### Yahoo Fallback Enrich CLI

`run_fundamental_yahoo_fallback_enrich.py` has default-off vintage opt-in.

Required flags:

- `--write-vintage`
- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`

Optional:

- `--vintage-normalization-run-id`

Default behavior remains latest/audit-only fallback enrich. Vintage mode writes one vintage per affected period, uses Yahoo fallback provenance for filled fields, preserves unknown retained fields as unknown, and uses a distinct missing-quarter insert provenance policy. It is covered by temp-DB tests. Quarter update currently calls fallback enrich without vintage flags.

## Future Quarter Update Vintage Opt-In Proposal

Future quarter update flags should be default-off:

```text
--write-vintage
--vintage-market usa
--vintage-available-at-utc YYYY-MM-DDTHH:MM:SSZ
--vintage-ingested-at-utc YYYY-MM-DDTHH:MM:SSZ
--vintage-run-id RUN_ID optional
--vintage-normalization-run-id RUN_ID optional
--vintage-mode sec_only|sec_plus_yahoo_fallback|yahoo_only_if_non_usa
```

Rules:

- `--write-vintage` defaults to false.
- If false, quarter update behavior must remain unchanged.
- If true, required PIT metadata must be explicit.
- Never infer `available_at_utc` from `period_end_date`.
- Never use current clock implicitly for required vintage timestamps.
- Do not silently downgrade to latest-only if vintage was requested but cannot be safely executed.
- Fail before provider/sub-step execution if required vintage metadata is missing.

`--vintage-run-id` can default to a deterministic child id such as `BASE__VINTAGE` only if that rule is explicitly implemented and tested. Otherwise require it. The safer first implementation is to require it when `--write-vintage` is set.

## Recommended First Implementation Scope

Four options were considered:

- Option 1: pass vintage flags only to SEC reconstruct path.
- Option 2: pass vintage flags to SEC reconstruct and Yahoo fallback enrich.
- Option 3: create one final mixed vintage after all SEC + Yahoo enrichment steps.
- Option 4: validate quarter_update vintage flags and expose summary plumbing, but do not execute vintage writes yet.

Recommended Phase 4I6: Option 4.

Reasoning:

- Current quarter update does not use the SEC reconstruct CLI vintage path directly; it uses `run_sec_raw_bootstrap(...)` plus `run_sec_quarterly_build_step(...)`.
- USA quarter update can run SEC and then Yahoo fallback in one ticker flow, creating duplicate/final-vs-intermediate vintage ambiguity.
- Non-USA quarter update can run Yahoo bridge, but that path has different provenance semantics from USA fallback.
- Quarter update also controls TTM, lifecycle, score, ack, and valuation; vintage failures must not leave ambiguous downstream state.
- Flag validation and summary plumbing can be tested without providers, real DB writes, or scheduler risk.

Phase 4I6 should add default-off flag parsing, validation, and summary fields only. It should not pass vintage flags to subpaths yet.

If a narrower implementation is later accepted, SEC-only mocked wiring can follow as Phase 4I7. Full SEC + Yahoo fallback mixed vintage orchestration should wait until duplicate/no-op/final-vintage behavior is proven in dedicated tests.

## SEC And Yahoo Fallback Coordination Policy

The conservative policy is one final accepted vintage per affected period after all enrichment steps in quarter update, when possible.

If SEC writes a vintage and Yahoo fallback later fills missing fields, there are two possible models:

- write a second mixed vintage after fallback
- avoid the intermediate SEC vintage and write only the final mixed vintage

The preferred policy for quarter update is final mixed vintage after all enrichment. This avoids treating a pre-fallback SEC-only row as the final accepted row when the same quarter_update run later changes fields using Yahoo fallback.

If future sub-step vintages are written, their role must be explicit:

- SEC sub-step vintage: intermediate/provider-stage evidence, not final accepted quarter_update row
- Yahoo fallback vintage: mixed final candidate only if all downstream enrichment for that period is complete
- no-op fallback: no new vintage

Duplicate vintages should be avoided by deterministic source hashes and by not writing final vintages twice for the same ticker/period/source hash. Plain `INSERT` integrity errors should surface clearly if duplicates occur.

## Summary Reporting Design

Future quarter update summary should include:

- `vintage_requested`
- `vintage_mode`
- `vintage_rows_inserted`
- `vintage_provenance_rows_inserted`
- `vintage_rows_skipped_noop`
- `vintage_rows_failed`
- `vintage_error_summary`

Per child step status should include:

- SEC reconstruct or SEC quarterly build vintage status
- Yahoo bridge vintage status
- Yahoo fallback vintage status
- final mixed vintage status if a final-write model is implemented

Default summary compatibility matters. If `--write-vintage` is false, either omit new fields or include stable zero/false fields only after tests confirm current consumers tolerate them. The safest Phase 4I6 approach is to add explicit summary fields only in opt-in or dry-run design mode.

## Tests Required For Implementation Phase

Future Phase 4I6 tests should cover:

1. default quarter update unchanged
2. `--write-vintage` requires metadata flags
3. summary includes `vintage_requested=false` by default if summary plumbing adds the field
4. opt-in with missing metadata fails before provider/subpath calls
5. opt-in SEC-only mocked path passes expected vintage flags to SEC reconstruct if SEC-only wiring is implemented later
6. opt-in does not call Yahoo fallback vintage path unless selected
7. no-op fallback does not create vintage
8. duplicate vintage errors surface clearly
9. existing quarter_update tests remain passing
10. no real DB/provider/network calls

If Phase 4I6 follows the recommended validation-only scope, tests 5-8 should be documented as later wiring tests, not implemented yet.

## Risks And Open Questions

- Quarter update currently spans refresh, enrichment, TTM, lifecycle, scoring, ack, and valuation.
- SEC and Yahoo fallback can both affect the same USA period, creating duplicate or intermediate-vs-final vintage ambiguity.
- A pre-fallback SEC vintage could be mistaken for the final accepted row if not explicitly modeled.
- `available_at_utc` for a multi-step run may differ between SEC observation, Yahoo fallback availability, and final accepted row time.
- Child run id naming needs a deterministic vintage convention.
- Summary compatibility with existing operational consumers must be preserved.
- Valuation may eventually need vintage availability semantics, but not in this phase.
- Quarter-state ack should not be coupled to vintage success until failure semantics are explicitly defined.

## Recommended Next Phase

Recommended Phase 4I6: quarter_update vintage flag validation and summary plumbing, no vintage execution.

This is intentionally conservative. It proves the public quarter_update interface and failure behavior without introducing provider, scheduler, real DB, or duplicate-vintage risk.

## Phase 4I6 Implementation Reference

Phase 4I6 implements the recommended validation-only scope in [Reported Vintage Quarter Update Validation Plumbing Phase 4I6](swingmaster_reported_vintage_quarter_update_validation_plumbing_phase4i6.md).

The implementation adds default-off CLI flags, metadata validation, and opt-in summary fields. It still does not execute vintage writes or pass vintage flags to SEC/Yahoo subpaths.

## Phase 4I7 Status

Phase 4I7 adds the first mocked SEC-only quarter_update forwarding path, documented in [Reported Vintage Quarter Update SEC Forwarding Phase 4I7](swingmaster_reported_vintage_quarter_update_sec_forwarding_phase4i7.md).

The new mode is `sec_reconstruct_only`. It remains default-off, requires explicit PIT metadata, and does not implement Yahoo fallback forwarding or final mixed-vintage orchestration.
