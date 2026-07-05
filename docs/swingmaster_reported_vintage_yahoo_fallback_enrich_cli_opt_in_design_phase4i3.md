# SwingMaster Reported Vintage Yahoo Fallback Enrich CLI Opt-In Design Phase 4I3

## Scope

Phase 4I3 is a design-only phase for a future default-off vintage opt-in in `run_fundamental_yahoo_fallback_enrich.py`.

This phase changes no runtime code, no tests, no migrations, and no DB data. It does not call providers, network APIs, refresh jobs, schedulers, or the real `fundamentals_usa.db`. It does not wire Yahoo fallback enrich, `run_fundamental_quarter_update.py`, SEC paths, UI, scoring, valuation, TTM, or ESS to vintage reads or writes.

## Current Fallback Enrich Behavior

The fallback enrich CLI reads current generic quarterly rows from `rc_fundamental_quarterly` and Yahoo staging rows from `rc_fundamental_yahoo_quarterly`.

Ticker selection is market-based:

- `market = usa` scans tickers in `rc_fundamental_quarterly` that do not end with `.HE`
- other markets scan tickers ending in `.HE`
- `--ticker` restricts processing to one ticker

For each generic quarterly row, the CLI loads Yahoo rows for the same `market + ticker` and resolves a Yahoo match:

- exact match when Yahoo `period_end_date` equals the generic row period
- otherwise same calendar year and quarter, with absolute date difference at most 7 days
- if multiple tolerance candidates exist, smallest absolute difference wins, then earlier Yahoo date wins

The CLI only fills currently `NULL` generic fields. It does not overwrite existing generic values. Fillable fields are:

- `revenue`
- `gross_profit`
- `operating_income`
- `net_income`
- `operating_cashflow`
- `capex`
- `free_cashflow`
- `cash`
- `total_debt`
- `shares_outstanding`

For each filled field, it updates `rc_fundamental_quarterly` and inserts one row into `rc_fundamental_quarterly_enrichment_audit`.

Current audit rows include:

- `ticker`
- `period_end_date`
- `field_name`
- `old_value = NULL`
- `new_value`
- `primary_source = sec_edgar`
- `fallback_source = yahoo`
- `enrichment_status = FILLED_FROM_YAHOO`
- `matched_yahoo_period_end_date`
- `match_method = EXACT` or `SAME_QUARTER_DATE_TOLERANCE`
- `run_id`
- `created_at_utc`

The CLI also has missing-quarter insertion behavior through `insert_missing_quarterly_row_from_yahoo(...)` when `detected_source_period_end_date` is supplied programmatically. It can insert an entire `rc_fundamental_quarterly` row from the matched Yahoo row if no generic quarterly row satisfies the detected period. Current CLI argument parsing does not expose `detected_source_period_end_date`, and this insertion path does not currently create enrichment audit rows.

Current summary output reports market, ticker/row counts, `fields_checked`, `fields_filled`, `rows_updated`, `rows_inserted`, no-match and match counts, dry-run, run id, and per-field fill counts.

## Future Vintage Opt-In Behavior

The future implementation should keep default behavior unchanged:

- `--write-vintage` defaults to false
- default mode writes latest/audit rows exactly as today
- default mode writes no `rc_fundamental_quarterly_vintage` rows
- default mode writes no `rc_fundamental_quarterly_field_provenance` rows

When `--write-vintage` is true, the CLI should still perform latest-compatible updates/inserts as today, then write one statement vintage for the resulting period row and field provenance rows for non-null fields.

For fallback fills into an existing generic row, the resulting vintage is a mixed-source vintage:

- retained fields keep their existing source if it is known
- Yahoo-filled fields are represented from the current audit rows
- unknown retained fields must not be silently attributed to Yahoo

For missing-quarter insertion from Yahoo, the resulting vintage is Yahoo-source rather than SEC-retained. It should be treated as a Yahoo missing-quarter insert, not as field-level fallback into an existing SEC row.

If no fields are filled and no missing quarter is inserted, the future CLI should not create a new vintage row. No-op enrich should remain no-op for vintage unless a later phase explicitly defines a restatement/revision reason.

The implementation should write one mixed vintage per affected ticker/period after all fills for that period, not one vintage per filled field. The field-level audit rows should feed provenance; the statement vintage should represent the resulting full row state.

## Required CLI Flags

The future CLI should add:

```text
--write-vintage
--vintage-market usa
--vintage-available-at-utc YYYY-MM-DDTHH:MM:SSZ
--vintage-ingested-at-utc YYYY-MM-DDTHH:MM:SSZ
--vintage-run-id RUN_ID
--vintage-normalization-run-id RUN_ID optional
```

Rules:

- require `--vintage-market` when `--write-vintage` is set
- require `--vintage-available-at-utc` when `--write-vintage` is set
- require `--vintage-ingested-at-utc` when `--write-vintage` is set
- require `--vintage-run-id` when `--write-vintage` is set
- allow `--vintage-normalization-run-id` as optional metadata
- never infer availability from `period_end_date`
- never use current clock implicitly for required vintage timestamps
- fail clearly if field provenance cannot be built according to the selected policy

## Provenance Policy

Yahoo-filled fields from fallback audit rows should use:

- `source_provider = yahoo`
- `source_table = rc_fundamental_quarterly_enrichment_audit`
- `provenance_role = FALLBACK_REPORTED`
- `merge_action = YAHOO_FILLED_MISSING`

SEC-retained fields should use existing SEC provenance if it is available. The preferred source is a field-source map equivalent to `build_sec_field_source_map(...)`, keyed by ticker and period.

If SEC provenance is not available for retained fields, do not mark those fields as Yahoo. Either omit them if the writer contract permits omission, or mark them with:

- `source_provider = unknown`
- `provenance_role = UNSPECIFIED` or `PRIMARY_REPORTED_UNKNOWN_LEGACY`
- `merge_action = SOURCE_NOT_PROVIDED`

The current Yahoo fallback scaffold already marks non-null fields without explicit source metadata as `unknown` / `UNSPECIFIED` / `SOURCE_NOT_PROVIDED`. Future CLI wiring should preserve this safety behavior unless a stronger legacy SEC provenance source is available.

For Yahoo inserted missing quarters, use a separate missing-quarter policy:

- `source_provider = yahoo`
- `provenance_role = PROVIDER_REPORTED`
- `merge_action = YAHOO_INSERTED_MISSING_QUARTER`

Rationale: inserted quarters are not fallback fills into retained SEC rows. They are complete Yahoo-sourced generic quarterly rows created because the generic row was missing. `PROVIDER_REPORTED` is more accurate than `FALLBACK_REPORTED`, and `YAHOO_INSERTED_MISSING_QUARTER` is more explicit than reusing `YAHOO_BRIDGED`.

Unknown non-null fields must not be silently labeled Yahoo in any path.

## Statement Vintage ID And Source Hash

Statement vintage ids must be deterministic and should continue using the Yahoo metadata contract mode that matches the write:

- `yahoo_fallback_enrichment` for mixed fallback fills
- `yahoo_missing_quarter_insert` for inserted missing quarters

The source hash for fallback enrichment should include:

- resulting normalized row after all fills for the period
- Yahoo fallback audit rows that explain the filled fields
- Yahoo quarterly source row metadata when available
- Yahoo payload hash when available
- retained SEC or unknown field provenance when available

The source hash for missing-quarter insert should include:

- inserted normalized row
- matched Yahoo quarterly source row metadata
- Yahoo payload hash when available
- detected source period and match method when available

Fallback value changes must change the source hash and statement vintage id. Audit row order must not affect the source hash; existing Yahoo source-hash helper already sorts enrichment audit rows before hashing.

## Required Tests For Implementation Phase

Future Phase 4I4 should add temp-DB tests that cover:

1. default fallback enrich remains unchanged and writes latest/audit only
2. default mode creates no vintage/provenance rows
3. `--write-vintage` requires all metadata flags
4. exact-date fallback fill writes one mixed vintage and provenance
5. same-quarter/tolerance fallback fill writes one mixed vintage and provenance
6. Yahoo inserted missing quarter writes Yahoo-source vintage and provenance
7. no changed fields creates no vintage
8. SEC-retained fields are not marked Yahoo
9. unknown retained fields are not silently marked Yahoo
10. PIT reader returns no row before `available_at_utc` and a row at/after it
11. duplicate statement vintage id raises or surfaces `sqlite3.IntegrityError`
12. existing fallback enrich tests pass unchanged

The tests should use temp DBs only and must not call providers, refresh jobs, schedulers, or the real DB.

## Recommended Implementation Sequence

Phase 4I4 should implement Yahoo fallback enrich CLI vintage opt-in with temp-DB tests only. It should remain default-off and should not touch real DBs.

Recommended sequence:

1. add CLI flags and metadata validation
2. collect affected final normalized rows by ticker/period after pending fills/inserts
3. collect fallback audit rows by ticker/period before writing vintage
4. collect Yahoo source rows by ticker/period
5. pass explicit SEC/unknown retained-field source maps when available
6. call the existing Yahoo fallback dual-write scaffold for changed periods
7. add missing-quarter insert source-map behavior if that path is included
8. keep `run_fundamental_quarter_update.py` unwired

Phase 4I5 can run bounded fixture or smoke verification if needed. Quarter-update opt-in should be considered only after fallback CLI opt-in behavior is proven and reviewed.

## Risks And Open Questions

- Existing SEC provenance may not be available for older latest rows at fallback time. The safe fallback is unknown provenance, not Yahoo attribution.
- Legacy baseline rows may already have vintage/provenance rows that do not directly map to the latest row being enriched. The implementation must decide whether to reuse latest available provenance, explicit SEC facts, or unknown.
- Missing-quarter insertion currently has no audit rows. Future vintage wiring must add an explicit source-map path or add audit evidence before writing provenance.
- No-op enrich should not create a vintage row unless a later phase defines a specific revision semantics.
- The implementation should create one mixed vintage per affected period after all field fills, not one per field.
- `available_at_utc` remains a policy input. Yahoo observed/ingested time is not the same as verified issuer publication time.
- `replace_audit_for_run` can delete audit rows for a run before insert. Vintage implementation must ensure the audit rows used for source hash/provenance match the final stored audit evidence.

## Phase 4I4 Implementation Reference

Phase 4I4 implements the default-off Yahoo fallback enrich CLI vintage opt-in described here, documented in [Reported Vintage Yahoo Fallback Enrich CLI Opt-In Phase 4I4](swingmaster_reported_vintage_yahoo_fallback_enrich_cli_opt_in_phase4i4.md).

The implementation keeps default fallback behavior unchanged, writes no vintage rows for no-op enriches, and keeps `run_fundamental_quarter_update.py`, providers, schedulers, and real DB execution unwired.
