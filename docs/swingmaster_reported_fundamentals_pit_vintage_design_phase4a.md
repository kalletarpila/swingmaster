# SwingMaster Reported Fundamentals PIT/Vintage Design Phase 4A

## 1. Scope

Phase 4A is a design phase for making `reported_fundamentals` point-in-time and vintage-aware in a later implementation phase. It does not add migrations, change runtime behavior, change provider logic, write DB files, run providers, run refresh jobs, or implement ESS integration.

The design is based only on current repo evidence. If a behavior is not visible in the repo, it is marked as unclear.

Target class covered here:

- `reported_fundamentals`

Out of scope:

- derived TTM/scoring lineage
- valuation price lineage
- percentile universe lineage
- event/expectation tables
- company/security-master

## 2. Current Reported-Fundamentals Write Model

| Table/path | Current key | Current timestamps | Source/provenance columns | Overwrite/upsert behavior | History preserved? | Restatement representation | PIT/backtest sufficiency | Gaps |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `rc_fundamental_statement_raw` | `(ticker, statement_type, period_end_date, field_name)` | `retrieved_at_utc` | `source`, `period_type`, `currency`, `run_id`; SEC metadata including `filed` is encoded in `field_name` | SEC bootstrap and reconstruction use `INSERT OR REPLACE` | Partial: multiple SEC facts can coexist if `field_name` differs; same key is replaced | No explicit restatement model; later facts may replace same key | Not enough: `filed` is not first-class, no `available_at_utc`, no vintage id | Needs provider observation time, source hash, document id, filed date column, and vintage linkage |
| SEC raw bootstrap: `run_fundamental_bootstrap_sec_raw.py` + `sec_edgar.py` | Same as raw table | Caller supplies `retrieved_at_utc` | `source='sec_edgar'`, SEC CIK returned in summary only, `field_name` embeds tag/form/unit/fy/fp/frame/start/filed | `INSERT OR REPLACE` into raw table | Partial, dependent on encoded `field_name` uniqueness | No explicit old/new vintage chain | Not enough: CIK/document/source hash not persisted as first-class metadata | Store provider document identity and source hash; parse `filed` into first-class column |
| SEC reconstruction: `sec_reconstruct_quarterly.py` | Reconstructed rows are written back to `rc_fundamental_statement_raw` with normalized field names | Caller supplies `retrieved_at_utc` | `source='sec_edgar'`, `period_type='reconstructed_quarterly'` by output row evidence, `run_id` | `INSERT OR REPLACE` into raw table | Limited: reconstructed rows can replace same normalized field | No explicit link from reconstructed row to source fact vintage set | Not enough: reconstructed rows do not retain input vintage hash | Add normalization run id, input raw hash/vintage id, and method version |
| Generic quarterly: `rc_fundamental_quarterly` + `build_quarterly.py` | `(ticker, period_end_date)` | None beyond `run_id` | Only `run_id`, no source/provider/field-level provenance | `INSERT OR REPLACE` | No; latest row overwrites prior value for ticker/period | Cannot represent multiple vintages or restatements | Not enough: no availability, filed date, source, vintage, or field provenance | Keep compatibility, but add vintage sidecar/table before changing readers |
| Yahoo raw: `rc_fundamental_yahoo_raw` + `run_fundamental_yahoo_audit.py` | Autoincrement `id`; indexes on `(market, symbol)`, `run_id`, `status` | `loaded_at_utc` | `provider`, `symbol`, payload JSON fields, `payload_hash`, `status`, `error_message`, `run_id` | Plain `INSERT`; no replace | Yes, multiple raw audit rows can exist | Provider corrections can be observed by payload hash changes, but no explicit vintage relation | Better than generic quarterly, but still lacks `available_at_utc` and canonical statement vintage linkage | Link raw payload rows to future quarterly vintages; define provider observed vs available time |
| Yahoo normalized quarterly: `rc_fundamental_yahoo_quarterly` + `run_fundamental_yahoo_quarterly_write.py` | `(market, symbol, period_end_date)` | `created_at_utc` | `source_run_id`, `run_id`, shares source/quality | Optional `--replace-symbol` deletes existing rows for symbol; insert has no replace clause but PK prevents duplicate without delete | Usually no, if replaced; source raw row remains in Yahoo raw | No explicit restatement/vintage chain | Not enough: staging can be replaced and does not model availability | Treat as provider staging; later vintage table should consume it with source hash/run ids |
| Yahoo to generic quarterly: `run_fundamental_yahoo_to_quarterly.py` | Generic `(ticker, period_end_date)` | None beyond `run_id` | Summary has `source='yahoo'`; row stores only `run_id` | Optional `--replace-symbol` deletes generic quarterly rows for ticker; insert uses `INSERT OR REPLACE` | No | No | Not enough and potentially destructive for PIT if used as canonical | Future PIT model should avoid direct destructive history loss; dual-write to vintage first |
| Yahoo fallback enrich: `run_fundamental_yahoo_fallback_enrich.py` | Updates generic `(ticker, period_end_date)` fields; audit rows are separate | Audit uses `created_at_utc` | Audit stores `primary_source='sec_edgar'`, `fallback_source='yahoo'`, `matched_yahoo_period_end_date`, `match_method`, `run_id` | Missing fields are updated in `rc_fundamental_quarterly`; missing quarter may be inserted from Yahoo; audit rows may be deleted for run with `--replace-audit-for-run` | Generic row history no; audit history partial | Can show field fill, not full vintage chain | Not enough: field-level source is only in audit, not current row/vintage | Future field-level provenance should link fills to statement vintage and source vintage |
| Enrichment audit: `rc_fundamental_quarterly_enrichment_audit` | Autoincrement `id` | `created_at_utc` | `old_value`, `new_value`, primary/fallback source, status, run; V2 adds matched Yahoo period and match method via migration helper | Plain insert, except optional delete by run | Partial; can be deleted by run | Field-level event log, not statement vintage | Useful but not enough for PIT because it is not the authoritative read model | Keep as operational audit; link to future vintage/provenance tables |
| Quarter state: `rc_fundamental_quarter_state` | `ticker` | `last_checked_at_utc`, `last_updated_at_utc` | `market`, `primary_source`, detection/ingest run ids | `ON CONFLICT(ticker) DO UPDATE` | No; operational latest state only | Not intended | Not a PIT table | Keep operational; do not use as historical availability source |
| Reporting frequency classification | `(ticker, market, as_of_date, classifier_version, run_id)` | `created_at_utc` | `classifier_version`, observed/missing periods, run | Insert by run/key | Yes by run | Not statement-level | Useful audit, not reported-fundamental vintage | Later can reference vintages but should not own them |
| Missing period recovery check | `(ticker, market, classification_run_id, missing_period_end_date, run_id)` | `checked_at_utc`, `created_at_utc` | classification run, recovery status, found dates, run | Insert by run/key | Yes by run | Not statement-level | Useful audit, not reported-fundamental vintage | Later can report vintage coverage gaps |

## 3. Target PIT/Vintage Semantics

These concepts apply to reported fundamentals only.

| Concept | Target meaning |
| --- | --- |
| `period_end_date` | Fiscal/reporting period end. It is not availability and must not be used as a decision timestamp. |
| `filed_at_utc` | Provider/company publication time when known. For SEC, repo evidence shows `filed` exists inside encoded raw fact metadata; exact time-of-day is unclear. |
| `available_at_utc` | The earliest time SwingMaster can safely use the row in a backtest/ESS read. Later ESS reads should filter with `available_at_utc <= decision_cutoff_utc`. |
| `ingested_at_utc` | Local time SwingMaster stored or processed the source data. Current equivalents are `retrieved_at_utc`, `loaded_at_utc`, and `created_at_utc` depending on path. |
| `provider_observed_at_utc` | Time SwingMaster observed the provider payload/fact. Often same as ingest time unless provider-specific observation metadata exists. |
| `source_provider` | Provider family such as `sec_edgar`, `yahoo`, or future provider names. |
| `source_document_id` | Provider document or payload identity. For SEC this could be accession/document identity if available later; current repo does not persist it. For Yahoo this can initially reference raw row id/run plus payload hash. |
| `source_hash` | Stable hash of raw provider input or normalized source bundle. Yahoo raw already has `payload_hash`; SEC needs an equivalent. |
| `statement_vintage_id` | Stable id for one accepted version of ticker/period reported fundamentals. Should identify the value set, provider lineage, and availability. |
| `revision_number` | Monotonic integer per `market+ticker+period_end_date` and statement scope, incremented for each new accepted vintage. |
| `is_restated` | True when the provider/company changed previously available facts for an already known period, not when SwingMaster merely backfills missing local data. |
| `supersedes_vintage_id` | Previous accepted vintage replaced or superseded by this vintage. |
| `availability_quality` | Classification of how reliable availability timing is, e.g. `FILED_AT_KNOWN`, `INGESTED_AT_ONLY`, `BACKFILLED_UNKNOWN_AVAILABILITY`, `MANUAL_CORRECTION`. |
| `run_id` | Current execution id that wrote the row. |
| `provider_run_id` | Provider raw/audit fetch run id. |
| `normalization_run_id` | Run id that converted raw/provider facts to normalized quarterly facts. |
| `enrichment_run_id` | Run id that applied fallback field fills or inserted a missing fallback quarter. |

Important rule:

```text
For backtest/ESS later, use only rows where available_at_utc <= decision_cutoff_utc.
```

## 4. Physical Schema Strategy Options

### Option A: Add PIT/vintage columns directly to `rc_fundamental_quarterly`

Benefits:

- Smallest conceptual change for current readers.
- Keeps one row source for downstream TTM, valuation, and snapshots.
- Simple to query latest values if only latest vintage matters.

Drawbacks:

- Current primary key `(ticker, period_end_date)` cannot represent multiple vintages.
- Adding fields directly does not solve field-level provenance after Yahoo fallback.
- Current `INSERT OR REPLACE`/update paths would still risk silent history loss unless rewritten.

Migration complexity:

- Medium for adding columns; high if primary key or uniqueness model changes.

Compatibility:

- Good if only nullable columns are added.
- Poor if multiple vintages require primary key changes.

Backfill complexity:

- Medium. Existing rows can get conservative `available_at_utc` values, but historical accuracy would be limited.

Test impact:

- Broad if write paths must preserve multiple rows.

ESS suitability:

- Weak as the final model because one row per ticker/period cannot represent restatements.

### Option B: Keep `rc_fundamental_quarterly` as current latest/canonical view and add `rc_fundamental_quarterly_vintage`

Benefits:

- Preserves compatibility with current downstream readers.
- New table can represent multiple vintages per ticker/period.
- Lets Phase 4 implementation dual-write without breaking current TTM/scoring/valuation.
- Supports `available_at_utc <= decision_cutoff_utc` reads.

Drawbacks:

- Requires synchronization policy between latest table and vintage table.
- More complex queries for future ESS adapter.
- Needs careful backfill and source-link strategy.

Migration complexity:

- Medium. New table can be introduced without changing existing primary keys.

Compatibility:

- Strong. Existing `rc_fundamental_quarterly` readers can continue.

Backfill complexity:

- Medium/high. Existing latest rows can become an initial vintage with conservative availability quality.

Test impact:

- Moderate. New tests can cover dual-write and point-in-time reads without rewriting every existing reader.

ESS suitability:

- Strong. This is the preferred direction.

### Option C: Add separate PIT/audit sidecar tables without changing `rc_fundamental_quarterly` immediately

Benefits:

- Lowest immediate risk to current canonical quarterly table.
- Can capture provenance/vintage metadata as sidecar rows.
- Can be introduced incrementally before full vintage row materialization.

Drawbacks:

- Harder to guarantee that a sidecar matches the exact values in quarterly row after updates.
- Field-level provenance can become fragmented.
- Future ESS reads may require joining latest values to sidecar metadata with ambiguity.

Migration complexity:

- Low/medium.

Compatibility:

- Strong for current readers.

Backfill complexity:

- Medium; easier than full vintage values but less complete.

Test impact:

- Moderate; needs consistency tests between sidecar and canonical row.

ESS suitability:

- Acceptable as a transitional step, weaker than a full vintage table.

### Recommendation

Prefer Option B, possibly with a short Option C-style transition for provenance.

Recommended later implementation direction:

- Keep `rc_fundamental_quarterly` as the current latest-compatible read model.
- Add a new `rc_fundamental_quarterly_vintage` table in a later migration phase.
- Add field-level provenance either as a companion table or encoded in a separate `rc_fundamental_quarterly_vintage_field_source` table.
- Do not break existing TTM/scoring/valuation readers until PIT reads are explicitly introduced.

## 5. Source/Provider Handling Design

### SEC flattened raw facts

Current raw SEC facts are flattened into `rc_fundamental_statement_raw`, with SEC metadata packed into `field_name`.

Later design should:

- parse `filed` from encoded `field_name` into first-class `filed_at_utc` or `filed_date`
- store `source_provider='sec_edgar'`
- store `source_hash` for the source fact set used for one statement vintage
- preserve raw rows for reproducibility
- add provider document identity if SEC accession/document id becomes available

Unclear from current repo:

- whether full SEC CompanyFacts JSON should be persisted
- whether SEC accession number is available in the current payload extraction

Recommendation:

- Flattened raw + deterministic source hash is likely enough for the first PIT implementation.
- Full CompanyFacts JSON storage can be deferred unless reproducibility gaps appear.

### SEC reconstructed quarter output

SEC reconstruction selects facts and writes normalized raw rows back to `rc_fundamental_statement_raw`.

Later design should:

- record `normalization_run_id`
- record selected input fact hashes or an `input_vintage_hash`
- treat reconstructed quarter output as the source for a quarterly statement vintage
- keep reconstruction rule/version metadata when introduced

### Yahoo raw payload

Yahoo raw has the best current hash support:

- `payload_hash`
- `loaded_at_utc`
- `provider`
- `symbol`
- status/error rows

Later design should:

- map raw Yahoo rows to provider source vintages
- store `provider_run_id`
- use `payload_hash` as source hash input
- avoid making Yahoo symbol canonical identity

### Yahoo normalized quarterly staging

Yahoo quarterly staging has `(market, symbol, period_end_date)`, `source_run_id`, `run_id`, and `created_at_utc`.

Later design should:

- treat staging rows as provider-normalized candidate values
- avoid destructive replacement as the only history mechanism
- link accepted values to the raw Yahoo source run/hash

### Yahoo fallback field fills

Current fallback keeps existing SEC values and only fills missing fields from Yahoo. Audit rows record field name, old/new value, primary/fallback source, matched Yahoo period, and match method.

Later design should:

- represent field-level source separately from the statement vintage row
- record that SEC fields were retained and Yahoo filled only specific missing fields
- link fallback audit rows to `statement_vintage_id`
- use `enrichment_run_id` for the fallback run

Suggested field-level provenance table for a later phase:

```text
rc_fundamental_quarterly_vintage_field_source
  statement_vintage_id
  field_name
  source_provider
  source_vintage_id_or_raw_id
  source_run_id
  source_hash
  match_method
  availability_quality
```

## 6. Restatement and Overwrite Policy

Future intended behavior:

- New provider data for the same ticker/period must not silently erase old historical truth.
- Restatements create a new statement vintage.
- The latest read model can still expose the latest accepted values.
- Backtests must select the latest vintage with `available_at_utc <= decision_cutoff_utc`.
- Local correction/backfill must be distinguishable from provider restatement.

### Scenario: normal new quarter

1. Provider data arrives for a new `period_end_date`.
2. Raw/provider rows are stored with source hash and observed/ingested time.
3. Normalization creates `revision_number=1`.
4. `available_at_utc` is set from filed time if known, otherwise conservative ingest time.
5. Latest-compatible quarterly table can be updated to the new latest value.

### Scenario: SEC restatement for old quarter

1. New SEC facts arrive for an existing `ticker+period_end_date`.
2. Source hash differs from prior accepted vintage.
3. A new `statement_vintage_id` is created with `revision_number = previous + 1`.
4. `is_restated=true` and `supersedes_vintage_id` points to the prior vintage.
5. Backtests before the new `available_at_utc` still see the old vintage.

### Scenario: Yahoo fills missing field after SEC import

1. SEC-origin vintage exists with missing fields.
2. Yahoo fallback finds matching period/date tolerance.
3. A new enriched vintage is created or a field-source sidecar records Yahoo-filled fields.
4. SEC-retained fields remain sourced to SEC.
5. Yahoo-filled fields are linked to Yahoo raw/staging source hash and `enrichment_run_id`.

### Scenario: provider correction changes raw value

1. Provider payload hash changes for the same period.
2. If accepted values change, create a new vintage.
3. If only raw payload metadata changes and accepted values do not, record source observation but do not necessarily create a new statement vintage.

### Scenario: normalization rule changes after the fact

1. Raw source facts remain the same.
2. Normalization logic/rule version changes accepted values.
3. Create a new local normalization vintage with `availability_quality='LOCAL_RULE_BACKFILL'` or similar.
4. Do not mark as provider restatement unless provider data changed.

## 7. Later Migration/Backfill Plan

### Phase 4B: schema migration design and temp-DB tests only

Goal:

- Design exact tables, indexes, keys, and migration SQL for vintage/PIT storage.

Likely files:

- new migration draft under `swingmaster/infra/sqlite/migrations/`
- migration helper tests
- design doc updates

Tests:

- temp-DB migration tests only
- no real DB writes

Explicit non-goals:

- no provider calls
- no production DB backfill
- no write-path changes

Rollback/safety:

- migration must be additive and reversible by restoring DB backup; do not alter current primary keys in 4B.

### Phase 4C: write-path dual-write or sidecar-write implementation with mocked tests

Goal:

- Add code paths that write current tables and new vintage/sidecar tables in the same operation, using mocks/temp DBs.

Likely files:

- SEC raw/reconstruction write paths
- Yahoo raw/quarterly/fallback paths
- new vintage helper module
- tests for dual-write idempotency

Tests:

- mocked provider payloads
- temp SQLite DBs
- no network

Explicit non-goals:

- no real DB backfill
- no ESS adapter reads

Rollback/safety:

- keep current write path behavior intact; new vintage writes should be additive and guarded by tests.

### Phase 4D: read-only backfill/preflight for existing local DBs

Goal:

- Build a read-only report estimating how existing DB rows would map to initial vintages.

Likely files:

- preflight/backfill analysis CLI
- docs
- tests with fixture DBs

Tests:

- temp DBs with representative current rows
- no writes

Explicit non-goals:

- no actual backfill
- no production DB mutation

Rollback/safety:

- read-only mode and `PRAGMA query_only=ON`.

### Phase 4E: guarded backup-confirmed migration/backfill for real DB if later approved

Goal:

- Apply additive migration and backfill real DBs only after explicit approval and backups.

Likely files:

- guarded backfill CLI
- backup checks
- dry-run mode
- audit output

Tests:

- temp DB backfill tests
- dry-run verification
- backup-required failure tests

Explicit non-goals:

- no provider refresh during backfill
- no scheduler integration

Rollback/safety:

- require explicit DB path, explicit backup confirmation, dry-run summary, and post-backfill validation before real writes.

## 8. Phase 4A Conclusion

Current reported-fundamentals storage is good enough for current operational latest-value workflows, but not enough for ESS-grade point-in-time backtests. The major missing pieces are first-class availability time, source hash, statement vintage id, revision/restatement semantics, and field-level provenance after SEC/Yahoo merge.

The recommended path is additive:

1. keep `rc_fundamental_quarterly` compatible for current readers
2. add a vintage table later
3. add field-level provenance for mixed SEC/Yahoo rows
4. backfill conservatively and only after a separate approved migration/backfill plan
