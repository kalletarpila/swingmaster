# SwingMaster Reported Vintage Production Write Path Design Phase 4H1

## Scope

Phase 4H1 is a design phase for future reported-vintage production write-path integration.

This phase does not:

- change production write paths
- wire SEC, Yahoo, provider, refresh, fallback, or quarter-update paths to the vintage writer
- write to the real DB
- run providers
- run refresh jobs or schedulers
- change current readers
- change TTM, scoring, valuation, or percentile logic
- implement ESS integration

The legacy baseline is already loaded and verified. The remaining question is how future reported-fundamentals writes should create new vintages without breaking current latest-table compatibility.

## Current Write Paths

| Path/module | Table(s) written | Current behavior/key | Timestamp/source metadata | PIT/vintage metadata available | Gaps | USA-critical | Wiring recommendation |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SEC raw bootstrap: `run_fundamental_bootstrap_sec_raw.py` + `sec_edgar.py` | `rc_fundamental_statement_raw` | `INSERT OR REPLACE`; key is raw table PK over ticker/type/period/field | `source='sec_edgar'`, `retrieved_at_utc`, `run_id`; SEC `filed` is encoded into `field_name` | Can infer source provider and filed date from encoded SEC fact fields; can build source hash from raw facts | No first-class CIK/accession/document id; `available_at_utc` not first-class; raw rows can replace previous raw row with same key | yes | Later. First define SEC source-hash and availability policy, then feed normalization/vintage builder. |
| SEC reconstruction: `sec_reconstruct_quarterly.py` + `run_fundamental_sec_reconstruct_quarterly.py` | `rc_fundamental_statement_raw` reconstructed quarterly rows | `INSERT OR REPLACE`; writes normalized quarterly raw rows back to raw table | `retrieved_at_utc`, `run_id`, `source='sec_edgar'`, `period_type='reconstructed_quarterly'`; source facts include encoded `filed` | Can select filed dates from chosen SEC facts; can infer field-level SEC source refs from selected encoded facts | Current output rows do not preserve selected input fact ids/hashes as first-class lineage | yes | Later. Needs lineage bundle from selected facts before production dual-write. |
| Generic quarterly builder: `build_quarterly.py` | `rc_fundamental_quarterly` | `INSERT OR REPLACE`; key `(ticker, period_end_date)` | only `run_id`; raw source must be traced through input raw rows | Can produce normalized values; no provider timing by itself | Cannot infer `available_at_utc`, provider document id, or field-level source without caller metadata | yes | Phase 4H2 candidate: add opt-in/test-only builder-level dual-write wrapper with explicit metadata. |
| Yahoo raw audit: `run_fundamental_yahoo_audit.py` | `rc_fundamental_yahoo_raw` | plain inserts by audit run | provider, symbol, raw JSON payloads, `payload_hash`, `loaded_at_utc`, status, run id | Can provide Yahoo payload hash and provider observed/loaded timestamp | Does not prove true report publication timestamp; raw payload is not canonical statement vintage by itself | yes/OMXH | Later. Use as provider staging source for Yahoo-derived vintages. |
| Yahoo normalized quarterly staging: `run_fundamental_yahoo_quarterly_write.py` | `rc_fundamental_yahoo_quarterly` | plain inserts; optional `--replace-symbol` deletes existing staging rows first | `source_run_id`, `run_id`, `created_at_utc`, shares source/quality | Can link back to Yahoo raw run and `payload_hash` indirectly; can use created/loaded time as observed time | Replace-symbol behavior loses staging history unless raw audit remains; no first-class availability | yes/OMXH | Later. Keep as staging; do not treat as final vintage without source hash/availability policy. |
| Yahoo to generic bridge: `run_fundamental_yahoo_to_quarterly.py` | `rc_fundamental_quarterly` | optional delete by ticker, then `INSERT OR REPLACE` into latest table | summary source is Yahoo; row stores only `run_id`; values come from Yahoo staging | Can infer provider from path and source run from staging if carried explicitly | Current generic row loses Yahoo payload hash and created/loaded timestamp | yes/OMXH | Later. Needs explicit metadata passing before production vintage writes. |
| Yahoo fallback enrich: `run_fundamental_yahoo_fallback_enrich.py` | updates `rc_fundamental_quarterly`; inserts `rc_fundamental_quarterly_enrichment_audit` | field-level `UPDATE` for missing latest fields; plain audit inserts; optional audit delete by run | audit stores `primary_source`, `fallback_source`, old/new values, matched Yahoo period, match method, `created_at_utc`, run id | Strong field-level provenance for Yahoo fills; knows SEC retained vs Yahoo-filled roles | No statement vintage creation; availability/source hash must combine SEC baseline and Yahoo payload source | yes | Later than 4H2. Important but should wait until builder-level metadata contract is stable. |
| Quarter update orchestrator: `run_fundamental_quarter_update.py` | multiple tables through SEC/Yahoo refresh, TTM, lifecycle, score, valuation, quarter-state ack | runs many write paths and downstream recalculations | child run ids per step; detected source period; current timestamps | Can coordinate future vintage writes after individual paths are safe | Too broad; mixes provider refresh, fallback, TTM/scoring/valuation, and ack behavior | yes | Not Phase 4H2. Wire only after lower-level dual-write behavior is proven. |
| Reported dual-write helper: `reported_quarterly_dual_write.py` | `rc_fundamental_quarterly`, `rc_fundamental_quarterly_vintage`, provenance table | latest write uses existing `insert_quarterly_rows`; vintage/provenance use plain inserts | requires explicit PIT metadata; supports `field_source_map` | Good low-level abstraction for future integration | Not wired to providers; caller must supply deterministic ids, availability, source hash, and field provenance | yes | Use as implementation foundation, but first behind opt-in/test-only entry points. |

## Target Future Write Model

### Latest Table Compatibility

`rc_fundamental_quarterly` remains the current latest/read-compatible table.

Existing TTM, scoring, valuation, current readers, and operational flows should continue to read latest rows initially.

The latest table may keep its current replace/upsert semantics while vintage tables preserve history.

### Vintage Table Behavior

Every new accepted reported-fundamentals row should create a new `rc_fundamental_quarterly_vintage` row.

Vintage writes must use plain `INSERT`.

`INSERT OR REPLACE` must not be used for vintage rows.

Duplicate `statement_vintage_id` must either fail or be treated as an already-known no-op by explicit logic. It must not silently replace an existing vintage.

`statement_vintage_id` must be deterministic from provider, market, ticker, period, value/source hash, and revision policy.

### Field Provenance

Each non-null field in a vintage row should have one provenance row.

SEC-retained fields should be marked as SEC primary/retained fields.

Yahoo-filled fields should be marked as Yahoo fallback fields.

Mixed SEC+Yahoo quarters must be represented with field-level provenance. Row-level `source_provider` alone is not enough.

The existing `field_source_map` support in `reported_quarterly_dual_write.py` is the right direction for mixed-source quarters.

### Available-At Policy For Future Provider Writes

Future provider writes must not use `LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL`.

SEC-derived rows should use actual SEC filing/accepted/publication timing when available. Repo evidence shows SEC `filed` is present inside encoded `field_name`; a later phase should parse it into first-class metadata. If exact accepted timestamp is unavailable, use a conservative provider observed or ingested timestamp and mark `availability_quality` accordingly.

Yahoo-derived rows should use provider observed/loaded timestamp unless a better verified release date exists.

`available_at_utc` must be explicit and must never silently equal `period_end_date`.

### Source Hash Policy

SEC source hash should be derived from the exact selected SEC raw facts used for the normalized quarter. A future full-payload hash can supersede or complement encoded raw-row hashing.

Yahoo source hash should use `payload_hash` from `rc_fundamental_yahoo_raw` when available.

Mixed SEC+Yahoo quarters may need both:

- statement-level normalized row hash
- field-level source hashes/source refs

The statement-level hash should change when accepted normalized values or selected source bundle changes.

### Restatement And Provider Correction Policy

If raw facts change for the same market, ticker, and period, create a new vintage.

Do not overwrite old vintages.

Latest table may move to the newest accepted value.

PIT reads should select vintages with:

```text
available_at_utc <= decision_cutoff_utc
```

`is_restated` should mean a provider/company changed previously available facts for an already known period. It should not be set merely because SwingMaster backfilled missing local history.

`supersedes_vintage_id` should link to the prior accepted vintage when a correction/restatement is accepted.

## Metadata Requirements By Source

### SEC

Needed metadata:

- `source_provider = sec_edgar`
- `filed_at_utc` from SEC `filed` or accepted timestamp when available
- `available_at_utc` from accepted/publication/provider-observed policy
- `source_document_id` from accession/document identity when available
- `source_hash` from selected raw facts or full SEC payload
- `provider_run_id`
- `normalization_run_id`

Current repo gap:

- `filed` exists inside encoded raw `field_name`, but not as a first-class column.
- accession/document id is not persisted as first-class metadata.
- selected fact lineage is not emitted by reconstruction as a structured source bundle.

### Yahoo

Needed metadata:

- `source_provider = yahoo`
- `source_hash = payload_hash` from raw audit when available
- `provider_observed_at_utc` or `ingested_at_utc` from raw/staging timestamps
- `source_document_id` as raw row id/run/payload reference if no provider document id exists
- field-level source refs for fallback fills

Current repo gap:

- generic quarterly rows do not preserve Yahoo raw payload hash.
- Yahoo staging can be replaced per symbol.
- true report publication timestamp is not proven by Yahoo load time.

## Recommended Phase 4H2 Scope

Recommended option:

```text
Option 1: add a pure builder-level dual-write function that produces latest, vintage, and provenance rows from normalized quarterly outputs, still not called by provider CLIs.
```

Do not wire the full production quarter update path in Phase 4H2.

Do not wire SEC/Yahoo provider refresh CLIs in Phase 4H2.

Do not wire Yahoo fallback enrich to production vintage writes in Phase 4H2.

### Phase 4H2 Goal

Create an opt-in/test-only dual-write builder path around normalized quarterly rows.

It should accept explicit metadata:

- market
- statement vintage id policy or explicit id
- source provider
- source document id
- source hash
- filed/available/ingested/provider-observed timestamps
- run ids
- field source map

It should produce or write:

- current latest-compatible row
- one vintage row
- field provenance rows for non-null fields

### Likely Files

- `swingmaster/fundamentals/reported_quarterly_dual_write.py`
- `swingmaster/fundamentals/build_quarterly.py` only if a pure wrapper is needed
- new focused tests around normalized rows and explicit metadata

### Tests

Phase 4H2 tests should use temp DBs only and cover:

- latest row compatibility
- vintage plain insert behavior
- duplicate vintage id failure/no-op policy
- field provenance for SEC-retained fields
- field provenance for Yahoo-filled fields through `field_source_map`
- no provider imports/calls
- no production CLI wiring

### Explicit Non-Goals

- no provider calls
- no real DB writes
- no quarter-update wiring
- no fallback enrich production wiring
- no TTM/scoring/valuation changes
- no ESS integration
- no reader migration

### Acceptance Criteria

Phase 4H2 is complete when a pure, tested builder-level dual-write path can create latest/vintage/provenance rows from explicit normalized input and explicit PIT metadata without invoking providers or production orchestration.

## Later Phases

After Phase 4H2:

- add SEC selected-fact lineage and source-hash construction
- parse SEC filed/accepted timing into first-class metadata
- pass Yahoo payload hash/source refs from raw/staging to normalized/generic writes
- model Yahoo fallback field provenance in production
- add opt-in production CLI wiring for a narrow path
- only then consider quarter-update orchestration and ESS read integration

## Risks

- Wiring too high in the stack first would mix provider refresh, fallback, TTM, scoring, valuation, and ack risks.
- Using `period_end_date` as availability would introduce lookahead.
- Reusing latest-table `INSERT OR REPLACE` semantics for vintage would destroy history.
- Row-level source metadata is insufficient for mixed SEC+Yahoo quarters.
- SEC reconstructed rows need selected-fact lineage before audit-grade provenance is possible.
- Yahoo load time is not the same as report publication time.

## Recommendation

Proceed to Phase 4H2 with a small, opt-in/test-only builder-level dual-write implementation.

Keep production provider paths unchanged until SEC/Yahoo availability and source-hash policies are explicit and tested.

## Phase 4H2 Status

Phase 4H2 adds `write_normalized_quarterly_rows_with_optional_vintage` in `reported_quarterly_dual_write.py` as an opt-in builder-level adapter for normalized quarterly rows.

Current default production paths are not wired to this adapter. `insert_quarterly_rows`, provider CLIs, refresh jobs, fallback enrichment, quarter-update orchestration, TTM, scoring, valuation, UI, and ESS behavior remain unchanged.

The adapter preserves latest-table compatibility by default. With `write_vintage=False`, it delegates to the existing latest-compatible `rc_fundamental_quarterly` insert behavior and does not write vintage or field-provenance rows.

Vintage writes are explicit. With `write_vintage=True`, every row must have metadata keyed by ticker and period, or by market/ticker/period when the row provides market. Required metadata includes `market`, `statement_vintage_id`, `source_provider`, `source_hash`, `available_at_utc`, `ingested_at_utc`, `run_id`, `revision_number`, `is_restated`, `availability_quality`, and `created_at_utc`. The adapter does not invent availability timestamps, vintage ids, source hashes, or provider metadata.

Vintage rows continue to use plain insert semantics through the vintage writer. Duplicate `statement_vintage_id` values raise SQLite integrity errors instead of replacing history. Field provenance is generated for non-null financial fields and can represent mixed SEC/Yahoo lineage through `field_source_map_by_key`.

Phase 4H2 remains a controlled integration primitive for later provider-specific phases. SEC/Yahoo source-hash policy, available-at policy, provider-path wiring, quarter-update wiring, and reader migration remain later work.
