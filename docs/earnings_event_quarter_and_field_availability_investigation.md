# Earnings Event Quarter And Field Availability Investigation

## Scope

This investigation was performed read-only against:

```text
/home/kalle/projects/swingmaster/fundamentals_usa.db
```

Runtime artifacts were written only under:

```text
temp/earnings_event_matching_investigation/20260731T000000Z/
```

No schema changes, match links, quarterly row updates, earnings-event updates, schedulers, backfills, SEC fetches, or Yahoo network fetches were run.

## Schema Findings

`rc_fundamental_quarterly` is the latest merged quarterly state. It has `ticker`, `period_end_date`, financial fields, `currency`, and `run_id`, but no field-level timestamps, source identifiers, `created_at`, or `updated_at`.

`rc_fundamental_quarterly_vintage` stores statement vintages with source metadata:

- `statement_vintage_id`
- `source_provider`
- `source_document_id`
- `source_hash`
- `filed_at_utc`
- `available_at_utc`
- `ingested_at_utc`
- `provider_observed_at_utc`
- run identifiers
- the financial field snapshot for that vintage

`rc_fundamental_quarterly_field_provenance` stores field-level provenance per statement vintage and non-null financial field:

- `field_name`
- `field_value`
- `source_provider`
- `source_table`
- `source_row_ref`
- `source_document_id`
- `source_hash`
- `provenance_role`
- `merge_action`
- `old_value`
- `new_value`
- `available_at_utc`
- `created_at_utc`

`rc_earnings_event` stores announcement events:

- `announcement_at`
- `announcement_date`
- `announcement_session`
- EPS fields
- `source_observed_at_utc`
- `source_timezone`
- create/update timestamps

It intentionally does not store `period_end_date`.

## Write-Path Findings

The latest quarterly writer still writes `rc_fundamental_quarterly` as the current merged state.

The vintage writer is opt-in. When enabled, it inserts one `rc_fundamental_quarterly_vintage` row and field-provenance rows for non-null financial fields.

Yahoo vintage metadata treats `available_at_utc` and `provider_observed_at_utc` as provider observation times. SEC vintage metadata uses caller-supplied `available_at_utc`, stores `filed_at_utc` as date precision when recoverable, and leaves `provider_observed_at_utc` null.

The real database currently contains:

| Source provider | Vintage rows | Field provenance rows |
| --- | ---: | ---: |
| `UNKNOWN_LEGACY` | 155331 | 1306388 |
| `sec_edgar` | 194 | 1250 |
| `yahoo` | 0 | 0 |

The legacy baseline preserves current values with a uniform availability timestamp of `2026-06-19T00:00:00Z`. That is a backfill boundary, not first public availability.

## Recoverability Conclusions

The current quarterly table contains only the latest merged state.

The vintage table preserves partial states only for periods written after the vintage flow was enabled, plus a legacy baseline. It does not reconstruct the original historical progression before the baseline.

Field provenance preserves provenance rows by `statement_vintage_id`, but the current real database has no Yahoo field-provenance rows. SEC field rows are filing-bound/source-observed by later ingestion, not exact historical first public availability.

Null-to-value completion and value revision history are recoverable only when separate vintages/provenance rows exist. For the legacy-baselined majority, the original null-to-value timing and value revision sequence has already been lost.

Full point-in-time reconstruction is not possible from current tables. Partial PIT reconstruction is possible for newer SEC-vintage/provenance-backed fields, subject to the precision of stored timestamps.

## Matching Algorithm

The read-only matcher separates three layers:

- Layer A: retrospective fiscal-period association between `ticker + period_end_date` and a completed earnings event.
- Layer B: source availability from Yahoo, SEC, or legacy/fallback sources.
- Layer C: field usability for downstream consumers.

Fiscal matching uses normalized tickers, ordered fiscal periods, ordered completed announcement events, and a configurable maximum delay. It requires the announcement to occur after the fiscal period end, preserves chronological one-to-one ordering, and leaves multi-event or sequence-conflict cases unresolved.

It deliberately does not use nearest absolute date matching.

Default maximum delay is `140` days. The empirical matched-delay distribution from the full audit was:

| Metric | Days |
| --- | ---: |
| min | 1 |
| p50 | 34 |
| p90 | 52 |
| p95 | 59 |
| p99 | 79 |
| max | 137 |

Confidence bands:

- high: `<= 70` days
- medium: `71-100` days
- low: `101-140` days

## Effective Trading Date

Announcement effective trading date is calculated but not persisted:

- `BEFORE_MARKET`: same trading day
- `DURING_MARKET`: same trading day
- `AFTER_MARKET`: next weekday
- `UNKNOWN`: null

Field effective trading date is based on the field's own availability timestamp or availability bound, not the announcement event.

## Full-Universe Audit

Audit artifacts:

- `temp/earnings_event_matching_investigation/20260731T000000Z/audit/full.json`
- `temp/earnings_event_matching_investigation/20260731T000000Z/audit/full.csv`

Aggregate results:

| Metric | Count |
| --- | ---: |
| tickers | 2936 |
| quarterly periods | 155571 |
| earnings events | 135055 |
| matched periods | 125554 |
| high confidence | 122757 |
| medium confidence | 2771 |
| low confidence | 26 |
| unmatched | 24412 |
| ambiguous | 5605 |

Match rate across all quarterly periods was `80.7053%`.

Availability counts:

| Availability status | Count |
| --- | ---: |
| `FIELD_AVAILABILITY_EXACT` | 0 |
| `FIELD_AVAILABILITY_SOURCE_OBSERVED` | 0 |
| `FIELD_AVAILABILITY_FILING_BOUND` | 1250 |
| `FIELD_AVAILABILITY_INFERRED` | 0 |
| unknown or not reconstructable | 1306388 |

No ticker has fully reconstructable field timing across all populated fields because every ticker has legacy-baseline provenance for some fields.

## Representative Tickers

| Ticker | Periods | Events | Matched | Ambiguous | Unmatched | Availability notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| AAPL | 74 | 81 | 72 | 2 | 0 | legacy/non-reconstructable field timing |
| MSFT | 76 | 78 | 76 | 0 | 0 | clean event sequence, legacy timing limits |
| JPM | 73 | 76 | 72 | 1 | 0 | many latest fields lack reconstructable timing |
| XOM | 74 | 79 | 72 | 2 | 0 | legacy timing limits |
| NVDA | 77 | 70 | 63 | 8 | 6 | irregular fiscal calendar and partial Yahoo history |
| GIS | 74 | 78 | 73 | 1 | 0 | SEC filing-bound rows present |
| LMT | 81 | 80 | 73 | 8 | 0 | mixed legacy plus SEC provenance |
| BBY | 84 | 79 | 72 | 12 | 0 | irregular fiscal calendar |
| ARWR | 71 | 54 | 54 | 1 | 16 | capped/partial Yahoo history |
| DGXX | 5 | 9 | 3 | 2 | 0 | short history with extra events |
| AVNS | 53 | 0 | 0 | 0 | 53 | no persisted completed Yahoo events |

## Yahoo Versus SEC Timing

Persisted Yahoo earnings events have `source_observed_at_utc`, but these are announcement-event observation timestamps, not field availability timestamps for quarterly fundamentals.

Current field provenance has no `source_provider='yahoo'` rows, so Yahoo field-level first availability cannot be recovered from `rc_fundamental_quarterly_field_provenance` in this database.

SEC provenance exists for 194 statement vintages and 1250 field rows. SEC raw statement facts store `retrieved_at_utc`; SEC filed dates can be parsed into metadata by code, but the raw statement table does not have dedicated accepted timestamp or accession columns.

No SEC-after-Yahoo or Yahoo-after-SEC field ordering was observed in current provenance because there are no Yahoo field-provenance rows.

## Row And Consumer Readiness

Candidate row states used by the diagnostic layer:

- `ANNOUNCED_NO_FUNDAMENTALS`
- `PARTIAL_FUNDAMENTALS_AVAILABLE`
- `MINIMUM_SCORING_FIELDS_AVAILABLE`
- `MATERIALLY_COMPLETE`
- `SEC_CONFIRMED`
- `LATER_SUPPLEMENTED`

Consumer readiness should be consumer-specific:

- TTM depends on revenue, gross profit, EBIT, EBITDA, free cash flow, cash, total debt, and shares outstanding.
- Scoring depends on TTM inputs and derived margins/growth/leverage/dilution.
- Valuation uses a smaller but overlapping field set.
- Snapshots can be available earlier than scoring if fewer fields are required.

These readiness dates cannot safely be collapsed into `earnings_announcement_date`.

## PIT Assessment

Retrospective quarter-to-event matching is reliable for most periods with Yahoo announcement coverage, but it is not a PIT availability model.

Current data can support:

- retrospective event-to-period analysis for matched periods;
- partial PIT-safe field timing for SEC-vintage/provenance-backed rows;
- consumer-specific readiness for rows whose required fields have trustworthy field timestamps.

Current data cannot support fully PIT-safe historical scoring across the whole universe because the initial public availability of most field values is not reconstructable.

## Missing Historical Evidence

Irretrievably missing or not currently represented for most historical rows:

- first public Yahoo availability per field;
- first public SEC availability per field before the legacy baseline;
- null-to-value completion timing before vintage tracking;
- value revision sequence before vintage tracking;
- exact SEC accepted timestamp in the quarterly/vintage schema;
- accession identifiers in normalized quarterly provenance for legacy rows;
- source replacement sequence between Yahoo and SEC for historical legacy data.

## Recommended Future Schema

Use separate persisted concepts:

- an earnings-event-to-period match table for retrospective semantic association;
- a field availability/provenance table for PIT source timing;
- optional row/consumer readiness materializations derived from field availability.

Do not store every concept in one table. The announcement event and field availability have different semantics and different confidence levels.

Recommended additions:

- `period_end_date` match table with match status, delay, confidence, algorithm version, and ambiguity reason;
- immutable field availability observations with first-seen and latest-seen timestamps;
- source-specific fields for provider observed time, SEC filed date, SEC accepted timestamp, accession/document id, and observation run id;
- explicit source replacement rows that preserve first availability while tracking latest preferred source;
- consumer readiness views or derived tables keyed by consumer/version.

## Recommended Write-Path Changes

Future quarter update and enrichment runs should:

- record first-seen timestamps for every non-null field;
- never overwrite first availability when a later preferred source confirms a value;
- record latest preferred source separately from first available source;
- preserve null-to-value transitions;
- preserve value revisions with statement vintage ids;
- store SEC accession and accepted timestamp when available;
- write Yahoo field provenance when Yahoo supplies or fills a field;
- keep announcement matching separate from field availability.

## Readiness

The repository now has read-only diagnostic code and audit artifacts sufficient to guide a later persisted implementation. The next phase can add durable match and availability tables, but should not treat earnings announcement date as a universal field availability date.
