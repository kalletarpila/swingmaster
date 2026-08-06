# Earnings Calendar And Quarter Ingestion Status

Date: 2026-08-06

Runtime artifacts:

```text
temp/earnings_calendar_and_ingestion_status/20260806T_apply/
temp/earnings_calendar_and_ingestion_status/20260806T_apply_idempotency/
```

Backup:

```text
temp/earnings_calendar_and_ingestion_status/20260806T_apply/backups/fundamentals_usa.db.pre_calendar_ingestion_status.bak
```

## Readiness Policy

The canonical persisted readiness fields are separate:

- `quarter_basic_complete`
- `ttm_input_complete`
- `score_history_complete`
- `valuation_input_ready`
- `historical_research_ready`

`quarter_basic_complete` requires a valid `period_end_date`, `revenue`, `ebit`, direct `free_cashflow` or both `operating_cashflow` and `capex`, `cash`, `total_debt`, and `shares_outstanding`.

It does not require `ebitda`, `currency`, `gross_profit`, `operating_income`, or `net_income`.

`ttm_input_complete` requires the actual four ordered quarterly component rows for the TTM row and each component row must be `quarter_basic_complete`.

`score_history_complete` requires the current four ordered component rows, the previous four ordered component rows, and a matching TTM row where the current score-history metrics are non-null:

```text
revenue_growth_ttm_yoy
ebit_margin_ttm
ebit_margin_trend_4q
fcf_margin_ttm
fcf_margin_trend_4q
net_debt_to_ebit
share_dilution_yoy
```

The inspected TTM builder uses ticker-sorted row order; it does not enforce calendar-quarter continuity. Completeness is metadata and does not block existing TTM or score production.

## Schemas

Added migrations:

- `034_rc_fundamental_quarter_ingestion_status.sql`
- `035_rc_earnings_calendar.sql`

`rc_fundamental_quarter_ingestion_status` stores one current status row per `(market, ticker, period_end_date)` with `basic_status`, `ingestion_status`, the five readiness fields, missing-field diagnostics, earnings-match metadata, source-comparison counters, and evidence metadata.

`rc_earnings_calendar` stores one current Yahoo calendar row per `(market, ticker, source)` with estimate date/time, New York-local status, date-change tracking, and completed-event reconciliation.

## Historical Backfill

Historical backfill writes:

```text
ingestion_status = UNKNOWN_HISTORICAL_INGEST_COMPLETENESS
ingestion_evidence_type = CURRENT_DB_STATE_ONLY
```

It never assigns historical `INGEST_COMPLETE`, because same-run source-response evidence is not available for old rows.

Backfill result for `fundamentals_usa.db`:

- status rows: `156030`
- `quarter_basic_complete`: `43989`
- `ttm_input_complete`: `31792`
- `score_history_complete`: `22798`
- `valuation_input_ready`: `131357`
- `historical_research_ready`: `154312`
- duplicate status keys: `0`
- invalid ingestion statuses: `0`
- historical `INGEST_COMPLETE` without evidence: `0`
- `PRAGMA quick_check`: `ok`

## Calendar Policy

Calendar status uses New York-local date semantics:

- future estimate: `UPCOMING`
- estimate date equals New York current date: `DUE_TODAY`
- estimate date passed without completed event: `DATE_PASSED_EVENT_NOT_FOUND`
- completed reported event with `reported_eps IS NOT NULL`: `COMPLETED_EVENT_FOUND`
- no future estimate: `NO_CURRENT_ESTIMATE`

An estimate is not publication evidence. Completed earnings events remain owned by the completed-event flow.

## INGEST_COMPLETE Evidence

`INGEST_COMPLETE` is valid only with:

```text
last_fetch_status = SUCCESS
last_fetch_source populated
last_source_observed_at_utc populated
ingestion_evidence_type = SAME_RUN_SOURCE_TO_PERSISTENCE_COMPARISON
persisted_matching_field_count = source_non_null_field_count
```

The historical backfill does not meet this contract.

## Current Limits

Yahoo live pilot was attempted as a 20-ticker dry-run but was interrupted after it did not complete promptly in this environment. No calendar rows were written; `rc_earnings_calendar` row count remained `0`.

Scheduler activation and broad automatic quarter fetching remain blocked until a successful Yahoo pilot and explicit approval for full-universe egress.
