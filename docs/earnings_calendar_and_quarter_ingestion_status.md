# Earnings Calendar And Quarter Ingestion Status

Date: 2026-08-06

Runtime artifacts:

```text
temp/earnings_calendar_and_ingestion_status/20260806T_apply/
temp/earnings_calendar_and_ingestion_status/20260806T_apply_idempotency/
temp/yahoo_earnings_calendar_reliability/20260806T_live_probe_aapl/
temp/yahoo_earnings_calendar_reliability/20260806T_live_probe_aapl_escalated/
temp/yahoo_earnings_calendar_reliability/20260806T_pilot20_dry_run/
temp/yahoo_earnings_calendar_reliability/20260806T_full_dry_run/
temp/yahoo_earnings_calendar_reliability/20260806T_full_apply/
temp/yahoo_earnings_calendar_reliability/20260806T_idempotency_first20/
```

Backup:

```text
temp/earnings_calendar_and_ingestion_status/20260806T_apply/backups/fundamentals_usa.db.pre_calendar_ingestion_status.bak
temp/yahoo_earnings_calendar_reliability/20260806T_full_apply/backups/fundamentals_usa.db.pre_earnings_calendar.bak
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

## Yahoo Calendar Reliability

The Yahoo calendar refresh is intentionally guarded:

- exactly one of `--dry-run` or `--apply` is required
- per-request timeout is passed to yfinance's underlying `cache_get(...)`
- total attempts per ticker are bounded by `--max-retries`
- per-ticker and per-run elapsed guards can stop a run with checkpoint artifacts
- consecutive failures can stop a run before broad source drift overwrites operational state
- source failures do not call `upsert_earnings_calendar(...)`, so existing calendar rows are preserved
- progress is printed per ticker and checkpoint JSON is rewritten after each ticker
- row and attempt CSV files are written for post-run diagnostics

Installed yfinance at test time was `1.5.2`. `Ticker.get_earnings_dates(...)` did not expose a timeout parameter, so the refresh uses the same Yahoo calendar HTML endpoint through yfinance's data layer with an explicit timeout. yfinance can still perform internal cookie, crumb, consent, and retry requests; the operational guard is therefore total attempts plus elapsed-time checkpointing, not a claim that each ticker can only perform one HTTP request.

Observed reliability runs on 2026-08-06:

- non-escalated AAPL dry-run: failed with DNS/connect error for `guce.yahoo.com`, confirming sandbox network blocking is diagnosed as `NETWORK_ERROR`
- escalated AAPL dry-run: 1/1 source success, future estimate found
- escalated 20-ticker dry-run pilot: 20/20 source success, 20 future estimates, 0 failures
- escalated full-universe dry-run: 2935/2936 source success; one transient `PARSE_ERROR` for `SRPT`; no timeouts, rate limits, or network errors
- escalated full-universe apply: 2936/2936 source success; 2936 inserted calendar rows; 0 source/parse/timeout/rate-limit/network failures
- 20-ticker apply idempotency smoke: 20/20 unchanged; 0 inserts and 0 updates

Post-apply `rc_earnings_calendar` status distribution:

```text
COMPLETED_EVENT_FOUND  2806
DUE_TODAY              4
NO_CURRENT_ESTIMATE    106
UPCOMING               20
```

Post-apply `PRAGMA quick_check` returned `ok`.

## Current Limits

Yahoo calendar rows are current observations from Yahoo Finance, not guaranteed future truth. Future estimate dates can move, and completed event reconciliation still belongs to `rc_earnings_event`.

Scheduler activation and broad automatic quarter fetching remain separate decisions. This phase only hardens and runs the Yahoo earnings-calendar refresh.
