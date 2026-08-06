# Fundamental Quarter Completeness And Ingestion Design

Audit date: 2026-08-06

Runtime artifacts:

```text
temp/fundamental_quarter_completeness_audit/20260806T_full/
```

This phase was read-only for `fundamentals_usa.db`. It added reusable audit code, a CLI, tests, and this design note, but did not create production tables, run migrations, run schedulers, fetch Yahoo/SEC data, or update current TTM, score, valuation, percentile, snapshot, state, report, or UI behavior.

## Actual Schema

`rc_fundamental_quarterly` currently has primary key `(ticker, period_end_date)` and these fields:

| Group | Fields |
| --- | --- |
| `IDENTITY_AND_PERIOD` | `ticker`, `period_end_date` |
| `INCOME_STATEMENT_CORE` | `revenue`, `gross_profit`, `operating_income`, `ebit`, `ebitda`, `net_income` |
| `CASH_FLOW_CORE` | `operating_cashflow`, `capex`, `free_cashflow` |
| `BALANCE_SHEET_CORE` | `cash`, `total_debt` |
| `SHARE_AND_EPS_CORE` | `shares_outstanding` |
| `DERIVED_OR_OPTIONAL` | none in the current quarterly table |
| `SOURCE_OR_OPERATIONAL_METADATA` | `currency`, `run_id` |

The table is narrow and latest-state oriented. It has no market column, no persisted source-observed timestamp, and no retained per-row source response. Duplicate prevention is the natural primary key. Latest writes use `INSERT OR REPLACE`.

Current observed table counts:

| Table | Rows |
| --- | ---: |
| `rc_fundamental_quarterly` | 156030 |
| `rc_fundamental_ttm` | 147124 |
| `rc_earnings_event` | 135055 |
| `rc_fundamental_quarter_earnings_match` | 125554 |
| `rc_fundamental_valuation` | 44030 |

Vintage/provenance tables exist and contain old rows, but `VINTAGE_PROVENANCE_WRITES_ENABLED = False`; future design must not reactivate those writes.

## Consumer Requirements

### TTM

`swingmaster/fundamentals/build_ttm.py` reads:

| Field | Required or optional | Null and zero behavior | Effect when missing |
| --- | --- | --- | --- |
| `revenue` | core | summed over non-null quarters; zero blocks ratios/growth denominator | no revenue TTM if all four are null; margins and growth may be null |
| `ebit` | core | summed over non-null quarters | no EBIT TTM if all four are null; margin/valuation may be unavailable |
| `free_cashflow` | core | summed over non-null quarters | FCF TTM and FCF margins/trends may be null |
| `ebitda` | optional | summed over non-null quarters | leverage denominator falls back to EBIT |
| `gross_profit` | optional | summed over non-null quarters | gross margin trend may be null |
| `cash` | optional for TTM row | current-quarter value only | net debt and leverage may be null |
| `total_debt` | optional for TTM row | current-quarter value only | net debt and leverage may be null |
| `shares_outstanding` | optional for TTM row | current and prior-year values; previous zero invalid | dilution may be null |

The builder requires at least four quarterly rows per ticker, but individual metrics tolerate missing fields.

### Score

`swingmaster/fundamentals/score.py` reads TTM metrics:

`revenue_growth_ttm_yoy`, `ebit_margin_ttm`, `ebit_margin_trend_4q`, `fcf_margin_ttm`, `net_debt_to_ebitda`, `share_dilution_yoy`, `lifecycle_class`, and recent history for the consistency component.

Missing factors are tolerated:

| Missing TTM field | Score behavior |
| --- | --- |
| `revenue_growth_ttm_yoy` | default growth component `6` |
| `ebit_margin_ttm` | margin component `0` |
| `ebit_margin_trend_4q` | default trend component `6` |
| `fcf_margin_ttm` | FCF component `0` |
| `net_debt_to_ebitda` | default leverage component `8` |
| `share_dilution_yoy` | default dilution component `5` |
| `lifecycle_class` | lifecycle component `0` |
| insufficient consistency history | consistency component `0` |

There is no minimum factor-count gate in current code. The practical score-readiness rule is therefore not "all factors present"; it is that the quarter can support revenue, EBIT, and FCF based TTM inputs, while recognizing that missing optional factors lower score quality rather than preventing a score.

### Valuation

Current valuation is in `swingmaster/cli/run_fundamental_valuation.py`; historical valuation reuses the same row builder.

| Metric | Required inputs | Denominator rules | Core or optional | Missing behavior |
| --- | --- | --- | --- | --- |
| `market_cap` | close price, `shares_outstanding` | shares must be positive | core | status `MISSING_PRICE` or `MISSING_SHARES` |
| `enterprise_value` | market cap, `total_debt`, `cash` | debt/cash are assumed zero if missing | core for EV metrics | available when market cap is available |
| `valuation_ev_ebit` | enterprise value, `ebit_ttm` | EBIT must be positive | core | status `INVALID_EBIT` |
| `valuation_fcf_yield` | market cap, `fcf_ttm` | market cap must be positive | core but independently nullable | status `MISSING_FCF` when FCF is absent |
| `valuation_ebit_margin` | `ebit_margin_ttm` | none | core for bucket | status `MISSING_EBIT_MARGIN` |
| valuation freshness | valuation date, fundamental as-of date | stale over 240 days invalid | core | status `TOO_STALE_FUNDAMENTALS` |

Valuation readiness should be independent. A quarter can be research-useful and score-ready while valuation is unavailable because shares or price context is missing.

### Snapshot, Percentile, Research

Historical snapshot logic requires an available effective-dated TTM row as the base. Score, percentiles, quarterly context, and valuation are attached when available; missing score makes snapshots partial, while valuation and percentiles are optional context.

## Lightweight Completeness Policy

Policy version: `fundamental_quarter_completeness_v1`

`BASIC_COMPLETE`:

- valid ticker and `period_end_date`;
- non-empty financial row;
- `revenue` present;
- at least one profitability field from `gross_profit`, `operating_income`, `ebit`, `net_income`;
- `free_cashflow` present;
- positive `shares_outstanding`.

`BASIC_PARTIAL`:

- valid period and non-empty financial row;
- enough information for partial research, either revenue/EBIT/FCF TTM contribution or at least four meaningful core financial fields;
- at least one important consumer may still be missing inputs.

`BASIC_INCOMPLETE`:

- valid period and some financial information exists;
- too few core values for normal TTM/score usefulness.

`EMPTY_OR_PLACEHOLDER`:

- no meaningful non-zero financial values.

`NOT_ASSESSABLE`:

- malformed identity or period prevents assessment.

Consumer readiness is separate:

- `ttm_ready`: revenue, EBIT, and FCF are present for the row-level contribution.
- `score_input_ready`: same practical row-level gate as TTM readiness; the score formula itself tolerates missing factors.
- `valuation_input_ready`: positive `shares_outstanding`; price and TTM freshness are outside the quarter row.
- `historical_research_ready`: valid, non-empty row with any useful financial information.

## Full Database Results

| Category | Count | Percent |
| --- | ---: | ---: |
| total quarter rows | 156030 | 100.0000 |
| distinct tickers | 2936 | n/a |
| `BASIC_COMPLETE` | 81340 | 52.1310 |
| `BASIC_PARTIAL` | 67501 | 43.2616 |
| `BASIC_INCOMPLETE` | 5846 | 3.7467 |
| `EMPTY_OR_PLACEHOLDER` | 1343 | 0.8607 |
| `NOT_ASSESSABLE` | 0 | 0.0000 |
| `ttm_ready` | 79454 | 50.9223 |
| `score_input_ready` | 79454 | 50.9223 |
| `valuation_input_ready` | 131357 | 84.1870 |

Retry recommendations:

| Recommendation | Count | Rationale |
| --- | ---: | --- |
| `NO_ACTION` | 81340 | basic-complete rows |
| `RETRY_YAHOO` | 14299 | local evidence suggests missing income-side values, often after SEC-flavored runs |
| `RETRY_SEC` | 30382 | missing balance/cash-flow values likely worth SEC enrichment |
| `RETRY_YAHOO_AND_SEC` | 16953 | broad missing income, cash-flow, and balance fields |
| `MANUAL_REVIEW` | 218 | malformed or ambiguous local evidence |
| `NOT_RETRYABLE` | 12838 | mostly old unmatched sparse history where operational value is low |

## Missing Field Findings

| Field | Non-null | Null | Zero | Missing percent |
| --- | ---: | ---: | ---: | ---: |
| `revenue` | 108842 | 47188 | 2898 | 30.2429 |
| `gross_profit` | 69939 | 86091 | 300 | 55.1759 |
| `operating_income` | 123386 | 32644 | 78 | 20.9216 |
| `ebit` | 121817 | 34213 | 77 | 21.9272 |
| `ebitda` | 0 | 156030 | 0 | 100.0000 |
| `net_income` | 128980 | 27050 | 392 | 17.3364 |
| `operating_cashflow` | 150563 | 5467 | 173 | 3.5038 |
| `capex` | 127404 | 28626 | 2462 | 18.3465 |
| `free_cashflow` | 128514 | 27516 | 61 | 17.6351 |
| `cash` | 144972 | 11058 | 550 | 7.0871 |
| `total_debt` | 74978 | 81052 | 1975 | 51.9464 |
| `shares_outstanding` | 131484 | 24546 | 126 | 15.7316 |
| `currency` | 0 | 156030 | 0 | 100.0000 |

Structurally sparse fields that should not be part of `BASIC_COMPLETE`: `ebitda`, `currency`, `gross_profit`, and `total_debt`.

Most frequent missing combinations:

| Rows | Missing fields |
| ---: | --- |
| 25594 | `ebitda` only |
| 12626 | `ebitda`, `total_debt` |
| 11541 | `gross_profit`, `ebitda` |
| 10026 | `gross_profit`, `ebitda`, `total_debt` |
| 6510 | `revenue`, `ebitda` |

Recent-year availability improved materially in 2024 and 2025. 2026 is dominated by newly published or not-yet-fully-populated periods: 963 complete, 2565 partial, and 251 incomplete rows in the current database.

## Ticker-Level Findings

| Classification | Tickers |
| --- | ---: |
| `ALL_HISTORY_USABLE` | 157 |
| `RECENT_HISTORY_USABLE_OLD_GAPS` | 2536 |
| `LATEST_QUARTER_INCOMPLETE` | 228 |
| `MULTIPLE_RECENT_INCOMPLETE` | 2 |
| `SPARSE_HISTORY` | 0 |
| `NO_USABLE_HISTORY` | 0 |
| `MANUAL_REVIEW` | 13 |

Age-aware issue counts:

- latest quarter incomplete tickers: 228;
- last-four-quarters incomplete tickers: 243;
- older-only incomplete tickers: 2536.

The dominant pattern is acceptable recent history with old sparse gaps. The next operational phase should focus on the latest quarter and last-four-quarter issue sets, not exhaustive old-history cleanup.

## Earnings Event Relationship

- matched earnings events with no quarterly row: 0;
- quarterly rows with an earnings match but incomplete data: 2053;
- latest reported earnings event without a quarter match: 759;
- median announcement-to-effective-day delay where reconstructable: 0 days.

Do not treat an estimated earnings date as publication. Publication should be detected only once a completed earnings event with reported EPS appears.

## Future Earnings Calendar Table

Proposed table: `rc_earnings_calendar`

```sql
CREATE TABLE rc_earnings_calendar (
    id INTEGER PRIMARY KEY,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    estimated_announcement_at TEXT,
    estimated_announcement_date TEXT,
    estimated_session TEXT NOT NULL DEFAULT 'UNKNOWN',
    calendar_status TEXT NOT NULL,
    source TEXT NOT NULL,
    source_observed_at_utc TEXT NOT NULL,
    first_observed_at_utc TEXT NOT NULL,
    last_observed_at_utc TEXT NOT NULL,
    previous_estimated_announcement_at TEXT,
    date_change_count INTEGER NOT NULL DEFAULT 0,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (market, ticker, source)
);
```

Statuses:

- `UPCOMING`;
- `DUE_TODAY`;
- `DATE_PASSED_EVENT_NOT_FOUND`;
- `COMPLETED_EVENT_FOUND`;
- `DATE_CHANGED`;
- `NO_CURRENT_ESTIMATE`.

Retain one next-event estimate per `(market, ticker, source)` initially. If Yahoo reliably returns multiple future rows later, add a sequence/date natural key. Upsert updates `last_observed_at_utc`, preserves `first_observed_at_utc`, stores prior estimate in `previous_estimated_announcement_at`, and increments `date_change_count` when the estimated datetime changes.

## Future Quarter Ingestion Status Table

Proposed table: `rc_fundamental_quarter_ingestion_status`

```sql
CREATE TABLE rc_fundamental_quarter_ingestion_status (
    id INTEGER PRIMARY KEY,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    period_end_date TEXT NOT NULL,
    earnings_event_id INTEGER,
    announcement_date TEXT,
    effective_trading_date TEXT,
    ingestion_status TEXT NOT NULL,
    basic_status TEXT NOT NULL,
    ttm_ready INTEGER NOT NULL,
    score_input_ready INTEGER NOT NULL,
    valuation_input_ready INTEGER NOT NULL,
    supported_source_field_count INTEGER,
    source_non_null_field_count INTEGER,
    persisted_matching_field_count INTEGER,
    missing_core_fields TEXT NOT NULL,
    last_fetch_status TEXT,
    last_fetch_source TEXT,
    last_source_observed_at_utc TEXT,
    last_checked_at_utc TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (market, ticker, period_end_date)
);
```

Operational statuses:

- `NOT_PUBLISHED`;
- `PUBLISHED_DATA_NOT_FETCHED`;
- `FUNDAMENTALS_PARTIAL`;
- `BASIC_COMPLETE`;
- `INGEST_COMPLETE`;
- `FETCH_FAILED`;
- `NOT_ASSESSABLE`;
- `UNKNOWN_HISTORICAL_INGEST_COMPLETENESS`.

`INGEST_COMPLETE` means: for the latest successful source request, all non-null supported values returned by that request were normalized and persisted correctly. It does not mean every possible field exists, the quarter is permanently final, Yahoo/SEC will never add fields, or every optional consumer metric is available.

Historical rows should not be assigned `INGEST_COMPLETE` unless a preserved source response exists for comparison. Use basic status and `UNKNOWN_HISTORICAL_INGEST_COMPLETENESS` instead.

## Status Transitions

Calendar refresh owns:

```text
UPCOMING -> DUE_TODAY
UPCOMING -> DATE_CHANGED
UPCOMING/DUE_TODAY -> DATE_PASSED_EVENT_NOT_FOUND
any open status -> NO_CURRENT_ESTIMATE
```

Completed earnings-event refresh owns:

```text
DUE_TODAY/DATE_PASSED_EVENT_NOT_FOUND/NO_CURRENT_ESTIMATE -> COMPLETED_EVENT_FOUND
```

Quarter update owns:

```text
COMPLETED_EVENT_FOUND -> PUBLISHED_DATA_NOT_FETCHED
PUBLISHED_DATA_NOT_FETCHED -> FUNDAMENTALS_PARTIAL
PUBLISHED_DATA_NOT_FETCHED -> FETCH_FAILED
```

Completeness assessment owns:

```text
FUNDAMENTALS_PARTIAL -> BASIC_COMPLETE
any fetched row -> NOT_ASSESSABLE
```

Source-to-persistence comparison owns:

```text
BASIC_COMPLETE/FUNDAMENTALS_PARTIAL -> INGEST_COMPLETE
```

## Integration Boundaries

The next implementation should be additive:

1. Add migrations for the two new status tables only.
2. Extend Yahoo earnings-date refresh to persist the next future estimate without changing completed-event backfill behavior.
3. Extend completed earnings-event refresh to mark publication only when reported EPS appears.
4. After normal quarter update, run the read-only completeness assessor and upsert status.
5. Add source-to-persistence comparison for the latest successful request before assigning `INGEST_COMPLETE`.
6. Add scheduler/UI diagnostics that surface latest-quarter and last-four-quarter issues without blocking unrelated research workflows.

Do not reactivate vintage/provenance, do not make incomplete optional data block normal screens, and do not use ingestion timestamps as market availability dates.

