# Fundamentals V3 Phase 2B Yahoo Bootstrap Field And Metadata Contract

Date: 2026-08-21

Classification: `FUNDAMENTALS_V3_PHASE2B_YAHOO_FIELD_METADATA_CONTRACT_LOCKED`

## Scope

Phase 2B locks the Yahoo-first bootstrap contract before implementing the V3 raw-cache adapter or
bootstrap runner.

No provider calls, bootstrap execution, canonical V3 population, Legacy/V2 writes, Check/Update
cutover, scheduler changes, or RawCandle changes are performed by this phase.

## Source Roles

The V3 bootstrap source roles are:

| Source | Role |
| --- | --- |
| Yahoo quarterly statements | Primary bootstrap source for quarterly fundamental values. |
| Legacy | Initial company universe authority, publication-date/availability enrichment where reliable, later deep-history extension and reconciliation source. |
| V2 | Fiscal identity enrichment, profile exclusion evidence, later deep-history extension and reconciliation source. |
| Existing result-event metadata | Publication/result-date enrichment where reliably matched. |

Yahoo-first is seed order and value bootstrap priority. It is not canonical authority over fiscal
identity or publication dates.

Locked Phase 2B architecture:

```text
APPROVED V3 COMPANY
        ↓
EXISTING THROTTLED YAHOO FETCHER
        ↓
EXTERNAL V3 RAW CACHE
        ↓
NORMALIZED YAHOO QUARTER ROW
        ↓
METADATA ENRICHMENT
        ├── fiscal_year / fiscal_quarter
        └── publish_date
        ↓
V3 MIGRATION CANDIDATE
        ↓
later canonical V3 application
```

## Canonical Field Storage

Yahoo-normalized values map to current `v3_quarter_fundamentals` without requiring a canonical V3
schema expansion:

| Yahoo/V3 value | V3 storage | Contract |
| --- | --- | --- |
| revenue | `v3_quarter_fundamentals.revenue` | Accepted when period identity is resolved. |
| gross profit | `gross_profit` | Optional enrichment. |
| EBIT | `ebit` | Store only direct/provider EBIT evidence; do not fill from operating income. |
| EBITDA | `ebitda` | Accepted direct Yahoo EBITDA may be staged as provider value; final canonical acceptance still follows V3 reconciliation policy. |
| net income | `net_income` | Optional enrichment. |
| operating cash flow | `operating_cashflow` | Supporting FCF derivation/audit field. |
| capex | `capex` | Keep Yahoo negative-capex convention. |
| free cash flow | `free_cashflow` | Direct Yahoo FCF or approved `operating_cashflow + capex` derivation. |
| cash | `cash` | Core field. |
| total debt | `total_debt` | Core field; later reconciliation may compare component-derived debt. |
| shares outstanding | `shares_outstanding` | Core field; snapshot fallback remains review-quality evidence. |

Fields intentionally not added to canonical `v3_quarter_fundamentals` in this phase:

- Yahoo operating income
- depreciation/amortization
- short-term debt
- long-term debt
- Yahoo field labels/source aliases per field
- provider HTTP/request metadata

Those details remain in the external V3 raw cache and provider-specific normalization artifacts until
a later consumer proves they must be promoted to canonical storage.

## Metadata Enrichment Contract

Yahoo yfinance quarterly statements provide `PERIOD_END_ONLY` evidence. They do not provide direct
`fiscal_year + fiscal_quarter` labels or reliable historical publication dates.

V3 quarter metadata rules:

| V3 metadata field | Source rule |
| --- | --- |
| `fiscal_year` / `fiscal_quarter` | Resolve from V2, Legacy/provider observation content, validated result-event matching, or later approved fiscal identity recovery. Do not infer from calendar quarter alone. |
| `period_end_date` | May use Yahoo statement column date as period-end evidence after the target fiscal identity is resolved. |
| `publish_date` | Must come from Legacy/V2/result-event metadata or another reliable publication-date source, not Yahoo statement column date. |
| `market_availability_date` | Follows accepted canonical publication-date availability invariant after publication date is known. |
| `provider observation/local fetch time` | Store in external raw cache metadata, not as `publish_date`. |

If a Yahoo value row cannot be matched to canonical fiscal identity, it remains raw/staged evidence
and must not create a canonical `v3_quarter` by calendar-period substitution.

## Raw Cache Schema Adjustment

Phase 1 already kept raw payloads outside the canonical V3 DB. Phase 2A showed the V3 raw-cache
interface must preserve fetch status and error metadata because Yahoo fetches can return `OK`,
`EMPTY`, or `ERROR` without canonical writes.

Minimal external raw-cache adjustment:

```text
v3_raw_cache_entry.status        TEXT NOT NULL DEFAULT 'OK' CHECK ('OK','EMPTY','ERROR')
v3_raw_cache_entry.error_message TEXT
```

No canonical V3 table expansion is required for Phase 2B.

## Phase 2C Adapter Requirements

The future adapter should:

1. use the existing `YahooFinanceClient.get_raw_payload(symbol)` fetcher
2. preserve raw payload JSON before normalization
3. write only to `rc_fundamentals_v3_raw.db`
4. store provider, provider symbol, fetch run id, payload hash, payload JSON, status, error message,
   observed/fetched timestamp, and created timestamp
5. leave canonical V3 tables untouched
6. expose normalized provider rows separately for later identity/reconciliation logic

The adapter must not:

- implement a new downloader
- infer fiscal identity from calendar quarter
- map Yahoo statement dates to `publish_date`
- write `rc_fundamentals_v3.db`
- call Legacy/V2 write paths
- weaken existing throttling

## Bootstrap Acceptance Gate

Yahoo quarterly values may become canonical V3 values only after:

1. company is admitted by the Legacy-authority V3 universe
2. Yahoo symbol maps to the admitted company
3. fiscal identity is resolved from approved enrichment evidence
4. period-end evidence is consistent with that fiscal identity
5. field semantics pass V3 normalizer rules
6. NULL-preserving write policy permits the value
7. non-null conflicts are routed to `v3_resolution_issue`

This preserves the architecture:

```text
Yahoo values first
Legacy/V2/result metadata for identity and publication enrichment
Legacy/V2 later for deep-history extension and reconciliation
```
