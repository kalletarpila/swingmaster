# Fundamentals V3 Phase 2D Post-B Price Activity And Delisting Triage

Date: 2026-08-22

Classification: `FUNDAMENTALS_V3_PRICE_ACTIVITY_TRIAGE_COMPLETE_READY_FOR_PHASE3`

## Scope

This phase classifies the approved V3 company universe by local price activity before Phase 3
canonical migration. It is read-only: no `osakedata`, Legacy, V2, raw-cache, canonical V3,
RawCandle, Check, or Update data is written.

Artifact root:

```text
temp/fundamentals_v3_phase2d_post_activity_triage/20260822T_POST_B_PRICE_ACTIVITY_TRIAGE/
```

## Osakedata Source

The authoritative local USA OHLCV source is RawCandle's osakedata database:

| Property | Value |
| --- | --- |
| DB path | `/home/kalle/projects/rawcandle/data/osakedata.db` |
| Table | `osakedata` |
| Ticker column | `osake` |
| Date column | `pvm` |
| OHLCV columns | `open`, `high`, `low`, `close`, `volume` |
| Market column | `market` |
| Date format | `TEXT YYYY-MM-DD` |

Relevant indexes observed:

```text
idx_osakedata_market_ticker_date
idx_osakedata_mkt_sec_ticker_date
idx_osakedata_mkt_ticker_date
idx_osake_pvm
```

The raw USA `MAX(pvm)` is `2026-08-22`, but that row is a singleton `BTC-USD` weekend observation.
It is not a broad USA equity trading session and is excluded from the active/inactive clock.

## Trading Calendar

The observed broad USA equity trading calendar is derived from `osakedata` dates with valid USA
close observations and broad market participation.

| Offset | Date | Rule |
| --- | --- | --- |
| T0 | 2026-08-21 | Active |
| T-1 | 2026-08-20 | Active |
| T-2 | 2026-08-19 | Active |
| T-3 | 2026-08-18 | Active |
| T-4 | 2026-08-17 | Active cutoff |
| T-5 | 2026-08-14 | Delisted/inactive boundary |

The rule uses observed trading-session positions only. It does not use calendar-day age.

## Activity Counts

| Metric | Count |
| --- | ---: |
| Legacy source tickers | 2,936 |
| Approved V3 universe | 2,812 |
| Legacy-only approved | 361 |
| V2-covered approved | 2,451 |
| BANK exclusions | 82 |
| INSURANCE exclusions | 42 |
| ACTIVE | 2,735 |
| DELISTED_OR_INACTIVE | 77 |
| NO_PRICE_HISTORY | 0 |

Reconciliation:

```text
2,735 + 77 + 0 = 2,812
```

The previous provisional active operational universe was 2,812. The evidence-based Phase 3
operational active baseline is 2,735.

## Yahoo Crosscheck

| Yahoo/activity combination | Count |
| --- | ---: |
| Yahoo EMPTY total | 58 |
| Yahoo EMPTY + ACTIVE | 2 |
| Yahoo EMPTY + DELISTED_OR_INACTIVE | 56 |
| Yahoo EMPTY + NO_PRICE_HISTORY | 0 |
| Yahoo OK + DELISTED_OR_INACTIVE | 21 |

`ACTIVE + Yahoo EMPTY` is not a delisting signal under the chosen rule. Those tickers remain active
for Phase 3 unless another positive exclusion reason is introduced later.

`DELISTED_OR_INACTIVE + Yahoo EMPTY` likely explains most Yahoo no-data results, but historical
membership and historical fundamentals remain retained.

## Source-Class Split

| Population | ACTIVE | DELISTED_OR_INACTIVE | NO_PRICE_HISTORY |
| --- | ---: | ---: | ---: |
| Legacy-only approved | 348 | 13 | 0 |
| V2-covered approved | 2,387 | 64 | 0 |

## Phase 3 Exceptions

The known fiscal reconciliation exceptions are all operationally active by price activity:

| Ticker | Activity | Last price date | Phase 3 exception |
| --- | --- | --- | --- |
| CAVA | ACTIVE | 2026-08-21 | Duplicate/split fiscal work-unit; merge with field provenance or manual review. |
| LFCR | ACTIVE | 2026-08-21 | Provider-period variant; exclude unless later official FY2025 Q4 values prove equivalence. |
| NEUP | ACTIVE | 2026-08-21 | Fiscal-quarter mapping correction; map 2026-03-31 to FY2026 Q3 and 2025-09-30 to FY2026 Q1. |

## Phase 3 Baseline

Recommended Phase 3 `v3_company.active` mapping:

| Activity classification | `v3_company.active` |
| --- | ---: |
| ACTIVE | 1 |
| DELISTED_OR_INACTIVE | 0 |
| NO_PRICE_HISTORY | 0 unless manual local ticker remap is explicitly approved |

Historical membership is unchanged. Historical candidate rows and historical fundamentals are not
removed.

Future V3 Check/Update should normally scope routine maintenance to `v3_company.active = 1`.
Companies with `active = 0` should be processed only for explicit historical remediation.

## Validation

Validated:

- all 2,812 approved companies classified exactly once
- observed trading sessions used instead of calendar-day age
- five-session boundary tested: T0..T-4 active, T-5 inactive
- weekend/holiday gap behavior tested with irregular observed sessions
- all 58 Yahoo EMPTY companies cross-classified
- Legacy-only and V2-covered populations reconciled
- no production writes
- no provider or network calls
