# Fundamentals V3 Phase 3C-2 - Legacy Deep-History Extension

Status:

`FUNDAMENTALS_V3_PHASE3C_2_LEGACY_DEEP_HISTORY_COMPLETE`

Production run ID:

`V3_PHASE3C2_LEGACY_DEEP_HISTORY_2026-08-23T074231Z`

Artifact root:

`temp/fundamentals_v3_phase3c_2_legacy_deep_history/20260823T_PHASE3C_2_LEGACY_DEEP_HISTORY/`

Source-boundary backup:

`temp/fundamentals_v3_phase3c_2_legacy_deep_history/20260823T_PHASE3C_2_LEGACY_DEEP_HISTORY/rc_fundamentals_v3_pre_phase3c2_backup.db`

## Scope

Phase 3C-2 imported the deterministic Phase 3C-1D READY population into canonical V3 using the
Phase 3C-1E SEC Q4 field policy. It did not rediscover identity, import HOLD rows, create pre-2018
quarters, write V2 contribution, change company universe/activity, call providers, or force
EBIT/EBITDA derivation.

Permanent historical floor:

`V3_HISTORICAL_PERIOD_END_FLOOR = 2018-01-01`

## Baseline

Before production apply:

| metric | count |
| --- | ---: |
| companies | 2552 |
| active | 2484 |
| inactive | 68 |
| canonical Q | 13017 |
| core-ready | 11926 |
| core-not-ready | 1091 |
| publish known | 11905 |
| publish NULL | 1112 |

Preflight integrity:

- `quick_check = ok`
- `foreign_key_check_rows = 0`

## READY Plan

Phase 3C-2 consumed the Phase 3C-1D plan directly.

| population | count |
| --- | ---: |
| READY candidates | 63135 |
| explicit Legacy Qs | 48502 |
| reconstructed SEC Q4s | 14633 |
| HOLD excluded | 4342 |
| pre-2018 candidates | 0 |
| duplicate canonical identities in plan | 0 |

## Dry Apply Gate

Dry apply gate passed.

| check | result |
| --- | ---: |
| HOLD leakage | 0 |
| pre-2018 leakage | 0 |
| identity duplicates | 0 |
| instant subtraction detected | 0 |
| unsafe Q4 EBITDA values | 0 |
| company universe changed | 0 |
| non-null overwrites | 0 |
| reported non-null conflicts without overwrite | 24766 |

The non-null conflicts are existing canonical values protected by the no-overwrite policy. They were
reported as conflicts/resolution evidence, not applied as replacements.

## Production Apply

| metric | count |
| --- | ---: |
| candidates examined | 63135 |
| candidates accepted | 63135 |
| canonical Qs created | 59481 |
| existing canonical Qs matched | 3654 |
| rejected/skipped candidates | 0 |
| resolution-required candidate rows | 3630 |

The created-Q count is below the READY count because `3654` READY candidates matched existing
canonical Q identities. Existing non-null values were preserved.

## Field Contribution

Planned source-field availability split:

| field | explicit Legacy | SEC Q4 |
| --- | ---: | ---: |
| revenue | 42243 | 12514 |
| gross_profit | 24495 | 7263 |
| operating_income | 43687 | 12765 |
| EBIT | 43482 | 0 |
| EBITDA | 1 | 0 |
| net_income | 40599 | 12987 |
| OCF | 48211 | 14079 |
| capex | 41434 | 12309 |
| FCF | 41463 | 12293 |
| cash | 45860 | 13718 |
| total_debt | 24831 | 8292 |
| shares_outstanding | 43428 | 12966 |

Actual production apply field activity includes fills/new inserts plus conflict evidence against
existing canonical values. Existing non-null canonical values were not overwritten.

## SEC Q4 Policy

SEC Q4 field contribution:

| field | populated | left NULL | mode |
| --- | ---: | ---: | --- |
| revenue | 12514 | 2119 | FY_MINUS_Q1_Q2_Q3 |
| gross_profit | 7263 | 7370 | FY_MINUS_Q1_Q2_Q3 |
| operating_income | 12765 | 1868 | FY_MINUS_Q1_Q2_Q3 |
| EBIT | 0 | 14633 | UNSAFE_LEAVE_NULL |
| EBITDA | 0 | 14633 | UNSAFE_LEAVE_NULL |
| net_income | 12987 | 1646 | FY_MINUS_Q1_Q2_Q3 |
| OCF | 14079 | 554 | FY_MINUS_9M |
| capex | 12309 | 2324 | FY_MINUS_9M |
| FCF | 12293 | 2340 | APPROVED_DERIVATION |
| cash | 13718 | 915 | DIRECT_FY_END_INSTANT |
| total_debt | 8292 | 6341 | DIRECT_FY_END_INSTANT |
| shares_outstanding | 12966 | 1667 | DIRECT_FY_END_INSTANT |
| publish_date | 14633 | 0 | ANNUAL_RESULT_OR_FILING_DATE |

Source-mode totals across SEC Q4 fields:

| mode | values |
| --- | ---: |
| FY_MINUS_Q1_Q2_Q3 | 45529 |
| FY_MINUS_9M | 26388 |
| DIRECT_FY_END_INSTANT | 34976 |
| APPROVED_DERIVATION | 12293 |
| NULL by policy/safety | 56410 |

Q4 EBIT and EBITDA remain NULL by design. Phase 4C is the dedicated derivation research phase.

## Publication Contribution

Production metadata:

| metadata outcome | count |
| --- | ---: |
| PERIOD_DATE_SET | 59481 |
| PERIOD_DATE_CONFLICT | 3654 |
| PUBLISH_DATE_SET | 53626 |
| PUBLISH_DATE_CONFIRMED | 532 |
| PUBLISH_DATE_CONFLICT | 2679 |
| PUBLISH_DATE_SKIPPED_NULL | 6298 |

## Post-Import State

| metric | count |
| --- | ---: |
| companies | 2552 |
| active | 2484 |
| inactive | 68 |
| canonical Q | 72498 |
| fundamentals rows | 72498 |
| core-ready | 11930 |
| core-not-ready | 60568 |
| core-not-ready missing EBITDA only | 24773 |
| publish known | 65531 |
| publish NULL | 6967 |

Core-ready increased only slightly because most added historical quarters intentionally lack EBITDA.

## Historical Depth

| metric | value |
| --- | ---: |
| companies with 4Q+ | 2499 |
| companies with 8Q+ | 2482 |
| companies with 12Q+ | 2433 |
| companies with 16Q+ | 2379 |
| companies with 20Q+ | 2293 |
| companies with 24Q+ | 2106 |
| companies with 28Q+ | 1822 |
| median Q/company | 32 |
| P25 / P75 | 26 / 33 |
| max Q/company | 35 |
| oldest canonical period | 2018-01-01 |

Newly imported candidates by period_end year:

| year | explicit Legacy | SEC Q4 | total |
| --- | ---: | ---: | ---: |
| 2018 | 5420 | 1999 | 7419 |
| 2019 | 5041 | 1896 | 6937 |
| 2020 | 6496 | 1893 | 8389 |
| 2021 | 6985 | 2168 | 9153 |
| 2022 | 7083 | 2237 | 9320 |
| 2023 | 7381 | 2273 | 9654 |
| 2024 | 7373 | 1950 | 9323 |
| 2025 | 2297 | 200 | 2497 |
| 2026 | 426 | 17 | 443 |

## Residual HOLD

The `4342` HOLD rows remain excluded. Phase 3C-2B input artifact:

`phase3c2b_residual_candidates.csv`

HOLD leakage:

`0`

## Idempotency

Second run with the same run ID:

| metric | count |
| --- | ---: |
| new Qs | 0 |
| new field inserts | 0 |
| new publish inserts | 0 |
| overwrites | 0 |
| conflicts reported without overwrite | 24766 |
| duplicate semantic issues | 0 |

## Integrity

Post-production integrity:

- `quick_check = ok`
- `foreign_key_check_rows = 0`
- duplicate company/FY/FQ = `0`
- pre-2018 canonical Q count = `0`
- company changes = `0`
- active-state changes = `0`
- provider/network calls = `0`
- V2 canonical contribution = `0`

## Phase 4C Handoff

Phase 4C inventory:

`phase4c_ebit_ebitda_derivation_inventory.csv`

Rows:

`60227`

Post-import missing field counts:

- EBIT NULL = `17928`
- EBITDA NULL = `60211`

Locked future phase:

`MASTER PLAN PHASE 4C — EBIT & EBITDA DERIVATION RESEARCH AND VALIDATION`

## Next Step

Recommended next phase:

`MASTER PLAN PHASE 3C-2B — LEGACY DEEP-HISTORY REPAIR`
