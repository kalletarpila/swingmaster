# Fundamentals V3 Phase 3C-2B - Legacy Deep-History Repair

Status:

`FUNDAMENTALS_V3_PHASE3C_2B_LEGACY_REPAIR_COMPLETE`

Production run ID:

`V3_PHASE3C2B_LEGACY_DEEP_HISTORY_REPAIR_2026-08-23T084536Z`

Artifact root:

`temp/fundamentals_v3_phase3c_2b_legacy_deep_history_repair/20260823T_PHASE3C_2B_LEGACY_REPAIR/`

Source-boundary backup:

`temp/fundamentals_v3_phase3c_2b_legacy_deep_history_repair/20260823T_PHASE3C_2B_LEGACY_REPAIR/rc_fundamentals_v3_pre_phase3c2b_backup.db`

## Scope

Phase 3C-2B classified all `4342` residual Legacy HOLD rows left after Phase 3C-2. The population
was not treated as missing history by default. Every row already collided with an existing canonical
FY/FQ and had a period-date variant against that canonical quarter.

No new canonical quarters were created. Four rows passed the conservative same-result gate and were
used only to fill NULL fields and eligible NULL publication dates on existing canonical quarters.

Permanent rules preserved:

- `LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`
- `V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`
- no pre-2018 writes
- no HOLD imports
- no V2 value contribution
- no EBIT/EBITDA derivation

## Residual Population

| metric | count |
| --- | ---: |
| residual Legacy rows | 4342 |
| affected companies | 1491 |
| canonical FY/FQ already exists | 4342 |
| same period under another FY/FQ | 0 |
| genuinely missing canonical Q | 0 |

Year distribution:

| year | rows |
| --- | ---: |
| 2018 | 435 |
| 2019 | 1371 |
| 2020 | 663 |
| 2021 | 486 |
| 2022 | 529 |
| 2023 | 327 |
| 2024 | 296 |
| 2025 | 162 |
| 2026 | 73 |

Quarter distribution:

| quarter | rows |
| --- | ---: |
| Q1 | 866 |
| Q2 | 925 |
| Q3 | 858 |
| Q4 | 1693 |

## Typology

All rows entered as `HOLD_DUPLICATE_OR_AMBIGUOUS` with `DUPLICATE_READY_FYFQ` evidence. The final
terminal classification is:

| classification | rows |
| --- | ---: |
| READY_EXISTING_Q_NULL_FILL | 4 |
| SAME_RESULT_PERIOD_VARIANT | 6 |
| SAME_RESULT_RESTATEMENT_VARIANT | 205 |
| HOLD_VALUE_CONFLICT | 3249 |
| HOLD_SEMANTIC_AMBIGUITY | 878 |

No rows qualified as `READY_NEW_Q`. The residual set is not a material source of additional
historical quarters.

## Repair Rule

A residual could become `READY_EXISTING_Q_NULL_FILL` only when:

- canonical FY/FQ already existed
- residual and canonical row had no conflicting non-null values
- at least three trusted fields matched exactly/equivalently
- target canonical fields were NULL
- incoming fields were not EBIT or EBITDA
- existing publish_date was NULL before any publish fill

Trusted fingerprint fields:

- revenue
- gross_profit
- operating_income
- net_income
- operating_cashflow

V2 was read only as corroboration. V2 never controlled canonical identity and wrote no values.

## V2 Evidence

| V2 evidence | rows |
| --- | ---: |
| SUPPORTS_SAME_RESULT | 804 |
| V2_AMBIGUOUS | 3538 |

## Production Repair

| metric | count |
| --- | ---: |
| new canonical Qs | 0 |
| existing Qs NULL-filled | 4 |
| field NULL fills | 11 |
| publish_date fills | 2 |
| existing non-null values overwritten | 0 |
| existing publish dates overwritten | 0 |
| HOLD leakage | 0 |
| pre-2018 writes | 0 |

Filled fields:

| field | fills |
| --- | ---: |
| cash | 4 |
| total_debt | 3 |
| capex | 1 |
| free_cashflow | 1 |
| operating_cashflow | 1 |
| shares_outstanding | 1 |

Production rows:

- ADUS FY2019 Q4
- CIEN FY2020 Q1
- ICHR FY2022 Q2
- ICHR FY2022 Q1

## Historical Coverage Impact

Because no new Qs were created, historical depth did not change:

| metric | before | after |
| --- | ---: | ---: |
| canonical Q | 72498 | 72498 |
| >=8 Q | 2482 | 2482 |
| >=16 Q | 2379 | 2379 |
| >=24 Q | 2106 | 2106 |
| >=28 Q | 1822 | 1822 |
| median Q/company | 32 | 32 |

## Field Coverage Delta

| field | NULL before | NULL after |
| --- | ---: | ---: |
| revenue | 8435 | 8435 |
| EBITDA | 60211 | 60211 |
| FCF | 9299 | 9298 |
| cash | 3601 | 3597 |
| total_debt | 29797 | 29794 |
| shares_outstanding | 6418 | 6417 |
| EBIT | 17928 | 17928 |
| publish_date | 6967 | 6965 |

## Remaining Residuals

Rows not written:

| bucket | rows |
| --- | ---: |
| same-result period variant, no useful safe write | 6 |
| same-result restatement/source-version variant | 205 |
| value conflict | 3249 |
| semantic ambiguity / insufficient fingerprint | 878 |

These rows are now deterministic residuals rather than an undifferentiated duplicate bucket.

## Integrity And Idempotency

Post-repair integrity:

- `quick_check = ok`
- `foreign_key_check_rows = 0`
- duplicate company/FY/FQ = `0`
- company count = `2552`
- active/inactive = `2484 / 68`

Second run:

- new Qs = `0`
- field fills = `0`
- publish fills = `0`
- overwrites = `0`
- duplicate semantic issues = `0`

## V2 Handoff

The remaining HOLD rows are not safe for Legacy repair. They are available as input for later V2
historical gap review:

`phase3c4_v2_historical_gap_candidates.csv`

Recommended next phase:

`MASTER PLAN PHASE 3C-3 — V2 RESIDUAL EXISTING-Q ENRICHMENT`
