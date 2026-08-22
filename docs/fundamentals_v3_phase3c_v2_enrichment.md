# Fundamentals V3 Phase 3C V2 Enrichment

Status: `FUNDAMENTALS_V3_PHASE3C_V2_ENRICHMENT_COMPLETE`

Artifact root:

`temp/fundamentals_v3_phase3c_v2_enrichment/20260822T_PHASE3C_V2_ENRICHMENT`

## Scope

Phase 3C applied V2 only as a confidence-gated enrichment source for the refined Yahoo-seeded V3 universe. V2 was not allowed to create canonical quarters and was not allowed to overwrite any existing non-null canonical V3 value or existing non-null `publish_date`.

Network/provider calls: 0.

Legacy canonical contribution: 0. Legacy was read only for the V2-only history crosscheck.

## Pre Baseline

The refined Phase 3B-U baseline was reproduced before writing:

| Metric | Count |
| --- | ---: |
| Companies | 2552 |
| Active companies | 2484 |
| Inactive companies | 68 |
| Canonical Q rows | 13017 |
| Core-ready Q rows | 11907 |
| Core-not-ready Q rows | 1110 |
| Publish-date known | 11808 |
| Publish-date NULL | 1209 |

## Identity Gate

Phase 3C reused the DIAG2 identity model and kept quarter identity separate from field value equivalence.

The revised gate starts from exact ticker + fiscal year + fiscal quarter, requires compatible period-end evidence, blocks material period conflicts and mapping-risk candidates, requires revenue not to contradict when revenue exists, and uses trusted field fingerprint plus same-Q-vs-adjacent evidence to classify `SAME_QUARTER_CONFIRMED`.

The 5% identity tolerance is used only for quarter identity evidence. Strict Phase 3A field comparison remains the value-equivalence rule for canonical fields.

## V2 Identity Population

| Metric | Count |
| --- | ---: |
| V2 source quarters examined on refined universe | 56502 |
| Exact ticker/FY/FQ candidates | 9727 |
| `SAME_QUARTER_CONFIRMED` | 8548 |
| `MAPPING_RISK` | 471 |
| `CLEAR_WRONG_QUARTER` | 200 |
| `PERIOD_IDENTITY_CONFLICT` | 17 |
| `AMBIGUOUS` | 368 |
| `PROBABLE_SAME_QUARTER` | 32 |
| `INSUFFICIENT_EVIDENCE` | 91 |

Apply states:

| Apply state | Count |
| --- | ---: |
| `AUTO_ENRICH_ALLOWED` | 8548 |
| `BLOCK_NO_WRITE` | 688 |
| `HOLD_NO_WRITE` | 491 |

Only `AUTO_ENRICH_ALLOWED` candidates were passed to the production migration engine.

## No-Overwrite Policy

The permanent V2 policy is:

`V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`

V2 may fill a canonical V3 `NULL` value only when same-quarter identity is independently confirmed. V2 differences against non-null V3 values are evidence only and are not corrections in Phase 3C.

No-overwrite proof:

| Check | Count |
| --- | ---: |
| Existing non-null canonical values checked | 147103 |
| Existing non-null canonical values overwritten | 0 |
| Existing publish dates checked | 11808 |
| Existing publish dates overwritten | 0 |

## Field Contribution

V2 filled 319 canonical field NULLs.

| Field | V2 NULL fills |
| --- | ---: |
| capex | 228 |
| gross_profit | 45 |
| operating_income | 14 |
| total_debt | 14 |
| EBITDA | 5 |
| cash | 3 |
| free_cashflow | 3 |
| operating_cashflow | 3 |
| EBIT | 2 |
| net_income | 1 |
| revenue | 1 |
| shares_outstanding | 0 |

Weighted-average share fields were explicitly rejected as canonical `shares_outstanding` substitutes.

## Metadata Contribution

| Metadata outcome | Count |
| --- | ---: |
| `PERIOD_DATE_CONFIRMED` | 8529 |
| `PERIOD_DATE_SAFE_VARIANT` | 9 |
| `PUBLISH_DATE_CONFIRMED` | 8531 |
| `PUBLISH_DATE_SET` | 6 |
| `PUBLISH_DATE_SKIPPED_NULL` | 1 |

Publication coverage:

| Metric | Before | After | Delta |
| --- | ---: | ---: | ---: |
| Publish-date known | 11808 | 11814 | +6 |
| Publish-date NULL | 1209 | 1203 | -6 |

## Core Readiness Delta

| Metric | Before | After | Delta |
| --- | ---: | ---: | ---: |
| Core-ready Q rows | 11907 | 11923 | +16 |
| Core-not-ready Q rows | 1110 | 1094 | -16 |

Core-field missingness:

| Field | NULL before | V2 fills | NULL after |
| --- | ---: | ---: | ---: |
| revenue | 577 | 1 | 576 |
| EBITDA | 735 | 5 | 730 |
| free_cashflow | 586 | 3 | 583 |
| cash | 577 | 3 | 574 |
| total_debt | 799 | 14 | 785 |
| shares_outstanding | 0 | 0 | 0 |

## Agreement And Differences

For confirmed same-quarter rows, V2 non-null values that matched strict value equivalence were recorded as agreement evidence. V2 values that differed from non-null V3 values were recorded as evidence-only conflicts and were not written as canonical corrections.

Largest evidence-only conflict groups:

| Field | Conflicts |
| --- | ---: |
| EBITDA | 6815 |
| total_debt | 5203 |
| free_cashflow | 4338 |
| shares_outstanding | 4087 |
| capex | 3373 |
| cash | 2825 |
| operating_cashflow | 1755 |
| operating_income | 1669 |
| gross_profit | 1393 |
| net_income | 930 |
| revenue | 228 |
| EBIT | 153 |
| publish_date | 11 |

These rows are not Phase 3C corrections. They remain comparison evidence for later reconciliation if needed.

## V2-Only Historical Inventory

V2-only FY/FQ candidates absent from canonical V3 were inventoried but not imported:

| Category | Count |
| --- | ---: |
| V2-only historical Q candidates | 46775 |
| V2-only with Legacy exact period-end match | 40619 |
| V2-only without Legacy exact period-end match | 6156 |

These rows are Phase 3D planning input. Phase 3C created zero V2 canonical quarters.

## Production Apply

Production V2 apply:

| Metric | Count |
| --- | ---: |
| Source rows examined | 8538 |
| Existing canonical quarters matched | 8538 |
| Canonical quarters created | 0 |
| Candidate rows accepted | 8538 |

The source rows passed to the engine were limited to confirmed candidates with actual V2 NULL-fill or publication-fill/confirmation work. Routine non-null V2 differences were not converted into permanent resolution issue explosions.

## Idempotency

Second semantic V2 apply:

| Check | Result |
| --- | --- |
| Row counts unchanged | true |
| New Q creations | 0 |
| New NULL fills | 0 |
| New publish fills | 0 |
| Duplicate semantic issues | 0 |

## Integrity

Post-apply production integrity:

| Check | Result |
| --- | --- |
| `PRAGMA quick_check` | `ok` |
| foreign key check rows | 0 |
| duplicate company keys | 0 |
| duplicate work-unit keys | 0 |
| orphan fundamentals | 0 |
| orphan migration audit company refs | 0 |
| orphan resolution issue quarter refs | 0 |
| V2-created canonical Q rows | 0 |

## Phase 3D Handoff

Phase 3D should use the same conceptual safety principle for Legacy overlap:

- fiscal identity candidate
- compatible dates
- trusted value fingerprint when overlap exists
- no blind trust in source FY/FQ alone
- no overwrite without explicit correction authority

Recommended next phase:

`MASTER PLAN PHASE 3D - LEGACY DEEP-HISTORY ENRICHMENT`
