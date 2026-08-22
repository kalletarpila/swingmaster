# Fundamentals V3 Phase 3C Legacy Existing-Q Enrichment

Status: `FUNDAMENTALS_V3_PHASE3C_LEGACY_EXISTING_Q_ENRICHMENT_COMPLETE`

Artifact root:

`temp/fundamentals_v3_phase3c_legacy_existing_q_enrichment/20260822T_PHASE3C_LEGACY_EXISTING_Q_ENRICHMENT`

## Scope

This phase applied Legacy only to canonical V3 quarters that already existed after the Yahoo seed,
financial-universe refinement, and the earlier safe V2 NULL-fill pass. Legacy did not create new
canonical quarters and did not overwrite existing non-null canonical values or publication dates.

Legacy now precedes residual V2 in the master-plan order because it is the preferred historical
backbone for existing-Q enrichment and Phase 3D deep-history extension. This is not blind trust:
Legacy still requires same-quarter validation and remains NULL-fill only for automatic enrichment.

Network/provider calls: 0.

V2 canonical contribution in this phase: 0.

## Pre Baseline

The current post-V2 production baseline was reproduced before Legacy writes:

| Metric | Count |
| --- | ---: |
| Companies | 2552 |
| Active companies | 2484 |
| Inactive companies | 68 |
| Canonical Q rows | 13017 |
| Core-ready Q rows | 11923 |
| Core-not-ready Q rows | 1094 |
| Publish-date known | 11814 |
| Publish-date NULL | 1203 |

Core NULLs before Legacy:

| Field | NULL before |
| --- | ---: |
| revenue | 576 |
| EBITDA | 730 |
| free_cashflow | 583 |
| cash | 574 |
| total_debt | 785 |
| shares_outstanding | 0 |

## Legacy Model

Legacy source tables used:

- `rc_fundamental_quarterly`: ticker, period_end_date, accepted fundamental values, currency, run id.
- `rc_fundamental_quarter_earnings_match`: actual announcement date evidence where matched.
- `rc_fundamental_quarterly_vintage` and field provenance tables exist, but Phase 3C used the current accepted quarterly row only.

Legacy does not provide canonical FY/FQ in `rc_fundamental_quarterly`, so existing-Q matching used
refined V3 ticker plus exact period end, then required financial fingerprint evidence before
automatic enrichment.

## Identity Gate

Legacy identity classification uses the same conceptual lesson as DIAG2:

`identity match != value equivalence`

Trusted identity fields:

- revenue
- gross_profit
- operating_income
- net_income
- operating_cashflow
- cash
- total_debt

Semantic-risk fields such as EBIT, EBITDA, free_cashflow, and shares_outstanding can contribute to
comparison evidence but do not veto identity alone.

## Identity Population

| Metric | Count |
| --- | ---: |
| Legacy rows examined on refined universe | 132630 |
| Existing canonical Q candidates | 10460 |
| `SAME_QUARTER_CONFIRMED` | 4204 |
| `PROBABLE_SAME_QUARTER` / ambiguous | 107 |
| `INSUFFICIENT_EVIDENCE` | 350 |
| `POSSIBLE_MAPPING_CONFLICT` | 5587 |
| `CLEAR_MAPPING_CONFLICT` | 212 |
| `PERIOD_IDENTITY_CONFLICT` | 0 |
| Blocked total | 5799 |

Only `SAME_QUARTER_CONFIRMED` rows were eligible for automatic enrichment.

## No-Overwrite Policy

`LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`

`V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`

Automatic enrichment sources may fill canonical `NULL` values only. Existing non-null values remain
unchanged even when the incoming source differs materially.

No-overwrite proof:

| Check | Count |
| --- | ---: |
| Existing non-null canonical values checked | 147422 |
| Existing non-null canonical values overwritten | 0 |
| Existing publish dates checked | 11814 |
| Existing publish dates overwritten | 0 |

## Legacy Contribution

Legacy filled 111 fundamental NULLs:

| Field | NULL fills | Confirmations | Strict conflicts |
| --- | ---: | ---: | ---: |
| revenue | 9 | 3930 | 113 |
| gross_profit | 0 | 2713 | 60 |
| operating_income | 15 | 3375 | 666 |
| EBIT | 21 | 1274 | 2283 |
| EBITDA | 0 | 17 | 0 |
| net_income | 7 | 3454 | 85 |
| operating_cashflow | 9 | 4141 | 52 |
| capex | 33 | 2320 | 1212 |
| free_cashflow | 7 | 2753 | 1254 |
| cash | 7 | 4105 | 48 |
| total_debt | 3 | 2892 | 578 |
| shares_outstanding | 0 | 229 | 3321 |

Strict conflicts are evidence only and were not applied as corrections.

## Metadata Contribution

| Outcome | Count |
| --- | ---: |
| `PERIOD_DATE_CONFIRMED` | 144 |
| `PUBLISH_DATE_SET` | 91 |
| `PUBLISH_DATE_SKIPPED_NULL` | 53 |

Publication coverage:

| Metric | Before | After | Delta |
| --- | ---: | ---: | ---: |
| Publish-date known | 11814 | 11905 | +91 |
| Publish-date NULL | 1203 | 1112 | -91 |

## Core Readiness Delta

| Metric | Before | After | Delta |
| --- | ---: | ---: | ---: |
| Core-ready Q rows | 11923 | 11926 | +3 |
| Core-not-ready Q rows | 1094 | 1091 | -3 |

Core missingness:

| Field | NULL before | Legacy fills | NULL after |
| --- | ---: | ---: | ---: |
| revenue | 576 | 9 | 567 |
| EBITDA | 730 | 0 | 730 |
| free_cashflow | 583 | 7 | 576 |
| cash | 574 | 7 | 567 |
| total_debt | 785 | 3 | 782 |
| shares_outstanding | 0 | 0 | 0 |

## V2 Fill Counterfactual

The completed V2 pass had already filled 319 field NULLs. Legacy was compared counterfactually
against those rows. No V2 fill was reverted or overwritten.

| Classification | Count |
| --- | ---: |
| Prior V2 fills audited | 319 |
| `LEGACY_NOT_AVAILABLE` | 261 |
| `LEGACY_AVAILABLE_SAME_VALUE` | 4 |
| `LEGACY_AVAILABLE_ROUNDING_EQUIVALENT` | 0 |
| `LEGACY_AVAILABLE_DIFFERENT_VALUE` | 12 |
| `LEGACY_IDENTITY_NOT_CONFIRMED` | 42 |

Interpretation: Legacy would not have supplied most of the V2 fills. Only 4 of 319 were safely
available with the same value, and 12 were safely available but materially different. The earlier
V2 pass remains valid under no-overwrite rules.

## Phase 3D History Inventory

Legacy-only historical rows absent from current canonical V3 were inventoried, not imported:

| Category | Count |
| --- | ---: |
| Legacy-only historical Q candidates | 122170 |
| Ready for Phase 3D identity validation | 122148 |
| Identity ambiguous | 0 |
| Duplicate source rows | 0 |
| Pre-1999 excluded | 22 |
| Other review | 0 |
| With V2 exact period counterpart | 40624 |
| Without V2 counterpart | 81546 |

## Idempotency And Integrity

Second Legacy apply:

- row counts unchanged: true
- new Q creations: 0
- new NULL fills: 0
- new publish fills: 0
- duplicate semantic issues: 0

Production integrity:

- `PRAGMA quick_check`: `ok`
- foreign key check rows: 0
- duplicate company keys: 0
- duplicate work-unit keys: 0
- orphan fundamentals: 0
- orphan migration audit company refs: 0
- orphan resolution issue quarter refs: 0

## Phase 3D Handoff

Recommended next phase:

`MASTER PLAN PHASE 3D - LEGACY DEEP-HISTORY EXTENSION`
