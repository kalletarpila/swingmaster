# Fundamentals V3 Phase 3B-U Universe Financial Refinement

Status: `FUNDAMENTALS_V3_FINANCIAL_UNIVERSE_REFINEMENT_COMPLETE`

Run artifact root:

`temp/fundamentals_v3_phase3b_universe_refinement/20260822T_PHASE3B_UNIVERSE_REFINEMENT`

## Scope

This phase refined the approved Fundamentals V3 universe by removing company profiles that are not appropriate for the ordinary industrial fundamentals model:

- banks
- insurers and reinsurers
- other pure financial companies such as asset managers, capital markets firms, credit services, mortgage finance, and financial conglomerates

REITs, including mortgage REITs, remain in the V3 universe. Financial infrastructure companies such as financial data and stock exchanges also remain in the V3 universe.

No provider/network acquisition was performed. The refined baseline was rebuilt from the completed local Yahoo raw cache and existing Phase 2D metadata artifacts. RawCandle was read for local taxonomy metadata only and was not changed.

## Classification Result

| Class | Companies | Decision |
| --- | ---: | --- |
| `EXCLUDE_BANK` | 68 | Exclude |
| `EXCLUDE_INSURANCE` | 45 | Exclude |
| `EXCLUDE_OTHER_FINANCIAL` | 147 | Exclude |
| `KEEP_REIT` | 154 | Keep |
| `KEEP_FINANCIAL_INFRASTRUCTURE` | 10 | Keep |
| `KEEP_OTHER` | 2388 | Keep |
| `MANUAL_REVIEW_REQUIRED` | 0 | Keep pending explicit policy |

Total excluded companies: 260.

## Universe Impact

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Approved companies | 2812 | 2552 | -260 |
| Active companies | 2735 | 2484 | -251 |
| Inactive companies | 77 | 68 | -9 |
| Canonical Q rows | 14345 | 13017 | -1328 |
| Core-ready Q rows | 12344 | 11907 | -437 |
| Core-not-ready Q rows | 2001 | 1110 | -891 |
| Publish-date-known Q rows | 12990 | 11808 | -1182 |
| Publish-date-null Q rows | 1355 | 1209 | -146 |
| Publication-ready percentage | 90.55% | 90.71% | +0.16 pp |

The refinement removed a disproportionate share of core-not-ready rows because many remaining gaps were bank, insurer, or pure-financial company rows whose statement shape is not suited to the ordinary V3 core model.

## Rebuilt Yahoo Baseline

The refined Yahoo-seeded baseline was rebuilt into a temporary candidate database and then promoted to production after integrity and parity gates passed.

Candidate accounting:

| Metric | Count |
| --- | ---: |
| Normalized Yahoo rows | 13045 |
| Candidate rows | 13045 |
| Missing metadata rows | 0 |
| Accepted rows | 13044 |
| Rejected rows | 1 |
| Canonical quarters created | 13017 |
| Existing canonical quarters matched | 27 |
| Resolution-required rows | 28 |

Candidate dispositions:

| Disposition | Count |
| --- | ---: |
| `DIRECT_CANONICAL_CANDIDATE` | 12956 |
| `FISCAL_MAPPING_CORRECTION` | 71 |
| `COMPLEMENTARY_SAME_FISCAL_Q` | 17 |
| `PROVIDER_PERIOD_VARIANT_EXCLUDED` | 1 |

## Integrity Gates

Temporary candidate database:

- `quick_check`: `ok`
- `foreign_key_check_rows`: 0
- duplicate company keys: 0
- duplicate work-unit keys: 0
- orphan fundamentals: 0
- orphan migration-audit company references: 0
- orphan resolution-issue quarter references: 0

Production database after replacement:

- `quick_check`: `ok`
- `foreign_key_check_rows`: 0
- duplicate company keys: 0
- duplicate work-unit keys: 0
- orphan fundamentals: 0
- orphan migration-audit company references: 0
- orphan resolution-issue quarter references: 0

Retained-company parity:

- retained company value differences: 0
- retained company metadata differences: 0

Idempotency:

- excluded companies remaining in production: 0
- retained values unchanged: true
- final production company total: 2552

Special-case parity:

- CAVA FY2026 Q1 present exactly once
- NEUP corrected FY2026 Q1/Q2/Q3 mappings present
- LFCR `2025-09-30` transition provider-period variant excluded from canonical quarters

## Core Coverage After Refinement

Final core-ready Q rows: 11907 / 13017.

Final core-not-ready Q rows: 1110 / 13017.

Remaining core-field missingness:

| Field | Missing before | Missing after |
| --- | ---: | ---: |
| revenue | 633 | 577 |
| EBITDA | 1613 | 735 |
| free cash flow | 646 | 586 |
| cash | 637 | 577 |
| total debt | 874 | 799 |
| shares outstanding | 0 | 0 |

## V2 Policy Lock

Phase 3B-U also locks the permanent V2 no-overwrite policy for later Phase 3C enrichment:

`V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`

V2 may fill canonical V3 `NULL` values only after same-quarter identity is confirmed. V2 must not overwrite a non-null canonical V3 financial value or non-null canonical V3 `publish_date`; materially different V2 data becomes conflict/audit evidence.

Refined V2 diagnostic population:

| Metric | Count |
| --- | ---: |
| Exact FY/FQ candidates | 9727 |
| Revised same-quarter confirmed | 9018 |
| Mapping-risk rows | 49 |
| Blocked rows | 660 |

Refined V2 safe-fill potential under the locked no-overwrite policy:

| Field | Current gate | Revised same-quarter gate |
| --- | ---: | ---: |
| revenue | 0 | 1 |
| EBITDA | 0 | 48 |
| free cash flow | 0 | 3 |
| cash | 0 | 4 |
| total debt | 0 | 15 |
| shares outstanding | 0 | 0 |
| publish date | 0 | 6 |

## Production State

`rc_fundamentals_v3.db` now contains the refined Phase 3B-U production baseline:

- 2552 companies
- 2484 active companies
- 68 inactive companies
- 13017 canonical quarters
- 11907 core-ready quarters
- 1110 core-not-ready quarters

The pre-refinement production backup is stored at:

`temp/fundamentals_v3_phase3b_universe_refinement/20260822T_PHASE3B_UNIVERSE_REFINEMENT/rc_fundamentals_v3.pre_financial_refinement.db`

## Next Step

Recommended next phase:

`MASTER PLAN PHASE 3C - V2 METADATA & VALUE ENRICHMENT`
