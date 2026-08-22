# Fundamentals V3 Phase 3C-1D Legacy Hold / Breakpoint Recovery

Phase 3C-1D extends the 2018+ Legacy validation model with explicit SEC Q4 structure and
independent segment re-anchoring. It is read-only: no canonical V3 quarters, fields, or publication
dates were written.

## Source Shape

Local Legacy data confirms the SEC source structure needed for this phase:

- `rc_fundamental_quarterly` is normalized by `ticker + period_end_date` and has no form type,
  fiscal period, period start, or duration columns.
- `rc_fundamental_statement_raw` contains SEC fact metadata embedded in `field_name`, including
  `form=10-Q/10-K`, `fy=...`, `fp=Q1/Q2/Q3/FY`, `start=...`, and `filed=...`.
- SEC `sec_fact` rows exist for income, balance, and cashflow statements.

This means missing explicit Q4 in `rc_fundamental_quarterly` is not automatically a fiscal-chain
break. For ordinary SEC issuers, Q4 is usually represented through annual `10-K` / `fp=FY` facts.

## Baseline

The Phase 3C-1C population was reproduced exactly:

| Metric | Count |
| --- | ---: |
| Legacy-only rows >=2018 | 67477 |
| Initial READY | 4321 |
| Initial HOLD | 63156 |
| `HOLD_BEHIND_BREAKPOINT` | 61093 |
| `HOLD_TRUE_FISCAL_TRANSITION` | 775 |
| `HOLD_INSUFFICIENT_EVIDENCE` | 1288 |

## SEC Q4 Structure

Fiscal-year structures from SEC raw facts:

| Structure | Fiscal years |
| --- | ---: |
| `Q1_Q2_Q3_FY` | 16827 |
| `Q1_Q2_Q3_Q4_FY` | 4 |
| `FY_ONLY` / partial FY available | 2162 |
| FY missing or partial non-FY structures | 4533 |

Q4 presence:

| Q4 state | Count |
| --- | ---: |
| `EXPECTED_SEC_Q4_NOT_SEPARATELY_FILED` | 16827 |
| `FY_ROW_AVAILABLE_FOR_Q4_RECONSTRUCTION` | 2162 |
| `Q4_EXPLICITLY_AVAILABLE` | 4 |
| `FY_ROW_MISSING` | 4533 |

## Q4 Field Policy

Q4 identity is separate from Q4 value completeness. A fiscal Q4 can be safe to create even when some
fields remain NULL.

Flow fields may be derived only with compatible concepts and durations:

- revenue, gross profit, operating income, net income: `FY - Q1 - Q2 - Q3` when concepts match.
- operating cashflow and capex: prefer `FY - 9M` when directly available and compatible.
- free cashflow: derive from reconstructed OCF + capex under the locked negative-capex convention.
- EBIT and EBITDA: not blindly derived.

Instant fields must use fiscal-year-end values directly:

- cash
- total debt or accepted debt components
- shares outstanding when period-end/instant semantics are valid

Instant fields are never differenced.

## Q4 Calibration

Known V3/Yahoo Q4 calibration tested 2601 canonical Q4 cases with SEC FY-period Legacy evidence.

| Field | Comparable | <=1% | <=2% | <=5% | <=10% | >10% | Sign conflicts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| revenue | 2263 | 2000 | 2006 | 2023 | 2035 | 228 | 0 |
| gross_profit | 1897 | 1849 | 1850 | 1858 | 1868 | 29 | 0 |
| operating_income | 2248 | 1323 | 1376 | 1484 | 1582 | 666 | 109 |
| net_income | 2188 | 1478 | 1495 | 1530 | 1580 | 608 | 111 |
| operating_cashflow | 2272 | 2078 | 2078 | 2083 | 2087 | 185 | 25 |
| capex | 2362 | 1720 | 1736 | 1762 | 1794 | 568 | 0 |
| free_cashflow | 2256 | 1712 | 1761 | 1835 | 1892 | 364 | 58 |
| cash | 2264 | 2231 | 2234 | 2239 | 2240 | 24 | 0 |
| total_debt | 2263 | 1182 | 1250 | 1391 | 1562 | 701 | 0 |
| shares_outstanding | 2522 | 647 | 1023 | 1587 | 1849 | 673 | 0 |
| EBIT | 2021 | 542 | 664 | 910 | 1145 | 876 | 98 |
| EBITDA | 1 | 1 | 1 | 1 | 1 | 0 | 0 |

Calibration supports Q4 identity and FY-end instant use strongly for cash. Flow derivation remains
field-specific. EBIT/EBITDA are not approved for generic derivation by this phase.

## Transition Reanalysis

The prior `775` transition HOLD rows decomposed as:

| Reanalysis | Rows |
| --- | ---: |
| `NORMAL_SEC_Q4_BOUNDARY` | 46 |
| `DATA_GAP` | 700 |
| `UNRESOLVED` | 29 |

The normal SEC Q4 boundary is no longer treated as a fiscal transition. Rows with SEC raw `fy/fp`
metadata or strong Legacy+V2 period evidence can be re-anchored as independent segments.

## V2 Role

V2 remains corroborating evidence only. V2 FY/FQ alone is not enough. The selected safe rule is:

`Legacy row + V2 exact period + SEC or trusted field evidence`

Known V3+Legacy+V2 calibration:

| Metric | Count |
| --- | ---: |
| Tested Qs | 7993 |
| Correct Q | 7150 |
| Wrong Q | 8 |
| Ambiguous | 835 |
| Precision | 99.89% |
| Recall | 89.45% |

V2 helped recover 27780 rows across 2006 companies.

## Final Classification

| Disposition | Rows |
| --- | ---: |
| `READY_EXISTING_CHAIN` | 4321 |
| `READY_SEC_Q4_STRUCTURE` | 14633 |
| `READY_REANCHORED_WITH_V2` | 27780 |
| `READY_REANCHORED_LEGACY_ONLY` | 16401 |
| `READY_BRIDGED_SEGMENT` | 0 |
| `HOLD_TRUE_FISCAL_TRANSITION` | 0 |
| `HOLD_V2_CONFLICT` | 0 |
| `HOLD_MAPPING_CONFLICT` | 0 |
| `HOLD_DUPLICATE_OR_AMBIGUOUS` | 4342 |
| `HOLD_INSUFFICIENT_EVIDENCE` | 0 |
| `HOLD_ISOLATED_ROW` | 0 |
| Total | 67477 |

Final READY: 63135. Final HOLD: 4342.

The generic `HOLD_BEHIND_BREAKPOINT` category is gone. The residual HOLD population is duplicate or
ambiguous and belongs to 3C-2B.

## Yearly Recovery

| Year | Initial READY | SEC Q4 | V2 | Legacy-only | Final READY | HOLD | READY % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026 | 0 | 17 | 19 | 407 | 443 | 73 | 85.85% |
| 2025 | 249 | 200 | 1099 | 949 | 2497 | 162 | 93.91% |
| 2024 | 645 | 1950 | 5179 | 1549 | 9323 | 296 | 96.92% |
| 2023 | 644 | 2273 | 5288 | 1449 | 9654 | 327 | 96.72% |
| 2022 | 621 | 2237 | 5138 | 1324 | 9320 | 529 | 94.63% |
| 2021 | 601 | 2168 | 5031 | 1353 | 9153 | 486 | 94.96% |
| 2020 | 564 | 1893 | 4621 | 1311 | 8389 | 663 | 92.68% |
| 2019 | 520 | 1896 | 1405 | 3116 | 6937 | 1371 | 83.50% |
| 2018 | 477 | 1999 | 0 | 4943 | 7419 | 435 | 94.46% |

## Phase 3C-2 Plan

Expected Phase 3C-2 contribution:

| Metric | Count |
| --- | ---: |
| READY_FOR_PHASE3C2_IMPORT Qs | 63135 |
| Explicit Legacy Qs | 48502 |
| Reconstructed SEC Q4s | 14633 |
| Companies gaining history | 2498 |
| Expected inserted field values | 571667 |
| Expected derived Q4 field values | 175596 |
| Expected FY-end instant values | 34976 |
| Publication dates | 56837 |
| Oldest period | 2018-01-01 |
| Median history depth | 28.0 |
| READY identity/sequence violations | 0 |

Phase 3C-2 can proceed with the deterministic READY plan. Phase 3C-2B should later address the
4342 duplicate/ambiguous rows.

## Safety

| Check | Result |
| --- | --- |
| V3 writes | 0 |
| Legacy writes | 0 |
| V2 writes | 0 |
| Provider/network calls | 0 |
| `PRAGMA quick_check` | `ok` |
| `PRAGMA foreign_key_check` rows | 0 |

Artifacts:

`temp/fundamentals_v3_phase3c_1d_legacy_hold_recovery/20260822T_PHASE3C_1D_LEGACY_HOLD_RECOVERY/`

Final classification:

`FUNDAMENTALS_V3_PHASE3C_1D_LEGACY_HOLD_RECOVERY_COMPLETE_READY_FOR_3C2`

Recommended next phase:

`MASTER PLAN PHASE 3C-2 - LEGACY DEEP-HISTORY EXTENSION`
