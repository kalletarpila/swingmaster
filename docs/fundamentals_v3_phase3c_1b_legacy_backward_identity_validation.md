# Fundamentals V3 Phase 3C-1B Legacy Backward Identity Validation

Phase 3C-1B validates Legacy-only historical quarter identity without writing production data.
The validator starts from the newest reliable V3 anchors and walks Legacy rows newest-to-oldest,
stopping at the first material break for each company. Phase 3C-2 may consume only rows marked
`READY_FOR_PHASE3C2_IMPORT`.

## Baseline

The Phase 3C-1 baseline was reproduced before analysis:

| Metric | Count |
| --- | ---: |
| V3 companies | 2552 |
| V3 canonical Q | 13017 |
| Legacy rows examined | 132630 |
| Existing-Q candidates | 10460 |
| Existing-Q `SAME_QUARTER_CONFIRMED` | 4204 |
| Possible mapping conflicts | 5587 |
| Clear mapping conflicts | 212 |
| Legacy-only history | 122170 |
| Pre-1999 excluded | 22 |
| Eligible 1999+ Legacy-only rows | 122148 |

## Method

Anchor hierarchy:

1. Recent 2026/2025 V3 quarters with Legacy `SAME_QUARTER_CONFIRMED` evidence.
2. Older V3 quarters with Legacy `SAME_QUARTER_CONFIRMED` evidence.
3. No automatic propagation when only an unconfirmed fallback row exists.

Backward predecessor rule:

| Current | Predecessor |
| --- | --- |
| FY N Q4 | FY N Q3 |
| FY N Q3 | FY N Q2 |
| FY N Q2 | FY N Q1 |
| FY N Q1 | FY N-1 Q4 |

Period continuity accepts normal quarterly spacing, 52/53-week variants, and small provider date
variants. Fiscal transition intervals are held for explicit review. Material gaps and out-of-order
periods stop automatic propagation.

The STOP-at-breakpoint rule is strict: newer validated rows stay validated, the breakpoint row is
held, and older rows remain `BEHIND_BREAKPOINT_UNCONFIRMED`.

## Recent Anchor Quality

| Fiscal year | Overlap rows | Same-quarter confirmed | Confirmed % | Revenue comparable | Revenue <=5% | Period exact/compatible |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026 | 2959 | 1205 | 40.72% | 2694 | 2589 | 2959 |
| 2025 | 7114 | 2987 | 41.99% | 6932 | 6549 | 7114 |

Anchor distribution:

| Bucket | Companies |
| --- | ---: |
| 2026 reliable anchor | 1136 |
| 2025 reliable anchor | 289 |
| Older reliable anchor | 2 |
| No reliable anchor | 1072 |

Conclusion: recent period-end matching is complete for the overlap population and revenue agreement
is strong where comparable. The validated import gate is therefore usable, but only for the narrow
READY population reached by continuous backward-chain evidence.

## Existing-Q Conflict Reanalysis

The prior 5799 Legacy conflict rows decompose as:

| Typology | Rows |
| --- | ---: |
| `SAME_QUARTER_LIKELY_FIELD_SEMANTICS` | 5055 |
| `SAME_QUARTER_LIKELY_GATE_TOO_STRICT` | 260 |
| `SCALE_OR_NORMALIZATION` | 0 |
| `POSSIBLE_WRONG_FISCAL_MAPPING` | 7 |
| `CLEAR_WRONG_FISCAL_MAPPING` | 197 |
| `SAME_QUARTER_LIKELY_REVISION` | 0 |
| `INSUFFICIENT` | 280 |

Estimated likely same underlying quarter: 5315 rows, or 91.65% of conflict rows.
True/possible mapping risk: 204 rows, or 3.52%.

Adjacent-quarter analysis:

| Population | SAME_Q_BEST | PREVIOUS_Q_BEST | NEXT_Q_BEST | TIE | INSUFFICIENT |
| --- | ---: | ---: | ---: | ---: | ---: |
| All overlap Qs | 10090 | 9 | 133 | 228 | 0 |
| Prior conflict rows | 5658 | 8 | 16 | 117 | 0 |

Interpretation: most prior Legacy conflicts are source-semantic or gate-strictness artifacts, not
actual quarter mapping errors. The remaining mapping-risk rows stay out of automatic deep-history
creation.

## Legacy-Only Classification

Every eligible Legacy-only row received exactly one diagnostic disposition:

| Disposition | Rows |
| --- | ---: |
| `BACKWARD_CHAIN_CONFIRMED` | 2193 |
| `V2_CORROBORATED_CHAIN_CONFIRMED` | 751 |
| `BEHIND_BREAKPOINT_UNCONFIRMED` | 117009 |
| `DIRECT_MAPPING_CONFLICT` | 0 |
| `DUPLICATE_OR_AMBIGUOUS` | 0 |
| `TRANSITION_REQUIRES_RESOLUTION` | 906 |
| `INSUFFICIENT_EVIDENCE` | 1289 |
| Total | 122148 |

Phase 3C-2 readiness:

| Recommendation | Rows |
| --- | ---: |
| `READY_FOR_PHASE3C2_IMPORT` | 2944 |
| `HOLD_FOR_PHASE3C2B_REVIEW` | 119204 |

Expected READY contribution:

| Metric | Value |
| --- | ---: |
| New canonical Q candidates | 2944 |
| Companies gaining historical Qs | 80 |
| Accepted field values | 27452 |
| Publication dates available | 2703 |
| Oldest expected imported year | 2008 |
| Median historical depth, quarters | 37.5 |

READY sequence violations: 0.

## Breakpoints

| Breakpoint reason | Rows |
| --- | ---: |
| `FISCAL_YEAR_TRANSITION_ANOMALY` | 906 |
| `PERIOD_END_CONTINUITY_BREAK` | 517 |

Fiscal-calendar-change and potential-label-repair rows are not automatically crossed in this phase.
They remain held for an explicit repair/review workflow.

## Reliability Curve

| Period-end year | Candidate Qs | Chain confirmed | V2 corroborated | Breakpoints | Behind breakpoint | Confirmed % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026 | 516 | 0 | 0 | 0 | 73 | 0.00% |
| 2025 | 2659 | 158 | 27 | 768 | 1083 | 5.94% |
| 2024 | 9619 | 259 | 134 | 97 | 9070 | 2.69% |
| 2023 | 9981 | 254 | 127 | 0 | 9725 | 2.54% |
| 2022 | 9849 | 243 | 134 | 0 | 9606 | 2.47% |

The curve shows that broad Legacy-only history is not globally importable yet. Confidence remains
useful for a small number of long, continuous company chains, while most rows are correctly held
behind breakpoints or missing anchors.

Company depth among validated chains:

| Depth | Companies |
| --- | ---: |
| >=1 year | 64 |
| >=2 years | 64 |
| >=3 years | 60 |
| >=5 years | 55 |
| >=8 years | 45 |
| >=10 years | 39 |
| >=15 years | 24 |
| >=20 years | 0 |
| Through 1999 | 0 |

## Special Cases

Known fiscal-calendar cases were preserved as regression checks, not hard-coded as generic rules:

| Ticker | Ready rows | Hold rows | Status |
| --- | ---: | ---: | --- |
| CAVA | 0 | 18 | Preserved / not in refined baseline |
| NEUP | 1 | 7 | Preserved / not in refined baseline |
| LFCR | 0 | 69 | Preserved / not in refined baseline |
| BNC | 0 | 63 | Preserved / not in refined baseline |
| SJM | 0 | 65 | Preserved / not in refined baseline |
| LYTS | 0 | 62 | Preserved / not in refined baseline |

## Safety

Read-only proof:

| Check | Result |
| --- | --- |
| V3 writes | 0 |
| Legacy writes | 0 |
| V2 writes | 0 |
| Network/provider calls | 0 |
| `PRAGMA quick_check` | `ok` |
| `PRAGMA foreign_key_check` rows | 0 |

Artifacts were written under:

`temp/fundamentals_v3_phase3c_1b_legacy_backward_validation/20260822T_PHASE3C_1B_LEGACY_BACKWARD_VALIDATION/`

## Phase 3C-2 Gate

Phase 3C-2 may create canonical historical quarters only from
`phase3c2_dry_import_plan.csv`, where:

- disposition is `BACKWARD_CHAIN_CONFIRMED` or `V2_CORROBORATED_CHAIN_CONFIRMED`
- recommendation is `READY_FOR_PHASE3C2_IMPORT`
- row has no sequence violation
- no production write was made by this validation phase

All `HOLD_FOR_PHASE3C2B_REVIEW` rows remain out of Phase 3C-2 automatic import.

Final classification:

`FUNDAMENTALS_V3_PHASE3C_1B_LEGACY_BACKWARD_VALIDATION_COMPLETE_READY_FOR_3C2`

Recommended next phase:

`MASTER PLAN PHASE 3C-2 - LEGACY DEEP-HISTORY EXTENSION`
