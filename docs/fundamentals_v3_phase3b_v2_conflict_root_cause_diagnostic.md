# Fundamentals V3 Phase 3B-DIAG2 V2 Conflict Root-Cause Diagnostic

Status: `V2_CONFLICT_DIAGNOSTIC_GATE_REVISION_READY`

Artifact root:

`temp/fundamentals_v3_phase3b_v2_conflict_diagnostic/20260822T_PHASE3B_V2_CONFLICT_DIAGNOSTIC/`

This phase was read-only. It did not modify V3, V2, Legacy, RawCandle, provider cache, Check, or Update.

## Why This Was Needed

Phase 3B-DIAG classified 7,261 of 10,622 exact V2/V3 FY/FQ candidates as `CONFLICT`. That count was too broad to interpret as wrong fiscal mapping. DIAG2 decomposes those conflicts into field disagreement, semantic differences, tolerance effects, adjacent-quarter behavior, and mapping-risk categories.

The previous adjacent metric, `17977 / 19030 = 94.47%`, was not a unique-quarter denominator. It was a candidate-adjacent pair count: each eligible exact V2/V3 FY/FQ candidate could compare against previous and next adjacent quarters. DIAG2 recomputed the result at unique Q level.

## Reproduced Population

| Metric | Count |
| --- | ---: |
| Exact V2/V3 FY/FQ candidates | 10,622 |
| `CONFLICT` | 7,261 |
| `PERIOD_IDENTITY_CONFLICT` | 16 |

## Revenue Findings

Revenue is mostly consistent even inside the `CONFLICT` population:

| Revenue bucket | Count |
| --- | ---: |
| comparable | 6,965 |
| <=1% | 6,031 |
| <=2% | 6,076 |
| <=5% | 6,142 |
| <=10% | 6,215 |
| >10% | 750 |
| >25% | 613 |
| >50% | 368 |
| unavailable | 296 |

Thus `6,142` conflict rows have revenue within 5%, and `6,215` have revenue within 10%. The initial `CONFLICT` label is therefore usually not evidence of a wrong quarter.

## Field-Level Conflict Drivers

Conflict participation counts:

| Field | Conflicts |
| --- | ---: |
| total_debt | 4,189 |
| EBITDA | 3,602 |
| cash | 3,096 |
| free_cashflow | 2,305 |
| capex | 1,805 |
| operating_cashflow | 1,495 |
| operating_income | 1,256 |
| gross_profit | 1,199 |
| net_income | 1,192 |
| revenue | 823 |
| shares_outstanding | 410 |
| EBIT | 87 |

Primary conflict field counts:

| Field | Count |
| --- | ---: |
| cash | 2,409 |
| total_debt | 2,299 |
| operating_cashflow | 1,037 |
| revenue | 823 |
| net_income | 693 |

Current conflicts are driven mainly by balance-sheet and cash-flow fields, not by basic reported revenue.

## Statement Fingerprints

Income-statement fingerprint: revenue, gross_profit, operating_income, net_income.

| Classification | Count |
| --- | ---: |
| all available income fields agree | 4,464 |
| most income fields agree | 1,226 |
| mixed | 563 |
| most/all income fields conflict | 749 |
| insufficient | 259 |

Balance fingerprint:

| Classification | Count |
| --- | ---: |
| all/most balance fields agree | 4,055 |
| mixed | 1,633 |
| most/all balance fields conflict | 1,368 |
| insufficient | 205 |

Cash-flow fingerprint:

| Classification | Count |
| --- | ---: |
| all/most cash-flow fields agree | 4,455 |
| mixed | 203 |
| most/all cash-flow fields conflict | 2,174 |
| insufficient | 429 |

Cross-checks:

| Pattern | Count |
| --- | ---: |
| balance agrees but income conflicts | 503 |
| income agrees but balance conflicts | 1,097 |

This supports separating same-quarter identity from field-value equivalence.

## Period Dates

| Period-end bucket | Conflicts |
| --- | ---: |
| exact same date | 7,238 |
| 1-7 days | 0 |
| 8-31 days | 23 |
| >31 days | 0 |

Same-period-end conflict rows:

| Metric | Count |
| --- | ---: |
| same period-end conflicts | 7,238 |
| same period-end + revenue <=5% | 6,130 |
| same period-end + income fingerprint agrees | 5,682 |

The large majority of conflicts occur on the exact same period end and have basic income evidence supporting the same underlying quarter.

## Q-Level Adjacent Check

All exact FY/FQ candidates:

| Result | Count |
| --- | ---: |
| eligible Q | 10,622 |
| `SAME_Q_BEST` | 9,708 |
| `PREVIOUS_Q_BEST` | 39 |
| `NEXT_Q_BEST` | 23 |
| `TIE` | 367 |
| `INSUFFICIENT` | 485 |
| `SAME_Q_BEST` percentage | 91.40% |

Conflict population only:

| Result | Count |
| --- | ---: |
| eligible Q | 7,261 |
| `SAME_Q_BEST` | 6,807 |
| `PREVIOUS_Q_BEST` | 37 |
| `NEXT_Q_BEST` | 20 |
| `TIE` | 354 |
| `INSUFFICIENT` | 43 |
| `SAME_Q_BEST` percentage | 93.75% |

This is the central finding: most current conflicts still look more like their same FY/FQ V3 quarter than adjacent quarters.

## Field Identity Quality

High-value identity fields:

- revenue
- gross_profit
- operating_income
- net_income
- operating_cashflow

Supporting fields:

- capex
- cash

Semantically unstable or unsuitable as identity veto fields:

- EBIT
- EBITDA
- free_cashflow
- total_debt
- shares_outstanding

EBIT has high numeric agreement in this run, but it remains classified as semantically risky because its availability/definition is less stable as a cross-provider identity veto.

## Alternative Fingerprints

| Fingerprint | Strong same-quarter | Probable | Conflict | Insufficient |
| --- | ---: | ---: | ---: | ---: |
| current Phase 3B-DIAG classifier | 2,111 `STRONG_MATCH` | 842 `PROBABLE_MATCH` | 7,261 `CONFLICT` | 389 |
| basic reported income | 6,253 | 2,088 | 1,462 | 819 |
| conservative cross-statement | 2,864 | 4,501 | 2,771 | 486 |
| revenue anchored | 4,297 | 3,941 | 1,834 | 550 |
| semantic-risk excluded | 2,198 | 4,205 | 3,734 | 485 |

Tolerance study on semantic-risk-excluded fingerprint:

| Tolerance | Strong | Probable | Conflict | Insufficient |
| --- | ---: | ---: | ---: | ---: |
| 2% | 1,563 | 4,046 | 4,528 | 485 |
| 5% | 2,198 | 4,205 | 3,734 | 485 |
| 10% | 3,004 | 4,177 | 2,956 | 485 |

5% remains a reasonable identity-evidence tolerance. 10% materially loosens the gate and should be used only as diagnostic/probable evidence unless later validated further.

## Cumulative / YTD And Scale Effects

YTD-like indicators were most visible in cash-flow style fields:

- free_cashflow Q4: 214
- free_cashflow Q3: 198
- free_cashflow Q2: 131
- operating_cashflow Q3: 114
- operating_cashflow Q4: 113
- operating_cashflow Q2: 73
- capex Q4: 121
- capex Q3: 88
- capex Q2: 67

Scale or sign normalization patterns were detected in 200 conflict rows. These should be reviewed separately and should not be interpreted directly as wrong quarter mappings.

## Conflict Typology

| Typology | Count | % of conflicts |
| --- | ---: | ---: |
| `SAME_QUARTER_LIKELY_TOLERANCE_TOO_STRICT` | 5,856 | 80.65% |
| `SAME_QUARTER_LIKELY_PROVIDER_REVISION` | 648 | 8.92% |
| `POSSIBLE_WRONG_QUARTER_MAPPING` | 337 | 4.64% |
| `SCALE_OR_NORMALIZATION_PROBLEM` | 200 | 2.75% |
| `INSUFFICIENT_TO_DIAGNOSE` | 169 | 2.33% |
| `CLEAR_WRONG_QUARTER_MAPPING` | 51 | 0.70% |

Estimated true-or-possible wrong-quarter population:

`388 = 337 possible + 51 clear`

Estimated same-underlying-quarter likely population:

`6,504 = 5,856 tolerance + 648 provider revision`

That is 89.57% of the current `CONFLICT` population.

## Gate Assessment

The current gate is correctly safe but materially too strict for interpreting V2 quarter identity. It conflates identity confirmation with field-value equivalence and lets unstable fields contribute too much to rejection.

V2 FY/FQ mapping appears broadly trustworthy for most candidates, but not perfect. The 388 possible/clear mapping-risk rows must remain blocked or manually reviewed before enrichment.

## Recommended Revised Gate

Recommended Phase 3C identity gate:

1. Start from exact ticker + fiscal_year + fiscal_quarter.
2. Require compatible or explicitly reviewed period end.
3. Require revenue agreement when revenue exists on both sides.
4. Require at least two additional trusted fields or a positive same-Q versus adjacent-Q margin.
5. Trusted identity fields: revenue, gross_profit, operating_income, net_income, operating_cashflow, cash, total_debt.
6. Use 5% identity tolerance with a 10,000 near-zero absolute floor and sign-sensitive handling.
7. EBIT, EBITDA, FCF, and shares_outstanding can support identity but cannot veto identity alone.
8. Keep `SAME_QUARTER_CONFIRMED` separate from `FIELD_VALUE_EQUIVALENT`.
9. Never overwrite non-null Yahoo canonical values in initial Phase 3C.

## Phase 3C Fill Potential

| Field | Current gate | Revised gate |
| --- | ---: | ---: |
| revenue | 0 | 0 |
| EBITDA | 53 | 320 |
| free_cashflow | 0 | 3 |
| cash | 0 | 2 |
| total_debt | 5 | 12 |
| shares_outstanding | 0 | 0 |
| publish_date | 1 | 5 |

The revised gate materially improves EBITDA and modestly improves debt/publish-date fill potential, while preserving NULL-fill-only behavior.

## Legacy Implication

Legacy Phase 3D should use the same identity principle: same ticker and period/FY/FQ are candidate identity only. Legacy values should be fingerprint-confirmed before automatic canonical enrichment, especially for cash-flow and balance-sheet fields.

## Safety

| Check | Result |
| --- | --- |
| V3 writes | 0 |
| V2 writes | 0 |
| Legacy writes | 0 |
| provider/network calls | 0 |
| quick_check | `ok` |
| foreign_key_check rows | 0 |

Recommended next phase:

`MASTER PLAN PHASE 3C - V2 METADATA & VALUE ENRICHMENT`
