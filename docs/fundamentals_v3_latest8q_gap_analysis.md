# Fundamentals V3 Latest8Q Gap Analysis

## Executive Summary

| Metric | Count | % |
| --- | ---: | ---: |
| Active tickers | 2470 | 100.0 |
| Already latest8Q fully clean | 92 | 3.7247 |
| Fully clean primary core | 92 | 3.7247 |
| All tracked fields complete | 76 | 3.0769 |
| Need local repair only | 596 | 24.1296 |
| Need external official evidence | 1717 | 69.5142 |
| Need structural/manual review | 496 | 20.081 |
| Legitimate <8Q history | 0 | 0.0 |
| Current-impact P1 | 877 | 35.5061 |

## Row Quality

- latest8Q total rows: `19728`
- fiscal-identity clean rows: `18599`
- primary-core complete rows: `11937`
- fully complete all-field rows: `10298`
- rows with one issue: `5798`
- rows with multiple issues: `3632`

## Issue Type Summary

| Issue | Rows |
| --- | ---: |
| DUPLICATE_ECONOMIC_QUARTER | 2 |
| FQ_CONFLICT_DIRECT_EXACT | 95 |
| FY_CONFLICT_DIRECT_EXACT | 587 |
| MISSING_QUARTER | 4127 |
| PERIOD_END_FISCAL_SLOT_CONFLICT | 96 |
| PERIOD_END_TRANSITION_REVIEW | 118 |
| PRIMARY_CORE_INCOMPLETE | 2865 |
| PUBLISH_BEFORE_OR_ON_PERIOD_END | 11 |
| PUBLISH_DATE_MISSING | 950 |
| PUBLISH_LATE_REVIEW | 24 |
| PUBLISH_SEQUENCE_CONFLICT | 78 |
| SECONDARY_FIELDS_INCOMPLETE | 3869 |
| TARGET_COLLISION | 763 |
| TRANSITION_REVIEW | 118 |
| TRANSITION_SEQUENCE | 118 |
| UNRESOLVED_BOUNDARY | 329 |
| UNRESOLVED_SEQUENCE | 329 |

## Top Evidence Needed

| Evidence needed | Rows |
| --- | ---: |
| NEED_MISSING_QUARTER_SOURCE | 4127 |
| NEED_GROSS_PROFIT | 2818 |
| NEED_DEBT | 1817 |
| NEED_SOURCE_SEMANTICS_CONFIRMATION | 1549 |
| NEED_EBITDA | 1386 |
| NEED_CAPEX | 1118 |
| NEED_FIRST_PUBLIC_RESULT_DATE | 1063 |
| NEED_EBIT | 886 |
| NEED_TARGET_COLLISION_RESOLUTION | 764 |
| NEED_LOCAL_LINEAGE_RECONCILIATION | 682 |

## Downstream

- TTM clean: `2199`
- TTM affected/unavailable: `271`
- Score available: `2044`
- Score blocked: `426`
- Lifecycle available: `1801`
- Lifecycle blocked: `669`
- Valuation available: `1726`
- Valuation blocked: `744`

## Full Closure Potential

- already fully clean: `92`
- repairable from local evidence only: `596`
- requiring external official evidence: `1717`
- requiring structural/manual decision: `496`
- legitimate <8Q history: `0`
- NO_MISSING_REQUIREMENT_IN_PLAN: `0`
- theoretical fully-clean after all identified repairs: `1974`
- theoretical fully-clean %: `79.919`
- external research facts required total: `6064`
- local repair actions required total: `2841`
- structural-review decisions required total: `1130`

## Artifacts

- quarter_level_csv: `temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F/latest8q_quarter_gap_detail.csv`
- ticker_level_csv: `temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F/latest8q_ticker_gap_summary.csv`
- local_repair_queue: `temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F/latest8q_local_repair_queue.csv`
- external_research_queue: `temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F/latest8q_external_research_queue.csv`
- structural_review_queue: `temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F/latest8q_structural_review_queue.csv`
- theoretical_closure_artifact: `temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F/latest8q_theoretical_closure_test.csv`
- known_13_artifact: `temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F/known_13_latest8q_gap_analysis.csv`
- summary_report: `docs/fundamentals_v3_latest8q_gap_analysis.md`

## Classification

`LATEST8Q_FULL_CLOSURE_MAP_COMPLETE_WITH_STRUCTURAL_DECISIONS`

## Next Action

RESOLVE LOCAL-ONLY CASES AND EXTERNAL EVIDENCE FIRST; KEEP ONLY TRUE TRANSITION / COLLISION / STRUCTURAL CASES FOR MANUAL DECISION
