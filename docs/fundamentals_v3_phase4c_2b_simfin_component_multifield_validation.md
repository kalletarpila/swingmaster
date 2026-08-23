# Fundamentals V3 Phase 4C-2B SimFin Component & Multi-Field Validation

Classification: `FUNDAMENTALS_V3_PHASE4C2B_SIMFIN_VALIDATION_COMPLETE_SEC_COMPONENT_ACQUISITION_STILL_REQUIRED`

Artifact root: `temp/fundamentals_v3_phase4c_2b_simfin_validation/20260823T_PHASE4C2B_SIMFIN_VALIDATION`

## Result

SimFin is useful as a normalized secondary validation and direct-recovery source for approved fields, but it does not eliminate the need for SEC component acquisition for residual EBIT/EBITDA semantics.

Canonical financial writes: `0`

Metadata writes: `0`

## Baseline

- Companies: 2550
- Canonical Q: 73075
- Core-ready: 44695
- EBIT missing: 7032
- EBITDA missing: 18179

## SimFin Files

- balance: 49560 rows, 3726 tickers, periods Q1|Q2|Q3|Q4, duplicates 0
- cashflow: 49562 rows, 3725 tickers, periods Q1|Q2|Q3|Q4, duplicates 0
- income: 49563 rows, 3725 tickers, periods Q1|Q2|Q3|Q4, duplicates 0

Flow rows are classified as `STANDALONE_QUARTER`. The local quarterly files contain only Q1-Q4 rows, so FY/YTD annual reconciliation is `NOT_AVAILABLE_NO_FY_ROWS`.

## Identity

- Exact FY/FQ matches: 3021
- Period-compatible matches: 24235
- Unresolved SimFin rows: 22318
- Identity conflicts: 0
- Wrong-quarter evidence: 0

Special tickers from prior exception phases were regression-checked without reopening fiscal identity.

## Field Policy

- capex: VALIDATION_ONLY; observations=26219; within_1pct/exact=0.7128; within_5pct/3day=0.7880; material_mismatch=0.1704
- cash: VALIDATION_ONLY; observations=27154; within_1pct/exact=0.6673; within_5pct/3day=0.6988; material_mismatch=0.3001
- free_cashflow: VALIDATION_ONLY; observations=26196; within_1pct/exact=0.7151; within_5pct/3day=0.8028; material_mismatch=0.1813
- gross_profit: APPROVED_CONDITIONALLY; observations=23042; within_1pct/exact=0.9470; within_5pct/3day=0.9539; material_mismatch=0.0451
- net_income: VALIDATION_ONLY; observations=27235; within_1pct/exact=0.9140; within_5pct/3day=0.9267; material_mismatch=0.0685
- net_income_common: VALIDATION_ONLY; observations=27235; within_1pct/exact=0.8721; within_5pct/3day=0.8972; material_mismatch=0.0968
- operating_cashflow: VALIDATION_ONLY; observations=27222; within_1pct/exact=0.9012; within_5pct/3day=0.9250; material_mismatch=0.0716
- operating_income: VALIDATION_ONLY; observations=27201; within_1pct/exact=0.6953; within_5pct/3day=0.7829; material_mismatch=0.2054
- publish_date: VALIDATION_ONLY; observations=27256; within_1pct/exact=0.5765; within_5pct/3day=0.8028; material_mismatch=0.1972
- revenue: APPROVED_CONDITIONALLY; observations=25473; within_1pct/exact=0.9562; within_5pct/3day=0.9642; material_mismatch=0.0340
- shares_outstanding: NOT_APPROVED_FOR_CANONICAL_SHARES; observations=27110; within_1pct/exact=0.5794; within_5pct/3day=0.8292; material_mismatch=0.1654
- total_debt: VALIDATION_ONLY; observations=21623; within_1pct/exact=0.6967; within_5pct/3day=0.7571; material_mismatch=0.2409

## Formula Results

- STRONG EBIT fingerprints: 79
- CONDITIONAL EBIT fingerprints: 428
- PROXY EBIT fingerprints: 90
- REJECTED EBIT fingerprints: 674
- STRONG EBITDA fingerprints: 51
- CONDITIONAL EBITDA fingerprints: 292
- PROXY EBITDA fingerprints: 364
- REJECTED EBITDA fingerprints: 675
- Strong EBIT dry fills: 4
- Strong EBITDA dry fills: 3
- SEC component acquisition still needed: True

Income-statement D&A and cash-flow D&A do not agree closely enough for broad automatic semantics. EBITDA derivation therefore remains company-specific and metadata-controlled.

## Recovery

Phase 4C-4 SimFin multi-field plan rows: 53

- capex: safe dry fills 0 of 5365 missing; remaining 5365
- cash: safe dry fills 0 of 2065 missing; remaining 2065
- ebit: safe dry fills 0 of 7032 missing; remaining 7032
- ebitda: safe dry fills 0 of 18179 missing; remaining 18179
- free_cashflow: safe dry fills 0 of 4661 missing; remaining 4661
- gross_profit: safe dry fills 35 of 16489 missing; remaining 16454
- net_income: safe dry fills 0 of 2408 missing; remaining 2408
- operating_cashflow: safe dry fills 0 of 695 missing; remaining 695
- operating_income: safe dry fills 0 of 2102 missing; remaining 2102
- publish_date: safe dry fills 0 of 3472 missing; remaining 3472
- revenue: safe dry fills 18 of 5150 missing; remaining 5132
- shares_outstanding: safe dry fills 0 of 5450 missing; remaining 5450
- total_debt: safe dry fills 0 of 17614 missing; remaining 17614

Core-ready estimate after strong/dry SimFin application: 44702 (+7).

## Source Architecture

SimFin role: `VALIDATION_SOURCE|SECONDARY_DIRECT_RECOVERY_SOURCE|PRIMARY_NORMALIZED_COMPONENT_SOURCE|FORMULA_CALIBRATION_SOURCE`

Future SEC role: `SEC_REQUIRED_FOR_RESIDUALS_AND_SEMANTIC_VALIDATION`

Next: `MASTER PLAN PHASE 4C-3 - FORMULA METADATA & EBIT/EBITDA PRODUCTION APPLY; THEN PHASE 4C-4 - SIMFIN MULTI-FIELD PRODUCTION RECOVERY`
