# Fundamentals V3 Phase 4C EBIT & EBITDA Derivation Research

Classification: `FUNDAMENTALS_V3_PHASE4C_EBIT_EBITDA_RESEARCH_COMPLETE_READY_FOR_PRODUCTION_APPLY`

Artifact root: `temp/fundamentals_v3_phase4c_ebit_ebitda_derivation/20260823T_PHASE4C_EBIT_EBITDA_DERIVATION`

## Policy Decisions

- Canonical EBIT: earnings before interest and taxes; direct EBIT evidence only in the current production plan.
- EBIT = Operating Income: `NOT_APPROVED` with 66019 observations, 86.24% within 1%, 89.73% within 5%.
- Canonical EBITDA: earnings before interest, taxes, depreciation and amortization; adjusted EBITDA is rejected.
- EBITDA = EBIT + D&A: `NOT_APPROVED` with 46933 observations, 67.30% within 1%, 77.60% within 5%.
- EBITDA = Operating Income + D&A: `NOT_APPROVED` with 51264 observations, 60.83% within 1%, 72.73% within 5%.
- Q4 EBIT reconstruction: `NOT_APPROVED`.
- Q4 EBITDA reconstruction: `NOT_APPROVED`.
- Q4 D&A reconstruction: `APPROVED_CONDITIONALLY`.

## External Semantic Evidence

- SEC Division of Corporation Finance non-GAAP C&DI 103.01 defines EBIT as earnings before interest and taxes and EBITDA as earnings before interest, taxes, depreciation and amortization, with earnings meaning GAAP net income. Source: https://www.sec.gov/rules-regulations/staff-guidance/corporation-finance-interpretations/non-gaap-financial-measures
- The same SEC guidance states that measures calculated differently should not be characterized as EBIT or EBITDA; Phase 4C therefore rejects adjusted EBITDA add-backs as canonical inputs without separate proof.

## Baseline

- Companies: 2550
- Canonical Qs: 73075
- EBIT missing: 7032
- EBITDA missing: 18179
- Phase 4C inventory rows: 22593

## Candidate Population

`phase4c_production_derivation_plan.csv` contains 252 dry-plan rows. Production writes were not performed.

Expected core-ready uplift from the dry plan: 0 rows.

Next: `MASTER PLAN PHASE 4C-APPLY - EBIT & EBITDA PRODUCTION DERIVATION`
