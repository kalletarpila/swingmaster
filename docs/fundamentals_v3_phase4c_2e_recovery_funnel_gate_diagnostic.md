# Fundamentals V3 Phase 4C-2E Recovery Funnel Gate Diagnostic

Classification: `FUNDAMENTALS_V3_PHASE4C2E_RECOVERY_DIAGNOSTIC_COMPLETE_GATE_REFINEMENT_REQUIRED`

Canonical writes: `0`

Metadata writes: `0`

## Reproduction

- EBIT missing: 7032
- EBITDA missing: 18179
- Quarterization-ready EBIT, current definition: 3904
- Canonical EBIT + D&A, current definition: 11895
- AUTO_STRONG EBIT: 0
- AUTO_STRONG EBITDA: 444

The prompt reference component-ready counts differ from the current committed 4C-2D definition, so the diagnostic treats that as metric-definition drift rather than a production mutation.

## Root Cause

`RESULT_B`: 444 is artificially low due to overstrict gates. No production planner bug was found.

`ZERO_EBIT_FILLS_IS_GATE_ARTIFACT`: EBIT has usable SEC component rows, but the current strict fingerprint gates do not approve target-NULL rows.

## EBIT Funnel

- Missing EBIT rows: 7032
- CIK available: 6714
- Pretax available: 4860
- Interest available: 4775
- Pretax + interest same Q: 3904
- Known EBIT target history: 3288
- Enough train observations: 922
- Enough test observations: 479
- Q-specific approval pass: 4
- AUTO_STRONG: 0

Top EBIT exclusions:

- TEST_SAMPLE_TOO_SMALL: 2809
- NO_PRETAX: 1854
- NO_INTEREST: 956

## EBITDA Funnel

- Missing EBITDA rows: 18179
- Canonical EBIT available: 15561
- SEC D&A available: 11895
- Known EBITDA target history: 11867
- D&A/EBITDA relationship calibrated: 11748
- Q-specific approval pass: 456
- AUTO_STRONG: 444

Top EBITDA exclusions:

- TEST_SAMPLE_TOO_SMALL: 4040
- NO_DA: 3225
- NO_CANONICAL_EBIT: 2538

## D&A-Only Diagnostic

- Companies evaluated: 2369
- STRONG D&A fingerprints: 3
- CONDITIONAL D&A: 114
- STRONG_SEMANTIC_LOW_SAMPLE D&A: 10
- Rejected D&A: 3144
- Recoverable missing EBITDA with canonical EBIT + STRONG D&A: 12

## Counterfactuals

- CURRENT_STRICT: EBIT 0, EBITDA 444, core uplift 444
- LOWER_SAMPLE_ONLY: EBIT 0, EBITDA 79, core uplift 79
- RANGE_EXTENSION_ONLY: EBIT 1, EBITDA 553, core uplift 553
- Q1_Q3_GROUPED: EBIT 0, EBITDA 0, core uplift 0
- DA_ONLY_PATH: EBIT 0, EBITDA 12, core uplift 12
- EVIDENCE_BASED_REFINED_GATE: EBIT 1, EBITDA 632, core uplift 632

## Implementation Audit

No NULL-target planner bug, formula lookup bug, status propagation bug, direct/formula de-dup bug, or Q-status mapping bug was found. The issue is gate architecture plus metric-definition drift, not a production mutation bug.

## Recommended Gate Architecture

- Keep Q4 separate.
- Add a distinct D&A-only path for canonical EBIT + validated SEC D&A.
- Evaluate `STRONG_SEMANTIC_LOW_SAMPLE` where official SEC concepts are direct, unambiguous, and all observed provider comparisons match strictly.
- Revalidate validity-range extension when exact component mapping is stable outside the calibration interval.

## Next

`MASTER PLAN PHASE 4C-2F - FORMULA GATE REFINEMENT & RECOVERY REVALIDATION`
