# Fundamentals V3 Phase 6B Score & Lifecycle Calibration Design

Classification: `FUNDAMENTALS_V3_PHASE6B_SCORE_LIFECYCLE_CALIBRATION_DESIGN_COMPLETE_READY_FOR_PHASE6C`

Phase 6B locks the score and lifecycle calibration framework before production score, valuation, or lifecycle writes.

## Windows

- Calibration: `2021-01-01 through 2025-12-31`
- OOS validation: `2026 YTD`
- Stress/robustness: `2020`
- 2018-2019: context only, not threshold fitting

## Existing Model

Existing score components found: `8`.

Sparse-scale components requiring recalibration: `8`.

Existing lifecycle states found: `8`. Current lifecycle is EBITDA-dependent and stateless, with no hysteresis or transition matrix.

## Locked Design

Primary score groups are Growth, Profitability/Quality, Valuation, and Balance Sheet/Risk. EBIT is the primary earnings metric. EBITDA is preserved only as secondary diagnostics.

Every scalar score component must use the full integer 0..N scale. Missing data, bad economic values, and not-meaningful ratios are distinct states.

Lifecycle is descriptive and trajectory-oriented. It is not an attractiveness score and must not be collapsed into the score.

## Safety

Production writes: `{'score': 0, 'lifecycle': 0, 'valuation': 0, 'ttm': 0, 'canonical': 0}`.

## Authoritative Phase 6 Plan

- Phase 6A - Downstream Inventory & Policy Lock: DONE
- Phase 6B - Score & Lifecycle Calibration Design: THIS PHASE
- Phase 6C - Score Distributions & Point Calibration
- Phase 6D - Lifecycle Recalibration
- Phase 6E - Out-of-Sample & Stress Validation
- Phase 6F - Valuation Engine Implementation
- Phase 6G - Score Engine Implementation
- Phase 6H - Lifecycle Engine Implementation
- Phase 6I - Production Rebuild & Proving
- Phase 6J - Phase 6 Closure


Next: `MASTER PLAN PHASE 6C - SCORE DISTRIBUTIONS & POINT CALIBRATION`
