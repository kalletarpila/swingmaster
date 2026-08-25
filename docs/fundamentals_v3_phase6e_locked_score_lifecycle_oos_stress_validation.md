# Fundamentals V3 Phase 6E Locked Score + Lifecycle OOS Stress Validation

Classification: `FUNDAMENTALS_V3_PHASE6E_LOCKED_SCORE_LIFECYCLE_VALIDATED_READY_FOR_IMPLEMENTATION`

## Frozen Score Verification

- Model version: `V3_LEGACY2_FUNDAMENTAL_SCORE_V1`
- Expected fingerprint: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- Actual fingerprint: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- Match: `True`
- Total max: `100`
- Market-price-dependent components: `0`

Frozen groups:
- `BALANCE_SHEET_RESILIENCE` max `15`: BALANCE_SHEET_RESILIENCE=15 (conditional net-debt/debt-service/runway metric without market cap)
- `CASH_FLOW_QUALITY` max `15`: CASH_QUALITY=15 (average of OCF/EBIT and FCF/OCF when meaningful)
- `CONSISTENCY` max `10`: CONSISTENCY=10 (8-quarter persistence/dispersion composite)
- `DILUTION` max `5`: DILUTION=5 (12-month endpoint shares_outstanding change)
- `GROWTH_EARNINGS_DEVELOPMENT` max `25`: REVENUE_GROWTH=8 (TTM Revenue_t vs TTM Revenue_t-4); EBIT_TRANSITION=10 (signed transition TTM EBIT_t vs TTM EBIT_t-4); FCF_TRANSITION=7 (signed transition TTM FCF_t vs TTM FCF_t-4)
- `MARGIN_DEVELOPMENT_TREND` max `15`: EBIT_MARGIN_TREND=10 (EBIT margin_t - EBIT margin_t-4); FCF_MARGIN_TREND=5 (FCF margin_t - FCF margin_t-4)
- `PROFITABILITY_LEVEL` max `15`: EBIT_MARGIN=8 (TTM EBIT / TTM Revenue); FCF_MARGIN=7 (TTM FCF / TTM Revenue)

## Frozen Lifecycle Verification

- Model version: `V3_LIFECYCLE_EBIT_FIRST_V1`
- Expected fingerprint: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`
- Actual fingerprint: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`
- Match: `True`

## No Leakage / Retuning

2026 and 2020 were not used to set score mappings, score thresholds, formulas, weights, applicability, lifecycle thresholds, lifecycle states, or hysteresis. The validation applies frozen artifacts unchanged.

## 2026 OOS

- Observations: `4360`
- Companies: `2496`
- Score-ready: `3709`
- Score distribution: `{'sample': 'OOS_2026_YTD', 'observations': 3709, 'min': 1.515152, 'max': 91.0, 'mean': 51.99500788163925, 'std': 15.75196474472418, 'skewness': -0.09729917465484658, 'p1': 16.470588, 'p5': 25.0, 'p10': 30.588235, 'p25': 41.25, 'p50': 52.173913, 'p75': 63.0, 'p90': 72.83229820000001, 'p95': 78.0, 'p99': 85.0, 'ready': 3709, 'total': 4360}`
- Score churn: `{'sample': 'OOS_2026_YTD', 'sequential_changes': 1626, 'median_abs_change': 3.0, 'p75_abs_change': 5.882353000000002, 'p90_abs_change': 11.0, 'major_jumps_gt_25': 20}`
- Lifecycle churn: `{'sample': 'OOS_2026_YTD', 'model': 'final_state', 'transitions': 1864, 'self_transition_rate': 96.40557939914163, 'transition_rate': 3.594420600858369, 'direct_jump_rate': 2.3605150214592276, 'immediate_reversal_count': 1, 'reversal_rate': 0.0536480686695279, 'median_state_duration': 2, 'one_quarter_state_share': 29.925868123293014, 'phase6d_self_transition_reference': 85.07, 'phase6d_transition_reference': 14.93, 'phase6d_median_duration_reference': 4.0, 'phase6d_one_quarter_state_reference': 11.4, 'classification': 'plausible regime behavior'}`

## 2020 Stress

- Observations: `6819`
- Companies: `1989`
- Score-ready: `3914`
- Score distribution: `{'sample': 'STRESS_2020', 'observations': 3914, 'min': 3.076923, 'max': 90.0, 'mean': 50.044871556463974, 'std': 15.884110600535324, 'skewness': -0.28926215348546575, 'p1': 13.039645020000002, 'p5': 22.5, 'p10': 28.0, 'p25': 38.85448875, 'p50': 51.25, 'p75': 62.0, 'p90': 70.0, 'p95': 75.0, 'p99': 81.0, 'ready': 3914, 'total': 6819}`
- Lifecycle churn: `{'sample': 'STRESS_2020', 'model': 'final_state', 'transitions': 4830, 'self_transition_rate': 88.9648033126294, 'transition_rate': 11.0351966873706, 'direct_jump_rate': 9.026915113871635, 'immediate_reversal_count': 2318, 'reversal_rate': 47.99171842650104, 'median_state_duration': 3.0, 'one_quarter_state_share': 21.094369547977795, 'phase6d_self_transition_reference': 85.07, 'phase6d_transition_reference': 14.93, 'phase6d_median_duration_reference': 4.0, 'phase6d_one_quarter_state_reference': 11.4, 'classification': 'plausible regime behavior'}`

## Decision

- Score health: `EXPECTED_ECONOMIC_BEHAVIOR`
- Lifecycle health: `EXPECTED_ECONOMIC_BEHAVIOR`
- Production writes: `{'score': 0, 'valuation': 0, 'lifecycle': 0, 'ttm': 0, 'canonical': 0}`

Next: `MASTER PLAN PHASE 6F - VALUATION ENGINE IMPLEMENTATION`
