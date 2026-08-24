# Fundamentals V3 Phase 6D Lifecycle Recalibration

Classification: `FUNDAMENTALS_V3_PHASE6D_LIFECYCLE_RECALIBRATED_READY_FOR_PHASE6E`

Lifecycle was recalibrated from 2021-2025 company + TTM endpoint observations using raw economic trajectory features. No score buckets, valuation metrics, 2026 outputs, 2020 outputs, or EBITDA-required inputs were used for calibration.

## Population

- Observations: `35928`
- Companies: `2532`
- Yearly observations: `{'2021': 8018, '2022': 8644, '2023': 8633, '2024': 5964, '2025': 4669}`
- Lifecycle-ready observations: `25894`
- NOT_READY observations: `10034`

## Model

- Version: `V3_LIFECYCLE_EBIT_FIRST_V1`
- Fingerprint: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`
- Old states: `8`
- Final observed states: `9`

## Churn

- Raw: `{'model': 'raw_state', 'transitions': 33396, 'self_transition_rate': 75.64079530482692, 'transition_rate': 24.359204695173077, 'direct_jump_rate': 16.70559348424961, 'immediate_reversal_count': 19526, 'reversal_rate': 58.46808000958199, 'median_state_duration': 2, 'one_quarter_state_share': 39.03628011624637}`
- Final: `{'model': 'final_state', 'transitions': 33396, 'self_transition_rate': 85.0700682716493, 'transition_rate': 14.9299317283507, 'direct_jump_rate': 11.091148640555755, 'immediate_reversal_count': 21813, 'reversal_rate': 65.31620553359684, 'median_state_duration': 4.0, 'one_quarter_state_share': 11.399308326682627}`

## States

[
  {
    "calibration_observations": 4981,
    "economic_meaning": "weak/contracting revenue with negative or deteriorating EBIT/FCF",
    "key_entry_conditions": "severe revenue contraction <= -0.1901 with deteriorating/crossing-negative EBIT, or deeply negative margins",
    "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs",
    "share_pct": 13.863838788688486,
    "state": "DISTRESS_CONTRACTION",
    "state_id": 1
  },
  {
    "calibration_observations": 977,
    "economic_meaning": "still weak level but EBIT/FCF trajectory improving",
    "key_entry_conditions": "negative-but-improving EBIT or nonpositive EBIT margin expanding",
    "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs",
    "share_pct": 2.7193275439768425,
    "state": "EARLY_RECOVERY",
    "state_id": 2
  },
  {
    "calibration_observations": 4124,
    "economic_meaning": "EBIT or FCF crossed positive",
    "key_entry_conditions": "EBIT or FCF crosses positive",
    "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs",
    "share_pct": 11.47851258071699,
    "state": "POSITIVE_INFLECTION",
    "state_id": 3
  },
  {
    "calibration_observations": 4383,
    "economic_meaning": "positive EBIT, growing revenue, stable or expanding margins",
    "key_entry_conditions": "revenue growth >= 0.2505, positive/growing EBIT, noncontracting margin",
    "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs",
    "share_pct": 12.19939879759519,
    "state": "PROFITABLE_GROWTH",
    "state_id": 4
  },
  {
    "calibration_observations": 459,
    "economic_meaning": "very strong revenue expansion with positive EBIT",
    "key_entry_conditions": "revenue growth >= 0.5967 and EBIT margin > 0",
    "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs",
    "share_pct": 1.277555110220441,
    "state": "HIGH_GROWTH_EXPANSION",
    "state_id": 5
  },
  {
    "calibration_observations": 5881,
    "economic_meaning": "positive profitability with lower/stable growth",
    "key_entry_conditions": "EBIT margin >= 0.1220 with revenue growth >= -0.0205",
    "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs",
    "share_pct": 16.36884880872857,
    "state": "MATURE_STABLE",
    "state_id": 6
  },
  {
    "calibration_observations": 1176,
    "economic_meaning": "profitable but weakening growth or margin contraction",
    "key_entry_conditions": "positive EBIT with margin change <= -0.0447 or weak revenue",
    "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs",
    "share_pct": 3.2732130928523713,
    "state": "DECELERATING",
    "state_id": 7
  },
  {
    "calibration_observations": 3297,
    "economic_meaning": "negative revenue/profit trajectory or crossing negative",
    "key_entry_conditions": "EBIT crosses negative or revenue/margin trajectory both deteriorate",
    "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs",
    "share_pct": 9.176686706746827,
    "state": "DECLINING",
    "state_id": 8
  },
  {
    "calibration_observations": 10650,
    "economic_meaning": "insufficient core lifecycle features",
    "key_entry_conditions": "missing revenue trajectory or EBIT trajectory/margin",
    "key_exit_conditions": "exit after another state condition is confirmed or hard inflection occurs",
    "share_pct": 29.642618570474284,
    "state": "NOT_READY",
    "state_id": 9
  }
]

## Safety

Production writes: `{'lifecycle': 0, 'score': 0, 'valuation': 0, 'ttm': 0, 'canonical': 0}`.

Next: `MASTER PLAN PHASE 6E - OUT-OF-SAMPLE & STRESS VALIDATION`
