# Fundamentals V3 Phase 6C Score Distributions & Point Calibration

Classification: `FUNDAMENTALS_V3_PHASE6C_SCORE_DISTRIBUTIONS_CALIBRATED_READY_FOR_PHASE6D`

Calibration uses only `2021-01-01 through 2025-12-31`. 2026 and 2020 were not used for fitting.

## Population

- Observations: `35928`
- Companies: `2532`
- Yearly observations: `{'2021': 8018, '2022': 8644, '2023': 8633, '2024': 5964, '2025': 4669}`

## Locked Components

Final primary components: `REVENUE_GROWTH, EBIT_GROWTH_TRANSITION, FCF_GROWTH_TRANSITION, EBIT_MARGIN, FCF_MARGIN, EV_EBIT, FCF_YIELD, EV_SALES, NET_DEBT_TO_MARKET_CAP`.

Every scalar score mapping defines the full integer score range. Theoretical dead score values: `0`.

## Stability

Stability classifications: `{"EBIT_GROWTH_TRANSITION": "REGIME_SENSITIVE", "EBIT_MARGIN": "MODERATELY_SHIFTING", "EV_EBIT": "STABLE", "EV_SALES": "MODERATELY_SHIFTING", "FCF_GROWTH_TRANSITION": "REGIME_SENSITIVE", "FCF_MARGIN": "MODERATELY_SHIFTING", "FCF_YIELD": "MODERATELY_SHIFTING", "NET_DEBT_TO_MARKET_CAP": "MODERATELY_SHIFTING", "REVENUE_GROWTH": "REGIME_SENSITIVE"}`.

## Aggregate Diagnostic

- Score-ready observations: `29399`
- Score-ready companies: `2291`

## Freeze

Calibration fingerprint: `d6c703e5b4b40eb8b54d5d2c70feb923af4d99b6d37d2a8a7ff42ee1ceced8a3`

## Safety

Production writes: `{'score': 0, 'valuation': 0, 'lifecycle': 0, 'ttm': 0, 'canonical': 0}`.

Next: `MASTER PLAN PHASE 6D - LIFECYCLE RECALIBRATION`
