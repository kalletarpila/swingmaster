# Fundamentals V3 Phase 6C-R Fundamental Score Architecture Reconciliation

Classification: `FUNDAMENTALS_V3_PHASE6CR_LEGACY2_SCORE_RECONCILED_READY_FOR_PHASE6E`

Legacy 2.0 is a valuation-independent 0-100 fundamental-state score. It is not a timing model, not a valuation model, and contains no market-price or market-cap inputs.

## Architecture

- Legacy total: `95`
- Phase 6C total: `120`
- Legacy 2.0 total: `100`
- Phase 6C valuation components removed: `EV_EBIT`, `FCF_YIELD`, `EV_SALES`, `NET_DEBT_TO_MARKET_CAP`
- Market-price-dependent Legacy 2.0 components: `0`

## Time Split

- Development / fitting: `2021-01-01 through 2023-12-31`
- 2024: validation only, no refinement
- 2025: locked OOS, no retuning
- 2026: not inspected
- 2020: not used

## Coverage

- Development observations: `25295`
- Development companies: `2436`
- 2024 validation observations: `5964`
- 2025 OOS observations: `4669`
- 2025 OOS companies: `2463`
- Score-ready development rows: `18398`
- Score-ready 2024 rows: `4637`
- Score-ready 2025 rows: `3305`

## Frozen Model

- Version: `V3_LEGACY2_FUNDAMENTAL_SCORE_V1`
- Fingerprint: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- Phase 6D lifecycle used only for bias grouping: `V3_LIFECYCLE_EBIT_FIRST_V1` / `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`

## Safety

Production writes: `{'score': 0, 'valuation': 0, 'lifecycle': 0, 'ttm': 0, 'canonical': 0}`.

Next: `MASTER PLAN PHASE 6E - LOCKED SCORE + LIFECYCLE OUT-OF-SAMPLE & STRESS VALIDATION`
