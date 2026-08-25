# Fundamentals V3 Phase 6G Legacy 2.0 Fundamental Score Engine Implementation

Classification: `FUNDAMENTALS_V3_PHASE6G_LEGACY2_SCORE_ENGINE_IMPLEMENTED_READY_FOR_PHASE6H`

## Frozen Verification

- Model version: `V3_LEGACY2_FUNDAMENTAL_SCORE_V1`
- Expected fingerprint: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- Actual fingerprint: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- Match: `True`
- Total max: `100`
- Market-price inputs: `0`

## Frozen Groups

- `BALANCE_SHEET_RESILIENCE` max `15`: `BALANCE_SHEET_RESILIENCE`
- `CASH_FLOW_QUALITY` max `15`: `CASH_QUALITY`
- `CONSISTENCY` max `10`: `CONSISTENCY`
- `DILUTION` max `5`: `DILUTION`
- `GROWTH_EARNINGS_DEVELOPMENT` max `25`: `REVENUE_GROWTH|EBIT_TRANSITION|FCF_TRANSITION`
- `MARGIN_DEVELOPMENT_TREND` max `15`: `EBIT_MARGIN_TREND|FCF_MARGIN_TREND`
- `PROFITABILITY_LEVEL` max `15`: `EBIT_MARGIN|FCF_MARGIN`

## Persistence

- Table: `v3_score`
- Unique identity: `(company_id, as_of_quarter_id, score_model_version)`
- TTM lineage: `endpoint_ttm_id`
- Stored detail: group scores, component scores, component statuses, coverage, confidence, applicability and fingerprints.

## Historical Dry Run

- Endpoints: `54038`
- Applicable: `54038`
- Score-ready: `34898`
- NOT_READY: `19140`
- NOT_APPLICABLE: `0`
- Median coverage: `85.0`
- Score min/max: `1.515152` / `91.0`
- Score P10/P25/P50/P75/P90: `29.0` / `40.0` / `52.0` / `62.0` / `71.0`

## Parity And Safety

- Parity mismatches: `0`
- Score fingerprint unchanged: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- Lifecycle fingerprint unchanged: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`
- Valuation engine unchanged: `True`
- Production writes: `{'canonical': 0, 'ttm': 0, 'lifecycle': 0, 'valuation': 0, 'score': 0}`

Full production score population remains deferred to Phase 6I.

Next: `MASTER PLAN PHASE 6H - LIFECYCLE ENGINE IMPLEMENTATION`
