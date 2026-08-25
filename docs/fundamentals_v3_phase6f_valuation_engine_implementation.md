# Fundamentals V3 Phase 6F Valuation Engine Implementation

Classification: `FUNDAMENTALS_V3_PHASE6F_VALUATION_ENGINE_IMPLEMENTED_READY_FOR_PHASE6G`

V3 valuation snapshots are separate from Legacy 2.0 fundamental score. The score fingerprint remains `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0` and no valuation metric is added to the 0-100 score.

## Persistent Snapshot Policy

For each eligible published fundamental / TTM endpoint, calculate and store valuation using the first actual trading day strictly after `publish_date` and that day's `close` price. The snapshot is immutable for the endpoint/model version and is not recalculated with current or latest price.

## Schema And Identity

- Table: `v3_valuation`
- Unique key: `(company_id, endpoint_ttm_id, model_version)`
- Model version: `V3_VALUATION_POST_PUBLICATION_SNAPSHOT_V1`
- Price source: `RAWCANDLE_OSAKEDATA_CLOSE`
- Existing rows: `0`
- Migration required for current production DB before apply: `1`

## Formulas

- Market Cap = `valuation_close_price * shares_outstanding`
- Net Debt = `total_debt - cash`
- Enterprise Value = `market_cap + total_debt - cash`
- EV/EBIT, EBIT Yield, FCF Yield, EV/Sales, EV/EBITDA, P/E and EV/OCF store numeric values only when economically meaningful.

## Statuses

`VALID`, `MISSING_INPUT`, `NOT_MEANINGFUL`, `NOT_APPLICABLE`, `MISSING_PUBLISH_DATE`, `MISSING_TARGET_DAY_PRICE`, `PENDING_PRICE_DATE`.

## Historical Dry Run

- TTM endpoints: `54038`
- With publish_date: `50734`
- Missing publish_date: `3304`
- Valuation dates resolved: `50734`
- Target prices available: `50614`
- Calculable snapshots: `43754`
- EV/EBIT valid: `24930`
- FCF Yield valid: `31228`
- EV/Sales valid: `38553`
- EV/EBITDA valid: `25973`
- P/E valid: `30062`
- Missing target price: `120`

## Production Safety

Phase 6F ran production data read-only and did not perform historical valuation backfill. Phase 6I remains responsible for authoritative production population/proving.

Production writes: `{'canonical': 0, 'ttm': 0, 'score': 0, 'lifecycle': 0, 'valuation': 0}`.

Next: `MASTER PLAN PHASE 6G - LEGACY 2.0 FUNDAMENTAL SCORE ENGINE IMPLEMENTATION`
