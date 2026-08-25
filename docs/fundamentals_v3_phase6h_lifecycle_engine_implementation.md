# Fundamentals V3 Phase 6H Lifecycle Engine Implementation

Classification: `FUNDAMENTALS_V3_PHASE6H_LIFECYCLE_ENGINE_IMPLEMENTED_READY_FOR_PHASE6I`

## Frozen Model

- Model version: `V3_LIFECYCLE_EBIT_FIRST_V1`
- Expected fingerprint: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`
- Actual fingerprint: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`
- Match: `True`
- States: `DISTRESS_CONTRACTION, EARLY_RECOVERY, POSITIVE_INFLECTION, PROFITABLE_GROWTH, HIGH_GROWTH_EXPANSION, MATURE_STABLE, DECELERATING, DECLINING, NOT_READY`
- EBIT primary: `True`
- EBITDA required: `False`
- Score inputs: `False`
- Valuation inputs: `False`

## Feature And State Rules

Revenue growth is TTM YoY against the same fiscal quarter t-4. EBIT signed transition and EBIT margin are required. FCF transition and FCF margin are optional confidence features. Margin changes are percentage-point differences, not relative growth across zero.

Raw state selection uses the frozen Phase 6D precedence. Final state is temporal: company histories are processed in fiscal order, normal transitions require two consecutive raw candidates, minimum state age is 1, and hard EBIT inflections or severe revenue contraction bypass confirmation.

## Persistence

- Table: `v3_lifecycle`
- Identity: `(company_id, endpoint_ttm_id, lifecycle_model_version)`
- Required lineage: `ttm_id`, endpoint quarter, fiscal year/quarter, period end, publish date, previous lifecycle endpoint/state.
- Production backfill owner: Phase 6I

## Historical Dry Run

- Endpoints: `54038`
- Lifecycle-ready: `33927`
- NOT_READY: `20111`
- State counts: `{"DECELERATING": 1435, "DECLINING": 4672, "DISTRESS_CONTRACTION": 6238, "EARLY_RECOVERY": 1190, "HIGH_GROWTH_EXPANSION": 507, "MATURE_STABLE": 7369, "NOT_READY": 22170, "POSITIVE_INFLECTION": 5068, "PROFITABLE_GROWTH": 5389}`
- Self-transition rate: `85.31205095343508`
- Transition rate: `14.687949046564915`
- Median state duration: `4.0`
- One-quarter state share: `8.31353919239905`
- Reversal rate: `67.53466154025399`

## Safety

- Parity mismatches: `0`
- Production writes: `{'canonical': 0, 'ttm': 0, 'score': 0, 'valuation': 0, 'lifecycle': 0}`
- Legacy 2.0 score fingerprint unchanged: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- Valuation engine unchanged: `V3_VALUATION_POST_PUBLICATION_SNAPSHOT_V1`

Implementation findings: persistent snapshots require lineage fields such as `ttm_id` to survive intermediate dataset builders; tests must use frozen contracts rather than transient calibration mappings; frozen semantics are not changed to satisfy tests.

Next: `MASTER PLAN PHASE 6I - PRODUCTION REBUILD & PROVING`
