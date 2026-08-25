# Fundamentals V3 Phase 6 Closure

Classification: `FUNDAMENTALS_V3_PHASE6_COMPLETE_READY_FOR_PHASE7_CHECK_V3`

## Purpose

Phase 6 completed the V3 derived-data layer on top of the Phase 5 TTM baseline: persistent valuation snapshots, the production fundamental score, lifecycle classification, production population, and the full pytest collection gate.

Phase 6 did not change canonical quarter data, TTM semantics, universe membership, RawCandle data, or production Check/Update cutover behavior.

## Phase Status

- Phase 6A - Downstream Inventory & Policy Lock: `DONE`
- Phase 6B - Score & Lifecycle Calibration Design: `DONE`
- Phase 6C - Score Distributions & Point Calibration: `DONE / SUPERSEDED FOR PRODUCTION SCORE`
- Phase 6C-R - Fundamental Score Architecture Reconciliation: `DONE`
- Phase 6D - Lifecycle Recalibration: `DONE`
- Phase 6E - Locked Score + Lifecycle OOS & Stress Validation: `DONE`
- Phase 6F - Valuation Engine Implementation: `DONE`
- Phase 6G - Legacy 2.0 Fundamental Score Engine Implementation: `DONE`
- Phase 6H - Lifecycle Engine Implementation: `DONE`
- Phase 6I - Production Rebuild & Proving: `DONE`
- Phase 6I test-gate closure / environment fix: `DONE`
- Phase 6J - Phase 6 Closure: `DONE`

## Final Architecture

Dependency direction is one way:

`Canonical quarters -> TTM -> Fundamental Score`

`Canonical quarters -> TTM -> Lifecycle`

`Canonical quarters -> TTM + publish_date + RawCandle target-date close -> Valuation`

Negative dependencies:

- Valuation does not feed the fundamental score.
- Fundamental score does not feed lifecycle.
- Lifecycle does not rewrite canonical quarters or TTM.

Persistent downstream snapshots preserve direct `ttm_id` lineage. Intermediate dataset builders must not drop `ttm_id`.

## Score

Production model: `V3_LEGACY2_FUNDAMENTAL_SCORE_V1`

Fingerprint: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`

Purpose: company fundamental strength, durability, quality, and development direction. The score is not valuation, technical timing, or a 30-90 day stock-return predictor.

Market-price-dependent score inputs: `0`

Total max score: `100`

Final groups and maxima:

- `GROWTH_EARNINGS_DEVELOPMENT`: `25`
- `PROFITABILITY_LEVEL`: `15`
- `MARGIN_DEVELOPMENT_TREND`: `15`
- `CASH_FLOW_QUALITY`: `15`
- `CONSISTENCY`: `10`
- `BALANCE_SHEET_RESILIENCE`: `15`
- `DILUTION`: `5`

Final subcomponents:

- `REVENUE_GROWTH`: `8`
- `EBIT_TRANSITION`: `10`
- `FCF_TRANSITION`: `7`
- `EBIT_MARGIN`: `8`
- `FCF_MARGIN`: `7`
- `EBIT_MARGIN_TREND`: `10`
- `FCF_MARGIN_TREND`: `5`
- `CASH_QUALITY`: `15`
- `CONSISTENCY`: `10`
- `BALANCE_SHEET_RESILIENCE`: `15`
- `DILUTION`: `5`

Production rows: `54038`

Score-ready: `34898`

NOT_READY: `19140`

Median coverage: `85.0`

Score distribution: min `1.515152`, P10 `29.0`, P25 `40.0`, P50 `52.0`, P75 `62.0`, P90 `71.0`, max `91.0`.

Phase 6C's original 120-point score is explicitly superseded for production score purposes by Phase 6C-R Legacy 2.0.

## Lifecycle

Production model: `V3_LIFECYCLE_EBIT_FIRST_V1`

Fingerprint: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`

Lifecycle is EBIT-first, stateful, hysteresis-aware, confirmation/persistence-aware, score-independent, and valuation-independent. EBITDA is not required.

Frozen states:

- `DISTRESS_CONTRACTION`
- `EARLY_RECOVERY`
- `POSITIVE_INFLECTION`
- `PROFITABLE_GROWTH`
- `HIGH_GROWTH_EXPANSION`
- `MATURE_STABLE`
- `DECELERATING`
- `DECLINING`
- `NOT_READY`

Production rows: `54038`

Lifecycle-ready: `33927`

NOT_READY: `20111`

Phase 6I lifecycle metrics: self-transition `85.31205095343508%`, transition `14.687949046564915%`, median state duration `4.0`, one-quarter state share `8.31353919239905%`.

## Valuation

Production model: `V3_VALUATION_POST_PUBLICATION_SNAPSHOT_V1`

Permanent policy: V3 valuation snapshots are stored persistently using the first trading day strictly after `publish_date` and that day's close price.

`valuation_date = first actual trading day strictly after publish_date`

Price source: `RAWCANDLE_OSAKEDATA_CLOSE`

Historical valuation snapshots are immutable derived facts for an endpoint/model version. They are not continuously recomputed from current price.

Production rows: `54038`

Status counts:

- `VALID`: `43754`
- `MISSING_PUBLISH_DATE`: `3304`
- `MISSING_TARGET_DAY_PRICE`: `120`
- `MISSING_INPUT`: `1787`
- `NOT_MEANINGFUL`: `5073`

Valid metric counts:

- EV/EBIT: `24930`
- FCF yield: `31228`
- EV/Sales: `38553`
- EV/EBITDA: `25973`
- P/E: `30062`

## Production Integrity

Read-only production verification:

- companies: `2550`
- active companies: `2482`
- inactive companies: `68`
- `v3_ttm`: `54038`
- `v3_valuation`: `54038`
- `v3_score`: `54038`
- `v3_lifecycle`: `54038`
- valuation duplicate identities: `0`
- score duplicate identities: `0`
- lifecycle duplicate identities: `0`
- valuation TTM orphans: `0`
- score TTM orphans: `0`
- lifecycle TTM orphans: `0`
- derived company orphans: `0`
- `PRAGMA quick_check`: `ok`

Phase 6J production writes: `0`

RawCandle writes: `0`

## Proving And Idempotency

Phase 6I production second run:

- valuation: `54038 NOOP`
- score: `54038 NOOP`
- lifecycle: `54038 NOOP`

Derived fingerprints run1 == run2. Canonical, TTM, and company source fingerprints remained unchanged during production proving.

## Test Evidence

- Phase 6F focused tests: `45 passed`
- Phase 6G focused tests: `74 passed`
- Phase 6H focused tests: `90 passed`
- Phase 6I targeted gate: `331 passed`
- Phase 6I-FIX focused tests: `19 passed`
- Phase 6I-FIX targeted Phase 5/6 gate: `686 passed`
- Full pytest collection after optional dependency fix: `3457 collected / 22 skipped / 0 errors`
- Full pytest execution after optional dependency fix: `3237 passed / 181 failed / 61 skipped / 0 errors`

The full suite did not pass completely. The remaining failures are known test debt, not a Phase 6 V3 regression.

## Known Debt

`TEST-001`: `KNOWN_PRE_EXISTING_REPORTED_VINTAGE_PIT_TEST_DEBT`

Older reported-vintage/PIT tests still expect write-enabled behavior. Current product policy disables vintage/provenance writes with `VINTAGE_PROVENANCE_WRITES_ENABLED=False` and `VINTAGE_PROVENANCE_WRITES_DISABLED`.

Required later cleanup decision: update those tests to current product policy, retire obsolete tests, or isolate disabled legacy behavior behind an explicit test mode.

`TEST-002`: optional ML stack absent in default environment.

`scikit-learn`, `joblib`, and `catboost` are optional for the current default environment. Collection now handles this through exact optional test skips and a core import-boundary fix.

`TEST-003`: one representative legacy `rebuild_net_debt_to_ebit` test fails in old `score.py` field expectations. This is not in the V3 Phase 6 TTM/valuation/score/lifecycle path.

Expected data limitations:

- valuation `MISSING_PUBLISH_DATE`: `3304`
- valuation `MISSING_TARGET_DAY_PRICE`: `120`
- score NOT_READY endpoints: `19140`
- lifecycle NOT_READY endpoints: `20111`

No blocking Phase 6 issue remains.

## Operational Rules

Score updates when relevant fundamentals/TTM change. It is endpoint-snapshot persisted, model-versioned, valuation-independent, and exposes coverage/readiness.

Lifecycle updates when a new relevant fundamental endpoint becomes available. It uses persisted previous-state context. Corrected historical input may require company-local forward recomputation.

Valuation updates for a new published endpoint only when `publish_date` is known and the first actual post-publication trading day's close is available. It persists that historical snapshot and does not use later prices.

Canonical/TTM corrections trigger only affected downstream recomputation:

- affected score recompute
- company-local lifecycle forward recompute where required
- affected valuation recompute under preserved historical price/lineage policy

Future model versions must coexist with older historical derived snapshots. Do not overwrite historical snapshots merely because a future score/lifecycle model is introduced.

## Phase 7 Handoff

Next phase: `MASTER PLAN PHASE 7 - CHECK V3`

Phase 7 should begin check-first from the populated production V3 state. It should validate integrated V3 read behavior, PIT/readiness exposure, downstream compatibility, derived object presence, and handling of NOT_READY/MISSING_* statuses.

Phase 7 must not silently alter canonical rows, TTM rows, derived snapshots, universe membership, model versions, frozen fingerprints, or RawCandle data during check-only work.
