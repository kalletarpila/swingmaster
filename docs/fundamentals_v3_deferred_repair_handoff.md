# Fundamentals V3 Deferred Repair Handoff

Phase 8 remains `IN PROGRESS`.

Unresolved P1 tickers: `BBY, DELL, FNGR, GCO, HAE, MRVL, POWW, RH, RL, SAIC, TJX, TRNS, VTGN`.

Do not re-research completed evidence unnecessarily. Resume from these phases and artifacts:

- A10B: current global P1 audit and external queue.
- A10C: local-evidence current-critical cases.
- A10D-R: global P1 segment reconciliation.
- A10E: one-year period-end shift root cause.
- A10E-R: official latest-8Q mapping.
- A10E-R2: financial-fingerprint mapping.
- A10E-R3: clean latest-8Q reconstruction.
- A10F: current-downstream safe subset and blockers.

Current decision: do not repair canonical data now; rebuild downstream temporarily and return to canonical repair before final cutover. Prevention hardening remains mandatory before final V3 cutover.

## Phase 8C Note

Phase 8C added fiscal-calendar metadata and fiscal-slot validation evidence. Use it when deferred canonical repairs resume; no Phase 8B defects are resolved by this note.

## Phase 8D - Fiscal Calendar Prevention Guards

Status: `FUNDAMENTALS_V3_PHASE8D_FISCAL_WRITE_GUARDS_ACTIVE`

Fiscal-calendar guard is active in `V3QuarterRepository.upsert_quarter` before canonical quarter mutation. Exact FY2026/FY2027 anchors are authoritative, backward inference assumes stable fiscal calendar unless positive transition evidence exists, and `REVIEW`/`BLOCK` candidates perform zero canonical writes.

Phase 8 remains `IN PROGRESS`.

## Historical Exact Fiscal-Year Anchors

FY1999-FY2027 verified exact fiscal-year-start anchors are now available for future fiscal-label provenance and guard reanalysis. Artifact root: `temp/fundamentals_v3_phase8c_ext_historical_anchors/20260828T_PHASE8C_EXT`.

## Phase 8D-7 Historical Anchor Repairability

Classification: `HISTORICAL_EXACT_ANCHORS_MATERIALLY_RESOLVE_FISCAL_IDENTITY_RISK`. Deterministic direct-exact auto relabel candidates `701` rows across `192` tickers; segment candidates `643` rows across `152` tickers. Artifact root: `temp/fundamentals_v3_phase8d7_historical_anchor_reanalysis/20260828T_PHASE8D7`.

## Phase 8E Frozen Deterministic Apply Set

Frozen safe apply set: `494` rows across `148` tickers. Blocked deterministic groups: `207`. Downstream rebuild remains deferred until production apply. Artifact root: `temp/fundamentals_v3_phase8e_rehearse_fiscal_repairs/20260828T_PHASE8E`.

## Phase 8E-PREAPPLY Deferred Groups

Groups excluded after preapply proving: `0`. Reasons: `NONE`. Artifact root: `temp/fundamentals_v3_phase8e_preapply_downstream_proving/20260829T_PHASE8E_PREAPPLY`.
