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

## Phase 8E-APPLY Remaining Deferred Defects

Remaining direct FY conflicts `2988`, direct FQ conflicts `728`, transition reviews `264`, unresolved `2703`. Original Phase 8E blocked rows remain deferred: `207`. Artifact root: `temp/fundamentals_v3_phase8e_apply/20260829T_PHASE8E_APPLY`.

## Phase 8F - Complete Latest-8Q Gap / Full-Closure Analysis

Classification: `LATEST8Q_FULL_CLOSURE_MAP_COMPLETE_WITH_STRUCTURAL_DECISIONS`

Artifact root: `temp/fundamentals_v3_phase8f_latest8q_full_closure/20260829T_PHASE8F`

Latest8Q rows audited: `19728` across `2470` active tickers. Already fully clean tickers: `92`. Primary-core clean tickers: `92`. Structural/manual-review tickers: `496`. `NO_MISSING_REQUIREMENT_IN_PLAN`: `0`.

Safety proof: production writes `0`, RawCandle writes `0`, fingerprints unchanged `True`.

Next action: RESOLVE LOCAL-ONLY CASES AND EXTERNAL EVIDENCE FIRST; KEEP ONLY TRUE TRANSITION / COLLISION / STRUCTURAL CASES FOR MANUAL DECISION

## Phase 8G - Local Latest8Q Downstream-Critical Repair

Classification: `LATEST8Q_LOCAL_CRITICAL_REPAIRS_COMPLETE_WITH_EXTERNAL_STRUCTURAL_WORK_REMAINING`

Final verification artifact root: `temp/fundamentals_v3_phase8g_local_latest8q_repairs/20260829T_PHASE8G_FINAL`

Local downstream-critical canonical relabels were applied through five guarded cascading passes: `277` rows / `277` groups / `92` tickers, failed groups `0`. Final read-only verification found local candidates `0` and `NO_MISSING_REQUIREMENT` `0`.

New minimal external facts `4413` vs Phase 8F `6064`. New material structural decisions `1095` vs Phase 8F `1130`.

Phase 8 remains `IN PROGRESS`.

Next action: USE THE NEW MINIMAL DOWNSTREAM-CRITICAL EXTERNAL RESEARCH QUEUE NEXT; DO NOT RESEARCH SECONDARY FIELDS THAT DO NOT AFFECT TTM / SCORE / LIFECYCLE / VALUATION


## Phase 8H - Latest8Q Downstream-Critical External Research Packaging

Classification: `LATEST8Q_EXTERNAL_RESEARCH_PACKAGE_READY_WITH_STRUCTURAL_DEPENDENCIES`

Artifact root: `temp/fundamentals_v3_phase8h_external_research_packaging/20260829T_PHASE8H`

Packaged Phase 8G external queue rows `4413` into `4413` research tasks covering `9491` normalized downstream-critical facts across `1689` tickers. Secondary-only removals `0`, duplicate facts removed `329`.

Wave 1 `1066` tasks / `810` tickers / `1857` facts. Wave 2 `370` tasks / `181` tickers / `548` facts. Wave 3 `2977` tasks / `1336` tickers / `7086` facts.

Structural queue remains separate: `1095` decisions / `482` tickers. External+structural mixed tickers `402`.

Phase 8 remains `IN PROGRESS`.

Next action: RUN WAVE 1 EXTERNAL RESEARCH FIRST WHILE KEEPING STRUCTURAL DECISIONS SEPARATE; USE NEW EVIDENCE TO REDUCE THE STRUCTURAL QUEUE BEFORE MANUAL REVIEW

## Phase 8H-3 Genuine Remaining Wave 1 Cases

- More external evidence facts: `76`
- External tickers: `36`
- Structural decisions: `11`
- Structural tickers: `11`

## Phase 8H-4 Post-Apply Remaining Wave 1 Cases

- External evidence: `17` tickers / `53` fact rows
- Structural review: `11` tickers / `11` decisions
- Local reconciliation: `3` tickers / `3` cases

## Phase 8H-5 Remaining Wave 1 Cases

- Local reconciliation: `3` cases
- Structural review: `11` decisions
- External follow-up: `17` tickers / `53` fact rows

Local tickers: `ALGM, POWW, TRNS`

Structural tickers: `ASTH, CECO, CGC, CTOR, MCS, MYGN, OSK, PTIX, RCEL, VCEL, WDAY`

## Phase 8H-5A Fiscal Identity Handoff

H3 false-positive structural cases are closed only when exact-anchor arbitration proves current V3 identity correct. Existing-row fiscal identity repair candidates are in `temp/fundamentals_v3_phase8h5a_fiscal_identity_root_cause/20260830T_PHASE8H5A`. Missing Q4 reconstruction remains deferred.

## Phase 8H-5A-PREAPPLY Fiscal Identity Residuals

Only non-apply fiscal identity residuals remain deferred after preapply proof: target collisions, insufficient FQ-confidence rows, and no-write H3/provider false positives. Missing Q4 reconstruction is still a separate deferred phase and was not started here.
