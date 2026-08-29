# Fundamentals V3 Known Deferred Defects

Status: `KNOWN DEFECTS - TEMPORARILY ACCEPTED FOR OPERATIONAL DOWNSTREAM REBUILD`

These defects are not resolved, not accepted as canonical quality, and do not close Phase 8.

The user deliberately postponed further canonical repair because the remaining work is time-consuming and the important affected tickers must stay in the V3 universe. Downstream outputs are rebuilt temporarily from current production canonical V3 with known input risk.

Known global A10B P1 population: `15 rows / 13 tickers`.

Known tickers: `BBY, DELL, FNGR, GCO, HAE, MRVL, POWW, RH, RL, SAIC, TJX, TRNS, VTGN`.

Known categories:

- Nine 52/53-week recent-segment mapping/reconstruction defects: `BBY`, `DELL`, `GCO`, `HAE`, `MRVL`, `RL`, `SAIC`, `TJX`, `TRNS`.
- FNGR residual structural cases.
- POWW residual case.
- RH duplicate/economic-quarter issue.
- VTGN residual case.
- A10F frozen-but-unapplied safe subset: `18 groups / 19 operations`.
- A10F blockers: `15`.

Machine-readable register: `temp/fundamentals_v3_phase8b_downstream_rebuild/20260827T_PHASE8B/fundamentals_v3_deferred_defect_register.csv`

## Phase 8C Note

Phase 8C added fiscal-calendar metadata and fiscal-slot validation evidence. Use it when deferred canonical repairs resume; no Phase 8B defects are resolved by this note.

## Phase 8D-2 Current-Risk Expansion

Phase 8D-2 did not resolve or modify defects. It expanded the current-risk register using the Phase 8D-1 full-audit artifacts and current downstream dependency mapping.

New current-risk tickers outside the original known-13 register: `511`.

Machine-readable current-risk register: `temp/fundamentals_v3_phase8d2_operational_risk/20260827T_PHASE8D2/new_current_risk_tickers.csv`

Operational conclusion: `CURRENT_V3_CURRENT_DOWNSTREAM_RISK_TOO_BROAD`. Do not proceed to Phase 8 operational closure before prioritizing current-latest / TTM-affecting fiscal identity repairs.

## Phase 8E-APPLY Baseline

The PREAPPLY-proven 494 deterministic fiscal identity rows were applied. Remaining deferred populations: direct FY conflicts `2988`, direct FQ conflicts `728`, transition reviews `264`, unresolved fiscal history `2703`, original Phase 8E blocked rows `207`.
