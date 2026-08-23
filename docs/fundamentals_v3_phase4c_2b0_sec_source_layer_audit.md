# Fundamentals V3 Phase 4C-2B0 SEC Source-Layer Audit

Classification: `FUNDAMENTALS_V3_PHASE4C2B0_SEC_SOURCE_AUDIT_COMPLETE_SIMFIN_FIRST_RECOMMENDED`

Prior Phase 4C-2 claim classification: `CLAIM_INCORRECT_LAYER_DESCRIPTION`

Artifact root: `temp/fundamentals_v3_phase4c_2b0_sec_source_layer_audit/20260823T_PHASE4C2B0_SEC_SOURCE_LAYER_AUDIT`

## Definitive Source Statement

PHASE 4C-2 USED FUNDAMENTALS_USA.DB RC_FUNDAMENTAL_STATEMENT_RAW, A SEC-DERIVED FILTERED STATEMENT LAYER, NOT ORIGINAL SEC COMPANYFACTS RAW JSON.

## Component Loss

Pretax, gross interest, D&A, depreciation, and amortization are absent from the retained local SEC-derived statement layer because the current companyfacts extractor is driven by a hard-coded `SEC_TAGS` allowlist that does not include those component families.

## Local Earliest SEC Layer

Earliest retained local SEC-derived layer: `fundamentals_usa.db.rc_fundamental_statement_raw`.

Original SEC companyfacts JSON cache present locally: `False`.

## Recommendation

- Existing local SEC raw sufficient: `False`
- New SEC download needed for SEC path: `True`
- SimFin-first recommended: `True`
- Next: `MASTER PLAN PHASE 4C-2B - SIMFIN COMPONENT & MULTI-FIELD VALIDATION`

Safety: canonical financial writes `0`, metadata writes `0`, provider/network calls `0`.
