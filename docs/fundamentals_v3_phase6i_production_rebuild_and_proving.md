# Fundamentals V3 Phase 6I Production Rebuild And Proving

Classification: `FUNDAMENTALS_V3_PHASE6I_PRODUCTION_REBUILD_PROVEN_READY_FOR_PHASE6J`

## Preflight

- Production DB: `/home/kalle/projects/swingmaster/rc_fundamentals_v3.db`
- DB size bytes: `283590656`
- Free bytes: `639006146560`
- Quick check: `ok`
- TTM rows before: `54038`
- Score rows before: `0`
- Valuation rows before: `0`
- Lifecycle table before: `False`
- Backup: `temp/fundamentals_v3_phase6i_production_rebuild/20260825T_PHASE6I_PRODUCTION_REBUILD/backup/rc_fundamentals_v3.db.20260825T120045Z.sqlite.backup`

## Models

- Score: `V3_LEGACY2_FUNDAMENTAL_SCORE_V1` / `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- Lifecycle: `V3_LIFECYCLE_EBIT_FIRST_V1` / `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`
- Valuation: `V3_VALUATION_POST_PUBLICATION_SNAPSHOT_V1`

## Production Population

- Valuation rows: `54038`, ready `43754`, missing publish `3304`, missing target price `120`
- Score rows: `54038`, ready `34898`, NOT_READY `19140`, median coverage `85.0`
- Lifecycle rows: `54038`, ready `33927`, NOT_READY `20111`, transition rate `14.687949046564915`

## Proving

- Run 2 idempotent: `True`
- Source drift: `False`
- Acceptance: `{'valuation': True, 'score': True, 'lifecycle': True, 'source_safety': True, 'idempotency': True, 'rollback_readiness': True, 'quick_check': True}`

## Validation

- Focused/targeted Phase 5/6 regression gate: `331 passed`
- Full pytest suite attempted before production writes: collection stopped on unrelated missing optional ML dependencies `sklearn` and `joblib` in 26 non-fundamentals test modules.
- Static validation: `compileall` passed and `git diff --check` passed.

Next: `MASTER PLAN PHASE 6J - PHASE 6 CLOSURE`
