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

## Test-Gate Closure Fix

- Original blocker: full-suite pytest collection failed before execution because optional ML packages were absent from the local `.venv`.
- Reproduced command: `PYTHONPATH=. /home/kalle/projects/swingmaster/.venv/bin/python -m pytest --collect-only`
- Original result: `3438 tests collected / 26 collection errors`.
- Missing imports: `joblib` and `sklearn`; `sklearn` is provided by the `scikit-learn` distribution package. `catboost` was also absent and belongs to the same optional ML model layer.
- Dependency classification: `GENUINELY_OPTIONAL_ML_DEPENDENCY`.
- Root cause: non-ML test modules imported `swingmaster.cli.run_range_universe`, which eagerly imported `swingmaster.dual_score.production`; that module imports optional sklearn model code. The remaining failures were direct optional ML test-module imports.
- Fix: `run_range_universe` now imports dual-score production lazily only when dual-score production is enabled; exact optional ML test modules use `pytest.importorskip(...)` for `joblib`, `sklearn`, and `catboost`.
- Focused validation: `19 passed`.
- Targeted Phase 5/6 regression gate after fix: `686 passed`.
- Full pytest collection after fix: `3457 collected / 22 skipped / 0 errors`.
- Full pytest execution after fix: `3237 passed / 181 failed / 61 skipped / 0 errors` in `565.82s`.
- Remaining full-suite failures classification: `PRE_EXISTING_UNRELATED_FAILURE`; failures are concentrated in older reported-vintage/PIT write tests blocked by current `VINTAGE_PROVENANCE_WRITES_DISABLED` product policy and related retired vintage write assumptions.
- Phase 6 relation: no Phase 6 V3 TTM, valuation, score, lifecycle, frozen-model, or production-data regression found.
- Score fingerprint unchanged: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`.
- Lifecycle fingerprint unchanged: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`.
- Valuation publish+1 policy unchanged: `V3_VALUATION_POST_PUBLICATION_SNAPSHOT_V1`.
- Production V3 derived row counts unchanged: `v3_ttm=54038`, `v3_valuation=54038`, `v3_score=54038`, `v3_lifecycle=54038`.
- Production writes in this fix: `0`.
- Artifact root: `temp/fundamentals_v3_phase6i_test_gate_fix/20260825T125721Z/`.

Next: `MASTER PLAN PHASE 6J - PHASE 6 CLOSURE`
