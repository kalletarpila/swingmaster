# Fundamentals V3 Phase 8A9 Period-End Apply

Status: `DONE_PARTIAL_GUARDED_APPLY_SEQUENCE_COLLISION_R1_RETAINED`

Classification: `FUNDAMENTALS_V3_PHASE8A9_SEQUENCE_COLLISION_R1_RETAINED`

Run artifact root: `temp/fundamentals_v3_phase8a9_period_end_apply/20260826T052001Z`

Input file: `temp/phase8_period_end_R1_verified.csv`

Phase 8A8 source queue used only for original-column reconciliation:
`temp/fundamentals_v3_phase8a8_r1_resolution/20260826T045013Z/external_research_queue_R1.csv`

## Input Reconciliation

The verified file matched the locked Phase 8A9 input contract:

| Metric | Count |
| --- | ---: |
| verified rows | 18 |
| unique quarters | 18 |
| DIFFERENT | 18 |
| HIGH confidence | 18 |
| PERIOD_END issue type | 18 |
| source count >= 2 | 12 |
| source count = 1 | 6 |
| Candidate Value == Verified Period End | 18 |
| identity conflicts | 0 |

The original 17 Phase 8A8 external queue columns reconciled by `Request ID`.

## Apply Result

Only rows passing current-value, collision, and fiscal-sequence guards were applied.

Applied rows:

| Ticker | FY/Q | Old period_end | New period_end |
| --- | --- | --- | --- |
| AMST | FY2024 Q4 | 2024-12-31 | 2024-06-30 |
| KALV | FY2025 Q3 | 2024-12-31 | 2025-01-31 |
| LYTS | FY2025 Q1 | 2025-03-31 | 2024-09-30 |

Rows applied: `3`

Write guard failures among frozen repairs: `0`

No `publish_date`, fundamentals values, source variants, TTM, score, lifecycle, valuation, RawCandle, or FY/FQ labels were changed.

## Retained R1

Retained R1 rows: `15`

| Guard | Count |
| --- | ---: |
| COLLISION | 10 |
| SEQUENCE_CONFLICT | 5 |

Collision rows:

`CRUS FY2025 Q4`, `DOMO FY2025 Q4`, `EEFT FY2025 Q3`, `IMMR FY2025 Q4`, `INBS FY2025 Q4`, `MNR FY2025 Q4`, `MNRO FY2025 Q4`, `NCNO FY2025 Q4`, `SKY FY2025 Q4`, `VIVS FY2025 Q4`

Sequence-conflict rows:

`FNGR FY2024 Q2`, `RBC FY2025 Q4`, `RCAT FY2024 Q3`, `VIVS FY2025 Q1`, `VIVS FY2025 Q2`

These rows were intentionally not written because applying the verified period_end while keeping FY/FQ hard-locked would either duplicate another same-company period_end or break chronological FY/FQ ordering.

## Production Integrity

| Metric | Result |
| --- | --- |
| quick_check | ok |
| companies | 2540 -> 2540 |
| canonical quarters | 72765 -> 72765 |
| fundamentals rows | 72765 -> 72765 |
| TTM rows | 53815 -> 53815 |
| score rows | 53815 -> 53815 |
| lifecycle rows | 53815 -> 53815 |
| valuation rows | 53815 -> 53815 |
| duplicate canonical identities | 0 |
| orphan fundamentals | 0 |
| fundamentals unchanged | true |
| TTM/score/lifecycle/valuation unchanged | true |

Same-company period_end duplicate groups were `3` before and `3` after A9; A9 introduced no new period_end duplicate group. The existing groups are from prior accepted A8 repairs: `POWW`, `RH`, and `VTGN`.

Derived data remains:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Downstream rebuild was not run.

## Backup

Backup: `temp/fundamentals_v3_phase8a9_period_end_apply/20260826T052001Z/backup/rc_fundamentals_v3_phase8a9_backup.db`

Backup SHA256:
`d4b114d35b4ae731096d05844f8613b7c7c1873d2ab67bc31feda950986b5c7f`

## Next Action

`RESOLVE_SEQUENCE_COLLISION_R1_BEFORE_COMBINED_DOWNSTREAM_REBUILD`
