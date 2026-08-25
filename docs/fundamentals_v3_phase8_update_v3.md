# Fundamentals V3 Phase 8 - Update V3

Date: 2026-08-25

Classification: `FUNDAMENTALS_V3_PHASE8_MANUAL_EVIDENCE_REQUIRED`

Artifact root: `temp/fundamentals_v3_phase8_update_v3/20260825T_PHASE8_UPDATE_V3`

Next phase after successful Phase 8: `MASTER PLAN PHASE 9 - PRODUCTION PROVING`

## Scope

Phase 8 ingested the material Phase 7 findings and performed Gate 1 diagnosis / repair-set freeze only.

No production repair was applied because material P1/P2 manual evidence is required before a safe bounded repair set can be approved.

## Phase 7 Input

Phase 7 artifacts were read from `temp/fundamentals_v3_phase7_check_v3/20260825T_PHASE7_CHECK_V3`.

The actual Phase 7 artifact names are:

- `canonical_publish_date_anomalies.csv`
- `field_semantic_outliers.csv`
- `issue_register.csv`
- `phase7_summary.json`

Input counts:

| Finding type | Expected | Ingested |
| --- | ---: | ---: |
| publish-date anomalies | 111 | 111 |
| semantic field outliers | 237 | 237 |

Every finding was mapped to exact `company_id`, ticker, market, fiscal year, fiscal quarter, period end, field/event, stored value and local V3 provenance where available.

Identity result:

| Metric | Count |
| --- | ---: |
| exact ticker/FY/FQ identity | 348 |
| ambiguous-quarter findings | 0 |
| unresolved identity findings | 0 |

## Age Profile

Publish-date anomalies:

| Bucket | Count |
| --- | ---: |
| <=2020 | 6 |
| 2021-2022 | 3 |
| 2023 | 0 |
| 2024 | 20 |
| 2025 | 54 |
| 2026 | 28 |

Semantic field outliers:

| Bucket | Count |
| --- | ---: |
| <=2020 | 60 |
| 2021-2022 | 91 |
| 2023 | 23 |
| 2024 | 14 |
| 2025 | 37 |
| 2026 | 12 |

## Priority

Overall priority classification:

| Priority | Count |
| --- | ---: |
| `P1_CURRENT_MATERIAL` | 142 |
| `P2_HISTORICAL_MATERIAL` | 145 |
| `P3_RECENT_UNCERTAIN` | 40 |
| `P4_LOW_CURRENT_MATERIALITY` | 21 |
| `MANUAL_REVIEW_REQUIRED` | 327 |

Manual priority split:

| Priority | Manual requests |
| --- | ---: |
| `P1_CURRENT_MATERIAL` | 142 |
| `P2_HISTORICAL_MATERIAL` | 145 |
| `P3_RECENT_UNCERTAIN` | 40 |

## Publish-Date Diagnosis

Publish-date root causes:

| Root cause | Count |
| --- | ---: |
| `PUBLISH_BEFORE_PERIOD_END` | 83 |
| `RECENT_UNCERTAIN` | 28 |

Decisions:

| Decision | Count |
| --- | ---: |
| `MANUAL_REVIEW_REQUIRED` | 104 |
| `NO_REPAIR_LOW_MATERIALITY` | 7 |

No publish-date repair candidate was frozen because local evidence showed the conflict shape but did not establish authoritative replacement publication dates.

## Semantic Outlier Diagnosis

Semantic outliers by field:

| Field | Count |
| --- | ---: |
| `revenue` | 199 |
| `cash` | 2 |
| `total_debt` | 3 |
| `shares_outstanding` | 33 |

Semantic root causes:

| Root cause | Count |
| --- | ---: |
| `SOURCE_CONFLICT` | 187 |
| `SPLIT_OR_SHARE_CLASS_ANOMALY` | 33 |
| `RECENT_UNCERTAIN` | 12 |
| `SIGN_OR_CONTEXT_CONFLICT` | 5 |

Decisions:

| Decision | Count |
| --- | ---: |
| `MANUAL_REVIEW_REQUIRED` | 223 |
| `NO_REPAIR_LOW_MATERIALITY` | 14 |

No semantic repair candidate was frozen. No clipping, winsorization, interpolation, smoothing or median replacement was used or proposed.

## Downstream Scope

If all manual findings were later confirmed and repaired, the bounded dependency scope would be:

| Dependency | Count |
| --- | ---: |
| affected TTM rows | 833 |
| affected score rows | 833 |
| affected lifecycle companies | 235 |
| affected valuation rows | 833 |
| latest company state affected findings | 108 |

This is not an apply plan. It is a maximum candidate scope derived from current local dependencies.

## Gate 1 Result

Frozen repair set:

- confirmed canonical repairs: `0`
- confirmed publish-date repairs: `0`
- expected canonical update rows: `0`

Reason:

Local V3 evidence is sufficient to identify each anomaly and downstream scope, but not sufficient to select authoritative replacement dates or fundamental values for P1/P2 rows. Production writes would therefore violate the Phase 8 no-guessing and repair-set quality gates.

Manual evidence queue:

- artifact: `manual_evidence_requests.csv`
- requests: `327`
- P1 manual cases: `142`
- P2 manual cases: `145`
- P3 manual cases: `40`

## Production Apply

Production apply was not performed.

Backup was not created because the workflow stopped before Gate 2 and no write transaction was opened.

Recorded apply/recompute/proving artifacts are placeholders documenting zero production changes:

- `canonical_apply_audit.csv`
- `publish_date_apply_audit.csv`
- `ttm_recompute_summary.csv`
- `score_recompute_summary.csv`
- `lifecycle_recompute_summary.csv`
- `valuation_recompute_summary.csv`
- `phase7_reaudit_comparison.csv`
- `unrelated_row_drift_proof.json`
- `phase8_idempotency_proof.json`
- `phase8_model_fingerprint_proof.json`

## Safety

Production writes:

- RawCandle writes: `0`
- broad canonical rebuild: `NO`
- broad TTM rebuild: `NO`
- broad score rebuild: `NO`
- broad lifecycle rebuild: `NO`
- broad valuation rebuild: `NO`

Model fingerprints preserved:

- score: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- lifecycle: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`

## Validation

Commands:

```bash
PYTHONPATH=. .venv/bin/python -m pytest swingmaster/tests/test_fundamentals_v3_phase8_update_v3.py -q
PYTHONPATH=. .venv/bin/python -m swingmaster.cli.run_fundamentals_v3_phase8_update_v3 --phase7-root temp/fundamentals_v3_phase7_check_v3/20260825T_PHASE7_CHECK_V3 --artifact-root temp/fundamentals_v3_phase8_update_v3/20260825T_PHASE8_UPDATE_V3 --v3-db rc_fundamentals_v3.db --rawcandle-db /home/kalle/projects/rawcandle/data/osakedata.db
```

Results:

- focused Phase 8 tests: `4 passed`
- production Gate 1 diagnosis classification: `FUNDAMENTALS_V3_PHASE8_MANUAL_EVIDENCE_REQUIRED`

## Handoff

Phase 8 is not complete. Resolve the exact manual queue before any production write.

Current handoff classification: `FUNDAMENTALS_V3_PHASE8_MANUAL_EVIDENCE_REQUIRED`

## Phase 8A2 - Manual Evidence Queue Reduction / Root-Cause Clustering

Date: 2026-08-25

Classification: `FUNDAMENTALS_V3_PHASE8A2_MANUAL_QUEUE_REDUCED_USER_EVIDENCE_REQUIRED`

Artifact root: `temp/fundamentals_v3_phase8a2_manual_queue_reduction/20260825T_PHASE8A2_QUEUE_REDUCTION`

Phase 8A2 reduced the original Gate 1 manual workload by clustering findings, preserving every `issue_id`, separating immediate user work from later/manual-if-needed review, and isolating wait/accept cases. This subphase remained read-only.

Input reconciliation:

| Metric | Expected | Actual |
| --- | ---: | ---: |
| total findings | 348 | 348 |
| publish-date findings | 111 | 111 |
| semantic findings | 237 | 237 |
| raw manual requests | 327 | 327 |

Final dispositions:

| Disposition | Findings |
| --- | ---: |
| `MANUAL_A` | 180 |
| `MANUAL_B` | 145 |
| `WAIT_FOR_REFRESH` | 2 |
| `LOW_MATERIALITY_ACCEPT` | 21 |

Final practical queues:

| Queue | Evidence units | Role |
| --- | ---: | --- |
| Queue A - must check now | 180 | Immediate user evidence queue required before Gate 1 can close |
| Queue B - check if needed | 145 | Historical/material fallback queue |
| Queue C - wait/accept/systematic | 23 | No immediate user work |

The immediate user workload is reduced from `327` raw manual requests to `180` Queue A evidence units, a `44.95%` reduction.

Publish-date clustering:

- total publish-date findings: `111`
- publish-before-period-end / local conflict pattern: `83`
- recent uncertain: `28`
- wrong-quarter local candidate matches: `20`
- no local same-date quarter match: `91`
- distinct source/provenance families: `3`
- systematic source-family candidates: `2`

Semantic clustering:

| Field | Findings |
| --- | ---: |
| `revenue` | 199 |
| `cash` | 2 |
| `total_debt` | 3 |
| `shares_outstanding` | 33 |

Dominant semantic source-pair families:

- `LEGACY:CANONICAL_APPLY:PHASE3C_1D_SEC_Q4_STRUCTURE_PLUS_PHASE3C_1E_FIELD_POLICY` / revenue: `75`
- `V2:CANONICAL_APPLY:UNKNOWN` / revenue: `34`
- `V2:PHASE4B_FIELD_RECOVERY:DIRECT_SAME_Q_NULL_FILL` / revenue: `21`
- `V2:CANONICAL_APPLY:UNKNOWN` / shares_outstanding: `19`
- `LEGACY:CANONICAL_APPLY:PHASE3C_1D_READY_EXPLICIT_LEGACY_QUARTER` / revenue: `18`

Recent 2026 split:

| Action | Findings |
| --- | ---: |
| `MANUAL_CHECK_NOW` | 38 |
| `WAIT_FOR_REFRESH` | 2 |

Historical reassessment:

| Bucket | Findings | Manual retained | Downgraded/accepted |
| --- | ---: | ---: | ---: |
| <=2020 | 66 | 60 | 6 |
| 2021-2022 | 94 | 85 | 9 |

Latest-state prioritization:

- latest-state findings: `108`
- latest-state evidence units: `108`
- Queue A latest-state units: `108`

Production safety:

| Layer | Before | After |
| --- | ---: | ---: |
| canonical | 73,075 | 73,075 |
| TTM | 54,038 | 54,038 |
| valuation | 54,038 | 54,038 |
| score | 54,038 | 54,038 |
| lifecycle | 54,038 | 54,038 |

Production writes: `0`

RawCandle writes: `0`

Next action:

`USER MANUAL EVIDENCE REVIEW - QUEUE A`

Key Phase 8A2 artifacts:

- `manual_evidence_queue_A_must_check_now.csv`
- `manual_evidence_queue_A_human_summary.md`
- `manual_evidence_queue_B_check_if_needed.csv`
- `manual_evidence_queue_C_wait_accept_systematic.csv`
- `manual_request_dedup_map.csv`
- `phase8a2_summary.json`
- `phase8_gate1_next_action.md`

## Phase 8A5 - Verified Publish-Date Evidence Ingest & Period-End Tolerance

Date: 2026-08-25

Classification: `FUNDAMENTALS_V3_PHASE8A5_PUBLISH_MANUAL_REVIEW_REMAINS`

Verified CSV source: `temp/fundamentals_v3_phase8_publish_date_manual_check_verified.csv`

Artifact root: `temp/fundamentals_v3_phase8a5_verified_publish_ingest/20260825T_PHASE8A5_VERIFIED_PUBLISH_INGEST`

Phase 8A5 ingested the externally verified publish-date file and froze only evidence-sufficient publish-date candidates. No production writes were performed.

Input reconciliation:

| Metric | Count |
| --- | ---: |
| rows | 111 |
| unique quarters | 111 |
| `MATCH` | 16 |
| `DIFFERENT` | 95 |
| `UNCERTAIN` / `NOT_FOUND` | 0 |
| Source 1 complete | 111 |
| Source 2 complete | 111 |

Fiscal identity reconciliation:

| Metric | Count |
| --- | ---: |
| FY matches | 111 |
| FY mismatches | 0 |
| FQ matches | 111 |
| FQ mismatches | 0 |
| current production identity/state mismatches | 0 |

Publish evidence classification:

| Classification | Count |
| --- | ---: |
| `MATCH_CONFIRMED` | 16 |
| `PUBLISH_DATE_REPAIR_CONFIRMED` | 78 |
| `PUBLISH_DATE_SEMANTICS_UNCERTAIN` | 17 |

The 17 semantics-uncertain rows are based on filing-date evidence without sufficiently clear earnings/result publication semantics. They are excluded from the frozen publish-date repair set.

Permanent period-end policy:

For the same confirmed canonical fiscal quarter, period_end differences within ±7 actual trading days are considered equivalent for V3 tracking. Within that tolerance, the later date is used as canonical period_end. Period End remains metadata, not canonical quarter identity. Differences above 7 trading days require review. Small fiscal-close vs month-end differences are not material errors under this policy.

Period-end tolerance analysis:

| Disposition | Count |
| --- | ---: |
| exact period-end match | 54 |
| differing period ends | 57 |
| within ±7 trading days | 25 |
| outside tolerance | 32 |
| current V3 already later | 18 |
| verified date later | 7 |

Frozen repair candidate files:

| Candidate set | Rows |
| --- | ---: |
| publish-date repairs | 78 |
| period-end metadata repairs | 7 |

Downstream planning for confirmed publish-date repairs:

| Layer | Potentially affected rows |
| --- | ---: |
| TTM PIT/availability metadata | 181 |
| score metadata/lineage | 181 |
| lifecycle metadata/lineage | 181 |
| valuation rows | 62 |
| valuation dates that would change | 62 |

Period-end-only downstream recompute required: `False`

Unresolved rows:

- publish semantics manual review rows: `17`
- period_end outside tolerance manual review rows: `32`
- unique unresolved rows: `49`

Next action:

`USER MANUAL REVIEW - ONLY REMAINING UNRESOLVED PUBLISH/PERIOD-END ROWS`

## Phase 8A6 - Safe Verified Repair Apply + Residual Evidence Resolution

Classification: `FUNDAMENTALS_V3_PHASE8A6_SAFE_REPAIRS_APPLIED_RESIDUAL_REVIEW_REMAINS`

Status: `DONE_SAFE_CANONICAL_APPLY_DOWNSTREAM_DEFERRED`

Artifact root: `temp/fundamentals_v3_phase8a6_safe_apply/20260825T181951Z`

Verified semantic CSV used: `temp/phase8_semantic_manual_check_verified.csv`

Publish input: `temp/fundamentals_v3_phase8a5_verified_publish_ingest/20260825T_PHASE8A5_VERIFIED_PUBLISH_INGEST/publish_date_frozen_repair_set.csv`

Period-end input: `temp/fundamentals_v3_phase8a5_verified_publish_ingest/20260825T_PHASE8A5_VERIFIED_PUBLISH_INGEST/period_end_frozen_repair_set.csv`

Semantic input reconciliation:

| Metric | Count |
| --- | ---: |
| rows | 237 |
| `MATCH` | 65 |
| `DIFFERENT` | 105 |
| `DIFFERENT + HIGH` | 87 |
| `DIFFERENT + MEDIUM` | 18 |
| `UNCERTAIN` | 29 |
| `NOT_FOUND` | 33 |
| `VALID_BUT_DIFFERENT_SEMANTICS` | 5 |

Safe semantic acceptance rule: automatic repair required `DIFFERENT`, `HIGH`, verified value, primary source, exact FY/FQ identity, current production value parity, period-end mapping under policy, and field-specific semantic proof for revenue, shares, debt, or cash. `MEDIUM`, `UNCERTAIN`, `NOT_FOUND`, `VALID_BUT_DIFFERENT_SEMANTICS`, source-semantics conflicts, and local evidence contradictions were excluded.

Safe repairs applied:

| Repair type | Rows |
| --- | ---: |
| publish_date | 78 |
| period_end metadata | 7 |
| semantic canonical value | 87 |
| total changed canonical cells | 172 |
| unique canonical quarters affected | 165 |
| write-guard failures | 0 |

Semantic repairs by field:

| Field | Rows |
| --- | ---: |
| revenue | 82 |
| shares_outstanding | 3 |
| cash | 1 |
| total_debt | 1 |

Production apply:

| Metric | Value |
| --- | --- |
| DB | `rc_fundamentals_v3.db` |
| preflight quick_check | `ok` |
| post-apply quick_check | `ok` |
| backup | `temp/fundamentals_v3_phase8a6_safe_apply/20260825T181951Z/backup/rc_fundamentals_v3_phase8a6_backup.db` |
| backup sha256 | `9c896c98c7b2ec4b4e19d59b47160893093d696bbb740f9d7056488b9d469081` |
| transaction result | `COMMITTED` |

Production row counts stayed unchanged: `v3_company=2550`, `v3_quarter=73075`, `v3_quarter_fundamentals=73075`, `v3_ttm=54038`, `v3_score=54038`, `v3_lifecycle=54038`, `v3_valuation=54038`.

Downstream rebuild was intentionally not run. Derived data is temporarily marked:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Downstream writes in Phase 8A6: `v3_ttm=0`, `v3_score=0`, `v3_lifecycle=0`, `v3_valuation=0`.

Score model fingerprint remained `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`.

Lifecycle model fingerprint remained `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`.

Residual evidence queues after safe apply:

| Queue | Rows |
| --- | ---: |
| R1 - must resolve before final canonical freeze | 32 |
| R2 - second source helpful | 40 |
| R3 - accept / document / wait | 127 |

Residual publish/period-end:

| Type | Rows |
| --- | ---: |
| unresolved publish semantics | 17 |
| outside-tolerance period-end | 32 |
| total publish/period-end residual rows | 49 |

Residual semantic evidence rows: `150`.

Post-apply Phase 7 read-only audit artifact: `temp/fundamentals_v3_phase8a6_safe_apply/20260825T181951Z/post_phase7_readonly_audit`

Post-apply Phase 7 residual CSV row counts: publish-date anomalies `50`, semantic outliers `156`.

Phase 8 remains `IN PROGRESS`. Exact next action: resolve R1/R2 residual evidence or explicitly accept documented R3 cases, then run a separate downstream rebuild phase.
