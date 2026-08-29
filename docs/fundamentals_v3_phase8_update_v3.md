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

For the same confirmed canonical fiscal quarter, period_end differences within ±7 actual trading days are considered equivalent for V3 tracking only when no stronger official issuer/SEC period-end evidence resolves the exact date. Within that tolerance, the later date may be used as canonical period_end as a fallback; official period-end evidence still outranks normalized provider dates. Period End remains metadata, not canonical quarter identity. Differences above 7 trading days require review. Small fiscal-close vs month-end differences are not material errors under this policy.

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

## Phase 8A7 - Canonical Repair Closure Before Downstream Rebuild

Classification: `FUNDAMENTALS_V3_PHASE8A7_CANONICAL_CLOSURE_RESIDUAL_R1_REVIEW_REQUIRED`

Status: `DONE_BOUNDED_CANONICAL_REPAIR_AND_FINANCIAL_REMOVAL_DOWNSTREAM_DEFERRED`

Artifact root: `temp/fundamentals_v3_phase8a7_canonical_closure/20260825T183549Z`

A6 baseline: publish repairs `78`, period-end metadata repairs `7`, semantic value repairs `87`, changed canonical cells `172`, unique quarters affected `165`.

Re-audit reconciliation:

| Area | A6 residual | Phase 7 re-audit | Explanation |
| --- | ---: | ---: | --- |
| publish/period-end | 49 | 50 | Phase 7-only rows: `BBW FY2025 Q3`, `GEF FY2026 Q2`, `GEF FY2026 Q3`; A6-only rows no longer flagged: `KALV FY2025 Q3`, `MNR FY2025 Q4`; net `+1`. |
| semantic | 150 | 156 | Phase 7-only rows: `ABVC FY2025 Q4`, `CAPS FY2018 Q4`, `FTFT FY2024 Q4`, `SLDP FY2026 Q2`, `VIR FY2026 Q1`, `WKHS FY2021 Q4`. These are expected audit flags after previously accepted/confirmed negative revenue semantics, not A6 residual rows. |

Five confirmed wrong-semantics Revenue repairs were applied with old-value guards:

| Ticker | FY/Q | Old | New | Root cause |
| --- | --- | ---: | ---: | --- |
| GDC | FY2020 Q3 | -45759 | 2465765 | Revenue subcomponent selected instead of consolidated Revenue |
| LIXT | FY2022 Q3 | -643957 | 0 | expense-related concept selected as Revenue |
| MBOT | FY2021 Q2 | -35000 | 0 | non-operating interest selected as Revenue |
| MBOT | FY2021 Q3 | -3000 | 0 | non-operating interest selected as Revenue |
| VAL | FY2019 Q3 | -2000000 | 551300000 | contract-asset / receivable movement selected as Revenue |

Revenue repair rows changed: `5`; write-guard failures: `0`.

Confirmed Revenue mapper failure modes:

- Revenue subcomponent selected instead of consolidated Revenue.
- Non-operating interest or expense selected as Revenue for a pre-revenue company.
- Contract asset / receivable movement selected as Revenue.
- Cumulative/YTD context cannot silently satisfy discrete-quarter Revenue.

Systemic scan candidates found: `5`. No additional rows beyond the five explicitly approved repairs were automatically changed.

Financial UNCERTAIN Revenue set:

| Metric | Count |
| --- | ---: |
| UNCERTAIN financial rows | 24 |
| unique companies | 10 |
| `REMOVE_FROM_V3` | 10 |
| `KEEP_IN_V3` | 0 |
| `MANUAL_REMOVAL_REVIEW` | 0 |

Removed tickers: `AOMR`, `ARR`, `DX`, `IVR`, `KREF`, `NLY`, `ORC`, `RC`, `RWT`, `TWO`.

Removal rationale: all frozen removal companies are mortgage-REIT / real-estate credit / lender style entities where the standard operating-company Revenue/EBIT/FCF model is semantically inappropriate. Ordinary operating REITs were not removed by this rule.

Financial removal deleted: companies `10`, canonical quarters `310`, fundamentals `310`, TTM `223`, score `223`, lifecycle `223`, valuation `223`, migration audit `1021`, resolution issues `67`. Unrelated-company changes: `0`.

Special-case results:

| Case | Result | Repair |
| --- | --- | --- |
| POWW FY2025 Q1 / 2025-03-31 | fiscal identity conflict; official structure points to FY2025 Q4 semantics | no, left R1 |
| RH FY2021 Q4 / 2021-05-01 | fiscal identity conflict; official structure points to FY2021 Q1 semantics | no, left R1 |
| VTGN FY2025 Q1 / 2025-03-31 | fiscal identity conflict; official structure points to FY2025 Q4 semantics | no, left R1 |
| VTGN FY2022 Q3 | same retained-company fiscal identity conflict family surfaced in residual R1 | no, left R1 |
| PRSU FY2024 Q4 | annual/9M evidence remains incompatible; no unsafe FY-minus-9M | no, R2 |
| TBLA FY2022 Q3 cash | only cash plus restricted cash found; cash-only value not accepted | no, R2 |

Residual queues after A7:

| Queue | Rows |
| --- | ---: |
| R1 | 36 |
| R2 | 37 |
| R3 | 126 |

R1 consists of retained-company period-end outside-tolerance rows `32` plus fiscal identity conflicts `4` (`POWW`, `RH`, `VTGN`, plus retained identity-conflict revenue row count from the residual set). R2 contains publish semantics second-source rows `17`, medium-confidence semantic rows `18`, plus PRSU/TBLA special cases `2`.

Post-A7 proving:

| Metric | Value |
| --- | --- |
| quick_check | `ok` |
| canonical rows | `72765` |
| duplicate identities | `0` |
| orphan issues | `0` |
| post-A7 publish audit count | `50` |
| post-A7 semantic audit count | `127` |

Production row counts changed only by the approved financial-company removals: `v3_company 2550 -> 2540`, active companies `2482 -> 2472`, inactive companies `68 -> 68`, `v3_quarter 73075 -> 72765`, `v3_ttm/v3_score/v3_lifecycle/v3_valuation 54038 -> 53815`.

Backup: `temp/fundamentals_v3_phase8a7_canonical_closure/20260825T183549Z/backup/rc_fundamentals_v3_phase8a7_backup.db`

Backup sha256: `f69931073ed4865a3862168e0582863d124757dd28a7e2b647b5da7014c986c9`

Downstream rebuild was not run. Derived data remains:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Phase 8 remains `IN PROGRESS`. Exact next action: `RESOLVE PHASE 8A7 RESIDUAL R1 BEFORE DOWNSTREAM REBUILD`.

## Phase 8A8 - Residual R1 Resolution Before Downstream Rebuild

Classification: `FUNDAMENTALS_V3_PHASE8A8_EXTERNAL_RESEARCH_REQUIRED`

Status: `DONE_VERIFIED_R1_REPAIRS_APPLIED_EXTERNAL_PERIOD_END_RESEARCH_REQUIRED`

Artifact root: `temp/fundamentals_v3_phase8a8_r1_resolution/20260826T045013Z`

Starting state after A7:

| Metric | Count |
| --- | ---: |
| R1 | 36 |
| R2 | 37 |
| R3 | 126 |
| companies | 2540 |
| canonical quarters | 72765 |
| TTM rows | 53815 |
| score rows | 53815 |
| lifecycle rows | 53815 |
| valuation rows | 53815 |

Four externally verified repairs were applied with identity and old-value guards:

| Ticker | FY/Q | Field | Old | New | Period End |
| --- | --- | --- | ---: | ---: | --- |
| POWW | FY2025 Q1 | revenue | -42159090 | 12281991 | `2025-03-31 -> 2024-06-30` |
| RH | FY2021 Q4 | revenue | -7453000 | 902741000 | `2021-05-01 -> 2022-01-29` |
| VTGN | FY2025 Q1 | revenue | -15000 | 84000 | `2025-03-31 -> 2024-06-30` |
| TBLA | FY2022 Q3 | cash | -445000 | 188477000 | unchanged `2022-09-30` |

Four-case rows applied: `4`; write failures: `0`.

POWW/RH/VTGN identity guards passed with period-collision observations: each corrected period_end already appeared on another existing canonical row for the same ticker, but canonical identity `company_id + fiscal_year + fiscal_quarter` remained unique. No fiscal label rewrite was performed in this phase.

Restated / continuing-operations policy recorded: when a later official filing presents historical quarterly values on a documented restated / continuing-operations basis after discontinued operations or a material business disposal, V3 may prefer the latest official comparable historical value for a consistent canonical series, provided identity is unchanged and lineage records the evidence. This is not a general automatic restatement rule.

R1 after four repairs and local review:

| Area | R1 |
| --- | ---: |
| publish | 0 |
| period_end | 18 |
| revenue | 0 |
| cash | 0 |
| debt | 0 |
| other | 0 |
| total | 18 |

Deep review outcomes:

| Review area | Outcome |
| --- | ---: |
| publish locally resolved / downgraded | 0 |
| period-end downgraded to R2 | 12 |
| period-end downgraded to R3 | 2 |
| period-end external research required | 18 |
| revenue repaired | 3 |
| revenue resolved valid-as-is | 1 |
| cash repaired | 1 |
| debt locally resolved | 0 |

No additional safe repair set beyond the four verified cases was found from local evidence. `phase8a8_additional_safe_repair_set.csv` is intentionally empty.

Final residual queues:

| Queue | Rows |
| --- | ---: |
| R1 | 18 |
| R2 | 49 |
| R3 | 132 |

Final R1 tickers/cases:

`AMST FY2024 Q4`, `CRUS FY2025 Q4`, `DOMO FY2025 Q4`, `EEFT FY2025 Q3`, `FNGR FY2024 Q2`, `IMMR FY2025 Q4`, `INBS FY2025 Q4`, `KALV FY2025 Q3`, `LYTS FY2025 Q1`, `MNR FY2025 Q4`, `MNRO FY2025 Q4`, `NCNO FY2025 Q4`, `RBC FY2025 Q4`, `RCAT FY2024 Q3`, `SKY FY2025 Q4`, `VIVS FY2025 Q1`, `VIVS FY2025 Q2`, `VIVS FY2025 Q4`.

External research queue: `temp/fundamentals_v3_phase8a8_r1_resolution/20260826T045013Z/external_research_queue_R1.csv`

Human summary: `temp/fundamentals_v3_phase8a8_r1_resolution/20260826T045013Z/external_research_queue_R1_human_summary.md`

Canonical proving:

| Metric | Value |
| --- | --- |
| quick_check | `ok` |
| companies before/after | `2540 -> 2540` |
| canonical rows before/after | `72765 -> 72765` |
| duplicate identities | `0` |
| orphan fundamentals | `0` |
| unrelated canonical drift | `0` |
| post-A8 publish audit count | `51` |
| post-A8 semantic audit count | `123` |

Post-A8 Phase 7 read-only limitation: Phase 7's file-metadata sentinel reported a RawCandle `osakedata.db` mtime change during the audit window while the V3 DB mtime stayed unchanged. RawCandle quick_check returned `ok`; row count check was blocked by an external SQLite lock. A8 performed no RawCandle writes.

Downstream rebuild was not run. Derived data remains:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Backup: `temp/fundamentals_v3_phase8a8_r1_resolution/20260826T045013Z/backup/rc_fundamentals_v3_phase8a7_backup.db`

Backup sha256: `641b4c3cb6df1ac6ecf3d983f6aa5ce99f67efe77e0266cdca0310444f770c2d`

Exact next action: `USER EXTERNAL RESEARCH — R1 QUEUE`.

## Phase 8A9 Period-End Apply

Classification: `FUNDAMENTALS_V3_PHASE8A9_SEQUENCE_COLLISION_R1_RETAINED`

Status: `DONE_PARTIAL_GUARDED_APPLY_SEQUENCE_COLLISION_R1_RETAINED`

Input reconciliation passed for `temp/phase8_period_end_R1_verified.csv`: `18` verified rows, `18` unique quarters, `18` DIFFERENT, `18` HIGH confidence, `18` Candidate Value equals Verified Period End, `0` identity conflicts.

Applied exact period_end repairs:

| Ticker | FY/Q | Old | New |
| --- | --- | --- | --- |
| AMST | FY2024 Q4 | 2024-12-31 | 2024-06-30 |
| KALV | FY2025 Q3 | 2024-12-31 | 2025-01-31 |
| LYTS | FY2025 Q1 | 2025-03-31 | 2024-09-30 |

Rows applied: `3`; write guard failures: `0`.

Retained R1: `15`, split as `COLLISION=10` and `SEQUENCE_CONFLICT=5`.

The retained rows require a separate sequence/identity repair decision. A9 intentionally did not relabel FY/FQ, merge quarters, remove rows, or create same-company period_end duplicates.

Production proof: quick_check `ok`, companies `2540 -> 2540`, canonical quarters `72765 -> 72765`, fundamentals rows `72765 -> 72765`, derived rows unchanged at `53815` for TTM, score, lifecycle, and valuation. Same-company period_end duplicate groups remained `3 -> 3`, so A9 introduced no new period_end duplicate group.

Downstream rebuild was not run.

Derived-data status remains:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Backup: `temp/fundamentals_v3_phase8a9_period_end_apply/20260826T052001Z/backup/rc_fundamentals_v3_phase8a9_backup.db`

Backup sha256: `d4b114d35b4ae731096d05844f8613b7c7c1873d2ab67bc31feda950986b5c7f`

Artifact root: `temp/fundamentals_v3_phase8a9_period_end_apply/20260826T052001Z`

Exact next action: `RESOLVE_SEQUENCE_COLLISION_R1_BEFORE_COMBINED_DOWNSTREAM_REBUILD`.

## Phase 8A10A Sequence-Collision Root Cause Analysis

Classification: `FUNDAMENTALS_V3_PHASE8A10A_SEQUENCE_COLLISIONS_EXTERNAL_EVIDENCE_REQUIRED`

Status: `DONE_READ_ONLY_STRUCTURAL_ANALYSIS_REPAIR_PLAN_NOT_PRODUCTION_READY`

Artifact root: `temp/fundamentals_v3_phase8a10a_sequence_collision_analysis/20260826T054812Z`

Phase 8 remains in progress. No production writes were performed.

Frozen R1 set:

| Metric | Count |
| --- | ---: |
| R1 rows | 15 |
| unique tickers | 13 |
| collision rows | 10 |
| sequence-conflict rows | 5 |

Root causes:

| Primary root cause | Count |
| --- | ---: |
| `SHIFTED_MULTI_QUARTER_SEQUENCE` | 11 |
| `52_53_WEEK_CALENDAR_HANDLING` | 4 |

Case summary:

| Ticker | FY/Q | Current | Verified | Root cause | Disposition | Prod-ready |
| --- | --- | --- | --- | --- | --- | --- |
| CRUS | FY2025 Q4 | 2026-03-31 | 2025-03-29 | `52_53_WEEK_CALENDAR_HANDLING` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| DOMO | FY2025 Q4 | 2026-01-31 | 2025-01-31 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| EEFT | FY2025 Q3 | 2025-12-31 | 2025-09-30 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| FNGR | FY2024 Q2 | 2024-05-31 | 2023-08-31 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| IMMR | FY2025 Q4 | 2026-01-31 | 2025-04-30 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| INBS | FY2025 Q4 | 2025-12-31 | 2025-06-30 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| MNR | FY2025 Q4 | 2025-09-30 | 2025-12-31 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| MNRO | FY2025 Q4 | 2026-03-31 | 2025-03-29 | `52_53_WEEK_CALENDAR_HANDLING` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| NCNO | FY2025 Q4 | 2026-01-31 | 2025-01-31 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| RBC | FY2025 Q4 | 2026-03-31 | 2025-03-29 | `52_53_WEEK_CALENDAR_HANDLING` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| RCAT | FY2024 Q3 | 2024-10-31 | 2024-01-31 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| SKY | FY2025 Q4 | 2026-03-31 | 2025-03-29 | `52_53_WEEK_CALENDAR_HANDLING` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| VIVS | FY2025 Q1 | 2025-03-31 | 2024-06-30 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| VIVS | FY2025 Q2 | 2025-06-30 | 2024-09-30 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |
| VIVS | FY2025 Q4 | 2025-12-31 | 2025-03-31 | `SHIFTED_MULTI_QUARTER_SEQUENCE` | `SHIFT_MULTI_QUARTER_SEGMENT` | NO |

The key finding is that the 15 retained cases are not safe single-cell period_end repairs. Each requires a segment-level structural decision because changing only the target period_end would collide with another canonical row or invert the FY/FQ chronology.

Publish-date context corroborates the structural problem: most affected rows have negative publish lags or publish chronology reversals under the current canonical period_end, which is consistent with source-period mapping drift. Publish date remains evidence only and was not repaired.

Initial Phase 8A10B audit rules were defined for fiscal continuity, period_end continuity, publish-date chronology, reporting lag bands, duplicate period_end detection, and 52/53-week exceptions. Initial threshold bands:

- period gap normal: `75-105` days; review: `50-74` or `106-130`; severe: `<=0`, `<50`, `>160`, or annual-like adjacent jumps
- publish gap normal: `60-130` days; review: `30-59` or `131-210`; severe: chronology reversal or `>210`
- reporting lag: negative `<0`, very short `0-6`, normal `7-120`, long `121-240`, extreme `>240`

These are A10A seed thresholds only; Phase 8A10B must recalibrate against the full retained V3 population.

Safety proof:

| Metric | Result |
| --- | --- |
| production writes | 0 |
| RawCandle writes | 0 |
| company rows | 2540 -> 2540 |
| canonical rows | 72765 -> 72765 |
| fundamentals rows | 72765 -> 72765 |
| TTM rows | 53815 -> 53815 |
| score rows | 53815 -> 53815 |
| lifecycle rows | 53815 -> 53815 |
| valuation rows | 53815 -> 53815 |

Downstream remains deferred:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `USER EXTERNAL RESEARCH - ONLY UNRESOLVED STRUCTURAL CASES`, then `PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT`.

## Phase 8A10A-R External Structural Remap Reconciliation

Classification: `FUNDAMENTALS_V3_PHASE8A10A_R_PARTIAL_APPLY_SET_READY_SPECIAL_CASES_REMAIN`

Status: `DONE_READ_ONLY_FROZEN_STRUCTURAL_APPLY_SET_READY_WITH_SPECIAL_CASES`

Artifact root: `temp/fundamentals_v3_phase8a10a_r_remap_reconciliation/20260826T071127Z`

External inputs from `temp/` were validated:

| File | Rows |
| --- | ---: |
| `phase8_structural_R1_official_fiscal_timelines.csv` | 116 |
| `phase8_structural_R1_case_resolution.csv` | 15 |
| `phase8_structural_R1_segment_remap.csv` | 75 |

External case-resolution split: `YES=12`, `NO=3`; external NO tickers are exactly `FNGR`, `IMMR`, and `RCAT`.

Current V3 reconciliation:

| Check | Count |
| --- | ---: |
| exact current row matches | 75 |
| economic match HIGH | 69 |
| economic match MEDIUM | 4 |
| economic match LOW | 2 |
| target empty | 36 |
| target exists, different economic quarter in same rotation | 33 |
| target exists, same economic quarter | 6 |
| unresolved non-null conflicts | 0 |

Frozen structural apply set:

| Metric | Count |
| --- | ---: |
| production-ready ticker groups | 10 |
| canonical rows in frozen apply set | 67 |
| atomic operations | 134 |

Production-ready groups: `CRUS`, `DOMO`, `EEFT`, `INBS`, `MNR`, `MNRO`, `NCNO`, `RBC`, `SKY`, `VIVS`.

Remaining special cases: `FNGR`, `IMMR`, `RCAT`.

- `FNGR`: bounded single-row repair appears possible only with separate approval; external classification remains NO because broader history is sparse.
- `IMMR`: identity and restated value repair are coupled; label-only repair remains blocked.
- `RCAT`: transition-year / 10-KT case; do not move the current 1534727 revenue row to 2024-01-31.

Safety proof: production writes `0`, RawCandle writes `0`, derived writes `0`; companies `2540 -> 2540`, canonical quarters `72765 -> 72765`, fundamentals rows `72765 -> 72765`, and derived rows stayed `53815` for TTM, score, lifecycle, and valuation.

Downstream remains deferred:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `PHASE 8A10A-APPLY - APPLY FROZEN STRUCTURAL QUARTER-SEQUENCE REPAIRS`, plus `SPECIAL CASE RESEARCH - FNGR / IMMR / RCAT AS APPLICABLE`.

## Phase 8A10A-APPLY Frozen Structural Quarter-Sequence Repair Apply

Classification: `FUNDAMENTALS_V3_PHASE8A10A_APPLY_FROZEN_STRUCTURAL_REPAIRS_APPLIED_SPECIAL_CASES_REMAIN`

Status: `DONE_PRODUCTION_STRUCTURAL_REPAIR_APPLIED_DOWNSTREAM_DEFERRED_SPECIAL_CASES_REMAIN`

Artifact root: `temp/fundamentals_v3_phase8a10a_apply/20260826T091635Z`

Frozen authoritative input: `temp/fundamentals_v3_phase8a10a_r_remap_reconciliation/20260826T071127Z/phase8a10a_r_v3_frozen_structural_apply_set.csv`.

Frozen scope:

| Metric | Count |
| --- | ---: |
| transformation groups | 10 |
| canonical rows | 67 |
| atomic operations | 134 |

Applied groups: `CRUS`, `DOMO`, `EEFT`, `INBS`, `MNR`, `MNRO`, `NCNO`, `RBC`, `SKY`, `VIVS`.

Excluded special cases remained untouched: `FNGR`, `IMMR`, `RCAT`.

Write guards and simulation:

| Check | Result |
| --- | ---: |
| groups passing write guards | 10 |
| groups failing write guards | 0 |
| rows passing old-state guards | 67 |
| drifted rows | 0 |
| simulation groups PASS | 10 |
| simulation groups FAIL | 0 |
| duplicate final FY/FQ in simulation | 0 |
| content-loss issues | 0 |
| lineage simulation issues | 0 |

Production apply:

| Metric | Count |
| --- | ---: |
| groups attempted | 10 |
| groups committed | 10 |
| groups rolled back | 0 |
| canonical rows transformed | 67 |
| atomic operations executed | 134 |
| write failures | 0 |

Per-group apply summary:

| Ticker | Rows | Atomic ops | Status |
| --- | ---: | ---: | --- |
| CRUS | 6 | 12 | COMMITTED |
| DOMO | 9 | 18 | COMMITTED |
| EEFT | 9 | 18 | COMMITTED |
| INBS | 5 | 10 | COMMITTED |
| MNR | 5 | 10 | COMMITTED |
| MNRO | 6 | 12 | COMMITTED |
| NCNO | 9 | 18 | COMMITTED |
| RBC | 6 | 12 | COMMITTED |
| SKY | 6 | 12 | COMMITTED |
| VIVS | 6 | 12 | COMMITTED |

Content and lineage integrity:

| Check | Result |
| --- | ---: |
| economic-quarter signatures preserved | 67 / 67 |
| non-null canonical cells before | 794 |
| non-null canonical cells after | 794 |
| publish_date ownership issues | 0 |
| lineage/provenance issues | 0 |
| unrelated canonical drift | 0 |

The 52/53-week tickers `CRUS`, `MNRO`, `RBC`, and `SKY` kept official weekend period ends; the repair did not normalize these to month-end. `VIVS` was applied as one coherent 6-row segment.

Production integrity:

| Metric | Before | After |
| --- | ---: | ---: |
| quick_check | ok | ok |
| companies | 2540 | 2540 |
| canonical quarters | 72765 | 72765 |
| fundamentals rows | 72765 | 72765 |
| TTM rows | 53815 | 53815 |
| score rows | 53815 | 53815 |
| lifecycle rows | 53815 | 53815 |
| valuation rows | 53815 | 53815 |
| duplicate FY/FQ identities | 0 | 0 |
| orphan rows | 0 | 0 |

RawCandle writes: `0`.

Residual structural R1:

| Metric | Count |
| --- | ---: |
| before | 15 |
| after | 3 |
| repaired tickers still R1 | 0 |
| new R1 cases | 0 |

Remaining structural R1 tickers: `FNGR`, `IMMR`, `RCAT`.

Backup: `temp/fundamentals_v3_phase8a10a_apply/20260826T091635Z/backup/rc_fundamentals_v3_phase8a10a_apply_backup.db`

Backup sha256: `8902fe0c9902995b0e92e7962e8a41e984463ed1f8632663a5bc0011a8f25491`

Downstream remains deferred:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `SPECIAL CASE RESEARCH - FNGR / IMMR / RCAT`, then `PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT`.

## Phase 8A10A-SPECIAL FNGR / IMMR / RCAT Structural Resolution

Classification: `FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_PARTIAL_APPLY_SET_READY_EVIDENCE_REMAINS`

Status: `DONE_READ_ONLY_SPECIAL_RESOLUTION_PARTIAL_APPLY_SET_READY`

Artifact root: `temp/fundamentals_v3_phase8a10a_special_resolution/20260826T093155Z`

Starting structural R1: `3` tickers: `FNGR`, `IMMR`, `RCAT`.

FNGR result:

- current row: FY2024 Q2, quarter_id `37082`, period_end `2024-05-31`, publish_date `2023-10-16`, Revenue `8373983`
- official identity: FY2024 Q2, period_end `2023-08-31`, Revenue `8373983`
- sparse-history conclusion: surrounding missing history is non-blocking because the individual economic quarter is independently proven
- production-ready: YES
- frozen operations: `UPDATE_PERIOD_END` to `2023-08-31` and `UPDATE_PUBLISH_DATE` to `2023-10-13`

IMMR result:

- affected current rows: `5`
- current FY2025 Q1-Q4 / FY2026 Q1 segment maps toward FY2025 Q4 / FY2026 Q1-Q4
- at least one restated Revenue repair is required: current `281376000` vs official `284876000` for FY2025 Q4
- other non-null fields still require restated official comparison before apply
- production-ready: NO

RCAT result:

- fiscal transition case: old April 30 fiscal-year regime changed to a December 31 regime through FY2024T transition reporting
- current quarter_id `59126`: FY2024 Q2, period_end `2024-07-31`, Revenue `886440`; official FY2024T Q1 Revenue is `2776535`, so value repair is required
- current quarter_id `59125`: FY2024 Q3, period_end `2024-10-31`, Revenue `1534727`; official transition label is FY2024T Q2 and the `1534727` Revenue stays with `2024-10-31`
- V3 needs explicit deterministic policy for encoding FY2024T transition labels before apply
- production-ready: NO

Frozen special apply set:

| Metric | Count |
| --- | ---: |
| production-ready tickers | 1 |
| transformation groups | 1 |
| canonical rows affected | 1 |
| operations | 2 |
| field-value repairs | 0 |
| identity repairs | 0 |
| period_end repairs | 1 |
| publish_date repairs | 1 |
| merges/deletes/recreates | 0 |

Production-ready ticker: `FNGR`.

Remaining evidence/policy queue: `IMMR`, `RCAT`.

External research queue size: `2`.

Full-V3 audit handoff rules added:

- sparse history is not automatically shifted sequence when the individual economic quarter is independently proven
- restatements may couple identity movement with financial-value replacement
- transition-year / 10-KT periods must be modeled separately from ordinary Q1-Q4 continuity

Safety proof: production writes `0`, RawCandle writes `0`, quick_check `ok -> ok`, companies `2540 -> 2540`, canonical quarters `72765 -> 72765`, fundamentals rows `72765 -> 72765`, TTM/Score/Lifecycle/Valuation `53815 -> 53815`.

Downstream remains deferred:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `PHASE 8A10A-SPECIAL-APPLY - APPLY FNGR BOUNDED SPECIAL REPAIR`, then resolve `IMMR` / `RCAT` evidence before `PHASE 8A10B`.

## Phase 8A10A-SPECIAL-APPLY FNGR Bounded Special Repair

Classification: `FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_APPLY_FNGR_COMPLETE_IMMR_RCAT_REMAIN`

Status: `DONE_PRODUCTION_FNGR_SPECIAL_REPAIR_APPLIED_DOWNSTREAM_DEFERRED`

Artifact root: `temp/fundamentals_v3_phase8a10a_special_apply/20260826T095727Z`

Authoritative input: `temp/fundamentals_v3_phase8a10a_special_resolution/20260826T093155Z/phase8a10a_special_frozen_apply_set.csv`.

Frozen scope:

| Metric | Count |
| --- | ---: |
| transformation groups | 1 |
| canonical rows | 1 |
| operations | 2 |

Target: `FNGR` quarter_id `37082`, FY2024 Q2. Fiscal identity stayed unchanged.

Applied bounded metadata repair:

| Field | Before | After |
| --- | --- | --- |
| period_end | 2024-05-31 | 2023-08-31 |
| publish_date | 2023-10-16 | 2023-10-13 |

Revenue stayed unchanged at `8373983`. All canonical fundamental fields stayed unchanged. Lineage refs stayed `2 -> 2`. Content signature excluding the two intended metadata fields matched before/after.

Sparse FNGR history remains non-blocking: this was not treated as a shifted segment, relabel, merge, or missing-quarter reconstruction.

Reporting lag corrected from a negative impossible lag to `43` calendar days.

Production proof:

| Metric | Before | After |
| --- | ---: | ---: |
| quick_check | ok | ok |
| companies | 2540 | 2540 |
| canonical quarters | 72765 | 72765 |
| fundamentals rows | 72765 | 72765 |
| TTM rows | 53815 | 53815 |
| score rows | 53815 | 53815 |
| lifecycle rows | 53815 | 53815 |
| valuation rows | 53815 | 53815 |
| duplicate FY/FQ identities | 0 | 0 |
| orphan rows | 0 | 0 |

Changed canonical metadata cells: `2`. Write failures: `0`. Unrelated canonical drift: `0`. RawCandle writes: `0`.

Structural R1 after apply: `2`, remaining tickers exactly `IMMR` and `RCAT`. FNGR is no longer structural R1 and is not publish-date R1 for the old `2023-10-16` date. New R1 cases: `0`.

Backup: `temp/fundamentals_v3_phase8a10a_special_apply/20260826T095727Z/backup/rc_fundamentals_v3_phase8a10a_special_fngr_backup.db`

Backup sha256: `f3fc19be53fc5802cf3abe1182065018cb1a6bd0c50994df73b2bb39bc172e70`

Downstream remains deferred:

`DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `USER EXTERNAL RESEARCH - IMMR / RCAT`, then `PHASE 8A10A-SPECIAL-FINAL-APPLY`, then `PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT`.

## Phase 8A10A-SPECIAL-FINAL-RECONCILE IMMR / RCAT External Evidence Reconciliation

Classification: `FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_FINAL_RECONCILE_BLOCKERS_REMAIN`

Status: `DONE_READ_ONLY_SPECIAL_FINAL_RECONCILE_BLOCKERS_REMAIN`

Artifact root: `temp/fundamentals_v3_phase8a10a_special_final_reconcile/20260826T143248Z`

The five external special-case files were located and validated:

| File | Rows |
| --- | ---: |
| `phase8_immr_rcat_official_quarter_matrix.csv` | 19 |
| `phase8_immr_rcat_v3_row_mapping.csv` | 15 |
| `phase8_immr_restatement_field_matrix.csv` | 48 |
| `phase8_immr_rcat_final_transformation_plan.csv` | 13 |
| `phase8_rcat_transition_policy.csv` | 11 |

Current V3 reconciliation matched `15 / 15` mapping rows with current-state drift `0`. Current reviewed segments were `IMMR=31` rows and `RCAT=21` rows.

IMMR is not production-ready. Official mapping covers `8` quarters and identifies `8` identity repairs, `5` publish_date repairs, `3` revenue restatement repairs, `30` other verified field repairs, `7` fields marked `NOT_VERIFIABLE_FROM_RESTATEMENT_SOURCE`, `1` obsolete row, and `5` target collisions. The blocker is qid `42578` / target FY2025 Q1: `DELETE_AND_RECREATE` would replace a non-matching current row while non-null `gross_profit` and `shares_outstanding` values remain unverified. Exact missing decision: whether those July-only fields may be discarded or must be separately preserved.

RCAT is not production-ready. The official evidence requires the FY2024T transition namespace and a STUB period; current canonical schema cannot encode either truthfully because `v3_quarter.fiscal_year` is `INTEGER` and `v3_quarter.fiscal_quarter` is constrained to `Q1`-`Q4`. Synthetic Q3 is explicitly rejected as semantically false, while excluding the STUB would lose an economic reporting period. RCAT has `4` identity repairs, `2` value repairs, `2` publish_date repairs, and `3` create/delete/merge-shaped row operations pending a schema or policy decision.

Frozen apply set is intentionally empty: production-ready tickers `0`, transformation groups `0`, canonical rows affected `0`, atomic operations `0`, canonical value writes `0`, identity writes `0`, metadata writes `0`, lineage actions `0`.

Safety proof: production writes `0`, RawCandle writes `0`, derived writes `0`; quick_check stayed `ok`, companies `2540`, canonical quarters `72765`, fundamentals rows `72765`, and TTM/Score/Lifecycle/Valuation rows `53815`.

Phase 8 remains: `IN PROGRESS - IMMR EVIDENCE BLOCKER AND RCAT TRANSITION/STUB POLICY BEFORE GLOBAL AUDIT`

Downstream remains: `DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: resolve exact IMMR evidence/preservation decision and RCAT transition/STUB architecture policy before any IMMR/RCAT production apply, then proceed to `PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT`.

## Phase 8A10A-SPECIAL-REMOVE IMMR / RCAT V3 Removal

Classification: `FUNDAMENTALS_V3_PHASE8A10A_SPECIAL_REMOVE_COMPLETE_STRUCTURAL_R1_CLOSED`

Status: `DONE_PRODUCTION_IMMR_RCAT_REMOVED_DOWNSTREAM_DEFERRED`

Artifact root: `temp/fundamentals_v3_phase8a10a_special_remove/20260826T145046Z`

User-approved universe decision: `IMMR` and `RCAT` were removed completely from V3 because their remaining structural repairs required disproportionate issuer-specific handling. This preserves the standard V3 architecture and avoids IMMR restatement remapping, RCAT FY2024T encoding, RCAT STUB encoding, and transition-period schema changes.

Frozen company identities:

| Ticker | company_id | active | market | admission |
| --- | ---: | ---: | --- | --- |
| `IMMR` | 1159 | 1 | usa | `PHASE3B_APPROVED_BASELINE` / `ACTIVE` |
| `RCAT` | 1919 | 1 | usa | `PHASE3B_APPROVED_BASELINE` / `ACTIVE` |

Inventory before removal:

| Ticker | canonical | fundamentals | lineage/source/status | TTM | Score | Lifecycle | Valuation | core-ready quarters |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `IMMR` | 31 | 31 | 89 | 22 | 22 | 22 | 22 | 5 |
| `RCAT` | 21 | 21 | 94 | 5 | 5 | 5 | 5 | 6 |

Delete plan affected `9` tables with `397` manually guarded rows. Company rows were deleted last after valuation, lifecycle, score, TTM, V3 provenance/status rows, fundamentals, and canonical quarters. All delete-count parity checks passed.

Production deltas:

| Table/count | Before | After | Delta |
| --- | ---: | ---: | ---: |
| companies | 2540 | 2538 | -2 |
| active companies | 2472 | 2470 | -2 |
| inactive companies | 68 | 68 | 0 |
| canonical quarters | 72765 | 72713 | -52 |
| fundamentals rows | 72765 | 72713 | -52 |
| TTM rows | 53815 | 53788 | -27 |
| Score rows | 53815 | 53788 | -27 |
| Lifecycle rows | 53815 | 53788 | -27 |
| Valuation rows | 53815 | 53788 | -27 |

Integrity proof: quick_check `ok -> ok`, duplicate FY/FQ `0`, orphan rows `0`, foreign_key_check rows `0`, retained unrelated drift `0`, RawCandle writes `0`.

Structural R1 closed: `2 -> 0`; remaining structural R1 tickers `[]`; new structural R1 `0`.

Residual high-level status after removal: publish R1 `12`, publish R2 `5`, publish R3 `0`; semantic R1/R2/R3 `0/0/0`; structural R1/R2/R3 `0/0/0`.

Backup: `temp/fundamentals_v3_phase8a10a_special_remove/20260826T145046Z/backup/rc_fundamentals_v3_phase8a10a_special_remove_backup.db`

Backup sha256: `5db135fe5f7661715ffc2989207cfd3801a2d1bb01e75c12a9b694aab451690c`

Phase 8 remains: `IN PROGRESS - FULL V3 AUDIT NEXT`

Downstream remains: `DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT`.

## Phase 8A10B Full V3 Fiscal Sequence / Period-End / Publish-Date Audit

Classification: `FUNDAMENTALS_V3_PHASE8A10B_FULL_AUDIT_EXTERNAL_RESEARCH_REQUIRED`

Status: `DONE_READ_ONLY_FULL_V3_AUDIT_P1_EXTERNAL_RESEARCH_REQUIRED`

Artifact root: `temp/fundamentals_v3_phase8a10b_full_sequence_audit/20260826T152346Z`

Baseline after IMMR/RCAT removal: companies `2538`, active `2470`, inactive `68`, canonical quarters `72713`, fundamentals rows `72713`, and TTM/Score/Lifecycle/Valuation rows `53788` each. `quick_check=ok`, duplicate FY/FQ `0`, orphan fundamentals `0`, and foreign_key_check rows `0`.

The audit covered all `2538` retained V3 companies. Fiscal/date sequence calculations covered `2533` companies with canonical quarter rows and `72713` quarters; `5` retained companies currently have zero canonical quarters.

Publish residual reconciliation resolved the apparent discrepancy from the removal phase. The current `17` publish residual rows are the post-removal heuristic population: `12` current true R1 publish-date anomalies plus `5` R2 market_availability-only flags. The earlier externally verified 17-row publish_date apply set remains R1-closed; only `1` of that original set is still residual, the known `BCTX` R2 market_availability stale case. Therefore the current `R1=12 / R2=5` does not mean the verified 17 repairs failed.

Empirical distributions:

| Distribution | Count | P50 | P90 | P95 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| period_end gap days | 70176 | 91 | 92 | 182 | 275 | 2011 |
| publish gap days | 65009 | 91 | 121 | 181 | 273 | 1210 |
| reporting lag days | 69248 | 37 | 58 | 73 | 455 | 1566 |

Hard rules used: duplicate FY/Q, non-positive period_end progression, duplicate period_end with corroborating context, and publish_date before period_end. Soft empirical rules used: unusually short/long period gaps, unusually long publish intervals, and long/extreme reporting lags. Missing expected FY/Q rows were classified as `MISSING_HISTORY` unless another signal supported wrong canonical mapping.

Audit results:

| Area | Count |
| --- | ---: |
| duplicate FY/Q | 0 |
| missing-quarter observations | 7434 |
| reverse fiscal-label sequences | 0 |
| one-year shift candidates | 165 |
| multi-quarter missing/shift observations | 1262 |
| likely duplicate economic quarter rows | 6 |
| negative reporting lags | 12 |
| valid 52/53-week sentinels | 4 |

Severity classification:

| Severity | Rows | Companies |
| --- | ---: | ---: |
| P1 | 15 | 13 |
| P2 | 5937 | 2368 |
| P3 | 4640 | 2320 |

All P1 rows are classified `EXTERNAL_RESEARCH_REQUIRED`. P1 tickers are `BBY`, `DELL`, `FNGR`, `GCO`, `HAE`, `MRVL`, `POWW`, `RH`, `RL`, `SAIC`, `TJX`, `TRNS`, and `VTGN`. Latest/current-state impact candidates: cross-signal latest/current impact `16`, current-TTM impact `16`. The bounded external research queue contains `15` rows / `13` tickers.

Safety proof: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle writes `0`, Valuation writes `0`, and baseline unchanged `1`.

Phase 8 remains: `IN PROGRESS - GLOBAL P1 EXTERNAL RESEARCH REQUIRED`

Downstream remains: `DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `USER EXTERNAL RESEARCH - GLOBAL P1 QUEUE`.

## Phase 8A10B-P2P3 - 2024+ / Last-8Q Reprioritization

Classification: `FUNDAMENTALS_V3_PHASE8A10B_P2P3_REPRIORITIZED_CURRENT_CRITICAL_REVIEW_REQUIRED`

Status: `DONE_READ_ONLY_P2P3_REPRIORITIZATION_CURRENT_CRITICAL_QUEUE_READY`

Artifact root: `temp/fundamentals_v3_phase8a10b_p2p3_reprioritization/20260826T162000Z`

This phase intentionally did not try to manually perfect all historical P2/P3 findings. Full historical perfection is not required for Phase 8 closure because most broad P2/P3 findings are old history gaps, long but plausible issuer/reporting intervals, or non-blocking informational sequence observations. The practical priority is current data quality: 2024+ rows, each company latest eight canonical quarters, the latest quarter, the current four-quarter TTM input window, and rows capable of distorting current Score/Lifecycle/Valuation.

Priority rules: `latest-8Q` is the primary user window, and companies with fewer than eight canonical quarters use all available quarters. `2024+` is a recency cut based on period_end or publish chronology, not fiscal-year inference. `P2A_CURRENT_CRITICAL_REVIEW` requires recent/latest-8Q scope plus current downstream impact or a material structural signal such as reverse chronology, negative reporting lag, one-year shift, duplicate economic quarter, or severe short period gap. `P2B_RECENT_NONBLOCKING` captures 2024+/latest-8Q P2 rows where evidence points more toward unusual reporting behavior, sparse/deep-history availability, or isolated metadata oddity than wrong canonical mapping. `P2C_HISTORICAL_DEFERRED` captures non-current P2 rows that should remain documented but not block Phase 8. `P3A_RECENT_INFORMATIONAL` captures recent/latest-8Q accepted or harmless P3 observations. `P3B_HISTORICAL_INFORMATIONAL` captures old accepted P3 observations. `P3_ESCALATED` is limited to recent/latest-8Q P3 rows with multiple independent material structural signals and current impact.

Starting population: P2 `5937` rows / `2368` companies, P3 `4640` rows / `2320` companies, with P1 `15` rows explicitly excluded because P1 is already in a separate external research queue.

Recent window mapping: retained companies `2538`, companies with quarters `2533`, companies with at least eight quarters `2514`, companies with fewer than eight quarters `19`, latest-8Q window quarters `20232`, latest-4Q window quarters `10132`, and 2024+ canonical quarters `24211`.

Reprioritization result:

| Bucket | Rows | Companies |
| --- | ---: | ---: |
| P2A current critical | 154 | 122 |
| P2B recent non-blocking | 2665 | 2290 |
| P2C historical deferred | 3118 | 1407 |
| P3A recent informational | 2495 | 2282 |
| P3B historical informational | 2132 | 1228 |
| P3 escalated | 12 | 12 |

Current-critical queue: `166` rows / `122` tickers. It contains latest-quarter rows `42`, latest-4Q rows `80`, latest-8Q rows `144`, and 2024+ rows `157`. Current downstream impact rows: TTM `57`, Score `10`, Lifecycle `10`, Valuation `10`.

Signal structure in the current-critical queue: single-signal rows `17`, multi-signal rows `149`, one-year shift candidates `49`, duplicate-economic-quarter candidates `2`, publish/period contradictions `85`, systemic recent pattern types `7`.

Recommended action counts in the queue: `EXTERNAL_RESEARCH=136`, `LOCAL_EVIDENCE_REVIEW=30`. No rows were classified as `MARKET_AVAILABILITY_ONLY`, `MISSING_HISTORY_NON_BLOCKING`, or `VALID_52_53_WEEK` inside the current-critical queue.

Safety proof: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle writes `0`, Valuation writes `0`, and baseline unchanged `1`.

Phase 8 remains: `IN PROGRESS - GLOBAL P1 AND CURRENT-CRITICAL P2/P3 REVIEW REQUIRED`

Downstream remains: `DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `RESOLVE CURRENT-CRITICAL 2024+ / LAST-8Q P2A/P3_ESCALATED`.

## Phase 8A10C - Local Review & Current-Downstream External Queue

Classification: `FUNDAMENTALS_V3_PHASE8A10C_LOCAL_REVIEW_COMPLETE_CURRENT_DOWNSTREAM_EXTERNAL_RESEARCH_REQUIRED`

Status: `DONE_READ_ONLY_LOCAL_REVIEW_CURRENT_DOWNSTREAM_QUEUE_READY`

Artifact root: `temp/fundamentals_v3_phase8a10c_local_review/20260826T165000Z`

This phase reviewed the `30` current-critical `LOCAL_EVIDENCE_REVIEW` rows from Phase 8A10B-P2P3 using only local V3, migration, provider acquisition, and resolution evidence. No web/network research was performed. The global P1 queue remains separate and was not mixed into this P2/P3 queue.

Local review results: exact current matches `24`, current snapshot drift `6`, locally confirmed valid false positives `23`, valid special cases `6`, external research still required `1`, local repair-ready cases `0`, P1 escalations `0`. The six drift rows were all snapshot publish-date enrichments where the A10B queue row had blank publish_date but current V3 contains a publish_date while period_end and identity still match: `BNC`, `WS`, `LFCR`, `ACI`, `BSLK`, and `FERG`.

Current-impact narrowing: before local review, the combined current-critical queue had current TTM impact `57`, Score `10`, Lifecycle `10`, and Valuation `10`. After local review and P1 exclusion, unresolved current-impact P2/P3 cases were TTM `35`, Score `3`, Lifecycle `3`, and Valuation `3`; unique current-impact issues before and after dedupe were both `35`.

Final external queue: `35` rows / `30` tickers. Priority split: Priority 1 `3`, Priority 2 `32`, Priority 3 `0`. The queue contains latest-quarter rows `3`, latest-4Q rows `26`, latest-8Q rows `35`, multi-signal rows `35`, and single-signal rows `0`.

Latest-8Q non-blocking backlog: `7` rows / `6` tickers, latest-quarter backlog `2`, latest-4Q backlog `3`. `BLOCKS_PHASE8_CLOSURE=NO`.

Global P1 exclusion was verified: P1 rows excluded `15`; final queue overlap with global P1 `0`.

Safety proof: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle writes `0`, Valuation writes `0`, and baseline unchanged `1`.

Closure policy: current downstream correctness blocks; wider historical/recent backlog does not block Phase 8 closure.

Phase 8 remains: `IN PROGRESS - GLOBAL P1 AND CURRENT-DOWNSTREAM P2/P3 EXTERNAL RESEARCH REQUIRED`

Downstream remains: `DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `USER EXTERNAL RESEARCH - CURRENT TTM / SCORE / LIFECYCLE / VALUATION QUEUE`.

## Phase 8A10A-PUBLISH-APPLY Verified Publish-Date Residual Closure

Classification: `FUNDAMENTALS_V3_PHASE8A10A_PUBLISH_VERIFIED_REPAIRS_COMPLETE`

Status: `DONE_PRODUCTION_PUBLISH_DATE_REPAIR_APPLIED_DOWNSTREAM_DEFERRED`

Artifact root: `temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z`

Authoritative input: `temp/phase8_publish_date_residual_17_verified.csv`.

Verified input contract passed: rows `17`, `DIFFERENT` `17`, `HIGH` confidence `17`, identity conflicts `0`, uncertain/not-found rows `0`, verified publish dates complete `17`, ISO dates `17`, unique ticker/FY/FQ identities `17`. Source coverage was `6` rows with 2+ sources and `11` rows with one source.

Applied scope was exactly the verified 17-row publish_date repair set for `ABVC`, `BCTX`, `BJDX`, `BOC`, `BRTX` (2 rows), `KLRS`, `LWLG`, `NWTG`, `OLB`, `OMEX`, `ORBS`, `PROP`, `RIME`, `RNAZ`, `SLXN`, and `TELO`.

Canonical publish_date policy remains first public disclosure of the relevant financial results. This phase preserved these regression cases:

- `BRTX` FY2020 Q2 and FY2020 Q3 both use `2021-04-12`; same-day multi-quarter publication is accepted for delayed reporting.
- `RIME` uses `2025-04-15` because the 10-K was public before the later earnings release.
- `KLRS`, `ORBS`, and `NWTG` ticker/name history was accepted as same-registrant evidence, not identity conflict.
- `LWLG` and `NWTG` use the earlier earnings release where it preceded the SEC filing.
- `BCTX` FY2026 Q1 keeps period_end `2025-10-31`; fiscal identity was not inferred from calendar year.

Production apply committed one transaction. Rows updated: `17`; changed canonical cells: `17`; write failures: `0`. Only `v3_quarter.publish_date` changed. Period_end changes `0`; FY/FQ changes `0`; fundamentals changes `0`; lineage changes `0`; unrelated canonical drift `0`.

Publish residual audit moved raw heuristic flags from `35` to `19`. For the original verified 17 cases, retained publish R1 is `0`; one case (`BCTX`) remains as R2 because `market_availability_date` is stale relative to the corrected publish_date. This phase intentionally did not change `market_availability_date`.

Production integrity stayed stable: quick_check `ok -> ok`, companies `2540 -> 2540`, canonical quarters `72765 -> 72765`, fundamentals rows `72765 -> 72765`, and TTM/Score/Lifecycle/Valuation rows `53815 -> 53815`. Duplicate FY/FQ identities stayed `0`, orphan rows stayed `0`, RawCandle writes `0`.

Backup: `temp/fundamentals_v3_phase8a10a_publish_apply/20260826T133006Z/backup/rc_fundamentals_v3_phase8a10a_publish_apply_backup.db`

Backup sha256: `812c6e4c75ec23b63cf91760b245f4487e558a5d7b06665ba9b1d0ff20346ef6`

Phase 8 remains: `IN PROGRESS - IMMR/RCAT SPECIAL RESEARCH BEFORE GLOBAL AUDIT`

Downstream remains: `DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `USER EXTERNAL RESEARCH - IMMR / RCAT`, then `PHASE 8A10A-SPECIAL-FINAL-APPLY`, then `PHASE 8A10B - FULL V3 FISCAL QUARTER SEQUENCE / PERIOD_END / PUBLISH_DATE AUDIT`.

## Phase 8A10D-R - Global P1 Segment Reconciliation

Classification: `FUNDAMENTALS_V3_PHASE8A10D_R_SEGMENT_RECONCILIATION_BLOCKERS_REMAIN`

Status: `DONE_READ_ONLY_SEGMENT_RECONCILIATION_PRODUCTION_APPLY_BLOCKED`

Artifact root: `temp/fundamentals_v3_phase8a10d_r_segment_reconciliation/20260826T171500Z`

This phase reconciled the full surrounding fiscal-quarter segments for the `13` global P1 tickers from the Phase 8A10B audit: `BBY`, `DELL`, `FNGR`, `GCO`, `HAE`, `MRVL`, `POWW`, `RH`, `RL`, `SAIC`, `TJX`, `TRNS`, and `VTGN`. Scope stayed read-only against production; the proposed external P1 transformation package was applied only to a disposable copy of `rc_fundamentals_v3.db`, then the exact A10B global P1 audit was re-run on that copy.

Input contract: current A10B P1 rows `15`, verified case rows `15`, transformation rows `19`, fundamental repair rows `2`, unique tickers `13`, confidence `HIGH=14 / MEDIUM=1`, production-ready flags from external package `YES=14 / NO=1`.

Rehearsal result: P1 before `15`, P1 after `16`, original P1 resolved `0`, new P1 introduced `0`. The rehearsal proved that applying the externally verified row-level corrections alone is not safe as a production apply set because the surrounding fiscal-year/quarter segments remain inconsistent.

Root-cause classification:

| Root cause | Tickers |
| --- | --- |
| `ONE_YEAR_PERIOD_SHIFT` | `BBY`, `DELL`, `GCO`, `HAE`, `MRVL`, `RL`, `SAIC`, `TJX`, `TRNS` |
| `MULTI_QUARTER_SEGMENT_SHIFT` | `FNGR` |
| `WRONG_PUBLISH_ASSIGNMENT` | `POWW`, `VTGN` |
| `DUPLICATE_ECONOMIC_QUARTER` | `RH` |

Repair-scope classification: nine FY2026 Q1 retail/fiscal-calendar cases are `MULTI_ROW_METADATA_SEGMENT`; `FNGR` is `MIXED_STRUCTURAL_AND_VALUE_REPAIR`; `POWW` and `VTGN` remain `SINGLE_ROW_METADATA` but did not pass the exact post-rehearsal A10B closure gate; `RH` remains `NO_SAFE_REPAIR_YET`.

RH collision finding: current `RH` FY2022 Q1 has period_end `2021-07-31` and publish_date `2021-09-08`, which maps to official RH FY2021 Q2, but the current canonical FY2021 Q2 target already exists and represents a different economic quarter. RH therefore remains blocked pending a field-level collision/merge policy.

Frozen production apply set: empty. Blocked ticker groups: `13`. Production-ready ticker groups: `0`. Rehearsal integrity stayed clean: quick_check `ok`, duplicate FY/FQ `0`, orphan rows `0`.

Safety proof: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle writes `0`, Valuation writes `0`. Production baseline stayed unchanged at companies `2538`, canonical/fundamentals rows `72713`, and TTM/Score/Lifecycle/Valuation rows `53788`.

Independent current-state A10B validation under `temp/fundamentals_v3_phase8a10d_r_segment_reconciliation/20260826T171500Z/validation_a10b_current` confirmed the same production baseline: P1 `15` rows / `13` companies, production writes `0`, RawCandle writes `0`.

Phase 8 remains: `IN PROGRESS - GLOBAL P1 SEGMENT BLOCKERS AND CURRENT-DOWNSTREAM P2/P3 EXTERNAL RESEARCH REQUIRED`

Downstream remains: `DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `DO NOT WRITE PRODUCTION - RESOLVE ONLY THE REMAINING BLOCKER SEGMENTS`.

## Phase 8A10E - One-Year Period-End Shift Root Cause

Classification: `FUNDAMENTALS_V3_PHASE8A10E_BLOCKERS_REMAIN`

Status: `DONE_READ_ONLY_ONE_YEAR_SHIFT_ANALYSIS_PERIOD_END_ONLY_REPAIR_REJECTED`

Artifact root: `temp/fundamentals_v3_phase8a10e_one_year_period_shift/20260826T174000Z`

Scope was exactly the nine A10D-R `ONE_YEAR_PERIOD_SHIFT` tickers: `BBY`, `DELL`, `GCO`, `HAE`, `MRVL`, `RL`, `SAIC`, `TJX`, and `TRNS`. `FNGR`, `POWW`, `RH`, and `VTGN` remained out of scope.

Starting state: nine-ticker P1 rows `9`, global P1 rows `15`, production baseline unchanged at companies `2538`, canonical/fundamentals rows `72713`, and TTM/Score/Lifecycle/Valuation rows `53788`.

The simple hypothesis was rejected. The common surface symptom is a 52/53-week Yahoo month-end period metadata row offset by one year or nearby month-end normalization, but the affected rows are not proven FY/FQ/content-correct. Content alignment found `FYQ_CORRECT_CONTENT_WRONG=18`, `FYQ_WRONG_CONTENT_CORRECT_FOR_ANOTHER_QUARTER=19`, and `UNRESOLVED=48`. Period-offset alignment found `PLUS_ONE_YEAR_MONTH_END_NORMALIZED=16`, `PLUS_ONE_YEAR_SAME_MONTH_DAY=2`, and `NO_OFFICIAL_MATCH=67`.

Per-ticker segment result:

| Ticker | FY/FQ correct | Content correct | Publish correct | First bad | Last bad | Bad rows | Offset pattern | Repair rows | Production-ready |
| --- | --- | --- | --- | --- | --- | ---: | --- | ---: | --- |
| `BBY` | NO | NO | NO/UNVERIFIED | FY2024 Q4 | FY2026 Q1 | 5 | `PLUS_ONE_YEAR_MONTH_END_NORMALIZED` | 0 | NO |
| `DELL` | NO | NO | NO/UNVERIFIED | FY2024 Q4 | FY2026 Q1 | 5 | `PLUS_ONE_YEAR_MONTH_END_NORMALIZED`, `PLUS_ONE_YEAR_SAME_MONTH_DAY` | 0 | NO |
| `GCO` | NO | NO | NO/UNVERIFIED | FY2024 Q4 | FY2026 Q1 | 4 | `PLUS_ONE_YEAR_MONTH_END_NORMALIZED` | 0 | NO |
| `HAE` | UNPROVEN | NO | NO/UNVERIFIED | FY2025 Q4 | FY2026 Q1 | 2 | `PLUS_ONE_YEAR_MONTH_END_NORMALIZED` | 0 | NO |
| `MRVL` | NO | NO | NO/UNVERIFIED | FY2024 Q4 | FY2026 Q1 | 5 | `PLUS_ONE_YEAR_MONTH_END_NORMALIZED` | 0 | NO |
| `RL` | NO | NO | NO/UNVERIFIED | FY2025 Q1 | FY2026 Q1 | 3 | `PLUS_ONE_YEAR_MONTH_END_NORMALIZED` | 0 | NO |
| `SAIC` | NO | NO | NO/UNVERIFIED | FY2024 Q1 | FY2026 Q1 | 6 | `PLUS_ONE_YEAR_MONTH_END_NORMALIZED`, `PLUS_ONE_YEAR_SAME_MONTH_DAY` | 0 | NO |
| `TJX` | NO | NO | NO/UNVERIFIED | FY2025 Q1 | FY2026 Q1 | 4 | `PLUS_ONE_YEAR_MONTH_END_NORMALIZED` | 0 | NO |
| `TRNS` | NO | NO | NO/UNVERIFIED | FY2025 Q1 | FY2026 Q1 | 3 | `PLUS_ONE_YEAR_MONTH_END_NORMALIZED` | 0 | NO |

Frozen repair result: repair rows `0`, period_end-only rows `0`, identity changes `0`, publish changes `0`, fundamental changes `0`, blocked rows `9`. No production apply set was frozen because period_end-only correctness was not proven for any of the nine ticker groups.

Source-code trace: likely responsible historical path is `swingmaster/fundamentals/v3_yahoo_canonical_seed.py::prepare_yahoo_seed`, which builds canonical candidates with `period_end_date=row["period_end_date"]` from normalized Yahoo rows while storing `official_period_end_date` only in metadata. `swingmaster/fundamentals/v3_canonical_migration.py::_apply_dates` then treats conflicting period dates through conflict/safe-variant policy rather than substituting the official period_end. This is classified as `HISTORICAL_MIGRATION_ARTIFACT=YES`, `ACTIVE_SYSTEMIC_INGESTION_BUG=NO`, with `future_prevention_required=YES` if the same Yahoo seed path is reused.

Rehearsal applied `0` repair rows on a disposable DB copy. Integrity stayed clean: quick_check `ok`, duplicate FY/FQ `0`, orphan rows `0`, unrelated drift `0`.

Exact A10B re-audit on the rehearsal DB: nine-ticker P1 `9 -> 9`, global P1 `15 -> 15`, new P1 `0`. Remaining P1 tickers are `BBY`, `DELL`, `FNGR`, `GCO`, `HAE`, `MRVL`, `POWW`, `RH`, `RL`, `SAIC`, `TJX`, `TRNS`, and `VTGN`.

Safety proof: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle writes `0`, Valuation writes `0`.

Phase 8 remains: `IN PROGRESS - NINE-TICKER FISCAL IDENTITY/CONTENT SEGMENTS AND OTHER P1 FOLLOW-UPS REQUIRED`

Downstream remains: `DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `DO NOT WRITE PRODUCTION - RESOLVE NINE-TICKER FISCAL IDENTITY/CONTENT SEGMENTS BEFORE PERIOD_END APPLY`.

## Phase 8A10E-R - Nine-Ticker Latest-8Q Official Mapping

Classification: `FUNDAMENTALS_V3_PHASE8A10E_R_MAPPING_BLOCKED`

Status: `DONE_READ_ONLY_LATEST8Q_MAPPING_REHEARSAL_BLOCKED`

Artifact root: `temp/fundamentals_v3_phase8a10e_r_latest8q_mapping/20260826T_PHASE8A10E_R`

Authoritative latest-8Q input: `temp/swingmaster_v3_official_fiscal_quarter_timeline_2026-08-26.csv`

The external latest-8Q timeline validated cleanly: rows `72`, tickers `9`, rows per ticker `8`, confidence `HIGH=72`. Latest included quarter is `TJX FY2027 Q2`; all other eight tickers end at `FY2027 Q1`. The primary 72-row timeline does not contain Revenue/OI/NI fingerprint columns, so `temp/phase8_global_P1_official_fiscal_timelines.csv` was used only as supplemental fingerprint evidence where available.

Current V3 candidate extraction covered `79` rows across the nine tickers. Current candidate rows by ticker: `BBY=10`, `DELL=8`, `GCO=10`, `HAE=7`, `MRVL=10`, `RL=7`, `SAIC=10`, `TJX=10`, `TRNS=7`.

Ticker-level result:

| Ticker | Exact mapped | Unmatched current | Missing official | Root cause | Proposed shape | Affected rows | Ready |
| --- | ---: | ---: | ---: | --- | --- | ---: | --- |
| `BBY` | 1 | 3 | 3 | `CONTENT_MAPPING_ERROR` | `UPDATE_PERIOD_END|UPDATE_PUBLISH_DATE` | 1 | NO |
| `DELL` | 1 | 1 | 3 | `CONTENT_MAPPING_ERROR` | `UPDATE_PERIOD_END|UPDATE_PUBLISH_DATE` | 1 | NO |
| `GCO` | 1 | 4 | 3 | `CONTENT_MAPPING_ERROR` | `UPDATE_PERIOD_END|UPDATE_PUBLISH_DATE` | 2 | NO |
| `HAE` | 0 | 2 | 4 | `CONTENT_MAPPING_ERROR` | `NO_WRITE` | 0 | NO |
| `MRVL` | 1 | 3 | 3 | `CONTENT_MAPPING_ERROR` | `UPDATE_PERIOD_END|UPDATE_PUBLISH_DATE` | 1 | NO |
| `RL` | 0 | 1 | 4 | `CONTENT_MAPPING_ERROR` | `NO_WRITE` | 0 | NO |
| `SAIC` | 1 | 2 | 3 | `CONTENT_MAPPING_ERROR` | `UPDATE_PERIOD_END` | 1 | NO |
| `TJX` | 1 | 3 | 3 | `CONTENT_MAPPING_ERROR` | `UPDATE_PERIOD_END|UPDATE_PUBLISH_DATE` | 1 | NO |
| `TRNS` | 0 | 1 | 4 | `CONTENT_MAPPING_ERROR` | `NO_WRITE` | 0 | NO |

All nine tickers still have missing latest-8Q official quarters, duplicate/target-collision risk, and content-verification conflicts. No ticker-level group passed the production-ready gate.

Rehearsal applied only the locally derivable metadata operations to a disposable DB copy. Groups attempted `6`, groups passed `6`, groups failed `0`, quick_check `ok`, duplicate FY/FQ `0`, orphan rows `0`, unrelated drift `0`, but official latest-8Q timeline parity failed and exact A10B did not improve: nine-ticker P1 `9 -> 9`, global P1 `15 -> 15`, new P1 `0`.

Frozen production apply set is empty: production-ready ticker groups `0`, blocked groups `9`, canonical rows affected `0`, atomic operations `0`, period_end writes `0`, publish writes `0`, identity writes `0`, value writes `0`, creates `0`, merges `0`, deletes `0`.

Prevention implication: the historical Yahoo seed failure is now refined as multiple failure modes, not a simple period_end-only defect. Official period_end must outrank normalized provider dates, but future write guards must also detect missing latest-8Q targets, economic-content shifts, target collisions, and content-verification gaps before allowing canonical writes.

Safety proof: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle writes `0`, Valuation writes `0`.

Phase 8 remains: `IN PROGRESS - NINE-TICKER MAPPING BLOCKERS AND CURRENT-DOWNSTREAM SAFE APPLY REMAIN`

Exact next action: `DO NOT WRITE PRODUCTION - RESOLVE ONLY BLOCKED TICKERS`

## Phase 8A10E-R2 - Nine-Ticker Financial-Fingerprint Mapping

Classification: `FUNDAMENTALS_V3_PHASE8A10E_R2_MAPPING_BLOCKED`

Status: `DONE_READ_ONLY_FINANCIAL_FINGERPRINT_MAPPING_REHEARSAL_BLOCKED`

Artifact root: `temp/fundamentals_v3_phase8a10e_r2_financial_mapping/20260827T_PHASE8A10E_R2_FINAL`

Authoritative enriched latest-8Q input: `temp/swingmaster_v3_official_fiscal_quarter_timeline_with_financials_2026-08-26.csv`

The enriched official timeline validated cleanly: rows `72`, tickers `9`, rows per ticker `8`, confidence `HIGH=72`, Revenue populated `72/72`, Operating Income populated `72/72`, and Net Income populated `72/72`. `TJX` latest included quarter is `FY2027 Q2`; all other eight tickers latest included quarter is `FY2027 Q1`.

Financial-fingerprint matching now dominates date/FY/FQ evidence. Current FY/FQ and period_end may be wrong, so the mapper compares Revenue, Operating Income, and Net Income using scale/rounding tolerance and blocks date-only matches when the financial fingerprint contradicts the official row.

Mapping improvement:

| Metric | Count |
| --- | ---: |
| Previous exact date mappings | 6 |
| New financial-high mappings | 33 |
| Ambiguous mappings | 0 |
| Missing official/current economic quarters | 39 |
| No-financial-match current rows | 22 |

Ticker-level result:

| Ticker | Financial-high mappings | Missing official/current economic quarters | Target collisions | Production ready |
| --- | ---: | ---: | ---: | --- |
| `BBY` | 2 | 6 | 1 | NO |
| `DELL` | 3 | 5 | 1 | NO |
| `GCO` | 4 | 4 | 2 | NO |
| `HAE` | 3 | 5 | 2 | NO |
| `MRVL` | 2 | 6 | 1 | NO |
| `RL` | 1 | 7 | 1 | NO |
| `SAIC` | 6 | 2 | 3 | NO |
| `TJX` | 6 | 2 | 3 | NO |
| `TRNS` | 6 | 2 | 2 | NO |

Financial evidence: Revenue exact/rounding matches `61`, Operating Income exact/rounding matches `41`, Net Income exact/rounding matches `64`, contradictory fingerprint matrix rows `43`. This proves the nine-ticker problem is economic-content displacement plus missing/colliding target segments, not a generic period_end-only offset.

Frozen production apply set is empty: production-ready ticker groups `0`, blocked groups `9`, operations `0`, period_end writes `0`, publish writes `0`, identity writes `0`, value writes `0`, creates `0`, merges `0`, deletes `0`.

Rehearsal applied `0` transformations on a disposable DB copy. Integrity stayed clean: quick_check `ok`, duplicate FY/FQ `0`, orphan rows `0`. Exact post-rehearsal A10B did not improve: nine-ticker P1 `9 -> 9`, global P1 `15 -> 15`, new P1 `0`.

Safety proof: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle writes `0`, Valuation writes `0`. RawCandle external drift was observed during the read-only run, but R2 does not read or write RawCandle; the exact A10B rehearsal used the same V3 audit logic with an unused RawCandle sentinel guard.

Phase 8 remains: `IN PROGRESS - NINE-TICKER FINANCIAL CONTENT SEGMENTS AND CURRENT-DOWNSTREAM SAFE APPLY REMAIN`

Exact next action: `DO NOT WRITE PRODUCTION - USE THE FINANCIAL MATCH MATRIX TO RESOLVE REMAINING BLOCKED ROWS`

## Phase 8A10E-R3 - Clean Latest-8Q Reconstruction for Nine 52/53-Week Tickers

Classification: `FUNDAMENTALS_V3_PHASE8A10E_R3_RECONSTRUCTION_BLOCKED`

Status: `DONE_READ_ONLY_CLEAN_LATEST8Q_RECONSTRUCTION_BLOCKED`

Artifact root: `temp/fundamentals_v3_phase8a10e_r3_latest8q_reconstruction/20260827T_PHASE8A10E_R3`

Authoritative enriched latest-8Q input: `temp/swingmaster_v3_official_fiscal_quarter_timeline_with_financials_2026-08-26.csv`

R3 abandons row-mapping as a production repair strategy for the nine 52/53-week tickers. R2 proved that the defect is not a generic period_end-only shift: current V3 labels, period_end dates, and some row content are displaced relative to the issuer economic quarter. R3 therefore builds clean official latest-8Q target segments first and then asks whether current/local source evidence can populate those targets safely.

Official timeline authority:

| Metric | Count |
| --- | ---: |
| Official rows | 72 |
| Tickers | 9 |
| Rows per ticker | 8 |
| HIGH confidence rows | 72 |
| Revenue populated | 72 |
| Operating Income populated | 72 |
| Net Income populated | 72 |

Fiscal-calendar slot inference was used only as structural mapping evidence, never as canonical date truth. FY2027 starts were loaded for all nine tickers. January/February boundary group: `BBY`, `DELL`, `GCO`, `MRVL`, `SAIC`, `TJX`. March/April boundary group: `HAE`, `RL`, `TRNS`. Slot model valid tickers `9/9`; slot ambiguities `0`; official period_end remains authoritative.

Clean reconstruction result:

| Metric | Count |
| --- | ---: |
| Clean target quarters | 72 |
| Reused current rows without repair | 0 |
| Reused with metadata repair | 0 |
| Reused with identity repair | 29 |
| Reconstructed from local source | 0 |
| Reconstructed from multiple sources | 0 |
| Partial targets | 43 |
| Source-insufficient target assignments | 43 |

Field reconstruction availability:

| Field | Verified targets |
| --- | ---: |
| Revenue | 72 |
| Operating Income | 72 |
| Net Income | 72 |
| Gross Profit | 29 |
| EBIT | 22 |
| EBITDA | 29 |
| OCF | 29 |
| Capex | 29 |
| FCF | 29 |
| Cash | 29 |
| Total Debt | 29 |
| Shares Outstanding | 29 |

Ticker readiness:

| Ticker | Current rows reused | First target | Last target | Production ready |
| --- | ---: | --- | --- | --- |
| `BBY` | 1 | 2024-08-03 | 2026-05-02 | NO |
| `DELL` | 2 | 2024-08-02 | 2026-05-01 | NO |
| `GCO` | 3 | 2024-08-03 | 2026-05-02 | NO |
| `HAE` | 3 | 2024-09-28 | 2026-06-27 | NO |
| `MRVL` | 1 | 2024-08-03 | 2026-05-02 | NO |
| `RL` | 1 | 2024-09-28 | 2026-06-27 | NO |
| `SAIC` | 6 | 2024-08-02 | 2026-05-01 | NO |
| `TJX` | 6 | 2024-11-02 | 2026-08-01 | NO |
| `TRNS` | 6 | 2024-09-28 | 2026-06-27 | NO |

Frozen replacement apply set is empty: ready ticker groups `0`, blocked tickers `9`, operations `0`, metadata writes `0`, identity writes `0`, canonical value writes `0`, creates `0`, deletes `0`, lineage actions `0`.

Rehearsal applied `0` operations on a disposable V3 copy. Integrity stayed clean: quick_check `ok`, duplicate FY/FQ `0`, orphan rows `0`, unrelated drift `0`. Fiscal-slot parity passed because the target model is coherent. Official timeline parity, financial parity, and latest-8Q window parity remain failing because no ticker group was safe to replace.

Exact A10B re-audit after rehearsal stayed unchanged: global P1 `15 -> 15`, nine-ticker P1 `9 -> 9`, new P1 `0`. Remaining P1 tickers are `BBY`, `DELL`, `FNGR`, `GCO`, `HAE`, `MRVL`, `POWW`, `RH`, `RL`, `SAIC`, `TJX`, `TRNS`, and `VTGN`.

Prevention findings: metadata-only failure count `0`, FY/Q displacement count `29`, hybrid row count `0`, missing-quarter count `43`. Future prevention must build clean official targets before writes and require financial fingerprint, fiscal-slot, target-collision, latest-window, and post-A10B gates.

Safety proof: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle writes `0`, Valuation writes `0`. RawCandle external drift was observed during the read-only run, but R3 did not read or write RawCandle.

Phase 8 remains: `IN PROGRESS - NINE-TICKER CLEAN RECONSTRUCTION BLOCKED AND CURRENT-DOWNSTREAM SAFE APPLY REMAIN`

Exact next action: `DO NOT WRITE PRODUCTION - CONSIDER REMOVING ONLY THE PERSISTENTLY UNRESOLVABLE TICKERS FROM V3`

## Phase 8A10F - Current-Downstream P2/P3 External Evidence Reconciliation

Classification: `FUNDAMENTALS_V3_PHASE8A10F_CURRENT_DOWNSTREAM_PARTIAL_APPLY_SET_READY_BLOCKERS_REMAIN`

Status: `DONE_READ_ONLY_CURRENT_DOWNSTREAM_RECONCILIATION_PARTIAL_APPLY_SET_FROZEN`

Artifact root: `temp/fundamentals_v3_phase8a10f_current_downstream_reconcile/20260826T181000Z`

External current-downstream package validated cleanly: case rows `35`, unique request IDs `35`, unique tickers `30`, externally ready `21`, externally blocked `14`, confidence `HIGH=34 / MEDIUM=1`, official timeline rows `96`, transformation operations `52`.

Current V3 reconciliation found exact current matches `26`, harmless/already-resolved drift `2`, material drift `7`, and row-not-found `0`. The key ready-case blocker was `LYTS` FY2025 Q4: the external file expected blank current publish_date, but current V3 already has `2025-09-11`, so its old-value guard does not pass.

Global-P1 overlap was isolated: same-ticker overlap `2` (`POWW`, `VTGN`), same canonical FY/FQ overlap `0`, duplicate repair operations `0`, P1-dependent cases `2`, P1 conflicts `0`. Those overlapping cases were excluded from the current-downstream apply set.

External-ready validation: locally ready operations `19`, already-correct operations `2`, blocked-by-current-state operations `1`, blocked-by-global-P1 operations `0`. The frozen write set contains only locally guarded metadata operations: publish_date writes `17`, period_end writes `2`, identity writes `0`, canonical value writes `0`, creates `0`, merges `0`, deletes `0`. Frozen production-ready groups `18`, repair operations `19`.

Blocked set after local reconciliation contains `15` cases: the original `14` externally blocked cases plus the `LYTS` current-state guard drift. Blocker classes: restatement/value reconciliation `5`, FY/Q structural collision/remap `7`, missing target quarter `1`, global-P1 dependent `2`, remaining external evidence required `0`.

Restatement/value matrix covered `5` tickers (`AIHS`, `ARAY`, `BNC`, `ILLR`, `RR`), `7` quarters, and `84` canonical field checks. Exact verified value repairs `0`; fields still not verifiable `84`. No fundamental value write was frozen.

Rehearsal applied the `19` locally ready metadata operations on a disposable DB copy. Groups attempted `18`, groups passed `18`, groups failed `0`. Integrity stayed clean: quick_check `ok`, duplicate FY/FQ `0`, orphan rows `0`, unrelated drift `0`.

Current-downstream post-rehearsal accounting for the original 35 cases: resolved by rehearsed safe repair `18`, still blocked `15`, already-correct/no-write `2`, new current-critical cases introduced `0`. Remaining current-impact blockers: TTM `15`, Score `2`, Lifecycle `2`, Valuation `2`.

Safety proof: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle writes `0`, Valuation writes `0`. Production baseline stayed unchanged at companies `2538`, canonical/fundamentals rows `72713`, and TTM/Score/Lifecycle/Valuation rows `53788`.

Phase 8 remains: `IN PROGRESS - CURRENT-DOWNSTREAM SAFE APPLY AND P1/STRUCTURAL/VALUE BLOCKERS REMAIN`

Downstream remains: `DERIVED_DATA_PENDING_REBUILD_AFTER_CANONICAL_REPAIR`

Exact next action: `PHASE 8A10F-APPLY - APPLY REHEARSED CURRENT-DOWNSTREAM REPAIRS`, after checking interaction with the still-running global-P1 workflow.

## Phase 8 Prevention Hardening - Canonical Ingestion / Update Regression Protection

Classification: `FUNDAMENTALS_V3_PHASE8_PREVENTION_HARDENING_POLICY_DOCUMENTED`

Status: `DOCUMENTATION_ONLY_IMPLEMENTATION_PENDING`

Authoritative prevention policy: `docs/fundamentals_v3_canonical_prevention_policy.md`

Phase 8 identified permanent prevention requirements for canonical ingestion, migration, backfill,
repair, and Update write paths. The policy locks the rule that issuer FY/FQ is canonical identity,
official period_end evidence outranks normalized provider period dates, publish_date is the first
authoritative public result date, economic-quarter content must move together, and write paths must
block target collisions, fiscal sequence errors, transition/STUB misencoding, restatement hybrids,
and silent month-end normalization.

The historical Yahoo seed failure mode is recorded as likely originating in
`swingmaster/fundamentals/v3_yahoo_canonical_seed.py::prepare_yahoo_seed`, where normalized Yahoo
`period_end_date` was used for canonical candidates while `official_period_end_date` remained
metadata. This remains classified as `HISTORICAL_MIGRATION_ARTIFACT`, not a proven active Update V3
bug, but future reuse of the seed/bootstrap/migration/backfill/recovery path must prove the failure
cannot recur.

Future implementation phase:

```text
PHASE 8 PREVENTION HARDENING - IMPLEMENT CANONICAL WRITE-PATH GUARDS & REGRESSION TESTS
```

Completion target: `FUNDAMENTALS_V3_PHASE8_PREVENTION_HARDENING_COMPLETE`

Safety: production writes `0`, RawCandle writes `0`, TTM writes `0`, Score writes `0`, Lifecycle
writes `0`, Valuation writes `0`.

## Phase 8B - Temporary downstream rebuild with known deferred canonical defects

Status: `DONE_DOWNSTREAM_REBUILD_WITH_DEFERRED_CANONICAL_DEFECTS`

Classification: `FUNDAMENTALS_V3_PHASE8B_DOWNSTREAM_REBUILD_COMPLETE_WITH_KNOWN_CANONICAL_DEFECTS`

Artifact root: `temp/fundamentals_v3_phase8b_downstream_rebuild/20260827T_PHASE8B`

The user explicitly changed the temporary operational order: canonical repairs are deferred, the current V3 canonical state is frozen as an operational baseline, and downstream TTM -> Score -> Lifecycle -> Valuation is rebuilt now from current production canonical data. This is not canonical closure and not Phase 8 completion.

Known unresolved global P1 remains `15 rows / 13 tickers`: `BBY, DELL, FNGR, GCO, HAE, MRVL, POWW, RH, RL, SAIC, TJX, TRNS, VTGN`. The nine 52/53-week tickers remain in V3 and are not repaired in this phase. A10F safe repairs remain frozen but unapplied.

Canonical fingerprint before and after downstream rebuild matched: `True`.

Downstream rows after rebuild: TTM `53781`, Score `53781`, Lifecycle `53781`, Valuation `53781`.

Safety: canonical writes `0`, RawCandle writes `0`; downstream writes were authorized and executed.

Phase 8 remains: `IN PROGRESS - DEFERRED CANONICAL REPAIR AND PREVENTION HARDENING REQUIRED BEFORE FINAL CUTOVER`

## Phase 8C - Fiscal Calendar Metadata Layer

Status: `FUNDAMENTALS_V3_PHASE8C_FISCAL_CALENDAR_METADATA_COMPLETE_WITH_REVIEW_ITEMS`

Artifact root: `temp/fundamentals_v3_phase8c_fiscal_calendar_metadata/20260827T_PHASE8C`

Fiscal-calendar profiles and exact FY anchors were imported as metadata-only production data. Canonical and downstream fingerprints remained unchanged. Phase 8 remains `IN PROGRESS`.

## Phase 8D - Fiscal Calendar Prevention Guards

Status: `FUNDAMENTALS_V3_PHASE8D_FISCAL_WRITE_GUARDS_ACTIVE`

Fiscal-calendar guard is active in `V3QuarterRepository.upsert_quarter` before canonical quarter mutation. Exact FY2026/FY2027 anchors are authoritative, backward inference assumes stable fiscal calendar unless positive transition evidence exists, and `REVIEW`/`BLOCK` candidates perform zero canonical writes.

Phase 8 remains `IN PROGRESS`.

## Phase 8D-1 - Full-Database Fiscal Guard Audit

Status: `FUNDAMENTALS_V3_PHASE8D1_FULL_FISCAL_AUDIT_COMPLETE`

Artifact root: `temp/fundamentals_v3_phase8d1_full_fiscal_audit/20260827T_PHASE8D1_FULL`

The Phase 8D dry-run sample was replaced with a complete read-only replay of every current V3 canonical quarter row. Rows audited: `72713`. Outcome distribution: PASS `57762`, PASS_WITH_WARNING `2195`, REVIEW `152`, BLOCK `12604`.

The BLOCK population is split by evidence strength rather than treated as one class. Exact-anchor-proven conflicts: `833`. Backward-inference blocks: `11291`. Block confidence distribution: PROVEN_HIGH `1054`, STRUCTURAL_HIGH `3644`, STRUCTURAL_MEDIUM `2602`, INFERENCE_RISK `5304`.

Inference-distance behavior: exact/current-anchor interval rows `4601` with BLOCK `848` (`18.4308%`); 1-year rows `9782` with BLOCK `1304` (`13.3306%`); 2-year rows `7661` with BLOCK `2930` (`38.2457%`); 3-year rows `8924` with BLOCK `951` (`10.6567%`); 4-5-year rows `18052` with BLOCK `2256` (`12.4972%`); 6-10-year rows `22049` with BLOCK `4313` (`19.5610%`). Unknown metadata rows `1642` produced PASS_WITH_WARNING only.

Calendar-type behavior: CALENDAR_YEAR `53751` rows with BLOCK `4151` (`7.7226%`); FIXED_DATE_FISCAL_YEAR `9626` rows with BLOCK `6101` (`63.3804%`); WEEK_BASED_52_53 `5173` rows with BLOCK `1150` (`22.2308%`); OTHER_VERIFIED `2521` rows with BLOCK `1202` (`47.6795%`); UNKNOWN `1642` rows with BLOCK `0`.

Current/recent behavior: 2024+ rows PASS `16907`, PASS_WITH_WARNING `569`, REVIEW `24`, BLOCK `5084`; latest 8Q rows PASS `15040`, PASS_WITH_WARNING `531`, REVIEW `23`, BLOCK `4638`; latest 4Q rows PASS `8396`, PASS_WITH_WARNING `261`, REVIEW `13`, BLOCK `1462`; current TTM input rows PASS `56124`, PASS_WITH_WARNING `2154`, REVIEW `127`, BLOCK `11642`; Score/Lifecycle/Valuation source rows PASS `43015`, PASS_WITH_WARNING `1734`, REVIEW `74`, BLOCK `8958`.

Known P1 replay across the existing canonical rows remained protected: PASS `46`, REVIEW `8`, BLOCK `267`. The audit recommends `KEEP_PHASE8D_GUARD_UNCHANGED`; no calibration was applied because the full-database audit did not establish a bounded false-positive category that could be weakened without reducing exact-anchor and known-defect protection. Long-distance backward-inference cases remain separately identified for future repair/review.

Safety: production writes `0`, RawCandle writes `0`, canonical/fundamentals/lineage/fiscal-anchor/TTM/Score/Lifecycle/Valuation fingerprints unchanged, integrity `quick_check=ok`, FK issues `0`, duplicate canonical FY/FQ `0`, orphan rows `0`.

Phase 8 remains `IN PROGRESS`.

## Phase 8D-2 - Current / Recent Operational Risk Assessment

Status: `FUNDAMENTALS_V3_PHASE8D2_OPERATIONAL_RISK_ASSESSED`

Artifact root: `temp/fundamentals_v3_phase8d2_operational_risk/20260827T_PHASE8D2`

The assessment reused the Phase 8D-1 full-audit artifacts and did not rerun the fiscal guard. Historical context remains BLOCK `12604` of `72713` rows, split into exact-anchor-proven `833` and backward-inference `11291`.

Current/recent exposure: 2024+ BLOCK `5084` of `22584`; 2025+ BLOCK `2154` of `14686`; latest8Q BLOCK `4638` of `20232`; latest4Q BLOCK `1462` of `10132`; latest-quarter BLOCK `301` of `2470`.

Current TTM risk distribution: TTM_CLEAN `2056`, TTM_WARNING_ONLY `69`, TTM_BACKWARD_INFERENCE_RISK `17`, TTM_EXACT_ANCHOR_CONFLICT `0`, TTM_MULTIPLE_STRUCTURAL_CONFLICTS `386`.

Current downstream exposure is broad: Score/Lifecycle/Valuation each have `492` current rows with known blocked TTM input risk, of which `479` are outside the original known-13 register. New current-risk tickers outside the known register: `511`; new P1 current-risk tickers: `236`.

Operational recommendation: `CURRENT_V3_CURRENT_DOWNSTREAM_RISK_TOO_BROAD`. Phase 8 remains `IN PROGRESS`; do not proceed to Phase 8 operational closure before prioritizing current-latest / TTM-affecting fiscal identity repairs. Keep the Phase 8D guard active.

## Phase 8D-3 - Empirical Fiscal Quarter-End / Publish-Date Calibration

Status: `FUNDAMENTALS_V3_PHASE8D3_QUARTER_SLOT_CALIBRATION_COMPLETE`

Artifact root: `temp/fundamentals_v3_phase8d3_quarter_slot_calibration/20260828T_PHASE8D3`

Calibration population: active tickers `2470`, recent rows considered `14817`, KNOWN_GOOD_HIGH `11107`, KNOWN_GOOD_MEDIUM `3632`, excluded structural risk `78`.

Period-end offsets for KNOWN_GOOD_HIGH: median signed `0`, median absolute `1`, abs P90 `3.0`, abs P95 `183.0`, abs P99 `366.0`. Window coverage: ±7 `94.2199%`, ±14 `94.2379%`.

Publish chronology: rows with publish_date `11107`, publish after period_end `99.982%`, strict next-quarter chronology `74.3135%`, +7 tolerance `74.3225%`, +14 tolerance `74.3315%`.

Current guard on KNOWN_GOOD_HIGH: PASS `9452`, PASS_WITH_WARNING `25`, REVIEW `9`, BLOCK `1621` (`14.5944%`).

Recommendation: `REWORK_FISCAL_SLOT_MODEL`. Guard behavior was not changed; production writes were `0`.

## Phase 8D-4 - Calendar-Type-Specific Fiscal Slot Model Rework

Status: `NEW_FISCAL_SLOT_MODEL_NEEDS_REFINEMENT`

Artifact root: `temp/fundamentals_v3_phase8d4_slot_model_rework/20260828T_PHASE8D4`

The generic 13/14-week slot model failed because fixed-date fiscal years were modeled as day-count slots and 53-week years assumed false precision. The candidate resolver uses calendar quarters for CALENDAR_YEAR, calendar-month addition for FIXED_DATE_FISCAL_YEAR, local-evidence week placement for WEEK_BASED_52_53, and conservative exact-anchor behavior for OTHER_VERIFIED.

Known-good population reuse: old `11107`, new `11107`, exact same population `True`. Period-end abs P95 old `183.0`, new `182.0`; ±7 coverage old `94.2199`, new `94.0873`.

Known-good guard simulation: old BLOCK `1621` (`14.5944%`), new BLOCK `562` (`5.0599%`), new REVIEW `204`.

Known P1 replay: tickers `13`, remain BLOCK `69`, become REVIEW `3`, incorrectly PASS `6`; high-confidence structural P1 silent PASS `0`.

The candidate model is not active in production writes. Production writes `0`; production guard activation changes `0`.

## Phase 8D-5 - Fiscal-Year Interval Assignment Refinement

Status: `FISCAL_YEAR_INTERVAL_HYPOTHESIS_REJECTED`

Artifact root: `temp/fundamentals_v3_phase8d5_fiscal_year_interval_refinement/20260828T_PHASE8D5`

Phase 8D-5 decomposed the Phase 8D-4 known-good residual BLOCK population and separated fiscal-year interval assignment from fiscal-quarter slot assignment. The candidate resolver now resolves issuer FY first from exact adjacent anchors or stable backward intervals, then assigns Q1-Q4 inside that FY interval. FY2026/FY2027 anchors remain authoritative and the refined resolver is not active in production.

Known-good population reuse: old `11107`, new `11107`, exact same population `True`. Phase 8D-4 residual BLOCK rows `562`.

Identity agreement: FY `92.9054% -> 90.9336%`, FQ `95.9755% -> 91.2218%`, combined `91.915% -> 87.5394%`.

Period-end tails: median abs `0 -> 0.0`, P90 `2.0 -> 0.0`, P95 `182.0 -> 3.0`, P99 `365.0 -> 274.0`.

Known-good guard simulation: BLOCK `562` (`5.0599%`) -> `656` (`5.9062%`), REVIEW `265`, WARNING `0`.

Residual decomposition: CALENDAR_YEAR `16`, FIXED_DATE_FISCAL_YEAR `426`, WEEK_BASED_52_53 `79`, OTHER_VERIFIED `41`. Root causes: KNOWN_GOOD_LABEL_NOT_STRUCTURALLY_SUPPORTED `513`, FY_INTERVAL_ASSIGNMENT_ERROR `43`, ANCHOR_PROPAGATION_ERROR `6`.

Offset modes in the 562 Phase 8D-4 residuals: ~90-day `10`, ~180-day `35`, ~270-day `24`, ~365-day `427`, ~371-day `24`, other `42`. The dominant mode is not normal quarter-end drift; it is one-fiscal-year label displacement relative to authoritative FY intervals.

Publish chronology improved on the strict next-quarter upper bound: `72.6389% -> 74.6286%`. Current/recent simulated BLOCK counts worsened versus Phase 8D-4 candidate baseline: 2024+ `1102 -> 1200`, latest8Q `1037 -> 1135`, latest4Q `520 -> 605`, latest-quarter `90 -> 98`, current-TTM affected `439 -> 536`.

Known P1 replay: tickers `13`, BLOCK `69`, REVIEW `3`, PASS `6`, high-confidence structural P1 PASS `0`.

Safety: production writes `0`; active guard changed `NO`; fingerprints unchanged `YES`.

## Phase 8D-6 - Recent FY/FQ Label Provenance Audit

Status: `RECENT_FY_FQ_LABEL_DERIVATION_BUG_CONFIRMED`

Artifact root: `temp/fundamentals_v3_phase8d6_label_provenance_audit/20260828T_PHASE8D6`

Phase 8D-6 audited FY/FQ label provenance for the `513` Phase 8D-5 rows where the economic quarter was high-confidence but the stored label was not structurally supported by authoritative FY intervals. Yahoo raw normalization provides period_end/value rows, not issuer FY/FQ; V3 labels enter through metadata enrichment and Phase 3B seed metadata artifacts, then canonical migration copies candidate FY/FQ.

Residual split: FIXED_DATE_FISCAL_YEAR `425`, WEEK_BASED_52_53 `31`, CALENDAR_YEAR `16`, OTHER `41`. Label errors: FY_LABEL_MINUS_ONE `405`, FY_LABEL_PLUS_ONE `0`, FY_AND_FQ_WRONG `67`, structurally correct `0`, unresolved `41`.

The ~365/~371-day cohort had `405` rows; systematic FY-minus-one cases `405`; stored FY equals fiscal-start calendar year `405` and period_end calendar year `308`. This confirms a start-year/period-year style label convention problem rather than bad Yahoo financial values for most rows.

Content integrity classification: LABEL_ONLY_ERROR_HIGH_CONFIDENCE `342`, LABEL_PLUS_METADATA_ERROR `130`, CONTENT_MAPPING_ERROR `0`, unresolved `41`.

Current repairability, read-only: AUTO_LABEL_REPAIR_READY rows `0`, collision review rows `3595`, content reconstruction rows `0`, unresolved rows `5565`.

The active guard remains unchanged and continues to catch these candidates. Production writes `0`; active guard changes `0`; fingerprints unchanged `True`.

## Phase 8C-EXT - Historical Exact FY Anchor Backfill

Status: `FUNDAMENTALS_V3_PHASE8C_EXT_HISTORICAL_ANCHOR_BACKFILL_COMPLETE_WITH_REVIEW_ITEMS`

Artifact root: `temp/fundamentals_v3_phase8c_ext_historical_anchors/20260828T_PHASE8C_EXT`

Imported historical verified exact fiscal-year starts from FY1999-FY2027 into the V3 fiscal-calendar metadata layer. Normalized populated source cells `35399`, new exact anchor inserts `32670`, already-existing exact anchors `2729`, total anchors after `35399`.

Current anchor reconciliation passed: FY2026 exact matches `2470`, FY2027 exact matches `259`, conflicts `0`. Chain/break metadata rows inserted `2470`.

Safety: canonical/fundamentals/TTM/Score/Lifecycle/Valuation fingerprints unchanged; active guard changes `0`; RawCandle writes `0`. Phase 8 remains `IN PROGRESS`.

## Phase 8D-7 - Historical Exact Anchor Fiscal-Identity Reanalysis

Status: `HISTORICAL_EXACT_ANCHORS_MATERIALLY_RESOLVE_FISCAL_IDENTITY_RISK`

Artifact root: `temp/fundamentals_v3_phase8d7_historical_anchor_reanalysis/20260828T_PHASE8D7`

Historical anchors now provide `32929` adjacent exact FY intervals and directly resolve `66127` canonical rows. Rows still using short inference `3883`, long inference `0`, transition/unresolved `2967`.

D6 FY-minus-one replay: original `405`, direct-exact confirmed `351`, short-inference confirmed `0`, transition review `0`, not confirmed `54`.

Full canonical reclassification: direct FY conflicts `3425`, direct FQ conflicts `785`, transition reviews `264`, unresolved `2703`, clean `65536`. Old backward-inference BLOCK rows now covered by direct intervals `10948` / `11291`.

Current repairability: AUTO_RELABEL_READY `701` rows / `192` tickers; ATOMIC_SEGMENT_RELABEL_READY `643` rows / `152` tickers. Phase 8 remains `IN PROGRESS`; production writes `0`; guard changes `0`.

## Phase 8E - Deterministic Fiscal Identity Repair Rehearsal

Status: `FUNDAMENTALS_V3_PHASE8E_PARTIAL_APPLY_SET_READY_BLOCKERS_REMAIN`

Artifact root: `temp/fundamentals_v3_phase8e_rehearse_fiscal_repairs/20260828T_PHASE8E`

Phase 8E rehearsed only the Phase 8D-7 deterministic identity subset: AUTO_RELABEL_READY `701` rows / `192` tickers and ATOMIC_SEGMENT_RELABEL_READY `643` rows / `152` tickers. Frozen safe set rows `494`, groups `168`, tickers `148`; blocked groups `207`.

Rehearsal used temporary identity rekeys on a disposable DB copy, preserved quarter_id, content signatures and lineage signatures, and left production unchanged. Rehearsal quick_check `ok`, duplicate FY/FQ `0`, content signature drift `0`, lineage failures `0`.

Full fiscal risk direct FY conflicts `3425 -> 2988`, direct FQ conflicts `785 -> 728`, clean rows `65536 -> 66030`. Current TTM affected tickers `405 -> 338`.

Disposable downstream rebuild was not completed in this bounded rehearsal. Downstream blocker remains `1`; the apply phase must rebuild TTM -> Score -> Lifecycle -> Valuation once after production identity repair.

Production writes `0`; fiscal metadata writes `0`; RawCandle writes `0`; guard changes `0`. Phase 8 remains `IN PROGRESS`.

## Phase 8E-PREAPPLY - Full Disposable Downstream Proving

Status: `FUNDAMENTALS_V3_PHASE8E_PREAPPLY_FULLY_PROVEN`

Artifact root: `temp/fundamentals_v3_phase8e_preapply_downstream_proving/20260829T_PHASE8E_PREAPPLY`

Frozen input was the Phase 8E apply set: `494` rows / `168` groups / `148` tickers. Stale-precondition groups `0`.

Disposable repair passed canonical integrity with quick_check `ok`, FK rows `0`, duplicate FY/FQ `0`, content drift `0`, lineage failures `0`.

Disposable downstream rebuild completed for TTM, Score, Lifecycle, and Valuation. Determinism: TTM `True`, Score `True`, Lifecycle `True`, Valuation `True`. Unrelated downstream drift `0`.

Fiscal risk direct FY conflicts `3425 -> 2988`, direct FQ conflicts `785 -> 728`, clean rows `65536 -> 66030`. Current TTM affected tickers `405 -> 323`.

Final production-ready set: `494` rows / `168` groups / `148` tickers. Production go/no-go: `GO_FOR_PHASE8E_PRODUCTION_APPLY`.

Production writes `0`; fiscal metadata writes `0`; RawCandle writes `0`; production fingerprints identical `True`. Phase 8 remains `IN PROGRESS`.

## Phase 8E-APPLY - Production Fiscal Identity Repair & Downstream Rebuild

Status: `FUNDAMENTALS_V3_PHASE8E_APPLY_COMPLETE_WITH_REMAINING_DEFERRED_DEFECTS`

Artifact root: `temp/fundamentals_v3_phase8e_apply/20260829T_PHASE8E_APPLY`

Applied exactly the PREAPPLY-proven fiscal identity set: `494` rows / `168` groups / `148` tickers. Failed groups `0`; original blocked rows touched `0`.

Canonical integrity passed: quick_check `ok`, FK rows `0`, duplicate FY/FQ `0`, content drift `0`, lineage failures `0`.

Production downstream was rebuilt once and proved deterministic. TTM rows `53781 -> 53490`, Score rows `53490`, Lifecycle rows `53490`, Valuation rows `53490`. Unrelated downstream drift `0`; PREAPPLY equivalent `True`.

Fiscal risk direct FY conflicts `3425 -> 2988`, direct FQ conflicts `785 -> 728`, clean rows `65536 -> 66030`.

Operational quality conclusion: `V3_CURRENT_DATA_QUALITY_GOOD_WITH_KNOWN_GAPS`. Phase 8 remains `IN PROGRESS`.

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
