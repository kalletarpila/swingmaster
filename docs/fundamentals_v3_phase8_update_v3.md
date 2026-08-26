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
