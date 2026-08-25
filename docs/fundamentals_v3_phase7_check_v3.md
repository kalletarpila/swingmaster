# Fundamentals V3 Phase 7 - Check V3

Date: 2026-08-25

Classification: `FUNDAMENTALS_V3_PHASE7_CHECK_COMPLETE_PHASE8_REPAIR_REQUIRED`

Next phase: `MASTER PLAN PHASE 8 - UPDATE V3`

Artifact root: `temp/fundamentals_v3_phase7_check_v3/20260825T_PHASE7_CHECK_V3`

## Scope

Phase 7 performed a read-only production audit of the finalized V3 stack before UPDATE V3 cutover. The audit covered production baseline counts, schema, universe, canonical quarters, field coverage, TTM math/PIT checks, score, lifecycle, valuation, cross-layer lineage, applicability outliers, and manual edge-case samples.

No production repair was performed.

## Read-Only Verification

Read-only status: `PASS`

Production V3 and RawCandle file metadata were unchanged during the audit:

- `rc_fundamentals_v3.db`: unchanged
- `rc_fundamentals_v3.db-wal`: absent before and after
- `rc_fundamentals_v3.db-shm`: absent before and after
- `/home/kalle/projects/rawcandle/data/osakedata.db`: unchanged

Expected production writes:

- company/universe: `0`
- canonical quarter: `0`
- source variants: `0`
- TTM: `0`
- valuation: `0`
- score: `0`
- lifecycle: `0`
- RawCandle: `0`

## Production Baseline

All locked Phase 6 baseline counts matched:

| Metric | Expected | Actual |
| --- | ---: | ---: |
| companies | 2,550 | 2,550 |
| active companies | 2,482 | 2,482 |
| inactive companies | 68 | 68 |
| canonical quarters | 73,075 | 73,075 |
| quarter fundamentals | 73,075 | 73,075 |
| TTM rows | 54,038 | 54,038 |
| valuation rows | 54,038 | 54,038 |
| score rows | 54,038 | 54,038 |
| lifecycle rows | 54,038 | 54,038 |

SQLite `PRAGMA quick_check`: `ok`

Known zero-quarter residuals remain bounded to the expected set: `ALTS`, `HOTH`, `PKST`, `QVCGA`, `STSS`.

## Engine Integrity

TTM parity checks passed:

- four-quarter flow recomputation failures: `0`
- endpoint instant-field parity failures: `0`
- PIT date failures: `0`

Score checks passed:

- model version: `V3_LEGACY2_FUNDAMENTAL_SCORE_V1`
- fingerprint: `8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0`
- score bounds failures: `0`
- score lineage failures: `0`
- score remains valuation-independent: `PASS`

Lifecycle checks passed:

- model version: `V3_LIFECYCLE_EBIT_FIRST_V1`
- fingerprint: `18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e`
- state-domain failures: `0`
- chronological previous-state failures: `0`

Valuation checks passed:

- formula parity failures: `0`
- RawCandle close-price parity failures: `0`
- publish+1 failures with revalidatable RawCandle rows: `0`

Valuation status distribution:

| Status | Rows |
| --- | ---: |
| `VALID` | 43,754 |
| `NOT_MEANINGFUL` | 5,073 |
| `MISSING_PUBLISH_DATE` | 3,304 |
| `MISSING_INPUT` | 1,787 |
| `MISSING_TARGET_DAY_PRICE` | 120 |

Cross-layer endpoint coverage is complete:

- TTM rows: `54,038`
- score rows: `54,038`
- lifecycle rows: `54,038`
- valuation rows: `54,038`

## Material Findings

Phase 7 found no production-integrity blocker and no unexpected writes.

It did find two Phase 8-relevant issue classes:

| Severity | Issue | Rows | Meaning |
| --- | --- | ---: | --- |
| HIGH | `PUBLISH_DATE_ANOMALY` | 111 | canonical rows where publish date is before period end, future-dated relative to 2026-08-25, or market availability is earlier than publish date |
| MEDIUM | `SEMANTIC_FIELD_OUTLIER` | 237 | negative revenue/cash/debt or non-positive shares rows requiring provider/value review |

The issue register is deterministic and stored at `issue_register.csv`; Phase 8 handoff rows are stored at `phase8_handoff_issues.csv`.

## Artifact Inventory

Key artifacts:

- `production_snapshot.json`
- `read_only_file_snapshot.json`
- `schema_audit.csv`
- `row_counts.csv`
- `production_baseline_counts.csv`
- `company_identity_audit.csv`
- `zero_quarter_companies.csv`
- `active_company_staleness.csv`
- `canonical_duplicate_fy_fq.csv`
- `canonical_invalid_quarters.csv`
- `canonical_publish_date_anomalies.csv`
- `canonical_sequence_gap_outliers.csv`
- `field_coverage_by_year.csv`
- `field_semantic_outliers.csv`
- `ttm_flow_parity_failures.csv`
- `ttm_instant_parity_failures.csv`
- `ttm_pit_failures.csv`
- `score_model_fingerprint_audit.csv`
- `score_distribution.csv`
- `score_market_independence_audit.md`
- `lifecycle_model_fingerprint_audit.csv`
- `lifecycle_state_distribution.csv`
- `lifecycle_transition_matrix.csv`
- `valuation_status_audit.csv`
- `valuation_publish_plus_one_failures.csv`
- `valuation_price_parity_failures.csv`
- `valuation_formula_failures.csv`
- `cross_layer_endpoint_coverage.csv`
- `cross_layer_metric_parity_failures.csv`
- `financial_applicability_name_outliers.csv`
- `reit_applicability_review.csv`
- `manual_edge_case_samples.csv`
- `issue_register.csv`
- `phase8_handoff_issues.csv`
- `phase7_summary.json`
- `recommended_next_step.md`

## Validation

Commands:

```bash
PYTHONPATH=. .venv/bin/python -m pytest swingmaster/tests/test_fundamentals_v3_phase7_check_v3.py -q
PYTHONPATH=. .venv/bin/python -m swingmaster.cli.run_fundamentals_v3_phase7_check_v3 --v3-db rc_fundamentals_v3.db --rawcandle-db /home/kalle/projects/rawcandle/data/osakedata.db --artifact-root temp/fundamentals_v3_phase7_check_v3/20260825T_PHASE7_CHECK_V3
```

Results:

- focused Phase 7 tests: `8 passed`
- production audit classification: `FUNDAMENTALS_V3_PHASE7_CHECK_COMPLETE_PHASE8_REPAIR_REQUIRED`

## Handoff

Phase 8 should repair or explicitly accept the 111 canonical publish-date anomalies and review the 237 semantic field outliers before enabling UPDATE V3 cutover.

Exact next phase: `MASTER PLAN PHASE 8 - UPDATE V3`

