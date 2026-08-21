# Fundamentals V3 Phase 2D Post-A Metadata Rejection Recovery Audit

Date: 2026-08-21

Classification: `FUNDAMENTALS_V3_METADATA_REJECTIONS_RECOVERY_PATH_LOCKED`

## Scope

This audit classifies all Yahoo normalized quarterly rows from bootstrap run
`V3_YAHOO_FULL_BOOTSTRAP_20260821T140717Z` that were emitted as
`METADATA_NOT_RESOLVED`.

The audit is read-only. It uses the completed Yahoo raw cache and local Legacy/V2/result-event
metadata. It performs no Yahoo, SEC, SimFin, or network calls and writes no Legacy, V2, canonical
V3, Check/Update, RawCandle, or raw-cache production data.

Artifact root:

```text
temp/fundamentals_v3_phase2d_post_metadata_recovery/20260821T_POST_A_METADATA_RECOVERY/
```

## Row Accounting

The rejected population reconciles exactly:

| Metric | Rows |
| --- | ---: |
| Yahoo normalized rows | 14,373 |
| Current rollout migration candidate records | 10,722 |
| Metadata rejected rows audited | 3,651 |
| Candidate records + metadata rejected rows | 14,373 |

The known 8 candidate-key collisions are kept separate from this 3,651-row rejection analysis.

## Component Missingness

The original coarse `METADATA_NOT_RESOLVED` state decomposes as:

| Component state | Rows |
| --- | ---: |
| `FY_FQ_MISSING_PUBLISH_PRESENT` | 1,989 |
| `ALL_REQUIRED_METADATA_MISSING` | 1,595 |
| `ALL_REQUIRED_METADATA_PRESENT_BUT_REJECTED` | 59 |
| `ONLY_PUBLISH_DATE_MISSING` | 8 |

Initial component counts:

| Missing component | Rows |
| --- | ---: |
| Fiscal year missing | 3,584 |
| Fiscal quarter missing | 3,584 |
| Publish date missing | 1,603 |

The 59 rows with all components present reflect evidence found by this expanded audit across
independent Legacy/provider-observation and result-event lookups. They were not candidates in Phase
2C because the current adapter requires its narrower exact enrichment path to resolve identity and
publication in one pass.

## Recovery Methods

V2 exact report-date recovery found no exact `rc_v2_quarter.report_date` matches for the 3,651
rejected rows.

| Method | Result |
| --- | ---: |
| V2 exact FY/FQ recoverable | 0 |
| V2 safe period-date variant recoverable | 0 |
| Legacy explicit FY/FQ recoverable | 67 |
| Validated company fiscal-calendar derived | 0 |
| Validated sequential derived | 1,914 |
| Anchor conflicts | 56 |
| Fiscal identity still unresolved | 1,670 |

The sequential recovery candidates use trusted fiscal anchors from already accepted bootstrap
candidates or direct explicit identity evidence. A row is marked `VALIDATED_SEQUENTIAL_DERIVED` only
when all usable anchors for that company produce the same FY/FQ assignment and the normalized Yahoo
period sequence is compatible with quarterly reporting. Rows with disagreeing anchors are classified
as `ANCHOR_CONFLICT`.

The V2 safe-variant audit is deliberately conservative. No row was counted as a safe variant because
the local durable evidence did not expose an already classified V2 multi-report-date recovery table
for these rows, and this phase does not introduce a broad arbitrary day-window matcher.

## Publication Recovery

Publication date recovery is independent from fiscal identity recovery.

| Publication result | Rows |
| --- | ---: |
| Publish date recovered from V2 | 0 |
| Publish date recovered from Legacy/result event | 2,048 |
| Publish date still unresolved | 1,603 |
| Canonical identity ready but publish unresolved | 886 |

No publication date is invented from Yahoo fetch time, cache time, ingestion time, SEC filing time,
or period-end-plus-offset rules.

## Value Quality

The rejected rows generally contain useful Yahoo fundamentals:

| Field | Present | Missing | Present % |
| --- | ---: | ---: | ---: |
| revenue | 3,477 | 174 | 95.23 |
| gross_profit | 2,751 | 900 | 75.35 |
| operating_income | 3,197 | 454 | 87.57 |
| ebit | 3,242 | 409 | 88.80 |
| ebitda | 3,190 | 461 | 87.37 |
| net_income | 3,477 | 174 | 95.23 |
| operating_cashflow | 3,474 | 177 | 95.15 |
| capex | 3,111 | 540 | 85.21 |
| free_cashflow | 3,472 | 179 | 95.10 |
| cash | 3,478 | 173 | 95.26 |
| total_debt | 3,375 | 276 | 92.44 |
| shares_outstanding | 3,651 | 0 | 100.00 |

Core value diagnostic, ignoring metadata:

| Core value state | Rows |
| --- | ---: |
| `CORE_VALUES_PRESENT` | 3,017 |
| Missing exactly one core value | 431 |
| Missing 2+ core values | 203 |

This is only a value-quality diagnostic. It is not canonical `Q_CORE_FIELDS_READY` unless fiscal
identity is also valid.

## Company Distribution

| Metric | Value |
| --- | ---: |
| Affected companies | 1,385 |
| Median rejected rows per company | 2 |
| Max rejected rows per company | 7 |
| Companies with 1 rejected row | 611 |
| Companies with 2-4 rejected rows | 416 |
| Companies with 5+ rejected rows | 358 |

## V2-Covered vs Legacy-Only

| Population | Rejected rows | Immediate identity | Derivable identity | Still unresolved | Publish recovered | Publish unresolved |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| V2-covered | 1,824 | 28 | 1,469 | 327 | 996 | 828 |
| Legacy-only | 1,827 | 39 | 445 | 1,343 | 1,052 | 775 |

Legacy-only companies remain the hardest unresolved population.

## Recovery Tiers

Every row is classified into exactly one tier:

| Tier | Rows |
| --- | ---: |
| Tier 1 - directly recoverable | 59 |
| Tier 2 - safely derivable | 1,036 |
| Tier 3 - identity ready, publication missing | 886 |
| Tier 4 - fundamentals usable, identity unresolved | 1,614 |
| Tier 5 - ambiguous/conflict | 56 |
| Tier 6 - fundamental data insufficient | 0 |

Derived rates:

| Metric | Rows |
| --- | ---: |
| `CANONICAL_IDENTITY_RECOVERABLE` | 1,981 |
| `IMMEDIATELY_FULLY_ENRICHED` | 1,095 |
| `CANONICAL_IDENTITY_READY_BUT_PUBLISH_UNRESOLVED` | 886 |

## Projected Candidate Counts

Using the rollout summary candidate-record count of 10,722:

| Gate | Additional rows | Projected total | Retention of 14,373 normalized rows |
| --- | ---: | ---: | ---: |
| Strict current gate, after safe recovery | 1,095 | 11,817 | 82.22% |
| Identity-ready gate, after safe recovery | 1,981 | 12,703 | 88.38% |

The unique `candidates.jsonl` count remains separately affected by the known 8 candidate-key
collisions.

## Candidate-Key Duplicates

The 8 duplicate candidate-key groups were inspected separately. All 8 groups contain two different
Yahoo `period_end_date` rows mapped to the same V3 fiscal work unit with different values. The
affected tickers are:

```text
BIVI, EXPO, GEF, KLXE, LDOS, NTNX, SURG, WS
```

Cause classification:

```text
period_date_variant_or_metadata_mapping_collision_with_different_values
```

Phase 3 must group by work unit and resolve period-end/value collisions deterministically. Insertion
order must not decide the canonical row.

## Phase 3 Recommendation

`RECOMMENDED_PHASE3_CONTRACT_CHANGE`

Separate canonical quarter identity readiness from market availability readiness:

| State | Required fields |
| --- | --- |
| `Q_CANONICAL_IDENTITY_READY` | company, fiscal_year, fiscal_quarter, accepted period_end_date |
| `Q_MARKET_AVAILABILITY_READY` | reliable publish_date |

Phase 3 should allow rows with valid fiscal identity and accepted period end to proceed into
canonicalization planning even when publish date remains NULL. Market-availability consumers must
remain gated until publish date is reliable.

This recommendation does not rewrite the architecture spec in this phase. It records the evidence
needed for the Phase 3 canonicalization gate.

## Required Artifacts

The audit produced:

- `metadata_rejection_rows.csv`
- `metadata_missingness_matrix.csv`
- `v2_exact_match_recovery.csv`
- `v2_safe_variant_recovery.csv`
- `legacy_identity_recovery.csv`
- `fiscal_sequence_analysis.csv`
- `sequential_recovery_candidates.csv`
- `publication_date_recovery.csv`
- `core_value_coverage.csv`
- `company_rejection_distribution.csv`
- `v2_vs_legacy_only_recovery.csv`
- `recovery_tiers.csv`
- `duplicate_candidate_keys_analysis.csv`
- `projected_candidate_recovery.md`
- `phase3_recommendation.md`
- `summary.json`
- `recommended_next_step.md`
