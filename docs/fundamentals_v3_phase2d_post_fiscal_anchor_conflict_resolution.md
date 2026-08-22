# Fundamentals V3 Phase 2D Post-A2 Fiscal Anchor Conflict Resolution

Date: 2026-08-21

Classification: `FUNDAMENTALS_V3_FISCAL_ANCHOR_CONFLICTS_RESOLVED`

## Scope

This phase resolves the 56 fiscal-identity `ANCHOR_CONFLICT` rows identified by the Phase 2D
Post-A metadata rejection audit and tests the same company-wide fiscal pattern method against the
remaining fiscal-identity-unresolved Yahoo rows.

The phase is read-only. It uses completed Yahoo bootstrap artifacts, Post-A recovery artifacts, V2
explicit quarter labels, Legacy/provider-observation labels, and current migration candidate
metadata. It performs no provider or network calls and writes no source, raw-cache, canonical V3,
Check/Update, or RawCandle data.

Artifact root:

```text
temp/fundamentals_v3_phase2d_post_fiscal_anchor_resolution/20260821T_POST_A2_FISCAL_ANCHOR_RESOLUTION/
```

## Why Local Anchors Can Conflict

Post-A propagated fiscal labels from accepted local anchors. That is safe when all anchors agree,
but it fails when one accepted anchor has a wrong fiscal-year or fiscal-quarter offset. In those
cases a nearby previous anchor and a nearby later anchor can derive incompatible labels for the same
Yahoo period.

The correct resolution unit is therefore the full company fiscal timeline, not the local anchor
majority around one row.

## Method

For every affected ticker, the audit builds a chronological fiscal evidence timeline combining:

- Yahoo normalized period-end rows
- current migration candidates
- V2 explicit `rc_v2_quarter` FY/FQ labels
- Legacy/provider explicit FY/FQ observations
- Post-A direct identity anchors
- publication/event metadata as supporting context

The audit then derives a company-wide month-to-fiscal-quarter and fiscal-year-offset pattern only
from repeated historical labels. It does not infer fiscal identity from calendar month alone.

Resolution requires:

1. a stable repeated company pattern for the period month
2. no internally contradictory company-wide pattern
3. coherent Q4 to next-FY Q1 rollover evidence where available
4. explicit identification of local anchors that disagree with the global company pattern

Disagreeing local anchors are recorded as `SUSPECT_FISCAL_ANCHOR`. Source rows are not modified.

## 56-Row Outcome

| Category | Rows |
| --- | ---: |
| `RESOLVED_BY_TRUSTED_Q4_FISCAL_YEAR_END_PATTERN` | 50 |
| `RESOLVED_BY_MULTI_YEAR_FISCAL_PATTERN` | 2 |
| `RESOLVED_BY_SOURCE_PRIORITY_PLUS_SEQUENCE` | 1 |
| `RESOLVED_BY_MANUAL_FISCAL_CALENDAR_EVIDENCE` | 3 |
| `UNRESOLVED_TRUE_ANCHOR_CONFLICT` | 0 |
| `RESOLVED_BIDIRECTIONALLY` | 0 |

Total: 56 rows across 27 companies.

After the initial company-wide evidence pass, 3 rows remained unresolved. User-supplied fiscal
calendar evidence for SJM and LYTS resolved those rows as manual migration evidence:

| Ticker | Period end | Final identity | Evidence |
| --- | --- | --- | --- |
| LYTS | 2025-03-31 | FY2025 Q3 | Fiscal year ends June 30; Q3 is Jan 1-Mar 31. |
| SJM | 2025-10-31 | FY2026 Q2 | Fiscal year ends April 30; Q2 is Aug 1-Oct 31. |
| SJM | 2026-01-31 | FY2026 Q3 | Fiscal year ends April 30; Q3 is Nov 1-Jan 31. |

These manual entries do not mutate V2, Legacy, raw-cache, or canonical V3 source rows.

## Root Causes

| Root cause | Rows |
| --- | ---: |
| `FISCAL_QUARTER_OFFSET_CONFLICT` | 56 |
| `OTHER` | 0 |
| `FISCAL_YEAR_OFFSET_CONFLICT` | 0 |
| `PERIOD_DATE_VARIANT_COLLISION` | 0 |
| `DUPLICATE_SOURCE_MAPPING` | 0 |

The common pattern is a source anchor mapping an adjacent period into the wrong fiscal-quarter
position. This often also changes the fiscal year, but the primary observed defect is quarter
sequence offset rather than same-quarter FY +/- 1.

## Suspect Anchors

| Source bucket | Suspect anchors |
| --- | ---: |
| Existing Yahoo/local metadata | 33 |
| Legacy/provider observation | 22 |
| V2 | 0 |
| Other | 0 |

This is diagnostic evidence only. It does not globally demote any source and does not mutate source
records.

## ADP Worked Example

For `ADP 2025-09-30`, local anchor propagation produced competing labels:

```text
FY2026 Q1
FY2025 Q3
```

The full ADP fiscal history shows a stable June fiscal-year end: June period ends are repeatedly
Q4, and September period ends are the next fiscal year's Q1.

Resolved identity:

```text
ADP 2025-09-30 = FY2026 Q1
```

The conflicting later local anchor path is classified as a suspect fiscal anchor. This is migration
evidence only; no source database row is changed.

## Additional Recovery

The same company-wide fiscal pattern method was applied to the 1,614 remaining rows that Post-A
classified as fundamentals-usable but fiscal-identity unresolved.

| Additional category | Rows |
| --- | ---: |
| `HISTORICAL_FISCAL_PATTERN_DERIVED` | 222 |
| `MULTI_YEAR_PATTERN_DERIVED` | 44 |
| `NOT_RECOVERED_BY_COMPANY_WIDE_PATTERN` | 1,348 |

Additional identity recoveries: 266 rows.

Breakdown:

| Additional recovery type | Rows |
| --- | ---: |
| One-sided anchor + established pattern | 5 |
| Anchorless stable historical fiscal pattern | 222 |
| Multi-year pattern with other anchor context | 44 |

## V2-Covered vs Legacy-Only

| Population | Conflict rows | Conflict rows resolved | Additional rows recovered | Total A2 recovery |
| --- | ---: | ---: | ---: | ---: |
| V2-covered | 56 | 53 | 240 | 293 |
| Legacy-only | 0 | 0 | 26 | 26 |

## Identity Coverage

| Metric | Rows |
| --- | ---: |
| Current normalized Yahoo rows | 14,373 |
| Original strict migration candidate records | 10,722 |
| Prior Post-A identity-ready projection | 12,703 |
| A2 additional identity recovery | 322 |
| New identity-ready total | 13,025 |
| Final identity-ready percentage | 90.62% |
| Still fiscal-identity unresolved after A2 | 1,348 |
| Still unresolved percentage of normalized rows | 9.38% |

Within the original 3,651 metadata-rejected rows:

| Metric | Rows |
| --- | ---: |
| Identity ready after A2 | 2,303 |
| Still identity unresolved after A2 | 1,348 |
| Publish date unresolved | 1,603 |

Publication date remains a separate dimension and is not required for this fiscal-identity
resolution.

## Duplicate Candidate-Key Interaction

Four conflict rows are in tickers that also appear in known duplicate candidate-key diagnostics, but
none directly overlaps the same duplicate candidate period. The duplicate-key problem remains a
separate Phase 3 deterministic work-unit resolution issue.

## Phase 3 Algorithm Recommendation

Phase 3 should implement fiscal identity recovery with this order:

1. Build a company-wide fiscal evidence timeline.
2. Infer company-specific fiscal-quarter pattern only from repeated explicit historical labels.
3. Use repeated Q4 fiscal-year-end placement and Q4 to next-FY Q1 rollover as high-confidence
   evidence.
4. Prefer global sequence coherence over local anchor count.
5. Mark disagreeing anchors as `SUSPECT_FISCAL_ANCHOR` without mutating source rows.
6. Keep true bidirectional or company-wide conflicts unresolved.
7. Keep publication readiness separate from fiscal identity readiness.

No ticker-specific runtime rules are recommended.

## Artifacts

The phase produced:

- `anchor_conflict_rows.csv`
- `anchor_conflict_company_timelines.csv`
- `company_fiscal_patterns.csv`
- `anchor_consistency_scores.csv`
- `suspect_anchors.csv`
- `resolved_anchor_conflicts.csv`
- `unresolved_anchor_conflicts.csv`
- `adp_case_analysis.md`
- `additional_unresolved_recovery.csv`
- `source_reliability_diagnostic.csv`
- `post_a2_identity_coverage.csv`
- `duplicate_key_interaction.csv`
- `phase3_fiscal_identity_algorithm.md`
- `summary.json`
- `recommended_next_step.md`
