# Fundamentals V3 Phase 2D Post-A3 Official Fiscal-Year Recovery

Date: 2026-08-22

Classification: `FUNDAMENTALS_V3_OFFICIAL_FISCAL_RECOVERY_COMPLETE`

## Scope

This phase resolves the 1,348 fiscal-identity-unresolved Yahoo rows left after
Phase 2D Post-A2 by researching official company fiscal-calendar evidence.

The phase is read-only against application data. It writes only audit artifacts under
`temp/`, performs no Yahoo refetch, no SimFin query, no raw-cache mutation, no
canonical V3 write, no Check/Update cutover, and no RawCandle change.

Artifact root:

```text
temp/fundamentals_v3_phase2d_post_official_fiscal_recovery/20260822T_A3_OFFICIAL_FISCAL_RECOVERY/
```

## Source Policy

Official company sources are preferred in this order:

1. official company Investor Relations site
2. official company earnings-release archive
3. official annual report / financial reports page
4. SEC filing metadata only when company sources are unavailable or insufficient

An initial SEC-supported baseline was retained where it had already produced valid evidence. The
remaining manual-review shortlist was then supplemented with IR-first research. Examples of company
sources used include Gap Inc. earnings releases, Vestis financial results, BlackSky quarterly
results, Ferguson quarterly results, Lifecore earnings releases, Greenidge IR releases, Newegg
press releases, Avalon investor filing pages, and Envirotech investor resources.

## Fiscal Identity Outcome

| Metric | Rows |
| --- | ---: |
| Unresolved rows entering A3 | 1,348 |
| Rows recovered from official fiscal-year end evidence | 1,313 |
| Rows recovered from official 52/53-week patterns | 32 |
| Total newly recovered rows | 1,345 |
| Final still unresolved rows | 3 |

Coverage against the full Yahoo normalized bootstrap:

| Metric | Rows |
| --- | ---: |
| Yahoo normalized rows | 14,373 |
| Identity ready before A3 | 13,025 |
| Final identity-ready rows | 14,370 |
| Final identity-ready percentage | 99.98% |
| Final unresolved percentage | 0.02% |

Within the original 3,651 `METADATA_NOT_RESOLVED` rows:

| Metric | Rows |
| --- | ---: |
| Identity ready after A3 | 3,648 |
| Still identity unresolved after A3 | 3 |

## BNC Transition Fiscal Year

User-supplied official reporting evidence resolves both previously unresolved BNC rows. FY2025 was
an exceptional transition fiscal year, so the normal Q1 to Q4 sequence must not be inferred across
the transition. The issuer's explicit labels are used directly:

| Period end | Fiscal identity | Publish date | Evidence |
| --- | --- | --- | --- |
| 2024-12-31 | FY2024 Q4 | 2025-03-27 | Official reported FY24 Q4 / full year |
| 2025-03-31 | FY2025 Q1 | 2025-05-15 | Official reported FY25 Q1 |
| 2025-07-31 | FY2026 Q1 | 2025-09-22 | Official reported FY26 Q1 |
| 2025-10-31 | FY2026 Q2 | 2025-12-15 | Official reported FY26 Q2 |

The new fiscal-calendar regime begins from FY2026. These entries are recorded as migration
evidence, not as hard-coded runtime rules.

## CAVA And DPZ 52/53-Week Calendars

CAVA uses a 52-53-week fiscal year ending in December. Domino's Pizza uses a 4-5-4 week cycle.
Official result calendars from 2024 onward were recorded as migration evidence.

For CAVA, the important unresolved row is:

```text
CAVA 2026-03-31 -> official FY2026 Q1 evidence exists
official period end -> 2026-04-19
publish_date -> 2026-05-19
```

It remains out of `phase3_fiscal_identity_input.csv` because it would create a duplicate fiscal
work unit; another Yahoo period already maps to FY2026 Q1. Phase 3 must resolve this as a duplicate
provider-period/work-unit case, not as an unknown fiscal-calendar case.

For DPZ, the official 4-5-4 evidence resolves the previously unresolved row:

```text
DPZ 2026-05-31 -> FY2026 Q2
official period end -> 2026-06-14
publish_date -> 2026-07-20
```

The Yahoo date is a provider period surrogate and is accepted only because it maps deterministically
to the official FY2026 Q2 result.

## LFCR Fiscal-Year Transition

LFCR's remaining row is no longer an unknown fiscal-calendar case. Official transition evidence
shows that `LFCR 2025-09-30` is not an independent official fiscal quarter-end.

The relevant official structure is:

| Fiscal identity | Official period end | Publish date |
| --- | --- | --- |
| FY2024 Q4 | 2024-05-26 |  |
| FY2025 Q1 | 2024-08-25 |  |
| FY2025 Q2 | 2024-11-24 |  |
| FY2025 Q3 | 2025-03-31 |  |
| FY2025 Q4 transition period | 2025-12-31 | 2026-03-16 |
| FY2026 Q1 | 2026-03-31 | 2026-05-06 |
| FY2026 Q2 | 2026-06-30 | 2026-08-05 |

Therefore A3 does not write:

```text
LFCR 2025-09-30 -> FY2025 Q4
```

The row is recorded as `TRANSITION_PERIOD_DATE_VARIANT`. Phase 3 may merge it into FY2025 Q4 only
if the Yahoo values can be identified as the same transition-result values. It must not create a
new canonical quarter from this row.

## NEUP June Fiscal Year

NEUP uses a June 30 fiscal year:

```text
Q1 -> Jul 1-Sep 30
Q2 -> Oct 1-Dec 31
Q3 -> Jan 1-Mar 31
Q4 -> Apr 1-Jun 30
```

Official result calendar evidence for 2024-2026 was recorded:

| Fiscal identity | Official period end | Publish date |
| --- | --- | --- |
| FY2024 Q1 | 2023-09-30 | 2023-11-14 |
| FY2024 Q2 | 2023-12-31 | 2024-11-21 |
| FY2024 Q3 | 2024-03-31 | 2024-05-15 |
| FY2024 Q4 | 2024-06-30 | 2024-10-31 |
| FY2025 Q1 | 2024-09-30 | 2024-11-14 |
| FY2025 Q2 | 2024-12-31 | 2025-02-14 |
| FY2025 Q3 | 2025-03-31 | 2025-05-20 |
| FY2025 Q4 | 2025-06-30 | 2025-09-29 |
| FY2026 Q1 | 2025-09-30 | 2025-11-14 |
| FY2026 Q2 | 2025-12-31 | 2026-02-17 |
| FY2026 Q3 | 2026-03-31 | 2026-05-15 |
| FY2026 Q4 | 2026-06-30 | 2026-09-28 estimated/future |

`NEUP 2025-09-30` is therefore not an unknown calendar case. It is official FY2026 Q1 with
publish date 2025-11-14.

The supplied FY2026 Q4 publication date, 2026-09-28, is estimated/future metadata only. It is not
stored or counted as an actual `publish_date`.

User-supplied Yahoo screenshot values further show that the competing `NEUP 2026-03-31` row is
FY2026 Q3, not FY2026 Q1:

| Period end | Correct fiscal identity | Publish date | Key screenshot values |
| --- | --- | --- | --- |
| 2025-09-30 | FY2026 Q1 | 2025-11-14 | revenue 0, operating income/EBIT -5.65M, EBITDA -5.49M, net income -9.91M |
| 2025-12-31 | FY2026 Q2 | 2026-02-17 | revenue 0, operating income/EBIT -2.35M, EBITDA -2.18M, net income 1.86M |
| 2026-03-31 | FY2026 Q3 | 2026-05-15 | revenue 0, operating income/EBIT -0.827M, EBITDA -0.662M, net income -0.505M |

Phase 3 should correct the existing `2026-03-31` work-unit mapping to FY2026 Q3 and import
`2025-09-30` as FY2026 Q1. It must not silently overwrite the existing row.

## A4 Final Closure

Classification: `FUNDAMENTALS_V3_FISCAL_IDENTITY_RECOVERY_CLOSED`

Fiscal-calendar research is closed. The final distinction is:

| Metric | Rows |
| --- | ---: |
| Normalized Yahoo rows | 14,373 |
| Rows with known fiscal identity or known reconciliation context | 14,373 |
| True fiscal-identity unknown rows | 0 |
| Fiscal identity knowledge coverage | 100.00% |
| Ready for direct canonical import | 14,370 |
| Canonical reconciliation exceptions | 3 |
| Duplicate work-unit reconciliation exceptions | 2 |
| Transition-period reconciliation exceptions | 1 |

This is not 100% canonical migration readiness. The three exception rows have known fiscal evidence
or known transition context, but Phase 3 must resolve the canonical work-unit treatment before any
canonical V3 write.

Final Phase 3 exception table:

| Ticker | Yahoo period end | Fiscal context | Exception | Field comparison | Recommended Phase 3 action |
| --- | --- | --- | --- | --- | --- |
| CAVA | 2026-03-31 | FY2026 Q1, official period ended 2026-04-19 | Duplicate fiscal work unit | `PARTIAL_OVERLAP_SAME_RESULT_INCOME_VALUES_SUPPLIED_BALANCE_FIELDS_STILL_SPLIT` | Merge only with field provenance, otherwise manual review; do not create a second canonical Q. |
| LFCR | 2025-09-30 | FY2025 Q4 transition context, official period ended 2025-12-31 | Transition-period date variant | `INSUFFICIENT_COMPARISON` | Merge only if field-equivalent, otherwise exclude provider variant from canonical import. |
| NEUP | 2025-09-30 | FY2026 Q1, official period ended 2025-09-30 | Fiscal-quarter mapping collision | `CONFLICTING_VALUES_DIFFERENT_OFFICIAL_QUARTERS` | Correct existing 2026-03-31 work unit to FY2026 Q3 and import 2025-09-30 as FY2026 Q1. |

CAVA field comparison was refined with user-supplied Yahoo screenshot values for `2026-03-31`:
revenue 438.27M, gross profit 111.19M, operating income/EBIT 34.14M, EBITDA 59.60M, net income
23.57M, and basic average shares 116.34M. The competing local `2026-04-30` FY2026 Q1 work unit
contains balance/share fields only. This indicates a split provider-period/work-unit reconciliation
case rather than a second canonical quarter.

Publication coverage after A4:

| Metric | Rows |
| --- | ---: |
| Publication-ready rows | 13,281 |
| Publication-unresolved rows | 1,092 |

## Remaining Manual Review

Three rows remain unresolved:

| Ticker | Period end | Reason |
| --- | --- | --- |
| CAVA | 2026-03-31 | Recovery would create a duplicate fiscal identity already represented by another period row. |
| LFCR | 2025-09-30 | Known transition-period date variant; do not create a new canonical quarter. |
| NEUP | 2025-09-30 | Official FY2026 Q1 label exists, but recovery would create a duplicate fiscal identity. |

These rows are not discarded. They are held for Phase 3 manual policy or duplicate work-unit
resolution.

## Publication-Date Recovery

Publication date remains orthogonal to fiscal identity. A row can be fiscal-identity ready while
publication date remains unresolved.

| Metric | Rows |
| --- | ---: |
| Publication-unresolved rows entering A3 | 1,603 |
| Publication dates recovered through official research | 511 |
| Publication dates recovered where fiscal identity was also newly resolved | 508 |
| Final publication-ready row count | 13,281 |
| Final publication-ready percentage | 92.40% |
| Remaining publication-unresolved rows | 1,092 |

Publication evidence was accepted only when the source could be tied to a fiscal quarter or period
end. Accepted evidence classes are official earnings releases, annual results releases, official IR
earnings-history pages, and SEC filing support where official company pages did not provide enough
detail.

## Phase 3 Input

The primary Phase 3 identity input is:

```text
temp/fundamentals_v3_phase2d_post_official_fiscal_recovery/20260822T_A3_OFFICIAL_FISCAL_RECOVERY/phase3_fiscal_identity_input.csv
```

It contains 1,342 recovered fiscal identities and any recovered publication date evidence. Phase 3
must still handle duplicate fiscal work units deterministically and must not treat publication date
as required for fiscal identity readiness.

## Artifacts

The phase produced:

- `unresolved_rows_entering_a3.csv`
- `unresolved_companies.csv`
- `research_priority.csv`
- `local_fiscal_pattern_recovery.csv`
- `official_fiscal_calendar_evidence.csv`
- `company_fiscal_calendar_models.csv`
- `official_quarter_labels.csv`
- `structural_calendar_changes.csv`
- `recovered_rows.csv`
- `duplicate_fiscal_identity_conflicts.csv`
- `still_unresolved_rows.csv`
- `still_unresolved_companies.csv`
- `phase3_fiscal_identity_input.csv`
- `manual_review_shortlist.csv`
- `official_publication_date_evidence.csv`
- `company_recent_result_calendar.csv`
- `post_a3_publication_coverage.csv`
- `summary.json`
- `recommended_next_step.md`
