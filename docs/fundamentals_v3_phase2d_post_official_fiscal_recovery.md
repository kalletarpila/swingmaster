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
| Rows recovered from official fiscal-year end evidence | 1,311 |
| Rows recovered from official 52/53-week patterns | 31 |
| Total newly recovered rows | 1,342 |
| Final still unresolved rows | 6 |

Coverage against the full Yahoo normalized bootstrap:

| Metric | Rows |
| --- | ---: |
| Yahoo normalized rows | 14,373 |
| Identity ready before A3 | 13,025 |
| Final identity-ready rows | 14,367 |
| Final identity-ready percentage | 99.96% |
| Final unresolved percentage | 0.04% |

Within the original 3,651 `METADATA_NOT_RESOLVED` rows:

| Metric | Rows |
| --- | ---: |
| Identity ready after A3 | 3,645 |
| Still identity unresolved after A3 | 6 |

## Remaining Manual Review

Six rows remain unresolved:

| Ticker | Period end | Reason |
| --- | --- | --- |
| BNC | 2024-12-31 | Yahoo period does not deterministically match the official April fiscal calendar. |
| BNC | 2025-03-31 | Yahoo period does not deterministically match the official April fiscal calendar. |
| CAVA | 2026-03-31 | Recovery would create a duplicate fiscal identity already represented by another period row. |
| DPZ | 2026-05-31 | Yahoo period does not deterministically match Domino's official 52/53-week period. |
| LFCR | 2025-09-30 | Lifecore fiscal-year transition period needs explicit Phase 3 policy. |
| NEUP | 2025-09-30 | Recovery would create a duplicate fiscal identity already represented by another period row. |

These rows are not discarded. They are held for Phase 3 manual policy or duplicate work-unit
resolution.

## Publication-Date Recovery

Publication date remains orthogonal to fiscal identity. A row can be fiscal-identity ready while
publication date remains unresolved.

| Metric | Rows |
| --- | ---: |
| Publication-unresolved rows entering A3 | 1,603 |
| Publication dates recovered through official research | 509 |
| Publication dates recovered where fiscal identity was also newly resolved | 505 |
| Final publication-ready row count | 13,279 |
| Final publication-ready percentage | 92.39% |
| Remaining publication-unresolved rows | 1,094 |

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
