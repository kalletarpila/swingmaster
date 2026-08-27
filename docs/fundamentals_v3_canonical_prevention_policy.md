# Fundamentals V3 Canonical Prevention Policy

Date: 2026-08-26

Status: `POLICY_LOCKED_IMPLEMENTATION_PENDING`

Target completion gate: `FUNDAMENTALS_V3_PHASE8_PREVENTION_HARDENING_COMPLETE`

This document is the authoritative permanent prevention policy for V3 canonical ingestion, migration,
backfill, repair, and Update write paths. It records the Phase 8 failure modes that must not be
reintroduced after cleanup. It is documentation only: no production data, RawCandle data, provider
fetching, TTM, Score, Lifecycle, or Valuation output is changed by this policy document.

## Mandatory Phase Gate

Phase 8 must include and complete:

```text
PHASE 8 PREVENTION HARDENING - CANONICAL INGESTION / UPDATE REGRESSION PROTECTION
```

This gate must be completed before final Phase 8 closure, V3 cutover, and Legacy retirement. Cleanup
alone is not sufficient while active or reusable canonical write paths can recreate the known Phase 8
failure modes.

The implementation work is a separate future phase:

```text
PHASE 8 PREVENTION HARDENING - IMPLEMENT CANONICAL WRITE-PATH GUARDS & REGRESSION TESTS
```

## Permanent Rules

1. Canonical identity is `company_id + fiscal_year + fiscal_quarter`. Fiscal labels are issuer
   labels. Never infer FY/FQ from calendar year/quarter, `period_end_date` year/month, or provider
   display labels. For example, FY2026 Q1 may end in calendar 2025. `period_end_date` is metadata,
   not identity.
2. Never synthesize `period_end_date` from FY, quarter, month, calendar-year assumptions, or visual
   label alignment. Never shift `period_end_date` year just to make it look consistent with the FY
   label.
3. Official issuer or SEC `official_period_end_date` outranks normalized provider dates.
   Yahoo/provider-normalized dates must not overwrite it. Preserve Saturday, Sunday, 52/53-week,
   13-week, and 14-week fiscal closes exactly.
4. Month-end normalization is diagnostic only. It must not silently convert weekend or retail fiscal
   closes such as `2025-05-03` to `2025-04-30`.
5. `publish_date` is the first authoritative date when the relevant financial results became public.
   It is event chronology, not source preference. Use an earnings release before a later filing, a
   filing before a later release, and do not replace the original publish date with an amendment,
   restatement, conference call date, provider fetch date, or local ingestion/update date.
6. Restatements must not create hybrid quarters. FY/FQ, period_end, publish_date, values, source
   lineage, original evidence, and restated evidence must be reconciled as one economic-period
   package. `NOT_VERIFIABLE` fields remain unresolved instead of being guessed or overwritten.
7. Economic-quarter content moves together: FY, FQ, period_end, publish_date, core/supporting values,
   accepted source, accession/context, and lineage. Do not perform label-only structural moves unless
   evidence proves the economic content belongs to the target quarter.
8. Before relabel, merge, recreate, multi-quarter shift, or delete operations, inspect the target
   canonical identity and classify it as absent, same economic identical, same economic
   complementary, same economic conflicting, or different economic. Never silently overwrite a
   populated target row.
9. Missing history is allowed. Do not shift later FY/Q labels, synthesize missing quarters, or
   duplicate neighboring rows to create visual continuity. Distinguish `MISSING_HISTORY` from
   `WRONG_CANONICAL_MAPPING`.
10. Fiscal transitions, 10-KT periods, short fiscal years, STUB periods, and fiscal-year changes must
    not be forced into ordinary Q1-Q4 if issuer reporting does not support it. If the current schema
    cannot represent the issuer truthfully, use an approved transition encoding, keep the period
    outside the standard quarterly model, or exclude the issuer.
11. Yahoo-first means seed order, not period-date authority. Phase 8 identified a historical failure
    mode in `swingmaster/fundamentals/v3_yahoo_canonical_seed.py::prepare_yahoo_seed`: normalized
    Yahoo `period_end_date` was used for canonical candidates while `official_period_end_date`
    remained metadata. This is classified as `HISTORICAL_MIGRATION_ARTIFACT`, not a proven active
    Update V3 bug. Any future reuse of that seed/bootstrap/migration/backfill/recovery path must
    prove normalized provider period_end cannot supersede available official period_end.
12. Pre-write guards are mandatory for company identity, FY/FQ uniqueness, target collision, fiscal
    sequence, period-end chronology and gaps, publish chronology, `publish_date >= period_end_date`,
    source-period compatibility, economic-content compatibility, 52/53-week handling, and prevention
    of silent month-end normalization. A hard guard failure blocks the write; heuristic auto-correct
    is not allowed.
13. Post-write audits are mandatory for affected-company duplicate FY/FQ, duplicate economic
    quarters, reverse period sequence, one-year period shifts, multi-quarter shifts, negative lags,
    publish chronology, orphan rows, unrelated canonical drift, and newly introduced P1 findings.
    Broad migration/bootstrap acceptance requires the equivalent global audit.
14. Permanent regression sentinels must cover: FY2026 Q1 ending in 2025; 52/53-week weekend period
    ends; +1-year errors; +1-year plus month-end normalization; official period_end outranking
    provider date; earnings release before filing; filing before later release; amendments and
    restatements not replacing original publish_date; legitimate same-day multi-quarter publication;
    name/ticker changes under the same registrant; sparse history; target FY/Q collisions;
    complementary versus conflicting duplicates; restatement plus identity coupling; transition/STUB
    periods; and provider-normalized period dates being unable to override official dates.

## Current Master-Plan Order

The remaining order before V3 cutover is:

1. Finish current P1 and current-downstream canonical repair work.
2. Run final canonical audit and closure proving.
3. Complete `PHASE 8 PREVENTION HARDENING`.
4. Run one combined downstream rebuild: `TTM -> Score -> Lifecycle -> Valuation`.
5. Complete final Phase 8 proving and closure.
6. Run Phase 9 Production Proving.
7. Perform Phase 10 V3 Cutover.
8. Perform Phase 11 Legacy Retirement.

V3 must not cut over while active canonical write paths can recreate known Phase 8 canonical mapping,
period-end, publish-date, restatement, transition/STUB, or target-collision failure modes.

## Phase 8C - Fiscal Calendar Metadata Layer

Status: `FUNDAMENTALS_V3_PHASE8C_FISCAL_CALENDAR_METADATA_COMPLETE_WITH_REVIEW_ITEMS`

Artifact root: `temp/fundamentals_v3_phase8c_fiscal_calendar_metadata/20260827T_PHASE8C`

Fiscal-calendar profiles and exact FY anchors were imported as metadata-only production data. Canonical and downstream fingerprints remained unchanged. Phase 8 remains `IN PROGRESS`.

Future Update V3 write order now includes fiscal-calendar exact anchors and slot validation before canonical write.
