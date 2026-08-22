# Fundamentals V3 Phase 3B Yahoo Canonical Seed

Status: `FUNDAMENTALS_V3_PHASE3B_YAHOO_CANONICAL_SEED_COMPLETE`

Phase 3B applies the completed Yahoo bootstrap as the first source-specific canonical V3 enrichment pass. It uses the Phase 3A source-agnostic engine and does not apply V2 or Legacy fundamental values.

## Scope

- Source: `YAHOO`
- Production canonical DB: `/home/kalle/projects/swingmaster/rc_fundamentals_v3.db`
- Bootstrap run: `V3_YAHOO_FULL_BOOTSTRAP_20260821T140717Z`
- Canonical apply API: `V3CanonicalMigrationEngine.apply_source_batch(..., source="YAHOO")`

V2 and Legacy evidence are used only through previously locked metadata/recovery artifacts. They do not contribute canonical values in this phase.

## Inputs

- Phase 2D Yahoo raw cache and normalized bootstrap artifacts
- Phase 2D Post-A metadata recovery artifacts
- Phase 2D Post-A2 fiscal anchor resolution artifacts
- Phase 2D Post-A3/A4 official fiscal recovery and exception artifacts
- Phase 2D Post-B activity baseline

The company baseline contains the approved historical V3 universe and the final `active` classification. Inactive companies remain eligible for historical Yahoo canonical rows.

## Candidate Policy

All normalized Yahoo rows receive a deterministic Phase 3B disposition:

- `DIRECT_CANONICAL_CANDIDATE`
- `COMPLEMENTARY_SAME_FISCAL_Q`
- `FISCAL_MAPPING_CORRECTION`
- `PROVIDER_PERIOD_VARIANT_EXCLUDED`
- `OTHER_RESOLUTION_REQUIRED` only if metadata is unexpectedly absent

Accepted Yahoo fields include `operating_income`, promoted from the Phase 2B provider-detail location for canonical Phase 3 storage.

## Special Cases

- CAVA FY2026 Q1: Yahoo provider-period variants are merged into one canonical `FY2026 Q1`; period-date differences are handled through the Phase 3A safe-variant hook.
- NEUP: official fiscal mapping is applied so `2025-09-30 -> FY2026 Q1`, `2025-12-31 -> FY2026 Q2`, and `2026-03-31 -> FY2026 Q3`.
- LFCR `2025-09-30`: excluded as a provider-period variant and not imported as a standalone canonical quarter.

## Safety

The runner performs a full temp dry-apply gate before production writes. Production apply is idempotent through Phase 3A candidate source records and migration audit keys. A source-boundary backup is created after Yahoo seed.

## Results

The executed Phase 3B source-contribution snapshot is stored in:

`temp/fundamentals_v3_phase3b_yahoo_seed/20260822T_PHASE3B_YAHOO_SEED/phase3c_baseline.json`

This snapshot is the exact pre-V2 baseline for Phase 3C.

## Executed Counts

- Migration run ID: `V3_PHASE3B_YAHOO_SEED_20260822T000000Z`
- Approved companies: `2812`
- Active companies: `2735`
- Inactive companies: `77`
- Normalized Yahoo rows examined: `14373`
- Direct canonical candidates: `14278`
- Complementary same-Q candidates: `17`
- Fiscal mapping corrections: `77`
- Provider-period variants excluded: `1`
- Canonical Q rows: `14345`
- Companies with zero Yahoo Q: `58`
- Core-ready Q rows: `12344`
- Core-not-ready Q rows: `2001`
- Publish date known: `12990`
- Publish date NULL: `1355`
- Resolution issues: `274`

Issue counts:

- `NON_NULL_FIELD_CONFLICT`: `248`
- `PERIOD_DATE_CONFLICT`: `19`
- `PUBLICATION_DATE_CONFLICT`: `6`
- `TRANSITION_PERIOD_VARIANT`: `1`

Integrity:

- `PRAGMA quick_check`: `ok`
- `PRAGMA foreign_key_check`: `0 rows`
- duplicate company key: `0`
- duplicate company/FY/FQ key: `0`

Idempotency:

- second-run Q creations: `0`
- inappropriate second-run field fills: `0`
- duplicate issue count: `0`

Source contribution exclusions:

- V2 canonical value contribution: `0`
- Legacy canonical value contribution: `0`
- provider/network calls: `0`
- RawCandle changes: `0`

Source-boundary backup:

`temp/fundamentals_v3_phase3b_yahoo_seed/20260822T_PHASE3B_YAHOO_SEED/backups/rc_fundamentals_v3.post_yahoo_seed.db`
