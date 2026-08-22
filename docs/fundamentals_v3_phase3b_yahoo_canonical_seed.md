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
