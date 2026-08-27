# Fundamentals V3 Fiscal Calendar Metadata

Status: Phase 8C metadata layer implemented.

The fiscal-calendar layer separates company profile metadata from exact issuer fiscal-year anchors. Exact anchors are normalized rows in `v3_company_fiscal_year_calendar` using issuer fiscal-year labels, for example `fiscal_year=2026` even when FY2026 starts during calendar 2025.

Profile table: `v3_company_fiscal_calendar_profile`.

Anchor table: `v3_company_fiscal_year_calendar`.

Profiles preserve the raw verified Finnish description, source type, and verbatim source reference. Parsed fields are used only when safe. Unparsed descriptions remain valid evidence, while exact fiscal-year anchors stay usable independently.

Authority order: exact fiscal-year anchor, exact official quarter period_end, explicit source metadata, verified company profile, analytical 13/14-week slot, publish-date cadence, generic assumptions.

Fiscal-slot inference returns analytical FY/FQ slot evidence and reason codes. It does not invent or overwrite official period_end values.

Validator statuses: `PASS`, `PASS_WITH_WARNING`, `REVIEW`, `BLOCK_CANDIDATE`.

Stable reason codes include `FY_SHIFT_PLUS_ONE`, `FY_SHIFT_MINUS_ONE`, `FQ_SLOT_MISMATCH`, `PERIOD_END_OUTSIDE_SLOT`, `WEEKDAY_MISMATCH`, `MONTH_END_NORMALIZATION_SUSPECT`, `PUBLISH_SEQUENCE_MISMATCH`, `FISCAL_ANCHOR_CONFLICT`, `DUPLICATE_IDENTITY`, `REVERSE_SEQUENCE`, `INSUFFICIENT_METADATA`, and `CALENDAR_TRANSITION_REVIEW`.

Phase 8C validation is read-only diagnostic evidence. Future Update V3 prevention hardening should activate the validator as a guarded write-path check after review.

Maintenance policy: add one new anchor row per company and fiscal year when official evidence becomes available. Do not add new FY-specific columns. Historical anchors are immutable unless stronger verified evidence proves an error.

Artifact root: `temp/fundamentals_v3_phase8c_fiscal_calendar_metadata/20260827T_PHASE8C`.

## Phase 8D - Fiscal Calendar Prevention Guards

Status: `FUNDAMENTALS_V3_PHASE8D_FISCAL_WRITE_GUARDS_ACTIVE`

Fiscal-calendar guard is active in `V3QuarterRepository.upsert_quarter` before canonical quarter mutation. Exact FY2026/FY2027 anchors are authoritative, backward inference assumes stable fiscal calendar unless positive transition evidence exists, and `REVIEW`/`BLOCK` candidates perform zero canonical writes.

Phase 8 remains `IN PROGRESS`.
