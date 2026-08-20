# Fundamentals V3 Implementation Readiness Contract

This document closes the V3 design phase for implementation. It does not create a V3 database,
change runtime code, call providers, write production data, change RawCandle, or cut over any
consumer.

## Readiness Classification

Classification: `READY_FOR_PHASE_1_IMPLEMENTATION`.

The current V3 architecture and canonical state specifications can be translated directly into
implementation after applying the contracts below. No new material architecture decision should be
made during Phase 1 coding.

## Locked Inputs

- Canonical architecture: `docs/fundamentals_v3_architecture_spec.md`
- Canonical state model: `docs/quarterly_result_canonical_state_model.md`
- Target DB: `rc_fundamentals_v3.db`
- Initial membership authority: current Legacy `fundamentals_usa.db`
- Approved initial V3 universe: `2,812` tickers
- Membership rule: include Legacy tickers unless positive BANK, INSURANCE, ETF/fund, or non-company
  evidence excludes them
- Fiscal identity: `market + ticker/company_id + fiscal_year + fiscal_quarter`, where fiscal year
  and quarter are the company's reported fiscal period

## Phase 1 Scope

Phase 1 coding should implement only schema, repositories, pure derivation helpers, and local tests.

Allowed:

- V3 migration file(s) or schema initializer
- repository classes/functions
- pure readiness/calendar-comparison derivation helpers
- CLI smoke hooks that create or inspect a caller-supplied local V3 DB path
- tests using temp SQLite DBs

Not allowed in Phase 1:

- provider/network calls
- production DB writes
- RawCandle changes
- Legacy/V2 cutover
- scheduler changes
- V3 bootstrap execution against production
- downstream consumer switch

## Schema Contract

Use separate tables rather than folding fundamentals into `v3_quarter`.

Minimum Phase 1 canonical DB tables:

| Table | Required decision |
| --- | --- |
| `v3_schema_version` | V3-local schema tracking. |
| `v3_run` | Run metadata for migration/bootstrap/update/rebuild operations. |
| `v3_company` | `company_id` surrogate key; unique `market + ticker`; persisted `profile='ORDINARY'` for admitted companies; active flag. |
| `v3_provider_symbol_alias` | Optional provider alias mapping; unique `company_id + provider + provider_symbol`. |
| `v3_quarter` | Unique `company_id + fiscal_year + fiscal_quarter`; stores period/publish/availability metadata and lifecycle. |
| `v3_quarter_fundamentals` | 1:1 with `v3_quarter`; stores canonical fields. |
| `v3_provider_q_acquisition` | Unique `quarter_id + provider`; stores provider outcome and retry/cache metadata. |
| `v3_result_calendar` | Expected/result-calendar state separate from canonical Q lifecycle. |
| `v3_operational_action` | Durable retry/backoff/manual-review/backfill queue where derived action is not enough. |
| `v3_event` | Compact event history for run reporting and today metrics. |
| `v3_ttm` | Persisted company-window TTM outputs. |
| `v3_score` | Persisted EBITDA-based score outputs and readiness. |
| `v3_valuation` | Persisted valuation outputs. |
| `v3_migration_audit` | Migration/bootstrap source traceability, exclusions, and universe admission evidence. |
| `v3_resolution_issue` | Identity conflicts, non-null correction candidates, and manual-review issues. |

Raw provider payloads are not stored in canonical `rc_fundamentals_v3.db`. Use external
`rc_fundamentals_v3_raw.db` by default for bootstrap/update raw cache, with a future filesystem
cache allowed only behind the same raw-cache repository interface.

Do not persist `CALENDAR_COMPARISON_PERIOD` in Phase 1 unless a test or query proves a concrete
consumer needs it. Implement it first as a pure derived helper using
`APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END`.

## Core Enum Contract

Use these initial values exactly unless a later migration deliberately extends them:

- `company.profile`: `ORDINARY`
- `q_lifecycle`: `RESULT_DETECTED`, `ENRICHING`, `OPERATIONALLY_SETTLED`
- `provider`: `YAHOO`, `LEGACY`, `V2`, `SEC`, `SIMFIN`
- `provider_acquisition_result`: `NOT_CHECKED`, `ACQUIRED`, `PARTIAL`, `NO_DATA`, `FAILED`,
  `UNSUPPORTED`
- `operational_action.action_type`: `CHECK_RESULT`, `FETCH_INITIAL`, `ENRICH_Q`,
  `RETRY_PROVIDER`, `CHECK_SEC`, `BACKFILL_HISTORICAL`, `MANUAL_REVIEW`
- `operational_action.status`: `ACTIVE`, `DEFERRED`, `BLOCKED`, `RESOLVED`, `CANCELLED`
- `sec_confirmation_state`: `NOT_APPLICABLE`, `NOT_YET_EXPECTED`, `PENDING`,
  `CHECKED_NO_EVIDENCE`, `PARTIAL_EVIDENCE`, `CONFIRMED`, `UNSUPPORTED`, `ERROR_RETRY`,
  `NOT_DERIVABLE`
- `calendar_comparison_method`: `APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END`,
  `ACTUAL_PERIOD_RANGE`
- `calendar_comparison_quality`: `APPROX_OVERLAP`, `ACTUAL_RANGE_OVERLAP`, `AMBIGUOUS`,
  `INSUFFICIENT_DATES`, `IRREGULAR_ACTUAL_PERIOD`

Do not use `INGESTION_COMPLETE` as a canonical V3 lifecycle or readiness state.

## Repository Contract

Implement repository boundaries around business concepts, not provider scripts:

- `V3CompanyRepository`: derive/admit Legacy-authority universe, apply positive exclusions, read
  active/admitted companies.
- `V3QuarterRepository`: upsert and look up canonical fiscal Q identity.
- `V3FundamentalsRepository`: NULL-preserving canonical field writes and field reads.
- `V3ProviderAcquisitionRepository`: provider outcome, retry metadata, and cache refs.
- `V3CalendarRepository`: expected result/calendar maintenance state.
- `V3OperationalActionRepository`: durable due actions and resolved action transitions.
- `V3ResolutionIssueRepository`: identity/value conflicts and manual review.
- `V3MigrationAuditRepository`: source traceability, admission/exclusion evidence, and run-level
  audit rows.
- `V3OutputRepository`: TTM, score, and valuation output writes/reads.
- `V3RawCacheRepository`: external raw payload/cache DB interface.

Repository writes must be transaction-friendly and must not open provider/network clients.

## Workflow Contract

### Bootstrap/Migration

Implementation order:

1. derive approved V3 companies from Legacy membership
2. apply positive exclusions
3. create V3 companies
4. Yahoo-bootstrap all approved tickers, including Legacy-only tickers
5. recover fiscal identities
6. reconcile Yahoo, Legacy, and V2 where available
7. create resolution issues for unresolved identity/value conflicts
8. rebuild TTM, score, and valuation

### Check

V3 Check should have a V3-specific plan version:

```text
fundamental_result_check_plan_v3
```

Stable work-unit identity:

```text
market|ticker|fiscal_year|fiscal_quarter
```

Check exit codes:

- `0`: `SUCCESS`
- `2`: `PARTIAL`
- `1`: `FAILED`

Check must emit at least:

- `check_status`
- `candidate_count`
- `executable_work_unit_count`
- `candidate_hash`
- `plan_json`
- provider/source counts
- due-action counts
- calendar maintenance counts
- provider timing summary

### Update

V3 Update consumes one validated V3 plan. It must reject stale or mismatched `candidate_hash` and
must support explicit `--only-work-unit-key` scope restriction before any production writes are
allowed.

Normal V3 Update behavior:

- fill NULL canonical values only when accepted evidence exists
- preserve non-null canonical values
- create `v3_resolution_issue` for non-null correction candidates
- update provider acquisition/action state
- emit compact events
- rebuild affected TTM/score/valuation or create durable follow-up action

## RawCandle Integration Contract

RawCandle remains unchanged in Phase 1. Current RawCandle integration invokes SwingMaster Check and
Update commands and surfaces summary/log fields. V3 cutover later requires RawCandle config and
summary parsing changes, but V3 business logic must stay inside SwingMaster.

Later RawCandle V3 wiring should add a V3 DB path/config and point to V3 Check/Update commands or
explicit V3 mode. It must not reintroduce a permanent Legacy/V2 dual-store coordinator.

## Design Closure

The remaining documented questions are validation or future-extension items, not Phase 1
architecture blockers:

- live Yahoo historical depth and fiscal-label availability
- future actual `period_start_date` source
- whether derived calendar comparison should be persisted after consumer pressure appears
- how many review/block multi-report-date groups can be resolved by stricter calendar validation
- exact event list extension after real run observability needs are seen

Proceed to Phase 1 coding with schema/repository tests first.
