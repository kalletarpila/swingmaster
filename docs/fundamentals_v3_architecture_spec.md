# Fundamentals V3 Architecture Specification

This document defines the target architecture for a single canonical SwingMaster fundamentals V3
store. It is a design specification only. It does not create a V3 database, change runtime code,
call providers, write production data, change RawCandle, or execute the EBITDA score rewrite.

The conceptual state terminology must remain aligned with
`docs/quarterly_result_canonical_state_model.md`. If the two documents differ, this document defines
the V3 implementation architecture and the canonical state document defines shared terminology.

## Goals

- Replace the long-term Legacy plus V2 dual-store model with one production fundamentals database.
- Preserve the company's reported fiscal-period identity as the canonical quarter identity.
- Bootstrap V3 from Yahoo historical quarterly fundamentals first, then fill remaining safe gaps from
  Legacy and V2.
- Separate Q lifecycle, provider acquisition, readiness, SEC assurance, operational actions,
  historical backfill, TTM, score, valuation, and event history.
- Minimize provenance to the metadata needed for reconciliation, recovery, correction, and migration.
- Keep Legacy and V2 as read-only archive and rollback sources after cutover.

## Non-Goals

- No V3 implementation, schema creation, provider calls, production DB writes, scheduler changes, or
  downstream cutover in this phase.
- No permanent Legacy/V2 dual-write requirement.
- No silent non-null correction policy.
- No attempt to fix the current Legacy TTM EBITDA blocker inside Legacy.

## Architecture Principles

- One eventual production database: recommended filename `rc_fundamentals_v3.db`.
- One canonical Q per `market + ticker + fiscal_year + fiscal_quarter`, where fiscal year and quarter
  mean the company's reported fiscal period.
- `period_end_date`, publication date, SEC filing date, provider observation date, and market
  availability date are metadata/evidence, not identity.
- `CALENDAR_COMPARISON_PERIOD` is separate analytical metadata. Prefer deriving it from
  `period_end_date` using `APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END`; persist only if query
  performance or UI requirements justify it.
- Provider progress belongs in provider-Q acquisition rows, not in composite Q lifecycle values.
- Canonical values are NULL-preserving by default. Normal Update fills NULLs when safe evidence
  exists and does not silently overwrite non-null canonical values.
- Q readiness is derived from canonical fields and downstream windows.
- Operational settlement means no normal automatic work is due under current policy. It does not mean
  core ready, SEC confirmed, score ready, or historically complete.
- Historical backfill is separate from latest-Q daily work.
- Detailed field provenance from V2 is not carried forward unless a concrete reconciliation need
  exists.

## Database Ownership

V3 becomes the only writable production fundamentals store after cutover. Legacy
`fundamentals_usa.db` and V2 `rc_fundamentals_v2.db` remain read-only archive/rollback sources.
RawCandle eventually points Check and Update to V3 and drops dual-store scope/hash orchestration.

## Table Model

Minimum useful V3 tables:

| Table | Natural key | Purpose | Persisted vs derived |
| --- | --- | --- | --- |
| `v3_company` | `market + ticker` | Company/security identity, profile, active flag. | Persisted. |
| `v3_provider_symbol_alias` | `company_id + provider + provider_symbol` | Provider symbol mapping when Yahoo/SEC/SimFin differ from ticker. | Persisted. |
| `v3_quarter` | `company_id + fiscal_year + fiscal_quarter` | Canonical reported fiscal Q identity and Q-level metadata. | Persisted. |
| `v3_quarter_fundamentals` | `quarter_id` | One canonical value per supported field. Could be folded into `v3_quarter`; use 1:1 table if it keeps metadata cleaner. | Persisted canonical values. |
| `v3_provider_q_acquisition` | `quarter_id + provider` | Independent provider acquisition/retry state. | Persisted operational state. |
| `v3_raw_provider_payload` | `provider + provider_symbol + fetch_run_id + payload_hash` | Reproducible raw/staging store for bootstrap and parser fixes. | Persisted compact raw evidence. |
| `v3_result_calendar` | `company_id + provider + expected_result_date` | Future expected results and calendar maintenance. | Persisted scheduling state. |
| `v3_operational_action` | `action_id` | Durable retry/backoff/manual-review queue where derived action is not enough. | Persisted selectively. |
| `v3_event` | `event_id` | Run observability: result detected, enriched, core ready changed, SEC confirmed, score ready changed. | Persisted compact history. |
| `v3_ttm` | `company_id + as_of_quarter_id` | Company-window TTM outputs. | Persisted outputs. |
| `v3_score` | `company_id + as_of_quarter_id + score_model_version` | EBITDA-based score outputs and readiness. | Persisted outputs. |
| `v3_valuation` | `company_id + valuation_date + model_version` | Valuation snapshot/output. | Persisted outputs. |
| `v3_migration_audit` | `migration_run_id + source + source_key` | Migration traceability and exclusions. | Persisted during migration. |
| `v3_resolution_issue` | `issue_id` | Migration identity conflicts, non-null correction candidates, and manual-review items. | Persisted only for material unresolved issues. |

Expected row scale: companies in the thousands, quarters in the low hundreds of thousands,
provider-Q rows as `quarters * providers`, and events/actions small relative to provider rows.

## Company Model

Use a surrogate `company_id` for foreign keys plus a unique natural key on `market + ticker`.
Supported company profiles are `ORDINARY`, `BANK`, and `INSURANCE`. Only `ORDINARY` is in scope for
the current core/score model. Provider aliases are optional rows, not columns on company.

## Canonical Quarter Model

`v3_quarter` stores:

- `company_id`
- `fiscal_year`
- `fiscal_quarter`
- `period_end_date`
- `publish_date`
- `market_availability_date`
- optional derived/persisted calendar comparison fields
- `q_lifecycle`
- `created_at_utc`
- `updated_at_utc`

The unique canonical identity is `company_id + fiscal_year + fiscal_quarter`. `period_end_date`
means the accepted fiscal period end-like date for that reported fiscal Q. `publish_date` means the
result publication/availability evidence date. `market_availability_date` is a downstream timing
date and may be derived from publish date plus trading-calendar policy.

The deterministic V3 historical cutoff is:

```text
include Q only if accepted period_end_date >= 1999-01-01
```

A quarter beginning in late 1998 but ending in 1999 is included. A quarter ending in 1998 is
excluded even if published in 1999. If period-end evidence is unavailable, do not silently use
publish date; classify the row for identity recovery or exclusion.

SimFin migration mapping:

```text
SimFin Report Date  -> V3 period_end_date
SimFin Publish Date -> V3 publish_date
```

V2 currently has 308
groups, covering 638 rows, where the same reported fiscal label has more than one SimFin
`Report Date`. Verification shows V2 `report_date` is not publication date; it is populated from
SimFin `Report Date` and V2 stores SimFin `Publish Date` separately in `publish_date`. These groups
therefore need fiscal-period-end validation or explicit canonical period-end selection. Do not
classify them as identity ambiguity merely because multiple publication observations exist, and do
not silently extend V3 canonical identity with `period_end_date`.

## Fundamental Field Model

Core V3 fields for ordinary companies:

- `revenue`
- `ebitda`
- `free_cashflow`
- `cash`
- `total_debt`
- `shares_outstanding`

Required downstream/supporting fields:

- `ebit`, for legacy valuation/net-debt-to-EBIT compatibility during transition
- `operating_cashflow` and `capex`, for FCF derivation/audit support

Optional fields include `gross_profit` and `net_income`. Weighted-average shares are not canonical
`shares_outstanding` and should not be migrated as equivalent.

`Q_CORE_FIELDS_READY` is derived for ordinary companies:

```text
valid reported fiscal identity
AND revenue IS NOT NULL
AND ebitda IS NOT NULL
AND free_cashflow IS NOT NULL
AND cash IS NOT NULL
AND total_debt IS NOT NULL
AND shares_outstanding IS NOT NULL
AND shares_outstanding > 0
```

## Q Lifecycle

Persist the smallest useful V3 lifecycle:

| State | Meaning | Entry | Exit |
| --- | --- | --- | --- |
| `RESULT_DETECTED` | Reliable evidence says the Q exists, but usable acquisition/canonicalization is not complete. | Calendar/result event, provider source period, or migration identity evidence. | Move to active enrichment or settled if no work is due. |
| `ENRICHING` | Automatic provider acquisition, NULL-fill, assurance, or due retry work is active. | Work selected or retry due. | Work succeeds, becomes blocked, or no due work remains. |
| `OPERATIONALLY_SETTLED` | No useful normal automatic Q-result work is currently due under policy. | No due action remains. | New evidence, retry due, backfill selection, or correction event reopens active work. |

`EXPECTED` belongs in `v3_result_calendar`, not persisted Q lifecycle. `INITIAL_DATA_ACQUIRED` is
an event or derived from provider acquisition rows. `RECONCILING` is a transient Update/migration
phase. Durable unresolved reconciliation creates a `v3_resolution_issue` and may set
`NEXT_ACTION=MANUAL_REVIEW`; it does not create a Q lifecycle state. `REOPENED` is an
event/transition back to `ENRICHING`, not a persisted state.

## Provider Acquisition Model

`v3_provider_q_acquisition` tracks provider progress independently:

| Status | Meaning |
| --- | --- |
| `NOT_CHECKED` | Provider has not been checked for this Q. |
| `ACQUIRED` | Provider supplied usable accepted or stageable data. |
| `PARTIAL` | Provider supplied usable partial data but not all expected fields. |
| `NO_DATA` | Provider checked successfully and returned no data for this Q. |
| `RETRYABLE` | Provider failure or partial state is eligible for retry. |
| `FAILED` | Provider failed and is not currently retryable under policy. |
| `UNSUPPORTED` | Provider cannot supply this field/Q/profile class. |
| `SETTLED` | No further useful provider work is due for this provider/Q under policy. |

Rows include `last_checked_at_utc`, `next_retry_at_utc`, `attempt_count`, `usable_field_count`,
`provider_payload_ref`, `last_error_code`, and `updated_at_utc`. Do not create composite Q states
such as `YAHOO_DONE_SEC_PENDING_SIMFIN_RETRY`.

## Yahoo-First Bootstrap

Initial V3 migration order is:

```text
Ticker universe
      |
      v
Yahoo historical fetch
      |
      v
Yahoo raw/staging
      |
      v
Initial V3 canonical population
      |
      v
Legacy gap-fill
      |
      v
V2 gap-fill
      |
      v
Conflict / integrity validation
      |
      v
TTM + Score + Valuation rebuild
```

The historical floor is deterministic: include a Q only if the accepted `period_end_date` is
`>= 1999-01-01`. A quarter beginning in late 1998 but ending in 1999 is included. A quarter ending
in 1998 is excluded even if it was published in 1999.

Yahoo bootstrap is not Yahoo-always-wins. It is seed order. Canonical selection still uses semantic
validation, cross-source reconciliation, retained accepted values, NULL-fill rules, and controlled
resolution issues.

The bootstrap runner must be resumable by ticker/provider/run, cache raw responses, throttle calls,
record per-ticker completion, and avoid restarting from ticker 1 after a partial failure. Normal V3
daily Update must not re-fetch all historical Yahoo periods.

`v3_raw_provider_payload` is bootstrap staging/cache, parser-recovery input, and reproducibility aid.
It is not permanent heavy analytical provenance. Retain raw payloads through successful canonical
ingestion, migration/update validation, and a safety window; after that, prune or archive compressed
raw outside the canonical DB. Permanent V3 rows should keep only compact accepted-source metadata:
`accepted_source_provider`, `accepted_at`, `update_run_id`, `derivation_method`, and a resolution
issue reference where applicable.

## Source Precedence and Migration

Yahoo-first is seed order, not canonical authority. Initial migration has three separate concerns:

1. Yahoo seed: fetch/cache/stage as much historical Yahoo quarterly data as available.
2. Cross-source reconciliation: compare Yahoo, Legacy, and V2 where canonical fiscal identity is
   sufficiently reliable.
3. Final canonical selection: write one selected V3 value per Q/field or create a resolution issue.

Default seed/fill order is:

1. Yahoo historical quarterly data, accepted only when field semantics and fiscal identity are safe.
2. Legacy safe NULL-fill for remaining gaps.
3. V2 safe NULL-fill for remaining gaps.

This order must not mean Yahoo always wins. Migration reconciliation classifies competing non-null
values as:

- `EXACT_MATCH`
- `ROUNDING_EQUIVALENT`
- `EXPECTED_SEMANTIC_DIFFERENCE`
- `SAME_SEMANTICS_DIFFERENT_VALUE`
- `SOURCE_VALUE_SUSPECT`
- `UNRESOLVED_CONFLICT`

Normal post-cutover V3 Update remains simpler than migration: fill NULLs when safe evidence exists,
retain accepted non-null canonical values, and use controlled correction/resolution issues for
non-null replacement.

Also compare `Yahoo -> V2 -> Legacy` read-only during implementation. If field-level order
materially improves semantic safety or readiness, use field-specific fallback precedence.

Legacy/V2 quarter matching classes:

- `EXACT_FISCAL_MATCH`
- `DATE_SUPPORTED_MATCH`
- `V2_ASSISTED_LEGACY_MATCH`
- `AMBIGUOUS`
- `UNMATCHED`

Legacy rows that expose only `period_end_date` cannot independently create reported fiscal identity
unless V2/provider fiscal labels or another validated fiscal-calendar mapping supports them.

Conflict policy:

- exact or rounding-equivalent values can be accepted without manual review
- expected semantic differences are logged
- likely bad source values create a resolution issue
- unresolved non-null conflicts do not become permanent runtime complexity
- migration never silently overwrites accepted non-null canonical values

Final field-specific precedence is documented in
`temp/fundamentals_v3_design_r1/20260820_214554/v3_final_field_source_precedence.csv`. In summary:
revenue, FCF, cash, total debt, OCF, and capex can use Yahoo/Legacy/V2 comparisons when field
semantics validate; EBITDA prefers validated V2/SimFin when Yahoo is sparse or unsafe; shares require
strict period-end shares semantics and must not use weighted-average shares as equivalent; EBIT is
supporting/transition-only.

## Multi-Report-Date Groups

R1 classified the 308 V2 same-fiscal-label groups with multiple SimFin `Report Date` values:

| Class | Groups | Import policy |
| --- | ---: | --- |
| `PERIOD_DATE_CORRECTION_OR_ENRICHMENT` | 5 | Auto-import if deterministic merge rule is implemented. |
| `SAFE_PERIOD_DATE_VARIANT` | 166 | Auto-import with explicit period-end selection. |
| `LIKELY_52_53_WEEK_CALENDAR_EFFECT` | 11 | Review unless adjacent-period fiscal-calendar validation is implemented. |
| `LIKELY_DUPLICATE_SOURCE_ROW` | 50 | Auto-import after duplicate collapse. |
| `TRUE_FISCAL_IDENTITY_CONFLICT` | 32 | Review/block. |
| `UNRESOLVED` | 44 | Review/block. |

Auto-importable by the current design heuristic: 221 groups. Review/block: 87 groups.

Canonical period-end selection rule for these groups:

1. collapse exact/near-duplicate rows when field values are equivalent
2. merge clear correction/enrichment versions field-wise when later evidence preserves existing
   common values and adds safer non-null values
3. select a period end only when source semantics, adjacent fiscal continuity, and cross-source
   evidence support it
4. do not use minimum date, maximum date, or latest publish date as an automatic rule
5. create a `v3_resolution_issue` for unresolved or materially conflicting rows

## Read-Only Projection Summary

The design-phase projection used only local read-only DB access. It made no provider calls.

| Metric | Value |
| --- | ---: |
| Legacy companies | 2,936 |
| V2 companies | 4,323 |
| Local Yahoo-cache companies | 2,933 |
| Legacy quarter rows, 1999+ | 156,070 |
| V2 quarter rows, 1999+ | 82,812 |
| Local Yahoo-cache quarter rows, 1999+ | 15,186 |
| V2 strict canonical fiscal Q identities | 82,482 |
| Legacy rows date-matched to V2 | 54,979 |
| Legacy date-only rows not matched to V2 | 101,091 |
| V2 same-fiscal-label groups with multiple SimFin Report Date values | 308 |
| V2 rows in those groups | 638 |
| Multi-report-date groups auto-importable by R1 heuristic | 221 |
| Multi-report-date groups requiring review/block | 87 |
| Latest-Q denominator | 4,323 |
| Projected latest-Q core ready after local Yahoo + Legacy + V2 | 1,829, or 42.31% |

Field coverage after local Yahoo + Legacy + V2 on the strict V2 fiscal-identity denominator:

| Field | Coverage |
| --- | ---: |
| revenue | 91.33% |
| ebitda | 92.70% |
| free_cashflow | 92.25% |
| cash | 97.99% |
| total_debt | 80.00% |
| shares_outstanding | 70.19% |
| ebit | 58.00% |
| operating_cashflow | 99.56% |
| capex | 91.58% |

Current 8Q completeness from V2 projection:

| Metric | Companies complete |
| --- | ---: |
| revenue | 3,069 / 4,323, or 70.99% |
| ebitda | 3,093 / 4,323, or 71.55% |
| free_cashflow | 2,963 / 4,323, or 68.54% |
| shares_outstanding | 1,823 / 4,323, or 42.17% |
| cash | 3,403 / 4,323, or 78.72% |
| total_debt | 2,495 / 4,323, or 57.71% |

V3 migration alone does not fully resolve the current Legacy TTM EBITDA blocker. V2 has much better
EBITDA coverage than Legacy, but SCORE_READY still depends on full windows for revenue, EBITDA,
FCF, leverage inputs, and shares/dilution.

Legacy identity recovery projection:

| Classification | Rows | Basis |
| --- | ---: | --- |
| `FISCAL_IDENTITY_RESOLVED` | 54,979 | Date-matched to V2 explicit fiscal identity. |
| `FISCAL_IDENTITY_RECOVERABLE` | 3,154 | Date-matched to local Yahoo cache; live Yahoo may improve this materially. |
| `DATE_ONLY_UNRESOLVED` | 97,937 | No local V2/Yahoo fiscal identity evidence. |
| `EXCLUDED` | 0 | 1999+ projection excludes earlier rows before this classification. |

The strict V2 fiscal-identity denominator is a local projection denominator, not the final V3
universe. Final V3 can grow when live Yahoo historical bootstrap supplies additional reported fiscal
identities or when Legacy date-only rows are recovered through provider fiscal labels, validated
company calendars, or period-end continuity.

Detailed R1 artifacts are under `temp/fundamentals_v3_design_r1/20260820_214554/`.

## SEC and Assurance

SEC can be both provider evidence and assurance. Keep assurance separate from Q lifecycle. Minimal
quarter-level assurance states:

- `NOT_APPLICABLE`
- `NOT_YET_DUE`
- `PENDING`
- `CHECKED_NO_EVIDENCE`
- `PARTIAL_EVIDENCE`
- `CONFIRMED`
- `UNSUPPORTED`
- `ERROR_RETRY`

SEC confirmation should not be required for operational settlement, core readiness, TTM readiness,
or score readiness unless a field-specific policy explicitly requires SEC evidence.

## Action, Retry, and Resolution Issues

Use a hybrid model. Ordinary next action can be derived from Q/provider/readiness state. Persist
`v3_operational_action` only when scheduling, retry/backoff, manual review, or historical backfill
needs durable queue state.

Minimal actions:

- `CHECK_RESULT`
- `FETCH_INITIAL`
- `ENRICH_Q`
- `RETRY_PROVIDER`
- `CHECK_SEC`
- `BACKFILL_HISTORICAL`
- `MANUAL_REVIEW`

Durable conflicts and manual-review cases live in `v3_resolution_issue`, not Q lifecycle. One table
is enough for migration identity conflicts, non-null value corrections, and manual-review items.
Conceptual fields:

- `issue_id`
- `quarter_id` or unresolved source identity
- `issue_type`
- `field_name` if field-specific
- `status`
- `created_at_utc`
- `resolved_at_utc`
- `resolution`
- compact source-value/details JSON

`NEXT_ACTION=MANUAL_REVIEW` may coexist with `q_lifecycle=OPERATIONALLY_SETTLED` when no automatic
work remains, or with `q_lifecycle=ENRICHING` when other automatic work is still due.

## Event History

Persist compact events for run reporting:

- `RESULT_DETECTED`
- `INITIAL_DATA_ACQUIRED`
- `Q_ENRICHED`
- `CORE_READY_CHANGED`
- `SEC_CONFIRMED`
- `SCORE_READY_CHANGED`
- `PROVIDER_FAILED`
- `RESOLUTION_ISSUE_CREATED`
- `REOPENED`

This is not event sourcing. Canonical tables remain authoritative snapshots.

## Historical Backfill

Historical data debt is derived from missing historical windows that materially block TTM, growth,
EBITDA trend, FCF history, dilution, leverage, score readiness, or valuation readiness. Historical
backfill actions are scheduled separately from latest-Q daily flow. Optional EBIT/OCF/capex do not
create debt unless a current consumer requires them.

## TTM Model

TTM is a company/window output, not Q lifecycle. V3 should persist TTM rows keyed by
`company_id + as_of_quarter_id` and rebuild affected windows deterministically after canonical Q
changes. Required outputs include:

- `revenue_ttm`
- `revenue_growth_ttm_yoy`
- `ebitda_ttm`
- `ebitda_margin_ttm`
- `ebitda_margin_trend_4q`
- `free_cashflow_ttm`
- `fcf_margin_ttm`
- `net_debt`
- `net_debt_to_ebitda`
- `share_dilution_yoy`

Keep `ebit_ttm` and `net_debt_to_ebit` only for transition/legacy compatibility.

## Score Model

V3 score uses the current EBITDA-based profitability model, not the old EBIT margin score. Persist:

- raw score
- lifecycle-adjusted/final score
- component points
- `score_model_version`
- `score_ready`
- missing readiness reasons

`SCORE_READY` is derived from supported company profile plus available TTM growth, EBITDA margin,
EBITDA margin trend, FCF margin, leverage, dilution, and EBITDA consistency inputs. A score row may
exist while `score_ready=false` if explanation/output needs that visibility.

## Valuation Model

Valuation is a separate output. Persist enough to explain displayed valuation:

- valuation date
- close price
- market cap
- enterprise value
- shares, cash, total debt used
- EBITDA/EBIT/FCF inputs
- EV/EBITDA and FCF yield outputs
- valuation bucket/status
- model version
- fundamental as-of quarter and staleness

Do not couple valuation readiness to Q lifecycle.

## Check Workflow

V3 Check should produce a fresh plan from V3 state:

1. refresh/result-calendar maintenance as due
2. detect new published results
3. identify Qs needing initial fetch
4. identify Qs needing enrichment or provider retry
5. identify SEC pending/confirmed work
6. keep historical backfill separate
7. emit current snapshot plus run events plus executable work plan

The plan should include stable work-unit identity, candidate hash, source/provider summaries, and
validation under the Update contract.

## Update Workflow

V3 Update consumes exactly one validated plan. It calls only needed providers, stores raw payloads
where useful, normalizes/stages data, applies NULL-fill canonical writes, updates provider
acquisition/action state, emits events, and rebuilds affected TTM, score, and valuation either
synchronously or as explicit deterministic post-steps.

Partial provider failure should not discard useful successful provider data. Canonical Q updates and
provider state updates should commit in a transaction. Downstream rebuild failures should leave a
durable follow-up action rather than rolling back accepted canonical evidence unless consistency
requires atomicity for that operation.

## Data Integrity and Indexing

Use SQLite constraints where simple:

- unique `company_id + fiscal_year + fiscal_quarter`
- fiscal quarter in `Q1`..`Q4`
- non-null canonical identity fields
- positive `shares_outstanding` when present
- no duplicate provider-Q acquisition row
- foreign keys for quarter/company/action/event/output rows
- no duplicate active unresolved follow-up for the same semantic action

Indexes should target actual access paths: latest Q by company, fiscal identity lookup, due actions,
provider retry, historical backfill, TTM/score lookup, calendar comparison, and active universe.

## RawCandle Impact

After V3 cutover, RawCandle should use V3 Check and Update commands, a V3 DB path/config, and a V3
summary parser. Dual-store V2 scope/hash semantics become obsolete. This phase makes no RawCandle
changes.

## Concepts Dropped or Replaced

- Legacy `quarter_basic_complete`: replace with derived `Q_CORE_FIELDS_READY`.
- Legacy `score_history_complete`: replace with derived `SCORE_READY`.
- V2 `core_complete` preflight flags: replace with derived readiness and provider acquisition.
- Heavy V2 field-source provenance rows: replace with compact accepted-source metadata and event or
  correction records.
- Dual-store update coordinator and runtime Legacy/V2 crosswalk: keep only for migration/archive.
- Old EBIT-centric score readiness: archive; target score is EBITDA-based.
- Provider-combination lifecycle statuses: drop.

## Cutover

Recommended cutover:

1. Build V3 schema/repositories/importers.
2. Run Yahoo bootstrap and Legacy/V2 migration into V3 with read-only validation.
3. Validate TTM, score, valuation, Check, and Update in shadow mode.
4. Switch Check and Update to V3 behind a config flag.
5. Switch downstream TTM/score/valuation/snapshot consumers to V3.
6. Freeze Legacy and V2 as read-only archives/rollback sources.

A temporary dual-write period is not recommended unless shadow validation finds a concrete gap that
cannot be resolved with a config-based cutover.

Rollback is config-based: keep Legacy and V2 untouched, take backups before V3 enablement, disable
the V3 execution path, restore the previous execution path, and validate DB integrity plus critical
downstream snapshots.

## Open Questions

- Live Yahoo historical depth, endpoint limits, and reported fiscal-label availability require a
  later network-authorized validation.
- Whether actual period-start dates from a future reliable source should supersede
  `APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END`.
- Whether calendar comparison fields stay derived or become persisted for query performance.
- Exact field-specific precedence after comparing `Yahoo -> Legacy -> V2` with
  `Yahoo -> V2 -> Legacy`.
- How many of the 87 review/block multi-Report-Date groups can be resolved by adjacent-period
  fiscal-calendar validation.
- Which event types are necessary for durable "today" reporting after implementation.

## Implementation Phases

1. Schema and repository layer.
2. Yahoo bootstrap raw cache, normalizer, and resumable bootstrap runner.
3. Legacy/V2 migration importer and read-only projection parity.
4. Provider acquisition model plus V3 Check.
5. V3 Update, canonicalization, resolution issues, and retry actions.
6. TTM, EBITDA score, valuation, and readiness rebuilds.
7. Production validation and shadow comparison.
8. Config-based cutover to V3.
9. Legacy/V2 archive freeze and cleanup of obsolete dual-store orchestration.

## Validation

- One eventual production DB: yes, `rc_fundamentals_v3.db`.
- No permanent Legacy/V2 dual-write: yes.
- Fiscal identity remains company-reported: yes.
- Calendar comparison remains separate analytical metadata: yes.
- Multi-provider state is separate from Q lifecycle: yes.
- Q lifecycle does not encode provider combinations: yes.
- Core readiness is not operational settlement: yes.
- SEC is not completion: yes.
- Historical debt is separate from latest-Q flow: yes.
- TTM and SCORE are company-window outputs: yes.
- Provenance is minimized: yes.
- Migration conflict complexity does not become permanent runtime complexity: yes.
