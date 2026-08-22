# Fundamentals V3 Architecture Specification

This document defines the target architecture for a single canonical SwingMaster fundamentals V3
store. It is a design specification only. It does not create a V3 database, change runtime code,
call providers, write production data, change RawCandle, or execute the EBITDA score rewrite.

The conceptual state terminology must remain aligned with
`docs/quarterly_result_canonical_state_model.md`. If the two documents differ, this document defines
the V3 implementation architecture and the canonical state document defines shared terminology.
The implementation-readiness contract is locked in
`docs/fundamentals_v3_implementation_readiness_contract.md` and should be used for Phase 1
schema/repository/workflow coding decisions.

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
| `v3_quarter_fundamentals` | `quarter_id` | One canonical value per supported field in a 1:1 table with `v3_quarter`. | Persisted canonical values. |
| `v3_provider_q_acquisition` | `quarter_id + provider` | Independent provider acquisition outcome plus provider-specific scheduling metadata. | Persisted operational/provider state. |
| external raw/cache store | `provider + provider_symbol + fetch_run_id + payload_hash` | Reproducible raw/staging cache for bootstrap and parser fixes. | Outside canonical V3 DB. |
| `v3_result_calendar` | Stable provider event key where available; else `company_id + provider + target fiscal identity`; fallback to one active unmatched expectation per company/provider/window | Future expected results and calendar maintenance. `expected_result_date` is mutable metadata, not identity. | Persisted scheduling state. |
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
the current core/score model. Initial V3 admission treats approved Legacy-derived companies as using
the ordinary-company fundamentals model. Persist a simple `ORDINARY` profile/support value for
admitted V3 companies if the schema needs a profile field; record `LEGACY_ONLY_UNCLASSIFIED_BY_V2`
in migration audit/provenance only if that distinction is operationally useful. Do not create a
separate runtime profile solely to preserve V2 absence. Provider aliases are optional rows, not
columns on company.

## Initial V3 Universe

The initial V3 company universe is derived from the current Legacy fundamentals database,
`fundamentals_usa.db`, using distinct tickers from `rc_fundamental_quarterly`. Legacy membership is
the admission authority.

Start with every ticker represented in the current Legacy fundamentals universe. Exclude only when
there is positive evidence that the ticker is `BANK`, `INSURANCE`, ETF/fund, or another non-company
security. A Legacy ticker's absence from V2, or absence of V2 profile metadata, is not exclusion
evidence.

The initial universe is not derived from:

- the full `osakedata.db` / OHLCV market-data universe
- the full V2 company universe
- Yahoo-returned symbols
- provider discovery
- result-calendar feeds

The broad market-data universe contains ETFs, funds, and other securities that are not intended for
the ordinary-company fundamentals model. V2 company/profile data may be used as current positive
classification evidence for Legacy tickers, but the full V2 universe is not the V3 universe
authority. V2 absence does not remove a Legacy ticker.

Read-only universe audit on 2026-08-20:

| Population | Count | Initial V3 treatment |
| --- | ---: | --- |
| Legacy fundamentals tickers | 2,936 | Universe authority before eligibility exclusions. |
| Legacy tickers with V2 `ORDINARY` evidence | 2,451 | Included. |
| Legacy-only tickers with no V2 match | 361 | Included by Legacy membership authority unless positive non-company evidence appears. |
| Legacy `BANK` tickers | 82 | Excluded until bank-specific model is approved. |
| Legacy `INSURANCE` tickers | 42 | Excluded until insurance-specific model is approved. |
| Positive ETF/fund/non-company exclusions | 0 | Excluded only with reliable local evidence. |
| Genuine review conflicts | 0 | Review only with contradictory instrument-type evidence. |
| Approved initial V3 universe | 2,812 | Legacy source universe minus positive exclusions. |

Provider bootstrap, provider aliases, V2 migration, and Legacy/V2 gap-fill may enrich only tickers
already admitted to the approved Legacy-derived universe. Tickers that exist only in V2, Yahoo,
provider discovery, calendar feeds, or OHLCV/osakedata do not enter the initial V3 database unless a
later explicit universe-expansion decision approves them. Company admission and Q-level fiscal
identity recovery are separate concerns.

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
canonical quarterly result publication date/evidence date. For accepted canonical quarterly
fundamentals, `market_availability_date` follows the established publication-date availability
invariant: accepted values are historically available from the canonical quarterly result
publication date. Provider first-seen time, local ingestion latency, and SEC filing latency do not
redefine historical market availability. If a trading-calendar transformation is needed by a
specific backtest consumer, treat that as downstream execution-date logic, not a different
fundamental-data availability truth.

The deterministic V3 historical cutoff is:

```text
include Q only if accepted period_end_date >= 2018-01-01
```

A quarter beginning before 2018 but ending on or after 2018-01-01 is included. A quarter ending in
2017 is excluded even if published in 2018. If period-end evidence is unavailable, do not silently use
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

`Q_CORE_FIELDS_READY` is derived for ordinary companies and is source-agnostic:

```text
valid reported fiscal identity
AND accepted canonical revenue IS NOT NULL
AND accepted canonical EBITDA IS NOT NULL
AND accepted canonical free_cashflow IS NOT NULL
AND accepted canonical cash IS NOT NULL
AND accepted canonical total_debt IS NOT NULL
AND accepted canonical shares_outstanding > 0
```

Readiness does not depend on provider source, direct-vs-derived origin, SEC confirmation, or
provenance richness. Accepted canonical FCF may be direct or an approved deterministic
`operating_cashflow + capex` derivation under the established negative-capex convention. Accepted
canonical EBITDA and total debt may likewise be direct or approved safe derivations if their
field-specific semantic contracts are satisfied.

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
`OPERATIONAL_ACTION=MANUAL_REVIEW`; it does not create a Q lifecycle state. `Q_REOPENED` is an
event/transition back to `ENRICHING`, not a persisted state.

## Provider Acquisition Model

`v3_provider_q_acquisition` tracks provider acquisition outcome independently:

| Provider acquisition result | Meaning |
| --- | --- |
| `NOT_CHECKED` | Provider has not been checked for this Q. |
| `ACQUIRED` | Provider supplied usable accepted or stageable data. |
| `PARTIAL` | Provider supplied usable partial data but not all expected fields. |
| `NO_DATA` | Provider checked successfully and returned no data for this Q. |
| `FAILED` | Provider acquisition failed. |
| `UNSUPPORTED` | Provider cannot supply this field/Q/profile class. |

Rows include `last_checked_at_utc`, `next_retry_at_utc`, `attempt_count`, `usable_field_count`,
`provider_cache_ref`, `last_error_code`, and `updated_at_utc`. `acquisition_result` is provider
outcome. `next_retry_at` and `attempt_count` are scheduling metadata. Whether provider work is due is
derived from the result, policy, due dates, and operational action rows. Provider "settled" means no
useful provider-specific automatic work is currently due; it is derived and must not replace the last
meaningful acquisition result. Do not create composite Q states such as
`YAHOO_DONE_SEC_PENDING_SIMFIN_RETRY`.

Minimum provider-Q contract:

- provider observation/outcome: `quarter_id`, `provider`, `acquisition_result`, `last_checked_at_utc`,
  `last_success_at_utc`, `usable_field_count`, `last_error_code`, `provider_cache_ref`
- scheduling metadata: `next_retry_at_utc`, `attempt_count`
- derived due-action state: computed from provider outcome, retry policy, current time, and
  `v3_operational_action`

## Yahoo-First Bootstrap

Initial V3 migration order is:

```text
Approved Legacy-derived V3 universe
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
`>= 2018-01-01`. A quarter beginning before 2018 but ending on or after 2018-01-01 is included. A
quarter ending in 2017 is excluded even if it was published in 2018.

Yahoo bootstrap is not Yahoo-always-wins. It is seed order. Canonical selection still uses semantic
validation, cross-source reconciliation, retained accepted values, NULL-fill rules, and controlled
resolution issues.

Yahoo bootstrap does not discover the initial V3 company universe. It fetches/stages data only for
the approved Legacy-derived tickers admitted by the universe policy above, including Legacy-only
tickers that are absent from V2.

The bootstrap runner must be resumable by ticker/provider/run, cache raw responses, throttle calls,
record per-ticker completion, and avoid restarting from ticker 1 after a partial failure. Normal V3
daily Update must not re-fetch all historical Yahoo periods.

Raw provider payload storage is bootstrap staging/cache, parser-recovery input, and reproducibility
aid. It is not canonical fundamentals data and should live outside `rc_fundamentals_v3.db`, either in
`rc_fundamentals_v3_raw.db` or a repository cache/filesystem location. Retain raw payloads through
successful canonical ingestion, migration/update validation, and a safety window; after that, prune
or archive compressed raw outside the canonical DB. Permanent V3 rows should keep only compact
accepted-source metadata: `accepted_source_provider`, `accepted_at`, `update_run_id`,
`derivation_method`, and a resolution issue reference where applicable.

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

V2 automatic enrichment is permanently no-overwrite:

`V2_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`

V2 same-quarter confirmation grants identity confidence only. It may fill canonical V3 `NULL`
financial values and `NULL` publication dates after the source-specific identity gate passes. It
must not automatically replace an existing non-null canonical financial value or non-null
`publish_date`; materially different V2 values are comparison or reconciliation evidence for a
later explicit correction workflow.

Legacy automatic enrichment follows the same no-overwrite rule:

`LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`

Legacy may fill existing canonical Q `NULL` values only after source-specific same-quarter
validation. It must not create new canonical Qs during existing-Q enrichment and must not
automatically replace values previously accepted from Yahoo, V2, Legacy, or another source.
Legacy-only historical rows are deep-history candidates for the separate history-extension phase.

Current Legacy implementation order is explicit: Phase 3C-1 performs Legacy existing-Q enrichment,
Phase 3C-1B validates Legacy-only history backward from recent anchors without writes, Phase 3C-1C
applies the 2018-01-01 historical floor and breakpoint diagnostic, Phase 3C-1D models SEC Q4/FY
structure and independent segment recovery without writes, and Phase 3C-2 may create only
the deep-history rows that the latest recovery dry plan classifies as
`READY_FOR_PHASE3C2_IMPORT`.

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
| Approved initial V3 companies | 2,812 |
| Legacy companies with V2 ORDINARY evidence | 2,451 |
| Legacy-only admitted companies not matched to V2 | 361 |
| V2-confirmed BANK exclusions | 82 |
| V2-confirmed INSURANCE exclusions | 42 |
| Positive ETF/fund/non-company exclusions | 0 |
| Genuine review conflicts | 0 |
| Yahoo bootstrap ticker count | 2,812 |
| V2-only tickers excluded from initial V3 | 2,038 |
| osakedata-only tickers excluded from initial V3 | 1,545 |
| V2 ORDINARY source/profile population, not V3 universe authority | 4,323 |
| Local Yahoo-cache companies | 2,933 |
| Approved Legacy quarter rows, historical Phase 0 projection with old 1999+ floor | 148,033 |
| V2 quarter rows, historical Phase 0 projection with old 1999+ floor | 82,812 |
| Local Yahoo-cache quarter rows, historical Phase 0 projection with old 1999+ floor | 15,186 |
| V2 strict canonical fiscal Q identities | 82,482 |
| Approved Legacy rows date-matched to V2 | 54,979 |
| Approved Legacy rows date-matched to local Yahoo cache | 12,896 |
| Approved Legacy rows not currently resolved/recoverable by local V2/Yahoo evidence | 90,331 |
| V2 same-fiscal-label groups with multiple SimFin Report Date values | 308 |
| V2 rows in those groups | 638 |
| Multi-report-date groups auto-importable by R1 heuristic | 221 |
| Multi-report-date groups requiring review/block | 87 |
| Active V3 operational universe | 2,812 |
| Active V3 with sufficient fiscal identity for current readiness projection | 2,451 |
| Not yet projectable admitted Legacy-only companies | 361 |
| Latest-Q core ready in projectable active V3 population | 1,696 / 2,451, or 69.20% |

Field coverage after local Yahoo + Legacy + V2 on the strict V2 fiscal-identity projection
denominator. This is source-projection evidence, not the initial V3 universe denominator:

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

Current 8Q completeness from V2 projection. These counts describe the V2 source/profile population,
not the locked initial V3 company universe:

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
| `FISCAL_IDENTITY_RESOLVED` | 55,231 | Approved Legacy rows date-matched to V2 explicit fiscal identity or local provider observation fiscal labels. |
| `FISCAL_IDENTITY_RECOVERABLE` | 2,471 | Approved Legacy rows date-matched to local Yahoo cache; live Yahoo may improve this materially. |
| `DATE_ONLY_UNRESOLVED` | 90,331 | Approved Legacy rows with no current local V2/Yahoo fiscal identity evidence. |
| `EXCLUDED` | 0 | Historical old-floor projection excluded earlier rows before this classification. Current V3 historical floor is 2018-01-01. |

Legacy-only fiscal identity recovery projection:

| Classification | Rows | Basis |
| --- | ---: | --- |
| `FISCAL_IDENTITY_RESOLVED` | 123 | Local provider observation content has reported fiscal labels for the same ticker/period-end row. |
| `FISCAL_IDENTITY_RECOVERABLE` | 1,775 | Date-matched to local Yahoo cache; future live Yahoo bootstrap may supply usable labels/values. |
| `DATE_ONLY_UNRESOLVED` | 12,221 | No current local fiscal-label evidence. |

Current inability to resolve all Legacy-only history is not a membership exclusion reason. Future
live Yahoo historical bootstrap is expected to be especially useful for Legacy-only V3 tickers
because it may provide fiscal-year labels, fiscal-quarter labels, historical period dates, and
canonical field values.

The strict V2 fiscal-identity denominator is a local source-projection denominator, not the initial
V3 universe. Reported fiscal identities from Yahoo, V2, Legacy recovery, validated company
calendars, or period-end continuity may help create quarter rows for already admitted companies.
They must not admit new companies into the initial V3 universe.

Population denominators must remain explicit:

| Population | Count | Definition |
| --- | ---: | --- |
| Approved initial V3 company universe | 2,812 | Legacy tickers minus positive BANK, INSURANCE, ETF/fund, non-company, and genuine review exclusions. This is the V3 company denominator. |
| Legacy fundamentals universe before exclusions | 2,936 | Distinct Legacy tickers in `fundamentals_usa.db.rc_fundamental_quarterly`. |
| Approved V3 company universe | 2,812 | Historical company membership denominator. Post-B price-activity triage does not remove companies from this universe. |
| Active V3 operational universe | 2,735 | Post-B evidence-based maintenance denominator: approved V3 companies with a valid `osakedata` USA price observation within T0..T-4, where T0 is the latest broad USA equity trading session observed in RawCandle `osakedata`. |
| Active V3 with sufficient fiscal identity for current projection | 2,451 | Active approved companies with V2 `ORDINARY` fiscal-identity evidence available today. |
| Active V3 not yet projectable | 361 | Admitted Legacy-only companies whose latest/current fiscal-Q readiness is not yet reliably projectable from local evidence. |
| V2 source/profile population | 4,613 | V2 companies with ticker; useful as source/profile evidence, not V3 universe authority. |
| V2 ORDINARY source/profile population | 4,323 | V2 `ORDINARY` companies with ticker; useful as source/projection evidence, not V3 universe authority. |
| V2 active operational population | 2,575 | V2 active companies eligible for current Check/Update; not V3 universe authority. |

Readiness by denominator:

| Metric | Numerator | Denominator | Coverage |
| --- | ---: | ---: | ---: |
| latest-Q core ready, V2 ORDINARY source/profile population | 1,696 | 4,323 | 39.23% |
| latest-Q core ready, active V3 with sufficient fiscal identity | 1,696 | 2,451 | 69.20% |
| waiting for latest-Q core, active V3 with sufficient fiscal identity | 755 | 2,451 | 30.80% |
| active V3 not yet projectable for latest-Q readiness | 361 | 2,812 | 12.84% |
| active V3 with sufficient fiscal identity 4Q revenue complete | 2,143 | 2,451 | 87.43% |
| active V3 with sufficient fiscal identity 4Q EBITDA complete | 2,166 | 2,451 | 88.37% |
| active V3 with sufficient fiscal identity 4Q FCF complete | 2,076 | 2,451 | 84.70% |
| active V3 with sufficient fiscal identity leverage inputs complete | 1,976 | 2,451 | 80.62% |
| active V3 with sufficient fiscal identity 8Q revenue complete | 2,066 | 2,451 | 84.29% |
| active V3 with sufficient fiscal identity 8Q EBITDA complete | 2,110 | 2,451 | 86.09% |
| active V3 with sufficient fiscal identity 8Q FCF complete | 1,962 | 2,451 | 80.05% |
| active V3 with sufficient fiscal identity 8Q shares/dilution complete | 953 | 2,451 | 38.88% |
| active V3 with sufficient fiscal identity TTM-ready proxy | 1,627 | 2,451 | 66.38% |
| active V3 with sufficient fiscal identity SCORE-ready proxy | 575 | 2,451 | 23.46% |
| active V3 with sufficient fiscal identity valuation-ready proxy | 1,696 | 2,451 | 69.20% |

For readiness projection, the 361 admitted Legacy-only companies are `NOT_YET_PROJECTABLE`, not
`NOT_READY_WITH_KNOWN_DATA`. They should not be penalized until Yahoo bootstrap, provider fiscal
labels, validated calendars, or other reliable local evidence produce reported fiscal-Q identity.

Calendar comparison derivability is separate from fiscal-identity derivability:

| Source | Fiscal identity derivability | Approximate calendar-comparison derivability |
| --- | --- | --- |
| Legacy | Legacy primary quarterly rows do not reliably expose reported fiscal year/quarter; `period_end_date` alone must not create canonical fiscal identity. | `156,094 / 156,094` total Legacy rows and `91,306` rows at or above the current 2018-01-01 V3 historical floor have valid `period_end_date` for the approved approximate method. |
| V2 | V2 has explicit fiscal labels and SimFin `Report Date` as period-end-like evidence. | `85,424 / 85,424` rows have valid `report_date` usable for the approved approximate method after acceptance as period-end evidence. |

Calendar comparison method and quality:

- method values: `APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END`, `ACTUAL_PERIOD_RANGE`
- quality values: `APPROX_OVERLAP`, `ACTUAL_RANGE_OVERLAP`, `AMBIGUOUS`,
  `INSUFFICIENT_DATES`, `IRREGULAR_ACTUAL_PERIOD`

The approximate method must not claim exact actual fiscal-period evidence.

Detailed R1 artifacts are under `temp/fundamentals_v3_design_r1/20260820_214554/`.

## SEC and Assurance

SEC can be both provider evidence and assurance. Keep assurance separate from Q lifecycle. Minimal
quarter-level assurance states:

- `NOT_APPLICABLE`
- `NOT_YET_EXPECTED`
- `PENDING`
- `CHECKED_NO_EVIDENCE`
- `PARTIAL_EVIDENCE`
- `CONFIRMED`
- `UNSUPPORTED`
- `ERROR_RETRY`

SEC confirmation should not be required for operational settlement, core readiness, TTM readiness,
or score readiness unless a field-specific policy explicitly requires SEC evidence. Due checks,
`next_sec_check_at`, and retry timing are action/scheduling metadata, not assurance-state names.

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

Minimum `v3_operational_action` contract:

- `action_id`
- `action_type`
- `company_id`
- nullable `quarter_id` for calendar/company-level actions
- nullable `provider`
- `due_at_utc`
- `status`
- `attempt_count`
- `last_error`
- `created_at_utc`
- `updated_at_utc`

Prevent duplicate active instances of the same semantic action. Do not encode Q readiness into
action status.

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

`OPERATIONAL_ACTION=MANUAL_REVIEW` may coexist with `q_lifecycle=OPERATIONALLY_SETTLED` when no
automatic work remains, or with `q_lifecycle=ENRICHING` when other automatic work is still due.
`DUE_ACTIONS` is a set, so provider enrichment, SEC check, backfill, and manual review can coexist.

## Event History

Persist compact events for run reporting:

- `RESULT_DETECTED`
- `Q_CREATED`
- `INITIAL_DATA_ACQUIRED`
- `Q_ENRICHED`
- `CORE_READINESS_CHANGED`
- `SEC_CONFIRMATION_RECEIVED`
- `SCORE_READINESS_CHANGED`
- `PROVIDER_FAILED`
- `RESOLUTION_ISSUE_CREATED`
- `Q_REOPENED`

This is not event sourcing. Canonical tables remain authoritative snapshots.

Minimum `v3_event` contract:

- `event_id`
- `event_type`
- `event_at_utc`
- `run_id`
- `company_id`
- nullable `quarter_id`
- compact details JSON with before/after values for readiness transitions

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

The design phase is closed for Phase 1 implementation. The following items are validation or
future-extension work, not architecture blockers for schema/repository/workflow coding:

- Live Yahoo historical depth, endpoint limits, and reported fiscal-label availability require a
  later network-authorized validation.
- Whether actual period-start dates from a future reliable source should supersede
  `APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END`.
- Whether calendar comparison fields should become persisted after a concrete query/UI consumer
  requires it. Phase 1 implements the calculation as a derived helper.
- Exact field-specific precedence after comparing `Yahoo -> Legacy -> V2` with
  `Yahoo -> V2 -> Legacy`; the implementation-readiness contract locks the default seed/fill order
  while this remains a read-only validation item.
- How many of the 87 review/block multi-Report-Date groups can be resolved by adjacent-period
  fiscal-calendar validation.
- Whether the compact event catalogue needs extension after real run observability needs are seen.

## Implementation Phases

1. Derive approved V3 company universe from Legacy fundamentals membership.
2. Apply positive BANK, INSURANCE, ETF/fund, non-company, and genuine review exclusions.
3. Build V3 schema/repository layer and create V3 companies for all approved Legacy-derived tickers.
4. Yahoo bootstrap all approved V3 tickers, including Legacy-only tickers absent from V2.
5. Recover reported fiscal identities and period metadata from Yahoo, V2, Legacy evidence, and
   validated local calendars where available.
6. Reconcile Legacy/V2 data where available; do not make V2 profile membership a prerequisite for
   company creation.
7. Provider acquisition model plus V3 Check.
8. V3 Update, canonicalization, resolution issues, and retry actions.
9. TTM, EBITDA score, valuation, and readiness rebuilds.
10. Production validation and shadow comparison.
11. Config-based cutover to V3.
12. Legacy/V2 archive freeze and cleanup of obsolete dual-store orchestration.

## Validation

- One eventual production DB: yes, `rc_fundamentals_v3.db`.
- Implementation-readiness contract exists: yes,
  `docs/fundamentals_v3_implementation_readiness_contract.md`.
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
- Phase 1 can proceed without new material architecture decisions: yes.
