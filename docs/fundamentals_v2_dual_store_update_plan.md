# Fundamentals V2 Dual-Store Update Plan

## 1. Purpose

This plan defines the 9H2 implementation shape for incremental USA quarterly fundamentals updates while SwingMaster still operates both the legacy fundamentals store and `rc_fundamentals_v2.db`.

The target is one executable Check/plan work-list, followed by Update Fundamentals executing independent legacy and V2 component decisions for each selected company and fiscal quarter. The plan is intentionally implementation-oriented, but it does not authorize provider calls, production writes, schema changes, RawCandle changes, or the actual 9H2 implementation.

## 2. Current problem: legacy and V2 are not synchronized

Legacy and V2 are not synchronized stores. They have different schemas, different identity keys, different provenance models, and different readiness semantics. During transition, downstream workflows still depend on legacy tables, while V2 is the canonical target being built in parallel.

The practical issue is that a work unit can require a legacy update, a V2 update, both, or neither. A legacy row can exist while V2 has no matching canonical quarter. V2 can have a canonical quarter and some fields while the legacy lifecycle still says `FUNDAMENTALS_PARTIAL`. Numeric differences between the stores are not automatically errors in 9H2 because reconciliation and bridge parity are later phases.

9H2 must therefore compute per-store state before applying work. It must not infer one store's currentness from the other.

## 3. Existing Check/plan contract

9H1 Check for Updates emits `fundamental_result_check_plan_v2`. A plan is executable only when `check_status` is `SUCCESS`, the candidate count and candidate hash validate, the decision date matches execution, and every row has an executable decision.

The current executable decisions are:

- `FETCH_NEW_QUARTER`
- `RETRY_PARTIAL_QUARTER`
- `RETRY_FETCH_FAILED`
- `REFRESH_SEC_CONFIRMATION`

Each work unit is selected by the result lifecycle and contains `market`, `ticker`, `target_period_end_date`, `canonical_fiscal_year`, `canonical_fiscal_quarter`, `canonical_report_date`, provider-due metadata, and a deterministic `work_unit_key` such as `usa:AAMI:2026:Q2`.

Check owns discovery and due selection. It does not canonicalize V2 financial facts.

## 4. Store ownership

SwingMaster owns result-check selection, field policies, provider adapters, legacy update behavior, V2 canonicalization, provenance, and status semantics.

Legacy operational rows live in `fundamentals_usa.db`, primarily `rc_fundamental_quarterly` and `rc_fundamental_quarter_ingestion_status`.

V2 canonical rows live in `rc_fundamentals_v2.db`, primarily `rc_v2_company`, `rc_v2_quarter`, `rc_v2_fundamental_quarterly`, and `rc_v2_fundamental_field_source`.

Provider raw/cache/fetch-state is owned by provider-specific SwingMaster adapters. A provider observation can be shared as evidence, but legacy and V2 still make separate canonicalization decisions.

RawCandle owns scheduling, process orchestration, logging, and supervision. It must not duplicate candidate eligibility, field rules, provider hierarchy, or canonical write logic.

## 5. LegacyState definition

`LegacyState` is computed for one selected work unit from `(market, ticker, target_period_end_date)`.

Recommended fields:

- `row_present`
- `ingestion_status`
- `quarter_basic_complete`
- `ttm_input_complete`
- `score_history_complete`
- `source_confirmation_status`
- `retry_recommendation`
- `missing_basic_fields`
- `last_fetch_status`
- `legacy_update_required`
- `legacy_blocker`

The primary sources are `rc_fundamental_quarterly`, `rc_fundamental_quarter_ingestion_status`, and the Check candidate decision.

## 6. V2State definition

`V2State` is computed for one selected work unit from `(market, ticker, canonical_fiscal_year, canonical_fiscal_quarter, canonical_report_date)`.

Recommended fields:

- `company_present`
- `company_id`
- `company_profile`
- `quarter_present`
- `quarter_id`
- `fundamental_row_present`
- field presence for `revenue`, `ebitda`, `free_cashflow`, `shares_outstanding`, `cash`, `total_debt`, `operating_cashflow`, `capex`, and `ebit`
- `p0_complete`
- `p0p1_complete`
- `p1_complete`
- `provenance_count`
- provider cache/fetch-state summaries relevant to the selected ticker
- `v2_update_required`
- `v2_blocker`

The primary sources are `rc_v2_company`, `rc_v2_quarter`, `rc_v2_fundamental_quarterly`, `rc_v2_fundamental_field_source`, SimFin API raw/fetch-state tables, and validated field-specific legacy/Yahoo/SEC observations.

## 7. Meaning of "current" per store

Legacy current means the selected target period is operationally handled for legacy downstream use: the target row exists where required, managed ingestion status is complete/current, retry is not due, and SEC/source-confirmation retry state does not require follow-up.

V2 current means the selected canonical company and fiscal quarter has the required V2 structures and is complete enough under V2 field policy for the current Update objective. For 9H2, this is primarily work-unit-scoped presence and NULL-fill eligibility for P0/P1 fields, not broad historical readiness.

Current does not mean the two stores are numerically equal.

## 8. Cross-store operational state

For each selected work unit, preflight should classify one of these operational states:

- `LEGACY_AND_V2_REQUIRED`
- `LEGACY_ONLY_REQUIRED`
- `V2_ONLY_REQUIRED`
- `NOOP_BOTH_CURRENT`
- `BLOCKED_UNSAFE_IDENTITY`
- `BLOCKED_POLICY_UNSUPPORTED`

This classification drives component execution and reporting. It does not replace existing lifecycle statuses.

## 9. Why sync != numeric equality

Legacy and V2 values can differ because of different provider timing, fiscal identity rules, validated fallback tiers, NULL-fill history, and canonical provenance rules. In 9H2, numeric mismatch is a diagnostic signal, not an automatic write trigger.

Replacing V2 non-null canonical values because legacy differs would violate the V2 NULL-fill and conflict policy. Replacing legacy values because V2 differs would change downstream behavior before Phase 11. Reconciliation belongs to later bridge/parity phases, not incremental 9H2 update execution.

## 10. Preflight algorithm

Preflight should run before any provider financial acquisition or canonical write:

1. Validate the Check plan as `fundamental_result_check_plan_v2`.
2. Normalize every work unit to `market`, `ticker`, `target_period_end_date`, canonical fiscal year, fiscal quarter, and report date.
3. Load `LegacyState` read-only.
4. Load `V2State` read-only.
5. Resolve identity blockers before provider calls.
6. Compute `legacy_update_required`.
7. Compute `v2_update_required`.
8. Emit dry-run/preflight artifacts with per-work-unit and aggregate state.
9. Execute only components whose required flag is true, unless the user requested dry-run.

Preflight must be deterministic and replayable. It should not call providers.

## 11. `legacy_update_required`

`legacy_update_required` should be true when the Check plan selected an executable legacy-relevant decision and the legacy side is not current for the selected target period.

It is true for missing target rows, `FUNDAMENTALS_PARTIAL`, `FETCH_FAILED`, `PUBLISHED_DATA_NOT_FETCHED`, incomplete `quarter_basic_complete`, or source-confirmation retry states that the plan selected for follow-up.

It is false when the target period is already complete/current and no retry/source-confirmation action is due. It can also be false for a future V2-only operational repair, but that repair type should be explicit and not inferred from arbitrary V2 gaps.

The flag must be based on selected work-unit scope, not a broad scan of legacy history.

## 12. `v2_update_required`

`v2_update_required` should be true when the selected work unit lacks required V2 structure or selected V2 fields are missing and eligible for current-quarter provider/cache evaluation under validated rules.

It is true when `rc_v2_company` or a deterministic `rc_v2_quarter` is missing and can be safely created, when the V2 fundamental row is missing, or when current-quarter fields remain NULL for applicable policy:

- P0: `revenue`, `ebitda`, `free_cashflow`
- P0/P1: `shares_outstanding`
- P1: `cash`, `total_debt`
- supporting: `operating_cashflow`, `capex`
- secondary/fallback: `ebit`

It is false when V2 is already current enough for the selected work-unit objective or when a missing field is not eligible under current provider policy. Unsupported eligibility should be represented as a blocker or no-op reason, not a silent broad repair.

## 13. Decision matrix

| Legacy state | V2 state | Legacy action | V2 action | Overall handling |
| --- | --- | --- | --- | --- |
| Not current | Not current | Attempt | Attempt | `SUCCESS` only if both required components succeed or no-op after preflight |
| Current | Not current | No-op | Attempt | V2-only execution with legacy no-op |
| Not current | Current | Attempt | No-op | Legacy-only execution with V2 no-op |
| Current | Current | No-op | No-op | No-op success |
| Identity ambiguous | Any | Block | Block unless independently safe | Failed or partial with explicit blocker |
| Unsupported field policy | Current or partial | No-op or block | Block unsupported V2 fields | Do not broaden provider scope |

Component failures must not be hidden. If one component commits and another required component fails, the work unit is partial and retry-visible.

## 14. Provider acquisition sharing

A provider payload can be acquired once for a selected ticker when policy allows, then interpreted separately by legacy and V2 adapters.

Yahoo can support discovery/status and validated field-specific fallback rules. SEC can support filing confirmation and only validated fact paths, currently SEC Revenue under its SAFE_SCOPED rule. SimFin statements and shares APIs are V2-oriented operational sources; they should be selected-ticker, cache-aware, rate-limited, and never invoked as broad historical apply runners inside 9H2.

Provider acquisition scope may be wider than one quarter because an API can return multiple periods. Canonical apply scope remains the selected company and fiscal quarter.

## 15. Work-unit canonical scope

The canonical write unit is exactly one company and one canonical fiscal quarter. For 9H1 plans this is represented by `market:ticker:fiscal_year:fiscal_quarter`, with report date retained as identity evidence.

If a provider returns multiple quarters, the adapter must filter before V2 canonical apply. V2 canonical financial values, canonical provenance, completeness decisions, and conflict decisions that affect readiness must only apply to the selected work unit.

## 16. Quarter creation

Legacy quarter creation follows the existing legacy Update path and keys rows by `(ticker, period_end_date)`.

V2 quarter creation is allowed only when company identity exists or can be deterministically resolved, fiscal year and fiscal quarter are known, and report date is deterministic enough for `rc_v2_quarter`. If report date or fiscal identity is ambiguous, the V2 component must block rather than invent a quarter.

Creating `rc_v2_quarter` and `rc_v2_fundamental_quarterly` for the selected work unit is in scope for 9H2 implementation. Creating unrelated historical quarters is not.

## 17. Historical-scope boundary

9H2 is not a historical backfill phase. It must not run existing broad backfill runners wholesale from Update Fundamentals.

Existing validated historical modules can contribute reusable helper logic, but the helper must evaluate one company and one canonical quarter. If the current code only exposes runner-shaped functions, 9H2 should extract work-unit helpers and have historical runners call those helpers later where practical.

## 18. Component result model

Each work unit should emit:

- `legacy_attempted`
- `legacy_status`
- `legacy_writes`
- `legacy_errors`
- `v2_attempted`
- `v2_status`
- `v2_writes`
- `v2_provenance_writes`
- `v2_retry_required`
- `v2_errors`
- `overall_status`

Recommended statuses are `SUCCESS`, `NOOP`, `PARTIAL`, `FAILED`, and `BLOCKED`. `NOOP` is successful when preflight determined no component action was required.

## 19. Mixed failure/retry behavior

Legacy and V2 updates are not one cross-database ACID transaction during transition. A successful legacy component should not be rolled back solely because a V2 shadow component failed. A successful V2 component should not hide a legacy failure while downstream still depends on legacy.

If any required component fails after another required component succeeds or no-ops, the work unit should be reported as partial and retry-required. If no required component produced a usable committed result, the work unit should be failed.

RawCandle must receive a deterministic process contract. Until RawCandle understands richer partial summaries, scheduler-visible retry cases must not be mapped to ordinary full success.

## 20. Idempotency/replay

Replay with unchanged inputs must produce:

- zero duplicate legacy work records
- zero duplicate V2 canonical financial deltas
- zero duplicate V2 canonical provenance deltas
- zero duplicate timing-equivalent observations
- stable component result summaries

V2 field provenance should use idempotent keys and `INSERT OR IGNORE`-style behavior only after the canonical field write is accepted. Canonical provenance must not be created for rejected, conflicting, or merely observed provider values.

## 21. Dry-run/preflight output

Dry-run should emit both aggregate and per-work-unit preflight output:

- plan path, candidate count, candidate hash
- provider call count, expected to be zero for preflight
- legacy state counts
- V2 state counts
- cross-store buckets
- required-action counts
- blocker counts
- selected provider policies that would be due during apply
- explicit non-goal confirmations

The read-only 9H2-P measurement against the latest SUCCESS plan found 44 executable work units: 5 `FETCH_NEW_QUARTER` and 39 `RETRY_PARTIAL_QUARTER`. Legacy target rows were present for 44/44, but 0/44 were `quarter_basic_complete`. V2 exact quarter rows were present for 25/44, V2 P0 complete for 20/44, V2 P0/P1 complete for 5/44, and V2 cash/debt P1 complete for 18/44.

## 22. UI contract

The UI should call SwingMaster backend semantics and display the resulting component summaries. It should not compute provider eligibility or field-level canonicalization rules.

The UI should be able to show:

- total work units
- legacy attempted/succeeded/no-op/failed
- V2 attempted/succeeded/no-op/failed
- partial retry-required rows
- blocked rows and blocker reasons
- provider calls made during apply
- artifact paths

The UI must not present mixed component failure as ordinary full success.

## 23. RawCandle contract

RawCandle should invoke the SwingMaster Check and Update commands, preserve logs, and interpret stable process status and summary fields. It must not duplicate Check eligibility, provider due rules, legacy/V2 preflight rules, or V2 field policies.

If 9H2 adds richer summary keys, RawCandle changes should be limited to parsing and displaying those keys. Business semantics remain in SwingMaster.

## 24. Persistent-state decision

Recommendation: `HYBRID_COMPUTED_PLUS_RUN_METADATA`.

The authoritative required-action decision should be computed at Update preflight from the current plan, legacy state, V2 state, provider cache/fetch-state, and validated policies. Persist run/component outcomes and retry metadata as evidence. Do not introduce a durable cross-store sync truth table in 9H2.

A sync table would create another authority before bridge parity and cutover rules exist. Computed preflight keeps the decision close to actual current state and avoids stale sync flags.

## 25. Current operational population measurements

Read-only measurement used:

- plan: `temp/rc_fundamentals_v2_phase9h1vr_full_check/20260815T125450Z/full_check/plan.json`
- check status: `SUCCESS`
- candidate hash: `85e68238fe8b51979a924033e074d0ab003b8dc8e42f634b3ba6f9829b7970ab`
- provider calls: 0
- production writes: 0

Measured counts:

| Metric | Count |
| --- | ---: |
| executable work units | 44 |
| `FETCH_NEW_QUARTER` | 5 |
| `RETRY_PARTIAL_QUARTER` | 39 |
| legacy rows present | 44 |
| legacy `quarter_basic_complete` | 0 |
| V2 companies present | 38 |
| V2 exact quarter rows present | 25 |
| V2 fundamental rows present | 25 |
| V2 P0 complete | 20 |
| V2 P0/P1 complete | 5 |
| V2 P1 cash/debt complete | 18 |
| preflight legacy update required | 44 |
| preflight V2 update required | 42 |

V2 field presence among the 44 work units:

| Field | Present |
| --- | ---: |
| revenue | 23 |
| ebitda | 20 |
| free_cashflow | 24 |
| shares_outstanding | 8 |
| cash | 24 |
| total_debt | 19 |
| operating_cashflow | 23 |
| capex | 15 |
| ebit | 20 |

This population confirms that dual-store preflight is required. The current executable plan is not cleanly legacy-only or V2-only.

## 26. Proposed code/helper structure

Recommended new or refactored SwingMaster helpers:

- `swingmaster/fundamentals/dual_store_update_preflight.py`
- `swingmaster/fundamentals/dual_store_update_result.py`
- `swingmaster/fundamentals_v2/work_unit_apply.py`
- field-specific V2 adapter helpers under `swingmaster/fundamentals_v2/`
- narrow CLI integration in `swingmaster/cli/run_fundamental_quarter_update.py`

`dual_store_update_preflight.py` should own read-only state loading and required-action computation. `work_unit_apply.py` should own V2 selected-quarter creation, provider/cache evaluation, NULL-fill canonicalization, provenance, and V2 component summaries.

Existing historical modules should expose reusable rule helpers before 9H2 calls them.

## 27. Test strategy

Required tests:

- plan validation still rejects non-success, stale, hash-mismatched, inactive, or non-executable plans
- preflight classifies legacy-only, V2-only, both-required, both-no-op, and blocked identity cases
- V2 canonical apply is selected-work-unit scoped even when provider payload includes other periods
- canonical non-null V2 fields are not overwritten
- provenance is inserted only for accepted canonical writes
- replay produces zero financial and provenance delta
- mixed legacy/V2 component failure reports partial or failed, not ordinary full success
- UI and scheduler use the same SwingMaster backend semantics
- RawCandle parsing, if changed, remains orchestration-only

Tests should prefer fixtures and local provider-cache payloads. Network/provider calls are not needed for preflight tests.

## 28. Production rollout gates

Before production enablement:

1. Read-only preflight must match the validated Check plan scope.
2. Dry-run must show zero provider calls when provider acquisition is disabled.
3. V2 apply must prove selected-work-unit scope.
4. Replay must show zero canonical financial delta and zero provenance delta.
5. Mixed failure behavior must be visible in CLI summaries and UI.
6. Final DB integrity checks must pass for both stores.
7. RawCandle behavior must be validated if CLI process status or summary parsing changes.

## 29. Explicit non-goals

9H2 should not implement:

- broad historical backfill
- provider calls during Check for V2 financial enrichment
- schema changes unless a later implementation phase explicitly approves them
- RawCandle business logic
- downstream bridge/cutover
- numeric reconciliation between legacy and V2
- SEC shares, operating cashflow, or capex semantics beyond already validated paths
- overwrites of existing V2 canonical non-null values
- writes outside selected work-unit scope

## 30. Open questions

Open questions for review:

- Should `v2_update_required` initially target all listed fields or only P0/P0-P1 fields for the first production 9H2 rollout?
- Should V2-only repair work units ever be generated by Check before Phase 11, or only as an explicit maintenance/debug mode?
- What exact CLI exit-code mapping should represent partial mixed component failure before RawCandle gains richer parsing?
- Should bank/insurance profiles be excluded from common industrial V2 field adapters in 9H2, or routed to specialized tables immediately?
- Which existing Yahoo fallback helpers are clean enough to extract before 9H2, and which need wrapper adapters first?

## 31. Recommended implementation decomposition

Recommended next implementation sequence:

1. Add the read-only dual-store preflight helper and tests.
2. Wire preflight into Update Fundamentals dry-run output only.
3. Add component result dataclasses and summary serialization.
4. Add V2 selected-quarter creation helper with identity blockers.
5. Extract/adapt one low-risk V2 field/provider helper into work-unit scope.
6. Prove replay and provenance idempotency on fixtures.
7. Expand to the remaining approved fields.
8. Add UI display of component summaries.
9. Validate RawCandle scheduler behavior and adjust parsing only if required.
10. Run production dry-run and replay gates before any production apply.

Final 9H2-P classification: `PHASE_9H2_P_DUAL_STORE_PLAN_READY_FOR_REVIEW`.
