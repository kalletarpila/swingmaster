# Fundamentals V2 Dual-Store Update Plan

## 1. Purpose

This plan defines the 9H2 implementation shape for incremental USA quarterly fundamentals updates while SwingMaster still operates both the legacy fundamentals store and `rc_fundamentals_v2.db`.

The simplified target is watermark-first. For each ticker, Update Fundamentals should determine the latest operational target quarter from the existing Check plan, compute independent legacy and V2 watermarks, decide what each store needs, then merge only required component actions into company + canonical fiscal-quarter execution units.

This is a design document. It does not authorize provider calls, production writes, schema changes, RawCandle changes, or the actual 9H2 implementation.

## 2. Current Problem

Legacy and V2 are not synchronized stores. They have different schemas, identity keys, provenance models, and readiness semantics. During transition, downstream workflows still depend on legacy tables, while V2 is maintained in parallel.

The initial dual-store plan correctly avoided numeric reconciliation, but its decision framing could be read as a cross-store sync model. 9H2 should not build a heavy temporary synchronization framework. The authoritative question is not "are legacy and V2 synchronized?" The authoritative question is "for the latest operational quarter selected by Check, is this store current enough under its own rules?"

Cross-store categories remain useful reporting summaries only.

## 3. Existing Check/Plan Contract

9H1 Check for Updates emits `fundamental_result_check_plan_v2`. A plan is executable only when `check_status` is `SUCCESS`, the candidate count and candidate hash validate, the decision date matches execution, and every row has an executable decision.

The current executable decisions are:

- `FETCH_NEW_QUARTER`
- `RETRY_PARTIAL_QUARTER`
- `RETRY_FETCH_FAILED`
- `REFRESH_SEC_CONFIRMATION`

Each work unit contains `market`, `ticker`, `target_period_end_date`, `canonical_fiscal_year`, `canonical_fiscal_quarter`, `canonical_report_date`, provider-due metadata, and deterministic `work_unit_key`.

Check remains the one operational workflow and produces one merged `fundamental_result_check_plan_v2`. It can have multiple internal selection sources:

- Source A: existing Legacy/result lifecycle candidates, such as `FETCH_NEW_QUARTER`, `RETRY_PARTIAL_QUARTER`, `RETRY_FETCH_FAILED`, and `REFRESH_SEC_CONFIRMATION`.
- Source B: due persisted V2 operational follow-up candidates from previously selected work units.

Source B is not result discovery. It reselects already-known company + quarter work units that 9H2 explicitly left with V2 operational follow-up due. There is no second V2 Check command, second V2 discovery queue, or separate scheduler path.

Overall result-check status remains distinct from quarter lifecycle status. An overall result-check `PARTIAL` is a run-level outcome and is non-executable under the current plan contract. A quarter lifecycle state such as `FUNDAMENTALS_PARTIAL` describes one company fiscal quarter and may appear as a follow-up work unit inside an overall `SUCCESS` Check plan.

## 4. Watermark Model

Operational scope: fiscal quarters greater than or equal to `2025 Q1`.

```text
                         latest operational target quarter
                                      |
                       +--------------+--------------+
                       |                             |
                    Legacy                          V2
                       |                             |
          latest_present_quarter          latest_present_quarter
          latest_complete_quarter         latest_core_complete_quarter
                       |                             |
                legacy planner                  V2 planner
                       |                             |
                       +--------------+--------------+
                                      |
                              merged work unit
```

The latest operational target quarter normally comes from the existing `fundamental_result_check_plan_v2` work units. For an Update run, this may simply be the latest selected plan quarter for that ticker. This differs from the latest quarter already present in legacy, the latest quarter already present in V2, and the latest future calendar estimate.

The plan must not invent persistent watermark columns or a durable cross-store sync state machine in initial 9H2. Compute watermarks from DB truth at preflight and persist only run/component metadata.

Watermarks are derived summaries. They do not replace legacy lifecycle/status semantics and they do not create a V2 clone of the legacy lifecycle.

Persisted V2 follow-up metadata is compatible with this model. It preserves operational follow-up intent and due timing between runs, but it does not become canonical financial truth, a persistent watermark, or a cross-store sync authority.

## 5. Store Ownership

SwingMaster owns result-check selection, field policies, provider adapters, legacy update behavior, V2 canonicalization, provenance, status semantics, UI backend actions, and scheduler-facing command contracts.

Legacy operational rows live in `fundamentals_usa.db`, primarily `rc_fundamental_quarterly` and `rc_fundamental_quarter_ingestion_status`.

V2 canonical rows live in `rc_fundamentals_v2.db`, primarily `rc_v2_company`, `rc_v2_quarter`, `rc_v2_fundamental_quarterly`, and `rc_v2_fundamental_field_source`.

RawCandle owns scheduling, process orchestration, logging, and supervision. It must call SwingMaster contracts and must not duplicate candidate eligibility, provider hierarchy, field rules, watermark logic, or canonical write rules.

## 6. Operational Scope Floor

Normal 9H2 operational planning starts at `2025 Q1`.

9H2 must not:

- plan pre-2025 Q1 catch-up
- lower a watermark because an older quarter is missing
- scan pre-2025 Q1 continuity
- call providers solely for old gaps
- become a historical convergence engine

If a selected target quarter is earlier than `2025 Q1`, mark it `OUT_OF_OPERATIONAL_SCOPE` and take no normal 9H2 action. Fiscal year and fiscal quarter are the primary guard. `report_date` should also be sanity-checked because a contradictory date can signal identity corruption.

Persisted V2 follow-up selection must also respect this floor. Pre-2025 Q1 records cannot re-enter normal Check through the V2 follow-up source.

## 7. LegacyState

`LegacyState` is computed for one ticker and the selected target quarter using legacy tables only.

Recommended fields:

- `latest_present_quarter`
- `latest_operationally_complete_quarter`
- `latest_sec_confirmed_quarter`, if useful
- `latest_retry_pending_quarter`, if useful
- target row present
- target ingestion status
- target `quarter_basic_complete`
- target source-confirmation status
- target retry recommendation
- target missing basic fields
- `legacy_action`
- `legacy_blocker`

Legacy state must not be redefined using V2 fields.

## 8. Legacy Watermarks

`legacy_latest_present_quarter` is the latest in-scope fiscal quarter where legacy has a meaningful quarterly result row. A row is meaningful only if the target identity is valid and at least one financial value required by legacy operation is non-null/non-empty. An empty shell row must not advance this watermark.

`legacy_latest_operationally_complete_quarter` is the latest in-scope fiscal quarter considered handled under existing legacy semantics. It should use the current lifecycle concepts: row exists, `quarter_basic_complete`, complete ingestion statuses such as `QUARTER_BASIC_COMPLETE` or `INGEST_COMPLETE`, retry recommendation, source confirmation or SEC retry state, and current Check decision.

`quarter_basic_complete` remains a strong legacy completeness signal. It is not the V2 first-rollout CORE signal.

## 9. Why `quarter_basic_complete = 0/44`

The previous 44-row measurement found legacy rows present for 44/44 but `quarter_basic_complete = 0/44`.

This is expected for the active SUCCESS plan population. The plan intentionally contains 5 `FETCH_NEW_QUARTER` rows and 39 `RETRY_PARTIAL_QUARTER` rows. Current legacy result-check logic selects target quarters for retry when `quarter_basic_complete` is false.

The legacy `quarter_basic_complete` assessment is strict. It requires revenue, profitability, free cash flow or OCF plus capex, cash, total debt, shares, valid identity/date, and meaningful financial values. Therefore a row can be present but still operationally partial.

Read-only watermark remeasurement found zero rows that were actually legacy-current despite `quarter_basic_complete=0`. Conclusion: `quarter_basic_complete` remains useful for legacy lifecycle completeness, but preflight should also consult ingestion status, retry recommendation, source-confirmation state, and row meaningfulness to avoid false decisions in other populations.

## 10. V2State

`V2State` is computed for one ticker and selected canonical fiscal quarter using V2 tables and provider cache/fetch-state.

Recommended fields:

- `company_present`
- `company_id`
- `company_profile`
- `latest_structure_quarter`
- `latest_present_quarter`
- `latest_core_complete_quarter`
- target `quarter_id`
- target fundamental row present
- target CORE field presence
- target opportunistic field presence
- provider cache/fetch-state
- `v2_action`
- `v2_blocker`
- persisted follow-up metadata, if active

V2 state is work-unit scoped for update decisions. It must not scan older history to create normal operational work.

The same work unit may have Legacy lifecycle current and V2 follow-up metadata with `retry_required=true`. This is valid and intentional during the transition.

## 11. V2 Watermarks

`v2_latest_present_quarter` is the latest in-scope canonical fiscal quarter where V2 has a meaningful canonical quarter/fundamental row. An empty quarter shell should not advance this watermark. If needed, report `v2_latest_structure_quarter` separately.

`v2_latest_core_complete_quarter` is the latest in-scope quarter where V2 first-rollout CORE is complete.

For initial 9H2 rollout, V2 CORE is:

- `revenue`
- `ebitda`
- `free_cashflow`
- `shares_outstanding`

`core_complete` is a data completeness metric. It is true only when all first-rollout CORE fields are present.

`core_update_required` is an operational due metric. It is true only when the quarter is in scope, identity/profile are supported, structure or CORE values need work, and at least one currently eligible provider/cache path can improve CORE, provider retry is due, or safe structural creation is pending.

These are intentionally different. `core_complete = false` and `core_update_required = false` is valid when no provider/update work is currently actionable. Use `NOOP_SETTLED_INCOMPLETE` or an equivalent preflight reason for that case. It is settled for this run, not complete forever.

## 12. Opportunistic Fields

These fields remain important but should not independently force repeated Update-required status in the first 9H2 rollout:

- `cash`
- `total_debt`
- `operating_cashflow`
- `capex`
- `ebit`

If V2 CORE is complete and only opportunistic fields are missing, do not call a provider solely for those gaps. If a provider payload is already fetched for a CORE reason and contains validated opportunistic values, NULL-fill them under existing rules.

This reduces repeated provider polling and prevents 9H2 from turning into broad completeness repair.

## 13. V2 Watermark Edge Cases

Quarter row exists but all CORE fields are NULL:

- `v2_latest_structure_quarter` may advance.
- `v2_latest_present_quarter` should not advance unless another meaningful field exists and the plan explicitly treats it as meaningful.
- `v2_latest_core_complete_quarter` does not advance.

Some CORE fields present:

- `v2_latest_present_quarter` may advance.
- `v2_latest_core_complete_quarter` does not advance.
- target action is usually `ENRICH_CORE`.

CORE complete but opportunistic fields missing:

- `v2_latest_core_complete_quarter` advances.
- no provider call is justified solely for opportunistic gaps.
- target action is `NOOP_CORE_CURRENT`.
- report opportunistic gaps as metadata, for example `opportunistic_gaps` and `opportunistic_fill_allowed_if_observation_available`.

Provider `NO_DATA` or retry:

- `NO_DATA` does not make CORE complete.
- retry/backoff state can classify target action as `RETRY_PROVIDER` or `NO_ELIGIBLE_PROVIDER`.
- watermark remains based on accepted canonical values, not provider attempts.
- if all currently eligible provider paths are exhausted or not due, classify as `NOOP_SETTLED_INCOMPLETE`; do not poll every scheduler run.

## 14. Legacy Update Planner

Inputs:

- latest operational target quarter from the Check plan
- legacy watermarks
- target legacy lifecycle/retry state
- operational floor `2025 Q1`

Recommended actions:

- `UPDATE_MISSING_TARGET`
- `RETRY_OR_UPDATE_TARGET`
- `REFRESH_SEC_CONFIRMATION`
- `NOOP`
- `OUT_OF_OPERATIONAL_SCOPE`
- `BLOCKED`

Decision examples:

- target quarter is later than legacy complete watermark: update
- target row exists but retry or SEC confirmation is due: retry/update
- target row is operationally complete and no retry is due: no-op
- target quarter is before 2025 Q1: out of scope

The planner may include only work units selected by the current operational Check plan plus explicit retry work attached to that workflow. It must not scan all historical quarters for gaps.

## 15. V2 Update Planner

Inputs:

- latest operational target quarter from the Check plan
- V2 watermarks
- selected target quarter state
- provider/cache due state
- operational floor `2025 Q1`
- company profile

Recommended actions:

- `CREATE_QUARTER_AND_FILL_CORE`
- `ENRICH_CORE`
- `NOOP_CORE_CURRENT`
- `NOOP_SETTLED_INCOMPLETE`
- `BLOCKED_IDENTITY`
- `BLOCKED_COMPANY_MISSING`
- `BLOCKED_POLICY_UNSUPPORTED`
- `NO_ELIGIBLE_PROVIDER`
- `RETRY_PROVIDER`
- `OUT_OF_OPERATIONAL_SCOPE`

The V2 planner must not create work merely because an older quarter has a historical gap.

`v2_update_required` is not equivalent to "any CORE field missing". It requires currently actionable work: supported identity/profile, safe structure creation or missing CORE, and an eligible provider/cache path, retry due state, or safe structural creation. If CORE is incomplete but no provider path is actionable and no retry is due, the V2 component is a successful no-op for this run with reason `NOOP_SETTLED_INCOMPLETE`.

## 16. Persisted V2 Follow-Up Metadata

9H2-C must persist enough operational component metadata for future Check runs to reselect due V2 work after Legacy becomes current.

Minimum semantic fields:

- `work_unit_key`
- `market`
- ticker/company identity
- `fiscal_year`
- `fiscal_quarter`
- `canonical_report_date`
- `last_v2_component_status`
- `followup_reason`
- `retry_required`
- `maintenance_required`
- `deferred_reason`
- `next_retry_at` or `next_check_at`, when applicable
- `last_attempt_at`
- provider due/backoff reason
- `last_run_id`
- `resolved_at` and active flag, if useful

This is not a general sync-state table. It tells Check that a previously known work unit has operational V2 follow-up that is due or intentionally unresolved. The authoritative currentness decision remains DB truth plus provider/cache state recomputed at preflight.

## 17. V2 Follow-Up Reselection Rules

Automatic future Check selection includes:

- `RETRY_PROVIDER`, when retry is due
- transient V2 execution failure, when retry is due
- any explicitly retriable V2 component condition, when retry is due

Do not automatically reselect every normal Check:

- `BLOCKED_COMPANY_MISSING`: maintenance-required, no provider retry loop
- `BLOCKED_POLICY_UNSUPPORTED`: deferred rollout limitation, no daily retry loop
- `NOOP_SETTLED_INCOMPLETE`: no re-entry until provider/cache/new evidence or policy makes work actionable
- `NOOP_CORE_CURRENT`: no V2 follow-up

V2-only follow-up work units are allowed only when they originate from a prior selected operational work unit, persisted explicit V2 follow-up metadata, and are now due. They must not originate from broad V2 NULL scans, historical comparisons against Legacy, arbitrary old incompleteness, or pre-2025 Q1 gaps.

## 18. Settled-Incomplete Reopening

`NOOP_SETTLED_INCOMPLETE` does not mean never check again. It can reopen when one of these actionable signals exists:

- provider no-data/backoff TTL expires
- new provider observation becomes available
- SEC confirmation creates a newly eligible validated source
- SimFin cache/freshness metadata indicates target-quarter availability
- explicit maintenance run requests reevaluation
- future field/provider policy changes

Normal Check should prefer provider/cache due metadata and existing operational lifecycle signals. It must not poll every incomplete quarter just to discover whether something changed.

## 19. V2 Company-Missing Policy

Initial 9H2 policy: `BLOCK_COMPANY_MISSING_IN_INITIAL_9H2`.

Current code has V2 company creation inside SimFin API statement apply (`get_or_create_company`) and SimFin seed/specialized import paths. Those paths are tied to provider payloads or historical/local imports and are not a standalone deterministic company bootstrap service for a Check work unit.

Therefore 9H2 should not invent ad-hoc V2 company creation. If `rc_v2_company` is missing for a selected ticker, block the V2 component with `BLOCKED_COMPANY_MISSING`. Legacy may continue its current update path.

This blocker is actionable maintenance, not provider retry. Recommended metadata: `retry_required=false`, `maintenance_required=true`.

## 20. Bank and Insurance Policy

Initial common V2 incremental adapter policy: `BANK/INSURANCE -> BLOCKED_POLICY_UNSUPPORTED`.

Profiles are represented in `rc_v2_company.company_profile`. Existing specialized imports use bank and insurance tables and policies. Ordinary field formulas such as EBITDA, FCF, total debt, and ordinary cash-flow metrics are not universally valid for those profiles.

Do not route bank or insurance companies through ordinary-company formulas to increase coverage. Legacy may continue its current path.

This blocker is a deferred rollout limitation, not a transient retry. Recommended metadata: `retry_required=false`, `maintenance_required=false`, `deferred_reason=specialized_profile_support`.

## 21. Merged Check Plan and Execution Work Units

Future Check merges Source A and Source B by `work_unit_key`.

Merge requirements:

- no duplicate company + quarter work units
- Legacy and V2 follow-up reasons can coexist on one merged unit
- one plan hash covers the merged candidate set
- plan status remains `SUCCESS`, `PARTIAL`, or `FAILED` under the existing result-check contract
- a V2-only due follow-up can exist even if Legacy is already current
- the output remains one Check workflow and one plan

Deterministic merge semantics:

1. Build normalized rows for Source A and Source B.
2. Key rows by `work_unit_key`.
3. If both sources select the same key, preserve the Source A result lifecycle decision and attach V2 follow-up fields/reasons.
4. If only Source B selects the key, emit a V2-only follow-up candidate with Legacy action expected to no-op at preflight unless recomputation says otherwise.
5. Sort merged rows deterministically before hashing.

After independent planners decide component needs, merge by company + canonical fiscal quarter.

The merged unit retains:

- `legacy_action`
- `v2_action`
- shared provider opportunities
- legacy component outcome
- V2 component outcome
- overall outcome

Examples:

- legacy `NOOP`, V2 `ENRICH_CORE`: execute V2 only
- legacy `RETRY_OR_UPDATE_TARGET`, V2 `ENRICH_CORE`: execute both
- legacy `RETRY_OR_UPDATE_TARGET`, V2 `NOOP_CORE_CURRENT`: execute legacy only
- legacy `RETRY_OR_UPDATE_TARGET`, V2 `BLOCKED_COMPANY_MISSING`: execute legacy; report maintenance limitation, not retry
- legacy `NOOP`, V2 `NOOP_CORE_CURRENT`: overall no-op success

Do not store a complex sync status.

## 22. Reporting-Only Cross-Store Summary

The plan may report derived categories:

- `BOTH_REQUIRE_ACTION`
- `LEGACY_ONLY`
- `V2_ONLY`
- `LEGACY_ACTION_V2_LIMITATION`
- `BOTH_NOOP`
- `GLOBALLY_BLOCKED`

These are summaries of independent planner results. They are not persisted authority and not the main decision model.

## 23. Provider Acquisition and Sharing

Retain the principle: fetch once where safe, interpret independently.

Implementation priority:

1. Correct independent planner decisions.
2. Work-unit write scope.
3. Component retry and idempotency.
4. Provider reuse where naturally safe.
5. Duplicate-provider-call optimization later if needed.

Do not aggressively refactor stable legacy provider acquisition in the first 9H2 rollout solely to share fetches.

## 24. Provider-Call Policy

CORE gaps may justify provider acquisition according to existing provider/cache policy. Opportunistic gaps alone generally should not trigger a provider call.

If V2 CORE is complete but cash is NULL, do not call SimFin solely for cash. If SimFin is fetched for missing EBITDA and contains cash, total debt, OCF, capex, or EBIT candidates under validated policy, opportunistically NULL-fill eligible fields.

Missing CORE values also do not automatically imply an immediate provider call. Provider acquisition requires an eligible provider/cache path or retry-due state. If every eligible path is exhausted or not due, the quarter is incomplete but settled for this run.

Check must not run full financial acquisition for V2 enrichment. Update Fundamentals owns financial provider acquisition.

## 25. Work-Unit Canonical Scope

The canonical write unit is exactly one company and one canonical fiscal quarter.

Provider acquisition may return multiple periods. V2 canonical financial values, canonical provenance, completeness decisions, and conflict decisions that affect readiness must be filtered to the selected work unit.

Existing historical fallback runners must not be called wholesale from incremental Update. Extract or wrap validated field rules into selected-work-unit helpers.

## 26. Quarter Creation

Legacy quarter creation follows the existing legacy Update path and keys rows by `(ticker, period_end_date)`.

V2 quarter creation is allowed only when the company exists, profile is supported, fiscal year and fiscal quarter are known, and report date is deterministic enough for `rc_v2_quarter`. If identity is ambiguous, block the V2 component instead of inventing a quarter.

Creating `rc_v2_quarter` and `rc_v2_fundamental_quarterly` for the selected work unit is in scope for 9H2 implementation. Creating unrelated historical quarters is not.

## 27. Component Result and NOOP Semantics

Each merged work unit should emit independent component results.

Legacy component statuses:

- `SUCCESS`
- `NOOP`
- `RETRY`
- `FAILED`
- `BLOCKED`

V2 component statuses:

- `SUCCESS`
- `NOOP`
- `RETRY`
- `FAILED`
- `BLOCKED`

`NOOP` means the component was assessed, no action was required, and the component is successful.

Overall status:

- both required components `SUCCESS` or `NOOP`: overall `SUCCESS`
- known non-retry maintenance/deferred limitation while executable work succeeds: overall `SUCCESS` with limitation metadata
- retry-required component while another component succeeds or no-ops: overall `PARTIAL`
- hard/global/unsafe failure: overall `FAILED`
- whole-work-unit identity or policy blocker: overall `BLOCKED` or an existing compatible representation

No-op current stores must not create false partial status.

Overall `BLOCKED` is reserved for cases where the entire work unit cannot safely proceed, such as ambiguous identity affecting both components. A V2-specific blocker must not prevent valid Legacy execution.

## 28. CLI, UI, and RawCandle Contract

Recommended CLI exit mapping for 9H2:

- exit `0`: all currently executable required components are `SUCCESS` or `NOOP`; no transient retry or hard/global failure occurred
- exit `2`: at least one component completed safely, but one or more executable components require retry or had recoverable/transient failure
- exit `1`: hard/global/unsafe failure

Current `run_fundamental_quarter_update.py` primarily reports summary fields and exits non-zero through raised exceptions. Introducing exit `2` for structured partial retry is compatible with the existing Check CLI precedent, where `PARTIAL` maps to exit `2`.

Known non-retry limitations may exist with exit `0`. That is not hiding the limitation; it separates coverage/rollout limitation from execution failure.

RawCandle should use process exit primarily for operational retry/failure:

- exit `0`: scheduler step succeeded operationally; may still report `maintenance_required`, deferred limitations, or blocked V2 component counts
- exit `2`: operational retry is required
- exit `1`: hard failure

RawCandle must not own interpretation of V2 blocker types. SwingMaster structured result remains authoritative.

UI should display execution status separately from V2 limitations: maintenance-required company missing, deferred unsupported profiles, retriable provider failures, settled incomplete, and CORE current/no-op. A successful operation with known V2 limitations must not be displayed as if V2 were fully complete.

`PARTIAL` does not necessarily imply `retry_required=true`. Examples:

- Legacy `SUCCESS`, V2 `BLOCKED_COMPANY_MISSING`: overall `SUCCESS`, exit `0`, `retry_required=false`, `maintenance_required=true`.
- Legacy `SUCCESS`, V2 `BLOCKED_POLICY_UNSUPPORTED`: overall `SUCCESS`, exit `0`, `retry_required=false`, deferred limitation reported.
- Legacy `SUCCESS`, V2 `RETRY_PROVIDER`: overall `PARTIAL`, exit `2`, `retry_required=true`.

Structured results must expose `overall_status`, `retry_required`, `maintenance_required`, `deferred_limitations_count`, `component_failures_count`, `component_retries_count`, `component_blocked_count`, legacy component summary, and V2 component summary.

## 29. Retry Behavior Under Watermarks

Watermark-first recomputation naturally handles mixed outcomes.

Run 1: legacy succeeds for Q2, V2 provider transiently fails.

- legacy complete watermark may advance to Q2
- V2 core watermark remains before Q2 or Q2 incomplete
- replay makes legacy `NOOP` and V2 `RETRY_PROVIDER` or `ENRICH_CORE`

Reverse: V2 succeeds for Q2, legacy transiently fails.

- V2 core watermark may advance to Q2
- legacy complete watermark remains before Q2 or retry-pending
- replay makes V2 `NOOP_CORE_CURRENT` and legacy retry/update

No persistent cross-store synchronization status is required.

If a quarter is CORE-incomplete but settled for now because no eligible provider is actionable, replay should remain `NOOP_SETTLED_INCOMPLETE` until provider policy or new evidence reopens work.

If Legacy becomes current while V2 remains `RETRY_PROVIDER`, persisted V2 follow-up metadata keeps the work unit eligible for future Check selection once retry is due. Legacy currentness must not make unresolved retriable V2 work disappear.

## 30. Follow-Up Resolution and Cleanup

Operational follow-up metadata must be resolved or cleared when it no longer represents due work:

- `RETRY_PROVIDER` succeeds later: set `retry_required=false`, set `resolved_at` or clear active follow-up, and stop V2-only Check selection.
- Work becomes CORE current: clear active CORE retry state.
- Company-missing maintenance later fixed: explicit maintenance/bootstrap can clear the blocker or reopen work.
- Unsupported profile later gains a specialized adapter: policy/version change can explicitly reopen affected work units.

Stale active follow-up records must not create permanent work.

## 31. Persistent State Decision

Recommendation remains `HYBRID_COMPUTED_PLUS_RUN_METADATA`.

Compute watermarks and required actions from DB truth at preflight. Persist run/component outcomes, retry evidence, provider observations, and artifacts. Do not create authoritative persistent watermark columns, a watermark table, or a cross-store sync table in initial 9H2.

Persisted V2 follow-up metadata fits this model because component action is recomputed at preflight. The metadata preserves due/retry/maintenance intent between runs; it does not declare that V2 is current, synchronized, complete, or authoritative.

If performance becomes a concern, measure before adding cache.

## 32. Historical Scope Boundary

9H2 is not a historical repair or convergence phase.

Historical pre-2025 Q1 gaps are deferred to targeted backfill, Phase 9I, Phase 10 readiness/maintenance, or explicit historical repair tooling. V2-only repair/backfill may exist later as a diagnostic or maintenance mode, but it is not normal result discovery.

Normal 9H2 operates on the selected Check plan work units and due persisted V2 follow-up records that originated from prior operational work units.

## 33. Current Population Measurements

Read-only measurement used:

- plan: `temp/rc_fundamentals_v2_phase9h1vr_full_check/20260815T125450Z/full_check/plan.json`
- check status: `SUCCESS`
- candidate hash: `85e68238fe8b51979a924033e074d0ab003b8dc8e42f634b3ba6f9829b7970ab`
- provider calls: 0
- production writes: 0

Old coarse measurement:

- legacy required: 44
- V2 required: 42

Watermark-first measurement:

| Metric | Count |
| --- | ---: |
| executable work units | 44 |
| below 2025 Q1 scope | 0 |
| legacy `RETRY_OR_UPDATE_TARGET` | 44 |
| V2 `core_complete` | 5 |
| V2 `core_incomplete` | 39 |
| V2 `core_update_required` | 28 |
| V2 `operationally_settled_but_incomplete` | 0 |
| V2 `CREATE_QUARTER_AND_FILL_CORE` | 8 |
| V2 `ENRICH_CORE` | 20 |
| V2 `NOOP_CORE_CURRENT` | 5 |
| V2 `NOOP_SETTLED_INCOMPLETE` | 0 |
| V2 `BLOCKED_COMPANY_MISSING` | 6 |
| V2 `BLOCKED_POLICY_UNSUPPORTED` | 5 |
| reporting `BOTH_REQUIRE_ACTION` | 28 |
| reporting `LEGACY_ONLY` | 5 |
| reporting `LEGACY_ACTION_V2_LIMITATION` | 11 |
| maintenance required | 6 |
| deferred limitations | 5 |
| retry required | 0 |

Material difference from old coarse V2 required `42`: corrected V2 CORE update-required is `28`. The prior watermark measurement already found 28 CORE actions, but incorrectly exposed 3 CORE-current/opportunistic-incomplete rows as a V2 action. Under the corrected model those 3 become `NOOP_CORE_CURRENT`, so V2 no-op CORE-current rows are `5`.

R3 scheduler-safe implication: six V2 company-missing rows and five unsupported-profile rows remain visible limitations, but they no longer imply retry-required/nonzero exit solely because of the limitation. If Legacy work succeeds and no transient retry/hard failure occurs, the operational run can still be exit `0` with `maintenance_required=6` and `deferred_limitations_count=5`.

## 34. Proposed Helper Structure

Recommended helper boundaries:

- read-only plan validation reuse
- inspect legacy ticker watermarks and selected target state
- inspect V2 ticker watermarks and selected target state
- plan legacy component action
- plan V2 component action
- read due V2 follow-up metadata
- merge component actions into execution work units
- serialize component results and aggregate summaries

Likely implementation files:

- `swingmaster/fundamentals/dual_store_update_preflight.py`
- `swingmaster/fundamentals/dual_store_update_result.py`
- `swingmaster/fundamentals_v2/work_unit_apply.py`

Names are less important than boundaries: preflight is read-only, apply is selected-work-unit scoped, and historical runners are not called wholesale.

## 35. Test Strategy

Required tests:

- plan validation still rejects non-success, stale, hash-mismatched, inactive, or non-executable plans
- operational floor blocks pre-2025 Q1 target work
- legacy watermark ignores empty shell rows
- legacy planner distinguishes current, retry, missing, and out-of-scope targets
- V2 watermark separates structure-present, meaningful-present, and CORE-complete
- V2 planner separates `core_complete` from `core_update_required`
- V2 planner treats opportunistic-only gaps as no provider-call triggers
- V2 planner supports `NOOP_SETTLED_INCOMPLETE`
- V2 company missing blocks initial rollout
- bank/insurance profiles block ordinary V2 adapters
- merge logic produces both-required, legacy-only, V2-only, no-op, component-limited, and globally blocked summaries
- Check merge dedupes Source A and Source B by `work_unit_key`
- V2-only follow-up can re-enter Check after Legacy is current
- broad V2 NULL scans cannot create V2-only follow-up
- no-op components are successful, not partial
- known non-retry component blockers can produce overall SUCCESS with limitation metadata
- exit `2` is reserved for operational retry/recoverable partial execution
- selected-work-unit V2 apply ignores unrelated provider periods
- canonical non-null V2 fields are not overwritten
- provenance is inserted only for accepted canonical writes
- replay produces zero financial and provenance delta
- CLI exit `0/2/1` behavior is covered, including exit `0` with visible maintenance/deferred limitations

Tests should use fixtures and local provider/cache payloads. Network/provider calls are not needed for preflight tests.

## 36. Production Rollout Gates

Before production enablement:

1. Read-only watermark preflight must match the validated Check plan scope.
2. Dry-run must show zero provider calls when provider acquisition is disabled.
3. V2 apply must prove selected-work-unit scope.
4. CORE-only provider-call gating must be visible in summaries.
5. Replay must show zero canonical financial delta and zero provenance delta.
6. Mixed failure behavior must be visible in CLI summaries and UI.
7. Final DB integrity checks must pass for both stores.
8. RawCandle behavior must be validated if CLI process status or summary parsing changes.
9. Future Check can reselect a V2 retry work unit after Legacy became current.
10. Legacy and V2 selection sources merge without duplicate work units.
11. RawCandle observes exit `0` for operational success with deferred/maintenance limitations.
12. RawCandle observes exit `2` only for true retry-required partial execution.

## 37. Explicit Non-Goals

9H2 should not implement:

- broad historical backfill
- a second V2 discovery queue
- provider calls during Check for V2 financial enrichment
- schema changes unless a later implementation phase explicitly approves them
- RawCandle business logic
- downstream bridge/cutover
- numeric reconciliation between legacy and V2
- SEC shares, operating cashflow, or capex semantics beyond already validated paths
- overwrites of existing V2 canonical non-null values
- writes outside selected work-unit scope
- provider calls solely for opportunistic V2 gaps
- pre-2025 Q1 convergence
- V2 lifecycle states cloned from legacy statuses
- repeated polling for settled-incomplete quarters with no actionable provider
- broad V2 NULL scan as a follow-up source
- scheduler failure solely because of known non-retry V2 rollout limitations

## 38. Open Questions

Closed implementation-policy decisions:

- `CORE_CURRENT_OPPORTUNISTIC_ENRICHMENT` is not an update action. Use `NOOP_CORE_CURRENT` plus opportunistic metadata.
- Known non-retry V2 limitations plus safe executable work produce overall operational `SUCCESS` with limitation metadata, not exit `2`.
- V2 company creation is not in initial 9H2. Revisit only after a deterministic non-provider bootstrap helper exists.
- BANK/INSURANCE ordinary V2 adapters remain unsupported in initial 9H2 and do not create recurring retry loops.
- CORE missing with no eligible provider/update path is `NOOP_SETTLED_INCOMPLETE` for this run.

No material policy question remains for 9H2-A. Human review can still challenge the choices before implementation.

## 39. Recommended Implementation Decomposition

Recommended next implementation sequence:

1. Add read-only watermark/preflight helper and tests.
2. Wire preflight into Update Fundamentals dry-run output only.
3. Add component result dataclasses and summary serialization.
4. Add persisted V2 follow-up metadata read semantics.
5. Add Check merge logic for current lifecycle selection plus due V2 follow-up selection.
6. Add merge logic for legacy and V2 planner outputs.
7. Add follow-up resolution/cleanup semantics.
8. Add execution-vs-limitation status and exit policy.
9. Implement 9H2-A: read-only watermark/preflight engine.
10. Implement 9H2-B: V2 selected-work-unit executor, gated on existing V2 company and supported profile.
11. Implement 9H2-C: integrated legacy + V2 Update backend with component persistence and structured JSON.
12. Implement 9H2-D: necessary RawCandle parsing and production rollout.

Do not split into microphases unless implementation proves necessary.

Final 9H2-P-R3 classification: `PHASE_9H2_P_R3_IMPLEMENTATION_SAFE_READY_FOR_9H2_A`.
