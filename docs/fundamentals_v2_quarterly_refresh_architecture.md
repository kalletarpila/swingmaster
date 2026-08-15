# Fundamentals V2 Quarterly Refresh Architecture

## 1. Purpose and Scope

This document defines the target architecture for incremental USA quarterly fundamentals refresh in `rc_fundamentals_v2.db`. It is grounded in the current SwingMaster result lifecycle, the Phase 9E provider timing audit, the Phase 9F lifecycle/parity audits, and the implemented V2 backfill/import modules.

This is an architecture specification for later 9H implementation. It does not change production result-check behavior, provider ordering, scheduler cadence, status semantics, or downstream readers.

## 2. Repository Ownership

SwingMaster owns fundamentals business logic: result-check selection, status semantics, plan generation and validation, provider interaction, quarter update semantics, V2 canonicalization, provenance, UI fundamentals actions, and field-level validation rules.

RawCandle owns scheduling cadence, process orchestration, logging, and process supervision. RawCandle must call SwingMaster contracts and must not duplicate candidate eligibility, plan validation, provider hierarchy, or canonical write rules.

## 3. Architecture Invariants

These invariants constrain 9H implementation:

1. SwingMaster owns fundamentals business semantics.
2. Check for Updates does not canonicalize V2 financial facts.
3. Update Fundamentals operates on company + canonical fiscal quarter work units.
4. Canonical non-null facts are not silently overwritten.
5. Replaying the same work unit with the same provider observations produces zero canonical financial delta, zero duplicate provenance delta, and no duplicate lifecycle/work records.
6. Provider timing observations are historical/audit-safe even if raw provider cache is mutable.
7. SEC confirmation is not the same thing as data completeness.
8. Legacy and V2 coexist until Phase 11 cutover.
9. UI and scheduler use the same SwingMaster backend semantics.

## 4. Existing Lifecycle/Status Model

The existing model is a layered lifecycle, not one monolithic state. Current code already supports calendar checks, completed-event detection, `plan.json`, `FUNDAMENTALS_PARTIAL`, `QUARTER_BASIC_COMPLETE`, `SEC_CONFIRMED`, `SEC_CONFIRMED_YAHOO_ENRICHED`, retry decisions, UI Check for New Results, UI Update Fundamentals, and Sunday scheduler update.

Future V2 refresh should reuse this model. New provider timing and provenance observations should be metadata, not new lifecycle states, unless a later implementation proves that existing statuses cannot represent the behavior.

## 5. Result Discovery Model

Result discovery answers: has a new fiscal-quarter result appeared or become due for follow-up?

Current owners are `swingmaster/fundamentals/result_check.py`, `swingmaster/fundamentals/quarter_refresh_decision.py`, Yahoo earnings calendar/event refresh, and earnings-event-to-quarter matching. Discovery may produce a SUCCESS plan with executable candidates, a PARTIAL check with no executable candidates, or a FAILED check.

Discovery must remain distinct from financial-data acquisition and canonicalization.

## 6. Yahoo Role

Yahoo remains the primary operational discovery provider for new result timing and completed-event detection.

Yahoo may also provide initial and repeated PARTIAL enrichment. For V2, Yahoo canonical writes are allowed only through field-specific validated rules already implemented or explicitly revalidated for incremental use. Existing validated Yahoo fallback rules for revenue, EBITDA, FCF, cash, total debt, shares, and EBIT each have their own implemented source semantics, validation scopes, provenance, and NULL-only behavior. This architecture document intentionally does not redefine those field-level formulas.

Yahoo values are mutable snapshots. Raw observations may be appended or refreshed, but canonical V2 values must not use last-provider-wins. Changed Yahoo values should create new raw/provenance observations and conflict diagnostics if they differ from existing canonical non-null values.

Yahoo rechecks should stop when the target quarter is no longer PARTIAL for the relevant operational completeness policy, or when retry limits/cadence say the provider is no longer due.

## 7. SEC Role

SEC is the authoritative filing confirmation and fiscal-period corroboration provider for USA. Filing confirmation is not the same as fact canonicalization.

8-K earnings information may support result/event presence but is not automatically canonical statement data. 10-Q and 10-K filings can confirm fiscal identity and provide facts only when the concept, duration, unit, and quarter reconstruction semantics are validated. Amendments should create later observations and may corroborate or conflict; they must not silently replace canonical values.

The implemented SEC Revenue path is production validated for SAFE_SCOPED reconstructed quarterly `Total Revenue`. Other SEC paths from Phase 9 remain audit-only unless later 9I phases validate field-specific semantics for shares, operating cashflow, capex, or other fields.

Standard concepts are not automatically safe. Extension concepts require explicit semantic mapping and validation. YTD-derived quarters require deterministic context subtraction and duplicate/context review before canonical writes.

## 8. SimFin Role

Current SimFin V2 implementation supports selected ticker acquisition and apply for statements and shares. It can query one or a small ticker list, uses local cache/no-data cache, has serial rate limiting, batches at most two tickers per request, retries one HTTP 429 after delay, and stops after a second 429.

Historical SimFin bulk CSV/local datasets were used for historical V2 seeding and backfill. They are not an operational new-quarter refresh mechanism.

Future operational incremental refresh should use the SimFin statements API for selected-ticker normalized statements and the separate SimFin common-shares API for point-in-time shares. Do not treat "SimFin seed" and "SimFin API" as one operational provider path.

SimFin statements populate revenue, gross profit, operating income, depreciation/amortization, EBITDA, net income, operating cashflow, capex, FCF, cash, and total debt. Shares use the common-shares endpoint and match observations to quarter report dates with an age limit.

Current SimFin apply is NULL-fill plus conflict counting. It does not overwrite existing non-null canonical values with different incoming values.

Provisional role:

- same-day manual update: supplementary and optional; do not assume availability.
- Sunday update: preferred delayed normalized source when due and cache/rate policy allows. This is not an unconditional SimFin-first or broad-universe refresh policy.
- PARTIAL recheck: supplementary delayed enrichment source after initial Yahoo/SEC checks.

Before a SimFin provider call, 9H2 policy must consider whether the company+quarter is selected, whether relevant canonical fields remain missing, whether cached SimFin data already satisfies the work unit, whether recent NO_DATA/backoff applies, provider call/rate-limit budget, and quarter age/provider-due policy.

## 9. Provider Roles by Lifecycle Stage

New-result discovery: Yahoo primary, SEC/SimFin not primary.

First PARTIAL processing: Yahoo may provide early facts; SEC may be attempted for confirmation; SimFin same-day is optional.

PARTIAL recheck: Yahoo short recheck, SEC confirmation retry, SimFin delayed supplementary check when due.

SEC confirmation: SEC owns filing/fiscal corroboration and validated fact enrichment.

Sunday Update: use the same SwingMaster backend as manual UI, with policy based on quarter age/context rather than caller identity.

Historical repair/backfill: use field-specific backfill modules and explicit provenance/risk tiers.

## 10. Field-Level Provider Policy

There is no universal provider hierarchy.

Revenue: operational SimFin statements API and validated field-specific Yahoo/SEC fallback rules are usable; historical SimFin seed remains historical seed/backfill input, not an operational refresh path. SEC Revenue is SAFE_SCOPED only under the implemented Phase 9 rule.

Operating income and depreciation/amortization: SimFin statement fields are current implemented canonical sources. Yahoo/SEC alternatives require separate validation.

EBITDA: SimFin-derived operating income plus D&A and the validated field-specific Yahoo EBITDA fallback rule may fill NULLs. Existing non-null conflicts must be diagnosed, not overwritten.

Operating cashflow and capex: SimFin cashflow fields are implemented. SEC paths are deferred to 9I. Yahoo component paths are not the chosen validated production path for FCF.

FCF: SimFin-derived OCF plus capex and the validated field-specific Yahoo FCF fallback rule may fill NULLs.

Shares outstanding: SimFin common-shares API and the validated field-specific Yahoo ordinary shares fallback rule may fill NULLs; SEC shares semantics are deferred to 9I.

Cash: SimFin balance field and the validated field-specific Yahoo cash fallback rule may fill NULLs.

Total debt: SimFin-derived short-term plus long-term debt and the validated field-specific Yahoo total debt fallback rule may fill NULLs.

EBIT: the validated field-specific Yahoo EBIT fallback rule is secondary/fallback. SimFin API currently leaves EBIT NULL.

## 11. PARTIAL Enrichment

`FUNDAMENTALS_PARTIAL` remains the lifecycle state for managed incomplete quarters.

Check for Updates may decide that a PARTIAL quarter is due for follow-up, perform lightweight discovery/status or provider-presence checks where permitted, record timing/check metadata, and emit a work unit. It must not write canonical V2 financial facts.

Update Fundamentals owns the financial enrichment part of a PARTIAL recheck: reread Yahoo financial data when due, check SEC filing/usable facts, optionally check SimFin when due, persist raw/cache observations needed for the update, NULL-fill V2 fields where policy allows, recalculate completeness, and either keep the work unit PARTIAL or advance it to complete enough for downstream usage.

No `PARTIAL_ENRICHED` status is needed. No SimFin-specific status is needed. Provider first-seen, observed fields, and enrichment attempts should be metadata.

## 12. SEC Confirmation vs Completeness

Filing confirmation and data completeness are separate axes.

A quarter can be SEC-confirmed but still missing canonical fields. A quarter can be richly populated from Yahoo/SimFin before SEC confirmation. A quarter can be complete enough for analysis but still unconfirmed.

Existing `source_confirmation_status` can represent the filing confirmation axis. V2 completeness should be computed from field presence, provider observations, and unresolved conflict metadata.

## 13. Canonical Write/Conflict Policy

Canonical writes are deterministic:

- NULL-fill is allowed only from validated field-specific provider rules.
- Same-provider refresh may update raw/cache observations, but canonical replacement requires explicit same-provider replacement policy; default is preserve and flag conflict.
- Cross-provider differences do not use last-provider-wins.
- Non-null canonical values are preserved unless a later phase implements an explicit source-precedence and migration/replay rule.
- Provenance must record provider, provider field, source dataset/file/hash, transformation, source value, import run, and risk tier where applicable.

Architecture invariant: re-running the same company+quarter work unit with the same provider observations must produce canonical financial delta = 0, duplicate provenance delta = 0, and no duplicate lifecycle/work records. A new provider observation may add timing/raw evidence, NULL-fill a missing eligible field, create a conflict diagnostic, or update retry/completeness metadata, but still must not silently overwrite existing non-null canonical facts.

## 14. Cross-Provider Quarter Identity

V2 rows are company + canonical fiscal quarter. Matching must use `swingmaster/fundamentals_v2/quarter_identity.py`.

Verified fiscal identity is preferred. Date-inferred matching is allowed only under explicit field-specific policy, must remain within tolerance, must reject ambiguity, and must be provenance-visible.

## 15. Check for Updates Contract

9H1 Check for Updates should own discovery, due selection, lightweight provider presence checks where appropriate, PARTIAL follow-up eligibility, status/check reporting, work-list generation, and next-check scheduling.

It must not write canonical V2 financial values. It must not perform full financial statement retrieval merely for enrichment, and must not run broad SimFin refresh. If current or future result-check logic needs Yahoo event/result information, that use is discovery/status metadata, not V2 financial canonicalization.

It may generate work units for known company + fiscal quarter.

## 16. Update Fundamentals Contract

9H2 Update Fundamentals should own provider financial-data retrieval, raw/cache persistence, V2 NULL-fill canonicalization, field provenance, completeness recalculation, status advancement, retry output, and legacy coexistence behavior.

It must validate a SUCCESS plan and use the same backend semantics for UI and scheduler callers.

## 17. Sunday vs Manual Behavior

Sunday scheduler and manual UI should use the same SwingMaster backend semantics. Differences may be orchestration only: scheduler uses a fresh SUCCESS plan and Sunday cadence; UI uses a valid same-day SUCCESS plan and can be user-triggered sooner.

Provider ordering should depend on quarter age, confirmation state, and provider due policy, not on whether the caller is UI or scheduler.

Sunday SimFin preference means that selected Sunday work units may prefer SimFin as a delayed normalized source when due. It does not mean query SimFin unconditionally for all companies, refresh the broad universe every Sunday, or query SimFin before checking cache/completeness/no-data/backoff state.

## 18. Failure/Retry Policy

One provider failure should not automatically prevent use of another provider unless fiscal identity is ambiguous or the work unit cannot be safely identified.

Rate limits are retriable with provider-specific backoff and stop rules. Parse failures, malformed responses, and provider conflicts are surfaced in logs/provenance diagnostics. Ambiguous fiscal identity is not retriable without new evidence. Semantic rejection is terminal for that provider/field rule until revalidation.

Legacy and V2 updates are not assumed to form one cross-database ACID transaction during transition. A successful legacy update should not be rolled back merely because the V2 shadow update fails. A V2 failure must remain visible as explicit retry/sync metadata and in logs/UI/reporting; the company+quarter work unit must remain replayable and must not disappear from follow-up merely because legacy succeeded. Conversely, if V2 succeeds and legacy fails, the legacy failure remains visible under current transition ownership, and Phase 11 decides when legacy dependency can be retired.

Replay invariant: retrying a failed or partial work unit with unchanged provider observations must not duplicate canonical values, provenance, timing-equivalent observations, or lifecycle/work records. New observations may update retry/completeness metadata or add conflict diagnostics.

## 19. Timing Instrumentation

Add a small observation-safe provider timing table or equivalent metadata in 9H. It should record provider, company, quarter identity, observation kind, observed_at_utc, provider-reported timestamp when available, field-presence fingerprint, payload hash/source reference, run id, and outcome.

Even if an operational provider raw/cache table stores only the latest payload or a refreshed mutable snapshot, timing/observation history must preserve an append-safe observation trail. Each relevant observation should retain provider, company, canonical or provisional quarter identity, observation kind, `observed_at_utc`, provider-reported timestamp if available, field-presence fingerprint, payload hash/source reference, run id, and result/outcome.

A repeated observation with an unchanged payload hash may be compacted or deduplicated only if the timing evidence needed for cadence analysis remains preserved.

Timing logs must not embed canonical semantics. They collect evidence for later empirical measurement of Yahoo progressive enrichment, SEC first-seen timing, SimFin first-seen timing, and provider stabilization/recheck cadence.

## 20. V2 Integration Boundary

During transition, legacy update should continue in parallel until downstream bridge phases are complete. 9H2 should maintain V2 incrementally as an additional canonical store, not replace legacy downstream reads yet.

V2 update should consume fresh provider raw/cache observations or validated legacy-normalized observations through explicit adapters. Historical SimFin bulk seed remains historical seed/backfill input; operational incremental refresh should use selected-ticker SimFin statements API and common-shares API where policy allows. Duplicate work is avoided by company+quarter work units, cache hits, no-data cache, NULL-fill policy, idempotent provenance, and conflict detection.

Legacy and V2 updates are not a single ACID transaction. 9H2 must explicitly report legacy result, V2 result, and retry/sync state for each work unit. Neither side's success should hide the other side's failure.

## 21. Legacy Coexistence During Transition

Legacy `fundamentals_usa.db` remains operational for current downstream workflows. V2 should be updated and measured in parallel. Phase 11 can remove legacy dependency only after bridge parity, recalibration, and cutover validation.

If legacy succeeds and V2 fails, do not roll back legacy solely for shadow V2 failure; keep V2 retry/sync condition visible. If V2 succeeds and legacy fails, keep the legacy failure visible and do not let V2 success imply downstream readiness while downstream still depends on legacy.

## 22. Operational Completeness

Quarter completeness should track:

- P0: revenue, EBITDA, FCF.
- P0/P1: shares_outstanding.
- P1: cash, total_debt.
- Supporting: operating_cashflow, capex.
- Secondary/fallback: EBIT.
- SEC-confirmed yes/no.
- providers observed.
- unresolved conflicts.

Do not create a status for every combination. Use computed metrics plus existing lifecycle states.

SEC-confirmed state, providers observed, and unresolved conflicts are orthogonal metadata/quality axes, not priority classes.

## 23. 9H1 Implementation Requirements

9H1 should extend Check for Updates around existing result-check services. Likely changes include result-check/work-list metadata, provider timing observation scaffolding, PARTIAL follow-up eligibility, plan/work-unit output, focused tests for SUCCESS/PARTIAL/FAILED and UI/scheduler parity, and minimal RawCandle contract changes only if command outputs change.

9H1 must not include V2 financial canonicalization, full statement retrieval merely for enrichment, or broad SimFin refresh. Lightweight provider presence/status checks are allowed only when explicitly part of discovery or availability logic.

## 24. 9H2 Implementation Requirements

9H2 should implement update work units, provider adapters, V2 canonicalization helpers, field-policy reuse, completeness recalculation, retry output, UI/scheduler execution parity, and rollout gates. It should preserve legacy update behavior until migration phases are ready.

9H2 must include work-unit financial acquisition, provider raw/cache persistence needed for executable updates, V2 NULL-fill canonicalization, provenance, legacy/V2 parallel-result handling, replay/idempotency checks, completeness calculation, and retry/sync state.

## 25. Explicit Non-Goals

9G does not implement provider calls, production writes, provider cadence changes, new statuses, downstream V2 bridge, SimFin same-day assumptions, SEC shares/OCF/capex semantics, or RawCandle business logic.

## 26. Open Questions / Items Deferred to 9I

SEC `shares_outstanding`, `operating_cashflow`, and `capex` canonical semantics remain deferred to 9I. SEC extension concept treatment and YTD-derived quarter reconstruction for non-revenue fields also require later validation. Exact SimFin first-availability latency should be measured by future timing instrumentation rather than assumed now.
