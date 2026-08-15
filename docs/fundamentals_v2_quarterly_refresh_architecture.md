# Fundamentals V2 Quarterly Refresh Architecture

## 1. Purpose and Scope

This document defines the target architecture for incremental USA quarterly fundamentals refresh in `rc_fundamentals_v2.db`. It is grounded in the current SwingMaster result lifecycle, the Phase 9E provider timing audit, the Phase 9F lifecycle/parity audits, and the implemented V2 backfill/import modules.

This is an architecture specification for later 9H implementation. It does not change production result-check behavior, provider ordering, scheduler cadence, status semantics, or downstream readers.

## 2. Repository Ownership

SwingMaster owns fundamentals business logic: result-check selection, status semantics, plan generation and validation, provider interaction, quarter update semantics, V2 canonicalization, provenance, UI fundamentals actions, and field-level validation rules.

RawCandle owns scheduling cadence, process orchestration, logging, and process supervision. RawCandle must call SwingMaster contracts and must not duplicate candidate eligibility, plan validation, provider hierarchy, or canonical write rules.

## 3. Existing Lifecycle/Status Model

The existing model is a layered lifecycle, not one monolithic state. Current code already supports calendar checks, completed-event detection, `plan.json`, `FUNDAMENTALS_PARTIAL`, `QUARTER_BASIC_COMPLETE`, `SEC_CONFIRMED`, `SEC_CONFIRMED_YAHOO_ENRICHED`, retry decisions, UI Check for New Results, UI Update Fundamentals, and Sunday scheduler update.

Future V2 refresh should reuse this model. New provider timing and provenance observations should be metadata, not new lifecycle states, unless a later implementation proves that existing statuses cannot represent the behavior.

## 4. Result Discovery Model

Result discovery answers: has a new fiscal-quarter result appeared or become due for follow-up?

Current owners are `swingmaster/fundamentals/result_check.py`, `swingmaster/fundamentals/quarter_refresh_decision.py`, Yahoo earnings calendar/event refresh, and earnings-event-to-quarter matching. Discovery may produce a SUCCESS plan with executable candidates, a PARTIAL check with no executable candidates, or a FAILED check.

Discovery must remain distinct from financial-data acquisition and canonicalization.

## 5. Yahoo Role

Yahoo remains the primary operational discovery provider for new result timing and completed-event detection.

Yahoo may also provide initial and repeated PARTIAL enrichment. For V2, Yahoo canonical writes are allowed only through field-specific validated rules already implemented or explicitly revalidated for incremental use. Existing validated Yahoo paths are direct revenue, EBITDA, FCF, cash, total debt, shares, and EBIT with their own validation scopes and NULL-only behavior.

Yahoo values are mutable snapshots. Raw observations may be appended or refreshed, but canonical V2 values must not use last-provider-wins. Changed Yahoo values should create new raw/provenance observations and conflict diagnostics if they differ from existing canonical non-null values.

Yahoo rechecks should stop when the target quarter is no longer PARTIAL for the relevant operational completeness policy, or when retry limits/cadence say the provider is no longer due.

## 6. SEC Role

SEC is the authoritative filing confirmation and fiscal-period corroboration provider for USA. Filing confirmation is not the same as fact canonicalization.

8-K earnings information may support result/event presence but is not automatically canonical statement data. 10-Q and 10-K filings can confirm fiscal identity and provide facts only when the concept, duration, unit, and quarter reconstruction semantics are validated. Amendments should create later observations and may corroborate or conflict; they must not silently replace canonical values.

The implemented SEC Revenue path is production validated for SAFE_SCOPED reconstructed quarterly `Total Revenue`. Other SEC paths from Phase 9 remain audit-only unless later 9I phases validate field-specific semantics for shares, operating cashflow, capex, or other fields.

Standard concepts are not automatically safe. Extension concepts require explicit semantic mapping and validation. YTD-derived quarters require deterministic context subtraction and duplicate/context review before canonical writes.

## 7. SimFin Role

Current SimFin V2 implementation supports selected ticker acquisition and apply for statements and shares. It can query one or a small ticker list, uses local cache/no-data cache, has serial rate limiting, batches at most two tickers per request, retries one HTTP 429 after delay, and stops after a second 429.

SimFin statements populate revenue, gross profit, operating income, depreciation/amortization, EBITDA, net income, operating cashflow, capex, FCF, cash, and total debt. Shares use the common-shares endpoint and match observations to quarter report dates with an age limit.

Current SimFin apply is NULL-fill plus conflict counting. It does not overwrite existing non-null canonical values with different incoming values.

Provisional role:

- same-day manual update: supplementary and optional; do not assume availability.
- Sunday update: preferred delayed normalized source when due and cache/rate policy allows.
- PARTIAL recheck: supplementary delayed enrichment source after initial Yahoo/SEC checks.

## 8. Provider Roles by Lifecycle Stage

New-result discovery: Yahoo primary, SEC/SimFin not primary.

First PARTIAL processing: Yahoo may provide early facts; SEC may be attempted for confirmation; SimFin same-day is optional.

PARTIAL recheck: Yahoo short recheck, SEC confirmation retry, SimFin delayed supplementary check when due.

SEC confirmation: SEC owns filing/fiscal corroboration and validated fact enrichment.

Sunday Update: use the same SwingMaster backend as manual UI, with policy based on quarter age/context rather than caller identity.

Historical repair/backfill: use field-specific backfill modules and explicit provenance/risk tiers.

## 9. Field-Level Provider Policy

There is no universal provider hierarchy.

Revenue: SimFin API/seed and validated Yahoo/SEC field-specific paths are usable; SEC Revenue is SAFE_SCOPED only under the implemented Phase 9 rule.

Operating income and depreciation/amortization: SimFin statement fields are current implemented canonical sources. Yahoo/SEC alternatives require separate validation.

EBITDA: SimFin-derived operating income plus D&A and validated Yahoo direct EBITDA may fill NULLs. Existing non-null conflicts must be diagnosed, not overwritten.

Operating cashflow and capex: SimFin cashflow fields are implemented. SEC paths are deferred to 9I. Yahoo component paths are not the chosen validated production path for FCF.

FCF: SimFin-derived OCF plus capex and validated Yahoo direct FCF may fill NULLs.

Shares outstanding: SimFin common-shares API and validated Yahoo ordinary shares path may fill NULLs; SEC shares semantics are deferred to 9I.

Cash: SimFin balance field and validated Yahoo cash+short-term-investments path may fill NULLs.

Total debt: SimFin-derived short-term plus long-term debt and validated Yahoo direct total debt may fill NULLs.

EBIT: validated Yahoo direct EBIT is secondary/fallback. SimFin API currently leaves EBIT NULL.

## 10. PARTIAL Enrichment

`FUNDAMENTALS_PARTIAL` remains the lifecycle state for managed incomplete quarters. A PARTIAL recheck can reread Yahoo, check SEC filing/usable facts, optionally check SimFin when due, persist raw/cache observations, NULL-fill V2 fields where policy allows, recalculate completeness, and either remain PARTIAL or become complete enough for downstream usage.

No `PARTIAL_ENRICHED` status is needed. No SimFin-specific status is needed. Provider first-seen, observed fields, and enrichment attempts should be metadata.

## 11. SEC Confirmation vs Completeness

Filing confirmation and data completeness are separate axes.

A quarter can be SEC-confirmed but still missing canonical fields. A quarter can be richly populated from Yahoo/SimFin before SEC confirmation. A quarter can be complete enough for analysis but still unconfirmed.

Existing `source_confirmation_status` can represent the filing confirmation axis. V2 completeness should be computed from field presence, provider observations, and unresolved conflict metadata.

## 12. Canonical Write/Conflict Policy

Canonical writes are deterministic:

- NULL-fill is allowed only from validated field-specific provider rules.
- Same-provider refresh may update raw/cache observations, but canonical replacement requires explicit same-provider replacement policy; default is preserve and flag conflict.
- Cross-provider differences do not use last-provider-wins.
- Non-null canonical values are preserved unless a later phase implements an explicit source-precedence and migration/replay rule.
- Provenance must record provider, provider field, source dataset/file/hash, transformation, source value, import run, and risk tier where applicable.

## 13. Cross-Provider Quarter Identity

V2 rows are company + canonical fiscal quarter. Matching must use `swingmaster/fundamentals_v2/quarter_identity.py`.

Verified fiscal identity is preferred. Date-inferred matching is allowed only under explicit field-specific policy, must remain within tolerance, must reject ambiguity, and must be provenance-visible.

## 14. Check for Updates Contract

9H1 Check for Updates should own discovery, due selection, lightweight provider presence checks where appropriate, PARTIAL follow-up eligibility, status/check reporting, work-list generation, and next-check scheduling.

It should not canonicalize financial values. It may generate work units for known company + fiscal quarter.

## 15. Update Fundamentals Contract

9H2 Update Fundamentals should own provider financial-data retrieval, raw/cache persistence, V2 NULL-fill canonicalization, field provenance, completeness recalculation, status advancement, retry output, and legacy coexistence behavior.

It must validate a SUCCESS plan and use the same backend semantics for UI and scheduler callers.

## 16. Sunday vs Manual Behavior

Sunday scheduler and manual UI should use the same SwingMaster backend semantics. Differences may be orchestration only: scheduler uses a fresh SUCCESS plan and Sunday cadence; UI uses a valid same-day SUCCESS plan and can be user-triggered sooner.

Provider ordering should depend on quarter age, confirmation state, and provider due policy, not on whether the caller is UI or scheduler.

## 17. Failure/Retry Policy

One provider failure should not automatically prevent use of another provider unless fiscal identity is ambiguous or the work unit cannot be safely identified.

Rate limits are retriable with provider-specific backoff and stop rules. Parse failures, malformed responses, and provider conflicts are surfaced in logs/provenance diagnostics. Ambiguous fiscal identity is not retriable without new evidence. Semantic rejection is terminal for that provider/field rule until revalidation.

## 18. Timing Instrumentation

Add a small observation-safe provider timing table or equivalent metadata in 9H. It should record provider, company, quarter identity, observation kind, observed_at_utc, provider-reported timestamp when available, field-presence fingerprint, payload hash/source reference, run id, and outcome.

Timing logs must not embed canonical semantics. They collect evidence for later cadence tuning.

## 19. V2 Integration Boundary

During transition, legacy update should continue in parallel until downstream bridge phases are complete. 9H2 should maintain V2 incrementally as an additional canonical store, not replace legacy downstream reads yet.

V2 update should consume fresh provider raw/cache observations or validated legacy-normalized observations through explicit adapters. Duplicate work is avoided by company+quarter work units, cache hits, no-data cache, NULL-fill policy, and provenance conflict detection.

## 20. Legacy Coexistence During Transition

Legacy `fundamentals_usa.db` remains operational for current downstream workflows. V2 should be updated and measured in parallel. Phase 11 can remove legacy dependency only after bridge parity, recalibration, and cutover validation.

## 21. Operational Completeness

Quarter completeness should track:

- P0: revenue, EBITDA, FCF.
- P0/P1: shares_outstanding, cash, total_debt.
- supporting: operating_cashflow, capex.
- SEC-confirmed yes/no.
- providers observed.
- unresolved conflicts.

Do not create a status for every combination. Use computed metrics plus existing lifecycle states.

## 22. 9H1 Implementation Requirements

9H1 should extend Check for Updates around existing result-check services. Likely changes include result-check/work-list metadata, provider timing observation scaffolding, PARTIAL follow-up eligibility, plan/work-unit output, focused tests for SUCCESS/PARTIAL/FAILED and UI/scheduler parity, and minimal RawCandle contract changes only if command outputs change.

## 23. 9H2 Implementation Requirements

9H2 should implement update work units, provider adapters, V2 canonicalization helpers, field-policy reuse, completeness recalculation, retry output, UI/scheduler execution parity, and rollout gates. It should preserve legacy update behavior until migration phases are ready.

## 24. Explicit Non-Goals

9G does not implement provider calls, production writes, provider cadence changes, new statuses, downstream V2 bridge, SimFin same-day assumptions, SEC shares/OCF/capex semantics, or RawCandle business logic.

## 25. Open Questions / Items Deferred to 9I

SEC `shares_outstanding`, `operating_cashflow`, and `capex` canonical semantics remain deferred to 9I. SEC extension concept treatment and YTD-derived quarter reconstruction for non-revenue fields also require later validation. Exact SimFin first-availability latency should be measured by future timing instrumentation rather than assumed now.
