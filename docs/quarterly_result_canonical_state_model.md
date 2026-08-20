# Canonical Quarterly Result State Model

This document defines the canonical state model for quarterly fundamental results in SwingMaster.
It is a conceptual specification. It does not change Legacy, V2, Check for Updates, Update
Fundamentals, scheduler behavior, database schema, or provider behavior.

## Executive Overview

A quarterly result does not have one single status.

One quarterly result has:

- one small sequential lifecycle
- several independent parallel state dimensions
- events that happen at specific points in time
- next-action and retry information
- provider and provenance evidence
- historical data-debt state

This separation matters because many useful combinations are valid. For example, a quarter can be:

- operationally settled
- core fields ready
- SEC confirmation still pending
- score not ready because older history is missing
- not currently scheduled for provider retry

Those facts should not be collapsed into one giant value such as `DONE`, `COMPLETE`, or `WAITING`.
Each word would hide important information.

The canonical model therefore uses a state vector. A state vector is just a set of named facts about
the same quarterly result.

Example:

```text
Identity:     usa / XYZ / 2026 / Q2
Role:         LATEST_OPERATIONAL_Q
Lifecycle:    OPERATIONALLY_SETTLED
Core fields:  Q_CORE_FIELDS_READY=true
SEC:          PENDING
Score:        SCORE_READY=false
Action:       NO_ACTION
Backfill:     company has older historical debt
```

This example is coherent. The quarter can be operationally settled because no normal immediate work
is due, while still lacking SEC confirmation or comparable score readiness.

## Canonical Q Identity

The canonical identity of one quarterly result is:

```text
market + ticker + fiscal_year + fiscal_quarter
```

| Element | Plain meaning | Technical meaning |
| --- | --- | --- |
| `market` | The market namespace for the security. | Example: `usa`. Prevents ticker collisions across markets. |
| `ticker` | The security/company symbol in that market. | The operational security identifier used by SwingMaster workflows. |
| `fiscal_year` | The fiscal year reported by the company. | The company's reported fiscal year. It is not the calendar year unless the company's fiscal calendar aligns with the calendar year. |
| `fiscal_quarter` | The fiscal quarter reported by the company. | The company's reported fiscal quarter, one of `Q1`, `Q2`, `Q3`, `Q4`. It is not inferred from the calendar quarter unless that inference is explicitly validated. |

`fiscal_year` and `fiscal_quarter` always refer to the company's reported fiscal period, not the
calendar year and calendar quarter. `period_end_date` and publication-related dates are separate
metadata and do not redefine the fiscal-period identity.

Non-calendar fiscal year example:

```text
market          = usa
ticker          = XYZ
fiscal_year     = 2026
fiscal_quarter  = Q1
period_end_date = 2025-08-31
```

The canonical identity is `usa + XYZ + 2026 + Q1`. It must not be re-identified as `2025 Q3`
merely because the period ended during calendar Q3 2025.

Dates and provider observations describe the Q, but they are not the canonical Q identity.

| Concept | Classification | Notes |
| --- | --- | --- |
| result/publication date | metadata | Describes when the result was published or became visible. |
| expected result date | event/scheduling evidence | Used to decide when to check, not part of identity. |
| market-availability date | metadata | Used by downstream timing and backtests. |
| period-end date | metadata/identity evidence | The fiscal period's end date. It supports matching but is not a substitute for reported fiscal year/quarter. |
| provider observation date | provenance evidence | Describes when a provider returned data. |
| SEC filing date | assurance/provenance evidence | Confirms or supports a Q; does not create a new Q identity. |
| canonical row id | storage detail | V2 uses row ids; the conceptual identity remains market/ticker/fiscal year/fiscal quarter. |
| vintage/provenance | evidence history | Multiple vintages can attach to one Q identity. |

When matching one Q across Legacy, V2, Yahoo, SEC, SimFin, or another provider, prefer reported
fiscal identity where it is reliably available. Use period-end date, publication date, filing date,
and provider observation dates as supporting evidence. Do not silently replace fiscal identity with
calendar-quarter identity.

## Quarterly Result

A `Quarterly Result` is one canonical fiscal-quarter fundamental result for one company/security in
one market.

One Q identity may accumulate multiple pieces of evidence over time:

- Yahoo event or fundamentals observations
- SEC filing evidence
- SimFin statement or share evidence
- provider retries
- accepted corrections
- provenance rows
- later enrichment

Those do not make it a different Q. They change the evidence, readiness, action, or lifecycle state
for the same Q identity.

## Sequential Lifecycle

`Q_RESULT_LIFECYCLE` is the minimal sequential lifecycle of the quarterly result itself. It must not
carry readiness, provider, retry, SEC, score, or historical backfill meaning.

| Canonical name | Plain-language description | Precise meaning | Entry condition | Exit condition | Normal next state | Can be skipped? | Can be revisited? | Applies to | Does not imply |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `EXPECTED` | A result is expected but not observed. | Calendar or provider evidence says a future Q result should arrive. | Expected date or calendar estimate exists. | Result is observed or the expectation is withdrawn/stale. | `RESULT_DETECTED` | Yes, if a result appears without prior calendar evidence. | Yes, for future expected quarters as estimates change. | Future/latest context, run/event level. | No canonical data, no provider success, no readiness. |
| `RESULT_DETECTED` | The system has seen evidence that the Q exists. | A result event or source period indicates the Q result has appeared, but canonical ingestion is not complete. | New result event/source period detected. | Canonical identity row or target mapping exists. | `CANONICALIZED` | Yes, if historical data is imported directly. | Yes, if a settled Q is later re-detected through new evidence. | Any Q, especially latest/current operations. | Core ready, SEC confirmed, score ready. |
| `CANONICALIZED` | The Q exists in canonical storage or mapped Legacy storage. | The system can identify a concrete Q row/target. It may have no fields or partial fields. | V2 quarter row exists or Legacy target/fact row exists. | Provider/enrichment work starts, or no work remains. | `INGESTING` or `OPERATIONALLY_SETTLED` | Yes, for conceptual expected-only records. | Yes. | Any Q. | Complete data, source assurance, or no historical debt. |
| `INGESTING` | The system is actively trying to fill or confirm the Q. | Fetch, enrichment, provider retry, or confirmation work is active under current policy. | Work is selected, retry due, or followup active. | Work succeeds, becomes blocked, or no eligible work remains. | `OPERATIONALLY_SETTLED` | Yes, if no work is needed. | Yes. | Any Q; normal new-result ingestion is latest/current. | Core readiness, score readiness, or SEC confirmation. |
| `OPERATIONALLY_SETTLED` | No normal immediate work is pending. | There is currently no normal immediate operational work pending for this Q in the standard quarterly-result workflow. | No due action remains under current policy. | New evidence, correction, backfill selection, or retry reopens the Q. | Usually stable; can go to `REOPENED`. | Yes. | Yes. | Any Q. | Core ready, SEC confirmed, score ready, historically complete, or all enrichment complete. |
| `REOPENED` | A previously settled Q needs renewed processing. | A settled Q is selected again because new evidence, correction, enrichment, or backfill is now relevant. | New evidence, accepted correction, new enrichment opportunity, or controlled backfill selection. | Reprocessing starts. | `INGESTING` | No, only after prior settlement. | Yes. | Any Q. | The prior data was wrong; only that it needs renewed handling. |

### Reopened Semantics

`REOPENED` exists because quarterly data can change after it looked settled. Examples:

- a provider returns new evidence
- a correction is accepted
- a new enrichment source becomes available
- a historical backfill phase selects the Q
- a previously failed provider path becomes retryable

The current discovery supports `REOPENED` as a lifecycle concept, but the implementation detail is
not locked. It may be persisted as a state, or represented as an event/transition back to
`INGESTING`.

Open implementation marker: `TERM_REQUIRES_FINAL_IMPLEMENTATION_CONTRACT` for whether `REOPENED`
is stored as a state or emitted only as a transition event.

## Q Role

`Q_ROLE` is contextual. It is not the lifecycle.

| Role | Meaning | Scope | Persistence recommendation |
| --- | --- | --- | --- |
| `FUTURE_EXPECTED_Q` | A future result is expected but is not yet a canonical/detected result. | Future expected context. | Derive from calendar/event evidence and decision date. |
| `LATEST_OPERATIONAL_Q` | The newest operational quarter for a ticker/company under the current decision date. | Latest Q only. | Derive from quarter ordering. |
| `HISTORICAL_Q` | A canonical/detected Q older than the current latest operational Q. | Historical Q. | Derive from quarter ordering. |

The role values are mutually exclusive for a given Q at a given decision time.

A Q can move from `LATEST_OPERATIONAL_Q` to `HISTORICAL_Q` when a newer result becomes the
operational latest quarter. That role change does not automatically change lifecycle, core readiness,
SEC confirmation, provenance, or historical backfill state.

## Core Field Readiness

`Q_CORE_FIELDS_READY` applies to any Q for supported ordinary-company semantics.

For an `ORDINARY` company, the rule is:

- valid Q identity
- `revenue` present
- `ebitda` present
- `free_cashflow` present as a direct canonical value
- `cash` present
- `total_debt` present
- `shares_outstanding` present and greater than zero

`LATEST_Q_CORE_READY` means:

> `Q_CORE_FIELDS_READY` evaluated for the current `LATEST_OPERATIONAL_Q`.

It is not a separate rule.

`Q_CORE_FIELDS_READY=true` does not imply:

- SEC confirmed
- TTM ready
- score ready
- valuation ready
- no historical gaps
- no possible enrichment
- no provider/provenance gaps

Legacy `quarter_basic_complete` is not a synonym. Current V2 preflight core completeness is not a
synonym.

## Field Completeness Layers

| Layer | Fields or examples | Meaning |
| --- | --- | --- |
| Core required | `revenue`, `ebitda`, `free_cashflow`, `cash`, `total_debt`, `shares_outstanding > 0` | Required for `Q_CORE_FIELDS_READY` under ordinary-company semantics. |
| Downstream-history required | Depends on the consumer window: revenue, EBITDA, FCF, shares, cash/debt leverage inputs. | Required when historical calculations, trend, consistency, dilution, leverage, or score comparability depend on the Q. |
| Optional enrichment | `ebit`, `operating_cashflow`, `capex` unless a named consumer requires them. | Useful extra information or reconstruction/audit support. |
| Provenance/audit only | provider field names, source file/hash, validation tier, import run. | Explains where values came from. |

A Q can be `Q_CORE_FIELDS_READY=true` while `ENRICHMENT_INCOMPLETE=true`. For example, a Q may have
all required core fields but still later receive `operating_cashflow`, `capex`, EBIT, or richer
provenance evidence.

## Downstream Readiness

Downstream readiness is not necessarily a state of one individual Q.

### TTM_READY

Scope: `COMPANY_WINDOW_STATE`.

TTM readiness depends on a window of multiple quarters, usually the current four-quarter window. A
single Q contributes to TTM readiness, but it does not own it alone.

### SCORE_READY

Scope: `COMPANY_WINDOW_STATE`.

Score readiness requires supported profile plus the validated EBITDA-based readiness principles:
growth, EBITDA margin, EBITDA margin trend, FCF, EBITDA consistency, leverage, and dilution
readiness. A latest Q can be core-ready while `SCORE_READY=false` because older historical quarters
or derived TTM metrics are missing.

### KEEP_SCORE_WITH_READINESS_FLAG

A score may exist even when full comparable readiness is false. Such a score should be kept with an
explicit readiness flag rather than treated as fully comparable.

### VALUATION_READY

Valuation readiness may depend on Q-level facts such as positive shares and latest/current market
price availability. Historical valuation needs historical market data and should be modeled
explicitly if it becomes a requirement.

## SEC / Assurance Dimension

`SEC_CONFIRMATION_STATE` is a parallel assurance dimension. It is not Q completion.

| State | Meaning | Any Q? | Core fields may already be ready? | Can arrive without value changes? |
| --- | --- | --- | --- | --- |
| `NOT_APPLICABLE` | SEC confirmation is not relevant for the market/profile/source. | Yes | Yes | Yes |
| `NOT_YET_EXPECTED` | It is too early to expect filing evidence. | Mostly latest/current | Yes | Yes |
| `PENDING` | Confirmation is expected but not yet obtained. | Yes | Yes | Yes |
| `CHECKED_NO_EVIDENCE` | The system checked and found no matching evidence. | Yes | Yes | Yes |
| `PARTIAL_EVIDENCE` | Some evidence exists but does not fully confirm the Q. | Yes | Yes | Yes |
| `CONFIRMED` | Authoritative SEC or equivalent evidence confirms the Q/source. | Yes | Yes | Yes |
| `UNSUPPORTED` | The confirmation policy cannot handle this case. | Yes | Yes | Yes |
| `ERROR_RETRY` | A confirmation attempt failed and may be retried. | Yes | Yes | Yes |
| `NOT_DERIVABLE` | Current evidence is insufficient to classify assurance. | Yes | Yes | Yes |

Valid combination:

```text
Q_CORE_FIELDS_READY=true
Q_RESULT_LIFECYCLE=OPERATIONALLY_SETTLED
SEC_CONFIRMATION_STATE=PENDING
```

## Provider / Provenance Dimension

Business/data state says what is true about the Q.

Provider/provenance evidence says where the information came from and what providers returned.

Examples:

- Yahoo event observed
- Yahoo fundamentals observed
- SEC filing observed
- SimFin statement observed
- SimFin shares observed
- provider `NO_DATA`
- provider `ERROR`
- retryable response
- cache hit
- source file/hash
- validation tier
- transformation

Provider outcomes generally must not become Q lifecycle states. A provider error may create
`NEXT_ACTION=RETRY_PROVIDER`; it does not mean the Q lifecycle itself is `ERROR`.

## Next Action / Retry Dimension

State means what is true. Action means what the system should do next.

Examples:

- core incomplete is a readiness state
- retry provider tomorrow is an action plus scheduling metadata
- SEC pending is an assurance state
- check SEC later is an action

| Action | Meaning | Typical trigger | Latest Q? | Historical Q? | Coexistence |
| --- | --- | --- | --- | --- | --- |
| `NO_ACTION` | No work is due under current policy. | Settled/no due work. | Yes | Yes | Can coexist with score not ready or SEC pending if not due. |
| `CHECK_EXPECTED_RESULT` | Check whether an expected result has appeared. | Calendar/expected date due. | Usually yes | No for normal daily flow. | May coexist with provider scheduling metadata. |
| `FETCH_NEW_RESULT` | Fetch first canonical data for a detected result. | New result detected and target resolved. | Usually yes | Rare/import/backfill only. | Can coexist with SEC pending. |
| `ENRICH_Q` | Fill eligible missing canonical or enrichment fields. | Partial row and provider/cache evidence. | Yes | Yes under controlled enrichment/backfill. | Can coexist with core ready if enrichment is optional. |
| `RETRY_PROVIDER` | Retry provider after failure, no-data, or backoff policy. | Retry/followup due. | Yes | Yes if controlled. | Can target provider evidence while Q is otherwise usable. |
| `CHECK_SEC_CONFIRMATION` | Check authoritative filing/source confirmation. | SEC pending and check due. | Yes | Yes. | Can coexist with core ready. |
| `BACKFILL_HISTORICAL` | Controlled historical data-debt handling. | Historical Q materially blocks downstream calculations. | No as historical action. | Yes. | Can coexist with settled lifecycle. |
| `MANUAL_REVIEW` | Human review needed. | Ambiguous identity, unsupported conflict, or policy limitation. | Yes | Yes. | May coexist with any non-terminal uncertainty. |
| `NOT_DERIVABLE` | Current evidence cannot determine the action. | Missing evidence. | Yes | Yes. | Use only when needed. |

Avoid generic `WAITING`. Use exact scheduling conditions instead:

- waiting for expected date: `CHECK_EXPECTED_RESULT` plus expected date
- waiting for provider retry: `RETRY_PROVIDER` plus `next_retry_at`
- waiting for SEC: `SEC_CONFIRMATION_STATE=PENDING` plus due/not-due schedule

## Historical Backfill

`NEEDS_HISTORICAL_BACKFILL` is a Q-level historical data-debt concept.

A historical Q should require backfill only when missing historical data materially prevents intended
downstream calculations, such as:

- TTM
- growth
- EBITDA margin trend
- consistency
- dilution
- leverage
- score readiness

Do not automatically treat missing optional EBIT, OCF, or capex as backfill debt unless a validated
downstream consumer requires those fields.

Company-level summary:

```text
HAS_HISTORICAL_BACKFILL_DEBT=true
```

means one or more historical Qs for that company have material backfill debt.

There is no persistent `historical_backfill_actionable` state in the agreed model. Actionability is
derived through `NEXT_ACTION` or a controlled backfill planner.

## Profile / Support

Profile support is separate from readiness.

| Profile support state | Meaning |
| --- | --- |
| `ORDINARY_SUPPORTED` | Ordinary-company fields and readiness rules apply. |
| `BANK_PROFILE` | Bank-specific model/rules are required. |
| `INSURANCE_PROFILE` | Insurance-specific model/rules are required. |
| `UNSUPPORTED_PROFILE` | Known profile is not supported by the current downstream consumer. |
| `UNKNOWN_PROFILE` | Insufficient profile evidence. |

Do not call a bank `SCORE_NOT_READY` merely because the ordinary scoring model does not apply. That
is unsupported or profile-specific, not incomplete ordinary data.

## Event Catalogue

An event happens at a point in time. A state describes what is true now.

| Event | Plain-language meaning | Scope | Affected dimension | Reconstructable today? | Persist history for reliable today metrics? |
| --- | --- | --- | --- | --- | --- |
| `CALENDAR_UPDATED` | Calendar estimate/status changed. | Company/ticker | Event timing, action schedule | Partially | Yes |
| `RESULT_EXPECTED_DATE_REACHED` | The expected result date is now due. | Run/event | Action scheduling | Current snapshot only | Yes |
| `NEW_Q_RESULT_DETECTED` | A new quarterly result was observed. | Any Q/latest context | Lifecycle, action | Partially | Yes |
| `Q_CREATED` | A canonical Q row was created. | Any Q | Lifecycle | Partially; stronger in V2 | Useful |
| `Q_ENRICHED` | Additional fields/evidence were added. | Any Q | Field/provenance readiness | Partially via provenance | Useful |
| `CORE_FIELDS_BECAME_READY` | Required core fields changed from not ready to ready. | Any Q | Core readiness | Current snapshot only | Yes |
| `SEC_CONFIRMATION_RECEIVED` | Authoritative confirmation arrived. | Any Q | Assurance | Partially | Yes |
| `SCORE_BECAME_READY` | Score dependencies became comparable-ready. | Company window | Downstream readiness | Current snapshot only | Yes |
| `Q_BECAME_HISTORICAL` | A newer Q became latest. | Any Q | Role | Yes from ordering | Usually no |
| `BACKFILL_COMPLETED` | Historical data debt was resolved. | Any Q/company | Historical backfill | Partially | Yes |

## Event vs State Examples

| Event | Current state after the event |
| --- | --- |
| `CORE_FIELDS_BECAME_READY` | `Q_CORE_FIELDS_READY=true` |
| `SEC_CONFIRMATION_RECEIVED` | `SEC_CONFIRMATION_STATE=CONFIRMED` |
| `NEW_Q_RESULT_DETECTED` | `Q_RESULT_LIFECYCLE=RESULT_DETECTED` or later |
| `Q_BECAME_HISTORICAL` | `Q_ROLE=HISTORICAL_Q` |
| `BACKFILL_COMPLETED` | `HISTORICAL_BACKFILL_STATE=NO_HISTORICAL_BACKFILL_DEBT` |

If the system did not persist the event time, it may still know the current state but not know
whether the transition happened today.

## Allowed Combinations

| Combination | Why it is valid |
| --- | --- |
| core ready + SEC pending | Core data can be available before authoritative confirmation. |
| core ready + score not ready | Score readiness depends on a multi-quarter history/window. |
| historical Q + backfill required | Older quarters can still block historical calculations. |
| SEC confirmed + core incomplete | Source assurance and field completeness are separate dimensions. |
| operationally settled + score not ready | No immediate Q workflow action may be due even if downstream readiness is false. |
| core ready + optional enrichment still possible | Optional fields/provenance can arrive after required core fields. |
| retry action + otherwise operationally usable Q | Retry may concern SEC, provenance, or enrichment rather than core usability. |

## Impossible or Contradictory Combinations

Validated contradictions:

- `Q_CORE_FIELDS_READY=true` while a required core field is NULL or `shares_outstanding <= 0`.
- `SCORE_READY=true` for an unsupported profile under ordinary score rules.
- `Q_ROLE=FUTURE_EXPECTED_Q` while the same Q is already detected or canonicalized.
- A resolved followup should not remain active.
- An inactive followup should not retain retry-required semantics.
- `SEC_CONFIRMATION_STATE=CONFIRMED` and `SEC_CONFIRMATION_STATE=NOT_YET_EXPECTED` at the same time.

Validated non-contradictions:

- SEC confirmed with core incomplete.
- Core ready with score not ready.
- Operationally settled with historical backfill debt.

## What Does "Done" Mean?

Do not use `done` alone. It is ambiguous.

| Term | Meaning |
| --- | --- |
| `RESULT_DETECTED` | The result exists and has been recognized. |
| `INGESTION_COMPLETE` | Current ingestion policy has produced a complete-enough row under that system's ingestion rules. `TERM_REQUIRES_FINAL_IMPLEMENTATION_CONTRACT` before using as canonical synonym. |
| `CORE_READY` | Required ordinary core fields exist under `Q_CORE_FIELDS_READY`. |
| `SEC_CONFIRMED` | SEC or equivalent authoritative assurance has been obtained. |
| `SCORE_READY` | Required historical/window data exists for a comparable score. |
| `OPERATIONALLY_SETTLED` | No normal immediate work remains in the Q-result workflow. |
| `HISTORICALLY_COMPLETE` | No material historical backfill debt remains under agreed requirements. |

## Latest-Q Example

```text
Identity:            usa / XYZ / 2026 / Q2
Role:                LATEST_OPERATIONAL_Q
Lifecycle:           OPERATIONALLY_SETTLED
Q core fields:       READY
SEC confirmation:    PENDING
Score readiness:     NOT_READY
Next action:         CHECK_SEC_CONFIRMATION later, not currently due
Historical backfill: company has older Q debt
Profile:             ORDINARY_SUPPORTED
```

This is coherent because the latest Q has the required fields and no immediate standard workflow
work is due. SEC assurance can still be pending, and score readiness can be blocked by older
historical data.

## Historical-Q Example

```text
Identity:            usa / ABC / 2024 / Q3
Role:                HISTORICAL_Q
Lifecycle:           OPERATIONALLY_SETTLED
Q core fields:       NOT_READY
SEC confirmation:    CONFIRMED
Historical backfill: NEEDS_HISTORICAL_BACKFILL
Next action:         BACKFILL_HISTORICAL under controlled backfill plan
Profile:             ORDINARY_SUPPORTED
```

This is different from a latest-Q retry problem. The daily new-result workflow should not treat this
as a newly detected latest quarter. It is historical data debt.

## Complete Canonical State Catalogue

| Canonical name | Type | Allowed values | Scope | Mutually exclusive? | Can coexist with | Plain-language meaning | Technical meaning | Latest Q? | Historical Q? | Persistence recommendation |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Q_RESULT_LIFECYCLE` | LIFECYCLE | `EXPECTED`, `RESULT_DETECTED`, `CANONICALIZED`, `INGESTING`, `OPERATIONALLY_SETTLED`, `REOPENED` | Any Q plus event context for expected | Yes within dimension | All parallel dimensions | Where the Q is in the standard result flow | Sequential lifecycle only | Yes | Yes | Derive initially |
| `Q_ROLE` | ROLE | `FUTURE_EXPECTED_Q`, `LATEST_OPERATIONAL_Q`, `HISTORICAL_Q` | Contextual | Yes within dimension | Lifecycle/readiness/action | Whether the Q is future, latest, or historical | Derived from ordering and decision date | Yes | Yes | Derive |
| `Q_CORE_FIELDS_READY` | READINESS | `true`, `false`, `not_applicable`, `not_derivable` | Any Q | Yes within dimension | SEC pending, score false, enrichment incomplete | Required ordinary fields exist | Six-field positive-share rule | Yes | Yes | Derive |
| `FIELD_COMPLETENESS_LAYER` | READINESS | `CORE_REQUIRED`, `DOWNSTREAM_HISTORY_REQUIRED`, `OPTIONAL_ENRICHMENT`, `PROVENANCE_ONLY` | Any Q/field | No | Core readiness/action/provenance | Which fields matter to which consumers | Static field classification | Yes | Yes | Static helper |
| `DOWNSTREAM_READINESS` | READINESS | `TTM_READY`, `SCORE_READY`, `VALUATION_READY` booleans | Company/window or latest-context | No | Q core ready true/false | Whether consumers can run comparably | Derived from multi-quarter/window inputs | Yes | As contributor | Derive |
| `SEC_CONFIRMATION_STATE` | ASSURANCE | `NOT_APPLICABLE`, `NOT_YET_EXPECTED`, `PENDING`, `CHECKED_NO_EVIDENCE`, `PARTIAL_EVIDENCE`, `CONFIRMED`, `UNSUPPORTED`, `ERROR_RETRY`, `NOT_DERIVABLE` | Any Q | Yes within dimension | Core ready true/false | Authoritative source assurance | SEC/equivalent confirmation state | Yes | Yes | Derive; event history later if needed |
| `PROVIDER_PROVENANCE_STATE` | PROVENANCE | Provider-specific facts plus small aggregates | Any Q/run event | No | All dimensions | Where data came from and provider outcomes | Field source, fetch state, cache/backoff/error | Yes | Yes | Persist facts, derive aggregates |
| `NEXT_ACTION` | ACTION | `NO_ACTION`, `CHECK_EXPECTED_RESULT`, `FETCH_NEW_RESULT`, `ENRICH_Q`, `RETRY_PROVIDER`, `CHECK_SEC_CONFIRMATION`, `BACKFILL_HISTORICAL`, `MANUAL_REVIEW`, `NOT_DERIVABLE` | Any Q/run event | Not necessarily | Readiness/assurance/provider states | What the system should do next | Selector/followup/planner output | Yes | Yes, controlled actions only | Derive; persist queues where needed |
| `HISTORICAL_BACKFILL_STATE` | BACKFILL | `NOT_HISTORICAL`, `NO_HISTORICAL_BACKFILL_DEBT`, `NEEDS_HISTORICAL_BACKFILL`, `NOT_DERIVABLE` | Historical Q plus company summary | Yes within dimension | Settled lifecycle, core false | Whether older Q gaps block historical consumers | Derived historical data debt | Not as historical debt | Yes | Derive |
| `PROFILE_SUPPORT_STATE` | SUPPORT | `ORDINARY_SUPPORTED`, `BANK_PROFILE`, `INSURANCE_PROFILE`, `UNSUPPORTED_PROFILE`, `UNKNOWN_PROFILE` | Company/ticker | Yes within dimension | All dimensions | Which rules apply | Accounting/profile support gate | Yes | Yes | Persist profile, derive support label |
| Canonical events | EVENT | See event catalogue | Point in time | No | States they change | Something happened | Transition/run metric | Yes | Yes | Persist only when reliable daily metrics require it |

## How Legacy Maps to the Canonical Model

Legacy database: `fundamentals_usa.db`.

Good or usable mappings:

- `rc_fundamental_quarterly` maps to canonical Q content.
- `rc_fundamental_quarter_ingestion_status.ingestion_status` approximates ingestion lifecycle.
- `source_confirmation_status` maps to SEC/assurance states.
- `retry_recommendation` and Check decisions map to `NEXT_ACTION`.
- `rc_fundamental_ttm` contributes to downstream readiness.

Approximate or overloaded mappings:

- `quarter_basic_complete` mixes Legacy completeness policy with readiness-like meaning.
- `score_history_complete` can be stale relative to the current EBITDA-based score requirement.
- many historical rows are `UNKNOWN_HISTORICAL_INGEST_COMPLETENESS`; this is not automatically a
  latest-Q lifecycle problem.
- Legacy quarterly/status tables store `period_end_date` but do not have explicit `fiscal_year` and
  `fiscal_quarter` columns. Therefore Legacy alone cannot always prove reported fiscal-period
  identity for non-calendar fiscal-year companies without additional provider or match evidence.

Important warning:

`quarter_basic_complete` is not `Q_CORE_FIELDS_READY`. Legacy basic completeness allows broader
semantics, including EBIT fallback, OCF+capex as FCF support, and shares being present without the
canonical positive-share rule.

Fiscal identity warning:

Some current Legacy-facing operational code derives work-unit fiscal year/quarter from
`period_end_date` when building current work-unit identity. That is a calendar-date inference and
must be treated as an operational approximation, not the canonical identity rule. For non-calendar
fiscal-year companies, a reliable reported fiscal identity should come from provider fiscal labels,
V2 quarter identity, or an explicit mapping, not from calendar quarter inference.

Legacy lacks sufficient evidence for:

- V2-style company profile support unless inferred externally
- complete provider-level provenance for all historical rows
- reliable event history for "became ready today" metrics
- consistently reported fiscal year/quarter on every quarterly row

## How V2 Maps to the Canonical Model

V2 database: `rc_fundamentals_v2.db`.

Good or usable mappings:

- `rc_v2_company` maps to company/profile support.
- `rc_v2_quarter` maps to canonical Q identity/structure through `fiscal_year`,
  `fiscal_period`, and `report_date`.
- `rc_v2_fundamental_quarterly` maps to ordinary canonical Q fields.
- bank and insurance tables separate profile-specific content.
- `rc_v2_fundamental_field_source` provides field-level provenance.
- `rc_v2_operational_followup` maps to retry/action/scheduling state.
- provider fetch-state tables map to provider timing/cache/backoff/error evidence.

Important warning:

Current V2 preflight core semantics are not `Q_CORE_FIELDS_READY`. V2 preflight core currently uses
`revenue`, `ebitda`, `free_cashflow`, and `shares_outstanding`. The canonical rule also requires
`cash`, `total_debt`, and positive shares.

Fiscal identity warning:

V2 is structurally stronger than Legacy because it stores `fiscal_year`, `fiscal_period`, and
`report_date` separately. However, cross-provider imports must still respect provider fiscal-label
quality. Date-only provider rows may be matched by period-date tolerance only when that relaxed path
is explicitly allowed and recorded as inferred, not fiscal-verified.

V2 is stronger than Legacy for:

- company-scoped identity
- explicit reported fiscal-period columns plus separate report date
- profile support
- field-level provenance
- operational followup state
- NULL-fill conflict preservation

V2 is weaker or insufficient alone for:

- Legacy TTM and score-readiness evidence
- SEC confirmation as an explicit quarter-level status
- full historical readiness without derived cross-system/window logic

## Legacy vs V2 Terminology Warnings

| Do not confuse | System | Meaning | Why it differs from canonical |
| --- | --- | --- | --- |
| `quarter_basic_complete` | Legacy | Legacy basic completeness policy. | Broader than `Q_CORE_FIELDS_READY`; not positive-share strict and allows fallback semantics. |
| `core_complete` / `core_complete_after` | V2 | V2 selected-work-unit core under current preflight field set. | Uses a four-field rule, not the six-field canonical rule. |
| `Q_CORE_FIELDS_READY` | Canonical | Agreed ordinary six-field positive-share readiness. | Derived concept; not an existing Legacy/V2 persisted status. |
| `score_history_complete` | Legacy | Persisted score-history readiness flag. | May not reflect current EBITDA-based score readiness without recomputation. |
| `NOOP_CORE_CURRENT` | V2 | No V2 provider work because V2 preflight core is current. | Does not imply canonical core ready, SEC confirmed, or score ready. |
| `period_end_date` | Legacy/V2/provider metadata | Fiscal period end date or provider period date. | Supports matching; must not replace reported fiscal year/quarter identity. |
| date-inferred fiscal match | V2/provider matching | Explicit relaxed match from date-only provider data. | Inferred evidence, not fiscal-verified identity. |
| `OPERATIONALLY_SETTLED` | Canonical | No immediate standard workflow action. | Does not mean done in every dimension. |
| `complete` / `done` | Ambiguous | Informal shorthand. | Must be qualified as core ready, SEC confirmed, score ready, etc. |

## Check for Updates Terminology

| Concept | Canonical classification | Notes |
| --- | --- | --- |
| Calendar updated | Event or state snapshot | Calendar/provider maintenance; not Q completion. |
| New Q result detected | Event and transition | Usually `EXPECTED -> RESULT_DETECTED`; can produce `FETCH_NEW_RESULT`. |
| Q enriched | Event | Fields/provenance added; may or may not affect core readiness. |
| Q core ready | State snapshot or transition if newly ready | `Q_CORE_FIELDS_READY=true`; transition event is `CORE_FIELDS_BECAME_READY`. |
| Waiting for SEC confirmation | Assurance state plus scheduling | Prefer `SEC_CONFIRMATION_STATE=PENDING`; avoid vague waiting status. |
| SEC confirmation received | Event and state | Event `SEC_CONFIRMATION_RECEIVED`; state `SEC_CONFIRMATION_STATE=CONFIRMED`. |
| Retry/action work required | Action count | `NEXT_ACTION=RETRY_PROVIDER`, `ENRICH_Q`, `CHECK_SEC_CONFIRMATION`, etc. |
| Historical backfill debt | Historical state snapshot | Separate from latest-result Check counts. |

## Update Fundamentals Terminology

Update reporting should separate:

1. what happened during the run
2. what is true after the run

| During-run event | Post-run state |
| --- | --- |
| `Q_CREATED` | `Q_RESULT_LIFECYCLE=CANONICALIZED` or later |
| `Q_ENRICHED` | Field/provenance state changed |
| `CORE_FIELDS_BECAME_READY` | `Q_CORE_FIELDS_READY=true` |
| `SEC_CONFIRMATION_RECEIVED` | `SEC_CONFIRMATION_STATE=CONFIRMED` |
| `BACKFILL_COMPLETED` | `HISTORICAL_BACKFILL_STATE=NO_HISTORICAL_BACKFILL_DEBT` |
| provider retry remains | `NEXT_ACTION=RETRY_PROVIDER` plus retry schedule |

A successful update can still end with SEC pending, score not ready, or historical backfill debt.

## ASCII State Diagram

```text
Q IDENTITY
market + ticker + fiscal_year + fiscal_quarter
   |
   v

ROLE
FUTURE_EXPECTED_Q -> LATEST_OPERATIONAL_Q -> HISTORICAL_Q
(role is contextual; it does not rewrite lifecycle)

SEQUENTIAL Q_RESULT_LIFECYCLE

EXPECTED
   |
   v
RESULT_DETECTED
   |
   v
CANONICALIZED
   |
   v
INGESTING
   |
   v
OPERATIONALLY_SETTLED
   ^                 |
   |                 v
   +----------- REOPENED

PARALLEL DIMENSIONS

CORE:       Q_CORE_FIELDS_READY=false | true | not_applicable | not_derivable
FIELDS:     CORE_REQUIRED | DOWNSTREAM_HISTORY_REQUIRED | OPTIONAL_ENRICHMENT | PROVENANCE_ONLY
SEC:        NOT_APPLICABLE | NOT_YET_EXPECTED | PENDING | CHECKED_NO_EVIDENCE
            PARTIAL_EVIDENCE | CONFIRMED | UNSUPPORTED | ERROR_RETRY | NOT_DERIVABLE
ACTION:     NO_ACTION | CHECK_EXPECTED_RESULT | FETCH_NEW_RESULT | ENRICH_Q
            RETRY_PROVIDER | CHECK_SEC_CONFIRMATION | BACKFILL_HISTORICAL
            MANUAL_REVIEW | NOT_DERIVABLE
PROVIDER:   observations | cache | no-data | error | retry/backoff | provenance rows
BACKFILL:   NOT_HISTORICAL | NO_HISTORICAL_BACKFILL_DEBT
            NEEDS_HISTORICAL_BACKFILL | NOT_DERIVABLE
PROFILE:    ORDINARY_SUPPORTED | BANK_PROFILE | INSURANCE_PROFILE
            UNSUPPORTED_PROFILE | UNKNOWN_PROFILE
TTM/SCORE:  company-window readiness, not one-Q lifecycle
```

## Glossary

| Term | Definition |
| --- | --- |
| Action | What the system should do next. |
| Assurance | Evidence that an authoritative source confirms the Q. |
| Backfill | Controlled work to fill historical data debt. |
| Canonical Q | One quarterly result identified by market, ticker, fiscal year, and fiscal quarter. |
| Core | Required ordinary fields for canonical readiness. |
| Enrichment | Useful additional fields or evidence beyond core readiness. |
| Event | Something that happened at a point in time. |
| Historical Q | A Q older than the latest operational Q for that ticker/company. |
| Lifecycle | The small sequential progress model for a Q result. |
| Latest Q | The current newest operational Q for a ticker/company. |
| Operationally settled | No normal immediate Q-result workflow action is pending. |
| Provenance | Evidence describing where data came from and how it was transformed. |
| Readiness | Whether a Q or company window has enough data for a specific consumer. |
| Retry | A next action caused by provider, confirmation, or enrichment work that may be attempted later. |
| Role | Contextual position of a Q as future, latest, or historical. |
| Score ready | A supported company window has enough comparable data for score calculation. |
| TTM ready | A company window has enough quarters for trailing-twelve-month calculations. |

## Open Questions / Implementation Decisions Still Not Locked

Conceptual model already decided:

- Do not use one giant status enum.
- Keep lifecycle, role, readiness, assurance, action, provider/provenance, historical debt, and
  profile support separate.
- `Q_CORE_FIELDS_READY` is the six-field positive-share rule for ordinary companies.
- `LATEST_Q_CORE_READY` is `Q_CORE_FIELDS_READY` on the current latest operational Q.
- `SCORE_READY` and `TTM_READY` are company-window readiness concepts.
- There is no persistent `historical_backfill_actionable` state in the agreed model.

Implementation details still open:

- Whether `REOPENED` is persisted as a lifecycle state or represented as a transition marker back to
  `INGESTING`.
- Which events need durable event-history persistence for reliable daily "today" metrics.
- Exact persistence strategy for canonical derived helper output, if any.
- How much SEC/equivalent assurance can be derived from V2 provenance without a new explicit
  quarter-level confirmation projection.
- Final implementation contract for `INGESTION_COMPLETE` if that term is retained.
- How Legacy should obtain reported fiscal year/quarter for rows that currently expose only
  `period_end_date`.
- Where current date-derived work-unit identity should be replaced or guarded for non-calendar
  fiscal-year companies.

## Authoritative Terminology

| Preferred canonical term | Use for |
| --- | --- |
| `Q_RESULT_LIFECYCLE` | Sequential result lifecycle. |
| `Q_ROLE` | Future/latest/historical context. |
| `Q_CORE_FIELDS_READY` | Six-field ordinary core readiness for any Q. |
| `LATEST_Q_CORE_READY` | `Q_CORE_FIELDS_READY` on the latest operational Q. |
| reported fiscal year/quarter | The company's reported fiscal period identity. |
| period-end date | Metadata/evidence attached to a Q, not a substitute for fiscal identity. |
| `SEC_CONFIRMATION_STATE` | Authoritative source assurance. |
| `NEXT_ACTION` | What the system should do next. |
| `HISTORICAL_BACKFILL_STATE` | Historical data debt. |
| `PROFILE_SUPPORT_STATE` | Ordinary/bank/insurance/support gate. |
| `CORE_FIELDS_BECAME_READY` | Event where core readiness changed to true. |

| Terms to avoid or qualify | Why |
| --- | --- |
| `complete` | Ambiguous: could mean ingestion, core, SEC, score, or historical completeness. |
| `done` | Ambiguous and should not appear alone in reports/specs. |
| `basic complete` | Legacy-specific; not canonical core readiness. |
| `core complete` | V2-specific unless qualified; current V2 core differs from canonical core. |
| `waiting` | Too vague; use pending state plus due/retry timestamp. |
| `score not ready` for unsupported profiles | Unsupported is not the same as incomplete. |
