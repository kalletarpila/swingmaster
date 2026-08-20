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
Due actions:  none
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

## Calendar Comparison Period

`CALENDAR_COMPARISON_PERIOD` is analytical period metadata attached to a canonical Q where it can be
derived reliably. It is not part of canonical Q identity.

Authoritative rule:

> Fiscal identity and calendar comparison period serve different purposes and must never replace each
> other. Fiscal year and fiscal quarter identify the company's reported result. Calendar comparison
> period is a derived analytical alignment used to compare companies whose fiscal calendars differ
> but whose reported periods substantially cover the same calendar/seasonal economic period.

Until reliable actual period-start dates are available, the calendar comparison period is derived
from an approximate three-calendar-month interval ending at the reported period end date. This
approximation is intentionally used for analytical peer alignment and must not be represented as the
company's actual fiscal-period start date.

### Purpose

`CALENDAR_COMPARISON_PERIOD` aligns quarterly results from companies with different fiscal calendars
to the same underlying calendar or seasonal economic period for cross-company comparison.

It is for questions such as:

- peer comparison
- cross-sectional industry comparison
- same-season comparison
- "which companies mostly reported the calendar Q4 2025 economic period?"

It is not for identifying the Q itself.

### Two Different Comparison Modes

| Analytical question | Use | Why |
| --- | --- | --- |
| Company's own historical comparison, such as `XYZ FY2026 Q2` vs `XYZ FY2025 Q2` | `fiscal_year + fiscal_quarter` | Preserves the company's own fiscal reporting cycle and seasonality. |
| Cross-company same-economic-period comparison, such as companies mostly covering calendar Q4 2025 | `CALENDAR_COMPARISON_PERIOD` | Aligns different fiscal calendars to a shared calendar/seasonal period. |

Do not silently replace company-specific YoY growth, margin trend, dilution history, TTM, score, or
existing fiscal-period logic with calendar-comparison periods. This document defines the concept; it
does not change scoring or TTM behavior.

### Recommended Definition

Calendar comparison is conceptually based on the reporting-period date range. The current practical
method uses an approximate date range because the current production databases do not reliably store
actual fiscal-period start dates.

Current practical method:

```text
derived_period_start_date = period_end_date - 3 calendar months
```

`derived_period_start_date` is not the company's actual reported fiscal-period start date. It is an
analytical approximation used only to derive calendar comparison alignment.

When subtracting three calendar months, preserve the day-of-month where possible. If the target
month does not have that day, clamp to the target month's last valid day.

Given the approximate interval:

```text
CALENDAR_COMPARISON_PERIOD =
  the calendar quarter with the maximum number of overlapped days
  in derived_period_start_date -> period_end_date
```

Recommended fields:

```text
period_end_date
derived_period_start_date
calendar_comparison_year
calendar_comparison_quarter
calendar_comparison_method
calendar_comparison_quality
```

Avoid the shorter name `calendar_quarter`; it can be misread as the calendar quarter containing
`period_end_date`.

Recommended method value for the current practical method:

```text
calendar_comparison_method = APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END
```

Future higher-quality method:

```text
calendar_comparison_method = ACTUAL_PERIOD_RANGE
```

may supersede the approximation for rows where reliable actual period start and end dates become
available.

### Derivation Rule

For one reported fiscal quarter:

1. compute `derived_period_start_date = period_end_date - 3 calendar months`
2. determine all calendar quarters overlapped by `derived_period_start_date -> period_end_date`
3. count approximate-interval days inside each calendar quarter
4. choose the calendar quarter with the largest overlap
5. if there is an exact tie, use the calendar quarter containing the midpoint of the approximate
   interval
6. if still unresolved, set `calendar_comparison_quality=AMBIGUOUS` and do not silently choose

If `period_end_date` is unavailable or invalid:

```text
CALENDAR_COMPARISON_PERIOD = UNKNOWN
calendar_comparison_quality = INSUFFICIENT_DATES
```

Do not invent calendar comparison values from fiscal labels alone.

Higher-quality future rule:

1. determine all calendar quarters overlapped by actual `period_start_date -> period_end_date`
2. count actual reporting-period days inside each calendar quarter
3. choose the calendar quarter with the largest overlap
4. if there is an exact tie, use the period midpoint's calendar quarter
5. if still ambiguous, set `calendar_comparison_quality=AMBIGUOUS` and do not silently choose

Recommended quality values:

| Quality | Meaning |
| --- | --- |
| `APPROX_OVERLAP` | The approximate three-calendar-month interval has a clear maximum-overlap calendar quarter. This does not claim actual fiscal-period exactness. |
| `ACTUAL_RANGE_OVERLAP` | A validated actual period range has a clear maximum-overlap calendar quarter. |
| `AMBIGUOUS` | Overlap does not produce a safe deterministic comparison period. |
| `INSUFFICIENT_DATES` | Start/end date evidence is missing or unreliable. |
| `IRREGULAR_ACTUAL_PERIOD` | Actual-period evidence shows an unusually short, long, transition/stub, 14-week, or 53-week period. |

### Why Period-End Date Alone Is Not Enough

Do not define calendar comparison period as "the calendar quarter containing `period_end_date`."

Example:

```text
fiscal_year     = 2026
fiscal_quarter  = Q2
period_end_date = 2026-01-31
derived_start   = 2025-10-31
```

A period-end-only rule would classify this as `2026 Q1`. The approximate maximum-overlap rule
classifies it as `2025 Q4`, because most of the approximate interval falls in calendar Q4 2025.

The fiscal identity remains `FY2026 Q2`.

Do not confuse:

```text
FY2026 Q2   = canonical fiscal identity
2025 Q4     = calendar comparison period
2026-01-31  = period end date
2025-10-31  = derived analytical start date
```

These are four different concepts.

### Examples

| Example | Fiscal identity | Reporting period | Calendar comparison period | Explanation |
| --- | --- | --- | --- | --- |
| Normal calendar-aligned company | `FY2025 Q4` | actual `2025-10-01 -> 2025-12-31`; approximate `2025-09-30 -> 2025-12-31` | `2025 Q4` | Fiscal quarter and calendar season align; the approximation still selects Q4. |
| Shifted fiscal calendar | `FY2026 Q2` | period end `2026-01-31`; derived analytical start `2025-10-31` | `2025 Q4` | Most approximate-interval days are in calendar Q4 2025; fiscal identity remains FY2026 Q2. |
| Current-data approximation | Example V2 rows such as `AAP FY2024 Q1 report_date=2024-04-30` | actual start not stored; derived analytical start `2024-01-30` | derived by maximum overlap from the approximate interval | V2 proves fiscal period can differ from report-date calendar quarter; the comparison period is analytical metadata, not identity. |

### Current Derivability in Legacy and V2

Read-only verification on 2026-08-20 found:

| System | Fiscal labels | Period end/report date | Period start date | Reliable max-overlap derivability | Notes |
| --- | --- | --- | --- | --- | --- |
| Legacy `fundamentals_usa.db` | Not stored on `rc_fundamental_quarterly` or ingestion status rows. Provider observation content may have fiscal labels for some observations. | `period_end_date` exists on `156094/156094` quarterly rows. | No explicit actual start-date column found in the current quarterly/status/provenance tables inspected. | Approximate method can be applied where `period_end_date` is valid. | Period-end-only calendar-quarter classification remains unsafe. Legacy fiscal identity remains ambiguous without additional evidence. |
| V2 `rc_fundamentals_v2.db` | `rc_v2_quarter.fiscal_year` and `fiscal_period` are explicit. | `rc_v2_quarter.report_date` exists on `85424/85424` quarter rows; `64910` belong to active companies. Code verification shows this is populated from SimFin `Report Date` and mapped by the V2 bridge as logical period end; `publish_date` is populated separately from SimFin `Publish Date`. | No explicit actual start-date column found in V2 tables. | Approximate method can be applied where SimFin `Report Date` is valid as period-end-like evidence. | `14108` V2 rows have fiscal label different from the calendar year/quarter of `report_date`, proving calendar-date substitution is unsafe. Groups with the same fiscal label but multiple report dates need fiscal-period-end validation, not publication-date treatment. |

Because reliable actual start dates are not currently present in the primary Legacy or V2 quarter
tables, the current agreed practical method is:

```text
derived_period_start_date = period_end_date - 3 calendar months
calendar_comparison_method = APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END
```

Rows with missing or invalid period-end evidence remain `UNKNOWN` with `INSUFFICIENT_DATES`.

### Legacy/V2 Comparison

For matching canonical Q identities, Legacy/V2 comparison of `CALENDAR_COMPARISON_PERIOD` can be
defined only after applying the same documented method to each side:

- Legacy can use valid `period_end_date` with the approximation.
- V2 can use valid `report_date` as period-end-like evidence with the approximation.
- Actual-period-range agreement cannot be evaluated today because neither primary store has actual
  period start.

No disagreement should be resolved by choosing the calendar quarter containing period end silently.

### Relationship to the State Model

`CALENDAR_COMPARISON_PERIOD` is analytical period metadata. It is not:

- lifecycle state
- readiness state
- SEC state
- action state
- provider state

It is analytical period metadata with scope `ANY_Q` where derivable.

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

Expected future results should be persisted in a result-calendar or expected-result structure, not
as fake canonical Q rows. A canonical Q row starts when a result is detected or when a
migration/import has enough fiscal identity evidence to create the reported fiscal-period row.
`REOPENED` is represented as an event/transition back to active enrichment, not as a persisted
lifecycle value.

| Canonical name | Plain-language description | Precise meaning | Entry condition | Exit condition | Normal next state | Can be skipped? | Can be revisited? | Applies to | Does not imply |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `RESULT_DETECTED` | The system has seen evidence that the Q exists. | A result event, provider source period, or migration source indicates that the reported fiscal Q exists, but no normal automatic enrichment state has been decided yet. | New result event/source period detected, or migration/import has sufficient fiscal identity evidence. | Work is due, or no work is currently due. | `ENRICHING` or `OPERATIONALLY_SETTLED` | Yes, if historical data is imported directly into an active or settled state. | Yes, if a settled Q is later re-detected through new evidence. | Any Q, especially latest/current operations. | Core ready, SEC confirmed, score ready, or canonical value completeness. |
| `ENRICHING` | Automatic work is active or due. | One or more normal automatic Q-level tasks are currently active or due: initial acquisition, provider enrichment, safe NULL-fill, provider retry, or due SEC confirmation check. | Work is selected, retry is due, or followup is active. | Work succeeds, creates a durable resolution issue, or no eligible automatic work remains. | `OPERATIONALLY_SETTLED` | Yes, if no work is needed. | Yes. | Any Q; normal new-result enrichment is latest/current. | Core readiness, score readiness, SEC confirmation, or absence of manual review issues. |
| `OPERATIONALLY_SETTLED` | No normal immediate work is pending. | No useful normal automatic Q-result work is currently due under current policy. | No due automatic action remains. | New evidence, retry due, correction, or backfill selection reopens the Q. | Stable until `Q_REOPENED` event returns it to `ENRICHING`. | Yes. | Yes. | Any Q. | Core ready, SEC confirmed, score ready, historically complete, all providers succeeded, or every optional field exists. |

The following terms are intentionally not persisted Q lifecycle states:

| Term | Classification | Reason |
| --- | --- | --- |
| `EXPECTED` | Result-calendar / scheduling state | A future expected result may exist before the canonical Q exists. |
| `INITIAL_DATA_ACQUIRED` | Event or derived condition | It is derived from provider acquisition and canonical field presence. |
| `CANONICALIZED` | Storage milestone / event | It says a row or mapping exists, not where the Q is in operational progression. |
| `RECONCILING` | Transient processing phase | Durable unresolved reconciliation creates a resolution issue and possibly `OPERATIONAL_ACTION=MANUAL_REVIEW`. |
| `REOPENED` / `Q_REOPENED` | Event / transition | A settled Q moves back to `ENRICHING`; the event is useful, the state is not. |
| `BLOCKED_REVIEW` / `MANUAL_REVIEW` | Action or resolution issue | Review need is orthogonal to lifecycle and may coexist with settled or enriching Qs. |

### Reopened Semantics

`REOPENED` exists because quarterly data can change after it looked settled. Examples:

- a provider returns new evidence
- a correction is accepted
- a new enrichment source becomes available
- a historical backfill phase selects the Q
- a previously failed provider path becomes retryable

The current architecture represents `REOPENED` as an event/transition back to active processing, not
as a persisted lifecycle state. Other implementations may still expose the concept in reporting, but
they should avoid storing it as a separate stable state unless there is a durable operational reason.

## Q Role

`Q_ROLE` is contextual. It is not the lifecycle.

| Role | Meaning | Scope | Persistence recommendation |
| --- | --- | --- | --- |
| `LATEST_OPERATIONAL_Q` | The newest operational quarter for a ticker/company under the current decision date. | Latest Q only. | Derive from quarter ordering. |
| `HISTORICAL_Q` | A canonical/detected Q older than the current latest operational Q. | Historical Q. | Derive from quarter ordering. |

The role values are mutually exclusive for a given Q at a given decision time.

A future expected result belongs to result-calendar or expected-result scheduling state. It is not a
role of an existing canonical Q row.

A Q can move from `LATEST_OPERATIONAL_Q` to `HISTORICAL_Q` when a newer result becomes the
operational latest quarter. That role change does not automatically change lifecycle, core readiness,
SEC confirmation, provenance, or historical backfill state.

## Core Field Readiness

`Q_CORE_FIELDS_READY` applies to any Q for supported ordinary-company semantics.

For an `ORDINARY` company, the rule is:

- valid Q identity
- `revenue` present
- `ebitda` present
- accepted canonical `free_cashflow` present
- `cash` present
- `total_debt` present
- `shares_outstanding` present and greater than zero

`Q_CORE_FIELDS_READY` is source-agnostic. It tests canonical data availability and semantics, not
provider source, direct-vs-derived origin, SEC confirmation, or provenance richness. Accepted
canonical values from approved deterministic derivations, such as `free_cashflow = operating_cashflow
+ capex` under the established negative-capex convention, satisfy readiness the same way valid direct
provider values do. The same principle applies to approved EBITDA and total-debt derivations.

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
- retry/backoff metadata
- cache hit
- source file/hash
- validation tier
- transformation

Provider outcomes generally must not become Q lifecycle states. A provider error may create
`OPERATIONAL_ACTION=RETRY_PROVIDER`; it does not mean the Q lifecycle itself is `ERROR`.

Provider acquisition outcome is a pure provider-result dimension:

| Provider acquisition result | Meaning |
| --- | --- |
| `NOT_CHECKED` | Provider has not been checked for this Q/target. |
| `ACQUIRED` | Provider supplied usable data. |
| `PARTIAL` | Provider supplied usable partial data. |
| `NO_DATA` | Provider checked successfully but had no data. |
| `FAILED` | Provider acquisition failed. |
| `UNSUPPORTED` | Provider cannot support this Q/field/profile target. |

Retry eligibility, `next_retry_at`, attempt count, and provider-work-due are operational scheduling
facts. Provider "settled" is derived as no useful provider-specific automatic work currently due; it
must not erase the last acquisition result.

## Operational Action / Retry Dimension

State means what is true. Action means what the system should do next.

Examples:

- core incomplete is a readiness state
- retry provider tomorrow is an action plus scheduling metadata
- SEC pending is an assurance state
- check SEC later is an action

| Operational action | Meaning | Typical trigger | Latest Q? | Historical Q? | Coexistence |
| --- | --- | --- | --- | --- | --- |
| `CHECK_EXPECTED_RESULT` | Check whether an expected result has appeared. | Calendar/expected date due. | Usually yes | No for normal daily flow. | May coexist with provider scheduling metadata. |
| `FETCH_NEW_RESULT` | Fetch first canonical data for a detected result. | New result detected and target resolved. | Usually yes | Rare/import/backfill only. | Can coexist with SEC pending. |
| `ENRICH_Q` | Fill eligible missing canonical or enrichment fields. | Partial row and provider/cache evidence. | Yes | Yes under controlled enrichment/backfill. | Can coexist with core ready if enrichment is optional. |
| `RETRY_PROVIDER` | Retry provider after failure, no-data, or backoff policy. | Retry/followup due. | Yes | Yes if controlled. | Can target provider evidence while Q is otherwise usable. |
| `CHECK_SEC` | Check authoritative filing/source confirmation. | SEC pending and check due. | Yes | Yes. | Can coexist with core ready. |
| `BACKFILL_HISTORICAL` | Controlled historical data-debt handling. | Historical Q materially blocks downstream calculations. | No as historical action. | Yes. | Can coexist with settled lifecycle. |
| `MANUAL_REVIEW` | Human review needed. | Ambiguous identity, unsupported conflict, or policy limitation. | Yes | Yes. | May coexist with any non-terminal uncertainty. |

`DUE_ACTIONS` is a derived set of currently due actions. Multiple actions can coexist. If a UI needs
one primary action, derive it by precedence; do not make that primary action canonical truth.

Avoid generic `WAITING`. Use exact scheduling conditions instead:

- waiting for expected date: `CHECK_EXPECTED_RESULT` plus expected date
- waiting for provider retry: `RETRY_PROVIDER` plus `next_retry_at`
- waiting for SEC: `SEC_CONFIRMATION_STATE=PENDING` plus `CHECK_SEC` scheduling metadata

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
derived through `DUE_ACTIONS` or a controlled backfill planner.

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

For V3, initial company-universe admission is an architecture decision, not a quarterly-result
state. The initial V3 company universe starts from Legacy fundamentals membership and excludes only
tickers with positive BANK, INSURANCE, ETF/fund, or other non-company evidence. Expected calendar
entries, Yahoo observations, provider discoveries, V2-only companies, OHLCV/osakedata symbols, and
other provider evidence do not create V3 companies by themselves. Absence of V2 profile evidence is
not an exclusion reason for a Legacy ticker; contradictory instrument-type evidence may still create
a review item.

## Event Catalogue

An event happens at a point in time. A state describes what is true now.

| Event | Plain-language meaning | Scope | Affected dimension | Reconstructable today? | Persist history for reliable today metrics? |
| --- | --- | --- | --- | --- | --- |
| `CALENDAR_UPDATED` | Calendar estimate/status changed. | Company/ticker | Event timing, action schedule | Partially | Yes |
| `RESULT_EXPECTED_DATE_REACHED` | The expected result date is now due. | Run/event | Action scheduling | Current snapshot only | Yes |
| `RESULT_DETECTED` | A quarterly result was observed. | Any Q/latest context | Lifecycle, action | Partially | Yes |
| `Q_CREATED` | A canonical Q row was created. | Any Q | Lifecycle | Partially; stronger in V2 | Useful |
| `INITIAL_DATA_ACQUIRED` | First usable fundamental data for a detected Q was acquired. | Any Q | Provider/canonical data | Partially | Useful |
| `Q_ENRICHED` | Additional fields/evidence were added. | Any Q | Field/provenance readiness | Partially via provenance | Useful |
| `CORE_READINESS_CHANGED` | Core readiness changed; details contain before/after. | Any Q | Core readiness | Current snapshot only | Yes |
| `SEC_CONFIRMATION_RECEIVED` | Authoritative confirmation arrived. | Any Q | Assurance | Partially | Yes |
| `SCORE_READINESS_CHANGED` | Score readiness changed; details contain before/after. | Company window | Downstream readiness | Current snapshot only | Yes |
| `PROVIDER_FAILED` | Provider acquisition failed. | Provider/Q or provider/company | Provider acquisition | Partially | Useful |
| `RESOLUTION_ISSUE_CREATED` | Durable issue/manual-review item was created. | Any Q/source identity | Action/resolution | Yes | Yes |
| `Q_REOPENED` | A settled Q was returned to active enrichment. | Any Q | Lifecycle/action | Partially | Yes |
| `Q_BECAME_HISTORICAL` | A newer Q became latest. | Any Q | Role | Yes from ordering | Usually no |
| `BACKFILL_COMPLETED` | Historical data debt was resolved. | Any Q/company | Historical backfill | Partially | Yes |

## Event vs State Examples

| Event | Current state after the event |
| --- | --- |
| `CORE_READINESS_CHANGED` | `Q_CORE_FIELDS_READY` changed; details describe before/after |
| `SEC_CONFIRMATION_RECEIVED` | `SEC_CONFIRMATION_STATE=CONFIRMED` |
| `RESULT_DETECTED` | `Q_RESULT_LIFECYCLE=RESULT_DETECTED` or later |
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
- result-calendar expectation represented as a canonical Q role before the Q exists.
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
| `SEC_CONFIRMATION_STATE=CONFIRMED` | SEC or equivalent authoritative assurance has been obtained. |
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
Due actions:         none; `CHECK_SEC` scheduled later
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
| `Q_RESULT_LIFECYCLE` | LIFECYCLE | `RESULT_DETECTED`, `ENRICHING`, `OPERATIONALLY_SETTLED` | Detected/imported canonical Qs | Yes within dimension | All parallel dimensions | Where the Q is in the standard result flow | Sequential lifecycle only | Yes | Yes | Derive initially; persist only if runtime needs a snapshot |
| `Q_ROLE` | ROLE | `LATEST_OPERATIONAL_Q`, `HISTORICAL_Q` | Existing canonical Qs | Yes within dimension | Lifecycle/readiness/action | Whether the existing Q is latest or historical | Derived from ordering and decision date | Yes | Yes | Derive |
| `CALENDAR_COMPARISON_PERIOD` | ANALYTICAL_METADATA | `calendar_comparison_year + calendar_comparison_quarter + method + quality` | Any Q where period-end evidence exists | Yes within metadata value | All state dimensions | Cross-company seasonal comparison bucket | Currently derived by maximum temporal overlap from an approximate three-calendar-month interval ending at period end; future actual-period-range method may supersede it | Yes | Yes | Derive; schema later only if consumers need it |
| `Q_CORE_FIELDS_READY` | READINESS | `true`, `false`, `not_applicable`, `not_derivable` | Any Q | Yes within dimension | SEC pending, score false, enrichment incomplete | Required ordinary fields exist | Six-field positive-share rule | Yes | Yes | Derive |
| `FIELD_COMPLETENESS_LAYER` | READINESS | `CORE_REQUIRED`, `DOWNSTREAM_HISTORY_REQUIRED`, `OPTIONAL_ENRICHMENT`, `PROVENANCE_ONLY` | Any Q/field | No | Core readiness/action/provenance | Which fields matter to which consumers | Static field classification | Yes | Yes | Static helper |
| `DOWNSTREAM_READINESS` | READINESS | `TTM_READY`, `SCORE_READY`, `VALUATION_READY` booleans | Company/window or latest-context | No | Q core ready true/false | Whether consumers can run comparably | Derived from multi-quarter/window inputs | Yes | As contributor | Derive |
| `SEC_CONFIRMATION_STATE` | ASSURANCE | `NOT_APPLICABLE`, `NOT_YET_EXPECTED`, `PENDING`, `CHECKED_NO_EVIDENCE`, `PARTIAL_EVIDENCE`, `CONFIRMED`, `UNSUPPORTED`, `ERROR_RETRY`, `NOT_DERIVABLE` | Any Q | Yes within dimension | Core ready true/false | Authoritative source assurance | SEC/equivalent confirmation state | Yes | Yes | Derive; event history later if needed |
| `PROVIDER_ACQUISITION_RESULT` | PROVIDER_OUTCOME | `NOT_CHECKED`, `ACQUIRED`, `PARTIAL`, `NO_DATA`, `FAILED`, `UNSUPPORTED` | Provider/Q or provider/target | Yes within provider target | Scheduling/action/provenance facts | Last meaningful provider acquisition outcome | Provider result only; retry/settlement derived separately | Yes | Yes | Persist provider rows where operationally useful |
| `DUE_ACTIONS` / `OPERATIONAL_ACTION` | ACTION | `CHECK_EXPECTED_RESULT`, `FETCH_NEW_RESULT`, `ENRICH_Q`, `RETRY_PROVIDER`, `CHECK_SEC`, `BACKFILL_HISTORICAL`, `MANUAL_REVIEW` | Q/company/calendar target | No, multiple may coexist | Readiness/assurance/provider states | What work is due or scheduled | Action set/planner output, not state truth | Yes | Yes, controlled actions only | Derive due set; persist durable queue rows where needed |
| `HISTORICAL_BACKFILL_STATE` | BACKFILL | `NOT_HISTORICAL`, `NO_HISTORICAL_BACKFILL_DEBT`, `NEEDS_HISTORICAL_BACKFILL`, `NOT_DERIVABLE` | Historical Q plus company summary | Yes within dimension | Settled lifecycle, core false | Whether older Q gaps block historical consumers | Derived historical data debt | Not as historical debt | Yes | Derive |
| `PROFILE_SUPPORT_STATE` | SUPPORT | `ORDINARY_SUPPORTED`, `BANK_PROFILE`, `INSURANCE_PROFILE`, `UNSUPPORTED_PROFILE`, `UNKNOWN_PROFILE` | Company/ticker | Yes within dimension | All dimensions | Which rules apply | Accounting/profile support gate | Yes | Yes | Persist profile, derive support label |
| Canonical events | EVENT | See event catalogue | Point in time | No | States they change | Something happened | Transition/run metric | Yes | Yes | Persist only when reliable daily metrics require it |

## How Legacy Maps to the Canonical Model

Legacy database: `fundamentals_usa.db`.

Good or usable mappings:

- `rc_fundamental_quarterly` maps to canonical Q content.
- `rc_fundamental_quarter_ingestion_status.ingestion_status` approximates ingestion lifecycle.
- `source_confirmation_status` maps to SEC/assurance states.
- `retry_recommendation` and Check decisions map to `OPERATIONAL_ACTION` / `DUE_ACTIONS`.
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
| New Q result detected | Event and transition | Result-calendar expectation can produce `RESULT_DETECTED` and `FETCH_NEW_RESULT`. |
| Q enriched | Event | Fields/provenance added; may or may not affect core readiness. |
| Q core ready | State snapshot or transition if changed | `Q_CORE_FIELDS_READY=true`; transition event is `CORE_READINESS_CHANGED`. |
| Waiting for SEC confirmation | Assurance state plus scheduling | Prefer `SEC_CONFIRMATION_STATE=PENDING`; avoid vague waiting status. |
| SEC confirmation received | Event and state | Event `SEC_CONFIRMATION_RECEIVED`; state `SEC_CONFIRMATION_STATE=CONFIRMED`. |
| Retry/action work required | Action count | `DUE_ACTIONS` may include `RETRY_PROVIDER`, `ENRICH_Q`, `CHECK_SEC`, etc. |
| Historical backfill debt | Historical state snapshot | Separate from latest-result Check counts. |

## Update Fundamentals Terminology

Update reporting should separate:

1. what happened during the run
2. what is true after the run

| During-run event | Post-run state |
| --- | --- |
| `Q_CREATED` | Canonical row/storage milestone; lifecycle is `RESULT_DETECTED`, `ENRICHING`, or `OPERATIONALLY_SETTLED` depending on due work |
| `Q_ENRICHED` | Field/provenance state changed |
| `CORE_READINESS_CHANGED` | `Q_CORE_FIELDS_READY` changed; details include before/after |
| `SEC_CONFIRMATION_RECEIVED` | `SEC_CONFIRMATION_STATE=CONFIRMED` |
| `BACKFILL_COMPLETED` | `HISTORICAL_BACKFILL_STATE=NO_HISTORICAL_BACKFILL_DEBT` |
| provider retry remains | `DUE_ACTIONS` includes `RETRY_PROVIDER` plus retry schedule |

A successful update can still end with SEC pending, score not ready, or historical backfill debt.

## ASCII State Diagram

```text
Q IDENTITY
market + ticker + fiscal_year + fiscal_quarter
   |
   v

ROLE
LATEST_OPERATIONAL_Q -> HISTORICAL_Q
(role is contextual; it does not rewrite lifecycle)

SEQUENTIAL Q_RESULT_LIFECYCLE

RESULT_DETECTED
   |
   v
ENRICHING
   |
   v
OPERATIONALLY_SETTLED
   ^                 |
   |                 v
   +----------- Q_REOPENED event

EXPECTED lives in result-calendar state. CANONICALIZED and INITIAL_DATA_ACQUIRED are events or
derived milestones. RECONCILING is a transient process phase. MANUAL_REVIEW is an operational action plus a
resolution issue.

PARALLEL DIMENSIONS

CORE:       Q_CORE_FIELDS_READY=false | true | not_applicable | not_derivable
FIELDS:     CORE_REQUIRED | DOWNSTREAM_HISTORY_REQUIRED | OPTIONAL_ENRICHMENT | PROVENANCE_ONLY
SEC:        NOT_APPLICABLE | NOT_YET_EXPECTED | PENDING | CHECKED_NO_EVIDENCE
            PARTIAL_EVIDENCE | CONFIRMED | UNSUPPORTED | ERROR_RETRY | NOT_DERIVABLE
ACTION:     DUE_ACTIONS set of CHECK_EXPECTED_RESULT | FETCH_NEW_RESULT | ENRICH_Q
            RETRY_PROVIDER | CHECK_SEC | BACKFILL_HISTORICAL | MANUAL_REVIEW
PROVIDER:   NOT_CHECKED | ACQUIRED | PARTIAL | NO_DATA | FAILED | UNSUPPORTED
            plus scheduling/cache/provenance metadata
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
| Role | Contextual position of an existing canonical Q as latest or historical. |
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

Phase 1 implementation contracts are locked in
`docs/fundamentals_v3_implementation_readiness_contract.md`. The items below are validation or
future-extension topics, not blockers for V3 schema/repository/workflow coding:

- Whether the compact event list needs extension for reliable daily "today" metrics after real V3
  run observability exists.
- Exact persistence strategy for canonical derived helper output, if any.
- How much SEC/equivalent assurance can be derived from V2 provenance without a new explicit
  quarter-level confirmation projection.
- Legacy fiscal identity recovery coverage for rows that currently expose only `period_end_date`.
- Replacement or guarding of current date-derived work-unit identity as V3 implementation adopts
  reported fiscal identity.
- Whether a future reliable actual `period_start_date` source should supersede the currently agreed
  `APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END` method for `CALENDAR_COMPARISON_PERIOD`.
- Whether `CALENDAR_COMPARISON_PERIOD` should remain purely derived or later be persisted for
  cross-sectional analytics.
- Exact implementation details for subtracting three calendar months should follow the documented
  day-preserving, end-of-month-clamped rule.

## Authoritative Terminology

| Preferred canonical term | Use for |
| --- | --- |
| `Q_RESULT_LIFECYCLE` | Sequential result lifecycle. |
| `Q_ROLE` | Latest/historical context for existing canonical Qs. |
| `Q_CORE_FIELDS_READY` | Six-field ordinary core readiness for any Q. |
| `LATEST_Q_CORE_READY` | `Q_CORE_FIELDS_READY` on the latest operational Q. |
| reported fiscal year/quarter | The company's reported fiscal period identity. |
| period-end date | Metadata/evidence attached to a Q, not a substitute for fiscal identity. |
| `CALENDAR_COMPARISON_PERIOD` | Derived analytical bucket for cross-company same-season comparison. |
| `calendar_comparison_quality` | Quality flag explaining whether calendar comparison derivation is reliable. |
| `calendar_comparison_method` | Method label explaining how the comparison period was derived. |
| `derived_period_start_date` | Approximate analytical start date, currently period end minus three calendar months. |
| `SEC_CONFIRMATION_STATE` | Authoritative source assurance. |
| `OPERATIONAL_ACTION` / `DUE_ACTIONS` | What work is scheduled or due; multiple actions may coexist. |
| `HISTORICAL_BACKFILL_STATE` | Historical data debt. |
| `PROFILE_SUPPORT_STATE` | Ordinary/bank/insurance/support gate. |
| `CORE_READINESS_CHANGED` | Event where core readiness changed; details include before/after. |

| Terms to avoid or qualify | Why |
| --- | --- |
| `complete` | Ambiguous: could mean ingestion, core, SEC, score, or historical completeness. |
| `done` | Ambiguous and should not appear alone in reports/specs. |
| `basic complete` | Legacy-specific; not canonical core readiness. |
| `core complete` | V2-specific unless qualified; current V2 core differs from canonical core. |
| `waiting` | Too vague; use pending state plus due/retry timestamp. |
| `score not ready` for unsupported profiles | Unsupported is not the same as incomplete. |
