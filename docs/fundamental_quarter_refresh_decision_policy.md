# Fundamental Quarter Refresh Decision Policy

Date: 2026-08-07

This phase adds a read-only decision audit for a future USA quarterly fundamentals refresh scheduler. It does not activate scheduler behavior, run provider fetches, or write production data.

## Purpose

The audit answers:

```text
If an automated quarterly fundamentals scheduler were active now, which tickers would it attempt to refresh, why, and which tickers would it deliberately leave alone?
```

The implementation is:

```bash
.venv/bin/python -m swingmaster.cli.audit_fundamental_quarter_refresh_decisions --fundamentals-db fundamentals_usa.db
```

Runtime artifacts are written only under `temp/`.

## Security Activity

Historical securities must remain queryable. Delisted, acquired, merged, inactive, quarterly, TTM, score, earnings-event, percentile, valuation, and other historical rows must not be deleted merely because live refresh is disabled.

The lightweight activity model is:

```text
security_status:
    ACTIVE
    DELISTED
    ACQUIRED_OR_MERGED
    INACTIVE_OTHER
    UNKNOWN

fundamental_fetch_enabled:
    1 / 0
```

Semantics:

```text
ACTIVE                  -> fundamental_fetch_enabled = 1
DELISTED               -> fundamental_fetch_enabled = 0
ACQUIRED_OR_MERGED     -> fundamental_fetch_enabled = 0
INACTIVE_OTHER         -> fundamental_fetch_enabled = 0
UNKNOWN                -> fundamental_fetch_enabled = 1
```

`UNKNOWN` does not automatically disable fetching. The audit does not infer inactive status from `NO_CURRENT_ESTIMATE`, Yahoo empty response, fetch failures, or missing recent fundamentals alone.

Current local evidence inspection found no canonical production security/listing-status table in `fundamentals_usa.db`. The existing read-only ticker cleanup audit has local delisting concepts (`active_status`, `delisted_status`) derived from Yahoo metadata and dependency inventory, but it is not a persisted security master. This phase reuses that local metadata interpretation where it is explicit and otherwise classifies securities as `UNKNOWN`.

Future dedicated listing-status refresh can populate the model safely by writing an explicit security activity table after reviewed evidence from exchange/listing status, delisting notices, corporate-action feeds, or trusted provider lifecycle data.

## Scheduler Order

The future scheduler contract begins with activity eligibility:

```text
security active/fetch eligibility
-> earnings calendar
-> completed earnings events
-> quarter refresh decision
-> selected fundamentals fetch
```

Inactive suppression has highest precedence:

```text
if fundamental_fetch_enabled = 0:
    decision = NO_ACTION_INACTIVE_SECURITY
    eligible_for_future_auto_fetch = false
```

This suppresses `DUE_TODAY`, `DATE_PASSED_EVENT_NOT_FOUND`, `PUBLISHED_DATA_NOT_FETCHED`, `RETRY_PARTIAL_QUARTER`, and `RETRY_FETCH_FAILED` traffic for known inactive historical securities.

## Calendar Semantics

`rc_earnings_calendar` is one current row per ticker/source and means:

```text
the ticker's next expected earnings event
```

Historical completed events belong in `rc_earnings_event`. A completed event id can remain calendar metadata, but it must not override a valid next future estimate.

Calendar status interpretation:

- `UPCOMING`: no refresh; wait.
- `DUE_TODAY`: watch for a completed event; do not fetch solely from the date.
- `DATE_PASSED_EVENT_NOT_FOUND`: review calendar/event coverage.
- `NO_CURRENT_ESTIMATE`: review source coverage, irregular reporting, or inactive status evidence; do not fetch automatically.

## Completed Event Confirmation

Publication is confirmed only by a completed earnings event:

```text
is_reported = 1
AND reported_eps IS NOT NULL
```

The audit does not treat the calendar date alone as proof of publication. It also guards against false positives from old historical events: for a calendar-bound decision, the latest completed event must be on or after the estimated announcement date to confirm the current expected event.

## Decision Classes

```text
NO_ACTION_INACTIVE_SECURITY
NO_ACTION_UPCOMING
NO_ACTION_COMPLETE
WATCH_DUE_TODAY
WATCH_POST_EVENT_GRACE
FETCH_NEW_QUARTER
RETRY_PARTIAL_QUARTER
RETRY_FETCH_FAILED
REVIEW_DATE_PASSED_NO_EVENT
REVIEW_NO_CALENDAR_ESTIMATE
REVIEW_AMBIGUOUS_PERIOD
```

Priority classes:

```text
P1_FETCH_NOW     FETCH_NEW_QUARTER
P2_RETRY         RETRY_FETCH_FAILED, RETRY_PARTIAL_QUARTER
P3_WATCH         WATCH_DUE_TODAY, WATCH_POST_EVENT_GRACE
P4_REVIEW        REVIEW_DATE_PASSED_NO_EVENT, REVIEW_NO_CALENDAR_ESTIMATE, REVIEW_AMBIGUOUS_PERIOD
P5_NO_ACTION     NO_ACTION_INACTIVE_SECURITY, NO_ACTION_UPCOMING, NO_ACTION_COMPLETE
```

`eligible_for_future_auto_fetch` is true only for P1/P2 fetch/retry decisions and only when `fundamental_fetch_enabled = 1`.

## Quarter Completeness

`QUARTER_BASIC_COMPLETE` is the current-quarter refetch boundary:

```text
valid period_end_date
AND revenue IS NOT NULL
AND ebit IS NOT NULL
AND (
    free_cashflow IS NOT NULL
    OR (
        operating_cashflow IS NOT NULL
        AND capex IS NOT NULL
    )
)
AND cash IS NOT NULL
AND total_debt IS NOT NULL
AND shares_outstanding IS NOT NULL
```

`ttm_input_complete` and `score_history_complete` are history quality metadata. If the corresponding quarter exists and `quarter_basic_complete = 1`, the decision is `NO_ACTION_COMPLETE` even when TTM or score history is incomplete. This prevents repeated fetching of an already complete current quarter.

The active leverage metric is `net_debt_to_ebit`; the deprecated `net_debt_to_ebitda` is not part of this policy.

## Grace Window

Exact historical provider-availability latency cannot be reconstructed from current local tables without treating ingestion timestamps as market availability, which would be incorrect for backfilled data.

Recommended conservative operational policy:

```text
completed event confirmed
AND safe fiscal period resolved
AND quarter missing
-> FETCH_NEW_QUARTER

if fetch returns empty or partial:
-> RETRY_* with bounded retry cadence

continue retry for 3-5 calendar days after confirmed event
-> then REVIEW_AMBIGUOUS_PERIOD or manual stale-missing-quarter review
```

This phase does not implement fetch attempts or retry scheduling.

## Production Audit

Full read-only audit artifact root:

```text
temp/quarter_refresh_decision_audit/20260807T_full_v2/
```

Status inputs:

```text
UPCOMING                     2258
DUE_TODAY                    40
NO_CURRENT_ESTIMATE          634
DATE_PASSED_EVENT_NOT_FOUND  4
```

Decision counts:

```text
NO_ACTION_UPCOMING           2258
WATCH_DUE_TODAY              40
REVIEW_NO_CALENDAR_ESTIMATE  634
REVIEW_DATE_PASSED_NO_EVENT  4
```

Priority counts:

```text
P3_WATCH     40
P4_REVIEW    638
P5_NO_ACTION 2258
```

Auto-fetch eligible count: `0`.

Security activity counts:

```text
ACTIVE                  0
DELISTED                0
ACQUIRED_OR_MERGED      0
INACTIVE_OTHER          0
UNKNOWN                 2936
fundamental_fetch_enabled=0  0
```

Inactive diagnostics:

```text
inactive_but_calendar_upcoming_count: 0
inactive_but_due_today_count: 0
inactive_with_fetch_candidate_count_before_suppression: 0
```

Session coverage:

```text
UNKNOWN 2936
```

Session data is not yet sufficient for clock-aware scheduling.

`NO_CURRENT_ESTIMATE` local breakdown:

```text
completed event older than 60 days: 523
completed event 15-60 days old:     5
no completed event:                 106
```

The four `DATE_PASSED_EVENT_NOT_FOUND` tickers are `DMLP`, `NUVL`, `OLPX`, and `TBRG`; each had estimated date `2026-08-06`, no completed event in the local completed-event table, latest DB period `2026-03-31`, and decision `REVIEW_DATE_PASSED_NO_EVENT`.

All 40 `DUE_TODAY` tickers were classified as `WATCH_DUE_TODAY`; their latest completed events, when present, were older historical events and did not confirm the current expected event.

## False-Positive Findings

The initial audit draft would have produced false P2 retry candidates by allowing the latest historical completed event to confirm a current `DUE_TODAY` calendar row. The final logic prevents that by requiring the completed event date to be on or after the estimated announcement date.

No P1/P2 candidates remain under current local evidence, so no automatic fetch should run now.

## Plan-Only Scheduler Contract

Future plan artifact:

```text
quarter_refresh_plan_<date>.csv
```

Columns:

```text
ticker
decision
priority
event_date
period_end_date
missing_fields
planned_action
reason
```

Write boundaries for a later scheduler:

1. refresh security activity status from trusted sources;
2. refresh earnings calendar;
3. refresh completed earnings events;
4. build read-only quarter refresh decisions;
5. select P1/P2 only where `fundamental_fetch_enabled = 1`;
6. fetch quarterly fundamentals for selected candidates;
7. reassess quarter completeness;
8. rebuild TTM/score/valuation only after persisted fundamentals change.

This phase implements only step 4 and plan artifacts. It does not activate automation.
