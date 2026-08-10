# Fundamental Quarter Refresh Decision Policy

Date: 2026-08-07

This phase implements a read-only decision audit for a future USA quarterly fundamentals refresh scheduler. It does not activate scheduler behavior, run provider fetches, or write production data.

## Purpose

The audit answers:

```text
If an automated quarterly fundamentals scheduler were active for a decision date, which tickers would it attempt to refresh, why, and which tickers would it deliberately leave alone?
```

The implementation is:

```bash
.venv/bin/python -m swingmaster.cli.audit_fundamental_quarter_refresh_decisions \
  --fundamentals-db fundamentals_usa.db \
  --ohlcv-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --decision-date 2026-08-07 \
  --ohlcv-stale-days 14
```

Runtime artifacts are written only under `temp/`.

## Market-Data Activity

Historical securities must remain queryable. Quarterly fundamentals, TTM, score, percentile, valuation, earnings-event, quarter-match, OHLCV, and other historical rows must not be deleted merely because live refresh is disabled.

This phase uses only operational market-data recency for live fetch eligibility. It does not claim that a stale security is legally delisted, acquired, merged, or otherwise subject to a corporate action.

The canonical activity signal comes from read-only `osakedata`:

```text
/home/kalle/projects/rawcandle/data/osakedata.db
table: osakedata
ticker column: osake
date column: pvm
market column: market
```

Rule:

```text
latest_ohlcv_date exists
AND age from decision_date <= ohlcv_stale_days
    -> market_data_activity_status = ACTIVE
    -> fundamental_fetch_enabled = 1

latest_ohlcv_date is missing
OR age from decision_date > ohlcv_stale_days
    -> market_data_activity_status = STALE_OR_INACTIVE
    -> fundamental_fetch_enabled = 0
```

The exposed activity fields are:

```text
market
ticker
latest_ohlcv_date
ohlcv_age_days
market_data_activity_status
fundamental_fetch_enabled
last_assessed_at_utc
```

No external delisting, merger, acquisition, or exchange-listing discovery is implemented in this phase. `osakedata.db` is opened read-only and is not modified.

## Scheduler Order

The future scheduler contract begins with operational fetch eligibility:

```text
market-data activity / fetch eligibility
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

This suppresses `DUE_TODAY`, `DATE_PASSED_EVENT_NOT_FOUND`, `PUBLISHED_DATA_NOT_FETCHED`, `RETRY_PARTIAL_QUARTER`, and `RETRY_FETCH_FAILED` traffic for operationally stale securities.

## Calendar Semantics

`rc_earnings_calendar` is one current row per ticker/source and means:

```text
the ticker's next expected earnings event
```

Historical completed events belong in `rc_earnings_event`. A completed event id can remain calendar metadata, but it must not override a valid next future estimate.

Calendar status interpretation:

- `UPCOMING`: no refresh; wait.
- `DUE_TODAY`: watch for a completed event; do not fetch solely from the date.
- `DATE_PASSED_EVENT_NOT_FOUND`: review calendar/event coverage unless suppressed by stale OHLCV.
- `NO_CURRENT_ESTIMATE`: review source coverage unless suppressed by stale OHLCV.

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
REVIEW_HISTORICAL_PARTIAL
```

Priority classes:

```text
P1_FETCH_NOW     FETCH_NEW_QUARTER
P2_RETRY         RETRY_FETCH_FAILED, RETRY_PARTIAL_QUARTER
P3_WATCH         WATCH_DUE_TODAY, WATCH_POST_EVENT_GRACE
P4_REVIEW        REVIEW_DATE_PASSED_NO_EVENT, REVIEW_NO_CALENDAR_ESTIMATE, REVIEW_AMBIGUOUS_PERIOD, REVIEW_HISTORICAL_PARTIAL
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

`UNKNOWN_HISTORICAL_INGEST_COMPLETENESS` means the row was assessed from current database state without preserved source-response evidence from a managed update. If that row is incomplete, it is not normal weekly retry work merely because a completed earnings event can be matched to it. The decision is `REVIEW_HISTORICAL_PARTIAL`, which is non-executable and surfaces the row for review or future historical maintenance.

`RETRY_PARTIAL_QUARTER` is reserved for explicit managed partial ingestion evidence, currently `FUNDAMENTALS_PARTIAL` or another non-historical ingestion evidence type. `FETCH_NEW_QUARTER` remains executable when a persisted completed event has a deterministic target period and that target quarter is missing, even if the calendar row has moved forward by 14-30 days or more. `RETRY_FETCH_FAILED` remains executable when either `ingestion_status` or `last_fetch_status` records a fetch failure.

Plan-based USA `Update Fundamentals` writes authoritative managed-attempt status for the target quarter after each candidate attempt. A complete target quarter is persisted as `QUARTER_BASIC_COMPLETE`; an incomplete but present target quarter is persisted as `FUNDAMENTALS_PARTIAL`; a provider/update fetch failure is persisted as `FETCH_FAILED`; and a successful provider interaction that still leaves no usable target-quarter row is persisted as `PUBLISHED_DATA_NOT_FETCHED`. A transient failure does not downgrade an existing complete managed status. Historical unknown rows remain `UNKNOWN_HISTORICAL_INGEST_COMPLETENESS` unless a managed update attempt actually runs for that target quarter.

Quarter completeness and source confirmation are separate. `quarter_basic_complete = 1` means the row is usable for TTM, lifecycle, scoring, and valuation. `source_confirmation_status` records whether the usable row is SEC-backed or still awaiting authoritative SEC confirmation.

USA plan-based `Update Fundamentals` uses this source order for a target quarter:

```text
SEC companyfacts/reconstruction first
Yahoo live raw -> rc_fundamental_yahoo_quarterly cache only if SEC target is unavailable
Yahoo fallback/enrichment from the cache
```

If SEC produces the target quarter, SEC is authoritative and Yahoo may only fill SEC NULL fields through the existing fallback enrichment path. The source state becomes `SEC_CONFIRMED` or `SEC_CONFIRMED_YAHOO_ENRICHED`.

If SEC does not produce the target but Yahoo cache/live data can create a usable row, the row may still become `QUARTER_BASIC_COMPLETE`; the source state becomes `YAHOO_BACKED_SEC_PENDING`. Future result checks emit `REFRESH_SEC_CONFIRMATION`, not `FETCH_NEW_QUARTER`, so the same fresh-plan update path retries SEC without treating usable fundamentals as missing.

If Yahoo creates only an incomplete target row, the ingestion status remains `FUNDAMENTALS_PARTIAL` and normal partial retry semantics apply. If no usable target data is available, the managed ingestion status remains unresolved (`PUBLISHED_DATA_NOT_FETCHED` or `FETCH_FAILED` depending on the failure). SEC transient failures after a Yahoo-complete row do not downgrade quarter completeness; they set `SEC_CONFIRMATION_FAILED_RETRYABLE`, which remains eligible for SEC follow-up.

Migration/bootstrap does not infer SEC confirmation for historical rows. Existing rows without managed source evidence use `SOURCE_CONFIRMATION_UNKNOWN` and do not create a new historical SEC-confirmation backlog.

The active leverage metric is `net_debt_to_ebit`; the deprecated `net_debt_to_ebitda` is not part of this policy.

## Production Audit

Full read-only audit artifact root:

```text
temp/quarter_refresh_decision_audit/20260807T_ohlcv_v2/
```

Inputs:

```text
decision_date:      2026-08-07
ohlcv_stale_days:   14
fundamentals rows:  read-only
osakedata rows:     read-only
```

Market-data activity:

```text
active_fetch_count       2868
stale_or_inactive_count  68
no_ohlcv_count           0
ohlcv_age_0_7_days       2861
ohlcv_age_8_14_days      7
ohlcv_age_15_30_days     26
ohlcv_age_over_30_days   42
```

Calendar status inputs:

```text
UPCOMING                     2258
DUE_TODAY                    40
NO_CURRENT_ESTIMATE          634
DATE_PASSED_EVENT_NOT_FOUND  4
```

Decision counts after OHLCV suppression:

```text
NO_ACTION_INACTIVE_SECURITY  68
NO_ACTION_UPCOMING           2250
WATCH_DUE_TODAY              40
REVIEW_NO_CALENDAR_ESTIMATE  577
REVIEW_DATE_PASSED_NO_EVENT  1
```

Priority counts:

```text
P3_WATCH      40
P4_REVIEW     578
P5_NO_ACTION  2318
```

Auto-fetch eligible count: `0`.

Previous `REVIEW_DATE_PASSED_NO_EVENT` cases suppressed by stale OHLCV:

```text
NUVL
OLPX
TBRG
```

Previous `REVIEW_NO_CALENDAR_ESTIMATE` cases suppressed by stale OHLCV:

```text
ACLX, ALTS, AMWD, APLS, ASGN, BK, BLD, BTM, CPRX, CSGS, CTLP, CTRA, CUK, CVGW,
CWAN, DAWN, EEX, EHAB, EKSO, ESPR, EVTV, EWCZ, EXPI, FFIC, FOLD, GDEN, GTLS,
HOTH, HTBK, IAC, JHG, KW, MASI, MCW, MEG, NSA, PKST, PRA, PSTG, QVCGA, SEM,
SEMR, SLNO, SNCY, STEL, STKL, STSS, TERN, THR, TIVC, TMHC, TPH, UDMY, USEG,
VRE, VSCO, WSR
```

Other inactive diagnostics:

```text
inactive_but_calendar_upcoming_count: 8
inactive_but_due_today_count: 0
inactive_with_fetch_candidate_count_before_suppression: 0
```

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
latest_ohlcv_date
ohlcv_age_days
market_data_activity_status
missing_fields
planned_action
reason
```

Write boundaries for a later scheduler:

1. assess market-data activity from read-only OHLCV;
2. refresh earnings calendar for activity-enabled tickers;
3. refresh completed earnings events for activity-enabled tickers;
4. build read-only quarter refresh decisions;
5. select P1/P2 only where `fundamental_fetch_enabled = 1`;
6. fetch quarterly fundamentals for selected candidates;
7. reassess quarter completeness;
8. rebuild TTM/score/valuation only after persisted fundamentals change.

This phase implements only step 4 and audit artifacts. It does not activate automation.
