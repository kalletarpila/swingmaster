# Incremental Earnings Calendar Maintenance

## Why Not Full-Universe Interactive Refresh

The USA manual result-check workflow used to refresh Yahoo earnings-calendar metadata for every OHLCV-active USA ticker before selecting completed-result candidates. With roughly 2,868 active USA tickers and the existing sequential Yahoo fetch behavior, that made `Check for New Results` proportional to the full universe and unsuitable for an interactive production button.

The plan-based update contract remains unchanged:

```text
maintained earnings calendar
-> small due candidate set
-> completed-result check
-> plan.json
-> Update Fundamentals
```

FIN, OMXH, RawCandle, `osakedata`, and the legacy quarter-state flag path are not changed.

## Existing State Reused

`rc_earnings_calendar` remains the canonical current expected-event table for USA calendar maintenance. Existing useful fields include:

```text
market
ticker
estimated_announcement_at
estimated_announcement_date
estimated_session
calendar_status
source
last_observed_at_utc
completed_earnings_event_id
```

The table now also stores lightweight provider-check state:

```text
calendar_last_checked_at_utc
calendar_check_status
calendar_last_failed_at_utc
calendar_failure_count
```

This state is needed to preserve a known future estimate when a provider call fails and to avoid selecting the same failing maintenance ticker on every manual run.

## Candidate Groups

The result check builds one deterministic provider-check set from three groups:

```text
1. DUE_FOR_RESULT_CHECK
2. DUE_FOR_CONFIRMATION
3. CALENDAR_MAINTENANCE
```

Duplicate tickers are removed after priority assignment.

## Selection Rules

`DUE_FOR_RESULT_CHECK` selects active tickers whose stored expected date is between:

```text
decision_date - event_watch_days_after
...
decision_date
```

The default `--event-watch-days-after` remains `5`.

`DUE_FOR_CONFIRMATION` starts from active tickers whose stored expected date is approaching:

```text
decision_date + 1
...
decision_date + calendar_confirmation_days_before
```

The default `--calendar-confirmation-days-before` is `7`.

This approaching-date window is only the watch window. It does not automatically mean every watched ticker gets a provider call every time the user presses `Check for New Results`.

Successful confirmation checks use this cadence:

```text
expected date 4-7 days away -> provider check at most once every 2 calendar days
expected date 1-3 days away -> provider check at most once per calendar day
```

Same-day repeated `Check for New Results` runs therefore do not refetch successfully confirmed future tickers solely because they remain inside the 7-day watch window.

Rows whose new `calendar_last_checked_at_utc` field is still `NULL` after migration use the existing `last_observed_at_utc` as the freshness fallback. This avoids an immediate bootstrap burst for rows that were already refreshed recently before the check-state columns existed. If both timestamps are unavailable, the ticker is treated as needing confirmation.

Expected dates on `decision_date` or in the recent past belong to `DUE_FOR_RESULT_CHECK`. They are not suppressed by the future-confirmation cadence.

`CALENDAR_MAINTENANCE` selects a capped deterministic backlog after due/result and confirmation tickers are removed. The default cap is:

```text
--calendar-maintenance-limit 100
```

Maintenance priority is:

```text
missing calendar row
retryable previous calendar failure
no current estimate
past expected date outside the result window
stale successful calendar check
```

The default stale threshold is:

```text
--calendar-stale-days 45
```

The default retry delay for previous non-imminent provider failures is:

```text
--calendar-failure-retry-days 3
```

For watched tickers whose expected date is 1-3 days away, failed provider checks retry the next calendar day. This avoids letting the default 3-day maintenance retry delay skip an imminent announcement window. Result-due tickers are still handled by `DUE_FOR_RESULT_CHECK`.

## Provider Behavior

The existing provider implementation remains sequential. One calendar refresh ticker can make up to `--max-retries` Yahoo requests, with existing timeout, backoff, and consecutive-failure controls. No concurrency was added; provider load is reduced by selecting fewer tickers.

A failed provider call records only check-state metadata. It does not delete or overwrite a previous future estimate.

Completed-event refresh remains a separate bounded stage for due/recent calendar candidates. If completed events are found, the existing earnings-event match rebuild and plan-generation logic are used. `rc_earnings_event` stores completed announcement evidence, and `rc_fundamental_quarter_earnings_match` maps completed events to deterministic target quarters for executable `plan.json` rows.

## Operational Summary

`Check for New Results` now reports:

```text
active_fetch_count
due_for_result_check_count
due_for_confirmation_watch_count
due_for_confirmation_count
maintenance_selected_count
unique_provider_check_ticker_count
calendar_rows_changed
completed_events_changed
candidate_count
maintenance_backlog_remaining
```

`due_for_confirmation_watch_count` is the count under watch. `due_for_confirmation_count` is the smaller count that actually needs a future-confirmation provider call now. This makes it visible when the workflow processed a bounded candidate set instead of the full USA universe.

## Limitations

Calendar freshness is intentionally traded against provider load. A ticker with no near-term event can wait for the bounded maintenance queue rather than being refreshed every interactive run.

The workflow still depends on Yahoo calendar availability and the existing completed-event/match semantics. It does not perform external delisting, merger, acquisition, or exchange-listing discovery, and it does not infer fiscal target periods from announcement-date math.
