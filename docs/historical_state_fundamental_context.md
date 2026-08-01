# Historical State Fundamental Context

## Integration Point

The first integration point is the new read-only CLI:

```bash
.venv/bin/python -m swingmaster.cli.inspect_historical_state
```

This is intentionally narrower than daily reports, performance reports, EW/dual reports, schedulers, or UI views. It inspects selected historical `rc_state_daily` rows and can optionally attach historical fundamental context at query time. It does not modify state calculation or any existing report default.

The selected path is appropriate because it is a research/inspection surface, has bounded ticker/date inputs, and can construct enrichment after state row serialization. That keeps fundamentals outside the technical state engine.

## State Logic Remains Technical

The state row payload is loaded from:

- `rc_state_daily`
- optional `rc_transition`
- optional `rc_signal_daily`
- previous `rc_state_daily` row for `previous_state`

Fundamentals are not passed into `evaluate_step`, policies, signal providers, transition logic, reason generation, or state persistence. The enrichment is attached after the state payload exists.

The CLI asserts this by keeping these fields unchanged when context is enabled:

- `state`
- `previous_state`
- `reason_codes`
- `transition`
- `signal_values`
- `state_attrs`

## Opt-In Behavior

Historical fundamentals are disabled by default.

Without:

```text
--include-historical-fundamentals
```

the CLI does not open the fundamentals database or price database, does not call the historical snapshot helper, and emits no `fundamental_context` section.

With the flag, each row gets one nested object:

```json
{
  "fundamental_context": {
    "fundamental_context_status": "OK",
    "fundamental_policy": "HISTORICAL_EFFECTIVE_DATE_SAFE_CONTEXT"
  }
}
```

Optional switches:

- `--no-percentile`: skip historical percentile work
- `--no-valuation`: skip historical valuation work

Audit artifacts are written only when `--audit-output-root` is supplied, and the path must be under repository `temp/`.

## Compact Contract

The compact context contract is:

- `fundamental_context_status`
- `fundamental_policy`
- `fundamental_requested_as_of_date`
- `fundamental_source_ttm_as_of_date`
- `fundamental_effective_trading_date`
- `fundamental_score`
- `fundamental_warnings`
- `fundamental_percentile`
- `fundamental_percentile_population_size`
- `historical_close_price`
- `historical_ev_ebit`
- `historical_fcf_yield`
- `valuation_status`
- `percentile_status`

The full historical snapshot object is not exposed by default.

## Effective-Date Rules

For every enriched state row:

```text
historical snapshot as-of date = rc_state_daily.date
```

The helper calls `build_historical_fundamental_snapshot(...)` and uses the existing effective-date-safe TTM, score, percentile, valuation, and price-selection policies. It never falls back to current/latest fundamentals for a historical signal date.

Vintage and provenance remain disabled as source data for this context.

## Missing Data

Missing data is non-blocking:

- no TTM: `fundamental_context_status = NO_AVAILABLE_TTM`
- score missing while TTM exists: `fundamental_context_status = PARTIAL` and `MISSING_SCORE` warning
- percentile unavailable: `percentile_status = NO_AVAILABLE_PERCENTILE`
- valuation or price unavailable: `valuation_status = VALUATION_UNAVAILABLE`
- disabled optional section: status is `DISABLED`

State rows remain valid even when context is missing or partial.

## Representative Verification

The representative real-DB check used:

`AAPL`, `MSFT`, `JPM`, `XOM`, `NVDA`, `GIS`, `LMT`, `BBY`, `ARWR`, `DGXX`, `AVNS`

on `2026-01-29`.

The audit evaluated 11 rows:

- `context_ok_count`: 7
- `context_partial_count`: 2
- `no_available_ttm_count`: 2
- `percentile_unavailable_count`: 2
- `valuation_unavailable_count`: 4
- `state_difference_count`: 0
- `reason_difference_count`: 0
- `transition_difference_count`: 0
- `signal_difference_count`: 0
- `state_attrs_difference_count`: 0

Median enrichment time was about 0.38 seconds per row in the representative run. P95 was about 0.39 seconds. A one-row CLI run took about 0.24 seconds without enrichment and about 0.59 seconds with enrichment.

A bounded 50-row audit on the same date returned:

- `rows_evaluated`: 50
- `context_ok_count`: 19
- `context_partial_count`: 17
- `no_available_ttm_count`: 14
- `percentile_unavailable_count`: 14
- `valuation_unavailable_count`: 31
- all state/reason/transition/signal/state-attrs difference counts: 0
- median enrichment seconds: 0.379471
- p95 enrichment seconds: 0.394906

The cost is acceptable for selected research inspection, not for broad default reports.

## Bounded Audit Mode

Example:

```bash
.venv/bin/python -m swingmaster.cli.inspect_historical_state \
  --state-db swingmaster_rc_usa_2024_2025.db \
  --fundamentals-db fundamentals_usa.db \
  --price-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --market usa \
  --date 2026-01-29 \
  --ticker AAPL \
  --include-historical-fundamentals \
  --audit-output-root temp/historical_state_fundamental_context/<UTC_TIMESTAMP>/audit \
  --json
```

The audit writes:

- `audit.json`
- `audit_rows.csv`
- `summary.json`
- `progress.log`

All paths are validated under repository `temp/`.

## Default Regression

Existing reports and CLIs are unchanged. The new CLI has no effect on:

- state transitions
- reason enums
- persisted state rows
- persisted transition rows
- episode rows
- EW scores
- dual scores
- daily reports
- performance reports
- UI
- scheduler behavior

## Deferred Options

Broader report integration can attach the same nested `fundamental_context` object to selected historical reports later. That should remain opt-in until query cost and field usefulness are proven.

Soft candidate ranking can be considered later for ordering rows inside an existing state such as `ENTRY_WINDOW`. That should not change state eligibility.

UI display can use this helper for an explicit historical-inspection panel, but it should avoid calling it for broad default table views.

Hard state input remains deferred and is not recommended for this product context.

## Recommended Next Step

Use `inspect_historical_state` for selected ticker/date research and review which context fields are actually useful. If the field set proves stable, add an opt-in context section to one bounded historical export path without changing default report output.
