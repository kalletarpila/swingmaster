# Historical Fundamental State Integration Audit

## Scope

This audit documents how SwingMaster state, transition, historical range-run, report, and research workflows currently interact with fundamental data. It is read-only design work: no state logic, transition rules, reason enums, reports, UI, schedulers, database schemas, or production behavior are changed.

Runtime audit artifacts are written only under `temp/historical_fundamental_state_integration_audit/`.

## Current State Architecture

The state engine is deterministic technical state logic. The central contract is `evaluate_step(prev_state, prev_attrs, signals, policy, ticker, as_of_date)`, which consumes:

- previous `State` and `StateAttrs`
- a `SignalSet`
- a transition policy implementation
- optional ticker/date metadata for policy history lookups

Current states are:

- `NO_TRADE`
- `DOWNTREND_EARLY`
- `DOWNTREND_LATE`
- `STABILIZING`
- `ENTRY_WINDOW`
- `PASS`

Reason codes are produced by the policy rules and transition evaluation. They are technical and policy metadata such as trend start, invalidation, stabilization, entry readiness, pass completion, and state persistence. No audited reason path reads TTM, quarterly fundamentals, score percentiles, valuation, or earnings-event tables.

## Dependency Map

| Path | Function or CLI | Inputs | Fundamental fields consumed | Impact | Classification | Recommendation |
| --- | --- | --- | --- | --- | --- | --- |
| `swingmaster/core/engine/evaluator.py` | `evaluate_step` | previous state attrs, `SignalSet`, transition policy | none | hard state/reason output | `STATE_CORE_NO_FUNDAMENTALS` | keep fundamentals outside hard state logic |
| `swingmaster/core/policy/rule_v1/policy.py`, `rule_v2/policy.py`, `rule_v3/policy.py` | `decide` | `SignalSet`, optional state/signal history | none | hard state/reason output | `STATE_CORE_NO_FUNDAMENTALS` | do not add hard fundamental gates without separate product decision |
| `swingmaster/app_api/providers/osakedata_signal_provider_v2.py`, `osakedata_signal_provider_v3.py` | `get_signals` | `osakedata` OHLCV on or before the signal date | none | hard only through emitted technical signals | `STATE_CORE_NO_FUNDAMENTALS` | keep signal generation technical |
| `swingmaster/cli/run_range_universe.py` | historical range execution | trading dates, ticker universe, app facade, state repo, transition repo, post-run episode/EW/dual phases | none | hard state writes, post-run derived outputs | `STATE_CORE_NO_FUNDAMENTALS` | optionally attach historical fundamentals in reports only |
| `swingmaster/cli/run_fundamental_ticker_snapshot.py` | ticker snapshot display | current/latest TTM, quarterly, score percentile, valuation | TTM, scores, percentiles, valuation | no state impact | `CONTEXT_ONLY_HISTORICAL_NEEDS_SAFE_SNAPSHOT` | use `build_historical_fundamental_snapshot` for historical research context |
| EW score, dual score, daily/performance report paths inspected | score/report CLIs | state, transition, episode, market, transaction data | none in audited state/report scoring paths | ranking/report output only | `CONTEXT_ONLY_CURRENT` | optional report enrichment only |
| earnings event and quarter-match CLIs | earnings maintenance | `rc_earnings_event`, `rc_fundamental_quarter_earnings_match` | announcement metadata | no state impact in audited paths | `DIAGNOSTIC_ONLY` | keep earnings blackout separate from fundamental availability |

## Hard Inputs Versus Context

Hard decision inputs are technical market signals, previous state, previous state attributes, policy-specific technical history, and transition-graph validity. The inspected signal providers derive signals from OHLCV data and date-filtered technical structures.

Contextual inputs are current/latest fundamental snapshots, TTM metrics, scores, percentiles, valuation rows, maintenance audit rows, and UI/report display data. These are useful for research but are not state dependencies unless code explicitly routes them into the state decision. No such route was found.

No audited state path consumes:

- `rc_fundamental_quarterly`
- `rc_fundamental_ttm`
- `rc_fundamental_score_percentile`
- `rc_fundamental_valuation`
- `rc_earnings_event`
- `rc_fundamental_quarter_earnings_match`

## Historical Range-Run Findings

`run_range_universe` resolves trading dates, iterates ticker/date work, calls the application facade, evaluates daily state, and persists `rc_state_daily`, `rc_signal_daily`, and `rc_transition`. Post phases can populate episodes, EW scores, and dual scores.

The audited range-run state calculation does not load a current/latest fundamental snapshot, does not cache one for the whole historical run, and does not pass fundamental values into the transition policy. Historical state classifications therefore are not lookahead exposed through fundamentals.

A separate historical research report could become lookahead exposed if it joined current/latest ticker snapshots against historical state rows. That is a report-context problem, not a current state-engine problem. The safe future read model for such reports is `build_historical_fundamental_snapshot(...)`.

## Earnings Blackout

Earnings announcement dates and quarter matches are separate source metadata. No audited state/range/report path wires those tables into a blackout rule. There is therefore no current state impact from the new earnings-event or quarter-match data.

Do not conflate earnings blackout with quarterly fundamental availability. A future blackout feature would need its own explicit contract covering whether it uses forward calendar dates, completed announcements, or both. That is outside this phase.

## Representative Comparisons

The read-only diagnostic CLI was run against representative USA tickers on `2026-01-29`:

`AAPL`, `MSFT`, `JPM`, `XOM`, `NVDA`, `GIS`, `LMT`, `BBY`, `ARWR`, `DGXX`, `AVNS`

For those 11 ticker/date rows:

- rows with different current-style versus effective-date-safe fundamental context: 5
- report-context rows that would change or carry missing-context warnings: 6
- rows with no safe snapshot: 2
- rows with partial safe snapshot: 2
- state classification differences: 0
- reason differences: 0
- ranking differences: 0

A deterministic sampled three-date audit over `2026-01-28` through `2026-01-30` evaluated 30 ticker/date rows:

- rows with different current-style versus effective-date-safe fundamental context: 7
- report-context rows that would change or carry missing-context warnings: 24
- rows with no safe snapshot: 15
- rows with partial safe snapshot: 6
- state classification differences: 0
- reason differences: 0
- ranking differences: 0

The USA state database had broad state coverage on `2026-01-29`: `NO_TRADE`, `DOWNTREND_EARLY`, `DOWNTREND_LATE`, `STABILIZING`, `ENTRY_WINDOW`, and `PASS` were all present.

## Controlled Comparison

There is no clean production dependency-injection boundary for swapping fundamental contexts in the state engine because fundamentals are not part of the state-engine dependency graph. The focused tests therefore combine:

- direct `evaluate_step` fixture checks showing state/reason determinism without any fundamental context;
- fixture database comparisons where current/latest fundamental context differs from effective-date-safe historical context;
- assertions that state and reasons remain unchanged when context is missing, partial, or future-dated fundamentals exist.

## Integration Options

Option A, no state integration, is viable and preserves a purely technical state machine. It is simple, but it leaves useful research context detached from historical state rows.

Option B, report/context enrichment only, is the best fit now. Historical fundamentals can be attached to historical reports and research outputs without changing state, reasons, or eligibility.

Option C, soft ranking input, may be useful later for ordering candidates within an existing state such as `ENTRY_WINDOW`. It should be deferred until the report-context contract proves useful and coverage gaps are better understood.

Option D, hard state input, is not recommended. It would turn missing or partial fundamentals into a trading-state concern and would complicate the currently clear technical state contract.

Recommended policy: `OPTION_B_REPORT_CONTEXT_ONLY`.

## Missing-Data Policy

Future historical context enrichment should use simple non-blocking behavior:

- no historical snapshot: state remains calculated from existing technical inputs and report context says no fundamental context;
- partial snapshot: attach available fields plus warnings;
- no percentile or valuation: do not block state;
- no TTM: do not synthesize fundamental context;
- never fall back to future/current fundamentals for a historical signal date.

## Minimal Future Contract

If Option B is implemented later, keep the context small:

- `fundamental_context_status`
- `fundamental_source_ttm_as_of_date`
- `fundamental_effective_trading_date`
- `fundamental_score`
- selected historical percentile fields
- selected historical valuation fields such as EV/EBIT or FCF yield
- `fundamental_warnings`

Do not include the full snapshot object by default in state tables or reports.

## Next Scope

The smallest useful next implementation would add optional report/research enrichment that calls `build_historical_fundamental_snapshot(...)` at query time for selected historical state rows. It should be opt-in, context-only, and should not modify persisted `rc_state_daily`, `rc_transition`, state rules, reason codes, schedulers, or UI behavior.

## Limitations

This was a representative audit, not a full-universe impact calculation. It did not run Yahoo, SEC, schedulers, refresh jobs, backfills, or the full test suite. Vintage and provenance tables remain disabled for read paths and were not used as source data.
