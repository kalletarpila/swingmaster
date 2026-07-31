# Fundamental Effective-Date Usage Audit

Read-only audit run: `temp/fundamental_effective_date_audit/20260731T163128Z/`

Database: `fundamentals_usa.db`

## Summary

SwingMaster currently stores quarterly fundamentals by fiscal `period_end_date`. TTM rows use `as_of_date = latest_period_end_date`, scores are written back onto those TTM rows, percentiles select `MAX(rc_fundamental_ttm.as_of_date) <= target_date`, and historical valuation selects latest TTM `as_of_date <= valuation date`.

That is acceptable for current/latest workflows, but retrospective research should not treat `period_end_date` as a market availability boundary. Under the intended lightweight policy, a quarter should become historically usable from:

```text
rc_fundamental_quarter_earnings_match.effective_trading_date
```

with:

```text
availability_policy = EARNINGS_EFFECTIVE_DATE_ASSUMED
```

Do not reactivate vintage or field-provenance writes for this use case.

## Full-DB Impact

Canonical USA quarterly universe excludes `.HE` tickers and contains `2,936` tickers.

Observed impact:

- matched quarters: `125,554`
- quarters with positive reporting delay: `125,554`
- median reporting delay: `34` days
- p95 reporting delay: `59` days
- total period-end to effective-date days: `4,418,513`
- affected ticker count: `2,804`
- null effective-date matches: `101`
- unmatched quarterly rows: `30,017`
- ambiguous or non-high-confidence matches: `2,797`
- potentially affected `rc_fundamental_ttm` rows: `123,503`
- potentially affected `rc_fundamental_score` rows: `123,503`
- potentially affected `rc_fundamental_valuation` rows: `509`

The exact affected `rc_fundamental_score_percentile` row count was intentionally not computed in the full audit because the direct 4.5M-row comparison was too costly for a read-only diagnostic. The code path is still materially exposed because it selects peer TTM rows by period-end `as_of_date <= target_date`.

Pre/post checks matched:

- `PRAGMA quick_check`: `ok`
- `rc_fundamental_quarterly`: `155,571`
- `rc_earnings_event`: `135,055`
- `rc_fundamental_quarter_earnings_match`: `125,554`

## Representative Findings

For `2026-07-31`, AAPL, MSFT, JPM, XOM, and NVDA all selected the same current and effective-date-safe latest quarter because their latest matched quarters were already effective by that date.

Around announcements, current period-end selection differs. Examples from the representative artifact:

- AAPL on `2026-04-13`: current selects `2026-03-28`, safe selects `2025-12-27`, look-ahead `17` days.
- MSFT on `2026-04-14`: current selects `2026-03-31`, safe selects `2025-12-31`, look-ahead `15` days.
- JPM on `2026-04-07`: current selects `2026-03-31`, safe selects `2025-12-31`, look-ahead `7` days.
- XOM on `2026-04-15`: current selects `2026-03-31`, safe selects `2025-12-31`, look-ahead `16` days.

## Consumer Map

| Path | Current Logic | Classification | Severity | Future Action |
| --- | --- | --- | --- | --- |
| `swingmaster/fundamentals/build_ttm.py` | Reads all `rc_fundamental_quarterly` rows ordered by `period_end_date`; TTM `as_of_date` equals latest component period end. | `DERIVED_TABLE_NEEDS_REBUILD_POLICY` | `MATERIAL_HISTORICAL_LOOKAHEAD` | Add a lightweight TTM availability date equal to max component effective date, or build separate historical research TTM rows. |
| `swingmaster/fundamentals/score.py` | Scores every TTM row by TTM `as_of_date`. | `DERIVED_TABLE_NEEDS_REBUILD_POLICY` | `MATERIAL_HISTORICAL_LOOKAHEAD` | Recompute historical scores after effective-dated TTM or filter TTM first. |
| `swingmaster/fundamentals/score_percentile.py` | Selects each peer by `MAX(t.as_of_date) <= target_date`. | `HISTORICAL_RESEARCH_NEEDS_EFFECTIVE_DATE` | `MATERIAL_HISTORICAL_LOOKAHEAD` | Historical percentiles must select each peer by that peer's own availability boundary. |
| `swingmaster/cli/run_fundamental_valuation.py` | Selects latest TTM `as_of_date <= valuation_date` and price `pvm <= valuation_date`. | `HISTORICAL_RESEARCH_NEEDS_EFFECTIVE_DATE` | `MATERIAL_HISTORICAL_LOOKAHEAD` | Keep current valuation unchanged; historical valuation should use effective-dated TTM. |
| `swingmaster/cli/run_fundamental_ticker_snapshot.py` | Displays latest TTM, quarterly, percentile, and valuation rows. | `DISPLAY_ONLY` | `CURRENT_STATE_ONLY` | Leave current snapshot unchanged; add availability metadata only for historical research snapshots. |
| `swingmaster/cli/run_fundamental_quarter_state.py` | Syncs `MAX(period_end_date)` into latest quarter state. | `CURRENT_ONLY_NO_CHANGE` | `CURRENT_STATE_ONLY` | No effective-date change for current state. |
| `swingmaster/cli/run_fundamental_quarter_update.py` | Current provider/update orchestration and downstream latest table refresh. | `CURRENT_ONLY_NO_CHANGE` | `CURRENT_STATE_ONLY` | Leave current/latest workflow unchanged. |
| Historical range/research/model CLIs | Use historical signal/as-of dates and persisted derived features. | `HISTORICAL_RESEARCH_NEEDS_EFFECTIVE_DATE` | `UNDETERMINED` | Use effective-date-safe fundamentals if fundamentals enter research features. |
| Technical snapshot readers under `analysis/` | Use technical confirmation/as-of dates; no fundamentals tables. | `DISPLAY_ONLY` | `NO_LOOKAHEAD` | No fundamentals change. |
| Earnings event and match maintenance CLIs | Build `rc_fundamental_quarter_earnings_match`. | `DIAGNOSTIC_ONLY` | `NO_LOOKAHEAD` | Keep as canonical effective-date source. |

## Required Conclusions

Current-state only workflows:

- quarter update orchestration;
- quarter state sync;
- latest valuation/dashboard-style output;
- latest ticker snapshot/export use.

Retrospective workflows:

- historical TTM sequence interpretation;
- historical score rows;
- historical cross-sectional percentiles;
- historical valuation;
- any range/backtest/training workflow that consumes fundamental-derived rows.

Current code can use quarters before announcement wherever historical logic treats `period_end_date` or TTM `as_of_date` as the availability boundary.

TTM creates historical look-ahead exposure because TTM rows are keyed to fiscal period end, not max component effective date.

Scoring inherits TTM exposure because scores are written to TTM rows.

Historical percentiles create cross-sectional exposure because each peer can contribute a TTM row whose quarter was not yet announced on `target_date`.

Historical valuation creates exposure when a historical price date is paired with a TTM row not yet effective.

Snapshots are display-oriented. Latest/current snapshots should remain unchanged; historical snapshots should expose `fundamentals_effective_date`, `source_period_end_date`, and match confidence if they become research inputs.

## Missing-Match Policy

Recommended conservative future rule:

```text
EXCLUDE_FROM_RETROSPECTIVE_USE
```

For retrospective research, if a quarter has no persisted match or `effective_trading_date` is null, do not use it unless a separate audited fallback policy is explicitly introduced later.

## Recommended Architecture

Use Option D, the lightweight hybrid:

- keep current/latest workflows unchanged;
- use query-time effective-date filtering for historical research;
- later add one lightweight availability field to TTM, computed as max component-quarter `effective_trading_date`;
- rebuild historical TTM, score, percentile, and valuation outputs only when producing effective-date-safe research outputs.

This isolates historical changes from current latest-state workflows and avoids vintage/provenance reactivation.
