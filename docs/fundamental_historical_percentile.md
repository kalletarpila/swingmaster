# Fundamental Historical Percentiles

## Existing Architecture

SwingMaster stores percentile rows in `rc_fundamental_score_percentile`, keyed by `(ticker, target_date, rule_id)`. The current builder is `swingmaster/fundamentals/score_percentile.py`; the CLI is `swingmaster/cli/run_fundamental_score_percentile.py`.

Current percentile population uses `rc_fundamental_ttm` and selects one score row per ticker by `MAX(as_of_date) <= target_date`. It then joins `ticker_meta` from osakedata for sector and industry, computes global, sector, and industry factor percentiles, blended percentiles, lifecycle-weighted percentiles, and sector/industry ranks. The writer uses `INSERT OR REPLACE` into the current percentile table.

This current path remains unchanged. It is suitable for current/latest screening, but historical use can look ahead because `as_of_date` is the fiscal period date, not the date the score became available.

## Dependency Map

| Path | Input Tables | Output | Date Logic | Classification | Action |
| --- | --- | --- | --- | --- | --- |
| `score_percentile.load_latest_percentile_snapshot` | `rc_fundamental_ttm`, `ticker_meta` | in-memory rows | `MAX(as_of_date) <= target_date` per ticker | `CURRENT_ONLY_NO_CHANGE` | Leave unchanged |
| `score_percentile.build_percentile_rows` | in-memory score population | in-memory percentile rows | caller-supplied population | `HISTORICAL_PERCENTILE_NEEDS_AS_OF_POPULATION` | Reuse for historical population |
| `score_percentile.write_percentile_rows` | in-memory percentile rows | `rc_fundamental_score_percentile` | writes target-date rows | `DERIVED_TABLE_NEEDS_NEW_MATERIALIZATION` | Leave unchanged |
| `run_fundamental_score_percentile.py` | fundamentals DB, osakedata DB | `rc_fundamental_score_percentile` unless dry-run | current builder path | `CURRENT_ONLY_NO_CHANGE` | Leave unchanged |
| `inspect_historical_fundamental_percentile.py` | fundamentals DB, osakedata DB | stdout JSON/text | `score_effective_trading_date <= D` | `DIAGNOSTIC_ONLY` | New read-only CLI |
| `audit_historical_fundamental_percentile.py` | fundamentals DB, osakedata DB | temp JSON/CSV | bounded date/sample comparison | `DIAGNOSTIC_ONLY` | New read-only CLI |
| snapshot/report/UI percentile readers | `rc_fundamental_score_percentile` | display/report output | stored current percentile rows | `DISPLAY_ONLY` | Leave unchanged |

## Historical Population Policy

Historical policy marker:

```text
LATEST_AVAILABLE_PEER_SCORE_AS_OF_DATE
```

For target date `D`, the historical population selects at most one score row per ticker:

```text
score_effective_trading_date IS NOT NULL
score_effective_trading_date <= D
```

Ordering per ticker:

1. `score_effective_trading_date DESC`
2. `as_of_date DESC`
3. `score_rule_lifecycle DESC`
4. `rowid DESC`

Each peer can contribute a different fiscal period and effective date. Null-effective rows are excluded and counted. Tickers with future effective scores but no score available by `D` are excluded and counted.

## Semantics

The historical helper reuses existing percentile metric definitions from `score_percentile.py`:

- factor columns and higher/lower-is-better direction
- tie policy using average rank
- minimum available factor count
- sector and industry minimum population rules
- blended and lifecycle-weighted formulas
- sector/industry rank assignment

If the requested ticker has no available score, the result status is `NO_AVAILABLE_SCORE`. If the ticker has an available score but the selected peer population is below the current market minimum, the result status is `UNIVERSE_TOO_SMALL`.

## Persistence Decision

Decision: `QUERY_TIME_ONLY`.

The current percentile table stores current-style target-date rows and cannot cleanly represent historical effective-date-safe populations without changing semantics. A daily historical table would be unnecessarily large. Query-time calculation is fast enough for research use, and future persistence can be added as event-date materialization or a separate research table if needed.

No database tables, production percentile rows, or indexes were added in this phase.

## Performance Findings

Measured against `fundamentals_usa.db` and `/home/kalle/projects/rawcandle/data/osakedata.db`:

- one ticker/date, AAPL on `2026-04-30`: about 0.56 seconds
- full peer selection for `2026-04-30`: about 0.21 seconds, 2743 selected peers
- bounded 3-date audit with 250 rows per date: about 1.32 seconds

Existing index `idx_fundamental_ttm_score_effective_date(ticker, score_effective_trading_date)` was sufficient for this phase.

## Representative Checks

Real DB representative checks confirmed that own score rows switch only on their effective dates:

- AAPL switched from `2025-12-27` effective `2026-01-29` to `2026-03-28` effective `2026-04-30`.
- MSFT switched from `2025-12-31` effective `2026-01-28` to `2026-03-31` effective `2026-04-29`.
- NVDA switched from `2026-01-25` effective `2026-02-25` to `2026-04-26` effective `2026-05-20`.
- GIS, LMT, BBY, XOM, JPM, and ARWR showed the same boundary behavior.
- AVNS and DGXX have no resolved score effective dates and return `NO_AVAILABLE_SCORE`.

Recent bounded audit dates:

```text
2026-07-29
2026-07-05
2026-06-17
```

Audit summary for metric `fundamental_score_percentile_blended_lifecycle_weighted`:

- rows evaluated: 593
- current-vs-safe differences: 593
- material differences at 5 percentile points: 36
- median absolute difference: 0.5586
- p95 absolute difference: 8.2045
- max absolute difference: 33.0218
- peer population min/median/max: 2748 / 2748 / 2748
- excluded null-effective score count across dates: 564
- excluded no-available score count across dates: 0

## Downstream Implications

Valuation, snapshots, states, reports, and UI still read the current percentile table and remain unchanged. A later downstream phase can decide whether to:

1. keep historical percentile as query-time research only;
2. materialize selected event dates;
3. add a separate research table for selected date batches.

The next recommended phase is a historical snapshot or valuation design that consumes the query-time historical percentile helper without changing current/latest screens.
