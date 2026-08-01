# Historical Fundamental Valuation Queries

This phase adds query-time historical valuation only. It does not create or alter
database tables, persist valuation rows, modify the current valuation CLI, or
write outside `temp/` for runtime artifacts.

## Query Policy

Historical valuation uses policy `LATEST_AVAILABLE_TTM_AND_CLOSE_AS_OF_DATE`:

- Select TTM fundamentals from `rc_fundamental_ttm` using
  `effective_trading_date <= requested_as_of_date`.
- Do not fall back to a TTM row whose effective trading date is after the
  requested date.
- Select price from `osakedata` using the latest close where
  `pvm <= requested_as_of_date`.
- Report whether the selected price is `EXACT_TRADING_DATE`,
  `PREVIOUS_TRADING_DATE`, or `NO_PRICE_AVAILABLE`.
- Reuse the existing current valuation formulas and null/status behavior from
  `swingmaster.cli.run_fundamental_valuation.build_valuation_row`.

The implementation intentionally does not use vintage/provenance tables for
selection. Those tables are unaffected and remain available for later phases.

## Formula Reuse

The historical query path reuses the current valuation row builder for:

- `market_cap = close_price * shares_outstanding`
- `enterprise_value = market_cap + total_debt - cash`
- `valuation_ev_ebit = enterprise_value / ebit_ttm`
- `valuation_fcf_yield = fcf_ttm / market_cap`
- `valuation_ebit_margin`
- expensive bucket thresholds and stale-fundamental status

Shares, cash, and debt are loaded from `rc_fundamental_quarterly` for the
quarter matching the selected TTM row's `latest_period_end_date`.

Current valuation has no active PE or EV/EBITDA metric, so the audit reports PE
material differences as zero and uses `valuation_ev_ebit` as the implemented EV
multiple comparison.

## Statuses

The query result keeps the raw current valuation row status inside
`valuation_row["valuation_status"]` and also exposes a historical query status:

- `OK`
- `NO_AVAILABLE_TTM`
- `NO_PRICE_AVAILABLE`
- `MISSING_REQUIRED_INPUT`
- `INVALID_DENOMINATOR`
- `CURRENCY_MISMATCH`

`CURRENCY_MISMATCH` is reserved. The current valuation formula path does not
perform FX conversion, and this phase preserves that behavior.

## CLIs

Inspect one ticker/date:

```bash
.venv/bin/python -m swingmaster.cli.inspect_historical_fundamental_valuation \
  --fundamentals-db fundamentals_usa.db \
  --price-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --ticker AAPL \
  --as-of-date 2026-04-30 \
  --market usa \
  --include-current-comparison \
  --json
```

Audit a bounded universe and write artifacts under `temp/`:

```bash
.venv/bin/python -m swingmaster.cli.audit_historical_fundamental_valuation \
  --fundamentals-db fundamentals_usa.db \
  --price-db /home/kalle/projects/rawcandle/data/osakedata.db \
  --market usa \
  --as-of-date 2026-04-30 \
  --ticker AAPL --ticker MSFT --ticker JPM --ticker XOM --ticker NVDA \
  --output-root temp/fundamental_historical_valuation/example \
  --json
```

The audit CLI validates runtime output paths so JSON, CSV, summary, and progress
log artifacts stay under the repository `temp/` directory.

## Real DB Validation

Read-only validation on `2026-08-01` used:

- fundamentals DB: `fundamentals_usa.db`
- price DB: `/home/kalle/projects/rawcandle/data/osakedata.db`
- representative tickers: `AAPL`, `MSFT`, `JPM`, `XOM`, `NVDA`, `GIS`, `LMT`,
  `BBY`, `ARWR`, `DGXX`, `AVNS`

For `AAPL` on `2026-04-30`, both current-style and effective-date-safe
selection used TTM `2026-03-28`, effective trading date `2026-04-30`, and exact
price date `2026-04-30`. The returned valuation was `OK` with EV/EBIT
`27.367287762642643` and FCF yield `0.03232691205837047`.

The 11-ticker audit for `2026-04-30` produced:

- rows evaluated: `11`
- rows with different TTM selection: `5`
- rows with no available TTM: `2`
- rows with no price: `0`
- material EV multiple differences: `0`
- material FCF yield differences: `0`
- median absolute EV/EBIT difference: `0.0`
- median absolute FCF yield difference: `0.0`

Boundary checks around each ticker's latest effective TTM confirmed that the
selector changes TTM only on or after `effective_trading_date`. Example:

- `AAPL` on `2026-04-29` selected TTM `2025-12-27`; on `2026-04-30` selected
  TTM `2026-03-28`.
- `MSFT` on `2026-04-28` selected TTM `2025-12-31`; on `2026-04-29` selected
  TTM `2026-03-31`.

Row counts before and after validation were unchanged:

- `rc_fundamental_ttm`: `146638`
- `rc_fundamental_valuation`: `41094`

## Deferred Work

Later phases can decide whether to persist historical valuation snapshots,
connect historical percentiles by default, add explicit FX policy, or add
state/snapshot integration. This phase leaves those concerns out of the write
path.
