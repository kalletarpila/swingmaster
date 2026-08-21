# Fundamentals V3 Phase 2A Yahoo Fetcher Validation

Date: 2026-08-21

Classification: `FUNDAMENTALS_V3_PHASE2A_EXISTING_YAHOO_FETCHER_REUSABLE_WITH_THIN_ADAPTER`

## Scope And Safety

This phase validated the existing Yahoo quarterly fundamentals path for future V3 bootstrap use.
It did not implement Yahoo bootstrap, create/populate `rc_fundamentals_v3.db`, write canonical V3
tables, run SEC/SimFin, execute production Check/Update, or change RawCandle.

Live validation used 8 approved V3 tickers and wrote only temp artifacts under:

```text
temp/fundamentals_v3_phase2a_yahoo_probe/20260821T123241Z/
```

An initial non-escalated probe failed with `Could not resolve host: guce.yahoo.com`, which was a
Codex network sandbox limitation. The prompt-authorized escalated probe succeeded.

## Existing Fetch Path

Authoritative quarterly fundamentals raw fetch path:

```text
swingmaster.cli.run_fundamental_yahoo_raw_load.run_fundamental_yahoo_raw_load
  -> run_batch
  -> swingmaster.cli.run_fundamental_yahoo_audit.run_yahoo_audit
  -> swingmaster.fundamentals.providers.yahoo.YahooFinanceClient.get_raw_payload
  -> yfinance.Ticker(symbol)
  -> ticker.info
  -> ticker.fast_info
  -> ticker.quarterly_income_stmt
  -> ticker.quarterly_balance_sheet
  -> ticker.quarterly_cashflow
  -> rc_fundamental_yahoo_raw
```

Raw-to-normalized quarterly path:

```text
rc_fundamental_yahoo_raw
  -> run_fundamental_yahoo_quarterly_prototype.build_normalized_rows
  -> run_fundamental_yahoo_quarterly_write.run_yahoo_quarterly_write
  -> rc_fundamental_yahoo_quarterly
```

Downstream legacy bridge path:

```text
rc_fundamental_yahoo_quarterly
  -> run_fundamental_yahoo_to_quarterly.run_yahoo_to_quarterly
  -> rc_fundamental_quarterly
```

Other Yahoo paths exist for earnings calendar/events and V2 field-specific fallbacks. They are not
the historical quarterly fundamentals raw fetcher.

## Implementation Properties

| Property | Current behavior |
| --- | --- |
| Module | `swingmaster/fundamentals/providers/yahoo.py` |
| Public fetch entrypoint | `YahooFinanceClient.get_raw_payload(symbol)` |
| CLI raw entrypoints | `run_fundamental_yahoo_audit.py`, `run_fundamental_yahoo_raw_load.py` |
| HTTP client | `yfinance`; repository code does not expose explicit URL/endpoints |
| Endpoint parameters | Provider symbol only at SwingMaster layer |
| Symbol mapping | Caller supplies Yahoo symbol; USA tickers are used directly |
| Raw output | `info`, `fast_info`, quarterly income/balance/cashflow payloads |
| Raw persistence | `rc_fundamental_yahoo_raw` |
| Normalized persistence | `rc_fundamental_yahoo_quarterly` |
| Single ticker | Yes |
| Batch-capable | Yes, via `run_fundamental_yahoo_raw_load` |
| Resumable | Partially: deterministic batches/run ids, but no durable per-ticker checkpoint |
| Idempotent raw writes | No unique raw constraint; repeated raw loads append rows |
| Idempotent normalized writes | Yes by `(market, symbol, period_end_date)` upsert |
| Legacy/V2 tied | Raw fetch is legacy-schema tied only by destination table; provider client is reusable |
| Reusable independently | Yes, through `YahooFinanceClient` and a thin V3 raw-cache adapter |

## Throttling And Retry Contract

| Item | Current behavior |
| --- | --- |
| Raw audit per symbol | No explicit sleep |
| USA raw-load batches | Deterministic batches; no explicit sleep between symbols or batches |
| OMXH batch pipeline | `YAHOO_TICKER_DELAY_SECONDS = 0.5` between symbols |
| Retry count | No explicit retry in SwingMaster Yahoo fetcher |
| Exponential backoff | None |
| HTTP 429 handling | yfinance exception is caught by `run_yahoo_audit` and stored as `ERROR` |
| 5xx/connection handling | yfinance exception is caught and stored as `ERROR` |
| Empty response handling | `EMPTY` if no usable quarterly statement data |
| Pause configurability | 0.5s OMXH pause is hard-coded; raw-load batch size is CLI configurable |

Phase 2A did not weaken throttling. The live probe used one symbol per raw-audit call with the
existing 0.5s ticker pause constant from the Yahoo batch path.

## Provider Output Contract

The current fetcher returns a combination:

- raw provider-like payloads serialized from yfinance DataFrames
- partially normalized quarterly rows after `build_normalized_rows`
- provider-specific cached rows in `rc_fundamental_yahoo_quarterly`

Raw row metadata:

- `market`
- `provider='yahoo'`
- `symbol`
- statement JSON payloads
- `payload_hash`
- `status`
- `error_message`
- local `loaded_at_utc`
- `run_id`

Normalized quarterly rows include:

- `period_end_date`
- mapped statement values
- `shares_source`
- `shares_quality`

They do not include direct fiscal-year/fiscal-quarter labels, publication dates, provider HTTP
request metadata, endpoint metadata, or source field provenance per normalized value.

## Field Inventory

| V3 candidate field | Current Yahoo support |
| --- | --- |
| `revenue` | `DIRECTLY_AVAILABLE` via `Total Revenue` / `Operating Revenue` |
| `gross_profit` | `DIRECTLY_AVAILABLE` via `Gross Profit` |
| `operating_income` | `DIRECTLY_AVAILABLE` via `Operating Income` / `Total Operating Income As Reported` |
| `ebit` | `DIRECTLY_AVAILABLE` via `EBIT`; not filled from operating income |
| `ebitda` | `DIRECTLY_AVAILABLE` via `EBITDA`; `Normalized EBITDA` also appears raw but is not mapped |
| `net_income` | `DIRECTLY_AVAILABLE` via mapped net-income aliases |
| `operating_cashflow` | `DIRECTLY_AVAILABLE` via `Operating Cash Flow` / continuing operations alias |
| `capex` | `DIRECTLY_AVAILABLE` via `Capital Expenditure`, `Purchase Of PPE`, or `Net PPE Purchase And Sale` |
| `free_cashflow` | `DIRECTLY_AVAILABLE`; also `DERIVABLE` from OCF + capex when direct FCF is missing |
| `cash` | `DIRECTLY_AVAILABLE` via cash/cash-equivalents aliases |
| `short-term debt` | `AVAILABLE_UNDER_PROVIDER_ALIAS` in raw balance fields, not normalized today |
| `long-term debt` | `AVAILABLE_UNDER_PROVIDER_ALIAS` in raw balance fields, not normalized today |
| `total_debt` | `DIRECTLY_AVAILABLE`; current fallback also maps `Long Term Debt And Capital Lease Obligation` |
| `shares_outstanding` | `DIRECTLY_AVAILABLE` via `Ordinary Shares Number`; fallback via issued minus treasury or current snapshot |

Important semantic guardrail: current normalization does not map Yahoo `Operating Income` to EBIT.

## Live Probe Results

Probe tickers: `AAPL`, `MSFT`, `WMT`, `NVDA`, `COST`, `UBER`, `SNOW`, `GE`.

Timing:

- elapsed seconds: `17.324`
- provider method calls: `8`
- underlying HTTP request count: not exposed by current yfinance fetcher
- pause between symbols: `0.5` seconds

Summary:

| Symbol | Status | Periods | Earliest | Latest | Income periods | Balance periods | Cashflow periods |
| --- | --- | ---: | --- | --- | ---: | ---: | ---: |
| AAPL | OK | 7 | 2024-12-31 | 2026-06-30 | 5 | 7 | 6 |
| MSFT | OK | 7 | 2024-12-31 | 2026-06-30 | 5 | 7 | 7 |
| WMT | OK | 6 | 2025-01-31 | 2026-04-30 | 6 | 6 | 6 |
| NVDA | OK | 6 | 2025-01-31 | 2026-04-30 | 5 | 6 | 6 |
| COST | OK | 7 | 2024-11-30 | 2026-05-31 | 7 | 5 | 7 |
| UBER | OK | 7 | 2024-12-31 | 2026-06-30 | 6 | 7 | 7 |
| SNOW | OK | 6 | 2025-01-31 | 2026-04-30 | 5 | 6 | 6 |
| GE | OK | 7 | 2024-12-31 | 2026-06-30 | 7 | 7 | 7 |

Across the probe, normalized core field counts were generally 5 periods for revenue, EBITDA, FCF,
cash, total debt, and operating cashflow/capex. Shares had 6-7 period markers because balance-sheet
share fields can exist where income/cashflow values are absent.

## Historical Depth

The code requests yfinance quarterly statement properties; it does not specify a historical start
date or long-range query window at the SwingMaster layer.

Yahoo actually returned only recent quarterly history in this probe:

- earliest normalized period ranged from `2024-11-30` to `2025-01-31`
- latest normalized period ranged from `2026-04-30` to `2026-06-30`
- statement depths differed by statement type for several tickers
- this is not enough for full V3 historical bootstrap by itself

Do not extrapolate this 8-ticker probe to the full 2,812 ticker universe.

## Fiscal Identity And Date Semantics

Fiscal identity support classification: `PERIOD_END_ONLY`.

The existing yfinance statement payloads expose statement columns that behave as period-end dates.
They do not provide direct `fiscal_year + fiscal_quarter` labels. Non-calendar fiscal companies in
the probe, such as WMT/NVDA/COST/SNOW, still returned period-end-style dates without direct fiscal
labels.

Date separation:

| Date concept | Current Yahoo path support |
| --- | --- |
| Fiscal period end | Supported as statement column date |
| Fiscal year/quarter label | Not supported |
| Publish/result availability date | Not supported by this path |
| Provider observation timestamp | Not directly exposed |
| Local fetch timestamp | Supported as `loaded_at_utc` |

V3 must not map Yahoo statement dates to `publish_date`. A later adapter can use them as period-end
evidence only.

## Raw Cache Fit

The existing fetcher is a good fit for the Phase 2B preferred shape:

```text
existing Yahoo fetcher
        ↓
thin V3 adapter
        ↓
V3 raw cache
        ↓
V3 normalizer
```

Recommended minimal Phase 2B adapter:

- call `YahooFinanceClient.get_raw_payload(symbol)` directly
- preserve raw payload JSON before normalization
- store into `rc_fundamentals_v3_raw.db` through `V3RawCacheRepository`
- copy compact metadata: provider, symbol, run id, payload hash, local observed/fetched timestamp,
  status, and error message
- keep current normalization separate and provider-specific
- do not write canonical V3 rows from the fetcher
- add explicit per-ticker checkpoint/resume metadata before full bootstrap

Recommended follow-up before production bootstrap:

- expose or wrap request/error metadata if yfinance allows it
- decide whether to retain the current 0.5s ticker pause or introduce a configurable V3 bootstrap
  throttle that is at least as conservative
- add retry/backoff policy around provider calls without modifying canonical semantics
- do not rely on Yahoo yfinance quarterly statements for long historical depth or direct fiscal
  labels
