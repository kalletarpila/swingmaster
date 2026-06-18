# SwingMaster Ticker Identity Contract Phase 2

## A. Executive summary

Phase 2 validates and documents the current SwingMaster fundamentals identity contract. The current accepted identity is:

```text
market + ticker
```

This is intentional for now. SwingMaster does not introduce a company/security-master in this phase, and current code should continue to treat SEC CIKs, Yahoo symbols, and other provider identifiers as provider mappings rather than canonical security identity.

This matters for future ESS integration because ESS is expected to select a ticker universe from canonical market/technical data first, then enrich those selected rows from SwingMaster fundamentals point-in-time outputs. If SwingMaster does not make its current identity assumptions explicit, later ESS adapter, valuation, percentile, and vintage work can accidentally mix markets, providers, or current metadata with historical rows.

The contract is acceptable for the next low-risk phases. The current domain rule is that `.HE` tickers always belong to `omxh`, non-`.HE` tickers belong to USA unless an explicit market override says otherwise, and the same canonical stock ticker is not expected to exist on two markets. Several fundamentals tables remain ticker-only and should be interpreted under this current ticker convention and DB/file context until a later migration design explicitly changes that.

## B. Contract rules

1. Canonical in-process ticker values should be normalized consistently before fundamentals reads/writes. Current repo evidence most often uses `ticker.upper()`.
2. For USA tickers, uppercase ticker without an exchange suffix is expected unless repo evidence says otherwise.
3. For OMXH, a ticker ending in `.HE` means `market="omxh"`.
4. The same canonical stock ticker is not expected to exist on two markets in the current SwingMaster universe.
5. Any cross-DB join to `osakedata.db` must include `market` when reading prices or metadata because those external tables are market-partitioned.
6. Fundamentals core tables that are ticker-only must be interpreted under the current ticker convention and DB/file context.
7. SEC CIK is a provider mapping used to fetch SEC data, not canonical SwingMaster security identity.
8. Yahoo `symbol` is a provider mapping/staging identifier, not canonical SwingMaster security identity.
9. Later ESS-facing reads should accept or produce `market + ticker`, not ticker-only identifiers.
10. No company/security-master should be added until a separate design proves `market + ticker` is insufficient.

## C. Current repo evidence

### Valuation path

`swingmaster/cli/run_fundamental_valuation.py` uses `market` in all `osakedata` price/universe reads:

- `load_market_ticker_universe()` reads distinct `osake` from `osakedata` with `WHERE market = ?`, then uppercases the returned symbols.
- `load_latest_close_price()` reads `close` from `osakedata` with `WHERE osake = ? AND market = ? AND pvm <= ?`.
- TTM and quarterly inputs are read from ticker-only fundamentals tables, so the fundamentals DB/file context is currently part of the market scope.

Existing valuation tests use market-specific temp `osakedata` rows and `.HE` OMXH tickers. The contract requires valuation calls to pass the correct market for the ticker convention.

### Percentile path

`swingmaster/fundamentals/score_percentile.py` uses `market` for metadata:

- `load_latest_percentile_snapshot()` selects latest `rc_fundamental_ttm` rows by ticker/as-of date.
- `load_market_metadata()` reads `ticker`, `sector`, and `industry` from `ticker_meta` with `WHERE market = ?`.
- Percentile rows persist `sector`, `industry`, `target_date`, `universe_size`, `sector_size`, and `industry_size`, but not a frozen universe version/hash.

Phase 2 test coverage now includes a temp metadata case for a `.HE` ticker under `omxh`.

### Quarter-state path

`swingmaster/cli/run_fundamental_quarter_state.py` stores both ticker and market in `rc_fundamental_quarter_state`, but migration `025` defines `PRIMARY KEY (ticker)`.

Repo evidence:

- `infer_market()` returns `omxh` for tickers ending in `.HE`, otherwise `usa`.
- `infer_primary_source()` returns `yahoo` for `omxh`, otherwise `sec_edgar`.
- `mark_detected_period()` uppercases ticker, lowercases explicit market, and stores market plus primary source.
- `upsert_state_from_quarterly()` infers market from ticker when syncing from ticker-only quarterly rows.

Phase 2 test coverage now includes explicit market override behavior for `mark_detected_period()`.

### SEC CIK path

`swingmaster/fundamentals/sec_edgar.py` treats CIK as a provider lookup:

- `load_ticker_cik_map()` fetches SEC company ticker metadata and maps uppercase ticker to zero-padded CIK.
- `resolve_cik()` uppercases the input ticker and returns the provider CIK.
- Extracted SEC raw rows are written with canonical-looking `ticker` plus `source='sec_edgar'`.

No persisted ticker-to-CIK mapping table is visible in the current repo. This supports the contract that CIK is a provider mapping, not the canonical identity key.

### Yahoo path

Yahoo paths use `symbol` in provider-specific tables and bridge/enrich that data into generic ticker-keyed fundamentals:

- `rc_fundamental_yahoo_raw` stores `market`, `provider`, `symbol`, payload JSON, `payload_hash`, status, load time, and run.
- `rc_fundamental_yahoo_quarterly` uses primary key `(market, symbol, period_end_date)`.
- `run_fundamental_yahoo_fallback_enrich.py` uppercases ticker values when loading/updating generic quarterly rows and reads Yahoo rows by `(market, symbol)`.

This supports treating Yahoo symbol as a provider/staging mapping. It also shows the risk that generic `rc_fundamental_quarterly` has no `market` column after bridge/enrichment.

### UI command builder/config path

`ui_fundamental_pipeline/command_builder.py` passes market explicitly to the main update and percentile paths:

- USA update command uses the USA fundamentals DB, configured `OSAKEDATA_DB`, and `--market usa`.
- FIN update command uses the FIN fundamentals DB, configured `OSAKEDATA_DB`, and `--market omxh`.
- Percentile command selects the fundamentals DB by market and passes `--market`.
- Snapshot command selects fundamentals DB by market and passes external `OSAKEDATA_DB` and `ANALYSIS_DB` for optional technical sections.

This reinforces the current market-scoped-by-DB-context model for ticker-only fundamentals tables.

### Table schemas

Core schema evidence:

- `rc_fundamental_statement_raw`: ticker-only primary key component, no `market`.
- `rc_fundamental_quarterly`: primary key `(ticker, period_end_date)`, no `market`.
- `rc_fundamental_ttm`: primary key `(ticker, as_of_date)`, no `market`.
- `rc_fundamental_score_percentile`: primary key `(ticker, target_date, rule_id)`, no `market`.
- `rc_fundamental_valuation`: primary key `(ticker, as_of_date)`, no `market`.
- `rc_fundamental_yahoo_raw`: includes `market` and `symbol`.
- `rc_fundamental_yahoo_quarterly`: primary key `(market, symbol, period_end_date)`.
- `rc_fundamental_quarter_state`: includes `market`, but primary key is `ticker`.
- `rc_fundamental_reporting_frequency_classification`: primary key includes `(ticker, market, as_of_date, classifier_version, run_id)`.
- `rc_fundamental_missing_period_recovery_check`: primary key includes `ticker` and `market`.

## D. Risk table

| Risk | Current severity | Current mitigation | Blocks next phases? | Later recommended fix |
| --- | --- | --- | --- | --- |
| Unexpected duplicate canonical ticker across markets | Low under current domain rule | Current convention says `.HE` is always `omxh`, non-`.HE` is USA unless explicitly overridden, and the same canonical stock is not expected on two markets. | Does not block Phase 3. | Keep a preflight check for unexpected duplicates in external metadata; do not design schema around a non-current case. |
| `.HE` suffix rule is not centralized | Low/Medium | `infer_market()` and price behavior helpers implement `.HE => omxh`; UI passes explicit market for core commands. | Does not block Phase 3. | Centralize/document normalization if later code starts sharing more identity logic. |
| Ticker-only primary keys in core tables | Low/Medium | Current ticker convention plus separate USA/FIN DB paths make ticker-only tables acceptable today. | Does not block read-only preflight; must be considered in PIT/vintage design. | Phase 4 migration design should decide whether explicit `market` columns are needed or whether per-market DB separation remains the contract. |
| Core tables without `market` | Low/Medium | UI command builder selects USA vs FIN DB; valuation/percentile use market for external joins. | Does not block Phase 3; relevant for Phase 10. | Adapter output should include market from request/context even if storage remains ticker-only. |
| Provider symbol mismatch | Medium | Yahoo staging tables include `market`; fallback reads `(market, symbol)` and writes generic ticker. | Does not block Phase 3. | Add provider mapping audit/preflight before broad provider expansion. |
| Ticker rename/corporate action not modeled | Medium | Not currently modeled; current contract accepts ticker as present symbol. | Does not block immediate phases, but limits long historical PIT correctness. | Defer company/security-master or symbol history until a concrete need appears. |
| Yahoo symbol and canonical ticker diverging | Medium | Current code usually passes same uppercased ticker as Yahoo symbol. | Does not block Phase 3. | Add explicit provider-symbol mapping only if real divergences are observed. |
| SEC CIK mapping not persisted | Low/Medium | SEC resolver maps current ticker to CIK dynamically. | Does not block Phase 3; relevant for vintage/reproducibility. | Phase 4 should consider storing provider mapping/vintage metadata or source hash. |
| Percentile ranks use current metadata | Medium | `target_date` is stored; metadata is market-filtered. | Does not block Phase 3; important for Phase 7. | Freeze universe and peer metadata with universe version/hash. |
| Valuation table lacks `market` | Medium | Valuation command receives `market`; table is market-scoped by DB/file context. | Does not block Phase 6 design, but must be explicit. | Phase 6 should add/derive market-date and price-source lineage; later migration may add market. |

## E. Phase 2 conclusion

Current `market + ticker` contract is acceptable for:

- Phase 3 read-only ESS readiness preflight: yes, if preflight accepts explicit market and treats ticker-only tables as market-scoped by DB/file context.
- Phase 4 reported fundamentals PIT/vintage design: yes as a design baseline, but Phase 4 must decide whether vintage tables need explicit `market`.
- Phase 6 valuation hardening: yes, because valuation already uses `market` for `osakedata` price reads; hardening should add clearer price lineage and market-date semantics.
- Phase 10 adapter contract: yes as the adapter key, and adapter output should always include explicit `market` and `ticker` even if current storage remains ticker-only.

Recommended next action remains Phase 3: build a read-only ESS readiness preflight using existing tables only. It should validate market/ticker assumptions without provider calls, schema changes, DB writes, or ESS decision integration.
