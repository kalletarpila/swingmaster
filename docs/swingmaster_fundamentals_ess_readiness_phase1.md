# SwingMaster Fundamentals ESS Readiness Phase 1

## 1. Executive summary

This document is a documentation-only repo audit and implementation plan for making SwingMaster fundamentals easier to consume from a future ESS workflow. Phase 1 does not implement ESS integration. It maps the current SwingMaster fundamentals code, tables, UI command paths, and snapshot sections to a target ESS-ready architecture.

Repo evidence suggests the intended future boundary is:

- `rawcandle` / `ec_` owns market data and canonical technical facts.
- SwingMaster owns ticker identity contract, reported fundamentals, derived fundamental metrics, market valuation metrics, cross-sectional fundamental ranks, and fundamental events/expectations.
- ESS should later read `ec_` first, choose a ticker universe, then read SwingMaster as point-in-time enrichment.

This phase explicitly does not change runtime code, tests, provider behavior, SEC EDGAR logic, Yahoo/yfinance fallback logic, normalization, scoring, valuation, percentile ranking, snapshots, UI, scheduler, migrations, or real DB files. It also does not create a company/security master.

The current identity contract should remain lightweight. `market + ticker` is accepted because current fundamentals tables and the `osakedata` valuation/metadata paths join through ticker symbols, and repo code consistently normalizes tickers to uppercase in the relevant fundamentals paths. Provider identifiers such as CIK and Yahoo symbols should be treated as provider mappings, not as a full security-master model.

Technical price structure must remain external. Current snapshot code can append price behavior, Dow, candlestick, divergence, moving-average, and technical relevance sections, but those sections read from `osakedata` and analysis-style readers. For ESS, these should be treated as external technical references or presentation-only append sections, not canonical SwingMaster fundamentals.

## 2. Target architecture classes

| Class | Purpose | Update trigger | Date semantics | Likely current tables/paths | ESS relevance | Explicit non-goals |
| --- | --- | --- | --- | --- | --- | --- |
| `0. ticker_identity_contract` | Lightweight identity model for current SwingMaster joins. `market + ticker` is the accepted key for now. CIK/Yahoo symbols are provider mappings. | Ticker-universe setup, UI commands, valuation/ranking runs, provider ingest commands. | Identity should be stable across `target_date`, `market_date`, and provider ingest dates. | `run_fundamental_quarter_state.py`, `run_fundamental_valuation.py`, `score_percentile.py`, `ui_fundamental_pipeline/command_builder.py`, `price_behavior_snapshot.py`. | ESS needs an unambiguous key to request point-in-time enrichment after it selects a universe from `ec_`. | Do not build company/security-master yet. Do not model listings, share classes, CUSIP/ISIN, or corporate actions in Phase 1. |
| `1. reported_fundamentals` | Company-reported period data that should not change because market prices change. Examples: revenue, net income, cash, debt, FCF, shares, currency, `period_end_date`. | SEC/Yahoo provider ingest, raw reconstruction, fallback enrichment. | `period_end_date` is present; future design needs `filed_at_utc`, `available_at_utc`, ingestion time, vintage/restatement metadata. | `rc_fundamental_statement_raw`, `rc_fundamental_quarterly`, `rc_fundamental_yahoo_raw`, `rc_fundamental_yahoo_quarterly`, `rc_fundamental_quarterly_enrichment_audit`, SEC/Yahoo CLIs. | ESS needs point-in-time fundamentals that would have been available at decision time. | Do not let daily price moves change this class. Do not merge provider identity with company master in Phase 1. |
| `2. derived_fundamental_metrics` | Metrics calculated from reported fundamentals only. Examples: TTM, growth, margins, lifecycle, rule-based score. | Quarterly rows change or are rebuilt; lifecycle/scoring commands run. | Current `as_of_date` usually follows latest period end; future design needs `computed_at_utc`, input vintage hash, config hash. | `rc_fundamental_ttm`, `build_ttm.py`, `lifecycle.py`, `score.py`, `run_fundamental_quarterly_to_ttm.py`. | ESS needs deterministic point-in-time derived metrics independent of current price. | Do not include daily close, market cap, valuation, or technical indicators. |
| `3. market_valuation_metrics` | Metrics combining fundamentals with daily price/market date. Examples: market cap, EV, EV/EBIT, P/FCF style yield, valuation bucket/status. | Valuation command after market data date is known; USA quarter update appears to call valuation after ticker processing. | Must use explicit `market_date` / `as_of_date`; current table uses `as_of_date` for valuation date and stores fundamental staleness metadata. | `rc_fundamental_valuation`, `run_fundamental_valuation.py`, `run_fundamental_quarter_update.py`, `load_latest_valuation_snapshot()`. | ESS needs to know which market price and which fundamental vintage were combined. | Do not make SwingMaster a canonical price store. Do not compute technical price structure here. |
| `4. cross_sectional_fundamental_ranks` | Relative ranks/percentiles against market, sector, industry, or future universe. | Explicit percentile command for a target date. | Current `target_date` and `as_of_date` exist; future design needs universe version/hash, peer scope, peer count, decision cutoff. | `rc_fundamental_score_percentile`, `score_percentile.py`, `run_fundamental_score_percentile.py`, snapshot percentile reads. | ESS needs historical ranks that do not accidentally use a future/current universe. | Do not rank historical data with today's universe. Do not mix ranking with provider ingest. |
| `5. fundamental_events_and_expectations` | Event calendar and expectations context. Examples: earnings date/time/status, consensus EPS/revenue, revisions, surprises. | Event-calendar provider ingest or manual event sync in a future phase. | Needs event date/time, known-at time, status, revision time, and decision cutoff. | No clear current canonical table found in the audited fundamentals paths. Snapshot price behavior has descriptive post-report price fields, not an event calendar. | ESS will need known future/near-term earnings context without lookahead. | First implementation should probably start with earnings calendar only, not analyst revisions/consensus/surprises. |
| `6. external_technical_reference` | Non-fundamental technical context from rawcandle/ec_/analysis sources. | Snapshot generation or future adapter reads after technical systems are refreshed. | Uses technical `as_of_date`, latest valid close date, signal confirmation dates, and lookback windows. | `price_behavior_snapshot.py`, `run_fundamental_ticker_snapshot.py`, `analysis.*_reader` imports and optional snapshot sections. | ESS can reference technical context from canonical technical systems, but SwingMaster should not own it as fundamentals. | Do not duplicate canonical technical truth in SwingMaster. Do not persist Dow/candlestick/divergence/moving averages as fundamentals. |

## 3. Current table/path classification

| Item | Current classification | Reason | Data role | Update trigger | ESS-readiness gap | Suggested later phase |
| --- | --- | --- | --- | --- | --- | --- |
| `rc_fundamental_statement_raw` | `reported_fundamentals` | Migration `001` stores raw statement rows keyed by ticker, statement type, period end, field name; SEC code stores `source`, `retrieved_at_utc`, `run_id`. | Source/staging raw provider facts. | SEC/yfinance raw ingest. | No explicit `market`; SEC filed date is encoded in `field_name`, not first-class; no source hash/vintage id/restatement flag. | Phase 4 |
| `rc_fundamental_quarterly` | `reported_fundamentals` | Migration `001` stores normalized quarterly statement metrics by `(ticker, period_end_date)`. | Canonical normalized quarterly fundamentals by repo evidence. | SEC reconstruction, generic build, Yahoo bridge/fallback insert. | No `market`, provider provenance, `available_at_utc`, `statement_vintage_id`, or restatement handling. | Phase 4 |
| `rc_fundamental_yahoo_raw` | `reported_fundamentals` | Migration `017` stores raw Yahoo payloads with market, provider, symbol, payload hash, status, loaded time, run. | Provider raw cache/audit. | Yahoo audit/load. | Symbol is provider mapping; no mapping table; no explicit decision availability semantics. | Phase 4 |
| `rc_fundamental_yahoo_quarterly` | `reported_fundamentals` | Migration `018` stores normalized Yahoo quarterly rows by `(market, symbol, period_end_date)`. | Provider-specific normalized staging. | Yahoo quarterly write. | Separate symbol grain from generic `ticker`; no first-class provider mapping contract. | Phase 2 and Phase 4 |
| `rc_fundamental_quarterly_enrichment_audit` | `reported_fundamentals` | Migration `023` and fallback code record old/new field values, primary/fallback sources, status, run, created time. | Audit/provenance for Yahoo fallback enrichment. | Yahoo fallback enrich. | V2 extra columns are applied by migration runner, not visible in SQL file; no full vintage model. | Phase 4 |
| `rc_fundamental_ttm` | `derived_fundamental_metrics` | Migration `001` stores TTM, growth, margin, leverage, lifecycle and score fields. `build_ttm.py` derives rows from quarterly rows only. | Derived metrics and score storage. | Quarterly-to-TTM, lifecycle, scoring. | `as_of_date` is period-derived; no computed time, config hash, input vintage hash, or source quarterly vintage id. | Phase 5 |
| `rc_fundamental_score_percentile` | `cross_sectional_fundamental_ranks` | Migration `001` stores target date, sector/industry, universe/sector/industry sizes, percentiles and ranks. | Cross-sectional rank output. | Percentile command. | Uses current `ticker_meta` for sector/industry; no universe version/hash or frozen peer membership. | Phase 7 |
| `rc_fundamental_valuation` | `market_valuation_metrics` | Migration `019` creates valuation table; migration runner adds V2 fields used by `run_fundamental_valuation.py`, including valuation model, fundamental as-of date, staleness, assumed cash/debt flags. | Valuation output. | Valuation command and USA quarter update final step. | `as_of_date` is valuation date but not named `market_date`; no explicit price source id or price-match status. | Phase 6 |
| `rc_fundamental_quarter_state` | `ticker_identity_contract` / operational state | Migration `025` stores ticker, market, primary source, latest DB period, detected source period and ingest run IDs. | Operational state. | Quarter state sync/detection/ack. | Primary key is `ticker`, not `(market, ticker)`; acceptable under the current rule that `.HE` is always OMXH and the same canonical ticker is not expected on two markets. | Phase 2 |
| `rc_fundamental_reporting_frequency_classification` | `derived_fundamental_metrics` / operational audit | Migration `026` stores reporting frequency class, coverage, observed/missing period dates, classifier version, run. | Derived audit/classification. | Reporting-frequency audit. | Mostly FIN-oriented by current UI command evidence; no global PIT vintage contract. | Phase 5 |
| `rc_fundamental_missing_period_recovery_check` | `reported_fundamentals` / operational audit | Migration `027` stores missing period recovery status and found dates. | Audit/recovery check. | Missing-period recovery check. | Recovery status is useful, but not integrated into reported-fundamental vintage metadata. | Phase 4 |
| `run_fundamental_quarter_update.py` | Orchestration across reported, derived, valuation, and operational state | Code indicates USA branch can call SEC raw bootstrap, quarterly build, Yahoo fallback enrich, TTM, lifecycle, score, ack, and USA valuation. | Operational CLI/orchestrator. | New-quarter state rows. | Mixes several target classes in one path; future ESS adapter should read outputs, not call this. | Phase 2, Phase 6, Phase 10 |
| `run_fundamental_valuation.py` | `market_valuation_metrics` | Reads TTM/quarterly fundamentals and `osakedata` close prices; writes valuation rows. | Valuation computation. | Explicit valuation run or quarter update. | Needs clearer `market_date`, price input lineage, and future `ec_` parity status. | Phase 6 |
| `run_fundamental_score_percentile.py` / `score_percentile.py` | `cross_sectional_fundamental_ranks` | Loads latest TTM rows as of target date and joins `ticker_meta` sector/industry from osakedata. | Rank computation. | Explicit percentile run. | No frozen historical universe; sector/industry metadata may reflect current metadata. | Phase 7 |
| Current snapshot generation paths | Presentation/export over multiple classes | `run_fundamental_ticker_snapshot.py` reads TTM, quarterly, stored percentiles, valuation, and optional external technical readers. | Presentation/read/export. | Manual/UI snapshot command. | Combined snapshot mixes fundamental and technical context; not a clean ESS adapter contract. | Phase 9 and Phase 10 |
| `price_behavior_snapshot` section | `external_technical_reference` | `price_behavior_snapshot.py` reads `osakedata` OHLCV and benchmark rows; comment marks descriptive/reporting use. | Presentation-only technical/price context. | Optional snapshot flag. | Should not be persisted as SwingMaster canonical fundamentals. | Phase 9 |
| `dow_context_snapshot` | `external_technical_reference` | Snapshot code calls `read_stock_dow_structure_raw_export` with analysis DB and OHLCV DB. | External technical reference. | Optional snapshot flag. | Canonical owner should remain technical system/rawcandle/ec_. | Phase 9 |
| `dow_recent_events_60td` | `external_technical_reference` | Same Dow reader returns recent event rows over a 60 trading-day window. | External technical reference. | Optional snapshot flag. | Presentation-only in current path. | Phase 9 |
| `candlestick_events_60td` | `external_technical_reference` | Snapshot code calls `read_candlestick_signal_raw_export`. | External technical reference. | Optional snapshot flag. | Presentation-only in current path. | Phase 9 |
| `divergence_context_snapshot` | `external_technical_reference` | Snapshot code calls `read_divergence_signal_raw_export`. | External technical reference. | Optional snapshot flag. | Presentation-only in current path. | Phase 9 |
| `divergence_signals_60td` | `external_technical_reference` | Same divergence reader returns recent signal rows. | External technical reference. | Optional snapshot flag. | Presentation-only in current path. | Phase 9 |
| `moving_averages_60td` | `external_technical_reference` | Snapshot code calls `read_moving_average_raw_export` against OHLCV DB. | External technical reference. | Optional snapshot flag. | Presentation-only in current path. | Phase 9 |

## 4. Ticker identity contract

Current agreed identity model:

- The current key is `market + ticker`.
- `osakedata.db` and fundamentals are expected to join with the same ticker symbol.
- Current fundamentals code frequently normalizes tickers with `.upper()`.
- USA tickers are inferred when a ticker does not end with `.HE`; a ticker ending in `.HE` is always OMXH in the current contract.
- The same canonical stock ticker is not expected to exist on two markets.
- CIK and Yahoo symbols are provider mappings.
- No company/security-master is implemented in this phase.
- A future company/security-master remains an open option only if repo evidence or ESS needs show that `market + ticker` is insufficient.

Repo evidence:

- `run_fundamental_quarter_state.py` stores `ticker`, `market`, and `primary_source`; it infers `market="omxh"` for `.HE`, otherwise `usa`, and `primary_source="sec_edgar"` for USA.
- `run_fundamental_valuation.py` loads the market universe from `osakedata.osake`, uppercases symbols, reads TTM rows by `ticker`, and reads close prices from `osakedata` by `osake`, `market`, and date.
- `score_percentile.py` reads sector/industry metadata from `ticker_meta` by `market` and `ticker`.
- `ui_fundamental_pipeline/command_builder.py` passes market-specific fundamentals DB paths and the same configured `OSAKEDATA_DB` to update, percentile, and snapshot commands.
- `sec_edgar.py` maps ticker to CIK dynamically through SEC company tickers. This is a provider lookup, not a canonical identity table.
- Yahoo raw/quarterly tables use `symbol`; fallback paths compare this to uppercased ticker for current use.

Visible ambiguity:

- `rc_fundamental_quarter_state` has primary key `ticker`, not `(market, ticker)`.
- Several core tables do not have a `market` column.
- `.HE` suffix handling is a domain rule in code, not a DB constraint.
- There is no persisted provider mapping table for ticker to CIK or ticker to Yahoo symbol.

Phase 2 follow-up: [SwingMaster Ticker Identity Contract Phase 2](swingmaster_ticker_identity_contract_phase2.md) documents the current `market + ticker` contract, including the rule that `.HE` tickers are OMXH and the same canonical ticker is not expected on two markets.

## 5. Point-in-time readiness gaps

| Concept | Target class | Why ESS/backtest needs it | Current repo evidence | Likely later phase |
| --- | --- | --- | --- | --- |
| `period_end_date` | Reported fundamentals | Aligns reported values to fiscal/reporting period. | Present in raw, quarterly, Yahoo quarterly, audit/recovery tables. | Phase 4 |
| `filed_at_utc` | Reported fundamentals | Prevents using statements before filing. | SEC `filed` appears encoded inside raw SEC `field_name`; no first-class column. | Phase 4 |
| `available_at_utc` | Reported fundamentals | Captures when SwingMaster considers a fact usable. | Not clearly supported. | Phase 4 |
| `ingested_at_utc` | Reported fundamentals | Separates provider filing time from local ingest time. | `retrieved_at_utc`, `loaded_at_utc`, and `created_at_utc` exist in some raw/audit tables. | Phase 4 |
| `computed_at_utc` | Derived metrics, valuation, ranks | Captures when a derived output was computed. | `created_at_utc` exists in percentiles, valuation, Yahoo quarterly and audits; not uniform for TTM/scoring. | Phase 5 |
| `market_date` | Market valuation metrics | Identifies the market price date used. | Valuation uses `as_of_date`; quarter update resolves latest close date from `osakedata`. | Phase 6 |
| `target_date` | Cross-sectional ranks | Identifies rank snapshot date. | Present in `rc_fundamental_score_percentile`. | Phase 7 |
| `as_of_utc` | All PIT outputs | Allows decision-time reads at sub-day granularity. | Not uniformly present; mostly date strings and created timestamps. | Phase 10 |
| `decision_cutoff_utc` | ESS adapter/ranks/events | Prevents lookahead in ESS decisions. | Not present. | Phase 10 |
| `statement_vintage_id` | Reported fundamentals | Tracks restatements and provider updates for the same period. | Not present. | Phase 4 |
| `source_hash` | Reported fundamentals | Detects raw payload changes and reproducibility. | Yahoo raw has `payload_hash`; SEC raw has no explicit source hash. | Phase 4 |
| `revision_number` | Reported fundamentals/events | Orders changes to the same fact or expectation. | Not present. | Phase 4 and Phase 8 |
| `is_restated` | Reported fundamentals | Flags restated statements. | Not present. | Phase 4 |
| `run_id` | All write outputs | Links rows to a deterministic execution. | Present in core fundamentals tables and operational audit tables. | Already partial; harden in Phases 4-7 |
| `config_hash` | Derived metrics/ranks/valuation | Reproduces calculations after rule changes. | Rule/model version strings exist in some paths; no generic config hash. | Phase 5 |
| `input_vintage_hash` | Derived metrics/ranks/valuation | Proves which input rows were used. | Not present. | Phase 5 |
| `universe_version` | Cross-sectional ranks | Freezes historical peer universe. | Not present; current ranking loads rows and osakedata metadata at run time. | Phase 7 |
| `peer_count` | Cross-sectional ranks | Documents rank denominator and missing peer scope. | `universe_size`, `sector_size`, and `industry_size` exist; no explicit peer membership hash. | Phase 7 |
| `event_status` | Fundamental events/expectations | Distinguishes scheduled, confirmed, reported, cancelled, estimated. | No clear canonical event table in audited paths. | Phase 8 |
| `price_match_status` | Market valuation metrics | Explains stale/missing price matches and fallback behavior. | Valuation has `valuation_status` and staleness, but no explicit price-match status/source id. | Phase 6 |

## 6. First implementation roadmap

| Phase | Goal | Likely files/modules | DB/migration impact | Test strategy | Explicit non-goals | Acceptance criteria |
| --- | --- | --- | --- | --- | --- | --- |
| Phase 1 | Classification / ESS-readiness documentation. Current task. | `docs/swingmaster_fundamentals_ess_readiness_phase1.md`, current-state doc reference. | None. | `git diff --check`; no Python tests if only docs change. | No runtime, tests, providers, DB, migrations, UI, scheduler, or ESS implementation. | New doc exists, current-state doc links it, commit is made. |
| Phase 2 | Ticker identity contract hardening/audit. Verify and document `market + ticker` consistency between fundamentals and osakedata valuation paths. See [SwingMaster Ticker Identity Contract Phase 2](swingmaster_ticker_identity_contract_phase2.md). | `run_fundamental_valuation.py`, `score_percentile.py`, `run_fundamental_quarter_state.py`, UI command builder, tests/docs. | Prefer none; only schema proposal if audit proves unavoidable. | Read-only or temp-DB tests for normalization/join assumptions. | No company/security-master, provider calls, schema changes by default. | Explicit identity contract doc/test coverage for ticker normalization and market joins. |
| Phase 3 | Read-only ESS readiness preflight using existing tables only. Implemented as [Phase 3 Preflight](swingmaster_fundamentals_ess_readiness_phase3_preflight.md). | `swingmaster/cli/preflight_fundamental_ess_readiness.py`, docs, tests. | None; no DB writes. | Temp fixture DB and read-only behavior tests. | No provider calls, refresh jobs, migrations, ESS integration, PIT/vintage hardening, or event-data implementation. | Preflight reports missing PIT fields and table readiness without modifying data; event data is reported as a future non-blocking gap. |
| Phase 4 | Reported fundamentals point-in-time/vintage hardening. Phase 4A design is documented in [Reported Fundamentals PIT/Vintage Design Phase 4A](swingmaster_reported_fundamentals_pit_vintage_design_phase4a.md); Phase 4B adds the additive vintage/provenance schema foundation; Phase 4C1 adds a helper writer; Phase 4C2 adds a read-only helper. | Migrations and provider persistence paths after separate design. | `rc_fundamental_quarterly_vintage` and field provenance tables exist for later phases; helper can insert/query rows in tests; production write paths and readers are unchanged. | Migration tests, helper temp-DB tests, SEC/Yahoo fixture tests, backfill safety tests. | No scoring/valuation changes except reading stable inputs. | Reported rows can represent multiple vintages without lookahead after later write/backfill phases. |
| Phase 5 | Derived metrics lineage. | `build_ttm.py`, `lifecycle.py`, `score.py`, related CLIs/tests. | Add lineage/config/input-vintage metadata after design. | Deterministic fixture tests for config and input hashes. | No provider behavior or valuation changes. | TTM/growth/lifecycle/score rows identify inputs and rules. |
| Phase 6 | Market valuation hardening. | `run_fundamental_valuation.py`, valuation migrations/tests. | Add market-date/source/staleness/price-match metadata after design. | Fixture osakedata + fundamentals DB tests. | Do not make SwingMaster a price canonical source. | Valuation rows identify price date/source and fundamental input. |
| Phase 7 | Cross-sectional ranks hardening. | `score_percentile.py`, percentile CLI/tests. | Add universe version/hash, peer scope, peer count/membership metadata after design. | Historical universe fixture tests. | Do not use today's universe for historical ranks. | Rank rows are reproducible for a fixed target date and universe. |
| Phase 8 | Fundamental events and expectations foundation. | New event-calendar module/table after design. | New tables likely required after design. | Fixture tests for event known-at and status transitions. | Do not start with analyst revisions/consensus/surprises. | Earnings-calendar-only data can be read point-in-time. |
| Phase 9 | Snapshot separation. Separate Fundamental Snapshot from Combined Research Snapshot. | `run_fundamental_ticker_snapshot.py`, UI snapshot command builder/docs. | Prefer none. | Snapshot output tests for fundamental-only vs combined sections. | Do not re-own technical data in SwingMaster. | Technical sections are clearly marked external/presentation-only. |
| Phase 10 | SwingMasterAdapter contract. Define point-in-time read output for ESS. | New adapter contract module/docs/tests. | Prefer none initially. | Contract tests against fixture DB. | Do not implement ESS decisions. | Adapter output schema is stable, PIT-explicit, and read-only. |
| Phase 11 | ESS-ready shadow export. Machine-readable export for selected tickers/date. | Adapter/export CLI after contract exists. | None unless separate export tracking is approved. | Golden-file export tests. | Not used by ESS decisions yet. | Shadow export can be generated for selected tickers/date without writes to fundamentals DB. |

## 7. Immediate next Codex task recommendation

Recommended next task: later Phase 4C, mocked dual-write or sidecar-write design for reported fundamentals PIT/vintage rows.

Phase 4A defines the target reported-fundamentals availability and vintage model, Phase 4B adds the additive schema foundation with temp-DB-only tests, Phase 4C1 adds an isolated helper writer, and Phase 4C2 adds an isolated read-only helper. The next safe step is to design and test additive production write behavior without switching current readers or changing provider behavior.

## 8. Existing docs update

`docs/swingmaster_usa_fundamentals_current_state.md` now points to this Phase 1 ESS-readiness plan as the follow-up architecture mapping document.

## 9. Verification

Only documentation files were changed in this phase. Python tests were intentionally not run.

Required check:

```bash
git diff --check
```

Expected result: no whitespace errors.
