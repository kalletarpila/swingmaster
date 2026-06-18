# SwingMaster USA Fundamentals Current State

## Scope and method

This document covers only the visible `swingmaster` repository.

It does not inspect the neighboring RawCandle repository or external production systems. When code references external DBs such as `/home/kalle/projects/rawcandle/data/osakedata.db` or `/home/kalle/projects/rawcandle/data/analysis.db`, this document records that as repo evidence only.

This is a current-state investigation, not an implementation plan. Statements about providers use wording such as "code indicates" and "repo evidence suggests" unless the behavior is also locked in by tests.

Follow-up architecture mapping: [SwingMaster Fundamentals ESS Readiness Phase 1](swingmaster_fundamentals_ess_readiness_phase1.md) classifies the current fundamentals tables, code paths, and snapshot sections against the future ESS-ready target classes.

## High-level current picture

Repo evidence suggests the current USA fundamentals path is centered on:

1. SEC EDGAR raw fact fetch into `rc_fundamental_statement_raw`
2. SEC fact reconstruction into normalized quarterly rows in `rc_fundamental_quarterly`
3. Yahoo-based fallback enrichment for missing quarterly fields and, in some cases, missing quarter insertion
4. TTM derivation into `rc_fundamental_ttm`
5. Lifecycle classification and rule-based scoring on `rc_fundamental_ttm`
6. Separate percentile scoring step using `osakedata.db` metadata
7. Separate valuation step using `osakedata.db` prices
8. Downstream ticker snapshot and UI workflows that read the stored outputs

The strongest repo-level operational description is in [FUNDAMENTAL_PIPELINE_MEMORYLIST.md](../FUNDAMENTAL_PIPELINE_MEMORYLIST.md), backed by CLI code under `swingmaster/cli/`.

## 1. Scope and entry points

### Main USA-relevant code paths

| Path | Purpose | Repo-evidence status | Main functions/classes | External/API reads | DB writes | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `swingmaster/cli/run_fundamental_pipeline.py` | Single-ticker end-to-end fundamentals pipeline | `manual/CLI by repo evidence` | `run_fundamental_pipeline`, `derive_child_run_ids` | SEC or yfinance path | yes | Supports `--source sec_edgar` and `--source yfinance` |
| `swingmaster/cli/run_fundamental_quarter_update.py` | Quarter-state driven ticker refresh through score and USA valuation | `active by repo evidence` | `run_fundamental_quarter_update`, `process_ticker`, `run_quarterly_refresh` | SEC, Yahoo, osakedata | yes | Referenced by UI and memory list as current operational USA path |
| `swingmaster/cli/run_usa_enrichment_batch.py` | Deterministic USA Yahoo raw load + quarterly normalization + fallback enrich orchestration | `manual/CLI by repo evidence` | `run_usa_enrichment_batch` | Yahoo path via subprocess CLIs | yes | Uses subprocess calls to other CLIs |
| `swingmaster/cli/run_fundamental_bootstrap_sec_raw.py` | SEC raw bootstrap | `manual/CLI by repo evidence` | `run_sec_raw_bootstrap`, `insert_sec_raw_rows` | SEC EDGAR | yes | Raw facts only |
| `swingmaster/fundamentals/sec_edgar.py` | SEC endpoint access and CompanyFacts extraction | `active by repo evidence` | `resolve_cik`, `fetch_companyfacts`, `extract_companyfacts_raw_rows` | SEC EDGAR | no direct writes | Network path defined here |
| `swingmaster/cli/run_fundamental_sec_reconstruct_quarterly.py` | SEC raw fact reconstruction CLI | `manual/CLI by repo evidence` | `run_sec_reconstruct_quarterly` | reads stored SEC raw facts | yes | Writes reconstructed raw rows back to raw table |
| `swingmaster/fundamentals/sec_reconstruct_quarterly.py` | SEC fact parsing and quarter reconstruction logic | `active by repo evidence` | `load_sec_fact_rows`, `reconstruct_quarterly_rows`, `parse_sec_field_name` | no network | yes via caller | Core SEC normalization logic |
| `swingmaster/fundamentals/build_quarterly.py` | Generic raw-to-quarterly normalization | `active by repo evidence` | `build_quarterly_rows`, `insert_quarterly_rows` | reads raw table | yes | Maps raw names to normalized columns |
| `swingmaster/fundamentals/fetch_raw_statements.py` | yfinance raw fetch and raw-row persistence | `active by repo evidence` | `fetch_quarterly_statements_raw`, `insert_raw_statement_rows` | yfinance | yes | Used by `run_fundamental_pipeline.py` only |
| `swingmaster/cli/run_fundamental_yahoo_audit.py` | Yahoo raw payload audit/persistence | `active by repo evidence` | `run_yahoo_audit`, `insert_audit_rows` | Yahoo/yfinance wrapper | yes | Current USA batch path uses this |
| `swingmaster/fundamentals/providers/yahoo.py` | Yahoo client wrapper | `active by repo evidence` | `YahooFinanceClient` | yfinance | no | Serializes `info`, `fast_info`, quarterly statements |
| `swingmaster/cli/run_fundamental_yahoo_quarterly_write.py` | Normalize Yahoo raw payloads into `rc_fundamental_yahoo_quarterly` | `active by repo evidence` | `run_yahoo_quarterly_write`, `build_persist_rows` | reads Yahoo raw cache | yes | Intermediate Yahoo quarterly store |
| `swingmaster/cli/run_fundamental_yahoo_to_quarterly.py` | Bridge Yahoo quarterly rows into generic quarterly table | `manual/CLI by repo evidence` | `run_yahoo_to_quarterly`, `map_to_generic_quarterly_rows` | reads Yahoo quarterly | yes | Main non-USA bridge path |
| `swingmaster/cli/run_fundamental_yahoo_fallback_enrich.py` | Fill missing generic quarterly fields from Yahoo quarterly fallback | `active by repo evidence` | `run_yahoo_fallback_enrich`, `resolve_yahoo_match`, `insert_missing_quarterly_row_from_yahoo` | reads Yahoo quarterly | yes | Important USA fallback path |
| `swingmaster/fundamentals/build_ttm.py` | Quarterly-to-TTM derivation | `active by repo evidence` | `build_ttm_rows`, `build_and_insert_ttm_rows` | reads quarterly | yes | Requires at least 4 quarterly rows |
| `swingmaster/fundamentals/lifecycle.py` | Lifecycle classification | `active by repo evidence` | `classify_lifecycle`, `run_lifecycle_classification` | reads TTM | yes | Updates `lifecycle_class` |
| `swingmaster/fundamentals/score.py` | Rule-based scoring | `active by repo evidence` | `calculate_fundamental_score`, `run_fundamental_scoring` | reads TTM | yes | Writes both baseline and lifecycle-weighted score components |
| `swingmaster/fundamentals/score_percentile.py` | Cross-sectional percentile system | `active by repo evidence` | `load_latest_percentile_snapshot`, `build_percentile_rows`, `run_fundamental_score_percentile` | reads `osakedata` metadata | yes | Separate explicit step |
| `swingmaster/cli/run_fundamental_valuation.py` | Deterministic valuation from TTM + quarterly + osakedata prices | `active by repo evidence` | `run_fundamental_valuation`, `build_valuation_row` | reads `osakedata` close prices | yes | Called automatically in USA quarter update |
| `swingmaster/cli/run_fundamental_ticker_snapshot.py` | Downstream read/export of stored fundamentals | `active by repo evidence` | `build_snapshot_matrix`, `load_latest_valuation_snapshot` | reads fundamentals DB and optional external DBs | no fundamentals writes | Consumes stored fundamentals |
| `ui_fundamental_pipeline/` | UI wrapper for quarter update, percentile, snapshot flows | `active by repo evidence` | `SwingMasterApp`, `build_usa_update_command` | no provider calls directly | launches CLIs | UI integration exists |

### Other relevant but secondary paths

| Path | Purpose | Repo-evidence status | Comment |
| --- | --- | --- | --- |
| `swingmaster/cli/run_fundamental_finnhub_audit.py` | Finnhub raw audit | `manual/CLI by repo evidence` | Present, but defaults target OMXH and not referenced in current USA flow |
| `swingmaster/fundamentals/providers/finnhub.py` | Finnhub client | `manual/CLI by repo evidence` | Alternate provider infrastructure exists |
| `swingmaster/cli/run_fundamental_reporting_frequency_audit.py` | Reporting-frequency classification | `manual/CLI by repo evidence` | Used in FIN chain; not current USA core path |
| `swingmaster/cli/run_fundamental_missing_period_recovery_check.py` | Missing-period audit/recovery check | `manual/CLI by repo evidence` | Used in FIN chain; not current USA core path |
| `swingmaster/cli/run_fundamental_yahoo_batch_fin.py` | FIN Yahoo batch pipeline | `active by repo evidence` | Important contrast path for OMXH |
| `FUNDAMENTAL_PIPELINE_MEMORYLIST.md` | Operational notes | `active by repo evidence` | Most direct narrative of intended current flows |
| `FUNDAMENTAL_TICKER_SNAPSHOT_EXPLANATION.md` | Snapshot output explanation | `active by repo evidence` | Confirms downstream use and field meanings |

### Production-vs-manual classification notes

- `run_fundamental_quarter_update.py` has the strongest `active by repo evidence` classification because:
  - it is described as the practical USA update path in `FUNDAMENTAL_PIPELINE_MEMORYLIST.md`
  - it is wired into the UI through `ui_fundamental_pipeline/command_builder.py`
  - it contains explicit USA-specific branching plus valuation integration
- `run_fundamental_pipeline.py` appears to be a valid end-to-end ticker CLI, but repo evidence presents it more as a manual/research pipeline than the main USA operational refresh path.
- `run_usa_enrichment_batch.py` is operationally relevant, but it is still a manual CLI wrapper around other CLIs.

## 2. Provider flow

### SEC EDGAR

Code indicates SEC EDGAR is used for USA fundamentals.

Evidence:

- `swingmaster/fundamentals/sec_edgar.py`
- `swingmaster/cli/run_fundamental_bootstrap_sec_raw.py`
- `swingmaster/cli/run_fundamental_quarter_update.py`
- `swingmaster/cli/run_fundamental_pipeline.py`

Observed flow:

1. `resolve_cik()` fetches `https://www.sec.gov/files/company_tickers.json`
2. `fetch_companyfacts()` fetches `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`
3. `extract_companyfacts_raw_rows()` filters `10-Q` and `10-K` facts with an `end` date
4. Extracted rows are inserted into `rc_fundamental_statement_raw` with `source='sec_edgar'` and `period_type='sec_fact'`

Ticker-to-CIK mapping:

- handled dynamically by `load_ticker_cik_map()` and `resolve_cik()`
- no local static ticker/CIK cache is visible in the repo

Visible SEC concept set:

- `SEC_TAGS` in `swingmaster/fundamentals/sec_edgar.py`
- examples: `Revenues`, `RevenueFromContractWithCustomerExcludingAssessedTax`, `OperatingIncomeLoss`, `NetIncomeLoss`, `CashAndCashEquivalentsAtCarryingValue`, `LongTermDebtCurrent`, `LongTermDebtNoncurrent`, `EntityCommonStockSharesOutstanding`, weighted-average share tags

Rate limiting / retry / caching:

- no explicit retry logic is visible in `sec_edgar.py`
- no explicit sleep/throttle logic is visible
- no local response-cache layer is visible
- failures are wrapped as `RuntimeError("SEC_FETCH_FAILED:...")`

### Yahoo / yfinance

Code indicates Yahoo-derived data is used in two different ways:

1. as a direct raw source for `run_fundamental_pipeline.py --source yfinance`
2. as a fallback/enrichment path in the current USA operational refresh model

Evidence:

- `swingmaster/fundamentals/fetch_raw_statements.py`
- `swingmaster/fundamentals/providers/yahoo.py`
- `swingmaster/cli/run_fundamental_yahoo_audit.py`
- `swingmaster/cli/run_fundamental_yahoo_quarterly_write.py`
- `swingmaster/cli/run_fundamental_yahoo_to_quarterly.py`
- `swingmaster/cli/run_fundamental_yahoo_fallback_enrich.py`
- `swingmaster/cli/run_usa_enrichment_batch.py`

Observed Yahoo flow:

1. `YahooFinanceClient.get_raw_payload()` collects `info`, `fast_info`, and three quarterly statement payloads
2. `run_fundamental_yahoo_audit.py` stores that payload in `rc_fundamental_yahoo_raw`
3. `run_fundamental_yahoo_quarterly_write.py` normalizes the raw payload into `rc_fundamental_yahoo_quarterly`
4. `run_fundamental_yahoo_fallback_enrich.py` uses those normalized Yahoo quarterly rows to fill only missing generic quarterly values or to insert a missing quarter row if SEC data still does not satisfy the detected quarter

Provider priority:

- current USA quarter-update flow suggests SEC is primary and Yahoo is fallback
- evidence:
  - `run_fundamental_quarter_state.py` infers `primary_source='sec_edgar'` for USA tickers
  - `run_fundamental_quarter_update.py` first checks whether SEC-style quarterly data satisfies the detected period
  - `run_fundamental_yahoo_fallback_enrich.py` writes audit rows with `primary_source='sec_edgar'` and `fallback_source='yahoo'`
  - tests explicitly verify that existing SEC values are not overwritten

What triggers fallback from SEC to Yahoo:

- in USA quarter update, after SEC refresh logic runs, `run_yahoo_fallback_enrich()` is always called for the ticker
- fallback matters when:
  - generic quarterly fields remain `NULL`
  - the detected quarter is still not satisfied after SEC refresh
  - a Yahoo row can be matched by exact date or same-quarter-within-7-days tolerance

Retry / skip / cache / recording behavior:

- Yahoo raw audit records `OK`, `EMPTY`, and `ERROR` statuses in `rc_fundamental_yahoo_raw`
- errors are recorded per symbol, not retried in visible code
- no explicit throttle or retry loop is visible
- raw payloads are cached in DB tables, not just in memory

### Other providers

Repo evidence shows a Finnhub path exists:

- `swingmaster/cli/run_fundamental_finnhub_audit.py`
- `swingmaster/fundamentals/providers/finnhub.py`
- `rc_fundamental_finnhub_raw`

Current USA evidence:

- no visible USA operational path currently uses Finnhub
- the present code appears to keep Finnhub as an alternative or exploratory raw audit path

## 3. Normalization and field mapping

### SEC raw encoding

`extract_companyfacts_raw_rows()` stores SEC facts as raw rows with:

- `statement_type`: derived from `SEC_STATEMENT_TYPE_BY_TAG`
- `period_end_date`
- `period_type='sec_fact'`
- `field_name`: an encoded string containing tag, form, unit, fiscal year, fiscal period, frame, start, filed
- `field_value`: normalized to `float` or `None`
- `currency`: SEC unit string such as `USD` or `shares`

This preserves duplicates at raw level because metadata is packed into `field_name`.

### SEC reconstruction

`swingmaster/fundamentals/sec_reconstruct_quarterly.py` is the most complex normalization layer.

Confirmed behavior from code and tests:

- parses encoded SEC metadata via `parse_sec_field_name()`
- supports `10-Q` and `10-K` facts
- accepts `fp` in `Q1`, `Q2`, `Q3`, `FY`
- distinguishes:
  - flow facts
  - snapshot facts
  - share facts
- uses tag-priority maps:
  - `FLOW_TAG_TO_FIELD`
  - `SNAPSHOT_TAG_TO_FIELD`
  - `FIELD_TAG_PRIORITY`
  - `DEBT_GROUPS`
- classifies duration types and prefers true quarterly duration over YTD where possible
- reconstructs quarterly flow fields from SEC facts
- does not simply treat annual `FY` values as Q4 in all cases
- reconstructs total debt from grouped current/non-current debt components
- uses special share-fact priority logic for weighted-average share concepts

Known mapping examples:

- revenue:
  - `Revenues`
  - `RevenueFromContractWithCustomerExcludingAssessedTax`
- operating cash flow:
  - `NetCashProvidedByUsedInOperatingActivities`
  - `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations`
- capex:
  - `PaymentsToAcquirePropertyPlantAndEquipment`
  - `PaymentsToAcquireProductiveAssets`
- cash:
  - `CashAndCashEquivalentsAtCarryingValue`
  - `CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents`
- shares:
  - `EntityCommonStockSharesOutstanding`
  - weighted-average share fallbacks

### Generic quarterly normalization

`swingmaster/fundamentals/build_quarterly.py` maps raw rows into normalized columns in `rc_fundamental_quarterly`.

Normalized quarterly fields:

- `revenue`
- `gross_profit`
- `operating_income`
- `ebit`
- `ebitda`
- `net_income`
- `operating_cashflow`
- `capex`
- `free_cashflow`
- `cash`
- `total_debt`
- `shares_outstanding`
- `currency`

Key mapping source:

- `FIELD_MAPPINGS`

Important behaviors:

- matches candidate field names in deterministic order
- inserts the union of all detected periods
- computes `free_cashflow = operating_cashflow + capex`
- if `total_debt` is absent, tries `LongTermDebtCurrent + LongTermDebtNoncurrent + ShortTermBorrowings`
- does not visibly perform currency conversion
- carries `currency` column but generic builder does not populate it from raw rows

### Yahoo normalization

Yahoo flows use two stages:

1. raw payload serialization into `rc_fundamental_yahoo_raw`
2. normalized quarterly persistence into `rc_fundamental_yahoo_quarterly`

`run_fundamental_yahoo_quarterly_write.py` persists:

- revenue, gross profit, operating income, net income
- operating cashflow, capex, free_cashflow
- cash, total_debt, shares_outstanding
- `shares_source`
- `shares_quality`
- provenance columns `source_run_id`, `run_id`, `created_at_utc`

`run_fundamental_yahoo_to_quarterly.py` then maps Yahoo quarterly rows into generic quarterly rows, setting:

- `ebit = operating_income`
- `ebitda = None`
- `currency = None`

### Quarterly vs annual handling

SEC path:

- explicit quarterly reconstruction logic exists
- annual facts may be used in reconstruction logic where appropriate, but not blindly copied as quarter rows

Yahoo path:

- visible code path is quarterly-oriented
- no separate annual-table model is visible in storage

### Period-end and fiscal handling

SEC path:

- period end date comes from SEC fact `end`
- fiscal year and fiscal period are preserved in encoded raw field names
- reconstruction logic uses `fy`, `fp`, `frame`, `start`, `filed`

Yahoo path:

- period columns are normalized to `YYYY-MM-DD`
- no separate stored fiscal year / fiscal quarter columns are visible

### Missing values and precedence

Confirmed by code/tests:

- SEC remains primary for USA generic quarterly values
- Yahoo fallback fills only missing fields
- existing SEC values are not overwritten
- if SEC still does not satisfy the detected quarter, Yahoo may insert a missing quarter row if a same-quarter match exists within 7 days

### Known weaknesses / unclear points

- no visible currency normalization across providers
- generic quarterly table has no explicit provider column, so provenance is indirect
- generic quarterly rows do not store fiscal quarter/year explicitly
- SEC mapping complexity is high and spread across priority and duration heuristics
- duplicate/restatement handling is heuristic, not fully modeled as separate versions

## 4. Database storage

### DB location by repo evidence

Documented/configured paths:

- `ui_fundamental_pipeline/config.py`
  - `fundamentals_usa.db`
  - `fundamentals_fin.db`
  - `/home/kalle/projects/rawcandle/data/osakedata.db`
  - `/home/kalle/projects/rawcandle/data/analysis.db`
- `FUNDAMENTAL_PIPELINE_MEMORYLIST.md` repeats the same fundamentals DB paths

Repo evidence suggests fundamentals are stored in a dedicated fundamentals DB, not inside `osakedata.db` or `analysis.db`.

### Read-only local schema inspection

Read-only inspection was run against local `fundamentals_usa.db`. Present local tables:

- `rc_fundamental_statement_raw`
- `rc_fundamental_quarterly`
- `rc_fundamental_ttm`
- `rc_fundamental_score_percentile`
- `rc_fundamental_valuation`
- `rc_fundamental_yahoo_raw`
- `rc_fundamental_yahoo_quarterly`
- `rc_fundamental_finnhub_raw`
- `rc_fundamental_quarterly_enrichment_audit`
- `rc_fundamental_quarter_state`
- `rc_fundamental_run`
- `rc_fundamental_schema_version`
- view `rc_fundamental_latest`

Notably, migrations also define:

- `rc_fundamental_reporting_frequency_classification`
- `rc_fundamental_missing_period_recovery_check`

but those were not present in the inspected local `fundamentals_usa.db`.

### Core tables

#### `rc_fundamental_statement_raw`

- PK: `(ticker, statement_type, period_end_date, field_name)`
- indexes:
  - `idx_fundamental_raw_ticker`
  - `idx_fundamental_raw_period`
- stores:
  - raw field name
  - raw numeric value
  - `currency`
  - `source`
  - `retrieved_at_utc`
  - `run_id`

History/preservation:

- `INSERT OR REPLACE` behavior means rows can be overwritten at the PK grain
- because SEC encodes metadata in `field_name`, multiple duplicate-ish fact variants can still coexist if encoded names differ

#### `rc_fundamental_quarterly`

- PK: `(ticker, period_end_date)`
- indexes:
  - `idx_fundamental_quarterly_ticker`
  - `idx_fundamental_quarterly_period`
- stores normalized quarter values
- no provider/source column
- no explicit fiscal-year/fiscal-quarter columns

History/preservation:

- overwritten at `(ticker, period_end_date)` grain

#### `rc_fundamental_ttm`

- PK: `(ticker, as_of_date)`
- indexes:
  - `idx_fundamental_ttm_ticker`
  - `idx_fundamental_ttm_as_of_date`
- stores:
  - TTM financial metrics
  - lifecycle class
  - baseline score
  - score component columns
  - lifecycle-weighted score columns
  - `run_id`

#### `rc_fundamental_score_percentile`

- PK: `(ticker, target_date, rule_id)`
- indexes:
  - `idx_rc_fund_score_pct_target`
  - `idx_rc_fund_score_pct_rule_run`
  - `idx_rc_fund_score_pct_sector`
  - `idx_rc_fund_score_pct_industry`
- stores:
  - target-date-specific percentile outputs
  - sector/industry metadata
  - global/sector/industry percentiles
  - blended and lifecycle-weighted percentiles
  - partition ranks

#### `rc_fundamental_valuation`

- PK: `(ticker, as_of_date)`
- indexes:
  - by ticker
  - by as-of date
  - by valuation bucket
  - by run id
- stores:
  - valuation outputs and status
  - market cap, EV, close price
  - shares, cash, debt used
  - valuation model version
  - fundamental staleness metadata
  - zero-assumption flags for debt/cash

### Audit and intermediate tables

#### `rc_fundamental_yahoo_raw`

- autoincrement `id`
- stores serialized raw Yahoo payload JSON fields plus status and error message
- acts as provider raw cache/audit log

#### `rc_fundamental_yahoo_quarterly`

- PK: `(market, symbol, period_end_date)`
- stores normalized Yahoo quarterly values and Yahoo-specific shares metadata

#### `rc_fundamental_finnhub_raw`

- autoincrement `id`
- stores raw Finnhub payload JSON and status

#### `rc_fundamental_quarterly_enrichment_audit`

- local schema inspection showed columns:
  - `ticker`
  - `period_end_date`
  - `field_name`
  - `old_value`
  - `new_value`
  - `primary_source`
  - `fallback_source`
  - `enrichment_status`
  - `run_id`
  - `created_at_utc`
  - `matched_yahoo_period_end_date`
  - `match_method`

This is the clearest provider-precedence audit trail in the current USA path.

#### `rc_fundamental_quarter_state`

- PK: `ticker`
- stores:
  - `market`
  - `primary_source`
  - `latest_db_period_end_date`
  - `detected_source_period_end_date`
  - `new_quarter_available`
  - timestamps and run ids for detection/ingest

### Foreign keys

- no explicit foreign keys were visible in inspected schema output

### Raw payload storage

- yes for Yahoo and Finnhub
- no full raw SEC JSON storage; SEC is flattened into raw fact rows

## 5. Refresh and maintenance model

### Current USA refresh model

Repo evidence suggests USA refresh is not a single scheduler-owned opaque process. It is composed of explicit CLIs and a UI wrapper.

Current practical USA path by repo evidence:

1. optional USA Yahoo enrichment batch:
   - `run_usa_enrichment_batch.py`
2. quarter-state driven refresh:
   - `run_fundamental_quarter_update.py --market usa`
3. percentile scoring:
   - `run_fundamental_score_percentile.py`
4. ticker snapshot:
   - `run_fundamental_ticker_snapshot.py`

### Manual vs scheduled vs UI

- CLI integration: yes, extensive
- UI integration: yes, in `ui_fundamental_pipeline/`
- scheduler integration: unclear inside this repo
- stock-update integration: not clearly evidenced inside this repo

There is no clear in-repo scheduler implementation for USA fundamentals refresh. The prompted quarter-state model explicitly assumes another process has already populated `rc_fundamental_quarter_state`.

### Missing/stale detection

Detected by current code:

- quarter freshness:
  - `rc_fundamental_quarter_state.new_quarter_available`
  - `detected_source_period_end_date`
- valuation freshness:
  - `valuation_fundamental_staleness_days`
  - invalid if staleness exceeds 240 days
- percentile target-date recency:
  - explicit `--as-of-date`

Not detected centrally:

- there is no single generic "fundamentals stale" service object visible
- quarterly generic rows themselves do not carry a freshness status column

### Failed ticker tracking

Partially present:

- Yahoo raw audit stores `OK` / `EMPTY` / `ERROR`
- Finnhub raw audit stores `OK` / `EMPTY` / `ERROR`
- quarter update prints failures and can continue in batch mode
- enrichment audit captures field-level fills

Missing or limited:

- no consolidated provider-failure table for SEC fetch failures
- no generic per-ticker freshness SLA table for USA fundamentals

### Delisted / invalid tickers

Limited evidence only:

- SEC path raises `SEC_TICKER_NOT_FOUND`
- valuation pulls universe from `osakedata`
- no explicit delisting lifecycle or tombstone model is visible

### USA vs OMXH differences

Confirmed code differences:

- `run_fundamental_quarter_state.py` infers `primary_source='sec_edgar'` for USA and `primary_source='yahoo'` for OMXH
- USA quarter update path:
  - SEC first
  - Yahoo fallback enrich
  - automatic valuation at end if `osakedata-db` is supplied
- OMXH path:
  - Yahoo raw audit -> Yahoo quarterly -> generic quarterly bridge
- percentile minimum universe and industry thresholds differ by market in `score_percentile.py`

## 6. SwingMaster usage

### Where fundamentals are used downstream

Confirmed downstream consumers:

- `run_fundamental_score_percentile.py`
- `run_fundamental_valuation.py`
- `run_fundamental_ticker_snapshot.py`
- `ui_fundamental_pipeline/`

### Effects on decision logic

Current code indicates fundamentals affect:

- lifecycle classification in `rc_fundamental_ttm`
- baseline and lifecycle-weighted rule-based score
- percentile ranking
- valuation categorization

This is more than display-only usage.

### Display/export usage

Confirmed outputs:

- ticker snapshot CSV/stdout exports via `run_fundamental_ticker_snapshot.py`
- snapshot includes:
  - quarterly raw values
  - scores
  - percentiles
  - valuation snapshot
  - optional technical append sections
- UI launches update, percentile, and snapshot commands

### Missing fundamentals handling

Current behavior is tolerant in many downstream places:

- percentile scoring uses available factors and renormalizes weights
- snapshot can print empty values when valuation/fundamental fields are missing
- valuation can mark rows invalid instead of crashing if key inputs are missing

But some upstream steps are strict:

- TTM build requires at least four quarterly rows
- SEC raw bootstrap fails if no SEC facts found
- USA quarter update can fail if neither SEC nor Yahoo satisfies detected quarter

## 7. Tests and current guarantees

### Main fundamentals test files

| Test path | What it verifies | Mocked or real-provider? | Current guarantee / gap |
| --- | --- | --- | --- |
| `swingmaster/tests/test_fundamental_sec_raw_bootstrap.py` | SEC fact extraction, filtering, idempotency, deterministic timestamping | mocked SEC fetch helpers | Strong unit coverage for raw SEC flattening; no real SEC network |
| `swingmaster/tests/test_fundamental_sec_reconstruct_quarterly.py` | SEC quarterly reconstruction heuristics, share-tag priority, debt sum, duplicate selection | fixture DB rows | Strong coverage for SEC concept/duration logic |
| `swingmaster/tests/test_fundamental_build_quarterly.py` | Generic raw-to-quarterly mapping and idempotency | fixture DB rows | Locks in normalized field mapping behavior |
| `swingmaster/tests/test_fundamental_build_ttm.py` | TTM math, null handling, debt/EBITDA fallback | fixture DB rows | Good coverage for derived TTM metrics |
| `swingmaster/tests/test_fundamental_yahoo_audit.py` | Yahoo payload serialization, status classification, persistence | mocked provider payloads | Confirms raw Yahoo cache model |
| `swingmaster/tests/test_fundamental_yahoo_quarterly_write.py` | Yahoo raw-to-quarterly normalization and replace behavior | fixture DB rows | Covers intermediate Yahoo quarterly table |
| `swingmaster/tests/test_fundamental_yahoo_to_quarterly.py` | Yahoo quarterly to generic quarterly mapping | fixture DB rows | Covers non-USA bridge semantics |
| `swingmaster/tests/test_fundamental_yahoo_fallback_enrich.py` | Missing-field fill only, exact/tolerant date matching, audit rows | fixture DB rows | Strong direct coverage of USA Yahoo fallback logic |
| `swingmaster/tests/test_fundamental_quarter_state.py` | Quarter-state sync, detection mark, ingest acknowledgement | fixture DB rows | Confirms state-table semantics |
| `swingmaster/tests/test_fundamental_quarter_update.py` | USA and non-USA refresh orchestration | heavily mocked internal calls | Intended guardrail for operational flow, but currently drifted from code |
| `swingmaster/tests/test_fundamental_score.py` | Score component math and lifecycle overlays | fixture DB rows | Strong ruleset coverage |
| `swingmaster/tests/test_fundamental_score_percentile.py` | Snapshot selection, percentile math, thresholds, ranks | fixture DB + small osakedata fixture | Strong percentile logic coverage |
| `swingmaster/tests/test_fundamental_valuation.py` | Valuation statuses, row writing, invalid/missing input handling | fixture fundamentals + osakedata DB | Good valuation logic coverage |
| `swingmaster/tests/test_fundamental_ticker_snapshot.py` | Snapshot rendering, valuation snapshot, optional sections, no-write behavior | fixture DBs | Strong downstream export coverage |
| `swingmaster/tests/test_usa_enrichment_batch.py` | USA batch command orchestration | mocked subprocess | Confirms current batch wrapper behavior |
| `ui_fundamental_pipeline/tests/test_command_builder.py` | UI command composition for USA/FIN flows | no provider calls | Confirms UI wiring to current CLI names |

### Test execution run during this investigation

Executed:

```bash
PYTHONPATH=. pytest -q \
  swingmaster/tests/test_fundamental_sec_raw_bootstrap.py \
  swingmaster/tests/test_fundamental_sec_reconstruct_quarterly.py \
  swingmaster/tests/test_fundamental_yahoo_fallback_enrich.py \
  swingmaster/tests/test_fundamental_quarter_update.py \
  swingmaster/tests/test_fundamental_pipeline.py \
  swingmaster/tests/test_fundamental_score_percentile.py \
  ui_fundamental_pipeline/tests/test_command_builder.py
```

Observed result:

- 119 passed
- 14 failed

Current failure themes:

1. `test_fundamental_quarter_update.py`
   - tests still call `run_fundamental_quarter_update()` without the now-required `osakedata_db_path` argument
   - expected child run ids do not include the newer `valuation` child run id
2. `test_fundamental_yahoo_fallback_enrich.py::test_cli_summary_output`
   - mocked summary fixture is out of sync with CLI output, which now expects `rows_inserted`

Interpretation:

- many fundamentals guarantees are strong at the unit-logic level
- orchestration tests have drifted behind current implementation in at least these areas

### Important test gaps

- no visible real-provider integration tests against SEC or Yahoo endpoints
- no explicit retry/throttle behavior tests because the code has little visible retry/throttle logic
- no end-to-end test for full USA refresh through valuation with a realistic local DB fixture
- no explicit schema-version parity test against the checked-in `fundamentals_usa.db`

## 8. Current risks and gaps

### Confirmed risks

1. Missing USA fundamentals risk
   - confirmed: USA path can fail when SEC does not satisfy detected quarter and Yahoo fallback cannot match/bridge it
2. Stale fundamentals risk
   - confirmed: quarter freshness depends on external maintenance of `rc_fundamental_quarter_state`
   - confirmed: valuation explicitly detects stale fundamentals, but generic quarterly/TTM storage has no unified freshness flag
3. SEC concept mapping complexity risk
   - confirmed: SEC reconstruction relies on many heuristics, priorities, and duration classifications
4. Yahoo fallback inconsistency risk
   - confirmed: fallback uses date-tolerance and partial fill logic; generic quarterly provenance is not retained per cell
5. Partial write / multi-step orchestration risk
   - confirmed: update flow is composed of many sequential writes across multiple tables, with no visible cross-step transaction boundary
6. Audit trail asymmetry risk
   - confirmed: Yahoo fallback has a field-level audit table, but SEC fetch/reconstruction path does not store full raw payload JSON or a comparable step-level audit trail
7. Silent or fragmented failure reporting risk
   - confirmed: provider raw audit tables capture some statuses, but SEC errors are mostly runtime exceptions rather than persisted per-ticker audit records
8. Schema drift risk
   - confirmed: migrations define tables not present in the inspected local `fundamentals_usa.db`
9. Test drift risk
   - confirmed: current orchestration tests are failing against current code

### Suspected or unclear risks

1. Rate limiting risk
   - suspected: SEC and Yahoo paths have no visible explicit throttling
2. Slow provider call risk
   - suspected: no local HTTP cache layer is visible for SEC, and Yahoo payload collection may be slow for larger batches
3. Wrong provider precedence risk in edge cases
   - suspected: quarterly-level precedence is clear, but downstream tables lose detailed source provenance once values are merged into `rc_fundamental_quarterly`
4. Future schema support risk
   - suspected: lack of explicit fiscal-quarter fields, provider columns in generic quarterly rows, and restatement/version modeling may become limiting

## 9. Recommended next phases

These are conservative phases only. They are not implemented here.

### Phase 1: read-only verification and inventory hardening

- goal: make the current state easier to verify repeatedly
- likely files:
  - `docs/swingmaster_usa_fundamentals_current_state.md`
  - possibly a dedicated read-only audit CLI
- expected tests:
  - schema inventory snapshots
  - CLI smoke tests for evidence reporting
- risks:
  - documentation going stale again
- explicitly do not change:
  - provider priority
  - scheduler behavior
  - DB content

### Phase 2: provider result audit and freshness model

- goal: make freshness and per-ticker provider outcomes explicit
- likely files:
  - quarter-state and audit-related CLIs/tables
  - valuation/freshness reporting
- expected tests:
  - stale detection
  - persisted SEC/Yahoo failure audit records
- risks:
  - audit-schema expansion without clear owner
- explicitly do not change:
  - scoring rules
  - downstream UI behavior

### Phase 3: schema hardening

- goal: close gaps between migrations, local DB expectations, and provenance needs
- likely files:
  - migrations
  - migration tests
  - read-model code
- expected tests:
  - schema parity tests
  - migration idempotency tests
- risks:
  - compatibility with existing DB files
- explicitly do not change:
  - provider selection semantics

### Phase 4: deterministic USA refresh CLI hardening

- goal: reduce orchestration drift and clarify required inputs like `osakedata_db_path`
- likely files:
  - `run_fundamental_quarter_update.py`
  - `run_usa_enrichment_batch.py`
  - related tests
- expected tests:
  - updated orchestration tests
  - argument/summary compatibility tests
- risks:
  - behavior changes leaking into production workflows
- explicitly do not change:
  - actual provider precedence unless separately approved

### Phase 5: scheduler or UI integration hardening

- goal: document and stabilize the operational trigger layer
- likely files:
  - `ui_fundamental_pipeline/`
  - any scheduler wrapper if one exists later
- expected tests:
  - UI command builder tests
  - execution-path smoke tests
- risks:
  - coupling UI assumptions to backend CLI changes
- explicitly do not change:
  - underlying scoring/valuation rules

### Phase 6: downstream usage clarification

- goal: define exactly which reports and decisions depend on which fundamentals fields
- likely files:
  - `run_fundamental_ticker_snapshot.py`
  - explanation docs
  - any reporting/export surfaces
- expected tests:
  - snapshot regression tests
  - missing-data tolerance tests
- risks:
  - surfacing inconsistencies between stored fundamentals and displayed interpretation
- explicitly do not change:
  - existing output semantics without explicit approval

## 10. Evidence searched

Commands used during this investigation:

```bash
git status --short
rg -n "fundamental|fundamentals|SEC|sec|EDGAR|edgar|companyfacts|company facts|CIK|cik|yahoo|yfinance|financials|quarterly|annual|revenue|eps|market cap|valuation|income statement|balance sheet|cash flow" swingmaster ui_fundamental_pipeline README.md FUNDAMENTAL_PIPELINE_MEMORYLIST.md FUNDAMENTAL_TICKER_SNAPSHOT_EXPLANATION.md
find . -iname '*fund*' -o -iname '*sec*' -o -iname '*yahoo*'
rg -n "fundamentals_usa\\.db|fundamentals_fin\\.db|osakedata\\.db|analysis\\.db|rc_fundamental" .
sqlite3 fundamentals_usa.db ".tables"
sqlite3 fundamentals_usa.db ".schema rc_fundamental_statement_raw"
sqlite3 fundamentals_usa.db ".schema rc_fundamental_quarterly"
sqlite3 fundamentals_usa.db ".schema rc_fundamental_ttm"
sqlite3 fundamentals_usa.db ".schema rc_fundamental_score_percentile"
sqlite3 fundamentals_usa.db ".schema rc_fundamental_valuation"
sqlite3 fundamentals_usa.db ".schema rc_fundamental_yahoo_raw"
sqlite3 fundamentals_usa.db ".schema rc_fundamental_yahoo_quarterly"
sqlite3 fundamentals_usa.db ".schema rc_fundamental_finnhub_raw"
sqlite3 fundamentals_usa.db ".schema rc_fundamental_quarter_state"
sqlite3 fundamentals_usa.db "PRAGMA table_info('rc_fundamental_quarterly_enrichment_audit');"
sqlite3 fundamentals_usa.db "SELECT name, type FROM sqlite_master WHERE name LIKE 'rc_fundamental_%' ORDER BY type, name;"
PYTHONPATH=. pytest -q swingmaster/tests/test_fundamental_sec_raw_bootstrap.py swingmaster/tests/test_fundamental_sec_reconstruct_quarterly.py swingmaster/tests/test_fundamental_yahoo_fallback_enrich.py swingmaster/tests/test_fundamental_quarter_update.py swingmaster/tests/test_fundamental_pipeline.py swingmaster/tests/test_fundamental_score_percentile.py ui_fundamental_pipeline/tests/test_command_builder.py
```

## 11. Bottom line

Repo evidence indicates that SwingMaster currently treats USA fundamentals as a hybrid SEC-first plus Yahoo-fallback system, persisted in a dedicated fundamentals DB and consumed downstream by TTM scoring, percentile ranking, valuation, snapshots, and a UI wrapper.

The strongest current operational path is `run_fundamental_quarter_update.py --market usa`, not the older single-ticker pipeline alone. The biggest practical current-state issues are orchestration/test drift, uneven auditability across providers, and dependence on external quarter-state maintenance plus external market-data DBs.

## 12. Stabilization status after test-parity update

The previously observed drift in `test_fundamental_quarter_update.py` has been aligned with current code behavior:

- tests now pass the current `osakedata_db_path` argument
- USA success/batch paths mock the valuation dependency instead of touching production `osakedata.db`
- expected child run ids include the current `valuation` child run id
- non-USA orchestration tests are market-scoped to avoid accidental USA valuation requirements

The Yahoo fallback CLI summary test has been aligned with the current summary shape:

- `rows_inserted` is included in the mocked summary and expected CLI output
- fallback semantics tests still verify that Yahoo fills missing values only and does not overwrite existing SEC values

Schema parity is now covered by a temporary-DB migration test:

- `test_run_migration_creates_reporting_frequency_and_recovery_tables`
- verifies that `REQUIRED_TABLES` and the migrated temp schema include `rc_fundamental_reporting_frequency_classification`
- verifies that `REQUIRED_TABLES` and the migrated temp schema include `rc_fundamental_missing_period_recovery_check`
- does not inspect or modify the real `fundamentals_usa.db`

Verification commands run for this stabilization:

```bash
PYTHONPATH=. pytest -q \
  swingmaster/tests/test_fundamental_quarter_update.py \
  swingmaster/tests/test_fundamental_yahoo_fallback_enrich.py \
  swingmaster/tests/test_fundamental_migrations.py
```

Result: `47 passed`.

```bash
PYTHONPATH=. pytest -q \
  swingmaster/tests/test_fundamental_sec_raw_bootstrap.py \
  swingmaster/tests/test_fundamental_sec_reconstruct_quarterly.py \
  swingmaster/tests/test_fundamental_yahoo_fallback_enrich.py \
  swingmaster/tests/test_fundamental_quarter_update.py \
  swingmaster/tests/test_fundamental_pipeline.py \
  swingmaster/tests/test_fundamental_score_percentile.py \
  ui_fundamental_pipeline/tests/test_command_builder.py \
  swingmaster/tests/test_fundamental_migrations.py
```

Result: `139 passed`.
