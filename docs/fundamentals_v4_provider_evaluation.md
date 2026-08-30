# Fundamentals V4 Provider Evaluation

## Sharadar AAPL Shares Validation

Tested fields: Sharadar `sharesbas`, `shareswa`, and `shareswadil` against V3 canonical `shares_outstanding` for AAPL ARQ rows.

Primary mapping under evaluation: `V4 shares_outstanding = Sharadar ARQ sharesbas`.

Latest-8 result: exact `0`, near `7`, material `0`, V3 missing `1`, Sharadar missing `0`.

Mean absolute percentage difference: V3 vs `sharesbas` `0.0038293173652539825`, V3 vs `shareswa` `0.0029396211080059387`, V3 vs `shareswadil` `0.0036565775411157757`.

ARQ/MRQ: matching periods `20`, same `0`, different `20`.

Finding: `sharesbas` aligns materially better with V3 point-in-time shares than weighted-average `shareswa` or diluted weighted-average `shareswadil`. Local SEC exact-period-end CommonStockSharesOutstanding evidence was not available for a direct three-way AAPL row check in this diagnostic. Split-adjustment semantics remain `SPLIT_ADJUSTMENT_SEMANTICS_NOT_PROVEN` from this sample alone.

Decision: `SHARESBAS_ACCEPT_WITH_VALIDATION_GUARD`.

## Sharadar Free API Integration Smoke Test

Direct API tested: `https://api.sharadar.com/v1.0`.

Authentication: `SHARADAR_API_KEY` loaded from the environment and sent as `x-api-key`.
No Nasdaq Data Link, `quandl`, or `nasdaqdatalink` client was used.

Artifact root: `temp/fundamentals_v4_sharadar_free_api_smoke/20260830T134917Z`.

Network calls: `12`.

Schema: `/schema/fundamentals` returned PostgreSQL DDL text. The client parses that DDL and confirmed the expected fundamentals fields: `ticker`, `dimension`, `calendardate`, `reportperiod`, `fiscalperiod`, `date`, `revenue`, `gp`, `opinc`, `ebit`, `ebitda`, `netinc`, `ncfo`, `capex`, `fcf`, `cashneq`, `debt`, `debtc`, `debtnc`, `sharesbas`, `shareswa`, and `shareswadil`.

Metadata: `/schema/tickers` exposes `permaticker`, so V4 can retain Sharadar-stable identity metadata separately from market ticker symbols. `/schema/actions` exposes action-oriented fields including `ticker`, `date`, `action`, `name`, and `value`, which is relevant for later split and ticker-change validation.

AAPL API smoke: `/data/fundamentals?ticker=AAPL&limit=100` returned `90` sample records. Observed dimensions matched the uploaded sample profile: `ARQ=20`, `MRQ=20`, `ART=20`, `MRT=20`, `ARY=5`, `MRY=5`.

Endpoint parity: the modern `/data/fundamentals` endpoint and legacy `/data/SF1` alias both returned `SUCCESS` for AAPL.

ARQ/MRQ filtering: `AAPL + ARQ` returned `20` records and `AAPL + MRQ` returned `20` records. The smoke records the filtered provider-native rows separately and does not canonicalize them into V4 quarters.

Field projection: a minimal `fields=` request succeeded and returned the requested core fields for the first 10 AAPL records.

Sample parity: AAPL ARQ API rows matched the uploaded `temp/fundamentals.csv` sample on `20` comparable rows. `revenue`, `ebit`, `ebitda`, `fcf`, `cashneq`, `debt`, `sharesbas`, `shareswa`, and `shareswadil` all had `20` exact matches, `0` differences, `0` API missing values, and `0` sample missing values.

Fiscal identity: API ARQ data confirmed `reportperiod=2025-12-27` maps to `fiscalperiod=2026-Q1`, avoiding calendar-year-derived fiscal identity. Explicit Q4 ARQ rows were present.

Free-tier boundary: `WDAY`, `ASTH`, and `CECO` each returned HTTP `403` with status `FREE_TIER_LIMIT`. This validates entitlement handling and is not an integration failure. Retries on deterministic 403 were `0`.

Security: the API key was not printed, committed, or persisted in artifacts. Artifact URLs use header authentication and contain no key.

Architecture: reusable client is `swingmaster/providers/sharadar.py`; smoke CLI is `swingmaster/cli/run_sharadar_v4_smoke.py`; implementation note is `docs/fundamentals_v4_sharadar_integration.md`. The raw provider boundary remains separate from any future V4 canonical model. No V4 database was created and V3 was not modified.

Decision: `SHARADAR_FREE_API_SMOKE_COMPLETE_WITH_FREE_TIER_LIMITS`.

Next action: `ACTIVATE ONE MONTH OF SHARADAR FUNDAMENTALS FULL HISTORY AND RUN THE DIFFICULT MULTI-TICKER V4 ACCEPTANCE SET USING THE SAME CLIENT WITHOUT CHANGING THE INTEGRATION CONTRACT`.
