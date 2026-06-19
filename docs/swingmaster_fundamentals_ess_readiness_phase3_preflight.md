# SwingMaster Fundamentals ESS Readiness Phase 3 Preflight

## Purpose

Phase 3 adds a read-only preflight CLI for checking how ready the current SwingMaster fundamentals tables are for a future ESS point-in-time enrichment adapter.

The preflight uses existing tables only. It does not call providers, does not run refresh jobs, does not add migrations, does not implement ESS integration, and does not write to fundamentals or osakedata databases.

The checker answers ticker-level questions such as:

- whether reported quarterly fundamentals exist
- whether TTM/derived metrics exist as of the requested date
- whether valuation rows exist as of the requested date
- whether percentile/rank rows exist as of the requested date
- whether quarter-state identity context exists and matches the requested market
- which current schema concepts are still missing for stronger ESS point-in-time readiness

## Command Examples

Text output:

```bash
python3 -m swingmaster.cli.preflight_fundamental_ess_readiness \
  --fundamentals-db /path/to/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-03-31 \
  --tickers AAPL,MSFT,NVDA \
  --format text
```

JSON output:

```bash
python3 -m swingmaster.cli.preflight_fundamental_ess_readiness \
  --fundamentals-db /path/to/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-03-31 \
  --tickers AAPL,MSFT,NVDA \
  --format json
```

Fail CI-style if readiness is not fully OK:

```bash
python3 -m swingmaster.cli.preflight_fundamental_ess_readiness \
  --fundamentals-db /path/to/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-03-31 \
  --tickers AAPL,MSFT,NVDA \
  --fail-if-not-ok
```

Optional bounded auto-discovery:

```bash
python3 -m swingmaster.cli.preflight_fundamental_ess_readiness \
  --fundamentals-db /path/to/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-03-31 \
  --max-tickers 50
```

## Status Meanings

| Status | Meaning |
| --- | --- |
| `OK` | Required current-table evidence exists for the checked category. |
| `PARTIAL` | Some usable data exists, but a relevant supporting category is missing or inconsistent. |
| `MISSING` | The table exists, but data for the ticker/date is missing. |
| `STALE` | Reserved for later staleness rules; Phase 3 does not apply a staleness threshold. |
| `UNKNOWN` | The schema is too incomplete or identity information is too inconsistent to determine readiness. |
| `NOT_APPLICABLE` | The category is intentionally non-blocking in Phase 3, usually because the table is optional or future-phase work. |

Overall ticker readiness is conservative:

- `OK` requires reported, derived, valuation, and rank status to be `OK`.
- `PARTIAL` means reported fundamentals exist but at least one derived/valuation/rank category is missing.
- `MISSING` means reported fundamentals are missing.
- `UNKNOWN` means the required schema is too incomplete to determine readiness.

Event data is not blocking in Phase 3. If no event table exists, event readiness is reported as `NOT_APPLICABLE` and the schema gap list records the missing event table.

## Current Non-Blocking Gaps

The preflight reports schema support for the following future ESS concepts:

- `market` in core quarterly/TTM tables
- `available_at_utc`
- `filed_at_utc`
- `statement_vintage_id`
- `source_hash`
- `input_vintage_hash`
- `config_hash`
- `universe_version`
- `universe_hash`
- `price_match_status`
- event table

These gaps are expected at Phase 3. They do not mean the preflight failed; they identify what later phases must design explicitly.

Phase 4A design follow-up: [Reported Fundamentals PIT/Vintage Design Phase 4A](swingmaster_reported_fundamentals_pit_vintage_design_phase4a.md) defines the proposed plan for addressing reported-fundamentals availability, vintage, source-hash, and restatement gaps. Phase 4B adds additive vintage/provenance tables, Phase 4C1 adds an isolated helper writer, and Phase 4C2 adds an isolated read-only helper, but this Phase 3 preflight still uses the existing read model unless it is separately enhanced later.

## How To Interpret Results

Use the summary first:

- `overall_status=OK` means all checked tickers have current reported, derived, valuation, and rank data as of the requested date.
- `overall_status=PARTIAL` means the database has some usable fundamentals, but not enough for fully enriched ESS reads.
- `overall_status=MISSING` means at least one checked ticker has no reported quarterly fundamentals as of the requested date.
- `overall_status=UNKNOWN` usually means required tables are missing.

Then inspect per-ticker `reasons` and `warnings`:

- Missing `derived`, `valuation`, or `rank` usually means existing pipeline steps need to be run outside this preflight.
- `QUARTER_STATE_MARKET_MISMATCH` means quarter-state metadata exists but does not match the requested market.
- `.HE` tickers are expected to be `omxh` under the current Phase 2 identity contract.

The schema gap section should be treated as a roadmap signal, not as a data-quality failure.

## Phase 4 Direction

Phase 4A addresses `reported_fundamentals` point-in-time and vintage design, Phase 4B adds additive schema tables, Phase 4C1 adds a helper writer, and Phase 4C2 adds a read-only helper without changing current write paths or readers. The highest-value concepts are:

- `available_at_utc`
- first-class `filed_at_utc`
- `statement_vintage_id`
- `source_hash`
- restatement/revision metadata

Those changes now have a Phase 4B schema foundation plus Phase 4C1/4C2 helper layers, but provider writes, dual-write behavior, backfill, preflight population checks, and reader changes should still be implemented only in later, separately approved phases.
