# SwingMaster Reported Vintage Legacy Availability Policy Phase 4F2

## Background

Legacy `rc_fundamental_quarterly` rows do not contain trusted `available_at_utc`.

For point-in-time reads, `period_end_date` is not enough. It is the fiscal/reporting period end, not the date when the report became available to SwingMaster or to the market.

Phase 4F2 hardens the dry-run so the user must choose how legacy availability is represented before any later real backfill can be approved.

This phase does not backfill data, write the real DB, call providers, run refresh jobs, or wire vintage readers/writers into production paths.

Phase 4F3 later documented a full real DB read-only dry-run using `live_safe_legacy_baseline`: [Reported Vintage Full Real DB Dry Run Phase 4F3](swingmaster_reported_vintage_full_realdb_dry_run_phase4f3.md).

## Policy Modes

| CLI value | Quality | `available_at_utc` source | Status impact |
| --- | --- | --- | --- |
| `policy_required` | `LEGACY_ESTIMATED` | None. Stays null. | Candidates remain `DRY_RUN_PARTIAL_POLICY_REQUIRED`. |
| `live_safe_legacy_baseline` | `LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL` | Explicit `--legacy-available-at-utc`. | Can become `DRY_RUN_READY` if no other blockers exist. |
| `research_estimated_legacy` | `LEGACY_ESTIMATED` | `period_end_date + --legacy-availability-lag-days`. | Can become `DRY_RUN_READY` if no other blockers exist. |
| `externally_verified_release_date` | `EXTERNALLY_VERIFIED` | Local verified availability file. | Can become `DRY_RUN_READY` only when every candidate has a verified match. |

## Risk Comparison

`policy_required` is the safest default. It refuses to synthesize historical availability and makes the remaining policy decision visible.

`live_safe_legacy_baseline` is safest for production/live-forward baseline use after a future backfill. It avoids historical lookahead by making all legacy rows available only from the chosen backfill timestamp onward. It is not suitable for historical backtests before that timestamp because those backtests will not see legacy vintages.

`research_estimated_legacy` is useful for exploratory historical analysis, but it is not audit-grade. Lag choices such as 45, 60, 90, or 120 days may be reasonable experiments, but the CLI does not pick a default and rejects zero or negative lag.

`externally_verified_release_date` is the preferred historical PIT/backtest target when the source metadata is actually verified. It must not be confused with an estimated lag. Missing verified rows stay non-ready, and duplicate verified rows fail clearly.

## Local Verified Availability File Contract

Phase 4F2 implements `externally_verified_release_date` only through a local CSV, JSONL, or NDJSON file.

Required fields:

- `market`
- `ticker`
- `period_end_date`
- `available_at_utc`
- `source_provider`
- `source_document_id`
- `source_hash`
- `verified_at_utc`

Optional fields:

- `filed_at_utc`
- `source_url_or_ref`
- `source_confidence`
- `notes`

Matching key:

```text
market + ticker + period_end_date
```

Rules:

- `ticker` is normalized with the current uppercase convention.
- `period_end_date` must be `YYYY-MM-DD`.
- `available_at_utc` and `verified_at_utc` must be `YYYY-MM-DDTHH:MM:SSZ`.
- Duplicate rows for the same key fail clearly.
- Missing matches keep the candidate in a policy-incomplete state.
- The dry-run never downgrades missing verified data to estimated data.
- The dry-run never upgrades estimated data to externally verified.

Example CSV:

```csv
market,ticker,period_end_date,available_at_utc,source_provider,source_document_id,source_hash,verified_at_utc
usa,AAPL,2026-03-31,2026-04-25T13:30:00Z,manual_verified,DOC-AAPL-2026Q1,abc123,2026-06-18T00:00:00Z
```

## Future Provider-Source Direction

Provider-derived release-date ingestion belongs to a later phase.

Possible future sources:

- SEC/EDGAR filing date, accepted date, or equivalent publication timestamp
- Yahoo earnings/reporting calendar if reliable enough
- paid provider actual report publication timestamp
- manually curated or audited release-date dataset

Phase 4F2 does not fetch provider data and does not validate release dates over the network.

## Recommendation

Keep `policy_required` as the default until a backfill apply phase is explicitly approved.

Use `live_safe_legacy_baseline` for production/live-forward baseline use after a future backfill timestamp is chosen.

Use `research_estimated_legacy` only for exploratory historical research.

Use `externally_verified_release_date` for the best historical PIT/backtest quality once verified release-date data is available locally with preserved source metadata.
