# SwingMaster Reported Vintage Backfill Dry Run Phase 4F1

## Scope

Phase 4F1 adds a guarded dry-run planner for converting existing legacy `rc_fundamental_quarterly` latest rows into candidate reported-vintage rows.

It does not run a backfill.

It does not write to the real USA fundamentals DB.

It does not insert into:

- `rc_fundamental_quarterly_vintage`
- `rc_fundamental_quarterly_field_provenance`

It does not call providers, refresh jobs, schedulers, UI paths, ESS paths, or production wiring.

## Critical Review Result

The legacy latest table can provide candidate values, but it still cannot provide true historical availability metadata. Therefore the dry-run intentionally returns `DRY_RUN_PARTIAL_POLICY_REQUIRED` for rows that can be planned only with placeholder metadata.

This is the correct stop point before any actual backfill because `available_at_utc` must not be invented from `period_end_date`.

## CLI

Module:

```bash
python3 -m swingmaster.cli.dry_run_reported_vintage_backfill
```

Example read-only smoke command:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_reported_vintage_backfill \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-06-19 \
  --max-rows 5 \
  --include-sample-rows 5 \
  --format json
```

Useful options:

- `--tickers AAPL,MSFT` limits source rows to named tickers.
- `--max-rows 5` limits source-row inspection.
- `--include-sample-rows 5` includes candidate previews.
- `--format json` emits machine-readable output.
- `--fail-if-blocked` exits nonzero when required schema is missing.
- `--legacy-availability-policy` selects the Phase 4F2 legacy availability policy.
- `--legacy-available-at-utc` is required by `live_safe_legacy_baseline`.
- `--legacy-availability-lag-days` is required by `research_estimated_legacy`.
- `--verified-availability-file` is required by `externally_verified_release_date`.

## Phase 4F2 Availability Policies

Phase 4F2 keeps `policy_required` as the default. The dry-run must not silently invent `available_at_utc` from `period_end_date`.

Supported policies:

| CLI value | Availability behavior | Intended use | Main risk |
| --- | --- | --- | --- |
| `policy_required` | Leaves `available_at_utc` null and requires a policy decision. | Safest default before any write. | Not ready for backfill apply. |
| `live_safe_legacy_baseline` | Uses explicit `--legacy-available-at-utc`. | Live/forward baseline from a backfill timestamp. | Historical backtests before that timestamp will not see legacy vintages. |
| `research_estimated_legacy` | Uses `period_end_date + --legacy-availability-lag-days`. | Exploratory historical research. | Estimated and not audit-grade. |
| `externally_verified_release_date` | Uses a local verified availability file. | Best historical PIT target when verified source metadata exists. | Requires curated/verified local release-date data. |

`period_end_date` is a fiscal period boundary, not a publication or data-availability timestamp. It must not be used directly as `available_at_utc`.

`live_safe_legacy_baseline` is conservative for live/forward use because rows become available only from the explicit backfill timestamp onward.

`research_estimated_legacy` can use lag choices such as 45, 60, 90, or 120 days, but the CLI intentionally has no default lag. The user must choose one explicitly.

`externally_verified_release_date` is the preferred historical/PIT direction, but Phase 4F2 only reads a local CSV/JSONL file. It does not fetch EDGAR, Yahoo, paid-provider, or other network data.

Phase 4F3 note: the full real DB read-only dry-run using `live_safe_legacy_baseline` is documented in [Reported Vintage Full Real DB Dry Run Phase 4F3](swingmaster_reported_vintage_full_realdb_dry_run_phase4f3.md).

## Read-Only Guarantees

The CLI opens SQLite with:

- URI `mode=ro`
- `PRAGMA query_only=ON`

The code path uses only:

- `SELECT name FROM sqlite_master`
- `SELECT` from `rc_fundamental_quarterly`
- `SELECT` from `rc_fundamental_quarterly_vintage`

It does not execute `INSERT`, `UPDATE`, `DELETE`, migration SQL, provider calls, refresh logic, or scheduler logic.

Tests verify that a candidate-producing dry-run leaves both vintage and provenance tables empty.

## Dry-Run Policy Summary

Candidate vintage rows are planned from legacy latest rows only when:

- ticker is present
- `period_end_date` is present
- required vintage/provenance tables exist
- the same `market+ticker+period_end_date` is not already present in the vintage table
- the computed `statement_vintage_id` is not already present

The dry-run uses placeholder legacy metadata:

- `source_provider`: `UNKNOWN_LEGACY`
- `source_document_id`: `NULL`
- `filed_at_utc`: `NULL`
- `available_at_utc`: `NULL`
- `ingested_at_utc`: `NULL`
- `provider_observed_at_utc`: `NULL`
- `revision_number`: `1`
- `is_restated`: `0`
- `availability_quality`: `LEGACY_ESTIMATED`

Because true availability is unknown, each planned candidate has:

- `requires_policy_decision`: `true`
- warning `AVAILABLE_AT_REQUIRES_POLICY_DECISION`
- warning `LEGACY_PLACEHOLDER_METADATA_ONLY`

## Statement Vintage ID Policy

The proposed legacy identifier format is:

```text
legacy:{market}:{ticker}:{period_end_date}:{source_hash_prefix_16}
```

The identifier is deterministic for the same legacy latest row and market.

It is proposed only by the dry-run. A future real backfill still needs separate approval before these ids are written.

## Source Hash Policy

The source hash is SHA-256 over a stable JSON payload containing:

- `market`
- normalized `ticker`
- `period_end_date`
- all configured financial fields
- `currency`

The hash intentionally excludes `run_id` so that value-equivalent legacy rows do not get different ids only because they were written by different operational runs.

## Provenance Preview Policy

For each planned vintage candidate, the dry-run counts one planned provenance row per non-null financial field.

Null financial fields do not get placeholder provenance rows.

The proposed provenance metadata is:

- `source_provider`: `UNKNOWN_LEGACY`
- `provenance_role`: `LEGACY_BASELINE`
- `merge_action`: `LEGACY_BACKFILL_BASELINE`
- `created_by_run_id`: the dry-run id

## Status Model

| Status | Meaning |
| --- | --- |
| `BLOCKED_MISSING_SCHEMA` | Required source or target tables are missing. |
| `NO_SOURCE_ROWS` | Schema exists, but no latest rows matched the filters and as-of date. |
| `DRY_RUN_PARTIAL_POLICY_REQUIRED` | Candidate rows exist, but they require an explicit placeholder/availability policy before any real write. |
| `DRY_RUN_READY` | No schema blocker and no candidate row currently requires policy resolution. |
| `UNKNOWN` | Internal fallback status for an empty report before classification. |

Skipped-row reasons include:

- `MISSING_TICKER`
- `MISSING_PERIOD_END_DATE`
- `SOURCE_HASH_INPUT_MISSING`
- `STATEMENT_VINTAGE_ID_INPUT_MISSING`
- `ALREADY_HAS_VINTAGE`
- `DUPLICATE_STATEMENT_VINTAGE_ID`

## Output Interpretation

Use `summary.overall_status` as the top-level gate.

If status is `DRY_RUN_PARTIAL_POLICY_REQUIRED`, the dry-run found rows that could be materialized mechanically, but a real backfill must still define approved availability and placeholder metadata semantics.

`planned_vintage_rows` is the number of candidate vintage rows the planner would create if a future approved backfill followed this policy.

`planned_provenance_rows` is the count of non-null financial field provenance rows implied by those candidates.

`candidate_samples` are previews only. They are not persisted.

## What This Does Not Do

This phase does not:

- apply or modify migrations
- modify `fundamentals_usa.db`
- insert vintage rows
- insert provenance rows
- run provider refreshes
- connect ESS or backtest readers to vintage tables
- replace current `rc_fundamental_quarterly` latest-table semantics

## Why Real Backfill Still Requires Separate Approval

The real DB now has the reported-vintage schema, but legacy latest rows still lack true first-class availability metadata.

Before any actual backfill can run, the project needs explicit approval for how legacy `available_at_utc`, `ingested_at_utc`, provider metadata, and provenance placeholders should be represented. Phase 4F1 makes those choices visible without writing them.
