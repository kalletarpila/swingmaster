# Fundamentals V3 Phase 2C Yahoo Raw Cache And Metadata Adapter

Date: 2026-08-21

Classification: `FUNDAMENTALS_V3_PHASE2C_YAHOO_RAW_CACHE_METADATA_ADAPTER_IMPLEMENTED`

## Scope

Phase 2C implements the reusable Yahoo bootstrap adapter layer. It connects approved V3 companies
to the existing Yahoo quarterly fetcher, the external V3 raw cache, normalized Yahoo quarterly rows,
metadata enrichment, and deterministic V3 migration candidates.

This phase does not run the full bootstrap, write canonical V3 quarter/fundamental rows, reconcile
Legacy/V2 values, implement Phase 3 canonical migration, change RawCandle, or change production
Check/Update.

## Implementation

Primary module:

```text
swingmaster/fundamentals/v3_yahoo_bootstrap.py
```

CLI wrapper:

```text
swingmaster/cli/run_fundamentals_v3_yahoo_bootstrap.py
```

The adapter flow is:

```text
APPROVED V3 COMPANY
        ↓
YahooFinanceClient.get_raw_payload(provider_symbol)
        ↓
V3RawCacheRepository -> rc_fundamentals_v3_raw.db
        ↓
existing Yahoo normalized quarterly mapper
        ↓
YahooMetadataEnricher exact metadata matching
        ↓
V3YahooMigrationCandidate
```

Requested ticker filters are fail-closed against `v3_company.active = 1`. A ticker that exists only
in V2, osakedata, or arbitrary caller input is rejected unless it is already present in the approved
V3 company universe.

## Raw Cache Contract

The adapter writes only to the external V3 raw cache through `V3RawCacheRepository`.

Persisted raw-cache metadata:

- `provider = YAHOO`
- `provider_symbol`
- `fetch_run_id`
- deterministic `payload_hash`
- canonical raw `payload_json`
- `status` in `OK`, `EMPTY`, `ERROR`
- `error_message`
- `observed_at_utc`

Provider `ERROR` rows are preserved in raw cache and do not produce normalized rows or migration
candidates.

Provider `EMPTY` rows are also preserved in raw cache and do not fabricate normalized quarterly
rows.

## Normalized Yahoo Quarter Contract

The adapter reuses the existing Yahoo normalizer. It does not introduce a second Yahoo field mapper.

Candidate value fields are limited to current V3 canonical fundamentals storage:

```text
revenue
gross_profit
ebit
ebitda
net_income
operating_cashflow
capex
free_cashflow
cash
total_debt
shares_outstanding
```

Yahoo `operating_income` remains provider detail only. It is not promoted into V3 candidate values
and is not mapped to EBIT.

## Metadata Enrichment Contract

`YahooMetadataEnricher` requires exact period-end metadata evidence. It does not infer fiscal
identity from calendar quarter.

Fiscal identity sources:

- V2 exact `rc_v2_company + rc_v2_quarter.report_date`
- provider observation content exact period match, when canonical FY/FQ is already present

Publication date sources:

- V2 `rc_v2_quarter.publish_date`
- result-event exact period match through `rc_fundamental_quarter_earnings_match.announcement_date`
- provider observation reported-at date only when that observation also carries exact canonical FY/FQ

Rows missing either fiscal identity or publication date are returned as metadata rejections and do
not become V3 migration candidates.

## Candidate Contract

Each `V3YahooMigrationCandidate` includes:

- company identity
- provider symbol
- fiscal year and fiscal quarter
- period end date
- publish date
- market availability date
- Yahoo value fields
- provider details
- provider cache reference
- fetch run id
- payload hash
- V3 work unit key
- deterministic candidate key
- derivation method
- candidate version

Candidates are deterministic and sorted by candidate key.

## Raw Cache Replay

The adapter exposes raw-cache replay through `replay_v3_yahoo_bootstrap_from_raw_cache`.

Replay does not call Yahoo. It loads existing `v3_raw_cache_entry` rows, applies the same
normalization and metadata enrichment path, and emits the same migration candidates for the same raw
payload.

## CLI Notes

The CLI reads approved companies from the V3 DB and writes raw payloads only to the external V3 raw
cache DB unless `--dry-run` is supplied.

`--dry-run` still fetches, normalizes, and builds candidates, but it does not write raw-cache rows.

`--replay-raw-cache` builds candidates from existing raw-cache rows without provider calls.

No CLI path writes canonical V3 quarters or fundamentals in Phase 2C.
