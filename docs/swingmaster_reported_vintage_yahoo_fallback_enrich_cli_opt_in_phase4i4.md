# SwingMaster Reported Vintage Yahoo Fallback Enrich CLI Opt-In Phase 4I4

## Purpose

Phase 4I4 adds a narrow, default-off vintage write option to `run_fundamental_yahoo_fallback_enrich.py`.

The Yahoo fallback enrich CLI still fills missing generic quarterly fields from existing `rc_fundamental_yahoo_quarterly` rows as before. Vintage writes happen only when `--write-vintage` is supplied with explicit PIT metadata.

## Default Behavior

Without `--write-vintage`, fallback enrich behavior remains unchanged:

- read current generic rows from `rc_fundamental_quarterly`
- read Yahoo fallback rows from `rc_fundamental_yahoo_quarterly`
- match exact dates or same-quarter dates within the existing 7-day tolerance
- fill only currently `NULL` generic fields
- do not overwrite existing values
- write latest-compatible updates as before
- write `FILLED_FROM_YAHOO` audit rows as before
- write no statement vintage rows
- write no field provenance rows

## Opt-In Flags

Vintage mode requires:

- `--write-vintage`
- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`

Optional:

- `--vintage-normalization-run-id`

Missing required vintage metadata fails before DB writes. Availability is never inferred from `period_end_date`, and required vintage timestamps are not read from the current clock.

## Opt-In Vintage Flow

When `--write-vintage` is enabled and the CLI is not in dry-run mode, the path is:

```text
existing fallback enrich scan
-> collect affected ticker/periods from pending audit rows and missing-quarter inserts
-> apply latest-compatible updates/inserts as before
-> write one vintage row per affected ticker/period
-> write field provenance rows
```

No vintage is written for a no-op enrich where no field was filled and no missing quarter was inserted.

## Fallback Fill Provenance

For fields filled from Yahoo audit rows:

- `source_provider = yahoo`
- `source_table = rc_fundamental_quarterly_enrichment_audit`
- `provenance_role = FALLBACK_REPORTED`
- `merge_action = YAHOO_FILLED_MISSING`

Retained non-null fields are not marked Yahoo. If explicit SEC provenance is not supplied by the current CLI path, retained fields use the existing scaffold safety behavior:

- `source_provider = unknown`
- `provenance_role = UNSPECIFIED`
- `merge_action = SOURCE_NOT_PROVIDED`

This preserves the rule that Yahoo fallback fills must not silently claim retained SEC/current fields.

## Missing-Quarter Insert Provenance

When the existing missing-quarter insert path inserts an entire generic row from Yahoo, Phase 4I4 writes a Yahoo-source vintage using mode `yahoo_missing_quarter_insert`.

For non-null inserted fields:

- `source_provider = yahoo`
- `source_table = rc_fundamental_yahoo_quarterly`
- `provenance_role = PROVIDER_REPORTED`
- `merge_action = YAHOO_INSERTED_MISSING_QUARTER`

This path is intentionally distinct from field-level fallback fill provenance.

## Temp-DB Verification

`test_yahoo_fallback_enrich_cli_vintage_opt_in.py` verifies:

- default mode writes latest/audit only
- default mode creates no vintage/provenance rows
- required vintage metadata flags fail clearly
- exact-date fallback fill writes one mixed vintage and provenance
- same-quarter tolerance fallback fill writes one mixed vintage
- Yahoo-filled fields use Yahoo fallback provenance
- retained fields are not marked Yahoo
- missing-quarter insert writes Yahoo-source vintage provenance
- no-op enrich creates no vintage/provenance rows
- PIT reader behavior before and at/after `available_at_utc`
- duplicate vintage ids surface SQLite integrity errors
- dry-run with `--write-vintage` writes nothing
- provider modules are not imported

## Production Status

Yahoo fallback enrich CLI now has a default-off vintage opt-in path.

Still not wired:

- `run_fundamental_quarter_update.py`
- provider refresh
- scheduler
- real DB execution
- TTM
- scoring
- valuation
- UI
- ESS readers

No provider, refresh, scheduler, or real DB run was executed in this phase. The real `fundamentals_usa.db` is not modified. Tests use pytest temp DBs.

## Recommended Next Phase

The next phase should keep quarter-update wiring separate. A reasonable next step is a bounded dry-run/preflight for opt-in provider CLI paths, still without scheduler or automatic production writes.
