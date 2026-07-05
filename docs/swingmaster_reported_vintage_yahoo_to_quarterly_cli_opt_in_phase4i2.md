# SwingMaster Reported Vintage Yahoo To Quarterly CLI Opt-In Phase 4I2

## Purpose

Phase 4I2 adds a narrow, default-off vintage write option to `run_fundamental_yahoo_to_quarterly.py`.

The Yahoo-to-generic bridge still reads existing rows from `rc_fundamental_yahoo_quarterly` and maps them to `rc_fundamental_quarterly` as before. Vintage writes happen only when `--write-vintage` is supplied with explicit PIT metadata.

## Default Behavior

Without `--write-vintage`, the CLI behavior remains the existing bridge path:

- read stored Yahoo quarterly rows for `market + symbol`
- map rows to generic quarterly rows using the existing mapping
- optionally delete existing latest rows when `--replace-symbol` is used
- write latest-compatible rows to `rc_fundamental_quarterly`
- do not write `rc_fundamental_quarterly_vintage`
- do not write `rc_fundamental_quarterly_field_provenance`

## Opt-In Flags

Vintage mode requires:

- `--write-vintage`
- `--vintage-market`
- `--vintage-available-at-utc`
- `--vintage-ingested-at-utc`
- `--vintage-run-id`

Optional:

- `--vintage-normalization-run-id`

Missing required vintage metadata fails before DB writes.

## Opt-In Vintage Flow

When `--write-vintage` is enabled and the CLI is not in dry-run mode, the path is:

```text
stored rc_fundamental_yahoo_quarterly rows
-> map_to_generic_quarterly_rows(...)
-> write_yahoo_quarterly_rows_with_optional_vintage(..., write_vintage=True)
```

The opt-in path writes:

- latest-compatible normalized quarterly rows in `rc_fundamental_quarterly`
- statement vintage rows in `rc_fundamental_quarterly_vintage`
- Yahoo field provenance rows in `rc_fundamental_quarterly_field_provenance`

`available_at_utc` is caller-provided. The CLI does not derive availability from `period_end_date` and does not use the current clock for required vintage timestamps.

## Yahoo Bridge Provenance

For non-null normalized fields from the Yahoo bridge, field provenance uses:

- `source_provider = yahoo`
- `source_table = rc_fundamental_yahoo_quarterly`
- `provenance_role = PROVIDER_REPORTED`
- `merge_action = YAHOO_BRIDGED`

The bridge's existing `ebit = operating_income` mapping is treated as Yahoo-derived because it is a direct generic-field alias of the existing Yahoo bridge output.

## Still Not Wired

Phase 4I2 does not wire:

- Yahoo fallback enrich production path
- `run_fundamental_quarter_update.py`
- SEC production paths
- provider refresh
- schedulers
- TTM
- scoring
- valuation
- UI
- ESS readers

No provider, refresh, scheduler, or real DB run was executed in this phase. The real `fundamentals_usa.db` is not modified. Tests use pytest temp DBs.

## Test Coverage

`test_yahoo_to_quarterly_cli_vintage_opt_in.py` covers:

- default path writes latest rows only and no vintage/provenance rows
- required vintage metadata flags fail clearly
- opt-in mode writes latest, vintage, and field provenance rows in a temp DB
- PIT reads return no row before `available_at_utc` and return a row at/after it
- Yahoo bridge field provenance uses `yahoo`, `PROVIDER_REPORTED`, and `YAHOO_BRIDGED`
- duplicate `statement_vintage_id` fails through SQLite integrity behavior
- dry-run with `--write-vintage` writes nothing

## Recommended Next Phase

The next phase can evaluate Yahoo fallback enrich production wiring only after an explicit availability policy and source metadata policy are accepted for that path. Quarter-update orchestration should remain separate until both bridge and fallback opt-in behavior have been verified against approved real-DB dry-run evidence.

## Phase 4I5 Quarter Update Design Reference

Phase 4I5 documents future quarter_update vintage opt-in design in [Reported Vintage Quarter Update Opt-In Design Phase 4I5](swingmaster_reported_vintage_quarter_update_opt_in_design_phase4i5.md).

The design keeps non-USA Yahoo bridge vintage execution from quarter_update out of scope until quarter_update flag validation and summary behavior are proven.
