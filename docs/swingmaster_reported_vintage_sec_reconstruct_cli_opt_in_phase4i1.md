# SwingMaster Reported Vintage SEC Reconstruct CLI Opt-In Phase 4I1

## Purpose

Phase 4I1 adds a narrow, default-off vintage write option to `run_fundamental_sec_reconstruct_quarterly.py`.

The CLI still reconstructs SEC quarterly raw rows from stored `sec_fact` rows as before. Vintage writes happen only when `--write-vintage` is supplied with explicit PIT metadata.

## Default Behavior

Without `--write-vintage`, the CLI behavior remains the existing raw-reconstruction path:

- read stored `source = sec_edgar` and `period_type = sec_fact` rows
- reconstruct SEC quarterly raw statement rows
- insert reconstructed `period_type = quarterly` rows into `rc_fundamental_statement_raw`
- do not write `rc_fundamental_quarterly`
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
stored SEC sec_fact rows
-> reconstruct_quarterly_rows(...)
-> insert reconstructed quarterly raw rows
-> build_quarterly_rows(...)
-> build_sec_contributing_facts_by_reconstructed_rows(...)
-> write_sec_reconstructed_quarterly_rows_with_optional_vintage(..., write_vintage=True)
```

The opt-in path writes:

- latest-compatible normalized quarterly row in `rc_fundamental_quarterly`
- statement vintage row in `rc_fundamental_quarterly_vintage`
- SEC field provenance rows in `rc_fundamental_quarterly_field_provenance`

`available_at_utc` is caller-provided. The CLI does not derive availability from `period_end_date`.

## Provenance Guard

Before writing vintage rows, the CLI verifies that every non-null normalized financial field has SEC contributing facts.

If provenance cannot be built, the CLI fails with:

```text
SEC_RECONSTRUCT_CLI_VINTAGE_PROVENANCE_MISSING:<ticker>,<period_end_date>,<field_name>
```

This prevents a vintage row from being written with silently missing SEC field lineage.

## Still Not Wired

Phase 4I1 does not wire:

- SEC provider refresh
- Yahoo provider/fallback paths
- `run_fundamental_quarter_update.py`
- schedulers
- TTM
- scoring
- valuation
- UI
- ESS readers

The real `fundamentals_usa.db` is not modified by this phase. Tests use pytest temp DBs.

## Test Coverage

`test_sec_reconstruct_cli_vintage_opt_in.py` covers:

- default path writes reconstructed raw rows only and no vintage rows
- required vintage metadata flags fail clearly
- opt-in mode writes latest, vintage, and field provenance rows in a temp DB
- PIT reads return no row before `available_at_utc` and return a row at/after it
- SEC field provenance uses `sec_edgar`, `PRIMARY_REPORTED`, and `SEC_RETAINED`
- duplicate `statement_vintage_id` fails through SQLite integrity behavior
- dry-run with `--write-vintage` writes nothing

## Phase 4I5 Quarter Update Design Reference

Phase 4I5 documents future quarter_update vintage opt-in design in [Reported Vintage Quarter Update Opt-In Design Phase 4I5](swingmaster_reported_vintage_quarter_update_opt_in_design_phase4i5.md).

The design notes that quarter_update does not currently call the SEC reconstruct CLI vintage path and recommends validation/summary plumbing before any quarter_update vintage execution.
