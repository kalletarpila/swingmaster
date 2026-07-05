# Reported Vintage Quarter Update SEC Latest-Writer Opt-In Phase 4K1

Phase 4K1 adds a default-off quarter_update mode that can write SEC latest-writer-aligned quarterly vintage rows during the existing USA SEC refresh path.

## Scope

This phase is intentionally narrow:

- only the `run_fundamental_quarter_update.py` USA SEC refresh branch is wired
- no runtime default behavior changes
- no real providers, scheduler jobs, refresh jobs, or real DB writes were run
- no changes to SEC reconstruction semantics, latest-writer normalization, TTM, scoring, valuation, UI, ESS, or migrations
- tests use migrated temp SQLite databases and mocked provider-facing functions

## Opt-In Contract

The new mode is:

```text
--write-vintage
--vintage-mode sec_latest_writer
--vintage-market usa
--vintage-available-at-utc YYYY-MM-DDTHH:MM:SSZ
--vintage-ingested-at-utc YYYY-MM-DDTHH:MM:SSZ
--vintage-run-id <run id>
```

`sec_latest_writer` is rejected unless `--write-vintage` is present. The existing PIT metadata validation remains explicit; the code does not infer `available_at_utc` from period dates and does not silently use current time for vintage availability metadata.

## Wiring Behavior

When `sec_latest_writer` is explicitly enabled and the USA SEC refresh branch runs:

- quarter_update runs the existing SEC raw bootstrap path
- quarter_update runs the existing latest quarterly build path
- the side-write reads the latest quarterly rows written by `child_run_ids["quarterly"]`
- the side-write reads local SEC `sec_fact` raw rows for the same ticker
- vintage rows are inserted into `rc_fundamental_quarterly_vintage`
- field provenance rows are inserted into `rc_fundamental_quarterly_field_provenance`
- Yahoo fallback enrichment still runs after the SEC path as before

The side-write does not call a provider. It uses local rows already present in the same temp/target DB. In production use, this mode would still depend on the existing SEC refresh step having run.

## Provenance Policy

Normalized vintage values come from the existing latest-writer row. SEC raw rows are used as source evidence only.

If a non-null latest value cannot be matched back to local SEC raw facts for the same ticker and period, the side-write blocks with:

```text
FUNDAMENTAL_QUARTER_UPDATE_SEC_LATEST_WRITER_UNKNOWN_PROVENANCE
```

The write builds all candidates before inserting rows, so unknown provenance prevents partial vintage/provenance writes for that side-write call.

Existing vintage rows for the same `ticker + period_end_date + market` are treated as no-op skips. The code does not use `INSERT OR REPLACE` for vintage rows and does not replace existing vintage history.

## Summary Fields

For `sec_latest_writer`, quarter_update reports child-derived counts in the existing vintage summary shape:

- `vintage_rows_inserted`
- `vintage_provenance_rows_inserted`
- `vintage_rows_skipped_noop`
- `vintage_rows_failed`
- `vintage_count_status=sec_latest_writer_execution`

Without `--write-vintage`, vintage summary fields remain omitted as before.

## Verification

Covered by temp-DB tests in `swingmaster/tests/test_quarter_update_sec_latest_writer_vintage.py`:

- default quarter update writes latest only, no vintage rows
- explicit `sec_latest_writer` opt-in writes one vintage row and SEC provenance rows
- repeated side-write skips an existing vintage instead of replacing it
- unknown provenance blocks and leaves vintage/provenance tables empty
- `sec_latest_writer` requires explicit `--write-vintage`

Targeted regression command:

```bash
PYTHONPATH=. pytest -q swingmaster/tests/test_quarter_update_sec_latest_writer_vintage.py swingmaster/tests/test_quarter_update_sec_vintage_forwarding.py swingmaster/tests/test_quarter_update_vintage_flags.py
```

Result: `21 passed`.

## Phase 4K2 Follow-Up

Phase 4K2 adds read-only/temp-tested post-run parity and Yahoo-impact guardrails in [Reported Vintage Yahoo Impact Guard Phase 4K2](swingmaster_reported_vintage_yahoo_impact_guard_phase4k2.md).

The follow-up confirms that Yahoo fallback enrichment runs after the 4K1 SEC latest-writer side-write and can therefore create post-SEC latest/vintage drift by inserting a missing quarter or filling NULL fields from Yahoo. Default behavior remains unchanged; the guard summary is surfaced only for explicit `sec_latest_writer` vintage mode.
