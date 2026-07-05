# SwingMaster SEC-derived vintage dry-run for missing latest rows

Date: 2026-07-05

Scope: `fundamentals_usa.db` in this repository only. This was a read-only
dry-run against the real DB. No provider jobs, refresh jobs, schedulers,
backfills, apply paths, or database writes were run.

## Critical review result

The prompt was valid with one implementation guard: `--source-run-id` alone is
not narrow enough in the real DB because the latest run id matches many rows
that already have vintage rows. The CLI therefore treats the default dry-run
candidate set as latest rows that are missing matching vintage rows, optionally
filtered by `run_id` and `ticker`.

## CLI added

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_sec_vintage_for_missing_latest \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --source-run-id USA_QUARTER_UPDATE_2026-07-05__QUARTERLY \
  --available-at-utc 2026-07-05T00:00:00Z \
  --ingested-at-utc 2026-07-05T00:00:00Z \
  --vintage-run-id USA_QUARTER_UPDATE_2026-07-05__SEC_VINTAGE_DRY_RUN \
  --format json
```

Read-only guards:

- Opens the DB with `file:<path>?mode=ro`.
- Sets `PRAGMA query_only=ON`.
- Uses only `SELECT` statements.
- Does not import or call provider modules.
- Does not write latest, vintage, provenance, WAL cleanup, temp output, or JSON
  files.

## Real DB dry-run result

Command run:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_sec_vintage_for_missing_latest \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --source-run-id USA_QUARTER_UPDATE_2026-07-05__QUARTERLY \
  --available-at-utc 2026-07-05T00:00:00Z \
  --ingested-at-utc 2026-07-05T00:00:00Z \
  --vintage-run-id USA_QUARTER_UPDATE_2026-07-05__SEC_VINTAGE_DRY_RUN \
  --format json \
  --sample-limit 50
```

Summary:

| Metric | Rows |
|---|---:|
| Latest rows missing matching vintage row | 42 |
| Candidates checked | 42 |
| Ready rows | 0 |
| Blocked rows | 42 |
| Skipped rows | 0 |
| Planned vintage rows | 0 |
| Planned provenance rows | 0 |
| Duplicate vintage rows | 0 |
| No SEC raw rows | 0 |
| Incomplete provenance rows | 0 |
| Reconstruction mismatch rows | 42 |

Overall status:

```text
DRY_RUN_BLOCKED
```

## Row counts unchanged

The real DB row counts after the dry-run matched the prior drift investigation
counts:

| Table | Rows after dry-run |
|---|---:|
| `rc_fundamental_quarterly` | 155373 |
| `rc_fundamental_quarterly_vintage` | 155331 |
| `rc_fundamental_quarterly_field_provenance` | 1306388 |

## Interpretation

The dry-run confirms that exact SEC raw rows exist for all 42 latest-only
`ticker + period_end_date` pairs, but current SEC reconstruction does not
reproduce the current latest rows exactly.

Because every candidate is blocked by `BLOCKED_RECONSTRUCTION_MISMATCH`, the
dry-run intentionally does not produce planned vintage rows, planned field
provenance rows, source hashes, or statement vintage ids for real DB apply
candidates.

This means the next step is not safe to be a guarded SEC-derived apply for these
42 rows using the current reconstruction logic.

## Blocked row summary

All 42 candidates have:

```text
status = BLOCKED_RECONSTRUCTION_MISMATCH
```

Examples of mismatch fields:

| Ticker | Period end | Mismatch fields |
|---|---|---|
| ACN | 2026-05-31 | `net_income`, `operating_cashflow`, `capex`, `free_cashflow`, `shares_outstanding` |
| AI | 2026-04-30 | `revenue`, `gross_profit`, `operating_income`, `ebit`, `net_income`, `operating_cashflow`, `capex`, `free_cashflow`, `shares_outstanding` |
| CCL | 2026-05-31 | `net_income`, `operating_cashflow`, `capex`, `free_cashflow`, `shares_outstanding` |
| KR | 2026-05-23 | `net_income`, `capex`, `free_cashflow`, `shares_outstanding` |
| STZ | 2026-05-31 | `gross_profit`, `net_income`, `capex`, `free_cashflow`, `total_debt` |

## Recommendation

Do not apply SEC-derived vintage rows for these 42 rows yet.

Recommended next step: investigate why the current latest update path and the
current SEC reconstruction helper produce different normalized rows from the
same SEC raw evidence. The likely next design decision is one of:

- Adjust the SEC reconstruction parity logic if the dry-run is using a stricter
  or different normalization path than the latest-table update used.
- Add an explicit reconstruction mode that mirrors the quarter-update latest
  path exactly, then rerun this dry-run.
- If exact provider-derived parity is not achievable from current helpers,
  reconsider a guarded legacy-baseline vintage path for these 42 rows, with
  explicit metadata that it is latest-derived rather than SEC-reconstructed.

Follow-up: the mismatch diagnostics are documented in
`docs/swingmaster_reported_vintage_sec_reconstruction_mismatch_diagnostics.md`.
They indicate that the dry-run reconstruction path differs from the latest
writer path, so the next phase should align the candidate builder with the
actual latest writer semantics before any apply.

## Checks run

```bash
python3 -m py_compile \
  swingmaster/cli/dry_run_sec_vintage_for_missing_latest.py \
  swingmaster/tests/test_dry_run_sec_vintage_for_missing_latest.py
```

```bash
PYTHONPATH=. pytest -q swingmaster/tests/test_dry_run_sec_vintage_for_missing_latest.py
```

Result:

```text
9 passed
```

## Runtime/default behavior changed

No default behavior changed.

The new CLI is opt-in only. Existing provider, refresh, scheduler, migration,
and write paths were not changed.
