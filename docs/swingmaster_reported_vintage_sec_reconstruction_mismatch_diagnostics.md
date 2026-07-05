# SwingMaster SEC vintage reconstruction mismatch diagnostics

Date: 2026-07-05

Scope: `fundamentals_usa.db` in this repository only. This was a read-only
diagnostics pass for the 42 latest-only rows that the SEC-derived vintage
dry-run classified as `BLOCKED_RECONSTRUCTION_MISMATCH`.

No DB writes, provider calls, refresh jobs, backfills, final mixed writes,
scheduler jobs, or production behavior changes were performed.

## Purpose

Diagnose why the 42 latest-only rows in `rc_fundamental_quarterly` do not match
the normalized rows reconstructed by the current SEC reconstruction helper from
available SEC raw facts.

## CLI added

```bash
PYTHONPATH=. python3 -m swingmaster.cli.diagnose_sec_vintage_reconstruction_mismatch \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --source-run-id USA_QUARTER_UPDATE_2026-07-05__QUARTERLY \
  --format json \
  --sample-limit 20
```

Read-only guards:

- Opens the DB with `file:<path>?mode=ro`.
- Sets `PRAGMA query_only=ON`.
- Uses only `SELECT` statements.
- Does not call SEC, Yahoo, yfinance, Finnhub, paid providers, refresh jobs, or
  schedulers.

## Real DB diagnostics summary

| Metric | Rows |
|---|---:|
| Candidate rows | 42 |
| Matched rows | 0 |
| Mismatched rows | 42 |
| SEC raw evidence rows by candidate presence | 42 |
| Yahoo quarterly evidence rows by candidate presence | 0 |
| Enrichment audit evidence rows by candidate presence | 0 |
| Quarter state rows by candidate presence | 42 |

Likely cause:

```text
DRY_RUN_RECONSTRUCTION_PATH_DIFFERS_FROM_LATEST_WRITER
```

Recommendation:

```text
ALIGN_SEC_DRY_RUN_WITH_LATEST_WRITER
```

## Field mismatch summary

| Field | Mismatched rows |
|---|---:|
| `shares_outstanding` | 40 |
| `capex` | 39 |
| `free_cashflow` | 39 |
| `net_income` | 38 |
| `operating_cashflow` | 34 |
| `gross_profit` | 27 |
| `operating_income` | 19 |
| `ebit` | 19 |
| `revenue` | 18 |
| `total_debt` | 5 |
| `cash` | 4 |

Field-status aggregate:

| Status | Count |
|---|---:|
| `VALUE_DIFF` | 157 |
| `RECON_HAS_VALUE_LATEST_NULL` | 109 |
| `LATEST_HAS_VALUE_RECON_NULL` | 16 |

This is not limited to derived fields such as `free_cashflow` or `total_debt`.
The mismatch is systematic across all 42 candidates.

## Sample mismatch rows

| Ticker | Period end | Mismatched fields |
|---|---|---|
| ACN | 2026-05-31 | `net_income`, `operating_cashflow`, `capex`, `free_cashflow`, `shares_outstanding` |
| AI | 2026-04-30 | `revenue`, `gross_profit`, `operating_income`, `ebit`, `net_income`, `operating_cashflow`, `capex`, `free_cashflow`, `shares_outstanding` |
| AIHS | 2026-03-31 | `revenue`, `gross_profit`, `operating_income`, `ebit`, `net_income`, `operating_cashflow`, `cash`, `shares_outstanding` |
| APOG | 2026-05-30 | `gross_profit`, `net_income`, `capex`, `free_cashflow`, `shares_outstanding` |
| ATEX | 2026-03-31 | `revenue`, `operating_income`, `ebit`, `net_income`, `operating_cashflow`, `capex`, `free_cashflow`, `shares_outstanding` |

Examples from the diagnostics output show a recurring sign/policy difference:

- For ACN `2026-05-31`, latest `capex` is `492491000.0`; SEC reconstruction
  returns `-186224000.0`.
- For ACN `2026-05-31`, latest `free_cashflow` is `9760438000.0`; SEC
  reconstruction returns `3599988000.0`.
- For APOG `2026-05-30`, latest `capex` is `6289000.0`; SEC reconstruction
  returns `-6289000.0`.

## Source evidence summary

All 42 candidates have exact SEC raw evidence for the same
`ticker + period_end_date`. None of the candidates had exact Yahoo quarterly
rows or enrichment audit rows in this diagnostics pass.

Therefore, the mismatch is not explained by Yahoo fallback or enrichment audit
evidence for these 42 rows.

## Code-path diagnosis

Repo evidence indicates a path mismatch:

- `run_fundamental_quarter_update.py` runs `run_sec_raw_bootstrap`, then
  `run_sec_reconstruct_quarterly` only when SEC vintage options are provided,
  and then runs `run_sec_quarterly_build_step`.
- `run_sec_quarterly_build_step` calls `build_and_insert_quarterly_rows`.
- `build_and_insert_quarterly_rows` loads rows directly from
  `rc_fundamental_statement_raw` and normalizes them with `build_quarterly_rows`.
- `dry_run_sec_vintage_for_missing_latest.py` and this diagnostics CLI use
  `reconstruct_quarterly_rows_with_provenance` followed by
  `build_quarterly_rows`.

The current latest rows therefore appear to have been written by the direct
statement-raw normalization path, while the SEC-derived vintage dry-run compares
against the SEC reconstruction helper path. Those paths have materially
different policies for multiple fields, especially capex sign/free cash flow and
whether additional SEC facts become normalized values.

## Recommendation

Do not backfill or apply SEC-derived vintage rows yet.

Recommended next phase: align the SEC vintage dry-run/apply candidate builder
with the actual latest writer path, or explicitly change the quarter-update
latest writer to use the SEC reconstruction path first. That decision should be
made in a separate fix task because it changes the semantic contract of the
reported fundamentals pipeline.

If the goal is to create vintage rows for exactly what latest currently stores,
the safer next design is a guarded latest-writer-aligned provenance plan. That
plan should document that the values are derived from the direct raw statement
normalizer, not from the current SEC reconstruction helper.

## Row counts unchanged

Post-diagnostics real DB counts:

| Table | Rows |
|---|---:|
| `rc_fundamental_quarterly` | 155373 |
| `rc_fundamental_quarterly_vintage` | 155331 |
| `rc_fundamental_quarterly_field_provenance` | 1306388 |

These match the pre-diagnostics counts from the prior drift investigation.

## Verification

```bash
PYTHONPATH=. pytest -q swingmaster/tests/test_diagnose_sec_vintage_reconstruction_mismatch.py
```

Result:

```text
9 passed
```
