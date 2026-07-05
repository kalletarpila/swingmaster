# SwingMaster SEC latest-writer vintage apply Phase 4J4

Date: 2026-07-05

Scope: guarded real DB apply for the 42 USA latest-only quarterly rows from:

```text
USA_QUARTER_UPDATE_2026-07-05__QUARTERLY
```

Real DB:

```text
/home/kalle/projects/swingmaster/fundamentals_usa.db
```

No providers, schedulers, refresh jobs, broad backfills, final mixed writes,
latest-table writes, updates, deletes, or replacement writes were run.

## Purpose

Apply exactly the 42 SEC latest-writer-aligned vintage rows and their field
provenance rows to restore latest/vintage parity for the July 5 Q-results.

## Backup

Backup path:

```text
/home/kalle/projects/swingmaster/fundamentals_usa.db.sec_latest_writer_vintage_apply.bak
```

Backup read-only integrity check:

```text
PRAGMA quick_check = ok
```

Backup counts:

| Table | Rows |
|---|---:|
| `rc_fundamental_quarterly` | 155373 |
| `rc_fundamental_quarterly_vintage` | 155331 |
| `rc_fundamental_quarterly_field_provenance` | 1306388 |

## Pre-apply safety checks

Real DB read-only precheck:

```text
PRAGMA quick_check = ok
```

Pre-apply counts:

| Table | Rows |
|---|---:|
| `rc_fundamental_quarterly` | 155373 |
| `rc_fundamental_quarterly_vintage` | 155331 |
| `rc_fundamental_quarterly_field_provenance` | 1306388 |

Pre-apply parity:

| Metric | Rows |
|---|---:|
| Latest without vintage for source run | 42 |
| Vintage without latest | 0 |

## Dry-run gates

Command:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_sec_vintage_for_missing_latest \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --source-run-id USA_QUARTER_UPDATE_2026-07-05__QUARTERLY \
  --candidate-mode latest_writer \
  --available-at-utc 2026-07-05T00:00:00Z \
  --ingested-at-utc 2026-07-05T00:00:00Z \
  --vintage-run-id USA_QUARTER_UPDATE_2026-07-05__SEC_LATEST_WRITER_VINTAGE_APPLY \
  --format json \
  --sample-limit 20 \
  --fail-if-blocked
```

Gate result:

| Gate | Result |
|---|---:|
| `overall_status` | `DRY_RUN_READY` |
| `candidates_checked` | 42 |
| `ready_rows` | 42 |
| `blocked_rows` | 0 |
| `skipped_rows` | 0 |
| `planned_vintage_rows` | 42 |
| `planned_provenance_rows` | 295 |
| `unknown_provenance_rows` | 0 |
| `unknown_provenance_field_counts` | `{}` |

## Apply

The apply CLI does not accept a `--format` option; it emits JSON by default.
The real DB apply was therefore run with the supported argument set:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.apply_sec_vintage_for_missing_latest \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --source-run-id USA_QUARTER_UPDATE_2026-07-05__QUARTERLY \
  --candidate-mode latest_writer \
  --available-at-utc 2026-07-05T00:00:00Z \
  --ingested-at-utc 2026-07-05T00:00:00Z \
  --vintage-run-id USA_QUARTER_UPDATE_2026-07-05__SEC_LATEST_WRITER_VINTAGE_APPLY \
  --expected-count 42 \
  --approval-token USER_APPROVES_SEC_LATEST_WRITER_VINTAGE_APPLY
```

Apply result:

| Metric | Rows |
|---|---:|
| Vintage rows inserted | 42 |
| Provenance rows inserted | 295 |

## Post-apply verification

Real DB post-apply read-only check:

```text
PRAGMA quick_check = ok
```

Post-apply counts:

| Table | Rows |
|---|---:|
| `rc_fundamental_quarterly` | 155373 |
| `rc_fundamental_quarterly_vintage` | 155373 |
| `rc_fundamental_quarterly_field_provenance` | 1306683 |

Parity and duplicate checks:

| Metric | Rows |
|---|---:|
| Latest without vintage for source run | 0 |
| Vintage without latest | 0 |
| Duplicate `statement_vintage_id` groups | 0 |
| Inserted vintage rows | 42 |
| Duplicate inserted `ticker + period_end_date + statement_vintage_id` groups | 0 |
| Duplicate inserted `ticker + period_end_date` groups | 0 |
| Inserted provenance rows | 295 |

PIT visibility:

| Metric | Rows |
|---|---:|
| Inserted rows visible at `2026-07-05T00:00:00Z` | 42 |
| Inserted rows visible before `2026-07-05T00:00:00Z` | 0 |
| Older same-key vintage rows visible before `2026-07-05T00:00:00Z` | 0 |

## Post-apply no-op dry-run

Post-apply aligned dry-run result:

| Metric | Value |
|---|---:|
| `overall_status` | `NO_CANDIDATES` |
| `latest_missing_vintage_rows` | 0 |
| `candidates_checked` | 0 |
| `planned_vintage_rows` | 0 |
| `planned_provenance_rows` | 0 |

## Recommendation

The July 5 USA quarterly latest/vintage parity gap is closed for the scoped
source run. Next phase should be a read-only PIT/query smoke test over these
rows and any downstream consumer expectations; do not rerun this apply unless a
new latest-without-vintage gap is detected.
