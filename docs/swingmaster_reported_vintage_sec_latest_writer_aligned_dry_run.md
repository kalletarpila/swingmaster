# SwingMaster SEC latest-writer aligned vintage dry-run

Date: 2026-07-05

Scope: `fundamentals_usa.db` in this repository only. This phase added a
latest-writer-aligned SEC vintage candidate path and a guarded apply CLI, but
the apply CLI was tested only on temp DBs and was not run against the real DB.

No provider calls, refresh jobs, schedulers, real DB writes, backfills, final
mixed writes, UI changes, TTM/scoring/valuation changes, or SEC reconstruction
behavior changes were performed.

## Purpose

The previous `sec_reconstruct` dry-run blocked all 42 latest-only rows because
it compared the existing latest rows against:

```text
sec_reconstruct_quarterly -> build_quarterly_rows
```

Diagnostics showed the latest rows were produced by the direct latest-writer
path:

```text
rc_fundamental_statement_raw -> build_quarterly_rows
```

This phase aligns the missing-vintage candidate construction with the latest
writer semantics. The candidate vintage row uses the existing latest row values
as the normalized row and uses local SEC raw facts only for metadata and
field-level provenance.

## Candidate semantics

Mode:

```text
--candidate-mode latest_writer
```

Policy:

- Use current `rc_fundamental_quarterly` values as the candidate vintage values.
- Require exact local SEC raw evidence for `ticker + period_end_date`.
- Assign SEC field provenance only when a local SEC raw fact clearly maps to the
  field and value.
- Assign `source_provider=unknown`, `provenance_role=UNKNOWN_RETAINED`, and
  `merge_action=SOURCE_NOT_PROVIDED` for non-null latest fields without clear
  SEC evidence.
- Do not claim Yahoo provenance.
- Do not call `sec_reconstruct_quarterly` in `latest_writer` candidate mode.

## Real DB aligned dry-run

Command run:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_sec_vintage_for_missing_latest \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --source-run-id USA_QUARTER_UPDATE_2026-07-05__QUARTERLY \
  --candidate-mode latest_writer \
  --available-at-utc 2026-07-05T00:00:00Z \
  --ingested-at-utc 2026-07-05T00:00:00Z \
  --vintage-run-id USA_QUARTER_UPDATE_2026-07-05__SEC_LATEST_WRITER_VINTAGE_DRY_RUN \
  --format json \
  --sample-limit 20
```

Summary:

| Metric | Rows |
|---|---:|
| Latest rows missing matching vintage row | 42 |
| Candidates checked | 42 |
| Ready rows | 42 |
| Ready with unknown provenance rows | 0 |
| Blocked rows | 0 |
| Skipped rows | 0 |
| Planned vintage rows | 42 |
| Planned provenance rows | 295 |
| Duplicate vintage rows | 0 |
| Metadata error rows | 0 |
| No SEC raw rows | 0 |

Overall status:

```text
DRY_RUN_READY
```

## Unknown provenance summary

No unknown provenance was needed in the real DB aligned dry-run:

| Metric | Rows |
|---|---:|
| Unknown provenance rows | 0 |
| Unknown provenance fields | 0 |

All 295 planned provenance rows were SEC-provenanced under latest-writer
matching semantics.

## Row counts unchanged

Post-dry-run real DB counts:

| Table | Rows |
|---|---:|
| `rc_fundamental_quarterly` | 155373 |
| `rc_fundamental_quarterly_vintage` | 155331 |
| `rc_fundamental_quarterly_field_provenance` | 1306388 |

These match the prior read-only counts.

## Guarded apply CLI

Implemented:

```text
swingmaster/cli/apply_sec_vintage_for_missing_latest.py
```

The apply CLI:

- Defaults to dry-run/no-write unless the exact approval token is supplied.
- Requires `--candidate-mode latest_writer`.
- Requires `--expected-count` for an approved apply.
- Creates a backup before writing.
- Writes only latest-without-vintage rows matching `--source-run-id`.
- Uses the latest-writer-aligned candidate builder.
- Was tested only on temp DBs.
- Was not run against `/home/kalle/projects/swingmaster/fundamentals_usa.db`.

Required approval token:

```text
USER_APPROVES_SEC_LATEST_WRITER_VINTAGE_APPLY
```

## Recommendation

The next phase can be a guarded real DB apply for exactly these 42 rows, using:

- `--candidate-mode latest_writer`
- `--source-run-id USA_QUARTER_UPDATE_2026-07-05__QUARTERLY`
- `--expected-count 42`
- the explicit approval token

Before applying, rerun the aligned dry-run immediately before the apply and
confirm:

- `overall_status=DRY_RUN_READY`
- `planned_vintage_rows=42`
- `planned_provenance_rows=295`
- `unknown_provenance_rows=0`
- real DB row counts have not changed unexpectedly

## Verification

The apply CLI was temp-tested only. The real DB apply was not run.
