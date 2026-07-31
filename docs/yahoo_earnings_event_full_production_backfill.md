# Yahoo Earnings Event Full Production Backfill

## Scope

This phase completed the guarded full-production Yahoo earnings-event backfill for the canonical USA fundamentals universe on July 31, 2026.

Approved external egress was limited to ticker symbols sent to Yahoo Finance for earnings announcement retrieval. No fundamentals values, portfolio data, database rows, or other local data were sent.

No future or unreported earnings events were persisted. No fiscal-quarter matching, scheduler, quarter update, SEC reconstruction, or other workflow was run.

## Environment

- Repository: `/home/kalle/projects/swingmaster`
- Python executable: `/home/kalle/projects/swingmaster/.venv/bin/python`
- Python version: `3.10.12`
- yfinance version: `1.5.2`
- Runtime subtree: `temp/yahoo_earnings_full_backfill/20260731T062518Z/`
- Source audit artifact: `temp/yahoo_earnings_coverage_audit/20260730T161944Z/full.json`
- Augmented run plan: `temp/yahoo_earnings_full_backfill/20260731T062518Z/plans/full_augmented_with_retries.json`

## Preflight

Initial repository state included pre-existing unrelated tracked deletions:

```text
D failed_yahoo_batch_FIN_YAHOO_BATCH_1.txt
D failed_yahoo_batch_FIN_YAHOO_BATCH_TEST5.txt
D failed_yahoo_batch_FIN_YAHOO_OMXH_BASELINE_1.txt
```

Those files were not restored or staged.

Initial database state:

- `PRAGMA quick_check`: `ok`
- `journal_mode`: `wal`
- `fundamentals_usa.db` size: `6572572672` bytes
- `rc_fundamental_quarterly` rows: `155571`
- `rc_earnings_event` rows: `2151`
- distinct earnings tickers: `50`
- duplicate natural-key groups: `0`
- unreported/future rows: `0`
- invalid metadata rows: `0`

Disk space was sufficient:

- filesystem size: `1007G`
- available: `715G`
- used: `26%`

## Audit Plan Validation

Canonical audit artifact validation:

- artifact schema version: `2`
- database path: `/home/kalle/projects/swingmaster/fundamentals_usa.db`
- ticker universe count: `2936`
- duplicate ticker entries: `0`
- deterministic ticker order: `true`
- eligible candidates before recovered retries: `2802`
- excluded no-row tickers before retries: `128`
- excluded source-failed tickers before retries: `6`
- excluded parse-failed tickers before retries: `0`
- already-present ticker count: `50`
- new candidate ticker count: `2752`
- audit artifact SHA-256: `a2756ae4a3d7f6fed8788bd02c820d3fa855fd1f9ed260ff51aada15092a6e44`

After targeted retry augmentation:

- recovered and included: `5`
- selected candidate count: `2807`
- selected completed events from plan: `135004`
- excluded no-row tickers: `129`
- remaining source-failed tickers: `0`
- remaining parse-failed tickers: `0`
- augmented artifact SHA-256: `21346db5575a9e1663d9487fcb6d8757bcb762c8d0bbeb0967ecb62d1c72d9dc`

Eligible classifications:

- `BACKFILL_READY_FULL_HISTORY`
- `BACKFILL_READY_PARTIAL_MARGIN_ONLY`
- `BACKFILL_PARTIAL_ACTUAL_HISTORY`

The source audit contained zero `BACKFILL_READY_FULL_HISTORY` candidates. Partial history was treated as eligible when completed Yahoo events existed.

## Prior Failure Retries

Targeted fresh read-only retries before the main run:

| Ticker | Status | Completed rows | Decision |
| --- | --- | ---: | --- |
| BE | `COVERAGE_PARTIAL` | 32 | include |
| BOOM | `COVERAGE_PARTIAL` | 72 | include |
| CYCN | `COVERAGE_PARTIAL` | 13 | include |
| ITRI | `COVERAGE_PARTIAL` | 76 | include |
| PPL | `COVERAGE_PARTIAL` | 79 | include |
| SNBR | `NO_YAHOO_ROWS` | 0 | exclude |

No aliases were invented and yfinance internals were not modified.

## Dry-Run

Command:

```bash
.venv/bin/python -m swingmaster.cli.backfill_yahoo_earnings_events --fundamentals-db fundamentals_usa.db --audit-json temp/yahoo_earnings_full_backfill/20260731T062518Z/plans/full_augmented_with_retries.json --checkpoint-json temp/yahoo_earnings_full_backfill/20260731T062518Z/dry_run/checkpoint.json --summary-json temp/yahoo_earnings_full_backfill/20260731T062518Z/dry_run/summary.json --output-csv temp/yahoo_earnings_full_backfill/20260731T062518Z/dry_run/per_ticker.csv --progress-log temp/yahoo_earnings_full_backfill/20260731T062518Z/dry_run/progress.log --sleep-min-seconds 0.8 --sleep-max-seconds 1.4 --rate-limit-backoff-seconds 30,60,120 --max-retries 2 --dry-run --json
```

The first dry-run invocation was interrupted during monitoring after 9 tickers; it was resumed from the atomic checkpoint and did not restart successful tickers.

Dry-run window:

- first checkpoint: `2026-07-31T06:40:26Z`
- final checkpoint: `2026-07-31T08:04:56Z`

Dry-run result:

- selected tickers: `2807`
- successful tickers: `2806`
- source failures: `1`
- parse failures: `0`
- eligible completed events: `135050`
- would insert: `132899`
- would update: `0`
- would unchanged: `2151`
- skipped: `0`
- duplicate input events: `0`

The only dry-run source failure was `FRHC`, after three attempts with `YAHOO_SOURCE_ERROR` and message `['Earnings Date']`. This was treated as an isolated transient source condition, not a broad Yahoo outage or rate-limit event.

After dry-run, database counts were unchanged:

- `rc_earnings_event` rows: `2151`
- duplicate natural-key groups: `0`
- `rc_fundamental_quarterly` rows: `155571`

## Backup

One verified pre-backfill backup was created before apply:

```text
temp/yahoo_earnings_full_backfill/20260731T062518Z/backups/fundamentals_usa.db.20260731T062518Z.pre_full_backfill.bak
```

Backup verification:

- file size: `6572572672` bytes
- `PRAGMA quick_check`: `ok`
- pre-backfill event rows: `2151`
- pre-backfill quarterly rows: `155571`
- duplicate natural-key groups: `0`
- earnings-event table present: `rc_earnings_event`
- earnings-event indexes present:
  - `idx_rc_earnings_event_announcement_date`
  - `idx_rc_earnings_event_reported`
  - `idx_rc_earnings_event_source`
  - `idx_rc_earnings_event_ticker_date`
  - `sqlite_autoindex_rc_earnings_event_1`

No per-ticker full-database backups were created.

## Apply

Command:

```bash
.venv/bin/python -m swingmaster.cli.backfill_yahoo_earnings_events --fundamentals-db fundamentals_usa.db --audit-json temp/yahoo_earnings_full_backfill/20260731T062518Z/plans/full_augmented_with_retries.json --checkpoint-json temp/yahoo_earnings_full_backfill/20260731T062518Z/apply/checkpoint.json --summary-json temp/yahoo_earnings_full_backfill/20260731T062518Z/apply/summary.json --output-csv temp/yahoo_earnings_full_backfill/20260731T062518Z/apply/per_ticker.csv --progress-log temp/yahoo_earnings_full_backfill/20260731T062518Z/apply/progress.log --sleep-min-seconds 0.8 --sleep-max-seconds 1.4 --rate-limit-backoff-seconds 30,60,120 --max-retries 2 --apply --prebatch-backup temp/yahoo_earnings_full_backfill/20260731T062518Z/backups/fundamentals_usa.db.20260731T062518Z.pre_full_backfill.bak --json
```

Apply settings:

- sequential Yahoo requests
- jitter: `0.8` to `1.4` seconds
- max retries: `2`, meaning three total attempts
- rate-limit backoff sequence: `30`, `60`, `120` seconds
- atomic checkpoint after every ticker
- one transaction per ticker
- completed events only

Apply window:

- first checkpoint: `2026-07-31T08:08:41Z`
- final checkpoint: `2026-07-31T09:31:41Z`

Apply result:

- selected tickers: `2807`
- successful tickers: `2807`
- source failures: `0`
- parse failures: `0`
- eligible completed events: `135055`
- inserted: `132904`
- updated: `0`
- unchanged: `2151`
- skipped: `0`
- duplicate input events: `0`

Exact real-database writes: `132904` completed Yahoo earnings-event rows were inserted into `rc_earnings_event`. No rows were updated or deleted.

## Resume Validation

The completed apply checkpoint was rerun with `--resume-from-json`. All successful tickers were skipped, no duplicate result entries were created, and no database writes were performed.

Resume validation result:

- completed tickers: `2807`
- successful tickers: `2807`
- inserted: `132904` in the carried checkpoint summary
- updated: `0`
- unchanged: `2151`
- source failures: `0`
- parse failures: `0`

## Idempotency

A second full safe apply pass was run with a fresh backup:

```text
temp/yahoo_earnings_full_backfill/20260731T062518Z/backups/fundamentals_usa.db.20260731T062518Z.before_idempotency_full.bak
```

Idempotency window:

- first checkpoint: `2026-07-31T09:36:36Z`
- final checkpoint: `2026-07-31T10:58:28Z`

Idempotency result:

- selected tickers: `2807`
- successful tickers: `2807`
- eligible completed events: `135055`
- inserted: `0`
- updated: `0`
- unchanged: `135055`
- source failures: `0`
- parse failures: `0`

Timestamp/content hash before and after idempotency matched:

```text
cfe630978179f35a8b730d6d4f8c09bf8f3b458b16afea18d81c04b606052fa9
```

This confirms `created_at_utc`, `updated_at_utc`, and `source_observed_at_utc` remained stable for semantically unchanged rows.

## Post-Backfill Verification

Final database state:

- `PRAGMA quick_check`: `ok`
- `fundamentals_usa.db` size: `6619226112` bytes
- total `rc_earnings_event` rows: `135055`
- distinct earnings tickers: `2806`
- rows inserted this run: `132904`
- rows updated this run: `0`
- rows unchanged during apply: `2151`
- duplicate natural-key groups: `0`
- unreported/future rows: `0`
- invalid market rows: `0`
- invalid source rows: `0`
- invalid timezone rows: `0`
- invalid session rows: `0`
- null `announcement_at` rows: `0`
- null `announcement_date` rows: `0`
- reported rows with null `reported_eps`: `0`
- announcement date mismatch against local `announcement_at` date: `0`
- `rc_fundamental_quarterly` rows: `155571`

All persisted rows have:

- `market='usa'`
- `source='YAHOO_FINANCE'`
- `source_timezone='America/New_York'`
- `is_reported=1`
- non-null `reported_eps`

Selected ticker containment:

- selected candidate count: `2807`
- distinct persisted event tickers: `2806`
- event tickers outside selected candidates: `0`
- selected candidates without persisted events: `AVNS`

`AVNS` was selected from the augmented audit plan, but Yahoo returned no completed rows during apply. No placeholder row was written.

## Per-Ticker And Groups

Per-ticker persistence summaries:

- `verification/per_ticker_persistence_summary.csv`
- `verification/per_ticker_persistence_summary.json`

Aggregate groups:

- successful with events: `2806`
- successful but no new rows: `49`
- source failed: `0`
- parse failed: `0`
- no Yahoo rows during apply: `1` (`AVNS`)
- partial actual history: `1573`
- margin-only partial: `1234`
- capped selected tickers: `18`

Partial history was not treated as a write failure.

## Capped Tickers

Capped selected tickers:

```text
ARWR, BGMS, CHEF, CLRB, CLSK, CORT, CRDF, CYTK, DCTH, HROW, IRD, LITS, MNKD, SRPT, SVRA, TGTX, VTGN, WKSP
```

For capped tickers, all returned valid completed events were persisted. The cap remains a coverage-quality limitation because some older actual fundamentals history may be missing; no unsupported request above the yfinance 100-row cap was attempted and no older events were synthesized.

## No-Row And Failure Lists

Final lists are stored in:

```text
verification/final_lists.json
```

Final counts:

- `NO_YAHOO_ROWS`: `130`
- `SOURCE_FAILED`: `0`
- `PARSE_FAILED`: `0`

The no-row count includes original excluded no-row tickers plus `SNBR` from targeted retry and `AVNS` from the apply result. Missing Yahoo rows are not interpreted as confirmed absence of an earnings announcement; they only mean this Yahoo source path did not provide a completed event.

## Restore Readiness

Restore-check copy:

```text
temp/yahoo_earnings_full_backfill/20260731T062518Z/restore_readiness/fundamentals_usa.db.20260731T062518Z.pre_full_backfill.restore_check.bak
```

Verification:

- file size: `6572572672` bytes
- `PRAGMA quick_check`: `ok`
- pre-backfill event rows: `2151`
- pre-backfill quarterly rows: `155571`
- duplicate natural-key groups: `0`
- rows beyond the 50-ticker pilot: `0`
- earnings-event table and indexes present

No restore over the real database was performed.

## Runtime Artifacts

All new runtime artifacts were kept under:

```text
temp/yahoo_earnings_full_backfill/20260731T062518Z/
```

The subtree size after the run was about `19G`, dominated by:

- pre-backfill backup
- idempotency backup
- restore-check copy
- JSON/CSV checkpoints and verification artifacts

## Tests

Before production execution, focused tests were run with `.venv`:

```bash
.venv/bin/python -m py_compile \
  swingmaster/fundamentals/earnings_events.py \
  swingmaster/fundamentals/earnings_event_repo.py \
  swingmaster/cli/apply_yahoo_earnings_events.py \
  swingmaster/cli/audit_yahoo_earnings_coverage.py \
  swingmaster/cli/backfill_yahoo_earnings_events.py

PYTHONPATH=. .venv/bin/python -m pytest -q \
  swingmaster/tests/test_yahoo_earnings_events.py \
  swingmaster/tests/test_yahoo_earnings_apply_audit.py \
  swingmaster/tests/test_yahoo_earnings_batch_backfill.py
```

Result:

```text
36 passed
```

The full SwingMaster suite was not run.

## Remaining Gaps

- 130 tickers have no completed Yahoo rows through this source path.
- 18 selected capped tickers may have missing older actual-history coverage due to the guarded yfinance 100-row cap.
- No fiscal-quarter matching has been implemented or run in this phase.

## Readiness

The Yahoo earnings-event source table is now populated for the full eligible backfill set and has passed integrity, idempotency, and restore-readiness checks. A later earnings-to-quarter matching phase can proceed from these persisted events, with separate design and verification for fiscal-period association.
