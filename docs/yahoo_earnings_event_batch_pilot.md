# Yahoo Earnings Event Batch Pilot

## Scope

This phase implemented and exercised a guarded, resumable Yahoo earnings-event batch apply path against a limited 50-ticker production pilot. It did not run the full 2,936-ticker production backfill.

All runtime artifacts created in this phase were stored under:

```text
temp/yahoo_earnings_batch_backfill/20260730T201836Z/
```

## Environment

- Repository: `/home/kalle/projects/swingmaster`
- Python executable: `/home/kalle/projects/swingmaster/.venv/bin/python`
- Python version: `3.10.12`
- yfinance version: `1.5.2`
- `.venv` initially did not have `pytest`.
- Because there is no canonical dev dependency file, `pytest` was installed into the local `.venv` only. No tracked dependency declaration was changed.

## Initial State

- Git head before this phase: `7870021 Audit Yahoo earnings coverage universe`
- `PRAGMA quick_check`: `ok`
- `rc_earnings_event` rows: `383`
- Existing pilot rows: `AAPL=80`, `JPM=76`, `MSFT=78`, `NVDA=70`, `XOM=79`
- Non-pilot rows: `0`
- Duplicate natural keys: `0`
- `rc_fundamental_quarterly` rows: `155571`

## Six Failure Diagnostics

The six full-audit source failures were diagnosed with direct yfinance calls and repository normalization retries. Diagnostic files:

- `failure_diagnostics/six_failure_diagnostics.json`
- `failure_diagnostics/six_failure_repository_retry.json`

Direct `yfinance.Ticker(ticker).get_earnings_dates(limit=100)` results on retry:

| Ticker | Direct result | Shape | Columns | Index |
| --- | --- | ---: | --- | --- |
| BE | dataframe | `33x3` | `EPS Estimate`, `Reported EPS`, `Surprise(%)` | `DatetimeIndex`, `Earnings Date`, `America/New_York` |
| BOOM | dataframe | `85x3` | same | same |
| CYCN | dataframe | `13x3` | same | same |
| ITRI | dataframe | `100x3` | same | same |
| PPL | dataframe | `100x3` | same | same |
| SNBR | dataframe | `1x3` | same | same |

Repository normalization retry:

| Ticker | Status | Completed rows | Oldest completed | Newest completed |
| --- | --- | ---: | --- | --- |
| BE | `COVERAGE_PARTIAL` | 32 | `2018-11-06` | `2026-07-28` |
| BOOM | `COVERAGE_PARTIAL` | 72 | `2008-10-30` | `2026-07-29` |
| CYCN | `COVERAGE_PARTIAL` | 13 | `2019-08-12` | `2026-03-31` |
| ITRI | `COVERAGE_PARTIAL` | 76 | `2007-11-01` | `2026-07-28` |
| PPL | `COVERAGE_PARTIAL` | 79 | `2006-10-31` | `2026-05-08` |
| SNBR | `NO_YAHOO_ROWS` | 0 | n/a | n/a |

Conclusion: the earlier `['Earnings Date']` failures were not reproduced as a stable malformed dataframe condition. Current Yahoo responses have the expected index and columns. The failure is preserved as a source instability/transient yfinance/Yahoo condition rather than silently reclassified.

## Batch CLI

New CLI:

```bash
python -m swingmaster.cli.backfill_yahoo_earnings_events
```

The CLI consumes the prior full audit artifact as the candidate plan input instead of recalculating the full universe. Default mode is dry-run. Writes require explicit `--apply`; `--dry-run` and `--apply` together are rejected.

Runtime artifact paths are validated to resolve under repository `temp/`. Checkpoints, summaries, CSV output, ticker files, backups, and restore-readiness copies outside `temp/` are rejected.

Eligible audit classifications by default:

- `BACKFILL_READY_FULL_HISTORY`
- `BACKFILL_READY_PARTIAL_MARGIN_ONLY`
- `BACKFILL_PARTIAL_ACTUAL_HISTORY`

Excluded by default:

- `BACKFILL_NO_YAHOO_ROWS`
- `BACKFILL_SOURCE_FAILED`
- `BACKFILL_PARSE_FAILED`

The full audit contained zero `BACKFILL_READY_FULL_HISTORY` tickers, so the pilot includes margin-only and partial-actual-history candidates.

## Safety Model

- One verified pre-batch SQLite backup is created before any batch apply write.
- The backup is created with SQLite's backup API.
- The backup must open independently, pass `PRAGMA quick_check`, have nonzero size, and match source row counts.
- The batch suppresses standalone per-ticker full-database backups only after a verified pre-batch backup context exists.
- Each ticker is applied in one explicit transaction.
- Source failure for one ticker is recorded and does not roll back prior committed ticker transactions.
- Database transaction or post-write verification failure records the ticker failure, checkpoints, stops the batch, and returns nonzero.
- Atomic checkpoint, summary, and CSV artifacts are written after every ticker.
- Resume validates schema, database path, audit artifact hash, execution mode, and backup context.

## Pilot Selection

Final pilot ticker file:

```text
pilot/pilot_50_tickers.txt
```

Selected tickers:

```text
A, AA, AAL, AAMI, AAOI, AAON, AAP, AAPL, AAT, AB, ABAT, ABBV, ABCB, ABEO, ABG, ABM, ABNB, ABOS, ABR, ABSI, ABT, ALMU, ARWR, BZFD, CCB, CHEF, CLRB, CLSK, CNXN, CORT, CRDF, DEC, DGXX, DOLE, FBK, FTFT, HASI, JPM, LLYVA, LLYVK, MSFT, NVDA, ONCY, PNFP, SCNI, SIGA, SN, UNIT, VS, XOM
```

The initial dry-run included `ABCL`, which failed with `['Earnings Date']` after three attempts. Because known source-failed tickers were excluded from the write pilot, `ABCL` was replaced with `AAMI` and the dry-run was repeated successfully.

Final pilot composition:

- `BACKFILL_PARTIAL_ACTUAL_HISTORY`: `28`
- `BACKFILL_READY_PARTIAL_MARGIN_ONLY`: `22`
- Existing production pilot tickers included: `AAPL`, `JPM`, `MSFT`, `NVDA`, `XOM`
- Capped tickers included: `ARWR`, `CHEF`, `CLRB`, `CLSK`, `CORT`, `CRDF`
- Short-history tickers included: `DEC`, `DGXX`, `DOLE`, `LLYVA`, `LLYVK`, `ONCY`, `PNFP`, `SCNI`, `SN`, `UNIT`

No eligible ticker with punctuation or symbol-normalization punctuation was present in the canonical full-audit artifact.

## Dry-Run Result

Final dry-run artifacts:

- `pilot/dry_run2_checkpoint.json`
- `pilot/dry_run2_summary.json`
- `pilot/dry_run2.csv`

Result:

- selected tickers: `50`
- successful tickers: `50`
- source failures: `0`
- parse failures: `0`
- eligible completed events: `2151`
- would insert: `1768`
- would update: `0`
- would unchanged: `383`
- skipped: `0`
- duplicate input events: `0`

No database backup was created for dry-run and no database rows were changed.

## Apply Result

Apply artifacts:

- `pilot/apply_checkpoint.json`
- `pilot/apply_summary.json`
- `pilot/apply.csv`
- `backups/fundamentals_usa.db.20260730T201836Z.prebatch.bak`

Result:

- selected tickers: `50`
- successful tickers: `50`
- source failures: `0`
- parse failures: `0`
- eligible completed events: `2151`
- inserted: `1768`
- updated: `0`
- unchanged: `383`
- skipped: `0`
- duplicate input events: `0`

The 383 unchanged rows were the five existing pilot tickers. The write inserted only completed Yahoo earnings events for the selected 50 tickers.

## Idempotency Rerun

Idempotency artifacts:

- `pilot/idempotency_checkpoint.json`
- `pilot/idempotency_summary.json`
- `pilot/idempotency.csv`
- `backups/fundamentals_usa.db.20260730T201836Z.before_idempotency.bak`

Result:

- selected tickers: `50`
- successful tickers: `50`
- eligible completed events: `2151`
- inserted: `0`
- updated: `0`
- unchanged: `2151`
- source failures: `0`
- parse failures: `0`

Timestamp snapshots before and after the idempotency rerun were identical for all 2,151 pilot rows, confirming that `source_observed_at_utc` alone did not create update churn.

## Post-Pilot Database Verification

- `PRAGMA quick_check`: `ok`
- Total `rc_earnings_event` rows after pilot and idempotency: `2151`
- Duplicate natural keys: `0`
- `rc_fundamental_quarterly` rows: `155571`
- Unreported or future persisted rows: `0`
- Invalid metadata rows: `0`
- Non-selected newly written tickers: `0`
- Existing pilot tickers remain valid: `AAPL=80`, `JPM=76`, `MSFT=78`, `NVDA=70`, `XOM=79`

## Restore Readiness

Restore-readiness copy:

```text
restore_readiness/fundamentals_usa.db.20260730T201836Z.prebatch.restore_check.bak
```

Verification on the isolated copy:

- `PRAGMA quick_check`: `ok`
- `rc_earnings_event` rows: `383`
- Non-initial-pilot rows: `0`
- Duplicate natural keys: `0`
- Source quarterly rows: `155571`
- `rc_earnings_event` table exists with the expected columns from the earnings-event migration.

Note: `rc_fundamental_schema_version` in this database records version `1`; migration 029 is therefore verified operationally by the presence and structure of `rc_earnings_event`, not by a schema-version row.

## Tests

Final tests were run with `/home/kalle/projects/swingmaster/.venv/bin/python`.

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

Focused batch tests cover dry-run defaults, explicit apply, classification filtering, deterministic selection, one pre-batch backup, checkpointing, resume skip/retry, duplicate checkpoint avoidance, source-failure isolation, idempotent rerun, observation timestamp stability, dry-run no-write behavior, and temp-path rejection.

## Readiness

The guarded batch path is ready for a later full production backfill phase from an engineering perspective, subject to a separate explicit approval and operational window. Remaining blockers before full apply:

- decide whether to retry newly transient source failures like `BE`, `BOOM`, `CYCN`, `ITRI`, and `PPL`;
- decide how to handle `SNBR`, which currently returns no completed events;
- review capped long-history tickers separately because the yfinance limit can truncate older events;
- preserve the one-prebatch-backup and resumable checkpoint policy for the full run.
