# Fundamental Score Effective Dates

SwingMaster does not currently store fundamental scores in a standalone `rc_fundamental_score` table. The score layer is persisted on `rc_fundamental_ttm` through columns such as `fundamental_score`, `fundamental_score_lifecycle`, score component columns, and score rule columns.

This phase adds score-specific effective-date metadata to the same table:

- `score_effective_trading_date`
- `score_effective_date_status`
- `score_effective_date_policy`
- `score_effective_date_source_ttm_as_of_date`

The policy is `SOURCE_TTM_EFFECTIVE_DATE`. For each scored TTM row, the score effective trading date is copied from the exact same `(ticker, as_of_date)` TTM row's `effective_trading_date`. Score values, score components, percentiles, valuation rows, snapshots, state tables, reports, and UI behavior are not recalculated by this phase.

Statuses:

- `RESOLVED`: exact source TTM row exists and has `effective_trading_date`.
- `SOURCE_TTM_EFFECTIVE_DATE_NULL`: exact source TTM row exists but its TTM effective date is null.
- `SOURCE_TTM_NOT_FOUND`: reserved for guard logic if a score row is ever separated from its source TTM row.
- `SOURCE_TTM_AMBIGUOUS`: reserved for guard logic if duplicate `(ticker, as_of_date)` TTM source rows are encountered.

Current/latest score selection remains based on `as_of_date DESC`, matching the existing score behavior. Historical/as-of score selection uses `score_effective_trading_date <= requested_date` and orders by the most recent effective trading date.

Use the guarded CLI:

```bash
python -m swingmaster.cli.rebuild_fundamental_score_effective_dates --fundamentals-db fundamentals_usa.db --dry-run
python -m swingmaster.cli.rebuild_fundamental_score_effective_dates --fundamentals-db fundamentals_usa.db --apply --backup
```

All checkpoints, summaries, CSV output, and backups are constrained to `temp/fundamental_score_effective_date/<UTC>/...`. The first apply requires `--backup`; once metadata is fully populated, an idempotency apply can run without creating another backup.

Downstream percentile and valuation layers remain intentionally unchanged. They can be evaluated in later phases using these score effective dates, but this phase only adds and populates lightweight score metadata.

## Real Database Result

Applied to `fundamentals_usa.db` after a full dry-run:

- score rows on `rc_fundamental_ttm`: 146638
- resolved score effective-date rows: 111218
- `SOURCE_TTM_EFFECTIVE_DATE_NULL`: 35420
- `SOURCE_TTM_NOT_FOUND`: 0
- `SOURCE_TTM_AMBIGUOUS`: 0
- score values unchanged: yes
- source table counts unchanged: yes
- `PRAGMA quick_check`: ok

Representative resolved rows show `score_effective_trading_date` equal to the exact source TTM row's `effective_trading_date`. Missing-source-availability examples such as `AVNS` and `DGXX` remain null and receive no invented dates.

The first production apply created one verified backup under:

```text
temp/fundamental_score_effective_date/20260731T_score/apply/backups/fundamentals_usa.pre_score_effective_date.20260731T184346Z.db
```

The idempotency apply reported:

- inserted: 0
- score value updates: 0
- score effective-date updates: 0
- unchanged: 146638

## Percentile Follow-Up

Percentile rows derive from cross-sectional score populations. A historically safe percentile for date `D` must use only peer score rows with `score_effective_trading_date <= D`; each peer may have a different effective date. That requires a separate percentile phase and was intentionally not implemented here.
