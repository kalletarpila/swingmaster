# Fundamental TTM Effective Date

This phase adds lightweight historical availability metadata to `rc_fundamental_ttm` without changing TTM financial formulas or any current/latest downstream workflow.

## Policy

For retrospective research, a quarterly fundamentals row is available from:

```text
rc_fundamental_quarter_earnings_match.effective_trading_date
```

For a TTM row composed of four quarterly rows:

```text
effective_trading_date = MAX(component quarter effective_trading_date)
```

No fallback is invented from period end, fixed delay, ingestion time, vintage, or provenance timestamps. Current/latest workflows can continue to use the latest TTM row regardless of effective-date metadata.

## Schema

Migration:

```text
031_rc_fundamental_ttm_effective_date.sql
```

The migration runner adds nullable columns to `rc_fundamental_ttm`:

- `effective_trading_date`
- `effective_date_status`
- `effective_date_policy`
- `effective_date_source_period_end`
- `effective_date_match_confidence`
- `effective_date_component_count`

Index:

```text
idx_fundamental_ttm_ticker_effective_date
ON rc_fundamental_ttm(ticker, effective_trading_date)
```

Policy marker:

```text
MAX_COMPONENT_QUARTER_EFFECTIVE_DATE
```

Statuses:

- `RESOLVED`
- `MISSING_QUARTER_MATCH`
- `NULL_COMPONENT_EFFECTIVE_DATE`
- `INSUFFICIENT_COMPONENT_QUARTERS`

## Component Rule

For each existing TTM row, the rebuild reconstructs the component quarters from the canonical quarterly series:

1. Select quarterly rows for the same ticker.
2. Order by `period_end_date ASC`, matching existing TTM build behavior.
3. For a TTM row's `latest_period_end_date`, use the latest four quarterly periods at or before that period.
4. Join each component to `rc_fundamental_quarter_earnings_match` by `market`, `ticker`, and `period_end_date`.
5. If all four components have non-null effective dates, store the max as the TTM `effective_trading_date`.
6. Otherwise store null with an explicit status.

The schema intentionally does not store component lists, but the rebuild CSV artifact does for diagnostics.

## Selectors

Current/latest selector:

```python
select_latest_ttm_current(conn, ticker)
```

This preserves current behavior: latest row by `as_of_date DESC`.

Historical/as-of selector:

```python
select_latest_ttm_as_of(conn, ticker, as_of_date)
```

This requires:

```text
effective_trading_date IS NOT NULL
effective_trading_date <= as_of_date
```

Ordering:

1. `effective_trading_date DESC`
2. `latest_period_end_date DESC`
3. `as_of_date DESC`
4. `rowid DESC`

If no row is available, the helper returns `NO_AVAILABLE_TTM` and does not fall back to period end.

## Real-DB Results

Dry-run artifact:

```text
temp/fundamental_ttm_effective_date/20260731T_dry_run/dry_run/
```

Production apply artifact:

```text
temp/fundamental_ttm_effective_date/20260731T_apply/apply/
```

Verified backup:

```text
temp/fundamental_ttm_effective_date/20260731T_apply/apply/backups/fundamentals_usa.pre_ttm_effective_date.20260731T171601Z.db
```

Backup verification:

- `PRAGMA quick_check = ok`
- backup size: `6,676,996,096` bytes
- `rc_fundamental_ttm`: `146,638`
- `rc_fundamental_quarterly`: `155,571`
- `rc_fundamental_quarter_earnings_match`: `125,554`

Dry-run and apply counts:

- total TTM rows: `146,638`
- resolved rows: `111,218`
- missing quarter-match rows: `35,107`
- null component effective-date rows: `313`
- insufficient component-quarter rows: `0`
- rows where TTM period end precedes effective date: `111,218`
- historically unavailable rows: `35,420`
- median TTM availability delay: `33` days
- p95 TTM availability delay: `58` days

Production apply:

- inserted rows: `0`
- financial value updates: `0`
- effective-date updates: `146,638`
- unchanged rows on first apply: `0`
- source-table counts unchanged: true
- financial hash unchanged: true
- quick check: `ok`

Post-apply verification:

- duplicate TTM natural keys: `0`
- invalid policy rows: `0`
- effective date earlier than TTM period end: `0`
- `rc_fundamental_ttm`: `146,638`
- `rc_fundamental_quarterly`: `155,571`
- `rc_fundamental_quarter_earnings_match`: `125,554`

Idempotency run:

```text
temp/fundamental_ttm_effective_date/20260731T_apply/idempotency_no_backup/
```

- inserted rows: `0`
- financial value updates: `0`
- effective-date updates: `0`
- unchanged rows: `146,638`
- effective hash before and after: `b59a1b41ba8012cefd48d86aad0d7cc1b3351656d234aa82169f95f77415547f`

## Representative Examples

Recent resolved rows:

- AAPL `2026-03-28`: components `2025-06-28,2025-09-27,2025-12-27,2026-03-28`; component effective dates `2025-07-31,2025-10-30,2026-01-29,2026-04-30`; TTM effective date `2026-04-30`.
- MSFT `2026-03-31`: TTM effective date `2026-04-29`.
- JPM `2026-03-31`: TTM effective date `2026-04-14`.
- XOM `2026-03-31`: TTM effective date `2026-05-01`.
- NVDA `2026-04-26`: TTM effective date `2026-05-20`.
- GIS `2026-05-31`: TTM effective date `2026-07-01`.
- LMT `2026-06-28`: TTM effective date `2026-07-23`.
- BBY `2026-05-02`: TTM effective date `2026-05-28`.
- ARWR `2026-03-31`: TTM effective date `2026-05-07`.

Missing-match examples:

- AVNS recent TTM rows remain `MISSING_QUARTER_MATCH` with null effective dates.
- DGXX recent TTM rows remain `MISSING_QUARTER_MATCH` with null effective dates.

No fallback dates were assigned to missing-match cases.

## Regression

For AAPL:

- current/latest selector returned `2026-03-28`, matching the prior latest `MAX(as_of_date)` query.
- historical selector on `2026-04-29` returned `2025-12-27`.
- historical selector on `2026-04-30` returned `2026-03-28`.
- historical selector on `2026-05-01` returned `2026-03-28`.
- historical selector on `2000-01-01` returned `NO_AVAILABLE_TTM`.

## Downstream Scope

This phase does not change:

- scoring;
- percentiles;
- valuation;
- ticker snapshots;
- SwingMaster state/transitions;
- reports or UI;
- quarterly rows;
- quarter earnings match rows;
- vintage/provenance behavior.

Next downstream phase can use `select_latest_ttm_as_of(...)` for historical score, percentile, valuation, snapshot, and state integration while leaving current/latest selectors unchanged.

## Known Limitation

During idempotency validation, an earlier CLI branch created an additional backup under:

```text
temp/fundamental_ttm_effective_date/20260731T_apply/idempotency/
```

It was not deleted because runtime artifacts must not be moved or removed. The branch was fixed, and the final idempotency run under `idempotency_no_backup/` did not create another backup.
