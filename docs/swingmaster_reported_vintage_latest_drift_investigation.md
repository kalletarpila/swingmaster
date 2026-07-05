# SwingMaster reported vintage latest drift investigation

Date: 2026-07-05

Scope: `fundamentals_usa.db` in this repository only. This was a read-only
investigation. No provider jobs, backfills, schedulers, migrations, update jobs,
or write paths were run.

## Critical review result

The requested investigation is valid and bounded. The repository has a real USA
fundamentals SQLite database at:

```text
/home/kalle/projects/swingmaster/fundamentals_usa.db
```

The investigation can be completed with read-only SQLite queries. No runtime code
change is required to answer the drift question.

## Read-only checks run

All database queries used a read-only SQLite URI:

```text
file:/home/kalle/projects/swingmaster/fundamentals_usa.db?mode=ro
```

Each query set:

```sql
PRAGMA query_only=ON;
```

Executed checks:

- `PRAGMA quick_check;` returned `ok`.
- `PRAGMA integrity_check;` was attempted but did not complete within a
  reasonable time and was interrupted. No write operation was performed.
- Row counts for latest, vintage, and vintage field provenance tables.
- Latest-vs-vintage parity checks by `ticker + period_end_date`.
- Source-evidence checks against SEC raw, Yahoo quarterly, Yahoo raw, enrichment
  audit, and vintage field provenance tables.

## Table counts

| Table | Rows |
|---|---:|
| `rc_fundamental_quarterly` | 155373 |
| `rc_fundamental_quarterly_vintage` | 155331 |
| `rc_fundamental_quarterly_field_provenance` | 1306388 |

## Drift counts

The latest table does not contain a `market` column. The comparison therefore
uses the stable key available in both tables: `ticker + period_end_date`.

| Metric | Rows |
|---|---:|
| Latest rows missing matching vintage row | 42 |
| Vintage rows missing matching latest row | 0 |

All 42 latest-only rows have this latest-table run id:

```text
USA_QUARTER_UPDATE_2026-07-05__QUARTERLY
```

The missing latest-only periods range from `2026-03-31` to `2026-05-31`.

## Missing latest rows

| Ticker | Period end | Non-null financial fields | Run id | Source evidence |
|---|---:|---:|---|---|
| ACN | 2026-05-31 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| AI | 2026-04-30 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| AIHS | 2026-03-31 | 4 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| APOG | 2026-05-30 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| ATEX | 2026-03-31 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| AVAV | 2026-04-30 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| AYI | 2026-05-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| BB | 2026-05-31 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| CASY | 2026-04-30 | 6 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| CCL | 2026-05-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| CMC | 2026-05-31 | 4 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| CNVS | 2026-03-31 | 5 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| CNXC | 2026-05-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| DAKT | 2026-05-02 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| EBF | 2026-05-31 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| FUL | 2026-05-30 | 5 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| GITS | 2026-03-31 | 5 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| JBL | 2026-05-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| KFY | 2026-04-30 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| KMX | 2026-05-31 | 6 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| KR | 2026-05-23 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| LEN | 2026-05-31 | 4 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| LZB | 2026-04-25 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| MDT | 2026-04-24 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| MEI | 2026-05-02 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| MKC | 2026-05-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| MSM | 2026-05-30 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| MU | 2026-05-28 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| ORCL | 2026-05-31 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| POWW | 2026-03-31 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| PRGS | 2026-05-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| QMCO | 2026-03-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| REPL | 2026-03-31 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| SJM | 2026-04-30 | 6 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| SNOA | 2026-03-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| SNX | 2026-05-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| STZ | 2026-05-31 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| SWBI | 2026-04-30 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| VNCE | 2026-05-02 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| WGO | 2026-05-30 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| WLY | 2026-04-30 | 8 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |
| XAIR | 2026-03-31 | 7 | `USA_QUARTER_UPDATE_2026-07-05__QUARTERLY` | `SEC_RAW_EXACT` |

## Source evidence summary

| Evidence class | Rows |
|---|---:|
| `SEC_RAW_EXACT` | 42 |

Detailed source evidence:

| Source | Source run id | Raw field rows | Ticker-periods | Min retrieved at UTC | Max retrieved at UTC |
|---|---|---:|---:|---|---|
| `sec_edgar` | `USA_QUARTER_UPDATE_2026-07-05__SEC_RAW` | 577 | 42 | `2026-07-05T15:09:29+00:00` | `2026-07-05T15:10:46+00:00` |

Negative evidence:

- Exact Yahoo quarterly rows for these 42 `ticker + period_end_date` pairs: `0`.
- Exact enrichment audit rows for these 42 `ticker + period_end_date` pairs: `0`.
- Vintage field provenance rows for these 42 `ticker + period_end_date` pairs: `0`.
- Yahoo raw `status='ok'` symbol snapshots for these 42 tickers: `0`.

## Interpretation

Repo/DB evidence indicates that the latest-only drift was introduced by a
quarterly latest-table update run:

```text
USA_QUARTER_UPDATE_2026-07-05__QUARTERLY
```

The source evidence for the missing vintage rows is exact SEC raw evidence, not
Yahoo fallback evidence. The matching SEC raw source run is:

```text
USA_QUARTER_UPDATE_2026-07-05__SEC_RAW
```

Because vintage field provenance is absent for these 42 rows, the vintage side
was not populated for this run, or the vintage write path did not persist
corresponding rows for these latest updates.

## Recommendation

Recommended next step: implement or run an explicit opt-in provider-derived SEC
raw to vintage reconstruction/backfill for these 42 `ticker + period_end_date`
pairs, with a dry-run first and an apply step only after the dry-run proves:

- 42 planned vintage rows.
- Source provider `sec_edgar`.
- Source run id `USA_QUARTER_UPDATE_2026-07-05__SEC_RAW`.
- Non-null field counts align with the current latest rows.
- Field provenance would be created for all non-null reconstructed vintage
  fields.

Do not use a generic legacy-baseline copy as the first choice for this drift,
because exact SEC raw evidence exists for every missing row. A legacy-baseline
fallback should be reserved for cases where provider-derived reconstruction
cannot reproduce the latest-row values or required vintage metadata.

Follow-up: the SEC-derived read-only dry-run was implemented and run in
`docs/swingmaster_reported_vintage_sec_missing_latest_dry_run.md`. It found that
all 42 rows are blocked by reconstruction mismatches, so a guarded SEC-derived
apply is not currently safe with the existing reconstruction logic.

## Runtime/default behavior changed

No.

This investigation changed documentation only. No runtime code, tests,
migrations, provider logic, scheduler logic, or database content was changed.
