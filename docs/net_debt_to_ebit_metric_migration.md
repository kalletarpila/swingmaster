# Net Debt To EBIT Metric Migration

Date: 2026-08-06

Runtime artifacts:

```text
temp/net_debt_to_ebit_migration/20260806T_dry_run_v2/
temp/net_debt_to_ebit_migration/20260806T_apply/
temp/net_debt_to_ebit_migration/20260806T_apply_idempotency/
temp/net_debt_to_ebit_migration/20260806T_apply_v2/
```

Backup:

```text
temp/net_debt_to_ebit_migration/20260806T_apply/backups/fundamentals_usa.db.pre_net_debt_to_ebit.bak
```

## Raw External Fields

The fundamentals database stores externally fetched raw statement observations in `rc_fundamental_statement_raw`:

- `ticker`
- `statement_type`
- `period_end_date`
- `period_type`
- `field_name`
- `field_value`
- `currency`
- `source`
- `retrieved_at_utc`
- `run_id`

The normalized latest-state quarterly table `rc_fundamental_quarterly` stores source-derived quarterly values:

- `revenue`
- `gross_profit`
- `operating_income`
- `ebit`
- `ebitda`
- `net_income`
- `operating_cashflow`
- `capex`
- `free_cashflow`
- `cash`
- `total_debt`
- `shares_outstanding`
- `currency`
- `run_id`

`net_debt`, `net_debt_to_ebitda`, and `net_debt_to_ebit` are not raw external fields. They are TTM-layer derived metrics in `rc_fundamental_ttm`.

Observed raw source field families in `fundamentals_usa.db` on 2026-08-06:

| Statement type | Raw field families |
| --- | --- |
| `income` | `Gross Profit`, `GrossProfit`, `Net Income`, `NetIncomeLoss`, `Operating Income`, `OperatingIncomeLoss`, `RevenueFromContractWithCustomerExcludingAssessedTax`, `Revenues`, `Total Revenue` |
| `cashflow` | `Capital Expenditure`, `NetCashProvidedByUsedInOperatingActivities`, `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations`, `Operating Cash Flow`, `PaymentsToAcquireProductiveAssets`, `PaymentsToAcquirePropertyPlantAndEquipment` |
| `balance` | `AssetsCurrent`, `Cash And Cash Equivalents`, `CashAndCashEquivalentsAtCarryingValue`, `CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents`, `LiabilitiesCurrent`, `LongTermDebtCurrent`, `LongTermDebtNoncurrent`, `Ordinary Shares Number`, `ShortTermBorrowings`, `Total Debt`, `WeightedAverageNumberOfDilutedSharesOutstanding`, `WeightedAverageNumberOfSharesOutstandingBasic` |

## Old Behavior

Before this migration, active TTM code wrote `net_debt_to_ebitda` but computed it with this precedence:

1. Use TTM EBITDA when available.
2. Fall back to TTM EBIT when EBITDA was missing.
3. Leave the ratio null when net debt was unavailable or the denominator was null or zero.

Scoring read the stored `net_debt_to_ebitda` value. When the value was null, the leverage component defaulted to `8`.

In `fundamentals_usa.db`, the old stored ratio matched the deterministic EBIT-based calculation for all `147124` TTM rows, so active data was already semantically net debt to EBIT despite the old column name.

## New Behavior

Active code now uses `net_debt_to_ebit`:

```text
net_debt = total_debt - cash
net_debt_to_ebit = net_debt / ebit_ttm
```

The result is null when `cash`, `total_debt`, or `ebit_ttm` is missing, or when `ebit_ttm` is zero. Negative EBIT remains a numeric denominator, preserving the existing score behavior where ratios `<= 0` receive the best leverage component.

The deprecated `net_debt_to_ebitda` column remains in the schema for compatibility and historical auditability, but new active TTM writes populate `net_debt_to_ebit`.

## Production Database Result

Applied to `fundamentals_usa.db`:

- total TTM rows: `147124`
- non-null `net_debt_to_ebit`: `62866`
- null `net_debt_to_ebit`: `84258`
- rows where old stored value differed from new EBIT-based value: `0`
- rows likely calculated with actual EBITDA: `0`
- leverage component changes: `0`
- total score changes: `0`
- lifecycle weighted score changes: `0`
- percentile source value changes: `0`
- valuation rebuild required: `False`
- percentile rebuild required: `False`
- `PRAGMA quick_check`: `ok`

The apply populated the new column without changing row counts, effective-date metadata, or the deprecated metric column.

## Threshold Decision

Thresholds were kept unchanged because the real stored distribution did not change:

- `NULL` -> `8`
- `<= 0` -> `15`
- `<= 1` -> `12`
- `<= 2` -> `8`
- `<= 3` -> `4`
- `> 3` -> `0`
