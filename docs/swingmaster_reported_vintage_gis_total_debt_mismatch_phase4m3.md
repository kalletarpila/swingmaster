# SwingMaster GIS Total Debt Mismatch Phase 4M3

Date: 2026-07-07

## Purpose

Phase 4M3 diagnoses the remaining read-only mismatch:

- ticker: `GIS`
- period_end_date: `2025-05-25`
- field: `total_debt`

No provider calls, scheduler runs, refresh jobs, backfills, recovery apply, Yahoo-aware apply, or real DB writes were run.

## Command

```bash
python3 -m swingmaster.cli.diagnose_reported_value_mismatch \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --ticker GIS \
  --period-end-date 2025-05-25 \
  --field total_debt \
  --format json \
  --sample-limit 50
```

The diagnostic CLI opens the DB using SQLite URI `mode=ro` and sets `PRAGMA query_only=ON`.

## Latest Row

Latest row:

- run_id: `USA_QUARTER_UPDATE_2026-07-07__QUARTERLY__QUARTERLY`
- `total_debt=14878600000.0`
- `cash=363900000.0`
- non-null reported field count: `11`

The latest value is supported by SEC raw component facts for `2025-05-25`:

- `LongTermDebtCurrent=1528400000.0`
- `LongTermDebtNoncurrent=12673200000.0`
- `ShortTermBorrowings=677000000.0`
- component sum: `14878600000.0`

This exactly matches the latest `total_debt`.

## Visible Vintage

Visible/current vintage:

- statement_vintage_id: `legacy:usa:GIS:2025-05-25:9441b8313c7894e3`
- source_provider: `UNKNOWN_LEGACY`
- run_id: `reported-vintage-legacy-backfill:usa:2026-06-19:2026-06-19T14:28:43Z`
- available_at_utc: `2026-06-19T00:00:00Z`
- ingested_at_utc: `2026-06-19T14:28:43Z`
- revision_number: `1`
- `total_debt=677000000.0`

The field provenance for `total_debt` is legacy baseline provenance:

- source_provider: `UNKNOWN_LEGACY`
- source_table: `rc_fundamental_quarterly`
- source_row_ref: `usa:GIS:2025-05-25`
- merge_action: `LEGACY_BACKFILL_BASELINE`
- provenance_role: `LEGACY_BASELINE`

The visible vintage value equals the SEC `ShortTermBorrowings` component only.

## SEC Evidence

SEC raw contains both:

- the single short-term borrowing component: `ShortTermBorrowings=677000000.0`
- the additional debt components needed for total debt:
  - `LongTermDebtCurrent=1528400000.0`
  - `LongTermDebtNoncurrent=12673200000.0`

Therefore the mismatch is not an unsupported latest value. It is a debt component policy difference between:

- legacy baseline vintage: short-term borrowings only
- current latest reconstruction: current debt + noncurrent debt + short-term borrowings

No strong period-mapping issue was found for the latest value. The SEC component rows are tied to `period_end_date=2025-05-25`.

## Yahoo Evidence

Yahoo quarterly has a nearby row:

- period_end_date: `2025-05-31`
- `total_debt=15296700000.0`
- run_id: `USA_YAHOO_QTR_20260505_NET`
- source_run_id: `USA_YAHOO_RAW_20260505_NET__RAW__B0012`

Yahoo therefore broadly supports a large total debt value, but it is not exact proof for this PIT/vintage row because:

- the Yahoo period is `2025-05-31`, not `2025-05-25`
- there is no enrichment audit row for `GIS:2025-05-25:total_debt`

## Diagnosis

Diagnostic status:

- `DEBT_COMPONENT_POLICY_DIFF`

Confidence:

- `high`

Reason:

- `latest_total_debt_equals_sec_component_sum_but_visible_vintage_equals_single_debt_component`

The latest value `14878600000.0` is likely the correct current normalized `total_debt` under the repository's current SEC reconstruction policy.

The visible vintage value `677000000.0` is a stale legacy baseline value that captured only one debt component.

## Recommended Next Action

Do not run Yahoo-aware apply or recovery for this case.

The proper next fix is a narrow provider-derived vintage/supersession path for this exact GIS row, after explicit review. The fix should preserve PIT semantics and should not broaden Yahoo-aware/final-mixed apply.

Before writing anything, implement a dry-run-only phase that proposes a new SEC-derived vintage for `GIS:2025-05-25` and shows:

- candidate `statement_vintage_id`
- candidate `total_debt=14878600000.0`
- provenance rows for `LongTermDebtCurrent`, `LongTermDebtNoncurrent`, and `ShortTermBorrowings`
- the legacy vintage it would supersede
- duplicate and parity checks

## Phase 4M4 Follow-Up

Phase 4M4 implemented the requested read-only dry-run:

- doc: `docs/swingmaster_reported_vintage_gis_provider_vintage_dry_run_phase4m4.md`
- candidate statement_vintage_id: `sec_edgar:usa:GIS:2025-05-25:dc65e8c864b43e37`
- candidate `total_debt=14878600000.0`
- visible legacy `total_debt=677000000.0`
- duplicate candidate count: `0`
- planned_vintage_rows: `1`
- planned_provenance_rows: `11`
- status: `DRY_RUN_READY_WITH_UNKNOWN_PROVENANCE`

The dry-run confirms the row/value fix is feasible, but it should not be written yet because `total_debt` still has unknown provenance in the candidate. The next phase should first make derived total debt provenance explicit from the SEC debt components and require `DRY_RUN_READY` before any write/apply phase.

## Phase 4M5 Follow-Up

Phase 4M5 added SEC component-derived provenance for latest-writer `total_debt`. The GIS dry-run now returns:

- status: `DRY_RUN_READY`
- unknown_provenance_count: `0`
- candidate statement_vintage_id: `sec_edgar:usa:GIS:2025-05-25:9a81f59e8511cac0`
- candidate `total_debt=14878600000.0`

The next phase can be a separate guarded one-row apply for this GIS provider-derived vintage, with backup and post-verify. No broad Yahoo-aware apply or recovery is indicated.
