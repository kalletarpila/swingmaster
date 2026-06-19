# SwingMaster Reported Vintage Post-Backfill Verification Phase 4G3

## Scope

Phase 4G3 verifies the reported-vintage legacy baseline after Phase 4G2.

This phase was read-only against the real DB.

It did not:

- write to the real DB
- insert, update, or delete rows
- run a backfill
- call providers
- run refresh jobs or schedulers
- run TTM/scoring/valuation/percentile recalculation
- change production readers
- implement ESS integration

Target DB:

- `/home/kalle/projects/swingmaster/fundamentals_usa.db`

## Commands Run

Baseline read-only checks:

```bash
sqlite3 -readonly /home/kalle/projects/swingmaster/fundamentals_usa.db "PRAGMA integrity_check;"
```

Verification CLI:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.verify_reported_vintage_backfill \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --available-at-utc 2026-06-19T00:00:00Z \
  --sample-size 5 \
  --format json
```

Post-backfill dry-run no-op:

```bash
PYTHONPATH=. python3 -m swingmaster.cli.dry_run_reported_vintage_backfill \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --as-of-date 2026-06-19 \
  --format json \
  --legacy-availability-policy live_safe_legacy_baseline \
  --legacy-available-at-utc 2026-06-19T00:00:00Z
```

## Row Counts

Read-only baseline counts:

| Table | Count |
| --- | ---: |
| `rc_fundamental_quarterly` | `155331` |
| `rc_fundamental_quarterly_vintage` | `155331` |
| `rc_fundamental_quarterly_field_provenance` | `1306388` |
| `rc_fundamental_statement_raw` | `5204869` |
| `rc_fundamental_ttm` | `146448` |
| `rc_fundamental_valuation` | `32286` |
| `rc_fundamental_score_percentile` | `4502178` |
| `rc_fundamental_quarter_state` | `2936` |

Integrity:

- `PRAGMA integrity_check`: `ok`

## Verification Checks

The new read-only CLI opens SQLite with URI `mode=ro` and sets `PRAGMA query_only=ON`.

It verifies:

- coverage parity between latest rows and legacy baseline vintage rows
- duplicate `statement_vintage_id` count
- duplicate baseline rows per `market+ticker+period_end_date+availability_quality`
- exact null-safe financial value parity
- legacy policy metadata
- provenance aggregate count and per-vintage count sanity
- provenance policy metadata
- PIT reader behavior on deterministic samples

An initial provenance mismatch SQL shape was too slow on the full real DB and was interrupted. The final CLI uses aggregate CTEs and completed successfully.

## Coverage Parity Result

| Field | Value |
| --- | ---: |
| latest count | `155331` |
| baseline vintage count | `155331` |
| missing latest rows | `0` |
| extra vintage rows | `0` |
| result | `OK` |

Duplicate checks:

| Check | Count |
| --- | ---: |
| duplicate `statement_vintage_id` | `0` |
| duplicate baseline ticker/period rows | `0` |

## Value Parity Result

Comparison used exact SQLite null-safe `IS NOT` checks.

Total value mismatches:

- `0`

Fields checked:

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

Each field had `0` mismatches.

## Metadata Policy Result

All `155331` legacy vintage rows match the intended policy:

- `source_provider = UNKNOWN_LEGACY`
- `availability_quality = LEGACY_BASELINE_AVAILABLE_FROM_BACKFILL`
- `available_at_utc = 2026-06-19T00:00:00Z`
- `revision_number = 1`
- `is_restated = 0`
- `supersedes_vintage_id IS NULL`

Metadata mismatch rows:

- `0`

## Provenance Sanity Result

| Field | Value |
| --- | ---: |
| total provenance rows | `1306388` |
| expected provenance rows | `1306388` |
| vintages with provenance count mismatch | `0` |
| provenance metadata mismatch rows | `0` |
| result | `OK` |

Provenance metadata policy:

- `source_provider = UNKNOWN_LEGACY`
- `provenance_role = LEGACY_BASELINE`
- `merge_action = LEGACY_BACKFILL_BASELINE`

## PIT Sample Result

The sample set used the first five and latest five deterministic vintage rows.

All samples passed:

- row exists at `decision_cutoff_utc = 2026-06-19T00:00:00Z`
- row does not exist at `decision_cutoff_utc = 2026-06-18T23:59:59Z`
- provenance rows exist

Examples:

| Ticker | Period | Provenance rows | At cutoff | Before cutoff |
| --- | --- | ---: | --- | --- |
| `A` | `2006-10-31` | `1` | yes | no |
| `A` | `2007-10-31` | `7` | yes | no |
| `A` | `2008-10-31` | `8` | yes | no |
| `ZYME` | `2026-03-31` | `7` | yes | no |
| `ZYME` | `2025-12-31` | `10` | yes | no |

## Post-Backfill Dry-Run No-Op Result

The live-safe dry-run after verification returned:

| Field | Value |
| --- | ---: |
| `overall_status` | `DRY_RUN_READY` |
| `total_latest_rows` | `155331` |
| `candidate_rows` | `0` |
| `planned_vintage_rows` | `0` |
| `planned_provenance_rows` | `0` |
| `already_has_vintage_rows` | `155331` |
| `skipped_rows` | `155331` |
| `blocked_rows` | `0` |
| `requires_policy_decision_rows` | `0` |

## Conclusion

The Phase 4G2 legacy vintage baseline is mechanically loaded and read-model ready as a baseline table.

Every current latest quarterly row has exactly one matching live-safe legacy vintage row.

Vintage values match latest-table values exactly for the checked fields.

Provenance count and metadata match the intended legacy baseline policy.

PIT reader behavior works for deterministic samples under the live-safe timestamp policy.

This does not mean the legacy baseline has true historical publication timing. The baseline remains live/forward-safe from `2026-06-19T00:00:00Z`, while externally verified release dates remain the better target for historical PIT quality.

## Recommended Next Phase

Recommended next phase:

```text
Phase 4H1: reported-vintage read integration planning, no production wiring yet
```

Do not add provider integration, ESS integration, or production dual-write wiring until read integration behavior is explicitly planned and reviewed.

Phase 4H1 production write-path design is documented in [Reported Vintage Production Write Path Design Phase 4H1](swingmaster_reported_vintage_production_write_path_design_phase4h1.md). It keeps provider paths unwired and recommends a small opt-in/test-only builder-level dual-write phase next.
