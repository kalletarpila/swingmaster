# SwingMaster Reported Vintage Single Ticker Final Mixed Preflight Phase 4J1

Phase 4J1 adds a read-only single-ticker preflight for future final mixed vintage execution.

## Purpose

The goal is to check whether a final mixed input package can be built from existing DB rows before any real final mixed write is attempted.

This phase does not write the real DB and does not call providers.

## Command Run

```bash
PYTHONPATH=. python3 -m swingmaster.cli.preflight_final_mixed_single_ticker \
  --fundamentals-db /home/kalle/projects/swingmaster/fundamentals_usa.db \
  --market usa \
  --ticker A \
  --as-of-date 2026-06-19 \
  --available-at-utc 2026-06-19T00:00:00Z \
  --format json
```

The CLI opens SQLite with `mode=ro` and sets `PRAGMA query_only=ON`.

## Real DB Read-Only Status

Read-only smoke completed successfully. The returned result included:

```json
{
  "query_only": 1,
  "ticker": "A",
  "period_end_date": "2026-04-30",
  "status": "INPUTS_INCOMPLETE_FOR_TRUE_FINAL_MIXED"
}
```

## Selected Ticker And Period

- ticker: `A`
- market: `usa`
- period: `2026-04-30`
- legacy statement vintage id: `legacy:usa:A:2026-04-30:8ea0110ed6b3069a`

The preflight built an in-memory candidate:

- source hash: `d9a626ac2e505c294728cd967a7c10e58562e49e2a8343bfca0c3ef30cf5c5a1`
- statement vintage id: `mixed_sec_yahoo:usa:A:2026-04-30:d9a626ac2e505c29`
- provenance rows: `8`
- provenance field count: `8`

## Classification

The candidate is not classified as a true provider-derived final mixed update.

Classification:

```text
INPUTS_INCOMPLETE_FOR_TRUE_FINAL_MIXED
```

Reason: the available data is legacy baseline provenance only. No Yahoo fallback audit or mixed SEC/Yahoo provider-derived update was present in the preflight result.

## Row Counts Unchanged

Counts before and after the real DB smoke were identical:

```text
rc_fundamental_quarterly                  155373
rc_fundamental_quarterly_vintage          155331
rc_fundamental_quarterly_field_provenance 1306388
```

No rows were inserted, updated, or deleted.

## Recommendation

Do not run a guarded real final mixed write yet based only on the legacy baseline.

Recommended next phase: implement provider-derived final mixed input extraction or a stricter real-run preflight that can prove SEC/Yahoo fallback provenance is present for the selected ticker/period before any write.
