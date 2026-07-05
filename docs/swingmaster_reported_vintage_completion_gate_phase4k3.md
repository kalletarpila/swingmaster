# Reported Vintage Completion Gate Phase 4K3

Phase 4K3 adds a default-off decision gate for classifying whether a `sec_latest_writer` quarter_update vintage run is complete after Yahoo fallback has run.

## Purpose

Phase 4K1 writes a SEC latest-writer-aligned vintage before Yahoo fallback enrichment. Phase 4K2 added read-only guards that detect whether Yahoo fallback later inserted rows, filled fields, or left latest/vintage parity drift.

Phase 4K3 converts those guard summaries into an explicit completion decision. It does not write final mixed rows, Yahoo rows, provider data, or real DB data.

## Why SEC Vintage Alone Is Not Always Sufficient

SEC-only vintage is sufficient only if the post-run latest row still matches the SEC vintage and Yahoo did not affect the row.

Repo evidence indicates Yahoo fallback can:

- insert a missing quarter from Yahoo quarterly staging
- fill NULL fields in an existing SEC-backed latest row
- record filled fields in `rc_fundamental_quarterly_enrichment_audit` with `FILLED_FROM_YAHOO`

When Yahoo fills fields after the SEC vintage side-write, the final latest row has mixed SEC-retained and Yahoo-filled provenance. A SEC-only vintage should then be treated as incomplete for that final latest state.

## Decision Statuses

The helper `classify_quarter_update_vintage_completion(...)` returns:

- `SEC_VINTAGE_SUFFICIENT`: no Yahoo impact and post-run parity is OK
- `FINAL_MIXED_REQUIRED`: Yahoo filled fields or Yahoo audit explains a value mismatch
- `YAHOO_VINTAGE_REQUIRED`: Yahoo inserted a missing quarter that is not represented by SEC vintage
- `BLOCKED_POST_RUN_DRIFT`: latest/vintage parity drift, duplicates, or unexplained mismatch requires investigation
- `UNKNOWN`: required run linkage or guard data is missing

## Next Action Mapping

- `SEC_VINTAGE_SUFFICIENT` -> `NONE`
- `FINAL_MIXED_REQUIRED` -> `CREATE_FINAL_MIXED_VINTAGE`
- `YAHOO_VINTAGE_REQUIRED` -> `CREATE_YAHOO_OR_FINAL_MIXED_VINTAGE`
- `BLOCKED_POST_RUN_DRIFT` -> `INVESTIGATE_DRIFT`
- `UNKNOWN` -> `IMPROVE_RUN_LINKAGE`

## Summary Fields

When `--write-vintage --vintage-mode sec_latest_writer` is explicitly enabled, quarter_update can surface:

- `vintage_completion_status`
- `vintage_completion_reason`
- `vintage_next_required_action`
- `vintage_sec_only_sufficient`
- `vintage_final_mixed_required`
- `vintage_yahoo_vintage_required`
- `vintage_blocked_post_run_drift`

Default behavior without vintage flags is unchanged and these fields are omitted.

## Yahoo Fallback Interpretation

Yahoo field fills on SEC-backed rows require final mixed treatment because the final row contains both SEC-retained and Yahoo-filled lineage.

Yahoo missing-quarter inserts require Yahoo-aware vintage handling, or final mixed handling if later source evidence shows mixed provenance.

Unexplained value mismatches, duplicate vintage identifiers, or inconsistent vintage/latest state are treated as blocked drift, not as automatically fixable Yahoo cases.

## Verification

Temp-DB/mocked tests cover:

- SEC vintage sufficient
- latest without vintage blocks drift
- Yahoo-filled fields require final mixed
- Yahoo inserted missing quarter requires Yahoo-aware vintage
- unknown run linkage returns `UNKNOWN`
- value mismatch with Yahoo audit requires final mixed
- value mismatch without explanation blocks drift
- quarter_update summary surfaces completion fields only in explicit `sec_latest_writer` mode
- default no-vintage behavior remains unchanged

Real DB/provider status: not run.

## Recommended Next Phase

Phase 4K4 should implement a default-off final mixed or post-run apply path that runs after Yahoo fallback and writes one final PIT vintage for the stable latest row. It should remain temp-tested first and should not enable production writes until real DB preflight confirms safe inputs.
