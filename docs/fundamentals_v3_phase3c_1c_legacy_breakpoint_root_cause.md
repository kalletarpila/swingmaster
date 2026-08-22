# Fundamentals V3 Phase 3C-1C Legacy Breakpoint Root Cause

Phase 3C-1C re-ran Legacy backward validation under the new permanent V3 historical floor:

```text
V3_HISTORICAL_PERIOD_END_FLOOR = 2018-01-01
```

Rows before 2018-01-01 are excluded from V3 deep-history import and no longer block a valid
2018+ chain. The old 1999 scope was retired because the business goal is robust recent historical
coverage, not maximum archive depth.

## Scope Impact

| Metric | Count |
| --- | ---: |
| Total Legacy rows | 156094 |
| Legacy rows before 2018 | 64788 |
| Legacy rows 2018+ | 91306 |
| Existing canonical overlap 2018+ | 10460 |
| Legacy-only rows 2018+ | 67477 |
| Companies represented 2018+ in Legacy | 2935 |
| V3 companies | 2552 |
| Companies affected by pre-2018 exclusion | 2263 |
| Deep-history candidate reduction vs 1999+ | 54671 |

## Accounting

Anchor accounting is now mutually exclusive across the full 2552-company V3 universe:

| Anchor category | Companies |
| --- | ---: |
| `ANCHOR_2026` | 1136 |
| `ANCHOR_2025` | 289 |
| `ANCHOR_2024_OR_OLDER_BUT_2018_PLUS` | 2 |
| `NO_RELIABLE_ANCHOR` | 1071 |
| `NO_LEGACY_2018_PLUS_HISTORY` | 54 |
| Total | 2552 |

Company status accounting:

| Status | Companies |
| --- | ---: |
| `FULL_OR_PARTIAL_VALID_CHAIN` | 132 |
| `BREAKPOINT_WITH_VALID_PREFIX` | 48 |
| `NO_RELIABLE_ANCHOR` | 1072 |
| `NO_LEGACY_2018_PLUS_HISTORY` | 1300 |
| Total | 2552 |

The status categories intentionally measure 2018+ Legacy-only importability, not the presence of
existing canonical V3 quarters.

## Root Cause

Phase 3C-1B's low READY count came from two causes:

1. The 1999 floor made the denominator much larger than the business requirement.
2. The original walk treated one-quarter missing representations and floor-adjacent history too
   conservatively.

Under 2018+ scope the corrected validator produced:

| Disposition | Rows |
| --- | ---: |
| `READY_DIRECT_CHAIN` | 1769 |
| `READY_BRIDGED_CHAIN` | 117 |
| `READY_V2_CORROBORATED` | 2435 |
| `HOLD_BEHIND_BREAKPOINT` | 61093 |
| `HOLD_TRUE_FISCAL_TRANSITION` | 775 |
| `HOLD_MAPPING_CONFLICT` | 0 |
| `HOLD_DUPLICATE_OR_AMBIGUOUS` | 0 |
| `HOLD_INSUFFICIENT_EVIDENCE` | 1288 |
| Total | 67477 |

READY total: 4321 rows. HOLD total: 63156 rows.

## Bridge Rules

Phase 3C-1C allows only a narrow diagnostic bridge:

- one missing quarter only
- no competing Legacy row between current and candidate period
- spacing compatible with one missing quarter
- no unresolved mapping conflict
- no synthetic Q creation

Bridge outcome is `READY_BRIDGED_CHAIN`; it validates the older observed row only. It does not
create the missing Q4.

Remaining breakpoints:

| Reason | Count |
| --- | ---: |
| `MULTI_QUARTER_DATA_GAP` | 630 |
| `PERIOD_END_TRUE_BREAK` | 665 |

Bridgeability:

| Classification | Meaning |
| --- | --- |
| `BRIDGEABLE` | Safe one-quarter representation gap handled as `READY_BRIDGED_CHAIN`. |
| `REQUIRES_3C2B_REVIEW` | Remaining breakpoint cannot be crossed automatically. |

No true mapping-conflict population dominates the remaining 2018+ HOLD set; the main blocker is
sparse or unanchored Legacy history.

## Period Calibration

Confirmed 2025-2026 overlap spacing:

| Metric | Days |
| --- | ---: |
| Median | 92 |
| P5 | 90 |
| P25 | 91 |
| P75 | 92 |
| P95 | 183 |

Breakpoint spacing:

| Bucket | Count |
| --- | ---: |
| `>150` | 1260 |
| `<70 days` | 35 |

The corrected validator accepts ordinary quarterly spacing, 52/53-week variants, small provider
date variants, and explicit one-quarter bridges only when deterministic.

## Overlap Recalibration

Existing-Q overlap remains strong:

| Metric | Count |
| --- | ---: |
| Overlap candidates | 10460 |
| Prior conflict rows | 5799 |
| Same-Q after corrected logic | 5315 |
| Possible wrong mapping | 7 |
| Clear wrong mapping | 197 |
| Likely same-quarter | 91.65% |
| True mapping risk | 3.52% |

Recent anchors:

| Fiscal year | Overlap | Same-Q confirmed | Confirmed % | Revenue <=5% | Period compatible |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2026 | 2959 | 1205 | 40.72% | 2589 | 2959 |
| 2025 | 7114 | 2987 | 41.99% | 6549 | 7114 |

Conclusion: recent anchors remain strong enough for a deterministic READY-only Phase 3C-2 import.

## Reliability 2018-2026

| Year | Candidate rows | READY direct | READY bridged | READY V2 | HOLD | READY % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026 | 516 | 0 | 0 | 0 | 516 | 0.00% |
| 2025 | 2659 | 133 | 89 | 27 | 2410 | 9.36% |
| 2024 | 9619 | 174 | 27 | 444 | 8974 | 6.71% |
| 2023 | 9981 | 182 | 0 | 462 | 9337 | 6.45% |
| 2022 | 9849 | 167 | 0 | 454 | 9228 | 6.31% |
| 2021 | 9639 | 163 | 0 | 438 | 9038 | 6.24% |
| 2020 | 9052 | 144 | 0 | 420 | 8488 | 6.23% |
| 2019 | 8308 | 329 | 1 | 190 | 7788 | 6.26% |
| 2018 | 7854 | 477 | 0 | 0 | 7377 | 6.07% |

The curve is stable after the 2018 floor. There is no pandemic-era 2020/2021-specific degradation;
READY percentages stay near 6.2%.

## Phase 3C-2 Gate

Phase 3C-2 may import only rows in `phase3c2_dry_import_plan.csv`:

- `period_end_date >= 2018-01-01`
- disposition is `READY_DIRECT_CHAIN`, `READY_BRIDGED_CHAIN`, or `READY_V2_CORROBORATED`
- no unresolved competing row
- no sequence violation
- no production write was made during this diagnostic

Expected contribution:

| Metric | Value |
| --- | ---: |
| New canonical Q candidates | 4321 |
| Companies gaining history | 180 |
| Inserted field values | 40326 |
| Core-field values | 18022 |
| Non-core values | 22304 |
| Publication dates available | 3878 |
| Oldest imported year | 2018 |
| Median quarters per READY company | 29.0 |
| READY sequence violations | 0 |

Phase 3C-2B is likely needed after the direct import because 63156 HOLD rows remain. This does not
block importing the deterministic READY subset.

## Special Cases

| Ticker | READY | HOLD | Notes |
| --- | ---: | ---: | --- |
| CAVA | 0 | 18 | Preserved; 2018+ Legacy-only history remains held. |
| NEUP | 1 | 7 | Preserved; one READY row remains deterministic. |
| LFCR | 0 | 33 | Preserved; transition/provider-period issues stay held. |
| BNC | 0 | 32 | Preserved; transition structure stays held. |
| SJM | 0 | 29 | Preserved; history remains held. |
| LYTS | 0 | 29 | Preserved; history remains held. |

## Safety

| Check | Result |
| --- | --- |
| V3 writes | 0 |
| Legacy writes | 0 |
| V2 writes | 0 |
| Provider/network calls | 0 |
| `PRAGMA quick_check` | `ok` |
| `PRAGMA foreign_key_check` rows | 0 |

Artifacts:

`temp/fundamentals_v3_phase3c_1c_legacy_breakpoint_diagnostic/20260822T_PHASE3C_1C_LEGACY_BREAKPOINT_DIAGNOSTIC/`

Final classification:

`FUNDAMENTALS_V3_PHASE3C_1C_LEGACY_BREAKPOINT_DIAGNOSTIC_COMPLETE_READY_FOR_3C2`

Recommended next phase:

`MASTER PLAN PHASE 3C-2 - LEGACY DEEP-HISTORY EXTENSION`
