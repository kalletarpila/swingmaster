# Fundamentals V3 Latest8Q Downstream Gap Analysis

Phase 8F was an all-field full-closure analysis. Phase 8G applies the narrower downstream-critical policy: secondary gaps are informational unless required to derive a primary downstream field.

## Executive Dashboard

| Metric | Clean / Available | Total | % |
| --- | ---: | ---: | ---: |
| Downstream-clean latest8Q tickers | 701 | 2470 | 28.3806 |
| Latest4Q downstream-clean | 1438 | 2470 | 58.2186 |
| Latest quarter downstream-clean | 1599 | 2470 | 64.7368 |
| Current TTM clean | 2208 | 2470 | 89.3927 |
| Score available | 2038 | 2470 | 82.5101 |
| Lifecycle available | 1764 | 2470 | 71.417 |
| Valuation available | 1729 | 2470 | 70.0 |
| External research still needed | 1689 | 2470 | 68.3806 |
| Structural review still needed | 482 | 2470 | 19.5142 |

## Local Repair Results

- final verification artifact: `temp/fundamentals_v3_phase8g_local_latest8q_repairs/20260829T_PHASE8G_FINAL`
- production apply passes: `5`
- cumulative local candidates analyzed: `277`
- cumulative local evidence sufficient: `277`
- cumulative local repair groups: `277`
- cumulative repaired rows: `277`
- cumulative repaired tickers: `92`
- failed groups: `0`
- final local candidates remaining: `0`

## Remaining External Work

- old Phase 8F external facts: `6064`
- new downstream-critical external facts: `4413`
- reduction %: `27.2263`

## Remaining Structural Work

- old Phase 8F structural decisions: `1130`
- new material structural decisions: `1095`
- structural tickers: `482`

## Full Downstream Closure

- already clean: `701`
- clean after local repair: `701`
- require external evidence: `1689`
- require structural decision: `482`
- NO_MISSING_REQUIREMENT: `0`
- theoretical downstream-clean tickers: `2470`
- theoretical downstream-clean %: `100.0`

## Classification

`LATEST8Q_LOCAL_CRITICAL_REPAIRS_COMPLETE_WITH_EXTERNAL_STRUCTURAL_WORK_REMAINING`

## Next Action

USE THE NEW MINIMAL DOWNSTREAM-CRITICAL EXTERNAL RESEARCH QUEUE NEXT; DO NOT RESEARCH SECONDARY FIELDS THAT DO NOT AFFECT TTM / SCORE / LIFECYCLE / VALUATION
