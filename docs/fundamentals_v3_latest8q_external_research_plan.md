# Fundamentals V3 Latest8Q External Research Plan

Phase 8H packages the post-8G external queue for official-source research. It does not browse, edit canonical data, rebuild downstream tables, or write RawCandle.

## Downstream-Critical Policy

Research requests are limited to fiscal identity, genuine missing quarters, official period_end, first-public publish_date, Revenue, EBIT, FCF, Cash, Total Debt, Shares Outstanding, and approved inputs needed to derive those fields.

Gross Profit, EBITDA, Net Income, Operating Income, OCF, and Capex are excluded when they are only secondary completeness gaps. OCF/Capex are retained only when needed to derive missing FCF; Operating Income is retained only as part of an approved EBIT derivation requirement.

## Package Counts

- starting Phase 8G external queue rows: `4413`
- raw normalized dependency facts before deduplication: `9820`
- normalized deduplicated critical facts: `9491`
- duplicate facts removed: `329`
- research tasks: `4413`
- external tickers: `1689`
- average facts/task: `2.1507`

## Waves

- Wave 1 P1_CURRENT: `1066` tasks / `810` tickers / `1857` facts
- Wave 2 P2_LATEST4Q: `370` tasks / `181` tickers / `548` facts
- Wave 3 P3_LATEST8Q: `2977` tasks / `1336` tickers / `7086` facts

## Top Evidence Needs

- `TOTAL_DEBT`: `1817`
- `SOURCE_SEMANTICS_CONFIRMATION`: `1549`
- `MISSING_QUARTER_EXISTENCE`: `1259`
- `FIRST_PUBLIC_PUBLISH_DATE`: `1063`
- `EBIT_DIRECT`: `886`
- `REVENUE`: `466`
- `FCF_DIRECT`: `463`
- `CAPEX_FOR_FCF`: `424`
- `OFFICIAL_FY_FQ_IDENTITY`: `329`
- `CASH`: `252`
- `LINEAGE_OWNERSHIP_EVIDENCE`: `226`
- `TARGET_COLLISION_EVIDENCE`: `225`
- `OFFICIAL_PERIOD_END`: `214`
- `SHARES_OUTSTANDING`: `121`
- `FISCAL_TRANSITION_EVIDENCE`: `118`

## First Batch

First batch uses deterministic impact score `>=175`, emphasizing current TTM, latest-quarter impact, number of downstream layers, and number of required facts. It contains `296` tasks / `210` tickers / `671` facts.

## Structural Separation

Structural decisions remain separate in `latest8q_structural_decisions_remaining.csv`. Mixed external+structural tickers are flagged in the ticker-level package and must not be treated as simple external-only repairs.

## Closure

- ALREADY_CLEAN: `701`
- YES_EXTERNAL_ONLY: `1287`
- YES_EXTERNAL_PLUS_STRUCTURAL: `402`
- YES_STRUCTURAL_ONLY: `80`
- NO_MISSING_REQUIREMENT: `0`

## Classification

`LATEST8Q_EXTERNAL_RESEARCH_PACKAGE_READY_WITH_STRUCTURAL_DEPENDENCIES`

## Next Action

RUN WAVE 1 EXTERNAL RESEARCH FIRST WHILE KEEPING STRUCTURAL DECISIONS SEPARATE; USE NEW EVIDENCE TO REDUCE THE STRUCTURAL QUEUE BEFORE MANUAL REVIEW

## Phase 8H-1 - Wave 2 / Wave 3 Exact-Anchor Cleanup

Phase 8H-1 cleans only Wave 2 and Wave 3. Wave 1 is not modified.

Historical exact FY anchors and local calendar-type FQ resolution must be exhausted before requesting external FY/FQ verification. OFFICIAL_FY_FQ_IDENTITY and identity-only SOURCE_SEMANTICS_CONFIRMATION requests are removed when exact anchors, period_end, fiscal slot logic, and sequence context resolve the quarter locally.

### Results

- Wave 2: `370` tasks -> `369` tasks, `548` facts -> `533` facts
- Wave 3: `2977` tasks -> `2695` tasks, `7086` facts -> `6073` facts
- combined facts removed: `1028`
- remaining facts: `6606`
- tickers no longer needing external research: `94`
- remaining external tickers: `1264`

Structural queue remains separate. Structural cases unchanged `1095`, simplified by locally resolved FY/FQ `0`.

Closure test: COMPLETE_CLOSURE_PATH `2470`, MISSING_REQUIREMENT `0`.

Safety: production writes `0`, network calls `0`, RawCandle writes `0`.

Classification: `WAVE23_EXTERNAL_QUEUE_CLEANUP_COMPLETE_WITH_STRUCTURAL_DEPENDENCIES`

Next action: USE THE CLEANED WAVE 2 / WAVE 3 FILES FOR FUTURE EXTERNAL RESEARCH; DO NOT REQUEST FY/FQ CONFIRMATION WHERE HISTORICAL EXACT ANCHORS ALREADY RESOLVE THE IDENTITY
