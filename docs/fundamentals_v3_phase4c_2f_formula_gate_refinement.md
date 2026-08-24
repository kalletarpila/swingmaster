# Fundamentals V3 Phase 4C-2F Formula Gate Refinement

Classification: `FUNDAMENTALS_V3_PHASE4C2F_GATE_REFINEMENT_COMPLETE_READY_FOR_PRODUCTION_APPLY`

Canonical financial writes: `0`

Metadata writes: `0`

## Evidence Architecture

The refined model separates semantic confidence, statistical confidence, and applicability. SEMANTIC_D/E, proxies, adjusted EBITDA, and InterestPaid are not auto-approved.

## Recovery

- Current strict: EBIT 0, EBITDA 444, uplift 444
- Phase 4C-2E counterfactual: EBIT 1, EBITDA 632, uplift 632
- Final refined: EBIT 104, EBITDA 484, uplift 484

## Backtest

- EBIT hidden observations predicted: 20
- EBIT <=1%: 20
- EBIT >5%: 0
- EBITDA/D&A hidden observations predicted: 47
- EBITDA/D&A <=1%: 47
- EBITDA/D&A >5%: 0

## Production Plan

The plan is dry-run only and includes no conditional or proxy rows. Q4 remains independently validated.

Next: `MASTER PLAN PHASE 4C-3 - EBIT & EBITDA PRODUCTION APPLY`
