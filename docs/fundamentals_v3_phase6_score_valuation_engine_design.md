# Fundamentals V3 Phase 6 Score & Valuation Engine Design

Classification: `FUNDAMENTALS_V3_PHASE6_SCORE_VALUATION_DESIGN_COMPLETE_READY_FOR_PRODUCTION_IMPLEMENTATION`

Phase 6 locks an EBIT-first downstream contract without writing production score, valuation, TTM or canonical rows.

## Existing Downstream

The existing V2 score path is EBITDA-first for margin, margin trend, consistency and leverage fallback. The existing V2 valuation path computes both EV/EBIT and EV/EBITDA, but bucket selection prefers EV/EBITDA before falling back to EV/EBIT.

## Locked Policy

Primary fields: `revenue, ebit, free_cashflow, cash, total_debt, shares_outstanding`.

Secondary fields retained: `gross_profit, operating_income, ebitda, net_income, operating_cashflow, capex`.

All 12 fundamental variables remain retained. EBIT is primary operating earnings. EBITDA is secondary and does not block primary readiness.

## Valuation

Market cap is `price * shares_outstanding`; net debt is `total_debt - cash`; EV is `market_cap + total_debt - cash`. Primary metrics are EV/EBIT, EBIT yield, FCF yield and EV/Sales. EV/EBITDA and P/E are retained as secondary metrics.

## Coverage

- EV/EBIT computable endpoints: `36864`
- EV/EBIT meaningful endpoints: `25558`
- EV/EBITDA computable endpoints: `33615`
- EV/EBITDA meaningful endpoints: `26352`
- FCF yield computable endpoints: `47825`
- EV/Sales computable endpoints: `40315`
- P/E computable endpoints: `49087`
- Primary score-ready endpoints: `34883`
- Latest companies primary score-ready: `2377`
- EBIT-primary coverage gain vs EBITDA-primary: `3249`

## Safety

Production writes: `{'score': 0, 'valuation': 0, 'ttm': 0, 'canonical': 0}`.

Next: `MASTER PLAN PHASE 6B - SCORE & LIFECYCLE CALIBRATION DESIGN`
