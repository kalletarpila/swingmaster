# Fundamentals V4 Provider Evaluation

## Sharadar AAPL Shares Validation

Tested fields: Sharadar `sharesbas`, `shareswa`, and `shareswadil` against V3 canonical `shares_outstanding` for AAPL ARQ rows.

Primary mapping under evaluation: `V4 shares_outstanding = Sharadar ARQ sharesbas`.

Latest-8 result: exact `0`, near `7`, material `0`, V3 missing `1`, Sharadar missing `0`.

Mean absolute percentage difference: V3 vs `sharesbas` `0.0038293173652539825`, V3 vs `shareswa` `0.0029396211080059387`, V3 vs `shareswadil` `0.0036565775411157757`.

ARQ/MRQ: matching periods `20`, same `0`, different `20`.

Finding: `sharesbas` aligns materially better with V3 point-in-time shares than weighted-average `shareswa` or diluted weighted-average `shareswadil`. Local SEC exact-period-end CommonStockSharesOutstanding evidence was not available for a direct three-way AAPL row check in this diagnostic. Split-adjustment semantics remain `SPLIT_ADJUSTMENT_SEMANTICS_NOT_PROVEN` from this sample alone.

Decision: `SHARESBAS_ACCEPT_WITH_VALIDATION_GUARD`.

