# Fundamental Score EBITDA Profitability Model

## Production Model

The ordinary fundamental score now uses the calibrated `MODEL_2_T2_L2` profitability model.

Profitability margin is `EBITDA_MARGIN_TTM = EBITDA_TTM / REVENUE_TTM`.

Margin component:

- `NULL -> 0`
- `< 0 -> 0`
- `0..<15% -> 4`
- `15%..<25% -> 8`
- `25%..<35% -> 12`
- `>=35% -> 15`

Margin trend component is current 4Q EBITDA margin minus previous 4Q EBITDA margin:

- `NULL -> 6`
- `< 0pp -> 2`
- `0pp..<4pp -> 6`
- `4pp..<10pp -> 10`
- `>=10pp -> 15`

The score is still produced when margin trend is `NULL`, but comparable score readiness is false.

## Lifecycle Inputs

Lifecycle classification now uses EBITDA margin and EBITDA margin trend for profitability conditions. Rule precedence is preserved.

- `DISTRESSED`: EBITDA margin `< -30%` and FCF margin `< -20%`
- `STARTUP`: growth `> 30%`, EBITDA margin `< 0%`, and FCF margin `< 0`
- `GROWTH`: growth `> 20%` and EBITDA margin `< 15%`
- `SCALING`: growth `> 10%`, EBITDA trend `> 0`, and EBITDA margin `>= 0`
- `MATURE`: EBITDA margin `>= 25%`, FCF margin `>= 5%`, and growth `>= -5%` or `NULL`
- `TRANSITION`: EBITDA margin `0..<25%`, FCF margin `>= 0`, growth `>= -5%` or `NULL`, and EBITDA trend `>= -7pp` or `NULL`
- `DECLINING`: growth `< -5%` or EBITDA trend `< -7pp`

Lifecycle score scaling is unchanged; only the profitability inputs changed.

## Readiness Policy

Scores remain available under `KEEP_SCORE_WITH_READINESS_FLAG`. Missing inputs do not normalize denominators.

Comparable score readiness is true only when all ordinary-profile checks are true:

- `SCORE_PROFILE_SUPPORTED`
- `GROWTH_READY`
- `EBITDA_MARGIN_READY`
- `EBITDA_MARGIN_TREND_READY`
- `FCF_READY`
- `EBITDA_CONSISTENCY_READY`
- `LEVERAGE_READY`
- `DILUTION_READY`

Unsupported profiles such as banks and insurance should be classified as `SCORE_PROFILE_UNSUPPORTED`, not as ordinary not-ready rows.

Latest-quarter core fields are current-quarter identity and period plus non-null revenue, EBITDA, free cash flow, cash, total debt, and positive shares outstanding. Historical depth is separate from latest-quarter core completeness.

## EBIT References

EBIT remains stored and built as a TTM metric for valuation, reporting, historical snapshots, and legacy compatibility. It is no longer a profitability dependency for ordinary scoring, margin trend scoring, score consistency profitability, lifecycle profitability, or percentile ranking source selection.

The percentile row model still exposes legacy attribute names for compatibility, but the database loaders alias EBITDA margin and EBITDA margin trend into those fields.

## Bias And Coverage

The change was made because EBIT coverage is materially weaker and less coherent for the ordinary company score population than EBITDA coverage. EBITDA also aligns the profitability component with the existing EBITDA-preferred leverage path.

The model is expected to shift scores downward for companies with 15-35% EBITDA margins or moderate positive EBITDA margin trend, because the calibrated bands are less generous than the older EBIT bands. This is intentional and should be interpreted through score readiness and cohort percentile calibration.

Existing legacy `rc_fundamental_ttm` rows created before EBITDA TTM support may need a rebuild or backfill before a production score rewrite. The score implementation intentionally treats missing EBITDA margin trend as not comparable instead of falling back to EBIT.
