# Fundamentals V3 Phase 4C-2 Company Formula Discovery

Classification: `FUNDAMENTALS_V3_PHASE4C2_FORMULA_DISCOVERY_PARTIAL`

Artifact root: `temp/fundamentals_v3_phase4c_2_company_formula_discovery/20260823T_PHASE4C2_COMPANY_FORMULA_DISCOVERY`

## Result

Company-specific formula discovery was implemented with temporal train/test validation and normalized metadata design. Local SEC/XBRL evidence did not contain enough approved pretax, interest, and D&A component coverage to approve STRONG canonical EBIT or EBITDA fingerprints. Operating-income fallback was evaluated only as `PROXY`.

## Baseline

- Companies: 2550
- Canonical Q: 73075
- EBIT missing: 7032
- EBITDA missing: 18179
- Known EBIT targets: 66043
- Known EBITDA targets: 54896

## Fingerprints

- STRONG EBIT: 0
- PROXY EBIT: 458
- STRONG EBITDA: 0
- CONDITIONAL EBITDA: 0

## Metadata Architecture

Metadata table design: `rc_company_fundamental_formula_profile`. No metadata rows were persisted to production in this phase; `company_formula_metadata_dry.csv` contains dry rows only.

## Recovery Potential

- Earlier direct EBIT recoverable: 252
- Additional STRONG EBIT recoverable: 0
- STRONG EBITDA recoverable: 0
- Expected core-ready uplift from STRONG EBITDA: 0

Next: `MASTER PLAN PHASE 4C-2B - SEC COMPONENT ACQUISITION FOR TRUE EBIT/EBITDA FORMULAS`
