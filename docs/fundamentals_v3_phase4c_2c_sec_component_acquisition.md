# Fundamentals V3 Phase 4C-2C SEC Component Acquisition

Classification: `FUNDAMENTALS_V3_PHASE4C2C_SEC_COMPONENT_LAYER_COMPLETE_READY_FOR_FORMULA_RERUN`

Canonical financial writes: `0`

## Source

Primary source is SEC companyfacts. Filing-level XBRL remains the designed fallback for issuer-specific extension gaps.

## Storage

- Raw cache: `temp/fundamentals_v3_sec_components_runtime/raw_companyfacts`
- Component DB: `temp/fundamentals_v3_sec_components_runtime/rc_fundamentals_v3_sec_components.db`
- Tables: sec_component_raw_cache, sec_component_acquisition_state, sec_component_fact, sec_component_concept_registry
- Natural key: `sha256(cik|namespace|concept|unit|start|end|accession|filed|fy|fp|dimensions_json)`

## Universe

- Approved companies: 2550
- CIK mapped: 2481
- CIK unmapped: 69
- Companies fetched/state rows: 2550
- Fetch OK: 2481
- Empty: 0
- CIK missing bounded residuals: 69
- Hard fetch/parse/rate failures: 0

## Coverage

- Pretax companies: 2405
- Interest companies: 2330
- D&A companies: 2463
- Missing EBIT with pretax + interest: 3114
- Missing EBITDA derivable EBIT + D&A: 6889

## Quarterization

- Q1 direct-ready: 290054
- Q2 direct-ready: 233361
- Q2 YTD-difference-ready: 289961
- Q3 direct-ready: 216418
- Q3 YTD-difference-ready: 267506
- Q4 FY-minus-9M-ready: 555720

## Issuer Extensions

Issuer extensions retained: `True`. Semantic approval is deferred to company-specific mapping.

## Next

`MASTER PLAN PHASE 4C-2D - COMPANY-SPECIFIC FORMULA DISCOVERY RERUN ON SEC COMPONENT LAYER`
