# Fundamentals V3 Phase 4C-3B Rejected Case Review

Outcome: `REJECTED_CASE_REVIEW_BOUNDED_REFINEMENT_JUSTIFIED`

The review opened 15 representative rejected EBIT/EBITDA cases after Phase 4C-3 production apply. No canonical financial or metadata writes were performed.

| Ticker | FY/FQ | Metric | Rejection | Candidate | Hist fit | Disposition |
| --- | --- | --- | --- | --- | --- | --- |
| ABR | FY2024 Q1 | ebit | MULTIPLE_INTEREST_CANDIDATES | PRETAX_PLUS_INTEREST `81470000.0` | 1/1 historical candidates within 1% | NEEDS_MORE_EVIDENCE |
| AIV | FY2026 Q1 | ebit | MULTIPLE_INTEREST_CANDIDATES | PRETAX_PLUS_INTEREST `-4686000.0` | 1/8 historical candidates within 1% | NEEDS_MORE_EVIDENCE |
| ACHC | FY2021 Q1 | ebit | INTEREST_SEMANTIC_AMBIGUITY |  `` | 4/8 historical candidates within 1% | REJECTION_CORRECT |
| ACRE | FY2018 Q1 | ebit | LOW_SAMPLE | PRETAX_PLUS_INTEREST `23698000.0` | no local historical target fit available | NEEDS_MORE_EVIDENCE |
| AA | FY2018 Q1 | ebit | COMPONENT_QUARTERIZATION_REJECTION | PRETAX_PLUS_INTEREST `23000000.0` | 0/5 historical candidates within 1% | REJECTION_CORRECT |
| ADM | FY2018 Q4 | ebit | Q4_REJECTION | PRETAX_PLUS_INTEREST `409000000.0` | 3/5 historical candidates within 1% | REJECTION_CORRECT |
| A | FY2018 Q1 | ebitda | CANONICAL_EBIT_AVAILABLE_BUT_DA_NOT_APPROVED | CANONICAL_EBIT_PLUS_DA `280000000.0` | 5/8 historical candidates within 1% | REJECTION_CORRECT |
| AAL | FY2018 Q1 | ebitda | CANONICAL_EBIT_AVAILABLE_BUT_DA_NOT_APPROVED | CANONICAL_EBIT_PLUS_DA `841000000.0` | 0/6 historical candidates within 1% | REJECTION_CORRECT |
| AHR | FY2018 Q1 | ebitda | COMBINED_DA_VS_DEP_AMORT_CONFLICT |  `` | 0/8 historical candidates within 1% | NEEDS_MORE_EVIDENCE |
| ACRE | FY2021 Q1 | ebitda | LOW_SAMPLE_DA |  `` | no local historical target fit available | REJECTION_CORRECT |
| AA | FY2018 Q1 | ebitda | D_AND_A_SEMANTIC_REJECTION |  `` | 0/5 historical candidates within 1% | REJECTION_CORRECT |
| AAOI | FY2018 Q4 | ebitda | Q4_DA_REJECTION | CANONICAL_EBIT_PLUS_DA `-4386000.0` | 5/8 historical candidates within 1% | REJECTION_CORRECT |
| AAON | FY2018 Q4 | ebitda | Q4_DA_REJECTION | CANONICAL_EBIT_PLUS_DA `21916000.0` | 1/8 historical candidates within 1% | REJECTION_CORRECT |
| ADNT | FY2018 Q4 | ebit | Q4_REJECTION | PRETAX_PLUS_INTEREST `-1045000000.0` | 3/5 historical candidates within 1% | REJECTION_CORRECT |
| AAP | FY2018 Q1 | ebitda | CANONICAL_EBIT_AVAILABLE_BUT_DA_NOT_APPROVED | CANONICAL_EBIT_PLUS_DA `269933000.0` | 4/8 historical candidates within 1% | REJECTION_CORRECT |

## Repeated Patterns

- `CANONICAL_EBIT_AVAILABLE_BUT_DA_NOT_APPROVED`: `JUSTIFIED_REJECTION`, rows `9089`, companies `1597`
- `Q4_DA_REJECTION`: `JUSTIFIED_REJECTION`, rows `4315`, companies `2434`
- `D_AND_A_SEMANTIC_REJECTION`: `JUSTIFIED_REJECTION`, rows `3994`, companies `1019`
- `COMPONENT_QUARTERIZATION_REJECTION`: `JUSTIFIED_REJECTION`, rows `3386`, companies `451`
- `Q4_REJECTION`: `JUSTIFIED_REJECTION`, rows `1939`, companies `836`
- `LOW_SAMPLE`: `JUSTIFIED_REJECTION`, rows `1054`, companies `139`
- `INTEREST_SEMANTIC_AMBIGUITY`: `JUSTIFIED_REJECTION`, rows `294`, companies `43`
- `MULTIPLE_INTEREST_CANDIDATES`: `CONCEPT_REGISTRY_GAP`, rows `255`, companies `37`
- `COMBINED_DA_VS_DEP_AMORT_CONFLICT`: `CONCEPT_REGISTRY_GAP`, rows `150`, companies `40`
- `LOW_SAMPLE_DA`: `JUSTIFIED_REJECTION`, rows `147`, companies `43`

Next: `MASTER PLAN PHASE 4C-3C - BOUNDED REJECTION-LOGIC REFINEMENT`
