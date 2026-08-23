# Fundamentals V3 Phase 3C-1E - SEC Q4 Field Semantics Validation

Status:

`FUNDAMENTALS_V3_PHASE3C_1E_SEC_Q4_FIELD_POLICY_COMPLETE_READY_FOR_3C2`

Artifact root:

`temp/fundamentals_v3_phase3c_1e_sec_q4_field_validation/20260823T_PHASE3C_1E_SEC_Q4_FIELD_VALIDATION/`

## Scope

Phase 3C-1E does not reopen Q4 identity. Phase 3C-1D already established that normal SEC annual rows represent the missing Q4 slot for the accepted fiscal-year chain. This phase validates which canonical fields may be populated for those Q4 rows and how.

No V3, Legacy, or V2 rows were written. RawCandle and provider acquisition were not touched.

## SEC Source Semantics

Local Legacy SEC data is stored in:

- `rc_fundamental_statement_raw`
- `rc_fundamental_quarterly`

`rc_fundamental_statement_raw` does not expose dedicated `form_type`, `period_start`, or `duration` columns. SEC fact metadata is embedded in `field_name` as attributes such as:

`concept|form=10-K|unit=USD|fy=2025|fp=FY|frame=...|start=...|filed=...`

This is sufficient for read-only policy validation and Phase 3C-2 compatibility checks:

- form evidence: available from `form=...`
- fiscal period/year: available from `fp=...` and `fy=...`
- start/duration hint: available from `start=...`; instant facts usually have `start=NULL`
- filing vintage: available from `filed=...`
- concept identity: available as the first pipe-delimited token

## Flow Versus Instant

Flow fields may be reconstructed only from compatible duration facts:

- `revenue`
- `gross_profit`
- `operating_income`
- `net_income`
- `operating_cashflow`
- `capex`
- `free_cashflow`

Instant fields must never be subtracted:

- `cash`
- `total_debt`
- `shares_outstanding`

The final helper policy rejects instant subtraction paths such as:

- `Q4 cash = FY cash - Q1 cash - Q2 cash - Q3 cash`
- `Q4 debt = FY debt - prior debt`
- `Q4 shares = FY shares - prior shares`

## Vintage Policy

The selected basis for Phase 3C-2 is latest-consistent annual basis where available. A Q4 flow derivation may use FY minus 9M or FY minus Q1/Q2/Q3 only when the values are compatible by concept, unit, fiscal year, duration semantics, and reporting vintage.

If the annual FY value and interim values cannot be aligned without mixing incompatible restatement vintages, Q4 identity remains valid but that field is left NULL with a `Q4_DERIVATION_VINTAGE_CONFLICT`-style reason.

## Final Field Matrix

| field | type | preferred mode | fallback | status | Phase 3C-2 policy |
| --- | --- | --- | --- | --- | --- |
| revenue | FLOW | FY_MINUS_Q1_Q2_Q3 | FY_MINUS_9M | APPROVED_HIGH_CONFIDENCE | populate from compatible SEC flow subtraction; else NULL |
| gross_profit | FLOW | FY_MINUS_Q1_Q2_Q3 | FY_MINUS_9M | APPROVED_HIGH_CONFIDENCE | populate from compatible SEC flow subtraction; else NULL |
| operating_income | FLOW | FY_MINUS_Q1_Q2_Q3 | UNSAFE_LEAVE_NULL | CONDITIONAL | populate only when exact operating-income semantics pass compatibility; else NULL |
| EBIT | FLOW | UNSAFE_LEAVE_NULL | UNSAFE_LEAVE_NULL | NOT_APPROVED_LEAVE_NULL | leave NULL unless later approved direct EBIT contract exists |
| EBITDA | FLOW | UNSAFE_LEAVE_NULL | UNSAFE_LEAVE_NULL | NOT_APPROVED_LEAVE_NULL | leave NULL for SEC-reconstructed Q4s |
| net_income | FLOW | FY_MINUS_Q1_Q2_Q3 | UNSAFE_LEAVE_NULL | CONDITIONAL | populate only when exact net-income semantics pass compatibility; else NULL |
| operating_cashflow | FLOW | FY_MINUS_9M | FY_MINUS_Q1_Q2_Q3 | APPROVED_HIGH_CONFIDENCE | prefer FY minus compatible 9M YTD; fallback to compatible Q1+Q2+Q3 |
| capex | FLOW | FY_MINUS_9M | FY_MINUS_Q1_Q2_Q3 | APPROVED_WITH_KNOWN_SEMANTIC_DIFFERENCES | populate from compatible subtraction and normalize capex as cash outflow negative |
| free_cashflow | FLOW | APPROVED_DERIVATION | UNSAFE_LEAVE_NULL | APPROVED_HIGH_CONFIDENCE | derive as reconstructed operating_cashflow + reconstructed capex; else NULL |
| cash | INSTANT | DIRECT_FY_END_INSTANT | UNSAFE_LEAVE_NULL | APPROVED_HIGH_CONFIDENCE | use direct FY-end instant SEC value; never subtract |
| total_debt | INSTANT | DIRECT_FY_END_INSTANT | DIRECT_FY_END_COMPONENT_DERIVATION | APPROVED_WITH_KNOWN_SEMANTIC_DIFFERENCES | use SEC FY-end instant debt source as semantic truth for Legacy-created Q4; document Yahoo differences |
| shares_outstanding | INSTANT | DIRECT_FY_END_INSTANT | UNSAFE_LEAVE_NULL | APPROVED_WITH_KNOWN_SEMANTIC_DIFFERENCES | use direct period-end instant shares when concept is accepted; never weighted average |

## Known-Q4 Calibration

Known canonical V3 Q4 calibration population:

`2601` Q4 periods.

| field | comparable | <=1% | <=2% | <=5% | <=10% | >10% | status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| revenue | 2263 | 2000 | 2006 | 2023 | 2035 | 228 | APPROVED_HIGH_CONFIDENCE |
| gross_profit | 1897 | 1849 | 1850 | 1858 | 1868 | 29 | APPROVED_HIGH_CONFIDENCE |
| operating_income | 2248 | 1323 | 1376 | 1484 | 1582 | 666 | CONDITIONAL |
| EBIT | 2021 | 542 | 664 | 910 | 1145 | 876 | NOT_APPROVED_LEAVE_NULL |
| EBITDA | 1 | 1 | 1 | 1 | 1 | 0 | NOT_APPROVED_LEAVE_NULL |
| net_income | 2188 | 1478 | 1495 | 1530 | 1580 | 608 | CONDITIONAL |
| operating_cashflow | 2272 | 2078 | 2078 | 2083 | 2087 | 185 | APPROVED_HIGH_CONFIDENCE |
| capex | 2362 | 1720 | 1736 | 1762 | 1794 | 568 | APPROVED_WITH_KNOWN_SEMANTIC_DIFFERENCES |
| free_cashflow | 2256 | 1712 | 1761 | 1835 | 1892 | 364 | APPROVED_HIGH_CONFIDENCE |
| cash | 2264 | 2231 | 2234 | 2239 | 2240 | 24 | APPROVED_HIGH_CONFIDENCE |
| total_debt | 2263 | 1182 | 1250 | 1391 | 1562 | 701 | APPROVED_WITH_KNOWN_SEMANTIC_DIFFERENCES |
| shares_outstanding | 2522 | 647 | 1023 | 1587 | 1849 | 673 | APPROVED_WITH_KNOWN_SEMANTIC_DIFFERENCES |

## Debt Decision

Debt comparison to Yahoo is weaker than expected, but this does not invalidate SEC FY-end debt semantics. The main discrepancy classes are:

- Yahoo aggregation differs from SEC concept/component selection.
- SEC may expose current, noncurrent, finance-lease, convertible, or borrowing components separately.
- Yahoo may include or exclude lease/current debt scopes differently.

For Legacy-created Q4 rows with no existing canonical value, SEC FY-end instant debt is the more semantically appropriate source. Phase 3C-2 must prefer direct total-debt concepts when available and may fall back to approved current+noncurrent component construction. It must not overwrite existing canonical values.

## Shares Decision

Shares comparison to Yahoo is also weaker, largely because Yahoo can use split-adjusted or weighted-average semantics that differ from period-end SEC shares. V3 Q4 historical shares require period-end instant shares outstanding.

Weighted-average basic shares, weighted-average diluted shares, and EPS denominators are rejected. SEC period-end instant shares are the preferred source for Legacy-created Q4 rows when the concept is accepted.

## EBIT And EBITDA

EBIT is not assumed to equal operating income. EBITDA is not generically reconstructed. Both remain NULL for SEC-reconstructed Q4 rows unless a later locked direct/approved contract exists.

This does not block Q4 creation.

## Planned 14,633 Q4 Coverage

| field | populated | left NULL |
| --- | ---: | ---: |
| revenue | 12514 | 2119 |
| gross_profit | 7263 | 7370 |
| operating_income | 12765 | 1868 |
| EBIT | 0 | 14633 |
| EBITDA | 0 | 14633 |
| net_income | 12987 | 1646 |
| operating_cashflow | 14079 | 554 |
| capex | 12309 | 2324 |
| free_cashflow | 12293 | 2340 |
| cash | 13718 | 915 |
| total_debt | 8292 | 6341 |
| shares_outstanding | 12966 | 1667 |
| publish_date | 14633 | 0 |

Expected core readiness for reconstructed Q4s:

| bucket | rows |
| --- | ---: |
| all core ready | 0 |
| missing EBITDA only | 6829 |
| other missing core combinations | 7804 |

Because EBITDA remains intentionally NULL, no reconstructed Q4 is expected to satisfy the full current core-field set solely from this SEC Q4 policy.

## Phase 3C-2 Import Contract

Machine-readable policy:

`temp/fundamentals_v3_phase3c_1e_sec_q4_field_validation/20260823T_PHASE3C_1E_SEC_Q4_FIELD_VALIDATION/phase3c2_q4_policy.json`

Repo-native policy source:

`swingmaster/fundamentals/v3_sec_q4_field_semantics.py`

Permanent invariants:

- `LEGACY_CAN_OVERWRITE_EXISTING_CANONICAL_VALUE = FALSE`
- Q4 identity and Q4 field completeness remain separate gates.
- Instant fields are direct FY-end values only.
- Flow fields require concept, unit, duration, fiscal-year, and vintage compatibility.
- Unsafe fields remain NULL; they do not block Q creation.

Recommended next phase:

`MASTER PLAN PHASE 3C-2 - LEGACY DEEP-HISTORY EXTENSION`
