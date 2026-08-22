# Fundamentals V3 Phase 3B Missing-Core Diagnostic

Status: `YAHOO_CORE_DIAGNOSTIC_CLEAN_V2_IDENTITY_GATE_READY`

Artifact root:

`temp/fundamentals_v3_phase3b_missing_core_diagnostic/20260822T_PHASE3B_MISSING_CORE_DIAGNOSTIC/`

This phase was read-only. It did not repair canonical V3 values and did not run provider acquisition.

## Baseline

The production V3 baseline matched the Phase 3B Yahoo seed checkpoint exactly:

| Metric | Count |
| --- | ---: |
| Canonical quarters | 14,345 |
| Core-ready quarters | 12,344 |
| Core-not-ready quarters | 2,001 |
| publish_date known | 12,990 |
| publish_date NULL | 1,355 |

Core missing counts:

| Field | Missing |
| --- | ---: |
| revenue | 633 |
| EBITDA | 1,613 |
| free_cashflow | 646 |
| cash | 637 |
| total_debt | 874 |
| shares_outstanding | 0 |

## Yahoo Gap Diagnosis

Every missing core value traces to Yahoo/source NULL evidence in the Phase 3B audit lineage. No accepted Yahoo non-null field was found that failed to reach canonical V3.

| Field | Primary cause | Count |
| --- | --- | ---: |
| revenue | `YAHOO_FIELD_NULL` | 633 |
| EBITDA | `YAHOO_DIRECT_EBITDA_NULL` | 1,613 |
| free_cashflow | `YAHOO_FIELD_NULL` | 646 |
| cash | `YAHOO_FIELD_NULL` | 637 |
| total_debt | `YAHOO_FIELD_NULL` | 874 |
| shares_outstanding | no missing rows | 0 |

Yahoo-local direct/derived recovery is zero for all missing core fields. In particular, EBITDA was not treated as derivable from operating income alone because V3 Yahoo canonical state does not carry a separate depreciation/amortization input.

Missing overlap:

| Missing core fields per quarter | Quarters |
| ---: | ---: |
| 1 | 1,302 |
| 2 | 61 |
| 3 | 32 |
| 4 | 147 |
| 5 | 459 |

Affected companies: 732 total, 726 active, 6 inactive. Of those affected companies, 539 have V2 coverage and 193 are legacy-only for this diagnostic.

## V2 Identity Gate

Exact `market + ticker + fiscal_year + fiscal_quarter` is only `IDENTITY_CANDIDATE_MATCH`.

The proposed same-quarter confirmation gate uses:

- compatible period-end relation: `EXACT_PERIOD_END`, `SMALL_KNOWN_PROVIDER_VARIANT`, or documented `KNOWN_FISCAL_CALENDAR_VARIANT`
- fingerprint fields split into Tier A/B/C
- 5% relative identity-evidence tolerance with a 10,000 absolute near-zero floor
- sign-mismatch protection for income and cash-flow fields
- no Tier A contradiction for automatic strong confirmation

`STRONG_MATCH` requires at least three comparable fingerprint fields, at least two comparable Tier A fields, at least two matching Tier A fields, compatible period end, and all or nearly all comparable fields matching.

Classification of exact V2 FY/FQ candidates:

| Classification | Count |
| --- | ---: |
| `STRONG_MATCH` | 2,111 |
| `STRONG_MATCH_LIMITED_FIELDS` | 3 |
| `PROBABLE_MATCH` | 842 |
| `INSUFFICIENT_EVIDENCE` | 389 |
| `CONFLICT` | 7,261 |
| `PERIOD_IDENTITY_CONFLICT` | 16 |

Period relation distribution:

| Relation | Count |
| --- | ---: |
| `EXACT_PERIOD_END` | 10,568 |
| `KNOWN_FISCAL_CALENDAR_VARIANT` | 37 |
| `MATERIAL_PERIOD_END_DIFFERENCE` | 17 |

Known unusual cases were not hard-coded. The diagnostic classifies examples such as CAVA, SJM, LYTS, and related fiscal-calendar anomalies as probable/conflict when values or provider periods do not support a blind same-quarter match.

## Agreement Distributions

The tolerance analysis shows that V2 values are not uniformly safe for direct same-quarter trust. Revenue is the strongest individual identity field, but single-field evidence remains insufficient.

Selected field quality:

| Field | Compared | >10% mismatch | Conflict rate | Recommendation |
| --- | ---: | ---: | ---: | --- |
| revenue | 9,770 | 761 | 7.79% | `STRONG_IDENTITY_FIELD` |
| operating_income | 9,260 | 1,325 | 14.31% | `SUPPORTING_IDENTITY_FIELD` |
| net_income | 9,882 | 1,115 | 11.28% | `DO_NOT_USE_AS_PRIMARY_IDENTITY_FIELD` |
| operating_cashflow | 9,942 | 1,278 | 12.85% | `DO_NOT_USE_AS_PRIMARY_IDENTITY_FIELD` |
| cash | 9,738 | 2,861 | 29.38% | `DO_NOT_USE_AS_PRIMARY_IDENTITY_FIELD` |
| total_debt | 9,496 | 3,102 | 32.67% | `DO_NOT_USE_AS_PRIMARY_IDENTITY_FIELD` |
| EBITDA | 9,229 | 3,652 | 39.57% | `DO_NOT_USE_AS_PRIMARY_IDENTITY_FIELD` |

Adjacent-quarter false-confidence check:

| Metric | Count |
| --- | ---: |
| adjacent comparisons | 19,030 |
| same FY/FQ score stronger | 17,977 |
| adjacent equal or stronger | 1,053 |
| same FY/FQ stronger percentage | 94.47% |

This supports using fingerprint evidence, but the 1,053 adjacent-equal-or-stronger cases justify keeping the Phase 3C automatic gate strict.

## Recovery Estimates

Safe V2 recovery is confidence-gated by `STRONG_MATCH`; raw V2 availability is not treated as safe enrichment.

| Field | V3 NULL | V2 value on FY/FQ candidate | V2 value on `STRONG_MATCH` | Legacy exact-period available |
| --- | ---: | ---: | ---: | ---: |
| revenue | 633 | 431 | 0 | 577 |
| EBITDA | 1,613 | 859 | 53 | 0 |
| free_cashflow | 646 | 428 | 0 | 568 |
| cash | 637 | 437 | 0 | 593 |
| total_debt | 874 | 366 | 5 | 407 |
| shares_outstanding | 0 | 0 | 0 | 0 |

V2 publication-date recoverability among V3 publish-null rows with exact V2 FY/FQ candidates:

| Metric | Count |
| --- | ---: |
| V3 publish NULL with V2 FY/FQ match | 8 |
| V2 publish_date available | 7 |
| `STRONG_MATCH` rows | 1 |
| `STRONG_MATCH` + V2 publish_date | 1 |
| non-strong rows with V2 publish_date | 6 |

Initial Phase 3C should therefore fill V2 publish_date only for `STRONG_MATCH` rows.

## Safety

Read-only proof:

| Check | Result |
| --- | --- |
| V3 writes | 0 |
| V2 writes | 0 |
| Legacy writes | 0 |
| V3 quick_check | `ok` |
| V3 foreign_key_check rows | 0 |
| potential Phase 3B migration bugs | 0 |

No RawCandle, Check/Update, provider, raw-cache, or canonical V3 write path was changed.

## Phase 3C Gate

Recommended Phase 3C automatic behavior:

- V3 NULL + confirmed V2 non-null -> candidate for fill
- V3 non-null + V2 same -> confirmation only
- V3 non-null + V2 different -> conflict/report
- never silently overwrite a non-null Yahoo canonical value
- `STRONG_MATCH_LIMITED_FIELDS`, `PROBABLE_MATCH`, `INSUFFICIENT_EVIDENCE`, `CONFLICT`, and `PERIOD_IDENTITY_CONFLICT` remain hold/report classes for the first V2 enrichment pass

Recommended next phase:

`MASTER PLAN PHASE 3C - V2 METADATA & VALUE ENRICHMENT`
