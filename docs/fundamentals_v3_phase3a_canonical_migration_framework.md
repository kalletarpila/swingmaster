# Fundamentals V3 Phase 3A Canonical Migration Framework

Status: `FUNDAMENTALS_V3_PHASE3A_CANONICAL_MIGRATION_FRAMEWORK_COMPLETE`

Phase 3A implements the source-agnostic canonical apply layer for later separated Phase 3 source passes. It does not execute production Yahoo, V2, or Legacy enrichment and does not write production canonical V3 data.

## Source-By-Source Strategy

The engine accepts one source batch at a time:

- `source="YAHOO"` for Phase 3B
- `source="V2"` for Phase 3C
- `source="LEGACY"` for Phase 3D

Each batch has its own `migration_run_id`, audit rows, candidate results, and contribution summary. This preserves source attribution and prevents a monolithic `Yahoo + V2 + Legacy` migration.

## Candidate Model

`V3CanonicalMigrationCandidate` carries:

- source identity: `source_system`, `source_record_id`, `migration_run_id`, optional `raw_evidence_ref`
- company identity: `market`, `ticker`, optional `company_name`, optional `approved_company_active`
- canonical fiscal identity: `fiscal_year`, `fiscal_quarter`
- dates: `period_end_date`, nullable `publish_date`, nullable `market_availability_date`
- canonical values: `revenue`, `gross_profit`, `operating_income`, `ebit`, `ebitda`, `net_income`, `operating_cashflow`, `capex`, `free_cashflow`, `cash`, `total_debt`, `shares_outstanding`
- compact policy/evidence inputs: `candidate_can_create_quarter`, `candidate_issue_type`, `period_date_policy`, `field_semantic_differences`, `derivation_inputs`, `value_metadata`

The canonical quarter key is always `company_id + fiscal_year + fiscal_quarter`. Provider `period_end_date` is metadata and never creates a second canonical quarter by itself.

## Field Apply Rules

For each retained field:

- canonical `NULL` plus incoming non-null fills the field.
- incoming `NULL` is a noop.
- same non-null value confirms.
- deterministic rounding-equivalent value confirms without overwrite.
- materially different non-null value creates `NON_NULL_FIELD_CONFLICT` and does not overwrite.
- explicit semantic-difference policy records `FIELD_EXPECTED_SEMANTIC_DIFFERENCE`.

The default rounding policy is intentionally narrow: absolute tolerance `1.0` or relative tolerance `0.000001`, whichever is larger. Callers may supply field-specific `V3FieldPolicy`.

## Derivations

Supported deterministic derivations:

- `free_cashflow = operating_cashflow + capex`
- `ebitda = operating_income + depreciation_amortization` when supplied for an ordinary-company candidate
- `total_debt = short_term_debt + long_term_debt`

Direct values are retained when both direct and derived values exist. The engine records compact derivation notes through `derivation_method`. `net_debt` remains downstream only and is not stored in quarterly fundamentals.

## Date Reconciliation

`period_end_date` is selected metadata:

- missing canonical date can be set.
- same date confirms.
- safe provider variants are recorded as `PERIOD_DATE_SAFE_VARIANT` without replacing canonical metadata.
- unresolved or conflicting variants create issue rows and do not silently replace.

`publish_date` is orthogonal:

- canonical Q creation is allowed with `publish_date = NULL`.
- missing publish date can be filled later.
- same publish date confirms.
- conflicting publish date creates `PUBLICATION_DATE_CONFLICT` without earliest/latest guessing.

## Company And Quarter Gates

The engine may create a company only when `approved_company_active` is supplied by a validated approved-universe caller. Existing companies are matched regardless of `active`; `active = 0` does not block historical import.

A new canonical quarter requires:

- approved or existing company
- resolved fiscal year and quarter
- accepted `period_end_date`
- `period_end_date >= 2018-01-01`
- `candidate_can_create_quarter = true`

`publish_date` may be null. Provider-period variants can be audited and excluded with `candidate_can_create_quarter = false`.

## Audit, Issues, And Transactions

One candidate is applied inside one SQLite savepoint transaction. The transaction includes company/Q match or create, date reconciliation, field apply, migration audit, and resolution issues. Controlled conflicts are recorded and the next candidate can continue. Programmer or database errors roll back the candidate and stop.

`v3_migration_audit` records one `CANONICAL_APPLY` row per `(migration_run_id, source, source_record_id)`, using the existing upsert key for idempotency.

Supported issue types:

- `DUPLICATE_FISCAL_WORK_UNIT`
- `PERIOD_DATE_CONFLICT`
- `NON_NULL_FIELD_CONFLICT`
- `TRANSITION_PERIOD_VARIANT`
- `FISCAL_MAPPING_CORRECTION`
- `PUBLICATION_DATE_CONFLICT`
- `OTHER_MIGRATION_REVIEW`

Issue creation is semantic-idempotent for active issues.

## Source Contribution Accounting

`V3CanonicalMigrationRunSummary` reports:

- rows examined, accepted, rejected
- companies created, quarters created, quarters matched
- metadata outcomes
- per-field outcome counters for every retained field
- issue counters
- candidate result summaries
- temp DB integrity result

The same structure is used for Yahoo, V2, and Legacy fixture runs.

## Special-Case Hooks

Known Phase 3 exception classes are represented generically:

- CAVA-like duplicate work units: same canonical FY/Q, complementary fields, safe period-date variant.
- NEUP-like correction: caller supplies corrected FY/FQ and evidence reference.
- LFCR-like transition provider variant: `candidate_can_create_quarter = false` plus `TRANSITION_PERIOD_VARIANT`.

No ticker-specific logic exists in the core engine.

## Phase 3B Handoff

Phase 3B should convert the completed Yahoo bootstrap candidates into `V3CanonicalMigrationCandidate` rows and call:

```python
V3CanonicalMigrationEngine(conn).apply_source_batch(
    yahoo_candidates,
    source="YAHOO",
    migration_run_id=run_id,
)
```

Phase 3B should persist the returned summary as the Yahoo canonical seed contribution report and handle any generated resolution issues before later source passes.
