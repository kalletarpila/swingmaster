# Fundamentals V3 Phase 1 Foundation Closure

Date: 2026-08-21

Classification: `FUNDAMENTALS_V3_PHASE1_FOUNDATION_COMPLETE`

## Scope Confirmation

Phase 1 closes schema, repositories, canonical helpers, and the Legacy-authority universe selector.

Safety invariants verified:

- Legacy production writes: 0
- V2 production writes: 0
- Production V3 DB creation/writes: 0
- Provider/network/Yahoo/SEC/SimFin calls: 0
- Production Check/Update execution: 0
- RawCandle changes: 0

All validation used `/home/kalle/projects/swingmaster/.venv/bin/python`.

## Schema Contract Parity

Canonical V3 table classification:

| Table | Classification |
| --- | --- |
| `v3_schema_version` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_run` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_company` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_provider_symbol_alias` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_quarter` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_quarter_fundamentals` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_provider_q_acquisition` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_result_calendar` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_operational_action` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_event` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_ttm` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_score` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_valuation` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_migration_audit` | `EXPECTED_AND_IMPLEMENTED` |
| `v3_resolution_issue` | `EXPECTED_AND_IMPLEMENTED` |

No `EXPECTED_BUT_MISSING` or `IMPLEMENTED_BUT_NOT_IN_CONTRACT` canonical V3 tables remain.

The external raw-cache table is intentionally excluded from the canonical V3 DB by default and is
created only through the explicit raw-cache initializer path.

Schema review covered primary keys, foreign keys, unique constraints, check constraints, not-null
columns, defaults, and indexes. The persisted locked enum constraints are covered by tests for:

- Q lifecycle
- provider values
- provider acquisition result
- SEC assurance state
- operational action type
- operational action status
- resolution issue status

`v3_result_calendar.calendar_status`, `v3_event.event_type`, and `v3_resolution_issue.issue_type`
remain unconstrained text because the locked Phase 1 contract does not define closed value sets for
those fields.

## Repository And Helper Coverage

Implemented and tested:

- Legacy-authority universe derivation with positive V2 `BANK` and `INSURANCE` exclusions.
- Canonical fiscal identity and V3 work-unit key serialization:
  `market|ticker|fiscal_year|fiscal_quarter`.
- V3 plan version constant: `fundamental_result_check_plan_v3`.
- Canonical scope hash with normalization, de-duplication, lexical ordering, and SHA-256 stability.
- NULL-preserving canonical fundamentals writes:
  NULL target plus non-null incoming fills, same non-null noops, different non-null conflicts without
  overwrite, and incoming NULL noops.
- `Q_CORE_FIELDS_READY` as source/provenance-agnostic canonical field readiness.
- Pure derivations for FCF, ordinary-company EBITDA, total debt, and net debt.
- Calendar comparison metadata by approximate three-calendar-month maximum overlap.

Calendar comparison helpers return analytical metadata only and do not create or infer fiscal
identity.

## Real Universe Selector

Read-only selector run against real local source DBs:

| Metric | Count |
| --- | ---: |
| Legacy authority candidates | 2,936 |
| Included | 2,812 |
| Positive profile exclusions | 124 |
| Other decisions | 0 |

This matches the locked approved initial V3 universe projection.

## SQLite Integrity

Fresh temp V3 DB integrity:

| Check | Result |
| --- | --- |
| `PRAGMA quick_check;` | `ok` |
| `PRAGMA foreign_key_check;` | `0 rows` |

## Validation Commands

```bash
PYTHONPATH=. .venv/bin/python -m pytest swingmaster/tests/test_fundamentals_v3_foundation.py
PYTHONPATH=. .venv/bin/python -m pytest swingmaster/tests/test_fundamental_migrations.py
PYTHONPATH=. .venv/bin/python -m swingmaster.cli.run_fundamentals_v3_schema --db-path /tmp/swingmaster_v3_phase1_smoke.db
```

Latest focused results:

- Phase 1 foundation tests: 14 passed
- Fundamental migration regression tests: 11 passed
- Temp CLI schema smoke: 15 canonical V3 tables validated

## Closure

Phase 1 is closed for foundation purposes. The next implementation phase may begin Yahoo bootstrap
work, but this closure does not itself perform bootstrap, provider acquisition, production V3
population, Check V3 cutover, Update V3 cutover, scheduler changes, or RawCandle integration.
