from __future__ import annotations

import sqlite3

from swingmaster.fundamentals.v3_canonical_migration import (
    CANONICAL_FIELD_NAMES,
    V3CanonicalMigrationCandidate,
    V3CanonicalMigrationEngine,
    V3FieldPolicy,
    V3SourceApplyPolicy,
)
from swingmaster.fundamentals.v3_repositories import V3CompanyRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema


NOW = "2026-08-22T00:00:00Z"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    conn.row_factory = sqlite3.Row
    return conn


def _candidate(**overrides) -> V3CanonicalMigrationCandidate:
    values = {
        "revenue": 1000.0,
        "gross_profit": 500.0,
        "operating_income": 180.0,
        "ebit": 170.0,
        "net_income": 120.0,
        "operating_cashflow": 140.0,
        "capex": -20.0,
        "cash": 300.0,
        "shares_outstanding": 10.0,
    }
    base = {
        "source_system": "YAHOO",
        "source_record_id": "YAHOO:AAA:2026:Q1",
        "migration_run_id": "RUN-YAHOO",
        "market": "usa",
        "ticker": "AAA",
        "fiscal_year": 2026,
        "fiscal_quarter": "Q1",
        "period_end_date": "2026-03-31",
        "publish_date": None,
        "values": values,
        "approved_company_active": True,
        "derivation_inputs": {"depreciation_amortization": 30.0, "short_term_debt": 25.0, "long_term_debt": 175.0},
    }
    base.update(overrides)
    return V3CanonicalMigrationCandidate(**base)


def test_candidate_validation_and_approved_company_guard() -> None:
    conn = _conn()
    engine = V3CanonicalMigrationEngine(conn)

    try:
        _candidate(source_system="SEC")
    except ValueError as exc:
        assert str(exc) == "V3_CANONICAL_INVALID_SOURCE:SEC"
    else:
        raise AssertionError("unsupported Phase 3 source was accepted")

    summary = engine.apply_source_batch(
        [_candidate(approved_company_active=None)],
        source="YAHOO",
        migration_run_id="RUN-YAHOO",
        now_utc=NOW,
    ).to_dict()

    assert summary["rows"]["candidate_rows_rejected"] == 1
    assert conn.execute("SELECT COUNT(*) FROM v3_company").fetchone()[0] == 0
    assert conn.execute("SELECT issue_type FROM v3_resolution_issue").fetchone()[0] == "OTHER_MIGRATION_REVIEW"


def test_yahoo_like_seed_creates_company_quarter_all_fields_and_allows_publish_null() -> None:
    conn = _conn()
    summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
        [_candidate()],
        source="YAHOO",
        migration_run_id="RUN-YAHOO",
        now_utc=NOW,
    ).to_dict()

    q = conn.execute("SELECT * FROM v3_quarter").fetchone()
    f = conn.execute("SELECT * FROM v3_quarter_fundamentals WHERE quarter_id = ?", (q["quarter_id"],)).fetchone()

    assert summary["rows"]["companies_created"] == 1
    assert summary["rows"]["canonical_quarters_created"] == 1
    assert summary["metadata"]["PUBLISH_DATE_SKIPPED_NULL"] == 1
    assert q["publish_date"] is None
    assert q["period_end_date"] == "2026-03-31"
    assert f["operating_income"] == 180.0
    assert f["free_cashflow"] == 120.0
    assert f["ebitda"] == 210.0
    assert f["total_debt"] == 200.0
    for field_name in CANONICAL_FIELD_NAMES:
        assert field_name in f.keys()


def test_v2_like_enrichment_fills_publish_and_null_fields_confirms_and_conflicts() -> None:
    conn = _conn()
    engine = V3CanonicalMigrationEngine(conn)
    engine.apply_source_batch([_candidate(values={"revenue": 1000.0, "cash": 300.0})], source="YAHOO", migration_run_id="RUN-YAHOO", now_utc=NOW)

    v2 = _candidate(
        source_system="V2",
        source_record_id="V2:AAA:2026:Q1",
        migration_run_id="RUN-V2",
        publish_date="2026-04-25",
        approved_company_active=None,
        values={"revenue": 1000.0, "gross_profit": 550.0, "cash": 301.5, "net_income": None},
    )
    summary = engine.apply_source_batch([v2], source="V2", migration_run_id="RUN-V2", now_utc=NOW).to_dict()
    q = conn.execute("SELECT publish_date FROM v3_quarter").fetchone()
    f = conn.execute("SELECT revenue, gross_profit, cash, net_income FROM v3_quarter_fundamentals").fetchone()

    assert summary["rows"]["existing_canonical_quarters_matched"] == 1
    assert summary["metadata"]["PUBLISH_DATE_SET"] == 1
    assert summary["field_contributions"]["revenue"]["FIELD_CONFIRMED_SAME"] == 1
    assert summary["field_contributions"]["gross_profit"]["FIELD_FILLED_FROM_NULL"] == 1
    assert summary["field_contributions"]["cash"]["FIELD_CONFLICT"] == 1
    assert q["publish_date"] == "2026-04-25"
    assert f["gross_profit"] == 550.0
    assert f["cash"] == 300.0
    assert f["net_income"] is None


def test_rounding_equivalent_is_deterministic_and_does_not_overwrite() -> None:
    conn = _conn()
    engine = V3CanonicalMigrationEngine(conn)
    engine.apply_source_batch([_candidate(values={"revenue": 1000.0})], source="YAHOO", migration_run_id="RUN-YAHOO", now_utc=NOW)
    policy = V3SourceApplyPolicy(source="V2", field_policies={"revenue": V3FieldPolicy(absolute_tolerance=0.01, relative_tolerance=0.0)})

    summary = engine.apply_source_batch(
        [
            _candidate(
                source_system="V2",
                source_record_id="V2:AAA:2026:Q1:ROUND",
                migration_run_id="RUN-V2",
                values={"revenue": 1000.005},
                approved_company_active=None,
            )
        ],
        source="V2",
        migration_run_id="RUN-V2",
        policy=policy,
        now_utc=NOW,
    ).to_dict()

    assert summary["field_contributions"]["revenue"]["FIELD_ROUNDING_EQUIVALENT"] == 1
    assert conn.execute("SELECT revenue FROM v3_quarter_fundamentals").fetchone()[0] == 1000.0


def test_period_and_publication_date_conflicts_create_issues_without_replacement() -> None:
    conn = _conn()
    engine = V3CanonicalMigrationEngine(conn)
    engine.apply_source_batch([_candidate(publish_date="2026-04-20")], source="YAHOO", migration_run_id="RUN-YAHOO", now_utc=NOW)

    summary = engine.apply_source_batch(
        [
            _candidate(
                source_system="LEGACY",
                source_record_id="LEGACY:AAA:2026:Q1",
                migration_run_id="RUN-LEGACY",
                period_end_date="2026-03-30",
                publish_date="2026-04-21",
                values={},
                approved_company_active=None,
            )
        ],
        source="LEGACY",
        migration_run_id="RUN-LEGACY",
        now_utc=NOW,
    ).to_dict()
    q = conn.execute("SELECT period_end_date, publish_date FROM v3_quarter").fetchone()

    assert summary["metadata"]["PERIOD_DATE_CONFLICT"] == 1
    assert summary["metadata"]["PUBLISH_DATE_CONFLICT"] == 1
    assert q["period_end_date"] == "2026-03-31"
    assert q["publish_date"] == "2026-04-20"
    assert conn.execute("SELECT COUNT(*) FROM v3_resolution_issue").fetchone()[0] == 2


def test_legacy_like_deep_history_inactive_company_and_pre_1999_rejection() -> None:
    conn = _conn()
    company_id = V3CompanyRepository(conn).admit_company(market="usa", ticker="OLD", active=False, now_utc=NOW)
    engine = V3CanonicalMigrationEngine(conn)
    summary = engine.apply_source_batch(
        [
            _candidate(
                source_system="LEGACY",
                source_record_id="LEGACY:OLD:2000:Q1",
                migration_run_id="RUN-LEGACY",
                ticker="OLD",
                fiscal_year=2000,
                period_end_date="2000-03-31",
                values={"revenue": 50.0},
                approved_company_active=None,
            ),
            _candidate(
                source_system="LEGACY",
                source_record_id="LEGACY:OLD:1998:Q4",
                migration_run_id="RUN-LEGACY",
                ticker="OLD",
                fiscal_year=1998,
                fiscal_quarter="Q4",
                period_end_date="1998-12-31",
                values={"revenue": 40.0},
                approved_company_active=None,
            ),
        ],
        source="LEGACY",
        migration_run_id="RUN-LEGACY",
        now_utc=NOW,
    ).to_dict()

    assert summary["rows"]["canonical_quarters_created"] == 1
    assert summary["rows"]["candidate_rows_rejected"] == 1
    assert conn.execute("SELECT active FROM v3_company WHERE company_id = ?", (company_id,)).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 1


def test_cava_like_complementary_work_units_merge_one_q_with_safe_period_variant() -> None:
    conn = _conn()
    engine = V3CanonicalMigrationEngine(conn)
    a = _candidate(ticker="CAVA", source_record_id="YAHOO:CAVA:A", values={"revenue": 438.27, "operating_income": 34.14})
    b = _candidate(
        ticker="CAVA",
        source_record_id="YAHOO:CAVA:B",
        period_end_date="2026-04-19",
        values={"cash": 100.0, "shares_outstanding": 118.32},
        period_date_policy="SAFE_VARIANT",
    )

    summary = engine.apply_source_batch([a, b], source="YAHOO", migration_run_id="RUN-YAHOO", now_utc=NOW).to_dict()
    f = conn.execute("SELECT revenue, operating_income, cash, shares_outstanding FROM v3_quarter_fundamentals").fetchone()

    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 1
    assert summary["metadata"]["PERIOD_DATE_SAFE_VARIANT"] == 1
    assert f["revenue"] == 438.27
    assert f["cash"] == 100.0


def test_neup_like_mapping_correction_uses_supplied_canonical_identity() -> None:
    conn = _conn()
    engine = V3CanonicalMigrationEngine(conn)
    correction = _candidate(
        ticker="NEUP",
        source_record_id="YAHOO:NEUP:2025-09-30",
        fiscal_year=2026,
        fiscal_quarter="Q1",
        period_end_date="2025-09-30",
        raw_evidence_ref="USER_SUPPLIED_OFFICIAL_FISCAL_MAPPING",
        candidate_issue_type="FISCAL_MAPPING_CORRECTION",
    )

    summary = engine.apply_source_batch([correction], source="YAHOO", migration_run_id="RUN-YAHOO", now_utc=NOW).to_dict()
    q = conn.execute("SELECT fiscal_year, fiscal_quarter, period_end_date FROM v3_quarter").fetchone()

    assert summary["candidate_results"][0]["work_unit_key"] == "usa|NEUP|2026|Q1"
    assert q["fiscal_year"] == 2026
    assert q["fiscal_quarter"] == "Q1"
    assert q["period_end_date"] == "2025-09-30"


def test_lfcr_like_provider_variant_is_audited_and_excluded() -> None:
    conn = _conn()
    summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
        [
            _candidate(
                ticker="LFCR",
                source_record_id="YAHOO:LFCR:2025-09-30",
                fiscal_year=2025,
                fiscal_quarter="Q4",
                period_end_date="2025-09-30",
                candidate_can_create_quarter=False,
                candidate_issue_type="TRANSITION_PERIOD_VARIANT",
            )
        ],
        source="YAHOO",
        migration_run_id="RUN-YAHOO",
        now_utc=NOW,
    ).to_dict()

    assert summary["rows"]["candidate_rows_rejected"] == 1
    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 0
    assert conn.execute("SELECT issue_type FROM v3_resolution_issue").fetchone()[0] == "TRANSITION_PERIOD_VARIANT"


def test_idempotent_reapply_and_source_run_isolation() -> None:
    conn = _conn()
    engine = V3CanonicalMigrationEngine(conn)
    fixture = [_candidate(publish_date="2026-04-20")]
    first = engine.apply_source_batch(fixture, source="YAHOO", migration_run_id="RUN-YAHOO", now_utc=NOW).to_dict()
    second = engine.apply_source_batch(fixture, source="YAHOO", migration_run_id="RUN-YAHOO", now_utc=NOW).to_dict()
    v2 = _candidate(
        source_system="V2",
        source_record_id="V2:AAA:2026:Q1",
        migration_run_id="RUN-V2",
        values={"revenue": 1002.0},
        approved_company_active=None,
    )
    engine.apply_source_batch([v2], source="V2", migration_run_id="RUN-V2", now_utc=NOW)

    assert first["rows"]["canonical_quarters_created"] == 1
    assert second["rows"]["existing_canonical_quarters_matched"] == 1
    assert second["field_contributions"]["revenue"]["FIELD_CONFIRMED_SAME"] == 1
    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM v3_resolution_issue WHERE issue_type='NON_NULL_FIELD_CONFLICT'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(DISTINCT migration_run_id) FROM v3_migration_audit").fetchone()[0] == 2


def test_dry_apply_rolls_back_all_writes_but_returns_plan_summary() -> None:
    conn = _conn()
    summary = V3CanonicalMigrationEngine(conn).apply_source_batch(
        [_candidate()],
        source="YAHOO",
        migration_run_id="RUN-YAHOO",
        dry_apply=True,
        now_utc=NOW,
    ).to_dict()

    assert summary["rows"]["canonical_quarters_created"] == 1
    assert conn.execute("SELECT COUNT(*) FROM v3_company").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM v3_migration_audit").fetchone()[0] == 0


def test_candidate_transaction_rolls_back_on_uncontrolled_failure() -> None:
    conn = _conn()
    engine = V3CanonicalMigrationEngine(conn)

    try:
        engine.apply_source_batch(
            [_candidate(period_end_date="not-a-date")],
            source="YAHOO",
            migration_run_id="RUN-YAHOO",
            now_utc=NOW,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("invalid period date was accepted")

    assert conn.execute("SELECT COUNT(*) FROM v3_company").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM v3_migration_audit").fetchone()[0] == 0


def test_integrity_after_fixture_runs() -> None:
    conn = _conn()
    engine = V3CanonicalMigrationEngine(conn)
    engine.apply_source_batch([_candidate()], source="YAHOO", migration_run_id="RUN-YAHOO", now_utc=NOW)
    result = engine.validate_integrity()

    assert result == {
        "quick_check": "ok",
        "foreign_key_check_rows": 0,
        "duplicate_company_fy_fq": 0,
        "orphan_fundamentals": 0,
    }
