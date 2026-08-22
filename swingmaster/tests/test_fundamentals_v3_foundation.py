from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.fundamentals.v3_helpers import (
    V3_CHECK_PLAN_VERSION,
    derive_free_cashflow,
    derive_calendar_comparison_period,
    derive_net_debt,
    derive_ordinary_ebitda,
    derive_q_core_fields_ready,
    derive_total_debt,
    make_v3_work_unit_key,
    parse_v3_work_unit_key,
    v3_canonical_scope_hash,
)
from swingmaster.fundamentals.v3_repositories import (
    ACTION_STATUSES,
    ACTION_TYPES,
    PROVIDER_RESULTS,
    Q_LIFECYCLES,
    SEC_CONFIRMATION_STATES,
    V3CompanyRepository,
    V3FundamentalsRepository,
    V3MigrationAuditRepository,
    V3OperationalActionRepository,
    V3OutputRepository,
    V3ProviderAcquisitionRepository,
    V3QuarterRepository,
    V3RawCacheRepository,
    V3ResolutionIssueRepository,
)
from swingmaster.fundamentals.v3_schema import V3_REQUIRED_TABLES, apply_v3_schema, run_v3_schema_migration, validate_v3_schema


NOW = "2026-08-21T00:00:00Z"


def test_v3_schema_creates_required_tables_and_constraints(tmp_path: Path) -> None:
    db_path = tmp_path / "rc_fundamentals_v3.db"

    migration_file, table_count = run_v3_schema_migration(db_path)
    second_file, second_count = run_v3_schema_migration(db_path)

    assert migration_file == second_file
    assert table_count == len(V3_REQUIRED_TABLES)
    assert second_count == len(V3_REQUIRED_TABLES)
    with sqlite3.connect(db_path) as conn:
        table_names = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                """
            )
        }
        assert set(V3_REQUIRED_TABLES).issubset(table_names)
        assert "v3_raw_cache_entry" not in table_names
        assert validate_v3_schema(conn) == len(V3_REQUIRED_TABLES)
        assert conn.execute("SELECT version FROM v3_schema_version").fetchall() == [(1,)]


def test_apply_v3_schema_enables_foreign_keys_for_current_connection() -> None:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)

    try:
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    finally:
        conn.close()


def test_v3_schema_can_create_explicit_external_raw_cache(tmp_path: Path) -> None:
    db_path = tmp_path / "rc_fundamentals_v3_raw.db"

    _migration_file, table_count = run_v3_schema_migration(db_path, include_raw_cache=True)

    assert table_count == len(V3_REQUIRED_TABLES) + 1
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='v3_raw_cache_entry'
            """
        ).fetchone() == ("v3_raw_cache_entry",)
        columns = {
            str(row[1])
            for row in conn.execute(
                """
                PRAGMA table_info(v3_raw_cache_entry)
                """
            )
        }
        assert {"status", "error_message", "observed_at_utc", "payload_hash", "payload_json"}.issubset(columns)


def test_v3_raw_cache_repository_preserves_fetch_status_and_error(tmp_path: Path) -> None:
    db_path = tmp_path / "rc_fundamentals_v3_raw.db"
    repo = V3RawCacheRepository(db_path)

    payload_hash = repo.put_payload(
        provider="YAHOO",
        provider_symbol="AAPL",
        fetch_run_id="RUN1",
        payload_json='{"quarterly_income_stmt":{"columns":["2026-06-30"]}}',
        status="ERROR",
        error_message="rate limited",
        observed_at_utc=NOW,
    )

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT provider, provider_symbol, fetch_run_id, payload_hash, status, error_message, observed_at_utc
            FROM v3_raw_cache_entry
            """
        ).fetchone()
    assert row == ("YAHOO", "AAPL", "RUN1", payload_hash, "ERROR", "rate limited", NOW)


def test_v3_schema_rejects_unsupported_schema_version(tmp_path: Path) -> None:
    db_path = tmp_path / "bad_version.db"
    run_v3_schema_migration(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute("INSERT INTO v3_schema_version (version, applied_at_utc) VALUES (999, ?)", (NOW,))
        conn.commit()
        try:
            validate_v3_schema(conn)
        except RuntimeError as exc:
            assert str(exc) == "FUNDAMENTALS_V3_UNSUPPORTED_SCHEMA_VERSION:1,999"
        else:
            raise AssertionError("unsupported schema version was accepted")


def test_v3_sqlite_integrity_on_fresh_temp_db(tmp_path: Path) -> None:
    db_path = tmp_path / "integrity.db"
    run_v3_schema_migration(db_path)

    with sqlite3.connect(db_path) as conn:
        quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]
        fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert quick_check == "ok"
    assert fk_rows == []


def test_v3_schema_check_constraints_match_locked_python_constants() -> None:
    sql = Path("swingmaster/infra/sqlite/migrations/038_create_fundamentals_v3_schema.sql").read_text()

    for value in Q_LIFECYCLES:
        assert f"'{value}'" in sql
    for value in PROVIDER_RESULTS:
        assert f"'{value}'" in sql
    for value in SEC_CONFIRMATION_STATES:
        assert f"'{value}'" in sql
    for value in ACTION_TYPES:
        assert f"'{value}'" in sql
    for value in ACTION_STATUSES:
        assert f"'{value}'" in sql


def test_v3_company_repository_derives_legacy_authority_universe_and_excludes_positive_profiles() -> None:
    legacy = sqlite3.connect(":memory:")
    v2 = sqlite3.connect(":memory:")
    legacy.executescript(
        """
        CREATE TABLE rc_fundamental_quarterly(ticker TEXT, period_end_date TEXT);
        INSERT INTO rc_fundamental_quarterly VALUES ('aaa', '2026-06-30');
        INSERT INTO rc_fundamental_quarterly VALUES ('BANK', '2026-06-30');
        INSERT INTO rc_fundamental_quarterly VALUES ('INS', '2026-06-30');
        INSERT INTO rc_fundamental_quarterly VALUES ('LEGACYONLY', '2026-06-30');
        """
    )
    v2.executescript(
        """
        CREATE TABLE rc_v2_company(market TEXT, ticker TEXT, company_profile TEXT);
        INSERT INTO rc_v2_company VALUES ('usa', 'AAA', 'ORDINARY');
        INSERT INTO rc_v2_company VALUES ('usa', 'BANK', 'BANK');
        INSERT INTO rc_v2_company VALUES ('usa', 'INS', 'INSURANCE');
        """
    )
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    companies = V3CompanyRepository(conn)
    audit = V3MigrationAuditRepository(conn)

    candidates = companies.derive_legacy_authority_universe(legacy, v2_conn=v2)
    summary = companies.apply_universe_candidates(candidates, migration_run_id="run-1", audit_repo=audit, now_utc=NOW)

    assert summary == {"included": 2, "excluded": 2}
    tickers = [row["ticker"] for row in companies.list_active_companies()]
    assert tickers == ["AAA", "LEGACYONLY"]
    audit_rows = conn.execute("SELECT decision, COUNT(*) FROM v3_migration_audit GROUP BY decision").fetchall()
    assert {row[0]: row[1] for row in audit_rows} == {"EXCLUDE_POSITIVE_PROFILE": 2, "INCLUDE": 2}


def test_v3_quarter_and_fundamentals_repositories_preserve_non_null_values() -> None:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    company_id = V3CompanyRepository(conn).admit_company(market="usa", ticker="aaa", now_utc=NOW)
    quarter_id = V3QuarterRepository(conn).upsert_quarter(
        company_id=company_id,
        fiscal_year=2026,
        fiscal_quarter="q2",
        period_end_date="2026-06-30",
        publish_date="2026-07-20",
        market_availability_date="2026-07-20",
        q_lifecycle="ENRICHING",
        now_utc=NOW,
    )
    fundamentals = V3FundamentalsRepository(conn)

    first = fundamentals.write_null_preserving_fields(
        quarter_id=quarter_id,
        values={"revenue": 100.0, "ebitda": 20.0, "free_cashflow": 9.0},
        accepted_source_provider="YAHOO",
        accepted_at_utc=NOW,
    )
    second = fundamentals.write_null_preserving_fields(
        quarter_id=quarter_id,
        values={"revenue": 101.0, "ebitda": 20.0, "cash": 30.0},
        accepted_source_provider="LEGACY",
        accepted_at_utc=NOW,
    )
    row = fundamentals.get_fundamentals(quarter_id=quarter_id)

    assert first["filled"] == ["revenue", "ebitda", "free_cashflow"]
    assert second == {"inserted": [], "filled": ["cash"], "preserved": ["ebitda"], "conflicts": ["revenue"]}
    assert row["revenue"] == 100.0
    assert row["ebitda"] == 20.0
    assert row["cash"] == 30.0


def test_v3_fundamentals_repository_incoming_null_is_noop() -> None:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    company_id = V3CompanyRepository(conn).admit_company(market="usa", ticker="aaa", now_utc=NOW)
    quarter_id = V3QuarterRepository(conn).upsert_quarter(company_id=company_id, fiscal_year=2026, fiscal_quarter="Q2", now_utc=NOW)
    repo = V3FundamentalsRepository(conn)

    first = repo.write_null_preserving_fields(
        quarter_id=quarter_id,
        values={"revenue": None, "cash": 10.0},
        accepted_source_provider="YAHOO",
        accepted_at_utc=NOW,
    )
    second = repo.write_null_preserving_fields(
        quarter_id=quarter_id,
        values={"revenue": None, "cash": None},
        accepted_source_provider="LEGACY",
        accepted_at_utc=NOW,
    )
    row = repo.get_fundamentals(quarter_id=quarter_id)

    assert first["filled"] == ["cash"]
    assert second == {"inserted": [], "filled": [], "preserved": [], "conflicts": []}
    assert row["revenue"] is None
    assert row["cash"] == 10.0


def test_v3_fundamentals_storage_accepts_yahoo_bootstrap_value_contract() -> None:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    company_id = V3CompanyRepository(conn).admit_company(market="usa", ticker="AAPL", now_utc=NOW)
    quarter_id = V3QuarterRepository(conn).upsert_quarter(
        company_id=company_id,
        fiscal_year=2026,
        fiscal_quarter="Q3",
        period_end_date="2026-06-30",
        publish_date="2026-07-30",
        market_availability_date="2026-07-30",
        q_lifecycle="OPERATIONALLY_SETTLED",
        now_utc=NOW,
    )
    values = {
        "revenue": 109417000000.0,
        "gross_profit": 54770000000.0,
        "operating_income": 35695000000.0,
        "ebit": 35695000000.0,
        "ebitda": 39015000000.0,
        "net_income": 29789000000.0,
        "operating_cashflow": 34369000000.0,
        "capex": -2455000000.0,
        "free_cashflow": 31914000000.0,
        "cash": 39544000000.0,
        "total_debt": 84344000000.0,
        "shares_outstanding": 14687356000.0,
    }

    result = V3FundamentalsRepository(conn).write_null_preserving_fields(
        quarter_id=quarter_id,
        values=values,
        accepted_source_provider="YAHOO",
        accepted_at_utc=NOW,
        update_run_id="YAHOO_BOOTSTRAP_TEST",
        derivation_method="YAHOO_DIRECT_OR_PROVIDER_NORMALIZED",
    )
    row = V3FundamentalsRepository(conn).get_fundamentals(quarter_id=quarter_id)

    assert sorted(result["filled"]) == sorted(values)
    for field_name, expected_value in values.items():
        assert row[field_name] == expected_value
    assert row["accepted_source_provider"] == "YAHOO"
    assert row["derivation_method"] == "YAHOO_DIRECT_OR_PROVIDER_NORMALIZED"


def test_v3_provider_actions_resolution_and_outputs() -> None:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    company_id = V3CompanyRepository(conn).admit_company(market="usa", ticker="aaa", now_utc=NOW)
    quarter_id = V3QuarterRepository(conn).upsert_quarter(company_id=company_id, fiscal_year=2026, fiscal_quarter="Q2", now_utc=NOW)

    provider_repo = V3ProviderAcquisitionRepository(conn)
    provider_repo.upsert_outcome(
        quarter_id=quarter_id,
        provider="YAHOO",
        acquisition_result="FAILED",
        next_retry_at_utc="2026-08-21T01:00:00Z",
        attempt_count=1,
        now_utc=NOW,
    )
    assert len(provider_repo.list_due(as_of_utc="2026-08-21T02:00:00Z", provider="YAHOO")) == 1

    action_repo = V3OperationalActionRepository(conn)
    action_id = action_repo.upsert_action(
        action_type="RETRY_PROVIDER",
        company_id=company_id,
        quarter_id=quarter_id,
        provider="YAHOO",
        due_at_utc="2026-08-21T01:00:00Z",
        now_utc=NOW,
    )
    same_action_id = action_repo.upsert_action(
        action_type="RETRY_PROVIDER",
        company_id=company_id,
        quarter_id=quarter_id,
        provider="YAHOO",
        due_at_utc="2026-08-21T03:00:00Z",
        now_utc=NOW,
    )
    assert same_action_id == action_id
    assert len(action_repo.list_due_actions(as_of_utc="2026-08-21T02:00:00Z")) == 0

    issue_id = V3ResolutionIssueRepository(conn).create_issue(
        issue_type="UNRESOLVED_CONFLICT",
        quarter_id=quarter_id,
        field_name="revenue",
        source_details={"YAHOO": 101, "LEGACY": 100},
        now_utc=NOW,
    )
    V3ResolutionIssueRepository(conn).resolve_issue(issue_id=issue_id, resolution="accepted legacy", now_utc=NOW)
    assert conn.execute("SELECT status FROM v3_resolution_issue WHERE issue_id=?", (issue_id,)).fetchone()[0] == "RESOLVED"

    output_id = V3OutputRepository(conn).upsert_score(
        company_id=company_id,
        as_of_quarter_id=quarter_id,
        score_model_version="ebitda-v1",
        score_ready=False,
        fundamental_score=55.0,
        output={"reason": "history_missing"},
        now_utc=NOW,
    )
    assert output_id > 0


def test_v3_helpers_core_readiness_and_calendar_comparison() -> None:
    valid = {
        "revenue": 1.0,
        "ebitda": 2.0,
        "free_cashflow": 3.0,
        "cash": 4.0,
        "total_debt": 5.0,
        "shares_outstanding": 6.0,
    }
    assert derive_q_core_fields_ready(
        valid,
        market="usa",
        ticker="AAA",
        fiscal_year=2026,
        fiscal_quarter="Q2",
    )
    for missing_field in ("revenue", "ebitda", "free_cashflow", "cash", "total_debt"):
        row = dict(valid)
        row[missing_field] = None
        assert not derive_q_core_fields_ready(
            row,
            market="usa",
            ticker="AAA",
            fiscal_year=2026,
            fiscal_quarter="Q2",
        )
    for shares in (None, 0.0, -1.0):
        row = dict(valid)
        row["shares_outstanding"] = shares
        assert not derive_q_core_fields_ready(
            row,
            market="usa",
            ticker="AAA",
            fiscal_year=2026,
            fiscal_quarter="Q2",
        )
    assert not derive_q_core_fields_ready(
        valid,
        market="usa",
        ticker="AAA",
        fiscal_year=2026,
        fiscal_quarter="BAD",
    )
    assert derive_q_core_fields_ready(
        valid,
        market="usa",
        ticker="AAA",
        fiscal_year=2026,
        fiscal_quarter="Q2",
        profile="ORDINARY",
    )

    shifted = derive_calendar_comparison_period("2026-01-31")
    assert shifted.derived_period_start_date == "2025-10-31"
    assert shifted.calendar_comparison_year == 2025
    assert shifted.calendar_comparison_quarter == "Q4"
    assert shifted.calendar_comparison_method == "APPROX_3_CALENDAR_MONTHS_FROM_PERIOD_END"
    assert shifted.calendar_comparison_quality == "APPROX_OVERLAP"

    invalid = derive_calendar_comparison_period(None)
    assert invalid.calendar_comparison_year is None
    assert invalid.calendar_comparison_quality == "INSUFFICIENT_DATES"


def test_v3_work_unit_key_and_scope_hash_contract() -> None:
    assert V3_CHECK_PLAN_VERSION == "fundamental_result_check_plan_v3"

    key = make_v3_work_unit_key(market="USA", ticker="aapl", fiscal_year="2026", fiscal_quarter="2")
    parsed = parse_v3_work_unit_key(key)

    assert key == "usa|AAPL|2026|Q2"
    assert parsed.market == "usa"
    assert parsed.ticker == "AAPL"
    assert parsed.fiscal_year == 2026
    assert parsed.fiscal_quarter == "Q2"
    for bad_key in ("usa|AAPL|2026", "usa|AAPL|2026|Q5", "usa||2026|Q2", "usa|AAPL|x|Q2"):
        try:
            parse_v3_work_unit_key(bad_key)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid key accepted: {bad_key}")

    hash_a = v3_canonical_scope_hash(["usa|AAPL|2026|Q2", "USA|msft|2026|2", "usa|AAPL|2026|Q2"])
    hash_b = v3_canonical_scope_hash(["usa|MSFT|2026|Q2", "usa|AAPL|2026|Q2"])
    hash_c = v3_canonical_scope_hash(["usa|MSFT|2026|Q3", "usa|AAPL|2026|Q2"])

    assert hash_a == hash_b
    assert hash_a == "d8148b8771e4c1fcc0641eaa3617edc2061581e99c8e286c568c40b99e61f3f2"
    assert hash_c != hash_a


def test_v3_derivation_helpers_are_null_and_sign_safe() -> None:
    assert derive_free_cashflow(100.0, -30.0) == 70.0
    assert derive_free_cashflow(100.0, 0.0) == 100.0
    assert derive_free_cashflow(None, -30.0) is None
    assert derive_free_cashflow(100.0, None) is None

    assert derive_ordinary_ebitda(20.0, 5.0) == 25.0
    assert derive_ordinary_ebitda(20.0, 0.0) == 20.0
    assert derive_ordinary_ebitda(None, 5.0) is None
    assert derive_ordinary_ebitda(20.0, None) is None

    assert derive_total_debt(10.0, 90.0) == 100.0
    assert derive_total_debt(None, 90.0) == 90.0
    assert derive_total_debt(10.0, None) == 10.0
    assert derive_total_debt(0.0, 0.0) == 0.0
    assert derive_total_debt(None, None) is None

    assert derive_net_debt(100.0, 25.0) == 75.0
    assert derive_net_debt(100.0, 0.0) == 100.0
    assert derive_net_debt(50.0, 75.0) == -25.0
    assert derive_net_debt(None, 25.0) is None
    assert derive_net_debt(100.0, None) is None


def test_v3_calendar_comparison_edge_cases_do_not_create_fiscal_identity() -> None:
    cases = {
        "2025-12-31": ("2025-09-30", 2025, "Q4"),
        "2026-01-31": ("2025-10-31", 2025, "Q4"),
        "2026-02-28": ("2025-11-28", 2026, "Q1"),
        "2024-02-29": ("2023-11-29", 2024, "Q1"),
        "2026-05-31": ("2026-02-28", 2026, "Q2"),
        "2026-03-31": ("2025-12-31", 2026, "Q1"),
    }
    for period_end, expected in cases.items():
        result = derive_calendar_comparison_period(period_end)
        assert (
            result.derived_period_start_date,
            result.calendar_comparison_year,
            result.calendar_comparison_quarter,
        ) == expected
        assert not hasattr(result, "fiscal_year")
        assert not hasattr(result, "fiscal_quarter")

    invalid = derive_calendar_comparison_period("not-a-date")
    assert invalid.calendar_comparison_year is None
    assert invalid.calendar_comparison_quality == "INSUFFICIENT_DATES"
