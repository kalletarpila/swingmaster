from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from swingmaster.fundamentals.dual_store_update_preflight import (
    V2_CREATE_QUARTER_AND_FILL_CORE,
    V2_ENRICH_CORE,
    V2_MAINTENANCE_REQUIRED,
    V2_NOOP_CORE_CURRENT,
    V2_RETRY_PROVIDER,
    work_unit_key,
)
from swingmaster.fundamentals.selected_v2_work_unit_executor import (
    SelectedV2WorkUnitInput,
    StaticProviderAdapter,
    build_sec_revenue_candidate,
    build_simfin_share_candidate,
    build_simfin_statement_candidates,
    build_yahoo_field_candidate,
    execute_selected_v2_work_unit,
    field_candidate,
)


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    _create_schema(conn)
    return conn


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE rc_v2_import_run (
            import_run_id TEXT PRIMARY KEY,
            market TEXT NOT NULL,
            simfin_dir TEXT NOT NULL,
            builder_version TEXT NOT NULL,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT
        );
        CREATE TABLE rc_v2_company (
            company_id INTEGER PRIMARY KEY,
            market TEXT NOT NULL,
            ticker TEXT,
            company_profile TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE rc_v2_quarter (
            quarter_id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL,
            fiscal_year INTEGER NOT NULL,
            fiscal_period TEXT NOT NULL,
            report_date TEXT NOT NULL,
            quarter_identity_source TEXT NOT NULL,
            has_income INTEGER NOT NULL DEFAULT 0,
            has_balance INTEGER NOT NULL DEFAULT 0,
            has_cashflow INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            UNIQUE (company_id, fiscal_year, fiscal_period, report_date),
            FOREIGN KEY (company_id) REFERENCES rc_v2_company(company_id)
        );
        CREATE TABLE rc_v2_fundamental_quarterly (
            quarter_id INTEGER PRIMARY KEY,
            revenue REAL,
            gross_profit REAL,
            operating_income REAL,
            depreciation_amortization REAL,
            ebit REAL,
            ebitda REAL,
            net_income REAL,
            operating_cashflow REAL,
            capex REAL,
            free_cashflow REAL,
            cash REAL,
            total_debt REAL,
            shares_outstanding REAL,
            weighted_average_shares_basic REAL,
            weighted_average_shares_diluted REAL,
            available_canonical_field_count INTEGER NOT NULL,
            has_income INTEGER NOT NULL,
            has_balance INTEGER NOT NULL,
            has_cashflow INTEGER NOT NULL,
            seed_status TEXT NOT NULL,
            missing_seed_fields_json TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            FOREIGN KEY (quarter_id) REFERENCES rc_v2_quarter(quarter_id)
        );
        CREATE TABLE rc_v2_fundamental_field_source (
            quarter_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            provider TEXT NOT NULL,
            provider_field TEXT NOT NULL,
            source_dataset TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            transformation TEXT NOT NULL,
            source_value TEXT,
            import_run_id TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            PRIMARY KEY (quarter_id, field_name, provider),
            FOREIGN KEY (quarter_id) REFERENCES rc_v2_quarter(quarter_id),
            FOREIGN KEY (import_run_id) REFERENCES rc_v2_import_run(import_run_id)
        );
        """
    )


def _company(conn: sqlite3.Connection, ticker: str = "TEST", *, company_id: int = 1, profile: str = "ORDINARY") -> None:
    conn.execute(
        "INSERT INTO rc_v2_company (company_id, market, ticker, company_profile, active) VALUES (?, 'usa', ?, ?, 1)",
        (company_id, ticker, profile),
    )
    conn.commit()


def _quarter(
    conn: sqlite3.Connection,
    *,
    company_id: int = 1,
    quarter_id: int = 10,
    fy: int = 2026,
    fq: str = "Q2",
    report_date: str = "2026-06-30",
) -> None:
    conn.execute(
        """
        INSERT INTO rc_v2_quarter
        (quarter_id, company_id, fiscal_year, fiscal_period, report_date, quarter_identity_source, created_at_utc, updated_at_utc)
        VALUES (?, ?, ?, ?, ?, 'fixture', '2026-08-15T00:00:00Z', '2026-08-15T00:00:00Z')
        """,
        (quarter_id, company_id, fy, fq, report_date),
    )
    conn.execute(
        """
        INSERT INTO rc_v2_fundamental_quarterly
        (quarter_id, available_canonical_field_count, has_income, has_balance, has_cashflow, seed_status, missing_seed_fields_json, created_at_utc, updated_at_utc)
        VALUES (?, 0, 0, 0, 0, 'fixture', '[]', '2026-08-15T00:00:00Z', '2026-08-15T00:00:00Z')
        """,
        (quarter_id,),
    )
    conn.commit()


def _work_unit(action: str = V2_ENRICH_CORE, *, run_id: str = "run-1", due: bool = True, profile: str = "ORDINARY") -> SelectedV2WorkUnitInput:
    return SelectedV2WorkUnitInput(
        work_unit_key=work_unit_key("usa", "TEST", 2026, "Q2"),
        market="usa",
        ticker="TEST",
        company_id=1,
        company_profile=profile,
        fiscal_year=2026,
        fiscal_quarter="Q2",
        canonical_report_date="2026-06-30",
        target_period_end_date="2026-06-30",
        identity_evidence={"fixture": True},
        preflight_v2_action=action,
        missing_core_fields=("revenue", "ebitda", "free_cashflow", "shares_outstanding"),
        opportunistic_gaps=("cash",),
        provider_due_summary={"simfin": "DUE_FOR_UPDATE_PROCESSING"} if due else {"simfin": "BACKOFF_NOT_DUE"},
        run_id=run_id,
    )


def _simfin_candidates(**overrides: float) -> list:
    values = {
        "revenue": 100.0,
        "ebitda": 20.0,
        "free_cashflow": 8.0,
        "operating_cashflow": 12.0,
        "capex": -4.0,
        "cash": 50.0,
        "total_debt": 30.0,
    }
    values.update(overrides)
    return build_simfin_statement_candidates(
        provider="SIMFIN_API_STATEMENTS",
        fiscal_year=2026,
        fiscal_quarter="Q2",
        report_date="2026-06-30",
        values=values,
        source_observation_id="simfin:TEST:2026Q2",
        payload_sha256="abc123",
    )


def test_executor_refuses_non_executable_action_and_unsupported_profile(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "v2.db")
    _company(conn)
    _quarter(conn)

    with pytest.raises(RuntimeError, match="V2_WORK_UNIT_ACTION_NOT_EXECUTABLE"):
        execute_selected_v2_work_unit(conn, _work_unit(V2_NOOP_CORE_CURRENT))
    with pytest.raises(RuntimeError, match="V2_WORK_UNIT_ACTION_NOT_EXECUTABLE"):
        execute_selected_v2_work_unit(conn, _work_unit(V2_MAINTENANCE_REQUIRED))
    with pytest.raises(RuntimeError, match="V2_WORK_UNIT_COMPANY_PROFILE_UNSUPPORTED"):
        execute_selected_v2_work_unit(conn, _work_unit(V2_ENRICH_CORE, profile="BANK"))


def test_executor_requires_existing_matching_company_row_before_structure_write(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "v2.db")
    conn.execute("PRAGMA foreign_keys=OFF")

    with pytest.raises(RuntimeError, match="V2_WORK_UNIT_COMPANY_NOT_FOUND"):
        execute_selected_v2_work_unit(
            conn,
            _work_unit(V2_CREATE_QUARTER_AND_FILL_CORE),
            provider_adapters=[StaticProviderAdapter("SIMFIN_FIXTURE", _simfin_candidates())],
        )

    assert conn.execute("SELECT COUNT(*) FROM rc_v2_quarter").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_quarterly").fetchone()[0] == 0


def test_create_structure_fill_core_opportunistic_and_replay_idempotent(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "v2.db")
    _company(conn)
    candidates = _simfin_candidates() + build_simfin_share_candidate(
        fiscal_year=2026,
        fiscal_quarter="Q2",
        report_date="2026-06-30",
        shares_outstanding=1000.0,
        source_observation_id="shares:TEST:2026Q2",
        payload_sha256="def456",
    )
    adapter = StaticProviderAdapter("SIMFIN_FIXTURE", candidates)

    first = execute_selected_v2_work_unit(conn, _work_unit(V2_CREATE_QUARTER_AND_FILL_CORE), provider_adapters=[adapter])
    second = execute_selected_v2_work_unit(conn, _work_unit(V2_ENRICH_CORE, run_id="run-1"), provider_adapters=[adapter])
    row = conn.execute(
        """
        SELECT revenue, ebitda, free_cashflow, shares_outstanding, cash, total_debt, operating_cashflow, capex
        FROM rc_v2_fundamental_quarterly
        """
    ).fetchone()

    assert first.structure_created is True
    assert first.fundamental_shell_created is True
    assert first.core_complete_after is True
    assert first.canonical_fields_written["revenue"] == 100.0
    assert first.opportunistic_fields_written["cash"] == 50.0
    assert first.provenance_rows_written == 8
    assert second.canonical_fields_written == {}
    assert second.provenance_rows_written == 0
    assert conn.execute("SELECT COUNT(*) FROM rc_v2_quarter").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM rc_v2_fundamental_field_source").fetchone()[0] == 8
    assert dict(row)["shares_outstanding"] == 1000.0


def test_selected_scope_poison_payload_does_not_touch_other_quarters(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "v2.db")
    _company(conn)
    _quarter(conn, quarter_id=10, fy=2026, fq="Q2", report_date="2026-06-30")
    _quarter(conn, quarter_id=9, fy=2026, fq="Q1", report_date="2026-03-31")
    poison = [
        field_candidate(
            field="revenue",
            value=999.0,
            provider="YAHOO",
            provider_field="Total Revenue",
            source_dataset="legacy_yahoo_raw",
            source_file="fixture",
            source_file_sha256="bad",
            source_observation_id="wrong-q",
            transformation="none",
            fiscal_year=2026,
            fiscal_quarter="Q1",
            report_date="2026-03-31",
            validation_tier="SAFE_SCOPED",
        )
    ]
    adapter = StaticProviderAdapter("YAHOO_FIXTURE", _simfin_candidates(revenue=111.0) + poison)

    result = execute_selected_v2_work_unit(conn, _work_unit(), provider_adapters=[adapter])

    assert result.canonical_fields_written["revenue"] == 111.0
    assert result.unrelated_quarter_canonical_write_count == 0
    assert conn.execute("SELECT revenue FROM rc_v2_fundamental_quarterly WHERE quarter_id=9").fetchone()[0] is None
    assert any(row.rejection_reason == "QUARTER_IDENTITY_MISMATCH" for row in result.rejections)


def test_non_null_conflict_preserves_canonical_and_replay_stable(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "v2.db")
    _company(conn)
    _quarter(conn)
    conn.execute("UPDATE rc_v2_fundamental_quarterly SET revenue=100.0, ebitda=20.0 WHERE quarter_id=10")
    conn.commit()
    candidates = build_yahoo_field_candidate(
        field="revenue",
        value=200.0,
        fiscal_year=2026,
        fiscal_quarter="Q2",
        report_date="2026-06-30",
        provider_field="Total Revenue",
        source_observation_id="yahoo-rev",
        payload_sha256="hash",
        validation_tier="ACCEPTED_RISK",
    ) + build_yahoo_field_candidate(
        field="ebitda",
        value=25.0,
        fiscal_year=2026,
        fiscal_quarter="Q2",
        report_date="2026-06-30",
        provider_field="EBITDA",
        source_observation_id="yahoo-ebitda",
        payload_sha256="hash",
        validation_tier="ACCEPTED_RISK",
    )
    adapter = StaticProviderAdapter("YAHOO_FIXTURE", candidates)

    first = execute_selected_v2_work_unit(conn, _work_unit(), provider_adapters=[adapter])
    second = execute_selected_v2_work_unit(conn, _work_unit(run_id="run-2"), provider_adapters=[adapter])

    assert conn.execute("SELECT revenue, ebitda FROM rc_v2_fundamental_quarterly WHERE quarter_id=10").fetchone()[:] == (100.0, 20.0)
    assert len(first.conflicts) == 2
    assert first.provenance_rows_written == 0
    assert second.canonical_fields_written == {}


def test_provider_call_gate_core_complete_does_not_call_for_opportunistic_gap(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "v2.db")
    _company(conn)
    _quarter(conn)
    conn.execute(
        """
        UPDATE rc_v2_fundamental_quarterly
        SET revenue=1, ebitda=2, free_cashflow=3, shares_outstanding=4
        WHERE quarter_id=10
        """
    )
    conn.commit()
    adapter = StaticProviderAdapter("SHOULD_NOT_RUN", _simfin_candidates(), calls_network=True)

    result = execute_selected_v2_work_unit(conn, _work_unit(due=True), provider_adapters=[adapter])

    assert result.providers_considered == []
    assert result.providers_called == []
    assert result.canonical_fields_written == {}
    assert conn.execute("SELECT cash FROM rc_v2_fundamental_quarterly WHERE quarter_id=10").fetchone()[0] is None


def test_core_incomplete_backoff_not_due_does_not_call_provider(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "v2.db")
    _company(conn)
    _quarter(conn)
    adapter = StaticProviderAdapter("BACKOFF_PROVIDER", _simfin_candidates(), calls_network=True)

    result = execute_selected_v2_work_unit(conn, _work_unit(due=False), provider_adapters=[adapter])

    assert result.execution_status == "NO_ELIGIBLE_PROVIDER_WORK"
    assert result.providers_considered == []
    assert result.canonical_fields_written == {}


def test_provider_failure_and_alternate_provider_success(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "v2.db")
    _company(conn)
    _quarter(conn)
    failing = StaticProviderAdapter("SIMFIN_FAIL", failure="TRANSIENT_503", cache_hit=False)
    yahoo = StaticProviderAdapter(
        "YAHOO_FIXTURE",
        build_yahoo_field_candidate(
            field="revenue",
            value=100.0,
            fiscal_year=2026,
            fiscal_quarter="Q2",
            report_date="2026-06-30",
            provider_field="Total Revenue",
            source_observation_id="yahoo-rev",
            payload_sha256="hash",
            validation_tier="SAFE_SCOPED",
        ),
    )

    result = execute_selected_v2_work_unit(conn, _work_unit(), provider_adapters=[failing, yahoo])

    assert result.provider_failures == [{"provider": "SIMFIN_FAIL", "error": "TRANSIENT_503"}]
    assert result.canonical_fields_written == {"revenue": 100.0}
    assert result.retry_required is True


def test_sec_revenue_candidate_and_simfin_ebit_rejection(tmp_path: Path) -> None:
    conn = _connect(tmp_path / "v2.db")
    _company(conn)
    _quarter(conn)
    candidates = build_sec_revenue_candidate(
        revenue=123.0,
        fiscal_year=2026,
        fiscal_quarter="Q2",
        report_date="2026-06-30",
        source_observation_id="sec-safe",
        payload_sha256="sec-hash",
    ) + [
        field_candidate(
            field="ebit",
            value=12.0,
            provider="SIMFIN_API_STATEMENTS",
            provider_field="Operating Income",
            source_dataset="simfin_statements",
            source_file="SIMFIN_API_RAW",
            source_file_sha256="hash",
            source_observation_id="simfin-ebit",
            transformation="none",
            fiscal_year=2026,
            fiscal_quarter="Q2",
            report_date="2026-06-30",
            validation_tier="SAFE_SCOPED",
        )
    ]

    result = execute_selected_v2_work_unit(conn, _work_unit(), provider_adapters=[StaticProviderAdapter("MIXED", candidates)])

    assert result.canonical_fields_written == {"revenue": 123.0}
    assert any(row.rejection_reason == "SIMFIN_OPERATING_INCOME_NOT_EBIT" for row in result.rejections)
