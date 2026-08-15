from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from swingmaster.fundamentals.operational_v2_provider_adapters import build_operational_v2_provider_adapter_factory
from swingmaster.fundamentals.selected_v2_work_unit_executor import (
    SelectedV2WorkUnitInput,
    execute_selected_v2_work_unit,
)
from swingmaster.fundamentals_v2 import simfin_api_shares, simfin_api_statements
from swingmaster.tests.test_selected_v2_work_unit_executor import _company, _connect, _quarter


def test_operational_adapters_fill_selected_work_unit_from_simfin_caches(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    with _connect(db) as conn:
        _company(conn, ticker="AJG")
        _quarter(conn, company_id=1, quarter_id=10)
        _persist_statement_cache(conn)
        _persist_share_cache(conn)
        conn.commit()

        adapters = build_operational_v2_provider_adapter_factory()(_work_unit())
        result = execute_selected_v2_work_unit(conn, _work_unit(), provider_adapters=adapters)

        assert result.execution_status == "SUCCESS_CORE_COMPLETE"
        assert result.providers_called == []
        assert result.cache_hits == ["SIMFIN_API_STATEMENTS_CACHE", "SIMFIN_API_SHARES_CACHE"]
        assert {field: result.canonical_fields_written[field] for field in _work_unit().missing_core_fields} == {
            "revenue": 4003000000.0,
            "ebitda": 946000000.0,
            "free_cashflow": -41000000.0,
            "shares_outstanding": 256300000.0,
        }
        assert result.opportunistic_fields_written == {
            "operating_cashflow": 100000000.0,
            "capex": -141000000.0,
            "cash": 1000000.0,
        }
        row = conn.execute(
            """
            SELECT revenue, ebitda, free_cashflow, shares_outstanding
            FROM rc_v2_fundamental_quarterly
            WHERE quarter_id=10
            """
        ).fetchone()
        assert dict(row) == {
            "revenue": 4003000000.0,
            "ebitda": 946000000.0,
            "free_cashflow": -41000000.0,
            "shares_outstanding": 256300000.0,
        }


def test_operational_statement_adapter_rejects_wrong_quarter_identity(tmp_path: Path) -> None:
    db = tmp_path / "v2.db"
    with _connect(db) as conn:
        _company(conn, ticker="AJG")
        _quarter(conn, company_id=1, quarter_id=10)
        _persist_statement_cache(conn, report_date="2026-03-31", fiscal_quarter="Q1")
        conn.commit()

        adapters = build_operational_v2_provider_adapter_factory()(_work_unit())
        result = execute_selected_v2_work_unit(conn, _work_unit(), provider_adapters=adapters)

        assert result.providers_called == []
        assert result.canonical_fields_written == {}
        assert result.provenance_rows_written == 0
        assert result.execution_status == "SUCCESS_PARTIAL_RETRY"


def _work_unit() -> SelectedV2WorkUnitInput:
    return SelectedV2WorkUnitInput(
        work_unit_key="usa|AJG|2026|Q2",
        market="usa",
        ticker="AJG",
        company_id=1,
        company_profile="ORDINARY",
        fiscal_year=2026,
        fiscal_quarter="Q2",
        canonical_report_date="2026-06-30",
        target_period_end_date="2026-06-30",
        identity_evidence={},
        preflight_v2_action="ENRICH_CORE",
        missing_core_fields=("revenue", "ebitda", "free_cashflow", "shares_outstanding"),
        opportunistic_gaps=(),
        provider_due_summary={"simfin": "CACHE_AVAILABLE"},
        run_id="RUN",
    )


def _persist_statement_cache(conn: sqlite3.Connection, *, report_date: str = "2026-06-30", fiscal_quarter: str = "Q2") -> None:
    simfin_api_statements.ensure_schema(conn)
    payload = [
        {
            "id": 66898,
            "ticker": "AJG",
            "name": "Arthur J Gallagher & Co",
            "currency": "USD",
            "statements": [
                {
                    "statement": "PL",
                    "columns": ["Fiscal Year", "Fiscal Period", "Report Date", "Revenue", "Operating Income (Loss)"],
                    "data": [[2026, fiscal_quarter, report_date, 4003000000.0, 809000000.0]],
                },
                {
                    "statement": "BS",
                    "columns": ["Fiscal Year", "Fiscal Period", "Report Date", "Cash, Cash Equivalents & Short Term Investments"],
                    "data": [[2026, fiscal_quarter, report_date, 1000000.0]],
                },
                {
                    "statement": "CF",
                    "columns": [
                        "Fiscal Year",
                        "Fiscal Period",
                        "Report Date",
                        "Depreciation & Amortization",
                        "Net Cash from Operating Activities",
                        "Change in Fixed Assets & Intangibles",
                    ],
                    "data": [[2026, fiscal_quarter, report_date, 137000000.0, 100000000.0, -141000000.0]],
                },
            ],
        }
    ]
    simfin_api_statements.persist_fetch_result(
        conn,
        market="usa",
        run_id="CACHE",
        result={
            "ticker": "AJG",
            "retrieved_at_utc": "2026-08-14T00:00:00Z",
            "http_status": 200,
            "provider_status": "SUCCESS",
            "payload_json": json.dumps(payload, sort_keys=True),
            "safe_headers_json": "{}",
        },
    )


def _persist_share_cache(conn: sqlite3.Connection) -> None:
    simfin_api_shares.ensure_schema(conn)
    payload = [{"ticker": "AJG", "pid": 66898, "data": [{"pid": 66898, "endDate": "2026-06-30", "value": 256300000.0}]}]
    simfin_api_shares.persist_fetch_result(
        conn,
        market="usa",
        run_id="CACHE",
        result={
            "ticker": "AJG",
            "retrieved_at_utc": "2026-08-13T00:00:00Z",
            "http_status": 200,
            "provider_status": "SUCCESS",
            "payload_json": json.dumps(payload, sort_keys=True),
            "safe_headers_json": "{}",
        },
    )
