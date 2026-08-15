from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable, Mapping

from swingmaster.fundamentals.selected_v2_work_unit_executor import (
    ProviderEvaluation,
    SelectedV2WorkUnitInput,
    SelectedWorkUnitProviderAdapter,
    build_simfin_share_candidate,
    build_simfin_statement_candidates,
)
from swingmaster.fundamentals_v2 import simfin_api_shares, simfin_api_statements


AdapterFactory = Callable[[Any], list[SelectedWorkUnitProviderAdapter]]


def build_operational_v2_provider_adapter_factory() -> AdapterFactory:
    def factory(_work_unit: Any) -> list[SelectedWorkUnitProviderAdapter]:
        return [
            SimFinStatementCacheAdapter(),
            SimFinShareCacheAdapter(),
        ]

    return factory


class SimFinStatementCacheAdapter:
    provider_name = "SIMFIN_API_STATEMENTS_CACHE"
    calls_network = False

    def evaluate(self, conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput) -> ProviderEvaluation:
        raw = simfin_api_statements.latest_successful_raw(
            conn,
            market=work_unit.market,
            ticker=work_unit.ticker,
            create_schema_if_missing=False,
        )
        if raw is None:
            return ProviderEvaluation(provider=self.provider_name, called=False, cache_hit=False, no_data=True)
        try:
            company = _statement_company_for_work_unit(conn, raw, work_unit)
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            return ProviderEvaluation(provider=self.provider_name, called=False, cache_hit=True, failure=str(exc), no_data=True)
        if company is None:
            return ProviderEvaluation(provider=self.provider_name, called=False, cache_hit=True, no_data=True)

        flat_rows = simfin_api_statements.flatten_statement_company(company)
        grouped = simfin_api_statements.row_by_statement_key(flat_rows)
        simfin_id = int(company["id"])
        target_key = (simfin_id, work_unit.fiscal_year, work_unit.fiscal_quarter, work_unit.canonical_report_date)
        quarter_rows = {
            "PL": grouped["PL"].get(target_key),
            "BS": grouped["BS"].get(target_key),
            "CF": grouped["CF"].get(target_key),
        }
        if not any(quarter_rows.values()):
            return ProviderEvaluation(provider=self.provider_name, called=False, cache_hit=True, no_data=True)
        fallback_da = simfin_api_statements.validated_pl_da_fallback_value(grouped, target_key)
        values = simfin_api_statements.map_api_ordinary_fields(
            quarter_rows,
            depreciation_amortization_fallback=fallback_da,
        )
        candidates = build_simfin_statement_candidates(
            provider=simfin_api_statements.SIMFIN_API_STATEMENTS_PROVIDER,
            fiscal_year=work_unit.fiscal_year,
            fiscal_quarter=work_unit.fiscal_quarter,
            report_date=work_unit.canonical_report_date,
            values=values,
            source_observation_id=f"rc_v2_simfin_api_raw:{raw['raw_id']}:{work_unit.fiscal_year}{work_unit.fiscal_quarter}:{work_unit.canonical_report_date}",
            payload_sha256=str(raw["payload_sha256"]),
        )
        return ProviderEvaluation(
            provider=self.provider_name,
            called=False,
            cache_hit=True,
            candidates=candidates,
            no_data=not candidates,
        )


class SimFinShareCacheAdapter:
    provider_name = "SIMFIN_API_SHARES_CACHE"
    calls_network = False

    def evaluate(self, conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput) -> ProviderEvaluation:
        raw = simfin_api_shares.latest_successful_raw(
            conn,
            market=work_unit.market,
            ticker=work_unit.ticker,
            create_schema_if_missing=False,
        )
        if raw is None:
            return ProviderEvaluation(provider=self.provider_name, called=False, cache_hit=False, no_data=True)
        try:
            observations = simfin_api_shares.parse_share_observations(json.loads(raw["payload_json"]))
        except json.JSONDecodeError as exc:
            return ProviderEvaluation(provider=self.provider_name, called=False, cache_hit=True, failure=str(exc), no_data=True)
        match = simfin_api_shares.match_observation_for_report_date(
            observations,
            ticker=work_unit.ticker,
            report_date=work_unit.canonical_report_date,
            max_age_days=simfin_api_shares.DEFAULT_MAX_AGE_DAYS,
        )
        if match is None:
            return ProviderEvaluation(provider=self.provider_name, called=False, cache_hit=True, no_data=True)
        candidates = build_simfin_share_candidate(
            fiscal_year=work_unit.fiscal_year,
            fiscal_quarter=work_unit.fiscal_quarter,
            report_date=work_unit.canonical_report_date,
            shares_outstanding=match.shares_outstanding,
            source_observation_id=f"rc_v2_simfin_api_shares_raw:{raw['raw_id']}:{match.observation_date}:{match.match_type}",
            payload_sha256=str(raw["payload_sha256"]),
        )
        return ProviderEvaluation(
            provider=self.provider_name,
            called=False,
            cache_hit=True,
            candidates=candidates,
            no_data=not candidates,
        )


def _statement_company_for_work_unit(
    conn: sqlite3.Connection,
    raw: sqlite3.Row,
    work_unit: SelectedV2WorkUnitInput,
) -> Mapping[str, Any] | None:
    companies = simfin_api_statements.parse_companies(json.loads(raw["payload_json"]))
    expected_ticker = work_unit.ticker.upper()
    expected_simfin_id = _company_simfin_id(conn=conn, work_unit=work_unit)
    for company in companies:
        provider_id = company.get("id")
        provider_ticker = str(company.get("ticker") or raw["ticker"] or "").upper()
        if expected_simfin_id and provider_id is not None:
            if int(provider_id) == expected_simfin_id:
                return company
            continue
        if provider_ticker == expected_ticker:
            return company
    return None


def _company_simfin_id(*, conn: sqlite3.Connection, work_unit: SelectedV2WorkUnitInput) -> int:
    if "simfin_id" not in _table_columns(conn, "rc_v2_company"):
        return 0
    row = conn.execute("SELECT simfin_id FROM rc_v2_company WHERE company_id=?", (work_unit.company_id,)).fetchone()
    return int(row["simfin_id"] or 0) if row is not None else 0


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
