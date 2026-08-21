from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from swingmaster.fundamentals.v3_repositories import V3CompanyRepository, V3RawCacheRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_yahoo_bootstrap import (
    ApprovedV3Company,
    YahooMetadataEnricher,
    build_v3_yahoo_migration_candidates,
    fetch_yahoo_to_v3_raw_cache,
    load_approved_v3_companies,
    normalize_yahoo_raw_cache_result,
    replay_v3_yahoo_bootstrap_from_raw_cache,
    run_v3_yahoo_bootstrap_adapter,
    select_approved_v3_yahoo_companies,
)


NOW = "2026-08-21T12:00:00Z"


class _FakeYahooClient:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.symbols: list[str] = []

    def get_raw_payload(self, symbol: str) -> dict[str, Any]:
        self.symbols.append(symbol)
        return self.payload


class _FailingYahooClient:
    def get_raw_payload(self, symbol: str) -> dict[str, Any]:
        raise RuntimeError(f"provider failed:{symbol}")


def test_v3_yahoo_adapter_writes_external_raw_cache_and_builds_deterministic_candidate(tmp_path: Path) -> None:
    raw_cache_db = tmp_path / "rc_fundamentals_v3_raw.db"
    company = ApprovedV3Company(company_id=7, market="usa", ticker="AAPL", provider_symbol="AAPL")
    client = _FakeYahooClient(_payload())
    v2_conn = _v2_metadata_conn()

    summary = run_v3_yahoo_bootstrap_adapter(
        companies=[company],
        raw_cache_repo=V3RawCacheRepository(raw_cache_db),
        metadata_enricher=YahooMetadataEnricher(v2_conn=v2_conn),
        fetch_run_id="PHASE2C_TEST",
        client=client,
        observed_at_utc=NOW,
        delay_seconds=0,
    )

    assert summary["raw_ok"] == 1
    assert summary["normalized_rows"] == 1
    assert summary["migration_candidates"] == 1
    assert summary["metadata_rejections"] == 0
    candidate = summary["candidates"][0]
    assert candidate.work_unit_key == "usa|AAPL|2026|Q3"
    assert candidate.candidate_key == f"usa|AAPL|2026|Q3|YAHOO|{candidate.payload_hash}"
    assert candidate.period_end_date == "2026-06-30"
    assert candidate.publish_date == "2026-07-31"
    assert candidate.market_availability_date == "2026-07-31"
    assert candidate.values["revenue"] == 109417000000.0
    assert candidate.values["free_cashflow"] == 31914000000.0
    assert "operating_income" not in candidate.values
    assert candidate.provider_details["operating_income"] == 35695000000.0
    assert candidate.provider_details["fiscal_identity_source"] == "V2_EXACT_REPORT_DATE"
    assert candidate.provider_details["publish_date_source"] == "V2_PUBLISH_DATE"
    assert client.symbols == ["AAPL"]

    with sqlite3.connect(raw_cache_db) as conn:
        row = conn.execute(
            """
            SELECT provider, provider_symbol, fetch_run_id, status, error_message, observed_at_utc, payload_json
            FROM v3_raw_cache_entry
            """
        ).fetchone()
    assert row[:6] == ("YAHOO", "AAPL", "PHASE2C_TEST", "OK", None, NOW)
    assert json.loads(row[6])["quarterly_income_stmt"]["columns"] == ["2026-06-30"]


def test_v3_yahoo_adapter_rejects_rows_without_exact_metadata(tmp_path: Path) -> None:
    raw_cache_db = tmp_path / "rc_fundamentals_v3_raw.db"
    company = ApprovedV3Company(company_id=8, market="usa", ticker="MSFT", provider_symbol="MSFT")
    raw_result = fetch_yahoo_to_v3_raw_cache(
        company,
        raw_cache_repo=V3RawCacheRepository(raw_cache_db),
        fetch_run_id="PHASE2C_TEST",
        client=_FakeYahooClient(_payload()),
        observed_at_utc=NOW,
    )
    normalized = normalize_yahoo_raw_cache_result(raw_result)

    candidates, rejections = build_v3_yahoo_migration_candidates(
        normalized,
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_metadata_conn(ticker="AAPL")),
    )

    assert candidates == []
    assert len(rejections) == 1
    assert rejections[0].reason == "METADATA_NOT_RESOLVED"
    assert rejections[0].period_end_date == "2026-06-30"


def test_v3_yahoo_adapter_preserves_provider_error_without_candidates(tmp_path: Path) -> None:
    raw_cache_db = tmp_path / "rc_fundamentals_v3_raw.db"
    company = ApprovedV3Company(company_id=7, market="usa", ticker="AAPL", provider_symbol="AAPL")

    summary = run_v3_yahoo_bootstrap_adapter(
        companies=[company],
        raw_cache_repo=V3RawCacheRepository(raw_cache_db),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_metadata_conn()),
        fetch_run_id="PHASE2C_TEST",
        client=_FailingYahooClient(),
        observed_at_utc=NOW,
        delay_seconds=0,
    )

    assert summary["raw_error"] == 1
    assert summary["normalized_rows"] == 0
    assert summary["migration_candidates"] == 0
    with sqlite3.connect(raw_cache_db) as conn:
        row = conn.execute(
            """
            SELECT status, error_message
            FROM v3_raw_cache_entry
            """
        ).fetchone()
    assert row == ("ERROR", "provider failed:AAPL")


def test_v3_yahoo_adapter_preserves_empty_response_without_candidates(tmp_path: Path) -> None:
    raw_cache_db = tmp_path / "rc_fundamentals_v3_raw.db"
    company = ApprovedV3Company(company_id=7, market="usa", ticker="AAPL", provider_symbol="AAPL")

    summary = run_v3_yahoo_bootstrap_adapter(
        companies=[company],
        raw_cache_repo=V3RawCacheRepository(raw_cache_db),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_metadata_conn()),
        fetch_run_id="PHASE2C_TEST",
        client=_FakeYahooClient(
            {
                "info": {},
                "fast_info": {},
                "quarterly_income_stmt": {"index": [], "columns": [], "data": []},
                "quarterly_balance_sheet": {"index": [], "columns": [], "data": []},
                "quarterly_cashflow": {"index": [], "columns": [], "data": []},
            }
        ),
        observed_at_utc=NOW,
        delay_seconds=0,
    )

    assert summary["raw_empty"] == 1
    assert summary["normalized_rows"] == 0
    assert summary["migration_candidates"] == 0
    with sqlite3.connect(raw_cache_db) as conn:
        row = conn.execute(
            """
            SELECT status, error_message
            FROM v3_raw_cache_entry
            """
        ).fetchone()
    assert row == ("EMPTY", None)


def test_v3_yahoo_adapter_dry_run_does_not_write_raw_cache(tmp_path: Path) -> None:
    raw_cache_db = tmp_path / "rc_fundamentals_v3_raw.db"
    company = ApprovedV3Company(company_id=7, market="usa", ticker="AAPL", provider_symbol="AAPL")

    summary = run_v3_yahoo_bootstrap_adapter(
        companies=[company],
        raw_cache_repo=V3RawCacheRepository(raw_cache_db),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_metadata_conn()),
        fetch_run_id="PHASE2C_TEST",
        client=_FakeYahooClient(_payload()),
        observed_at_utc=NOW,
        delay_seconds=0,
        dry_run=True,
    )

    assert summary["dry_run"] == 1
    assert summary["migration_candidates"] == 1
    assert not raw_cache_db.exists()


def test_v3_yahoo_raw_cache_replay_is_deterministic_without_provider_call(tmp_path: Path) -> None:
    raw_cache_db = tmp_path / "rc_fundamentals_v3_raw.db"
    company = ApprovedV3Company(company_id=7, market="usa", ticker="AAPL", provider_symbol="AAPL")
    client = _FakeYahooClient(_payload())
    live_summary = run_v3_yahoo_bootstrap_adapter(
        companies=[company],
        raw_cache_repo=V3RawCacheRepository(raw_cache_db),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_metadata_conn()),
        fetch_run_id="PHASE2C_TEST",
        client=client,
        observed_at_utc=NOW,
        delay_seconds=0,
    )

    replay_summary = replay_v3_yahoo_bootstrap_from_raw_cache(
        companies=[company],
        raw_cache_repo=V3RawCacheRepository(raw_cache_db),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_metadata_conn()),
        fetch_run_id="PHASE2C_TEST",
    )

    assert client.symbols == ["AAPL"]
    assert replay_summary["raw_ok"] == live_summary["raw_ok"] == 1
    assert replay_summary["normalized_rows"] == live_summary["normalized_rows"] == 1
    assert replay_summary["metadata_rejections"] == live_summary["metadata_rejections"] == 0
    assert replay_summary["migration_candidates"] == live_summary["migration_candidates"] == 1
    assert replay_summary["candidates"] == live_summary["candidates"]


def test_v3_yahoo_metadata_enrichment_uses_result_event_publish_when_v2_publish_missing() -> None:
    v2_conn = _v2_metadata_conn(publish_date=None)
    legacy_conn = sqlite3.connect(":memory:")
    legacy_conn.row_factory = sqlite3.Row
    legacy_conn.execute(
        """
        CREATE TABLE rc_fundamental_quarter_earnings_match (
            market TEXT,
            ticker TEXT,
            period_end_date TEXT,
            announcement_date TEXT,
            effective_trading_date TEXT
        )
        """
    )
    legacy_conn.execute(
        """
        INSERT INTO rc_fundamental_quarter_earnings_match
        VALUES ('usa', 'AAPL', '2026-06-30', '2026-07-30', '2026-07-31')
        """
    )

    metadata = YahooMetadataEnricher(v2_conn=v2_conn, legacy_conn=legacy_conn).enrich(
        market="usa",
        ticker="AAPL",
        period_end_date="2026-06-30",
    )

    assert metadata is not None
    assert metadata.fiscal_year == 2026
    assert metadata.fiscal_quarter == "Q3"
    assert metadata.publish_date == "2026-07-30"
    assert metadata.market_availability_date == "2026-07-31"
    assert metadata.publish_date_source == "RESULT_EVENT_EXACT_PERIOD_MATCH"


def test_v3_yahoo_adapter_reads_approved_companies_and_does_not_write_canonical_quarters(tmp_path: Path) -> None:
    v3_conn = sqlite3.connect(":memory:")
    apply_v3_schema(v3_conn)
    company_id = V3CompanyRepository(v3_conn).admit_company(
        market="usa",
        ticker="AAPL",
        admission_source="LEGACY",
        now_utc=NOW,
    )
    v3_conn.execute(
        """
        INSERT INTO v3_provider_symbol_alias (
            company_id, provider, provider_symbol, created_at_utc, updated_at_utc
        )
        VALUES (?, 'YAHOO', 'AAPL', ?, ?)
        """,
        (company_id, NOW, NOW),
    )

    companies = load_approved_v3_companies(v3_conn, market="usa")
    summary = run_v3_yahoo_bootstrap_adapter(
        companies=companies,
        raw_cache_repo=V3RawCacheRepository(tmp_path / "rc_fundamentals_v3_raw.db"),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_metadata_conn()),
        fetch_run_id="PHASE2C_TEST",
        client=_FakeYahooClient(_payload()),
        observed_at_utc=NOW,
        delay_seconds=0,
    )

    assert companies == [ApprovedV3Company(company_id=company_id, market="usa", ticker="AAPL", provider_symbol="AAPL")]
    assert summary["migration_candidates"] == 1
    assert v3_conn.execute("SELECT COUNT(*) FROM v3_quarter").fetchone()[0] == 0
    assert v3_conn.execute("SELECT COUNT(*) FROM v3_quarter_fundamentals").fetchone()[0] == 0


def test_v3_yahoo_company_selection_enforces_approved_v3_universe() -> None:
    v3_conn = sqlite3.connect(":memory:")
    apply_v3_schema(v3_conn)
    company_repo = V3CompanyRepository(v3_conn)
    aapl_id = company_repo.admit_company(market="usa", ticker="AAPL", admission_source="LEGACY", now_utc=NOW)
    legacy_only_id = company_repo.admit_company(market="usa", ticker="LEGACY", admission_source="LEGACY", now_utc=NOW)
    v3_conn.execute(
        """
        INSERT INTO v3_provider_symbol_alias (
            company_id, provider, provider_symbol, created_at_utc, updated_at_utc
        )
        VALUES (?, 'YAHOO', 'AAPL', ?, ?)
        """,
        (aapl_id, NOW, NOW),
    )

    assert select_approved_v3_yahoo_companies(v3_conn, market="usa", ticker="AAPL") == [
        ApprovedV3Company(company_id=aapl_id, market="usa", ticker="AAPL", provider_symbol="AAPL")
    ]
    assert select_approved_v3_yahoo_companies(v3_conn, market="usa", ticker="LEGACY") == [
        ApprovedV3Company(company_id=legacy_only_id, market="usa", ticker="LEGACY", provider_symbol="LEGACY")
    ]
    for rejected in ("V2ONLY", "OSAKEDATA", "ARBITRARY"):
        try:
            select_approved_v3_yahoo_companies(v3_conn, market="usa", ticker=rejected)
        except ValueError as exc:
            assert str(exc) == f"V3_YAHOO_TICKER_NOT_APPROVED:{rejected}"
        else:
            raise AssertionError(f"Expected rejected ticker: {rejected}")


def _v2_metadata_conn(*, ticker: str = "AAPL", publish_date: str | None = "2026-07-31") -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE rc_v2_company (
            company_id INTEGER PRIMARY KEY,
            market TEXT NOT NULL,
            ticker TEXT NOT NULL,
            company_profile TEXT
        );
        CREATE TABLE rc_v2_quarter (
            quarter_id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL,
            fiscal_year INTEGER NOT NULL,
            fiscal_period TEXT NOT NULL,
            report_date TEXT NOT NULL,
            publish_date TEXT
        );
        """
    )
    conn.execute(
        "INSERT INTO rc_v2_company VALUES (1, 'usa', ?, 'ORDINARY')",
        (ticker,),
    )
    conn.execute(
        "INSERT INTO rc_v2_quarter VALUES (10, 1, 2026, 'Q3', '2026-06-30', ?)",
        (publish_date,),
    )
    return conn


def _payload() -> dict[str, Any]:
    return {
        "info": {"sharesOutstanding": None},
        "fast_info": {},
        "quarterly_income_stmt": {
            "index": [
                "Total Revenue",
                "Gross Profit",
                "Operating Income",
                "EBIT",
                "EBITDA",
                "Net Income",
            ],
            "columns": ["2026-06-30"],
            "data": [
                [109417000000.0],
                [54770000000.0],
                [35695000000.0],
                [35695000000.0],
                [39015000000.0],
                [29789000000.0],
            ],
        },
        "quarterly_balance_sheet": {
            "index": ["Cash And Cash Equivalents", "Total Debt", "Ordinary Shares Number"],
            "columns": ["2026-06-30"],
            "data": [[39544000000.0], [84344000000.0], [14687356000.0]],
        },
        "quarterly_cashflow": {
            "index": ["Operating Cash Flow", "Capital Expenditure", "Free Cash Flow"],
            "columns": ["2026-06-30"],
            "data": [[34369000000.0], [-2455000000.0], [31914000000.0]],
        },
    }
