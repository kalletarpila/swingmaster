from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from swingmaster.fundamentals.v3_repositories import V3CompanyRepository, V3RawCacheRepository
from swingmaster.fundamentals.v3_schema import apply_v3_schema
from swingmaster.fundamentals.v3_yahoo_bootstrap_rollout import (
    build_rollout_plan,
    run_v3_yahoo_bootstrap_rollout,
    select_rollout_companies,
    validate_temp_path,
)
from swingmaster.fundamentals.v3_yahoo_bootstrap import YahooMetadataEnricher


NOW = "2026-08-21T12:00:00Z"


class _FakeYahooClient:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.symbols: list[str] = []

    def get_raw_payload(self, symbol: str) -> dict[str, Any]:
        self.symbols.append(symbol)
        return self.payload


class _FailOnceYahooClient:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.calls: list[str] = []

    def get_raw_payload(self, symbol: str) -> dict[str, Any]:
        self.calls.append(symbol)
        if self.calls.count(symbol) == 1:
            raise RuntimeError(f"temporary failure:{symbol}")
        return self.payload


def test_rollout_requires_explicit_bound_for_fetch_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swingmaster.fundamentals.v3_yahoo_bootstrap_rollout.temp_root", lambda: tmp_path)
    v3_conn = _v3_conn(("AAPL", "MSFT"))

    with pytest.raises(ValueError, match="V3_YAHOO_ROLLOUT_REQUIRES_BOUND"):
        select_rollout_companies(v3_conn, market="usa")

    assert [company.ticker for company in select_rollout_companies(v3_conn, market="usa", limit=1)] == ["AAPL"]


def test_rollout_plan_is_deterministic_and_path_safety_is_fail_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swingmaster.fundamentals.v3_yahoo_bootstrap_rollout.temp_root", lambda: tmp_path)
    companies = select_rollout_companies(_v3_conn(("MSFT", "AAPL")), market="usa", limit=2)

    first = build_rollout_plan(companies)
    second = build_rollout_plan(companies)

    assert first == second
    assert first["work_keys"] == ["usa|AAPL|YAHOO|AAPL", "usa|MSFT|YAHOO|MSFT"]
    assert first["duplicate_work_key_count"] == 0
    assert validate_temp_path(tmp_path / "ok" / "checkpoint.json") == (tmp_path / "ok" / "checkpoint.json").resolve()
    with pytest.raises(ValueError, match="V3_YAHOO_ROLLOUT_PATH_OUTSIDE_TEMP"):
        validate_temp_path(tmp_path.parent / "escape.json")


def test_rollout_processes_bounded_companies_and_writes_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swingmaster.fundamentals.v3_yahoo_bootstrap_rollout.temp_root", lambda: tmp_path)
    companies = select_rollout_companies(_v3_conn(("AAPL", "MSFT")), market="usa", limit=2)
    raw_cache_db = tmp_path / "raw.db"

    payload, exit_code = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=V3RawCacheRepository(raw_cache_db),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL", "MSFT"))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "checkpoint.json",
        summary_json=tmp_path / "summary.json",
        candidates_jsonl=tmp_path / "candidates.jsonl",
        rejections_jsonl=tmp_path / "rejections.jsonl",
        progress_log=tmp_path / "progress.log",
        client=_FakeYahooClient(_payload()),
        delay_seconds=0,
    )

    assert exit_code == 0
    assert payload["complete"] is True
    assert payload["summary"]["processed_company_count"] == 2
    assert payload["summary"]["status_counts"] == {"CANDIDATE_READY": 2}
    assert payload["summary"]["migration_candidates"] == 2
    assert payload["per_ticker_results"][0]["work_key"] == "usa|AAPL|YAHOO|AAPL"
    assert payload["per_ticker_results"][0]["attempt_count"] == 1
    assert len(_jsonl(tmp_path / "candidates.jsonl")) == 2
    assert (tmp_path / "rejections.jsonl").read_text(encoding="utf-8") == ""
    assert json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))["selected_tickers"] == ["AAPL", "MSFT"]
    with sqlite3.connect(raw_cache_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM v3_raw_cache_entry").fetchone()[0] == 2


def test_rollout_dry_run_emits_plan_without_provider_calls_or_raw_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swingmaster.fundamentals.v3_yahoo_bootstrap_rollout.temp_root", lambda: tmp_path)
    companies = select_rollout_companies(_v3_conn(("AAPL",)), market="usa", ticker="AAPL")
    client = _FakeYahooClient(_payload())

    payload, exit_code = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=V3RawCacheRepository(tmp_path / "raw.db"),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL",))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "checkpoint.json",
        summary_json=tmp_path / "summary.json",
        candidates_jsonl=tmp_path / "candidates.jsonl",
        rejections_jsonl=tmp_path / "rejections.jsonl",
        client=client,
        delay_seconds=0,
        dry_run=True,
    )

    assert exit_code == 0
    assert client.symbols == []
    assert payload["plan"]["company_count"] == 1
    assert payload["summary"]["status_counts"] == {"PLANNED": 1}
    assert payload["summary"]["migration_candidates"] == 0
    assert not (tmp_path / "raw.db").exists()


def test_rollout_replay_uses_existing_raw_cache_without_provider_call(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swingmaster.fundamentals.v3_yahoo_bootstrap_rollout.temp_root", lambda: tmp_path)
    companies = select_rollout_companies(_v3_conn(("AAPL",)), market="usa", ticker="AAPL")
    raw_cache_repo = V3RawCacheRepository(tmp_path / "raw.db")
    live_payload, _ = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=raw_cache_repo,
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL",))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "live_checkpoint.json",
        summary_json=tmp_path / "live_summary.json",
        candidates_jsonl=tmp_path / "live_candidates.jsonl",
        rejections_jsonl=tmp_path / "live_rejections.jsonl",
        client=_FakeYahooClient(_payload()),
        delay_seconds=0,
    )

    replay_payload, exit_code = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=raw_cache_repo,
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL",))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "replay_checkpoint.json",
        summary_json=tmp_path / "replay_summary.json",
        candidates_jsonl=tmp_path / "replay_candidates.jsonl",
        rejections_jsonl=tmp_path / "replay_rejections.jsonl",
        client=_FailOnceYahooClient(_payload()),
        delay_seconds=0,
        replay_raw_cache=True,
    )

    assert exit_code == 0
    assert replay_payload["summary"] == live_payload["summary"]
    assert _jsonl(tmp_path / "replay_candidates.jsonl") == _jsonl(tmp_path / "live_candidates.jsonl")


def test_rollout_segment_cap_leaves_checkpoint_incomplete_then_resume_finishes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swingmaster.fundamentals.v3_yahoo_bootstrap_rollout.temp_root", lambda: tmp_path)
    companies = select_rollout_companies(_v3_conn(("AAPL", "MSFT", "NVDA")), market="usa", limit=3)
    client = _FakeYahooClient(_payload())

    first, _ = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=V3RawCacheRepository(tmp_path / "raw.db"),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL", "MSFT", "NVDA"))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "checkpoint.json",
        summary_json=tmp_path / "summary.json",
        candidates_jsonl=tmp_path / "candidates.jsonl",
        rejections_jsonl=tmp_path / "rejections.jsonl",
        client=client,
        delay_seconds=0,
        max_tickers_this_run=2,
    )
    second, _ = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=V3RawCacheRepository(tmp_path / "raw.db"),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL", "MSFT", "NVDA"))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "resume_checkpoint.json",
        summary_json=tmp_path / "resume_summary.json",
        candidates_jsonl=tmp_path / "resume_candidates.jsonl",
        rejections_jsonl=tmp_path / "resume_rejections.jsonl",
        resume_from_json=tmp_path / "checkpoint.json",
        client=client,
        delay_seconds=0,
    )

    assert first["complete"] is False
    assert first["summary"]["processed_company_count"] == 2
    assert second["complete"] is True
    assert second["summary"]["processed_company_count"] == 3
    assert client.symbols == ["AAPL", "MSFT", "NVDA"]


def test_rollout_resume_skips_success_and_retries_source_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swingmaster.fundamentals.v3_yahoo_bootstrap_rollout.temp_root", lambda: tmp_path)
    companies = select_rollout_companies(_v3_conn(("AAPL",)), market="usa", ticker="AAPL")
    client = _FailOnceYahooClient(_payload())

    first, _ = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=V3RawCacheRepository(tmp_path / "raw.db"),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL",))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "checkpoint.json",
        summary_json=tmp_path / "summary.json",
        candidates_jsonl=tmp_path / "candidates.jsonl",
        rejections_jsonl=tmp_path / "rejections.jsonl",
        client=client,
        delay_seconds=0,
    )
    assert first["summary"]["status_counts"] == {"SOURCE_ERROR": 1}

    skipped, _ = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=V3RawCacheRepository(tmp_path / "raw.db"),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL",))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "checkpoint_skip.json",
        summary_json=tmp_path / "summary_skip.json",
        candidates_jsonl=tmp_path / "candidates_skip.jsonl",
        rejections_jsonl=tmp_path / "rejections_skip.jsonl",
        resume_from_json=tmp_path / "checkpoint.json",
        client=client,
        delay_seconds=0,
    )
    assert skipped["summary"]["status_counts"] == {"SOURCE_ERROR": 1}
    assert client.calls == ["AAPL"]

    retried, _ = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=V3RawCacheRepository(tmp_path / "raw.db"),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL",))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "checkpoint_retry.json",
        summary_json=tmp_path / "summary_retry.json",
        candidates_jsonl=tmp_path / "candidates_retry.jsonl",
        rejections_jsonl=tmp_path / "rejections_retry.jsonl",
        resume_from_json=tmp_path / "checkpoint.json",
        retry_failed_on_resume=True,
        client=client,
        delay_seconds=0,
    )
    assert retried["summary"]["status_counts"] == {"CANDIDATE_READY": 1}
    assert client.calls == ["AAPL", "AAPL"]
    assert len(_jsonl(tmp_path / "candidates_retry.jsonl")) == 1


def test_rollout_resume_recovers_running_and_rewrites_jsonl_idempotently(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swingmaster.fundamentals.v3_yahoo_bootstrap_rollout.temp_root", lambda: tmp_path)
    companies = select_rollout_companies(_v3_conn(("AAPL", "MSFT")), market="usa", limit=2)
    raw_cache_repo = V3RawCacheRepository(tmp_path / "raw.db")
    first, _ = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=raw_cache_repo,
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL", "MSFT"))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "checkpoint.json",
        summary_json=tmp_path / "summary.json",
        candidates_jsonl=tmp_path / "candidates.jsonl",
        rejections_jsonl=tmp_path / "rejections.jsonl",
        client=_FakeYahooClient(_payload()),
        delay_seconds=0,
    )
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text(encoding="utf-8"))
    checkpoint["per_ticker_results"][1]["status"] = "RUNNING"
    checkpoint["per_ticker_results"][1]["candidate_records"] = []
    checkpoint["per_ticker_results"][1]["candidate_keys"] = []
    (tmp_path / "interrupted.json").write_text(json.dumps(checkpoint), encoding="utf-8")

    resumed, _ = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=raw_cache_repo,
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL", "MSFT"))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "resume_checkpoint.json",
        summary_json=tmp_path / "resume_summary.json",
        candidates_jsonl=tmp_path / "resume_candidates.jsonl",
        rejections_jsonl=tmp_path / "resume_rejections.jsonl",
        resume_from_json=tmp_path / "interrupted.json",
        client=_FakeYahooClient(_payload()),
        delay_seconds=0,
    )
    rerun, _ = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=raw_cache_repo,
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL", "MSFT"))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "rerun_checkpoint.json",
        summary_json=tmp_path / "rerun_summary.json",
        candidates_jsonl=tmp_path / "rerun_candidates.jsonl",
        rejections_jsonl=tmp_path / "rerun_rejections.jsonl",
        resume_from_json=tmp_path / "resume_checkpoint.json",
        client=_FakeYahooClient(_payload()),
        delay_seconds=0,
    )

    assert first["summary"] == resumed["summary"] == rerun["summary"]
    assert len(_jsonl(tmp_path / "resume_candidates.jsonl")) == 2
    assert _jsonl(tmp_path / "resume_candidates.jsonl") == _jsonl(tmp_path / "rerun_candidates.jsonl")


def test_rollout_source_error_circuit_breaker_stops_broad_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("swingmaster.fundamentals.v3_yahoo_bootstrap_rollout.temp_root", lambda: tmp_path)
    companies = select_rollout_companies(_v3_conn(("AAPL", "MSFT", "NVDA")), market="usa", limit=3)

    payload, exit_code = run_v3_yahoo_bootstrap_rollout(
        companies=companies,
        raw_cache_repo=V3RawCacheRepository(tmp_path / "raw.db"),
        metadata_enricher=YahooMetadataEnricher(v2_conn=_v2_conn(("AAPL", "MSFT", "NVDA"))),
        run_id="PHASE2D_TEST",
        checkpoint_json=tmp_path / "checkpoint.json",
        summary_json=tmp_path / "summary.json",
        candidates_jsonl=tmp_path / "candidates.jsonl",
        rejections_jsonl=tmp_path / "rejections.jsonl",
        client=_FailOnceYahooClient(_payload()),
        delay_seconds=0,
        max_consecutive_source_errors=1,
    )

    assert exit_code == 1
    assert payload["complete"] is False
    assert payload["summary"]["processed_company_count"] == 1


def _v3_conn(tickers: tuple[str, ...]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    apply_v3_schema(conn)
    repo = V3CompanyRepository(conn)
    for ticker in tickers:
        repo.admit_company(market="usa", ticker=ticker, admission_source="LEGACY", now_utc=NOW)
    return conn


def _v2_conn(tickers: tuple[str, ...]) -> sqlite3.Connection:
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
    for index, ticker in enumerate(tickers, start=1):
        conn.execute("INSERT INTO rc_v2_company VALUES (?, 'usa', ?, 'ORDINARY')", (index, ticker))
        conn.execute(
            "INSERT INTO rc_v2_quarter VALUES (?, ?, 2026, 'Q3', '2026-06-30', '2026-07-31')",
            (index, index),
        )
    return conn


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _payload() -> dict[str, Any]:
    return {
        "info": {"sharesOutstanding": None},
        "fast_info": {},
        "quarterly_income_stmt": {
            "index": ["Total Revenue", "EBIT", "EBITDA"],
            "columns": ["2026-06-30"],
            "data": [[100.0], [20.0], [25.0]],
        },
        "quarterly_balance_sheet": {
            "index": ["Cash And Cash Equivalents", "Total Debt", "Ordinary Shares Number"],
            "columns": ["2026-06-30"],
            "data": [[30.0], [10.0], [1000.0]],
        },
        "quarterly_cashflow": {
            "index": ["Operating Cash Flow", "Capital Expenditure", "Free Cash Flow"],
            "columns": ["2026-06-30"],
            "data": [[12.0], [-2.0], [10.0]],
        },
    }
