from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import backfill_yahoo_earnings_events
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_events import (
    EarningsEventRecord,
    YahooEarningsResult,
    YahooParseDiagnostics,
    assess_earnings_coverage,
    plan_earnings_history_range,
    plan_yahoo_earnings_limit,
)


OBSERVED = "2026-07-30T14:00:00Z"


def _db(tmp_path: Path) -> Path:
    path = tmp_path / "fundamentals.db"
    run_migration(path)
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (ticker, period_end_date, revenue, run_id)
            VALUES ('AAPL', '2020-03-31', 1.0, 'RUN'),
                   ('MSFT', '2020-03-31', 1.0, 'RUN'),
                   ('AA', '2020-03-31', 1.0, 'RUN'),
                   ('NOPE', '2020-03-31', 1.0, 'RUN'),
                   ('BAD', '2020-03-31', 1.0, 'RUN')
            """
        )
        conn.commit()
    return path


def _record(ticker: str = "AAPL", observed: str = OBSERVED, reported_eps: float | None = 1.25) -> EarningsEventRecord:
    return EarningsEventRecord(
        market="usa",
        ticker=ticker,
        announcement_at="2020-04-30T16:00:00-04:00",
        announcement_date="2020-04-30",
        announcement_session="DURING_MARKET",
        is_reported=reported_eps is not None,
        reported_eps=reported_eps,
        estimated_eps=1.13,
        surprise_pct=9.97,
        source="YAHOO_FINANCE",
        source_observed_at_utc=observed,
        source_timezone="America/New_York",
    )


def _source_result(db_path: Path, ticker: str, records: tuple[EarningsEventRecord, ...] | None = None, status: str | None = None) -> YahooEarningsResult:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        range_plan = plan_earnings_history_range(conn, ticker)
    limit_plan = plan_yahoo_earnings_limit(range_plan.fetch_lower_bound or "2020-01-01", as_of_date="2026-07-30")
    output_records = (_record(ticker),) if records is None else records
    source_failed = status == "SOURCE_FAILED"
    parse_failed = status == "PARSE_FAILED"
    coverage = assess_earnings_coverage(output_records, range_plan=range_plan, source_failed=source_failed, parse_failed=parse_failed)
    return YahooEarningsResult(
        ticker=ticker,
        normalized_ticker=ticker,
        requested_limit=limit_plan.requested_limit,
        source_observed_at_utc=OBSERVED,
        records=output_records,
        diagnostics=YahooParseDiagnostics(("EPS Estimate", "Reported EPS"), len(output_records), 0, sum(1 for item in output_records if item.is_reported), 0, 0, 0),
        coverage=coverage,
        status=status or coverage.coverage_status,
        error_message="429 Too Many Requests" if source_failed else None,
    )


def _audit_json(temp_root: Path) -> Path:
    path = temp_root / "audit.json"
    rows = [
        {"ticker": "AAPL", "planning_classification": "BACKFILL_READY_PARTIAL_MARGIN_ONLY", "completed_qualifying_count": 1},
        {"ticker": "MSFT", "planning_classification": "BACKFILL_READY_PARTIAL_MARGIN_ONLY", "completed_qualifying_count": 1},
        {"ticker": "AA", "planning_classification": "BACKFILL_PARTIAL_ACTUAL_HISTORY", "completed_qualifying_count": 1},
        {"ticker": "NOPE", "planning_classification": "BACKFILL_NO_YAHOO_ROWS", "completed_qualifying_count": 0},
        {"ticker": "BAD", "planning_classification": "BACKFILL_SOURCE_FAILED", "completed_qualifying_count": 0},
    ]
    path.write_text(json.dumps({"artifact_schema_version": 2, "results": rows}), encoding="utf-8")
    return path


def _args(db_path: Path, temp_root: Path, *extra: str) -> list[str]:
    return [
        "--fundamentals-db",
        str(db_path),
        "--audit-json",
        str(_audit_json(temp_root)),
        "--checkpoint-json",
        str(temp_root / "checkpoint.json"),
        "--summary-json",
        str(temp_root / "summary.json"),
        "--output-csv",
        str(temp_root / "out.csv"),
        "--sleep-min-seconds",
        "0",
        "--sleep-max-seconds",
        "0",
        *extra,
    ]


def test_batch_defaults_to_dry_run_filters_candidates_and_writes_no_db(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _db(tmp_path)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    monkeypatch.setattr(backfill_yahoo_earnings_events, "temp_root", lambda: temp_root)
    monkeypatch.setattr(backfill_yahoo_earnings_events, "fetch_yahoo_earnings_events", lambda **kwargs: _source_result(db_path, str(kwargs["ticker"])))

    before = _event_count(db_path)
    payload, exit_code = backfill_yahoo_earnings_events.run_batch(backfill_yahoo_earnings_events.parse_args(_args(db_path, temp_root, "--first-n", "2")))

    assert exit_code == 0
    assert payload["execution_mode"] == "dry-run"
    assert payload["prebatch_backup_verified"] is False
    assert payload["selected_ticker_count"] == 2
    assert payload["summary"]["inserted_count"] == 2
    assert _event_count(db_path) == before


def test_batch_apply_creates_one_backup_and_idempotent_rerun(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _db(tmp_path)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    monkeypatch.setattr(backfill_yahoo_earnings_events, "temp_root", lambda: temp_root)
    monkeypatch.setattr(backfill_yahoo_earnings_events, "fetch_yahoo_earnings_events", lambda **kwargs: _source_result(db_path, str(kwargs["ticker"])))

    apply_args = backfill_yahoo_earnings_events.parse_args(_args(db_path, temp_root, "--ticker", "AAPL", "--apply"))
    first, exit_code = backfill_yahoo_earnings_events.run_batch(apply_args)
    assert exit_code == 0
    assert first["prebatch_backup_verified"] is True
    assert first["summary"]["inserted_count"] == 1
    assert len(list(temp_root.glob("yahoo_earnings_batch_backfill/*/backups/*.bak"))) == 1
    created = _timestamps(db_path)

    second_temp = temp_root / "second"
    second_temp.mkdir()
    rerun_args = backfill_yahoo_earnings_events.parse_args(
        [
            "--fundamentals-db",
            str(db_path),
            "--audit-json",
            str(_audit_json(temp_root)),
            "--checkpoint-json",
            str(second_temp / "checkpoint.json"),
            "--summary-json",
            str(second_temp / "summary.json"),
            "--output-csv",
            str(second_temp / "out.csv"),
            "--sleep-min-seconds",
            "0",
            "--sleep-max-seconds",
            "0",
            "--ticker",
            "AAPL",
            "--apply",
        ]
    )
    second, exit_code = backfill_yahoo_earnings_events.run_batch(rerun_args)
    assert exit_code == 0
    assert second["summary"]["inserted_count"] == 0
    assert second["summary"]["updated_count"] == 0
    assert second["summary"]["unchanged_count"] == 1
    assert _timestamps(db_path) == created


def test_batch_resume_skips_success_and_retries_failure_without_duplicate_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _db(tmp_path)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    monkeypatch.setattr(backfill_yahoo_earnings_events, "temp_root", lambda: temp_root)
    calls: list[str] = []

    def fake_fetch(**kwargs: object) -> YahooEarningsResult:
        ticker = str(kwargs["ticker"])
        calls.append(ticker)
        if ticker == "MSFT" and calls.count("MSFT") == 1:
            return _source_result(db_path, ticker, (), "SOURCE_FAILED")
        return _source_result(db_path, ticker)

    monkeypatch.setattr(backfill_yahoo_earnings_events, "fetch_yahoo_earnings_events", fake_fetch)
    args = backfill_yahoo_earnings_events.parse_args(_args(db_path, temp_root, "--ticker", "AAPL", "--ticker", "MSFT", "--rate-limit-backoff-seconds", "0"))
    first, _ = backfill_yahoo_earnings_events.run_batch(args)
    assert first["summary"]["failed_tickers"] == 0
    checkpoint = temp_root / "checkpoint.json"
    payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    payload["per_ticker_results"][1]["source_status"] = "SOURCE_FAILED"
    payload["per_ticker_results"][1]["transaction_status"] = "NOT_STARTED"
    checkpoint.write_text(json.dumps(payload), encoding="utf-8")
    calls.clear()

    resumed, _ = backfill_yahoo_earnings_events.run_batch(
        backfill_yahoo_earnings_events.parse_args(
            _args(
                db_path,
                temp_root,
                "--ticker",
                "AAPL",
                "--ticker",
                "MSFT",
                "--resume-from-json",
                str(checkpoint),
                "--retry-failed-on-resume",
                "--rate-limit-backoff-seconds",
                "0",
            )
        )
    )

    assert calls == ["MSFT", "MSFT"]
    assert [row["ticker"] for row in resumed["per_ticker_results"]] == ["AAPL", "MSFT"]


def test_batch_resume_preserves_failure_without_explicit_retry_and_logs_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _db(tmp_path)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    monkeypatch.setattr(backfill_yahoo_earnings_events, "temp_root", lambda: temp_root)
    calls: list[str] = []

    def fake_fetch(**kwargs: object) -> YahooEarningsResult:
        ticker = str(kwargs["ticker"])
        calls.append(ticker)
        return _source_result(db_path, ticker)

    monkeypatch.setattr(backfill_yahoo_earnings_events, "fetch_yahoo_earnings_events", fake_fetch)
    checkpoint = temp_root / "checkpoint.json"
    args = backfill_yahoo_earnings_events.parse_args(
        _args(db_path, temp_root, "--ticker", "AAPL", "--checkpoint-json", str(checkpoint), "--progress-log", str(temp_root / "progress.log"))
    )
    first, _ = backfill_yahoo_earnings_events.run_batch(args)
    payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    payload["per_ticker_results"][0]["source_status"] = "SOURCE_FAILED"
    payload["per_ticker_results"][0]["transaction_status"] = "NOT_STARTED"
    checkpoint.write_text(json.dumps(payload), encoding="utf-8")
    calls.clear()

    resumed, _ = backfill_yahoo_earnings_events.run_batch(
        backfill_yahoo_earnings_events.parse_args(
            _args(
                db_path,
                temp_root,
                "--ticker",
                "AAPL",
                "--resume-from-json",
                str(checkpoint),
                "--progress-log",
                str(temp_root / "progress.log"),
            )
        )
    )

    assert calls == []
    assert resumed["summary"]["failed_tickers"] == 1
    assert "processed=1/1" in (temp_root / "progress.log").read_text(encoding="utf-8")


def test_batch_rejects_paths_outside_temp_and_incompatible_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _db(tmp_path)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    monkeypatch.setattr(backfill_yahoo_earnings_events, "temp_root", lambda: temp_root)

    with pytest.raises(ValueError, match="RUNTIME_PATH_OUTSIDE_TEMP"):
        backfill_yahoo_earnings_events.parse_args(_args(db_path, temp_root, "--checkpoint-json", str(tmp_path / "outside.json")))
        backfill_yahoo_earnings_events.run_batch(backfill_yahoo_earnings_events.parse_args(_args(db_path, temp_root, "--checkpoint-json", str(tmp_path / "outside.json"))))


def test_batch_source_failure_does_not_rollback_prior_ticker_and_db_failure_stops(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _db(tmp_path)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    monkeypatch.setattr(backfill_yahoo_earnings_events, "temp_root", lambda: temp_root)

    def fake_fetch(**kwargs: object) -> YahooEarningsResult:
        ticker = str(kwargs["ticker"])
        if ticker == "MSFT":
            return _source_result(db_path, ticker, (), "SOURCE_FAILED")
        return _source_result(db_path, ticker)

    monkeypatch.setattr(backfill_yahoo_earnings_events, "fetch_yahoo_earnings_events", fake_fetch)
    payload, exit_code = backfill_yahoo_earnings_events.run_batch(
        backfill_yahoo_earnings_events.parse_args(_args(db_path, temp_root, "--ticker", "AAPL", "--ticker", "MSFT", "--apply", "--rate-limit-backoff-seconds", "0"))
    )
    assert exit_code == 0
    assert payload["summary"]["inserted_count"] == 1
    assert payload["summary"]["source_failures"] == 1
    assert _event_count(db_path) == 1


def _event_count(path: Path) -> int:
    with sqlite3.connect(str(path)) as conn:
        return int(conn.execute("SELECT COUNT(*) FROM rc_earnings_event").fetchone()[0])


def _timestamps(path: Path) -> tuple[str, str]:
    with sqlite3.connect(str(path)) as conn:
        row = conn.execute("SELECT created_at_utc, updated_at_utc FROM rc_earnings_event WHERE ticker='AAPL'").fetchone()
    return str(row[0]), str(row[1])
