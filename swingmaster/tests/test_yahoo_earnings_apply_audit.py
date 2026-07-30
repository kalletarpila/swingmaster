from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from swingmaster.cli import apply_yahoo_earnings_events, audit_yahoo_earnings_coverage
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_event_repo import (
    apply_earnings_event_upsert,
    count_events_for_ticker,
    plan_earnings_event_upsert,
)
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
                   ('AAPL', '2020-06-30', 1.0, 'RUN'),
                   ('MSFT', '2021-03-31', 1.0, 'RUN'),
                   ('JPM', '2019-12-31', 1.0, 'RUN')
            """
        )
        conn.commit()
    return path


def _record(
    date_text: str = "2020-01-28",
    *,
    reported_eps: float | None = 1.25,
    estimated_eps: float | None = 1.13,
    surprise_pct: float | None = 9.97,
    observed: str = OBSERVED,
    ticker: str = "AAPL",
) -> EarningsEventRecord:
    suffix = "-05:00" if date_text.endswith("-28") or date_text.endswith("-30") else "-04:00"
    return EarningsEventRecord(
        market="usa",
        ticker=ticker,
        announcement_at=f"{date_text}T16:00:00{suffix}",
        announcement_date=date_text,
        announcement_session="DURING_MARKET",
        is_reported=reported_eps is not None,
        reported_eps=reported_eps,
        estimated_eps=estimated_eps,
        surprise_pct=surprise_pct,
        source="YAHOO_FINANCE",
        source_observed_at_utc=observed,
        source_timezone="America/New_York",
    )


def _source_result(records: tuple[EarningsEventRecord, ...], status: str = "COVERAGE_OK", error: str | None = None) -> YahooEarningsResult:
    range_plan = plan_earnings_history_range(_memory_conn_with_quarter(), "AAPL")
    limit_plan = plan_yahoo_earnings_limit(range_plan.fetch_lower_bound or "2020-01-01", as_of_date="2026-07-30")
    coverage = assess_earnings_coverage(records, range_plan=range_plan, source_failed=status == "SOURCE_FAILED", parse_failed=status == "PARSE_FAILED")
    return YahooEarningsResult(
        ticker="AAPL",
        normalized_ticker="AAPL",
        requested_limit=limit_plan.requested_limit,
        source_observed_at_utc=OBSERVED,
        records=records,
        diagnostics=YahooParseDiagnostics(("EPS Estimate", "Reported EPS", "Surprise(%)"), len(records), 0, sum(1 for r in records if r.is_reported), sum(1 for r in records if not r.is_reported), 0, 0),
        coverage=coverage,
        status=status,
        error_message=error,
    )


def _memory_conn_with_quarter() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE rc_fundamental_quarterly (
            ticker TEXT NOT NULL,
            period_end_date TEXT NOT NULL,
            run_id TEXT NOT NULL,
            PRIMARY KEY (ticker, period_end_date)
        )
        """
    )
    conn.execute("INSERT INTO rc_fundamental_quarterly VALUES ('AAPL', '2020-03-31', 'RUN')")
    conn.commit()
    return conn


def test_repo_insert_update_unchanged_created_updated_and_no_delete(tmp_path: Path) -> None:
    path = _db(tmp_path)
    first = _record(observed="2026-07-30T14:00:00Z")
    changed = _record(reported_eps=1.30, observed="2026-07-31T14:00:00Z")

    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row
        inserted = apply_earnings_event_upsert(conn, [first], ticker="AAPL", applied_at_utc="2026-07-30T15:00:00Z")
        unchanged = apply_earnings_event_upsert(conn, [first], ticker="AAPL", applied_at_utc="2026-07-30T16:00:00Z")
        updated = apply_earnings_event_upsert(conn, [changed], ticker="AAPL", applied_at_utc="2026-07-31T15:00:00Z")
        row = conn.execute("SELECT * FROM rc_earnings_event WHERE ticker='AAPL'").fetchone()
        no_delete = apply_earnings_event_upsert(conn, [], ticker="AAPL", applied_at_utc="2026-08-01T15:00:00Z")
        assert count_events_for_ticker(conn, ticker="AAPL") == 1

    assert inserted.inserted_count == 1
    assert unchanged.unchanged_count == 1
    assert updated.updated_count == 1
    assert no_delete.fetched_record_count == 0
    assert no_delete.inserted_count == 0
    assert row["created_at_utc"] == "2026-07-30T15:00:00Z"
    assert row["updated_at_utc"] == "2026-07-31T15:00:00Z"
    assert row["reported_eps"] == 1.30


def test_repo_second_apply_observation_only_is_idempotent_and_preserves_timestamps(tmp_path: Path) -> None:
    path = _db(tmp_path)
    first = _record(observed="2026-07-30T14:00:00Z")
    observed_again = _record(observed="2026-07-31T14:00:00Z")

    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row
        inserted = apply_earnings_event_upsert(conn, [first], ticker="AAPL", applied_at_utc="2026-07-30T15:00:00Z")
        second = apply_earnings_event_upsert(
            conn,
            [observed_again],
            ticker="AAPL",
            applied_at_utc="2026-07-31T15:00:00Z",
        )
        row = conn.execute("SELECT * FROM rc_earnings_event WHERE ticker='AAPL'").fetchone()

    assert inserted.inserted_count == 1
    assert second.inserted_count == 0
    assert second.updated_count == 0
    assert second.unchanged_count == 1
    assert row["source_observed_at_utc"] == "2026-07-30T14:00:00Z"
    assert row["created_at_utc"] == "2026-07-30T15:00:00Z"
    assert row["updated_at_utc"] == "2026-07-30T15:00:00Z"


def test_repo_multiple_ticker_transactions_and_16_timestamp(tmp_path: Path) -> None:
    path = _db(tmp_path)

    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row
        aapl = apply_earnings_event_upsert(conn, [_record("2020-01-28")], ticker="AAPL")
        msft = apply_earnings_event_upsert(conn, [_record("2020-01-29", ticker="MSFT")], ticker="MSFT")
        bad = _record("2020-01-30", ticker="MSFT")
        with pytest.raises(ValueError):
            apply_earnings_event_upsert(conn, [_record("2020-04-30", ticker="JPM"), bad], ticker="JPM")
        counts = {
            row[0]: row[1]
            for row in conn.execute(
                """
                SELECT ticker, COUNT(*)
                FROM rc_earnings_event
                GROUP BY ticker
                """
            )
        }
        row = conn.execute(
            """
            SELECT announcement_at, announcement_date, announcement_session
            FROM rc_earnings_event
            WHERE ticker = 'AAPL'
            """
        ).fetchone()

    assert aapl.inserted_count == 1
    assert msft.inserted_count == 1
    assert counts == {"AAPL": 1, "MSFT": 1}
    assert row["announcement_at"].endswith("16:00:00-05:00")
    assert row["announcement_date"] == "2020-01-28"
    assert row["announcement_session"] == "DURING_MARKET"


def test_repo_rolls_back_duplicates_and_skips_future(tmp_path: Path) -> None:
    path = _db(tmp_path)
    future = _record("2026-07-30", reported_eps=None)
    duplicate = _record("2020-01-28")
    invalid = _record("2020-04-30", ticker="MSFT")

    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row
        planned = plan_earnings_event_upsert(conn, [future, duplicate, duplicate], ticker="AAPL")
        assert planned.skipped_count == 1
        assert planned.duplicate_count == 1
        assert planned.eligible_record_count == 1

        with pytest.raises(ValueError):
            apply_earnings_event_upsert(conn, [_record("2020-01-28"), invalid], ticker="AAPL")
        assert count_events_for_ticker(conn, ticker="AAPL") == 0


def test_repo_requires_migrated_earnings_event_table(tmp_path: Path) -> None:
    path = tmp_path / "unmigrated.db"
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE rc_fundamental_quarterly (
                ticker TEXT NOT NULL,
                period_end_date TEXT NOT NULL,
                run_id TEXT NOT NULL,
                PRIMARY KEY (ticker, period_end_date)
            )
            """
        )
        conn.commit()
        with pytest.raises(RuntimeError, match="EARNINGS_EVENT_TABLE_MISSING"):
            apply_earnings_event_upsert(conn, [_record()], ticker="AAPL")


def test_apply_cli_default_dry_run_apply_backup_and_conflict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = _db(tmp_path)
    monkeypatch.setattr(apply_yahoo_earnings_events, "fetch_yahoo_earnings_events", lambda **_: _source_result((_record(),)))

    assert apply_yahoo_earnings_events.main(["--ticker", "AAPL", "--fundamentals-db", str(path), "--json"]) == 0
    dry_payload = json.loads(capsys.readouterr().out)
    assert dry_payload["mode"] == "dry-run"
    assert dry_payload["backup_path"] is None
    assert dry_payload["apply_summary"]["inserted_count"] == 1
    with sqlite3.connect(str(path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_earnings_event").fetchone()[0] == 0

    assert apply_yahoo_earnings_events.main(["--ticker", "AAPL", "--fundamentals-db", str(path), "--apply", "--backup", str(tmp_path), "--json"]) == 0
    apply_payload = json.loads(capsys.readouterr().out)
    assert apply_payload["mode"] == "apply"
    assert Path(apply_payload["backup_path"]).exists()
    assert apply_payload["apply_summary"]["transaction_status"] == "COMMITTED"
    with sqlite3.connect(str(path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_earnings_event").fetchone()[0] == 1

    assert apply_yahoo_earnings_events.main(["--ticker", "AAPL", "--fundamentals-db", str(path), "--dry-run", "--apply"]) == 1


def test_apply_cli_source_parse_and_verification_failures_do_not_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _db(tmp_path)
    monkeypatch.setattr(apply_yahoo_earnings_events, "fetch_yahoo_earnings_events", lambda **_: _source_result((), "SOURCE_FAILED", "network"))
    assert apply_yahoo_earnings_events.main(["--ticker", "AAPL", "--fundamentals-db", str(path), "--apply"]) == 1
    with sqlite3.connect(str(path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_earnings_event").fetchone()[0] == 0

    monkeypatch.setattr(apply_yahoo_earnings_events, "fetch_yahoo_earnings_events", lambda **_: _source_result((), "PARSE_FAILED", "columns"))
    assert apply_yahoo_earnings_events.main(["--ticker", "AAPL", "--fundamentals-db", str(path), "--apply"]) == 1
    with sqlite3.connect(str(path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM rc_earnings_event").fetchone()[0] == 0

    monkeypatch.setattr(apply_yahoo_earnings_events, "fetch_yahoo_earnings_events", lambda **_: _source_result((_record("2020-04-30"),)))
    monkeypatch.setattr(apply_yahoo_earnings_events, "count_events_for_ticker", lambda *_, **__: 999)
    assert apply_yahoo_earnings_events.main(["--ticker", "AAPL", "--fundamentals-db", str(path), "--apply"]) == 1


def test_audit_universe_selection_artifacts_resume_and_no_network(tmp_path: Path) -> None:
    path = _db(tmp_path)
    output_json = tmp_path / "audit.json"
    output_csv = tmp_path / "audit.csv"
    args = audit_yahoo_earnings_coverage.parse_args(
        [
            "--fundamentals-db",
            str(path),
            "--first-n",
            "2",
            "--no-network",
            "--output-json",
            str(output_json),
            "--output-csv",
            str(output_csv),
        ]
    )
    payload, exit_code = audit_yahoo_earnings_coverage.run_audit(args)

    assert exit_code == 0
    assert [row["ticker"] for row in payload["results"]] == ["AAPL", "JPM"]
    assert all(row["source_status"] == "NOT_REQUESTED" for row in payload["results"])
    assert output_json.exists()
    assert output_csv.read_text(encoding="utf-8").startswith("ticker,fundamentals_row_count")

    resume = json.loads(output_json.read_text(encoding="utf-8"))
    resume["results"][0]["source_status"] = "SOURCE_FAILED"
    resume["results"][0]["coverage_status"] = "SOURCE_FAILED"
    resume_path = tmp_path / "resume.json"
    resume_path.write_text(json.dumps(resume), encoding="utf-8")

    calls: list[str] = []

    def fake_audit(ticker: str, **_: object) -> dict[str, object]:
        calls.append(ticker)
        return {"ticker": ticker, "source_status": "COVERAGE_OK", "coverage_status": "COVERAGE_OK", "covers_oldest_fundamentals_period": True, "limit_was_capped": False}

    audit_args = audit_yahoo_earnings_coverage.parse_args(
        ["--fundamentals-db", str(path), "--first-n", "2", "--resume-from-json", str(resume_path), "--sleep-seconds", "0"]
    )
    original = audit_yahoo_earnings_coverage.audit_ticker
    audit_yahoo_earnings_coverage.audit_ticker = fake_audit
    try:
        resumed, _ = audit_yahoo_earnings_coverage.run_audit(audit_args)
    finally:
        audit_yahoo_earnings_coverage.audit_ticker = original
    assert calls == ["AAPL"]
    assert resumed["results"][1]["ticker"] == "JPM"


def test_audit_ticker_sources_aggregation_selection_and_incompatible_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _db(tmp_path)
    ticker_file = tmp_path / "tickers.txt"
    ticker_file.write_text("MSFT\nAAPL\n", encoding="utf-8")
    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row
        assert audit_yahoo_earnings_coverage.select_tickers(
            audit_yahoo_earnings_coverage.parse_args(["--fundamentals-db", str(path), "--tickers-file", str(ticker_file)]),
            conn,
        ) == ["AAPL", "MSFT"]
        sampled = audit_yahoo_earnings_coverage.select_tickers(
            audit_yahoo_earnings_coverage.parse_args(["--fundamentals-db", str(path), "--sample-size", "2", "--random-seed", "7"]),
            conn,
        )
        assert sampled == sorted(sampled)
        assert len(sampled) == 2

    def fake_fetch(**kwargs: object) -> YahooEarningsResult:
        ticker = str(kwargs["ticker"])
        range_plan = kwargs["range_plan"]
        limit_plan = kwargs["limit_plan"]
        if ticker == "MSFT":
            return _source_result((), "SOURCE_FAILED", "429 Too Many Requests")
        records = (_record("2020-01-28", ticker=ticker),)
        coverage = assess_earnings_coverage(records, range_plan=range_plan)
        return YahooEarningsResult(
            ticker=ticker,
            normalized_ticker=ticker,
            requested_limit=limit_plan.requested_limit,
            source_observed_at_utc=OBSERVED,
            records=records,
            diagnostics=YahooParseDiagnostics(("EPS Estimate", "Reported EPS"), 1, 0, 1, 0, 0, 0),
            coverage=coverage,
            status=coverage.coverage_status,
        )

    monkeypatch.setattr(audit_yahoo_earnings_coverage, "fetch_yahoo_earnings_events", fake_fetch)
    payload, _ = audit_yahoo_earnings_coverage.run_audit(
        audit_yahoo_earnings_coverage.parse_args(["--fundamentals-db", str(path), "--ticker", "AAPL", "--ticker", "MSFT", "--sleep-seconds", "0", "--max-retries", "0"])
    )
    assert payload["summary"]["source_failed_count"] == 1
    assert payload["summary"]["covers_oldest_fundamentals_period_count"] >= 1
    assert payload["results"][1]["error_type"] == "RATE_LIMIT"

    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"artifact_schema_version": 999, "database_identity": audit_yahoo_earnings_coverage.db_identity(path), "results": []}), encoding="utf-8")
    with pytest.raises(ValueError):
        audit_yahoo_earnings_coverage.load_resume_artifact(bad, audit_yahoo_earnings_coverage.db_identity(path))


def test_read_only_planning_does_not_change_database_content(tmp_path: Path) -> None:
    path = _db(tmp_path)
    before = _table_counts(path)
    args = audit_yahoo_earnings_coverage.parse_args(["--fundamentals-db", str(path), "--ticker", "AAPL", "--no-network"])
    audit_yahoo_earnings_coverage.run_audit(args)
    after = _table_counts(path)
    assert after == before


def _table_counts(path: Path) -> dict[str, int]:
    with sqlite3.connect(str(path)) as conn:
        return {
            "quarterly": conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0],
            "events": conn.execute("SELECT COUNT(*) FROM rc_earnings_event").fetchone()[0],
            "schema": conn.execute("SELECT COUNT(*) FROM rc_fundamental_schema_version").fetchone()[0],
        }
