from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.earnings_calendar import record_earnings_calendar_check_failure
from swingmaster.fundamentals import result_check
from swingmaster.fundamentals.provider_observations import work_unit_identity


def _migrated_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    return db_path


def _ohlcv_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "osakedata.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE osakedata (market TEXT NOT NULL, osake TEXT NOT NULL, pvm TEXT NOT NULL)")
        conn.executemany(
            "INSERT INTO osakedata (market, osake, pvm) VALUES ('usa', ?, ?)",
            [("AAPL", "2026-08-07"), ("MSFT", "2026-08-06"), ("STALE", "2026-07-01")],
        )
        conn.commit()
    return db_path


def _ohlcv_db_for(tmp_path: Path, tickers: list[str], latest_date: str = "2026-08-30") -> Path:
    db_path = tmp_path / "osakedata_backlog.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE osakedata (market TEXT NOT NULL, osake TEXT NOT NULL, pvm TEXT NOT NULL)")
        conn.executemany(
            "INSERT INTO osakedata (market, osake, pvm) VALUES ('usa', ?, ?)",
            [(ticker, latest_date) for ticker in tickers],
        )
        conn.commit()
    return db_path


def _insert_quarter(db_path: Path, ticker: str, period: str = "2026-03-31") -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker, period_end_date, revenue, ebit, free_cashflow, cash, total_debt, shares_outstanding, run_id
            ) VALUES (?, ?, 100, 10, 8, 20, 5, 10, 'FIXTURE')
            """,
            (ticker, period),
        )
        conn.commit()


def _insert_partial_quarter(db_path: Path, ticker: str, period: str) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarterly (
                ticker, period_end_date, revenue, ebit, free_cashflow, cash, total_debt, shares_outstanding, run_id
            ) VALUES (?, ?, NULL, 10, 8, 20, 5, 10, 'FIXTURE')
            """,
            (ticker, period),
        )
        conn.commit()


def _insert_ingestion_status(
    db_path: Path,
    ticker: str,
    period: str,
    *,
    quarter_basic_complete: int,
    ingestion_status: str = "UNKNOWN_HISTORICAL_INGEST_COMPLETENESS",
    missing_basic_fields: str = "[]",
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_ingestion_status (
                market, ticker, period_end_date, ingestion_status, basic_status,
                quarter_basic_complete, ttm_input_complete, score_history_complete,
                valuation_input_ready, historical_research_ready, missing_basic_fields,
                missing_core_fields_json, missing_ttm_fields_json, missing_score_fields_json,
                data_quality_warnings_json, retry_recommendation, last_checked_at_utc,
                assessment_policy_version, ingestion_evidence_type, run_id, assessed_at_utc,
                created_at_utc, updated_at_utc
            ) VALUES (
                'usa', ?, ?, ?, 'TEST', ?, 0, 0, 0, 0, ?, '[]', '[]', '[]', '[]',
                'TEST', '2026-08-07T00:00:00Z', 'test', 'CURRENT_DB_STATE_ONLY',
                'TEST', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z'
            )
            """,
            (ticker, period, ingestion_status, quarter_basic_complete, missing_basic_fields),
        )
        conn.commit()


def _insert_calendar(
    db_path: Path,
    ticker: str,
    status: str,
    estimated_date: str | None,
    *,
    last_observed_at_utc: str = "2026-08-07T00:00:00Z",
    check_status: str | None = "SUCCESS",
    failure_count: int = 0,
    source: str = "fixture",
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_calendar (
                market, ticker, estimated_announcement_at, estimated_announcement_date, estimated_session,
                calendar_status, source, source_observed_at_utc, first_observed_at_utc, last_observed_at_utc,
                calendar_last_checked_at_utc, calendar_check_status, calendar_failure_count,
                created_at_utc, updated_at_utc
            ) VALUES ('usa', ?, ?, ?, 'AMC', ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                f"{estimated_date}T20:00:00Z" if estimated_date else None,
                estimated_date,
                status,
                source,
                last_observed_at_utc,
                last_observed_at_utc,
                last_observed_at_utc,
                last_observed_at_utc,
                check_status,
                failure_count,
                last_observed_at_utc,
                last_observed_at_utc,
            ),
        )
        conn.commit()


def _set_calendar_status(db_path: Path, ticker: str, status: str, estimated_date: str | None) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE rc_earnings_calendar
            SET estimated_announcement_at = ?,
                estimated_announcement_date = ?,
                calendar_status = ?,
                updated_at_utc = '2026-08-07T00:00:00Z'
            WHERE market = 'usa' AND ticker = ?
            """,
            (f"{estimated_date}T20:00:00Z" if estimated_date else None, estimated_date, status, ticker),
        )
        conn.commit()


def _insert_event_and_match(db_path: Path, ticker: str, event_date: str, period: str, event_id: int = 1) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                id, market, ticker, announcement_at, announcement_date, announcement_session, is_reported,
                reported_eps, estimated_eps, surprise_pct, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES (?, 'usa', ?, ?, ?, 'AMC', 1, 1.0, 0.9, 10.0, 'fixture',
                '2026-08-07T00:00:00Z', 'UTC', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """,
            (event_id, ticker, f"{event_date}T20:00:00Z", event_date),
        )
        conn.execute(
            """
            INSERT INTO rc_fundamental_quarter_earnings_match (
                market, ticker, period_end_date, earnings_event_id, announcement_at, announcement_date,
                announcement_session, effective_trading_date, effective_date_status, reporting_delay_days,
                matching_status, matching_confidence, matching_method, candidate_count, availability_policy,
                matcher_version, created_at_utc, updated_at_utc
            ) VALUES ('usa', ?, ?, ?, ?, ?, 'AMC', ?, 'SAME_DAY', 37, 'MATCHED', 'HIGH',
                'fixture', 1, 'fixture', 'fixture', '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """,
            (ticker, period, event_id, f"{event_date}T20:00:00Z", event_date, event_date),
        )
        conn.commit()


def _mock_result_check_provider_stages(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        result_check,
        "_run_calendar_refresh",
        lambda root, db, tickers, **kwargs: {
            "stage": {"stage": "calendar_refresh", "status": "SUCCESS", "selected_tickers": len(tickers)},
            "summary": {"selected_tickers": len(tickers), "inserted_count": 0, "updated_count": 0},
        },
    )
    monkeypatch.setattr(
        result_check,
        "_run_completed_event_refresh",
        lambda root, db, tickers, **kwargs: {
            "stage": {"stage": "completed_event_refresh", "status": "SUCCESS", "selected_tickers": len(tickers)},
            "summary": {"selected_tickers": len(tickers), "failed_tickers": 0, "results": []},
        },
    )
    monkeypatch.setattr(
        result_check,
        "_run_match_rebuild",
        lambda root, db, enabled, **kwargs: {
            "stage": {"stage": "event_match_rebuild", "status": "SUCCESS", "enabled": enabled},
            "summary": {"mocked": True},
        },
    )


def test_result_check_creates_one_run_level_backup_before_mutating_stages(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_run_backup"
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")

    created_backups: list[Path] = []
    received_backups: list[dict[str, object]] = []

    def fake_backup(db_path: Path, backup_arg: str | None = None) -> Path:
        assert db_path == fundamentals_db
        assert backup_arg is not None
        backup_path = Path(backup_arg)
        assert backup_path.name.startswith("fundamentals.db.pre_result_check.")
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(backup_path)) as conn:
            conn.execute("CREATE TABLE backup_marker (id INTEGER)")
            conn.commit()
        created_backups.append(backup_path)
        return backup_path.resolve()

    def fake_calendar(root: Path, db: Path, tickers: list[str], **kwargs: object) -> dict[str, object]:
        received_backups.append(dict(kwargs["result_check_backup"]))  # type: ignore[arg-type]
        return {"stage": {"stage": "calendar_refresh", "status": "SUCCESS"}, "summary": {}}

    def fake_completed(root: Path, db: Path, tickers: list[str], **kwargs: object) -> dict[str, object]:
        received_backups.append(dict(kwargs["result_check_backup"]))  # type: ignore[arg-type]
        return {"stage": {"stage": "completed_event_refresh", "status": "SUCCESS"}, "summary": {"selected_tickers": len(tickers)}}

    def fake_match(root: Path, db: Path, *, enabled: bool, **kwargs: object) -> dict[str, object]:
        received_backups.append(dict(kwargs["result_check_backup"]))  # type: ignore[arg-type]
        return {"stage": {"stage": "event_match_rebuild", "status": "SUCCESS", "enabled": enabled}, "summary": {}}

    monkeypatch.setattr(result_check.apply_yahoo_earnings_events, "create_sqlite_backup", fake_backup)
    monkeypatch.setattr(result_check, "_run_calendar_refresh", fake_calendar)
    monkeypatch.setattr(result_check, "_run_completed_event_refresh", fake_completed)
    monkeypatch.setattr(result_check, "_run_match_rebuild", fake_match)

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["AAPL"],
    )

    assert result["check_status"] == "SUCCESS"
    assert len(created_backups) == 1
    assert created_backups[0].parent == output_root / "backups"
    assert all(backup["backup_verified"] is True for backup in received_backups)
    assert {backup["backup_path"] for backup in received_backups} == {str(created_backups[0].resolve())}
    backup_stages = [stage for stage in result["stages"] if stage["stage"] == "result_check_backup"]
    assert len(backup_stages) == 1
    assert backup_stages[0]["backup_path"] == str(created_backups[0].resolve())


def _run_check(fundamentals_db: Path, ohlcv_db: Path, decision_date: date, label: str) -> dict[str, object]:
    return result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date=decision_date,
        ohlcv_stale_days=60,
        output_root=Path.cwd() / "temp" / label,
    )


def _candidate_pairs(plan: dict[str, object]) -> list[tuple[str, str]]:
    return sorted((str(row["ticker"]), str(row["target_period_end_date"])) for row in plan["candidates"])


def test_result_check_builds_executable_plan_after_completed_event(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_plan"
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    _insert_event_and_match(fundamentals_db, "AAPL", "2026-08-07", "2026-06-30")

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers, **kwargs: {"stage": {"stage": "calendar_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_completed_event_refresh", lambda root, db, tickers, **kwargs: {"stage": {"stage": "completed_event_refresh", "status": "SUCCESS"}, "summary": {"selected_tickers": len(tickers)}})
    monkeypatch.setattr(result_check, "_run_match_rebuild", lambda root, db, enabled, **kwargs: {"stage": {"stage": "event_match_rebuild", "status": "SUCCESS", "enabled": enabled}, "summary": {}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["AAPL"],
    )

    assert result["check_status"] == "SUCCESS"
    assert result["plan"]["candidate_count"] == 1
    assert result["plan"]["candidates"][0]["ticker"] == "AAPL"
    assert result["plan"]["candidates"][0]["target_period_end_date"] == "2026-06-30"
    assert result["plan"]["plan_schema_version"] == 2
    assert result["plan"]["executable_work_unit_count"] == 1
    work_unit = result["plan"]["work_units"][0]
    assert work_unit["work_unit_key"] == "usa:AAPL:2026:Q2"
    assert work_unit["canonical_fiscal_year"] == 2026
    assert work_unit["canonical_fiscal_quarter"] == "Q2"
    assert result["plan"]["candidates"][0]["work_unit_key"] == "usa:AAPL:2026:Q2"
    assert result_check.validate_candidate_hash(result["plan"])
    assert Path(result["artifact_paths"]["plan_json"]).exists()
    work_units = json.loads(Path(result["artifact_paths"]["work_units_json"]).read_text(encoding="utf-8"))
    assert [row["work_unit_key"] for row in work_units] == ["usa:AAPL:2026:Q2"]


def test_work_unit_key_is_stable_for_same_fiscal_quarter_date_offset() -> None:
    exact = work_unit_identity(market="usa", ticker="AAPL", period_end_date="2026-06-30")
    offset = work_unit_identity(market="usa", ticker="aapl", period_end_date="2026-06-29")

    assert exact.work_unit_key == offset.work_unit_key == "usa:AAPL:2026:Q2"


def test_stale_ohlcv_suppresses_provider_candidate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_stale"
    _insert_quarter(fundamentals_db, "STALE")
    _insert_calendar(fundamentals_db, "STALE", "DUE_TODAY", "2026-08-07")
    _insert_event_and_match(fundamentals_db, "STALE", "2026-08-07", "2026-06-30")

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers, **kwargs: {"stage": {"stage": "calendar_refresh", "status": "SUCCESS", "selected_tickers": len(tickers)}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_completed_event_refresh", lambda root, db, tickers, **kwargs: {"stage": {"stage": "completed_event_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_match_rebuild", lambda root, db, enabled, **kwargs: {"stage": {"stage": "event_match_rebuild", "status": "SUCCESS"}, "summary": {}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["STALE"],
    )

    assert result["check_status"] == "SUCCESS"
    assert result["plan"]["candidate_count"] == 0
    assert result["plan"]["candidates"] == []


def test_completed_event_candidate_selection_uses_due_today_and_recent_passed(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    for ticker in ("AAPL", "MSFT", "OLD", "FUTURE"):
        _insert_quarter(fundamentals_db, ticker)
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    _insert_calendar(fundamentals_db, "MSFT", "DATE_PASSED_EVENT_NOT_FOUND", "2026-08-04")
    _insert_calendar(fundamentals_db, "OLD", "DATE_PASSED_EVENT_NOT_FOUND", "2026-07-01")
    _insert_calendar(fundamentals_db, "FUTURE", "UPCOMING", "2026-08-20")

    selected = result_check.select_completed_event_refresh_candidates(
        fundamentals_db=fundamentals_db,
        tickers=["AAPL", "MSFT", "OLD", "FUTURE"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )

    assert selected == ["AAPL", "MSFT"]


def test_completed_event_refresh_uses_one_batch_backup_for_multiple_tickers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    root = Path.cwd() / "temp" / "pytest_completed_event_batch_backup"
    backup_paths: list[Path] = []
    apply_args: list[object] = []

    def fake_backup(db_path: Path, backup_arg: str | None = None) -> Path:
        assert db_path == fundamentals_db
        assert backup_arg is not None
        backup_path = Path(backup_arg)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(backup_path)) as conn:
            conn.execute("CREATE TABLE backup_marker (id INTEGER)")
            conn.commit()
        backup_paths.append(backup_path)
        return backup_path.resolve()

    def fake_apply(args: object) -> tuple[dict[str, object], int]:
        apply_args.append(args)
        assert getattr(args, "backup_already_created") is True
        assert getattr(args, "backup") is None
        return {"mode": "apply", "backup_path": None, "apply_summary": {"transaction_status": "COMMITTED"}}, 0

    monkeypatch.setattr(result_check.apply_yahoo_earnings_events, "create_sqlite_backup", fake_backup)
    monkeypatch.setattr(result_check.apply_yahoo_earnings_events, "build_apply_summary", fake_apply)

    payload = result_check._run_completed_event_refresh(root, fundamentals_db, ["AAPL", "MSFT", "JPM"])

    assert payload["stage"]["status"] == result_check.CHECK_STATUS_SUCCESS
    assert payload["summary"]["selected_tickers"] == 3
    assert payload["summary"]["batch_backup"]["created"] is True
    assert payload["summary"]["batch_backup"]["verified"] is True
    assert payload["summary"]["batch_backup"]["path"] == str(backup_paths[0].resolve())
    assert len(backup_paths) == 1
    assert len(apply_args) == 3
    assert all(row["summary"]["backup_path"] is None for row in payload["summary"]["results"])


def test_completed_event_refresh_zero_candidates_creates_no_backup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    root = Path.cwd() / "temp" / "pytest_completed_event_zero_backup"

    def fail_backup(*_: object, **__: object) -> None:
        raise AssertionError("backup should not be created for zero candidates")

    def fail_apply(*_: object, **__: object) -> None:
        raise AssertionError("ticker apply should not run for zero candidates")

    monkeypatch.setattr(result_check.apply_yahoo_earnings_events, "create_sqlite_backup", fail_backup)
    monkeypatch.setattr(result_check.apply_yahoo_earnings_events, "build_apply_summary", fail_apply)

    payload = result_check._run_completed_event_refresh(root, fundamentals_db, [])

    assert payload["stage"]["status"] == result_check.CHECK_STATUS_SUCCESS
    assert payload["stage"]["backup_created"] is False
    assert payload["summary"]["selected_tickers"] == 0
    assert payload["summary"]["batch_backup"] == {"created": False, "path": None, "verified": False}
    assert payload["summary"]["results"] == []


def test_completed_event_refresh_backup_failure_stops_before_ticker_apply(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    root = Path.cwd() / "temp" / "pytest_completed_event_backup_failure"
    apply_started = False

    def fail_backup(*_: object, **__: object) -> Path:
        raise RuntimeError("disk full")

    def fake_apply(*_: object, **__: object) -> tuple[dict[str, object], int]:
        nonlocal apply_started
        apply_started = True
        return {}, 1

    monkeypatch.setattr(result_check.apply_yahoo_earnings_events, "create_sqlite_backup", fail_backup)
    monkeypatch.setattr(result_check.apply_yahoo_earnings_events, "build_apply_summary", fake_apply)

    payload = result_check._run_completed_event_refresh(root, fundamentals_db, ["AAPL", "MSFT"])

    assert payload["stage"]["status"] == result_check.CHECK_STATUS_FAILED
    assert payload["stage"]["failed_tickers"] == 2
    assert payload["summary"]["failed_tickers"] == 2
    assert payload["summary"]["batch_backup"]["created"] is False
    assert payload["summary"]["batch_backup"]["error_type"] == "RuntimeError"
    assert payload["summary"]["results"] == []
    assert apply_started is False


def test_result_check_substeps_reuse_verified_run_backup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    root = Path.cwd() / "temp" / "pytest_result_check_substep_backup_reuse"
    run_backup = {"backup_verified": True, "backup_path": str(root / "backups" / "fundamentals.db.pre_result_check.TEST.bak")}
    calendar_argv: list[str] = []
    match_args: list[object] = []

    def fake_calendar_main(argv: list[str]) -> int:
        calendar_argv.extend(argv)
        summary_path = Path(argv[argv.index("--summary-json") + 1])
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps({"selected_tickers": 1}) + "\n", encoding="utf-8")
        return 0

    def fail_completed_backup(*_: object, **__: object) -> Path:
        raise AssertionError("completed-event substep should reuse the result-check backup")

    def fake_apply(args: object) -> tuple[dict[str, object], int]:
        assert getattr(args, "backup_already_created") is True
        return {"mode": "apply", "backup_path": None, "apply_summary": {"transaction_status": "COMMITTED"}}, 0

    def fake_match_run_cli(args: object) -> dict[str, object]:
        match_args.append(args)
        return {"backup": {"created": False, "verified": True, "path": getattr(args, "backup")}, "summary": {}}

    monkeypatch.setattr(result_check.refresh_yahoo_earnings_calendar, "main", fake_calendar_main)
    monkeypatch.setattr(result_check.apply_yahoo_earnings_events, "create_sqlite_backup", fail_completed_backup)
    monkeypatch.setattr(result_check.apply_yahoo_earnings_events, "build_apply_summary", fake_apply)
    monkeypatch.setattr(result_check.rebuild_earnings_event_matches, "run_cli", fake_match_run_cli)

    calendar_payload = result_check._run_calendar_refresh(root, fundamentals_db, ["AAPL"], result_check_backup=run_backup)
    completed_payload = result_check._run_completed_event_refresh(root, fundamentals_db, ["AAPL"], result_check_backup=run_backup)
    match_payload = result_check._run_match_rebuild(root, fundamentals_db, enabled=True, result_check_backup=run_backup)

    assert calendar_payload["stage"]["status"] == result_check.CHECK_STATUS_SUCCESS
    assert "--backup-already-created" in calendar_argv
    assert completed_payload["summary"]["batch_backup"] == {
        "created": False,
        "path": run_backup["backup_path"],
        "verified": True,
        "reused_from_result_check": True,
    }
    assert completed_payload["stage"]["backup_reused"] is True
    assert len(match_args) == 1
    assert getattr(match_args[0], "backup_already_created") is True
    assert getattr(match_args[0], "backup") == run_backup["backup_path"]
    assert match_payload["summary"]["backup"]["created"] is False


def test_calendar_selector_normal_day_bounds_provider_work_to_due_and_maintenance(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    active = [f"T{i:04d}" for i in range(3000)]
    for ticker in active[30:130]:
        _insert_calendar(fundamentals_db, ticker, "UPCOMING", "2026-11-15")
    for ticker in active[:20]:
        _insert_calendar(fundamentals_db, ticker, "UPCOMING", "2026-08-12", last_observed_at_utc="2026-08-05T00:00:00Z")
    for ticker in active[20:30]:
        _insert_calendar(fundamentals_db, ticker, "DATE_PASSED_EVENT_NOT_FOUND", "2026-08-05")

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=active,
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
        maintenance_limit=50,
    )

    assert len(selected["due_for_confirmation"]) == 20
    assert len(selected["due_for_confirmation_watch"]) == 20
    assert len(selected["due_for_result_check"]) == 10
    assert len(selected["calendar_maintenance"]) == 50
    assert len(selected["selected_tickers"]) == 80
    assert len(selected["selected_tickers"]) < 3000


def test_calendar_selector_earnings_season_prioritizes_due_and_caps_maintenance(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    active = [f"E{i:04d}" for i in range(500)]
    for ticker in active[:150]:
        _insert_calendar(fundamentals_db, ticker, "DATE_PASSED_EVENT_NOT_FOUND", "2026-08-06")
    for ticker in active[150:310]:
        _insert_calendar(fundamentals_db, ticker, "UPCOMING", "2026-08-10", last_observed_at_utc="2026-08-06T00:00:00Z")

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=active,
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
        maintenance_limit=25,
    )

    assert len(selected["due_for_result_check"]) == 150
    assert len(selected["due_for_confirmation"]) == 160
    assert len(selected["calendar_maintenance"]) == 25
    assert len(selected["selected_tickers"]) == 335
    assert selected["selected_tickers"] == sorted(selected["selected_tickers"])


def test_calendar_selector_date_moved_later_is_no_longer_due_after_successful_refresh(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(fundamentals_db, "MOVE", "UPCOMING", "2026-08-08", last_observed_at_utc="2026-08-06T00:00:00Z")

    before = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["MOVE"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )
    assert before["due_for_confirmation"] == ["MOVE"]

    with sqlite3.connect(str(fundamentals_db)) as conn:
        conn.execute(
            """
            UPDATE rc_earnings_calendar
            SET estimated_announcement_date = '2026-08-17',
                estimated_announcement_at = '2026-08-17T20:00:00Z',
                last_observed_at_utc = '2026-08-07T12:00:00Z',
                calendar_last_checked_at_utc = '2026-08-07T12:00:00Z',
                calendar_check_status = 'SUCCESS'
            WHERE ticker = 'MOVE'
            """
        )
        conn.commit()

    after = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["MOVE"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )
    assert after["selected_tickers"] == []


def test_calendar_selector_provider_failure_preserves_date_and_defers_retry(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(
        fundamentals_db,
        "FAIL",
        "UPCOMING",
        "2026-09-01",
        last_observed_at_utc="2026-08-01T00:00:00Z",
        check_status="TIMEOUT",
        failure_count=1,
    )

    same_day = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["FAIL"],
        decision_date=result_check._parse_date("2026-08-02"),
        event_watch_days_after=5,
        failure_retry_days=3,
    )
    assert same_day["selected_tickers"] == []

    retry = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["FAIL"],
        decision_date=result_check._parse_date("2026-08-04"),
        event_watch_days_after=5,
        failure_retry_days=3,
    )
    assert retry["calendar_maintenance"] == ["FAIL"]
    with sqlite3.connect(str(fundamentals_db)) as conn:
        assert conn.execute("SELECT estimated_announcement_date FROM rc_earnings_calendar WHERE ticker='FAIL'").fetchone()[0] == "2026-09-01"


def test_calendar_failure_record_preserves_existing_future_estimate(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(fundamentals_db, "KEEP", "UPCOMING", "2026-09-01", source="YAHOO_FINANCE")

    with sqlite3.connect(str(fundamentals_db)) as conn:
        record_earnings_calendar_check_failure(
            conn,
            market="usa",
            ticker="KEEP",
            observed_at_utc="2026-08-07T12:00:00Z",
            failure_status="TIMEOUT",
        )
        conn.commit()
        row = conn.execute(
            """
            SELECT estimated_announcement_date, calendar_status, calendar_check_status, calendar_failure_count
            FROM rc_earnings_calendar
            WHERE ticker = 'KEEP'
            """
        ).fetchone()

    assert row == ("2026-09-01", "UPCOMING", "TIMEOUT", 1)


def test_calendar_selector_missing_metadata_enters_bounded_maintenance(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    active = [f"M{i:03d}" for i in range(12)]

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=active,
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
        maintenance_limit=5,
    )

    assert selected["calendar_maintenance"] == active[:5]
    assert selected["maintenance_backlog_remaining"] == 7


def test_calendar_selector_default_maintenance_limit_is_one_hundred(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    active = [f"D{i:03d}" for i in range(125)]

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=active,
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )

    assert result_check.DEFAULT_CALENDAR_MAINTENANCE_LIMIT == 100
    assert selected["calendar_maintenance"] == active[:100]
    assert selected["maintenance_backlog_remaining"] == 25


def test_calendar_selector_past_expected_date_remains_in_result_grace(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(fundamentals_db, "PAST", "DATE_PASSED_EVENT_NOT_FOUND", "2026-08-04")

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["PAST"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=3,
    )

    assert selected["due_for_result_check"] == ["PAST"]


def test_calendar_selector_repeat_same_day_skips_fresh_non_due_rows(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(fundamentals_db, "FRESH", "UPCOMING", "2026-10-20")

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["FRESH"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )

    assert selected["selected_tickers"] == []


def test_calendar_selector_same_day_repeat_suppresses_future_confirmation_provider_calls(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    active = [f"R{i:03d}" for i in range(100)]
    for ticker in active:
        _insert_calendar(fundamentals_db, ticker, "UPCOMING", "2026-08-12")

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=active,
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
        maintenance_limit=50,
    )

    assert len(selected["due_for_confirmation_watch"]) == 100
    assert selected["due_for_confirmation"] == []
    assert selected["selected_tickers"] == []


def test_calendar_selector_confirmation_cadence_for_four_to_seven_days_away(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(fundamentals_db, "YDAY", "UPCOMING", "2026-08-12", last_observed_at_utc="2026-08-06T00:00:00Z")
    _insert_calendar(fundamentals_db, "OLD", "UPCOMING", "2026-08-12", last_observed_at_utc="2026-08-05T00:00:00Z")

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["YDAY", "OLD"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )

    assert selected["due_for_confirmation_watch"] == ["OLD", "YDAY"]
    assert selected["due_for_confirmation"] == ["OLD"]
    assert selected["selected_tickers"] == ["OLD"]


def test_calendar_selector_confirmation_cadence_for_one_to_three_days_away(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(fundamentals_db, "TODAY", "UPCOMING", "2026-08-09", last_observed_at_utc="2026-08-07T00:00:00Z")
    _insert_calendar(fundamentals_db, "YDAY", "UPCOMING", "2026-08-09", last_observed_at_utc="2026-08-06T00:00:00Z")

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["TODAY", "YDAY"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )

    assert selected["due_for_confirmation_watch"] == ["TODAY", "YDAY"]
    assert selected["due_for_confirmation"] == ["YDAY"]
    assert selected["selected_tickers"] == ["YDAY"]


def test_calendar_selector_result_due_exempt_from_future_confirmation_freshness(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(fundamentals_db, "DUE", "DUE_TODAY", "2026-08-07")

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["DUE"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )

    assert selected["due_for_result_check"] == ["DUE"]
    assert selected["due_for_confirmation"] == []
    assert selected["selected_tickers"] == ["DUE"]


def test_calendar_selector_imminent_failure_retries_next_day(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(
        fundamentals_db,
        "SOON",
        "UPCOMING",
        "2026-08-09",
        last_observed_at_utc="2026-08-06T00:00:00Z",
        check_status="TIMEOUT",
        failure_count=1,
    )
    _insert_calendar(
        fundamentals_db,
        "LATER",
        "UPCOMING",
        "2026-08-12",
        last_observed_at_utc="2026-08-06T00:00:00Z",
        check_status="TIMEOUT",
        failure_count=1,
    )

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["SOON", "LATER"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
        failure_retry_days=3,
    )

    assert selected["due_for_confirmation_watch"] == ["LATER", "SOON"]
    assert selected["due_for_confirmation"] == ["SOON"]
    assert selected["selected_tickers"] == ["SOON"]


def test_calendar_selector_deduplicates_result_due_before_failure_retry_and_maintenance(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(
        fundamentals_db,
        "DUP",
        "DATE_PASSED_EVENT_NOT_FOUND",
        "2026-08-06",
        last_observed_at_utc="2026-08-01T00:00:00Z",
        check_status="TIMEOUT",
        failure_count=2,
    )

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["DUP"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
        maintenance_limit=50,
    )

    assert selected["due_for_result_check"] == ["DUP"]
    assert selected["due_for_confirmation"] == []
    assert selected["calendar_maintenance"] == []
    assert selected["selected_tickers"] == ["DUP"]


def test_calendar_selector_new_check_state_null_uses_existing_last_observed_freshness(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    _insert_calendar(fundamentals_db, "BOOT", "UPCOMING", "2026-08-12")
    with sqlite3.connect(str(fundamentals_db)) as conn:
        conn.execute(
            """
            UPDATE rc_earnings_calendar
            SET calendar_last_checked_at_utc = NULL,
                calendar_check_status = NULL
            WHERE ticker = 'BOOT'
            """
        )
        conn.commit()

    selected = result_check.select_calendar_refresh_candidates(
        fundamentals_db=fundamentals_db,
        active_tickers=["BOOT"],
        decision_date=result_check._parse_date("2026-08-07"),
        event_watch_days_after=5,
    )

    assert selected["due_for_confirmation_watch"] == ["BOOT"]
    assert selected["due_for_confirmation"] == []
    assert selected["selected_tickers"] == []


def test_result_check_reconstructs_accumulated_backlog_for_fourteen_days_without_update(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    tickers = ["A", "B", "C", "D"]
    ohlcv_db = _ohlcv_db_for(tmp_path, tickers)
    _mock_result_check_provider_stages(monkeypatch)
    for ticker in tickers:
        _insert_quarter(fundamentals_db, ticker, "2026-03-31")
        _insert_calendar(fundamentals_db, ticker, "UPCOMING", "2026-10-15")

    discovered: dict[str, str] = {}
    plans: dict[int, dict[str, object]] = {}
    for day_index in range(1, 15):
        current = date(2026, 8, 1) + timedelta(days=day_index - 1)
        if day_index in {1, 3, 6, 12}:
            ticker = {1: "A", 3: "B", 6: "C", 12: "D"}[day_index]
            event_id = day_index
            event_date = current.isoformat()
            _set_calendar_status(fundamentals_db, ticker, "DUE_TODAY", event_date)
            _insert_event_and_match(fundamentals_db, ticker, event_date, "2026-06-30", event_id=event_id)
            _set_calendar_status(fundamentals_db, ticker, "UPCOMING", "2026-10-15")
            discovered[ticker] = "2026-06-30"

        result = _run_check(fundamentals_db, ohlcv_db, current, f"pytest_backlog_14d_day_{day_index}")
        plans[day_index] = result["plan"]
        assert _candidate_pairs(result["plan"]) == sorted(discovered.items())

    assert _candidate_pairs(plans[8])[:1] == [("A", "2026-06-30")]
    assert _candidate_pairs(plans[14]) == [("A", "2026-06-30"), ("B", "2026-06-30"), ("C", "2026-06-30"), ("D", "2026-06-30")]


def test_result_check_reconstructs_mixed_unresolved_decisions_after_calendar_window_expires(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    tickers = ["NEW", "PART", "FAIL"]
    ohlcv_db = _ohlcv_db_for(tmp_path, tickers)
    _mock_result_check_provider_stages(monkeypatch)
    for ticker in tickers:
        _insert_quarter(fundamentals_db, ticker, "2026-03-31")
        _insert_calendar(fundamentals_db, ticker, "UPCOMING", "2026-10-15")
        _insert_event_and_match(fundamentals_db, ticker, "2026-08-01", "2026-06-30", event_id={"NEW": 11, "PART": 12, "FAIL": 13}[ticker])
    _insert_partial_quarter(fundamentals_db, "PART", "2026-06-30")
    _insert_ingestion_status(
        fundamentals_db,
        "PART",
        "2026-06-30",
        quarter_basic_complete=0,
        missing_basic_fields='["revenue"]',
        ingestion_status="FUNDAMENTALS_PARTIAL",
    )
    _insert_partial_quarter(fundamentals_db, "FAIL", "2026-06-30")
    _insert_ingestion_status(
        fundamentals_db,
        "FAIL",
        "2026-06-30",
        quarter_basic_complete=0,
        ingestion_status="FETCH_FAILED",
    )

    result = _run_check(fundamentals_db, ohlcv_db, date(2026, 8, 21), "pytest_backlog_mixed_day_21")
    decisions = {row["ticker"]: row["decision"] for row in result["plan"]["candidates"]}

    assert decisions == {
        "FAIL": "RETRY_FETCH_FAILED",
        "NEW": "FETCH_NEW_QUARTER",
        "PART": "RETRY_PARTIAL_QUARTER",
    }
    assert _candidate_pairs(result["plan"]) == [("FAIL", "2026-06-30"), ("NEW", "2026-06-30"), ("PART", "2026-06-30")]
    work_unit_keys = sorted(row["work_unit_key"] for row in result["plan"]["candidates"])
    assert work_unit_keys == ["usa:FAIL:2026:Q2", "usa:NEW:2026:Q2", "usa:PART:2026:Q2"]
    assert result["summary"]["partial_follow_up_count"] == 1


def test_historical_unknown_partial_is_manual_review_not_plan_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db_for(tmp_path, ["HIST"])
    _mock_result_check_provider_stages(monkeypatch)
    _insert_quarter(fundamentals_db, "HIST", "2026-03-31")
    _insert_calendar(fundamentals_db, "HIST", "UPCOMING", "2026-10-15")
    _insert_event_and_match(fundamentals_db, "HIST", "2026-08-01", "2026-06-30", event_id=21)
    _insert_partial_quarter(fundamentals_db, "HIST", "2026-06-30")
    _insert_ingestion_status(
        fundamentals_db,
        "HIST",
        "2026-06-30",
        quarter_basic_complete=0,
        missing_basic_fields='["revenue"]',
        ingestion_status="UNKNOWN_HISTORICAL_INGEST_COMPLETENESS",
    )

    result = _run_check(fundamentals_db, ohlcv_db, date(2026, 8, 21), "pytest_historical_unknown_partial")

    assert result["plan"]["candidate_count"] == 0
    manual_review = Path(result["artifact_paths"]["manual_review_csv"]).read_text(encoding="utf-8")
    assert "REVIEW_HISTORICAL_PARTIAL" in manual_review


def test_successful_update_naturally_removes_candidate_without_ack(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    tickers = ["DONE", "OPEN"]
    ohlcv_db = _ohlcv_db_for(tmp_path, tickers)
    _mock_result_check_provider_stages(monkeypatch)
    for index, ticker in enumerate(tickers, start=1):
        _insert_quarter(fundamentals_db, ticker, "2026-03-31")
        _insert_calendar(fundamentals_db, ticker, "UPCOMING", "2026-10-15")
        _insert_event_and_match(fundamentals_db, ticker, "2026-08-01", "2026-06-30", event_id=index)

    before = _run_check(fundamentals_db, ohlcv_db, date(2026, 8, 14), "pytest_backlog_before_update")
    assert _candidate_pairs(before["plan"]) == [("DONE", "2026-06-30"), ("OPEN", "2026-06-30")]

    _insert_quarter(fundamentals_db, "DONE", "2026-06-30")
    _insert_ingestion_status(fundamentals_db, "DONE", "2026-06-30", quarter_basic_complete=1)

    after = _run_check(fundamentals_db, ohlcv_db, date(2026, 8, 15), "pytest_backlog_after_update")
    assert _candidate_pairs(after["plan"]) == [("OPEN", "2026-06-30")]


def test_stale_plan_is_not_backlog_storage_for_fresh_day_fourteen_plan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db_for(tmp_path, ["A", "B"])
    _mock_result_check_provider_stages(monkeypatch)
    for ticker in ("A", "B"):
        _insert_quarter(fundamentals_db, ticker, "2026-03-31")
        _insert_calendar(fundamentals_db, ticker, "UPCOMING", "2026-10-15")

    _insert_event_and_match(fundamentals_db, "A", "2026-08-01", "2026-06-30", event_id=1)
    day_1 = _run_check(fundamentals_db, ohlcv_db, date(2026, 8, 1), "pytest_stale_plan_day_1")
    assert _candidate_pairs(day_1["plan"]) == [("A", "2026-06-30")]

    _insert_event_and_match(fundamentals_db, "B", "2026-08-03", "2026-06-30", event_id=2)
    day_14 = _run_check(fundamentals_db, ohlcv_db, date(2026, 8, 14), "pytest_stale_plan_day_14")

    assert day_14["artifact_paths"]["plan_json"] != day_1["artifact_paths"]["plan_json"]
    assert _candidate_pairs(day_14["plan"]) == [("A", "2026-06-30"), ("B", "2026-06-30")]


def test_unresolved_candidate_persists_for_thirty_days_without_update(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db_for(tmp_path, ["LONG"], latest_date="2026-08-30")
    _mock_result_check_provider_stages(monkeypatch)
    _insert_quarter(fundamentals_db, "LONG", "2026-03-31")
    _insert_calendar(fundamentals_db, "LONG", "UPCOMING", "2026-10-15")
    _insert_event_and_match(fundamentals_db, "LONG", "2026-08-01", "2026-06-30", event_id=30)

    result = _run_check(fundamentals_db, ohlcv_db, date(2026, 8, 30), "pytest_backlog_30d")

    assert _candidate_pairs(result["plan"]) == [("LONG", "2026-06-30")]


def test_calendar_moves_and_duplicate_events_do_not_hide_or_duplicate_unresolved_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db_for(tmp_path, ["MOVE"])
    _mock_result_check_provider_stages(monkeypatch)
    _insert_quarter(fundamentals_db, "MOVE", "2026-03-31")
    _insert_calendar(fundamentals_db, "MOVE", "DUE_TODAY", "2026-08-01")
    _insert_event_and_match(fundamentals_db, "MOVE", "2026-08-01", "2026-06-30", event_id=41)
    with sqlite3.connect(str(fundamentals_db)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                id, market, ticker, announcement_at, announcement_date, announcement_session, is_reported,
                reported_eps, estimated_eps, surprise_pct, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES (40, 'usa', 'MOVE', '2026-08-01T20:01:00Z', '2026-08-01', 'AMC', 1,
                1.0, 0.9, 10.0, 'fixture', '2026-08-01T00:00:00Z', 'UTC',
                '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z')
            """
        )
        conn.commit()
    _set_calendar_status(fundamentals_db, "MOVE", "UPCOMING", "2026-11-01")

    result = _run_check(fundamentals_db, ohlcv_db, date(2026, 8, 20), "pytest_backlog_calendar_moved_duplicate")

    assert _candidate_pairs(result["plan"]) == [("MOVE", "2026-06-30")]


def test_partial_event_refresh_disables_executable_plan(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_partial"
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    _insert_event_and_match(fundamentals_db, "AAPL", "2026-08-07", "2026-06-30")

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers, **kwargs: {"stage": {"stage": "calendar_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_completed_event_refresh", lambda root, db, tickers, **kwargs: {"stage": {"stage": "completed_event_refresh", "status": "PARTIAL"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_match_rebuild", lambda root, db, enabled, **kwargs: {"stage": {"stage": "event_match_rebuild", "status": "SUCCESS", "enabled": enabled}, "summary": {}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["AAPL"],
    )

    assert result["check_status"] == "PARTIAL"
    assert result["plan"]["candidate_count"] == 0
    assert result["plan"]["executable_work_unit_count"] == 0
    assert result["plan"]["work_units"] == []


def test_ambiguous_target_period_is_manual_review_not_executable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_ambiguous"
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    with sqlite3.connect(str(fundamentals_db)) as conn:
        conn.execute(
            """
            INSERT INTO rc_earnings_event (
                market, ticker, announcement_at, announcement_date, announcement_session, is_reported,
                reported_eps, estimated_eps, surprise_pct, source, source_observed_at_utc, source_timezone,
                created_at_utc, updated_at_utc
            ) VALUES ('usa', 'AAPL', '2026-08-07T20:00:00Z', '2026-08-07', 'AMC', 1,
                1.0, 0.9, 10.0, 'fixture', '2026-08-07T00:00:00Z', 'UTC',
                '2026-08-07T00:00:00Z', '2026-08-07T00:00:00Z')
            """
        )
        conn.commit()

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers, **kwargs: {"stage": {"stage": "calendar_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_completed_event_refresh", lambda root, db, tickers, **kwargs: {"stage": {"stage": "completed_event_refresh", "status": "SUCCESS"}, "summary": {}})
    monkeypatch.setattr(result_check, "_run_match_rebuild", lambda root, db, enabled, **kwargs: {"stage": {"stage": "event_match_rebuild", "status": "SUCCESS"}, "summary": {}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["AAPL"],
    )

    assert result["check_status"] == "SUCCESS"
    assert result["plan"]["candidate_count"] == 0
    manual_review = Path(result["artifact_paths"]["manual_review_csv"]).read_text(encoding="utf-8")
    assert "REVIEW_AMBIGUOUS_PERIOD" in manual_review


def test_failed_calendar_refresh_writes_failed_empty_plan(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    output_root = Path.cwd() / "temp" / "pytest_result_check_failed"
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")

    monkeypatch.setattr(result_check, "_run_calendar_refresh", lambda root, db, tickers, **kwargs: {"stage": {"stage": "calendar_refresh", "status": "FAILED"}, "summary": {"error": "boom"}})

    result = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=output_root,
        tickers=["AAPL"],
    )

    assert result["check_status"] == "FAILED"
    assert result["plan"]["candidate_count"] == 0
    assert json.loads(Path(result["artifact_paths"]["plan_json"]).read_text(encoding="utf-8"))["check_status"] == "FAILED"
    provider_counts = json.loads(result["summary"]["provider_call_counts_json"])
    assert provider_counts["yahoo_calendar_tickers"] == 1
    assert result["summary"]["provider_timing_observation_count"] == 1
    with sqlite3.connect(str(fundamentals_db)) as conn:
        kind = conn.execute(
            """
            SELECT observation_kind
            FROM rc_fundamental_provider_observation_content
            """
        ).fetchone()[0]
    assert kind == "PROVIDER_ERROR_RETRY"


def test_result_check_records_timing_content_once_and_poll_seen_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    _mock_result_check_provider_stages(monkeypatch)
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    _insert_event_and_match(fundamentals_db, "AAPL", "2026-08-07", "2026-06-30")

    first = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=Path.cwd() / "temp" / "pytest_result_check_timing_first",
        tickers=["AAPL"],
    )
    second = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=Path.cwd() / "temp" / "pytest_result_check_timing_second",
        tickers=["AAPL"],
    )

    assert first["summary"]["provider_timing_content_inserted_count"] >= 2
    assert second["summary"]["provider_timing_content_inserted_count"] == 0
    assert second["summary"]["provider_timing_content_reused_count"] >= 2
    with sqlite3.connect(str(fundamentals_db)) as conn:
        content_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_provider_observation_content").fetchone()[0]
        seen_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_provider_observation_seen").fetchone()[0]
        provenance_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_field_provenance").fetchone()[0]
    assert content_count >= 2
    assert seen_count >= content_count * 2
    assert provenance_count == 0


def test_result_check_changed_provider_payload_creates_new_content_observation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    ohlcv_db = _ohlcv_db(tmp_path)
    _mock_result_check_provider_stages(monkeypatch)
    _insert_quarter(fundamentals_db, "AAPL")
    _insert_calendar(fundamentals_db, "AAPL", "DUE_TODAY", "2026-08-07")
    _insert_event_and_match(fundamentals_db, "AAPL", "2026-08-07", "2026-06-30")

    result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=Path.cwd() / "temp" / "pytest_result_check_changed_payload_first",
        tickers=["AAPL"],
    )
    with sqlite3.connect(str(fundamentals_db)) as conn:
        conn.execute("UPDATE rc_earnings_calendar SET estimated_session='BMO' WHERE ticker='AAPL'")
        conn.commit()
    second = result_check.run_manual_result_check(
        fundamentals_db=fundamentals_db,
        ohlcv_db=ohlcv_db,
        decision_date="2026-08-07",
        output_root=Path.cwd() / "temp" / "pytest_result_check_changed_payload_second",
        tickers=["AAPL"],
    )

    assert second["summary"]["provider_timing_content_inserted_count"] >= 1


def test_output_root_must_be_under_temp(tmp_path: Path) -> None:
    fundamentals_db = _migrated_db(tmp_path)
    with pytest.raises(ValueError, match="RUNTIME_PATH_OUTSIDE_TEMP"):
        result_check.run_manual_result_check(
            fundamentals_db=fundamentals_db,
            ohlcv_db=tmp_path / "missing_osakedata.db",
            decision_date="2026-08-07",
            output_root=tmp_path / "outside_temp",
            tickers=["AAPL"],
        )
