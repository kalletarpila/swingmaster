from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from swingmaster.cli import run_fundamental_quarter_update
from swingmaster.cli.run_fundamental_migrations import run_migration
from swingmaster.fundamentals.result_check import PLAN_VERSION, candidate_hash


def _candidate(ticker: str, target_period_end_date: str = "2026-06-30", decision: str = "FETCH_NEW_QUARTER") -> dict:
    return {
        "market": "usa",
        "ticker": ticker,
        "decision": decision,
        "priority": "P1_FETCH_NOW",
        "fundamental_fetch_enabled": 1,
        "calendar_status": "DUE_TODAY",
        "estimated_announcement_date": "2026-08-07",
        "estimated_session": "AMC",
        "completed_earnings_event_id": 1000,
        "completed_event_date": "2026-08-07",
        "target_period_end_date": target_period_end_date,
        "target_period_resolution_status": "MATCHED_EARNINGS_EVENT",
        "current_quarter_exists": 0,
        "quarter_basic_complete": None,
        "ttm_input_complete": None,
        "score_history_complete": None,
        "missing_basic_fields": None,
        "ingestion_status": None,
        "last_fetch_status": None,
        "planned_action": "PLAN_FETCH_QUARTERLY_FUNDAMENTALS",
        "reason": "fixture",
        "eligible_for_execution": 1,
    }


def _write_plan(tmp_name: str, db_path: Path, candidates: list[dict]) -> Path:
    plan_path = Path.cwd() / "temp" / tmp_name / "plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "plan_version": PLAN_VERSION,
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "decision_date": "2026-08-07",
        "fundamentals_db": str(db_path.resolve()),
        "ohlcv_db": str((plan_path.parent / "osakedata.db").resolve()),
        "ohlcv_stale_days": 14,
        "candidate_count": len(candidates),
        "candidate_hash": candidate_hash(candidates),
        "check_status": "SUCCESS",
        "stages": [{"stage": "fixture", "status": "SUCCESS"}],
        "candidates": candidates,
    }
    plan_path.write_text(json.dumps(payload), encoding="utf-8")
    return plan_path


def _mock_usa_valuation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_fundamental_quarter_update, "resolve_latest_close_as_of_date", lambda *_args, **_kwargs: "2026-08-07")
    monkeypatch.setattr(run_fundamental_quarter_update, "run_fundamental_valuation", lambda **_kwargs: {"rows_written": 0})


def _run_plan(db_path: Path, osakedata_db: Path, plan_path: Path) -> dict[str, object]:
    return run_fundamental_quarter_update.run_fundamental_quarter_update(
        db_path=db_path,
        osakedata_db_path=osakedata_db,
        run_id="HIGH_VOLUME_FIXTURE",
        market="usa",
        ticker=None,
        limit=None,
        dry_run=False,
        skip_ack=False,
        quarter_refresh_plan_json=plan_path,
    )


def _summary_json_from_stdout(output: str, key: str) -> list[dict]:
    prefix = f"SUMMARY {key}="
    for line in output.splitlines():
        if line.startswith(prefix):
            return json.loads(line[len(prefix) :])
    raise AssertionError(f"missing {key} summary line")


def test_plan_mode_handles_200_successful_candidates_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "high_volume_success.db"
    run_migration(db_path)
    candidates = [_candidate(f"T{i:03d}", f"2026-06-{(i % 28) + 1:02d}") for i in range(200)]
    plan_path = _write_plan("pytest_high_volume_success", db_path, candidates)
    attempted: list[tuple[str, str]] = []

    def _process_ticker(**kwargs):
        row = kwargs["row"]
        target = str(kwargs["target_period_end_date"])
        attempted.append((str(row["ticker"]), target))
        return {
            "target_period_end_date": target,
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 1,
            "target_ttm_input_complete": 1,
            "target_score_history_complete": 1,
            "post_update_result": "UPDATED_COMPLETE",
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _process_ticker)
    _mock_usa_valuation(monkeypatch)

    summary = _run_plan(db_path, tmp_path / "osakedata.db", plan_path)

    assert len(attempted) == 200
    assert len(set(attempted)) == 200
    assert attempted == sorted(attempted)
    assert summary["tickers_total"] == 200
    assert summary["tickers_processed"] == 200
    assert summary["tickers_succeeded"] == 200
    assert summary["tickers_failed"] == 0
    assert summary["updated_complete_count"] == 200
    assert summary["plan_candidate_count"] == 200
    assert len(summary["candidate_results"]) == 200
    assert summary["failed_candidates"] == []


def test_plan_mode_mixed_failures_are_isolated_and_reported(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    db_path = tmp_path / "high_volume_mixed.db"
    run_migration(db_path)
    candidates = [_candidate(f"M{i:03d}") for i in range(120)]
    plan_path = _write_plan("pytest_high_volume_mixed", db_path, candidates)
    attempted: list[str] = []

    def _process_ticker(**kwargs):
        ticker = str(kwargs["row"]["ticker"])
        attempted.append(ticker)
        target = str(kwargs["target_period_end_date"])
        if ticker == "M010":
            raise RuntimeError("SEC_REFRESH_TIMEOUT")
        if ticker == "M025":
            raise ValueError("unexpected fixture failure")
        if ticker in {"M040", "M041"}:
            return {
                "target_period_end_date": target,
                "target_quarter_exists": 1,
                "target_quarter_basic_complete": 0,
                "target_ttm_input_complete": 0,
                "target_score_history_complete": 0,
                "post_update_result": "UPDATED_PARTIAL",
            }
        if ticker == "M060":
            return {
                "target_period_end_date": target,
                "target_quarter_exists": 0,
                "target_quarter_basic_complete": 0,
                "target_ttm_input_complete": None,
                "target_score_history_complete": None,
                "post_update_result": "NO_NEW_DATA",
            }
        return {
            "target_period_end_date": target,
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 1,
            "target_ttm_input_complete": 1,
            "target_score_history_complete": 1,
            "post_update_result": "UPDATED_COMPLETE",
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _process_ticker)
    _mock_usa_valuation(monkeypatch)

    with pytest.raises(RuntimeError, match="FUNDAMENTAL_QUARTER_UPDATE_BATCH_FAILED:tickers_failed=2"):
        _run_plan(db_path, tmp_path / "osakedata.db", plan_path)

    output = capsys.readouterr().out
    failed = _summary_json_from_stdout(output, "failed_candidates_json")
    results = _summary_json_from_stdout(output, "candidate_results_json")
    assert len(attempted) == 120
    assert attempted[-1] == "M119"
    assert len(results) == 120
    assert len(failed) == 2
    assert {row["ticker"] for row in failed} == {"M010", "M025"}
    assert {row["failure_step"] for row in failed} == {"quarterly_refresh", "unknown"}
    assert "SUMMARY tickers_succeeded=118" in output
    assert "SUMMARY updated_partial_count=2" in output
    assert "SUMMARY no_new_data_count=1" in output
    assert "SUMMARY fetch_failed_count=2" in output


def test_plan_mode_rejects_duplicate_high_volume_candidates(tmp_path: Path) -> None:
    db_path = tmp_path / "high_volume_duplicate.db"
    run_migration(db_path)
    candidates = [_candidate("DUP"), _candidate("DUP")]
    plan_path = _write_plan("pytest_high_volume_duplicate", db_path, candidates)

    with pytest.raises(RuntimeError, match="RESULT_CHECK_PLAN_DUPLICATE_CANDIDATE"):
        run_fundamental_quarter_update.load_plan_rows(
            plan_path=plan_path,
            db_path=db_path,
            ticker=None,
            limit=None,
        )


def test_plan_replay_is_idempotent_for_completed_targets(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "high_volume_replay.db"
    run_migration(db_path)
    candidates = [_candidate(f"R{i:03d}") for i in range(100)]
    plan_path = _write_plan("pytest_high_volume_replay", db_path, candidates)
    insert_attempts = 0

    def _process_ticker(**kwargs):
        nonlocal insert_attempts
        ticker = str(kwargs["row"]["ticker"])
        target = str(kwargs["target_period_end_date"])
        with sqlite3.connect(str(db_path)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM rc_fundamental_quarterly WHERE ticker = ? AND period_end_date = ?",
                (ticker, target),
            ).fetchone()
            if exists is None:
                insert_attempts += 1
                conn.execute(
                    """
                    INSERT INTO rc_fundamental_quarterly (
                        ticker, period_end_date, revenue, ebit, free_cashflow, cash, total_debt, shares_outstanding, run_id
                    ) VALUES (?, ?, 100, 10, 8, 20, 5, 10, 'FIXTURE')
                    """,
                    (ticker, target),
                )
                conn.commit()
        return {
            "target_period_end_date": target,
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 1,
            "target_ttm_input_complete": 1,
            "target_score_history_complete": 1,
            "post_update_result": "UPDATED_COMPLETE",
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _process_ticker)
    _mock_usa_valuation(monkeypatch)

    first = _run_plan(db_path, tmp_path / "osakedata.db", plan_path)
    second = _run_plan(db_path, tmp_path / "osakedata.db", plan_path)

    with sqlite3.connect(str(db_path)) as conn:
        row_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
    assert first["tickers_succeeded"] == 100
    assert second["tickers_succeeded"] == 100
    assert row_count == 100
    assert insert_attempts == 100


def test_partial_run_can_be_restarted_with_same_fresh_plan(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "high_volume_restart.db"
    run_migration(db_path)
    candidates = [_candidate(f"P{i:03d}") for i in range(100)]
    plan_path = _write_plan("pytest_high_volume_restart", db_path, candidates)
    first_attempts: list[str] = []
    second_attempts: list[str] = []

    def _insert_if_missing(ticker: str, target: str) -> None:
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO rc_fundamental_quarterly (
                    ticker, period_end_date, revenue, ebit, free_cashflow, cash, total_debt, shares_outstanding, run_id
                ) VALUES (?, ?, 100, 10, 8, 20, 5, 10, 'FIXTURE')
                """,
                (ticker, target),
            )
            conn.commit()

    def _interrupting_process(**kwargs):
        ticker = str(kwargs["row"]["ticker"])
        target = str(kwargs["target_period_end_date"])
        first_attempts.append(ticker)
        if len(first_attempts) > 35:
            raise KeyboardInterrupt("fixture interruption")
        _insert_if_missing(ticker, target)
        return {
            "target_period_end_date": target,
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 1,
            "post_update_result": "UPDATED_COMPLETE",
        }

    def _restart_process(**kwargs):
        ticker = str(kwargs["row"]["ticker"])
        target = str(kwargs["target_period_end_date"])
        second_attempts.append(ticker)
        _insert_if_missing(ticker, target)
        return {
            "target_period_end_date": target,
            "target_quarter_exists": 1,
            "target_quarter_basic_complete": 1,
            "post_update_result": "UPDATED_COMPLETE",
        }

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _interrupting_process)
    _mock_usa_valuation(monkeypatch)
    with pytest.raises(KeyboardInterrupt):
        _run_plan(db_path, tmp_path / "osakedata.db", plan_path)

    monkeypatch.setattr(run_fundamental_quarter_update, "process_ticker", _restart_process)
    summary = _run_plan(db_path, tmp_path / "osakedata.db", plan_path)

    with sqlite3.connect(str(db_path)) as conn:
        row_count = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly").fetchone()[0]
    assert first_attempts == [f"P{i:03d}" for i in range(36)]
    assert len(second_attempts) == 100
    assert row_count == 100
    assert summary["tickers_succeeded"] == 100
