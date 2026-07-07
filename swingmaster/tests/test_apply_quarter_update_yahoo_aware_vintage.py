from __future__ import annotations

import sqlite3
from pathlib import Path

from swingmaster.cli import apply_quarter_update_yahoo_aware_vintage as apply_cli
from swingmaster.cli.run_fundamental_migrations import run_migration


def test_without_approval_token_does_not_write(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        before = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]

    summary = apply_cli.run_apply_quarter_update_yahoo_aware_vintage(
        fundamentals_db=db_path,
        market="usa",
        source_run_id="USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
        vintage_run_id="USA_QUARTER_UPDATE_2026-05-10__YAHOO_AWARE_VINTAGE",
        available_at_utc="2026-05-10T12:00:00Z",
        ingested_at_utc="2026-05-10T12:00:00Z",
        approval_token="",
    )

    with sqlite3.connect(str(db_path)) as conn:
        after = conn.execute("SELECT COUNT(*) FROM rc_fundamental_quarterly_vintage").fetchone()[0]

    assert summary["vintage_yahoo_aware_execution_status"] == "APPROVAL_REQUIRED"
    assert summary["vintage_yahoo_aware_error"] == "APPROVAL_TOKEN_REQUIRED"
    assert after == before


def test_parse_args_requires_explicit_metadata() -> None:
    args = apply_cli.parse_args(
        [
            "--fundamentals-db",
            "fundamentals.db",
            "--market",
            "usa",
            "--source-run-id",
            "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
            "--vintage-run-id",
            "USA_QUARTER_UPDATE_2026-05-10__YAHOO_AWARE_VINTAGE",
            "--available-at-utc",
            "2026-05-10T12:00:00Z",
            "--ingested-at-utc",
            "2026-05-10T12:00:00Z",
            "--approval-token",
            apply_cli.APPROVAL_TOKEN,
        ]
    )

    assert args.market == "usa"
    assert args.approval_token == apply_cli.APPROVAL_TOKEN


def test_dry_run_json_args_parse() -> None:
    args = apply_cli.parse_args(
        [
            "--fundamentals-db",
            "fundamentals.db",
            "--market",
            "usa",
            "--source-run-id",
            "USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
            "--vintage-run-id",
            "USA_QUARTER_UPDATE_2026-05-10__YAHOO_AWARE_VINTAGE_RECOVERY",
            "--available-at-utc",
            "2026-05-10T12:00:00Z",
            "--ingested-at-utc",
            "2026-05-10T12:00:00Z",
            "--dry-run",
            "--format",
            "json",
            "--expected-final-mixed-count",
            "2",
            "--expected-yahoo-vintage-count",
            "0",
        ]
    )

    assert args.dry_run is True
    assert args.format == "json"
    assert args.expected_final_mixed_count == 2
    assert args.expected_yahoo_vintage_count == 0


def test_expected_count_mismatch_blocks_before_execution(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "fundamentals.db"
    run_migration(db_path)
    executed = {"called": False}

    monkeypatch.setattr(
        apply_cli,
        "build_quarter_update_vintage_post_run_guard_summary",
        lambda *args, **kwargs: {
            "vintage_yahoo_aware_planning_status": "FINAL_MIXED_PLAN_READY",
            "vintage_planned_final_mixed_rows": 2,
            "vintage_planned_yahoo_vintage_rows": 0,
            "vintage_planned_yahoo_aware_provenance_rows": 10,
            "vintage_yahoo_aware_blocked_rows": 0,
            "vintage_yahoo_aware_unknown_provenance_fields": "",
        },
    )
    monkeypatch.setattr(apply_cli, "_latest_rows_for_run_ids", lambda *args, **kwargs: [])
    monkeypatch.setattr(apply_cli, "_sec_provenance_by_key", lambda *args, **kwargs: {})
    monkeypatch.setattr(apply_cli, "_yahoo_audit_rows_by_key", lambda *args, **kwargs: {})
    monkeypatch.setattr(apply_cli, "_yahoo_rows_by_key", lambda *args, **kwargs: {})
    monkeypatch.setattr(apply_cli, "_build_yahoo_aware_execution_candidates", lambda *args, **kwargs: ({}, {}))

    def _execute(*args, **kwargs):
        executed["called"] = True
        return {}

    monkeypatch.setattr(apply_cli, "execute_quarter_update_yahoo_aware_vintage_plan", _execute)

    summary = apply_cli.run_apply_quarter_update_yahoo_aware_vintage(
        fundamentals_db=db_path,
        market="usa",
        source_run_id="USA_QUARTER_UPDATE_2026-05-10__QUARTERLY",
        vintage_run_id="USA_QUARTER_UPDATE_2026-05-10__YAHOO_AWARE_VINTAGE_RECOVERY",
        available_at_utc="2026-05-10T12:00:00Z",
        ingested_at_utc="2026-05-10T12:00:00Z",
        approval_token=apply_cli.APPROVAL_TOKEN,
        expected_final_mixed_count=1,
        expected_yahoo_vintage_count=0,
    )

    assert summary["overall_status"] == "YAHOO_AWARE_RECOVERY_READY"
    assert summary["vintage_yahoo_aware_execution_status"] == "EXECUTION_BLOCKED"
    assert "EXPECTED_FINAL_MIXED_COUNT_MISMATCH" in str(summary["vintage_yahoo_aware_error"])
    assert executed["called"] is False
