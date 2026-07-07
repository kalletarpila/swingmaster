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
