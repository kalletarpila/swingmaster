"""Tests for regression audit CLI."""

from __future__ import annotations

import sqlite3
import sys


def test_run_regression_audit_passes_empty_db(tmp_path, capsys, monkeypatch):
    from swingmaster.cli import run_regression_audit as mod

    db_path = tmp_path / "rc.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE rc_state_daily (
            ticker TEXT,
            date TEXT,
            state TEXT,
            reasons_json TEXT,
            confidence INTEGER,
            age INTEGER,
            run_id TEXT,
            PRIMARY KEY (ticker, date)
        );
        CREATE TABLE rc_transition (
            ticker TEXT,
            date TEXT,
            from_state TEXT,
            to_state TEXT,
            reasons_json TEXT,
            run_id TEXT,
            PRIMARY KEY (ticker, date)
        );
        CREATE TABLE rc_signal_daily (
            ticker TEXT,
            date TEXT,
            signal_keys_json TEXT,
            run_id TEXT,
            PRIMARY KEY (ticker, date)
        );
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(sys, "argv", ["run_regression_audit.py", "--db", str(db_path)])
    try:
        mod.main()
    except SystemExit as exc:
        assert exc.code == 0

    out = capsys.readouterr().out
    assert "OVERALL: PASS" in out
